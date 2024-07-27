// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

use std::sync::Arc;

use crate::aggregates::group_values::GroupValues;
use ahash::RandomState;
use arrow::array::AsArray as _;
use arrow::compute::cast;
use arrow::datatypes::UInt32Type;
use arrow::record_batch::RecordBatch;
use arrow::row::{RowConverter, Rows, SortField};
use arrow_array::{Array, ArrayRef, StringViewArray};
use arrow_schema::{DataType, SchemaRef};
// use datafusion_common::hash_utils::create_hashes;
use datafusion_common::{DataFusionError, Result};
use datafusion_execution::memory_pool::proxy::RawTableAllocExt;
use datafusion_expr::EmitTo;
use datafusion_physical_expr_common::binary_view_map::ArrowBytesViewMap;
use hashbrown::raw::RawTable;
use itertools::Itertools;

struct VarLenGroupValues {
    map: ArrowBytesViewMap<u32>,
    num_groups: u32,
}

impl VarLenGroupValues {
    fn new() -> Self {
        Self {
            map: ArrowBytesViewMap::new(
                datafusion_physical_expr::binary_map::OutputType::Utf8View,
            ),
            num_groups: 0,
        }
    }
}

/// A [`GroupValues`] making use of [`Rows`]
pub struct GroupValuesRows {
    /// The output schema
    schema: SchemaRef,

    /// Converter for the group values
    row_converter: RowConverter,

    /// Logically maps group values to a group_index in
    /// [`Self::group_values`] and in each accumulator
    ///
    /// Uses the raw API of hashbrown to avoid actually storing the
    /// keys (group values) in the table
    ///
    /// keys: u64 hashes of the GroupValue
    /// values: (hash, group_index)
    map: RawTable<(u64, usize)>,

    /// The size of `map` in bytes
    map_size: usize,

    /// The actual group by values, stored in arrow [`Row`] format.
    /// `group_values[i]` holds the group value for group_index `i`.
    ///
    /// The row format is used to compare group keys quickly and store
    /// them efficiently in memory. Quick comparison is especially
    /// important for multi-column group keys.
    ///
    /// [`Row`]: arrow::row::Row
    group_values: Option<Rows>,

    /// reused buffer to store rows
    rows_buffer: Rows,

    // variable length column map
    var_len_map: Vec<VarLenGroupValues>,

    /// Random state for creating hashes
    random_state: RandomState,
}

impl GroupValuesRows {
    pub fn try_new(schema: SchemaRef) -> Result<Self> {
        let mut var_len_map = Vec::new();
        let row_converter = RowConverter::new(
            schema
                .fields()
                .iter()
                .map(|f| {
                    if f.data_type() == &DataType::Utf8View {
                        var_len_map.push(VarLenGroupValues::new());
                        SortField::new(DataType::UInt32)
                    } else {
                        SortField::new(f.data_type().clone())
                    }
                })
                .collect(),
        )?;

        let map = RawTable::with_capacity(0);

        let starting_rows_capacity = 1000;
        let starting_data_capacity = 64 * starting_rows_capacity;
        let rows_buffer =
            row_converter.empty_rows(starting_rows_capacity, starting_data_capacity);
        Ok(Self {
            schema,
            row_converter,
            map,
            map_size: 0,
            group_values: None,
            rows_buffer,
            var_len_map,
            random_state: Default::default(),
        })
    }

    fn transform_col_to_fixed_len(&mut self, input: &[ArrayRef]) -> Vec<ArrayRef> {
        let mut cur_var_len_idx = 0;
        let transformed_cols: Vec<ArrayRef> = input
            .iter()
            .map(|c| {
                if let DataType::Utf8View = c.data_type() {
                    let mut var_groups = Vec::with_capacity(c.len());
                    let group_values = &mut self.var_len_map[cur_var_len_idx];
                    group_values.map.insert_if_new(
                        c,
                        |_value| {
                            let group_idx = group_values.num_groups;
                            group_values.num_groups += 1;
                            group_idx
                        },
                        |group_idx| {
                            var_groups.push(group_idx);
                        },
                    );
                    cur_var_len_idx += 1;
                    std::sync::Arc::new(arrow_array::UInt32Array::from(var_groups))
                        as ArrayRef
                } else {
                    c.clone()
                }
            })
            .collect();
        transformed_cols
    }

    fn transform_col_to_var_len(&mut self, output: Vec<ArrayRef>) -> Vec<ArrayRef> {
        let mut cur_var_len_idx = 0;
        let output = output
            .into_iter()
            .enumerate()
            .map(|(i, array)| {
                let data_type = self.schema.field(i).data_type();
                if data_type == &DataType::Utf8View {
                    let arr = array.as_primitive::<UInt32Type>();
                    let mut views = Vec::with_capacity(arr.len());

                    let map_content =
                        &mut self.var_len_map[cur_var_len_idx].map.take().into_state();
                    let map_content = map_content.as_string_view();

                    for v in arr.iter() {
                        if let Some(index) = v {
                            let value = unsafe {
                                map_content.views().get_unchecked(index as usize)
                            };
                            views.push(*value);
                        } else {
                            views.push(0);
                        }
                    }
                    let output_str = unsafe {
                        StringViewArray::new_unchecked(
                            views.into(),
                            map_content.data_buffers().to_vec(),
                            map_content.nulls().map(|v| v.clone()),
                        )
                    };
                    cur_var_len_idx += 1;
                    Arc::new(output_str) as ArrayRef
                } else {
                    array
                }
            })
            .collect_vec();
        output
    }
}

impl GroupValues for GroupValuesRows {
    fn intern(&mut self, cols: &[ArrayRef], groups: &mut Vec<usize>) -> Result<()> {
        let transformed_cols: Vec<ArrayRef> = self.transform_col_to_fixed_len(cols);

        let group_rows = &mut self.rows_buffer;
        group_rows.clear();
        self.row_converter.append(group_rows, &transformed_cols)?;

        let mut group_values = match self.group_values.take() {
            Some(group_values) => group_values,
            None => self.row_converter.empty_rows(0, 0),
        };

        // tracks to which group each of the input rows belongs
        groups.clear();

        for row in group_rows.iter() {
            let hash = self.random_state.hash_one(row.as_ref());
            let entry = self.map.get_mut(hash, |(_hash, group_idx)| {
                // verify that a group that we are inserting with hash is
                // actually the same key value as the group in
                // existing_idx  (aka group_values @ row)
                row.as_ref().len() == group_values.row(*group_idx).as_ref().len()
                    && row == group_values.row(*group_idx)
            });
            let group_idx = match entry {
                // Existing group_index for this group value
                Some((_hash, group_idx)) => *group_idx,
                //  1.2 Need to create new entry for the group
                None => {
                    // Add new entry to aggr_state and save newly created index
                    let group_idx = group_values.num_rows();
                    group_values.push(row);

                    // for hasher function, use precomputed hash value
                    self.map.insert_accounted(
                        (hash, group_idx),
                        |(hash, _group_index)| *hash,
                        &mut self.map_size,
                    );
                    group_idx
                }
            };
            groups.push(group_idx);
        }

        self.group_values = Some(group_values);

        Ok(())
    }

    fn size(&self) -> usize {
        let group_values_size = self.group_values.as_ref().map(|v| v.size()).unwrap_or(0);
        self.row_converter.size()
            + group_values_size
            + self.map_size
            + self.rows_buffer.size()
    }

    fn is_empty(&self) -> bool {
        self.len() == 0
    }

    fn len(&self) -> usize {
        self.group_values
            .as_ref()
            .map(|group_values| group_values.num_rows())
            .unwrap_or(0)
    }

    fn emit(&mut self, emit_to: EmitTo) -> Result<Vec<ArrayRef>> {
        let mut group_values = self
            .group_values
            .take()
            .expect("Can not emit from empty rows");

        let mut output = match emit_to {
            EmitTo::All => {
                let output = self.row_converter.convert_rows(&group_values)?;
                let output = self.transform_col_to_var_len(output);
                group_values.clear();
                output
            }
            EmitTo::First(_n) => {
                unimplemented!("Not supported yet!")
            }
        };

        // TODO: Materialize dictionaries in group keys (#7647)
        for (field, array) in self.schema.fields.iter().zip(&mut output) {
            let expected = field.data_type();
            if let DataType::Dictionary(_, v) = expected {
                let actual = array.data_type();
                if v.as_ref() != actual {
                    return Err(DataFusionError::Internal(format!(
                        "Converted group rows expected dictionary of {v} got {actual}"
                    )));
                }
                *array = cast(array.as_ref(), expected)?;
            }
        }

        self.group_values = Some(group_values);
        Ok(output)
    }

    fn clear_shrink(&mut self, batch: &RecordBatch) {
        let count = batch.num_rows();
        self.group_values = self.group_values.take().map(|mut rows| {
            rows.clear();
            rows
        });
        self.map.clear();
        self.map.shrink_to(count, |_| 0); // hasher does not matter since the map is cleared
        self.map_size = self.map.capacity() * std::mem::size_of::<(u64, usize)>();
    }
}
