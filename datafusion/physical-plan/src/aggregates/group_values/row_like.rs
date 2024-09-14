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

use std::ops::DerefMut;

use crate::aggregates::group_values::GroupValues;
use ahash::RandomState;
use arrow::array::{GenericStringBuilder, PrimitiveBuilder};
use arrow::compute::cast;
use arrow::datatypes::{
    Date32Type, Date64Type, Float32Type, Float64Type, Int16Type, Int32Type, Int64Type,
    Int8Type, UInt16Type, UInt32Type, UInt64Type, UInt8Type,
};
use arrow::record_batch::RecordBatch;
use arrow::row::{RowConverter, Rows, SortField};
use arrow_array::{Array, ArrayRef};
use arrow_schema::{DataType, SchemaRef};
use datafusion_common::hash_utils::create_hashes;
use datafusion_common::{internal_err, DataFusionError, Result};
use datafusion_execution::memory_pool::proxy::{RawTableAllocExt, VecAllocExt};
use datafusion_expr::EmitTo;
use datafusion_physical_expr_common::group_value_row::ArrayEqV2;
use hashbrown::raw::RawTable;

pub(super) const INITIAL_CAPACITY: usize = 8 * 1024;

/// A [`GroupValues`] making use of [`Rows`]
pub struct GroupValuesRowLike {
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
    // group_values: Option<Rows>,

    /// reused buffer to store hashes
    hashes_buffer: Vec<u64>,

    /// reused buffer to store rows
    rows_buffer: Rows,

    /// Random state for creating hashes
    random_state: RandomState,
    group_values_v2: Option<Vec<Box<dyn ArrayEqV2>>>,
}

impl GroupValuesRowLike {
    pub fn try_new(schema: SchemaRef) -> Result<Self> {
        let row_converter = RowConverter::new(
            schema
                .fields()
                .iter()
                .map(|f| SortField::new(f.data_type().clone()))
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
            // group_values: None,
            group_values_v2: None,
            hashes_buffer: Default::default(),
            rows_buffer,
            random_state: Default::default(),
        })
    }
}

impl GroupValues for GroupValuesRowLike {
    fn intern(&mut self, cols: &[ArrayRef], groups: &mut Vec<usize>) -> Result<()> {
        // Convert the group keys into the row format
        // let group_rows = &mut self.rows_buffer;
        // group_rows.clear();
        // self.row_converter.append(group_rows, cols)?;
        // let n_rows = group_rows.num_rows();

        // let mut group_values = match self.group_values.take() {
        //     Some(group_values) => group_values,
        //     None => self.row_converter.empty_rows(0, 0),
        // };

        let n_rows = cols[0].len();
        let mut group_values_v2 = match self.group_values_v2.take() {
            Some(group_values) => group_values,
            None => {
                let len = cols.len();
                let mut v = Vec::with_capacity(len);
                // Move to `try_new`
                for (i, f) in self.schema.fields().iter().enumerate() {
                    match f.data_type() {
                        &DataType::Int8 => {
                            let b = PrimitiveBuilder::<Int8Type>::new();
                            v.push(Box::new(b) as _)
                        }
                        &DataType::Int16 => {
                            let b = PrimitiveBuilder::<Int16Type>::new();
                            v.push(Box::new(b) as _)
                        }
                        &DataType::Int32 => {
                            let b = PrimitiveBuilder::<Int32Type>::new();
                            v.push(Box::new(b) as _)
                        }
                        &DataType::Int64 => {
                            let b = PrimitiveBuilder::<Int64Type>::new();
                            v.push(Box::new(b) as _)
                        }
                        &DataType::UInt8 => {
                            let b = PrimitiveBuilder::<UInt8Type>::new();
                            v.push(Box::new(b) as _)
                        }
                        &DataType::UInt16 => {
                            let b = PrimitiveBuilder::<UInt16Type>::new();
                            v.push(Box::new(b) as _)
                        }
                        &DataType::UInt32 => {
                            let b = PrimitiveBuilder::<UInt32Type>::new();
                            v.push(Box::new(b) as _)
                        }
                        &DataType::UInt64 => {
                            let b = PrimitiveBuilder::<UInt64Type>::new();
                            v.push(Box::new(b) as _)
                        }
                        &DataType::Float32 => {
                            let b = PrimitiveBuilder::<Float32Type>::new();
                            v.push(Box::new(b) as _)
                        }
                        &DataType::Float64 => {
                            let b = PrimitiveBuilder::<Float64Type>::new();
                            v.push(Box::new(b) as _)
                        }
                        &DataType::Date32 => {
                            let b = PrimitiveBuilder::<Date32Type>::new();
                            v.push(Box::new(b) as _)
                        }
                        &DataType::Date64 => {
                            let b = PrimitiveBuilder::<Date64Type>::new();
                            v.push(Box::new(b) as _)
                        }
                        &DataType::Utf8 => {
                            let b = GenericStringBuilder::<i32>::with_capacity(
                                INITIAL_CAPACITY,
                                INITIAL_CAPACITY,
                            );
                            v.push(Box::new(b) as _)
                        }
                        &DataType::LargeUtf8 => {
                            let b = GenericStringBuilder::<i64>::with_capacity(
                                INITIAL_CAPACITY,
                                INITIAL_CAPACITY,
                            );
                            v.push(Box::new(b) as _)
                        }
                        dt => todo!("{dt} not impl"),
                    }
                }
                v
            }
        };

        // tracks to which group each of the input rows belongs
        groups.clear();

        // 1.1 Calculate the group keys for the group values
        let batch_hashes = &mut self.hashes_buffer;
        batch_hashes.clear();
        batch_hashes.resize(n_rows, 0);
        create_hashes(cols, &self.random_state, batch_hashes)?;

        for (row, &target_hash) in batch_hashes.iter().enumerate() {
            let entry = self.map.get_mut(target_hash, |(exist_hash, group_idx)| {
                // Somewhat surprisingly, this closure can be called even if the
                // hash doesn't match, so check the hash first with an integer
                // comparison first avoid the more expensive comparison with
                // group value. https://github.com/apache/datafusion/pull/11718
                if target_hash != *exist_hash {
                    return false;
                }
                // verify that the group that we are inserting with hash is
                // actually the same key value as the group in
                // existing_idx  (aka group_values @ row)
                // && group_rows.row(row) == group_values.row(*group_idx)

                fn compare_equal(
                    arry_eq: &dyn ArrayEqV2,
                    lhs_row: usize,
                    array: &ArrayRef,
                    rhs_row: usize,
                ) -> bool {
                    arry_eq.equal_to(lhs_row, array, rhs_row)
                }

                for (i, group_val) in group_values_v2.iter().enumerate() {
                    if !compare_equal(group_val.as_ref(), *group_idx, &cols[i], row) {
                        return false;
                    }
                }

                true
            });

            let group_idx = match entry {
                // Existing group_index for this group value
                Some((_hash, group_idx)) => *group_idx,
                //  1.2 Need to create new entry for the group
                None => {
                    // Add new entry to aggr_state and save newly created index
                    // let group_idx = group_values.num_rows();
                    // group_values.push(group_rows.row(row));

                    let mut checklen = 0;
                    let group_idx = group_values_v2[0].len();
                    for (i, group_value) in group_values_v2.iter_mut().enumerate() {
                        group_value.append_val(&cols[i], row);
                        let len = group_value.len();
                        if i == 0 {
                            checklen = len;
                        } else {
                            assert_eq!(checklen, len);
                        }
                    }

                    // for hasher function, use precomputed hash value
                    self.map.insert_accounted(
                        (target_hash, group_idx),
                        |(hash, _group_index)| *hash,
                        &mut self.map_size,
                    );
                    group_idx
                }
            };
            groups.push(group_idx);
        }

        // self.group_values = Some(group_values);
        self.group_values_v2 = Some(group_values_v2);

        Ok(())
    }

    fn size(&self) -> usize {
        // TODO: get real size
        let group_values_size =
            self.group_values_v2.as_ref().map(|v| v.len()).unwrap_or(0);
        // let group_values_size = self.group_values.as_ref().map(|v| v.size()).unwrap_or(0);
        self.row_converter.size()
            + group_values_size
            + self.map_size
            + self.rows_buffer.size()
            + self.hashes_buffer.allocated_size()
    }

    fn is_empty(&self) -> bool {
        self.len() == 0
    }

    fn len(&self) -> usize {
        // self.group_values
        //     .as_ref()
        //     .map(|group_values| group_values.num_rows())
        //     .unwrap_or(0)

        self.group_values_v2
            .as_ref()
            .map(|v| v[0].len())
            .unwrap_or(0)
    }

    fn emit(&mut self, emit_to: EmitTo) -> Result<Vec<ArrayRef>> {
        // println!("emit");
        // let mut group_values = self
        //     .group_values
        //     .take()
        //     .expect("Can not emit from empty rows");

        let mut group_values_v2 = self
            .group_values_v2
            .take()
            .expect("Can not emit from empty rows");

        let mut output = match emit_to {
            EmitTo::All => {
                let output = group_values_v2
                    .into_iter()
                    .map(|mut v| {
                        let p = v.deref_mut().build();
                        p
                    })
                    .collect::<Vec<_>>();
                // let output = self.row_converter.convert_rows(&group_values)?;
                // group_values.clear();
                // println!("output: {:?}", output);
                self.group_values_v2 = None;
                output
            }
            EmitTo::First(n) => {
                return internal_err!("there is error");

                // println!("to first n");
                let len = group_values_v2.len();
                let first_n: Vec<Box<dyn ArrayEqV2>> =
                    group_values_v2.drain(..n).collect();
                let output = first_n
                    .into_iter()
                    .map(|mut v| {
                        let p = v.deref_mut().build();
                        p
                    })
                    .collect::<Vec<_>>();
                assert_eq!(len, group_values_v2.len() + n);
                self.group_values_v2 = Some(group_values_v2);

                // let groups_rows = group_values.iter().take(n);
                // let output = self.row_converter.convert_rows(groups_rows)?;
                // // Clear out first n group keys by copying them to a new Rows.
                // // TODO file some ticket in arrow-rs to make this more efficient?
                // let mut new_group_values = self.row_converter.empty_rows(0, 0);
                // for row in group_values.iter().skip(n) {
                //     new_group_values.push(row);
                // }
                // std::mem::swap(&mut new_group_values, &mut group_values);

                // SAFETY: self.map outlives iterator and is not modified concurrently
                unsafe {
                    for bucket in self.map.iter() {
                        // Decrement group index by n
                        match bucket.as_ref().1.checked_sub(n) {
                            // Group index was >= n, shift value down
                            Some(sub) => bucket.as_mut().1 = sub,
                            // Group index was < n, so remove from table
                            None => self.map.erase(bucket),
                        }
                    }
                }
                output
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

        // self.group_values = Some(group_values);
        Ok(output)
    }

    fn clear_shrink(&mut self, batch: &RecordBatch) {
        let count = batch.num_rows();
        // self.group_values = self.group_values.take().map(|mut rows| {
        //     rows.clear();
        //     rows
        // });
        self.group_values_v2 = self.group_values_v2.take().map(|mut rows| {
            rows.clear();
            rows
        });
        self.map.clear();
        self.map.shrink_to(count, |_| 0); // hasher does not matter since the map is cleared
        self.map_size = self.map.capacity() * std::mem::size_of::<(u64, usize)>();
        self.hashes_buffer.clear();
        self.hashes_buffer.shrink_to(count);
    }
}
