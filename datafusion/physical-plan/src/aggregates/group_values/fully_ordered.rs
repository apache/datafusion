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

use crate::aggregates::group_values::GroupValues;
use crate::common::transpose;
use ahash::RandomState;
use arrow::compute::{cast, SortColumn};
use arrow::record_batch::RecordBatch;
use arrow::row::{RowConverter, Rows, SortField};
use arrow_array::{Array, ArrayRef};
use arrow_schema::{DataType, SchemaRef, SortOptions};
use datafusion_common::hash_utils::create_hashes;
use datafusion_common::utils::{evaluate_partition_ranges, get_row_at_idx};
use datafusion_common::{DataFusionError, Result, ScalarValue};
use datafusion_execution::memory_pool::proxy::{RawTableAllocExt, VecAllocExt};
use datafusion_expr::EmitTo;
use datafusion_physical_expr::LexOrdering;
use hashbrown::raw::RawTable;

/// A [`GroupValues`] making use of [`Rows`]
pub struct FullOrderedGroupValues {
    /// The output schema
    schema: SchemaRef,

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
    group_values: Vec<Vec<ScalarValue>>,

    // buffer to be reused to store hashes
    hashes_buffer: Vec<u64>,

    /// Random state for creating hashes
    random_state: RandomState,
    sort_exprs: LexOrdering,
}

impl FullOrderedGroupValues {
    pub fn try_new(schema: SchemaRef, sort_exprs: LexOrdering) -> Result<Self> {
        let map = RawTable::with_capacity(0);

        Ok(Self {
            schema,
            map,
            map_size: 0,
            group_values: vec![],
            hashes_buffer: Default::default(),
            random_state: Default::default(),
            sort_exprs,
        })
    }
}

impl GroupValues for FullOrderedGroupValues {
    fn intern(&mut self, cols: &[ArrayRef], groups: &mut Vec<usize>) -> Result<()> {
        // Convert the group keys into the row format
        // Avoid reallocation when https://github.com/apache/arrow-rs/issues/4479 is available
        assert!(cols.len() > 0);
        let n_rows = cols[0].len();
        if n_rows == 0 {
            return Ok(());
        }

        // tracks to which group each of the input rows belongs
        groups.clear();

        assert_eq!(cols.len(), self.sort_exprs.len());
        // TODO: May need to change iteration order
        let sort_columns = cols
            .iter()
            .zip(&self.sort_exprs)
            .map(|(col, sort_expr)| SortColumn {
                values: col.clone(),
                options: Some(sort_expr.options),
            })
            .collect::<Vec<_>>();
        let ranges = evaluate_partition_ranges(n_rows, &sort_columns)?;
        assert!(!ranges.is_empty());
        let first_section = &ranges[0];
        let row = get_row_at_idx(cols, first_section.start)?;
        let mut should_insert = false;
        if let Some(last_group) = self.group_values.last() {
            if !row.eq(last_group) {
                // Group by value changed
                should_insert = true;
            }
        } else {
            should_insert = true;
        }
        if should_insert {
            self.group_values.push(row);
        }
        groups.extend(vec![
            self.group_values.len() - 1;
            first_section.end - first_section.start
        ]);
        for range in ranges[1..].iter() {
            let row = get_row_at_idx(cols, range.start)?;
            self.group_values.push(row);
            groups.extend(vec![self.group_values.len() - 1; range.end - range.start]);
        }

        Ok(())
    }

    fn size(&self) -> usize {
        let group_values_size: usize = self
            .group_values
            .iter()
            .map(|row| ScalarValue::size_of_vec(row))
            .sum();
        group_values_size + self.map_size + self.hashes_buffer.allocated_size()
    }

    fn is_empty(&self) -> bool {
        self.len() == 0
    }

    fn len(&self) -> usize {
        self.group_values.len()
    }

    fn emit(&mut self, emit_to: EmitTo) -> Result<Vec<ArrayRef>> {
        // println!("self group values: {:?}", self.group_values);
        // println!("emit to :{:?}", emit_to);
        let mut output: Vec<ArrayRef> = match emit_to {
            EmitTo::All => {
                let res = transpose(self.group_values.clone());
                self.group_values.clear();
                res.into_iter()
                    .map(|items| ScalarValue::iter_to_array(items))
                    .collect::<Result<Vec<_>>>()?
            }
            EmitTo::First(n) => {
                let first_n_section = self.group_values[0..n].to_vec();
                self.group_values = self.group_values[n..].to_vec();
                let res = transpose(first_n_section);
                res.into_iter()
                    .map(|items| ScalarValue::iter_to_array(items))
                    .collect::<Result<Vec<_>>>()?
            }
        };
        // println!("output: {:?}", output);

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

        Ok(output)
    }

    fn clear_shrink(&mut self, batch: &RecordBatch) {
        let count = batch.num_rows();
        self.group_values.clear();
        self.map.clear();
        self.map.shrink_to(count, |_| 0); // hasher does not matter since the map is cleared
        self.map_size = self.map.capacity() * std::mem::size_of::<(u64, usize)>();
        self.hashes_buffer.clear();
        self.hashes_buffer.shrink_to(count);
    }
}
