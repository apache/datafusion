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

use std::mem;

use crate::aggregates::group_values::GroupValues;
use ahash::RandomState;
use arrow::compute::cast;
use arrow::record_batch::RecordBatch;
use arrow::row::{RowConverter, Rows, SortField};
use arrow_array::{Array, ArrayRef};
use arrow_schema::{DataType, SchemaRef};
use datafusion_common::hash_utils::create_hashes;
use datafusion_common::{DataFusionError, Result};
use datafusion_execution::memory_pool::proxy::{RawTableAllocExt, VecAllocExt};
use datafusion_expr::EmitTo;
use datafusion_functions_aggregate_common::aggregate::groups_accumulator::{
    BlockedGroupIndex, Blocks,
};
use hashbrown::raw::RawTable;

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
    group_values: Blocks<Rows>,

    /// reused buffer to store hashes
    hashes_buffer: Vec<u64>,

    /// reused buffer to store rows
    rows_buffer: Rows,

    /// Random state for creating hashes
    random_state: RandomState,

    /// Mode about current GroupValuesRows
    block_size: Option<usize>,
}

impl GroupValuesRows {
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
            group_values: Blocks::new(),
            hashes_buffer: Default::default(),
            rows_buffer,
            random_state: Default::default(),
            block_size: None,
        })
    }
}

impl GroupValues for GroupValuesRows {
    fn intern(&mut self, cols: &[ArrayRef], groups: &mut Vec<usize>) -> Result<()> {
        // Convert the group keys into the row format
        let group_rows = &mut self.rows_buffer;
        group_rows.clear();
        self.row_converter.append(group_rows, cols)?;
        let n_rows = group_rows.num_rows();

        let mut group_values = mem::take(&mut self.group_values);
        if group_values.num_blocks() == 0 {
            let block = match self.block_size {
                Some(blk_size) => self.row_converter.empty_rows(blk_size, 0),
                None => self.row_converter.empty_rows(0, 0),
            };

            group_values.push_block(block);
        }

        // tracks to which group each of the input rows belongs
        groups.clear();

        // 1.1 Calculate the group keys for the group values
        let batch_hashes = &mut self.hashes_buffer;
        batch_hashes.clear();
        batch_hashes.resize(n_rows, 0);
        create_hashes(cols, &self.random_state, batch_hashes)?;

        let mut get_or_create_groups =
            |group_index_parse_fn: fn(usize) -> BlockedGroupIndex| {
                for (row, &target_hash) in batch_hashes.iter().enumerate() {
                    let entry =
                        self.map.get_mut(target_hash, |(exist_hash, group_idx)| {
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
                            let blocked_index = group_index_parse_fn(*group_idx);
                            group_rows.row(row)
                                == group_values[blocked_index.block_id]
                                    .row(blocked_index.block_offset)
                        });

                    let group_idx = match entry {
                        // Existing group_index for this group value
                        Some((_hash, group_idx)) => *group_idx,
                        //  1.2 Need to create new entry for the group
                        None => {
                            // Add new entry to aggr_state and save newly created index
                            if let Some(blk_size) = self.block_size {
                                if group_values.current().unwrap().num_rows() == blk_size
                                {
                                    // Use blk_size as offset cap,
                                    // and old block's buffer size as buffer cap
                                    let new_buf_cap =
                                        rows_buffer_size(group_values.current().unwrap());
                                    let new_blk = self
                                        .row_converter
                                        .empty_rows(blk_size, new_buf_cap);
                                    group_values.push_block(new_blk);
                                }
                            }

                            let blk_id = group_values.num_blocks() - 1;
                            let cur_blk = group_values.current_mut().unwrap();
                            let blk_offset = cur_blk.num_rows();
                            cur_blk.push(group_rows.row(row));

                            let blocked_index = BlockedGroupIndex::new_from_parts(
                                blk_id,
                                blk_offset,
                                self.block_size.is_some(),
                            );
                            let group_idx = blocked_index.as_packed_index();

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
            };

        if self.block_size.is_some() {
            get_or_create_groups(BlockedGroupIndex::new_blocked);
        } else {
            get_or_create_groups(BlockedGroupIndex::new_flat);
        };

        self.group_values = group_values;

        Ok(())
    }

    fn size(&self) -> usize {
        // TODO: support size stats in `Blocks`,
        // it is too expansive to calculate it again and again.
        let group_values_size = self
            .group_values
            .iter()
            .map(|rows| rows.size())
            .sum::<usize>();
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
        let num_blocks = self.group_values.num_blocks();
        if num_blocks == 0 {
            return 0;
        }

        let mut group_len = if let Some(blk_size) = self.block_size {
            (num_blocks - 1) * blk_size
        } else {
            0
        };

        group_len += self.group_values.current().unwrap().num_rows();

        group_len
    }

    fn emit(&mut self, emit_to: EmitTo) -> Result<Vec<ArrayRef>> {
        let mut group_values = mem::take(&mut self.group_values);

        let mut output = match emit_to {
            EmitTo::All => {
                debug_assert!(self.block_size.is_none());

                let blk = group_values.pop_first_block().unwrap();
                self.row_converter.convert_rows(blk.into_iter())?
            }
            EmitTo::First(n) => {
                debug_assert!(self.block_size.is_none());

                let blk = group_values.current_mut().unwrap();
                let groups_rows = blk.iter().take(n);
                let output = self.row_converter.convert_rows(groups_rows)?;
                // Clear out first n group keys by copying them to a new Rows.
                // TODO file some ticket in arrow-rs to make this more efficient?
                let mut new_group_values = self.row_converter.empty_rows(0, 0);
                for row in blk.iter().skip(n) {
                    new_group_values.push(row);
                }
                std::mem::swap(&mut new_group_values, blk);

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
            EmitTo::NextBlock(true) => {
                debug_assert!(self.block_size.is_some());

                let cur_blk = group_values.pop_first_block().unwrap();
                let output = self.row_converter.convert_rows(cur_blk.iter())?;

                let mut shift_down_values =
                    |group_index_parse_fn: fn(usize) -> BlockedGroupIndex| unsafe {
                        for bucket in self.map.iter() {
                            // Decrement group index by n
                            let group_idx = bucket.as_ref().1;
                            let old_blk_idx = group_index_parse_fn(group_idx);
                            match old_blk_idx.block_id.checked_sub(1) {
                                // Group index was >= n, shift value down
                                Some(new_blk_id) => {
                                    let new_group_idx = BlockedGroupIndex::new_from_parts(
                                        new_blk_id,
                                        old_blk_idx.block_offset,
                                        true,
                                    );
                                    bucket.as_mut().1 = new_group_idx.as_packed_index();
                                }
                                // Group index was < n, so remove from table
                                None => self.map.erase(bucket),
                            }
                        }
                    };

                if self.block_size.is_some() {
                    shift_down_values(BlockedGroupIndex::new_blocked);
                } else {
                    shift_down_values(BlockedGroupIndex::new_flat);
                };

                output
            }
            EmitTo::NextBlock(false) => {
                let cur_blk = group_values.pop_first_block().unwrap();
                self.row_converter.convert_rows(cur_blk.iter())?
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

        self.group_values = group_values;

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

    fn supports_blocked_mode(&self) -> bool {
        true
    }

    fn alter_block_size(&mut self, block_size: Option<usize>) -> Result<()> {
        self.map.clear();
        self.group_values.clear();
        self.block_size = block_size;

        Ok(())
    }
}

#[inline]
fn rows_buffer_size(rows: &Rows) -> usize {
    let total_size = rows.size();
    let offset_size = (rows.num_rows() + 1) * mem::size_of::<usize>();
    total_size - offset_size - mem::size_of::<Rows>()
}
