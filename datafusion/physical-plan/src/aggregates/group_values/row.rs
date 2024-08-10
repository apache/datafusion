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

use std::collections::VecDeque;
use std::mem;

use crate::aggregates::group_values::{GroupBlock, GroupIdx, GroupValues};
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
    map: RawTable<(u64, GroupIdx)>,

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
    group_values_blocks: VecDeque<Rows>,

    /// reused buffer to store hashes
    hashes_buffer: Vec<u64>,

    /// reused buffer to store rows
    rows_buffer: Rows,

    /// Random state for creating hashes
    random_state: RandomState,

    max_block_size: usize,

    cur_block_id: u16,
}

impl GroupValuesRows {
    pub fn try_new(schema: SchemaRef, page_size: usize) -> Result<Self> {
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
            group_values_blocks: VecDeque::new(),
            hashes_buffer: Default::default(),
            rows_buffer,
            random_state: Default::default(),
            max_block_size: page_size,
            cur_block_id: 0,
        })
    }
}

impl GroupValues for GroupValuesRows {
    fn intern(&mut self, cols: &[ArrayRef], groups: &mut Vec<GroupIdx>) -> Result<()> {
        // Convert the group keys into the row format
        let group_rows = &mut self.rows_buffer;
        group_rows.clear();
        self.row_converter.append(group_rows, cols)?;
        let n_rows = group_rows.num_rows();

        if self.group_values_blocks.is_empty() {
            // TODO: calc and use the capacity to init.
            let block = self.row_converter.empty_rows(0, 0);
            self.group_values_blocks.push_back(block);
        };

        let mut group_values_blocks = mem::take(&mut self.group_values_blocks);

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
                let block_id = group_idx.block_id();
                let block_offset = group_idx.block_offset();
                let group_value = group_values_blocks[block_id].row(block_offset);
                group_rows.row(row) == group_value
            });

            let group_idx = match entry {
                // Existing group_index for this group value
                Some((_hash, group_idx)) => *group_idx,
                //  1.2 Need to create new entry for the group
                None => {
                    // Check if the block size has reached the limit, if so we switch to next block.
                    let block_size =  group_values_blocks.back().unwrap().num_rows();
                    if block_size == self.max_block_size {
                        self.cur_block_id += 1;
                        // TODO: calc and use the capacity to init.
                        let block = self.row_converter.empty_rows(0, 0);
                        self.group_values_blocks.push_back(block);
                    }

                    // Add new entry to aggr_state and save newly created index
                    let cur_group_values = self.group_values_blocks.back_mut().unwrap();
                    let block_offset = cur_group_values.num_rows();
                    let group_idx = GroupIdx::new(self.cur_block_id, block_offset as u64);
                    cur_group_values.push(group_rows.row(row));

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

        self.group_values_blocks = group_values_blocks;

        Ok(())
    }

    fn size(&self) -> usize {
        let group_values_size = self.group_values_blocks.as_ref().map(|v| v.size()).unwrap_or(0);
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
        self.group_values_blocks
            .as_ref()
            .map(|group_values| group_values.num_rows())
            .unwrap_or(0)
    }

    fn emit(&mut self, emit_to: EmitTo) -> Result<Vec<Vec<ArrayRef>>> {
        let mut group_values_blocks = mem::take(&mut self
            .group_values_blocks);

        if group_values_blocks.is_empty() {
            return Ok(Vec::new());
        }

        let mut output = match emit_to {
            EmitTo::All => {
                group_values_blocks.iter_mut().map(|rows_block| {
                    let output = self.row_converter.convert_rows(rows_block.iter())?;
                    rows_block.clear();
                    Ok(output)
                }).collect::<Result<Vec<_>>>()?
            }
            EmitTo::First(n) => {
                // convert it to block
                let num_emitted_blocks = if n > self.max_block_size {
                    n / self.max_block_size
                } else {
                    1
                };
                
                let mut emitted_blocks = Vec::with_capacity(num_emitted_blocks);
                for _ in 0..num_emitted_blocks {
                    let block = group_values_blocks.pop_front().unwrap();
                    let converted_block = self.row_converter.convert_rows(block.into_iter())?;
                    emitted_blocks.push(converted_block);
                }

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
                        // Decrement block id by `num_emitted_blocks`
                        let (_, group_idx, ) = bucket.as_ref();
                        let new_block_id = group_idx.block_id().checked_sub(num_emitted_blocks);
                        match new_block_id {
                            // Group index was >= n, shift value down
                            Some(bid) => {
                                let block_offset = group_idx.block_offset();
                                bucket.as_mut().1 = GroupIdx::new(bid as u16, block_offset as u64);
                            },
                            // Group index was < n, so remove from table
                            None =>  self.map.erase(bucket),
                        }
                    }
                }
                emitted_blocks
            }
        };

        // TODO: Materialize dictionaries in group keys (#7647)
        for one_output in output.iter_mut() {
            for (field, array) in self.schema.fields.iter().zip(one_output) {
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
        }

        self.group_values_blocks = group_values_blocks;

        Ok(output)
    }

    fn clear_shrink(&mut self, batch: &RecordBatch) {
        let count = batch.num_rows();
        self.group_values_blocks = self.group_values_blocks.take().map(|mut rows| {
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
