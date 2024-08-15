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
use datafusion_expr::groups_accumulator::GroupStatesMode;
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
    group_values: VecDeque<Rows>,

    /// reused buffer to store hashes
    hashes_buffer: Vec<u64>,

    /// reused buffer to store rows
    rows_buffer: Rows,

    /// Random state for creating hashes
    random_state: RandomState,

    /// Mode about current GroupValuesRows
    mode: GroupStatesMode,
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
            group_values: VecDeque::new(),
            hashes_buffer: Default::default(),
            rows_buffer,
            random_state: Default::default(),
            mode: GroupStatesMode::Flat,
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
        if group_values.is_empty() {
            let block = match self.mode {
                GroupStatesMode::Flat => self.row_converter.empty_rows(0, 0),
                GroupStatesMode::Blocked(blk_size) => {
                    self.row_converter.empty_rows(blk_size, 0)
                }
            };

            group_values.push_back(block);
        }

        // tracks to which group each of the input rows belongs
        groups.clear();

        // 1.1 Calculate the group keys for the group values
        let batch_hashes = &mut self.hashes_buffer;
        batch_hashes.clear();
        batch_hashes.resize(n_rows, 0);
        create_hashes(cols, &self.random_state, batch_hashes)?;

        for (row, &target_hash) in batch_hashes.iter().enumerate() {
            let mode = self.mode;
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
                match mode {
                    GroupStatesMode::Flat => {
                        group_rows.row(row)
                            == group_values.back().unwrap().row(*group_idx)
                    }
                    GroupStatesMode::Blocked(_) => {
                        let block_id =
                            ((*group_idx as u64 >> 32) & 0x00000000ffffffff) as usize;
                        let block_offset =
                            ((*group_idx as u64) & 0x00000000ffffffff) as usize;
                        group_rows.row(row) == group_values[block_id].row(block_offset)
                    }
                }
            });

            let group_idx = match entry {
                // Existing group_index for this group value
                Some((_hash, group_idx)) => *group_idx,
                //  1.2 Need to create new entry for the group
                None => {
                    // Add new entry to aggr_state and save newly created index
                    let group_idx = match mode {
                        GroupStatesMode::Flat => {
                            let blk = group_values.back_mut().unwrap();
                            let group_idx = blk.num_rows();
                            blk.push(group_rows.row(row));
                            group_idx
                        }
                        GroupStatesMode::Blocked(blk_size) => {
                            if group_values.back().unwrap().num_rows() == blk_size {
                                // Use blk_size as offset cap,
                                // and old block's buffer size as buffer cap
                                let new_buf_cap =
                                    rows_buffer_size(group_values.back().unwrap());
                                let new_blk =
                                    self.row_converter.empty_rows(blk_size, new_buf_cap);
                                group_values.push_back(new_blk);
                            }

                            let blk_id = (group_values.len() - 1) as u64;
                            let cur_blk = group_values.back_mut().unwrap();
                            let blk_offset = cur_blk.num_rows() as u64;
                            cur_blk.push(group_rows.row(row));

                            (((blk_id << 32) & 0xffffffff00000000)
                                | (blk_offset & 0x00000000ffffffff))
                                as usize
                        }
                    };

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

        self.group_values = group_values;

        Ok(())
    }

    fn size(&self) -> usize {
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
        self.group_values
            .iter()
            .map(|rows| rows.num_rows())
            .sum::<usize>()
    }

    fn emit(&mut self, emit_to: EmitTo) -> Result<Vec<ArrayRef>> {
        let mut group_values = mem::take(&mut self.group_values);

        let mut output = match emit_to {
            EmitTo::All => match self.mode {
                GroupStatesMode::Flat => {
                    let blk = group_values.pop_back().unwrap();
                    let output = self.row_converter.convert_rows(blk.into_iter())?;
                    output
                }
                GroupStatesMode::Blocked(_) => {
                    let mut total_rows_num = 0;
                    let mut total_buffer_size = 0;
                    group_values.iter().for_each(|rows| {
                        let rows_num = rows.num_rows();
                        let rows_buffer_size = rows_buffer_size(rows);
                        total_rows_num += rows_num;
                        total_buffer_size += rows_buffer_size;
                    });

                    let mut total_rows = self
                        .row_converter
                        .empty_rows(total_rows_num, total_buffer_size);
                    for rows in &group_values {
                        for row in rows.into_iter() {
                            total_rows.push(row);
                        }
                    }

                    group_values.clear();

                    let output =
                        self.row_converter.convert_rows(total_rows.into_iter())?;
                    output
                }
            },
            EmitTo::First(n) => {
                if matches!(self.mode, GroupStatesMode::Blocked(_)) {
                    panic!("kamille debug");
                }

                let blk = group_values.back_mut().unwrap();
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
            EmitTo::CurrentBlock(true) => {
                let cur_blk = group_values.pop_front().unwrap();
                let output = self.row_converter.convert_rows(cur_blk.iter())?;
                unsafe {
                    for bucket in self.map.iter() {
                        // Decrement group index by n
                        let group_idx = bucket.as_ref().1 as u64;
                        let blk_id = (group_idx >> 32) & 0x00000000ffffffff;
                        let blk_offset = group_idx & 0x00000000ffffffff;
                        match blk_id.checked_sub(1) {
                            // Group index was >= n, shift value down
                            Some(bid) => {
                                let new_group_idx = (((bid << 32) & 0xffffffff00000000)
                                    | (blk_offset & 0x00000000ffffffff))
                                    as usize;
                                bucket.as_mut().1 = new_group_idx;
                            }
                            // Group index was < n, so remove from table
                            None => self.map.erase(bucket),
                        }
                    }
                }
                output
            }
            EmitTo::CurrentBlock(false) => {
                let cur_blk = group_values.pop_front().unwrap();
                let output = self.row_converter.convert_rows(cur_blk.iter())?;
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

    fn switch_to_mode(&mut self, mode: GroupStatesMode) -> Result<()> {
        self.map.clear();
        self.group_values.clear();
        self.mode = mode;

        Ok(())
    }
}

#[inline]
fn rows_buffer_size(rows: &Rows) -> usize {
    let total_size = rows.size();
    let offset_size = (rows.num_rows() + 1) * mem::size_of::<usize>();
    total_size - offset_size - mem::size_of::<Rows>()
}
