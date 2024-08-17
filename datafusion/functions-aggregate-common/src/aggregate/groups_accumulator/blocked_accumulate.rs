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

//! [`GroupsAccumulator`] helpers: [`NullState`] and [`accumulate_indices`]
//!
//! [`GroupsAccumulator`]: datafusion_expr_common::groups_accumulator::GroupsAccumulator

use std::collections::VecDeque;

use arrow::array::{Array, BooleanArray, BooleanBufferBuilder, PrimitiveArray};
use arrow::buffer::{BooleanBuffer, NullBuffer};
use arrow::datatypes::ArrowPrimitiveType;

use datafusion_common::Result;
use datafusion_expr_common::groups_accumulator::{
    BlockedGroupIndex, EmitTo, GroupStatesMode,
};

/// Similar as the [NullState] but supported the blocked version accumulator
#[derive(Debug)]
pub struct BlockedNullState {
    /// Have we seen any non-filtered input values for `group_index`?
    ///
    /// If `seen_values[i]` is true, have seen at least one non null
    /// value for group `i`
    ///
    /// If `seen_values[i]` is false, have not seen any values that
    /// pass the filter yet for group `i`
    seen_values_blocks: VecDeque<BooleanBufferBuilder>,

    mode: GroupStatesMode,
}

impl Default for BlockedNullState {
    fn default() -> Self {
        Self::new(GroupStatesMode::Flat)
    }
}

impl BlockedNullState {
    pub fn new(mode: GroupStatesMode) -> Self {
        Self {
            seen_values_blocks: VecDeque::new(),
            mode,
        }
    }

    /// return the size of all buffers allocated by this null state, not including self
    pub fn size(&self) -> usize {
        // capacity is in bits, so convert to bytes
        self.seen_values_blocks
            .iter()
            .map(|blk| blk.capacity() / 8)
            .sum::<usize>()
    }

    /// Similar as [NullState::accumulate]  but supported the blocked version accumulator
    pub fn accumulate<T, F>(
        &mut self,
        group_indices: &[usize],
        values: &PrimitiveArray<T>,
        opt_filter: Option<&BooleanArray>,
        total_num_groups: usize,
        mut value_fn: F,
    ) where
        T: ArrowPrimitiveType + Send,
        F: FnMut(usize, T::Native) + Send,
    {
        let data: &[T::Native] = values.values();
        assert_eq!(data.len(), group_indices.len());

        // ensure the seen_values is big enough (start everything at
        // "not seen" valid)
        ensure_enough_room_for_nulls(
            &mut self.seen_values_blocks,
            self.mode,
            total_num_groups,
            false,
        );
        let seen_values_blocks = &mut self.seen_values_blocks;

        match (values.null_count() > 0, opt_filter) {
            // no nulls, no filter,
            (false, None) => {
                let iter = group_indices.iter().zip(data.iter());
                for (&group_index, &new_value) in iter {
                    match self.mode {
                        GroupStatesMode::Flat => seen_values_blocks
                            .back_mut()
                            .unwrap()
                            .set_bit(group_index, true),
                        GroupStatesMode::Blocked(_) => {
                            let blocked_index = BlockedGroupIndex::new(group_index);
                            // dbg!(seen_values_blocks.len());
                            // dbg!(blocked_index.block_id);
                            // dbg!(blk_size);
                            seen_values_blocks[blocked_index.block_id]
                                .set_bit(blocked_index.block_offset, true);
                        }
                    }
                    value_fn(group_index, new_value);
                }
            }
            // nulls, no filter
            (true, None) => {
                let nulls = values.nulls().unwrap();
                // This is based on (ahem, COPY/PASTE) arrow::compute::aggregate::sum
                // iterate over in chunks of 64 bits for more efficient null checking
                let group_indices_chunks = group_indices.chunks_exact(64);
                let data_chunks = data.chunks_exact(64);
                let bit_chunks = nulls.inner().bit_chunks();

                let group_indices_remainder = group_indices_chunks.remainder();
                let data_remainder = data_chunks.remainder();

                group_indices_chunks
                    .zip(data_chunks)
                    .zip(bit_chunks.iter())
                    .for_each(|((group_index_chunk, data_chunk), mask)| {
                        // index_mask has value 1 << i in the loop
                        let mut index_mask = 1;
                        group_index_chunk.iter().zip(data_chunk.iter()).for_each(
                            |(&group_index, &new_value)| {
                                // valid bit was set, real value
                                let is_valid = (mask & index_mask) != 0;
                                if is_valid {
                                    match self.mode {
                                        GroupStatesMode::Flat => seen_values_blocks
                                            .back_mut()
                                            .unwrap()
                                            .set_bit(group_index, true),
                                        GroupStatesMode::Blocked(_) => {
                                            let blocked_index =
                                                BlockedGroupIndex::new(group_index);
                                            seen_values_blocks[blocked_index.block_id]
                                                .set_bit(
                                                    blocked_index.block_offset,
                                                    true,
                                                );
                                        }
                                    }
                                    value_fn(group_index, new_value);
                                }
                                index_mask <<= 1;
                            },
                        )
                    });

                // handle any remaining bits (after the initial 64)
                let remainder_bits = bit_chunks.remainder_bits();
                group_indices_remainder
                    .iter()
                    .zip(data_remainder.iter())
                    .enumerate()
                    .for_each(|(i, (&group_index, &new_value))| {
                        let is_valid = remainder_bits & (1 << i) != 0;
                        if is_valid {
                            match self.mode {
                                GroupStatesMode::Flat => seen_values_blocks
                                    .back_mut()
                                    .unwrap()
                                    .set_bit(group_index, true),
                                GroupStatesMode::Blocked(_) => {
                                    let blocked_index =
                                        BlockedGroupIndex::new(group_index);
                                    seen_values_blocks[blocked_index.block_id]
                                        .set_bit(blocked_index.block_offset, true);
                                }
                            }
                            value_fn(group_index, new_value);
                        }
                    });
            }
            // no nulls, but a filter
            (false, Some(filter)) => {
                assert_eq!(filter.len(), group_indices.len());
                // The performance with a filter could be improved by
                // iterating over the filter in chunks, rather than a single
                // iterator. TODO file a ticket
                group_indices
                    .iter()
                    .zip(data.iter())
                    .zip(filter.iter())
                    .for_each(|((&group_index, &new_value), filter_value)| {
                        if let Some(true) = filter_value {
                            match self.mode {
                                GroupStatesMode::Flat => seen_values_blocks
                                    .back_mut()
                                    .unwrap()
                                    .set_bit(group_index, true),
                                GroupStatesMode::Blocked(_) => {
                                    let blocked_index =
                                        BlockedGroupIndex::new(group_index);
                                    seen_values_blocks[blocked_index.block_id]
                                        .set_bit(blocked_index.block_offset, true);
                                }
                            }
                            value_fn(group_index, new_value);
                        }
                    })
            }
            // both null values and filters
            (true, Some(filter)) => {
                assert_eq!(filter.len(), group_indices.len());
                // The performance with a filter could be improved by
                // iterating over the filter in chunks, rather than using
                // iterators. TODO file a ticket
                filter
                    .iter()
                    .zip(group_indices.iter())
                    .zip(values.iter())
                    .for_each(|((filter_value, &group_index), new_value)| {
                        if let Some(true) = filter_value {
                            if let Some(new_value) = new_value {
                                match self.mode {
                                    GroupStatesMode::Flat => seen_values_blocks
                                        .back_mut()
                                        .unwrap()
                                        .set_bit(group_index, true),
                                    GroupStatesMode::Blocked(_) => {
                                        let blocked_index =
                                            BlockedGroupIndex::new(group_index);
                                        seen_values_blocks[blocked_index.block_id]
                                            .set_bit(blocked_index.block_offset, true);
                                    }
                                }
                                value_fn(group_index, new_value)
                            }
                        }
                    })
            }
        }
    }

    /// Similar as [NullState::build] but support the blocked version accumulator
    pub fn build(&mut self, emit_to: EmitTo) -> Result<NullBuffer> {
        if self.seen_values_blocks.is_empty() {
            return Ok(NullBuffer::new(BooleanBufferBuilder::new(0).finish()));
        }

        let nulls = match emit_to {
            EmitTo::All => match self.mode {
                GroupStatesMode::Flat => {
                    self.seen_values_blocks.back_mut().unwrap().finish()
                }
                GroupStatesMode::Blocked(blk_size) => {
                    let total_num = (self.seen_values_blocks.len() - 1) * blk_size
                        + self.seen_values_blocks.back().unwrap().len();
                    let mut total_buffer = BooleanBufferBuilder::new(total_num);

                    for blk in self.seen_values_blocks.iter_mut() {
                        let nulls = blk.finish();
                        for seen in nulls.iter() {
                            total_buffer.append(seen);
                        }
                    }

                    total_buffer.finish()
                }
            },
            EmitTo::First(n) => {
                let blk = self.seen_values_blocks.back_mut().unwrap();
                // split off the first N values in seen_values
                //
                // TODO make this more efficient rather than two
                // copies and bitwise manipulation
                let nulls = blk.finish();
                let first_n_null: BooleanBuffer = nulls.iter().take(n).collect();
                // reset the existing seen buffer
                for seen in nulls.iter().skip(n) {
                    blk.append(seen);
                }
                first_n_null
            }
            EmitTo::CurrentBlock(_) => {
                let mut cur_blk = self.seen_values_blocks.pop_front().unwrap();
                cur_blk.finish()
            }
        };

        Ok(NullBuffer::new(nulls))
    }
}

/// This function is called to update the accumulator state per row
/// when the value is not needed (e.g. COUNT)
///
/// `F`: Invoked like `value_fn(group_index) for all non null values
/// passing the filter. Note that no tracking is done for null inputs
/// or which groups have seen any values
///
/// See [`NullState::accumulate`], for more details on other
/// arguments.
pub fn accumulate_indices<F>(
    group_indices: &[usize],
    nulls: Option<&NullBuffer>,
    opt_filter: Option<&BooleanArray>,
    mut index_fn: F,
) where
    F: FnMut(usize) + Send,
{
    match (nulls, opt_filter) {
        (None, None) => {
            for &group_index in group_indices.iter() {
                index_fn(group_index)
            }
        }
        (None, Some(filter)) => {
            assert_eq!(filter.len(), group_indices.len());
            // The performance with a filter could be improved by
            // iterating over the filter in chunks, rather than a single
            // iterator. TODO file a ticket
            let iter = group_indices.iter().zip(filter.iter());
            for (&group_index, filter_value) in iter {
                if let Some(true) = filter_value {
                    index_fn(group_index)
                }
            }
        }
        (Some(valids), None) => {
            assert_eq!(valids.len(), group_indices.len());
            // This is based on (ahem, COPY/PASTA) arrow::compute::aggregate::sum
            // iterate over in chunks of 64 bits for more efficient null checking
            let group_indices_chunks = group_indices.chunks_exact(64);
            let bit_chunks = valids.inner().bit_chunks();

            let group_indices_remainder = group_indices_chunks.remainder();

            group_indices_chunks.zip(bit_chunks.iter()).for_each(
                |(group_index_chunk, mask)| {
                    // index_mask has value 1 << i in the loop
                    let mut index_mask = 1;
                    group_index_chunk.iter().for_each(|&group_index| {
                        // valid bit was set, real vale
                        let is_valid = (mask & index_mask) != 0;
                        if is_valid {
                            index_fn(group_index);
                        }
                        index_mask <<= 1;
                    })
                },
            );

            // handle any remaining bits (after the initial 64)
            let remainder_bits = bit_chunks.remainder_bits();
            group_indices_remainder
                .iter()
                .enumerate()
                .for_each(|(i, &group_index)| {
                    let is_valid = remainder_bits & (1 << i) != 0;
                    if is_valid {
                        index_fn(group_index)
                    }
                });
        }

        (Some(valids), Some(filter)) => {
            assert_eq!(filter.len(), group_indices.len());
            assert_eq!(valids.len(), group_indices.len());
            // The performance with a filter could likely be improved by
            // iterating over the filter in chunks, rather than using
            // iterators. TODO file a ticket
            filter
                .iter()
                .zip(group_indices.iter())
                .zip(valids.iter())
                .for_each(|((filter_value, &group_index), is_valid)| {
                    if let (Some(true), true) = (filter_value, is_valid) {
                        index_fn(group_index)
                    }
                })
        }
    }
}

/// Ensures that `builder` contains a `BooleanBufferBuilder with at
/// least `total_num_groups`.
///
/// All new entries are initialized to `default_value`
fn ensure_enough_room_for_nulls(
    builder_blocks: &mut VecDeque<BooleanBufferBuilder>,
    mode: GroupStatesMode,
    total_num_groups: usize,
    default_value: bool,
) {
    match mode {
        // It flat mode, we just a single builder, and grow it constantly.
        GroupStatesMode::Flat => {
            if builder_blocks.is_empty() {
                builder_blocks.push_back(BooleanBufferBuilder::new(0));
            }

            let builder = builder_blocks.back_mut().unwrap();
            if builder.len() < total_num_groups {
                let new_groups = total_num_groups - builder.len();
                builder.append_n(new_groups, default_value);
            }
        }
        // It blocked mode, we ensure the blks are enough first,
        // and then ensure slots in blks are enough.
        GroupStatesMode::Blocked(blk_size) => {
            let (mut cur_blk_idx, exist_slots) = if !builder_blocks.is_empty() {
                let cur_blk_idx = builder_blocks.len() - 1;
                let exist_slots = (builder_blocks.len() - 1) * blk_size
                    + builder_blocks.back().unwrap().len();

                (cur_blk_idx, exist_slots)
            } else {
                (0, 0)
            };
            let exist_blks = builder_blocks.len();

            // Ensure blks are enough.
            let new_blks = (total_num_groups + blk_size - 1) / blk_size - exist_blks;
            builder_blocks.reserve(new_blks);
            for _ in 0..new_blks {
                builder_blocks.push_back(BooleanBufferBuilder::new(blk_size));
            }

            // Ensure slots are enough.
            let mut new_slots = total_num_groups - exist_slots;
            // Expand current blk.
            let cur_blk_rest_slots = blk_size - builder_blocks[cur_blk_idx].len();
            if cur_blk_rest_slots >= new_slots {
                builder_blocks[cur_blk_idx].append_n(new_slots, default_value);
                return;
            } else {
                builder_blocks[cur_blk_idx].append_n(cur_blk_rest_slots, default_value);
                new_slots -= cur_blk_rest_slots;
                cur_blk_idx += 1;
            }

            // Expand blks
            let expand_blks = new_slots / blk_size;
            for _ in 0..expand_blks {
                builder_blocks[cur_blk_idx].append_n(blk_size, default_value);
                cur_blk_idx += 1;
            }

            // Expand the last blk.
            let last_expand_slots = new_slots % blk_size;
            builder_blocks
                .back_mut()
                .unwrap()
                .append_n(last_expand_slots, default_value);
        }
    }
}

#[cfg(test)]
mod test {
    // use super::*;

    // use arrow::array::UInt32Array;
    // use rand::{rngs::ThreadRng, Rng};
    // use std::collections::HashSet;

    // #[test]
    // fn accumulate() {
    //     let group_indices = (0..100).collect();
    //     let values = (0..100).map(|i| (i + 1) * 10).collect();
    //     let values_with_nulls = (0..100)
    //         .map(|i| if i % 3 == 0 { None } else { Some((i + 1) * 10) })
    //         .collect();

    //     // default to every fifth value being false, every even
    //     // being null
    //     let filter: BooleanArray = (0..100)
    //         .map(|i| {
    //             let is_even = i % 2 == 0;
    //             let is_fifth = i % 5 == 0;
    //             if is_even {
    //                 None
    //             } else if is_fifth {
    //                 Some(false)
    //             } else {
    //                 Some(true)
    //             }
    //         })
    //         .collect();

    //     Fixture {
    //         group_indices,
    //         values,
    //         values_with_nulls,
    //         filter,
    //     }
    //     .run()
    // }

    // #[test]
    // fn accumulate_fuzz() {
    //     let mut rng = rand::thread_rng();
    //     for _ in 0..100 {
    //         Fixture::new_random(&mut rng).run();
    //     }
    // }

    // /// Values for testing (there are enough values to exercise the 64 bit chunks
    // struct Fixture {
    //     /// 100..0
    //     group_indices: Vec<usize>,

    //     /// 10, 20, ... 1010
    //     values: Vec<u32>,

    //     /// same as values, but every third is null:
    //     /// None, Some(20), Some(30), None ...
    //     values_with_nulls: Vec<Option<u32>>,

    //     /// filter (defaults to None)
    //     filter: BooleanArray,
    // }

    // impl Fixture {
    //     fn new_random(rng: &mut ThreadRng) -> Self {
    //         // Number of input values in a batch
    //         let num_values: usize = rng.gen_range(1..200);
    //         // number of distinct groups
    //         let num_groups: usize = rng.gen_range(2..1000);
    //         let max_group = num_groups - 1;

    //         let group_indices: Vec<usize> = (0..num_values)
    //             .map(|_| rng.gen_range(0..max_group))
    //             .collect();

    //         let values: Vec<u32> = (0..num_values).map(|_| rng.gen()).collect();

    //         // 10% chance of false
    //         // 10% change of null
    //         // 80% chance of true
    //         let filter: BooleanArray = (0..num_values)
    //             .map(|_| {
    //                 let filter_value = rng.gen_range(0.0..1.0);
    //                 if filter_value < 0.1 {
    //                     Some(false)
    //                 } else if filter_value < 0.2 {
    //                     None
    //                 } else {
    //                     Some(true)
    //                 }
    //             })
    //             .collect();

    //         // random values with random number and location of nulls
    //         // random null percentage
    //         let null_pct: f32 = rng.gen_range(0.0..1.0);
    //         let values_with_nulls: Vec<Option<u32>> = (0..num_values)
    //             .map(|_| {
    //                 let is_null = null_pct < rng.gen_range(0.0..1.0);
    //                 if is_null {
    //                     None
    //                 } else {
    //                     Some(rng.gen())
    //                 }
    //             })
    //             .collect();

    //         Self {
    //             group_indices,
    //             values,
    //             values_with_nulls,
    //             filter,
    //         }
    //     }

    //     /// returns `Self::values` an Array
    //     fn values_array(&self) -> UInt32Array {
    //         UInt32Array::from(self.values.clone())
    //     }

    //     /// returns `Self::values_with_nulls` as an Array
    //     fn values_with_nulls_array(&self) -> UInt32Array {
    //         UInt32Array::from(self.values_with_nulls.clone())
    //     }

    //     /// Calls `NullState::accumulate` and `accumulate_indices`
    //     /// with all combinations of nulls and filter values
    //     fn run(&self) {
    //         let total_num_groups = *self.group_indices.iter().max().unwrap() + 1;

    //         let group_indices = &self.group_indices;
    //         let values_array = self.values_array();
    //         let values_with_nulls_array = self.values_with_nulls_array();
    //         let filter = &self.filter;

    //         // no null, no filters
    //         Self::accumulate_test(group_indices, &values_array, None, total_num_groups);

    //         // nulls, no filters
    //         Self::accumulate_test(
    //             group_indices,
    //             &values_with_nulls_array,
    //             None,
    //             total_num_groups,
    //         );

    //         // no nulls, filters
    //         Self::accumulate_test(
    //             group_indices,
    //             &values_array,
    //             Some(filter),
    //             total_num_groups,
    //         );

    //         // nulls, filters
    //         Self::accumulate_test(
    //             group_indices,
    //             &values_with_nulls_array,
    //             Some(filter),
    //             total_num_groups,
    //         );
    //     }

    //     /// Calls `NullState::accumulate` and `accumulate_indices` to
    //     /// ensure it generates the correct values.
    //     ///
    //     fn accumulate_test(
    //         group_indices: &[usize],
    //         values: &UInt32Array,
    //         opt_filter: Option<&BooleanArray>,
    //         total_num_groups: usize,
    //     ) {
    //         Self::accumulate_values_test(
    //             group_indices,
    //             values,
    //             opt_filter,
    //             total_num_groups,
    //         );
    //         Self::accumulate_indices_test(group_indices, values.nulls(), opt_filter);

    //         // Convert values into a boolean array (anything above the
    //         // average is true, otherwise false)
    //         let avg: usize = values.iter().filter_map(|v| v.map(|v| v as usize)).sum();
    //         let boolean_values: BooleanArray =
    //             values.iter().map(|v| v.map(|v| v as usize > avg)).collect();
    //         Self::accumulate_boolean_test(
    //             group_indices,
    //             &boolean_values,
    //             opt_filter,
    //             total_num_groups,
    //         );
    //     }

    //     /// This is effectively a different implementation of
    //     /// accumulate that we compare with the above implementation
    //     fn accumulate_values_test(
    //         group_indices: &[usize],
    //         values: &UInt32Array,
    //         opt_filter: Option<&BooleanArray>,
    //         total_num_groups: usize,
    //     ) {
    //         let mut accumulated_values = vec![];
    //         let mut null_state = BlockedNullState::new(GroupStatesMode::Flat);

    //         null_state.accumulate(
    //             group_indices,
    //             values,
    //             opt_filter,
    //             total_num_groups,
    //             |group_index, value| {
    //                 accumulated_values.push((group_index, value));
    //             },
    //         );

    //         // Figure out the expected values
    //         let mut expected_values = vec![];
    //         let mut mock = MockNullState::new();

    //         match opt_filter {
    //             None => group_indices.iter().zip(values.iter()).for_each(
    //                 |(&group_index, value)| {
    //                     if let Some(value) = value {
    //                         mock.saw_value(group_index);
    //                         expected_values.push((group_index, value));
    //                     }
    //                 },
    //             ),
    //             Some(filter) => {
    //                 group_indices
    //                     .iter()
    //                     .zip(values.iter())
    //                     .zip(filter.iter())
    //                     .for_each(|((&group_index, value), is_included)| {
    //                         // if value passed filter
    //                         if let Some(true) = is_included {
    //                             if let Some(value) = value {
    //                                 mock.saw_value(group_index);
    //                                 expected_values.push((group_index, value));
    //                             }
    //                         }
    //                     });
    //             }
    //         }

    //         assert_eq!(accumulated_values, expected_values,
    //                    "\n\naccumulated_values:{accumulated_values:#?}\n\nexpected_values:{expected_values:#?}");
    //         let seen_values = null_state.seen_values.finish_cloned();
    //         mock.validate_seen_values(&seen_values);

    //         // Validate the final buffer (one value per group)
    //         let expected_null_buffer = mock.expected_null_buffer(total_num_groups);

    //         let null_buffer = null_state.build(EmitTo::All).unwrap();

    //         assert_eq!(null_buffer, expected_null_buffer);
    //     }

    //     // Calls `accumulate_indices`
    //     // and opt_filter and ensures it calls the right values
    //     fn accumulate_indices_test(
    //         group_indices: &[usize],
    //         nulls: Option<&NullBuffer>,
    //         opt_filter: Option<&BooleanArray>,
    //     ) {
    //         let mut accumulated_values = vec![];

    //         accumulate_indices(group_indices, nulls, opt_filter, |group_index| {
    //             accumulated_values.push(group_index);
    //         });

    //         // Figure out the expected values
    //         let mut expected_values = vec![];

    //         match (nulls, opt_filter) {
    //             (None, None) => group_indices.iter().for_each(|&group_index| {
    //                 expected_values.push(group_index);
    //             }),
    //             (Some(nulls), None) => group_indices.iter().zip(nulls.iter()).for_each(
    //                 |(&group_index, is_valid)| {
    //                     if is_valid {
    //                         expected_values.push(group_index);
    //                     }
    //                 },
    //             ),
    //             (None, Some(filter)) => group_indices.iter().zip(filter.iter()).for_each(
    //                 |(&group_index, is_included)| {
    //                     if let Some(true) = is_included {
    //                         expected_values.push(group_index);
    //                     }
    //                 },
    //             ),
    //             (Some(nulls), Some(filter)) => {
    //                 group_indices
    //                     .iter()
    //                     .zip(nulls.iter())
    //                     .zip(filter.iter())
    //                     .for_each(|((&group_index, is_valid), is_included)| {
    //                         // if value passed filter
    //                         if let (true, Some(true)) = (is_valid, is_included) {
    //                             expected_values.push(group_index);
    //                         }
    //                     });
    //             }
    //         }

    //         assert_eq!(accumulated_values, expected_values,
    //                    "\n\naccumulated_values:{accumulated_values:#?}\n\nexpected_values:{expected_values:#?}");
    //     }
    // }

    // /// Parallel implementation of NullState to check expected values
    // #[derive(Debug, Default)]
    // struct MockNullState {
    //     /// group indices that had values that passed the filter
    //     seen_values: HashSet<usize>,
    // }

    // impl MockNullState {
    //     fn new() -> Self {
    //         Default::default()
    //     }

    //     fn saw_value(&mut self, group_index: usize) {
    //         self.seen_values.insert(group_index);
    //     }

    //     /// did this group index see any input?
    //     fn expected_seen(&self, group_index: usize) -> bool {
    //         self.seen_values.contains(&group_index)
    //     }

    //     /// Validate that the seen_values matches self.seen_values
    //     fn validate_seen_values(&self, seen_values: &BooleanBuffer) {
    //         for (group_index, is_seen) in seen_values.iter().enumerate() {
    //             let expected_seen = self.expected_seen(group_index);
    //             assert_eq!(
    //                 expected_seen, is_seen,
    //                 "mismatch at for group {group_index}"
    //             );
    //         }
    //     }

    //     /// Create the expected null buffer based on if the input had nulls and a filter
    //     fn expected_null_buffer(&self, total_num_groups: usize) -> NullBuffer {
    //         (0..total_num_groups)
    //             .map(|group_index| self.expected_seen(group_index))
    //             .collect()
    //     }
    // }
}
