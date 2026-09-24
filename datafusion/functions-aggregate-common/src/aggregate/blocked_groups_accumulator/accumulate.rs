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

//! [`GroupsAccumulator`] helpers: [`BlockedNullState`] and [`accumulate_indices`]
//!
//! [`GroupsAccumulator`]: datafusion_expr_common::groups_accumulator::GroupsAccumulator

use arrow::array::{Array, BooleanArray, BooleanBufferBuilder, PrimitiveArray};
use arrow::buffer::NullBuffer;
use arrow::datatypes::ArrowPrimitiveType;

use crate::aggregate::groups_accumulator::accumulate::accumulate;
use datafusion_common::Result;
use datafusion_expr_common::blocked_groups_accumulator::{
    BlockedEmitTo, BlockedGroupSelection, BlocksIndex,
};
use datafusion_expr_common::blocked_helpers::BlockedBooleanBufferBuilder;

/// If the input has nulls, then the accumulator must potentially
/// handle each input null value specially (e.g. for `SUM` to mark the
/// corresponding sum as null)
///
/// `NullState` tracks if it has seen *any* value for each group when filters or
/// sparse group indices may omit input for a registered group.
#[derive(Debug)]
pub enum BlockedSeenValues {
    /// All groups seen so far have seen at least one non-null value
    All {
        num_values: usize,
        block_size: usize,
    },
    // Some groups have not yet seen a non-null value
    Some {
        values: BlockedBooleanBufferBuilder,
    },
}

impl BlockedSeenValues {
    pub fn new(block_size: usize) -> Self {
        BlockedSeenValues::All {
            num_values: 0,
            block_size,
        }
    }

    pub fn block_size(&self) -> usize {
        match self {
            BlockedSeenValues::All { block_size, .. } => *block_size,
            BlockedSeenValues::Some { values } => values.block_size(),
        }
    }

    /// Return a mutable reference to the `BooleanBufferBuilder` in `BlockedSeenValues::Some`.
    ///
    /// If `self` is `BlockedSeenValues::All`, it is transitioned to `BlockedSeenValues::Some`
    /// by creating a new `BooleanBufferBuilder` where the first `num_values` are true.
    ///
    /// The builder is then ensured to have at least `total_num_groups` length,
    /// with any new entries initialized to false.
    fn get_builder(
        &mut self,
        total_num_groups: usize,
    ) -> &mut BlockedBooleanBufferBuilder {
        match self {
            BlockedSeenValues::All {
                num_values,
                block_size,
            } => {
                let mut builder = BlockedBooleanBufferBuilder::new(*block_size);
                builder.push_value_n(true, *num_values);
                if total_num_groups > *num_values {
                    builder.push_value_n(false, total_num_groups - *num_values);
                }
                *self = BlockedSeenValues::Some { values: builder };
                match self {
                    BlockedSeenValues::Some { values } => values,
                    _ => unreachable!(),
                }
            }
            BlockedSeenValues::Some { values } => {
                if values.len() < total_num_groups {
                    values.push_value_n(false, total_num_groups - values.len());
                }
                values
            }
        }
    }
}

/// Returns true when all newly registered groups are present in `group_indices`.
///
/// Group indices are assigned in first-seen order, so an unfiltered batch visits
/// new groups in ascending order. Pre-filtered input can omit a new group, making
/// the indices sparse even though the accumulator no longer receives a filter.
fn new_groups_are_dense(
    group_indices: &[BlocksIndex],
    block_size: usize,
    first_new_group: usize,
    total_num_groups: usize,
) -> bool {
    let first_new_group =
        BlocksIndex::from_index_in_fixed_block_size(first_new_group, block_size);
    let total_num_groups =
        BlocksIndex::from_index_in_fixed_block_size(total_num_groups, block_size);

    if first_new_group == total_num_groups {
        return true;
    }

    let mut next_new_group = first_new_group;
    for &group_index in group_indices {
        if group_index == next_new_group {
            next_new_group = next_new_group.increment(block_size);
        } else if group_index > next_new_group {
            return false;
        }
    }
    next_new_group == total_num_groups
}

/// Track the accumulator null state per row: if any values for that
/// group were null and if any values have been seen at all for that group.
///
/// This is part of the inner loop for many [`GroupsAccumulator`]s,
/// and thus the performance is critical and so there are multiple
/// specialized implementations, invoked depending on the specific
/// combinations of the input.
///
/// Typically there are 4 potential combinations of inputs must be
/// special cased for performance:
///
/// * With / Without filter
/// * With / Without nulls in the input
///
/// If the input has nulls, then the accumulator must potentially
/// handle each input null value specially (e.g. for `SUM` to mark the
/// corresponding sum as null)
///
/// `NullState` tracks if it has seen *any* value for each group when filters or
/// sparse group indices may omit input for a registered group.
///
/// [`GroupsAccumulator`]: datafusion_expr_common::groups_accumulator::GroupsAccumulator
#[derive(Debug)]
pub struct BlockedNullState {
    /// Have we seen any non-filtered input values for `group_index`?
    ///
    /// If `seen_values` is `SeenValues::Some(buffer)` and buffer\[i\] is true, have seen at least one non null
    /// value for group `i`
    ///
    /// If `seen_values` is `SeenValues::Some(buffer)` and buffer\[i\] is false, have not seen any values that
    /// pass the filter yet for group `i`
    ///
    /// If `seen_values` is `SeenValues::All`, all groups have seen at least one non null value
    seen_values: BlockedSeenValues,
}

impl BlockedNullState {
    pub fn new(block_size: usize) -> Self {
        Self {
            seen_values: BlockedSeenValues::All {
                num_values: 0,
                block_size,
            },
        }
    }

    /// return the size of all buffers allocated by this null state, not including self
    pub fn size(&self) -> usize {
        match &self.seen_values {
            BlockedSeenValues::All { .. } => 0,
            BlockedSeenValues::Some { values } => values.allocated_size(),
        }
    }

    /// Invokes `value_fn(group_index, value)` for each non null, non
    /// filtered value of `value`, while tracking which groups have
    /// seen null inputs and which groups have seen any inputs if necessary
    //
    /// # Arguments:
    ///
    /// * `values`: the input arguments to the accumulator
    /// * `group_indices`:  To which groups do the rows in `values` belong, (aka group_index)
    /// * `opt_filter`: if present, only rows for which is Some(true) are included
    /// * `value_fn`: function invoked for  (group_index, value) where value is non null
    ///
    /// See [`accumulate`], for more details on how value_fn is called
    ///
    /// When value_fn is called it also sets
    ///
    /// 1. `self.seen_values[group_index]` to true for all rows that had a non null value
    pub fn accumulate<T, F>(
        &mut self,
        group_indices: &[BlocksIndex],
        values: &PrimitiveArray<T>,
        opt_filter: Option<&BooleanArray>,
        total_num_groups: usize,
        mut value_fn: F,
    ) where
        T: ArrowPrimitiveType + Send,
        F: FnMut(BlocksIndex, T::Native) + Send,
    {
        // Skip per-value null handling when every input value is valid and all
        // newly registered groups are represented. Pre-filtered inputs can have
        // sparse group indices despite not passing a filter to the accumulator.
        if opt_filter.is_none()
            && values.null_count() == 0
            && let BlockedSeenValues::All {
                num_values,
                block_size,
            } = &mut self.seen_values
            && new_groups_are_dense(
                group_indices,
                *block_size,
                *num_values,
                total_num_groups,
            )
        {
            accumulate(group_indices, values, None, value_fn);
            *num_values = total_num_groups;
            return;
        }

        let seen_values = self.seen_values.get_builder(total_num_groups);
        accumulate(group_indices, values, opt_filter, |group_index, value| {
            seen_values.set_bit(group_index, true);
            value_fn(group_index, value);
        });
    }

    /// Invokes `value_fn(group_index, value)` for each non null, non
    /// filtered value in `values`, while tracking which groups have
    /// seen null inputs and which groups have seen any inputs, for
    /// [`BooleanArray`]s.
    ///
    /// Since `BooleanArray` is not a [`PrimitiveArray`] it must be
    /// handled specially.
    ///
    /// See [`Self::accumulate`], which handles `PrimitiveArray`s, for
    /// more details on other arguments.
    pub fn accumulate_boolean<F>(
        &mut self,
        group_indices: &[BlocksIndex],
        values: &BooleanArray,
        opt_filter: Option<&BooleanArray>,
        total_num_groups: usize,
        mut value_fn: F,
    ) where
        F: FnMut(BlocksIndex, bool) + Send,
    {
        let data = values.values();
        assert_eq!(data.len(), group_indices.len());

        // Skip per-value null handling when every input value is valid and all
        // newly registered groups are represented. Pre-filtered inputs can have
        // sparse group indices despite not passing a filter to the accumulator.
        if opt_filter.is_none()
            && values.null_count() == 0
            && let BlockedSeenValues::All {
                num_values,
                block_size,
            } = &mut self.seen_values
            && new_groups_are_dense(
                group_indices,
                *block_size,
                *num_values,
                total_num_groups,
            )
        {
            group_indices
                .iter()
                .zip(data.iter())
                .for_each(|(&group_index, new_value)| value_fn(group_index, new_value));
            *num_values = total_num_groups;

            return;
        }

        let seen_values = self.seen_values.get_builder(total_num_groups);

        // These could be made more performant by iterating in chunks of 64 bits at a time
        match (values.null_count() > 0, opt_filter) {
            // no nulls, no filter,
            (false, None) => {
                // if we have previously seen nulls, ensure the null
                // buffer is big enough (start everything at valid)
                group_indices.iter().zip(data.iter()).for_each(
                    |(&group_index, new_value)| {
                        seen_values.set_bit(group_index, true);
                        value_fn(group_index, new_value)
                    },
                )
            }
            // nulls, no filter
            (true, None) => {
                let nulls = values.nulls().unwrap();
                group_indices
                    .iter()
                    .zip(data.iter())
                    .zip(nulls.iter())
                    .for_each(|((&group_index, new_value), is_valid)| {
                        if is_valid {
                            seen_values.set_bit(group_index, true);
                            value_fn(group_index, new_value);
                        }
                    })
            }
            // no nulls, but a filter
            (false, Some(filter)) => {
                assert_eq!(filter.len(), group_indices.len());

                group_indices
                    .iter()
                    .zip(data.iter())
                    .zip(filter.iter())
                    .for_each(|((&group_index, new_value), filter_value)| {
                        if filter_value == Some(true) {
                            seen_values.set_bit(group_index, true);
                            value_fn(group_index, new_value);
                        }
                    })
            }
            // both null values and filters
            (true, Some(filter)) => {
                assert_eq!(filter.len(), group_indices.len());
                filter
                    .iter()
                    .zip(group_indices.iter())
                    .zip(values.iter())
                    .for_each(|((filter_value, &group_index), new_value)| {
                        if filter_value == Some(true)
                            && let Some(new_value) = new_value
                        {
                            seen_values.set_bit(group_index, true);
                            value_fn(group_index, new_value)
                        }
                    })
            }
        }
    }

    /// Creates a [`NullBuffer`] for `selection` without changing this state.
    pub fn build_preserving(
        &self,
        selection: BlockedGroupSelection<'_>,
    ) -> Result<Option<NullBuffer>> {
        let selected_len = selection.len();
        match &self.seen_values {
            BlockedSeenValues::All {
                num_values,
                block_size: _,
            } => {
                selection.validate_num_groups(*num_values)?;
                Ok(None)
            }
            BlockedSeenValues::Some { values } => {
                selection.validate_num_groups(values.len())?;
                let mut selected = BooleanBufferBuilder::new(selected_len);
                for index in selection.iter() {
                    selected.append(values.get_bit(index));
                }
                Ok(Some(NullBuffer::new(selected.finish())))
            }
        }
    }

    /// Creates the a [`NullBuffer`] representing which group_indices
    /// should have null values (because they never saw any values)
    /// for the `emit_to` rows.
    ///
    /// resets the internal state appropriately
    pub fn build(&mut self, emit_to: BlockedEmitTo) -> Vec<Option<NullBuffer>> {
        let block_size = self.seen_values.block_size();
        let old_seen =
            std::mem::replace(&mut self.seen_values, BlockedSeenValues::new(block_size));

        match old_seen {
            BlockedSeenValues::All {
                num_values,
                block_size,
            } => match emit_to {
                BlockedEmitTo::All => vec![None; num_values.div_ceil(block_size)],
                BlockedEmitTo::NextBlock => {
                    self.seen_values = BlockedSeenValues::All {
                        num_values: num_values.saturating_sub(block_size),
                        block_size,
                    };

                    if num_values == 0 { vec![] } else { vec![None] }
                }
                BlockedEmitTo::First(n) => {
                    assert!(
                        n < block_size,
                        "emit_to First(n) must be less than block_size"
                    );
                    self.seen_values = BlockedSeenValues::All {
                        num_values: num_values.saturating_sub(n),
                        block_size,
                    };
                    if num_values == 0 { vec![] } else { vec![None] }
                }
            },
            BlockedSeenValues::Some { mut values } => {
                let emitted = values.emit(emit_to);

                self.seen_values = BlockedSeenValues::Some { values };

                emitted
                    .into_iter()
                    .map(|block| Some(NullBuffer::new(block)))
                    .collect()
            }
        }
    }
}
