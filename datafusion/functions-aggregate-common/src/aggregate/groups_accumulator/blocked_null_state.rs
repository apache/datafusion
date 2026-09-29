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

//! [`BlockedNullState`]: [`NullState`] for blocked group indices.
//!
//! [`NullState`]: super::accumulate::NullState

use arrow::array::{
    Array, ArrowPrimitiveType, BooleanArray, BooleanBufferBuilder, PrimitiveArray,
};
use arrow::buffer::{BooleanBuffer, NullBuffer};
use datafusion_expr_common::blocked_groups_accumulator::{BlockedEmitTo, BlocksIndex};

use super::blocked_vec::{BlockedVec, SeenGroups};

/// Tracks which groups have seen at least one non null, non filtered value,
/// like [`NullState`], for state stored in a [`BlockedVec`].
///
/// The seen bits are stored in one bitmap per block, with the same block
/// boundaries as the [`BlockedVec`], so each emitted block gets its own
/// [`NullBuffer`].
///
/// [`NullState`]: super::accumulate::NullState
#[derive(Debug)]
pub struct BlockedNullState {
    block_size: usize,
    seen: Seen,
}

#[derive(Debug)]
enum Seen {
    /// Every group seen so far has seen a value, so no bitmap is needed
    All { num_groups: usize },
    /// Every bitmap but the last holds exactly `block_size` bits
    Some { blocks: Vec<BooleanBufferBuilder> },
}

impl BlockedNullState {
    pub fn new(block_size: usize) -> Self {
        Self {
            block_size,
            seen: Seen::All { num_groups: 0 },
        }
    }

    /// Size of all buffers allocated by this null state, not including self.
    pub fn size(&self) -> usize {
        match &self.seen {
            Seen::All { .. } => 0,
            Seen::Some { blocks } => blocks.iter().map(|b| b.capacity() / 8).sum(),
        }
    }

    /// Updates `state` with every non null, non filtered value of `values`
    /// through [`BlockedVec::update_values`], and marks the groups of those
    /// values as seen.
    ///
    /// Like [`NullState::accumulate`], no bits are tracked while every value
    /// is valid and every new group is present in `group_indices`.
    ///
    /// [`NullState::accumulate`]: super::accumulate::NullState::accumulate
    #[expect(clippy::too_many_arguments, reason = "mirrors `NullState::accumulate`")]
    pub fn accumulate<T, V, F>(
        &mut self,
        state: &mut BlockedVec<T>,
        total_num_groups: usize,
        starting_value: T,
        group_indices: &[BlocksIndex],
        values: &PrimitiveArray<V>,
        opt_filter: Option<&BooleanArray>,
        update_fn: F,
    ) where
        T: Copy + Default,
        V: ArrowPrimitiveType,
        F: FnMut(&mut T, V::Native),
    {
        if opt_filter.is_none()
            && values.null_count() == 0
            && let Seen::All { num_groups } = &mut self.seen
            && new_groups_are_dense(
                group_indices,
                *num_groups,
                total_num_groups,
                self.block_size,
            )
        {
            state.update_values(
                total_num_groups,
                starting_value,
                group_indices,
                values,
                None,
                &mut (),
                update_fn,
            );
            *num_groups = total_num_groups;
            return;
        }

        let mut seen = SeenBits {
            blocks: self.bitmaps(total_num_groups),
        };
        state.update_values(
            total_num_groups,
            starting_value,
            group_indices,
            values,
            opt_filter,
            &mut seen,
            update_fn,
        );
    }

    /// Returns the bitmaps, grown to `total_num_groups` bits (new groups have
    /// not seen a value), switching from [`Seen::All`] if needed.
    fn bitmaps(&mut self, total_num_groups: usize) -> &mut Vec<BooleanBufferBuilder> {
        if let Seen::All { num_groups } = self.seen {
            let mut blocks = vec![];
            append_bits(&mut blocks, self.block_size, num_groups, true);
            self.seen = Seen::Some { blocks };
        }
        let Seen::Some { blocks } = &mut self.seen else {
            unreachable!("switched to `Seen::Some` above")
        };
        let len = bits_len(blocks);
        if total_num_groups > len {
            append_bits(blocks, self.block_size, total_num_groups - len, false);
        }
        blocks
    }

    /// Builds the null buffer of every block emitted by `emit_to`, `None`
    /// when a block has no nulls, and removes them from the state.
    ///
    /// Returns one entry per emitted block, following the same rules as
    /// `BlockedGroupsAccumulator::evaluate`.
    pub fn build(&mut self, emit_to: BlockedEmitTo) -> Vec<Option<NullBuffer>> {
        let block_size = self.block_size;
        match &mut self.seen {
            Seen::All { num_groups } => {
                let emitted_blocks = match emit_to {
                    BlockedEmitTo::All => {
                        let blocks = num_groups.div_ceil(block_size);
                        *num_groups = 0;
                        blocks
                    }
                    BlockedEmitTo::NextBlock => {
                        let blocks = usize::from(*num_groups > 0);
                        *num_groups -= (*num_groups).min(block_size);
                        blocks
                    }
                    BlockedEmitTo::First(n) => {
                        *num_groups -= n;
                        1
                    }
                };
                vec![None; emitted_blocks]
            }
            Seen::Some { blocks } => match emit_to {
                BlockedEmitTo::All => {
                    let nulls = blocks.iter_mut().map(|b| Some(finish(b))).collect();
                    self.seen = Seen::All { num_groups: 0 };
                    nulls
                }
                BlockedEmitTo::NextBlock => {
                    if blocks.is_empty() {
                        return vec![];
                    }
                    vec![Some(finish(&mut blocks.remove(0)))]
                }
                BlockedEmitTo::First(n) => {
                    // Rare path (ordered input): re-chunk the remaining bits
                    let bits: Vec<BooleanBuffer> =
                        blocks.iter_mut().map(|b| b.finish()).collect();
                    let first = bits[0].slice(0, n);
                    let mut remaining = vec![];
                    for (i, block) in bits.iter().enumerate() {
                        let skip = if i == 0 { n } else { 0 };
                        let block = block.slice(skip, block.len() - skip);
                        append_buffer(&mut remaining, block_size, &block);
                    }
                    *blocks = remaining;
                    vec![Some(NullBuffer::new(first))]
                }
            },
        }
    }
}

/// Marks groups as seen in the per-block bitmaps.
struct SeenBits<'a> {
    blocks: &'a mut Vec<BooleanBufferBuilder>,
}

impl SeenGroups for SeenBits<'_> {
    #[inline]
    fn mark(&mut self, index: BlocksIndex) {
        self.blocks[index.block_index()].set_bit(index.index_in_block(), true);
    }
}

/// Returns true when all newly registered groups are present in
/// `group_indices`, see the flat `new_groups_are_dense`.
fn new_groups_are_dense(
    group_indices: &[BlocksIndex],
    first_new_group: usize,
    total_num_groups: usize,
    block_size: usize,
) -> bool {
    if first_new_group == total_num_groups {
        return true;
    }

    let mut next_new_group = first_new_group;
    for &group_index in group_indices {
        let group_index = group_index.flat(block_size);
        if group_index == next_new_group {
            next_new_group += 1;
        } else if group_index > next_new_group {
            return false;
        }
    }
    next_new_group == total_num_groups
}

fn bits_len(blocks: &[BooleanBufferBuilder]) -> usize {
    blocks.iter().map(|b| b.len()).sum()
}

/// Appends `n` bits of `value`, filling the last bitmap up to `block_size`
/// bits before starting a new one.
fn append_bits(
    blocks: &mut Vec<BooleanBufferBuilder>,
    block_size: usize,
    mut n: usize,
    value: bool,
) {
    while n > 0 {
        let last = match blocks.last_mut() {
            Some(last) if last.len() < block_size => last,
            _ => {
                blocks.push(BooleanBufferBuilder::new(n.min(block_size)));
                blocks.last_mut().expect("just pushed")
            }
        };
        let count = n.min(block_size - last.len());
        last.append_n(count, value);
        n -= count;
    }
}

/// Appends the bits of `buffer`, like [`append_bits`].
fn append_buffer(
    blocks: &mut Vec<BooleanBufferBuilder>,
    block_size: usize,
    buffer: &BooleanBuffer,
) {
    let mut offset = 0;
    while offset < buffer.len() {
        let last = match blocks.last_mut() {
            Some(last) if last.len() < block_size => last,
            _ => {
                blocks.push(BooleanBufferBuilder::new(
                    (buffer.len() - offset).min(block_size),
                ));
                blocks.last_mut().expect("just pushed")
            }
        };
        let count = (buffer.len() - offset).min(block_size - last.len());
        last.append_buffer(&buffer.slice(offset, count));
        offset += count;
    }
}

fn finish(builder: &mut BooleanBufferBuilder) -> NullBuffer {
    NullBuffer::new(builder.finish())
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::Int64Array;
    use arrow::datatypes::Int64Type;

    fn indices(flats: &[usize]) -> Vec<BlocksIndex> {
        flats
            .iter()
            .map(|&f| BlocksIndex::from_flat(f, 4))
            .collect()
    }

    fn nulls_of(nulls: &[Option<NullBuffer>]) -> Vec<Option<Vec<bool>>> {
        nulls
            .iter()
            .map(|n| n.as_ref().map(|n| n.iter().collect()))
            .collect()
    }

    #[test]
    fn all_seen_needs_no_bitmap() {
        let mut state = BlockedVec::<i64>::new(4);
        let mut nulls = BlockedNullState::new(4);
        let values = Int64Array::from(vec![1, 2, 3, 4, 5, 6]);
        nulls.accumulate(
            &mut state,
            6,
            0,
            &indices(&[0, 1, 2, 3, 4, 5]),
            &values,
            None,
            |s, v| *s += v,
        );
        assert_eq!(nulls.size(), 0);
        assert_eq!(nulls.build(BlockedEmitTo::All), vec![None, None]);
    }

    #[test]
    fn tracks_unseen_groups_across_blocks() {
        let mut state = BlockedVec::<i64>::new(4);
        let mut nulls = BlockedNullState::new(4);
        // group 1 only gets a null, group 5 is filtered out
        let values =
            Int64Array::from(vec![Some(1), None, Some(3), Some(4), Some(5), Some(6)]);
        let filter = BooleanArray::from(vec![true, true, true, true, false, true]);
        nulls.accumulate(
            &mut state,
            7,
            0,
            &indices(&[0, 1, 2, 3, 5, 6]),
            &values,
            Some(&filter),
            |s, v| *s += v,
        );
        let expected = vec![
            Some(vec![true, false, true, true]),
            Some(vec![false, false, true]),
        ];
        assert_eq!(nulls_of(&nulls.build(BlockedEmitTo::All)), expected);
    }

    #[test]
    fn next_block_and_first_n_shift_bits() {
        let mut state = BlockedVec::<i64>::new(4);
        let mut nulls = BlockedNullState::new(4);
        let values =
            Int64Array::from(vec![Some(1), None, Some(3), Some(4), None, Some(6)]);
        nulls.accumulate(
            &mut state,
            6,
            0,
            &indices(&[0, 1, 2, 3, 4, 5]),
            &values,
            None,
            |s, v| *s += v,
        );
        assert_eq!(
            nulls_of(&nulls.build(BlockedEmitTo::First(1))),
            vec![Some(vec![true])]
        );
        // remaining: [f, t, t, f | t]
        assert_eq!(
            nulls_of(&nulls.build(BlockedEmitTo::NextBlock)),
            vec![Some(vec![false, true, true, false])]
        );
        assert_eq!(
            nulls_of(&nulls.build(BlockedEmitTo::All)),
            vec![Some(vec![true])]
        );
        let _ = Int64Type::DATA_TYPE;
    }
}
