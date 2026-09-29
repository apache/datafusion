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

//! [`BlockedPrimitiveDistinctCountGroupsAccumulator`]: distinct count with
//! state stored in blocks.

use std::hash::Hash;
use std::mem::size_of;
use std::sync::Arc;

use arrow::array::{
    ArrayRef, AsArray, BooleanArray, Int64Array, ListArray, PrimitiveArray,
};
use arrow::buffer::{OffsetBuffer, ScalarBuffer};
use arrow::datatypes::{ArrowPrimitiveType, Field};
use datafusion_common::HashSet;
use datafusion_common::hash_utils::RandomState;
use datafusion_expr_common::blocked_groups_accumulator::{
    BlockedEmitTo, BlockedGroupsAccumulator, BlocksIndex,
};

use super::groups::distinct_convert_to_state;
use crate::aggregate::groups_accumulator::accumulate::accumulate_blocked;
use crate::aggregate::groups_accumulator::blocked_vec::BlockedVec;

/// Distinct values seen by the groups of one block, keyed by the group's
/// position in the block.
type BlockSeen<V> = HashSet<(u32, V), RandomState>;

/// [`PrimitiveDistinctCountGroupsAccumulator`] with state stored in blocks.
///
/// The distinct values are kept in one set per block (instead of one set for
/// all groups), so a block is emitted by draining only its own set, and its
/// memory is released with it.
///
/// [`PrimitiveDistinctCountGroupsAccumulator`]: super::PrimitiveDistinctCountGroupsAccumulator
pub struct BlockedPrimitiveDistinctCountGroupsAccumulator<T: ArrowPrimitiveType>
where
    T::Native: Eq + Hash,
{
    /// One set per block of `counts`
    seen: Vec<BlockSeen<T::Native>>,
    counts: BlockedVec<i64>,
}

impl<T: ArrowPrimitiveType> BlockedPrimitiveDistinctCountGroupsAccumulator<T>
where
    T::Native: Eq + Hash,
{
    pub fn new(block_size: usize) -> Self {
        Self {
            seen: vec![],
            counts: BlockedVec::new(block_size),
        }
    }

    /// Grows `counts` to `total_num_groups` and adds a set for every new block.
    fn grow_to(&mut self, total_num_groups: usize) {
        self.counts.grow_to(total_num_groups, 0);
        self.seen
            .resize_with(self.counts.num_blocks(), BlockSeen::default);
    }

    /// Adds `value` to the group at `index`, counting it if it is new.
    #[inline]
    fn insert(
        seen: &mut [BlockSeen<T::Native>],
        counts: &mut BlockedVec<i64>,
        index: BlocksIndex,
        value: T::Native,
    ) {
        if seen[index.block_index()].insert((index.index_in_block() as u32, value)) {
            // SAFETY: `index` was registered by `grow_to`
            unsafe { *counts.get_unchecked_mut(index) += 1 };
        }
    }

    /// Removes the counts and distinct values of the groups selected by
    /// `emit_to`, one entry per emitted block.
    fn take(&mut self, emit_to: BlockedEmitTo) -> Vec<(Vec<i64>, BlockSeen<T::Native>)> {
        match emit_to {
            BlockedEmitTo::All => {
                let counts = self.counts.take_all();
                let seen = std::mem::take(&mut self.seen);
                counts.into_iter().zip(seen).collect()
            }
            BlockedEmitTo::NextBlock => match self.counts.take_next_block() {
                Some(counts) => vec![(counts, self.seen.remove(0))],
                None => vec![],
            },
            BlockedEmitTo::First(n) => {
                // Rare path (ordered input): renumber every remaining group
                let block_size = self.counts.block_size();
                let counts = self.counts.take_first(n);
                let mut emitted = BlockSeen::default();
                let mut remaining: Vec<BlockSeen<T::Native>> =
                    (0..self.counts.num_blocks())
                        .map(|_| BlockSeen::default())
                        .collect();
                for (block_index, block) in
                    std::mem::take(&mut self.seen).into_iter().enumerate()
                {
                    for (index_in_block, value) in block {
                        let flat = BlocksIndex::new(block_index, index_in_block as usize)
                            .flat(block_size);
                        match flat.checked_sub(n) {
                            None => {
                                emitted.insert((index_in_block, value));
                            }
                            Some(flat) => {
                                let index = BlocksIndex::from_flat(flat, block_size);
                                remaining[index.block_index()]
                                    .insert((index.index_in_block() as u32, value));
                            }
                        }
                    }
                }
                self.seen = remaining;
                vec![(counts, emitted)]
            }
        }
    }
}

impl<T: ArrowPrimitiveType + Send + std::fmt::Debug> BlockedGroupsAccumulator
    for BlockedPrimitiveDistinctCountGroupsAccumulator<T>
where
    T::Native: Eq + Hash,
{
    fn block_size(&self) -> usize {
        self.counts.block_size()
    }

    fn update_batch(
        &mut self,
        values: &[ArrayRef],
        group_indices: &[BlocksIndex],
        opt_filter: Option<&BooleanArray>,
        total_num_groups: usize,
    ) -> datafusion_common::Result<()> {
        debug_assert_eq!(values.len(), 1);
        self.grow_to(total_num_groups);
        let arr = values[0].as_primitive::<T>();
        let (seen, counts) = (&mut self.seen, &mut self.counts);
        accumulate_blocked(group_indices, arr, opt_filter, |index, value| {
            Self::insert(seen, counts, index, value)
        });
        Ok(())
    }

    fn merge_batch(
        &mut self,
        values: &[ArrayRef],
        group_indices: &[BlocksIndex],
        total_num_groups: usize,
    ) -> datafusion_common::Result<()> {
        debug_assert_eq!(values.len(), 1);
        self.grow_to(total_num_groups);
        let list_array = values[0].as_list::<i32>();
        let inner_values = list_array.values().as_primitive::<T>().values();
        let offsets = list_array.offsets();

        for (row_idx, &index) in group_indices.iter().enumerate() {
            let start = offsets[row_idx] as usize;
            let end = offsets[row_idx + 1] as usize;
            for &value in &inner_values[start..end] {
                Self::insert(&mut self.seen, &mut self.counts, index, value);
            }
        }
        Ok(())
    }

    fn evaluate(
        &mut self,
        emit_to: BlockedEmitTo,
    ) -> datafusion_common::Result<Vec<ArrayRef>> {
        Ok(self
            .take(emit_to)
            .into_iter()
            // The emitted sets are dropped here, releasing their memory
            .map(|(counts, _)| Arc::new(Int64Array::from(counts)) as ArrayRef)
            .collect())
    }

    fn state(
        &mut self,
        emit_to: BlockedEmitTo,
    ) -> datafusion_common::Result<Vec<Vec<ArrayRef>>> {
        Ok(self
            .take(emit_to)
            .into_iter()
            .map(|(counts, seen)| {
                // Prefix-sum the counts of the block into list offsets, then
                // move every distinct value to its group's list
                let mut offsets = Vec::with_capacity(counts.len() + 1);
                offsets.push(0i32);
                let mut total = 0i32;
                for &count in &counts {
                    total += count as i32;
                    offsets.push(total);
                }
                let mut values = vec![T::Native::default(); total as usize];
                let mut cursors: Vec<i32> = offsets[..counts.len()].to_vec();
                for (index_in_block, value) in seen {
                    let cursor = &mut cursors[index_in_block as usize];
                    values[*cursor as usize] = value;
                    *cursor += 1;
                }
                let values =
                    Arc::new(PrimitiveArray::<T>::new(ScalarBuffer::from(values), None));
                vec![Arc::new(ListArray::new(
                    Arc::new(Field::new_list_field(T::DATA_TYPE, true)),
                    OffsetBuffer::new(offsets.into()),
                    values,
                    None,
                )) as ArrayRef]
            })
            .collect())
    }

    fn convert_to_state(
        &self,
        values: &[ArrayRef],
        opt_filter: Option<&BooleanArray>,
    ) -> datafusion_common::Result<Vec<ArrayRef>> {
        distinct_convert_to_state::<T>(values, opt_filter)
    }

    fn size(&self) -> usize {
        let entry = size_of::<(u32, T::Native)>() + size_of::<u64>();
        size_of::<Self>()
            + self
                .seen
                .iter()
                .map(|s| s.capacity() * entry)
                .sum::<usize>()
            + self.counts.allocated_size()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Array, Int32Array};
    use arrow::datatypes::Int32Type;
    use datafusion_common::Result;

    fn indices(flats: &[usize]) -> Vec<BlocksIndex> {
        flats
            .iter()
            .map(|&f| BlocksIndex::from_flat(f, 4))
            .collect()
    }

    fn counts(arrays: &[ArrayRef]) -> Vec<Vec<i64>> {
        arrays
            .iter()
            .map(|a| {
                a.as_primitive::<arrow::datatypes::Int64Type>()
                    .values()
                    .to_vec()
            })
            .collect()
    }

    fn accumulator() -> Result<BlockedPrimitiveDistinctCountGroupsAccumulator<Int32Type>>
    {
        let mut acc = BlockedPrimitiveDistinctCountGroupsAccumulator::<Int32Type>::new(4);
        let values: ArrayRef = Arc::new(Int32Array::from(vec![
            Some(1),
            Some(1),
            Some(2),
            None,
            Some(7),
            Some(7),
            Some(8),
        ]));
        acc.update_batch(&[values], &indices(&[0, 0, 0, 1, 5, 5, 5]), None, 6)?;
        Ok(acc)
    }

    #[test]
    fn counts_distinct_values_per_block() -> Result<()> {
        let mut acc = accumulator()?;
        assert_eq!(
            counts(&acc.evaluate(BlockedEmitTo::NextBlock)?),
            vec![vec![2, 0, 0, 0]]
        );
        assert_eq!(acc.seen.len(), 1, "the emitted block's set is released");
        assert_eq!(counts(&acc.evaluate(BlockedEmitTo::All)?), vec![vec![0, 2]]);
        Ok(())
    }

    #[test]
    fn state_round_trips_through_merge() -> Result<()> {
        let mut acc = accumulator()?;
        let state = acc.state(BlockedEmitTo::All)?;
        assert_eq!(state.len(), 2);
        let mut merged =
            BlockedPrimitiveDistinctCountGroupsAccumulator::<Int32Type>::new(4);
        for (block_index, columns) in state.iter().enumerate() {
            let rows = columns[0].len();
            let groups: Vec<BlocksIndex> = (0..rows)
                .map(|i| BlocksIndex::new(block_index, i))
                .collect();
            merged.merge_batch(columns, &groups, 6)?;
        }
        // merging the same values again does not change the distinct counts
        merged.merge_batch(&state[1], &indices(&[4, 5]), 6)?;
        assert_eq!(
            counts(&merged.evaluate(BlockedEmitTo::All)?),
            vec![vec![2, 0, 0, 0], vec![0, 2]]
        );
        Ok(())
    }

    #[test]
    fn first_n_renumbers_across_blocks() -> Result<()> {
        let mut acc = accumulator()?;
        assert_eq!(
            counts(&acc.evaluate(BlockedEmitTo::First(1))?),
            vec![vec![2]]
        );
        // group 5 moved to position 4 (block 1, index 0)
        let values: ArrayRef = Arc::new(Int32Array::from(vec![7, 9]));
        acc.update_batch(&[values], &indices(&[4, 4]), None, 5)?;
        assert_eq!(
            counts(&acc.evaluate(BlockedEmitTo::All)?),
            vec![vec![0, 0, 0, 0], vec![3]]
        );
        Ok(())
    }
}
