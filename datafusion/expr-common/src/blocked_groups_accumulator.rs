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

//! Vectorized [`BlockedGroupsAccumulator`]: [`GroupsAccumulator`] with state
//! stored in blocks.
//!
//! Storing per-group state in fixed-size blocks lets hash aggregation emit and
//! free its state one block at a time instead of materializing every group
//! into one large array. See <https://github.com/apache/datafusion/issues/24704>.
//!
//! [`GroupsAccumulator`]: crate::groups_accumulator::GroupsAccumulator

use std::any::Any;
use std::sync::Arc;

use arrow::array::{ArrayRef, BooleanArray};
use datafusion_common::{Result, exec_err, not_impl_err};

use crate::accumulator::AggregateMetrics;
use crate::groups_accumulator::EmitTo;

/// Identifies one group in blocked storage: the block that holds the group
/// and the group's position inside that block.
///
/// This is the only place that knows how a group index is laid out; storage
/// and accumulators only use the accessors below.
#[repr(transparent)]
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, Default)]
pub struct BlocksIndex(u64);

impl BlocksIndex {
    /// Creates the index of the `index_in_block`-th group of block
    /// `block_index`.
    #[inline]
    pub const fn new(block_index: usize, index_in_block: usize) -> Self {
        debug_assert!(block_index <= u32::MAX as usize);
        debug_assert!(index_in_block <= u32::MAX as usize);
        Self(((block_index as u64) << 32) | index_in_block as u64)
    }

    /// Block that holds this group.
    #[inline]
    pub const fn block_index(self) -> usize {
        (self.0 >> 32) as usize
    }

    /// Position of this group inside its block.
    #[inline]
    pub const fn index_in_block(self) -> usize {
        self.0 as u32 as usize
    }

    /// Creates the index of the group at flat index `flat`, counting from the
    /// first group, when every block holds `block_size` groups.
    #[inline]
    pub const fn from_flat(flat: usize, block_size: usize) -> Self {
        Self::new(flat / block_size, flat % block_size)
    }

    /// Flat index of this group, counting from the first group, when every
    /// block holds `block_size` groups.
    #[inline]
    pub const fn flat(self, block_size: usize) -> usize {
        self.block_index() * block_size + self.index_in_block()
    }

    /// Writes [`Self::from_flat`] of every index in `flat` to `out`,
    /// replacing its contents.
    pub fn from_flat_slice(flat: &[usize], block_size: usize, out: &mut Vec<Self>) {
        out.clear();
        if block_size.is_power_of_two() {
            let shift = block_size.trailing_zeros();
            let mask = block_size - 1;
            out.extend(flat.iter().map(|&f| Self::new(f >> shift, f & mask)));
        } else {
            out.extend(flat.iter().map(|&f| Self::from_flat(f, block_size)));
        }
    }

    /// Writes [`Self::flat`] of every index in `indices` to `out`, replacing
    /// its contents.
    pub fn to_flat_slice(indices: &[Self], block_size: usize, out: &mut Vec<usize>) {
        out.clear();
        if block_size.is_power_of_two() {
            let shift = block_size.trailing_zeros();
            out.extend(
                indices
                    .iter()
                    .map(|i| (i.block_index() << shift) | i.index_in_block()),
            );
        } else {
            out.extend(indices.iter().map(|i| i.flat(block_size)));
        }
    }
}

/// Which groups to emit from a [`BlockedGroupsAccumulator`].
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum BlockedEmitTo {
    /// Emit every group, as one array per block, and reset the state.
    All,
    /// Emit the first block (or nothing if there are no groups).
    ///
    /// The remaining groups move down by one block, like [`EmitTo::First`]
    /// with the first block's length. To emit every group, use [`Self::All`].
    NextBlock,
    /// Emit the first `n` groups, where `0 < n < block_size`, and shift the
    /// remaining groups down by `n`, like [`EmitTo::First`].
    First(usize),
}

impl BlockedEmitTo {
    /// Splits a flat [`EmitTo`] into block-sized emits.
    ///
    /// `EmitTo::First(n)` becomes `n / block_size` [`Self::NextBlock`]s
    /// followed by one [`Self::First`] with the remainder, if any. Each emit
    /// shifts the remaining groups, so they must all be applied, in order,
    /// before any new group is added.
    pub fn from_emit_to(emit_to: EmitTo, block_size: usize) -> Vec<Self> {
        match emit_to {
            EmitTo::All => vec![Self::All],
            EmitTo::First(n) => {
                let mut emits = vec![Self::NextBlock; n / block_size];
                if !n.is_multiple_of(block_size) {
                    emits.push(Self::First(n % block_size));
                }
                emits
            }
        }
    }
}

/// Selects groups for a non-destructive read of blocked state, like
/// [`GroupSelection`] for flat state.
///
/// Selections created by [`Self::try_from_indices`] preserve the requested
/// order and support duplicate indices.
///
/// [`GroupSelection`]: crate::groups_accumulator::GroupSelection
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct BlockedGroupSelection<'a> {
    total_num_groups: usize,
    block_size: usize,
    indices: Option<&'a [BlocksIndex]>,
}

impl<'a> BlockedGroupSelection<'a> {
    /// Selects all `total_num_groups` groups, stored in blocks of
    /// `block_size` groups, in group order.
    pub fn all(total_num_groups: usize, block_size: usize) -> Self {
        Self {
            total_num_groups,
            block_size,
            indices: None,
        }
    }

    /// Selects groups in the order specified by `indices`.
    ///
    /// Returns an error if an index is not one of the first
    /// `total_num_groups` groups. Empty selections are valid, and duplicate
    /// indices are preserved.
    pub fn try_from_indices(
        indices: &'a [BlocksIndex],
        total_num_groups: usize,
        block_size: usize,
    ) -> Result<Self> {
        if let Some(index) = indices
            .iter()
            .find(|index| index.flat(block_size) >= total_num_groups)
        {
            return exec_err!(
                "Group index {index:?} is out of bounds for {total_num_groups} groups"
            );
        }
        Ok(Self {
            total_num_groups,
            block_size,
            indices: Some(indices),
        })
    }

    /// Returns the group count against which this selection was constructed.
    pub fn total_num_groups(self) -> usize {
        self.total_num_groups
    }

    /// Ensures this selection is being applied to the same number of groups
    /// against which it was constructed, see
    /// `GroupSelection::validate_num_groups`.
    pub fn validate_num_groups(self, actual_num_groups: usize) -> Result<()> {
        if actual_num_groups != self.total_num_groups {
            return exec_err!(
                "Group selection was constructed for {} groups but applied to {actual_num_groups} groups",
                self.total_num_groups
            );
        }
        Ok(())
    }

    /// Returns the number of selected groups.
    pub fn len(self) -> usize {
        self.indices
            .map_or(self.total_num_groups, |indices| indices.len())
    }

    /// Returns `true` if no groups are selected.
    pub fn is_empty(self) -> bool {
        self.len() == 0
    }

    /// Returns the selected groups in output order.
    pub fn iter(self) -> impl Iterator<Item = BlocksIndex> + 'a {
        let block_size = self.block_size;
        let (all, indices): (_, &'a [BlocksIndex]) = match self.indices {
            None => (0..self.total_num_groups, &[]),
            Some(indices) => (0..0, indices),
        };
        all.map(move |flat| BlocksIndex::from_flat(flat, block_size))
            .chain(indices.iter().copied())
    }
}

/// Like [`GroupsAccumulator`], but the per-group state is stored in blocks of
/// [`Self::block_size`] groups, and emitting returns one array per block
/// instead of one large array.
///
/// Hash aggregation uses a `BlockedGroupsAccumulator` only when every
/// aggregate in the query (and the group keys) support blocked storage;
/// otherwise the whole aggregation uses [`GroupsAccumulator`].
///
/// [`GroupsAccumulator`]: crate::groups_accumulator::GroupsAccumulator
pub trait BlockedGroupsAccumulator: Send + Any {
    /// Number of groups per block. Fixed for the accumulator's lifetime.
    fn block_size(&self) -> usize;

    /// See `GroupsAccumulator::set_metrics`.
    fn set_metrics(&mut self, _metrics: Arc<dyn AggregateMetrics>) {}

    /// Updates the state of each group with the values of its rows.
    ///
    /// Same contract as `GroupsAccumulator::update_batch`, with the group of
    /// each row given as a [`BlocksIndex`].
    fn update_batch(
        &mut self,
        values: &[ArrayRef],
        group_indices: &[BlocksIndex],
        opt_filter: Option<&BooleanArray>,
        total_num_groups: usize,
    ) -> Result<()>;

    /// Merges intermediate state produced by [`Self::state`].
    ///
    /// Same contract as `GroupsAccumulator::merge_batch`.
    fn merge_batch(
        &mut self,
        values: &[ArrayRef],
        group_indices: &[BlocksIndex],
        total_num_groups: usize,
    ) -> Result<()>;

    /// Returns the final value of the groups selected by `emit_to`, and
    /// removes their state.
    ///
    /// * [`BlockedEmitTo::All`]: one array per block
    /// * [`BlockedEmitTo::NextBlock`]: zero or one arrays
    /// * [`BlockedEmitTo::First`]: exactly one array
    fn evaluate(&mut self, emit_to: BlockedEmitTo) -> Result<Vec<ArrayRef>>;

    /// Returns final values without changing the state or the group indices,
    /// see `GroupsAccumulator::evaluate_preserving`.
    ///
    /// Rows are returned in the order of `selection`, in arrays of at most
    /// [`Self::block_size`] rows. An empty selection returns no arrays.
    fn evaluate_preserving(
        &mut self,
        _selection: BlockedGroupSelection<'_>,
    ) -> Result<Vec<ArrayRef>> {
        not_impl_err!("Preserving grouped evaluation is not implemented")
    }

    /// Returns `true` if [`Self::evaluate_preserving`] is implemented.
    fn supports_evaluate_preserving(&self) -> bool {
        false
    }

    /// Returns the intermediate state of the groups selected by `emit_to`,
    /// indexed `[block][state column]`, and removes their state.
    ///
    /// Blocks follow the same rules as [`Self::evaluate`].
    fn state(&mut self, emit_to: BlockedEmitTo) -> Result<Vec<Vec<ArrayRef>>>;

    /// Returns intermediate state without changing the state or the group
    /// indices, see `GroupsAccumulator::state_preserving`.
    ///
    /// Indexed `[chunk][state column]`, where chunks follow the same rules as
    /// [`Self::evaluate_preserving`].
    fn state_preserving(
        &mut self,
        _selection: BlockedGroupSelection<'_>,
    ) -> Result<Vec<Vec<ArrayRef>>> {
        not_impl_err!("Preserving grouped state is not implemented")
    }

    /// Returns `true` if [`Self::state_preserving`] is implemented.
    fn supports_state_preserving(&self) -> bool {
        false
    }

    /// Converts an input batch directly to intermediate state.
    ///
    /// Same contract as `GroupsAccumulator::convert_to_state`.
    fn convert_to_state(
        &self,
        values: &[ArrayRef],
        opt_filter: Option<&BooleanArray>,
    ) -> Result<Vec<ArrayRef>>;

    /// Amount of memory used to store the state, in bytes.
    fn size(&self) -> usize;
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn from_emit_to_splits_into_blocks() {
        assert_eq!(
            BlockedEmitTo::from_emit_to(EmitTo::All, 4),
            vec![BlockedEmitTo::All]
        );
        assert_eq!(
            BlockedEmitTo::from_emit_to(EmitTo::First(3), 4),
            vec![BlockedEmitTo::First(3)]
        );
        assert_eq!(
            BlockedEmitTo::from_emit_to(EmitTo::First(4), 4),
            vec![BlockedEmitTo::NextBlock]
        );
        assert_eq!(
            BlockedEmitTo::from_emit_to(EmitTo::First(9), 4),
            vec![
                BlockedEmitTo::NextBlock,
                BlockedEmitTo::NextBlock,
                BlockedEmitTo::First(1)
            ]
        );
        assert!(BlockedEmitTo::from_emit_to(EmitTo::First(0), 4).is_empty());
    }

    #[test]
    fn blocks_index_slice_conversions() {
        for block_size in [4, 6] {
            let flat = vec![0, 3, 4, 13, 7];
            let mut blocked = vec![BlocksIndex::new(9, 9)];
            BlocksIndex::from_flat_slice(&flat, block_size, &mut blocked);
            let expected: Vec<_> = flat
                .iter()
                .map(|&f| BlocksIndex::from_flat(f, block_size))
                .collect();
            assert_eq!(blocked, expected);
            let mut back = vec![42];
            BlocksIndex::to_flat_slice(&blocked, block_size, &mut back);
            assert_eq!(back, flat);
        }
    }

    #[test]
    fn blocks_index_round_trips() {
        let index = BlocksIndex::new(3, 7);
        assert_eq!(index.block_index(), 3);
        assert_eq!(index.index_in_block(), 7);
        assert_eq!(index.flat(16), 55);
        assert_eq!(BlocksIndex::from_flat(55, 16), index);
        assert!(BlocksIndex::new(0, 9) < BlocksIndex::new(1, 0));
    }
}
