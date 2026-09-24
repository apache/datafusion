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

//! Vectorized [`GroupsAccumulator`]

use arrow::array::{ArrayRef, BooleanArray};
use datafusion_common::{
    Result, assert_ne_or_internal_err, assert_or_internal_err, exec_err, not_impl_err,
    utils::split_vec_min_alloc,
};
use std::cmp::Ordering;
use crate::groups_accumulator::EmitTo;

/// Selects groups for a non-destructive grouped aggregation read.
///
/// Unlike [`BlockedEmitTo`], this selection does not remove groups or change their
/// indices. Selections created by [`Self::try_from_indices`] preserve the
/// requested order and support duplicate indices.
///
/// A selection is validated once when it is constructed and can then be reused
/// for the group values and accumulators participating in the same snapshot.
/// Construct a new selection if the number or indexing of those groups changes.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct BlockedGroupSelection<'a> {
    block_size: usize,
    total_num_groups: usize,
    indices: Option<&'a [BlocksIndex]>,
}

impl<'a> BlockedGroupSelection<'a> {
    /// Selects all `total_num_groups` groups in group-index order.
    pub fn all(total_num_groups: usize, block_size: usize) -> Self {
        Self {
            total_num_groups,
            indices: None,
            block_size,
        }
    }

    /// Selects groups in the order specified by `indices`.
    ///
    /// Returns an error if an index is not less than `total_num_groups`. Empty
    /// selections are valid, and duplicate indices are preserved.
    pub fn try_from_indices(
        indices: &'a [BlocksIndex],
        total_num_groups: usize,
        block_size: usize,
    ) -> Result<Self> {
        let total_num_groups_parsed =
            BlocksIndex::from_index_in_fixed_block_size(total_num_groups, block_size);
        if let Some(index) = indices
            .iter()
            .find(|&&index| index >= total_num_groups_parsed)
        {
            return exec_err!(
                "Group index {index:?} is out of bounds for {total_num_groups_parsed:?} ({total_num_groups}) groups"
            );
        }
        Ok(Self {
            total_num_groups,
            indices: Some(indices),
            block_size,
        })
    }

    /// Returns the group count against which this selection was constructed.
    pub fn total_num_groups(self) -> usize {
        self.total_num_groups
    }

    /// Ensures this selection is being applied to the same number of groups
    /// against which it was constructed.
    ///
    /// Preserving-read implementations should call this method with their
    /// stored group count before using [`Self::iter`]. This check is `O(1)`;
    /// the selected indices were already checked by [`Self::try_from_indices`].
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

    /// Returns the selected group indices in output order.
    pub fn iter(self) -> impl Iterator<Item = BlocksIndex> + 'a {
        let (all, indices): (_, &'a [BlocksIndex]) = match self.indices {
            None => (0..self.total_num_groups, &[]),
            Some(indices) => (0..0, indices),
        };
        let block_size = self.block_size;
        all.map(move |index| {
            BlocksIndex::from_index_in_fixed_block_size(index, block_size)
        })
        .chain(indices.iter().copied())
    }

    #[doc(hidden)]
    pub fn indices(&self) -> Option<&[BlocksIndex]> {
        self.indices
    }
}

/// Two `u32`s (8 bytes) rather than two `usize`s: an index is stored per group in every blocked
/// hash table entry, so its size decides the size of the tables. Both halves fit `u32` by a wide
/// margin, `index_in_block < block_size` (the batch size) and the number of blocks is
/// `groups / block_size`. The API keeps taking and returning `usize`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Hash)]
pub struct BlocksIndex {
    block_index: u32,
    index_in_block: u32,
}

impl BlocksIndex {
    pub const ZERO: Self = Self {
        // flat_index: 0,
        block_index: 0,
        index_in_block: 0,
    };
    pub const MAX: Self = Self {
        // flat_index: usize::MAX,
        block_index: u32::MAX,
        index_in_block: u32::MAX,
    };

    pub fn new(block_index: usize, index_in_block: usize) -> Self {
      Self {
        block_index: Self::narrow(block_index),
        index_in_block: Self::narrow(index_in_block),
      }
    }

    #[inline(always)]
    fn narrow(value: usize) -> u32 {
        debug_assert!(value <= u32::MAX as usize, "block index part {value} does not fit in u32");
        value as u32
    }

    #[inline(always)]
    pub fn new_in_first_block(index_in_block: usize) -> Self {
        // Implementation note:
        // not having From<usize> that will do this instead even when it will be more convenient
        // so we can later change the layout to be a single usize with bit shifts
        Self::new(0, index_in_block)
        // Self {
        //     flat_index: index_in_block,
        // }
    }

    #[inline(always)]
    pub fn from_index_in_fixed_block_size(index: usize, block_size: usize) -> Self {
        Self::new(index / block_size, index % block_size)
    }

    #[inline(always)]
    pub fn into_index_in_fixed_block_size(self, block_size: usize) -> usize {
        self.block_index as usize * block_size + self.index_in_block as usize
    }

    #[inline(always)]
    pub fn block_index(&self) -> usize {
        self.block_index as usize
    }

    #[inline(always)]
    pub fn index_in_block(&self) -> usize {
        self.index_in_block as usize
    }

    pub fn sub_flat_checked(self, rhs_flat: usize, block_size: usize) -> Option<Self> {
        let self_flat = self.into_index_in_fixed_block_size(block_size);
        self_flat.checked_sub(rhs_flat).map(|v| Self::from_index_in_fixed_block_size(v, block_size))
    }

    pub fn sub_flat(self, rhs_flat: usize, block_size: usize) -> Self {
        Self::from_index_in_fixed_block_size(self.into_index_in_fixed_block_size(block_size) - rhs_flat, block_size)
    }

    pub fn gte_flat(self, rhs_flat: usize, block_size: usize) -> bool {
        self.into_index_in_fixed_block_size(block_size) >= rhs_flat
    }

    pub fn prev_block(mut self) -> Self {
        self.block_index -= 1;

        self
    }

    pub fn prev_block_checked(mut self) -> Option<Self> {
        self.block_index.checked_sub(1).map(|v| {
            self.block_index = v;
            self
        })
    }

    pub fn add(self, n: usize, block_size: usize) -> BlocksIndex {
        if self.index_in_block + n as u32 >= block_size as u32 {
            let new_flat = self.into_index_in_fixed_block_size(block_size) + n;
            Self::from_index_in_fixed_block_size(new_flat, block_size)
        } else {
            Self::new(self.block_index as usize, self.index_in_block as usize + n)
        }
    }

    pub fn increment(mut self, block_size: usize) -> Self {
        if self.index_in_block + 1 >= block_size as u32 {
            self.block_index += 1;
            self.index_in_block = 0;
        } else {
            self.index_in_block += 1;
        }

        self
    }
}

impl PartialOrd for BlocksIndex {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for BlocksIndex {
    fn cmp(&self, other: &Self) -> Ordering {
        self.block_index.cmp(&other.block_index).then(self.index_in_block.cmp(&other.index_in_block))
    }
}

/// Describes how many rows should be emitted during grouping.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BlockedEmitTo {
    All,
    /// Emit next group
    NextBlock,
    /// Emit only the first `n` groups and shift all existing group
    /// indexes down by `n`.
    ///
    /// For example, if `n=10`, group_index `0, 1, ... 9` are emitted
    /// and group indexes `10, 11, 12, ...` become `0, 1, 2, ...`.
    ///
    /// Requirements:
    /// 1. `n` must be smaller than block_size
    /// 2. `n` is not 0
    First(usize),
}

impl BlockedEmitTo {
    pub fn into_emit_to(
        self,
        len: usize,
        block_size: usize,
    ) -> Result<impl Iterator<Item = EmitTo> + Clone> {
        let mut to_emit = match self {
            Self::All => len,
            Self::NextBlock => len.min(block_size),
            Self::First(n) => {
                assert_ne_or_internal_err!(n, 0);
                assert_or_internal_err!(
                    n <= len,
                    "n ({n}) must be less than or equal current length ({})",
                    len
                );
                assert_or_internal_err!(
                    n < block_size,
                    "n ({n}) must be less than current block size ({})",
                    block_size
                );
                n
            }
        };

        let mut blocks = vec![];

        let emitting_all = len == to_emit;

        while to_emit > block_size {
            blocks.push(EmitTo::First(block_size));
            to_emit -= block_size
        }

        if to_emit > 0 && emitting_all {
            blocks.push(EmitTo::All);
        } else if to_emit > 0 {
            blocks.push(EmitTo::First(to_emit));
        }

        Ok(blocks.into_iter())
    }
}

/// `GroupsAccumulator` implements a single aggregate (e.g. AVG) and
/// stores the state for *all* groups internally.
///
/// Logically, a [`GroupsAccumulator`] stores a mapping from each group index to
/// the state of the aggregate for that group. For example an implementation for
/// `min` might look like
///
/// ```text
///    ┌─────┐
///    │  0  │───────────▶   100
///    ├─────┤
///    │  1  │───────────▶   200
///    └─────┘
///      ...                 ...
///    ┌─────┐
///    │ N-2 │───────────▶    50
///    ├─────┤
///    │ N-1 │───────────▶   200
///    └─────┘
///
///
///  Logical group      Current Min
///     number          value for that
///                     group
/// ```
///
/// # Notes on Implementing `GroupsAccumulator`
///
/// All aggregates must first implement the simpler [`Accumulator`] trait, which
/// handles state for a single group. Implementing `GroupsAccumulator` is
/// optional and is harder to implement than `Accumulator`, but can be much
/// faster for queries with many group values.  See the [Aggregating Millions of
/// Groups Fast blog] for more background.
/// For more background, please also see the [Aggregating Millions of Groups Fast in Apache Arrow DataFusion 28.0.0 blog]
///
/// [Aggregating Millions of Groups Fast in Apache Arrow DataFusion 28.0.0 blog]: https://datafusion.apache.org/blog/2023/08/05/datafusion_fast_grouping
///
/// [`NullState`] can help keep the state for groups that have not seen any
/// values and produce the correct output for those groups.
///
/// [`NullState`]: https://docs.rs/datafusion/latest/datafusion/physical_expr/struct.NullState.html
///
/// # Details
/// Each group is assigned a `group_index` by the hash table and each
/// accumulator manages the specific state, one per `group_index`.
///
/// `group_index`es are contiguous (there aren't gaps), and thus it is
/// expected that each `GroupsAccumulator` will use something like `Vec<..>`
/// to store the group states.
///
/// [`Accumulator`]: crate::accumulator::Accumulator
/// [Aggregating Millions of Groups Fast blog]: https://arrow.apache.org/blog/2023/08/05/datafusion_fast_grouping/
pub trait BlockedGroupsAccumulator: Send + std::any::Any {
    fn batch_size(&self) -> usize;

    fn update_batch(
        &mut self,
        values: &[ArrayRef],
        // Get blocks index rather than flat index to avoid extra computation when inserting and searching (BlocksIndex can be implemented as flat usize)
        group_indices: &[BlocksIndex],
        opt_filter: Option<&BooleanArray>,
        total_num_groups: usize,
    ) -> Result<()>;

    /// Same as `GroupsAccumulator::evaluate` but returning vec of blocks and different EmitTo
    /// For
    /// - `BlockedEmitTo::All` it should return `Vec<Block>`
    /// - `BlockedEmitTo::NextBlock` it should return single item vector with the block or empty vec in case of no blocks
    /// - `BlockedEmitTo::First(n)` it should return single item vector with the first n rows in the first block. n must be smaller than block size and length
    ///
    fn evaluate(&mut self, emit_to: BlockedEmitTo) -> Result<Vec<ArrayRef>>;

    // Same as `GroupsAccumulator::evaluate_preserving` but with blocked indices
    fn evaluate_preserving(
        &mut self,
        _selection: BlockedGroupSelection<'_>,
    ) -> Result<ArrayRef> {
        not_impl_err!("Preserving grouped evaluation is not implemented")
    }

    /// Returns `true` if [`Self::evaluate_preserving`] is implemented.
    fn supports_evaluate_preserving(&self) -> bool {
        false
    }

    /// Same as `GroupsAccumulator::state` but returning vec of blocks and different EmitTo
    /// For
    /// - `BlockedEmitTo::All` it should return `Vec<Block>`
    /// - `BlockedEmitTo::NextBlock` it should return single item vector with the block or empty vec in case of no blocks
    /// - `BlockedEmitTo::First(n)` it should return single item vector with the first n rows in the first block. n must be smaller than block size and length
    ///
    fn state(&mut self, emit_to: BlockedEmitTo) -> Result<Vec<Vec<ArrayRef>>>;

    /// Same as `GroupsAccumulator::state_preserving` but with blocked indices
    fn state_preserving(
        &mut self,
        _selection: BlockedGroupSelection<'_>,
    ) -> Result<Vec<ArrayRef>> {
        not_impl_err!("Preserving grouped state is not implemented")
    }

    /// Returns `true` if [`Self::state_preserving`] is implemented.
    fn supports_state_preserving(&self) -> bool {
        false
    }
    fn merge_batch(
        &mut self,
        values: &[ArrayRef],

        // Get blocks index rather than flat index to avoid extra computation when inserting and searching (BlocksIndex can be implemented as flat usize)
        group_indices: &[BlocksIndex],
        total_num_groups: usize,
    ) -> Result<()>;

    /// Same as `GroupsAccumulator::convert_to_state`
    fn convert_to_state(
        &self,
        values: &[ArrayRef],
        opt_filter: Option<&BooleanArray>,
    ) -> Result<Vec<ArrayRef>>;

    /// Same as `GroupsAccumulator::size`
    fn size(&self) -> usize;
}
