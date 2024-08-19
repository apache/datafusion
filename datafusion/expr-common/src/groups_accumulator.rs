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

use std::{
    cmp::min,
    collections::VecDeque,
    iter,
    ops::{Index, IndexMut},
};

use arrow::array::{ArrayRef, BooleanArray};
use datafusion_common::{not_impl_err, DataFusionError, Result};
use std::fmt;

const BLOCKED_INDEX_HIGH_32_BITS_MASK: u64 = 0xffffffff00000000;
const BLOCKED_INDEX_LOW_32_BITS_MASK: u64 = 0x00000000ffffffff;

/// Describes how many rows should be emitted during grouping.
#[derive(Debug, Clone, Copy)]
pub enum EmitTo {
    /// Emit all groups
    All,
    /// Emit only the first `n` groups and shift all existing group
    /// indexes down by `n`.
    ///
    /// For example, if `n=10`, group_index `0, 1, ... 9` are emitted
    /// and group indexes '`10, 11, 12, ...` become `0, 1, 2, ...`.
    First(usize),
    /// Emit next block in the blocked managed groups
    ///
    /// The flag's meaning:
    /// - `true` represents it will be added new groups again,
    ///   we don't need to shift the values down.
    /// - `false` represents new groups still be possible to be
    ///   added, and we need to shift the values down.
    NextBlock(bool),
}

impl EmitTo {
    /// Removes the number of rows from `v` required to emit the right
    /// number of rows, returning a `Vec` with elements taken, and the
    /// remaining values in `v`.
    ///
    /// This avoids copying if Self::All
    pub fn take_needed<T>(&self, v: &mut Vec<T>) -> Vec<T> {
        match self {
            Self::All => {
                // Take the entire vector, leave new (empty) vector
                std::mem::take(v)
            }
            Self::First(n) => {
                // get end n+1,.. values into t
                let mut t = v.split_off(*n);
                // leave n+1,.. in v
                std::mem::swap(v, &mut t);
                t
            }
            Self::NextBlock(_) => unreachable!(
                "can not support blocked emission in take_needed, you should use take_needed_from_blocks"
            ),
        }
    }

    /// Removes the number of rows from `blocks` required to emit,
    /// returning a `Vec` with elements taken.
    ///
    /// The detailed behavior in different emissions:
    ///   - For Emit::CurrentBlock, the first block will be taken and return.
    ///   - For Emit::All and Emit::First, it will be only supported in `GroupStatesMode::Flat`,
    ///     similar as `take_needed`.
    pub fn take_needed_from_blocks<T>(
        &self,
        blocks: &mut VecBlocks<T>,
        mode: GroupStatesMode,
    ) -> Vec<T> {
        match self {
            Self::All => {
                debug_assert!(matches!(mode, GroupStatesMode::Flat));
                blocks.pop_first_block().unwrap()
            }
            Self::First(n) => {
                debug_assert!(matches!(mode, GroupStatesMode::Flat));

                let block = blocks.current_mut().unwrap();
                let split_at = min(block.len(), *n);

                // get end n+1,.. values into t
                let mut t = block.split_off(split_at);
                // leave n+1,.. in v
                std::mem::swap(block, &mut t);
                t
            }
            Self::NextBlock(_) => {
                debug_assert!(matches!(mode, GroupStatesMode::Blocked(_)));
                blocks.pop_first_block().unwrap()
            }
        }
    }
}

/// Mode for `accumulators` and `group values`
///
/// Their meanings:
///   - Flat, the values in them will be managed with a single `Vec`.
///     It will grow constantly when more and more values are inserted,
///     that leads to a considerable amount of copying, and finally a bad performance.
///
///   - Blocked, the values in them will be managed with multiple `Vec`s.
///     When the block is large enough, a new block will be allocated and used
///     for inserting. Obviously, this strategy can avoid copying and get a good performance.
#[derive(Debug, Clone, Copy)]
pub enum GroupStatesMode {
    Flat,
    Blocked(usize),
}

/// Blocked style group index used in blocked mode group values and accumulators
///
/// Parts in index:
///   - High 32 bits represent `block_id`
///   - Low 32 bits represent `block_offset`
#[derive(Debug, Clone, Copy)]
pub struct BlockedGroupIndex {
    pub block_id: usize,
    pub block_offset: usize,
}

impl BlockedGroupIndex {
    pub fn new(group_index: usize) -> Self {
        let block_id =
            ((group_index as u64 >> 32) & BLOCKED_INDEX_LOW_32_BITS_MASK) as usize;
        let block_offset =
            ((group_index as u64) & BLOCKED_INDEX_LOW_32_BITS_MASK) as usize;

        Self {
            block_id,
            block_offset,
        }
    }

    pub fn new_from_parts(block_id: usize, block_offset: usize) -> Self {
        Self {
            block_id,
            block_offset,
        }
    }

    pub fn as_packed_index(&self) -> usize {
        ((((self.block_id as u64) << 32) & BLOCKED_INDEX_HIGH_32_BITS_MASK)
            | (self.block_offset as u64 & BLOCKED_INDEX_LOW_32_BITS_MASK))
            as usize
    }
}

/// The basic data structure for blocked aggregation intermediate results
pub struct Blocks<T> {
    /// The current block, it should be pushed into `previous`
    /// when next block is pushed
    current: Option<T>,

    /// Previous blocks pushed before `current`
    previous: VecDeque<T>,
}

impl<T> Blocks<T> {
    pub fn new() -> Self {
        Self {
            current: None,
            previous: VecDeque::new(),
        }
    }

    pub fn current(&self) -> Option<&T> {
        self.current.as_ref()
    }

    pub fn current_mut(&mut self) -> Option<&mut T> {
        self.current.as_mut()
    }

    pub fn push_block(&mut self, block: T) {
        // If empty, use the block as initialized current
        if self.current.is_none() {
            self.current = Some(block);
            return;
        }

        // Take and push the old current to `previous`,
        // use input `block` as the new `current`
        let old_cur = std::mem::replace(&mut self.current, Some(block)).unwrap();
        self.previous.push_back(old_cur);
    }

    pub fn pop_first_block(&mut self) -> Option<T> {
        // If `previous` not empty, pop the first of them
        if !self.previous.is_empty() {
            return self.previous.pop_front();
        }

        // Otherwise, we pop the current
        std::mem::take(&mut self.current)
    }

    pub fn num_blocks(&self) -> usize {
        if self.current.is_none() {
            return 0;
        }

        self.previous.len() + 1
    }

    // TODO: maybe impl a specific Iterator rather than use the trait object,
    // it can slightly improve performance by eliminating the dynamic dispatch.
    pub fn iter(&self) -> Box<dyn Iterator<Item = &'_ T> + '_> {
        // If current is None, it means no data, return empty iter
        if self.current.is_none() {
            return Box::new(iter::empty());
        }

        let cur_iter = iter::once(self.current.as_ref().unwrap());

        if !self.previous.is_empty() {
            let previous_iter = self.previous.iter();
            Box::new(previous_iter.chain(cur_iter))
        } else {
            Box::new(cur_iter)
        }
    }

    // TODO: maybe impl a specific Iterator rather than use the trait object,
    // it can slightly improve performance by eliminating the dynamic dispatch.
    pub fn iter_mut(&mut self) -> Box<dyn Iterator<Item = &'_ mut T> + '_> {
        // If current is None, it means no data, return empty iter
        if self.current.is_none() {
            return Box::new(iter::empty());
        }

        let cur_iter = iter::once(self.current.as_mut().unwrap());

        if !self.previous.is_empty() {
            let previous_iter = self.previous.iter_mut();
            Box::new(previous_iter.chain(cur_iter))
        } else {
            Box::new(cur_iter)
        }
    }

    pub fn clear(&mut self) {
        *self = Self::new();
    }
}

impl<T: fmt::Debug> fmt::Debug for Blocks<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Blocks")
            .field("current", &self.current)
            .field("previous", &self.previous)
            .finish()
    }
}

impl<T> Index<usize> for Blocks<T> {
    type Output = T;

    fn index(&self, index: usize) -> &T {
        if index < self.previous.len() {
            &self.previous[index]
        } else {
            self.current.as_ref().unwrap()
        }
    }
}

impl<T> IndexMut<usize> for Blocks<T> {
    fn index_mut(&mut self, index: usize) -> &mut T {
        if index < self.previous.len() {
            &mut self.previous[index]
        } else {
            self.current.as_mut().unwrap()
        }
    }
}

impl<T> Default for Blocks<T> {
    fn default() -> Self {
        Self::new()
    }
}

pub type VecBlocks<T> = Blocks<Vec<T>>;

impl<T> VecBlocks<T> {
    pub fn capacity(&self) -> usize {
        let cur_cap = self.current.as_ref().map(|blk| blk.capacity()).unwrap_or(0);
        let prev_cap = self.previous.iter().map(|p| p.capacity()).sum::<usize>();

        cur_cap + prev_cap
    }
}

/// `GroupAccumulator` implements a single aggregate (e.g. AVG) and
/// stores the state for *all* groups internally.
///
/// # Notes on Implementing `GroupAccumulator`
///
/// All aggregates must first implement the simpler [`Accumulator`] trait, which
/// handles state for a single group. Implementing `GroupsAccumulator` is
/// optional and is harder to implement than `Accumulator`, but can be much
/// faster for queries with many group values.  See the [Aggregating Millions of
/// Groups Fast blog] for more background.
///
/// # Details
/// Each group is assigned a `group_index` by the hash table and each
/// accumulator manages the specific state, one per `group_index`.
///
/// `group_index`es are contiguous (there aren't gaps), and thus it is
/// expected that each `GroupAccumulator` will use something like `Vec<..>`
/// to store the group states.
///
/// [`Accumulator`]: crate::accumulator::Accumulator
/// [Aggregating Millions of Groups Fast blog]: https://arrow.apache.org/blog/2023/08/05/datafusion_fast_grouping/
pub trait GroupsAccumulator: Send {
    /// Updates the accumulator's state from its arguments, encoded as
    /// a vector of [`ArrayRef`]s.
    ///
    /// * `values`: the input arguments to the accumulator
    ///
    /// * `group_indices`: To which groups do the rows in `values`
    ///   belong, group id)
    ///
    /// * `opt_filter`: if present, only update aggregate state using
    ///   `values[i]` if `opt_filter[i]` is true
    ///
    /// * `total_num_groups`: the number of groups (the largest
    ///   group_index is thus `total_num_groups - 1`).
    ///
    /// Note that subsequent calls to update_batch may have larger
    /// total_num_groups as new groups are seen.
    fn update_batch(
        &mut self,
        values: &[ArrayRef],
        group_indices: &[usize],
        opt_filter: Option<&BooleanArray>,
        total_num_groups: usize,
    ) -> Result<()>;

    /// Returns the final aggregate value for each group as a single
    /// `RecordBatch`, resetting the internal state.
    ///
    /// The rows returned *must* be in group_index order: The value
    /// for group_index 0, followed by 1, etc.  Any group_index that
    /// did not have values, should be null.
    ///
    /// For example, a `SUM` accumulator maintains a running sum for
    /// each group, and `evaluate` will produce that running sum as
    /// its output for all groups, in group_index order
    ///
    /// If `emit_to`` is [`EmitTo::All`], the accumulator should
    /// return all groups and release / reset its internal state
    /// equivalent to when it was first created.
    ///
    /// If `emit_to` is [`EmitTo::First`], only the first `n` groups
    /// should be emitted and the state for those first groups
    /// removed. State for the remaining groups must be retained for
    /// future use. The group_indices on subsequent calls to
    /// `update_batch` or `merge_batch` will be shifted down by
    /// `n`. See [`EmitTo::First`] for more details.
    fn evaluate(&mut self, emit_to: EmitTo) -> Result<ArrayRef>;

    /// Returns the intermediate aggregate state for this accumulator,
    /// used for multi-phase grouping, resetting its internal state.
    ///
    /// See [`Accumulator::state`] for more information on multi-phase
    /// aggregation.
    ///
    /// For example, `AVG` might return two arrays: `SUM` and `COUNT`
    /// but the `MIN` aggregate would just return a single array.
    ///
    /// Note more sophisticated internal state can be passed as
    /// single `StructArray` rather than multiple arrays.
    ///
    /// See [`Self::evaluate`] for details on the required output
    /// order and `emit_to`.
    ///
    /// [`Accumulator::state`]: crate::accumulator::Accumulator::state
    fn state(&mut self, emit_to: EmitTo) -> Result<Vec<ArrayRef>>;

    /// Returns `true` if this accumulator supports blocked mode.
    fn supports_blocked_mode(&self) -> bool {
        false
    }

    /// Switch the accumulator to flat or blocked mode.
    ///
    /// After switching mode, all data in previous mode will be cleared.
    fn switch_to_mode(&mut self, mode: GroupStatesMode) -> Result<()> {
        if matches!(&mode, GroupStatesMode::Blocked(_)) {
            return Err(DataFusionError::NotImplemented(
                "this accumulator doesn't support blocked mode yet".to_string(),
            ));
        }

        Ok(())
    }

    /// Merges intermediate state (the output from [`Self::state`])
    /// into this accumulator's current state.
    ///
    /// For some aggregates (such as `SUM`), `merge_batch` is the same
    /// as `update_batch`, but for some aggregates (such as `COUNT`,
    /// where the partial counts must be summed) the operations
    /// differ. See [`Self::state`] for more details on how state is
    /// used and merged.
    ///
    /// * `values`: arrays produced from calling `state` previously to the accumulator
    ///
    /// Other arguments are the same as for [`Self::update_batch`];
    fn merge_batch(
        &mut self,
        values: &[ArrayRef],
        group_indices: &[usize],
        opt_filter: Option<&BooleanArray>,
        total_num_groups: usize,
    ) -> Result<()>;

    /// Converts an input batch directly the intermediate aggregate state.
    ///
    /// This is the equivalent of treating each input row as its own group. It
    /// is invoked when the Partial phase of a multi-phase aggregation is not
    /// reducing the cardinality enough to warrant spending more effort on
    /// pre-aggregation (see `Background` section below), and switches to
    /// passing intermediate state directly on to the next aggregation phase.
    ///
    /// Examples:
    /// * `COUNT`: an array of 1s for each row in the input batch.
    /// * `SUM/MIN/MAX`: the input values themselves.
    ///
    /// # Arguments
    /// * `values`: the input arguments to the accumulator
    /// * `opt_filter`: if present, any row where `opt_filter[i]` is false should be ignored
    ///
    /// # Background
    ///
    /// In a multi-phase aggregation (see [`Accumulator::state`]), the initial
    /// Partial phase reduces the cardinality of the input data as soon as
    /// possible in the plan.
    ///
    /// This strategy is very effective for queries with a small number of
    /// groups, as most of the data is aggregated immediately and only a small
    /// amount of data must be repartitioned (see [`Accumulator::state`] for
    /// background)
    ///
    /// However, for queries with a large number of groups, the Partial phase
    /// often does not reduce the cardinality enough to warrant the memory and
    /// CPU cost of actually performing the aggregation. For such cases, the
    /// HashAggregate operator will dynamically switch to passing intermediate
    /// state directly to the next aggregation phase with minimal processing
    /// using this method.
    ///
    /// [`Accumulator::state`]: crate::accumulator::Accumulator::state
    fn convert_to_state(
        &self,
        _values: &[ArrayRef],
        _opt_filter: Option<&BooleanArray>,
    ) -> Result<Vec<ArrayRef>> {
        not_impl_err!("Input batch conversion to state not implemented")
    }

    /// Returns `true` if [`Self::convert_to_state`] is implemented to support
    /// intermediate aggregate state conversion.
    fn supports_convert_to_state(&self) -> bool {
        false
    }

    /// Amount of memory used to store the state of this accumulator,
    /// in bytes.
    ///
    /// This function is called once per batch, so it should be `O(n)` to
    /// compute, not `O(num_groups)`
    fn size(&self) -> usize;
}
