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

//! Utilities for implementing GroupsAccumulator
//! Adapter that makes [`GroupsAccumulator`] out of [`Accumulator`]

pub mod accumulate;
pub mod bool_op;
pub mod nulls;
pub mod prim_op;

use std::{
    cmp::min,
    collections::VecDeque,
    fmt::{self, Debug},
    iter,
    ops::{Index, IndexMut},
};

use arrow::{
    array::{ArrayRef, AsArray, BooleanArray, PrimitiveArray},
    compute,
    datatypes::UInt32Type,
};
use datafusion_common::{
    arrow_datafusion_err, utils::get_arrayref_at_indices, DataFusionError, Result,
    ScalarValue,
};
use datafusion_expr_common::accumulator::Accumulator;
use datafusion_expr_common::groups_accumulator::{EmitTo, GroupsAccumulator};

pub const MAX_PREALLOC_BLOCK_SIZE: usize = 8192;
const FLAT_GROUP_INDEX_ID_MASK: u64 = 0;
const FLAT_GROUP_INDEX_OFFSET_MASK: u64 = u64::MAX;
const BLOCKED_GROUP_INDEX_ID_MASK: u64 = 0xffffffff00000000;
const BLOCKED_GROUP_INDEX_OFFSET_MASK: u64 = 0x00000000ffffffff;

/// An adapter that implements [`GroupsAccumulator`] for any [`Accumulator`]
///
/// While [`Accumulator`] are simpler to implement and can support
/// more general calculations (like retractable window functions),
/// they are not as fast as a specialized `GroupsAccumulator`. This
/// interface bridges the gap so the group by operator only operates
/// in terms of [`Accumulator`].
pub struct GroupsAccumulatorAdapter {
    factory: Box<dyn Fn() -> Result<Box<dyn Accumulator>> + Send>,

    /// state for each group, stored in group_index order
    states: Vec<AccumulatorState>,

    /// Current memory usage, in bytes.
    ///
    /// Note this is incrementally updated with deltas to avoid the
    /// call to size() being a bottleneck. We saw size() being a
    /// bottleneck in earlier implementations when there were many
    /// distinct groups.
    allocation_bytes: usize,
}

struct AccumulatorState {
    /// [`Accumulator`] that stores the per-group state
    accumulator: Box<dyn Accumulator>,

    // scratch space: indexes in the input array that will be fed to
    // this accumulator. Stores indexes as `u32` to match the arrow
    // `take` kernel input.
    indices: Vec<u32>,
}

impl AccumulatorState {
    fn new(accumulator: Box<dyn Accumulator>) -> Self {
        Self {
            accumulator,
            indices: vec![],
        }
    }

    /// Returns the amount of memory taken by this structure and its accumulator
    fn size(&self) -> usize {
        self.accumulator.size()
            + std::mem::size_of_val(self)
            + self.indices.allocated_size()
    }
}

impl GroupsAccumulatorAdapter {
    /// Create a new adapter that will create a new [`Accumulator`]
    /// for each group, using the specified factory function
    pub fn new<F>(factory: F) -> Self
    where
        F: Fn() -> Result<Box<dyn Accumulator>> + Send + 'static,
    {
        Self {
            factory: Box::new(factory),
            states: vec![],
            allocation_bytes: 0,
        }
    }

    /// Ensure that self.accumulators has total_num_groups
    fn make_accumulators_if_needed(&mut self, total_num_groups: usize) -> Result<()> {
        // can't shrink
        assert!(total_num_groups >= self.states.len());
        let vec_size_pre = self.states.allocated_size();

        // instantiate new accumulators
        let new_accumulators = total_num_groups - self.states.len();
        for _ in 0..new_accumulators {
            let accumulator = (self.factory)()?;
            let state = AccumulatorState::new(accumulator);
            self.add_allocation(state.size());
            self.states.push(state);
        }

        self.adjust_allocation(vec_size_pre, self.states.allocated_size());
        Ok(())
    }

    /// invokes f(accumulator, values) for each group that has values
    /// in group_indices.
    ///
    /// This function first reorders the input and filter so that
    /// values for each group_index are contiguous and then invokes f
    /// on the contiguous ranges, to minimize per-row overhead
    ///
    /// ```text
    /// ┌─────────┐   ┌─────────┐   ┌ ─ ─ ─ ─ ┐                       ┌─────────┐   ┌ ─ ─ ─ ─ ┐
    /// │ ┌─────┐ │   │ ┌─────┐ │     ┌─────┐              ┏━━━━━┓    │ ┌─────┐ │     ┌─────┐
    /// │ │  2  │ │   │ │ 200 │ │   │ │  t  │ │            ┃  0  ┃    │ │ 200 │ │   │ │  t  │ │
    /// │ ├─────┤ │   │ ├─────┤ │     ├─────┤              ┣━━━━━┫    │ ├─────┤ │     ├─────┤
    /// │ │  2  │ │   │ │ 100 │ │   │ │  f  │ │            ┃  0  ┃    │ │ 300 │ │   │ │  t  │ │
    /// │ ├─────┤ │   │ ├─────┤ │     ├─────┤              ┣━━━━━┫    │ ├─────┤ │     ├─────┤
    /// │ │  0  │ │   │ │ 200 │ │   │ │  t  │ │            ┃  1  ┃    │ │ 200 │ │   │ │NULL │ │
    /// │ ├─────┤ │   │ ├─────┤ │     ├─────┤   ────────▶  ┣━━━━━┫    │ ├─────┤ │     ├─────┤
    /// │ │  1  │ │   │ │ 200 │ │   │ │NULL │ │            ┃  2  ┃    │ │ 200 │ │   │ │  t  │ │
    /// │ ├─────┤ │   │ ├─────┤ │     ├─────┤              ┣━━━━━┫    │ ├─────┤ │     ├─────┤
    /// │ │  0  │ │   │ │ 300 │ │   │ │  t  │ │            ┃  2  ┃    │ │ 100 │ │   │ │  f  │ │
    /// │ └─────┘ │   │ └─────┘ │     └─────┘              ┗━━━━━┛    │ └─────┘ │     └─────┘
    /// └─────────┘   └─────────┘   └ ─ ─ ─ ─ ┘                       └─────────┘   └ ─ ─ ─ ─ ┘
    ///
    /// logical group   values      opt_filter           logical group  values       opt_filter
    ///
    /// ```
    fn invoke_per_accumulator<F>(
        &mut self,
        values: &[ArrayRef],
        group_indices: &[usize],
        opt_filter: Option<&BooleanArray>,
        total_num_groups: usize,
        f: F,
    ) -> Result<()>
    where
        F: Fn(&mut dyn Accumulator, &[ArrayRef]) -> Result<()>,
    {
        self.make_accumulators_if_needed(total_num_groups)?;

        assert_eq!(values[0].len(), group_indices.len());

        // figure out which input rows correspond to which groups.
        // Note that self.state.indices starts empty for all groups
        // (it is cleared out below)
        for (idx, group_index) in group_indices.iter().enumerate() {
            self.states[*group_index].indices.push(idx as u32);
        }

        // groups_with_rows holds a list of group indexes that have
        // any rows that need to be accumulated, stored in order of
        // group_index

        let mut groups_with_rows = vec![];

        // batch_indices holds indices into values, each group is contiguous
        let mut batch_indices = vec![];

        // offsets[i] is index into batch_indices where the rows for
        // group_index i starts
        let mut offsets = vec![0];

        let mut offset_so_far = 0;
        for (group_index, state) in self.states.iter_mut().enumerate() {
            let indices = &state.indices;
            if indices.is_empty() {
                continue;
            }

            groups_with_rows.push(group_index);
            batch_indices.extend_from_slice(indices);
            offset_so_far += indices.len();
            offsets.push(offset_so_far);
        }
        let batch_indices = batch_indices.into();

        // reorder the values and opt_filter by batch_indices so that
        // all values for each group are contiguous, then invoke the
        // accumulator once per group with values
        let values = get_arrayref_at_indices(values, &batch_indices)?;
        let opt_filter = get_filter_at_indices(opt_filter, &batch_indices)?;

        // invoke each accumulator with the appropriate rows, first
        // pulling the input arguments for this group into their own
        // RecordBatch(es)
        let iter = groups_with_rows.iter().zip(offsets.windows(2));

        let mut sizes_pre = 0;
        let mut sizes_post = 0;
        for (&group_idx, offsets) in iter {
            let state = &mut self.states[group_idx];
            sizes_pre += state.size();

            let values_to_accumulate =
                slice_and_maybe_filter(&values, opt_filter.as_ref(), offsets)?;
            (f)(state.accumulator.as_mut(), &values_to_accumulate)?;

            // clear out the state so they are empty for next
            // iteration
            state.indices.clear();
            sizes_post += state.size();
        }

        self.adjust_allocation(sizes_pre, sizes_post);
        Ok(())
    }

    /// Increment the allocation by `n`
    ///
    /// See [`Self::allocation_bytes`] for rationale.
    fn add_allocation(&mut self, size: usize) {
        self.allocation_bytes += size;
    }

    /// Decrease the allocation by `n`
    ///
    /// See [`Self::allocation_bytes`] for rationale.
    fn free_allocation(&mut self, size: usize) {
        // use saturating sub to avoid errors if the accumulators
        // report erronious sizes
        self.allocation_bytes = self.allocation_bytes.saturating_sub(size)
    }

    /// Adjusts the allocation for something that started with
    /// start_size and now has new_size avoiding overflow
    ///
    /// See [`Self::allocation_bytes`] for rationale.
    fn adjust_allocation(&mut self, old_size: usize, new_size: usize) {
        if new_size > old_size {
            self.add_allocation(new_size - old_size)
        } else {
            self.free_allocation(old_size - new_size)
        }
    }
}

impl GroupsAccumulator for GroupsAccumulatorAdapter {
    fn update_batch(
        &mut self,
        values: &[ArrayRef],
        group_indices: &[usize],
        opt_filter: Option<&BooleanArray>,
        total_num_groups: usize,
    ) -> Result<()> {
        self.invoke_per_accumulator(
            values,
            group_indices,
            opt_filter,
            total_num_groups,
            |accumulator, values_to_accumulate| {
                accumulator.update_batch(values_to_accumulate)
            },
        )?;
        Ok(())
    }

    fn evaluate(&mut self, emit_to: EmitTo) -> Result<ArrayRef> {
        let vec_size_pre = self.states.allocated_size();

        let states = emit_to.take_needed(&mut self.states);

        let results: Vec<ScalarValue> = states
            .into_iter()
            .map(|mut state| {
                self.free_allocation(state.size());
                state.accumulator.evaluate()
            })
            .collect::<Result<_>>()?;

        let result = ScalarValue::iter_to_array(results);

        self.adjust_allocation(vec_size_pre, self.states.allocated_size());

        result
    }

    fn state(&mut self, emit_to: EmitTo) -> Result<Vec<ArrayRef>> {
        let vec_size_pre = self.states.allocated_size();
        let states = emit_to.take_needed(&mut self.states);

        // each accumulator produces a potential vector of values
        // which we need to form into columns
        let mut results: Vec<Vec<ScalarValue>> = vec![];

        for mut state in states {
            self.free_allocation(state.size());
            let accumulator_state = state.accumulator.state()?;
            results.resize_with(accumulator_state.len(), Vec::new);
            for (idx, state_val) in accumulator_state.into_iter().enumerate() {
                results[idx].push(state_val);
            }
        }

        // create an array for each intermediate column
        let arrays = results
            .into_iter()
            .map(ScalarValue::iter_to_array)
            .collect::<Result<Vec<_>>>()?;

        // double check each array has the same length (aka the
        // accumulator was implemented correctly
        if let Some(first_col) = arrays.first() {
            for arr in &arrays {
                assert_eq!(arr.len(), first_col.len())
            }
        }
        self.adjust_allocation(vec_size_pre, self.states.allocated_size());

        Ok(arrays)
    }

    fn merge_batch(
        &mut self,
        values: &[ArrayRef],
        group_indices: &[usize],
        opt_filter: Option<&BooleanArray>,
        total_num_groups: usize,
    ) -> Result<()> {
        self.invoke_per_accumulator(
            values,
            group_indices,
            opt_filter,
            total_num_groups,
            |accumulator, values_to_accumulate| {
                accumulator.merge_batch(values_to_accumulate)?;
                Ok(())
            },
        )?;
        Ok(())
    }

    fn size(&self) -> usize {
        self.allocation_bytes
    }
}

/// Extension trait for [`Vec`] to account for allocations.
pub trait VecAllocExt {
    /// Item type.
    type T;
    /// Return the amount of memory allocated by this Vec (not
    /// recursively counting any heap allocations contained within the
    /// structure). Does not include the size of `self`
    fn allocated_size(&self) -> usize;
}

impl<T> VecAllocExt for Vec<T> {
    type T = T;
    fn allocated_size(&self) -> usize {
        std::mem::size_of::<T>() * self.capacity()
    }
}

pub trait EmitToExt {
    /// Removes the number of rows from `v` required to emit the right
    /// number of rows, returning a `Vec` with elements taken, and the
    /// remaining values in `v`.
    ///
    /// This avoids copying if Self::All
    fn take_needed<T>(&self, v: &mut Vec<T>) -> Vec<T>;

    /// Removes the number of rows from `blocks` required to emit,
    /// returning a `Vec` with elements taken.
    ///
    /// The detailed behavior in different emissions:
    ///   - For Emit::CurrentBlock, the first block will be taken and return.
    ///   - For Emit::All and Emit::First, it will be only supported in `GroupStatesMode::Flat`,
    ///     similar as `take_needed`.
    fn take_needed_from_blocks<T>(&self, blocks: &mut VecBlocks<T>) -> Vec<T>;
}

impl EmitToExt for EmitTo {
    fn take_needed<T>(&self, v: &mut Vec<T>) -> Vec<T> {
        match self {
            Self::All => {
                // Take the entire vector, leave new (empty) vector
                std::mem::take(v)
            }
            Self::First(n) => {
                let split_at = min(v.len(), *n);
                // get end n+1,.. values into t
                let mut t = v.split_off(split_at);
                // leave n+1,.. in v
                std::mem::swap(v, &mut t);
                t
            }
            Self::NextBlock(_) => unreachable!(
                "can not support blocked emission in take_needed, you should use take_needed_from_blocks"
            ),
        }
    }

    fn take_needed_from_blocks<T>(&self, blocks: &mut VecBlocks<T>) -> Vec<T> {
        match self {
            Self::All => {
                debug_assert!(blocks.num_blocks() == 1);
                blocks.pop_first_block().unwrap_or_default()
            }
            Self::First(n) => {
                debug_assert!(blocks.num_blocks() == 1);
                let block = blocks.current_mut().unwrap();
                let split_at = min(block.len(), *n);

                // get end n+1,.. values into t
                let mut t = block.split_off(split_at);
                // leave n+1,.. in v
                std::mem::swap(block, &mut t);
                t
            }
            Self::NextBlock(_) => blocks.pop_first_block().unwrap_or_default(),
        }
    }
}

/// Blocked style group index used in blocked mode group values and accumulators
///
/// In blocked mode(is_blocked=true):
///   - High 32 bits represent `block_id`
///   - Low 32 bits represent `block_offset`
///
/// In flat mode(is_blocked=false)
///   - `block_id` is always 0
///   - total 64 bits used to represent the `block offset`
///
#[derive(Debug, Clone, Copy)]
pub struct BlockedGroupIndex {
    pub block_id: u32,
    pub block_offset: u64,
}

impl BlockedGroupIndex {
    #[inline]
    pub fn new_from_parts(block_id: u32, block_offset: u64) -> Self {
        Self {
            block_id,
            block_offset,
        }
    }

    #[inline]
    pub fn block_id(&self) -> usize {
        self.block_id as usize
    }

    #[inline]
    pub fn block_offset(&self) -> usize {
        self.block_offset as usize
    }

    #[inline]
    pub fn as_packed_index(&self) -> usize {
        (((self.block_id as u64) << 32) | self.block_offset) as usize
    }
}

pub struct BlockedGroupIndexBuilder {
    block_id_mask: u64,
    block_offset_mask: u64,
}

impl BlockedGroupIndexBuilder {
    #[inline]
    pub fn new(is_blocked: bool) -> Self {
        if is_blocked {
            Self {
                block_id_mask: BLOCKED_GROUP_INDEX_ID_MASK,
                block_offset_mask: BLOCKED_GROUP_INDEX_OFFSET_MASK,
            }
        } else {
            Self {
                block_id_mask: FLAT_GROUP_INDEX_ID_MASK,
                block_offset_mask: FLAT_GROUP_INDEX_OFFSET_MASK,
            }
        }
    }

    #[inline]
    pub fn build(&self, packed_index: usize) -> BlockedGroupIndex {
        let block_id = (((packed_index as u64) & self.block_id_mask) >> 32) as u32;
        let block_offset = (packed_index as u64) & self.block_offset_mask;

        BlockedGroupIndex {
            block_id,
            block_offset,
        }
    }
}

/// The basic data structure for blocked aggregation intermediate results
///
/// The reason why not use `VecDeque` directly:
///
/// `current` and `current_mut` will be called frequently,
/// and if we use `VecDeque` directly, they will be mapped
/// to `back` and `back_mut` in it.
///
/// `back` and `back_mut` are implemented using indexed operation
/// which need some computation about address that will be a bit
/// more expansive than we keep the latest element in `current`,
/// and just return reference of it directly.
///
/// This small optimization can bring slight performance improvement
/// in the single block case(e.g. when blocked optimization is disabled).
///
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

    pub fn len(&self) -> usize {
        let cur_len = self.current.as_ref().map(|blk| blk.len()).unwrap_or(0);
        let prev_len = self.previous.iter().map(|p| p.len()).sum::<usize>();

        cur_len + prev_len
    }

    pub fn is_empty(&self) -> bool {
        self.current.is_none()
    }
}

fn get_filter_at_indices(
    opt_filter: Option<&BooleanArray>,
    indices: &PrimitiveArray<UInt32Type>,
) -> Result<Option<ArrayRef>> {
    opt_filter
        .map(|filter| {
            compute::take(
                &filter, indices, None, // None: no index check
            )
        })
        .transpose()
        .map_err(|e| arrow_datafusion_err!(e))
}

// Copied from physical-plan
pub(crate) fn slice_and_maybe_filter(
    aggr_array: &[ArrayRef],
    filter_opt: Option<&ArrayRef>,
    offsets: &[usize],
) -> Result<Vec<ArrayRef>> {
    let (offset, length) = (offsets[0], offsets[1] - offsets[0]);
    let sliced_arrays: Vec<ArrayRef> = aggr_array
        .iter()
        .map(|array| array.slice(offset, length))
        .collect();

    if let Some(f) = filter_opt {
        let filter_array = f.slice(offset, length);
        let filter_array = filter_array.as_boolean();

        sliced_arrays
            .iter()
            .map(|array| {
                compute::filter(array, filter_array).map_err(|e| arrow_datafusion_err!(e))
            })
            .collect()
    } else {
        Ok(sliced_arrays)
    }
}

/// Expend blocked values to a big enough size for holding `total_num_groups` groups.
///
/// For example,
///
/// before expanding:
///   values: [x, x, x], [x, x, x] (blocks=2, block_size=3)
///   total_num_groups: 8
///
/// After expanding:
///   values: [x, x, x], [x, x, x], [default, default, default]
///
pub fn ensure_enough_room_for_values<T: Clone>(
    values: &mut VecBlocks<T>,
    total_num_groups: usize,
    block_size: Option<usize>,
    default_value: T,
) {
    let calc_block_size = block_size.unwrap_or(usize::MAX);
    // In blocked mode, we ensure the blks are enough first,
    // and then ensure slots in blks are enough.
    let (mut cur_blk_idx, exist_slots) = if values.num_blocks() > 0 {
        let cur_blk_idx = values.num_blocks() - 1;
        let exist_slots =
            (values.num_blocks() - 1) * calc_block_size + values.current().unwrap().len();

        (cur_blk_idx, exist_slots)
    } else {
        (0, 0)
    };

    // No new groups, don't need to expand, just return.
    if exist_slots >= total_num_groups {
        return;
    }

    // Ensure blks are enough.
    let exist_blks = values.num_blocks();
    let new_blks = total_num_groups.saturating_add(calc_block_size - 1) / calc_block_size
        - exist_blks;

    if new_blks > 0 {
        let prealloc_size = block_size.unwrap_or(0).min(MAX_PREALLOC_BLOCK_SIZE);
        for _ in 0..new_blks {
            values.push_block(Vec::with_capacity(calc_block_size.min(prealloc_size)));
        }
    }

    // Ensure slots are enough.
    let mut new_slots = total_num_groups - exist_slots;

    // Expand current blk.
    let cur_blk_rest_slots = calc_block_size - values[cur_blk_idx].len();
    if cur_blk_rest_slots >= new_slots {
        // We just need to expand current blocks.
        values[cur_blk_idx].extend(iter::repeat(default_value.clone()).take(new_slots));
        return;
    }

    // Expand current blk to full, and expand next blks
    values[cur_blk_idx]
        .extend(iter::repeat(default_value.clone()).take(cur_blk_rest_slots));
    new_slots -= cur_blk_rest_slots;
    cur_blk_idx += 1;

    // Expand whole blks
    let expand_blks = new_slots / calc_block_size;
    for _ in 0..expand_blks {
        values[cur_blk_idx]
            .extend(iter::repeat(default_value.clone()).take(calc_block_size));
        cur_blk_idx += 1;
    }

    // Expand the last blk if needed
    let last_expand_slots = new_slots % calc_block_size;
    if last_expand_slots > 0 {
        values
            .current_mut()
            .unwrap()
            .extend(iter::repeat(default_value.clone()).take(last_expand_slots));
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_ensure_room_for_values() {
        let mut blocks = VecBlocks::new();
        let block_size = 4;

        // 0 total_num_groups, should be no blocks
        ensure_enough_room_for_values(&mut blocks, 0, Some(block_size), 0);
        assert_eq!(blocks.num_blocks(), 0);
        assert_eq!(blocks.len(), 0);

        // 0 -> 3 total_num_groups, blocks should look like:
        // [d, d, d, empty]
        ensure_enough_room_for_values(&mut blocks, 3, Some(block_size), 0);
        assert_eq!(blocks.num_blocks(), 1);
        assert_eq!(blocks.len(), 3);

        // 3 -> 8 total_num_groups, blocks should look like:
        // [d, d, d, d], [d, d, d, d]
        ensure_enough_room_for_values(&mut blocks, 8, Some(block_size), 0);
        assert_eq!(blocks.num_blocks(), 2);
        assert_eq!(blocks.len(), 8);

        // 8 -> 13 total_num_groups, blocks should look like:
        // [d, d, d, d], [d, d, d, d], [d, d, d, d], [d, empty, empty, empty]
        ensure_enough_room_for_values(&mut blocks, 13, Some(block_size), 0);
        assert_eq!(blocks.num_blocks(), 4);
        assert_eq!(blocks.len(), 13);

        // Block size none, it means we will always use one single block
        // [] -> [d, d, d,...,d]
        blocks.clear();
        ensure_enough_room_for_values(&mut blocks, 42, None, 0);
        assert_eq!(blocks.num_blocks(), 1);
        assert_eq!(blocks.len(), 42);
    }

    #[test]
    fn test_blocks_ops() {
        let mut blocks = VecBlocks::<i32>::new();

        // Test empty blocks
        assert!(blocks.current().is_none());
        assert!(blocks.current_mut().is_none());
        assert!(blocks.pop_first_block().is_none());
        assert_eq!(blocks.num_blocks(), 0);
        {
            let mut iter = blocks.iter();
            assert!(iter.next().is_none());
        }
        {
            let mut iter_mut = blocks.iter_mut();
            assert!(iter_mut.next().is_none());
        }

        // Test push block
        for cnt in 0..100 {
            blocks.push_block(Vec::with_capacity(4));

            assert!(blocks.current().is_some());
            assert!(blocks.current_mut().is_some());
            assert_eq!(blocks.num_blocks(), cnt + 1);

            let block_num = blocks.iter().count();
            assert_eq!(block_num, cnt + 1);
            let block_num = blocks.iter_mut().count();
            assert_eq!(block_num, cnt + 1);
        }

        // Test pop block
        for cnt in 0..100 {
            blocks.pop_first_block();

            let rest_blk_num = 100 - cnt - 1;
            assert_eq!(blocks.num_blocks(), rest_blk_num);
            if rest_blk_num > 0 {
                assert!(blocks.current().is_some());
                assert!(blocks.current_mut().is_some());
            } else {
                assert!(blocks.current().is_none());
                assert!(blocks.current_mut().is_none());
            }

            let block_num = blocks.iter().count();
            assert_eq!(block_num, rest_blk_num);
            let block_num = blocks.iter_mut().count();
            assert_eq!(block_num, rest_blk_num);
        }
    }

    #[test]
    fn test_take_need() {
        let values = vec![1, 2, 3, 4, 5, 6, 7, 8];

        // Test emit all
        let emit = EmitTo::All;
        let mut source = values.clone();
        let expected = values.clone();
        let actual = emit.take_needed(&mut source);
        assert_eq!(actual, expected);
        assert!(source.is_empty());

        // Test emit first n
        // n < source len
        let emit = EmitTo::First(4);
        let mut origin = values.clone();
        let expected = origin[0..4].to_vec();
        let rest_expected = origin[4..].to_vec();
        let actual = emit.take_needed(&mut origin);
        assert_eq!(actual, expected);
        assert_eq!(origin, rest_expected);

        // n > source len
        let emit = EmitTo::First(9);
        let mut origin = values.clone();
        let expected = values.clone();
        let actual = emit.take_needed(&mut origin);
        assert_eq!(actual, expected);
        assert!(origin.is_empty());
    }

    #[test]
    fn test_take_need_from_blocks() {
        let block1 = vec![1, 2, 3, 4];
        let block2 = vec![5, 6, 7, 8];

        let mut values = VecBlocks::new();
        values.push_block(block1.clone());
        values.push_block(block2.clone());

        // Test emit block
        let emit = EmitTo::NextBlock(false);
        let actual = emit.take_needed_from_blocks(&mut values);
        assert_eq!(actual, block1);

        let actual = emit.take_needed_from_blocks(&mut values);
        assert_eq!(actual, block2);

        let actual = emit.take_needed_from_blocks(&mut values);
        assert!(actual.is_empty());
    }
}
