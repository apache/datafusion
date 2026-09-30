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

//! [`BlockedVec`]: group state stored in fixed size blocks

use std::mem::size_of;

use arrow::array::BooleanArray;
use arrow::buffer::NullBuffer;

use datafusion_expr_common::blocked_groups_accumulator::BlocksIndex;

use super::accumulate::accumulate_blocked_indices;

/// Rows per chunk whose element addresses are resolved before any of them is
/// updated, see [`BlockedVec::update`].
const RESOLVE_CHUNK: usize = 256;

/// A growable vector of per group state stored as a list of blocks, used by
/// [`BlockedGroupsAccumulator`] implementations.
///
/// It stays a single flat `Vec` (growing by doubling) until it reaches
/// `block_size` elements. That `Vec` then becomes block 0 without any copy.
/// Every following block also grows by doubling, up to `block_size`, so a
/// block that only holds a few groups does not allocate a whole block.
/// Accumulators update their state with [`Self::update`] and
/// [`Self::update_with`], which pick the fastest loop for the current layout
/// once per batch.
///
/// Emitting the first block ([`Self::take_next_block`]) or all blocks
/// ([`Self::take_all`]) moves the block `Vec`s out without copying.
///
/// `FIXED_BLOCK_SIZE = false` is reserved for the values of nested children
/// (e.g. list elements), where block boundaries are driven by the parent
/// through a future `start_new_block()`. It is not implemented yet, but the
/// layout (one `Vec` per block, the block length being the `Vec` length)
/// already supports it.
///
/// # Implementation Notes
///
/// ## Why `T: Copy`?
///
/// 1. So the [`BlockedVec::allocated_size`] will be accurate since the size of T is known, and not include heap allocations (like `String` or `Vec`) that are not part of the allocated size of the `BlockedVec`
/// 2. So we can provide mutable access to the items (e.g. `IndexMut`) since if `T` is not `Copy` (like when `T` is a `Vec`) we could change the size of it without the [`BlockedVec::allocated_size`] changing, which would be confusing and lead to bugs
///
/// [`BlockedGroupsAccumulator`]: datafusion_expr_common::blocked_groups_accumulator::BlockedGroupsAccumulator
#[derive(Debug)]
pub struct BlockedVec<T, const FIXED_BLOCK_SIZE: bool = true> {
    /// Every block except the last one holds exactly `block_size` elements
    blocks: Vec<Vec<T>>,
    /// Total number of elements over all blocks
    len: usize,
    block_size: usize,
    /// Running total of the bytes allocated by the blocks' buffers
    allocated: usize,
}

impl<T: Copy, const FIXED_BLOCK_SIZE: bool> BlockedVec<T, FIXED_BLOCK_SIZE> {
    /// Creates an empty vector.
    ///
    /// # Panics
    /// If `block_size` is 0.
    pub fn new(block_size: usize) -> Self {
        if !FIXED_BLOCK_SIZE {
            // Reserved for nested child values, whose block sizes are driven
            // by the parent via a future `start_new_block()`.
            unimplemented!("BlockedVec with dynamic block sizes");
        }
        assert!(block_size > 0, "block_size must be positive");
        Self {
            blocks: Vec::new(),
            len: 0,
            block_size,
            allocated: 0,
        }
    }

    /// Maximum number of elements in a block.
    pub fn block_size(&self) -> usize {
        self.block_size
    }

    /// Total number of elements.
    #[inline]
    pub fn len(&self) -> usize {
        self.len
    }

    /// Returns `true` if there are no elements.
    #[inline]
    pub fn is_empty(&self) -> bool {
        self.len == 0
    }

    /// Number of blocks, including a partially filled last block.
    #[inline]
    pub fn num_blocks(&self) -> usize {
        self.blocks.len()
    }

    /// Grows the vector to `new_len` elements, filling with `value`.
    /// Never shrinks: does nothing if `new_len <= self.len()`.
    pub fn grow_to(&mut self, new_len: usize, value: T) {
        while self.len < new_len {
            let last_len = self.last_block_for_append();
            let additional = (new_len - self.len).min(self.block_size - last_len);
            let last = self.reserve_last(last_len + additional);
            last.resize(last_len + additional, value);
            self.len += additional;
        }
    }

    /// Appends `value` and returns its index.
    #[inline]
    pub fn push(&mut self, value: T) -> BlocksIndex {
        self.len += 1;
        let num_blocks = self.blocks.len();
        match self.blocks.last_mut() {
            // Block capacities never exceed `block_size`, so this never
            // reallocates
            Some(last) if last.len() < last.capacity() => {
                last.push(value);
                BlocksIndex::new(num_blocks - 1, last.len() - 1)
            }
            _ => self.push_slow(value),
        }
    }

    #[cold]
    fn push_slow(&mut self, value: T) -> BlocksIndex {
        let last_len = self.last_block_for_append();
        self.reserve_last(last_len + 1).push(value);
        BlocksIndex::new(self.blocks.len() - 1, last_len)
    }

    /// Makes sure the last block has room for at least one more element,
    /// adding a new block if needed, and returns its length.
    fn last_block_for_append(&mut self) -> usize {
        match self.blocks.last() {
            Some(last) if last.len() < self.block_size => last.len(),
            // Every block, including block 0 (the flat phase), grows by
            // doubling in `reserve_last`
            _ => {
                self.blocks.push(Vec::new());
                0
            }
        }
    }

    /// Makes sure the last block can hold `required <= block_size` elements,
    /// growing it by doubling (capped at `block_size`), and returns it.
    fn reserve_last(&mut self, required: usize) -> &mut Vec<T> {
        let block_size = self.block_size;
        let last = self.blocks.last_mut().expect("at least one block");
        let old_capacity = last.capacity();
        if old_capacity < required {
            let new_capacity = (old_capacity * 2).max(required).min(block_size);
            last.reserve_exact(new_capacity - last.len());
            self.allocated += (last.capacity() - old_capacity) * size_of::<T>();
        }
        last
    }

    /// Returns the element at `index`.
    ///
    /// # Panics
    /// If `index` is out of bounds.
    #[inline]
    pub fn get(&self, index: BlocksIndex) -> T {
        self.blocks[index.block_index()][index.index_in_block()]
    }

    /// Returns a mutable reference to the element at `index`.
    ///
    /// # Safety
    /// `index` must point to an element of this vector.
    #[inline]
    pub unsafe fn get_unchecked_mut(&mut self, index: BlocksIndex) -> &mut T {
        // SAFETY: guaranteed by the caller
        unsafe {
            self.blocks
                .get_unchecked_mut(index.block_index())
                .get_unchecked_mut(index.index_in_block())
        }
    }

    /// Returns all elements as one slice if there is at most one block, so
    /// hot loops can run on a flat slice exactly like today's accumulators.
    #[inline]
    pub fn as_single_block_mut(&mut self) -> Option<&mut [T]> {
        match self.blocks.as_mut_slice() {
            [] => Some(&mut []),
            [block] => Some(block.as_mut_slice()),
            _ => None,
        }
    }

    /// Returns the address of every block, for resolving many indices
    /// without going through the outer `Vec` each time.
    ///
    /// The returned [`BlockPtrs`] is invalidated by any later use of `self`.
    fn block_ptrs_mut(&mut self) -> BlockPtrs<T> {
        BlockPtrs {
            ptrs: self.blocks.iter_mut().map(|b| b.as_mut_ptr()).collect(),
        }
    }

    /// Grows the vector to `total_num_groups` elements (new ones are
    /// `starting_value`), then calls `update_fn` on the element of every
    /// row's group, skipping rows that are null in `nulls` or not selected by
    /// `opt_filter`.
    ///
    /// Chooses the loop once per call: with a single block it is a plain flat
    /// loop; with several blocks and no nulls or filter, the addresses of each
    /// chunk of rows are resolved before any of them is updated, so the cache
    /// misses of the updates don't wait on each other.
    ///
    /// `group_indices` must only point to the first `total_num_groups`
    /// groups, which is the contract of
    /// [`BlockedGroupsAccumulator::update_batch`] and
    /// [`BlockedGroupsAccumulator::merge_batch`].
    ///
    /// [`BlockedGroupsAccumulator::update_batch`]: datafusion_expr_common::blocked_groups_accumulator::BlockedGroupsAccumulator::update_batch
    /// [`BlockedGroupsAccumulator::merge_batch`]: datafusion_expr_common::blocked_groups_accumulator::BlockedGroupsAccumulator::merge_batch
    ///
    /// # Panics
    /// If an index in `group_indices` is not below `total_num_groups`.
    /// if there is an index in block that is larger than block size
    pub fn update<F>(
        &mut self,
        total_num_groups: usize,
        starting_value: T,
        group_indices: &[BlocksIndex],
        nulls: Option<&NullBuffer>,
        opt_filter: Option<&BooleanArray>,
        update_fn: F,
    ) where
        F: FnMut(&mut T),
    {
        self.grow_to(total_num_groups, starting_value);
        self.assert_in_bounds(group_indices);

        // SAFETY: grown to `total_num_groups` and checked above
        unsafe {
            self.update_unchecked(group_indices, nulls, opt_filter, update_fn);
        }
    }

    /// Same as [`Self::update`], but does not grow the vector and the caller guarantees that indices are in bounds.
    ///
    /// # Safety
    /// The behavior is undefined if any of the following are true:
    /// 1. `groups_indices` contain block index outside the number of blocks (e.g. you did not call `grow_to` first to the expected number of values)
    /// 2. `groups_indices` index in block is larger than block size
    /// 3. `groups_indices` index in block for last blocks is larger than the number of elements in the last block
    pub unsafe fn update_unchecked<F>(
        &mut self,
        group_indices: &[BlocksIndex],
        nulls: Option<&NullBuffer>,
        opt_filter: Option<&BooleanArray>,
        mut update_fn: F,
    ) where
        F: FnMut(&mut T),
    {
        if let Some(block) = self.as_single_block_mut() {
            accumulate_blocked_indices(group_indices, nulls, opt_filter, |index| {
                // SAFETY: indices are in bounds, guaranteed by the caller
                update_fn(unsafe { block.get_unchecked_mut(index.index_in_block()) })
            });
            return;
        }

        let ptrs = self.block_ptrs_mut();
        if nulls.is_none() && opt_filter.is_none() {
            let mut addrs = [std::ptr::null_mut::<T>(); RESOLVE_CHUNK];
            for chunk in group_indices.chunks(RESOLVE_CHUNK) {
                for (addr, &index) in addrs.iter_mut().zip(chunk) {
                    // SAFETY: indices are in bounds, guaranteed by the caller
                    *addr = unsafe { ptrs.ptr(index) };
                }
                for &addr in &addrs[..chunk.len()] {
                    // SAFETY: resolved above from a block that is not
                    // touched while `ptrs` lives
                    update_fn(unsafe { &mut *addr });
                }
            }
        } else {
            accumulate_blocked_indices(group_indices, nulls, opt_filter, |index| {
                // SAFETY: indices are in bounds, guaranteed by the caller
                update_fn(unsafe { &mut *ptrs.ptr(index) })
            });
        }
    }

    /// Grows the vector to `total_num_groups` elements (new ones are
    /// `starting_value`), then calls `update_fn` with the element of every
    /// row's group and the row's value in `values`.
    ///
    /// Chooses the loop once per call, like [`Self::update`].
    ///
    /// `group_indices` must only point to the first `total_num_groups`
    /// groups, which is the contract of
    /// [`BlockedGroupsAccumulator::update_batch`] and
    /// [`BlockedGroupsAccumulator::merge_batch`].
    ///
    /// [`BlockedGroupsAccumulator::update_batch`]: datafusion_expr_common::blocked_groups_accumulator::BlockedGroupsAccumulator::update_batch
    /// [`BlockedGroupsAccumulator::merge_batch`]: datafusion_expr_common::blocked_groups_accumulator::BlockedGroupsAccumulator::merge_batch
    ///
    /// # Panics
    /// If `values` and `group_indices` have different lengths, or an index in
    /// `group_indices` is not below `total_num_groups`.
    pub fn update_with<V, F>(
        &mut self,
        total_num_groups: usize,
        starting_value: T,
        group_indices: &[BlocksIndex],
        values: &[V],
        update_fn: F,
    ) where
        V: Copy,
        F: FnMut(&mut T, V),
    {
        self.grow_to(total_num_groups, starting_value);
        self.assert_in_bounds(group_indices);

        // SAFETY: grown to `total_num_groups` and checked above
        unsafe { self.update_with_unchecked(group_indices, values, update_fn) }
    }

    /// Same as [`Self::update_with`], but does not grow the vector and the caller guarantees that indices are in bounds.
    ///
    /// # Safety
    /// The behavior is undefined if any of the following are true:
    /// 1. `groups_indices` contain block index outside the number of blocks (e.g. you did not call `grow_to` first to the expected number of values)
    /// 2. `groups_indices` index in block is larger than block size
    /// 3. `groups_indices` index in block for last block is larger than the number of elements in the last block (e.g. you did not call `grow_to` first to the expected number of values)
    pub unsafe fn update_with_unchecked<V, F>(
        &mut self,
        group_indices: &[BlocksIndex],
        values: &[V],
        mut update_fn: F,
    ) where
        V: Copy,
        F: FnMut(&mut T, V),
    {
        assert_eq!(group_indices.len(), values.len());
        if let Some(block) = self.as_single_block_mut() {
            for (&index, &value) in group_indices.iter().zip(values) {
                // SAFETY: indices are in bounds, guaranteed by the caller
                update_fn(
                    unsafe { block.get_unchecked_mut(index.index_in_block()) },
                    value,
                );
            }
            return;
        }

        let ptrs = self.block_ptrs_mut();
        let mut addrs = [std::ptr::null_mut::<T>(); RESOLVE_CHUNK];
        for (indices, values) in group_indices
            .chunks(RESOLVE_CHUNK)
            .zip(values.chunks(RESOLVE_CHUNK))
        {
            for (addr, &index) in addrs.iter_mut().zip(indices) {
                // SAFETY: indices are in bounds, guaranteed by the caller
                *addr = unsafe { ptrs.ptr(index) };
            }
            for (&addr, &value) in addrs.iter().zip(values) {
                // SAFETY: resolved above from a block that is not touched
                // while `ptrs` lives
                update_fn(unsafe { &mut *addr }, value);
            }
        }
    }

    /// Checks that every index points to an element, so the update loops
    /// can index without bounds checks. Every block but the last holds
    /// `block_size` elements, see [`BlocksIndex::all_in_bounds`].
    #[inline]
    fn assert_in_bounds(&self, group_indices: &[BlocksIndex]) {
        assert!(
            BlocksIndex::all_in_bounds(group_indices, self.block_size, self.len),
            "group index out of bounds"
        );
    }

    /// Removes and returns the first block, or `None` if empty. Elements
    /// after it move down by the removed block's length.
    pub fn take_next_block(&mut self) -> Option<Vec<T>> {
        if self.blocks.is_empty() {
            return None;
        }
        // There are few blocks, so shifting the outer `Vec` is cheap
        let block = self.blocks.remove(0);
        self.len -= block.len();
        self.allocated -= block.capacity() * size_of::<T>();
        Some(block)
    }

    /// Removes and returns all blocks, leaving the vector empty.
    pub fn take_all(&mut self) -> Vec<Vec<T>> {
        self.len = 0;
        self.allocated = 0;
        std::mem::take(&mut self.blocks)
    }

    /// Removes and returns the first `n` elements; the remaining elements
    /// move down by `n`, like `EmitTo::First` on a flat `Vec`.
    ///
    /// # Panics
    /// If `n > self.len()`.
    pub fn take_first(&mut self, n: usize) -> Vec<T> {
        assert!(n <= self.len, "take_first({n}) with len {}", self.len);
        if n == 0 {
            return Vec::new();
        }

        // Move out whole leading blocks
        let mut taken = Vec::new();
        while let Some(first) = self.blocks.first()
            && n - taken.len() >= first.len()
        {
            let block = self.blocks.remove(0);
            if taken.is_empty() {
                taken = block;
                taken.reserve_exact(n - taken.len());
            } else {
                taken.extend(block);
            }
        }

        // Take the rest from the (now) first block, and shift every following
        // block down by `rest` so all blocks but the last stay full
        let rest = n - taken.len();
        if rest > 0 {
            taken.extend(self.blocks[0].drain(..rest));
            for i in 1..self.blocks.len() {
                let (previous, current) = self.blocks.split_at_mut(i);
                let current = &mut current[0];
                let moved = rest.min(current.len());
                previous[i - 1].extend(current.drain(..moved));
            }
            if self.blocks.last().is_some_and(|b| b.is_empty()) {
                self.blocks.pop();
            }
        }

        self.len -= n;
        if self.blocks.is_empty() {
            self.blocks = Vec::new();
        }
        // Rare path: recompute instead of tracking each block
        self.allocated = self
            .blocks
            .iter()
            .map(|b| b.capacity() * size_of::<T>())
            .sum();
        taken
    }

    /// Bytes allocated by this vector, O(1).
    #[inline]
    pub fn allocated_size(&self) -> usize {
        self.allocated + self.blocks.capacity() * size_of::<Vec<T>>()
    }
}

/// Raw addresses of the blocks of a [`BlockedVec`], see
/// [`BlockedVec::block_ptrs_mut`].
///
/// The pointers are invalidated by any mutation (or other use) of the
/// [`BlockedVec`] they were created from, so only keep a `BlockPtrs` for the
/// duration of one hot loop.
#[derive(Debug)]
struct BlockPtrs<T> {
    ptrs: Vec<*mut T>,
}

impl<T> BlockPtrs<T> {
    /// Returns the address of the element at `index`.
    ///
    /// # Safety
    /// `index` must point to an element of the [`BlockedVec`] when this
    /// `BlockPtrs` was created, and that [`BlockedVec`] must not have been
    /// used since.
    #[inline]
    unsafe fn ptr(&self, index: BlocksIndex) -> *mut T {
        // SAFETY: guaranteed by the caller
        unsafe {
            self.ptrs
                .get_unchecked(index.block_index())
                .add(index.index_in_block())
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn stays_flat_until_first_block_fills() {
        let mut v = BlockedVec::<i64>::new(4);
        v.grow_to(3, 0);
        assert_eq!(v.num_blocks(), 1);
        assert!(v.as_single_block_mut().is_some());
        v.grow_to(4, 0);
        assert_eq!(v.num_blocks(), 1);
        v.grow_to(5, 0);
        assert_eq!(v.num_blocks(), 2);
        assert!(v.as_single_block_mut().is_none());
    }

    #[test]
    fn indexing_across_blocks() {
        let mut v = BlockedVec::<i64>::new(4);
        for i in 0..10 {
            assert_eq!(v.push(i as i64), BlocksIndex::from_flat(i, 4));
        }
        for i in 0..10 {
            assert_eq!(v.get(BlocksIndex::from_flat(i, 4)), i as i64);
        }
        let ptrs = v.block_ptrs_mut();
        unsafe { *ptrs.ptr(BlocksIndex::from_flat(9, 4)) += 100 };
        assert_eq!(v.get(BlocksIndex::from_flat(9, 4)), 109);
    }

    #[test]
    fn take_next_block_and_all() {
        let mut v = BlockedVec::<i64>::new(4);
        (0..10).for_each(|i| {
            v.push(i);
        });
        assert_eq!(v.take_next_block(), Some(vec![0, 1, 2, 3]));
        assert_eq!(v.len(), 6);
        assert_eq!(v.take_all(), vec![vec![4, 5, 6, 7], vec![8, 9]]);
        assert!(v.is_empty());
        assert_eq!(v.take_next_block(), None);
        assert_eq!(v.allocated_size(), 0);
    }

    #[test]
    fn take_first_shifts_across_blocks() {
        let mut v = BlockedVec::<i64>::new(4);
        (0..10).for_each(|i| {
            v.push(i);
        });
        assert_eq!(v.take_first(3), vec![0, 1, 2]);
        assert_eq!(v.len(), 7);
        let rest: Vec<i64> = (0..7)
            .map(|i| v.get(BlocksIndex::from_flat(i, 4)))
            .collect();
        assert_eq!(rest, vec![3, 4, 5, 6, 7, 8, 9]);
        assert_eq!(v.num_blocks(), 2);
    }

    #[test]
    fn allocated_size_tracks_blocks() {
        let mut v = BlockedVec::<i64>::new(4);
        v.grow_to(9, 0);
        assert!(v.allocated_size() >= 9 * 8);
        let before = v.allocated_size();
        v.take_next_block();
        assert!(v.allocated_size() < before);
    }

    #[test]
    fn grow_to_spills_into_several_blocks_at_once() {
        let mut v = BlockedVec::<i64>::new(4);
        v.grow_to(2, 7);
        assert_eq!(v.num_blocks(), 1);
        v.grow_to(11, 1);
        assert_eq!(v.len(), 11);
        assert_eq!(v.num_blocks(), 3);
        let all: Vec<i64> = (0..11)
            .map(|i| v.get(BlocksIndex::from_flat(i, 4)))
            .collect();
        assert_eq!(all, vec![7, 7, 1, 1, 1, 1, 1, 1, 1, 1, 1]);
        // Shrinking is a no-op
        v.grow_to(3, 0);
        assert_eq!(v.len(), 11);
        assert!(v.allocated_size() >= 3 * 4 * size_of::<i64>());
        let blocks = v.take_all();
        assert_eq!(blocks, vec![vec![7, 7, 1, 1], vec![1; 4], vec![1; 3]]);
        // Blocks grow by doubling up to the block size, so the last one does
        // not allocate a whole block
        assert!(blocks[..2].iter().all(|b| b.capacity() == 4));
        assert_eq!(blocks[2].capacity(), 3);
    }

    #[test]
    fn take_first_whole_first_block() {
        let mut v = BlockedVec::<i64>::new(4);
        (0..10).for_each(|i| {
            v.push(i);
        });
        assert_eq!(v.take_first(4), vec![0, 1, 2, 3]);
        assert_eq!(v.len(), 6);
        assert_eq!(v.num_blocks(), 2);
        let rest: Vec<i64> = (0..6)
            .map(|i| v.get(BlocksIndex::from_flat(i, 4)))
            .collect();
        assert_eq!(rest, vec![4, 5, 6, 7, 8, 9]);
        // All remaining elements, emptying the vec
        assert_eq!(v.take_first(6), vec![4, 5, 6, 7, 8, 9]);
        assert!(v.is_empty());
        assert_eq!(v.num_blocks(), 0);
        assert_eq!(v.allocated_size(), 0);
        // Still usable afterwards
        assert_eq!(v.push(42), BlocksIndex::new(0, 0));
        assert_eq!(v.get(BlocksIndex::from_flat(0, 4)), 42);
    }

    #[test]
    fn take_first_within_single_block() {
        let mut v = BlockedVec::<i64>::new(8);
        (0..5).for_each(|i| {
            v.push(i);
        });
        assert_eq!(v.take_first(2), vec![0, 1]);
        assert_eq!(v.as_single_block_mut().unwrap(), &mut [2, 3, 4]);
    }

    #[test]
    fn allocated_size_zero_after_take_all() {
        let mut v = BlockedVec::<i64>::new(4);
        v.grow_to(3, 0);
        assert!(v.allocated_size() >= 3 * 8);
        let _ = v.take_all();
        assert_eq!(v.allocated_size(), 0);
        v.grow_to(9, 0);
        assert_eq!(v.num_blocks(), 3);
        let _ = v.take_all();
        assert_eq!(v.allocated_size(), 0);
        assert_eq!(v.num_blocks(), 0);
    }

    fn indices(flats: &[usize]) -> Vec<BlocksIndex> {
        flats
            .iter()
            .map(|&f| BlocksIndex::from_flat(f, 4))
            .collect()
    }

    fn values(v: &BlockedVec<i64>) -> Vec<i64> {
        (0..v.len())
            .map(|f| v.get(BlocksIndex::from_flat(f, 4)))
            .collect()
    }

    #[test]
    fn update_single_and_multi_block() {
        let mut v = BlockedVec::<i64>::new(4);
        v.update(3, 0, &indices(&[0, 2, 2]), None, None, |x| *x += 1);
        assert_eq!(values(&v), vec![1, 0, 2]);

        // several blocks, more rows than one resolve chunk
        let rows: Vec<usize> = (0..RESOLVE_CHUNK * 2 + 3).map(|i| i % 10).collect();
        v.update(10, 0, &indices(&rows), None, None, |x| *x += 1);
        let expected: Vec<i64> = (0..10)
            .map(|g| rows.iter().filter(|&&r| r == g).count() as i64)
            .zip([1, 0, 2, 0, 0, 0, 0, 0, 0, 0])
            .map(|(a, b)| a + b)
            .collect();
        assert_eq!(values(&v), expected);
    }

    #[test]
    fn update_skips_nulls_and_filtered_rows() {
        let mut v = BlockedVec::<i64>::new(4);
        let nulls = NullBuffer::from(vec![true, false, true, true]);
        let filter = BooleanArray::from(vec![true, true, false, true]);
        v.update(
            9,
            0,
            &indices(&[0, 5, 8, 8]),
            Some(&nulls),
            Some(&filter),
            |x| *x += 1,
        );
        assert_eq!(values(&v), vec![1, 0, 0, 0, 0, 0, 0, 0, 1]);
    }

    #[test]
    fn update_with_values_across_blocks() {
        let mut v = BlockedVec::<i64>::new(4);
        let rows: Vec<usize> = (0..RESOLVE_CHUNK + 5).map(|i| i % 10).collect();
        let vals: Vec<i64> = (0..rows.len() as i64).collect();
        v.update_with(10, 0, &indices(&rows), &vals, |x, val| *x += val);
        let expected: Vec<i64> = (0..10)
            .map(|g| {
                rows.iter()
                    .zip(&vals)
                    .filter(|(r, _)| **r == g)
                    .map(|(_, val)| val)
                    .sum()
            })
            .collect();
        assert_eq!(values(&v), expected);
    }

    #[test]
    #[should_panic(expected = "block_size must be positive")]
    fn zero_block_size_panics() {
        let _ = BlockedVec::<i64>::new(0);
    }

    #[test]
    #[should_panic(expected = "group index out of bounds")]
    fn update_out_of_bounds_panics() {
        let mut v = BlockedVec::<u64>::new(4);
        v.update(1, 0, &[BlocksIndex::new(0, 1)], None, None, |v| *v += 1);
    }

    #[test]
    #[should_panic(expected = "group index out of bounds")]
    fn update_with_out_of_bounds_panics() {
        let mut v = BlockedVec::<u64>::new(4);
        v.update_with(6, 0, &[BlocksIndex::new(0, 4)], &[1], |v, x| *v += x);
    }
}
