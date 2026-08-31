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

//! Per-group accumulator state, held in one allocation while it is small and in
//! fixed-size blocks once it is large.

use std::mem::size_of;

use datafusion_expr_common::groups_accumulator::EmitTo;

/// Groups per block once blocked. Sixteen batches' worth, and a megabyte for an
/// eight byte state, so a block can be handed to Arrow as a buffer of its own.
pub const BLOCK_LEN: usize = 1 << 17;

/// Groups a state may reach before it is worth holding in blocks.
///
/// Below this the state is one plain `Vec`: growing it copies at most this many elements
/// in total, which is not worth paying a second load on every group update to avoid.
pub const THRESHOLD_LEN: usize = 1 << 20;

const BLOCK_SHIFT: u32 = BLOCK_LEN.trailing_zeros();
const BLOCK_MASK: usize = BLOCK_LEN - 1;

/// The block and offset a group index falls in.
#[inline]
pub fn block_offset(index: usize) -> (usize, usize) {
    (index >> BLOCK_SHIFT, index & BLOCK_MASK)
}

enum Storage<T> {
    /// One allocation. Growing copies, which is cheap while it is small.
    Flat(Vec<T>),
    /// Fixed-size blocks. Growing appends a block and never moves what is already there.
    Blocked(Vec<Vec<T>>),
}

/// A growable sequence of per-group state, addressed by group index.
///
/// Growing one allocation copies everything already in it, and for a grouping with
/// millions of groups that copying is a large part of what the aggregate does. Reaching
/// through a block index costs a second load on *every* group update, though, so blocks
/// are only worth it once the copying they avoid outweighs the lookups they add: this
/// stays flat up to [`THRESHOLD_LEN`] groups and blocks above it.
///
/// Callers should take [`Self::storage_mut`] once and loop inside the arm rather than
/// indexing through this type per group, so the representation is resolved once per
/// batch instead of once per update.
pub struct BlockedVec<T> {
    storage: Storage<T>,
    len: usize,
}

impl<T: Clone> Default for BlockedVec<T> {
    fn default() -> Self {
        Self::new()
    }
}

impl<T: Clone> std::fmt::Debug for BlockedVec<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("BlockedVec")
            .field("len", &self.len)
            .field("blocked", &self.is_blocked())
            .finish()
    }
}

/// A borrowed view of the state, taken once per batch so the update loop does not
/// re-check which representation is in use on every group.
pub enum StorageMut<'a, T> {
    Flat(&'a mut [T]),
    Blocked(&'a mut [Vec<T>]),
}

impl<T: Clone> BlockedVec<T> {
    pub fn new() -> Self {
        Self {
            storage: Storage::Flat(Vec::new()),
            len: 0,
        }
    }

    pub fn len(&self) -> usize {
        self.len
    }

    pub fn is_empty(&self) -> bool {
        self.len == 0
    }

    /// Whether the state has moved to blocks.
    pub fn is_blocked(&self) -> bool {
        matches!(self.storage, Storage::Blocked(_))
    }

    /// Bytes held, counting space reserved but not filled.
    pub fn capacity_bytes(&self) -> usize {
        let elements = match &self.storage {
            Storage::Flat(values) => values.capacity(),
            Storage::Blocked(blocks) => blocks.iter().map(|b| b.capacity()).sum(),
        };
        elements * size_of::<T>()
    }

    /// Resolves the representation once, so the caller's loop is free of the check.
    #[inline]
    pub fn storage_mut(&mut self) -> StorageMut<'_, T> {
        match &mut self.storage {
            Storage::Flat(values) => StorageMut::Flat(values.as_mut_slice()),
            Storage::Blocked(blocks) => StorageMut::Blocked(blocks.as_mut_slice()),
        }
    }

    /// Grows to `total_num_groups`, filling with `value`.
    pub fn resize(&mut self, total_num_groups: usize, value: T) {
        if total_num_groups <= self.len {
            self.truncate(total_num_groups);
            return;
        }
        if total_num_groups > THRESHOLD_LEN && !self.is_blocked() {
            self.switch_to_blocked();
        }
        match &mut self.storage {
            Storage::Flat(values) => values.resize(total_num_groups, value),
            Storage::Blocked(blocks) => {
                let mut len = self.len;
                while len < total_num_groups {
                    let (block, offset) = block_offset(len);
                    if block == blocks.len() {
                        blocks.push(Vec::with_capacity(BLOCK_LEN));
                    }
                    let take = std::cmp::min(BLOCK_LEN - offset, total_num_groups - len);
                    blocks[block].resize(offset + take, value.clone());
                    len += take;
                }
            }
        }
        self.len = total_num_groups;
    }

    /// Chops the single allocation into blocks. Paid once, at the threshold.
    fn switch_to_blocked(&mut self) {
        let Storage::Flat(values) = &mut self.storage else {
            return;
        };
        let mut rest = std::mem::take(values);
        let mut blocks: Vec<Vec<T>> = Vec::with_capacity(rest.len() / BLOCK_LEN + 1);
        while rest.len() > BLOCK_LEN {
            let tail = rest.split_off(BLOCK_LEN);
            blocks.push(rest);
            rest = tail;
        }
        // The last piece keeps room to fill out its block.
        let mut last = Vec::with_capacity(BLOCK_LEN);
        last.extend(rest);
        blocks.push(last);
        self.storage = Storage::Blocked(blocks);
    }

    fn truncate(&mut self, len: usize) {
        if len >= self.len {
            return;
        }
        match &mut self.storage {
            Storage::Flat(values) => values.truncate(len),
            Storage::Blocked(blocks) => {
                blocks.truncate(len.div_ceil(BLOCK_LEN));
                if let Some(last) = blocks.last_mut() {
                    let offset = len & BLOCK_MASK;
                    if offset != 0 {
                        last.truncate(offset);
                    }
                }
            }
        }
        self.len = len;
    }

    #[inline]
    pub fn get(&self, index: usize) -> Option<&T> {
        match &self.storage {
            Storage::Flat(values) => values.get(index),
            Storage::Blocked(blocks) => {
                let (block, offset) = block_offset(index);
                blocks.get(block)?.get(offset)
            }
        }
    }

    /// Everything in one allocation, leaving this empty.
    ///
    /// While flat this hands over the allocation as it stands. Once blocked it copies,
    /// which is what a per-block emit would avoid.
    pub fn take_contiguous(&mut self) -> Vec<T> {
        let storage = std::mem::replace(&mut self.storage, Storage::Flat(Vec::new()));
        self.len = 0;
        match storage {
            Storage::Flat(values) => values,
            Storage::Blocked(mut blocks) => match blocks.len() {
                0 => Vec::new(),
                1 => blocks.pop().unwrap_or_default(),
                _ => {
                    let mut out =
                        Vec::with_capacity(blocks.iter().map(|b| b.len()).sum());
                    for block in &blocks {
                        out.extend(block.iter().cloned());
                    }
                    out
                }
            },
        }
    }

    /// Removes the first `n` elements, shifting the rest down.
    pub fn take_first(&mut self, n: usize) -> Vec<T> {
        let mut all = self.take_contiguous();
        let rest = all.split_off(std::cmp::min(n, all.len()));
        let len = rest.len();
        self.storage = Storage::Flat(rest);
        self.len = len;
        if len > THRESHOLD_LEN {
            self.switch_to_blocked();
        }
        all
    }
}

/// [`EmitTo`] applied to a [`BlockedVec`], returning one contiguous allocation.
pub fn take_blocked<T: Clone>(values: &mut BlockedVec<T>, emit_to: EmitTo) -> Vec<T> {
    match emit_to {
        EmitTo::All => values.take_contiguous(),
        EmitTo::First(n) => values.take_first(n),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn stays_flat_below_threshold() {
        let mut v: BlockedVec<u64> = BlockedVec::new();
        v.resize(THRESHOLD_LEN, 0);
        assert!(!v.is_blocked(), "should still be one allocation");
        v.resize(THRESHOLD_LEN + 1, 0);
        assert!(v.is_blocked(), "should have switched to blocks");
    }

    #[test]
    fn values_survive_the_switch() {
        let mut v: BlockedVec<u64> = BlockedVec::new();
        let n = THRESHOLD_LEN + BLOCK_LEN + 7;
        v.resize(1000, 0);
        for i in 0..1000 {
            unsafe { *v.get_unchecked_mut_for_test(i) = i as u64 };
        }
        v.resize(n, 0);
        for i in 1000..n {
            unsafe { *v.get_unchecked_mut_for_test(i) = i as u64 };
        }
        assert!(v.is_blocked());
        assert_eq!(v.len(), n);
        for i in (0..n).step_by(997) {
            assert_eq!(v.get(i).copied(), Some(i as u64), "at {i}");
        }
        let flat = v.take_contiguous();
        assert_eq!(flat.len(), n);
        assert!(flat.iter().enumerate().all(|(i, x)| *x == i as u64));
    }

    #[test]
    fn resize_keeps_existing_values() {
        let mut v: BlockedVec<u64> = BlockedVec::new();
        v.resize(10, 1);
        v.resize(THRESHOLD_LEN + 5, 2);
        assert_eq!(v.get(0).copied(), Some(1));
        assert_eq!(v.get(9).copied(), Some(1));
        assert_eq!(v.get(10).copied(), Some(2));
        assert_eq!(v.len(), THRESHOLD_LEN + 5);
    }

    #[test]
    fn take_first_shifts_down() {
        let mut v: BlockedVec<u64> = BlockedVec::new();
        let n = THRESHOLD_LEN + 100;
        v.resize(n, 0);
        for i in 0..n {
            unsafe { *v.get_unchecked_mut_for_test(i) = i as u64 };
        }
        let taken = v.take_first(50);
        assert_eq!(taken.len(), 50);
        assert!(taken.iter().enumerate().all(|(i, x)| *x == i as u64));
        assert_eq!(v.len(), n - 50);
        assert_eq!(v.get(0).copied(), Some(50));
        assert_eq!(v.get(v.len() - 1).copied(), Some(n as u64 - 1));
    }

    impl<T: Clone> BlockedVec<T> {
        /// Indexing one group at a time, for tests only; the accumulators go through
        /// [`BlockedVec::storage_mut`] instead.
        ///
        /// # Safety
        /// `index` must be less than [`BlockedVec::len`].
        unsafe fn get_unchecked_mut_for_test(&mut self, index: usize) -> &mut T {
            match &mut self.storage {
                Storage::Flat(values) => unsafe { values.get_unchecked_mut(index) },
                Storage::Blocked(blocks) => {
                    let (block, offset) = block_offset(index);
                    unsafe { blocks.get_unchecked_mut(block).get_unchecked_mut(offset) }
                }
            }
        }
    }
}
