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

//! [`BlockedVec`]: per-group state stored in fixed-size blocks.

use std::marker::PhantomData;
use std::mem::size_of;

use datafusion_common::utils::split_vec_min_alloc;

/// Number of groups per block, as a power of two: `1 << DEFAULT_BLOCK_SHIFT`.
///
/// Every table with at most this many groups is a single block, which is
/// read and updated exactly like a flat `Vec`. Larger tables pay one extra
/// address-resolution pass per batch (see [`AddressResolver`]).
pub const DEFAULT_BLOCK_SHIFT: u32 = 20;

/// Per-group values stored in blocks of `1 << SHIFT` elements: group `g`
/// lives in block `g >> SHIFT` at offset `g & ((1 << SHIFT) - 1)`.
///
/// Blocked storage lets a hash aggregation grow, emit and free its state one
/// block at a time instead of as one contiguous allocation. The cost that
/// made earlier blocked designs slower is *how* the state is addressed in the
/// update loop, not the blocks themselves: `blocks[g >> SHIFT][g & MASK]` is
/// a dependent load, so the CPU cannot issue the next group's cache miss
/// until the block pointer has resolved, and the flat `Vec` wins on
/// memory-level parallelism alone. [`AddressResolver`] resolves the
/// addresses of a chunk of rows in a separate, independent pass first, after
/// which the update loop issues its misses exactly as the flat loop does.
#[derive(Debug)]
pub struct BlockedVec<T, const SHIFT: u32 = DEFAULT_BLOCK_SHIFT> {
    blocks: Vec<Vec<T>>,
    len: usize,
}

impl<T, const SHIFT: u32> Default for BlockedVec<T, SHIFT> {
    fn default() -> Self {
        Self::new()
    }
}

impl<T, const SHIFT: u32> BlockedVec<T, SHIFT> {
    /// Elements per block.
    pub const BLOCK_LEN: usize = 1 << SHIFT;
    const MASK: usize = Self::BLOCK_LEN - 1;

    pub const fn new() -> Self {
        Self {
            blocks: Vec::new(),
            len: 0,
        }
    }

    /// Reserves room for `capacity` elements in the first block.
    pub fn with_capacity(capacity: usize) -> Self {
        Self {
            blocks: vec![Vec::with_capacity(capacity.min(Self::BLOCK_LEN))],
            len: 0,
        }
    }

    pub fn len(&self) -> usize {
        self.len
    }

    pub fn is_empty(&self) -> bool {
        self.len == 0
    }

    /// Number of blocks currently allocated.
    pub fn num_blocks(&self) -> usize {
        self.blocks.len()
    }

    /// Bytes allocated by the blocks, not including `self`.
    pub fn allocated_size(&self) -> usize {
        self.blocks
            .iter()
            .map(|b| b.capacity() * size_of::<T>())
            .sum()
    }

    #[inline]
    fn block_of(index: usize) -> (usize, usize) {
        (index >> SHIFT, index & Self::MASK)
    }

    /// The whole storage as one slice when it fits in a single block, so the
    /// common case is read and updated exactly like a flat `Vec`.
    #[inline]
    pub fn as_single_block_mut(&mut self) -> Option<&mut [T]> {
        match self.blocks.as_mut_slice() {
            [] => Some(&mut []),
            [block] => Some(block.as_mut_slice()),
            _ => None,
        }
    }

    /// Reads the element at `index`.
    #[inline]
    pub fn get(&self, index: usize) -> &T {
        let (block, offset) = Self::block_of(index);
        &self.blocks[block][offset]
    }

    /// Mutable access through the dependent `block -> offset` lookup. For
    /// batch updates prefer [`Self::address_resolver`].
    #[inline]
    pub fn get_mut(&mut self, index: usize) -> &mut T {
        let (block, offset) = Self::block_of(index);
        &mut self.blocks[block][offset]
    }

    pub fn push(&mut self, value: T) {
        if self.len & Self::MASK == 0 && self.len >> SHIFT == self.blocks.len() {
            self.blocks.push(Vec::new());
        }
        self.blocks[self.len >> SHIFT].push(value);
        self.len += 1;
    }

    /// Removes every element, keeping the first block's allocation.
    pub fn clear(&mut self) {
        self.blocks.truncate(1);
        if let Some(block) = self.blocks.first_mut() {
            block.clear();
        }
        self.len = 0;
    }

    /// Shrinks the retained allocation to `capacity` elements at most.
    pub fn shrink_to(&mut self, capacity: usize) {
        if let Some(block) = self.blocks.first_mut() {
            block.shrink_to(capacity.min(Self::BLOCK_LEN));
        }
    }

    /// Takes the elements out in order. A single block is moved without a
    /// copy; several blocks are concatenated.
    pub fn take_all(&mut self) -> Vec<T> {
        self.len = 0;
        match self.blocks.len() {
            0 => Vec::new(),
            1 => std::mem::take(&mut self.blocks[0]),
            _ => {
                let mut all =
                    Vec::with_capacity(self.blocks.iter().map(|b| b.len()).sum());
                for block in self.blocks.drain(..) {
                    all.extend(block);
                }
                all
            }
        }
    }

    /// Takes the first `n` elements, shifting the rest down so the element
    /// at `n` becomes element `0`.
    pub fn take_first(&mut self, n: usize) -> Vec<T> {
        if n == self.len {
            return self.take_all();
        }
        if self.blocks.len() > 1 && n == Self::BLOCK_LEN {
            // A whole first block is moved out; the rest is already packed.
            self.len -= n;
            return self.blocks.remove(0);
        }
        if self.blocks.len() <= 1 {
            let taken = match self.blocks.first_mut() {
                Some(block) => split_vec_min_alloc(block, n),
                None => Vec::new(),
            };
            self.len -= taken.len();
            return taken;
        }
        // Whole blocks come first, so they are moved rather than copied.
        let whole = n >> SHIFT;
        let mut taken: Vec<T> = Vec::with_capacity(n);
        for block in self.blocks.drain(..whole) {
            taken.extend(block);
        }
        let rest = n & Self::MASK;
        if rest > 0 {
            taken.extend(self.blocks[0].drain(..rest));
        }
        self.len -= n;
        // Blocks after the first are now offset by `rest`; re-pack.
        if rest > 0 && self.blocks.len() > 1 {
            let mut remaining = std::mem::take(&mut self.blocks);
            let mut carry: Vec<T> = remaining.drain(1..).flatten().collect();
            let first = &mut remaining[0];
            let fill = Self::BLOCK_LEN - first.len();
            first.extend(carry.drain(..fill.min(carry.len())));
            self.blocks = remaining;
            let mut carry = carry.into_iter();
            loop {
                let block: Vec<T> = carry.by_ref().take(Self::BLOCK_LEN).collect();
                if block.is_empty() {
                    break;
                }
                self.blocks.push(block);
            }
        }
        taken
    }

    /// Resolves group indices to element addresses for a batch update; see
    /// [`AddressResolver`].
    pub fn address_resolver(&mut self) -> AddressResolver<'_, T, SHIFT> {
        AddressResolver {
            bases: self.blocks.iter_mut().map(|b| b.as_mut_ptr()).collect(),
            len: self.len,
            _lifetime: PhantomData,
        }
    }
}

impl<T: Copy, const SHIFT: u32> BlockedVec<T, SHIFT> {
    /// Grows to `new_len` elements, filling with `value`; blocks are added
    /// as needed and the storage never shrinks.
    ///
    /// Every block grows by doubling like a `Vec` until it is full, so only
    /// the last block ever carries growth slack and the allocated size stays
    /// proportional to the number of groups.
    pub fn resize(&mut self, new_len: usize, value: T) {
        while self.len < new_len {
            let block = self.len >> SHIFT;
            if block == self.blocks.len() {
                self.blocks.push(Vec::new());
            }
            let block = &mut self.blocks[block];
            let target = (new_len - (self.len - block.len())).min(Self::BLOCK_LEN);
            block.resize(target, value);
            self.len = (self.len & !Self::MASK) + block.len();
        }
    }
}

/// Turns group indices into the addresses of their elements, for
/// `accumulate_resolved` in the `accumulate` module.
///
/// The block base table is a few pointers and stays in L1, so resolving a
/// chunk of rows is one short independent pass; the update loop that follows
/// walks the resolved addresses, and its cache misses are as independent of
/// one another as a flat `values[g]` loop's are. That is what the dependent
/// `blocks[g >> SHIFT][g & MASK]` lookup of [`BlockedVec::get_mut`] loses.
pub struct AddressResolver<'a, T, const SHIFT: u32> {
    bases: Vec<*mut T>,
    len: usize,
    _lifetime: PhantomData<&'a mut T>,
}

impl<T, const SHIFT: u32> AddressResolver<'_, T, SHIFT> {
    const MASK: usize = (1 << SHIFT) - 1;

    /// Fills `addrs[i]` with the address of element `group_indices[i]` and
    /// starts fetching each element's cache line, so that by the time the
    /// update loop reaches a row its state is on the way in.
    ///
    /// Every group index must be below the vector's length, as for any
    /// group index passed to an accumulator. The pointers stay valid for the
    /// resolver's lifetime, during which it borrows the blocks mutably.
    #[inline]
    pub fn resolve(&self, group_indices: &[usize], addrs: &mut [*mut T]) {
        assert!(addrs.len() >= group_indices.len());
        for (addr, &group) in addrs.iter_mut().zip(group_indices) {
            debug_assert!(group < self.len, "group index {group} out of bounds");
            // SAFETY: `group < len`, so its block exists and, as only the
            // last block can be partially filled, `group & MASK` is within it.
            let element = unsafe {
                self.bases
                    .get_unchecked(group >> SHIFT)
                    .add(group & Self::MASK)
            };
            prefetch_for_write(element);
            *addr = element;
        }
    }
}

/// Hints the CPU to fetch the cache line holding `ptr` for a coming write.
///
/// Issued from the resolve pass, where the address is known without any
/// dependent load, this overlaps the state's memory latency with the rest of
/// the pass; it is a hint only and never faults. A no-op on targets without
/// a stable prefetch instruction.
#[inline(always)]
fn prefetch_for_write<T>(ptr: *mut T) {
    #[cfg(target_arch = "x86_64")]
    // SAFETY: `_mm_prefetch` is a hint and does not access memory.
    unsafe {
        std::arch::x86_64::_mm_prefetch::<{ std::arch::x86_64::_MM_HINT_T0 }>(
            ptr as *const i8,
        )
    }
    #[cfg(target_arch = "aarch64")]
    // SAFETY: `prfm` is a hint that reads no memory and has no side effects
    // on registers or flags.
    unsafe {
        std::arch::asm!(
            "prfm pstl1keep, [{0}]",
            in(reg) ptr,
            options(nostack, preserves_flags, readonly)
        );
    }
    #[cfg(not(any(target_arch = "x86_64", target_arch = "aarch64")))]
    let _ = ptr;
}

#[cfg(test)]
mod tests {
    use super::*;

    type Small = BlockedVec<u32, 2>; // four elements per block

    fn filled(n: usize) -> Small {
        let mut v = Small::new();
        for i in 0..n as u32 {
            v.push(i);
        }
        v
    }

    fn contents(v: &Small) -> Vec<u32> {
        (0..v.len()).map(|i| *v.get(i)).collect()
    }

    #[test]
    fn push_resize_and_index_across_blocks() {
        let mut v = filled(6);
        assert_eq!(v.num_blocks(), 2);
        assert_eq!(contents(&v), (0..6).collect::<Vec<_>>());
        v.resize(11, 9);
        assert_eq!(v.len(), 11);
        assert_eq!(v.num_blocks(), 3);
        assert_eq!(contents(&v), vec![0, 1, 2, 3, 4, 5, 9, 9, 9, 9, 9]);
        *v.get_mut(10) = 7;
        assert_eq!(*v.get(10), 7);
        assert!(v.as_single_block_mut().is_none());
        assert!(filled(4).as_single_block_mut().is_some());
    }

    #[test]
    fn blocks_grow_by_doubling_not_by_block_len() {
        let mut v = BlockedVec::<u64>::new();
        v.resize(1000, 0);
        assert_eq!(v.num_blocks(), 1);
        assert!(v.allocated_size() < 2 * 1000 * size_of::<u64>());
        v.resize(BlockedVec::<u64>::BLOCK_LEN + 1, 0);
        assert_eq!(v.num_blocks(), 2);
        assert!(
            v.allocated_size() < (BlockedVec::<u64>::BLOCK_LEN + 8) * size_of::<u64>()
        );
    }

    #[test]
    fn take_first_block_moves_it_without_copying() {
        let mut v = Small::new();
        for i in 0..10u32 {
            v.push(i);
        }
        let before = v.allocated_size();
        let first = v.take_first(Small::BLOCK_LEN);
        assert_eq!(first, vec![0, 1, 2, 3]);
        assert_eq!(v.len(), 6);
        assert_eq!(contents(&v), vec![4, 5, 6, 7, 8, 9]);
        assert_eq!(
            v.allocated_size(),
            before - first.capacity() * size_of::<u32>()
        );
        // The last, partial block is handed over whole as well.
        assert_eq!(v.take_first(4), vec![4, 5, 6, 7]);
        assert_eq!(v.take_first(2), vec![8, 9]);
        assert!(v.is_empty());
    }

    #[test]
    fn take_all_and_take_first_keep_order() {
        assert_eq!(filled(10).take_all(), (0..10).collect::<Vec<_>>());
        for n in 0..=10 {
            let mut v = filled(10);
            assert_eq!(v.take_first(n), (0..n as u32).collect::<Vec<_>>());
            assert_eq!(v.len(), 10 - n);
            assert_eq!(contents(&v), (n as u32..10).collect::<Vec<_>>());
            // The layout is packed again: pushes land after the last element.
            v.push(99);
            assert_eq!(*v.get(10 - n), 99);
        }
    }

    #[test]
    fn resolver_addresses_every_block() {
        let groups: Vec<usize> = (0..5000).map(|i| (i * 7) % 11).collect();
        let mut v = Small::new();
        v.resize(11, 0);
        let resolver = v.address_resolver();
        let mut addrs = vec![std::ptr::null_mut(); 64];
        for groups in groups.chunks(64) {
            resolver.resolve(groups, &mut addrs);
            for &addr in &addrs[..groups.len()] {
                // SAFETY: resolved from live blocks the resolver borrows
                unsafe { *addr += 1 };
            }
        }
        drop(resolver);
        let mut expected = vec![0u32; 11];
        for &g in &groups {
            expected[g] += 1;
        }
        assert_eq!(contents(&v), expected);
    }
}
