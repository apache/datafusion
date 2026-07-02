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

//! Blocked [`BlockStore`] implementation.

use std::{
    fmt::Debug,
    mem,
    ops::{Index, IndexMut},
};

use crate::aggregate::groups_accumulator::block_store::{Block, BlockStore};

/// Structure used to store aggregation intermediate results in `blocked approach`
///
/// Aggregation intermediate results will be stored as multiple [`Block`]s
/// (simply you can think a [`Block`] as a `Vec`). And `Blocks` is the structure
/// to represent such multiple [`Block`]s.
///
/// Blocks are popped in FIFO order by keeping a cursor into `inner`. Popping a
/// block swaps it out in O(1) using `mem::take` and advances the cursor,
/// avoiding the O(n) shift cost of `Vec::remove(0)`.
///
/// More details about `blocked approach` can see in: [`GroupsAccumulator::supports_blocked_groups`].
///
/// [`GroupsAccumulator::supports_blocked_groups`]: datafusion_expr_common::groups_accumulator::GroupsAccumulator::supports_blocked_groups
///
#[derive(Debug)]
pub struct BlockedBlockStore<B: Block> {
    inner: Vec<B>,
    /// Index of the next active block.
    cursor: usize,
    block_size: usize,
}

impl<B: Block> BlockedBlockStore<B> {
    /// Create a new blocked store with the given fixed block size.
    pub fn new(block_size: usize) -> Self {
        Self {
            inner: Vec::new(),
            cursor: 0,
            block_size,
        }
    }
}

impl<B: Block> BlockStore<B> for BlockedBlockStore<B> {
    fn push_block(&mut self, block: B) {
        self.inner.push(block);
    }

    fn pop_block(&mut self) -> Option<B> {
        if self.cursor >= self.inner.len() {
            return None;
        }

        let block = mem::take(&mut self.inner[self.cursor]);
        self.cursor += 1;
        Some(block)
    }

    fn reserve_blocks(&mut self) {
        let block_size = self.block_size;
        if self.num_blocks() == 0
            || self
                .inner
                .last()
                .is_some_and(|block| block.len() == block_size)
        {
            self.inner.push(B::new(block_size));
        }
    }

    fn resize(&mut self, total_num_groups: usize, default_value: B::T) {
        let block_size = self.block_size;
        let num_blocks = self.num_blocks();
        let current_len = if num_blocks == 0 {
            0
        } else {
            (num_blocks - 1) * block_size + self.inner.last().unwrap().len()
        };

        if current_len >= total_num_groups {
            return;
        }

        let mut to_fill = total_num_groups - current_len;

        if num_blocks > 0 {
            let last_block = self.inner.last_mut().unwrap();
            let available = block_size - last_block.len();
            let fill_len = to_fill.min(available);
            if fill_len > 0 {
                last_block.fill_default_value(fill_len, default_value.clone());
                to_fill -= fill_len;
            }
        }

        while to_fill >= block_size {
            let mut block = B::new(block_size);
            block.fill_default_value(block_size, default_value.clone());
            self.inner.push(block);
            to_fill -= block_size;
        }

        if to_fill > 0 {
            let mut block = B::new(block_size);
            block.fill_default_value(to_fill, default_value.clone());
            self.inner.push(block);
        }
    }

    fn num_blocks(&self) -> usize {
        debug_assert!(self.cursor <= self.inner.len());
        self.inner.len() - self.cursor
    }

    fn block_size(&self) -> Option<usize> {
        Some(self.block_size)
    }

    fn clear(&mut self) {
        self.inner.clear();
        self.cursor = 0;
    }
}

impl<B: Block> Index<usize> for BlockedBlockStore<B> {
    type Output = B;

    #[inline]
    fn index(&self, index: usize) -> &Self::Output {
        unsafe { self.inner.get_unchecked(self.cursor + index) }
    }
}

impl<B: Block> IndexMut<usize> for BlockedBlockStore<B> {
    #[inline]
    fn index_mut(&mut self, index: usize) -> &mut Self::Output {
        unsafe { self.inner.get_unchecked_mut(self.cursor + index) }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    type TestBlocks = BlockedBlockStore<Vec<u32>>;

    fn assert_block(block: &[u32], expected: &[u32]) {
        assert_eq!(block, expected);
    }

    // ---- push_block ----

    // Covers the generic BlockStore trait behavior for Vec-backed blocks.
    // Example: resize to 5 with block_size = 2 yields [42, 42], [42, 42], [42].
    #[test]
    fn test_block_store_trait_resizes_vec_blocks() {
        let mut store = BlockedBlockStore::<Vec<u32>>::new(2);
        store.resize(5, 42);
        assert_eq!(BlockStore::num_blocks(&store), 3);
        assert_eq!(store[0], vec![42, 42]);
        assert_eq!(store[1], vec![42, 42]);
        assert_eq!(store[2], vec![42]);
    }

    // ---- pop_block ----

    // Covers draining blocks in FIFO order and updating active block count.
    // Example: [[1, 42, 42], [2, 42, 42], [3]] pops in that exact order.
    #[test]
    fn test_pop_block_drains_blocks_in_fifo_order() {
        let mut blocks = TestBlocks::new(3);
        blocks.resize(7, 42);
        blocks[0][0] = 1;
        blocks[1][0] = 2;
        blocks[2][0] = 3;
        assert_eq!(blocks.num_blocks(), 3);

        let blk0 = blocks.pop_block().unwrap();
        assert_block(&blk0, &[1, 42, 42]);
        assert_eq!(blocks.num_blocks(), 2);
        assert_ne!(blocks.num_blocks(), 0);

        let blk1 = blocks.pop_block().unwrap();
        assert_block(&blk1, &[2, 42, 42]);
        assert_eq!(blocks.num_blocks(), 1);

        let blk2 = blocks.pop_block().unwrap();
        assert_block(&blk2, &[3]);
        assert_eq!(blocks.num_blocks(), 0);
        assert_eq!(blocks.num_blocks(), 0);
        assert!(blocks.pop_block().is_none());
    }

    // Covers push_block appending behind the current cursor.
    // Example: after popping [1, 2], a newly pushed [4, 5] is emitted after existing [3].
    #[test]
    fn test_push_block_appends_after_unpopped_blocks() {
        let mut blocks = TestBlocks::new(2);
        blocks.push_block(vec![1, 2]);
        blocks.push_block(vec![3]);

        assert_eq!(blocks.pop_block(), Some(vec![1, 2]));

        blocks.push_block(vec![4, 5]);
        assert_eq!(blocks.num_blocks(), 2);
        assert_eq!(blocks.pop_block(), Some(vec![3]));
        assert_eq!(blocks.pop_block(), Some(vec![4, 5]));
        assert_eq!(blocks.pop_block(), None);

        blocks.push_block(vec![6]);
        assert_eq!(blocks.num_blocks(), 1);
        assert_eq!(blocks.pop_block(), Some(vec![6]));
    }

    // Covers appending a fresh block after all existing blocks have been popped.
    // Example: after [1] is emitted, pushing [2] makes [2] the only active block.
    #[test]
    fn test_push_block_appends_after_exhausted_prefix() {
        let mut blocks = TestBlocks::new(2);
        blocks.push_block(vec![1]);

        assert_eq!(blocks.pop_block(), Some(vec![1]));
        assert_eq!(blocks.pop_block(), None);

        blocks.push_block(vec![2]);
        assert_eq!(blocks.num_blocks(), 1);
        assert_eq!(blocks[0], vec![2]);
        assert_eq!(blocks.pop_block(), Some(vec![2]));
        assert_eq!(blocks.pop_block(), None);
    }

    // ---- index ----

    // Covers index and index_mut addressing active blocks after the pop cursor moves.
    // Example: after popping [1], store[0] addresses [2, 3] and can mutate it.
    #[test]
    fn test_index_accesses_active_blocks_after_pop_cursor_moves() {
        let mut blocks = TestBlocks::new(2);
        blocks.push_block(vec![1]);
        blocks.push_block(vec![2, 3]);

        assert_eq!(blocks.pop_block(), Some(vec![1]));
        assert_eq!(blocks[0], vec![2, 3]);

        blocks[0][0] = 4;
        assert_eq!(blocks.pop_block(), Some(vec![4, 3]));
        assert_eq!(blocks.pop_block(), None);
    }

    // ---- reserve_blocks ----

    // Covers reserve_blocks only appending when the current tail block is full.
    // Example: block_size = 2 appends a second block only after pushing two values.
    #[test]
    fn test_reserve_blocks_appends_only_when_tail_block_is_full() {
        let mut store = BlockedBlockStore::<Vec<u32>>::new(2);
        store.reserve_blocks();
        assert_eq!(BlockStore::num_blocks(&store), 1);
        assert_eq!(store[0].capacity(), 2);

        store[0].push(1);
        store.reserve_blocks();
        assert_eq!(BlockStore::num_blocks(&store), 1);

        store[0].push(2);
        store.reserve_blocks();
        assert_eq!(BlockStore::num_blocks(&store), 2);
        assert_eq!(store[1].capacity(), 2);
        assert!(store[1].is_empty());
    }

    // Covers reserve_blocks appending after all previous blocks have been popped.
    // Example: after popping [1, 2], reserve_blocks creates a new active empty block.
    #[test]
    fn test_reserve_blocks_appends_after_pop_cursor_is_exhausted() {
        let mut store = BlockedBlockStore::<Vec<u32>>::new(2);
        store.push_block(vec![1, 2]);

        assert_eq!(store.pop_block(), Some(vec![1, 2]));
        assert_eq!(store.num_blocks(), 0);

        store.reserve_blocks();
        assert_eq!(store.num_blocks(), 1);
        assert_eq!(store[0].capacity(), 2);
        assert!(store[0].is_empty());
    }

    // ---- resize ----

    // Covers growth that stays within a single block.
    // Example: block_size = 10, resize 0 -> 5 -> 10 keeps one block: [42; 10].
    #[test]
    fn test_resize_grows_within_one_block() {
        let mut blocks = TestBlocks::new(10);
        assert_eq!(blocks.num_blocks(), 0);

        for _ in 0..2 {
            blocks.resize(5, 42);
            assert_eq!(blocks.num_blocks(), 1);
            assert_eq!(blocks[0].len(), 5);
            blocks[0].iter().for_each(|num| assert_eq!(*num, 42));

            blocks.resize(10, 42);
            assert_eq!(blocks.num_blocks(), 1);
            assert_eq!(blocks[0].len(), 10);
            blocks[0].iter().for_each(|num| assert_eq!(*num, 42));

            blocks.clear();
            assert_eq!(blocks.num_blocks(), 0);
        }
    }

    // Covers growth across multiple fixed-size blocks.
    // Example: block_size = 3, resize to 10 creates [3, 3, 3, 1] sized blocks.
    #[test]
    fn test_resize_grows_across_multiple_blocks() {
        let mut blocks = TestBlocks::new(3);
        assert_eq!(blocks.num_blocks(), 0);

        for _ in 0..2 {
            blocks.resize(5, 42);
            assert_eq!(blocks.num_blocks(), 2);
            assert_eq!(blocks[0].len(), 3);
            blocks[0].iter().for_each(|num| assert_eq!(*num, 42));
            assert_eq!(blocks[1].len(), 2);
            blocks[1].iter().for_each(|num| assert_eq!(*num, 42));

            blocks.resize(10, 42);
            assert_eq!(blocks.num_blocks(), 4);
            assert_eq!(blocks[0].len(), 3);
            blocks[0].iter().for_each(|num| assert_eq!(*num, 42));
            assert_eq!(blocks[1].len(), 3);
            blocks[1].iter().for_each(|num| assert_eq!(*num, 42));
            assert_eq!(blocks[2].len(), 3);
            blocks[2].iter().for_each(|num| assert_eq!(*num, 42));
            assert_eq!(blocks[3].len(), 1);
            blocks[3].iter().for_each(|num| assert_eq!(*num, 42));

            blocks.clear();
            assert_eq!(blocks.num_blocks(), 0);
        }
    }

    // Covers resize as a monotonic grow-only operation that preserves existing values.
    // Example: resizing 4 groups down to 2 does not truncate [10, 99, 10], [77].
    #[test]
    fn test_resize_preserves_values_when_requested_size_shrinks() {
        let mut store = BlockedBlockStore::<Vec<u32>>::new(3);
        store.resize(4, 10);
        store[0][1] = 99;
        store[1][0] = 77;

        store.resize(2, 0);
        assert_eq!(store.num_blocks(), 2);
        assert_block(&store[0], &[10, 99, 10]);
        assert_block(&store[1], &[77]);

        store.resize(8, 5);
        assert_eq!(store.num_blocks(), 3);
        assert_block(&store[0], &[10, 99, 10]);
        assert_block(&store[1], &[77, 5, 5]);
        assert_block(&store[2], &[5, 5]);
    }

    // Covers resize growing only the active suffix after the pop cursor moves.
    // Example: after popping [1, 2], resizing active [3] to 4 groups yields [3, 9], [9, 9].
    #[test]
    fn test_resize_grows_active_blocks_after_pop_cursor_moves() {
        let mut store = BlockedBlockStore::<Vec<u32>>::new(2);
        store.push_block(vec![1, 2]);
        store.push_block(vec![3]);

        assert_eq!(store.pop_block(), Some(vec![1, 2]));
        store.resize(4, 9);

        assert_eq!(store.num_blocks(), 2);
        assert_block(&store[0], &[3, 9]);
        assert_block(&store[1], &[9, 9]);
        assert_eq!(store.pop_block(), Some(vec![3, 9]));
        assert_eq!(store.pop_block(), Some(vec![9, 9]));
        assert_eq!(store.pop_block(), None);
    }

    // Covers resize reusing an empty block created by reserve_blocks.
    // Example: reserve_blocks with block_size = 4, resize to 3 fills the existing block with [11, 11, 11].
    #[test]
    fn test_resize_fills_empty_block_created_by_reserve_blocks() {
        let mut store = BlockedBlockStore::<Vec<u32>>::new(4);
        store.reserve_blocks();

        store.resize(3, 11);
        assert_eq!(store.num_blocks(), 1);
        assert_block(&store[0], &[11, 11, 11]);

        store.resize(6, 22);
        assert_eq!(store.num_blocks(), 2);
        assert_block(&store[0], &[11, 11, 11, 22]);
        assert_block(&store[1], &[22, 22]);
    }

    // ---- clear ----

    // Covers clear resetting both accumulated blocks and the pop cursor.
    // Example: after popping the first of three blocks, clear allows a fresh resize to [9].
    #[test]
    fn test_clear_resets_pop_cursor() {
        let mut store = BlockedBlockStore::<Vec<u32>>::new(2);
        store.resize(5, 1);
        assert_eq!(store.pop_block(), Some(vec![1, 1]));
        assert_eq!(store.num_blocks(), 2);

        store.clear();
        assert_eq!(store.num_blocks(), 0);

        store.resize(1, 9);
        assert_eq!(store.num_blocks(), 1);
        assert_eq!(store.pop_block(), Some(vec![9]));
    }
}
