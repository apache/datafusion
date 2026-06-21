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

//! Flat [`BlockStore`] implementation backed by a single block.

use std::ops::{Index, IndexMut};
use std::{fmt::Debug, mem};

use crate::aggregate::groups_accumulator::block_store::{Block, BlockStore};

/// A flat [`BlockStore`] implementation backed by a single block.
///
/// This newtype is intended for flat group state where `block_id` is always 0.
/// The physical layout is exactly one `B`, avoiding the extra `Vec<B>` lookup
/// needed by blocked storage.
#[derive(Debug)]
pub struct FlatBlockStore<B: Block>(B);

impl<B: Block> FlatBlockStore<B> {
    /// Create a new flat store with an empty default block.
    pub fn new() -> Self {
        Self(B::default())
    }
}

impl<B: Block> Default for FlatBlockStore<B> {
    fn default() -> Self {
        Self::new()
    }
}

impl<B: Block> BlockStore<B> for FlatBlockStore<B> {
    fn push_block(&mut self, block: B) {
        self.0 = block;
    }

    fn pop_block(&mut self) -> Option<B> {
        let block = mem::take(&mut self.0);
        Some(block)
    }

    fn reserve_blocks(&mut self) {}

    fn resize(&mut self, total_num_groups: usize, default_value: B::T) {
        if total_num_groups == 0 {
            return;
        }

        if self.0.is_empty() {
            self.0 = B::new(0);
        }

        let existing_len = self.0.len();
        if existing_len < total_num_groups {
            self.0
                .fill_default_value(total_num_groups - existing_len, default_value);
        }
    }

    fn num_blocks(&self) -> usize {
        1
    }

    fn block_size(&self) -> Option<usize> {
        None
    }

    fn clear(&mut self) {
        self.0 = B::default();
    }
}

impl<B: Block> Index<usize> for FlatBlockStore<B> {
    type Output = B;

    #[inline]
    fn index(&self, index: usize) -> &Self::Output {
        assert_eq!(index, 0, "flat block store only has block 0");
        &self.0
    }
}

impl<B: Block> IndexMut<usize> for FlatBlockStore<B> {
    #[inline]
    fn index_mut(&mut self, index: usize) -> &mut Self::Output {
        assert_eq!(index, 0, "flat block store only has block 0");
        &mut self.0
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    type TestBlock = Vec<u32>;

    // ---- push_block ----

    // Covers push_block replacing the whole single backing block.
    // Example: pushing [7, 8, 9] over [1, 1] leaves exactly [7, 8, 9].
    #[test]
    fn test_push_block_replaces_inner_flat_block() {
        let mut store = FlatBlockStore::<TestBlock>::new();
        store.resize(2, 1);

        store.push_block(vec![7, 8, 9]);
        assert_eq!(store.num_blocks(), 1);
        assert_eq!(store[0], vec![7, 8, 9]);
    }

    // ---- pop_block ----

    // Covers pop_block taking the single backing block from flat storage.
    // Example: popping a new flat store returns [], while popping [42, 7, 42] returns those values.
    #[test]
    fn test_pop_block_takes_single_flat_block() {
        let mut store = FlatBlockStore::<TestBlock>::new();
        assert_eq!(store.pop_block(), Some(vec![]));

        store.resize(3, 42);
        store[0][1] = 7;

        assert_eq!(store.pop_block(), Some(vec![42, 7, 42]));
        assert_ne!(store.num_blocks(), 0);
        assert_eq!(store.num_blocks(), 1);
        assert_eq!(store.pop_block(), Some(vec![]));

        store.resize(1, 5);
        assert_eq!(store.pop_block(), Some(vec![5]));
    }

    // ---- reserve_blocks ----

    // Covers reserve_blocks being a no-op for flat storage.
    // Example: reserve_blocks on a new flat store still leaves num_blocks() == 1.
    #[test]
    fn test_reserve_blocks_keeps_flat_store_unchanged() {
        let mut store = FlatBlockStore::<TestBlock>::new();
        store.reserve_blocks();
        assert_eq!(store.num_blocks(), 1);

        store.resize(1, 42);
        store.reserve_blocks();
        assert_eq!(store.num_blocks(), 1);
        assert_eq!(store[0], vec![42]);
    }

    // ---- resize ----

    // Covers flat storage growth within its single backing block.
    // Example: resize 0 -> 3 -> 5 yields one block [42, 42, 42, 7, 7].
    #[test]
    fn test_resize_grows_single_flat_block() {
        let mut store = FlatBlockStore::<TestBlock>::new();
        assert_ne!(store.num_blocks(), 0);
        assert_eq!(store.num_blocks(), 1);

        store.resize(3, 42);
        assert_eq!(store.num_blocks(), 1);
        assert_eq!(store[0], vec![42, 42, 42]);

        store.resize(5, 7);
        assert_eq!(store.num_blocks(), 1);
        assert_eq!(store[0], vec![42, 42, 42, 7, 7]);

        store.clear();
        assert_ne!(store.num_blocks(), 0);
        assert_eq!(store.num_blocks(), 1);
    }

    // Covers resize as a grow-only operation that preserves existing flat values.
    // Example: resizing from 3 down to 1 keeps [99, 10, 10], then grows to append [7, 7].
    #[test]
    fn test_resize_preserves_flat_values_when_requested_size_shrinks() {
        let mut store = FlatBlockStore::<TestBlock>::new();
        store.resize(3, 10);
        store[0][0] = 99;

        store.resize(1, 0);
        assert_eq!(store[0], vec![99, 10, 10]);

        store.resize(5, 7);
        assert_eq!(store[0], vec![99, 10, 10, 7, 7]);
    }

    // Covers resize(0, _) preserving the empty backing block in flat storage.
    // Example: resize to zero keeps num_blocks() == 1 and pop_block returns [].
    #[test]
    fn test_resize_keeps_backing_block_empty_when_requested_size_is_zero() {
        let mut store = FlatBlockStore::<TestBlock>::new();
        store.resize(0, 42);
        assert_ne!(store.num_blocks(), 0);
        assert_eq!(store.num_blocks(), 1);
        assert_eq!(store.pop_block(), Some(vec![]));
    }

    // ---- index ----

    // Covers index 0 read/write access to the single backing block.
    // Example: writing store[0][1] = 7 changes [42, 42] into [42, 7].
    #[test]
    fn test_index_accesses_inner_flat_block() {
        let mut store = FlatBlockStore::<TestBlock>::new();
        store.resize(2, 42);

        store[0][1] = 7;
        assert_eq!(store[0], vec![42, 7]);
    }

    // Covers rejecting any index other than 0 for flat storage.
    // Example: store[1] panics because flat storage only exposes block 0.
    #[test]
    #[should_panic(expected = "flat block store only has block 0")]
    fn test_index_rejects_non_zero_flat_block_index() {
        let store = FlatBlockStore::<TestBlock>::new();
        let _ = &store[1];
    }
}
