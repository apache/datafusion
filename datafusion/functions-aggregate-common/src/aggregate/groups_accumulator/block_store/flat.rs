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

use crate::aggregate::groups_accumulator::block_store::BlockStore;
use crate::aggregate::groups_accumulator::blocks::Block;

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

    /// Return the underlying block.
    pub fn inner(&self) -> &B {
        &self.0
    }

    /// Return the underlying block mutably.
    pub fn inner_mut(&mut self) -> &mut B {
        &mut self.0
    }

    /// Consume the store and return the underlying block.
    pub fn into_inner(self) -> B {
        self.0
    }
}

impl<B: Block> Default for FlatBlockStore<B> {
    fn default() -> Self {
        Self::new()
    }
}

impl<B: Block> BlockStore<B> for FlatBlockStore<B> {
    fn new(block_size: Option<usize>) -> Self {
        debug_assert!(
            block_size.is_none(),
            "flat block store should not be created with a block size"
        );
        Self::new()
    }

    fn resize<F>(&mut self, total_num_groups: usize, new_block: F, default_value: B::T)
    where
        F: Fn(Option<usize>) -> B,
    {
        if total_num_groups == 0 {
            return;
        }

        if self.0.is_empty() {
            self.0 = new_block(None);
        }

        let existing_len = self.0.len();
        if existing_len < total_num_groups {
            self.0
                .fill_default_value(total_num_groups - existing_len, default_value);
        }
    }

    fn num_blocks(&self) -> usize {
        usize::from(!self.is_empty())
    }

    fn is_empty(&self) -> bool {
        self.0.is_empty()
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

    fn new_block(block_size: Option<usize>) -> TestBlock {
        Vec::with_capacity(block_size.unwrap_or(0))
    }

    #[test]
    fn flat_block_store_resizes_single_block() {
        let mut store = FlatBlockStore::<TestBlock>::new();
        assert!(store.is_empty());
        assert_eq!(store.num_blocks(), 0);

        store.resize(3, new_block, 42);
        assert_eq!(store.num_blocks(), 1);
        assert_eq!(store[0], vec![42, 42, 42]);

        store.resize(5, new_block, 7);
        assert_eq!(store.num_blocks(), 1);
        assert_eq!(store[0], vec![42, 42, 42, 7, 7]);

        store.clear();
        assert!(store.is_empty());
        assert_eq!(store.num_blocks(), 0);
    }

    #[test]
    fn flat_block_store_index_accesses_inner_block() {
        let mut store = FlatBlockStore::<TestBlock>::new();
        store.resize(2, new_block, 42);

        store[0][1] = 7;
        assert_eq!(store[0], vec![42, 7]);
    }
}
