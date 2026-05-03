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

//! Blocked [`BlockStore`] implementation backed by the existing [`Blocks`].

use crate::aggregate::groups_accumulator::block_store::BlockStore;
use crate::aggregate::groups_accumulator::blocks::{Block, Blocks};

/// The blocked implementation of [`BlockStore`].
///
/// This is the existing [`Blocks`] layout, which stores active blocks in a
/// `Vec<B>` and addresses blocks by `block_id`.
pub type BlockedBlockStore<B> = Blocks<B>;

impl<B: Block> BlockStore<B> for Blocks<B> {
    fn new(block_size: Option<usize>) -> Self {
        Self::new(block_size.expect("blocked block store should have block size"))
    }

    fn reserve_blocks<F>(&mut self, new_block: F)
    where
        F: Fn(Option<usize>) -> B,
    {
        Self::reserve_blocks(self, new_block)
    }

    fn resize<F>(&mut self, total_num_groups: usize, new_block: F, default_value: B::T)
    where
        F: Fn(Option<usize>) -> B,
    {
        Self::resize(self, total_num_groups, new_block, default_value)
    }

    fn num_blocks(&self) -> usize {
        Self::num_blocks(self)
    }

    fn is_empty(&self) -> bool {
        Self::is_empty(self)
    }

    fn clear(&mut self) {
        Self::clear(self)
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
    fn existing_blocks_implements_block_store() {
        let mut store = Blocks::<TestBlock>::new(2);
        store.resize(5, new_block, 42);
        assert_eq!(BlockStore::num_blocks(&store), 3);
        assert_eq!(store[0], vec![42, 42]);
        assert_eq!(store[1], vec![42, 42]);
        assert_eq!(store[2], vec![42]);
    }

    #[test]
    fn blocked_block_store_reserves_new_block_when_full() {
        let mut store = Blocks::<TestBlock>::new(2);
        store.reserve_blocks(new_block);
        assert_eq!(BlockStore::num_blocks(&store), 1);

        store[0].push(1);
        store.reserve_blocks(new_block);
        assert_eq!(BlockStore::num_blocks(&store), 1);

        store[0].push(2);
        store.reserve_blocks(new_block);
        assert_eq!(BlockStore::num_blocks(&store), 2);
    }
}
