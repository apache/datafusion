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

use datafusion_common::{Result, internal_datafusion_err, internal_err};
use datafusion_expr_common::groups_accumulator::EmitTo;

use crate::aggregate::groups_accumulator::block_store::{
    Block, BlockStore, VecBlockStore,
};

/// Structure used to store aggregation intermediate results in `blocked approach`
///
/// Aggregation intermediate results will be stored as multiple [`Block`]s
/// (simply you can think a [`Block`] as a `Vec`). And `Blocks` is the structure
/// to represent such multiple [`Block`]s.
///
/// The lifecycle is split into two phases:
/// - **Accumulation** (`emit_cursor` is `None`): blocks are appended via
///   [`BlockStore::resize`], [`BlockStore::allocate_block`], or [`Self::push_block`].
/// - **Emission** (`emit_cursor` is `Some`): blocks are drained one-by-one via
///   [`Self::pop_block`] until all blocks are consumed, then [`BlockStore::clear`]
///   resets the state back to accumulation.
///
/// When blocks are popped via [`Self::pop_block`], the block is swapped
/// out in O(1) using `mem::replace` and the cursor advances, avoiding
/// the O(n) shift cost of `Vec::remove(0)`.
///
/// More details about `blocked approach` can see in: [`GroupsAccumulator::supports_blocked_groups`].
///
/// [`GroupsAccumulator::supports_blocked_groups`]: datafusion_expr_common::groups_accumulator::GroupsAccumulator::supports_blocked_groups
///
#[derive(Debug)]
pub struct BlockedBlockStore<B: Block> {
    inner: Vec<B>,
    /// Tracks emission progress. `None` during accumulation, `Some(idx)`
    /// points to the next block to pop during emission.
    emit_cursor: Option<usize>,
    block_size: usize,
}

impl<B: Block> BlockedBlockStore<B> {
    pub fn new(block_size: usize) -> Self {
        Self {
            inner: Vec::new(),
            emit_cursor: None,
            block_size,
        }
    }

    /// Push a new block to the end of the blocks.
    pub fn push_block(&mut self, block: B) {
        assert!(
            self.emit_cursor.is_none(),
            "push_block must not be called during emission"
        );
        self.inner.push(block);
    }

    /// Pop the first active block in O(1) by swapping it with an empty
    /// placeholder and advancing the emit cursor.
    pub fn pop_block(&mut self) -> Option<B> {
        let cursor = self.emit_cursor.get_or_insert(0);
        if *cursor >= self.inner.len() {
            None
        } else {
            let block = mem::take(&mut self.inner[*cursor]);
            *cursor += 1;
            Some(block)
        }
    }

    pub fn iter(&self) -> impl Iterator<Item = &B> {
        let start = self.emit_cursor.unwrap_or(0);
        self.inner[start..].iter()
    }
}

impl<B: Block> BlockStore<B> for BlockedBlockStore<B> {
    fn allocate_block(&mut self) {
        assert!(
            self.emit_cursor.is_none(),
            "allocate_block must not be called during emission"
        );

        let block_size = self.block_size;
        if self.inner.is_empty()
            || self
                .inner
                .last()
                .is_some_and(|block| block.len() == block_size)
        {
            self.inner.push(B::new(block_size));
        }
    }

    fn resize(&mut self, total_num_groups: usize, default_value: B::T) {
        assert!(
            self.emit_cursor.is_none(),
            "resize must not be called during emission"
        );

        let block_size = self.block_size;
        let n = self.inner.len();
        let current_len = if n == 0 {
            0
        } else {
            (n - 1) * block_size + self.inner.last().unwrap().len()
        };

        if current_len >= total_num_groups {
            return;
        }

        let mut to_fill = total_num_groups - current_len;

        // Fill remaining capacity in the last block
        if let Some(last) = self.inner.last_mut() {
            let available = block_size - last.len();
            let fill = to_fill.min(available);
            if fill > 0 {
                last.fill_default_value(fill, default_value.clone());
                to_fill -= fill;
            }
        }

        // Add full blocks
        while to_fill >= block_size {
            let mut block = B::new(block_size);
            block.fill_default_value(block_size, default_value.clone());
            self.inner.push(block);
            to_fill -= block_size;
        }

        // Add final partial block if needed
        if to_fill > 0 {
            let mut block = B::new(block_size);
            block.fill_default_value(to_fill, default_value.clone());
            self.inner.push(block);
        }
    }

    fn num_blocks(&self) -> usize {
        let start = self.emit_cursor.unwrap_or(0);
        self.inner.len() - start
    }

    fn is_empty(&self) -> bool {
        self.num_blocks() == 0
    }

    fn clear(&mut self) {
        self.inner.clear();
        self.emit_cursor = None;
    }
}

impl<B: Block> Index<usize> for BlockedBlockStore<B> {
    type Output = B;

    #[inline]
    fn index(&self, index: usize) -> &Self::Output {
        debug_assert!(
            self.emit_cursor.is_none(),
            "index must not be called during emission"
        );
        unsafe { self.inner.get_unchecked(index) }
    }
}

impl<B: Block> IndexMut<usize> for BlockedBlockStore<B> {
    #[inline]
    fn index_mut(&mut self, index: usize) -> &mut Self::Output {
        debug_assert!(
            self.emit_cursor.is_none(),
            "index_mut must not be called during emission"
        );
        unsafe { self.inner.get_unchecked_mut(index) }
    }
}



impl<T: Clone + Debug> VecBlockStore<T> for BlockedBlockStore<Vec<T>> {
    fn emit(&mut self, emit_to: EmitTo) -> Result<Vec<T>> {
        match emit_to {
            EmitTo::NextBlock => self
                .pop_block()
                .ok_or_else(|| internal_datafusion_err!("no more blocks to emit")),
            EmitTo::All | EmitTo::First(_) => {
                internal_err!(
                    "blocks value block store does not support emitting all or first"
                )
            }
        }
    }
}



#[cfg(test)]
mod tests {
    use super::*;
    use crate::aggregate::groups_accumulator::block_store::BlockStore;

    type TestBlocks = BlockedBlockStore<Vec<u32>>;

    #[test]
    fn test_resize_within_one_block() {
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

    #[test]
    fn test_multi_blocks_resize() {
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

    #[test]
    fn test_pop_block() {
        let mut blocks = TestBlocks::new(3);
        blocks.resize(7, 42);
        assert_eq!(blocks.num_blocks(), 3);

        let blk0 = blocks.pop_block().unwrap();
        assert_eq!(blk0.len(), 3);
        assert_eq!(blocks.num_blocks(), 2);

        let blk1 = blocks.pop_block().unwrap();
        assert_eq!(blk1.len(), 3);
        assert_eq!(blocks.num_blocks(), 1);

        let blk2 = blocks.pop_block().unwrap();
        assert_eq!(blk2.len(), 1);
        assert_eq!(blocks.num_blocks(), 0);
        assert!(blocks.is_empty());
        assert!(blocks.pop_block().is_none());
    }

    #[test]
    fn existing_blocks_implements_block_store() {
        let mut store = BlockedBlockStore::<Vec<u32>>::new(2);
        store.resize(5, 42);
        assert_eq!(BlockStore::num_blocks(&store), 3);
        assert_eq!(store[0], vec![42, 42]);
        assert_eq!(store[1], vec![42, 42]);
        assert_eq!(store[2], vec![42]);
    }

    #[test]
    fn blocked_block_store_allocates_new_block_when_full() {
        let mut store = BlockedBlockStore::<Vec<u32>>::new(2);
        store.allocate_block();
        assert_eq!(BlockStore::num_blocks(&store), 1);

        store[0].push(1);
        store.allocate_block();
        assert_eq!(BlockStore::num_blocks(&store), 1);

        store[0].push(2);
        store.allocate_block();
        assert_eq!(BlockStore::num_blocks(&store), 2);
    }
}
