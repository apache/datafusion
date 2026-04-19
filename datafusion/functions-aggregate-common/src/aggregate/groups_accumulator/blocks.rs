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

//! Aggregation intermediate results blocks in blocked approach

use std::{
    fmt::Debug,
    iter, mem,
    ops::{Index, IndexMut},
};

use datafusion_expr_common::groups_accumulator::EmitTo;

/// Structure used to store aggregation intermediate results in `blocked approach`
///
/// Aggregation intermediate results will be stored as multiple [`Block`]s
/// (simply you can think a [`Block`] as a `Vec`). And `Blocks` is the structure
/// to represent such multiple [`Block`]s.
///
/// Internally uses a `Vec<B>` with a `start` offset to track the first active
/// block. When blocks are popped via [`Self::pop_block`], the block is swapped
/// out in O(1) using `mem::replace` and the `start` cursor advances, avoiding
/// the O(n) shift cost of `Vec::remove(0)`.
///
/// More details about `blocked approach` can see in: [`GroupsAccumulator::supports_blocked_groups`].
///
/// [`GroupsAccumulator::supports_blocked_groups`]: datafusion_expr_common::groups_accumulator::GroupsAccumulator::supports_blocked_groups
///
#[derive(Debug)]
pub struct Blocks<B: Block> {
    inner: Vec<B>,
    /// Index of the first active block. All blocks before this index have been
    /// popped and replaced with empty placeholders.
    start: usize,
    block_size: Option<usize>,
}

impl<B: Block> Blocks<B> {
    pub fn new(block_size: Option<usize>) -> Self {
        Self {
            inner: Vec::new(),
            start: 0,
            block_size,
        }
    }

    pub fn resize<F>(
        &mut self,
        total_num_groups: usize,
        new_block: F,
        default_value: B::T,
    ) where
        F: Fn(Option<usize>) -> B,
    {
        let block_size = self.block_size.unwrap_or(usize::MAX);
        // For resize, we need to:
        //   1. Ensure the blks are enough first
        //   2. and then ensure slots in blks are enough
        let active_len = self.inner.len() - self.start;
        let (mut cur_blk_idx, exist_slots_num) = if active_len > 0 {
            let cur_blk_idx = self.inner.len() - 1;
            let exist_slots =
                (active_len - 1) * block_size + self.inner.last().unwrap().len();

            (cur_blk_idx, exist_slots)
        } else {
            (self.inner.len(), 0)
        };

        // No new groups, don't need to expand, just return
        if exist_slots_num >= total_num_groups {
            return;
        }

        // 1. Ensure blks are enough
        let new_blks_num = total_num_groups.div_ceil(block_size) - active_len;
        if new_blks_num > 0 {
            for _ in 0..new_blks_num {
                let block = new_block(self.block_size);
                self.inner.push(block);
            }
        }

        // 2. Ensure slots are enough
        let mut new_slots_num = total_num_groups - exist_slots_num;

        // When no active blocks existed before, cur_blk_idx now points to the
        // first newly pushed block.
        if active_len == 0 {
            cur_blk_idx = self.start;
        }

        // 2.1 Only fill current blk if it may be already enough
        let cur_blk_rest_slots_num = block_size - self.inner[cur_blk_idx].len();
        if cur_blk_rest_slots_num >= new_slots_num {
            self.inner[cur_blk_idx]
                .fill_default_value(new_slots_num, default_value.clone());
            return;
        }

        // 2.2 Fill current blk to full
        self.inner[cur_blk_idx]
            .fill_default_value(cur_blk_rest_slots_num, default_value.clone());
        new_slots_num -= cur_blk_rest_slots_num;

        // 2.3 Fill complete blks
        let complete_blks_num = new_slots_num / block_size;
        for _ in 0..complete_blks_num {
            cur_blk_idx += 1;
            self.inner[cur_blk_idx].fill_default_value(block_size, default_value.clone());
        }

        // 2.4 Fill last blk if needed
        let rest_slots_num = new_slots_num % block_size;
        if rest_slots_num > 0 {
            self.inner
                .last_mut()
                .unwrap()
                .fill_default_value(rest_slots_num, default_value);
        }
    }

    /// Pop the first active block in O(1) by swapping it with an empty
    /// placeholder and advancing the `start` cursor.
    pub fn pop_block(&mut self) -> Option<B> {
        if self.start >= self.inner.len() {
            None
        } else {
            let block = mem::take(&mut self.inner[self.start]);
            self.start += 1;
            Some(block)
        }
    }

    pub fn num_blocks(&self) -> usize {
        self.inner.len() - self.start
    }

    pub fn len(&self) -> usize {
        self.inner.len() - self.start
    }

    pub fn is_empty(&self) -> bool {
        self.start >= self.inner.len()
    }

    pub fn iter(&self) -> impl Iterator<Item = &B> {
        self.inner[self.start..].iter()
    }

    pub fn clear(&mut self) {
        self.inner.clear();
        self.start = 0;
    }
}

impl<B: Block> Index<usize> for Blocks<B> {
    type Output = B;

    #[inline]
    fn index(&self, index: usize) -> &Self::Output {
        &self.inner[self.start + index]
    }
}

impl<B: Block> IndexMut<usize> for Blocks<B> {
    #[inline]
    fn index_mut(&mut self, index: usize) -> &mut Self::Output {
        &mut self.inner[self.start + index]
    }
}

/// The abstraction to represent one aggregation intermediate result block
/// in `blocked approach`, multiple blocks compose a [`Blocks`]
///
/// Many types of aggregation intermediate result exist, and we define an interface
/// to abstract the necessary behaviors of various intermediate result types.
///
pub trait Block: Debug + Default {
    type T: Clone;

    /// Fill the block with default value
    fn fill_default_value(&mut self, fill_len: usize, default_value: Self::T);

    /// Return the length of the block
    fn len(&self) -> usize;

    /// Return true if the block is empty
    fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

/// Usually we use `Vec` to represent `Block`, so we define `Blocks<Vec<T>>`
/// as the `VecBlocks<T>`
pub type VecBlocks<T> = Blocks<Vec<T>>;

/// We usually use `Vec` to represent `Block`,
/// so we implement `Block` trait for `Vec`
impl<Ty: Clone + Debug> Block for Vec<Ty> {
    type T = Ty;

    fn fill_default_value(&mut self, fill_len: usize, default_value: Self::T) {
        self.extend(iter::repeat_n(default_value, fill_len));
    }

    fn len(&self) -> usize {
        self.len()
    }
}

impl<T: Clone + Debug> VecBlocks<T> {
    pub fn emit(&mut self, emit_to: EmitTo) -> Vec<T> {
        if matches!(emit_to, EmitTo::NextBlock) {
            assert!(
                self.block_size.is_some(),
                "only support emit next block in blocked groups"
            );
            self.pop_block()
                .expect("should not call emit for empty blocks")
        } else {
            // TODO: maybe remove `EmitTo::take_needed` and move the
            // pattern matching codes here after supporting blocked approach
            // for all exist accumulators, to avoid matching twice
            assert!(
                self.block_size.is_none(),
                "only support emit all/first in flat groups"
            );
            emit_to.take_needed(&mut self[0])
        }
    }
}

#[cfg(test)]
mod test {
    use crate::aggregate::groups_accumulator::blocks::Blocks;

    type TestBlocks = Blocks<Vec<u32>>;

    #[test]
    fn test_single_block_resize() {
        let new_block = |block_size: Option<usize>| {
            let cap = block_size.unwrap_or(0);
            Vec::with_capacity(cap)
        };

        let mut blocks = TestBlocks::new(None);
        assert_eq!(blocks.num_blocks(), 0);

        for _ in 0..2 {
            // Should have single block, 5 block len, all data are 42
            blocks.resize(5, new_block, 42);
            assert_eq!(blocks.num_blocks(), 1);
            assert_eq!(blocks[0].len(), 5);
            blocks[0].iter().for_each(|num| assert_eq!(*num, 42));

            // Resize to a larger block
            // Should still have single block, 10 block len, all data are 42
            blocks.resize(10, new_block, 42);
            assert_eq!(blocks.num_blocks(), 1);
            assert_eq!(blocks[0].len(), 10);
            blocks[0].iter().for_each(|num| assert_eq!(*num, 42));

            // Clear
            // Should have nothing after clearing
            blocks.clear();
            assert_eq!(blocks.num_blocks(), 0);

            // Test resize after clear in next round
        }
    }

    #[test]
    fn test_multi_blocks_resize() {
        let new_block = |block_size: Option<usize>| {
            let cap = block_size.unwrap_or(0);
            Vec::with_capacity(cap)
        };

        let mut blocks = TestBlocks::new(Some(3));
        assert_eq!(blocks.num_blocks(), 0);

        for _ in 0..2 {
            // Should have:
            //  - 2 blocks
            //  - `block 0` of 3 len
            //  - `block 1` of 2 len
            //  - all data are 42
            blocks.resize(5, new_block, 42);
            assert_eq!(blocks.num_blocks(), 2);
            assert_eq!(blocks[0].len(), 3);
            blocks[0].iter().for_each(|num| assert_eq!(*num, 42));
            assert_eq!(blocks[1].len(), 2);
            blocks[1].iter().for_each(|num| assert_eq!(*num, 42));

            // Resize to larger blocks
            // Should have:
            //  - 4 blocks
            //  - `block 0` of 3 len
            //  - `block 1` of 3 len
            //  - `block 2` of 3 len
            //  - `block 3` of 1 len
            //  - all data are 42
            blocks.resize(10, new_block, 42);
            assert_eq!(blocks.num_blocks(), 4);
            assert_eq!(blocks[0].len(), 3);
            blocks[0].iter().for_each(|num| assert_eq!(*num, 42));
            assert_eq!(blocks[1].len(), 3);
            blocks[1].iter().for_each(|num| assert_eq!(*num, 42));
            assert_eq!(blocks[2].len(), 3);
            blocks[2].iter().for_each(|num| assert_eq!(*num, 42));
            assert_eq!(blocks[3].len(), 1);
            blocks[3].iter().for_each(|num| assert_eq!(*num, 42));

            // Clear
            // Should have nothing after clearing
            blocks.clear();
            assert_eq!(blocks.num_blocks(), 0);

            // Test resize after clear in next round
        }
    }

    #[test]
    fn test_pop_block() {
        let new_block = |block_size: Option<usize>| {
            let cap = block_size.unwrap_or(0);
            Vec::with_capacity(cap)
        };

        let mut blocks = TestBlocks::new(Some(3));
        blocks.resize(7, new_block, 42);
        // 3 blocks: [42,42,42], [42,42,42], [42]
        assert_eq!(blocks.num_blocks(), 3);

        let blk0 = blocks.pop_block().unwrap();
        assert_eq!(blk0.len(), 3);
        assert_eq!(blocks.num_blocks(), 2);
        // blocks[0] should now be what was blocks[1]
        assert_eq!(blocks[0].len(), 3);
        assert_eq!(blocks[1].len(), 1);

        let blk1 = blocks.pop_block().unwrap();
        assert_eq!(blk1.len(), 3);
        assert_eq!(blocks.num_blocks(), 1);
        assert_eq!(blocks[0].len(), 1);

        let blk2 = blocks.pop_block().unwrap();
        assert_eq!(blk2.len(), 1);
        assert_eq!(blocks.num_blocks(), 0);
        assert!(blocks.is_empty());
        assert!(blocks.pop_block().is_none());
    }
}
