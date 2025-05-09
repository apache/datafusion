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
    collections::VecDeque,
    fmt::Debug,
    iter,
    ops::{Index, IndexMut},
};

use datafusion_expr_common::groups_accumulator::EmitTo;

/// Structure used to store aggregation intermediate results in `blocked approach`
///
/// Aggregation intermediate results will be stored as multiple [`Block`]s
/// (simply you can think a [`Block`] as a `Vec`). And `Blocks` is the structure
/// to represent such multiple [`Block`]s.
///
/// More details about `blocked approach` can see in: [`GroupsAccumulator::supports_blocked_groups`].
///
/// [`GroupsAccumulator::supports_blocked_groups`]: datafusion_expr_common::groups_accumulator::GroupsAccumulator::supports_blocked_groups
///
#[derive(Debug)]
pub struct Blocks<B: Block> {
    inner: Vec<B>,
    next_emit_block_id: usize,
    last_block_len: usize,
    block_size: Option<usize>,
}

impl<B: Block> Blocks<B> {
    #[inline]
    pub fn new(block_size: Option<usize>) -> Self {
        Self {
            inner: Vec::new(),
            next_emit_block_id: 0,
            last_block_len: 0,
            block_size,
        }
    }

    pub fn resize(&mut self, total_num_groups: usize, default_val: B::T) {
        if let Some(block_size) = self.block_size {
            let blocks_cap = self.inner.len() * block_size;
            if blocks_cap < total_num_groups {
                let allocated_blocks =
                    (total_num_groups - blocks_cap).div_ceil(block_size);
                self.inner.extend(
                    iter::repeat_with(|| B::build(self.block_size, default_val.clone()))
                        .take(allocated_blocks),
                );
            }
            self.last_block_len = block_size + total_num_groups - self.inner.len() * block_size;
        } else {
            if self.is_empty() {
                self.inner.push(B::build(self.block_size, default_val.clone()));
            }
            let single_block = self.inner.last_mut().unwrap();
            single_block.resize(total_num_groups, default_val.clone());
        };
    }

    #[inline]
    pub fn pop_block(&mut self) -> Option<B> {
        if self.next_emit_block_id >= self.inner.len() {
            return None;
        }

        let emit_block_id = self.next_emit_block_id;
        let mut emit_blk = std::mem::take(&mut self.inner[emit_block_id]);
        self.next_emit_block_id += 1;

        if self.next_emit_block_id == self.inner.len() {
            emit_blk.resize(self.last_block_len, B::T::default());
        }

        Some(emit_blk)
    }

    #[inline]
    pub fn len(&self) -> usize {
        self.inner.len()
    }

    #[inline]
    pub fn is_empty(&self) -> bool {
        self.inner.is_empty()
    }

    #[inline]
    pub fn iter(&self) -> impl Iterator<Item = &B> {
        self.inner.iter()
    }

    #[inline]
    pub fn clear(&mut self) {
        self.inner.clear();
        self.next_emit_block_id = 0;
    }
}

impl<B: Block> Index<usize> for Blocks<B> {
    type Output = B;

    #[inline]
    fn index(&self, index: usize) -> &Self::Output {
        &self.inner[index]
    }
}

impl<B: Block> IndexMut<usize> for Blocks<B> {
    #[inline]
    fn index_mut(&mut self, index: usize) -> &mut Self::Output {
        &mut self.inner[index]
    }
}

/// The abstraction to represent one aggregation intermediate result block
/// in `blocked approach`, multiple blocks compose a [`Blocks`]
///
/// Many types of aggregation intermediate result exist, and we define an interface
/// to abstract the necessary behaviors of various intermediate result types.
///
pub trait Block: Debug + Default {
    type T: Clone + Default;

    fn build(block_size: Option<usize>, default_val: Self::T) -> Self;

    fn resize(&mut self, new_len: usize, default_val: Self::T);

    fn len(&self) -> usize;

    fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

/// Usually we use `Vec` to represent `Block`, so we define `Blocks<Vec<T>>`
/// as the `GeneralBlocks<T>`
pub type GeneralBlocks<T> = Blocks<Vec<T>>;

/// As mentioned in [`GeneralBlocks`], we usually use `Vec` to represent `Block`,
/// so we implement `Block` trait for `Vec`
impl<Ty: Clone + Default + Debug> Block for Vec<Ty> {
    type T = Ty;

    fn build(block_size: Option<usize>, default_val: Self::T) -> Self {
        if let Some(blk_size) = block_size {
            vec![default_val; blk_size]
        } else {
            Vec::new()
        }
    }

    fn resize(&mut self, new_len: usize, default_val: Self::T) {
        self.resize(new_len, default_val);
    }

    fn len(&self) -> usize {
        self.len()
    }
}

impl<T: Clone + Debug + Default> GeneralBlocks<T> {
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
            emit_to.take_needed(&mut self.inner[0])
        }
    }
}

#[cfg(test)]
mod test {
    use crate::aggregate::groups_accumulator::blocks::Blocks;

    type TestBlocks = Blocks<Vec<u32>>;

    // #[test]
    // fn test_single_block_resize() {
    //     let new_block = |block_size: Option<usize>| {
    //         let cap = block_size.unwrap_or(0);
    //         Vec::with_capacity(cap)
    //     };

    //     let mut blocks = TestBlocks::new(None);
    //     assert_eq!(blocks.len(), 0);

    //     for _ in 0..2 {
    //         // Should have single block, 5 block len, all data are 42
    //         blocks.resize(5, new_block, 42);
    //         assert_eq!(blocks.len(), 1);
    //         assert_eq!(blocks[0].len(), 5);
    //         blocks[0].iter().for_each(|num| assert_eq!(*num, 42));

    //         // Resize to a larger block
    //         // Should still have single block, 10 block len, all data are 42
    //         blocks.resize(10, new_block, 42);
    //         assert_eq!(blocks.len(), 1);
    //         assert_eq!(blocks[0].len(), 10);
    //         blocks[0].iter().for_each(|num| assert_eq!(*num, 42));

    //         // Clear
    //         // Should have nothing after clearing
    //         blocks.clear();
    //         assert_eq!(blocks.len(), 0);

    //         // Test resize after clear in next round
    //     }
    // }

    // #[test]
    // fn test_multi_blocks_resize() {
    //     let new_block = |block_size: Option<usize>| {
    //         let cap = block_size.unwrap_or(0);
    //         Vec::with_capacity(cap)
    //     };

    //     let mut blocks = TestBlocks::new(Some(3));
    //     assert_eq!(blocks.len(), 0);

    //     for _ in 0..2 {
    //         // Should have:
    //         //  - 2 blocks
    //         //  - `block 0` of 3 len
    //         //  - `block 1` of 2 len
    //         //  - all data are 42
    //         blocks.resize(5, new_block, 42);
    //         assert_eq!(blocks.len(), 2);
    //         assert_eq!(blocks[0].len(), 3);
    //         blocks[0].iter().for_each(|num| assert_eq!(*num, 42));
    //         assert_eq!(blocks[1].len(), 2);
    //         blocks[1].iter().for_each(|num| assert_eq!(*num, 42));

    //         // Resize to larger blocks
    //         // Should have:
    //         //  - 4 blocks
    //         //  - `block 0` of 3 len
    //         //  - `block 1` of 3 len
    //         //  - `block 2` of 3 len
    //         //  - `block 3` of 1 len
    //         //  - all data are 42
    //         blocks.resize(10, new_block, 42);
    //         assert_eq!(blocks.len(), 4);
    //         assert_eq!(blocks[0].len(), 3);
    //         blocks[0].iter().for_each(|num| assert_eq!(*num, 42));
    //         assert_eq!(blocks[1].len(), 3);
    //         blocks[1].iter().for_each(|num| assert_eq!(*num, 42));
    //         assert_eq!(blocks[2].len(), 3);
    //         blocks[2].iter().for_each(|num| assert_eq!(*num, 42));
    //         assert_eq!(blocks[3].len(), 1);
    //         blocks[3].iter().for_each(|num| assert_eq!(*num, 42));

    //         // Clear
    //         // Should have nothing after clearing
    //         blocks.clear();
    //         assert_eq!(blocks.len(), 0);

    //         // Test resize after clear in next round
    //     }
    // }
}
