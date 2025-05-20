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

// ========================================================================
// Basic abstractions: `Blocks` and `Block`
// ========================================================================

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
pub struct Blocks<B: Block, E: EmitBlockBuilder<B = B>> {
    /// Data in blocks
    inner: Vec<B>,

    /// Optimization for high cardinality case
    current: B,

    /// Block size
    ///
    /// It states:
    ///   - `Some(blk_size)`, it represents multiple block exists, each one
    ///     has the `blk_size` len.
    ///   - `None` , only single block exists.
    block_size: Option<usize>,

    /// Total groups number in blocks
    total_num_groups: usize,

    /// Emit state used to control the emitting process
    emit_state: EmitBlocksState<E>,
}

impl<B: Block, E: EmitBlockBuilder<B = B>> Blocks<B, E> {
    #[inline]
    pub fn new(block_size: Option<usize>) -> Self {
        Self {
            inner: Vec::with_capacity(1024),
            current: B::default(),
            total_num_groups: 0,
            block_size,
            emit_state: EmitBlocksState::Init,
        }
    }

    /// Expand blocks to make it large enough to store `total_num_groups` groups,
    /// and we fill the new allocated block with `default_val`
    pub fn expand(&mut self, total_num_groups: usize, default_val: B::T) {
        assert!(!self.is_emitting(), "can not update groups during emitting");
        if self.total_num_groups >= total_num_groups {
            return;
        }

        // We compute how many blocks we need to store the `total_num_groups` groups.
        // And if found the `exist_blocks` are not enough, we allocate more.
        let needed_blocks =
            total_num_groups.div_ceil(self.block_size.unwrap_or(usize::MAX));
        let exist_blocks = self
            .total_num_groups
            .div_ceil(self.block_size.unwrap_or(usize::MAX));
        if exist_blocks < needed_blocks {
            // Take current and push into `inner`, because it is already not the last
            let old_last = mem::take(&mut self.current);

            if !old_last.is_empty() {
                self.inner.push(old_last);
            }

            // allocate blocks
            let allocated_blocks = needed_blocks - exist_blocks;
            self.inner.extend(
                iter::repeat_with(|| {
                    let build_ctx = self.block_size.map(|blk_size| {
                        BuildBlockContext::new(blk_size, default_val.clone())
                    });
                    B::build(build_ctx)
                })
                .take(allocated_blocks),
            );

            // pop the last, and set the current
            let new_last = self.inner.pop().unwrap();
            self.current = new_last;
        }

        // If in `blocked approach`, we can return now.
        // But In `flat approach`, we keep only `single block`, if found the
        // `single block` not large enough, we allocate a larger one and copy
        // the exist data to it(such copy is really expansive).
        if self.block_size.is_none() {
            self.current.expand(total_num_groups, default_val.clone());
        }

        self.total_num_groups = total_num_groups;
    }

    /// Push block
    pub fn push_block(&mut self, block: B) {
        assert!(!self.is_emitting(), "can not update groups during emitting");

        let old_last = mem::take(&mut self.current);
        if !old_last.is_empty() {
            self.inner.push(old_last);
        }
        let block_len = block.len();
        self.current = block;
        self.total_num_groups += block_len;
    }

    /// Emit blocks iteratively
    ///
    /// Because we don't know few about how to init the[`EmitBlockBuilder`],
    /// so we expose `init_block_builder` to let the caller define it.
    pub fn emit_next_block<F>(&mut self, mut init_block_builder: F) -> Option<B>
    where
        F: FnMut(&mut Vec<B>) -> E,
    {
        let (total_num_groups, block_size) = if !self.is_emitting() {
            // TODO: I think, we should set `total_num_groups` to 0 when blocks
            // emitting starts. because it is used to represent number of `exist groups`,
            // and I think `emitting groups` actually not exist anymore.
            // But we still can't do it now for keep the same semantic with `GroupValues`.

            let old_last = mem::take(&mut self.current);
            if !old_last.is_empty() {
                self.inner.push(old_last);
            }

            (self.total_num_groups, self.block_size.unwrap_or(usize::MAX))
        } else {
            (0, 0)
        };

        let emit_block =
            self.emit_state
                .emit_block(total_num_groups, block_size, || {
                    init_block_builder(&mut self.inner)
                })?;

        self.total_num_groups -= emit_block.len();

        Some(emit_block)
    }

    #[inline]
    pub fn num_blocks(&self) -> usize {
        self.total_num_groups
            .div_ceil(self.block_size.unwrap_or(usize::MAX))
    }

    #[inline]
    pub fn total_num_groups(&self) -> usize {
        self.total_num_groups
    }

    // FIXME
    #[inline]
    pub fn iter(&self) -> impl Iterator<Item = &B> {
        self.inner.iter()
    }

    #[inline]
    pub fn clear(&mut self) {
        self.inner.clear();
        self.current = B::default();
        self.total_num_groups = 0;
    }

    #[inline]
    fn is_emitting(&self) -> bool {
        self.emit_state.is_emitting()
    }
}

impl<B: Block, E: EmitBlockBuilder<B = B>> Index<usize> for Blocks<B, E> {
    type Output = B;

    #[inline]
    fn index(&self, index: usize) -> &Self::Output {
        &self.inner[index]
    }
}

impl<B: Block, E: EmitBlockBuilder<B = B>> IndexMut<usize> for Blocks<B, E> {
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
    type T: Clone;

    /// How to build the block
    fn build(build_ctx: Option<BuildBlockContext<Self::T>>) -> Self;

    /// Expand the block to `new_len` with `default_val`
    ///
    /// In `flat approach`, we will only keep single block, and need to
    /// expand it when it is not large enough.
    fn expand(&mut self, new_len: usize, default_val: Self::T);

    /// Truncate the block to `new_len`
    fn truncate(&mut self, new_len: usize);

    /// Block len
    fn len(&self) -> usize;

    fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

pub struct BuildBlockContext<T> {
    block_size: usize,
    default_val: T,
}

impl<T> BuildBlockContext<T> {
    pub fn new(block_size: usize, default_val: T) -> Self {
        Self {
            block_size,
            default_val,
        }
    }
}

// ========================================================================
// The common blocks emitting logic
// ========================================================================

/// Emit blocks state
///
/// There are two states:
///   - Init, we can only update blocks in this state
///
///   - Emitting, we can't update blocks in this state until all
///     blocks are emitted, and the state is reset to `Init`
///
#[derive(Debug)]
pub enum EmitBlocksState<E: EmitBlockBuilder> {
    Init,
    Emitting(EmitBlocksContext<E>),
}

/// Emit blocks context
#[derive(Debug)]
pub struct EmitBlocksContext<E: EmitBlockBuilder> {
    /// Index of next emitted block
    next_emit_index: usize,

    /// Block size of emitting [`Blocks`]
    block_size: usize,

    /// Number of blocks needed to emit
    num_blocks: usize,

    /// The len of last block
    ///
    /// Due to the last block is possibly non-full, so we compute
    /// and store its len.
    ///
    last_block_len: usize,

    /// Emitted block builder
    block_builder: E,
}

impl<E: EmitBlockBuilder> EmitBlocksState<E> {
    pub fn emit_block<F>(
        &mut self,
        total_num_groups: usize,
        block_size: usize,
        mut init_block_builder: F,
    ) -> Option<E::B>
    where
        F: FnMut() -> E,
    {
        loop {
            match self {
                Self::Init => {
                    // Init needed contexts
                    let num_blocks = total_num_groups.div_ceil(block_size);
                    let mut last_block_len = total_num_groups % block_size;
                    last_block_len = if last_block_len > 0 {
                        last_block_len
                    } else {
                        block_size
                    };

                    let block_builder = init_block_builder();

                    let emit_ctx = EmitBlocksContext {
                        next_emit_index: 0,
                        block_size,
                        num_blocks,
                        last_block_len,
                        block_builder,
                    };

                    *self = Self::Emitting(emit_ctx);
                }

                Self::Emitting(EmitBlocksContext {
                    next_emit_index,
                    block_size,
                    num_blocks,
                    last_block_len,
                    block_builder,
                }) => {
                    // Found empty blocks, return and reset directly
                    if next_emit_index == num_blocks {
                        *self = Self::Init;
                        break None;
                    }

                    // Get current emit block idx
                    let emit_index = *next_emit_index;
                    // And then we advance the block idx
                    *next_emit_index += 1;

                    // Process and generate the emit block
                    let is_last_block = next_emit_index == num_blocks;
                    let emit_block = block_builder.build(
                        emit_index,
                        *block_size,
                        is_last_block,
                        *last_block_len,
                    );

                    // Finally we check if all blocks emitted, if so, we reset the
                    // emit context to allow new updates
                    if next_emit_index == num_blocks {
                        *self = Self::Init;
                    }

                    break Some(emit_block);
                }
            }
        }
    }

    #[inline]
    pub fn is_emitting(&self) -> bool {
        !matches!(self, Self::Init)
    }
}

pub trait EmitBlockBuilder: Debug {
    type B;

    fn build(
        &mut self,
        emit_index: usize,
        block_size: usize,
        is_last_block: bool,
        last_block_len: usize,
    ) -> Self::B;
}

// ========================================================================
// The most commonly used implementation `GeneralBlocks<T>`
// ========================================================================

/// Usually we use `Vec` to represent `Block`, so we define `Blocks<Vec<T>>`
/// as the `GeneralBlocks<T>`
pub type GeneralBlocks<T> = Blocks<Vec<T>, Vec<Vec<T>>>;

/// As mentioned in [`GeneralBlocks`], we usually use `Vec` to represent `Block`,
/// so we implement `Block` trait for `Vec`
impl<Ty: Clone + Debug> Block for Vec<Ty> {
    type T = Ty;

    fn build(build_ctx: Option<BuildBlockContext<Self::T>>) -> Self {
        if let Some(BuildBlockContext {
            block_size,
            default_val,
        }) = build_ctx
        {
            vec![default_val; block_size]
        } else {
            Vec::new()
        }
    }

    #[inline]
    fn expand(&mut self, new_len: usize, default_val: Self::T) {
        self.resize(new_len, default_val);
    }

    #[inline]
    fn truncate(&mut self, new_len: usize) {
        self.truncate(new_len);
    }

    #[inline]
    fn len(&self) -> usize {
        self.len()
    }
}

impl<T: Clone + Debug> GeneralBlocks<T> {
    pub fn emit(&mut self, emit_to: EmitTo) -> Option<Vec<T>> {
        let init_block_builder = |inner: &mut Vec<Vec<T>>| mem::take(inner);

        if matches!(emit_to, EmitTo::NextBlock) {
            assert!(
                self.block_size.is_some(),
                "only support emit next block in blocked groups"
            );
            self.emit_next_block(init_block_builder)
        } else {
            // TODO: maybe remove `EmitTo::take_needed` and move the
            // pattern matching codes here after supporting blocked approach
            // for all exist accumulators, to avoid matching twice
            assert!(
                self.block_size.is_none(),
                "only support emit all/first in flat groups"
            );

            // We perform single block emitting through steps:
            //   - Pop the `block` firstly
            //   - Take `need rows` from `block`
            //   - Push back the `block` if still some rows in it
            let mut block = self.emit_next_block(init_block_builder)?;
            let emit_block = emit_to.take_needed(&mut block);

            if !block.is_empty() {
                self.push_block(block);
            }

            Some(emit_block)
        }
    }

    #[inline(always)]
    pub fn get_mut(&mut self, block_id: usize, block_offset: usize) -> &mut T {
        if block_id == self.inner.len() {
            unsafe {
                return self.current.get_unchecked_mut(block_offset);
            }
        }

        unsafe {
            self.inner
                .get_unchecked_mut(block_id)
                .get_unchecked_mut(block_offset)
        }
    }

    #[inline(always)]
    pub fn get(&self, block_id: usize, block_offset: usize) -> &T {
        if block_id == self.inner.len() {
            unsafe {
                return self.current.get_unchecked(block_offset);
            }
        }

        &self.inner[block_id][block_offset]
    }
}

impl<T: Debug> EmitBlockBuilder for Vec<Vec<T>> {
    type B = Vec<T>;

    fn build(
        &mut self,
        emit_index: usize,
        _block_size: usize,
        is_last_block: bool,
        last_block_len: usize,
    ) -> Self::B {
        let mut emit_block = mem::take(&mut self[emit_index]);
        if is_last_block {
            emit_block.truncate(last_block_len);
        }
        emit_block
    }
}

#[cfg(test)]
mod test {
    // use datafusion_expr_common::groups_accumulator::EmitTo;

    // use crate::aggregate::groups_accumulator::{
    //     blocks::GeneralBlocks,
    //     group_index_operations::{
    //         BlockedGroupIndexOperations, FlatGroupIndexOperations, GroupIndexOperations,
    //     },
    // };

    // type TestBlocks = GeneralBlocks<i32>;

    // fn fill_blocks<O: GroupIndexOperations>(
    //     blocks: &mut TestBlocks,
    //     offset: usize,
    //     values: &[i32],
    // ) {
    //     let block_size = blocks.block_size.unwrap_or_default();
    //     for (idx, &val) in values.iter().enumerate() {
    //         let group_index = offset + idx;
    //         let block_id = O::get_block_id(group_index, block_size);
    //         let block_offset = O::get_block_offset(group_index, block_size);
    //         blocks[block_id][block_offset] = val;
    //     }
    // }

    // #[test]
    // fn test_single_block() {
    //     let mut blocks = TestBlocks::new(None);
    //     assert_eq!(blocks.num_blocks(), 0);
    //     assert_eq!(blocks.total_num_groups(), 0);

    //     for _ in 0..2 {
    //         // Expand to 5 groups with 42, contexts to check:
    //         //   - Exist num blocks: 1
    //         //   - Exist num groups: 5
    //         //   - Exist groups: `[42; 5]`
    //         blocks.expand(5, 42);
    //         assert_eq!(blocks.num_blocks(), 1);
    //         assert_eq!(blocks.total_num_groups(), 5);
    //         assert_eq!(&blocks[0], &[42; 5]);

    //         // Modify the first 5 groups to `[0, 1, 2, 3, 4]`, contexts to check:
    //         //   - Exist groups: `[0, 1, 2, 3, 4]`
    //         let values0 = [0, 1, 2, 3, 4];
    //         fill_blocks::<FlatGroupIndexOperations>(&mut blocks, 0, &values0);
    //         assert_eq!(&blocks[0], &values0);

    //         // Expand to 10 groups with 42, contexts to check:
    //         //   - Exist num blocks: 1
    //         //   - Exist num groups: 10
    //         //   - Exist groups: `[0, 1, 2, 3, 4, 42, 42, 42, 42, 42]`
    //         blocks.expand(10, 42);
    //         assert_eq!(blocks.num_blocks(), 1);
    //         assert_eq!(blocks.total_num_groups(), 10);
    //         assert_eq!(&blocks[0], &[0, 1, 2, 3, 4, 42, 42, 42, 42, 42]);

    //         // Modify the last 5 groups to `[5, 6, 7, 8, 9]`, contexts to check:
    //         //   - Exist groups: `[0, 1, 2, 3, 4, 5, 6, 7, 8, 9]`
    //         let values1 = [0, 1, 2, 3, 4, 5, 6, 7, 8, 9];
    //         fill_blocks::<FlatGroupIndexOperations>(&mut blocks, 5, &values1[5..10]);
    //         assert_eq!(&blocks[0], &values1);

    //         // Emit first 4 groups, contexts to check:
    //         //   - Emitted num groups: 4
    //         //   - Emitted groups: `[0, 1 ,2, 3]`
    //         //   - Exist num blocks: 1
    //         //   - Exist num groups: 6
    //         //   - Exist groups: `[4, 5, 6, 7, 8, 9]`
    //         let emit_block = blocks.emit(EmitTo::First(4)).unwrap();
    //         assert_eq!(emit_block.len(), 4);
    //         assert_eq!(&emit_block, &[0, 1, 2, 3]);
    //         assert_eq!(blocks.num_blocks(), 1);
    //         assert_eq!(blocks.total_num_groups(), 6);
    //         assert_eq!(&blocks[0], &[4, 5, 6, 7, 8, 9]);

    //         // Resize back to 12 groups after emit first 4 with 42, contexts to check:
    //         //   - Exist num blocks: 1
    //         //   - Exist num groups: 12
    //         //   - Exist groups: `[4, 5, 6, 7, 8, 9, 42, 42, 42, 42, 42, 42]`
    //         blocks.expand(12, 42);
    //         assert_eq!(blocks.num_blocks(), 1);
    //         assert_eq!(blocks.total_num_groups(), 12);
    //         assert_eq!(&blocks[0], &[4, 5, 6, 7, 8, 9, 42, 42, 42, 42, 42, 42]);

    //         // Modify the last 6 groups to `[20, 21, 22, 23, 24, 25]`, contexts to check:
    //         //   - Exist groups: `[4, 5, 6, 7, 8, 9, 20, 21, 22, 23, 24, 25]`
    //         let values2 = [4, 5, 6, 7, 8, 9, 20, 21, 22, 23, 24, 25];
    //         fill_blocks::<FlatGroupIndexOperations>(&mut blocks, 6, &values2[6..12]);
    //         assert_eq!(&blocks[0], &values2);

    //         // Emit all, contexts to check:
    //         //   - Emitted num groups: 12
    //         //   - Emitted groups: `[4, 5, 6, 7, 8, 9, 20, 21, 22, 23, 24, 25]`
    //         //   - Exist num blocks: 0
    //         //   - Exist num groups: 0
    //         let emit_block = blocks.emit(EmitTo::All).unwrap();
    //         assert_eq!(emit_block.len(), 12);
    //         assert_eq!(&emit_block, &[4, 5, 6, 7, 8, 9, 20, 21, 22, 23, 24, 25]);
    //         assert_eq!(blocks.num_blocks(), 0);
    //         assert_eq!(blocks.total_num_groups(), 0);

    //         // Check emit empty blocks
    //         assert!(blocks.emit(EmitTo::All).is_none());
    //         assert!(blocks.emit(EmitTo::First(1)).is_none());

    //         // Test in next round
    //     }
    // }

    // #[test]
    // fn test_multi_blocks() {
    //     let mut blocks = TestBlocks::new(Some(3));
    //     assert_eq!(blocks.num_blocks(), 0);
    //     assert_eq!(blocks.total_num_groups(), 0);

    //     for _ in 0..2 {
    //         // Expand to 5 groups with 42, contexts to check:
    //         //   - Exist num blocks: 2
    //         //   - Exist num groups: 5
    //         //   - Exist block 0: `[42; 3]`
    //         //   - Exist block 1: `[42; 3]`
    //         //     (in `blocked approach`, groups will always be expanded to len
    //         //     of `block_size * N`)
    //         blocks.expand(5, 42);
    //         assert_eq!(blocks.num_blocks(), 2);
    //         assert_eq!(blocks.total_num_groups(), 5);
    //         assert_eq!(&blocks[0], &[42; 3]);
    //         assert_eq!(&blocks[1], &[42; 3]);

    //         // Modify the first 5 groups to `[0, 1, 2, 3, 4]`, contexts to check:
    //         //   - Exist block 0: `[0, 1, 2]`
    //         //   - Exist block 1: `[3, 4, 42]`
    //         let values = [0, 1, 2, 3, 4];
    //         fill_blocks::<BlockedGroupIndexOperations>(&mut blocks, 0, &values);
    //         assert_eq!(&blocks[0], &[0, 1, 2]);
    //         assert_eq!(&blocks[1], &[3, 4, 42]);

    //         // Expand to 10 groups with 42, contexts to check:
    //         //   - Exist num blocks: 4
    //         //   - Exist num groups: 10
    //         //   - Exist block 0: `[0, 1, 2]`
    //         //   - Exist block 1: `[3, 4, 42]`
    //         //   - Exist block 2: `[42, 42, 42]`
    //         //   - Exist block 3: `[42, 42, 42]`
    //         blocks.expand(10, 42);
    //         assert_eq!(blocks.num_blocks(), 4);
    //         assert_eq!(blocks.total_num_groups(), 10);
    //         assert_eq!(&blocks[0], &[0, 1, 2]);
    //         assert_eq!(&blocks[1], &[3, 4, 42]);
    //         assert_eq!(&blocks[2], &[42, 42, 42]);
    //         assert_eq!(&blocks[3], &[42, 42, 42]);

    //         // Modify the last 5 groups to `[5, 6, 7, 8, 9]`, contexts to check:
    //         //   - Exist block 0: `[0, 1, 2]`
    //         //   - Exist block 1: `[3, 4, 5]`
    //         //   - Exist block 2: `[6, 7, 8]`
    //         //   - Exist block 3: `[9, 42, 42]`
    //         let values = [5, 6, 7, 8, 9];
    //         fill_blocks::<BlockedGroupIndexOperations>(&mut blocks, 5, &values);
    //         assert_eq!(&blocks[0], &[0, 1, 2]);
    //         assert_eq!(&blocks[1], &[3, 4, 5]);
    //         assert_eq!(&blocks[2], &[6, 7, 8]);
    //         assert_eq!(&blocks[3], &[9, 42, 42]);

    //         // Emit blocks, it is actually an alternative of `EmitTo::All`, so when we
    //         // start to emit the first block, contexts should be:
    //         //   - Emitted block 0: `[0, 1, 2]`
    //         //   - Exist num blocks: 3
    //         //   - Exist num groups: 7
    //         //   - Emitting flag: true
    //         let emit_block = blocks.emit(EmitTo::NextBlock).unwrap();
    //         assert_eq!(blocks.num_blocks(), 3);
    //         assert_eq!(blocks.total_num_groups(), 7);
    //         assert!(blocks.is_emitting());
    //         assert_eq!(&emit_block, &[0, 1, 2]);

    //         // Continue to emit rest 3 blocks, contexts to check:
    //         //   - Exist num blocks checking
    //         //   - Exist num groups checking
    //         //   - Emitting flag checking
    //         //   - Emitted block 1: `[3, 4, 5]`
    //         //   - Emitted block 2: `[6, 7, 8]`
    //         //   - Emitted block 3: `[9]`
    //         let emit_block = blocks.emit(EmitTo::NextBlock).unwrap();
    //         assert_eq!(blocks.num_blocks(), 2);
    //         assert_eq!(blocks.total_num_groups(), 4);
    //         assert!(blocks.is_emitting());
    //         assert_eq!(&emit_block, &[3, 4, 5]);

    //         let emit_block = blocks.emit(EmitTo::NextBlock).unwrap();
    //         assert_eq!(blocks.num_blocks(), 1);
    //         assert_eq!(blocks.total_num_groups(), 1);
    //         assert!(blocks.is_emitting());
    //         assert_eq!(&emit_block, &[6, 7, 8]);

    //         let emit_block = blocks.emit(EmitTo::NextBlock).unwrap();
    //         assert_eq!(blocks.num_blocks(), 0);
    //         assert_eq!(blocks.total_num_groups(), 0);
    //         assert!(!blocks.is_emitting());
    //         assert_eq!(&emit_block, &[9]);

    //         // Check emit empty blocks
    //         assert!(blocks.emit(EmitTo::NextBlock).is_none());
    //         // Again for check if it will always be `None`
    //         assert!(blocks.emit(EmitTo::NextBlock).is_none());

    //         // Test in next round
    //     }
    // }
}
