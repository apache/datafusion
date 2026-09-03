use crate::blocked_helpers::take_n_helpers::{
    BlockBuilder, create_adjusted_block_size_iter_for_fixed_blocks, take_n_from_blocks,
};
use crate::groups_accumulator::BlocksIndex;
use datafusion_common::utils::proxy::VecDequeAllocExt;
use std::collections::VecDeque;
use std::fmt::Debug;
use std::marker::PhantomData;
use std::ops::{Index, IndexMut};

pub trait BlockProvider {
    type Block: Block;

    fn new_block(&self) -> Self::Block;

    fn allocated_size(&self) -> usize;
}

pub trait BlockProviderFinish: BlockProvider {
    type FinishedBlock;

    fn finish(&self, block: Self::Block) -> Self::FinishedBlock;
}

pub trait Block {
    // Forcing the item to be Copy so size_of
    type Item: Copy;

    /// Get allocated bytes on heap (not including `size_of::<Self>()`)
    fn allocated_size(&self) -> usize;

    fn push(&mut self, item: Self::Item);

    fn extend(&mut self, iter: impl Iterator<Item = Self::Item>);

    /// Number of items in the block
    fn len(&self) -> usize;

    fn is_empty(&self) -> bool;
}

pub trait BlockWithSlice: Block {
    fn copy_from_slice(&mut self, slice: &[Self::Item]);
    fn append_n(&mut self, item: Self::Item, n: usize);
}

/// When `FIXED_BLOCK_SIZING` is true, the block size is the `Self::block_size` otherwise,
/// the callers control the block size
#[derive(Debug)]
pub struct BlockedCustomInputBuilder<
    const FIXED_BLOCK_SIZING: bool,
    CustomBlockProvider: BlockProvider,
> {
    blocks_provider: CustomBlockProvider,
    /// Using `VecDeque` so we can remove the first block and reclaim memory
    blocks: VecDeque<CustomBlockProvider::Block>,

    /// The size of each block
    block_size: usize,

    /// The total number of items, not the number of offset since in each block there is the initial offset
    len: usize,

    /// The index of the current block
    current_block_index: usize,

    finished_blocks_allocated_memory: usize,
}

impl<const FIXED_BLOCK_SIZING: bool, CustomBlockProvider: BlockProvider>
    BlockedCustomInputBuilder<FIXED_BLOCK_SIZING, CustomBlockProvider>
{
    // TODO - some want to preallocate the blocks and some don't,
    //        there should be a way while avoiding having a lot of memory used if all are prealocatting
    pub fn new(block_size: usize, blocks_provider: CustomBlockProvider) -> Self {
        if FIXED_BLOCK_SIZING {
            assert_ne!(block_size, 0, "block size must be greater than 0");
        }

        let blocks = VecDeque::from(vec![blocks_provider.new_block()]);
        BlockedCustomInputBuilder {
            blocks_provider,
            blocks,
            block_size,
            len: 0,
            current_block_index: 0,
            finished_blocks_allocated_memory: 0,
        }
    }

    pub fn blocks_provider(&self) -> &CustomBlockProvider {
        &self.blocks_provider
    }

    pub fn is_empty(&self) -> bool {
        self.len == 0
    }

    pub fn len(&self) -> usize {
        self.len
    }

    pub fn num_blocks(&self) -> usize {
        self.blocks.len()
    }

    pub fn block(&self, block_index: usize) -> &CustomBlockProvider::Block {
        &self.blocks[block_index]
    }

    pub fn block_size(&self) -> usize {
        self.block_size
    }

    pub fn allocated_size(&self) -> usize {
        self.blocks_provider.allocated_size()
            + self.finished_blocks_allocated_memory
            + self.blocks.allocated_size()
            + self.blocks.back().map_or(0, |b| b.allocated_size())
    }

    /// Get the number of elements in the current block
    pub fn current_block_len(&self) -> usize {
        self.blocks[self.current_block_index].len()
    }

    pub fn start_new_block(&mut self) {
        // Don't add to number of blocks since we might not insert into it
        self.current_block_index += 1;
        self.finished_blocks_allocated_memory +=
            self.blocks.back().map_or(0, |b| b.allocated_size());
        let new_block = self.blocks_provider.new_block();
        self.blocks.push_back(new_block);
    }

    pub(crate) fn reserve_blocks(&mut self, n: usize) {
        self.blocks.reserve(n);
    }

    /// Push length and return if the current block is now full
    pub fn push(&mut self, value: <CustomBlockProvider::Block as Block>::Item) -> bool {
        let block = &mut self.blocks[self.current_block_index];

        block.push(value);
        self.len += 1;

        let finished_block = FIXED_BLOCK_SIZING && block.len() == self.block_size;

        if finished_block {
            self.start_new_block();
            true
        } else {
            false
        }
    }

    /// Extends iterator of lengths within current block
    /// Returns if the current block has finished
    ///
    /// # Panics
    /// Panics if the iterator length exceeds the remaining size of the current block
    pub(super) fn extend_in_block(
        &mut self,
        iter: impl Iterator<Item = <CustomBlockProvider::Block as Block>::Item>,
    ) -> bool {
        let block = &mut self.blocks[self.current_block_index];

        let prev_block_len = block.len();
        block.extend(iter);

        if FIXED_BLOCK_SIZING {
            assert!(
                block.len() <= self.block_size,
                "overflow from block new block length: {}, block size: {}",
                block.len(),
                self.block_size
            );
        }

        let added_items = block.len() - prev_block_len;
        self.len += added_items;

        let finished_block = FIXED_BLOCK_SIZING && block.len() == self.block_size;

        if finished_block {
            self.start_new_block();
            true
        } else {
            false
        }
    }

    /// Extends from slice within current block
    /// Returns if the current block has finished
    ///
    /// # Panics
    /// Panics if the iterator length exceeds the remaining size of the current block
    pub(super) fn extend_from_slice_in_block(
        &mut self,
        slice: &[<CustomBlockProvider::Block as Block>::Item],
    ) -> bool
    where
        CustomBlockProvider::Block: BlockWithSlice,
    {
        let block = &mut self.blocks[self.current_block_index];

        let prev_block_len = block.len();
        block.copy_from_slice(slice);

        if FIXED_BLOCK_SIZING {
            assert!(
                block.len() <= self.block_size,
                "overflow from block new block length: {}, block size: {}",
                block.len(),
                self.block_size
            );
        }

        let added_items = block.len() - prev_block_len;
        self.len += added_items;

        let finished_block = FIXED_BLOCK_SIZING && block.len() == self.block_size;

        if finished_block {
            self.start_new_block();
            true
        } else {
            false
        }
    }

    /// Extend the length from the current offsets
    pub fn extend_from_slice(
        &mut self,
        mut buffer: &[<CustomBlockProvider::Block as Block>::Item],
    ) where
        CustomBlockProvider::Block: BlockWithSlice,
    {
        // If not fixed, then treat all offsets as single block
        if !FIXED_BLOCK_SIZING {
            self.extend_from_slice_in_block(buffer);

            return;
        }

        let number_of_blocks_to_reserve = buffer
            .len()
            .saturating_sub(self.current_block_remaining_len())
            .div_ceil(self.block_size);
        self.reserve_blocks(number_of_blocks_to_reserve);

        while !buffer.is_empty() {
            let remaining_in_current_block = self.current_block_remaining_len();
            let to_add = remaining_in_current_block.min(buffer.len());

            let (to_copy, rest) = buffer.split_at(to_add);
            buffer = rest;

            self.extend_from_slice_in_block(to_copy);
        }
    }

    pub fn current_block_remaining_len(&self) -> usize {
        assert!(
            FIXED_BLOCK_SIZING,
            "current block remaining length is only relevant for manual block size"
        );
        self.block_size - self.blocks[self.current_block_index].len()
    }

    pub fn push_value_n_within_block(
        &mut self,
        value: <CustomBlockProvider::Block as Block>::Item,
        n: usize,
    ) -> bool
    where
        CustomBlockProvider::Block: BlockWithSlice,
    {
        self.len += n;
        let block = &mut self.blocks[self.current_block_index];

        if FIXED_BLOCK_SIZING {
            let new_len = block.len() + n;
            assert!(
                new_len <= self.block_size,
                "overflow from block new block length: {new_len}, block size: {}",
                self.block_size
            );
        }
        block.append_n(value, n);

        let finished_block = FIXED_BLOCK_SIZING && block.len() == self.block_size;

        if finished_block {
            self.start_new_block();
            true
        } else {
            false
        }
    }

    /// Push default
    pub fn push_default_n(&mut self, n: usize)
    where
        CustomBlockProvider::Block: BlockWithSlice,
        <CustomBlockProvider::Block as Block>::Item: Default + Clone,
    {
        self.push_value_n(<CustomBlockProvider::Block as Block>::Item::default(), n);
    }

    pub fn push_value_n(
        &mut self,
        value: <CustomBlockProvider::Block as Block>::Item,
        mut n: usize,
    ) where
        CustomBlockProvider::Block: BlockWithSlice,
        <CustomBlockProvider::Block as Block>::Item: Clone,
    {
        // If not fixed, then treat all offsets as single block
        if !FIXED_BLOCK_SIZING {
            self.push_value_n_within_block(value, n);
            return;
        }

        let number_of_blocks_to_reserve = n
            .saturating_sub(self.current_block_remaining_len())
            .div_ceil(self.block_size);
        self.reserve_blocks(number_of_blocks_to_reserve);

        while n > 0 {
            let remaining_in_current_block = self.current_block_remaining_len();

            let to_add = remaining_in_current_block.min(n);
            n -= to_add;

            self.push_value_n_within_block(value.clone(), to_add);
        }
    }

    /// Take the first block, `None` once there are no more items
    pub fn take_block(&mut self) -> Option<CustomBlockProvider::Block> {
        if self.len == 0 {
            return None;
        }

        let block = self
            .blocks
            .pop_front()
            .expect("len > 0 so must have a block");

        if self.blocks.is_empty() {
            self.current_block_index = 0;
            self.blocks.push_back(self.blocks_provider.new_block());
        } else {
            self.current_block_index -= 1;

            // Only reduce memory if not the last one since the last block is calculated separately
            self.finished_blocks_allocated_memory -= block.allocated_size();
        }

        self.len -= block.len();

        Some(block)
    }

    pub fn take_block_finished(&mut self) -> Option<CustomBlockProvider::FinishedBlock>
    where
        CustomBlockProvider: BlockProviderFinish,
    {
        let block = self.take_block()?;

        let finished = self.blocks_provider.finish(block);

        Some(finished)
    }

    /// Take every non empty block
    pub fn take_all(&mut self) -> Vec<CustomBlockProvider::Block> {
        let blocks = std::mem::take(&mut self.blocks);
        self.reset();

        blocks
            .into_iter()
            .filter(|block| !block.is_empty())
            .collect()
    }

    pub fn take_n(
        &mut self,
        n: usize,
        adjusted_block_size_iter: Option<impl Iterator<Item = usize> + Clone>,
    ) -> <CustomBlockProvider::Block as BlockBuilder>::Output
    where
      CustomBlockProvider::Block: BlockBuilder,
    {
        assert_eq!(FIXED_BLOCK_SIZING, adjusted_block_size_iter.is_none());

        let (taken, layout) = if let Some(iter) = adjusted_block_size_iter {
            take_n_from_blocks(&mut self.blocks, self.len, n, None, iter)
        } else {
            take_n_from_blocks(
                &mut self.blocks,
                self.len,
                n,
                Some(self.block_size),
                create_adjusted_block_size_iter_for_fixed_blocks(
                    self.len,
                    n,
                    self.block_size,
                ),
            )
        };

        self.len = layout.len;
        self.current_block_index = layout.current_block_index;
        self.finished_blocks_allocated_memory = layout.finished_blocks_allocated_size;

        taken
    }

    pub fn reset(&mut self) {
        self.blocks = VecDeque::from(vec![self.blocks_provider.new_block()]);
        self.len = 0;
        self.current_block_index = 0;
        self.finished_blocks_allocated_memory = 0;
    }
}


impl<CustomBlockProvider: BlockProvider>
BlockedCustomInputBuilder<true, CustomBlockProvider>
{
    pub fn take_n_fixed(&mut self, n: usize) -> <CustomBlockProvider::Block as BlockBuilder>::Output
    where
      CustomBlockProvider::Block: BlockBuilder,
    {
        self.take_n(n, None::<std::iter::Empty<_>>)
    }
}

impl<const FIXED_BLOCK_SIZING: bool, CustomBlockProvider: BlockProvider>
    Extend<<CustomBlockProvider::Block as Block>::Item>
    for BlockedCustomInputBuilder<FIXED_BLOCK_SIZING, CustomBlockProvider>
{
    fn extend<T: IntoIterator<Item = <CustomBlockProvider::Block as Block>::Item>>(
        &mut self,
        iter: T,
    ) {
        if !FIXED_BLOCK_SIZING {
            self.extend_in_block(iter.into_iter());

            return;
        }

        let mut iter = iter.into_iter();

        loop {
            let remaining_in_current_block = self.current_block_remaining_len();
            let block_finished =
                self.extend_in_block(iter.by_ref().take(remaining_in_current_block));

            if !block_finished {
                break;
            }
        }
    }
}

impl<CustomBlockProvider> Index<usize>
    for BlockedCustomInputBuilder<true, CustomBlockProvider>
where
    CustomBlockProvider: BlockProvider,
    CustomBlockProvider::Block:
        Index<usize, Output = <CustomBlockProvider::Block as Block>::Item>,
{
    type Output = <CustomBlockProvider::Block as Block>::Item;

    fn index(&self, index: usize) -> &Self::Output {
        self.index(BlocksIndex::from_index_in_fixed_block_size(
            index,
            self.block_size,
        ))
    }
}

impl<CustomBlockProvider> IndexMut<usize>
    for BlockedCustomInputBuilder<true, CustomBlockProvider>
where
    CustomBlockProvider: BlockProvider,
    CustomBlockProvider::Block:
        IndexMut<usize, Output = <CustomBlockProvider::Block as Block>::Item>,
{
    fn index_mut(&mut self, index: usize) -> &mut Self::Output {
        self.index_mut(BlocksIndex::from_index_in_fixed_block_size(
            index,
            self.block_size,
        ))
    }
}

impl<const FIXED_BLOCK_SIZING: bool, CustomBlockProvider> Index<BlocksIndex>
    for BlockedCustomInputBuilder<FIXED_BLOCK_SIZING, CustomBlockProvider>
where
    CustomBlockProvider: BlockProvider,
    CustomBlockProvider::Block:
        Index<usize, Output = <CustomBlockProvider::Block as Block>::Item>,
{
    type Output = <CustomBlockProvider::Block as Block>::Item;

    fn index(&self, index: BlocksIndex) -> &Self::Output {
        &self.blocks[index.block_index()][index.index_in_block()]
    }
}

impl<const FIXED_BLOCK_SIZING: bool, CustomBlockProvider> IndexMut<BlocksIndex>
    for BlockedCustomInputBuilder<FIXED_BLOCK_SIZING, CustomBlockProvider>
where
    CustomBlockProvider: BlockProvider,
    CustomBlockProvider::Block:
        Index<usize, Output = <CustomBlockProvider::Block as Block>::Item>,
    CustomBlockProvider::Block:
        IndexMut<usize, Output = <CustomBlockProvider::Block as Block>::Item>,
{
    fn index_mut(&mut self, index: BlocksIndex) -> &mut Self::Output {
        &mut self.blocks[index.block_index()][index.index_in_block()]
    }
}
