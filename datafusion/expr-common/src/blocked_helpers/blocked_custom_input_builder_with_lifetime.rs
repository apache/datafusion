use crate::blocked_helpers::take_n_helpers::{
    BlockBuilder, create_adjusted_block_size_iter_for_fixed_blocks, take_n_from_blocks,
};
use crate::groups_accumulator::BlocksIndex;
use datafusion_common::utils::proxy::VecDequeAllocExt;
use std::collections::VecDeque;
use std::fmt::Debug;

pub trait BlockWithLifetimeProvider {
    type Block: BlockWithLifetime;

    fn new_block(&self) -> Self::Block;

    fn allocated_size(&self) -> usize;
}

pub trait BlockProviderWithLifetimeFinish: BlockWithLifetimeProvider {
    type FinishedBlock;

    fn finish(&self, block: Self::Block) -> Self::FinishedBlock;
}

pub trait BlockWithLifetime {
    type Item<'a>;

    /// Get allocated bytes on heap (not including `size_of::<Self>()`)
    fn allocated_size(&self) -> usize;

    fn push(&mut self, item: Self::Item<'_>);

    fn extend<'a>(&mut self, iter: impl Iterator<Item = Self::Item<'a>>);

    /// Number of items in the block
    fn len(&self) -> usize;

    fn is_empty(&self) -> bool;

    fn index(&self, index: usize) -> Self::Item<'_>;
}

pub trait BlockWithLifetimeWithSlice: BlockWithLifetime {
    fn extend_from_slice(&mut self, slice: &[Self::Item<'_>]);
    fn append_n(&mut self, item: Self::Item<'_>, n: usize);
}

/// When `FIXED_BLOCK_SIZING` is true, the block size is the `Self::block_size` otherwise,
/// the callers control the block size
#[derive(Debug)]
pub struct BlockedCustomInputBuilderWithLifetime<
    const FIXED_BLOCK_SIZING: bool,
    CustomBlockProvider: BlockWithLifetimeProvider,
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

impl<const FIXED_BLOCK_SIZING: bool, CustomBlockProvider: BlockWithLifetimeProvider>
    BlockedCustomInputBuilderWithLifetime<FIXED_BLOCK_SIZING, CustomBlockProvider>
{
    pub fn new(block_size: usize, blocks_provider: CustomBlockProvider) -> Self {
        if FIXED_BLOCK_SIZING {
            assert_ne!(block_size, 0, "block size must be greater than 0");
        }

        let blocks = VecDeque::from(vec![blocks_provider.new_block()]);
        BlockedCustomInputBuilderWithLifetime {
            blocks_provider,
            blocks,
            block_size,
            len: 0,
            current_block_index: 0,
            finished_blocks_allocated_memory: 0,
        }
    }

    pub fn provider(&self) -> &CustomBlockProvider {
        &self.blocks_provider
    }

    pub fn len(&self) -> usize {
        self.len
    }

    pub fn is_empty(&self) -> bool {
        self.len == 0
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

    pub fn num_blocks(&self) -> usize {
        self.blocks.len()
    }

    pub fn current_block_index(&self) -> usize {
        self.current_block_index
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
    pub fn push(
        &mut self,
        value: <CustomBlockProvider::Block as BlockWithLifetime>::Item<'_>,
    ) -> bool {
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
    pub fn extend_in_block<'a>(
        &mut self,
        iter: impl Iterator<
            Item = <CustomBlockProvider::Block as BlockWithLifetime>::Item<'a>,
        > + 'a,
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
        slice: &[<CustomBlockProvider::Block as BlockWithLifetime>::Item<'_>],
    ) -> bool
    where
        CustomBlockProvider::Block: BlockWithLifetimeWithSlice,
    {
        let block = &mut self.blocks[self.current_block_index];

        let prev_block_len = block.len();
        block.extend_from_slice(slice);

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
        mut buffer: &[<CustomBlockProvider::Block as BlockWithLifetime>::Item<'_>],
    ) where
        CustomBlockProvider::Block: BlockWithLifetimeWithSlice,
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

    pub(super) fn current_block_remaining_len(&self) -> usize {
        assert!(
            FIXED_BLOCK_SIZING,
            "current block remaining length is only relevant for manual block size"
        );
        self.block_size - self.blocks[self.current_block_index].len()
    }

    pub(crate) fn push_value_n_within_block(
        &mut self,
        value: <CustomBlockProvider::Block as BlockWithLifetime>::Item<'_>,
        n: usize,
    ) -> bool
    where
        CustomBlockProvider::Block: BlockWithLifetimeWithSlice,
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
    pub fn push_default_n<'a>(&mut self, n: usize)
    where
        CustomBlockProvider::Block: BlockWithLifetimeWithSlice,
        <CustomBlockProvider::Block as BlockWithLifetime>::Item<'a>: Default + Copy,
    {
        self.push_value_n(
            <CustomBlockProvider::Block as BlockWithLifetime>::Item::default(),
            n,
        );
    }

    pub fn push_value_n<'a>(
        &mut self,
        value: <CustomBlockProvider::Block as BlockWithLifetime>::Item<'a>,
        mut n: usize,
    ) where
        CustomBlockProvider::Block: BlockWithLifetimeWithSlice,
        <CustomBlockProvider::Block as BlockWithLifetime>::Item<'a>: Copy,
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

            self.push_value_n_within_block(value, to_add);
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

    /// Take every non empty block
    pub fn take_all(&mut self) -> Vec<CustomBlockProvider::Block> {
        let blocks = std::mem::take(&mut self.blocks);
        self.reset();

        blocks
            .into_iter()
            .filter(|block| !block.is_empty())
            .collect()
    }

    pub fn take_block_finished(&mut self) -> Option<CustomBlockProvider::FinishedBlock>
    where
        CustomBlockProvider: BlockProviderWithLifetimeFinish,
    {
        let block = self.take_block()?;

        let finished = self.blocks_provider.finish(block);

        Some(finished)
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

    pub fn value(
        &self,
        index: BlocksIndex,
    ) -> <CustomBlockProvider::Block as BlockWithLifetime>::Item<'_> {
        let block_index = index.block_index();
        let item_index = index.index_in_block();

        let block = &self.blocks[block_index];
        block.index(item_index)
    }

    pub fn reset(&mut self) {
        self.blocks = VecDeque::from(vec![self.blocks_provider.new_block()]);
        self.len = 0;
        self.current_block_index = 0;
        self.finished_blocks_allocated_memory = 0;
    }
}
