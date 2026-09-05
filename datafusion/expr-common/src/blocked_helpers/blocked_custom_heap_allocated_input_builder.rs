use crate::blocked_helpers::take_n_helpers::{
    BlockBuilder, create_adjusted_block_size_iter_for_fixed_blocks, take_n_from_blocks,
};
use crate::groups_accumulator::BlocksIndex;
use datafusion_common::utils::proxy::VecDequeAllocExt;
use std::collections::VecDeque;
use std::fmt::Debug;
use std::marker::PhantomData;
use std::ops::{Index, IndexMut};
use crate::blocked_helpers::{GetHeapAllocatedSize, OnlyOnStackSize};
use crate::blocked_helpers::take_n_helpers_heap_allocated::{take_n_from_heap_blocks, HeapAllocatedBlockBuilder};


pub trait HeapAllocatedBlockProvider {
    type Block: HeapAllocatedBlock;

    fn new_block(&self) -> Self::Block;

    fn allocated_size(&self) -> usize;
}

pub trait HeapAllocatedBlockProviderFinish: HeapAllocatedBlockProvider {
    type FinishedBlock;

    fn finish(&self, block: Self::Block) -> Self::FinishedBlock;
}

pub trait HeapAllocatedBlock {
    type Item;

    /// An optimization flag if the block already hold the heap allocated size of it's items
    /// and will return it in the allocated size
    const ALLOCATED_SIZE_INCLUDE_ITEMS: bool;

    /// Get allocated bytes on heap (not including the items) (not including `size_of::<Self>()`)
    fn allocated_size(&self) -> usize;

    fn push(&mut self, item: Self::Item);

    fn extend(&mut self, iter: impl Iterator<Item = Self::Item>);

    /// Number of items in the block
    fn len(&self) -> usize;

    fn is_empty(&self) -> bool;
}

pub trait HeapAllocatedBlockWithSlice: HeapAllocatedBlock {
    fn extend_from_slice(&mut self, slice: &[Self::Item]);
    fn append_n(&mut self, item: Self::Item, n: usize);
}

/// When `FIXED_BLOCK_SIZING` is true, the block size is the `Self::block_size` otherwise,
/// the callers control the block size
#[derive(Debug)]
pub struct BlockedCustomHeapAllocatedInputBuilder<
    const FIXED_BLOCK_SIZING: bool,
    CustomBlockProvider: HeapAllocatedBlockProvider,
    HeapAllocatedSize: GetHeapAllocatedSize<<CustomBlockProvider::Block as HeapAllocatedBlock>::Item> = OnlyOnStackSize,
> {
    blocks_provider: CustomBlockProvider,
    /// Using `VecDeque` so we can remove the first block and reclaim memory
    blocks: VecDeque<CustomBlockProvider::Block>,
    blocks_heap_allocated_sizes: VecDeque<usize>,
    finished_blocks_allocated_memory: usize,

    /// The size of each block
    block_size: usize,

    /// The total number of items, not the number of offset since in each block there is the initial offset
    len: usize,

    /// The index of the current block
    current_block_index: usize,

    _phantom: PhantomData<HeapAllocatedSize>,
}

impl<const FIXED_BLOCK_SIZING: bool, CustomBlockProvider: HeapAllocatedBlockProvider,
    HeapAllocatedSize: GetHeapAllocatedSize<<CustomBlockProvider::Block as HeapAllocatedBlock>::Item>>
BlockedCustomHeapAllocatedInputBuilder<FIXED_BLOCK_SIZING, CustomBlockProvider, HeapAllocatedSize>
{
    // TODO - some want to preallocate the blocks and some don't,
    //        there should be a way while avoiding having a lot of memory used if all are prealocatting
    pub fn new(block_size: usize, blocks_provider: CustomBlockProvider) -> Self {
        if FIXED_BLOCK_SIZING {
            assert_ne!(block_size, 0, "block size must be greater than 0");
        }

        let blocks = VecDeque::from(vec![blocks_provider.new_block()]);
        let blocks_heap_allocated_sizes = if Self::should_track_blocks_heap_allocation() {
            VecDeque::from(vec![0])
        } else {
            VecDeque::new()
        };
        Self {
            blocks_provider,
            blocks,
            blocks_heap_allocated_sizes,
            finished_blocks_allocated_memory: 0,
            block_size,
            len: 0,
            current_block_index: 0,
            _phantom: PhantomData,
        }
    }

    #[inline(always)]
    const fn should_track_blocks_heap_allocation() -> bool {
        HeapAllocatedSize::HAS_HEAP_ALLOCATION && !CustomBlockProvider::Block::ALLOCATED_SIZE_INCLUDE_ITEMS
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

    pub fn block_size(&self) -> usize {
        self.block_size
    }

    pub fn allocated_size(&self) -> usize {

        self.blocks_provider.allocated_size()
          // contain both blocks allocated_size and each item heap allocated size
            + self.finished_blocks_allocated_memory
            + self.blocks.allocated_size()
            + self.blocks.back().map_or(0, |b| b.allocated_size())
            + self.blocks_heap_allocated_sizes.allocated_size()
            + self.blocks_heap_allocated_sizes.back().map_or(0, |b| *b)
    }

    /// Get the number of elements in the current block
    pub fn current_block_len(&self) -> usize {
        self.blocks[self.current_block_index].len()
    }

    pub fn start_new_block(&mut self) {
        // Don't add to number of blocks since we might not insert into it
        self.current_block_index += 1;
        self.finished_blocks_allocated_memory +=
            self.blocks.back().map_or(0, |b| b.allocated_size())
            + self.blocks_heap_allocated_sizes.back().map_or(0, |b| *b);

        if Self::should_track_blocks_heap_allocation() {
            self.blocks_heap_allocated_sizes.push_back(0);
        }
        let new_block = self.blocks_provider.new_block();
        self.blocks.push_back(new_block);
    }

    pub(crate) fn reserve_blocks(&mut self, n: usize) {
        self.blocks.reserve(n);
        if Self::should_track_blocks_heap_allocation() {
            self.blocks_heap_allocated_sizes.reserve(n);
        }
    }

    pub fn index_mut_with_size(&mut self, index: BlocksIndex, update_fn: impl FnOnce(&mut <CustomBlockProvider::Block as HeapAllocatedBlock>::Item))
    where Self:
    IndexMut<BlocksIndex, Output = <CustomBlockProvider::Block as HeapAllocatedBlock>::Item> {
        if Self::should_track_blocks_heap_allocation() {
            let before = HeapAllocatedSize::get_heap_allocated_size(&self[index]);
            update_fn(&mut self[index]);
            let after = HeapAllocatedSize::get_heap_allocated_size(&self[index]);
            let mem = &mut self.blocks_heap_allocated_sizes[index.block_index()];
            *mem = *mem - before + after;

            // Finished blocks are already counted in the total, keep it in sync
            // so that taking the block later subtracts what was added
            if index.block_index() != self.current_block_index {
                self.finished_blocks_allocated_memory =
                    self.finished_blocks_allocated_memory - before + after;
            }
        } else {
            update_fn(&mut self[index]);
        }
    }

    /// Push length and return if the current block is now full
    pub fn push(&mut self, value: <CustomBlockProvider::Block as HeapAllocatedBlock>::Item) -> bool {
        let block = &mut self.blocks[self.current_block_index];

        if Self::should_track_blocks_heap_allocation() {
            self.blocks_heap_allocated_sizes[self.current_block_index] += HeapAllocatedSize::get_heap_allocated_size(&value);
        }

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
        iter: impl Iterator<Item = <CustomBlockProvider::Block as HeapAllocatedBlock>::Item>,
    ) -> bool {
        let block = &mut self.blocks[self.current_block_index];

        let prev_block_len = block.len();
        if Self::should_track_blocks_heap_allocation() {
            let mut heap_allocated = 0;
            block.extend(iter.map(|item| {
                heap_allocated += HeapAllocatedSize::get_heap_allocated_size(&item);
                item
            }));

            self.blocks_heap_allocated_sizes[self.current_block_index] += heap_allocated;
        } else {
        block.extend(iter);
        }

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
        slice: &[<CustomBlockProvider::Block as HeapAllocatedBlock>::Item],
    ) -> bool
    where
        CustomBlockProvider::Block: HeapAllocatedBlockWithSlice,
    {
        let block = &mut self.blocks[self.current_block_index];

        let prev_block_len = block.len();
        if Self::should_track_blocks_heap_allocation() {
            self.blocks_heap_allocated_sizes[self.current_block_index] += slice.iter().map(|item| HeapAllocatedSize::get_heap_allocated_size(item)).sum::<usize>();
        }

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
        mut buffer: &[<CustomBlockProvider::Block as HeapAllocatedBlock>::Item],
    ) where
        CustomBlockProvider::Block: HeapAllocatedBlockWithSlice,
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
        value: <CustomBlockProvider::Block as HeapAllocatedBlock>::Item,
        n: usize,
    ) -> bool
    where
        CustomBlockProvider::Block: HeapAllocatedBlockWithSlice,
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

        if Self::should_track_blocks_heap_allocation() {
            let item_size = HeapAllocatedSize::get_heap_allocated_size(&value);
            self.blocks_heap_allocated_sizes[self.current_block_index] += item_size;
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
        CustomBlockProvider::Block: HeapAllocatedBlockWithSlice,
        <CustomBlockProvider::Block as HeapAllocatedBlock>::Item: Default + Clone,
    {
        self.push_value_n(<CustomBlockProvider::Block as HeapAllocatedBlock>::Item::default(), n);
    }

    pub fn push_value_n(
        &mut self,
        value: <CustomBlockProvider::Block as HeapAllocatedBlock>::Item,
        mut n: usize,
    ) where
        CustomBlockProvider::Block: HeapAllocatedBlockWithSlice,
        <CustomBlockProvider::Block as HeapAllocatedBlock>::Item: Clone,
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

        let block_heap_size = if Self::should_track_blocks_heap_allocation() {
            self.blocks_heap_allocated_sizes.pop_front().expect("len > 0 so must have block size")
        } else {
            0
        };

        if self.blocks.is_empty() {
            assert_eq!(self.blocks_heap_allocated_sizes.len(), 0, "if blocks are empty, heap allocated sizes should be empty as well");

            self.current_block_index = 0;
            self.blocks.push_back(self.blocks_provider.new_block());
            if Self::should_track_blocks_heap_allocation() {
                self.blocks_heap_allocated_sizes.push_back(0);
            }
        } else {
            self.current_block_index -= 1;

            // Only reduce memory if not the last one since the last block is calculated separately
            self.finished_blocks_allocated_memory -= block.allocated_size();

            if Self::should_track_blocks_heap_allocation() {
                self.finished_blocks_allocated_memory -= block_heap_size
            }
        }

        self.len -= block.len();

        Some(block)
    }

    pub fn take_block_finished(&mut self) -> Option<CustomBlockProvider::FinishedBlock>
    where
        CustomBlockProvider: HeapAllocatedBlockProviderFinish,
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

    /// Take every non empty block
    pub fn take_all_with_mem(&mut self) -> Vec<(CustomBlockProvider::Block, usize)> {
        let blocks = std::mem::take(&mut self.blocks);
        let blocks_mem = if Self::should_track_blocks_heap_allocation() {
            std::mem::take(&mut self.blocks_heap_allocated_sizes)
        } else {
            VecDeque::from(vec![0; blocks.len()])
        };
        self.reset();

        blocks
          .into_iter()
          .zip(blocks_mem.into_iter())
          .filter(|((block, _mem))| !block.is_empty())
          .collect()
    }

    pub fn take_n(
        &mut self,
        n: usize,
        adjusted_block_size_iter: Option<impl Iterator<Item = usize> + Clone>,
    ) -> <CustomBlockProvider::Block as HeapAllocatedBlockBuilder>::Output
    where
      CustomBlockProvider::Block: HeapAllocatedBlockBuilder,
    HeapAllocatedSize: GetHeapAllocatedSize<<<CustomBlockProvider::Block as HeapAllocatedBlockBuilder>::Output as HeapAllocatedBlock>::Item>,
    {
        assert_eq!(FIXED_BLOCK_SIZING, adjusted_block_size_iter.is_none());

        let (taken, layout) = if Self::should_track_blocks_heap_allocation() {
            if let Some(iter) = adjusted_block_size_iter {
                take_n_from_heap_blocks::<CustomBlockProvider::Block, HeapAllocatedSize>(&mut self.blocks, &mut self.blocks_heap_allocated_sizes, self.len, n, None, iter)
            } else {
                take_n_from_heap_blocks::<CustomBlockProvider::Block, HeapAllocatedSize>(
                    &mut self.blocks,
                    &mut self.blocks_heap_allocated_sizes,
                    self.len,
                    n,
                    Some(self.block_size),
                    create_adjusted_block_size_iter_for_fixed_blocks(
                        self.len,
                        n,
                        self.block_size,
                    ),
                )
            }
        } else {
            if let Some(iter) = adjusted_block_size_iter {
                take_n_from_heap_blocks::<CustomBlockProvider::Block, OnlyOnStackSize>(&mut self.blocks, &mut VecDeque::new(), self.len, n, None, iter)
            } else {
                take_n_from_heap_blocks::<CustomBlockProvider::Block, OnlyOnStackSize>(
                    &mut self.blocks,
                    &mut VecDeque::new(),
                    self.len,
                    n,
                    Some(self.block_size),
                    create_adjusted_block_size_iter_for_fixed_blocks(
                        self.len,
                        n,
                        self.block_size,
                    ),
                )
            }
        };

        self.len = layout.len;
        self.current_block_index = layout.current_block_index;
        self.finished_blocks_allocated_memory = layout.finished_blocks_allocated_size;
        if Self::should_track_blocks_heap_allocation() {
            assert_ne!(layout.block_heap_allocated_size.len(), 0);
            self.blocks_heap_allocated_sizes = layout.block_heap_allocated_size;
        } else {
            assert_eq!(layout.block_heap_allocated_size.len(), 0);
        }

        taken
    }

    pub fn reset(&mut self) {
        self.blocks = VecDeque::from(vec![self.blocks_provider.new_block()]);
        if Self::should_track_blocks_heap_allocation() {
            self.blocks_heap_allocated_sizes = VecDeque::from(vec![0]);
        }
        self.len = 0;
        self.current_block_index = 0;
        self.finished_blocks_allocated_memory = 0;
    }
}

impl<CustomBlockProvider: HeapAllocatedBlockProvider,
    HeapAllocatedSize: GetHeapAllocatedSize<<CustomBlockProvider::Block as HeapAllocatedBlock>::Item>>
BlockedCustomHeapAllocatedInputBuilder<true, CustomBlockProvider, HeapAllocatedSize> {
    pub fn take_n_fixed(&mut self, n: usize) -> <CustomBlockProvider::Block as HeapAllocatedBlockBuilder>::Output
    where
      CustomBlockProvider::Block: HeapAllocatedBlockBuilder,
      HeapAllocatedSize: GetHeapAllocatedSize<<<CustomBlockProvider::Block as HeapAllocatedBlockBuilder>::Output as HeapAllocatedBlock>::Item>,
    {
        self.take_n(n, None::<std::iter::Empty<_>>)
    }
}

impl<const FIXED_BLOCK_SIZING: bool, CustomBlockProvider: HeapAllocatedBlockProvider, HeapAllocatedSize>
    Extend<<CustomBlockProvider::Block as HeapAllocatedBlock>::Item>
    for BlockedCustomHeapAllocatedInputBuilder<FIXED_BLOCK_SIZING, CustomBlockProvider, HeapAllocatedSize>
where
  HeapAllocatedSize: GetHeapAllocatedSize<<CustomBlockProvider::Block as HeapAllocatedBlock>::Item>,
{
    fn extend<T: IntoIterator<Item = <CustomBlockProvider::Block as HeapAllocatedBlock>::Item>>(
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

impl<CustomBlockProvider, HeapAllocatedSize> Index<usize>
    for BlockedCustomHeapAllocatedInputBuilder<true, CustomBlockProvider, HeapAllocatedSize>
where
    CustomBlockProvider: HeapAllocatedBlockProvider,
    CustomBlockProvider::Block:
        Index<usize, Output = <CustomBlockProvider::Block as HeapAllocatedBlock>::Item>,
    HeapAllocatedSize: GetHeapAllocatedSize<<CustomBlockProvider::Block as HeapAllocatedBlock>::Item>,
{
    type Output = <CustomBlockProvider::Block as HeapAllocatedBlock>::Item;

    fn index(&self, index: usize) -> &Self::Output {
        self.index(BlocksIndex::from_index_in_fixed_block_size(
            index,
            self.block_size,
        ))
    }
}

impl<CustomBlockProvider, HeapAllocatedSize> IndexMut<usize>
    for BlockedCustomHeapAllocatedInputBuilder<true, CustomBlockProvider, HeapAllocatedSize>
where
    CustomBlockProvider: HeapAllocatedBlockProvider,
    CustomBlockProvider::Block:
        IndexMut<usize, Output = <CustomBlockProvider::Block as HeapAllocatedBlock>::Item>,
    HeapAllocatedSize: GetHeapAllocatedSize<<CustomBlockProvider::Block as HeapAllocatedBlock>::Item>,

{
    fn index_mut(&mut self, index: usize) -> &mut Self::Output {
        self.index_mut(BlocksIndex::from_index_in_fixed_block_size(
            index,
            self.block_size,
        ))
    }
}

impl<const FIXED_BLOCK_SIZING: bool, CustomBlockProvider, HeapAllocatedSize> Index<BlocksIndex>
    for BlockedCustomHeapAllocatedInputBuilder<FIXED_BLOCK_SIZING, CustomBlockProvider, HeapAllocatedSize>
where
    CustomBlockProvider: HeapAllocatedBlockProvider,
    CustomBlockProvider::Block:
        Index<usize, Output = <CustomBlockProvider::Block as HeapAllocatedBlock>::Item>,
    HeapAllocatedSize: GetHeapAllocatedSize<<CustomBlockProvider::Block as HeapAllocatedBlock>::Item>,
{
    type Output = <CustomBlockProvider::Block as HeapAllocatedBlock>::Item;

    fn index(&self, index: BlocksIndex) -> &Self::Output {
        &self.blocks[index.block_index()][index.index_in_block()]
    }
}

impl<const FIXED_BLOCK_SIZING: bool, CustomBlockProvider,
    HeapAllocatedSize,
> IndexMut<BlocksIndex>
    for BlockedCustomHeapAllocatedInputBuilder<FIXED_BLOCK_SIZING, CustomBlockProvider, HeapAllocatedSize>
where
    CustomBlockProvider: HeapAllocatedBlockProvider,
    CustomBlockProvider::Block:
        Index<usize, Output = <CustomBlockProvider::Block as HeapAllocatedBlock>::Item>,
    CustomBlockProvider::Block:
        IndexMut<usize, Output = <CustomBlockProvider::Block as HeapAllocatedBlock>::Item>,
    HeapAllocatedSize: GetHeapAllocatedSize<<CustomBlockProvider::Block as HeapAllocatedBlock>::Item>,

{
    fn index_mut(&mut self, index: BlocksIndex) -> &mut Self::Output {
        &mut self.blocks[index.block_index()][index.index_in_block()]
    }
}
