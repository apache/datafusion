use datafusion_common::utils::proxy::{VecAllocExt};
use std::collections::VecDeque;
use std::fmt::Debug;
use std::ops::{Index, IndexMut};
use arrow::array::BooleanBufferBuilder;
use arrow::buffer::{BooleanBuffer, ScalarBuffer};
use arrow::datatypes::ArrowNativeType;
use crate::blocked_groups_accumulator::{BlockedEmitTo, BlocksIndex};


/// Blocked Vec
///
/// # Implementation Notes
///
/// ## Why `T: Copy`?
///
///
/// 1. So the [`BlockedVec::allocated_size`] will be accurate since the size of T is known, and not include heap allocations (like `String` or `Vec`) that are not part of the allocated size of the `BlockedVec`
/// 2. So we can provide mutable access to the items (e.g. `IndexMut`) since if `T` is not `Copy` (like when `T` is a `Vec`) we could change the size of it without the [`BlockedVec::allocated_size`] changing, which would be confusing and lead to bugs
///
#[derive(Debug)]
pub struct BlockedBooleanBufferBuilder {
    /// Using `VecDeque` so we can remove the first block and reclaim memory
    blocks: VecDeque<BooleanBufferBuilder>,

    /// The size of each block
    block_size: usize,

    /// The total number of items, not the number of offset since in each block there is the initial offset
    len: usize,

    /// The index of the current block
    current_block_index: usize,
    should_count_current_block: bool,

    finished_blocks_allocated_memory: usize,
}

impl BlockedBooleanBufferBuilder
{
    // TODO - some want to preallocate the blocks and some don't,
    //        there should be a way while avoiding having a lot of memory used if all are prealocatting
    pub fn new(block_size: usize) -> Self {
        assert_ne!(block_size, 0, "block size must be greater than 0");

        let blocks = VecDeque::from(vec![BooleanBufferBuilder::new(block_size)]);
        Self {
            blocks,
            block_size,
            should_count_current_block: false,
            len: 0,
            current_block_index: 0,
            finished_blocks_allocated_memory: 0,
        }
    }

    pub fn is_empty(&self) -> bool {
        self.len == 0
    }

    pub fn len(&self) -> usize {
        self.len
    }

    pub fn num_blocks(&self) -> usize {
        self.current_block_index + (self.should_count_current_block() as usize)
    }

    fn should_count_current_block(&self) -> bool {
        self.should_count_current_block || !self.blocks[self.current_block_index].is_empty()
    }

    pub fn block(&self, block_index: usize) -> &BooleanBufferBuilder {
        &self.blocks[block_index]
    }

    pub fn block_size(&self) -> usize {
        self.block_size
    }

    pub fn allocated_size(&self) -> usize {
        self.finished_blocks_allocated_memory
            + self.blocks.capacity() * size_of::<BooleanBufferBuilder>()
            + self.blocks.back().map_or(0, |b| builder_allocated_size(b))
    }

    /// Get the number of elements in the current block
    pub fn current_block_len(&self) -> usize {
        self.blocks[self.current_block_index].len()
    }

    pub fn start_new_block(&mut self) {
        // a block that was only pre-opened becomes the started block instead of an empty
        // block of its own
        if self.should_count_current_block() {
            self.end_current_block();
        }
        self.should_count_current_block = true;
    }

    pub(crate) fn mark_current_block_counted(&mut self) {
        self.should_count_current_block = true;
    }

    pub fn end_current_block(&mut self) {
        self.end_current_block_inner();
    }

    fn end_current_block_inner(&mut self) {
        // Don't add to number of blocks since we might not insert into it
        self.current_block_index += 1;
        self.finished_blocks_allocated_memory +=
          self.blocks.back().map_or(0, |b| builder_allocated_size(b));
        let new_block = BooleanBufferBuilder::new(self.block_size);

        // Don't count current block since we might not insert into it
        self.should_count_current_block = false;
        self.blocks.push_back(new_block);
    }

    pub(crate) fn reserve_blocks(&mut self, n: usize) {
        self.blocks.reserve(n);
    }

    /// Push length and return if the current block is now full
    pub fn push(&mut self, value: bool) -> bool {
        let block = &mut self.blocks[self.current_block_index];

        block.append(value);
        self.len += 1;

        let finished_block = block.len() == self.block_size;

        if finished_block {
            self.end_current_block_inner();
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
        iter: impl Iterator<Item = bool>,
    ) -> bool {
        let block = &mut self.blocks[self.current_block_index];

        let prev_block_len = block.len();

        // TODO - add to arrow extend support for iterators of bools, but for now just append one by one
        for value in iter {
            block.append(value);
        }

        assert!(
            block.len() <= self.block_size,
            "overflow from block new block length: {}, block size: {}",
            block.len(),
            self.block_size
        );

        let added_items = block.len() - prev_block_len;
        self.len += added_items;

        let finished_block = block.len() == self.block_size;

        if finished_block {
            self.end_current_block_inner();
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
        slice: &[bool],
    ) -> bool
    {
        let block = &mut self.blocks[self.current_block_index];

        let prev_block_len = block.len();
        block.append_slice(slice);

        assert!(
            block.len() <= self.block_size,
            "overflow from block new block length: {}, block size: {}",
            block.len(),
            self.block_size
        );

        let added_items = block.len() - prev_block_len;
        self.len += added_items;

        let finished_block = block.len() == self.block_size;

        if finished_block {
            self.end_current_block_inner();
            true
        } else {
            false
        }
    }

    /// Extend the length from the current offsets
    pub fn extend_from_slice(
        &mut self,
        mut buffer: &[bool],
    ) {
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
        self.block_size - self.blocks[self.current_block_index].len()
    }

    pub fn push_value_n_within_block(
        &mut self,
        value: bool,
        n: usize,
    ) -> bool
    {
        self.len += n;
        let block = &mut self.blocks[self.current_block_index];

        let new_len = block.len() + n;
        assert!(
            new_len <= self.block_size,
            "overflow from block new block length: {new_len}, block size: {}",
            self.block_size
        );
        block.append_n(n, value);

        let finished_block = block.len() == self.block_size;

        if finished_block {
            self.end_current_block_inner();
            true
        } else {
            false
        }
    }

    pub fn push_value_n(
        &mut self,
        value: bool,
        mut n: usize,
    ) {
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

    pub fn push_value_n_to_len(&mut self, value: bool, new_len: usize) {
        assert!(new_len >= self.len, "new_len must be greater than or equal to current len");
        let n = new_len - self.len;
        self.push_value_n(value, n);
    }

    pub fn get_bit(&self, index: BlocksIndex) -> bool {
        let block_index = index.block_index();
        let index_in_block = index.index_in_block();

        self.blocks[block_index].get_bit(index_in_block)
    }

    pub fn set_bit(&mut self, index: BlocksIndex, value: bool) {
        let block_index = index.block_index();
        let index_in_block = index.index_in_block();

        self.blocks[block_index].set_bit(index_in_block, value)
    }

    /// Block `block_index` without bounds checking
    ///
    /// # Safety
    /// `block_index < self.num_blocks()`
    #[inline]
    pub unsafe fn block_unchecked(&self, block_index: usize) -> &BooleanBufferBuilder {
        debug_assert!(block_index < self.blocks.len());
        unsafe { self.blocks.get(block_index).unwrap_unchecked() }
    }

    /// Mutable block `block_index` without bounds checking
    ///
    /// # Safety
    /// `block_index < self.num_blocks()`
    #[inline]
    pub unsafe fn block_unchecked_mut(
        &mut self,
        block_index: usize,
    ) -> &mut BooleanBufferBuilder {
        debug_assert!(block_index < self.blocks.len());
        unsafe { self.blocks.get_mut(block_index).unwrap_unchecked() }
    }

    pub fn blocks_mut(&mut self) -> impl Iterator<Item = &mut BooleanBufferBuilder> {
        let num_blocks = self.num_blocks();
        self.blocks.iter_mut().take(num_blocks)
    }

    pub fn current_block_mut(&mut self) -> &mut BooleanBufferBuilder {
        &mut self.blocks[self.current_block_index]
    }

    pub fn current_or_block_mut(&mut self, block_index: usize) -> &mut BooleanBufferBuilder {
        &mut self.blocks[block_index]
    }

    pub fn emit(&mut self, emit_to: BlockedEmitTo) -> Vec<BooleanBuffer> {
        match emit_to {
            BlockedEmitTo::All => {
                let counts = self.take_all();

                counts
            }
            BlockedEmitTo::NextBlock => {
                self.take_block().map_or(vec![], |next| vec![next])
            }
            BlockedEmitTo::First(n) => {
                vec![self.take_n(n)]
            }
        }
    }

    /// Take every block that counts, see [`Self::num_blocks`]
    pub fn take_all(&mut self) -> Vec<BooleanBuffer> {
        let num_blocks = self.num_blocks();
        let mut blocks = std::mem::take(&mut self.blocks);
        blocks.truncate(num_blocks);
        self.reset();

        blocks.into_iter().map(BooleanBuffer::from).collect()
    }

    /// Take the first block, `None` once there are no more items
    pub fn take_block(&mut self) -> Option<BooleanBuffer> {
        if self.num_blocks() == 0 {
            return None;
        }
        Some(self.take_first_block())
    }

    /// Take the first block even when it is empty, for callers that know from
    /// elsewhere that the block holds items
    pub fn take_first_block(&mut self) -> BooleanBuffer {
        let block = self.blocks.pop_front().expect("always at least one block");

        if self.blocks.is_empty() {
            self.current_block_index = 0;
            self.should_count_current_block = false;
            self.blocks.push_back(BooleanBufferBuilder::new(self.block_size));
        } else {
            self.current_block_index -= 1;

            // Only reduce memory if not the last one since the last block is calculated separately
            self.finished_blocks_allocated_memory -= builder_allocated_size(&block);
        }

        self.len -= block.len();

        block.build()
    }

    pub fn take_n(
        &mut self,
        n: usize,
    ) -> BooleanBuffer {
        assert_ne!(n, 0, "n must be greater than 0");
        assert!(n <= self.len, "n ({n}) must be <= len ({})", self.len);
        assert!(n < self.block_size, "n ({n}) must be lower than the block size ({}), instead use `take_block` and take_n with the remainder", self.block_size);

        if n == self.len {
            let blocks = self.take_all();
            return blocks.into_iter().next().unwrap_or_else(|| BooleanBuffer::new_unset(0))
        }

        if n == self.blocks[0].len() {
            return self.take_block().expect("must have at least one block since n < len and n != 0");
        }

        let prev_len = self.len;


        debug_assert!(
            self.blocks.back().is_some_and(|block| block.len() < self.block_size),
            "the last block must not be full (since it should be the writable tail)"
        );

        // The emitted items are always fully contained in the first block
        let mut taken = BooleanBufferBuilder::new(n);
        taken.append_packed_range(0..n, self.blocks[0].as_slice());

        {
            let first = &mut self.blocks[0];
            let first_len = first.len();

            // TODO - reuse the same allocation by adding copy_within
            let mut new_first = BooleanBufferBuilder::new(first_len - n);
            new_first.append_packed_range(n..first_len, first.as_slice());
            *first = new_first;
        }

        // Every block gives its first `n` items to the end of the previous block and
        // shifts the rest down, so every finished block stays full and keeps its allocation
        for i in 1..self.blocks.len() {
            let mut block = std::mem::replace(&mut self.blocks[i], BooleanBufferBuilder::new(self.block_size));
            let block_len = block.len();
            let moved = n.min(block_len);

            self.blocks[i - 1].append_packed_range(0..moved, block.as_slice());

            // Copy to the new block
            self.blocks[i].append_packed_range(moved..block.len(), block.as_slice());
        }

        // The old last block is the only one that can end up empty.
        // If the block before it is full it is the writable tail as is and keeps its allocation,
        // otherwise the block before it is the writable tail and the empty one is not part of the layout
        let tail_is_empty = self.blocks.back().is_some_and(|b| b.is_empty());
        let prev_is_full = self.blocks.len() >= 2 && self.blocks[self.blocks.len() - 2].len() == self.block_size;

        if tail_is_empty && !prev_is_full {
            self.blocks.pop_back();
        }

        self.len = prev_len - n;
        self.current_block_index = self.blocks.len() - 1;
        self.should_count_current_block = !tail_is_empty || !prev_is_full;
        self.finished_blocks_allocated_memory = self
            .blocks
            .iter()
            .take(self.current_block_index)
            .map(|block| builder_allocated_size(block))
            .sum();

        taken.build()
    }

    pub fn reset(&mut self) {
        self.blocks = VecDeque::from(vec![BooleanBufferBuilder::new(self.block_size)]);
        self.len = 0;
        self.current_block_index = 0;
        self.finished_blocks_allocated_memory = 0;
        self.should_count_current_block = false;
    }
}

fn builder_allocated_size(block: &BooleanBufferBuilder) -> usize {
    block.capacity() / 8 // capacity in bits
}
