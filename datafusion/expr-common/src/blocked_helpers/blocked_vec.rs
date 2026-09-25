use crate::blocked_groups_accumulator::{BlockedEmitTo, BlocksIndex};
use datafusion_common::utils::proxy::VecAllocExt;
use std::collections::VecDeque;
use std::fmt::Debug;
use std::ops::{Index, IndexMut};

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
pub struct BlockedVec<T: Copy> {
    /// Using `VecDeque` so we can remove the first block and reclaim memory
    blocks: VecDeque<Vec<T>>,

    /// The size of each block
    block_size: usize,

    /// The total number of items, not the number of offset since in each block there is the initial offset
    len: usize,

    /// The index of the current block
    current_block_index: usize,
    should_count_current_block: bool,

    finished_blocks_allocated_memory: usize,
}

impl<T: Copy> BlockedVec<T> {
    // TODO - some want to preallocate the blocks and some don't,
    //        there should be a way while avoiding having a lot of memory used if all are prealocatting
    pub fn new(block_size: usize) -> Self {
        assert_ne!(block_size, 0, "block size must be greater than 0");

        let blocks = VecDeque::from(vec![vec![]]);
        BlockedVec {
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

    fn num_blocks(&self) -> usize {
        self.current_block_index + (self.should_count_current_block() as usize)
    }

    fn should_count_current_block(&self) -> bool {
        self.should_count_current_block
            || !self.blocks[self.current_block_index].is_empty()
    }

    pub fn block_size(&self) -> usize {
        self.block_size
    }

    pub fn allocated_size(&self) -> usize {
        self.finished_blocks_allocated_memory
            + self.blocks.capacity() * size_of::<Vec<T>>()
            + self.blocks.back().map_or(0, |b| b.allocated_size())
    }

    fn end_current_block(&mut self) {
        // Don't add to number of blocks since we might not insert into it
        self.current_block_index += 1;
        self.finished_blocks_allocated_memory +=
            self.blocks.back().map_or(0, |b| b.allocated_size());
        let new_block = vec![];

        // Don't count current block since we might not insert into it
        self.should_count_current_block = false;
        self.blocks.push_back(new_block);
    }

    fn reserve_blocks(&mut self, n: usize) {
        self.blocks.reserve(n);
    }

    /// Push length and return if the current block is now full
    pub fn push(&mut self, value: T) -> bool {
        let block = &mut self.blocks[self.current_block_index];

        block.push(value);
        self.len += 1;

        let finished_block = block.len() == self.block_size;

        if finished_block {
            self.end_current_block();
            true
        } else {
            false
        }
    }

    fn current_block_remaining_len(&self) -> usize {
        self.block_size - self.blocks[self.current_block_index].len()
    }

    fn push_value_n_within_block(&mut self, value: T, n: usize) -> bool {
        self.len += n;
        let block = &mut self.blocks[self.current_block_index];

        let new_len = block.len() + n;
        assert!(
            new_len <= self.block_size,
            "overflow from block new block length: {new_len}, block size: {}",
            self.block_size
        );
        block.resize(new_len, value);

        let finished_block = block.len() == self.block_size;

        if finished_block {
            self.end_current_block();
            true
        } else {
            false
        }
    }

    pub fn push_value_n(&mut self, value: T, mut n: usize) {
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

    pub fn push_value_n_to_len(&mut self, value: T, new_len: usize) {
        assert!(
            new_len >= self.len,
            "new_len must be greater than or equal to current len"
        );
        let n = new_len - self.len;
        self.push_value_n(value, n);
    }

    /// Block `block_index` without bounds checking
    ///
    /// # Safety
    /// `block_index < self.num_blocks()`
    #[inline]
    unsafe fn block_unchecked(&self, block_index: usize) -> &Vec<T> {
        debug_assert!(block_index < self.blocks.len());
        unsafe { self.blocks.get(block_index).unwrap_unchecked() }
    }

    /// Mutable block `block_index` without bounds checking
    ///
    /// # Safety
    /// `block_index < self.num_blocks()`
    #[inline]
    unsafe fn block_unchecked_mut(&mut self, block_index: usize) -> &mut Vec<T> {
        debug_assert!(block_index < self.blocks.len());
        unsafe { self.blocks.get_mut(block_index).unwrap_unchecked() }
    }

    /// Get reference to the item at `index` without bounds checking
    ///
    /// # Safety
    /// index must be in bound
    pub unsafe fn get_unchecked(&self, index: BlocksIndex) -> &T {
        let block = unsafe { self.block_unchecked(index.block_index()) };
        unsafe { block.get_unchecked(index.index_in_block()) }
    }

    /// Get mutable reference to the item at `index` without bounds checking
    ///
    /// # Safety
    /// index must be in bound
    pub unsafe fn get_unchecked_mut(&mut self, index: BlocksIndex) -> &mut T {
        let block = unsafe { self.block_unchecked_mut(index.block_index()) };
        unsafe { block.get_unchecked_mut(index.index_in_block()) }
    }

    pub fn emit(&mut self, emit_to: BlockedEmitTo) -> Vec<Vec<T>> {
        match emit_to {
            BlockedEmitTo::All => self.take_all(),
            BlockedEmitTo::NextBlock => {
                self.take_block().map_or(vec![], |next| vec![next])
            }
            BlockedEmitTo::First(n) => {
                vec![self.take_n(n)]
            }
        }
    }

    /// Take every block that counts, see [`Self::num_blocks`]
    pub fn take_all(&mut self) -> Vec<Vec<T>> {
        let num_blocks = self.num_blocks();
        let mut blocks = std::mem::take(&mut self.blocks);
        blocks.truncate(num_blocks);
        self.reset();

        blocks.into()
    }

    /// Take the first block, `None` once there are no more items
    pub fn take_block(&mut self) -> Option<Vec<T>> {
        if self.num_blocks() == 0 {
            return None;
        }
        let block = self.blocks.pop_front().expect("always at least one block");

        if self.blocks.is_empty() {
            self.current_block_index = 0;
            self.should_count_current_block = false;
            self.blocks.push_back(vec![]);
        } else {
            self.current_block_index -= 1;

            // Only reduce memory if not the last one since the last block is calculated separately
            self.finished_blocks_allocated_memory -= block.allocated_size();
        }

        self.len -= block.len();

        Some(block)
    }

    pub fn take_n(&mut self, n: usize) -> Vec<T> {
        assert_ne!(n, 0, "n must be greater than 0");
        assert!(n <= self.len, "n ({n}) must be <= len ({})", self.len);
        assert!(
            n < self.block_size,
            "n ({n}) must be lower than the block size ({}), instead use `take_block` and take_n with the remainder",
            self.block_size
        );

        if n == self.len {
            let blocks = self.take_all();
            return blocks.into_iter().next().unwrap_or_default();
        }

        if n == self.blocks[0].len() {
            return self
                .take_block()
                .expect("must have at least one block since n < len and n != 0");
        }

        let prev_len = self.len;

        debug_assert!(
            self.blocks
                .back()
                .is_some_and(|block| block.len() < self.block_size),
            "the last block must not be full (since it should be the writable tail)"
        );

        // The emitted items are always fully contained in the first block
        let mut taken = Vec::with_capacity(n);
        taken.extend_from_slice(&self.blocks[0][..n]);

        {
            let first = &mut self.blocks[0];
            let first_len = first.len();
            first.copy_within(n.., 0);
            first.truncate(first_len - n);
        }

        // Every block gives its first `n` items to the end of the previous block and
        // shifts the rest down, so every finished block stays full and keeps its allocation
        for i in 1..self.blocks.len() {
            let mut block = std::mem::take(&mut self.blocks[i]);
            let block_len = block.len();
            let moved = n.min(block_len);

            self.blocks[i - 1].extend_from_slice(&block[..moved]);

            block.copy_within(moved.., 0);
            block.truncate(block_len - moved);

            self.blocks[i] = block;
        }

        // The old last block is the only one that can end up empty.
        // If the block before it is full it is the writable tail as is and keeps its allocation,
        // otherwise the block before it is the writable tail and the empty one is not part of the layout
        let tail_is_empty = self.blocks.back().is_some_and(Vec::is_empty);
        let prev_is_full = self.blocks.len() >= 2
            && self.blocks[self.blocks.len() - 2].len() == self.block_size;

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
            .map(|block| block.allocated_size())
            .sum();

        taken
    }

    pub fn reset(&mut self) {
        self.blocks = VecDeque::from(vec![vec![]]);
        self.len = 0;
        self.current_block_index = 0;
        self.finished_blocks_allocated_memory = 0;
        self.should_count_current_block = false;
    }
}

impl<T> Index<BlocksIndex> for BlockedVec<T>
where
    T: Copy,
{
    type Output = T;

    fn index(&self, index: BlocksIndex) -> &Self::Output {
        &self.blocks[index.block_index()][index.index_in_block()]
    }
}

impl<T> IndexMut<BlocksIndex> for BlockedVec<T>
where
    T: Copy,
{
    fn index_mut(&mut self, index: BlocksIndex) -> &mut Self::Output {
        &mut self.blocks[index.block_index()][index.index_in_block()]
    }
}
