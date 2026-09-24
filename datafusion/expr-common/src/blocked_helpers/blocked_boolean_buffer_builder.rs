use crate::blocked_groups_accumulator::{BlockedEmitTo, BlocksIndex};
use arrow::array::BooleanBufferBuilder;
use arrow::buffer::BooleanBuffer;
use std::collections::VecDeque;
use std::fmt::Debug;

/// Blocked [`BooleanBufferBuilder`]
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

impl BlockedBooleanBufferBuilder {
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
            + self.blocks.capacity() * size_of::<BooleanBufferBuilder>()
            + self.blocks.back().map_or(0, builder_allocated_size)
    }

    fn end_current_block(&mut self) {
        // Don't add to number of blocks since we might not insert into it
        self.current_block_index += 1;
        self.finished_blocks_allocated_memory +=
            self.blocks.back().map_or(0, builder_allocated_size);
        let new_block = BooleanBufferBuilder::new(self.block_size);

        // Don't count current block since we might not insert into it
        self.should_count_current_block = false;
        self.blocks.push_back(new_block);
    }

    fn reserve_blocks(&mut self, n: usize) {
        self.blocks.reserve(n);
    }

    /// Push length and return if the current block is now full
    pub fn push(&mut self, value: bool) -> bool {
        let block = &mut self.blocks[self.current_block_index];

        block.append(value);
        self.len += 1;

        let finished_block = block.len() == self.block_size;

        if finished_block {
            self.end_current_block();
            true
        } else {
            false
        }
    }

    pub fn current_block_remaining_len(&self) -> usize {
        self.block_size - self.blocks[self.current_block_index].len()
    }

    fn push_value_n_within_block(&mut self, value: bool, n: usize) -> bool {
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
            self.end_current_block();
            true
        } else {
            false
        }
    }

    pub fn push_value_n(&mut self, value: bool, mut n: usize) {
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

    pub fn emit(&mut self, emit_to: BlockedEmitTo) -> Vec<BooleanBuffer> {
        match emit_to {
            BlockedEmitTo::All => self.take_all(),
            BlockedEmitTo::NextBlock => {
                self.take_block().map_or(vec![], |next| vec![next])
            }
            BlockedEmitTo::First(n) => vec![self.take_n(n)],
        }
    }

    /// Take every block that counts, see [`Self::num_blocks`]
    fn take_all(&mut self) -> Vec<BooleanBuffer> {
        let num_blocks = self.num_blocks();
        let mut blocks = std::mem::take(&mut self.blocks);
        blocks.truncate(num_blocks);
        self.reset();

        blocks.into_iter().map(BooleanBuffer::from).collect()
    }

    /// Take the first block, `None` once there are no more items
    fn take_block(&mut self) -> Option<BooleanBuffer> {
        if self.num_blocks() == 0 {
            return None;
        }
        Some(self.take_first_block())
    }

    /// Take the first block even when it is empty, for callers that know from
    /// elsewhere that the block holds items
    fn take_first_block(&mut self) -> BooleanBuffer {
        let block = self.blocks.pop_front().expect("always at least one block");

        if self.blocks.is_empty() {
            self.current_block_index = 0;
            self.should_count_current_block = false;
            self.blocks
                .push_back(BooleanBufferBuilder::new(self.block_size));
        } else {
            self.current_block_index -= 1;

            // Only reduce memory if not the last one since the last block is calculated separately
            self.finished_blocks_allocated_memory -= builder_allocated_size(&block);
        }

        self.len -= block.len();

        block.build()
    }

    fn take_n(&mut self, n: usize) -> BooleanBuffer {
        assert_ne!(n, 0, "n must be greater than 0");
        assert!(n <= self.len, "n ({n}) must be <= len ({})", self.len);
        assert!(
            n < self.block_size,
            "n ({n}) must be lower than the block size ({}), instead use `take_block` and take_n with the remainder",
            self.block_size
        );

        if n == self.len {
            let blocks = self.take_all();
            return blocks
                .into_iter()
                .next()
                .unwrap_or_else(|| BooleanBuffer::new_unset(0));
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
            let block = std::mem::replace(
                &mut self.blocks[i],
                BooleanBufferBuilder::new(self.block_size),
            );
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
            .map(builder_allocated_size)
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
