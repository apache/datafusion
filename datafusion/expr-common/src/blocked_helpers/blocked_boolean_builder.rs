use crate::blocked_helpers::take_n_helpers::{BlockBuilder, take_n_from_blocks};
use crate::groups_accumulator::BlocksIndex;
use arrow::array::{AsArray, BooleanBufferBuilder, new_empty_array};
use arrow::buffer::BooleanBuffer;
use arrow::datatypes::DataType;
use datafusion_common::utils::proxy::VecDequeAllocExt;
use std::collections::VecDeque;
use std::ops::{Index, Range};

#[derive(Debug)]
pub struct BlockedBooleanBuilder<const FIXED_BLOCK_SIZING: bool> {
    /// Using `VecDeque` so we can remove the first block and reclaim memory
    blocks: VecDeque<BooleanBufferBuilder>,

    /// The size of each block
    block_size: usize,

    /// The index of the current block
    current_block_index: usize,

    len: usize,

    finished_blocks_allocated_size: usize,
}

impl<const FIXED_BLOCK_SIZING: bool> BlockedBooleanBuilder<FIXED_BLOCK_SIZING> {
    pub fn new(block_size: usize) -> Self {
        if FIXED_BLOCK_SIZING {
            assert_ne!(block_size, 0, "block size must be greater than 0");
        }

        let blocks = VecDeque::from(vec![BooleanBufferBuilder::new(block_size)]);

        BlockedBooleanBuilder {
            blocks,
            block_size,
            current_block_index: 0,
            len: 0,
            finished_blocks_allocated_size: 0,
        }
    }

    pub fn len(&self) -> usize {
        self.len
    }

    pub fn is_empty(&self) -> bool {
        self.len == 0
    }

    pub fn allocated_size(&self) -> usize {
        self.finished_blocks_allocated_size
            + self.blocks.allocated_size()
            + self.blocks.back().map_or(0, allocated_size_for_builder)
    }

    pub fn block_size(&self) -> usize {
        assert!(
            FIXED_BLOCK_SIZING,
            "block size is only available for manual block"
        );
        self.block_size
    }

    pub fn start_new_block(&mut self) {
        self.current_block_index += 1;
        self.finished_blocks_allocated_size +=
            self.blocks.back().map_or(0, allocated_size_for_builder);
        let new_block = BooleanBufferBuilder::new(self.block_size);
        self.blocks.push_back(new_block);
    }

    pub(crate) fn reserve_blocks(&mut self, n: usize) {
        self.blocks.reserve(n);
    }

    fn current_block_remaining_len(&self) -> usize {
        assert!(
            FIXED_BLOCK_SIZING,
            "remaining block only available for manual block"
        );
        self.block_size - self.blocks[self.current_block_index].len()
    }

    fn push_n_within_block(&mut self, n: usize, is_set: bool) {
        self.len += n;
        let block = &mut self.blocks[self.current_block_index];

        block.append_n(n, is_set);

        assert!(
            !FIXED_BLOCK_SIZING || block.len() <= self.block_size,
            "overflow from block new block length: {}, block size: {}",
            block.len(),
            self.block_size
        );

        if FIXED_BLOCK_SIZING && block.len() == self.block_size {
            self.start_new_block();
        }
    }

    pub fn append_n(&mut self, mut n: usize, is_set: bool) {
        if !FIXED_BLOCK_SIZING {
            self.push_n_within_block(n, is_set);
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

            self.push_n_within_block(to_add, is_set);
        }
    }

    pub fn append(&mut self, is_set: bool) {
        let block = &mut self.blocks[self.current_block_index];

        block.append(is_set);
        self.len += 1;

        if FIXED_BLOCK_SIZING && block.len() == self.block_size {
            self.start_new_block();
        }
    }

    pub fn get_bit(&self, blocked_index: BlocksIndex) -> bool {
        self.blocks[blocked_index.block_index()].get_bit(blocked_index.index_in_block())
    }

    pub fn set_bit(&mut self, blocked_index: BlocksIndex, is_set: bool) {
        self.blocks[blocked_index.block_index()]
            .set_bit(blocked_index.index_in_block(), is_set)
    }

    /// Extends iterator of validity within current block
    /// Returns how many items were added
    ///
    /// # Panics
    /// Panics if the iterator length exceeds the remaining size of the current block
    pub fn extend_validity_in_block(
        &mut self,
        iter: impl Iterator<Item = bool>,
    ) -> usize {
        let block = &mut self.blocks[self.current_block_index];

        let prev_block_len = block.len();

        for is_valid in iter {
            block.append(is_valid);
        }

        assert!(
            !FIXED_BLOCK_SIZING || block.len() <= self.block_size,
            "overflow from block new block length: {}, block size: {}",
            block.len(),
            self.block_size
        );

        let added_items = block.len() - prev_block_len;
        self.len += added_items;

        if FIXED_BLOCK_SIZING && block.len() == self.block_size {
            self.start_new_block();
        }

        added_items
    }

    /// Take the first block, `None` once there are no more items
    pub fn take_block(&mut self) -> Option<BooleanBuffer> {
        if self.len == 0 {
            return None;
        }

        let block = self
            .blocks
            .pop_front()
            .expect("len > 0 so must have a block");
        self.len -= block.len();

        if self.blocks.is_empty() {
            self.current_block_index = 0;
            self.blocks
                .push_back(BooleanBufferBuilder::new(self.block_size));
        } else {
            self.current_block_index -= 1;

            // Only if not the current block reduce the memory since current block is calculated separately
            self.finished_blocks_allocated_size -= allocated_size_for_builder(&block);
        }

        Some(block.build())
    }

    /// Take every non empty block
    pub fn take_all(&mut self) -> Vec<BooleanBuffer> {
        let blocks = std::mem::take(&mut self.blocks);
        assert_eq!(self.current_block_index, blocks.len() - 1);

        // TODO - should preallocate? can be expensive for large schema
        self.blocks
            .push_back(BooleanBufferBuilder::new(self.block_size));
        self.len = 0;
        self.current_block_index = 0;
        self.finished_blocks_allocated_size = 0;

        blocks
            .into_iter()
            .filter(|b| !b.is_empty())
            .map(|b| b.build())
            .collect()
    }

    /// Take the first `n` values
    ///
    /// `block_size_iterator` is iterator over the number of items in each block **after** emitting `n`
    ///
    /// this is `None` when `FIXED_BLOCK_SIZING` is true
    ///
    /// The adjusted iterator must meet this requirement:
    /// ```
    /// assert_eq!(n + adjusted_block_size_iter.sum(), self.len);
    /// ```
    ///
    /// TODO - shrink to fit
    ///
    ///
    pub fn take_n(
        &mut self,
        n: usize,
        adjusted_block_size_iter: Option<impl Iterator<Item = usize> + Clone>,
    ) -> BooleanBuffer {
        assert_eq!(FIXED_BLOCK_SIZING, adjusted_block_size_iter.is_none());
        if let Some(adjusted_block_size_iter) = adjusted_block_size_iter {
            let (taken, layout) = take_n_from_blocks(
                &mut self.blocks,
                self.len,
                n,
                None,
                adjusted_block_size_iter,
            );

            self.len = layout.len;
            self.current_block_index = layout.current_block_index;
            self.finished_blocks_allocated_size = layout.finished_blocks_allocated_size;

            taken
        } else {
            assert!(n <= self.len, "n ({n}) must be <= len ({}) than", self.len);
            assert!(
                n <= self.block_size,
                "n ({n}) must be lower than block size ({}), instead use `take_block` and take_n with the remainder",
                self.block_size
            );

            // Not moving anything
            if n == 0 {
                return Self::new_empty_buffer();
            }

            if n == self.len || n == self.block_size {
                return self.take_block().expect("must have block");
            }

            // Every block other than the last one is exactly `block_size` long and `n` is smaller
            // than that, so the emitted values are always fully contained in the first block
            let mut taken = BooleanBufferBuilder::new(n);
            taken.append_packed_range(0..n, self.blocks[0].as_slice());

            // Reused for swapping blocks out of the deque, `new(0)` holds no buffer
            let mut placeholder = BooleanBufferBuilder::new(0);

            // Shift every block down by `n` and refill it from the front of the next one
            // so that all blocks but the last stay exactly `block_size` long
            for index in 0..self.blocks.len() {
                let block_len = self.blocks[index].len();

                if block_len <= n {
                    // Only reachable for the last block, everything it held was already
                    // pulled into the previous block
                    self.blocks[index].truncate(0);
                } else {
                    self.blocks[index].shift_down(n, block_len - n);
                }

                let next_index = index + 1;

                if next_index < self.blocks.len() {
                    // Move the next block aside so the current one can be borrowed mutably
                    // it has not been shifted yet, so its first `n` values are the ones we want
                    std::mem::swap(&mut self.blocks[next_index], &mut placeholder);

                    let to_copy = n.min(placeholder.len());

                    self.blocks[index]
                        .append_packed_range(0..to_copy, placeholder.as_slice());

                    std::mem::swap(&mut self.blocks[next_index], &mut placeholder);
                }
            }

            self.len -= n;

            // The last block is allowed to be empty, which is the state `start_new_block` leaves
            // behind when a block fills up exactly
            let new_blocks_count = self.len / self.block_size + 1;

            while self.blocks.len() > new_blocks_count {
                // The back block is measured separately so dropping it needs no adjustment
                self.blocks.pop_back();

                // Whatever is now at the back stopped being a finished block
                self.finished_blocks_allocated_size -=
                    self.blocks.back().map_or(0, allocated_size_for_builder);
            }

            self.current_block_index = self.blocks.len() - 1;

            taken.build()
        }
    }

    fn new_empty_buffer() -> BooleanBuffer {
        let empty_array = new_empty_array(&DataType::Boolean);

        empty_array.as_boolean().clone().into_parts().0
    }
}

impl BlockBuilder for BooleanBufferBuilder {
    type Output = BooleanBuffer;

    fn with_capacity(capacity: usize) -> Self {
        BooleanBufferBuilder::new(capacity)
    }

    fn len(&self) -> usize {
        BooleanBufferBuilder::len(self)
    }

    fn truncate(&mut self, len: usize) {
        BooleanBufferBuilder::truncate(self, len)
    }

    fn append_range(&mut self, src: &Self, range: Range<usize>) {
        self.append_packed_range(range, src.as_slice())
    }

    fn shift_down(&mut self, offset: usize, len: usize) {
        if offset == 0 {
            BooleanBufferBuilder::truncate(self, len);
            return;
        }

        let byte_offset = offset / 8;
        let bit_offset = offset % 8;
        let dst_bytes = len.div_ceil(8);

        {
            let bytes = self.as_slice_mut();
            let src_bytes = bytes.len();

            if bit_offset == 0 {
                bytes.copy_within(byte_offset..byte_offset + dst_bytes, 0);
            } else {
                // Destination byte is always at or below the source byte
                // so a forward pass never reads a byte that was already overwritten
                for dst in 0..dst_bytes {
                    let src = dst + byte_offset;

                    let low = bytes[src] >> bit_offset;
                    let high = if src + 1 < src_bytes {
                        bytes[src + 1] << (8 - bit_offset)
                    } else {
                        0
                    };

                    bytes[dst] = low | high;
                }
            }

            // Clear the stale bits in the last byte so later appends see zeroed padding
            let trailing = len % 8;
            if trailing != 0 {
                bytes[dst_bytes - 1] &= (1u8 << trailing) - 1;
            }
        }

        BooleanBufferBuilder::truncate(self, len);
    }

    fn allocated_size(&self) -> usize {
        allocated_size_for_builder(self)
    }

    fn finish(self) -> BooleanBuffer {
        self.build()
    }
}

fn allocated_size_for_builder(builder: &BooleanBufferBuilder) -> usize {
    // capacity returns in bits
    // once we upgrade arrow to have the allocated_size function, we can remove this function
    builder.capacity() / 8
}

// Only when we control the blocking since otherwise each block is not the same size
impl Index<usize> for BlockedBooleanBuilder<true> {
    type Output = bool;

    fn index(&self, index: usize) -> &Self::Output {
        self.index(BlocksIndex::from_index_in_fixed_block_size(
            index,
            self.block_size,
        ))
    }
}

impl<const FIXED_BLOCK_SIZING: bool> Index<BlocksIndex>
    for BlockedBooleanBuilder<FIXED_BLOCK_SIZING>
{
    type Output = bool;

    fn index(&self, blocked_index: BlocksIndex) -> &Self::Output {
        if self.blocks[blocked_index.block_index()]
            .get_bit(blocked_index.index_in_block())
        {
            &true
        } else {
            &false
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    type Fixed = BlockedBooleanBuilder<true>;
    type Manual = BlockedBooleanBuilder<false>;

    /// Deterministic bit pattern that is not periodic in 8 so byte boundaries get exercised
    fn pattern(seed: usize, len: usize) -> Vec<bool> {
        let mut state =
            (seed as u64).wrapping_mul(6364136223846793005) ^ 0x14057B7EF767814F;
        (0..len)
            .map(|_| {
                state = state.wrapping_mul(6364136223846793005).wrapping_add(1);
                (state >> 33) & 1 == 1
            })
            .collect()
    }

    fn bits(builder: &Fixed) -> Vec<bool> {
        (0..builder.len()).map(|i| builder[i]).collect()
    }

    fn buffer_bits(buffer: &BooleanBuffer) -> Vec<bool> {
        buffer.iter().collect()
    }

    /// Every block but the last is exactly `block_size` long and the last always has room
    fn check_fixed_layout(builder: &Fixed) {
        let block_size = builder.block_size();
        let blocks = &builder.blocks;
        assert_eq!(
            blocks.len(),
            builder.len() / block_size + 1,
            "unexpected number of blocks for len {} and block size {block_size}",
            builder.len()
        );
        assert_eq!(builder.current_block_index, blocks.len() - 1);
        for block in blocks.iter().take(blocks.len() - 1) {
            assert_eq!(block.len(), block_size);
        }
        assert_eq!(blocks.back().unwrap().len(), builder.len() % block_size);
        assert_eq!(
            builder.finished_blocks_allocated_size,
            blocks
                .iter()
                .take(blocks.len() - 1)
                .map(allocated_size_for_builder)
                .sum::<usize>()
        );
    }

    fn fixed_with(block_size: usize, values: &[bool]) -> Fixed {
        let mut builder = Fixed::new(block_size);
        for &v in values {
            builder.append(v);
        }
        check_fixed_layout(&builder);
        builder
    }

    #[test]
    fn new_is_empty() {
        let builder = Fixed::new(4);
        assert_eq!(builder.len(), 0);
        assert_eq!(builder.block_size(), 4);
        check_fixed_layout(&builder);
    }

    #[test]
    #[should_panic(expected = "block size must be greater than 0")]
    fn fixed_zero_block_size_panics() {
        Fixed::new(0);
    }

    #[test]
    fn append_spans_blocks() {
        let values = pattern(1, 11);
        let builder = fixed_with(4, &values);
        assert_eq!(builder.len(), 11);
        assert_eq!(bits(&builder), values);

        // exactly to the boundary
        let builder = fixed_with(4, &pattern(2, 8));
        assert_eq!(builder.blocks.len(), 3);
        assert_eq!(builder.blocks.back().unwrap().len(), 0);
    }

    #[test]
    fn append_n_spans_blocks() {
        let mut builder = Fixed::new(5);
        builder.append_n(0, true);
        assert_eq!(builder.len(), 0);
        check_fixed_layout(&builder);

        builder.append_n(3, true);
        builder.append_n(9, false);
        builder.append_n(3, true);
        assert_eq!(builder.len(), 15);
        check_fixed_layout(&builder);

        let mut expected = vec![true; 3];
        expected.extend(vec![false; 9]);
        expected.extend(vec![true; 3]);
        assert_eq!(bits(&builder), expected);

        builder.append_n(6, true);
        expected.extend(vec![true; 6]);
        assert_eq!(bits(&builder), expected);
        check_fixed_layout(&builder);
    }

    #[test]
    fn extend_validity_in_block() {
        let mut builder = Fixed::new(4);
        assert_eq!(
            builder.extend_validity_in_block([true, false].into_iter()),
            2
        );
        assert_eq!(builder.len(), 2);
        check_fixed_layout(&builder);

        // filling the block starts a new one
        assert_eq!(
            builder.extend_validity_in_block([true, true].into_iter()),
            2
        );
        assert_eq!(builder.blocks.len(), 2);
        check_fixed_layout(&builder);
        assert_eq!(bits(&builder), [true, false, true, true]);
    }

    #[test]
    #[should_panic(expected = "overflow from block")]
    fn extend_validity_in_block_overflow_panics() {
        let mut builder = Fixed::new(2);
        builder.extend_validity_in_block([true, true, true].into_iter());
    }

    #[test]
    fn get_set_and_index() {
        let values = pattern(3, 10);
        let mut builder = fixed_with(3, &values);

        for (i, &expected) in values.iter().enumerate() {
            let index = BlocksIndex::from_index_in_fixed_block_size(i, 3);
            assert_eq!(builder.get_bit(index), expected);
            assert_eq!(builder[index], expected);
            assert_eq!(builder[i], expected);
        }

        builder.set_bit(BlocksIndex::new(1, 2), !values[5]);
        builder.set_bit(BlocksIndex::new(3, 0), !values[9]);
        assert_eq!(builder[5], !values[5]);
        assert_eq!(builder[9], !values[9]);
        assert_eq!(builder.len(), 10);
    }

    #[test]
    fn take_block_returns_none_when_empty() {
        let mut builder = Fixed::new(4);
        assert!(builder.take_block().is_none());
        check_fixed_layout(&builder);

        builder.append(true);
        assert_eq!(bits(&builder), [true]);
    }

    #[test]
    fn take_block_shifts_remaining() {
        let values = pattern(4, 8);
        let mut builder = fixed_with(3, &values);

        let block = builder.take_block().unwrap();
        assert_eq!(buffer_bits(&block), &values[0..3]);
        assert_eq!(builder.len(), 5);
        assert_eq!(bits(&builder), &values[3..]);
        assert_eq!(builder.get_bit(BlocksIndex::new(1, 1)), values[7]);
        check_fixed_layout(&builder);

        let block = builder.take_block().unwrap();
        assert_eq!(buffer_bits(&block), &values[3..6]);
        check_fixed_layout(&builder);

        // partial last block
        let block = builder.take_block().unwrap();
        assert_eq!(buffer_bits(&block), &values[6..8]);
        assert_eq!(builder.len(), 0);
        check_fixed_layout(&builder);

        assert!(builder.take_block().is_none());
    }

    #[test]
    fn take_block_when_len_is_multiple_of_block_size() {
        let values = pattern(5, 6);
        let mut builder = fixed_with(3, &values);

        assert_eq!(buffer_bits(&builder.take_block().unwrap()), &values[0..3]);
        check_fixed_layout(&builder);
        assert_eq!(buffer_bits(&builder.take_block().unwrap()), &values[3..6]);
        check_fixed_layout(&builder);
        assert!(builder.take_block().is_none());
        check_fixed_layout(&builder);
    }

    #[test]
    fn append_after_take_block_continues_layout() {
        let values = pattern(6, 20);
        let mut builder = fixed_with(3, &values[..7]);
        builder.take_block();

        for &v in &values[7..] {
            builder.append(v);
        }
        assert_eq!(bits(&builder), &values[3..]);
        check_fixed_layout(&builder);

        let mut drained = vec![];
        while let Some(block) = builder.take_block() {
            assert!(block.len() <= 3);
            drained.extend(buffer_bits(&block));
        }
        assert_eq!(drained, &values[3..]);
        check_fixed_layout(&builder);
    }

    #[test]
    fn take_all_returns_only_non_empty_blocks() {
        let values = pattern(7, 7);
        let mut builder = fixed_with(3, &values);
        let blocks: Vec<Vec<bool>> = builder.take_all().iter().map(buffer_bits).collect();
        assert_eq!(
            blocks,
            vec![
                values[0..3].to_vec(),
                values[3..6].to_vec(),
                values[6..7].to_vec()
            ]
        );
        assert_eq!(builder.len(), 0);
        check_fixed_layout(&builder);

        // exact multiple, the trailing empty block is not returned
        let mut builder = fixed_with(3, &values[..6]);
        let blocks: Vec<Vec<bool>> = builder.take_all().iter().map(buffer_bits).collect();
        assert_eq!(blocks, vec![values[0..3].to_vec(), values[3..6].to_vec()]);
        check_fixed_layout(&builder);

        let mut builder = Fixed::new(3);
        assert!(builder.take_all().is_empty());
        check_fixed_layout(&builder);

        // usable after
        builder.append_n(4, true);
        assert_eq!(bits(&builder), vec![true; 4]);
        check_fixed_layout(&builder);
    }

    #[test]
    fn take_n_zero_is_noop() {
        let values = pattern(8, 7);
        let mut builder = fixed_with(3, &values);
        let taken = builder.take_n(0, None::<std::iter::Empty<usize>>);
        assert_eq!(taken.len(), 0);
        assert_eq!(bits(&builder), values);
        check_fixed_layout(&builder);

        let mut builder = Fixed::new(3);
        let taken = builder.take_n(0, None::<std::iter::Empty<usize>>);
        assert_eq!(taken.len(), 0);
        check_fixed_layout(&builder);
    }

    #[test]
    fn take_n_everything_when_less_than_block() {
        let values = pattern(9, 3);
        let mut builder = fixed_with(5, &values);
        let taken = builder.take_n(3, None::<std::iter::Empty<usize>>);
        assert_eq!(buffer_bits(&taken), values);
        assert_eq!(builder.len(), 0);
        check_fixed_layout(&builder);
        assert!(builder.take_block().is_none());
    }

    #[test]
    fn take_n_full_block_equals_take_block() {
        let values = pattern(10, 10);
        let mut builder = fixed_with(4, &values);
        let taken = builder.take_n(4, None::<std::iter::Empty<usize>>);
        assert_eq!(buffer_bits(&taken), &values[..4]);
        assert_eq!(bits(&builder), &values[4..]);
        check_fixed_layout(&builder);
    }

    #[test]
    fn take_n_relayouts_across_byte_boundaries() {
        // block size and n are not multiples of 8 so the bit shifting path is used
        let values = pattern(11, 40);
        let mut builder = fixed_with(13, &values);
        let taken = builder.take_n(5, None::<std::iter::Empty<usize>>);
        assert_eq!(buffer_bits(&taken), &values[..5]);
        assert_eq!(builder.len(), 35);
        assert_eq!(bits(&builder), &values[5..]);
        check_fixed_layout(&builder);

        // stale padding bits must have been cleared so appends land correctly
        for &v in &values[..10] {
            builder.append(v);
        }
        let mut expected = values[5..].to_vec();
        expected.extend_from_slice(&values[..10]);
        assert_eq!(bits(&builder), expected);
        check_fixed_layout(&builder);
    }

    #[test]
    #[should_panic(expected = "must be <= len")]
    fn take_n_more_than_len_panics() {
        let mut builder = fixed_with(4, &[true, false]);
        builder.take_n(3, None::<std::iter::Empty<usize>>);
    }

    #[test]
    #[should_panic(expected = "must be lower than block size")]
    fn take_n_more_than_block_size_panics() {
        let mut builder = fixed_with(2, &pattern(0, 6));
        builder.take_n(3, None::<std::iter::Empty<usize>>);
    }

    #[test]
    fn take_n_matches_model_and_stays_usable() {
        for block_size in [1, 2, 3, 7, 8, 9, 13, 16, 17, 31, 64, 65] {
            for total in
                (0..=(3 * block_size + 1)).step_by(if block_size > 16 { 5 } else { 1 })
            {
                for n in 0..=total.min(block_size) {
                    let values = pattern(block_size * 1000 + total, total);
                    let mut builder = fixed_with(block_size, &values);
                    let mut model = values.clone();

                    let taken = builder.take_n(n, None::<std::iter::Empty<usize>>);
                    let expected_taken: Vec<bool> = model.drain(..n).collect();
                    assert_eq!(
                        buffer_bits(&taken),
                        expected_taken,
                        "taken mismatch bs={block_size} total={total} n={n}"
                    );
                    assert_eq!(
                        bits(&builder),
                        model,
                        "remaining mismatch bs={block_size} total={total} n={n}"
                    );
                    check_fixed_layout(&builder);

                    // keep using it after the re-layout
                    let more = pattern(n + 1, 2 * block_size + 1);
                    for &v in &more {
                        builder.append(v);
                    }
                    model.extend_from_slice(&more);
                    assert_eq!(
                        bits(&builder),
                        model,
                        "after append mismatch bs={block_size} total={total} n={n}"
                    );
                    check_fixed_layout(&builder);

                    // and drain block by block
                    let mut drained = vec![];
                    while let Some(block) = builder.take_block() {
                        assert!(block.len() <= block_size);
                        drained.extend(buffer_bits(&block));
                    }
                    assert_eq!(
                        drained, model,
                        "drain mismatch bs={block_size} total={total} n={n}"
                    );
                    assert_eq!(builder.len(), 0);
                    check_fixed_layout(&builder);
                }
            }
        }
    }

    #[test]
    fn repeated_take_n_set_bit_and_append() {
        let block_size = 11;
        let mut builder = Fixed::new(block_size);
        let mut model: Vec<bool> = vec![];

        for step in 0..80 {
            let pushed = pattern(step, (step * 7) % 23);
            builder.append_n(pushed.len(), false);
            let offset = model.len();
            model.extend(std::iter::repeat_n(false, pushed.len()));
            for (i, &v) in pushed.iter().enumerate() {
                if v {
                    builder.set_bit(
                        BlocksIndex::from_index_in_fixed_block_size(
                            offset + i,
                            block_size,
                        ),
                        true,
                    );
                    model[offset + i] = true;
                }
            }

            let n = ((step * 3) % block_size).min(model.len());
            let taken = builder.take_n(n, None::<std::iter::Empty<usize>>);
            let expected: Vec<bool> = model.drain(..n).collect();
            assert_eq!(buffer_bits(&taken), expected, "step {step}");
            assert_eq!(bits(&builder), model, "step {step}");
            check_fixed_layout(&builder);

            if step % 4 == 0 {
                let block = builder.take_block();
                let expected_len = model.len().min(block_size);
                let expected: Vec<bool> = model.drain(..expected_len).collect();
                assert_eq!(
                    block.as_ref().map(buffer_bits).unwrap_or_default(),
                    expected,
                    "step {step}"
                );
                check_fixed_layout(&builder);
            }
        }
    }

    #[test]
    fn allocated_size_follows_blocks() {
        let mut builder = Fixed::new(64);
        let empty = builder.allocated_size();

        builder.append_n(64 * 3 + 1, true);
        let full = builder.allocated_size();
        assert!(full > empty);

        builder.take_block();
        let after_take = builder.allocated_size();
        assert!(after_take < full);

        builder.take_all();
        assert!(builder.allocated_size() < after_take);
    }

    // ---- manual block sizing ----

    fn manual_with_blocks(blocks: &[Vec<bool>]) -> Manual {
        let mut builder = Manual::new(0);
        for (i, block) in blocks.iter().enumerate() {
            if i > 0 {
                builder.start_new_block();
            }
            for &v in block {
                builder.append(v);
            }
        }
        builder
    }

    fn drain_manual(builder: &mut Manual) -> Vec<Vec<bool>> {
        let mut out = vec![];
        while let Some(block) = builder.take_block() {
            out.push(buffer_bits(&block));
        }
        out
    }

    #[test]
    fn manual_append_never_finishes_block() {
        let mut builder = Manual::new(0);
        let values = pattern(12, 20);
        for &v in &values {
            builder.append(v);
        }
        builder.append_n(3, true);
        builder.extend_validity_in_block([false, true].into_iter());
        assert_eq!(builder.blocks.len(), 1);
        assert_eq!(builder.len(), 25);
        let mut expected = values.clone();
        expected.extend([true, true, true, false, true]);
        let actual: Vec<bool> = (0..25)
            .map(|i| builder.get_bit(BlocksIndex::new(0, i)))
            .collect();
        assert_eq!(actual, expected);
    }

    #[test]
    fn manual_start_new_block_and_take_block() {
        let a = pattern(13, 5);
        let b = pattern(14, 2);
        let c = pattern(15, 12);
        let mut builder = manual_with_blocks(&[a.clone(), b.clone(), c.clone()]);
        assert_eq!(builder.blocks.len(), 3);
        assert_eq!(builder.len(), 19);
        assert_eq!(builder.get_bit(BlocksIndex::new(1, 1)), b[1]);
        assert_eq!(builder.get_bit(BlocksIndex::new(2, 11)), c[11]);

        assert_eq!(buffer_bits(&builder.take_block().unwrap()), a);
        assert_eq!(builder.len(), 14);
        assert_eq!(builder.current_block_index, 1);
        assert_eq!(builder.get_bit(BlocksIndex::new(0, 1)), b[1]);

        // appends go to the last block
        builder.append(true);
        assert!(builder.get_bit(BlocksIndex::new(1, 12)));

        let mut c_plus = c.clone();
        c_plus.push(true);
        assert_eq!(drain_manual(&mut builder), vec![b, c_plus]);
        assert_eq!(builder.len(), 0);
        assert!(builder.take_block().is_none());
        assert_eq!(builder.blocks.len(), 1);
    }

    #[test]
    fn manual_take_all() {
        let a = pattern(16, 5);
        let b = pattern(17, 9);
        let mut builder = manual_with_blocks(&[a.clone(), b.clone()]);
        let blocks: Vec<Vec<bool>> = builder.take_all().iter().map(buffer_bits).collect();
        assert_eq!(blocks, vec![a.clone(), b]);
        assert_eq!(builder.len(), 0);

        // trailing empty block from start_new_block is dropped
        let mut builder = manual_with_blocks(std::slice::from_ref(&a));
        builder.start_new_block();
        let blocks: Vec<Vec<bool>> = builder.take_all().iter().map(buffer_bits).collect();
        assert_eq!(blocks, vec![a]);
    }

    #[test]
    fn manual_take_n_relayouts() {
        let values = pattern(18, 30);
        let blocks = || {
            vec![
                values[0..13].to_vec(),
                values[13..20].to_vec(),
                values[20..30].to_vec(),
            ]
        };

        // shrink first block only
        let mut builder = manual_with_blocks(&blocks());
        let taken = builder.take_n(5, Some([8usize, 7, 10].into_iter()));
        assert_eq!(buffer_bits(&taken), &values[..5]);
        assert_eq!(builder.len(), 25);
        assert_eq!(
            drain_manual(&mut builder),
            vec![
                values[5..13].to_vec(),
                values[13..20].to_vec(),
                values[20..30].to_vec()
            ]
        );

        // merge everything, crossing byte boundaries
        let mut builder = manual_with_blocks(&blocks());
        let taken = builder.take_n(3, Some([27usize].into_iter()));
        assert_eq!(buffer_bits(&taken), &values[..3]);
        assert_eq!(drain_manual(&mut builder), vec![values[3..30].to_vec()]);

        // split into odd sized pieces
        let mut builder = manual_with_blocks(&blocks());
        let taken = builder.take_n(1, Some([3usize, 9, 1, 16].into_iter()));
        assert_eq!(buffer_bits(&taken), &values[..1]);
        assert_eq!(
            drain_manual(&mut builder),
            vec![
                values[1..4].to_vec(),
                values[4..13].to_vec(),
                values[13..14].to_vec(),
                values[14..30].to_vec()
            ]
        );

        // whole first block
        let mut builder = manual_with_blocks(&blocks());
        let taken = builder.take_n(13, Some([7usize, 10].into_iter()));
        assert_eq!(buffer_bits(&taken), &values[..13]);
        assert_eq!(
            drain_manual(&mut builder),
            vec![values[13..20].to_vec(), values[20..30].to_vec()]
        );

        // nothing
        let mut builder = manual_with_blocks(&blocks());
        let taken = builder.take_n(0, Some([13usize, 7, 10].into_iter()));
        assert_eq!(taken.len(), 0);
        assert_eq!(drain_manual(&mut builder), blocks());

        // everything
        let mut builder = manual_with_blocks(&blocks()[..1]);
        let taken = builder.take_n(13, Some(std::iter::empty::<usize>()));
        assert_eq!(buffer_bits(&taken), &values[..13]);
        assert_eq!(builder.len(), 0);
        assert!(builder.take_block().is_none());
        builder.append(true);
        assert!(builder.get_bit(BlocksIndex::new(0, 0)));
    }

    #[test]
    fn manual_take_n_matches_model() {
        // random layouts before and after, verified against a flat model
        for seed in 0..200usize {
            let sizes: Vec<usize> = pattern(seed, 4)
                .iter()
                .enumerate()
                .map(|(i, &b)| 1 + (seed * (i + 3) + b as usize * 5) % 19)
                .collect();
            let total: usize = sizes.iter().sum();
            let values = pattern(seed + 7, total);

            let mut offset = 0;
            let blocks: Vec<Vec<bool>> = sizes
                .iter()
                .map(|&s| {
                    let block = values[offset..offset + s].to_vec();
                    offset += s;
                    block
                })
                .collect();

            // n == 0 keeps the layout as is and is covered elsewhere
            let n = 1 + seed % sizes[0];
            let remaining = total - n;
            // new layout: chunks derived from the seed, except when the whole first block
            // is taken which must keep the rest of the layout as is
            let mut adjusted = vec![];
            if n == sizes[0] {
                adjusted.extend_from_slice(&sizes[1..]);
            } else {
                let mut left = remaining;
                let mut i = 0;
                while left > 0 {
                    let chunk = (1 + (seed + i * 5) % 11).min(left);
                    adjusted.push(chunk);
                    left -= chunk;
                    i += 1;
                }
            }

            let mut builder = manual_with_blocks(&blocks);
            let taken = builder.take_n(n, Some(adjusted.clone().into_iter()));
            assert_eq!(buffer_bits(&taken), &values[..n], "seed {seed}");
            assert_eq!(builder.len(), remaining, "seed {seed}");

            let drained = drain_manual(&mut builder);
            let drained_sizes: Vec<usize> = drained.iter().map(Vec::len).collect();
            assert_eq!(drained_sizes, adjusted, "seed {seed}");
            assert_eq!(drained.concat(), &values[n..], "seed {seed}");
        }
    }

    #[test]
    #[should_panic(expected = "must equal the length")]
    fn manual_take_n_wrong_adjusted_sizes_panics() {
        let mut builder = manual_with_blocks(&[pattern(0, 5), pattern(1, 3)]);
        builder.take_n(2, Some([3usize, 2].into_iter()));
    }
}
