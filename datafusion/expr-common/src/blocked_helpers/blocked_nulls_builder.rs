use crate::blocked_helpers::take_n_helpers::{
    BlockBuilder, create_adjusted_block_size_iter_for_fixed_blocks, take_n_from_blocks,
};
use crate::groups_accumulator::{BlockedGroupSelection, BlocksIndex, GroupSelection};
use arrow::array::NullBufferBuilder;
use arrow::buffer::NullBuffer;
use arrow::util::bit_util::apply_bitwise_binary_op;
use datafusion_common::utils::proxy::VecDequeAllocExt;
use std::collections::VecDeque;
use std::ops::{Index, Range};

#[derive(Debug)]
pub struct BlockedNullsBuilder<const FIXED_BLOCK_SIZING: bool> {
    /// Using `VecDeque` so we can remove the first block and reclaim memory
    blocks: VecDeque<NullBufferBuilder>,

    /// The size of each block
    block_size: usize,

    /// The index of the current block
    current_block_index: usize,

    len: usize,

    finished_blocks_allocated_size: usize,

    might_have_nulls: bool,
}

impl<const FIXED_BLOCK_SIZING: bool> BlockedNullsBuilder<FIXED_BLOCK_SIZING> {
    pub fn new(block_size: usize) -> Self {
        if FIXED_BLOCK_SIZING {
            assert_ne!(block_size, 0, "block size must be greater than 0");
        }

        let blocks = VecDeque::from(vec![NullBufferBuilder::new(block_size)]);

        BlockedNullsBuilder {
            blocks,
            block_size,
            current_block_index: 0,
            len: 0,
            finished_blocks_allocated_size: 0,
            might_have_nulls: false,
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
            + self.blocks.back().map_or(0, |b| b.allocated_size())
    }

    pub fn block_size(&self) -> usize {
        assert!(
            FIXED_BLOCK_SIZING,
            "block size is only available for manual block"
        );
        self.block_size
    }

    pub fn might_have_nulls(&self) -> bool {
        self.might_have_nulls
    }

    pub fn start_new_block(&mut self) {
        self.current_block_index += 1;
        self.finished_blocks_allocated_size +=
            self.blocks.back().map_or(0, |b| b.allocated_size());
        let new_block = NullBufferBuilder::new(self.block_size);
        self.blocks.push_back(new_block);
    }

    pub(crate) fn reserve_blocks(&mut self, n: usize) {
        self.blocks.reserve(n);
    }

    /// Extend the null buffer
    /// when it is guaranteed that len is less than the remaining block size
    pub(super) fn extends_from_null_buffer_in_current_block(
        &mut self,
        null_buffer: &NullBuffer,
    ) {
        assert_ne!(null_buffer.len(), 0);
        if FIXED_BLOCK_SIZING {
            assert!(
                self.current_block_remaining_len() >= null_buffer.len(),
                "the amount to add exceed the current block size"
            );
        }
        if null_buffer.null_count() == 0 {
            self.push_n_within_block(null_buffer.len(), true);
            return;
        }

        self.might_have_nulls = true;

        let block = &mut self.blocks[self.current_block_index];
        let prev_block_size = block.len();

        // Do fast large copy
        block.append_buffer(null_buffer);

        let added_items = block.len() - prev_block_size;

        self.len += added_items;

        if FIXED_BLOCK_SIZING && block.len() == self.block_size {
            self.start_new_block();
        }
    }

    /// Extend the length from the current offsets
    pub fn extends_from_null_buffer(&mut self, null_buffer: &NullBuffer) {
        if null_buffer.null_count() == 0 {
            self.push_n(null_buffer.len(), true);
            return;
        }

        if !FIXED_BLOCK_SIZING {
            self.extends_from_null_buffer_in_current_block(null_buffer);
            return;
        }

        let number_of_blocks_to_reserve = null_buffer
            .len()
            .saturating_sub(self.current_block_remaining_len())
            .div_ceil(self.block_size);
        self.reserve_blocks(number_of_blocks_to_reserve);

        let mut len = null_buffer.len();
        let mut index = 0;

        while len > 0 {
            let remaining_in_current_block = self.current_block_remaining_len();

            let to_add = remaining_in_current_block.min(len);
            if to_add == len {
                // Avoid slice which does null counting
                if index == 0 {
                    self.extends_from_null_buffer_in_current_block(null_buffer);
                } else {
                    self.extends_from_null_buffer_in_current_block(
                        &null_buffer.slice(index, len),
                    );
                }

                break;
            }

            let null_section = null_buffer.slice(index, to_add);
            index += to_add;
            len -= to_add;

            self.extends_from_null_buffer_in_current_block(&null_section);
        }
    }

    /// Extend the length from the current offsets in the indexes
    /// when it is guaranteed that len is less than the remaining block size
    fn extends_from_null_buffer_in_indexes_in_current_block(
        &mut self,
        null_buffer: &NullBuffer,
        indexes: &[usize],
    ) -> usize {
        assert_ne!(null_buffer.len(), 0);
        assert_ne!(indexes.len(), 0);

        if FIXED_BLOCK_SIZING {
            assert!(
                self.current_block_remaining_len() >= indexes.len(),
                "the amount to add exceed the current block size"
            );
        }

        let block = &mut self.blocks[self.current_block_index];
        let prev_block_size = block.len();

        // TODO - reserve in block and set each byte without extra checks
        for &index_to_copy in indexes {
            block.append(null_buffer.is_valid(index_to_copy));
        }

        let added_items = block.len() - prev_block_size;

        self.len += added_items;

        self.might_have_nulls |= block.as_slice().is_some();

        if FIXED_BLOCK_SIZING && block.len() == self.block_size {
            self.start_new_block();
        }

        added_items
    }

    /// Extend the length from the current offsets
    pub fn extends_from_null_buffer_in_indexes(
        &mut self,
        null_buffer: &NullBuffer,
        mut indexes: &[usize],
    ) {
        if !FIXED_BLOCK_SIZING {
            self.extends_from_null_buffer_in_indexes_in_current_block(
                null_buffer,
                indexes,
            );
            return;
        }

        let number_of_blocks_to_reserve = indexes
            .len()
            .saturating_sub(self.current_block_remaining_len())
            .div_ceil(self.block_size);
        self.reserve_blocks(number_of_blocks_to_reserve);

        while !indexes.is_empty() {
            let remaining_in_current_block = self.current_block_remaining_len();

            let to_add = remaining_in_current_block.min(indexes.len());
            let (to_copy, left) = indexes.split_at(to_add);
            indexes = left;

            self.extends_from_null_buffer_in_indexes_in_current_block(
                null_buffer,
                to_copy,
            );
        }
    }

    fn current_block_remaining_len(&self) -> usize {
        assert!(
            FIXED_BLOCK_SIZING,
            "remaining block only available for manual block"
        );
        self.block_size - self.blocks[self.current_block_index].len()
    }

    fn push_n_within_block(&mut self, n: usize, is_valid: bool) {
        self.len += n;
        let block = &mut self.blocks[self.current_block_index];

        if is_valid {
            block.append_n_non_nulls(n)
        } else {
            block.append_n_nulls(n);
            self.might_have_nulls = true;
        }

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

    pub fn push_n(&mut self, mut n: usize, is_valid: bool) {
        if !FIXED_BLOCK_SIZING {
            self.push_n_within_block(n, is_valid);
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

            self.push_n_within_block(to_add, is_valid);
        }
    }

    pub fn push_n_nulls(&mut self, n: usize) {
        self.push_n(n, false);
    }

    pub fn push_n_non_nulls(&mut self, n: usize) {
        self.push_n(n, true);
    }

    pub fn push_non_null(&mut self) {
        let block = &mut self.blocks[self.current_block_index];

        block.append_non_null();
        self.len += 1;

        self.might_have_nulls |= block.as_slice().is_some();

        if FIXED_BLOCK_SIZING && block.len() == self.block_size {
            self.start_new_block();
        }
    }

    pub fn push_null(&mut self) {
        let block = &mut self.blocks[self.current_block_index];

        block.append_null();
        self.len += 1;

        self.might_have_nulls = true;

        if FIXED_BLOCK_SIZING && block.len() == self.block_size {
            self.start_new_block();
        }
    }

    pub fn is_null(&self, blocked_index: BlocksIndex) -> bool {
        !self.blocks[blocked_index.block_index()].is_valid(blocked_index.index_in_block())
    }

    /// Extends iterator of validity within current block
    /// Returns how many items were added
    ///
    /// # Panics
    /// Panics if the iterator length exceeds the remaining size of the current block
    pub(super) fn extend_validity_in_block(
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

        self.might_have_nulls |= block.as_slice().is_some();
        if FIXED_BLOCK_SIZING && block.len() == self.block_size {
            self.start_new_block();
        }

        added_items
    }


    pub fn build_preserving(
        &self,
        selection: BlockedGroupSelection<'_>,
    ) -> datafusion_common::Result<Option<NullBuffer>> {
        selection.validate_num_groups(self.len())?;
        if !self.might_have_nulls {
            return Ok(None);
        }

        let mut selected = NullBufferBuilder::new(selection.len());
        for index in selection.iter() {
            selected.append(!self.is_null(index));
        }
        Ok(selected.finish())
    }

    /// Take the first block, `None` once there are no more items
    ///
    /// `Some(None)` means the block has no nulls
    pub fn take_block(&mut self) -> Option<Option<NullBuffer>> {
        if self.len == 0 {
            return None;
        }

        let block = self
            .blocks
            .pop_front()
            .expect("len > 0 so must have a block");
        self.len -= block.len();

        // Never have empty blocks since we won't be able to add more items
        if self.blocks.is_empty() {
            self.might_have_nulls = false;
            self.current_block_index = 0;
            let empty_block = NullBufferBuilder::new(self.block_size);
            self.blocks.push_back(empty_block);
        } else {
            self.current_block_index -= 1;

            // Only reduce memory if not the last one since the last block is calculated separately
            self.finished_blocks_allocated_size -= block.allocated_size();
        }

        Some(block.build().filter(|b| b.null_count() > 0))
    }

    /// Take every non empty block, `None` entries have no nulls
    pub fn take_all(&mut self) -> Vec<Option<NullBuffer>> {
        let blocks = std::mem::take(&mut self.blocks);
        self.len = 0;
        self.might_have_nulls = false;
        self.finished_blocks_allocated_size = 0;
        self.current_block_index = 0;

        // Never have empty blocks since we won't be able to add more items
        let empty_block = NullBufferBuilder::new(self.block_size);
        self.blocks.push_back(empty_block);

        blocks
            .into_iter()
            .filter(|b| !b.is_empty())
            .map(|item| item.build().filter(|b| b.null_count() > 0))
            .collect()
    }

    pub fn take_n(
        &mut self,
        n: usize,
        adjusted_block_size_iter: Option<impl Iterator<Item = usize> + Clone>,
    ) -> Option<NullBuffer> {
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
        self.finished_blocks_allocated_size = layout.finished_blocks_allocated_size;

        // Copying bits materializes the bitmap even when every bit is valid
        taken.filter(|b| b.null_count() > 0)
    }
}

impl BlockBuilder for NullBufferBuilder {
    type Output = Option<NullBuffer>;

    fn with_capacity(capacity: usize) -> Self {
        NullBufferBuilder::new(capacity)
    }

    fn len(&self) -> usize {
        self.len()
    }

    fn truncate(&mut self, len: usize) {
        self.truncate(len)
    }

    fn append_range(&mut self, src: &Self, range: Range<usize>) {
        let Some(src_slice) = src.as_slice() else {
            self.append_n_non_nulls(range.len());
            return;
        };

        let offset_write = self.len();
        let len = range.end - range.start;
        // allocate new bits as 0
        self.append_n_nulls(len);
        // copy bits from to_set into self.buffer a word at a time
        apply_bitwise_binary_op(
            self.as_slice_mut().expect("must be materialized"),
            offset_write,
            src_slice,
            range.start,
            len,
            |_a, b| b, // copy bits from to_set
        );
    }

    fn shift_down(&mut self, offset: usize, len: usize) {
        if offset == 0 {
            self.truncate(len);
            return;
        }

        let byte_offset = offset / 8;
        let bit_offset = offset % 8;
        let dst_bytes = len.div_ceil(8);

        if let Some(bytes) = self.as_slice_mut() {
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

        // truncate is enough for both materialized (since we just shifted) and non-materialized (since all are the same value) case
        self.truncate(len)
    }

    fn allocated_size(&self) -> usize {
        self.allocated_size()
    }

    fn finish(self) -> Self::Output {
        self.build()
    }
}

impl<const MANUAL_BLOCK_SIZE: bool> Extend<bool>
    for BlockedNullsBuilder<MANUAL_BLOCK_SIZE>
{
    fn extend<T: IntoIterator<Item = bool>>(&mut self, iter: T) {
        let mut iter = iter.into_iter();

        if !MANUAL_BLOCK_SIZE {
            self.extend_validity_in_block(iter);
            return;
        }

        loop {
            let remaining_in_current_block = self.current_block_remaining_len();
            let added_items = self
                .extend_validity_in_block(iter.by_ref().take(remaining_in_current_block));

            if added_items == 0 {
                break;
            }
        }
    }
}

// Only when we control the blocking since otherwise each block is not the same size
impl Index<usize> for BlockedNullsBuilder<true> {
    type Output = bool;

    fn index(&self, index: usize) -> &Self::Output {
        self.index(BlocksIndex::from_index_in_fixed_block_size(
            index,
            self.block_size,
        ))
    }
}

impl<const FIXED_BLOCK_SIZING: bool> Index<BlocksIndex>
    for BlockedNullsBuilder<FIXED_BLOCK_SIZING>
{
    type Output = bool;

    fn index(&self, blocked_index: BlocksIndex) -> &Self::Output {
        if self.blocks[blocked_index.block_index()]
            .is_valid(blocked_index.index_in_block())
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

    type Fixed = BlockedNullsBuilder<true>;
    type Manual = BlockedNullsBuilder<false>;

    /// Validity pattern that is not periodic in 8 so byte boundaries get exercised
    fn pattern(seed: usize, len: usize) -> Vec<bool> {
        let mut state =
            (seed as u64).wrapping_mul(6364136223846793005) ^ 0x14057B7EF767814F;
        (0..len)
            .map(|_| {
                state = state.wrapping_mul(6364136223846793005).wrapping_add(1);
                !(state >> 33).is_multiple_of(3)
            })
            .collect()
    }

    fn validity(builder: &Fixed) -> Vec<bool> {
        (0..builder.len()).map(|i| builder[i]).collect()
    }

    /// `None` means the block has no nulls
    fn block_validity(block: Option<&NullBuffer>, len: usize) -> Vec<bool> {
        match block {
            None => vec![true; len],
            Some(nulls) => {
                assert_eq!(nulls.len(), len);
                assert!(nulls.null_count() > 0, "a block without nulls must be None");
                nulls.iter().collect()
            }
        }
    }

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
                .map(|b| b.allocated_size())
                .sum::<usize>()
        );
    }

    fn fixed_with(block_size: usize, values: &[bool]) -> Fixed {
        let mut builder = Fixed::new(block_size);
        for &valid in values {
            if valid {
                builder.push_non_null();
            } else {
                builder.push_null();
            }
        }
        check_fixed_layout(&builder);
        builder
    }

    /// Takes block by block and checks each against the model
    fn drain_and_check(builder: &mut Fixed, model: &[bool]) {
        let block_size = builder.block_size();
        for chunk in model.chunks(block_size) {
            let block = builder.take_block().expect("must have a block");
            assert_eq!(block_validity(block.as_ref(), chunk.len()), chunk);
        }
        assert!(builder.take_block().is_none());
        assert_eq!(builder.len(), 0);
        check_fixed_layout(builder);
    }

    #[test]
    fn new_is_empty() {
        let mut builder = Fixed::new(4);
        assert_eq!(builder.len(), 0);
        assert!(!builder.might_have_nulls());
        check_fixed_layout(&builder);
        assert!(builder.take_block().is_none());
        assert!(builder.take_all().is_empty());
    }

    #[test]
    #[should_panic(expected = "block size must be greater than 0")]
    fn fixed_zero_block_size_panics() {
        Fixed::new(0);
    }

    #[test]
    fn push_spans_blocks() {
        let values = pattern(1, 11);
        let builder = fixed_with(4, &values);
        assert_eq!(builder.len(), 11);
        assert_eq!(validity(&builder), values);
        assert!(builder.might_have_nulls());
        for (i, &valid) in values.iter().enumerate() {
            let index = BlocksIndex::from_index_in_fixed_block_size(i, 4);
            assert_eq!(builder.is_null(index), !valid);
            assert_eq!(builder[index], valid);
        }

        // exactly to the boundary
        let mut builder = fixed_with(4, &values[..8]);
        assert_eq!(builder.blocks.len(), 3);
        drain_and_check(&mut builder, &values[..8]);
    }

    #[test]
    fn only_non_nulls_never_materializes_a_bitmap() {
        let mut builder = Fixed::new(3);
        builder.push_non_null();
        builder.push_n_non_nulls(7);
        builder.extend([true, true]);
        builder.extends_from_null_buffer(&NullBuffer::from(vec![true; 5]));
        assert!(!builder.might_have_nulls());
        assert_eq!(builder.len(), 15);
        check_fixed_layout(&builder);

        let blocks = builder.take_all();
        assert_eq!(blocks, vec![None, None, None, None, None]);
    }

    #[test]
    fn push_n_spans_blocks() {
        let mut builder = Fixed::new(5);
        builder.push_n_nulls(0);
        builder.push_n_non_nulls(0);
        assert_eq!(builder.len(), 0);
        assert!(!builder.might_have_nulls());

        builder.push_n_non_nulls(3);
        assert!(!builder.might_have_nulls());
        builder.push_n_nulls(9);
        assert!(builder.might_have_nulls());
        builder.push_n_non_nulls(3);
        assert_eq!(builder.len(), 15);
        check_fixed_layout(&builder);

        let mut expected = vec![true; 3];
        expected.extend(vec![false; 9]);
        expected.extend(vec![true; 3]);
        assert_eq!(validity(&builder), expected);
        drain_and_check(&mut builder, &expected);
    }

    #[test]
    fn extends_from_null_buffer_spans_blocks() {
        let values = pattern(2, 23);
        let mut builder = Fixed::new(5);
        builder.push_null();
        builder.extends_from_null_buffer(&NullBuffer::from(values.clone()));
        let mut expected = vec![false];
        expected.extend_from_slice(&values);
        assert_eq!(validity(&builder), expected);
        check_fixed_layout(&builder);

        // exactly fills the last block
        let remaining = 5 - expected.len() % 5;
        builder.extends_from_null_buffer(&NullBuffer::from(vec![false; remaining]));
        expected.extend(vec![false; remaining]);
        assert_eq!(builder.blocks.back().unwrap().len(), 0);
        check_fixed_layout(&builder);

        // no nulls at all takes the fast path
        builder.extends_from_null_buffer(&NullBuffer::from(vec![true; 7]));
        expected.extend(vec![true; 7]);
        assert_eq!(validity(&builder), expected);
        check_fixed_layout(&builder);
        drain_and_check(&mut builder, &expected);
    }

    #[test]
    fn extends_from_null_buffer_in_indexes_spans_blocks() {
        let source = NullBuffer::from(vec![true, false, true, true, false]);
        let mut builder = Fixed::new(2);
        builder.extends_from_null_buffer_in_indexes(&source, &[4, 0, 1, 2, 3]);
        assert_eq!(validity(&builder), [false, true, false, true, true]);
        assert!(builder.might_have_nulls());
        check_fixed_layout(&builder);

        let mut builder = Fixed::new(2);
        builder.extends_from_null_buffer_in_indexes(&source, &[0, 2]);
        assert!(!builder.might_have_nulls());
        assert_eq!(builder.take_all(), vec![None]);
    }

    #[test]
    fn extend_spans_blocks() {
        let values = pattern(3, 10);
        let mut builder = Fixed::new(4);
        builder.extend(values.iter().copied());
        assert_eq!(validity(&builder), values);
        check_fixed_layout(&builder);

        builder.extend(std::iter::empty());
        assert_eq!(builder.len(), 10);
        check_fixed_layout(&builder);
    }

    #[test]
    fn take_block_reports_blocks_without_nulls_as_none() {
        let mut builder = Fixed::new(3);
        builder.push_n_non_nulls(3);
        builder.push_null();
        builder.push_n_non_nulls(2);
        builder.push_n_non_nulls(2);

        assert_eq!(builder.take_block(), Some(None));
        assert_eq!(builder.len(), 5);
        check_fixed_layout(&builder);
        assert_eq!(
            block_validity(builder.take_block().unwrap().as_ref(), 3),
            [false, true, true]
        );
        assert_eq!(builder.take_block(), Some(None));
        assert_eq!(builder.take_block(), None);
        assert!(!builder.might_have_nulls());
        check_fixed_layout(&builder);
    }

    #[test]
    fn push_after_take_block_continues_layout() {
        let values = pattern(4, 20);
        let mut builder = fixed_with(3, &values[..7]);
        builder.take_block();

        for &valid in &values[7..] {
            if valid {
                builder.push_non_null();
            } else {
                builder.push_null();
            }
        }
        assert_eq!(validity(&builder), &values[3..]);
        check_fixed_layout(&builder);
        drain_and_check(&mut builder, &values[3..]);
    }

    #[test]
    fn take_all_returns_only_non_empty_blocks() {
        let values = pattern(5, 6);
        let mut builder = fixed_with(3, &values);
        assert_eq!(builder.blocks.len(), 3);
        let blocks = builder.take_all();
        assert_eq!(blocks.len(), 2);
        assert_eq!(block_validity(blocks[0].as_ref(), 3), &values[..3]);
        assert_eq!(block_validity(blocks[1].as_ref(), 3), &values[3..]);
        assert_eq!(builder.len(), 0);
        assert!(!builder.might_have_nulls());
        check_fixed_layout(&builder);

        builder.push_null();
        assert_eq!(builder.take_all().len(), 1);
    }

    #[test]
    fn take_n_zero_and_everything() {
        let values = pattern(6, 3);
        let mut builder = fixed_with(5, &values);
        let taken = builder.take_n(0, None::<std::iter::Empty<usize>>);
        assert_eq!(block_validity(taken.as_ref(), 0), Vec::<bool>::new());
        assert_eq!(validity(&builder), values);
        check_fixed_layout(&builder);

        let taken = builder.take_n(3, None::<std::iter::Empty<usize>>);
        assert_eq!(block_validity(taken.as_ref(), 3), values);
        assert_eq!(builder.len(), 0);
        check_fixed_layout(&builder);

        let mut builder = Fixed::new(5);
        assert!(builder.take_n(0, None::<std::iter::Empty<usize>>).is_none());
        check_fixed_layout(&builder);
    }

    #[test]
    fn take_n_matches_model_and_stays_usable() {
        for block_size in [1, 2, 3, 7, 8, 9, 13, 16, 17] {
            for total in 0..=(3 * block_size + 1) {
                for n in 0..=total.min(block_size) {
                    let values = pattern(block_size * 1000 + total, total);
                    let mut builder = fixed_with(block_size, &values);
                    let mut model = values.clone();

                    let taken = builder.take_n(n, None::<std::iter::Empty<usize>>);
                    let expected_taken: Vec<bool> = model.drain(..n).collect();
                    assert_eq!(
                        block_validity(taken.as_ref(), n),
                        expected_taken,
                        "taken mismatch bs={block_size} total={total} n={n}"
                    );
                    assert_eq!(
                        validity(&builder),
                        model,
                        "remaining mismatch bs={block_size} total={total} n={n}"
                    );
                    check_fixed_layout(&builder);

                    let more = pattern(n + 1, 2 * block_size + 1);
                    builder.extends_from_null_buffer(&NullBuffer::from(more.clone()));
                    model.extend_from_slice(&more);
                    assert_eq!(
                        validity(&builder),
                        model,
                        "after extend mismatch bs={block_size} total={total} n={n}"
                    );
                    check_fixed_layout(&builder);

                    drain_and_check(&mut builder, &model);
                }
            }
        }
    }

    // ---- manual block sizing ----

    fn manual_with_blocks(blocks: &[Vec<bool>]) -> Manual {
        let mut builder = Manual::new(0);
        for (i, block) in blocks.iter().enumerate() {
            if i > 0 {
                builder.start_new_block();
            }
            builder.extend(block.iter().copied());
        }
        builder
    }

    fn drain_manual(builder: &mut Manual, sizes: &[usize]) -> Vec<Vec<bool>> {
        let mut out = vec![];
        for &size in sizes {
            let block = builder.take_block().expect("must have a block");
            out.push(block_validity(block.as_ref(), size));
        }
        assert!(builder.take_block().is_none());
        out
    }

    #[test]
    fn manual_push_never_starts_a_block() {
        let mut builder = Manual::new(0);
        let values = pattern(7, 20);
        builder.extend(values.iter().copied());
        builder.push_null();
        builder.push_non_null();
        builder.push_n_nulls(3);
        builder.extends_from_null_buffer(&NullBuffer::from(vec![true, false]));
        assert_eq!(builder.blocks.len(), 1);
        assert_eq!(builder.len(), 27);
        let mut expected = values;
        expected.extend([false, true, false, false, false, true, false]);
        let actual: Vec<bool> =
            (0..27).map(|i| builder[BlocksIndex::new(0, i)]).collect();
        assert_eq!(actual, expected);
    }

    #[test]
    fn manual_start_new_block_and_take_block() {
        let a = pattern(8, 5);
        let b = vec![true; 2];
        let c = pattern(9, 12);
        let mut builder = manual_with_blocks(&[a.clone(), b.clone(), c.clone()]);
        assert_eq!(builder.blocks.len(), 3);
        assert_eq!(builder.len(), 19);
        assert_eq!(builder.is_null(BlocksIndex::new(2, 11)), !c[11]);

        let first = builder.take_block().unwrap();
        assert_eq!(block_validity(first.as_ref(), 5), a);
        assert_eq!(builder.current_block_index, 1);
        assert_eq!(builder.len(), 14);

        builder.push_null();
        let mut c_plus = c.clone();
        c_plus.push(false);
        assert_eq!(drain_manual(&mut builder, &[2, 13]), vec![b, c_plus]);
        assert_eq!(builder.blocks.len(), 1);
    }

    #[test]
    fn manual_take_n_relayouts() {
        let values = pattern(10, 30);
        let blocks = || {
            vec![
                values[0..13].to_vec(),
                values[13..20].to_vec(),
                values[20..30].to_vec(),
            ]
        };

        let mut builder = manual_with_blocks(&blocks());
        let taken = builder.take_n(5, Some([8usize, 7, 10].into_iter()));
        assert_eq!(block_validity(taken.as_ref(), 5), &values[..5]);
        assert_eq!(builder.len(), 25);
        assert_eq!(
            drain_manual(&mut builder, &[8, 7, 10]),
            vec![
                values[5..13].to_vec(),
                values[13..20].to_vec(),
                values[20..30].to_vec()
            ]
        );

        let mut builder = manual_with_blocks(&blocks());
        let taken = builder.take_n(3, Some([27usize].into_iter()));
        assert_eq!(block_validity(taken.as_ref(), 3), &values[..3]);
        assert_eq!(
            drain_manual(&mut builder, &[27]),
            vec![values[3..30].to_vec()]
        );

        let mut builder = manual_with_blocks(&blocks());
        let taken = builder.take_n(1, Some([3usize, 9, 1, 16].into_iter()));
        assert_eq!(block_validity(taken.as_ref(), 1), &values[..1]);
        assert_eq!(
            drain_manual(&mut builder, &[3, 9, 1, 16]),
            vec![
                values[1..4].to_vec(),
                values[4..13].to_vec(),
                values[13..14].to_vec(),
                values[14..30].to_vec()
            ]
        );

        let mut builder = manual_with_blocks(&blocks());
        let taken = builder.take_n(13, Some([7usize, 10].into_iter()));
        assert_eq!(block_validity(taken.as_ref(), 13), &values[..13]);
        assert_eq!(
            drain_manual(&mut builder, &[7, 10]),
            vec![values[13..20].to_vec(), values[20..30].to_vec()]
        );

        let mut builder = manual_with_blocks(&blocks());
        let taken = builder.take_n(0, Some([13usize, 7, 10].into_iter()));
        assert!(taken.is_none());
        assert_eq!(drain_manual(&mut builder, &[13, 7, 10]), blocks());
    }

    #[test]
    fn manual_take_n_matches_model() {
        for seed in 0..200usize {
            let sizes: Vec<usize> = (0..4).map(|i| 1 + (seed * (i + 3)) % 19).collect();
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

            let n = 1 + seed % sizes[0];
            let remaining = total - n;
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
            assert_eq!(
                block_validity(taken.as_ref(), n),
                &values[..n],
                "seed {seed}"
            );
            assert_eq!(builder.len(), remaining, "seed {seed}");

            let drained = drain_manual(&mut builder, &adjusted);
            assert_eq!(drained.concat(), &values[n..], "seed {seed}");
        }
    }
}
