use crate::blocked_helpers::CopyItemBlockedVecBuilder;
use crate::groups_accumulator::BlockedIndex;
use arrow::array::BooleanBufferBuilder;
use arrow::buffer::BooleanBuffer;
use arrow::util::bit_util::apply_bitwise_binary_op;
use std::collections::VecDeque;
use std::iter::once;
use std::ops::Index;

/// Bits of every block live contiguously in one mmap'ed region as `u64` words, blocks
/// are only a layout over them, see [`CopyItemBlockedVecBuilder`]. Positions are absolute
/// bit indexes, `words[0]` holds bit `store_base`
#[derive(Debug)]
pub struct BlockedBooleanBuilder<const FIXED_BLOCK_SIZING: bool> {
    words: CopyItemBlockedVecBuilder<false, u64>,
    /// Absolute bit index of the first live bit
    head: usize,
    /// Absolute bit index one past the last live bit
    tail: usize,
    /// Absolute bit index of `words[0]`, always a multiple of 64
    store_base: usize,
    block_size: usize,
    /// Fixed sizing only: absolute bit index at which the current block is full
    next_block_end: usize,
    /// Manual sizing only: absolute start of every block, `block_starts[0] == head`
    block_starts: VecDeque<usize>,
}

impl<const FIXED_BLOCK_SIZING: bool> BlockedBooleanBuilder<FIXED_BLOCK_SIZING> {
    pub fn new(block_size: usize) -> Self {
        if FIXED_BLOCK_SIZING {
            assert_ne!(block_size, 0, "block size must be greater than 0");
        }
        let mut this = Self {
            words: CopyItemBlockedVecBuilder::new(0),
            head: 0,
            tail: 0,
            store_base: 0,
            block_size,
            next_block_end: 0,
            block_starts: VecDeque::from([0]),
        };
        this.relayout_fixed();
        this
    }

    pub fn len(&self) -> usize {
        self.tail - self.head
    }

    pub fn is_empty(&self) -> bool {
        self.head == self.tail
    }

    pub fn allocated_size(&self) -> usize {
        self.words.allocated_size() + self.block_starts.capacity() * size_of::<usize>()
    }

    pub fn block_size(&self) -> usize {
        assert!(
            FIXED_BLOCK_SIZING,
            "block size is only available for manual block"
        );
        self.block_size
    }

    pub fn num_blocks(&self) -> usize {
        if FIXED_BLOCK_SIZING {
            self.len() / self.block_size + 1
        } else {
            self.block_starts.len()
        }
    }

    /// Number of bits in the current block
    pub fn current_block_len(&self) -> usize {
        let start = if FIXED_BLOCK_SIZING {
            self.next_block_end - self.block_size
        } else {
            *self.block_starts.back().expect("always at least one block")
        };
        self.tail - start
    }

    pub fn start_new_block(&mut self) {
        assert!(
            !FIXED_BLOCK_SIZING,
            "fixed sizing finishes blocks on its own"
        );
        self.block_starts.push_back(self.tail);
    }

    fn current_block_remaining_len(&self) -> usize {
        assert!(
            FIXED_BLOCK_SIZING,
            "remaining block only available for manual block"
        );
        self.block_size - self.current_block_len()
    }

    pub fn append_n(&mut self, mut n: usize, is_set: bool) {
        // fill up the partial word, then whole words at once, then start the last one
        let (word, bit) = self.position(self.tail);
        if bit != 0 && n > 0 {
            let count = n.min(64 - bit);
            let mask = Self::mask(count) << bit;
            if is_set {
                self.words[word] |= mask;
            } else {
                self.words[word] &= !mask;
            }
            self.tail += count;
            n -= count;
        }
        let whole_words = n / 64;
        if whole_words > 0 {
            self.words
                .push_value_n(if is_set { u64::MAX } else { 0 }, whole_words);
            self.tail += whole_words * 64;
            n -= whole_words * 64;
        }
        if n > 0 {
            self.words.push(if is_set { Self::mask(n) } else { 0 });
            self.tail += n;
        }
        self.relayout_fixed();
    }

    /// Append a bit and return whether the current block is now full
    #[inline]
    pub fn append(&mut self, is_set: bool) -> bool {
        let (word, bit) = self.position(self.tail);
        if bit == 0 {
            self.words.push(u64::from(is_set));
        } else if is_set {
            self.words[word] |= 1 << bit;
        }
        self.tail += 1;
        if FIXED_BLOCK_SIZING && self.tail == self.next_block_end {
            self.next_block_end += self.block_size;
            true
        } else {
            false
        }
    }

    #[inline]
    pub fn get_bit<I: BlockedIndex>(&self, index: I) -> bool {
        let (word, bit) = self.position(self.head + self.offset(index));
        (self.words[word] >> bit) & 1 == 1
    }

    #[inline]
    pub fn set_bit<I: BlockedIndex>(&mut self, index: I, is_set: bool) {
        let (word, bit) = self.position(self.head + self.offset(index));
        if is_set {
            self.words[word] |= 1 << bit;
        } else {
            self.words[word] &= !(1 << bit);
        }
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
        let remaining = if FIXED_BLOCK_SIZING {
            self.current_block_remaining_len()
        } else {
            usize::MAX
        };
        let prev = self.tail;
        for is_set in iter {
            self.append(is_set);
        }
        let added_items = self.tail - prev;
        assert!(
            added_items <= remaining,
            "overflow from block new block length: {}, block size: {}",
            remaining + added_items,
            self.block_size
        );
        added_items
    }

    /// Append `n` bits without writing them, they read as unset since the untouched
    /// mapping is zero filled
    pub(crate) fn skip_n(&mut self, n: usize) {
        let words_needed =
            (self.tail + n - self.store_base).div_ceil(64) - self.words.len();
        // SAFETY: zero is a valid u64
        unsafe { self.words.advance_untouched(words_needed) };
        self.tail += n;
        self.relayout_fixed();
    }

    /// Set every bit of the flat `range`
    pub(crate) fn set_bits(&mut self, range: std::ops::Range<usize>, is_set: bool) {
        let mut abs = self.head + range.start;
        let end = self.head + range.end;
        while abs < end {
            let (word, bit) = self.position(abs);
            let count = (end - abs).min(64 - bit);
            let mask = Self::mask(count) << bit;
            if is_set {
                self.words[word] |= mask;
            } else {
                self.words[word] &= !mask;
            }
            abs += count;
        }
    }

    /// Append `len` bits starting at `offset` of the packed `bits`
    pub(crate) fn append_packed_range(&mut self, bits: &[u8], offset: usize, len: usize) {
        if len == 0 {
            return;
        }
        let write_offset = self.tail - self.store_base;
        self.append_n(len, false);
        apply_bitwise_binary_op(
            self.bytes_mut(),
            write_offset,
            bits,
            offset,
            len,
            |_a, b| b,
        );
    }

    /// Take the first block, `None` once there are no more items
    pub fn take_block(&mut self) -> Option<BooleanBuffer> {
        if self.is_empty() {
            return None;
        }
        let n = self.block_len(0);
        let block = self.take_first(n);
        if !FIXED_BLOCK_SIZING {
            self.block_starts.pop_front();
            if self.block_starts.is_empty() {
                self.block_starts.push_back(self.tail);
            }
        }
        self.relayout_fixed();
        Some(block)
    }

    /// Take every non empty block
    pub fn take_all(&mut self) -> Vec<BooleanBuffer> {
        let mut blocks = vec![];
        while let Some(block) = self.take_block() {
            if !block.is_empty() {
                blocks.push(block);
            }
        }
        self.reset();
        blocks
    }

    /// Take the first `n` values
    ///
    /// `adjusted_block_size_iter` is iterator over the number of items in each block **after** emitting `n`
    ///
    /// this is `None` when `FIXED_BLOCK_SIZING` is true
    ///
    /// The adjusted iterator must meet this requirement:
    /// ```
    /// assert_eq!(n + adjusted_block_size_iter.sum(), self.len);
    /// ```
    pub fn take_n(
        &mut self,
        n: usize,
        adjusted_block_size_iter: Option<impl Iterator<Item = usize> + Clone>,
    ) -> BooleanBuffer {
        assert_eq!(FIXED_BLOCK_SIZING, adjusted_block_size_iter.is_none());
        assert!(n <= self.len(), "n ({n}) must be <= len ({})", self.len());

        let taken = self.take_first(n);

        if let Some(sizes) = adjusted_block_size_iter {
            self.block_starts.clear();
            let mut start = self.head;
            for size in sizes {
                self.block_starts.push_back(start);
                start += size;
            }
            assert_eq!(
                start, self.tail,
                "adjusted block sizes must equal the length of the remaining items"
            );
            if self.block_starts.is_empty() {
                self.block_starts.push_back(self.head);
            }
        }
        self.relayout_fixed();
        taken
    }

    // ---- internals ----

    pub(crate) fn reset(&mut self) {
        self.words.reset();
        self.head = 0;
        self.tail = 0;
        self.store_base = 0;
        self.block_starts.clear();
        self.block_starts.push_back(0);
        self.relayout_fixed();
    }

    /// The lowest `count` bits set, `count <= 64`
    #[inline]
    fn mask(count: usize) -> u64 {
        u64::MAX >> (64 - count)
    }

    /// Word and bit of an absolute bit index inside `words`
    #[inline]
    fn position(&self, abs: usize) -> (usize, usize) {
        let rel = abs - self.store_base;
        (rel >> 6, rel & 63)
    }

    #[inline]
    fn offset<I: BlockedIndex>(&self, index: I) -> usize {
        if FIXED_BLOCK_SIZING {
            index.flat(self.block_size)
        } else {
            index.flat_in_blocks(&self.block_starts, self.head)
        }
    }

    fn bytes(&self) -> &[u8] {
        let words = self.words.as_slice();
        // SAFETY: a u64 slice is valid to read as 8x as many bytes, arrow bit order is little endian
        unsafe {
            std::slice::from_raw_parts(words.as_ptr().cast::<u8>(), words.len() * 8)
        }
    }

    fn bytes_mut(&mut self) -> &mut [u8] {
        let words = self.words.as_mut_slice();
        // SAFETY: as in `bytes`
        unsafe {
            std::slice::from_raw_parts_mut(
                words.as_mut_ptr().cast::<u8>(),
                words.len() * 8,
            )
        }
    }

    fn block_len(&self, block_index: usize) -> usize {
        if FIXED_BLOCK_SIZING {
            (self.len() - block_index * self.block_size).min(self.block_size)
        } else {
            let start = self.block_starts[block_index];
            self.block_starts
                .get(block_index + 1)
                .copied()
                .unwrap_or(self.tail)
                - start
        }
    }

    fn relayout_fixed(&mut self) {
        if FIXED_BLOCK_SIZING {
            self.next_block_end =
                self.head + (self.len() / self.block_size + 1) * self.block_size;
        }
    }

    /// Hand out the first `n` bits and drop the whole words they leave behind.
    /// Zero copy when the range ends on a word boundary, the bits are copied otherwise
    fn take_first(&mut self, n: usize) -> BooleanBuffer {
        let start = self.head - self.store_base;
        let end = start + n;
        let whole_words = end / 64;
        let remaining_words = self.words.len() - whole_words;

        let taken = if end.is_multiple_of(64) {
            let words = self.words.take_n(whole_words, Some(once(remaining_words)));
            BooleanBuffer::new(words.into(), start, n)
        } else {
            let mut copy = BooleanBufferBuilder::new(n);
            copy.append_packed_range(start..end, self.bytes());
            if whole_words > 0 {
                drop(self.words.take_n(whole_words, Some(once(remaining_words))));
            }
            copy.finish()
        };
        self.store_base += whole_words * 64;
        self.head += n;
        taken
    }
}

impl<const FIXED_BLOCK_SIZING: bool, I: BlockedIndex> Index<I>
    for BlockedBooleanBuilder<FIXED_BLOCK_SIZING>
{
    type Output = bool;

    fn index(&self, index: I) -> &bool {
        if self.get_bit(index) { &true } else { &false }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::groups_accumulator::BlocksIndex;

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
        assert_eq!(
            builder.num_blocks(),
            builder.len() / block_size + 1,
            "unexpected number of blocks for len {} and block size {block_size}",
            builder.len()
        );
        assert_eq!(builder.current_block_len(), builder.len() % block_size);
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
        assert_eq!(builder.num_blocks(), 3);
        assert_eq!(builder.current_block_len(), 0);
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
        assert_eq!(builder.num_blocks(), 2);
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
        // memory is returned page by page, so make a block span a whole page
        let page = unsafe { libc::sysconf(libc::_SC_PAGESIZE) } as usize;
        let block_size = page * 8;
        let mut builder = Fixed::new(block_size);
        let empty = builder.allocated_size();

        builder.append_n(block_size * 3 + 1, true);
        let full = builder.allocated_size();
        assert!(full >= empty + 3 * page);

        let block = builder.take_block();
        drop(block);
        assert_eq!(builder.allocated_size(), full - page);

        builder.take_all();
        assert!(builder.allocated_size() < 2 * page);
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
        assert_eq!(builder.num_blocks(), 1);
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
        assert_eq!(builder.num_blocks(), 3);
        assert_eq!(builder.len(), 19);
        assert_eq!(builder.get_bit(BlocksIndex::new(1, 1)), b[1]);
        assert_eq!(builder.get_bit(BlocksIndex::new(2, 11)), c[11]);

        assert_eq!(buffer_bits(&builder.take_block().unwrap()), a);
        assert_eq!(builder.len(), 14);
        assert_eq!(builder.num_blocks(), 2);
        assert_eq!(builder.get_bit(BlocksIndex::new(0, 1)), b[1]);

        // appends go to the last block
        builder.append(true);
        assert!(builder.get_bit(BlocksIndex::new(1, 12)));

        let mut c_plus = c.clone();
        c_plus.push(true);
        assert_eq!(drain_manual(&mut builder), vec![b, c_plus]);
        assert_eq!(builder.len(), 0);
        assert!(builder.take_block().is_none());
        assert_eq!(builder.num_blocks(), 1);
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
