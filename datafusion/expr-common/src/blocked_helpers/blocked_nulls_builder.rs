use crate::blocked_helpers::BlockedBooleanBuilder;
use crate::groups_accumulator::{BlockedGroupSelection, BlockedIndex};
use arrow::array::NullBufferBuilder;
use arrow::buffer::NullBuffer;
use std::collections::VecDeque;
use std::iter::once;
use std::ops::{Index, Range};

/// Validity bits over one contiguous mmap'ed bitmap, see [`BlockedBooleanBuilder`].
///
/// A block is only materialized by its first null: until then nothing is written for
/// it, so its pages stay untouched, and it is handed out as `None`. Materializing a block
/// writes ones for the values it already holds, later values write their own bit.
/// Positions are absolute value indexes
#[derive(Debug)]
pub struct BlockedNullsBuilder<const FIXED_BLOCK_SIZING: bool> {
    /// Validity of the live values, flat from `head`. Bits of unmaterialized blocks are
    /// never written and must not be read
    bits: BlockedBooleanBuilder<false>,
    /// Whether each block was materialized (has nulls), one entry per block
    materialized: VecDeque<bool>,
    /// Absolute index of the first live value
    head: usize,
    /// Absolute index one past the last live value
    tail: usize,
    block_size: usize,
    /// Fixed sizing only: absolute index at which the current block is full
    next_block_end: usize,
    /// Manual sizing only: absolute start of every block, `block_starts[0] == head`
    block_starts: VecDeque<usize>,
}

impl<const FIXED_BLOCK_SIZING: bool> BlockedNullsBuilder<FIXED_BLOCK_SIZING> {
    pub fn new(block_size: usize) -> Self {
        if FIXED_BLOCK_SIZING {
            assert_ne!(block_size, 0, "block size must be greater than 0");
        }
        let mut this = Self {
            bits: BlockedBooleanBuilder::new(0),
            materialized: VecDeque::from([false]),
            head: 0,
            tail: 0,
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

    /// Only materialized blocks hold memory
    pub fn allocated_size(&self) -> usize {
        (0..self.num_blocks())
            .filter(|&b| self.materialized[b])
            .map(|b| self.block_range(b).len().div_ceil(8))
            .sum::<usize>()
            + (self.materialized.capacity() + self.block_starts.capacity())
                * size_of::<usize>()
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

    pub fn current_block_len(&self) -> usize {
        self.tail - self.current_block_start()
    }

    pub fn might_have_nulls(&self) -> bool {
        self.materialized.iter().any(|&m| m)
    }

    pub fn start_new_block(&mut self) {
        assert!(
            !FIXED_BLOCK_SIZING,
            "fixed sizing finishes blocks on its own"
        );
        self.block_starts.push_back(self.tail);
        self.materialized.push_back(false);
    }

    /// Extend from the null buffer
    pub fn extends_from_null_buffer(&mut self, null_buffer: &NullBuffer) {
        if null_buffer.null_count() == 0 {
            self.push_n(null_buffer.len(), true);
            return;
        }
        let mut offset = 0;
        while offset < null_buffer.len() {
            let count = self.chunk_len(null_buffer.len() - offset);
            let chunk = null_buffer.slice(offset, count);
            if chunk.null_count() == 0 {
                self.push_valid_in_block(count);
            } else {
                self.materialize_current_block();
                let inner = chunk.inner();
                self.bits.append_packed_range(
                    inner.values(),
                    inner.offset(),
                    inner.len(),
                );
                self.advance_n(count);
            }
            offset += count;
        }
    }

    /// Extend with the validity of the values at `indexes`
    pub fn extends_from_null_buffer_in_indexes(
        &mut self,
        null_buffer: &NullBuffer,
        indexes: &[usize],
    ) {
        self.extend(indexes.iter().map(|&index| null_buffer.is_valid(index)));
    }

    pub fn push_n(&mut self, mut n: usize, is_valid: bool) {
        while n > 0 {
            let count = self.chunk_len(n);
            if is_valid {
                self.push_valid_in_block(count);
            } else {
                // nulls are the unset bits the untouched mapping already holds
                self.materialize_current_block();
                self.bits.skip_n(count);
                self.advance_n(count);
            }
            n -= count;
        }
    }

    pub fn push_n_nulls(&mut self, n: usize) {
        self.push_n(n, false);
    }

    pub fn push_n_non_nulls(&mut self, n: usize) {
        self.push_n(n, true);
    }

    /// Push a valid value and return whether the current block is now full
    #[inline]
    pub fn push_non_null(&mut self) -> bool {
        if *self.materialized.back().expect("always a block") {
            self.bits.append(true);
        } else {
            self.bits.skip_n(1);
        }
        self.advance_n(1)
    }

    /// Push a null and return whether the current block is now full
    #[inline]
    pub fn push_null(&mut self) -> bool {
        self.materialize_current_block();
        self.bits.skip_n(1);
        self.advance_n(1)
    }

    #[inline]
    pub fn is_null<I: BlockedIndex>(&self, index: I) -> bool {
        let (block, offset) = if FIXED_BLOCK_SIZING {
            (
                index.fixed_block(self.block_size),
                index.flat(self.block_size),
            )
        } else {
            (
                index.block_in_blocks(&self.block_starts, self.head),
                index.flat_in_blocks(&self.block_starts, self.head),
            )
        };
        self.materialized[block] && !self.bits.get_bit(offset)
    }

    pub fn build_preserving(
        &self,
        selection: BlockedGroupSelection<'_>,
    ) -> datafusion_common::Result<Option<NullBuffer>> {
        selection.validate_num_groups(self.len())?;
        if !self.might_have_nulls() {
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
        if self.is_empty() {
            return None;
        }
        let n = self.block_range(0).len();
        let materialized = self.materialized.pop_front().expect("always a block");
        let block = self.take_first(n, materialized);
        if !FIXED_BLOCK_SIZING {
            self.block_starts.pop_front();
            if self.block_starts.is_empty() {
                self.block_starts.push_back(self.tail);
            }
        }
        if self.materialized.is_empty() {
            self.materialized.push_back(false);
        }
        self.relayout_fixed();
        Some(block)
    }

    /// Take every non empty block, `None` entries have no nulls
    pub fn take_all(&mut self) -> Vec<Option<NullBuffer>> {
        let mut blocks = vec![];
        while !self.is_empty() {
            let len_before = self.len();
            let block = self.take_block().expect("not empty");
            if len_before != self.len() {
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
    ) -> Option<NullBuffer> {
        assert_eq!(FIXED_BLOCK_SIZING, adjusted_block_size_iter.is_none());
        assert!(n <= self.len(), "n ({n}) must be <= len ({})", self.len());

        // The new layout, absolute ranges, the taken values form the first one
        let new_ranges: Vec<Range<usize>> = if let Some(sizes) = adjusted_block_size_iter
        {
            let mut start = self.head + n;
            let mut ranges = Vec::new();
            ranges.push(self.head..start);
            for size in sizes {
                ranges.push(start..start + size);
                start += size;
            }
            assert_eq!(
                start, self.tail,
                "adjusted block sizes must equal the length of the remaining items"
            );
            ranges
        } else {
            // every block but the last is full and the last always has room
            let first = self.head + n;
            let remaining = self.tail - first;
            let mut ranges = Vec::new();
            ranges.push(self.head..first);
            for b in 0..=(remaining / self.block_size) {
                let start = first + b * self.block_size;
                ranges.push(start..(start + self.block_size).min(self.tail));
            }
            ranges
        };

        let new_materialized = self.relayout(&new_ranges);
        let taken = self.take_first(n, new_materialized[0]);

        self.materialized = new_materialized.into_iter().skip(1).collect();
        if self.materialized.is_empty() {
            self.materialized.push_back(false);
        }
        if !FIXED_BLOCK_SIZING {
            self.block_starts = new_ranges.iter().skip(1).map(|r| r.start).collect();
            if self.block_starts.is_empty() {
                self.block_starts.push_back(self.head);
            }
        }
        self.relayout_fixed();
        taken
    }

    // ---- internals ----

    /// Number of values that still fit the current block, capped at `n`
    fn chunk_len(&self, n: usize) -> usize {
        if FIXED_BLOCK_SIZING {
            (self.next_block_end - self.tail).min(n)
        } else {
            n
        }
    }

    /// Push `count` valid values that fit in the current block
    fn push_valid_in_block(&mut self, count: usize) {
        if *self.materialized.back().expect("always a block") {
            self.bits.append_n(count, true);
        } else {
            self.bits.skip_n(count);
        }
        self.advance_n(count);
    }

    /// Write ones for the values the current block already holds
    fn materialize_current_block(&mut self) {
        let flag = self.materialized.back_mut().expect("always a block");
        if !*flag {
            *flag = true;
            let start = self.current_block_start() - self.head;
            self.bits.set_bits(start..self.len(), true);
        }
    }

    /// Move `tail`, return whether a fixed block just got full
    #[inline]
    fn advance_n(&mut self, n: usize) -> bool {
        self.tail += n;
        if FIXED_BLOCK_SIZING && self.tail == self.next_block_end {
            self.next_block_end += self.block_size;
            self.materialized.push_back(false);
            true
        } else {
            false
        }
    }

    fn current_block_start(&self) -> usize {
        if FIXED_BLOCK_SIZING {
            self.next_block_end - self.block_size
        } else {
            *self.block_starts.back().expect("always at least one block")
        }
    }

    /// Absolute range of a block
    fn block_range(&self, block_index: usize) -> Range<usize> {
        if FIXED_BLOCK_SIZING {
            let start = self.head + block_index * self.block_size;
            start..(start + self.block_size).min(self.tail)
        } else {
            let start = self.block_starts[block_index];
            start
                ..self
                    .block_starts
                    .get(block_index + 1)
                    .copied()
                    .unwrap_or(self.tail)
        }
    }

    /// Materialization of `new_ranges`: a new block is materialized when any old block it
    /// overlaps is, and the parts it takes from unmaterialized old blocks get their ones
    fn relayout(&mut self, new_ranges: &[Range<usize>]) -> Vec<bool> {
        let old: Vec<(Range<usize>, bool)> = (0..self.num_blocks())
            .map(|b| (self.block_range(b), self.materialized[b]))
            .collect();
        new_ranges
            .iter()
            .map(|new| {
                let overlapping = old
                    .iter()
                    .filter(|(range, _)| range.start < new.end && new.start < range.end);
                let materialized = overlapping.clone().any(|(_, m)| *m);
                if materialized {
                    for (range, _) in overlapping.filter(|(_, m)| !*m) {
                        let start = range.start.max(new.start) - self.head;
                        let end = range.end.min(new.end) - self.head;
                        self.bits.set_bits(start..end, true);
                    }
                }
                materialized
            })
            .collect()
    }

    fn relayout_fixed(&mut self) {
        if FIXED_BLOCK_SIZING {
            self.next_block_end =
                self.head + (self.len() / self.block_size + 1) * self.block_size;
        }
    }

    fn reset(&mut self) {
        self.bits.reset();
        self.materialized.clear();
        self.materialized.push_back(false);
        self.head = 0;
        self.tail = 0;
        self.block_starts.clear();
        self.block_starts.push_back(0);
        self.relayout_fixed();
    }

    /// Hand out the first `n` values, `None` when they have no nulls
    fn take_first(&mut self, n: usize, materialized: bool) -> Option<NullBuffer> {
        let bits = self.bits.take_n(n, Some(once(self.len() - n)));
        self.head += n;
        if !materialized {
            return None;
        }
        Some(NullBuffer::new(bits)).filter(|b| b.null_count() > 0)
    }
}

impl<const FIXED_BLOCK_SIZING: bool> Extend<bool>
    for BlockedNullsBuilder<FIXED_BLOCK_SIZING>
{
    fn extend<T: IntoIterator<Item = bool>>(&mut self, iter: T) {
        for is_valid in iter {
            if is_valid {
                self.push_non_null();
            } else {
                self.push_null();
            }
        }
    }
}

impl<const FIXED_BLOCK_SIZING: bool, I: BlockedIndex> Index<I>
    for BlockedNullsBuilder<FIXED_BLOCK_SIZING>
{
    type Output = bool;

    fn index(&self, index: I) -> &bool {
        if self.is_null(index) { &false } else { &true }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::groups_accumulator::BlocksIndex;

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
        assert_eq!(builder.num_blocks(), 3);
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
        assert_eq!(builder.current_block_len(), 0);
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
        assert_eq!(builder.num_blocks(), 3);
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
        assert_eq!(builder.num_blocks(), 1);
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
        assert_eq!(builder.num_blocks(), 3);
        assert_eq!(builder.len(), 19);
        assert_eq!(builder.is_null(BlocksIndex::new(2, 11)), !c[11]);

        let first = builder.take_block().unwrap();
        assert_eq!(block_validity(first.as_ref(), 5), a);
        assert_eq!(builder.num_blocks(), 2);
        assert_eq!(builder.len(), 14);

        builder.push_null();
        let mut c_plus = c.clone();
        c_plus.push(false);
        assert_eq!(drain_manual(&mut builder, &[2, 13]), vec![b, c_plus]);
        assert_eq!(builder.num_blocks(), 1);
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
