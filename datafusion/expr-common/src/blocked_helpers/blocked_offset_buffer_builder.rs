use crate::blocked_helpers::{CopyItemBlockedVecBuilder, MmapVec};
use crate::groups_accumulator::BlocksIndex;
use arrow::array::OffsetSizeTrait;
use arrow::buffer::{OffsetBuffer, ScalarBuffer};
use std::ops::Index;

/// Offsets of every block live contiguously in one mmap'ed region, each block starts
/// with its own zero offset so a block of `block_size` items takes `block_size + 1` slots.
///
/// When `FIXED_BLOCK_SIZING` is true, the block size is the `Self::block_size` otherwise,
/// the callers control the block size
#[derive(Debug)]
pub struct BlockedOffsetBufferBuilder<const FIXED_BLOCK_SIZING: bool, O: OffsetSizeTrait>
{
    /// Fixed sizing: the block size of the slots is `block_size + 1` to fit the initial offset
    slots: CopyItemBlockedVecBuilder<FIXED_BLOCK_SIZING, O>,

    /// The total number of items, not the number of slots
    len: usize,

    /// The last offset in the current block
    last_offset: O,
}

impl<const FIXED_BLOCK_SIZING: bool, O: OffsetSizeTrait>
    BlockedOffsetBufferBuilder<FIXED_BLOCK_SIZING, O>
{
    pub fn new(block_size: usize) -> Self {
        if FIXED_BLOCK_SIZING {
            assert_ne!(block_size, 0, "block size must be greater than 0");
        }

        let mut slots = CopyItemBlockedVecBuilder::new(block_size + 1);
        slots.push(O::zero());
        BlockedOffsetBufferBuilder {
            slots,
            len: 0,
            last_offset: O::zero(),
        }
    }

    pub fn len(&self) -> usize {
        self.len
    }

    pub fn is_empty(&self) -> bool {
        self.len == 0
    }

    /// Number of items per block, the internal block size has one more slot for the initial offset
    pub fn block_size(&self) -> usize {
        self.slots.block_size() - 1
    }

    pub fn allocated_size(&self) -> usize {
        self.slots.allocated_size()
    }

    /// Get the number of elements in the current block (not the number of offsets since the first offset is always 0)
    pub fn current_block_len(&self) -> usize {
        self.slots.current_block_len() - 1
    }

    pub fn current_block_index(&self) -> usize {
        self.slots.num_blocks() - 1
    }

    pub fn num_blocks(&self) -> usize {
        self.slots.num_blocks()
    }

    pub fn last_offset(&self) -> O {
        self.last_offset
    }

    pub fn start_new_block(&mut self) {
        self.slots.start_new_block();
        self.open_block();
    }

    /// Write the initial offset of a block that was just opened
    fn open_block(&mut self) {
        self.last_offset = O::zero();
        self.slots.push(self.last_offset);
    }

    /// Fixed sizing: open the next block if the current one just got full, returns if it did
    fn finish_block_if_full(&mut self) -> bool {
        if FIXED_BLOCK_SIZING && self.slots.current_block_len() == 0 {
            self.open_block();
            true
        } else {
            false
        }
    }

    pub fn push_next_offset_in_block(&mut self, next_offset_in_block: O) -> bool {
        assert!(
            next_offset_in_block >= self.last_offset,
            "offsets must be monotonically increasing"
        );
        self.last_offset = next_offset_in_block;
        self.len += 1;
        if self.slots.push(self.last_offset) {
            self.open_block();
            true
        } else {
            false
        }
    }

    /// Push length and return if the current block is now full
    #[inline]
    pub fn push_length(&mut self, length: usize) -> bool {
        self.last_offset += O::usize_as(length);
        self.len += 1;
        if self.slots.push(self.last_offset) {
            self.open_block();
            true
        } else {
            false
        }
    }

    /// Append `offsets[1..]` rebased onto the last offset, all within the current block
    pub(super) fn extends_length_from_offsets_in_current_block(
        &mut self,
        offset_buffer_slice: &[O],
    ) -> bool {
        assert_ne!(offset_buffer_slice.len(), 0);
        if FIXED_BLOCK_SIZING {
            assert!(
                self.current_block_remaining_len() >= offset_buffer_slice.len() - 1,
                "the amount to add exceed the current block size"
            );
        }

        let added = offset_buffer_slice.len() - 1;
        let start = self.slots.len();
        self.slots.extend_from_slice(&offset_buffer_slice[1..]);

        // Rebase, easily SIMD-ed: `[0, 2, 3]` extended with `[6, 9, 10]` becomes `[0, 2, 3, 6, 7]`
        let base = offset_buffer_slice[0];
        let last_offset = self.last_offset;
        for offset in &mut self.slots.as_mut_slice()[start..] {
            *offset = *offset - base + last_offset;
        }

        self.last_offset = self.slots.as_slice()[self.slots.len() - 1];
        self.len += added;
        self.finish_block_if_full()
    }

    /// Extend the length from the current offsets
    pub fn extends_length_from_offsets(&mut self, mut offset_buffer_slice: &[O]) {
        // If not fixed, then treat all offsets as single block
        if !FIXED_BLOCK_SIZING {
            self.extends_length_from_offsets_in_current_block(offset_buffer_slice);
            return;
        }

        let mut len = offset_buffer_slice.len() - 1;
        while len > 0 {
            let to_add = self.current_block_remaining_len().min(len);
            let offsets_in_block = &offset_buffer_slice[..=to_add];
            offset_buffer_slice = &offset_buffer_slice[to_add..];
            len -= to_add;

            self.extends_length_from_offsets_in_current_block(offsets_in_block);
        }
    }

    /// Extend the length from the current offsets in the indexes
    pub fn extends_length_from_offsets_in_indexes(
        &mut self,
        offset_buffer_slice: &[O],
        indexes: &[usize],
    ) {
        self.slots.reserve(indexes.len());
        for &index in indexes {
            let length = offset_buffer_slice[index + 1] - offset_buffer_slice[index];
            self.push_length(length.as_usize());
        }
    }

    pub(super) fn current_block_remaining_len(&self) -> usize {
        assert!(
            FIXED_BLOCK_SIZING,
            "current block remaining length is only relevant for manual block size"
        );
        self.block_size() - self.current_block_len()
    }

    pub(crate) fn push_empty_within_block(&mut self, n: usize) -> bool {
        if FIXED_BLOCK_SIZING {
            assert!(
                n <= self.current_block_remaining_len(),
                "overflow from block new block length: {}, block size: {}",
                self.current_block_len() + n,
                self.block_size()
            );
        }
        self.len += n;
        self.slots.push_value_n(self.last_offset, n);
        self.finish_block_if_full()
    }

    /// Push length 0
    pub fn push_empty_n(&mut self, n: usize) {
        self.push_length_n(0, n);
    }

    /// Push `n` items of `len` bytes each
    pub fn push_length_n(&mut self, len: usize, mut n: usize) {
        // If not fixed, then treat all offsets as single block
        if !FIXED_BLOCK_SIZING {
            self.push_length_within_block(len, n);
            return;
        }

        self.slots.reserve(n + n / self.block_size() + 1);
        while n > 0 {
            let to_add = self.current_block_remaining_len().min(n);
            n -= to_add;
            self.push_length_within_block(len, to_add);
        }
    }

    fn push_length_within_block(&mut self, len: usize, n: usize) {
        if len == 0 {
            self.push_empty_within_block(n);
            return;
        }
        let step = O::usize_as(len);
        let mut last_offset = self.last_offset;
        self.slots.extend((0..n).map(|_| {
            last_offset += step;
            last_offset
        }));
        self.last_offset = last_offset;
        self.len += n;
        self.finish_block_if_full();
    }

    /// Take the first block, `None` once there are no more items
    pub fn take_block(&mut self) -> Option<ScalarBuffer<O>> {
        if self.len == 0 {
            return None;
        }

        let block = self.slots.take_first_block().into_scalar_buffer();
        self.len -= block.len() - 1;
        if self.slots.is_empty() {
            self.open_block();
        }
        Some(block)
    }

    pub fn take_block_finished(&mut self) -> Option<OffsetBuffer<O>> {
        self.take_block().map(Self::offsets_from_vec)
    }

    fn offsets_from_vec(block: ScalarBuffer<O>) -> OffsetBuffer<O> {
        // SAFETY: this is safe as we are the one that control the offsets
        unsafe { OffsetBuffer::new_unchecked(block) }
    }

    /// Take every non empty block
    pub fn take_all(&mut self) -> Vec<ScalarBuffer<O>> {
        let blocks = self
            .slots
            .take_all()
            .into_iter()
            .filter(|b| b.len() > 1)
            .map(MmapVec::into_scalar_buffer)
            .collect();
        self.len = 0;
        self.open_block();
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
    ) -> ScalarBuffer<O> {
        assert_eq!(FIXED_BLOCK_SIZING, adjusted_block_size_iter.is_none());
        assert!(n <= self.len, "n ({n}) must be <= len ({})", self.len);

        // Every block is self relative so the remaining items have to be rebased anyway,
        // replay their lengths into a fresh region and let the old one go
        let fresh = Self::new(self.block_size()).slots;
        let mut old = std::mem::replace(&mut self.slots, fresh);
        let old_len = self.len;
        self.len = 0;
        self.last_offset = O::zero();

        {
            let mut lengths = (0..old.num_blocks())
                .flat_map(|b| old.block(b).windows(2))
                .map(|w| (w[1] - w[0]).as_usize())
                .skip(n);

            match adjusted_block_size_iter {
                None => {
                    for length in lengths {
                        self.push_length(length);
                    }
                }
                Some(sizes) => {
                    for (i, size) in sizes.enumerate() {
                        if i > 0 {
                            self.start_new_block();
                        }
                        for _ in 0..size {
                            let length = lengths.next().expect(
                                "sum of adjusted block sizes + n must equal the length",
                            );
                            self.push_length(length);
                        }
                    }
                    assert_eq!(
                        n + self.len,
                        old_len,
                        "sum of adjusted block sizes ({}) + n ({n}) must equal the length {old_len}",
                        self.len
                    );
                }
            }
        }

        // The first block starts at zero so its first `n + 1` slots are the taken offsets
        if FIXED_BLOCK_SIZING {
            old.take_n(n + 1, None::<std::iter::Empty<usize>>)
        } else {
            old.take_n(n + 1, Some(std::iter::once(old.len() - n - 1)))
        }
        .into_scalar_buffer()
    }

    pub fn blocks_iter(&self) -> impl Iterator<Item = &[O]> + Clone {
        (0..self.num_blocks()).map(|b| self.slots.block(b))
    }
}

impl<O: OffsetSizeTrait> BlockedOffsetBufferBuilder<true, O> {
    pub fn take_n_fixed(&mut self, n: usize) -> ScalarBuffer<O> {
        self.take_n(n, None::<std::iter::Empty<usize>>)
    }
}

impl<const FIXED_BLOCK_SIZING: bool, O: OffsetSizeTrait> Extend<usize>
    for BlockedOffsetBufferBuilder<FIXED_BLOCK_SIZING, O>
{
    fn extend<T: IntoIterator<Item = usize>>(&mut self, iter: T) {
        let iter = iter.into_iter();
        self.slots.reserve(iter.size_hint().0);
        for length in iter {
            self.push_length(length);
        }
    }
}

impl<O: OffsetSizeTrait> Index<usize> for BlockedOffsetBufferBuilder<true, O> {
    type Output = O;

    fn index(&self, index: usize) -> &Self::Output {
        &self[BlocksIndex::from_index_in_fixed_block_size(index, self.block_size())]
    }
}

impl<const FIXED_BLOCK_SIZING: bool, O: OffsetSizeTrait> Index<BlocksIndex>
    for BlockedOffsetBufferBuilder<FIXED_BLOCK_SIZING, O>
{
    type Output = O;

    #[inline]
    fn index(&self, index: BlocksIndex) -> &Self::Output {
        &self.slots[index]
    }
}

impl<const FIXED_BLOCK_SIZING: bool, O: OffsetSizeTrait> IntoIterator
    for BlockedOffsetBufferBuilder<FIXED_BLOCK_SIZING, O>
{
    type Item = OffsetBuffer<O>;
    type IntoIter = std::vec::IntoIter<OffsetBuffer<O>>;

    fn into_iter(mut self) -> Self::IntoIter {
        self.take_all()
            .into_iter()
            .map(Self::offsets_from_vec)
            .collect::<Vec<_>>()
            .into_iter()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::buffer::OffsetBuffer;

    #[test]
    fn newly_created_builder_should_return_none() {
        let mut builder = BlockedOffsetBufferBuilder::<true, i32>::new(10);
        assert_eq!(builder.take_block(), None);
        assert_eq!(builder.take_block(), None);
    }

    #[test]
    fn newly_created_builder_with_exactly_1_block_should_return_1_block_and_then_none() {
        let block_size = 6;
        let lengths_to_add = vec![3; block_size];
        run_on_all_ways_to_add::<i32>(block_size, &lengths_to_add, |builder, source| {
            let expected_offsets =
                OffsetBuffer::<i32>::from_lengths(lengths_to_add.clone());
            assert_eq!(
                builder.take_block().as_deref(),
                Some(expected_offsets.as_ref()),
                "failed when source is {source}"
            );
            assert_eq!(builder.take_block(), None, "failed when source is {source}");
        });
    }

    #[test]
    fn newly_created_builder_with_exactly_n_block_should_return_n_block_and_then_none() {
        let block_size = 6;
        let number_of_blocks = 3;
        let lengths_blocked = vec![vec![3; block_size]; number_of_blocks];
        let lengths_to_add = lengths_blocked
            .iter()
            .flatten()
            .copied()
            .collect::<Vec<_>>();
        run_on_all_ways_to_add::<i32>(block_size, &lengths_to_add, |builder, source| {
            for length_blocked in &lengths_blocked {
                let expected_offsets =
                    OffsetBuffer::<i32>::from_lengths(length_blocked.clone());
                assert_eq!(
                    builder.take_block().as_deref(),
                    Some(expected_offsets.as_ref()),
                    "failed when source is {source}"
                );
            }

            assert_eq!(builder.take_block(), None, "failed when source is {source}");
        });
    }

    fn run_on_all_ways_to_add<O: OffsetSizeTrait>(
        block_size: usize,
        lengths_to_add: &[usize],
        on_added: impl Fn(&mut BlockedOffsetBufferBuilder<true, O>, &'static str),
    ) {
        {
            let mut builder = BlockedOffsetBufferBuilder::<true, O>::new(block_size);

            for _ in 0..2 {
                for &len in lengths_to_add {
                    builder.push_length(len);
                }
                on_added(&mut builder, "push_length");
            }
        }

        {
            let mut builder = BlockedOffsetBufferBuilder::<true, O>::new(block_size);

            for _ in 0..2 {
                builder.extend(lengths_to_add.iter().copied());
                on_added(&mut builder, "extend");
            }
        }

        if !lengths_to_add.is_empty() {
            let mut builder = BlockedOffsetBufferBuilder::<true, O>::new(block_size);

            for _ in 0..2 {
                let mut current_len: usize = lengths_to_add[0];
                let mut repeat: usize = 1;

                for &len in &lengths_to_add[1..] {
                    if len == current_len {
                        repeat += 1;
                    } else {
                        builder.push_length_n(current_len, repeat);
                        current_len = len;
                        repeat = 1;
                    }
                }

                builder.push_length_n(current_len, repeat);

                on_added(&mut builder, "push_length_n");
            }
        }

        {
            let mut builder = BlockedOffsetBufferBuilder::<true, O>::new(block_size);

            for _ in 0..2 {
                let offset_buffer_input =
                    OffsetBuffer::<O>::from_lengths(lengths_to_add.iter().copied());
                builder.extends_length_from_offsets(&offset_buffer_input);
                on_added(&mut builder, "extends_length_from_offsets");
            }
        }

        {
            let mut builder = BlockedOffsetBufferBuilder::<true, O>::new(block_size);

            let mut lengths_to_add_modified = lengths_to_add.to_vec();
            let mut indices = (0..lengths_to_add.len() + 2).collect::<Vec<_>>();
            lengths_to_add_modified.insert(lengths_to_add.len() - 1, 10);
            indices.remove(lengths_to_add.len());

            lengths_to_add_modified.insert(0, 40);
            indices.remove(0);

            let offset_buffer_input =
                OffsetBuffer::<O>::from_lengths(lengths_to_add_modified);

            for _ in 0..2 {
                builder.extends_length_from_offsets_in_indexes(
                    &offset_buffer_input,
                    &indices,
                );
                on_added(&mut builder, "extends_length_from_offsets_in_indexes");
            }
        }
    }

    // ---- fixed block sizing ----

    type Fixed = BlockedOffsetBufferBuilder<true, i32>;
    type Manual = BlockedOffsetBufferBuilder<false, i64>;

    fn lengths<O: OffsetSizeTrait>(offsets: &[O]) -> Vec<usize> {
        offsets
            .windows(2)
            .map(|w| (w[1] - w[0]).as_usize())
            .collect()
    }

    fn model_lengths(n: usize) -> Vec<usize> {
        (0..n).map(|i| (i * 7) % 5).collect()
    }

    /// Every block starts at 0, all but the last hold exactly `block_size` items and there
    /// is always room in the last one for the next push
    fn check_fixed_layout(builder: &Fixed, block_size: usize) {
        assert_eq!(
            builder.num_blocks(),
            builder.len() / block_size + 1,
            "unexpected number of blocks for len {} and block size {block_size}",
            builder.len()
        );
        assert_eq!(builder.current_block_index(), builder.num_blocks() - 1);
        assert_eq!(builder.current_block_len(), builder.len() % block_size);
        let num_blocks = builder.num_blocks();
        for (i, block) in builder.blocks_iter().enumerate() {
            assert_eq!(block[0], 0, "block {i} must start at offset 0");
            if i + 1 < num_blocks {
                assert_eq!(block.len(), block_size + 1, "block {i} must be full");
            }
        }
        assert_eq!(
            builder.last_offset(),
            *builder.blocks_iter().last().unwrap().last().unwrap()
        );
    }

    fn fixed_with(block_size: usize, lens: &[usize]) -> Fixed {
        let mut builder = Fixed::new(block_size);
        for &len in lens {
            builder.push_length(len);
        }
        check_fixed_layout(&builder, block_size);
        builder
    }

    fn drain<const F: bool, O: OffsetSizeTrait>(
        builder: &mut BlockedOffsetBufferBuilder<F, O>,
    ) -> Vec<Vec<usize>> {
        let mut out = vec![];
        while let Some(block) = builder.take_block() {
            assert_eq!(block[0], O::zero());
            out.push(lengths(&block));
        }
        out
    }

    fn all_lengths(builder: &Fixed, block_size: usize) -> Vec<usize> {
        (0..builder.len())
            .map(|i| {
                let index = BlocksIndex::from_index_in_fixed_block_size(i, block_size);
                (builder[index.next_index_in_block()] - builder[index]) as usize
            })
            .collect()
    }

    #[test]
    fn new_is_empty() {
        let mut builder = Fixed::new(4);
        assert_eq!(builder.len(), 0);
        assert_eq!(
            builder.block_size(),
            4,
            "block size is in items, not offsets"
        );
        assert_eq!(builder.last_offset(), 0);
        check_fixed_layout(&builder, 4);
        assert_eq!(builder.take_block(), None);
        assert_eq!(builder.take_block_finished(), None);
        assert!(builder.take_all().is_empty());
        check_fixed_layout(&builder, 4);
    }

    #[test]
    #[should_panic(expected = "block size must be greater than 0")]
    fn fixed_zero_block_size_panics() {
        Fixed::new(0);
    }

    #[test]
    fn push_length_spans_blocks() {
        let mut builder = Fixed::new(3);
        let finished: Vec<bool> = (1..=7).map(|len| builder.push_length(len)).collect();
        assert_eq!(finished, [false, false, true, false, false, true, false]);
        assert_eq!(builder.len(), 7);
        assert_eq!(builder.last_offset(), 7);
        check_fixed_layout(&builder, 3);
        assert_eq!(all_lengths(&builder, 3), (1..=7).collect::<Vec<_>>());

        assert_eq!(
            drain(&mut builder),
            vec![vec![1, 2, 3], vec![4, 5, 6], vec![7]]
        );
        assert_eq!(builder.len(), 0);
        check_fixed_layout(&builder, 3);
    }

    #[test]
    fn index_returns_start_offset_within_block() {
        let builder = fixed_with(3, &[1, 2, 3, 4, 5, 6, 7]);
        // block 0 offsets [0,1,3,6], block 1 [0,4,9,15], block 2 [0,7]
        assert_eq!(builder[0], 0);
        assert_eq!(builder[1], 1);
        assert_eq!(builder[2], 3);
        assert_eq!(builder[3], 0);
        assert_eq!(builder[4], 4);
        assert_eq!(builder[5], 9);
        assert_eq!(builder[6], 0);
        assert_eq!(builder[BlocksIndex::new(0, 3)], 6);
        assert_eq!(builder[BlocksIndex::new(1, 3)], 15);
        assert_eq!(builder[BlocksIndex::new(2, 1)], 7);
    }

    #[test]
    fn extend_spans_blocks() {
        let mut builder = Fixed::new(3);
        builder.extend(std::iter::empty());
        check_fixed_layout(&builder, 3);
        assert_eq!(builder.len(), 0);

        builder.extend([1usize, 2, 3, 4, 5, 6]);
        assert_eq!(builder.len(), 6);
        assert_eq!(builder.num_blocks(), 3);
        assert_eq!(builder.current_block_len(), 0);
        assert_eq!(builder.last_offset(), 0);
        check_fixed_layout(&builder, 3);

        builder.extend([7usize]);
        assert_eq!(
            drain(&mut builder),
            vec![vec![1, 2, 3], vec![4, 5, 6], vec![7]]
        );
    }

    #[test]
    fn extends_length_from_offsets_rebases_the_offsets() {
        let mut builder = Fixed::new(4);
        builder.push_length(2);

        // offsets that do not start at zero, as produced by slicing an array
        builder.extends_length_from_offsets(&[5, 7, 10]);
        assert_eq!(all_lengths(&builder, 4), [2, 2, 3]);
        assert_eq!(builder.last_offset(), 7);

        // offsets that start below the current last offset
        builder.extends_length_from_offsets(&[0, 1]);
        assert_eq!(all_lengths(&builder, 4), [2, 2, 3, 1]);
        check_fixed_layout(&builder, 4);

        // spanning several blocks
        let more = OffsetBuffer::<i32>::from_lengths(model_lengths(10));
        builder.extends_length_from_offsets(&more);
        let mut expected = vec![2, 2, 3, 1];
        expected.extend(model_lengths(10));
        assert_eq!(all_lengths(&builder, 4), expected);
        check_fixed_layout(&builder, 4);
    }

    #[test]
    fn extends_length_from_offsets_in_indexes_uses_item_indexes() {
        let source = OffsetBuffer::<i32>::from_lengths([1, 2, 3, 4, 5]);

        let mut builder = Fixed::new(4);
        builder.extends_length_from_offsets_in_indexes(&source, &[4, 0, 2]);
        assert_eq!(all_lengths(&builder, 4), [5, 1, 3]);
        check_fixed_layout(&builder, 4);

        let mut builder = Fixed::new(2);
        builder.extends_length_from_offsets_in_indexes(&source, &[0, 1, 2, 3, 4]);
        assert_eq!(all_lengths(&builder, 2), [1, 2, 3, 4, 5]);
        check_fixed_layout(&builder, 2);
        assert_eq!(drain(&mut builder), vec![vec![1, 2], vec![3, 4], vec![5]]);
    }

    #[test]
    fn push_length_n_and_push_empty_n_span_blocks() {
        let mut builder = Fixed::new(3);
        builder.push_length_n(2, 0);
        builder.push_empty_n(0);
        assert_eq!(builder.len(), 0);

        builder.push_length_n(2, 7);
        assert_eq!(all_lengths(&builder, 3), vec![2; 7]);
        check_fixed_layout(&builder, 3);

        builder.push_empty_n(4);
        let mut expected = vec![2; 7];
        expected.extend([0; 4]);
        assert_eq!(all_lengths(&builder, 3), expected);
        check_fixed_layout(&builder, 3);

        builder.push_length_n(0, 1);
        expected.push(0);
        assert_eq!(all_lengths(&builder, 3), expected);
        assert_eq!(builder.num_blocks(), 5);
        check_fixed_layout(&builder, 3);
    }

    #[test]
    fn take_block_finished_returns_offset_buffer() {
        let mut builder = fixed_with(2, &[1, 2, 3]);
        let first = builder.take_block_finished().unwrap();
        assert_eq!(first.as_ref(), &[0, 1, 3]);
        let second = builder.take_block_finished().unwrap();
        assert_eq!(second.as_ref(), &[0, 3]);
        assert_eq!(builder.take_block_finished(), None);
    }

    #[test]
    fn take_block_then_push_continues_layout() {
        let mut builder = fixed_with(3, &[1, 2, 3, 4]);
        assert_eq!(
            builder.take_block().map(|b| b.to_vec()),
            Some(vec![0, 1, 3, 6])
        );
        assert_eq!(builder.len(), 1);
        assert_eq!(builder.last_offset(), 4);
        check_fixed_layout(&builder, 3);

        builder.push_length(5);
        builder.push_length(6);
        builder.push_length(7);
        check_fixed_layout(&builder, 3);
        assert_eq!(drain(&mut builder), vec![vec![4, 5, 6], vec![7]]);
    }

    #[test]
    fn take_all_returns_only_non_empty_blocks() {
        let mut builder = fixed_with(3, &[1, 2, 3, 4, 5, 6]);
        assert_eq!(builder.num_blocks(), 3);
        assert_eq!(
            builder.take_all(),
            vec![vec![0, 1, 3, 6], vec![0, 4, 9, 15]]
        );
        assert_eq!(builder.len(), 0);
        assert_eq!(builder.last_offset(), 0);
        check_fixed_layout(&builder, 3);

        builder.push_length(9);
        assert_eq!(builder.take_all(), vec![vec![0, 9]]);
    }

    #[test]
    fn into_iter_yields_non_empty_blocks() {
        let builder = fixed_with(3, &[1, 2, 3, 4, 5, 6]);
        let blocks: Vec<Vec<i32>> = builder.into_iter().map(|b| b.to_vec()).collect();
        assert_eq!(blocks, vec![vec![0, 1, 3, 6], vec![0, 4, 9, 15]]);

        assert_eq!(Fixed::new(3).into_iter().count(), 0);
    }

    #[test]
    fn take_n_zero_and_everything() {
        let mut builder = fixed_with(4, &[1, 2, 3]);
        assert_eq!(builder.take_n(0, None::<std::iter::Empty<usize>>), vec![0]);
        assert_eq!(all_lengths(&builder, 4), [1, 2, 3]);
        check_fixed_layout(&builder, 4);

        assert_eq!(
            builder.take_n(3, None::<std::iter::Empty<usize>>),
            vec![0, 1, 3, 6]
        );
        assert_eq!(builder.len(), 0);
        check_fixed_layout(&builder, 4);
        assert_eq!(builder.take_n(0, None::<std::iter::Empty<usize>>), vec![0]);
        assert_eq!(builder.take_block(), None);
    }

    #[test]
    #[should_panic(expected = "must be <= len")]
    fn take_n_more_than_len_panics() {
        let mut builder = fixed_with(4, &[1, 2]);
        builder.take_n(3, None::<std::iter::Empty<usize>>);
    }

    #[test]
    fn take_n_matches_model_and_stays_usable() {
        for block_size in 1..=5 {
            for total in 0..=(3 * block_size + 1) {
                for n in 0..=total.min(block_size) {
                    let mut model = model_lengths(total);
                    let mut builder = fixed_with(block_size, &model);

                    let taken = builder.take_n(n, None::<std::iter::Empty<usize>>);
                    let expected_taken: Vec<usize> = model.drain(..n).collect();
                    assert_eq!(taken[0], 0);
                    assert_eq!(
                        lengths(&taken),
                        expected_taken,
                        "taken mismatch bs={block_size} total={total} n={n}"
                    );
                    assert_eq!(
                        all_lengths(&builder, block_size),
                        model,
                        "remaining mismatch bs={block_size} total={total} n={n}"
                    );
                    check_fixed_layout(&builder, block_size);

                    let more: Vec<usize> = (0..=2 * block_size).map(|i| i % 3).collect();
                    for &len in &more {
                        builder.push_length(len);
                    }
                    model.extend_from_slice(&more);
                    assert_eq!(
                        all_lengths(&builder, block_size),
                        model,
                        "after push mismatch bs={block_size} total={total} n={n}"
                    );
                    check_fixed_layout(&builder, block_size);

                    let drained: Vec<usize> = drain(&mut builder).concat();
                    assert_eq!(
                        drained, model,
                        "drain mismatch bs={block_size} total={total} n={n}"
                    );
                    check_fixed_layout(&builder, block_size);
                }
            }
        }
    }

    #[test]
    fn allocated_size_follows_blocks() {
        // memory is returned page by page, so make a block span whole pages
        let page = unsafe { libc::sysconf(libc::_SC_PAGESIZE) } as usize;
        let block_size = page / size_of::<i32>() - 1;
        let mut builder = Fixed::new(block_size);
        let empty = builder.allocated_size();
        builder.push_length_n(1, 2 * block_size + 1);
        let full = builder.allocated_size();
        assert!(full > empty);
        builder.take_block();
        assert_eq!(builder.allocated_size(), full - page);
        check_fixed_layout(&builder, block_size);
    }

    // ---- manual block sizing ----

    fn manual_with_blocks(blocks: &[Vec<usize>]) -> Manual {
        let mut builder = Manual::new(0);
        for (i, block) in blocks.iter().enumerate() {
            if i > 0 {
                builder.start_new_block();
            }
            for &len in block {
                builder.push_length(len);
            }
        }
        builder
    }

    #[test]
    fn manual_push_never_finishes_block() {
        let mut builder = Manual::new(0);
        for len in 0..10 {
            assert!(!builder.push_length(len));
        }
        assert_eq!(builder.num_blocks(), 1);
        assert_eq!(builder.len(), 10);
        assert_eq!(builder.current_block_len(), 10);
        assert_eq!(builder.last_offset(), 45);
        assert_eq!(builder[BlocksIndex::new(0, 10)], 45);
    }

    #[test]
    fn manual_push_next_offset_in_block() {
        let mut builder = Manual::new(0);
        builder.push_next_offset_in_block(3);
        builder.push_next_offset_in_block(3);
        builder.push_next_offset_in_block(10);
        assert_eq!(builder.len(), 3);
        assert_eq!(builder.last_offset(), 10);
        assert_eq!(drain(&mut builder), vec![vec![3, 0, 7]]);
    }

    #[test]
    #[should_panic(expected = "monotonically increasing")]
    fn manual_push_decreasing_offset_panics() {
        let mut builder = Manual::new(0);
        builder.push_next_offset_in_block(3);
        builder.push_next_offset_in_block(2);
    }

    #[test]
    fn manual_start_new_block_and_take_block() {
        let mut builder = manual_with_blocks(&[vec![1, 2], vec![3], vec![4, 5, 6]]);
        assert_eq!(builder.num_blocks(), 3);
        assert_eq!(builder.len(), 6);
        assert_eq!(builder.last_offset(), 15);
        assert_eq!(builder[BlocksIndex::new(2, 3)], 15);

        assert_eq!(
            builder.take_block().map(|b| b.to_vec()),
            Some(vec![0, 1, 3])
        );
        assert_eq!(builder.len(), 4);
        assert_eq!(builder.current_block_index(), 1);
        // last offset is still the one of the block being written to
        assert_eq!(builder.last_offset(), 15);
        builder.push_length(7);
        assert_eq!(drain(&mut builder), vec![vec![3], vec![4, 5, 6, 7]]);
        assert_eq!(builder.last_offset(), 0);
        assert_eq!(builder.take_block(), None);
    }

    #[test]
    fn manual_take_all_drops_trailing_empty_block() {
        let mut builder = manual_with_blocks(&[vec![1, 2], vec![3]]);
        builder.start_new_block();
        assert_eq!(builder.take_all(), vec![vec![0, 1, 3], vec![0, 3]]);
        assert_eq!(builder.len(), 0);
        assert_eq!(builder.num_blocks(), 1);
    }

    #[test]
    fn manual_take_n_relayouts() {
        let blocks = || vec![vec![1, 2, 3, 4, 5], vec![6, 7, 8], vec![9, 10, 11, 12]];

        // shrink first block only
        let mut builder = manual_with_blocks(&blocks());
        let taken = builder.take_n(2, Some([3usize, 3, 4].into_iter()));
        assert_eq!(taken, vec![0, 1, 3]);
        assert_eq!(builder.len(), 10);
        assert_eq!(
            drain(&mut builder),
            vec![vec![3, 4, 5], vec![6, 7, 8], vec![9, 10, 11, 12]]
        );

        // merge everything
        let mut builder = manual_with_blocks(&blocks());
        let taken = builder.take_n(2, Some([10usize].into_iter()));
        assert_eq!(lengths(&taken), [1, 2]);
        assert_eq!(drain(&mut builder), vec![(3..=12).collect::<Vec<_>>()]);

        // split into pieces
        let mut builder = manual_with_blocks(&blocks());
        let taken = builder.take_n(1, Some([2usize, 3, 1, 5].into_iter()));
        assert_eq!(lengths(&taken), [1]);
        assert_eq!(
            drain(&mut builder),
            vec![vec![2, 3], vec![4, 5, 6], vec![7], vec![8, 9, 10, 11, 12]]
        );

        // whole first block
        let mut builder = manual_with_blocks(&blocks());
        let taken = builder.take_n(5, Some([3usize, 4].into_iter()));
        assert_eq!(lengths(&taken), [1, 2, 3, 4, 5]);
        assert_eq!(
            drain(&mut builder),
            vec![vec![6, 7, 8], vec![9, 10, 11, 12]]
        );

        // nothing
        let mut builder = manual_with_blocks(&blocks());
        let taken = builder.take_n(0, Some([5usize, 3, 4].into_iter()));
        assert_eq!(taken, vec![0]);
        assert_eq!(drain(&mut builder), blocks());

        // everything
        let mut builder = manual_with_blocks(&blocks()[..1]);
        let taken = builder.take_n(5, Some(std::iter::empty::<usize>()));
        assert_eq!(lengths(&taken), [1, 2, 3, 4, 5]);
        assert_eq!(builder.len(), 0);
        assert_eq!(builder.last_offset(), 0);
        assert_eq!(builder.take_block(), None);
        builder.push_length(1);
        assert_eq!(drain(&mut builder), vec![vec![1]]);
    }

    #[test]
    fn manual_take_n_then_push_continues_in_last_block() {
        let mut builder = manual_with_blocks(&[vec![1, 2, 3], vec![4, 5]]);
        builder.take_n(1, Some([2usize, 2].into_iter()));
        assert_eq!(builder.last_offset(), 9);
        builder.push_length(6);
        assert_eq!(builder.last_offset(), 15);
        assert_eq!(drain(&mut builder), vec![vec![2, 3], vec![4, 5, 6]]);
    }

    #[test]
    fn manual_take_n_matches_model() {
        for seed in 0..200usize {
            let sizes: Vec<usize> = (0..4).map(|i| 1 + (seed * (i + 3)) % 9).collect();
            let total: usize = sizes.iter().sum();
            let lens: Vec<usize> = (0..total).map(|i| (i * 7 + seed) % 5).collect();

            let mut offset = 0;
            let blocks: Vec<Vec<usize>> = sizes
                .iter()
                .map(|&s| {
                    let block = lens[offset..offset + s].to_vec();
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
                    let chunk = (1 + (seed + i * 5) % 6).min(left);
                    adjusted.push(chunk);
                    left -= chunk;
                    i += 1;
                }
            }

            let mut builder = manual_with_blocks(&blocks);
            let taken = builder.take_n(n, Some(adjusted.clone().into_iter()));
            assert_eq!(lengths(&taken), &lens[..n], "seed {seed}");
            assert_eq!(builder.len(), remaining, "seed {seed}");

            let drained = drain(&mut builder);
            let drained_sizes: Vec<usize> = drained.iter().map(Vec::len).collect();
            assert_eq!(drained_sizes, adjusted, "seed {seed}");
            assert_eq!(drained.concat(), &lens[n..], "seed {seed}");
        }
    }

    #[test]
    #[should_panic(expected = "must equal the length")]
    fn manual_take_n_wrong_adjusted_sizes_panics() {
        let mut builder = manual_with_blocks(&[vec![1, 2, 3], vec![4, 5]]);
        builder.take_n(1, Some([2usize, 1].into_iter()));
    }
}
