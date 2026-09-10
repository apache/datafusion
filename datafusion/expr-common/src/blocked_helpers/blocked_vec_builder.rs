use super::blocked_custom_input_builder::{
    Block, BlockProvider, BlockProviderFinish, BlockWithSlice, BlockedCustomInputBuilder,
};
use crate::blocked_helpers::take_n_helpers::BlockBuilder;
use crate::groups_accumulator::BlocksIndex;
use arrow::buffer::{Buffer, ScalarBuffer};
use arrow::datatypes::ArrowNativeType;
use std::fmt::Debug;
use std::marker::PhantomData;
use std::ops::{Deref, DerefMut, Index, IndexMut, Range};

/// A block taken out of a [`CopyItemBlockedVecBuilder`], exclusively owned like a `Vec`
/// so it can be mutated in place, and convertible into an arrow buffer without copying.
pub struct MmapVec<T: Copy> {
    data: Vec<T>,
}

impl<T: Copy> MmapVec<T> {
    pub fn as_slice(&self) -> &[T] {
        &self.data
    }

    pub fn as_mut_slice(&mut self) -> &mut [T] {
        &mut self.data
    }

    #[inline]
    pub fn len(&self) -> usize {
        self.data.len()
    }

    #[inline]
    pub fn is_empty(&self) -> bool {
        self.data.is_empty()
    }

    /// Wrap into an arrow buffer without copying
    pub fn into_buffer(self) -> Buffer
    where
        T: ArrowNativeType,
    {
        Buffer::from_vec(self.data)
    }

    pub fn into_scalar_buffer(self) -> ScalarBuffer<T>
    where
        T: ArrowNativeType,
    {
        ScalarBuffer::from(self.data)
    }
}

impl<T: Copy> Deref for MmapVec<T> {
    type Target = [T];

    #[inline]
    fn deref(&self) -> &[T] {
        &self.data
    }
}

impl<T: Copy> DerefMut for MmapVec<T> {
    #[inline]
    fn deref_mut(&mut self) -> &mut [T] {
        &mut self.data
    }
}

impl<T: Copy> AsRef<[T]> for MmapVec<T> {
    fn as_ref(&self) -> &[T] {
        self
    }
}

impl<T: Copy> Index<usize> for MmapVec<T> {
    type Output = T;

    #[inline]
    fn index(&self, index: usize) -> &T {
        &self.data[index]
    }
}

impl<T: Copy> IndexMut<usize> for MmapVec<T> {
    #[inline]
    fn index_mut(&mut self, index: usize) -> &mut T {
        &mut self.data[index]
    }
}

impl<T: ArrowNativeType> From<MmapVec<T>> for ScalarBuffer<T> {
    fn from(block: MmapVec<T>) -> Self {
        block.into_scalar_buffer()
    }
}

impl<T: ArrowNativeType> From<MmapVec<T>> for Buffer {
    fn from(block: MmapVec<T>) -> Self {
        block.into_buffer()
    }
}

impl<T: Copy + Debug> Debug for MmapVec<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        Debug::fmt(self.as_slice(), f)
    }
}

impl<T: Copy + PartialEq, U: AsRef<[T]>> PartialEq<U> for MmapVec<T> {
    fn eq(&self, other: &U) -> bool {
        self.as_slice() == other.as_ref()
    }
}

impl<T: Copy> Block for MmapVec<T> {
    type Item = T;

    fn allocated_size(&self) -> usize {
        size_of::<T>() * self.data.capacity()
    }

    #[inline]
    fn push(&mut self, item: T) {
        self.data.push(item)
    }

    fn extend(&mut self, iter: impl Iterator<Item = T>) {
        self.data.extend(iter)
    }

    fn len(&self) -> usize {
        self.data.len()
    }

    fn is_empty(&self) -> bool {
        self.data.is_empty()
    }
}

impl<T: Copy> BlockWithSlice for MmapVec<T> {
    fn copy_from_slice(&mut self, slice: &[T]) {
        self.data.extend_from_slice(slice)
    }

    fn append_n(&mut self, item: T, n: usize) {
        self.data.resize(self.data.len() + n, item)
    }
}

impl<T: Copy> BlockBuilder for MmapVec<T> {
    type Output = MmapVec<T>;

    fn with_capacity(capacity: usize) -> Self {
        MmapVec {
            data: Vec::with_capacity(capacity),
        }
    }

    fn len(&self) -> usize {
        self.data.len()
    }

    fn truncate(&mut self, len: usize) {
        self.data.truncate(len)
    }

    fn append_range(&mut self, src: &Self, range: Range<usize>) {
        self.data.extend_from_slice(&src.data[range])
    }

    fn shift_down(&mut self, offset: usize, len: usize) {
        if offset > 0 {
            self.data.copy_within(offset..offset + len, 0);
        }
        self.data.truncate(len)
    }

    fn allocated_size(&self) -> usize {
        size_of::<T>() * self.data.capacity()
    }

    fn finish(self) -> MmapVec<T> {
        self
    }
}

#[derive(Debug)]
pub struct VecBlockProvider<T>(PhantomData<T>);

impl<T> Default for VecBlockProvider<T> {
    fn default() -> Self {
        Self(PhantomData)
    }
}

impl<T: Copy> BlockProvider for VecBlockProvider<T> {
    type Block = MmapVec<T>;

    fn new_block(&self) -> MmapVec<T> {
        MmapVec { data: Vec::new() }
    }

    fn allocated_size(&self) -> usize {
        0
    }
}

impl<T: ArrowNativeType> BlockProviderFinish for VecBlockProvider<T> {
    type FinishedBlock = ScalarBuffer<T>;

    fn finish(&self, block: MmapVec<T>) -> ScalarBuffer<T> {
        block.into_scalar_buffer()
    }
}

/// Blocks are separate `Vec`s in a `VecDeque`, so a block is handed out by moving it and
/// items are addressed by `(block, index in block)`
#[derive(Debug)]
pub struct CopyItemBlockedVecBuilder<const FIXED_BLOCK_SIZING: bool, T: Copy>(
    BlockedCustomInputBuilder<FIXED_BLOCK_SIZING, VecBlockProvider<T>>,
);

impl<const FIXED_BLOCK_SIZING: bool, T: Copy> Deref
    for CopyItemBlockedVecBuilder<FIXED_BLOCK_SIZING, T>
{
    type Target = BlockedCustomInputBuilder<FIXED_BLOCK_SIZING, VecBlockProvider<T>>;

    #[inline]
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl<const FIXED_BLOCK_SIZING: bool, T: Copy> DerefMut
    for CopyItemBlockedVecBuilder<FIXED_BLOCK_SIZING, T>
{
    #[inline]
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}

impl<const FIXED_BLOCK_SIZING: bool, T: Copy>
    CopyItemBlockedVecBuilder<FIXED_BLOCK_SIZING, T>
{
    pub fn new(block_size: usize) -> Self {
        assert!(size_of::<T>() > 0, "zero sized items are not supported");
        Self(BlockedCustomInputBuilder::new(
            block_size,
            VecBlockProvider::default(),
        ))
    }

    /// Kept for API compatibility with the mmap implementation, blocks grow on demand
    pub fn with_reservation(block_size: usize, _reserved_bytes: usize) -> Self {
        Self::new(block_size)
    }

    /// Make `n` more items live, they read as all zero bytes
    ///
    /// # Safety
    /// `T` must be valid when all of its bytes are zero
    pub unsafe fn advance_untouched(&mut self, n: usize) {
        let zero: T = unsafe { std::mem::zeroed() };
        self.0.push_value_n(zero, n);
    }

    /// Make room for `extra` more items in the current block
    #[inline]
    pub fn reserve(&mut self, extra: usize) {
        let block_size = self.0.block_size();
        let block = self.0.current_block_mut();
        let extra = if FIXED_BLOCK_SIZING {
            extra.min(block_size - block.len())
        } else {
            extra
        };
        block.data.reserve(extra);
    }

    /// `(block, index in block)` of `index`. Fixed sizing addresses blocks directly.
    /// Manual sizing callers address items flat, counting from the block they name (the
    /// mmap implementation kept items contiguous so this was a plain offset), which
    /// here walks the blocks
    /// TODO: O(blocks) walk, manual sizing is the rare path and its blocks are few
    #[inline]
    fn locate(&self, index: BlocksIndex) -> (usize, usize) {
        let block_size = self.0.block_size();
        let mut block = index.block_index(block_size);
        let mut offset = index.index_in_block(block_size);
        if FIXED_BLOCK_SIZING {
            return (block, offset);
        }
        let last = self.0.num_blocks() - 1;
        while block < last {
            let len = self.0.block(block).len();
            if offset < len {
                break;
            }
            offset -= len;
            block += 1;
        }
        (block, offset)
    }

    pub fn reserve_blocks(&mut self, n: usize) {
        self.0.reserve_blocks(n);
    }

    /// Every item, block by block
    pub fn iter_mut(&mut self) -> impl Iterator<Item = &mut T> {
        self.0.blocks_mut().flat_map(|block| block.data.iter_mut())
    }

    /// Item at `index` without bounds checking
    ///
    /// # Safety
    /// `index` must point at an existing item
    #[inline]
    pub unsafe fn get_unchecked(&self, index: BlocksIndex) -> &T {
        let (block, offset) = self.locate(index);
        unsafe { self.0.block_unchecked(block).data.get_unchecked(offset) }
    }

    /// Mutable item at `index` without bounds checking
    ///
    /// # Safety
    /// `index` must point at an existing item
    #[inline]
    pub unsafe fn get_unchecked_mut(&mut self, index: BlocksIndex) -> &mut T {
        let (block, offset) = self.locate(index);
        unsafe { self.0.block_unchecked_mut(block).data.get_unchecked_mut(offset) }
    }
}

impl<const FIXED_BLOCK_SIZING: bool, T: Copy> Index<BlocksIndex>
    for CopyItemBlockedVecBuilder<FIXED_BLOCK_SIZING, T>
{
    type Output = T;

    #[inline]
    fn index(&self, index: BlocksIndex) -> &T {
        let (block, offset) = self.locate(index);
        &self.0.block(block).data[offset]
    }
}

impl<const FIXED_BLOCK_SIZING: bool, T: Copy> IndexMut<BlocksIndex>
    for CopyItemBlockedVecBuilder<FIXED_BLOCK_SIZING, T>
{
    #[inline]
    fn index_mut(&mut self, index: BlocksIndex) -> &mut T {
        let (block, offset) = self.locate(index);
        &mut self.0.current_or_block_mut(block).data[offset]
    }
}

impl<T: Copy> Index<usize> for CopyItemBlockedVecBuilder<true, T> {
    type Output = T;

    #[inline]
    fn index(&self, index: usize) -> &T {
        &self[BlocksIndex::from_index_in_fixed_block_size(index, self.0.block_size())]
    }
}

impl<T: Copy> IndexMut<usize> for CopyItemBlockedVecBuilder<true, T> {
    #[inline]
    fn index_mut(&mut self, index: usize) -> &mut T {
        let index = BlocksIndex::from_index_in_fixed_block_size(index, self.0.block_size());
        &mut self[index]
    }
}

/// Still used by the builders that keep `Vec` blocks (e.g. the bytes buffer builder)
impl<T: Copy> BlockBuilder for Vec<T> {
    type Output = Vec<T>;

    fn with_capacity(capacity: usize) -> Self {
        Vec::with_capacity(capacity)
    }

    fn len(&self) -> usize {
        self.as_slice().len()
    }

    fn truncate(&mut self, len: usize) {
        Vec::truncate(self, len)
    }

    fn append_range(&mut self, src: &Self, range: Range<usize>) {
        self.extend_from_slice(&src[range])
    }

    fn shift_down(&mut self, offset: usize, len: usize) {
        if offset > 0 {
            self.copy_within(offset..offset + len, 0);
        }

        Vec::truncate(self, len)
    }

    fn allocated_size(&self) -> usize {
        size_of::<T>() * self.capacity()
    }

    fn finish(self) -> Vec<T> {
        self
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::groups_accumulator::BlocksIndex;

    type Fixed = CopyItemBlockedVecBuilder<true, i32>;
    type Manual = CopyItemBlockedVecBuilder<false, i32>;

    fn to_vec(builder: &Fixed) -> Vec<i32> {
        (0..builder.num_blocks()).flat_map(|i| builder.block(i).to_vec()).collect()
    }

    /// A fixed builder always keeps a trailing block with room for the next push
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

    fn fixed_with(block_size: usize, values: &[i32]) -> Fixed {
        let mut builder = Fixed::new(block_size);
        builder.extend_from_slice(values);
        check_fixed_layout(&builder);
        builder
    }

    fn values(range: Range<usize>) -> Vec<i32> {
        range.map(|v| v as i32).collect()
    }

    #[test]
    fn new_is_empty() {
        let builder = Fixed::new(4);
        assert!(builder.is_empty());
        assert_eq!(builder.len(), 0);
        assert_eq!(builder.num_blocks(), 1);
        assert_eq!(builder.block_size(), 4);
        assert_eq!(builder.current_block_len(), 0);
        check_fixed_layout(&builder);
    }

    #[test]
    #[should_panic(expected = "block size must be greater than 0")]
    fn fixed_zero_block_size_panics() {
        Fixed::new(0);
    }

    #[test]
    fn push_reports_when_block_fills() {
        let mut builder = Fixed::new(3);
        assert!(!builder.push(1));
        assert!(!builder.push(2));
        assert_eq!(builder.current_block_len(), 2);
        assert!(builder.push(3));
        assert_eq!(builder.len(), 3);
        assert_eq!(builder.num_blocks(), 2);
        assert_eq!(builder.current_block_len(), 0);
        assert!(!builder.push(4));
        assert_eq!(to_vec(&builder), [1, 2, 3, 4]);
        check_fixed_layout(&builder);
    }

    #[test]
    fn push_with_block_size_one() {
        let mut builder = Fixed::new(1);
        for v in 0..5 {
            assert!(builder.push(v));
        }
        assert_eq!(builder.num_blocks(), 6);
        assert_eq!(to_vec(&builder), values(0..5));
        check_fixed_layout(&builder);
    }

    #[test]
    fn push_value_n_spans_blocks() {
        let mut builder = Fixed::new(4);
        builder.push_value_n(7, 0);
        assert!(builder.is_empty());
        check_fixed_layout(&builder);

        builder.push_value_n(7, 10);
        assert_eq!(builder.len(), 10);
        assert_eq!(builder.num_blocks(), 3);
        assert_eq!(to_vec(&builder), vec![7; 10]);
        check_fixed_layout(&builder);

        // fill exactly to a block boundary
        builder.push_value_n(8, 2);
        assert_eq!(builder.len(), 12);
        assert_eq!(builder.num_blocks(), 4);
        assert_eq!(builder.current_block_len(), 0);
        check_fixed_layout(&builder);
    }

    #[test]
    fn push_default_n() {
        let mut builder = Fixed::new(3);
        builder.push(5);
        builder.push_default_n(5);
        assert_eq!(to_vec(&builder), [5, 0, 0, 0, 0, 0]);
        check_fixed_layout(&builder);
    }

    #[test]
    fn extend_spans_blocks() {
        let mut builder = Fixed::new(4);
        builder.extend(values(0..0));
        assert!(builder.is_empty());
        check_fixed_layout(&builder);

        builder.extend(values(0..3));
        builder.extend(values(3..11));
        assert_eq!(to_vec(&builder), values(0..11));
        check_fixed_layout(&builder);

        // exactly to the boundary
        builder.extend(values(11..12));
        assert_eq!(builder.num_blocks(), 4);
        assert_eq!(builder.current_block_len(), 0);
        check_fixed_layout(&builder);
    }

    #[test]
    fn extend_from_slice_spans_blocks() {
        let mut builder = Fixed::new(4);
        builder.extend_from_slice(&[]);
        assert!(builder.is_empty());

        builder.extend_from_slice(&values(0..3));
        builder.extend_from_slice(&values(3..13));
        assert_eq!(to_vec(&builder), values(0..13));
        assert_eq!(builder.num_blocks(), 4);
        check_fixed_layout(&builder);
    }

    #[test]
    fn index_by_usize_and_blocks_index() {
        let mut builder = fixed_with(3, &values(0..8));

        for i in 0..8 {
            assert_eq!(builder[i], i as i32);
            let blocks_index = BlocksIndex::from_index_in_fixed_block_size(i, 3);
            assert_eq!(builder[blocks_index], i as i32);
        }
        assert_eq!(builder[BlocksIndex::new(1, 2)], 5);
        assert_eq!(builder[BlocksIndex::new(2, 1)], 7);

        builder[4] = 40;
        builder[BlocksIndex::new(2, 0)] = 60;
        assert_eq!(to_vec(&builder), [0, 1, 2, 3, 40, 5, 60, 7]);
    }

    #[test]
    fn take_block_returns_none_when_empty() {
        let mut builder = Fixed::new(4);
        assert!(builder.take_block().is_none());
        assert!(builder.take_block_finished().is_none());
        check_fixed_layout(&builder);

        // still usable after
        builder.push(1);
        assert_eq!(to_vec(&builder), [1]);
    }

    #[test]
    fn take_block_shifts_remaining() {
        let mut builder = fixed_with(3, &values(0..8));

        assert_eq!(builder.take_block().map(|b| b.to_vec()), Some(values(0..3)));
        assert_eq!(builder.len(), 5);
        assert_eq!(to_vec(&builder), values(3..8));
        assert_eq!(builder[0], 3);
        assert_eq!(builder[BlocksIndex::new(1, 1)], 7);
        check_fixed_layout(&builder);

        assert_eq!(builder.take_block().map(|b| b.to_vec()), Some(values(3..6)));
        assert_eq!(to_vec(&builder), values(6..8));
        check_fixed_layout(&builder);

        // partial last block
        assert_eq!(builder.take_block().map(|b| b.to_vec()), Some(values(6..8)));
        assert!(builder.is_empty());
        check_fixed_layout(&builder);

        assert_eq!(builder.take_block(), None);
    }

    #[test]
    fn take_block_when_len_is_multiple_of_block_size() {
        let mut builder = fixed_with(3, &values(0..6));
        assert_eq!(builder.num_blocks(), 3);

        assert_eq!(builder.take_block().map(|b| b.to_vec()), Some(values(0..3)));
        check_fixed_layout(&builder);
        assert_eq!(builder.take_block().map(|b| b.to_vec()), Some(values(3..6)));
        check_fixed_layout(&builder);
        assert_eq!(builder.take_block(), None);
        check_fixed_layout(&builder);
    }

    #[test]
    fn push_after_take_block_continues_layout() {
        let mut builder = fixed_with(3, &values(0..7));
        builder.take_block();

        builder.extend_from_slice(&values(7..12));
        assert_eq!(to_vec(&builder), values(3..12));
        check_fixed_layout(&builder);

        assert_eq!(builder.take_block().map(|b| b.to_vec()), Some(values(3..6)));
        assert_eq!(builder.take_block().map(|b| b.to_vec()), Some(values(6..9)));
        assert_eq!(
            builder.take_block().map(|b| b.to_vec()),
            Some(values(9..12))
        );
        assert_eq!(builder.take_block(), None);
        check_fixed_layout(&builder);
    }

    #[test]
    fn take_block_finished_returns_scalar_buffer() {
        let mut builder = fixed_with(2, &values(0..3));
        let finished = builder.take_block_finished().unwrap();
        assert_eq!(finished.as_ref(), &[0, 1]);
        let finished = builder.take_block_finished().unwrap();
        assert_eq!(finished.as_ref(), &[2]);
        assert!(builder.take_block_finished().is_none());
    }

    #[test]
    fn take_all_returns_only_non_empty_blocks() {
        let mut builder = fixed_with(3, &values(0..7));
        let blocks = builder.take_all();
        assert_eq!(blocks, vec![values(0..3), values(3..6), values(6..7)]);
        assert!(builder.is_empty());
        check_fixed_layout(&builder);

        // exact multiple, the trailing empty block is not returned
        let mut builder = fixed_with(3, &values(0..6));
        assert_eq!(builder.take_all(), vec![values(0..3), values(3..6)]);
        assert!(builder.is_empty());
        check_fixed_layout(&builder);

        let mut builder = Fixed::new(3);
        assert!(builder.take_all().is_empty());
        check_fixed_layout(&builder);
    }

    #[test]
    fn usable_after_take_all() {
        let mut builder = fixed_with(3, &values(0..7));
        builder.take_all();

        builder.extend_from_slice(&values(0..4));
        assert_eq!(to_vec(&builder), values(0..4));
        check_fixed_layout(&builder);
        assert_eq!(builder.take_block().map(|b| b.to_vec()), Some(values(0..3)));
        assert_eq!(builder.take_block().map(|b| b.to_vec()), Some(values(3..4)));
        assert_eq!(builder.take_block(), None);
    }

    #[test]
    fn reset_clears_everything() {
        let mut builder = fixed_with(3, &values(0..7));
        builder.reset();
        assert!(builder.is_empty());
        assert_eq!(builder.num_blocks(), 1);
        check_fixed_layout(&builder);
        assert!(builder.take_block().is_none());

        builder.push(1);
        assert_eq!(to_vec(&builder), [1]);
    }

    #[test]
    fn take_n_zero_is_noop() {
        let mut builder = fixed_with(3, &values(0..7));
        let taken = builder.take_n(0, None::<std::iter::Empty<usize>>);
        assert!(taken.is_empty());
        assert_eq!(to_vec(&builder), values(0..7));
        check_fixed_layout(&builder);

        let mut builder = Fixed::new(3);
        let taken = builder.take_n(0, None::<std::iter::Empty<usize>>);
        assert!(taken.is_empty());
        assert!(builder.is_empty());
        check_fixed_layout(&builder);
    }

    #[test]
    fn take_n_everything_when_less_than_block() {
        let mut builder = fixed_with(5, &values(0..3));
        let taken = builder.take_n(3, None::<std::iter::Empty<usize>>);
        assert_eq!(taken, values(0..3));
        assert!(builder.is_empty());
        check_fixed_layout(&builder);
        assert!(builder.take_block().is_none());
    }

    #[test]
    fn take_n_relayouts_blocks() {
        let mut builder = fixed_with(4, &values(0..10));
        let taken = builder.take_n(3, None::<std::iter::Empty<usize>>);
        assert_eq!(taken, values(0..3));
        assert_eq!(builder.len(), 7);
        assert_eq!(to_vec(&builder), values(3..10));
        check_fixed_layout(&builder);

        // blocks are now [3,4,5,6] [7,8,9]
        assert_eq!(builder[BlocksIndex::new(0, 3)], 6);
        assert_eq!(builder[BlocksIndex::new(1, 0)], 7);
        assert_eq!(builder.take_block().map(|b| b.to_vec()), Some(values(3..7)));
        assert_eq!(
            builder.take_block().map(|b| b.to_vec()),
            Some(values(7..10))
        );
        assert_eq!(builder.take_block(), None);
    }

    #[test]
    fn take_n_full_block_equals_take_block() {
        let mut builder = fixed_with(4, &values(0..10));
        let taken = builder.take_n(4, None::<std::iter::Empty<usize>>);
        assert_eq!(taken, values(0..4));
        assert_eq!(to_vec(&builder), values(4..10));
        check_fixed_layout(&builder);
    }

    #[test]
    #[should_panic(expected = "must be <= len")]
    fn take_n_more_than_len_panics() {
        let mut builder = fixed_with(4, &values(0..2));
        builder.take_n(3, None::<std::iter::Empty<usize>>);
    }

    #[test]
    fn take_n_matches_model_and_stays_usable() {
        for block_size in 1..=6 {
            for total in 0..=(3 * block_size + 1) {
                for n in 0..=total.min(block_size) {
                    let mut builder = fixed_with(block_size, &values(0..total));
                    let mut model = values(0..total);

                    let taken = builder.take_n(n, None::<std::iter::Empty<usize>>);
                    let expected_taken: Vec<i32> = model.drain(..n).collect();
                    assert_eq!(
                        taken, expected_taken,
                        "taken mismatch bs={block_size} total={total} n={n}"
                    );
                    assert_eq!(
                        to_vec(&builder),
                        model,
                        "remaining mismatch bs={block_size} total={total} n={n}"
                    );
                    check_fixed_layout(&builder);

                    // keep using it after the re-layout
                    let more = values(1000..(1000 + 2 * block_size + 1));
                    builder.extend_from_slice(&more);
                    model.extend_from_slice(&more);
                    assert_eq!(
                        to_vec(&builder),
                        model,
                        "after push mismatch bs={block_size} total={total} n={n}"
                    );
                    check_fixed_layout(&builder);

                    // and drain block by block
                    let mut drained = vec![];
                    while let Some(block) = builder.take_block() {
                        assert!(block.len() <= block_size);
                        drained.extend_from_slice(&block);
                    }
                    assert_eq!(
                        drained, model,
                        "drain mismatch bs={block_size} total={total} n={n}"
                    );
                    assert!(builder.is_empty());
                    check_fixed_layout(&builder);
                }
            }
        }
    }

    #[test]
    fn repeated_take_n_and_push() {
        let block_size = 5;
        let mut builder = Fixed::new(block_size);
        let mut model: Vec<i32> = vec![];
        let mut next = 0;

        for step in 0..50 {
            let to_push = (step * 7) % 11;
            let pushed = values(next..next + to_push);
            next += to_push;
            builder.extend_from_slice(&pushed);
            model.extend_from_slice(&pushed);

            let n = ((step * 3) % block_size).min(model.len());
            let taken = builder.take_n(n, None::<std::iter::Empty<usize>>);
            let expected: Vec<i32> = model.drain(..n).collect();
            assert_eq!(taken, expected, "step {step}");
            assert_eq!(to_vec(&builder), model, "step {step}");
            check_fixed_layout(&builder);

            if step % 4 == 0 {
                let block = builder.take_block();
                let expected_len = model.len().min(block_size);
                let expected: Vec<i32> = model.drain(..expected_len).collect();
                assert_eq!(
                    block.map(|b| b.to_vec()).unwrap_or_default(),
                    expected,
                    "step {step}"
                );
                assert_eq!(to_vec(&builder), model, "step {step}");
                check_fixed_layout(&builder);
            }
        }
    }

    #[test]
    fn allocated_size_follows_blocks() {
        let block_size = 1024;
        let mut builder = Fixed::new(block_size);
        let empty = builder.allocated_size();

        builder.extend_from_slice(&values(0..2 * block_size + 1));
        let full = builder.allocated_size();
        assert!(full >= empty + (2 * block_size + 1) * size_of::<i32>());

        // a taken block leaves with its memory
        builder.take_block();
        let after_take = builder.allocated_size();
        assert!(after_take <= full - block_size * size_of::<i32>());

        builder.take_all();
        assert!(builder.is_empty());
        assert!(builder.allocated_size() < after_take);
    }

    #[test]
    fn relocates_past_the_initial_mapping() {
        const RESERVED: usize = 1 << 20;
        let mut builder = Fixed::with_reservation(1024, RESERVED);
        let total = RESERVED / size_of::<i32>() + 5000;
        let mut expected = vec![];
        let mut next = 0;
        while builder.len() + expected.len() < total {
            // drain a block now and then so the live window stays small
            if next % 7 == 0 {
                let block = builder.take_block().map(|b| b.to_vec()).unwrap_or_default();
                assert_eq!(block, expected.drain(..block.len()).collect::<Vec<_>>());
            }
            let chunk = values(next..next + 3000);
            builder.extend_from_slice(&chunk);
            expected.extend_from_slice(&chunk);
            next += 3000;
            builder.push(-1);
            expected.push(-1);
            next += 1;
        }
        assert_eq!(to_vec(&builder), expected);
        check_fixed_layout(&builder);

        // grow while everything stays live
        let mut builder = Fixed::with_reservation(1024, RESERVED);
        let big = values(0..RESERVED / size_of::<i32>() + 3);
        builder.extend_from_slice(&big);
        assert_eq!(to_vec(&builder), big);
        assert_eq!(builder[BlocksIndex::new(3, 7)], big[3 * 1024 + 7]);
        check_fixed_layout(&builder);
    }

    // ---- manual block sizing ----

    /// Items of a manual builder that only has a single block
    fn manual_to_vec(builder: &Manual) -> Vec<i32> {
        assert_eq!(builder.num_blocks(), 1);
        (0..builder.len())
            .map(|i| builder[BlocksIndex::new(0, i)])
            .collect()
    }

    #[test]
    fn manual_push_never_finishes_block() {
        let mut builder = Manual::new(0);
        for v in 0..10 {
            assert!(!builder.push(v));
        }
        assert_eq!(builder.len(), 10);
        assert_eq!(builder.num_blocks(), 1);
        assert_eq!(builder.current_block_len(), 10);
        assert_eq!(builder[BlocksIndex::new(0, 9)], 9);
    }

    #[test]
    fn manual_extend_goes_into_current_block() {
        let mut builder = Manual::new(0);
        builder.extend(values(0..5));
        builder.extend_from_slice(&values(5..8));
        builder.push_value_n(9, 2);
        assert_eq!(builder.num_blocks(), 1);
        assert_eq!(builder.len(), 10);
        assert_eq!(manual_to_vec(&builder), [0, 1, 2, 3, 4, 5, 6, 7, 9, 9]);
    }

    #[test]
    fn manual_start_new_block_and_take_block() {
        let mut builder = Manual::new(0);
        builder.extend(values(0..5));
        builder.start_new_block();
        builder.extend(values(5..7));
        builder.start_new_block();
        builder.extend(values(7..11));
        assert_eq!(builder.num_blocks(), 3);
        assert_eq!(builder.len(), 11);
        assert_eq!(builder.current_block_len(), 4);
        assert_eq!(builder[BlocksIndex::new(1, 1)], 6);
        assert_eq!(builder[BlocksIndex::new(2, 3)], 10);

        assert_eq!(builder.take_block().map(|b| b.to_vec()), Some(values(0..5)));
        assert_eq!(builder.len(), 6);
        assert_eq!(builder[BlocksIndex::new(0, 1)], 6);
        assert_eq!(builder.take_block().map(|b| b.to_vec()), Some(values(5..7)));
        assert_eq!(
            builder.take_block().map(|b| b.to_vec()),
            Some(values(7..11))
        );
        assert!(builder.is_empty());
        assert_eq!(builder.take_block(), None);
        assert_eq!(builder.num_blocks(), 1);

        builder.push(1);
        assert_eq!(builder[BlocksIndex::new(0, 0)], 1);
    }

    #[test]
    fn manual_take_all() {
        let mut builder = Manual::new(0);
        builder.extend(values(0..5));
        builder.start_new_block();
        builder.extend(values(5..7));
        assert_eq!(builder.take_all(), vec![values(0..5), values(5..7)]);
        assert!(builder.is_empty());
        assert_eq!(builder.num_blocks(), 1);

        // trailing empty block from start_new_block is dropped
        let mut builder = Manual::new(0);
        builder.extend(values(0..5));
        builder.start_new_block();
        assert_eq!(builder.take_all(), vec![values(0..5)]);
        assert!(builder.is_empty());
    }

    fn manual_with_blocks(blocks: &[Vec<i32>]) -> Manual {
        let mut builder = Manual::new(0);
        for (i, block) in blocks.iter().enumerate() {
            if i > 0 {
                builder.start_new_block();
            }
            builder.extend_from_slice(block);
        }
        builder
    }

    fn drain_manual(builder: &mut Manual) -> Vec<Vec<i32>> {
        let mut out = vec![];
        while let Some(block) = builder.take_block() {
            out.push(block.to_vec());
        }
        out
    }

    #[test]
    fn manual_take_n_shrinks_first_block() {
        let mut builder =
            manual_with_blocks(&[values(0..5), values(5..8), values(8..12)]);
        let taken = builder.take_n(2, Some([3usize, 3, 4].into_iter()));
        assert_eq!(taken, values(0..2));
        assert_eq!(builder.len(), 10);
        assert_eq!(
            drain_manual(&mut builder),
            vec![values(2..5), values(5..8), values(8..12)]
        );
    }

    #[test]
    fn manual_take_n_merges_blocks() {
        let mut builder =
            manual_with_blocks(&[values(0..5), values(5..8), values(8..12)]);
        let taken = builder.take_n(2, Some([10usize].into_iter()));
        assert_eq!(taken, values(0..2));
        assert_eq!(drain_manual(&mut builder), vec![values(2..12)]);

        let mut builder =
            manual_with_blocks(&[values(0..5), values(5..8), values(8..12)]);
        let taken = builder.take_n(2, Some([4usize, 6].into_iter()));
        assert_eq!(taken, values(0..2));
        assert_eq!(
            drain_manual(&mut builder),
            vec![values(2..6), values(6..12)]
        );
    }

    #[test]
    fn manual_take_n_splits_blocks() {
        let mut builder =
            manual_with_blocks(&[values(0..5), values(5..8), values(8..12)]);
        let taken = builder.take_n(2, Some(std::iter::repeat_n(1usize, 10)));
        assert_eq!(taken, values(0..2));
        let expected: Vec<Vec<i32>> = (2..12).map(|v| vec![v]).collect();
        assert_eq!(drain_manual(&mut builder), expected);

        let mut builder =
            manual_with_blocks(&[values(0..5), values(5..8), values(8..12)]);
        let taken = builder.take_n(1, Some([2usize, 3, 1, 5].into_iter()));
        assert_eq!(taken, values(0..1));
        assert_eq!(
            drain_manual(&mut builder),
            vec![values(1..3), values(3..6), values(6..7), values(7..12)]
        );
    }

    #[test]
    fn manual_take_n_whole_first_block() {
        let mut builder =
            manual_with_blocks(&[values(0..5), values(5..8), values(8..12)]);
        let taken = builder.take_n(5, Some([3usize, 4].into_iter()));
        assert_eq!(taken, values(0..5));
        assert_eq!(builder.len(), 7);
        assert_eq!(
            drain_manual(&mut builder),
            vec![values(5..8), values(8..12)]
        );
    }

    #[test]
    fn manual_take_n_zero_keeps_layout() {
        let mut builder = manual_with_blocks(&[values(0..5), values(5..8)]);
        let taken = builder.take_n(0, Some([5usize, 3].into_iter()));
        assert!(taken.is_empty());
        assert_eq!(builder.len(), 8);
        assert_eq!(drain_manual(&mut builder), vec![values(0..5), values(5..8)]);
    }

    #[test]
    fn manual_take_n_everything() {
        let mut builder = manual_with_blocks(&[values(0..5)]);
        let taken = builder.take_n(5, Some(std::iter::empty::<usize>()));
        assert_eq!(taken, values(0..5));
        assert!(builder.is_empty());
        assert_eq!(builder.num_blocks(), 1);
        assert_eq!(builder.take_block(), None);

        builder.push(1);
        assert_eq!(builder[BlocksIndex::new(0, 0)], 1);
    }

    #[test]
    fn manual_take_n_then_push_continues_in_last_block() {
        let mut builder = manual_with_blocks(&[values(0..5), values(5..8)]);
        builder.take_n(2, Some([3usize, 3].into_iter()));
        builder.push(100);
        assert_eq!(builder.len(), 7);
        assert_eq!(builder.current_block_len(), 4);
        assert_eq!(
            drain_manual(&mut builder),
            vec![values(2..5), vec![5, 6, 7, 100]]
        );
    }

    #[test]
    #[should_panic(expected = "must equal the length")]
    fn manual_take_n_wrong_adjusted_sizes_panics() {
        let mut builder = manual_with_blocks(&[values(0..5), values(5..8)]);
        builder.take_n(2, Some([3usize, 2].into_iter()));
    }
}
