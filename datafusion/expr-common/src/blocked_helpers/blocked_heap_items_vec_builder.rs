use crate::blocked_helpers::blocked_custom_heap_allocated_input_builder::{BlockedCustomHeapAllocatedInputBuilder, HeapAllocatedBlock, HeapAllocatedBlockIterable, HeapAllocatedBlockProvider, HeapAllocatedBlockProviderFinish, HeapAllocatedBlockWithSlice};
use crate::blocked_helpers::take_n_helpers_heap_allocated::HeapAllocatedBlockBuilderProvider;
use crate::blocked_helpers::{GetHeapAllocatedSize, OnlyOnStackSize};
use arrow::buffer::ScalarBuffer;
use arrow::datatypes::ArrowNativeType;
use std::marker::PhantomData;
use std::ops::{Deref, DerefMut, Range};

#[derive(Debug)]
pub struct BlockedVecBuilder<
    const FIXED_BLOCK_SIZING: bool,
    T: Clone,
    HeapAllocatedSize: GetHeapAllocatedSize<T> = OnlyOnStackSize,
>(
    BlockedCustomHeapAllocatedInputBuilder<
        FIXED_BLOCK_SIZING,
        HeapVecBlockProvider<T>,
        HeapAllocatedSize,
    >,
);

impl<const FIXED_BLOCK_SIZING: bool, T: Clone, HeapAllocatedSize: GetHeapAllocatedSize<T>>
    BlockedVecBuilder<FIXED_BLOCK_SIZING, T, HeapAllocatedSize>
{
    pub fn new(block_size: usize) -> Self {
        BlockedVecBuilder(BlockedCustomHeapAllocatedInputBuilder::new(
            block_size,
            HeapVecBlockProvider::<T>::default(),
        ))
    }
}

impl<const FIXED_BLOCK_SIZING: bool, T: Clone, HeapAllocatedSize: GetHeapAllocatedSize<T>>
    Deref for BlockedVecBuilder<FIXED_BLOCK_SIZING, T, HeapAllocatedSize>
{
    type Target = BlockedCustomHeapAllocatedInputBuilder<
        FIXED_BLOCK_SIZING,
        HeapVecBlockProvider<T>,
        HeapAllocatedSize,
    >;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl<const FIXED_BLOCK_SIZING: bool, T: Clone, HeapAllocatedSize: GetHeapAllocatedSize<T>>
    DerefMut for BlockedVecBuilder<FIXED_BLOCK_SIZING, T, HeapAllocatedSize>
{
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}

#[derive(Debug)]
pub struct HeapVecBlockProvider<T>(PhantomData<T>);

impl<T> Default for HeapVecBlockProvider<T> {
    fn default() -> Self {
        Self(PhantomData)
    }
}

impl<T: Clone> HeapAllocatedBlockProvider for HeapVecBlockProvider<T> {
    type Block = Vec<T>;

    fn new_block(&self) -> Self::Block {
        vec![]
    }

    fn allocated_size(&self) -> usize {
        0
    }
}

impl<T: ArrowNativeType> HeapAllocatedBlockProviderFinish for HeapVecBlockProvider<T> {
    type FinishedBlock = ScalarBuffer<T>;

    fn finish(&self, block: Self::Block) -> Self::FinishedBlock {
        ScalarBuffer::from(block)
    }
}

impl<T: Clone> HeapAllocatedBlock for Vec<T> {
    type Item = T;
    const ALLOCATED_SIZE_INCLUDE_ITEMS: bool = false;

    fn allocated_size(&self) -> usize {
        size_of::<T>() * self.capacity()
    }

    fn push(&mut self, item: Self::Item) {
        Self::push(self, item)
    }

    fn extend(&mut self, iter: impl Iterator<Item = Self::Item>) {
        Extend::extend(self, iter)
    }

    fn len(&self) -> usize {
        Vec::len(self)
    }

    fn is_empty(&self) -> bool {
        Vec::is_empty(self)
    }
}

impl<T: Clone> HeapAllocatedBlockWithSlice for Vec<T> {
    fn extend_from_slice(&mut self, slice: &[Self::Item]) {
        Vec::extend_from_slice(self, slice)
    }

    fn append_n(&mut self, item: Self::Item, n: usize) {
        self.resize(self.len() + n, item)
    }
}

impl<T: Clone> HeapAllocatedBlockIterable for Vec<T> {
    type Iter<'a> = std::slice::Iter<'a, T> where T: 'a;
    type IterMut<'a> = std::slice::IterMut<'a, T> where T: 'a;

    fn iter(&self) -> Self::Iter<'_> {
        self.as_slice().iter()
    }

    fn iter_mut(&mut self) -> Self::IterMut<'_> {
        self.as_mut_slice().iter_mut()
    }
}

impl<T: Clone> HeapAllocatedBlockBuilderProvider for HeapVecBlockProvider<T> {
    type Output = Vec<T>;

    fn with_capacity(&self, capacity: usize) -> Self::Block {
        Vec::with_capacity(capacity)
    }

    fn len(&self, block: &Self::Block) -> usize {
        block.len()
    }

    fn truncate(&self, block: &mut Self::Block, len: usize) {
        block.truncate(len)
    }

    fn append_range(&self, dest: &mut Self::Block, src: &Self::Block, range: Range<usize>) {
        dest.extend_from_slice(&src[range])
    }

    fn calculate_memory_of_range<
        HeapAllocatedSize: GetHeapAllocatedSize<<Self::Block as HeapAllocatedBlock>::Item>,
    >(
        &self,
        block: &Self::Block,
        range: Range<usize>,
    ) -> usize {
        block[range]
            .iter()
            .map(|item| HeapAllocatedSize::get_heap_allocated_size(item))
            .sum()
    }

    fn shift_down(&self, block: &mut Self::Block, offset: usize, len: usize) {
        if offset > 0 {
            // truncate first so drain does not memmove elements we are about to drop
            block.truncate(offset + len);
            block.drain(..offset);
        }

        block.truncate(len);
    }

    fn block_allocated_size(&self, block: &Self::Block) -> usize {
        size_of::<T>() * block.capacity()
    }

    fn finish(&self, block: Self::Block) -> Vec<T> {
        block
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::groups_accumulator::BlocksIndex;

    type Fixed = BlockedVecBuilder<true, i32>;
    type Manual = BlockedVecBuilder<false, i32>;

    fn to_vec(builder: &Fixed) -> Vec<i32> {
        (0..builder.len()).map(|i| builder[i]).collect()
    }

    /// A fixed builder counts the minimum number of blocks that hold its items
    fn check_fixed_layout(builder: &Fixed) {
        let block_size = builder.block_size();
        assert_eq!(
            builder.num_blocks(),
            builder.len().div_ceil(block_size),
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
        assert_eq!(builder.num_blocks(), 0);
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
        assert_eq!(builder.num_blocks(), 1);
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
        assert_eq!(builder.num_blocks(), 5);
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
        assert_eq!(builder.num_blocks(), 3);
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
        assert_eq!(builder.num_blocks(), 3);
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

        assert_eq!(builder.take_block(), Some(values(0..3)));
        assert_eq!(builder.len(), 5);
        assert_eq!(to_vec(&builder), values(3..8));
        assert_eq!(builder[0], 3);
        assert_eq!(builder[BlocksIndex::new(1, 1)], 7);
        check_fixed_layout(&builder);

        assert_eq!(builder.take_block(), Some(values(3..6)));
        assert_eq!(to_vec(&builder), values(6..8));
        check_fixed_layout(&builder);

        // partial last block
        assert_eq!(builder.take_block(), Some(values(6..8)));
        assert!(builder.is_empty());
        check_fixed_layout(&builder);

        assert_eq!(builder.take_block(), None);
    }

    #[test]
    fn take_block_when_len_is_multiple_of_block_size() {
        let mut builder = fixed_with(3, &values(0..6));
        assert_eq!(builder.num_blocks(), 2);

        assert_eq!(builder.take_block(), Some(values(0..3)));
        check_fixed_layout(&builder);
        assert_eq!(builder.take_block(), Some(values(3..6)));
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

        assert_eq!(builder.take_block(), Some(values(3..6)));
        assert_eq!(builder.take_block(), Some(values(6..9)));
        assert_eq!(builder.take_block(), Some(values(9..12)));
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
    fn usable_after_take_all() {
        let mut builder = fixed_with(3, &values(0..7));
        builder.take_all();

        builder.extend_from_slice(&values(0..4));
        assert_eq!(to_vec(&builder), values(0..4));
        check_fixed_layout(&builder);
        assert_eq!(builder.take_block(), Some(values(0..3)));
        assert_eq!(builder.take_block(), Some(values(3..4)));
        assert_eq!(builder.take_block(), None);
    }

    #[test]
    fn reset_clears_everything() {
        let mut builder = fixed_with(3, &values(0..7));
        builder.reset();
        assert!(builder.is_empty());
        assert_eq!(builder.num_blocks(), 0);
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
        assert_eq!(builder.take_block(), Some(values(3..7)));
        assert_eq!(builder.take_block(), Some(values(7..10)));
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
                assert_eq!(block.unwrap_or_default(), expected, "step {step}");
                assert_eq!(to_vec(&builder), model, "step {step}");
                check_fixed_layout(&builder);
            }
        }
    }

    #[test]
    fn growing_an_item_in_a_finished_block_keeps_memory_in_sync() {
        use crate::blocked_helpers::get_heap_allocated_size::CommonHeapAllocatorSize;

        let mut builder =
            BlockedVecBuilder::<true, Vec<i32>, CommonHeapAllocatorSize>::new(2);
        builder.push_default_n(5);
        let before = builder.allocated_size();

        // group 1 is in the first block, which is already finished
        builder.index_mut_with_size(BlocksIndex::new(0, 1), |item| {
            Extend::extend(item, 0..1000)
        });
        assert!(builder.allocated_size() >= before + 1000 * size_of::<i32>());

        // and shrink one in the current block
        builder.index_mut_with_size(BlocksIndex::new(2, 0), |item| {
            Extend::extend(item, 0..10);
            *item = vec![];
        });

        let first = builder.take_block().unwrap();
        assert_eq!(first[1].len(), 1000);
        assert!(builder.take_block().is_some());
        assert!(builder.take_block().is_some());
        assert!(builder.take_block().is_none());
        // only the deque capacities remain
        assert!(builder.allocated_size() < before);
    }

    #[test]
    fn allocated_size_follows_blocks() {
        let mut builder = Fixed::new(4);
        let empty = builder.allocated_size();

        builder.extend_from_slice(&values(0..10));
        let full = builder.allocated_size();
        assert!(full >= empty + 10 * size_of::<i32>());

        builder.take_block();
        let after_take = builder.allocated_size();
        assert!(after_take < full);

        builder.take_all();
        assert!(builder.allocated_size() < after_take);
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

        assert_eq!(builder.take_block(), Some(values(0..5)));
        assert_eq!(builder.len(), 6);
        assert_eq!(builder[BlocksIndex::new(0, 1)], 6);
        assert_eq!(builder.take_block(), Some(values(5..7)));
        assert_eq!(builder.take_block(), Some(values(7..11)));
        assert!(builder.is_empty());
        assert_eq!(builder.take_block(), None);
        assert_eq!(builder.num_blocks(), 0);

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
        assert_eq!(builder.num_blocks(), 0);
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
            out.push(block);
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
        assert_eq!(builder.num_blocks(), 0);
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

    /// Builds a manual builder from `steps`: a digit pushes that many items, `E` ends the
    /// current block and `S` starts a new one
    fn manual_from_steps(steps: &str) -> Manual {
        let mut builder = Manual::new(0);
        for step in steps.chars() {
            match step {
                'E' => builder.end_current_block(),
                'S' => builder.start_new_block(),
                digit => {
                    for _ in 0..digit.to_digit(10).expect("digit") {
                        builder.push(0);
                    }
                }
            }
        }
        builder
    }

    /// Builds a fixed builder with block size 3 holding `len` items
    fn fixed_of_len(len: usize) -> Fixed {
        let mut builder = Fixed::new(3);
        for i in 0..len {
            builder.push(i as i32);
        }
        builder
    }

    /// (steps, blocks that count): an ended block counts even while empty and leaves a
    /// pre-opened one that does not count until it is used, started or ended; a started block
    /// counts right away, and a block that was only pre-opened becomes the started one
    const MANUAL_CASES: &[(&str, usize)] = &[
        ("", 0),
        ("S", 1),
        ("SS", 2),
        ("E", 1),
        ("EE", 2),
        ("2", 1),
        ("2E", 1),
        ("2EE", 2),
        ("2E3", 2),
        ("2E3E", 2),
        ("2S", 2),
        ("2S3", 2),
        ("2SS", 3),
        ("S2E", 1),
        ("2EE3", 3),
        ("E2", 2),
    ];

    #[test]
    fn fixed_num_blocks_matches_take_all_and_take_block() {
        for len in 0..=10 {
            let blocks = assert_blocks_consistent!(|| fixed_of_len(len));
            assert_eq!(blocks.len(), len.div_ceil(3), "len {len}");
            assert_eq!(blocks.iter().map(|b| b.len()).sum::<usize>(), len);
        }
    }

    #[test]
    fn fixed_take_n_leaving_no_open_block_or_a_partial_one() {
        // 7 items in blocks of 3: taking 1 leaves 6, an exact multiple with nothing open,
        // taking 2 leaves 5 with a partial last block, taking 3 a whole block
        for (take, expected_blocks) in [(0, 3), (1, 2), (2, 2), (3, 2)] {
            let blocks = assert_blocks_consistent!(|| {
                let mut builder = fixed_of_len(7);
                builder.take_n(take, None::<std::iter::Empty<usize>>);
                builder
            });
            assert_eq!(blocks.len(), expected_blocks, "take {take}");
        }
        // everything, block by block
        let blocks = assert_blocks_consistent!(|| {
            let mut builder = fixed_of_len(6);
            builder.take_n(3, None::<std::iter::Empty<usize>>);
            builder.take_n(3, None::<std::iter::Empty<usize>>);
            builder
        });
        assert!(blocks.is_empty());
        // usable again after everything was taken
        let blocks = assert_blocks_consistent!(|| {
            let mut builder = fixed_of_len(6);
            builder.take_all();
            for _ in 0..4 {
                builder.push(0);
            }
            builder
        });
        assert_eq!(blocks.len(), 2);
    }

    #[test]
    fn fixed_take_block_then_the_rest() {
        let blocks = assert_blocks_consistent!(|| {
            let mut builder = fixed_of_len(7);
            builder.take_block();
            builder
        });
        assert_eq!(blocks.len(), 2);
        let blocks = assert_blocks_consistent!(|| {
            let mut builder = fixed_of_len(6);
            builder.take_block();
            builder
        });
        assert_eq!(blocks.len(), 1);
    }

    #[test]
    fn manual_num_blocks_matches_take_all_and_take_block() {
        for (steps, expected) in MANUAL_CASES {
            let blocks = assert_blocks_consistent!(|| manual_from_steps(steps));
            assert_eq!(blocks.len(), *expected, "steps {steps:?}");
        }
    }

    #[test]
    fn manual_take_n_with_and_without_empty_blocks() {
        // blocks of 2 and 3 items, take 1 and re-block the remaining 4
        for (sizes, expected) in [
            (vec![1, 3], 2),
            (vec![1, 3, 0], 3),
            (vec![0, 4], 2),
            (vec![4], 1),
            (vec![0, 0, 4], 3),
            (vec![2, 2, 0, 0], 4),
        ] {
            let blocks = assert_blocks_consistent!(|| {
                let mut builder = manual_from_steps("2E3");
                builder.take_n(1, Some(sizes.clone().into_iter()));
                builder
            });
            assert_eq!(blocks.len(), expected, "sizes {sizes:?}");
        }
        // everything, without and with a requested empty block
        let blocks = assert_blocks_consistent!(|| {
            let mut builder = manual_from_steps("2");
            builder.take_n(2, Some(std::iter::empty()));
            builder
        });
        assert!(blocks.is_empty());
        let blocks = assert_blocks_consistent!(|| {
            let mut builder = manual_from_steps("2");
            builder.take_n(2, Some(std::iter::once(0)));
            builder
        });
        assert_eq!(blocks.len(), 1);
        // an empty block in the middle survives a take that does not touch it
        let blocks = assert_blocks_consistent!(|| {
            let mut builder = manual_from_steps("2EE3");
            builder.take_n(2, Some([0usize, 3].into_iter()));
            builder
        });
        assert_eq!(blocks.len(), 2);
    }

    /// Nine items with block size 3: fixed sizing ends a block by itself when it fills up,
    /// manual sizing ends it with `end_current_block`, both look the same from outside
    #[test]
    fn fixed_and_manual_block_ends_match() {
        let mut fixed = Fixed::new(3);
        let mut manual = Manual::new(0);
        for i in 0..9 {
            fixed.push(i as i32);
            manual.push(i as i32);
            if (i + 1) % 3 == 0 {
                manual.end_current_block();
            }
        }
        assert_eq!(fixed.len(), 9);
        assert_eq!(manual.len(), 9);
        assert_eq!(fixed.num_blocks(), 3);
        assert_eq!(manual.num_blocks(), 3);
        let fixed_blocks = fixed.take_all();
        let manual_blocks = manual.take_all();
        assert_eq!(fixed_blocks.len(), 3);
        assert_eq!(manual_blocks.len(), 3);
        assert_eq!(fixed_blocks, manual_blocks);
        assert_eq!(fixed.num_blocks(), 0);
        assert_eq!(manual.num_blocks(), 0);
    }
}
