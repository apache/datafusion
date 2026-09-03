use std::collections::VecDeque;
use std::ops::Range;

/// A single block inside a blocked builder
///
/// Implemented per element type so the block re-layout logic can be shared
pub trait BlockBuilder: Sized {
    /// What a block turns into once it is emitted
    type Output;

    /// Must not allocate when `capacity` is 0
    fn with_capacity(capacity: usize) -> Self;

    fn len(&self) -> usize;

    fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Drops everything from `len` onward, keeps the allocation
    fn truncate(&mut self, len: usize);

    /// Appends `src[range]` to the end of self
    fn append_range(&mut self, src: &Self, range: Range<usize>);

    /// Moves the items in `[offset, offset + len)` down to the start of the buffer
    /// and shrinks self to `len`, reusing the same allocation
    fn shift_down(&mut self, offset: usize, len: usize);

    fn allocated_size(&self) -> usize;

    fn finish(self) -> Self::Output;
}

/// The state a blocked builder has to write back after a re-layout
pub(crate) struct BlocksLayout {
    pub len: usize,
    pub current_block_index: usize,
    pub finished_blocks_allocated_size: usize,
}

pub fn create_adjusted_block_size_iter_for_fixed_blocks(
    len: usize,
    n: usize,
    block_size: usize,
) -> impl Iterator<Item = usize> + Clone {
    assert!(n <= len, "n ({n}) must be <= len ({len}) than");
    let new_len = len - n;
    let should_have_remainder = !new_len.is_multiple_of(block_size);
    std::iter::repeat_n(block_size, new_len / block_size).chain(std::iter::repeat_n(
        new_len % block_size,
        should_have_remainder as usize,
    ))
}

/// Takes the first `n` items out of `blocks` and re-blocks whatever is left
/// according to `adjusted_block_size_iter`
///
/// `min_last_block_capacity` is the capacity of the replacement block when the
/// layout ends up with nothing left, pass 0 when the builder has no block size hint
///
/// See `BlockedBooleanBuilder::take_n` for what the adjusted sizes mean
pub(crate) fn take_n_from_blocks<B: BlockBuilder>(
    blocks: &mut VecDeque<B>,
    len: usize,
    n: usize,
    block_size: Option<usize>,
    adjusted_block_size_iter: impl Iterator<Item = usize> + Clone,
) -> (B::Output, BlocksLayout) {
    assert!(n <= len, "n ({n}) must be <= len ({len}) than");
    assert!(
        n <= blocks[0].len(),
        "n ({n}) must be lower than the first block ({}), instead use `take_block` and take_n with the remainder",
        blocks[0].len()
    );

    let prev_len = len;

    // Fast paths when nothing moves, this is not always the case when `n` is 0 or a whole block,
    // for example the bytes of a block whose items are all empty can still move around

    // Not moving anything
    if n == 0
        && layout_unchanged(
            adjusted_block_size_iter.clone(),
            blocks.iter().map(|b| b.len()),
        )
    {
        ensure_writable_tail(blocks, block_size);

        let layout = layout_for(blocks, prev_len);

        return (B::with_capacity(0).finish(), layout);
    }

    // Only the first block goes
    if n == blocks[0].len()
        && layout_unchanged(
            adjusted_block_size_iter.clone(),
            blocks.iter().skip(1).map(|b| b.len()),
        )
    {
        let taken = blocks.pop_front().expect("must have block");

        ensure_writable_tail(blocks, block_size);

        let layout = layout_for(blocks, prev_len - n);

        return (taken.finish(), layout);
    }

    // The emitted items are always fully contained in the first block
    let mut taken = B::with_capacity(n);
    taken.append_range(&blocks[0], 0..n);

    // Read cursor into the old layout, starts right after the emitted items
    let mut src_index = 0;
    let mut src_offset = n;

    // Write cursor into the new layout
    let mut dst_index = 0;

    let mut sum = 0;

    // Reused for swapping blocks out of the deque, a 0 capacity block holds no buffer
    let mut placeholder = B::with_capacity(0);

    for new_block_size in adjusted_block_size_iter {
        sum += new_block_size;

        if new_block_size == 0 {
            // An empty destination block, nothing has to be read for it
            // (the bytes of a block whose values are all empty or null for example)
            if dst_index < src_index {
                blocks[dst_index].truncate(0);
            } else {
                blocks.insert(dst_index, B::with_capacity(0));
                src_index += 1;
            }
            dst_index += 1;
            continue;
        }

        // Skip over source blocks that were fully read
        while src_index < blocks.len() && src_offset >= blocks[src_index].len() {
            src_index += 1;
            src_offset = 0;
        }

        assert!(
            src_index < blocks.len(),
            "sum of adjusted block sizes + n ({n}) is larger than the length ({prev_len})"
        );

        // Invariant, the destination never runs ahead of the read cursor
        // so writing into `dst_index` can never clobber items that were not read yet
        debug_assert!(dst_index <= src_index);

        if dst_index == src_index {
            let remaining_in_src = blocks[src_index].len() - src_offset;

            if new_block_size < remaining_in_src {
                // The old block is being split, its tail is still needed by later
                // destinations so it cannot be shifted down in place
                // Give the split off part its own slot and push the old block one to the right
                let mut split = B::with_capacity(new_block_size);
                split.append_range(
                    &blocks[src_index],
                    src_offset..src_offset + new_block_size,
                );

                blocks.insert(dst_index, split);

                src_index += 1;
                src_offset += new_block_size;
                dst_index += 1;
                continue;
            }

            // The whole tail of this block belongs to the destination, shift it down
            // over the items that were consumed and reuse the same allocation
            blocks[dst_index].shift_down(src_offset, remaining_in_src);

            src_index += 1;
            src_offset = 0;

            if remaining_in_src == new_block_size {
                dst_index += 1;
                continue;
            }
        } else {
            // This slot held a block that is already fully read, reuse it as an empty destination
            blocks[dst_index].truncate(0);
        }

        let mut remaining = new_block_size - blocks[dst_index].len();

        while remaining > 0 {
            while src_index < blocks.len() && src_offset >= blocks[src_index].len() {
                src_index += 1;
                src_offset = 0;
            }

            assert!(
                src_index < blocks.len(),
                "sum of adjusted block sizes + n ({n}) is larger than the length ({prev_len}), missing {remaining} items"
            );

            // Move the source block aside so the destination can be borrowed mutably
            std::mem::swap(&mut blocks[src_index], &mut placeholder);

            let to_copy = (placeholder.len() - src_offset).min(remaining);

            blocks[dst_index]
                .append_range(&placeholder, src_offset..src_offset + to_copy);

            std::mem::swap(&mut blocks[src_index], &mut placeholder);

            src_offset += to_copy;
            remaining -= to_copy;
        }

        dst_index += 1;
    }

    assert_eq!(
        prev_len,
        sum + n,
        "sum of adjusted block sizes ({sum}) + n ({n}) must equal the length {prev_len}"
    );

    // Drop the old blocks that the new layout did not need
    blocks.truncate(dst_index);

    ensure_writable_tail(blocks, block_size);

    let layout = layout_for(blocks, sum);

    (taken.finish(), layout)
}

/// Whether the adjusted block sizes describe exactly the current block sizes
///
/// This has to be exact, a builder whose blocks may legitimately be empty (bytes) can not
/// tell a trailing empty block apart from a block of empty values
pub(crate) fn layout_unchanged(
    adjusted_block_size_iter: impl Iterator<Item = usize>,
    current_block_sizes: impl Iterator<Item = usize>,
) -> bool {
    adjusted_block_size_iter.eq(current_block_sizes)
}

/// Like [`layout_unchanged`] but a trailing empty block, the one waiting for the next push,
/// does not count. Only valid for builders whose blocks are never empty otherwise
pub(crate) fn layout_unchanged_ignoring_trailing_empty(
    adjusted_block_size_iter: impl Iterator<Item = usize>,
    current_block_sizes: impl Iterator<Item = usize>,
) -> bool {
    let mut expected: Vec<usize> = adjusted_block_size_iter.collect();
    let mut actual: Vec<usize> = current_block_sizes.collect();
    while expected.last() == Some(&0) {
        expected.pop();
    }
    while actual.last() == Some(&0) {
        actual.pop();
    }
    expected == actual
}

/// There must always be a block that the next push can go into,
/// with a fixed block size that means the back block must not be full
fn ensure_writable_tail<B: BlockBuilder>(
    blocks: &mut VecDeque<B>,
    block_size: Option<usize>,
) {
    let tail_is_full = block_size.is_some_and(|block_size| {
        blocks.back().is_some_and(|block| block.len() == block_size)
    });

    if blocks.is_empty() || tail_is_full {
        blocks.push_back(B::with_capacity(block_size.unwrap_or(0)));
    }
}

/// The back block is the one still being written to and is measured separately
fn layout_for<B: BlockBuilder>(blocks: &VecDeque<B>, len: usize) -> BlocksLayout {
    let finished_blocks_count = blocks.len() - 1;

    BlocksLayout {
        len,
        current_block_index: finished_blocks_count,
        finished_blocks_allocated_size: blocks
            .iter()
            .take(finished_blocks_count)
            .map(|block| block.allocated_size())
            .sum(),
    }
}
