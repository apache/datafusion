use std::collections::VecDeque;
use std::ops::Range;
use itertools::Itertools;

/// A single block inside a blocked builder
///
/// Implemented per element type so the block re-layout logic can be shared
pub trait BlockBuilder: Sized {
  /// What a block turns into once it is emitted
  type Output;

  /// Must not allocate when `capacity` is 0
  fn with_capacity(capacity: usize) -> Self;

  fn len(&self) -> usize;

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

pub fn create_adjusted_block_size_iter_for_fixed_blocks(len: usize, n: usize, block_size: usize) -> impl Iterator<Item = usize> + Clone {
  assert!(n <= len, "n ({n}) must be <= len ({len}) than");
  let new_len = len - n;
  let should_have_remainder = new_len % block_size != 0;
  std::iter::repeat_n(block_size, new_len / block_size).chain(
    std::iter::repeat_n(new_len % block_size, should_have_remainder as usize),
  )
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
  mut adjusted_block_size_iter: impl Iterator<Item = usize>,
) -> (B::Output, BlocksLayout) {
  assert!(n <= len, "n ({n}) must be <= len ({len}) than");
  assert!(n <= blocks[0].len(), "n ({n}) must be lower than the first block ({}), instead use `take_block` and take_n with the remainder", blocks[0].len());

  // The trailing block may be an empty one waiting for the next push, it is not part of the layout
  if blocks.len() > 1 && blocks.back().is_some_and(|block| block.len() == 0) {
    blocks.pop_back();
  }

  let prev_len = len;

  if n == blocks[0].len() {
    adjusted_block_size_iter
      .zip_eq(blocks.iter().skip(1))
      .for_each(|(len, current_block)| assert_eq!(len, current_block.len()));

    let taken = blocks.pop_front().expect("must have block");

    ensure_writable_tail(blocks, block_size);

    let layout = layout_for(blocks, prev_len - n);

    return (taken.finish(), layout);
  }

  assert_ne!(
    n, len,
    "n must ne smaller than the first block which is smaller that len"
  );

  // Not moving anything
  if n == 0 {
    adjusted_block_size_iter.zip_eq(blocks.iter()).for_each(|(len, current_block)| {
      assert_eq!(
        len,
        current_block.len(),
        "when n is 0 and not equal the first block size, we should keep the length as is"
      )
    });

    ensure_writable_tail(blocks, block_size);

    let layout = layout_for(blocks, prev_len);

    return (B::with_capacity(0).finish(), layout);
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

  while let Some(new_block_size) = adjusted_block_size_iter.next() {
    sum += new_block_size;

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
        split.append_range(&blocks[src_index], src_offset..src_offset + new_block_size);

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

      blocks[dst_index].append_range(&placeholder, src_offset..src_offset + to_copy);

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

/// There must always be a block that the next push can go into,
/// with a fixed block size that means the back block must not be full
fn ensure_writable_tail<B: BlockBuilder>(blocks: &mut VecDeque<B>, block_size: Option<usize>) {
  let tail_is_full = block_size
    .is_some_and(|block_size| blocks.back().is_some_and(|block| block.len() == block_size));

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


