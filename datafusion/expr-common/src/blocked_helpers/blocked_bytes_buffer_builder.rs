use crate::blocked_helpers::take_n_helpers::take_n_from_blocks;
use arrow::array::OffsetSizeTrait;
use arrow::buffer::Buffer;
use datafusion_common::utils::proxy::{VecAllocExt, VecDequeAllocExt};
use std::collections::VecDeque;

#[derive(Debug)]
pub struct BlockedBytesBufferBuilder {
    /// Using `VecDeque` so we can remove the first block and reclaim memory
    blocks: VecDeque<Vec<u8>>,

    len: usize,

    finished_blocks_mem: usize,
}

impl Default for BlockedBytesBufferBuilder {
    fn default() -> Self {
        Self::new()
    }
}

impl BlockedBytesBufferBuilder {
    pub fn new() -> Self {
        let blocks = VecDeque::from(vec![vec![]]);
        BlockedBytesBufferBuilder {
            blocks,
            finished_blocks_mem: 0,
            len: 0,
        }
    }

    pub fn num_blocks(&self) -> usize {
        self.blocks.len()
    }

    pub fn len(&self) -> usize {
        self.len
    }

    pub fn allocated_size(&self) -> usize {
        self.finished_blocks_mem
            + self.blocks.allocated_size()
            + self.blocks.back().map_or(0, VecAllocExt::allocated_size)
    }

    pub fn current_block_len(&self) -> usize {
        self.blocks[self.blocks.len() - 1].len()
    }

    pub fn block(&self, block_index: usize) -> &Vec<u8> {
        &self.blocks[block_index]
    }

    pub fn is_empty(&self) -> bool {
        self.len == 0
    }

    pub fn reserve_bytes_in_current_block(&mut self, capacity: usize) {
        let block = self.blocks.back_mut().unwrap();

        // Not adding to finished blocks mem since it does not contain the last block
        // A failed reserve only means the next write allocates
        let _ = block.try_reserve(capacity);
    }

    pub fn reserve_blocks(&mut self, n: usize) {
        self.blocks.reserve(n);
    }

    pub fn start_new_block(&mut self) {
        self.finished_blocks_mem +=
            self.blocks.back().map_or(0, VecAllocExt::allocated_size);
        self.blocks.push_back(vec![]);
    }

    pub fn extend_from_slice(&mut self, slice: &[u8]) {
        let block = self.blocks.back_mut().unwrap();
        self.len += slice.len();

        block.extend_from_slice(slice);
    }

    /// Extend the bytes of the items at `indexes`
    pub fn extends_bytes_from_offsets_indexes_in_current_block<O: OffsetSizeTrait>(
        &mut self,
        bytes: &[u8],
        offset_buffer_slice: &[O],
        indexes: &[usize],
    ) {
        let block = self.blocks.back_mut().unwrap();

        for &index_to_copy in indexes {
            let from = offset_buffer_slice[index_to_copy].as_usize();
            let to = offset_buffer_slice[index_to_copy + 1].as_usize();

            block.extend_from_slice(&bytes[from..to]);
            self.len += to - from;
        }
    }

    /// Take the first block, `None` once there are no blocks with bytes left
    ///
    /// A block may legitimately be empty (all its items are empty or null) so unlike the
    /// other builders the block count, not the byte count, decides when we are done
    pub fn take_block(&mut self) -> Option<Vec<u8>> {
        if self.blocks.len() == 1 && self.blocks[0].is_empty() {
            return None;
        }

        Some(self.take_first_block())
    }

    /// Take the first block even when it is empty, for callers that know from
    /// elsewhere (the offsets) that the block holds items
    pub fn take_first_block(&mut self) -> Vec<u8> {
        let block = self.blocks.pop_front().expect("must have a block");
        self.len -= block.len();

        if self.blocks.is_empty() {
            // Add a new empty block for next emit
            self.blocks.push_back(vec![]);
        } else {
            // Only if not the last block since the current block is being calculated separately
            self.finished_blocks_mem -= VecAllocExt::allocated_size(&block);
        }

        block
    }

    /// Take every block, a trailing empty block is dropped
    pub fn take_all(&mut self) -> Vec<Vec<u8>> {
        let mut blocks = std::mem::take(&mut self.blocks);
        if blocks.len() > 1 && blocks.back().is_some_and(|b| b.is_empty()) {
            blocks.pop_back();
        }
        if blocks.len() == 1 && blocks[0].is_empty() {
            blocks.clear();
        }

        // TODO - should preallocate? can be expensive for large schema
        self.blocks.push_back(vec![]);
        self.finished_blocks_mem = 0;
        self.len = 0;

        blocks.into()
    }

    pub fn take_block_finished(&mut self) -> Option<Buffer> {
        let block = self.take_block()?;
        Some(Buffer::from(block))
    }

    pub fn take_n(
        &mut self,
        n: usize,
        adjusted_block_size_iter: impl Iterator<Item = usize> + Clone,
    ) -> Vec<u8> {
        let (taken, layout) = take_n_from_blocks(
            &mut self.blocks,
            self.len,
            n,
            None,
            adjusted_block_size_iter,
        );

        self.len = layout.len;
        self.finished_blocks_mem = layout.finished_blocks_allocated_size;

        taken
    }
}

impl<'a> Extend<&'a [u8]> for BlockedBytesBufferBuilder {
    fn extend<T: IntoIterator<Item = &'a [u8]>>(&mut self, iter: T) {
        let block = self.blocks.back_mut().unwrap();
        let before = block.len();
        for slice in iter {
            block.extend_from_slice(slice);
        }

        // The current block is measured separately so only the length changes
        self.len += block.len() - before;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn with_blocks(blocks: &[&[u8]]) -> BlockedBytesBufferBuilder {
        let mut builder = BlockedBytesBufferBuilder::new();
        for (i, block) in blocks.iter().enumerate() {
            if i > 0 {
                builder.start_new_block();
            }
            builder.extend_from_slice(block);
        }
        builder
    }

    fn drain(builder: &mut BlockedBytesBufferBuilder) -> Vec<Vec<u8>> {
        let mut out = vec![];
        while let Some(block) = builder.take_block() {
            out.push(block);
        }
        assert_eq!(builder.len(), 0);
        assert_eq!(builder.num_blocks(), 1);
        out
    }

    #[test]
    fn new_is_empty() {
        let mut builder = BlockedBytesBufferBuilder::new();
        assert_eq!(builder.len(), 0);
        assert_eq!(builder.num_blocks(), 1);
        assert_eq!(builder.current_block_len(), 0);
        assert_eq!(builder.take_block(), None);
        assert_eq!(builder.take_block_finished(), None);
        assert!(builder.take_all().is_empty());
    }

    #[test]
    fn extend_and_blocks() {
        let mut builder = with_blocks(&[b"abc", b"de", b"", b"f"]);
        assert_eq!(builder.len(), 6);
        assert_eq!(builder.num_blocks(), 4);
        assert_eq!(builder.block(1), b"de");
        assert_eq!(builder.block(2), b"");
        assert_eq!(builder.current_block_len(), 1);

        builder.extend([b"gh".as_slice(), b"", b"i"]);
        assert_eq!(builder.len(), 9);
        assert_eq!(builder.current_block_len(), 4);
        assert_eq!(builder.block(3), b"fghi");
    }

    #[test]
    fn take_block_keeps_empty_middle_blocks() {
        let mut builder = with_blocks(&[b"abc", b"de", b"", b"f"]);
        assert_eq!(builder.take_block(), Some(b"abc".to_vec()));
        assert_eq!(builder.len(), 3);
        assert_eq!(builder.num_blocks(), 3);
        assert_eq!(
            drain(&mut builder),
            vec![b"de".to_vec(), vec![], b"f".to_vec()]
        );

        // a trailing empty block waiting for the next write is not a block to take
        let mut builder = with_blocks(&[b"abc"]);
        builder.start_new_block();
        assert_eq!(drain(&mut builder), vec![b"abc".to_vec()]);

        // but it is still there to write into
        builder.extend_from_slice(b"x");
        assert_eq!(drain(&mut builder), vec![b"x".to_vec()]);
    }

    #[test]
    fn take_block_finished_returns_buffer() {
        let mut builder = with_blocks(&[b"abc", b"de"]);
        assert_eq!(builder.take_block_finished().unwrap().as_slice(), b"abc");
        assert_eq!(builder.take_block_finished().unwrap().as_slice(), b"de");
        assert_eq!(builder.take_block_finished(), None);
    }

    #[test]
    fn take_all_drops_trailing_empty_block() {
        let mut builder = with_blocks(&[b"abc", b"", b"de"]);
        builder.start_new_block();
        assert_eq!(
            builder.take_all(),
            vec![b"abc".to_vec(), vec![], b"de".to_vec()]
        );
        assert_eq!(builder.len(), 0);
        assert_eq!(builder.num_blocks(), 1);

        builder.extend_from_slice(b"z");
        assert_eq!(builder.take_all(), vec![b"z".to_vec()]);
    }

    #[test]
    fn extends_bytes_from_offsets_indexes_uses_item_indexes() {
        let bytes = b"aabbbc";
        let offsets = [0i32, 2, 5, 6];
        let mut builder = BlockedBytesBufferBuilder::new();
        builder.extends_bytes_from_offsets_indexes_in_current_block(
            bytes,
            &offsets,
            &[2, 0],
        );
        builder.extends_bytes_from_offsets_indexes_in_current_block(
            bytes,
            &offsets,
            &[1],
        );
        assert_eq!(builder.len(), 6);
        assert_eq!(drain(&mut builder), vec![b"caabbb".to_vec()]);
    }

    #[test]
    fn reserve_does_not_change_contents() {
        let mut builder = with_blocks(&[b"abc"]);
        builder.reserve_bytes_in_current_block(200);
        builder.reserve_blocks(3);
        assert_eq!(builder.len(), 3);
        assert!(builder.allocated_size() >= 200);
        assert_eq!(drain(&mut builder), vec![b"abc".to_vec()]);
    }

    #[test]
    fn allocated_size_follows_blocks() {
        let mut builder = BlockedBytesBufferBuilder::new();
        let empty = builder.allocated_size();
        builder.extend_from_slice(&[1; 100]);
        builder.start_new_block();
        builder.extend_from_slice(&[2; 100]);
        let full = builder.allocated_size();
        assert!(full >= empty + 200);
        builder.take_block();
        assert!(builder.allocated_size() < full);
        builder.take_all();
        assert!(builder.allocated_size() < 200);
    }

    #[test]
    fn take_n_relayouts() {
        let blocks: &[&[u8]] = &[b"abcde", b"fg", b"hij"];

        let mut builder = with_blocks(blocks);
        assert_eq!(builder.take_n(2, [3usize, 2, 3].into_iter()), b"ab");
        assert_eq!(builder.len(), 8);
        assert_eq!(
            drain(&mut builder),
            vec![b"cde".to_vec(), b"fg".to_vec(), b"hij".to_vec()]
        );

        let mut builder = with_blocks(blocks);
        assert_eq!(builder.take_n(1, std::iter::once(9usize)), b"a");
        assert_eq!(drain(&mut builder), vec![b"bcdefghij".to_vec()]);

        let mut builder = with_blocks(blocks);
        assert_eq!(builder.take_n(5, [2usize, 3].into_iter()), b"abcde");
        assert_eq!(drain(&mut builder), vec![b"fg".to_vec(), b"hij".to_vec()]);

        let mut builder = with_blocks(blocks);
        assert_eq!(builder.take_n(0, [5usize, 2, 3].into_iter()), b"");
        assert_eq!(
            drain(&mut builder),
            vec![b"abcde".to_vec(), b"fg".to_vec(), b"hij".to_vec()]
        );
    }

    #[test]
    fn take_n_with_empty_blocks_in_the_layout() {
        // taking no bytes can still move bytes between blocks, when the taken items were all empty
        let mut builder = with_blocks(&[b"abc", b"de"]);
        assert_eq!(builder.take_n(0, [0usize, 3, 2].into_iter()), b"");
        assert_eq!(
            drain(&mut builder),
            vec![vec![], b"abc".to_vec(), b"de".to_vec()]
        );

        // the whole first block is taken but the remaining layout still changes
        let mut builder = with_blocks(&[b"abc", b"de"]);
        assert_eq!(builder.take_n(3, [0usize, 0, 2].into_iter()), b"abc");
        assert_eq!(drain(&mut builder), vec![vec![], vec![], b"de".to_vec()]);

        // empty blocks in the middle of the new layout and a trailing one
        let mut builder = with_blocks(&[b"abcd", b"", b"ef"]);
        assert_eq!(builder.take_n(1, [2usize, 0, 3, 0].into_iter()), b"a");
        assert_eq!(builder.num_blocks(), 4);
        assert_eq!(
            drain(&mut builder),
            vec![b"bc".to_vec(), vec![], b"def".to_vec()]
        );

        // everything is taken and the new layout is only empty blocks
        let mut builder = with_blocks(&[b"ab"]);
        assert_eq!(builder.take_n(2, [0usize, 0].into_iter()), b"ab");
        assert_eq!(builder.num_blocks(), 2);
        assert_eq!(drain(&mut builder), vec![Vec::<u8>::new()]);
    }

    #[test]
    fn take_n_then_write_continues_in_last_block() {
        let mut builder = with_blocks(&[b"abc", b"de"]);
        builder.take_n(1, [2usize, 2].into_iter());
        builder.extend_from_slice(b"f");
        assert_eq!(builder.len(), 5);
        assert_eq!(drain(&mut builder), vec![b"bc".to_vec(), b"def".to_vec()]);
    }

    #[test]
    #[should_panic(expected = "must equal the length")]
    fn take_n_wrong_adjusted_sizes_panics() {
        let mut builder = with_blocks(&[b"abc", b"de"]);
        builder.take_n(1, [2usize, 1].into_iter());
    }
}
