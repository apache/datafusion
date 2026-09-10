use crate::blocked_helpers::CopyItemBlockedVecBuilder;
use arrow::array::OffsetSizeTrait;
use arrow::buffer::Buffer;

/// Bytes of every block live contiguously in one mmap'ed region, blocks are only a
/// layout over it, see [`CopyItemBlockedVecBuilder`]
#[derive(Debug)]
pub struct BlockedBytesBufferBuilder {
    bytes: CopyItemBlockedVecBuilder<false, u8>,
}

impl Default for BlockedBytesBufferBuilder {
    fn default() -> Self {
        Self::new()
    }
}

impl BlockedBytesBufferBuilder {
    pub fn new() -> Self {
        BlockedBytesBufferBuilder {
            bytes: CopyItemBlockedVecBuilder::new(0),
        }
    }

    pub fn num_blocks(&self) -> usize {
        self.bytes.num_blocks()
    }

    pub fn len(&self) -> usize {
        self.bytes.len()
    }

    pub fn allocated_size(&self) -> usize {
        self.bytes.allocated_size()
    }

    pub fn current_block_len(&self) -> usize {
        self.bytes.current_block_len()
    }

    pub fn block(&self, block_index: usize) -> &[u8] {
        self.bytes.block(block_index)
    }

    pub fn is_empty(&self) -> bool {
        self.bytes.is_empty()
    }

    pub fn reserve_bytes_in_current_block(&mut self, capacity: usize) {
        self.bytes.reserve(capacity);
    }

    pub fn reserve_blocks(&mut self, n: usize) {
        self.bytes.reserve_blocks(n);
    }

    pub fn start_new_block(&mut self) {
        self.bytes.start_new_block();
    }

    pub fn extend_from_slice(&mut self, slice: &[u8]) {
        self.bytes.extend_from_slice(slice);
    }

    /// Extend the bytes of the items at `indexes`
    pub fn extends_bytes_from_offsets_indexes_in_current_block<O: OffsetSizeTrait>(
        &mut self,
        bytes: &[u8],
        offset_buffer_slice: &[O],
        indexes: &[usize],
    ) {
        for &index_to_copy in indexes {
            let from = offset_buffer_slice[index_to_copy].as_usize();
            let to = offset_buffer_slice[index_to_copy + 1].as_usize();
            self.bytes.extend_from_slice(&bytes[from..to]);
        }
    }

    /// Take the first block, `None` once there are no blocks with bytes left
    ///
    /// A block may legitimately be empty (all its items are empty or null) so unlike the
    /// other builders the block count, not the byte count, decides when we are done
    pub fn take_block(&mut self) -> Option<Buffer> {
        if self.num_blocks() == 1 && self.is_empty() {
            return None;
        }

        Some(self.take_first_block())
    }

    /// Take the first block even when it is empty, for callers that know from
    /// elsewhere (the offsets) that the block holds items
    pub fn take_first_block(&mut self) -> Buffer {
        self.bytes.take_first_block().into_buffer()
    }

    /// Take every block, a trailing empty block is dropped
    pub fn take_all(&mut self) -> Vec<Buffer> {
        let mut blocks: Vec<Buffer> = (0..self.num_blocks())
            .map(|_| self.take_first_block())
            .collect();
        if blocks.last().is_some_and(Buffer::is_empty) {
            blocks.pop();
        }
        blocks
    }

    pub fn take_block_finished(&mut self) -> Option<Buffer> {
        self.take_block()
    }

    pub fn take_n(
        &mut self,
        n: usize,
        adjusted_block_size_iter: impl Iterator<Item = usize> + Clone,
    ) -> Buffer {
        self.bytes
            .take_n(n, Some(adjusted_block_size_iter))
            .into_buffer()
    }
}

impl<'a> Extend<&'a [u8]> for BlockedBytesBufferBuilder {
    fn extend<T: IntoIterator<Item = &'a [u8]>>(&mut self, iter: T) {
        for slice in iter {
            self.bytes.extend_from_slice(slice);
        }
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
            out.push(block.to_vec());
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
        assert_eq!(
            builder.take_block().map(|b| b.to_vec()),
            Some(b"abc".to_vec())
        );
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
            builder
                .take_all()
                .iter()
                .map(|b| b.to_vec())
                .collect::<Vec<_>>(),
            vec![b"abc".to_vec(), vec![], b"de".to_vec()]
        );
        assert_eq!(builder.len(), 0);
        assert_eq!(builder.num_blocks(), 1);

        builder.extend_from_slice(b"z");
        assert_eq!(
            builder
                .take_all()
                .iter()
                .map(|b| b.to_vec())
                .collect::<Vec<_>>(),
            vec![b"z".to_vec()]
        );
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
        // memory is returned page by page, so make a block span a whole page
        let page = unsafe { libc::sysconf(libc::_SC_PAGESIZE) } as usize;
        let mut builder = BlockedBytesBufferBuilder::new();
        let empty = builder.allocated_size();
        builder.extend_from_slice(&vec![1; page]);
        builder.start_new_block();
        builder.extend_from_slice(&vec![2; page]);
        let full = builder.allocated_size();
        assert!(full >= empty + 2 * page);
        builder.take_block();
        assert_eq!(builder.allocated_size(), full - page);
        builder.take_all();
        assert!(builder.allocated_size() < 2 * page);
    }

    #[test]
    fn take_n_relayouts() {
        let blocks: &[&[u8]] = &[b"abcde", b"fg", b"hij"];

        let mut builder = with_blocks(blocks);
        assert_eq!(
            builder.take_n(2, [3usize, 2, 3].into_iter()).as_slice(),
            b"ab"
        );
        assert_eq!(builder.len(), 8);
        assert_eq!(
            drain(&mut builder),
            vec![b"cde".to_vec(), b"fg".to_vec(), b"hij".to_vec()]
        );

        let mut builder = with_blocks(blocks);
        assert_eq!(builder.take_n(1, std::iter::once(9usize)).as_slice(), b"a");
        assert_eq!(drain(&mut builder), vec![b"bcdefghij".to_vec()]);

        let mut builder = with_blocks(blocks);
        assert_eq!(
            builder.take_n(5, [2usize, 3].into_iter()).as_slice(),
            b"abcde"
        );
        assert_eq!(drain(&mut builder), vec![b"fg".to_vec(), b"hij".to_vec()]);

        let mut builder = with_blocks(blocks);
        assert_eq!(
            builder.take_n(0, [5usize, 2, 3].into_iter()).as_slice(),
            b""
        );
        assert_eq!(
            drain(&mut builder),
            vec![b"abcde".to_vec(), b"fg".to_vec(), b"hij".to_vec()]
        );
    }

    #[test]
    fn take_n_with_empty_blocks_in_the_layout() {
        // taking no bytes can still move bytes between blocks, when the taken items were all empty
        let mut builder = with_blocks(&[b"abc", b"de"]);
        assert_eq!(
            builder.take_n(0, [0usize, 3, 2].into_iter()).as_slice(),
            b""
        );
        assert_eq!(
            drain(&mut builder),
            vec![vec![], b"abc".to_vec(), b"de".to_vec()]
        );

        // the whole first block is taken but the remaining layout still changes
        let mut builder = with_blocks(&[b"abc", b"de"]);
        assert_eq!(
            builder.take_n(3, [0usize, 0, 2].into_iter()).as_slice(),
            b"abc"
        );
        assert_eq!(drain(&mut builder), vec![vec![], vec![], b"de".to_vec()]);

        // empty blocks in the middle of the new layout and a trailing one
        let mut builder = with_blocks(&[b"abcd", b"", b"ef"]);
        assert_eq!(
            builder.take_n(1, [2usize, 0, 3, 0].into_iter()).as_slice(),
            b"a"
        );
        assert_eq!(builder.num_blocks(), 4);
        assert_eq!(
            drain(&mut builder),
            vec![b"bc".to_vec(), vec![], b"def".to_vec()]
        );

        // everything is taken and the new layout is only empty blocks
        let mut builder = with_blocks(&[b"ab"]);
        assert_eq!(builder.take_n(2, [0usize, 0].into_iter()).as_slice(), b"ab");
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
