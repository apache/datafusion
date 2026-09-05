use super::blocked_bytes_buffer_builder::BlockedBytesBufferBuilder;
use super::blocked_nulls_builder::BlockedNullsBuilder;
use super::blocked_offset_buffer_builder::BlockedOffsetBufferBuilder;
use crate::groups_accumulator::BlocksIndex;
use arrow::array::GenericByteArray;
use arrow::buffer::{Buffer, NullBuffer, OffsetBuffer, ScalarBuffer};
use arrow::datatypes::{ArrowNativeType, ByteArrayType};
use itertools::Itertools;

pub struct BlockedByteArrayBuilder<const FIXED_BLOCK_SIZING: bool, B: ByteArrayType> {
    blocked_offsets: BlockedOffsetBufferBuilder<FIXED_BLOCK_SIZING, B::Offset>,
    blocked_bytes: BlockedBytesBufferBuilder,
    blocked_nulls: BlockedNullsBuilder<FIXED_BLOCK_SIZING>,
}

impl<const FIXED_BLOCK_SIZING: bool, B: ByteArrayType>
    BlockedByteArrayBuilder<FIXED_BLOCK_SIZING, B>
{
    pub fn new(block_size: usize) -> Self {
        assert_ne!(block_size, 0, "block size must be greater than 0");

        Self {
            blocked_offsets: BlockedOffsetBufferBuilder::new(block_size),
            blocked_bytes: BlockedBytesBufferBuilder::new(),
            blocked_nulls: BlockedNullsBuilder::new(block_size),
        }
    }

    pub fn block_size(&self) -> usize {
        self.blocked_offsets.block_size()
    }

    pub fn len(&self) -> usize {
        self.blocked_offsets.len()
    }

    pub fn is_empty(&self) -> bool {
        self.blocked_offsets.is_empty()
    }

    pub fn allocated_size(&self) -> usize {
        self.blocked_offsets.allocated_size()
            + self.blocked_bytes.allocated_size()
            + self.blocked_nulls.allocated_size()
    }

    pub fn push_null(&mut self) {
        self.blocked_nulls.push_null();
        let should_open_new_block = self.blocked_offsets.push_length(0);
        if should_open_new_block {
            self.blocked_bytes.start_new_block();
        }
    }

    /// Append n valids to the builder with the bytes being later appended
    pub fn append_n_valids(&mut self, n: usize) {
        self.blocked_nulls.push_n(n, true);
    }

    /// Append `n` nulls to the builder
    pub fn append_n_nulls(&mut self, mut n: usize) {
        self.blocked_nulls.push_n(n, false);

        if !FIXED_BLOCK_SIZING {
            self.blocked_offsets.push_empty_within_block(n);
            return;
        }

        while n > 0 {
            let remaining_in_current_block = self.current_block_remaining_len();
            let to_add = remaining_in_current_block.min(n);
            n -= to_add;

            let should_create_new_block =
                self.blocked_offsets.push_empty_within_block(to_add);
            if should_create_new_block {
                self.blocked_bytes.start_new_block();
            }
        }
    }

    pub fn push(&mut self, item: Option<&[u8]>) {
        let Some(bytes) = item else {
            self.push_null();

            return;
        };

        self.append_valid();
        self.append_valid_slice(bytes);
    }

    pub fn append_valid(&mut self) {
        self.blocked_nulls.push_non_null();
    }

    pub fn append_valid_slice(&mut self, bytes: &[u8]) {
        self.blocked_bytes.extend_from_slice(bytes);

        let should_open_new_block = self.blocked_offsets.push_length(bytes.len());
        if should_open_new_block {
            self.blocked_bytes.start_new_block();
        }
    }

    pub fn start_new_block(&mut self) {
        assert!(
            !FIXED_BLOCK_SIZING,
            "only valid when FIXED_BLOCK_SIZING is false"
        );
        self.blocked_bytes.start_new_block();
        self.blocked_offsets.start_new_block();
        self.blocked_nulls.start_new_block();
    }

    pub fn current_block_bytes_len(&self) -> usize {
        // TODO - should always exists
        self.blocked_bytes.current_block_len()
    }

    fn current_block_remaining_len(&self) -> usize {
        self.blocked_offsets.current_block_remaining_len()
    }

    /// Append every item of `array`
    pub fn extends_from_array(&mut self, array: &GenericByteArray<B>) {
        // ponytail: per item pushes, bulk copy the offsets/bytes/nulls per block if this shows up in profiles
        for item in array.iter() {
            self.push(item.map(|value| value.as_ref()));
        }
    }

    pub fn value(&self, index: BlocksIndex) -> Option<&[u8]> {
        if !self.blocked_nulls[index] {
            return None;
        }

        Some(self.value_bytes(index))
    }

    /// return the current value of the specified row irrespective of null
    pub fn value_bytes(&self, index: BlocksIndex) -> &[u8] {
        let start_in_block = self.blocked_offsets[index].as_usize();

        // Offset in block + 1 always exists for offsets since block size is + 1
        let end_in_block = self.blocked_offsets[index.next_index_in_block()].as_usize();
        let bytes_block = self.blocked_bytes.block(index.block_index());

        // Safety: the offsets are constructed correctly and never decrease
        unsafe {
            bytes_block
                .as_slice()
                .get_unchecked(start_in_block..end_in_block)
        }
    }

    pub fn is_valid(&self, index: BlocksIndex) -> bool {
        self.blocked_nulls[index]
    }

    pub fn is_null(&self, index: BlocksIndex) -> bool {
        !self.is_valid(index)
    }

    pub fn value_len(&self, index: BlocksIndex) -> usize {
        let start_in_block = self.blocked_offsets[index];

        // Offset in block + 1 always exists for offsets since block size is + 1
        let end_in_block = self.blocked_offsets[index.next_index_in_block()];

        end_in_block.as_usize() - start_in_block.as_usize()
    }

    /// Take the first block, `None` once there are no more items
    pub fn take_block(&mut self) -> Option<GenericByteArray<B>> {
        let (offsets, bytes, nulls) = self.take_block_parts()?;

        Some(GenericByteArray::new(offsets, bytes, nulls))
    }

    /// Take a block but build it unchecked
    ///
    /// # Safety
    /// The builder only ever produces valid offsets, for string types the caller must
    /// have pushed valid utf8 only
    pub unsafe fn take_block_unchecked(&mut self) -> Option<GenericByteArray<B>> {
        let (offsets, bytes, nulls) = self.take_block_parts()?;

        Some(unsafe { GenericByteArray::new_unchecked(offsets, bytes, nulls) })
    }

    fn take_block_parts(
        &mut self,
    ) -> Option<(OffsetBuffer<B::Offset>, Buffer, Option<NullBuffer>)> {
        let offsets = self.blocked_offsets.take_block_finished()?;
        let nulls = self
            .blocked_nulls
            .take_block()
            .expect("nulls and offsets have the same blocks");
        // The bytes block may be empty when every item in it is empty or null
        let bytes = self.blocked_bytes.take_first_block();

        Some((offsets, Buffer::from(bytes), nulls))
    }

    /// Take every non empty block
    pub fn take_all(&mut self) -> Vec<GenericByteArray<B>> {
        self.take_all_parts()
            .map(|(offsets, bytes, nulls)| GenericByteArray::new(offsets, bytes, nulls))
            .collect()
    }

    /// Take all but build blocks unchecked
    ///
    /// # Safety
    /// See [`Self::take_block_unchecked`]
    pub unsafe fn take_all_unchecked(&mut self) -> Vec<GenericByteArray<B>> {
        self.take_all_parts()
            .map(|(offsets, bytes, nulls)| unsafe {
                GenericByteArray::new_unchecked(offsets, bytes, nulls)
            })
            .collect()
    }

    fn take_all_parts(
        &mut self,
    ) -> impl Iterator<Item = (OffsetBuffer<B::Offset>, Buffer, Option<NullBuffer>)> {
        // The bytes builder does not know how many items are in a block so it can not tell
        // a block of empty values from an unused trailing block, take as many as the offsets have
        let offsets = self.blocked_offsets.take_all();
        let nulls = self.blocked_nulls.take_all();
        let mut bytes = self.blocked_bytes.take_all();
        bytes.resize_with(offsets.len(), Vec::new);

        offsets.into_iter().zip_eq(bytes).zip_eq(nulls).map(
            |((offsets, bytes), nulls)| {
                (Self::offsets_from_vec(offsets), Buffer::from(bytes), nulls)
            },
        )
    }

    pub fn take_n(
        &mut self,
        n: usize,
        adjusted_block_size: Option<impl Iterator<Item = usize> + Clone>,
    ) -> GenericByteArray<B> {
        let (offsets, bytes, nulls) = self.take_n_parts(n, adjusted_block_size);

        GenericByteArray::new(offsets, bytes, nulls)
    }

    /// Take n items but build it unchecked
    ///
    /// # Safety
    /// See [`Self::take_block_unchecked`]
    pub unsafe fn take_n_unchecked(
        &mut self,
        n: usize,
        adjusted_block_size: Option<impl Iterator<Item = usize> + Clone>,
    ) -> GenericByteArray<B> {
        let (offsets, bytes, nulls) = self.take_n_parts(n, adjusted_block_size);

        unsafe { GenericByteArray::new_unchecked(offsets, bytes, nulls) }
    }

    fn take_n_parts(
        &mut self,
        n: usize,
        adjusted_block_size: Option<impl Iterator<Item = usize> + Clone>,
    ) -> (OffsetBuffer<B::Offset>, Buffer, Option<NullBuffer>) {
        assert_eq!(FIXED_BLOCK_SIZING, adjusted_block_size.is_none());

        let offsets = Self::offsets_from_vec(
            self.blocked_offsets.take_n(n, adjusted_block_size.clone()),
        );
        let nulls = self.blocked_nulls.take_n(n, adjusted_block_size);

        // The offsets are already re-laid out, the last offset of each block is its byte count
        let bytes_per_block: Vec<usize> = self
            .blocked_offsets
            .blocks_iter()
            .map(|block| block[block.len() - 1].as_usize())
            .collect();
        let bytes = self.blocked_bytes.take_n(
            offsets[offsets.len() - 1].as_usize(),
            bytes_per_block.into_iter(),
        );

        (offsets, Buffer::from(bytes), nulls)
    }

    fn offsets_from_vec(offsets: Vec<B::Offset>) -> OffsetBuffer<B::Offset> {
        // SAFETY: the offsets builder only ever produces monotonically increasing offsets starting at 0
        unsafe { OffsetBuffer::new_unchecked(ScalarBuffer::from(offsets)) }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{GenericBinaryArray, StringArray};
    use arrow::datatypes::{GenericBinaryType, Utf8Type};

    type Fixed = BlockedByteArrayBuilder<true, Utf8Type>;
    type Manual = BlockedByteArrayBuilder<false, GenericBinaryType<i32>>;

    /// Mix of nulls, empty strings and values of different lengths
    fn model(seed: usize, n: usize) -> Vec<Option<String>> {
        (0..n)
            .map(|i| match (i + seed) % 5 {
                0 => None,
                1 => Some(String::new()),
                k => Some(format!("{}", (i + seed) * 13).repeat(k)),
            })
            .collect()
    }

    fn array_of(values: &[Option<String>]) -> StringArray {
        StringArray::from(values.to_vec())
    }

    fn values_of(array: &StringArray) -> Vec<Option<String>> {
        array.iter().map(|v| v.map(str::to_string)).collect()
    }

    fn values(builder: &Fixed, block_size: usize) -> Vec<Option<String>> {
        (0..builder.len())
            .map(|i| {
                let index = BlocksIndex::from_index_in_fixed_block_size(i, block_size);
                builder
                    .value(index)
                    .map(|bytes| String::from_utf8(bytes.to_vec()).unwrap())
            })
            .collect()
    }

    fn fixed_with(block_size: usize, values: &[Option<String>]) -> Fixed {
        let mut builder = Fixed::new(block_size);
        for value in values {
            builder.push(value.as_deref().map(str::as_bytes));
        }
        assert_eq!(builder.len(), values.len());
        builder
    }

    fn drain(builder: &mut Fixed) -> Vec<Vec<Option<String>>> {
        let mut out = vec![];
        while let Some(block) = builder.take_block() {
            out.push(values_of(&block));
        }
        assert_eq!(builder.len(), 0);
        out
    }

    fn chunks(values: &[Option<String>], block_size: usize) -> Vec<Vec<Option<String>>> {
        values.chunks(block_size).map(|c| c.to_vec()).collect()
    }

    #[test]
    fn new_is_empty() {
        let mut builder = Fixed::new(3);
        assert_eq!(builder.len(), 0);
        assert_eq!(builder.current_block_bytes_len(), 0);
        assert!(builder.take_block().is_none());
        assert!(builder.take_all().is_empty());
    }

    #[test]
    fn push_and_take_block() {
        let model = model(0, 8);
        let mut builder = fixed_with(3, &model);
        assert_eq!(values(&builder, 3), model);

        assert_eq!(drain(&mut builder), chunks(&model, 3));
        assert!(builder.take_block().is_none());

        // usable after
        builder.push(Some(b"again"));
        assert_eq!(drain(&mut builder), vec![vec![Some("again".to_string())]]);
    }

    #[test]
    fn value_accessors() {
        let mut builder = Fixed::new(2);
        builder.push(Some(b"ab"));
        builder.push(None);
        builder.push(Some(b""));
        builder.push(Some(b"cde"));

        let at = |i| BlocksIndex::from_index_in_fixed_block_size(i, 2);
        assert_eq!(builder.value(at(0)), Some(b"ab".as_slice()));
        assert_eq!(builder.value(at(1)), None);
        assert_eq!(builder.value(at(2)), Some(b"".as_slice()));
        assert_eq!(builder.value(at(3)), Some(b"cde".as_slice()));

        assert_eq!(builder.value_bytes(at(1)), b"");
        assert_eq!(builder.value_bytes(at(3)), b"cde");
        assert!(builder.is_null(at(1)));
        assert!(builder.is_valid(at(2)));
        assert_eq!(builder.value_len(at(0)), 2);
        assert_eq!(builder.value_len(at(1)), 0);
        assert_eq!(builder.value_len(at(3)), 3);
        // the 4th push filled the block so the bytes block being written to is a fresh one
        assert_eq!(builder.current_block_bytes_len(), 0);
    }

    #[test]
    fn append_n_nulls_and_valids() {
        let mut builder = Fixed::new(3);
        builder.append_n_nulls(4);
        assert_eq!(values(&builder, 3), vec![None; 4]);

        builder.append_n_valids(2);
        builder.append_valid_slice(b"a");
        builder.append_valid_slice(b"bc");
        let mut expected = vec![None; 4];
        expected.push(Some("a".to_string()));
        expected.push(Some("bc".to_string()));
        assert_eq!(values(&builder, 3), expected);

        builder.append_valid();
        builder.append_valid_slice(b"");
        expected.push(Some(String::new()));
        assert_eq!(values(&builder, 3), expected);
        assert_eq!(drain(&mut builder), chunks(&expected, 3));
    }

    #[test]
    fn extends_from_array_spans_blocks() {
        let model = model(1, 11);
        let mut builder = Fixed::new(4);
        builder.push(Some(b"first"));
        builder.extends_from_array(&array_of(&model));
        builder.extends_from_array(&array_of(&[]));

        let mut expected = vec![Some("first".to_string())];
        expected.extend(model);
        assert_eq!(values(&builder, 4), expected);
        assert_eq!(drain(&mut builder), chunks(&expected, 4));
    }

    #[test]
    fn take_all_returns_only_non_empty_blocks() {
        let model = model(2, 6);
        let mut builder = fixed_with(3, &model);
        let blocks: Vec<_> = builder.take_all().iter().map(values_of).collect();
        assert_eq!(blocks, chunks(&model, 3));
        assert_eq!(builder.len(), 0);
        assert!(builder.take_all().is_empty());

        // a block whose items hold no bytes at all is still a block
        let empties = vec![Some(String::new()), None, Some(String::new())];
        let mut builder = fixed_with(3, &empties);
        builder.push(Some(b"x"));
        let blocks: Vec<_> = builder.take_all().iter().map(values_of).collect();
        assert_eq!(blocks, vec![empties, vec![Some("x".to_string())]]);
    }

    #[test]
    fn unchecked_variants_match_checked() {
        let model = model(3, 7);
        let mut checked = fixed_with(3, &model);
        let mut unchecked = fixed_with(3, &model);

        assert_eq!(
            checked.take_block().unwrap(),
            unsafe { unchecked.take_block_unchecked() }.unwrap()
        );
        assert_eq!(checked.take_n(1, None::<std::iter::Empty<usize>>), unsafe {
            unchecked.take_n_unchecked(1, None::<std::iter::Empty<usize>>)
        });
        assert_eq!(checked.take_all(), unsafe {
            unchecked.take_all_unchecked()
        });
    }

    #[test]
    fn take_n_with_blocks_that_hold_no_bytes() {
        let empty = || Some(String::new());
        let model = vec![
            empty(),
            empty(),
            Some("abc".to_string()),
            None,
            Some("d".to_string()),
            Some("e".to_string()),
        ];
        let mut builder = fixed_with(2, &model);

        // no bytes are taken but bytes still move between blocks
        let taken = builder.take_n(1, None::<std::iter::Empty<usize>>);
        assert_eq!(values_of(&taken), vec![empty()]);
        assert_eq!(values(&builder, 2), model[1..]);
        assert_eq!(drain(&mut builder), chunks(&model[1..], 2));

        // the taken items hold all the bytes of the first block
        let model = vec![
            Some("abc".to_string()),
            empty(),
            empty(),
            Some("d".to_string()),
        ];
        let mut builder = fixed_with(3, &model);
        let taken = builder.take_n(1, None::<std::iter::Empty<usize>>);
        assert_eq!(values_of(&taken), model[..1]);
        assert_eq!(drain(&mut builder), chunks(&model[1..], 3));
    }

    #[test]
    fn take_n_matches_model_and_stays_usable() {
        for block_size in [1, 2, 3, 5] {
            for total in 0..=(3 * block_size + 1) {
                for n in 0..=total.min(block_size) {
                    let mut model = model(block_size * 100 + total, total);
                    let mut builder = fixed_with(block_size, &model);

                    let taken = builder.take_n(n, None::<std::iter::Empty<usize>>);
                    let expected_taken: Vec<_> = model.drain(..n).collect();
                    assert_eq!(
                        values_of(&taken),
                        expected_taken,
                        "taken mismatch bs={block_size} total={total} n={n}"
                    );
                    assert_eq!(
                        values(&builder, block_size),
                        model,
                        "remaining mismatch bs={block_size} total={total} n={n}"
                    );

                    let more = self::model(n + 1, 2 * block_size + 1);
                    builder.extends_from_array(&array_of(&more));
                    model.extend(more);
                    assert_eq!(
                        values(&builder, block_size),
                        model,
                        "after push mismatch bs={block_size} total={total} n={n}"
                    );

                    assert_eq!(
                        drain(&mut builder),
                        chunks(&model, block_size),
                        "drain mismatch bs={block_size} total={total} n={n}"
                    );
                }
            }
        }
    }

    #[test]
    fn allocated_size_follows_blocks() {
        let mut builder = Fixed::new(4);
        let empty = builder.allocated_size();
        for _ in 0..10 {
            builder.push(Some(&[7; 50]));
        }
        let full = builder.allocated_size();
        assert!(full >= empty + 500);
        builder.take_block();
        assert!(builder.allocated_size() < full);
    }

    // ---- manual block sizing ----

    fn manual_values(array: &GenericBinaryArray<i32>) -> Vec<Option<Vec<u8>>> {
        array.iter().map(|v| v.map(<[u8]>::to_vec)).collect()
    }

    fn manual_with_blocks(blocks: &[Vec<Option<&[u8]>>]) -> Manual {
        let mut builder = Manual::new(1);
        for (i, block) in blocks.iter().enumerate() {
            if i > 0 {
                builder.start_new_block();
            }
            for value in block {
                builder.push(*value);
            }
        }
        builder
    }

    fn drain_manual(builder: &mut Manual) -> Vec<Vec<Option<Vec<u8>>>> {
        let mut out = vec![];
        while let Some(block) = builder.take_block() {
            out.push(manual_values(&block));
        }
        out
    }

    #[test]
    fn manual_blocks_and_take_n() {
        let a: Vec<Option<&[u8]>> = vec![Some(b"ab"), None, Some(b""), Some(b"cde")];
        let b: Vec<Option<&[u8]>> = vec![Some(b"f"), Some(b"gh")];
        let owned = |block: &[Option<&[u8]>]| -> Vec<Option<Vec<u8>>> {
            block.iter().map(|v| v.map(<[u8]>::to_vec)).collect()
        };

        let mut builder = manual_with_blocks(&[a.clone(), b.clone()]);
        assert_eq!(builder.len(), 6);
        assert_eq!(
            builder.value(BlocksIndex::new(1, 1)),
            Some(b"gh".as_slice())
        );
        assert_eq!(builder.value(BlocksIndex::new(0, 1)), None);
        assert_eq!(drain_manual(&mut builder), vec![owned(&a), owned(&b)]);

        // re-layout into different block sizes
        let mut builder = manual_with_blocks(&[a.clone(), b.clone()]);
        let taken = builder.take_n(1, Some([2usize, 1, 2].into_iter()));
        assert_eq!(manual_values(&taken), owned(&a[..1]));
        assert_eq!(
            drain_manual(&mut builder),
            vec![owned(&a[1..3]), owned(&a[3..]), owned(&b)]
        );

        // take the whole first block
        let mut builder = manual_with_blocks(&[a.clone(), b.clone()]);
        let taken = builder.take_n(4, Some([2usize].into_iter()));
        assert_eq!(manual_values(&taken), owned(&a));
        assert_eq!(drain_manual(&mut builder), vec![owned(&b)]);

        // merge everything
        let mut builder = manual_with_blocks(&[a.clone(), b.clone()]);
        let taken = builder.take_n(2, Some([4usize].into_iter()));
        assert_eq!(manual_values(&taken), owned(&a[..2]));
        let mut rest = owned(&a[2..]);
        rest.extend(owned(&b));
        assert_eq!(drain_manual(&mut builder), vec![rest]);
    }
}
