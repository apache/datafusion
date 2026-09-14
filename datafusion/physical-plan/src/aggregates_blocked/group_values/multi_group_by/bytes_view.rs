// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

use crate::aggregates_blocked::group_values::multi_group_by::{
    BlockedGroupColumn, Nulls, nulls_equal_to,
};
use arrow::array::{
    Array, ArrayRef, AsArray, BooleanBufferBuilder, ByteView, GenericByteViewArray,
    make_view,
};
use arrow::buffer::{Buffer, NullBuffer, ScalarBuffer};
use arrow::datatypes::ByteViewType;
use datafusion_common::Result;
use datafusion_common::utils::proxy::VecAllocExt;
use datafusion_expr_common::blocked_helpers::{
    BlockedBytesBufferBuilder, BlockedNullsBuilder, CopyItemBlockedVecBuilder,
};
use datafusion_expr_common::groups_accumulator::{BlockedGroupSelection, BlocksIndex};
use std::iter::once;
use std::marker::PhantomData;
use std::mem::size_of;
use std::sync::Arc;

const BYTE_VIEW_MAX_BLOCK_SIZE: usize = 2 * 1024 * 1024;

/// An implementation of [`BlockedGroupColumn`] for binary view and utf8 view types.
///
/// Stores a collection of binary view or utf8 view group values in a buffer
/// whose structure is similar to `GenericByteViewArray`, and we can get benefits:
///
/// 1. Efficient comparison of incoming rows to existing rows
/// 2. Efficient construction of the final output array
/// 3. Zero copy `take_next_block`: every views block owns whole bytes blocks, so an
///    emitted block never shares (or keeps alive) the bytes of another one
pub struct ByteViewGroupValueBuilder<const FIXED_BLOCK_SIZING: bool, B: ByteViewType> {
    /// The views of string values
    ///
    /// If string len <= 12, the view's format will be:
    ///   string(12B) | len(4B)
    ///
    /// If string len > 12, its format will be:
    ///     offset(4B) | buffer_index(4B) | prefix(4B) | len(4B)
    ///
    /// where `buffer_index` is relative to the first bytes block of the views block
    /// the value is in
    views: CopyItemBlockedVecBuilder<FIXED_BLOCK_SIZING, u128>,

    /// The bytes of the non inlined values, every views block owns whole bytes blocks
    bytes: BlockedBytesBufferBuilder,

    /// Number of bytes blocks of every views block, the open one included
    num_bytes_blocks_per_block: Vec<usize>,

    /// Index of the first bytes block of every views block, parallel to
    /// `num_bytes_blocks_per_block`
    block_starts: Vec<usize>,

    /// The max size of a bytes block
    ///
    /// A new bytes block is started when the current one has not enough remaining
    /// capacity (`max_block_size` - its len) to store the appended value.
    ///
    /// Currently it is fixed at 2MB.
    max_block_size: usize,

    /// Nulls
    nulls: BlockedNullsBuilder<FIXED_BLOCK_SIZING>,

    /// phantom data so the type requires `<B>`
    _phantom: PhantomData<B>,
}

impl<const FIXED_BLOCK_SIZING: bool, B: ByteViewType>
    ByteViewGroupValueBuilder<FIXED_BLOCK_SIZING, B>
{
    pub fn new(block_size: usize) -> Self {
        if FIXED_BLOCK_SIZING {
            assert_ne!(block_size, 0);
        }

        Self {
            views: CopyItemBlockedVecBuilder::new(block_size),
            bytes: BlockedBytesBufferBuilder::new(),
            num_bytes_blocks_per_block: vec![1],
            block_starts: vec![0],
            max_block_size: BYTE_VIEW_MAX_BLOCK_SIZE,
            nulls: BlockedNullsBuilder::new(block_size),
            _phantom: PhantomData {},
        }
    }

    /// Set the max block size
    fn with_max_block_size(mut self, max_block_size: usize) -> Self {
        self.max_block_size = max_block_size;
        self
    }

    /// The bytes block referenced by `view`, which belongs to the value at `index`
    #[inline]
    fn bytes_block_of(&self, index: BlocksIndex, view: &ByteView) -> usize {
        let views_block = self.nulls.locate(index).0;
        self.block_starts[views_block] + view.buffer_index as usize
    }

    /// A views block was completed, the next one starts with a fresh bytes block
    fn open_views_block(&mut self) {
        self.bytes.start_new_block();
        self.block_starts.push(self.bytes.num_blocks() - 1);
        self.num_bytes_blocks_per_block.push(1);
    }

    #[inline]
    fn push_view(&mut self, view: u128) {
        if self.views.push(view) {
            self.open_views_block();
        }
    }

    /// Bulk appends with fixed sizing cross views block boundaries silently, open the
    /// bytes blocks they need
    fn sync_bytes_blocks(&mut self) {
        while self.num_bytes_blocks_per_block.len() < self.views.num_blocks() {
            self.open_views_block();
        }
    }

    fn append_null(&mut self) {
        self.nulls.push_null();
        self.push_view(0);
    }

    /// Copies the bytes of a non inlined value and returns the `(buffer_index, offset)`
    /// to reference them from a view
    fn append_bytes(&mut self, value: &[u8]) -> (u32, u32) {
        debug_assert!(value.len() > 12);

        // If the current bytes block isn't big enough, start a new one for this views block
        let current_len = self.bytes.current_block_len();
        if current_len > 0 && current_len + value.len() > self.max_block_size {
            self.bytes.start_new_block();
            *self
                .num_bytes_blocks_per_block
                .last_mut()
                .expect("always has the open views block") += 1;
        }

        let buffer_index = (self.num_bytes_blocks_per_block.last().unwrap() - 1) as u32;
        let offset = u32::try_from(self.bytes.current_block_len())
            .expect("a single value exceeds u32::MAX bytes");
        self.bytes.extend_from_slice(value);

        (buffer_index, offset)
    }

    /// Appends a non null value given as bytes
    fn append_value(&mut self, value: &[u8]) {
        let view = if value.len() <= 12 {
            make_view(value, 0, 0)
        } else {
            let (buffer_index, offset) = self.append_bytes(value);
            make_view(value, buffer_index, offset)
        };
        self.push_view(view);
    }

    fn equal_to_inner(&self, lhs_row: BlocksIndex, array: &ArrayRef, rhs_row: usize) -> bool {
        let array = array.as_byte_view::<B>();
        // since this is a single row comparison, don't bother specializing for nulls/buffers
        self.do_equal_to_inner::<true, true>(lhs_row, array, rhs_row)
    }

    fn append_val_inner(&mut self, array: &ArrayRef, row: usize) {
        let arr = array.as_byte_view::<B>();

        if arr.is_null(row) {
            self.append_null();
            return;
        }

        self.nulls.push_non_null();
        self.do_append_val_inner(arr, row);
    }

    // Don't inline to keep the code small and give LLVM the best chance of
    // vectorizing the inner loop
    #[inline(never)]
    fn vectorized_equal_to_inner<const HAS_NULLS: bool, const HAS_BUFFERS: bool>(
        &self,
        lhs_rows: &[BlocksIndex],
        array: &GenericByteViewArray<B>,
        rhs_rows: &[usize],
        equal_to_results: &mut BooleanBufferBuilder,
    ) {
        for (idx, (&lhs_row, &rhs_row)) in
            lhs_rows.iter().zip(rhs_rows.iter()).enumerate()
        {
            if !equal_to_results.get_bit(idx) {
                continue;
            }

            if !self.do_equal_to_inner::<HAS_NULLS, HAS_BUFFERS>(lhs_row, array, rhs_row)
            {
                equal_to_results.set_bit(idx, false);
            }
        }
    }

    fn vectorized_append_inner(&mut self, array: &ArrayRef, rows: &[usize]) {
        let arr = array.as_byte_view::<B>();
        let null_count = array.null_count();
        let num_rows = array.len();
        let all_null_or_non_null = if null_count == 0 {
            Nulls::None
        } else if null_count == num_rows {
            Nulls::All
        } else {
            Nulls::Some
        };

        match all_null_or_non_null {
            Nulls::Some => {
                for &row in rows {
                    self.append_val_inner(array, row);
                }
            }

            Nulls::None => {
                self.nulls.push_n_non_nulls(rows.len());
                if arr.data_buffers().is_empty() {
                    // Fast path: all strings are inline (<= 12 bytes) so the input views
                    // can be copied as is
                    self.views.extend(rows.iter().map(|&row| arr.views()[row]));
                    self.sync_bytes_blocks();
                } else {
                    for &row in rows {
                        self.do_append_val_inner(arr, row);
                    }
                }
            }

            Nulls::All => {
                self.nulls.push_n_nulls(rows.len());
                self.views.push_value_n(0, rows.len());
                self.sync_bytes_blocks();
            }
        }
    }

    /// Appends the non null value at `row`, reusing the prefix of the input view
    fn do_append_val_inner(&mut self, array: &GenericByteViewArray<B>, row: usize) {
        // SAFETY: the caller ensures `row` is valid
        let view = unsafe { *array.views().get_unchecked(row) };

        if (view as u32) <= 12 {
            // Inline value: the view is already self-contained, push as-is
            self.push_view(view);
            return;
        }

        // Non-inline value: copy the bytes and point the view into our own bytes blocks
        let value: &[u8] = unsafe { array.value_unchecked(row).as_ref() };
        let (buffer_index, offset) = self.append_bytes(value);

        let src = ByteView::from(view);
        let new_view = ByteView {
            length: src.length,
            prefix: src.prefix,
            buffer_index,
            offset,
        }
        .as_u128();
        self.push_view(new_view);
    }

    /// The bytes referenced by the non inlined view of the value at `index`
    #[inline]
    fn non_inlined_value(&self, index: BlocksIndex, view: ByteView) -> &[u8] {
        let offset = view.offset as usize;
        let length = view.length as usize;
        let block = self.bytes.block(self.bytes_block_of(index, &view));
        debug_assert!(offset + length <= block.len());
        // SAFETY: views only ever point into bytes this builder appended to that block
        unsafe { block.get_unchecked(offset..offset + length) }
    }

    /// Compare the value at `lhs_row` in this builder with
    /// the value at `rhs_row` in input `array`
    ///
    /// Templated so that the inner compare loop can be
    /// specialized based on the input array
    #[inline(always)]
    fn do_equal_to_inner<const HAS_NULLS: bool, const HAS_BUFFERS: bool>(
        &self,
        lhs_row: BlocksIndex,
        array: &GenericByteViewArray<B>,
        rhs_row: usize,
    ) -> bool {
        // Check if nulls equal firstly
        if HAS_NULLS {
            let exist_null = self.nulls.is_null(lhs_row);
            let input_null = array.is_null(rhs_row);
            if let Some(result) = nulls_equal_to(exist_null, input_null) {
                return result;
            }
        }

        // Otherwise, we need to check their values

        // SAFETY: `lhs_row` is a live group index
        let exist_view = unsafe { *self.views.get_unchecked(lhs_row) };
        let exist_view_len = exist_view as u32;

        // SAFETY: `rhs_row` is valid
        let input_view = unsafe { *array.views().get_unchecked(rhs_row) };
        let input_view_len = input_view as u32;

        // fast path, if we know there are no buffers, then the view must be inlined
        // so we can simply compare the u128 views
        if !HAS_BUFFERS {
            return exist_view == input_view;
        }

        // The check logic
        //   - Check len equality
        //   - If inlined, check inlined value
        //   - If non-inlined, check prefix and then check value in buffer
        //     when needed
        if exist_view_len != input_view_len {
            return false;
        }

        if exist_view_len <= 12 {
            // both inlined, so compare inlined value
            exist_view == input_view
        } else {
            let exist_prefix =
                unsafe { GenericByteViewArray::<B>::inline_value(&exist_view, 4) };
            let input_prefix =
                unsafe { GenericByteViewArray::<B>::inline_value(&input_view, 4) };

            if exist_prefix != input_prefix {
                return false;
            }

            // get the full values and compare
            let exist_full = self.non_inlined_value(lhs_row, ByteView::from(exist_view));
            let input_full: &[u8] = unsafe { array.value_unchecked(rhs_row).as_ref() };
            exist_full == input_full
        }
    }

    /// Returns the bytes stored at `index`, irrespective of nullness.
    fn value(&self, index: BlocksIndex) -> &[u8] {
        let view = &self.views[index];
        let byte_view = ByteView::from(*view);
        let length = byte_view.length as usize;
        if length <= 12 {
            // SAFETY: `view` is a valid inline view with `length` bytes.
            unsafe { GenericByteViewArray::<B>::inline_value(view, length) }
        } else {
            self.non_inlined_value(index, byte_view)
        }
    }

    fn values_preserving_inner(&self, selection: BlockedGroupSelection<'_>) -> Result<ArrayRef> {
        selection.validate_num_groups(self.len())?;

        // A block big enough for the whole selection so it comes out as a single array
        let mut selected =
            Self::new(selection.len().max(1)).with_max_block_size(self.max_block_size);
        for index in selection.iter() {
            if self.nulls.is_null(index) {
                selected.append_null();
            } else {
                selected.nulls.push_non_null();
                selected.append_value(self.value(index));
            }
        }

        Ok(Box::new(selected)
            .take_all()
            .pop()
            .unwrap_or_else(|| Arc::new(GenericByteViewArray::<B>::new_null(0))))
    }

    fn build(
        views: ScalarBuffer<u128>,
        buffers: Vec<Buffer>,
        nulls: Option<NullBuffer>,
    ) -> ArrayRef {
        // A block of inlined values only does not need data buffers
        let buffers = if buffers.iter().all(Buffer::is_empty) {
            vec![]
        } else {
            buffers
        };
        // Safety:
        // * all views were correctly made
        // * (if utf8): Input was valid Utf8 so buffer contents are
        // valid utf8 as well
        Arc::new(unsafe { GenericByteViewArray::<B>::new_unchecked(views, buffers, nulls) })
    }
}

impl<const FIXED_BLOCK_SIZING: bool, B: ByteViewType> BlockedGroupColumn<FIXED_BLOCK_SIZING>
    for ByteViewGroupValueBuilder<FIXED_BLOCK_SIZING, B>
{
    fn batch_size(&self) -> usize {
        self.views.block_size()
    }

    fn equal_to(&self, lhs_row: BlocksIndex, array: &ArrayRef, rhs_row: usize) -> bool {
        self.equal_to_inner(lhs_row, array, rhs_row)
    }

    fn append_val(&mut self, array: &ArrayRef, row: usize) -> Result<()> {
        self.append_val_inner(array, row);
        Ok(())
    }

    fn vectorized_equal_to(
        &self,
        group_indices: &[BlocksIndex],
        array: &ArrayRef,
        rows: &[usize],
        equal_to_results: &mut BooleanBufferBuilder,
    ) {
        let has_nulls = array.null_count() != 0;
        let array = array.as_byte_view::<B>();
        let has_buffers = !array.data_buffers().is_empty();
        // call specialized version based on nulls and buffers presence
        match (has_nulls, has_buffers) {
            (true, true) => self.vectorized_equal_to_inner::<true, true>(
                group_indices,
                array,
                rows,
                equal_to_results,
            ),
            (true, false) => self.vectorized_equal_to_inner::<true, false>(
                group_indices,
                array,
                rows,
                equal_to_results,
            ),
            (false, true) => self.vectorized_equal_to_inner::<false, true>(
                group_indices,
                array,
                rows,
                equal_to_results,
            ),
            (false, false) => self.vectorized_equal_to_inner::<false, false>(
                group_indices,
                array,
                rows,
                equal_to_results,
            ),
        }
    }

    fn vectorized_append(&mut self, array: &ArrayRef, rows: &[usize]) -> Result<()> {
        self.vectorized_append_inner(array, rows);
        Ok(())
    }

    fn len(&self) -> usize {
        self.views.len()
    }

    fn size(&self) -> usize {
        self.nulls.allocated_size()
            + self.views.allocated_size()
            + self.bytes.allocated_size()
            + self.num_bytes_blocks_per_block.allocated_size()
            + self.block_starts.allocated_size()
            + size_of::<Self>()
    }

    fn values_preserving(&self, selection: BlockedGroupSelection<'_>) -> Result<ArrayRef> {
        self.values_preserving_inner(selection)
    }

    fn take_all(self: Box<Self>) -> Vec<ArrayRef> {
        let mut this = *self;
        let mut blocks = Vec::with_capacity(this.views.num_blocks());
        while let Some(block) = this.take_next_block() {
            // manual sizing may have opened a block and never filled it
            if !block.is_empty() {
                blocks.push(block);
            }
        }
        blocks
    }

    /// Take the first `n` values, the remaining values shift down by `n`
    ///
    /// Nothing is copied: the taken views and bytes are a prefix of the builders. The
    /// remaining bytes are only re-blocked so that every views block still owns whole
    /// bytes blocks, which means rewriting the buffer index and offset of the remaining
    /// non inlined views
    fn take_n(&mut self, n: usize) -> ArrayRef {
        debug_assert!(self.len() >= n);
        let block_size = self.views.block_size();
        let taken_views = self.views.take_n(n, None::<std::iter::Empty<_>>);
        let nulls = self.nulls.take_n(n, None::<std::iter::Empty<_>>);

        // Bytes are appended in value order, so the taken values own a prefix of the
        // bytes ending where the last non inlined taken value ends, in bytes block `b` of
        // the first views block whose bytes blocks start at 0
        let (b, end) = taken_views
            .iter()
            .rev()
            .find(|view| (**view as u32) > 12)
            .map(|view| {
                let view = ByteView::from(*view);
                (view.buffer_index as usize, (view.offset + view.length) as usize)
            })
            .unwrap_or((0, 0));

        let old_block_lens: Vec<usize> =
            (0..self.bytes.num_blocks()).map(|i| self.bytes.block(i).len()).collect();
        let taken_bytes = old_block_lens[..b].iter().sum::<usize>() + end;
        let remaining_bytes = self.bytes.len() - taken_bytes;

        // Old block boundaries that fall in the remaining bytes, relative to their start
        let mut old_boundaries = old_block_lens[b..]
            .iter()
            .scan(0usize, |acc, len| {
                *acc += len;
                Some(*acc - end)
            })
            .filter(|&boundary| boundary > 0 && boundary < remaining_bytes)
            .peekable();

        // New layout: a bytes block starts at every views block and at every old
        // boundary that is kept, views are rewritten to point into it
        let mut starts: Vec<usize> = vec![];
        let mut per_views_block: Vec<usize> = Vec::new();
        let mut pos = 0usize;
        let mut first_block_of_current = 0usize;
        for (k, view) in self.views.iter_mut().enumerate() {
            if k % block_size == 0 {
                first_block_of_current = starts.len();
                starts.push(pos);
                per_views_block.push(1);
            }
            let len = *view as u32;
            if len <= 12 {
                continue;
            }
            while old_boundaries.peek().is_some_and(|&boundary| boundary <= pos) {
                let boundary = old_boundaries.next().unwrap();
                if boundary == pos && starts.last() != Some(&pos) {
                    starts.push(pos);
                    *per_views_block.last_mut().unwrap() += 1;
                }
            }
            let mut byte_view = ByteView::from(*view);
            byte_view.buffer_index = (starts.len() - 1 - first_block_of_current) as u32;
            byte_view.offset = (pos - starts[starts.len() - 1]) as u32;
            *view = byte_view.as_u128();
            pos += len as usize;
        }
        debug_assert_eq!(pos, remaining_bytes);
        // The views block being written to always owns a bytes block, even when empty
        if self.views.len().is_multiple_of(block_size) {
            starts.push(pos);
            per_views_block.push(1);
        }
        let sizes: Vec<usize> = starts
            .iter()
            .zip(starts.iter().skip(1).chain(once(&remaining_bytes)))
            .map(|(start, next)| next - start)
            .collect();

        let mut buffers: Vec<Buffer> =
            (0..b).map(|_| self.bytes.take_first_block()).collect();
        let last = self.bytes.take_n(end, sizes.into_iter());
        if !last.is_empty() {
            buffers.push(last);
        }

        // First bytes block of every new views block
        self.block_starts = once(0)
            .chain(per_views_block.iter().scan(0, |acc, c| {
                *acc += c;
                Some(*acc)
            }))
            .take(per_views_block.len())
            .collect();
        self.num_bytes_blocks_per_block = per_views_block;

        Self::build(taken_views.into(), buffers, nulls)
    }

    fn take_next_block(&mut self) -> Option<ArrayRef> {
        let views = self.views.take_block()?;
        let nulls = self
            .nulls
            .take_block()
            .expect("nulls have the same blocks as the views");

        // The bytes blocks may be empty when every value of the block is inlined or null
        let count = self
            .num_bytes_blocks_per_block
            .remove(0);
        let buffers = (0..count).map(|_| self.bytes.take_first_block()).collect();

        // The taken bytes blocks shift every remaining block index down
        self.block_starts.remove(0);
        for start in &mut self.block_starts {
            *start -= count;
        }
        if self.num_bytes_blocks_per_block.is_empty() {
            // the bytes builder keeps one open block after everything was taken
            self.num_bytes_blocks_per_block.push(1);
            self.block_starts.push(0);
        }

        Some(Self::build(views.into(), buffers, nulls))
    }

    fn start_new_block(&mut self) {
        self.views.start_new_block();
        self.nulls.start_new_block();
        self.open_views_block();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::StringViewArray;
    use arrow::datatypes::StringViewType;

    type Fixed = ByteViewGroupValueBuilder<true, StringViewType>;
    type Manual = ByteViewGroupValueBuilder<false, StringViewType>;

    const LONG_A: &str = "a long value that is not inline";
    const LONG_B: &str = "another long value, also not inline";

    fn array(values: &[Option<&str>]) -> ArrayRef {
        Arc::new(StringViewArray::from(values.to_vec()))
    }

    fn strings(array: &ArrayRef) -> Vec<Option<String>> {
        array
            .as_string_view()
            .iter()
            .map(|v| v.map(str::to_string))
            .collect()
    }

    fn owned(values: &[Option<&str>]) -> Vec<Option<String>> {
        values.iter().map(|v| v.map(str::to_string)).collect()
    }

    /// Reads every stored row back through `value` / `is_null`
    fn stored<const F: bool>(
        builder: &ByteViewGroupValueBuilder<F, StringViewType>,
        block_size: usize,
    ) -> Vec<Option<String>> {
        (0..builder.len())
            .map(|i| {
                let index = BlocksIndex::from_index_in_fixed_block_size(i, block_size);
                if builder.nulls.is_null(index) {
                    None
                } else {
                    Some(String::from_utf8(builder.value(index).to_vec()).unwrap())
                }
            })
            .collect()
    }

    fn sample() -> Vec<Option<&'static str>> {
        vec![
            Some("a"),
            Some(LONG_A),
            None,
            Some("exactly12byt"),
            Some(""),
            Some(LONG_B),
            Some("b"),
            None,
            Some(LONG_A),
        ]
    }

    fn fixed_with(block_size: usize, values: &[Option<&str>]) -> Fixed {
        let mut builder = Fixed::new(block_size).with_max_block_size(40);
        for row in 0..values.len() {
            builder.append_val(&array(values), row).unwrap();
        }
        assert_eq!(builder.len(), values.len());
        builder
    }

    #[test]
    fn append_and_take_blocks() {
        let values = sample();
        let mut builder = fixed_with(4, &values);
        assert_eq!(builder.batch_size(), 4);
        assert_eq!(stored(&builder, 4), owned(&values));
        // one bytes block per views block, both counting the open one
        assert_eq!(builder.views.num_blocks(), 3);
        assert_eq!(builder.bytes.num_blocks(), 3);

        let input = array(&values);
        for (row, _) in values.iter().enumerate() {
            for (other, _) in values.iter().enumerate() {
                let index = BlocksIndex::from_index_in_fixed_block_size(row, 4);
                assert_eq!(
                    builder.equal_to(index, &input, other),
                    values[row] == values[other],
                    "row {row} vs {other}"
                );
            }
        }

        let mut blocks = vec![];
        while let Some(block) = builder.take_next_block() {
            blocks.push(strings(&block));
        }
        let expected: Vec<Vec<Option<String>>> =
            values.chunks(4).map(owned).collect();
        assert_eq!(blocks, expected);
        assert_eq!(builder.len(), 0);

        // usable after everything was taken
        builder.append_val(&array(&[Some(LONG_B)]), 0).unwrap();
        assert_eq!(stored(&builder, 4), owned(&[Some(LONG_B)]));
    }

    #[test]
    fn take_block_then_append_keeps_lookups_valid() {
        let values = sample();
        let mut builder = fixed_with(3, &values);
        assert_eq!(strings(&builder.take_next_block().unwrap()), owned(&values[..3]));
        assert_eq!(stored(&builder, 3), owned(&values[3..]));

        builder.vectorized_append(&array(&[Some(LONG_B), Some("c"), None]), &[0, 1, 2]).unwrap();
        let mut expected = owned(&values[3..]);
        expected.extend(owned(&[Some(LONG_B), Some("c"), None]));
        assert_eq!(stored(&builder, 3), expected);

        let blocks = Box::new(builder).take_all();
        let all: Vec<Option<String>> = blocks.iter().flat_map(strings).collect();
        assert_eq!(all, expected);
    }

    #[test]
    fn vectorized_append_special_cases() {
        let mut builder = Fixed::new(3);

        let all_nulls = array(&[None, None, None, None]);
        builder.vectorized_append(&all_nulls, &[0, 1, 2, 3]).unwrap();

        let inline_only = array(&[Some("x"), Some("y")]);
        builder.vectorized_append(&inline_only, &[1, 0]).unwrap();

        let with_long = array(&[Some(LONG_A), Some("z")]);
        builder.vectorized_append(&with_long, &[0, 1]).unwrap();

        let expected = owned(&[None, None, None, None, Some("y"), Some("x"), Some(LONG_A), Some("z")]);
        assert_eq!(stored(&builder, 3), expected);

        let mut results = BooleanBufferBuilder::new(2);
        results.append_n(2, true);
        builder.vectorized_equal_to(
            &[
                BlocksIndex::from_index_in_fixed_block_size(6, 3),
                BlocksIndex::from_index_in_fixed_block_size(7, 3),
            ],
            &with_long,
            &[0, 0],
            &mut results,
        );
        assert!(results.get_bit(0));
        assert!(!results.get_bit(1));

        let blocks = Box::new(builder).take_all();
        let all: Vec<Option<String>> = blocks.iter().flat_map(strings).collect();
        assert_eq!(all, expected);
    }

    #[test]
    fn taken_blocks_own_exactly_their_bytes() {
        // long values only, 4 per views block
        let values: Vec<String> = (0..12).map(|i| format!("long value number {i:02}")).collect();
        let values: Vec<Option<&str>> = values.iter().map(|v| Some(v.as_str())).collect();
        let value_len = values[0].unwrap().len();
        // two values per bytes block, so two bytes blocks per views block
        let mut builder = Fixed::new(4).with_max_block_size(2 * value_len + 1);
        let input = array(&values);
        for row in 0..values.len() {
            builder.append_val(&input, row).unwrap();
        }
        // 3 full views blocks with 2 bytes blocks each plus the open one
        assert_eq!(builder.bytes.num_blocks(), 7);
        assert_eq!(builder.num_bytes_blocks_per_block, Vec::from(vec![2, 2, 2, 1]));
        assert_eq!(builder.block_starts, Vec::from(vec![0, 2, 4, 6]));
        assert_eq!(builder.bytes.len(), 12 * value_len);
        let size_before = builder.size();

        // a taken block owns its two bytes blocks and nothing else
        let block = builder.take_next_block().unwrap();
        assert_eq!(strings(&block), owned(&values[..4]));
        let buffers = block.as_string_view().data_buffers();
        assert_eq!(buffers.len(), 2);
        assert_eq!(buffers[0].len() + buffers[1].len(), 4 * value_len);
        assert_eq!(builder.bytes.num_blocks(), 5);
        assert_eq!(builder.block_starts, Vec::from(vec![0, 2, 4]));
        assert_eq!(builder.bytes.len(), 8 * value_len);
        // mapped pages are released at page granularity, never grow
        assert!(builder.size() <= size_before);
        assert_eq!(stored(&builder, 4), owned(&values[4..]));

        // taking values releases exactly their bytes and keeps the rest addressable
        let taken = builder.take_n(1);
        assert_eq!(strings(&taken), owned(&values[4..5]));
        assert_eq!(taken.as_string_view().data_buffers()[0].len(), value_len);
        assert_eq!(builder.bytes.len(), 7 * value_len);
        // the old bytes block boundary after value 5 is kept, value 8 moved into block 0
        assert_eq!(builder.num_bytes_blocks_per_block, Vec::from(vec![3, 2]));
        assert_eq!(stored(&builder, 4), owned(&values[5..]));
        let taken = builder.take_n(1);
        assert_eq!(strings(&taken), owned(&values[5..6]));
        assert_eq!(stored(&builder, 4), owned(&values[6..]));
        for (row, value) in values.iter().enumerate().skip(6) {
            let index = BlocksIndex::from_index_in_fixed_block_size(row - 6, 4);
            assert!(builder.equal_to(index, &input, row), "{value:?}");
        }

        let taken = builder.take_n(3);
        assert_eq!(strings(&taken), owned(&values[6..9]));
        let blocks = Box::new(builder).take_all();
        let all: Vec<Option<String>> = blocks.iter().flat_map(strings).collect();
        assert_eq!(all, owned(&values[9..]));
    }

    #[test]
    fn inline_only_block_has_no_data_buffer() {
        let values = [Some("a"), Some("bb"), None, Some("exactly12byt")];
        let mut builder = fixed_with(4, &values);
        let block = builder.take_next_block().unwrap();
        assert_eq!(strings(&block), owned(&values));
        assert!(block.as_string_view().data_buffers().is_empty());
        assert!(builder.take_next_block().is_none());
    }

    #[test]
    fn take_n_shifts_remaining_values() {
        let values = sample();
        let mut builder = fixed_with(4, &values);

        let taken = builder.take_n(3);
        assert_eq!(strings(&taken), owned(&values[..3]));
        assert_eq!(builder.len(), 6);
        assert_eq!(stored(&builder, 4), owned(&values[3..]));

        // still comparable and appendable after the shift
        let input = array(&values);
        assert!(builder.equal_to(BlocksIndex::from_index_in_fixed_block_size(2, 4), &input, 5));
        assert!(!builder.equal_to(BlocksIndex::from_index_in_fixed_block_size(2, 4), &input, 1));
        builder.append_val(&input, 0).unwrap();

        let taken = builder.take_n(0);
        assert_eq!(taken.len(), 0);

        let blocks = Box::new(builder).take_all();
        let all: Vec<Option<String>> = blocks.iter().flat_map(strings).collect();
        let mut expected = owned(&values[3..]);
        expected.push(Some("a".to_string()));
        assert_eq!(all, expected);
    }

    #[test]
    fn values_preserving_selection() {
        let values = sample();
        let builder = fixed_with(4, &values);

        let all = builder
            .values_preserving(BlockedGroupSelection::all(values.len(), 4))
            .unwrap();
        assert_eq!(strings(&all), owned(&values));

        let indices: Vec<BlocksIndex> = [8, 2, 0, 5, 5]
            .iter()
            .map(|&i| BlocksIndex::from_index_in_fixed_block_size(i, 4))
            .collect();
        let selection =
            BlockedGroupSelection::try_from_indices(&indices, values.len(), 4).unwrap();
        let selected = builder.values_preserving(selection).unwrap();
        assert_eq!(
            strings(&selected),
            owned(&[Some(LONG_A), None, Some("a"), Some(LONG_B), Some(LONG_B)])
        );

        let empty = builder
            .values_preserving(BlockedGroupSelection::try_from_indices(&[], values.len(), 4).unwrap())
            .unwrap();
        assert_eq!(empty.len(), 0);

        // preserving did not change the builder
        assert_eq!(stored(&builder, 4), owned(&values));
    }

    #[test]
    fn manual_blocks() {
        let values = sample();
        let mut builder = Manual::new(0).with_max_block_size(40);
        let input = array(&values);
        for row in 0..4 {
            builder.append_val(&input, row).unwrap();
        }
        builder.start_new_block();
        builder.vectorized_append(&input, &[4, 5, 6, 7, 8]).unwrap();
        assert_eq!(builder.views.num_blocks(), 2);

        // manual sizing: the index is flat over the items, block 0 holds 4
        assert!(builder.equal_to(BlocksIndex::new_in_first_block(5), &input, 5));
        assert!(builder.equal_to(BlocksIndex::new_in_first_block(8), &input, 1));
        assert_eq!(
            String::from_utf8(builder.value(BlocksIndex::new_in_first_block(1)).to_vec()).unwrap(),
            LONG_A
        );

        let blocks = Box::new(builder).take_all();
        let blocks: Vec<Vec<Option<String>>> = blocks.iter().map(strings).collect();
        assert_eq!(blocks, vec![owned(&values[..4]), owned(&values[4..])]);
    }
}
