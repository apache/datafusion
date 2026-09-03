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
use datafusion_common::utils::proxy::VecDequeAllocExt;
use datafusion_expr_common::blocked_helpers::{BlockedNullsBuilder, CopyItemBlockedVecBuilder};
use datafusion_expr_common::groups_accumulator::{BlockedGroupSelection, BlocksIndex};
use std::collections::VecDeque;
use std::marker::PhantomData;
use std::mem::{replace, size_of};
use std::sync::Arc;

const BYTE_VIEW_MAX_BLOCK_SIZE: usize = 2 * 1024 * 1024;

/// An implementation of [`BlockedGroupColumn`] for binary view and utf8 view types.
///
/// Stores a collection of binary view or utf8 view group values in a buffer
/// whose structure is similar to `GenericByteViewArray`, and we can get benefits:
///
/// 1. Efficient comparison of incoming rows to existing rows
/// 2. Efficient construction of the final output array
/// 3. Efficient `take_n` / `take_next_block`, the views are moved between blocks
///    without touching the bytes since they reference the buffers by an absolute index
pub struct ByteViewGroupValueBuilder<const FIXED_BLOCK_SIZING: bool, B: ByteViewType> {
    /// The views of string values
    ///
    /// If string len <= 12, the view's format will be:
    ///   string(12B) | len(4B)
    ///
    /// If string len > 12, its format will be:
    ///     offset(4B) | buffer_index(4B) | prefix(4B) | len(4B)
    ///
    /// where `buffer_index` is absolute: `completed_base + position in completed`,
    /// or one past the last completed buffer for `in_progress`
    views: CopyItemBlockedVecBuilder<FIXED_BLOCK_SIZING, u128>,

    /// The progressing block
    ///
    /// New values will be inserted into it until its capacity
    /// is not enough(detail can see `max_block_size`).
    in_progress: Vec<u8>,

    /// The completed blocks
    ///
    /// Buffers that no stored view references anymore are dropped from the front
    completed: VecDeque<Buffer>,

    /// The buffer index of `completed[0]`, grows as dropped buffers leave the front
    completed_base: usize,

    /// The max size of `in_progress`
    ///
    /// `in_progress` will be flushed into `completed`, and create new `in_progress`
    /// when found its remaining capacity(`max_block_size` - `len(in_progress)`),
    /// is no enough to store the appended value.
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
            in_progress: Vec::new(),
            completed: VecDeque::new(),
            completed_base: 0,
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

    /// The buffer index views of values in `in_progress` get
    fn in_progress_index(&self) -> usize {
        self.completed_base + self.completed.len()
    }

    /// The bytes of the buffer with the absolute `buffer_index`
    fn buffer(&self, buffer_index: usize) -> &[u8] {
        if buffer_index == self.in_progress_index() {
            &self.in_progress
        } else {
            &self.completed[buffer_index - self.completed_base]
        }
    }

    fn append_null(&mut self) {
        self.nulls.push_null();
        self.views.push(0);
    }

    /// Copies the bytes of a non inlined value and returns the `(buffer_index, offset)`
    /// to reference them from a view
    fn append_bytes(&mut self, value: &[u8]) -> (u32, u32) {
        debug_assert!(value.len() > 12);

        // If current block isn't big enough, flush it and create a new in progress block
        if !self.in_progress.is_empty()
            && self.in_progress.len() + value.len() > self.max_block_size
        {
            self.flush_in_progress();
        }

        let buffer_index = self.in_progress_index() as u32;
        let offset = self.in_progress.len() as u32;
        self.in_progress.extend_from_slice(value);

        (buffer_index, offset)
    }

    fn flush_in_progress(&mut self) {
        let flushed_block = replace(
            &mut self.in_progress,
            Vec::with_capacity(self.max_block_size),
        );
        self.completed.push_back(Buffer::from_vec(flushed_block));
    }

    /// Appends a non null value given as bytes
    fn append_value(&mut self, value: &[u8]) {
        let view = if value.len() <= 12 {
            make_view(value, 0, 0)
        } else {
            let (buffer_index, offset) = self.append_bytes(value);
            make_view(value, buffer_index, offset)
        };
        self.views.push(view);
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
                } else {
                    for &row in rows {
                        self.do_append_val_inner(arr, row);
                    }
                }
            }

            Nulls::All => {
                self.nulls.push_n_nulls(rows.len());
                self.views.push_value_n(0, rows.len());
            }
        }
    }

    /// Appends the non null value at `row`, reusing the prefix of the input view
    fn do_append_val_inner(&mut self, array: &GenericByteViewArray<B>, row: usize) {
        // SAFETY: the caller ensures `row` is valid
        let view = unsafe { *array.views().get_unchecked(row) };

        if (view as u32) <= 12 {
            // Inline value: the view is already self-contained, push as-is
            self.views.push(view);
            return;
        }

        // Non-inline value: copy the bytes and point the view into our own buffers
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
        self.views.push(new_view);
    }

    /// The bytes referenced by a non inlined view
    fn non_inlined_value(&self, view: ByteView) -> &[u8] {
        let offset = view.offset as usize;
        &self.buffer(view.buffer_index as usize)[offset..offset + view.length as usize]
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
        let exist_view = self.views[lhs_row];
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
            let exist_full = self.non_inlined_value(ByteView::from(exist_view));
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
            self.non_inlined_value(byte_view)
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

    /// Builds the array for views that were taken out of `self.views`
    ///
    /// The completed buffers they reference are shared, `in_progress` is only copied
    /// up to the last referenced byte (or flushed when everything in it is referenced),
    /// and the buffer indexes are rebased so the array starts at buffer 0
    fn build_taken(&mut self, mut views: Vec<u128>, nulls: Option<NullBuffer>) -> ArrayRef {
        let mut min_buffer = usize::MAX;
        let mut max_buffer = 0;
        // the end of the referenced bytes in the last referenced buffer
        let mut max_buffer_end = 0;
        for view in views.iter().filter(|view| (**view as u32) > 12) {
            let view = ByteView::from(*view);
            let buffer_index = view.buffer_index as usize;
            let end = (view.offset + view.length) as usize;
            min_buffer = min_buffer.min(buffer_index);
            if buffer_index > max_buffer {
                max_buffer = buffer_index;
                max_buffer_end = end;
            } else if buffer_index == max_buffer {
                max_buffer_end = max_buffer_end.max(end);
            }
        }

        if min_buffer == usize::MAX {
            // Everything inlined
            return Self::build(ScalarBuffer::from(views), Vec::new(), nulls);
        }

        if max_buffer == self.in_progress_index() && max_buffer_end == self.in_progress.len() {
            // Everything in `in_progress` is taken, share it instead of copying it
            self.flush_in_progress();
        }

        let buffers = (min_buffer..=max_buffer)
            .map(|buffer_index| {
                if buffer_index == self.in_progress_index() {
                    Buffer::from(&self.in_progress[..max_buffer_end])
                } else {
                    self.completed[buffer_index - self.completed_base].clone()
                }
            })
            .collect();

        for view in views.iter_mut().filter(|view| (**view as u32) > 12) {
            let mut byte_view = ByteView::from(*view);
            byte_view.buffer_index -= min_buffer as u32;
            *view = byte_view.as_u128();
        }

        Self::build(ScalarBuffer::from(views), buffers, nulls)
    }

    /// Drops the completed buffers that no stored view references anymore
    ///
    /// Values are appended in order so the first non inlined view references the
    /// smallest buffer index
    fn drop_unreferenced_buffers(&mut self) {
        if self.completed.is_empty() {
            return;
        }

        let first_referenced = (0..self.views.num_blocks())
            .flat_map(|block| self.views.block(block).iter())
            .find(|view| (**view as u32) > 12)
            .map(|view| ByteView::from(*view).buffer_index as usize)
            .unwrap_or_else(|| self.in_progress_index());

        let to_drop = first_referenced - self.completed_base;
        self.completed.drain(..to_drop);
        self.completed_base = first_referenced;
    }

    fn build(
        views: ScalarBuffer<u128>,
        buffers: Vec<Buffer>,
        nulls: Option<NullBuffer>,
    ) -> ArrayRef {
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
            + self.in_progress.capacity()
            + self.completed.iter().map(|b| b.capacity()).sum::<usize>()
            + self.completed.allocated_size()
            + size_of::<Self>()
    }

    fn values_preserving(&self, selection: BlockedGroupSelection<'_>) -> Result<ArrayRef> {
        self.values_preserving_inner(selection)
    }

    fn take_all(self: Box<Self>) -> Vec<ArrayRef> {
        let mut this = *self;
        let views_blocks = this.views.take_all();
        let nulls_blocks = this.nulls.take_all();

        let blocks = views_blocks
            .into_iter()
            .zip(nulls_blocks)
            .map(|(views, nulls)| this.build_taken(views, nulls))
            .collect();

        this.completed_base = this.in_progress_index();
        this.completed.clear();
        this.in_progress.clear();

        blocks
    }

    fn take_n(&mut self, n: usize) -> ArrayRef {
        debug_assert!(self.len() >= n);
        let views = self.views.take_n(n, None::<std::iter::Empty<_>>);
        let nulls = self.nulls.take_n(n, None::<std::iter::Empty<_>>);

        let array = self.build_taken(views, nulls);
        self.drop_unreferenced_buffers();
        array
    }

    fn take_next_block(&mut self) -> Option<ArrayRef> {
        let views = self.views.take_block()?;
        let nulls = self
            .nulls
            .take_block()
            .expect("nulls have the same blocks as the views");

        let array = self.build_taken(views, nulls);
        self.drop_unreferenced_buffers();
        Some(array)
    }

    fn start_new_block(&mut self) {
        self.views.start_new_block();
        self.nulls.start_new_block();
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
        // the long values do not fit the tiny max block size together
        assert!(!builder.completed.is_empty());

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
    fn taken_arrays_share_buffers_and_dead_buffers_are_dropped() {
        // long values only, two per data block
        let values: Vec<String> = (0..12).map(|i| format!("long value number {i:02}")).collect();
        let values: Vec<Option<&str>> = values.iter().map(|v| Some(v.as_str())).collect();
        let mut builder = Fixed::new(4).with_max_block_size(45);
        let input = array(&values);
        for row in 0..values.len() {
            builder.append_val(&input, row).unwrap();
        }
        assert_eq!(builder.completed.len(), 5);
        let size_before = builder.size();

        // the first block references the first two completed buffers, both are shared
        let block = builder.take_next_block().unwrap();
        assert_eq!(strings(&block), owned(&values[..4]));
        assert_eq!(block.as_string_view().data_buffers().len(), 2);
        assert_eq!(builder.completed.len(), 3);
        assert_eq!(builder.completed_base, 2);
        assert!(builder.size() < size_before);
        assert_eq!(stored(&builder, 4), owned(&values[4..]));

        // taking one value keeps its buffer alive for the value that shares it
        let taken = builder.take_n(1);
        assert_eq!(strings(&taken), owned(&values[4..5]));
        assert_eq!(builder.completed.len(), 3);
        let taken = builder.take_n(1);
        assert_eq!(strings(&taken), owned(&values[5..6]));
        assert_eq!(builder.completed.len(), 2);
        assert_eq!(stored(&builder, 4), owned(&values[6..]));

        // the last value lives in `in_progress`, taking it does not copy but shares
        let taken = builder.take_n(3);
        assert_eq!(strings(&taken), owned(&values[6..9]));
        let blocks = Box::new(builder).take_all();
        let all: Vec<Option<String>> = blocks.iter().flat_map(strings).collect();
        assert_eq!(all, owned(&values[9..]));
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

        assert!(builder.equal_to(BlocksIndex::new(1, 1), &input, 5));
        assert!(builder.equal_to(BlocksIndex::new(1, 4), &input, 1));
        assert_eq!(
            String::from_utf8(builder.value(BlocksIndex::new(0, 1)).to_vec()).unwrap(),
            LONG_A
        );

        let blocks = Box::new(builder).take_all();
        let blocks: Vec<Vec<Option<String>>> = blocks.iter().map(strings).collect();
        assert_eq!(blocks, vec![owned(&values[..4]), owned(&values[4..])]);
    }
}
