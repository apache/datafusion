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

use std::{marker::PhantomData, sync::Arc};

use arrow::{
    array::{AsArray, ByteView},
    compute::SlicesIterator,
    datatypes::ByteViewType,
};
use arrow_array::{Array, ArrayRef, GenericByteViewArray};
use arrow_buffer::{Buffer, ScalarBuffer};
use datafusion_common::Result;

use super::{
    FilterCoalescer, FilterPredicate, IndexIterator, IterationStrategy,
    MaybeNullBufferBuilder,
};

const BYTE_VIEW_MAX_BLOCK_SIZE: usize = 2 * 1024 * 1024;

pub struct ByteViewFilterBuilder<B: ByteViewType> {
    /// The views of string values
    ///
    /// If string len <= 12, the view's format will be:
    ///   string(12B) | len(4B)
    ///
    /// If string len > 12, its format will be:
    ///     offset(4B) | buffer_index(4B) | prefix(4B) | len(4B)
    views: Vec<u128>,

    /// The progressing block
    ///
    /// New values will be inserted into it until its capacity
    /// is not enough(detail can see `max_block_size`).
    in_progress: Vec<u8>,

    /// The completed blocks
    completed: Vec<Buffer>,

    /// The max size of `in_progress`
    ///
    /// `in_progress` will be flushed into `completed`, and create new `in_progress`
    /// when found its remaining capacity(`max_block_size` - `len(in_progress)`),
    /// is no enough to store the appended value.
    ///
    /// Currently it is fixed at 2MB.
    max_block_size: usize,

    /// Nulls
    nulls: MaybeNullBufferBuilder,

    /// phantom data so the type requires `<B>`
    _phantom: PhantomData<B>,
}

impl<B: ByteViewType> ByteViewFilterBuilder<B> {
    pub fn new() -> Self {
        Self {
            views: Vec::new(),
            in_progress: Vec::new(),
            completed: Vec::new(),
            max_block_size: BYTE_VIEW_MAX_BLOCK_SIZE,
            nulls: MaybeNullBufferBuilder::new(),
            _phantom: PhantomData {},
        }
    }

    fn do_append_val_inner(&mut self, array: &GenericByteViewArray<B>, row: usize, view: u128)
    where
        B: ByteViewType,
    {
        let value_len = view as u32;
        if value_len <= 12 {
            self.views.push(view);
        } else {
            // Ensure big enough block to hold the value first
            self.ensure_in_progress_big_enough(value_len as usize);

            // Append value
            let buffer_index = self.completed.len();
            let offset = self.in_progress.len();
            self.in_progress.extend_from_slice(array.value(row).as_ref());

            let view = make_view_for_non_inline_string(array.value(row).as_ref(), buffer_index as u32, offset as u32);
            self.views.push(view);
        }
    }

    fn ensure_in_progress_big_enough(&mut self, value_len: usize) {
        debug_assert!(value_len > 12);
        let require_cap = self.in_progress.len() + value_len;

        // If current block isn't big enough, flush it and create a new in progress block
        if require_cap > self.max_block_size {
            let flushed_block = std::mem::replace(
                &mut self.in_progress,
                Vec::with_capacity(self.max_block_size),
            );
            let buffer = Buffer::from_vec(flushed_block);
            self.completed.push(buffer);
        }
    }

    // append if all the rows are null
    fn append_nulls(&mut self, arr_len:usize, predicate: &FilterPredicate) {
        match &predicate.strategy {
            IterationStrategy::SlicesIterator => {
                for (start, end) in SlicesIterator::new(&predicate.filter) {
                    self.nulls.append_n(end - start, true);
                    self.views.extend(std::iter::repeat(0).take(end - start));
                }
            }
            IterationStrategy::Slices(slices) => {
                for (start, end) in slices {
                    let len = *end - *start;
                    self.nulls.append_n(len, true);
                    self.views.extend(std::iter::repeat(0).take(len));
                }
            }
            IterationStrategy::IndexIterator => {
                self.nulls.append_n(predicate.count, true);
                self.views.extend(std::iter::repeat(0).take(predicate.count));
            }
            IterationStrategy::Indices(indices) => {
                self.nulls.append_n(indices.len(), true);
                self.views.extend(std::iter::repeat(0).take(indices.len()));
            }
            IterationStrategy::None => {}
            IterationStrategy::All => {
                self.nulls.append_n(arr_len, true);
                self.views.extend(std::iter::repeat(0).take(arr_len));
            }
        }
    }

    // append if all the rows are non null
    fn append_non_nulls(&mut self, arr: &GenericByteViewArray<B>, arr_len:usize, predicate: &FilterPredicate) {
        let views = arr.views();
        match &predicate.strategy {
            IterationStrategy::SlicesIterator => {
                for (start, end) in SlicesIterator::new(&predicate.filter) {
                    self.nulls.append_n(end - start, false);
                    for row in start..end {
                        self.do_append_val_inner(arr, row, views[row]);
                    }
                }
            }
            IterationStrategy::Slices(slices) => {
                for (start, end) in slices {
                    let len = *end - *start;
                    self.nulls.append_n(len, false);
                    for row in *start..*end {
                        self.do_append_val_inner(arr, row, views[row]);
                    }
                }
            }
            IterationStrategy::IndexIterator => {
                self.nulls.append_n(predicate.count, false);
                for row in IndexIterator::new(&predicate.filter, predicate.count) {
                    self.do_append_val_inner(arr, row, views[row]);
                }
            }
            IterationStrategy::Indices(indices) => {
                self.nulls.append_n(indices.len(), false);
                for row in indices.iter() {
                    self.do_append_val_inner(arr, *row, views[*row]);
                }
            }
            IterationStrategy::None => {}
            IterationStrategy::All => {
                self.nulls.append_n(arr_len, false);
                for row in 0..arr_len {
                    self.do_append_val_inner(arr, row, views[row]);
                }
            }
        }
    }
}

impl<B: ByteViewType> FilterCoalescer for ByteViewFilterBuilder<B> {
    fn append_filtered_array(
        &mut self,
        array: &ArrayRef,
        predicate: &FilterPredicate,
    ) -> Result<()> {
        let arr = array.as_byte_view::<B>();
        let views = arr.views();
        if arr.null_count() == arr.len() {
            self.append_nulls(arr.len(), predicate);
            return Ok(());
        }
        if arr.null_count() == 0 {
            self.append_non_nulls(arr, arr.len(), predicate);
            return Ok(());
        }

        match &predicate.strategy {
            IterationStrategy::SlicesIterator => {
                for (start, end) in SlicesIterator::new(&predicate.filter) {
                    for row in start..end {
                        if arr.is_null(row) {
                            self.nulls.append(true);
                            self.views.push(0);
                        } else {
                            self.nulls.append(false);
                            self.do_append_val_inner(arr, row, views[row]);
                        }
                    }
                }
            }
            IterationStrategy::Slices(slices) => {
                for (start, end) in slices {
                    for row in *start..*end {
                        if arr.is_null(row) {
                            self.nulls.append(true);
                            self.views.push(0);
                        } else {
                            self.nulls.append(false);
                            self.do_append_val_inner(arr, row, views[row]);
                        }
                    }
                }
            }
            IterationStrategy::IndexIterator => {
                for row in IndexIterator::new(&predicate.filter, predicate.count) {
                    if arr.is_null(row) {
                        self.nulls.append(true);
                        self.views.push(0);
                    } else {
                        self.nulls.append(false);
                        self.do_append_val_inner(arr, row, views[row]);
                    }
                }
            }
            IterationStrategy::Indices(indices) => {
                for row in indices.iter() {
                    let row = *row;
                    if arr.is_null(row) {
                        self.nulls.append(true);
                        self.views.push(0);
                    } else {
                        self.nulls.append(false);
                        self.do_append_val_inner(arr, row, views[row]);
                    }
                }
            }
            IterationStrategy::None => {}
            IterationStrategy::All => {
                for row in 0..arr.len() {
                    if arr.is_null(row) {
                        self.nulls.append(true);
                        self.views.push(0);
                    } else {
                        self.nulls.append(false);
                        self.do_append_val_inner(arr, row, views[row]);
                    }
                }
            }
        }

        Ok(())
    }


    fn row_count(&self) -> usize {
        self.views.len()
    }

    fn build(self: Box<Self>) -> ArrayRef {
        let Self {
            views,
            in_progress,
            mut completed,
            nulls,
            ..
        } = *self;

        // Build nulls
        let null_buffer = nulls.build();

        // Build values
        // Flush `in_process` firstly
        if !in_progress.is_empty() {
            let buffer = Buffer::from(in_progress);
            completed.push(buffer);
        }

        let views = ScalarBuffer::from(views);

        // Safety:
        // * all views were correctly made
        // * (if utf8): Input was valid Utf8 so buffer contents are
        // valid utf8 as well
        unsafe {
            Arc::new(GenericByteViewArray::<B>::new_unchecked(
                views,
                completed,
                null_buffer,
            ))
        }
    }
}

#[inline(never)]
pub fn make_view_for_non_inline_string(data: &[u8], block_id: u32, offset: u32) -> u128 {
    let len = data.len();
    let view = ByteView {
        length: len as u32,
        prefix: u32::from_le_bytes(data[0..4].try_into().unwrap()),
        buffer_index: block_id,
        offset,
    };
    view.as_u128()
}