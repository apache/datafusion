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

//! Common utilities for implementing unicode functions

use arrow::array::{
    Array, ArrayAccessor, ArrayIter, ArrayRef, ByteView, GenericStringArray, Int64Array,
    OffsetSizeTrait, StringViewArray, make_view,
};
use arrow::datatypes::DataType;
use arrow_buffer::{NullBuffer, ScalarBuffer};
use datafusion_common::cast::{
    as_generic_string_array, as_int64_array, as_string_view_array,
};
use datafusion_common::exec_err;
use std::cmp::Ordering;
use std::ops::Range;
use std::sync::Arc;

/// A trait for `left` and `right` byte slicing operations
pub(crate) trait LeftRightSlicer {
    fn slice(string: &str, n: i64) -> Range<usize>;
}

pub(crate) struct LeftSlicer {}

impl LeftRightSlicer for LeftSlicer {
    fn slice(string: &str, n: i64) -> Range<usize> {
        if n == 0 {
            // Return nothing for `n=0`
            0..0
        } else {
            0..left_right_byte_length(string, n)
        }
    }
}

pub(crate) struct RightSlicer {}

impl LeftRightSlicer for RightSlicer {
    fn slice(string: &str, n: i64) -> Range<usize> {
        if n == 0 {
            // Return nothing for `n=0`
            0..0
        } else if n == i64::MIN {
            // Special case for i64::MIN overflow
            0..0
        } else {
            left_right_byte_length(string, -n)..string.len()
        }
    }
}

/// Calculate the byte length of the substring of `n` chars from string `string`
#[inline]
fn left_right_byte_length(string: &str, n: i64) -> usize {
    match n.cmp(&0) {
        Ordering::Less => string
            .char_indices()
            .nth_back((n.unsigned_abs().min(usize::MAX as u64) - 1) as usize)
            .map(|(index, _)| index)
            .unwrap_or(0),
        Ordering::Equal => 0,
        Ordering::Greater => string
            .char_indices()
            .nth(n.unsigned_abs().min(usize::MAX as u64) as usize)
            .map(|(index, _)| index)
            .unwrap_or(string.len()),
    }
}

/// General implementation for `left` and `right` functions
pub(crate) fn general_left_right<F: LeftRightSlicer>(
    args: &[ArrayRef],
) -> datafusion_common::Result<ArrayRef> {
    let n_array = as_int64_array(&args[1])?;

    match args[0].data_type() {
        DataType::Utf8 => {
            let string_array = as_generic_string_array::<i32>(&args[0])?;
            general_left_right_array::<i32, _, F>(string_array, n_array)
        }
        DataType::LargeUtf8 => {
            let string_array = as_generic_string_array::<i64>(&args[0])?;
            general_left_right_array::<i64, _, F>(string_array, n_array)
        }
        DataType::Utf8View => {
            let string_view_array = as_string_view_array(&args[0])?;
            general_left_right_view::<F>(string_view_array, n_array)
        }
        _ => exec_err!("Not supported"),
    }
}

/// `general_left_right` implementation for strings
fn general_left_right_array<
    'a,
    T: OffsetSizeTrait,
    V: ArrayAccessor<Item = &'a str>,
    F: LeftRightSlicer,
>(
    string_array: V,
    n_array: &Int64Array,
) -> datafusion_common::Result<ArrayRef> {
    let iter = ArrayIter::new(string_array);
    let result = iter
        .zip(n_array.iter())
        .map(|(string, n)| match (string, n) {
            (Some(string), Some(n)) => {
                let range = F::slice(string, n);
                // Extract a given range from a byte-indexed slice
                Some(&string[range])
            }
            _ => None,
        })
        .collect::<GenericStringArray<T>>();

    Ok(Arc::new(result) as ArrayRef)
}

/// `general_left_right` implementation for StringViewArray
fn general_left_right_view<F: LeftRightSlicer>(
    string_view_array: &StringViewArray,
    n_array: &Int64Array,
) -> datafusion_common::Result<ArrayRef> {
    let len = n_array.len();

    let views = string_view_array.views();
    // Every string in StringViewArray has one corresponding view in `views`
    debug_assert!(views.len() == string_view_array.len());

    // Compose null buffer at once
    let string_nulls = string_view_array.nulls();
    let n_nulls = n_array.nulls();
    let new_nulls = NullBuffer::union(string_nulls, n_nulls);

    let new_views = (0..len)
        .map(|idx| {
            let view = views[idx];

            let is_valid = match &new_nulls {
                Some(nulls_buf) => nulls_buf.is_valid(idx),
                None => true,
            };

            if is_valid {
                let string: &str = string_view_array.value(idx);
                let n = n_array.value(idx);

                // Input string comes from StringViewArray, so it should fit in 32-bit length
                let range = F::slice(string, n);
                let result_bytes = &string.as_bytes()[range.clone()];

                let byte_view = ByteView::from(view);
                // New offsets starts at 0 for left, and at `range.start` for right,
                // which is encoded in the given range
                let new_offset = byte_view.offset + (range.start as u32);
                // Reuse buffer
                make_view(result_bytes, byte_view.buffer_index, new_offset)
            } else {
                // For nulls, keep the original view
                view
            }
        })
        .collect::<Vec<u128>>();

    // Buffers are unchanged
    let result = StringViewArray::try_new(
        ScalarBuffer::from(new_views),
        Vec::from(string_view_array.data_buffers()),
        new_nulls,
    )?;
    Ok(Arc::new(result) as ArrayRef)
}
