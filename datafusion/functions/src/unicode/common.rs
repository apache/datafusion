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
    Array, ArrayRef, ByteView, GenericStringArray, Int64Array, OffsetSizeTrait,
    StringViewArray, make_view,
};
use arrow::datatypes::DataType;
use arrow_buffer::{NullBuffer, ScalarBuffer};
use datafusion_common::Result;
use datafusion_common::ScalarValue;
use datafusion_common::cast::{
    as_generic_string_array, as_int64_array, as_string_view_array,
};
use datafusion_common::exec_err;
use datafusion_expr::ColumnarValue;
use std::cmp::Ordering;
use std::ops::Range;
use std::sync::Arc;

/// If `cv` is a non-null scalar string, return its value.
pub(crate) fn try_as_scalar_str(cv: &ColumnarValue) -> Option<&str> {
    match cv {
        ColumnarValue::Scalar(s) => s.try_as_str().flatten(),
        _ => None,
    }
}

/// If `cv` is a non-null scalar Int64, return its value.
pub(crate) fn try_as_scalar_i64(cv: &ColumnarValue) -> Option<i64> {
    match cv {
        ColumnarValue::Scalar(ScalarValue::Int64(v)) => *v,
        _ => None,
    }
}

/// A trait for `left` and `right` byte slicing operations
pub(crate) trait LeftRightSlicer {
    fn slice(string: &str, n: i64) -> Range<usize>;
}

pub(crate) struct LeftSlicer {}

impl LeftRightSlicer for LeftSlicer {
    fn slice(string: &str, n: i64) -> Range<usize> {
        0..left_right_byte_length(string, n)
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

/// Returns the byte offset of the `n`th codepoint in `string`,
/// or `string.len()` if the string has fewer than `n` codepoints.
#[inline]
pub(crate) fn byte_offset_of_char(string: &str, n: usize) -> usize {
    string
        .char_indices()
        .nth(n)
        .map_or(string.len(), |(i, _)| i)
}

/// If `string` has more than `n` codepoints, returns the byte offset of
/// the `n`-th codepoint boundary. Otherwise returns the total codepoint count.
#[inline]
pub(crate) fn char_count_or_boundary(string: &str, n: usize) -> StringCharLen {
    let mut count = 0;
    for (byte_idx, _) in string.char_indices() {
        if count == n {
            return StringCharLen::ByteOffset(byte_idx);
        }
        count += 1;
    }
    StringCharLen::CharCount(count)
}

/// Result of [`char_count_or_boundary`].
pub(crate) enum StringCharLen {
    /// The string has more than `n` codepoints; contains the byte offset
    /// at the `n`-th codepoint boundary.
    ByteOffset(usize),
    /// The string has `n` or fewer codepoints; contains the exact count.
    CharCount(usize),
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
        Ordering::Greater => {
            byte_offset_of_char(string, n.unsigned_abs().min(usize::MAX as u64) as usize)
        }
    }
}

/// General implementation for `left` and `right` functions
pub(crate) fn general_left_right<F: LeftRightSlicer>(
    args: &[ArrayRef],
) -> Result<ArrayRef> {
    let n_array = as_int64_array(&args[1])?;

    match args[0].data_type() {
        DataType::Utf8 => {
            let string_array = as_generic_string_array::<i32>(&args[0])?;
            general_left_right_array::<i32, F>(string_array, n_array)
        }
        DataType::LargeUtf8 => {
            let string_array = as_generic_string_array::<i64>(&args[0])?;
            general_left_right_array::<i64, F>(string_array, n_array)
        }
        DataType::Utf8View => {
            let string_view_array = as_string_view_array(&args[0])?;
            general_left_right_view::<F>(string_view_array, n_array)
        }
        _ => exec_err!("Not supported"),
    }
}

/// Returns true if all offsets in the array fit in i32, meaning the values
/// buffer can be referenced by StringView's offset field.
fn values_fit_in_i32<T: OffsetSizeTrait>(string_array: &GenericStringArray<T>) -> bool {
    string_array
        .offsets()
        .last()
        .map(|offset| offset.as_usize() <= i32::MAX as usize)
        .unwrap_or(true)
}

/// `left`/`right` for Utf8/LargeUtf8 input.
///
/// When offsets fit in i32, produces a zero-copy `StringViewArray` with views
/// pointing into the input values buffer. Otherwise falls back to building a
/// `StringViewArray` by copying.
fn general_left_right_array<T: OffsetSizeTrait, F: LeftRightSlicer>(
    string_array: &GenericStringArray<T>,
    n_array: &Int64Array,
) -> Result<ArrayRef> {
    if !values_fit_in_i32(string_array) {
        let result = string_array
            .iter()
            .zip(n_array.iter())
            .map(|(string, n)| match (string, n) {
                (Some(string), Some(n)) => Some(&string[F::slice(string, n)]),
                _ => None,
            })
            .collect::<StringViewArray>();
        return Ok(Arc::new(result) as ArrayRef);
    }

    let len = string_array.len();
    let offsets = string_array.value_offsets();
    let nulls = NullBuffer::union(string_array.nulls(), n_array.nulls());

    let mut views_buf = Vec::with_capacity(len);
    let mut has_out_of_line = false;

    for (i, offset) in offsets.iter().enumerate().take(len) {
        if nulls.as_ref().is_some_and(|n| n.is_null(i)) {
            views_buf.push(0);
            continue;
        }

        // SAFETY: we just checked validity above
        let string = unsafe { string_array.value_unchecked(i) };
        let n = n_array.value(i);
        let range = F::slice(string, n);
        let result_bytes = &string.as_bytes()[range.clone()];
        if result_bytes.len() > 12 {
            has_out_of_line = true;
        }

        let buf_offset = offset.as_usize() as u32 + range.start as u32;
        views_buf.push(make_view(result_bytes, 0, buf_offset));
    }

    let views = ScalarBuffer::from(views_buf);
    let data_buffers = if has_out_of_line {
        vec![string_array.values().clone()]
    } else {
        vec![]
    };

    // SAFETY:
    // - Each view is produced by `make_view` with correct bytes and offset
    // - Out-of-line views reference buffer index 0, which is the original
    //   values buffer included in data_buffers when has_out_of_line is true
    // - values_fit_in_i32 guarantees all offsets fit in i32
    unsafe {
        let array = StringViewArray::new_unchecked(views, data_buffers, nulls);
        Ok(Arc::new(array) as ArrayRef)
    }
}

/// `general_left_right` for StringViewArray input.
fn general_left_right_view<F: LeftRightSlicer>(
    string_view_array: &StringViewArray,
    n_array: &Int64Array,
) -> Result<ArrayRef> {
    let views = string_view_array.views();
    let new_nulls = NullBuffer::union(string_view_array.nulls(), n_array.nulls());
    let len = n_array.len();
    let mut has_out_of_line = false;

    let new_views = (0..len)
        .map(|idx| {
            if new_nulls.as_ref().is_some_and(|n| n.is_null(idx)) {
                return 0;
            }

            // SAFETY: we just checked validity above
            let string: &str = unsafe { string_view_array.value_unchecked(idx) };
            let n = n_array.value(idx);

            let range = F::slice(string, n);
            let result_bytes = &string.as_bytes()[range.clone()];
            if result_bytes.len() > 12 {
                has_out_of_line = true;
            }

            let byte_view = ByteView::from(views[idx]);
            let new_offset = byte_view.offset + (range.start as u32);
            make_view(result_bytes, byte_view.buffer_index, new_offset)
        })
        .collect::<Vec<u128>>();

    let views = ScalarBuffer::from(new_views);
    let data_buffers = if has_out_of_line {
        string_view_array.data_buffers().to_vec()
    } else {
        vec![]
    };

    // SAFETY:
    // - Each view is produced by `make_view` with correct bytes and offset
    // - Out-of-line views reuse the original buffer index and adjusted offset
    unsafe {
        let array = StringViewArray::new_unchecked(views, data_buffers, new_nulls);
        Ok(Arc::new(array) as ArrayRef)
    }
}
