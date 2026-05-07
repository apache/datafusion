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

//! Common utilities for implementing string functions

use std::sync::Arc;

use crate::strings::{
    GenericStringArrayBuilder, STRING_VIEW_INIT_BLOCK_SIZE, STRING_VIEW_MAX_BLOCK_SIZE,
    StringViewArrayBuilder, append_view,
};
use arrow::array::{
    Array, ArrayRef, GenericStringArray, NullBufferBuilder, OffsetSizeTrait,
    StringViewArray, new_null_array,
};
use arrow::buffer::{Buffer, OffsetBuffer, ScalarBuffer};
use arrow::datatypes::DataType;
use datafusion_common::Result;
use datafusion_common::cast::{as_generic_string_array, as_string_view_array};
use datafusion_common::{ScalarValue, exec_err};
use datafusion_expr::ColumnarValue;

/// Trait for trim operations, allowing compile-time dispatch instead of runtime matching.
///
/// Each implementation performs its specific trim operation and returns
/// (trimmed_str, start_offset) where start_offset is the byte offset
/// from the beginning of the input string where the trimmed result starts.
pub(crate) trait Trimmer {
    fn trim<'a>(input: &'a str, pattern: &[char]) -> (&'a str, u32);

    /// Optimized trim for a single ASCII byte.
    /// Uses byte-level scanning instead of char-level iteration.
    fn trim_ascii_char(input: &str, byte: u8) -> (&str, u32);
}

/// Returns the number of leading bytes matching `byte`
#[inline]
fn leading_bytes(bytes: &[u8], byte: u8) -> usize {
    bytes.iter().take_while(|&&b| b == byte).count()
}

/// Returns the number of trailing bytes matching `byte`
#[inline]
fn trailing_bytes(bytes: &[u8], byte: u8) -> usize {
    bytes.iter().rev().take_while(|&&b| b == byte).count()
}

/// Left trim - removes leading characters
pub(crate) struct TrimLeft;

impl Trimmer for TrimLeft {
    #[inline]
    fn trim<'a>(input: &'a str, pattern: &[char]) -> (&'a str, u32) {
        if pattern.len() == 1 && pattern[0].is_ascii() {
            return Self::trim_ascii_char(input, pattern[0] as u8);
        }
        let trimmed = input.trim_start_matches(pattern);
        let offset = (input.len() - trimmed.len()) as u32;
        (trimmed, offset)
    }

    #[inline]
    fn trim_ascii_char(input: &str, byte: u8) -> (&str, u32) {
        let start = leading_bytes(input.as_bytes(), byte);
        (&input[start..], start as u32)
    }
}

/// Right trim - removes trailing characters
pub(crate) struct TrimRight;

impl Trimmer for TrimRight {
    #[inline]
    fn trim<'a>(input: &'a str, pattern: &[char]) -> (&'a str, u32) {
        if pattern.len() == 1 && pattern[0].is_ascii() {
            return Self::trim_ascii_char(input, pattern[0] as u8);
        }
        let trimmed = input.trim_end_matches(pattern);
        (trimmed, 0)
    }

    #[inline]
    fn trim_ascii_char(input: &str, byte: u8) -> (&str, u32) {
        let bytes = input.as_bytes();
        let end = bytes.len() - trailing_bytes(bytes, byte);
        (&input[..end], 0)
    }
}

/// Both trim - removes both leading and trailing characters
pub(crate) struct TrimBoth;

impl Trimmer for TrimBoth {
    #[inline]
    fn trim<'a>(input: &'a str, pattern: &[char]) -> (&'a str, u32) {
        if pattern.len() == 1 && pattern[0].is_ascii() {
            return Self::trim_ascii_char(input, pattern[0] as u8);
        }
        let left_trimmed = input.trim_start_matches(pattern);
        let offset = (input.len() - left_trimmed.len()) as u32;
        let trimmed = left_trimmed.trim_end_matches(pattern);
        (trimmed, offset)
    }

    #[inline]
    fn trim_ascii_char(input: &str, byte: u8) -> (&str, u32) {
        let bytes = input.as_bytes();
        let start = leading_bytes(bytes, byte);
        let end = bytes.len() - trailing_bytes(&bytes[start..], byte);
        (&input[start..end], start as u32)
    }
}

pub(crate) fn general_trim<T: OffsetSizeTrait, Tr: Trimmer>(
    args: &[ArrayRef],
    use_string_view: bool,
) -> Result<ArrayRef> {
    if use_string_view {
        string_view_trim::<Tr>(args)
    } else {
        string_trim::<T, Tr>(args)
    }
}

/// Applies the trim function to the given string view array(s)
/// and returns a new string view array with the trimmed values.
///
/// Pre-computes the pattern characters once for scalar patterns to avoid
/// repeated allocations per row.
fn string_view_trim<Tr: Trimmer>(args: &[ArrayRef]) -> Result<ArrayRef> {
    let string_view_array = as_string_view_array(&args[0])?;
    let mut views_buf = Vec::with_capacity(string_view_array.len());
    let mut null_builder = NullBufferBuilder::new(string_view_array.len());

    match args.len() {
        1 => {
            // Trim spaces by default
            for (src_str_opt, raw_view) in string_view_array
                .iter()
                .zip(string_view_array.views().iter())
            {
                if let Some(src_str) = src_str_opt {
                    let (trimmed, offset) = Tr::trim_ascii_char(src_str, b' ');
                    append_view(&mut views_buf, raw_view, trimmed, offset);
                    null_builder.append_non_null();
                } else {
                    null_builder.append_null();
                    views_buf.push(0);
                }
            }
        }
        2 => {
            let characters_array = as_string_view_array(&args[1])?;

            if characters_array.len() == 1 {
                // Scalar pattern - pre-compute pattern chars once
                if characters_array.is_null(0) {
                    return Ok(new_null_array(
                        &DataType::Utf8View,
                        string_view_array.len(),
                    ));
                }

                let pattern: Vec<char> = characters_array.value(0).chars().collect();
                for (src_str_opt, raw_view) in string_view_array
                    .iter()
                    .zip(string_view_array.views().iter())
                {
                    trim_and_append_view::<Tr>(
                        src_str_opt,
                        &pattern,
                        &mut views_buf,
                        &mut null_builder,
                        raw_view,
                    );
                }
            } else {
                // Per-row pattern - must compute pattern chars for each row
                let mut pattern: Vec<char> = Vec::new();
                for ((src_str_opt, raw_view), characters_opt) in string_view_array
                    .iter()
                    .zip(string_view_array.views().iter())
                    .zip(characters_array.iter())
                {
                    if let (Some(src_str), Some(characters)) =
                        (src_str_opt, characters_opt)
                    {
                        pattern.clear();
                        pattern.extend(characters.chars());
                        let (trimmed, offset) = Tr::trim(src_str, &pattern);
                        append_view(&mut views_buf, raw_view, trimmed, offset);
                        null_builder.append_non_null();
                    } else {
                        null_builder.append_null();
                        views_buf.push(0);
                    }
                }
            }
        }
        other => {
            return exec_err!(
                "Function TRIM was called with {other} arguments. It requires at least 1 and at most 2."
            );
        }
    }

    let views_buf = ScalarBuffer::from(views_buf);
    let nulls_buf = null_builder.finish();

    // Safety:
    // (1) The blocks of the given views are all provided
    // (2) Each of the range `view.offset+start..end` of view in views_buf is within
    // the bounds of each of the blocks
    unsafe {
        let array = StringViewArray::new_unchecked(
            views_buf,
            string_view_array.data_buffers().to_vec(),
            nulls_buf,
        );
        Ok(Arc::new(array) as ArrayRef)
    }
}

/// Trims the given string and appends the trimmed string to the views buffer
/// and the null buffer.
///
/// Arguments
/// - `src_str_opt`: The original string value (represented by the view)
/// - `pattern`: Pre-computed character pattern to trim
/// - `views_buf`: The buffer to append the updated views to
/// - `null_builder`: The buffer to append the null values to
/// - `original_view`: The original view value (that contains src_str_opt)
#[inline]
fn trim_and_append_view<Tr: Trimmer>(
    src_str_opt: Option<&str>,
    pattern: &[char],
    views_buf: &mut Vec<u128>,
    null_builder: &mut NullBufferBuilder,
    original_view: &u128,
) {
    if let Some(src_str) = src_str_opt {
        let (trimmed, offset) = Tr::trim(src_str, pattern);
        append_view(views_buf, original_view, trimmed, offset);
        null_builder.append_non_null();
    } else {
        null_builder.append_null();
        views_buf.push(0);
    }
}

/// Applies the trim function to the given string array(s)
/// and returns a new string array with the trimmed values.
///
/// Pre-computes the pattern characters once for scalar patterns to avoid
/// repeated allocations per row.
fn string_trim<T: OffsetSizeTrait, Tr: Trimmer>(args: &[ArrayRef]) -> Result<ArrayRef> {
    let string_array = as_generic_string_array::<T>(&args[0])?;

    match args.len() {
        1 => {
            // Trim spaces by default
            let result = string_array
                .iter()
                .map(|string| string.map(|s| Tr::trim_ascii_char(s, b' ').0))
                .collect::<GenericStringArray<T>>();

            Ok(Arc::new(result) as ArrayRef)
        }
        2 => {
            let characters_array = as_generic_string_array::<T>(&args[1])?;

            if characters_array.len() == 1 {
                // Scalar pattern - pre-compute pattern chars once
                if characters_array.is_null(0) {
                    return Ok(new_null_array(
                        string_array.data_type(),
                        string_array.len(),
                    ));
                }

                let pattern: Vec<char> = characters_array.value(0).chars().collect();
                let result = string_array
                    .iter()
                    .map(|item| item.map(|s| Tr::trim(s, &pattern).0))
                    .collect::<GenericStringArray<T>>();
                return Ok(Arc::new(result) as ArrayRef);
            }

            // Per-row pattern - must compute pattern chars for each row
            let mut pattern: Vec<char> = Vec::new();
            let result = string_array
                .iter()
                .zip(characters_array.iter())
                .map(|(string, characters)| match (string, characters) {
                    (Some(s), Some(c)) => {
                        pattern.clear();
                        pattern.extend(c.chars());
                        Some(Tr::trim(s, &pattern).0)
                    }
                    _ => None,
                })
                .collect::<GenericStringArray<T>>();

            Ok(Arc::new(result) as ArrayRef)
        }
        other => {
            exec_err!(
                "Function TRIM was called with {other} arguments. It requires at least 1 and at most 2."
            )
        }
    }
}

pub(crate) fn to_lower(args: &[ColumnarValue], name: &str) -> Result<ColumnarValue> {
    case_conversion(args, true, name)
}

pub(crate) fn to_upper(args: &[ColumnarValue], name: &str) -> Result<ColumnarValue> {
    case_conversion(args, false, name)
}

#[inline]
fn unicode_case(s: &str, lower: bool) -> String {
    if lower {
        s.to_lowercase()
    } else {
        s.to_uppercase()
    }
}

fn case_conversion(
    args: &[ColumnarValue],
    lower: bool,
    name: &str,
) -> Result<ColumnarValue> {
    match &args[0] {
        ColumnarValue::Array(array) => match array.data_type() {
            DataType::Utf8 => Ok(ColumnarValue::Array(case_conversion_array::<i32>(
                array, lower,
            )?)),
            DataType::LargeUtf8 => Ok(ColumnarValue::Array(
                case_conversion_array::<i64>(array, lower)?,
            )),
            DataType::Utf8View => {
                let string_array = as_string_view_array(array)?;
                if string_array.is_ascii() {
                    return Ok(ColumnarValue::Array(Arc::new(
                        case_conversion_utf8view_ascii(string_array, lower),
                    )));
                }
                let item_len = string_array.len();
                // Null-preserving: reuse the input null buffer as the output null buffer.
                let nulls = string_array.nulls().cloned();
                let mut builder = StringViewArrayBuilder::with_capacity(item_len);

                if let Some(ref n) = nulls {
                    for i in 0..item_len {
                        if n.is_null(i) {
                            builder.append_placeholder();
                        } else {
                            // SAFETY: `n.is_null(i)` was false in the branch above.
                            let s = unsafe { string_array.value_unchecked(i) };
                            builder.append_value(&unicode_case(s, lower));
                        }
                    }
                } else {
                    for i in 0..item_len {
                        // SAFETY: no null buffer means every index is valid.
                        let s = unsafe { string_array.value_unchecked(i) };
                        builder.append_value(&unicode_case(s, lower));
                    }
                }

                Ok(ColumnarValue::Array(Arc::new(builder.finish(nulls)?)))
            }
            other => exec_err!("Unsupported data type {other:?} for function {name}"),
        },
        ColumnarValue::Scalar(scalar) => match scalar {
            ScalarValue::Utf8(a) => {
                let result = a.as_ref().map(|x| unicode_case(x, lower));
                Ok(ColumnarValue::Scalar(ScalarValue::Utf8(result)))
            }
            ScalarValue::LargeUtf8(a) => {
                let result = a.as_ref().map(|x| unicode_case(x, lower));
                Ok(ColumnarValue::Scalar(ScalarValue::LargeUtf8(result)))
            }
            ScalarValue::Utf8View(a) => {
                let result = a.as_ref().map(|x| unicode_case(x, lower));
                Ok(ColumnarValue::Scalar(ScalarValue::Utf8View(result)))
            }
            other => exec_err!("Unsupported data type {other:?} for function {name}"),
        },
    }
}

fn case_conversion_array<O: OffsetSizeTrait>(
    array: &ArrayRef,
    lower: bool,
) -> Result<ArrayRef> {
    const PRE_ALLOC_BYTES: usize = 8;

    let string_array = as_generic_string_array::<O>(array)?;
    if string_array.is_ascii() {
        return case_conversion_ascii_array::<O>(string_array, lower);
    }

    // Values contain non-ASCII.
    let item_len = string_array.len();
    let offsets = string_array.value_offsets();
    let start = offsets.first().unwrap().as_usize();
    let end = offsets.last().unwrap().as_usize();
    let capacity = (end - start) + PRE_ALLOC_BYTES;
    // Null-preserving: reuse the input null buffer as the output null buffer.
    let nulls = string_array.nulls().cloned();
    let mut builder = GenericStringArrayBuilder::<O>::with_capacity(item_len, capacity);

    if let Some(ref n) = nulls {
        for i in 0..item_len {
            if n.is_null(i) {
                builder.append_placeholder();
            } else {
                // SAFETY: `n.is_null(i)` was false in the branch above.
                let s = unsafe { string_array.value_unchecked(i) };
                builder.append_value(&unicode_case(s, lower));
            }
        }
    } else {
        for i in 0..item_len {
            // SAFETY: no null buffer means every index is valid.
            let s = unsafe { string_array.value_unchecked(i) };
            builder.append_value(&unicode_case(s, lower));
        }
    }
    Ok(Arc::new(builder.finish(nulls)?))
}

/// Fast path for case conversion on an all-ASCII `StringViewArray`.
fn case_conversion_utf8view_ascii(
    array: &StringViewArray,
    lower: bool,
) -> StringViewArray {
    // Specialize per conversion so the byte call inlines in the hot loops below.
    if lower {
        case_conversion_utf8view_ascii_inner(array, u8::to_ascii_lowercase)
    } else {
        case_conversion_utf8view_ascii_inner(array, u8::to_ascii_uppercase)
    }
}

/// Walks the views once and produces a new `StringViewArray` with
/// case-converted bytes. Inline strings (<= 12 bytes) are converted in-place;
/// long strings copy-and-convert into output buffers and have their view fields
/// rewritten to address the new bytes. ASCII case conversion preserves is byte
/// length, so no row migrates between the inline and long layouts.
fn case_conversion_utf8view_ascii_inner<F: Fn(&u8) -> u8>(
    array: &StringViewArray,
    convert: F,
) -> StringViewArray {
    let item_len = array.len();
    let views = array.views();
    let data_buffers = array.data_buffers();
    let nulls = array.nulls();

    let mut new_views: Vec<u128> = Vec::with_capacity(item_len);
    // Long values are packed into `in_progress`; when full it is sealed into
    // `completed` and a new, larger block is started — same block-doubling
    // scheme as Arrow's `GenericByteViewBuilder`.
    let mut in_progress: Vec<u8> = Vec::new();
    let mut completed: Vec<Buffer> = Vec::new();
    let mut block_size: u32 = STRING_VIEW_INIT_BLOCK_SIZE;

    for i in 0..item_len {
        if nulls.is_some_and(|n| n.is_null(i)) {
            // Zero view = empty, no buffer reference; the null buffer is what
            // marks the row null, so the view's value is irrelevant.
            new_views.push(0);
            continue;
        }
        let view = views[i];
        // Length is the low 32 bits; `as u32` discards the rest of the view.
        let len = view as u32 as usize;
        if len == 0 {
            new_views.push(0);
            continue;
        }
        let mut bytes = view.to_le_bytes();
        if len <= 12 {
            // Inline: value is in bytes[4..4+len], no buffer reference. Convert
            // in place; nothing else in the view needs to change.
            for b in &mut bytes[4..4 + len] {
                *b = convert(b);
            }
            new_views.push(u128::from_le_bytes(bytes));
        } else {
            // Long: input view points into shared `data_buffers` we can't
            // mutate, so copy-convert into our own buffer and rewrite the
            // view's prefix/buffer_index/offset (length is preserved).

            // Ensure the current block has room; otherwise flush and grow.
            let required_cap = in_progress.len() + len;
            if in_progress.capacity() < required_cap {
                if !in_progress.is_empty() {
                    completed.push(Buffer::from_vec(std::mem::take(&mut in_progress)));
                }
                if block_size < STRING_VIEW_MAX_BLOCK_SIZE {
                    block_size = block_size.saturating_mul(2);
                }
                let to_reserve = len.max(block_size as usize);
                in_progress.reserve(to_reserve);
            }

            // The in-progress block will be sealed at index `completed.len()`,
            // and our value starts at the current write position within it.
            let buffer_index: u32 = i32::try_from(completed.len())
                .expect("buffer count exceeds i32::MAX")
                as u32;
            let new_offset: u32 =
                i32::try_from(in_progress.len()).expect("offset exceeds i32::MAX") as u32;

            // Source location from the input view: bytes 8..12 are buffer
            // index, bytes 12..16 are the offset within it.
            let src_buffer_index =
                u32::from_le_bytes(bytes[8..12].try_into().unwrap()) as usize;
            let src_offset =
                u32::from_le_bytes(bytes[12..16].try_into().unwrap()) as usize;
            let src =
                &data_buffers[src_buffer_index].as_slice()[src_offset..src_offset + len];

            let prefix_start = in_progress.len();
            in_progress.extend(src.iter().map(&convert));

            // Rewrite the three long-view fields; bytes[0..4] (length) is
            // left untouched. The prefix is read back from the bytes we just
            // wrote so the converted value has a single source of truth.
            let prefix: [u8; 4] = in_progress[prefix_start..prefix_start + 4]
                .try_into()
                .unwrap();
            bytes[4..8].copy_from_slice(&prefix);
            bytes[8..12].copy_from_slice(&buffer_index.to_le_bytes());
            bytes[12..16].copy_from_slice(&new_offset.to_le_bytes());
            new_views.push(u128::from_le_bytes(bytes));
        }
    }

    if !in_progress.is_empty() {
        completed.push(Buffer::from_vec(in_progress));
    }

    // SAFETY: each long view's buffer_index addresses a buffer we wrote, and
    // its offset addresses bytes within that buffer; prefixes were copied from
    // those same bytes; inline views were rewritten from valid inline bytes;
    // null/empty rows are zero views with no buffer reference; row count is
    // unchanged.
    unsafe {
        StringViewArray::new_unchecked(
            ScalarBuffer::from(new_views),
            completed,
            array.nulls().cloned(),
        )
    }
}

/// Fast path for case conversion on an all-ASCII string array. ASCII case
/// conversion is byte-length-preserving, so we can convert the entire addressed
/// byte range in one pass over the value buffer and reuse the offsets and nulls
/// buffers — rebasing the offsets when the input is a sliced array.
fn case_conversion_ascii_array<O: OffsetSizeTrait>(
    string_array: &GenericStringArray<O>,
    lower: bool,
) -> Result<ArrayRef> {
    let value_offsets = string_array.value_offsets();
    let start = value_offsets.first().unwrap().as_usize();
    let end = value_offsets.last().unwrap().as_usize();
    let relevant = &string_array.value_data()[start..end];

    let converted: Vec<u8> = if lower {
        relevant.iter().map(u8::to_ascii_lowercase).collect()
    } else {
        relevant.iter().map(u8::to_ascii_uppercase).collect()
    };
    let values = Buffer::from_vec(converted);

    // Shift offsets from `start`-based to 0-based so they index into `values`.
    let offsets = if start == 0 {
        string_array.offsets().clone()
    } else {
        let s = O::usize_as(start);
        let rebased: Vec<O> = value_offsets.iter().map(|&o| o - s).collect();
        // SAFETY: subtracting a constant from monotonic offsets preserves
        // monotonicity, and `start` is the minimum offset, so no underflow.
        unsafe { OffsetBuffer::new_unchecked(ScalarBuffer::from(rebased)) }
    };

    let nulls = string_array.nulls().cloned();
    // SAFETY: offsets are monotonic and in-bounds for `values`; nulls
    // (if any) match the slice length.
    Ok(Arc::new(unsafe {
        GenericStringArray::<O>::new_unchecked(offsets, values, nulls)
    }))
}
