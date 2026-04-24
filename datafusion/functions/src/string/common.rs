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

use crate::strings::{GenericStringArrayBuilder, StringViewArrayBuilder, append_view};
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
    case_conversion(args, |string| string.to_lowercase(), name)
}

pub(crate) fn to_upper(args: &[ColumnarValue], name: &str) -> Result<ColumnarValue> {
    case_conversion(args, |string| string.to_uppercase(), name)
}

fn case_conversion<'a, F>(
    args: &'a [ColumnarValue],
    op: F,
    name: &str,
) -> Result<ColumnarValue>
where
    F: Fn(&'a str) -> String,
{
    match &args[0] {
        ColumnarValue::Array(array) => match array.data_type() {
            DataType::Utf8 => Ok(ColumnarValue::Array(case_conversion_array::<i32, _>(
                array, op,
            )?)),
            DataType::LargeUtf8 => Ok(ColumnarValue::Array(case_conversion_array::<
                i64,
                _,
            >(array, op)?)),
            DataType::Utf8View => {
                let string_array = as_string_view_array(array)?;
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
                            builder.append_value(&op(s));
                        }
                    }
                } else {
                    for i in 0..item_len {
                        // SAFETY: no null buffer means every index is valid.
                        let s = unsafe { string_array.value_unchecked(i) };
                        builder.append_value(&op(s));
                    }
                }

                Ok(ColumnarValue::Array(Arc::new(builder.finish(nulls)?)))
            }
            other => exec_err!("Unsupported data type {other:?} for function {name}"),
        },
        ColumnarValue::Scalar(scalar) => match scalar {
            ScalarValue::Utf8(a) => {
                let result = a.as_ref().map(|x| op(x));
                Ok(ColumnarValue::Scalar(ScalarValue::Utf8(result)))
            }
            ScalarValue::LargeUtf8(a) => {
                let result = a.as_ref().map(|x| op(x));
                Ok(ColumnarValue::Scalar(ScalarValue::LargeUtf8(result)))
            }
            ScalarValue::Utf8View(a) => {
                let result = a.as_ref().map(|x| op(x));
                Ok(ColumnarValue::Scalar(ScalarValue::Utf8View(result)))
            }
            other => exec_err!("Unsupported data type {other:?} for function {name}"),
        },
    }
}

fn case_conversion_array<'a, O, F>(array: &'a ArrayRef, op: F) -> Result<ArrayRef>
where
    O: OffsetSizeTrait,
    F: Fn(&'a str) -> String,
{
    const PRE_ALLOC_BYTES: usize = 8;

    let string_array = as_generic_string_array::<O>(array)?;
    if string_array.is_ascii() {
        return case_conversion_ascii_array::<O, _>(string_array, op);
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
                builder.append_value(&op(s));
            }
        }
    } else {
        for i in 0..item_len {
            // SAFETY: no null buffer means every index is valid.
            let s = unsafe { string_array.value_unchecked(i) };
            builder.append_value(&op(s));
        }
    }
    Ok(Arc::new(builder.finish(nulls)?))
}

/// Fast path for case conversion on an all-ASCII string array. ASCII case
/// conversion is byte-length-preserving, so we can convert the entire addressed
/// range in one call and reuse the offsets and nulls buffers — rebasing the
/// offsets when the input is a sliced array.
fn case_conversion_ascii_array<'a, O, F>(
    string_array: &'a GenericStringArray<O>,
    op: F,
) -> Result<ArrayRef>
where
    O: OffsetSizeTrait,
    F: Fn(&'a str) -> String,
{
    let value_offsets = string_array.value_offsets();
    let start = value_offsets.first().unwrap().as_usize();
    let end = value_offsets.last().unwrap().as_usize();
    let relevant = &string_array.value_data()[start..end];

    // SAFETY: `relevant` is a subslice of the string array's value buffer,
    // which is valid UTF-8.
    let str_values = unsafe { std::str::from_utf8_unchecked(relevant) };

    let converted_values = op(str_values);
    debug_assert_eq!(converted_values.len(), str_values.len());
    let values = Buffer::from_vec(converted_values.into_bytes());

    // Shift offsets from `start`-based to 0-based so they index into `values`.
    let offsets = if start == 0 {
        string_array.offsets().clone()
    } else {
        let s = O::usize_as(start);
        let rebased: Vec<O> = value_offsets.iter().map(|&o| o - s).collect();
        // SAFETY: subtracting a constant from monotonic offsets preserves
        // monotonicity, and `start` is the minimum offset so no underflow.
        unsafe { OffsetBuffer::new_unchecked(ScalarBuffer::from(rebased)) }
    };

    let nulls = string_array.nulls().cloned();
    // SAFETY: offsets are monotonic and in-bounds for `values`; nulls
    // (if any) match the slice length.
    Ok(Arc::new(unsafe {
        GenericStringArray::<O>::new_unchecked(offsets, values, nulls)
    }))
}
