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

use std::fmt::{Display, Formatter};
use std::sync::Arc;

use crate::strings::make_and_append_view;
use arrow::array::{
    new_null_array, Array, ArrayRef, GenericStringArray, GenericStringBuilder,
    OffsetSizeTrait, StringBuilder, StringViewArray,
};
use arrow::buffer::Buffer;
use arrow::datatypes::DataType;
use arrow_buffer::{NullBufferBuilder, ScalarBuffer};
use datafusion_common::cast::{as_generic_string_array, as_string_view_array};
use datafusion_common::Result;
use datafusion_common::{exec_err, ScalarValue};
use datafusion_expr::ColumnarValue;

pub(crate) enum TrimType {
    Left,
    Right,
    Both,
}

impl Display for TrimType {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            TrimType::Left => write!(f, "ltrim"),
            TrimType::Right => write!(f, "rtrim"),
            TrimType::Both => write!(f, "btrim"),
        }
    }
}

pub(crate) fn general_trim<T: OffsetSizeTrait>(
    args: &[ArrayRef],
    trim_type: TrimType,
    use_string_view: bool,
) -> Result<ArrayRef> {
    let func = match trim_type {
        TrimType::Left => |input, pattern: &str| {
            let pattern = pattern.chars().collect::<Vec<char>>();
            let ltrimmed_str =
                str::trim_start_matches::<&[char]>(input, pattern.as_ref());
            // `ltrimmed_str` is actually `input`[start_offset..],
            // so `start_offset` = len(`input`) - len(`ltrimmed_str`)
            let start_offset = input.as_bytes().len() - ltrimmed_str.as_bytes().len();

            (ltrimmed_str, start_offset as u32)
        },
        TrimType::Right => |input, pattern: &str| {
            let pattern = pattern.chars().collect::<Vec<char>>();
            let rtrimmed_str = str::trim_end_matches::<&[char]>(input, pattern.as_ref());

            // `ltrimmed_str` is actually `input`[0..new_len], so `start_offset` is 0
            (rtrimmed_str, 0)
        },
        TrimType::Both => |input, pattern: &str| {
            let pattern = pattern.chars().collect::<Vec<char>>();
            let ltrimmed_str =
                str::trim_start_matches::<&[char]>(input, pattern.as_ref());
            // `btrimmed_str` can be got by rtrim(ltrim(`input`)),
            // so its `start_offset` should be same as ltrim situation above
            let start_offset = input.as_bytes().len() - ltrimmed_str.as_bytes().len();
            let btrimmed_str =
                str::trim_end_matches::<&[char]>(ltrimmed_str, pattern.as_ref());

            (btrimmed_str, start_offset as u32)
        },
    };

    if use_string_view {
        string_view_trim(func, args)
    } else {
        string_trim::<T>(func, args)
    }
}

/// Applies the trim function to the given string view array(s)
/// and returns a new string view array with the trimmed values.
///
/// # `trim_func`: The function to apply to each string view.
///
/// ## Arguments
/// - The original string
/// - the pattern to trim
///
/// ## Returns
///  - trimmed str (must be a substring of the first argument)
///  - start offset, needed in `string_view_trim`
///
/// ## Examples
///
/// For `ltrim`:
/// - `fn("  abc", " ") -> ("abc", 2)`
/// - `fn("abd", " ") -> ("abd", 0)`
///
/// For `btrim`:
/// - `fn("  abc  ", " ") -> ("abc", 2)`
/// - `fn("abd", " ") -> ("abd", 0)`
// removing 'a will cause compiler complaining lifetime of `func`
fn string_view_trim<'a>(
    trim_func: fn(&'a str, &'a str) -> (&'a str, u32),
    args: &'a [ArrayRef],
) -> Result<ArrayRef> {
    let string_view_array = as_string_view_array(&args[0])?;
    let mut views_buf = Vec::with_capacity(string_view_array.len());
    let mut null_builder = NullBufferBuilder::new(string_view_array.len());

    match args.len() {
        1 => {
            let array_iter = string_view_array.iter();
            let views_iter = string_view_array.views().iter();
            for (src_str_opt, raw_view) in array_iter.zip(views_iter) {
                trim_and_append_str(
                    src_str_opt,
                    Some(" "),
                    trim_func,
                    &mut views_buf,
                    &mut null_builder,
                    raw_view,
                );
            }
        }
        2 => {
            let characters_array = as_string_view_array(&args[1])?;

            if characters_array.len() == 1 {
                // Only one `trim characters` exist
                if characters_array.is_null(0) {
                    return Ok(new_null_array(
                        // The schema is expecting utf8 as null
                        &DataType::Utf8View,
                        string_view_array.len(),
                    ));
                }

                let characters = characters_array.value(0);
                let array_iter = string_view_array.iter();
                let views_iter = string_view_array.views().iter();
                for (src_str_opt, raw_view) in array_iter.zip(views_iter) {
                    trim_and_append_str(
                        src_str_opt,
                        Some(characters),
                        trim_func,
                        &mut views_buf,
                        &mut null_builder,
                        raw_view,
                    );
                }
            } else {
                // A specific `trim characters` for a row in the string view array
                let characters_iter = characters_array.iter();
                let array_iter = string_view_array.iter();
                let views_iter = string_view_array.views().iter();
                for ((src_str_opt, raw_view), characters_opt) in
                    array_iter.zip(views_iter).zip(characters_iter)
                {
                    trim_and_append_str(
                        src_str_opt,
                        characters_opt,
                        trim_func,
                        &mut views_buf,
                        &mut null_builder,
                        raw_view,
                    );
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
/// Calls `trim_func` on the string value in `original_view`, for non_null
/// values and appends the updated view to the views buffer / null_builder.
///
/// Arguments
/// - `src_str_opt`: The original string value (represented by the view)
/// - `trim_characters_opt`: The characters to trim from the string
/// - `trim_func`: The function to apply to the string (see [`string_view_trim`] for details)
/// - `views_buf`: The buffer to append the updated views to
/// - `null_builder`: The buffer to append the null values to
/// - `original_view`: The original view value (that contains src_str_opt)
fn trim_and_append_str<'a>(
    src_str_opt: Option<&'a str>,
    trim_characters_opt: Option<&'a str>,
    trim_func: fn(&'a str, &'a str) -> (&'a str, u32),
    views_buf: &mut Vec<u128>,
    null_builder: &mut NullBufferBuilder,
    original_view: &u128,
) {
    if let (Some(src_str), Some(characters)) = (src_str_opt, trim_characters_opt) {
        let (trim_str, start_offset) = trim_func(src_str, characters);
        make_and_append_view(
            views_buf,
            null_builder,
            original_view,
            trim_str,
            start_offset,
        );
    } else {
        null_builder.append_null();
        views_buf.push(0);
    }
}

/// Applies the trim function to the given string array(s)
/// and returns a new string array with the trimmed values.
///
/// See [`string_view_trim`] for details on `func`
fn string_trim<'a, T: OffsetSizeTrait>(
    func: fn(&'a str, &'a str) -> (&'a str, u32),
    args: &'a [ArrayRef],
) -> Result<ArrayRef> {
    let string_array = as_generic_string_array::<T>(&args[0])?;

    match args.len() {
        1 => {
            let result = string_array
                .iter()
                .map(|string| string.map(|string: &str| func(string, " ").0))
                .collect::<GenericStringArray<T>>();

            Ok(Arc::new(result) as ArrayRef)
        }
        2 => {
            let characters_array = as_generic_string_array::<T>(&args[1])?;

            if characters_array.len() == 1 {
                if characters_array.is_null(0) {
                    return Ok(new_null_array(
                        string_array.data_type(),
                        string_array.len(),
                    ));
                }

                let characters = characters_array.value(0);
                let result = string_array
                    .iter()
                    .map(|item| item.map(|string| func(string, characters).0))
                    .collect::<GenericStringArray<T>>();
                return Ok(Arc::new(result) as ArrayRef);
            }

            let result = string_array
                .iter()
                .zip(characters_array.iter())
                .map(|(string, characters)| match (string, characters) {
                    (Some(string), Some(characters)) => Some(func(string, characters).0),
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
                let mut string_builder = StringBuilder::with_capacity(
                    string_array.len(),
                    string_array.get_array_memory_size(),
                );

                for str in string_array.iter() {
                    if let Some(str) = str {
                        string_builder.append_value(op(str));
                    } else {
                        string_builder.append_null();
                    }
                }

                Ok(ColumnarValue::Array(Arc::new(string_builder.finish())))
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
                Ok(ColumnarValue::Scalar(ScalarValue::Utf8(result)))
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
    let value_data = string_array.value_data();

    // All values are ASCII.
    if value_data.is_ascii() {
        return case_conversion_ascii_array::<O, _>(string_array, op);
    }

    // Values contain non-ASCII.
    let item_len = string_array.len();
    let capacity = string_array.value_data().len() + PRE_ALLOC_BYTES;
    let mut builder = GenericStringBuilder::<O>::with_capacity(item_len, capacity);

    if string_array.null_count() == 0 {
        let iter =
            (0..item_len).map(|i| Some(op(unsafe { string_array.value_unchecked(i) })));
        builder.extend(iter);
    } else {
        let iter = string_array.iter().map(|string| string.map(&op));
        builder.extend(iter);
    }
    Ok(Arc::new(builder.finish()))
}

/// All values of string_array are ASCII, and when converting case, there is no changes in the byte
/// array length. Therefore, the StringArray can be treated as a complete ASCII string for
/// case conversion, and we can reuse the offsets buffer and the nulls buffer.
fn case_conversion_ascii_array<'a, O, F>(
    string_array: &'a GenericStringArray<O>,
    op: F,
) -> Result<ArrayRef>
where
    O: OffsetSizeTrait,
    F: Fn(&'a str) -> String,
{
    let value_data = string_array.value_data();
    // SAFETY: all items stored in value_data satisfy UTF8.
    // ref: impl ByteArrayNativeType for str {...}
    let str_values = unsafe { std::str::from_utf8_unchecked(value_data) };

    // conversion
    let converted_values = op(str_values);
    assert_eq!(converted_values.len(), str_values.len());
    let bytes = converted_values.into_bytes();

    // build result
    let values = Buffer::from_vec(bytes);
    let offsets = string_array.offsets().clone();
    let nulls = string_array.nulls().cloned();
    // SAFETY: offsets and nulls are consistent with the input array.
    Ok(Arc::new(unsafe {
        GenericStringArray::<O>::new_unchecked(offsets, values, nulls)
    }))
}
