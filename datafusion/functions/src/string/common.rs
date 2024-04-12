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

use std::fmt::{Display, Formatter};
use std::sync::Arc;

use arrow::array::{
    new_null_array, Array, ArrayRef, BufferBuilder, GenericStringArray,
    GenericStringBuilder, OffsetSizeTrait,
};
use arrow::buffer::{Buffer, OffsetBuffer, ScalarBuffer};
use arrow::datatypes::DataType;

use datafusion_common::cast::as_generic_string_array;
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
) -> Result<ArrayRef> {
    let func = match trim_type {
        TrimType::Left => |input, pattern: &str| {
            let pattern = pattern.chars().collect::<Vec<char>>();
            str::trim_start_matches::<&[char]>(input, pattern.as_ref())
        },
        TrimType::Right => |input, pattern: &str| {
            let pattern = pattern.chars().collect::<Vec<char>>();
            str::trim_end_matches::<&[char]>(input, pattern.as_ref())
        },
        TrimType::Both => |input, pattern: &str| {
            let pattern = pattern.chars().collect::<Vec<char>>();
            str::trim_end_matches::<&[char]>(
                str::trim_start_matches::<&[char]>(input, pattern.as_ref()),
                pattern.as_ref(),
            )
        },
    };

    let string_array = as_generic_string_array::<T>(&args[0])?;

    match args.len() {
        1 => {
            let result = string_array
                .iter()
                .map(|string| string.map(|string: &str| func(string, " ")))
                .collect::<GenericStringArray<T>>();

            Ok(Arc::new(result) as ArrayRef)
        }
        2 => {
            let characters_array = as_generic_string_array::<T>(&args[1])?;

            if characters_array.len() == 1 {
                if characters_array.is_null(0) {
                    return Ok(new_null_array(args[0].data_type(), args[0].len()));
                }

                let characters = characters_array.value(0);
                let result = string_array
                    .iter()
                    .map(|item| item.map(|string| func(string, characters)))
                    .collect::<GenericStringArray<T>>();
                return Ok(Arc::new(result) as ArrayRef);
            }

            let result = string_array
                .iter()
                .zip(characters_array.iter())
                .map(|(string, characters)| match (string, characters) {
                    (Some(string), Some(characters)) => Some(func(string, characters)),
                    _ => None,
                })
                .collect::<GenericStringArray<T>>();

            Ok(Arc::new(result) as ArrayRef)
        }
        other => {
            exec_err!(
            "{trim_type} was called with {other} arguments. It requires at least 1 and at most 2."
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
            other => exec_err!("Unsupported data type {other:?} for function {name}"),
        },
    }
}

fn case_conversion_array<'a, O, F>(array: &'a ArrayRef, op: F) -> Result<ArrayRef>
where
    O: OffsetSizeTrait,
    F: Fn(&'a str) -> String,
{
    let string_array = as_generic_string_array::<O>(array)?;
    let item_len = string_array.len();

    // Find the first nonascii string at the beginning.
    let find_the_first_nonascii = || {
        for (i, item) in string_array.iter().enumerate() {
            if let Some(str) = item {
                if !str.as_bytes().is_ascii() {
                    return i;
                }
            }
        }
        item_len
    };
    let the_first_nonascii_index = find_the_first_nonascii();

    // Case1: maybe optimization
    if the_first_nonascii_index == 0 {
        let iter = string_array.iter().map(|string| string.map(&op));
        let capacity = string_array.value_data().len() + 8;
        let mut builder = GenericStringBuilder::<O>::with_capacity(item_len, capacity);
        builder.extend(iter);
        return Ok(Arc::new(builder.finish()));
    }

    // Case2: full optimization
    if the_first_nonascii_index == item_len {
        return case_conversion_ascii_array::<O, _>(array, op);
    }

    // Case3: partial optimization
    let value_data = string_array.value_data();
    let offsets = string_array.offsets();
    let nulls = string_array.nulls().cloned();

    // Init new offsets buffer builder
    let mut offsets_builder = BufferBuilder::<O>::new(item_len + 1);
    offsets_builder.append_slice(&offsets.as_ref()[..the_first_nonascii_index + 1]);

    // convert ascii
    let end: O = unsafe { *offsets.get_unchecked(the_first_nonascii_index) };
    let end = end.as_usize();
    let ascii = unsafe { std::str::from_utf8_unchecked(&value_data[..end]) };
    let mut converted_values = op(ascii);
    // To avoid repeatedly allocating memory, perform a reserve in advance.
    converted_values.reserve(value_data.len() - end + 8);

    // Convert remaining items
    for j in the_first_nonascii_index..item_len {
        if string_array.is_valid(j) {
            let item = unsafe { string_array.value_unchecked(j) };
            // Memory will be continuously allocated here, but it is unavoidable.
            let converted = op(item);
            converted_values.push_str(&converted);
        }
        offsets_builder
            .append(O::from_usize(converted_values.len()).expect("offset overflow"));
    }
    let offsets_buffer = offsets_builder.finish();

    // Build result
    let bytes = converted_values.into_bytes();
    let values = Buffer::from_vec(bytes);
    let offsets = OffsetBuffer::new(ScalarBuffer::new(offsets_buffer, 0, item_len + 1));
    // SAFETY: offsets and nulls are consistent with the input array.
    Ok(Arc::new(unsafe {
        GenericStringArray::<O>::new_unchecked(offsets, values, nulls)
    }))
}

fn case_conversion_ascii_array<'a, O, F>(array: &'a ArrayRef, op: F) -> Result<ArrayRef>
where
    O: OffsetSizeTrait,
    F: Fn(&'a str) -> String,
{
    let string_array = as_generic_string_array::<O>(array)?;
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
