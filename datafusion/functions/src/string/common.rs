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
    new_null_array, Array, ArrayAccessor, ArrayDataBuilder, ArrayRef, GenericStringArray,
    GenericStringBuilder, OffsetSizeTrait, StringArray,
};
use arrow::buffer::{Buffer, MutableBuffer, NullBuffer};
use arrow::datatypes::DataType;

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

    if use_string_view {
        string_view_trim::<T>(trim_type, func, args)
    } else {
        string_trim::<T>(trim_type, func, args)
    }
}

// removing 'a will cause compiler complaining lifetime of `func`
fn string_view_trim<'a, T: OffsetSizeTrait>(
    trim_type: TrimType,
    func: fn(&'a str, &'a str) -> &'a str,
    args: &'a [ArrayRef],
) -> Result<ArrayRef> {
    let string_array = as_string_view_array(&args[0])?;

    match args.len() {
        1 => {
            let result = string_array
                .iter()
                .map(|string| string.map(|string: &str| func(string, " ")))
                .collect::<GenericStringArray<T>>();

            Ok(Arc::new(result) as ArrayRef)
        }
        2 => {
            let characters_array = as_string_view_array(&args[1])?;

            if characters_array.len() == 1 {
                if characters_array.is_null(0) {
                    return Ok(new_null_array(
                        // The schema is expecting utf8 as null
                        &DataType::Utf8,
                        string_array.len(),
                    ));
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

fn string_trim<'a, T: OffsetSizeTrait>(
    trim_type: TrimType,
    func: fn(&'a str, &'a str) -> &'a str,
    args: &'a [ArrayRef],
) -> Result<ArrayRef> {
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
                    return Ok(new_null_array(
                        string_array.data_type(),
                        string_array.len(),
                    ));
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

pub(crate) enum ColumnarValueRef<'a> {
    Scalar(&'a [u8]),
    NullableArray(&'a StringArray),
    NonNullableArray(&'a StringArray),
}

impl<'a> ColumnarValueRef<'a> {
    #[inline]
    pub fn is_valid(&self, i: usize) -> bool {
        match &self {
            Self::Scalar(_) | Self::NonNullableArray(_) => true,
            Self::NullableArray(array) => array.is_valid(i),
        }
    }

    #[inline]
    pub fn nulls(&self) -> Option<NullBuffer> {
        match &self {
            Self::Scalar(_) | Self::NonNullableArray(_) => None,
            Self::NullableArray(array) => array.nulls().cloned(),
        }
    }
}

/// Optimized version of the StringBuilder in Arrow that:
/// 1. Precalculating the expected length of the result, avoiding reallocations.
/// 2. Avoids creating / incrementally creating a `NullBufferBuilder`
pub(crate) struct StringArrayBuilder {
    offsets_buffer: MutableBuffer,
    value_buffer: MutableBuffer,
}

impl StringArrayBuilder {
    pub fn with_capacity(item_capacity: usize, data_capacity: usize) -> Self {
        let mut offsets_buffer = MutableBuffer::with_capacity(
            (item_capacity + 1) * std::mem::size_of::<i32>(),
        );
        // SAFETY: the first offset value is definitely not going to exceed the bounds.
        unsafe { offsets_buffer.push_unchecked(0_i32) };
        Self {
            offsets_buffer,
            value_buffer: MutableBuffer::with_capacity(data_capacity),
        }
    }

    pub fn write<const CHECK_VALID: bool>(
        &mut self,
        column: &ColumnarValueRef,
        i: usize,
    ) {
        match column {
            ColumnarValueRef::Scalar(s) => {
                self.value_buffer.extend_from_slice(s);
            }
            ColumnarValueRef::NullableArray(array) => {
                if !CHECK_VALID || array.is_valid(i) {
                    self.value_buffer
                        .extend_from_slice(array.value(i).as_bytes());
                }
            }
            ColumnarValueRef::NonNullableArray(array) => {
                self.value_buffer
                    .extend_from_slice(array.value(i).as_bytes());
            }
        }
    }

    pub fn append_offset(&mut self) {
        let next_offset: i32 = self
            .value_buffer
            .len()
            .try_into()
            .expect("byte array offset overflow");
        unsafe { self.offsets_buffer.push_unchecked(next_offset) };
    }

    pub fn finish(self, null_buffer: Option<NullBuffer>) -> StringArray {
        let array_builder = ArrayDataBuilder::new(DataType::Utf8)
            .len(self.offsets_buffer.len() / std::mem::size_of::<i32>() - 1)
            .add_buffer(self.offsets_buffer.into())
            .add_buffer(self.value_buffer.into())
            .nulls(null_buffer);
        // SAFETY: all data that was appended was valid UTF8 and the values
        // and offsets were created correctly
        let array_data = unsafe { array_builder.build_unchecked() };
        StringArray::from(array_data)
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
