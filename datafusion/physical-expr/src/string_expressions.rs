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

// Some of these functions reference the Postgres documentation
// or implementation to ensure compatibility and are subject to
// the Postgres license.

//! String expressions

use std::sync::Arc;

use arrow::array::ArrayDataBuilder;
use arrow::{
    array::{
        Array, ArrayRef, GenericStringArray, Int32Array, Int64Array, OffsetSizeTrait,
        StringArray,
    },
    datatypes::DataType,
};
use arrow_buffer::{MutableBuffer, NullBuffer};

use datafusion_common::Result;
use datafusion_common::{
    cast::{as_generic_string_array, as_string_array},
    exec_err, ScalarValue,
};
use datafusion_expr::ColumnarValue;

enum ColumnarValueRef<'a> {
    Scalar(&'a [u8]),
    NullableArray(&'a StringArray),
    NonNullableArray(&'a StringArray),
}

impl<'a> ColumnarValueRef<'a> {
    #[inline]
    fn is_valid(&self, i: usize) -> bool {
        match &self {
            Self::Scalar(_) | Self::NonNullableArray(_) => true,
            Self::NullableArray(array) => array.is_valid(i),
        }
    }

    #[inline]
    fn nulls(&self) -> Option<NullBuffer> {
        match &self {
            Self::Scalar(_) | Self::NonNullableArray(_) => None,
            Self::NullableArray(array) => array.nulls().cloned(),
        }
    }
}

/// Optimized version of the StringBuilder in Arrow that:
/// 1. Precalculating the expected length of the result, avoiding reallocations.
/// 2. Avoids creating / incrementally creating a `NullBufferBuilder`
struct StringArrayBuilder {
    offsets_buffer: MutableBuffer,
    value_buffer: MutableBuffer,
}

impl StringArrayBuilder {
    fn with_capacity(item_capacity: usize, data_capacity: usize) -> Self {
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

    fn write<const CHECK_VALID: bool>(&mut self, column: &ColumnarValueRef, i: usize) {
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

    fn append_offset(&mut self) {
        let next_offset: i32 = self
            .value_buffer
            .len()
            .try_into()
            .expect("byte array offset overflow");
        unsafe { self.offsets_buffer.push_unchecked(next_offset) };
    }

    fn finish(self, null_buffer: Option<NullBuffer>) -> StringArray {
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

/// Concatenates the text representations of all the arguments. NULL arguments are ignored.
/// concat('abcde', 2, NULL, 22) = 'abcde222'
pub fn concat(args: &[ColumnarValue]) -> Result<ColumnarValue> {
    let array_len = args
        .iter()
        .filter_map(|x| match x {
            ColumnarValue::Array(array) => Some(array.len()),
            _ => None,
        })
        .next();

    // Scalar
    if array_len.is_none() {
        let mut result = String::new();
        for arg in args {
            if let ColumnarValue::Scalar(ScalarValue::Utf8(Some(v))) = arg {
                result.push_str(v);
            }
        }
        return Ok(ColumnarValue::Scalar(ScalarValue::Utf8(Some(result))));
    }

    // Array
    let len = array_len.unwrap();
    let mut data_size = 0;
    let mut columns = Vec::with_capacity(args.len());

    for arg in args {
        match arg {
            ColumnarValue::Scalar(ScalarValue::Utf8(maybe_value)) => {
                if let Some(s) = maybe_value {
                    data_size += s.len() * len;
                    columns.push(ColumnarValueRef::Scalar(s.as_bytes()));
                }
            }
            ColumnarValue::Array(array) => {
                let string_array = as_string_array(array)?;
                data_size += string_array.values().len();
                let column = if array.is_nullable() {
                    ColumnarValueRef::NullableArray(string_array)
                } else {
                    ColumnarValueRef::NonNullableArray(string_array)
                };
                columns.push(column);
            }
            _ => unreachable!(),
        }
    }

    let mut builder = StringArrayBuilder::with_capacity(len, data_size);
    for i in 0..len {
        columns
            .iter()
            .for_each(|column| builder.write::<true>(column, i));
        builder.append_offset();
    }
    Ok(ColumnarValue::Array(Arc::new(builder.finish(None))))
}

/// Concatenates all but the first argument, with separators. The first argument is used as the separator string, and should not be NULL. Other NULL arguments are ignored.
/// concat_ws(',', 'abcde', 2, NULL, 22) = 'abcde,2,22'
pub fn concat_ws(args: &[ColumnarValue]) -> Result<ColumnarValue> {
    // do not accept 0 or 1 arguments.
    if args.len() < 2 {
        return exec_err!(
            "concat_ws was called with {} arguments. It requires at least 2.",
            args.len()
        );
    }

    let array_len = args
        .iter()
        .filter_map(|x| match x {
            ColumnarValue::Array(array) => Some(array.len()),
            _ => None,
        })
        .next();

    // Scalar
    if array_len.is_none() {
        let sep = match &args[0] {
            ColumnarValue::Scalar(ScalarValue::Utf8(Some(s))) => s,
            ColumnarValue::Scalar(ScalarValue::Utf8(None)) => {
                return Ok(ColumnarValue::Scalar(ScalarValue::Utf8(None)));
            }
            _ => unreachable!(),
        };

        let mut result = String::new();
        let iter = &mut args[1..].iter();

        for arg in iter.by_ref() {
            match arg {
                ColumnarValue::Scalar(ScalarValue::Utf8(Some(s))) => {
                    result.push_str(s);
                    break;
                }
                ColumnarValue::Scalar(ScalarValue::Utf8(None)) => {}
                _ => unreachable!(),
            }
        }

        for arg in iter.by_ref() {
            match arg {
                ColumnarValue::Scalar(ScalarValue::Utf8(Some(s))) => {
                    result.push_str(sep);
                    result.push_str(s);
                }
                ColumnarValue::Scalar(ScalarValue::Utf8(None)) => {}
                _ => unreachable!(),
            }
        }

        return Ok(ColumnarValue::Scalar(ScalarValue::Utf8(Some(result))));
    }

    // Array
    let len = array_len.unwrap();
    let mut data_size = 0;

    // parse sep
    let sep = match &args[0] {
        ColumnarValue::Scalar(ScalarValue::Utf8(Some(s))) => {
            data_size += s.len() * len * (args.len() - 2); // estimate
            ColumnarValueRef::Scalar(s.as_bytes())
        }
        ColumnarValue::Scalar(ScalarValue::Utf8(None)) => {
            return Ok(ColumnarValue::Array(Arc::new(StringArray::new_null(len))));
        }
        ColumnarValue::Array(array) => {
            let string_array = as_string_array(array)?;
            data_size += string_array.values().len() * (args.len() - 2); // estimate
            if array.is_nullable() {
                ColumnarValueRef::NullableArray(string_array)
            } else {
                ColumnarValueRef::NonNullableArray(string_array)
            }
        }
        _ => unreachable!(),
    };

    let mut columns = Vec::with_capacity(args.len() - 1);
    for arg in &args[1..] {
        match arg {
            ColumnarValue::Scalar(ScalarValue::Utf8(maybe_value)) => {
                if let Some(s) = maybe_value {
                    data_size += s.len() * len;
                    columns.push(ColumnarValueRef::Scalar(s.as_bytes()));
                }
            }
            ColumnarValue::Array(array) => {
                let string_array = as_string_array(array)?;
                data_size += string_array.values().len();
                let column = if array.is_nullable() {
                    ColumnarValueRef::NullableArray(string_array)
                } else {
                    ColumnarValueRef::NonNullableArray(string_array)
                };
                columns.push(column);
            }
            _ => unreachable!(),
        }
    }

    let mut builder = StringArrayBuilder::with_capacity(len, data_size);
    for i in 0..len {
        if !sep.is_valid(i) {
            builder.append_offset();
            continue;
        }

        let mut iter = columns.iter();
        for column in iter.by_ref() {
            if column.is_valid(i) {
                builder.write::<false>(column, i);
                break;
            }
        }

        for column in iter {
            if column.is_valid(i) {
                builder.write::<false>(&sep, i);
                builder.write::<false>(column, i);
            }
        }

        builder.append_offset();
    }

    Ok(ColumnarValue::Array(Arc::new(builder.finish(sep.nulls()))))
}

/// Converts the first letter of each word to upper case and the rest to lower case. Words are sequences of alphanumeric characters separated by non-alphanumeric characters.
/// initcap('hi THOMAS') = 'Hi Thomas'
pub fn initcap<T: OffsetSizeTrait>(args: &[ArrayRef]) -> Result<ArrayRef> {
    let string_array = as_generic_string_array::<T>(&args[0])?;

    // first map is the iterator, second is for the `Option<_>`
    let result = string_array
        .iter()
        .map(|string| {
            string.map(|string: &str| {
                let mut char_vector = Vec::<char>::new();
                let mut previous_character_letter_or_number = false;
                for c in string.chars() {
                    if previous_character_letter_or_number {
                        char_vector.push(c.to_ascii_lowercase());
                    } else {
                        char_vector.push(c.to_ascii_uppercase());
                    }
                    previous_character_letter_or_number = c.is_ascii_uppercase()
                        || c.is_ascii_lowercase()
                        || c.is_ascii_digit();
                }
                char_vector.iter().collect::<String>()
            })
        })
        .collect::<GenericStringArray<T>>();

    Ok(Arc::new(result) as ArrayRef)
}

/// Returns the position of the first occurrence of substring in string.
/// The position is counted from 1. If the substring is not found, returns 0.
/// For example, instr('Helloworld', 'world') = 6.
pub fn instr<T: OffsetSizeTrait>(args: &[ArrayRef]) -> Result<ArrayRef> {
    let string_array = as_generic_string_array::<T>(&args[0])?;
    let substr_array = as_generic_string_array::<T>(&args[1])?;

    match args[0].data_type() {
        DataType::Utf8 => {
            let result = string_array
                .iter()
                .zip(substr_array.iter())
                .map(|(string, substr)| match (string, substr) {
                    (Some(string), Some(substr)) => string
                        .find(substr)
                        .map_or(Some(0), |index| Some((index + 1) as i32)),
                    _ => None,
                })
                .collect::<Int32Array>();

            Ok(Arc::new(result) as ArrayRef)
        }
        DataType::LargeUtf8 => {
            let result = string_array
                .iter()
                .zip(substr_array.iter())
                .map(|(string, substr)| match (string, substr) {
                    (Some(string), Some(substr)) => string
                        .find(substr)
                        .map_or(Some(0), |index| Some((index + 1) as i64)),
                    _ => None,
                })
                .collect::<Int64Array>();

            Ok(Arc::new(result) as ArrayRef)
        }
        other => {
            exec_err!(
                "instr was called with {other} datatype arguments. It requires Utf8 or LargeUtf8."
            )
        }
    }
}

/// Returns true if string starts with prefix.
/// starts_with('alphabet', 'alph') = 't'
pub fn starts_with<T: OffsetSizeTrait>(args: &[ArrayRef]) -> Result<ArrayRef> {
    let left = as_generic_string_array::<T>(&args[0])?;
    let right = as_generic_string_array::<T>(&args[1])?;

    let result = arrow::compute::kernels::comparison::starts_with(left, right)?;

    Ok(Arc::new(result) as ArrayRef)
}

/// Returns true if string ends with suffix.
/// ends_with('alphabet', 'abet') = 't'
pub fn ends_with<T: OffsetSizeTrait>(args: &[ArrayRef]) -> Result<ArrayRef> {
    let left = as_generic_string_array::<T>(&args[0])?;
    let right = as_generic_string_array::<T>(&args[1])?;

    let result = arrow::compute::kernels::comparison::ends_with(left, right)?;

    Ok(Arc::new(result) as ArrayRef)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn concat() -> Result<()> {
        let c0 =
            ColumnarValue::Array(Arc::new(StringArray::from(vec!["foo", "bar", "baz"])));
        let c1 = ColumnarValue::Scalar(ScalarValue::Utf8(Some(",".to_string())));
        let c2 = ColumnarValue::Array(Arc::new(StringArray::from(vec![
            Some("x"),
            None,
            Some("z"),
        ])));
        let args = &[c0, c1, c2];

        let result = super::concat(args)?;
        let expected =
            Arc::new(StringArray::from(vec!["foo,x", "bar,", "baz,z"])) as ArrayRef;
        match &result {
            ColumnarValue::Array(array) => {
                assert_eq!(&expected, array);
            }
            _ => panic!(),
        }
        Ok(())
    }

    #[test]
    fn concat_ws() -> Result<()> {
        // sep is scalar
        let c0 = ColumnarValue::Scalar(ScalarValue::Utf8(Some(",".to_string())));
        let c1 =
            ColumnarValue::Array(Arc::new(StringArray::from(vec!["foo", "bar", "baz"])));
        let c2 = ColumnarValue::Array(Arc::new(StringArray::from(vec![
            Some("x"),
            None,
            Some("z"),
        ])));
        let args = &[c0, c1, c2];

        let result = super::concat_ws(args)?;
        let expected =
            Arc::new(StringArray::from(vec!["foo,x", "bar", "baz,z"])) as ArrayRef;
        match &result {
            ColumnarValue::Array(array) => {
                assert_eq!(&expected, array);
            }
            _ => panic!(),
        }

        // sep is nullable array
        let c0 = ColumnarValue::Array(Arc::new(StringArray::from(vec![
            Some(","),
            None,
            Some("+"),
        ])));
        let c1 =
            ColumnarValue::Array(Arc::new(StringArray::from(vec!["foo", "bar", "baz"])));
        let c2 = ColumnarValue::Array(Arc::new(StringArray::from(vec![
            Some("x"),
            Some("y"),
            Some("z"),
        ])));
        let args = &[c0, c1, c2];

        let result = super::concat_ws(args)?;
        let expected =
            Arc::new(StringArray::from(vec![Some("foo,x"), None, Some("baz+z")]))
                as ArrayRef;
        match &result {
            ColumnarValue::Array(array) => {
                assert_eq!(&expected, array);
            }
            _ => panic!(),
        }

        Ok(())
    }
}
