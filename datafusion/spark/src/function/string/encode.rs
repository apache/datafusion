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

use std::borrow::Cow;
use std::sync::Arc;

use arrow::array::{
    Array, ArrayRef, AsArray, BinaryBuilder, StringArray, StringArrayType,
};
use arrow::datatypes::{DataType, Field, FieldRef};
use datafusion_common::utils::take_function_args;
use datafusion_common::{Result, ScalarValue, exec_err, plan_err};
use datafusion_expr::{
    ColumnarValue, ReturnFieldArgs, ScalarFunctionArgs, ScalarUDFImpl, Signature,
    Volatility,
};

/// Spark-compatible `encode` expression.
/// Encodes a string or binary value into binary using the specified character encoding.
/// Binary input is interpreted as UTF-8 with lossy conversion (invalid bytes become U+FFFD).
///
/// # Target Spark version
/// Emulates Spark 3.5 semantics:
/// - Accepts canonical charset names and common aliases (`UTF8`, `LATIN1`,
///   `ISO88591`, `ASCII`, `UTF-32BE`, etc.).
/// - Unmappable characters (non-ASCII in `US-ASCII`, code points above
///   `U+00FF` in `ISO-8859-1`) are silently replaced with `?`.
/// - `UTF-32` is an alias for `UTF-32BE` (no BOM), matching both Spark 3.5
///   and Spark 4.1.
///
/// # Spark 4.0 differences (not implemented)
/// Spark 4.0 tightened `encode` in two ways, each gated by a `spark.sql.legacy.*`
/// config that can restore the 3.5 behavior:
///
/// - Charset whitelist — rejects aliases with `INVALID_PARAMETER_VALUE.CHARSET`.
///   Controlled by `spark.sql.legacy.javaCharsets`.
/// - Unmappable characters — raises `MALFORMED_CHARACTER_CODING`.
///   Controlled by `spark.sql.legacy.codingErrorAction`.
///
/// TODO: wire both configs so Spark 4.0 behavior can be selected at runtime.
/// See: <https://spark.apache.org/docs/4.0.0/sql-migration-guide.html>
///
/// <https://spark.apache.org/docs/latest/api/sql/index.html#encode>
#[derive(Debug, PartialEq, Eq, Hash)]
pub struct SparkEncode {
    signature: Signature,
}

impl Default for SparkEncode {
    fn default() -> Self {
        Self::new()
    }
}

impl SparkEncode {
    pub fn new() -> Self {
        Self {
            signature: Signature::user_defined(Volatility::Immutable),
        }
    }
}

/// Encodes a single string value using the specified charset.
/// Unmappable characters are silently replaced with `?` (Spark 3.5 behavior).
fn encode_string(s: &str, charset: &str) -> Result<Vec<u8>> {
    match charset {
        "UTF-8" | "UTF8" => Ok(s.as_bytes().to_vec()),
        "US-ASCII" | "ASCII" => Ok(s
            .chars()
            .map(|c| if c.is_ascii() { c as u8 } else { b'?' })
            .collect()),
        "ISO-8859-1" | "ISO88591" | "LATIN1" => Ok(s
            .chars()
            .map(|c| {
                let cp = c as u32;
                if cp <= 255 { cp as u8 } else { b'?' }
            })
            .collect()),
        "UTF-16BE" | "UTF16BE" => {
            let mut bytes = Vec::new();
            for code_unit in s.encode_utf16() {
                bytes.extend_from_slice(&code_unit.to_be_bytes());
            }
            Ok(bytes)
        }
        "UTF-16LE" | "UTF16LE" => {
            let mut bytes = Vec::new();
            for code_unit in s.encode_utf16() {
                bytes.extend_from_slice(&code_unit.to_le_bytes());
            }
            Ok(bytes)
        }
        "UTF-16" | "UTF16" => {
            // BOM (big-endian marker) followed by UTF-16BE encoded bytes
            let mut bytes = vec![0xFE, 0xFF];
            for code_unit in s.encode_utf16() {
                bytes.extend_from_slice(&code_unit.to_be_bytes());
            }
            Ok(bytes)
        }
        // Spark treats UTF-32 as UTF-32BE (no BOM), matching Spark 3.5 and 4.1.
        "UTF-32" | "UTF32" | "UTF-32BE" | "UTF32BE" => {
            let mut bytes = Vec::with_capacity(s.len() * 4);
            for c in s.chars() {
                bytes.extend_from_slice(&(c as u32).to_be_bytes());
            }
            Ok(bytes)
        }
        "UTF-32LE" | "UTF32LE" => {
            let mut bytes = Vec::with_capacity(s.len() * 4);
            for c in s.chars() {
                bytes.extend_from_slice(&(c as u32).to_le_bytes());
            }
            Ok(bytes)
        }
        _ => exec_err!(
            "Unsupported charset for encode: '{}'. Supported: US-ASCII, ISO-8859-1, UTF-8, UTF-16, UTF-16BE, UTF-16LE, UTF-32, UTF-32BE, UTF-32LE",
            charset
        ),
    }
}

/// The charset for each row. `Constant` is a single uppercased charset; `PerRow`
/// uppercases each row on demand, yielding `None` for null entries.
enum Charsets<'a> {
    Constant(&'a str),
    PerRow(&'a StringArray),
}

impl Charsets<'_> {
    fn get(&self, i: usize) -> Option<Cow<'_, str>> {
        match self {
            Charsets::Constant(charset) => Some(Cow::Borrowed(charset)),
            Charsets::PerRow(array) => {
                (!array.is_null(i)).then(|| Cow::Owned(array.value(i).to_uppercase()))
            }
        }
    }
}

fn null_binary_array(len: usize) -> ArrayRef {
    let mut builder = BinaryBuilder::new();
    for _ in 0..len {
        builder.append_null();
    }
    Arc::new(builder.finish())
}

/// Encodes each row to binary. A row is null if its value or charset is null;
/// otherwise it is `encode_string(decode(i), charset)`. `decode` yields the row's
/// value as text — a borrow for string input, or lossy UTF-8 for binary input
/// (invalid bytes become U+FFFD, matching Spark).
fn encode_rows<'a>(
    len: usize,
    is_null: impl Fn(usize) -> bool,
    decode: impl Fn(usize) -> Cow<'a, str>,
    charsets: &Charsets,
) -> Result<ArrayRef> {
    let mut builder = BinaryBuilder::with_capacity(len, len * 4);
    for i in 0..len {
        match (is_null(i), charsets.get(i)) {
            (false, Some(charset)) => {
                builder.append_value(&encode_string(&decode(i), &charset)?)
            }
            _ => builder.append_null(),
        }
    }
    Ok(Arc::new(builder.finish()))
}

fn encode_array<'a, S: StringArrayType<'a>>(
    array: &S,
    charsets: &Charsets,
) -> Result<ArrayRef> {
    encode_rows(
        array.len(),
        |i| array.is_null(i),
        |i| Cow::Borrowed(array.value(i)),
        charsets,
    )
}

fn encode_binary_array<'a, B: arrow::array::BinaryArrayType<'a>>(
    array: &'a B,
    charsets: &Charsets,
) -> Result<ArrayRef> {
    encode_rows(
        array.len(),
        |i| array.is_null(i),
        |i| String::from_utf8_lossy(array.value(i)),
        charsets,
    )
}

fn encode_dispatch(arr: &ArrayRef, charsets: &Charsets) -> Result<ArrayRef> {
    match arr.data_type() {
        DataType::Utf8 => encode_array(&arr.as_string::<i32>(), charsets),
        DataType::LargeUtf8 => encode_array(&arr.as_string::<i64>(), charsets),
        DataType::Utf8View => encode_array(&arr.as_string_view(), charsets),
        DataType::Binary => encode_binary_array(&arr.as_binary::<i32>(), charsets),
        DataType::LargeBinary => encode_binary_array(&arr.as_binary::<i64>(), charsets),
        DataType::BinaryView => encode_binary_array(&arr.as_binary_view(), charsets),
        DataType::Null => Ok(null_binary_array(arr.len())),
        dt => exec_err!("encode expects a string or binary argument, got {dt:?}"),
    }
}

/// The uppercased charset from a scalar, or `None` if it is null.
fn scalar_charset(scalar: &ScalarValue) -> Result<Option<String>> {
    match scalar {
        ScalarValue::Utf8(s) | ScalarValue::LargeUtf8(s) | ScalarValue::Utf8View(s) => {
            Ok(s.as_ref().map(|s| s.to_uppercase()))
        }
        ScalarValue::Null => Ok(None),
        other => exec_err!("encode charset argument must be a string, got {other:?}"),
    }
}

impl ScalarUDFImpl for SparkEncode {
    fn name(&self) -> &str {
        "encode"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn coerce_types(&self, arg_types: &[DataType]) -> Result<Vec<DataType>> {
        let [value_type, charset_type] = take_function_args(self.name(), arg_types)?;

        let value_type = match value_type {
            DataType::Utf8
            | DataType::LargeUtf8
            | DataType::Utf8View
            | DataType::Binary
            | DataType::LargeBinary
            | DataType::BinaryView
            | DataType::Null => value_type.clone(),
            other => {
                return plan_err!(
                    "encode expects a string or binary first argument, got {other:?}"
                );
            }
        };

        match charset_type {
            DataType::Utf8
            | DataType::LargeUtf8
            | DataType::Utf8View
            | DataType::Null => {}
            other => {
                return plan_err!(
                    "encode expects a string charset second argument, got {other:?}"
                );
            }
        }

        Ok(vec![value_type, DataType::Utf8])
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        datafusion_common::internal_err!(
            "return_type should not be called, use return_field_from_args instead"
        )
    }

    fn return_field_from_args(&self, args: ReturnFieldArgs) -> Result<FieldRef> {
        let nullable = args.arg_fields.iter().any(|f| f.is_nullable());
        Ok(Arc::new(Field::new(
            self.name(),
            DataType::Binary,
            nullable,
        )))
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let [value, charset_arg] = take_function_args(self.name(), args.args)?;

        let len = [&value, &charset_arg]
            .into_iter()
            .find_map(|arg| match arg {
                ColumnarValue::Array(a) => Some(a.len()),
                _ => None,
            });
        let inferred_length = len.unwrap_or(1);
        let is_scalar = len.is_none();

        let value_arr = match value {
            ColumnarValue::Scalar(scalar) => scalar.to_array_of_size(inferred_length)?,
            ColumnarValue::Array(array) => array,
        };

        // A null charset yields a null result for that row (Spark NullIntolerant).
        let result = match &charset_arg {
            ColumnarValue::Scalar(scalar) => match scalar_charset(scalar)? {
                Some(charset) => {
                    encode_dispatch(&value_arr, &Charsets::Constant(&charset))?
                }
                None => null_binary_array(value_arr.len()),
            },
            ColumnarValue::Array(charset_array) => encode_dispatch(
                &value_arr,
                &Charsets::PerRow(charset_array.as_string::<i32>()),
            )?,
        };

        if is_scalar {
            ScalarValue::try_from_array(&result, 0).map(ColumnarValue::Scalar)
        } else {
            Ok(ColumnarValue::Array(result))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_encode_return_field_nullable() {
        let func = SparkEncode::new();

        let nullable = func
            .return_field_from_args(ReturnFieldArgs {
                arg_fields: &[
                    Arc::new(Field::new("input", DataType::Utf8, true)),
                    Arc::new(Field::new("charset", DataType::Utf8, false)),
                ],
                scalar_arguments: &[None, None],
            })
            .unwrap();
        assert!(nullable.is_nullable());
        assert_eq!(nullable.data_type(), &DataType::Binary);

        let non_nullable = func
            .return_field_from_args(ReturnFieldArgs {
                arg_fields: &[
                    Arc::new(Field::new("input", DataType::Utf8, false)),
                    Arc::new(Field::new("charset", DataType::Utf8, false)),
                ],
                scalar_arguments: &[None, None],
            })
            .unwrap();
        assert!(!non_nullable.is_nullable());
    }
}
