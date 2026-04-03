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

use std::sync::Arc;

use arrow::array::{ArrayRef, AsArray, BinaryBuilder, StringArrayType};
use arrow::datatypes::{DataType, Field, FieldRef};
use datafusion_common::types::{
    NativeType, logical_binary, logical_null, logical_string,
};
use datafusion_common::{Result, ScalarValue, exec_err};
use datafusion_expr::{
    Coercion, ColumnarValue, ReturnFieldArgs, ScalarFunctionArgs, ScalarUDFImpl,
    Signature, TypeSignatureClass, Volatility,
};

/// Spark-compatible `encode` expression.
/// Encodes a string or binary value into binary using the specified character encoding.
/// Binary input is interpreted as UTF-8 with lossy conversion (invalid bytes become U+FFFD).
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
            signature: Signature::coercible(
                vec![
                    Coercion::new_implicit(
                        TypeSignatureClass::Native(logical_string()),
                        vec![
                            TypeSignatureClass::Native(logical_null()),
                            TypeSignatureClass::Native(logical_binary()),
                        ],
                        NativeType::String,
                    ),
                    Coercion::new_implicit(
                        TypeSignatureClass::Native(logical_string()),
                        vec![TypeSignatureClass::Native(logical_null())],
                        NativeType::String,
                    ),
                ],
                Volatility::Immutable,
            ),
        }
    }
}

/// Encodes a single string value using the specified charset.
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
                if cp > 255 { b'?' } else { cp as u8 }
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
        _ => exec_err!(
            "Unsupported charset for encode: '{}'. Supported: US-ASCII, ISO-8859-1, UTF-8, UTF-16, UTF-16BE, UTF-16LE",
            charset
        ),
    }
}

/// Encodes a string array using the given charset, producing a BinaryArray.
fn encode_array<'a, S: StringArrayType<'a>>(
    string_array: &S,
    charset: &str,
) -> Result<ArrayRef> {
    let mut builder =
        BinaryBuilder::with_capacity(string_array.len(), string_array.len() * 4);
    for i in 0..string_array.len() {
        if string_array.is_null(i) {
            builder.append_null();
        } else {
            let s = string_array.value(i);
            let encoded = encode_string(s, charset)?;
            builder.append_value(&encoded);
        }
    }
    Ok(Arc::new(builder.finish()))
}

/// Encodes a binary array using lossy UTF-8 conversion, then re-encodes with the given charset.
/// Invalid UTF-8 bytes become U+FFFD (replacement character), matching Spark behavior.
fn encode_binary_array<'a, B: arrow::array::BinaryArrayType<'a>>(
    binary_array: &'a B,
    charset: &str,
) -> Result<ArrayRef> {
    let mut builder =
        BinaryBuilder::with_capacity(binary_array.len(), binary_array.len() * 4);
    for i in 0..binary_array.len() {
        if binary_array.is_null(i) {
            builder.append_null();
        } else {
            let s = String::from_utf8_lossy(binary_array.value(i));
            let encoded = encode_string(&s, charset)?;
            builder.append_value(&encoded);
        }
    }
    Ok(Arc::new(builder.finish()))
}

/// Dispatches to the correct typed array encoder based on the DataType.
fn encode_dispatch(arr: &ArrayRef, charset: &str) -> Result<ArrayRef> {
    match arr.data_type() {
        DataType::Utf8 => encode_array(&arr.as_string::<i32>(), charset),
        DataType::LargeUtf8 => encode_array(&arr.as_string::<i64>(), charset),
        DataType::Utf8View => encode_array(&arr.as_string_view(), charset),
        DataType::Binary => encode_binary_array(&arr.as_binary::<i32>(), charset),
        DataType::LargeBinary => encode_binary_array(&arr.as_binary::<i64>(), charset),
        DataType::BinaryView => encode_binary_array(&arr.as_binary_view(), charset),
        DataType::Null => {
            let mut builder = BinaryBuilder::new();
            for _ in 0..arr.len() {
                builder.append_null();
            }
            Ok(Arc::new(builder.finish()))
        }
        dt => exec_err!("encode expects a string or binary argument, got {:?}", dt),
    }
}

/// Extracts a charset string from a ColumnarValue, normalizing to uppercase.
pub(crate) fn extract_charset(charset_arg: &ColumnarValue) -> Result<String> {
    match charset_arg {
        ColumnarValue::Scalar(scalar) => match scalar {
            ScalarValue::Utf8(Some(s))
            | ScalarValue::LargeUtf8(Some(s))
            | ScalarValue::Utf8View(Some(s)) => Ok(s.to_uppercase()),
            _ => exec_err!("encode charset argument must be a non-null string"),
        },
        ColumnarValue::Array(arr) => {
            fn validate_constant_charset<'a, S: StringArrayType<'a>>(
                arr: &'a S,
            ) -> Result<String> {
                if arr.is_null(0) {
                    return exec_err!(
                        "encode charset argument must be a non-null string"
                    );
                }
                let charset = arr.value(0).to_uppercase();
                for i in 1..arr.len() {
                    if arr.is_null(i) || arr.value(i).to_uppercase() != charset {
                        return exec_err!(
                            "encode charset argument must be constant across all rows"
                        );
                    }
                }
                Ok(charset)
            }
            match arr.data_type() {
                DataType::Utf8 => validate_constant_charset(&arr.as_string::<i32>()),
                DataType::LargeUtf8 => validate_constant_charset(&arr.as_string::<i64>()),
                DataType::Utf8View => validate_constant_charset(&arr.as_string_view()),
                dt => exec_err!("encode charset argument must be a string, got {:?}", dt),
            }
        }
    }
}

impl ScalarUDFImpl for SparkEncode {
    fn name(&self) -> &str {
        "encode"
    }

    fn signature(&self) -> &Signature {
        &self.signature
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
        if args.args.len() != 2 {
            return exec_err!(
                "encode requires exactly 2 arguments, got {}",
                args.args.len()
            );
        }

        let charset = extract_charset(&args.args[1])?;

        // Determine if the result should be scalar or array
        let len = args.args.iter().find_map(|arg| match arg {
            ColumnarValue::Array(a) => Some(a.len()),
            _ => None,
        });
        let inferred_length = len.unwrap_or(1);
        let is_scalar = len.is_none();

        let string_arr = match &args.args[0] {
            ColumnarValue::Scalar(scalar) => scalar.to_array_of_size(inferred_length)?,
            ColumnarValue::Array(array) => Arc::clone(array),
        };

        let result = encode_dispatch(&string_arr, &charset)?;

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
    use arrow::array::{Array, BinaryArray, StringArray, StringViewArray};
    use datafusion_common::config::ConfigOptions;
    use datafusion_expr::ScalarUDF;

    /// Helper to invoke encode as a scalar with two literal string arguments.
    fn eval_encode_scalar(input: ScalarValue, charset: &str) -> Result<ColumnarValue> {
        let func = SparkEncode::new();
        let input_field = Arc::new(Field::new("input", input.data_type(), true));
        let charset_field = Arc::new(Field::new("charset", DataType::Utf8, false));
        func.invoke_with_args(ScalarFunctionArgs {
            args: vec![
                ColumnarValue::Scalar(input),
                ColumnarValue::Scalar(ScalarValue::Utf8(Some(charset.to_string()))),
            ],
            arg_fields: vec![input_field, charset_field],
            number_rows: 1,
            return_field: Arc::new(Field::new("encode", DataType::Binary, true)),
            config_options: Arc::new(ConfigOptions::default()),
        })
    }

    fn expect_binary_scalar(result: ColumnarValue) -> Vec<u8> {
        match result {
            ColumnarValue::Scalar(ScalarValue::Binary(Some(bytes))) => bytes,
            other => panic!("Expected Binary scalar, got {:?}", other),
        }
    }

    #[test]
    fn test_encode_utf8() {
        let result =
            eval_encode_scalar(ScalarValue::Utf8(Some("Spark SQL".into())), "UTF-8")
                .unwrap();
        let bytes = expect_binary_scalar(result);
        assert_eq!(bytes, b"Spark SQL");
    }

    #[test]
    fn test_encode_us_ascii() {
        let result =
            eval_encode_scalar(ScalarValue::Utf8(Some("Hello".into())), "US-ASCII")
                .unwrap();
        assert_eq!(expect_binary_scalar(result), b"Hello");
    }

    #[test]
    fn test_encode_iso_8859_1() {
        let result = eval_encode_scalar(
            ScalarValue::Utf8(Some("caf\u{00E9}".into())),
            "ISO-8859-1",
        )
        .unwrap();
        assert_eq!(expect_binary_scalar(result), vec![0x63, 0x61, 0x66, 0xE9]);
    }

    #[test]
    fn test_encode_utf16() {
        let result =
            eval_encode_scalar(ScalarValue::Utf8(Some("Spark SQL".into())), "UTF-16")
                .unwrap();
        let bytes = expect_binary_scalar(result);
        // BOM (FEFF) + UTF-16BE encoded
        assert_eq!(
            hex_encode(&bytes),
            "FEFF0053007000610072006B002000530051004C"
        );
    }

    #[test]
    fn test_encode_utf16be() {
        let result =
            eval_encode_scalar(ScalarValue::Utf8(Some("AB".into())), "UTF-16BE").unwrap();
        assert_eq!(expect_binary_scalar(result), vec![0x00, 0x41, 0x00, 0x42]);
    }

    #[test]
    fn test_encode_utf16le() {
        let result =
            eval_encode_scalar(ScalarValue::Utf8(Some("AB".into())), "UTF-16LE").unwrap();
        assert_eq!(expect_binary_scalar(result), vec![0x41, 0x00, 0x42, 0x00]);
    }

    #[test]
    fn test_encode_case_insensitive_charset() {
        let result =
            eval_encode_scalar(ScalarValue::Utf8(Some("hello".into())), "utf-8").unwrap();
        assert_eq!(expect_binary_scalar(result), b"hello");
    }

    #[test]
    fn test_encode_unsupported_charset() {
        let result =
            eval_encode_scalar(ScalarValue::Utf8(Some("hello".into())), "EBCDIC");
        assert!(result.is_err());
        assert!(
            result
                .unwrap_err()
                .to_string()
                .contains("Unsupported charset")
        );
    }

    #[test]
    fn test_encode_null_input() {
        let func = SparkEncode::new();
        let arr: ArrayRef =
            Arc::new(StringArray::from(vec![Some("hello"), None, Some("world")]));
        let result = func
            .invoke_with_args(ScalarFunctionArgs {
                args: vec![
                    ColumnarValue::Array(arr),
                    ColumnarValue::Scalar(ScalarValue::Utf8(Some("UTF-8".into()))),
                ],
                arg_fields: vec![
                    Arc::new(Field::new("input", DataType::Utf8, true)),
                    Arc::new(Field::new("charset", DataType::Utf8, false)),
                ],
                number_rows: 3,
                return_field: Arc::new(Field::new("encode", DataType::Binary, true)),
                config_options: Arc::new(ConfigOptions::default()),
            })
            .unwrap();

        let arr = result.into_array(3).unwrap();
        let binary = arr.as_any().downcast_ref::<BinaryArray>().unwrap();
        assert_eq!(binary.value(0), b"hello");
        assert!(binary.is_null(1));
        assert_eq!(binary.value(2), b"world");
    }

    #[test]
    fn test_encode_utf8view_column() {
        let func = SparkEncode::new();
        let arr: ArrayRef = Arc::new(StringViewArray::from(vec!["foo", "bar"]));
        let result = func
            .invoke_with_args(ScalarFunctionArgs {
                args: vec![
                    ColumnarValue::Array(arr),
                    ColumnarValue::Scalar(ScalarValue::Utf8(Some("UTF-8".into()))),
                ],
                arg_fields: vec![
                    Arc::new(Field::new("input", DataType::Utf8View, false)),
                    Arc::new(Field::new("charset", DataType::Utf8, false)),
                ],
                number_rows: 2,
                return_field: Arc::new(Field::new("encode", DataType::Binary, true)),
                config_options: Arc::new(ConfigOptions::default()),
            })
            .unwrap();

        let arr = result.into_array(2).unwrap();
        let binary = arr.as_any().downcast_ref::<BinaryArray>().unwrap();
        assert_eq!(binary.value(0), b"foo");
        assert_eq!(binary.value(1), b"bar");
    }

    #[test]
    fn test_encode_binary_input() {
        let result =
            eval_encode_scalar(ScalarValue::Binary(Some(b"Hello".to_vec())), "UTF-8")
                .unwrap();
        assert_eq!(expect_binary_scalar(result), b"Hello");
    }

    #[test]
    fn test_encode_binary_lossy_utf8() {
        // UTF-16 bytes are NOT valid UTF-8; from_utf8_lossy replaces invalid bytes with U+FFFD
        let utf16_bytes = vec![0xFE, 0xFF, 0x00, 0x61, 0x00, 0x73];
        let result =
            eval_encode_scalar(ScalarValue::Binary(Some(utf16_bytes)), "UTF-8").unwrap();
        let bytes = expect_binary_scalar(result);
        let expected = vec![
            0xEF, 0xBF, 0xBD, // U+FFFD for 0xFE
            0xEF, 0xBF, 0xBD, // U+FFFD for 0xFF
            0x00, 0x61, // NUL, 'a'
            0x00, 0x73, // NUL, 's'
        ];
        assert_eq!(bytes, expected);
    }

    #[test]
    fn test_return_field_nullable() {
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

    #[test]
    fn test_function_name() {
        let func = SparkEncode::new();
        assert_eq!(func.name(), "encode");
    }

    #[test]
    fn test_udf_registration() {
        let udf = ScalarUDF::from(SparkEncode::new());
        assert_eq!(udf.name(), "encode");
    }

    /// Simple hex encoding for test assertions.
    fn hex_encode(bytes: &[u8]) -> String {
        bytes
            .iter()
            .map(|b| format!("{:02X}", b))
            .collect::<String>()
    }
}
