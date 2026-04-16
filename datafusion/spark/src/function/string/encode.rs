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
/// In ANSI mode, unmappable characters cause an error.
/// In legacy mode, unmappable characters are replaced with `?`.
fn encode_string(s: &str, charset: &str, enable_ansi_mode: bool) -> Result<Vec<u8>> {
    match charset {
        "UTF-8" | "UTF8" => Ok(s.as_bytes().to_vec()),
        "US-ASCII" | "ASCII" => {
            let mut bytes = Vec::with_capacity(s.len());
            for c in s.chars() {
                if c.is_ascii() {
                    bytes.push(c as u8);
                } else if enable_ansi_mode {
                    return exec_err!(
                        "cannot encode character '{}' (U+{:04X}) in US-ASCII",
                        c,
                        c as u32
                    );
                } else {
                    bytes.push(b'?');
                }
            }
            Ok(bytes)
        }
        "ISO-8859-1" | "ISO88591" | "LATIN1" => {
            let mut bytes = Vec::with_capacity(s.len());
            for c in s.chars() {
                let cp = c as u32;
                if cp <= 255 {
                    bytes.push(cp as u8);
                } else if enable_ansi_mode {
                    return exec_err!(
                        "cannot encode character '{}' (U+{:04X}) in ISO-8859-1",
                        c,
                        c as u32
                    );
                } else {
                    bytes.push(b'?');
                }
            }
            Ok(bytes)
        }
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
        "UTF-32BE" | "UTF32BE" => {
            let mut bytes = Vec::new();
            for c in s.chars() {
                bytes.extend_from_slice(&(c as u32).to_be_bytes());
            }
            Ok(bytes)
        }
        "UTF-32LE" | "UTF32LE" => {
            let mut bytes = Vec::new();
            for c in s.chars() {
                bytes.extend_from_slice(&(c as u32).to_le_bytes());
            }
            Ok(bytes)
        }
        "UTF-32" | "UTF32" => {
            // BOM (big-endian marker) followed by UTF-32BE encoded bytes
            let mut bytes = vec![0x00, 0x00, 0xFE, 0xFF];
            for c in s.chars() {
                bytes.extend_from_slice(&(c as u32).to_be_bytes());
            }
            Ok(bytes)
        }
        _ => exec_err!(
            "Unsupported charset for encode: '{}'. Supported: US-ASCII, ISO-8859-1, UTF-8, UTF-16, UTF-16BE, UTF-16LE, UTF-32, UTF-32BE, UTF-32LE",
            charset
        ),
    }
}

/// Encodes a string array using the given charset, producing a BinaryArray.
fn encode_array<'a, S: StringArrayType<'a>>(
    string_array: &S,
    charset: &str,
    enable_ansi_mode: bool,
) -> Result<ArrayRef> {
    let mut builder =
        BinaryBuilder::with_capacity(string_array.len(), string_array.len() * 4);
    for i in 0..string_array.len() {
        if string_array.is_null(i) {
            builder.append_null();
        } else {
            let s = string_array.value(i);
            let encoded = encode_string(s, charset, enable_ansi_mode)?;
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
    enable_ansi_mode: bool,
) -> Result<ArrayRef> {
    let mut builder =
        BinaryBuilder::with_capacity(binary_array.len(), binary_array.len() * 4);
    for i in 0..binary_array.len() {
        if binary_array.is_null(i) {
            builder.append_null();
        } else {
            let s = String::from_utf8_lossy(binary_array.value(i));
            let encoded = encode_string(&s, charset, enable_ansi_mode)?;
            builder.append_value(&encoded);
        }
    }
    Ok(Arc::new(builder.finish()))
}

/// Dispatches to the correct typed array encoder based on the DataType.
fn encode_dispatch(
    arr: &ArrayRef,
    charset: &str,
    enable_ansi_mode: bool,
) -> Result<ArrayRef> {
    match arr.data_type() {
        DataType::Utf8 => {
            encode_array(&arr.as_string::<i32>(), charset, enable_ansi_mode)
        }
        DataType::LargeUtf8 => {
            encode_array(&arr.as_string::<i64>(), charset, enable_ansi_mode)
        }
        DataType::Utf8View => {
            encode_array(&arr.as_string_view(), charset, enable_ansi_mode)
        }
        DataType::Binary => {
            encode_binary_array(&arr.as_binary::<i32>(), charset, enable_ansi_mode)
        }
        DataType::LargeBinary => {
            encode_binary_array(&arr.as_binary::<i64>(), charset, enable_ansi_mode)
        }
        DataType::BinaryView => {
            encode_binary_array(&arr.as_binary_view(), charset, enable_ansi_mode)
        }
        DataType::Null => {
            let mut builder = BinaryBuilder::new();
            for _ in 0..arr.len() {
                builder.append_null();
            }
            Ok(Arc::new(builder.finish()))
        }
        dt => exec_err!("encode expects a string or binary argument, got {dt:?}"),
    }
}

/// Extracts a charset string from a ColumnarValue, normalizing to uppercase.
fn extract_charset(charset_arg: &ColumnarValue) -> Result<String> {
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
                dt => exec_err!("encode charset argument must be a string, got {dt:?}"),
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
        let enable_ansi_mode = args.config_options.execution.enable_ansi_mode;

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

        let result = encode_dispatch(&string_arr, &charset, enable_ansi_mode)?;

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
    use arrow::array::{BinaryArray, StringViewArray};
    use datafusion_common::config::ConfigOptions;

    /// Helper to invoke encode as a scalar with two literal string arguments.
    fn eval_encode_scalar_with_ansi(
        input: ScalarValue,
        charset: &str,
        enable_ansi_mode: bool,
    ) -> Result<ColumnarValue> {
        let func = SparkEncode::new();
        let input_field = Arc::new(Field::new("input", input.data_type(), true));
        let charset_field = Arc::new(Field::new("charset", DataType::Utf8, false));
        let mut config = ConfigOptions::default();
        config.execution.enable_ansi_mode = enable_ansi_mode;
        func.invoke_with_args(ScalarFunctionArgs {
            args: vec![
                ColumnarValue::Scalar(input),
                ColumnarValue::Scalar(ScalarValue::Utf8(Some(charset.to_string()))),
            ],
            arg_fields: vec![input_field, charset_field],
            number_rows: 1,
            return_field: Arc::new(Field::new("encode", DataType::Binary, true)),
            config_options: Arc::new(config),
        })
    }

    /// Helper with ANSI mode disabled (legacy behavior).
    fn eval_encode_scalar(input: ScalarValue, charset: &str) -> Result<ColumnarValue> {
        eval_encode_scalar_with_ansi(input, charset, false)
    }

    fn expect_binary_scalar(result: ColumnarValue) -> Vec<u8> {
        match result {
            ColumnarValue::Scalar(ScalarValue::Binary(Some(bytes))) => bytes,
            other => panic!("Expected Binary scalar, got {other:?}"),
        }
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
    fn test_encode_utf16le() {
        let result =
            eval_encode_scalar(ScalarValue::Utf8(Some("AB".into())), "UTF-16LE").unwrap();
        assert_eq!(expect_binary_scalar(result), vec![0x41, 0x00, 0x42, 0x00]);
    }

    #[test]
    fn test_encode_ascii_unmappable_legacy_mode() {
        // Legacy mode: non-ASCII chars replaced with '?'
        let result = eval_encode_scalar(
            ScalarValue::Utf8(Some("\u{00E9}".into())), // é
            "US-ASCII",
        )
        .unwrap();
        assert_eq!(expect_binary_scalar(result), vec![b'?']);
    }

    #[test]
    fn test_encode_ascii_unmappable_ansi_mode() {
        // ANSI mode: non-ASCII chars cause an error
        let result = eval_encode_scalar_with_ansi(
            ScalarValue::Utf8(Some("\u{00E9}".into())),
            "US-ASCII",
            true,
        );
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("cannot encode"));
    }

    #[test]
    fn test_encode_iso8859_unmappable_legacy_mode() {
        // Legacy mode: chars > U+00FF replaced with '?'
        let result = eval_encode_scalar(
            ScalarValue::Utf8(Some("\u{0100}".into())), // Ā (U+0100)
            "ISO-8859-1",
        )
        .unwrap();
        assert_eq!(expect_binary_scalar(result), vec![b'?']);
    }

    #[test]
    fn test_encode_iso8859_unmappable_ansi_mode() {
        // ANSI mode: chars > U+00FF cause an error
        let result = eval_encode_scalar_with_ansi(
            ScalarValue::Utf8(Some("\u{0100}".into())),
            "ISO-8859-1",
            true,
        );
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("cannot encode"));
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

    #[test]
    fn test_encode_large_binary_input() {
        let result = eval_encode_scalar(
            ScalarValue::LargeBinary(Some(b"Hello".to_vec())),
            "UTF-8",
        )
        .unwrap();
        assert_eq!(expect_binary_scalar(result), b"Hello");
    }

    #[test]
    fn test_encode_binary_view_input() {
        let result =
            eval_encode_scalar(ScalarValue::BinaryView(Some(b"Hello".to_vec())), "UTF-8")
                .unwrap();
        assert_eq!(expect_binary_scalar(result), b"Hello");
    }

    #[test]
    fn test_encode_emoji_utf8() {
        // U+1F600 (😀) is 4 bytes in UTF-8: F0 9F 98 80
        let result =
            eval_encode_scalar(ScalarValue::Utf8(Some("😀".into())), "UTF-8").unwrap();
        assert_eq!(expect_binary_scalar(result), vec![0xF0, 0x9F, 0x98, 0x80]);
    }

    #[test]
    fn test_encode_emoji_utf16be() {
        // U+1F600 (😀) is a surrogate pair in UTF-16: D83D DE00
        let result =
            eval_encode_scalar(ScalarValue::Utf8(Some("😀".into())), "UTF-16BE").unwrap();
        assert_eq!(expect_binary_scalar(result), vec![0xD8, 0x3D, 0xDE, 0x00]);
    }

    #[test]
    fn test_encode_emoji_utf16le() {
        // U+1F600 (😀) surrogate pair in little-endian: 3DD8 00DE
        let result =
            eval_encode_scalar(ScalarValue::Utf8(Some("😀".into())), "UTF-16LE").unwrap();
        assert_eq!(expect_binary_scalar(result), vec![0x3D, 0xD8, 0x00, 0xDE]);
    }

    #[test]
    fn test_encode_emoji_utf16_with_bom() {
        // UTF-16 = BOM (FEFF) + UTF-16BE surrogate pair
        let result =
            eval_encode_scalar(ScalarValue::Utf8(Some("😀".into())), "UTF-16").unwrap();
        assert_eq!(
            expect_binary_scalar(result),
            vec![0xFE, 0xFF, 0xD8, 0x3D, 0xDE, 0x00]
        );
    }

    #[test]
    fn test_encode_utf32le() {
        // 'A' = U+0041 → 41 00 00 00, 'B' = U+0042 → 42 00 00 00
        let result =
            eval_encode_scalar(ScalarValue::Utf8(Some("AB".into())), "UTF-32LE").unwrap();
        assert_eq!(
            expect_binary_scalar(result),
            vec![0x41, 0x00, 0x00, 0x00, 0x42, 0x00, 0x00, 0x00]
        );
    }

    #[test]
    fn test_encode_utf32_with_bom() {
        // UTF-32 = BOM (0000FEFF) + UTF-32BE
        let result =
            eval_encode_scalar(ScalarValue::Utf8(Some("A".into())), "UTF-32").unwrap();
        assert_eq!(
            expect_binary_scalar(result),
            vec![0x00, 0x00, 0xFE, 0xFF, 0x00, 0x00, 0x00, 0x41]
        );
    }

    #[test]
    fn test_encode_emoji_utf32be() {
        // U+1F600 (😀) → 00 01 F6 00
        let result =
            eval_encode_scalar(ScalarValue::Utf8(Some("😀".into())), "UTF-32BE").unwrap();
        assert_eq!(expect_binary_scalar(result), vec![0x00, 0x01, 0xF6, 0x00]);
    }

    #[test]
    fn test_encode_emoji_utf32le() {
        // U+1F600 (😀) → 00 F6 01 00 (little-endian)
        let result =
            eval_encode_scalar(ScalarValue::Utf8(Some("😀".into())), "UTF-32LE").unwrap();
        assert_eq!(expect_binary_scalar(result), vec![0x00, 0xF6, 0x01, 0x00]);
    }

    #[test]
    fn test_encode_emoji_utf32_with_bom() {
        // UTF-32 = BOM (0000FEFF) + UTF-32BE: 00 01 F6 00
        let result =
            eval_encode_scalar(ScalarValue::Utf8(Some("😀".into())), "UTF-32").unwrap();
        assert_eq!(
            expect_binary_scalar(result),
            vec![0x00, 0x00, 0xFE, 0xFF, 0x00, 0x01, 0xF6, 0x00]
        );
    }

    /// Simple hex encoding for test assertions.
    fn hex_encode(bytes: &[u8]) -> String {
        bytes.iter().map(|b| format!("{b:02X}")).collect::<String>()
    }
}
