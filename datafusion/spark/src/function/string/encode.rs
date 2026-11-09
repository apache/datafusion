// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: See the NOTICE file distributed with this work for additional information regarding copyright ownership
// SPDX-FileContributor: Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements

use std::borrow::Cow;
use std::sync::Arc;

use arrow::array::{
    Array, ArrayRef, AsArray, BinaryArray, BinaryBuilder, StringArray, StringArrayType,
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
/// By default emulates Spark 4.0+ semantics:
/// - Accepts only the 7 canonical Spark charsets (`US-ASCII`, `ISO-8859-1`, `UTF-8`,
///   `UTF-16`, `UTF-16BE`, `UTF-16LE`, `UTF-32`). Non-canonical aliases (e.g., `UTF8`,
///   `LATIN1`, `ASCII`, `UTF-32BE`, `UTF-32LE`) raise `[INVALID_PARAMETER_VALUE.CHARSET]`.
///   Controlled by `datafusion.spark.legacy_java_charsets`.
/// - Unmappable characters (non-ASCII in `US-ASCII`, code points above `U+00FF` in
///   `ISO-8859-1`) raise `[MALFORMED_CHARACTER_CODING] ... SQLSTATE: 22000`.
///   Controlled by `datafusion.spark.legacy_coding_error_action`.
/// - `UTF-32` is encoded as big-endian without BOM, matching Spark 3.5 and 4.x.
///
/// Setting `datafusion.spark.legacy_java_charsets = true` restores Spark 3.5 charset
/// alias resolution, and setting `datafusion.spark.legacy_coding_error_action = true`
/// restores Spark 3.5 silent replacement of unmappable characters with `?`.
///
/// <https://spark.apache.org/docs/latest/api/sql/index.html#encode>
/// <https://spark.apache.org/docs/4.0.0/sql-migration-guide.html>
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

/// The seven canonical Spark 4.0 charset names (case-insensitive).
const CANONICAL_CHARSETS: [&str; 7] = [
    "US-ASCII",
    "ISO-8859-1",
    "UTF-8",
    "UTF-16",
    "UTF-16BE",
    "UTF-16LE",
    "UTF-32",
];

fn is_canonical_charset(upper: &str) -> bool {
    CANONICAL_CHARSETS.contains(&upper)
}

fn validate_charset(raw: &str, upper: &str, legacy_java_charsets: bool) -> Result<()> {
    if !legacy_java_charsets {
        if !is_canonical_charset(upper) {
            return exec_err!(
                "[INVALID_PARAMETER_VALUE.CHARSET] The value of parameter(s) 'charset' in 'encode' is invalid: expects one of the [US-ASCII, ISO-8859-1, UTF-8, UTF-16, UTF-16BE, UTF-16LE, UTF-32], but got '{raw}'."
            );
        }
    } else {
        match upper {
            "UTF-8" | "UTF8" | "US-ASCII" | "ASCII" | "ISO-8859-1" | "ISO88591"
            | "LATIN1" | "UTF-16BE" | "UTF16BE" | "UTF-16LE" | "UTF16LE" | "UTF-16"
            | "UTF16" | "UTF-32" | "UTF32" | "UTF-32BE" | "UTF32BE" | "UTF-32LE"
            | "UTF32LE" => {}
            _ => {
                return exec_err!(
                    "Unsupported charset for encode: '{raw}'. Supported: US-ASCII, ISO-8859-1, UTF-8, UTF-16, UTF-16BE, UTF-16LE, UTF-32, UTF-32BE, UTF-32LE"
                );
            }
        }
    }
    Ok(())
}

/// Encodes a single string value using the specified charset into `bytes`,
/// replacing its previous contents.
fn encode_string(
    s: &str,
    raw_charset: &str,
    upper_charset: &str,
    legacy_coding_error_action: bool,
    bytes: &mut Vec<u8>,
) -> Result<()> {
    bytes.clear();
    match upper_charset {
        "UTF-8" | "UTF8" => bytes.extend_from_slice(s.as_bytes()),
        "US-ASCII" | "ASCII" => {
            if legacy_coding_error_action {
                bytes
                    .extend(s.chars().map(|c| if c.is_ascii() { c as u8 } else { b'?' }));
            } else {
                let _ = bytes.try_reserve(s.len());
                for c in s.chars() {
                    if c.is_ascii() {
                        bytes.push(c as u8);
                    } else {
                        return exec_err!(
                            "[MALFORMED_CHARACTER_CODING] Invalid value found when performing 'encode' with {raw_charset}: character '{c}' (U+{:04X}) cannot be encoded. SQLSTATE: 22000",
                            c as u32
                        );
                    }
                }
            }
        }
        "ISO-8859-1" | "ISO88591" | "LATIN1" => {
            if legacy_coding_error_action {
                bytes.extend(s.chars().map(|c| {
                    let cp = c as u32;
                    if cp <= 255 { cp as u8 } else { b'?' }
                }));
            } else {
                let _ = bytes.try_reserve(s.len());
                for c in s.chars() {
                    let cp = c as u32;
                    if cp <= 255 {
                        bytes.push(cp as u8);
                    } else {
                        return exec_err!(
                            "[MALFORMED_CHARACTER_CODING] Invalid value found when performing 'encode' with {raw_charset}: character '{c}' (U+{:04X}) cannot be encoded. SQLSTATE: 22000",
                            cp
                        );
                    }
                }
            }
        }
        "UTF-16BE" | "UTF16BE" => {
            for code_unit in s.encode_utf16() {
                bytes.extend_from_slice(&code_unit.to_be_bytes());
            }
        }
        "UTF-16LE" | "UTF16LE" => {
            for code_unit in s.encode_utf16() {
                bytes.extend_from_slice(&code_unit.to_le_bytes());
            }
        }
        "UTF-16" | "UTF16" => {
            // BOM (big-endian marker) followed by UTF-16BE encoded bytes
            bytes.extend_from_slice(&[0xFE, 0xFF]);
            for code_unit in s.encode_utf16() {
                bytes.extend_from_slice(&code_unit.to_be_bytes());
            }
        }
        // Spark treats UTF-32 as UTF-32BE (no BOM), matching Spark 3.5 and 4.1.
        "UTF-32" | "UTF32" | "UTF-32BE" | "UTF32BE" => {
            for c in s.chars() {
                bytes.extend_from_slice(&(c as u32).to_be_bytes());
            }
        }
        "UTF-32LE" | "UTF32LE" => {
            for c in s.chars() {
                bytes.extend_from_slice(&(c as u32).to_le_bytes());
            }
        }
        _ => unreachable!("charset validation must precede encode_string"),
    }
    Ok(())
}

/// The charset for each row. `Constant` holds a single raw and uppercased charset;
/// `PerRow` retrieves and uppercases each row on demand, yielding `None` for null entries.
enum Charsets<'a> {
    Constant { raw: &'a str, upper: String },
    PerRow(&'a StringArray),
}

impl<'a> Charsets<'a> {
    fn constant(raw: &'a str) -> Self {
        let upper = raw.to_uppercase();
        Self::Constant { raw, upper }
    }

    fn per_row(array: &'a StringArray) -> Self {
        Self::PerRow(array)
    }

    fn is_constant(&self) -> bool {
        matches!(self, Self::Constant { .. })
    }

    fn get(&self, i: usize) -> Option<(Cow<'_, str>, Cow<'_, str>)> {
        match self {
            Charsets::Constant { raw, upper } => {
                Some((Cow::Borrowed(*raw), Cow::Borrowed(upper.as_str())))
            }
            Charsets::PerRow(array) => {
                if array.is_null(i) {
                    None
                } else {
                    let raw = array.value(i);
                    Some((Cow::Borrowed(raw), Cow::Owned(raw.to_uppercase())))
                }
            }
        }
    }
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
    legacy_java_charsets: bool,
    legacy_coding_error_action: bool,
) -> Result<ArrayRef> {
    let mut builder = BinaryBuilder::with_capacity(len, len * 4);
    let mut bytes = Vec::new();
    let is_constant = charsets.is_constant();
    for i in 0..len {
        match charsets.get(i) {
            Some((raw, upper)) => {
                if !is_constant {
                    validate_charset(&raw, &upper, legacy_java_charsets)?;
                }
                if is_null(i) {
                    builder.append_null();
                } else {
                    encode_string(
                        &decode(i),
                        &raw,
                        &upper,
                        legacy_coding_error_action,
                        &mut bytes,
                    )?;
                    builder.append_value(&bytes);
                }
            }
            None => builder.append_null(),
        }
    }
    Ok(Arc::new(builder.finish()))
}

fn encode_array<'a, S: StringArrayType<'a>>(
    array: &S,
    charsets: &Charsets,
    legacy_java_charsets: bool,
    legacy_coding_error_action: bool,
) -> Result<ArrayRef> {
    encode_rows(
        array.len(),
        |i| array.is_null(i),
        |i| Cow::Borrowed(array.value(i)),
        charsets,
        legacy_java_charsets,
        legacy_coding_error_action,
    )
}

fn encode_binary_array<'a, B: arrow::array::BinaryArrayType<'a>>(
    array: &'a B,
    charsets: &Charsets,
    legacy_java_charsets: bool,
    legacy_coding_error_action: bool,
) -> Result<ArrayRef> {
    encode_rows(
        array.len(),
        |i| array.is_null(i),
        |i| String::from_utf8_lossy(array.value(i)),
        charsets,
        legacy_java_charsets,
        legacy_coding_error_action,
    )
}

fn encode_dispatch(
    arr: &ArrayRef,
    charsets: &Charsets,
    legacy_java_charsets: bool,
    legacy_coding_error_action: bool,
) -> Result<ArrayRef> {
    match arr.data_type() {
        DataType::Utf8 => encode_array(
            &arr.as_string::<i32>(),
            charsets,
            legacy_java_charsets,
            legacy_coding_error_action,
        ),
        DataType::LargeUtf8 => encode_array(
            &arr.as_string::<i64>(),
            charsets,
            legacy_java_charsets,
            legacy_coding_error_action,
        ),
        DataType::Utf8View => encode_array(
            &arr.as_string_view(),
            charsets,
            legacy_java_charsets,
            legacy_coding_error_action,
        ),
        DataType::Binary => encode_binary_array(
            &arr.as_binary::<i32>(),
            charsets,
            legacy_java_charsets,
            legacy_coding_error_action,
        ),
        DataType::LargeBinary => encode_binary_array(
            &arr.as_binary::<i64>(),
            charsets,
            legacy_java_charsets,
            legacy_coding_error_action,
        ),
        DataType::BinaryView => encode_binary_array(
            &arr.as_binary_view(),
            charsets,
            legacy_java_charsets,
            legacy_coding_error_action,
        ),
        DataType::Null => Ok(Arc::new(BinaryArray::new_null(arr.len()))),
        dt => exec_err!("encode expects a string or binary argument, got {dt:?}"),
    }
}

/// The raw charset string from a scalar, or `None` if it is null.
fn scalar_charset(scalar: &ScalarValue) -> Result<Option<&str>> {
    match scalar {
        ScalarValue::Utf8(s) | ScalarValue::LargeUtf8(s) | ScalarValue::Utf8View(s) => {
            Ok(s.as_deref())
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

        // Preserve binary inputs because Arrow's binary-to-string cast rejects malformed UTF-8,
        // while Spark's `encode` replaces malformed bytes with U+FFFD.
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

        let legacy_java_charsets = args.config_options.spark.legacy_java_charsets;
        let legacy_coding_error_action =
            args.config_options.spark.legacy_coding_error_action;

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
                    let charsets = Charsets::constant(charset);
                    if let Some((raw, upper)) = charsets.get(0) {
                        validate_charset(&raw, &upper, legacy_java_charsets)?;
                    }
                    encode_dispatch(
                        &value_arr,
                        &charsets,
                        legacy_java_charsets,
                        legacy_coding_error_action,
                    )?
                }
                None => {
                    return Ok(ColumnarValue::Scalar(ScalarValue::Binary(None)));
                }
            },
            ColumnarValue::Array(charset_array) => encode_dispatch(
                &value_arr,
                &Charsets::per_row(charset_array.as_string::<i32>()),
                legacy_java_charsets,
                legacy_coding_error_action,
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
    use datafusion_common::config::ConfigOptions;

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

    fn invoke_encode(
        input: &str,
        charset: &str,
        legacy_charsets: bool,
        legacy_coding: bool,
    ) -> Result<Vec<u8>> {
        let func = SparkEncode::new();
        let mut config = ConfigOptions::default();
        config.spark.legacy_java_charsets = legacy_charsets;
        config.spark.legacy_coding_error_action = legacy_coding;

        let args = ScalarFunctionArgs {
            arg_fields: vec![],
            args: vec![
                ColumnarValue::Scalar(ScalarValue::Utf8(Some(input.to_string()))),
                ColumnarValue::Scalar(ScalarValue::Utf8(Some(charset.to_string()))),
            ],
            number_rows: 1,
            return_field: Arc::new(Field::new("encode", DataType::Binary, true)),
            config_options: Arc::new(config),
        };

        match func.invoke_with_args(args)? {
            ColumnarValue::Scalar(ScalarValue::Binary(Some(bytes))) => Ok(bytes),
            other => panic!("expected scalar binary, got {other:?}"),
        }
    }

    #[test]
    fn test_canonical_charsets_strict_mode() {
        // UTF-8
        let res = invoke_encode("Spark SQL", "utf-8", false, false).unwrap();
        assert_eq!(res, b"Spark SQL");

        // US-ASCII
        let res = invoke_encode("Hello", "US-ASCII", false, false).unwrap();
        assert_eq!(res, b"Hello");

        // ISO-8859-1 (naïve: ï = 0xEF)
        let res = invoke_encode("naïve", "iso-8859-1", false, false).unwrap();
        assert_eq!(res, &[0x6E, 0x61, 0xEF, 0x76, 0x65]);

        // UTF-16BE
        let res = invoke_encode("AB", "utf-16be", false, false).unwrap();
        assert_eq!(res, &[0x00, 0x41, 0x00, 0x42]);

        // UTF-16LE
        let res = invoke_encode("AB", "utf-16le", false, false).unwrap();
        assert_eq!(res, &[0x41, 0x00, 0x42, 0x00]);

        // UTF-16 (with BOM)
        let res = invoke_encode("AB", "utf-16", false, false).unwrap();
        assert_eq!(res, &[0xFE, 0xFF, 0x00, 0x41, 0x00, 0x42]);

        // UTF-32 (big-endian without BOM, matching Spark 4.0)
        let res = invoke_encode("A", "utf-32", false, false).unwrap();
        assert_eq!(res, &[0x00, 0x00, 0x00, 0x41]);

        // UTF-32 with emoji (non-BMP: 😀 = U+1F600)
        let res = invoke_encode("😀", "UTF-32", false, false).unwrap();
        assert_eq!(res, &[0x00, 0x01, 0xF6, 0x00]);
    }

    #[test]
    fn test_canonical_case_insensitivity() {
        let res1 = invoke_encode("hello", "Utf-8", false, false).unwrap();
        let res2 = invoke_encode("hello", "uS-AsCiI", false, false).unwrap();
        let res3 = invoke_encode("hello", "Iso-8859-1", false, false).unwrap();
        let res4 = invoke_encode("hello", "uTf-16bE", false, false).unwrap();
        let res5 = invoke_encode("hello", "UtF-16lE", false, false).unwrap();
        let res6 = invoke_encode("hello", "UtF-16", false, false).unwrap();
        let res7 = invoke_encode("hello", "UtF-32", false, false).unwrap();
        assert_eq!(res1, b"hello");
        assert_eq!(res2, b"hello");
        assert_eq!(res3, b"hello");
        assert_eq!(res4.len(), 10);
        assert_eq!(res5.len(), 10);
        assert_eq!(res6.len(), 12);
        assert_eq!(res7.len(), 20);
    }

    #[test]
    fn test_alias_rejection_strict_mode() {
        // Non-canonical aliases rejected under Spark 4.0 strict mode
        let aliases = [
            "utf8", "ascii", "latin1", "iso88591", "utf16", "utf16be", "utf16le",
            "utf32", "utf-32be", "utf-32le", "ebcdic",
        ];
        for alias in aliases {
            let err = invoke_encode("A", alias, false, false).unwrap_err();
            let msg = err.to_string();
            assert!(
                msg.contains("[INVALID_PARAMETER_VALUE.CHARSET]"),
                "expected INVALID_PARAMETER_VALUE.CHARSET for alias '{alias}', got: {msg}"
            );
            assert!(
                msg.contains(&format!("got '{alias}'")),
                "expected error to mention raw alias '{alias}', got: {msg}"
            );
        }
    }

    #[test]
    fn test_alias_acceptance_legacy_mode() {
        // When legacy_java_charsets = true, common aliases are accepted
        let res_utf8 = invoke_encode("Spark", "utf8", true, false).unwrap();
        assert_eq!(res_utf8, b"Spark");

        let res_ascii = invoke_encode("Spark", "ascii", true, false).unwrap();
        assert_eq!(res_ascii, b"Spark");

        let res_latin1 = invoke_encode("naïve", "latin1", true, false).unwrap();
        assert_eq!(res_latin1, &[0x6E, 0x61, 0xEF, 0x76, 0x65]);

        let res_utf32be = invoke_encode("A", "utf-32be", true, false).unwrap();
        assert_eq!(res_utf32be, &[0x00, 0x00, 0x00, 0x41]);

        let res_utf32le = invoke_encode("AB", "utf-32le", true, false).unwrap();
        assert_eq!(
            res_utf32le,
            &[0x41, 0x00, 0x00, 0x00, 0x42, 0x00, 0x00, 0x00]
        );

        // Truly unsupported charset still errors with "Unsupported charset for encode"
        let err = invoke_encode("A", "EBCDIC", true, false).unwrap_err();
        assert!(
            err.to_string()
                .contains("Unsupported charset for encode: 'EBCDIC'")
        );
    }

    #[test]
    fn test_unmappable_characters_strict_mode() {
        // US-ASCII with 'é' (non-ASCII)
        let err_ascii = invoke_encode("é", "us-ascii", false, false).unwrap_err();
        let msg_ascii = err_ascii.to_string();
        assert!(
            msg_ascii.contains("[MALFORMED_CHARACTER_CODING]"),
            "expected MALFORMED_CHARACTER_CODING for US-ASCII, got: {msg_ascii}"
        );
        assert!(
            msg_ascii.contains("with us-ascii"),
            "expected charset context in error, got: {msg_ascii}"
        );
        assert!(
            msg_ascii.contains("SQLSTATE: 22000"),
            "expected SQLSTATE: 22000 in error, got: {msg_ascii}"
        );

        // ISO-8859-1 with 'Ā' (U+0100 > 0xFF)
        let err_iso = invoke_encode("Ā", "iso-8859-1", false, false).unwrap_err();
        let msg_iso = err_iso.to_string();
        assert!(
            msg_iso.contains("[MALFORMED_CHARACTER_CODING]"),
            "expected MALFORMED_CHARACTER_CODING for ISO-8859-1, got: {msg_iso}"
        );
        assert!(
            msg_iso.contains("with iso-8859-1"),
            "expected charset context in error, got: {msg_iso}"
        );
        assert!(
            msg_iso.contains("SQLSTATE: 22000"),
            "expected SQLSTATE: 22000 in error, got: {msg_iso}"
        );
    }

    #[test]
    fn test_unmappable_characters_legacy_mode() {
        // US-ASCII with 'é' replaced with '?' (b'?')
        let res_ascii = invoke_encode("é", "us-ascii", false, true).unwrap();
        assert_eq!(res_ascii, &[b'?']);

        // ISO-8859-1 with 'Ā' replaced with '?' (b'?')
        let res_iso = invoke_encode("Ā", "iso-8859-1", false, true).unwrap();
        assert_eq!(res_iso, &[b'?']);

        // Mixed valid and invalid
        let res_mixed = invoke_encode("AéB", "us-ascii", false, true).unwrap();
        assert_eq!(res_mixed, &[b'A', b'?', b'B']);
    }

    #[test]
    fn test_independent_flags() {
        // 1. legacy_java_charsets = true, legacy_coding_error_action = false:
        // Alias 'ascii' is accepted, but 'é' triggers MALFORMED_CHARACTER_CODING
        let err = invoke_encode("é", "ascii", true, false).unwrap_err();
        assert!(err.to_string().contains("[MALFORMED_CHARACTER_CODING]"));

        // 2. legacy_java_charsets = false, legacy_coding_error_action = true:
        // 'us-ascii' accepted, and 'é' replaced with '?'
        let res = invoke_encode("é", "us-ascii", false, true).unwrap();
        assert_eq!(res, &[b'?']);

        // But non-canonical 'ascii' is still rejected
        let err2 = invoke_encode("é", "ascii", false, true).unwrap_err();
        assert!(
            err2.to_string()
                .contains("[INVALID_PARAMETER_VALUE.CHARSET]")
        );
    }

    #[test]
    fn test_empty_and_null_inputs() {
        let func = SparkEncode::new();
        let config = Arc::new(ConfigOptions::default());

        // Empty string
        let args = ScalarFunctionArgs {
            arg_fields: vec![],
            args: vec![
                ColumnarValue::Scalar(ScalarValue::Utf8(Some(String::new()))),
                ColumnarValue::Scalar(ScalarValue::Utf8(Some("utf-8".to_string()))),
            ],
            number_rows: 1,
            return_field: Arc::new(Field::new("encode", DataType::Binary, true)),
            config_options: Arc::clone(&config),
        };
        match func.invoke_with_args(args).unwrap() {
            ColumnarValue::Scalar(ScalarValue::Binary(Some(b))) => {
                assert_eq!(b, Vec::<u8>::new())
            }
            other => panic!("expected empty binary, got {other:?}"),
        }

        // Null value yields null
        let args_null_val = ScalarFunctionArgs {
            arg_fields: vec![],
            args: vec![
                ColumnarValue::Scalar(ScalarValue::Utf8(None)),
                ColumnarValue::Scalar(ScalarValue::Utf8(Some("utf-8".to_string()))),
            ],
            number_rows: 1,
            return_field: Arc::new(Field::new("encode", DataType::Binary, true)),
            config_options: Arc::clone(&config),
        };
        match func.invoke_with_args(args_null_val).unwrap() {
            ColumnarValue::Scalar(ScalarValue::Binary(None)) => {}
            other => panic!("expected null binary, got {other:?}"),
        }

        // Null charset yields null (NullIntolerant)
        let args_null_charset = ScalarFunctionArgs {
            arg_fields: vec![],
            args: vec![
                ColumnarValue::Scalar(ScalarValue::Utf8(Some("hello".to_string()))),
                ColumnarValue::Scalar(ScalarValue::Utf8(None)),
            ],
            number_rows: 1,
            return_field: Arc::new(Field::new("encode", DataType::Binary, true)),
            config_options: Arc::clone(&config),
        };
        match func.invoke_with_args(args_null_charset).unwrap() {
            ColumnarValue::Scalar(ScalarValue::Binary(None)) => {}
            other => panic!("expected null binary, got {other:?}"),
        }
    }

    #[test]
    fn test_per_row_charsets_array() {
        let func = SparkEncode::new();
        let config = Arc::new(ConfigOptions::default());

        let values = Arc::new(StringArray::from(vec![Some("hello"), Some("world")]));
        let charsets = Arc::new(StringArray::from(vec![Some("utf-8"), Some("utf-16be")]));

        let args = ScalarFunctionArgs {
            arg_fields: vec![],
            args: vec![ColumnarValue::Array(values), ColumnarValue::Array(charsets)],
            number_rows: 2,
            return_field: Arc::new(Field::new("encode", DataType::Binary, true)),
            config_options: Arc::clone(&config),
        };

        let res = func.invoke_with_args(args).unwrap();
        match res {
            ColumnarValue::Array(arr) => {
                let bin = arr.as_binary::<i32>();
                assert_eq!(bin.value(0), b"hello");
                assert_eq!(
                    bin.value(1),
                    &[0x00, 0x77, 0x00, 0x6F, 0x00, 0x72, 0x00, 0x6C, 0x00, 0x64]
                );
            }
            other => panic!("expected array, got {other:?}"),
        }

        // Array with invalid alias in strict mode should fail
        let bad_charsets = Arc::new(StringArray::from(vec![Some("utf-8"), Some("utf8")]));
        let bad_args = ScalarFunctionArgs {
            arg_fields: vec![],
            args: vec![
                ColumnarValue::Array(Arc::new(StringArray::from(vec![
                    Some("hello"),
                    Some("world"),
                ]))),
                ColumnarValue::Array(bad_charsets),
            ],
            number_rows: 2,
            return_field: Arc::new(Field::new("encode", DataType::Binary, true)),
            config_options: Arc::clone(&config),
        };
        let err = func.invoke_with_args(bad_args).unwrap_err();
        assert!(
            err.to_string()
                .contains("[INVALID_PARAMETER_VALUE.CHARSET]")
        );
    }
}
