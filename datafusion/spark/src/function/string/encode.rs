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
/// Follows Spark 4.0 and later by default. Two options under `datafusion.spark`
/// restore the Spark 3.x behavior; both mirror a `spark.sql.legacy.*` config and
/// default to `false`, as in Spark:
///
/// - Charset names: only `us-ascii`, `iso-8859-1`, `utf-8`, `utf-16be`,
///   `utf-16le`, `utf-16` and `utf-32` (case-insensitive) are accepted. Any other
///   name, including aliases such as `UTF8`, `LATIN1` or `UTF-32BE`, raises
///   `[INVALID_PARAMETER_VALUE.CHARSET]`. Setting `legacy_java_charsets`
///   (`spark.sql.legacy.javaCharsets`) to `true` also accepts those aliases and
///   `UTF-32BE` / `UTF-32LE`.
/// - Unmappable characters: a character the charset cannot represent (non-ASCII
///   in `US-ASCII`, code points above `U+00FF` in `ISO-8859-1`) raises
///   `[MALFORMED_CHARACTER_CODING]`. Setting `legacy_coding_error_action`
///   (`spark.sql.legacy.codingErrorAction`) to `true` replaces it with `?`.
///
/// `UTF-32` is `UTF-32BE` without a BOM, as in Spark. Spark 3.x accepted any Java
/// charset name; names outside the set listed above are not implemented here and
/// raise an error even when `legacy_java_charsets` is `true`.
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

/// A charset that `encode` can produce.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Charset {
    UsAscii,
    Iso88591,
    Utf8,
    Utf16Be,
    Utf16Le,
    /// A BOM (big-endian marker) followed by UTF-16BE.
    Utf16,
    /// Also the target of `UTF-32`, which Spark encodes without a BOM.
    Utf32Be,
    Utf32Le,
}

/// The charset names Spark 4.0 and later accept, as listed in its error message.
const SPARK_4_CHARSETS: &str =
    "iso-8859-1, us-ascii, utf-16, utf-16be, utf-16le, utf-32, utf-8";

/// The charset names accepted when `legacy_java_charsets` is set, for the error
/// message of a name that is still unknown.
const LEGACY_CHARSETS: &str =
    "US-ASCII, ISO-8859-1, UTF-8, UTF-16, UTF-16BE, UTF-16LE, UTF-32, UTF-32BE, UTF-32LE";

/// Resolves a charset name, case-insensitively. With `legacy_java_charsets` it also
/// accepts the aliases Spark 3.x took from Java, such as `UTF8` and `LATIN1`.
fn resolve_charset(name: &str, legacy_java_charsets: bool) -> Result<Charset> {
    let lower = name.to_ascii_lowercase();
    let charset = match lower.as_str() {
        "us-ascii" => Some(Charset::UsAscii),
        "iso-8859-1" => Some(Charset::Iso88591),
        "utf-8" => Some(Charset::Utf8),
        "utf-16be" => Some(Charset::Utf16Be),
        "utf-16le" => Some(Charset::Utf16Le),
        "utf-16" => Some(Charset::Utf16),
        "utf-32" => Some(Charset::Utf32Be),
        _ if legacy_java_charsets => match lower.as_str() {
            "utf8" => Some(Charset::Utf8),
            "ascii" => Some(Charset::UsAscii),
            "iso88591" | "latin1" => Some(Charset::Iso88591),
            "utf16be" => Some(Charset::Utf16Be),
            "utf16le" => Some(Charset::Utf16Le),
            "utf16" => Some(Charset::Utf16),
            "utf32" | "utf-32be" | "utf32be" => Some(Charset::Utf32Be),
            "utf-32le" | "utf32le" => Some(Charset::Utf32Le),
            _ => None,
        },
        _ => None,
    };
    match charset {
        Some(charset) => Ok(charset),
        None if legacy_java_charsets => exec_err!(
            "Unsupported charset for encode: '{name}'. Supported: {LEGACY_CHARSETS}"
        ),
        None => exec_err!(
            "[INVALID_PARAMETER_VALUE.CHARSET] The value of parameter(s) `charset` in \
             `encode` is invalid: expects one of the {SPARK_4_CHARSETS}, but got {name}."
        ),
    }
}

/// Encodes `s` into `bytes` for a charset that maps each code point up to `max` to
/// one byte of the same value. Any other character becomes `?` if
/// `replace_unmappable`, and is an error otherwise.
fn encode_single_byte(
    s: &str,
    max: u32,
    charset_name: &str,
    replace_unmappable: bool,
    bytes: &mut Vec<u8>,
) -> Result<()> {
    for c in s.chars() {
        let code_point = c as u32;
        if code_point <= max {
            bytes.push(code_point as u8);
        } else if replace_unmappable {
            bytes.push(b'?');
        } else {
            return exec_err!(
                "[MALFORMED_CHARACTER_CODING] Invalid value found when performing \
                 `encode` with {charset_name}"
            );
        }
    }
    Ok(())
}

/// Encodes a single string value using the specified charset into `bytes`,
/// replacing its previous contents. `charset_name` is the name as the user wrote it,
/// used in error messages. Only `US-ASCII` and `ISO-8859-1` can have unmappable
/// characters, since every Unicode scalar value is representable in the others.
fn encode_string(
    s: &str,
    charset: Charset,
    charset_name: &str,
    replace_unmappable: bool,
    bytes: &mut Vec<u8>,
) -> Result<()> {
    bytes.clear();
    match charset {
        Charset::Utf8 => bytes.extend_from_slice(s.as_bytes()),
        Charset::UsAscii => {
            encode_single_byte(s, 0x7F, charset_name, replace_unmappable, bytes)?
        }
        Charset::Iso88591 => {
            encode_single_byte(s, 0xFF, charset_name, replace_unmappable, bytes)?
        }
        Charset::Utf16Be => {
            for code_unit in s.encode_utf16() {
                bytes.extend_from_slice(&code_unit.to_be_bytes());
            }
        }
        Charset::Utf16Le => {
            for code_unit in s.encode_utf16() {
                bytes.extend_from_slice(&code_unit.to_le_bytes());
            }
        }
        Charset::Utf16 => {
            bytes.extend_from_slice(&[0xFE, 0xFF]);
            for code_unit in s.encode_utf16() {
                bytes.extend_from_slice(&code_unit.to_be_bytes());
            }
        }
        Charset::Utf32Be => {
            for c in s.chars() {
                bytes.extend_from_slice(&(c as u32).to_be_bytes());
            }
        }
        Charset::Utf32Le => {
            for c in s.chars() {
                bytes.extend_from_slice(&(c as u32).to_le_bytes());
            }
        }
    }
    Ok(())
}

/// How `encode` treats the behavior that differs between Spark 3.x and 4.x,
/// taken from `datafusion.spark.*`.
#[derive(Debug, Clone, Copy)]
struct EncodeOptions {
    legacy_java_charsets: bool,
    replace_unmappable: bool,
}

/// The charset for each row. `Constant` resolves the charset once, but only
/// reports an unknown name from a row that is actually encoded; `PerRow` resolves
/// each row's charset on demand, yielding `None` for null entries.
enum Charsets<'a> {
    Constant {
        name: &'a str,
        resolved: Option<Charset>,
    },
    PerRow(&'a StringArray),
}

impl<'a> Charsets<'a> {
    fn constant(name: &'a str, options: EncodeOptions) -> Self {
        Charsets::Constant {
            name,
            resolved: resolve_charset(name, options.legacy_java_charsets).ok(),
        }
    }

    /// The charset and the name it was given by, or `None` if the row's charset is
    /// null.
    fn get(&self, i: usize, options: EncodeOptions) -> Result<Option<(Charset, &str)>> {
        let (name, resolved) = match self {
            Charsets::Constant { name, resolved } => (*name, *resolved),
            Charsets::PerRow(array) => {
                if array.is_null(i) {
                    return Ok(None);
                }
                (array.value(i), None)
            }
        };
        let charset = match resolved {
            Some(charset) => charset,
            None => resolve_charset(name, options.legacy_java_charsets)?,
        };
        Ok(Some((charset, name)))
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
    options: EncodeOptions,
) -> Result<ArrayRef> {
    let mut builder = BinaryBuilder::with_capacity(len, len * 4);
    let mut bytes = Vec::new();
    for i in 0..len {
        if is_null(i) {
            builder.append_null();
            continue;
        }
        match charsets.get(i, options)? {
            Some((charset, name)) => {
                encode_string(
                    &decode(i),
                    charset,
                    name,
                    options.replace_unmappable,
                    &mut bytes,
                )?;
                builder.append_value(&bytes);
            }
            None => builder.append_null(),
        }
    }
    Ok(Arc::new(builder.finish()))
}

fn encode_array<'a, S: StringArrayType<'a>>(
    array: &S,
    charsets: &Charsets,
    options: EncodeOptions,
) -> Result<ArrayRef> {
    encode_rows(
        array.len(),
        |i| array.is_null(i),
        |i| Cow::Borrowed(array.value(i)),
        charsets,
        options,
    )
}

fn encode_binary_array<'a, B: arrow::array::BinaryArrayType<'a>>(
    array: &'a B,
    charsets: &Charsets,
    options: EncodeOptions,
) -> Result<ArrayRef> {
    encode_rows(
        array.len(),
        |i| array.is_null(i),
        |i| String::from_utf8_lossy(array.value(i)),
        charsets,
        options,
    )
}

fn encode_dispatch(
    arr: &ArrayRef,
    charsets: &Charsets,
    options: EncodeOptions,
) -> Result<ArrayRef> {
    match arr.data_type() {
        DataType::Utf8 => encode_array(&arr.as_string::<i32>(), charsets, options),
        DataType::LargeUtf8 => encode_array(&arr.as_string::<i64>(), charsets, options),
        DataType::Utf8View => encode_array(&arr.as_string_view(), charsets, options),
        DataType::Binary => {
            encode_binary_array(&arr.as_binary::<i32>(), charsets, options)
        }
        DataType::LargeBinary => {
            encode_binary_array(&arr.as_binary::<i64>(), charsets, options)
        }
        DataType::BinaryView => {
            encode_binary_array(&arr.as_binary_view(), charsets, options)
        }
        DataType::Null => Ok(Arc::new(BinaryArray::new_null(arr.len()))),
        dt => exec_err!("encode expects a string or binary argument, got {dt:?}"),
    }
}

/// The charset name from a scalar, or `None` if it is null.
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
        let spark_options = &args.config_options.spark;
        let options = EncodeOptions {
            legacy_java_charsets: spark_options.legacy_java_charsets,
            replace_unmappable: spark_options.legacy_coding_error_action,
        };
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
                Some(charset) => encode_dispatch(
                    &value_arr,
                    &Charsets::constant(charset, options),
                    options,
                )?,
                None => {
                    return Ok(ColumnarValue::Scalar(ScalarValue::Binary(None)));
                }
            },
            ColumnarValue::Array(charset_array) => encode_dispatch(
                &value_arr,
                &Charsets::PerRow(charset_array.as_string::<i32>()),
                options,
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

    /// Runs `encode` over `values` with the given charset argument and the two
    /// `datafusion.spark` legacy options.
    fn run_encode(
        values: &[Option<&str>],
        charset: ColumnarValue,
        legacy_java_charsets: bool,
        legacy_coding_error_action: bool,
    ) -> Result<Vec<Option<Vec<u8>>>> {
        let mut config = ConfigOptions::default();
        config.spark.legacy_java_charsets = legacy_java_charsets;
        config.spark.legacy_coding_error_action = legacy_coding_error_action;

        let result = SparkEncode::new().invoke_with_args(ScalarFunctionArgs {
            args: vec![
                ColumnarValue::Array(Arc::new(StringArray::from(values.to_vec()))),
                charset,
            ],
            arg_fields: vec![
                Arc::new(Field::new("value", DataType::Utf8, true)),
                Arc::new(Field::new("charset", DataType::Utf8, true)),
            ],
            number_rows: values.len(),
            return_field: Arc::new(Field::new("encode", DataType::Binary, true)),
            config_options: Arc::new(config),
        })?;
        let ColumnarValue::Array(array) = result else {
            panic!("expected an array result");
        };
        Ok(array
            .as_binary::<i32>()
            .iter()
            .map(|v| v.map(<[u8]>::to_vec))
            .collect())
    }

    fn charset(name: &str) -> ColumnarValue {
        ColumnarValue::Scalar(ScalarValue::Utf8(Some(name.to_string())))
    }

    /// Encodes one string with a constant charset.
    fn encode_one(
        value: &str,
        name: &str,
        legacy_java_charsets: bool,
        legacy_coding_error_action: bool,
    ) -> Result<Vec<u8>> {
        let rows = run_encode(
            &[Some(value)],
            charset(name),
            legacy_java_charsets,
            legacy_coding_error_action,
        )?;
        Ok(rows.into_iter().next().unwrap().unwrap())
    }

    #[test]
    fn test_spark4_accepted_charsets() {
        // the seven names Spark 4 accepts, in any case
        for (name, expected) in [
            ("us-ascii", vec![0x41]),
            ("US-ASCII", vec![0x41]),
            ("iso-8859-1", vec![0x41]),
            ("utf-8", vec![0x41]),
            ("Utf-8", vec![0x41]),
            ("utf-16be", vec![0x00, 0x41]),
            ("utf-16le", vec![0x41, 0x00]),
            ("utf-16", vec![0xFE, 0xFF, 0x00, 0x41]),
            ("utf-32", vec![0x00, 0x00, 0x00, 0x41]),
            ("UTF-32", vec![0x00, 0x00, 0x00, 0x41]),
        ] {
            assert_eq!(
                encode_one("A", name, false, false).unwrap(),
                expected,
                "{name}"
            );
        }
    }

    #[test]
    fn test_spark4_rejects_other_charsets() {
        for name in [
            "utf-32be", "utf-32le", "UTF8", "ascii", "latin1", "iso88591", "utf16",
            "EBCDIC", "",
        ] {
            let err = encode_one("A", name, false, false).unwrap_err().to_string();
            assert!(
                err.contains("[INVALID_PARAMETER_VALUE.CHARSET]"),
                "{name}: {err}"
            );
            assert!(err.contains("`encode`"), "{name}: {err}");
            assert!(
                err.contains(
                    "iso-8859-1, us-ascii, utf-16, utf-16be, utf-16le, utf-32, utf-8"
                ),
                "{name}: {err}"
            );
            assert!(err.contains(&format!("but got {name}.")), "{name}: {err}");
        }
    }

    #[test]
    fn test_spark4_unmappable_characters_error() {
        // 'é' (U+00E9) is not ASCII; 'Ā' (U+0100) and an emoji are not Latin-1
        for (value, name) in [
            ("é", "us-ascii"),
            ("aé", "US-ASCII"),
            ("Ā", "iso-8859-1"),
            ("😀", "ISO-8859-1"),
        ] {
            let err = encode_one(value, name, false, false)
                .unwrap_err()
                .to_string();
            assert!(
                err.contains("[MALFORMED_CHARACTER_CODING]"),
                "{value}: {err}"
            );
            assert!(err.contains("`encode`"), "{value}: {err}");
            // the charset appears as the user wrote it
            assert!(err.ends_with(&format!("with {name}")), "{value}: {err}");
        }
    }

    #[test]
    fn test_spark4_mappable_characters_succeed() {
        // 'é' is U+00E9, which Latin-1 represents; every charset below it holds
        // any Unicode scalar value
        assert_eq!(
            encode_one("é", "iso-8859-1", false, false).unwrap(),
            vec![0xE9]
        );
        assert_eq!(
            encode_one("\u{7f}", "us-ascii", false, false).unwrap(),
            vec![0x7F]
        );
        assert_eq!(encode_one("é😀", "utf-8", false, false).unwrap().len(), 6);
        assert_eq!(
            encode_one("😀", "utf-32", false, false).unwrap(),
            vec![0x00, 0x01, 0xF6, 0x00]
        );
        assert_eq!(
            encode_one("", "us-ascii", false, false).unwrap(),
            Vec::<u8>::new()
        );
    }

    #[test]
    fn test_legacy_java_charsets() {
        for (name, expected) in [
            ("UTF8", vec![0x41]),
            ("ascii", vec![0x41]),
            ("LATIN1", vec![0x41]),
            ("ISO88591", vec![0x41]),
            ("utf-32be", vec![0x00, 0x00, 0x00, 0x41]),
            ("UTF32BE", vec![0x00, 0x00, 0x00, 0x41]),
            ("utf-32le", vec![0x41, 0x00, 0x00, 0x00]),
            ("UTF32LE", vec![0x41, 0x00, 0x00, 0x00]),
            ("UTF16", vec![0xFE, 0xFF, 0x00, 0x41]),
            ("UTF16BE", vec![0x00, 0x41]),
            ("UTF16LE", vec![0x41, 0x00]),
            ("UTF32", vec![0x00, 0x00, 0x00, 0x41]),
            // the seven Spark 4 names still work
            ("utf-8", vec![0x41]),
            ("utf-32", vec![0x00, 0x00, 0x00, 0x41]),
        ] {
            assert_eq!(
                encode_one("A", name, true, false).unwrap(),
                expected,
                "{name}"
            );
        }

        // a name outside the supported set still fails, without the Spark 4 code
        let err = encode_one("A", "EBCDIC", true, false)
            .unwrap_err()
            .to_string();
        assert!(
            err.contains("Unsupported charset for encode: 'EBCDIC'"),
            "{err}"
        );
        assert!(!err.contains("INVALID_PARAMETER_VALUE"), "{err}");
    }

    #[test]
    fn test_legacy_coding_error_action() {
        assert_eq!(encode_one("é", "us-ascii", false, true).unwrap(), b"?");
        assert_eq!(encode_one("aéb", "US-ASCII", false, true).unwrap(), b"a?b");
        // one '?' for each code point, including one above the BMP
        assert_eq!(encode_one("Ā😀", "iso-8859-1", false, true).unwrap(), b"??");
        assert_eq!(
            encode_one("é", "iso-8859-1", false, true).unwrap(),
            vec![0xE9]
        );
    }

    #[test]
    fn test_legacy_options_are_independent() {
        // legacy charsets alone still reject unmappable characters
        let err = encode_one("é", "ascii", true, false)
            .unwrap_err()
            .to_string();
        assert!(err.contains("[MALFORMED_CHARACTER_CODING]"), "{err}");

        // legacy coding error action alone still rejects aliases
        let err = encode_one("é", "ascii", false, true)
            .unwrap_err()
            .to_string();
        assert!(err.contains("[INVALID_PARAMETER_VALUE.CHARSET]"), "{err}");

        // both together are the Spark 3.5 behavior
        assert_eq!(encode_one("é", "ascii", true, true).unwrap(), b"?");
    }

    #[test]
    fn test_per_row_charsets() {
        let names = ColumnarValue::Array(Arc::new(StringArray::from(vec![
            Some("US-ASCII"),
            Some("utf-16be"),
            None,
        ])));
        let rows =
            run_encode(&[Some("a"), Some("b"), Some("c")], names, false, false).unwrap();
        assert_eq!(rows, vec![Some(vec![0x61]), Some(vec![0x00, 0x62]), None]);

        // a row's charset is checked on its own
        let names = ColumnarValue::Array(Arc::new(StringArray::from(vec![
            Some("utf-8"),
            Some("utf-32le"),
        ])));
        let err = run_encode(&[Some("a"), Some("b")], names, false, false)
            .unwrap_err()
            .to_string();
        assert!(err.contains("[INVALID_PARAMETER_VALUE.CHARSET]"), "{err}");
        assert!(err.contains("but got utf-32le."), "{err}");

        // an unmappable character in one row fails the query
        let names = ColumnarValue::Array(Arc::new(StringArray::from(vec![
            Some("utf-8"),
            Some("us-ascii"),
        ])));
        let err = run_encode(&[Some("a"), Some("é")], names, false, false)
            .unwrap_err()
            .to_string();
        assert!(err.contains("[MALFORMED_CHARACTER_CODING]"), "{err}");
    }

    #[test]
    fn test_nulls_are_not_validated() {
        // a NULL value or NULL charset yields NULL for the row, whatever the charset
        let rows = run_encode(
            &[None, Some("é")],
            ColumnarValue::Array(Arc::new(StringArray::from(vec![Some("bogus"), None]))),
            false,
            false,
        )
        .unwrap();
        assert_eq!(rows, vec![None, None]);
    }

    #[test]
    fn test_binary_input_with_invalid_utf8() {
        // Invalid UTF-8 becomes U+FFFD, which US-ASCII cannot represent: an error by
        // default, '?' with the legacy coding error action
        let args = |legacy_coding_error_action: bool| {
            let mut config = ConfigOptions::default();
            config.spark.legacy_coding_error_action = legacy_coding_error_action;
            ScalarFunctionArgs {
                args: vec![
                    ColumnarValue::Scalar(ScalarValue::Binary(Some(vec![0x61, 0xFF]))),
                    charset("us-ascii"),
                ],
                arg_fields: vec![
                    Arc::new(Field::new("value", DataType::Binary, true)),
                    Arc::new(Field::new("charset", DataType::Utf8, true)),
                ],
                number_rows: 1,
                return_field: Arc::new(Field::new("encode", DataType::Binary, true)),
                config_options: Arc::new(config),
            }
        };
        let err = SparkEncode::new()
            .invoke_with_args(args(false))
            .unwrap_err()
            .to_string();
        assert!(err.contains("[MALFORMED_CHARACTER_CODING]"), "{err}");

        let ColumnarValue::Scalar(ScalarValue::Binary(Some(bytes))) =
            SparkEncode::new().invoke_with_args(args(true)).unwrap()
        else {
            panic!("expected a binary scalar");
        };
        assert_eq!(bytes, b"a?");
    }
}
