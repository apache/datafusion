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

use arrow::array::{Array, ArrayRef, AsArray, BinaryBuilder};
use arrow::datatypes::DataType;
use base64::Engine as _;
use base64::engine::DecodePaddingMode;
use base64::engine::general_purpose::{GeneralPurpose, GeneralPurposeConfig};
use datafusion_common::{Result, ScalarValue, exec_err, plan_err};
use datafusion_expr::{
    ColumnarValue, ScalarFunctionArgs, ScalarUDFImpl, Signature, TypeSignature,
    Volatility,
};

use crate::function::math::unhex::unhex_scalar;

/// The binary formats accepted by `to_binary` / `try_to_binary`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum BinaryFormat {
    Hex,
    Utf8,
    Base64,
}

impl BinaryFormat {
    /// Spark matches the format case-insensitively against a fixed set.
    fn parse(fmt: &str) -> Option<Self> {
        match fmt.to_lowercase().as_str() {
            "hex" => Some(Self::Hex),
            "utf-8" | "utf8" => Some(Self::Utf8),
            "base64" => Some(Self::Base64),
            _ => None,
        }
    }
}

/// Spark-compatible `to_binary` expression
/// <https://spark.apache.org/docs/latest/api/sql/index.html#to_binary>
///
/// `to_binary(str[, fmt])` converts `str` to binary using `fmt`, which must be a
/// case-insensitive literal of `hex`, `utf-8`, `utf8` or `base64`. `fmt` defaults
/// to `hex`. The result is NULL if any input is NULL.
#[derive(Debug, PartialEq, Eq, Hash)]
pub struct SparkToBinary {
    signature: Signature,
}

impl Default for SparkToBinary {
    fn default() -> Self {
        Self::new()
    }
}

impl SparkToBinary {
    pub fn new() -> Self {
        Self {
            signature: to_binary_signature(),
        }
    }
}

impl ScalarUDFImpl for SparkToBinary {
    fn name(&self) -> &str {
        "to_binary"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Binary)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        to_binary_inner(self.name(), &args.args, true)
    }
}

/// Spark-compatible `try_to_binary` expression
/// <https://spark.apache.org/docs/latest/api/sql/index.html#try_to_binary>
///
/// Identical to [`SparkToBinary`] except that a value which cannot be converted,
/// and a format which is not recognised, both yield NULL instead of an error.
/// Spark expresses this as `TryEval(ToBinary(expr, fmt, nullOnInvalidFormat = true))`.
#[derive(Debug, PartialEq, Eq, Hash)]
pub struct SparkTryToBinary {
    signature: Signature,
}

impl Default for SparkTryToBinary {
    fn default() -> Self {
        Self::new()
    }
}

impl SparkTryToBinary {
    pub fn new() -> Self {
        Self {
            signature: to_binary_signature(),
        }
    }
}

impl ScalarUDFImpl for SparkTryToBinary {
    fn name(&self) -> &str {
        "try_to_binary"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Binary)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        to_binary_inner(self.name(), &args.args, false)
    }
}

fn to_binary_signature() -> Signature {
    let mut variants = Vec::with_capacity(12);
    for str_type in [DataType::Utf8, DataType::Utf8View, DataType::LargeUtf8] {
        variants.push(TypeSignature::Exact(vec![str_type.clone()]));
        for fmt_type in [DataType::Utf8, DataType::Utf8View, DataType::LargeUtf8] {
            variants.push(TypeSignature::Exact(vec![str_type.clone(), fmt_type]));
        }
    }
    Signature::one_of(variants, Volatility::Immutable)
}

/// Reads the format argument. Spark requires it to be foldable, so only a scalar
/// is accepted here; a NULL format makes the whole expression NULL.
fn read_format(
    name: &str,
    fmt: Option<&ColumnarValue>,
    fail_on_error: bool,
) -> Result<Option<BinaryFormat>> {
    let Some(fmt) = fmt else {
        return Ok(Some(BinaryFormat::Hex)); // the default when `fmt` is omitted
    };
    let ColumnarValue::Scalar(scalar) = fmt else {
        return plan_err!("{name}: the fmt argument must be a constant, not a column");
    };
    let fmt = match scalar {
        ScalarValue::Utf8(v) | ScalarValue::Utf8View(v) | ScalarValue::LargeUtf8(v) => v,
        other => {
            return plan_err!("{name}: the fmt argument must be a string, got {other:?}");
        }
    };
    match fmt {
        // A NULL format yields NULL rather than an error, in both variants.
        None => Ok(None),
        Some(fmt) => match BinaryFormat::parse(fmt) {
            Some(parsed) => Ok(Some(parsed)),
            // try_to_binary returns NULL for an unrecognised format
            // (`nullOnInvalidFormat`); to_binary rejects it.
            None if !fail_on_error => Ok(None),
            None => plan_err!(
                "{name}: invalid fmt '{fmt}', expected one of \
                 'hex', 'utf-8', 'utf8' or 'base64'"
            ),
        },
    }
}

fn to_binary_inner(
    name: &str,
    args: &[ColumnarValue],
    fail_on_error: bool,
) -> Result<ColumnarValue> {
    let (value, fmt) = match args {
        [value] => (value, None),
        [value, fmt] => (value, Some(fmt)),
        _ => {
            return exec_err!("{name} expects 1 or 2 arguments, got {}", args.len());
        }
    };

    let Some(format) = read_format(name, fmt, fail_on_error)? else {
        // NULL or (for try_to_binary) unrecognised format: the whole result is NULL.
        return Ok(ColumnarValue::Scalar(ScalarValue::Binary(None)));
    };

    match value {
        ColumnarValue::Array(array) => {
            let converted = convert_array(name, array, format, fail_on_error)?;
            Ok(ColumnarValue::Array(converted))
        }
        ColumnarValue::Scalar(scalar) => {
            let value = match scalar {
                ScalarValue::Utf8(v)
                | ScalarValue::Utf8View(v)
                | ScalarValue::LargeUtf8(v) => v.as_deref(),
                other => {
                    return exec_err!(
                        "{name}: expected a string argument, got {other:?}"
                    );
                }
            };
            let converted = match value {
                None => None,
                Some(v) => convert_one(name, v, format, fail_on_error)?,
            };
            Ok(ColumnarValue::Scalar(ScalarValue::Binary(converted)))
        }
    }
}

fn convert_array(
    name: &str,
    array: &ArrayRef,
    format: BinaryFormat,
    fail_on_error: bool,
) -> Result<ArrayRef> {
    let values: Vec<Option<&str>> = match array.data_type() {
        DataType::Utf8 => array.as_string::<i32>().iter().collect(),
        DataType::LargeUtf8 => array.as_string::<i64>().iter().collect(),
        DataType::Utf8View => array.as_string_view().iter().collect(),
        other => {
            return exec_err!("{name}: expected a string argument, got {other}");
        }
    };

    let mut builder = BinaryBuilder::with_capacity(values.len(), array.len());
    for value in values {
        match value {
            None => builder.append_null(),
            Some(v) => match convert_one(name, v, format, fail_on_error)? {
                Some(bytes) => builder.append_value(&bytes),
                None => builder.append_null(),
            },
        }
    }
    Ok(Arc::new(builder.finish()))
}

/// Converts one value. `Ok(None)` means "NULL", which only happens when
/// `fail_on_error` is false; otherwise an invalid value is an error.
fn convert_one(
    name: &str,
    value: &str,
    format: BinaryFormat,
    fail_on_error: bool,
) -> Result<Option<Vec<u8>>> {
    let converted = match format {
        BinaryFormat::Utf8 => Some(value.as_bytes().to_vec()),
        BinaryFormat::Hex => unhex_scalar(value),
        BinaryFormat::Base64 => decode_base64(value),
    };
    match converted {
        Some(bytes) => Ok(Some(bytes)),
        None if fail_on_error => exec_err!(
            "{name}: cannot convert '{value}' to binary using format '{}'",
            match format {
                BinaryFormat::Hex => "hex",
                BinaryFormat::Utf8 => "utf-8",
                BinaryFormat::Base64 => "base64",
            }
        ),
        None => Ok(None),
    }
}

/// The base64 engine Spark's decoding amounts to: the standard alphabet, the
/// padding optional, and the unused trailing bits of a short final group
/// ignored, as Java's MIME decoder does.
const SPARK_BASE64_DECODE: GeneralPurpose = GeneralPurpose::new(
    &base64::alphabet::STANDARD,
    GeneralPurposeConfig::new()
        .with_decode_allow_trailing_bits(true)
        .with_decode_padding_mode(DecodePaddingMode::Indifferent),
);

fn is_base64_byte(byte: u8) -> bool {
    byte.is_ascii_alphanumeric() || matches!(byte, b'+' | b'/' | b'=')
}

/// Decodes a base64 string, returning `None` if it is not valid base64.
///
/// Java's MIME decoder skips characters outside the alphabet, which the engine
/// has no setting for, so a failed decode is retried with those characters
/// removed.
fn decode_base64(value: &str) -> Option<Vec<u8>> {
    let bytes = value.as_bytes();
    match SPARK_BASE64_DECODE.decode(bytes) {
        Ok(decoded) => Some(decoded),
        Err(_) if bytes.iter().any(|byte| !is_base64_byte(*byte)) => {
            let filtered: Vec<u8> = bytes
                .iter()
                .copied()
                .filter(|b| is_base64_byte(*b))
                .collect();
            SPARK_BASE64_DECODE.decode(filtered).ok()
        }
        Err(_) => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::StringArray;
    use datafusion_common::internal_err;

    fn to_binary(value: &str, fmt: Option<&str>, fail: bool) -> Result<Option<Vec<u8>>> {
        let mut args = vec![ColumnarValue::Scalar(ScalarValue::Utf8(Some(
            value.to_string(),
        )))];
        if let Some(fmt) = fmt {
            args.push(ColumnarValue::Scalar(ScalarValue::Utf8(Some(
                fmt.to_string(),
            ))));
        }
        let name = if fail { "to_binary" } else { "try_to_binary" };
        match to_binary_inner(name, &args, fail)? {
            ColumnarValue::Scalar(ScalarValue::Binary(v)) => Ok(v),
            other => internal_err!("unexpected result {other:?}"),
        }
    }

    #[test]
    fn test_utf8_format() -> Result<()> {
        assert_eq!(
            to_binary("abc", Some("utf-8"), true)?,
            Some(b"abc".to_vec())
        );
        assert_eq!(to_binary("abc", Some("utf8"), true)?, Some(b"abc".to_vec()));
        assert_eq!(
            to_binary("abc", Some("UTF-8"), true)?,
            Some(b"abc".to_vec())
        );
        // multi-byte input round-trips as its UTF-8 bytes
        assert_eq!(to_binary("é", Some("utf-8"), true)?, Some(vec![0xc3, 0xa9]));
        Ok(())
    }

    #[test]
    fn test_hex_is_the_default_format() -> Result<()> {
        assert_eq!(
            to_binary("537061726B", None, true)?,
            Some(b"Spark".to_vec())
        );
        assert_eq!(
            to_binary("537061726B", Some("hex"), true)?,
            Some(b"Spark".to_vec())
        );
        // an odd number of digits is left-padded with '0'
        assert_eq!(to_binary("F", None, true)?, Some(vec![0x0f]));
        Ok(())
    }

    #[test]
    fn test_base64_format() -> Result<()> {
        assert_eq!(
            to_binary("U3Bhcms=", Some("base64"), true)?,
            Some(b"Spark".to_vec())
        );
        assert_eq!(
            to_binary("YWJj", Some("base64"), true)?,
            Some(b"abc".to_vec())
        );
        // whitespace between symbols is ignored
        assert_eq!(
            to_binary("U3Bh\ncms=", Some("base64"), true)?,
            Some(b"Spark".to_vec())
        );
        // an unpadded final group is accepted
        assert_eq!(
            to_binary("U3Bhcms", Some("base64"), true)?,
            Some(b"Spark".to_vec())
        );
        Ok(())
    }

    #[test]
    fn test_empty_input() -> Result<()> {
        for fmt in ["hex", "utf-8", "base64"] {
            assert_eq!(to_binary("", Some(fmt), true)?, Some(vec![]));
        }
        Ok(())
    }

    #[test]
    fn test_invalid_value_errors_or_nulls() -> Result<()> {
        // to_binary raises error, try_to_binary returns NULL
        assert!(to_binary("zz", Some("hex"), true).is_err());
        assert_eq!(to_binary("zz", Some("hex"), false)?, None);

        assert!(to_binary("a!", Some("base64"), true).is_err());
        assert_eq!(to_binary("a!", Some("base64"), false)?, None);
        Ok(())
    }

    #[test]
    fn test_invalid_format() -> Result<()> {
        // to_binary rejects an unrecognised format, try_to_binary returns NULL
        assert!(to_binary("abc", Some("invalidFormat"), true).is_err());
        assert_eq!(to_binary("abc", Some("invalidFormat"), false)?, None);
        Ok(())
    }

    #[test]
    fn test_null_inputs() -> Result<()> {
        // a NULL value yields NULL
        let args = vec![ColumnarValue::Scalar(ScalarValue::Utf8(None))];
        assert!(matches!(
            to_binary_inner("to_binary", &args, true)?,
            ColumnarValue::Scalar(ScalarValue::Binary(None))
        ));

        // a NULL format yields NULL, and is not an error even for to_binary
        let args = vec![
            ColumnarValue::Scalar(ScalarValue::Utf8(Some("abc".into()))),
            ColumnarValue::Scalar(ScalarValue::Utf8(None)),
        ];
        assert!(matches!(
            to_binary_inner("to_binary", &args, true)?,
            ColumnarValue::Scalar(ScalarValue::Binary(None))
        ));
        Ok(())
    }

    #[test]
    fn test_column_input() -> Result<()> {
        let array: ArrayRef =
            Arc::new(StringArray::from(vec![Some("abc"), None, Some("déf")]));
        let args = vec![
            ColumnarValue::Array(array),
            ColumnarValue::Scalar(ScalarValue::Utf8(Some("utf-8".into()))),
        ];
        let ColumnarValue::Array(result) = to_binary_inner("to_binary", &args, true)?
        else {
            unreachable!()
        };
        let result = result.as_binary::<i32>();
        assert_eq!(result.value(0), b"abc");
        assert!(result.is_null(1));
        assert_eq!(result.value(2), "déf".as_bytes());
        Ok(())
    }

    #[test]
    fn test_column_with_invalid_value() -> Result<()> {
        let array: ArrayRef = Arc::new(StringArray::from(vec![Some("4142"), Some("zz")]));
        let args = vec![
            ColumnarValue::Array(Arc::clone(&array)),
            ColumnarValue::Scalar(ScalarValue::Utf8(Some("hex".into()))),
        ];
        // to_binary fails the whole batch
        assert!(to_binary_inner("to_binary", &args, true).is_err());

        // try_to_binary nulls only the offending row
        let ColumnarValue::Array(result) =
            to_binary_inner("try_to_binary", &args, false)?
        else {
            unreachable!()
        };
        let result = result.as_binary::<i32>();
        assert_eq!(result.value(0), b"AB");
        assert!(result.is_null(1));
        Ok(())
    }

    #[test]
    fn test_base64_validation() {
        assert_eq!(decode_base64("YWJj"), Some(b"abc".to_vec()));
        assert_eq!(decode_base64(""), Some(vec![]));
        assert_eq!(decode_base64("a!"), None); // invalid character
        assert_eq!(decode_base64("YQ==="), None); // too much padding
        assert_eq!(decode_base64("YQ=x"), None); // data after padding
        assert_eq!(decode_base64("YWJjY"), None); // dangling symbol
        assert_eq!(decode_base64("YWJjYQ=="), Some(b"abca".to_vec()));
        assert_eq!(decode_base64("YWJj=="), None); // final group already complete
    }
}
