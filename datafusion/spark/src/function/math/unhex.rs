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

use arrow::array::{Array, ArrayRef, BinaryBuilder};
use arrow::datatypes::DataType;
use datafusion_common::cast::{
    as_large_string_array, as_string_array, as_string_view_array,
};
use datafusion_common::types::logical_string;
use datafusion_common::{DataFusionError, Result, ScalarValue, exec_err};
use datafusion_expr::{
    Coercion, ColumnarValue, ScalarFunctionArgs, ScalarUDFImpl, Signature, TypeSignature,
    TypeSignatureClass, Volatility,
};
use std::any::Any;
use std::sync::Arc;

/// <https://spark.apache.org/docs/latest/api/sql/index.html#unhex>
#[derive(Debug, PartialEq, Eq, Hash)]
pub struct SparkUnhex {
    signature: Signature,
    aliases: Vec<String>,
}

impl Default for SparkUnhex {
    fn default() -> Self {
        Self::new()
    }
}

impl SparkUnhex {
    pub fn new() -> Self {
        let string = Coercion::new_exact(TypeSignatureClass::Native(logical_string()));
        // accepts string types (Utf8, Utf8View, LargeUtf8)
        let variants = vec![TypeSignature::Coercible(vec![string])];

        Self {
            signature: Signature::one_of(variants, Volatility::Immutable),
            aliases: vec![],
        }
    }
}

impl ScalarUDFImpl for SparkUnhex {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "unhex"
    }

    fn aliases(&self) -> &[String] {
        &self.aliases
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Binary)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        spark_unhex(&args.args)
    }
}

#[inline]
fn hex_nibble(c: u8) -> Option<u8> {
    match c {
        b'0'..=b'9' => Some(c - b'0'),
        b'a'..=b'f' => Some(c - b'a' + 10),
        b'A'..=b'F' => Some(c - b'A' + 10),
        _ => None,
    }
}
fn unhex_common(bytes: &[u8], out: &mut Vec<u8>) -> bool {
    if bytes.is_empty() {
        return true;
    }

    let mut i = 0usize;

    // If the hex string length is odd, implicitly left-pad with '0'.
    if (bytes.len() & 1) == 1 {
        match hex_nibble(bytes[0]) {
            // Equivalent to (0 << 4) | lo
            Some(lo) => out.push(lo),
            None => return false,
        }
        i = 1;
    }

    while i + 1 < bytes.len() {
        match (hex_nibble(bytes[i]), hex_nibble(bytes[i + 1])) {
            (Some(hi), Some(lo)) => out.push((hi << 4) | lo),
            _ => return false,
        }
        i += 2;
    }

    true
}

fn unhex_array<I, T>(iter: I, len: usize) -> Result<ArrayRef, DataFusionError>
where
    I: Iterator<Item = Option<T>>,
    T: AsRef<str>,
{
    let mut builder = BinaryBuilder::with_capacity(len, len * 32);
    let mut buffer = Vec::new();

    for v in iter {
        if let Some(s) = v {
            buffer.clear();
            buffer.reserve(s.as_ref().len().div_ceil(2));
            if unhex_common(s.as_ref().as_bytes(), &mut buffer) {
                builder.append_value(&buffer);
            } else {
                builder.append_null();
            }
        } else {
            builder.append_null();
        }
    }

    Ok(Arc::new(builder.finish()))
}

fn unhex_scalar(s: &str) -> Option<Vec<u8>> {
    let mut buffer = Vec::with_capacity(s.len().div_ceil(2));
    if unhex_common(s.as_bytes(), &mut buffer) {
        Some(buffer)
    } else {
        None
    }
}

pub fn spark_unhex(args: &[ColumnarValue]) -> Result<ColumnarValue, DataFusionError> {
    if args.len() != 1 {
        return exec_err!("unhex tasks exactly 1 argument, but got: {}", args.len());
    }

    match &args[0] {
        ColumnarValue::Array(array) => match array.data_type() {
            DataType::Utf8 => {
                let array = as_string_array(array)?;
                Ok(ColumnarValue::Array(unhex_array(
                    array.iter(),
                    array.len(),
                )?))
            }
            DataType::Utf8View => {
                let array = as_string_view_array(array)?;
                Ok(ColumnarValue::Array(unhex_array(
                    array.iter(),
                    array.len(),
                )?))
            }
            DataType::LargeUtf8 => {
                let array = as_large_string_array(array)?;
                Ok(ColumnarValue::Array(unhex_array(
                    array.iter(),
                    array.len(),
                )?))
            }
            _ => exec_err!(
                "unhex only supports string argument, but got: {}",
                array.data_type()
            ),
        },
        ColumnarValue::Scalar(sv) => match sv {
            ScalarValue::Utf8(None)
            | ScalarValue::Utf8View(None)
            | ScalarValue::LargeUtf8(None) => {
                Ok(ColumnarValue::Scalar(ScalarValue::Binary(None)))
            }
            ScalarValue::Utf8(Some(s))
            | ScalarValue::Utf8View(Some(s))
            | ScalarValue::LargeUtf8(Some(s)) => {
                Ok(ColumnarValue::Scalar(ScalarValue::Binary(unhex_scalar(s))))
            }
            _ => {
                exec_err!(
                    "unhex only supports string argument, but got: {}",
                    sv.data_type()
                )
            }
        },
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{BinaryArray, LargeStringArray, StringArray};
    use datafusion_common::cast::as_binary_array;
    use datafusion_expr::ColumnarValue;

    #[test]
    fn test_unhex_scalar_valid() {
        let test_cases = vec![
            ("414243", vec![0x41, 0x42, 0x43]),
            ("DEADBEEF", vec![0xDE, 0xAD, 0xBE, 0xEF]),
            ("deadbeef", vec![0xDE, 0xAD, 0xBE, 0xEF]),
            ("00", vec![0x00]),
            ("ff", vec![0xFF]),
            (
                "0123456789ABCDEF",
                vec![0x01, 0x23, 0x45, 0x67, 0x89, 0xAB, 0xCD, 0xEF],
            ),
        ];

        for (input, expected) in test_cases {
            let args = ColumnarValue::Scalar(ScalarValue::from(input));
            let result = spark_unhex(&[args]).unwrap();

            match result {
                ColumnarValue::Scalar(ScalarValue::Binary(Some(actual))) => {
                    assert_eq!(actual, expected, "Failed for input: {input}");
                }
                _ => panic!("Unexpected result type for input: {input}"),
            }
        }
    }

    #[test]
    fn test_unhex_scalar_odd_length() {
        let test_cases = vec![
            ("1A2B3", vec![0x01, 0xA2, 0xB3]),
            ("1", vec![0x01]),
            ("ABC", vec![0x0A, 0xBC]),
            ("123", vec![0x01, 0x23]),
        ];

        for (input, expected) in test_cases {
            let args = ColumnarValue::Scalar(ScalarValue::from(input));
            let result = spark_unhex(&[args]).unwrap();

            match result {
                ColumnarValue::Scalar(ScalarValue::Binary(Some(actual))) => {
                    assert_eq!(
                        actual, expected,
                        "Failed for odd-length input: {input}",
                    );
                }
                _ => panic!("Unexpected result type for odd-length input: {input}", ),
            }
        }
    }

    #[test]
    fn test_unhex_scalar_empty() {
        let args = ColumnarValue::Scalar(ScalarValue::from(""));
        let result = spark_unhex(&[args]).unwrap();

        match result {
            ColumnarValue::Scalar(ScalarValue::Binary(Some(actual))) => {
                assert_eq!(actual, Vec::<u8>::new());
            }
            _ => panic!("Expected binary value for empty string"),
        }
    }

    #[test]
    fn test_unhex_scalar_invalid() {
        let args = ColumnarValue::Scalar(ScalarValue::from("GG"));
        let result = spark_unhex(&[args]).unwrap();

        match result {
            ColumnarValue::Scalar(ScalarValue::Binary(None)) => {}
            _ => panic!("Expected null binary for invalid hex input"),
        }
    }

    #[test]
    fn test_unhex_scalar_null() {
        let args = ColumnarValue::Scalar(ScalarValue::Utf8(None));
        let result = spark_unhex(&[args]).unwrap();

        match result {
            ColumnarValue::Scalar(ScalarValue::Binary(None)) => {}
            _ => panic!("Expected null binary for null input"),
        }
    }

    #[test]
    fn test_unhex_array_utf8() {
        let input = StringArray::from(vec![
            Some("414243"),
            None,
            Some("DEAD"),
            Some("INVALID"),
            Some(""),
        ]);
        let expected = BinaryArray::from(vec![
            Some(&[0x41, 0x42, 0x43][..]),
            None,
            Some(&[0xDE, 0xAD][..]),
            None,
            Some(&[][..]),
        ]);

        let args = ColumnarValue::Array(Arc::new(input));
        let result = spark_unhex(&[args]).unwrap();

        match result {
            ColumnarValue::Array(array) => {
                let result = as_binary_array(&array).unwrap();
                assert_eq!(result, &expected);
            }
            _ => panic!("Expected array result"),
        }
    }

    #[test]
    fn test_unhex_array_large_utf8() {
        let input =
            LargeStringArray::from(vec![Some("414243"), Some("1A2B3"), None, Some("FF")]);
        let expected = BinaryArray::from(vec![
            Some(&[0x41, 0x42, 0x43][..]),
            Some(&[0x01, 0xA2, 0xB3][..]),
            None,
            Some(&[0xFF][..]),
        ]);

        let args = ColumnarValue::Array(Arc::new(input));
        let result = spark_unhex(&[args]).unwrap();

        match result {
            ColumnarValue::Array(array) => {
                let result = as_binary_array(&array).unwrap();
                assert_eq!(result, &expected);
            }
            _ => panic!("Expected array result"),
        }
    }

    #[test]
    fn test_unhex_array_mixed_cases() {
        let input = StringArray::from(vec![
            Some("deadbeef"),
            Some("DEADBEEF"),
            Some("AbCdEf"),
            Some("1234567890"),
        ]);
        let expected = BinaryArray::from(vec![
            Some(&[0xDE, 0xAD, 0xBE, 0xEF][..]),
            Some(&[0xDE, 0xAD, 0xBE, 0xEF][..]),
            Some(&[0xAB, 0xCD, 0xEF][..]),
            Some(&[0x12, 0x34, 0x56, 0x78, 0x90][..]),
        ]);

        let args = ColumnarValue::Array(Arc::new(input));
        let result = spark_unhex(&[args]).unwrap();

        match result {
            ColumnarValue::Array(array) => {
                let result = as_binary_array(&array).unwrap();
                assert_eq!(result, &expected);
            }
            _ => panic!("Expected array result"),
        }
    }

    #[test]
    fn test_unhex_array_empty() {
        let input: StringArray = StringArray::from(Vec::<&str>::new());
        let expected: BinaryArray = BinaryArray::from(Vec::<Option<&[u8]>>::new());

        let args = ColumnarValue::Array(Arc::new(input));
        let result = spark_unhex(&[args]).unwrap();

        match result {
            ColumnarValue::Array(array) => {
                let result = as_binary_array(&array).unwrap();
                assert_eq!(result, &expected);
            }
            _ => panic!("Expected array result"),
        }
    }
}
