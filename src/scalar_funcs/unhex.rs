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

use arrow_array::OffsetSizeTrait;
use arrow_schema::DataType;
use datafusion::logical_expr::ColumnarValue;
use datafusion_common::{cast::as_generic_string_array, exec_err, DataFusionError, ScalarValue};

/// Helper function to convert a hex digit to a binary value.
fn unhex_digit(c: u8) -> Result<u8, DataFusionError> {
    match c {
        b'0'..=b'9' => Ok(c - b'0'),
        b'A'..=b'F' => Ok(10 + c - b'A'),
        b'a'..=b'f' => Ok(10 + c - b'a'),
        _ => Err(DataFusionError::Execution(
            "Input to unhex_digit is not a valid hex digit".to_string(),
        )),
    }
}

/// Convert a hex string to binary and store the result in `result`. Returns an error if the input
/// is not a valid hex string.
fn unhex(hex_str: &str, result: &mut Vec<u8>) -> Result<(), DataFusionError> {
    let bytes = hex_str.as_bytes();

    let mut i = 0;

    if (bytes.len() & 0x01) != 0 {
        let v = unhex_digit(bytes[0])?;

        result.push(v);
        i += 1;
    }

    while i < bytes.len() {
        let first = unhex_digit(bytes[i])?;
        let second = unhex_digit(bytes[i + 1])?;
        result.push((first << 4) | second);

        i += 2;
    }

    Ok(())
}

fn spark_unhex_inner<T: OffsetSizeTrait>(
    array: &ColumnarValue,
    fail_on_error: bool,
) -> Result<ColumnarValue, DataFusionError> {
    match array {
        ColumnarValue::Array(array) => {
            let string_array = as_generic_string_array::<T>(array)?;

            let mut encoded = Vec::new();
            let mut builder = arrow::array::BinaryBuilder::new();

            for item in string_array.iter() {
                if let Some(s) = item {
                    if unhex(s, &mut encoded).is_ok() {
                        builder.append_value(encoded.as_slice());
                    } else if fail_on_error {
                        return exec_err!("Input to unhex is not a valid hex string: {s}");
                    } else {
                        builder.append_null();
                    }
                    encoded.clear();
                } else {
                    builder.append_null();
                }
            }
            Ok(ColumnarValue::Array(Arc::new(builder.finish())))
        }
        ColumnarValue::Scalar(ScalarValue::Utf8(Some(string))) => {
            let mut encoded = Vec::new();

            if unhex(string, &mut encoded).is_ok() {
                Ok(ColumnarValue::Scalar(ScalarValue::Binary(Some(encoded))))
            } else if fail_on_error {
                exec_err!("Input to unhex is not a valid hex string: {string}")
            } else {
                Ok(ColumnarValue::Scalar(ScalarValue::Binary(None)))
            }
        }
        ColumnarValue::Scalar(ScalarValue::Utf8(None)) => {
            Ok(ColumnarValue::Scalar(ScalarValue::Binary(None)))
        }
        _ => {
            exec_err!(
                "The first argument must be a string scalar or array, but got: {:?}",
                array
            )
        }
    }
}

/// Spark-compatible `unhex` expression
pub fn spark_unhex(args: &[ColumnarValue]) -> Result<ColumnarValue, DataFusionError> {
    if args.len() > 2 {
        return exec_err!("unhex takes at most 2 arguments, but got: {}", args.len());
    }

    let val_to_unhex = &args[0];
    let fail_on_error = if args.len() == 2 {
        match &args[1] {
            ColumnarValue::Scalar(ScalarValue::Boolean(Some(fail_on_error))) => *fail_on_error,
            _ => {
                return exec_err!(
                    "The second argument must be boolean scalar, but got: {:?}",
                    args[1]
                );
            }
        }
    } else {
        false
    };

    match val_to_unhex.data_type() {
        DataType::Utf8 => spark_unhex_inner::<i32>(val_to_unhex, fail_on_error),
        DataType::LargeUtf8 => spark_unhex_inner::<i64>(val_to_unhex, fail_on_error),
        other => exec_err!(
            "The first argument must be a Utf8 or LargeUtf8: {:?}",
            other
        ),
    }
}

#[cfg(test)]
mod test {
    use std::sync::Arc;

    use arrow::array::{BinaryBuilder, StringBuilder};
    use arrow_array::make_array;
    use arrow_data::ArrayData;
    use datafusion::logical_expr::ColumnarValue;
    use datafusion_common::ScalarValue;

    use super::unhex;

    #[test]
    fn test_spark_unhex_null() -> Result<(), Box<dyn std::error::Error>> {
        let input = ArrayData::new_null(&arrow_schema::DataType::Utf8, 2);
        let output = ArrayData::new_null(&arrow_schema::DataType::Binary, 2);

        let input = ColumnarValue::Array(Arc::new(make_array(input)));
        let expected = ColumnarValue::Array(Arc::new(make_array(output)));

        let result = super::spark_unhex(&[input])?;

        match (result, expected) {
            (ColumnarValue::Array(result), ColumnarValue::Array(expected)) => {
                assert_eq!(*result, *expected);
                Ok(())
            }
            _ => Err("Unexpected result type".into()),
        }
    }

    #[test]
    fn test_partial_error() -> Result<(), Box<dyn std::error::Error>> {
        let mut input = StringBuilder::new();

        input.append_value("1CGG"); // 1C is ok, but GG is invalid
        input.append_value("537061726B2053514C"); // followed by valid

        let input = ColumnarValue::Array(Arc::new(input.finish()));
        let fail_on_error = ColumnarValue::Scalar(ScalarValue::Boolean(Some(false)));

        let result = super::spark_unhex(&[input, fail_on_error])?;

        let mut expected = BinaryBuilder::new();
        expected.append_null();
        expected.append_value("Spark SQL".as_bytes());

        match (result, ColumnarValue::Array(Arc::new(expected.finish()))) {
            (ColumnarValue::Array(result), ColumnarValue::Array(expected)) => {
                assert_eq!(*result, *expected);

                Ok(())
            }
            _ => Err("Unexpected result type".into()),
        }
    }

    #[test]
    fn test_unhex_valid() -> Result<(), Box<dyn std::error::Error>> {
        let mut result = Vec::new();

        unhex("537061726B2053514C", &mut result)?;
        let result_str = std::str::from_utf8(&result)?;
        assert_eq!(result_str, "Spark SQL");
        result.clear();

        unhex("1C", &mut result)?;
        assert_eq!(result, vec![28]);
        result.clear();

        unhex("737472696E67", &mut result)?;
        assert_eq!(result, "string".as_bytes());
        result.clear();

        unhex("1", &mut result)?;
        assert_eq!(result, vec![1]);
        result.clear();

        Ok(())
    }

    #[test]
    fn test_odd_length() -> Result<(), Box<dyn std::error::Error>> {
        let mut result = Vec::new();

        unhex("A1B", &mut result)?;
        assert_eq!(result, vec![10, 27]);
        result.clear();

        unhex("0A1B", &mut result)?;
        assert_eq!(result, vec![10, 27]);
        result.clear();

        Ok(())
    }

    #[test]
    fn test_unhex_empty() {
        let mut result = Vec::new();

        // Empty hex string
        unhex("", &mut result).unwrap();
        assert!(result.is_empty());
    }

    #[test]
    fn test_unhex_invalid() {
        let mut result = Vec::new();

        // Invalid hex strings
        assert!(unhex("##", &mut result).is_err());
        assert!(unhex("G123", &mut result).is_err());
        assert!(unhex("hello", &mut result).is_err());
        assert!(unhex("\0", &mut result).is_err());
    }
}
