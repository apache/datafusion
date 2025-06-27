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

use std::{any::Any, sync::Arc};

use arrow::array::{Array, AsArray, BooleanArray};
use arrow::datatypes::DataType;
use arrow::datatypes::DataType::Boolean;
use datafusion_common::types::logical_string;
use datafusion_common::utils::take_function_args;
use datafusion_common::{exec_err, Result, ScalarValue};
use datafusion_expr::{
    Coercion, ColumnarValue, ScalarFunctionArgs, ScalarUDFImpl, Signature,
    TypeSignatureClass, Volatility,
};

/// Spark-compatible `luhn_check` expression
/// <https://spark.apache.org/docs/latest/api/sql/index.html#luhn_check>
#[derive(Debug)]
pub struct SparkLuhnCheck {
    signature: Signature,
}

impl Default for SparkLuhnCheck {
    fn default() -> Self {
        Self::new()
    }
}

impl SparkLuhnCheck {
    pub fn new() -> Self {
        Self {
            signature: Signature::coercible(
                vec![Coercion::new_exact(TypeSignatureClass::Native(
                    logical_string(),
                ))],
                Volatility::Immutable,
            ),
        }
    }
}

impl ScalarUDFImpl for SparkLuhnCheck {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "luhn_check"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(Boolean)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let [array] = take_function_args(self.name(), &args.args)?;

        match array {
            ColumnarValue::Array(array) => match array.data_type() {
                DataType::Utf8View => {
                    let str_array = array.as_string_view();
                    let values = str_array
                        .iter()
                        .map(|s| s.map(luhn_check_impl))
                        .collect::<BooleanArray>();
                    Ok(ColumnarValue::Array(Arc::new(values)))
                }
                DataType::Utf8 => {
                    let str_array = array.as_string::<i32>();
                    let values = str_array
                        .iter()
                        .map(|s| s.map(luhn_check_impl))
                        .collect::<BooleanArray>();
                    Ok(ColumnarValue::Array(Arc::new(values)))
                }
                DataType::LargeUtf8 => {
                    let str_array = array.as_string::<i64>();
                    let values = str_array
                        .iter()
                        .map(|s| s.map(luhn_check_impl))
                        .collect::<BooleanArray>();
                    Ok(ColumnarValue::Array(Arc::new(values)))
                }
                other => {
                    exec_err!("Unsupported data type {other:?} for function `luhn_check`")
                }
            },
            ColumnarValue::Scalar(ScalarValue::Utf8(Some(s)))
            | ColumnarValue::Scalar(ScalarValue::LargeUtf8(Some(s)))
            | ColumnarValue::Scalar(ScalarValue::Utf8View(Some(s))) => Ok(
                ColumnarValue::Scalar(ScalarValue::Boolean(Some(luhn_check_impl(&s)))),
            ),
            ColumnarValue::Scalar(ScalarValue::Utf8(None))
            | ColumnarValue::Scalar(ScalarValue::LargeUtf8(None))
            | ColumnarValue::Scalar(ScalarValue::Utf8View(None)) => {
                Ok(ColumnarValue::Scalar(ScalarValue::Boolean(None)))
            }
            other => {
                exec_err!("Unsupported data type {other:?} for function `luhn_check`")
            }
        }
    }
}

/// Validates a string using the Luhn algorithm.
/// Returns `true` if the input is a valid Luhn number.
fn luhn_check_impl(input: &str) -> bool {
    let mut sum = 0u32;
    let mut alt = false;
    let mut digits_processed = 0;

    for b in input.as_bytes().iter().rev() {
        let digit = match b {
            b'0'..=b'9' => {
                digits_processed += 1;
                b - b'0'
            }
            _ => return false,
        };

        let mut val = digit as u32;
        if alt {
            val = val * 2;
            if val > 9 {
                val -= 9;
            }
        }
        sum += val;
        alt = !alt;
    }

    digits_processed > 0 && sum % 10 == 0
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{ArrayRef, BooleanArray, StringArray, StringViewArray};
    use arrow::datatypes::{DataType::Utf8, Field};
    use datafusion_common::Result;
    use datafusion_expr::ColumnarValue;
    use std::sync::Arc;

    fn check_array(input: ArrayRef, expected: ArrayRef) -> Result<()> {
        let func = SparkLuhnCheck::new();
        let args = ScalarFunctionArgs {
            number_rows: input.len(),
            args: vec![ColumnarValue::Array(input.clone())],
            arg_fields: vec![Arc::new(Field::new("a", input.data_type().clone(), true))],
            return_field: Arc::new(Field::new("f", Utf8, true)),
        };

        match func.invoke_with_args(args)? {
            ColumnarValue::Array(result) => assert_eq!(&result, &expected),
            _ => panic!("Expected array output"),
        }

        Ok(())
    }

    fn check_scalar(input: ScalarValue, expected: ScalarValue) -> Result<()> {
        let func = SparkLuhnCheck::new();
        let args = ScalarFunctionArgs {
            number_rows: 1,
            args: vec![ColumnarValue::Scalar(input)],
            arg_fields: vec![Arc::new(Field::new("f", Utf8, true))],
            return_field: Arc::new(Field::new("f", Utf8, true)),
        };

        match func.invoke_with_args(args)? {
            ColumnarValue::Scalar(actual) => assert_eq!(actual, expected),
            _ => panic!("Expected scalar output"),
        }

        Ok(())
    }

    #[test]
    fn test_luhn_check_basic() {
        assert!(luhn_check_impl("79927398713"));
        assert!(!luhn_check_impl("79927398714"));
        assert!(!luhn_check_impl(" 7992 7398 713 "));
        assert!(!luhn_check_impl("abc123"));
        assert!(!luhn_check_impl(""));
    }

    #[test]
    fn test_array_utf8() -> Result<()> {
        let input = Arc::new(StringArray::from(vec![
            Some("79927398713"),
            Some("4417123456789113"),
            Some("7992 7398 714"),
            Some("79927398714"),
            Some(""),
            Some("abc123"),
            None,
        ])) as ArrayRef;

        let expected = Arc::new(BooleanArray::from(vec![
            Some(true),
            Some(true),
            Some(false),
            Some(false),
            Some(false),
            Some(false),
            None,
        ])) as ArrayRef;

        check_array(input, expected)
    }

    #[test]
    fn test_array_utf8view() -> Result<()> {
        let input = Arc::new(StringViewArray::from(vec![
            Some("79927398713"),
            Some("4417123456789113"),
            Some("7992 7398 714"),
            Some("79927398714"),
            Some(""),
            Some("abc123"),
            None,
        ])) as ArrayRef;

        let expected = Arc::new(BooleanArray::from(vec![
            Some(true),
            Some(true),
            Some(false),
            Some(false),
            Some(false),
            Some(false),
            None,
        ])) as ArrayRef;

        check_array(input, expected)
    }

    #[test]
    fn test_scalar_utf8() -> Result<()> {
        let cases = vec![
            (Some("79927398713"), Some(true)),
            (Some("79927398714"), Some(false)),
            (Some("abc123"), Some(false)),
            (Some(" 7992 7398 713 "), Some(false)),
            (None::<&str>, None),
        ];

        for (input, expected) in cases {
            let input_scalar = ScalarValue::Utf8(input.map(str::to_string));
            let expected_scalar = ScalarValue::Boolean(expected);
            check_scalar(input_scalar, expected_scalar)?;
        }

        Ok(())
    }
}
