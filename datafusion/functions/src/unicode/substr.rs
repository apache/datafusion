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

use std::any::Any;
use std::cmp::max;
use std::sync::Arc;

use arrow::array::{ArrayRef, GenericStringArray, OffsetSizeTrait};
use arrow::datatypes::DataType;

use datafusion_common::cast::{as_generic_string_array, as_int64_array};
use datafusion_common::{exec_err, Result};
use datafusion_expr::TypeSignature::Exact;
use datafusion_expr::{ColumnarValue, ScalarUDFImpl, Signature, Volatility};

use crate::utils::{make_scalar_function, utf8_to_str_type};

#[derive(Debug)]
pub struct SubstrFunc {
    signature: Signature,
}

impl SubstrFunc {
    pub fn new() -> Self {
        use DataType::*;
        Self {
            signature: Signature::one_of(
                vec![
                    Exact(vec![Utf8, Int64]),
                    Exact(vec![LargeUtf8, Int64]),
                    Exact(vec![Utf8, Int64, Int64]),
                    Exact(vec![LargeUtf8, Int64, Int64]),
                ],
                Volatility::Immutable,
            ),
        }
    }
}

impl ScalarUDFImpl for SubstrFunc {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "substr"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        utf8_to_str_type(&arg_types[0], "substr")
    }

    fn invoke(&self, args: &[ColumnarValue]) -> Result<ColumnarValue> {
        match args[0].data_type() {
            DataType::Utf8 => make_scalar_function(substr::<i32>, vec![])(args),
            DataType::LargeUtf8 => make_scalar_function(substr::<i64>, vec![])(args),
            other => exec_err!("Unsupported data type {other:?} for function substr"),
        }
    }
}

/// Extracts the substring of string starting at the start'th character, and extending for count characters if that is specified. (Same as substring(string from start for count).)
/// substr('alphabet', 3) = 'phabet'
/// substr('alphabet', 3, 2) = 'ph'
/// The implementation uses UTF-8 code points as characters
pub fn substr<T: OffsetSizeTrait>(args: &[ArrayRef]) -> Result<ArrayRef> {
    match args.len() {
        2 => {
            let string_array = as_generic_string_array::<T>(&args[0])?;
            let start_array = as_int64_array(&args[1])?;

            let result = string_array
                .iter()
                .zip(start_array.iter())
                .map(|(string, start)| match (string, start) {
                    (Some(string), Some(start)) => {
                        if start <= 0 {
                            Some(string.to_string())
                        } else {
                            Some(string.chars().skip(start as usize - 1).collect())
                        }
                    }
                    _ => None,
                })
                .collect::<GenericStringArray<T>>();

            Ok(Arc::new(result) as ArrayRef)
        }
        3 => {
            let string_array = as_generic_string_array::<T>(&args[0])?;
            let start_array = as_int64_array(&args[1])?;
            let count_array = as_int64_array(&args[2])?;

            let result = string_array
                .iter()
                .zip(start_array.iter())
                .zip(count_array.iter())
                .map(|((string, start), count)| match (string, start, count) {
                    (Some(string), Some(start), Some(count)) => {
                        if count < 0 {
                            exec_err!(
                                "negative substring length not allowed: substr(<str>, {start}, {count})"
                            )
                        } else {
                            let skip = max(0, start - 1);
                            let count = max(0, count + (if start < 1 {start - 1} else {0}));
                            Ok(Some(string.chars().skip(skip as usize).take(count as usize).collect::<String>()))
                        }
                    }
                    _ => Ok(None),
                })
                .collect::<Result<GenericStringArray<T>>>()?;

            Ok(Arc::new(result) as ArrayRef)
        }
        other => {
            exec_err!("substr was called with {other} arguments. It requires 2 or 3.")
        }
    }
}

#[cfg(test)]
mod tests {
    use arrow::array::{Array, StringArray};
    use arrow::datatypes::DataType::Utf8;

    use datafusion_common::{exec_err, Result, ScalarValue};
    use datafusion_expr::{ColumnarValue, ScalarUDFImpl};

    use crate::unicode::substr::SubstrFunc;
    use crate::utils::test::test_function;

    #[test]
    fn test_functions() -> Result<()> {
        test_function!(
            SubstrFunc::new(),
            &[
                ColumnarValue::Scalar(ScalarValue::from("alphabet")),
                ColumnarValue::Scalar(ScalarValue::from(0i64)),
            ],
            Ok(Some("alphabet")),
            &str,
            Utf8,
            StringArray
        );
        test_function!(
            SubstrFunc::new(),
            &[
                ColumnarValue::Scalar(ScalarValue::from("joséésoj")),
                ColumnarValue::Scalar(ScalarValue::from(5i64)),
            ],
            Ok(Some("ésoj")),
            &str,
            Utf8,
            StringArray
        );
        test_function!(
            SubstrFunc::new(),
            &[
                ColumnarValue::Scalar(ScalarValue::from("joséésoj")),
                ColumnarValue::Scalar(ScalarValue::from(-5i64)),
            ],
            Ok(Some("joséésoj")),
            &str,
            Utf8,
            StringArray
        );
        test_function!(
            SubstrFunc::new(),
            &[
                ColumnarValue::Scalar(ScalarValue::from("alphabet")),
                ColumnarValue::Scalar(ScalarValue::from(1i64)),
            ],
            Ok(Some("alphabet")),
            &str,
            Utf8,
            StringArray
        );
        test_function!(
            SubstrFunc::new(),
            &[
                ColumnarValue::Scalar(ScalarValue::from("alphabet")),
                ColumnarValue::Scalar(ScalarValue::from(2i64)),
            ],
            Ok(Some("lphabet")),
            &str,
            Utf8,
            StringArray
        );
        test_function!(
            SubstrFunc::new(),
            &[
                ColumnarValue::Scalar(ScalarValue::from("alphabet")),
                ColumnarValue::Scalar(ScalarValue::from(3i64)),
            ],
            Ok(Some("phabet")),
            &str,
            Utf8,
            StringArray
        );
        test_function!(
            SubstrFunc::new(),
            &[
                ColumnarValue::Scalar(ScalarValue::from("alphabet")),
                ColumnarValue::Scalar(ScalarValue::from(-3i64)),
            ],
            Ok(Some("alphabet")),
            &str,
            Utf8,
            StringArray
        );
        test_function!(
            SubstrFunc::new(),
            &[
                ColumnarValue::Scalar(ScalarValue::from("alphabet")),
                ColumnarValue::Scalar(ScalarValue::from(30i64)),
            ],
            Ok(Some("")),
            &str,
            Utf8,
            StringArray
        );
        test_function!(
            SubstrFunc::new(),
            &[
                ColumnarValue::Scalar(ScalarValue::from("alphabet")),
                ColumnarValue::Scalar(ScalarValue::Int64(None)),
            ],
            Ok(None),
            &str,
            Utf8,
            StringArray
        );
        test_function!(
            SubstrFunc::new(),
            &[
                ColumnarValue::Scalar(ScalarValue::from("alphabet")),
                ColumnarValue::Scalar(ScalarValue::from(3i64)),
                ColumnarValue::Scalar(ScalarValue::from(2i64)),
            ],
            Ok(Some("ph")),
            &str,
            Utf8,
            StringArray
        );
        test_function!(
            SubstrFunc::new(),
            &[
                ColumnarValue::Scalar(ScalarValue::from("alphabet")),
                ColumnarValue::Scalar(ScalarValue::from(3i64)),
                ColumnarValue::Scalar(ScalarValue::from(20i64)),
            ],
            Ok(Some("phabet")),
            &str,
            Utf8,
            StringArray
        );
        test_function!(
            SubstrFunc::new(),
            &[
                ColumnarValue::Scalar(ScalarValue::from("alphabet")),
                ColumnarValue::Scalar(ScalarValue::from(0i64)),
                ColumnarValue::Scalar(ScalarValue::from(5i64)),
            ],
            Ok(Some("alph")),
            &str,
            Utf8,
            StringArray
        );
        // starting from 5 (10 + -5)
        test_function!(
            SubstrFunc::new(),
            &[
                ColumnarValue::Scalar(ScalarValue::from("alphabet")),
                ColumnarValue::Scalar(ScalarValue::from(-5i64)),
                ColumnarValue::Scalar(ScalarValue::from(10i64)),
            ],
            Ok(Some("alph")),
            &str,
            Utf8,
            StringArray
        );
        // starting from -1 (4 + -5)
        test_function!(
            SubstrFunc::new(),
            &[
                ColumnarValue::Scalar(ScalarValue::from("alphabet")),
                ColumnarValue::Scalar(ScalarValue::from(-5i64)),
                ColumnarValue::Scalar(ScalarValue::from(4i64)),
            ],
            Ok(Some("")),
            &str,
            Utf8,
            StringArray
        );
        // starting from 0 (5 + -5)
        test_function!(
            SubstrFunc::new(),
            &[
                ColumnarValue::Scalar(ScalarValue::from("alphabet")),
                ColumnarValue::Scalar(ScalarValue::from(-5i64)),
                ColumnarValue::Scalar(ScalarValue::from(5i64)),
            ],
            Ok(Some("")),
            &str,
            Utf8,
            StringArray
        );
        test_function!(
            SubstrFunc::new(),
            &[
                ColumnarValue::Scalar(ScalarValue::from("alphabet")),
                ColumnarValue::Scalar(ScalarValue::Int64(None)),
                ColumnarValue::Scalar(ScalarValue::from(20i64)),
            ],
            Ok(None),
            &str,
            Utf8,
            StringArray
        );
        test_function!(
            SubstrFunc::new(),
            &[
                ColumnarValue::Scalar(ScalarValue::from("alphabet")),
                ColumnarValue::Scalar(ScalarValue::from(3i64)),
                ColumnarValue::Scalar(ScalarValue::Int64(None)),
            ],
            Ok(None),
            &str,
            Utf8,
            StringArray
        );
        test_function!(
            SubstrFunc::new(),
            &[
                ColumnarValue::Scalar(ScalarValue::from("alphabet")),
                ColumnarValue::Scalar(ScalarValue::from(1i64)),
                ColumnarValue::Scalar(ScalarValue::from(-1i64)),
            ],
            exec_err!("negative substring length not allowed: substr(<str>, 1, -1)"),
            &str,
            Utf8,
            StringArray
        );
        test_function!(
            SubstrFunc::new(),
            &[
                ColumnarValue::Scalar(ScalarValue::from("joséésoj")),
                ColumnarValue::Scalar(ScalarValue::from(5i64)),
                ColumnarValue::Scalar(ScalarValue::from(2i64)),
            ],
            Ok(Some("és")),
            &str,
            Utf8,
            StringArray
        );
        #[cfg(not(feature = "unicode_expressions"))]
        test_function!(
            SubstrFunc::new(),
            &[
                ColumnarValue::Scalar(ScalarValue::from("alphabet")),
                ColumnarValue::Scalar(ScalarValue::from(0i64)),
            ],
            internal_err!(
                "function substr requires compilation with feature flag: unicode_expressions."
            ),
            &str,
            Utf8,
            StringArray
        );

        Ok(())
    }
}
