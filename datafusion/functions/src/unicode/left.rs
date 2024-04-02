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
use std::cmp::Ordering;
use std::sync::Arc;

use arrow::array::{ArrayRef, GenericStringArray, OffsetSizeTrait};
use arrow::datatypes::DataType;

use datafusion_common::cast::{as_generic_string_array, as_int64_array};
use datafusion_common::exec_err;
use datafusion_common::Result;
use datafusion_expr::TypeSignature::Exact;
use datafusion_expr::{ColumnarValue, ScalarUDFImpl, Signature, Volatility};

use crate::utils::{make_scalar_function, utf8_to_str_type};

#[derive(Debug)]
pub struct LeftFunc {
    signature: Signature,
}

impl LeftFunc {
    pub fn new() -> Self {
        use DataType::*;
        Self {
            signature: Signature::one_of(
                vec![Exact(vec![Utf8, Int64]), Exact(vec![LargeUtf8, Int64])],
                Volatility::Immutable,
            ),
        }
    }
}

impl ScalarUDFImpl for LeftFunc {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "left"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        utf8_to_str_type(&arg_types[0], "left")
    }

    fn invoke(&self, args: &[ColumnarValue]) -> Result<ColumnarValue> {
        match args[0].data_type() {
            DataType::Utf8 => make_scalar_function(left::<i32>, vec![])(args),
            DataType::LargeUtf8 => make_scalar_function(left::<i64>, vec![])(args),
            other => exec_err!("Unsupported data type {other:?} for function left"),
        }
    }
}

/// Returns first n characters in the string, or when n is negative, returns all but last |n| characters.
/// left('abcde', 2) = 'ab'
/// The implementation uses UTF-8 code points as characters
pub fn left<T: OffsetSizeTrait>(args: &[ArrayRef]) -> Result<ArrayRef> {
    let string_array = as_generic_string_array::<T>(&args[0])?;
    let n_array = as_int64_array(&args[1])?;
    let result = string_array
        .iter()
        .zip(n_array.iter())
        .map(|(string, n)| match (string, n) {
            (Some(string), Some(n)) => match n.cmp(&0) {
                Ordering::Less => {
                    let len = string.chars().count() as i64;
                    Some(if n.abs() < len {
                        string.chars().take((len + n) as usize).collect::<String>()
                    } else {
                        "".to_string()
                    })
                }
                Ordering::Equal => Some("".to_string()),
                Ordering::Greater => {
                    Some(string.chars().take(n as usize).collect::<String>())
                }
            },
            _ => None,
        })
        .collect::<GenericStringArray<T>>();

    Ok(Arc::new(result) as ArrayRef)
}

#[cfg(test)]
mod tests {
    use arrow::array::{Array, StringArray};
    use arrow::datatypes::DataType::Utf8;

    use datafusion_common::{Result, ScalarValue};
    use datafusion_expr::{ColumnarValue, ScalarUDFImpl};

    use crate::unicode::left::LeftFunc;
    use crate::utils::test::test_function;

    #[test]
    fn test_functions() -> Result<()> {
        test_function!(
            LeftFunc::new(),
            &[
                ColumnarValue::Scalar(ScalarValue::from("abcde")),
                ColumnarValue::Scalar(ScalarValue::from(2i64)),
            ],
            Ok(Some("ab")),
            &str,
            Utf8,
            StringArray
        );
        test_function!(
            LeftFunc::new(),
            &[
                ColumnarValue::Scalar(ScalarValue::from("abcde")),
                ColumnarValue::Scalar(ScalarValue::from(200i64)),
            ],
            Ok(Some("abcde")),
            &str,
            Utf8,
            StringArray
        );
        test_function!(
            LeftFunc::new(),
            &[
                ColumnarValue::Scalar(ScalarValue::from("abcde")),
                ColumnarValue::Scalar(ScalarValue::from(-2i64)),
            ],
            Ok(Some("abc")),
            &str,
            Utf8,
            StringArray
        );
        test_function!(
            LeftFunc::new(),
            &[
                ColumnarValue::Scalar(ScalarValue::from("abcde")),
                ColumnarValue::Scalar(ScalarValue::from(-200i64)),
            ],
            Ok(Some("")),
            &str,
            Utf8,
            StringArray
        );
        test_function!(
            LeftFunc::new(),
            &[
                ColumnarValue::Scalar(ScalarValue::from("abcde")),
                ColumnarValue::Scalar(ScalarValue::from(0i64)),
            ],
            Ok(Some("")),
            &str,
            Utf8,
            StringArray
        );
        test_function!(
            LeftFunc::new(),
            &[
                ColumnarValue::Scalar(ScalarValue::Utf8(None)),
                ColumnarValue::Scalar(ScalarValue::from(2i64)),
            ],
            Ok(None),
            &str,
            Utf8,
            StringArray
        );
        test_function!(
            LeftFunc::new(),
            &[
                ColumnarValue::Scalar(ScalarValue::from("abcde")),
                ColumnarValue::Scalar(ScalarValue::Int64(None)),
            ],
            Ok(None),
            &str,
            Utf8,
            StringArray
        );
        test_function!(
            LeftFunc::new(),
            &[
                ColumnarValue::Scalar(ScalarValue::from("joséésoj")),
                ColumnarValue::Scalar(ScalarValue::from(5i64)),
            ],
            Ok(Some("joséé")),
            &str,
            Utf8,
            StringArray
        );
        test_function!(
            LeftFunc::new(),
            &[
                ColumnarValue::Scalar(ScalarValue::from("joséésoj")),
                ColumnarValue::Scalar(ScalarValue::from(-3i64)),
            ],
            Ok(Some("joséé")),
            &str,
            Utf8,
            StringArray
        );
        #[cfg(not(feature = "unicode_expressions"))]
        test_function!(
            LeftFunc::new(),
            &[
                ColumnarValue::Scalar(ScalarValue::from("abcde")),
                ColumnarValue::Scalar(ScalarValue::from(2i64)),
            ],
            internal_err!(
                "function left requires compilation with feature flag: unicode_expressions."
            ),
            &str,
            Utf8,
            StringArray
        );

        Ok(())
    }
}
