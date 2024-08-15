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
use std::sync::Arc;

use arrow::array::{ArrayRef, GenericStringArray, OffsetSizeTrait};
use arrow::datatypes::DataType;

use datafusion_common::cast::{
    as_generic_string_array, as_int64_array, as_string_view_array,
};
use datafusion_common::{exec_err, Result};
use datafusion_expr::TypeSignature::*;
use datafusion_expr::{ColumnarValue, Volatility};
use datafusion_expr::{ScalarUDFImpl, Signature};

use crate::utils::{make_scalar_function, utf8_to_str_type};

#[derive(Debug)]
pub struct SplitPartFunc {
    signature: Signature,
}

impl Default for SplitPartFunc {
    fn default() -> Self {
        Self::new()
    }
}

impl SplitPartFunc {
    pub fn new() -> Self {
        use DataType::*;
        Self {
            signature: Signature::one_of(
                vec![
                    Exact(vec![Utf8View, Utf8View, Int64]),
                    Exact(vec![Utf8View, Utf8, Int64]),
                    Exact(vec![Utf8View, LargeUtf8, Int64]),
                    Exact(vec![Utf8, Utf8View, Int64]),
                    Exact(vec![Utf8, Utf8, Int64]),
                    Exact(vec![LargeUtf8, Utf8View, Int64]),
                    Exact(vec![LargeUtf8, Utf8, Int64]),
                    Exact(vec![Utf8, LargeUtf8, Int64]),
                    Exact(vec![LargeUtf8, LargeUtf8, Int64]),
                ],
                Volatility::Immutable,
            ),
        }
    }
}

impl ScalarUDFImpl for SplitPartFunc {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "split_part"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        utf8_to_str_type(&arg_types[0], "split_part")
    }

    fn invoke(&self, args: &[ColumnarValue]) -> Result<ColumnarValue> {
        match (args[0].data_type(), args[1].data_type()) {
            (
                DataType::Utf8 | DataType::Utf8View,
                DataType::Utf8 | DataType::Utf8View,
            ) => make_scalar_function(split_part::<i32, i32>, vec![])(args),
            (DataType::LargeUtf8, DataType::LargeUtf8) => {
                make_scalar_function(split_part::<i64, i64>, vec![])(args)
            }
            (_, DataType::LargeUtf8) => {
                make_scalar_function(split_part::<i32, i64>, vec![])(args)
            }
            (DataType::LargeUtf8, _) => {
                make_scalar_function(split_part::<i64, i32>, vec![])(args)
            }
            (first_type, second_type) => exec_err!(
                "unsupported first type {} and second type {} for split_part function",
                first_type,
                second_type
            ),
        }
    }
}

macro_rules! process_split_part {
    ($string_array: expr, $delimiter_array: expr, $n_array: expr) => {{
        let result = $string_array
            .iter()
            .zip($delimiter_array.iter())
            .zip($n_array.iter())
            .map(|((string, delimiter), n)| match (string, delimiter, n) {
                (Some(string), Some(delimiter), Some(n)) => {
                    let split_string: Vec<&str> = string.split(delimiter).collect();
                    let len = split_string.len();

                    let index = match n.cmp(&0) {
                        std::cmp::Ordering::Less => len as i64 + n,
                        std::cmp::Ordering::Equal => {
                            return exec_err!("field position must not be zero");
                        }
                        std::cmp::Ordering::Greater => n - 1,
                    } as usize;

                    if index < len {
                        Ok(Some(split_string[index]))
                    } else {
                        Ok(Some(""))
                    }
                }
                _ => Ok(None),
            })
            .collect::<Result<GenericStringArray<StringLen>>>()?;
        Ok(Arc::new(result) as ArrayRef)
    }};
}

/// Splits string at occurrences of delimiter and returns the n'th field (counting from one).
/// split_part('abc~@~def~@~ghi', '~@~', 2) = 'def'
fn split_part<StringLen: OffsetSizeTrait, DelimiterLen: OffsetSizeTrait>(
    args: &[ArrayRef],
) -> Result<ArrayRef> {
    let n_array = as_int64_array(&args[2])?;
    match (args[0].data_type(), args[1].data_type()) {
        (DataType::Utf8View, _) => {
            let string_array = as_string_view_array(&args[0])?;
            match args[1].data_type() {
                DataType::Utf8View => {
                    let delimiter_array = as_string_view_array(&args[1])?;
                    process_split_part!(string_array, delimiter_array, n_array)
                }
                _ => {
                    let delimiter_array =
                        as_generic_string_array::<DelimiterLen>(&args[1])?;
                    process_split_part!(string_array, delimiter_array, n_array)
                }
            }
        }
        (_, DataType::Utf8View) => {
            let delimiter_array = as_string_view_array(&args[1])?;
            match args[0].data_type() {
                DataType::Utf8View => {
                    let string_array = as_string_view_array(&args[0])?;
                    process_split_part!(string_array, delimiter_array, n_array)
                }
                _ => {
                    let string_array = as_generic_string_array::<StringLen>(&args[0])?;
                    process_split_part!(string_array, delimiter_array, n_array)
                }
            }
        }
        (_, _) => {
            let string_array = as_generic_string_array::<StringLen>(&args[0])?;
            let delimiter_array = as_generic_string_array::<DelimiterLen>(&args[1])?;
            process_split_part!(string_array, delimiter_array, n_array)
        }
    }
}

#[cfg(test)]
mod tests {
    use arrow::array::{Array, StringArray};
    use arrow::datatypes::DataType::Utf8;

    use datafusion_common::ScalarValue;
    use datafusion_common::{exec_err, Result};
    use datafusion_expr::{ColumnarValue, ScalarUDFImpl};

    use crate::string::split_part::SplitPartFunc;
    use crate::utils::test::test_function;

    #[test]
    fn test_functions() -> Result<()> {
        test_function!(
            SplitPartFunc::new(),
            &[
                ColumnarValue::Scalar(ScalarValue::Utf8(Some(String::from(
                    "abc~@~def~@~ghi"
                )))),
                ColumnarValue::Scalar(ScalarValue::Utf8(Some(String::from("~@~")))),
                ColumnarValue::Scalar(ScalarValue::Int64(Some(2))),
            ],
            Ok(Some("def")),
            &str,
            Utf8,
            StringArray
        );
        test_function!(
            SplitPartFunc::new(),
            &[
                ColumnarValue::Scalar(ScalarValue::Utf8(Some(String::from(
                    "abc~@~def~@~ghi"
                )))),
                ColumnarValue::Scalar(ScalarValue::Utf8(Some(String::from("~@~")))),
                ColumnarValue::Scalar(ScalarValue::Int64(Some(20))),
            ],
            Ok(Some("")),
            &str,
            Utf8,
            StringArray
        );
        test_function!(
            SplitPartFunc::new(),
            &[
                ColumnarValue::Scalar(ScalarValue::Utf8(Some(String::from(
                    "abc~@~def~@~ghi"
                )))),
                ColumnarValue::Scalar(ScalarValue::Utf8(Some(String::from("~@~")))),
                ColumnarValue::Scalar(ScalarValue::Int64(Some(-1))),
            ],
            Ok(Some("ghi")),
            &str,
            Utf8,
            StringArray
        );
        test_function!(
            SplitPartFunc::new(),
            &[
                ColumnarValue::Scalar(ScalarValue::Utf8(Some(String::from(
                    "abc~@~def~@~ghi"
                )))),
                ColumnarValue::Scalar(ScalarValue::Utf8(Some(String::from("~@~")))),
                ColumnarValue::Scalar(ScalarValue::Int64(Some(0))),
            ],
            exec_err!("field position must not be zero"),
            &str,
            Utf8,
            StringArray
        );

        Ok(())
    }
}
