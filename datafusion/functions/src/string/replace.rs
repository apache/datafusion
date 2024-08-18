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

use arrow::array::{ArrayRef, GenericStringArray, OffsetSizeTrait, StringArray};
use arrow::datatypes::DataType;

use datafusion_common::cast::{as_generic_string_array, as_string_view_array};
use datafusion_common::{exec_err, Result};
use datafusion_expr::TypeSignature::*;
use datafusion_expr::{ColumnarValue, Volatility};
use datafusion_expr::{ScalarUDFImpl, Signature};

use crate::utils::{make_scalar_function, utf8_to_str_type};

#[derive(Debug)]
pub struct ReplaceFunc {
    signature: Signature,
}

impl Default for ReplaceFunc {
    fn default() -> Self {
        Self::new()
    }
}

impl ReplaceFunc {
    pub fn new() -> Self {
        use DataType::*;
        Self {
            signature: Signature::one_of(
                vec![
                    Exact(vec![Utf8View, Utf8View, Utf8View]),
                    Exact(vec![Utf8, Utf8, Utf8]),
                    Exact(vec![LargeUtf8, LargeUtf8, LargeUtf8]),
                ],
                Volatility::Immutable,
            ),
        }
    }
}

impl ScalarUDFImpl for ReplaceFunc {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "replace"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        utf8_to_str_type(&arg_types[0], "replace")
    }

    fn invoke(&self, args: &[ColumnarValue]) -> Result<ColumnarValue> {
        match args[0].data_type() {
            DataType::Utf8 => make_scalar_function(replace::<i32>, vec![])(args),
            DataType::LargeUtf8 => make_scalar_function(replace::<i64>, vec![])(args),
            DataType::Utf8View => make_scalar_function(replace_view, vec![])(args),
            other => {
                exec_err!("Unsupported data type {other:?} for function replace")
            }
        }
    }
}

fn replace_view(args: &[ArrayRef]) -> Result<ArrayRef> {
    let string_array = as_string_view_array(&args[0])?;
    let from_array = as_string_view_array(&args[1])?;
    let to_array = as_string_view_array(&args[2])?;

    let result = string_array
        .iter()
        .zip(from_array.iter())
        .zip(to_array.iter())
        .map(|((string, from), to)| match (string, from, to) {
            (Some(string), Some(from), Some(to)) => Some(string.replace(from, to)),
            _ => None,
        })
        .collect::<StringArray>();

    Ok(Arc::new(result) as ArrayRef)
}
/// Replaces all occurrences in string of substring from with substring to.
/// replace('abcdefabcdef', 'cd', 'XX') = 'abXXefabXXef'
fn replace<T: OffsetSizeTrait>(args: &[ArrayRef]) -> Result<ArrayRef> {
    let string_array = as_generic_string_array::<T>(&args[0])?;
    let from_array = as_generic_string_array::<T>(&args[1])?;
    let to_array = as_generic_string_array::<T>(&args[2])?;

    let result = string_array
        .iter()
        .zip(from_array.iter())
        .zip(to_array.iter())
        .map(|((string, from), to)| match (string, from, to) {
            (Some(string), Some(from), Some(to)) => Some(string.replace(from, to)),
            _ => None,
        })
        .collect::<GenericStringArray<T>>();

    Ok(Arc::new(result) as ArrayRef)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::utils::test::test_function;
    use arrow::array::Array;
    use arrow::array::LargeStringArray;
    use arrow::array::StringArray;
    use arrow::datatypes::DataType::{LargeUtf8, Utf8};
    use datafusion_common::ScalarValue;
    #[test]
    fn test_functions() -> Result<()> {
        test_function!(
            ReplaceFunc::new(),
            &[
                ColumnarValue::Scalar(ScalarValue::Utf8(Some(String::from("aabbdqcbb")))),
                ColumnarValue::Scalar(ScalarValue::Utf8(Some(String::from("bb")))),
                ColumnarValue::Scalar(ScalarValue::Utf8(Some(String::from("ccc")))),
            ],
            Ok(Some("aacccdqcccc")),
            &str,
            Utf8,
            StringArray
        );

        test_function!(
            ReplaceFunc::new(),
            &[
                ColumnarValue::Scalar(ScalarValue::LargeUtf8(Some(String::from(
                    "aabbb"
                )))),
                ColumnarValue::Scalar(ScalarValue::LargeUtf8(Some(String::from("bbb")))),
                ColumnarValue::Scalar(ScalarValue::LargeUtf8(Some(String::from("cc")))),
            ],
            Ok(Some("aacc")),
            &str,
            LargeUtf8,
            LargeStringArray
        );

        test_function!(
            ReplaceFunc::new(),
            &[
                ColumnarValue::Scalar(ScalarValue::Utf8View(Some(String::from(
                    "aabbbcw"
                )))),
                ColumnarValue::Scalar(ScalarValue::Utf8View(Some(String::from("bb")))),
                ColumnarValue::Scalar(ScalarValue::Utf8View(Some(String::from("cc")))),
            ],
            Ok(Some("aaccbcw")),
            &str,
            Utf8,
            StringArray
        );

        Ok(())
    }
}
