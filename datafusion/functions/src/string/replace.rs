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

use crate::utils::{make_scalar_function, utf8_to_str_type};
use datafusion_common::cast::{as_generic_string_array, as_string_view_array};
use datafusion_common::types::logical_string;
use datafusion_common::{Result, exec_err};
use datafusion_expr::type_coercion::binary::{
    binary_to_string_coercion, string_coercion,
};
use datafusion_expr::{
    Coercion, ColumnarValue, Documentation, ScalarFunctionArgs, ScalarUDFImpl, Signature,
    TypeSignatureClass, Volatility,
};
use datafusion_macros::user_doc;
#[user_doc(
    doc_section(label = "String Functions"),
    description = "Replaces all occurrences of a specified substring in a string with a new substring.",
    syntax_example = "replace(str, substr, replacement)",
    sql_example = r#"```sql
> select replace('ABabbaBA', 'ab', 'cd');
+-------------------------------------------------+
| replace(Utf8("ABabbaBA"),Utf8("ab"),Utf8("cd")) |
+-------------------------------------------------+
| ABcdbaBA                                        |
+-------------------------------------------------+
```"#,
    standard_argument(name = "str", prefix = "String"),
    standard_argument(
        name = "substr",
        prefix = "Substring expression to replace in the input string. Substring"
    ),
    standard_argument(name = "replacement", prefix = "Replacement substring")
)]
#[derive(Debug, PartialEq, Eq, Hash)]
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
        Self {
            signature: Signature::coercible(
                vec![
                    Coercion::new_exact(TypeSignatureClass::Native(logical_string())),
                    Coercion::new_exact(TypeSignatureClass::Native(logical_string())),
                    Coercion::new_exact(TypeSignatureClass::Native(logical_string())),
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
        if let Some(coercion_data_type) = string_coercion(&arg_types[0], &arg_types[1])
            .and_then(|dt| string_coercion(&dt, &arg_types[2]))
            .or_else(|| {
                binary_to_string_coercion(&arg_types[0], &arg_types[1])
                    .and_then(|dt| binary_to_string_coercion(&dt, &arg_types[2]))
            })
        {
            utf8_to_str_type(&coercion_data_type, "replace")
        } else {
            exec_err!(
                "Unsupported data types for replace. Expected Utf8, LargeUtf8 or Utf8View"
            )
        }
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let data_types = args
            .args
            .iter()
            .map(|arg| arg.data_type())
            .collect::<Vec<_>>();

        if let Some(coercion_type) = string_coercion(&data_types[0], &data_types[1])
            .and_then(|dt| string_coercion(&dt, &data_types[2]))
            .or_else(|| {
                binary_to_string_coercion(&data_types[0], &data_types[1])
                    .and_then(|dt| binary_to_string_coercion(&dt, &data_types[2]))
            })
        {
            let mut converted_args = Vec::with_capacity(args.args.len());
            for arg in &args.args {
                if arg.data_type() == coercion_type {
                    converted_args.push(arg.clone());
                } else {
                    let converted = arg.cast_to(&coercion_type, None)?;
                    converted_args.push(converted);
                }
            }

            match coercion_type {
                DataType::Utf8 => {
                    make_scalar_function(replace::<i32>, vec![])(&converted_args)
                }
                DataType::LargeUtf8 => {
                    make_scalar_function(replace::<i64>, vec![])(&converted_args)
                }
                DataType::Utf8View => {
                    make_scalar_function(replace_view, vec![])(&converted_args)
                }
                other => exec_err!(
                    "Unsupported coercion data type {other:?} for function replace"
                ),
            }
        } else {
            exec_err!(
                "Unsupported data type {}, {:?}, {:?} for function replace.",
                data_types[0],
                data_types[1],
                data_types[2]
            )
        }
    }

    fn documentation(&self) -> Option<&Documentation> {
        self.doc()
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
            vec![
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
            vec![
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
            vec![
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
