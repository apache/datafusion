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

use crate::utils::{make_scalar_function, utf8_to_str_type};
use arrow::array::{
    ArrayRef, AsArray, GenericStringArray, GenericStringBuilder, Int64Array,
    OffsetSizeTrait, StringArrayType, StringViewArray,
};
use arrow::datatypes::DataType;
use arrow::datatypes::DataType::{LargeUtf8, Utf8, Utf8View};
use datafusion_common::cast::as_int64_array;
use datafusion_common::types::{logical_int64, logical_string, NativeType};
use datafusion_common::{exec_err, DataFusionError, Result};
use datafusion_expr::{ColumnarValue, Documentation, Volatility};
use datafusion_expr::{ScalarFunctionArgs, ScalarUDFImpl, Signature};
use datafusion_expr_common::signature::{Coercion, TypeSignatureClass};
use datafusion_macros::user_doc;

#[user_doc(
    doc_section(label = "String Functions"),
    description = "Returns a string with an input string repeated a specified number.",
    syntax_example = "repeat(str, n)",
    sql_example = r#"```sql
> select repeat('data', 3);
+-------------------------------+
| repeat(Utf8("data"),Int64(3)) |
+-------------------------------+
| datadatadata                  |
+-------------------------------+
```"#,
    standard_argument(name = "str", prefix = "String"),
    argument(
        name = "n",
        description = "Number of times to repeat the input string."
    )
)]
#[derive(Debug, PartialEq, Eq, Hash)]
pub struct RepeatFunc {
    signature: Signature,
}

impl Default for RepeatFunc {
    fn default() -> Self {
        Self::new()
    }
}

impl RepeatFunc {
    pub fn new() -> Self {
        Self {
            signature: Signature::coercible(
                vec![
                    Coercion::new_exact(TypeSignatureClass::Native(logical_string())),
                    // Accept all integer types but cast them to i64
                    Coercion::new_implicit(
                        TypeSignatureClass::Native(logical_int64()),
                        vec![TypeSignatureClass::Integer],
                        NativeType::Int64,
                    ),
                ],
                Volatility::Immutable,
            ),
        }
    }
}

impl ScalarUDFImpl for RepeatFunc {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "repeat"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        utf8_to_str_type(&arg_types[0], "repeat")
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        make_scalar_function(repeat, vec![])(&args.args)
    }

    fn documentation(&self) -> Option<&Documentation> {
        self.doc()
    }
}

/// Repeats string the specified number of times.
/// repeat('Pg', 4) = 'PgPgPgPg'
fn repeat(args: &[ArrayRef]) -> Result<ArrayRef> {
    let number_array = as_int64_array(&args[1])?;
    match args[0].data_type() {
        Utf8View => {
            let string_view_array = args[0].as_string_view();
            repeat_impl::<i32, &StringViewArray>(
                &string_view_array,
                number_array,
                i32::MAX as usize,
            )
        }
        Utf8 => {
            let string_array = args[0].as_string::<i32>();
            repeat_impl::<i32, &GenericStringArray<i32>>(
                &string_array,
                number_array,
                i32::MAX as usize,
            )
        }
        LargeUtf8 => {
            let string_array = args[0].as_string::<i64>();
            repeat_impl::<i64, &GenericStringArray<i64>>(
                &string_array,
                number_array,
                i64::MAX as usize,
            )
        }
        other => exec_err!(
            "Unsupported data type {other:?} for function repeat. \
        Expected Utf8, Utf8View or LargeUtf8."
        ),
    }
}

fn repeat_impl<'a, T, S>(
    string_array: &S,
    number_array: &Int64Array,
    max_str_len: usize,
) -> Result<ArrayRef>
where
    T: OffsetSizeTrait,
    S: StringArrayType<'a>,
{
    let mut total_capacity = 0;
    string_array.iter().zip(number_array.iter()).try_for_each(
        |(string, number)| -> Result<(), DataFusionError> {
            match (string, number) {
                (Some(string), Some(number)) if number >= 0 => {
                    let item_capacity = string.len() * number as usize;
                    if item_capacity > max_str_len {
                        return exec_err!(
                            "string size overflow on repeat, max size is {}, but got {}",
                            max_str_len,
                            number as usize * string.len()
                        );
                    }
                    total_capacity += item_capacity;
                }
                _ => (),
            }
            Ok(())
        },
    )?;

    let mut builder =
        GenericStringBuilder::<T>::with_capacity(string_array.len(), total_capacity);

    string_array.iter().zip(number_array.iter()).try_for_each(
        |(string, number)| -> Result<(), DataFusionError> {
            match (string, number) {
                (Some(string), Some(number)) if number >= 0 => {
                    builder.append_value(string.repeat(number as usize));
                }
                (Some(_), Some(_)) => builder.append_value(""),
                _ => builder.append_null(),
            }
            Ok(())
        },
    )?;
    let array = builder.finish();

    Ok(Arc::new(array) as ArrayRef)
}

#[cfg(test)]
mod tests {
    use arrow::array::{Array, StringArray};
    use arrow::datatypes::DataType::Utf8;

    use datafusion_common::ScalarValue;
    use datafusion_common::{exec_err, Result};
    use datafusion_expr::{ColumnarValue, ScalarUDFImpl};

    use crate::string::repeat::RepeatFunc;
    use crate::utils::test::test_function;

    #[test]
    fn test_functions() -> Result<()> {
        test_function!(
            RepeatFunc::new(),
            vec![
                ColumnarValue::Scalar(ScalarValue::Utf8(Some(String::from("Pg")))),
                ColumnarValue::Scalar(ScalarValue::Int64(Some(4))),
            ],
            Ok(Some("PgPgPgPg")),
            &str,
            Utf8,
            StringArray
        );
        test_function!(
            RepeatFunc::new(),
            vec![
                ColumnarValue::Scalar(ScalarValue::Utf8(None)),
                ColumnarValue::Scalar(ScalarValue::Int64(Some(4))),
            ],
            Ok(None),
            &str,
            Utf8,
            StringArray
        );
        test_function!(
            RepeatFunc::new(),
            vec![
                ColumnarValue::Scalar(ScalarValue::Utf8(Some(String::from("Pg")))),
                ColumnarValue::Scalar(ScalarValue::Int64(None)),
            ],
            Ok(None),
            &str,
            Utf8,
            StringArray
        );

        test_function!(
            RepeatFunc::new(),
            vec![
                ColumnarValue::Scalar(ScalarValue::Utf8View(Some(String::from("Pg")))),
                ColumnarValue::Scalar(ScalarValue::Int64(Some(4))),
            ],
            Ok(Some("PgPgPgPg")),
            &str,
            Utf8,
            StringArray
        );
        test_function!(
            RepeatFunc::new(),
            vec![
                ColumnarValue::Scalar(ScalarValue::Utf8View(None)),
                ColumnarValue::Scalar(ScalarValue::Int64(Some(4))),
            ],
            Ok(None),
            &str,
            Utf8,
            StringArray
        );
        test_function!(
            RepeatFunc::new(),
            vec![
                ColumnarValue::Scalar(ScalarValue::Utf8View(Some(String::from("Pg")))),
                ColumnarValue::Scalar(ScalarValue::Int64(None)),
            ],
            Ok(None),
            &str,
            Utf8,
            StringArray
        );
        test_function!(
            RepeatFunc::new(),
            vec![
                ColumnarValue::Scalar(ScalarValue::Utf8(Some(String::from("Pg")))),
                ColumnarValue::Scalar(ScalarValue::Int64(Some(1073741824))),
            ],
            exec_err!(
                "string size overflow on repeat, max size is {}, but got {}",
                i32::MAX,
                2usize * 1073741824
            ),
            &str,
            Utf8,
            StringArray
        );

        Ok(())
    }
}
