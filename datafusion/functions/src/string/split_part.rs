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

use crate::utils::utf8_to_str_type;
use arrow::array::{
    ArrayRef, AsArray, GenericStringBuilder, Int64Array, StringArrayType,
    StringLikeArrayBuilder, StringViewBuilder,
};
use arrow::datatypes::DataType;
use datafusion_common::ScalarValue;
use datafusion_common::cast::as_int64_array;
use datafusion_common::types::{NativeType, logical_int64, logical_string};
use datafusion_common::{Result, exec_datafusion_err, exec_err};
use datafusion_expr::{
    Coercion, ColumnarValue, Documentation, TypeSignatureClass, Volatility,
};
use datafusion_expr::{ScalarFunctionArgs, ScalarUDFImpl, Signature};
use datafusion_macros::user_doc;
use std::sync::Arc;

#[user_doc(
    doc_section(label = "String Functions"),
    description = "Splits a string based on a specified delimiter and returns the substring in the specified position.",
    syntax_example = "split_part(str, delimiter, pos)",
    sql_example = r#"```sql
> select split_part('1.2.3.4.5', '.', 3);
+--------------------------------------------------+
| split_part(Utf8("1.2.3.4.5"),Utf8("."),Int64(3)) |
+--------------------------------------------------+
| 3                                                |
+--------------------------------------------------+
```"#,
    standard_argument(name = "str", prefix = "String"),
    argument(name = "delimiter", description = "String or character to split on."),
    argument(
        name = "pos",
        description = "Position of the part to return (counting from 1). Negative values count backward from the end of the string."
    )
)]
#[derive(Debug, PartialEq, Eq, Hash)]
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
        Self {
            signature: Signature::coercible(
                vec![
                    Coercion::new_exact(TypeSignatureClass::Native(logical_string())),
                    Coercion::new_exact(TypeSignatureClass::Native(logical_string())),
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

impl ScalarUDFImpl for SplitPartFunc {
    fn name(&self) -> &str {
        "split_part"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        if arg_types[0] == DataType::Utf8View {
            Ok(DataType::Utf8View)
        } else {
            utf8_to_str_type(&arg_types[0], "split_part")
        }
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let ScalarFunctionArgs { args, .. } = args;

        // First, determine if any of the arguments is an Array
        let len = args.iter().find_map(|arg| match arg {
            ColumnarValue::Array(a) => Some(a.len()),
            _ => None,
        });

        let inferred_length = len.unwrap_or(1);
        let is_scalar = len.is_none();

        // Convert all ColumnarValues to ArrayRefs
        let args = args
            .iter()
            .map(|arg| match arg {
                ColumnarValue::Scalar(scalar) => scalar.to_array_of_size(inferred_length),
                ColumnarValue::Array(array) => Ok(Arc::clone(array)),
            })
            .collect::<Result<Vec<_>>>()?;

        // Unpack the ArrayRefs from the arguments
        let n_array = as_int64_array(&args[2])?;

        // Dispatch on delimiter type for a given string array and builder.
        macro_rules! split_part_for_delimiter_type {
            ($str_arr:expr, $builder:expr) => {
                match args[1].data_type() {
                    DataType::Utf8View => split_part_impl(
                        $str_arr,
                        &args[1].as_string_view(),
                        n_array,
                        $builder,
                    ),
                    DataType::Utf8 => split_part_impl(
                        $str_arr,
                        &args[1].as_string::<i32>(),
                        n_array,
                        $builder,
                    ),
                    DataType::LargeUtf8 => split_part_impl(
                        $str_arr,
                        &args[1].as_string::<i64>(),
                        n_array,
                        $builder,
                    ),
                    other => {
                        exec_err!("Unsupported delimiter type {other:?} for split_part")
                    }
                }
            };
        }

        let result = match args[0].data_type() {
            DataType::Utf8View => split_part_for_delimiter_type!(
                &args[0].as_string_view(),
                StringViewBuilder::with_capacity(inferred_length)
            ),
            DataType::Utf8 => {
                let str_arr = &args[0].as_string::<i32>();
                split_part_for_delimiter_type!(
                    str_arr,
                    GenericStringBuilder::<i32>::with_capacity(
                        inferred_length,
                        str_arr.value_data().len(),
                    )
                )
            }
            DataType::LargeUtf8 => {
                let str_arr = &args[0].as_string::<i64>();
                split_part_for_delimiter_type!(
                    str_arr,
                    GenericStringBuilder::<i64>::with_capacity(
                        inferred_length,
                        str_arr.value_data().len(),
                    )
                )
            }
            other => exec_err!("Unsupported string type {other:?} for split_part"),
        };
        if is_scalar {
            // If all inputs are scalar, keep the output as scalar
            let result = result.and_then(|arr| ScalarValue::try_from_array(&arr, 0));
            result.map(ColumnarValue::Scalar)
        } else {
            result.map(ColumnarValue::Array)
        }
    }

    fn documentation(&self) -> Option<&Documentation> {
        self.doc()
    }
}

/// Finds the nth split part of `string` by `delimiter`.
#[inline]
fn split_nth<'a>(string: &'a str, delimiter: &str, n: usize) -> Option<&'a str> {
    if delimiter.len() == 1 {
        // A single-byte UTF-8 string is always ASCII, so we can safely cast
        // just the first byte to a character. `str::split(char)` internally
        // uses memchr::memchr and is notably faster than `str::split(&str)`,
        // even for a single character string.
        string.split(delimiter.as_bytes()[0] as char).nth(n)
    } else {
        string.split(delimiter).nth(n)
    }
}

/// Like `split_nth` but splits from the right.
#[inline]
fn rsplit_nth<'a>(string: &'a str, delimiter: &str, n: usize) -> Option<&'a str> {
    if delimiter.len() == 1 {
        // A single-byte UTF-8 string is always ASCII, so we can safely cast
        // just the first byte to a character. `str::rsplit(char)` internally
        // uses memchr::memrchr and is notably faster than `str::rsplit(&str)`,
        // even for a single character string.
        string.rsplit(delimiter.as_bytes()[0] as char).nth(n)
    } else {
        string.rsplit(delimiter).nth(n)
    }
}

fn split_part_impl<'a, StringArrType, DelimiterArrType, B>(
    string_array: &StringArrType,
    delimiter_array: &DelimiterArrType,
    n_array: &Int64Array,
    mut builder: B,
) -> Result<ArrayRef>
where
    StringArrType: StringArrayType<'a>,
    DelimiterArrType: StringArrayType<'a>,
    B: StringLikeArrayBuilder,
{
    for ((string, delimiter), n) in string_array
        .iter()
        .zip(delimiter_array.iter())
        .zip(n_array.iter())
    {
        match (string, delimiter, n) {
            (Some(string), Some(delimiter), Some(n)) => {
                let result = match n.cmp(&0) {
                    std::cmp::Ordering::Greater => {
                        let idx: usize = (n - 1).try_into().map_err(|_| {
                            exec_datafusion_err!(
                                "split_part index {n} exceeds maximum supported value"
                            )
                        })?;
                        if delimiter.is_empty() {
                            // Match PostgreSQL's behavior: empty delimiter
                            // treats input as a single field, so only position
                            // 1 returns data.
                            (n == 1).then_some(string)
                        } else {
                            split_nth(string, delimiter, idx)
                        }
                    }
                    std::cmp::Ordering::Less => {
                        let idx: usize =
                            (n.unsigned_abs() - 1).try_into().map_err(|_| {
                                exec_datafusion_err!(
                                    "split_part index {n} exceeds minimum supported value"
                                )
                            })?;
                        if delimiter.is_empty() {
                            // Match PostgreSQL's behavior: empty delimiter
                            // treats input as a single field, so only position
                            // -1 returns data.
                            (n == -1).then_some(string)
                        } else {
                            rsplit_nth(string, delimiter, idx)
                        }
                    }
                    std::cmp::Ordering::Equal => {
                        return exec_err!("field position must not be zero");
                    }
                };
                builder.append_value(result.unwrap_or(""));
            }
            _ => builder.append_null(),
        }
    }

    Ok(Arc::new(builder.finish()) as ArrayRef)
}

#[cfg(test)]
mod tests {
    use arrow::array::{Array, StringArray};
    use arrow::datatypes::DataType::Utf8;

    use datafusion_common::ScalarValue;
    use datafusion_common::{Result, exec_err};
    use datafusion_expr::{ColumnarValue, ScalarUDFImpl};

    use crate::string::split_part::SplitPartFunc;
    use crate::utils::test::test_function;

    #[test]
    fn test_functions() -> Result<()> {
        test_function!(
            SplitPartFunc::new(),
            vec![
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
            vec![
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
            vec![
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
            vec![
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
        test_function!(
            SplitPartFunc::new(),
            vec![
                ColumnarValue::Scalar(ScalarValue::Utf8(Some(String::from(
                    "abc~@~def~@~ghi"
                )))),
                ColumnarValue::Scalar(ScalarValue::Utf8(Some(String::from("~@~")))),
                ColumnarValue::Scalar(ScalarValue::Int64(Some(i64::MIN))),
            ],
            Ok(Some("")),
            &str,
            Utf8,
            StringArray
        );
        // Edge cases with delimiters
        test_function!(
            SplitPartFunc::new(),
            vec![
                ColumnarValue::Scalar(ScalarValue::Utf8(Some(String::from("a,b")))),
                ColumnarValue::Scalar(ScalarValue::Utf8(Some(String::from(",")))),
                ColumnarValue::Scalar(ScalarValue::Int64(Some(1))),
            ],
            Ok(Some("a")),
            &str,
            Utf8,
            StringArray
        );
        test_function!(
            SplitPartFunc::new(),
            vec![
                ColumnarValue::Scalar(ScalarValue::Utf8(Some(String::from("a,b")))),
                ColumnarValue::Scalar(ScalarValue::Utf8(Some(String::from(",")))),
                ColumnarValue::Scalar(ScalarValue::Int64(Some(3))),
            ],
            Ok(Some("")),
            &str,
            Utf8,
            StringArray
        );
        test_function!(
            SplitPartFunc::new(),
            vec![
                ColumnarValue::Scalar(ScalarValue::Utf8(Some(String::from("a,b")))),
                ColumnarValue::Scalar(ScalarValue::Utf8(Some(String::from("")))),
                ColumnarValue::Scalar(ScalarValue::Int64(Some(1))),
            ],
            Ok(Some("a,b")),
            &str,
            Utf8,
            StringArray
        );
        test_function!(
            SplitPartFunc::new(),
            vec![
                ColumnarValue::Scalar(ScalarValue::Utf8(Some(String::from("a,b")))),
                ColumnarValue::Scalar(ScalarValue::Utf8(Some(String::from("")))),
                ColumnarValue::Scalar(ScalarValue::Int64(Some(2))),
            ],
            Ok(Some("")),
            &str,
            Utf8,
            StringArray
        );
        test_function!(
            SplitPartFunc::new(),
            vec![
                ColumnarValue::Scalar(ScalarValue::Utf8(Some(String::from("a,b")))),
                ColumnarValue::Scalar(ScalarValue::Utf8(Some(String::from(" ")))),
                ColumnarValue::Scalar(ScalarValue::Int64(Some(1))),
            ],
            Ok(Some("a,b")),
            &str,
            Utf8,
            StringArray
        );
        test_function!(
            SplitPartFunc::new(),
            vec![
                ColumnarValue::Scalar(ScalarValue::Utf8(Some(String::from("a,b")))),
                ColumnarValue::Scalar(ScalarValue::Utf8(Some(String::from(" ")))),
                ColumnarValue::Scalar(ScalarValue::Int64(Some(2))),
            ],
            Ok(Some("")),
            &str,
            Utf8,
            StringArray
        );

        // Edge cases with delimiters with negative n
        test_function!(
            SplitPartFunc::new(),
            vec![
                ColumnarValue::Scalar(ScalarValue::Utf8(Some(String::from("a,b")))),
                ColumnarValue::Scalar(ScalarValue::Utf8(Some(String::from("")))),
                ColumnarValue::Scalar(ScalarValue::Int64(Some(-1))),
            ],
            Ok(Some("a,b")),
            &str,
            Utf8,
            StringArray
        );
        test_function!(
            SplitPartFunc::new(),
            vec![
                ColumnarValue::Scalar(ScalarValue::Utf8(Some(String::from("a,b")))),
                ColumnarValue::Scalar(ScalarValue::Utf8(Some(String::from(" ")))),
                ColumnarValue::Scalar(ScalarValue::Int64(Some(-1))),
            ],
            Ok(Some("a,b")),
            &str,
            Utf8,
            StringArray
        );
        test_function!(
            SplitPartFunc::new(),
            vec![
                ColumnarValue::Scalar(ScalarValue::Utf8(Some(String::from("a,b")))),
                ColumnarValue::Scalar(ScalarValue::Utf8(Some(String::from("")))),
                ColumnarValue::Scalar(ScalarValue::Int64(Some(-2))),
            ],
            Ok(Some("")),
            &str,
            Utf8,
            StringArray
        );

        Ok(())
    }
}
