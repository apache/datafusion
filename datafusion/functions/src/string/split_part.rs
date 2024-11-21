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

use crate::strings::StringArrayType;
use crate::utils::utf8_to_str_type;
use arrow::array::{
    ArrayRef, GenericStringArray, Int64Array, OffsetSizeTrait, StringViewArray,
};
use arrow::array::{AsArray, GenericStringBuilder};
use arrow::datatypes::DataType;
use datafusion_common::cast::as_int64_array;
use datafusion_common::ScalarValue;
use datafusion_common::{exec_err, DataFusionError, Result};
use datafusion_expr::scalar_doc_sections::DOC_SECTION_STRING;
use datafusion_expr::{ColumnarValue, Documentation, TypeSignature, Volatility};
use datafusion_expr::{ScalarUDFImpl, Signature};
use std::any::Any;
use std::sync::{Arc, OnceLock};

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
                    TypeSignature::Exact(vec![Utf8View, Utf8View, Int64]),
                    TypeSignature::Exact(vec![Utf8View, Utf8, Int64]),
                    TypeSignature::Exact(vec![Utf8View, LargeUtf8, Int64]),
                    TypeSignature::Exact(vec![Utf8, Utf8View, Int64]),
                    TypeSignature::Exact(vec![Utf8, Utf8, Int64]),
                    TypeSignature::Exact(vec![LargeUtf8, Utf8View, Int64]),
                    TypeSignature::Exact(vec![LargeUtf8, Utf8, Int64]),
                    TypeSignature::Exact(vec![Utf8, LargeUtf8, Int64]),
                    TypeSignature::Exact(vec![LargeUtf8, LargeUtf8, Int64]),
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
        let result = match (args[0].data_type(), args[1].data_type()) {
            (DataType::Utf8View, DataType::Utf8View) => {
                split_part_impl::<&StringViewArray, &StringViewArray, i32>(
                    args[0].as_string_view(),
                    args[1].as_string_view(),
                    n_array,
                )
            }
            (DataType::Utf8View, DataType::Utf8) => {
                split_part_impl::<&StringViewArray, &GenericStringArray<i32>, i32>(
                    args[0].as_string_view(),
                    args[1].as_string::<i32>(),
                    n_array,
                )
            }
            (DataType::Utf8View, DataType::LargeUtf8) => {
                split_part_impl::<&StringViewArray, &GenericStringArray<i64>, i32>(
                    args[0].as_string_view(),
                    args[1].as_string::<i64>(),
                    n_array,
                )
            }
            (DataType::Utf8, DataType::Utf8View) => {
                split_part_impl::<&GenericStringArray<i32>, &StringViewArray, i32>(
                    args[0].as_string::<i32>(),
                    args[1].as_string_view(),
                    n_array,
                )
            }
            (DataType::LargeUtf8, DataType::Utf8View) => {
                split_part_impl::<&GenericStringArray<i64>, &StringViewArray, i64>(
                    args[0].as_string::<i64>(),
                    args[1].as_string_view(),
                    n_array,
                )
            }
            (DataType::Utf8, DataType::Utf8) => {
                split_part_impl::<&GenericStringArray<i32>, &GenericStringArray<i32>, i32>(
                    args[0].as_string::<i32>(),
                    args[1].as_string::<i32>(),
                    n_array,
                )
            }
            (DataType::LargeUtf8, DataType::LargeUtf8) => {
                split_part_impl::<&GenericStringArray<i64>, &GenericStringArray<i64>, i64>(
                    args[0].as_string::<i64>(),
                    args[1].as_string::<i64>(),
                    n_array,
                )
            }
            (DataType::Utf8, DataType::LargeUtf8) => {
                split_part_impl::<&GenericStringArray<i32>, &GenericStringArray<i64>, i32>(
                    args[0].as_string::<i32>(),
                    args[1].as_string::<i64>(),
                    n_array,
                )
            }
            (DataType::LargeUtf8, DataType::Utf8) => {
                split_part_impl::<&GenericStringArray<i64>, &GenericStringArray<i32>, i64>(
                    args[0].as_string::<i64>(),
                    args[1].as_string::<i32>(),
                    n_array,
                )
            }
            _ => exec_err!("Unsupported combination of argument types for split_part"),
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
        Some(get_split_part_doc())
    }
}

static DOCUMENTATION: OnceLock<Documentation> = OnceLock::new();

fn get_split_part_doc() -> &'static Documentation {
    DOCUMENTATION.get_or_init(|| {
        Documentation::builder()
            .with_doc_section(DOC_SECTION_STRING)
            .with_description("Splits a string based on a specified delimiter and returns the substring in the specified position.")
            .with_syntax_example("split_part(str, delimiter, pos)")
            .with_sql_example(r#"```sql
> select split_part('1.2.3.4.5', '.', 3);
+--------------------------------------------------+
| split_part(Utf8("1.2.3.4.5"),Utf8("."),Int64(3)) |
+--------------------------------------------------+
| 3                                                |
+--------------------------------------------------+
```"#)
            .with_standard_argument("str", Some("String"))
            .with_argument("delimiter", "String or character to split on.")
            .with_argument("pos", "Position of the part to return.")
            .build()
            .unwrap()
    })
}

/// impl
pub fn split_part_impl<'a, StringArrType, DelimiterArrType, StringArrayLen>(
    string_array: StringArrType,
    delimiter_array: DelimiterArrType,
    n_array: &Int64Array,
) -> Result<ArrayRef>
where
    StringArrType: StringArrayType<'a>,
    DelimiterArrType: StringArrayType<'a>,
    StringArrayLen: OffsetSizeTrait,
{
    let mut builder: GenericStringBuilder<StringArrayLen> = GenericStringBuilder::new();

    string_array
        .iter()
        .zip(delimiter_array.iter())
        .zip(n_array.iter())
        .try_for_each(|((string, delimiter), n)| -> Result<(), DataFusionError> {
            match (string, delimiter, n) {
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
                        builder.append_value(split_string[index]);
                    } else {
                        builder.append_value("");
                    }
                }
                _ => builder.append_null(),
            }
            Ok(())
        })?;

    Ok(Arc::new(builder.finish()) as ArrayRef)
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
