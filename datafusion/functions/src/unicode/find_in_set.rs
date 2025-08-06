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

use arrow::array::{
    new_null_array, ArrayAccessor, ArrayIter, ArrayRef, ArrowPrimitiveType, AsArray,
    OffsetSizeTrait, PrimitiveArray,
};
use arrow::datatypes::{ArrowNativeType, DataType, Int32Type, Int64Type};

use crate::utils::utf8_to_int_type;
use datafusion_common::{
    exec_err, internal_err, utils::take_function_args, Result, ScalarValue,
};
use datafusion_expr::TypeSignature::Exact;
use datafusion_expr::{
    ColumnarValue, Documentation, ScalarFunctionArgs, ScalarUDFImpl, Signature,
    Volatility,
};
use datafusion_macros::user_doc;

#[user_doc(
    doc_section(label = "String Functions"),
    description = "Returns a value in the range of 1 to N if the string str is in the string list strlist consisting of N substrings.",
    syntax_example = "find_in_set(str, strlist)",
    sql_example = r#"```sql
> select find_in_set('b', 'a,b,c,d');
+----------------------------------------+
| find_in_set(Utf8("b"),Utf8("a,b,c,d")) |
+----------------------------------------+
| 2                                      |
+----------------------------------------+
```"#,
    argument(name = "str", description = "String expression to find in strlist."),
    argument(
        name = "strlist",
        description = "A string list is a string composed of substrings separated by , characters."
    )
)]
#[derive(Debug)]
pub struct FindInSetFunc {
    signature: Signature,
}

impl Default for FindInSetFunc {
    fn default() -> Self {
        Self::new()
    }
}

impl FindInSetFunc {
    pub fn new() -> Self {
        use DataType::*;
        Self {
            signature: Signature::one_of(
                vec![
                    Exact(vec![Utf8View, Utf8View]),
                    Exact(vec![Utf8, Utf8]),
                    Exact(vec![LargeUtf8, LargeUtf8]),
                ],
                Volatility::Immutable,
            ),
        }
    }
}

impl ScalarUDFImpl for FindInSetFunc {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "find_in_set"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        utf8_to_int_type(&arg_types[0], "find_in_set")
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let ScalarFunctionArgs { args, .. } = args;

        let [string, str_list] = take_function_args(self.name(), args)?;

        match (string, str_list) {
            // both inputs are scalars
            (
                ColumnarValue::Scalar(
                    ScalarValue::Utf8View(string)
                    | ScalarValue::Utf8(string)
                    | ScalarValue::LargeUtf8(string),
                ),
                ColumnarValue::Scalar(
                    ScalarValue::Utf8View(str_list)
                    | ScalarValue::Utf8(str_list)
                    | ScalarValue::LargeUtf8(str_list),
                ),
            ) => {
                let res = match (string, str_list) {
                    (Some(string), Some(str_list)) => {
                        let position = str_list
                            .split(',')
                            .position(|s| s == string)
                            .map_or(0, |idx| idx + 1);

                        Some(position as i32)
                    }
                    _ => None,
                };
                Ok(ColumnarValue::Scalar(ScalarValue::from(res)))
            }

            // `string` is an array, `str_list` is scalar
            (
                ColumnarValue::Array(str_array),
                ColumnarValue::Scalar(
                    ScalarValue::Utf8View(str_list_literal)
                    | ScalarValue::Utf8(str_list_literal)
                    | ScalarValue::LargeUtf8(str_list_literal),
                ),
            ) => {
                let result_array = match str_list_literal {
                    // find_in_set(column_a, null) = null
                    None => new_null_array(str_array.data_type(), str_array.len()),
                    Some(str_list_literal) => {
                        let str_list = str_list_literal.split(',').collect::<Vec<&str>>();
                        let result = match str_array.data_type() {
                            DataType::Utf8 => {
                                let string_array = str_array.as_string::<i32>();
                                find_in_set_right_literal::<Int32Type, _>(
                                    string_array,
                                    str_list,
                                )
                            }
                            DataType::LargeUtf8 => {
                                let string_array = str_array.as_string::<i64>();
                                find_in_set_right_literal::<Int64Type, _>(
                                    string_array,
                                    str_list,
                                )
                            }
                            DataType::Utf8View => {
                                let string_array = str_array.as_string_view();
                                find_in_set_right_literal::<Int32Type, _>(
                                    string_array,
                                    str_list,
                                )
                            }
                            other => {
                                exec_err!("Unsupported data type {other:?} for function find_in_set")
                            }
                        };
                        Arc::new(result?)
                    }
                };
                Ok(ColumnarValue::Array(result_array))
            }

            // `string` is scalar, `str_list` is an array
            (
                ColumnarValue::Scalar(
                    ScalarValue::Utf8View(string_literal)
                    | ScalarValue::Utf8(string_literal)
                    | ScalarValue::LargeUtf8(string_literal),
                ),
                ColumnarValue::Array(str_list_array),
            ) => {
                let res = match string_literal {
                    // find_in_set(null, column_b) = null
                    None => {
                        new_null_array(str_list_array.data_type(), str_list_array.len())
                    }
                    Some(string) => {
                        let result = match str_list_array.data_type() {
                            DataType::Utf8 => {
                                let str_list = str_list_array.as_string::<i32>();
                                find_in_set_left_literal::<Int32Type, _>(string, str_list)
                            }
                            DataType::LargeUtf8 => {
                                let str_list = str_list_array.as_string::<i64>();
                                find_in_set_left_literal::<Int64Type, _>(string, str_list)
                            }
                            DataType::Utf8View => {
                                let str_list = str_list_array.as_string_view();
                                find_in_set_left_literal::<Int32Type, _>(string, str_list)
                            }
                            other => {
                                exec_err!("Unsupported data type {other:?} for function find_in_set")
                            }
                        };
                        Arc::new(result?)
                    }
                };
                Ok(ColumnarValue::Array(res))
            }

            // both inputs are arrays
            (ColumnarValue::Array(base_array), ColumnarValue::Array(exp_array)) => {
                let res = find_in_set(base_array, exp_array)?;

                Ok(ColumnarValue::Array(res))
            }
            _ => {
                internal_err!("Invalid argument types for `find_in_set` function")
            }
        }
    }

    fn documentation(&self) -> Option<&Documentation> {
        self.doc()
    }
}

/// Returns a value in the range of 1 to N if the string `str` is in the string list `strlist`
/// consisting of N substrings. A string list is a string composed of substrings separated by `,`
/// characters.
fn find_in_set(str: ArrayRef, str_list: ArrayRef) -> Result<ArrayRef> {
    match str.data_type() {
        DataType::Utf8 => {
            let string_array = str.as_string::<i32>();
            let str_list_array = str_list.as_string::<i32>();
            find_in_set_general::<Int32Type, _>(string_array, str_list_array)
        }
        DataType::LargeUtf8 => {
            let string_array = str.as_string::<i64>();
            let str_list_array = str_list.as_string::<i64>();
            find_in_set_general::<Int64Type, _>(string_array, str_list_array)
        }
        DataType::Utf8View => {
            let string_array = str.as_string_view();
            let str_list_array = str_list.as_string_view();
            find_in_set_general::<Int32Type, _>(string_array, str_list_array)
        }
        other => {
            exec_err!("Unsupported data type {other:?} for function find_in_set")
        }
    }
}

pub fn find_in_set_general<'a, T, V>(
    string_array: V,
    str_list_array: V,
) -> Result<ArrayRef>
where
    T: ArrowPrimitiveType,
    T::Native: OffsetSizeTrait,
    V: ArrayAccessor<Item = &'a str>,
{
    let string_iter = ArrayIter::new(string_array);
    let str_list_iter = ArrayIter::new(str_list_array);

    let mut builder = PrimitiveArray::<T>::builder(string_iter.len());

    string_iter
        .zip(str_list_iter)
        .for_each(
            |(string_opt, str_list_opt)| match (string_opt, str_list_opt) {
                (Some(string), Some(str_list)) => {
                    let position = str_list
                        .split(',')
                        .position(|s| s == string)
                        .map_or(0, |idx| idx + 1);
                    builder.append_value(T::Native::from_usize(position).unwrap());
                }
                _ => builder.append_null(),
            },
        );

    Ok(Arc::new(builder.finish()) as ArrayRef)
}

fn find_in_set_left_literal<'a, T, V>(
    string: String,
    str_list_array: V,
) -> Result<ArrayRef>
where
    T: ArrowPrimitiveType,
    T::Native: OffsetSizeTrait,
    V: ArrayAccessor<Item = &'a str>,
{
    let mut builder = PrimitiveArray::<T>::builder(str_list_array.len());

    let str_list_iter = ArrayIter::new(str_list_array);

    str_list_iter.for_each(|str_list_opt| match str_list_opt {
        Some(str_list) => {
            let position = str_list
                .split(',')
                .position(|s| s == string)
                .map_or(0, |idx| idx + 1);
            builder.append_value(T::Native::from_usize(position).unwrap());
        }
        None => builder.append_null(),
    });

    Ok(Arc::new(builder.finish()) as ArrayRef)
}

fn find_in_set_right_literal<'a, T, V>(
    string_array: V,
    str_list: Vec<&str>,
) -> Result<ArrayRef>
where
    T: ArrowPrimitiveType,
    T::Native: OffsetSizeTrait,
    V: ArrayAccessor<Item = &'a str>,
{
    let mut builder = PrimitiveArray::<T>::builder(string_array.len());

    let string_iter = ArrayIter::new(string_array);

    string_iter.for_each(|string_opt| match string_opt {
        Some(string) => {
            let position = str_list
                .iter()
                .position(|s| *s == string)
                .map_or(0, |idx| idx + 1);
            builder.append_value(T::Native::from_usize(position).unwrap());
        }
        None => builder.append_null(),
    });

    Ok(Arc::new(builder.finish()) as ArrayRef)
}

#[cfg(test)]
mod tests {
    use crate::unicode::find_in_set::FindInSetFunc;
    use crate::utils::test::test_function;
    use arrow::array::{Array, Int32Array, StringArray};
    use arrow::datatypes::{DataType::Int32, Field};
    use datafusion_common::config::ConfigOptions;
    use datafusion_common::{Result, ScalarValue};
    use datafusion_expr::{ColumnarValue, ScalarFunctionArgs, ScalarUDFImpl};
    use std::sync::Arc;

    #[test]
    fn test_functions() -> Result<()> {
        test_function!(
            FindInSetFunc::new(),
            vec![
                ColumnarValue::Scalar(ScalarValue::Utf8(Some(String::from("a")))),
                ColumnarValue::Scalar(ScalarValue::Utf8(Some(String::from("a,b,c")))),
            ],
            Ok(Some(1)),
            i32,
            Int32,
            Int32Array
        );
        test_function!(
            FindInSetFunc::new(),
            vec![
                ColumnarValue::Scalar(ScalarValue::Utf8View(Some(String::from("🔥")))),
                ColumnarValue::Scalar(ScalarValue::Utf8View(Some(String::from(
                    "a,Д,🔥"
                )))),
            ],
            Ok(Some(3)),
            i32,
            Int32,
            Int32Array
        );
        test_function!(
            FindInSetFunc::new(),
            vec![
                ColumnarValue::Scalar(ScalarValue::Utf8View(Some(String::from("d")))),
                ColumnarValue::Scalar(ScalarValue::Utf8View(Some(String::from("a,b,c")))),
            ],
            Ok(Some(0)),
            i32,
            Int32,
            Int32Array
        );
        test_function!(
            FindInSetFunc::new(),
            vec![
                ColumnarValue::Scalar(ScalarValue::Utf8View(Some(String::from(
                    "Apache Software Foundation"
                )))),
                ColumnarValue::Scalar(ScalarValue::Utf8View(Some(String::from(
                    "Github,Apache Software Foundation,DataFusion"
                )))),
            ],
            Ok(Some(2)),
            i32,
            Int32,
            Int32Array
        );
        test_function!(
            FindInSetFunc::new(),
            vec![
                ColumnarValue::Scalar(ScalarValue::Utf8(Some(String::from("")))),
                ColumnarValue::Scalar(ScalarValue::Utf8(Some(String::from("a,b,c")))),
            ],
            Ok(Some(0)),
            i32,
            Int32,
            Int32Array
        );
        test_function!(
            FindInSetFunc::new(),
            vec![
                ColumnarValue::Scalar(ScalarValue::Utf8(Some(String::from("a")))),
                ColumnarValue::Scalar(ScalarValue::Utf8(Some(String::from("")))),
            ],
            Ok(Some(0)),
            i32,
            Int32,
            Int32Array
        );
        test_function!(
            FindInSetFunc::new(),
            vec![
                ColumnarValue::Scalar(ScalarValue::Utf8View(Some(String::from("a")))),
                ColumnarValue::Scalar(ScalarValue::Utf8View(None)),
            ],
            Ok(None),
            i32,
            Int32,
            Int32Array
        );
        test_function!(
            FindInSetFunc::new(),
            vec![
                ColumnarValue::Scalar(ScalarValue::Utf8View(None)),
                ColumnarValue::Scalar(ScalarValue::Utf8View(Some(String::from("a,b,c")))),
            ],
            Ok(None),
            i32,
            Int32,
            Int32Array
        );

        Ok(())
    }

    macro_rules! test_find_in_set {
        ($test_name:ident, $args:expr, $expected:expr) => {
            #[test]
            fn $test_name() -> Result<()> {
                let fis = crate::unicode::find_in_set();

                let args = $args;
                let expected = $expected;

                let type_array = args.iter().map(|a| a.data_type()).collect::<Vec<_>>();
                let cardinality = args
                    .iter()
                    .fold(Option::<usize>::None, |acc, arg| match arg {
                        ColumnarValue::Scalar(_) => acc,
                        ColumnarValue::Array(a) => Some(a.len()),
                    })
                    .unwrap_or(1);
                let return_type = fis.return_type(&type_array)?;
                let arg_fields = args
                    .iter()
                    .enumerate()
                    .map(|(idx, a)| {
                        Field::new(format!("arg_{idx}"), a.data_type(), true).into()
                    })
                    .collect::<Vec<_>>();
                let result = fis.invoke_with_args(ScalarFunctionArgs {
                    args,
                    arg_fields,
                    number_rows: cardinality,
                    return_field: Field::new("f", return_type, true).into(),
                    config_options: Arc::new(ConfigOptions::default()),
                });
                assert!(result.is_ok());

                let result = result?
                    .to_array(cardinality)
                    .expect("Failed to convert to array");
                let result = result
                    .as_any()
                    .downcast_ref::<Int32Array>()
                    .expect("Failed to convert to type");
                assert_eq!(*result, expected);

                Ok(())
            }
        };
    }

    test_find_in_set!(
        test_find_in_set_with_scalar_args,
        vec![
            ColumnarValue::Array(Arc::new(StringArray::from(vec![
                "", "a", "b", "c", "d"
            ]))),
            ColumnarValue::Scalar(ScalarValue::Utf8(Some("b,c,d".to_string()))),
        ],
        Int32Array::from(vec![0, 0, 1, 2, 3])
    );
    test_find_in_set!(
        test_find_in_set_with_scalar_args_2,
        vec![
            ColumnarValue::Scalar(ScalarValue::Utf8View(Some(
                "ApacheSoftware".to_string()
            ))),
            ColumnarValue::Array(Arc::new(StringArray::from(vec![
                "a,b,c",
                "ApacheSoftware,Github,DataFusion",
                ""
            ]))),
        ],
        Int32Array::from(vec![0, 1, 0])
    );
    test_find_in_set!(
        test_find_in_set_with_scalar_args_3,
        vec![
            ColumnarValue::Array(Arc::new(StringArray::from(vec![None::<&str>; 3]))),
            ColumnarValue::Scalar(ScalarValue::Utf8View(Some("a,b,c".to_string()))),
        ],
        Int32Array::from(vec![None::<i32>; 3])
    );
    test_find_in_set!(
        test_find_in_set_with_scalar_args_4,
        vec![
            ColumnarValue::Scalar(ScalarValue::Utf8View(Some("a".to_string()))),
            ColumnarValue::Array(Arc::new(StringArray::from(vec![None::<&str>; 3]))),
        ],
        Int32Array::from(vec![None::<i32>; 3])
    );
}
