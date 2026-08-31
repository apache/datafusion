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

use std::sync::Arc;

use arrow::array::{
    ArrayAccessor, ArrayRef, ArrowPrimitiveType, AsArray, OffsetSizeTrait, PrimitiveArray,
};
use arrow::datatypes::{ArrowNativeType, DataType, Int32Type, Int64Type};
use arrow_buffer::NullBuffer;

use crate::utils::utf8_to_int_type;
use datafusion_common::{
    HashMap, Result, ScalarValue, exec_err, internal_err, utils::take_function_args,
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
#[derive(Debug, PartialEq, Eq, Hash)]
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
        let return_field = args.return_field;
        let [string, str_list] = take_function_args(self.name(), args.args)?;

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
                match str_list_literal {
                    // find_in_set(column_a, null) = null
                    None => Ok(ColumnarValue::Scalar(ScalarValue::try_new_null(
                        return_field.data_type(),
                    )?)),
                    Some(str_list_literal) => {
                        let str_list = str_list_literal.split(',').collect::<Vec<&str>>();
                        let result = match str_array.data_type() {
                            DataType::Utf8 => {
                                let string_array = str_array.as_string::<i32>();
                                find_in_set_right_literal::<Int32Type, _>(
                                    string_array,
                                    &str_list,
                                )
                            }
                            DataType::LargeUtf8 => {
                                let string_array = str_array.as_string::<i64>();
                                find_in_set_right_literal::<Int64Type, _>(
                                    string_array,
                                    &str_list,
                                )
                            }
                            DataType::Utf8View => {
                                let string_array = str_array.as_string_view();
                                find_in_set_right_literal::<Int32Type, _>(
                                    string_array,
                                    &str_list,
                                )
                            }
                            other => {
                                exec_err!(
                                    "Unsupported data type {other:?} for function find_in_set"
                                )
                            }
                        };
                        Ok(ColumnarValue::Array(Arc::new(result?)))
                    }
                }
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
                match string_literal {
                    // find_in_set(null, column_b) = null
                    None => Ok(ColumnarValue::Scalar(ScalarValue::try_new_null(
                        return_field.data_type(),
                    )?)),
                    Some(string) => {
                        let result = match str_list_array.data_type() {
                            DataType::Utf8 => {
                                let str_list = str_list_array.as_string::<i32>();
                                find_in_set_left_literal::<Int32Type, _>(
                                    &string, str_list,
                                )
                            }
                            DataType::LargeUtf8 => {
                                let str_list = str_list_array.as_string::<i64>();
                                find_in_set_left_literal::<Int64Type, _>(
                                    &string, str_list,
                                )
                            }
                            DataType::Utf8View => {
                                let str_list = str_list_array.as_string_view();
                                find_in_set_left_literal::<Int32Type, _>(
                                    &string, str_list,
                                )
                            }
                            other => {
                                exec_err!(
                                    "Unsupported data type {other:?} for function find_in_set"
                                )
                            }
                        };
                        Ok(ColumnarValue::Array(Arc::new(result?)))
                    }
                }
            }

            // both inputs are arrays
            (ColumnarValue::Array(base_array), ColumnarValue::Array(exp_array)) => {
                let res = find_in_set(&base_array, &exp_array)?;

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
fn find_in_set(str: &ArrayRef, str_list: &ArrayRef) -> Result<ArrayRef> {
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

fn find_in_set_general<'a, T, V>(string_array: V, str_list_array: V) -> Result<ArrayRef>
where
    T: ArrowPrimitiveType,
    T::Native: OffsetSizeTrait,
    V: ArrayAccessor<Item = &'a str> + Copy,
{
    let len = string_array.len();
    let nulls = NullBuffer::union(string_array.nulls(), str_list_array.nulls());
    let zero = T::Native::from_usize(0).unwrap();

    let values: Vec<T::Native> = (0..len)
        .map(|i| {
            if nulls.as_ref().is_some_and(|n| n.is_null(i)) {
                return zero;
            }
            let string = string_array.value(i);
            let str_list = str_list_array.value(i);
            let position = str_list
                .split(',')
                .position(|s| s == string)
                .map_or(0, |idx| idx + 1);
            T::Native::from_usize(position).unwrap()
        })
        .collect();

    Ok(Arc::new(PrimitiveArray::<T>::new(values.into(), nulls)) as ArrayRef)
}

fn find_in_set_left_literal<'a, T, V>(string: &str, str_list_array: V) -> Result<ArrayRef>
where
    T: ArrowPrimitiveType,
    T::Native: OffsetSizeTrait,
    V: ArrayAccessor<Item = &'a str> + Copy,
{
    let len = str_list_array.len();
    let nulls = str_list_array.nulls().cloned();
    let zero = T::Native::from_usize(0).unwrap();

    let values: Vec<T::Native> = (0..len)
        .map(|i| {
            if nulls.as_ref().is_some_and(|n| n.is_null(i)) {
                return zero;
            }
            let str_list = str_list_array.value(i);
            let position = str_list
                .split(',')
                .position(|s| s == string)
                .map_or(0, |idx| idx + 1);
            T::Native::from_usize(position).unwrap()
        })
        .collect();

    Ok(Arc::new(PrimitiveArray::<T>::new(values.into(), nulls)) as ArrayRef)
}

/// Minimum set length at which a pre-built lookup beats a per-row linear scan.
/// Below this, the linear scan's small constant factor wins, so short sets are
/// left untouched to avoid regressing them.
const FIND_IN_SET_LOOKUP_THRESHOLD: usize = 16;

fn find_in_set_right_literal<'a, T, V>(
    string_array: V,
    str_list: &[&str],
) -> Result<ArrayRef>
where
    T: ArrowPrimitiveType,
    T::Native: OffsetSizeTrait,
    V: ArrayAccessor<Item = &'a str> + Copy,
{
    let len = string_array.len();
    let nulls = string_array.nulls().cloned();
    let zero = T::Native::from_usize(0).unwrap();

    // The set (`str_list`) is constant across all rows. For a large set, the
    // per-row `position` linear scan is O(set_len). Building a lookup from each
    // distinct entry to its 1-based position once turns each row into an O(1)
    // probe (first occurrence wins, exactly matching `position`). Below the
    // threshold the linear scan's small constant factor is faster, so the map is
    // built at most once here rather than per row.
    let map: Option<HashMap<&str, usize>> =
        (str_list.len() >= FIND_IN_SET_LOOKUP_THRESHOLD).then(|| {
            let mut map = HashMap::with_capacity(str_list.len());
            for (idx, entry) in str_list.iter().enumerate() {
                map.entry(*entry).or_insert(idx + 1);
            }
            map
        });

    let values: Vec<T::Native> = (0..len)
        .map(|i| {
            if nulls.as_ref().is_some_and(|n| n.is_null(i)) {
                return zero;
            }
            let string = string_array.value(i);
            let position = match &map {
                Some(map) => map.get(string).copied().unwrap_or(0),
                None => str_list
                    .iter()
                    .position(|s| *s == string)
                    .map_or(0, |idx| idx + 1),
            };
            T::Native::from_usize(position).unwrap()
        })
        .collect();

    Ok(Arc::new(PrimitiveArray::<T>::new(values.into(), nulls)) as ArrayRef)
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

    // Exercises both the lookup-map path (list length >= threshold) and the
    // linear-scan path (short list), including a duplicate entry to confirm the
    // first occurrence wins in both.
    #[test]
    fn test_right_literal_lookup_matches_linear() {
        use super::find_in_set_right_literal;
        use arrow::datatypes::Int32Type;

        // 40 unique entries plus a duplicate of "item5" appended at index 40, so
        // the length is well over FIND_IN_SET_LOOKUP_THRESHOLD.
        let mut long_list: Vec<String> = (0..40).map(|i| format!("item{i}")).collect();
        long_list.push("item5".to_string());
        let long_refs: Vec<&str> = long_list.iter().map(|s| s.as_str()).collect();
        let short_refs = ["a", "b", "c"];

        let strings = StringArray::from(vec![
            Some("item0"),
            Some("item39"),
            Some("item5"),
            Some("missing"),
            None,
            Some("b"),
        ]);

        let long =
            find_in_set_right_literal::<Int32Type, _>(&strings, &long_refs).unwrap();
        let long = long.as_any().downcast_ref::<Int32Array>().unwrap();
        assert_eq!(long.value(0), 1);
        assert_eq!(long.value(1), 40);
        assert_eq!(long.value(2), 6); // first occurrence of "item5"
        assert_eq!(long.value(3), 0);
        assert!(long.is_null(4));
        assert_eq!(long.value(5), 0);

        let short =
            find_in_set_right_literal::<Int32Type, _>(&strings, &short_refs).unwrap();
        let short = short.as_any().downcast_ref::<Int32Array>().unwrap();
        assert_eq!(short.value(0), 0);
        assert!(short.is_null(4));
        assert_eq!(short.value(5), 2); // "b" at position 2
    }
}
