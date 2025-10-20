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

use arrow::array::{as_largestring_array, Array, ArrayRef};
use arrow::datatypes::{DataType, Field};
use datafusion_expr::sort_properties::ExprProperties;
use std::any::Any;
use std::sync::Arc;

use crate::string::concat;
use crate::strings::{
    ColumnarValueRef, LargeStringArrayBuilder, StringArrayBuilder, StringViewArrayBuilder,
};
use datafusion_common::cast::{as_string_array, as_string_view_array};
use datafusion_common::{internal_err, plan_err, Result, ScalarValue};
use datafusion_expr::expr::ScalarFunction;
use datafusion_expr::simplify::{ExprSimplifyResult, SimplifyInfo};
use datafusion_expr::{lit, ColumnarValue, Documentation, Expr, Volatility};
use datafusion_expr::{ScalarFunctionArgs, ScalarUDFImpl, Signature};
use datafusion_macros::user_doc;

#[user_doc(
    doc_section(label = "String Functions"),
    description = "Concatenates multiple strings together.",
    syntax_example = "concat(str[, ..., str_n])",
    sql_example = r#"```sql
> select concat('data', 'f', 'us', 'ion');
+-------------------------------------------------------+
| concat(Utf8("data"),Utf8("f"),Utf8("us"),Utf8("ion")) |
+-------------------------------------------------------+
| datafusion                                            |
+-------------------------------------------------------+
```"#,
    standard_argument(name = "str", prefix = "String"),
    argument(
        name = "str_n",
        description = "Subsequent string expressions to concatenate."
    ),
    related_udf(name = "concat_ws")
)]
#[derive(Debug, PartialEq, Eq, Hash)]
pub struct ConcatFunc {
    signature: Signature,
}

impl Default for ConcatFunc {
    fn default() -> Self {
        ConcatFunc::new()
    }
}

impl ConcatFunc {
    pub fn new() -> Self {
        Self {
            signature: Signature::user_defined(Volatility::Immutable),
        }
    }

    fn concat_arrays(&self, args: &[ColumnarValue]) -> Result<ColumnarValue> {
        use arrow::array::{make_array, ArrayData, AsArray};
        use arrow::compute;

        let arrays: Result<Vec<ArrayRef>> = args
            .iter()
            .map(|arg| match arg {
                ColumnarValue::Array(array) => Ok(Arc::clone(array)),
                ColumnarValue::Scalar(scalar) => scalar.to_array_of_size(1),
            })
            .collect();
        let arrays = arrays?;

        let all_null = arrays.iter().all(|array| array.null_count() == array.len());

        if all_null {
            let return_type = arrays
                .iter()
                .map(|arg| arg.data_type())
                .find(|d| !d.is_null())
                .unwrap_or_else(|| arrays[0].data_type())
                .clone();

            return Ok(ColumnarValue::Array(make_array(ArrayData::new_null(
                &return_type,
                arrays[0].len(),
            ))));
        }

        let mut result_elements = Vec::new();

        for array in &arrays {
            match array.data_type() {
                DataType::List(_) => {
                    let list_array = array.as_list::<i32>();
                    for i in 0..list_array.len() {
                        if !list_array.is_null(i) {
                            let elements = list_array.value(i);
                            result_elements.extend(
                                (0..elements.len()).map(|j| elements.slice(j, 1)),
                            );
                        }
                    }
                }
                DataType::LargeList(_) => {
                    let list_array = array.as_list::<i64>();
                    for i in 0..list_array.len() {
                        if !list_array.is_null(i) {
                            let elements = list_array.value(i);
                            result_elements.extend(
                                (0..elements.len()).map(|j| elements.slice(j, 1)),
                            );
                        }
                    }
                }
                DataType::FixedSizeList(_, size) => {
                    let list_array = array.as_fixed_size_list();
                    for i in 0..list_array.len() {
                        if !list_array.is_null(i) {
                            let elements = list_array.value(i);
                            result_elements.extend(
                                (0..*size as usize).map(|j| elements.slice(j, 1)),
                            );
                        }
                    }
                }
                _ => {
                    return internal_err!(
                        "Unsupported array type for concatenation: {}",
                        array.data_type()
                    )
                }
            }
        }

        if result_elements.is_empty() {
            return plan_err!("No elements to concatenate");
        }

        let element_refs: Vec<&dyn Array> =
            result_elements.iter().map(|a| a.as_ref()).collect();
        let concatenated = compute::concat(&element_refs)?;

        let field = Field::new_list_field(concatenated.data_type().clone(), true);
        let offsets = arrow::buffer::OffsetBuffer::from_lengths([concatenated.len()]);
        let result =
            arrow::array::ListArray::new(Arc::new(field), offsets, concatenated, None);

        Ok(ColumnarValue::Array(Arc::new(result)))
    }
}

impl ScalarUDFImpl for ConcatFunc {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "concat"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn coerce_types(&self, arg_types: &[DataType]) -> Result<Vec<DataType>> {
        use DataType::*;

        if arg_types
            .iter()
            .any(|dt| matches!(dt, List(_) | LargeList(_) | FixedSizeList(_, _)))
        {
            return Ok(arg_types.to_vec());
        }

        for arg_type in arg_types {
            match arg_type {
                Utf8 | Utf8View | LargeUtf8 => {}
                Dictionary(_, value_type) => {
                    if !matches!(value_type.as_ref(), Utf8 | Utf8View | LargeUtf8) {
                        return Ok(vec![Utf8; arg_types.len()]);
                    }
                }
                _ => return Ok(vec![Utf8; arg_types.len()]),
            }
        }

        let mut best_string_type = Utf8;
        for arg_type in arg_types {
            match arg_type {
                Utf8View => best_string_type = Utf8View,
                LargeUtf8 if best_string_type != Utf8View => best_string_type = LargeUtf8,
                Utf8 => {}
                Dictionary(_, value_type) => match value_type.as_ref() {
                    Utf8View => best_string_type = Utf8View,
                    LargeUtf8 if best_string_type != Utf8View => {
                        best_string_type = LargeUtf8
                    }
                    _ => {}
                },
                _ => {}
            }
        }

        Ok(vec![best_string_type; arg_types.len()])
    }

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        use DataType::*;

        if arg_types
            .iter()
            .any(|dt| matches!(dt, List(_) | LargeList(_) | FixedSizeList(_, _)))
        {
            return Ok(arg_types[0].clone());
        }

        let mut dt = &Utf8;
        arg_types.iter().for_each(|data_type| {
            if data_type == &Utf8View {
                dt = data_type;
            }
            if data_type == &LargeUtf8 && dt != &Utf8View {
                dt = data_type;
            }
        });

        Ok(dt.to_owned())
    }

    /// Concatenates the text representations of all the arguments. NULL arguments are ignored.
    /// concat('abcde', 2, NULL, 22) = 'abcde222'
    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let ScalarFunctionArgs { args, .. } = args;

        if args.is_empty() {
            return plan_err!("concat requires at least one argument");
        }

        if args.iter().any(|arg| {
            matches!(
                arg.data_type(),
                DataType::List(_)
                    | DataType::LargeList(_)
                    | DataType::FixedSizeList(_, _)
            )
        }) {
            return self.concat_arrays(&args);
        }

        let has_dictionary_types = args
            .iter()
            .any(|arg| matches!(arg.data_type(), DataType::Dictionary(_, _)));
        let args = if has_dictionary_types {
            let mut processed_args = Vec::with_capacity(args.len());
            for arg in args {
                match arg.data_type() {
                    DataType::Dictionary(_, value_type) => match value_type.as_ref() {
                        DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View => {
                            use arrow::compute::cast;
                            match arg {
                                ColumnarValue::Array(array) => {
                                    let casted =
                                        cast(array.as_ref(), value_type.as_ref())?;
                                    processed_args.push(ColumnarValue::Array(casted));
                                }
                                ColumnarValue::Scalar(ref scalar) => match scalar {
                                    ScalarValue::Dictionary(_, v) => {
                                        processed_args.push(ColumnarValue::Scalar(
                                            v.as_ref().clone(),
                                        ));
                                    }
                                    _ => processed_args.push(arg),
                                },
                            }
                        }
                        _ => {
                            return plan_err!("Dictionary with value type {value_type} is not supported for concat");
                        }
                    },
                    _ => {
                        processed_args.push(arg);
                    }
                }
            }
            processed_args
        } else {
            args
        };

        let mut return_datatype = DataType::Utf8;
        args.iter().for_each(|col| {
            if col.data_type() == DataType::Utf8View {
                return_datatype = col.data_type();
            }
            if col.data_type() == DataType::LargeUtf8
                && return_datatype != DataType::Utf8View
            {
                return_datatype = col.data_type();
            }
        });

        let array_len = args
            .iter()
            .filter_map(|x| match x {
                ColumnarValue::Array(array) => Some(array.len()),
                _ => None,
            })
            .next();

        // Scalar
        if array_len.is_none() {
            let mut result = String::new();
            for arg in args {
                let ColumnarValue::Scalar(scalar) = arg else {
                    return internal_err!("concat expected scalar value, got {arg:?}");
                };

                match scalar.try_as_str() {
                    Some(Some(v)) => result.push_str(v),
                    Some(None) => {} // null literal
                    None => {
                        // For non-string types, convert to string representation
                        if scalar.is_null() {
                            // Skip null values
                        } else {
                            result.push_str(&format!("{scalar}"));
                        }
                    }
                }
            }

            return match return_datatype {
                DataType::Utf8View => {
                    Ok(ColumnarValue::Scalar(ScalarValue::Utf8View(Some(result))))
                }
                DataType::Utf8 => {
                    Ok(ColumnarValue::Scalar(ScalarValue::Utf8(Some(result))))
                }
                DataType::LargeUtf8 => {
                    Ok(ColumnarValue::Scalar(ScalarValue::LargeUtf8(Some(result))))
                }
                other => {
                    plan_err!("Concat function does not support datatype of {other}")
                }
            };
        }

        // Array
        let len = array_len.unwrap();
        let mut data_size = 0;
        let mut columns = Vec::with_capacity(args.len());

        for arg in &args {
            match arg {
                ColumnarValue::Scalar(ScalarValue::Utf8(maybe_value))
                | ColumnarValue::Scalar(ScalarValue::LargeUtf8(maybe_value))
                | ColumnarValue::Scalar(ScalarValue::Utf8View(maybe_value)) => {
                    if let Some(s) = maybe_value {
                        data_size += s.len() * len;
                        columns.push(ColumnarValueRef::Scalar(s.as_bytes()));
                    }
                }
                ColumnarValue::Array(array) => {
                    match array.data_type() {
                        DataType::Utf8 => {
                            let string_array = as_string_array(array)?;

                            data_size += string_array.values().len();
                            let column = if array.is_nullable() {
                                ColumnarValueRef::NullableArray(string_array)
                            } else {
                                ColumnarValueRef::NonNullableArray(string_array)
                            };
                            columns.push(column);
                        },
                        DataType::LargeUtf8 => {
                            let string_array = as_largestring_array(array);

                            data_size += string_array.values().len();
                            let column = if array.is_nullable() {
                                ColumnarValueRef::NullableLargeStringArray(string_array)
                            } else {
                                ColumnarValueRef::NonNullableLargeStringArray(string_array)
                            };
                            columns.push(column);
                        },
                        DataType::Utf8View => {
                            let string_array = as_string_view_array(array)?;

                            data_size += string_array.len();
                            let column = if array.is_nullable() {
                                ColumnarValueRef::NullableStringViewArray(string_array)
                            } else {
                                ColumnarValueRef::NonNullableStringViewArray(string_array)
                            };
                            columns.push(column);
                        },
                        other => {
                            return plan_err!("Input was {other} which is not a supported datatype for concat function")
                        }
                    };
                }
                _ => {
                    return plan_err!(
                        "Unsupported argument type for concat: {}",
                        arg.data_type()
                    )
                }
            }
        }

        match return_datatype {
            DataType::Utf8 => {
                let mut builder = StringArrayBuilder::with_capacity(len, data_size);
                for i in 0..len {
                    columns
                        .iter()
                        .for_each(|column| builder.write::<true>(column, i));
                    builder.append_offset();
                }

                let string_array = builder.finish(None);
                Ok(ColumnarValue::Array(Arc::new(string_array)))
            }
            DataType::Utf8View => {
                let mut builder = StringViewArrayBuilder::with_capacity(len, data_size);
                for i in 0..len {
                    columns
                        .iter()
                        .for_each(|column| builder.write::<true>(column, i));
                    builder.append_offset();
                }

                let string_array = builder.finish();
                Ok(ColumnarValue::Array(Arc::new(string_array)))
            }
            DataType::LargeUtf8 => {
                let mut builder = LargeStringArrayBuilder::with_capacity(len, data_size);
                for i in 0..len {
                    columns
                        .iter()
                        .for_each(|column| builder.write::<true>(column, i));
                    builder.append_offset();
                }

                let string_array = builder.finish(None);
                Ok(ColumnarValue::Array(Arc::new(string_array)))
            }
            _ => {
                plan_err!("Unsupported return datatype for concat: {return_datatype}")
            }
        }
    }

    /// Simplify the `concat` function by
    /// 1. filtering out all `null` literals
    /// 2. concatenating contiguous literal arguments
    ///
    /// For example:
    /// `concat(col(a), 'hello ', 'world', col(b), null)`
    /// will be optimized to
    /// `concat(col(a), 'hello world', col(b))`
    fn simplify(
        &self,
        args: Vec<Expr>,
        _info: &dyn SimplifyInfo,
    ) -> Result<ExprSimplifyResult> {
        simplify_concat(args)
    }

    fn documentation(&self) -> Option<&Documentation> {
        self.doc()
    }

    fn preserves_lex_ordering(&self, _inputs: &[ExprProperties]) -> Result<bool> {
        Ok(true)
    }
}

pub(crate) fn simplify_concat(args: Vec<Expr>) -> Result<ExprSimplifyResult> {
    let mut new_args = Vec::with_capacity(args.len());
    let mut contiguous_scalar = "".to_string();

    let return_type = {
        let data_types: Vec<_> = args
            .iter()
            .filter_map(|expr| match expr {
                Expr::Literal(l, _) => Some(l.data_type()),
                _ => None,
            })
            .collect();
        ConcatFunc::new().return_type(&data_types)
    }?;

    for arg in args.iter() {
        match arg {
            Expr::Literal(ScalarValue::Utf8(None), _) => {}
            Expr::Literal(ScalarValue::LargeUtf8(None), _) => {}
            Expr::Literal(ScalarValue::Utf8View(None), _) => {}

            Expr::Literal(ScalarValue::Utf8(Some(v)), _) => {
                contiguous_scalar += v;
            }
            Expr::Literal(ScalarValue::LargeUtf8(Some(v)), _) => {
                contiguous_scalar += v;
            }
            Expr::Literal(ScalarValue::Utf8View(Some(v)), _) => {
                contiguous_scalar += v;
            }

            Expr::Literal(_x, _) => {
                if !contiguous_scalar.is_empty() {
                    match return_type {
                        DataType::Utf8 => new_args.push(lit(contiguous_scalar)),
                        DataType::LargeUtf8 => new_args
                            .push(lit(ScalarValue::LargeUtf8(Some(contiguous_scalar)))),
                        DataType::Utf8View => new_args
                            .push(lit(ScalarValue::Utf8View(Some(contiguous_scalar)))),
                        _ => return Ok(ExprSimplifyResult::Original(args)),
                    }
                    contiguous_scalar = "".to_string();
                }
                new_args.push(arg.clone());
            }
            arg => {
                if !contiguous_scalar.is_empty() {
                    match return_type {
                        DataType::Utf8 => new_args.push(lit(contiguous_scalar)),
                        DataType::LargeUtf8 => new_args
                            .push(lit(ScalarValue::LargeUtf8(Some(contiguous_scalar)))),
                        DataType::Utf8View => new_args
                            .push(lit(ScalarValue::Utf8View(Some(contiguous_scalar)))),
                        _ => return Ok(ExprSimplifyResult::Original(args)),
                    }
                    contiguous_scalar = "".to_string();
                }
                new_args.push(arg.clone());
            }
        }
    }

    if !contiguous_scalar.is_empty() {
        match return_type {
            DataType::Utf8 => new_args.push(lit(contiguous_scalar)),
            DataType::LargeUtf8 => {
                new_args.push(lit(ScalarValue::LargeUtf8(Some(contiguous_scalar))))
            }
            DataType::Utf8View => {
                new_args.push(lit(ScalarValue::Utf8View(Some(contiguous_scalar))))
            }
            _ => return Ok(ExprSimplifyResult::Original(args)),
        }
    }

    if !args.eq(&new_args) {
        Ok(ExprSimplifyResult::Simplified(Expr::ScalarFunction(
            ScalarFunction {
                func: concat(),
                args: new_args,
            },
        )))
    } else {
        Ok(ExprSimplifyResult::Original(args))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::utils::test::test_function;
    use arrow::array::{Array, LargeStringArray, StringViewArray};
    use arrow::array::{ArrayRef, StringArray};
    use arrow::datatypes::Field;
    use datafusion_common::config::ConfigOptions;
    use DataType::*;

    #[test]
    fn test_functions() -> Result<()> {
        test_function!(
            ConcatFunc::new(),
            vec![
                ColumnarValue::Scalar(ScalarValue::from("aa")),
                ColumnarValue::Scalar(ScalarValue::from("bb")),
                ColumnarValue::Scalar(ScalarValue::from("cc")),
            ],
            Ok(Some("aabbcc")),
            &str,
            Utf8,
            StringArray
        );
        test_function!(
            ConcatFunc::new(),
            vec![
                ColumnarValue::Scalar(ScalarValue::from("aa")),
                ColumnarValue::Scalar(ScalarValue::Utf8(None)),
                ColumnarValue::Scalar(ScalarValue::from("cc")),
            ],
            Ok(Some("aacc")),
            &str,
            Utf8,
            StringArray
        );
        test_function!(
            ConcatFunc::new(),
            vec![ColumnarValue::Scalar(ScalarValue::Utf8(None))],
            Ok(Some("")),
            &str,
            Utf8,
            StringArray
        );
        test_function!(
            ConcatFunc::new(),
            vec![
                ColumnarValue::Scalar(ScalarValue::from("aa")),
                ColumnarValue::Scalar(ScalarValue::Utf8View(None)),
                ColumnarValue::Scalar(ScalarValue::LargeUtf8(None)),
                ColumnarValue::Scalar(ScalarValue::from("cc")),
            ],
            Ok(Some("aacc")),
            &str,
            Utf8View,
            StringViewArray
        );
        test_function!(
            ConcatFunc::new(),
            vec![
                ColumnarValue::Scalar(ScalarValue::from("aa")),
                ColumnarValue::Scalar(ScalarValue::LargeUtf8(None)),
                ColumnarValue::Scalar(ScalarValue::from("cc")),
            ],
            Ok(Some("aacc")),
            &str,
            LargeUtf8,
            LargeStringArray
        );
        test_function!(
            ConcatFunc::new(),
            vec![
                ColumnarValue::Scalar(ScalarValue::Utf8View(Some("aa".to_string()))),
                ColumnarValue::Scalar(ScalarValue::Utf8(Some("cc".to_string()))),
            ],
            Ok(Some("aacc")),
            &str,
            Utf8View,
            StringViewArray
        );

        Ok(())
    }

    #[test]
    fn concat() -> Result<()> {
        let c0 =
            ColumnarValue::Array(Arc::new(StringArray::from(vec!["foo", "bar", "baz"])));
        let c1 = ColumnarValue::Scalar(ScalarValue::Utf8(Some(",".to_string())));
        let c2 = ColumnarValue::Array(Arc::new(StringArray::from(vec![
            Some("x"),
            None,
            Some("z"),
        ])));
        let c3 = ColumnarValue::Scalar(ScalarValue::Utf8View(Some(",".to_string())));
        let c4 = ColumnarValue::Array(Arc::new(StringViewArray::from(vec![
            Some("a"),
            None,
            Some("b"),
        ])));
        let arg_fields = vec![
            Field::new("a", Utf8, true),
            Field::new("a", Utf8, true),
            Field::new("a", Utf8, true),
            Field::new("a", Utf8View, true),
            Field::new("a", Utf8View, true),
        ]
        .into_iter()
        .map(Arc::new)
        .collect::<Vec<_>>();

        let args = ScalarFunctionArgs {
            args: vec![c0, c1, c2, c3, c4],
            arg_fields,
            number_rows: 3,
            return_field: Field::new("f", Utf8, true).into(),
            config_options: Arc::new(ConfigOptions::default()),
        };

        let result = ConcatFunc::new().invoke_with_args(args)?;
        let expected =
            Arc::new(StringViewArray::from(vec!["foo,x,a", "bar,,", "baz,z,b"]))
                as ArrayRef;
        match &result {
            ColumnarValue::Array(array) => {
                assert_eq!(&expected, array);
            }
            _ => panic!(),
        }
        Ok(())
    }

    #[test]
    fn test_array_concat() -> Result<()> {
        use arrow::array::{Int64Array, ListArray};
        use arrow::buffer::OffsetBuffer;
        use DataType::*;

        // Create list arrays: [1, 2] and [3, 4]
        let list1_values = Arc::new(Int64Array::from(vec![1, 2]));
        let list1_field = Arc::new(Field::new_list_field(Int64, true));
        let list1_offsets = OffsetBuffer::from_lengths([2]);
        let list1 =
            ListArray::new(list1_field.clone(), list1_offsets, list1_values, None);

        let list2_values = Arc::new(Int64Array::from(vec![3, 4]));
        let list2_offsets = OffsetBuffer::from_lengths([2]);
        let list2 =
            ListArray::new(list1_field.clone(), list2_offsets, list2_values, None);

        let args = vec![
            ColumnarValue::Array(Arc::new(list1)),
            ColumnarValue::Array(Arc::new(list2)),
        ];

        let result = ConcatFunc::new().concat_arrays(&args)?;

        // Expected result: [1, 2, 3, 4]
        match result {
            ColumnarValue::Array(array) => {
                let list_array = array.as_any().downcast_ref::<ListArray>().unwrap();
                assert_eq!(list_array.len(), 1);
                let values = list_array.value(0);
                let int_values = values.as_any().downcast_ref::<Int64Array>().unwrap();
                assert_eq!(int_values.values(), &[1, 2, 3, 4]);
            }
            _ => panic!("Expected array result"),
        }

        Ok(())
    }

    #[test]
    fn test_concat_with_integers() -> Result<()> {
        use datafusion_common::config::ConfigOptions;
        use DataType::*;

        let args = vec![
            ColumnarValue::Scalar(ScalarValue::Utf8(Some("abc".to_string()))),
            ColumnarValue::Scalar(ScalarValue::Int64(Some(123))),
            ColumnarValue::Scalar(ScalarValue::Utf8(None)), // NULL
            ColumnarValue::Scalar(ScalarValue::Int64(Some(456))),
        ];

        let arg_fields = vec![
            Field::new("a", Utf8, true),
            Field::new("b", Int64, true),
            Field::new("c", Utf8, true),
            Field::new("d", Int64, true),
        ]
        .into_iter()
        .map(Arc::new)
        .collect::<Vec<_>>();

        let func_args = ScalarFunctionArgs {
            args,
            arg_fields,
            number_rows: 1,
            return_field: Field::new("f", Utf8, true).into(),
            config_options: Arc::new(ConfigOptions::default()),
        };

        let result = ConcatFunc::new().invoke_with_args(func_args)?;

        // Expected result should be "abc123456"
        match result {
            ColumnarValue::Scalar(ScalarValue::Utf8(Some(s))) => {
                assert_eq!(s, "abc123456");
            }
            _ => panic!("Expected scalar UTF8 result, got {:?}", result),
        }

        Ok(())
    }
}
