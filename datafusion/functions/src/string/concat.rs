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

use arrow::array::{as_largestring_array, Array};
use arrow::datatypes::DataType;
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
    description = "Concatenates multiple strings or arrays together.",
    syntax_example = "concat(str[, ..., str_n]) or concat(array[, ..., array_n])",
    sql_example = r#"```sql
> select concat('data', 'f', 'us', 'ion');
+-------------------------------------------------------+
| concat(Utf8("data"),Utf8("f"),Utf8("us"),Utf8("ion")) |
+-------------------------------------------------------+
| datafusion                                            |
+-------------------------------------------------------+
> select concat(make_array(1, 2), make_array(3, 4));
+------------------------------------------+
| concat(make_array(1, 2), make_array(3, 4)) |
+------------------------------------------+
| [1, 2, 3, 4]                             |
+------------------------------------------+
```"#,
    standard_argument(name = "str_or_array", prefix = "String or Array"),
    argument(
        name = "str_or_array_n",
        description = "Subsequent string or array expressions to concatenate. Cannot mix strings and arrays."
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

    /// Get the string type with highest precedence: Utf8View > LargeUtf8 > Utf8
    ///
    /// Utf8View is preferred for performance (zero-copy views),
    /// LargeUtf8 supports larger strings (i64 offsets),
    /// Utf8 is the fallback standard string type
    fn get_string_type_precedence(&self, arg_types: &[DataType]) -> DataType {
        use DataType::*;

        for data_type in arg_types {
            if data_type == &Utf8View {
                return Utf8View;
            }
        }

        for data_type in arg_types {
            if data_type == &LargeUtf8 {
                return LargeUtf8;
            }
        }

        Utf8
    }

    /// Concatenate array arguments
    fn concat_arrays(&self, args: &[ColumnarValue]) -> Result<ColumnarValue> {
        if args.is_empty() {
            return plan_err!("concat requires at least one argument");
        }

        // Convert ColumnarValue arguments to ArrayRef
        let arrays = ColumnarValue::values_to_arrays(args)?;

        // Check if all arrays are null - concat errors in this case
        // This matches PostgreSQL behavior where concat() with all NULL values returns an error
        let mut all_null = true;
        for arg in &arrays {
            if arg.data_type() == &DataType::Null {
                continue;
            }
            if arg.null_count() < arg.len() {
                all_null = false;
            }
        }

        if all_null {
            return plan_err!("No valid arrays to concatenate");
        }

        // Delegate to shared array concatenation
        let result = crate::utils::concat_arrays(&arrays)?;
        Ok(ColumnarValue::Array(result))
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

        if arg_types.is_empty() {
            return plan_err!("concat requires at least one argument");
        }

        let has_arrays = arg_types
            .iter()
            .any(|dt| matches!(dt, List(_) | LargeList(_) | FixedSizeList(_, _)));
        let has_non_arrays = arg_types
            .iter()
            .any(|dt| !matches!(dt, List(_) | LargeList(_) | FixedSizeList(_, _) | Null));

        if has_arrays && has_non_arrays {
            return plan_err!(
                "Cannot mix array and non-array arguments in concat function."
            );
        }

        if has_arrays {
            return Ok(arg_types.to_vec());
        }

        let target_type = self.get_string_type_precedence(arg_types);

        // Only coerce types that need coercion, keep string types as-is
        let coerced_types = arg_types
            .iter()
            .map(|data_type| match data_type {
                Utf8View | Utf8 | LargeUtf8 => data_type.clone(),
                _ => target_type.clone(),
            })
            .collect();
        Ok(coerced_types)
    }

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        use DataType::*;

        if arg_types.is_empty() {
            return plan_err!("concat requires at least one argument");
        }

        // After coercion, all arguments have the same type category, so check only the first
        if let List(field) | LargeList(field) | FixedSizeList(field, _) = &arg_types[0] {
            return Ok(List(Arc::new(arrow::datatypes::Field::new(
                "item",
                field.data_type().clone(),
                true,
            ))));
        }

        // For non-array arguments, return string type based on precedence
        let dt = self.get_string_type_precedence(arg_types);
        Ok(dt)
    }

    /// Concatenates the text representations of all the arguments. NULL arguments are ignored.
    /// concat('abcde', 2, NULL, 22) = 'abcde222'
    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        use DataType::*;
        let ScalarFunctionArgs { args, .. } = args;

        if args.is_empty() {
            return plan_err!("concat requires at least one argument");
        }

        // After coercion, all arguments have the same type category, so check only the first
        let is_array = match &args[0] {
            ColumnarValue::Array(array) => matches!(
                array.data_type(),
                List(_) | LargeList(_) | FixedSizeList(_, _)
            ),
            ColumnarValue::Scalar(scalar) => matches!(
                scalar.data_type(),
                List(_) | LargeList(_) | FixedSizeList(_, _)
            ),
        };
        if is_array {
            return self.concat_arrays(&args);
        }

        let data_types: Vec<DataType> = args.iter().map(|col| col.data_type()).collect();
        let return_datatype = self.get_string_type_precedence(&data_types);

        let array_len = args
            .iter()
            .filter_map(|x| match x {
                ColumnarValue::Array(array) => Some(array.len()),
                _ => None,
            })
            .next();

        // Scalar case
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
                        if scalar.is_null() {
                            // Skip null values
                        } else {
                            result.push_str(&format!("{scalar}"));
                        }
                    }
                }
            }

            return match return_datatype {
                Utf8View => {
                    Ok(ColumnarValue::Scalar(ScalarValue::Utf8View(Some(result))))
                }
                Utf8 => Ok(ColumnarValue::Scalar(ScalarValue::Utf8(Some(result)))),
                LargeUtf8 => {
                    Ok(ColumnarValue::Scalar(ScalarValue::LargeUtf8(Some(result))))
                }
                other => {
                    plan_err!("Concat function does not support datatype of {other}")
                }
            };
        }

        // Array case
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
                        Utf8 => {
                            let string_array = as_string_array(array)?;

                            data_size += string_array.values().len();
                            let column = if array.is_nullable() {
                                ColumnarValueRef::NullableArray(string_array)
                            } else {
                                ColumnarValueRef::NonNullableArray(string_array)
                            };
                            columns.push(column);
                        }
                        LargeUtf8 => {
                            let string_array = as_largestring_array(array);

                            data_size += string_array.values().len();
                            let column = if array.is_nullable() {
                                ColumnarValueRef::NullableLargeStringArray(string_array)
                            } else {
                                ColumnarValueRef::NonNullableLargeStringArray(
                                    string_array,
                                )
                            };
                            columns.push(column);
                        }
                        Utf8View => {
                            let string_array = as_string_view_array(array)?;

                            data_size += string_array.len();
                            let column = if array.is_nullable() {
                                ColumnarValueRef::NullableStringViewArray(string_array)
                            } else {
                                ColumnarValueRef::NonNullableStringViewArray(string_array)
                            };
                            columns.push(column);
                        }
                        other => {
                            return plan_err!("Input was {other} which is not a supported datatype for concat function")
                        }
                    };
                }
                _ => return plan_err!("Unsupported argument type: {}", arg.data_type()),
            }
        }

        match return_datatype {
            Utf8 => {
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
            Utf8View => {
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
            LargeUtf8 => {
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
            _ => plan_err!("Unsupported return datatype: {return_datatype}"),
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
    use DataType::*;

    if args.is_empty() {
        return plan_err!("concat requires at least one argument");
    }
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

            // filter out `null` args
            // All literals have been converted to Utf8 or LargeUtf8 in type_coercion.
            // Concatenate it with the `contiguous_scalar`.
            Expr::Literal(ScalarValue::Utf8(Some(v)), _) => {
                contiguous_scalar += v;
            }
            Expr::Literal(ScalarValue::LargeUtf8(Some(v)), _) => {
                contiguous_scalar += v;
            }
            Expr::Literal(ScalarValue::Utf8View(Some(v)), _) => {
                contiguous_scalar += v;
            }

            Expr::Literal(scalar_val, _) => {
                // Convert non-string, non-array literals to their string representation
                // Skip array literals - they should be handled at runtime
                if matches!(
                    scalar_val.data_type(),
                    List(_) | LargeList(_) | FixedSizeList(_, _)
                ) {
                    if !contiguous_scalar.is_empty() {
                        match return_type {
                            Utf8 => new_args.push(lit(contiguous_scalar)),
                            LargeUtf8 => new_args.push(lit(ScalarValue::LargeUtf8(
                                Some(contiguous_scalar),
                            ))),
                            Utf8View => new_args.push(lit(ScalarValue::Utf8View(Some(
                                contiguous_scalar,
                            )))),
                            _ => return Ok(ExprSimplifyResult::Original(args)),
                        }
                        contiguous_scalar = "".to_string();
                    }
                    new_args.push(arg.clone());
                } else {
                    // Convert non-string, non-array literals to their string representation
                    // This is needed during simplification phase which happens before coercion
                    // Skip NULL values (concat ignores NULLs)
                    if !scalar_val.is_null() {
                        let string_repr = format!("{scalar_val}");
                        contiguous_scalar += &string_repr;
                    }
                }
            }
            // If the arg is not a literal, we should first push the current `contiguous_scalar`
            // to the `new_args` (if it is not empty) and reset it to empty string.
            // Then pushing this arg to the `new_args`.
            arg => {
                if !contiguous_scalar.is_empty() {
                    match return_type {
                        Utf8 => new_args.push(lit(contiguous_scalar)),
                        LargeUtf8 => new_args
                            .push(lit(ScalarValue::LargeUtf8(Some(contiguous_scalar)))),
                        Utf8View => new_args
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
            Utf8 => new_args.push(lit(contiguous_scalar)),
            LargeUtf8 => {
                new_args.push(lit(ScalarValue::LargeUtf8(Some(contiguous_scalar))))
            }
            Utf8View => {
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
        .collect();

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
    fn test_concat_with_integers() -> Result<()> {
        use datafusion_common::config::ConfigOptions;

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
        .collect();

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
            _ => panic!("Expected scalar UTF8 result, got {result:?}"),
        }

        Ok(())
    }

    #[test]
    fn test_array_concatenation_comprehensive() -> Result<()> {
        use arrow::array::{Int32Array, ListArray};
        use arrow::datatypes::{Field, Int32Type};
        use datafusion_common::config::ConfigOptions;

        // Test basic array concatenation
        let arr1 = Arc::new(ListArray::from_iter_primitive::<Int32Type, _, _>(vec![
            Some(vec![Some(1), Some(2)]),
        ]));
        let arr2 = Arc::new(ListArray::from_iter_primitive::<Int32Type, _, _>(vec![
            Some(vec![Some(3), Some(4)]),
        ]));

        let args = vec![ColumnarValue::Array(arr1), ColumnarValue::Array(arr2)];

        let arg_fields = vec![
            Field::new("a", List(Arc::new(Field::new("item", Int32, true))), true),
            Field::new("b", List(Arc::new(Field::new("item", Int32, true))), true),
        ]
        .into_iter()
        .map(Arc::new)
        .collect();

        let func_args = ScalarFunctionArgs {
            args,
            arg_fields,
            number_rows: 1,
            return_field: Field::new(
                "result",
                List(Arc::new(Field::new("item", Int32, true))),
                true,
            )
            .into(),
            config_options: Arc::new(ConfigOptions::default()),
        };

        let result = ConcatFunc::new().invoke_with_args(func_args)?;

        match result {
            ColumnarValue::Array(array) => {
                let list_array = array.as_any().downcast_ref::<ListArray>().unwrap();
                let concatenated = list_array.value(0);
                let int_array =
                    concatenated.as_any().downcast_ref::<Int32Array>().unwrap();

                assert_eq!(int_array.len(), 4);
                assert_eq!(int_array.value(0), 1);
                assert_eq!(int_array.value(1), 2);
                assert_eq!(int_array.value(2), 3);
                assert_eq!(int_array.value(3), 4);
            }
            _ => panic!("Expected array result"),
        }

        Ok(())
    }

    #[test]
    fn test_mixed_type_error() -> Result<()> {
        use arrow::datatypes::Field;

        // Test that coerce_types properly rejects mixed array/non-array types
        let func = ConcatFunc::new();
        let arg_types = vec![List(Arc::new(Field::new("item", Int32, true))), Utf8];

        let result = func.coerce_types(&arg_types);
        assert!(result.is_err());
        let err_msg = result.unwrap_err().to_string();
        assert!(err_msg.contains("Cannot mix array and non-array arguments"));

        Ok(())
    }
}
