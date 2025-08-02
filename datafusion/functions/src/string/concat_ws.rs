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

use arrow::array::{as_largestring_array, Array, StringArray};
use std::any::Any;
use std::sync::Arc;

use arrow::datatypes::DataType;

use crate::string::concat;
use crate::string::concat::simplify_concat;
use crate::string::concat_ws;
use crate::strings::{ColumnarValueRef, StringArrayBuilder};
use datafusion_common::cast::{as_string_array, as_string_view_array};
use datafusion_common::{exec_err, internal_err, plan_err, Result, ScalarValue};
use datafusion_expr::expr::ScalarFunction;
use datafusion_expr::simplify::{ExprSimplifyResult, SimplifyInfo};
use datafusion_expr::{lit, ColumnarValue, Documentation, Expr, Volatility};
use datafusion_expr::{ScalarFunctionArgs, ScalarUDFImpl, Signature};
use datafusion_macros::user_doc;

#[user_doc(
    doc_section(label = "String Functions"),
    description = "Concatenates multiple strings together with a specified separator.",
    syntax_example = "concat_ws(separator, str[, ..., str_n])",
    sql_example = r#"```sql
> select concat_ws('_', 'data', 'fusion');
+--------------------------------------------------+
| concat_ws(Utf8("_"),Utf8("data"),Utf8("fusion")) |
+--------------------------------------------------+
| data_fusion                                      |
+--------------------------------------------------+
```"#,
    argument(
        name = "separator",
        description = "Separator to insert between concatenated strings."
    ),
    argument(
        name = "str",
        description = "String expression to operate on. Can be a constant, column, or function, and any combination of operators."
    ),
    argument(
        name = "str_n",
        description = "Subsequent string expressions to concatenate."
    ),
    related_udf(name = "concat")
)]
#[derive(Debug)]
pub struct ConcatWsFunc {
    signature: Signature,
}

impl Default for ConcatWsFunc {
    fn default() -> Self {
        ConcatWsFunc::new()
    }
}

impl ConcatWsFunc {
    pub fn new() -> Self {
        use DataType::*;
        Self {
            signature: Signature::variadic(
                vec![Utf8View, Utf8, LargeUtf8],
                Volatility::Immutable,
            ),
        }
    }
}

impl ScalarUDFImpl for ConcatWsFunc {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "concat_ws"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        use DataType::*;
        Ok(Utf8)
    }

    /// Concatenates all but the first argument, with separators. The first argument is used as the separator string, and should not be NULL. Other NULL arguments are ignored.
    /// concat_ws(',', 'abcde', 2, NULL, 22) = 'abcde,2,22'
    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let ScalarFunctionArgs { args, .. } = args;

        // do not accept 0 arguments.
        if args.len() < 2 {
            return exec_err!(
                "concat_ws was called with {} arguments. It requires at least 2.",
                args.len()
            );
        }

        let array_len = args
            .iter()
            .filter_map(|x| match x {
                ColumnarValue::Array(array) => Some(array.len()),
                _ => None,
            })
            .next();

        // Scalar
        if array_len.is_none() {
            let ColumnarValue::Scalar(scalar) = &args[0] else {
                // loop above checks for all args being scalar
                unreachable!()
            };
            let sep = match scalar.try_as_str() {
                Some(Some(s)) => s,
                Some(None) => {
                    // null literal string
                    return Ok(ColumnarValue::Scalar(ScalarValue::Utf8(None)));
                }
                None => return internal_err!("Expected string literal, got {scalar:?}"),
            };

            let mut result = String::new();
            // iterator over Option<str>
            let iter = &mut args[1..].iter().map(|arg| {
                let ColumnarValue::Scalar(scalar) = arg else {
                    // loop above checks for all args being scalar
                    unreachable!()
                };
                scalar.try_as_str()
            });

            // append first non null arg
            for scalar in iter.by_ref() {
                match scalar {
                    Some(Some(s)) => {
                        result.push_str(s);
                        break;
                    }
                    Some(None) => {} // null literal string
                    None => {
                        return internal_err!("Expected string literal, got {scalar:?}")
                    }
                }
            }

            // handle subsequent non null args
            for scalar in iter.by_ref() {
                match scalar {
                    Some(Some(s)) => {
                        result.push_str(sep);
                        result.push_str(s);
                    }
                    Some(None) => {} // null literal string
                    None => {
                        return internal_err!("Expected string literal, got {scalar:?}")
                    }
                }
            }

            return Ok(ColumnarValue::Scalar(ScalarValue::Utf8(Some(result))));
        }

        // Array
        let len = array_len.unwrap();
        let mut data_size = 0;

        // parse sep
        let sep = match &args[0] {
            ColumnarValue::Scalar(ScalarValue::Utf8(Some(s))) => {
                data_size += s.len() * len * (args.len() - 2); // estimate
                ColumnarValueRef::Scalar(s.as_bytes())
            }
            ColumnarValue::Scalar(ScalarValue::Utf8(None)) => {
                return Ok(ColumnarValue::Array(Arc::new(StringArray::new_null(len))));
            }
            ColumnarValue::Array(array) => {
                let string_array = as_string_array(array)?;
                data_size += string_array.values().len() * (args.len() - 2); // estimate
                if array.is_nullable() {
                    ColumnarValueRef::NullableArray(string_array)
                } else {
                    ColumnarValueRef::NonNullableArray(string_array)
                }
            }
            _ => unreachable!("concat ws"),
        };

        let mut columns = Vec::with_capacity(args.len() - 1);
        for arg in &args[1..] {
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

                            data_size += string_array.data_buffers().iter().map(|buf| buf.len()).sum::<usize>();
                            let column = if array.is_nullable() {
                                ColumnarValueRef::NullableStringViewArray(string_array)
                            } else {
                                ColumnarValueRef::NonNullableStringViewArray(string_array)
                            };
                            columns.push(column);
                        },
                        other => {
                            return plan_err!("Input was {other} which is not a supported datatype for concat_ws function.")
                        }
                    };
                }
                _ => unreachable!(),
            }
        }

        let mut builder = StringArrayBuilder::with_capacity(len, data_size);
        for i in 0..len {
            if !sep.is_valid(i) {
                builder.append_offset();
                continue;
            }

            let mut iter = columns.iter();
            for column in iter.by_ref() {
                if column.is_valid(i) {
                    builder.write::<false>(column, i);
                    break;
                }
            }

            for column in iter {
                if column.is_valid(i) {
                    builder.write::<false>(&sep, i);
                    builder.write::<false>(column, i);
                }
            }

            builder.append_offset();
        }

        Ok(ColumnarValue::Array(Arc::new(builder.finish(sep.nulls()))))
    }

    /// Simply the `concat_ws` function by
    /// 1. folding to `null` if the delimiter is null
    /// 2. filtering out `null` arguments
    /// 3. using `concat` to replace `concat_ws` if the delimiter is an empty string
    /// 4. concatenating contiguous literals if the delimiter is a literal.
    fn simplify(
        &self,
        args: Vec<Expr>,
        _info: &dyn SimplifyInfo,
    ) -> Result<ExprSimplifyResult> {
        match &args[..] {
            [delimiter, vals @ ..] => simplify_concat_ws(delimiter, vals),
            _ => Ok(ExprSimplifyResult::Original(args)),
        }
    }

    fn documentation(&self) -> Option<&Documentation> {
        self.doc()
    }
}

fn simplify_concat_ws(delimiter: &Expr, args: &[Expr]) -> Result<ExprSimplifyResult> {
    match delimiter {
        Expr::Literal(
            ScalarValue::Utf8(delimiter)
            | ScalarValue::LargeUtf8(delimiter)
            | ScalarValue::Utf8View(delimiter),
            _,
        ) => {
            match delimiter {
                // when the delimiter is an empty string,
                // we can use `concat` to replace `concat_ws`
                Some(delimiter) if delimiter.is_empty() => {
                    match simplify_concat(args.to_vec())? {
                        ExprSimplifyResult::Original(_) => {
                            Ok(ExprSimplifyResult::Simplified(Expr::ScalarFunction(
                                ScalarFunction {
                                    func: concat(),
                                    args: args.to_vec(),
                                },
                            )))
                        }
                        expr => Ok(expr),
                    }
                }
                Some(delimiter) => {
                    let mut new_args = Vec::with_capacity(args.len());
                    new_args.push(lit(delimiter));
                    let mut contiguous_scalar = None;
                    for arg in args {
                        match arg {
                            // filter out null args
                            Expr::Literal(ScalarValue::Utf8(None) | ScalarValue::LargeUtf8(None) | ScalarValue::Utf8View(None), _) => {}
                            Expr::Literal(ScalarValue::Utf8(Some(v)) | ScalarValue::LargeUtf8(Some(v)) | ScalarValue::Utf8View(Some(v)), _) => {
                                match contiguous_scalar {
                                    None => contiguous_scalar = Some(v.to_string()),
                                    Some(mut pre) => {
                                        pre += delimiter;
                                        pre += v;
                                        contiguous_scalar = Some(pre)
                                    }
                                }
                            }
                            Expr::Literal(s, _) => return internal_err!("The scalar {s} should be casted to string type during the type coercion."),
                            // If the arg is not a literal, we should first push the current `contiguous_scalar`
                            // to the `new_args` and reset it to None.
                            // Then pushing this arg to the `new_args`.
                            arg => {
                                if let Some(val) = contiguous_scalar {
                                    new_args.push(lit(val));
                                }
                                new_args.push(arg.clone());
                                contiguous_scalar = None;
                            }
                        }
                    }
                    if let Some(val) = contiguous_scalar {
                        new_args.push(lit(val));
                    }

                    Ok(ExprSimplifyResult::Simplified(Expr::ScalarFunction(
                        ScalarFunction {
                            func: concat_ws(),
                            args: new_args,
                        },
                    )))
                }
                // if the delimiter is null, then the value of the whole expression is null.
                None => Ok(ExprSimplifyResult::Simplified(Expr::Literal(
                    ScalarValue::Utf8(None),
                    None,
                ))),
            }
        }
        Expr::Literal(d, _) => internal_err!(
            "The scalar {d} should be casted to string type during the type coercion."
        ),
        _ => {
            let mut args = args
                .iter()
                .filter(|&x| !is_null(x))
                .cloned()
                .collect::<Vec<Expr>>();
            args.insert(0, delimiter.clone());
            Ok(ExprSimplifyResult::Original(args))
        }
    }
}

fn is_null(expr: &Expr) -> bool {
    match expr {
        Expr::Literal(v, _) => v.is_null(),
        _ => false,
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use crate::string::concat_ws::ConcatWsFunc;
    use arrow::array::{Array, ArrayRef, StringArray};
    use arrow::datatypes::DataType::Utf8;
    use arrow::datatypes::Field;
    use datafusion_common::config::ConfigOptions;
    use datafusion_common::Result;
    use datafusion_common::ScalarValue;
    use datafusion_expr::{ColumnarValue, ScalarFunctionArgs, ScalarUDFImpl};

    use crate::utils::test::test_function;

    #[test]
    fn test_functions() -> Result<()> {
        test_function!(
            ConcatWsFunc::new(),
            vec![
                ColumnarValue::Scalar(ScalarValue::from("|")),
                ColumnarValue::Scalar(ScalarValue::from("aa")),
                ColumnarValue::Scalar(ScalarValue::from("bb")),
                ColumnarValue::Scalar(ScalarValue::from("cc")),
            ],
            Ok(Some("aa|bb|cc")),
            &str,
            Utf8,
            StringArray
        );
        test_function!(
            ConcatWsFunc::new(),
            vec![
                ColumnarValue::Scalar(ScalarValue::from("|")),
                ColumnarValue::Scalar(ScalarValue::Utf8(None)),
            ],
            Ok(Some("")),
            &str,
            Utf8,
            StringArray
        );
        test_function!(
            ConcatWsFunc::new(),
            vec![
                ColumnarValue::Scalar(ScalarValue::Utf8(None)),
                ColumnarValue::Scalar(ScalarValue::from("aa")),
                ColumnarValue::Scalar(ScalarValue::from("bb")),
                ColumnarValue::Scalar(ScalarValue::from("cc")),
            ],
            Ok(None),
            &str,
            Utf8,
            StringArray
        );
        test_function!(
            ConcatWsFunc::new(),
            vec![
                ColumnarValue::Scalar(ScalarValue::from("|")),
                ColumnarValue::Scalar(ScalarValue::from("aa")),
                ColumnarValue::Scalar(ScalarValue::Utf8(None)),
                ColumnarValue::Scalar(ScalarValue::from("cc")),
            ],
            Ok(Some("aa|cc")),
            &str,
            Utf8,
            StringArray
        );

        Ok(())
    }

    #[test]
    fn concat_ws() -> Result<()> {
        // sep is scalar
        let c0 = ColumnarValue::Scalar(ScalarValue::Utf8(Some(",".to_string())));
        let c1 =
            ColumnarValue::Array(Arc::new(StringArray::from(vec!["foo", "bar", "baz"])));
        let c2 = ColumnarValue::Array(Arc::new(StringArray::from(vec![
            Some("x"),
            None,
            Some("z"),
        ])));

        let arg_fields = vec![
            Field::new("a", Utf8, true).into(),
            Field::new("a", Utf8, true).into(),
            Field::new("a", Utf8, true).into(),
        ];
        let args = ScalarFunctionArgs {
            args: vec![c0, c1, c2],
            arg_fields,
            number_rows: 3,
            return_field: Field::new("f", Utf8, true).into(),
            config_options: Arc::new(ConfigOptions::default()),
        };

        let result = ConcatWsFunc::new().invoke_with_args(args)?;
        let expected =
            Arc::new(StringArray::from(vec!["foo,x", "bar", "baz,z"])) as ArrayRef;
        match &result {
            ColumnarValue::Array(array) => {
                assert_eq!(&expected, array);
            }
            _ => panic!(),
        }

        // sep is nullable array
        let c0 = ColumnarValue::Array(Arc::new(StringArray::from(vec![
            Some(","),
            None,
            Some("+"),
        ])));
        let c1 =
            ColumnarValue::Array(Arc::new(StringArray::from(vec!["foo", "bar", "baz"])));
        let c2 = ColumnarValue::Array(Arc::new(StringArray::from(vec![
            Some("x"),
            Some("y"),
            Some("z"),
        ])));

        let arg_fields = vec![
            Field::new("a", Utf8, true).into(),
            Field::new("a", Utf8, true).into(),
            Field::new("a", Utf8, true).into(),
        ];
        let args = ScalarFunctionArgs {
            args: vec![c0, c1, c2],
            arg_fields,
            number_rows: 3,
            return_field: Field::new("f", Utf8, true).into(),
            config_options: Arc::new(ConfigOptions::default()),
        };

        let result = ConcatWsFunc::new().invoke_with_args(args)?;
        let expected =
            Arc::new(StringArray::from(vec![Some("foo,x"), None, Some("baz+z")]))
                as ArrayRef;
        match &result {
            ColumnarValue::Array(array) => {
                assert_eq!(&expected, array);
            }
            _ => panic!(),
        }

        Ok(())
    }
}
