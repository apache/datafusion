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
use std::sync::{Arc, OnceLock};

use arrow::datatypes::DataType;

use crate::string::concat::simplify_concat;
use crate::string::concat_ws;
use crate::strings::{ColumnarValueRef, StringArrayBuilder};
use datafusion_common::cast::{as_string_array, as_string_view_array};
use datafusion_common::{exec_err, internal_err, plan_err, Result, ScalarValue};
use datafusion_expr::expr::ScalarFunction;
use datafusion_expr::scalar_doc_sections::DOC_SECTION_STRING;
use datafusion_expr::simplify::{ExprSimplifyResult, SimplifyInfo};
use datafusion_expr::{lit, ColumnarValue, Documentation, Expr, Volatility};
use datafusion_expr::{ScalarUDFImpl, Signature};

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
    fn invoke(&self, args: &[ColumnarValue]) -> Result<ColumnarValue> {
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
            let sep = match &args[0] {
                ColumnarValue::Scalar(ScalarValue::Utf8(Some(s)))
                | ColumnarValue::Scalar(ScalarValue::Utf8View(Some(s)))
                | ColumnarValue::Scalar(ScalarValue::LargeUtf8(Some(s))) => s,
                ColumnarValue::Scalar(ScalarValue::Utf8(None))
                | ColumnarValue::Scalar(ScalarValue::Utf8View(None))
                | ColumnarValue::Scalar(ScalarValue::LargeUtf8(None)) => {
                    return Ok(ColumnarValue::Scalar(ScalarValue::Utf8(None)));
                }
                _ => unreachable!(),
            };

            let mut result = String::new();
            let iter = &mut args[1..].iter();

            for arg in iter.by_ref() {
                match arg {
                    ColumnarValue::Scalar(ScalarValue::Utf8(Some(s)))
                    | ColumnarValue::Scalar(ScalarValue::Utf8View(Some(s)))
                    | ColumnarValue::Scalar(ScalarValue::LargeUtf8(Some(s))) => {
                        result.push_str(s);
                        break;
                    }
                    ColumnarValue::Scalar(ScalarValue::Utf8(None))
                    | ColumnarValue::Scalar(ScalarValue::Utf8View(None))
                    | ColumnarValue::Scalar(ScalarValue::LargeUtf8(None)) => {}
                    _ => unreachable!(),
                }
            }

            for arg in iter.by_ref() {
                match arg {
                    ColumnarValue::Scalar(ScalarValue::Utf8(Some(s)))
                    | ColumnarValue::Scalar(ScalarValue::Utf8View(Some(s)))
                    | ColumnarValue::Scalar(ScalarValue::LargeUtf8(Some(s))) => {
                        result.push_str(sep);
                        result.push_str(s);
                    }
                    ColumnarValue::Scalar(ScalarValue::Utf8(None))
                    | ColumnarValue::Scalar(ScalarValue::Utf8View(None))
                    | ColumnarValue::Scalar(ScalarValue::LargeUtf8(None)) => {}
                    _ => unreachable!(),
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
            _ => unreachable!(),
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
        Some(get_concat_ws_doc())
    }
}

static DOCUMENTATION: OnceLock<Documentation> = OnceLock::new();

fn get_concat_ws_doc() -> &'static Documentation {
    DOCUMENTATION.get_or_init(|| {
        Documentation::builder()
            .with_doc_section(DOC_SECTION_STRING)
            .with_description(
                "Concatenates multiple strings together with a specified separator.",
            )
            .with_syntax_example("concat_ws(separator, str[, ..., str_n])")
            .with_sql_example(
                r#"```sql
> select concat_ws('_', 'data', 'fusion');
+--------------------------------------------------+
| concat_ws(Utf8("_"),Utf8("data"),Utf8("fusion")) |
+--------------------------------------------------+
| data_fusion                                      |
+--------------------------------------------------+
```"#,
            )
            .with_argument(
                "separator",
                "Separator to insert between concatenated strings.",
            )
            .with_standard_argument("str", Some("String"))
            .with_argument("str_n", "Subsequent string expressions to concatenate.")
            .with_related_udf("concat")
            .build()
            .unwrap()
    })
}

fn simplify_concat_ws(delimiter: &Expr, args: &[Expr]) -> Result<ExprSimplifyResult> {
    match delimiter {
        Expr::Literal(
            ScalarValue::Utf8(delimiter)
            | ScalarValue::LargeUtf8(delimiter)
            | ScalarValue::Utf8View(delimiter),
        ) => {
            match delimiter {
                // when the delimiter is an empty string,
                // we can use `concat` to replace `concat_ws`
                Some(delimiter) if delimiter.is_empty() => simplify_concat(args.to_vec()),
                Some(delimiter) => {
                    let mut new_args = Vec::with_capacity(args.len());
                    new_args.push(lit(delimiter));
                    let mut contiguous_scalar = None;
                    for arg in args {
                        match arg {
                            // filter out null args
                            Expr::Literal(ScalarValue::Utf8(None) | ScalarValue::LargeUtf8(None) | ScalarValue::Utf8View(None)) => {}
                            Expr::Literal(ScalarValue::Utf8(Some(v)) | ScalarValue::LargeUtf8(Some(v)) | ScalarValue::Utf8View(Some(v))) => {
                                match contiguous_scalar {
                                    None => contiguous_scalar = Some(v.to_string()),
                                    Some(mut pre) => {
                                        pre += delimiter;
                                        pre += v;
                                        contiguous_scalar = Some(pre)
                                    }
                                }
                            }
                            Expr::Literal(s) => return internal_err!("The scalar {s} should be casted to string type during the type coercion."),
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
                ))),
            }
        }
        Expr::Literal(d) => internal_err!(
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
        Expr::Literal(v) => v.is_null(),
        _ => false,
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow::array::{Array, ArrayRef, StringArray};
    use arrow::datatypes::DataType::Utf8;

    use crate::string::concat_ws::ConcatWsFunc;
    use datafusion_common::Result;
    use datafusion_common::ScalarValue;
    use datafusion_expr::{ColumnarValue, ScalarUDFImpl};

    use crate::utils::test::test_function;

    #[test]
    fn test_functions() -> Result<()> {
        test_function!(
            ConcatWsFunc::new(),
            &[
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
            &[
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
            &[
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
            &[
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
        let args = &[c0, c1, c2];

        let result = ConcatWsFunc::new().invoke(args)?;
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
        let args = &[c0, c1, c2];

        let result = ConcatWsFunc::new().invoke(args)?;
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
