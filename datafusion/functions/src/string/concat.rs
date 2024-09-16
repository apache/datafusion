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
use std::any::Any;
use std::sync::Arc;

use datafusion_common::cast::{as_string_array, as_string_view_array};
use datafusion_common::{internal_err, plan_err, Result, ScalarValue};
use datafusion_expr::expr::ScalarFunction;
use datafusion_expr::simplify::{ExprSimplifyResult, SimplifyInfo};
use datafusion_expr::{lit, ColumnarValue, Expr, Volatility};
use datafusion_expr::{ScalarUDFImpl, Signature};

use crate::string::common::*;
use crate::string::concat;

#[derive(Debug)]
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
        use DataType::*;
        Self {
            signature: Signature::variadic(
                vec![Utf8, Utf8View, LargeUtf8],
                Volatility::Immutable,
            ),
        }
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

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        use DataType::*;
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
    fn invoke(&self, args: &[ColumnarValue]) -> Result<ColumnarValue> {
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
                if let ColumnarValue::Scalar(ScalarValue::Utf8(Some(v))) = arg {
                    result.push_str(v);
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

        for arg in args {
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
                _ => unreachable!(),
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
            _ => unreachable!(),
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
}

pub fn simplify_concat(args: Vec<Expr>) -> Result<ExprSimplifyResult> {
    let mut new_args = Vec::with_capacity(args.len());
    let mut contiguous_scalar = "".to_string();

    for arg in args.clone() {
        match arg {
            // filter out `null` args
            Expr::Literal(ScalarValue::Utf8(None) | ScalarValue::LargeUtf8(None) | ScalarValue::Utf8View(None)) => {}
            // All literals have been converted to Utf8 or LargeUtf8 in type_coercion.
            // Concatenate it with the `contiguous_scalar`.
            Expr::Literal(
                ScalarValue::Utf8(Some(v)) | ScalarValue::LargeUtf8(Some(v)) | ScalarValue::Utf8View(Some(v)),
            ) => contiguous_scalar += &v,
            Expr::Literal(x) => {
                return internal_err!(
                    "The scalar {x} should be casted to string type during the type coercion."
                )
            }
            // If the arg is not a literal, we should first push the current `contiguous_scalar`
            // to the `new_args` (if it is not empty) and reset it to empty string.
            // Then pushing this arg to the `new_args`.
            arg => {
                if !contiguous_scalar.is_empty() {
                    new_args.push(lit(contiguous_scalar));
                    contiguous_scalar = "".to_string();
                }
                new_args.push(arg);
            }
        }
    }

    if !contiguous_scalar.is_empty() {
        new_args.push(lit(contiguous_scalar));
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
    use DataType::*;

    #[test]
    fn test_functions() -> Result<()> {
        test_function!(
            ConcatFunc::new(),
            &[
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
            &[
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
            &[ColumnarValue::Scalar(ScalarValue::Utf8(None))],
            Ok(Some("")),
            &str,
            Utf8,
            StringArray
        );
        test_function!(
            ConcatFunc::new(),
            &[
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
            &[
                ColumnarValue::Scalar(ScalarValue::from("aa")),
                ColumnarValue::Scalar(ScalarValue::LargeUtf8(None)),
                ColumnarValue::Scalar(ScalarValue::from("cc")),
            ],
            Ok(Some("aacc")),
            &str,
            LargeUtf8,
            LargeStringArray
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
        let args = &[c0, c1, c2];

        let result = ConcatFunc::new().invoke(args)?;
        let expected =
            Arc::new(StringArray::from(vec!["foo,x", "bar,", "baz,z"])) as ArrayRef;
        match &result {
            ColumnarValue::Array(array) => {
                assert_eq!(&expected, array);
            }
            _ => panic!(),
        }
        Ok(())
    }
}
