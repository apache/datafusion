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

use crate::binaries::{
    ConcatBinaryBuilder, ConcatBinaryViewBuilder, ConcatLargeBinaryBuilder,
};
use crate::string::concat;
use crate::strings::{
    ColumnarValueRef, ConcatBuilder, ConcatLargeStringBuilder, ConcatStringBuilder,
    ConcatStringViewBuilder, widest_binary_type, widest_string_type,
};
use arrow::array::Array;
use arrow::datatypes::DataType;
use datafusion_common::{
    Result, ScalarValue, exec_datafusion_err, internal_err, plan_err,
};
use datafusion_expr::expr::ScalarFunction;
use datafusion_expr::simplify::{ExprSimplifyResult, SimplifyContext};
use datafusion_expr::sort_properties::ExprProperties;
use datafusion_expr::{ColumnarValue, Documentation, Expr, Volatility, lit};
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
            // Use `Signature::UserDefined` to allow different argument types.
            // `Variadic` requires every argument to be coerced to the same string type,
            // so the UDF cannot distinguish between binary and string inputs.
            signature: Signature::user_defined(Volatility::Immutable),
        }
    }
}

// Supports string + string concatenation, binary + binary concatenation,
// and mixed string + binary concatenation (binary is coerced to the widest
// string type).
impl ScalarUDFImpl for ConcatFunc {
    fn name(&self) -> &str {
        "concat"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    /// Coerce all arguments to the widest type within the binary / string family
    fn coerce_types(&self, arg_types: &[DataType]) -> Result<Vec<DataType>> {
        if arg_types.is_empty() {
            plan_err!("concat does not support zero arguments")
        } else {
            coerce_arg_types(arg_types)
        }
    }

    /// mixed inputs, prefer Utf8View; prefer LargeUtf8 over Utf8 to avoid
    /// potential overflow on LargeUtf8 input.
    /// For binaries, use the similar hierarchy
    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        Ok(deduce_return_type(arg_types))
    }

    /// Concatenates the text representations of all the arguments. NULL arguments are ignored.
    /// concat('abcde', 2, NULL, 22) = 'abcde222'
    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let return_datatype = args.return_type().clone();
        let ScalarFunctionArgs { args, .. } = args;

        let array_len = args.iter().find_map(|x| match x {
            ColumnarValue::Array(array) => Some(array.len()),
            _ => None,
        });

        // Scalar
        if array_len.is_none() {
            let mut values: Vec<&[u8]> = Vec::with_capacity(args.len());
            for arg in &args {
                let ColumnarValue::Scalar(scalar) = arg else {
                    return internal_err!("concat expected scalar value, got {arg:?}");
                };
                if let ScalarValue::Binary(Some(value)) = scalar {
                    values.push(value);
                } else if let ScalarValue::LargeBinary(Some(value)) = scalar {
                    values.push(value);
                } else if let ScalarValue::BinaryView(Some(value)) = scalar {
                    values.push(value);
                } else if scalar.is_null() {
                    // null binary scalar: skip (consistent with null string behaviour)
                } else {
                    // String case
                    match scalar.try_as_str() {
                        Some(Some(v)) => values.push(v.as_bytes()),
                        Some(None) => {} // null literal
                        None => plan_err!(
                            "Concat function does not support scalar type {}",
                            scalar
                        )?,
                    }
                }
            }
            let concat_bytes = values.concat();

            return match return_datatype {
                DataType::Utf8View => {
                    let result = std::str::from_utf8(&concat_bytes)
                        .map_err(|_| {
                            exec_datafusion_err!("invalid UTF-8 in binary literal")
                        })?
                        .to_string();
                    Ok(ColumnarValue::Scalar(ScalarValue::Utf8View(Some(result))))
                }
                DataType::Utf8 => {
                    let result = std::str::from_utf8(&concat_bytes)
                        .map_err(|_| {
                            exec_datafusion_err!("invalid UTF-8 in binary literal")
                        })?
                        .to_string();
                    Ok(ColumnarValue::Scalar(ScalarValue::Utf8(Some(result))))
                }
                DataType::LargeUtf8 => {
                    let result = std::str::from_utf8(&concat_bytes)
                        .map_err(|_| {
                            exec_datafusion_err!("invalid UTF-8 in binary literal")
                        })?
                        .to_string();
                    Ok(ColumnarValue::Scalar(ScalarValue::LargeUtf8(Some(result))))
                }
                DataType::Binary => Ok(ColumnarValue::Scalar(ScalarValue::Binary(Some(
                    concat_bytes,
                )))),
                // Serves LargeBinary and FixedSizeBinary inputs
                DataType::LargeBinary => Ok(ColumnarValue::Scalar(
                    ScalarValue::LargeBinary(Some(concat_bytes)),
                )),
                DataType::BinaryView => Ok(ColumnarValue::Scalar(
                    ScalarValue::BinaryView(Some(concat_bytes)),
                )),
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
            if let Some(column) =
                ColumnarValueRef::from_columnar_value(arg, &mut data_size, len, 1, false)?
            {
                columns.push(column);
            }
        }

        match return_datatype {
            DataType::Utf8 => build_concat(
                ConcatStringBuilder::with_capacity(len, data_size),
                &columns,
                len,
            ),
            DataType::Utf8View => build_concat(
                ConcatStringViewBuilder::with_capacity(len, data_size),
                &columns,
                len,
            ),
            DataType::LargeUtf8 => build_concat(
                ConcatLargeStringBuilder::with_capacity(len, data_size),
                &columns,
                len,
            ),
            DataType::Binary => build_concat(
                ConcatBinaryBuilder::with_capacity(len, data_size),
                &columns,
                len,
            ),
            // Serves LargeBinary and FixedSizeBinary inputs
            DataType::LargeBinary => build_concat(
                ConcatLargeBinaryBuilder::with_capacity(len, data_size),
                &columns,
                len,
            ),
            DataType::BinaryView => build_concat(
                ConcatBinaryViewBuilder::with_capacity(len, data_size),
                &columns,
                len,
            ),
            _ => unreachable!("concat"),
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
        _info: &SimplifyContext,
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

pub(crate) fn deduce_return_type(arg_types: &[DataType]) -> DataType {
    use DataType::*;
    if arg_types.contains(&BinaryView) {
        BinaryView
    } else if arg_types.contains(&LargeBinary) {
        // Serves LargeBinary and FixedSizeBinary inputs
        LargeBinary
    } else if arg_types.contains(&Binary) {
        Binary
    } else if arg_types.contains(&Utf8View) {
        Utf8View
    } else if arg_types.contains(&LargeUtf8) {
        LargeUtf8
    } else {
        Utf8
    }
}

/// Coerce all arguments to the widest type within the binary / string family
pub(crate) fn coerce_arg_types(arg_types: &[DataType]) -> Result<Vec<DataType>> {
    let has_binary = arg_types.iter().any(|dt| dt.is_binary());
    let has_string = arg_types.iter().any(|dt| dt.is_string());
    if has_binary && has_string {
        // Mixed string+binary: coerce everything to the widest string type
        // This behaviour is seen for Spark, DuckDB
        Ok(vec![widest_string_type(arg_types); arg_types.len()])
    } else if has_binary {
        // Pure binary+binary concatenation: coerce to the widest binary type
        Ok(vec![widest_binary_type(arg_types); arg_types.len()])
    } else {
        // Pure string+string concatenation: coerce to the widest string type
        Ok(vec![widest_string_type(arg_types); arg_types.len()])
    }
}

/// Build a `concats` output array using a generic [`ConcatBuilder`].
fn build_concat<B: ConcatBuilder>(
    mut builder: B,
    columns: &[ColumnarValueRef],
    len: usize,
) -> Result<ColumnarValue> {
    for i in 0..len {
        for column in columns {
            builder.write::<true>(column, i)?;
        }
        builder.append_offset()?;
    }

    let array = builder.finish(None)?;
    Ok(ColumnarValue::Array(array))
}

pub(crate) fn simplify_concat(args: Vec<Expr>) -> Result<ExprSimplifyResult> {
    // Skip simplification when binary literals are present, because it
    // handles only strings
    for arg in &args {
        match arg {
            Expr::Literal(dt, _) if dt.data_type().is_binary() => {
                return Ok(ExprSimplifyResult::Original(args));
            }
            _ => {}
        }
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

    for arg in args.clone() {
        match arg {
            Expr::Literal(ScalarValue::Utf8(None), _) => {}
            Expr::Literal(ScalarValue::LargeUtf8(None), _) => {}
            Expr::Literal(ScalarValue::Utf8View(None), _) => {}

            // filter out `null` args
            // All literals have been converted to Utf8 or LargeUtf8 in type_coercion.
            // Concatenate it with the `contiguous_scalar`.
            Expr::Literal(ScalarValue::Utf8(Some(v)), _) => {
                contiguous_scalar += &v;
            }
            Expr::Literal(ScalarValue::LargeUtf8(Some(v)), _) => {
                contiguous_scalar += &v;
            }
            Expr::Literal(ScalarValue::Utf8View(Some(v)), _) => {
                contiguous_scalar += &v;
            }

            Expr::Literal(x, _) => {
                return internal_err!(
                    "The scalar {x} should be casted to string type during the type coercion."
                );
            }
            // If the arg is not a literal, we should first push the current `contiguous_scalar`
            // to the `new_args` (if it is not empty) and reset it to empty string.
            // Then pushing this arg to the `new_args`.
            arg => {
                if !contiguous_scalar.is_empty() {
                    match return_type {
                        DataType::Utf8 => new_args.push(lit(contiguous_scalar)),
                        DataType::LargeUtf8 => new_args
                            .push(lit(ScalarValue::LargeUtf8(Some(contiguous_scalar)))),
                        DataType::Utf8View => new_args
                            .push(lit(ScalarValue::Utf8View(Some(contiguous_scalar)))),
                        _ => unreachable!(),
                    }
                    contiguous_scalar = "".to_string();
                }
                new_args.push(arg);
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
            _ => unreachable!(),
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
    use DataType::*;
    use arrow::array::{
        ArrayRef, BinaryArray, BinaryViewArray, LargeBinaryArray, StringArray,
    };
    use arrow::array::{LargeStringArray, StringViewArray};
    use arrow::datatypes::Field;
    use datafusion_common::config::ConfigOptions;
    use std::sync::Arc;

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
    fn test_scalar_binary() -> Result<()> {
        test_function!(
            ConcatFunc::new(),
            vec![
                ColumnarValue::Scalar(ScalarValue::Binary(Some(
                    "Café".as_bytes().into()
                ))),
                ColumnarValue::Scalar(ScalarValue::Binary(Some("cc".as_bytes().into()))),
            ],
            Ok(Some("Cafécc".as_bytes())),
            &[u8],
            Binary,
            BinaryArray
        );
        test_function!(
            ConcatFunc::new(),
            vec![
                ColumnarValue::Scalar(ScalarValue::Binary(Some(
                    "Café".as_bytes().into()
                ))),
                ColumnarValue::Scalar(ScalarValue::LargeBinary(Some(
                    "cc".as_bytes().into()
                ))),
            ],
            Ok(Some("Cafécc".as_bytes())),
            &[u8],
            LargeBinary,
            LargeBinaryArray
        );
        test_function!(
            ConcatFunc::new(),
            vec![
                ColumnarValue::Scalar(ScalarValue::Binary(Some(
                    "Café".as_bytes().into()
                ))),
                ColumnarValue::Scalar(ScalarValue::BinaryView(Some(
                    "cc".as_bytes().into()
                ))),
            ],
            Ok(Some("Cafécc".as_bytes())),
            &[u8],
            BinaryView,
            BinaryViewArray
        );
        test_function!(
            ConcatFunc::new(),
            vec![
                ColumnarValue::Scalar(ScalarValue::BinaryView(Some(
                    "Café".as_bytes().into()
                ))),
                ColumnarValue::Scalar(ScalarValue::BinaryView(Some(
                    "cc".as_bytes().into()
                ))),
            ],
            Ok(Some("Cafécc".as_bytes())),
            &[u8],
            BinaryView,
            BinaryViewArray
        );
        // Skip one Binary(None)
        test_function!(
            ConcatFunc::new(),
            vec![
                ColumnarValue::Scalar(ScalarValue::Binary(None)),
                ColumnarValue::Scalar(ScalarValue::Binary(Some(b"hello".to_vec()))),
            ],
            Ok(Some(b"hello".as_ref())),
            &[u8],
            Binary,
            BinaryArray
        );
        // Skip all Binary(None), producing an empty array
        test_function!(
            ConcatFunc::new(),
            vec![ColumnarValue::Scalar(ScalarValue::Binary(None))],
            Ok(Some(b"".as_ref())),
            &[u8],
            Binary,
            BinaryArray
        );
        Ok(())
    }

    #[test]
    fn test_array_string() -> Result<()> {
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
            return_field: Field::new("f", Utf8View, true).into(),
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
    fn test_array_binary() -> Result<()> {
        let c0 = ColumnarValue::Array(Arc::new(BinaryArray::from_vec(vec![
            b"foo", b"bar", b"baz",
        ])));
        let c1 = ColumnarValue::Scalar(ScalarValue::LargeBinary(Some(b",".to_vec())));
        let c2 = ColumnarValue::Array(Arc::new(BinaryArray::from_opt_vec(vec![
            Some(b"x"),
            None,
            Some(b"z"),
        ])));
        let c3 = ColumnarValue::Scalar(ScalarValue::BinaryView(Some(b",".to_vec())));
        let c4 = ColumnarValue::Array(Arc::new(BinaryViewArray::from_iter(vec![
            Some(b"a"),
            None,
            Some(b"b"),
        ])));
        let arg_fields = vec![
            Field::new("a", Binary, true),
            Field::new("a", LargeBinary, true),
            Field::new("a", Binary, true),
            Field::new("a", BinaryView, true),
            Field::new("a", BinaryView, true),
        ]
        .into_iter()
        .map(Arc::new)
        .collect::<Vec<_>>();

        let args = ScalarFunctionArgs {
            args: vec![c0, c1, c2, c3, c4],
            arg_fields,
            number_rows: 3,
            return_field: Field::new("f", BinaryView, true).into(),
            config_options: Arc::new(ConfigOptions::default()),
        };

        let result = ConcatFunc::new().invoke_with_args(args)?;
        let expected = Arc::new(BinaryViewArray::from_iter(vec![
            Some(b"foo,x,a".to_vec()),
            Some(b"bar,,".to_vec()),
            Some(b"baz,z,b".to_vec()),
        ])) as ArrayRef;
        match &result {
            ColumnarValue::Array(array) => {
                assert_eq!(&expected, array);
            }
            _ => panic!(),
        }
        Ok(())
    }
}
