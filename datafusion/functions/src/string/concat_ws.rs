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

use arrow::array::Array;
use arrow::datatypes::DataType;

use crate::binaries::{
    ConcatBinaryBuilder, ConcatBinaryViewBuilder, ConcatLargeBinaryBuilder,
};
use crate::string::concat;
use crate::string::concat::{coerce_arg_types, deduce_return_type, simplify_concat};
use crate::string::concat_ws;
use crate::strings::{
    ColumnarValueRef, ConcatBuilder, ConcatLargeStringBuilder, ConcatStringBuilder,
    ConcatStringViewBuilder,
};
use datafusion_common::{Result, ScalarValue, exec_err, internal_err, plan_err};
use datafusion_expr::expr::ScalarFunction;
use datafusion_expr::simplify::{ExprSimplifyResult, SimplifyContext};
use datafusion_expr::{ColumnarValue, Documentation, Expr, Volatility, lit};
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
#[derive(Debug, PartialEq, Eq, Hash)]
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
        Self {
            // Use `Signature::UserDefined` to allow different argument types.
            // `Variadic` requires every argument to be coerced to the same string type,
            // so the UDF cannot distinguish between binary and string inputs.
            signature: Signature::user_defined(Volatility::Immutable),
        }
    }
}

impl ScalarUDFImpl for ConcatWsFunc {
    fn name(&self) -> &str {
        "concat_ws"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    /// Coerce all arguments to the widest type within the binary / string family
    fn coerce_types(&self, arg_types: &[DataType]) -> Result<Vec<DataType>> {
        if arg_types.len() < 2 {
            plan_err!(
                "concat_ws expects at least 2 arguments, got {}",
                arg_types.len()
            )
        } else {
            coerce_arg_types(arg_types)
        }
    }

    /// Match the return type to the input types. Delegates to `concat` implementation.
    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        Ok(deduce_return_type(arg_types))
    }

    /// Concatenates all but the first argument, with separators. The first
    /// argument is used as the separator string, and should not be NULL. Other
    /// NULL arguments are ignored.
    /// concat_ws(',', 'abcde', 2, NULL, 22) = 'abcde,2,22'
    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let return_datatype = args.return_type().clone();
        let ScalarFunctionArgs { args, .. } = args;

        if args.len() < 2 {
            return exec_err!(
                "concat_ws was called with {} arguments. It requires at least 2.",
                args.len()
            );
        }

        let arg_types: Vec<DataType> = args.iter().map(|c| c.data_type()).collect();

        let with_binary = arg_types.iter().any(|dt| dt.is_binary());

        let array_len = args.iter().find_map(|x| match x {
            ColumnarValue::Array(array) => Some(array.len()),
            _ => None,
        });

        // Scalar
        if array_len.is_none() {
            let ColumnarValue::Scalar(scalar) = &args[0] else {
                unreachable!()
            };

            return if with_binary {
                // Binary scalar path
                let sep_bytes: &[u8] = match scalar {
                    ScalarValue::Binary(Some(v))
                    | ScalarValue::LargeBinary(Some(v))
                    | ScalarValue::BinaryView(Some(v)) => v.as_slice(),
                    ScalarValue::FixedSizeBinary(_, Some(v)) => v.as_slice(),
                    scalar if scalar.is_null() => {
                        return Ok(null_scalar(&return_datatype));
                    }
                    other => {
                        return internal_err!("Expected binary separator, got {other:?}");
                    }
                };

                let mut values: Vec<&[u8]> = Vec::with_capacity(args.len() - 1);
                for arg in &args[1..] {
                    let ColumnarValue::Scalar(s) = arg else {
                        unreachable!()
                    };
                    match s {
                        ScalarValue::Binary(Some(v))
                        | ScalarValue::LargeBinary(Some(v))
                        | ScalarValue::BinaryView(Some(v)) => values.push(v.as_slice()),
                        ScalarValue::FixedSizeBinary(_, Some(v)) => {
                            values.push(v.as_slice())
                        }
                        // skip null
                        scalar if scalar.is_null() => {}
                        other => {
                            return internal_err!("Expected binary value, got {other:?}");
                        }
                    }
                }
                let result = values.join(sep_bytes);

                match return_datatype {
                    DataType::Binary => {
                        Ok(ColumnarValue::Scalar(ScalarValue::Binary(Some(result))))
                    }
                    DataType::LargeBinary => Ok(ColumnarValue::Scalar(
                        ScalarValue::LargeBinary(Some(result)),
                    )),
                    DataType::BinaryView => {
                        Ok(ColumnarValue::Scalar(ScalarValue::BinaryView(Some(result))))
                    }
                    other => {
                        plan_err!("concat_ws does not support return type {other}")
                    }
                }
            } else {
                // String scalar path
                let sep = match scalar.try_as_str() {
                    Some(Some(s)) => s,
                    Some(None) => {
                        return Ok(null_scalar(&return_datatype));
                    }
                    None => {
                        return internal_err!("Expected string literal, got {scalar:?}");
                    }
                };

                let mut values = Vec::with_capacity(args.len() - 1);
                for arg in &args[1..] {
                    let ColumnarValue::Scalar(scalar) = arg else {
                        unreachable!()
                    };

                    match scalar.try_as_str() {
                        Some(Some(v)) => values.push(v),
                        Some(None) => {} // null literal string
                        None => {
                            return internal_err!(
                                "Expected string literal, got {scalar:?}"
                            );
                        }
                    }
                }
                let result = values.join(sep);

                match return_datatype {
                    DataType::Utf8View => {
                        Ok(ColumnarValue::Scalar(ScalarValue::Utf8View(Some(result))))
                    }
                    DataType::LargeUtf8 => {
                        Ok(ColumnarValue::Scalar(ScalarValue::LargeUtf8(Some(result))))
                    }
                    DataType::Utf8 => {
                        Ok(ColumnarValue::Scalar(ScalarValue::Utf8(Some(result))))
                    }
                    other => {
                        plan_err!("concat_ws does not support return type {other}")
                    }
                }
            };
        }

        // Array
        let len = array_len.unwrap();
        let mut data_size = 0;

        let sep_column = &args[0];

        // A null scalar separator makes the entire result null for all rows.
        if matches!(sep_column, ColumnarValue::Scalar(s) if s.is_null()) {
            return Ok(null_scalar(&return_datatype));
        }

        let sep: ColumnarValueRef = ColumnarValueRef::from_columnar_value(sep_column, &mut data_size, len, args.len() - 2, true)?
            .map(Ok)
            .unwrap_or_else(|| plan_err!(
                "Input {sep_column} which is not a supported datatype for concat_ws separator"
            ))?;

        let mut columns = Vec::with_capacity(args.len() - 1);
        for arg in &args[1..] {
            if let Some(column) =
                ColumnarValueRef::from_columnar_value(arg, &mut data_size, len, 1, false)?
            {
                columns.push(column);
            }
        }

        match return_datatype {
            DataType::Utf8 => build_concat_ws(
                ConcatStringBuilder::with_capacity(len, data_size),
                &sep,
                &columns,
                len,
            ),
            DataType::LargeUtf8 => build_concat_ws(
                ConcatLargeStringBuilder::with_capacity(len, data_size),
                &sep,
                &columns,
                len,
            ),
            DataType::Utf8View => build_concat_ws(
                ConcatStringViewBuilder::with_capacity(len, data_size),
                &sep,
                &columns,
                len,
            ),
            DataType::Binary => build_concat_ws(
                ConcatBinaryBuilder::with_capacity(len, data_size),
                &sep,
                &columns,
                len,
            ),
            DataType::LargeBinary => build_concat_ws(
                ConcatLargeBinaryBuilder::with_capacity(len, data_size),
                &sep,
                &columns,
                len,
            ),
            DataType::BinaryView => build_concat_ws(
                ConcatBinaryViewBuilder::with_capacity(len, data_size),
                &sep,
                &columns,
                len,
            ),
            other => plan_err!("concat_ws does not support return type {other}"),
        }
    }

    /// Simply the `concat_ws` function by
    /// 1. folding to `null` if the delimiter is null
    /// 2. filtering out `null` arguments
    /// 3. using `concat` to replace `concat_ws` if the delimiter is an empty string
    /// 4. concatenating contiguous literals if the delimiter is a literal.
    fn simplify(
        &self,
        args: Vec<Expr>,
        _info: &SimplifyContext,
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

/// Build a `concat_ws` output array using a generic [`ConcatBuilder`].
/// Write non-null column values per row, inserting the separator between them
fn build_concat_ws<B: ConcatBuilder>(
    mut builder: B,
    sep: &ColumnarValueRef,
    columns: &[ColumnarValueRef],
    len: usize,
) -> Result<ColumnarValue> {
    for i in 0..len {
        if !sep.is_valid(i) {
            builder.append_offset()?;
            continue;
        }
        let mut first = true;
        for column in columns {
            if column.is_valid(i) {
                if !first {
                    builder.write::<false>(sep, i)?;
                }
                builder.write::<false>(column, i)?;
                first = false;
            }
        }
        builder.append_offset()?;
    }
    let array = builder.finish(sep.nulls())?;
    Ok(ColumnarValue::Array(array))
}

fn null_scalar(dt: &DataType) -> ColumnarValue {
    ColumnarValue::Scalar(
        ScalarValue::try_new_null(dt).unwrap_or(ScalarValue::Utf8(None)),
    )
}

fn simplify_concat_ws(delimiter: &Expr, args: &[Expr]) -> Result<ExprSimplifyResult> {
    // Preserve the delimiter's string type for any new literals produced
    // during simplification.
    let delimiter_type = match delimiter {
        Expr::Literal(v, _) => v.data_type(),
        _ => DataType::Utf8,
    };

    // Shortcut for binary delimiters
    if delimiter_type.is_binary() {
        let mut args = args
            .iter()
            .filter(|x| !is_null(x))
            .cloned()
            .collect::<Vec<_>>();
        args.insert(0, delimiter.clone());
        return Ok(ExprSimplifyResult::Original(args));
    }

    let typed_lit = |s: String| -> Expr {
        match delimiter_type {
            DataType::LargeUtf8 => lit(ScalarValue::LargeUtf8(Some(s))),
            DataType::Utf8View => lit(ScalarValue::Utf8View(Some(s))),
            _ => lit(s),
        }
    };

    match delimiter {
        Expr::Literal(
            ScalarValue::Utf8(delimiter)
            | ScalarValue::LargeUtf8(delimiter)
            | ScalarValue::Utf8View(delimiter),
            _,
        ) => {
            match delimiter {
                // When the delimiter is the empty string, replace `concat_ws`
                // with `concat`
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
                    new_args.push(typed_lit(delimiter.to_string()));
                    let mut contiguous_scalar = None;
                    for arg in args {
                        match arg {
                            // filter out null args
                            Expr::Literal(
                                ScalarValue::Utf8(None)
                                | ScalarValue::LargeUtf8(None)
                                | ScalarValue::Utf8View(None),
                                _,
                            ) => {}
                            Expr::Literal(
                                ScalarValue::Utf8(Some(v))
                                | ScalarValue::LargeUtf8(Some(v))
                                | ScalarValue::Utf8View(Some(v)),
                                _,
                            ) => match contiguous_scalar {
                                None => contiguous_scalar = Some(v.to_string()),
                                Some(mut pre) => {
                                    pre += delimiter;
                                    pre += v;
                                    contiguous_scalar = Some(pre)
                                }
                            },
                            Expr::Literal(s, _) => {
                                return internal_err!(
                                    "The scalar {s} should be casted to string type during the type coercion."
                                );
                            }
                            // If the arg is not a literal, we should first push the current `contiguous_scalar`
                            // to the `new_args` and reset it to None.
                            // Then pushing this arg to the `new_args`.
                            arg => {
                                if let Some(val) = contiguous_scalar {
                                    new_args.push(typed_lit(val));
                                }
                                new_args.push(arg.clone());
                                contiguous_scalar = None;
                            }
                        }
                    }
                    if let Some(val) = contiguous_scalar {
                        new_args.push(typed_lit(val));
                    }

                    Ok(ExprSimplifyResult::Simplified(Expr::ScalarFunction(
                        ScalarFunction {
                            func: concat_ws(),
                            args: new_args,
                        },
                    )))
                }
                // If the delimiter is null, then the value of the whole expression is null.
                None => {
                    let null_scalar = match delimiter_type {
                        DataType::LargeUtf8 => ScalarValue::LargeUtf8(None),
                        DataType::Utf8View => ScalarValue::Utf8View(None),
                        _ => ScalarValue::Utf8(None),
                    };
                    Ok(ExprSimplifyResult::Simplified(Expr::Literal(
                        null_scalar,
                        None,
                    )))
                }
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
    use arrow::array::{
        Array, ArrayRef, BinaryArray, LargeBinaryArray, LargeStringArray, StringArray,
        StringViewArray,
    };
    use arrow::datatypes::DataType::{Binary, LargeBinary, LargeUtf8, Utf8, Utf8View};
    use arrow::datatypes::Field;
    use datafusion_common::Result;
    use datafusion_common::ScalarValue;
    use datafusion_common::config::ConfigOptions;
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

    #[test]
    fn concat_ws_utf8view_scalar_separator() -> Result<()> {
        let c0 = ColumnarValue::Scalar(ScalarValue::Utf8View(Some(",".to_string())));
        let c1 =
            ColumnarValue::Array(Arc::new(StringArray::from(vec!["foo", "bar", "baz"])));
        let c2 = ColumnarValue::Array(Arc::new(StringArray::from(vec![
            Some("x"),
            None,
            Some("z"),
        ])));

        let arg_fields = vec![
            Field::new("a", Utf8View, true).into(),
            Field::new("a", Utf8, true).into(),
            Field::new("a", Utf8, true).into(),
        ];
        let args = ScalarFunctionArgs {
            args: vec![c0, c1, c2],
            arg_fields,
            number_rows: 3,
            return_field: Field::new("f", Utf8View, true).into(),
            config_options: Arc::new(ConfigOptions::default()),
        };

        let result = ConcatWsFunc::new().invoke_with_args(args)?;
        let expected =
            Arc::new(StringViewArray::from(vec!["foo,x", "bar", "baz,z"])) as ArrayRef;
        match &result {
            ColumnarValue::Array(array) => {
                assert_eq!(&expected, array);
            }
            _ => panic!("Expected array result"),
        }

        Ok(())
    }

    #[test]
    fn concat_ws_largeutf8_scalar_separator() -> Result<()> {
        let c0 = ColumnarValue::Scalar(ScalarValue::LargeUtf8(Some(",".to_string())));
        let c1 =
            ColumnarValue::Array(Arc::new(StringArray::from(vec!["foo", "bar", "baz"])));
        let c2 = ColumnarValue::Array(Arc::new(StringArray::from(vec![
            Some("x"),
            None,
            Some("z"),
        ])));

        let arg_fields = vec![
            Field::new("a", LargeUtf8, true).into(),
            Field::new("a", Utf8, true).into(),
            Field::new("a", Utf8, true).into(),
        ];
        let args = ScalarFunctionArgs {
            args: vec![c0, c1, c2],
            arg_fields,
            number_rows: 3,
            return_field: Field::new("f", LargeUtf8, true).into(),
            config_options: Arc::new(ConfigOptions::default()),
        };

        let result = ConcatWsFunc::new().invoke_with_args(args)?;
        let expected =
            Arc::new(LargeStringArray::from(vec!["foo,x", "bar", "baz,z"])) as ArrayRef;
        match &result {
            ColumnarValue::Array(array) => {
                assert_eq!(&expected, array);
            }
            _ => panic!("Expected array result"),
        }

        Ok(())
    }

    #[test]
    fn concat_ws_utf8view_nullable_separator() -> Result<()> {
        let c0 = ColumnarValue::Array(Arc::new(StringViewArray::from(vec![
            Some(","),
            None,
            Some("+"),
        ])));
        let c1 = ColumnarValue::Array(Arc::new(StringViewArray::from(vec![
            "foo", "bar", "baz",
        ])));
        let c2 = ColumnarValue::Array(Arc::new(StringViewArray::from(vec![
            Some("x"),
            Some("y"),
            Some("z"),
        ])));

        let arg_fields = vec![
            Field::new("a", Utf8View, true).into(),
            Field::new("a", Utf8View, true).into(),
            Field::new("a", Utf8View, true).into(),
        ];
        let args = ScalarFunctionArgs {
            args: vec![c0, c1, c2],
            arg_fields,
            number_rows: 3,
            return_field: Field::new("f", Utf8View, true).into(),
            config_options: Arc::new(ConfigOptions::default()),
        };

        let result = ConcatWsFunc::new().invoke_with_args(args)?;
        let expected = Arc::new(StringViewArray::from(vec![
            Some("foo,x"),
            None,
            Some("baz+z"),
        ])) as ArrayRef;
        match &result {
            ColumnarValue::Array(array) => {
                assert_eq!(&expected, array);
            }
            _ => panic!("Expected array result"),
        }

        Ok(())
    }

    #[test]
    fn concat_ws_largeutf8_arrays() -> Result<()> {
        let c0 = ColumnarValue::Scalar(ScalarValue::LargeUtf8(Some(",".to_string())));
        let c1 = ColumnarValue::Array(Arc::new(LargeStringArray::from(vec![
            "foo", "bar", "baz",
        ])));
        let c2 = ColumnarValue::Array(Arc::new(LargeStringArray::from(vec![
            Some("x"),
            None,
            Some("z"),
        ])));

        let arg_fields = vec![
            Field::new("a", LargeUtf8, true).into(),
            Field::new("a", LargeUtf8, true).into(),
            Field::new("a", LargeUtf8, true).into(),
        ];
        let args = ScalarFunctionArgs {
            args: vec![c0, c1, c2],
            arg_fields,
            number_rows: 3,
            return_field: Field::new("f", LargeUtf8, true).into(),
            config_options: Arc::new(ConfigOptions::default()),
        };

        let result = ConcatWsFunc::new().invoke_with_args(args)?;
        let expected =
            Arc::new(LargeStringArray::from(vec!["foo,x", "bar", "baz,z"])) as ArrayRef;
        match &result {
            ColumnarValue::Array(array) => {
                assert_eq!(&expected, array);
            }
            _ => panic!("Expected array result"),
        }

        Ok(())
    }

    #[test]
    fn concat_ws_utf8view_null_separator() -> Result<()> {
        // All-scalar path: null Utf8View separator should return Utf8View(None)
        let c0 = ColumnarValue::Scalar(ScalarValue::Utf8View(None));
        let c1 = ColumnarValue::Scalar(ScalarValue::Utf8View(Some("aa".to_string())));
        let c2 = ColumnarValue::Scalar(ScalarValue::Utf8View(Some("bb".to_string())));

        let arg_fields = vec![
            Field::new("a", Utf8View, true).into(),
            Field::new("a", Utf8View, true).into(),
            Field::new("a", Utf8View, true).into(),
        ];
        let args = ScalarFunctionArgs {
            args: vec![c0, c1, c2],
            arg_fields,
            number_rows: 1,
            return_field: Field::new("f", Utf8View, true).into(),
            config_options: Arc::new(ConfigOptions::default()),
        };

        let result = ConcatWsFunc::new().invoke_with_args(args)?;
        match result {
            ColumnarValue::Scalar(ScalarValue::Utf8View(None)) => {}
            other => panic!("Expected Utf8View(None), got {other:?}"),
        }

        // Array path: null Utf8View scalar separator with array args
        let c0 = ColumnarValue::Scalar(ScalarValue::Utf8View(None));
        let c1 =
            ColumnarValue::Array(Arc::new(StringViewArray::from(vec!["foo", "bar"])));

        let arg_fields = vec![
            Field::new("a", Utf8View, true).into(),
            Field::new("a", Utf8View, true).into(),
        ];
        let args = ScalarFunctionArgs {
            args: vec![c0, c1],
            arg_fields,
            number_rows: 2,
            return_field: Field::new("f", Utf8View, true).into(),
            config_options: Arc::new(ConfigOptions::default()),
        };

        let result = ConcatWsFunc::new().invoke_with_args(args)?;
        match result {
            ColumnarValue::Scalar(ScalarValue::Utf8View(None)) => {}
            other => panic!("Expected Utf8View(None), got {other:?}"),
        }

        Ok(())
    }

    #[test]
    fn concat_ws_largeutf8_null_separator() -> Result<()> {
        // All-scalar path: null LargeUtf8 separator should return LargeUtf8(None)
        let c0 = ColumnarValue::Scalar(ScalarValue::LargeUtf8(None));
        let c1 = ColumnarValue::Scalar(ScalarValue::LargeUtf8(Some("aa".to_string())));
        let c2 = ColumnarValue::Scalar(ScalarValue::LargeUtf8(Some("bb".to_string())));

        let arg_fields = vec![
            Field::new("a", LargeUtf8, true).into(),
            Field::new("a", LargeUtf8, true).into(),
            Field::new("a", LargeUtf8, true).into(),
        ];
        let args = ScalarFunctionArgs {
            args: vec![c0, c1, c2],
            arg_fields,
            number_rows: 1,
            return_field: Field::new("f", LargeUtf8, true).into(),
            config_options: Arc::new(ConfigOptions::default()),
        };

        let result = ConcatWsFunc::new().invoke_with_args(args)?;
        match result {
            ColumnarValue::Scalar(ScalarValue::LargeUtf8(None)) => {}
            other => panic!("Expected LargeUtf8(None), got {other:?}"),
        }

        // Array path: null LargeUtf8 scalar separator with array args
        let c0 = ColumnarValue::Scalar(ScalarValue::LargeUtf8(None));
        let c1 =
            ColumnarValue::Array(Arc::new(LargeStringArray::from(vec!["foo", "bar"])));

        let arg_fields = vec![
            Field::new("a", LargeUtf8, true).into(),
            Field::new("a", LargeUtf8, true).into(),
        ];
        let args = ScalarFunctionArgs {
            args: vec![c0, c1],
            arg_fields,
            number_rows: 2,
            return_field: Field::new("f", LargeUtf8, true).into(),
            config_options: Arc::new(ConfigOptions::default()),
        };

        let result = ConcatWsFunc::new().invoke_with_args(args)?;
        match result {
            ColumnarValue::Scalar(ScalarValue::LargeUtf8(None)) => {}
            other => panic!("Expected LargeUtf8(None), got {other:?}"),
        }

        Ok(())
    }

    #[test]
    fn concat_ws_binary_scalars() -> Result<()> {
        let c0 = ColumnarValue::Scalar(ScalarValue::Binary(Some(b"|".to_vec())));
        let c1 = ColumnarValue::Scalar(ScalarValue::Binary(Some(b"aa".to_vec())));
        let c2 = ColumnarValue::Scalar(ScalarValue::Binary(None));
        let c3 = ColumnarValue::Scalar(ScalarValue::Binary(Some(b"cc".to_vec())));

        let arg_fields = vec![
            Field::new("a", Binary, true).into(),
            Field::new("a", Binary, true).into(),
            Field::new("a", Binary, true).into(),
            Field::new("a", Binary, true).into(),
        ];
        let args = ScalarFunctionArgs {
            args: vec![c0, c1, c2, c3],
            arg_fields,
            number_rows: 1,
            return_field: Field::new("f", Binary, true).into(),
            config_options: Arc::new(ConfigOptions::default()),
        };
        let result = ConcatWsFunc::new().invoke_with_args(args)?;
        match result {
            ColumnarValue::Scalar(ScalarValue::Binary(Some(v))) => {
                assert_eq!(v, b"aa|cc");
            }
            other => panic!("Expected Binary scalar, got {other:?}"),
        }

        Ok(())
    }

    #[test]
    fn concat_ws_binary_arrays() -> Result<()> {
        for c1_large_binary in [false, true] {
            let c0 = ColumnarValue::Scalar(ScalarValue::Binary(Some(b",".to_vec())));
            let c1 = if c1_large_binary {
                ColumnarValue::Array(Arc::new(LargeBinaryArray::from_vec(vec![
                    b"foo".as_ref(),
                    b"bar",
                    b"baz",
                ])))
            } else {
                ColumnarValue::Array(Arc::new(BinaryArray::from_vec(vec![
                    b"foo".as_ref(),
                    b"bar",
                    b"baz",
                ])))
            };
            let c2 =
                ColumnarValue::Array(Arc::new(LargeBinaryArray::from_opt_vec(vec![
                    Some(b"x".as_ref()),
                    None,
                    Some(b"z"),
                ])));

            let arg_fields = vec![
                Field::new("a", Binary, true).into(),
                Field::new("a", Binary, true).into(),
                Field::new("a", LargeBinary, true).into(),
            ];
            let args = ScalarFunctionArgs {
                args: vec![c0, c1, c2],
                arg_fields,
                number_rows: 3,
                return_field: Field::new("f", LargeBinary, true).into(),
                config_options: Arc::new(ConfigOptions::default()),
            };

            let result = ConcatWsFunc::new().invoke_with_args(args)?;
            let expected = Arc::new(LargeBinaryArray::from_opt_vec(vec![
                Some(b"foo,x".as_ref()),
                Some(b"bar"),
                Some(b"baz,z"),
            ])) as ArrayRef;
            match &result {
                ColumnarValue::Array(array) => assert_eq!(&expected, array),
                _ => panic!("Expected array result"),
            }
        }

        Ok(())
    }
}
