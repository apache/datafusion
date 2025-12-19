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

use arrow::array::ArrayRef;
use arrow::datatypes::DataType;
use datafusion_expr::simplify::{ExprSimplifyResult, SimplifyInfo};
use datafusion_expr::type_coercion::binary::{
    binary_to_string_coercion, string_coercion,
};

use crate::utils::make_scalar_function;
use datafusion_common::types::logical_string;
use datafusion_common::{Result, ScalarValue, internal_err};
use datafusion_expr::{
    Coercion, ColumnarValue, Documentation, Expr, Like, ScalarFunctionArgs,
    ScalarUDFImpl, Signature, TypeSignatureClass, Volatility, cast,
};
use datafusion_macros::user_doc;

/// Returns true if string starts with prefix.
/// starts_with('alphabet', 'alph') = 't'
fn starts_with(args: &[ArrayRef]) -> Result<ArrayRef> {
    if let Some(coercion_data_type) =
        string_coercion(args[0].data_type(), args[1].data_type()).or_else(|| {
            binary_to_string_coercion(args[0].data_type(), args[1].data_type())
        })
    {
        let arg0 = if args[0].data_type() == &coercion_data_type {
            Arc::clone(&args[0])
        } else {
            arrow::compute::kernels::cast::cast(&args[0], &coercion_data_type)?
        };
        let arg1 = if args[1].data_type() == &coercion_data_type {
            Arc::clone(&args[1])
        } else {
            arrow::compute::kernels::cast::cast(&args[1], &coercion_data_type)?
        };
        let result = arrow::compute::kernels::comparison::starts_with(&arg0, &arg1)?;
        Ok(Arc::new(result) as ArrayRef)
    } else {
        internal_err!(
            "Unsupported data types for starts_with. Expected Utf8, LargeUtf8 or Utf8View"
        )
    }
}

#[user_doc(
    doc_section(label = "String Functions"),
    description = "Tests if a string starts with a substring.",
    syntax_example = "starts_with(str, substr)",
    sql_example = r#"```sql
> select starts_with('datafusion','data');
+----------------------------------------------+
| starts_with(Utf8("datafusion"),Utf8("data")) |
+----------------------------------------------+
| true                                         |
+----------------------------------------------+
```"#,
    standard_argument(name = "str", prefix = "String"),
    argument(name = "substr", description = "Substring to test for.")
)]
#[derive(Debug, PartialEq, Eq, Hash)]
pub struct StartsWithFunc {
    signature: Signature,
}

impl Default for StartsWithFunc {
    fn default() -> Self {
        Self::new()
    }
}

impl StartsWithFunc {
    pub fn new() -> Self {
        Self {
            signature: Signature::coercible(
                vec![
                    Coercion::new_exact(TypeSignatureClass::Native(logical_string())),
                    Coercion::new_exact(TypeSignatureClass::Native(logical_string())),
                ],
                Volatility::Immutable,
            ),
        }
    }
}

impl ScalarUDFImpl for StartsWithFunc {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "starts_with"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Boolean)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        make_scalar_function(starts_with, vec![])(&args.args)
    }

    fn simplify(
        &self,
        args: Vec<Expr>,
        info: &dyn SimplifyInfo,
    ) -> Result<ExprSimplifyResult> {
        if let Expr::Literal(scalar_value, _) = &args[1] {
            // Convert starts_with(col, 'prefix') to col LIKE 'prefix%' with proper escaping
            // Escapes pattern characters: starts_with(col, 'j\_a%') -> col LIKE 'j\\\_a\%%'
            //   1. 'j\_a%'         (input pattern)
            //   2. 'j\\\_a\%'       (escape special chars '%', '_' and '\')
            //   3. 'j\\\_a\%%'      (add unescaped % suffix for starts_with)
            let like_expr = match scalar_value {
                ScalarValue::Utf8(Some(pattern))
                | ScalarValue::LargeUtf8(Some(pattern))
                | ScalarValue::Utf8View(Some(pattern)) => {
                    let escaped_pattern = pattern
                        .replace("\\", "\\\\")
                        .replace("%", "\\%")
                        .replace("_", "\\_");
                    let like_pattern = format!("{escaped_pattern}%");
                    Expr::Literal(ScalarValue::Utf8(Some(like_pattern)), None)
                }
                _ => return Ok(ExprSimplifyResult::Original(args)),
            };

            let expr_data_type = info.get_data_type(&args[0])?;
            let pattern_data_type = info.get_data_type(&like_expr)?;

            if let Some(coercion_data_type) =
                string_coercion(&expr_data_type, &pattern_data_type).or_else(|| {
                    binary_to_string_coercion(&expr_data_type, &pattern_data_type)
                })
            {
                let expr = if expr_data_type == coercion_data_type {
                    args[0].clone()
                } else {
                    cast(args[0].clone(), coercion_data_type.clone())
                };

                let pattern = if pattern_data_type == coercion_data_type {
                    like_expr
                } else {
                    cast(like_expr, coercion_data_type)
                };

                return Ok(ExprSimplifyResult::Simplified(Expr::Like(Like {
                    negated: false,
                    expr: Box::new(expr),
                    pattern: Box::new(pattern),
                    escape_char: None,
                    case_insensitive: false,
                })));
            }
        }

        Ok(ExprSimplifyResult::Original(args))
    }

    fn documentation(&self) -> Option<&Documentation> {
        self.doc()
    }
}

#[cfg(test)]
mod tests {
    use crate::utils::test::test_function;
    use arrow::array::{Array, BooleanArray};
    use arrow::datatypes::DataType::Boolean;
    use datafusion_common::{Result, ScalarValue};
    use datafusion_expr::{ColumnarValue, ScalarUDFImpl};

    use super::*;

    #[test]
    fn test_functions() -> Result<()> {
        // Generate test cases for starts_with
        let test_cases = vec![
            (Some("alphabet"), Some("alph"), Some(true)),
            (Some("alphabet"), Some("bet"), Some(false)),
            (
                Some("somewhat large string"),
                Some("somewhat large"),
                Some(true),
            ),
            (Some("somewhat large string"), Some("large"), Some(false)),
        ]
        .into_iter()
        .flat_map(|(a, b, c)| {
            let utf_8_args = vec![
                ColumnarValue::Scalar(ScalarValue::Utf8(a.map(|s| s.to_string()))),
                ColumnarValue::Scalar(ScalarValue::Utf8(b.map(|s| s.to_string()))),
            ];

            let large_utf_8_args = vec![
                ColumnarValue::Scalar(ScalarValue::LargeUtf8(a.map(|s| s.to_string()))),
                ColumnarValue::Scalar(ScalarValue::LargeUtf8(b.map(|s| s.to_string()))),
            ];

            let utf_8_view_args = vec![
                ColumnarValue::Scalar(ScalarValue::Utf8View(a.map(|s| s.to_string()))),
                ColumnarValue::Scalar(ScalarValue::Utf8View(b.map(|s| s.to_string()))),
            ];

            vec![(utf_8_args, c), (large_utf_8_args, c), (utf_8_view_args, c)]
        });

        for (args, expected) in test_cases {
            test_function!(
                StartsWithFunc::new(),
                args,
                Ok(expected),
                bool,
                Boolean,
                BooleanArray
            );
        }

        Ok(())
    }
}
