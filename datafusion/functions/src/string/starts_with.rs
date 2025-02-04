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

use crate::utils::make_scalar_function;
use datafusion_common::{internal_err, Result, ScalarValue};
use datafusion_expr::{ColumnarValue, Documentation, Expr, Like};
use datafusion_expr::{ScalarUDFImpl, Signature, Volatility};
use datafusion_macros::user_doc;

/// Returns true if string starts with prefix.
/// starts_with('alphabet', 'alph') = 't'
pub fn starts_with(args: &[ArrayRef]) -> Result<ArrayRef> {
    let result = arrow::compute::kernels::comparison::starts_with(&args[0], &args[1])?;
    Ok(Arc::new(result) as ArrayRef)
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
#[derive(Debug)]
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
            signature: Signature::string(2, Volatility::Immutable),
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

    fn invoke_batch(
        &self,
        args: &[ColumnarValue],
        _number_rows: usize,
    ) -> Result<ColumnarValue> {
        match args[0].data_type() {
            DataType::Utf8View | DataType::Utf8 | DataType::LargeUtf8 => {
                make_scalar_function(starts_with, vec![])(args)
            }
            _ => internal_err!("Unsupported data types for starts_with. Expected Utf8, LargeUtf8 or Utf8View")?,
        }
    }

    fn simplify(
        &self,
        args: Vec<Expr>,
        _info: &dyn SimplifyInfo,
    ) -> Result<ExprSimplifyResult> {
        if let Expr::Literal(scalar_value) = &args[1] {
            // Convert starts_with(col, 'prefix') to col LIKE 'prefix%' with proper escaping
            // Example: starts_with(col, 'ja%') -> col LIKE 'ja\%%'
            //   1. 'ja%'         (input pattern)
            //   2. 'ja\%'        (escape special char '%')
            //   3. 'ja\%%'       (add suffix for starts_with)
            let like_expr = match scalar_value {
                ScalarValue::Utf8(Some(pattern)) => {
                    let escaped_pattern = pattern.replace("%", "\\%");
                    let like_pattern = format!("{}%", escaped_pattern);
                    Expr::Literal(ScalarValue::Utf8(Some(like_pattern)))
                }
                ScalarValue::LargeUtf8(Some(pattern)) => {
                    let escaped_pattern = pattern.replace("%", "\\%");
                    let like_pattern = format!("{}%", escaped_pattern);
                    Expr::Literal(ScalarValue::LargeUtf8(Some(like_pattern)))
                }
                ScalarValue::Utf8View(Some(pattern)) => {
                    let escaped_pattern = pattern.replace("%", "\\%");
                    let like_pattern = format!("{}%", escaped_pattern);
                    Expr::Literal(ScalarValue::Utf8View(Some(like_pattern)))
                }
                _ => return Ok(ExprSimplifyResult::Original(args)),
            };

            return Ok(ExprSimplifyResult::Simplified(Expr::Like(Like {
                negated: false,
                expr: Box::new(args[0].clone()),
                pattern: Box::new(like_expr),
                escape_char: None,
                case_insensitive: false,
            })));
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
