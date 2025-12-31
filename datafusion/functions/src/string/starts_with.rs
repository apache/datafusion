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

use arrow::array::{ArrayRef, Scalar};
use arrow::compute::kernels::comparison::starts_with as arrow_starts_with;
use arrow::datatypes::DataType;
use datafusion_common::utils::take_function_args;
use datafusion_expr::simplify::{ExprSimplifyResult, SimplifyInfo};
use datafusion_expr::type_coercion::binary::{
    binary_to_string_coercion, string_coercion,
};

use datafusion_common::types::logical_string;
use datafusion_common::{Result, ScalarValue, exec_err};
use datafusion_expr::{
    Coercion, ColumnarValue, Documentation, Expr, Like, ScalarFunctionArgs,
    ScalarUDFImpl, Signature, TypeSignatureClass, Volatility, cast,
};
use datafusion_macros::user_doc;

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
        let [str_arg, prefix_arg] = take_function_args(self.name(), &args.args)?;

        // Determine the common type for coercion
        let coercion_type = string_coercion(
            &str_arg.data_type(),
            &prefix_arg.data_type(),
        )
        .or_else(|| {
            binary_to_string_coercion(&str_arg.data_type(), &prefix_arg.data_type())
        });

        let Some(coercion_type) = coercion_type else {
            return exec_err!(
                "Unsupported data types {:?}, {:?} for function `starts_with`.",
                str_arg.data_type(),
                prefix_arg.data_type()
            );
        };

        // Helper to cast an array if needed
        let maybe_cast = |arr: &ArrayRef, target: &DataType| -> Result<ArrayRef> {
            if arr.data_type() == target {
                Ok(Arc::clone(arr))
            } else {
                Ok(arrow::compute::kernels::cast::cast(arr, target)?)
            }
        };

        match (str_arg, prefix_arg) {
            // Both scalars - just compute directly
            (ColumnarValue::Scalar(str_scalar), ColumnarValue::Scalar(prefix_scalar)) => {
                let str_arr = str_scalar.to_array_of_size(1)?;
                let prefix_arr = prefix_scalar.to_array_of_size(1)?;
                let str_arr = maybe_cast(&str_arr, &coercion_type)?;
                let prefix_arr = maybe_cast(&prefix_arr, &coercion_type)?;
                let result = arrow_starts_with(&str_arr, &prefix_arr)?;
                Ok(ColumnarValue::Scalar(ScalarValue::try_from_array(
                    &result, 0,
                )?))
            }
            // String is array, prefix is scalar - use Scalar wrapper for optimization
            (ColumnarValue::Array(str_arr), ColumnarValue::Scalar(prefix_scalar)) => {
                let str_arr = maybe_cast(str_arr, &coercion_type)?;
                let prefix_arr = prefix_scalar.to_array_of_size(1)?;
                let prefix_arr = maybe_cast(&prefix_arr, &coercion_type)?;
                let prefix_scalar = Scalar::new(prefix_arr);
                let result = arrow_starts_with(&str_arr, &prefix_scalar)?;
                Ok(ColumnarValue::Array(Arc::new(result)))
            }
            // String is scalar, prefix is array - use Scalar wrapper for string
            (ColumnarValue::Scalar(str_scalar), ColumnarValue::Array(prefix_arr)) => {
                let str_arr = str_scalar.to_array_of_size(1)?;
                let str_arr = maybe_cast(&str_arr, &coercion_type)?;
                let str_scalar = Scalar::new(str_arr);
                let prefix_arr = maybe_cast(prefix_arr, &coercion_type)?;
                let result = arrow_starts_with(&str_scalar, &prefix_arr)?;
                Ok(ColumnarValue::Array(Arc::new(result)))
            }
            // Both arrays - pass directly
            (ColumnarValue::Array(str_arr), ColumnarValue::Array(prefix_arr)) => {
                let str_arr = maybe_cast(str_arr, &coercion_type)?;
                let prefix_arr = maybe_cast(prefix_arr, &coercion_type)?;
                let result = arrow_starts_with(&str_arr, &prefix_arr)?;
                Ok(ColumnarValue::Array(Arc::new(result)))
            }
        }
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
    use arrow::array::{Array, BooleanArray, StringArray};
    use arrow::datatypes::DataType::Boolean;
    use arrow::datatypes::{DataType, Field};
    use datafusion_common::config::ConfigOptions;
    use datafusion_common::{Result, ScalarValue};
    use datafusion_expr::{ColumnarValue, ScalarFunctionArgs, ScalarUDFImpl};
    use std::sync::Arc;

    use super::*;

    #[test]
    fn test_scalar_scalar() -> Result<()> {
        // Test Scalar + Scalar combinations
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

    #[test]
    fn test_array_scalar() -> Result<()> {
        // Test Array + Scalar (the optimized path)
        let array = ColumnarValue::Array(Arc::new(StringArray::from(vec![
            Some("alphabet"),
            Some("alphabet"),
            Some("beta"),
            None,
        ])));
        let scalar = ColumnarValue::Scalar(ScalarValue::Utf8(Some("alph".to_string())));

        let args = vec![array, scalar];
        test_function!(
            StartsWithFunc::new(),
            args,
            Ok(Some(true)), // First element result
            bool,
            Boolean,
            BooleanArray
        );

        Ok(())
    }

    #[test]
    fn test_array_scalar_full_result() {
        // Test Array + Scalar and verify all results
        let func = StartsWithFunc::new();
        let array = Arc::new(StringArray::from(vec![
            Some("alphabet"),
            Some("alphabet"),
            Some("beta"),
            None,
        ]));
        let args = vec![
            ColumnarValue::Array(array),
            ColumnarValue::Scalar(ScalarValue::Utf8(Some("alph".to_string()))),
        ];

        let result = func
            .invoke_with_args(ScalarFunctionArgs {
                args,
                arg_fields: vec![
                    Field::new("a", DataType::Utf8, true).into(),
                    Field::new("b", DataType::Utf8, true).into(),
                ],
                number_rows: 4,
                return_field: Field::new("f", Boolean, true).into(),
                config_options: Arc::new(ConfigOptions::default()),
            })
            .unwrap();

        let result_array = result.into_array(4).unwrap();
        let bool_array = result_array
            .as_any()
            .downcast_ref::<BooleanArray>()
            .unwrap();

        assert!(bool_array.value(0)); // "alphabet" starts with "alph"
        assert!(bool_array.value(1)); // "alphabet" starts with "alph"
        assert!(!bool_array.value(2)); // "beta" does not start with "alph"
        assert!(bool_array.is_null(3)); // null input -> null output
    }

    #[test]
    fn test_scalar_array() {
        // Test Scalar + Array
        let func = StartsWithFunc::new();
        let prefixes = Arc::new(StringArray::from(vec![
            Some("alph"),
            Some("bet"),
            Some("alpha"),
            None,
        ]));
        let args = vec![
            ColumnarValue::Scalar(ScalarValue::Utf8(Some("alphabet".to_string()))),
            ColumnarValue::Array(prefixes),
        ];

        let result = func
            .invoke_with_args(ScalarFunctionArgs {
                args,
                arg_fields: vec![
                    Field::new("a", DataType::Utf8, true).into(),
                    Field::new("b", DataType::Utf8, true).into(),
                ],
                number_rows: 4,
                return_field: Field::new("f", Boolean, true).into(),
                config_options: Arc::new(ConfigOptions::default()),
            })
            .unwrap();

        let result_array = result.into_array(4).unwrap();
        let bool_array = result_array
            .as_any()
            .downcast_ref::<BooleanArray>()
            .unwrap();

        assert!(bool_array.value(0)); // "alphabet" starts with "alph"
        assert!(!bool_array.value(1)); // "alphabet" does not start with "bet"
        assert!(bool_array.value(2)); // "alphabet" starts with "alpha"
        assert!(bool_array.is_null(3)); // null prefix -> null output
    }

    #[test]
    fn test_array_array() {
        // Test Array + Array
        let func = StartsWithFunc::new();
        let strings = Arc::new(StringArray::from(vec![
            Some("alphabet"),
            Some("rust"),
            Some("datafusion"),
            None,
        ]));
        let prefixes = Arc::new(StringArray::from(vec![
            Some("alph"),
            Some("ru"),
            Some("hello"),
            Some("test"),
        ]));
        let args = vec![
            ColumnarValue::Array(strings),
            ColumnarValue::Array(prefixes),
        ];

        let result = func
            .invoke_with_args(ScalarFunctionArgs {
                args,
                arg_fields: vec![
                    Field::new("a", DataType::Utf8, true).into(),
                    Field::new("b", DataType::Utf8, true).into(),
                ],
                number_rows: 4,
                return_field: Field::new("f", Boolean, true).into(),
                config_options: Arc::new(ConfigOptions::default()),
            })
            .unwrap();

        let result_array = result.into_array(4).unwrap();
        let bool_array = result_array
            .as_any()
            .downcast_ref::<BooleanArray>()
            .unwrap();

        assert!(bool_array.value(0)); // "alphabet" starts with "alph"
        assert!(bool_array.value(1)); // "rust" starts with "ru"
        assert!(!bool_array.value(2)); // "datafusion" does not start with "hello"
        assert!(bool_array.is_null(3)); // null string -> null output
    }
}
