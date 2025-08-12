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

use crate::utils::make_scalar_function;
use arrow::array::{Array, ArrayRef, AsArray};
use arrow::compute::contains as arrow_contains;
use arrow::datatypes::DataType;
use arrow::datatypes::DataType::{Boolean, LargeUtf8, Utf8, Utf8View};
use datafusion_common::types::logical_string;
use datafusion_common::{exec_err, DataFusionError, Result};
use datafusion_expr::binary::{binary_to_string_coercion, string_coercion};
use datafusion_expr::{
    Coercion, ColumnarValue, Documentation, ScalarFunctionArgs, ScalarUDFImpl, Signature,
    TypeSignatureClass, Volatility,
};
use datafusion_macros::user_doc;
use std::any::Any;
use std::sync::Arc;

#[user_doc(
    doc_section(label = "String Functions"),
    description = "Return true if search_str is found within string (case-sensitive).",
    syntax_example = "contains(str, search_str)",
    sql_example = r#"```sql
> select contains('the quick brown fox', 'row');
+---------------------------------------------------+
| contains(Utf8("the quick brown fox"),Utf8("row")) |
+---------------------------------------------------+
| true                                              |
+---------------------------------------------------+
```"#,
    standard_argument(name = "str", prefix = "String"),
    argument(name = "search_str", description = "The string to search for in str.")
)]
#[derive(Debug)]
pub struct ContainsFunc {
    signature: Signature,
}

impl Default for ContainsFunc {
    fn default() -> Self {
        ContainsFunc::new()
    }
}

impl ContainsFunc {
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

impl ScalarUDFImpl for ContainsFunc {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "contains"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _: &[DataType]) -> Result<DataType> {
        Ok(Boolean)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        make_scalar_function(contains, vec![])(&args.args)
    }

    fn documentation(&self) -> Option<&Documentation> {
        self.doc()
    }
}

/// use `arrow::compute::contains` to do the calculation for contains
fn contains(args: &[ArrayRef]) -> Result<ArrayRef, DataFusionError> {
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

        match coercion_data_type {
            Utf8View => {
                let mod_str = arg0.as_string_view();
                let match_str = arg1.as_string_view();
                let res = arrow_contains(mod_str, match_str)?;
                Ok(Arc::new(res) as ArrayRef)
            }
            Utf8 => {
                let mod_str = arg0.as_string::<i32>();
                let match_str = arg1.as_string::<i32>();
                let res = arrow_contains(mod_str, match_str)?;
                Ok(Arc::new(res) as ArrayRef)
            }
            LargeUtf8 => {
                let mod_str = arg0.as_string::<i64>();
                let match_str = arg1.as_string::<i64>();
                let res = arrow_contains(mod_str, match_str)?;
                Ok(Arc::new(res) as ArrayRef)
            }
            other => {
                exec_err!("Unsupported data type {other:?} for function `contains`.")
            }
        }
    } else {
        exec_err!(
            "Unsupported data type {:?}, {:?} for function `contains`.",
            args[0].data_type(),
            args[1].data_type()
        )
    }
}

#[cfg(test)]
mod test {
    use super::ContainsFunc;
    use crate::expr_fn::contains;
    use arrow::array::{BooleanArray, StringArray};
    use arrow::datatypes::{DataType, Field};
    use datafusion_common::config::ConfigOptions;
    use datafusion_common::ScalarValue;
    use datafusion_expr::{ColumnarValue, Expr, ScalarFunctionArgs, ScalarUDFImpl};
    use std::sync::Arc;

    #[test]
    fn test_contains_udf() {
        let udf = ContainsFunc::new();
        let array = ColumnarValue::Array(Arc::new(StringArray::from(vec![
            Some("xxx?()"),
            Some("yyy?()"),
        ])));
        let scalar = ColumnarValue::Scalar(ScalarValue::Utf8(Some("x?(".to_string())));
        let arg_fields = vec![
            Field::new("a", DataType::Utf8, true).into(),
            Field::new("a", DataType::Utf8, true).into(),
        ];

        let args = ScalarFunctionArgs {
            args: vec![array, scalar],
            arg_fields,
            number_rows: 2,
            return_field: Field::new("f", DataType::Boolean, true).into(),
            config_options: Arc::new(ConfigOptions::default()),
        };

        let actual = udf.invoke_with_args(args).unwrap();
        let expect = ColumnarValue::Array(Arc::new(BooleanArray::from(vec![
            Some(true),
            Some(false),
        ])));
        assert_eq!(
            *actual.into_array(2).unwrap(),
            *expect.into_array(2).unwrap()
        );
    }

    #[test]
    fn test_contains_api() {
        let expr = contains(
            Expr::Literal(
                ScalarValue::Utf8(Some("the quick brown fox".to_string())),
                None,
            ),
            Expr::Literal(ScalarValue::Utf8(Some("row".to_string())), None),
        );
        assert_eq!(
            expr.to_string(),
            "contains(Utf8(\"the quick brown fox\"), Utf8(\"row\"))"
        );
    }
}
