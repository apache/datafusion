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

use arrow::array::{ArrayRef, Scalar};
use arrow::compute::contains as arrow_contains;
use arrow::datatypes::DataType;
use arrow::datatypes::DataType::Boolean;
use datafusion_common::types::logical_string;
use datafusion_common::{Result, ScalarValue, exec_err};
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
#[derive(Debug, PartialEq, Eq, Hash)]
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
        let [str_arg, search_arg] = args.args.as_slice() else {
            return exec_err!(
                "contains was called with {} arguments, expected 2",
                args.args.len()
            );
        };

        // Determine the common type for coercion
        let coercion_type = string_coercion(
            &str_arg.data_type(),
            &search_arg.data_type(),
        )
        .or_else(|| {
            binary_to_string_coercion(&str_arg.data_type(), &search_arg.data_type())
        });

        let Some(coercion_type) = coercion_type else {
            return exec_err!(
                "Unsupported data types {:?}, {:?} for function `contains`.",
                str_arg.data_type(),
                search_arg.data_type()
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

        match (str_arg, search_arg) {
            // Both scalars - just compute directly
            (ColumnarValue::Scalar(str_scalar), ColumnarValue::Scalar(search_scalar)) => {
                let str_arr = str_scalar.to_array_of_size(1)?;
                let search_arr = search_scalar.to_array_of_size(1)?;
                let str_arr = maybe_cast(&str_arr, &coercion_type)?;
                let search_arr = maybe_cast(&search_arr, &coercion_type)?;
                let result = arrow_contains(&str_arr, &search_arr)?;
                Ok(ColumnarValue::Scalar(ScalarValue::try_from_array(
                    &result, 0,
                )?))
            }
            // String is array, search is scalar - use Scalar wrapper for optimization
            (ColumnarValue::Array(str_arr), ColumnarValue::Scalar(search_scalar)) => {
                let str_arr = maybe_cast(str_arr, &coercion_type)?;
                let search_arr = search_scalar.to_array_of_size(1)?;
                let search_arr = maybe_cast(&search_arr, &coercion_type)?;
                let search_scalar = Scalar::new(search_arr);
                let result = arrow_contains(&str_arr, &search_scalar)?;
                Ok(ColumnarValue::Array(Arc::new(result)))
            }
            // String is scalar, search is array - use Scalar wrapper for string
            (ColumnarValue::Scalar(str_scalar), ColumnarValue::Array(search_arr)) => {
                let str_arr = str_scalar.to_array_of_size(1)?;
                let str_arr = maybe_cast(&str_arr, &coercion_type)?;
                let str_scalar = Scalar::new(str_arr);
                let search_arr = maybe_cast(search_arr, &coercion_type)?;
                let result = arrow_contains(&str_scalar, &search_arr)?;
                Ok(ColumnarValue::Array(Arc::new(result)))
            }
            // Both arrays - pass directly
            (ColumnarValue::Array(str_arr), ColumnarValue::Array(search_arr)) => {
                let str_arr = maybe_cast(str_arr, &coercion_type)?;
                let search_arr = maybe_cast(search_arr, &coercion_type)?;
                let result = arrow_contains(&str_arr, &search_arr)?;
                Ok(ColumnarValue::Array(Arc::new(result)))
            }
        }
    }

    fn documentation(&self) -> Option<&Documentation> {
        self.doc()
    }
}

#[cfg(test)]
mod test {
    use super::ContainsFunc;
    use crate::expr_fn::contains;
    use arrow::array::{BooleanArray, StringArray};
    use arrow::datatypes::{DataType, Field};
    use datafusion_common::ScalarValue;
    use datafusion_common::config::ConfigOptions;
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
