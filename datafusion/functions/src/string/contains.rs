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

use arrow::array::{Array, ArrayRef, Scalar};
use arrow::compute::contains as arrow_contains;
use arrow::datatypes::DataType;
use arrow::datatypes::DataType::{Boolean, LargeUtf8, Utf8, Utf8View};
use datafusion_common::types::logical_string;
use datafusion_common::{Result, exec_err};
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
        contains(args.args.as_slice())
    }

    fn documentation(&self) -> Option<&Documentation> {
        self.doc()
    }
}

fn to_array(value: &ColumnarValue) -> Result<(ArrayRef, bool)> {
    match value {
        ColumnarValue::Array(array) => Ok((Arc::clone(array), false)),
        ColumnarValue::Scalar(scalar) => Ok((scalar.to_array()?, true)),
    }
}

/// Helper to call arrow_contains with proper Datum handling.
/// When an argument is marked as scalar, we wrap it in `Scalar` to tell arrow's
/// kernel to use the optimized single-value code path instead of iterating.
fn call_arrow_contains(
    haystack: &ArrayRef,
    haystack_is_scalar: bool,
    needle: &ArrayRef,
    needle_is_scalar: bool,
) -> Result<ColumnarValue> {
    // Arrow's Datum trait is implemented for ArrayRef, Arc<dyn Array>, and Scalar<T>
    // We pass ArrayRef directly when not scalar, or wrap in Scalar when it is
    let result = match (haystack_is_scalar, needle_is_scalar) {
        (false, false) => arrow_contains(haystack, needle)?,
        (false, true) => arrow_contains(haystack, &Scalar::new(Arc::clone(needle)))?,
        (true, false) => arrow_contains(&Scalar::new(Arc::clone(haystack)), needle)?,
        (true, true) => arrow_contains(
            &Scalar::new(Arc::clone(haystack)),
            &Scalar::new(Arc::clone(needle)),
        )?,
    };

    // If both inputs were scalar, return a scalar result
    if haystack_is_scalar && needle_is_scalar {
        let scalar = datafusion_common::ScalarValue::try_from_array(&result, 0)?;
        Ok(ColumnarValue::Scalar(scalar))
    } else {
        Ok(ColumnarValue::Array(Arc::new(result)))
    }
}

/// use `arrow::compute::contains` to do the calculation for contains
fn contains(args: &[ColumnarValue]) -> Result<ColumnarValue> {
    let (haystack, haystack_is_scalar) = to_array(&args[0])?;
    let (needle, needle_is_scalar) = to_array(&args[1])?;

    if let Some(coercion_data_type) =
        string_coercion(haystack.data_type(), needle.data_type()).or_else(|| {
            binary_to_string_coercion(haystack.data_type(), needle.data_type())
        })
    {
        let haystack = if haystack.data_type() == &coercion_data_type {
            haystack
        } else {
            arrow::compute::kernels::cast::cast(&haystack, &coercion_data_type)?
        };
        let needle = if needle.data_type() == &coercion_data_type {
            needle
        } else {
            arrow::compute::kernels::cast::cast(&needle, &coercion_data_type)?
        };

        match coercion_data_type {
            Utf8View | Utf8 | LargeUtf8 => call_arrow_contains(
                &haystack,
                haystack_is_scalar,
                &needle,
                needle_is_scalar,
            ),
            other => {
                exec_err!("Unsupported data type {other:?} for function `contains`.")
            }
        }
    } else {
        exec_err!(
            "Unsupported data type {}, {:?} for function `contains`.",
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
