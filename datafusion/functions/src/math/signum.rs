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

use arrow::array::AsArray;
use arrow::datatypes::DataType::{Float32, Float64};
use arrow::datatypes::{DataType, Float32Type, Float64Type};

use datafusion_common::utils::take_function_args;
use datafusion_common::{Result, ScalarValue, internal_err};
use datafusion_expr::sort_properties::{ExprProperties, SortProperties};
use datafusion_expr::{
    ColumnarValue, Documentation, ScalarFunctionArgs, ScalarUDFImpl, Signature,
    Volatility,
};
use datafusion_macros::user_doc;

#[user_doc(
    doc_section(label = "Math Functions"),
    description = r#"Returns the sign of a number.
Negative numbers return `-1`.
Zero and positive numbers return `1`."#,
    syntax_example = "signum(numeric_expression)",
    standard_argument(name = "numeric_expression", prefix = "Numeric"),
    sql_example = r#"```sql
> SELECT signum(-42);
+-------------+
| signum(-42) |
+-------------+
| -1          |
+-------------+
```"#
)]
#[derive(Debug, PartialEq, Eq, Hash)]
pub struct SignumFunc {
    signature: Signature,
}

impl Default for SignumFunc {
    fn default() -> Self {
        SignumFunc::new()
    }
}

impl SignumFunc {
    pub fn new() -> Self {
        use DataType::*;
        Self {
            signature: Signature::uniform(
                1,
                vec![Float64, Float32],
                Volatility::Immutable,
            ),
        }
    }
}

impl ScalarUDFImpl for SignumFunc {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "signum"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        match &arg_types[0] {
            Float32 => Ok(Float32),
            _ => Ok(Float64),
        }
    }

    fn output_ordering(&self, input: &[ExprProperties]) -> Result<SortProperties> {
        // Non-decreasing for all real numbers x.
        Ok(input[0].sort_properties)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let return_type = args.return_type().clone();
        let [arg] = take_function_args(self.name(), args.args)?;

        match arg {
            ColumnarValue::Scalar(scalar) => {
                if scalar.is_null() {
                    return ColumnarValue::Scalar(ScalarValue::Null)
                        .cast_to(&return_type, None);
                }

                match scalar {
                    ScalarValue::Float64(Some(v)) => {
                        let result = if v == 0.0 { 0.0 } else { v.signum() };
                        Ok(ColumnarValue::Scalar(ScalarValue::Float64(Some(result))))
                    }
                    ScalarValue::Float32(Some(v)) => {
                        let result = if v == 0.0 { 0.0 } else { v.signum() };
                        Ok(ColumnarValue::Scalar(ScalarValue::Float32(Some(result))))
                    }
                    _ => {
                        internal_err!(
                            "Unexpected scalar type for signum: {:?}",
                            scalar.data_type()
                        )
                    }
                }
            }
            ColumnarValue::Array(array) => match array.data_type() {
                Float64 => Ok(ColumnarValue::Array(Arc::new(
                    array.as_primitive::<Float64Type>().unary::<_, Float64Type>(
                        |x: f64| {
                            if x == 0.0 { 0.0 } else { x.signum() }
                        },
                    ),
                ))),
                Float32 => Ok(ColumnarValue::Array(Arc::new(
                    array.as_primitive::<Float32Type>().unary::<_, Float32Type>(
                        |x: f32| {
                            if x == 0.0 { 0.0 } else { x.signum() }
                        },
                    ),
                ))),
                other => {
                    internal_err!("Unsupported data type {other:?} for function signum")
                }
            },
        }
    }

    fn documentation(&self) -> Option<&Documentation> {
        self.doc()
    }
}

#[cfg(test)]
mod test {
    use std::sync::Arc;

    use arrow::array::{ArrayRef, Float32Array, Float64Array};
    use arrow::datatypes::{DataType, Field};
    use datafusion_common::cast::{as_float32_array, as_float64_array};
    use datafusion_common::config::ConfigOptions;
    use datafusion_expr::{ColumnarValue, ScalarFunctionArgs, ScalarUDFImpl};

    use crate::math::signum::SignumFunc;

    #[test]
    fn test_signum_f32() {
        let array = Arc::new(Float32Array::from(vec![
            -1.0,
            -0.0,
            0.0,
            1.0,
            -0.01,
            0.01,
            f32::NAN,
            f32::INFINITY,
            f32::NEG_INFINITY,
        ]));
        let arg_fields = vec![Field::new("a", DataType::Float32, false).into()];
        let args = ScalarFunctionArgs {
            args: vec![ColumnarValue::Array(Arc::clone(&array) as ArrayRef)],
            arg_fields,
            number_rows: array.len(),
            return_field: Field::new("f", DataType::Float32, true).into(),
            config_options: Arc::new(ConfigOptions::default()),
        };
        let result = SignumFunc::new()
            .invoke_with_args(args)
            .expect("failed to initialize function signum");

        match result {
            ColumnarValue::Array(arr) => {
                let floats = as_float32_array(&arr)
                    .expect("failed to convert result to a Float32Array");

                assert_eq!(floats.len(), 9);
                assert_eq!(floats.value(0), -1.0);
                assert_eq!(floats.value(1), 0.0);
                assert_eq!(floats.value(2), 0.0);
                assert_eq!(floats.value(3), 1.0);
                assert_eq!(floats.value(4), -1.0);
                assert_eq!(floats.value(5), 1.0);
                assert!(floats.value(6).is_nan());
                assert_eq!(floats.value(7), 1.0);
                assert_eq!(floats.value(8), -1.0);
            }
            ColumnarValue::Scalar(_) => {
                panic!("Expected an array value")
            }
        }
    }

    #[test]
    fn test_signum_f64() {
        let array = Arc::new(Float64Array::from(vec![
            -1.0,
            -0.0,
            0.0,
            1.0,
            -0.01,
            0.01,
            f64::NAN,
            f64::INFINITY,
            f64::NEG_INFINITY,
        ]));
        let arg_fields = vec![Field::new("a", DataType::Float64, false).into()];
        let args = ScalarFunctionArgs {
            args: vec![ColumnarValue::Array(Arc::clone(&array) as ArrayRef)],
            arg_fields,
            number_rows: array.len(),
            return_field: Field::new("f", DataType::Float64, true).into(),
            config_options: Arc::new(ConfigOptions::default()),
        };
        let result = SignumFunc::new()
            .invoke_with_args(args)
            .expect("failed to initialize function signum");

        match result {
            ColumnarValue::Array(arr) => {
                let floats = as_float64_array(&arr)
                    .expect("failed to convert result to a Float32Array");

                assert_eq!(floats.len(), 9);
                assert_eq!(floats.value(0), -1.0);
                assert_eq!(floats.value(1), 0.0);
                assert_eq!(floats.value(2), 0.0);
                assert_eq!(floats.value(3), 1.0);
                assert_eq!(floats.value(4), -1.0);
                assert_eq!(floats.value(5), 1.0);
                assert!(floats.value(6).is_nan());
                assert_eq!(floats.value(7), 1.0);
                assert_eq!(floats.value(8), -1.0);
            }
            ColumnarValue::Scalar(_) => {
                panic!("Expected an array value")
            }
        }
    }
}
