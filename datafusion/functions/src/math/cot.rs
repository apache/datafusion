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
use datafusion_expr::{ColumnarValue, Documentation, ScalarFunctionArgs};
use datafusion_expr::{ScalarUDFImpl, Signature, Volatility};
use datafusion_macros::user_doc;

#[user_doc(
    doc_section(label = "Math Functions"),
    description = "Returns the cotangent of a number.",
    syntax_example = r#"cot(numeric_expression)"#,
    sql_example = r#"```sql
> SELECT cot(1);
+---------+
| cot(1)  |
+---------+
| 0.64209 |
+---------+
```"#,
    standard_argument(name = "numeric_expression", prefix = "Numeric")
)]
#[derive(Debug, PartialEq, Eq, Hash)]
pub struct CotFunc {
    signature: Signature,
}

impl Default for CotFunc {
    fn default() -> Self {
        CotFunc::new()
    }
}

impl CotFunc {
    pub fn new() -> Self {
        use DataType::*;
        Self {
            // math expressions expect 1 argument of type f64 or f32
            // priority is given to f64 because e.g. `sqrt(1i32)` is in IR (real numbers) and thus we
            // return the best approximation for it (in f64).
            // We accept f32 because in this case it is clear that the best approximation
            // will be as good as the number of digits in the number
            signature: Signature::uniform(
                1,
                vec![Float64, Float32],
                Volatility::Immutable,
            ),
        }
    }
}

impl ScalarUDFImpl for CotFunc {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "cot"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        match arg_types[0] {
            Float32 => Ok(Float32),
            _ => Ok(Float64),
        }
    }

    fn documentation(&self) -> Option<&Documentation> {
        self.doc()
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let return_field = args.return_field;
        let [arg] = take_function_args(self.name(), args.args)?;

        match arg {
            ColumnarValue::Scalar(scalar) => {
                if scalar.is_null() {
                    return ColumnarValue::Scalar(ScalarValue::Null)
                        .cast_to(return_field.data_type(), None);
                }

                match scalar {
                    ScalarValue::Float64(Some(v)) => Ok(ColumnarValue::Scalar(
                        ScalarValue::Float64(Some(compute_cot64(v))),
                    )),
                    ScalarValue::Float32(Some(v)) => Ok(ColumnarValue::Scalar(
                        ScalarValue::Float32(Some(compute_cot32(v))),
                    )),
                    _ => {
                        internal_err!(
                            "Unexpected scalar type for cot: {:?}",
                            scalar.data_type()
                        )
                    }
                }
            }
            ColumnarValue::Array(array) => match array.data_type() {
                Float64 => Ok(ColumnarValue::Array(Arc::new(
                    array
                        .as_primitive::<Float64Type>()
                        .unary::<_, Float64Type>(compute_cot64),
                ))),
                Float32 => Ok(ColumnarValue::Array(Arc::new(
                    array
                        .as_primitive::<Float32Type>()
                        .unary::<_, Float32Type>(compute_cot32),
                ))),
                other => {
                    internal_err!("Unexpected data type {other:?} for function cot")
                }
            },
        }
    }
}

fn compute_cot32(x: f32) -> f32 {
    let a = f32::tan(x);
    1.0 / a
}

fn compute_cot64(x: f64) -> f64 {
    let a = f64::tan(x);
    1.0 / a
}

#[cfg(test)]
mod test {
    use std::sync::Arc;

    use arrow::array::{ArrayRef, Float32Array, Float64Array};
    use arrow::datatypes::{DataType, Field};
    use datafusion_common::ScalarValue;
    use datafusion_common::cast::{as_float32_array, as_float64_array};
    use datafusion_common::config::ConfigOptions;
    use datafusion_expr::{ColumnarValue, ScalarFunctionArgs, ScalarUDFImpl};

    use crate::math::cot::CotFunc;

    #[test]
    fn test_cot_f32() {
        let array = Arc::new(Float32Array::from(vec![12.1, 30.0, 90.0, -30.0]));
        let arg_fields = vec![Field::new("a", DataType::Float32, false).into()];
        let args = ScalarFunctionArgs {
            args: vec![ColumnarValue::Array(Arc::clone(&array) as ArrayRef)],
            arg_fields,
            number_rows: array.len(),
            return_field: Field::new("f", DataType::Float32, true).into(),
            config_options: Arc::new(ConfigOptions::default()),
        };
        let result = CotFunc::new()
            .invoke_with_args(args)
            .expect("failed to initialize function cot");

        match result {
            ColumnarValue::Array(arr) => {
                let floats = as_float32_array(&arr)
                    .expect("failed to convert result to a Float32Array");

                let expected = Float32Array::from(vec![
                    -1.986_460_4,
                    -0.156_119_96,
                    -0.501_202_8,
                    0.156_119_96,
                ]);

                let eps = 1e-6;
                assert_eq!(floats.len(), 4);
                assert!((floats.value(0) - expected.value(0)).abs() < eps);
                assert!((floats.value(1) - expected.value(1)).abs() < eps);
                assert!((floats.value(2) - expected.value(2)).abs() < eps);
                assert!((floats.value(3) - expected.value(3)).abs() < eps);
            }
            ColumnarValue::Scalar(_) => {
                panic!("Expected an array value")
            }
        }
    }

    #[test]
    fn test_cot_f64() {
        let array = Arc::new(Float64Array::from(vec![12.1, 30.0, 90.0, -30.0]));
        let arg_fields = vec![Field::new("a", DataType::Float64, false).into()];
        let args = ScalarFunctionArgs {
            args: vec![ColumnarValue::Array(Arc::clone(&array) as ArrayRef)],
            arg_fields,
            number_rows: array.len(),
            return_field: Field::new("f", DataType::Float64, true).into(),
            config_options: Arc::new(ConfigOptions::default()),
        };
        let result = CotFunc::new()
            .invoke_with_args(args)
            .expect("failed to initialize function cot");

        match result {
            ColumnarValue::Array(arr) => {
                let floats = as_float64_array(&arr)
                    .expect("failed to convert result to a Float64Array");

                let expected = Float64Array::from(vec![
                    -1.986_458_685_881_4,
                    -0.156_119_952_161_6,
                    -0.501_202_783_380_1,
                    0.156_119_952_161_6,
                ]);

                let eps = 1e-12;
                assert_eq!(floats.len(), 4);
                assert!((floats.value(0) - expected.value(0)).abs() < eps);
                assert!((floats.value(1) - expected.value(1)).abs() < eps);
                assert!((floats.value(2) - expected.value(2)).abs() < eps);
                assert!((floats.value(3) - expected.value(3)).abs() < eps);
            }
            ColumnarValue::Scalar(_) => {
                panic!("Expected an array value")
            }
        }
    }

    #[test]
    fn test_cot_scalar_f64() {
        let arg_fields = vec![Field::new("a", DataType::Float64, false).into()];
        let args = ScalarFunctionArgs {
            args: vec![ColumnarValue::Scalar(ScalarValue::Float64(Some(1.0)))],
            arg_fields,
            number_rows: 1,
            return_field: Field::new("f", DataType::Float64, false).into(),
            config_options: Arc::new(ConfigOptions::default()),
        };
        let result = CotFunc::new()
            .invoke_with_args(args)
            .expect("cot scalar should succeed");

        match result {
            ColumnarValue::Scalar(ScalarValue::Float64(Some(v))) => {
                // cot(1.0) = 1/tan(1.0) ≈ 0.6420926159343306
                let expected = 1.0_f64 / 1.0_f64.tan();
                assert!((v - expected).abs() < 1e-12);
            }
            _ => panic!("Expected Float64 scalar"),
        }
    }

    #[test]
    fn test_cot_scalar_f32() {
        let arg_fields = vec![Field::new("a", DataType::Float32, false).into()];
        let args = ScalarFunctionArgs {
            args: vec![ColumnarValue::Scalar(ScalarValue::Float32(Some(1.0)))],
            arg_fields,
            number_rows: 1,
            return_field: Field::new("f", DataType::Float32, false).into(),
            config_options: Arc::new(ConfigOptions::default()),
        };
        let result = CotFunc::new()
            .invoke_with_args(args)
            .expect("cot scalar should succeed");

        match result {
            ColumnarValue::Scalar(ScalarValue::Float32(Some(v))) => {
                let expected = 1.0_f32 / 1.0_f32.tan();
                assert!((v - expected).abs() < 1e-6);
            }
            _ => panic!("Expected Float32 scalar"),
        }
    }

    #[test]
    fn test_cot_scalar_null() {
        let arg_fields = vec![Field::new("a", DataType::Float64, true).into()];
        let args = ScalarFunctionArgs {
            args: vec![ColumnarValue::Scalar(ScalarValue::Float64(None))],
            arg_fields,
            number_rows: 1,
            return_field: Field::new("f", DataType::Float64, true).into(),
            config_options: Arc::new(ConfigOptions::default()),
        };
        let result = CotFunc::new()
            .invoke_with_args(args)
            .expect("cot null should succeed");

        match result {
            ColumnarValue::Scalar(scalar) => {
                assert!(scalar.is_null());
            }
            _ => panic!("Expected scalar result"),
        }
    }

    #[test]
    fn test_cot_scalar_zero() {
        let arg_fields = vec![Field::new("a", DataType::Float64, false).into()];
        let args = ScalarFunctionArgs {
            args: vec![ColumnarValue::Scalar(ScalarValue::Float64(Some(0.0)))],
            arg_fields,
            number_rows: 1,
            return_field: Field::new("f", DataType::Float64, false).into(),
            config_options: Arc::new(ConfigOptions::default()),
        };
        let result = CotFunc::new()
            .invoke_with_args(args)
            .expect("cot zero should succeed");

        match result {
            ColumnarValue::Scalar(ScalarValue::Float64(Some(v))) => {
                // cot(0) = 1/tan(0) = infinity
                assert!(v.is_infinite());
            }
            _ => panic!("Expected Float64 scalar"),
        }
    }

    #[test]
    fn test_cot_scalar_pi() {
        let arg_fields = vec![Field::new("a", DataType::Float64, false).into()];
        let args = ScalarFunctionArgs {
            args: vec![ColumnarValue::Scalar(ScalarValue::Float64(Some(
                std::f64::consts::PI,
            )))],
            arg_fields,
            number_rows: 1,
            return_field: Field::new("f", DataType::Float64, false).into(),
            config_options: Arc::new(ConfigOptions::default()),
        };
        let result = CotFunc::new()
            .invoke_with_args(args)
            .expect("cot pi should succeed");

        match result {
            ColumnarValue::Scalar(ScalarValue::Float64(Some(v))) => {
                // cot(PI) = 1/tan(PI) - very large negative number due to floating point
                let expected = 1.0_f64 / std::f64::consts::PI.tan();
                assert!((v - expected).abs() < 1e-6);
            }
            _ => panic!("Expected Float64 scalar"),
        }
    }
}
