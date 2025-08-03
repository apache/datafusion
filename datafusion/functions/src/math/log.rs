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

//! Math function: `log()`.

use std::any::Any;
use std::sync::Arc;

use super::power::PowerFunc;

use arrow::array::{Array, ArrayRef, AsArray, PrimitiveArray};
use arrow::compute::{kernels, try_binary};
use arrow::datatypes::DataType::{Decimal128, Decimal256};
use arrow::datatypes::{
    DataType, Decimal128Type, Decimal256Type, Float32Type, Float64Type,
};
use arrow::error::ArrowError;
use arrow_buffer::i256;
use datafusion_common::{
    exec_err, internal_err, plan_datafusion_err, plan_err, Result, ScalarValue,
};
use datafusion_expr::expr::ScalarFunction;
use datafusion_expr::simplify::{ExprSimplifyResult, SimplifyInfo};
use datafusion_expr::sort_properties::{ExprProperties, SortProperties};
use datafusion_expr::{
    lit, ColumnarValue, Documentation, Expr, ScalarFunctionArgs, ScalarUDF,
    TypeSignature::*,
};
use datafusion_expr::{ScalarUDFImpl, Signature, Volatility};
use datafusion_macros::user_doc;

#[user_doc(
    doc_section(label = "Math Functions"),
    description = "Returns the base-x logarithm of a number. Can either provide a specified base, or if omitted then takes the base-10 of a number.",
    syntax_example = r#"log(base, numeric_expression)
log(numeric_expression)"#,
    standard_argument(name = "base", prefix = "Base numeric"),
    standard_argument(name = "numeric_expression", prefix = "Numeric")
)]
#[derive(Debug)]
pub struct LogFunc {
    signature: Signature,
}

impl Default for LogFunc {
    fn default() -> Self {
        Self::new()
    }
}

impl LogFunc {
    pub fn new() -> Self {
        Self {
            signature: Signature::one_of(
                vec![Numeric(1), Numeric(2)],
                Volatility::Immutable,
            ),
        }
    }
}

/// Convert Decimal128 components (value and scale) to an unscaled i128
fn decimal128_to_i128(value: i128, scale: i8) -> Result<i128, ArrowError> {
    if scale < 0 {
        Err(ArrowError::ComputeError(
            "Negative scale is not supported".into(),
        ))
    } else if scale == 0 {
        Ok(value)
    } else {
        match i128::from(10).checked_pow(scale as u32) {
            Some(divisor) => Ok(value / divisor),
            None => Err(ArrowError::ComputeError(format!(
                "Cannot get a power of {scale}"
            ))),
        }
    }
}

/// Binary function to calculate an integer logarithm of Decimal128 `value` using `base` base
/// Returns error if base is invalid
fn log_decimal128(value: i128, scale: i8, base: f64) -> Result<f64, ArrowError> {
    if value <= 0 {
        // Reflect f64::log behaviour
        Ok(f64::NAN)
    } else if !base.is_finite() || base.trunc() != base || (base as u32) < 2 {
        Err(ArrowError::ComputeError(format!(
            "Log cannot use non-integer or small base {base}"
        )))
    } else {
        let unscaled_value = decimal128_to_i128(value, scale)?;
        let log_value: u32 = unscaled_value.ilog(base as i128);
        Ok(log_value as f64)
    }
}

/// Binary function to calculate an integer logarithm of Decimal128 `value` using `base` base
/// Returns error if base is invalid or if value is out of bounds of Decimal128
fn log_decimal256(value: i256, scale: i8, base: f64) -> Result<f64, ArrowError> {
    if value <= i256::ZERO {
        // Reflect f64::log behaviour
        Ok(f64::NAN)
    } else if !base.is_finite() || base.trunc() != base || (base as u32) < 2 {
        Err(ArrowError::ComputeError(format!(
            "Log cannot use non-integer or small base {base}"
        )))
    } else {
        match value.to_i128() {
            Some(short_value) => {
                // Calculate logarithm only for 128-bit decimals
                let unscaled_value = decimal128_to_i128(short_value, scale)?;
                let log_value: u32 = unscaled_value.ilog(base as i128);
                Ok(log_value as f64)
            }
            None => Err(ArrowError::ComputeError(format!(
                "Log of a large Decimal256 is not supported: {value}"
            ))),
        }
    }
}

impl ScalarUDFImpl for LogFunc {
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn name(&self) -> &str {
        "log"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        // Check last argument (value)
        match &arg_types.last().ok_or(plan_datafusion_err!("No args"))? {
            DataType::Float32 => Ok(DataType::Float32),
            _ => Ok(DataType::Float64),
        }
    }

    fn output_ordering(&self, input: &[ExprProperties]) -> Result<SortProperties> {
        let (base_sort_properties, num_sort_properties) = if input.len() == 1 {
            // log(x) defaults to log(10, x)
            (SortProperties::Singleton, input[0].sort_properties)
        } else {
            (input[0].sort_properties, input[1].sort_properties)
        };
        match (num_sort_properties, base_sort_properties) {
            (first @ SortProperties::Ordered(num), SortProperties::Ordered(base))
                if num.descending != base.descending
                    && num.nulls_first == base.nulls_first =>
            {
                Ok(first)
            }
            (
                first @ (SortProperties::Ordered(_) | SortProperties::Singleton),
                SortProperties::Singleton,
            ) => Ok(first),
            (SortProperties::Singleton, second @ SortProperties::Ordered(_)) => {
                Ok(-second)
            }
            _ => Ok(SortProperties::Unordered),
        }
    }

    // Support overloaded log(base, x) and log(x) which defaults to log(10, x)
    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let args = ColumnarValue::values_to_arrays(&args.args)?;

        let (base, x) = if args.len() == 2 {
            // note in f64::log params order is different than in sql. e.g in sql log(base, x) == f64::log(x, base)
            (ColumnarValue::Array(Arc::clone(&args[0])), &args[1])
        } else {
            // log(num) - assume base is 10
            (
                ColumnarValue::Array(
                    ScalarValue::new_ten(args[0].data_type())?
                        .to_array_of_size(args[0].len())?,
                ),
                &args[0],
            )
        };

        let arr: ArrayRef = match x.data_type() {
            DataType::Float64 => match base {
                ColumnarValue::Scalar(ScalarValue::Float32(Some(base))) => {
                    Arc::new(x.as_primitive::<Float64Type>().unary::<_, Float64Type>(
                        |value: f64| f64::log(value, base as f64),
                    ))
                }
                ColumnarValue::Array(base) => {
                    let x = x.as_primitive::<Float64Type>();
                    let base = base.as_primitive::<Float64Type>();
                    let result = arrow::compute::binary::<_, _, _, Float64Type>(
                        x,
                        base,
                        f64::log,
                    )?;
                    Arc::new(result) as _
                }
                _ => {
                    return exec_err!("log function requires a scalar or array for base")
                }
            },

            DataType::Float32 => match base {
                ColumnarValue::Scalar(ScalarValue::Float32(Some(base))) => Arc::new(
                    x.as_primitive::<Float32Type>()
                        .unary::<_, Float32Type>(|value: f32| f32::log(value, base)),
                ),
                ColumnarValue::Array(base) => {
                    let x = x.as_primitive::<Float32Type>();
                    let base = base.as_primitive::<Float32Type>();
                    let result = arrow::compute::binary::<_, _, _, Float32Type>(
                        x,
                        base,
                        f32::log,
                    )?;
                    Arc::new(result) as _
                }
                _ => {
                    return exec_err!("log function requires a scalar or array for base")
                }
            },

            Decimal128(_preicison, scale) => match base {
                ColumnarValue::Scalar(ScalarValue::Float32(Some(base))) => {
                    let x = x.as_primitive::<Decimal128Type>();
                    let array: PrimitiveArray<Float64Type> =
                        x.try_unary(|x| log_decimal128(x, *scale, base as f64))?;
                    Arc::new(array) as _
                }
                ColumnarValue::Scalar(scalar) => {
                    return exec_err!("unsupported log function base type {scalar}")
                }
                ColumnarValue::Array(base) => {
                    let x = x.as_primitive::<Decimal128Type>();
                    let base_array = kernels::cast::cast(&base, &DataType::Float64)?;
                    let base = base_array.as_primitive::<Float64Type>();
                    let array: PrimitiveArray<Float64Type> =
                        try_binary(x, base, |pvalue, pbase| {
                            log_decimal128(pvalue, *scale, pbase)
                        })?;
                    Arc::new(array) as _
                }
            },

            Decimal256(_preicison, scale) => match base {
                ColumnarValue::Scalar(ScalarValue::Float32(Some(base))) => {
                    let x = x.as_primitive::<Decimal256Type>();
                    let array: PrimitiveArray<Float64Type> =
                        x.try_unary(|x| log_decimal256(x, *scale, base as f64))?;
                    Arc::new(array) as _
                }
                ColumnarValue::Scalar(scalar) => {
                    return exec_err!("unsupported log function base type {scalar}")
                }
                ColumnarValue::Array(base) => {
                    let x = x.as_primitive::<Decimal256Type>();
                    let base_array = kernels::cast::cast(&base, &DataType::Float64)?;
                    let base = base_array.as_primitive::<Float64Type>();
                    let array: PrimitiveArray<Float64Type> =
                        try_binary(x, base, |pvalue, pbase| {
                            log_decimal256(pvalue, *scale, pbase)
                        })?;
                    Arc::new(array) as _
                }
            },

            other => {
                return exec_err!("Unsupported data type {other:?} for function log")
            }
        };

        Ok(ColumnarValue::Array(arr))
    }

    fn documentation(&self) -> Option<&Documentation> {
        self.doc()
    }

    /// Simplify the `log` function by the relevant rules:
    /// 1. Log(a, 1) ===> 0
    /// 2. Log(a, Power(a, b)) ===> b
    /// 3. Log(a, a) ===> 1
    fn simplify(
        &self,
        mut args: Vec<Expr>,
        info: &dyn SimplifyInfo,
    ) -> Result<ExprSimplifyResult> {
        // Args are either
        // log(number)
        // log(base, number)
        let num_args = args.len();
        if num_args > 2 {
            return plan_err!("Expected log to have 1 or 2 arguments, got {num_args}");
        }
        let number = args.pop().ok_or_else(|| {
            plan_datafusion_err!("Expected log to have 1 or 2 arguments, got 0")
        })?;
        let number_datatype = info.get_data_type(&number)?;
        // default to base 10
        let base = if let Some(base) = args.pop() {
            base
        } else {
            lit(ScalarValue::new_ten(&number_datatype)?)
        };

        match number {
            Expr::Literal(value, _)
                if value == ScalarValue::new_one(&number_datatype)? =>
            {
                Ok(ExprSimplifyResult::Simplified(lit(ScalarValue::new_zero(
                    &info.get_data_type(&base)?,
                )?)))
            }
            Expr::ScalarFunction(ScalarFunction { func, mut args })
                if is_pow(&func) && args.len() == 2 && base == args[0] =>
            {
                let b = args.pop().unwrap(); // length checked above
                Ok(ExprSimplifyResult::Simplified(b))
            }
            number => {
                if number == base {
                    Ok(ExprSimplifyResult::Simplified(lit(ScalarValue::new_one(
                        &number_datatype,
                    )?)))
                } else {
                    let args = match num_args {
                        1 => vec![number],
                        2 => vec![base, number],
                        _ => {
                            return internal_err!(
                                "Unexpected number of arguments in log::simplify"
                            )
                        }
                    };
                    Ok(ExprSimplifyResult::Original(args))
                }
            }
        }
    }
}

/// Returns true if the function is `PowerFunc`
fn is_pow(func: &ScalarUDF) -> bool {
    func.inner().as_any().downcast_ref::<PowerFunc>().is_some()
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use super::*;

    use arrow::array::{
        Decimal128Array, Decimal256Array, Float32Array, Float64Array, Int64Array,
    };
    use arrow::compute::SortOptions;
    use arrow::datatypes::Field;
    use datafusion_common::cast::{as_float32_array, as_float64_array};
    use datafusion_common::DFSchema;
    use datafusion_expr::execution_props::ExecutionProps;
    use datafusion_expr::simplify::SimplifyContext;

    #[test]
    #[should_panic]
    fn test_log_invalid_base_type() {
        let arg_fields = vec![
            Field::new("b", DataType::Int64, false).into(),
            Field::new("n", DataType::Float64, false).into(),
        ];
        let args = ScalarFunctionArgs {
            args: vec![
                ColumnarValue::Array(Arc::new(Int64Array::from(vec![5, 10, 15, 20]))), // base
                ColumnarValue::Array(Arc::new(Float64Array::from(vec![
                    10.0, 100.0, 1000.0, 10000.0,
                ]))), // num
            ],
            arg_fields,
            number_rows: 4,
            return_field: Field::new("f", DataType::Float64, true).into(),
        };
        let _ = LogFunc::new().invoke_with_args(args);
    }

    #[test]
    fn test_log_invalid_value() {
        let arg_field = Field::new("a", DataType::Int64, false).into();
        let args = ScalarFunctionArgs {
            args: vec![
                ColumnarValue::Array(Arc::new(Int64Array::from(vec![10]))), // num
            ],
            arg_fields: vec![arg_field],
            number_rows: 1,
            return_field: Field::new("f", DataType::Float64, true).into(),
        };

        let result = LogFunc::new().invoke_with_args(args);
        result.expect_err("expected error");
    }

    #[test]
    fn test_log_scalar_f32_unary() {
        let arg_field = Field::new("a", DataType::Float32, false).into();
        let args = ScalarFunctionArgs {
            args: vec![
                ColumnarValue::Scalar(ScalarValue::Float32(Some(10.0))), // num
            ],
            arg_fields: vec![arg_field],
            number_rows: 1,
            return_field: Field::new("f", DataType::Float32, true).into(),
        };
        let result = LogFunc::new()
            .invoke_with_args(args)
            .expect("failed to initialize function log");

        match result {
            ColumnarValue::Array(arr) => {
                let floats = as_float32_array(&arr)
                    .expect("failed to convert result to a Float32Array");

                assert_eq!(floats.len(), 1);
                assert!((floats.value(0) - 1.0).abs() < 1e-10);
            }
            ColumnarValue::Scalar(_) => {
                panic!("Expected an array value")
            }
        }
    }

    #[test]
    fn test_log_scalar_f64_unary() {
        let arg_field = Field::new("a", DataType::Float64, false).into();
        let args = ScalarFunctionArgs {
            args: vec![
                ColumnarValue::Scalar(ScalarValue::Float64(Some(10.0))), // num
            ],
            arg_fields: vec![arg_field],
            number_rows: 1,
            return_field: Field::new("f", DataType::Float64, true).into(),
        };
        let result = LogFunc::new()
            .invoke_with_args(args)
            .expect("failed to initialize function log");

        match result {
            ColumnarValue::Array(arr) => {
                let floats = as_float64_array(&arr)
                    .expect("failed to convert result to a Float64Array");

                assert_eq!(floats.len(), 1);
                assert!((floats.value(0) - 1.0).abs() < 1e-10);
            }
            ColumnarValue::Scalar(_) => {
                panic!("Expected an array value")
            }
        }
    }

    #[test]
    fn test_log_scalar_f32() {
        let arg_fields = vec![
            Field::new("a", DataType::Float32, false).into(),
            Field::new("a", DataType::Float32, false).into(),
        ];
        let args = ScalarFunctionArgs {
            args: vec![
                ColumnarValue::Scalar(ScalarValue::Float32(Some(2.0))), // base
                ColumnarValue::Scalar(ScalarValue::Float32(Some(32.0))), // num
            ],
            arg_fields,
            number_rows: 1,
            return_field: Field::new("f", DataType::Float32, true).into(),
        };
        let result = LogFunc::new()
            .invoke_with_args(args)
            .expect("failed to initialize function log");

        match result {
            ColumnarValue::Array(arr) => {
                let floats = as_float32_array(&arr)
                    .expect("failed to convert result to a Float32Array");

                assert_eq!(floats.len(), 1);
                assert!((floats.value(0) - 5.0).abs() < 1e-10);
            }
            ColumnarValue::Scalar(_) => {
                panic!("Expected an array value")
            }
        }
    }

    #[test]
    fn test_log_scalar_f64() {
        let arg_fields = vec![
            Field::new("a", DataType::Float64, false).into(),
            Field::new("a", DataType::Float64, false).into(),
        ];
        let args = ScalarFunctionArgs {
            args: vec![
                ColumnarValue::Scalar(ScalarValue::Float64(Some(2.0))), // base
                ColumnarValue::Scalar(ScalarValue::Float64(Some(64.0))), // num
            ],
            arg_fields,
            number_rows: 1,
            return_field: Field::new("f", DataType::Float64, true).into(),
        };
        let result = LogFunc::new()
            .invoke_with_args(args)
            .expect("failed to initialize function log");

        match result {
            ColumnarValue::Array(arr) => {
                let floats = as_float64_array(&arr)
                    .expect("failed to convert result to a Float64Array");

                assert_eq!(floats.len(), 1);
                assert!((floats.value(0) - 6.0).abs() < 1e-10);
            }
            ColumnarValue::Scalar(_) => {
                panic!("Expected an array value")
            }
        }
    }

    #[test]
    fn test_log_f64_unary() {
        let arg_field = Field::new("a", DataType::Float64, false).into();
        let args = ScalarFunctionArgs {
            args: vec![
                ColumnarValue::Array(Arc::new(Float64Array::from(vec![
                    10.0, 100.0, 1000.0, 10000.0,
                ]))), // num
            ],
            arg_fields: vec![arg_field],
            number_rows: 4,
            return_field: Field::new("f", DataType::Float64, true).into(),
        };
        let result = LogFunc::new()
            .invoke_with_args(args)
            .expect("failed to initialize function log");

        match result {
            ColumnarValue::Array(arr) => {
                let floats = as_float64_array(&arr)
                    .expect("failed to convert result to a Float64Array");

                assert_eq!(floats.len(), 4);
                assert!((floats.value(0) - 1.0).abs() < 1e-10);
                assert!((floats.value(1) - 2.0).abs() < 1e-10);
                assert!((floats.value(2) - 3.0).abs() < 1e-10);
                assert!((floats.value(3) - 4.0).abs() < 1e-10);
            }
            ColumnarValue::Scalar(_) => {
                panic!("Expected an array value")
            }
        }
    }

    #[test]
    fn test_log_f32_unary() {
        let arg_field = Field::new("a", DataType::Float32, false).into();
        let args = ScalarFunctionArgs {
            args: vec![
                ColumnarValue::Array(Arc::new(Float32Array::from(vec![
                    10.0, 100.0, 1000.0, 10000.0,
                ]))), // num
            ],
            arg_fields: vec![arg_field],
            number_rows: 4,
            return_field: Field::new("f", DataType::Float32, true).into(),
        };
        let result = LogFunc::new()
            .invoke_with_args(args)
            .expect("failed to initialize function log");

        match result {
            ColumnarValue::Array(arr) => {
                let floats = as_float32_array(&arr)
                    .expect("failed to convert result to a Float64Array");

                assert_eq!(floats.len(), 4);
                assert!((floats.value(0) - 1.0).abs() < 1e-10);
                assert!((floats.value(1) - 2.0).abs() < 1e-10);
                assert!((floats.value(2) - 3.0).abs() < 1e-10);
                assert!((floats.value(3) - 4.0).abs() < 1e-10);
            }
            ColumnarValue::Scalar(_) => {
                panic!("Expected an array value")
            }
        }
    }

    #[test]
    fn test_log_f64() {
        let arg_fields = vec![
            Field::new("a", DataType::Float64, false).into(),
            Field::new("a", DataType::Float64, false).into(),
        ];
        let args = ScalarFunctionArgs {
            args: vec![
                ColumnarValue::Array(Arc::new(Float64Array::from(vec![
                    2.0, 2.0, 3.0, 5.0, 5.0,
                ]))), // base
                ColumnarValue::Array(Arc::new(Float64Array::from(vec![
                    8.0, 4.0, 81.0, 625.0, -123.0,
                ]))), // num
            ],
            arg_fields,
            number_rows: 5,
            return_field: Field::new("f", DataType::Float64, true).into(),
        };
        let result = LogFunc::new()
            .invoke_with_args(args)
            .expect("failed to initialize function log");

        match result {
            ColumnarValue::Array(arr) => {
                let floats = as_float64_array(&arr)
                    .expect("failed to convert result to a Float64Array");

                assert_eq!(floats.len(), 5);
                assert!((floats.value(0) - 3.0).abs() < 1e-10);
                assert!((floats.value(1) - 2.0).abs() < 1e-10);
                assert!((floats.value(2) - 4.0).abs() < 1e-10);
                assert!((floats.value(3) - 4.0).abs() < 1e-10);
                assert!(floats.value(4).is_nan());
            }
            ColumnarValue::Scalar(_) => {
                panic!("Expected an array value")
            }
        }
    }

    #[test]
    fn test_log_f32() {
        let arg_fields = vec![
            Field::new("a", DataType::Float32, false).into(),
            Field::new("a", DataType::Float32, false).into(),
        ];
        let args = ScalarFunctionArgs {
            args: vec![
                ColumnarValue::Array(Arc::new(Float32Array::from(vec![
                    2.0, 2.0, 3.0, 5.0,
                ]))), // base
                ColumnarValue::Array(Arc::new(Float32Array::from(vec![
                    8.0, 4.0, 81.0, 625.0,
                ]))), // num
            ],
            arg_fields,
            number_rows: 4,
            return_field: Field::new("f", DataType::Float32, true).into(),
        };
        let result = LogFunc::new()
            .invoke_with_args(args)
            .expect("failed to initialize function log");

        match result {
            ColumnarValue::Array(arr) => {
                let floats = as_float32_array(&arr)
                    .expect("failed to convert result to a Float32Array");

                assert_eq!(floats.len(), 4);
                assert!((floats.value(0) - 3.0).abs() < f32::EPSILON);
                assert!((floats.value(1) - 2.0).abs() < f32::EPSILON);
                assert!((floats.value(2) - 4.0).abs() < f32::EPSILON);
                assert!((floats.value(3) - 4.0).abs() < f32::EPSILON);
            }
            ColumnarValue::Scalar(_) => {
                panic!("Expected an array value")
            }
        }
    }
    #[test]
    // Test log() simplification errors
    fn test_log_simplify_errors() {
        let props = ExecutionProps::new();
        let schema =
            Arc::new(DFSchema::new_with_metadata(vec![], HashMap::new()).unwrap());
        let context = SimplifyContext::new(&props).with_schema(schema);
        // Expect 0 args to error
        let _ = LogFunc::new().simplify(vec![], &context).unwrap_err();
        // Expect 3 args to error
        let _ = LogFunc::new()
            .simplify(vec![lit(1), lit(2), lit(3)], &context)
            .unwrap_err();
    }

    #[test]
    // Test that non-simplifiable log() expressions are unchanged after simplification
    fn test_log_simplify_original() {
        let props = ExecutionProps::new();
        let schema =
            Arc::new(DFSchema::new_with_metadata(vec![], HashMap::new()).unwrap());
        let context = SimplifyContext::new(&props).with_schema(schema);
        // One argument with no simplifications
        let result = LogFunc::new().simplify(vec![lit(2)], &context).unwrap();
        let ExprSimplifyResult::Original(args) = result else {
            panic!("Expected ExprSimplifyResult::Original")
        };
        assert_eq!(args.len(), 1);
        assert_eq!(args[0], lit(2));
        // Two arguments with no simplifications
        let result = LogFunc::new()
            .simplify(vec![lit(2), lit(3)], &context)
            .unwrap();
        let ExprSimplifyResult::Original(args) = result else {
            panic!("Expected ExprSimplifyResult::Original")
        };
        assert_eq!(args.len(), 2);
        assert_eq!(args[0], lit(2));
        assert_eq!(args[1], lit(3));
    }

    #[test]
    fn test_log_output_ordering() {
        // [Unordered, Ascending, Descending, Literal]
        let orders = vec![
            ExprProperties::new_unknown(),
            ExprProperties::new_unknown().with_order(SortProperties::Ordered(
                SortOptions {
                    descending: false,
                    nulls_first: true,
                },
            )),
            ExprProperties::new_unknown().with_order(SortProperties::Ordered(
                SortOptions {
                    descending: true,
                    nulls_first: true,
                },
            )),
            ExprProperties::new_unknown().with_order(SortProperties::Singleton),
        ];

        let log = LogFunc::new();

        // Test log(num)
        for order in orders.iter().cloned() {
            let result = log.output_ordering(&[order.clone()]).unwrap();
            assert_eq!(result, order.sort_properties);
        }

        // Test log(base, num), where `nulls_first` is the same
        let mut results = Vec::with_capacity(orders.len() * orders.len());
        for base_order in orders.iter() {
            for num_order in orders.iter().cloned() {
                let result = log
                    .output_ordering(&[base_order.clone(), num_order])
                    .unwrap();
                results.push(result);
            }
        }
        let expected = vec![
            // base: Unordered
            SortProperties::Unordered,
            SortProperties::Unordered,
            SortProperties::Unordered,
            SortProperties::Unordered,
            // base: Ascending, num: Unordered
            SortProperties::Unordered,
            // base: Ascending, num: Ascending
            SortProperties::Unordered,
            // base: Ascending, num: Descending
            SortProperties::Ordered(SortOptions {
                descending: true,
                nulls_first: true,
            }),
            // base: Ascending, num: Literal
            SortProperties::Ordered(SortOptions {
                descending: true,
                nulls_first: true,
            }),
            // base: Descending, num: Unordered
            SortProperties::Unordered,
            // base: Descending, num: Ascending
            SortProperties::Ordered(SortOptions {
                descending: false,
                nulls_first: true,
            }),
            // base: Descending, num: Descending
            SortProperties::Unordered,
            // base: Descending, num: Literal
            SortProperties::Ordered(SortOptions {
                descending: false,
                nulls_first: true,
            }),
            // base: Literal, num: Unordered
            SortProperties::Unordered,
            // base: Literal, num: Ascending
            SortProperties::Ordered(SortOptions {
                descending: false,
                nulls_first: true,
            }),
            // base: Literal, num: Descending
            SortProperties::Ordered(SortOptions {
                descending: true,
                nulls_first: true,
            }),
            // base: Literal, num: Literal
            SortProperties::Singleton,
        ];
        assert_eq!(results, expected);

        // Test with different `nulls_first`
        let base_order = ExprProperties::new_unknown().with_order(
            SortProperties::Ordered(SortOptions {
                descending: true,
                nulls_first: true,
            }),
        );
        let num_order = ExprProperties::new_unknown().with_order(
            SortProperties::Ordered(SortOptions {
                descending: false,
                nulls_first: false,
            }),
        );
        assert_eq!(
            log.output_ordering(&[base_order, num_order]).unwrap(),
            SortProperties::Unordered
        );
    }

    #[test]
    fn test_log_scalar_decimal128_unary() {
        let arg_field = Field::new("a", Decimal128(38, 0), false).into();
        let args = ScalarFunctionArgs {
            args: vec![
                ColumnarValue::Scalar(ScalarValue::Decimal128(Some(10), 38, 0)), // num
            ],
            arg_fields: vec![arg_field],
            number_rows: 1,
            return_field: Field::new("f", Decimal128(38, 0), true).into(),
        };
        let result = LogFunc::new()
            .invoke_with_args(args)
            .expect("failed to initialize function log");

        match result {
            ColumnarValue::Array(arr) => {
                let floats = as_float64_array(&arr)
                    .expect("failed to convert result to a Decimal128Array");
                assert_eq!(floats.len(), 1);
                assert!((floats.value(0) - 1.0).abs() < 1e-10);
            }
            ColumnarValue::Scalar(_) => {
                panic!("Expected an array value")
            }
        }
    }

    #[test]
    fn test_log_scalar_decimal128() {
        let arg_fields = vec![
            Field::new("b", DataType::Float64, false).into(),
            Field::new("x", Decimal128(38, 0), false).into(),
        ];
        let args = ScalarFunctionArgs {
            args: vec![
                ColumnarValue::Scalar(ScalarValue::Float64(Some(2.0))), // base
                ColumnarValue::Scalar(ScalarValue::Decimal128(Some(64), 38, 0)), // num
            ],
            arg_fields,
            number_rows: 1,
            return_field: Field::new("f", DataType::Float64, true).into(),
        };
        let result = LogFunc::new()
            .invoke_with_args(args)
            .expect("failed to initialize function log");

        match result {
            ColumnarValue::Array(arr) => {
                let floats = as_float64_array(&arr)
                    .expect("failed to convert result to a Float64Array");

                assert_eq!(floats.len(), 1);
                assert!((floats.value(0) - 6.0).abs() < 1e-10);
            }
            ColumnarValue::Scalar(_) => {
                panic!("Expected an array value")
            }
        }
    }

    #[test]
    fn test_log_decimal128_unary() {
        let arg_field = Field::new("a", Decimal128(38, 0), false).into();
        let args = ScalarFunctionArgs {
            args: vec![
                ColumnarValue::Array(Arc::new(Decimal128Array::from(vec![
                    10, 100, 1000, 10000, 12600, -123,
                ]))), // num
            ],
            arg_fields: vec![arg_field],
            number_rows: 6,
            return_field: Field::new("f", DataType::Float64, true).into(),
        };
        let result = LogFunc::new()
            .invoke_with_args(args)
            .expect("failed to initialize function log");

        match result {
            ColumnarValue::Array(arr) => {
                let floats = as_float64_array(&arr)
                    .expect("failed to convert result to a Float64Array");

                assert_eq!(floats.len(), 6);
                assert!((floats.value(0) - 1.0).abs() < 1e-10);
                assert!((floats.value(1) - 2.0).abs() < 1e-10);
                assert!((floats.value(2) - 3.0).abs() < 1e-10);
                assert!((floats.value(3) - 4.0).abs() < 1e-10);
                assert!((floats.value(4) - 4.0).abs() < 1e-10); // Integer rounding
                assert!(floats.value(5).is_nan());
            }
            ColumnarValue::Scalar(_) => {
                panic!("Expected an array value")
            }
        }
    }

    #[test]
    fn test_log_decimal128_base_decimal() {
        // Base stays 2 despite scaling
        for base in [
            ScalarValue::Decimal128(Some(i128::from(2)), 38, 0),
            ScalarValue::Decimal128(Some(i128::from(2000)), 38, 3),
        ] {
            let arg_fields = vec![
                Field::new("b", Decimal128(38, 0), false).into(),
                Field::new("x", Decimal128(38, 0), false).into(),
            ];
            let args = ScalarFunctionArgs {
                args: vec![
                    ColumnarValue::Scalar(base), // base
                    ColumnarValue::Scalar(ScalarValue::Decimal128(Some(64), 38, 0)), // num
                ],
                arg_fields,
                number_rows: 1,
                return_field: Field::new("f", DataType::Float64, true).into(),
            };
            let result = LogFunc::new()
                .invoke_with_args(args)
                .expect("failed to initialize function log");

            match result {
                ColumnarValue::Array(arr) => {
                    let floats = as_float64_array(&arr)
                        .expect("failed to convert result to a Float64Array");

                    assert_eq!(floats.len(), 1);
                    assert!((floats.value(0) - 6.0).abs() < 1e-10);
                }
                ColumnarValue::Scalar(_) => {
                    panic!("Expected an array value")
                }
            }
        }
    }

    #[test]
    fn test_log_decimal256_unary() {
        let arg_field = Field::new("a", Decimal256(38, 0), false).into();
        let args = ScalarFunctionArgs {
            args: vec![
                ColumnarValue::Array(Arc::new(Decimal256Array::from(vec![
                    Some(i256::from(10)),
                    Some(i256::from(100)),
                    Some(i256::from(1000)),
                    Some(i256::from(10000)),
                    Some(i256::from(12600)),
                    // Slightly lower than i128 max - can calculate
                    Some(i256::from_i128(i128::MAX) - i256::from(1000)),
                    // Give NaN for incorrect inputs, as in f64::log
                    Some(i256::from(-123)),
                ]))), // num
            ],
            arg_fields: vec![arg_field],
            number_rows: 7,
            return_field: Field::new("f", DataType::Float64, true).into(),
        };
        let result = LogFunc::new()
            .invoke_with_args(args)
            .expect("failed to initialize function log");

        match result {
            ColumnarValue::Array(arr) => {
                let floats = as_float64_array(&arr)
                    .expect("failed to convert result to a Float64Array");

                assert_eq!(floats.len(), 7);
                eprintln!("floats {:?}", &floats);
                assert!((floats.value(0) - 1.0).abs() < 1e-10);
                assert!((floats.value(1) - 2.0).abs() < 1e-10);
                assert!((floats.value(2) - 3.0).abs() < 1e-10);
                assert!((floats.value(3) - 4.0).abs() < 1e-10);
                assert!((floats.value(4) - 4.0).abs() < 1e-10); // Integer rounding for float log
                assert!((floats.value(5) - 38.0).abs() < 1e-10);
                assert!(floats.value(6).is_nan());
            }
            ColumnarValue::Scalar(_) => {
                panic!("Expected an array value")
            }
        }
    }

    #[test]
    fn test_log_decimal128_wrong_base() {
        let arg_fields = vec![
            Field::new("b", DataType::Float64, false).into(),
            Field::new("x", Decimal128(38, 0), false).into(),
        ];
        let args = ScalarFunctionArgs {
            args: vec![
                ColumnarValue::Scalar(ScalarValue::Float64(Some(-2.0))), // base
                ColumnarValue::Scalar(ScalarValue::Decimal128(Some(64), 38, 0)), // num
            ],
            arg_fields,
            number_rows: 1,
            return_field: Field::new("f", DataType::Float64, true).into(),
        };
        let result = LogFunc::new().invoke_with_args(args);
        assert!(result.is_err());
        assert_eq!(
            "Arrow error: Compute error: Log cannot use non-integer or small base -2",
            result.unwrap_err().to_string()
        );
    }

    #[test]
    fn test_log_decimal256_error() {
        let arg_field = Field::new("a", Decimal256(38, 0), false).into();
        let args = ScalarFunctionArgs {
            args: vec![
                ColumnarValue::Array(Arc::new(Decimal256Array::from(vec![
                    // Slightly larger than i128
                    Some(i256::from_i128(i128::MAX) + i256::from(1000)),
                ]))), // num
            ],
            arg_fields: vec![arg_field],
            number_rows: 1,
            return_field: Field::new("f", DataType::Float64, true).into(),
        };
        let result = LogFunc::new().invoke_with_args(args);
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().starts_with(
            "Arrow error: Compute error: Log of a large Decimal256 is not supported"
        ));
    }
}
