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

//! Math function: `power()`.
use std::any::Any;
use std::sync::Arc;

use super::log::LogFunc;

use crate::utils::{calculate_binary_math, decimal128_to_i128, decimal256_to_i256};
use arrow::array::{Array, ArrayRef, PrimitiveArray};
use arrow::datatypes::{
    ArrowNativeTypeOp, DataType, Decimal128Type, Decimal256Type, DecimalType,
    Float32Type, Float64Type, Int32Type, Int64Type,
};
use arrow::error::ArrowError;
use arrow_buffer::i256;
use datafusion_common::{exec_err, plan_datafusion_err, plan_err, Result, ScalarValue};
use datafusion_expr::expr::ScalarFunction;
use datafusion_expr::simplify::{ExprSimplifyResult, SimplifyInfo};
use datafusion_expr::{
    ColumnarValue, Documentation, Expr, ScalarFunctionArgs, ScalarUDF, TypeSignature,
};
use datafusion_expr::{ScalarUDFImpl, Signature, Volatility};
use datafusion_expr_common::signature::TypeSignature::Numeric;
use datafusion_macros::user_doc;

#[user_doc(
    doc_section(label = "Math Functions"),
    description = "Returns a base expression raised to the power of an exponent.",
    syntax_example = "power(base, exponent)",
    sql_example = r#"```sql
> SELECT power(2, 3);
+-------------+
| power(2,3)  |
+-------------+
| 8           |
+-------------+
```"#,
    standard_argument(name = "base", prefix = "Numeric"),
    standard_argument(name = "exponent", prefix = "Exponent numeric")
)]
#[derive(Debug, PartialEq, Eq, Hash)]
pub struct PowerFunc {
    signature: Signature,
    aliases: Vec<String>,
}

impl Default for PowerFunc {
    fn default() -> Self {
        Self::new()
    }
}

impl PowerFunc {
    pub fn new() -> Self {
        use DataType::*;
        Self {
            signature: Signature::one_of(
                vec![
                    TypeSignature::Exact(vec![Int32, Int32]),
                    TypeSignature::Exact(vec![Int64, Int64]),
                    TypeSignature::Exact(vec![Float32, Float32]),
                    TypeSignature::Exact(vec![Float64, Float64]),
                    // Extra signatures for decimals to avoid casting them to floats
                    TypeSignature::Exact(vec![
                        Decimal128(DECIMAL128_MAX_PRECISION, 0),
                        Int64,
                    ]),
                    TypeSignature::Exact(vec![
                        Decimal128(DECIMAL128_MAX_PRECISION, 0),
                        Float64,
                    ]),
                    Numeric(2), // Catch-all for all decimals
                ],
                Volatility::Immutable,
            ),
            aliases: vec![String::from("pow")],
        }
    }
}

fn pow_decimal128_helper(base: i128, scale: i8, exp: u32) -> Result<i128, ArrowError> {
    decimal128_to_i128(base, scale)?.pow_checked(exp)
}

/// Binary function to calculate a math power
/// Returns error if base is invalid
fn pow_decimal128_float(base: i128, scale: i8, exp: f64) -> Result<i128, ArrowError> {
    if !exp.is_finite() || exp.trunc() != exp {
        return Err(ArrowError::ComputeError(format!(
            "Cannot use non-integer exp: {exp}"
        )));
    }
    if exp < 0f64 || exp >= u32::MAX as f64 {
        return Err(ArrowError::ArithmeticOverflow(format!(
            "Unsupported exp value: {exp}"
        )));
    }

    pow_decimal128_helper(base, scale, exp as u32)
}

/// Binary function to calculate a math power
fn pow_decimal128_int(base: i128, scale: i8, exp: i64) -> Result<i128, ArrowError> {
    match exp.try_into() {
        Ok(exp) => pow_decimal128_helper(base, scale, exp),
        Err(_) => Err(ArrowError::ArithmeticOverflow(format!(
            "Cannot use non-positive exp: {exp}"
        ))),
    }
}

fn pow_decimal256_helper(base: i256, scale: i8, exp: u32) -> Result<i256, ArrowError> {
    decimal256_to_i256(base, scale)?.pow_checked(exp)
}

/// Binary function to calculate a math power
/// Returns error if base is invalid
fn pow_decimal256_float(base: i256, scale: i8, exp: f64) -> Result<i256, ArrowError> {
    if !exp.is_finite() || exp.trunc() != exp {
        return Err(ArrowError::ComputeError(format!(
            "Cannot use non-integer exp: {exp}"
        )));
    }
    if exp < 0f64 || exp >= u32::MAX as f64 {
        return Err(ArrowError::ArithmeticOverflow(format!(
            "Unsupported exp value: {exp}"
        )));
    }

    pow_decimal256_helper(base, scale, exp as u32)
}

/// Binary function to calculate a math power
/// Returns error if base is invalid
fn pow_decimal256_int(base: i256, scale: i8, exp: i64) -> Result<i256, ArrowError> {
    match exp.try_into() {
        Ok(exp) => pow_decimal256_helper(base, scale, exp),
        Err(_) => Err(ArrowError::ArithmeticOverflow(format!(
            "Unsupported exp value: {exp}"
        ))),
    }
}

/// Change scale of decimal array to 0 instead of default scale 10
/// It is required to reset scale for decimals. automatically constructed from
/// Apache Arrow operations on i128/i256 type
fn reset_decimal_scale<T>(array: &PrimitiveArray<T>) -> Result<PrimitiveArray<T>>
where
    T: DecimalType,
{
    let precision = array.precision();
    Ok(array.clone().with_precision_and_scale(precision, 0)?)
}

impl ScalarUDFImpl for PowerFunc {
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn name(&self) -> &str {
        "power"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        match arg_types[0] {
            DataType::Int32 => Ok(DataType::Int32),
            DataType::Int64 => Ok(DataType::Int64),
            DataType::Float32 => Ok(DataType::Float32),
            DataType::Float64 => Ok(DataType::Float64),
            DataType::Decimal128(precision, scale) => {
                Ok(DataType::Decimal128(precision, scale))
            }
            DataType::Decimal256(precision, scale) => {
                Ok(DataType::Decimal256(precision, scale))
            }
            _ => plan_err!(
                "Unsupported arguments {arg_types:?} for function {}",
                self.name()
            ),
        }
    }

    fn aliases(&self) -> &[String] {
        &self.aliases
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let args = ColumnarValue::values_to_arrays(&args.args)?;

        let base = &args[0];
        let exponent = ColumnarValue::Array(Arc::clone(&args[1]));

        let arr: ArrayRef = match base.data_type() {
            DataType::Float32 => {
                calculate_binary_math::<Float32Type, Float32Type, Float32Type, _>(
                    base,
                    &exponent,
                    |b, e| Ok(f32::powf(b, e)),
                )?
            }
            DataType::Float64 => {
                calculate_binary_math::<Float64Type, Float64Type, Float64Type, _>(
                    &base,
                    &exponent,
                    |b, e| Ok(f64::powf(b, e)),
                )?
            }
            DataType::Int32 => {
                calculate_binary_math::<Int32Type, Int32Type, Int32Type, _>(
                    &base,
                    &exponent,
                    |b, e| match e.try_into() {
                        Ok(exp_u32) => b.pow_checked(exp_u32),
                        Err(_) => Err(ArrowError::ArithmeticOverflow(format!(
                            "Exponent {e} in integer computation is out of bounds."
                        ))),
                    },
                )?
            }
            DataType::Int64 => {
                calculate_binary_math::<Int64Type, Int64Type, Int64Type, _>(
                    &base,
                    &exponent,
                    |b, e| match e.try_into() {
                        Ok(exp_u32) => b.pow_checked(exp_u32),
                        Err(_) => Err(ArrowError::ArithmeticOverflow(format!(
                            "Exponent {e} in integer computation is out of bounds."
                        ))),
                    },
                )?
            }
            DataType::Decimal128(_precision, scale) => {
                let array = match exponent.data_type() {
                    DataType::Int64 => {
                        calculate_binary_math::<
                            Decimal128Type,
                            Int64Type,
                            Decimal128Type,
                            _,
                        >(&base, &exponent, |b, e| {
                            pow_decimal128_int(b, *scale, e)
                        })?
                    }
                    DataType::Float64 => {
                        calculate_binary_math::<
                            Decimal128Type,
                            Float64Type,
                            Decimal128Type,
                            _,
                        >(&base, &exponent, |b, e| {
                            pow_decimal128_float(b, *scale, e)
                        })?
                    }
                    other => {
                        return exec_err!("Unsupported data type {other:?} for exponent")
                    }
                };
                Arc::new(reset_decimal_scale(array.as_ref())?)
            }
            DataType::Decimal256(_precision, scale) => {
                let array = match exponent.data_type() {
                    DataType::Int64 => {
                        calculate_binary_math::<
                            Decimal256Type,
                            Int64Type,
                            Decimal256Type,
                            _,
                        >(&base, &exponent, |b, e| {
                            pow_decimal256_int(b, *scale, e)
                        })?
                    }
                    DataType::Float64 => {
                        calculate_binary_math::<
                            Decimal256Type,
                            Float64Type,
                            Decimal256Type,
                            _,
                        >(&base, &exponent, |b, e| {
                            pow_decimal256_float(b, *scale, e)
                        })?
                    }
                    other => {
                        return exec_err!("Unsupported data type {other:?} for exponent")
                    }
                };
                Arc::new(reset_decimal_scale(array.as_ref())?)
            }
            other => {
                return exec_err!(
                    "Unsupported data type {other:?} for function {}",
                    self.name()
                )
            }
        };

        Ok(ColumnarValue::Array(arr))
    }

    /// Simplify the `power` function by the relevant rules:
    /// 1. Power(a, 0) ===> 0
    /// 2. Power(a, 1) ===> a
    /// 3. Power(a, Log(a, b)) ===> b
    fn simplify(
        &self,
        mut args: Vec<Expr>,
        info: &dyn SimplifyInfo,
    ) -> Result<ExprSimplifyResult> {
        let exponent = args.pop().ok_or_else(|| {
            plan_datafusion_err!("Expected power to have 2 arguments, got 0")
        })?;
        let base = args.pop().ok_or_else(|| {
            plan_datafusion_err!("Expected power to have 2 arguments, got 1")
        })?;

        let exponent_type = info.get_data_type(&exponent)?;
        match exponent {
            Expr::Literal(value, _)
                if value == ScalarValue::new_zero(&exponent_type)? =>
            {
                Ok(ExprSimplifyResult::Simplified(Expr::Literal(
                    ScalarValue::new_one(&info.get_data_type(&base)?)?,
                    None,
                )))
            }
            Expr::Literal(value, _) if value == ScalarValue::new_one(&exponent_type)? => {
                Ok(ExprSimplifyResult::Simplified(base))
            }
            Expr::ScalarFunction(ScalarFunction { func, mut args })
                if is_log(&func) && args.len() == 2 && base == args[0] =>
            {
                let b = args.pop().unwrap(); // length checked above
                Ok(ExprSimplifyResult::Simplified(b))
            }
            _ => Ok(ExprSimplifyResult::Original(vec![base, exponent])),
        }
    }

    fn documentation(&self) -> Option<&Documentation> {
        self.doc()
    }
}

/// Return true if this function call is a call to `Log`
fn is_log(func: &ScalarUDF) -> bool {
    func.inner().as_any().downcast_ref::<LogFunc>().is_some()
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Array, Decimal128Array, Float64Array, Int64Array};
    use arrow::datatypes::{Field, DECIMAL128_MAX_SCALE, DECIMAL256_MAX_SCALE};
    use datafusion_common::cast::{
        as_decimal128_array, as_decimal256_array, as_float64_array, as_int64_array,
    };
    use datafusion_common::config::ConfigOptions;

    #[test]
    fn test_power_f64() {
        let arg_fields = vec![
            Field::new("a", DataType::Float64, true).into(),
            Field::new("a", DataType::Float64, true).into(),
        ];
        let args = ScalarFunctionArgs {
            args: vec![
                ColumnarValue::Array(Arc::new(Float64Array::from(vec![
                    2.0, 2.0, 3.0, 5.0,
                ]))), // base
                ColumnarValue::Array(Arc::new(Float64Array::from(vec![
                    3.0, 2.0, 4.0, 4.0,
                ]))), // exponent
            ],
            arg_fields,
            number_rows: 4,
            return_field: Field::new("f", DataType::Float64, true).into(),
            config_options: Arc::new(ConfigOptions::default()),
        };
        let result = PowerFunc::new()
            .invoke_with_args(args)
            .expect("failed to initialize function power");

        match result {
            ColumnarValue::Array(arr) => {
                let floats = as_float64_array(&arr)
                    .expect("failed to convert result to a Float64Array");
                assert_eq!(floats.len(), 4);
                assert_eq!(floats.value(0), 8.0);
                assert_eq!(floats.value(1), 4.0);
                assert_eq!(floats.value(2), 81.0);
                assert_eq!(floats.value(3), 625.0);
            }
            ColumnarValue::Scalar(_) => {
                panic!("Expected an array value")
            }
        }
    }

    #[test]
    fn test_power_i64() {
        let arg_fields = vec![
            Field::new("a", DataType::Int64, true).into(),
            Field::new("a", DataType::Int64, true).into(),
        ];
        let args = ScalarFunctionArgs {
            args: vec![
                ColumnarValue::Array(Arc::new(Int64Array::from(vec![2, 2, 3, 5]))), // base
                ColumnarValue::Array(Arc::new(Int64Array::from(vec![3, 2, 4, 4]))), // exponent
            ],
            arg_fields,
            number_rows: 4,
            return_field: Field::new("f", DataType::Int64, true).into(),
            config_options: Arc::new(ConfigOptions::default()),
        };
        let result = PowerFunc::new()
            .invoke_with_args(args)
            .expect("failed to initialize function power");

        match result {
            ColumnarValue::Array(arr) => {
                let ints = as_int64_array(&arr)
                    .expect("failed to convert result to a Int64Array");

                assert_eq!(ints.len(), 4);
                assert_eq!(ints.value(0), 8);
                assert_eq!(ints.value(1), 4);
                assert_eq!(ints.value(2), 81);
                assert_eq!(ints.value(3), 625);
            }
            ColumnarValue::Scalar(_) => {
                panic!("Expected an array value")
            }
        }
    }

    #[test]
    fn test_power_i128_exp_int() {
        let arg_fields = vec![
            Field::new(
                "a",
                DataType::Decimal128(DECIMAL128_MAX_SCALE as u8, 0),
                true,
            )
            .into(),
            Field::new("a", DataType::Int64, true).into(),
        ];
        let args = ScalarFunctionArgs {
            args: vec![
                ColumnarValue::Array(Arc::new(
                    Decimal128Array::from(vec![2, 2, 3, 5, 0, 5])
                        .with_precision_and_scale(DECIMAL128_MAX_SCALE as u8, 0)
                        .unwrap(),
                )), // base
                ColumnarValue::Array(Arc::new(Int64Array::from(vec![3, 2, 4, 4, 4, 0]))), // exponent
            ],
            arg_fields,
            number_rows: 6,
            return_field: Field::new(
                "f",
                DataType::Decimal128(DECIMAL128_MAX_SCALE as u8, 0),
                true,
            )
            .into(),
            config_options: Arc::new(ConfigOptions::default()),
        };
        let result = PowerFunc::new()
            .invoke_with_args(args)
            .expect("failed to initialize function power");

        match result {
            ColumnarValue::Array(arr) => {
                let ints = as_decimal128_array(&arr)
                    .expect("failed to convert result to an array");

                assert_eq!(ints.len(), 6);
                assert_eq!(ints.value(0), i128::from(8));
                assert_eq!(ints.value(1), i128::from(4));
                assert_eq!(ints.value(2), i128::from(81));
                assert_eq!(ints.value(3), i128::from(625));
                assert_eq!(ints.value(4), i128::from(0));
                assert_eq!(ints.value(5), i128::from(1));
            }
            ColumnarValue::Scalar(_) => {
                panic!("Expected an array value")
            }
        }
    }

    #[test]
    fn test_power_i128_exp_int_scalar() {
        let arg_fields = vec![
            Field::new(
                "a",
                DataType::Decimal128(DECIMAL128_MAX_SCALE as u8, 0),
                true,
            )
            .into(),
            Field::new("a", DataType::Int64, true).into(),
        ];
        let args = ScalarFunctionArgs {
            args: vec![
                ColumnarValue::Scalar(ScalarValue::Decimal128(
                    Some(2),
                    DECIMAL128_MAX_SCALE as u8,
                    0,
                )), // base
                ColumnarValue::Scalar(ScalarValue::Int64(Some(3))), // exponent
            ],
            arg_fields,
            number_rows: 1,
            return_field: Field::new(
                "f",
                DataType::Decimal128(DECIMAL128_MAX_SCALE as u8, 0),
                true,
            )
            .into(),
            config_options: Arc::new(ConfigOptions::default()),
        };
        let result = PowerFunc::new()
            .invoke_with_args(args)
            .expect("failed to initialize function power");

        match result {
            ColumnarValue::Array(arr) => {
                let ints = as_decimal128_array(&arr)
                    .expect("failed to convert result to an array");

                assert_eq!(ints.len(), 1);
                assert_eq!(ints.value(0), i128::from(8));

                // Value is the same as expected, but scale should be 0
                if let DataType::Decimal128(_precision, scale) = arr.data_type() {
                    assert_eq!(*scale, 0);
                } else {
                    panic!("Expected Decimal256 result")
                }
            }
            ColumnarValue::Scalar(_) => {
                panic!("Expected an array value")
            }
        }
    }

    #[test]
    fn test_power_i128_exp_float() {
        let arg_fields = vec![
            Field::new(
                "a",
                DataType::Decimal128(DECIMAL128_MAX_SCALE as u8, 0),
                true,
            )
            .into(),
            Field::new("a", DataType::Float64, true).into(),
        ];
        let args = ScalarFunctionArgs {
            args: vec![
                ColumnarValue::Array(Arc::new(
                    Decimal128Array::from(vec![2, 2, 3, 5, 0, 5])
                        .with_precision_and_scale(DECIMAL128_MAX_SCALE as u8, 0)
                        .unwrap(),
                )), // base
                ColumnarValue::Array(Arc::new(Float64Array::from(vec![
                    3.0, 2.0, 4.0, 4.0, 4.0, 0.0,
                ]))), // exponent
            ],
            arg_fields,
            number_rows: 6,
            return_field: Field::new(
                "f",
                DataType::Decimal128(DECIMAL128_MAX_SCALE as u8, 0),
                true,
            )
            .into(),
            config_options: Arc::new(ConfigOptions::default()),
        };
        let result = PowerFunc::new()
            .invoke_with_args(args)
            .expect("failed to initialize function power");

        match result {
            ColumnarValue::Array(arr) => {
                let ints = as_decimal128_array(&arr)
                    .expect("failed to convert result to an array");

                assert_eq!(ints.len(), 6);
                assert_eq!(ints.value(0), i128::from(8));
                assert_eq!(ints.value(1), i128::from(4));
                assert_eq!(ints.value(2), i128::from(81));
                assert_eq!(ints.value(3), i128::from(625));
                assert_eq!(ints.value(4), i128::from(0));
                assert_eq!(ints.value(5), i128::from(1));
            }
            ColumnarValue::Scalar(_) => {
                panic!("Expected an array value")
            }
        }
    }

    #[test]
    fn test_power_i128_exp_float_fail() {
        let bad_exponents = [
            3.5,
            -1.0,
            u32::MAX as f64 + 10.0,
            f64::INFINITY,
            -f64::INFINITY,
            f64::NAN,
        ];
        for exp in bad_exponents {
            let arg_fields = vec![
                Field::new(
                    "a",
                    DataType::Decimal128(DECIMAL128_MAX_SCALE as u8, 0),
                    true,
                )
                .into(),
                Field::new("a", DataType::Float64, true).into(),
            ];
            let args = ScalarFunctionArgs {
                args: vec![
                    ColumnarValue::Scalar(ScalarValue::Decimal128(Some(2), 38, 0)), // base
                    ColumnarValue::Scalar(ScalarValue::Float64(Some(exp))), // exponent
                ],
                arg_fields,
                number_rows: 1,
                return_field: Field::new(
                    "f",
                    DataType::Decimal128(DECIMAL128_MAX_SCALE as u8, 0),
                    true,
                )
                .into(),
                config_options: Arc::new(ConfigOptions::default()),
            };
            let result = PowerFunc::new().invoke_with_args(args);
            assert!(result.is_err());
        }
    }

    #[test]
    fn test_power_i256_exp_int_scalar() {
        let arg_fields = vec![
            Field::new(
                "a",
                DataType::Decimal256(DECIMAL256_MAX_SCALE as u8, 0),
                true,
            )
            .into(),
            Field::new("a", DataType::Int64, true).into(),
        ];
        let args = ScalarFunctionArgs {
            args: vec![
                ColumnarValue::Scalar(ScalarValue::Decimal256(
                    Some(i256::from(2)),
                    DECIMAL256_MAX_SCALE as u8,
                    0,
                )), // base
                ColumnarValue::Scalar(ScalarValue::Int64(Some(3))), // exponent
            ],
            arg_fields,
            number_rows: 1,
            return_field: Field::new(
                "f",
                DataType::Decimal256(DECIMAL256_MAX_SCALE as u8, 0),
                true,
            )
            .into(),
            config_options: Arc::new(ConfigOptions::default()),
        };
        let result = PowerFunc::new()
            .invoke_with_args(args)
            .expect("failed to initialize function power");

        match result {
            ColumnarValue::Array(arr) => {
                let ints = as_decimal256_array(&arr)
                    .expect("failed to convert result to an array");

                assert_eq!(ints.len(), 1);
                assert_eq!(ints.value(0), i256::from(8));

                // Value is the same as expected, but scale should be 0
                if let DataType::Decimal256(_precision, scale) = arr.data_type() {
                    assert_eq!(*scale, 0);
                } else {
                    panic!("Expected Decimal256 result")
                }
            }
            ColumnarValue::Scalar(_) => {
                panic!("Expected an array value")
            }
        }
    }
}
