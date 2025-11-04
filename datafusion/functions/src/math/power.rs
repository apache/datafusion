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

use crate::utils::calculate_binary_math;
use arrow::array::{Array, ArrayRef, PrimitiveArray};
use arrow::datatypes::{
    ArrowNativeTypeOp, DataType, Decimal128Type, Decimal256Type, Decimal32Type,
    Decimal64Type, DecimalType, Float64Type, Int64Type,
};
use arrow::error::ArrowError;
use arrow_buffer::i256;
use datafusion_common::utils::take_function_args;
use datafusion_common::{exec_err, not_impl_err, plan_datafusion_err, Result, ScalarValue};
use datafusion_expr::expr::ScalarFunction;
use datafusion_expr::simplify::{ExprSimplifyResult, SimplifyInfo};
use datafusion_expr::type_coercion::is_decimal;
use datafusion_expr::{
    ColumnarValue, Documentation, Expr, ScalarFunctionArgs, ScalarUDF,
};
use datafusion_expr::{ScalarUDFImpl, Signature, Volatility};
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
        Self {
            signature: Signature::user_defined(Volatility::Immutable),
            aliases: vec![String::from("pow")],
        }
    }
}

macro_rules! make_pow_fn {
    ($t:ty, $name_int:ident, $name_float:ident) => {
        /// Binary function to calculate a math power to integer exponent
        /// for scaled integer types.
        ///
        /// Formula
        /// The power for a scaled integer `b` is
        ///
        /// ```text
        /// (b * 10^(-s)) ^ e
        /// ```
        /// However, the result should be scaled back from scale 0 to scale `s`,
        /// which is done by multiplying by `10^s`.
        /// At the end, the formula is:
        ///
        /// ```text
        ///   b^e * 10^(-s * e) * 10^s = b^e / 10^(s * (e-1))
        /// ```
        /// Example of 2.5 ^ 4 = 39:
        ///   2.5 is represented as 25 with scale 1
        ///   The unscaled result is 25^4 = 390625
        ///   Scale it back to 1: 390625 / 10^4 = 39
        ///
        /// Returns error if base is invalid
        fn $name_int(base: $t, scale: i8, exp: i64) -> Result<$t, ArrowError> {
            let scale: u32 = scale.try_into().map_err(|_| {
                ArrowError::NotYetImplemented(format!(
                    "Negative scale is not yet supported value: {scale}"
                ))
            })?;
            if exp == 0 {
                // Edge case to provide 1 as result (10^s with scale)
                let result: $t = <$t>::from(10).checked_pow(scale).ok_or(
                    ArrowError::ArithmeticOverflow(format!(
                        "Cannot make unscale factor for {scale} and {exp}"
                    )),
                )?;
                return Ok(result);
            }
            let exp: u32 = exp.try_into().map_err(|_| {
                ArrowError::ArithmeticOverflow(format!("Unsupported exp value: {exp}"))
            })?;
            let powered: $t = base.pow_checked(exp).map_err(|_| {
                ArrowError::ArithmeticOverflow(format!(
                    "Cannot raise base {base} to exp {exp}"
                ))
            })?;
            let unscale_factor: $t = <$t>::from(10)
                .checked_pow(scale * (exp - 1))
                .ok_or(ArrowError::ArithmeticOverflow(format!(
                    "Cannot make unscale factor for {scale} and {exp}"
                )))?;

            powered
                .checked_div(unscale_factor)
                .ok_or(ArrowError::ArithmeticOverflow(format!(
                    "Cannot divide in power"
                )))
        }

        /// Binary function to calculate a math power to float exponent
        /// for scaled integer types.
        /// Returns error if exponent is negative or non-integer
        fn $name_float(base: $t, scale: i8, exp: f64) -> Result<$t, ArrowError> {
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
            $name_int(base, scale, exp as i64)
        }
    };
}

// Generate functions for numeric types
make_pow_fn!(i32, pow_decimal32_int, pow_decimal32_float);
make_pow_fn!(i64, pow_decimal64_int, pow_decimal64_float);
make_pow_fn!(i128, pow_decimal128_int, pow_decimal128_float);
make_pow_fn!(i256, pow_decimal256_int, pow_decimal256_float);

/// Helper function to set precision and scale on a decimal array
fn rescale_decimal<T>(
    array: Arc<PrimitiveArray<T>>,
    precision: u8,
    scale: i8,
) -> Result<Arc<PrimitiveArray<T>>>
where
    T: DecimalType,
{
    if scale < 0 {
        return not_impl_err!("Negative scale is not supported for power for decimal types");
    }
    Ok(Arc::new(
        array
            .as_ref()
            .clone()
            .with_precision_and_scale(precision, scale)?,
    ))
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
        Ok(arg_types[0].clone())
    }

    fn aliases(&self) -> &[String] {
        &self.aliases
    }

    fn coerce_types(&self, arg_types: &[DataType]) -> Result<Vec<DataType>> {
        let [arg1, arg2] = take_function_args(self.name(), arg_types)?;

        fn coerced_type_base(name: &str, data_type: &DataType) -> Result<DataType> {
            match data_type {
                DataType::Null => Ok(DataType::Int64),
                d if d.is_floating() => Ok(DataType::Float64),
                d if d.is_integer() => Ok(DataType::Int64),
                d if is_decimal(d) => Ok(d.clone()),
                other => {
                    exec_err!("Unsupported data type {other:?} for {} function", name)
                }
            }
        }

        fn coerced_type_exp(name: &str, data_type: &DataType) -> Result<DataType> {
            match data_type {
                DataType::Null => Ok(DataType::Int64),
                d if d.is_floating() => Ok(DataType::Float64),
                d if d.is_integer() => Ok(DataType::Int64),
                d if is_decimal(d) => Ok(DataType::Float64),
                other => {
                    exec_err!("Unsupported data type {other:?} for {} function", name)
                }
            }
        }

        Ok(vec![
            coerced_type_base(self.name(), arg1)?,
            coerced_type_exp(self.name(), arg2)?,
        ])
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let base = &args.args[0].to_array(args.number_rows)?;
        let exponent = &args.args[1];

        let arr: ArrayRef = match base.data_type() {
            DataType::Float64 => {
                calculate_binary_math::<Float64Type, Float64Type, Float64Type, _>(
                    &base,
                    exponent,
                    |b, e| Ok(f64::powf(b, e)),
                )?
            }
            DataType::Int64 => {
                calculate_binary_math::<Int64Type, Int64Type, Int64Type, _>(
                    &base,
                    exponent,
                    |b, e| match e.try_into() {
                        Ok(exp_u32) => b.pow_checked(exp_u32),
                        Err(_) => Err(ArrowError::ArithmeticOverflow(format!(
                            "Exponent {e} in integer computation is out of bounds."
                        ))),
                    },
                )?
            }
            DataType::Decimal32(precision, scale) => match exponent.data_type() {
                DataType::Int64 => rescale_decimal(
                    calculate_binary_math::<Decimal32Type, Int64Type, Decimal32Type, _>(
                        &base,
                        exponent,
                        |b, e| pow_decimal32_int(b, *scale, e),
                    )?,
                    *precision,
                    *scale,
                )?,
                DataType::Float64 => rescale_decimal(
                    calculate_binary_math::<Decimal32Type, Float64Type, Decimal32Type, _>(
                        &base,
                        exponent,
                        |b, e| pow_decimal32_float(b, *scale, e),
                    )?,
                    *precision,
                    *scale,
                )?,
                other => {
                    return exec_err!("Unsupported data type {other:?} for exponent")
                }
            },
            DataType::Decimal64(precision, scale) => match exponent.data_type() {
                DataType::Int64 => rescale_decimal(
                    calculate_binary_math::<Decimal64Type, Int64Type, Decimal64Type, _>(
                        &base,
                        exponent,
                        |b, e| pow_decimal64_int(b, *scale, e),
                    )?,
                    *precision,
                    *scale,
                )?,
                DataType::Float64 => rescale_decimal(
                    calculate_binary_math::<Decimal64Type, Float64Type, Decimal64Type, _>(
                        &base,
                        exponent,
                        |b, e| pow_decimal64_float(b, *scale, e),
                    )?,
                    *precision,
                    *scale,
                )?,
                other => {
                    return exec_err!("Unsupported data type {other:?} for exponent")
                }
            },
            DataType::Decimal128(precision, scale) => match exponent.data_type() {
                DataType::Int64 => rescale_decimal(
                    calculate_binary_math::<Decimal128Type, Int64Type, Decimal128Type, _>(
                        &base,
                        exponent,
                        |b, e| pow_decimal128_int(b, *scale, e),
                    )?,
                    *precision,
                    *scale,
                )?,
                DataType::Float64 => rescale_decimal(
                    calculate_binary_math::<
                        Decimal128Type,
                        Float64Type,
                        Decimal128Type,
                        _,
                    >(&base, exponent, |b, e| {
                        pow_decimal128_float(b, *scale, e)
                    })?,
                    *precision,
                    *scale,
                )?,
                other => {
                    return exec_err!("Unsupported data type {other:?} for exponent")
                }
            },
            DataType::Decimal256(precision, scale) => match exponent.data_type() {
                DataType::Int64 => rescale_decimal(
                    calculate_binary_math::<Decimal256Type, Int64Type, Decimal256Type, _>(
                        &base,
                        exponent,
                        |b, e| pow_decimal256_int(b, *scale, e),
                    )?,
                    *precision,
                    *scale,
                )?,
                DataType::Float64 => rescale_decimal(
                    calculate_binary_math::<
                        Decimal256Type,
                        Float64Type,
                        Decimal256Type,
                        _,
                    >(&base, exponent, |b, e| {
                        pow_decimal256_float(b, *scale, e)
                    })?,
                    *precision,
                    *scale,
                )?,

                other => {
                    return exec_err!("Unsupported data type {other:?} for exponent")
                }
            },
            other => {
                return exec_err!(
                    "Unsupported base data type {other:?} for function {}",
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
    use arrow::datatypes::{Field, DECIMAL128_MAX_SCALE};
    use arrow_buffer::NullBuffer;
    use datafusion_common::cast::{
        as_decimal128_array, as_float64_array, as_int64_array,
    };
    use datafusion_common::config::ConfigOptions;

    #[cfg(test)]
    #[ctor::ctor]
    fn init() {
        // Enable RUST_LOG logging configuration for test
        let _ = env_logger::try_init();
    }

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
    fn test_power_i128() {
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
    fn test_power_array_null() {
        let arg_fields = vec![
            Field::new("a", DataType::Int64, true).into(),
            Field::new("a", DataType::Int64, true).into(),
        ];
        let args = ScalarFunctionArgs {
            args: vec![
                ColumnarValue::Array(Arc::new(Int64Array::from(vec![2, 2, 2]))), // base
                ColumnarValue::Array(Arc::new(Int64Array::from_iter_values_with_nulls(
                    vec![1, 2, 3],
                    Some(NullBuffer::from(vec![true, false, true])),
                ))), // exponent
            ],
            arg_fields,
            number_rows: 1,
            return_field: Field::new("f", DataType::Int64, true).into(),
            config_options: Arc::new(ConfigOptions::default()),
        };
        let result = PowerFunc::new()
            .invoke_with_args(args)
            .expect("failed to initialize function power");

        match result {
            ColumnarValue::Array(arr) => {
                let ints =
                    as_int64_array(&arr).expect("failed to convert result to an array");

                assert_eq!(ints.len(), 3);
                assert!(!ints.is_null(0));
                assert_eq!(ints.value(0), i64::from(2));
                assert!(ints.is_null(1));
                assert!(!ints.is_null(2));
                assert_eq!(ints.value(2), i64::from(8));
            }
            ColumnarValue::Scalar(_) => {
                panic!("Expected an array value")
            }
        }
    }

    #[test]
    fn test_power_decimal_with_scale() {
        // 2.5 ^ 4 = 39
        // 2.5 is 25 in Decimal128(2, 1) by parsing rules
        // Signature is Decimal128(2, 1) -> Int64 -> Decimal128(2, 1), therefore
        // result is 390 in Decimal128(2, 1) aka 39 in unscaled Decimal128(2, 0)
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
                    Some(i128::from(25)),
                    2,
                    1,
                )), // base
                ColumnarValue::Scalar(ScalarValue::Int64(Some(4))), // exponent
            ],
            arg_fields,
            number_rows: 1,
            return_field: Field::new("f", DataType::Decimal128(2, 1), true).into(),
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
                assert_eq!(ints.value(0), i128::from(390));
                // Signature stays the same as input
                assert_eq!(*arr.data_type(), DataType::Decimal128(2, 1));
            }
            ColumnarValue::Scalar(_) => {
                panic!("Expected an array value")
            }
        }
    }

    #[test]
    fn test_pow_decimal128_helper() {
        // Expression: 2.5 ^ 4 = 39.0625
        assert_eq!(pow_decimal128_int(25, 1, 4).unwrap(), i128::from(390));
        assert_eq!(pow_decimal128_int(2500, 3, 4).unwrap(), i128::from(39062));
        assert_eq!(pow_decimal128_int(25000, 4, 4).unwrap(), i128::from(390625));

        // Expression: 25 ^ 4 = 390625
        assert_eq!(pow_decimal128_int(25, 0, 4).unwrap(), i128::from(390625));

        // Expressions for edge cases
        assert_eq!(pow_decimal128_int(25, 1, 1).unwrap(), i128::from(25));
        assert_eq!(pow_decimal128_int(25, 0, 1).unwrap(), i128::from(25));
        assert_eq!(pow_decimal128_int(25, 0, 0).unwrap(), i128::from(1));
        assert_eq!(pow_decimal128_int(25, 1, 0).unwrap(), i128::from(10));

        assert_eq!(
            pow_decimal128_int(25, -1, 4).unwrap_err().to_string(),
            "Not yet implemented: Negative scale is not yet supported value: -1"
        );
    }
}
