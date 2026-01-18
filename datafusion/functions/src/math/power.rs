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

use super::log::LogFunc;

use crate::utils::{
    calculate_binary_math, calculate_binary_math_decimal, calculate_binary_math_numeric,
};
use arrow::array::{Array, ArrayRef};
use arrow::datatypes::i256;
use arrow::datatypes::{
    ArrowNativeType, ArrowNativeTypeOp, DataType, Decimal32Type, Decimal64Type,
    Decimal128Type, Decimal256Type, Float64Type, Int64Type,
};
use arrow::error::ArrowError;
use datafusion_common::types::{NativeType, logical_float64, logical_int64};
use datafusion_common::utils::take_function_args;
use datafusion_common::{Result, ScalarValue, internal_err};
use datafusion_expr::expr::ScalarFunction;
use datafusion_expr::simplify::{ExprSimplifyResult, SimplifyContext};
use datafusion_expr::{
    Coercion, ColumnarValue, Documentation, Expr, ScalarFunctionArgs, ScalarUDF,
    ScalarUDFImpl, Signature, TypeSignature, TypeSignatureClass, Volatility, lit,
};
use datafusion_macros::user_doc;
use num_traits::{NumCast, ToPrimitive};

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
        let integer = Coercion::new_implicit(
            TypeSignatureClass::Native(logical_int64()),
            vec![TypeSignatureClass::Integer],
            NativeType::Int64,
        );
        let decimal = Coercion::new_exact(TypeSignatureClass::Decimal);
        let float = Coercion::new_implicit(
            TypeSignatureClass::Native(logical_float64()),
            vec![TypeSignatureClass::Numeric],
            NativeType::Float64,
        );
        Self {
            signature: Signature::one_of(
                vec![
                    TypeSignature::Coercible(vec![decimal.clone(), integer]),
                    TypeSignature::Coercible(vec![decimal.clone(), float.clone()]),
                    TypeSignature::Coercible(vec![float; 2]),
                ],
                Volatility::Immutable,
            ),
            aliases: vec![String::from("pow")],
        }
    }
}

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
fn pow_decimal_int<T>(base: T, scale: i8, exp: i64) -> Result<T, ArrowError>
where
    T: ArrowNativeType + ArrowNativeTypeOp + ToPrimitive + NumCast + Copy,
{
    // Negative exponent: fall back to float computation
    if exp < 0 {
        return pow_decimal_float(base, scale, exp as f64);
    }

    let exp: u32 = exp.try_into().map_err(|_| {
        ArrowError::ArithmeticOverflow(format!("Unsupported exp value: {exp}"))
    })?;
    // Handle edge case for exp == 0
    // If scale < 0, 10^scale (e.g., 10^-2 = 0.01) becomes 0 in integer arithmetic.
    if exp == 0 {
        return if scale >= 0 {
            T::usize_as(10).pow_checked(scale as u32).map_err(|_| {
                ArrowError::ArithmeticOverflow(format!(
                    "Cannot make unscale factor for {scale} and {exp}"
                ))
            })
        } else {
            Ok(T::ZERO)
        };
    }
    let powered: T = base.pow_checked(exp).map_err(|_| {
        ArrowError::ArithmeticOverflow(format!("Cannot raise base {base:?} to exp {exp}"))
    })?;

    // Calculate the scale adjustment: s * (e - 1)
    // We use i64 to prevent overflow during the intermediate multiplication
    let mul_exp = (scale as i64).wrapping_mul(exp as i64 - 1);

    if mul_exp == 0 {
        return Ok(powered);
    }

    // If mul_exp is positive, we divide (standard case).
    // If mul_exp is negative, we multiply (negative scale case).
    if mul_exp > 0 {
        let div_factor: T =
            T::usize_as(10).pow_checked(mul_exp as u32).map_err(|_| {
                ArrowError::ArithmeticOverflow(format!(
                    "Cannot make div factor for {scale} and {exp}"
                ))
            })?;
        powered.div_checked(div_factor)
    } else {
        // mul_exp is negative, so we multiply by 10^(-mul_exp)
        let abs_exp = mul_exp.checked_neg().ok_or_else(|| {
            ArrowError::ArithmeticOverflow(
                "Overflow while negating scale exponent".to_string(),
            )
        })?;
        let mul_factor: T =
            T::usize_as(10).pow_checked(abs_exp as u32).map_err(|_| {
                ArrowError::ArithmeticOverflow(format!(
                    "Cannot make mul factor for {scale} and {exp}"
                ))
            })?;
        powered.mul_checked(mul_factor)
    }
}

/// Binary function to calculate a math power to float exponent
/// for scaled integer types.
fn pow_decimal_float<T>(base: T, scale: i8, exp: f64) -> Result<T, ArrowError>
where
    T: ArrowNativeType + ArrowNativeTypeOp + ToPrimitive + NumCast + Copy,
{
    if exp.is_finite() && exp.trunc() == exp && exp >= 0f64 && exp < u32::MAX as f64 {
        return pow_decimal_int(base, scale, exp as i64);
    }

    if !exp.is_finite() {
        return Err(ArrowError::ComputeError(format!(
            "Cannot use non-finite exp: {exp}"
        )));
    }

    pow_decimal_float_fallback(base, scale, exp)
}

/// Compute the f64 power result and scale it back.
/// Returns the rounded i128 result for conversion to target type.
#[inline]
fn compute_pow_f64_result(
    base_f64: f64,
    scale: i8,
    exp: f64,
) -> Result<i128, ArrowError> {
    let result_f64 = base_f64.powf(exp);

    if !result_f64.is_finite() {
        return Err(ArrowError::ArithmeticOverflow(format!(
            "Result of {base_f64}^{exp} is not finite"
        )));
    }

    let scale_factor = 10f64.powi(scale as i32);
    let result_scaled = result_f64 * scale_factor;
    let result_rounded = result_scaled.round();

    if result_rounded.abs() > i128::MAX as f64 {
        return Err(ArrowError::ArithmeticOverflow(format!(
            "Result {result_rounded} is too large for the target decimal type"
        )));
    }

    Ok(result_rounded as i128)
}

/// Convert i128 result to target decimal native type using NumCast.
/// Returns error if value overflows the target type.
#[inline]
fn decimal_from_i128<T>(value: i128) -> Result<T, ArrowError>
where
    T: NumCast,
{
    NumCast::from(value).ok_or_else(|| {
        ArrowError::ArithmeticOverflow(format!(
            "Value {value} is too large for the target decimal type"
        ))
    })
}

/// Fallback implementation using f64 for negative or non-integer exponents.
/// This handles cases that cannot be computed using integer arithmetic.
fn pow_decimal_float_fallback<T>(base: T, scale: i8, exp: f64) -> Result<T, ArrowError>
where
    T: ToPrimitive + NumCast + Copy,
{
    if scale < 0 {
        return Err(ArrowError::NotYetImplemented(format!(
            "Negative scale is not yet supported: {scale}"
        )));
    }

    let scale_factor = 10f64.powi(scale as i32);
    let base_f64 = base.to_f64().ok_or_else(|| {
        ArrowError::ComputeError("Cannot convert base to f64".to_string())
    })? / scale_factor;

    let result_i128 = compute_pow_f64_result(base_f64, scale, exp)?;

    decimal_from_i128(result_i128)
}

/// Decimal256 specialized float exponent version.
fn pow_decimal256_float(base: i256, scale: i8, exp: f64) -> Result<i256, ArrowError> {
    if exp.is_finite() && exp.trunc() == exp && exp >= 0f64 && exp < u32::MAX as f64 {
        return pow_decimal256_int(base, scale, exp as i64);
    }

    if !exp.is_finite() {
        return Err(ArrowError::ComputeError(format!(
            "Cannot use non-finite exp: {exp}"
        )));
    }

    pow_decimal256_float_fallback(base, scale, exp)
}

/// Decimal256 specialized integer exponent version.
fn pow_decimal256_int(base: i256, scale: i8, exp: i64) -> Result<i256, ArrowError> {
    if exp < 0 {
        return pow_decimal256_float(base, scale, exp as f64);
    }

    let exp: u32 = exp.try_into().map_err(|_| {
        ArrowError::ArithmeticOverflow(format!("Unsupported exp value: {exp}"))
    })?;

    if exp == 0 {
        return if scale >= 0 {
            i256::from_i128(10).pow_checked(scale as u32).map_err(|_| {
                ArrowError::ArithmeticOverflow(format!(
                    "Cannot make unscale factor for {scale} and {exp}"
                ))
            })
        } else {
            Ok(i256::from_i128(0))
        };
    }

    let powered: i256 = base.pow_checked(exp).map_err(|_| {
        ArrowError::ArithmeticOverflow(format!("Cannot raise base {base:?} to exp {exp}"))
    })?;

    let mul_exp = (scale as i64).wrapping_mul(exp as i64 - 1);

    if mul_exp == 0 {
        return Ok(powered);
    }

    if mul_exp > 0 {
        let div_factor: i256 =
            i256::from_i128(10)
                .pow_checked(mul_exp as u32)
                .map_err(|_| {
                    ArrowError::ArithmeticOverflow(format!(
                        "Cannot make div factor for {scale} and {exp}"
                    ))
                })?;
        powered.div_checked(div_factor)
    } else {
        let abs_exp = mul_exp.checked_neg().ok_or_else(|| {
            ArrowError::ArithmeticOverflow(
                "Overflow while negating scale exponent".to_string(),
            )
        })?;
        let mul_factor: i256 =
            i256::from_i128(10)
                .pow_checked(abs_exp as u32)
                .map_err(|_| {
                    ArrowError::ArithmeticOverflow(format!(
                        "Cannot make mul factor for {scale} and {exp}"
                    ))
                })?;
        powered.mul_checked(mul_factor)
    }
}

/// Fallback implementation for Decimal256.
fn pow_decimal256_float_fallback(
    base: i256,
    scale: i8,
    exp: f64,
) -> Result<i256, ArrowError> {
    if scale < 0 {
        return Err(ArrowError::NotYetImplemented(format!(
            "Negative scale is not yet supported: {scale}"
        )));
    }

    let scale_factor = 10f64.powi(scale as i32);
    let base_f64 = base.to_f64().ok_or_else(|| {
        ArrowError::ComputeError("Cannot convert base to f64".to_string())
    })? / scale_factor;

    let result_i128 = compute_pow_f64_result(base_f64, scale, exp)?;

    // i256 can be constructed from i128 directly
    Ok(i256::from_i128(result_i128))
}

/// Fallback implementation for decimal power when exponent is an array.
/// Casts decimal to float64, computes power, and casts back to original decimal type.
/// This is used for performance when exponent varies per-row.
fn pow_decimal_with_float_fallback(
    base: &ArrayRef,
    exponent: &ColumnarValue,
    num_rows: usize,
) -> Result<ColumnarValue> {
    use arrow::compute::cast;

    let original_type = base.data_type().clone();
    let base_f64 = cast(base.as_ref(), &DataType::Float64)?;

    let exp_f64 = match exponent {
        ColumnarValue::Array(arr) => cast(arr.as_ref(), &DataType::Float64)?,
        ColumnarValue::Scalar(scalar) => {
            let scalar_f64 = scalar.cast_to(&DataType::Float64)?;
            scalar_f64.to_array_of_size(num_rows)?
        }
    };

    let result_f64 = calculate_binary_math::<Float64Type, Float64Type, Float64Type, _>(
        &base_f64,
        &ColumnarValue::Array(exp_f64),
        |b, e| Ok(f64::powf(b, e)),
    )?;

    let result = cast(result_f64.as_ref(), &original_type)?;
    Ok(ColumnarValue::Array(result))
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
        if arg_types[0].is_null() {
            Ok(DataType::Float64)
        } else {
            Ok(arg_types[0].clone())
        }
    }

    fn aliases(&self) -> &[String] {
        &self.aliases
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let [base, exponent] = take_function_args(self.name(), &args.args)?;

        // For decimal types, only use native decimal
        // operations when we have a scalar exponent. When the exponent is an array,
        // fall back to float computation for better performance.
        let use_float_fallback = matches!(
            base.data_type(),
            DataType::Decimal32(_, _)
                | DataType::Decimal64(_, _)
                | DataType::Decimal128(_, _)
                | DataType::Decimal256(_, _)
        ) && matches!(exponent, ColumnarValue::Array(_));

        let base = base.to_array(args.number_rows)?;

        // If decimal with array exponent, cast to float and compute
        if use_float_fallback {
            return pow_decimal_with_float_fallback(&base, exponent, args.number_rows);
        }

        let out_type = base.data_type();
        // Safety: all `dec_scale.expect` calls below are infallible since the left argument
        // is decimal array as per `calculate_binary_math` contract.
        let arr: ArrayRef = match (base.data_type(), exponent.data_type()) {
            (DataType::Float64, DataType::Float64) => {
                calculate_binary_math_numeric::<Float64Type, Float64Type, Float64Type, _>(
                    &base,
                    exponent,
                    |b, e, _| Ok(f64::powf(b, e)),
                    out_type,
                )?
            }
            (DataType::Decimal32(_, _), DataType::Int64) => {
                calculate_binary_math_decimal::<Decimal32Type, Int64Type, Decimal32Type, _>(
                    &base,
                    exponent,
                    |b, e, dec_scale| {
                        pow_decimal_int(b, dec_scale.expect("left is decimal").1, e)
                    },
                    out_type,
                )?
            }
            (DataType::Decimal32(_, _), DataType::Float64) => {
                calculate_binary_math_decimal::<
                    Decimal32Type,
                    Float64Type,
                    Decimal32Type,
                    _,
                >(
                    &base,
                    exponent,
                    |b, e, dec_scale| {
                        pow_decimal_float(b, dec_scale.expect("left is decimal").1, e)
                    },
                    out_type,
                )?
            }
            (DataType::Decimal64(_, _), DataType::Int64) => {
                calculate_binary_math_decimal::<Decimal64Type, Int64Type, Decimal64Type, _>(
                    &base,
                    exponent,
                    |b, e, dec_scale| {
                        pow_decimal_int(b, dec_scale.expect("left is decimal").1, e)
                    },
                    out_type,
                )?
            }
            (DataType::Decimal64(_, _), DataType::Float64) => {
                calculate_binary_math_decimal::<
                    Decimal64Type,
                    Float64Type,
                    Decimal64Type,
                    _,
                >(
                    &base,
                    exponent,
                    |b, e, dec_scale| {
                        pow_decimal_float(b, dec_scale.expect("left is decimal").1, e)
                    },
                    out_type,
                )?
            }
            (DataType::Decimal128(_, _), DataType::Int64) => {
                calculate_binary_math_decimal::<
                    Decimal128Type,
                    Int64Type,
                    Decimal128Type,
                    _,
                >(
                    &base,
                    exponent,
                    |b, e, dec_scale| {
                        pow_decimal_int(b, dec_scale.expect("left is decimal").1, e)
                    },
                    out_type,
                )?
            }
            (DataType::Decimal128(_, _), DataType::Float64) => {
                calculate_binary_math_decimal::<
                    Decimal128Type,
                    Float64Type,
                    Decimal128Type,
                    _,
                >(
                    &base,
                    exponent,
                    |b, e, dec_scale| {
                        pow_decimal_float(b, dec_scale.expect("left is decimal").1, e)
                    },
                    out_type,
                )?
            }
            (DataType::Decimal256(_, _), DataType::Int64) => {
                calculate_binary_math_decimal::<
                    Decimal256Type,
                    Int64Type,
                    Decimal256Type,
                    _,
                >(
                    &base,
                    exponent,
                    |b, e, dec_scale| {
                        pow_decimal256_int(b, dec_scale.expect("left is decimal").1, e)
                    },
                    out_type,
                )?
            }
            (DataType::Decimal256(_, _), DataType::Float64) => {
                calculate_binary_math_decimal::<
                    Decimal256Type,
                    Float64Type,
                    Decimal256Type,
                    _,
                >(
                    &base,
                    exponent,
                    |b, e, dec_scale| {
                        pow_decimal256_float(b, dec_scale.expect("left is decimal").1, e)
                    },
                    out_type,
                )?
            }
            (base_type, exp_type) => {
                return internal_err!(
                    "Unsupported data types for base {base_type:?} and exponent {exp_type:?} for power"
                );
            }
        };
        Ok(ColumnarValue::Array(arr))
    }

    /// Simplify the `power` function by the relevant rules:
    /// 1. Power(a, 0) ===> 1
    /// 2. Power(a, 1) ===> a
    /// 3. Power(a, Log(a, b)) ===> b
    fn simplify(
        &self,
        args: Vec<Expr>,
        info: &SimplifyContext,
    ) -> Result<ExprSimplifyResult> {
        let [base, exponent] = take_function_args("power", args)?;
        let base_type = info.get_data_type(&base)?;
        let exponent_type = info.get_data_type(&exponent)?;

        // Null propagation
        if base_type.is_null() || exponent_type.is_null() {
            let return_type = self.return_type(&[base_type, exponent_type])?;
            return Ok(ExprSimplifyResult::Simplified(lit(
                ScalarValue::Null.cast_to(&return_type)?
            )));
        }

        match exponent {
            Expr::Literal(value, _)
                if value == ScalarValue::new_zero(&exponent_type)? =>
            {
                Ok(ExprSimplifyResult::Simplified(lit(ScalarValue::new_one(
                    &base_type,
                )?)))
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

    #[test]
    fn test_pow_decimal128_helper() {
        // Expression: 2.5 ^ 4 = 39.0625
        assert_eq!(pow_decimal_int(25i128, 1, 4).unwrap(), 390i128);
        assert_eq!(pow_decimal_int(2500i128, 3, 4).unwrap(), 39062i128);
        assert_eq!(pow_decimal_int(25000i128, 4, 4).unwrap(), 390625i128);

        // Expression: 25 ^ 4 = 390625
        assert_eq!(pow_decimal_int(25i128, 0, 4).unwrap(), 390625i128);

        // Expressions for edge cases
        assert_eq!(pow_decimal_int(25i128, 1, 1).unwrap(), 25i128);
        assert_eq!(pow_decimal_int(25i128, 0, 1).unwrap(), 25i128);
        assert_eq!(pow_decimal_int(25i128, 0, 0).unwrap(), 1i128);
        assert_eq!(pow_decimal_int(25i128, 1, 0).unwrap(), 10i128);

        assert_eq!(pow_decimal_int(25i128, -1, 4).unwrap(), 390625000i128);
    }

    #[test]
    fn test_pow_decimal_float_fallback() {
        // Test negative exponent: 4^(-1) = 0.25
        // 4 with scale 2 = 400, result should be 25 (0.25 with scale 2)
        let result: i128 = pow_decimal_float(400i128, 2, -1.0).unwrap();
        assert_eq!(result, 25);

        // Test non-integer exponent: 4^0.5 = 2
        // 4 with scale 2 = 400, result should be 200 (2.0 with scale 2)
        let result: i128 = pow_decimal_float(400i128, 2, 0.5).unwrap();
        assert_eq!(result, 200);

        // Test 8^(1/3) = 2 (cube root)
        // 8 with scale 1 = 80, result should be 20 (2.0 with scale 1)
        let result: i128 = pow_decimal_float(80i128, 1, 1.0 / 3.0).unwrap();
        assert_eq!(result, 20);

        // Test negative base with integer exponent still works
        // (-2)^3 = -8
        // -2 with scale 1 = -20, result should be -80 (-8.0 with scale 1)
        let result: i128 = pow_decimal_float(-20i128, 1, 3.0).unwrap();
        assert_eq!(result, -80);

        // Test positive integer exponent goes through fast path
        // 2.5^4 = 39.0625
        // 25 with scale 1, result should be 390 (39.0 with scale 1) - truncated
        let result: i128 = pow_decimal_float(25i128, 1, 4.0).unwrap();
        assert_eq!(result, 390); // Uses integer path

        // Test non-finite exponent returns error
        assert!(pow_decimal_float(100i128, 2, f64::NAN).is_err());
        assert!(pow_decimal_float(100i128, 2, f64::INFINITY).is_err());
    }
}
