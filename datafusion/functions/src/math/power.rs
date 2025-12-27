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

use crate::utils::{calculate_binary_decimal_math, calculate_binary_math};
use arrow::array::{Array, ArrayRef};
use arrow::datatypes::{
    ArrowNativeTypeOp, DataType, Decimal32Type, Decimal64Type, Decimal128Type,
    Decimal256Type, Float64Type, Int64Type,
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
///
/// Returns error if base is invalid
fn pow_decimal_int<T>(base: T, scale: i8, exp: i64) -> Result<T, ArrowError>
where
    T: From<i32> + ArrowNativeTypeOp,
{
    let exp: u32 = exp.try_into().map_err(|_| {
        ArrowError::ArithmeticOverflow(format!("Unsupported exp value: {exp}"))
    })?;
    // Handle edge case for exp == 0
    // If scale < 0, 10^scale (e.g., 10^-2 = 0.01) becomes 0 in integer arithmetic.
    if exp == 0 {
        return if scale >= 0 {
            T::from(10).pow_checked(scale as u32).map_err(|_| {
                ArrowError::ArithmeticOverflow(format!(
                    "Cannot make unscale factor for {scale} and {exp}"
                ))
            })
        } else {
            Ok(T::from(0))
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
        let div_factor: T = T::from(10).pow_checked(mul_exp as u32).map_err(|_| {
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
        let mul_factor: T = T::from(10).pow_checked(abs_exp as u32).map_err(|_| {
            ArrowError::ArithmeticOverflow(format!(
                "Cannot make mul factor for {scale} and {exp}"
            ))
        })?;
        powered.mul_checked(mul_factor)
    }
}

/// Binary function to calculate a math power to float exponent
/// for scaled integer types.
/// Returns error if exponent is negative or non-integer, or base invalid
fn pow_decimal_float<T>(base: T, scale: i8, exp: f64) -> Result<T, ArrowError>
where
    T: From<i32> + ArrowNativeTypeOp,
{
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
    pow_decimal_int(base, scale, exp as i64)
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
        let base = base.to_array(args.number_rows)?;

        let arr: ArrayRef = match (base.data_type(), exponent.data_type()) {
            (DataType::Float64, DataType::Float64) => {
                calculate_binary_math::<Float64Type, Float64Type, Float64Type, _>(
                    &base,
                    exponent,
                    |b, e| Ok(f64::powf(b, e)),
                )?
            }
            (DataType::Decimal32(precision, scale), DataType::Int64) => {
                calculate_binary_decimal_math::<Decimal32Type, Int64Type, Decimal32Type, _>(
                    &base,
                    exponent,
                    |b, e| pow_decimal_int(b, *scale, e),
                    *precision,
                    *scale,
                )?
            }
            (DataType::Decimal32(precision, scale), DataType::Float64) => {
                calculate_binary_decimal_math::<
                    Decimal32Type,
                    Float64Type,
                    Decimal32Type,
                    _,
                >(
                    &base,
                    exponent,
                    |b, e| pow_decimal_float(b, *scale, e),
                    *precision,
                    *scale,
                )?
            }
            (DataType::Decimal64(precision, scale), DataType::Int64) => {
                calculate_binary_decimal_math::<Decimal64Type, Int64Type, Decimal64Type, _>(
                    &base,
                    exponent,
                    |b, e| pow_decimal_int(b, *scale, e),
                    *precision,
                    *scale,
                )?
            }
            (DataType::Decimal64(precision, scale), DataType::Float64) => {
                calculate_binary_decimal_math::<
                    Decimal64Type,
                    Float64Type,
                    Decimal64Type,
                    _,
                >(
                    &base,
                    exponent,
                    |b, e| pow_decimal_float(b, *scale, e),
                    *precision,
                    *scale,
                )?
            }
            (DataType::Decimal128(precision, scale), DataType::Int64) => {
                calculate_binary_decimal_math::<
                    Decimal128Type,
                    Int64Type,
                    Decimal128Type,
                    _,
                >(
                    &base,
                    exponent,
                    |b, e| pow_decimal_int(b, *scale, e),
                    *precision,
                    *scale,
                )?
            }
            (DataType::Decimal128(precision, scale), DataType::Float64) => {
                calculate_binary_decimal_math::<
                    Decimal128Type,
                    Float64Type,
                    Decimal128Type,
                    _,
                >(
                    &base,
                    exponent,
                    |b, e| pow_decimal_float(b, *scale, e),
                    *precision,
                    *scale,
                )?
            }
            (DataType::Decimal256(precision, scale), DataType::Int64) => {
                calculate_binary_decimal_math::<
                    Decimal256Type,
                    Int64Type,
                    Decimal256Type,
                    _,
                >(
                    &base,
                    exponent,
                    |b, e| pow_decimal_int(b, *scale, e),
                    *precision,
                    *scale,
                )?
            }
            (DataType::Decimal256(precision, scale), DataType::Float64) => {
                calculate_binary_decimal_math::<
                    Decimal256Type,
                    Float64Type,
                    Decimal256Type,
                    _,
                >(
                    &base,
                    exponent,
                    |b, e| pow_decimal_float(b, *scale, e),
                    *precision,
                    *scale,
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
        assert_eq!(pow_decimal_int(25, 1, 4).unwrap(), i128::from(390));
        assert_eq!(pow_decimal_int(2500, 3, 4).unwrap(), i128::from(39062));
        assert_eq!(pow_decimal_int(25000, 4, 4).unwrap(), i128::from(390625));

        // Expression: 25 ^ 4 = 390625
        assert_eq!(pow_decimal_int(25, 0, 4).unwrap(), i128::from(390625));

        // Expressions for edge cases
        assert_eq!(pow_decimal_int(25, 1, 1).unwrap(), i128::from(25));
        assert_eq!(pow_decimal_int(25, 0, 1).unwrap(), i128::from(25));
        assert_eq!(pow_decimal_int(25, 0, 0).unwrap(), i128::from(1));
        assert_eq!(pow_decimal_int(25, 1, 0).unwrap(), i128::from(10));

        assert_eq!(pow_decimal_int(25, -1, 4).unwrap(), i128::from(390625000));
    }
}
