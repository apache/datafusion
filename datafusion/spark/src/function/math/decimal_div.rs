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

//! Spark-compatible decimal division functions.
//!
//! This module implements Spark's decimal division semantics, which require
//! special handling for precision and scale that differs from standard SQL.
//!
//! **Note:** These UDFs are intended for use at the physical plan level only.
//! They do not handle type coercion to Decimal128 — callers must ensure
//! inputs are already Decimal128.
//!
//! # Scale Expansion
//!
//! For Decimal(p1, s1) / Decimal(p2, s2) = Decimal(p3, s3):
//! The dividend needs to be scaled to s2 + s3 + 1 to get correct precision.
//! This can exceed Decimal128's maximum scale (38), requiring BigInt fallback.

use arrow::array::{Array, ArrayRef, AsArray, Decimal128Array};
use arrow::datatypes::{DECIMAL128_MAX_PRECISION, DataType, Decimal128Type};
use datafusion_common::utils::take_function_args;
use datafusion_common::{Result, internal_err};
use datafusion_expr::{
    ColumnarValue, ScalarFunctionArgs, ScalarUDFImpl, Signature, TypeSignature,
    Volatility,
};
use num::{BigInt, Signed, ToPrimitive};
use std::any::Any;
use std::sync::Arc;

/// Extract precision and scale from a Decimal128 DataType.
fn get_precision_scale(data_type: &DataType) -> (u8, i8) {
    match data_type {
        DataType::Decimal128(p, s) => (*p, *s),
        _ => unreachable!("Expected Decimal128 type"),
    }
}

/// Internal implementation for both regular and integral decimal division.
///
/// # Type Parameters
/// * `IS_INTEGRAL_DIV` - If true, performs integer division (truncates result)
///
/// # Arguments
/// * `left` - The dividend array (must be Decimal128)
/// * `right` - The divisor array (must be Decimal128)
/// * `result_precision` - The precision of the result type
/// * `result_scale` - The scale of the result type
fn spark_decimal_div_internal<const IS_INTEGRAL_DIV: bool>(
    left: &ArrayRef,
    right: &ArrayRef,
    result_precision: u8,
    result_scale: i8,
) -> Result<ColumnarValue> {
    let left = left.as_primitive::<Decimal128Type>();
    let right = right.as_primitive::<Decimal128Type>();
    let (p1, s1) = get_precision_scale(left.data_type());
    let (p2, s2) = get_precision_scale(right.data_type());

    // Calculate the scale expansion needed
    // To get Decimal(p3, s3) from p1/p2, we need to widen s1 to s2 + s3 + 1
    let scale_sum = s2 as i32 + result_scale as i32 + 1;
    let l_exp = (scale_sum - s1 as i32).max(0) as u32;
    let r_exp = (s1 as i32 - scale_sum).max(0) as u32;

    let result: Decimal128Array = if p1 as u32 + l_exp > DECIMAL128_MAX_PRECISION as u32
        || p2 as u32 + r_exp > DECIMAL128_MAX_PRECISION as u32
    {
        // Use BigInt for high precision calculations that would overflow i128
        let ten = BigInt::from(10);
        let l_mul = ten.pow(l_exp);
        let r_mul = ten.pow(r_exp);
        let five = BigInt::from(5);
        let zero = BigInt::from(0);

        arrow::compute::kernels::arity::try_binary(left, right, |l, r| {
            let l = BigInt::from(l) * &l_mul;
            let r = BigInt::from(r) * &r_mul;
            // Legacy mode: divide by zero returns 0
            let div = if r.eq(&zero) { zero.clone() } else { &l / &r };
            let res = if IS_INTEGRAL_DIV {
                div
            } else if div.is_negative() {
                div - &five
            } else {
                div + &five
            } / &ten;
            Ok(res.to_i128().unwrap_or_else(|| {
                if res.is_negative() {
                    i128::MIN
                } else {
                    i128::MAX
                }
            }))
        })?
    } else {
        // Standard i128 calculation when precision is within bounds
        let l_mul = 10_i128.pow(l_exp);
        let r_mul = 10_i128.pow(r_exp);

        arrow::compute::kernels::arity::try_binary(left, right, |l, r| {
            let l = l.checked_mul(l_mul).unwrap_or_else(|| {
                if l.is_negative() {
                    i128::MIN
                } else {
                    i128::MAX
                }
            });
            let r = r.checked_mul(r_mul).unwrap_or_else(|| {
                if r.is_negative() {
                    i128::MIN
                } else {
                    i128::MAX
                }
            });
            // Legacy mode: divide by zero returns 0
            let div = if r == 0 { 0 } else { l / r };
            let res = if IS_INTEGRAL_DIV {
                div
            } else if div.is_negative() {
                div - 5
            } else {
                div + 5
            } / 10;
            Ok(res)
        })?
    };

    let result =
        result.with_data_type(DataType::Decimal128(result_precision, result_scale));
    Ok(ColumnarValue::Array(Arc::new(result)))
}

/// Spark-compatible decimal division function.
///
/// Performs division with Spark's rounding behavior (round half away from zero).
pub fn spark_decimal_div(
    args: &[ColumnarValue],
    result_precision: u8,
    result_scale: i8,
) -> Result<ColumnarValue> {
    let arrays = ColumnarValue::values_to_arrays(args)?;
    let [left, right] = take_function_args("spark_decimal_div", arrays)?;
    spark_decimal_div_internal::<false>(&left, &right, result_precision, result_scale)
}

/// Spark-compatible integral decimal division function.
///
/// Performs integer division (truncates toward zero).
pub fn spark_decimal_integral_div(
    args: &[ColumnarValue],
    result_precision: u8,
    result_scale: i8,
) -> Result<ColumnarValue> {
    let arrays = ColumnarValue::values_to_arrays(args)?;
    let [left, right] = take_function_args("spark_decimal_integral_div", arrays)?;
    spark_decimal_div_internal::<true>(&left, &right, result_precision, result_scale)
}

/// SparkDecimalDiv implements the Spark-compatible decimal division function.
///
/// This UDF takes the result precision and scale as part of its configuration,
/// since Spark determines these at query planning time.
///
/// **Note:** This UDF is intended for use at the physical plan level only.
/// It does not handle type coercion — inputs must already be Decimal128.
#[derive(Debug, PartialEq, Eq, Hash)]
pub struct SparkDecimalDiv {
    signature: Signature,
    result_precision: u8,
    result_scale: i8,
}

impl Default for SparkDecimalDiv {
    fn default() -> Self {
        Self::new(38, 18).expect("default precision/scale should be valid")
    }
}

impl SparkDecimalDiv {
    /// Create a new SparkDecimalDiv with the specified result precision and scale.
    ///
    /// Returns an error if `result_precision` exceeds `DECIMAL128_MAX_PRECISION` or
    /// `result_scale.unsigned_abs()` exceeds `result_precision`.
    pub fn new(result_precision: u8, result_scale: i8) -> Result<Self> {
        if result_precision > DECIMAL128_MAX_PRECISION {
            return internal_err!(
                "result_precision ({result_precision}) exceeds DECIMAL128_MAX_PRECISION ({DECIMAL128_MAX_PRECISION})"
            );
        }
        if result_scale.unsigned_abs() > result_precision {
            return internal_err!(
                "result_scale ({result_scale}) exceeds result_precision ({result_precision})"
            );
        }
        Ok(Self {
            signature: Signature::new(
                TypeSignature::Exact(vec![
                    DataType::Decimal128(DECIMAL128_MAX_PRECISION, 0),
                    DataType::Decimal128(DECIMAL128_MAX_PRECISION, 0),
                ]),
                Volatility::Immutable,
            ),
            result_precision,
            result_scale,
        })
    }
}

impl ScalarUDFImpl for SparkDecimalDiv {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "spark_decimal_div"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Decimal128(
            self.result_precision,
            self.result_scale,
        ))
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        spark_decimal_div(&args.args, self.result_precision, self.result_scale)
    }
}

/// SparkDecimalIntegralDiv implements Spark-compatible integral decimal division.
///
/// Returns the integer quotient of division (truncates toward zero).
///
/// **Note:** This UDF is intended for use at the physical plan level only.
/// It does not handle type coercion — inputs must already be Decimal128.
#[derive(Debug, PartialEq, Eq, Hash)]
pub struct SparkDecimalIntegralDiv {
    signature: Signature,
    result_precision: u8,
    result_scale: i8,
}

impl Default for SparkDecimalIntegralDiv {
    fn default() -> Self {
        Self::new(38, 0).expect("default precision/scale should be valid")
    }
}

impl SparkDecimalIntegralDiv {
    /// Create a new SparkDecimalIntegralDiv with the specified result precision and scale.
    ///
    /// Returns an error if `result_precision` exceeds `DECIMAL128_MAX_PRECISION` or
    /// `result_scale.unsigned_abs()` exceeds `result_precision`.
    pub fn new(result_precision: u8, result_scale: i8) -> Result<Self> {
        if result_precision > DECIMAL128_MAX_PRECISION {
            return internal_err!(
                "result_precision ({result_precision}) exceeds DECIMAL128_MAX_PRECISION ({DECIMAL128_MAX_PRECISION})"
            );
        }
        if result_scale.unsigned_abs() > result_precision {
            return internal_err!(
                "result_scale ({result_scale}) exceeds result_precision ({result_precision})"
            );
        }
        Ok(Self {
            signature: Signature::new(
                TypeSignature::Exact(vec![
                    DataType::Decimal128(DECIMAL128_MAX_PRECISION, 0),
                    DataType::Decimal128(DECIMAL128_MAX_PRECISION, 0),
                ]),
                Volatility::Immutable,
            ),
            result_precision,
            result_scale,
        })
    }
}

impl ScalarUDFImpl for SparkDecimalIntegralDiv {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "spark_decimal_integral_div"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Decimal128(
            self.result_precision,
            self.result_scale,
        ))
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        spark_decimal_integral_div(&args.args, self.result_precision, self.result_scale)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::Decimal128Array;
    use arrow::datatypes::DataType;

    fn create_decimal_array(
        values: Vec<Option<i128>>,
        precision: u8,
        scale: i8,
    ) -> ArrayRef {
        Arc::new(
            Decimal128Array::from(values)
                .with_data_type(DataType::Decimal128(precision, scale)),
        )
    }

    #[test]
    fn test_basic_decimal_division() {
        // 10.00 / 2.00 = 5.00
        let left = create_decimal_array(vec![Some(1000)], 10, 2);
        let right = create_decimal_array(vec![Some(200)], 10, 2);

        let left_cv = ColumnarValue::Array(left);
        let right_cv = ColumnarValue::Array(right);

        let result = spark_decimal_div(&[left_cv, right_cv], 10, 2).unwrap();

        if let ColumnarValue::Array(arr) = result {
            let decimal_arr = arr.as_primitive::<Decimal128Type>();
            assert_eq!(decimal_arr.value(0), 500); // 5.00
        } else {
            panic!("Expected array result");
        }
    }

    #[test]
    fn test_decimal_division_with_rounding() {
        // 10.00 / 3.00 = 3.33 (rounded)
        let left = create_decimal_array(vec![Some(1000)], 10, 2);
        let right = create_decimal_array(vec![Some(300)], 10, 2);

        let left_cv = ColumnarValue::Array(left);
        let right_cv = ColumnarValue::Array(right);

        let result = spark_decimal_div(&[left_cv, right_cv], 10, 2).unwrap();

        if let ColumnarValue::Array(arr) = result {
            let decimal_arr = arr.as_primitive::<Decimal128Type>();
            assert_eq!(decimal_arr.value(0), 333); // 3.33
        } else {
            panic!("Expected array result");
        }
    }

    #[test]
    fn test_division_by_zero() {
        // 10.00 / 0.00 = 0 (Legacy mode)
        let left = create_decimal_array(vec![Some(1000)], 10, 2);
        let right = create_decimal_array(vec![Some(0)], 10, 2);

        let left_cv = ColumnarValue::Array(left);
        let right_cv = ColumnarValue::Array(right);

        let result = spark_decimal_div(&[left_cv, right_cv], 10, 2).unwrap();

        if let ColumnarValue::Array(arr) = result {
            let decimal_arr = arr.as_primitive::<Decimal128Type>();
            assert_eq!(decimal_arr.value(0), 0); // 0.00 (divide by zero returns 0)
        } else {
            panic!("Expected array result");
        }
    }

    #[test]
    fn test_integral_division() {
        // 10.00 / 3.00 = 3 (truncated)
        let left = create_decimal_array(vec![Some(1000)], 10, 2);
        let right = create_decimal_array(vec![Some(300)], 10, 2);

        let left_cv = ColumnarValue::Array(left);
        let right_cv = ColumnarValue::Array(right);

        let result = spark_decimal_integral_div(&[left_cv, right_cv], 10, 0).unwrap();

        if let ColumnarValue::Array(arr) = result {
            let decimal_arr = arr.as_primitive::<Decimal128Type>();
            assert_eq!(decimal_arr.value(0), 3); // 3 (truncated)
        } else {
            panic!("Expected array result");
        }
    }

    #[test]
    fn test_negative_division() {
        // -10.00 / 3.00 = -3.33 (rounded away from zero)
        let left = create_decimal_array(vec![Some(-1000)], 10, 2);
        let right = create_decimal_array(vec![Some(300)], 10, 2);

        let left_cv = ColumnarValue::Array(left);
        let right_cv = ColumnarValue::Array(right);

        let result = spark_decimal_div(&[left_cv, right_cv], 10, 2).unwrap();

        if let ColumnarValue::Array(arr) = result {
            let decimal_arr = arr.as_primitive::<Decimal128Type>();
            assert_eq!(decimal_arr.value(0), -333); // -3.33
        } else {
            panic!("Expected array result");
        }
    }

    #[test]
    fn test_null_handling() {
        let left = create_decimal_array(vec![Some(1000), None], 10, 2);
        let right = create_decimal_array(vec![Some(200), Some(200)], 10, 2);

        let left_cv = ColumnarValue::Array(left);
        let right_cv = ColumnarValue::Array(right);

        let result = spark_decimal_div(&[left_cv, right_cv], 10, 2).unwrap();

        if let ColumnarValue::Array(arr) = result {
            let decimal_arr = arr.as_primitive::<Decimal128Type>();
            assert_eq!(decimal_arr.value(0), 500); // 5.00
            assert!(decimal_arr.is_null(1)); // NULL / x = NULL
        } else {
            panic!("Expected array result");
        }
    }

    #[test]
    fn test_new_rejects_precision_exceeding_max() {
        let result = SparkDecimalDiv::new(DECIMAL128_MAX_PRECISION + 1, 0);
        assert!(result.is_err());

        let result = SparkDecimalIntegralDiv::new(DECIMAL128_MAX_PRECISION + 1, 0);
        assert!(result.is_err());
    }

    #[test]
    fn test_new_rejects_scale_exceeding_precision() {
        // Positive scale exceeding precision
        let result = SparkDecimalDiv::new(10, 11);
        assert!(result.is_err());

        // Negative scale exceeding precision
        let result = SparkDecimalDiv::new(10, -11);
        assert!(result.is_err());

        let result = SparkDecimalIntegralDiv::new(10, 11);
        assert!(result.is_err());

        let result = SparkDecimalIntegralDiv::new(10, -11);
        assert!(result.is_err());
    }

    #[test]
    fn test_new_accepts_valid_precision_scale() {
        assert!(SparkDecimalDiv::new(38, 18).is_ok());
        assert!(SparkDecimalDiv::new(10, 0).is_ok());
        assert!(SparkDecimalDiv::new(10, -10).is_ok());
        assert!(SparkDecimalIntegralDiv::new(38, 0).is_ok());
    }

    #[test]
    fn test_negative_scale_division() {
        // Use negative result_scale: result represents multiples of 10
        // Decimal(10,2) values: 12345.67 (1234567) / 100.00 (10000)
        // With result_scale=-1, result is in units of 10, so 123 means 1230
        let left = create_decimal_array(vec![Some(1234567)], 10, 2);
        let right = create_decimal_array(vec![Some(10000)], 10, 2);

        let left_cv = ColumnarValue::Array(left);
        let right_cv = ColumnarValue::Array(right);

        // This exercises the negative scale_sum path
        let result = spark_decimal_div(&[left_cv, right_cv], 10, -1).unwrap();

        if let ColumnarValue::Array(arr) = result {
            let decimal_arr = arr.as_primitive::<Decimal128Type>();
            // With scale=-1, the value represents multiples of 10
            // 12345.67 / 100.00 = 123.4567, rounded to tens = 120 -> stored as 12
            assert_eq!(decimal_arr.value(0), 12);
        } else {
            panic!("Expected array result");
        }
    }

    #[test]
    fn test_i128_multiplication_overflow_saturates() {
        // Use max i128 value with scale expansion that would cause overflow
        // Decimal(38,0) with value near i128::MAX, result_scale forces l_exp > 0
        let large_val = i128::MAX / 10; // large but fits in Decimal(38,0)
        let left = create_decimal_array(vec![Some(large_val)], 38, 0);
        let right = create_decimal_array(vec![Some(1)], 38, 0);

        let left_cv = ColumnarValue::Array(left);
        let right_cv = ColumnarValue::Array(right);

        // result_scale=6 forces l_exp = 0 + 6 + 1 - 0 = 7, so l * 10^7 overflows
        // This should not panic — it should saturate via checked_mul
        let result = spark_decimal_div(&[left_cv, right_cv], 38, 6);
        assert!(result.is_ok());
    }
}
