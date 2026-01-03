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
//! # Scale Expansion
//!
//! For Decimal(p1, s1) / Decimal(p2, s2) = Decimal(p3, s3):
//! The dividend needs to be scaled to s2 + s3 + 1 to get correct precision.
//! This can exceed Decimal128's maximum scale (38), requiring BigInt fallback.

use arrow::array::{Array, ArrayRef, AsArray, Decimal128Array};
use arrow::datatypes::{DECIMAL128_MAX_PRECISION, DataType, Decimal128Type};
use datafusion_common::{Result, assert_eq_or_internal_err};
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
/// # Arguments
/// * `args` - Two ColumnarValue arguments (dividend and divisor)
/// * `result_precision` - The precision of the result type
/// * `result_scale` - The scale of the result type
/// * `is_integral_div` - If true, performs integer division (truncates result)
fn spark_decimal_div_internal(
    args: &[ColumnarValue],
    result_precision: u8,
    result_scale: i8,
    is_integral_div: bool,
) -> Result<ColumnarValue> {
    assert_eq_or_internal_err!(
        args.len(),
        2,
        "decimal division expects exactly two arguments"
    );

    let left = &args[0];
    let right = &args[1];

    let (left, right): (ArrayRef, ArrayRef) = match (left, right) {
        (ColumnarValue::Array(l), ColumnarValue::Array(r)) => {
            (Arc::clone(l), Arc::clone(r))
        }
        (ColumnarValue::Scalar(l), ColumnarValue::Array(r)) => {
            (l.to_array_of_size(r.len())?, Arc::clone(r))
        }
        (ColumnarValue::Array(l), ColumnarValue::Scalar(r)) => {
            (Arc::clone(l), r.to_array_of_size(l.len())?)
        }
        (ColumnarValue::Scalar(l), ColumnarValue::Scalar(r)) => {
            (l.to_array()?, r.to_array()?)
        }
    };

    let left = left.as_primitive::<Decimal128Type>();
    let right = right.as_primitive::<Decimal128Type>();
    let (p1, s1) = get_precision_scale(left.data_type());
    let (p2, s2) = get_precision_scale(right.data_type());

    // Calculate the scale expansion needed
    // To get Decimal(p3, s3) from p1/p2, we need to widen s1 to s2 + s3 + 1
    let l_exp = ((s2 + result_scale + 1) as u32).saturating_sub(s1 as u32);
    let r_exp = (s1 as u32).saturating_sub((s2 + result_scale + 1) as u32);

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
            let res = if is_integral_div {
                div
            } else if div.is_negative() {
                div - &five
            } else {
                div + &five
            } / &ten;
            Ok(res.to_i128().unwrap_or(i128::MAX))
        })?
    } else {
        // Standard i128 calculation when precision is within bounds
        let l_mul = 10_i128.pow(l_exp);
        let r_mul = 10_i128.pow(r_exp);

        arrow::compute::kernels::arity::try_binary(left, right, |l, r| {
            let l = l * l_mul;
            let r = r * r_mul;
            // Legacy mode: divide by zero returns 0
            let div = if r == 0 { 0 } else { l / r };
            let res = if is_integral_div {
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
    spark_decimal_div_internal(args, result_precision, result_scale, false)
}

/// Spark-compatible integral decimal division function.
///
/// Performs integer division (truncates toward zero).
pub fn spark_decimal_integral_div(
    args: &[ColumnarValue],
    result_precision: u8,
    result_scale: i8,
) -> Result<ColumnarValue> {
    spark_decimal_div_internal(args, result_precision, result_scale, true)
}

/// SparkDecimalDiv implements the Spark-compatible decimal division function.
///
/// This UDF takes the result precision and scale as part of its configuration,
/// since Spark determines these at query planning time.
#[derive(Debug)]
pub struct SparkDecimalDiv {
    signature: Signature,
    result_precision: u8,
    result_scale: i8,
}

impl PartialEq for SparkDecimalDiv {
    fn eq(&self, other: &Self) -> bool {
        self.result_precision == other.result_precision
            && self.result_scale == other.result_scale
    }
}

impl Eq for SparkDecimalDiv {}

impl std::hash::Hash for SparkDecimalDiv {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.result_precision.hash(state);
        self.result_scale.hash(state);
    }
}

impl Default for SparkDecimalDiv {
    fn default() -> Self {
        Self::new(38, 18)
    }
}

impl SparkDecimalDiv {
    /// Create a new SparkDecimalDiv with the specified result precision and scale.
    pub fn new(result_precision: u8, result_scale: i8) -> Self {
        Self {
            signature: Signature::new(TypeSignature::Any(2), Volatility::Immutable),
            result_precision,
            result_scale,
        }
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
#[derive(Debug)]
pub struct SparkDecimalIntegralDiv {
    signature: Signature,
    result_precision: u8,
    result_scale: i8,
}

impl PartialEq for SparkDecimalIntegralDiv {
    fn eq(&self, other: &Self) -> bool {
        self.result_precision == other.result_precision
            && self.result_scale == other.result_scale
    }
}

impl Eq for SparkDecimalIntegralDiv {}

impl std::hash::Hash for SparkDecimalIntegralDiv {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.result_precision.hash(state);
        self.result_scale.hash(state);
    }
}

impl Default for SparkDecimalIntegralDiv {
    fn default() -> Self {
        Self::new(38, 0)
    }
}

impl SparkDecimalIntegralDiv {
    /// Create a new SparkDecimalIntegralDiv with the specified result precision and scale.
    pub fn new(result_precision: u8, result_scale: i8) -> Self {
        Self {
            signature: Signature::new(TypeSignature::Any(2), Volatility::Immutable),
            result_precision,
            result_scale,
        }
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
}
