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

use std::sync::Arc;

use arrow::array::{ArrayRef, BooleanArray, Scalar, new_null_array};
use arrow::compute::cast;
use arrow::compute::kernels::{
    boolean::{and, is_not_null, or},
    cmp::{eq, lt, neq},
    numeric::{add_wrapping, neg, rem},
    zip::zip,
};
use arrow::datatypes::DataType;
use arrow::error::ArrowError;
use datafusion_common::{Result, ScalarValue, assert_eq_or_internal_err};
use datafusion_expr::{
    ColumnarValue, ScalarFunctionArgs, ScalarUDFImpl, Signature, Volatility,
};

/// Returns a one element array holding negative zero, for the floating point
/// types only.
///
/// Arrow's comparison kernels order floating point values totally, so `-0.0`
/// compares as distinct from, and less than, `0.0`. Java, and therefore Spark,
/// treats `-0.0` as equal to zero and as not less than zero. The helpers below
/// use this to restore the IEEE 754 answer.
fn negative_zero(data_type: &DataType) -> Result<Option<ArrayRef>> {
    match data_type {
        DataType::Float16 | DataType::Float32 | DataType::Float64 => {
            let zero = ScalarValue::new_zero(data_type)?.to_array()?;
            Ok(Some(neg(zero.as_ref())?))
        }
        _ => Ok(None),
    }
}

/// Rows of `values` that equal zero, counting `-0.0` as zero.
fn is_zero(values: &ArrayRef) -> Result<BooleanArray> {
    let zero = ScalarValue::new_zero(values.data_type())?.to_array()?;
    let mask = eq(values, &Scalar::new(zero))?;
    match negative_zero(values.data_type())? {
        Some(negative_zero) => Ok(or(&mask, &eq(values, &Scalar::new(negative_zero))?)?),
        None => Ok(mask),
    }
}

/// Computes `rem(left, right)` with divide-by-zero handling.
/// In ANSI mode, a zero divisor of any numeric type causes an error, with
/// `-0.0` counting as zero; a row whose dividend is NULL never raises, to
/// match Spark's null-intolerant remainder. In legacy mode (ANSI off), zero
/// divisors are replaced with NULL before computing the remainder, so those
/// positions return NULL while others compute normally.
fn try_rem(
    left: &ArrayRef,
    right: &ArrayRef,
    enable_ansi_mode: bool,
) -> Result<ArrayRef> {
    let divisor_is_zero = is_zero(right)?;
    // Null out zero divisors so that the remainder kernels never see one:
    // division by zero then returns NULL instead of erroring (integers) or
    // returning NaN (floats). ANSI mode reports the error itself below, so
    // this substitution is harmless on rows that must raise.
    let null = Scalar::new(new_null_array(right.data_type(), 1));
    let safe_right = zip(&divisor_is_zero, &null, right)?;
    if enable_ansi_mode {
        // Spark's remainder expressions are null intolerant, so a row whose
        // dividend is NULL evaluates to NULL and never raises, even when the
        // divisor on that row is zero. Mask the check by the validity of the
        // dividend to match.
        let raises = and(&divisor_is_zero, &is_not_null(left.as_ref())?)?;
        if raises.iter().flatten().any(|raises| raises) {
            return Err(ArrowError::DivideByZero.into());
        }
    }
    Ok(rem(left, &safe_right)?)
}

/// Spark-compatible `mod` function
/// In ANSI mode, division by zero throws an error.
/// In legacy mode, division by zero returns NULL (Spark behavior).
pub fn spark_mod(
    args: &[ColumnarValue],
    enable_ansi_mode: bool,
) -> Result<ColumnarValue> {
    assert_eq_or_internal_err!(args.len(), 2, "mod expects exactly two arguments");
    let args = ColumnarValue::values_to_arrays(args)?;
    let result = try_rem(&args[0], &args[1], enable_ansi_mode)?;
    Ok(ColumnarValue::Array(result))
}

/// Rows of `values` that are less than zero, with `-0.0` counting as not
/// negative.
fn is_negative(values: &ArrayRef) -> Result<BooleanArray> {
    let zero = ScalarValue::new_zero(values.data_type())?.to_array()?;
    let mask = lt(values, &Scalar::new(zero))?;
    match negative_zero(values.data_type())? {
        Some(negative_zero) => {
            Ok(and(&mask, &neq(values, &Scalar::new(negative_zero))?)?)
        }
        None => Ok(mask),
    }
}

/// Spark-compatible `pmod` function
///
/// Spark evaluates `val r = a % n; if (r < 0) (r + n) % n else r`, so the
/// adjustment only applies where the plain remainder is negative.
///
/// In ANSI mode, division by zero throws an error.
/// In legacy mode, division by zero returns NULL (Spark behavior).
///
/// `pmod` does not share [`try_rem`] with `mod` because it needs the
/// zero-divisor-masked divisor again for the `(r + n) % n` step, which
/// [`try_rem`] does not hand back. The zero-divisor handling itself matches.
pub fn spark_pmod(
    args: &[ColumnarValue],
    enable_ansi_mode: bool,
) -> Result<ColumnarValue> {
    assert_eq_or_internal_err!(args.len(), 2, "pmod expects exactly two arguments");
    let args = ColumnarValue::values_to_arrays(args)?;
    let left = &args[0];
    let right = &args[1];

    let divisor_is_zero = is_zero(right)?;
    let divisor = if enable_ansi_mode {
        // Spark's `pmod` is null intolerant, so a row whose dividend is NULL
        // evaluates to NULL and never raises, even when the divisor on that row
        // is zero. Mask the check by the validity of the dividend to match.
        let raises = and(&divisor_is_zero, &is_not_null(left.as_ref())?)?;
        if raises.iter().flatten().any(|raises| raises) {
            return Err(ArrowError::DivideByZero.into());
        }
        Arc::clone(right)
    } else {
        // In legacy mode, null out zero divisors so that division by zero
        // returns NULL instead of erroring (integers) or returning NaN (floats).
        let null = Scalar::new(new_null_array(right.data_type(), 1));
        zip(&divisor_is_zero, &null, right)?
    };

    let remainder = rem(left, &divisor)?;

    // Spark evaluates `(r + n) % n` with Java arithmetic. Java promotes `byte`
    // and `short` operands to `int`, so those widths never overflow, while
    // `int` and `long` are evaluated at their own width and wrap around. Widen
    // the narrow integer types so the addition reproduces Java's promotion, and
    // use the wrapping addition elsewhere so that, for example,
    // `pmod(-1, -2147483648)` yields `2147483647` the way Spark does instead of
    // reporting an overflow.
    let widen = matches!(remainder.data_type(), DataType::Int8 | DataType::Int16);
    let (wide_remainder, wide_divisor) = if widen {
        (
            cast(&remainder, &DataType::Int32)?,
            cast(&divisor, &DataType::Int32)?,
        )
    } else {
        (Arc::clone(&remainder), Arc::clone(&divisor))
    };
    let shifted = add_wrapping(&wide_remainder, &wide_divisor)?;
    let shifted = rem(&shifted, &wide_divisor)?;
    let shifted = if shifted.data_type() == remainder.data_type() {
        shifted
    } else {
        cast(&shifted, remainder.data_type())?
    };

    // Select `(r + n) % n` only where `r < 0`. Adding zero to the other rows
    // instead would turn Spark's `-0.0` results into `0.0`.
    let result = zip(&is_negative(&remainder)?, &shifted, &remainder)?;
    Ok(ColumnarValue::Array(result))
}

/// SparkMod implements the Spark-compatible modulo function
#[derive(Debug, PartialEq, Eq, Hash)]
pub struct SparkMod {
    signature: Signature,
}

impl Default for SparkMod {
    fn default() -> Self {
        Self::new()
    }
}

impl SparkMod {
    pub fn new() -> Self {
        Self {
            signature: Signature::numeric(2, Volatility::Immutable),
        }
    }
}

impl ScalarUDFImpl for SparkMod {
    fn name(&self) -> &str {
        "mod"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        assert_eq_or_internal_err!(
            arg_types.len(),
            2,
            "mod expects exactly two arguments"
        );

        // Return the same type as the first argument for simplicity
        // Arrow's rem function handles type promotion internally
        Ok(arg_types[0].clone())
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        spark_mod(&args.args, args.config_options.execution.enable_ansi_mode)
    }
}

/// SparkMod implements the Spark-compatible modulo function
#[derive(Debug, PartialEq, Eq, Hash)]
pub struct SparkPmod {
    signature: Signature,
}

impl Default for SparkPmod {
    fn default() -> Self {
        Self::new()
    }
}

impl SparkPmod {
    pub fn new() -> Self {
        Self {
            signature: Signature::numeric(2, Volatility::Immutable),
        }
    }
}

impl ScalarUDFImpl for SparkPmod {
    fn name(&self) -> &str {
        "pmod"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        assert_eq_or_internal_err!(
            arg_types.len(),
            2,
            "pmod expects exactly two arguments"
        );

        // Return the same type as the first argument for simplicity
        // Arrow's rem function handles type promotion internally
        Ok(arg_types[0].clone())
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        spark_pmod(&args.args, args.config_options.execution.enable_ansi_mode)
    }
}

#[cfg(test)]
mod test {
    use std::sync::Arc;

    use super::*;
    use arrow::array::*;
    use datafusion_common::ScalarValue;

    #[test]
    fn test_mod_int32() {
        let left = Int32Array::from(vec![Some(10), Some(7), Some(15), None]);
        let right = Int32Array::from(vec![Some(3), Some(2), Some(4), Some(5)]);

        let left_value = ColumnarValue::Array(Arc::new(left));
        let right_value = ColumnarValue::Array(Arc::new(right));

        let result = spark_mod(&[left_value, right_value], false).unwrap();

        if let ColumnarValue::Array(result_array) = result {
            let result_int32 =
                result_array.as_any().downcast_ref::<Int32Array>().unwrap();
            assert_eq!(result_int32.value(0), 1); // 10 % 3 = 1
            assert_eq!(result_int32.value(1), 1); // 7 % 2 = 1
            assert_eq!(result_int32.value(2), 3); // 15 % 4 = 3
            assert!(result_int32.is_null(3)); // None % 5 = None
        } else {
            panic!("Expected array result");
        }
    }

    #[test]
    fn test_mod_int64() {
        let left = Int64Array::from(vec![Some(100), Some(50), Some(200)]);
        let right = Int64Array::from(vec![Some(30), Some(25), Some(60)]);

        let left_value = ColumnarValue::Array(Arc::new(left));
        let right_value = ColumnarValue::Array(Arc::new(right));

        let result = spark_mod(&[left_value, right_value], false).unwrap();

        if let ColumnarValue::Array(result_array) = result {
            let result_int64 =
                result_array.as_any().downcast_ref::<Int64Array>().unwrap();
            assert_eq!(result_int64.value(0), 10); // 100 % 30 = 10
            assert_eq!(result_int64.value(1), 0); // 50 % 25 = 0
            assert_eq!(result_int64.value(2), 20); // 200 % 60 = 20
        } else {
            panic!("Expected array result");
        }
    }

    #[test]
    fn test_mod_float64() {
        let left = Float64Array::from(vec![
            Some(10.5),
            Some(7.2),
            Some(15.8),
            Some(f64::NAN),
            Some(f64::INFINITY),
            Some(5.0),
            Some(5.0),
            Some(f64::NAN),
            Some(f64::INFINITY),
            Some(10.5),
            Some(15.8),
        ]);
        let right = Float64Array::from(vec![
            Some(3.0),
            Some(2.5),
            Some(4.2),
            Some(2.0),
            Some(2.0),
            Some(f64::NAN),
            Some(f64::INFINITY),
            Some(f64::INFINITY),
            Some(f64::NAN),
            Some(0.0),
            Some(0.0),
        ]);

        let left_value = ColumnarValue::Array(Arc::new(left));
        let right_value = ColumnarValue::Array(Arc::new(right));

        let result = spark_mod(&[left_value, right_value], false).unwrap();

        if let ColumnarValue::Array(result_array) = result {
            let result_float64 = result_array
                .as_any()
                .downcast_ref::<Float64Array>()
                .unwrap();
            // Regular cases
            assert!((result_float64.value(0) - 1.5).abs() < f64::EPSILON); // 10.5 % 3.0 = 1.5
            assert!((result_float64.value(1) - 2.2).abs() < f64::EPSILON); // 7.2 % 2.5 = 2.2
            assert!((result_float64.value(2) - 3.2).abs() < f64::EPSILON); // 15.8 % 4.2 = 3.2
            // nan % 2.0 = nan
            assert!(result_float64.value(3).is_nan());
            // inf % 2.0 = nan (IEEE 754)
            assert!(result_float64.value(4).is_nan());
            // 5.0 % nan = nan
            assert!(result_float64.value(5).is_nan());
            // 5.0 % inf = 5.0
            assert!((result_float64.value(6) - 5.0).abs() < f64::EPSILON);
            // nan % inf = nan
            assert!(result_float64.value(7).is_nan());
            // inf % nan = nan
            assert!(result_float64.value(8).is_nan());
            // Division by zero returns NULL
            assert!(result_float64.is_null(9)); // 10.5 % 0.0 = NULL
            assert!(result_float64.is_null(10)); // 15.8 % 0.0 = NULL
        } else {
            panic!("Expected array result");
        }
    }

    #[test]
    fn test_mod_float32() {
        let left = Float32Array::from(vec![
            Some(10.5),
            Some(7.2),
            Some(15.8),
            Some(f32::NAN),
            Some(f32::INFINITY),
            Some(5.0),
            Some(5.0),
            Some(f32::NAN),
            Some(f32::INFINITY),
            Some(10.5),
            Some(15.8),
        ]);
        let right = Float32Array::from(vec![
            Some(3.0),
            Some(2.5),
            Some(4.2),
            Some(2.0),
            Some(2.0),
            Some(f32::NAN),
            Some(f32::INFINITY),
            Some(f32::INFINITY),
            Some(f32::NAN),
            Some(0.0),
            Some(0.0),
        ]);

        let left_value = ColumnarValue::Array(Arc::new(left));
        let right_value = ColumnarValue::Array(Arc::new(right));

        let result = spark_mod(&[left_value, right_value], false).unwrap();

        if let ColumnarValue::Array(result_array) = result {
            let result_float32 = result_array
                .as_any()
                .downcast_ref::<Float32Array>()
                .unwrap();
            // Regular cases
            assert!((result_float32.value(0) - 1.5).abs() < f32::EPSILON); // 10.5 % 3.0 = 1.5
            assert!((result_float32.value(1) - 2.2).abs() < f32::EPSILON * 3.0); // 7.2 % 2.5 = 2.2
            assert!((result_float32.value(2) - 3.2).abs() < f32::EPSILON * 10.0); // 15.8 % 4.2 = 3.2
            // nan % 2.0 = nan
            assert!(result_float32.value(3).is_nan());
            // inf % 2.0 = nan (IEEE 754)
            assert!(result_float32.value(4).is_nan());
            // 5.0 % nan = nan
            assert!(result_float32.value(5).is_nan());
            // 5.0 % inf = 5.0
            assert!((result_float32.value(6) - 5.0).abs() < f32::EPSILON);
            // nan % inf = nan
            assert!(result_float32.value(7).is_nan());
            // inf % nan = nan
            assert!(result_float32.value(8).is_nan());
            // Division by zero returns NULL
            assert!(result_float32.is_null(9)); // 10.5 % 0.0 = NULL
            assert!(result_float32.is_null(10)); // 15.8 % 0.0 = NULL
        } else {
            panic!("Expected array result");
        }
    }

    #[test]
    fn test_mod_scalar() {
        let left = Int32Array::from(vec![Some(10), Some(7), Some(15)]);
        let right_value = ColumnarValue::Scalar(ScalarValue::Int32(Some(3)));

        let left_value = ColumnarValue::Array(Arc::new(left));

        let result = spark_mod(&[left_value, right_value], false).unwrap();

        if let ColumnarValue::Array(result_array) = result {
            let result_int32 =
                result_array.as_any().downcast_ref::<Int32Array>().unwrap();
            assert_eq!(result_int32.value(0), 1); // 10 % 3 = 1
            assert_eq!(result_int32.value(1), 1); // 7 % 3 = 1
            assert_eq!(result_int32.value(2), 0); // 15 % 3 = 0
        } else {
            panic!("Expected array result");
        }
    }

    #[test]
    fn test_mod_wrong_arg_count() {
        let left = Int32Array::from(vec![Some(10)]);
        let left_value = ColumnarValue::Array(Arc::new(left));

        let result = spark_mod(&[left_value], false);
        assert!(result.is_err());
    }

    #[test]
    fn test_mod_zero_division_legacy() {
        // In legacy mode (ANSI off), division by zero returns NULL per-element
        let left = Int32Array::from(vec![Some(10), Some(7), Some(15)]);
        let right = Int32Array::from(vec![Some(0), Some(2), Some(4)]);

        let left_value = ColumnarValue::Array(Arc::new(left));
        let right_value = ColumnarValue::Array(Arc::new(right));

        let result = spark_mod(&[left_value, right_value], false).unwrap();

        if let ColumnarValue::Array(result_array) = result {
            let result_int32 =
                result_array.as_any().downcast_ref::<Int32Array>().unwrap();
            assert!(result_int32.is_null(0)); // 10 % 0 = NULL
            assert_eq!(result_int32.value(1), 1); // 7 % 2 = 1
            assert_eq!(result_int32.value(2), 3); // 15 % 4 = 3
        } else {
            panic!("Expected array result");
        }
    }

    #[test]
    fn test_mod_zero_division_ansi() {
        // In ANSI mode, division by zero should error
        let left = Int32Array::from(vec![Some(10), Some(7), Some(15)]);
        let right = Int32Array::from(vec![Some(0), Some(2), Some(4)]);

        let left_value = ColumnarValue::Array(Arc::new(left));
        let right_value = ColumnarValue::Array(Arc::new(right));

        let result = spark_mod(&[left_value, right_value], true);
        assert!(result.is_err());
    }

    #[test]
    fn test_mod_zero_division_ansi_float() {
        // In ANSI mode a zero divisor of any numeric type must raise,
        // including floating point, where Arrow's `rem` follows IEEE 754
        // and quietly returns NaN (#23894)
        let left = Float64Array::from(vec![Some(10.5), Some(7.2)]);
        let right = Float64Array::from(vec![Some(0.0), Some(2.0)]);

        let left_value = ColumnarValue::Array(Arc::new(left));
        let right_value = ColumnarValue::Array(Arc::new(right));

        let result = spark_mod(&[left_value, right_value], true);
        assert!(result.is_err());
    }

    #[test]
    fn test_mod_negative_zero_divisor_legacy() {
        // `-0.0` counts as a zero divisor, so it returns NULL in legacy
        // mode rather than NaN (#23894)
        let left = Float64Array::from(vec![Some(10.5), Some(7.5)]);
        let right = Float64Array::from(vec![Some(-0.0), Some(2.0)]);

        let left_value = ColumnarValue::Array(Arc::new(left));
        let right_value = ColumnarValue::Array(Arc::new(right));

        let result = spark_mod(&[left_value, right_value], false).unwrap();

        if let ColumnarValue::Array(result_array) = result {
            let result_float64 = result_array
                .as_any()
                .downcast_ref::<Float64Array>()
                .unwrap();
            assert!(result_float64.is_null(0)); // 10.5 % -0.0 = NULL
            assert_eq!(result_float64.value(1), 1.5); // 7.5 % 2.0 = 1.5
        } else {
            panic!("Expected array result");
        }
    }

    #[test]
    fn test_mod_negative_zero_divisor_ansi() {
        // `-0.0` counts as a zero divisor, so it raises in ANSI mode (#23894)
        let left = Float64Array::from(vec![Some(10.5)]);
        let right = Float64Array::from(vec![Some(-0.0)]);

        let left_value = ColumnarValue::Array(Arc::new(left));
        let right_value = ColumnarValue::Array(Arc::new(right));

        let result = spark_mod(&[left_value, right_value], true);
        assert!(result.is_err());
    }

    #[test]
    fn test_mod_zero_division_ansi_null_dividend() {
        // Spark's remainder expressions are null intolerant: a NULL dividend
        // short-circuits to NULL before the divisor is validated, so a zero
        // divisor on such a row must not raise, even in ANSI mode (#23894)
        let left = Int32Array::from(vec![None, Some(10)]);
        let right = Int32Array::from(vec![Some(0), Some(3)]);

        let left_value = ColumnarValue::Array(Arc::new(left));
        let right_value = ColumnarValue::Array(Arc::new(right));

        let result = spark_mod(&[left_value, right_value], true).unwrap();

        if let ColumnarValue::Array(result_array) = result {
            let result_int32 =
                result_array.as_any().downcast_ref::<Int32Array>().unwrap();
            assert!(result_int32.is_null(0)); // NULL % 0 = NULL (no error)
            assert_eq!(result_int32.value(1), 1); // 10 % 3 = 1
        } else {
            panic!("Expected array result");
        }

        // Same for floating point
        let left = Float64Array::from(vec![None, Some(10.5)]);
        let right = Float64Array::from(vec![Some(0.0), Some(2.0)]);

        let left_value = ColumnarValue::Array(Arc::new(left));
        let right_value = ColumnarValue::Array(Arc::new(right));

        let result = spark_mod(&[left_value, right_value], true).unwrap();

        if let ColumnarValue::Array(result_array) = result {
            let result_float64 = result_array
                .as_any()
                .downcast_ref::<Float64Array>()
                .unwrap();
            assert!(result_float64.is_null(0)); // NULL % 0.0 = NULL (no error)
            assert!((result_float64.value(1) - 0.5).abs() < f64::EPSILON); // 10.5 % 2.0 = 0.5
        } else {
            panic!("Expected array result");
        }
    }

    // PMOD tests
    #[test]
    fn test_pmod_int32() {
        let left = Int32Array::from(vec![Some(10), Some(-7), Some(15), Some(-15), None]);
        let right = Int32Array::from(vec![Some(3), Some(3), Some(4), Some(4), Some(5)]);

        let left_value = ColumnarValue::Array(Arc::new(left));
        let right_value = ColumnarValue::Array(Arc::new(right));

        let result = spark_pmod(&[left_value, right_value], false).unwrap();

        if let ColumnarValue::Array(result_array) = result {
            let result_int32 =
                result_array.as_any().downcast_ref::<Int32Array>().unwrap();
            assert_eq!(result_int32.value(0), 1); // 10 pmod 3 = 1
            assert_eq!(result_int32.value(1), 2); // -7 pmod 3 = 2 (positive remainder)
            assert_eq!(result_int32.value(2), 3); // 15 pmod 4 = 3
            assert_eq!(result_int32.value(3), 1); // -15 pmod 4 = 1 (positive remainder)
            assert!(result_int32.is_null(4)); // None pmod 5 = None
        } else {
            panic!("Expected array result");
        }
    }

    #[test]
    fn test_pmod_int64() {
        let left = Int64Array::from(vec![Some(100), Some(-50), Some(200), Some(-200)]);
        let right = Int64Array::from(vec![Some(30), Some(30), Some(60), Some(60)]);

        let left_value = ColumnarValue::Array(Arc::new(left));
        let right_value = ColumnarValue::Array(Arc::new(right));

        let result = spark_pmod(&[left_value, right_value], false).unwrap();

        if let ColumnarValue::Array(result_array) = result {
            let result_int64 =
                result_array.as_any().downcast_ref::<Int64Array>().unwrap();
            assert_eq!(result_int64.value(0), 10); // 100 pmod 30 = 10
            assert_eq!(result_int64.value(1), 10); // -50 pmod 30 = 10 (positive remainder)
            assert_eq!(result_int64.value(2), 20); // 200 pmod 60 = 20
            assert_eq!(result_int64.value(3), 40); // -200 pmod 60 = 40 (positive remainder)
        } else {
            panic!("Expected array result");
        }
    }

    #[test]
    fn test_pmod_float64() {
        let left = Float64Array::from(vec![
            Some(10.5),
            Some(-7.2),
            Some(15.8),
            Some(-15.8),
            Some(f64::NAN),
            Some(f64::INFINITY),
            Some(5.0),
            Some(-5.0),
            Some(10.5),
            Some(-7.2),
        ]);
        let right = Float64Array::from(vec![
            Some(3.0),
            Some(3.0),
            Some(4.2),
            Some(4.2),
            Some(2.0),
            Some(2.0),
            Some(f64::INFINITY),
            Some(f64::INFINITY),
            Some(0.0),
            Some(0.0),
        ]);

        let left_value = ColumnarValue::Array(Arc::new(left));
        let right_value = ColumnarValue::Array(Arc::new(right));

        let result = spark_pmod(&[left_value, right_value], false).unwrap();

        if let ColumnarValue::Array(result_array) = result {
            let result_float64 = result_array
                .as_any()
                .downcast_ref::<Float64Array>()
                .unwrap();
            // Regular cases
            assert!((result_float64.value(0) - 1.5).abs() < f64::EPSILON); // 10.5 pmod 3.0 = 1.5
            assert!((result_float64.value(1) - 1.8).abs() < f64::EPSILON * 3.0); // -7.2 pmod 3.0 = 1.8 (positive)
            assert!((result_float64.value(2) - 3.2).abs() < f64::EPSILON * 3.0); // 15.8 pmod 4.2 = 3.2
            assert!((result_float64.value(3) - 1.0).abs() < f64::EPSILON * 3.0); // -15.8 pmod 4.2 = 1.0 (positive)
            // nan pmod 2.0 = nan
            assert!(result_float64.value(4).is_nan());
            // inf pmod 2.0 = nan (IEEE 754)
            assert!(result_float64.value(5).is_nan());
            // 5.0 pmod inf = 5.0
            assert!((result_float64.value(6) - 5.0).abs() < f64::EPSILON);
            // -5.0 pmod inf = NaN
            assert!(result_float64.value(7).is_nan());
            // Division by zero returns NULL
            assert!(result_float64.is_null(8)); // 10.5 pmod 0.0 = NULL
            assert!(result_float64.is_null(9)); // -7.2 pmod 0.0 = NULL
        } else {
            panic!("Expected array result");
        }
    }

    #[test]
    fn test_pmod_float32() {
        let left = Float32Array::from(vec![
            Some(10.5),
            Some(-7.2),
            Some(15.8),
            Some(-15.8),
            Some(f32::NAN),
            Some(f32::INFINITY),
            Some(5.0),
            Some(-5.0),
            Some(10.5),
            Some(-7.2),
        ]);
        let right = Float32Array::from(vec![
            Some(3.0),
            Some(3.0),
            Some(4.2),
            Some(4.2),
            Some(2.0),
            Some(2.0),
            Some(f32::INFINITY),
            Some(f32::INFINITY),
            Some(0.0),
            Some(0.0),
        ]);

        let left_value = ColumnarValue::Array(Arc::new(left));
        let right_value = ColumnarValue::Array(Arc::new(right));

        let result = spark_pmod(&[left_value, right_value], false).unwrap();

        if let ColumnarValue::Array(result_array) = result {
            let result_float32 = result_array
                .as_any()
                .downcast_ref::<Float32Array>()
                .unwrap();
            // Regular cases
            assert!((result_float32.value(0) - 1.5).abs() < f32::EPSILON); // 10.5 pmod 3.0 = 1.5
            assert!((result_float32.value(1) - 1.8).abs() < f32::EPSILON * 3.0); // -7.2 pmod 3.0 = 1.8 (positive)
            assert!((result_float32.value(2) - 3.2).abs() < f32::EPSILON * 10.0); // 15.8 pmod 4.2 = 3.2
            assert!((result_float32.value(3) - 1.0).abs() < f32::EPSILON * 10.0); // -15.8 pmod 4.2 = 1.0 (positive)
            // nan pmod 2.0 = nan
            assert!(result_float32.value(4).is_nan());
            // inf pmod 2.0 = nan (IEEE 754)
            assert!(result_float32.value(5).is_nan());
            // 5.0 pmod inf = 5.0
            assert!((result_float32.value(6) - 5.0).abs() < f32::EPSILON * 10.0);
            // -5.0 pmod inf = NaN
            assert!(result_float32.value(7).is_nan());
            // Division by zero returns NULL
            assert!(result_float32.is_null(8)); // 10.5 pmod 0.0 = NULL
            assert!(result_float32.is_null(9)); // -7.2 pmod 0.0 = NULL
        } else {
            panic!("Expected array result");
        }
    }

    #[test]
    fn test_pmod_scalar() {
        let left = Int32Array::from(vec![Some(10), Some(-7), Some(15), Some(-15)]);
        let right_value = ColumnarValue::Scalar(ScalarValue::Int32(Some(3)));

        let left_value = ColumnarValue::Array(Arc::new(left));

        let result = spark_pmod(&[left_value, right_value], false).unwrap();

        if let ColumnarValue::Array(result_array) = result {
            let result_int32 =
                result_array.as_any().downcast_ref::<Int32Array>().unwrap();
            assert_eq!(result_int32.value(0), 1); // 10 pmod 3 = 1
            assert_eq!(result_int32.value(1), 2); // -7 pmod 3 = 2 (positive remainder)
            assert_eq!(result_int32.value(2), 0); // 15 pmod 3 = 0
            assert_eq!(result_int32.value(3), 0); // -15 pmod 3 = 0 (positive remainder)
        } else {
            panic!("Expected array result");
        }
    }

    #[test]
    fn test_pmod_wrong_arg_count() {
        let left = Int32Array::from(vec![Some(10)]);
        let left_value = ColumnarValue::Array(Arc::new(left));

        let result = spark_pmod(&[left_value], false);
        assert!(result.is_err());
    }

    #[test]
    fn test_pmod_zero_division_legacy() {
        // In legacy mode (ANSI off), division by zero returns NULL per-element
        let left = Int32Array::from(vec![Some(10), Some(-7), Some(15)]);
        let right = Int32Array::from(vec![Some(0), Some(0), Some(4)]);

        let left_value = ColumnarValue::Array(Arc::new(left));
        let right_value = ColumnarValue::Array(Arc::new(right));

        let result = spark_pmod(&[left_value, right_value], false).unwrap();

        if let ColumnarValue::Array(result_array) = result {
            let result_int32 =
                result_array.as_any().downcast_ref::<Int32Array>().unwrap();
            assert!(result_int32.is_null(0)); // 10 pmod 0 = NULL
            assert!(result_int32.is_null(1)); // -7 pmod 0 = NULL
            assert_eq!(result_int32.value(2), 3); // 15 pmod 4 = 3
        } else {
            panic!("Expected array result");
        }
    }

    #[test]
    fn test_pmod_zero_division_ansi() {
        // In ANSI mode, division by zero should error
        let left = Int32Array::from(vec![Some(10), Some(-7), Some(15)]);
        let right = Int32Array::from(vec![Some(0), Some(0), Some(4)]);

        let left_value = ColumnarValue::Array(Arc::new(left));
        let right_value = ColumnarValue::Array(Arc::new(right));

        let result = spark_pmod(&[left_value, right_value], true);
        assert!(result.is_err());
    }

    #[test]
    fn test_pmod_zero_division_ansi_float() {
        // pmod routes through `try_rem` twice, so it needs the same coverage
        // as mod: in ANSI mode a zero divisor of any numeric type must
        // raise, including floating point, where Arrow's `rem` follows
        // IEEE 754 and quietly returns NaN (#23894)
        let left = Float64Array::from(vec![Some(10.5), Some(7.2)]);
        let right = Float64Array::from(vec![Some(0.0), Some(2.0)]);

        let left_value = ColumnarValue::Array(Arc::new(left));
        let right_value = ColumnarValue::Array(Arc::new(right));

        let result = spark_pmod(&[left_value, right_value], true);
        assert!(result.is_err());
    }

    #[test]
    fn test_pmod_negative_zero_divisor_legacy() {
        // `-0.0` counts as a zero divisor, so it returns NULL in legacy
        // mode rather than NaN (#23894)
        let left = Float64Array::from(vec![Some(10.5), Some(-7.5)]);
        let right = Float64Array::from(vec![Some(-0.0), Some(2.0)]);

        let left_value = ColumnarValue::Array(Arc::new(left));
        let right_value = ColumnarValue::Array(Arc::new(right));

        let result = spark_pmod(&[left_value, right_value], false).unwrap();

        if let ColumnarValue::Array(result_array) = result {
            let result_float64 = result_array
                .as_any()
                .downcast_ref::<Float64Array>()
                .unwrap();
            assert!(result_float64.is_null(0)); // 10.5 pmod -0.0 = NULL
            assert!((result_float64.value(1) - 0.5).abs() < f64::EPSILON); // -7.5 pmod 2.0 = 0.5
        } else {
            panic!("Expected array result");
        }
    }

    #[test]
    fn test_pmod_negative_zero_divisor_ansi() {
        // `-0.0` counts as a zero divisor, so it raises in ANSI mode (#23894)
        let left = Float64Array::from(vec![Some(10.5)]);
        let right = Float64Array::from(vec![Some(-0.0)]);

        let left_value = ColumnarValue::Array(Arc::new(left));
        let right_value = ColumnarValue::Array(Arc::new(right));

        let result = spark_pmod(&[left_value, right_value], true);
        assert!(result.is_err());
    }

    #[test]
    fn test_pmod_zero_division_ansi_null_dividend() {
        // Spark's remainder expressions are null intolerant: a NULL dividend
        // short-circuits to NULL before the divisor is validated, so a zero
        // divisor on such a row must not raise, even in ANSI mode (#23894)
        let left = Int32Array::from(vec![None, Some(10)]);
        let right = Int32Array::from(vec![Some(0), Some(3)]);

        let left_value = ColumnarValue::Array(Arc::new(left));
        let right_value = ColumnarValue::Array(Arc::new(right));

        let result = spark_pmod(&[left_value, right_value], true).unwrap();

        if let ColumnarValue::Array(result_array) = result {
            let result_int32 =
                result_array.as_any().downcast_ref::<Int32Array>().unwrap();
            assert!(result_int32.is_null(0)); // NULL pmod 0 = NULL (no error)
            assert_eq!(result_int32.value(1), 1); // 10 pmod 3 = 1
        } else {
            panic!("Expected array result");
        }

        // Same for floating point
        let left = Float64Array::from(vec![None, Some(10.5)]);
        let right = Float64Array::from(vec![Some(0.0), Some(2.0)]);

        let left_value = ColumnarValue::Array(Arc::new(left));
        let right_value = ColumnarValue::Array(Arc::new(right));

        let result = spark_pmod(&[left_value, right_value], true).unwrap();

        if let ColumnarValue::Array(result_array) = result {
            let result_float64 = result_array
                .as_any()
                .downcast_ref::<Float64Array>()
                .unwrap();
            assert!(result_float64.is_null(0)); // NULL pmod 0.0 = NULL (no error)
            assert!((result_float64.value(1) - 0.5).abs() < f64::EPSILON); // 10.5 pmod 2.0 = 0.5
        } else {
            panic!("Expected array result");
        }
    }

    #[test]
    fn test_pmod_negative_divisor() {
        // PMOD with negative divisor should still work like regular mod
        let left = Int32Array::from(vec![Some(10), Some(-7), Some(15)]);
        let right = Int32Array::from(vec![Some(-3), Some(-3), Some(-4)]);

        let left_value = ColumnarValue::Array(Arc::new(left));
        let right_value = ColumnarValue::Array(Arc::new(right));

        let result = spark_pmod(&[left_value, right_value], false).unwrap();

        if let ColumnarValue::Array(result_array) = result {
            let result_int32 =
                result_array.as_any().downcast_ref::<Int32Array>().unwrap();
            assert_eq!(result_int32.value(0), 1); // 10 pmod -3 = 1
            assert_eq!(result_int32.value(1), -1); // -7 pmod -3 = -1
            assert_eq!(result_int32.value(2), 3); // 15 pmod -4 = 3
        } else {
            panic!("Expected array result");
        }
    }

    /// Spark keeps the sign of a negative zero remainder, because `r < 0` is
    /// false for `-0.0` in Java. The `.slt` runner renders `-0.0` and `0.0`
    /// identically, so the sign can only be asserted here.
    #[test]
    fn test_pmod_negative_zero_result() {
        let left = Float64Array::from(vec![
            Some(-5.0),
            Some(-3.0),
            Some(-0.0),
            Some(5.0),
            Some(0.0),
        ]);
        let right = Float64Array::from(vec![
            Some(2.5),
            Some(3.0),
            Some(3.0),
            Some(2.5),
            Some(3.0),
        ]);

        let result = spark_pmod(
            &[
                ColumnarValue::Array(Arc::new(left)),
                ColumnarValue::Array(Arc::new(right)),
            ],
            false,
        )
        .unwrap();

        if let ColumnarValue::Array(result_array) = result {
            let result = result_array
                .as_any()
                .downcast_ref::<Float64Array>()
                .unwrap();
            // -5.0 pmod 2.5 = -0.0
            assert_eq!(result.value(0), 0.0);
            assert!(result.value(0).is_sign_negative());
            // -3.0 pmod 3.0 = -0.0
            assert!(result.value(1).is_sign_negative());
            // -0.0 pmod 3.0 = -0.0
            assert!(result.value(2).is_sign_negative());
            // 5.0 pmod 2.5 = 0.0
            assert!(result.value(3).is_sign_positive());
            // 0.0 pmod 3.0 = 0.0
            assert!(result.value(4).is_sign_positive());
        } else {
            panic!("Expected array result");
        }
    }

    /// Spark evaluates `(r + n) % n` with Java arithmetic, which wraps around
    /// for `int` and `long` and promotes `byte` and `short` to `int`.
    #[test]
    fn test_pmod_integer_boundaries() {
        let left =
            Int32Array::from(vec![Some(-1), Some(-2), Some(i32::MAX), Some(i32::MIN)]);
        let right = Int32Array::from(vec![
            Some(i32::MIN),
            Some(i32::MIN),
            Some(i32::MIN),
            Some(-1),
        ]);
        let result = spark_pmod(
            &[
                ColumnarValue::Array(Arc::new(left)),
                ColumnarValue::Array(Arc::new(right)),
            ],
            false,
        )
        .unwrap();
        if let ColumnarValue::Array(result_array) = result {
            let result = result_array.as_any().downcast_ref::<Int32Array>().unwrap();
            assert_eq!(result.value(0), i32::MAX);
            assert_eq!(result.value(1), 2147483646);
            assert_eq!(result.value(2), i32::MAX);
            assert_eq!(result.value(3), 0);
        } else {
            panic!("Expected array result");
        }

        let left = Int64Array::from(vec![Some(-1), Some(i64::MIN)]);
        let right = Int64Array::from(vec![Some(i64::MIN), Some(-1)]);
        let result = spark_pmod(
            &[
                ColumnarValue::Array(Arc::new(left)),
                ColumnarValue::Array(Arc::new(right)),
            ],
            false,
        )
        .unwrap();
        if let ColumnarValue::Array(result_array) = result {
            let result = result_array.as_any().downcast_ref::<Int64Array>().unwrap();
            assert_eq!(result.value(0), i64::MAX);
            assert_eq!(result.value(1), 0);
        } else {
            panic!("Expected array result");
        }

        // Java widens byte and short to int, so `-1 pmod -128` stays `-1`
        // rather than wrapping to `127`.
        let left = Int8Array::from(vec![Some(-1), Some(i8::MIN)]);
        let right = Int8Array::from(vec![Some(i8::MIN), Some(-1)]);
        let result = spark_pmod(
            &[
                ColumnarValue::Array(Arc::new(left)),
                ColumnarValue::Array(Arc::new(right)),
            ],
            false,
        )
        .unwrap();
        if let ColumnarValue::Array(result_array) = result {
            let result = result_array.as_any().downcast_ref::<Int8Array>().unwrap();
            assert_eq!(result.value(0), -1);
            assert_eq!(result.value(1), 0);
        } else {
            panic!("Expected array result");
        }

        let left = Int16Array::from(vec![Some(-1), Some(i16::MIN), Some(128)]);
        let right = Int16Array::from(vec![Some(i16::MIN), Some(-1), Some(i16::MIN)]);
        let result = spark_pmod(
            &[
                ColumnarValue::Array(Arc::new(left)),
                ColumnarValue::Array(Arc::new(right)),
            ],
            false,
        )
        .unwrap();
        if let ColumnarValue::Array(result_array) = result {
            let result = result_array.as_any().downcast_ref::<Int16Array>().unwrap();
            assert_eq!(result.value(0), -1);
            assert_eq!(result.value(1), 0);
            assert_eq!(result.value(2), 128);
        } else {
            panic!("Expected array result");
        }
    }

    /// Spark raises on a zero divisor of any numeric type in ANSI mode, and
    /// treats `-0.0` as a zero divisor.
    #[test]
    fn test_pmod_zero_divisor_by_type() {
        let float_args = |divisor: f64| {
            [
                ColumnarValue::Array(Arc::new(Float64Array::from(vec![Some(10.5)]))),
                ColumnarValue::Array(Arc::new(Float64Array::from(vec![Some(divisor)]))),
            ]
        };

        for divisor in [0.0, -0.0] {
            assert!(spark_pmod(&float_args(divisor), true).is_err());

            let result = spark_pmod(&float_args(divisor), false).unwrap();
            if let ColumnarValue::Array(result_array) = result {
                assert!(result_array.is_null(0));
            } else {
                panic!("Expected array result");
            }
        }

        // A NULL dividend short circuits to NULL before the divisor is
        // validated, so ANSI mode does not raise here.
        let args = [
            ColumnarValue::Array(Arc::new(Int32Array::from(vec![None, Some(-7)]))),
            ColumnarValue::Array(Arc::new(Int32Array::from(vec![Some(0), Some(3)]))),
        ];
        let result = spark_pmod(&args, true).unwrap();
        if let ColumnarValue::Array(result_array) = result {
            let result = result_array.as_any().downcast_ref::<Int32Array>().unwrap();
            assert!(result.is_null(0));
            assert_eq!(result.value(1), 2);
        } else {
            panic!("Expected array result");
        }
    }

    #[test]
    fn test_pmod_edge_cases() {
        // Test edge cases for PMOD
        let left = Int32Array::from(vec![
            Some(0),  // 0 pmod 5 = 0
            Some(-1), // -1 pmod 5 = 4
            Some(1),  // 1 pmod 5 = 1
            Some(-5), // -5 pmod 5 = 0
            Some(5),  // 5 pmod 5 = 0
            Some(-6), // -6 pmod 5 = 4
            Some(6),  // 6 pmod 5 = 1
        ]);
        let right = Int32Array::from(vec![
            Some(5),
            Some(5),
            Some(5),
            Some(5),
            Some(5),
            Some(5),
            Some(5),
        ]);

        let left_value = ColumnarValue::Array(Arc::new(left));
        let right_value = ColumnarValue::Array(Arc::new(right));

        let result = spark_pmod(&[left_value, right_value], false).unwrap();

        if let ColumnarValue::Array(result_array) = result {
            let result_int32 =
                result_array.as_any().downcast_ref::<Int32Array>().unwrap();
            assert_eq!(result_int32.value(0), 0); // 0 pmod 5 = 0
            assert_eq!(result_int32.value(1), 4); // -1 pmod 5 = 4
            assert_eq!(result_int32.value(2), 1); // 1 pmod 5 = 1
            assert_eq!(result_int32.value(3), 0); // -5 pmod 5 = 0
            assert_eq!(result_int32.value(4), 0); // 5 pmod 5 = 0
            assert_eq!(result_int32.value(5), 4); // -6 pmod 5 = 4
            assert_eq!(result_int32.value(6), 1); // 6 pmod 5 = 1
        } else {
            panic!("Expected array result");
        }
    }
}
