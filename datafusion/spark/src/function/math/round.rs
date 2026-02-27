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

use arrow::array::{
    Array, ArrowNativeTypeOp, AsArray, Decimal128Array, Int8Array, Int16Array,
    Int32Array, Int64Array,
};
use arrow::compute::kernels::arity::try_unary;
use arrow::datatypes::{
    DataType, Decimal128Type, Field, FieldRef, Float32Type, Float64Type,
};
use arrow::error::ArrowError;
use datafusion_common::{Result, ScalarValue, exec_err, internal_err};
use datafusion_expr::{
    ColumnarValue, ReturnFieldArgs, ScalarFunctionArgs, ScalarUDFImpl, Signature,
    TypeSignature, Volatility,
};

/// Spark-compatible `round` expression
/// <https://spark.apache.org/docs/latest/api/sql/index.html#round>
///
/// Rounds the value to `d` decimal places using HALF_UP rounding mode.
/// When `d` is negative, rounds integer types with overflow checking in ANSI mode.
#[derive(Debug, PartialEq, Eq, Hash)]
pub struct SparkRound {
    signature: Signature,
}

impl Default for SparkRound {
    fn default() -> Self {
        Self::new()
    }
}

impl SparkRound {
    pub fn new() -> Self {
        Self {
            signature: Signature::one_of(
                vec![TypeSignature::Numeric(1), TypeSignature::Any(2)],
                Volatility::Immutable,
            ),
        }
    }
}

impl ScalarUDFImpl for SparkRound {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "round"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        // Default return type when decimal places is unknown (assumed 0)
        Ok(arg_types[0].clone())
    }

    fn return_field_from_args(&self, args: ReturnFieldArgs) -> Result<FieldRef> {
        let input_type = args.arg_fields[0].data_type();
        let point = if args.scalar_arguments.len() >= 2 {
            match args.scalar_arguments[1] {
                Some(ScalarValue::Int64(Some(p))) => *p,
                Some(ScalarValue::Int32(Some(p))) => *p as i64,
                Some(ScalarValue::Int16(Some(p))) => *p as i64,
                Some(ScalarValue::Int8(Some(p))) => *p as i64,
                _ => 0,
            }
        } else {
            0
        };

        let return_type = compute_round_return_type(input_type, point);
        Ok(Arc::new(Field::new(self.name(), return_type, true)))
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let point = extract_point(&args.args)?;
        let return_type = args.return_type().clone();
        let enable_ansi_mode = args.config_options.execution.enable_ansi_mode;
        spark_round(&args.args[0], point, &return_type, enable_ansi_mode)
    }
}

/// Computes the return type for round based on input type and decimal places.
///
/// Follows Spark's `RoundBase.dataType` logic:
/// - Float/Integer types: return same type
/// - Decimal128(p, s) with d < 0: Decimal128(p - s + |d| + 1, 0)
/// - Decimal128(p, s) with d >= 0: Decimal128(p - s + min(s, d) + 1, min(s, d))
fn compute_round_return_type(input_type: &DataType, point: i64) -> DataType {
    match input_type {
        DataType::Decimal128(p, s) => {
            let p = *p as i64;
            let s = *s as i64;
            if point < 0 {
                let abs_point = point.unsigned_abs() as i64;
                let new_p = (p - s + abs_point + 1).clamp(1, 38) as u8;
                DataType::Decimal128(new_p, 0)
            } else {
                let new_s = s.min(point);
                let new_p = (p - s + new_s + 1).clamp(1, 38) as u8;
                DataType::Decimal128(new_p, new_s as i8)
            }
        }
        other => other.clone(),
    }
}

/// Extracts the decimal places parameter from function arguments.
fn extract_point(args: &[ColumnarValue]) -> Result<i64> {
    if args.len() < 2 {
        return Ok(0);
    }
    match &args[1] {
        ColumnarValue::Scalar(ScalarValue::Int64(Some(p))) => Ok(*p),
        ColumnarValue::Scalar(ScalarValue::Int32(Some(p))) => Ok(*p as i64),
        ColumnarValue::Scalar(ScalarValue::Int16(Some(p))) => Ok(*p as i64),
        ColumnarValue::Scalar(ScalarValue::Int8(Some(p))) => Ok(*p as i64),
        other => internal_err!(
            "round requires a constant integer for decimal places, got: {:?}",
            other
        ),
    }
}

/// Rounds an integer value to the nearest multiple of `div` using HALF_UP rounding.
///
/// When `enable_ansi_mode` is true, returns an error on arithmetic overflow.
/// When false, uses wrapping arithmetic.
macro_rules! integer_round {
    ($x:expr, $div:expr, $half:expr, $enable_ansi_mode:expr) => {{
        let rem = $x % $div;
        if rem <= -$half {
            if $enable_ansi_mode {
                ($x - rem).sub_checked($div).map_err(|_| {
                    ArrowError::ComputeError(format!(
                        "[ARITHMETIC_OVERFLOW] integer overflow in round"
                    ))
                })
            } else {
                Ok(($x - rem).sub_wrapping($div))
            }
        } else if rem >= $half {
            if $enable_ansi_mode {
                ($x - rem).add_checked($div).map_err(|_| {
                    ArrowError::ComputeError(format!(
                        "[ARITHMETIC_OVERFLOW] integer overflow in round"
                    ))
                })
            } else {
                Ok(($x - rem).add_wrapping($div))
            }
        } else if $enable_ansi_mode {
            $x.sub_checked(rem).map_err(|_| {
                ArrowError::ComputeError(format!(
                    "[ARITHMETIC_OVERFLOW] integer overflow in round"
                ))
            })
        } else {
            Ok($x.sub_wrapping(rem))
        }
    }};
}

macro_rules! round_integer_array {
    ($array:expr, $point:expr, $array_type:ty, $native_type:ty, $enable_ansi_mode:expr) => {{
        let array = $array.as_any().downcast_ref::<$array_type>().unwrap();
        let ten: $native_type = 10;
        let result: $array_type = if let Some(div) = ten.checked_pow((-$point) as u32) {
            let half = div / 2;
            try_unary(array, |x| integer_round!(x, div, half, $enable_ansi_mode))?
        } else {
            try_unary(array, |_| Ok(0 as $native_type))?
        };
        Ok(ColumnarValue::Array(Arc::new(result)))
    }};
}

macro_rules! round_integer_scalar {
    ($scalar_opt:expr, $point:expr, $scalar_variant:ident, $native_type:ty, $enable_ansi_mode:expr) => {{
        let ten: $native_type = 10;
        if let Some(div) = ten.checked_pow((-$point) as u32) {
            let half = div / 2;
            let result = $scalar_opt
                .map(|x| integer_round!(x, div, half, $enable_ansi_mode))
                .transpose()
                .map_err(|e| {
                    datafusion_common::DataFusionError::ArrowError(Box::new(e), None)
                })?;
            Ok(ColumnarValue::Scalar(ScalarValue::$scalar_variant(result)))
        } else {
            Ok(ColumnarValue::Scalar(ScalarValue::$scalar_variant(Some(0))))
        }
    }};
}

/// Rounds a float value to the specified number of decimal places.
#[inline]
fn round_float(value: f64, point: i64) -> f64 {
    if value.is_nan() || value.is_infinite() {
        return value;
    }
    // Clamp to avoid overflow in powi (f64 can represent up to ~10^308)
    let point = point.clamp(-308, 308) as i32;
    if point >= 0 {
        let factor = 10f64.powi(point);
        (value * factor).round() / factor
    } else {
        let factor = 10f64.powi(-point);
        (value / factor).round() * factor
    }
}

/// Rounds a Decimal128 value to the specified number of decimal places.
///
/// Uses Spark's BigDecimal-style rounding:
/// 1. Add half of the divisor (adjusted for sign)
/// 2. Truncate by division
/// 3. Adjust precision by multiplication (for negative point)
#[inline]
fn round_decimal(x: i128, scale: i64, point: i64) -> i128 {
    if point < 0 {
        if let Some(div) = 10_i128.checked_pow(((-point) as u32) + (scale as u32)) {
            let half = div / 2;
            let mul = 10_i128.pow((-point) as u32);
            (x + x.signum() * half) / div * mul
        } else {
            0
        }
    } else {
        let diff = (scale as u32).saturating_sub(point.min(scale) as u32);
        let div = 10_i128.pow(diff);
        let half = div / 2;
        (x + x.signum() * half) / div
    }
}

fn spark_round(
    value: &ColumnarValue,
    point: i64,
    return_type: &DataType,
    enable_ansi_mode: bool,
) -> Result<ColumnarValue> {
    match value {
        ColumnarValue::Array(array) => match array.data_type() {
            DataType::Float32 => {
                let result = array
                    .as_primitive::<Float32Type>()
                    .unary::<_, Float32Type>(|x| round_float(x as f64, point) as f32);
                Ok(ColumnarValue::Array(Arc::new(result)))
            }
            DataType::Float64 => {
                let result = array
                    .as_primitive::<Float64Type>()
                    .unary::<_, Float64Type>(|x| round_float(x, point));
                Ok(ColumnarValue::Array(Arc::new(result)))
            }
            DataType::Int64 if point < 0 => {
                round_integer_array!(array, point, Int64Array, i64, enable_ansi_mode)
            }
            DataType::Int32 if point < 0 => {
                round_integer_array!(array, point, Int32Array, i32, enable_ansi_mode)
            }
            DataType::Int16 if point < 0 => {
                round_integer_array!(array, point, Int16Array, i16, enable_ansi_mode)
            }
            DataType::Int8 if point < 0 => {
                round_integer_array!(array, point, Int8Array, i8, enable_ansi_mode)
            }
            dt if dt.is_integer() => {
                // Rounding to >= 0 decimal places on integers is a no-op
                Ok(ColumnarValue::Array(Arc::clone(array)))
            }
            DataType::Decimal128(_, s) if *s >= 0 => {
                let scale = *s as i64;
                let result: Decimal128Array = array
                    .as_primitive::<Decimal128Type>()
                    .unary(|x| round_decimal(x, scale, point));
                let result = result.with_data_type(return_type.clone());
                Ok(ColumnarValue::Array(Arc::new(result)))
            }
            dt => exec_err!("Unsupported data type for round: {dt}"),
        },
        ColumnarValue::Scalar(sv) => match sv {
            ScalarValue::Float32(v) => Ok(ColumnarValue::Scalar(ScalarValue::Float32(
                v.map(|x| round_float(x as f64, point) as f32),
            ))),
            ScalarValue::Float64(v) => Ok(ColumnarValue::Scalar(ScalarValue::Float64(
                v.map(|x| round_float(x, point)),
            ))),
            ScalarValue::Int64(v) if point < 0 => {
                round_integer_scalar!(v, point, Int64, i64, enable_ansi_mode)
            }
            ScalarValue::Int32(v) if point < 0 => {
                round_integer_scalar!(v, point, Int32, i32, enable_ansi_mode)
            }
            ScalarValue::Int16(v) if point < 0 => {
                round_integer_scalar!(v, point, Int16, i16, enable_ansi_mode)
            }
            ScalarValue::Int8(v) if point < 0 => {
                round_integer_scalar!(v, point, Int8, i8, enable_ansi_mode)
            }
            sv if sv.data_type().is_integer() => {
                // Rounding to >= 0 decimal places on integers is a no-op
                Ok(ColumnarValue::Scalar(sv.clone()))
            }
            ScalarValue::Decimal128(v, _, s) if *s >= 0 => {
                let scale = *s as i64;
                let result = v.map(|x| round_decimal(x, scale, point));
                let DataType::Decimal128(p, s) = return_type else {
                    return internal_err!("Expected Decimal128 return type for round");
                };
                Ok(ColumnarValue::Scalar(ScalarValue::Decimal128(
                    result, *p, *s,
                )))
            }
            dt => exec_err!("Unsupported data type for round: {dt}"),
        },
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Float32Array, Float64Array, Int64Array};

    #[test]
    fn test_round_float64() {
        let input = Float64Array::from(vec![Some(2.5), Some(3.5), Some(-2.5), None]);
        let result = spark_round(
            &ColumnarValue::Array(Arc::new(input)),
            0,
            &DataType::Float64,
            false,
        )
        .unwrap();
        let result = match result {
            ColumnarValue::Array(arr) => arr,
            _ => panic!("Expected array"),
        };
        let result = result.as_primitive::<Float64Type>();
        assert_eq!(
            result,
            &Float64Array::from(vec![Some(3.0), Some(4.0), Some(-3.0), None])
        );
    }

    #[test]
    fn test_round_float64_with_decimal_places() {
        let input = Float64Array::from(vec![Some(1.2345), Some(-1.2345)]);
        let result = spark_round(
            &ColumnarValue::Array(Arc::new(input)),
            2,
            &DataType::Float64,
            false,
        )
        .unwrap();
        let result = match result {
            ColumnarValue::Array(arr) => arr,
            _ => panic!("Expected array"),
        };
        let result = result.as_primitive::<Float64Type>();
        assert_eq!(result, &Float64Array::from(vec![Some(1.23), Some(-1.23)]));
    }

    #[test]
    fn test_round_float32_with_decimal_places() {
        let input = Float32Array::from(vec![
            Some(125.2345f32),
            Some(15.3455f32),
            Some(0.1234f32),
            Some(0.125f32),
            Some(0.785f32),
            Some(123.123f32),
        ]);
        let result = spark_round(
            &ColumnarValue::Array(Arc::new(input)),
            2,
            &DataType::Float32,
            false,
        )
        .unwrap();
        let result = match result {
            ColumnarValue::Array(arr) => arr,
            _ => panic!("Expected array"),
        };
        let result = result.as_primitive::<Float32Type>();
        assert_eq!(
            result,
            &Float32Array::from(vec![
                Some(125.23f32),
                Some(15.35f32),
                Some(0.12f32),
                Some(0.13f32),
                Some(0.79f32),
                Some(123.12f32),
            ])
        );
    }

    #[test]
    fn test_round_int64_negative_point() {
        let input = Int64Array::from(vec![Some(1234), Some(-1234), Some(1250), None]);
        let result = spark_round(
            &ColumnarValue::Array(Arc::new(input)),
            -2,
            &DataType::Int64,
            false,
        )
        .unwrap();
        let result = match result {
            ColumnarValue::Array(arr) => arr,
            _ => panic!("Expected array"),
        };
        let result = result.as_primitive::<arrow::datatypes::Int64Type>();
        assert_eq!(
            result,
            &Int64Array::from(vec![Some(1200), Some(-1200), Some(1300), None])
        );
    }

    #[test]
    fn test_round_int64_noop() {
        // Rounding with non-negative decimal places is a no-op for integers
        let input = Int64Array::from(vec![Some(42), Some(-42), None]);
        let result = spark_round(
            &ColumnarValue::Array(Arc::new(input.clone())),
            0,
            &DataType::Int64,
            false,
        )
        .unwrap();
        let result = match result {
            ColumnarValue::Array(arr) => arr,
            _ => panic!("Expected array"),
        };
        let result = result.as_primitive::<arrow::datatypes::Int64Type>();
        assert_eq!(result, &input);
    }

    #[test]
    fn test_round_decimal128() {
        // Decimal128(10, 3): values 1235 = 1.235, -1235 = -1.235
        let return_type = DataType::Decimal128(9, 2);
        let input = Decimal128Array::from(vec![Some(1235), Some(-1235), None])
            .with_data_type(DataType::Decimal128(10, 3));
        let result = spark_round(
            &ColumnarValue::Array(Arc::new(input)),
            2,
            &return_type,
            false,
        )
        .unwrap();
        let result = match result {
            ColumnarValue::Array(arr) => arr,
            _ => panic!("Expected array"),
        };
        let result = result.as_primitive::<Decimal128Type>();
        // 1.235 rounded to 2 decimal places = 1.24, -1.235 rounded = -1.24
        let expected = Decimal128Array::from(vec![Some(124), Some(-124), None])
            .with_data_type(return_type);
        assert_eq!(result, &expected);
    }

    #[test]
    fn test_round_scalar_float64() {
        let result = spark_round(
            &ColumnarValue::Scalar(ScalarValue::Float64(Some(2.5))),
            0,
            &DataType::Float64,
            false,
        )
        .unwrap();
        match result {
            ColumnarValue::Scalar(ScalarValue::Float64(Some(v))) => {
                assert_eq!(v, 3.0);
            }
            _ => panic!("Expected scalar float64"),
        }
    }

    #[test]
    fn test_round_scalar_float32() {
        let result = spark_round(
            &ColumnarValue::Scalar(ScalarValue::Float32(Some(125.2345f32))),
            2,
            &DataType::Float32,
            false,
        )
        .unwrap();
        match result {
            ColumnarValue::Scalar(ScalarValue::Float32(Some(v))) => {
                assert_eq!(v, 125.23f32);
            }
            _ => panic!("Expected scalar float32"),
        }
    }

    #[test]
    fn test_round_scalar_int64_negative_point() {
        let result = spark_round(
            &ColumnarValue::Scalar(ScalarValue::Int64(Some(1234))),
            -2,
            &DataType::Int64,
            false,
        )
        .unwrap();
        match result {
            ColumnarValue::Scalar(ScalarValue::Int64(Some(v))) => {
                assert_eq!(v, 1200);
            }
            _ => panic!("Expected scalar int64"),
        }
    }

    #[test]
    fn test_round_return_type() {
        // Float/integer types return same type
        assert_eq!(
            compute_round_return_type(&DataType::Float64, 2),
            DataType::Float64
        );
        assert_eq!(
            compute_round_return_type(&DataType::Int64, -2),
            DataType::Int64
        );

        // Decimal128(10, 3) rounded to 2 places: Decimal128(10-3+2+1=10, 2) -> Decimal128(10, 2)
        assert_eq!(
            compute_round_return_type(&DataType::Decimal128(10, 3), 2),
            DataType::Decimal128(10, 2)
        );

        // Decimal128(10, 3) rounded to -1 places: Decimal128(10-3+1+1=9, 0) -> Decimal128(9, 0)
        assert_eq!(
            compute_round_return_type(&DataType::Decimal128(10, 3), -1),
            DataType::Decimal128(9, 0)
        );

        // Decimal128(10, 3) rounded to 5 places (more than scale): min(3,5)=3
        // Decimal128(10-3+3+1=11, 3) -> Decimal128(11, 3)
        assert_eq!(
            compute_round_return_type(&DataType::Decimal128(10, 3), 5),
            DataType::Decimal128(11, 3)
        );
    }

    #[test]
    fn test_round_integer_overflow_ansi_mode() {
        // In ANSI mode, rounding i64::MAX with negative point should overflow
        let result = spark_round(
            &ColumnarValue::Scalar(ScalarValue::Int64(Some(i64::MAX))),
            -1,
            &DataType::Int64,
            true,
        );
        assert!(result.is_err());
        assert!(
            result
                .unwrap_err()
                .to_string()
                .contains("ARITHMETIC_OVERFLOW")
        );
    }

    #[test]
    fn test_round_integer_overflow_legacy_mode() {
        // In legacy mode, wrapping arithmetic is used (no error)
        let result = spark_round(
            &ColumnarValue::Scalar(ScalarValue::Int64(Some(i64::MAX))),
            -1,
            &DataType::Int64,
            false,
        );
        assert!(result.is_ok());
    }
}
