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

use crate::function::error_utils::{
    invalid_arg_count_exec_err, unsupported_data_type_exec_err,
};
use arrow::array::*;
use arrow::datatypes::DataType;
use arrow::datatypes::*;
use datafusion_common::{internal_err, DataFusionError, Result, ScalarValue};
use datafusion_expr::{
    ColumnarValue, ScalarFunctionArgs, ScalarUDFImpl, Signature, Volatility,
};
use std::any::Any;
use std::sync::Arc;

/// Spark-compatible `abs` expression
/// <https://spark.apache.org/docs/latest/api/sql/index.html#abs>
///
/// Returns the absolute value of input
/// Returns NULL if input is NULL, returns NaN if input is NaN.
///
/// Differences with DataFusion abs:
///  - Spark's ANSI-compliant dialect, when off (i.e. `spark.sql.ansi.enabled=false`), taking absolute values on minimal values of signed integers returns the value as is. DataFusion's abs throws "DataFusion error: Arrow error: Compute error" on arithmetic overflow
///  - Spark's abs also supports ANSI interval types: YearMonthIntervalType and DayTimeIntervalType. DataFusion's abs doesn't.
///
#[derive(Debug, PartialEq, Eq, Hash)]
pub struct SparkAbs {
    signature: Signature,
}

impl Default for SparkAbs {
    fn default() -> Self {
        Self::new()
    }
}

impl SparkAbs {
    pub fn new() -> Self {
        Self {
            signature: Signature::numeric(1, Volatility::Immutable),
        }
    }
}

impl ScalarUDFImpl for SparkAbs {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "abs"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        Ok(arg_types[0].clone())
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        spark_abs(&args.args)
    }

    fn coerce_types(&self, arg_types: &[DataType]) -> Result<Vec<DataType>> {
        if arg_types.len() > 2 {
            return Err(invalid_arg_count_exec_err("abs", (1, 2), arg_types.len()));
        }
        match &arg_types[0] {
            DataType::Null
            | DataType::UInt8
            | DataType::UInt16
            | DataType::UInt32
            | DataType::UInt64
            | DataType::Int8
            | DataType::Int16
            | DataType::Int32
            | DataType::Int64
            | DataType::Float32
            | DataType::Float64
            | DataType::Decimal128(_, _)
            | DataType::Decimal256(_, _)
            | DataType::Interval(IntervalUnit::YearMonth)
            | DataType::Interval(IntervalUnit::DayTime) => Ok(vec![arg_types[0].clone()]),
            other => {
                if other.is_numeric() {
                    Ok(vec![DataType::Float64])
                } else {
                    Err(unsupported_data_type_exec_err(
                        "abs",
                        "Numeric Type or Interval Type",
                        &arg_types[0],
                    ))
                }
            }
        }
    }
}

macro_rules! legacy_compute_op {
    ($ARRAY:expr, $FUNC:ident, $TYPE:ident, $RESULT:ident) => {{
        let array = $ARRAY.as_any().downcast_ref::<$TYPE>().unwrap();
        let res: $RESULT = arrow::compute::kernels::arity::unary(array, |x| x.$FUNC());
        res
    }};
}

macro_rules! ansi_compute_op {
    ($ARRAY:expr, $FUNC:ident, $TYPE:ident, $RESULT:ident, $MIN:expr, $FROM_TYPE:expr) => {{
        let array = $ARRAY.as_any().downcast_ref::<$TYPE>().unwrap();
        match arrow::compute::kernels::arity::try_unary(array, |x| {
            if x == $MIN {
                Err(arrow::error::ArrowError::ArithmeticOverflow(
                    $FROM_TYPE.to_string(),
                ))
            } else {
                Ok(x.$FUNC())
            }
        }) {
            Ok(res) => Ok(ColumnarValue::Array(Arc::<PrimitiveArray<$RESULT>>::new(
                res,
            ))),
            Err(_) => Err(arithmetic_overflow_error($FROM_TYPE)),
        }
    }};
}

fn arithmetic_overflow_error(from_type: &str) -> DataFusionError {
    DataFusionError::Execution(format!("arithmetic overflow from {from_type}"))
}

pub fn spark_abs(args: &[ColumnarValue]) -> Result<ColumnarValue, DataFusionError> {
    if args.len() > 2 {
        return internal_err!("abs takes at most 2 arguments, but got: {}", args.len());
    }

    let fail_on_error = if args.len() == 2 {
        match &args[1] {
            ColumnarValue::Scalar(ScalarValue::Boolean(Some(fail_on_error))) => {
                *fail_on_error
            }
            _ => {
                return internal_err!(
                    "The second argument must be boolean scalar, but got: {:?}",
                    args[1]
                );
            }
        }
    } else {
        false
    };

    match &args[0] {
        ColumnarValue::Array(array) => match array.data_type() {
            DataType::Null
            | DataType::UInt8
            | DataType::UInt16
            | DataType::UInt32
            | DataType::UInt64 => Ok(args[0].clone()),
            DataType::Int8 => {
                if !fail_on_error {
                    let result =
                        legacy_compute_op!(array, wrapping_abs, Int8Array, Int8Array);
                    Ok(ColumnarValue::Array(Arc::new(result)))
                } else {
                    ansi_compute_op!(array, abs, Int8Array, Int8Type, i8::MIN, "Int8")
                }
            }
            DataType::Int16 => {
                if !fail_on_error {
                    let result =
                        legacy_compute_op!(array, wrapping_abs, Int16Array, Int16Array);
                    Ok(ColumnarValue::Array(Arc::new(result)))
                } else {
                    ansi_compute_op!(array, abs, Int16Array, Int16Type, i16::MIN, "Int16")
                }
            }
            DataType::Int32 => {
                if !fail_on_error {
                    let result =
                        legacy_compute_op!(array, wrapping_abs, Int32Array, Int32Array);
                    Ok(ColumnarValue::Array(Arc::new(result)))
                } else {
                    ansi_compute_op!(array, abs, Int32Array, Int32Type, i32::MIN, "Int32")
                }
            }
            DataType::Int64 => {
                if !fail_on_error {
                    let result =
                        legacy_compute_op!(array, wrapping_abs, Int64Array, Int64Array);
                    Ok(ColumnarValue::Array(Arc::new(result)))
                } else {
                    ansi_compute_op!(array, abs, Int64Array, Int64Type, i64::MIN, "Int64")
                }
            }
            DataType::Float32 => {
                let result = legacy_compute_op!(array, abs, Float32Array, Float32Array);
                Ok(ColumnarValue::Array(Arc::new(result)))
            }
            DataType::Float64 => {
                let result = legacy_compute_op!(array, abs, Float64Array, Float64Array);
                Ok(ColumnarValue::Array(Arc::new(result)))
            }
            DataType::Decimal128(precision, scale) => {
                if !fail_on_error {
                    let result = legacy_compute_op!(
                        array,
                        wrapping_abs,
                        Decimal128Array,
                        Decimal128Array
                    );
                    let result =
                        result.with_data_type(DataType::Decimal128(*precision, *scale));
                    Ok(ColumnarValue::Array(Arc::new(result)))
                } else {
                    // Need to pass precision and scale from input, so not using ansi_compute_op
                    let input = array.as_any().downcast_ref::<Decimal128Array>();
                    match input {
                        Some(i) => {
                            match arrow::compute::kernels::arity::try_unary(i, |x| {
                                if x == i128::MIN {
                                    Err(arrow::error::ArrowError::ArithmeticOverflow(
                                        "Decimal128".to_string(),
                                    ))
                                } else {
                                    Ok(x.abs())
                                }
                            }) {
                                Ok(res) => Ok(ColumnarValue::Array(Arc::<
                                    PrimitiveArray<Decimal128Type>,
                                >::new(
                                    res.with_data_type(DataType::Decimal128(
                                        *precision, *scale,
                                    )),
                                ))),
                                Err(_) => Err(arithmetic_overflow_error("Decimal128")),
                            }
                        }
                        _ => Err(DataFusionError::Internal(
                            "Invalid data type".to_string(),
                        )),
                    }
                }
            }
            DataType::Decimal256(precision, scale) => {
                if !fail_on_error {
                    let result = legacy_compute_op!(
                        array,
                        wrapping_abs,
                        Decimal256Array,
                        Decimal256Array
                    );
                    let result =
                        result.with_data_type(DataType::Decimal256(*precision, *scale));
                    Ok(ColumnarValue::Array(Arc::new(result)))
                } else {
                    // Need to pass precision and scale from input, so not using ansi_compute_op
                    let input = array.as_any().downcast_ref::<Decimal256Array>();
                    match input {
                        Some(i) => {
                            match arrow::compute::kernels::arity::try_unary(i, |x| {
                                if x == i256::MIN {
                                    Err(arrow::error::ArrowError::ArithmeticOverflow(
                                        "Decimal256".to_string(),
                                    ))
                                } else {
                                    Ok(x.wrapping_abs()) // i256 doesn't define abs() method
                                }
                            }) {
                                Ok(res) => Ok(ColumnarValue::Array(Arc::<
                                    PrimitiveArray<Decimal256Type>,
                                >::new(
                                    res.with_data_type(DataType::Decimal256(
                                        *precision, *scale,
                                    )),
                                ))),
                                Err(_) => Err(arithmetic_overflow_error("Decimal256")),
                            }
                        }
                        _ => Err(DataFusionError::Internal(
                            "Invalid data type".to_string(),
                        )),
                    }
                }
            }
            DataType::Interval(unit) => match unit {
                IntervalUnit::YearMonth => {
                    if !fail_on_error {
                        let result = legacy_compute_op!(
                            array,
                            wrapping_abs,
                            IntervalYearMonthArray,
                            IntervalYearMonthArray
                        );
                        let result = result.with_data_type(DataType::Interval(*unit));
                        Ok(ColumnarValue::Array(Arc::new(result)))
                    } else {
                        ansi_compute_op!(
                            array,
                            abs,
                            IntervalYearMonthArray,
                            IntervalYearMonthType,
                            i32::MIN,
                            "IntervalYearMonth"
                        )
                    }
                }
                IntervalUnit::DayTime => {
                    if !fail_on_error {
                        let result = legacy_compute_op!(
                            array,
                            wrapping_abs,
                            IntervalDayTimeArray,
                            IntervalDayTimeArray
                        );
                        let result = result.with_data_type(DataType::Interval(*unit));
                        Ok(ColumnarValue::Array(Arc::new(result)))
                    } else {
                        ansi_compute_op!(
                            array,
                            wrapping_abs,
                            IntervalDayTimeArray,
                            IntervalDayTimeType,
                            IntervalDayTime::MIN,
                            "IntervalDayTime"
                        )
                    }
                }
                IntervalUnit::MonthDayNano => internal_err!(
                    "MonthDayNano is not a supported Interval unit for Spark ABS"
                ),
            },
            dt => internal_err!("Not supported datatype for Spark ABS: {dt}"),
        },
        ColumnarValue::Scalar(sv) => match sv {
            ScalarValue::Null
            | ScalarValue::UInt8(_)
            | ScalarValue::UInt16(_)
            | ScalarValue::UInt32(_)
            | ScalarValue::UInt64(_) => Ok(args[0].clone()),
            ScalarValue::Int8(a) => match a {
                None => Ok(args[0].clone()),
                Some(v) => match v.checked_abs() {
                    Some(abs_val) => {
                        Ok(ColumnarValue::Scalar(ScalarValue::Int8(Some(abs_val))))
                    }
                    None => {
                        if !fail_on_error {
                            // return the original value
                            Ok(ColumnarValue::Scalar(ScalarValue::Int8(Some(*v))))
                        } else {
                            Err(arithmetic_overflow_error("Int8"))
                        }
                    }
                },
            },
            ScalarValue::Int16(a) => match a {
                None => Ok(args[0].clone()),
                Some(v) => match v.checked_abs() {
                    Some(abs_val) => {
                        Ok(ColumnarValue::Scalar(ScalarValue::Int16(Some(abs_val))))
                    }
                    None => {
                        if !fail_on_error {
                            // return the original value
                            Ok(ColumnarValue::Scalar(ScalarValue::Int16(Some(*v))))
                        } else {
                            Err(arithmetic_overflow_error("Int16"))
                        }
                    }
                },
            },
            ScalarValue::Int32(a) => match a {
                None => Ok(args[0].clone()),
                Some(v) => match v.checked_abs() {
                    Some(abs_val) => {
                        Ok(ColumnarValue::Scalar(ScalarValue::Int32(Some(abs_val))))
                    }
                    None => {
                        if !fail_on_error {
                            // return the original value
                            Ok(ColumnarValue::Scalar(ScalarValue::Int32(Some(*v))))
                        } else {
                            Err(arithmetic_overflow_error("Int32"))
                        }
                    }
                },
            },
            ScalarValue::Int64(a) => match a {
                None => Ok(args[0].clone()),
                Some(v) => match v.checked_abs() {
                    Some(abs_val) => {
                        Ok(ColumnarValue::Scalar(ScalarValue::Int64(Some(abs_val))))
                    }
                    None => {
                        if !fail_on_error {
                            // return the original value
                            Ok(ColumnarValue::Scalar(ScalarValue::Int64(Some(*v))))
                        } else {
                            Err(arithmetic_overflow_error("Int64"))
                        }
                    }
                },
            },
            ScalarValue::Float32(a) => match a {
                None => Ok(args[0].clone()),
                Some(v) => Ok(ColumnarValue::Scalar(ScalarValue::Float32(Some(v.abs())))),
            },
            ScalarValue::Float64(a) => match a {
                None => Ok(args[0].clone()),
                Some(v) => Ok(ColumnarValue::Scalar(ScalarValue::Float64(Some(v.abs())))),
            },
            ScalarValue::Decimal128(a, precision, scale) => {
                match a {
                    None => Ok(args[0].clone()),
                    Some(v) => match v.checked_abs() {
                        Some(abs_val) => Ok(ColumnarValue::Scalar(
                            ScalarValue::Decimal128(Some(abs_val), *precision, *scale),
                        )),
                        None => {
                            if !fail_on_error {
                                // return the original value
                                Ok(ColumnarValue::Scalar(ScalarValue::Decimal128(
                                    Some(*v),
                                    *precision,
                                    *scale,
                                )))
                            } else {
                                Err(arithmetic_overflow_error("Decimal128"))
                            }
                        }
                    },
                }
            }
            ScalarValue::Decimal256(a, precision, scale) => {
                match a {
                    None => Ok(args[0].clone()),
                    Some(v) => match v.checked_abs() {
                        Some(abs_val) => Ok(ColumnarValue::Scalar(
                            ScalarValue::Decimal256(Some(abs_val), *precision, *scale),
                        )),
                        None => {
                            if !fail_on_error {
                                // return the original value
                                Ok(ColumnarValue::Scalar(ScalarValue::Decimal256(
                                    Some(*v),
                                    *precision,
                                    *scale,
                                )))
                            } else {
                                Err(arithmetic_overflow_error("Decimal256"))
                            }
                        }
                    },
                }
            }
            ScalarValue::IntervalYearMonth(a) => match a {
                None => Ok(args[0].clone()),
                Some(v) => match v.checked_abs() {
                    Some(abs_val) => Ok(ColumnarValue::Scalar(
                        ScalarValue::IntervalYearMonth(Some(abs_val)),
                    )),
                    None => {
                        if !fail_on_error {
                            // return the original value
                            Ok(ColumnarValue::Scalar(ScalarValue::IntervalYearMonth(
                                Some(*v),
                            )))
                        } else {
                            Err(arithmetic_overflow_error("IntervalYearMonth"))
                        }
                    }
                },
            },
            ScalarValue::IntervalDayTime(a) => match a {
                None => Ok(args[0].clone()),
                Some(v) => match v.checked_abs() {
                    Some(abs_val) => Ok(ColumnarValue::Scalar(
                        ScalarValue::IntervalDayTime(Some(abs_val)),
                    )),
                    None => {
                        if !fail_on_error {
                            // return the original value
                            Ok(ColumnarValue::Scalar(ScalarValue::IntervalDayTime(Some(
                                *v,
                            ))))
                        } else {
                            Err(arithmetic_overflow_error("IntervalYearMonth"))
                        }
                    }
                },
            },

            dt => internal_err!("Not supported datatype for Spark ABS: {dt}"),
        },
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion_common::cast::{
        as_decimal128_array, as_decimal256_array, as_float32_array, as_float64_array,
        as_int16_array, as_int32_array, as_int64_array, as_int8_array,
        as_interval_dt_array, as_interval_ym_array, as_uint64_array,
    };

    fn with_fail_on_error<F: Fn(bool) -> Result<()>>(test_fn: F) {
        for fail_on_error in [true, false] {
            let _ = test_fn(fail_on_error);
        }
    }

    #[test]
    fn test_abs_zero_arg() {
        assert!(spark_abs(&[]).is_err());
    }

    macro_rules! eval_legacy_mode {
        ($TYPE:ident, $VAL:expr) => {{
            let args = ColumnarValue::Scalar(ScalarValue::$TYPE(Some($VAL)));
            match spark_abs(&[args]) {
                Ok(ColumnarValue::Scalar(ScalarValue::$TYPE(Some(result)))) => {
                    assert_eq!(result, $VAL);
                }
                _ => unreachable!(),
            }
        }};
        ($TYPE:ident, $VAL:expr, $RESULT:expr) => {{
            let args = ColumnarValue::Scalar(ScalarValue::$TYPE(Some($VAL)));
            match spark_abs(&[args]) {
                Ok(ColumnarValue::Scalar(ScalarValue::$TYPE(Some(result)))) => {
                    assert_eq!(result, $RESULT);
                }
                _ => unreachable!(),
            }
        }};
        ($TYPE:ident, $VAL:expr, $PRECISION:expr, $SCALE:expr) => {{
            let args =
                ColumnarValue::Scalar(ScalarValue::$TYPE(Some($VAL), $PRECISION, $SCALE));
            match spark_abs(&[args]) {
                Ok(ColumnarValue::Scalar(ScalarValue::$TYPE(
                    Some(result),
                    precision,
                    scale,
                ))) => {
                    assert_eq!(result, $VAL);
                    assert_eq!(precision, $PRECISION);
                    assert_eq!(scale, $SCALE);
                }
                _ => unreachable!(),
            }
        }};
        ($TYPE:ident, $VAL:expr, $PRECISION:expr, $SCALE:expr, $RESULT:expr) => {{
            let args =
                ColumnarValue::Scalar(ScalarValue::$TYPE(Some($VAL), $PRECISION, $SCALE));
            match spark_abs(&[args]) {
                Ok(ColumnarValue::Scalar(ScalarValue::$TYPE(
                    Some(result),
                    precision,
                    scale,
                ))) => {
                    assert_eq!(result, $RESULT);
                    assert_eq!(precision, $PRECISION);
                    assert_eq!(scale, $SCALE);
                }
                _ => unreachable!(),
            }
        }};
    }

    #[test]
    fn test_abs_scalar_legacy_mode() {
        // NumericType, DayTimeIntervalType, and YearMonthIntervalType MIN
        eval_legacy_mode!(UInt8, u8::MIN);
        eval_legacy_mode!(UInt16, u16::MIN);
        eval_legacy_mode!(UInt32, u32::MIN);
        eval_legacy_mode!(UInt64, u64::MIN);
        eval_legacy_mode!(Int8, i8::MIN);
        eval_legacy_mode!(Int16, i16::MIN);
        eval_legacy_mode!(Int32, i32::MIN);
        eval_legacy_mode!(Int64, i64::MIN);
        eval_legacy_mode!(Float32, f32::MIN, f32::MAX);
        eval_legacy_mode!(Float64, f64::MIN, f64::MAX);
        eval_legacy_mode!(Decimal128, i128::MIN, 18, 10);
        eval_legacy_mode!(Decimal256, i256::MIN, 10, 2);
        eval_legacy_mode!(IntervalYearMonth, i32::MIN);
        eval_legacy_mode!(IntervalDayTime, IntervalDayTime::MIN);

        // NumericType, DayTimeIntervalType, and YearMonthIntervalType not MIN
        eval_legacy_mode!(Int8, -1i8, 1i8);
        eval_legacy_mode!(Int16, -1i16, 1i16);
        eval_legacy_mode!(Int32, -1i32, 1i32);
        eval_legacy_mode!(Int64, -1i64, 1i64);
        eval_legacy_mode!(Decimal128, -1i128, 18, 10, 1i128);
        eval_legacy_mode!(Decimal256, i256::from(-1i8), 10, 2, i256::from(1i8));
        eval_legacy_mode!(IntervalYearMonth, -1i32, 1i32);
        eval_legacy_mode!(
            IntervalDayTime,
            IntervalDayTime::new(-1i32, -1i32),
            IntervalDayTime::new(1i32, 1i32)
        );

        // Float32, Float64
        eval_legacy_mode!(Float32, f32::NEG_INFINITY, f32::INFINITY);
        eval_legacy_mode!(Float32, f32::INFINITY, f32::INFINITY);
        eval_legacy_mode!(Float32, 0.0f32, 0.0f32);
        eval_legacy_mode!(Float32, -0.0f32, 0.0f32);
        eval_legacy_mode!(Float64, f64::NEG_INFINITY, f64::INFINITY);
        eval_legacy_mode!(Float64, f64::INFINITY, f64::INFINITY);
        eval_legacy_mode!(Float64, 0.0f64, 0.0f64);
        eval_legacy_mode!(Float64, -0.0f64, 0.0f64);
    }

    macro_rules! eval_ansi_mode {
        ($TYPE:ident, $VAL:expr) => {{
            let args = ColumnarValue::Scalar(ScalarValue::$TYPE(Some($VAL)));
            let fail_on_error = ColumnarValue::Scalar(ScalarValue::Boolean(Some(true)));
            match spark_abs(&[args, fail_on_error]) {
                Err(e) => {
                    assert!(
                        e.to_string().contains("arithmetic overflow"),
                        "Error message did not match. Actual message: {e}"
                    );
                }
                _ => unreachable!(),
            }
        }};
        ($TYPE:ident, $VAL:expr, $RESULT:expr) => {{
            let args = ColumnarValue::Scalar(ScalarValue::$TYPE(Some($VAL)));
            let fail_on_error = ColumnarValue::Scalar(ScalarValue::Boolean(Some(true)));
            match spark_abs(&[args, fail_on_error]) {
                Ok(ColumnarValue::Scalar(ScalarValue::$TYPE(Some(result)))) => {
                    assert_eq!(result, $RESULT);
                }
                _ => unreachable!(),
            }
        }};
        ($TYPE:ident, $VAL:expr, $PRECISION:expr, $SCALE:expr) => {{
            let args =
                ColumnarValue::Scalar(ScalarValue::$TYPE(Some($VAL), $PRECISION, $SCALE));
            let fail_on_error = ColumnarValue::Scalar(ScalarValue::Boolean(Some(true)));
            match spark_abs(&[args, fail_on_error]) {
                Err(e) => {
                    assert!(
                        e.to_string().contains("arithmetic overflow"),
                        "Error message did not match. Actual message: {e}"
                    );
                }
                _ => unreachable!(),
            }
        }};
        ($TYPE:ident, $VAL:expr, $PRECISION:expr, $SCALE:expr, $RESULT:expr) => {{
            let args =
                ColumnarValue::Scalar(ScalarValue::$TYPE(Some($VAL), $PRECISION, $SCALE));
            let fail_on_error = ColumnarValue::Scalar(ScalarValue::Boolean(Some(true)));
            match spark_abs(&[args, fail_on_error]) {
                Ok(ColumnarValue::Scalar(ScalarValue::$TYPE(
                    Some(result),
                    precision,
                    scale,
                ))) => {
                    assert_eq!(result, $RESULT);
                    assert_eq!(precision, $PRECISION);
                    assert_eq!(scale, $SCALE);
                }
                _ => unreachable!(),
            }
        }};
    }

    #[test]
    fn test_abs_scalar_ansi_mode() {
        eval_ansi_mode!(Int8, i8::MIN);
        eval_ansi_mode!(Int16, i16::MIN);
        eval_ansi_mode!(Int32, i32::MIN);
        eval_ansi_mode!(Int64, i64::MIN);
        eval_ansi_mode!(Decimal128, i128::MIN, 18, 10);
        eval_ansi_mode!(Decimal256, i256::MIN, 10, 2);
        eval_ansi_mode!(IntervalYearMonth, i32::MIN);
        eval_ansi_mode!(IntervalDayTime, IntervalDayTime::MIN);

        eval_ansi_mode!(UInt8, u8::MIN, u8::MIN);
        eval_ansi_mode!(UInt16, u16::MIN, u16::MIN);
        eval_ansi_mode!(UInt32, u32::MIN, u32::MIN);
        eval_ansi_mode!(UInt64, u64::MIN, u64::MIN);
        eval_ansi_mode!(Float32, f32::MIN, f32::MAX);
        eval_ansi_mode!(Float64, f64::MIN, f64::MAX);

        // NumericType, DayTimeIntervalType, and YearMonthIntervalType not MIN
        eval_ansi_mode!(Int8, -1i8, 1i8);
        eval_ansi_mode!(Int16, -1i16, 1i16);
        eval_ansi_mode!(Int32, -1i32, 1i32);
        eval_ansi_mode!(Int64, -1i64, 1i64);
        eval_ansi_mode!(Decimal128, -1i128, 18, 10, 1i128);
        eval_ansi_mode!(Decimal256, i256::from(-1i8), 10, 2, i256::from(1i8));
        eval_ansi_mode!(IntervalYearMonth, -1i32, 1i32);
        eval_ansi_mode!(
            IntervalDayTime,
            IntervalDayTime::new(-1i32, -1i32),
            IntervalDayTime::new(1i32, 1i32)
        );

        // Float32, Float64
        eval_ansi_mode!(Float32, f32::NEG_INFINITY, f32::INFINITY);
        eval_ansi_mode!(Float32, f32::INFINITY, f32::INFINITY);
        eval_ansi_mode!(Float32, 0.0f32, 0.0f32);
        eval_ansi_mode!(Float32, -0.0f32, 0.0f32);
        eval_ansi_mode!(Float64, f64::NEG_INFINITY, f64::INFINITY);
        eval_ansi_mode!(Float64, f64::INFINITY, f64::INFINITY);
        eval_ansi_mode!(Float64, 0.0f64, 0.0f64);
        eval_ansi_mode!(Float64, -0.0f64, 0.0f64);
    }

    macro_rules! eval_array_legacy_mode {
        ($INPUT:expr, $OUTPUT:expr, $FUNC:ident) => {{
            let input = $INPUT;
            let args = ColumnarValue::Array(Arc::new(input));
            let fail_on_error = ColumnarValue::Scalar(ScalarValue::Boolean(Some(false)));
            let expected = $OUTPUT;
            match spark_abs(&[args, fail_on_error]) {
                Ok(ColumnarValue::Array(result)) => {
                    let actual = datafusion_common::cast::$FUNC(&result).unwrap();
                    assert_eq!(actual, &expected);
                }
                _ => unreachable!(),
            }
        }};
    }

    #[test]
    fn test_abs_array_legacy_mode() {
        eval_array_legacy_mode!(
            Int8Array::from(vec![Some(-1), Some(i8::MIN), Some(i8::MAX), None]),
            Int8Array::from(vec![Some(1), Some(i8::MIN), Some(i8::MAX), None]),
            as_int8_array
        );

        eval_array_legacy_mode!(
            Int16Array::from(vec![Some(-1), Some(i16::MIN), Some(i16::MAX), None]),
            Int16Array::from(vec![Some(1), Some(i16::MIN), Some(i16::MAX), None]),
            as_int16_array
        );

        eval_array_legacy_mode!(
            Int32Array::from(vec![Some(-1), Some(i32::MIN), Some(i32::MAX), None]),
            Int32Array::from(vec![Some(1), Some(i32::MIN), Some(i32::MAX), None]),
            as_int32_array
        );

        eval_array_legacy_mode!(
            Int64Array::from(vec![Some(-1), Some(i64::MIN), Some(i64::MAX), None]),
            Int64Array::from(vec![Some(1), Some(i64::MIN), Some(i64::MAX), None]),
            as_int64_array
        );

        eval_array_legacy_mode!(
            Float32Array::from(vec![
                Some(-1f32),
                Some(f32::MIN),
                Some(f32::MAX),
                None,
                Some(f32::NAN),
                Some(f32::INFINITY),
                Some(f32::NEG_INFINITY),
                Some(0.0),
                Some(-0.0),
            ]),
            Float32Array::from(vec![
                Some(1f32),
                Some(f32::MAX),
                Some(f32::MAX),
                None,
                Some(f32::NAN),
                Some(f32::INFINITY),
                Some(f32::INFINITY),
                Some(0.0),
                Some(0.0),
            ]),
            as_float32_array
        );

        eval_array_legacy_mode!(
            Float64Array::from(vec![
                Some(-1f64),
                Some(f64::MIN),
                Some(f64::MAX),
                None,
                Some(f64::NAN),
                Some(f64::INFINITY),
                Some(f64::NEG_INFINITY),
                Some(0.0),
                Some(-0.0),
            ]),
            Float64Array::from(vec![
                Some(1f64),
                Some(f64::MAX),
                Some(f64::MAX),
                None,
                Some(f64::NAN),
                Some(f64::INFINITY),
                Some(f64::INFINITY),
                Some(0.0),
                Some(0.0),
            ]),
            as_float64_array
        );

        eval_array_legacy_mode!(
            Decimal128Array::from(vec![Some(i128::MIN), None])
                .with_precision_and_scale(38, 37)
                .unwrap(),
            Decimal128Array::from(vec![Some(i128::MIN), None])
                .with_precision_and_scale(38, 37)
                .unwrap(),
            as_decimal128_array
        );

        eval_array_legacy_mode!(
            Decimal256Array::from(vec![Some(i256::MIN), None])
                .with_precision_and_scale(5, 2)
                .unwrap(),
            Decimal256Array::from(vec![Some(i256::MIN), None])
                .with_precision_and_scale(5, 2)
                .unwrap(),
            as_decimal256_array
        );

        eval_array_legacy_mode!(
            IntervalYearMonthArray::from(vec![i32::MIN, -1]),
            IntervalYearMonthArray::from(vec![i32::MIN, 1]),
            as_interval_ym_array
        );

        eval_array_legacy_mode!(
            IntervalDayTimeArray::from(vec![IntervalDayTime::new(i32::MIN, i32::MIN,)]),
            IntervalDayTimeArray::from(vec![IntervalDayTime::new(i32::MIN, i32::MIN,)]),
            as_interval_dt_array
        );
    }

    macro_rules! eval_array_ansi_mode {
        ($INPUT:expr) => {{
            let input = $INPUT;
            let args = ColumnarValue::Array(Arc::new(input));
            let fail_on_error = ColumnarValue::Scalar(ScalarValue::Boolean(Some(true)));
            match spark_abs(&[args, fail_on_error]) {
                Err(e) => {
                    assert!(
                        e.to_string().contains("arithmetic overflow"),
                        "Error message did not match. Actual message: {e}"
                    );
                }
                _ => unreachable!(),
            }
        }};
        ($INPUT:expr, $OUTPUT:expr, $FUNC:ident) => {{
            let input = $INPUT;
            let args = ColumnarValue::Array(Arc::new(input));
            let fail_on_error = ColumnarValue::Scalar(ScalarValue::Boolean(Some(true)));
            let expected = $OUTPUT;
            match spark_abs(&[args, fail_on_error]) {
                Ok(ColumnarValue::Array(result)) => {
                    let actual = datafusion_common::cast::$FUNC(&result).unwrap();
                    assert_eq!(actual, &expected);
                }
                _ => unreachable!(),
            }
        }};
    }
    #[test]
    fn test_abs_array_ansi_mode() {
        eval_array_ansi_mode!(
            UInt64Array::from(vec![Some(u64::MIN), Some(u64::MAX), None]),
            UInt64Array::from(vec![Some(u64::MIN), Some(u64::MAX), None]),
            as_uint64_array
        );

        eval_array_ansi_mode!(Int8Array::from(vec![
            Some(-1),
            Some(i8::MIN),
            Some(i8::MAX),
            None
        ]));
        eval_array_ansi_mode!(Int16Array::from(vec![
            Some(-1),
            Some(i16::MIN),
            Some(i16::MAX),
            None
        ]));
        eval_array_ansi_mode!(Int32Array::from(vec![
            Some(-1),
            Some(i32::MIN),
            Some(i32::MAX),
            None
        ]));
        eval_array_ansi_mode!(Int64Array::from(vec![
            Some(-1),
            Some(i64::MIN),
            Some(i64::MAX),
            None
        ]));
        eval_array_ansi_mode!(
            Float32Array::from(vec![
                Some(-1f32),
                Some(f32::MIN),
                Some(f32::MAX),
                None,
                Some(f32::NAN),
                Some(f32::INFINITY),
                Some(f32::NEG_INFINITY),
                Some(0.0),
                Some(-0.0),
            ]),
            Float32Array::from(vec![
                Some(1f32),
                Some(f32::MAX),
                Some(f32::MAX),
                None,
                Some(f32::NAN),
                Some(f32::INFINITY),
                Some(f32::INFINITY),
                Some(0.0),
                Some(0.0),
            ]),
            as_float32_array
        );

        eval_array_ansi_mode!(
            Float64Array::from(vec![
                Some(-1f64),
                Some(f64::MIN),
                Some(f64::MAX),
                None,
                Some(f64::NAN),
                Some(f64::INFINITY),
                Some(f64::NEG_INFINITY),
                Some(0.0),
                Some(-0.0),
            ]),
            Float64Array::from(vec![
                Some(1f64),
                Some(f64::MAX),
                Some(f64::MAX),
                None,
                Some(f64::NAN),
                Some(f64::INFINITY),
                Some(f64::INFINITY),
                Some(0.0),
                Some(0.0),
            ]),
            as_float64_array
        );

        eval_array_ansi_mode!(Decimal128Array::from(vec![Some(i128::MIN), None])
            .with_precision_and_scale(38, 37)
            .unwrap());
        eval_array_ansi_mode!(Decimal256Array::from(vec![Some(i256::MIN), None])
            .with_precision_and_scale(5, 2)
            .unwrap());
        eval_array_ansi_mode!(IntervalYearMonthArray::from(vec![i32::MIN, -1]));
        eval_array_ansi_mode!(IntervalDayTimeArray::from(vec![IntervalDayTime::new(
            i32::MIN,
            i32::MIN,
        )]));
    }
}
