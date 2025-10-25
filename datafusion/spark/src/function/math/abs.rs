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
use datafusion_common::DataFusionError::ArrowError;
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
        let n = $ARRAY.as_any().downcast_ref::<$TYPE>();
        match n {
            Some(array) => {
                let res: $RESULT =
                    arrow::compute::kernels::arity::unary(array, |x| x.$FUNC());
                Ok(res)
            }
            _ => Err(DataFusionError::Internal(format!(
                "Invalid data type for abs"
            ))),
        }
    }};
}

macro_rules! ansi_compute_op {
    ($ARRAY:expr, $FUNC:ident, $TYPE:ident, $RESULT:ident, $NATIVE:ident, $FROM_TYPE:expr) => {{
        let n = $ARRAY.as_any().downcast_ref::<$TYPE>();
        match n {
            Some(array) => {
                match arrow::compute::kernels::arity::try_unary(array, |x| {
                    if x == $NATIVE::MIN {
                        Err(arrow::error::ArrowError::ArithmeticOverflow(
                            $FROM_TYPE.to_string(),
                        ))
                    } else {
                        Ok(x.$FUNC())
                    }
                }) {
                    Ok(res) => Ok(ColumnarValue::Array(
                        Arc::<PrimitiveArray<$RESULT>>::new(res),
                    )),
                    Err(_) => Err(arithmetic_overflow_error($FROM_TYPE)),
                }
            }
            _ => Err(DataFusionError::Internal("Invalid data type".to_string())),
        }
    }};
}

fn arithmetic_overflow_error(from_type: &str) -> DataFusionError {
    ArrowError(
        Box::from(arrow::error::ArrowError::ComputeError(format!(
            "arithmetic overflow from {from_type}",
        ))),
        None,
    )
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
                    Ok(ColumnarValue::Array(Arc::new(result?)))
                } else {
                    ansi_compute_op!(array, abs, Int8Array, Int8Type, i8, "Int8")
                }
            }
            DataType::Int16 => {
                if !fail_on_error {
                    let result =
                        legacy_compute_op!(array, wrapping_abs, Int16Array, Int16Array);
                    Ok(ColumnarValue::Array(Arc::new(result?)))
                } else {
                    ansi_compute_op!(array, abs, Int16Array, Int16Type, i16, "Int16")
                }
            }
            DataType::Int32 => {
                if !fail_on_error {
                    let result =
                        legacy_compute_op!(array, wrapping_abs, Int32Array, Int32Array);
                    Ok(ColumnarValue::Array(Arc::new(result?)))
                } else {
                    ansi_compute_op!(array, abs, Int32Array, Int32Type, i32, "Int32")
                }
            }
            DataType::Int64 => {
                if !fail_on_error {
                    let result =
                        legacy_compute_op!(array, wrapping_abs, Int64Array, Int64Array);
                    Ok(ColumnarValue::Array(Arc::new(result?)))
                } else {
                    ansi_compute_op!(array, abs, Int64Array, Int64Type, i64, "Int64")
                }
            }
            DataType::Float32 => {
                let result = legacy_compute_op!(array, abs, Float32Array, Float32Array);
                Ok(ColumnarValue::Array(Arc::new(result?)))
            }
            DataType::Float64 => {
                let result = legacy_compute_op!(array, abs, Float64Array, Float64Array);
                Ok(ColumnarValue::Array(Arc::new(result?)))
            }
            DataType::Decimal128(precision, scale) => {
                if !fail_on_error {
                    let result = legacy_compute_op!(
                        array,
                        wrapping_abs,
                        Decimal128Array,
                        Decimal128Array
                    )?;
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
                    )?;
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
                    let result = legacy_compute_op!(
                        array,
                        wrapping_abs,
                        IntervalYearMonthArray,
                        IntervalYearMonthArray
                    )?;
                    let result = result.with_data_type(DataType::Interval(*unit));
                    Ok(ColumnarValue::Array(Arc::new(result)))
                }
                IntervalUnit::DayTime => {
                    let result = legacy_compute_op!(
                        array,
                        wrapping_abs,
                        IntervalDayTimeArray,
                        IntervalDayTimeArray
                    )?;
                    let result = result.with_data_type(DataType::Interval(*unit));
                    Ok(ColumnarValue::Array(Arc::new(result)))
                }
                IntervalUnit::MonthDayNano => internal_err!(
                    "MonthDayNano is not a supported Interval unit for Spark ABS"
                ),
            },
            dt => internal_err!("Not supported datatype for Spark ABS: {dt}"),
        },
        ColumnarValue::Scalar(sv) => {
            match sv {
                ScalarValue::Null
                | ScalarValue::UInt8(_)
                | ScalarValue::UInt16(_)
                | ScalarValue::UInt32(_)
                | ScalarValue::UInt64(_) => Ok(args[0].clone()),
                ScalarValue::Int8(a) => a
                    .map(|v| match v.checked_abs() {
                        Some(abs_val) => {
                            Ok(ColumnarValue::Scalar(ScalarValue::Int8(Some(abs_val))))
                        }
                        None => {
                            if !fail_on_error {
                                // return the original value
                                Ok(ColumnarValue::Scalar(ScalarValue::Int8(Some(v))))
                            } else {
                                Err(arithmetic_overflow_error("Int8"))
                            }
                        }
                    })
                    .unwrap(),
                ScalarValue::Int16(a) => a
                    .map(|v| match v.checked_abs() {
                        Some(abs_val) => {
                            Ok(ColumnarValue::Scalar(ScalarValue::Int16(Some(abs_val))))
                        }
                        None => {
                            if !fail_on_error {
                                // return the original value
                                Ok(ColumnarValue::Scalar(ScalarValue::Int16(Some(v))))
                            } else {
                                Err(arithmetic_overflow_error("Int16"))
                            }
                        }
                    })
                    .unwrap(),
                ScalarValue::Int32(a) => a
                    .map(|v| match v.checked_abs() {
                        Some(abs_val) => {
                            Ok(ColumnarValue::Scalar(ScalarValue::Int32(Some(abs_val))))
                        }
                        None => {
                            if !fail_on_error {
                                // return the original value
                                Ok(ColumnarValue::Scalar(ScalarValue::Int32(Some(v))))
                            } else {
                                Err(arithmetic_overflow_error("Int32"))
                            }
                        }
                    })
                    .unwrap(),
                ScalarValue::Int64(a) => a
                    .map(|v| match v.checked_abs() {
                        Some(abs_val) => {
                            Ok(ColumnarValue::Scalar(ScalarValue::Int64(Some(abs_val))))
                        }
                        None => {
                            if !fail_on_error {
                                // return the original value
                                Ok(ColumnarValue::Scalar(ScalarValue::Int64(Some(v))))
                            } else {
                                Err(arithmetic_overflow_error("Int64"))
                            }
                        }
                    })
                    .unwrap(),
                ScalarValue::Float32(a) => Ok(ColumnarValue::Scalar(
                    ScalarValue::Float32(a.map(|x| x.abs())),
                )),
                ScalarValue::Float64(a) => Ok(ColumnarValue::Scalar(
                    ScalarValue::Float64(a.map(|x| x.abs())),
                )),
                ScalarValue::Decimal128(a, precision, scale) => a
                    .map(|v| match v.checked_abs() {
                        Some(abs_val) => Ok(ColumnarValue::Scalar(
                            ScalarValue::Decimal128(Some(abs_val), *precision, *scale),
                        )),
                        None => {
                            if !fail_on_error {
                                // return the original value
                                Ok(ColumnarValue::Scalar(ScalarValue::Decimal128(
                                    Some(v),
                                    *precision,
                                    *scale,
                                )))
                            } else {
                                Err(arithmetic_overflow_error("Decimal128"))
                            }
                        }
                    })
                    .unwrap(),
                ScalarValue::Decimal256(a, precision, scale) => a
                    .map(|v| match v.checked_abs() {
                        Some(abs_val) => Ok(ColumnarValue::Scalar(
                            ScalarValue::Decimal256(Some(abs_val), *precision, *scale),
                        )),
                        None => {
                            if !fail_on_error {
                                // return the original value
                                Ok(ColumnarValue::Scalar(ScalarValue::Decimal256(
                                    Some(v),
                                    *precision,
                                    *scale,
                                )))
                            } else {
                                Err(arithmetic_overflow_error("Decimal256"))
                            }
                        }
                    })
                    .unwrap(),
                ScalarValue::IntervalYearMonth(a) => {
                    let result = a.map(|v| v.wrapping_abs());
                    Ok(ColumnarValue::Scalar(ScalarValue::IntervalYearMonth(
                        result,
                    )))
                }
                ScalarValue::IntervalDayTime(a) => {
                    let result = a.map(|v| v.wrapping_abs());
                    Ok(ColumnarValue::Scalar(ScalarValue::IntervalDayTime(result)))
                }

                dt => internal_err!("Not supported datatype for Spark ABS: {dt}"),
            }
        }
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

    // Unsigned types, return as is
    #[test]
    fn test_abs_u8_scalar() {
        with_fail_on_error(|fail_on_error| {
            let args = ColumnarValue::Scalar(ScalarValue::UInt8(Some(u8::MAX)));
            let fail_on_error_arg =
                ColumnarValue::Scalar(ScalarValue::Boolean(Some(fail_on_error)));
            match spark_abs(&[args, fail_on_error_arg]) {
                Ok(ColumnarValue::Scalar(ScalarValue::UInt8(Some(result)))) => {
                    assert_eq!(result, u8::MAX);
                    Ok(())
                }
                Err(e) => {
                    if fail_on_error {
                        assert!(
                            e.to_string().contains("ARITHMETIC_OVERFLOW"),
                            "Error message did not match. Actual message: {e}"
                        );
                        Ok(())
                    } else {
                        panic!("Didn't expect error, but got: {e:?}")
                    }
                }
                _ => unreachable!(),
            }
        });
    }

    #[test]
    fn test_abs_i8_scalar() {
        with_fail_on_error(|fail_on_error| {
            let args = ColumnarValue::Scalar(ScalarValue::Int8(Some(i8::MIN)));
            let fail_on_error_arg =
                ColumnarValue::Scalar(ScalarValue::Boolean(Some(fail_on_error)));
            match spark_abs(&[args, fail_on_error_arg]) {
                Ok(ColumnarValue::Scalar(ScalarValue::Int8(Some(result)))) => {
                    assert_eq!(result, i8::MIN);
                    Ok(())
                }
                Err(e) => {
                    if fail_on_error {
                        assert!(
                            e.to_string().contains("arithmetic overflow"),
                            "Error message did not match. Actual message: {e}"
                        );
                        Ok(())
                    } else {
                        panic!("Didn't expect error, but got: {e:?}")
                    }
                }
                _ => unreachable!(),
            }
        });
    }

    #[test]
    fn test_abs_i16_scalar() {
        with_fail_on_error(|fail_on_error| {
            let args = ColumnarValue::Scalar(ScalarValue::Int16(Some(i16::MIN)));
            let fail_on_error_arg =
                ColumnarValue::Scalar(ScalarValue::Boolean(Some(fail_on_error)));
            match spark_abs(&[args, fail_on_error_arg]) {
                Ok(ColumnarValue::Scalar(ScalarValue::Int16(Some(result)))) => {
                    assert_eq!(result, i16::MIN);
                    Ok(())
                }
                Err(e) => {
                    if fail_on_error {
                        assert!(
                            e.to_string().contains("arithmetic overflow"),
                            "Error message did not match. Actual message: {e}"
                        );
                        Ok(())
                    } else {
                        panic!("Didn't expect error, but got: {e:?}")
                    }
                }
                _ => unreachable!(),
            }
        });
    }

    #[test]
    fn test_abs_i32_scalar() {
        with_fail_on_error(|fail_on_error| {
            let args = ColumnarValue::Scalar(ScalarValue::Int32(Some(i32::MIN)));
            let fail_on_error_arg =
                ColumnarValue::Scalar(ScalarValue::Boolean(Some(fail_on_error)));
            match spark_abs(&[args, fail_on_error_arg]) {
                Ok(ColumnarValue::Scalar(ScalarValue::Int32(Some(result)))) => {
                    assert_eq!(result, i32::MIN);
                    Ok(())
                }
                Err(e) => {
                    if fail_on_error {
                        assert!(
                            e.to_string().contains("arithmetic overflow"),
                            "Error message did not match. Actual message: {e}"
                        );
                        Ok(())
                    } else {
                        panic!("Didn't expect error, but got: {e:?}")
                    }
                }
                _ => unreachable!(),
            }
        });
    }

    #[test]
    fn test_abs_i64_scalar() {
        with_fail_on_error(|fail_on_error| {
            let args = ColumnarValue::Scalar(ScalarValue::Int64(Some(i64::MIN)));
            let fail_on_error_arg =
                ColumnarValue::Scalar(ScalarValue::Boolean(Some(fail_on_error)));
            match spark_abs(&[args, fail_on_error_arg]) {
                Ok(ColumnarValue::Scalar(ScalarValue::Int64(Some(result)))) => {
                    assert_eq!(result, i64::MIN);
                    Ok(())
                }
                Err(e) => {
                    if fail_on_error {
                        assert!(
                            e.to_string().contains("arithmetic overflow"),
                            "Error message did not match. Actual message: {e}"
                        );
                        Ok(())
                    } else {
                        panic!("Didn't expect error, but got: {e:?}")
                    }
                }
                _ => unreachable!(),
            }
        });
    }

    #[test]
    fn test_abs_decimal128_scalar() {
        with_fail_on_error(|fail_on_error| {
            let args =
                ColumnarValue::Scalar(ScalarValue::Decimal128(Some(i128::MIN), 18, 10));
            let fail_on_error_arg =
                ColumnarValue::Scalar(ScalarValue::Boolean(Some(fail_on_error)));
            match spark_abs(&[args, fail_on_error_arg]) {
                Ok(ColumnarValue::Scalar(ScalarValue::Decimal128(
                    Some(result),
                    precision,
                    scale,
                ))) => {
                    assert_eq!(result, i128::MIN);
                    assert_eq!(precision, 18);
                    assert_eq!(scale, 10);
                    Ok(())
                }
                Err(e) => {
                    if fail_on_error {
                        assert!(
                            e.to_string().contains("arithmetic overflow"),
                            "Error message did not match. Actual message: {e}"
                        );
                        Ok(())
                    } else {
                        panic!("Didn't expect error, but got: {e:?}")
                    }
                }
                _ => unreachable!(),
            }
        });
    }

    #[test]
    fn test_abs_decimal256_scalar() {
        with_fail_on_error(|fail_on_error| {
            let args =
                ColumnarValue::Scalar(ScalarValue::Decimal256(Some(i256::MIN), 10, 2));
            let fail_on_error_arg =
                ColumnarValue::Scalar(ScalarValue::Boolean(Some(fail_on_error)));
            match spark_abs(&[args, fail_on_error_arg]) {
                Ok(ColumnarValue::Scalar(ScalarValue::Decimal256(
                    Some(result),
                    precision,
                    scale,
                ))) => {
                    assert_eq!(result, i256::MIN);
                    assert_eq!(precision, 10);
                    assert_eq!(scale, 2);
                    Ok(())
                }
                Err(e) => {
                    if fail_on_error {
                        assert!(
                            e.to_string().contains("arithmetic overflow"),
                            "Error message did not match. Actual message: {e}"
                        );
                        Ok(())
                    } else {
                        panic!("Didn't expect error, but got: {e:?}")
                    }
                }
                _ => unreachable!(),
            }
        });
    }

    #[test]
    fn test_abs_interval_year_month_scalar() {
        with_fail_on_error(|fail_on_error| {
            let args =
                ColumnarValue::Scalar(ScalarValue::IntervalYearMonth(Some(i32::MIN)));
            let fail_on_error_arg =
                ColumnarValue::Scalar(ScalarValue::Boolean(Some(fail_on_error)));
            match spark_abs(&[args, fail_on_error_arg]) {
                Ok(ColumnarValue::Scalar(ScalarValue::IntervalYearMonth(Some(
                    result,
                )))) => {
                    assert_eq!(result, i32::MIN);
                    Ok(())
                }
                Err(e) => {
                    if fail_on_error {
                        assert!(
                            e.to_string().contains("arithmetic overflow"),
                            "Error message did not match. Actual message: {e}"
                        );
                        Ok(())
                    } else {
                        panic!("Didn't expect error, but got: {e:?}")
                    }
                }
                _ => unreachable!(),
            }
        });
    }

    #[test]
    fn test_abs_interval_day_time_scalar() {
        with_fail_on_error(|fail_on_error| {
            let args = ColumnarValue::Scalar(ScalarValue::IntervalDayTime(Some(
                IntervalDayTime::MIN,
            )));
            let fail_on_error_arg =
                ColumnarValue::Scalar(ScalarValue::Boolean(Some(fail_on_error)));
            match spark_abs(&[args, fail_on_error_arg]) {
                Ok(ColumnarValue::Scalar(ScalarValue::IntervalDayTime(Some(result)))) => {
                    assert_eq!(result, IntervalDayTime::MIN);
                    Ok(())
                }
                Err(e) => {
                    if fail_on_error {
                        assert!(
                            e.to_string().contains("arithmetic overflow"),
                            "Error message did not match. Actual message: {e}"
                        );
                        Ok(())
                    } else {
                        panic!("Didn't expect error, but got: {e:?}")
                    }
                }
                _ => unreachable!(),
            }
        });
    }

    #[test]
    fn test_abs_i8_array() {
        with_fail_on_error(|fail_on_error| {
            let input =
                Int8Array::from(vec![Some(-1), Some(i8::MIN), Some(i8::MAX), None]);
            let args = ColumnarValue::Array(Arc::new(input));
            let expected =
                Int8Array::from(vec![Some(1), Some(i8::MIN), Some(i8::MAX), None]);
            let fail_on_error_arg =
                ColumnarValue::Scalar(ScalarValue::Boolean(Some(fail_on_error)));

            match spark_abs(&[args, fail_on_error_arg]) {
                Ok(ColumnarValue::Array(result)) => {
                    let actual = as_int8_array(&result)?;
                    assert_eq!(actual, &expected);
                    Ok(())
                }
                Err(e) => {
                    if fail_on_error {
                        assert!(
                            e.to_string().contains("arithmetic overflow"),
                            "Error message did not match. Actual message: {e}"
                        );
                        Ok(())
                    } else {
                        panic!("Didn't expect error, but got: {e:?}")
                    }
                }
                _ => unreachable!(),
            }
        });
    }

    #[test]
    fn test_abs_i16_array() {
        with_fail_on_error(|fail_on_error| {
            let input =
                Int16Array::from(vec![Some(-1), Some(i16::MIN), Some(i16::MAX), None]);
            let args = ColumnarValue::Array(Arc::new(input));
            let expected =
                Int16Array::from(vec![Some(1), Some(i16::MIN), Some(i16::MAX), None]);
            let fail_on_error_arg =
                ColumnarValue::Scalar(ScalarValue::Boolean(Some(fail_on_error)));

            match spark_abs(&[args, fail_on_error_arg]) {
                Ok(ColumnarValue::Array(result)) => {
                    let actual = as_int16_array(&result)?;
                    assert_eq!(actual, &expected);
                    Ok(())
                }
                Err(e) => {
                    if fail_on_error {
                        assert!(
                            e.to_string().contains("arithmetic overflow"),
                            "Error message did not match. Actual message: {e}"
                        );
                        Ok(())
                    } else {
                        panic!("Didn't expect error, but got: {e:?}")
                    }
                }
                _ => unreachable!(),
            }
        });
    }

    #[test]
    fn test_abs_i32_array() {
        with_fail_on_error(|fail_on_error| {
            let input =
                Int32Array::from(vec![Some(-1), Some(i32::MIN), Some(i32::MAX), None]);
            let args = ColumnarValue::Array(Arc::new(input));
            let expected =
                Int32Array::from(vec![Some(1), Some(i32::MIN), Some(i32::MAX), None]);
            let fail_on_error_arg =
                ColumnarValue::Scalar(ScalarValue::Boolean(Some(fail_on_error)));

            match spark_abs(&[args, fail_on_error_arg]) {
                Ok(ColumnarValue::Array(result)) => {
                    let actual = as_int32_array(&result)?;
                    assert_eq!(actual, &expected);
                    Ok(())
                }
                Err(e) => {
                    if fail_on_error {
                        assert!(
                            e.to_string().contains("arithmetic overflow"),
                            "Error message did not match. Actual message: {e}"
                        );
                        Ok(())
                    } else {
                        panic!("Didn't expect error, but got: {e:?}")
                    }
                }
                _ => unreachable!(),
            }
        });
    }

    #[test]
    fn test_abs_i64_array() {
        with_fail_on_error(|fail_on_error| {
            let input =
                Int64Array::from(vec![Some(-1), Some(i64::MIN), Some(i64::MAX), None]);
            let args = ColumnarValue::Array(Arc::new(input));
            let expected =
                Int64Array::from(vec![Some(1), Some(i64::MIN), Some(i64::MAX), None]);
            let fail_on_error_arg =
                ColumnarValue::Scalar(ScalarValue::Boolean(Some(fail_on_error)));

            match spark_abs(&[args, fail_on_error_arg]) {
                Ok(ColumnarValue::Array(result)) => {
                    let actual = as_int64_array(&result)?;
                    assert_eq!(actual, &expected);
                    Ok(())
                }
                Err(e) => {
                    if fail_on_error {
                        assert!(
                            e.to_string().contains("arithmetic overflow"),
                            "Error message did not match. Actual message: {e}"
                        );
                        Ok(())
                    } else {
                        panic!("Didn't expect error, but got: {e:?}")
                    }
                }
                _ => unreachable!(),
            }
        });
    }

    #[test]
    fn test_abs_f32_array() {
        with_fail_on_error(|fail_on_error| {
            let input = Float32Array::from(vec![
                Some(-1f32),
                Some(f32::MIN),
                Some(f32::MAX),
                None,
                Some(f32::NAN),
            ]);
            let args = ColumnarValue::Array(Arc::new(input));
            let expected = Float32Array::from(vec![
                Some(1f32),
                Some(f32::MAX),
                Some(f32::MAX),
                None,
                Some(f32::NAN),
            ]);
            let fail_on_error_arg =
                ColumnarValue::Scalar(ScalarValue::Boolean(Some(fail_on_error)));

            match spark_abs(&[args, fail_on_error_arg]) {
                Ok(ColumnarValue::Array(result)) => {
                    let actual = as_float32_array(&result)?;
                    assert_eq!(actual, &expected);
                    Ok(())
                }
                Err(e) => {
                    if fail_on_error {
                        assert!(
                            e.to_string().contains("arithmetic overflow"),
                            "Error message did not match. Actual message: {e}"
                        );
                        Ok(())
                    } else {
                        panic!("Didn't expect error, but got: {e:?}")
                    }
                }
                _ => unreachable!(),
            }
        });
    }

    #[test]
    fn test_abs_f64_array() {
        with_fail_on_error(|fail_on_error| {
            let input = Float64Array::from(vec![
                Some(-1f64),
                Some(f64::MIN),
                Some(f64::MAX),
                None,
                Some(f64::NAN),
            ]);
            let args = ColumnarValue::Array(Arc::new(input));
            let expected = Float64Array::from(vec![
                Some(1f64),
                Some(f64::MAX),
                Some(f64::MAX),
                None,
                Some(f64::NAN),
            ]);
            let fail_on_error_arg =
                ColumnarValue::Scalar(ScalarValue::Boolean(Some(fail_on_error)));

            match spark_abs(&[args, fail_on_error_arg]) {
                Ok(ColumnarValue::Array(result)) => {
                    let actual = as_float64_array(&result)?;
                    assert_eq!(actual, &expected);
                    Ok(())
                }
                Err(e) => {
                    if fail_on_error {
                        assert!(
                            e.to_string().contains("arithmetic overflow"),
                            "Error message did not match. Actual message: {e}"
                        );
                        Ok(())
                    } else {
                        panic!("Didn't expect error, but got: {e:?}")
                    }
                }
                _ => unreachable!(),
            }
        });
    }

    #[test]
    fn test_abs_decimal128_array() {
        with_fail_on_error(|fail_on_error| {
            let input = Decimal128Array::from(vec![Some(i128::MIN), None])
                .with_precision_and_scale(38, 37)?;
            let args = ColumnarValue::Array(Arc::new(input));
            let expected = Decimal128Array::from(vec![Some(i128::MIN), None])
                .with_precision_and_scale(38, 37)?;
            let fail_on_error_arg =
                ColumnarValue::Scalar(ScalarValue::Boolean(Some(fail_on_error)));

            match spark_abs(&[args, fail_on_error_arg]) {
                Ok(ColumnarValue::Array(result)) => {
                    let actual = as_decimal128_array(&result)?;
                    assert_eq!(actual, &expected);
                    Ok(())
                }
                Err(e) => {
                    if fail_on_error {
                        assert!(
                            e.to_string().contains("arithmetic overflow"),
                            "Error message did not match. Actual message: {e}"
                        );
                        Ok(())
                    } else {
                        panic!("Didn't expect error, but got: {e:?}")
                    }
                }
                _ => unreachable!(),
            }
        });
    }

    #[test]
    fn test_abs_decimal256_array() {
        with_fail_on_error(|fail_on_error| {
            let input = Decimal256Array::from(vec![Some(i256::MIN), None])
                .with_precision_and_scale(5, 2)?;
            let args = ColumnarValue::Array(Arc::new(input));
            let expected = Decimal256Array::from(vec![Some(i256::MIN), None])
                .with_precision_and_scale(5, 2)?;
            let fail_on_error_arg =
                ColumnarValue::Scalar(ScalarValue::Boolean(Some(fail_on_error)));

            match spark_abs(&[args, fail_on_error_arg]) {
                Ok(ColumnarValue::Array(result)) => {
                    let actual = as_decimal256_array(&result)?;
                    assert_eq!(actual, &expected);
                    Ok(())
                }
                Err(e) => {
                    if fail_on_error {
                        assert!(
                            e.to_string().contains("arithmetic overflow"),
                            "Error message did not match. Actual message: {e}"
                        );
                        Ok(())
                    } else {
                        panic!("Didn't expect error, but got: {e:?}")
                    }
                }
                _ => unreachable!(),
            }
        });
    }

    #[test]
    fn test_abs_interval_year_month_array() {
        with_fail_on_error(|fail_on_error| {
            let input = IntervalYearMonthArray::from(vec![i32::MIN, -1]);
            let args = ColumnarValue::Array(Arc::new(input));
            let expected = IntervalYearMonthArray::from(vec![i32::MIN, 1]);
            let fail_on_error_arg =
                ColumnarValue::Scalar(ScalarValue::Boolean(Some(fail_on_error)));

            match spark_abs(&[args, fail_on_error_arg]) {
                Ok(ColumnarValue::Array(result)) => {
                    let actual = as_interval_ym_array(&result)?;
                    assert_eq!(actual, &expected);
                    Ok(())
                }
                Err(e) => {
                    if fail_on_error {
                        assert!(
                            e.to_string().contains("arithmetic overflow"),
                            "Error message did not match. Actual message: {e}"
                        );
                        Ok(())
                    } else {
                        panic!("Didn't expect error, but got: {e:?}")
                    }
                }
                _ => unreachable!(),
            }
        });
    }

    #[test]
    fn test_abs_interval_day_time_array() {
        with_fail_on_error(|fail_on_error| {
            let input = IntervalDayTimeArray::from(vec![IntervalDayTime::new(
                i32::MIN,
                i32::MIN,
            )]);
            let args = ColumnarValue::Array(Arc::new(input));
            let expected = IntervalDayTimeArray::from(vec![IntervalDayTime::new(
                i32::MIN,
                i32::MIN,
            )]);
            let fail_on_error_arg =
                ColumnarValue::Scalar(ScalarValue::Boolean(Some(fail_on_error)));

            match spark_abs(&[args, fail_on_error_arg]) {
                Ok(ColumnarValue::Array(result)) => {
                    let actual = as_interval_dt_array(&result)?;
                    assert_eq!(actual, &expected);
                    Ok(())
                }
                Err(e) => {
                    if fail_on_error {
                        assert!(
                            e.to_string().contains("arithmetic overflow"),
                            "Error message did not match. Actual message: {e}"
                        );
                        Ok(())
                    } else {
                        panic!("Didn't expect error, but got: {e:?}")
                    }
                }
                _ => unreachable!(),
            }
        });
    }

    #[test]
    fn test_abs_u64_array() {
        with_fail_on_error(|fail_on_error| {
            let input = UInt64Array::from(vec![Some(u64::MIN), Some(u64::MAX), None]);
            let args = ColumnarValue::Array(Arc::new(input));
            let expected = UInt64Array::from(vec![Some(u64::MIN), Some(u64::MAX), None]);
            let fail_on_error_arg =
                ColumnarValue::Scalar(ScalarValue::Boolean(Some(fail_on_error)));

            match spark_abs(&[args, fail_on_error_arg]) {
                Ok(ColumnarValue::Array(result)) => {
                    let actual = as_uint64_array(&result)?;
                    assert_eq!(actual, &expected);
                    Ok(())
                }
                Err(e) => {
                    if fail_on_error {
                        assert!(
                            e.to_string().contains("arithmetic overflow"),
                            "Error message did not match. Actual message: {e}"
                        );
                        Ok(())
                    } else {
                        panic!("Didn't expect error, but got: {e:?}")
                    }
                }
                _ => unreachable!(),
            }
        });
    }
}
