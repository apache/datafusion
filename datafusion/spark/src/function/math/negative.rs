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

use arrow::array::types::*;
use arrow::array::*;
use arrow::datatypes::{DataType, IntervalDayTime, IntervalMonthDayNano, IntervalUnit};
use arrow::error::ArrowError;
use bigdecimal::num_traits::WrappingNeg;
use datafusion_common::utils::take_function_args;
use datafusion_common::{Result, ScalarValue, not_impl_err};
use datafusion_expr::{
    ColumnarValue, ScalarFunctionArgs, ScalarUDFImpl, Signature, TypeSignature,
    Volatility,
};
use std::any::Any;
use std::sync::Arc;

/// Spark-compatible `negative` expression
/// <https://spark.apache.org/docs/latest/api/sql/index.html#negative>
///
/// Returns the negation of input (equivalent to unary minus)
/// Returns NULL if input is NULL, returns NaN if input is NaN.
///
/// ANSI mode support:
///  - When ANSI mode is disabled (`spark.sql.ansi.enabled=false`), negating the minimal
///    value of a signed integer wraps around. For example: negative(i32::MIN) returns
///    i32::MIN (wraps instead of error).
///  - When ANSI mode is enabled (`spark.sql.ansi.enabled=true`), overflow conditions
///    throw an ARITHMETIC_OVERFLOW error instead of wrapping.
///
#[derive(Debug, PartialEq, Eq, Hash)]
pub struct SparkNegative {
    signature: Signature,
}

impl Default for SparkNegative {
    fn default() -> Self {
        Self::new()
    }
}

impl SparkNegative {
    pub fn new() -> Self {
        Self {
            signature: Signature {
                type_signature: TypeSignature::OneOf(vec![
                    // Numeric types: signed integers, float, decimals
                    TypeSignature::Numeric(1),
                    // Interval types: YearMonth, DayTime, MonthDayNano
                    TypeSignature::Uniform(
                        1,
                        vec![
                            DataType::Interval(IntervalUnit::YearMonth),
                            DataType::Interval(IntervalUnit::DayTime),
                            DataType::Interval(IntervalUnit::MonthDayNano),
                        ],
                    ),
                ]),
                volatility: Volatility::Immutable,
                parameter_names: None,
            },
        }
    }
}

impl ScalarUDFImpl for SparkNegative {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "negative"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        Ok(arg_types[0].clone())
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        spark_negative(&args.args, args.config_options.execution.enable_ansi_mode)
    }
}

/// Core implementation of Spark's negative function
fn spark_negative(
    args: &[ColumnarValue],
    enable_ansi_mode: bool,
) -> Result<ColumnarValue> {
    let [arg] = take_function_args("negative", args)?;

    match arg {
        ColumnarValue::Array(array) => match array.data_type() {
            DataType::Null => Ok(arg.clone()),

            // Signed integers - use checked negation in ANSI mode, wrapping in legacy mode
            DataType::Int8 => {
                let array = array.as_primitive::<Int8Type>();
                let result: PrimitiveArray<Int8Type> = if enable_ansi_mode {
                    array.try_unary(|x| {
                        x.checked_neg().ok_or_else(|| {
                            ArrowError::ComputeError(format!(
                                "Int8 overflow on negative({x})"
                            ))
                        })
                    })?
                } else {
                    array.unary(|x| x.wrapping_neg())
                };
                Ok(ColumnarValue::Array(Arc::new(result)))
            }
            DataType::Int16 => {
                let array = array.as_primitive::<Int16Type>();
                let result: PrimitiveArray<Int16Type> = if enable_ansi_mode {
                    array.try_unary(|x| {
                        x.checked_neg().ok_or_else(|| {
                            ArrowError::ComputeError(format!(
                                "Int16 overflow on negative({x})"
                            ))
                        })
                    })?
                } else {
                    array.unary(|x| x.wrapping_neg())
                };
                Ok(ColumnarValue::Array(Arc::new(result)))
            }
            DataType::Int32 => {
                let array = array.as_primitive::<Int32Type>();
                let result: PrimitiveArray<Int32Type> = if enable_ansi_mode {
                    array.try_unary(|x| {
                        x.checked_neg().ok_or_else(|| {
                            ArrowError::ComputeError(format!(
                                "Int32 overflow on negative({x})"
                            ))
                        })
                    })?
                } else {
                    array.unary(|x| x.wrapping_neg())
                };
                Ok(ColumnarValue::Array(Arc::new(result)))
            }
            DataType::Int64 => {
                let array = array.as_primitive::<Int64Type>();
                let result: PrimitiveArray<Int64Type> = if enable_ansi_mode {
                    array.try_unary(|x| {
                        x.checked_neg().ok_or_else(|| {
                            ArrowError::ComputeError(format!(
                                "Int64 overflow on negative({x})"
                            ))
                        })
                    })?
                } else {
                    array.unary(|x| x.wrapping_neg())
                };
                Ok(ColumnarValue::Array(Arc::new(result)))
            }

            // Floating point - simple negation (no overflow possible)
            DataType::Float16 => {
                let array = array.as_primitive::<Float16Type>();
                let result: PrimitiveArray<Float16Type> = array.unary(|x| -x);
                Ok(ColumnarValue::Array(Arc::new(result)))
            }
            DataType::Float32 => {
                let array = array.as_primitive::<Float32Type>();
                let result: PrimitiveArray<Float32Type> = array.unary(|x| -x);
                Ok(ColumnarValue::Array(Arc::new(result)))
            }
            DataType::Float64 => {
                let array = array.as_primitive::<Float64Type>();
                let result: PrimitiveArray<Float64Type> = array.unary(|x| -x);
                Ok(ColumnarValue::Array(Arc::new(result)))
            }

            // Decimal types - use checked negation in ANSI mode, wrapping in legacy mode
            DataType::Decimal32(_, _) => {
                let array = array.as_primitive::<Decimal32Type>();
                let result: PrimitiveArray<Decimal32Type> = if enable_ansi_mode {
                    array
                        .try_unary(|x| {
                            x.checked_neg().ok_or_else(|| {
                                ArrowError::ComputeError(format!(
                                    "Decimal32 overflow on negative({x})"
                                ))
                            })
                        })?
                        .with_data_type(array.data_type().clone())
                } else {
                    array.unary(|x| x.wrapping_neg())
                };
                Ok(ColumnarValue::Array(Arc::new(result)))
            }
            DataType::Decimal64(_, _) => {
                let array = array.as_primitive::<Decimal64Type>();
                let result: PrimitiveArray<Decimal64Type> = if enable_ansi_mode {
                    array
                        .try_unary(|x| {
                            x.checked_neg().ok_or_else(|| {
                                ArrowError::ComputeError(format!(
                                    "Decimal64 overflow on negative({x})"
                                ))
                            })
                        })?
                        .with_data_type(array.data_type().clone())
                } else {
                    array.unary(|x| x.wrapping_neg())
                };
                Ok(ColumnarValue::Array(Arc::new(result)))
            }
            DataType::Decimal128(_, _) => {
                let array = array.as_primitive::<Decimal128Type>();
                let result: PrimitiveArray<Decimal128Type> = if enable_ansi_mode {
                    array
                        .try_unary(|x| {
                            x.checked_neg().ok_or_else(|| {
                                ArrowError::ComputeError(format!(
                                    "Decimal128 overflow on negative({x})"
                                ))
                            })
                        })?
                        .with_data_type(array.data_type().clone())
                } else {
                    array.unary(|x| x.wrapping_neg())
                };
                Ok(ColumnarValue::Array(Arc::new(result)))
            }
            DataType::Decimal256(_, _) => {
                let array = array.as_primitive::<Decimal256Type>();
                let result: PrimitiveArray<Decimal256Type> = if enable_ansi_mode {
                    array
                        .try_unary(|x| {
                            x.checked_neg().ok_or_else(|| {
                                ArrowError::ComputeError(format!(
                                    "Decimal256 overflow on negative({x})"
                                ))
                            })
                        })?
                        .with_data_type(array.data_type().clone())
                } else {
                    array.unary(|x| x.wrapping_neg())
                };
                Ok(ColumnarValue::Array(Arc::new(result)))
            }

            // interval type - use checked negation in ANSI mode, wrapping in legacy mode
            DataType::Interval(IntervalUnit::YearMonth) => {
                let array = array.as_primitive::<IntervalYearMonthType>();
                let result: PrimitiveArray<IntervalYearMonthType> = if enable_ansi_mode {
                    array.try_unary(|x| {
                        x.checked_neg().ok_or_else(|| {
                            ArrowError::ComputeError(format!(
                                "IntervalYearMonth overflow on negative({x})"
                            ))
                        })
                    })?
                } else {
                    array.unary(|x| x.wrapping_neg())
                };
                Ok(ColumnarValue::Array(Arc::new(result)))
            }
            DataType::Interval(IntervalUnit::DayTime) => {
                let array = array.as_primitive::<IntervalDayTimeType>();
                let result: PrimitiveArray<IntervalDayTimeType> = if enable_ansi_mode {
                    array.try_unary(|x| {
                        let days = x.days.checked_neg().ok_or_else(|| {
                            ArrowError::ComputeError(format!(
                                "IntervalDayTime overflow on negative (days: {})",
                                x.days
                            ))
                        })?;
                        let milliseconds = x.milliseconds.checked_neg().ok_or_else(|| {
                            ArrowError::ComputeError(format!(
                                "IntervalDayTime overflow on negative (milliseconds: {})",
                                x.milliseconds
                            ))
                        })?;
                        Ok::<_, ArrowError>(IntervalDayTime { days, milliseconds })
                    })?
                } else {
                    array.unary(|x| IntervalDayTime {
                        days: x.days.wrapping_neg(),
                        milliseconds: x.milliseconds.wrapping_neg(),
                    })
                };
                Ok(ColumnarValue::Array(Arc::new(result)))
            }
            DataType::Interval(IntervalUnit::MonthDayNano) => {
                let array = array.as_primitive::<IntervalMonthDayNanoType>();
                let result: PrimitiveArray<IntervalMonthDayNanoType> = if enable_ansi_mode
                {
                    array.try_unary(|x| {
                        let months = x.months.checked_neg().ok_or_else(|| {
                            ArrowError::ComputeError(format!(
                                "IntervalMonthDayNano overflow on negative (months: {})",
                                x.months
                            ))
                        })?;
                        let days = x.days.checked_neg().ok_or_else(|| {
                            ArrowError::ComputeError(format!(
                                "IntervalMonthDayNano overflow on negative (days: {})",
                                x.days
                            ))
                        })?;
                        let nanoseconds = x.nanoseconds.checked_neg().ok_or_else(|| {
                            ArrowError::ComputeError(format!(
                                "IntervalMonthDayNano overflow on negative (nanoseconds: {})",
                                x.nanoseconds
                            ))
                        })?;
                        Ok::<_, ArrowError>(IntervalMonthDayNano {
                            months,
                            days,
                            nanoseconds,
                        })
                    })?
                } else {
                    array.unary(|x| IntervalMonthDayNano {
                        months: x.months.wrapping_neg(),
                        days: x.days.wrapping_neg(),
                        nanoseconds: x.nanoseconds.wrapping_neg(),
                    })
                };
                Ok(ColumnarValue::Array(Arc::new(result)))
            }

            dt => not_impl_err!("Not supported datatype for Spark negative(): {dt}"),
        },
        ColumnarValue::Scalar(sv) => match sv {
            ScalarValue::Null => Ok(arg.clone()),
            _ if sv.is_null() => Ok(arg.clone()),

            // Signed integers - use checked negation in ANSI mode, wrapping in legacy mode
            ScalarValue::Int8(Some(v)) => {
                let result = if enable_ansi_mode {
                    v.checked_neg().ok_or_else(|| {
                        ArrowError::ComputeError(format!(
                            "Int8 overflow on negative({v})"
                        ))
                    })?
                } else {
                    v.wrapping_neg()
                };
                Ok(ColumnarValue::Scalar(ScalarValue::Int8(Some(result))))
            }
            ScalarValue::Int16(Some(v)) => {
                let result = if enable_ansi_mode {
                    v.checked_neg().ok_or_else(|| {
                        ArrowError::ComputeError(format!(
                            "Int16 overflow on negative({v})"
                        ))
                    })?
                } else {
                    v.wrapping_neg()
                };
                Ok(ColumnarValue::Scalar(ScalarValue::Int16(Some(result))))
            }
            ScalarValue::Int32(Some(v)) => {
                let result = if enable_ansi_mode {
                    v.checked_neg().ok_or_else(|| {
                        ArrowError::ComputeError(format!(
                            "Int32 overflow on negative({v})"
                        ))
                    })?
                } else {
                    v.wrapping_neg()
                };
                Ok(ColumnarValue::Scalar(ScalarValue::Int32(Some(result))))
            }
            ScalarValue::Int64(Some(v)) => {
                let result = if enable_ansi_mode {
                    v.checked_neg().ok_or_else(|| {
                        ArrowError::ComputeError(format!(
                            "Int64 overflow on negative({v})"
                        ))
                    })?
                } else {
                    v.wrapping_neg()
                };
                Ok(ColumnarValue::Scalar(ScalarValue::Int64(Some(result))))
            }

            // Floating point - simple negation
            ScalarValue::Float16(Some(v)) => {
                Ok(ColumnarValue::Scalar(ScalarValue::Float16(Some(-v))))
            }
            ScalarValue::Float32(Some(v)) => {
                Ok(ColumnarValue::Scalar(ScalarValue::Float32(Some(-v))))
            }
            ScalarValue::Float64(Some(v)) => {
                Ok(ColumnarValue::Scalar(ScalarValue::Float64(Some(-v))))
            }

            // Decimal types - use checked negation in ANSI mode, wrapping in legacy mode
            ScalarValue::Decimal32(Some(v), precision, scale) => {
                let result = if enable_ansi_mode {
                    v.checked_neg().ok_or_else(|| {
                        ArrowError::ComputeError(format!(
                            "Decimal32 overflow on negative({v})"
                        ))
                    })?
                } else {
                    v.wrapping_neg()
                };
                Ok(ColumnarValue::Scalar(ScalarValue::Decimal32(
                    Some(result),
                    *precision,
                    *scale,
                )))
            }
            ScalarValue::Decimal64(Some(v), precision, scale) => {
                let result = if enable_ansi_mode {
                    v.checked_neg().ok_or_else(|| {
                        ArrowError::ComputeError(format!(
                            "Decimal64 overflow on negative({v})"
                        ))
                    })?
                } else {
                    v.wrapping_neg()
                };
                Ok(ColumnarValue::Scalar(ScalarValue::Decimal64(
                    Some(result),
                    *precision,
                    *scale,
                )))
            }
            ScalarValue::Decimal128(Some(v), precision, scale) => {
                let result = if enable_ansi_mode {
                    v.checked_neg().ok_or_else(|| {
                        ArrowError::ComputeError(format!(
                            "Decimal128 overflow on negative({v})"
                        ))
                    })?
                } else {
                    v.wrapping_neg()
                };
                Ok(ColumnarValue::Scalar(ScalarValue::Decimal128(
                    Some(result),
                    *precision,
                    *scale,
                )))
            }
            ScalarValue::Decimal256(Some(v), precision, scale) => {
                let result = if enable_ansi_mode {
                    v.checked_neg().ok_or_else(|| {
                        ArrowError::ComputeError(format!(
                            "Decimal256 overflow on negative({v})"
                        ))
                    })?
                } else {
                    v.wrapping_neg()
                };
                Ok(ColumnarValue::Scalar(ScalarValue::Decimal256(
                    Some(result),
                    *precision,
                    *scale,
                )))
            }

            //interval type - use checked negation in ANSI mode, wrapping in legacy mode
            ScalarValue::IntervalYearMonth(Some(v)) => {
                let result = if enable_ansi_mode {
                    v.checked_neg().ok_or_else(|| {
                        ArrowError::ComputeError(format!(
                            "IntervalYearMonth overflow on negative({v})"
                        ))
                    })?
                } else {
                    v.wrapping_neg()
                };
                Ok(ColumnarValue::Scalar(ScalarValue::IntervalYearMonth(Some(
                    result,
                ))))
            }
            ScalarValue::IntervalDayTime(Some(v)) => {
                let result = if enable_ansi_mode {
                    let days = v.days.checked_neg().ok_or_else(|| {
                        ArrowError::ComputeError(format!(
                            "IntervalDayTime overflow on negative (days: {})",
                            v.days
                        ))
                    })?;
                    let milliseconds = v.milliseconds.checked_neg().ok_or_else(|| {
                        ArrowError::ComputeError(format!(
                            "IntervalDayTime overflow on negative (milliseconds: {})",
                            v.milliseconds
                        ))
                    })?;
                    IntervalDayTime { days, milliseconds }
                } else {
                    IntervalDayTime {
                        days: v.days.wrapping_neg(),
                        milliseconds: v.milliseconds.wrapping_neg(),
                    }
                };
                Ok(ColumnarValue::Scalar(ScalarValue::IntervalDayTime(Some(
                    result,
                ))))
            }
            ScalarValue::IntervalMonthDayNano(Some(v)) => {
                let result = if enable_ansi_mode {
                    let months = v.months.checked_neg().ok_or_else(|| {
                        ArrowError::ComputeError(format!(
                            "IntervalMonthDayNano overflow on negative (months: {})",
                            v.months
                        ))
                    })?;
                    let days = v.days.checked_neg().ok_or_else(|| {
                        ArrowError::ComputeError(format!(
                            "IntervalMonthDayNano overflow on negative (days: {})",
                            v.days
                        ))
                    })?;
                    let nanoseconds = v.nanoseconds.checked_neg().ok_or_else(|| {
                        ArrowError::ComputeError(format!(
                            "IntervalMonthDayNano overflow on negative (nanoseconds: {})",
                            v.nanoseconds
                        ))
                    })?;
                    IntervalMonthDayNano {
                        months,
                        days,
                        nanoseconds,
                    }
                } else {
                    IntervalMonthDayNano {
                        months: v.months.wrapping_neg(),
                        days: v.days.wrapping_neg(),
                        nanoseconds: v.nanoseconds.wrapping_neg(),
                    }
                };
                Ok(ColumnarValue::Scalar(ScalarValue::IntervalMonthDayNano(
                    Some(result),
                )))
            }

            dt => not_impl_err!("Not supported datatype for Spark negative(): {dt}"),
        },
    }
}
