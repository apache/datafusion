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
use bigdecimal::num_traits::WrappingNeg;
use datafusion_common::utils::take_function_args;
use datafusion_common::{Result, ScalarValue, exec_err, not_impl_err};
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

/// Macro to implement negation for integer array types
macro_rules! impl_integer_array_negative {
    ($array:expr, $type:ty, $type_name:expr, $enable_ansi_mode:expr) => {{
        let array = $array.as_primitive::<$type>();
        let result: PrimitiveArray<$type> = if $enable_ansi_mode {
            array.try_unary(|x| {
                x.checked_neg().ok_or_else(|| {
                    (exec_err!("{} overflow on negative({x})", $type_name)
                        as Result<(), _>)
                        .unwrap_err()
                })
            })?
        } else {
            array.unary(|x| x.wrapping_neg())
        };
        Ok(ColumnarValue::Array(Arc::new(result)))
    }};
}

/// Macro to implement negation for float array types
macro_rules! impl_float_array_negative {
    ($array:expr, $type:ty) => {{
        let array = $array.as_primitive::<$type>();
        let result: PrimitiveArray<$type> = array.unary(|x| -x);
        Ok(ColumnarValue::Array(Arc::new(result)))
    }};
}

/// Macro to implement negation for decimal array types
macro_rules! impl_decimal_array_negative {
    ($array:expr, $type:ty, $type_name:expr, $enable_ansi_mode:expr) => {{
        let array = $array.as_primitive::<$type>();
        let result: PrimitiveArray<$type> = if $enable_ansi_mode {
            array
                .try_unary(|x| {
                    x.checked_neg().ok_or_else(|| {
                        (exec_err!("{} overflow on negative({x})", $type_name)
                            as Result<(), _>)
                            .unwrap_err()
                    })
                })?
                .with_data_type(array.data_type().clone())
        } else {
            array.unary(|x| x.wrapping_neg())
        };
        Ok(ColumnarValue::Array(Arc::new(result)))
    }};
}

/// Macro to implement negation for integer scalar types
macro_rules! impl_integer_scalar_negative {
    ($v:expr, $type_name:expr, $variant:ident, $enable_ansi_mode:expr) => {{
        let result = if $enable_ansi_mode {
            $v.checked_neg().ok_or_else(|| {
                (exec_err!("{} overflow on negative({})", $type_name, $v)
                    as Result<(), _>)
                    .unwrap_err()
            })?
        } else {
            $v.wrapping_neg()
        };
        Ok(ColumnarValue::Scalar(ScalarValue::$variant(Some(result))))
    }};
}

/// Macro to implement negation for decimal scalar types
macro_rules! impl_decimal_scalar_negative {
    ($v:expr, $precision:expr, $scale:expr, $type_name:expr, $variant:ident, $enable_ansi_mode:expr) => {{
        let result = if $enable_ansi_mode {
            $v.checked_neg().ok_or_else(|| {
                (exec_err!("{} overflow on negative({})", $type_name, $v)
                    as Result<(), _>)
                    .unwrap_err()
            })?
        } else {
            $v.wrapping_neg()
        };
        Ok(ColumnarValue::Scalar(ScalarValue::$variant(
            Some(result),
            *$precision,
            *$scale,
        )))
    }};
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
                impl_integer_array_negative!(array, Int8Type, "Int8", enable_ansi_mode)
            }
            DataType::Int16 => {
                impl_integer_array_negative!(array, Int16Type, "Int16", enable_ansi_mode)
            }
            DataType::Int32 => {
                impl_integer_array_negative!(array, Int32Type, "Int32", enable_ansi_mode)
            }
            DataType::Int64 => {
                impl_integer_array_negative!(array, Int64Type, "Int64", enable_ansi_mode)
            }

            // Floating point - simple negation (no overflow possible)
            DataType::Float16 => impl_float_array_negative!(array, Float16Type),
            DataType::Float32 => impl_float_array_negative!(array, Float32Type),
            DataType::Float64 => impl_float_array_negative!(array, Float64Type),

            // Decimal types - use checked negation in ANSI mode, wrapping in legacy mode
            DataType::Decimal32(_, _) => impl_decimal_array_negative!(
                array,
                Decimal32Type,
                "Decimal32",
                enable_ansi_mode
            ),
            DataType::Decimal64(_, _) => impl_decimal_array_negative!(
                array,
                Decimal64Type,
                "Decimal64",
                enable_ansi_mode
            ),
            DataType::Decimal128(_, _) => impl_decimal_array_negative!(
                array,
                Decimal128Type,
                "Decimal128",
                enable_ansi_mode
            ),
            DataType::Decimal256(_, _) => impl_decimal_array_negative!(
                array,
                Decimal256Type,
                "Decimal256",
                enable_ansi_mode
            ),

            // interval type - use checked negation in ANSI mode, wrapping in legacy mode
            DataType::Interval(IntervalUnit::YearMonth) => {
                impl_integer_array_negative!(
                    array,
                    IntervalYearMonthType,
                    "IntervalYearMonth",
                    enable_ansi_mode
                )
            }
            DataType::Interval(IntervalUnit::DayTime) => {
                let array = array.as_primitive::<IntervalDayTimeType>();
                let result: PrimitiveArray<IntervalDayTimeType> = if enable_ansi_mode {
                    array.try_unary(|x| {
                        let days = x.days.checked_neg().ok_or_else(|| {
                            (exec_err!(
                                "IntervalDayTime overflow on negative (days: {})",
                                x.days
                            ) as Result<(), _>)
                                .unwrap_err()
                        })?;
                        let milliseconds =
                            x.milliseconds.checked_neg().ok_or_else(|| {
                                (exec_err!(
                                "IntervalDayTime overflow on negative (milliseconds: {})",
                                x.milliseconds
                            ) as Result<(), _>)
                                .unwrap_err()
                            })?;
                        Ok::<_, arrow::error::ArrowError>(IntervalDayTime {
                            days,
                            milliseconds,
                        })
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
                            (exec_err!(
                                "IntervalMonthDayNano overflow on negative (months: {})",
                                x.months
                            ) as Result<(), _>)
                                .unwrap_err()
                        })?;
                        let days = x.days.checked_neg().ok_or_else(|| {
                            (exec_err!(
                                "IntervalMonthDayNano overflow on negative (days: {})",
                                x.days
                            ) as Result<(), _>)
                                .unwrap_err()
                        })?;
                        let nanoseconds = x.nanoseconds.checked_neg().ok_or_else(|| {
                            (exec_err!(
                                "IntervalMonthDayNano overflow on negative (nanoseconds: {})",
                                x.nanoseconds
                            ) as Result<(), _>)
                                .unwrap_err()
                        })?;
                        Ok::<_, arrow::error::ArrowError>(IntervalMonthDayNano {
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
                impl_integer_scalar_negative!(v, "Int8", Int8, enable_ansi_mode)
            }
            ScalarValue::Int16(Some(v)) => {
                impl_integer_scalar_negative!(v, "Int16", Int16, enable_ansi_mode)
            }
            ScalarValue::Int32(Some(v)) => {
                impl_integer_scalar_negative!(v, "Int32", Int32, enable_ansi_mode)
            }
            ScalarValue::Int64(Some(v)) => {
                impl_integer_scalar_negative!(v, "Int64", Int64, enable_ansi_mode)
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
                impl_decimal_scalar_negative!(
                    v,
                    precision,
                    scale,
                    "Decimal32",
                    Decimal32,
                    enable_ansi_mode
                )
            }
            ScalarValue::Decimal64(Some(v), precision, scale) => {
                impl_decimal_scalar_negative!(
                    v,
                    precision,
                    scale,
                    "Decimal64",
                    Decimal64,
                    enable_ansi_mode
                )
            }
            ScalarValue::Decimal128(Some(v), precision, scale) => {
                impl_decimal_scalar_negative!(
                    v,
                    precision,
                    scale,
                    "Decimal128",
                    Decimal128,
                    enable_ansi_mode
                )
            }
            ScalarValue::Decimal256(Some(v), precision, scale) => {
                impl_decimal_scalar_negative!(
                    v,
                    precision,
                    scale,
                    "Decimal256",
                    Decimal256,
                    enable_ansi_mode
                )
            }

            //interval type - use checked negation in ANSI mode, wrapping in legacy mode
            ScalarValue::IntervalYearMonth(Some(v)) => {
                impl_integer_scalar_negative!(
                    v,
                    "IntervalYearMonth",
                    IntervalYearMonth,
                    enable_ansi_mode
                )
            }
            ScalarValue::IntervalDayTime(Some(v)) => {
                let result = if enable_ansi_mode {
                    let days = v.days.checked_neg().ok_or_else(|| {
                        (exec_err!(
                            "IntervalDayTime overflow on negative (days: {})",
                            v.days
                        ) as Result<(), _>)
                            .unwrap_err()
                    })?;
                    let milliseconds = v.milliseconds.checked_neg().ok_or_else(|| {
                        (exec_err!(
                            "IntervalDayTime overflow on negative (milliseconds: {})",
                            v.milliseconds
                        ) as Result<(), _>)
                            .unwrap_err()
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
                        (exec_err!(
                            "IntervalMonthDayNano overflow on negative (months: {})",
                            v.months
                        ) as Result<(), _>)
                            .unwrap_err()
                    })?;
                    let days = v.days.checked_neg().ok_or_else(|| {
                        (exec_err!(
                            "IntervalMonthDayNano overflow on negative (days: {})",
                            v.days
                        ) as Result<(), _>)
                            .unwrap_err()
                    })?;
                    let nanoseconds = v.nanoseconds.checked_neg().ok_or_else(|| {
                        (exec_err!(
                            "IntervalMonthDayNano overflow on negative (nanoseconds: {})",
                            v.nanoseconds
                        ) as Result<(), _>)
                            .unwrap_err()
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
