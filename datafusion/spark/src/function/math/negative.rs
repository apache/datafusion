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
use arrow::datatypes::{DataType, IntervalDayTime, IntervalMonthDayNano};
use bigdecimal::num_traits::WrappingNeg;
use datafusion_common::utils::take_function_args;
use datafusion_common::{Result, ScalarValue, not_impl_err};
use datafusion_expr::{
    ColumnarValue, ScalarFunctionArgs, ScalarUDFImpl, Signature, Volatility,
};
use std::any::Any;
use std::sync::Arc;

/// Spark-compatible `negative` expression
/// <https://spark.apache.org/docs/latest/api/sql/index.html#negative>
///
/// Returns the negation of input (equivalent to unary minus)
/// Returns NULL if input is NULL, returns NaN if input is NaN.
///
/// ANSI mode support see (<https://github.com/apache/datafusion/issues/20034>):
///  - Spark's ANSI-compliant dialect, when off (i.e. `spark.sql.ansi.enabled=false`),
///    negating the minimal value of a signed integer wraps around.
///    For example: negative(i32::MIN) returns i32::MIN (wraps instead of error).
///    This is the current implementation (legacy mode only).
///  - Spark's ANSI mode (when `spark.sql.ansi.enabled=true`) should throw an
///    ARITHMETIC_OVERFLOW error on integer overflow instead of wrapping.
///    This is not yet implemented - all operations currently use wrapping behavior.
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
            signature: Signature::numeric(1, Volatility::Immutable),
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
        spark_negative(&args.args)
    }
}

/// Core implementation of Spark's negative function
fn spark_negative(args: &[ColumnarValue]) -> Result<ColumnarValue> {
    let [_] = take_function_args("negative", args)?;

    match &args[0] {
        ColumnarValue::Array(array) => match array.data_type() {
            DataType::Null
            | DataType::UInt8
            | DataType::UInt16
            | DataType::UInt32
            | DataType::UInt64 => Ok(args[0].clone()),

            // Signed integers - use wrapping negation (Spark legacy mode behavior)
            DataType::Int8 => {
                let array = array.as_primitive::<Int8Type>();
                let result: PrimitiveArray<Int8Type> = array.unary(|x| x.wrapping_neg());
                Ok(ColumnarValue::Array(Arc::new(result)))
            }
            DataType::Int16 => {
                let array = array.as_primitive::<Int16Type>();
                let result: PrimitiveArray<Int16Type> = array.unary(|x| x.wrapping_neg());
                Ok(ColumnarValue::Array(Arc::new(result)))
            }
            DataType::Int32 => {
                let array = array.as_primitive::<Int32Type>();
                let result: PrimitiveArray<Int32Type> = array.unary(|x| x.wrapping_neg());
                Ok(ColumnarValue::Array(Arc::new(result)))
            }
            DataType::Int64 => {
                let array = array.as_primitive::<Int64Type>();
                let result: PrimitiveArray<Int64Type> = array.unary(|x| x.wrapping_neg());
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

            // Decimal types - wrapping negation
            DataType::Decimal32(_, _) => {
                let array = array.as_primitive::<Decimal32Type>();
                let result: PrimitiveArray<Decimal32Type> =
                    array.unary(|x| x.wrapping_neg());
                Ok(ColumnarValue::Array(Arc::new(result)))
            }
            DataType::Decimal64(_, _) => {
                let array = array.as_primitive::<Decimal64Type>();
                let result: PrimitiveArray<Decimal64Type> =
                    array.unary(|x| x.wrapping_neg());
                Ok(ColumnarValue::Array(Arc::new(result)))
            }
            DataType::Decimal128(_, _) => {
                let array = array.as_primitive::<Decimal128Type>();
                let result: PrimitiveArray<Decimal128Type> =
                    array.unary(|x| x.wrapping_neg());
                Ok(ColumnarValue::Array(Arc::new(result)))
            }
            DataType::Decimal256(_, _) => {
                let array = array.as_primitive::<Decimal256Type>();
                let result: PrimitiveArray<Decimal256Type> =
                    array.unary(|x| x.wrapping_neg());
                Ok(ColumnarValue::Array(Arc::new(result)))
            }

            // interval type
            DataType::Interval(arrow::datatypes::IntervalUnit::YearMonth) => {
                let array = array.as_primitive::<IntervalYearMonthType>();
                let result: PrimitiveArray<IntervalYearMonthType> =
                    array.unary(|x| x.wrapping_neg());
                Ok(ColumnarValue::Array(Arc::new(result)))
            }
            DataType::Interval(arrow::datatypes::IntervalUnit::DayTime) => {
                let array = array.as_primitive::<IntervalDayTimeType>();
                let result: PrimitiveArray<IntervalDayTimeType> =
                    array.unary(|x| IntervalDayTime {
                        days: x.days.wrapping_neg(),
                        milliseconds: x.milliseconds.wrapping_neg(),
                    });
                Ok(ColumnarValue::Array(Arc::new(result)))
            }
            DataType::Interval(arrow::datatypes::IntervalUnit::MonthDayNano) => {
                let array = array.as_primitive::<IntervalMonthDayNanoType>();
                let result: PrimitiveArray<IntervalMonthDayNanoType> =
                    array.unary(|x| IntervalMonthDayNano {
                        months: x.months.wrapping_neg(),
                        days: x.days.wrapping_neg(),
                        nanoseconds: x.nanoseconds.wrapping_neg(),
                    });
                Ok(ColumnarValue::Array(Arc::new(result)))
            }

            dt => not_impl_err!("Not supported datatype for Spark NEGATIVE: {dt}"),
        },
        ColumnarValue::Scalar(sv) => match sv {
            ScalarValue::Null => Ok(args[0].clone()),
            sv if sv.is_null() => Ok(args[0].clone()),

            // Signed integers - wrapping negation
            ScalarValue::Int8(Some(v)) => {
                let result = v.wrapping_neg();
                Ok(ColumnarValue::Scalar(ScalarValue::Int8(Some(result))))
            }
            ScalarValue::Int16(Some(v)) => {
                let result = v.wrapping_neg();
                Ok(ColumnarValue::Scalar(ScalarValue::Int16(Some(result))))
            }
            ScalarValue::Int32(Some(v)) => {
                let result = v.wrapping_neg();
                Ok(ColumnarValue::Scalar(ScalarValue::Int32(Some(result))))
            }
            ScalarValue::Int64(Some(v)) => {
                let result = v.wrapping_neg();
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

            // Decimal types - wrapping negation
            ScalarValue::Decimal32(Some(v), precision, scale) => {
                let result = v.wrapping_neg();
                Ok(ColumnarValue::Scalar(ScalarValue::Decimal32(
                    Some(result),
                    *precision,
                    *scale,
                )))
            }
            ScalarValue::Decimal64(Some(v), precision, scale) => {
                let result = v.wrapping_neg();
                Ok(ColumnarValue::Scalar(ScalarValue::Decimal64(
                    Some(result),
                    *precision,
                    *scale,
                )))
            }
            ScalarValue::Decimal128(Some(v), precision, scale) => {
                let result = v.wrapping_neg();
                Ok(ColumnarValue::Scalar(ScalarValue::Decimal128(
                    Some(result),
                    *precision,
                    *scale,
                )))
            }
            ScalarValue::Decimal256(Some(v), precision, scale) => {
                let result = v.wrapping_neg();
                Ok(ColumnarValue::Scalar(ScalarValue::Decimal256(
                    Some(result),
                    *precision,
                    *scale,
                )))
            }

            //interval type
            ScalarValue::IntervalYearMonth(Some(v)) => Ok(ColumnarValue::Scalar(
                ScalarValue::IntervalYearMonth(Some(v.wrapping_neg())),
            )),
            ScalarValue::IntervalDayTime(Some(v)) => Ok(ColumnarValue::Scalar(
                ScalarValue::IntervalDayTime(Some(IntervalDayTime {
                    days: v.days.wrapping_neg(),
                    milliseconds: v.milliseconds.wrapping_neg(),
                })),
            )),
            ScalarValue::IntervalMonthDayNano(Some(v)) => Ok(ColumnarValue::Scalar(
                ScalarValue::IntervalMonthDayNano(Some(IntervalMonthDayNano {
                    months: v.months.wrapping_neg(),
                    days: v.days.wrapping_neg(),
                    nanoseconds: v.nanoseconds.wrapping_neg(),
                })),
            )),

            dt => not_impl_err!("Not supported datatype for Spark NEGATIVE: {dt}"),
        },
    }
}
