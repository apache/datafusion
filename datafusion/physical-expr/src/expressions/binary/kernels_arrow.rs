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

//! This module contains computation kernels that are eventually
//! destined for arrow-rs but are in datafusion until they are ported.

use arrow::compute::{
    add_dyn, add_scalar_dyn, divide_dyn_checked, divide_scalar_dyn, modulus_dyn,
    modulus_scalar_dyn, multiply_dyn, multiply_scalar_dyn, subtract_dyn,
    subtract_scalar_dyn, try_unary,
};
use arrow::datatypes::{Date32Type, Date64Type, Decimal128Type};
use arrow::{array::*, datatypes::ArrowNumericType, downcast_dictionary_array};
use arrow_array::ArrowNativeTypeOp;
use arrow_schema::{DataType, IntervalUnit};
use chrono::{Days, Duration, Months, NaiveDate, NaiveDateTime};
use datafusion_common::cast::{as_date32_array, as_date64_array, as_decimal128_array};
use datafusion_common::scalar::{date32_op, date64_op};
use datafusion_common::{DataFusionError, Result, ScalarValue};
use std::ops::Add;
use std::sync::Arc;

use arrow::compute::unary;
use arrow::datatypes::*;

use arrow_array::temporal_conversions::{MILLISECONDS_IN_DAY, NANOSECONDS_IN_DAY};
use datafusion_common::delta::shift_months;
use datafusion_common::scalar::{
    calculate_naives, microseconds_add, microseconds_sub, milliseconds_add,
    milliseconds_sub, nanoseconds_add, nanoseconds_sub, op_dt, op_dt_mdn, op_mdn, op_ym,
    op_ym_dt, op_ym_mdn, parse_timezones, seconds_add, MILLISECOND_MODE, NANOSECOND_MODE,
};

use arrow::datatypes::TimeUnit;

use datafusion_common::cast::{
    as_interval_dt_array, as_interval_mdn_array, as_interval_ym_array,
    as_timestamp_microsecond_array, as_timestamp_millisecond_array,
    as_timestamp_nanosecond_array, as_timestamp_second_array,
};
use datafusion_common::scalar::*;

// Simple (low performance) kernels until optimized kernels are added to arrow
// See https://github.com/apache/arrow-rs/issues/960

macro_rules! distinct_float {
    ($LEFT:expr, $RIGHT:expr, $LEFT_ISNULL:expr, $RIGHT_ISNULL:expr) => {{
        $LEFT_ISNULL != $RIGHT_ISNULL
            || $LEFT.is_nan() != $RIGHT.is_nan()
            || (!$LEFT.is_nan() && !$RIGHT.is_nan() && $LEFT != $RIGHT)
    }};
}

pub(crate) fn is_distinct_from_bool(
    left: &BooleanArray,
    right: &BooleanArray,
) -> Result<BooleanArray> {
    // Different from `neq_bool` because `null is distinct from null` is false and not null
    Ok(left
        .iter()
        .zip(right.iter())
        .map(|(left, right)| Some(left != right))
        .collect())
}

pub(crate) fn is_not_distinct_from_bool(
    left: &BooleanArray,
    right: &BooleanArray,
) -> Result<BooleanArray> {
    Ok(left
        .iter()
        .zip(right.iter())
        .map(|(left, right)| Some(left == right))
        .collect())
}

pub(crate) fn is_distinct_from<T>(
    left: &PrimitiveArray<T>,
    right: &PrimitiveArray<T>,
) -> Result<BooleanArray>
where
    T: ArrowPrimitiveType,
{
    distinct(
        left,
        right,
        |left_value, right_value, left_isnull, right_isnull| {
            left_isnull != right_isnull || left_value != right_value
        },
    )
}

pub(crate) fn is_not_distinct_from<T>(
    left: &PrimitiveArray<T>,
    right: &PrimitiveArray<T>,
) -> Result<BooleanArray>
where
    T: ArrowNumericType,
{
    distinct(
        left,
        right,
        |left_value, right_value, left_isnull, right_isnull| {
            !(left_isnull != right_isnull || left_value != right_value)
        },
    )
}

fn distinct<
    T,
    F: FnMut(
        <T as ArrowPrimitiveType>::Native,
        <T as ArrowPrimitiveType>::Native,
        bool,
        bool,
    ) -> bool,
>(
    left: &PrimitiveArray<T>,
    right: &PrimitiveArray<T>,
    mut op: F,
) -> Result<BooleanArray>
where
    T: ArrowPrimitiveType,
{
    let left_values = left.values();
    let right_values = right.values();
    let left_nulls = left.nulls();
    let right_nulls = right.nulls();

    let array_len = left.len().min(right.len());
    let distinct = arrow_buffer::MutableBuffer::collect_bool(array_len, |i| {
        op(
            left_values[i],
            right_values[i],
            left_nulls.map(|x| x.is_null(i)).unwrap_or_default(),
            right_nulls.map(|x| x.is_null(i)).unwrap_or_default(),
        )
    });
    let array_data = ArrayData::builder(arrow_schema::DataType::Boolean)
        .len(array_len)
        .add_buffer(distinct.into());

    Ok(BooleanArray::from(unsafe { array_data.build_unchecked() }))
}

pub(crate) fn is_distinct_from_f32(
    left: &Float32Array,
    right: &Float32Array,
) -> Result<BooleanArray> {
    distinct(
        left,
        right,
        |left_value, right_value, left_isnull, right_isnull| {
            distinct_float!(left_value, right_value, left_isnull, right_isnull)
        },
    )
}

pub(crate) fn is_not_distinct_from_f32(
    left: &Float32Array,
    right: &Float32Array,
) -> Result<BooleanArray> {
    distinct(
        left,
        right,
        |left_value, right_value, left_isnull, right_isnull| {
            !(distinct_float!(left_value, right_value, left_isnull, right_isnull))
        },
    )
}

pub(crate) fn is_distinct_from_f64(
    left: &Float64Array,
    right: &Float64Array,
) -> Result<BooleanArray> {
    distinct(
        left,
        right,
        |left_value, right_value, left_isnull, right_isnull| {
            distinct_float!(left_value, right_value, left_isnull, right_isnull)
        },
    )
}

pub(crate) fn is_not_distinct_from_f64(
    left: &Float64Array,
    right: &Float64Array,
) -> Result<BooleanArray> {
    distinct(
        left,
        right,
        |left_value, right_value, left_isnull, right_isnull| {
            !(distinct_float!(left_value, right_value, left_isnull, right_isnull))
        },
    )
}

pub(crate) fn is_distinct_from_utf8<OffsetSize: OffsetSizeTrait>(
    left: &GenericStringArray<OffsetSize>,
    right: &GenericStringArray<OffsetSize>,
) -> Result<BooleanArray> {
    Ok(left
        .iter()
        .zip(right.iter())
        .map(|(x, y)| Some(x != y))
        .collect())
}

pub(crate) fn is_distinct_from_binary<OffsetSize: OffsetSizeTrait>(
    left: &GenericBinaryArray<OffsetSize>,
    right: &GenericBinaryArray<OffsetSize>,
) -> Result<BooleanArray> {
    Ok(left
        .iter()
        .zip(right.iter())
        .map(|(x, y)| Some(x != y))
        .collect())
}

pub(crate) fn is_distinct_from_null(
    left: &NullArray,
    _right: &NullArray,
) -> Result<BooleanArray> {
    let length = left.len();
    make_boolean_array(length, false)
}

pub(crate) fn is_not_distinct_from_null(
    left: &NullArray,
    _right: &NullArray,
) -> Result<BooleanArray> {
    let length = left.len();
    make_boolean_array(length, true)
}

fn make_boolean_array(length: usize, value: bool) -> Result<BooleanArray> {
    Ok((0..length).map(|_| Some(value)).collect())
}

pub(crate) fn is_not_distinct_from_utf8<OffsetSize: OffsetSizeTrait>(
    left: &GenericStringArray<OffsetSize>,
    right: &GenericStringArray<OffsetSize>,
) -> Result<BooleanArray> {
    Ok(left
        .iter()
        .zip(right.iter())
        .map(|(x, y)| Some(x == y))
        .collect())
}

pub(crate) fn is_not_distinct_from_binary<OffsetSize: OffsetSizeTrait>(
    left: &GenericBinaryArray<OffsetSize>,
    right: &GenericBinaryArray<OffsetSize>,
) -> Result<BooleanArray> {
    Ok(left
        .iter()
        .zip(right.iter())
        .map(|(x, y)| Some(x == y))
        .collect())
}

pub(crate) fn is_distinct_from_decimal(
    left: &Decimal128Array,
    right: &Decimal128Array,
) -> Result<BooleanArray> {
    Ok(left
        .iter()
        .zip(right.iter())
        .map(|(left, right)| match (left, right) {
            (None, None) => Some(false),
            (None, Some(_)) | (Some(_), None) => Some(true),
            (Some(left), Some(right)) => Some(left != right),
        })
        .collect())
}

pub(crate) fn is_not_distinct_from_decimal(
    left: &Decimal128Array,
    right: &Decimal128Array,
) -> Result<BooleanArray> {
    Ok(left
        .iter()
        .zip(right.iter())
        .map(|(left, right)| match (left, right) {
            (None, None) => Some(true),
            (None, Some(_)) | (Some(_), None) => Some(false),
            (Some(left), Some(right)) => Some(left == right),
        })
        .collect())
}

pub(crate) fn add_dyn_decimal(
    left: &dyn Array,
    right: &dyn Array,
    result_type: &DataType,
) -> Result<ArrayRef> {
    let (precision, scale) = get_precision_scale(result_type)?;
    let array = add_dyn(left, right)?;
    decimal_array_with_precision_scale(array, precision, scale)
}

pub(crate) fn add_decimal_dyn_scalar(
    left: &dyn Array,
    right: i128,
    result_type: &DataType,
) -> Result<ArrayRef> {
    let (precision, scale) = get_precision_scale(result_type)?;

    let array = add_scalar_dyn::<Decimal128Type>(left, right)?;
    decimal_array_with_precision_scale(array, precision, scale)
}

pub(crate) fn add_dyn_temporal(left: &ArrayRef, right: &ArrayRef) -> Result<ArrayRef> {
    match (left.data_type(), right.data_type()) {
        (DataType::Timestamp(..), DataType::Timestamp(..)) => ts_array_op(left, right),
        (DataType::Interval(..), DataType::Interval(..)) => {
            interval_array_op(left, right, 1)
        }
        (DataType::Timestamp(..), DataType::Interval(..)) => {
            ts_interval_array_op(left, 1, right)
        }
        (DataType::Interval(..), DataType::Timestamp(..)) => {
            ts_interval_array_op(right, 1, left)
        }
        _ => {
            // fall back to kernels in arrow-rs
            Ok(add_dyn(left, right)?)
        }
    }
}

pub(crate) fn add_dyn_temporal_right_scalar(
    left: &ArrayRef,
    right: &ScalarValue,
) -> Result<ArrayRef> {
    match (left.data_type(), right.get_datatype()) {
        // Date32 + Interval
        (DataType::Date32, DataType::Interval(..)) => {
            let left = as_date32_array(&left)?;
            let ret = Arc::new(try_unary::<Date32Type, _, Date32Type>(left, |days| {
                Ok(date32_op(days, right, 1)?)
            })?) as _;
            Ok(ret)
        }
        // Date64 + Interval
        (DataType::Date64, DataType::Interval(..)) => {
            let left = as_date64_array(&left)?;
            let ret = Arc::new(try_unary::<Date64Type, _, Date64Type>(left, |ms| {
                Ok(date64_op(ms, right, 1)?)
            })?) as _;
            Ok(ret)
        }
        // Interval + Interval
        (DataType::Interval(..), DataType::Interval(..)) => {
            interval_op_scalar_interval(left, 1, right)
        }
        // Timestamp + Interval
        (DataType::Timestamp(..), DataType::Interval(..)) => {
            ts_op_scalar_interval(left, 1, right)
        }
        _ => {
            // fall back to kernels in arrow-rs
            Ok(add_dyn(left, &right.to_array())?)
        }
    }
}

pub(crate) fn add_dyn_temporal_left_scalar(
    left: &ScalarValue,
    right: &ArrayRef,
) -> Result<ArrayRef> {
    match (left.get_datatype(), right.data_type()) {
        // Date32 + Interval
        (DataType::Date32, DataType::Interval(..)) => {
            if let ScalarValue::Date32(Some(left)) = left {
                scalar_date32_array_interval_op(
                    *left,
                    right,
                    NaiveDate::checked_add_days,
                    NaiveDate::checked_add_months,
                )
            } else {
                Err(DataFusionError::Internal(
                    "Date32 value is None".to_string(),
                ))
            }
        }
        // Date64 + Interval
        (DataType::Date64, DataType::Interval(..)) => {
            if let ScalarValue::Date64(Some(left)) = left {
                scalar_date64_array_interval_op(
                    *left,
                    right,
                    NaiveDate::checked_add_days,
                    NaiveDate::checked_add_months,
                )
            } else {
                Err(DataFusionError::Internal(
                    "Date64 value is None".to_string(),
                ))
            }
        }
        // Interval + Interval
        (DataType::Interval(..), DataType::Interval(..)) => {
            scalar_interval_op_interval(left, 1, right)
        }
        // Timestamp + Interval
        (DataType::Timestamp(..), DataType::Interval(..)) => {
            scalar_ts_op_interval(left, 1, right)
        }
        _ => {
            // fall back to kernels in arrow-rs
            Ok(add_dyn(&left.to_array(), right)?)
        }
    }
}

pub(crate) fn subtract_decimal_dyn_scalar(
    left: &dyn Array,
    right: i128,
    result_type: &DataType,
) -> Result<ArrayRef> {
    let (precision, scale) = get_precision_scale(result_type)?;

    let array = subtract_scalar_dyn::<Decimal128Type>(left, right)?;
    decimal_array_with_precision_scale(array, precision, scale)
}

pub(crate) fn subtract_dyn_temporal(
    left: &ArrayRef,
    right: &ArrayRef,
) -> Result<ArrayRef> {
    match (left.data_type(), right.data_type()) {
        (DataType::Timestamp(..), DataType::Timestamp(..)) => ts_array_op(left, right),
        (DataType::Interval(..), DataType::Interval(..)) => {
            interval_array_op(left, right, -1)
        }
        (DataType::Timestamp(..), DataType::Interval(..)) => {
            ts_interval_array_op(left, -1, right)
        }
        (DataType::Interval(..), DataType::Timestamp(..)) => {
            ts_interval_array_op(right, -1, left)
        }
        _ => {
            // fall back to kernels in arrow-rs
            Ok(subtract_dyn(left, right)?)
        }
    }
}

pub(crate) fn subtract_dyn_temporal_right_scalar(
    left: &ArrayRef,
    right: &ScalarValue,
) -> Result<ArrayRef> {
    match (left.data_type(), right.get_datatype()) {
        // Date32 - Interval
        (DataType::Date32, DataType::Interval(..)) => {
            let left = as_date32_array(&left)?;
            let ret = Arc::new(try_unary::<Date32Type, _, Date32Type>(left, |days| {
                Ok(date32_op(days, right, -1)?)
            })?) as _;
            Ok(ret)
        }
        // Date64 - Interval
        (DataType::Date64, DataType::Interval(..)) => {
            let left = as_date64_array(&left)?;
            let ret = Arc::new(try_unary::<Date64Type, _, Date64Type>(left, |ms| {
                Ok(date64_op(ms, right, -1)?)
            })?) as _;
            Ok(ret)
        }
        // Timestamp - Timestamp
        (DataType::Timestamp(..), DataType::Timestamp(..)) => {
            ts_sub_scalar_ts(left, right)
        }
        // Interval - Interval
        (DataType::Interval(..), DataType::Interval(..)) => {
            interval_op_scalar_interval(left, -1, right)
        }
        // Timestamp - Interval
        (DataType::Timestamp(..), DataType::Interval(..)) => {
            ts_op_scalar_interval(left, -1, right)
        }
        _ => {
            // fall back to kernels in arrow-rs
            Ok(subtract_dyn(left, &right.to_array())?)
        }
    }
}

pub(crate) fn subtract_dyn_temporal_left_scalar(
    left: &ScalarValue,
    right: &ArrayRef,
) -> Result<ArrayRef> {
    match (left.get_datatype(), right.data_type()) {
        // Date32 - Interval
        (DataType::Date32, DataType::Interval(..)) => {
            if let ScalarValue::Date32(Some(left)) = left {
                scalar_date32_array_interval_op(
                    *left,
                    right,
                    NaiveDate::checked_sub_days,
                    NaiveDate::checked_sub_months,
                )
            } else {
                Err(DataFusionError::Internal(
                    "Date32 value is None".to_string(),
                ))
            }
        }
        // Date64 - Interval
        (DataType::Date64, DataType::Interval(..)) => {
            if let ScalarValue::Date64(Some(left)) = left {
                scalar_date64_array_interval_op(
                    *left,
                    right,
                    NaiveDate::checked_sub_days,
                    NaiveDate::checked_sub_months,
                )
            } else {
                Err(DataFusionError::Internal(
                    "Date64 value is None".to_string(),
                ))
            }
        }
        // Timestamp - Timestamp
        (DataType::Timestamp(..), DataType::Timestamp(..)) => {
            scalar_ts_sub_ts(left, right)
        }
        // Interval - Interval
        (DataType::Interval(..), DataType::Interval(..)) => {
            scalar_interval_op_interval(left, -1, right)
        }
        // Timestamp - Interval
        (DataType::Timestamp(..), DataType::Interval(..)) => {
            scalar_ts_op_interval(left, -1, right)
        }
        _ => {
            // fall back to kernels in arrow-rs
            Ok(subtract_dyn(&left.to_array(), right)?)
        }
    }
}

fn scalar_date32_array_interval_op(
    left: i32,
    right: &ArrayRef,
    day_op: fn(NaiveDate, Days) -> Option<NaiveDate>,
    month_op: fn(NaiveDate, Months) -> Option<NaiveDate>,
) -> Result<ArrayRef> {
    let epoch = NaiveDate::from_ymd_opt(1970, 1, 1)
        .ok_or_else(|| DataFusionError::Execution("Invalid Date entered".to_string()))?;
    let prior = epoch.add(Duration::days(left as i64));
    match right.data_type() {
        DataType::Interval(IntervalUnit::YearMonth) => {
            date32_interval_ym_op(right, &epoch, &prior, month_op)
        }
        DataType::Interval(IntervalUnit::DayTime) => {
            date32_interval_dt_op(right, &epoch, &prior, day_op)
        }
        DataType::Interval(IntervalUnit::MonthDayNano) => {
            date32_interval_mdn_op(right, &epoch, &prior, day_op, month_op)
        }
        _ => Err(DataFusionError::Internal(format!(
            "Expected type is an interval, but {} is found",
            right.data_type()
        ))),
    }
}

fn scalar_date64_array_interval_op(
    left: i64,
    right: &ArrayRef,
    day_op: fn(NaiveDate, Days) -> Option<NaiveDate>,
    month_op: fn(NaiveDate, Months) -> Option<NaiveDate>,
) -> Result<ArrayRef> {
    let epoch = NaiveDate::from_ymd_opt(1970, 1, 1)
        .ok_or_else(|| DataFusionError::Execution("Invalid Date entered".to_string()))?;
    let prior = epoch.add(Duration::milliseconds(left));
    match right.data_type() {
        DataType::Interval(IntervalUnit::YearMonth) => {
            date64_interval_ym_op(right, &epoch, &prior, month_op)
        }
        DataType::Interval(IntervalUnit::DayTime) => {
            date64_interval_dt_op(right, &epoch, &prior, day_op)
        }
        DataType::Interval(IntervalUnit::MonthDayNano) => {
            date64_interval_mdn_op(right, &epoch, &prior, day_op, month_op)
        }
        _ => Err(DataFusionError::Internal(format!(
            "Expected type is an interval, but {} is found",
            right.data_type()
        ))),
    }
}

fn get_precision_scale(data_type: &DataType) -> Result<(u8, i8)> {
    match data_type {
        DataType::Decimal128(precision, scale) => Ok((*precision, *scale)),
        DataType::Dictionary(_, value_type) => match value_type.as_ref() {
            DataType::Decimal128(precision, scale) => Ok((*precision, *scale)),
            _ => Err(DataFusionError::Internal(
                "Unexpected data type".to_string(),
            )),
        },
        _ => Err(DataFusionError::Internal(
            "Unexpected data type".to_string(),
        )),
    }
}

fn decimal_array_with_precision_scale(
    array: ArrayRef,
    precision: u8,
    scale: i8,
) -> Result<ArrayRef> {
    let array = array.as_ref();
    let decimal_array = match array.data_type() {
        DataType::Decimal128(_, _) => {
            let array = as_decimal128_array(array)?;
            Arc::new(array.clone().with_precision_and_scale(precision, scale)?)
                as ArrayRef
        }
        DataType::Dictionary(_, _) => {
            downcast_dictionary_array!(
                array => match array.values().data_type() {
                    DataType::Decimal128(_, _) => {
                        let decimal_dict_array = array.downcast_dict::<Decimal128Array>().unwrap();
                        let decimal_array = decimal_dict_array.values().clone();
                        let decimal_array = decimal_array.with_precision_and_scale(precision, scale)?;
                        Arc::new(array.with_values(&decimal_array)) as ArrayRef
                    }
                    t => return Err(DataFusionError::Internal(format!("Unexpected dictionary value type {t}"))),
                },
                t => return Err(DataFusionError::Internal(format!("Unexpected datatype {t}"))),
            )
        }
        _ => {
            return Err(DataFusionError::Internal(
                "Unexpected data type".to_string(),
            ))
        }
    };
    Ok(decimal_array)
}

pub(crate) fn multiply_decimal_dyn_scalar(
    left: &dyn Array,
    right: i128,
    result_type: &DataType,
) -> Result<ArrayRef> {
    let (precision, scale) = get_precision_scale(result_type)?;
    let array = multiply_scalar_dyn::<Decimal128Type>(left, right)?;
    decimal_array_with_precision_scale(array, precision, scale)
}

pub(crate) fn divide_decimal_dyn_scalar(
    left: &dyn Array,
    right: i128,
    result_type: &DataType,
) -> Result<ArrayRef> {
    let (precision, scale) = get_precision_scale(result_type)?;

    let mul = 10_i128.pow(scale as u32);
    let array = multiply_scalar_dyn::<Decimal128Type>(left, mul)?;

    let array = divide_scalar_dyn::<Decimal128Type>(&array, right)?;
    decimal_array_with_precision_scale(array, precision, scale)
}

pub(crate) fn subtract_dyn_decimal(
    left: &dyn Array,
    right: &dyn Array,
    result_type: &DataType,
) -> Result<ArrayRef> {
    let (precision, scale) = get_precision_scale(result_type)?;
    let array = subtract_dyn(left, right)?;
    decimal_array_with_precision_scale(array, precision, scale)
}

pub(crate) fn multiply_dyn_decimal(
    left: &dyn Array,
    right: &dyn Array,
    result_type: &DataType,
) -> Result<ArrayRef> {
    let (precision, scale) = get_precision_scale(result_type)?;
    let array = multiply_dyn(left, right)?;
    decimal_array_with_precision_scale(array, precision, scale)
}

pub(crate) fn divide_dyn_checked_decimal(
    left: &dyn Array,
    right: &dyn Array,
    result_type: &DataType,
) -> Result<ArrayRef> {
    let (precision, scale) = get_precision_scale(result_type)?;

    let mul = 10_i128.pow(scale as u32);
    let array = multiply_scalar_dyn::<Decimal128Type>(left, mul)?;

    // Restore to original precision and scale (metadata only)
    let (org_precision, org_scale) = get_precision_scale(right.data_type())?;
    let array = decimal_array_with_precision_scale(array, org_precision, org_scale)?;
    let array = divide_dyn_checked(&array, right)?;
    decimal_array_with_precision_scale(array, precision, scale)
}

pub(crate) fn modulus_dyn_decimal(
    left: &dyn Array,
    right: &dyn Array,
    result_type: &DataType,
) -> Result<ArrayRef> {
    let (precision, scale) = get_precision_scale(result_type)?;
    let array = modulus_dyn(left, right)?;
    decimal_array_with_precision_scale(array, precision, scale)
}

pub(crate) fn modulus_decimal_dyn_scalar(
    left: &dyn Array,
    right: i128,
    result_type: &DataType,
) -> Result<ArrayRef> {
    let (precision, scale) = get_precision_scale(result_type)?;

    let array = modulus_scalar_dyn::<Decimal128Type>(left, right)?;
    decimal_array_with_precision_scale(array, precision, scale)
}

macro_rules! sub_timestamp_macro {
    ($array:expr, $rhs:expr, $caster:expr, $interval_type:ty, $opt_tz_lhs:expr, $multiplier:expr,
        $opt_tz_rhs:expr, $unit_sub:expr, $naive_sub_fn:expr, $counter:expr) => {{
        let prim_array = $caster(&$array)?;
        let ret: PrimitiveArray<$interval_type> = try_unary(prim_array, |lhs| {
            let (parsed_lhs_tz, parsed_rhs_tz) =
                (parse_timezones($opt_tz_lhs)?, parse_timezones($opt_tz_rhs)?);
            let (naive_lhs, naive_rhs) = calculate_naives::<$unit_sub>(
                lhs.mul_wrapping($multiplier),
                parsed_lhs_tz,
                $rhs.mul_wrapping($multiplier),
                parsed_rhs_tz,
            )?;
            Ok($naive_sub_fn($counter(&naive_lhs), $counter(&naive_rhs)))
        })?;
        Arc::new(ret) as _
    }};
}

macro_rules! sub_timestamp_left_scalar_macro {
    ($array:expr, $lhs:expr, $caster:expr, $interval_type:ty, $opt_tz_lhs:expr, $multiplier:expr,
        $opt_tz_rhs:expr, $unit_sub:expr, $naive_sub_fn:expr, $counter:expr) => {{
        let prim_array = $caster(&$array)?;
        let ret: PrimitiveArray<$interval_type> = try_unary(prim_array, |rhs| {
            let (parsed_lhs_tz, parsed_rhs_tz) =
                (parse_timezones($opt_tz_lhs)?, parse_timezones($opt_tz_rhs)?);
            let (naive_lhs, naive_rhs) = calculate_naives::<$unit_sub>(
                $lhs.mul_wrapping($multiplier),
                parsed_lhs_tz,
                rhs.mul_wrapping($multiplier),
                parsed_rhs_tz,
            )?;
            Ok($naive_sub_fn($counter(&naive_lhs), $counter(&naive_rhs)))
        })?;
        Arc::new(ret) as _
    }};
}

macro_rules! op_timestamp_interval_macro {
    ($array:expr, $as_timestamp:expr, $ts_type:ty, $fn_op:expr, $scalar:expr, $sign:expr, $tz:expr) => {{
        let array = $as_timestamp(&$array)?;
        let ret: PrimitiveArray<$ts_type> =
            try_unary::<$ts_type, _, $ts_type>(array, |ts_s| {
                Ok($fn_op(ts_s, $scalar, $sign)?)
            })?;
        Arc::new(ret.with_timezone_opt($tz.clone())) as _
    }};
}

macro_rules! scalar_ts_op_interval_macro {
    ($ts:ident, $tz:ident, $interval:ident, $sign:ident,
        $caster1:expr, $type1:ty, $type2:ty, $op:expr, $back_caster:expr) => {{
        let interval = $caster1(&$interval)?;
        let ret: PrimitiveArray<$type1> =
            try_unary::<$type2, _, $type1>(interval, |e| {
                let prior = $ts.ok_or_else(|| {
                    DataFusionError::Internal("Timestamp is out-of-range".to_string())
                })?;
                Ok($back_caster(&$op(prior, e, $sign)))
            })?;
        Arc::new(ret.with_timezone_opt($tz.clone())) as _
    }};
}

macro_rules! op_interval_macro {
    ($array:expr, $as_interval:expr, $interval_type:ty, $fn_op:expr, $scalar:expr, $sign:expr) => {{
        let array = $as_interval(&$array)?;
        let ret: PrimitiveArray<$interval_type> =
            unary(array, |lhs| $fn_op(lhs, *$scalar, $sign));
        Arc::new(ret) as _
    }};
}

macro_rules! op_interval_cross_macro {
    ($array:expr, $as_interval:expr, $commute:expr, $fn_op:expr, $scalar:expr, $sign:expr, $t1:ty, $t2:ty) => {{
        let array = $as_interval(&$array)?;
        let ret: PrimitiveArray<IntervalMonthDayNanoType> = if $commute {
            unary(array, |lhs| {
                $fn_op(*$scalar as $t1, lhs as $t2, $sign, $commute)
            })
        } else {
            unary(array, |lhs| {
                $fn_op(lhs as $t1, *$scalar as $t2, $sign, $commute)
            })
        };
        Arc::new(ret) as _
    }};
}

macro_rules! ts_sub_op {
    ($lhs:ident, $rhs:ident, $lhs_tz:ident, $rhs_tz:ident, $coef:expr, $caster:expr, $op:expr, $ts_unit:expr, $mode:expr, $type_out:ty) => {{
        let prim_array_lhs = $caster(&$lhs)?;
        let prim_array_rhs = $caster(&$rhs)?;
        let ret: PrimitiveArray<$type_out> =
            arrow::compute::try_binary(prim_array_lhs, prim_array_rhs, |ts1, ts2| {
                let (parsed_lhs_tz, parsed_rhs_tz) = (
                    parse_timezones($lhs_tz.as_deref())?,
                    parse_timezones($rhs_tz.as_deref())?,
                );
                let (naive_lhs, naive_rhs) = calculate_naives::<$mode>(
                    ts1.mul_wrapping($coef),
                    parsed_lhs_tz,
                    ts2.mul_wrapping($coef),
                    parsed_rhs_tz,
                )?;
                Ok($op($ts_unit(&naive_lhs), $ts_unit(&naive_rhs)))
            })?;
        Arc::new(ret) as _
    }};
}

macro_rules! interval_op {
    ($lhs:ident, $rhs:ident, $caster:expr, $op:expr, $sign:ident, $type_in:ty) => {{
        let prim_array_lhs = $caster(&$lhs)?;
        let prim_array_rhs = $caster(&$rhs)?;
        Arc::new(arrow::compute::binary::<$type_in, $type_in, _, $type_in>(
            prim_array_lhs,
            prim_array_rhs,
            |interval1, interval2| $op(interval1, interval2, $sign),
        )?) as _
    }};
}

macro_rules! interval_cross_op {
    ($lhs:ident, $rhs:ident, $caster1:expr, $caster2:expr, $op:expr, $sign:ident, $commute:ident, $type_in1:ty, $type_in2:ty) => {{
        let prim_array_lhs = $caster1(&$lhs)?;
        let prim_array_rhs = $caster2(&$rhs)?;
        Arc::new(arrow::compute::binary::<
            $type_in1,
            $type_in2,
            _,
            IntervalMonthDayNanoType,
        >(
            prim_array_lhs,
            prim_array_rhs,
            |interval1, interval2| $op(interval1, interval2, $sign, $commute),
        )?) as _
    }};
}

macro_rules! ts_interval_op {
    ($lhs:ident, $rhs:ident, $tz:ident, $caster1:expr, $caster2:expr, $op:expr, $sign:ident, $type_in1:ty, $type_in2:ty) => {{
        let prim_array_lhs = $caster1(&$lhs)?;
        let prim_array_rhs = $caster2(&$rhs)?;
        let ret: PrimitiveArray<$type_in1> = arrow::compute::try_binary(
            prim_array_lhs,
            prim_array_rhs,
            |ts, interval| Ok($op(ts, interval as i128, $sign)?),
        )?;
        Arc::new(ret.with_timezone_opt($tz.clone())) as _
    }};
}

/// This function handles timestamp - timestamp operations where the former is
/// an array and the latter is a scalar, resulting in an array.
pub fn ts_sub_scalar_ts(array: &ArrayRef, scalar: &ScalarValue) -> Result<ArrayRef> {
    let ret = match (array.data_type(), scalar) {
        (
            DataType::Timestamp(TimeUnit::Second, opt_tz_lhs),
            ScalarValue::TimestampSecond(Some(rhs), opt_tz_rhs),
        ) => {
            sub_timestamp_macro!(
                array,
                rhs,
                as_timestamp_second_array,
                IntervalDayTimeType,
                opt_tz_lhs.as_deref(),
                1000,
                opt_tz_rhs.as_deref(),
                MILLISECOND_MODE,
                seconds_sub,
                NaiveDateTime::timestamp
            )
        }
        (
            DataType::Timestamp(TimeUnit::Millisecond, opt_tz_lhs),
            ScalarValue::TimestampMillisecond(Some(rhs), opt_tz_rhs),
        ) => {
            sub_timestamp_macro!(
                array,
                rhs,
                as_timestamp_millisecond_array,
                IntervalDayTimeType,
                opt_tz_lhs.as_deref(),
                1,
                opt_tz_rhs.as_deref(),
                MILLISECOND_MODE,
                milliseconds_sub,
                NaiveDateTime::timestamp_millis
            )
        }
        (
            DataType::Timestamp(TimeUnit::Microsecond, opt_tz_lhs),
            ScalarValue::TimestampMicrosecond(Some(rhs), opt_tz_rhs),
        ) => {
            sub_timestamp_macro!(
                array,
                rhs,
                as_timestamp_microsecond_array,
                IntervalMonthDayNanoType,
                opt_tz_lhs.as_deref(),
                1000,
                opt_tz_rhs.as_deref(),
                NANOSECOND_MODE,
                microseconds_sub,
                NaiveDateTime::timestamp_micros
            )
        }
        (
            DataType::Timestamp(TimeUnit::Nanosecond, opt_tz_lhs),
            ScalarValue::TimestampNanosecond(Some(rhs), opt_tz_rhs),
        ) => {
            sub_timestamp_macro!(
                array,
                rhs,
                as_timestamp_nanosecond_array,
                IntervalMonthDayNanoType,
                opt_tz_lhs.as_deref(),
                1,
                opt_tz_rhs.as_deref(),
                NANOSECOND_MODE,
                nanoseconds_sub,
                NaiveDateTime::timestamp_nanos
            )
        }
        (_, _) => {
            return Err(DataFusionError::Internal(format!(
                "Invalid array - scalar types for Timestamp subtraction: {:?} - {:?}",
                array.data_type(),
                scalar.get_datatype()
            )));
        }
    };
    Ok(ret)
}

/// This function handles timestamp - timestamp operations where the former is
/// a scalar and the latter is an array, resulting in an array.
pub fn scalar_ts_sub_ts(scalar: &ScalarValue, array: &ArrayRef) -> Result<ArrayRef> {
    let ret = match (scalar, array.data_type()) {
        (
            ScalarValue::TimestampSecond(Some(lhs), opt_tz_lhs),
            DataType::Timestamp(TimeUnit::Second, opt_tz_rhs),
        ) => {
            sub_timestamp_left_scalar_macro!(
                array,
                lhs,
                as_timestamp_second_array,
                IntervalDayTimeType,
                opt_tz_lhs.as_deref(),
                1000,
                opt_tz_rhs.as_deref(),
                MILLISECOND_MODE,
                seconds_sub,
                NaiveDateTime::timestamp
            )
        }
        (
            ScalarValue::TimestampMillisecond(Some(lhs), opt_tz_lhs),
            DataType::Timestamp(TimeUnit::Millisecond, opt_tz_rhs),
        ) => {
            sub_timestamp_left_scalar_macro!(
                array,
                lhs,
                as_timestamp_millisecond_array,
                IntervalDayTimeType,
                opt_tz_lhs.as_deref(),
                1,
                opt_tz_rhs.as_deref(),
                MILLISECOND_MODE,
                milliseconds_sub,
                NaiveDateTime::timestamp_millis
            )
        }
        (
            ScalarValue::TimestampMicrosecond(Some(lhs), opt_tz_lhs),
            DataType::Timestamp(TimeUnit::Microsecond, opt_tz_rhs),
        ) => {
            sub_timestamp_left_scalar_macro!(
                array,
                lhs,
                as_timestamp_microsecond_array,
                IntervalMonthDayNanoType,
                opt_tz_lhs.as_deref(),
                1000,
                opt_tz_rhs.as_deref(),
                NANOSECOND_MODE,
                microseconds_sub,
                NaiveDateTime::timestamp_micros
            )
        }
        (
            ScalarValue::TimestampNanosecond(Some(lhs), opt_tz_lhs),
            DataType::Timestamp(TimeUnit::Nanosecond, opt_tz_rhs),
        ) => {
            sub_timestamp_left_scalar_macro!(
                array,
                lhs,
                as_timestamp_nanosecond_array,
                IntervalMonthDayNanoType,
                opt_tz_lhs.as_deref(),
                1,
                opt_tz_rhs.as_deref(),
                NANOSECOND_MODE,
                nanoseconds_sub,
                NaiveDateTime::timestamp_nanos
            )
        }
        (_, _) => {
            return Err(DataFusionError::Internal(format!(
                "Invalid scalar - array types for Timestamp subtraction: {:?} - {:?}",
                scalar.get_datatype(),
                array.data_type()
            )));
        }
    };
    Ok(ret)
}

/// This function handles timestamp +/- interval operations where the former is
/// an array and the latter is a scalar, resulting in an array.
pub fn ts_op_scalar_interval(
    array: &ArrayRef,
    sign: i32,
    scalar: &ScalarValue,
) -> Result<ArrayRef> {
    let ret = match array.data_type() {
        DataType::Timestamp(TimeUnit::Second, tz) => {
            op_timestamp_interval_macro!(
                array,
                as_timestamp_second_array,
                TimestampSecondType,
                seconds_add,
                scalar,
                sign,
                tz
            )
        }
        DataType::Timestamp(TimeUnit::Millisecond, tz) => {
            op_timestamp_interval_macro!(
                array,
                as_timestamp_millisecond_array,
                TimestampMillisecondType,
                milliseconds_add,
                scalar,
                sign,
                tz
            )
        }
        DataType::Timestamp(TimeUnit::Microsecond, tz) => {
            op_timestamp_interval_macro!(
                array,
                as_timestamp_microsecond_array,
                TimestampMicrosecondType,
                microseconds_add,
                scalar,
                sign,
                tz
            )
        }
        DataType::Timestamp(TimeUnit::Nanosecond, tz) => {
            op_timestamp_interval_macro!(
                array,
                as_timestamp_nanosecond_array,
                TimestampNanosecondType,
                nanoseconds_add,
                scalar,
                sign,
                tz
            )
        }
        _ => Err(DataFusionError::Internal(format!(
            "Invalid lhs type for Timestamp vs Interval operations: {}",
            array.data_type()
        )))?,
    };
    Ok(ret)
}

/// This function handles timestamp +/- interval operations where the former is
/// a scalar and the latter is an array, resulting in an array.
pub fn scalar_ts_op_interval(
    scalar: &ScalarValue,
    sign: i32,
    array: &ArrayRef,
) -> Result<ArrayRef> {
    use DataType::*;
    use IntervalUnit::*;
    use ScalarValue::*;
    let ret = match (scalar, array.data_type()) {
        // Second op YearMonth
        (TimestampSecond(Some(ts_sec), tz), Interval(YearMonth)) => {
            let naive_date = NaiveDateTime::from_timestamp_opt(*ts_sec, 0);
            scalar_ts_op_interval_macro!(
                naive_date,
                tz,
                array,
                sign,
                as_interval_ym_array,
                TimestampSecondType,
                IntervalYearMonthType,
                shift_months,
                NaiveDateTime::timestamp
            )
        }
        // Millisecond op YearMonth
        (TimestampMillisecond(Some(ts_ms), tz), Interval(YearMonth)) => {
            let naive_date = NaiveDateTime::from_timestamp_millis(*ts_ms);
            scalar_ts_op_interval_macro!(
                naive_date,
                tz,
                array,
                sign,
                as_interval_ym_array,
                TimestampSecondType,
                IntervalYearMonthType,
                shift_months,
                NaiveDateTime::timestamp
            )
        }
        // Microsecond op YearMonth
        (TimestampMicrosecond(Some(ts_us), tz), Interval(YearMonth)) => {
            let naive_date = NaiveDateTime::from_timestamp_micros(*ts_us);
            scalar_ts_op_interval_macro!(
                naive_date,
                tz,
                array,
                sign,
                as_interval_ym_array,
                TimestampSecondType,
                IntervalYearMonthType,
                shift_months,
                NaiveDateTime::timestamp
            )
        }
        // Nanosecond op YearMonth
        (TimestampNanosecond(Some(ts_ns), tz), Interval(YearMonth)) => {
            let naive_date = NaiveDateTime::from_timestamp_opt(
                ts_ns.div_euclid(1_000_000_000),
                ts_ns.rem_euclid(1_000_000_000).try_into().map_err(|_| {
                    DataFusionError::Internal("Overflow of divison".to_string())
                })?,
            );
            scalar_ts_op_interval_macro!(
                naive_date,
                tz,
                array,
                sign,
                as_interval_ym_array,
                TimestampSecondType,
                IntervalYearMonthType,
                shift_months,
                NaiveDateTime::timestamp
            )
        }
        // Second op DayTime
        (TimestampSecond(Some(ts_sec), tz), Interval(DayTime)) => {
            let naive_date = NaiveDateTime::from_timestamp_opt(*ts_sec, 0);
            scalar_ts_op_interval_macro!(
                naive_date,
                tz,
                array,
                sign,
                as_interval_dt_array,
                TimestampSecondType,
                IntervalDayTimeType,
                add_day_time,
                NaiveDateTime::timestamp
            )
        }
        // Millisecond op DayTime
        (TimestampMillisecond(Some(ts_ms), tz), Interval(DayTime)) => {
            let naive_date = NaiveDateTime::from_timestamp_millis(*ts_ms);
            scalar_ts_op_interval_macro!(
                naive_date,
                tz,
                array,
                sign,
                as_interval_dt_array,
                TimestampMillisecondType,
                IntervalDayTimeType,
                add_day_time,
                NaiveDateTime::timestamp_millis
            )
        }
        // Microsecond op DayTime
        (TimestampMicrosecond(Some(ts_us), tz), Interval(DayTime)) => {
            let naive_date = NaiveDateTime::from_timestamp_micros(*ts_us);
            scalar_ts_op_interval_macro!(
                naive_date,
                tz,
                array,
                sign,
                as_interval_dt_array,
                TimestampMicrosecondType,
                IntervalDayTimeType,
                add_day_time,
                NaiveDateTime::timestamp_micros
            )
        }
        // Nanosecond op DayTime
        (TimestampNanosecond(Some(ts_ns), tz), Interval(DayTime)) => {
            let naive_date = NaiveDateTime::from_timestamp_opt(
                ts_ns.div_euclid(1_000_000_000),
                ts_ns.rem_euclid(1_000_000_000).try_into().map_err(|_| {
                    DataFusionError::Internal("Overflow of divison".to_string())
                })?,
            );
            scalar_ts_op_interval_macro!(
                naive_date,
                tz,
                array,
                sign,
                as_interval_dt_array,
                TimestampNanosecondType,
                IntervalDayTimeType,
                add_day_time,
                NaiveDateTime::timestamp_nanos
            )
        }
        // Second op MonthDayNano
        (TimestampSecond(Some(ts_sec), tz), Interval(MonthDayNano)) => {
            let naive_date = NaiveDateTime::from_timestamp_opt(*ts_sec, 0);
            scalar_ts_op_interval_macro!(
                naive_date,
                tz,
                array,
                sign,
                as_interval_mdn_array,
                TimestampSecondType,
                IntervalMonthDayNanoType,
                add_m_d_nano,
                NaiveDateTime::timestamp
            )
        }
        // Millisecond op MonthDayNano
        (TimestampMillisecond(Some(ts_ms), tz), Interval(MonthDayNano)) => {
            let naive_date = NaiveDateTime::from_timestamp_millis(*ts_ms);
            scalar_ts_op_interval_macro!(
                naive_date,
                tz,
                array,
                sign,
                as_interval_mdn_array,
                TimestampMillisecondType,
                IntervalMonthDayNanoType,
                add_m_d_nano,
                NaiveDateTime::timestamp_millis
            )
        }
        // Microsecond op MonthDayNano
        (TimestampMicrosecond(Some(ts_us), tz), Interval(MonthDayNano)) => {
            let naive_date = NaiveDateTime::from_timestamp_micros(*ts_us);
            scalar_ts_op_interval_macro!(
                naive_date,
                tz,
                array,
                sign,
                as_interval_mdn_array,
                TimestampMicrosecondType,
                IntervalMonthDayNanoType,
                add_m_d_nano,
                NaiveDateTime::timestamp_micros
            )
        }

        // Nanosecond op MonthDayNano
        (TimestampNanosecond(Some(ts_ns), tz), Interval(MonthDayNano)) => {
            let naive_date = NaiveDateTime::from_timestamp_opt(
                ts_ns.div_euclid(1_000_000_000),
                ts_ns.rem_euclid(1_000_000_000).try_into().map_err(|_| {
                    DataFusionError::Internal("Overflow of divison".to_string())
                })?,
            );
            scalar_ts_op_interval_macro!(
                naive_date,
                tz,
                array,
                sign,
                as_interval_mdn_array,
                TimestampNanosecondType,
                IntervalMonthDayNanoType,
                add_m_d_nano,
                NaiveDateTime::timestamp_nanos
            )
        }
        _ => Err(DataFusionError::Internal(
            "Invalid types for Timestamp vs Interval operations".to_string(),
        ))?,
    };
    Ok(ret)
}

/// This function handles interval +/- interval operations where the former is
/// an array and the latter is a scalar, resulting in an interval array.
pub fn interval_op_scalar_interval(
    array: &ArrayRef,
    sign: i32,
    scalar: &ScalarValue,
) -> Result<ArrayRef> {
    use DataType::*;
    use IntervalUnit::*;
    use ScalarValue::*;
    let ret = match (array.data_type(), scalar) {
        (Interval(YearMonth), IntervalYearMonth(Some(rhs))) => {
            op_interval_macro!(
                array,
                as_interval_ym_array,
                IntervalYearMonthType,
                op_ym,
                rhs,
                sign
            )
        }
        (Interval(YearMonth), IntervalDayTime(Some(rhs))) => {
            op_interval_cross_macro!(
                array,
                as_interval_ym_array,
                false,
                op_ym_dt,
                rhs,
                sign,
                i32,
                i64
            )
        }
        (Interval(YearMonth), IntervalMonthDayNano(Some(rhs))) => {
            op_interval_cross_macro!(
                array,
                as_interval_ym_array,
                false,
                op_ym_mdn,
                rhs,
                sign,
                i32,
                i128
            )
        }
        (Interval(DayTime), IntervalYearMonth(Some(rhs))) => {
            op_interval_cross_macro!(
                array,
                as_interval_dt_array,
                true,
                op_ym_dt,
                rhs,
                sign,
                i32,
                i64
            )
        }
        (Interval(DayTime), IntervalDayTime(Some(rhs))) => {
            op_interval_macro!(
                array,
                as_interval_dt_array,
                IntervalDayTimeType,
                op_dt,
                rhs,
                sign
            )
        }
        (Interval(DayTime), IntervalMonthDayNano(Some(rhs))) => {
            op_interval_cross_macro!(
                array,
                as_interval_dt_array,
                false,
                op_dt_mdn,
                rhs,
                sign,
                i64,
                i128
            )
        }
        (Interval(MonthDayNano), IntervalYearMonth(Some(rhs))) => {
            op_interval_cross_macro!(
                array,
                as_interval_mdn_array,
                true,
                op_ym_mdn,
                rhs,
                sign,
                i32,
                i128
            )
        }
        (Interval(MonthDayNano), IntervalDayTime(Some(rhs))) => {
            op_interval_cross_macro!(
                array,
                as_interval_mdn_array,
                true,
                op_dt_mdn,
                rhs,
                sign,
                i64,
                i128
            )
        }
        (Interval(MonthDayNano), IntervalMonthDayNano(Some(rhs))) => {
            op_interval_macro!(
                array,
                as_interval_mdn_array,
                IntervalMonthDayNanoType,
                op_mdn,
                rhs,
                sign
            )
        }
        _ => Err(DataFusionError::Internal(format!(
            "Invalid operands for Interval vs Interval operations: {} - {}",
            array.data_type(),
            scalar.get_datatype(),
        )))?,
    };
    Ok(ret)
}

/// This function handles interval +/- interval operations where the former is
/// a scalar and the latter is an array, resulting in an interval array.
pub fn scalar_interval_op_interval(
    scalar: &ScalarValue,
    sign: i32,
    array: &ArrayRef,
) -> Result<ArrayRef> {
    use DataType::*;
    use IntervalUnit::*;
    use ScalarValue::*;
    let ret = match (scalar, array.data_type()) {
        // YearMonth op YearMonth
        (IntervalYearMonth(Some(lhs)), Interval(YearMonth)) => {
            let array = as_interval_ym_array(&array)?;
            let ret: PrimitiveArray<IntervalYearMonthType> =
                unary(array, |rhs| op_ym(*lhs, rhs, sign));
            Arc::new(ret) as _
        }
        // DayTime op YearMonth
        (IntervalDayTime(Some(lhs)), Interval(YearMonth)) => {
            let array = as_interval_ym_array(&array)?;
            let ret: PrimitiveArray<IntervalMonthDayNanoType> =
                unary(array, |rhs| op_ym_dt(rhs, *lhs, sign, true));
            Arc::new(ret) as _
        }
        // MonthDayNano op YearMonth
        (IntervalMonthDayNano(Some(lhs)), Interval(YearMonth)) => {
            let array = as_interval_ym_array(&array)?;
            let ret: PrimitiveArray<IntervalMonthDayNanoType> =
                unary(array, |rhs| op_ym_mdn(rhs, *lhs, sign, true));
            Arc::new(ret) as _
        }
        // YearMonth op DayTime
        (IntervalYearMonth(Some(lhs)), Interval(DayTime)) => {
            let array = as_interval_dt_array(&array)?;
            let ret: PrimitiveArray<IntervalMonthDayNanoType> =
                unary(array, |rhs| op_ym_dt(*lhs, rhs, sign, false));
            Arc::new(ret) as _
        }
        // DayTime op DayTime
        (IntervalDayTime(Some(lhs)), Interval(DayTime)) => {
            let array = as_interval_dt_array(&array)?;
            let ret: PrimitiveArray<IntervalDayTimeType> =
                unary(array, |rhs| op_dt(*lhs, rhs, sign));
            Arc::new(ret) as _
        }
        // MonthDayNano op DayTime
        (IntervalMonthDayNano(Some(lhs)), Interval(DayTime)) => {
            let array = as_interval_dt_array(&array)?;
            let ret: PrimitiveArray<IntervalMonthDayNanoType> =
                unary(array, |rhs| op_dt_mdn(rhs, *lhs, sign, true));
            Arc::new(ret) as _
        }
        // YearMonth op MonthDayNano
        (IntervalYearMonth(Some(lhs)), Interval(MonthDayNano)) => {
            let array = as_interval_mdn_array(&array)?;
            let ret: PrimitiveArray<IntervalMonthDayNanoType> =
                unary(array, |rhs| op_ym_mdn(*lhs, rhs, sign, false));
            Arc::new(ret) as _
        }
        // DayTime op MonthDayNano
        (IntervalDayTime(Some(lhs)), Interval(MonthDayNano)) => {
            let array = as_interval_mdn_array(&array)?;
            let ret: PrimitiveArray<IntervalMonthDayNanoType> =
                unary(array, |rhs| op_dt_mdn(*lhs, rhs, sign, false));
            Arc::new(ret) as _
        }
        // MonthDayNano op MonthDayNano
        (IntervalMonthDayNano(Some(lhs)), Interval(MonthDayNano)) => {
            let array = as_interval_mdn_array(&array)?;
            let ret: PrimitiveArray<IntervalMonthDayNanoType> =
                unary(array, |rhs| op_mdn(*lhs, rhs, sign));
            Arc::new(ret) as _
        }
        _ => Err(DataFusionError::Internal(format!(
            "Invalid operands for Interval vs Interval operations: {} - {}",
            scalar.get_datatype(),
            array.data_type(),
        )))?,
    };
    Ok(ret)
}

/// Performs a timestamp subtraction operation on two arrays and returns the resulting array.
pub fn ts_array_op(array_lhs: &ArrayRef, array_rhs: &ArrayRef) -> Result<ArrayRef> {
    use DataType::*;
    use TimeUnit::*;
    match (array_lhs.data_type(), array_rhs.data_type()) {
        (Timestamp(Second, opt_tz_lhs), Timestamp(Second, opt_tz_rhs)) => Ok(ts_sub_op!(
            array_lhs,
            array_rhs,
            opt_tz_lhs,
            opt_tz_rhs,
            1000i64,
            as_timestamp_second_array,
            seconds_sub,
            NaiveDateTime::timestamp,
            MILLISECOND_MODE,
            IntervalDayTimeType
        )),
        (Timestamp(Millisecond, opt_tz_lhs), Timestamp(Millisecond, opt_tz_rhs)) => {
            Ok(ts_sub_op!(
                array_lhs,
                array_rhs,
                opt_tz_lhs,
                opt_tz_rhs,
                1i64,
                as_timestamp_millisecond_array,
                milliseconds_sub,
                NaiveDateTime::timestamp_millis,
                MILLISECOND_MODE,
                IntervalDayTimeType
            ))
        }
        (Timestamp(Microsecond, opt_tz_lhs), Timestamp(Microsecond, opt_tz_rhs)) => {
            Ok(ts_sub_op!(
                array_lhs,
                array_rhs,
                opt_tz_lhs,
                opt_tz_rhs,
                1000i64,
                as_timestamp_microsecond_array,
                microseconds_sub,
                NaiveDateTime::timestamp_micros,
                NANOSECOND_MODE,
                IntervalMonthDayNanoType
            ))
        }
        (Timestamp(Nanosecond, opt_tz_lhs), Timestamp(Nanosecond, opt_tz_rhs)) => {
            Ok(ts_sub_op!(
                array_lhs,
                array_rhs,
                opt_tz_lhs,
                opt_tz_rhs,
                1i64,
                as_timestamp_nanosecond_array,
                nanoseconds_sub,
                NaiveDateTime::timestamp_nanos,
                NANOSECOND_MODE,
                IntervalMonthDayNanoType
            ))
        }
        (_, _) => Err(DataFusionError::Execution(format!(
            "Invalid array types for Timestamp subtraction: {} - {}",
            array_lhs.data_type(),
            array_rhs.data_type()
        ))),
    }
}
/// Performs an interval operation on two arrays and returns the resulting array.
/// The operation sign determines whether to perform addition or subtraction.
/// The data type and unit of the two input arrays must match the supported combinations.
pub fn interval_array_op(
    array_lhs: &ArrayRef,
    array_rhs: &ArrayRef,
    sign: i32,
) -> Result<ArrayRef> {
    use DataType::*;
    use IntervalUnit::*;
    match (array_lhs.data_type(), array_rhs.data_type()) {
        (Interval(YearMonth), Interval(YearMonth)) => Ok(interval_op!(
            array_lhs,
            array_rhs,
            as_interval_ym_array,
            op_ym,
            sign,
            IntervalYearMonthType
        )),
        (Interval(YearMonth), Interval(DayTime)) => Ok(interval_cross_op!(
            array_lhs,
            array_rhs,
            as_interval_ym_array,
            as_interval_dt_array,
            op_ym_dt,
            sign,
            false,
            IntervalYearMonthType,
            IntervalDayTimeType
        )),
        (Interval(YearMonth), Interval(MonthDayNano)) => Ok(interval_cross_op!(
            array_lhs,
            array_rhs,
            as_interval_ym_array,
            as_interval_mdn_array,
            op_ym_mdn,
            sign,
            false,
            IntervalYearMonthType,
            IntervalMonthDayNanoType
        )),
        (Interval(DayTime), Interval(YearMonth)) => Ok(interval_cross_op!(
            array_rhs,
            array_lhs,
            as_interval_ym_array,
            as_interval_dt_array,
            op_ym_dt,
            sign,
            true,
            IntervalYearMonthType,
            IntervalDayTimeType
        )),
        (Interval(DayTime), Interval(DayTime)) => Ok(interval_op!(
            array_lhs,
            array_rhs,
            as_interval_dt_array,
            op_dt,
            sign,
            IntervalDayTimeType
        )),
        (Interval(DayTime), Interval(MonthDayNano)) => Ok(interval_cross_op!(
            array_lhs,
            array_rhs,
            as_interval_dt_array,
            as_interval_mdn_array,
            op_dt_mdn,
            sign,
            false,
            IntervalDayTimeType,
            IntervalMonthDayNanoType
        )),
        (Interval(MonthDayNano), Interval(YearMonth)) => Ok(interval_cross_op!(
            array_rhs,
            array_lhs,
            as_interval_ym_array,
            as_interval_mdn_array,
            op_ym_mdn,
            sign,
            true,
            IntervalYearMonthType,
            IntervalMonthDayNanoType
        )),
        (Interval(MonthDayNano), Interval(DayTime)) => Ok(interval_cross_op!(
            array_rhs,
            array_lhs,
            as_interval_dt_array,
            as_interval_mdn_array,
            op_dt_mdn,
            sign,
            true,
            IntervalDayTimeType,
            IntervalMonthDayNanoType
        )),
        (Interval(MonthDayNano), Interval(MonthDayNano)) => Ok(interval_op!(
            array_lhs,
            array_rhs,
            as_interval_mdn_array,
            op_mdn,
            sign,
            IntervalMonthDayNanoType
        )),
        (_, _) => Err(DataFusionError::Execution(format!(
            "Invalid array types for Interval operation: {} {} {}",
            array_lhs.data_type(),
            sign,
            array_rhs.data_type()
        ))),
    }
}

/// Performs a timestamp/interval operation on two arrays and returns the resulting array.
/// The operation sign determines whether to perform addition or subtraction.
/// The data type and unit of the two input arrays must match the supported combinations.
pub fn ts_interval_array_op(
    array_lhs: &ArrayRef,
    sign: i32,
    array_rhs: &ArrayRef,
) -> Result<ArrayRef> {
    use DataType::*;
    use IntervalUnit::*;
    use TimeUnit::*;
    match (array_lhs.data_type(), array_rhs.data_type()) {
        (Timestamp(Second, tz), Interval(YearMonth)) => Ok(ts_interval_op!(
            array_lhs,
            array_rhs,
            tz,
            as_timestamp_second_array,
            as_interval_ym_array,
            seconds_add_array::<YM_MODE>,
            sign,
            TimestampSecondType,
            IntervalYearMonthType
        )),
        (Timestamp(Second, tz), Interval(DayTime)) => Ok(ts_interval_op!(
            array_lhs,
            array_rhs,
            tz,
            as_timestamp_second_array,
            as_interval_dt_array,
            seconds_add_array::<DT_MODE>,
            sign,
            TimestampSecondType,
            IntervalDayTimeType
        )),
        (Timestamp(Second, tz), Interval(MonthDayNano)) => Ok(ts_interval_op!(
            array_lhs,
            array_rhs,
            tz,
            as_timestamp_second_array,
            as_interval_mdn_array,
            seconds_add_array::<MDN_MODE>,
            sign,
            TimestampSecondType,
            IntervalMonthDayNanoType
        )),
        (Timestamp(Millisecond, tz), Interval(YearMonth)) => Ok(ts_interval_op!(
            array_lhs,
            array_rhs,
            tz,
            as_timestamp_millisecond_array,
            as_interval_ym_array,
            milliseconds_add_array::<YM_MODE>,
            sign,
            TimestampMillisecondType,
            IntervalYearMonthType
        )),
        (Timestamp(Millisecond, tz), Interval(DayTime)) => Ok(ts_interval_op!(
            array_lhs,
            array_rhs,
            tz,
            as_timestamp_millisecond_array,
            as_interval_dt_array,
            milliseconds_add_array::<DT_MODE>,
            sign,
            TimestampMillisecondType,
            IntervalDayTimeType
        )),
        (Timestamp(Millisecond, tz), Interval(MonthDayNano)) => Ok(ts_interval_op!(
            array_lhs,
            array_rhs,
            tz,
            as_timestamp_millisecond_array,
            as_interval_mdn_array,
            milliseconds_add_array::<MDN_MODE>,
            sign,
            TimestampMillisecondType,
            IntervalMonthDayNanoType
        )),
        (Timestamp(Microsecond, tz), Interval(YearMonth)) => Ok(ts_interval_op!(
            array_lhs,
            array_rhs,
            tz,
            as_timestamp_microsecond_array,
            as_interval_ym_array,
            microseconds_add_array::<YM_MODE>,
            sign,
            TimestampMicrosecondType,
            IntervalYearMonthType
        )),
        (Timestamp(Microsecond, tz), Interval(DayTime)) => Ok(ts_interval_op!(
            array_lhs,
            array_rhs,
            tz,
            as_timestamp_microsecond_array,
            as_interval_dt_array,
            microseconds_add_array::<DT_MODE>,
            sign,
            TimestampMicrosecondType,
            IntervalDayTimeType
        )),
        (Timestamp(Microsecond, tz), Interval(MonthDayNano)) => Ok(ts_interval_op!(
            array_lhs,
            array_rhs,
            tz,
            as_timestamp_microsecond_array,
            as_interval_mdn_array,
            microseconds_add_array::<MDN_MODE>,
            sign,
            TimestampMicrosecondType,
            IntervalMonthDayNanoType
        )),
        (Timestamp(Nanosecond, tz), Interval(YearMonth)) => Ok(ts_interval_op!(
            array_lhs,
            array_rhs,
            tz,
            as_timestamp_nanosecond_array,
            as_interval_ym_array,
            nanoseconds_add_array::<YM_MODE>,
            sign,
            TimestampNanosecondType,
            IntervalYearMonthType
        )),
        (Timestamp(Nanosecond, tz), Interval(DayTime)) => Ok(ts_interval_op!(
            array_lhs,
            array_rhs,
            tz,
            as_timestamp_nanosecond_array,
            as_interval_dt_array,
            nanoseconds_add_array::<DT_MODE>,
            sign,
            TimestampNanosecondType,
            IntervalDayTimeType
        )),
        (Timestamp(Nanosecond, tz), Interval(MonthDayNano)) => Ok(ts_interval_op!(
            array_lhs,
            array_rhs,
            tz,
            as_timestamp_nanosecond_array,
            as_interval_mdn_array,
            nanoseconds_add_array::<MDN_MODE>,
            sign,
            TimestampNanosecondType,
            IntervalMonthDayNanoType
        )),
        (_, _) => Err(DataFusionError::Execution(format!(
            "Invalid array types for Timestamp Interval operation: {} {} {}",
            array_lhs.data_type(),
            sign,
            array_rhs.data_type()
        ))),
    }
}

#[inline]
pub fn date32_interval_ym_op(
    right: &Arc<dyn Array>,
    epoch: &NaiveDate,
    prior: &NaiveDate,
    month_op: fn(NaiveDate, Months) -> Option<NaiveDate>,
) -> Result<ArrayRef> {
    let right: &PrimitiveArray<IntervalYearMonthType> = right.as_primitive();
    let ret = Arc::new(try_unary::<IntervalYearMonthType, _, Date32Type>(
        right,
        |ym| {
            let months = Months::new(ym.try_into().map_err(|_| {
                DataFusionError::Internal(
                    "Interval values cannot be casted as unsigned integers".to_string(),
                )
            })?);
            let value = month_op(*prior, months).ok_or_else(|| {
                DataFusionError::Internal("Resulting date is out of range".to_string())
            })?;
            Ok((value - *epoch).num_days() as i32)
        },
    )?) as _;
    Ok(ret)
}

#[inline]
pub fn date32_interval_dt_op(
    right: &Arc<dyn Array>,
    epoch: &NaiveDate,
    prior: &NaiveDate,
    day_op: fn(NaiveDate, Days) -> Option<NaiveDate>,
) -> Result<ArrayRef> {
    let right: &PrimitiveArray<IntervalDayTimeType> = right.as_primitive();
    let ret = Arc::new(try_unary::<IntervalDayTimeType, _, Date32Type>(
        right,
        |dt| {
            let (days, millis) = IntervalDayTimeType::to_parts(dt);
            let days = Days::new(days.try_into().map_err(|_| {
                DataFusionError::Internal(
                    "Interval values cannot be casted as unsigned integers".to_string(),
                )
            })?);
            let value = day_op(*prior, days).ok_or_else(|| {
                DataFusionError::Internal("Resulting date is out of range".to_string())
            })?;
            let milli_days = millis as i64 / MILLISECONDS_IN_DAY;
            Ok(((value - *epoch).num_days() - milli_days) as i32)
        },
    )?) as _;
    Ok(ret)
}

#[inline]
pub fn date32_interval_mdn_op(
    right: &Arc<dyn Array>,
    epoch: &NaiveDate,
    prior: &NaiveDate,
    day_op: fn(NaiveDate, Days) -> Option<NaiveDate>,
    month_op: fn(NaiveDate, Months) -> Option<NaiveDate>,
) -> Result<ArrayRef> {
    let cast_err = |_| {
        DataFusionError::Internal(
            "Interval values cannot be casted as unsigned integers".to_string(),
        )
    };
    let out_of_range =
        || DataFusionError::Internal("Resulting date is out of range".to_string());
    let right: &PrimitiveArray<IntervalMonthDayNanoType> = right.as_primitive();
    let ret = Arc::new(try_unary::<IntervalMonthDayNanoType, _, Date32Type>(
        right,
        |mdn| {
            let (months, days, nanos) = IntervalMonthDayNanoType::to_parts(mdn);
            let months_obj = Months::new(months.try_into().map_err(cast_err)?);
            let month_diff = month_op(*prior, months_obj).ok_or_else(out_of_range)?;
            let days_obj = Days::new(days.try_into().map_err(cast_err)?);
            let value = day_op(month_diff, days_obj).ok_or_else(out_of_range)?;
            let nano_days = nanos / NANOSECONDS_IN_DAY;
            Ok(((value - *epoch).num_days() - nano_days) as i32)
        },
    )?) as _;
    Ok(ret)
}

#[inline]
pub fn date64_interval_ym_op(
    right: &Arc<dyn Array>,
    epoch: &NaiveDate,
    prior: &NaiveDate,
    month_op: fn(NaiveDate, Months) -> Option<NaiveDate>,
) -> Result<ArrayRef> {
    let right: &PrimitiveArray<IntervalYearMonthType> = right.as_primitive();
    let ret = Arc::new(try_unary::<IntervalYearMonthType, _, Date64Type>(
        right,
        |ym| {
            let months_obj = Months::new(ym.try_into().map_err(|_| {
                DataFusionError::Internal(
                    "Interval values cannot be casted as unsigned integers".to_string(),
                )
            })?);
            let date = month_op(*prior, months_obj).ok_or_else(|| {
                DataFusionError::Internal("Resulting date is out of range".to_string())
            })?;
            Ok((date - *epoch).num_milliseconds())
        },
    )?) as _;
    Ok(ret)
}

#[inline]
pub fn date64_interval_dt_op(
    right: &Arc<dyn Array>,
    epoch: &NaiveDate,
    prior: &NaiveDate,
    day_op: fn(NaiveDate, Days) -> Option<NaiveDate>,
) -> Result<ArrayRef> {
    let right: &PrimitiveArray<IntervalDayTimeType> = right.as_primitive();
    let ret = Arc::new(try_unary::<IntervalDayTimeType, _, Date64Type>(
        right,
        |dt| {
            let (days, millis) = IntervalDayTimeType::to_parts(dt);
            let days_obj = Days::new(days.try_into().map_err(|_| {
                DataFusionError::Internal(
                    "Interval values cannot be casted as unsigned integers".to_string(),
                )
            })?);
            let date = day_op(*prior, days_obj).ok_or_else(|| {
                DataFusionError::Internal("Resulting date is out of range".to_string())
            })?;
            Ok((date - *epoch).num_milliseconds() - millis as i64)
        },
    )?) as _;
    Ok(ret)
}

#[inline]
pub fn date64_interval_mdn_op(
    right: &Arc<dyn Array>,
    epoch: &NaiveDate,
    prior: &NaiveDate,
    day_op: fn(NaiveDate, Days) -> Option<NaiveDate>,
    month_op: fn(NaiveDate, Months) -> Option<NaiveDate>,
) -> Result<ArrayRef> {
    let cast_err = |_| {
        DataFusionError::Internal(
            "Interval values cannot be casted as unsigned integers".to_string(),
        )
    };
    let out_of_range =
        || DataFusionError::Internal("Resulting date is out of range".to_string());
    let right: &PrimitiveArray<IntervalMonthDayNanoType> = right.as_primitive();
    let ret = Arc::new(try_unary::<IntervalMonthDayNanoType, _, Date64Type>(
        right,
        |mdn| {
            let (months, days, nanos) = IntervalMonthDayNanoType::to_parts(mdn);
            let months_obj = Months::new(months.try_into().map_err(cast_err)?);
            let month_diff = month_op(*prior, months_obj).ok_or_else(out_of_range)?;
            let days_obj = Days::new(days.try_into().map_err(cast_err)?);
            let value = day_op(month_diff, days_obj).ok_or_else(out_of_range)?;
            Ok((value - *epoch).num_milliseconds() - nanos / 1_000_000)
        },
    )?) as _;
    Ok(ret)
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion_expr::type_coercion::binary::decimal_op_mathematics_type;
    use datafusion_expr::Operator;

    fn create_decimal_array(
        array: &[Option<i128>],
        precision: u8,
        scale: i8,
    ) -> Decimal128Array {
        let mut decimal_builder = Decimal128Builder::with_capacity(array.len());

        for value in array.iter().copied() {
            decimal_builder.append_option(value)
        }
        decimal_builder
            .finish()
            .with_precision_and_scale(precision, scale)
            .unwrap()
    }

    fn create_int_array(array: &[Option<i32>]) -> Int32Array {
        let mut int_builder = Int32Builder::with_capacity(array.len());

        for value in array.iter().copied() {
            int_builder.append_option(value)
        }
        int_builder.finish()
    }

    #[test]
    fn comparison_decimal_op_test() -> Result<()> {
        let value_i128: i128 = 123;
        let decimal_array = create_decimal_array(
            &[
                Some(value_i128),
                None,
                Some(value_i128 - 1),
                Some(value_i128 + 1),
            ],
            25,
            3,
        );
        let left_decimal_array = decimal_array;
        let right_decimal_array = create_decimal_array(
            &[
                Some(value_i128 - 1),
                Some(value_i128),
                Some(value_i128 + 1),
                Some(value_i128 + 1),
            ],
            25,
            3,
        );

        // is_distinct: left distinct right
        let result = is_distinct_from(&left_decimal_array, &right_decimal_array)?;
        assert_eq!(
            BooleanArray::from(vec![Some(true), Some(true), Some(true), Some(false)]),
            result
        );
        // is_distinct: left distinct right
        let result = is_not_distinct_from(&left_decimal_array, &right_decimal_array)?;
        assert_eq!(
            BooleanArray::from(vec![Some(false), Some(false), Some(false), Some(true)]),
            result
        );
        Ok(())
    }

    #[test]
    fn arithmetic_decimal_op_test() -> Result<()> {
        let value_i128: i128 = 123;
        let left_decimal_array = create_decimal_array(
            &[
                Some(value_i128),
                None,
                Some(value_i128 - 1),
                Some(value_i128 + 1),
            ],
            25,
            3,
        );
        let right_decimal_array = create_decimal_array(
            &[
                Some(value_i128),
                Some(value_i128),
                Some(value_i128),
                Some(value_i128),
            ],
            25,
            3,
        );
        // add
        let result_type = decimal_op_mathematics_type(
            &Operator::Plus,
            left_decimal_array.data_type(),
            right_decimal_array.data_type(),
        )
        .unwrap();
        let result =
            add_dyn_decimal(&left_decimal_array, &right_decimal_array, &result_type)?;
        let result = as_decimal128_array(&result)?;
        let expect =
            create_decimal_array(&[Some(246), None, Some(245), Some(247)], 26, 3);
        assert_eq!(&expect, result);
        let result = add_decimal_dyn_scalar(&left_decimal_array, 10, &result_type)?;
        let result = as_decimal128_array(&result)?;
        let expect =
            create_decimal_array(&[Some(133), None, Some(132), Some(134)], 26, 3);
        assert_eq!(&expect, result);
        // subtract
        let result_type = decimal_op_mathematics_type(
            &Operator::Minus,
            left_decimal_array.data_type(),
            right_decimal_array.data_type(),
        )
        .unwrap();
        let result = subtract_dyn_decimal(
            &left_decimal_array,
            &right_decimal_array,
            &result_type,
        )?;
        let result = as_decimal128_array(&result)?;
        let expect = create_decimal_array(&[Some(0), None, Some(-1), Some(1)], 26, 3);
        assert_eq!(&expect, result);
        let result = subtract_decimal_dyn_scalar(&left_decimal_array, 10, &result_type)?;
        let result = as_decimal128_array(&result)?;
        let expect =
            create_decimal_array(&[Some(113), None, Some(112), Some(114)], 26, 3);
        assert_eq!(&expect, result);
        // multiply
        let result_type = decimal_op_mathematics_type(
            &Operator::Multiply,
            left_decimal_array.data_type(),
            right_decimal_array.data_type(),
        )
        .unwrap();
        let result = multiply_dyn_decimal(
            &left_decimal_array,
            &right_decimal_array,
            &result_type,
        )?;
        let result = as_decimal128_array(&result)?;
        let expect =
            create_decimal_array(&[Some(15129), None, Some(15006), Some(15252)], 38, 6);
        assert_eq!(&expect, result);
        let result = multiply_decimal_dyn_scalar(&left_decimal_array, 10, &result_type)?;
        let result = as_decimal128_array(&result)?;
        let expect =
            create_decimal_array(&[Some(1230), None, Some(1220), Some(1240)], 38, 6);
        assert_eq!(&expect, result);
        // divide
        let result_type = decimal_op_mathematics_type(
            &Operator::Divide,
            left_decimal_array.data_type(),
            right_decimal_array.data_type(),
        )
        .unwrap();
        let left_decimal_array = create_decimal_array(
            &[
                Some(1234567),
                None,
                Some(1234567),
                Some(1234567),
                Some(1234567),
            ],
            25,
            3,
        );
        let right_decimal_array = create_decimal_array(
            &[Some(10), Some(100), Some(55), Some(-123), None],
            25,
            3,
        );
        let result = divide_dyn_checked_decimal(
            &left_decimal_array,
            &right_decimal_array,
            &result_type,
        )?;
        let result = as_decimal128_array(&result)?;
        let expect = create_decimal_array(
            &[
                Some(12345670000000000000000000000000000),
                None,
                Some(2244667272727272727272727272727272),
                Some(-1003713008130081300813008130081300),
                None,
            ],
            38,
            29,
        );
        assert_eq!(&expect, result);
        let result = divide_decimal_dyn_scalar(&left_decimal_array, 10, &result_type)?;
        let result = as_decimal128_array(&result)?;
        let expect = create_decimal_array(
            &[
                Some(12345670000000000000000000000000000),
                None,
                Some(12345670000000000000000000000000000),
                Some(12345670000000000000000000000000000),
                Some(12345670000000000000000000000000000),
            ],
            38,
            29,
        );
        assert_eq!(&expect, result);
        // modulus
        let result_type = decimal_op_mathematics_type(
            &Operator::Modulo,
            left_decimal_array.data_type(),
            right_decimal_array.data_type(),
        )
        .unwrap();
        let result =
            modulus_dyn_decimal(&left_decimal_array, &right_decimal_array, &result_type)?;
        let result = as_decimal128_array(&result)?;
        let expect =
            create_decimal_array(&[Some(7), None, Some(37), Some(16), None], 25, 3);
        assert_eq!(&expect, result);
        let result = modulus_decimal_dyn_scalar(&left_decimal_array, 10, &result_type)?;
        let result = as_decimal128_array(&result)?;
        let expect =
            create_decimal_array(&[Some(7), None, Some(7), Some(7), Some(7)], 25, 3);
        assert_eq!(&expect, result);

        Ok(())
    }

    #[test]
    fn arithmetic_decimal_divide_by_zero() {
        let left_decimal_array = create_decimal_array(&[Some(101)], 10, 1);
        let right_decimal_array = create_decimal_array(&[Some(0)], 1, 1);

        let result_type = decimal_op_mathematics_type(
            &Operator::Divide,
            left_decimal_array.data_type(),
            right_decimal_array.data_type(),
        )
        .unwrap();
        let err =
            divide_decimal_dyn_scalar(&left_decimal_array, 0, &result_type).unwrap_err();
        assert_eq!("Arrow error: Divide by zero error", err.to_string());
        let result_type = decimal_op_mathematics_type(
            &Operator::Modulo,
            left_decimal_array.data_type(),
            right_decimal_array.data_type(),
        )
        .unwrap();
        let err =
            modulus_dyn_decimal(&left_decimal_array, &right_decimal_array, &result_type)
                .unwrap_err();
        assert_eq!("Arrow error: Divide by zero error", err.to_string());
        let err =
            modulus_decimal_dyn_scalar(&left_decimal_array, 0, &result_type).unwrap_err();
        assert_eq!("Arrow error: Divide by zero error", err.to_string());
    }

    #[test]
    fn is_distinct_from_non_nulls() -> Result<()> {
        let left_int_array =
            create_int_array(&[Some(0), Some(1), Some(2), Some(3), Some(4)]);
        let right_int_array =
            create_int_array(&[Some(4), Some(3), Some(2), Some(1), Some(0)]);

        assert_eq!(
            BooleanArray::from(vec![
                Some(true),
                Some(true),
                Some(false),
                Some(true),
                Some(true),
            ]),
            is_distinct_from(&left_int_array, &right_int_array)?
        );
        assert_eq!(
            BooleanArray::from(vec![
                Some(false),
                Some(false),
                Some(true),
                Some(false),
                Some(false),
            ]),
            is_not_distinct_from(&left_int_array, &right_int_array)?
        );
        Ok(())
    }

    #[test]
    fn is_distinct_from_nulls() -> Result<()> {
        let left_int_array =
            create_int_array(&[Some(0), Some(0), None, Some(3), Some(0), Some(0)]);
        let right_int_array =
            create_int_array(&[Some(0), None, None, None, Some(0), None]);

        assert_eq!(
            BooleanArray::from(vec![
                Some(false),
                Some(true),
                Some(false),
                Some(true),
                Some(false),
                Some(true),
            ]),
            is_distinct_from(&left_int_array, &right_int_array)?
        );

        assert_eq!(
            BooleanArray::from(vec![
                Some(true),
                Some(false),
                Some(true),
                Some(false),
                Some(true),
                Some(false),
            ]),
            is_not_distinct_from(&left_int_array, &right_int_array)?
        );
        Ok(())
    }
}
