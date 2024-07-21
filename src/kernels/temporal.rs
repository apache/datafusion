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

//! temporal kernels

use chrono::{DateTime, Datelike, Duration, NaiveDateTime, Timelike, Utc};

use std::sync::Arc;

use arrow::{array::*, datatypes::DataType};
use arrow_array::{
    downcast_dictionary_array, downcast_temporal_array,
    temporal_conversions::*,
    timezone::Tz,
    types::{ArrowDictionaryKeyType, ArrowTemporalType, Date32Type, TimestampMicrosecondType},
    ArrowNumericType,
};

use arrow_schema::TimeUnit;

use crate::SparkError;

// Copied from arrow_arith/temporal.rs
macro_rules! return_compute_error_with {
    ($msg:expr, $param:expr) => {
        return { Err(SparkError::Internal(format!("{}: {:?}", $msg, $param))) }
    };
}

// The number of days between the beginning of the proleptic gregorian calendar (0001-01-01)
// and the beginning of the Unix Epoch (1970-01-01)
const DAYS_TO_UNIX_EPOCH: i32 = 719_163;

// Copied from arrow_arith/temporal.rs with modification to the output datatype
// Transforms a array of NaiveDate to an array of Date32 after applying an operation
fn as_datetime_with_op<A: ArrayAccessor<Item = T::Native>, T: ArrowTemporalType, F>(
    iter: ArrayIter<A>,
    mut builder: PrimitiveBuilder<Date32Type>,
    op: F,
) -> Date32Array
where
    F: Fn(NaiveDateTime) -> i32,
    i64: From<T::Native>,
{
    iter.into_iter().for_each(|value| {
        if let Some(value) = value {
            match as_datetime::<T>(i64::from(value)) {
                Some(dt) => builder.append_value(op(dt)),
                None => builder.append_null(),
            }
        } else {
            builder.append_null();
        }
    });

    builder.finish()
}

#[inline]
fn as_datetime_with_op_single<F>(
    value: Option<i32>,
    builder: &mut PrimitiveBuilder<Date32Type>,
    op: F,
) where
    F: Fn(NaiveDateTime) -> i32,
{
    if let Some(value) = value {
        match as_datetime::<Date32Type>(i64::from(value)) {
            Some(dt) => builder.append_value(op(dt)),
            None => builder.append_null(),
        }
    } else {
        builder.append_null();
    }
}

// Based on arrow_arith/temporal.rs:extract_component_from_datetime_array
// Transforms an array of DateTime<Tz> to an arrayOf TimeStampMicrosecond after applying an
// operation
fn as_timestamp_tz_with_op<A: ArrayAccessor<Item = T::Native>, T: ArrowTemporalType, F>(
    iter: ArrayIter<A>,
    mut builder: PrimitiveBuilder<TimestampMicrosecondType>,
    tz: &str,
    op: F,
) -> Result<TimestampMicrosecondArray, SparkError>
where
    F: Fn(DateTime<Tz>) -> i64,
    i64: From<T::Native>,
{
    let tz: Tz = tz.parse()?;
    for value in iter {
        match value {
            Some(value) => match as_datetime_with_timezone::<T>(value.into(), tz) {
                Some(time) => builder.append_value(op(time)),
                _ => {
                    return Err(SparkError::Internal(
                        "Unable to read value as datetime".to_string(),
                    ));
                }
            },
            None => builder.append_null(),
        }
    }
    Ok(builder.finish())
}

fn as_timestamp_tz_with_op_single<T: ArrowTemporalType, F>(
    value: Option<T::Native>,
    builder: &mut PrimitiveBuilder<TimestampMicrosecondType>,
    tz: &Tz,
    op: F,
) -> Result<(), SparkError>
where
    F: Fn(DateTime<Tz>) -> i64,
    i64: From<T::Native>,
{
    match value {
        Some(value) => match as_datetime_with_timezone::<T>(value.into(), *tz) {
            Some(time) => builder.append_value(op(time)),
            _ => {
                return Err(SparkError::Internal(
                    "Unable to read value as datetime".to_string(),
                ));
            }
        },
        None => builder.append_null(),
    }
    Ok(())
}

#[inline]
fn as_days_from_unix_epoch(dt: Option<NaiveDateTime>) -> i32 {
    dt.unwrap().num_days_from_ce() - DAYS_TO_UNIX_EPOCH
}

// Apply the Tz to the Naive Date Time,,convert to UTC, and return as microseconds in Unix epoch
#[inline]
fn as_micros_from_unix_epoch_utc(dt: Option<DateTime<Tz>>) -> i64 {
    dt.unwrap().with_timezone(&Utc).timestamp_micros()
}

#[inline]
fn trunc_date_to_year<T: Datelike + Timelike>(dt: T) -> Option<T> {
    Some(dt)
        .and_then(|d| d.with_nanosecond(0))
        .and_then(|d| d.with_second(0))
        .and_then(|d| d.with_minute(0))
        .and_then(|d| d.with_hour(0))
        .and_then(|d| d.with_day0(0))
        .and_then(|d| d.with_month0(0))
}

/// returns the month of the beginning of the quarter
#[inline]
fn quarter_month<T: Datelike>(dt: &T) -> u32 {
    1 + 3 * ((dt.month() - 1) / 3)
}

#[inline]
fn trunc_date_to_quarter<T: Datelike + Timelike>(dt: T) -> Option<T> {
    Some(dt)
        .and_then(|d| d.with_nanosecond(0))
        .and_then(|d| d.with_second(0))
        .and_then(|d| d.with_minute(0))
        .and_then(|d| d.with_hour(0))
        .and_then(|d| d.with_day0(0))
        .and_then(|d| d.with_month(quarter_month(&d)))
}

#[inline]
fn trunc_date_to_month<T: Datelike + Timelike>(dt: T) -> Option<T> {
    Some(dt)
        .and_then(|d| d.with_nanosecond(0))
        .and_then(|d| d.with_second(0))
        .and_then(|d| d.with_minute(0))
        .and_then(|d| d.with_hour(0))
        .and_then(|d| d.with_day0(0))
}

#[inline]
fn trunc_date_to_week<T>(dt: T) -> Option<T>
where
    T: Datelike + Timelike + std::ops::Sub<Duration, Output = T> + Copy,
{
    Some(dt)
        .map(|d| d - Duration::try_seconds(60 * 60 * 24 * d.weekday() as i64).unwrap())
        .and_then(|d| d.with_nanosecond(0))
        .and_then(|d| d.with_second(0))
        .and_then(|d| d.with_minute(0))
        .and_then(|d| d.with_hour(0))
}

#[inline]
fn trunc_date_to_day<T: Timelike>(dt: T) -> Option<T> {
    Some(dt)
        .and_then(|d| d.with_nanosecond(0))
        .and_then(|d| d.with_second(0))
        .and_then(|d| d.with_minute(0))
        .and_then(|d| d.with_hour(0))
}

#[inline]
fn trunc_date_to_hour<T: Timelike>(dt: T) -> Option<T> {
    Some(dt)
        .and_then(|d| d.with_nanosecond(0))
        .and_then(|d| d.with_second(0))
        .and_then(|d| d.with_minute(0))
}

#[inline]
fn trunc_date_to_minute<T: Timelike>(dt: T) -> Option<T> {
    Some(dt)
        .and_then(|d| d.with_nanosecond(0))
        .and_then(|d| d.with_second(0))
}

#[inline]
fn trunc_date_to_second<T: Timelike>(dt: T) -> Option<T> {
    Some(dt).and_then(|d| d.with_nanosecond(0))
}

#[inline]
fn trunc_date_to_ms<T: Timelike>(dt: T) -> Option<T> {
    Some(dt).and_then(|d| d.with_nanosecond(1_000_000 * (d.nanosecond() / 1_000_000)))
}

#[inline]
fn trunc_date_to_microsec<T: Timelike>(dt: T) -> Option<T> {
    Some(dt).and_then(|d| d.with_nanosecond(1_000 * (d.nanosecond() / 1_000)))
}

///
/// Implements the spark [TRUNC](https://spark.apache.org/docs/latest/api/sql/index.html#trunc)
/// function where the specified format is a scalar value
///
///   array is an array of Date32 values. The array may be a dictionary array.
///
///   format is a scalar string specifying the format to apply to the timestamp value.
pub(crate) fn date_trunc_dyn(array: &dyn Array, format: String) -> Result<ArrayRef, SparkError> {
    match array.data_type().clone() {
        DataType::Dictionary(_, _) => {
            downcast_dictionary_array!(
                array => {
                    let truncated_values = date_trunc_dyn(array.values(), format)?;
                    Ok(Arc::new(array.with_values(truncated_values)))
                }
                dt => return_compute_error_with!("date_trunc does not support", dt),
            )
        }
        _ => {
            downcast_temporal_array!(
                array => {
                   date_trunc(array, format)
                    .map(|a| Arc::new(a) as ArrayRef)
                }
                dt => return_compute_error_with!("date_trunc does not support", dt),
            )
        }
    }
}

pub(crate) fn date_trunc<T>(
    array: &PrimitiveArray<T>,
    format: String,
) -> Result<Date32Array, SparkError>
where
    T: ArrowTemporalType + ArrowNumericType,
    i64: From<T::Native>,
{
    let builder = Date32Builder::with_capacity(array.len());
    let iter = ArrayIter::new(array);
    match array.data_type() {
        DataType::Date32 => match format.to_uppercase().as_str() {
            "YEAR" | "YYYY" | "YY" => Ok(as_datetime_with_op::<&PrimitiveArray<T>, T, _>(
                iter,
                builder,
                |dt| as_days_from_unix_epoch(trunc_date_to_year(dt)),
            )),
            "QUARTER" => Ok(as_datetime_with_op::<&PrimitiveArray<T>, T, _>(
                iter,
                builder,
                |dt| as_days_from_unix_epoch(trunc_date_to_quarter(dt)),
            )),
            "MONTH" | "MON" | "MM" => Ok(as_datetime_with_op::<&PrimitiveArray<T>, T, _>(
                iter,
                builder,
                |dt| as_days_from_unix_epoch(trunc_date_to_month(dt)),
            )),
            "WEEK" => Ok(as_datetime_with_op::<&PrimitiveArray<T>, T, _>(
                iter,
                builder,
                |dt| as_days_from_unix_epoch(trunc_date_to_week(dt)),
            )),
            _ => Err(SparkError::Internal(format!(
                "Unsupported format: {:?} for function 'date_trunc'",
                format
            ))),
        },
        dt => return_compute_error_with!(
            "Unsupported input type '{:?}' for function 'date_trunc'",
            dt
        ),
    }
}

///
/// Implements the spark [TRUNC](https://spark.apache.org/docs/latest/api/sql/index.html#trunc)
/// function where the specified format may be an array
///
///   array is an array of Date32 values. The array may be a dictionary array.
///
///   format is an array of strings specifying the format to apply to the corresponding date value.
///             The array may be a dictionary array.
pub(crate) fn date_trunc_array_fmt_dyn(
    array: &dyn Array,
    formats: &dyn Array,
) -> Result<ArrayRef, SparkError> {
    match (array.data_type().clone(), formats.data_type().clone()) {
        (DataType::Dictionary(_, v), DataType::Dictionary(_, f)) => {
            if !matches!(*v, DataType::Date32) {
                return_compute_error_with!("date_trunc does not support", v)
            }
            if !matches!(*f, DataType::Utf8) {
                return_compute_error_with!("date_trunc does not support format type ", f)
            }
            downcast_dictionary_array!(
                formats => {
                    downcast_dictionary_array!(
                        array => {
                            date_trunc_array_fmt_dict_dict(
                                    &array.downcast_dict::<Date32Array>().unwrap(),
                                    &formats.downcast_dict::<StringArray>().unwrap())
                            .map(|a| Arc::new(a) as ArrayRef)
                        }
                        dt => return_compute_error_with!("date_trunc does not support", dt)
                    )
                }
                fmt => return_compute_error_with!("date_trunc does not support format type", fmt),
            )
        }
        (DataType::Dictionary(_, v), DataType::Utf8) => {
            if !matches!(*v, DataType::Date32) {
                return_compute_error_with!("date_trunc does not support", v)
            }
            downcast_dictionary_array!(
                array => {
                  date_trunc_array_fmt_dict_plain(
                        &array.downcast_dict::<Date32Array>().unwrap(),
                        formats.as_any().downcast_ref::<StringArray>()
                            .expect("Unexpected value type in formats"))
                  .map(|a| Arc::new(a) as ArrayRef)
                }
                dt => return_compute_error_with!("date_trunc does not support", dt),
            )
        }
        (DataType::Date32, DataType::Dictionary(_, f)) => {
            if !matches!(*f, DataType::Utf8) {
                return_compute_error_with!("date_trunc does not support format type ", f)
            }
            downcast_dictionary_array!(
                formats => {
                downcast_temporal_array!(array => {
                        date_trunc_array_fmt_plain_dict(
                            array.as_any().downcast_ref::<Date32Array>()
                                .expect("Unexpected error in casting date array"),
                            &formats.downcast_dict::<StringArray>().unwrap())
                        .map(|a| Arc::new(a) as ArrayRef)
                    }
                    dt => return_compute_error_with!("date_trunc does not support", dt),
                    )
                }
                fmt => return_compute_error_with!("date_trunc does not support format type", fmt),
            )
        }
        (DataType::Date32, DataType::Utf8) => date_trunc_array_fmt_plain_plain(
            array
                .as_any()
                .downcast_ref::<Date32Array>()
                .expect("Unexpected error in casting date array"),
            formats
                .as_any()
                .downcast_ref::<StringArray>()
                .expect("Unexpected value type in formats"),
        )
        .map(|a| Arc::new(a) as ArrayRef),
        (dt, fmt) => Err(SparkError::Internal(format!(
            "Unsupported datatype: {:}, format: {:?} for function 'date_trunc'",
            dt, fmt
        ))),
    }
}

macro_rules! date_trunc_array_fmt_helper {
    ($array: ident, $formats: ident, $datatype: ident) => {{
        let mut builder = Date32Builder::with_capacity($array.len());
        let iter = $array.into_iter();
        match $datatype {
            DataType::Date32 => {
                for (index, val) in iter.enumerate() {
                    let op_result = match $formats.value(index).to_uppercase().as_str() {
                        "YEAR" | "YYYY" | "YY" => {
                            Ok(as_datetime_with_op_single(val, &mut builder, |dt| {
                                as_days_from_unix_epoch(trunc_date_to_year(dt))
                            }))
                        }
                        "QUARTER" => Ok(as_datetime_with_op_single(val, &mut builder, |dt| {
                            as_days_from_unix_epoch(trunc_date_to_quarter(dt))
                        })),
                        "MONTH" | "MON" | "MM" => {
                            Ok(as_datetime_with_op_single(val, &mut builder, |dt| {
                                as_days_from_unix_epoch(trunc_date_to_month(dt))
                            }))
                        }
                        "WEEK" => Ok(as_datetime_with_op_single(val, &mut builder, |dt| {
                            as_days_from_unix_epoch(trunc_date_to_week(dt))
                        })),
                        _ => Err(SparkError::Internal(format!(
                            "Unsupported format: {:?} for function 'date_trunc'",
                            $formats.value(index)
                        ))),
                    };
                    op_result?
                }
                Ok(builder.finish())
            }
            dt => return_compute_error_with!(
                "Unsupported input type '{:?}' for function 'date_trunc'",
                dt
            ),
        }
    }};
}

fn date_trunc_array_fmt_plain_plain(
    array: &Date32Array,
    formats: &StringArray,
) -> Result<Date32Array, SparkError>
where
{
    let data_type = array.data_type();
    date_trunc_array_fmt_helper!(array, formats, data_type)
}

fn date_trunc_array_fmt_plain_dict<K>(
    array: &Date32Array,
    formats: &TypedDictionaryArray<K, StringArray>,
) -> Result<Date32Array, SparkError>
where
    K: ArrowDictionaryKeyType,
{
    let data_type = array.data_type();
    date_trunc_array_fmt_helper!(array, formats, data_type)
}

fn date_trunc_array_fmt_dict_plain<K>(
    array: &TypedDictionaryArray<K, Date32Array>,
    formats: &StringArray,
) -> Result<Date32Array, SparkError>
where
    K: ArrowDictionaryKeyType,
{
    let data_type = array.values().data_type();
    date_trunc_array_fmt_helper!(array, formats, data_type)
}

fn date_trunc_array_fmt_dict_dict<K, F>(
    array: &TypedDictionaryArray<K, Date32Array>,
    formats: &TypedDictionaryArray<F, StringArray>,
) -> Result<Date32Array, SparkError>
where
    K: ArrowDictionaryKeyType,
    F: ArrowDictionaryKeyType,
{
    let data_type = array.values().data_type();
    date_trunc_array_fmt_helper!(array, formats, data_type)
}

///
/// Implements the spark [DATE_TRUNC](https://spark.apache.org/docs/latest/api/sql/index.html#date_trunc)
/// function where the specified format is a scalar value
///
///   array is an array of Timestamp(Microsecond) values. Timestamp values must have a valid
///            timezone or no timezone. The array may be a dictionary array.
///
///   format is a scalar string specifying the format to apply to the timestamp value.
pub(crate) fn timestamp_trunc_dyn(
    array: &dyn Array,
    format: String,
) -> Result<ArrayRef, SparkError> {
    match array.data_type().clone() {
        DataType::Dictionary(_, _) => {
            downcast_dictionary_array!(
                array => {
                    let truncated_values = timestamp_trunc_dyn(array.values(), format)?;
                    Ok(Arc::new(array.with_values(truncated_values)))
                }
                dt => return_compute_error_with!("timestamp_trunc does not support", dt),
            )
        }
        _ => {
            downcast_temporal_array!(
                array => {
                   timestamp_trunc(array, format)
                    .map(|a| Arc::new(a) as ArrayRef)
                }
                dt => return_compute_error_with!("timestamp_trunc does not support", dt),
            )
        }
    }
}

pub(crate) fn timestamp_trunc<T>(
    array: &PrimitiveArray<T>,
    format: String,
) -> Result<TimestampMicrosecondArray, SparkError>
where
    T: ArrowTemporalType + ArrowNumericType,
    i64: From<T::Native>,
{
    let builder = TimestampMicrosecondBuilder::with_capacity(array.len());
    let iter = ArrayIter::new(array);
    match array.data_type() {
        DataType::Timestamp(TimeUnit::Microsecond, Some(tz)) => {
            match format.to_uppercase().as_str() {
                "YEAR" | "YYYY" | "YY" => {
                    as_timestamp_tz_with_op::<&PrimitiveArray<T>, T, _>(iter, builder, tz, |dt| {
                        as_micros_from_unix_epoch_utc(trunc_date_to_year(dt))
                    })
                }
                "QUARTER" => {
                    as_timestamp_tz_with_op::<&PrimitiveArray<T>, T, _>(iter, builder, tz, |dt| {
                        as_micros_from_unix_epoch_utc(trunc_date_to_quarter(dt))
                    })
                }
                "MONTH" | "MON" | "MM" => {
                    as_timestamp_tz_with_op::<&PrimitiveArray<T>, T, _>(iter, builder, tz, |dt| {
                        as_micros_from_unix_epoch_utc(trunc_date_to_month(dt))
                    })
                }
                "WEEK" => {
                    as_timestamp_tz_with_op::<&PrimitiveArray<T>, T, _>(iter, builder, tz, |dt| {
                        as_micros_from_unix_epoch_utc(trunc_date_to_week(dt))
                    })
                }
                "DAY" | "DD" => {
                    as_timestamp_tz_with_op::<&PrimitiveArray<T>, T, _>(iter, builder, tz, |dt| {
                        as_micros_from_unix_epoch_utc(trunc_date_to_day(dt))
                    })
                }
                "HOUR" => {
                    as_timestamp_tz_with_op::<&PrimitiveArray<T>, T, _>(iter, builder, tz, |dt| {
                        as_micros_from_unix_epoch_utc(trunc_date_to_hour(dt))
                    })
                }
                "MINUTE" => {
                    as_timestamp_tz_with_op::<&PrimitiveArray<T>, T, _>(iter, builder, tz, |dt| {
                        as_micros_from_unix_epoch_utc(trunc_date_to_minute(dt))
                    })
                }
                "SECOND" => {
                    as_timestamp_tz_with_op::<&PrimitiveArray<T>, T, _>(iter, builder, tz, |dt| {
                        as_micros_from_unix_epoch_utc(trunc_date_to_second(dt))
                    })
                }
                "MILLISECOND" => {
                    as_timestamp_tz_with_op::<&PrimitiveArray<T>, T, _>(iter, builder, tz, |dt| {
                        as_micros_from_unix_epoch_utc(trunc_date_to_ms(dt))
                    })
                }
                "MICROSECOND" => {
                    as_timestamp_tz_with_op::<&PrimitiveArray<T>, T, _>(iter, builder, tz, |dt| {
                        as_micros_from_unix_epoch_utc(trunc_date_to_microsec(dt))
                    })
                }
                _ => Err(SparkError::Internal(format!(
                    "Unsupported format: {:?} for function 'timestamp_trunc'",
                    format
                ))),
            }
        }
        dt => return_compute_error_with!(
            "Unsupported input type '{:?}' for function 'timestamp_trunc'",
            dt
        ),
    }
}

///
/// Implements the spark [DATE_TRUNC](https://spark.apache.org/docs/latest/api/sql/index.html#date_trunc)
/// function where the specified format may be an array
///
///   array is an array of Timestamp(Microsecond) values. Timestamp values must have a valid
///            timezone or no timezone. The array may be a dictionary array.
///
///   format is an array of strings specifying the format to apply to the corresponding timestamp
///             value. The array may be a dictionary array.
pub(crate) fn timestamp_trunc_array_fmt_dyn(
    array: &dyn Array,
    formats: &dyn Array,
) -> Result<ArrayRef, SparkError> {
    match (array.data_type().clone(), formats.data_type().clone()) {
        (DataType::Dictionary(_, _), DataType::Dictionary(_, _)) => {
            downcast_dictionary_array!(
                formats => {
                    downcast_dictionary_array!(
                        array => {
                            timestamp_trunc_array_fmt_dict_dict(
                                    &array.downcast_dict::<TimestampMicrosecondArray>().unwrap(),
                                    &formats.downcast_dict::<StringArray>().unwrap())
                            .map(|a| Arc::new(a) as ArrayRef)
                        }
                        dt => return_compute_error_with!("timestamp_trunc does not support", dt)
                    )
                }
                fmt => return_compute_error_with!("timestamp_trunc does not support format type", fmt),
            )
        }
        (DataType::Dictionary(_, _), DataType::Utf8) => {
            downcast_dictionary_array!(
                array => {
                  timestamp_trunc_array_fmt_dict_plain(
                        &array.downcast_dict::<PrimitiveArray<TimestampMicrosecondType>>().unwrap(),
                        formats.as_any().downcast_ref::<StringArray>()
                            .expect("Unexpected value type in formats"))
                  .map(|a| Arc::new(a) as ArrayRef)
                }
                dt => return_compute_error_with!("timestamp_trunc does not support", dt),
            )
        }
        (DataType::Timestamp(TimeUnit::Microsecond, _), DataType::Dictionary(_, _)) => {
            downcast_dictionary_array!(
                formats => {
                downcast_temporal_array!(array => {
                        timestamp_trunc_array_fmt_plain_dict(
                                array,
                                &formats.downcast_dict::<StringArray>().unwrap())
                        .map(|a| Arc::new(a) as ArrayRef)
                    }
                    dt => return_compute_error_with!("timestamp_trunc does not support", dt),
                    )
                }
                fmt => return_compute_error_with!("timestamp_trunc does not support format type", fmt),
            )
        }
        (DataType::Timestamp(TimeUnit::Microsecond, _), DataType::Utf8) => {
            downcast_temporal_array!(
                array => {
                    timestamp_trunc_array_fmt_plain_plain(array,
                        formats.as_any().downcast_ref::<StringArray>().expect("Unexpected value type in formats"))
                    .map(|a| Arc::new(a) as ArrayRef)
                },
                dt => return_compute_error_with!("timestamp_trunc does not support", dt),
            )
        }
        (dt, fmt) => Err(SparkError::Internal(format!(
            "Unsupported datatype: {:}, format: {:?} for function 'timestamp_trunc'",
            dt, fmt
        ))),
    }
}

macro_rules! timestamp_trunc_array_fmt_helper {
    ($array: ident, $formats: ident, $datatype: ident) => {{
        let mut builder = TimestampMicrosecondBuilder::with_capacity($array.len());
        let iter = $array.into_iter();
        assert_eq!(
            $array.len(),
            $formats.len(),
            "lengths of values array and format array must be the same"
        );
        match $datatype {
            DataType::Timestamp(TimeUnit::Microsecond, Some(tz)) => {
                let tz: Tz = tz.parse()?;
                for (index, val) in iter.enumerate() {
                    let op_result = match $formats.value(index).to_uppercase().as_str() {
                        "YEAR" | "YYYY" | "YY" => {
                            as_timestamp_tz_with_op_single::<T, _>(val, &mut builder, &tz, |dt| {
                                as_micros_from_unix_epoch_utc(trunc_date_to_year(dt))
                            })
                        }
                        "QUARTER" => {
                            as_timestamp_tz_with_op_single::<T, _>(val, &mut builder, &tz, |dt| {
                                as_micros_from_unix_epoch_utc(trunc_date_to_quarter(dt))
                            })
                        }
                        "MONTH" | "MON" | "MM" => {
                            as_timestamp_tz_with_op_single::<T, _>(val, &mut builder, &tz, |dt| {
                                as_micros_from_unix_epoch_utc(trunc_date_to_month(dt))
                            })
                        }
                        "WEEK" => {
                            as_timestamp_tz_with_op_single::<T, _>(val, &mut builder, &tz, |dt| {
                                as_micros_from_unix_epoch_utc(trunc_date_to_week(dt))
                            })
                        }
                        "DAY" | "DD" => {
                            as_timestamp_tz_with_op_single::<T, _>(val, &mut builder, &tz, |dt| {
                                as_micros_from_unix_epoch_utc(trunc_date_to_day(dt))
                            })
                        }
                        "HOUR" => {
                            as_timestamp_tz_with_op_single::<T, _>(val, &mut builder, &tz, |dt| {
                                as_micros_from_unix_epoch_utc(trunc_date_to_hour(dt))
                            })
                        }
                        "MINUTE" => {
                            as_timestamp_tz_with_op_single::<T, _>(val, &mut builder, &tz, |dt| {
                                as_micros_from_unix_epoch_utc(trunc_date_to_minute(dt))
                            })
                        }
                        "SECOND" => {
                            as_timestamp_tz_with_op_single::<T, _>(val, &mut builder, &tz, |dt| {
                                as_micros_from_unix_epoch_utc(trunc_date_to_second(dt))
                            })
                        }
                        "MILLISECOND" => {
                            as_timestamp_tz_with_op_single::<T, _>(val, &mut builder, &tz, |dt| {
                                as_micros_from_unix_epoch_utc(trunc_date_to_ms(dt))
                            })
                        }
                        "MICROSECOND" => {
                            as_timestamp_tz_with_op_single::<T, _>(val, &mut builder, &tz, |dt| {
                                as_micros_from_unix_epoch_utc(trunc_date_to_microsec(dt))
                            })
                        }
                        _ => Err(SparkError::Internal(format!(
                            "Unsupported format: {:?} for function 'timestamp_trunc'",
                            $formats.value(index)
                        ))),
                    };
                    op_result?
                }
                Ok(builder.finish())
            }
            dt => {
                return_compute_error_with!(
                    "Unsupported input type '{:?}' for function 'timestamp_trunc'",
                    dt
                )
            }
        }
    }};
}

fn timestamp_trunc_array_fmt_plain_plain<T>(
    array: &PrimitiveArray<T>,
    formats: &StringArray,
) -> Result<TimestampMicrosecondArray, SparkError>
where
    T: ArrowTemporalType + ArrowNumericType,
    i64: From<T::Native>,
{
    let data_type = array.data_type();
    timestamp_trunc_array_fmt_helper!(array, formats, data_type)
}
fn timestamp_trunc_array_fmt_plain_dict<T, K>(
    array: &PrimitiveArray<T>,
    formats: &TypedDictionaryArray<K, StringArray>,
) -> Result<TimestampMicrosecondArray, SparkError>
where
    T: ArrowTemporalType + ArrowNumericType,
    i64: From<T::Native>,
    K: ArrowDictionaryKeyType,
{
    let data_type = array.data_type();
    timestamp_trunc_array_fmt_helper!(array, formats, data_type)
}

fn timestamp_trunc_array_fmt_dict_plain<T, K>(
    array: &TypedDictionaryArray<K, PrimitiveArray<T>>,
    formats: &StringArray,
) -> Result<TimestampMicrosecondArray, SparkError>
where
    T: ArrowTemporalType + ArrowNumericType,
    i64: From<T::Native>,
    K: ArrowDictionaryKeyType,
{
    let data_type = array.values().data_type();
    timestamp_trunc_array_fmt_helper!(array, formats, data_type)
}

fn timestamp_trunc_array_fmt_dict_dict<T, K, F>(
    array: &TypedDictionaryArray<K, PrimitiveArray<T>>,
    formats: &TypedDictionaryArray<F, StringArray>,
) -> Result<TimestampMicrosecondArray, SparkError>
where
    T: ArrowTemporalType + ArrowNumericType,
    i64: From<T::Native>,
    K: ArrowDictionaryKeyType,
    F: ArrowDictionaryKeyType,
{
    let data_type = array.values().data_type();
    timestamp_trunc_array_fmt_helper!(array, formats, data_type)
}

#[cfg(test)]
mod tests {
    use crate::kernels::temporal::{
        date_trunc, date_trunc_array_fmt_dyn, timestamp_trunc, timestamp_trunc_array_fmt_dyn,
    };
    use arrow_array::{
        builder::{PrimitiveDictionaryBuilder, StringDictionaryBuilder},
        iterator::ArrayIter,
        types::{Date32Type, Int32Type, TimestampMicrosecondType},
        Array, Date32Array, PrimitiveArray, StringArray, TimestampMicrosecondArray,
    };
    use std::sync::Arc;

    #[test]
    #[cfg_attr(miri, ignore)] // test takes too long with miri
    fn test_date_trunc() {
        let size = 1000;
        let mut vec: Vec<i32> = Vec::with_capacity(size);
        for i in 0..size {
            vec.push(i as i32);
        }
        let array = Date32Array::from(vec);
        for fmt in [
            "YEAR", "YYYY", "YY", "QUARTER", "MONTH", "MON", "MM", "WEEK",
        ] {
            match date_trunc(&array, fmt.to_string()) {
                Ok(a) => {
                    for i in 0..size {
                        assert!(array.values().get(i) >= a.values().get(i))
                    }
                }
                _ => unreachable!(),
            }
        }
    }

    #[test]
    // This test only verifies that the various input array types work. Actually correctness to
    // ensure this produces the same results as spark is verified in the JVM tests
    fn test_date_trunc_array_fmt_dyn() {
        let size = 10;
        let formats = [
            "YEAR", "YYYY", "YY", "QUARTER", "MONTH", "MON", "MM", "WEEK",
        ];
        let mut vec: Vec<i32> = Vec::with_capacity(size * formats.len());
        let mut fmt_vec: Vec<&str> = Vec::with_capacity(size * formats.len());
        for i in 0..size {
            for fmt_value in &formats {
                vec.push(i as i32 * 1_000_001);
                fmt_vec.push(fmt_value);
            }
        }

        // timestamp array
        let array = Date32Array::from(vec);

        // formats array
        let fmt_array = StringArray::from(fmt_vec);

        // timestamp dictionary array
        let mut date_dict_builder = PrimitiveDictionaryBuilder::<Int32Type, Date32Type>::new();
        for v in array.iter() {
            date_dict_builder
                .append(v.unwrap())
                .expect("Error in building timestamp array");
        }
        let mut array_dict = date_dict_builder.finish();
        // apply timezone
        array_dict = array_dict.with_values(Arc::new(
            array_dict
                .values()
                .as_any()
                .downcast_ref::<Date32Array>()
                .unwrap()
                .clone(),
        ));

        // formats dictionary array
        let mut formats_dict_builder = StringDictionaryBuilder::<Int32Type>::new();
        for v in fmt_array.iter() {
            formats_dict_builder
                .append(v.unwrap())
                .expect("Error in building formats array");
        }
        let fmt_dict = formats_dict_builder.finish();

        // verify input arrays
        let iter = ArrayIter::new(&array);
        let mut dict_iter = array_dict
            .downcast_dict::<PrimitiveArray<Date32Type>>()
            .unwrap()
            .into_iter();
        for val in iter {
            assert_eq!(
                dict_iter
                    .next()
                    .expect("array and dictionary array do not match"),
                val
            )
        }

        // verify input format arrays
        let fmt_iter = ArrayIter::new(&fmt_array);
        let mut fmt_dict_iter = fmt_dict.downcast_dict::<StringArray>().unwrap().into_iter();
        for val in fmt_iter {
            assert_eq!(
                fmt_dict_iter
                    .next()
                    .expect("formats and dictionary formats do not match"),
                val
            )
        }

        // test cases
        if let Ok(a) = date_trunc_array_fmt_dyn(&array, &fmt_array) {
            for i in 0..array.len() {
                assert!(
                    array.value(i) >= a.as_any().downcast_ref::<Date32Array>().unwrap().value(i)
                )
            }
        } else {
            unreachable!()
        }
        if let Ok(a) = date_trunc_array_fmt_dyn(&array_dict, &fmt_array) {
            for i in 0..array.len() {
                assert!(
                    array.value(i) >= a.as_any().downcast_ref::<Date32Array>().unwrap().value(i)
                )
            }
        } else {
            unreachable!()
        }
        if let Ok(a) = date_trunc_array_fmt_dyn(&array, &fmt_dict) {
            for i in 0..array.len() {
                assert!(
                    array.value(i) >= a.as_any().downcast_ref::<Date32Array>().unwrap().value(i)
                )
            }
        } else {
            unreachable!()
        }
        if let Ok(a) = date_trunc_array_fmt_dyn(&array_dict, &fmt_dict) {
            for i in 0..array.len() {
                assert!(
                    array.value(i) >= a.as_any().downcast_ref::<Date32Array>().unwrap().value(i)
                )
            }
        } else {
            unreachable!()
        }
    }

    #[test]
    #[cfg_attr(miri, ignore)] // test takes too long with miri
    fn test_timestamp_trunc() {
        let size = 1000;
        let mut vec: Vec<i64> = Vec::with_capacity(size);
        for i in 0..size {
            vec.push(i as i64);
        }
        let array = TimestampMicrosecondArray::from(vec).with_timezone_utc();
        for fmt in [
            "YEAR",
            "YYYY",
            "YY",
            "QUARTER",
            "MONTH",
            "MON",
            "MM",
            "WEEK",
            "DAY",
            "DD",
            "HOUR",
            "MINUTE",
            "SECOND",
            "MILLISECOND",
            "MICROSECOND",
        ] {
            match timestamp_trunc(&array, fmt.to_string()) {
                Ok(a) => {
                    for i in 0..size {
                        assert!(array.values().get(i) >= a.values().get(i))
                    }
                }
                _ => unreachable!(),
            }
        }
    }

    #[test]
    // test takes too long with miri
    #[cfg_attr(miri, ignore)]
    // This test only verifies that the various input array types work. Actually correctness to
    // ensure this produces the same results as spark is verified in the JVM tests
    fn test_timestamp_trunc_array_fmt_dyn() {
        let size = 10;
        let formats = [
            "YEAR",
            "YYYY",
            "YY",
            "QUARTER",
            "MONTH",
            "MON",
            "MM",
            "WEEK",
            "DAY",
            "DD",
            "HOUR",
            "MINUTE",
            "SECOND",
            "MILLISECOND",
            "MICROSECOND",
        ];
        let mut vec: Vec<i64> = Vec::with_capacity(size * formats.len());
        let mut fmt_vec: Vec<&str> = Vec::with_capacity(size * formats.len());
        for i in 0..size {
            for fmt_value in &formats {
                vec.push(i as i64 * 1_000_000_001);
                fmt_vec.push(fmt_value);
            }
        }

        // timestamp array
        let array = TimestampMicrosecondArray::from(vec).with_timezone_utc();

        // formats array
        let fmt_array = StringArray::from(fmt_vec);

        // timestamp dictionary array
        let mut timestamp_dict_builder =
            PrimitiveDictionaryBuilder::<Int32Type, TimestampMicrosecondType>::new();
        for v in array.iter() {
            timestamp_dict_builder
                .append(v.unwrap())
                .expect("Error in building timestamp array");
        }
        let mut array_dict = timestamp_dict_builder.finish();
        // apply timezone
        array_dict = array_dict.with_values(Arc::new(
            array_dict
                .values()
                .as_any()
                .downcast_ref::<TimestampMicrosecondArray>()
                .unwrap()
                .clone()
                .with_timezone_utc(),
        ));

        // formats dictionary array
        let mut formats_dict_builder = StringDictionaryBuilder::<Int32Type>::new();
        for v in fmt_array.iter() {
            formats_dict_builder
                .append(v.unwrap())
                .expect("Error in building formats array");
        }
        let fmt_dict = formats_dict_builder.finish();

        // verify input arrays
        let iter = ArrayIter::new(&array);
        let mut dict_iter = array_dict
            .downcast_dict::<PrimitiveArray<TimestampMicrosecondType>>()
            .unwrap()
            .into_iter();
        for val in iter {
            assert_eq!(
                dict_iter
                    .next()
                    .expect("array and dictionary array do not match"),
                val
            )
        }

        // verify input format arrays
        let fmt_iter = ArrayIter::new(&fmt_array);
        let mut fmt_dict_iter = fmt_dict.downcast_dict::<StringArray>().unwrap().into_iter();
        for val in fmt_iter {
            assert_eq!(
                fmt_dict_iter
                    .next()
                    .expect("formats and dictionary formats do not match"),
                val
            )
        }

        // test cases
        if let Ok(a) = timestamp_trunc_array_fmt_dyn(&array, &fmt_array) {
            for i in 0..array.len() {
                assert!(
                    array.value(i)
                        >= a.as_any()
                            .downcast_ref::<TimestampMicrosecondArray>()
                            .unwrap()
                            .value(i)
                )
            }
        } else {
            unreachable!()
        }
        if let Ok(a) = timestamp_trunc_array_fmt_dyn(&array_dict, &fmt_array) {
            for i in 0..array.len() {
                assert!(
                    array.value(i)
                        >= a.as_any()
                            .downcast_ref::<TimestampMicrosecondArray>()
                            .unwrap()
                            .value(i)
                )
            }
        } else {
            unreachable!()
        }
        if let Ok(a) = timestamp_trunc_array_fmt_dyn(&array, &fmt_dict) {
            for i in 0..array.len() {
                assert!(
                    array.value(i)
                        >= a.as_any()
                            .downcast_ref::<TimestampMicrosecondArray>()
                            .unwrap()
                            .value(i)
                )
            }
        } else {
            unreachable!()
        }
        if let Ok(a) = timestamp_trunc_array_fmt_dyn(&array_dict, &fmt_dict) {
            for i in 0..array.len() {
                assert!(
                    array.value(i)
                        >= a.as_any()
                            .downcast_ref::<TimestampMicrosecondArray>()
                            .unwrap()
                            .value(i)
                )
            }
        } else {
            unreachable!()
        }
    }
}
