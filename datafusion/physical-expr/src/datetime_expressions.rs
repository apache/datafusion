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

//! DateTime expressions

use crate::expressions::cast_column;
use arrow::compute::cast;
use arrow::{
    array::{Array, ArrayRef, Float64Array, OffsetSizeTrait, PrimitiveArray},
    compute::kernels::cast_utils::string_to_timestamp_nanos,
    datatypes::{
        ArrowNumericType, ArrowPrimitiveType, ArrowTemporalType, DataType,
        IntervalDayTimeType, IntervalMonthDayNanoType, TimestampMicrosecondType,
        TimestampMillisecondType, TimestampNanosecondType, TimestampSecondType,
    },
};
use arrow::{
    compute::kernels::temporal,
    datatypes::TimeUnit,
    temporal_conversions::{as_datetime_with_timezone, timestamp_ns_to_datetime},
};
use arrow_array::temporal_conversions::NANOSECONDS;
use arrow_array::timezone::Tz;
use arrow_array::types::ArrowTimestampType;
use arrow_array::GenericStringArray;
use chrono::prelude::*;
use chrono::LocalResult::Single;
use chrono::{Duration, Months, NaiveDate};
use datafusion_common::cast::{
    as_date32_array, as_date64_array, as_generic_string_array, as_primitive_array,
    as_timestamp_microsecond_array, as_timestamp_millisecond_array,
    as_timestamp_nanosecond_array, as_timestamp_second_array,
};
use datafusion_common::{
    exec_err, internal_err, not_impl_err, DataFusionError, Result, ScalarType,
    ScalarValue,
};
use datafusion_expr::ColumnarValue;
use itertools::Either;
use std::str::FromStr;
use std::sync::Arc;

/// Error message if nanosecond conversion request beyond supported interval
const ERR_NANOSECONDS_NOT_SUPPORTED: &str = "The dates that can be represented as nanoseconds have to be between 1677-09-21T00:12:44.0 and 2262-04-11T23:47:16.854775804";

/// Accepts a string with a `chrono` format and converts it to a
/// nanosecond precision timestamp.
///
/// See [`chrono::format::strftime`] for the full set of supported formats.
///
/// Implements the `to_timestamp` function to convert a string to a
/// timestamp, following the model of spark SQL’s to_`timestamp`.
///
/// Internally, this function uses the `chrono` library for the
/// datetime parsing
///
/// ## Timestamp Precision
///
/// Function uses the maximum precision timestamps supported by
/// Arrow (nanoseconds stored as a 64-bit integer) timestamps. This
/// means the range of dates that timestamps can represent is ~1677 AD
/// to 2262 AM
///
/// ## Timezone / Offset Handling
///
/// Numerical values of timestamps are stored compared to offset UTC.
///
/// Any timestamp in the formatting string is handled according to the rules
/// defined by `chrono`.
///
/// [`chrono::format::strftime`]: https://docs.rs/chrono/latest/chrono/format/strftime/index.html
///
#[inline]
pub(crate) fn string_to_timestamp_nanos_formatted(
    s: &str,
    format: &str,
) -> Result<i64, DataFusionError> {
    string_to_datetime_formatted(&Utc, s, format)?
        .naive_utc()
        .timestamp_nanos_opt()
        .ok_or_else(|| {
            DataFusionError::Execution(ERR_NANOSECONDS_NOT_SUPPORTED.to_string())
        })
}

/// Accepts a string and parses it using the [`chrono::format::strftime`] specifiers
/// relative to the provided `timezone`
///
/// [IANA timezones] are only supported if the `arrow-array/chrono-tz` feature is enabled
///
/// * `2023-01-01 040506 America/Los_Angeles`
///
/// If a timestamp is ambiguous, for example as a result of daylight-savings time, an error
/// will be returned
///
/// [`chrono::format::strftime`]: https://docs.rs/chrono/latest/chrono/format/strftime/index.html
/// [IANA timezones]: https://www.iana.org/time-zones
pub(crate) fn string_to_datetime_formatted<T: TimeZone>(
    timezone: &T,
    s: &str,
    format: &str,
) -> Result<DateTime<T>, DataFusionError> {
    let err = |err_ctx: &str| {
        DataFusionError::Execution(format!(
            "Error parsing timestamp from '{s}' using format '{format}': {err_ctx}"
        ))
    };

    // attempt to parse the string assuming it has a timezone
    let dt = DateTime::parse_from_str(s, format);

    if let Err(e) = &dt {
        // no timezone or other failure, try without a timezone
        let ndt = NaiveDateTime::parse_from_str(s, format);
        if let Err(e) = &ndt {
            return Err(err(&e.to_string()));
        }

        if let Single(e) = &timezone.from_local_datetime(&ndt.unwrap()) {
            Ok(e.to_owned())
        } else {
            Err(err(&e.to_string()))
        }
    } else {
        Ok(dt.unwrap().with_timezone(timezone))
    }
}

/// given a function `op` that maps a `&str` to a Result of an arrow native type,
/// returns a `PrimitiveArray` after the application
/// of the function to `args[0]`.
/// # Errors
/// This function errors iff:
/// * the number of arguments is not 1 or
/// * the first argument is not castable to a `GenericStringArray` or
/// * the function `op` errors
pub(crate) fn unary_string_to_primitive_function<'a, T, O, F>(
    args: &[&'a dyn Array],
    op: F,
    name: &str,
) -> Result<PrimitiveArray<O>>
where
    O: ArrowPrimitiveType,
    T: OffsetSizeTrait,
    F: Fn(&'a str) -> Result<O::Native>,
{
    if args.len() != 1 {
        return internal_err!(
            "{:?} args were supplied but {} takes exactly one argument",
            args.len(),
            name
        );
    }

    let array = as_generic_string_array::<T>(args[0])?;

    // first map is the iterator, second is for the `Option<_>`
    array.iter().map(|x| x.map(&op).transpose()).collect()
}

/// given a function `op` that maps `&str`, `&str` to the first successful Result
/// of an arrow native type, returns a `PrimitiveArray` after the application of the
/// function to `args` and the subsequence application of the `op2` function to any
/// successful result. This function calls the `op` function with the first and second
/// argument and if not successful continues with first and third, first and fourth,
/// etc until the result was successful or no more arguments are present.
/// # Errors
/// This function errors iff:
/// * the number of arguments is not > 1 or
/// * the array arguments are not castable to a `GenericStringArray` or
/// * the function `op` errors for all input
pub(crate) fn strings_to_primitive_function<'a, T, O, F, F2>(
    args: &'a [ColumnarValue],
    op: F,
    op2: F2,
    name: &str,
) -> Result<PrimitiveArray<O>>
where
    O: ArrowPrimitiveType,
    T: OffsetSizeTrait,
    F: Fn(&'a str, &'a str) -> Result<O::Native>,
    F2: Fn(O::Native) -> O::Native,
{
    if args.len() < 2 {
        return internal_err!(
            "{:?} args were supplied but {} takes 2 or more arguments",
            args.len(),
            name
        );
    }

    // this will throw the error if any of the array args are not castable to GenericStringArray
    let data = args
        .iter()
        .map(|a| match a {
            ColumnarValue::Array(a) => {
                Ok(Either::Left(as_generic_string_array::<T>(a.as_ref())?))
            }
            ColumnarValue::Scalar(s) => match s {
                ScalarValue::Utf8(a) | ScalarValue::LargeUtf8(a) => Ok(Either::Right(a)),
                other => internal_err!(
                    "Unexpected scalar type encountered '{other}' for function '{name}'"
                ),
            },
        })
        .collect::<Result<Vec<Either<&GenericStringArray<T>, &Option<String>>>>>()?;

    let first_arg = &data.first().unwrap().left().unwrap();

    first_arg
        .iter()
        .enumerate()
        .map(|(pos, x)| {
            let mut val = None;

            if let Some(x) = x {
                let param_args = data.iter().skip(1);

                // go through the args and find the first successful result. Only the last
                // failure will be returned if no successful result was received.
                for param_arg in param_args {
                    // param_arg is an array, use the corresponding index into the array as the arg
                    // we're currently parsing
                    let p = *param_arg;
                    let r = if p.is_left() {
                        let p = p.left().unwrap();
                        op(x, p.value(pos))
                    }
                    // args is a scalar, use it directly
                    else if let Some(p) = p.right().unwrap() {
                        op(x, p.as_str())
                    } else {
                        continue;
                    };

                    if r.is_ok() {
                        val = Some(Ok(op2(r.unwrap())));
                        break;
                    } else {
                        val = Some(r);
                    }
                }
            };

            val.transpose()
        })
        .collect()
}

// given an function that maps a `&str` to an arrow native type,
// returns a `ColumnarValue` where the function is applied to either a `ArrayRef` or `ScalarValue`
// depending on the `args`'s variant.
fn handle<'a, O, F, S>(
    args: &'a [ColumnarValue],
    op: F,
    name: &str,
) -> Result<ColumnarValue>
where
    O: ArrowPrimitiveType,
    S: ScalarType<O::Native>,
    F: Fn(&'a str) -> Result<O::Native>,
{
    match &args[0] {
        ColumnarValue::Array(a) => match a.data_type() {
            DataType::Utf8 | DataType::LargeUtf8 => Ok(ColumnarValue::Array(Arc::new(
                unary_string_to_primitive_function::<i32, O, _>(&[a.as_ref()], op, name)?,
            ))),
            other => exec_err!("Unsupported data type {other:?} for function {name}"),
        },
        ColumnarValue::Scalar(scalar) => match scalar {
            ScalarValue::Utf8(a) | ScalarValue::LargeUtf8(a) => {
                let result = a.as_ref().map(|x| (op)(x)).transpose()?;
                Ok(ColumnarValue::Scalar(S::scalar(result)))
            }
            other => exec_err!("Unsupported data type {other:?} for function {name}"),
        },
    }
}

// given an function that maps a `&str`, `&str` to an arrow native type,
// returns a `ColumnarValue` where the function is applied to either a `ArrayRef` or `ScalarValue`
// depending on the `args`'s variant.
fn handle_multiple<'a, O, F, S, M>(
    args: &'a [ColumnarValue],
    op: F,
    op2: M,
    name: &str,
) -> Result<ColumnarValue>
where
    O: ArrowPrimitiveType,
    S: ScalarType<O::Native>,
    F: Fn(&'a str, &'a str) -> Result<O::Native>,
    M: Fn(O::Native) -> O::Native,
{
    match &args[0] {
        ColumnarValue::Array(a) => match a.data_type() {
            DataType::Utf8 | DataType::LargeUtf8 => {
                // validate the column types
                for (pos, arg) in args.iter().enumerate() {
                    match arg {
                        ColumnarValue::Array(arg) => match arg.data_type() {
                                DataType::Utf8 | DataType::LargeUtf8 => {
                                    // all good
                                },
                                other => return exec_err!("Unsupported data type {other:?} for function {name}, arg # {pos}"),
                            },
                        ColumnarValue::Scalar(arg) => { match arg.data_type() {
                            DataType::Utf8 | DataType::LargeUtf8 => {
                                // all good
                            },
                            other => return exec_err!("Unsupported data type {other:?} for function {name}, arg # {pos}"),
                        }}
                    }
                }

                Ok(ColumnarValue::Array(Arc::new(
                    strings_to_primitive_function::<i32, O, _, _>(args, op, op2, name)?,
                )))
            }
            other => {
                exec_err!("Unsupported data type {other:?} for function {name}")
            }
        },
        // if the first argument is a scalar utf8 all arguments are expected to be scalar utf8
        ColumnarValue::Scalar(scalar) => match scalar {
            ScalarValue::Utf8(a) | ScalarValue::LargeUtf8(a) => {
                let mut val: Option<Result<ColumnarValue>> = None;
                let mut err: Option<DataFusionError> = None;

                match a {
                    Some(a) => {
                        // enumerate all the values finding the first one that returns an Ok result
                        for (pos, v) in args.iter().enumerate().skip(1) {
                            if let ColumnarValue::Scalar(s) = v {
                                if let ScalarValue::Utf8(x) | ScalarValue::LargeUtf8(x) =
                                    s
                                {
                                    if let Some(s) = x {
                                        match op(a.as_str(), s.as_str()) {
                                            Ok(r) => {
                                                val = Some(Ok(ColumnarValue::Scalar(
                                                    S::scalar(Some(op2(r))),
                                                )));
                                                break;
                                            }
                                            Err(e) => {
                                                err = Some(e);
                                            }
                                        }
                                    }
                                } else {
                                    return exec_err!("Unsupported data type {s:?} for function {name}, arg # {pos}");
                                }
                            } else {
                                return exec_err!("Unsupported data type {v:?} for function {name}, arg # {pos}");
                            }
                        }
                    }
                    None => (),
                }

                if let Some(v) = val {
                    v
                } else {
                    Err(err.unwrap())
                }
            }
            other => {
                exec_err!("Unsupported data type {other:?} for function {name}")
            }
        },
    }
}

/// Calls string_to_timestamp_nanos and converts the error type
fn string_to_timestamp_nanos_shim(s: &str) -> Result<i64> {
    string_to_timestamp_nanos(s).map_err(|e| e.into())
}

fn to_timestamp_impl<T: ArrowTimestampType + ScalarType<i64>>(
    args: &[ColumnarValue],
    name: &str,
) -> Result<ColumnarValue> {
    let factor = match T::UNIT {
        TimeUnit::Second => 1_000_000_000,
        TimeUnit::Millisecond => 1_000_000,
        TimeUnit::Microsecond => 1_000,
        TimeUnit::Nanosecond => 1,
    };

    match args.len() {
        1 => handle::<T, _, T>(
            args,
            |s| string_to_timestamp_nanos_shim(s).map(|n| n / factor),
            name,
        ),
        n if n >= 2 => handle_multiple::<T, _, T, _>(
            args,
            string_to_timestamp_nanos_formatted,
            |n| n / factor,
            name,
        ),
        _ => internal_err!("Unsupported 0 argument count for function {name}"),
    }
}

/// to_timestamp SQL function
///
/// Note: `to_timestamp` returns `Timestamp(Nanosecond)` though its arguments are interpreted as **seconds**.
/// The supported range for integer input is between `-9223372037` and `9223372036`.
/// Supported range for string input is between `1677-09-21T00:12:44.0` and `2262-04-11T23:47:16.0`.
/// Please use `to_timestamp_seconds` for the input outside of supported bounds.
pub fn to_timestamp(args: &[ColumnarValue]) -> Result<ColumnarValue> {
    to_timestamp_impl::<TimestampNanosecondType>(args, "to_timestamp")
}

/// to_timestamp_millis SQL function
pub fn to_timestamp_millis(args: &[ColumnarValue]) -> Result<ColumnarValue> {
    to_timestamp_impl::<TimestampMillisecondType>(args, "to_timestamp_millis")
}

/// to_timestamp_micros SQL function
pub fn to_timestamp_micros(args: &[ColumnarValue]) -> Result<ColumnarValue> {
    to_timestamp_impl::<TimestampMicrosecondType>(args, "to_timestamp_micros")
}

/// to_timestamp_nanos SQL function
pub fn to_timestamp_nanos(args: &[ColumnarValue]) -> Result<ColumnarValue> {
    to_timestamp_impl::<TimestampNanosecondType>(args, "to_timestamp_nanos")
}

/// to_timestamp_seconds SQL function
pub fn to_timestamp_seconds(args: &[ColumnarValue]) -> Result<ColumnarValue> {
    to_timestamp_impl::<TimestampSecondType>(args, "to_timestamp_seconds")
}

/// Create an implementation of `now()` that always returns the
/// specified timestamp.
///
/// The semantics of `now()` require it to return the same value
/// wherever it appears within a single statement. This value is
/// chosen during planning time.
pub fn make_now(
    now_ts: DateTime<Utc>,
) -> impl Fn(&[ColumnarValue]) -> Result<ColumnarValue> {
    let now_ts = now_ts.timestamp_nanos_opt();
    move |_arg| {
        Ok(ColumnarValue::Scalar(ScalarValue::TimestampNanosecond(
            now_ts,
            Some("+00:00".into()),
        )))
    }
}

/// Create an implementation of `current_date()` that always returns the
/// specified current date.
///
/// The semantics of `current_date()` require it to return the same value
/// wherever it appears within a single statement. This value is
/// chosen during planning time.
pub fn make_current_date(
    now_ts: DateTime<Utc>,
) -> impl Fn(&[ColumnarValue]) -> Result<ColumnarValue> {
    let days = Some(
        now_ts.num_days_from_ce()
            - NaiveDate::from_ymd_opt(1970, 1, 1)
                .unwrap()
                .num_days_from_ce(),
    );
    move |_arg| Ok(ColumnarValue::Scalar(ScalarValue::Date32(days)))
}

/// Create an implementation of `current_time()` that always returns the
/// specified current time.
///
/// The semantics of `current_time()` require it to return the same value
/// wherever it appears within a single statement. This value is
/// chosen during planning time.
pub fn make_current_time(
    now_ts: DateTime<Utc>,
) -> impl Fn(&[ColumnarValue]) -> Result<ColumnarValue> {
    let nano = now_ts.timestamp_nanos_opt().map(|ts| ts % 86400000000000);
    move |_arg| Ok(ColumnarValue::Scalar(ScalarValue::Time64Nanosecond(nano)))
}

fn quarter_month<T>(date: &T) -> u32
where
    T: chrono::Datelike,
{
    1 + 3 * ((date.month() - 1) / 3)
}

fn _date_trunc_coarse<T>(granularity: &str, value: Option<T>) -> Result<Option<T>>
where
    T: chrono::Datelike
        + chrono::Timelike
        + std::ops::Sub<chrono::Duration, Output = T>
        + std::marker::Copy,
{
    let value = match granularity {
        "millisecond" => value,
        "microsecond" => value,
        "second" => value.and_then(|d| d.with_nanosecond(0)),
        "minute" => value
            .and_then(|d| d.with_nanosecond(0))
            .and_then(|d| d.with_second(0)),
        "hour" => value
            .and_then(|d| d.with_nanosecond(0))
            .and_then(|d| d.with_second(0))
            .and_then(|d| d.with_minute(0)),
        "day" => value
            .and_then(|d| d.with_nanosecond(0))
            .and_then(|d| d.with_second(0))
            .and_then(|d| d.with_minute(0))
            .and_then(|d| d.with_hour(0)),
        "week" => value
            .and_then(|d| d.with_nanosecond(0))
            .and_then(|d| d.with_second(0))
            .and_then(|d| d.with_minute(0))
            .and_then(|d| d.with_hour(0))
            .map(|d| d - Duration::seconds(60 * 60 * 24 * d.weekday() as i64)),
        "month" => value
            .and_then(|d| d.with_nanosecond(0))
            .and_then(|d| d.with_second(0))
            .and_then(|d| d.with_minute(0))
            .and_then(|d| d.with_hour(0))
            .and_then(|d| d.with_day0(0)),
        "quarter" => value
            .and_then(|d| d.with_nanosecond(0))
            .and_then(|d| d.with_second(0))
            .and_then(|d| d.with_minute(0))
            .and_then(|d| d.with_hour(0))
            .and_then(|d| d.with_day0(0))
            .and_then(|d| d.with_month(quarter_month(&d))),
        "year" => value
            .and_then(|d| d.with_nanosecond(0))
            .and_then(|d| d.with_second(0))
            .and_then(|d| d.with_minute(0))
            .and_then(|d| d.with_hour(0))
            .and_then(|d| d.with_day0(0))
            .and_then(|d| d.with_month0(0)),
        unsupported => {
            return exec_err!("Unsupported date_trunc granularity: {unsupported}");
        }
    };
    Ok(value)
}

fn _date_trunc_coarse_with_tz(
    granularity: &str,
    value: Option<DateTime<Tz>>,
) -> Result<Option<i64>> {
    let value = _date_trunc_coarse::<DateTime<Tz>>(granularity, value)?;
    Ok(value.and_then(|value| value.timestamp_nanos_opt()))
}

fn _date_trunc_coarse_without_tz(
    granularity: &str,
    value: Option<NaiveDateTime>,
) -> Result<Option<i64>> {
    let value = _date_trunc_coarse::<NaiveDateTime>(granularity, value)?;
    Ok(value.and_then(|value| value.timestamp_nanos_opt()))
}

/// Tuncates the single `value`, expressed in nanoseconds since the
/// epoch, for granularities greater than 1 second, in taking into
/// account that some granularities are not uniform durations of time
/// (e.g. months are not always the same lengths, leap seconds, etc)
fn date_trunc_coarse(granularity: &str, value: i64, tz: Option<Tz>) -> Result<i64> {
    let value = match tz {
        Some(tz) => {
            // Use chrono DateTime<Tz> to clear the various fields because need to clear per timezone,
            // and NaiveDateTime (ISO 8601) has no concept of timezones
            let value = as_datetime_with_timezone::<TimestampNanosecondType>(value, tz)
                .ok_or(DataFusionError::Execution(format!(
                "Timestamp {value} out of range"
            )))?;
            _date_trunc_coarse_with_tz(granularity, Some(value))
        }
        None => {
            // Use chrono NaiveDateTime to clear the various fields, if we don't have a timezone.
            let value = timestamp_ns_to_datetime(value).ok_or_else(|| {
                DataFusionError::Execution(format!("Timestamp {value} out of range"))
            })?;
            _date_trunc_coarse_without_tz(granularity, Some(value))
        }
    }?;

    // `with_x(0)` are infallible because `0` are always a valid
    Ok(value.unwrap())
}

// truncates a single value with the given timeunit to the specified granularity
fn general_date_trunc(
    tu: TimeUnit,
    value: &Option<i64>,
    tz: Option<Tz>,
    granularity: &str,
) -> Result<Option<i64>, DataFusionError> {
    let scale = match tu {
        TimeUnit::Second => 1_000_000_000,
        TimeUnit::Millisecond => 1_000_000,
        TimeUnit::Microsecond => 1_000,
        TimeUnit::Nanosecond => 1,
    };

    let Some(value) = value else {
        return Ok(None);
    };

    // convert to nanoseconds
    let nano = date_trunc_coarse(granularity, scale * value, tz)?;

    let result = match tu {
        TimeUnit::Second => match granularity {
            "minute" => Some(nano / 1_000_000_000 / 60 * 60),
            _ => Some(nano / 1_000_000_000),
        },
        TimeUnit::Millisecond => match granularity {
            "minute" => Some(nano / 1_000_000 / 1_000 / 60 * 1_000 * 60),
            "second" => Some(nano / 1_000_000 / 1_000 * 1_000),
            _ => Some(nano / 1_000_000),
        },
        TimeUnit::Microsecond => match granularity {
            "minute" => Some(nano / 1_000 / 1_000_000 / 60 * 60 * 1_000_000),
            "second" => Some(nano / 1_000 / 1_000_000 * 1_000_000),
            "millisecond" => Some(nano / 1_000 / 1_000 * 1_000),
            _ => Some(nano / 1_000),
        },
        _ => match granularity {
            "minute" => Some(nano / 1_000_000_000 / 60 * 1_000_000_000 * 60),
            "second" => Some(nano / 1_000_000_000 * 1_000_000_000),
            "millisecond" => Some(nano / 1_000_000 * 1_000_000),
            "microsecond" => Some(nano / 1_000 * 1_000),
            _ => Some(nano),
        },
    };
    Ok(result)
}

fn parse_tz(tz: &Option<Arc<str>>) -> Result<Option<Tz>> {
    tz.as_ref()
        .map(|tz| {
            Tz::from_str(tz).map_err(|op| {
                DataFusionError::Execution(format!("failed on timezone {tz}: {:?}", op))
            })
        })
        .transpose()
}

/// date_trunc SQL function
pub fn date_trunc(args: &[ColumnarValue]) -> Result<ColumnarValue> {
    let (granularity, array) = (&args[0], &args[1]);

    let granularity =
        if let ColumnarValue::Scalar(ScalarValue::Utf8(Some(v))) = granularity {
            v.to_lowercase()
        } else {
            return exec_err!("Granularity of `date_trunc` must be non-null scalar Utf8");
        };

    fn process_array<T: ArrowTimestampType>(
        array: &dyn Array,
        granularity: String,
        tz_opt: &Option<Arc<str>>,
    ) -> Result<ColumnarValue> {
        let parsed_tz = parse_tz(tz_opt)?;
        let array = as_primitive_array::<T>(array)?;
        let array = array
            .iter()
            .map(|x| general_date_trunc(T::UNIT, &x, parsed_tz, granularity.as_str()))
            .collect::<Result<PrimitiveArray<T>>>()?
            .with_timezone_opt(tz_opt.clone());
        Ok(ColumnarValue::Array(Arc::new(array)))
    }

    fn process_scalar<T: ArrowTimestampType>(
        v: &Option<i64>,
        granularity: String,
        tz_opt: &Option<Arc<str>>,
    ) -> Result<ColumnarValue> {
        let parsed_tz = parse_tz(tz_opt)?;
        let value = general_date_trunc(T::UNIT, v, parsed_tz, granularity.as_str())?;
        let value = ScalarValue::new_timestamp::<T>(value, tz_opt.clone());
        Ok(ColumnarValue::Scalar(value))
    }

    Ok(match array {
        ColumnarValue::Scalar(ScalarValue::TimestampNanosecond(v, tz_opt)) => {
            process_scalar::<TimestampNanosecondType>(v, granularity, tz_opt)?
        }
        ColumnarValue::Scalar(ScalarValue::TimestampMicrosecond(v, tz_opt)) => {
            process_scalar::<TimestampMicrosecondType>(v, granularity, tz_opt)?
        }
        ColumnarValue::Scalar(ScalarValue::TimestampMillisecond(v, tz_opt)) => {
            process_scalar::<TimestampMillisecondType>(v, granularity, tz_opt)?
        }
        ColumnarValue::Scalar(ScalarValue::TimestampSecond(v, tz_opt)) => {
            process_scalar::<TimestampSecondType>(v, granularity, tz_opt)?
        }
        ColumnarValue::Array(array) => {
            let array_type = array.data_type();
            match array_type {
                DataType::Timestamp(TimeUnit::Second, tz_opt) => {
                    process_array::<TimestampSecondType>(array, granularity, tz_opt)?
                }
                DataType::Timestamp(TimeUnit::Millisecond, tz_opt) => {
                    process_array::<TimestampMillisecondType>(array, granularity, tz_opt)?
                }
                DataType::Timestamp(TimeUnit::Microsecond, tz_opt) => {
                    process_array::<TimestampMicrosecondType>(array, granularity, tz_opt)?
                }
                DataType::Timestamp(TimeUnit::Nanosecond, tz_opt) => {
                    process_array::<TimestampNanosecondType>(array, granularity, tz_opt)?
                }
                _ => process_array::<TimestampNanosecondType>(array, granularity, &None)?,
            }
        }
        _ => {
            return exec_err!(
                "second argument of `date_trunc` must be nanosecond timestamp scalar or array"
            );
        }
    })
}

// return time in nanoseconds that the source timestamp falls into based on the stride and origin
fn date_bin_nanos_interval(stride_nanos: i64, source: i64, origin: i64) -> i64 {
    let time_diff = source - origin;

    // distance from origin to bin
    let time_delta = compute_distance(time_diff, stride_nanos);

    origin + time_delta
}

// distance from origin to bin
fn compute_distance(time_diff: i64, stride: i64) -> i64 {
    let time_delta = time_diff - (time_diff % stride);

    if time_diff < 0 && stride > 1 {
        // The origin is later than the source timestamp, round down to the previous bin
        time_delta - stride
    } else {
        time_delta
    }
}

// return time in nanoseconds that the source timestamp falls into based on the stride and origin
fn date_bin_months_interval(stride_months: i64, source: i64, origin: i64) -> i64 {
    // convert source and origin to DateTime<Utc>
    let source_date = to_utc_date_time(source);
    let origin_date = to_utc_date_time(origin);

    // calculate the number of months between the source and origin
    let month_diff = (source_date.year() - origin_date.year()) * 12
        + source_date.month() as i32
        - origin_date.month() as i32;

    // distance from origin to bin
    let month_delta = compute_distance(month_diff as i64, stride_months);

    let mut bin_time = if month_delta < 0 {
        origin_date - Months::new(month_delta.unsigned_abs() as u32)
    } else {
        origin_date + Months::new(month_delta as u32)
    };

    // If origin is not midnight of first date of the month, the bin_time may be larger than the source
    // In this case, we need to move back to previous bin
    if bin_time > source_date {
        let month_delta = month_delta - stride_months;
        bin_time = if month_delta < 0 {
            origin_date - Months::new(month_delta.unsigned_abs() as u32)
        } else {
            origin_date + Months::new(month_delta as u32)
        };
    }

    bin_time.timestamp_nanos_opt().unwrap()
}

fn to_utc_date_time(nanos: i64) -> DateTime<Utc> {
    let secs = nanos / 1_000_000_000;
    let nsec = (nanos % 1_000_000_000) as u32;
    let date = NaiveDateTime::from_timestamp_opt(secs, nsec).unwrap();
    DateTime::<Utc>::from_naive_utc_and_offset(date, Utc)
}

/// DATE_BIN sql function
pub fn date_bin(args: &[ColumnarValue]) -> Result<ColumnarValue> {
    if args.len() == 2 {
        // Default to unix EPOCH
        let origin = ColumnarValue::Scalar(ScalarValue::TimestampNanosecond(
            Some(0),
            Some("+00:00".into()),
        ));
        date_bin_impl(&args[0], &args[1], &origin)
    } else if args.len() == 3 {
        date_bin_impl(&args[0], &args[1], &args[2])
    } else {
        exec_err!("DATE_BIN expected two or three arguments")
    }
}

enum Interval {
    Nanoseconds(i64),
    Months(i64),
}

impl Interval {
    /// Returns (`stride_nanos`, `fn`) where
    ///
    /// 1. `stride_nanos` is a width, in nanoseconds
    /// 2. `fn` is a function that takes (stride_nanos, source, origin)
    ///
    /// `source` is the timestamp being binned
    ///
    /// `origin`  is the time, in nanoseconds, where windows are measured from
    fn bin_fn(&self) -> (i64, fn(i64, i64, i64) -> i64) {
        match self {
            Interval::Nanoseconds(nanos) => (*nanos, date_bin_nanos_interval),
            Interval::Months(months) => (*months, date_bin_months_interval),
        }
    }
}

// Supported intervals:
//  1. IntervalDayTime: this means that the stride is in days, hours, minutes, seconds and milliseconds
//     We will assume month interval won't be converted into this type
//     TODO (my next PR): without `INTERVAL` keyword, the stride was converted into ScalarValue::IntervalDayTime somwhere
//             for month interval. I need to find that and make it ScalarValue::IntervalMonthDayNano instead
// 2. IntervalMonthDayNano
fn date_bin_impl(
    stride: &ColumnarValue,
    array: &ColumnarValue,
    origin: &ColumnarValue,
) -> Result<ColumnarValue> {
    let stride = match stride {
        ColumnarValue::Scalar(ScalarValue::IntervalDayTime(Some(v))) => {
            let (days, ms) = IntervalDayTimeType::to_parts(*v);
            let nanos = (Duration::days(days as i64) + Duration::milliseconds(ms as i64))
                .num_nanoseconds();

            match nanos {
                Some(v) => Interval::Nanoseconds(v),
                _ => return exec_err!("DATE_BIN stride argument is too large"),
            }
        }
        ColumnarValue::Scalar(ScalarValue::IntervalMonthDayNano(Some(v))) => {
            let (months, days, nanos) = IntervalMonthDayNanoType::to_parts(*v);

            // If interval is months, its origin must be midnight of first date of the month
            if months != 0 {
                // Return error if days or nanos is not zero
                if days != 0 || nanos != 0 {
                    return not_impl_err!(
                        "DATE_BIN stride does not support combination of month, day and nanosecond intervals"
                    );
                } else {
                    Interval::Months(months as i64)
                }
            } else {
                let nanos = (Duration::days(days as i64) + Duration::nanoseconds(nanos))
                    .num_nanoseconds();
                match nanos {
                    Some(v) => Interval::Nanoseconds(v),
                    _ => return exec_err!("DATE_BIN stride argument is too large"),
                }
            }
        }
        ColumnarValue::Scalar(v) => {
            return exec_err!(
                "DATE_BIN expects stride argument to be an INTERVAL but got {}",
                v.data_type()
            )
        }
        ColumnarValue::Array(_) => {
            return not_impl_err!(
            "DATE_BIN only supports literal values for the stride argument, not arrays"
        )
        }
    };

    let origin = match origin {
        ColumnarValue::Scalar(ScalarValue::TimestampNanosecond(Some(v), _)) => *v,
        ColumnarValue::Scalar(v) => {
            return exec_err!(
                "DATE_BIN expects origin argument to be a TIMESTAMP with nanosececond precision but got {}",
                v.data_type()
            )
        }
        ColumnarValue::Array(_) => return not_impl_err!(
            "DATE_BIN only supports literal values for the origin argument, not arrays"
        ),
    };

    let (stride, stride_fn) = stride.bin_fn();

    // Return error if stride is 0
    if stride == 0 {
        return exec_err!("DATE_BIN stride must be non-zero");
    }

    fn stride_map_fn<T: ArrowTimestampType>(
        origin: i64,
        stride: i64,
        stride_fn: fn(i64, i64, i64) -> i64,
    ) -> impl Fn(Option<i64>) -> Option<i64> {
        let scale = match T::UNIT {
            TimeUnit::Nanosecond => 1,
            TimeUnit::Microsecond => NANOSECONDS / 1_000_000,
            TimeUnit::Millisecond => NANOSECONDS / 1_000,
            TimeUnit::Second => NANOSECONDS,
        };
        move |x: Option<i64>| x.map(|x| stride_fn(stride, x * scale, origin) / scale)
    }

    Ok(match array {
        ColumnarValue::Scalar(ScalarValue::TimestampNanosecond(v, tz_opt)) => {
            let apply_stride_fn =
                stride_map_fn::<TimestampNanosecondType>(origin, stride, stride_fn);
            ColumnarValue::Scalar(ScalarValue::TimestampNanosecond(
                apply_stride_fn(*v),
                tz_opt.clone(),
            ))
        }
        ColumnarValue::Scalar(ScalarValue::TimestampMicrosecond(v, tz_opt)) => {
            let apply_stride_fn =
                stride_map_fn::<TimestampMicrosecondType>(origin, stride, stride_fn);
            ColumnarValue::Scalar(ScalarValue::TimestampMicrosecond(
                apply_stride_fn(*v),
                tz_opt.clone(),
            ))
        }
        ColumnarValue::Scalar(ScalarValue::TimestampMillisecond(v, tz_opt)) => {
            let apply_stride_fn =
                stride_map_fn::<TimestampMillisecondType>(origin, stride, stride_fn);
            ColumnarValue::Scalar(ScalarValue::TimestampMillisecond(
                apply_stride_fn(*v),
                tz_opt.clone(),
            ))
        }
        ColumnarValue::Scalar(ScalarValue::TimestampSecond(v, tz_opt)) => {
            let apply_stride_fn =
                stride_map_fn::<TimestampSecondType>(origin, stride, stride_fn);
            ColumnarValue::Scalar(ScalarValue::TimestampSecond(
                apply_stride_fn(*v),
                tz_opt.clone(),
            ))
        }

        ColumnarValue::Array(array) => {
            fn transform_array_with_stride<T>(
                origin: i64,
                stride: i64,
                stride_fn: fn(i64, i64, i64) -> i64,
                array: &ArrayRef,
                tz_opt: &Option<Arc<str>>,
            ) -> Result<ColumnarValue>
            where
                T: ArrowTimestampType,
            {
                let array = as_primitive_array::<T>(array)?;
                let apply_stride_fn = stride_map_fn::<T>(origin, stride, stride_fn);
                let array = array
                    .iter()
                    .map(apply_stride_fn)
                    .collect::<PrimitiveArray<T>>()
                    .with_timezone_opt(tz_opt.clone());

                Ok(ColumnarValue::Array(Arc::new(array)))
            }
            match array.data_type() {
                DataType::Timestamp(TimeUnit::Nanosecond, tz_opt) => {
                    transform_array_with_stride::<TimestampNanosecondType>(
                        origin, stride, stride_fn, array, tz_opt,
                    )?
                }
                DataType::Timestamp(TimeUnit::Microsecond, tz_opt) => {
                    transform_array_with_stride::<TimestampMicrosecondType>(
                        origin, stride, stride_fn, array, tz_opt,
                    )?
                }
                DataType::Timestamp(TimeUnit::Millisecond, tz_opt) => {
                    transform_array_with_stride::<TimestampMillisecondType>(
                        origin, stride, stride_fn, array, tz_opt,
                    )?
                }
                DataType::Timestamp(TimeUnit::Second, tz_opt) => {
                    transform_array_with_stride::<TimestampSecondType>(
                        origin, stride, stride_fn, array, tz_opt,
                    )?
                }
                _ => {
                    return exec_err!(
                        "DATE_BIN expects source argument to be a TIMESTAMP but got {}",
                        array.data_type()
                    )
                }
            }
        }
        _ => {
            return exec_err!(
                "DATE_BIN expects source argument to be a TIMESTAMP scalar or array"
            );
        }
    })
}

macro_rules! extract_date_part {
    ($ARRAY: expr, $FN:expr) => {
        match $ARRAY.data_type() {
            DataType::Date32 => {
                let array = as_date32_array($ARRAY)?;
                Ok($FN(array)
                    .map(|v| cast(&(Arc::new(v) as ArrayRef), &DataType::Float64))?)
            }
            DataType::Date64 => {
                let array = as_date64_array($ARRAY)?;
                Ok($FN(array)
                    .map(|v| cast(&(Arc::new(v) as ArrayRef), &DataType::Float64))?)
            }
            DataType::Timestamp(time_unit, _) => match time_unit {
                TimeUnit::Second => {
                    let array = as_timestamp_second_array($ARRAY)?;
                    Ok($FN(array)
                        .map(|v| cast(&(Arc::new(v) as ArrayRef), &DataType::Float64))?)
                }
                TimeUnit::Millisecond => {
                    let array = as_timestamp_millisecond_array($ARRAY)?;
                    Ok($FN(array)
                        .map(|v| cast(&(Arc::new(v) as ArrayRef), &DataType::Float64))?)
                }
                TimeUnit::Microsecond => {
                    let array = as_timestamp_microsecond_array($ARRAY)?;
                    Ok($FN(array)
                        .map(|v| cast(&(Arc::new(v) as ArrayRef), &DataType::Float64))?)
                }
                TimeUnit::Nanosecond => {
                    let array = as_timestamp_nanosecond_array($ARRAY)?;
                    Ok($FN(array)
                        .map(|v| cast(&(Arc::new(v) as ArrayRef), &DataType::Float64))?)
                }
            },
            datatype => internal_err!("Extract does not support datatype {:?}", datatype),
        }
    };
}

/// DATE_PART SQL function
pub fn date_part(args: &[ColumnarValue]) -> Result<ColumnarValue> {
    if args.len() != 2 {
        return exec_err!("Expected two arguments in DATE_PART");
    }
    let (date_part, array) = (&args[0], &args[1]);

    let date_part = if let ColumnarValue::Scalar(ScalarValue::Utf8(Some(v))) = date_part {
        v
    } else {
        return exec_err!("First argument of `DATE_PART` must be non-null scalar Utf8");
    };

    let is_scalar = matches!(array, ColumnarValue::Scalar(_));

    let array = match array {
        ColumnarValue::Array(array) => array.clone(),
        ColumnarValue::Scalar(scalar) => scalar.to_array()?,
    };

    let arr = match date_part.to_lowercase().as_str() {
        "year" => extract_date_part!(&array, temporal::year),
        "quarter" => extract_date_part!(&array, temporal::quarter),
        "month" => extract_date_part!(&array, temporal::month),
        "week" => extract_date_part!(&array, temporal::week),
        "day" => extract_date_part!(&array, temporal::day),
        "doy" => extract_date_part!(&array, temporal::doy),
        "dow" => extract_date_part!(&array, temporal::num_days_from_sunday),
        "hour" => extract_date_part!(&array, temporal::hour),
        "minute" => extract_date_part!(&array, temporal::minute),
        "second" => extract_date_part!(&array, seconds),
        "millisecond" => extract_date_part!(&array, millis),
        "microsecond" => extract_date_part!(&array, micros),
        "nanosecond" => extract_date_part!(&array, nanos),
        "epoch" => extract_date_part!(&array, epoch),
        _ => exec_err!("Date part '{date_part}' not supported"),
    }?;

    Ok(if is_scalar {
        ColumnarValue::Scalar(ScalarValue::try_from_array(&arr?, 0)?)
    } else {
        ColumnarValue::Array(arr?)
    })
}

fn to_ticks<T>(array: &PrimitiveArray<T>, frac: i32) -> Result<Float64Array>
where
    T: ArrowTemporalType + ArrowNumericType,
    i64: From<T::Native>,
{
    let zipped = temporal::second(array)?
        .values()
        .iter()
        .zip(temporal::nanosecond(array)?.values().iter())
        .map(|o| ((*o.0 as f64 + (*o.1 as f64) / 1_000_000_000.0) * (frac as f64)))
        .collect::<Vec<f64>>();

    Ok(Float64Array::from(zipped))
}

fn seconds<T>(array: &PrimitiveArray<T>) -> Result<Float64Array>
where
    T: ArrowTemporalType + ArrowNumericType,
    i64: From<T::Native>,
{
    to_ticks(array, 1)
}

fn millis<T>(array: &PrimitiveArray<T>) -> Result<Float64Array>
where
    T: ArrowTemporalType + ArrowNumericType,
    i64: From<T::Native>,
{
    to_ticks(array, 1_000)
}

fn micros<T>(array: &PrimitiveArray<T>) -> Result<Float64Array>
where
    T: ArrowTemporalType + ArrowNumericType,
    i64: From<T::Native>,
{
    to_ticks(array, 1_000_000)
}

fn nanos<T>(array: &PrimitiveArray<T>) -> Result<Float64Array>
where
    T: ArrowTemporalType + ArrowNumericType,
    i64: From<T::Native>,
{
    to_ticks(array, 1_000_000_000)
}

fn epoch<T>(array: &PrimitiveArray<T>) -> Result<Float64Array>
where
    T: ArrowTemporalType + ArrowNumericType,
    i64: From<T::Native>,
{
    let b = match array.data_type() {
        DataType::Timestamp(tu, _) => {
            let scale = match tu {
                TimeUnit::Second => 1,
                TimeUnit::Millisecond => 1_000,
                TimeUnit::Microsecond => 1_000_000,
                TimeUnit::Nanosecond => 1_000_000_000,
            } as f64;
            array.unary(|n| {
                let n: i64 = n.into();
                n as f64 / scale
            })
        }
        DataType::Date32 => {
            let seconds_in_a_day = 86400_f64;
            array.unary(|n| {
                let n: i64 = n.into();
                n as f64 * seconds_in_a_day
            })
        }
        DataType::Date64 => array.unary(|n| {
            let n: i64 = n.into();
            n as f64 / 1_000_f64
        }),
        _ => return internal_err!("Can not convert {:?} to epoch", array.data_type()),
    };
    Ok(b)
}

fn validate_to_timestamp_data_types(
    args: &[ColumnarValue],
    name: &str,
) -> Option<Result<ColumnarValue>> {
    for (idx, a) in args.iter().skip(1).enumerate() {
        match a.data_type() {
            DataType::Utf8 | DataType::LargeUtf8 => {
                // all good
            }
            _ => {
                return Some(internal_err!(
                    "{name} function unsupported data type at index {}: {}",
                    idx + 1,
                    a.data_type()
                ));
            }
        }
    }

    None
}

/// to_timestamp() SQL function implementation
pub fn to_timestamp_invoke(args: &[ColumnarValue]) -> Result<ColumnarValue> {
    if args.is_empty() {
        return internal_err!(
            "to_timestamp function requires 1 or more arguments, got {}",
            args.len()
        );
    }

    // validate that any args after the first one are Utf8
    if args.len() > 1 {
        if let Some(value) = validate_to_timestamp_data_types(args, "to_timestamp") {
            return value;
        }
    }

    match args[0].data_type() {
        DataType::Int32 | DataType::Int64 => cast_column(
            &cast_column(&args[0], &DataType::Timestamp(TimeUnit::Second, None), None)?,
            &DataType::Timestamp(TimeUnit::Nanosecond, None),
            None,
        ),
        DataType::Null | DataType::Float64 => cast_column(
            &args[0],
            &DataType::Timestamp(TimeUnit::Nanosecond, None),
            None,
        ),
        DataType::Timestamp(_, None) => cast_column(
            &args[0],
            &DataType::Timestamp(TimeUnit::Nanosecond, None),
            None,
        ),
        DataType::Utf8 => to_timestamp(args),
        other => {
            internal_err!(
                "Unsupported data type {:?} for function to_timestamp",
                other
            )
        }
    }
}

/// to_timestamp_millis() SQL function implementation
pub fn to_timestamp_millis_invoke(args: &[ColumnarValue]) -> Result<ColumnarValue> {
    if args.is_empty() {
        return internal_err!(
            "to_timestamp_millis function requires 1 or more arguments, got {}",
            args.len()
        );
    }

    // validate that any args after the first one are Utf8
    if args.len() > 1 {
        if let Some(value) = validate_to_timestamp_data_types(args, "to_timestamp_millis")
        {
            return value;
        }
    }

    match args[0].data_type() {
        DataType::Null
        | DataType::Int32
        | DataType::Int64
        | DataType::Timestamp(_, None) => cast_column(
            &args[0],
            &DataType::Timestamp(TimeUnit::Millisecond, None),
            None,
        ),
        DataType::Utf8 => to_timestamp_millis(args),
        other => {
            internal_err!(
                "Unsupported data type {:?} for function to_timestamp_millis",
                other
            )
        }
    }
}

/// to_timestamp_micros() SQL function implementation
pub fn to_timestamp_micros_invoke(args: &[ColumnarValue]) -> Result<ColumnarValue> {
    if args.is_empty() {
        return internal_err!(
            "to_timestamp_micros function requires 1 or more arguments, got {}",
            args.len()
        );
    }

    // validate that any args after the first one are Utf8
    if args.len() > 1 {
        if let Some(value) = validate_to_timestamp_data_types(args, "to_timestamp_micros")
        {
            return value;
        }
    }

    match args[0].data_type() {
        DataType::Null
        | DataType::Int32
        | DataType::Int64
        | DataType::Timestamp(_, None) => cast_column(
            &args[0],
            &DataType::Timestamp(TimeUnit::Microsecond, None),
            None,
        ),
        DataType::Utf8 => to_timestamp_micros(args),
        other => {
            internal_err!(
                "Unsupported data type {:?} for function to_timestamp_micros",
                other
            )
        }
    }
}

/// to_timestamp_nanos() SQL function implementation
pub fn to_timestamp_nanos_invoke(args: &[ColumnarValue]) -> Result<ColumnarValue> {
    if args.is_empty() {
        return internal_err!(
            "to_timestamp_nanos function requires 1 or more arguments, got {}",
            args.len()
        );
    }

    // validate that any args after the first one are Utf8
    if args.len() > 1 {
        if let Some(value) = validate_to_timestamp_data_types(args, "to_timestamp_nanos")
        {
            return value;
        }
    }

    match args[0].data_type() {
        DataType::Null
        | DataType::Int32
        | DataType::Int64
        | DataType::Timestamp(_, None) => cast_column(
            &args[0],
            &DataType::Timestamp(TimeUnit::Nanosecond, None),
            None,
        ),
        DataType::Utf8 => to_timestamp_nanos(args),
        other => {
            internal_err!(
                "Unsupported data type {:?} for function to_timestamp_nanos",
                other
            )
        }
    }
}

/// to_timestamp_seconds() SQL function implementation
pub fn to_timestamp_seconds_invoke(args: &[ColumnarValue]) -> Result<ColumnarValue> {
    if args.is_empty() {
        return internal_err!(
            "to_timestamp_seconds function requires 1 or more arguments, got {}",
            args.len()
        );
    }

    // validate that any args after the first one are Utf8
    if args.len() > 1 {
        if let Some(value) =
            validate_to_timestamp_data_types(args, "to_timestamp_seconds")
        {
            return value;
        }
    }

    match args[0].data_type() {
        DataType::Null
        | DataType::Int32
        | DataType::Int64
        | DataType::Timestamp(_, None) => {
            cast_column(&args[0], &DataType::Timestamp(TimeUnit::Second, None), None)
        }
        DataType::Utf8 => to_timestamp_seconds(args),
        other => {
            internal_err!(
                "Unsupported data type {:?} for function to_timestamp_seconds",
                other
            )
        }
    }
}

/// from_unixtime() SQL function implementation
pub fn from_unixtime_invoke(args: &[ColumnarValue]) -> Result<ColumnarValue> {
    if args.len() != 1 {
        return internal_err!(
            "from_unixtime function requires 1 argument, got {}",
            args.len()
        );
    }

    match args[0].data_type() {
        DataType::Int64 => {
            cast_column(&args[0], &DataType::Timestamp(TimeUnit::Second, None), None)
        }
        other => {
            internal_err!(
                "Unsupported data type {:?} for function from_unixtime",
                other
            )
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow::array::{
        as_primitive_array, ArrayRef, Int64Array, IntervalDayTimeArray, StringBuilder,
    };
    use arrow_array::types::Int64Type;
    use arrow_array::{
        TimestampMicrosecondArray, TimestampMillisecondArray, TimestampNanosecondArray,
        TimestampSecondArray,
    };
    use datafusion_common::assert_contains;

    use super::*;

    #[test]
    fn to_timestamp_arrays_and_nulls() -> Result<()> {
        // ensure that arrow array implementation is wired up and handles nulls correctly

        let mut string_builder = StringBuilder::with_capacity(2, 1024);
        let mut ts_builder = TimestampNanosecondArray::builder(2);

        string_builder.append_value("2020-09-08T13:42:29.190855");
        ts_builder.append_value(1599572549190855000);

        string_builder.append_null();
        ts_builder.append_null();
        let expected_timestamps = &ts_builder.finish() as &dyn Array;

        let string_array =
            ColumnarValue::Array(Arc::new(string_builder.finish()) as ArrayRef);
        let parsed_timestamps = to_timestamp(&[string_array])
            .expect("that to_timestamp parsed values without error");
        if let ColumnarValue::Array(parsed_array) = parsed_timestamps {
            assert_eq!(parsed_array.len(), 2);
            assert_eq!(expected_timestamps, parsed_array.as_ref());
        } else {
            panic!("Expected a columnar array")
        }
        Ok(())
    }

    #[test]
    fn to_timestamp_with_formats_arrays_and_nulls() -> Result<()> {
        // ensure that arrow array implementation is wired up and handles nulls correctly

        let mut date_string_builder = StringBuilder::with_capacity(2, 1024);
        let mut format1_builder = StringBuilder::with_capacity(2, 1024);
        let mut format2_builder = StringBuilder::with_capacity(2, 1024);
        let mut format3_builder = StringBuilder::with_capacity(2, 1024);
        let mut ts_builder = TimestampNanosecondArray::builder(2);

        date_string_builder.append_null();
        format1_builder.append_null();
        format2_builder.append_null();
        format3_builder.append_null();
        ts_builder.append_null();

        date_string_builder.append_value("2020-09-08T13:42:29.19085Z");
        format1_builder.append_value("%s");
        format2_builder.append_value("%c");
        format3_builder.append_value("%+");
        ts_builder.append_value(1599572549190850000);

        let expected_timestamps = &ts_builder.finish() as &dyn Array;

        let string_array = [
            ColumnarValue::Array(Arc::new(date_string_builder.finish()) as ArrayRef),
            ColumnarValue::Array(Arc::new(format1_builder.finish()) as ArrayRef),
            ColumnarValue::Array(Arc::new(format2_builder.finish()) as ArrayRef),
            ColumnarValue::Array(Arc::new(format3_builder.finish()) as ArrayRef),
        ];
        let parsed_timestamps = to_timestamp(&string_array)
            .expect("that to_timestamp with format args parsed values without error");
        if let ColumnarValue::Array(parsed_array) = parsed_timestamps {
            assert_eq!(parsed_array.len(), 2);
            assert_eq!(expected_timestamps, parsed_array.as_ref());
        } else {
            panic!("Expected a columnar array")
        }
        Ok(())
    }

    #[test]
    fn date_trunc_test() {
        let cases = vec![
            (
                "2020-09-08T13:42:29.190855Z",
                "second",
                "2020-09-08T13:42:29.000000Z",
            ),
            (
                "2020-09-08T13:42:29.190855Z",
                "minute",
                "2020-09-08T13:42:00.000000Z",
            ),
            (
                "2020-09-08T13:42:29.190855Z",
                "hour",
                "2020-09-08T13:00:00.000000Z",
            ),
            (
                "2020-09-08T13:42:29.190855Z",
                "day",
                "2020-09-08T00:00:00.000000Z",
            ),
            (
                "2020-09-08T13:42:29.190855Z",
                "week",
                "2020-09-07T00:00:00.000000Z",
            ),
            (
                "2020-09-08T13:42:29.190855Z",
                "month",
                "2020-09-01T00:00:00.000000Z",
            ),
            (
                "2020-09-08T13:42:29.190855Z",
                "year",
                "2020-01-01T00:00:00.000000Z",
            ),
            // week
            (
                "2021-01-01T13:42:29.190855Z",
                "week",
                "2020-12-28T00:00:00.000000Z",
            ),
            (
                "2020-01-01T13:42:29.190855Z",
                "week",
                "2019-12-30T00:00:00.000000Z",
            ),
            // quarter
            (
                "2020-01-01T13:42:29.190855Z",
                "quarter",
                "2020-01-01T00:00:00.000000Z",
            ),
            (
                "2020-02-01T13:42:29.190855Z",
                "quarter",
                "2020-01-01T00:00:00.000000Z",
            ),
            (
                "2020-03-01T13:42:29.190855Z",
                "quarter",
                "2020-01-01T00:00:00.000000Z",
            ),
            (
                "2020-04-01T13:42:29.190855Z",
                "quarter",
                "2020-04-01T00:00:00.000000Z",
            ),
            (
                "2020-08-01T13:42:29.190855Z",
                "quarter",
                "2020-07-01T00:00:00.000000Z",
            ),
            (
                "2020-11-01T13:42:29.190855Z",
                "quarter",
                "2020-10-01T00:00:00.000000Z",
            ),
            (
                "2020-12-01T13:42:29.190855Z",
                "quarter",
                "2020-10-01T00:00:00.000000Z",
            ),
        ];

        cases.iter().for_each(|(original, granularity, expected)| {
            let left = string_to_timestamp_nanos(original).unwrap();
            let right = string_to_timestamp_nanos(expected).unwrap();
            let result = date_trunc_coarse(granularity, left, None).unwrap();
            assert_eq!(result, right, "{original} = {expected}");
        });
    }

    #[test]
    fn test_date_trunc_timezones() {
        let cases = vec![
            (
                vec![
                    "2020-09-08T00:00:00Z",
                    "2020-09-08T01:00:00Z",
                    "2020-09-08T02:00:00Z",
                    "2020-09-08T03:00:00Z",
                    "2020-09-08T04:00:00Z",
                ],
                Some("+00".into()),
                vec![
                    "2020-09-08T00:00:00Z",
                    "2020-09-08T00:00:00Z",
                    "2020-09-08T00:00:00Z",
                    "2020-09-08T00:00:00Z",
                    "2020-09-08T00:00:00Z",
                ],
            ),
            (
                vec![
                    "2020-09-08T00:00:00Z",
                    "2020-09-08T01:00:00Z",
                    "2020-09-08T02:00:00Z",
                    "2020-09-08T03:00:00Z",
                    "2020-09-08T04:00:00Z",
                ],
                None,
                vec![
                    "2020-09-08T00:00:00Z",
                    "2020-09-08T00:00:00Z",
                    "2020-09-08T00:00:00Z",
                    "2020-09-08T00:00:00Z",
                    "2020-09-08T00:00:00Z",
                ],
            ),
            (
                vec![
                    "2020-09-08T00:00:00Z",
                    "2020-09-08T01:00:00Z",
                    "2020-09-08T02:00:00Z",
                    "2020-09-08T03:00:00Z",
                    "2020-09-08T04:00:00Z",
                ],
                Some("-02".into()),
                vec![
                    "2020-09-07T02:00:00Z",
                    "2020-09-07T02:00:00Z",
                    "2020-09-08T02:00:00Z",
                    "2020-09-08T02:00:00Z",
                    "2020-09-08T02:00:00Z",
                ],
            ),
            (
                vec![
                    "2020-09-08T00:00:00+05",
                    "2020-09-08T01:00:00+05",
                    "2020-09-08T02:00:00+05",
                    "2020-09-08T03:00:00+05",
                    "2020-09-08T04:00:00+05",
                ],
                Some("+05".into()),
                vec![
                    "2020-09-08T00:00:00+05",
                    "2020-09-08T00:00:00+05",
                    "2020-09-08T00:00:00+05",
                    "2020-09-08T00:00:00+05",
                    "2020-09-08T00:00:00+05",
                ],
            ),
            (
                vec![
                    "2020-09-08T00:00:00+08",
                    "2020-09-08T01:00:00+08",
                    "2020-09-08T02:00:00+08",
                    "2020-09-08T03:00:00+08",
                    "2020-09-08T04:00:00+08",
                ],
                Some("+08".into()),
                vec![
                    "2020-09-08T00:00:00+08",
                    "2020-09-08T00:00:00+08",
                    "2020-09-08T00:00:00+08",
                    "2020-09-08T00:00:00+08",
                    "2020-09-08T00:00:00+08",
                ],
            ),
        ];

        cases.iter().for_each(|(original, tz_opt, expected)| {
            let input = original
                .iter()
                .map(|s| Some(string_to_timestamp_nanos(s).unwrap()))
                .collect::<TimestampNanosecondArray>()
                .with_timezone_opt(tz_opt.clone());
            let right = expected
                .iter()
                .map(|s| Some(string_to_timestamp_nanos(s).unwrap()))
                .collect::<TimestampNanosecondArray>()
                .with_timezone_opt(tz_opt.clone());
            let result = date_trunc(&[
                ColumnarValue::Scalar(ScalarValue::from("day")),
                ColumnarValue::Array(Arc::new(input)),
            ])
            .unwrap();
            if let ColumnarValue::Array(result) = result {
                assert_eq!(
                    result.data_type(),
                    &DataType::Timestamp(TimeUnit::Nanosecond, tz_opt.clone())
                );
                let left = as_primitive_array::<TimestampNanosecondType>(&result);
                assert_eq!(left, &right);
            } else {
                panic!("unexpected column type");
            }
        });
    }

    #[test]
    fn test_date_bin_single() {
        use chrono::Duration;

        let cases = vec![
            (
                (
                    Duration::minutes(15),
                    "2004-04-09T02:03:04.123456789Z",
                    "2001-01-01T00:00:00",
                ),
                "2004-04-09T02:00:00Z",
            ),
            (
                (
                    Duration::minutes(15),
                    "2004-04-09T02:03:04.123456789Z",
                    "2001-01-01T00:02:30",
                ),
                "2004-04-09T02:02:30Z",
            ),
            (
                (
                    Duration::minutes(15),
                    "2004-04-09T02:03:04.123456789Z",
                    "2005-01-01T00:02:30",
                ),
                "2004-04-09T02:02:30Z",
            ),
            (
                (
                    Duration::hours(1),
                    "2004-04-09T02:03:04.123456789Z",
                    "2001-01-01T00:00:00",
                ),
                "2004-04-09T02:00:00Z",
            ),
            (
                (
                    Duration::seconds(10),
                    "2004-04-09T02:03:11.123456789Z",
                    "2001-01-01T00:00:00",
                ),
                "2004-04-09T02:03:10Z",
            ),
        ];

        cases
            .iter()
            .for_each(|((stride, source, origin), expected)| {
                let stride1 = stride.num_nanoseconds().unwrap();
                let source1 = string_to_timestamp_nanos(source).unwrap();
                let origin1 = string_to_timestamp_nanos(origin).unwrap();

                let expected1 = string_to_timestamp_nanos(expected).unwrap();
                let result = date_bin_nanos_interval(stride1, source1, origin1);
                assert_eq!(result, expected1, "{source} = {expected}");
            })
    }

    #[test]
    fn test_date_bin() {
        let res = date_bin(&[
            ColumnarValue::Scalar(ScalarValue::IntervalDayTime(Some(1))),
            ColumnarValue::Scalar(ScalarValue::TimestampNanosecond(Some(1), None)),
            ColumnarValue::Scalar(ScalarValue::TimestampNanosecond(Some(1), None)),
        ]);
        assert!(res.is_ok());

        let timestamps = Arc::new((1..6).map(Some).collect::<TimestampNanosecondArray>());
        let res = date_bin(&[
            ColumnarValue::Scalar(ScalarValue::IntervalDayTime(Some(1))),
            ColumnarValue::Array(timestamps),
            ColumnarValue::Scalar(ScalarValue::TimestampNanosecond(Some(1), None)),
        ]);
        assert!(res.is_ok());

        let res = date_bin(&[
            ColumnarValue::Scalar(ScalarValue::IntervalDayTime(Some(1))),
            ColumnarValue::Scalar(ScalarValue::TimestampNanosecond(Some(1), None)),
        ]);
        assert!(res.is_ok());

        // stride supports month-day-nano
        let res = date_bin(&[
            ColumnarValue::Scalar(ScalarValue::IntervalMonthDayNano(Some(1))),
            ColumnarValue::Scalar(ScalarValue::TimestampNanosecond(Some(1), None)),
            ColumnarValue::Scalar(ScalarValue::TimestampNanosecond(Some(1), None)),
        ]);
        assert!(res.is_ok());

        //
        // Fallible test cases
        //

        // invalid number of arguments
        let res =
            date_bin(&[ColumnarValue::Scalar(ScalarValue::IntervalDayTime(Some(1)))]);
        assert_eq!(
            res.err().unwrap().strip_backtrace(),
            "Execution error: DATE_BIN expected two or three arguments"
        );

        // stride: invalid type
        let res = date_bin(&[
            ColumnarValue::Scalar(ScalarValue::IntervalYearMonth(Some(1))),
            ColumnarValue::Scalar(ScalarValue::TimestampNanosecond(Some(1), None)),
            ColumnarValue::Scalar(ScalarValue::TimestampNanosecond(Some(1), None)),
        ]);
        assert_eq!(
            res.err().unwrap().strip_backtrace(),
            "Execution error: DATE_BIN expects stride argument to be an INTERVAL but got Interval(YearMonth)"
        );

        // stride: invalid value
        let res = date_bin(&[
            ColumnarValue::Scalar(ScalarValue::IntervalDayTime(Some(0))),
            ColumnarValue::Scalar(ScalarValue::TimestampNanosecond(Some(1), None)),
            ColumnarValue::Scalar(ScalarValue::TimestampNanosecond(Some(1), None)),
        ]);
        assert_eq!(
            res.err().unwrap().strip_backtrace(),
            "Execution error: DATE_BIN stride must be non-zero"
        );

        // stride: overflow of day-time interval
        let res = date_bin(&[
            ColumnarValue::Scalar(ScalarValue::IntervalDayTime(Some(i64::MAX))),
            ColumnarValue::Scalar(ScalarValue::TimestampNanosecond(Some(1), None)),
            ColumnarValue::Scalar(ScalarValue::TimestampNanosecond(Some(1), None)),
        ]);
        assert_eq!(
            res.err().unwrap().strip_backtrace(),
            "Execution error: DATE_BIN stride argument is too large"
        );

        // stride: overflow of month-day-nano interval
        let res = date_bin(&[
            ColumnarValue::Scalar(ScalarValue::new_interval_mdn(0, i32::MAX, 1)),
            ColumnarValue::Scalar(ScalarValue::TimestampNanosecond(Some(1), None)),
            ColumnarValue::Scalar(ScalarValue::TimestampNanosecond(Some(1), None)),
        ]);
        assert_eq!(
            res.err().unwrap().strip_backtrace(),
            "Execution error: DATE_BIN stride argument is too large"
        );

        // stride: month intervals
        let res = date_bin(&[
            ColumnarValue::Scalar(ScalarValue::new_interval_mdn(1, 1, 1)),
            ColumnarValue::Scalar(ScalarValue::TimestampNanosecond(Some(1), None)),
            ColumnarValue::Scalar(ScalarValue::TimestampNanosecond(Some(1), None)),
        ]);
        assert_eq!(
            res.err().unwrap().strip_backtrace(),
            "This feature is not implemented: DATE_BIN stride does not support combination of month, day and nanosecond intervals"
        );

        // origin: invalid type
        let res = date_bin(&[
            ColumnarValue::Scalar(ScalarValue::IntervalDayTime(Some(1))),
            ColumnarValue::Scalar(ScalarValue::TimestampNanosecond(Some(1), None)),
            ColumnarValue::Scalar(ScalarValue::TimestampMicrosecond(Some(1), None)),
        ]);
        assert_eq!(
            res.err().unwrap().strip_backtrace(),
            "Execution error: DATE_BIN expects origin argument to be a TIMESTAMP with nanosececond precision but got Timestamp(Microsecond, None)"
        );

        let res = date_bin(&[
            ColumnarValue::Scalar(ScalarValue::IntervalDayTime(Some(1))),
            ColumnarValue::Scalar(ScalarValue::TimestampMicrosecond(Some(1), None)),
            ColumnarValue::Scalar(ScalarValue::TimestampNanosecond(Some(1), None)),
        ]);
        assert!(res.is_ok());

        // unsupported array type for stride
        let intervals = Arc::new((1..6).map(Some).collect::<IntervalDayTimeArray>());
        let res = date_bin(&[
            ColumnarValue::Array(intervals),
            ColumnarValue::Scalar(ScalarValue::TimestampNanosecond(Some(1), None)),
            ColumnarValue::Scalar(ScalarValue::TimestampNanosecond(Some(1), None)),
        ]);
        assert_eq!(
            res.err().unwrap().strip_backtrace(),
            "This feature is not implemented: DATE_BIN only supports literal values for the stride argument, not arrays"
        );

        // unsupported array type for origin
        let timestamps = Arc::new((1..6).map(Some).collect::<TimestampNanosecondArray>());
        let res = date_bin(&[
            ColumnarValue::Scalar(ScalarValue::IntervalDayTime(Some(1))),
            ColumnarValue::Scalar(ScalarValue::TimestampNanosecond(Some(1), None)),
            ColumnarValue::Array(timestamps),
        ]);
        assert_eq!(
            res.err().unwrap().strip_backtrace(),
            "This feature is not implemented: DATE_BIN only supports literal values for the origin argument, not arrays"
        );
    }

    #[test]
    fn test_date_bin_timezones() {
        let cases = vec![
            (
                vec![
                    "2020-09-08T00:00:00Z",
                    "2020-09-08T01:00:00Z",
                    "2020-09-08T02:00:00Z",
                    "2020-09-08T03:00:00Z",
                    "2020-09-08T04:00:00Z",
                ],
                Some("+00".into()),
                "1970-01-01T00:00:00Z",
                vec![
                    "2020-09-08T00:00:00Z",
                    "2020-09-08T00:00:00Z",
                    "2020-09-08T00:00:00Z",
                    "2020-09-08T00:00:00Z",
                    "2020-09-08T00:00:00Z",
                ],
            ),
            (
                vec![
                    "2020-09-08T00:00:00Z",
                    "2020-09-08T01:00:00Z",
                    "2020-09-08T02:00:00Z",
                    "2020-09-08T03:00:00Z",
                    "2020-09-08T04:00:00Z",
                ],
                None,
                "1970-01-01T00:00:00Z",
                vec![
                    "2020-09-08T00:00:00Z",
                    "2020-09-08T00:00:00Z",
                    "2020-09-08T00:00:00Z",
                    "2020-09-08T00:00:00Z",
                    "2020-09-08T00:00:00Z",
                ],
            ),
            (
                vec![
                    "2020-09-08T00:00:00Z",
                    "2020-09-08T01:00:00Z",
                    "2020-09-08T02:00:00Z",
                    "2020-09-08T03:00:00Z",
                    "2020-09-08T04:00:00Z",
                ],
                Some("-02".into()),
                "1970-01-01T00:00:00Z",
                vec![
                    "2020-09-08T00:00:00Z",
                    "2020-09-08T00:00:00Z",
                    "2020-09-08T00:00:00Z",
                    "2020-09-08T00:00:00Z",
                    "2020-09-08T00:00:00Z",
                ],
            ),
            (
                vec![
                    "2020-09-08T00:00:00+05",
                    "2020-09-08T01:00:00+05",
                    "2020-09-08T02:00:00+05",
                    "2020-09-08T03:00:00+05",
                    "2020-09-08T04:00:00+05",
                ],
                Some("+05".into()),
                "1970-01-01T00:00:00+05",
                vec![
                    "2020-09-08T00:00:00+05",
                    "2020-09-08T00:00:00+05",
                    "2020-09-08T00:00:00+05",
                    "2020-09-08T00:00:00+05",
                    "2020-09-08T00:00:00+05",
                ],
            ),
            (
                vec![
                    "2020-09-08T00:00:00+08",
                    "2020-09-08T01:00:00+08",
                    "2020-09-08T02:00:00+08",
                    "2020-09-08T03:00:00+08",
                    "2020-09-08T04:00:00+08",
                ],
                Some("+08".into()),
                "1970-01-01T00:00:00+08",
                vec![
                    "2020-09-08T00:00:00+08",
                    "2020-09-08T00:00:00+08",
                    "2020-09-08T00:00:00+08",
                    "2020-09-08T00:00:00+08",
                    "2020-09-08T00:00:00+08",
                ],
            ),
        ];

        cases
            .iter()
            .for_each(|(original, tz_opt, origin, expected)| {
                let input = original
                    .iter()
                    .map(|s| Some(string_to_timestamp_nanos(s).unwrap()))
                    .collect::<TimestampNanosecondArray>()
                    .with_timezone_opt(tz_opt.clone());
                let right = expected
                    .iter()
                    .map(|s| Some(string_to_timestamp_nanos(s).unwrap()))
                    .collect::<TimestampNanosecondArray>()
                    .with_timezone_opt(tz_opt.clone());
                let result = date_bin(&[
                    ColumnarValue::Scalar(ScalarValue::new_interval_dt(1, 0)),
                    ColumnarValue::Array(Arc::new(input)),
                    ColumnarValue::Scalar(ScalarValue::TimestampNanosecond(
                        Some(string_to_timestamp_nanos(origin).unwrap()),
                        tz_opt.clone(),
                    )),
                ])
                .unwrap();
                if let ColumnarValue::Array(result) = result {
                    assert_eq!(
                        result.data_type(),
                        &DataType::Timestamp(TimeUnit::Nanosecond, tz_opt.clone())
                    );
                    let left = as_primitive_array::<TimestampNanosecondType>(&result);
                    assert_eq!(left, &right);
                } else {
                    panic!("unexpected column type");
                }
            });
    }

    #[test]
    fn to_timestamp_invalid_input_type() -> Result<()> {
        // pass the wrong type of input array to to_timestamp and test
        // that we get an error.

        let mut builder = Int64Array::builder(1);
        builder.append_value(1);
        let int64array = ColumnarValue::Array(Arc::new(builder.finish()));

        let expected_err =
            "Execution error: Unsupported data type Int64 for function to_timestamp";
        match to_timestamp(&[int64array]) {
            Ok(_) => panic!("Expected error but got success"),
            Err(e) => {
                assert!(
                    e.to_string().contains(expected_err),
                    "Can not find expected error '{expected_err}'. Actual error '{e}'"
                );
            }
        }
        Ok(())
    }

    #[test]
    fn to_timestamp_with_formats_invalid_input_type() -> Result<()> {
        // pass the wrong type of input array to to_timestamp and test
        // that we get an error.

        let mut builder = Int64Array::builder(1);
        builder.append_value(1);
        let int64array = [
            ColumnarValue::Array(Arc::new(builder.finish())),
            ColumnarValue::Array(Arc::new(builder.finish())),
        ];

        let expected_err =
            "Execution error: Unsupported data type Int64 for function to_timestamp";
        match to_timestamp(&int64array) {
            Ok(_) => panic!("Expected error but got success"),
            Err(e) => {
                assert!(
                    e.to_string().contains(expected_err),
                    "Can not find expected error '{expected_err}'. Actual error '{e}'"
                );
            }
        }
        Ok(())
    }

    #[test]
    fn to_timestamp_with_unparseable_data() -> Result<()> {
        let mut date_string_builder = StringBuilder::with_capacity(2, 1024);

        date_string_builder.append_null();

        date_string_builder.append_value("2020-09-08 - 13:42:29.19085Z");

        let string_array =
            ColumnarValue::Array(Arc::new(date_string_builder.finish()) as ArrayRef);

        let expected_err =
            "Arrow error: Parser error: Error parsing timestamp from '2020-09-08 - 13:42:29.19085Z': error parsing time";
        match to_timestamp(&[string_array]) {
            Ok(_) => panic!("Expected error but got success"),
            Err(e) => {
                assert!(
                    e.to_string().contains(expected_err),
                    "Can not find expected error '{expected_err}'. Actual error '{e}'"
                );
            }
        }
        Ok(())
    }

    #[test]
    fn to_timestamp_with_no_matching_formats() -> Result<()> {
        let mut date_string_builder = StringBuilder::with_capacity(2, 1024);
        let mut format1_builder = StringBuilder::with_capacity(2, 1024);
        let mut format2_builder = StringBuilder::with_capacity(2, 1024);
        let mut format3_builder = StringBuilder::with_capacity(2, 1024);

        date_string_builder.append_null();
        format1_builder.append_null();
        format2_builder.append_null();
        format3_builder.append_null();

        date_string_builder.append_value("2020-09-08T13:42:29.19085Z");
        format1_builder.append_value("%s");
        format2_builder.append_value("%c");
        format3_builder.append_value("%H:%M:%S");

        let string_array = [
            ColumnarValue::Array(Arc::new(date_string_builder.finish()) as ArrayRef),
            ColumnarValue::Array(Arc::new(format1_builder.finish()) as ArrayRef),
            ColumnarValue::Array(Arc::new(format2_builder.finish()) as ArrayRef),
            ColumnarValue::Array(Arc::new(format3_builder.finish()) as ArrayRef),
        ];

        let expected_err =
            "Execution error: Error parsing timestamp from '2020-09-08T13:42:29.19085Z' using format '%H:%M:%S': input contains invalid characters";
        match to_timestamp(&string_array) {
            Ok(_) => panic!("Expected error but got success"),
            Err(e) => {
                assert!(
                    e.to_string().contains(expected_err),
                    "Can not find expected error '{expected_err}'. Actual error '{e}'"
                );
            }
        }
        Ok(())
    }

    #[test]
    fn string_to_timestamp_formatted() {
        // Explicit timezone
        assert_eq!(
            1599572549190855000,
            parse_timestamp_formatted("2020-09-08T13:42:29.190855+00:00", "%+").unwrap()
        );
        assert_eq!(
            1599572549190855000,
            parse_timestamp_formatted("2020-09-08T13:42:29.190855Z", "%+").unwrap()
        );
        assert_eq!(
            1599572549000000000,
            parse_timestamp_formatted("2020-09-08T13:42:29Z", "%+").unwrap()
        ); // no fractional part
        assert_eq!(
            1599590549190855000,
            parse_timestamp_formatted("2020-09-08T13:42:29.190855-05:00", "%+").unwrap()
        );
        assert_eq!(
            1599590549000000000,
            parse_timestamp_formatted("1599590549", "%s").unwrap()
        );
        assert_eq!(
            1599572549000000000,
            parse_timestamp_formatted("09-08-2020 13/42/29", "%m-%d-%Y %H/%M/%S")
                .unwrap()
        );
    }

    fn parse_timestamp_formatted(s: &str, format: &str) -> Result<i64, DataFusionError> {
        let result = string_to_timestamp_nanos_formatted(s, format);
        if let Err(e) = &result {
            eprintln!("Error parsing timestamp '{s}' using format '{format}': {e:?}");
        }
        result
    }

    #[test]
    fn string_to_timestamp_formatted_invalid() {
        // Test parsing invalid formats
        let cases = [
            ("", "%Y%m%d %H%M%S", "premature end of input"),
            ("SS", "%c", "premature end of input"),
            ("Wed, 18 Feb 2015 23:16:09 GMT", "", "trailing input"),
            (
                "Wed, 18 Feb 2015 23:16:09 GMT",
                "%XX",
                "input contains invalid characters",
            ),
            (
                "Wed, 18 Feb 2015 23:16:09 GMT",
                "%Y%m%d %H%M%S",
                "input contains invalid characters",
            ),
        ];

        for (s, f, ctx) in cases {
            let expected = format!("Execution error: Error parsing timestamp from '{s}' using format '{f}': {ctx}");
            let actual = string_to_datetime_formatted(&Utc, s, f)
                .unwrap_err()
                .to_string();
            assert_eq!(actual, expected)
        }
    }

    #[test]
    fn string_to_timestamp_invalid_arguments() {
        // Test parsing invalid formats
        let cases = [
            ("", "%Y%m%d %H%M%S", "premature end of input"),
            ("SS", "%c", "premature end of input"),
            ("Wed, 18 Feb 2015 23:16:09 GMT", "", "trailing input"),
            (
                "Wed, 18 Feb 2015 23:16:09 GMT",
                "%XX",
                "input contains invalid characters",
            ),
            (
                "Wed, 18 Feb 2015 23:16:09 GMT",
                "%Y%m%d %H%M%S",
                "input contains invalid characters",
            ),
        ];

        for (s, f, ctx) in cases {
            let expected = format!("Execution error: Error parsing timestamp from '{s}' using format '{f}': {ctx}");
            let actual = string_to_datetime_formatted(&Utc, s, f)
                .unwrap_err()
                .to_string();
            assert_eq!(actual, expected)
        }
    }

    #[test]
    fn test_to_timestamp_arg_validation() {
        let mut date_string_builder = StringBuilder::with_capacity(2, 1024);
        date_string_builder.append_value("2020-09-08T13:42:29.19085Z");

        let data = date_string_builder.finish();
        let mut funcs = Vec::<(
            Box<dyn Fn(&[ColumnarValue]) -> Result<ColumnarValue>>,
            TimeUnit,
        )>::new();
        funcs.push((Box::new(|a| to_timestamp(a)), TimeUnit::Nanosecond));
        funcs.push((Box::new(|a| to_timestamp_micros(a)), TimeUnit::Microsecond));
        funcs.push((Box::new(|a| to_timestamp_millis(a)), TimeUnit::Millisecond));
        funcs.push((Box::new(|a| to_timestamp_nanos(a)), TimeUnit::Nanosecond));
        funcs.push((Box::new(|a| to_timestamp_seconds(a)), TimeUnit::Second));

        let mut nanos_builder = TimestampNanosecondArray::builder(2);
        let mut millis_builder = TimestampMillisecondArray::builder(2);
        let mut micros_builder = TimestampMicrosecondArray::builder(2);
        let mut sec_builder = TimestampSecondArray::builder(2);

        nanos_builder.append_value(1599572549190850000);
        millis_builder.append_value(1599572549190);
        micros_builder.append_value(1599572549190850);
        sec_builder.append_value(1599572549);

        let nanos_expected_timestamps = &nanos_builder.finish() as &dyn Array;
        let millis_expected_timestamps = &millis_builder.finish() as &dyn Array;
        let micros_expected_timestamps = &micros_builder.finish() as &dyn Array;
        let sec_expected_timestamps = &sec_builder.finish() as &dyn Array;

        for (func, time_unit) in funcs {
            // test UTF8
            let string_array = [
                ColumnarValue::Array(Arc::new(data.clone()) as ArrayRef),
                ColumnarValue::Scalar(ScalarValue::Utf8(Some("%s".to_string()))),
                ColumnarValue::Scalar(ScalarValue::Utf8(Some("%c".to_string()))),
                ColumnarValue::Scalar(ScalarValue::Utf8(Some("%+".to_string()))),
            ];
            let parsed_timestamps = func(&string_array)
                .expect("that to_timestamp with format args parsed values without error");
            if let ColumnarValue::Array(parsed_array) = parsed_timestamps {
                assert_eq!(parsed_array.len(), 1);
                match time_unit {
                    TimeUnit::Nanosecond => {
                        assert_eq!(nanos_expected_timestamps, parsed_array.as_ref())
                    }
                    TimeUnit::Millisecond => {
                        assert_eq!(millis_expected_timestamps, parsed_array.as_ref())
                    }
                    TimeUnit::Microsecond => {
                        assert_eq!(micros_expected_timestamps, parsed_array.as_ref())
                    }
                    TimeUnit::Second => {
                        assert_eq!(sec_expected_timestamps, parsed_array.as_ref())
                    }
                };
            } else {
                panic!("Expected a columnar array")
            }

            // test LargeUTF8
            let string_array = [
                ColumnarValue::Array(Arc::new(data.clone()) as ArrayRef),
                ColumnarValue::Scalar(ScalarValue::LargeUtf8(Some("%s".to_string()))),
                ColumnarValue::Scalar(ScalarValue::LargeUtf8(Some("%c".to_string()))),
                ColumnarValue::Scalar(ScalarValue::LargeUtf8(Some("%+".to_string()))),
            ];
            let parsed_timestamps = func(&string_array)
                .expect("that to_timestamp with format args parsed values without error");
            if let ColumnarValue::Array(parsed_array) = parsed_timestamps {
                assert_eq!(parsed_array.len(), 1);
                match time_unit {
                    TimeUnit::Nanosecond => {
                        assert_eq!(nanos_expected_timestamps, parsed_array.as_ref())
                    }
                    TimeUnit::Millisecond => {
                        assert_eq!(millis_expected_timestamps, parsed_array.as_ref())
                    }
                    TimeUnit::Microsecond => {
                        assert_eq!(micros_expected_timestamps, parsed_array.as_ref())
                    }
                    TimeUnit::Second => {
                        assert_eq!(sec_expected_timestamps, parsed_array.as_ref())
                    }
                };
            } else {
                panic!("Expected a columnar array")
            }

            // test other types
            let string_array = [
                ColumnarValue::Array(Arc::new(data.clone()) as ArrayRef),
                ColumnarValue::Scalar(ScalarValue::Int32(Some(1))),
                ColumnarValue::Scalar(ScalarValue::Int32(Some(2))),
                ColumnarValue::Scalar(ScalarValue::Int32(Some(3))),
            ];

            let expected = "Unsupported data type Int32 for function".to_string();
            let actual = func(&string_array).unwrap_err().to_string();
            assert_contains!(actual, expected);

            // test other types
            let string_array = [
                ColumnarValue::Array(Arc::new(data.clone()) as ArrayRef),
                ColumnarValue::Array(Arc::new(PrimitiveArray::<Int64Type>::new(
                    vec![1i64].into(),
                    None,
                )) as ArrayRef),
            ];

            let expected = "Unsupported data type".to_string();
            let actual = func(&string_array).unwrap_err().to_string();
            assert_contains!(actual, expected);
        }
    }
}
