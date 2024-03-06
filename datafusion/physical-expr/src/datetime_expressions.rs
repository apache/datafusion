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

use std::ops::{Add, Sub};
use std::str::FromStr;
use std::sync::Arc;

use arrow::compute::cast;
use arrow::util::display::{ArrayFormatter, DurationFormat, FormatOptions};
use arrow::{
    array::{Array, ArrayRef, Float64Array, PrimitiveArray},
    datatypes::{
        ArrowNumericType, ArrowTemporalType, DataType, IntervalDayTimeType,
        IntervalMonthDayNanoType, TimestampMicrosecondType, TimestampMillisecondType,
        TimestampNanosecondType, TimestampSecondType,
    },
};
use arrow::{
    compute::kernels::temporal,
    datatypes::TimeUnit,
    temporal_conversions::{as_datetime_with_timezone, timestamp_ns_to_datetime},
};
use arrow_array::builder::PrimitiveBuilder;
use arrow_array::cast::AsArray;
use arrow_array::temporal_conversions::NANOSECONDS;
use arrow_array::timezone::Tz;
use arrow_array::types::{ArrowTimestampType, Date32Type, Int32Type};
use arrow_array::StringArray;
use chrono::prelude::*;
use chrono::{Duration, LocalResult, Months, NaiveDate};

use datafusion_common::cast::{
    as_date32_array, as_date64_array, as_primitive_array, as_timestamp_microsecond_array,
    as_timestamp_millisecond_array, as_timestamp_nanosecond_array,
    as_timestamp_second_array,
};
use datafusion_common::{exec_err, not_impl_err, DataFusionError, Result, ScalarValue};
use datafusion_expr::ColumnarValue;

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

/// Returns a string representation of a date, time, timestamp or duration based
/// on a Chrono pattern.
///
/// The syntax for the patterns can be found at
/// <https://docs.rs/chrono/latest/chrono/format/strftime/index.html>
///
/// # Examples
///
/// ```ignore
/// # use chrono::prelude::*;
/// # use datafusion::prelude::*;
/// # use datafusion::error::Result;
/// # use datafusion_common::ScalarValue::TimestampNanosecond;
/// # use std::sync::Arc;
/// # use arrow_array::{Date32Array, RecordBatch, StringArray};
/// # use arrow_schema::{DataType, Field, Schema};
/// # #[tokio::main]
/// # async fn main() -> Result<()> {
/// let schema = Arc::new(Schema::new(vec![
///     Field::new("values", DataType::Date32, false),
///     Field::new("patterns", DataType::Utf8, false),
/// ]));
///
/// let batch = RecordBatch::try_new(
///     schema,
///     vec![
///         Arc::new(Date32Array::from(vec![
///             18506,
///             18507,
///             18508,
///             18509,
///         ])),
///         Arc::new(StringArray::from(vec![
///             "%Y-%m-%d",
///             "%Y:%m:%d",
///             "%Y%m%d",
///             "%d-%m-%Y",
///         ])),
///     ],
/// )?;
///
/// let ctx = SessionContext::new();
/// ctx.register_batch("t", batch)?;
/// let df = ctx.table("t").await?;
///
/// // use the to_char function to convert col 'values',
/// // to strings using patterns in col 'patterns'
/// let df = df.with_column(
///     "date_str",
///     to_char(col("values"), col("patterns"))
/// )?;
/// // Note that providing a scalar value for the pattern
/// // is more performant
/// let df = df.with_column(
///     "date_str2",
///     to_char(col("values"), lit("%d-%m-%Y"))
/// )?;
/// // literals can be used as well with dataframe calls
/// let timestamp = "2026-07-08T09:10:11"
///     .parse::<NaiveDateTime>()
///     .unwrap()
///     .with_nanosecond(56789)
///     .unwrap()
///     .timestamp_nanos_opt()
///     .unwrap();
/// let df = df.with_column(
///     "timestamp_str",
///     to_char(lit(TimestampNanosecond(Some(timestamp), None)), lit("%d-%m-%Y %H:%M:%S"))
/// )?;
///
/// df.show().await?;
///
/// # Ok(())
/// # }
/// ```
pub fn to_char(args: &[ColumnarValue]) -> Result<ColumnarValue> {
    if args.len() != 2 {
        return exec_err!("to_char function requires 2 arguments, got {}", args.len());
    }

    match &args[1] {
        // null format, use default formats
        ColumnarValue::Scalar(ScalarValue::Utf8(None))
        | ColumnarValue::Scalar(ScalarValue::Null) => {
            _to_char_scalar(args[0].clone(), None)
        }
        // constant format
        ColumnarValue::Scalar(ScalarValue::Utf8(Some(format))) => {
            // invoke to_char_scalar with the known string, without converting to array
            _to_char_scalar(args[0].clone(), Some(format))
        }
        ColumnarValue::Array(_) => _to_char_array(args),
        _ => {
            exec_err!(
                "Format for `to_char` must be non-null Utf8, received {:?}",
                args[1].data_type()
            )
        }
    }
}

fn _build_format_options<'a>(
    data_type: &DataType,
    format: Option<&'a str>,
) -> Result<FormatOptions<'a>, Result<ColumnarValue>> {
    let Some(format) = format else {
        return Ok(FormatOptions::new());
    };
    let format_options = match data_type {
        DataType::Date32 => FormatOptions::new().with_date_format(Some(format)),
        DataType::Date64 => FormatOptions::new().with_datetime_format(Some(format)),
        DataType::Time32(_) => FormatOptions::new().with_time_format(Some(format)),
        DataType::Time64(_) => FormatOptions::new().with_time_format(Some(format)),
        DataType::Timestamp(_, _) => FormatOptions::new()
            .with_timestamp_format(Some(format))
            .with_timestamp_tz_format(Some(format)),
        DataType::Duration(_) => FormatOptions::new().with_duration_format(
            if "ISO8601".eq_ignore_ascii_case(format) {
                DurationFormat::ISO8601
            } else {
                DurationFormat::Pretty
            },
        ),
        other => {
            return Err(exec_err!(
                "to_char only supports date, time, timestamp and duration data types, received {other:?}"
            ));
        }
    };
    Ok(format_options)
}

/// Special version when arg\[1] is a scalar
fn _to_char_scalar(
    expression: ColumnarValue,
    format: Option<&str>,
) -> Result<ColumnarValue> {
    // it's possible that the expression is a scalar however because
    // of the implementation in arrow-rs we need to convert it to an array
    let data_type = &expression.data_type();
    let is_scalar_expression = matches!(&expression, ColumnarValue::Scalar(_));
    let array = expression.into_array(1)?;
    let format_options = match _build_format_options(data_type, format) {
        Ok(value) => value,
        Err(value) => return value,
    };

    let formatter = ArrayFormatter::try_new(array.as_ref(), &format_options)?;
    let formatted: Result<Vec<_>, arrow_schema::ArrowError> = (0..array.len())
        .map(|i| formatter.value(i).try_to_string())
        .collect();

    if let Ok(formatted) = formatted {
        if is_scalar_expression {
            Ok(ColumnarValue::Scalar(ScalarValue::Utf8(Some(
                formatted.first().unwrap().to_string(),
            ))))
        } else {
            Ok(ColumnarValue::Array(
                Arc::new(StringArray::from(formatted)) as ArrayRef
            ))
        }
    } else {
        exec_err!("{}", formatted.unwrap_err())
    }
}

fn _to_char_array(args: &[ColumnarValue]) -> Result<ColumnarValue> {
    let arrays = ColumnarValue::values_to_arrays(args)?;
    let mut results: Vec<String> = vec![];
    let format_array = arrays[1].as_string::<i32>();
    let data_type = arrays[0].data_type();

    for idx in 0..arrays[0].len() {
        let format = if format_array.is_null(idx) {
            None
        } else {
            Some(format_array.value(idx))
        };
        let format_options = match _build_format_options(data_type, format) {
            Ok(value) => value,
            Err(value) => return value,
        };
        // this isn't ideal but this can't use ValueFormatter as it isn't independent
        // from ArrayFormatter
        let formatter = ArrayFormatter::try_new(arrays[0].as_ref(), &format_options)?;
        let result = formatter.value(idx).try_to_string();
        match result {
            Ok(value) => results.push(value),
            Err(e) => return exec_err!("{}", e),
        }
    }

    match args[0] {
        ColumnarValue::Array(_) => Ok(ColumnarValue::Array(Arc::new(StringArray::from(
            results,
        )) as ArrayRef)),
        ColumnarValue::Scalar(_) => Ok(ColumnarValue::Scalar(ScalarValue::Utf8(Some(
            results.first().unwrap().to_string(),
        )))),
    }
}

/// make_date(year, month, day) SQL function implementation
pub fn make_date(args: &[ColumnarValue]) -> Result<ColumnarValue> {
    if args.len() != 3 {
        return exec_err!(
            "make_date function requires 3 arguments, got {}",
            args.len()
        );
    }

    // first, identify if any of the arguments is an Array. If yes, store its `len`,
    // as any scalar will need to be converted to an array of len `len`.
    let len = args
        .iter()
        .fold(Option::<usize>::None, |acc, arg| match arg {
            ColumnarValue::Scalar(_) => acc,
            ColumnarValue::Array(a) => Some(a.len()),
        });

    let is_scalar = len.is_none();
    let array_size = if is_scalar { 1 } else { len.unwrap() };

    let years = args[0].cast_to(&DataType::Int32, None)?;
    let months = args[1].cast_to(&DataType::Int32, None)?;
    let days = args[2].cast_to(&DataType::Int32, None)?;

    // since the epoch for the date32 datatype is the unix epoch
    // we need to subtract the unix epoch from the current date
    // note this can result in a negative value
    let unix_days_from_ce = NaiveDate::from_ymd_opt(1970, 1, 1)
        .unwrap()
        .num_days_from_ce();

    let mut builder: PrimitiveBuilder<Date32Type> = PrimitiveArray::builder(array_size);

    let construct_date_fn = |builder: &mut PrimitiveBuilder<Date32Type>,
                             year: i32,
                             month: i32,
                             day: i32,
                             unix_days_from_ce: i32|
     -> Result<()> {
        let Ok(m) = u32::try_from(month) else {
            return exec_err!("Month value '{month:?}' is out of range");
        };
        let Ok(d) = u32::try_from(day) else {
            return exec_err!("Day value '{day:?}' is out of range");
        };

        let date = NaiveDate::from_ymd_opt(year, m, d);

        match date {
            Some(d) => builder.append_value(d.num_days_from_ce() - unix_days_from_ce),
            None => return exec_err!("Unable to parse date from {year}, {month}, {day}"),
        };
        Ok(())
    };

    let scalar_value_fn = |col: &ColumnarValue| -> Result<i32> {
        let ColumnarValue::Scalar(s) = col else {
            return exec_err!("Expected scalar value");
        };
        let ScalarValue::Int32(Some(i)) = s else {
            return exec_err!("Unable to parse date from null/empty value");
        };
        Ok(*i)
    };

    // For scalar only columns the operation is faster without using the PrimitiveArray
    if is_scalar {
        construct_date_fn(
            &mut builder,
            scalar_value_fn(&years)?,
            scalar_value_fn(&months)?,
            scalar_value_fn(&days)?,
            unix_days_from_ce,
        )?;
    } else {
        let to_primitive_array = |col: &ColumnarValue,
                                  scalar_count: usize|
         -> Result<PrimitiveArray<Int32Type>> {
            match col {
                ColumnarValue::Array(a) => Ok(a.as_primitive::<Int32Type>().to_owned()),
                _ => {
                    let v = scalar_value_fn(col).unwrap();
                    Ok(PrimitiveArray::<Int32Type>::from_value(v, scalar_count))
                }
            }
        };

        let years = to_primitive_array(&years, array_size).unwrap();
        let months = to_primitive_array(&months, array_size).unwrap();
        let days = to_primitive_array(&days, array_size).unwrap();
        for i in 0..array_size {
            construct_date_fn(
                &mut builder,
                years.value(i),
                months.value(i),
                days.value(i),
                unix_days_from_ce,
            )?;
        }
    }

    let arr = builder.finish();

    if is_scalar {
        // If all inputs are scalar, keeps output as scalar
        Ok(ColumnarValue::Scalar(ScalarValue::Date32(Some(
            arr.value(0),
        ))))
    } else {
        Ok(ColumnarValue::Array(Arc::new(arr)))
    }
}

fn quarter_month<T>(date: &T) -> u32
where
    T: Datelike,
{
    1 + 3 * ((date.month() - 1) / 3)
}

fn _date_trunc_coarse<T>(granularity: &str, value: Option<T>) -> Result<Option<T>>
where
    T: Datelike + Timelike + Sub<Duration, Output = T> + Copy,
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
    if let Some(value) = value {
        let local = value.naive_local();
        let truncated = _date_trunc_coarse::<NaiveDateTime>(granularity, Some(local))?;
        let truncated = truncated.and_then(|truncated| {
            match truncated.and_local_timezone(value.timezone()) {
                LocalResult::None => {
                    // This can happen if the date_trunc operation moves the time into
                    // an hour that doesn't exist due to daylight savings. On known example where
                    // this can happen is with historic dates in the America/Sao_Paulo time zone.
                    // To account for this adjust the time by a few hours, convert to local time,
                    // and then adjust the time back.
                    truncated
                        .sub(Duration::hours(3))
                        .and_local_timezone(value.timezone())
                        .single()
                        .map(|v| v.add(Duration::hours(3)))
                }
                LocalResult::Single(datetime) => Some(datetime),
                LocalResult::Ambiguous(datetime1, datetime2) => {
                    // Because we are truncating from an equally or more specific time
                    // the original time must have been within the ambiguous local time
                    // period. Therefore the offset of one of these times should match the
                    // offset of the original time.
                    if datetime1.offset().fix() == value.offset().fix() {
                        Some(datetime1)
                    } else {
                        Some(datetime2)
                    }
                }
            }
        });
        Ok(truncated.and_then(|value| value.timestamp_nanos_opt()))
    } else {
        _date_trunc_coarse::<NaiveDateTime>(granularity, None)?;
        Ok(None)
    }
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
            datatype => exec_err!("Extract does not support datatype {:?}", datatype),
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
        _ => return exec_err!("Can not convert {:?} to epoch", array.data_type()),
    };
    Ok(b)
}

/// from_unixtime() SQL function implementation
pub fn from_unixtime_invoke(args: &[ColumnarValue]) -> Result<ColumnarValue> {
    if args.len() != 1 {
        return exec_err!(
            "from_unixtime function requires 1 argument, got {}",
            args.len()
        );
    }

    match args[0].data_type() {
        DataType::Int64 => {
            args[0].cast_to(&DataType::Timestamp(TimeUnit::Second, None), None)
        }
        other => {
            exec_err!(
                "Unsupported data type {:?} for function from_unixtime",
                other
            )
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow::array::{as_primitive_array, ArrayRef, Int64Array, IntervalDayTimeArray};
    use arrow::compute::kernels::cast_utils::string_to_timestamp_nanos;
    use arrow_array::{
        Date32Array, Date64Array, Int32Array, Time32MillisecondArray, Time32SecondArray,
        Time64MicrosecondArray, Time64NanosecondArray, TimestampMicrosecondArray,
        TimestampMillisecondArray, TimestampNanosecondArray, TimestampSecondArray,
        UInt32Array,
    };
    use datafusion_common::ScalarValue;

    use super::*;

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
            (
                vec![
                    "2024-10-26T23:00:00Z",
                    "2024-10-27T00:00:00Z",
                    "2024-10-27T01:00:00Z",
                    "2024-10-27T02:00:00Z",
                ],
                Some("Europe/Berlin".into()),
                vec![
                    "2024-10-27T00:00:00+02",
                    "2024-10-27T00:00:00+02",
                    "2024-10-27T00:00:00+02",
                    "2024-10-27T00:00:00+02",
                ],
            ),
            (
                vec![
                    "2018-02-18T00:00:00Z",
                    "2018-02-18T01:00:00Z",
                    "2018-02-18T02:00:00Z",
                    "2018-02-18T03:00:00Z",
                    "2018-11-04T01:00:00Z",
                    "2018-11-04T02:00:00Z",
                    "2018-11-04T03:00:00Z",
                    "2018-11-04T04:00:00Z",
                ],
                Some("America/Sao_Paulo".into()),
                vec![
                    "2018-02-17T00:00:00-02",
                    "2018-02-17T00:00:00-02",
                    "2018-02-17T00:00:00-02",
                    "2018-02-18T00:00:00-03",
                    "2018-11-03T00:00:00-03",
                    "2018-11-03T00:00:00-03",
                    "2018-11-04T01:00:00-02",
                    "2018-11-04T01:00:00-02",
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
    fn test_date_trunc_hour_timezones() {
        let cases = vec![
            (
                vec![
                    "2020-09-08T00:30:00Z",
                    "2020-09-08T01:30:00Z",
                    "2020-09-08T02:30:00Z",
                    "2020-09-08T03:30:00Z",
                    "2020-09-08T04:30:00Z",
                ],
                Some("+00".into()),
                vec![
                    "2020-09-08T00:00:00Z",
                    "2020-09-08T01:00:00Z",
                    "2020-09-08T02:00:00Z",
                    "2020-09-08T03:00:00Z",
                    "2020-09-08T04:00:00Z",
                ],
            ),
            (
                vec![
                    "2020-09-08T00:30:00Z",
                    "2020-09-08T01:30:00Z",
                    "2020-09-08T02:30:00Z",
                    "2020-09-08T03:30:00Z",
                    "2020-09-08T04:30:00Z",
                ],
                None,
                vec![
                    "2020-09-08T00:00:00Z",
                    "2020-09-08T01:00:00Z",
                    "2020-09-08T02:00:00Z",
                    "2020-09-08T03:00:00Z",
                    "2020-09-08T04:00:00Z",
                ],
            ),
            (
                vec![
                    "2020-09-08T00:30:00Z",
                    "2020-09-08T01:30:00Z",
                    "2020-09-08T02:30:00Z",
                    "2020-09-08T03:30:00Z",
                    "2020-09-08T04:30:00Z",
                ],
                Some("-02".into()),
                vec![
                    "2020-09-08T00:00:00Z",
                    "2020-09-08T01:00:00Z",
                    "2020-09-08T02:00:00Z",
                    "2020-09-08T03:00:00Z",
                    "2020-09-08T04:00:00Z",
                ],
            ),
            (
                vec![
                    "2020-09-08T00:30:00+05",
                    "2020-09-08T01:30:00+05",
                    "2020-09-08T02:30:00+05",
                    "2020-09-08T03:30:00+05",
                    "2020-09-08T04:30:00+05",
                ],
                Some("+05".into()),
                vec![
                    "2020-09-08T00:00:00+05",
                    "2020-09-08T01:00:00+05",
                    "2020-09-08T02:00:00+05",
                    "2020-09-08T03:00:00+05",
                    "2020-09-08T04:00:00+05",
                ],
            ),
            (
                vec![
                    "2020-09-08T00:30:00+08",
                    "2020-09-08T01:30:00+08",
                    "2020-09-08T02:30:00+08",
                    "2020-09-08T03:30:00+08",
                    "2020-09-08T04:30:00+08",
                ],
                Some("+08".into()),
                vec![
                    "2020-09-08T00:00:00+08",
                    "2020-09-08T01:00:00+08",
                    "2020-09-08T02:00:00+08",
                    "2020-09-08T03:00:00+08",
                    "2020-09-08T04:00:00+08",
                ],
            ),
            (
                vec![
                    "2024-10-26T23:30:00Z",
                    "2024-10-27T00:30:00Z",
                    "2024-10-27T01:30:00Z",
                    "2024-10-27T02:30:00Z",
                ],
                Some("Europe/Berlin".into()),
                vec![
                    "2024-10-27T01:00:00+02",
                    "2024-10-27T02:00:00+02",
                    "2024-10-27T02:00:00+01",
                    "2024-10-27T03:00:00+01",
                ],
            ),
            (
                vec![
                    "2018-02-18T00:30:00Z",
                    "2018-02-18T01:30:00Z",
                    "2018-02-18T02:30:00Z",
                    "2018-02-18T03:30:00Z",
                    "2018-11-04T01:00:00Z",
                    "2018-11-04T02:00:00Z",
                    "2018-11-04T03:00:00Z",
                    "2018-11-04T04:00:00Z",
                ],
                Some("America/Sao_Paulo".into()),
                vec![
                    "2018-02-17T22:00:00-02",
                    "2018-02-17T23:00:00-02",
                    "2018-02-17T23:00:00-03",
                    "2018-02-18T00:00:00-03",
                    "2018-11-03T22:00:00-03",
                    "2018-11-03T23:00:00-03",
                    "2018-11-04T01:00:00-02",
                    "2018-11-04T02:00:00-02",
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
                ColumnarValue::Scalar(ScalarValue::from("hour")),
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
    fn test_make_date() {
        let res = make_date(&[
            ColumnarValue::Scalar(ScalarValue::Int32(Some(2024))),
            ColumnarValue::Scalar(ScalarValue::Int64(Some(1))),
            ColumnarValue::Scalar(ScalarValue::UInt32(Some(14))),
        ])
        .expect("that make_date parsed values without error");

        if let ColumnarValue::Scalar(ScalarValue::Date32(date)) = res {
            assert_eq!(19736, date.unwrap());
        } else {
            panic!("Expected a scalar value")
        }

        let res = make_date(&[
            ColumnarValue::Scalar(ScalarValue::Int64(Some(2024))),
            ColumnarValue::Scalar(ScalarValue::UInt64(Some(1))),
            ColumnarValue::Scalar(ScalarValue::UInt32(Some(14))),
        ])
        .expect("that make_date parsed values without error");

        if let ColumnarValue::Scalar(ScalarValue::Date32(date)) = res {
            assert_eq!(19736, date.unwrap());
        } else {
            panic!("Expected a scalar value")
        }

        let res = make_date(&[
            ColumnarValue::Scalar(ScalarValue::Utf8(Some("2024".to_string()))),
            ColumnarValue::Scalar(ScalarValue::LargeUtf8(Some("1".to_string()))),
            ColumnarValue::Scalar(ScalarValue::Utf8(Some("14".to_string()))),
        ])
        .expect("that make_date parsed values without error");

        if let ColumnarValue::Scalar(ScalarValue::Date32(date)) = res {
            assert_eq!(19736, date.unwrap());
        } else {
            panic!("Expected a scalar value")
        }

        let years = Arc::new((2021..2025).map(Some).collect::<Int64Array>());
        let months = Arc::new((1..5).map(Some).collect::<Int32Array>());
        let days = Arc::new((11..15).map(Some).collect::<UInt32Array>());
        let res = make_date(&[
            ColumnarValue::Array(years),
            ColumnarValue::Array(months),
            ColumnarValue::Array(days),
        ])
        .expect("that make_date parsed values without error");

        if let ColumnarValue::Array(array) = res {
            assert_eq!(array.len(), 4);
            let mut builder = Date32Array::builder(4);
            builder.append_value(18_638);
            builder.append_value(19_035);
            builder.append_value(19_429);
            builder.append_value(19_827);
            assert_eq!(&builder.finish() as &dyn Array, array.as_ref());
        } else {
            panic!("Expected a columnar array")
        }

        //
        // Fallible test cases
        //

        // invalid number of arguments
        let res = make_date(&[ColumnarValue::Scalar(ScalarValue::Int32(Some(1)))]);
        assert_eq!(
            res.err().unwrap().strip_backtrace(),
            "Execution error: make_date function requires 3 arguments, got 1"
        );

        // invalid type
        let res = make_date(&[
            ColumnarValue::Scalar(ScalarValue::IntervalYearMonth(Some(1))),
            ColumnarValue::Scalar(ScalarValue::TimestampNanosecond(Some(1), None)),
            ColumnarValue::Scalar(ScalarValue::TimestampNanosecond(Some(1), None)),
        ]);
        assert_eq!(
            res.err().unwrap().strip_backtrace(),
            "Arrow error: Cast error: Casting from Interval(YearMonth) to Int32 not supported"
        );

        // overflow of month
        let res = make_date(&[
            ColumnarValue::Scalar(ScalarValue::Int32(Some(2023))),
            ColumnarValue::Scalar(ScalarValue::UInt64(Some(u64::MAX))),
            ColumnarValue::Scalar(ScalarValue::Int32(Some(22))),
        ]);
        assert_eq!(
            res.err().unwrap().strip_backtrace(),
            "Arrow error: Cast error: Can't cast value 18446744073709551615 to type Int32"
        );

        // overflow of day
        let res = make_date(&[
            ColumnarValue::Scalar(ScalarValue::Int32(Some(2023))),
            ColumnarValue::Scalar(ScalarValue::Int32(Some(22))),
            ColumnarValue::Scalar(ScalarValue::UInt32(Some(u32::MAX))),
        ]);
        assert_eq!(
            res.err().unwrap().strip_backtrace(),
            "Arrow error: Cast error: Can't cast value 4294967295 to type Int32"
        );
    }

    #[test]
    fn test_to_char() {
        let date = "2020-01-02T03:04:05"
            .parse::<NaiveDateTime>()
            .unwrap()
            .with_nanosecond(12345)
            .unwrap();
        let date2 = "2026-07-08T09:10:11"
            .parse::<NaiveDateTime>()
            .unwrap()
            .with_nanosecond(56789)
            .unwrap();

        let scalar_data = vec![
            (
                ScalarValue::Date32(Some(18506)),
                ScalarValue::Utf8(Some("%Y::%m::%d".to_string())),
                "2020::09::01".to_string(),
            ),
            (
                ScalarValue::Date64(Some(date.timestamp_millis())),
                ScalarValue::Utf8(Some("%Y::%m::%d".to_string())),
                "2020::01::02".to_string(),
            ),
            (
                ScalarValue::Time32Second(Some(31851)),
                ScalarValue::Utf8(Some("%H-%M-%S".to_string())),
                "08-50-51".to_string(),
            ),
            (
                ScalarValue::Time32Millisecond(Some(18506000)),
                ScalarValue::Utf8(Some("%H-%M-%S".to_string())),
                "05-08-26".to_string(),
            ),
            (
                ScalarValue::Time64Microsecond(Some(12344567000)),
                ScalarValue::Utf8(Some("%H-%M-%S %f".to_string())),
                "03-25-44 567000000".to_string(),
            ),
            (
                ScalarValue::Time64Nanosecond(Some(12344567890000)),
                ScalarValue::Utf8(Some("%H-%M-%S %f".to_string())),
                "03-25-44 567890000".to_string(),
            ),
            (
                ScalarValue::TimestampSecond(Some(date.timestamp()), None),
                ScalarValue::Utf8(Some("%Y::%m::%d %S::%M::%H".to_string())),
                "2020::01::02 05::04::03".to_string(),
            ),
            (
                ScalarValue::TimestampMillisecond(Some(date.timestamp_millis()), None),
                ScalarValue::Utf8(Some("%Y::%m::%d %S::%M::%H".to_string())),
                "2020::01::02 05::04::03".to_string(),
            ),
            (
                ScalarValue::TimestampMicrosecond(Some(date.timestamp_micros()), None),
                ScalarValue::Utf8(Some("%Y::%m::%d %S::%M::%H %f".to_string())),
                "2020::01::02 05::04::03 000012000".to_string(),
            ),
            (
                ScalarValue::TimestampNanosecond(
                    Some(date.timestamp_nanos_opt().unwrap()),
                    None,
                ),
                ScalarValue::Utf8(Some("%Y::%m::%d %S::%M::%H %f".to_string())),
                "2020::01::02 05::04::03 000012345".to_string(),
            ),
        ];

        for (value, format, expected) in scalar_data {
            let result =
                to_char(&[ColumnarValue::Scalar(value), ColumnarValue::Scalar(format)])
                    .expect("that to_char parsed values without error");

            if let ColumnarValue::Scalar(ScalarValue::Utf8(date)) = result {
                assert_eq!(expected, date.unwrap());
            } else {
                panic!("Expected a scalar value")
            }
        }

        let scalar_array_data = vec![
            (
                ScalarValue::Date32(Some(18506)),
                StringArray::from(vec!["%Y::%m::%d".to_string()]),
                "2020::09::01".to_string(),
            ),
            (
                ScalarValue::Date64(Some(date.timestamp_millis())),
                StringArray::from(vec!["%Y::%m::%d".to_string()]),
                "2020::01::02".to_string(),
            ),
            (
                ScalarValue::Time32Second(Some(31851)),
                StringArray::from(vec!["%H-%M-%S".to_string()]),
                "08-50-51".to_string(),
            ),
            (
                ScalarValue::Time32Millisecond(Some(18506000)),
                StringArray::from(vec!["%H-%M-%S".to_string()]),
                "05-08-26".to_string(),
            ),
            (
                ScalarValue::Time64Microsecond(Some(12344567000)),
                StringArray::from(vec!["%H-%M-%S %f".to_string()]),
                "03-25-44 567000000".to_string(),
            ),
            (
                ScalarValue::Time64Nanosecond(Some(12344567890000)),
                StringArray::from(vec!["%H-%M-%S %f".to_string()]),
                "03-25-44 567890000".to_string(),
            ),
            (
                ScalarValue::TimestampSecond(Some(date.timestamp()), None),
                StringArray::from(vec!["%Y::%m::%d %S::%M::%H".to_string()]),
                "2020::01::02 05::04::03".to_string(),
            ),
            (
                ScalarValue::TimestampMillisecond(Some(date.timestamp_millis()), None),
                StringArray::from(vec!["%Y::%m::%d %S::%M::%H".to_string()]),
                "2020::01::02 05::04::03".to_string(),
            ),
            (
                ScalarValue::TimestampMicrosecond(Some(date.timestamp_micros()), None),
                StringArray::from(vec!["%Y::%m::%d %S::%M::%H %f".to_string()]),
                "2020::01::02 05::04::03 000012000".to_string(),
            ),
            (
                ScalarValue::TimestampNanosecond(
                    Some(date.timestamp_nanos_opt().unwrap()),
                    None,
                ),
                StringArray::from(vec!["%Y::%m::%d %S::%M::%H %f".to_string()]),
                "2020::01::02 05::04::03 000012345".to_string(),
            ),
        ];

        for (value, format, expected) in scalar_array_data {
            let result = to_char(&[
                ColumnarValue::Scalar(value),
                ColumnarValue::Array(Arc::new(format) as ArrayRef),
            ])
            .expect("that to_char parsed values without error");

            if let ColumnarValue::Scalar(ScalarValue::Utf8(date)) = result {
                assert_eq!(expected, date.unwrap());
            } else {
                panic!("Expected a scalar value")
            }
        }

        let array_scalar_data = vec![
            (
                Arc::new(Date32Array::from(vec![18506, 18507])) as ArrayRef,
                ScalarValue::Utf8(Some("%Y::%m::%d".to_string())),
                StringArray::from(vec!["2020::09::01", "2020::09::02"]),
            ),
            (
                Arc::new(Date64Array::from(vec![
                    date.timestamp_millis(),
                    date2.timestamp_millis(),
                ])) as ArrayRef,
                ScalarValue::Utf8(Some("%Y::%m::%d".to_string())),
                StringArray::from(vec!["2020::01::02", "2026::07::08"]),
            ),
        ];

        let array_array_data = vec![
            (
                Arc::new(Date32Array::from(vec![18506, 18507])) as ArrayRef,
                StringArray::from(vec!["%Y::%m::%d", "%d::%m::%Y"]),
                StringArray::from(vec!["2020::09::01", "02::09::2020"]),
            ),
            (
                Arc::new(Date64Array::from(vec![
                    date.timestamp_millis(),
                    date2.timestamp_millis(),
                ])) as ArrayRef,
                StringArray::from(vec!["%Y::%m::%d", "%d::%m::%Y"]),
                StringArray::from(vec!["2020::01::02", "08::07::2026"]),
            ),
            (
                Arc::new(Time32MillisecondArray::from(vec![1850600, 1860700]))
                    as ArrayRef,
                StringArray::from(vec!["%H:%M:%S", "%H::%M::%S"]),
                StringArray::from(vec!["00:30:50", "00::31::00"]),
            ),
            (
                Arc::new(Time32SecondArray::from(vec![18506, 18507])) as ArrayRef,
                StringArray::from(vec!["%H:%M:%S", "%H::%M::%S"]),
                StringArray::from(vec!["05:08:26", "05::08::27"]),
            ),
            (
                Arc::new(Time64MicrosecondArray::from(vec![12344567000, 22244567000]))
                    as ArrayRef,
                StringArray::from(vec!["%H:%M:%S", "%H::%M::%S"]),
                StringArray::from(vec!["03:25:44", "06::10::44"]),
            ),
            (
                Arc::new(Time64NanosecondArray::from(vec![
                    1234456789000,
                    2224456789000,
                ])) as ArrayRef,
                StringArray::from(vec!["%H:%M:%S", "%H::%M::%S"]),
                StringArray::from(vec!["00:20:34", "00::37::04"]),
            ),
            (
                Arc::new(TimestampSecondArray::from(vec![
                    date.timestamp(),
                    date2.timestamp(),
                ])) as ArrayRef,
                StringArray::from(vec!["%Y::%m::%d %S::%M::%H", "%d::%m::%Y %S-%M-%H"]),
                StringArray::from(vec![
                    "2020::01::02 05::04::03",
                    "08::07::2026 11-10-09",
                ]),
            ),
            (
                Arc::new(TimestampMillisecondArray::from(vec![
                    date.timestamp_millis(),
                    date2.timestamp_millis(),
                ])) as ArrayRef,
                StringArray::from(vec![
                    "%Y::%m::%d %S::%M::%H %f",
                    "%d::%m::%Y %S-%M-%H %f",
                ]),
                StringArray::from(vec![
                    "2020::01::02 05::04::03 000000000",
                    "08::07::2026 11-10-09 000000000",
                ]),
            ),
            (
                Arc::new(TimestampMicrosecondArray::from(vec![
                    date.timestamp_micros(),
                    date2.timestamp_micros(),
                ])) as ArrayRef,
                StringArray::from(vec![
                    "%Y::%m::%d %S::%M::%H %f",
                    "%d::%m::%Y %S-%M-%H %f",
                ]),
                StringArray::from(vec![
                    "2020::01::02 05::04::03 000012000",
                    "08::07::2026 11-10-09 000056000",
                ]),
            ),
            (
                Arc::new(TimestampNanosecondArray::from(vec![
                    date.timestamp_nanos_opt().unwrap(),
                    date2.timestamp_nanos_opt().unwrap(),
                ])) as ArrayRef,
                StringArray::from(vec![
                    "%Y::%m::%d %S::%M::%H %f",
                    "%d::%m::%Y %S-%M-%H %f",
                ]),
                StringArray::from(vec![
                    "2020::01::02 05::04::03 000012345",
                    "08::07::2026 11-10-09 000056789",
                ]),
            ),
        ];

        for (value, format, expected) in array_scalar_data {
            let result = to_char(&[
                ColumnarValue::Array(value as ArrayRef),
                ColumnarValue::Scalar(format),
            ])
            .expect("that to_char parsed values without error");

            if let ColumnarValue::Array(result) = result {
                assert_eq!(result.len(), 2);
                assert_eq!(&expected as &dyn Array, result.as_ref());
            } else {
                panic!("Expected an array value")
            }
        }

        for (value, format, expected) in array_array_data {
            let result = to_char(&[
                ColumnarValue::Array(value),
                ColumnarValue::Array(Arc::new(format) as ArrayRef),
            ])
            .expect("that to_char parsed values without error");

            if let ColumnarValue::Array(result) = result {
                assert_eq!(result.len(), 2);
                assert_eq!(&expected as &dyn Array, result.as_ref());
            } else {
                panic!("Expected an array value")
            }
        }

        //
        // Fallible test cases
        //

        // invalid number of arguments
        let result = to_char(&[ColumnarValue::Scalar(ScalarValue::Int32(Some(1)))]);
        assert_eq!(
            result.err().unwrap().strip_backtrace(),
            "Execution error: to_char function requires 2 arguments, got 1"
        );

        // invalid type
        let result = to_char(&[
            ColumnarValue::Scalar(ScalarValue::Int32(Some(1))),
            ColumnarValue::Scalar(ScalarValue::TimestampNanosecond(Some(1), None)),
        ]);
        assert_eq!(
            result.err().unwrap().strip_backtrace(),
            "Execution error: Format for `to_char` must be non-null Utf8, received Timestamp(Nanosecond, None)"
        );
    }
}
