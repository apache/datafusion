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
use std::num::NonZeroI64;
use std::ops::{Add, Sub};
use std::str::FromStr;
use std::sync::Arc;

use arrow::array::temporal_conversions::{
    MICROSECONDS, MILLISECONDS, NANOSECONDS, as_datetime_with_timezone,
    timestamp_ns_to_datetime,
};
use arrow::array::timezone::Tz;
use arrow::array::types::{
    ArrowTimestampType, Time32MillisecondType, Time32SecondType, Time64MicrosecondType,
    Time64NanosecondType, TimestampMicrosecondType, TimestampMillisecondType,
    TimestampNanosecondType, TimestampSecondType,
};
use arrow::array::{Array, ArrayRef, PrimitiveArray};
use arrow::datatypes::DataType::{self, Time32, Time64, Timestamp};
use arrow::datatypes::TimeUnit::{self, Microsecond, Millisecond, Nanosecond, Second};
use datafusion_common::cast::as_primitive_array;
use datafusion_common::types::{NativeType, logical_date, logical_string};
use datafusion_common::{
    DataFusionError, Result, ScalarValue, exec_datafusion_err, exec_err,
};
use datafusion_expr::sort_properties::{ExprProperties, SortProperties};
use datafusion_expr::{
    ColumnarValue, Documentation, ScalarUDFImpl, Signature, TypeSignature, Volatility,
};
use datafusion_expr_common::signature::{Coercion, TypeSignatureClass};
use datafusion_macros::user_doc;

use chrono::{
    DateTime, Datelike, Duration, LocalResult, NaiveDateTime, Offset, TimeDelta, Timelike,
};

/// Represents the granularity for date truncation operations
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum DateTruncGranularity {
    Microsecond,
    Millisecond,
    Second,
    Minute,
    Hour,
    Day,
    Week,
    Month,
    Quarter,
    Year,
}

impl DateTruncGranularity {
    /// List of all supported granularity values
    /// Cannot use HashMap here as it would require lazy_static or once_cell,
    /// Rust does not support const HashMap yet.
    const SUPPORTED_GRANULARITIES: &[&str] = &[
        "microsecond",
        "millisecond",
        "second",
        "minute",
        "hour",
        "day",
        "week",
        "month",
        "quarter",
        "year",
    ];

    /// Parse a granularity string into a DateTruncGranularity enum
    fn from_str(s: &str) -> Result<Self> {
        // Using match for O(1) lookup - compiler optimizes this into a jump table or perfect hash
        match s.to_lowercase().as_str() {
            "microsecond" => Ok(Self::Microsecond),
            "millisecond" => Ok(Self::Millisecond),
            "second" => Ok(Self::Second),
            "minute" => Ok(Self::Minute),
            "hour" => Ok(Self::Hour),
            "day" => Ok(Self::Day),
            "week" => Ok(Self::Week),
            "month" => Ok(Self::Month),
            "quarter" => Ok(Self::Quarter),
            "year" => Ok(Self::Year),
            _ => {
                let supported = Self::SUPPORTED_GRANULARITIES.join(", ");
                exec_err!(
                    "Unsupported date_trunc granularity: '{s}'. Supported values are: {supported}"
                )
            }
        }
    }

    /// Returns true if this granularity can be handled with simple arithmetic
    /// (fine granularity: second, minute, millisecond, microsecond)
    fn is_fine_granularity(&self) -> bool {
        matches!(
            self,
            Self::Second | Self::Minute | Self::Millisecond | Self::Microsecond
        )
    }

    /// Returns true if this granularity can be handled with simple arithmetic in UTC
    /// (hour and day in addition to fine granularities)
    fn is_fine_granularity_utc(&self) -> bool {
        self.is_fine_granularity() || matches!(self, Self::Hour | Self::Day)
    }

    /// Returns true if this granularity is valid for Time types
    /// Time types don't have date components, so day/week/month/quarter/year are not valid
    fn valid_for_time(&self) -> bool {
        matches!(
            self,
            Self::Hour
                | Self::Minute
                | Self::Second
                | Self::Millisecond
                | Self::Microsecond
        )
    }
}

#[user_doc(
    doc_section(label = "Time and Date Functions"),
    description = "Truncates a timestamp or time value to a specified precision.",
    syntax_example = "date_trunc(precision, expression)",
    argument(
        name = "precision",
        description = r#"Time precision to truncate to. The following precisions are supported:

    For Timestamp types:
    - year / YEAR
    - quarter / QUARTER
    - month / MONTH
    - week / WEEK
    - day / DAY
    - hour / HOUR
    - minute / MINUTE
    - second / SECOND
    - millisecond / MILLISECOND
    - microsecond / MICROSECOND

    For Time types (hour, minute, second, millisecond, microsecond only):
    - hour / HOUR
    - minute / MINUTE
    - second / SECOND
    - millisecond / MILLISECOND
    - microsecond / MICROSECOND
"#
    ),
    argument(
        name = "expression",
        description = "Timestamp or time expression to operate on. Can be a constant, column, or function."
    )
)]
#[derive(Debug, PartialEq, Eq, Hash)]
pub struct DateTruncFunc {
    signature: Signature,
    aliases: Vec<String>,
}

impl Default for DateTruncFunc {
    fn default() -> Self {
        Self::new()
    }
}

impl DateTruncFunc {
    pub fn new() -> Self {
        Self {
            signature: Signature::one_of(
                vec![
                    TypeSignature::Coercible(vec![
                        Coercion::new_exact(TypeSignatureClass::Native(logical_string())),
                        Coercion::new_implicit(
                            TypeSignatureClass::Timestamp,
                            // Allow implicit cast from string and date to timestamp for backward compatibility
                            vec![
                                TypeSignatureClass::Native(logical_string()),
                                TypeSignatureClass::Native(logical_date()),
                            ],
                            NativeType::Timestamp(Nanosecond, None),
                        ),
                    ]),
                    TypeSignature::Coercible(vec![
                        Coercion::new_exact(TypeSignatureClass::Native(logical_string())),
                        Coercion::new_exact(TypeSignatureClass::Time),
                    ]),
                ],
                Volatility::Immutable,
            ),
            aliases: vec![String::from("datetrunc")],
        }
    }
}

impl ScalarUDFImpl for DateTruncFunc {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "date_trunc"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        if arg_types[1].is_null() {
            Ok(Timestamp(Nanosecond, None))
        } else {
            Ok(arg_types[1].clone())
        }
    }

    fn invoke_with_args(
        &self,
        args: datafusion_expr::ScalarFunctionArgs,
    ) -> Result<ColumnarValue> {
        let args = args.args;
        let (granularity, array) = (&args[0], &args[1]);

        let granularity_str = if let ColumnarValue::Scalar(ScalarValue::Utf8(Some(v))) =
            granularity
        {
            v.to_lowercase()
        } else if let ColumnarValue::Scalar(ScalarValue::Utf8View(Some(v))) = granularity
        {
            v.to_lowercase()
        } else if let ColumnarValue::Scalar(ScalarValue::LargeUtf8(Some(v))) = granularity
        {
            v.to_lowercase()
        } else {
            return exec_err!("Granularity of `date_trunc` must be non-null scalar Utf8");
        };

        let granularity = DateTruncGranularity::from_str(&granularity_str)?;

        // Check upfront if granularity is valid for Time types
        let is_time_type = matches!(array.data_type(), Time64(_) | Time32(_));
        if is_time_type && !granularity.valid_for_time() {
            return exec_err!(
                "date_trunc does not support '{}' granularity for Time types. Valid values are: hour, minute, second, millisecond, microsecond",
                granularity_str
            );
        }

        fn process_array<T: ArrowTimestampType>(
            array: &dyn Array,
            granularity: DateTruncGranularity,
            tz_opt: &Option<Arc<str>>,
        ) -> Result<ColumnarValue> {
            let parsed_tz = parse_tz(tz_opt)?;
            let array = as_primitive_array::<T>(array)?;

            // fast path for fine granularity
            // For modern timezones, it's correct to truncate "minute" in this way.
            // Both datafusion and arrow are ignoring historical timezone's non-minute granularity
            // bias (e.g., Asia/Kathmandu before 1919 is UTC+05:41:16).
            // In UTC, "hour" and "day" have uniform durations and can be truncated with simple arithmetic
            if granularity.is_fine_granularity()
                || (parsed_tz.is_none() && granularity.is_fine_granularity_utc())
            {
                let result = general_date_trunc_array_fine_granularity(
                    T::UNIT,
                    array,
                    granularity,
                    tz_opt.clone(),
                )?;
                return Ok(ColumnarValue::Array(result));
            }

            let array: PrimitiveArray<T> = array
                .try_unary(|x| general_date_trunc(T::UNIT, x, parsed_tz, granularity))?
                .with_timezone_opt(tz_opt.clone());
            Ok(ColumnarValue::Array(Arc::new(array)))
        }

        fn process_scalar<T: ArrowTimestampType>(
            v: &Option<i64>,
            granularity: DateTruncGranularity,
            tz_opt: &Option<Arc<str>>,
        ) -> Result<ColumnarValue> {
            let parsed_tz = parse_tz(tz_opt)?;
            let value = if let Some(v) = v {
                Some(general_date_trunc(T::UNIT, *v, parsed_tz, granularity)?)
            } else {
                None
            };
            let value = ScalarValue::new_timestamp::<T>(value, tz_opt.clone());
            Ok(ColumnarValue::Scalar(value))
        }

        Ok(match array {
            ColumnarValue::Scalar(ScalarValue::Null) => {
                // NULL input returns NULL timestamp
                ColumnarValue::Scalar(ScalarValue::TimestampNanosecond(None, None))
            }
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
            ColumnarValue::Scalar(ScalarValue::Time64Nanosecond(v)) => {
                let truncated = v.map(|val| truncate_time_nanos(val, granularity));
                ColumnarValue::Scalar(ScalarValue::Time64Nanosecond(truncated))
            }
            ColumnarValue::Scalar(ScalarValue::Time64Microsecond(v)) => {
                let truncated = v.map(|val| truncate_time_micros(val, granularity));
                ColumnarValue::Scalar(ScalarValue::Time64Microsecond(truncated))
            }
            ColumnarValue::Scalar(ScalarValue::Time32Millisecond(v)) => {
                let truncated = v.map(|val| truncate_time_millis(val, granularity));
                ColumnarValue::Scalar(ScalarValue::Time32Millisecond(truncated))
            }
            ColumnarValue::Scalar(ScalarValue::Time32Second(v)) => {
                let truncated = v.map(|val| truncate_time_secs(val, granularity));
                ColumnarValue::Scalar(ScalarValue::Time32Second(truncated))
            }
            ColumnarValue::Array(array) => {
                let array_type = array.data_type();
                match array_type {
                    Timestamp(Second, tz_opt) => {
                        process_array::<TimestampSecondType>(array, granularity, tz_opt)?
                    }
                    Timestamp(Millisecond, tz_opt) => process_array::<
                        TimestampMillisecondType,
                    >(
                        array, granularity, tz_opt
                    )?,
                    Timestamp(Microsecond, tz_opt) => process_array::<
                        TimestampMicrosecondType,
                    >(
                        array, granularity, tz_opt
                    )?,
                    Timestamp(Nanosecond, tz_opt) => process_array::<
                        TimestampNanosecondType,
                    >(
                        array, granularity, tz_opt
                    )?,
                    Time64(Nanosecond) => {
                        let arr = as_primitive_array::<Time64NanosecondType>(array)?;
                        let result: PrimitiveArray<Time64NanosecondType> =
                            arr.unary(|v| truncate_time_nanos(v, granularity));
                        ColumnarValue::Array(Arc::new(result))
                    }
                    Time64(Microsecond) => {
                        let arr = as_primitive_array::<Time64MicrosecondType>(array)?;
                        let result: PrimitiveArray<Time64MicrosecondType> =
                            arr.unary(|v| truncate_time_micros(v, granularity));
                        ColumnarValue::Array(Arc::new(result))
                    }
                    Time32(Millisecond) => {
                        let arr = as_primitive_array::<Time32MillisecondType>(array)?;
                        let result: PrimitiveArray<Time32MillisecondType> =
                            arr.unary(|v| truncate_time_millis(v, granularity));
                        ColumnarValue::Array(Arc::new(result))
                    }
                    Time32(Second) => {
                        let arr = as_primitive_array::<Time32SecondType>(array)?;
                        let result: PrimitiveArray<Time32SecondType> =
                            arr.unary(|v| truncate_time_secs(v, granularity));
                        ColumnarValue::Array(Arc::new(result))
                    }
                    _ => {
                        return exec_err!(
                            "second argument of `date_trunc` is an unsupported array type: {array_type}"
                        );
                    }
                }
            }
            _ => {
                return exec_err!(
                    "second argument of `date_trunc` must be timestamp, time scalar or array"
                );
            }
        })
    }

    fn aliases(&self) -> &[String] {
        &self.aliases
    }

    fn output_ordering(&self, input: &[ExprProperties]) -> Result<SortProperties> {
        // The DATE_TRUNC function preserves the order of its second argument.
        let precision = &input[0];
        let date_value = &input[1];

        if precision.sort_properties.eq(&SortProperties::Singleton) {
            Ok(date_value.sort_properties)
        } else {
            Ok(SortProperties::Unordered)
        }
    }
    fn documentation(&self) -> Option<&Documentation> {
        self.doc()
    }
}

const NANOS_PER_MICROSECOND: i64 = NANOSECONDS / MICROSECONDS;
const NANOS_PER_MILLISECOND: i64 = NANOSECONDS / MILLISECONDS;
const NANOS_PER_SECOND: i64 = NANOSECONDS;
const NANOS_PER_MINUTE: i64 = 60 * NANOS_PER_SECOND;
const NANOS_PER_HOUR: i64 = 60 * NANOS_PER_MINUTE;

const MICROS_PER_MILLISECOND: i64 = MICROSECONDS / MILLISECONDS;
const MICROS_PER_SECOND: i64 = MICROSECONDS;
const MICROS_PER_MINUTE: i64 = 60 * MICROS_PER_SECOND;
const MICROS_PER_HOUR: i64 = 60 * MICROS_PER_MINUTE;

const MILLIS_PER_SECOND: i32 = MILLISECONDS as i32;
const MILLIS_PER_MINUTE: i32 = 60 * MILLIS_PER_SECOND;
const MILLIS_PER_HOUR: i32 = 60 * MILLIS_PER_MINUTE;

const SECS_PER_MINUTE: i32 = 60;
const SECS_PER_HOUR: i32 = 60 * SECS_PER_MINUTE;

/// Truncate time in nanoseconds to the specified granularity
fn truncate_time_nanos(value: i64, granularity: DateTruncGranularity) -> i64 {
    match granularity {
        DateTruncGranularity::Hour => value - (value % NANOS_PER_HOUR),
        DateTruncGranularity::Minute => value - (value % NANOS_PER_MINUTE),
        DateTruncGranularity::Second => value - (value % NANOS_PER_SECOND),
        DateTruncGranularity::Millisecond => value - (value % NANOS_PER_MILLISECOND),
        DateTruncGranularity::Microsecond => value - (value % NANOS_PER_MICROSECOND),
        // Other granularities are not valid for time - should be caught earlier
        _ => value,
    }
}

/// Truncate time in microseconds to the specified granularity
fn truncate_time_micros(value: i64, granularity: DateTruncGranularity) -> i64 {
    match granularity {
        DateTruncGranularity::Hour => value - (value % MICROS_PER_HOUR),
        DateTruncGranularity::Minute => value - (value % MICROS_PER_MINUTE),
        DateTruncGranularity::Second => value - (value % MICROS_PER_SECOND),
        DateTruncGranularity::Millisecond => value - (value % MICROS_PER_MILLISECOND),
        DateTruncGranularity::Microsecond => value, // Already at microsecond precision
        // Other granularities are not valid for time
        _ => value,
    }
}

/// Truncate time in milliseconds to the specified granularity
fn truncate_time_millis(value: i32, granularity: DateTruncGranularity) -> i32 {
    match granularity {
        DateTruncGranularity::Hour => value - (value % MILLIS_PER_HOUR),
        DateTruncGranularity::Minute => value - (value % MILLIS_PER_MINUTE),
        DateTruncGranularity::Second => value - (value % MILLIS_PER_SECOND),
        DateTruncGranularity::Millisecond => value, // Already at millisecond precision
        DateTruncGranularity::Microsecond => value, // Can't truncate to finer precision
        // Other granularities are not valid for time
        _ => value,
    }
}

/// Truncate time in seconds to the specified granularity
fn truncate_time_secs(value: i32, granularity: DateTruncGranularity) -> i32 {
    match granularity {
        DateTruncGranularity::Hour => value - (value % SECS_PER_HOUR),
        DateTruncGranularity::Minute => value - (value % SECS_PER_MINUTE),
        DateTruncGranularity::Second => value, // Already at second precision
        DateTruncGranularity::Millisecond => value, // Can't truncate to finer precision
        DateTruncGranularity::Microsecond => value, // Can't truncate to finer precision
        // Other granularities are not valid for time
        _ => value,
    }
}

fn _date_trunc_coarse<T>(
    granularity: DateTruncGranularity,
    value: Option<T>,
) -> Result<Option<T>>
where
    T: Datelike + Timelike + Sub<Duration, Output = T> + Copy,
{
    let value = match granularity {
        DateTruncGranularity::Millisecond => value,
        DateTruncGranularity::Microsecond => value,
        DateTruncGranularity::Second => value.and_then(|d| d.with_nanosecond(0)),
        DateTruncGranularity::Minute => value
            .and_then(|d| d.with_nanosecond(0))
            .and_then(|d| d.with_second(0)),
        DateTruncGranularity::Hour => value
            .and_then(|d| d.with_nanosecond(0))
            .and_then(|d| d.with_second(0))
            .and_then(|d| d.with_minute(0)),
        DateTruncGranularity::Day => value
            .and_then(|d| d.with_nanosecond(0))
            .and_then(|d| d.with_second(0))
            .and_then(|d| d.with_minute(0))
            .and_then(|d| d.with_hour(0)),
        DateTruncGranularity::Week => value
            .and_then(|d| d.with_nanosecond(0))
            .and_then(|d| d.with_second(0))
            .and_then(|d| d.with_minute(0))
            .and_then(|d| d.with_hour(0))
            .map(|d| {
                d - TimeDelta::try_seconds(60 * 60 * 24 * d.weekday() as i64).unwrap()
            }),
        DateTruncGranularity::Month => value
            .and_then(|d| d.with_nanosecond(0))
            .and_then(|d| d.with_second(0))
            .and_then(|d| d.with_minute(0))
            .and_then(|d| d.with_hour(0))
            .and_then(|d| d.with_day0(0)),
        DateTruncGranularity::Quarter => value
            .and_then(|d| d.with_nanosecond(0))
            .and_then(|d| d.with_second(0))
            .and_then(|d| d.with_minute(0))
            .and_then(|d| d.with_hour(0))
            .and_then(|d| d.with_day0(0))
            .and_then(|d| d.with_month(quarter_month(&d))),
        DateTruncGranularity::Year => value
            .and_then(|d| d.with_nanosecond(0))
            .and_then(|d| d.with_second(0))
            .and_then(|d| d.with_minute(0))
            .and_then(|d| d.with_hour(0))
            .and_then(|d| d.with_day0(0))
            .and_then(|d| d.with_month0(0)),
    };
    Ok(value)
}

fn quarter_month<T>(date: &T) -> u32
where
    T: Datelike,
{
    1 + 3 * ((date.month() - 1) / 3)
}

fn _date_trunc_coarse_with_tz(
    granularity: DateTruncGranularity,
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
                        .sub(TimeDelta::try_hours(3).unwrap())
                        .and_local_timezone(value.timezone())
                        .single()
                        .map(|v| v.add(TimeDelta::try_hours(3).unwrap()))
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
    granularity: DateTruncGranularity,
    value: Option<NaiveDateTime>,
) -> Result<Option<i64>> {
    let value = _date_trunc_coarse::<NaiveDateTime>(granularity, value)?;
    Ok(value.and_then(|value| value.and_utc().timestamp_nanos_opt()))
}

/// Truncates the single `value`, expressed in nanoseconds since the
/// epoch, for granularities greater than 1 second, in taking into
/// account that some granularities are not uniform durations of time
/// (e.g. months are not always the same lengths, leap seconds, etc)
fn date_trunc_coarse(
    granularity: DateTruncGranularity,
    value: i64,
    tz: Option<Tz>,
) -> Result<i64> {
    let value = match tz {
        Some(tz) => {
            // Use chrono DateTime<Tz> to clear the various fields because need to clear per timezone,
            // and NaiveDateTime (ISO 8601) has no concept of timezones
            let value = as_datetime_with_timezone::<TimestampNanosecondType>(value, tz)
                .ok_or(exec_datafusion_err!("Timestamp {value} out of range"))?;
            _date_trunc_coarse_with_tz(granularity, Some(value))
        }
        None => {
            // Use chrono NaiveDateTime to clear the various fields, if we don't have a timezone.
            let value = timestamp_ns_to_datetime(value)
                .ok_or_else(|| exec_datafusion_err!("Timestamp {value} out of range"))?;
            _date_trunc_coarse_without_tz(granularity, Some(value))
        }
    }?;

    // `with_x(0)` are infallible because `0` are always a valid
    Ok(value.unwrap())
}

/// Fast path for fine granularities (hour and smaller) that can be handled
/// with simple arithmetic operations without calendar complexity.
///
/// This function is timezone-agnostic and should only be used when:
/// - No timezone is specified in the input, OR
/// - The granularity is less than hour as hour can be affected by DST transitions in some cases
fn general_date_trunc_array_fine_granularity<T: ArrowTimestampType>(
    tu: TimeUnit,
    array: &PrimitiveArray<T>,
    granularity: DateTruncGranularity,
    tz_opt: Option<Arc<str>>,
) -> Result<ArrayRef> {
    let unit = match (tu, granularity) {
        (Second, DateTruncGranularity::Minute) => NonZeroI64::new(60),
        (Second, DateTruncGranularity::Hour) => NonZeroI64::new(3600),
        (Second, DateTruncGranularity::Day) => NonZeroI64::new(86400),

        (Millisecond, DateTruncGranularity::Second) => NonZeroI64::new(1_000),
        (Millisecond, DateTruncGranularity::Minute) => NonZeroI64::new(60_000),
        (Millisecond, DateTruncGranularity::Hour) => NonZeroI64::new(3_600_000),
        (Millisecond, DateTruncGranularity::Day) => NonZeroI64::new(86_400_000),

        (Microsecond, DateTruncGranularity::Millisecond) => NonZeroI64::new(1_000),
        (Microsecond, DateTruncGranularity::Second) => NonZeroI64::new(1_000_000),
        (Microsecond, DateTruncGranularity::Minute) => NonZeroI64::new(60_000_000),
        (Microsecond, DateTruncGranularity::Hour) => NonZeroI64::new(3_600_000_000),
        (Microsecond, DateTruncGranularity::Day) => NonZeroI64::new(86_400_000_000),

        (Nanosecond, DateTruncGranularity::Microsecond) => NonZeroI64::new(1_000),
        (Nanosecond, DateTruncGranularity::Millisecond) => NonZeroI64::new(1_000_000),
        (Nanosecond, DateTruncGranularity::Second) => NonZeroI64::new(1_000_000_000),
        (Nanosecond, DateTruncGranularity::Minute) => NonZeroI64::new(60_000_000_000),
        (Nanosecond, DateTruncGranularity::Hour) => NonZeroI64::new(3_600_000_000_000),
        (Nanosecond, DateTruncGranularity::Day) => NonZeroI64::new(86_400_000_000_000),
        _ => None,
    };

    if let Some(unit) = unit {
        let unit = unit.get();
        let array = PrimitiveArray::<T>::from_iter_values_with_nulls(
            array
                .values()
                .iter()
                .map(|v| *v - i64::rem_euclid(*v, unit)),
            array.nulls().cloned(),
        )
        .with_timezone_opt(tz_opt);
        Ok(Arc::new(array))
    } else {
        // truncate to the same or smaller unit
        Ok(Arc::new(array.clone()))
    }
}

// truncates a single value with the given timeunit to the specified granularity
fn general_date_trunc(
    tu: TimeUnit,
    value: i64,
    tz: Option<Tz>,
    granularity: DateTruncGranularity,
) -> Result<i64, DataFusionError> {
    let scale = match tu {
        Second => 1_000_000_000,
        Millisecond => 1_000_000,
        Microsecond => 1_000,
        Nanosecond => 1,
    };

    // convert to nanoseconds
    let nano = date_trunc_coarse(granularity, scale * value, tz)?;

    let result = match tu {
        Second => match granularity {
            DateTruncGranularity::Minute => nano / 1_000_000_000 / 60 * 60,
            _ => nano / 1_000_000_000,
        },
        Millisecond => match granularity {
            DateTruncGranularity::Minute => nano / 1_000_000 / 1_000 / 60 * 1_000 * 60,
            DateTruncGranularity::Second => nano / 1_000_000 / 1_000 * 1_000,
            _ => nano / 1_000_000,
        },
        Microsecond => match granularity {
            DateTruncGranularity::Minute => {
                nano / 1_000 / 1_000_000 / 60 * 60 * 1_000_000
            }
            DateTruncGranularity::Second => nano / 1_000 / 1_000_000 * 1_000_000,
            DateTruncGranularity::Millisecond => nano / 1_000 / 1_000 * 1_000,
            _ => nano / 1_000,
        },
        _ => match granularity {
            DateTruncGranularity::Minute => {
                nano / 1_000_000_000 / 60 * 1_000_000_000 * 60
            }
            DateTruncGranularity::Second => nano / 1_000_000_000 * 1_000_000_000,
            DateTruncGranularity::Millisecond => nano / 1_000_000 * 1_000_000,
            DateTruncGranularity::Microsecond => nano / 1_000 * 1_000,
            _ => nano,
        },
    };
    Ok(result)
}

fn parse_tz(tz: &Option<Arc<str>>) -> Result<Option<Tz>> {
    tz.as_ref()
        .map(|tz| {
            Tz::from_str(tz)
                .map_err(|op| exec_datafusion_err!("failed on timezone {tz}: {op:?}"))
        })
        .transpose()
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use crate::datetime::date_trunc::{
        DateTruncFunc, DateTruncGranularity, date_trunc_coarse,
    };

    use arrow::array::cast::as_primitive_array;
    use arrow::array::types::TimestampNanosecondType;
    use arrow::array::{Array, TimestampNanosecondArray};
    use arrow::compute::kernels::cast_utils::string_to_timestamp_nanos;
    use arrow::datatypes::{DataType, Field, TimeUnit};
    use datafusion_common::ScalarValue;
    use datafusion_common::config::ConfigOptions;
    use datafusion_expr::{ColumnarValue, ScalarUDFImpl};

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
            let granularity_enum = DateTruncGranularity::from_str(granularity).unwrap();
            let result = date_trunc_coarse(granularity_enum, left, None).unwrap();
            assert_eq!(result, right, "{original} = {expected}");
        });
    }

    #[test]
    fn test_date_trunc_timezones() {
        let cases = [
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
            let batch_len = input.len();
            let arg_fields = vec![
                Field::new("a", DataType::Utf8, false).into(),
                Field::new("b", input.data_type().clone(), false).into(),
            ];
            let args = datafusion_expr::ScalarFunctionArgs {
                args: vec![
                    ColumnarValue::Scalar(ScalarValue::from("day")),
                    ColumnarValue::Array(Arc::new(input)),
                ],
                arg_fields,
                number_rows: batch_len,
                return_field: Field::new(
                    "f",
                    DataType::Timestamp(TimeUnit::Nanosecond, tz_opt.clone()),
                    true,
                )
                .into(),
                config_options: Arc::new(ConfigOptions::default()),
            };
            let result = DateTruncFunc::new().invoke_with_args(args).unwrap();
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
        let cases = [
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
            (
                vec![
                    "2024-10-26T23:30:00Z",
                    "2024-10-27T00:30:00Z",
                    "2024-10-27T01:30:00Z",
                    "2024-10-27T02:30:00Z",
                ],
                Some("Asia/Kathmandu".into()), // UTC+5:45
                vec![
                    "2024-10-27T05:00:00+05:45",
                    "2024-10-27T06:00:00+05:45",
                    "2024-10-27T07:00:00+05:45",
                    "2024-10-27T08:00:00+05:45",
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
            let batch_len = input.len();
            let arg_fields = vec![
                Field::new("a", DataType::Utf8, false).into(),
                Field::new("b", input.data_type().clone(), false).into(),
            ];
            let args = datafusion_expr::ScalarFunctionArgs {
                args: vec![
                    ColumnarValue::Scalar(ScalarValue::from("hour")),
                    ColumnarValue::Array(Arc::new(input)),
                ],
                arg_fields,
                number_rows: batch_len,
                return_field: Field::new(
                    "f",
                    DataType::Timestamp(TimeUnit::Nanosecond, tz_opt.clone()),
                    true,
                )
                .into(),
                config_options: Arc::new(ConfigOptions::default()),
            };
            let result = DateTruncFunc::new().invoke_with_args(args).unwrap();
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
    fn test_date_trunc_fine_granularity_timezones() {
        let cases = [
            // Test "second" granularity
            (
                vec![
                    "2020-09-08T13:42:29.190855Z",
                    "2020-09-08T13:42:30.500000Z",
                    "2020-09-08T13:42:31.999999Z",
                ],
                Some("+00".into()),
                "second",
                vec![
                    "2020-09-08T13:42:29.000000Z",
                    "2020-09-08T13:42:30.000000Z",
                    "2020-09-08T13:42:31.000000Z",
                ],
            ),
            (
                vec![
                    "2020-09-08T13:42:29.190855+05",
                    "2020-09-08T13:42:30.500000+05",
                    "2020-09-08T13:42:31.999999+05",
                ],
                Some("+05".into()),
                "second",
                vec![
                    "2020-09-08T13:42:29.000000+05",
                    "2020-09-08T13:42:30.000000+05",
                    "2020-09-08T13:42:31.000000+05",
                ],
            ),
            (
                vec![
                    "2020-09-08T13:42:29.190855Z",
                    "2020-09-08T13:42:30.500000Z",
                    "2020-09-08T13:42:31.999999Z",
                ],
                Some("Europe/Berlin".into()),
                "second",
                vec![
                    "2020-09-08T13:42:29.000000Z",
                    "2020-09-08T13:42:30.000000Z",
                    "2020-09-08T13:42:31.000000Z",
                ],
            ),
            // Test "minute" granularity
            (
                vec![
                    "2020-09-08T13:42:29.190855Z",
                    "2020-09-08T13:43:30.500000Z",
                    "2020-09-08T13:44:31.999999Z",
                ],
                Some("+00".into()),
                "minute",
                vec![
                    "2020-09-08T13:42:00.000000Z",
                    "2020-09-08T13:43:00.000000Z",
                    "2020-09-08T13:44:00.000000Z",
                ],
            ),
            (
                vec![
                    "2020-09-08T13:42:29.190855+08",
                    "2020-09-08T13:43:30.500000+08",
                    "2020-09-08T13:44:31.999999+08",
                ],
                Some("+08".into()),
                "minute",
                vec![
                    "2020-09-08T13:42:00.000000+08",
                    "2020-09-08T13:43:00.000000+08",
                    "2020-09-08T13:44:00.000000+08",
                ],
            ),
            (
                vec![
                    "2020-09-08T13:42:29.190855Z",
                    "2020-09-08T13:43:30.500000Z",
                    "2020-09-08T13:44:31.999999Z",
                ],
                Some("America/Sao_Paulo".into()),
                "minute",
                vec![
                    "2020-09-08T13:42:00.000000Z",
                    "2020-09-08T13:43:00.000000Z",
                    "2020-09-08T13:44:00.000000Z",
                ],
            ),
            // Test with None (no timezone)
            (
                vec![
                    "2020-09-08T13:42:29.190855Z",
                    "2020-09-08T13:43:30.500000Z",
                    "2020-09-08T13:44:31.999999Z",
                ],
                None,
                "minute",
                vec![
                    "2020-09-08T13:42:00.000000Z",
                    "2020-09-08T13:43:00.000000Z",
                    "2020-09-08T13:44:00.000000Z",
                ],
            ),
            // Test millisecond granularity
            (
                vec![
                    "2020-09-08T13:42:29.190855Z",
                    "2020-09-08T13:42:29.191999Z",
                    "2020-09-08T13:42:29.192500Z",
                ],
                Some("Asia/Kolkata".into()),
                "millisecond",
                vec![
                    "2020-09-08T19:12:29.190000+05:30",
                    "2020-09-08T19:12:29.191000+05:30",
                    "2020-09-08T19:12:29.192000+05:30",
                ],
            ),
        ];

        cases
            .iter()
            .for_each(|(original, tz_opt, granularity, expected)| {
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
                let batch_len = input.len();
                let arg_fields = vec![
                    Field::new("a", DataType::Utf8, false).into(),
                    Field::new("b", input.data_type().clone(), false).into(),
                ];
                let args = datafusion_expr::ScalarFunctionArgs {
                    args: vec![
                        ColumnarValue::Scalar(ScalarValue::from(*granularity)),
                        ColumnarValue::Array(Arc::new(input)),
                    ],
                    arg_fields,
                    number_rows: batch_len,
                    return_field: Field::new(
                        "f",
                        DataType::Timestamp(TimeUnit::Nanosecond, tz_opt.clone()),
                        true,
                    )
                    .into(),
                    config_options: Arc::new(ConfigOptions::default()),
                };
                let result = DateTruncFunc::new().invoke_with_args(args).unwrap();
                if let ColumnarValue::Array(result) = result {
                    assert_eq!(
                        result.data_type(),
                        &DataType::Timestamp(TimeUnit::Nanosecond, tz_opt.clone()),
                        "Failed for granularity: {granularity}, timezone: {tz_opt:?}"
                    );
                    let left = as_primitive_array::<TimestampNanosecondType>(&result);
                    assert_eq!(
                        left, &right,
                        "Failed for granularity: {granularity}, timezone: {tz_opt:?}"
                    );
                } else {
                    panic!("unexpected column type");
                }
            });
    }
}
