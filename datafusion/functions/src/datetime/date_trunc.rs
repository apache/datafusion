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
use arrow::datatypes::DataType::{self, Date32, Date64, Time32, Time64, Timestamp};
use arrow::datatypes::TimeUnit::{self, Microsecond, Millisecond, Nanosecond, Second};
use arrow::datatypes::{Field, FieldRef};
use datafusion_common::cast::as_primitive_array;
use datafusion_common::types::{NativeType, logical_date, logical_string};
use datafusion_common::utils::take_function_args;
use datafusion_common::{
    DataFusionError, Result, ScalarValue, exec_datafusion_err, exec_err, internal_err,
};
use datafusion_expr::preimage::PreimageResult;
use datafusion_expr::simplify::SimplifyContext;
use datafusion_expr::sort_properties::{ExprProperties, SortProperties};
use datafusion_expr::{
    Cast, ColumnarValue, Documentation, Expr, ReturnFieldArgs, ScalarFunctionArgs,
    ScalarUDFImpl, Signature, TypeSignature, Volatility, interval_arithmetic::Interval,
};
use datafusion_expr_common::signature::{Coercion, TypeSignatureClass};
use datafusion_macros::user_doc;

use crate::datetime::common::date_to_scalar;

use chrono::{
    DateTime, Datelike, Duration, LocalResult, Months, NaiveDate, NaiveDateTime, Offset,
    TimeDelta, Timelike, Weekday,
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

    /// Returns true if this granularity has variable width when expressed in a
    /// timezone — i.e. it can span 23 or 25 wall-clock hours around DST.
    /// Used by `preimage` to stay conservative for timezone-aware coarse buckets.
    fn has_variable_width_in_tz(&self) -> bool {
        matches!(
            self,
            Self::Day | Self::Week | Self::Month | Self::Quarter | Self::Year
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
    ),
    sql_example = r#"```sql
> SELECT date_trunc('month', '2024-05-15T10:30:00');
+-----------------------------------------------+
| date_trunc(Utf8("month"),Utf8("2024-05-15T10:30:00")) |
+-----------------------------------------------+
| 2024-05-01T00:00:00                           |
+-----------------------------------------------+
> SELECT date_trunc('hour', '2024-05-15T10:30:00');
+----------------------------------------------+
| date_trunc(Utf8("hour"),Utf8("2024-05-15T10:30:00")) |
+----------------------------------------------+
| 2024-05-15T10:00:00                          |
+----------------------------------------------+
```"#
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
    fn name(&self) -> &str {
        "date_trunc"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        internal_err!("return_field_from_args should be called instead")
    }

    fn return_field_from_args(&self, args: ReturnFieldArgs) -> Result<FieldRef> {
        let field = &args.arg_fields[1];
        let return_type = if field.data_type().is_null() {
            Timestamp(Nanosecond, None)
        } else {
            field.data_type().clone()
        };
        Ok(Arc::new(Field::new(
            self.name(),
            return_type,
            field.is_nullable(),
        )))
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
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

    /// Compute the preimage for `date_trunc(granularity, col) = literal`.
    ///
    /// `date_trunc` floors a timestamp to the start of the given period, so
    /// for any aligned literal `L` the preimage is the half-open interval
    /// `[L, L + 1 granularity)`.
    fn preimage(
        &self,
        args: &[Expr],
        lit_expr: &Expr,
        info: &SimplifyContext,
    ) -> Result<PreimageResult> {
        let [precision, col_expr] = take_function_args(self.name(), args)?;

        // Granularity literal (e.g. "month").
        let Some(granularity) = precision
            .as_literal()
            .and_then(|sv| sv.try_as_str().flatten())
            .and_then(|s| DateTruncGranularity::from_str(s).ok())
        else {
            return Ok(PreimageResult::None);
        };

        // If the column is a Date32/Date64 wrapped in the signature's implicit
        // cast to Timestamp(ns), emit bounds in the Date type against the raw
        // column. `unwrap_cast_in_comparison` doesn't currently support Date
        // targets, so without this branch the cast survives in the plan and
        // blocks pruning on Date partition columns.
        if let Some(result) = date_column_preimage(col_expr, lit_expr, granularity, info)
        {
            return Ok(result);
        }

        // Time columns reach preimage as raw Time (the signature has a
        // dedicated Coercible variant for Time, no implicit cast). All valid
        // Time granularities are constant-step.
        if let Some(result) = time_column_preimage(col_expr, lit_expr, granularity, info)
        {
            return Ok(result);
        }

        // The bounds must match the column's type. The simplifier coerces the
        // literal to that type before calling `preimage`, but using
        // `info.get_data_type` as the source of truth (and matches `date_part`).
        // Date columns reach here as `Cast(_, Timestamp(ns))` and are fully
        // handled by the Date arm above; Time columns are handled by the Time
        // arm. Anything else bails.
        let target_type = info.get_data_type(col_expr)?;
        let (unit, tz_opt) = match &target_type {
            Timestamp(unit, tz) => (*unit, tz.clone()),
            _ => return Ok(PreimageResult::None),
        };

        // Coarse, timezone-aware buckets can have variable widths around DST.
        // Stay conservative for those.
        if tz_opt.is_some() && granularity.has_variable_width_in_tz() {
            return Ok(PreimageResult::None);
        }

        // Extract the literal value and verify it matches the column's type.
        let Some(value) = lit_expr
            .as_literal()
            .and_then(|lit| timestamp_value(lit, unit, &tz_opt))
        else {
            return Ok(PreimageResult::None);
        };

        let Some((lower, upper)) =
            timestamp_preimage_bounds(unit, value, &tz_opt, granularity)
        else {
            return Ok(PreimageResult::None);
        };

        Ok(PreimageResult::Range {
            expr: col_expr.clone(),
            interval: Box::new(Interval::try_new(
                timestamp_to_scalar(unit, lower, &tz_opt),
                timestamp_to_scalar(unit, upper, &tz_opt),
            )?),
        })
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

/// Extract an `i64` timestamp value from a literal that matches the column's
/// `(unit, tz)`. Returns `None` if the literal is null or not a matching
/// `Timestamp*` variant — the simplifier is responsible for coercing the
/// literal first, so a mismatch here means the rewrite is not applicable.
/// Peel the signature-inserted `Cast(date_col, Timestamp(ns))` wrapper and
/// emit Date-type preimage bounds against the raw column. Returns `None` if
/// the column isn't a Date wrapped in the expected cast, or if the literal
/// isn't a midnight-UTC value aligned to the granularity boundary.
fn date_column_preimage(
    col_expr: &Expr,
    lit_expr: &Expr,
    granularity: DateTruncGranularity,
    info: &SimplifyContext,
) -> Option<PreimageResult> {
    // The signature wraps Date columns as exactly Cast(_, Timestamp(Nanosecond, None)).
    let Expr::Cast(Cast { expr: inner, field }) = col_expr else {
        return None;
    };
    if !matches!(field.data_type(), Timestamp(Nanosecond, None)) {
        return None;
    }
    let inner_type = info.get_data_type(inner).ok()?;
    if !matches!(inner_type, Date32 | Date64) {
        return None;
    }

    // The post-coercion literal is Timestamp(ns) with no tz. Anything not at
    // midnight UTC can never equal date_trunc(g, date_col); bail so the
    // unsimplified predicate runs and returns its (empty) result.
    let ts_ns = match lit_expr.as_literal()? {
        ScalarValue::TimestampNanosecond(Some(v), tz) if tz.is_none() => *v,
        _ => return None,
    };
    let dt = timestamp_ns_to_datetime(ts_ns)?;
    if dt.time() != chrono::NaiveTime::from_hms_opt(0, 0, 0)? {
        return None;
    }
    let lower = dt.date();

    let upper = date_preimage_upper(lower, granularity)?;

    Some(PreimageResult::Range {
        expr: inner.as_ref().clone(),
        interval: Box::new(
            Interval::try_new(
                date_to_scalar(lower, &inner_type)?,
                date_to_scalar(upper, &inner_type)?,
            )
            .ok()?,
        ),
    })
}

/// Upper bound (exclusive) of the date bucket starting at `lower` for the
/// given granularity. Returns `None` if `lower` isn't aligned to the bucket
/// boundary (e.g. month-granularity with day != 1).
///
/// Date columns have day-level precision, so every value casts to midnight
/// UTC. Since `date_trunc(any_g, midnight)` is still midnight, sub-day
/// granularities collapse to a 1-day interval (same as `Day`).
fn date_preimage_upper(
    lower: NaiveDate,
    granularity: DateTruncGranularity,
) -> Option<NaiveDate> {
    match granularity {
        DateTruncGranularity::Microsecond
        | DateTruncGranularity::Millisecond
        | DateTruncGranularity::Second
        | DateTruncGranularity::Minute
        | DateTruncGranularity::Hour
        | DateTruncGranularity::Day => lower.succ_opt(),
        DateTruncGranularity::Week => {
            if lower.weekday() != Weekday::Mon {
                return None;
            }
            lower.checked_add_signed(Duration::days(7))
        }
        DateTruncGranularity::Month => {
            if lower.day() != 1 {
                return None;
            }
            lower.checked_add_months(Months::new(1))
        }
        DateTruncGranularity::Quarter => {
            if lower.day() != 1 || !matches!(lower.month(), 1 | 4 | 7 | 10) {
                return None;
            }
            lower.checked_add_months(Months::new(3))
        }
        DateTruncGranularity::Year => {
            if lower.day() != 1 || lower.month() != 1 {
                return None;
            }
            lower.checked_add_months(Months::new(12))
        }
    }
}

/// Emit constant-step preimage bounds for `date_trunc(g, time_col) = lit` on
/// `Time32`/`Time64` columns. Time has no timezone and no calendar arithmetic,
/// so every supported granularity (Hour and finer) is a fixed-width bucket.
fn time_column_preimage(
    col_expr: &Expr,
    lit_expr: &Expr,
    granularity: DateTruncGranularity,
    info: &SimplifyContext,
) -> Option<PreimageResult> {
    let target_type = info.get_data_type(col_expr).ok()?;
    let unit = match &target_type {
        Time32(unit) | Time64(unit) => *unit,
        _ => return None,
    };
    if !granularity.valid_for_time() {
        return None;
    }

    let value = lit_expr.as_literal().and_then(|lit| time_value(lit, unit))?;
    let step = constant_granularity_step(unit, granularity)?;
    let step = if step == 0 { 1 } else { step };
    // Alignment: literal must already be on a bucket boundary, else the
    // original predicate is unsatisfiable.
    if value % step != 0 {
        return None;
    }
    let upper = value.checked_add(step)?;

    Some(PreimageResult::Range {
        expr: col_expr.clone(),
        interval: Box::new(
            Interval::try_new(time_to_scalar(unit, value), time_to_scalar(unit, upper))
                .ok()?,
        ),
    })
}

/// Extract the underlying i64 representation from a Time scalar of the given
/// unit. Time32 values are sign-extended; valid times fit comfortably.
fn time_value(literal: &ScalarValue, unit: TimeUnit) -> Option<i64> {
    match (unit, literal) {
        (Second, ScalarValue::Time32Second(Some(v))) => Some(*v as i64),
        (Millisecond, ScalarValue::Time32Millisecond(Some(v))) => Some(*v as i64),
        (Microsecond, ScalarValue::Time64Microsecond(Some(v))) => Some(*v),
        (Nanosecond, ScalarValue::Time64Nanosecond(Some(v))) => Some(*v),
        _ => None,
    }
}

/// Wrap an i64 value back into the matching Time scalar. The unit uniquely
/// determines whether the result is `Time32` or `Time64`.
fn time_to_scalar(unit: TimeUnit, value: i64) -> ScalarValue {
    match unit {
        Second => ScalarValue::Time32Second(Some(value as i32)),
        Millisecond => ScalarValue::Time32Millisecond(Some(value as i32)),
        Microsecond => ScalarValue::Time64Microsecond(Some(value)),
        Nanosecond => ScalarValue::Time64Nanosecond(Some(value)),
    }
}

fn timestamp_value(
    literal: &ScalarValue,
    unit: TimeUnit,
    tz_opt: &Option<Arc<str>>,
) -> Option<i64> {
    match (unit, literal) {
        (Second, ScalarValue::TimestampSecond(Some(v), tz)) if tz == tz_opt => Some(*v),
        (Millisecond, ScalarValue::TimestampMillisecond(Some(v), tz)) if tz == tz_opt => {
            Some(*v)
        }
        (Microsecond, ScalarValue::TimestampMicrosecond(Some(v), tz)) if tz == tz_opt => {
            Some(*v)
        }
        (Nanosecond, ScalarValue::TimestampNanosecond(Some(v), tz)) if tz == tz_opt => {
            Some(*v)
        }
        _ => None,
    }
}

/// Wrap a raw `i64` timestamp value back into the matching `ScalarValue` for
/// the given `(unit, tz)`. Mirrors `date_part::date_to_scalar`.
fn timestamp_to_scalar(
    unit: TimeUnit,
    value: i64,
    tz_opt: &Option<Arc<str>>,
) -> ScalarValue {
    match unit {
        Second => ScalarValue::TimestampSecond(Some(value), tz_opt.clone()),
        Millisecond => ScalarValue::TimestampMillisecond(Some(value), tz_opt.clone()),
        Microsecond => ScalarValue::TimestampMicrosecond(Some(value), tz_opt.clone()),
        Nanosecond => ScalarValue::TimestampNanosecond(Some(value), tz_opt.clone()),
    }
}

/// Returns timestamp preimage bounds `[lower, upper)` for
/// `date_trunc(granularity, expr) = lower`. Returns `None` if the literal is
/// not aligned to the granularity (e.g. `date_trunc('month', col) = '2024-05-15'`,
/// which is unsatisfiable) or if the upper bound would overflow.
fn timestamp_preimage_bounds(
    unit: TimeUnit,
    lower: i64,
    tz_opt: &Option<Arc<str>>,
    granularity: DateTruncGranularity,
) -> Option<(i64, i64)> {
    // Verify the literal is already truncated to this granularity. If it
    // isn't, the original predicate is unsatisfiable; returning None defers to
    // the unsimplified plan rather than producing a bogus interval.
    let tz = parse_tz(tz_opt).ok().flatten();
    if general_date_trunc(unit, lower, tz, granularity).ok()? != lower {
        return None;
    }

    let upper = if let Some(step) = constant_granularity_step(unit, granularity) {
        // step == 0 happens when the granularity is finer than the unit's
        // resolution (e.g. truncating a TimestampSecond to microseconds is a
        // no-op); the preimage is then the singleton `[lower, lower + 1 unit)`.
        let step = if step == 0 { 1 } else { step };
        lower.checked_add(step)?
    } else {
        next_calendar_boundary_utc(unit, lower, granularity)?
    };

    Some((lower, upper))
}

/// Width of one bucket for `(unit, granularity)` measured in `unit`s, when
/// that width is constant. Returns `None` for calendar-variable buckets
/// (month/quarter/year), which are handled by `next_calendar_boundary_utc`.
fn constant_granularity_step(
    unit: TimeUnit,
    granularity: DateTruncGranularity,
) -> Option<i64> {
    match (unit, granularity) {
        (Second, DateTruncGranularity::Microsecond) => Some(0),
        (Second, DateTruncGranularity::Millisecond) => Some(0),
        (Second, DateTruncGranularity::Second) => Some(1),
        (Second, DateTruncGranularity::Minute) => Some(60),
        (Second, DateTruncGranularity::Hour) => Some(3_600),
        (Second, DateTruncGranularity::Day) => Some(86_400),
        (Second, DateTruncGranularity::Week) => Some(604_800),

        (Millisecond, DateTruncGranularity::Microsecond) => Some(0),
        (Millisecond, DateTruncGranularity::Millisecond) => Some(1),
        (Millisecond, DateTruncGranularity::Second) => Some(1_000),
        (Millisecond, DateTruncGranularity::Minute) => Some(60_000),
        (Millisecond, DateTruncGranularity::Hour) => Some(3_600_000),
        (Millisecond, DateTruncGranularity::Day) => Some(86_400_000),
        (Millisecond, DateTruncGranularity::Week) => Some(604_800_000),

        (Microsecond, DateTruncGranularity::Microsecond) => Some(1),
        (Microsecond, DateTruncGranularity::Millisecond) => Some(1_000),
        (Microsecond, DateTruncGranularity::Second) => Some(1_000_000),
        (Microsecond, DateTruncGranularity::Minute) => Some(60_000_000),
        (Microsecond, DateTruncGranularity::Hour) => Some(3_600_000_000),
        (Microsecond, DateTruncGranularity::Day) => Some(86_400_000_000),
        (Microsecond, DateTruncGranularity::Week) => Some(604_800_000_000),

        (Nanosecond, DateTruncGranularity::Microsecond) => Some(1_000),
        (Nanosecond, DateTruncGranularity::Millisecond) => Some(1_000_000),
        (Nanosecond, DateTruncGranularity::Second) => Some(1_000_000_000),
        (Nanosecond, DateTruncGranularity::Minute) => Some(60_000_000_000),
        (Nanosecond, DateTruncGranularity::Hour) => Some(3_600_000_000_000),
        (Nanosecond, DateTruncGranularity::Day) => Some(86_400_000_000_000),
        (Nanosecond, DateTruncGranularity::Week) => Some(604_800_000_000_000),
        _ => None,
    }
}

/// Compute the next month/quarter/year boundary in UTC and convert it back
/// to the timestamp's `unit`. Uses `chrono::Months` for calendar arithmetic,
/// matching the chrono-first style used in `date_part::preimage`.
fn next_calendar_boundary_utc(
    unit: TimeUnit,
    lower: i64,
    granularity: DateTruncGranularity,
) -> Option<i64> {
    let scale = timestamp_unit_scale(unit);
    let lower_ns = lower.checked_mul(scale)?;
    let lower_dt = timestamp_ns_to_datetime(lower_ns)?;

    let months = match granularity {
        DateTruncGranularity::Month => 1,
        DateTruncGranularity::Quarter => 3,
        DateTruncGranularity::Year => 12,
        _ => return None,
    };
    let next_dt = lower_dt.checked_add_months(Months::new(months))?;

    let upper_ns = next_dt.and_utc().timestamp_nanos_opt()?;
    if upper_ns % scale != 0 {
        return None;
    }
    Some(upper_ns / scale)
}

fn timestamp_unit_scale(unit: TimeUnit) -> i64 {
    match unit {
        Second => 1_000_000_000,
        Millisecond => 1_000_000,
        Microsecond => 1_000,
        Nanosecond => 1,
    }
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
    use datafusion_common::{DFSchema, DFSchemaRef};
    use datafusion_expr::preimage::PreimageResult;
    use datafusion_expr::simplify::SimplifyContext;
    use datafusion_expr::{ColumnarValue, Expr, ScalarFunctionArgs, ScalarUDFImpl, col};

    /// Build a `SimplifyContext` whose schema declares `x` with the given type.
    /// The preimage method consults `info.get_data_type(col_expr)`, so unit
    /// tests need a real schema (matches the date_part / floor test pattern).
    fn ctx_with_x(data_type: DataType) -> SimplifyContext {
        let schema: DFSchemaRef = Arc::new(
            DFSchema::from_unqualified_fields(
                vec![Field::new("x", data_type, true)].into(),
                Default::default(),
            )
            .unwrap(),
        );
        SimplifyContext::builder().with_schema(schema).build()
    }

    fn assert_preimage_range(
        granularity: &str,
        literal: ScalarValue,
        expected_lower: ScalarValue,
        expected_upper: ScalarValue,
    ) {
        let date_trunc = DateTruncFunc::new();
        let args = vec![
            Expr::Literal(ScalarValue::Utf8(Some(granularity.to_string())), None),
            col("x"),
        ];
        let lit_expr = Expr::Literal(literal.clone(), None);
        // The bounds should match the column's type, so derive `x`'s type from
        // the literal — they're expected to agree by the time preimage runs.
        let info = ctx_with_x(literal.data_type());

        let result = date_trunc.preimage(&args, &lit_expr, &info).unwrap();

        match result {
            PreimageResult::Range { expr, interval } => {
                assert_eq!(expr, col("x"));
                assert_eq!(interval.lower().clone(), expected_lower);
                assert_eq!(interval.upper().clone(), expected_upper);
            }
            PreimageResult::None => {
                panic!("expected range preimage for literal {literal:?}")
            }
        }
    }

    fn assert_preimage_none(granularity: &str, literal: ScalarValue) {
        let date_trunc = DateTruncFunc::new();
        let args = vec![
            Expr::Literal(ScalarValue::Utf8(Some(granularity.to_string())), None),
            col("x"),
        ];
        let lit_expr = Expr::Literal(literal.clone(), None);
        let info = ctx_with_x(literal.data_type());

        let result = date_trunc.preimage(&args, &lit_expr, &info).unwrap();

        assert!(
            matches!(result, PreimageResult::None),
            "expected no preimage for literal {literal:?}"
        );
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
            let granularity_enum = DateTruncGranularity::from_str(granularity).unwrap();
            let result = date_trunc_coarse(granularity_enum, left, None).unwrap();
            assert_eq!(result, right, "{original} = {expected}");
        });
    }

    #[test]
    fn test_date_trunc_preimage_timestamp_valid_cases() {
        let day_start = string_to_timestamp_nanos("2024-05-01T00:00:00Z").unwrap();
        let next_day = string_to_timestamp_nanos("2024-05-02T00:00:00Z").unwrap();
        assert_preimage_range(
            "day",
            ScalarValue::TimestampNanosecond(Some(day_start), None),
            ScalarValue::TimestampNanosecond(Some(day_start), None),
            ScalarValue::TimestampNanosecond(Some(next_day), None),
        );

        let month_start = string_to_timestamp_nanos("2024-05-01T00:00:00Z").unwrap();
        let next_month = string_to_timestamp_nanos("2024-06-01T00:00:00Z").unwrap();
        assert_preimage_range(
            "month",
            ScalarValue::TimestampNanosecond(Some(month_start), None),
            ScalarValue::TimestampNanosecond(Some(month_start), None),
            ScalarValue::TimestampNanosecond(Some(next_month), None),
        );

        // Quarter: April 1 → July 1
        let q2_start = string_to_timestamp_nanos("2024-04-01T00:00:00Z").unwrap();
        let q3_start = string_to_timestamp_nanos("2024-07-01T00:00:00Z").unwrap();
        assert_preimage_range(
            "quarter",
            ScalarValue::TimestampNanosecond(Some(q2_start), None),
            ScalarValue::TimestampNanosecond(Some(q2_start), None),
            ScalarValue::TimestampNanosecond(Some(q3_start), None),
        );

        // Year: 2024-01-01 → 2025-01-01 (covers Feb 29 in the leap-year span)
        let year_start = string_to_timestamp_nanos("2024-01-01T00:00:00Z").unwrap();
        let next_year = string_to_timestamp_nanos("2025-01-01T00:00:00Z").unwrap();
        assert_preimage_range(
            "year",
            ScalarValue::TimestampNanosecond(Some(year_start), None),
            ScalarValue::TimestampNanosecond(Some(year_start), None),
            ScalarValue::TimestampNanosecond(Some(next_year), None),
        );

        // Week: date_trunc floors to Monday
        let monday = string_to_timestamp_nanos("2024-04-29T00:00:00Z").unwrap();
        let next_monday = string_to_timestamp_nanos("2024-05-06T00:00:00Z").unwrap();
        assert_preimage_range(
            "week",
            ScalarValue::TimestampNanosecond(Some(monday), None),
            ScalarValue::TimestampNanosecond(Some(monday), None),
            ScalarValue::TimestampNanosecond(Some(next_monday), None),
        );

        // Granularity finer than the unit's resolution still produces a
        // singleton bucket of one underlying tick.
        assert_preimage_range(
            "microsecond",
            ScalarValue::TimestampSecond(Some(1_700_000_000), None),
            ScalarValue::TimestampSecond(Some(1_700_000_000), None),
            ScalarValue::TimestampSecond(Some(1_700_000_001), None),
        );
    }

    #[test]
    fn test_date_trunc_preimage_timestamp_timezone_fine_granularity() {
        let lower = string_to_timestamp_nanos("2024-10-27T02:00:00+02:00").unwrap();
        let upper = string_to_timestamp_nanos("2024-10-27T02:01:00+02:00").unwrap();
        let tz = Some(Arc::<str>::from("+02"));

        assert_preimage_range(
            "minute",
            ScalarValue::TimestampNanosecond(Some(lower), tz.clone()),
            ScalarValue::TimestampNanosecond(Some(lower), tz.clone()),
            ScalarValue::TimestampNanosecond(Some(upper), tz),
        );
    }

    #[test]
    fn test_date_trunc_preimage_none_cases() {
        // Literal not aligned to the granularity → unsatisfiable, skip rewrite.
        let non_truncated = string_to_timestamp_nanos("2024-05-01T10:30:15Z").unwrap();
        assert_preimage_none(
            "minute",
            ScalarValue::TimestampNanosecond(Some(non_truncated), None),
        );

        // Coarse granularity with a named timezone: bucket width is variable
        // around DST, so we conservatively skip the rewrite.
        let day_start_tz =
            string_to_timestamp_nanos("2024-10-27T00:00:00+02:00").unwrap();
        assert_preimage_none(
            "day",
            ScalarValue::TimestampNanosecond(
                Some(day_start_tz),
                Some(Arc::<str>::from("Europe/Berlin")),
            ),
        );

        // Unsupported literal type (Int32) → no preimage.
        assert_preimage_none("day", ScalarValue::Int32(Some(0)));
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
            let args = ScalarFunctionArgs {
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
            let args = ScalarFunctionArgs {
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
                let args = ScalarFunctionArgs {
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
