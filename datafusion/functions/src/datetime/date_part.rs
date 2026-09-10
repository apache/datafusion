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

use std::iter::repeat_n;
use std::str::FromStr;
use std::sync::Arc;

use arrow::array::temporal_conversions::as_datetime_with_timezone;
use arrow::array::timezone::Tz;
use arrow::array::{Array, ArrayRef, Float64Array, Int32Array, Int64Array};
use arrow::compute::{DatePart, binary, date_part};
use arrow::datatypes::DataType::{
    Date32, Date64, Duration, Interval, Time32, Time64, Timestamp,
};
use arrow::datatypes::TimeUnit::{Microsecond, Millisecond, Nanosecond, Second};
use arrow::datatypes::{
    ArrowTimestampType, DataType, Date32Type, Date64Type, Field, FieldRef,
    IntervalUnit as ArrowIntervalUnit, TimeUnit, TimestampMicrosecondType,
    TimestampMillisecondType, TimestampNanosecondType, TimestampSecondType,
};
use chrono::{Datelike, NaiveDate, Offset};
use datafusion_common::types::{NativeType, logical_date};

use crate::datetime::common::parse_tz;
use datafusion_common::{
    Result, ScalarValue,
    cast::{
        as_date32_array, as_date64_array, as_int32_array, as_interval_dt_array,
        as_interval_mdn_array, as_interval_ym_array, as_primitive_array,
        as_time32_millisecond_array, as_time32_second_array, as_time64_microsecond_array,
        as_time64_nanosecond_array, as_timestamp_microsecond_array,
        as_timestamp_millisecond_array, as_timestamp_nanosecond_array,
        as_timestamp_second_array,
    },
    exec_err, internal_err, not_impl_err,
    types::logical_string,
    utils::take_function_args,
};
use datafusion_expr::preimage::PreimageResult;
use datafusion_expr::simplify::SimplifyContext;
use datafusion_expr::{
    ColumnarValue, Documentation, Expr, ReturnFieldArgs, ScalarFunctionArgs,
    ScalarUDFImpl, Signature, TypeSignature, Volatility, interval_arithmetic,
};
use datafusion_expr_common::signature::{Coercion, TypeSignatureClass};
use datafusion_macros::user_doc;

#[user_doc(
    doc_section(label = "Time and Date Functions"),
    description = "Returns the specified part of the date as an integer.",
    syntax_example = "date_part(part, expression)",
    alternative_syntax = "extract(field FROM source)",
    argument(
        name = "part",
        description = r#"Part of the date to return. The following date parts are supported:

    - year
    - isoyear (ISO 8601 week-numbering year)
    - quarter (emits value in inclusive range [1, 4] based on which quartile of the year the date is in)
    - month
    - week (week of the year)
    - day (day of the month)
    - hour
    - minute
    - second
    - millisecond
    - microsecond
    - nanosecond
    - dow (day of the week where Sunday is 0)
    - doy (day of the year)
    - epoch (seconds since Unix epoch for timestamps/dates, total seconds for intervals)
    - isodow (ISO 8601 day of the week where Monday is 1 and Sunday is 7)
    - timezone (UTC offset in seconds)
    - timezone_hour (whole hours of the UTC offset)
    - timezone_minute (whole minutes of the UTC offset, excluding the hours)

    The `timezone`, `timezone_hour` and `timezone_minute` parts are only defined for timestamps that carry a timezone; extracting them from a timezone-naive timestamp, a date, a time or an interval is an error. They report the offset that applies at that instant, so they follow daylight saving time: `Europe/Brussels` yields `3600` in January and `7200` in July. For a negative offset both parts carry the sign, so `America/St_Johns` in January yields `-3` hours and `-30` minutes.
"#
    ),
    argument(
        name = "expression",
        description = "Time expression to operate on. Can be a constant, column, or function."
    ),
    sql_example = r#"```sql
> SELECT date_part('year', '2024-05-01T00:00:00');
+-----------------------------------------------------+
| date_part(Utf8("year"),Utf8("2024-05-01T00:00:00")) |
+-----------------------------------------------------+
| 2024                                                |
+-----------------------------------------------------+
> SELECT extract(day FROM timestamp '2024-05-01T00:00:00');
+----------------------------------------------------+
| date_part(Utf8("DAY"),Utf8("2024-05-01T00:00:00")) |
+----------------------------------------------------+
| 1                                                  |
+----------------------------------------------------+
> SELECT date_part('timezone', TIMESTAMP '2024-07-01T12:00:00' AT TIME ZONE 'Europe/Brussels') AS utc_offset_seconds;
+--------------------+
| utc_offset_seconds |
+--------------------+
| 7200               |
+--------------------+
```"#
)]
#[derive(Debug, PartialEq, Eq, Hash)]
pub struct DatePartFunc {
    signature: Signature,
    aliases: Vec<String>,
}

impl Default for DatePartFunc {
    fn default() -> Self {
        Self::new()
    }
}

impl DatePartFunc {
    pub fn new() -> Self {
        Self {
            signature: Signature::one_of(
                vec![
                    TypeSignature::Coercible(vec![
                        Coercion::new_exact(TypeSignatureClass::Native(logical_string())),
                        Coercion::new_implicit(
                            TypeSignatureClass::Timestamp,
                            // Not consistent with Postgres and DuckDB but to avoid regression we implicit cast string to timestamp
                            vec![TypeSignatureClass::Native(logical_string())],
                            NativeType::Timestamp(Nanosecond, None),
                        ),
                    ]),
                    TypeSignature::Coercible(vec![
                        Coercion::new_exact(TypeSignatureClass::Native(logical_string())),
                        Coercion::new_exact(TypeSignatureClass::Native(logical_date())),
                    ]),
                    TypeSignature::Coercible(vec![
                        Coercion::new_exact(TypeSignatureClass::Native(logical_string())),
                        Coercion::new_exact(TypeSignatureClass::Time),
                    ]),
                    TypeSignature::Coercible(vec![
                        Coercion::new_exact(TypeSignatureClass::Native(logical_string())),
                        Coercion::new_exact(TypeSignatureClass::Interval),
                    ]),
                    TypeSignature::Coercible(vec![
                        Coercion::new_exact(TypeSignatureClass::Native(logical_string())),
                        Coercion::new_exact(TypeSignatureClass::Duration),
                    ]),
                ],
                Volatility::Immutable,
            ),
            aliases: vec![String::from("datepart")],
        }
    }
}

impl ScalarUDFImpl for DatePartFunc {
    fn name(&self) -> &str {
        "date_part"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        internal_err!("return_field_from_args should be called instead")
    }

    fn return_field_from_args(&self, args: ReturnFieldArgs) -> Result<FieldRef> {
        let [field, _] = take_function_args(self.name(), args.scalar_arguments)?;
        let nullable = args.arg_fields[1].is_nullable();

        field
            .and_then(|sv| {
                sv.try_as_str()
                    .flatten()
                    .filter(|s| !s.is_empty())
                    .map(|part| {
                        if is_epoch(part) {
                            Field::new(self.name(), DataType::Float64, nullable)
                        } else if is_nanosecond(part) {
                            // See notes on [seconds_ns] for rationale
                            Field::new(self.name(), DataType::Int64, nullable)
                        } else {
                            Field::new(self.name(), DataType::Int32, nullable)
                        }
                    })
            })
            .map(Arc::new)
            .map_or_else(
                || exec_err!("{} requires non-empty constant string", self.name()),
                Ok,
            )
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let args = args.args;
        let [part, array] = take_function_args(self.name(), args)?;

        let part = if let ColumnarValue::Scalar(ScalarValue::Utf8(Some(v))) = part {
            v
        } else if let ColumnarValue::Scalar(ScalarValue::Utf8View(Some(v))) = part {
            v
        } else {
            return exec_err!(
                "First argument of `DATE_PART` must be non-null scalar Utf8"
            );
        };

        let is_scalar = matches!(array, ColumnarValue::Scalar(_));

        let array = match array {
            ColumnarValue::Array(array) => Arc::clone(&array),
            ColumnarValue::Scalar(scalar) => scalar.to_array()?,
        };

        let part_trim = part_normalization(&part);

        let arr = match DatePart::from_str(part_trim) {
            Ok(DatePart::Second) => seconds_as_i32(array.as_ref(), Second)?,
            Ok(DatePart::Millisecond) => seconds_as_i32(array.as_ref(), Millisecond)?,
            Ok(DatePart::Microsecond) => seconds_as_i32(array.as_ref(), Microsecond)?,
            Ok(DatePart::Nanosecond) => seconds_ns(array.as_ref())?,
            Ok(part) => date_part(array.as_ref(), part)?,
            Err(_) if is_epoch(part_trim) => epoch(array.as_ref())?,
            // `timezone`, `timezone_hour` and `timezone_minute` have no
            // `DatePart` equivalent, so they are resolved once `DatePart::from_str`
            // has failed. `TimezonePart::parse` matches lowercase spellings only.
            Err(_) => match TimezonePart::parse(&part_trim.to_lowercase()) {
                Some(tz_part) => timezone_part(array.as_ref(), tz_part)?,
                None => return exec_err!("Date part '{part}' not supported"),
            },
        };

        Ok(if is_scalar {
            ColumnarValue::Scalar(ScalarValue::try_from_array(arr.as_ref(), 0)?)
        } else {
            ColumnarValue::Array(arr)
        })
    }

    // Only casting the year is supported since pruning other date parts is not possible
    // date_part(col, YEAR) = 2024 => col >= '2024-01-01' and col < '2025-01-01'
    // But for anything less than YEAR simplifying is not possible without specifying the bigger interval
    // date_part(col, MONTH) = 1 => col = '2023-01-01' or col = '2024-01-01' or ... or col = '3000-01-01'
    fn preimage(
        &self,
        args: &[Expr],
        lit_expr: &Expr,
        info: &SimplifyContext,
    ) -> Result<PreimageResult> {
        let [part, col_expr] = take_function_args(self.name(), args)?;

        // Get the date part from the part argument
        let date_part = part
            .as_literal()
            .and_then(|sv| sv.try_as_str().flatten())
            .map(part_normalization)
            .and_then(|s| DatePart::from_str(s).ok());

        // only support extracting year
        match date_part {
            Some(DatePart::Year) => (),
            _ => return Ok(PreimageResult::None),
        }

        // Check if the argument is a literal (e.g. date_part(YEAR, col) = 2024)
        let Some(argument_literal) = lit_expr.as_literal() else {
            return Ok(PreimageResult::None);
        };

        // Extract i32 year from Scalar value
        let year = match argument_literal {
            ScalarValue::Int32(Some(y)) => *y,
            _ => return Ok(PreimageResult::None),
        };

        // Can only extract year from Date32/64 and Timestamp column
        let target_type = match info.get_data_type(col_expr)? {
            Date32 | Date64 | Timestamp(_, _) => &info.get_data_type(col_expr)?,
            _ => return Ok(PreimageResult::None),
        };

        // Compute the Interval bounds
        let Some(start_time) = NaiveDate::from_ymd_opt(year, 1, 1) else {
            return Ok(PreimageResult::None);
        };
        let Some(end_time) = start_time.with_year(year + 1) else {
            return Ok(PreimageResult::None);
        };

        // Convert to ScalarValues
        let (Some(lower), Some(upper)) = (
            date_to_scalar(start_time, target_type),
            date_to_scalar(end_time, target_type),
        ) else {
            return Ok(PreimageResult::None);
        };
        let interval = Box::new(interval_arithmetic::Interval::try_new(lower, upper)?);

        Ok(PreimageResult::Range {
            expr: col_expr.clone(),
            interval,
        })
    }

    fn aliases(&self) -> &[String] {
        &self.aliases
    }

    fn documentation(&self) -> Option<&Documentation> {
        self.doc()
    }
}

fn is_epoch(part: &str) -> bool {
    let part = part_normalization(part);
    matches!(part.to_lowercase().as_str(), "epoch")
}

fn is_nanosecond(part: &str) -> bool {
    DatePart::from_str(part_normalization(part))
        .map(|p| matches!(p, DatePart::Nanosecond))
        .unwrap_or(false)
}

/// The `timezone`, `timezone_hour` and `timezone_minute` fields, which report
/// the UTC offset that applies to a timezone-aware timestamp *at that instant*.
///
/// Because the offset of a named timezone changes with daylight saving time,
/// these are per-row values, not a property of the type alone.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum TimezonePart {
    /// `timezone`: the whole UTC offset, in seconds
    Offset,
    /// `timezone_hour`: whole hours of the UTC offset
    Hour,
    /// `timezone_minute`: whole minutes of the UTC offset, excluding the hours.
    /// The sign follows the sign of the offset (PostgreSQL behaviour), so
    /// `-03:30` yields `-3` hours and `-30` minutes.
    Minute,
}

impl TimezonePart {
    fn parse(part: &str) -> Option<Self> {
        match part {
            "timezone" => Some(Self::Offset),
            "timezone_hour" => Some(Self::Hour),
            "timezone_minute" => Some(Self::Minute),
            _ => None,
        }
    }

    fn name(&self) -> &'static str {
        match self {
            Self::Offset => "timezone",
            Self::Hour => "timezone_hour",
            Self::Minute => "timezone_minute",
        }
    }

    /// Extract this field from a UTC offset expressed in seconds.
    ///
    /// Rust integer division truncates towards zero, which is what makes the
    /// sign of `timezone_minute` follow the sign of the offset, matching
    /// PostgreSQL.
    fn project(&self, offset_seconds: i32) -> i32 {
        match self {
            Self::Offset => offset_seconds,
            Self::Hour => offset_seconds / 3_600,
            Self::Minute => (offset_seconds % 3_600) / 60,
        }
    }
}

/// Compute a [`TimezonePart`] for every element of a timezone-aware timestamp
/// array.
///
/// Errors for any other input type, including timezone-naive timestamps, which
/// carry no offset at all. This matches PostgreSQL, which rejects
/// `date_part('timezone', <timestamp without time zone>)`.
fn timezone_part(array: &dyn Array, part: TimezonePart) -> Result<ArrayRef> {
    let Timestamp(unit, tz_opt) = array.data_type() else {
        return exec_err!(
            "Date part '{}' is only supported for timestamps with a timezone, got {}",
            part.name(),
            array.data_type()
        );
    };
    let Some(tz) = parse_tz(tz_opt.as_ref())? else {
        return exec_err!(
            "Date part '{}' is not supported for timezone-naive timestamps, got {}",
            part.name(),
            array.data_type()
        );
    };

    match unit {
        Second => timezone_part_typed::<TimestampSecondType>(array, tz, part),
        Millisecond => timezone_part_typed::<TimestampMillisecondType>(array, tz, part),
        Microsecond => timezone_part_typed::<TimestampMicrosecondType>(array, tz, part),
        Nanosecond => timezone_part_typed::<TimestampNanosecondType>(array, tz, part),
    }
}

fn timezone_part_typed<T: ArrowTimestampType>(
    array: &dyn Array,
    tz: Tz,
    part: TimezonePart,
) -> Result<ArrayRef> {
    let array = as_primitive_array::<T>(array)?;
    // `unary_opt` keeps the input nulls and additionally nulls out any value
    // that cannot be represented as a `DateTime` (out of chrono's range).
    let result: Int32Array = array.unary_opt(|value| {
        as_datetime_with_timezone::<T>(value, tz)
            .map(|dt| part.project(dt.offset().fix().local_minus_utc()))
    });
    Ok(Arc::new(result))
}

fn date_to_scalar(date: NaiveDate, target_type: &DataType) -> Option<ScalarValue> {
    Some(match target_type {
        Date32 => ScalarValue::Date32(Some(Date32Type::from_naive_date(date))),
        Date64 => ScalarValue::Date64(Some(Date64Type::from_naive_date(date))),

        Timestamp(unit, tz_opt) => {
            let naive_midnight = date.and_hms_opt(0, 0, 0)?;
            let tz: Option<Tz> = tz_opt.clone().and_then(|s| s.parse().ok());

            match unit {
                Second => ScalarValue::TimestampSecond(
                    TimestampSecondType::from_naive_datetime(naive_midnight, tz.as_ref()),
                    tz_opt.clone(),
                ),
                Millisecond => ScalarValue::TimestampMillisecond(
                    TimestampMillisecondType::from_naive_datetime(
                        naive_midnight,
                        tz.as_ref(),
                    ),
                    tz_opt.clone(),
                ),
                Microsecond => ScalarValue::TimestampMicrosecond(
                    TimestampMicrosecondType::from_naive_datetime(
                        naive_midnight,
                        tz.as_ref(),
                    ),
                    tz_opt.clone(),
                ),
                Nanosecond => ScalarValue::TimestampNanosecond(
                    TimestampNanosecondType::from_naive_datetime(
                        naive_midnight,
                        tz.as_ref(),
                    ),
                    tz_opt.clone(),
                ),
            }
        }
        _ => return None,
    })
}

// Try to remove quote if exist, if the quote is invalid, return original string and let the downstream function handle the error
fn part_normalization(part: &str) -> &str {
    part.strip_prefix(|c| c == '\'' || c == '\"')
        .and_then(|s| s.strip_suffix(|c| c == '\'' || c == '\"'))
        .unwrap_or(part)
}

/// Invoke [`date_part`] on an `array` (e.g. Timestamp) and convert the
/// result to a total number of seconds, milliseconds, microseconds or
/// nanoseconds as an `Int32Array`
fn seconds_as_i32(array: &dyn Array, unit: TimeUnit) -> Result<ArrayRef> {
    // Nanosecond is neither supported in Postgres nor DuckDB, to avoid dealing
    // with overflow and precision issue we don't support nanosecond
    if unit == Nanosecond {
        return not_impl_err!("Date part {unit:?} not supported");
    }

    // Fast path with seconds - no need to compute nanoseconds
    if unit == Second {
        return Ok(date_part(array, DatePart::Second)?);
    }

    // Fast path for Date32 and Date64 - no seconds
    if array.data_type() == &Date32 || array.data_type() == &Date64 {
        return Ok(Arc::new(Int32Array::from_iter_values_with_nulls(
            repeat_n(0, array.len()),
            array.nulls().cloned(),
        )));
    }

    let conversion_factor = match unit {
        Second => 1_000_000_000,
        Millisecond => 1_000_000,
        Microsecond => 1_000,
        Nanosecond => 1,
    };

    let second_factor = match unit {
        Second => 1,
        Millisecond => 1_000,
        Microsecond => 1_000_000,
        Nanosecond => 1_000_000_000,
    };

    let secs = date_part(array, DatePart::Second)?;
    // This assumes array is primitive and not a dictionary
    let secs = as_int32_array(secs.as_ref())?;
    let subsecs = date_part(array, DatePart::Nanosecond)?;
    let subsecs = as_int32_array(subsecs.as_ref())?;

    // Special case where there are no nulls.
    if subsecs.null_count() == 0 {
        let r: Int32Array = binary(secs, subsecs, |secs, subsecs| {
            secs * second_factor + (subsecs % 1_000_000_000) / conversion_factor
        })?;
        Ok(Arc::new(r))
    } else {
        // Nulls in secs are preserved, nulls in subsecs are treated as zero to account for the case
        // where the number of nanoseconds overflows.
        let r: Int32Array = secs
            .iter()
            .zip(subsecs)
            .map(|(secs, subsecs)| {
                secs.map(|secs| {
                    let subsecs = subsecs.unwrap_or(0);
                    secs * second_factor + (subsecs % 1_000_000_000) / conversion_factor
                })
            })
            .collect();
        Ok(Arc::new(r))
    }
}

/// Invoke [`date_part`] on an `array` (e.g. Timestamp) and convert the
/// result to a total number of seconds, milliseconds, microseconds or
/// nanoseconds
///
/// Given epoch return f64, this is a duplicated function to optimize for f64 type
fn seconds(array: &dyn Array, unit: TimeUnit) -> Result<ArrayRef> {
    let sf = match unit {
        Second => 1_f64,
        Millisecond => 1_000_f64,
        Microsecond => 1_000_000_f64,
        Nanosecond => 1_000_000_000_f64,
    };
    let secs = date_part(array, DatePart::Second)?;
    // This assumes array is primitive and not a dictionary
    let secs = as_int32_array(secs.as_ref())?;
    let subsecs = date_part(array, DatePart::Nanosecond)?;
    let subsecs = as_int32_array(subsecs.as_ref())?;

    // Special case where there are no nulls.
    if subsecs.null_count() == 0 {
        let r: Float64Array = binary(secs, subsecs, |secs, subsecs| {
            (secs as f64 + ((subsecs % 1_000_000_000) as f64 / 1_000_000_000_f64)) * sf
        })?;
        Ok(Arc::new(r))
    } else {
        // Nulls in secs are preserved, nulls in subsecs are treated as zero to account for the case
        // where the number of nanoseconds overflows.
        let r: Float64Array = secs
            .iter()
            .zip(subsecs)
            .map(|(secs, subsecs)| {
                secs.map(|secs| {
                    let subsecs = subsecs.unwrap_or(0);
                    (secs as f64 + ((subsecs % 1_000_000_000) as f64 / 1_000_000_000_f64))
                        * sf
                })
            })
            .collect();
        Ok(Arc::new(r))
    }
}

fn epoch(array: &dyn Array) -> Result<ArrayRef> {
    const SECONDS_IN_A_DAY: f64 = 86400_f64;
    // Note: Month-to-second conversion uses 30 days as an approximation.
    // This matches PostgreSQL's behavior for interval epoch extraction,
    // but does not represent exact calendar months (which vary 28-31 days).
    // See: https://doxygen.postgresql.org/datatype_2timestamp_8h.html
    const DAYS_PER_MONTH: f64 = 30_f64;

    let f: Float64Array = match array.data_type() {
        Timestamp(Second, _) => as_timestamp_second_array(array)?.unary(|x| x as f64),
        Timestamp(Millisecond, _) => {
            as_timestamp_millisecond_array(array)?.unary(|x| x as f64 / 1_000_f64)
        }
        Timestamp(Microsecond, _) => {
            as_timestamp_microsecond_array(array)?.unary(|x| x as f64 / 1_000_000_f64)
        }
        Timestamp(Nanosecond, _) => {
            as_timestamp_nanosecond_array(array)?.unary(|x| x as f64 / 1_000_000_000_f64)
        }
        Date32 => as_date32_array(array)?.unary(|x| x as f64 * SECONDS_IN_A_DAY),
        Date64 => as_date64_array(array)?.unary(|x| x as f64 / 1_000_f64),
        Time32(Second) => as_time32_second_array(array)?.unary(|x| x as f64),
        Time32(Millisecond) => {
            as_time32_millisecond_array(array)?.unary(|x| x as f64 / 1_000_f64)
        }
        Time64(Microsecond) => {
            as_time64_microsecond_array(array)?.unary(|x| x as f64 / 1_000_000_f64)
        }
        Time64(Nanosecond) => {
            as_time64_nanosecond_array(array)?.unary(|x| x as f64 / 1_000_000_000_f64)
        }
        Interval(ArrowIntervalUnit::YearMonth) => as_interval_ym_array(array)?
            .unary(|x| x as f64 * DAYS_PER_MONTH * SECONDS_IN_A_DAY),
        Interval(ArrowIntervalUnit::DayTime) => as_interval_dt_array(array)?.unary(|x| {
            x.days as f64 * SECONDS_IN_A_DAY + x.milliseconds as f64 / 1_000_f64
        }),
        Interval(ArrowIntervalUnit::MonthDayNano) => {
            as_interval_mdn_array(array)?.unary(|x| {
                x.months as f64 * DAYS_PER_MONTH * SECONDS_IN_A_DAY
                    + x.days as f64 * SECONDS_IN_A_DAY
                    + x.nanoseconds as f64 / 1_000_000_000_f64
            })
        }
        Duration(_) => return seconds(array, Second),
        d => return exec_err!("Cannot convert {d:?} to epoch"),
    };
    Ok(Arc::new(f))
}

/// Invoke [`date_part`] on an `array` (e.g. Timestamp) and convert the
/// result to a total number of nanoseconds as an Int64 array.
///
/// This returns an Int64 rather than Int32 because  there 1 billion
/// `nanosecond`s in each second, so representing up to 60 seconds as
/// nanoseconds can be values up to 60 billion, which does not fit in Int32.
fn seconds_ns(array: &dyn Array) -> Result<ArrayRef> {
    // Fast path for Date32 and Date64 - no nanoseconds
    if array.data_type() == &Date32 || array.data_type() == &Date64 {
        return Ok(Arc::new(Int64Array::from_iter_values_with_nulls(
            repeat_n(0, array.len()),
            array.nulls().cloned(),
        )));
    }

    let secs = date_part(array, DatePart::Second)?;
    // This assumes array is primitive and not a dictionary
    let secs = as_int32_array(secs.as_ref())?;
    let subsecs = date_part(array, DatePart::Nanosecond)?;
    let subsecs = as_int32_array(subsecs.as_ref())?;

    // Special case where there are no nulls.
    if subsecs.null_count() == 0 {
        let r: Int64Array = binary(secs, subsecs, |secs, subsecs| {
            (secs as i64) * 1_000_000_000 + (subsecs as i64)
        })?;
        Ok(Arc::new(r))
    } else {
        // Nulls in secs are preserved, nulls in subsecs are treated as zero to account for the case
        // where the number of nanoseconds overflows.
        let r: Int64Array = secs
            .iter()
            .zip(subsecs)
            .map(|(secs, subsecs)| {
                secs.map(|secs| {
                    let subsecs = subsecs.unwrap_or(0);
                    (secs as i64) * 1_000_000_000 + (subsecs as i64)
                })
            })
            .collect();
        Ok(Arc::new(r))
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use super::DatePartFunc;
    use arrow::array::{
        Array, Int32Array, TimestampMicrosecondArray, TimestampMillisecondArray,
        TimestampNanosecondArray, TimestampSecondArray,
    };
    use arrow::compute::kernels::cast_utils::string_to_timestamp_nanos;
    use arrow::datatypes::{DataType, Field, FieldRef, TimeUnit};
    use datafusion_common::ScalarValue;
    use datafusion_common::cast::as_int32_array;
    use datafusion_common::config::ConfigOptions;
    use datafusion_expr::{
        ColumnarValue, ReturnFieldArgs, ScalarFunctionArgs, ScalarUDFImpl,
    };

    /// Invoke `date_part(part, value)` with a single (already typed) argument.
    fn invoke_date_part(
        part: &str,
        value: ColumnarValue,
        number_rows: usize,
    ) -> datafusion_common::Result<ColumnarValue> {
        let value_field: FieldRef = Field::new("b", value.data_type(), true).into();
        let args = ScalarFunctionArgs {
            args: vec![ColumnarValue::Scalar(ScalarValue::from(part)), value],
            arg_fields: vec![Field::new("a", DataType::Utf8, false).into(), value_field],
            number_rows,
            return_field: Field::new("f", DataType::Int32, true).into(),
            config_options: Arc::new(ConfigOptions::default()),
        };
        DatePartFunc::new().invoke_with_args(args)
    }

    fn ts_nanos(s: &str) -> i64 {
        string_to_timestamp_nanos(s).unwrap()
    }

    /// `date_part(part, <ts with tz>)` over a single-element array.
    fn tz_part(part: &str, tz: &str, utc_instant: &str) -> Option<i32> {
        let array = TimestampNanosecondArray::from(vec![Some(ts_nanos(utc_instant))])
            .with_timezone(tz);
        let result = invoke_date_part(part, ColumnarValue::Array(Arc::new(array)), 1)
            .unwrap()
            .to_array(1)
            .unwrap();
        assert_eq!(result.data_type(), &DataType::Int32);
        let result = as_int32_array(&result).unwrap();
        result.is_valid(0).then(|| result.value(0))
    }

    /// Expected values were cross-checked against PostgreSQL 17:
    /// `SET TimeZone TO '<tz>'; SELECT date_part('timezone', '<instant>'::timestamptz);`
    #[test]
    fn timezone_parts_match_postgres() {
        // (timezone, UTC instant, offset seconds, offset hours, offset minutes)
        let cases = [
            // Fixed offsets carried directly by the Arrow type
            ("+05:30", "2024-07-01T12:00:00Z", 19_800, 5, 30),
            ("-03:30", "2024-07-01T12:00:00Z", -12_600, -3, -30),
            ("+00:00", "2024-07-01T12:00:00Z", 0, 0, 0),
            ("UTC", "2024-07-01T12:00:00Z", 0, 0, 0),
            // Named timezone, standard time vs daylight saving time
            ("Europe/Brussels", "2024-01-01T12:00:00Z", 3_600, 1, 0),
            ("Europe/Brussels", "2024-07-01T12:00:00Z", 7_200, 2, 0),
            ("America/Denver", "2024-01-01T12:00:00Z", -25_200, -7, 0),
            ("America/Denver", "2024-07-01T12:00:00Z", -21_600, -6, 0),
            // 45 minute offsets (southern hemisphere: January is DST)
            ("Pacific/Chatham", "2024-01-01T12:00:00Z", 49_500, 13, 45),
            ("Pacific/Chatham", "2024-07-01T12:00:00Z", 45_900, 12, 45),
            // Negative offsets keep the sign on both hour and minute
            ("America/St_Johns", "2024-01-01T12:00:00Z", -12_600, -3, -30),
            ("America/St_Johns", "2024-07-01T12:00:00Z", -9_000, -2, -30),
            (
                "Pacific/Marquesas",
                "2024-07-01T12:00:00Z",
                -34_200,
                -9,
                -30,
            ),
            ("Asia/Kolkata", "2024-07-01T12:00:00Z", 19_800, 5, 30),
            ("Asia/Kathmandu", "2024-07-01T12:00:00Z", 20_700, 5, 45),
        ];

        for (tz, instant, secs, hours, minutes) in cases {
            assert_eq!(
                tz_part("timezone", tz, instant),
                Some(secs),
                "timezone for {tz} at {instant}"
            );
            assert_eq!(
                tz_part("timezone_hour", tz, instant),
                Some(hours),
                "timezone_hour for {tz} at {instant}"
            );
            assert_eq!(
                tz_part("timezone_minute", tz, instant),
                Some(minutes),
                "timezone_minute for {tz} at {instant}"
            );
        }
    }

    #[test]
    fn timezone_parts_are_case_insensitive() {
        assert_eq!(
            tz_part("TIMEZONE", "Europe/Brussels", "2024-07-01T12:00:00Z"),
            Some(7_200)
        );
        // The spelling produced by `EXTRACT(TIMEZONE_HOUR FROM ..)`
        assert_eq!(
            tz_part("TIMEZONE_HOUR", "Asia/Kolkata", "2024-07-01T12:00:00Z"),
            Some(5)
        );
        assert_eq!(
            tz_part("Timezone_Minute", "Asia/Kolkata", "2024-07-01T12:00:00Z"),
            Some(30)
        );
    }

    #[test]
    fn timezone_parts_over_array_with_dst_transition_and_nulls() {
        // Two instants either side of the Brussels DST switch, plus a null
        let array = TimestampNanosecondArray::from(vec![
            Some(ts_nanos("2024-01-15T00:00:00Z")),
            Some(ts_nanos("2024-07-15T00:00:00Z")),
            None,
            Some(ts_nanos("2024-11-15T00:00:00Z")),
        ])
        .with_timezone("Europe/Brussels");

        let result = invoke_date_part(
            "timezone",
            ColumnarValue::Array(Arc::new(array.clone())),
            4,
        )
        .unwrap()
        .to_array(4)
        .unwrap();
        assert_eq!(
            as_int32_array(&result).unwrap(),
            &Int32Array::from(vec![Some(3_600), Some(7_200), None, Some(3_600)])
        );

        let result =
            invoke_date_part("timezone_hour", ColumnarValue::Array(Arc::new(array)), 4)
                .unwrap()
                .to_array(4)
                .unwrap();
        assert_eq!(
            as_int32_array(&result).unwrap(),
            &Int32Array::from(vec![Some(1), Some(2), None, Some(1)])
        );
    }

    #[test]
    fn timezone_parts_for_all_time_units() {
        // 2024-07-01T12:00:00Z in Kolkata (+05:30) regardless of precision
        let second =
            TimestampSecondArray::from(vec![1_719_835_200]).with_timezone("Asia/Kolkata");
        let milli = TimestampMillisecondArray::from(vec![1_719_835_200_000])
            .with_timezone("Asia/Kolkata");
        let micro = TimestampMicrosecondArray::from(vec![1_719_835_200_000_000])
            .with_timezone("Asia/Kolkata");
        let nano = TimestampNanosecondArray::from(vec![1_719_835_200_000_000_000])
            .with_timezone("Asia/Kolkata");

        for array in [
            Arc::new(second) as Arc<dyn Array>,
            Arc::new(milli),
            Arc::new(micro),
            Arc::new(nano),
        ] {
            let dt = array.data_type().clone();
            let result = invoke_date_part("timezone", ColumnarValue::Array(array), 1)
                .unwrap()
                .to_array(1)
                .unwrap();
            assert_eq!(
                as_int32_array(&result).unwrap(),
                &Int32Array::from(vec![19_800]),
                "unexpected offset for {dt}"
            );
        }
    }

    #[test]
    fn timezone_parts_on_scalar_input() {
        let scalar = ColumnarValue::Scalar(ScalarValue::TimestampNanosecond(
            Some(ts_nanos("2024-07-01T12:00:00Z")),
            Some("Europe/Brussels".into()),
        ));
        let ColumnarValue::Scalar(result) =
            invoke_date_part("timezone", scalar, 1).unwrap()
        else {
            panic!("expected a scalar result for a scalar input");
        };
        assert_eq!(result, ScalarValue::Int32(Some(7_200)));
    }

    #[test]
    fn timezone_parts_on_null_input() {
        for part in ["timezone", "timezone_hour", "timezone_minute"] {
            let scalar = ColumnarValue::Scalar(ScalarValue::TimestampNanosecond(
                None,
                Some("Europe/Brussels".into()),
            ));
            let ColumnarValue::Scalar(result) =
                invoke_date_part(part, scalar, 1).unwrap()
            else {
                panic!("expected a scalar result for a scalar input");
            };
            assert_eq!(result, ScalarValue::Int32(None));
        }
    }

    #[test]
    fn timezone_parts_reject_timezone_naive_timestamps() {
        for part in ["timezone", "timezone_hour", "timezone_minute"] {
            let scalar = ColumnarValue::Scalar(ScalarValue::TimestampNanosecond(
                Some(ts_nanos("2024-07-01T12:00:00Z")),
                None,
            ));
            let err = invoke_date_part(part, scalar, 1).unwrap_err().to_string();
            assert!(
                err.contains(&format!(
                    "Date part '{part}' is not supported for timezone-naive timestamps"
                )),
                "unexpected error for {part}: {err}"
            );
        }
    }

    #[test]
    fn timezone_parts_reject_non_timestamp_input() {
        let scalar = ColumnarValue::Scalar(ScalarValue::Date32(Some(19_875)));
        let err = invoke_date_part("timezone", scalar, 1)
            .unwrap_err()
            .to_string();
        assert!(
            err.contains(
                "Date part 'timezone' is only supported for timestamps with a timezone"
            ),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn timezone_parts_return_int32() {
        let arg_fields: Vec<FieldRef> = vec![
            Field::new("a", DataType::Utf8, false).into(),
            Field::new(
                "b",
                DataType::Timestamp(TimeUnit::Nanosecond, Some("Europe/Brussels".into())),
                true,
            )
            .into(),
        ];
        for part in ["timezone", "timezone_hour", "timezone_minute"] {
            let part = ScalarValue::from(part);
            let field = DatePartFunc::new()
                .return_field_from_args(ReturnFieldArgs {
                    arg_fields: &arg_fields,
                    scalar_arguments: &[Some(&part), None],
                })
                .unwrap();
            assert_eq!(field.data_type(), &DataType::Int32);
            assert!(field.is_nullable());
        }
    }
}
