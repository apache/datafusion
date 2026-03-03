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
use std::str::FromStr;
use std::sync::Arc;

use arrow::array::timezone::Tz;
use arrow::array::{Array, ArrayRef, Float64Array, Int32Array};
use arrow::compute::kernels::cast_utils::IntervalUnit;
use arrow::compute::{DatePart, binary, date_part};
use arrow::datatypes::DataType::{
    Date32, Date64, Duration, Interval, Time32, Time64, Timestamp,
};
use arrow::datatypes::TimeUnit::{Microsecond, Millisecond, Nanosecond, Second};
use arrow::datatypes::{
    DataType, Date32Type, Date64Type, Field, FieldRef, IntervalUnit as ArrowIntervalUnit,
    TimeUnit,
};
use chrono::{Datelike, NaiveDate, TimeZone, Utc};
use datafusion_common::types::{NativeType, logical_date};

use datafusion_common::{
    Result, ScalarValue,
    cast::{
        as_date32_array, as_date64_array, as_int32_array, as_interval_dt_array,
        as_interval_mdn_array, as_interval_ym_array, as_time32_millisecond_array,
        as_time32_second_array, as_time64_microsecond_array, as_time64_nanosecond_array,
        as_timestamp_microsecond_array, as_timestamp_millisecond_array,
        as_timestamp_nanosecond_array, as_timestamp_second_array,
    },
    exec_err, internal_err, not_impl_err,
    types::logical_string,
    utils::take_function_args,
};
use datafusion_expr::preimage::PreimageResult;
use datafusion_expr::simplify::SimplifyContext;
use datafusion_expr::{
    ColumnarValue, Documentation, Expr, ReturnFieldArgs, ScalarUDFImpl, Signature,
    TypeSignature, Volatility, interval_arithmetic,
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
    - isodow (day of the week where Monday is 0)
"#
    ),
    argument(
        name = "expression",
        description = "Time expression to operate on. Can be a constant, column, or function."
    )
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
    fn as_any(&self) -> &dyn Any {
        self
    }

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

    fn invoke_with_args(
        &self,
        args: datafusion_expr::ScalarFunctionArgs,
    ) -> Result<ColumnarValue> {
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

        // using IntervalUnit here means we hand off all the work of supporting plurals (like "seconds")
        // and synonyms ( like "ms,msec,msecond,millisecond") to Arrow
        let arr = if let Ok(interval_unit) = IntervalUnit::from_str(part_trim) {
            match interval_unit {
                IntervalUnit::Year => date_part(array.as_ref(), DatePart::Year)?,
                IntervalUnit::Month => date_part(array.as_ref(), DatePart::Month)?,
                IntervalUnit::Week => date_part(array.as_ref(), DatePart::Week)?,
                IntervalUnit::Day => date_part(array.as_ref(), DatePart::Day)?,
                IntervalUnit::Hour => date_part(array.as_ref(), DatePart::Hour)?,
                IntervalUnit::Minute => date_part(array.as_ref(), DatePart::Minute)?,
                IntervalUnit::Second => seconds_as_i32(array.as_ref(), Second)?,
                IntervalUnit::Millisecond => seconds_as_i32(array.as_ref(), Millisecond)?,
                IntervalUnit::Microsecond => seconds_as_i32(array.as_ref(), Microsecond)?,
                IntervalUnit::Nanosecond => seconds_as_i32(array.as_ref(), Nanosecond)?,
                // century and decade are not supported by `DatePart`, although they are supported in postgres
                _ => return exec_err!("Date part '{part}' not supported"),
            }
        } else {
            // special cases that can be extracted (in postgres) but are not interval units
            match part_trim.to_lowercase().as_str() {
                "isoyear" => date_part(array.as_ref(), DatePart::YearISO)?,
                "qtr" | "quarter" => date_part(array.as_ref(), DatePart::Quarter)?,
                "doy" => date_part(array.as_ref(), DatePart::DayOfYear)?,
                "dow" => date_part(array.as_ref(), DatePart::DayOfWeekSunday0)?,
                "isodow" => date_part(array.as_ref(), DatePart::DayOfWeekMonday0)?,
                "epoch" => epoch(array.as_ref())?,
                _ => return exec_err!("Date part '{part}' not supported"),
            }
        };

        Ok(if is_scalar {
            ColumnarValue::Scalar(ScalarValue::try_from_array(arr.as_ref(), 0)?)
        } else {
            ColumnarValue::Array(arr)
        })
    }

    // Only casting the year is supported since pruning other IntervalUnit is not possible
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

        // Get the interval unit from the part argument
        let interval_unit = part
            .as_literal()
            .and_then(|sv| sv.try_as_str().flatten())
            .map(part_normalization)
            .and_then(|s| IntervalUnit::from_str(s).ok());

        // only support extracting year
        match interval_unit {
            Some(IntervalUnit::Year) => (),
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

fn date_to_scalar(date: NaiveDate, target_type: &DataType) -> Option<ScalarValue> {
    Some(match target_type {
        Date32 => ScalarValue::Date32(Some(Date32Type::from_naive_date(date))),
        Date64 => ScalarValue::Date64(Some(Date64Type::from_naive_date(date))),

        Timestamp(unit, tz_opt) => {
            let naive_midnight = date.and_hms_opt(0, 0, 0)?;

            let utc_dt = if let Some(tz_str) = tz_opt {
                let tz: Tz = tz_str.parse().ok()?;

                let local = tz.from_local_datetime(&naive_midnight);

                let local_dt = match local {
                    chrono::offset::LocalResult::Single(dt) => dt,
                    chrono::offset::LocalResult::Ambiguous(dt1, _dt2) => dt1,
                    chrono::offset::LocalResult::None => local.earliest()?,
                };

                local_dt.with_timezone(&Utc)
            } else {
                Utc.from_utc_datetime(&naive_midnight)
            };

            match unit {
                Second => {
                    ScalarValue::TimestampSecond(Some(utc_dt.timestamp()), tz_opt.clone())
                }
                Millisecond => ScalarValue::TimestampMillisecond(
                    Some(utc_dt.timestamp_millis()),
                    tz_opt.clone(),
                ),
                Microsecond => ScalarValue::TimestampMicrosecond(
                    Some(utc_dt.timestamp_micros()),
                    tz_opt.clone(),
                ),
                Nanosecond => ScalarValue::TimestampNanosecond(
                    Some(utc_dt.timestamp_nanos_opt()?),
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
/// nanoseconds
fn seconds_as_i32(array: &dyn Array, unit: TimeUnit) -> Result<ArrayRef> {
    // Nanosecond is neither supported in Postgres nor DuckDB, to avoid dealing
    // with overflow and precision issue we don't support nanosecond
    if unit == Nanosecond {
        return not_impl_err!("Date part {unit:?} not supported");
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
