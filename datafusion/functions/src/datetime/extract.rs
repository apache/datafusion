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
use arrow::array::{Array, ArrayRef, Float64Array, Int32Array, PrimitiveBuilder};
use arrow::compute::kernels::cast_utils::IntervalUnit;
use arrow::compute::{binary, date_part, DatePart};
use arrow::datatypes::DataType::{
    Date32, Date64, Duration, Interval, Time32, Time64, Timestamp,
};
use arrow::datatypes::TimeUnit::{Microsecond, Millisecond, Nanosecond, Second};
use arrow::datatypes::{
    ArrowTimestampType, DataType, Field, FieldRef, Int32Type, TimeUnit,
    TimestampMicrosecondType, TimestampMillisecondType, TimestampNanosecondType,
    TimestampSecondType,
};
use chrono::{DateTime, MappedLocalTime, Offset, TimeDelta, TimeZone, Utc};
use datafusion_common::cast::as_primitive_array;
use datafusion_common::types::{logical_date, NativeType};
use std::ops::Add;

use datafusion_common::{
    cast::{
        as_date32_array, as_date64_array, as_int32_array, as_time32_millisecond_array,
        as_time32_second_array, as_time64_microsecond_array, as_time64_nanosecond_array,
        as_timestamp_microsecond_array, as_timestamp_millisecond_array,
        as_timestamp_nanosecond_array, as_timestamp_second_array,
    },
    exec_err, internal_datafusion_err, internal_err, not_impl_err,
    types::logical_string,
    utils::take_function_args,
    Result, ScalarValue,
};
use datafusion_expr::{
    ColumnarValue, Documentation, ReturnFieldArgs, ScalarUDFImpl, Signature,
    TypeSignature, Volatility,
};
use datafusion_expr_common::signature::{Coercion, TypeSignatureClass};
use datafusion_macros::user_doc;

#[user_doc(
    doc_section(label = "Time and Date Functions"),
    description = "Returns the specified part of the date as an integer.",
    syntax_example = "extract(field FROM source)",
    argument(
        name = "field",
        description = r#"Part of the date to return. The following date parts are supported:

- year
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
- epoch (seconds since Unix epoch)
- isodow (day of the week where Monday is 0)
"#
    ),
    argument(
        name = "source",
        description = "Time expression to operate on. Can be a constant, column, or function."
    )
)]
#[derive(Debug, PartialEq, Eq, Hash)]
pub struct ExtractFunc {
    signature: Signature,
}

impl Default for ExtractFunc {
    fn default() -> Self {
        Self::new()
    }
}

impl ExtractFunc {
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
        }
    }
}

impl ScalarUDFImpl for ExtractFunc {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "extract"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        internal_err!("return_field_from_args should be called instead")
    }

    fn return_field_from_args(&self, args: ReturnFieldArgs) -> Result<FieldRef> {
        let [field, _] = take_function_args(self.name(), args.scalar_arguments)?;

        field
            .and_then(|sv| {
                sv.try_as_str()
                    .flatten()
                    .filter(|s| !s.is_empty())
                    .map(|part| {
                        if is_epoch(part) {
                            Field::new(self.name(), DataType::Float64, true)
                        } else {
                            Field::new(self.name(), DataType::Int32, true)
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
        let config = &args.config_options;
        let args = args.args;
        let [part, array] = take_function_args(self.name(), args)?;

        let part = if let ColumnarValue::Scalar(ScalarValue::Utf8(Some(v))) = part {
            v
        } else if let ColumnarValue::Scalar(ScalarValue::Utf8View(Some(v))) = part {
            v
        } else {
            return exec_err!("First argument of `EXTRACT` must be non-null scalar Utf8");
        };

        let is_scalar = matches!(array, ColumnarValue::Scalar(_));

        let array = match array {
            ColumnarValue::Array(array) => Arc::clone(&array),
            ColumnarValue::Scalar(scalar) => scalar.to_array()?,
        };

        let (is_timezone_aware, tz_str_opt) = match array.data_type() {
            Timestamp(_, Some(tz_str)) => (true, Some(Arc::clone(tz_str))),
            _ => (false, None),
        };

        // Adjust timestamps for extraction
        let array = if is_timezone_aware {
            // For timezone-aware timestamps, extract in their own timezone
            let tz_str = tz_str_opt.as_ref().unwrap();
            let tz = match tz_str.parse::<Tz>() {
                Ok(tz) => tz,
                Err(_) => return exec_err!("Invalid timezone"),
            };
            match array.data_type() {
                Timestamp(time_unit, _) => match time_unit {
                    Nanosecond => {
                        adjust_timestamp_array::<TimestampNanosecondType>(&array, tz)?
                    }
                    Microsecond => {
                        adjust_timestamp_array::<TimestampMicrosecondType>(&array, tz)?
                    }
                    Millisecond => {
                        adjust_timestamp_array::<TimestampMillisecondType>(&array, tz)?
                    }
                    Second => adjust_timestamp_array::<TimestampSecondType>(&array, tz)?,
                },
                _ => array,
            }
        } else if let Timestamp(time_unit, None) = array.data_type() {
            // For naive timestamps, interpret in session timezone
            let tz = match config.execution.time_zone.parse::<Tz>() {
                Ok(tz) => tz,
                Err(_) => return exec_err!("Invalid timezone"),
            };
            match time_unit {
                Nanosecond => {
                    adjust_timestamp_array::<TimestampNanosecondType>(&array, tz)?
                }
                Microsecond => {
                    adjust_timestamp_array::<TimestampMicrosecondType>(&array, tz)?
                }
                Millisecond => {
                    adjust_timestamp_array::<TimestampMillisecondType>(&array, tz)?
                }
                Second => adjust_timestamp_array::<TimestampSecondType>(&array, tz)?,
            }
        } else {
            array
        };

        let part_trim = part_normalization(&part);

        // using IntervalUnit here means we hand off all the work of supporting plurals (like "seconds")
        // and synonyms ( like "ms,msec,msecond,millisecond") to Arrow
        let mut arr = if let Ok(interval_unit) = IntervalUnit::from_str(part_trim) {
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
                "qtr" | "quarter" => date_part(array.as_ref(), DatePart::Quarter)?,
                "doy" => date_part(array.as_ref(), DatePart::DayOfYear)?,
                "dow" => date_part(array.as_ref(), DatePart::DayOfWeekSunday0)?,
                "isodow" => date_part(array.as_ref(), DatePart::DayOfWeekMonday0)?,
                "epoch" => epoch(array.as_ref())?,
                _ => return exec_err!("Date part '{part}' not supported"),
            }
        };

        // Special adjustment for hour extraction on timezone-aware timestamps
        if is_timezone_aware && part_trim.to_lowercase() == "hour" {
            if let Some(tz_str) = &tz_str_opt {
                let offset_hours = if tz_str.as_ref() == "+00:00" {
                    0
                } else {
                    let sign = if tz_str.starts_with('+') { 1i32 } else { -1i32 };
                    let hours_str = &tz_str[1..3];
                    let hours: i32 = hours_str.parse().unwrap();
                    sign * hours
                };
                let int_arr = as_int32_array(&arr)?;
                let mut builder = PrimitiveBuilder::<Int32Type>::new();
                for i in 0..arr.len() {
                    if arr.is_null(i) {
                        builder.append_null();
                    } else {
                        let v = int_arr.value(i);
                        builder.append_value(v + offset_hours);
                    }
                }
                arr = Arc::new(builder.finish());
            }
        }

        Ok(if is_scalar {
            ColumnarValue::Scalar(ScalarValue::try_from_array(arr.as_ref(), 0)?)
        } else {
            ColumnarValue::Array(arr)
        })
    }

    fn aliases(&self) -> &[String] {
        &[]
    }

    fn documentation(&self) -> Option<&Documentation> {
        self.doc()
    }
}

fn adjust_to_local_time<T: ArrowTimestampType>(ts: i64, tz: Tz) -> Result<i64> {
    fn convert_timestamp<F>(ts: i64, converter: F) -> Result<DateTime<Utc>>
    where
        F: Fn(i64) -> MappedLocalTime<DateTime<Utc>>,
    {
        match converter(ts) {
            MappedLocalTime::Ambiguous(earliest, latest) => exec_err!(
                "Ambiguous timestamp. Do you mean {:?} or {:?}",
                earliest,
                latest
            ),
            MappedLocalTime::None => exec_err!(
                "The local time does not exist because there is a gap in the local time."
            ),
            MappedLocalTime::Single(date_time) => Ok(date_time),
        }
    }

    let date_time = match T::UNIT {
        Nanosecond => Utc.timestamp_nanos(ts),
        Microsecond => convert_timestamp(ts, |ts| Utc.timestamp_micros(ts))?,
        Millisecond => convert_timestamp(ts, |ts| Utc.timestamp_millis_opt(ts))?,
        Second => convert_timestamp(ts, |ts| Utc.timestamp_opt(ts, 0))?,
    };

    let offset_seconds: i64 = tz
        .offset_from_utc_datetime(&date_time.naive_utc())
        .fix()
        .local_minus_utc() as i64;

    let adjusted_date_time = date_time.add(
        TimeDelta::try_seconds(offset_seconds)
            .ok_or_else(|| internal_datafusion_err!("Offset seconds should be less than i64::MAX / 1_000 or greater than -i64::MAX / 1_000"))?,
    );

    // convert back to i64
    match T::UNIT {
        Nanosecond => adjusted_date_time.timestamp_nanos_opt().ok_or_else(|| {
            internal_datafusion_err!(
                "Failed to convert DateTime to timestamp in nanosecond. This error may occur if the date is out of range. The supported date ranges are between 1677-09-21T00:12:43.145224192 and 2262-04-11T23:47:16.854775807"
            )
        }),
        Microsecond => Ok(adjusted_date_time.timestamp_micros()),
        Millisecond => Ok(adjusted_date_time.timestamp_millis()),
        Second => Ok(adjusted_date_time.timestamp()),
    }
}

fn adjust_timestamp_array<T: ArrowTimestampType>(
    array: &ArrayRef,
    tz: Tz,
) -> Result<ArrayRef> {
    let mut builder = PrimitiveBuilder::<T>::new();
    let primitive_array = as_primitive_array::<T>(array)?;
    for ts_opt in primitive_array.iter() {
        match ts_opt {
            None => builder.append_null(),
            Some(ts) => {
                let adjusted_ts = adjust_to_local_time::<T>(ts, tz)?;
                builder.append_value(adjusted_ts);
            }
        }
    }
    Ok(Arc::new(builder.finish()))
}

fn is_epoch(part: &str) -> bool {
    let part = part_normalization(part);
    matches!(part.to_lowercase().as_str(), "epoch")
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
        Interval(_) | Duration(_) => return seconds(array, Second),
        d => return exec_err!("Cannot convert {d:?} to epoch"),
    };
    Ok(Arc::new(f))
}
