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
use std::sync::{Arc, OnceLock};

use arrow::array::{Array, ArrayRef, Float64Array};
use arrow::compute::kernels::cast_utils::IntervalUnit;
use arrow::compute::{binary, cast, date_part, DatePart};
use arrow::datatypes::DataType::{
    Date32, Date64, Duration, Float64, Interval, Time32, Time64, Timestamp, Utf8,
    Utf8View,
};
use arrow::datatypes::IntervalUnit::{DayTime, MonthDayNano, YearMonth};
use arrow::datatypes::TimeUnit::{Microsecond, Millisecond, Nanosecond, Second};
use arrow::datatypes::{DataType, TimeUnit};

use datafusion_common::cast::{
    as_date32_array, as_date64_array, as_int32_array, as_time32_millisecond_array,
    as_time32_second_array, as_time64_microsecond_array, as_time64_nanosecond_array,
    as_timestamp_microsecond_array, as_timestamp_millisecond_array,
    as_timestamp_nanosecond_array, as_timestamp_second_array,
};
use datafusion_common::{exec_err, Result, ScalarValue};
use datafusion_expr::scalar_doc_sections::DOC_SECTION_DATETIME;
use datafusion_expr::TypeSignature::Exact;
use datafusion_expr::{
    ColumnarValue, Documentation, ScalarUDFImpl, Signature, Volatility, TIMEZONE_WILDCARD,
};

#[derive(Debug)]
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
                    Exact(vec![Utf8, Timestamp(Nanosecond, None)]),
                    Exact(vec![Utf8View, Timestamp(Nanosecond, None)]),
                    Exact(vec![
                        Utf8,
                        Timestamp(Nanosecond, Some(TIMEZONE_WILDCARD.into())),
                    ]),
                    Exact(vec![
                        Utf8View,
                        Timestamp(Nanosecond, Some(TIMEZONE_WILDCARD.into())),
                    ]),
                    Exact(vec![Utf8, Timestamp(Millisecond, None)]),
                    Exact(vec![Utf8View, Timestamp(Millisecond, None)]),
                    Exact(vec![
                        Utf8,
                        Timestamp(Millisecond, Some(TIMEZONE_WILDCARD.into())),
                    ]),
                    Exact(vec![
                        Utf8View,
                        Timestamp(Millisecond, Some(TIMEZONE_WILDCARD.into())),
                    ]),
                    Exact(vec![Utf8, Timestamp(Microsecond, None)]),
                    Exact(vec![Utf8View, Timestamp(Microsecond, None)]),
                    Exact(vec![
                        Utf8,
                        Timestamp(Microsecond, Some(TIMEZONE_WILDCARD.into())),
                    ]),
                    Exact(vec![
                        Utf8View,
                        Timestamp(Microsecond, Some(TIMEZONE_WILDCARD.into())),
                    ]),
                    Exact(vec![Utf8, Timestamp(Second, None)]),
                    Exact(vec![Utf8View, Timestamp(Second, None)]),
                    Exact(vec![
                        Utf8,
                        Timestamp(Second, Some(TIMEZONE_WILDCARD.into())),
                    ]),
                    Exact(vec![
                        Utf8View,
                        Timestamp(Second, Some(TIMEZONE_WILDCARD.into())),
                    ]),
                    Exact(vec![Utf8, Date64]),
                    Exact(vec![Utf8View, Date64]),
                    Exact(vec![Utf8, Date32]),
                    Exact(vec![Utf8View, Date32]),
                    Exact(vec![Utf8, Time32(Second)]),
                    Exact(vec![Utf8View, Time32(Second)]),
                    Exact(vec![Utf8, Time32(Millisecond)]),
                    Exact(vec![Utf8View, Time32(Millisecond)]),
                    Exact(vec![Utf8, Time64(Microsecond)]),
                    Exact(vec![Utf8View, Time64(Microsecond)]),
                    Exact(vec![Utf8, Time64(Nanosecond)]),
                    Exact(vec![Utf8View, Time64(Nanosecond)]),
                    Exact(vec![Utf8, Interval(YearMonth)]),
                    Exact(vec![Utf8View, Interval(YearMonth)]),
                    Exact(vec![Utf8, Interval(DayTime)]),
                    Exact(vec![Utf8View, Interval(DayTime)]),
                    Exact(vec![Utf8, Interval(MonthDayNano)]),
                    Exact(vec![Utf8View, Interval(MonthDayNano)]),
                    Exact(vec![Utf8, Duration(Second)]),
                    Exact(vec![Utf8View, Duration(Second)]),
                    Exact(vec![Utf8, Duration(Millisecond)]),
                    Exact(vec![Utf8View, Duration(Millisecond)]),
                    Exact(vec![Utf8, Duration(Microsecond)]),
                    Exact(vec![Utf8View, Duration(Microsecond)]),
                    Exact(vec![Utf8, Duration(Nanosecond)]),
                    Exact(vec![Utf8View, Duration(Nanosecond)]),
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
        Ok(Float64)
    }

    fn invoke(&self, args: &[ColumnarValue]) -> Result<ColumnarValue> {
        if args.len() != 2 {
            return exec_err!("Expected two arguments in DATE_PART");
        }
        let (part, array) = (&args[0], &args[1]);

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
            ColumnarValue::Array(array) => Arc::clone(array),
            ColumnarValue::Scalar(scalar) => scalar.to_array()?,
        };

        // to remove quotes at most 2 characters
        let part_trim = part.trim_matches(|c| c == '\'' || c == '\"');
        if ![2, 0].contains(&(part.len() - part_trim.len())) {
            return exec_err!("Date part '{part}' not supported");
        }

        // using IntervalUnit here means we hand off all the work of supporting plurals (like "seconds")
        // and synonyms ( like "ms,msec,msecond,millisecond") to Arrow
        let arr = if let Ok(interval_unit) = IntervalUnit::from_str(part_trim) {
            match interval_unit {
                IntervalUnit::Year => date_part_f64(array.as_ref(), DatePart::Year)?,
                IntervalUnit::Month => date_part_f64(array.as_ref(), DatePart::Month)?,
                IntervalUnit::Week => date_part_f64(array.as_ref(), DatePart::Week)?,
                IntervalUnit::Day => date_part_f64(array.as_ref(), DatePart::Day)?,
                IntervalUnit::Hour => date_part_f64(array.as_ref(), DatePart::Hour)?,
                IntervalUnit::Minute => date_part_f64(array.as_ref(), DatePart::Minute)?,
                IntervalUnit::Second => seconds(array.as_ref(), Second)?,
                IntervalUnit::Millisecond => seconds(array.as_ref(), Millisecond)?,
                IntervalUnit::Microsecond => seconds(array.as_ref(), Microsecond)?,
                IntervalUnit::Nanosecond => seconds(array.as_ref(), Nanosecond)?,
                // century and decade are not supported by `DatePart`, although they are supported in postgres
                _ => return exec_err!("Date part '{part}' not supported"),
            }
        } else {
            // special cases that can be extracted (in postgres) but are not interval units
            match part_trim.to_lowercase().as_str() {
                "qtr" | "quarter" => date_part_f64(array.as_ref(), DatePart::Quarter)?,
                "doy" => date_part_f64(array.as_ref(), DatePart::DayOfYear)?,
                "dow" => date_part_f64(array.as_ref(), DatePart::DayOfWeekSunday0)?,
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

    fn aliases(&self) -> &[String] {
        &self.aliases
    }
    fn documentation(&self) -> Option<&Documentation> {
        Some(get_date_part_doc())
    }
}

static DOCUMENTATION: OnceLock<Documentation> = OnceLock::new();

fn get_date_part_doc() -> &'static Documentation {
    DOCUMENTATION.get_or_init(|| {
        Documentation::builder()
            .with_doc_section(DOC_SECTION_DATETIME)
            .with_description("Returns the specified part of the date as an integer.")
            .with_syntax_example("date_part(part, expression)")
            .with_argument(
                "part",
                r#"Part of the date to return. The following date parts are supported:

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
    - dow (day of the week)
    - doy (day of the year)
    - epoch (seconds since Unix epoch)
"#,
            )
            .with_argument(
                "expression",
                "Time expression to operate on. Can be a constant, column, or function.",
            )
            .with_alternative_syntax("extract(field FROM source)")
            .build()
            .unwrap()
    })
}

/// Invoke [`date_part`] and cast the result to Float64
fn date_part_f64(array: &dyn Array, part: DatePart) -> Result<ArrayRef> {
    Ok(cast(date_part(array, part)?.as_ref(), &Float64)?)
}

/// Invoke [`date_part`] on an `array` (e.g. Timestamp) and convert the
/// result to a total number of seconds, milliseconds, microseconds or
/// nanoseconds
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
