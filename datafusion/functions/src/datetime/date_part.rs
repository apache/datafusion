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

use arrow::array::{Array, ArrayRef, Float64Array, Int32Array};
use arrow::compute::kernels::cast_utils::IntervalUnit;
use arrow::compute::{binary, date_part, DatePart};
use arrow::datatypes::DataType::{
    Date32, Date64, Duration, Interval, Time32, Time64, Timestamp,
};
use arrow::datatypes::TimeUnit::{Microsecond, Millisecond, Nanosecond, Second};
use arrow::datatypes::{DataType, TimeUnit};

use crate::utils::take_function_args;
use datafusion_common::not_impl_err;
use datafusion_common::{
    cast::{
        as_date32_array, as_date64_array, as_int32_array, as_time32_millisecond_array,
        as_time32_second_array, as_time64_microsecond_array, as_time64_nanosecond_array,
        as_timestamp_microsecond_array, as_timestamp_millisecond_array,
        as_timestamp_nanosecond_array, as_timestamp_second_array,
    },
    exec_err, internal_err,
    types::logical_string,
    Result, ScalarValue,
};
use datafusion_expr::{
    ColumnarValue, Documentation, ReturnInfo, ReturnTypeArgs, ScalarUDFImpl, Signature,
    TypeSignature, Volatility,
};
use datafusion_expr_common::signature::TypeSignatureClass;
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
"#
    ),
    argument(
        name = "expression",
        description = "Time expression to operate on. Can be a constant, column, or function."
    )
)]
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
                    TypeSignature::Coercible(vec![
                        TypeSignatureClass::Native(logical_string()),
                        TypeSignatureClass::Timestamp,
                    ]),
                    TypeSignature::Coercible(vec![
                        TypeSignatureClass::Native(logical_string()),
                        TypeSignatureClass::Date,
                    ]),
                    TypeSignature::Coercible(vec![
                        TypeSignatureClass::Native(logical_string()),
                        TypeSignatureClass::Time,
                    ]),
                    TypeSignature::Coercible(vec![
                        TypeSignatureClass::Native(logical_string()),
                        TypeSignatureClass::Interval,
                    ]),
                    TypeSignature::Coercible(vec![
                        TypeSignatureClass::Native(logical_string()),
                        TypeSignatureClass::Duration,
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
        internal_err!("return_type_from_args should be called instead")
    }

    fn return_type_from_args(&self, args: ReturnTypeArgs) -> Result<ReturnInfo> {
        let [field, _] = take_function_args(self.name(), args.scalar_arguments)?;

        field
            .and_then(|sv| {
                sv.try_as_str()
                    .flatten()
                    .filter(|s| !s.is_empty())
                    .map(|part| {
                        if is_epoch(part) {
                            ReturnInfo::new_nullable(DataType::Float64)
                        } else {
                            ReturnInfo::new_nullable(DataType::Int32)
                        }
                    })
            })
            .map_or_else(
                || exec_err!("{} requires non-empty constant string", self.name()),
                Ok,
            )
    }

    fn invoke_batch(
        &self,
        args: &[ColumnarValue],
        _number_rows: usize,
    ) -> Result<ColumnarValue> {
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

        let part_trim = part_normalization(part);

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
                "qtr" | "quarter" => date_part(array.as_ref(), DatePart::Quarter)?,
                "doy" => date_part(array.as_ref(), DatePart::DayOfYear)?,
                "dow" => date_part(array.as_ref(), DatePart::DayOfWeekSunday0)?,
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
        self.doc()
    }
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
