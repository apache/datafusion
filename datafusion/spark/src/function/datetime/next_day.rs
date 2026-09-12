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

use std::sync::Arc;

use arrow::array::{Array, ArrayRef, AsArray, Date32Array, StringArrayType};
use arrow::datatypes::{DataType, Date32Type, Field, FieldRef};
use chrono::{Datelike, Weekday};
use datafusion_common::{Result, ScalarValue, exec_err, internal_err};
use datafusion_expr::{
    ColumnarValue, ReturnFieldArgs, ScalarFunctionArgs, ScalarUDFImpl, Signature,
    Volatility,
};

/// <https://spark.apache.org/docs/latest/api/sql/index.html#next_day>
#[derive(Debug, PartialEq, Eq, Hash)]
pub struct SparkNextDay {
    signature: Signature,
}

impl Default for SparkNextDay {
    fn default() -> Self {
        Self::new()
    }
}

impl SparkNextDay {
    pub fn new() -> Self {
        Self {
            signature: Signature::exact(
                vec![DataType::Date32, DataType::Utf8],
                Volatility::Immutable,
            ),
        }
    }
}

impl ScalarUDFImpl for SparkNextDay {
    fn name(&self) -> &str {
        "next_day"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        internal_err!("return_field_from_args should be used instead")
    }

    fn return_field_from_args(&self, _args: ReturnFieldArgs) -> Result<FieldRef> {
        // Spark marks next_day as always nullable because invalid day_of_week values
        // can yield NULL even when inputs are non-null.
        Ok(Arc::new(Field::new(self.name(), DataType::Date32, true)))
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let ScalarFunctionArgs {
            args,
            config_options,
            ..
        } = args;
        let ansi_mode = config_options.execution.enable_ansi_mode;
        let [date, day_of_week] = args.as_slice() else {
            return exec_err!(
                "Spark `next_day` function requires 2 arguments, got {}",
                args.len()
            );
        };

        match (date, day_of_week) {
            (ColumnarValue::Scalar(date), ColumnarValue::Scalar(day_of_week)) => {
                match (date, day_of_week) {
                    (
                        ScalarValue::Date32(days),
                        ScalarValue::Utf8(day_of_week)
                        | ScalarValue::LargeUtf8(day_of_week)
                        | ScalarValue::Utf8View(day_of_week),
                    ) => {
                        // Spark's `NextDay` is null intolerant, so a NULL in either
                        // argument short circuits to NULL even under ANSI mode.
                        if let (Some(days), Some(day_of_week)) = (days, day_of_week) {
                            match parse_day_of_week(day_of_week.as_str()) {
                                Some(weekday) => {
                                    Ok(ColumnarValue::Scalar(ScalarValue::Date32(
                                        next_date_for_day_of_week(*days, weekday),
                                    )))
                                }
                                None if ansi_mode => {
                                    illegal_day_of_week_err(day_of_week.as_str())
                                }
                                None => {
                                    Ok(ColumnarValue::Scalar(ScalarValue::Date32(None)))
                                }
                            }
                        } else {
                            Ok(ColumnarValue::Scalar(ScalarValue::Date32(None)))
                        }
                    }
                    _ => exec_err!(
                        "Spark `next_day` function: first arg must be date, second arg must be string. Got {args:?}"
                    ),
                }
            }
            (ColumnarValue::Array(date_array), ColumnarValue::Scalar(day_of_week)) => {
                match (date_array.data_type(), day_of_week) {
                    (
                        DataType::Date32,
                        ScalarValue::Utf8(day_of_week)
                        | ScalarValue::LargeUtf8(day_of_week)
                        | ScalarValue::Utf8View(day_of_week),
                    ) => {
                        match day_of_week
                            .as_ref()
                            .map(|d| (d.as_str(), parse_day_of_week(d.as_str())))
                        {
                            Some((_, Some(weekday))) => {
                                let result: Date32Array = date_array
                                    .as_primitive::<Date32Type>()
                                    .unary_opt(|days| {
                                        next_date_for_day_of_week(days, weekday)
                                    })
                                    .with_data_type(DataType::Date32);
                                Ok(ColumnarValue::Array(Arc::new(result) as ArrayRef))
                            }
                            // The day name is unparsable. Spark's `NextDay` is null
                            // intolerant per row, so a NULL start date short circuits
                            // to NULL before the day name is validated. Raise only if
                            // at least one row has a non-NULL start date, which is
                            // exactly when the row-wise `process_next_day_arrays` path
                            // raises for the same inputs.
                            Some((raw, None))
                                if ansi_mode
                                    && date_array.null_count() < date_array.len() =>
                            {
                                illegal_day_of_week_err(raw)
                            }
                            // Every remaining case yields NULL for every row: an
                            // unparsable day name with ANSI mode off or with no
                            // non-NULL start date, or a NULL `day_of_week`.
                            _ => Ok(ColumnarValue::Scalar(ScalarValue::Date32(None))),
                        }
                    }
                    _ => exec_err!(
                        "Spark `next_day` function: first arg must be date, second arg must be string. Got {args:?}"
                    ),
                }
            }
            (ColumnarValue::Scalar(date), ColumnarValue::Array(day_of_week_array)) => {
                let date_array = date.to_array_of_size(day_of_week_array.len())?;
                Ok(ColumnarValue::Array(next_day_arrays(
                    &date_array,
                    day_of_week_array,
                    ansi_mode,
                )?))
            }
            (
                ColumnarValue::Array(date_array),
                ColumnarValue::Array(day_of_week_array),
            ) => Ok(ColumnarValue::Array(next_day_arrays(
                date_array,
                day_of_week_array,
                ansi_mode,
            )?)),
        }
    }
}

fn next_day_arrays(
    date_array: &ArrayRef,
    day_of_week_array: &ArrayRef,
    ansi_mode: bool,
) -> Result<ArrayRef> {
    match (date_array.data_type(), day_of_week_array.data_type()) {
        (DataType::Date32, DataType::Utf8) => process_next_day_arrays(
            date_array.as_primitive::<Date32Type>(),
            day_of_week_array.as_string::<i32>(),
            ansi_mode,
        ),
        (DataType::Date32, DataType::LargeUtf8) => process_next_day_arrays(
            date_array.as_primitive::<Date32Type>(),
            day_of_week_array.as_string::<i64>(),
            ansi_mode,
        ),
        (DataType::Date32, DataType::Utf8View) => process_next_day_arrays(
            date_array.as_primitive::<Date32Type>(),
            day_of_week_array.as_string_view(),
            ansi_mode,
        ),
        (left, right) => exec_err!(
            "Spark `next_day` function: first arg must be date, second arg must be string. Got {left:?}, {right:?}"
        ),
    }
}

fn process_next_day_arrays<'a, S>(
    date_array: &Date32Array,
    day_of_week_array: &'a S,
    ansi_mode: bool,
) -> Result<ArrayRef>
where
    &'a S: StringArrayType<'a>,
{
    let mut builder = Date32Array::builder(date_array.len());
    for (days, day_of_week) in date_array.iter().zip(day_of_week_array.iter()) {
        // Spark's `NextDay` is null intolerant, so a NULL in either argument
        // short circuits to NULL even under ANSI mode.
        let (Some(days), Some(day_of_week)) = (days, day_of_week) else {
            builder.append_null();
            continue;
        };
        match parse_day_of_week(day_of_week) {
            Some(weekday) => {
                builder.append_option(next_date_for_day_of_week(days, weekday))
            }
            None if ansi_mode => return illegal_day_of_week_err(day_of_week),
            None => builder.append_null(),
        }
    }
    Ok(Arc::new(builder.finish()) as ArrayRef)
}

/// Mirrors Spark's `DateTimeUtils.getDayOfWeekFromString`: case insensitive, and
/// with no whitespace trimming.
fn parse_day_of_week(day_of_week: &str) -> Option<Weekday> {
    match day_of_week.to_uppercase().as_str() {
        "SU" | "SUN" | "SUNDAY" => Some(Weekday::Sun),
        "MO" | "MON" | "MONDAY" => Some(Weekday::Mon),
        "TU" | "TUE" | "TUESDAY" => Some(Weekday::Tue),
        "WE" | "WED" | "WEDNESDAY" => Some(Weekday::Wed),
        "TH" | "THU" | "THURSDAY" => Some(Weekday::Thu),
        "FR" | "FRI" | "FRIDAY" => Some(Weekday::Fri),
        "SA" | "SAT" | "SATURDAY" => Some(Weekday::Sat),
        _ => None,
    }
}

/// The first date strictly after `days` that falls on `weekday`. Equivalent to
/// Spark's `DateTimeUtils.getNextDateForDayOfWeek`, so a start date already on
/// `weekday` advances a full week.
///
/// Computes the result on the epoch day directly instead of constructing a
/// `NaiveDate` for it: the result can land past `NaiveDate::MAX` (epoch day
/// 95026236), and building that date panics (`NaiveDate + TimeDelta
/// overflowed`). Spark's `getNextDateForDayOfWeek` is pure `Int` arithmetic and
/// keeps producing a value up to `Int.MaxValue`.
///
/// Returns `None` when `days` is not a representable date, since the weekday is
/// still derived via `NaiveDate`.
fn next_date_for_day_of_week(days: i32, weekday: Weekday) -> Option<i32> {
    let date = Date32Type::to_naive_date_opt(days)?;
    let delta = 7 - date.weekday().days_since(weekday) as i32;
    days.checked_add(delta)
}

/// Matches Spark's `ILLEGAL_DAY_OF_WEEK` error, raised when ANSI mode is enabled
/// and `day_of_week` does not name a day.
fn illegal_day_of_week_err<T>(day_of_week: &str) -> Result<T> {
    exec_err!("Illegal input for day of week: {day_of_week}.")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn return_type_is_not_used() {
        let func = SparkNextDay::new();
        let err = func
            .return_type(&[DataType::Date32, DataType::Utf8])
            .unwrap_err();
        assert!(
            err.to_string()
                .contains("return_field_from_args should be used instead")
        );
    }

    #[test]
    fn next_day_is_always_nullable() {
        let func = SparkNextDay::new();
        let date_field: FieldRef =
            Arc::new(Field::new("start_date", DataType::Date32, false));
        let day_field: FieldRef =
            Arc::new(Field::new("day_of_week", DataType::Utf8, false));

        let field = func
            .return_field_from_args(ReturnFieldArgs {
                arg_fields: &[Arc::clone(&date_field), Arc::clone(&day_field)],
                scalar_arguments: &[None, None],
            })
            .unwrap();

        assert_eq!(field.data_type(), &DataType::Date32);
        assert!(field.is_nullable());
    }

    #[test]
    fn next_day_rejects_whitespace_padded_day_names() {
        assert_eq!(parse_day_of_week(" MO "), None);
        assert_eq!(parse_day_of_week("MO "), None);
        assert_eq!(parse_day_of_week(""), None);
        assert_eq!(parse_day_of_week("mo"), Some(Weekday::Mon));
    }

    #[test]
    fn next_day_advances_a_full_week_on_the_same_weekday() {
        let monday = 19723; // 2024-01-01
        assert_eq!(
            next_date_for_day_of_week(monday, Weekday::Mon),
            Some(monday + 7)
        );
        assert_eq!(
            next_date_for_day_of_week(monday, Weekday::Tue),
            Some(monday + 1)
        );
    }

    #[test]
    fn next_day_returns_values_past_the_last_representable_date() {
        // `chrono::NaiveDate::MAX` is +262142-12-31, which is a Monday. The
        // result is computed on the epoch day, so a next occurrence past that
        // date still produces a value, as Spark's `Int` arithmetic does.
        let max = 95026236;
        assert_eq!(next_date_for_day_of_week(max, Weekday::Mon), Some(max + 7));
        assert_eq!(next_date_for_day_of_week(max - 1, Weekday::Mon), Some(max));
        // A start date already on the requested weekday advances a full week,
        // so it lands one day past `max` from six days earlier.
        assert_eq!(
            next_date_for_day_of_week(max - 6, Weekday::Tue),
            Some(max + 1)
        );
        assert_eq!(next_date_for_day_of_week(max - 6, Weekday::Mon), Some(max));
        // The weekday is still derived via `NaiveDate`, so a *start* day that is
        // not a representable date returns NULL.
        assert_eq!(next_date_for_day_of_week(i32::MAX, Weekday::Mon), None);
        assert_eq!(next_date_for_day_of_week(i32::MIN, Weekday::Mon), None);
    }

    #[test]
    fn next_day_handles_far_future_start_dates() {
        // Regression for #23891: for start dates near the end of the
        // representable `Date32` range, the next occurrence can land past
        // `chrono::NaiveDate::MAX` (epoch day 95026236). Computing the result
        // on the epoch day directly (as Spark does) must return a value rather
        // than panicking with `NaiveDate + TimeDelta overflowed`.
        //
        // 95026236 is a Monday, so `next_day(.., "Mon")` advances a full week.
        assert_eq!(
            next_date_for_day_of_week(95026236, Weekday::Mon),
            Some(95026243)
        );
        assert_eq!(
            next_date_for_day_of_week(95026230, Weekday::Tue),
            Some(95026237)
        );
    }
}
