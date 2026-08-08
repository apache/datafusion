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

use arrow::array::{ArrayRef, AsArray, Date32Array, StringArrayType};
use arrow::datatypes::{DataType, Date32Type, Field, FieldRef};
use chrono::{Datelike, Duration, Weekday};
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
        let ScalarFunctionArgs { args, .. } = args;
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
                        if let Some(days) = days {
                            if let Some(day_of_week) = day_of_week {
                                Ok(ColumnarValue::Scalar(ScalarValue::Date32(
                                    parse_day_of_week(day_of_week)
                                        .and_then(|dow| spark_next_day(*days, dow)),
                                )))
                            } else {
                                // TODO: if spark.sql.ansi.enabled is false,
                                //  returns NULL instead of an error for a malformed dayOfWeek.
                                Ok(ColumnarValue::Scalar(ScalarValue::Date32(None)))
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
                        if let Some(day_of_week) = day_of_week {
                            // The day name is the same for every row, so parse it
                            // once here rather than on each call below.
                            let Some(day_of_week) = parse_day_of_week(day_of_week) else {
                                return Ok(ColumnarValue::Scalar(ScalarValue::Date32(
                                    None,
                                )));
                            };
                            let result: Date32Array = date_array
                                .as_primitive::<Date32Type>()
                                .unary_opt(|days| spark_next_day(days, day_of_week))
                                .with_data_type(DataType::Date32);
                            Ok(ColumnarValue::Array(Arc::new(result) as ArrayRef))
                        } else {
                            // TODO: if spark.sql.ansi.enabled is false,
                            //  returns NULL instead of an error for a malformed dayOfWeek.
                            Ok(ColumnarValue::Scalar(ScalarValue::Date32(None)))
                        }
                    }
                    _ => exec_err!(
                        "Spark `next_day` function: first arg must be date, second arg must be string. Got {args:?}"
                    ),
                }
            }
            (
                ColumnarValue::Array(date_array),
                ColumnarValue::Array(day_of_week_array),
            ) => {
                let result = match (date_array.data_type(), day_of_week_array.data_type())
                {
                    (
                        DataType::Date32,
                        DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View,
                    ) => {
                        let date_array: &Date32Array =
                            date_array.as_primitive::<Date32Type>();
                        match day_of_week_array.data_type() {
                            DataType::Utf8 => {
                                let day_of_week_array =
                                    day_of_week_array.as_string::<i32>();
                                process_next_day_arrays(date_array, day_of_week_array)
                            }
                            DataType::LargeUtf8 => {
                                let day_of_week_array =
                                    day_of_week_array.as_string::<i64>();
                                process_next_day_arrays(date_array, day_of_week_array)
                            }
                            DataType::Utf8View => {
                                let day_of_week_array =
                                    day_of_week_array.as_string_view();
                                process_next_day_arrays(date_array, day_of_week_array)
                            }
                            other => {
                                exec_err!(
                                    "Spark `next_day` function: second arg must be string. Got {other:?}"
                                )
                            }
                        }
                    }
                    (left, right) => {
                        exec_err!(
                            "Spark `next_day` function: first arg must be date, second arg must be string. Got {left:?}, {right:?}"
                        )
                    }
                }?;
                Ok(ColumnarValue::Array(result))
            }
            _ => exec_err!("Unsupported args {args:?} for Spark function `next_day`"),
        }
    }
}

fn process_next_day_arrays<'a, S>(
    date_array: &Date32Array,
    day_of_week_array: &'a S,
) -> Result<ArrayRef>
where
    &'a S: StringArrayType<'a>,
{
    let result = date_array
        .iter()
        .zip(day_of_week_array.iter())
        .map(|(days, day_of_week)| {
            if let Some(days) = days {
                if let Some(day_of_week) = day_of_week {
                    parse_day_of_week(day_of_week)
                        .and_then(|dow| spark_next_day(days, dow))
                } else {
                    // TODO: if spark.sql.ansi.enabled is false,
                    //  returns NULL instead of an error for a malformed dayOfWeek.
                    None
                }
            } else {
                None
            }
        })
        .collect::<Date32Array>();
    Ok(Arc::new(result) as ArrayRef)
}

/// Longest day name accepted by Spark, `"WEDNESDAY"`.
const MAX_DAY_NAME_LEN: usize = 9;

/// Maps an already-upper-cased day name to its [`Weekday`].
fn weekday_from_upper(name: &[u8]) -> Option<Weekday> {
    match name {
        b"MO" | b"MON" | b"MONDAY" => Some(Weekday::Mon),
        b"TU" | b"TUE" | b"TUESDAY" => Some(Weekday::Tue),
        b"WE" | b"WED" | b"WEDNESDAY" => Some(Weekday::Wed),
        b"TH" | b"THU" | b"THURSDAY" => Some(Weekday::Thu),
        b"FR" | b"FRI" | b"FRIDAY" => Some(Weekday::Fri),
        b"SA" | b"SAT" | b"SATURDAY" => Some(Weekday::Sat),
        b"SU" | b"SUN" | b"SUNDAY" => Some(Weekday::Sun),
        // TODO: if spark.sql.ansi.enabled is false,
        //  returns NULL instead of an error for a malformed dayOfWeek.
        _ => None,
    }
}

/// Parses a Spark day-of-week name, without allocating for ASCII input.
fn parse_day_of_week(day_of_week: &str) -> Option<Weekday> {
    let bytes = day_of_week.as_bytes();
    if day_of_week.is_ascii() {
        // No accepted name is longer than `MAX_DAY_NAME_LEN`, so anything longer
        // cannot match and the upper-casing fits in a stack buffer.
        if bytes.len() > MAX_DAY_NAME_LEN {
            return None;
        }
        let mut buf = [0u8; MAX_DAY_NAME_LEN];
        let buf = &mut buf[..bytes.len()];
        buf.copy_from_slice(bytes);
        buf.make_ascii_uppercase();
        weekday_from_upper(buf)
    } else {
        // `str::to_uppercase` is Unicode-aware, matching Spark's
        // `toUpperCase(Locale.ROOT)`. Handling non-ASCII input on the branch
        // above would change which strings match, so it keeps the allocation.
        weekday_from_upper(day_of_week.to_uppercase().as_bytes())
    }
}

fn spark_next_day(days: i32, day_of_week: Weekday) -> Option<i32> {
    let date = Date32Type::to_naive_date_opt(days)?;

    Some(Date32Type::from_naive_date(
        date + Duration::days((7 - date.weekday().days_since(day_of_week)) as i64),
    ))
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
    }

    #[test]
    fn parse_day_of_week_is_case_insensitive() {
        for (input, expected) in [
            ("mon", Weekday::Mon),
            ("MONDAY", Weekday::Mon),
            ("Tu", Weekday::Tue),
            ("wEdNeSdAy", Weekday::Wed),
            ("THU", Weekday::Thu),
            ("fri", Weekday::Fri),
            ("Sat", Weekday::Sat),
            ("su", Weekday::Sun),
        ] {
            assert_eq!(parse_day_of_week(input), Some(expected), "input: {input}");
        }
    }

    #[test]
    fn parse_day_of_week_rejects_unknown_names() {
        for input in ["", "M", "MOND", "WEDNESDAYS", "funday", "🙂"] {
            assert_eq!(parse_day_of_week(input), None, "input: {input}");
        }
    }

    /// The ASCII fast path must accept exactly what the Unicode-aware
    /// `to_uppercase` accepts, since Spark upper-cases with `Locale.ROOT`.
    #[test]
    fn parse_day_of_week_matches_unicode_uppercasing() {
        // U+017F LATIN SMALL LETTER LONG S upper-cases to 'S'.
        assert_eq!(parse_day_of_week("\u{17f}un"), Some(Weekday::Sun));
        assert_eq!(parse_day_of_week("\u{17f}unday"), Some(Weekday::Sun));
    }
}
