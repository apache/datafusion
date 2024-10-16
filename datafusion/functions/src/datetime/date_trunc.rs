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
use std::ops::{Add, Sub};
use std::str::FromStr;
use std::sync::{Arc, OnceLock};

use arrow::array::temporal_conversions::{
    as_datetime_with_timezone, timestamp_ns_to_datetime,
};
use arrow::array::timezone::Tz;
use arrow::array::types::{
    ArrowTimestampType, TimestampMicrosecondType, TimestampMillisecondType,
    TimestampNanosecondType, TimestampSecondType,
};
use arrow::array::{Array, PrimitiveArray};
use arrow::datatypes::DataType::{self, Null, Timestamp, Utf8, Utf8View};
use arrow::datatypes::TimeUnit::{self, Microsecond, Millisecond, Nanosecond, Second};
use datafusion_common::cast::as_primitive_array;
use datafusion_common::{exec_err, plan_err, DataFusionError, Result, ScalarValue};
use datafusion_expr::sort_properties::{ExprProperties, SortProperties};
use datafusion_expr::TypeSignature::Exact;
use datafusion_expr::{
    ColumnarValue, Documentation, ScalarUDFImpl, Signature, Volatility, TIMEZONE_WILDCARD,
};

use chrono::{
    DateTime, Datelike, Duration, LocalResult, NaiveDateTime, Offset, TimeDelta, Timelike,
};
use datafusion_expr::scalar_doc_sections::DOC_SECTION_DATETIME;

#[derive(Debug)]
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
        match &arg_types[1] {
            Timestamp(Nanosecond, None) | Utf8 | DataType::Date32 | Null => {
                Ok(Timestamp(Nanosecond, None))
            }
            Timestamp(Nanosecond, tz_opt) => Ok(Timestamp(Nanosecond, tz_opt.clone())),
            Timestamp(Microsecond, tz_opt) => Ok(Timestamp(Microsecond, tz_opt.clone())),
            Timestamp(Millisecond, tz_opt) => Ok(Timestamp(Millisecond, tz_opt.clone())),
            Timestamp(Second, tz_opt) => Ok(Timestamp(Second, tz_opt.clone())),
            _ => plan_err!(
                "The date_trunc function can only accept timestamp as the second arg."
            ),
        }
    }

    fn invoke(&self, args: &[ColumnarValue]) -> Result<ColumnarValue> {
        let (granularity, array) = (&args[0], &args[1]);

        let granularity = if let ColumnarValue::Scalar(ScalarValue::Utf8(Some(v))) =
            granularity
        {
            v.to_lowercase()
        } else if let ColumnarValue::Scalar(ScalarValue::Utf8View(Some(v))) = granularity
        {
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
                if let Timestamp(unit, tz_opt) = array_type {
                    match unit {
                        Second => process_array::<TimestampSecondType>(
                            array,
                            granularity,
                            tz_opt,
                        )?,
                        Millisecond => process_array::<TimestampMillisecondType>(
                            array,
                            granularity,
                            tz_opt,
                        )?,
                        Microsecond => process_array::<TimestampMicrosecondType>(
                            array,
                            granularity,
                            tz_opt,
                        )?,
                        Nanosecond => process_array::<TimestampNanosecondType>(
                            array,
                            granularity,
                            tz_opt,
                        )?,
                    }
                } else {
                    return exec_err!("second argument of `date_trunc` is an unsupported array type: {array_type}");
                }
            }
            _ => {
                return exec_err!(
                    "second argument of `date_trunc` must be timestamp scalar or array"
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
        Some(get_date_trunc_doc())
    }
}

static DOCUMENTATION: OnceLock<Documentation> = OnceLock::new();

fn get_date_trunc_doc() -> &'static Documentation {
    DOCUMENTATION.get_or_init(|| {
        Documentation::builder()
            .with_doc_section(DOC_SECTION_DATETIME)
            .with_description("Truncates a timestamp value to a specified precision.")
            .with_syntax_example("date_trunc(precision, expression)")
            .with_argument(
                "precision",
                r#"Time precision to truncate to. The following precisions are supported:

    - year / YEAR
    - quarter / QUARTER
    - month / MONTH
    - week / WEEK
    - day / DAY
    - hour / HOUR
    - minute / MINUTE
    - second / SECOND
"#,
            )
            .with_argument(
                "expression",
                "Time expression to operate on. Can be a constant, column, or function.",
            )
            .build()
            .unwrap()
    })
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
            .map(|d| {
                d - TimeDelta::try_seconds(60 * 60 * 24 * d.weekday() as i64).unwrap()
            }),
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

fn quarter_month<T>(date: &T) -> u32
where
    T: Datelike,
{
    1 + 3 * ((date.month() - 1) / 3)
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
    granularity: &str,
    value: Option<NaiveDateTime>,
) -> Result<Option<i64>> {
    let value = _date_trunc_coarse::<NaiveDateTime>(granularity, value)?;
    Ok(value.and_then(|value| value.and_utc().timestamp_nanos_opt()))
}

/// Truncates the single `value`, expressed in nanoseconds since the
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
        Second => 1_000_000_000,
        Millisecond => 1_000_000,
        Microsecond => 1_000,
        Nanosecond => 1,
    };

    let Some(value) = value else {
        return Ok(None);
    };

    // convert to nanoseconds
    let nano = date_trunc_coarse(granularity, scale * value, tz)?;

    let result = match tu {
        Second => match granularity {
            "minute" => Some(nano / 1_000_000_000 / 60 * 60),
            _ => Some(nano / 1_000_000_000),
        },
        Millisecond => match granularity {
            "minute" => Some(nano / 1_000_000 / 1_000 / 60 * 1_000 * 60),
            "second" => Some(nano / 1_000_000 / 1_000 * 1_000),
            _ => Some(nano / 1_000_000),
        },
        Microsecond => match granularity {
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

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use crate::datetime::date_trunc::{date_trunc_coarse, DateTruncFunc};

    use arrow::array::cast::as_primitive_array;
    use arrow::array::types::TimestampNanosecondType;
    use arrow::array::TimestampNanosecondArray;
    use arrow::compute::kernels::cast_utils::string_to_timestamp_nanos;
    use arrow::datatypes::{DataType, TimeUnit};
    use datafusion_common::ScalarValue;
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
            let result = DateTruncFunc::new()
                .invoke(&[
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
            let result = DateTruncFunc::new()
                .invoke(&[
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
}
