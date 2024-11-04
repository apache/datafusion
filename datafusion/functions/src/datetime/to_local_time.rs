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
use std::ops::Add;
use std::sync::{Arc, OnceLock};

use arrow::array::timezone::Tz;
use arrow::array::{Array, ArrayRef, PrimitiveBuilder};
use arrow::datatypes::DataType::Timestamp;
use arrow::datatypes::TimeUnit::{Microsecond, Millisecond, Nanosecond, Second};
use arrow::datatypes::{
    ArrowTimestampType, DataType, TimestampMicrosecondType, TimestampMillisecondType,
    TimestampNanosecondType, TimestampSecondType,
};

use chrono::{DateTime, MappedLocalTime, Offset, TimeDelta, TimeZone, Utc};
use datafusion_common::cast::as_primitive_array;
use datafusion_common::{exec_err, plan_err, DataFusionError, Result, ScalarValue};
use datafusion_expr::scalar_doc_sections::DOC_SECTION_DATETIME;
use datafusion_expr::{
    ColumnarValue, Documentation, ScalarUDFImpl, Signature, Volatility,
};

/// A UDF function that converts a timezone-aware timestamp to local time (with no offset or
/// timezone information). In other words, this function strips off the timezone from the timestamp,
/// while keep the display value of the timestamp the same.
#[derive(Debug)]
pub struct ToLocalTimeFunc {
    signature: Signature,
}

impl Default for ToLocalTimeFunc {
    fn default() -> Self {
        Self::new()
    }
}

impl ToLocalTimeFunc {
    pub fn new() -> Self {
        Self {
            signature: Signature::user_defined(Volatility::Immutable),
        }
    }

    fn to_local_time(&self, args: &[ColumnarValue]) -> Result<ColumnarValue> {
        if args.len() != 1 {
            return exec_err!(
                "to_local_time function requires 1 argument, got {}",
                args.len()
            );
        }

        let time_value = &args[0];
        let arg_type = time_value.data_type();
        match arg_type {
            Timestamp(_, None) => {
                // if no timezone specified, just return the input
                Ok(time_value.clone())
            }
            // If has timezone, adjust the underlying time value. The current time value
            // is stored as i64 in UTC, even though the timezone may not be in UTC. Therefore,
            // we need to adjust the time value to the local time. See [`adjust_to_local_time`]
            // for more details.
            //
            // Then remove the timezone in return type, i.e. return None
            Timestamp(_, Some(timezone)) => {
                let tz: Tz = timezone.parse()?;

                match time_value {
                    ColumnarValue::Scalar(ScalarValue::TimestampNanosecond(
                        Some(ts),
                        Some(_),
                    )) => {
                        let adjusted_ts =
                            adjust_to_local_time::<TimestampNanosecondType>(*ts, tz)?;
                        Ok(ColumnarValue::Scalar(ScalarValue::TimestampNanosecond(
                            Some(adjusted_ts),
                            None,
                        )))
                    }
                    ColumnarValue::Scalar(ScalarValue::TimestampMicrosecond(
                        Some(ts),
                        Some(_),
                    )) => {
                        let adjusted_ts =
                            adjust_to_local_time::<TimestampMicrosecondType>(*ts, tz)?;
                        Ok(ColumnarValue::Scalar(ScalarValue::TimestampMicrosecond(
                            Some(adjusted_ts),
                            None,
                        )))
                    }
                    ColumnarValue::Scalar(ScalarValue::TimestampMillisecond(
                        Some(ts),
                        Some(_),
                    )) => {
                        let adjusted_ts =
                            adjust_to_local_time::<TimestampMillisecondType>(*ts, tz)?;
                        Ok(ColumnarValue::Scalar(ScalarValue::TimestampMillisecond(
                            Some(adjusted_ts),
                            None,
                        )))
                    }
                    ColumnarValue::Scalar(ScalarValue::TimestampSecond(
                        Some(ts),
                        Some(_),
                    )) => {
                        let adjusted_ts =
                            adjust_to_local_time::<TimestampSecondType>(*ts, tz)?;
                        Ok(ColumnarValue::Scalar(ScalarValue::TimestampSecond(
                            Some(adjusted_ts),
                            None,
                        )))
                    }
                    ColumnarValue::Array(array) => {
                        fn transform_array<T: ArrowTimestampType>(
                            array: &ArrayRef,
                            tz: Tz,
                        ) -> Result<ColumnarValue> {
                            let mut builder = PrimitiveBuilder::<T>::new();

                            let primitive_array = as_primitive_array::<T>(array)?;
                            for ts_opt in primitive_array.iter() {
                                match ts_opt {
                                    None => builder.append_null(),
                                    Some(ts) => {
                                        let adjusted_ts: i64 =
                                            adjust_to_local_time::<T>(ts, tz)?;
                                        builder.append_value(adjusted_ts)
                                    }
                                }
                            }

                            Ok(ColumnarValue::Array(Arc::new(builder.finish())))
                        }

                        match array.data_type() {
                            Timestamp(_, None) => {
                                // if no timezone specified, just return the input
                                Ok(time_value.clone())
                            }
                            Timestamp(Nanosecond, Some(_)) => {
                                transform_array::<TimestampNanosecondType>(array, tz)
                            }
                            Timestamp(Microsecond, Some(_)) => {
                                transform_array::<TimestampMicrosecondType>(array, tz)
                            }
                            Timestamp(Millisecond, Some(_)) => {
                                transform_array::<TimestampMillisecondType>(array, tz)
                            }
                            Timestamp(Second, Some(_)) => {
                                transform_array::<TimestampSecondType>(array, tz)
                            }
                            _ => {
                                exec_err!("to_local_time function requires timestamp argument in array, got {:?}", array.data_type())
                            }
                        }
                    }
                    _ => {
                        exec_err!(
                        "to_local_time function requires timestamp argument, got {:?}",
                        time_value.data_type()
                    )
                    }
                }
            }
            _ => {
                exec_err!(
                    "to_local_time function requires timestamp argument, got {:?}",
                    arg_type
                )
            }
        }
    }
}

/// This function converts a timestamp with a timezone to a timestamp without a timezone.
/// The display value of the adjusted timestamp remain the same, but the underlying timestamp
/// representation is adjusted according to the relative timezone offset to UTC.
///
/// This function uses chrono to handle daylight saving time changes.
///
/// For example,
///
/// ```text
/// '2019-03-31T01:00:00Z'::timestamp at time zone 'Europe/Brussels'
/// ```
///
/// is displayed as follows in datafusion-cli:
///
/// ```text
/// 2019-03-31T01:00:00+01:00
/// ```
///
/// and is represented in DataFusion as:
///
/// ```text
/// TimestampNanosecond(Some(1_553_990_400_000_000_000), Some("Europe/Brussels"))
/// ```
///
/// To strip off the timezone while keeping the display value the same, we need to
/// adjust the underlying timestamp with the timezone offset value using `adjust_to_local_time()`
///
/// ```text
/// adjust_to_local_time(1_553_990_400_000_000_000, "Europe/Brussels") --> 1_553_994_000_000_000_000
/// ```
///
/// The difference between `1_553_990_400_000_000_000` and `1_553_994_000_000_000_000` is
/// `3600_000_000_000` ns, which corresponds to 1 hour. This matches with the timezone
/// offset for "Europe/Brussels" for this date.
///
/// Note that the offset varies with daylight savings time (DST), which makes this tricky! For
/// example, timezone "Europe/Brussels" has a 2-hour offset during DST and a 1-hour offset
/// when DST ends.
///
/// Consequently, DataFusion can represent the timestamp in local time (with no offset or
/// timezone information) as
///
/// ```text
/// TimestampNanosecond(Some(1_553_994_000_000_000_000), None)
/// ```
///
/// which is displayed as follows in datafusion-cli:
///
/// ```text
/// 2019-03-31T01:00:00
/// ```
///
/// See `test_adjust_to_local_time()` for example
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
        // This should not fail under normal circumstances as the
        // maximum possible offset is 26 hours (93,600 seconds)
        TimeDelta::try_seconds(offset_seconds)
            .ok_or(DataFusionError::Internal("Offset seconds should be less than i64::MAX / 1_000 or greater than -i64::MAX / 1_000".to_string()))?,
    );

    // convert the naive datetime back to i64
    match T::UNIT {
        Nanosecond => adjusted_date_time.timestamp_nanos_opt().ok_or(
            DataFusionError::Internal(
                "Failed to convert DateTime to timestamp in nanosecond. This error may occur if the date is out of range. The supported date ranges are between 1677-09-21T00:12:43.145224192 and 2262-04-11T23:47:16.854775807".to_string(),
            ),
        ),
        Microsecond => Ok(adjusted_date_time.timestamp_micros()),
        Millisecond => Ok(adjusted_date_time.timestamp_millis()),
        Second => Ok(adjusted_date_time.timestamp()),
    }
}

impl ScalarUDFImpl for ToLocalTimeFunc {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "to_local_time"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        if arg_types.len() != 1 {
            return exec_err!(
                "to_local_time function requires 1 argument, got {:?}",
                arg_types.len()
            );
        }

        match &arg_types[0] {
            Timestamp(timeunit, _) => Ok(Timestamp(*timeunit, None)),
            _ => exec_err!(
                "The to_local_time function can only accept timestamp as the arg, got {:?}", arg_types[0]
            )
        }
    }

    fn invoke(&self, args: &[ColumnarValue]) -> Result<ColumnarValue> {
        if args.len() != 1 {
            return exec_err!(
                "to_local_time function requires 1 argument, got {:?}",
                args.len()
            );
        }

        self.to_local_time(args)
    }

    fn coerce_types(&self, arg_types: &[DataType]) -> Result<Vec<DataType>> {
        if arg_types.len() != 1 {
            return plan_err!(
                "to_local_time function requires 1 argument, got {:?}",
                arg_types.len()
            );
        }

        let first_arg = arg_types[0].clone();
        match &first_arg {
            Timestamp(Nanosecond, timezone) => {
                Ok(vec![Timestamp(Nanosecond, timezone.clone())])
            }
            Timestamp(Microsecond, timezone) => {
                Ok(vec![Timestamp(Microsecond, timezone.clone())])
            }
            Timestamp(Millisecond, timezone) => {
                Ok(vec![Timestamp(Millisecond, timezone.clone())])
            }
            Timestamp(Second, timezone) => Ok(vec![Timestamp(Second, timezone.clone())]),
            _ => plan_err!("The to_local_time function can only accept Timestamp as the arg got {first_arg}"),
        }
    }
    fn documentation(&self) -> Option<&Documentation> {
        Some(get_to_local_time_doc())
    }
}

static DOCUMENTATION: OnceLock<Documentation> = OnceLock::new();

fn get_to_local_time_doc() -> &'static Documentation {
    DOCUMENTATION.get_or_init(|| {
        Documentation::builder()
            .with_doc_section(DOC_SECTION_DATETIME)
            .with_description("Converts a timestamp with a timezone to a timestamp without a timezone (with no offset or timezone information). This function handles daylight saving time changes.")
            .with_syntax_example("to_local_time(expression)")
            .with_argument(
                "expression",
                "Time expression to operate on. Can be a constant, column, or function."
            )
            .with_sql_example(r#"```sql
> SELECT to_local_time('2024-04-01T00:00:20Z'::timestamp);
+---------------------------------------------+
| to_local_time(Utf8("2024-04-01T00:00:20Z")) |
+---------------------------------------------+
| 2024-04-01T00:00:20                         |
+---------------------------------------------+

> SELECT to_local_time('2024-04-01T00:00:20Z'::timestamp AT TIME ZONE 'Europe/Brussels');
+---------------------------------------------+
| to_local_time(Utf8("2024-04-01T00:00:20Z")) |
+---------------------------------------------+
| 2024-04-01T00:00:20                         |
+---------------------------------------------+

> SELECT
  time,
  arrow_typeof(time) as type,
  to_local_time(time) as to_local_time,
  arrow_typeof(to_local_time(time)) as to_local_time_type
FROM (
  SELECT '2024-04-01T00:00:20Z'::timestamp AT TIME ZONE 'Europe/Brussels' AS time
);
+---------------------------+------------------------------------------------+---------------------+-----------------------------+
| time                      | type                                           | to_local_time       | to_local_time_type          |
+---------------------------+------------------------------------------------+---------------------+-----------------------------+
| 2024-04-01T00:00:20+02:00 | Timestamp(Nanosecond, Some("Europe/Brussels")) | 2024-04-01T00:00:20 | Timestamp(Nanosecond, None) |
+---------------------------+------------------------------------------------+---------------------+-----------------------------+

# combine `to_local_time()` with `date_bin()` to bin on boundaries in the timezone rather
# than UTC boundaries

> SELECT date_bin(interval '1 day', to_local_time('2024-04-01T00:00:20Z'::timestamp AT TIME ZONE 'Europe/Brussels')) AS date_bin;
+---------------------+
| date_bin            |
+---------------------+
| 2024-04-01T00:00:00 |
+---------------------+

> SELECT date_bin(interval '1 day', to_local_time('2024-04-01T00:00:20Z'::timestamp AT TIME ZONE 'Europe/Brussels')) AT TIME ZONE 'Europe/Brussels' AS date_bin_with_timezone;
+---------------------------+
| date_bin_with_timezone    |
+---------------------------+
| 2024-04-01T00:00:00+02:00 |
+---------------------------+
```"#)
            .build()
            .unwrap()
    })
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow::array::{types::TimestampNanosecondType, TimestampNanosecondArray};
    use arrow::compute::kernels::cast_utils::string_to_timestamp_nanos;
    use arrow::datatypes::{DataType, TimeUnit};
    use chrono::NaiveDateTime;
    use datafusion_common::ScalarValue;
    use datafusion_expr::{ColumnarValue, ScalarUDFImpl};

    use super::{adjust_to_local_time, ToLocalTimeFunc};

    #[test]
    fn test_adjust_to_local_time() {
        let timestamp_str = "2020-03-31T13:40:00";
        let tz: arrow::array::timezone::Tz =
            "America/New_York".parse().expect("Invalid timezone");

        let timestamp = timestamp_str
            .parse::<NaiveDateTime>()
            .unwrap()
            .and_local_timezone(tz) // this is in a local timezone
            .unwrap()
            .timestamp_nanos_opt()
            .unwrap();

        let expected_timestamp = timestamp_str
            .parse::<NaiveDateTime>()
            .unwrap()
            .and_utc() // this is in UTC
            .timestamp_nanos_opt()
            .unwrap();

        let res = adjust_to_local_time::<TimestampNanosecondType>(timestamp, tz).unwrap();
        assert_eq!(res, expected_timestamp);
    }

    #[test]
    fn test_to_local_time_scalar() {
        let timezone = Some("Europe/Brussels".into());
        let timestamps_with_timezone = vec![
            (
                ScalarValue::TimestampNanosecond(
                    Some(1_123_123_000_000_000_000),
                    timezone.clone(),
                ),
                ScalarValue::TimestampNanosecond(Some(1_123_130_200_000_000_000), None),
            ),
            (
                ScalarValue::TimestampMicrosecond(
                    Some(1_123_123_000_000_000),
                    timezone.clone(),
                ),
                ScalarValue::TimestampMicrosecond(Some(1_123_130_200_000_000), None),
            ),
            (
                ScalarValue::TimestampMillisecond(
                    Some(1_123_123_000_000),
                    timezone.clone(),
                ),
                ScalarValue::TimestampMillisecond(Some(1_123_130_200_000), None),
            ),
            (
                ScalarValue::TimestampSecond(Some(1_123_123_000), timezone),
                ScalarValue::TimestampSecond(Some(1_123_130_200), None),
            ),
        ];

        for (input, expected) in timestamps_with_timezone {
            test_to_local_time_helper(input, expected);
        }
    }

    #[test]
    fn test_timezone_with_daylight_savings() {
        let timezone_str = "America/New_York";
        let tz: arrow::array::timezone::Tz =
            timezone_str.parse().expect("Invalid timezone");

        // Test data:
        // (
        //    the string display of the input timestamp,
        //    the i64 representation of the timestamp before adjustment in nanosecond,
        //    the i64 representation of the timestamp after adjustment in nanosecond,
        // )
        let test_cases = vec![
            (
                // DST time
                "2020-03-31T13:40:00",
                1_585_676_400_000_000_000,
                1_585_662_000_000_000_000,
            ),
            (
                // End of DST
                "2020-11-04T14:06:40",
                1_604_516_800_000_000_000,
                1_604_498_800_000_000_000,
            ),
        ];

        for (
            input_timestamp_str,
            expected_input_timestamp,
            expected_adjusted_timestamp,
        ) in test_cases
        {
            let input_timestamp = input_timestamp_str
                .parse::<NaiveDateTime>()
                .unwrap()
                .and_local_timezone(tz) // this is in a local timezone
                .unwrap()
                .timestamp_nanos_opt()
                .unwrap();
            assert_eq!(input_timestamp, expected_input_timestamp);

            let expected_timestamp = input_timestamp_str
                .parse::<NaiveDateTime>()
                .unwrap()
                .and_utc() // this is in UTC
                .timestamp_nanos_opt()
                .unwrap();
            assert_eq!(expected_timestamp, expected_adjusted_timestamp);

            let input = ScalarValue::TimestampNanosecond(
                Some(input_timestamp),
                Some(timezone_str.into()),
            );
            let expected =
                ScalarValue::TimestampNanosecond(Some(expected_timestamp), None);
            test_to_local_time_helper(input, expected)
        }
    }

    fn test_to_local_time_helper(input: ScalarValue, expected: ScalarValue) {
        let res = ToLocalTimeFunc::new()
            .invoke(&[ColumnarValue::Scalar(input)])
            .unwrap();
        match res {
            ColumnarValue::Scalar(res) => {
                assert_eq!(res, expected);
            }
            _ => panic!("unexpected return type"),
        }
    }

    #[test]
    fn test_to_local_time_timezones_array() {
        let cases = [
            (
                vec![
                    "2020-09-08T00:00:00",
                    "2020-09-08T01:00:00",
                    "2020-09-08T02:00:00",
                    "2020-09-08T03:00:00",
                    "2020-09-08T04:00:00",
                ],
                None::<Arc<str>>,
                vec![
                    "2020-09-08T00:00:00",
                    "2020-09-08T01:00:00",
                    "2020-09-08T02:00:00",
                    "2020-09-08T03:00:00",
                    "2020-09-08T04:00:00",
                ],
            ),
            (
                vec![
                    "2020-09-08T00:00:00",
                    "2020-09-08T01:00:00",
                    "2020-09-08T02:00:00",
                    "2020-09-08T03:00:00",
                    "2020-09-08T04:00:00",
                ],
                Some("+01:00".into()),
                vec![
                    "2020-09-08T00:00:00",
                    "2020-09-08T01:00:00",
                    "2020-09-08T02:00:00",
                    "2020-09-08T03:00:00",
                    "2020-09-08T04:00:00",
                ],
            ),
        ];

        cases.iter().for_each(|(source, _tz_opt, expected)| {
            let input = source
                .iter()
                .map(|s| Some(string_to_timestamp_nanos(s).unwrap()))
                .collect::<TimestampNanosecondArray>();
            let right = expected
                .iter()
                .map(|s| Some(string_to_timestamp_nanos(s).unwrap()))
                .collect::<TimestampNanosecondArray>();
            let result = ToLocalTimeFunc::new()
                .invoke(&[ColumnarValue::Array(Arc::new(input))])
                .unwrap();
            if let ColumnarValue::Array(result) = result {
                assert_eq!(
                    result.data_type(),
                    &DataType::Timestamp(TimeUnit::Nanosecond, None)
                );
                let left = arrow::array::cast::as_primitive_array::<
                    TimestampNanosecondType,
                >(&result);
                assert_eq!(left, &right);
            } else {
                panic!("unexpected column type");
            }
        });
    }
}
