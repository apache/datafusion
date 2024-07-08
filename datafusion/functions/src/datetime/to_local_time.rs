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
use std::sync::Arc;

use arrow::array::timezone::Tz;
use arrow::array::{ArrayRef, PrimitiveArray};
use arrow::datatypes::DataType::Timestamp;
use arrow::datatypes::{
    ArrowTimestampType, DataType, TimestampMicrosecondType, TimestampMillisecondType,
    TimestampNanosecondType, TimestampSecondType,
};
use arrow::datatypes::{
    TimeUnit,
    TimeUnit::{Microsecond, Millisecond, Nanosecond, Second},
};

use chrono::{Offset, TimeDelta, TimeZone, Utc};
use datafusion_common::cast::as_primitive_array;
use datafusion_common::{exec_err, Result, ScalarValue};
use datafusion_expr::TypeSignature::Exact;
use datafusion_expr::{
    ColumnarValue, ScalarUDFImpl, Signature, Volatility, TIMEZONE_WILDCARD,
};

/// A UDF function that converts a timezone-aware timestamp to local time (with no offset or
/// timezone information). In other words, this function strips off the timezone from the timestamp,
/// while keep the display value of the timestamp the same.
///
/// # Example
///
/// ```
/// # use datafusion_common::ScalarValue;
/// # use datafusion_expr::ColumnarValue;
///
/// // 2019-03-31 01:00:00 +01:00
/// let res = ToLocalTimeFunc::new()
/// .invoke(&[ColumnarValue::Scalar(ScalarValue::TimestampSecond(
///     Some(1_553_990_400),
///     Some("Europe/Brussels".into()),
/// ))])
/// .unwrap();
///
/// // 2019-03-31 01:00:00 <-- this timestamp no longer has +01:00 offset
/// let expected = ScalarValue::TimestampSecond(Some(1_553_994_000), None);
///
/// match res {
/// ColumnarValue::Scalar(res) => {
///     assert_eq!(res, expected);
/// }
/// _ => panic!("unexpected return type"),
/// }
/// ```

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
        let base_sig = |array_type: TimeUnit| {
            vec![
                Exact(vec![Timestamp(array_type, None)]),
                Exact(vec![Timestamp(array_type, Some(TIMEZONE_WILDCARD.into()))]),
            ]
        };

        let full_sig = [Nanosecond, Microsecond, Millisecond, Second]
            .into_iter()
            .map(base_sig)
            .collect::<Vec<_>>()
            .concat();

        Self {
            signature: Signature::one_of(full_sig, Volatility::Immutable),
        }
    }

    fn to_local_time(&self, args: &[ColumnarValue]) -> Result<ColumnarValue> {
        if args.len() != 1 {
            return exec_err!(
                "to_local_time function requires 1 argument, got {}",
                args.len()
            );
        }

        let time_value = args[0].clone();
        let arg_type = time_value.data_type();
        match arg_type {
            DataType::Timestamp(_, None) => {
                // if no timezone specificed, just return the input
                Ok(time_value.clone())
            }
            // if has timezone, adjust the underlying time value. the current time value
            // is stored as i64 in UTC, even though the timezone may not be in UTC, so
            // we need to adjust the time value to the local time. see [`adjust_to_local_time`]
            // for more details.
            //
            // Then remove the timezone in return type, i.e. return None
            DataType::Timestamp(_, Some(_)) => match time_value {
                ColumnarValue::Scalar(ScalarValue::TimestampNanosecond(
                    Some(ts),
                    Some(tz),
                )) => {
                    let adjusted_ts =
                        adjust_to_local_time::<TimestampNanosecondType>(ts, &tz);
                    Ok(ColumnarValue::Scalar(ScalarValue::TimestampNanosecond(
                        Some(adjusted_ts),
                        None,
                    )))
                }
                ColumnarValue::Scalar(ScalarValue::TimestampMicrosecond(
                    Some(ts),
                    Some(tz),
                )) => {
                    let adjusted_ts =
                        adjust_to_local_time::<TimestampMicrosecondType>(ts, &tz);
                    Ok(ColumnarValue::Scalar(ScalarValue::TimestampMicrosecond(
                        Some(adjusted_ts),
                        None,
                    )))
                }
                ColumnarValue::Scalar(ScalarValue::TimestampMillisecond(
                    Some(ts),
                    Some(tz),
                )) => {
                    let adjusted_ts =
                        adjust_to_local_time::<TimestampMillisecondType>(ts, &tz);
                    Ok(ColumnarValue::Scalar(ScalarValue::TimestampMillisecond(
                        Some(adjusted_ts),
                        None,
                    )))
                }
                ColumnarValue::Scalar(ScalarValue::TimestampSecond(
                    Some(ts),
                    Some(tz),
                )) => {
                    let adjusted_ts =
                        adjust_to_local_time::<TimestampSecondType>(ts, &tz);
                    Ok(ColumnarValue::Scalar(ScalarValue::TimestampSecond(
                        Some(adjusted_ts),
                        None,
                    )))
                }
                ColumnarValue::Array(array) => {
                    fn transform_array<T>(
                        array: &ArrayRef,
                        tz: &str,
                    ) -> Result<ColumnarValue>
                    where
                        T: ArrowTimestampType,
                    {
                        let array = as_primitive_array::<T>(array)?;
                        let array: PrimitiveArray<T> =
                            array.unary(|ts| adjust_to_local_time::<T>(ts, tz));

                        Ok(ColumnarValue::Array(Arc::new(array)))
                    }

                    match array.data_type() {
                        Timestamp(_, None) => {
                            // if no timezone specificed, just return the input
                            Ok(ColumnarValue::Array(Arc::new(array)))
                        }
                        Timestamp(Nanosecond, Some(tz)) => {
                            transform_array::<TimestampNanosecondType>(&array, tz)
                        }
                        Timestamp(Microsecond, Some(tz)) => {
                            transform_array::<TimestampMicrosecondType>(&array, tz)
                        }
                        Timestamp(Millisecond, Some(tz)) => {
                            transform_array::<TimestampMillisecondType>(&array, tz)
                        }
                        Timestamp(Second, Some(tz)) => {
                            transform_array::<TimestampSecondType>(&array, tz)
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
            },
            _ => {
                exec_err!(
                    "to_local_time function requires timestamp argument, got {:?}",
                    arg_type
                )
            }
        }
    }
}

// TODO chunchun: add description
fn adjust_to_local_time<T: ArrowTimestampType>(ts: i64, timezone: &str) -> i64 {
    let date_time = match T::UNIT {
        Nanosecond => Utc.timestamp_nanos(ts),
        Microsecond => Utc.timestamp_micros(ts).unwrap(), // TODO chunchun: replace unwrap
        Millisecond => Utc.timestamp_millis_opt(ts).unwrap(),
        Second => Utc.timestamp_opt(ts, 0).unwrap(),
    };

    let tz: Tz = timezone.parse().unwrap();
    let offset_seconds: i64 = tz
        .offset_from_utc_datetime(&date_time.naive_utc())
        .fix()
        .local_minus_utc() as i64;

    let adjusted_date_time =
        date_time.add(TimeDelta::try_seconds(offset_seconds).unwrap());

    // convert the navie datetime back to i64
    match T::UNIT {
        Nanosecond => adjusted_date_time.timestamp_nanos_opt().unwrap(), // TODO chuynchun: remove unwrap
        Microsecond => adjusted_date_time.timestamp_micros(),
        Millisecond => adjusted_date_time.timestamp_millis(),
        Second => adjusted_date_time.timestamp(),
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
            Timestamp(Nanosecond, _) => Ok(Timestamp(Nanosecond, None)),
            Timestamp(Microsecond, _) => Ok(Timestamp(Microsecond, None)),
            Timestamp(Millisecond, _) => Ok(Timestamp(Millisecond, None)),
            Timestamp(Second, _) => Ok(Timestamp(Second, None)),
            _ => exec_err!(
                "The to_local_time function can only accept timestamp as the arg, got {:?}", arg_types[0]
            ),
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
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow::array::{types::TimestampNanosecondType, TimestampNanosecondArray};
    use arrow::compute::kernels::cast_utils::string_to_timestamp_nanos;
    use arrow::datatypes::{DataType, TimeUnit};
    use chrono::{TimeZone, Utc};
    use datafusion_common::ScalarValue;
    use datafusion_expr::{ColumnarValue, ScalarUDFImpl};

    use super::ToLocalTimeFunc;

    #[test]
    fn test_to_local_time() {
        let res = ToLocalTimeFunc::new().invoke(&[ColumnarValue::Scalar(
            ScalarValue::TimestampSecond(Some(1), None),
        )]);
        assert!(res.is_ok());

        let res = ToLocalTimeFunc::new().invoke(&[ColumnarValue::Scalar(
            ScalarValue::TimestampSecond(Some(1), Some("+01:00".into())),
        )]);
        assert!(res.is_ok());

        let res = ToLocalTimeFunc::new().invoke(&[ColumnarValue::Scalar(
            ScalarValue::TimestampNanosecond(Some(1), Some("Europe/Brussels".into())),
        )]);
        assert!(res.is_ok());

        //
        // Fallible test cases
        //

        // invalid number of arguments -- no argument
        let res = ToLocalTimeFunc::new().invoke(&[]);
        assert_eq!(
            res.err().unwrap().strip_backtrace(),
            "Execution error: to_local_time function requires 1 argument, got 0"
        );

        // invalid number of arguments -- more than 1 argument
        let res = ToLocalTimeFunc::new().invoke(&[
            ColumnarValue::Scalar(ScalarValue::TimestampSecond(Some(1), None)),
            ColumnarValue::Scalar(ScalarValue::Utf8(Some("some string".to_string()))),
        ]);
        assert_eq!(
            res.err().unwrap().strip_backtrace(),
            "Execution error: to_local_time function requires 1 argument, got 2"
        );

        // invalid argument data type
        let res = ToLocalTimeFunc::new().invoke(&[ColumnarValue::Scalar(
            ScalarValue::Utf8(Some("some string".to_string())),
        )]);
        assert_eq!(
            res.err().unwrap().strip_backtrace(),
            "Execution error: to_local_time function requires timestamp argument, got Utf8"
        );

        // TODO chunchun: invalid timestamp: us, ms, s
    }

    #[test]
    fn test_to_local_time_scalar() {
        let timestamps_without_timezone = vec![
            (
                ScalarValue::TimestampNanosecond(Some(1_123_123_000_000_000_000), None),
                ScalarValue::TimestampNanosecond(Some(1_123_123_000_000_000_000), None),
            ),
            (
                ScalarValue::TimestampMicrosecond(Some(1_123_123_000_000_000), None),
                ScalarValue::TimestampMicrosecond(Some(1_123_123_000_000_000), None),
            ),
            (
                ScalarValue::TimestampMillisecond(Some(1_123_123_000_000), None),
                ScalarValue::TimestampMillisecond(Some(1_123_123_000_000), None),
            ),
            (
                ScalarValue::TimestampSecond(Some(1_123_123_000), None),
                ScalarValue::TimestampSecond(Some(1_123_123_000), None),
            ),
        ];

        for (input, expected) in timestamps_without_timezone {
            test_to_local_time_helpfer(input, expected);
        }

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
            test_to_local_time_helpfer(input, expected);
        }
    }

    #[test]
    fn test_timezone_with_daylight_savings() {
        let timezone_str = "America/New_York";
        let tz: arrow::array::timezone::Tz =
            timezone_str.parse().expect("Invalid timezone");

        // Test data:
        // (
        //    input timestamp in second,
        //    the string display of the input timestamp,
        //    the expected timestamp after applied `to_local_time`,
        //    the string display of the expected timestamp,
        // )
        let test_cases = vec![
            (
                // DST time
                1_585_676_400,
                "2020-03-31 13:40:00 -04:00",
                1_585_662_000,
                "2020-03-31 13:40:00 UTC",
            ),
            (
                // End of DST
                1_604_516_800,
                "2020-11-04 14:06:40 -05:00",
                1_604_498_800,
                "2020-11-04 14:06:40 UTC",
            ),
        ];

        for (
            input_timestamp_s,
            expected_input_timestamp_display,
            expected_adjusted_timstamp,
            expected_adjusted_timstamp_display,
        ) in test_cases
        {
            let input_timestamp_display = Utc
                .timestamp_opt(input_timestamp_s, 0)
                .unwrap()
                .with_timezone(&tz)
                .to_string();
            assert_eq!(input_timestamp_display, expected_input_timestamp_display);

            let dst_timestamp = ScalarValue::TimestampSecond(
                Some(input_timestamp_s),
                Some(timezone_str.into()),
            );
            let expected_dst_timestamp =
                ScalarValue::TimestampSecond(Some(expected_adjusted_timstamp), None);
            test_to_local_time_helpfer(dst_timestamp, expected_dst_timestamp);

            let adjusted_timstamp_display = Utc
                .timestamp_opt(expected_adjusted_timstamp, 0)
                .unwrap()
                .to_string();
            assert_eq!(
                adjusted_timstamp_display,
                expected_adjusted_timstamp_display
            )
        }
    }

    fn test_to_local_time_helpfer(input: ScalarValue, expected: ScalarValue) {
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
        // TODO chunchun: might need to change string to Timestamp in cases
        let cases = [
            (
                vec![
                    "2020-09-08T00:00:00Z",
                    "2020-09-08T01:00:00Z",
                    "2020-09-08T02:00:00Z",
                    "2020-09-08T03:00:00Z",
                    "2020-09-08T04:00:00Z",
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
