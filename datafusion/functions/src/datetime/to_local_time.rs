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

use chrono::{DateTime, Offset, TimeDelta, TimeZone};
use datafusion_common::cast::as_primitive_array;
use datafusion_common::{exec_err, Result, ScalarValue};
use datafusion_expr::TypeSignature::Exact;
use datafusion_expr::{
    ColumnarValue, ScalarUDFImpl, Signature, Volatility, TIMEZONE_WILDCARD,
};

#[derive(Debug)]
pub struct ToLocalTimeFunc {
    signature: Signature,
}

impl Default for ToLocalTimeFunc {
    fn default() -> Self {
        Self::new()
    }
}

// TODO chunchun: add description, with example
// strip timezone
impl ToLocalTimeFunc {
    pub fn new() -> Self {
        let base_sig = |array_type: TimeUnit| {
            vec![
                Exact(vec![Timestamp(array_type.clone(), None)]),
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
                    let adjusted_ts = adjust_to_local_time(ts, &tz);
                    Ok(ColumnarValue::Scalar(ScalarValue::TimestampNanosecond(
                        Some(adjusted_ts),
                        None,
                    )))
                }
                ColumnarValue::Scalar(ScalarValue::TimestampMicrosecond(
                    Some(ts),
                    Some(tz),
                )) => {
                    let adjusted_ts = adjust_to_local_time(ts, &tz);
                    Ok(ColumnarValue::Scalar(ScalarValue::TimestampMicrosecond(
                        Some(adjusted_ts),
                        None,
                    )))
                }
                ColumnarValue::Scalar(ScalarValue::TimestampMillisecond(
                    Some(ts),
                    Some(tz),
                )) => {
                    let adjusted_ts = adjust_to_local_time(ts, &tz);
                    Ok(ColumnarValue::Scalar(ScalarValue::TimestampMillisecond(
                        Some(adjusted_ts),
                        None,
                    )))
                }
                ColumnarValue::Scalar(ScalarValue::TimestampSecond(
                    Some(ts),
                    Some(tz),
                )) => {
                    let adjusted_ts = adjust_to_local_time(ts, &tz);
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
                            array.unary(|ts| adjust_to_local_time(ts, tz));

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
fn adjust_to_local_time(ts: i64, timezone: &str) -> i64 {
    let tz: Tz = timezone.parse().unwrap();

    let date_time = DateTime::from_timestamp_nanos(ts).naive_utc();

    let offset_seconds: i64 = tz
        .offset_from_utc_datetime(&date_time)
        .fix()
        .local_minus_utc() as i64;

    let adjusted_date_time =
        date_time.add(TimeDelta::try_seconds(offset_seconds).unwrap());

    // convert the navie datetime into i64
    adjusted_date_time.and_utc().timestamp_nanos_opt().unwrap()
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
    }

    #[test]
    fn test_to_local_time_timezones_scalar() {
        // TODO chunchun: this is wrong
        let expect = ScalarValue::TimestampSecond(Some(3_602_000_000_000), None);
        let res = ToLocalTimeFunc::new()
            .invoke(&[ColumnarValue::Scalar(ScalarValue::TimestampSecond(
                Some(2_000_000_000),            // 2033-05-18T03:33:20Z
                Some("Europe/Brussels".into()), // 2033-05-18T05:33:20+02:00
            ))])
            .unwrap();
        if let ColumnarValue::Scalar(res) = res {
            assert_eq!(res.data_type(), DataType::Timestamp(TimeUnit::Second, None));
            assert_eq!(res, expect);
        } else {
            panic!("unexpected return type")
        }

        let expect =
            ScalarValue::TimestampNanosecond(Some(1_711_929_601_000_000_000), None); // 2024-04-01T00:00:01
        let res = ToLocalTimeFunc::new()
            .invoke(&[ColumnarValue::Scalar(ScalarValue::TimestampNanosecond(
                Some(1_711_922_401_000_000_000), // 2024-03-31T22:00:01Z
                Some("Europe/Brussels".into()),  // 2024-04-01T00:00:01+02:00
            ))])
            .unwrap();
        if let ColumnarValue::Scalar(res) = res {
            assert_eq!(
                res.data_type(),
                DataType::Timestamp(TimeUnit::Nanosecond, None)
            );
            assert_eq!(res, expect);
        } else {
            panic!("unexpected return type")
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
