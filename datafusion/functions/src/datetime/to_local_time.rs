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

use arrow::array::timezone::Tz;
use arrow::datatypes::DataType;
use arrow::datatypes::DataType::Timestamp;
use arrow::datatypes::TimeUnit::{Microsecond, Millisecond, Nanosecond, Second};

use chrono::{DateTime, Offset, TimeDelta, TimeZone};
use datafusion_common::{exec_err, Result, ScalarValue};
use datafusion_expr::{ColumnarValue, ScalarUDFImpl, Signature, Volatility};

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
        Self {
            signature: Signature::variadic_any(Volatility::Immutable),
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
                // TODO chunchun: add ColumnarValue::Array
                _ => {
                    exec_err!(
                        "to_local_time function requires timestamp argument, got {:?}",
                        arg_type
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
        if arg_types.is_empty() {
            return exec_err!("to_local_time function requires 1 arguments, got 0");
        }

        match &arg_types[0] {
            Timestamp(Nanosecond, _) => Ok(Timestamp(Nanosecond, None)),
            Timestamp(Microsecond, _) => Ok(Timestamp(Microsecond, None)),
            Timestamp(Millisecond, _) => Ok(Timestamp(Millisecond, None)),
            Timestamp(Second, _) => Ok(Timestamp(Second, None)),
            _ => exec_err!(
                "The to_local_time function can only accept timestamp as the arg."
            ),
        }
    }

    fn invoke(&self, args: &[ColumnarValue]) -> Result<ColumnarValue> {
        if args.is_empty() {
            return exec_err!(
                "to_local_time function requires 1 or more arguments, got 0"
            );
        }

        self.to_local_time(args)
    }
}

#[cfg(test)]
mod tests {
    use datafusion_common::ScalarValue;
    use datafusion_expr::{ColumnarValue, ScalarUDFImpl};

    use super::ToLocalTimeFunc;

    #[test]
    fn test_to_local_time() {
        // TODO chunchun: update test cases to assert the result
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

        let res = ToLocalTimeFunc::new().invoke(&[ColumnarValue::Scalar(
            ScalarValue::TimestampSecond(
                Some(2_000_000_000), // 2033-05-18T03:33:20Z
                Some("Europe/Brussels".into()),
            ),
        )]);
        assert!(res.is_ok());

        let res = ToLocalTimeFunc::new().invoke(&[ColumnarValue::Scalar(
            ScalarValue::TimestampNanosecond(
                Some(1711922401000000000),      // 2024-03-31T22:00:01Z
                Some("Europe/Brussels".into()), // 2024-04-01T00:00:01+02:00
            ),
        )]);
        assert!(res.is_ok());
    }
}
