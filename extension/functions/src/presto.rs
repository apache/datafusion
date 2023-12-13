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

use std::{
    sync::Arc,
    time::{SystemTime, UNIX_EPOCH},
};

use arrow::{
    array::{
        ArrayRef, Int64Array, Time32MillisecondArray, TimestampMillisecondArray,
        TimestampNanosecondArray,
    },
    datatypes::{DataType, IntervalUnit, TimeUnit},
};
use chrono::{Datelike, Timelike};
use datafusion::error::Result;
use datafusion_common::DataFusionError;
use datafusion_expr::{
    ReturnTypeFunction, ScalarFunctionDef, ScalarFunctionPackage, Signature,
    TypeSignature, Volatility,
};

use arrow::array::*;
use arrow::error::ArrowError;

use chrono::{Duration, NaiveDate, TimeZone, Utc};

#[derive(Debug)]
pub struct HumanReadableSecondsFunction;

impl ScalarFunctionDef for HumanReadableSecondsFunction {
    fn name(&self) -> &str {
        "human_readable_seconds"
    }

    fn signature(&self) -> Signature {
        Signature::exact(vec![DataType::Float64], Volatility::Immutable)
    }

    fn return_type(&self) -> ReturnTypeFunction {
        let return_type = Arc::new(DataType::Utf8);
        Arc::new(move |_| Ok(return_type.clone()))
    }

    fn execute(&self, args: &[ArrayRef]) -> Result<ArrayRef> {
        assert_eq!(args.len(), 1);
        let input_array = args[0]
            .as_any()
            .downcast_ref::<Float64Array>()
            .expect("cast to Float64Array failed");

        let array = input_array
            .into_iter()
            .map(|sec| {
                let seconds = sec.map(|value| value).unwrap();
                let weeks = (seconds / 604800.0) as i64;
                let days = ((seconds % 604800.0) / 86400.0) as i64;
                let hours = ((seconds % 86400.0) / 3600.0) as i64;
                let minutes = ((seconds % 3600.0) / 60.0) as i64;
                let seconds_remainder = (seconds % 60.0) as i64;

                let mut formatted = String::new();

                if weeks > 0 {
                    formatted += &format!(
                        "{} week{}{}",
                        weeks,
                        if weeks > 1 { "s" } else { "" },
                        if days + hours + minutes + seconds_remainder > 0 {
                            ", "
                        } else {
                            ""
                        }
                    ); //for splitting ,
                }
                if days > 0 {
                    formatted += &format!(
                        "{} day{}{}",
                        days,
                        if days > 1 { "s" } else { "" },
                        if hours + minutes + seconds_remainder > 0 {
                            ", "
                        } else {
                            ""
                        }
                    ); //for splitting ,
                }
                if hours > 0 {
                    formatted += &format!(
                        "{} hour{}{}",
                        hours,
                        if hours > 1 { "s" } else { "" },
                        if minutes + seconds_remainder > 0 {
                            ", "
                        } else {
                            ""
                        }
                    ); //for splitting ,
                }
                if minutes > 0 {
                    formatted += &format!(
                        "{} minute{}{}",
                        minutes,
                        if minutes > 1 { "s" } else { "" },
                        if seconds_remainder > 0 { ", " } else { "" }
                    );
                }
                if seconds_remainder > 0 {
                    formatted += &format!(
                        "{} second{}",
                        seconds_remainder,
                        if seconds_remainder > 1 { "s" } else { "" }
                    );
                }
                if weeks + days + hours + minutes + seconds_remainder == 0 {
                    formatted = "0 second".to_string();
                }
                Some(formatted)
            })
            .collect::<StringArray>();
        Ok(Arc::new(array) as ArrayRef)
    }
}

#[derive(Debug)]
pub struct CurrentTimeFunction;

impl ScalarFunctionDef for CurrentTimeFunction {
    fn name(&self) -> &str {
        "current_time"
    }

    fn signature(&self) -> Signature {
        Signature::exact(vec![], Volatility::Immutable)
    }

    fn return_type(&self) -> ReturnTypeFunction {
        let return_type = Arc::new(DataType::Time32(TimeUnit::Millisecond));
        Arc::new(move |_| Ok(return_type.clone()))
    }

    fn execute(&self, _args: &[ArrayRef]) -> Result<ArrayRef> {
        let current_time = chrono::Utc::now().time();
        let milliseconds_since_midnight = current_time.num_seconds_from_midnight() * 1000;
        let array =
            Time32MillisecondArray::from(vec![Some(milliseconds_since_midnight as i32)]);
        Ok(Arc::new(array) as ArrayRef)
    }
}
#[derive(Debug)]
pub struct CurrentTimestampFunction;

impl ScalarFunctionDef for CurrentTimestampFunction {
    fn name(&self) -> &str {
        "current_timestamp"
    }

    fn signature(&self) -> Signature {
        Signature::exact(vec![], Volatility::Immutable)
    }

    fn return_type(&self) -> ReturnTypeFunction {
        Arc::new(move |_| Ok(Arc::new(DataType::Timestamp(TimeUnit::Microsecond, None))))
    }

    fn execute(&self, _args: &[ArrayRef]) -> Result<ArrayRef> {
        let n = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_err(|err| DataFusionError::Execution(err.to_string()))?;
        let array = TimestampMillisecondArray::from(vec![Some(n.as_millis() as i64)]);
        Ok(Arc::new(array) as ArrayRef)
    }
}
#[derive(Debug)]
pub struct CurrentTimestampPFunction;

impl ScalarFunctionDef for CurrentTimestampPFunction {
    fn name(&self) -> &str {
        "current_timestamp_p"
    }

    fn signature(&self) -> Signature {
        Signature::exact(vec![DataType::Int64], Volatility::Immutable)
    }

    fn return_type(&self) -> ReturnTypeFunction {
        Arc::new(move |_| Ok(Arc::new(DataType::Timestamp(TimeUnit::Nanosecond, None))))
    }

    fn execute(&self, args: &[ArrayRef]) -> Result<ArrayRef> {
        let precision = args[0]
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .value(0) as usize;

        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_err(|err| DataFusionError::Execution(err.to_string()))?;
        let nanos = now.as_nanos() as i64;

        let adjusted_nanos = if precision > 9 {
            nanos
        } else {
            let factor = 10_i64.pow(9 - precision as u32);
            (nanos / factor) * factor
        };

        let array = TimestampNanosecondArray::from(vec![Some(adjusted_nanos)]);
        Ok(Arc::new(array) as ArrayRef)
    }
}
#[derive(Debug)]
pub struct ToMilliSecondsFunction;

impl ScalarFunctionDef for ToMilliSecondsFunction {
    fn name(&self) -> &str {
        "to_milliseconds"
    }

    fn signature(&self) -> Signature {
        Signature::exact(
            vec![DataType::Interval(IntervalUnit::MonthDayNano)],
            Volatility::Immutable,
        )
    }

    fn return_type(&self) -> ReturnTypeFunction {
        let return_type = Arc::new(DataType::Int64);
        Arc::new(move |_| Ok(return_type.clone()))
    }

    fn execute(&self, args: &[ArrayRef]) -> Result<ArrayRef> {
        let input_array = args[0]
            .as_any()
            .downcast_ref::<IntervalMonthDayNanoArray>()
            .expect("cast to MonthDayNanoArray");

        let array = input_array
            .iter()
            .map(|arg| {
                let value = arg.unwrap() as u128;
                let months_part: i32 =
                    ((value & 0xFFFFFFFF000000000000000000000000) >> 96) as i32;
                    assert!(months_part == 0, "Error: You try to use Trino to_milliseconds(days-seconds). months must be zero");
                let days_part: i32 = ((value & 0xFFFFFFFF0000000000000000) >> 64) as i32;
                let nanoseconds_part: i64 = (value & 0xFFFFFFFFFFFFFFFF) as i64;
                let milliseconds:i64 = (days_part * 24*60*60*1000).into();
                let milliseconds= milliseconds + nanoseconds_part / 1_000_000;
                Some(milliseconds)
            })
            .collect::<Vec<_>>();
        let array = Int64Array::from(array);
        Ok(Arc::new(array) as ArrayRef)
    }
}
#[derive(Debug)]
pub struct ToIso8601Function;

impl ScalarFunctionDef for ToIso8601Function {
    fn name(&self) -> &str {
        "to_iso8601"
    }

    fn signature(&self) -> Signature {
        // This function accepts a Date, Timestamp, or Timestamp with TimeZone
        Signature::one_of(
            vec![
                TypeSignature::Exact(vec![DataType::Date32]),
                TypeSignature::Exact(vec![DataType::Timestamp(
                    TimeUnit::Nanosecond,
                    None,
                )]),
                TypeSignature::Exact(vec![DataType::Utf8]),
            ],
            Volatility::Immutable,
        )
    }

    fn return_type(&self) -> ReturnTypeFunction {
        let return_type = Arc::new(DataType::Utf8);
        Arc::new(move |_| Ok(return_type.clone()))
    }

    fn execute(&self, args: &[ArrayRef]) -> Result<ArrayRef> {
        if args.is_empty() {
            return Err(ArrowError::InvalidArgumentError(
                "args is empty".to_string(),
            ))
            .map_err(|err| DataFusionError::Execution(format!("Cast error: {}", err)));
        }

        let input = &args[0];
        let result = match input.data_type() {
            DataType::Date32 => {
                // Assuming the first array in args is a DateArray
                let date_array = args[0]
                    .as_any()
                    .downcast_ref::<Date32Array>()
                    .expect("Expected a date array");

                // Map each date32 to an ISO 8601 string
                let iso_strings: Vec<String> = date_array
                    .iter()
                    .map(|date| {
                        date.map(|date| {
                            let naive_date = NaiveDate::from_ymd_opt(1970, 1, 1).unwrap()
                                + Duration::days(date as i64);
                            naive_date.format("%Y-%m-%d").to_string()
                        })
                        .unwrap_or_else(|| String::from("null")) // Handle null values
                    })
                    .collect();

                // Create a new StringArray from the ISO 8601 strings
                Ok(Arc::new(StringArray::from(iso_strings)) as Arc<dyn Array>)
            }
            DataType::Timestamp(TimeUnit::Nanosecond, None) => {
                let timestamp_array = input
                    .as_any()
                    .downcast_ref::<TimestampNanosecondArray>()
                    .expect("Expected a NanosecondTimeStamp array");
                let milliseconds_array = timestamp_array
                    .iter()
                    .map(|timestamp| timestamp.map(|timestamp| timestamp / 1_000_000))
                    .collect::<TimestampMillisecondArray>();

                let iso_strings: Vec<String> = milliseconds_array
                    .iter()
                    .map(|timestamp| {
                        timestamp
                            .map(|timestamp| {
                                let datetime =
                                    Utc.timestamp_millis_opt(timestamp).unwrap();
                                format!("{}", datetime.format("%Y-%m-%dT%H:%M:%S%.3f"))
                            })
                            .unwrap_or_else(|| String::from("null")) // Handle null values
                    })
                    .collect();

                Ok(Arc::new(StringArray::from(iso_strings)) as Arc<dyn Array>)
            }
            // timestamp with timezone todo
            _ => Err(ArrowError::InvalidArgumentError(
                "Invalid input type".to_string(),
            ))
            .map_err(|err| DataFusionError::Execution(format!("Cast error: {}", err))),
        };
        result
    }
}
#[derive(Debug)]
pub struct FromIso8601DateFunction;
///presto `from_iso8601_date` function ([https://trino.io/docs/current/functions/datetime.html])
impl ScalarFunctionDef for FromIso8601DateFunction {
    fn name(&self) -> &str {
        "from_iso8601_date"
    }

    fn signature(&self) -> Signature {
        Signature::exact(vec![DataType::Utf8], Volatility::Immutable)
    }

    fn return_type(&self) -> ReturnTypeFunction {
        let return_type = Arc::new(DataType::Date32);
        Arc::new(move |_| Ok(return_type.clone()))
    }

    fn execute(&self, args: &[ArrayRef]) -> Result<ArrayRef> {
        let input = &args[0]
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("Failed to downcast StringArray");
        let mut result = Vec::new();

        for i in 0..input.len() {
            if input.is_null(i) {
                result.push(None);
            } else {
                let value = input.value(i);
                let parsed_date = NaiveDate::parse_from_str(value, "%Y-%m-%d")
                    .or_else(|_| NaiveDate::parse_from_str(value, "%G-W%V-%u"))
                    .or_else(|_| {
                        NaiveDate::parse_from_str(&format!("{}-1", value), "%G-W%V-%u")
                    })
                    .or_else(|_| NaiveDate::parse_from_str(value, "%Y-%j"));

                match parsed_date {
                    Ok(date) => result.push(Some(date.num_days_from_ce() - 719163)), // Adjust for Unix time
                    Err(_) => result.push(None),
                }
            }
        }

        Ok(Arc::new(Date32Array::from(result)) as ArrayRef)
    }
}

// Function package declaration
pub struct FunctionPackage;

impl ScalarFunctionPackage for FunctionPackage {
    fn functions(&self) -> Vec<Box<dyn ScalarFunctionDef>> {
        vec![
            Box::new(HumanReadableSecondsFunction),
            Box::new(CurrentTimeFunction),
            Box::new(ToMilliSecondsFunction),
            Box::new(CurrentTimestampFunction),
            Box::new(CurrentTimestampPFunction),
            Box::new(ToIso8601Function),
            Box::new(FromIso8601DateFunction),
        ]
    }
}

#[cfg(test)]
mod test {
    use std::{
        sync::Arc,
        time::{SystemTime, UNIX_EPOCH},
    };

    use arrow::array::{
        Array, ArrayRef, Int64Array, TimestampMillisecondArray, TimestampNanosecondArray,
    };
    use chrono::Utc;
    use datafusion::error::Result;
    use datafusion::prelude::SessionContext;
    use datafusion_expr::ScalarFunctionDef;
    use tokio;

    use crate::{
        presto::{CurrentTimestampFunction, CurrentTimestampPFunction},
        utils::{execute, test_expression},
    };

    use super::FunctionPackage;

    #[tokio::test]
    async fn test_human_readable_seconds() -> Result<()> {
        test_expression!("human_readable_seconds(604800.0)", "1 week");
        test_expression!("human_readable_seconds(86400.0)", "1 day");
        test_expression!("human_readable_seconds(3600.0)", "1 hour");
        test_expression!("human_readable_seconds(60.0)", "1 minute");
        test_expression!("human_readable_seconds(1.0)", "1 second");
        test_expression!("human_readable_seconds(0.0)", "0 second");
        test_expression!("human_readable_seconds(96)", "1 minute, 36 seconds");
        test_expression!(
            "human_readable_seconds(3762)",
            "1 hour, 2 minutes, 42 seconds"
        );
        test_expression!(
            "human_readable_seconds(56363463)",
            "93 weeks, 1 day, 8 hours, 31 minutes, 3 seconds"
        );
        Ok(())
    }

    #[tokio::test]
    async fn test_current_time() -> Result<()> {
        let current = Utc::now();
        let formatted = current.format("%H:%M:%S").to_string();
        test_expression!("current_time()", formatted);

        Ok(())
    }

    #[tokio::test]
    async fn test_to_milliseconds() -> Result<()> {
        test_expression!("to_milliseconds(interval '1' day)", "86400000");
        test_expression!("to_milliseconds(interval '1' hour)", "3600000");
        test_expression!("to_milliseconds(interval '10' day)", "864000000");
        Ok(())
    }

    fn roughly_equal_to_now(millisecond: i64) -> bool {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_millis() as i64;
        (millisecond - now).abs() <= 1
    }

    #[tokio::test]
    async fn test_current_timestamp() -> Result<()> {
        let current_timestamp_function = CurrentTimestampFunction {};
        let result = current_timestamp_function.execute(&[]).unwrap();
        let result_array = result
            .as_any()
            .downcast_ref::<TimestampMillisecondArray>()
            .unwrap()
            .value(0);
        assert!(roughly_equal_to_now(result_array));
        Ok(())
    }

    #[tokio::test]
    async fn test_current_timestamp_p() -> Result<()> {
        let function = CurrentTimestampPFunction {};
        let precision_array = Int64Array::from(vec![9]);
        let args = vec![Arc::new(precision_array) as ArrayRef];
        let result = function.execute(&args).unwrap();
        let result_array = result
            .as_any()
            .downcast_ref::<TimestampNanosecondArray>()
            .unwrap()
            .value(0);
        assert!(roughly_equal_to_now(result_array / 1_000_000));
        Ok(())
    }

    #[tokio::test]
    async fn test_to_iso8601() -> Result<()> {
        //Test cases for different input types
        // Date
        test_expression!("to_iso8601(Date '2023-03-15')", "2023-03-15");
        // Timestamp
        test_expression!(
            "to_iso8601( timestamp '2001-04-13T02:00:00')",
            "2001-04-13T02:00:00.000"
        );
        //TIMESTAMP '2020-06-10 15:55:23.383345'
        test_expression!(
            "to_iso8601( timestamp '2020-06-10 15:55:23.383345')",
            "2020-06-10T15:55:23.383"
        );
        Ok(())
    }

    #[tokio::test]
    async fn test_from_iso8601_date() -> Result<()> {
        test_expression!("from_iso8601_date('2020-05-11')", "2020-05-11");
        test_expression!("from_iso8601_date('2020-W10')", "2020-03-02");
        test_expression!("from_iso8601_date('2020-W10-1')", "2020-03-02");
        test_expression!("from_iso8601_date('2020-123')", "2020-05-02");
        Ok(())
    }
}
