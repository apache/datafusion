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

use arrow::{
    array::*,
    datatypes::{DataType, IntervalUnit, TimeUnit},
};
use chrono::Timelike;
use datafusion::error::Result;
use datafusion_expr::{
    ReturnTypeFunction, ScalarFunctionDef, ScalarFunctionPackage, Signature, Volatility,
};

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
        let current_time = chrono::Local::now().time();
        let milliseconds_since_midnight = current_time.num_seconds_from_midnight() * 1000;
        let array =
            Time32MillisecondArray::from(vec![Some(milliseconds_since_midnight as i32)]);
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
// Function package declaration
pub struct FunctionPackage;

impl ScalarFunctionPackage for FunctionPackage {
    fn functions(&self) -> Vec<Box<dyn ScalarFunctionDef>> {
        vec![
            Box::new(HumanReadableSecondsFunction),
            Box::new(CurrentTimeFunction),
            Box::new(ToMilliSecondsFunction),
        ]
    }
}

#[cfg(test)]
mod test {
    use chrono::Local;
    use datafusion::error::Result;
    use datafusion::prelude::SessionContext;
    use tokio;

    use crate::utils::{execute, test_expression};

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
        let current = Local::now();
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
}
