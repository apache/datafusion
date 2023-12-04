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

use std::sync::Arc;

use arrow::array::*;
use arrow::error::ArrowError;
use arrow::{
    array::{ArrayRef, Time32MillisecondArray},
    datatypes::{DataType, TimeUnit},
};
use chrono::{NaiveDate, TimeZone, Timelike, Utc};
use datafusion::error::Result;
use datafusion_common::DataFusionError;
use datafusion_expr::{
    ReturnTypeFunction, ScalarFunctionDef, ScalarFunctionPackage, Signature,
    TypeSignature, Volatility,
};

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
                    TimeUnit::Millisecond,
                    None,
                )]),
                TypeSignature::Exact(vec![DataType::Timestamp(
                    TimeUnit::Millisecond,
                    Some(String::new().into()),
                )]),
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
                "Invalid input type".to_string(),
            ))
            .map_err(|err| DataFusionError::Execution(format!("Cast error: {}", err)));
        }

        let input = &args[0];
        let result = match input.data_type() {
            DataType::Date32 => {
                // Convert Date to ISO 8601 String
                // Implementation details depend on how Date is represented
                let date_array = input.as_any().downcast_ref::<Date32Array>().unwrap();
                let mut builder =
                    StringBuilder::with_capacity(date_array.len(), date_array.len());

                for i in 0..date_array.len() {
                    let date_option = date_array.value(i);
                    let date_string = NaiveDate::from_ymd_opt(1970, 1, 1)
                        .unwrap()
                        .checked_add_signed(chrono::Duration::days(date_option as i64))
                        .unwrap()
                        .format("%Y-%m-%d")
                        .to_string();
                    builder.append_value(date_string);
                }

                Ok::<ArrayRef, ArrowError>(Arc::new(builder.finish()))
                // Ok(Arc::new(builder.finish()) as ArrayRef)
            }
            DataType::Timestamp(TimeUnit::Millisecond, None) => {
                // Convert Timestamp to ISO 8601 String
                // Implementation details depend on how Timestamp is represented
                let timestamp_array = input
                    .as_any()
                    .downcast_ref::<TimestampMillisecondArray>()
                    .unwrap();
                let mut builder = StringBuilder::with_capacity(
                    timestamp_array.len(),
                    timestamp_array.len() * 24,
                );

                for i in 0..timestamp_array.len() {
                    let timestamp_option = timestamp_array.value(i);
                    let timestamp_string = Utc
                        .timestamp_millis_opt(timestamp_option)
                        .unwrap()
                        .to_rfc3339_opts(chrono::SecondsFormat::Secs, true);
                    builder.append_value(&timestamp_string);
                }

                Ok(Arc::new(builder.finish()) as ArrayRef)
            }
            DataType::Timestamp(TimeUnit::Millisecond, Some(timezone)) => {
                // Convert Timestamp with TimeZone to ISO 8601 String
                // Implementation details depend on how TimestampTZ is represented

                let timestamp_array = input
                    .as_any()
                    .downcast_ref::<TimestampMillisecondArray>()
                    .unwrap();
                let mut builder = StringBuilder::with_capacity(
                    timestamp_array.len(),
                    timestamp_array.len() * 24,
                );
                let tz: chrono_tz::Tz = timezone.parse().unwrap();

                for i in 0..timestamp_array.len() {
                    let timestamp_option = timestamp_array.value(i);
                    let timestamp_string = tz
                        .timestamp_millis_opt(timestamp_option)
                        .unwrap()
                        .to_rfc3339_opts(chrono::SecondsFormat::Secs, true);
                    builder.append_value(&timestamp_string);
                }

                Ok(Arc::new(builder.finish()) as ArrayRef)
            }
            _ => {
                return Err(ArrowError::InvalidArgumentError(
                    "Invalid input type".to_string(),
                ))
                .map_err(|err| {
                    DataFusionError::Execution(format!("Cast error: {}", err))
                })
            }
        }
        .unwrap();

        Ok(result)
    }
}

// Function package declaration
pub struct FunctionPackage;

impl ScalarFunctionPackage for FunctionPackage {
    fn functions(&self) -> Vec<Box<dyn ScalarFunctionDef>> {
        //vec![Box::new(AddOneFunction), Box::new(MultiplyTwoFunction)]
        vec![Box::new(ToIso8601Function)]
    }
}

#[cfg(test)]
mod test {
    use datafusion::error::Result;
    use datafusion::prelude::SessionContext;
    use tokio;

    use crate::utils::{execute, test_expression};

    use super::FunctionPackage;

    #[tokio::test]
    async fn test_to_iso8601() -> Result<()> {
        // test_expression!(
        //     "age(timestamp '2001-04-10', timestamp '2001-04-11')",
        //     "0 years 0 mons -1 days 0 hours 0 mins 0.000 secs"
        // );
        // Test cases for different input types
        test_expression!("to_iso8601(Date '2023-03-15')", "2023-03-15"); // Date
        test_expression!("to_iso8601(timestamp '2023-03-15T12:45:30')", "'2023-03-15T12:45:30'"); // Timestamp
        test_expression!(
            "to_iso8601(timestamp '2023-03-15T12:45:30+02:00')",
            "'2023-03-15T12:45:30+02:00'"
        ); // Timestamp with TimeZone

        Ok(())
    }
}
