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

use arrow::array::*;

use arrow::array::{ArrayRef, TimestampNanosecondArray};
use arrow::compute::cast;
use arrow::datatypes::IntervalDayTimeType;
use arrow::datatypes::IntervalUnit;
use arrow::datatypes::{DataType, TimeUnit};
use datafusion::error::Result;

use chrono::NaiveDateTime;
use datafusion::logical_expr::Volatility;

use datafusion_common::DataFusionError;
use datafusion_expr::{
    ReturnTypeFunction, ScalarFunctionDef, ScalarFunctionPackage, Signature,
};
use std::sync::Arc;

#[derive(Debug)]
pub struct AgeFunction;

impl ScalarFunctionDef for AgeFunction {
    fn name(&self) -> &str {
        "age"
    }

    fn signature(&self) -> Signature {
        Signature::exact(
            vec![
                DataType::Timestamp(TimeUnit::Nanosecond, None),
                DataType::Timestamp(TimeUnit::Nanosecond, None),
            ],
            Volatility::Immutable,
        )
    }

    fn return_type(&self) -> ReturnTypeFunction {
        let return_type = Arc::new(DataType::Timestamp(TimeUnit::Microsecond, None));
        Arc::new(move |_| Ok(return_type.clone()))
    }

    fn execute(&self, args: &[ArrayRef]) -> Result<ArrayRef> {
        assert_eq!(args.len(), 2);
        let start_array = args[0]
            .as_any()
            .downcast_ref::<TimestampNanosecondArray>()
            .expect("cast to TimestampNanosecondArray failed");
        let end_array = args[1]
            .as_any()
            .downcast_ref::<TimestampNanosecondArray>()
            .expect("cast to TimestampNanosecondArray failed");

        let mut b = IntervalDayTimeBuilder::with_capacity(start_array.len());

        for i in 0..start_array.len() {
            if start_array.is_null(i) || end_array.is_null(i) {
                b.append_null();
                continue;
            }

            let start_value = NaiveDateTime::from_timestamp_opt(
                start_array.value(i) / 1_000_000_000,
                0,
            );
            let end_value =
                NaiveDateTime::from_timestamp_opt(end_array.value(i) / 1_000_000_000, 0);

            let duration = start_value
                .unwrap()
                .signed_duration_since(end_value.unwrap());
            let days = duration.num_days();
            let millisecond = duration.num_milliseconds() - days * 24 * 60 * 60 * 1000;
            b.append_value(IntervalDayTimeType::make_value(
                days as i32,
                millisecond as i32,
            ));
        }
        let result = b.finish();
        cast(
            &(Arc::new(result) as ArrayRef),
            &DataType::Interval(IntervalUnit::DayTime),
        )
        .map_err(|err| DataFusionError::Execution(format!("Cast error: {}", err)))
    }
}

// Function package declaration
pub struct FunctionPackage;

impl ScalarFunctionPackage for FunctionPackage {
    fn functions(&self) -> Vec<Box<dyn ScalarFunctionDef>> {
        vec![Box::new(AgeFunction)]
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
    async fn test_age_function() -> Result<()> {
        // Test date difference within the same month
        test_expression!(
            "age(timestamp '2001-04-10', timestamp '2001-04-11')",
            "0 years 0 mons -1 days 0 hours 0 mins 0.000 secs"
        );
        test_expression!(
            "age(timestamp '2001-04-10T23:00:00', timestamp '2001-04-11T22:00:00')",
            "0 years 0 mons 0 days -23 hours 0 mins 0.000 secs"
        );
        // // Test date difference between different months within the same year
        test_expression!(
            "age( timestamp '2001-04-11T22:00:00', timestamp '2001-04-10T23:00:00')",
            "0 years 0 mons 0 days 23 hours 0 mins 0.000 secs"
        );
        // Test date difference across different years
        test_expression!(
            "age(timestamp '2000-04-10T01:00:00', timestamp '2001-04-13T02:00:00')",
            "0 years 0 mons -368 days -1 hours 0 mins 0.000 secs"
        );
        // Test date difference involving a leap year
        test_expression!(
            "age(timestamp '2001-04-13T02:00:00',timestamp '2000-04-10T01:00:00')",
            "0 years 0 mons 368 days 1 hours 0 mins 0.000 secs"
        );
        // Test timestamp difference
        test_expression!(
            "age(timestamp '2001-04-10T23:00:00.000006', timestamp '2001-04-11T22:00:00.000006')",
            "0 years 0 mons 0 days -23 hours 0 mins 0.000 secs"
        );

        Ok(())
    }
}
