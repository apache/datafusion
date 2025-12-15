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

use arrow::array::timezone::Tz;
use arrow::datatypes::DataType;
use arrow::datatypes::DataType::Time64;
use arrow::datatypes::TimeUnit::Nanosecond;
use chrono::TimeZone;
use chrono::Timelike;
use datafusion_common::{Result, ScalarValue, internal_err};
use datafusion_expr::simplify::{ExprSimplifyResult, SimplifyInfo};
use datafusion_expr::{
    ColumnarValue, Documentation, Expr, ScalarUDFImpl, Signature, Volatility,
};
use datafusion_macros::user_doc;
use std::any::Any;

#[user_doc(
    doc_section(label = "Time and Date Functions"),
    description = r#"
Returns the current time in the session time zone.

The `current_time()` return value is determined at query time and will return the same time, no matter when in the query plan the function executes.

The session time zone can be set using the statement 'SET datafusion.execution.time_zone = desired time zone'. The time zone can be a value like +00:00, 'Europe/London' etc.
"#,
    syntax_example = r#"current_time()
    (optional) SET datafusion.execution.time_zone = '+00:00';
    SELECT current_time();"#
)]
#[derive(Debug, PartialEq, Eq, Hash)]
pub struct CurrentTimeFunc {
    signature: Signature,
}

impl Default for CurrentTimeFunc {
    fn default() -> Self {
        Self::new()
    }
}

impl CurrentTimeFunc {
    pub fn new() -> Self {
        Self {
            signature: Signature::nullary(Volatility::Stable),
        }
    }
}

/// Create an implementation of `current_time()` that always returns the
/// specified current time.
///
/// The semantics of `current_time()` require it to return the same value
/// wherever it appears within a single statement. This value is
/// chosen during planning time.
impl ScalarUDFImpl for CurrentTimeFunc {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "current_time"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(Time64(Nanosecond))
    }

    fn invoke_with_args(
        &self,
        _args: datafusion_expr::ScalarFunctionArgs,
    ) -> Result<ColumnarValue> {
        internal_err!(
            "invoke should not be called on a simplified current_time() function"
        )
    }

    fn simplify(
        &self,
        _args: Vec<Expr>,
        info: &dyn SimplifyInfo,
    ) -> Result<ExprSimplifyResult> {
        let now_ts = info.execution_props().query_execution_start_time;

        // Try to get timezone from config and convert to local time
        let nano = info
            .execution_props()
            .config_options()
            .and_then(|config| {
                config
                    .execution
                    .time_zone
                    .as_ref()
                    .map(|tz| tz.parse::<Tz>().ok())
            })
            .flatten()
            .map_or_else(
                || datetime_to_time_nanos(&now_ts),
                |tz| {
                    let local_now = tz.from_utc_datetime(&now_ts.naive_utc());
                    datetime_to_time_nanos(&local_now)
                },
            );

        Ok(ExprSimplifyResult::Simplified(Expr::Literal(
            ScalarValue::Time64Nanosecond(nano),
            None,
        )))
    }

    fn documentation(&self) -> Option<&Documentation> {
        self.doc()
    }
}

// Helper function for conversion of datetime to a timestamp.
fn datetime_to_time_nanos<Tz: TimeZone>(dt: &chrono::DateTime<Tz>) -> Option<i64> {
    let hour = dt.hour() as i64;
    let minute = dt.minute() as i64;
    let second = dt.second() as i64;
    let nanosecond = dt.nanosecond() as i64;
    Some((hour * 3600 + minute * 60 + second) * 1_000_000_000 + nanosecond)
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::datatypes::{DataType, TimeUnit::Nanosecond};
    use chrono::{DateTime, Utc};
    use datafusion_common::{Result, ScalarValue};
    use datafusion_expr::execution_props::ExecutionProps;
    use datafusion_expr::simplify::{ExprSimplifyResult, SimplifyInfo};
    use std::sync::Arc;

    struct MockSimplifyInfo {
        execution_props: ExecutionProps,
    }

    impl SimplifyInfo for MockSimplifyInfo {
        fn is_boolean_type(&self, _expr: &Expr) -> Result<bool> {
            Ok(false)
        }

        fn nullable(&self, _expr: &Expr) -> Result<bool> {
            Ok(true)
        }

        fn execution_props(&self) -> &ExecutionProps {
            &self.execution_props
        }

        fn get_data_type(&self, _expr: &Expr) -> Result<DataType> {
            Ok(Time64(Nanosecond))
        }
    }

    fn set_session_timezone_env(tz: &str, start_time: DateTime<Utc>) -> MockSimplifyInfo {
        let mut config = datafusion_common::config::ConfigOptions::default();
        config.execution.time_zone = if tz.is_empty() {
            None
        } else {
            Some(tz.to_string())
        };
        let mut execution_props =
            ExecutionProps::new().with_query_execution_start_time(start_time);
        execution_props.config_options = Some(Arc::new(config));
        MockSimplifyInfo { execution_props }
    }

    #[test]
    fn test_current_time_timezone_offset() {
        // Use a fixed start time for consistent testing
        let start_time = Utc.with_ymd_and_hms(2025, 1, 1, 12, 0, 0).unwrap();

        // Test with UTC+05:00
        let info_plus_5 = set_session_timezone_env("+05:00", start_time);
        let result_plus_5 = CurrentTimeFunc::new()
            .simplify(vec![], &info_plus_5)
            .unwrap();

        // Test with UTC-05:00
        let info_minus_5 = set_session_timezone_env("-05:00", start_time);
        let result_minus_5 = CurrentTimeFunc::new()
            .simplify(vec![], &info_minus_5)
            .unwrap();

        // Extract nanoseconds from results
        let nanos_plus_5 = match result_plus_5 {
            ExprSimplifyResult::Simplified(Expr::Literal(
                ScalarValue::Time64Nanosecond(Some(n)),
                _,
            )) => n,
            _ => panic!("Expected Time64Nanosecond literal"),
        };

        let nanos_minus_5 = match result_minus_5 {
            ExprSimplifyResult::Simplified(Expr::Literal(
                ScalarValue::Time64Nanosecond(Some(n)),
                _,
            )) => n,
            _ => panic!("Expected Time64Nanosecond literal"),
        };

        // Calculate the difference: UTC+05:00 should be 10 hours ahead of UTC-05:00
        let difference = nanos_plus_5 - nanos_minus_5;

        // 10 hours in nanoseconds
        let expected_offset = 10i64 * 3600 * 1_000_000_000;

        assert_eq!(
            difference, expected_offset,
            "Expected 10-hour offset difference in nanoseconds between UTC+05:00 and UTC-05:00"
        );
    }
}
