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

use arrow::array::timezone::Tz;
use arrow::datatypes::DataType;
use arrow::datatypes::DataType::Date32;
use chrono::{Datelike, NaiveDate, TimeZone};

use datafusion_common::{Result, ScalarValue, internal_err};
use datafusion_expr::simplify::{ExprSimplifyResult, SimplifyInfo};
use datafusion_expr::{
    ColumnarValue, Documentation, Expr, ScalarUDFImpl, Signature, Volatility,
};
use datafusion_macros::user_doc;

#[user_doc(
    doc_section(label = "Time and Date Functions"),
    description = r#"
Returns the current date in the session time zone.

The `current_date()` return value is determined at query time and will return the same date, no matter when in the query plan the function executes.
"#,
    syntax_example = r#"current_date()
    (optional) SET datafusion.execution.time_zone = '+00:00';
    SELECT current_date();"#
)]
#[derive(Debug, PartialEq, Eq, Hash)]
pub struct CurrentDateFunc {
    signature: Signature,
    aliases: Vec<String>,
}

impl Default for CurrentDateFunc {
    fn default() -> Self {
        Self::new()
    }
}

impl CurrentDateFunc {
    pub fn new() -> Self {
        Self {
            signature: Signature::nullary(Volatility::Stable),
            aliases: vec![String::from("today")],
        }
    }
}

/// Create an implementation of `current_date()` that always returns the
/// specified current date.
///
/// The semantics of `current_date()` require it to return the same value
/// wherever it appears within a single statement. This value is
/// chosen during planning time.
impl ScalarUDFImpl for CurrentDateFunc {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "current_date"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(Date32)
    }

    fn invoke_with_args(
        &self,
        _args: datafusion_expr::ScalarFunctionArgs,
    ) -> Result<ColumnarValue> {
        internal_err!(
            "invoke should not be called on a simplified current_date() function"
        )
    }

    fn aliases(&self) -> &[String] {
        &self.aliases
    }

    fn simplify(
        &self,
        _args: Vec<Expr>,
        info: &dyn SimplifyInfo,
    ) -> Result<ExprSimplifyResult> {
        let now_ts = info.execution_props().query_execution_start_time;

        // Get timezone from config and convert to local time
        let days = info
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
                || datetime_to_days(&now_ts),
                |tz| {
                    let local_now = tz.from_utc_datetime(&now_ts.naive_utc());
                    datetime_to_days(&local_now)
                },
            );
        Ok(ExprSimplifyResult::Simplified(Expr::Literal(
            ScalarValue::Date32(Some(days)),
            None,
        )))
    }

    fn documentation(&self) -> Option<&Documentation> {
        self.doc()
    }
}

/// Converts a DateTime to the number of days since Unix epoch (1970-01-01)
fn datetime_to_days<T: Datelike>(dt: &T) -> i32 {
    dt.num_days_from_ce()
        - NaiveDate::from_ymd_opt(1970, 1, 1)
            .unwrap()
            .num_days_from_ce()
}
