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

use arrow::datatypes::DataType::Timestamp;
use arrow::datatypes::TimeUnit::Nanosecond;
use arrow::datatypes::{DataType, Field, FieldRef};
use std::sync::Arc;

use datafusion_common::config::ConfigOptions;
use datafusion_common::{Result, ScalarValue, internal_err};
use datafusion_expr::simplify::{ExprSimplifyResult, SimplifyContext};
use datafusion_expr::{
    ColumnarValue, Documentation, Expr, ReturnFieldArgs, ScalarFunctionArgs, ScalarUDF,
    ScalarUDFImpl, Signature, Volatility,
};
use datafusion_macros::user_doc;

#[user_doc(
    doc_section(label = "Time and Date Functions"),
    description = r#"
Returns the current timestamp in the system configured timezone (None by default).

The `now()` return value is determined at query time and will return the same timestamp, no matter when in the query plan the function executes.
"#,
    syntax_example = "now()",
    sql_example = r#"```sql
> SELECT now();
+----------------------------------+
| now()                            |
+----------------------------------+
| 2024-12-23T06:30:00.123456789    |
+----------------------------------+

-- The timezone of the returned timestamp depends on the session time zone
> SET datafusion.execution.time_zone = 'America/New_York';
> SELECT now();
+--------------------------------------+
| now()                                |
+--------------------------------------+
| 2024-12-23T01:30:00.123456789-05:00  |
+--------------------------------------+
```"#
)]
#[derive(Debug, PartialEq, Eq, Hash)]
pub struct NowFunc {
    signature: Signature,
    aliases: Vec<String>,
    timezone: Option<Arc<str>>,
}

impl Default for NowFunc {
    fn default() -> Self {
        Self::new_with_config(&ConfigOptions::default())
    }
}

impl NowFunc {
    pub fn new_with_config(config: &ConfigOptions) -> Self {
        Self {
            signature: Signature::nullary(Volatility::Stable),
            aliases: vec!["current_timestamp".to_string()],
            timezone: config
                .execution
                .time_zone
                .as_ref()
                .map(|tz| Arc::from(tz.as_str())),
        }
    }
}

/// Create an implementation of `now()` that always returns the
/// specified timestamp.
///
/// The semantics of `now()` require it to return the same value
/// wherever it appears within a single statement. This value is
/// chosen during planning time.
impl ScalarUDFImpl for NowFunc {
    fn name(&self) -> &str {
        "now"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn with_updated_config(&self, config: &ConfigOptions) -> Option<ScalarUDF> {
        Some(Self::new_with_config(config).into())
    }

    fn return_field_from_args(&self, _args: ReturnFieldArgs) -> Result<FieldRef> {
        Ok(Field::new(
            self.name(),
            Timestamp(Nanosecond, self.timezone.clone()),
            false,
        )
        .into())
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        internal_err!("return_field_from_args should be called instead")
    }

    fn invoke_with_args(&self, _args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        internal_err!("invoke should not be called on a simplified now() function")
    }

    fn simplify(
        &self,
        args: Vec<Expr>,
        info: &SimplifyContext,
    ) -> Result<ExprSimplifyResult> {
        let Some(now_ts) = info.query_execution_start_time() else {
            return Ok(ExprSimplifyResult::Original(args));
        };

        Ok(ExprSimplifyResult::Simplified(Expr::Literal(
            ScalarValue::TimestampNanosecond(
                now_ts.timestamp_nanos_opt(),
                self.timezone.clone(),
            ),
            None,
        )))
    }

    fn aliases(&self) -> &[String] {
        &self.aliases
    }

    fn documentation(&self) -> Option<&Documentation> {
        self.doc()
    }
}
