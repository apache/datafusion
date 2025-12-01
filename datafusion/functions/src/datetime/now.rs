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
use std::any::Any;
use std::sync::Arc;

use datafusion_common::config::ConfigOptions;
use datafusion_common::{internal_err, Result, ScalarValue};
use datafusion_expr::simplify::{ExprSimplifyResult, SimplifyInfo};
use datafusion_expr::{
    ColumnarValue, Documentation, Expr, ReturnFieldArgs, ScalarUDF, ScalarUDFImpl,
    Signature, Volatility,
};
use datafusion_macros::user_doc;

#[user_doc(
    doc_section(label = "Time and Date Functions"),
    description = r#"
Returns the current timestamp in the system configured timezone (None by default).

The `now()` return value is determined at query time and will return the same timestamp, no matter when in the query plan the function executes.
"#,
    syntax_example = "now()"
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
    #[deprecated(since = "50.2.0", note = "use `new_with_config` instead")]
    /// Deprecated constructor retained for backwards compatibility.
    ///
    /// Prefer [`NowFunc::new_with_config`] which allows specifying the
    /// timezone via [`ConfigOptions`]. This helper now mirrors the
    /// canonical default offset (None) provided by `ConfigOptions::default()`.
    pub fn new() -> Self {
        Self::new_with_config(&ConfigOptions::default())
    }

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
    fn as_any(&self) -> &dyn Any {
        self
    }

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

    fn invoke_with_args(
        &self,
        _args: datafusion_expr::ScalarFunctionArgs,
    ) -> Result<ColumnarValue> {
        internal_err!("invoke should not be called on a simplified now() function")
    }

    fn simplify(
        &self,
        _args: Vec<Expr>,
        info: &dyn SimplifyInfo,
    ) -> Result<ExprSimplifyResult> {
        let now_ts = info
            .execution_props()
            .query_execution_start_time
            .timestamp_nanos_opt();

        Ok(ExprSimplifyResult::Simplified(Expr::Literal(
            ScalarValue::TimestampNanosecond(now_ts, self.timezone.clone()),
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

#[cfg(test)]
mod tests {
    use super::*;

    #[expect(deprecated)]
    #[test]
    fn now_func_default_matches_config() {
        let default_config = ConfigOptions::default();

        let legacy_now = NowFunc::new();
        let configured_now = NowFunc::new_with_config(&default_config);

        let empty_fields: [FieldRef; 0] = [];
        let empty_scalars: [Option<&ScalarValue>; 0] = [];

        let legacy_field = legacy_now
            .return_field_from_args(ReturnFieldArgs {
                arg_fields: &empty_fields,
                scalar_arguments: &empty_scalars,
            })
            .expect("legacy now() return field");

        let configured_field = configured_now
            .return_field_from_args(ReturnFieldArgs {
                arg_fields: &empty_fields,
                scalar_arguments: &empty_scalars,
            })
            .expect("configured now() return field");

        assert_eq!(legacy_field.as_ref(), configured_field.as_ref());

        let legacy_scalar =
            ScalarValue::TimestampNanosecond(None, legacy_now.timezone.clone());
        let configured_scalar =
            ScalarValue::TimestampNanosecond(None, configured_now.timezone.clone());

        assert_eq!(legacy_scalar, configured_scalar);
        assert_eq!(None, legacy_now.timezone.as_deref());
    }
}
