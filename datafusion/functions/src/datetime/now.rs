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

use arrow::datatypes::DataType;
use arrow::datatypes::DataType::Timestamp;
use arrow::datatypes::TimeUnit::Nanosecond;
use std::any::Any;
use std::sync::OnceLock;

use datafusion_common::{internal_err, ExprSchema, Result, ScalarValue};
use datafusion_expr::scalar_doc_sections::DOC_SECTION_DATETIME;
use datafusion_expr::simplify::{ExprSimplifyResult, SimplifyInfo};
use datafusion_expr::{
    ColumnarValue, Documentation, Expr, ScalarUDFImpl, Signature, Volatility,
};

#[derive(Debug)]
pub struct NowFunc {
    signature: Signature,
    aliases: Vec<String>,
}

impl Default for NowFunc {
    fn default() -> Self {
        Self::new()
    }
}

impl NowFunc {
    pub fn new() -> Self {
        Self {
            signature: Signature::uniform(0, vec![], Volatility::Stable),
            aliases: vec!["current_timestamp".to_string()],
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

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(Timestamp(Nanosecond, Some("+00:00".into())))
    }

    fn invoke(&self, _args: &[ColumnarValue]) -> Result<ColumnarValue> {
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
            ScalarValue::TimestampNanosecond(now_ts, Some("+00:00".into())),
        )))
    }
    fn documentation(&self) -> Option<&Documentation> {
        Some(get_to_unixtime_doc())
    }

    fn aliases(&self) -> &[String] {
        &self.aliases
    }

    fn is_nullable(&self, _args: &[Expr], _schema: &dyn ExprSchema) -> bool {
        false
    }
}

static DOCUMENTATION: OnceLock<Documentation> = OnceLock::new();

fn get_to_unixtime_doc() -> &'static Documentation {
    DOCUMENTATION.get_or_init(|| {
        Documentation::builder()
            .with_doc_section(DOC_SECTION_DATETIME)
            .with_description(r#"
Returns the current UTC timestamp.

The `now()` return value is determined at query time and will return the same timestamp, no matter when in the query plan the function executes.
"#)
            .with_syntax_example("now()")
            .build()
            .unwrap()
    })
}
