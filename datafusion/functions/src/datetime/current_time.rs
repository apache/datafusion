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
use arrow::datatypes::DataType::Time64;
use arrow::datatypes::TimeUnit::Nanosecond;
use std::any::Any;
use std::sync::OnceLock;

use datafusion_common::{internal_err, Result, ScalarValue};
use datafusion_expr::scalar_doc_sections::DOC_SECTION_DATETIME;
use datafusion_expr::simplify::{ExprSimplifyResult, SimplifyInfo};
use datafusion_expr::{
    ColumnarValue, Documentation, Expr, ScalarUDFImpl, Signature, Volatility,
};

#[derive(Debug)]
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
            signature: Signature::uniform(0, vec![], Volatility::Stable),
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

    fn invoke(&self, _args: &[ColumnarValue]) -> Result<ColumnarValue> {
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
        let nano = now_ts.timestamp_nanos_opt().map(|ts| ts % 86400000000000);
        Ok(ExprSimplifyResult::Simplified(Expr::Literal(
            ScalarValue::Time64Nanosecond(nano),
        )))
    }

    fn documentation(&self) -> Option<&Documentation> {
        Some(get_current_time_doc())
    }
}

static DOCUMENTATION: OnceLock<Documentation> = OnceLock::new();

fn get_current_time_doc() -> &'static Documentation {
    DOCUMENTATION.get_or_init(|| {
        Documentation::builder()
            .with_doc_section(DOC_SECTION_DATETIME)
            .with_description(r#"
Returns the current UTC time.

The `current_time()` return value is determined at query time and will return the same time, no matter when in the query plan the function executes.
"#)
            .with_syntax_example("current_time()")
            .build()
            .unwrap()
    })
}
