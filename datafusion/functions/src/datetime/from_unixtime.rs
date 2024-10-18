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
use arrow::datatypes::DataType::{Int64, Timestamp};
use arrow::datatypes::TimeUnit::Second;
use std::any::Any;
use std::sync::OnceLock;

use datafusion_common::{exec_err, Result};
use datafusion_expr::scalar_doc_sections::DOC_SECTION_DATETIME;
use datafusion_expr::{
    ColumnarValue, Documentation, ScalarUDFImpl, Signature, Volatility,
};

#[derive(Debug)]
pub struct FromUnixtimeFunc {
    signature: Signature,
}

impl Default for FromUnixtimeFunc {
    fn default() -> Self {
        Self::new()
    }
}

impl FromUnixtimeFunc {
    pub fn new() -> Self {
        Self {
            signature: Signature::uniform(1, vec![Int64], Volatility::Immutable),
        }
    }
}

impl ScalarUDFImpl for FromUnixtimeFunc {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "from_unixtime"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(Timestamp(Second, None))
    }

    fn invoke(&self, args: &[ColumnarValue]) -> Result<ColumnarValue> {
        if args.len() != 1 {
            return exec_err!(
                "from_unixtime function requires 1 argument, got {}",
                args.len()
            );
        }

        match args[0].data_type() {
            Int64 => args[0].cast_to(&Timestamp(Second, None), None),
            other => {
                exec_err!(
                    "Unsupported data type {:?} for function from_unixtime",
                    other
                )
            }
        }
    }
    fn documentation(&self) -> Option<&Documentation> {
        Some(get_from_unixtime_doc())
    }
}

static DOCUMENTATION: OnceLock<Documentation> = OnceLock::new();

fn get_from_unixtime_doc() -> &'static Documentation {
    DOCUMENTATION.get_or_init(|| {
        Documentation::builder()
            .with_doc_section(DOC_SECTION_DATETIME)
            .with_description("Converts an integer to RFC3339 timestamp format (`YYYY-MM-DDT00:00:00.000000000Z`). Integers and unsigned integers are interpreted as nanoseconds since the unix epoch (`1970-01-01T00:00:00Z`) return the corresponding timestamp.")
            .with_syntax_example("from_unixtime(expression)")
            .with_argument(
                "expression",
                "Expression to operate on. Can be a constant, column, or function, and any combination of arithmetic operators."
            )
            .build()
            .unwrap()
    })
}
