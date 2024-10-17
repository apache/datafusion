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

use arrow::datatypes::{DataType, TimeUnit};
use std::any::Any;
use std::sync::OnceLock;

use super::to_timestamp::ToTimestampSecondsFunc;
use crate::datetime::common::*;
use datafusion_common::{exec_err, Result};
use datafusion_expr::scalar_doc_sections::DOC_SECTION_DATETIME;
use datafusion_expr::{
    ColumnarValue, Documentation, ScalarUDFImpl, Signature, Volatility,
};

#[derive(Debug)]
pub struct ToUnixtimeFunc {
    signature: Signature,
}

impl Default for ToUnixtimeFunc {
    fn default() -> Self {
        Self::new()
    }
}

impl ToUnixtimeFunc {
    pub fn new() -> Self {
        Self {
            signature: Signature::variadic_any(Volatility::Immutable),
        }
    }
}

impl ScalarUDFImpl for ToUnixtimeFunc {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "to_unixtime"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Int64)
    }

    fn invoke(&self, args: &[ColumnarValue]) -> Result<ColumnarValue> {
        if args.is_empty() {
            return exec_err!("to_unixtime function requires 1 or more arguments, got 0");
        }

        // validate that any args after the first one are Utf8
        if args.len() > 1 {
            validate_data_types(args, "to_unixtime")?;
        }

        match args[0].data_type() {
            DataType::Int32 | DataType::Int64 | DataType::Null | DataType::Float64 => {
                args[0].cast_to(&DataType::Int64, None)
            }
            DataType::Date64 | DataType::Date32 | DataType::Timestamp(_, None) => args[0]
                .cast_to(&DataType::Timestamp(TimeUnit::Second, None), None)?
                .cast_to(&DataType::Int64, None),
            DataType::Utf8 => ToTimestampSecondsFunc::new()
                .invoke(args)?
                .cast_to(&DataType::Int64, None),
            other => {
                exec_err!("Unsupported data type {:?} for function to_unixtime", other)
            }
        }
    }
    fn documentation(&self) -> Option<&Documentation> {
        Some(get_to_unixtime_doc())
    }
}

static DOCUMENTATION: OnceLock<Documentation> = OnceLock::new();

fn get_to_unixtime_doc() -> &'static Documentation {
    DOCUMENTATION.get_or_init(|| {
        Documentation::builder()
            .with_doc_section(DOC_SECTION_DATETIME)
            .with_description("Converts a value to seconds since the unix epoch (`1970-01-01T00:00:00Z`). Supports strings, dates, timestamps and double types as input. Strings are parsed as RFC3339 (e.g. '2023-07-20T05:44:00') if no [Chrono formats](https://docs.rs/chrono/latest/chrono/format/strftime/index.html) are provided.")
            .with_syntax_example("to_unixtime(expression[, ..., format_n])")
            .with_argument(
                "expression",
                "Expression to operate on. Can be a constant, column, or function, and any combination of arithmetic operators."
            ).with_argument(
            "format_n",
            "Optional [Chrono format](https://docs.rs/chrono/latest/chrono/format/strftime/index.html) strings to use to parse the expression. Formats will be tried in the order they appear with the first successful one being returned. If none of the formats successfully parse the expression an error will be returned.")
            .with_sql_example(r#"
```sql
> select to_unixtime('2020-09-08T12:00:00+00:00');
+------------------------------------------------+
| to_unixtime(Utf8("2020-09-08T12:00:00+00:00")) |
+------------------------------------------------+
| 1599566400                                     |
+------------------------------------------------+
> select to_unixtime('01-14-2023 01:01:30+05:30', '%q', '%d-%m-%Y %H/%M/%S', '%+', '%m-%d-%Y %H:%M:%S%#z');
+-----------------------------------------------------------------------------------------------------------------------------+
| to_unixtime(Utf8("01-14-2023 01:01:30+05:30"),Utf8("%q"),Utf8("%d-%m-%Y %H/%M/%S"),Utf8("%+"),Utf8("%m-%d-%Y %H:%M:%S%#z")) |
+-----------------------------------------------------------------------------------------------------------------------------+
| 1673638290                                                                                                                  |
+-----------------------------------------------------------------------------------------------------------------------------+
```
"#)
            .build()
            .unwrap()
    })
}
