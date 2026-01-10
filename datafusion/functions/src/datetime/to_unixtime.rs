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

use super::to_timestamp::ToTimestampSecondsFunc;
use crate::datetime::common::*;
use arrow::datatypes::{DataType, TimeUnit};
use datafusion_common::{Result, exec_err};
use datafusion_expr::{
    ColumnarValue, Documentation, ScalarUDFImpl, Signature, Volatility,
};
use datafusion_macros::user_doc;
use std::any::Any;

#[user_doc(
    doc_section(label = "Time and Date Functions"),
    description = "Converts a value to seconds since the unix epoch (`1970-01-01T00:00:00`). Supports strings, dates, timestamps, integer, unsigned integer, and float types as input. Strings are parsed as RFC3339 (e.g. '2023-07-20T05:44:00') if no [Chrono formats](https://docs.rs/chrono/latest/chrono/format/strftime/index.html) are provided. Integers, unsigned integers, and floats are interpreted as seconds since the unix epoch (`1970-01-01T00:00:00`).",
    syntax_example = "to_unixtime(expression[, ..., format_n])",
    sql_example = r#"
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
"#,
    argument(
        name = "expression",
        description = "Expression to operate on. Can be a constant, column, or function, and any combination of arithmetic operators."
    ),
    argument(
        name = "format_n",
        description = "Optional [Chrono format](https://docs.rs/chrono/latest/chrono/format/strftime/index.html) strings to use to parse the expression. Formats will be tried in the order they appear with the first successful one being returned. If none of the formats successfully parse the expression an error will be returned."
    )
)]
#[derive(Debug, PartialEq, Eq, Hash)]
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

    fn invoke_with_args(
        &self,
        args: datafusion_expr::ScalarFunctionArgs,
    ) -> Result<ColumnarValue> {
        let arg_args = &args.args;
        if arg_args.is_empty() {
            return exec_err!("to_unixtime function requires 1 or more arguments, got 0");
        }

        // validate that any args after the first one are Utf8
        if arg_args.len() > 1 {
            // Format arguments only make sense for string inputs
            match arg_args[0].data_type() {
                DataType::Utf8View | DataType::LargeUtf8 | DataType::Utf8 => {
                    validate_data_types(arg_args, "to_unixtime")?;
                }
                _ => {
                    return exec_err!(
                        "to_unixtime function only accepts format arguments with string input, got {} arguments",
                        arg_args.len()
                    );
                }
            }
        }

        match arg_args[0].data_type() {
            DataType::Int8
            | DataType::Int16
            | DataType::Int32
            | DataType::Int64
            | DataType::UInt8
            | DataType::UInt16
            | DataType::UInt32
            | DataType::UInt64
            | DataType::Float16
            | DataType::Float32
            | DataType::Float64
            | DataType::Null => arg_args[0].cast_to(&DataType::Int64, None),
            DataType::Date64 | DataType::Date32 => arg_args[0]
                .cast_to(&DataType::Timestamp(TimeUnit::Second, None), None)?
                .cast_to(&DataType::Int64, None),
            DataType::Timestamp(_, tz) => arg_args[0]
                .cast_to(&DataType::Timestamp(TimeUnit::Second, tz), None)?
                .cast_to(&DataType::Int64, None),
            DataType::Utf8View | DataType::LargeUtf8 | DataType::Utf8 => {
                ToTimestampSecondsFunc::new_with_config(args.config_options.as_ref())
                    .invoke_with_args(args)?
                    .cast_to(&DataType::Int64, None)
            }
            other => {
                exec_err!("Unsupported data type {} for function to_unixtime", other)
            }
        }
    }

    fn documentation(&self) -> Option<&Documentation> {
        self.doc()
    }
}
