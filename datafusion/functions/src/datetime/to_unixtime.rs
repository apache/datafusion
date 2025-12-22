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
use datafusion_common::{Result, exec_err, internal_err, plan_err};
use datafusion_expr::simplify::{ExprSimplifyResult, SimplifyInfo};
use datafusion_expr::{
    ColumnarValue, Documentation, Expr, ScalarUDFImpl, Signature, Volatility, cast,
};
use datafusion_macros::user_doc;
use std::any::Any;

#[user_doc(
    doc_section(label = "Time and Date Functions"),
    description = "Converts a value to seconds since the unix epoch (`1970-01-01T00:00:00Z`). Supports strings, dates, timestamps, integer, unsigned integer, float, and decimal types as input. Strings are parsed as RFC3339 (e.g. '2023-07-20T05:44:00') if no [Chrono formats](https://docs.rs/chrono/latest/chrono/format/strftime/index.html) are provided. Integers, unsigned integers, floats, and decimals are interpreted as seconds since the unix epoch (`1970-01-01T00:00:00Z`).",
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
            signature: Signature::user_defined(Volatility::Immutable),
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

        match arg_args[0].data_type() {
            DataType::Int64 => Ok(arg_args[0].clone()),
            DataType::Null => arg_args[0].cast_to(&DataType::Int64, None),
            DataType::Timestamp(_, _) => arg_args[0].cast_to(&DataType::Int64, None),
            DataType::Utf8View | DataType::LargeUtf8 | DataType::Utf8 => internal_err!(
                "to_unixtime should have been simplified to to_timestamp_seconds"
            ),
            other => {
                exec_err!("Unsupported data type {} for function to_unixtime", other)
            }
        }
    }

    fn simplify(
        &self,
        args: Vec<Expr>,
        info: &dyn SimplifyInfo,
    ) -> Result<ExprSimplifyResult> {
        if args.is_empty() {
            return plan_err!("to_unixtime function requires 1 or more arguments, got 0");
        }

        let input_type = info.get_data_type(&args[0])?;
        match input_type {
            DataType::Utf8View | DataType::LargeUtf8 | DataType::Utf8 => {
                Ok(ExprSimplifyResult::Simplified(cast(
                    super::to_timestamp_seconds().call(args),
                    DataType::Int64,
                )))
            }
            _ => Ok(ExprSimplifyResult::Original(args)),
        }
    }

    fn coerce_types(&self, arg_types: &[DataType]) -> Result<Vec<DataType>> {
        if arg_types.is_empty() {
            return plan_err!("to_unixtime function requires 1 or more arguments, got 0");
        }

        // validate that any args after the first one are Utf8
        for (idx, data_type) in arg_types.iter().skip(1).enumerate() {
            match data_type {
                DataType::Utf8View | DataType::LargeUtf8 | DataType::Utf8 => {
                    // all good
                }
                _ => {
                    return plan_err!(
                        "to_unixtime function unsupported data type at index {}: {}",
                        idx + 1,
                        data_type
                    );
                }
            }
        }

        let coerced_first = match &arg_types[0] {
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
            | DataType::Decimal128(_, _)
            | DataType::Decimal256(_, _)
            | DataType::Null => DataType::Int64,
            DataType::Date32 | DataType::Date64 => {
                DataType::Timestamp(TimeUnit::Second, None)
            }
            DataType::Timestamp(_, tz) => {
                DataType::Timestamp(TimeUnit::Second, tz.clone())
            }
            DataType::Utf8View | DataType::LargeUtf8 | DataType::Utf8 => {
                arg_types[0].clone()
            }
            other => {
                return plan_err!(
                    "Unsupported data type {} for function to_unixtime",
                    other
                );
            }
        };

        Ok(std::iter::once(coerced_first)
            .chain(arg_types.iter().skip(1).cloned())
            .collect())
    }

    fn documentation(&self) -> Option<&Documentation> {
        self.doc()
    }
}
