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
use datafusion_expr::sort_properties::{ExprProperties, SortProperties};
use datafusion_expr::{
    ColumnarValue, Documentation, ScalarFunctionArgs, ScalarUDFImpl, Signature,
    Volatility,
};
use datafusion_macros::user_doc;

#[user_doc(
    doc_section(label = "Time and Date Functions"),
    description = r#"
Converts a value to seconds since the unix epoch (`1970-01-01T00:00:00`).
Supports strings, dates, timestamps, integer, unsigned integer, and float types as input.
Strings are parsed as RFC3339 (e.g. '2023-07-20T05:44:00')
if no [Chrono formats](https://docs.rs/chrono/latest/chrono/format/strftime/index.html) are provided.
Integers, unsigned integers, and floats are interpreted as seconds since the unix epoch (`1970-01-01T00:00:00`)."#,
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
        description = "Optional [Chrono format](https://docs.rs/chrono/latest/chrono/format/strftime/index.html) strings to use to parse the expression. Formats will be tried in the order they appear with the first successful one being returned. If none of the formats successfully parse the expression an error will be returned. NULL formats are skipped. If every format is NULL the result is NULL."
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
    fn name(&self) -> &str {
        "to_unixtime"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Int64)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
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

    fn output_ordering(&self, input: &[ExprProperties]) -> Result<SortProperties> {
        if let [value] = input
            && (value.range.data_type().is_integer()
                || matches!(
                    value.range.data_type(),
                    DataType::Date32 | DataType::Date64 | DataType::Timestamp(_, _)
                ))
        {
            // Conversions preserve epoch order and report overflow instead of adding NULLs.
            Ok(value.sort_properties)
        } else {
            Ok(SortProperties::Unordered)
        }
    }

    fn documentation(&self) -> Option<&Documentation> {
        self.doc()
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow::array::{Array, Int64Array};
    use arrow::compute::{SortOptions, sort};
    use arrow::datatypes::DataType::*;
    use datafusion_common::{config::ConfigOptions, datatype::DataTypeExt};
    use datafusion_expr::interval_arithmetic::Interval;

    use super::*;

    #[test]
    fn ordering_matches_runtime_conversion() -> Result<()> {
        let function = ToUnixtimeFunc::new();
        let mut cases = vec![
            (Int8, true),
            (Int16, true),
            (Int32, true),
            (Int64, true),
            (UInt8, true),
            (UInt16, true),
            (UInt32, true),
            (UInt64, true),
            (Date32, true),
            (Date64, true),
            (Float64, false),
            (Decimal128(20, 0), false),
            (Utf8, false),
            (Null, false),
        ];
        for unit in [
            TimeUnit::Second,
            TimeUnit::Millisecond,
            TimeUnit::Microsecond,
            TimeUnit::Nanosecond,
        ] {
            for timezone in [
                None,
                Some("UTC".into()),
                Some("+05:30".into()),
                Some("America/New_York".into()),
            ] {
                cases.push((Timestamp(unit, timezone), true));
            }
        }
        for (data_type, supported) in cases {
            for (descending, nulls_first) in
                [(false, false), (false, true), (true, false), (true, true)]
            {
                let options = SortOptions {
                    descending,
                    nulls_first,
                };
                let ordering = SortProperties::Ordered(options);
                let input = ExprProperties::new_unknown()
                    .with_order(ordering)
                    .with_range(Interval::make_unbounded(&data_type)?);
                assert_eq!(
                    function.output_ordering(&[input])?,
                    if supported {
                        ordering
                    } else {
                        SortProperties::Unordered
                    }
                );
                if !supported || data_type.is_integer() {
                    continue;
                }

                let (min, max) = if data_type == Date32 {
                    (i32::MIN as i64, i32::MAX as i64)
                } else {
                    (i64::MIN, i64::MAX)
                };
                let values = Int64Array::from(vec![
                    Some(min),
                    Some(-1),
                    Some(0),
                    Some(1),
                    Some(max),
                    None,
                ]);
                let argument = ColumnarValue::Array(sort(&values, Some(options))?)
                    .cast_to(&data_type, None)?;
                let result = function
                    .invoke_with_args(ScalarFunctionArgs {
                        args: vec![argument],
                        arg_fields: vec![data_type.clone().into_nullable_field_ref()],
                        number_rows: values.len(),
                        return_field: Int64.into_nullable_field_ref(),
                        config_options: Arc::new(ConfigOptions::default()),
                    })?
                    .to_array(values.len())?;
                assert_eq!(
                    result.as_ref(),
                    sort(result.as_ref(), Some(options))?.as_ref()
                );
                assert_eq!(result.null_count(), values.null_count());
            }
        }
        let input = ExprProperties::new_unknown()
            .with_order(SortProperties::Ordered(SortOptions::default()))
            .with_range(Interval::make_unbounded(&Int64)?);
        assert_eq!(
            function.output_ordering(&[input.clone(), input])?,
            SortProperties::Unordered
        );
        assert_eq!(function.output_ordering(&[])?, SortProperties::Unordered);
        Ok(())
    }
}
