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

use crate::datetime::common::*;
use arrow::datatypes::DataType;
use arrow::datatypes::DataType::*;
use arrow::error::ArrowError::ParseError;
use arrow::{array::types::Date32Type, compute::kernels::cast_utils::Parser};
use datafusion_common::{arrow_err, exec_err, internal_datafusion_err, Result};
use datafusion_expr::{
    ColumnarValue, Documentation, ScalarUDFImpl, Signature, Volatility,
};
use datafusion_macros::user_doc;
use std::any::Any;

#[user_doc(
    doc_section(label = "Time and Date Functions"),
    description = r"Converts a value to a date (`YYYY-MM-DD`).
Supports strings, integer and double types as input.
Strings are parsed as YYYY-MM-DD (e.g. '2023-07-20') if no [Chrono format](https://docs.rs/chrono/latest/chrono/format/strftime/index.html)s are provided.
Integers and doubles are interpreted as days since the unix epoch (`1970-01-01T00:00:00Z`).
Returns the corresponding date.

Note: `to_date` returns Date32, which represents its values as the number of days since unix epoch(`1970-01-01`) stored as signed 32 bit value. The largest supported date value is `9999-12-31`.",
    syntax_example = "to_date('2017-05-31', '%Y-%m-%d')",
    sql_example = r#"```sql
> select to_date('2023-01-31');
+-------------------------------+
| to_date(Utf8("2023-01-31")) |
+-------------------------------+
| 2023-01-31                    |
+-------------------------------+
> select to_date('2023/01/31', '%Y-%m-%d', '%Y/%m/%d');
+---------------------------------------------------------------------+
| to_date(Utf8("2023/01/31"),Utf8("%Y-%m-%d"),Utf8("%Y/%m/%d")) |
+---------------------------------------------------------------------+
| 2023-01-31                                                          |
+---------------------------------------------------------------------+
```

Additional examples can be found [here](https://github.com/apache/datafusion/blob/main/datafusion-examples/examples/builtin_functions/date_time.rs)
"#,
    standard_argument(name = "expression", prefix = "String"),
    argument(
        name = "format_n",
        description = r"Optional [Chrono format](https://docs.rs/chrono/latest/chrono/format/strftime/index.html) strings to use to parse the expression. Formats will be tried in the order
  they appear with the first successful one being returned. If none of the formats successfully parse the expression
  an error will be returned."
    )
)]
#[derive(Debug, PartialEq, Eq, Hash)]
pub struct ToDateFunc {
    signature: Signature,
}

impl Default for ToDateFunc {
    fn default() -> Self {
        Self::new()
    }
}

impl ToDateFunc {
    pub fn new() -> Self {
        Self {
            signature: Signature::variadic_any(Volatility::Immutable),
        }
    }

    fn to_date(&self, args: &[ColumnarValue]) -> Result<ColumnarValue> {
        match args.len() {
            1 => handle::<Date32Type, _, Date32Type>(
                args,
                |s| match Date32Type::parse(s) {
                    Some(v) => Ok(v),
                    None => arrow_err!(ParseError(
                        "Unable to cast to Date32 for converting from i64 to i32 failed"
                            .to_string()
                    )),
                },
                "to_date",
            ),
            2.. => handle_multiple::<Date32Type, _, Date32Type, _>(
                args,
                |s, format| {
                    string_to_timestamp_millis_formatted(s, format)
                        .map(|n| n / (24 * 60 * 60 * 1_000))
                        .and_then(|v| {
                            v.try_into().map_err(|_| {
                                internal_datafusion_err!("Unable to cast to Date32 for converting from i64 to i32 failed")
                            })
                        })
                },
                |n| n,
                "to_date",
            ),
            0 => exec_err!("Unsupported 0 argument count for function to_date"),
        }
    }
}

impl ScalarUDFImpl for ToDateFunc {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "to_date"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(Date32)
    }

    fn invoke_with_args(
        &self,
        args: datafusion_expr::ScalarFunctionArgs,
    ) -> Result<ColumnarValue> {
        let args = args.args;
        if args.is_empty() {
            return exec_err!("to_date function requires 1 or more arguments, got 0");
        }

        // validate that any args after the first one are Utf8
        if args.len() > 1 {
            validate_data_types(&args, "to_date")?;
        }

        match args[0].data_type() {
            Int32 | Int64 | Null | Float64 | Date32 | Date64 => {
                args[0].cast_to(&Date32, None)
            }
            Utf8View | LargeUtf8 | Utf8 => self.to_date(&args),
            other => {
                exec_err!("Unsupported data type {} for function to_date", other)
            }
        }
    }

    fn documentation(&self) -> Option<&Documentation> {
        self.doc()
    }
}

#[cfg(test)]
mod tests {
    use super::ToDateFunc;
    use arrow::array::{Array, Date32Array, GenericStringArray, StringViewArray};
    use arrow::datatypes::{DataType, Field};
    use arrow::{compute::kernels::cast_utils::Parser, datatypes::Date32Type};
    use datafusion_common::config::ConfigOptions;
    use datafusion_common::{DataFusionError, ScalarValue};
    use datafusion_expr::{ColumnarValue, ScalarUDFImpl};
    use std::sync::Arc;

    fn invoke_to_date_with_args(
        args: Vec<ColumnarValue>,
        number_rows: usize,
    ) -> Result<ColumnarValue, DataFusionError> {
        let arg_fields = args
            .iter()
            .map(|arg| Field::new("a", arg.data_type(), true).into())
            .collect::<Vec<_>>();

        let args = datafusion_expr::ScalarFunctionArgs {
            args,
            arg_fields,
            number_rows,
            return_field: Field::new("f", DataType::Date32, true).into(),
            config_options: Arc::new(ConfigOptions::default()),
        };
        ToDateFunc::new().invoke_with_args(args)
    }

    #[test]
    fn test_to_date_without_format() {
        struct TestCase {
            name: &'static str,
            date_str: &'static str,
        }

        let test_cases = vec![
            TestCase {
                name: "Largest four-digit year (9999)",
                date_str: "9999-12-31",
            },
            TestCase {
                name: "Year 1 (0001)",
                date_str: "0001-12-31",
            },
            TestCase {
                name: "Year before epoch (1969)",
                date_str: "1969-01-01",
            },
            TestCase {
                name: "Switch Julian/Gregorian calendar (1582-10-10)",
                date_str: "1582-10-10",
            },
        ];

        for tc in &test_cases {
            test_scalar(ScalarValue::Utf8(Some(tc.date_str.to_string())), tc);
            test_scalar(ScalarValue::LargeUtf8(Some(tc.date_str.to_string())), tc);
            test_scalar(ScalarValue::Utf8View(Some(tc.date_str.to_string())), tc);

            test_array::<GenericStringArray<i32>>(tc);
            test_array::<GenericStringArray<i64>>(tc);
            test_array::<StringViewArray>(tc);
        }

        fn test_scalar(sv: ScalarValue, tc: &TestCase) {
            let to_date_result =
                invoke_to_date_with_args(vec![ColumnarValue::Scalar(sv)], 1);

            match to_date_result {
                Ok(ColumnarValue::Scalar(ScalarValue::Date32(date_val))) => {
                    let expected = Date32Type::parse_formatted(tc.date_str, "%Y-%m-%d");
                    assert_eq!(
                        date_val, expected,
                        "{}: to_date created wrong value",
                        tc.name
                    );
                }
                _ => panic!("Could not convert '{}' to Date", tc.date_str),
            }
        }

        fn test_array<A>(tc: &TestCase)
        where
            A: From<Vec<&'static str>> + Array + 'static,
        {
            let date_array = A::from(vec![tc.date_str]);
            let batch_len = date_array.len();
            let to_date_result = invoke_to_date_with_args(
                vec![ColumnarValue::Array(Arc::new(date_array))],
                batch_len,
            );

            match to_date_result {
                Ok(ColumnarValue::Array(a)) => {
                    assert_eq!(a.len(), 1);

                    let expected = Date32Type::parse_formatted(tc.date_str, "%Y-%m-%d");
                    let mut builder = Date32Array::builder(4);
                    builder.append_value(expected.unwrap());

                    assert_eq!(
                        &builder.finish() as &dyn Array,
                        a.as_ref(),
                        "{}: to_date created wrong value",
                        tc.name
                    );
                }
                _ => panic!("Could not convert '{}' to Date", tc.date_str),
            }
        }
    }

    #[test]
    fn test_to_date_with_format() {
        struct TestCase {
            name: &'static str,
            date_str: &'static str,
            format_str: &'static str,
            formatted_date: &'static str,
        }

        let test_cases = vec![
            TestCase {
                name: "Largest four-digit year (9999)",
                date_str: "9999-12-31",
                format_str: "%Y%m%d",
                formatted_date: "99991231",
            },
            TestCase {
                name: "Smallest four-digit year (-9999)",
                date_str: "-9999-12-31",
                format_str: "%Y/%m/%d",
                formatted_date: "-9999/12/31",
            },
            TestCase {
                name: "Year 1 (0001)",
                date_str: "0001-12-31",
                format_str: "%Y%m%d",
                formatted_date: "00011231",
            },
            TestCase {
                name: "Year before epoch (1969)",
                date_str: "1969-01-01",
                format_str: "%Y%m%d",
                formatted_date: "19690101",
            },
            TestCase {
                name: "Switch Julian/Gregorian calendar (1582-10-10)",
                date_str: "1582-10-10",
                format_str: "%Y%m%d",
                formatted_date: "15821010",
            },
            TestCase {
                name: "Negative Year, BC (-42-01-01)",
                date_str: "-42-01-01",
                format_str: "%Y/%m/%d",
                formatted_date: "-42/01/01",
            },
        ];

        for tc in &test_cases {
            test_scalar(ScalarValue::Utf8(Some(tc.formatted_date.to_string())), tc);
            test_scalar(
                ScalarValue::LargeUtf8(Some(tc.formatted_date.to_string())),
                tc,
            );
            test_scalar(
                ScalarValue::Utf8View(Some(tc.formatted_date.to_string())),
                tc,
            );

            test_array::<GenericStringArray<i32>>(tc);
            test_array::<GenericStringArray<i64>>(tc);
            test_array::<StringViewArray>(tc);
        }

        fn test_scalar(sv: ScalarValue, tc: &TestCase) {
            let format_scalar = ScalarValue::Utf8(Some(tc.format_str.to_string()));

            let to_date_result = invoke_to_date_with_args(
                vec![
                    ColumnarValue::Scalar(sv),
                    ColumnarValue::Scalar(format_scalar),
                ],
                1,
            );

            match to_date_result {
                Ok(ColumnarValue::Scalar(ScalarValue::Date32(date_val))) => {
                    let expected = Date32Type::parse_formatted(tc.date_str, "%Y-%m-%d");
                    assert_eq!(date_val, expected, "{}: to_date created wrong value for date '{}' with format string '{}'", tc.name, tc.formatted_date, tc.format_str);
                }
                _ => panic!(
                    "Could not convert '{}' with format string '{}'to Date",
                    tc.date_str, tc.format_str
                ),
            }
        }

        fn test_array<A>(tc: &TestCase)
        where
            A: From<Vec<&'static str>> + Array + 'static,
        {
            let date_array = A::from(vec![tc.formatted_date]);
            let format_array = A::from(vec![tc.format_str]);
            let batch_len = date_array.len();

            let to_date_result = invoke_to_date_with_args(
                vec![
                    ColumnarValue::Array(Arc::new(date_array)),
                    ColumnarValue::Array(Arc::new(format_array)),
                ],
                batch_len,
            );

            match to_date_result {
                Ok(ColumnarValue::Array(a)) => {
                    assert_eq!(a.len(), 1);

                    let expected = Date32Type::parse_formatted(tc.date_str, "%Y-%m-%d");
                    let mut builder = Date32Array::builder(4);
                    builder.append_value(expected.unwrap());

                    assert_eq!(
                        &builder.finish() as &dyn Array, a.as_ref(),
                        "{}: to_date created wrong value for date '{}' with format string '{}'",
                        tc.name,
                        tc.formatted_date,
                        tc.format_str
                    );
                }
                _ => panic!(
                    "Could not convert '{}' with format string '{}'to Date: {:?}",
                    tc.formatted_date, tc.format_str, to_date_result
                ),
            }
        }
    }

    #[test]
    fn test_to_date_multiple_format_strings() {
        let formatted_date_scalar = ScalarValue::Utf8(Some("2023/01/31".into()));
        let format1_scalar = ScalarValue::Utf8(Some("%Y-%m-%d".into()));
        let format2_scalar = ScalarValue::Utf8(Some("%Y/%m/%d".into()));

        let to_date_result = invoke_to_date_with_args(
            vec![
                ColumnarValue::Scalar(formatted_date_scalar),
                ColumnarValue::Scalar(format1_scalar),
                ColumnarValue::Scalar(format2_scalar),
            ],
            1,
        );

        match to_date_result {
            Ok(ColumnarValue::Scalar(ScalarValue::Date32(date_val))) => {
                let expected = Date32Type::parse_formatted("2023-01-31", "%Y-%m-%d");
                assert_eq!(
                    date_val, expected,
                    "to_date created wrong value for date with 2 format strings"
                );
            }
            _ => panic!("Conversion failed",),
        }
    }

    #[test]
    fn test_to_date_from_timestamp() {
        let test_cases = vec![
            "2020-09-08T13:42:29Z",
            "2020-09-08T13:42:29.190855-05:00",
            "2020-09-08 12:13:29",
        ];
        for date_str in test_cases {
            let formatted_date_scalar = ScalarValue::Utf8(Some(date_str.into()));

            let to_date_result = invoke_to_date_with_args(
                vec![ColumnarValue::Scalar(formatted_date_scalar)],
                1,
            );

            match to_date_result {
                Ok(ColumnarValue::Scalar(ScalarValue::Date32(date_val))) => {
                    let expected = Date32Type::parse_formatted("2020-09-08", "%Y-%m-%d");
                    assert_eq!(date_val, expected, "to_date created wrong value");
                }
                _ => panic!("Conversion of {date_str} failed"),
            }
        }
    }

    #[test]
    fn test_to_date_string_with_valid_number() {
        let date_str = "20241231";
        let date_scalar = ScalarValue::Utf8(Some(date_str.into()));

        let to_date_result =
            invoke_to_date_with_args(vec![ColumnarValue::Scalar(date_scalar)], 1);

        match to_date_result {
            Ok(ColumnarValue::Scalar(ScalarValue::Date32(date_val))) => {
                let expected = Date32Type::parse_formatted("2024-12-31", "%Y-%m-%d");
                assert_eq!(
                    date_val, expected,
                    "to_date created wrong value for {date_str}"
                );
            }
            _ => panic!("Conversion of {date_str} failed"),
        }
    }

    #[test]
    fn test_to_date_string_with_invalid_number() {
        let date_str = "202412311";
        let date_scalar = ScalarValue::Utf8(Some(date_str.into()));

        let to_date_result =
            invoke_to_date_with_args(vec![ColumnarValue::Scalar(date_scalar)], 1);

        if let Ok(ColumnarValue::Scalar(ScalarValue::Date32(_))) = to_date_result {
            panic!("Conversion of {date_str} succeeded, but should have failed. ");
        }
    }
}
