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
use arrow::datatypes::DataType::Date32;
use arrow::error::ArrowError::ParseError;
use arrow::{array::types::Date32Type, compute::kernels::cast_utils::Parser};
use datafusion_common::error::DataFusionError;
use datafusion_common::{arrow_err, exec_err, internal_datafusion_err, Result};
use datafusion_expr::scalar_doc_sections::DOC_SECTION_DATETIME;
use datafusion_expr::{
    ColumnarValue, Documentation, ScalarUDFImpl, Signature, Volatility,
};
use std::any::Any;
use std::sync::OnceLock;

#[derive(Debug)]
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

static DOCUMENTATION: OnceLock<Documentation> = OnceLock::new();

fn get_to_date_doc() -> &'static Documentation {
    DOCUMENTATION.get_or_init(|| {
        Documentation::builder()
            .with_doc_section(DOC_SECTION_DATETIME)
            .with_description(r#"Converts a value to a date (`YYYY-MM-DD`).
Supports strings, integer and double types as input.
Strings are parsed as YYYY-MM-DD (e.g. '2023-07-20') if no [Chrono format](https://docs.rs/chrono/latest/chrono/format/strftime/index.html)s are provided.
Integers and doubles are interpreted as days since the unix epoch (`1970-01-01T00:00:00Z`).
Returns the corresponding date.

Note: `to_date` returns Date32, which represents its values as the number of days since unix epoch(`1970-01-01`) stored as signed 32 bit value. The largest supported date value is `9999-12-31`.
"#)
            .with_syntax_example("to_date('2017-05-31', '%Y-%m-%d')")
            .with_sql_example(r#"```sql
> select to_date('2023-01-31');
+-----------------------------+
| to_date(Utf8("2023-01-31")) |
+-----------------------------+
| 2023-01-31                  |
+-----------------------------+
> select to_date('2023/01/31', '%Y-%m-%d', '%Y/%m/%d');
+---------------------------------------------------------------+
| to_date(Utf8("2023/01/31"),Utf8("%Y-%m-%d"),Utf8("%Y/%m/%d")) |
+---------------------------------------------------------------+
| 2023-01-31                                                    |
+---------------------------------------------------------------+
```

Additional examples can be found [here](https://github.com/apache/datafusion/blob/main/datafusion-examples/examples/to_date.rs)
"#)
            .with_standard_argument("expression", "String")
            .with_argument(
                "format_n",
                "Optional [Chrono format](https://docs.rs/chrono/latest/chrono/format/strftime/index.html) strings to use to parse the expression. Formats will be tried in the order
  they appear with the first successful one being returned. If none of the formats successfully parse the expression
  an error will be returned.",
            )
            .build()
            .unwrap()
    })
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

    fn invoke(&self, args: &[ColumnarValue]) -> Result<ColumnarValue> {
        if args.is_empty() {
            return exec_err!("to_date function requires 1 or more arguments, got 0");
        }

        // validate that any args after the first one are Utf8
        if args.len() > 1 {
            validate_data_types(args, "to_date")?;
        }

        match args[0].data_type() {
            DataType::Int32
            | DataType::Int64
            | DataType::Null
            | DataType::Float64
            | DataType::Date32
            | DataType::Date64 => args[0].cast_to(&DataType::Date32, None),
            DataType::Utf8 => self.to_date(args),
            other => {
                exec_err!("Unsupported data type {:?} for function to_date", other)
            }
        }
    }

    fn documentation(&self) -> Option<&Documentation> {
        Some(get_to_date_doc())
    }
}

#[cfg(test)]
mod tests {
    use arrow::{compute::kernels::cast_utils::Parser, datatypes::Date32Type};
    use datafusion_common::ScalarValue;
    use datafusion_expr::{ColumnarValue, ScalarUDFImpl};

    use super::ToDateFunc;

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
            let date_scalar = ScalarValue::Utf8(Some(tc.date_str.to_string()));
            let to_date_result =
                ToDateFunc::new().invoke(&[ColumnarValue::Scalar(date_scalar)]);

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
            let formatted_date_scalar =
                ScalarValue::Utf8(Some(tc.formatted_date.to_string()));
            let format_scalar = ScalarValue::Utf8(Some(tc.format_str.to_string()));

            let to_date_result = ToDateFunc::new().invoke(&[
                ColumnarValue::Scalar(formatted_date_scalar),
                ColumnarValue::Scalar(format_scalar),
            ]);

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
    }

    #[test]
    fn test_to_date_multiple_format_strings() {
        let formatted_date_scalar = ScalarValue::Utf8(Some("2023/01/31".into()));
        let format1_scalar = ScalarValue::Utf8(Some("%Y-%m-%d".into()));
        let format2_scalar = ScalarValue::Utf8(Some("%Y/%m/%d".into()));

        let to_date_result = ToDateFunc::new().invoke(&[
            ColumnarValue::Scalar(formatted_date_scalar),
            ColumnarValue::Scalar(format1_scalar),
            ColumnarValue::Scalar(format2_scalar),
        ]);

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

            let to_date_result =
                ToDateFunc::new().invoke(&[ColumnarValue::Scalar(formatted_date_scalar)]);

            match to_date_result {
                Ok(ColumnarValue::Scalar(ScalarValue::Date32(date_val))) => {
                    let expected = Date32Type::parse_formatted("2020-09-08", "%Y-%m-%d");
                    assert_eq!(date_val, expected, "to_date created wrong value");
                }
                _ => panic!("Conversion of {} failed", date_str),
            }
        }
    }

    #[test]
    fn test_to_date_string_with_valid_number() {
        let date_str = "20241231";
        let date_scalar = ScalarValue::Utf8(Some(date_str.into()));

        let to_date_result =
            ToDateFunc::new().invoke(&[ColumnarValue::Scalar(date_scalar)]);

        match to_date_result {
            Ok(ColumnarValue::Scalar(ScalarValue::Date32(date_val))) => {
                let expected = Date32Type::parse_formatted("2024-12-31", "%Y-%m-%d");
                assert_eq!(
                    date_val, expected,
                    "to_date created wrong value for {}",
                    date_str
                );
            }
            _ => panic!("Conversion of {} failed", date_str),
        }
    }

    #[test]
    fn test_to_date_string_with_invalid_number() {
        let date_str = "202412311";
        let date_scalar = ScalarValue::Utf8(Some(date_str.into()));

        let to_date_result =
            ToDateFunc::new().invoke(&[ColumnarValue::Scalar(date_scalar)]);

        if let Ok(ColumnarValue::Scalar(ScalarValue::Date32(_))) = to_date_result {
            panic!(
                "Conversion of {} succeded, but should have failed, ",
                date_str
            );
        }
    }
}
