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

use std::any::Any;

use arrow::datatypes::DataType;
use arrow::datatypes::DataType::Date32;
use arrow::error::ArrowError::ParseError;
use arrow::{array::types::Date32Type, compute::kernels::cast_utils::Parser};
use chrono::Utc;

use crate::datetime::common::*;
use datafusion_common::error::DataFusionError;
use datafusion_common::{arrow_err, exec_err, internal_datafusion_err, Result};
use datafusion_expr::{ColumnarValue, ScalarUDFImpl, Signature, Volatility};

#[derive(Debug)]
pub struct ToDateFunc {
    signature: Signature,
}

impl Default for ToDateFunc {
    fn default() -> Self {
        Self::new()
    }
}

fn string_to_timestamp_millis_formatted(s: &str, format: &str) -> Result<i64> {
    Ok(string_to_datetime_formatted(&Utc, s, format)?
        .naive_utc()
        .and_utc()
        .timestamp_millis())
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
                // Although not being speficied, previous implementations have supported parsing date values that are followed
                // by a time part separated either a whitespace ('2023-01-10 12:34:56.000') or a 'T' ('2023-01-10T12:34:56.000').
                // Parsing these strings with "%Y-%m-%d" will fail. Therefore the string is split and the first part is used for parsing.
                |s| {
                    let string_to_parse;
                    if s.find('T').is_some() {
                        string_to_parse = s.split('T').next();
                    } else {
                        string_to_parse = s.split_whitespace().next();
                    }
                    if string_to_parse.is_none() {
                        return Err(internal_datafusion_err!(
                            "Cannot cast emtpy string to Date32"
                        ));
                    }

                    // For most cases 'Date32Type::parse' would also work here, as it semantically parses
                    // assuming a YYYY-MM-DD format. However 'Date32Type::parse' cannot handle negative
                    // values for year (e.g. years in BC). Date32Type::parse_formatted can handle also these.
                    match Date32Type::parse_formatted(string_to_parse.unwrap(), "%Y-%m-%d") {
                    Some(v) => Ok(v),
                    None => arrow_err!(ParseError(
                        "Unable to cast to Date32 for converting from i64 to i32 failed".to_string()
                    )),
                }
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
}

#[cfg(test)]
mod tests {
    use arrow::{compute::kernels::cast_utils::Parser, datatypes::Date32Type};
    use datafusion_common::ScalarValue;
    use datafusion_expr::{ColumnarValue, ScalarUDFImpl};

    use super::ToDateFunc;

    #[test]
    fn test_to_date() {
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

        // Step 1: Test without format string
        for tc in &test_cases {
            let date_scalar = ScalarValue::Utf8(Some(tc.date_str.to_string()));
            let to_date_result =
                ToDateFunc::new().invoke(&[ColumnarValue::Scalar(date_scalar)]);

            match to_date_result {
                Ok(ColumnarValue::Scalar(ScalarValue::Date32(date_val))) => {
                    let expected = Date32Type::parse_formatted(&tc.date_str, "%Y-%m-%d");
                    assert_eq!(
                        date_val, expected,
                        "{}: to_date created wrong value",
                        tc.name
                    );
                }
                _ => panic!("Could not convert '{}' to Date", tc.date_str),
            }
        }

        // Step 2: Test with format string
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
                    let expected = Date32Type::parse_formatted(&tc.date_str, "%Y-%m-%d");
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
}
