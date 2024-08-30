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

use arrow::array::types::Date32Type;
use arrow::compute::kernels::cast_utils::Parser;
use arrow::datatypes::DataType;
use arrow::datatypes::DataType::Date32;

use crate::datetime::common::*;
use datafusion_common::{exec_err, internal_datafusion_err, Result};
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
                // For most cases 'Date32Type::parse' would also work here, as it semantically parses
                // assuming a YYYY-MM-DD format. However 'Date32Type::parse' cannot handle negative
                // values for year (e.g. years in BC). Date32Type::parse_formatted can handle also these.
                |s| match Date32Type::parse_formatted(s, "%Y-%m-%d") {
                    Some(v) => Ok(v),
                    None => Err(internal_datafusion_err!(
                        "Unable to cast to Date32 for converting from i64 to i32 failed"
                    )),
                },
                "to_date",
            ),
            2.. => handle_multiple::<Date32Type, _, Date32Type, _>(
                args,
                |s, format| match Date32Type::parse_formatted(s, format) {
                    Some(v) => Ok(v),
                    None => Err(internal_datafusion_err!(
                        "Unable to cast to Date32 for converting from i64 to i32 failed"
                    )),
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
                format_str: "%Y#%m#%d",
                formatted_date: "-42#01#01",
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
}
