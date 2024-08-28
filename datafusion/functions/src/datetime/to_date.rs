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
use arrow::compute::kernels::cast_utils::string_to_datetime;
use arrow::datatypes::DataType;
use arrow::datatypes::DataType::Date32;
use chrono::Utc;

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

pub fn string_to_timestamp_millis(s: &str) -> Result<i64> {
    Ok(string_to_datetime(&Utc, s)?
        .naive_utc()
        .and_utc()
        .timestamp_millis())
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
                |s| {
                    string_to_timestamp_millis(s)
                        .map(|n| n / (24 * 60 * 60 * 1_000))
                        .and_then(|v| {
                            v.try_into().map_err(|_| {
                                internal_datafusion_err!("Unable to cast to Date32 for converting from i64 to i32 failed")
                            })
                        })
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
    use datafusion_common::ScalarValue;
    use datafusion_expr::{ColumnarValue, ScalarUDFImpl};

    use super::ToDateFunc;

    #[test]
    fn test_year_9999() {
        let date_str = "9999-12-31";
        let date_scalar = ScalarValue::Utf8(Some(date_str.to_string()));

        let res = ToDateFunc::new().invoke(&[ColumnarValue::Scalar(date_scalar)]);

        assert!(res.is_ok(), "Could not convert '{}' to Date", date_str);
    }

    #[test]
    fn test_year_9999_formatted() {
        let date_str = "99991231";
        let format_str = "%Y%m%d";
        let date_scalar = ScalarValue::Utf8(Some(date_str.to_string()));
        let format_scalar = ScalarValue::Utf8(Some(format_str.to_string()));

        let res = ToDateFunc::new().invoke(&[
            ColumnarValue::Scalar(date_scalar),
            ColumnarValue::Scalar(format_scalar),
        ]);

        assert!(
            res.is_ok(),
            "Could not convert '{}' with fornat string '{}'to Date",
            date_str,
            format_str
        );
    }
}
