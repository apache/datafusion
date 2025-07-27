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
use std::sync::Arc;

use arrow::array::{ArrayRef, AsArray, Date32Array};
use arrow::datatypes::{DataType, Date32Type};
use chrono::{Datelike, Duration, NaiveDate};
use datafusion_common::{exec_datafusion_err, internal_err, Result, ScalarValue};
use datafusion_expr::{
    ColumnarValue, ScalarFunctionArgs, ScalarUDFImpl, Signature, Volatility,
};

#[derive(Debug)]
pub struct SparkLastDay {
    signature: Signature,
}

impl Default for SparkLastDay {
    fn default() -> Self {
        Self::new()
    }
}

impl SparkLastDay {
    pub fn new() -> Self {
        Self {
            signature: Signature::exact(vec![DataType::Date32], Volatility::Immutable),
        }
    }
}

impl ScalarUDFImpl for SparkLastDay {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "last_day"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Date32)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let ScalarFunctionArgs { args, .. } = args;
        let [arg] = args.as_slice() else {
            return internal_err!(
                "Spark `last_day` function requires 1 argument, got {}",
                args.len()
            );
        };
        match arg {
            ColumnarValue::Scalar(ScalarValue::Date32(days)) => {
                if let Some(days) = days {
                    Ok(ColumnarValue::Scalar(ScalarValue::Date32(Some(
                        spark_last_day(*days)?,
                    ))))
                } else {
                    Ok(ColumnarValue::Scalar(ScalarValue::Date32(None)))
                }
            }
            ColumnarValue::Array(array) => {
                let result = match array.data_type() {
                    DataType::Date32 => {
                        let result: Date32Array = array
                            .as_primitive::<Date32Type>()
                            .try_unary(spark_last_day)?
                            .with_data_type(DataType::Date32);
                        Ok(Arc::new(result) as ArrayRef)
                    }
                    other => {
                        internal_err!("Unsupported data type {other:?} for Spark function `last_day`")
                    }
                }?;
                Ok(ColumnarValue::Array(result))
            }
            other => {
                internal_err!("Unsupported arg {other:?} for Spark function `last_day")
            }
        }
    }
}

fn spark_last_day(days: i32) -> Result<i32> {
    let date = Date32Type::to_naive_date(days);

    let (year, month) = (date.year(), date.month());
    let (next_year, next_month) = if month == 12 {
        (year + 1, 1)
    } else {
        (year, month + 1)
    };

    let first_day_next_month = NaiveDate::from_ymd_opt(next_year, next_month, 1)
        .ok_or_else(|| {
            exec_datafusion_err!(
                "Spark `last_day`: Unable to parse date from {next_year}, {next_month}, 1"
            )
        })?;

    Ok(Date32Type::from_naive_date(
        first_day_next_month - Duration::days(1),
    ))
}
