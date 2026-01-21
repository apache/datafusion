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
use arrow::datatypes::{DataType, Date32Type, Field, FieldRef};
use chrono::{Datelike, Duration, NaiveDate};
use datafusion_common::utils::take_function_args;
use datafusion_common::{Result, ScalarValue, exec_datafusion_err, internal_err};
use datafusion_expr::{
    ColumnarValue, ReturnFieldArgs, ScalarFunctionArgs, ScalarUDFImpl, Signature,
    Volatility,
};

#[derive(Debug, PartialEq, Eq, Hash)]
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
        internal_err!("return_field_from_args should be used instead")
    }

    fn return_field_from_args(&self, args: ReturnFieldArgs) -> Result<FieldRef> {
        let Some(field) = args.arg_fields.first() else {
            return internal_err!("Spark `last_day` expects exactly one argument");
        };

        Ok(Arc::new(Field::new(
            self.name(),
            DataType::Date32,
            field.is_nullable(),
        )))
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let ScalarFunctionArgs { args, .. } = args;
        let [arg] = take_function_args("last_day", args)?;
        match arg {
            ColumnarValue::Scalar(ScalarValue::Date32(days)) => {
                if let Some(days) = days {
                    Ok(ColumnarValue::Scalar(ScalarValue::Date32(Some(
                        spark_last_day(days)?,
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
                        internal_err!(
                            "Unsupported data type {other:?} for Spark function `last_day`"
                        )
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::function::utils::test::test_scalar_function;
    use arrow::array::{Array, Date32Array};
    use arrow::datatypes::Field;
    use datafusion_common::ScalarValue;
    use datafusion_expr::{ColumnarValue, ReturnFieldArgs};

    #[test]
    fn test_last_day_nullability_matches_input() {
        let func = SparkLastDay::new();

        let non_nullable_arg = Arc::new(Field::new("arg", DataType::Date32, false));
        let nullable_arg = Arc::new(Field::new("arg", DataType::Date32, true));

        let non_nullable_out = func
            .return_field_from_args(ReturnFieldArgs {
                arg_fields: &[Arc::clone(&non_nullable_arg)],
                scalar_arguments: &[None],
            })
            .expect("non-nullable arg should succeed");
        assert_eq!(non_nullable_out.data_type(), &DataType::Date32);
        assert!(!non_nullable_out.is_nullable());

        let nullable_out = func
            .return_field_from_args(ReturnFieldArgs {
                arg_fields: &[Arc::clone(&nullable_arg)],
                scalar_arguments: &[None],
            })
            .expect("nullable arg should succeed");
        assert_eq!(nullable_out.data_type(), &DataType::Date32);
        assert!(nullable_out.is_nullable());
    }

    #[test]
    fn test_last_day_scalar_evaluation() {
        test_scalar_function!(
            SparkLastDay::new(),
            vec![ColumnarValue::Scalar(ScalarValue::Date32(Some(0)))],
            Ok(Some(30)),
            i32,
            DataType::Date32,
            Date32Array
        );

        test_scalar_function!(
            SparkLastDay::new(),
            vec![ColumnarValue::Scalar(ScalarValue::Date32(None))],
            Ok(None),
            i32,
            DataType::Date32,
            Date32Array
        );
    }
}
