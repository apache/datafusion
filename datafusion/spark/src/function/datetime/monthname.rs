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

use std::sync::Arc;

use arrow::array::{AsArray, StringArray};
use arrow::compute::{DatePart, date_part};
use arrow::datatypes::{DataType, Field, FieldRef};
use datafusion_common::utils::take_function_args;
use datafusion_common::{Result, ScalarValue, exec_err, internal_err};
use datafusion_expr::{
    Coercion, ColumnarValue, ReturnFieldArgs, ScalarFunctionArgs, ScalarUDFImpl,
    Signature, TypeSignatureClass, Volatility,
};

const MONTH_NAMES: [&str; 12] = [
    "Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec",
];

fn month_number_to_name(month: i32) -> Option<&'static str> {
    MONTH_NAMES.get((month - 1) as usize).copied()
}

/// Spark-compatible `monthname` expression.
/// Returns the three-letter abbreviated month name from a date or timestamp.
///
/// <https://spark.apache.org/docs/latest/api/sql/index.html#monthname>
#[derive(Debug, PartialEq, Eq, Hash)]
pub struct SparkMonthName {
    signature: Signature,
}

impl Default for SparkMonthName {
    fn default() -> Self {
        Self::new()
    }
}

impl SparkMonthName {
    pub fn new() -> Self {
        Self {
            signature: Signature::coercible(
                vec![Coercion::new_exact(TypeSignatureClass::Timestamp)],
                Volatility::Immutable,
            ),
        }
    }
}

impl ScalarUDFImpl for SparkMonthName {
    fn name(&self) -> &str {
        "monthname"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        internal_err!("return_field_from_args should be used instead")
    }

    fn return_field_from_args(&self, args: ReturnFieldArgs) -> Result<FieldRef> {
        let nullable = args.arg_fields.iter().any(|f| f.is_nullable());
        Ok(Arc::new(Field::new(self.name(), DataType::Utf8, nullable)))
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let [arg] = take_function_args(self.name(), args.args)?;
        match arg {
            ColumnarValue::Scalar(scalar) => {
                if scalar.is_null() {
                    return Ok(ColumnarValue::Scalar(ScalarValue::Utf8(None)));
                }
                let arr = scalar.to_array_of_size(1)?;
                let month_arr = date_part(&arr, DatePart::Month)?;
                let month_val = month_arr
                    .as_primitive::<arrow::datatypes::Int32Type>()
                    .value(0);
                match month_number_to_name(month_val) {
                    Some(name) => Ok(ColumnarValue::Scalar(ScalarValue::Utf8(Some(
                        name.to_string(),
                    )))),
                    None => {
                        exec_err!("Invalid month number: {month_val}")
                    }
                }
            }
            ColumnarValue::Array(arr) => {
                let month_arr = date_part(&arr, DatePart::Month)?;
                let int_arr = month_arr.as_primitive::<arrow::datatypes::Int32Type>();

                let result: StringArray = int_arr
                    .iter()
                    .map(|maybe_month| match maybe_month {
                        Some(m) => Ok(month_number_to_name(m)),
                        None => Ok(None),
                    })
                    .collect::<Result<StringArray>>()?;

                Ok(ColumnarValue::Array(Arc::new(result)))
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Array, ArrayRef, Date32Array};
    use arrow::datatypes::TimeUnit;
    use datafusion_common::config::ConfigOptions;

    fn make_args(
        args: Vec<ColumnarValue>,
        arg_fields: Vec<FieldRef>,
        number_rows: usize,
    ) -> ScalarFunctionArgs {
        ScalarFunctionArgs {
            args,
            arg_fields,
            number_rows,
            return_field: Arc::new(Field::new("monthname", DataType::Utf8, true)),
            config_options: Arc::new(ConfigOptions::default()),
        }
    }

    #[test]
    fn test_monthname_scalar_date() {
        let func = SparkMonthName::new();
        // 2024-03-15 = 19797 days since epoch
        let result = func
            .invoke_with_args(make_args(
                vec![ColumnarValue::Scalar(ScalarValue::Date32(Some(19797)))],
                vec![Arc::new(Field::new("d", DataType::Date32, true))],
                1,
            ))
            .unwrap();
        match result {
            ColumnarValue::Scalar(ScalarValue::Utf8(Some(name))) => {
                assert_eq!(name, "Mar");
            }
            other => panic!("Expected scalar Utf8, got {other:?}"),
        }
    }

    #[test]
    fn test_monthname_array_dates() {
        let func = SparkMonthName::new();
        let date_array: ArrayRef = Arc::new(Date32Array::from(vec![
            Some(19723), // 2024-01-01 => Jan
            Some(19797), // 2024-03-15 => Mar
            Some(20088), // 2024-12-31 => Dec
            None,
        ]));

        let result = func
            .invoke_with_args(make_args(
                vec![ColumnarValue::Array(date_array)],
                vec![Arc::new(Field::new("d", DataType::Date32, true))],
                4,
            ))
            .unwrap();

        match result {
            ColumnarValue::Array(arr) => {
                let str_arr = arr.as_any().downcast_ref::<StringArray>().unwrap();
                assert_eq!(str_arr.value(0), "Jan");
                assert_eq!(str_arr.value(1), "Mar");
                assert_eq!(str_arr.value(2), "Dec");
                assert!(str_arr.is_null(3));
            }
            other => panic!("Expected array, got {other:?}"),
        }
    }

    #[test]
    fn test_monthname_null_scalar() {
        let func = SparkMonthName::new();
        let result = func
            .invoke_with_args(make_args(
                vec![ColumnarValue::Scalar(ScalarValue::Date32(None))],
                vec![Arc::new(Field::new("d", DataType::Date32, true))],
                1,
            ))
            .unwrap();
        match result {
            ColumnarValue::Scalar(ScalarValue::Utf8(None)) => {}
            other => panic!("Expected Utf8(None), got {other:?}"),
        }
    }

    #[test]
    fn test_monthname_timestamp_micros() {
        let func = SparkMonthName::new();
        // 2024-07-15 10:30:00 UTC in microseconds
        let result = func
            .invoke_with_args(make_args(
                vec![ColumnarValue::Scalar(ScalarValue::TimestampMicrosecond(
                    Some(1721038200000000),
                    None,
                ))],
                vec![Arc::new(Field::new(
                    "ts",
                    DataType::Timestamp(TimeUnit::Microsecond, None),
                    true,
                ))],
                1,
            ))
            .unwrap();
        match result {
            ColumnarValue::Scalar(ScalarValue::Utf8(Some(name))) => {
                assert_eq!(name, "Jul");
            }
            other => panic!("Expected scalar Utf8, got {other:?}"),
        }
    }

    #[test]
    fn test_monthname_all_months() {
        let func = SparkMonthName::new();
        let dates: Vec<Option<i32>> = vec![
            Some(19737), // 2024-01-15
            Some(19768), // 2024-02-15
            Some(19797), // 2024-03-15
            Some(19828), // 2024-04-15
            Some(19858), // 2024-05-15
            Some(19889), // 2024-06-15
            Some(19919), // 2024-07-15
            Some(19950), // 2024-08-15
            Some(19981), // 2024-09-15
            Some(20011), // 2024-10-15
            Some(20042), // 2024-11-15
            Some(20072), // 2024-12-15
        ];
        let date_array: ArrayRef = Arc::new(Date32Array::from(dates));

        let result = func
            .invoke_with_args(make_args(
                vec![ColumnarValue::Array(date_array)],
                vec![Arc::new(Field::new("d", DataType::Date32, true))],
                12,
            ))
            .unwrap();

        let expected = [
            "Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov",
            "Dec",
        ];
        match result {
            ColumnarValue::Array(arr) => {
                let str_arr = arr.as_any().downcast_ref::<StringArray>().unwrap();
                for (i, exp) in expected.iter().enumerate() {
                    assert_eq!(str_arr.value(i), *exp, "Month {} mismatch", i + 1);
                }
            }
            other => panic!("Expected array, got {other:?}"),
        }
    }

    #[test]
    fn test_monthname_return_field_nullable() {
        let func = SparkMonthName::new();

        let nullable = func
            .return_field_from_args(ReturnFieldArgs {
                arg_fields: &[Arc::new(Field::new("d", DataType::Date32, true))],
                scalar_arguments: &[None],
            })
            .unwrap();
        assert!(nullable.is_nullable());
        assert_eq!(nullable.data_type(), &DataType::Utf8);

        let non_nullable = func
            .return_field_from_args(ReturnFieldArgs {
                arg_fields: &[Arc::new(Field::new("d", DataType::Date32, false))],
                scalar_arguments: &[None],
            })
            .unwrap();
        assert!(!non_nullable.is_nullable());
    }
}
