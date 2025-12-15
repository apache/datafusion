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

use arrow::array::{ArrayRef, AsArray, Date32Array, StringArrayType, new_null_array};
use arrow::datatypes::{DataType, Date32Type, Field, FieldRef};
use chrono::{Datelike, Duration, Weekday};
use datafusion_common::{Result, ScalarValue, exec_err, internal_err};
use datafusion_expr::{
    ColumnarValue, ReturnFieldArgs, ScalarFunctionArgs, ScalarUDFImpl, Signature,
    Volatility,
};

/// <https://spark.apache.org/docs/latest/api/sql/index.html#next_day>
#[derive(Debug, PartialEq, Eq, Hash)]
pub struct SparkNextDay {
    signature: Signature,
}

impl Default for SparkNextDay {
    fn default() -> Self {
        Self::new()
    }
}

impl SparkNextDay {
    pub fn new() -> Self {
        Self {
            signature: Signature::exact(
                vec![DataType::Date32, DataType::Utf8],
                Volatility::Immutable,
            ),
        }
    }
}

impl ScalarUDFImpl for SparkNextDay {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "next_day"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        internal_err!("return_field_from_args should be used instead")
    }

    fn return_field_from_args(&self, args: ReturnFieldArgs) -> Result<FieldRef> {
        let [date_field, weekday_field] = args.arg_fields else {
            return internal_err!("Spark `next_day` expects exactly two arguments");
        };

        let has_invalid_scalar = args.scalar_arguments.iter().any(|arg| match arg {
            Some(ScalarValue::Utf8(Some(s)))
            | Some(ScalarValue::LargeUtf8(Some(s)))
            | Some(ScalarValue::Utf8View(Some(s))) => !is_valid_weekday(s),
            Some(v) => v.is_null(),
            _ => false,
        });

        let nullable =
            date_field.is_nullable() || weekday_field.is_nullable() || has_invalid_scalar;

        Ok(Arc::new(Field::new(
            self.name(),
            DataType::Date32,
            nullable,
        )))
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let ScalarFunctionArgs { args, .. } = args;
        let [date, day_of_week] = args.as_slice() else {
            return exec_err!(
                "Spark `next_day` function requires 2 arguments, got {}",
                args.len()
            );
        };

        match (date, day_of_week) {
            (ColumnarValue::Scalar(date), ColumnarValue::Scalar(day_of_week)) => {
                match (date, day_of_week) {
                    (
                        ScalarValue::Date32(days),
                        ScalarValue::Utf8(day_of_week)
                        | ScalarValue::LargeUtf8(day_of_week)
                        | ScalarValue::Utf8View(day_of_week),
                    ) => {
                        if let Some(days) = days {
                            if let Some(day_of_week) = day_of_week {
                                Ok(ColumnarValue::Scalar(ScalarValue::Date32(
                                    spark_next_day(*days, day_of_week.as_str()),
                                )))
                            } else {
                                Ok(ColumnarValue::Scalar(ScalarValue::Date32(None)))
                            }
                        } else {
                            Ok(ColumnarValue::Scalar(ScalarValue::Date32(None)))
                        }
                    }
                    _ => exec_err!(
                        "Spark `next_day` function: first arg must be date, second arg must be string. Got {args:?}"
                    ),
                }
            }
            (ColumnarValue::Array(date_array), ColumnarValue::Scalar(day_of_week)) => {
                match (date_array.data_type(), day_of_week) {
                    (
                        DataType::Date32,
                        ScalarValue::Utf8(day_of_week)
                        | ScalarValue::LargeUtf8(day_of_week)
                        | ScalarValue::Utf8View(day_of_week),
                    ) => {
                        if let Some(day_of_week) = day_of_week {
                            let result: Date32Array = date_array
                                .as_primitive::<Date32Type>()
                                .unary_opt(|days| {
                                    spark_next_day(days, day_of_week.as_str())
                                })
                                .with_data_type(DataType::Date32);
                            Ok(ColumnarValue::Array(Arc::new(result) as ArrayRef))
                        } else {
                            Ok(ColumnarValue::Array(Arc::new(new_null_array(
                                &DataType::Date32,
                                date_array.len(),
                            ))))
                        }
                    }
                    _ => exec_err!(
                        "Spark `next_day` function: first arg must be date, second arg must be string. Got {args:?}"
                    ),
                }
            }
            (
                ColumnarValue::Array(date_array),
                ColumnarValue::Array(day_of_week_array),
            ) => {
                let result = match (date_array.data_type(), day_of_week_array.data_type())
                {
                    (
                        DataType::Date32,
                        DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View,
                    ) => {
                        let date_array: &Date32Array =
                            date_array.as_primitive::<Date32Type>();
                        match day_of_week_array.data_type() {
                            DataType::Utf8 => process_next_day_arrays(
                                date_array,
                                day_of_week_array.as_string::<i32>(),
                            ),
                            DataType::LargeUtf8 => process_next_day_arrays(
                                date_array,
                                day_of_week_array.as_string::<i64>(),
                            ),
                            DataType::Utf8View => process_next_day_arrays(
                                date_array,
                                day_of_week_array.as_string_view(),
                            ),
                            other => exec_err!(
                                "Spark `next_day` function: second arg must be string. Got {other:?}"
                            ),
                        }
                    }
                    (left, right) => exec_err!(
                        "Spark `next_day` function: first arg must be date, second arg must be string. Got {left:?}, {right:?}"
                    ),
                }?;
                Ok(ColumnarValue::Array(result))
            }
            _ => exec_err!("Unsupported args {args:?} for Spark function `next_day`"),
        }
    }
}

fn process_next_day_arrays<'a, S>(
    date_array: &Date32Array,
    day_of_week_array: &'a S,
) -> Result<ArrayRef>
where
    &'a S: StringArrayType<'a>,
{
    let result = date_array
        .iter()
        .zip(day_of_week_array.iter())
        .map(|(days, day_of_week)| match (days, day_of_week) {
            (Some(days), Some(day_of_week)) => spark_next_day(days, day_of_week),
            _ => None,
        })
        .collect::<Date32Array>();

    Ok(Arc::new(result) as ArrayRef)
}

fn spark_next_day(days: i32, day_of_week: &str) -> Option<i32> {
    let date = Date32Type::to_naive_date(days);

    let s = day_of_week.trim();

    let day_of_week = if s.eq_ignore_ascii_case("MO")
        || s.eq_ignore_ascii_case("MON")
        || s.eq_ignore_ascii_case("MONDAY")
    {
        Weekday::Mon
    } else if s.eq_ignore_ascii_case("TU")
        || s.eq_ignore_ascii_case("TUE")
        || s.eq_ignore_ascii_case("TUESDAY")
    {
        Weekday::Tue
    } else if s.eq_ignore_ascii_case("WE")
        || s.eq_ignore_ascii_case("WED")
        || s.eq_ignore_ascii_case("WEDNESDAY")
    {
        Weekday::Wed
    } else if s.eq_ignore_ascii_case("TH")
        || s.eq_ignore_ascii_case("THU")
        || s.eq_ignore_ascii_case("THURSDAY")
    {
        Weekday::Thu
    } else if s.eq_ignore_ascii_case("FR")
        || s.eq_ignore_ascii_case("FRI")
        || s.eq_ignore_ascii_case("FRIDAY")
    {
        Weekday::Fri
    } else if s.eq_ignore_ascii_case("SA")
        || s.eq_ignore_ascii_case("SAT")
        || s.eq_ignore_ascii_case("SATURDAY")
    {
        Weekday::Sat
    } else if s.eq_ignore_ascii_case("SU")
        || s.eq_ignore_ascii_case("SUN")
        || s.eq_ignore_ascii_case("SUNDAY")
    {
        Weekday::Sun
    } else {
        return None;
    };

    Some(Date32Type::from_naive_date(
        date + Duration::days((7 - date.weekday().days_since(day_of_week)) as i64),
    ))
}

fn is_valid_weekday(s: &str) -> bool {
    let s = s.trim();

    s.eq_ignore_ascii_case("MO")
        || s.eq_ignore_ascii_case("MON")
        || s.eq_ignore_ascii_case("MONDAY")
        || s.eq_ignore_ascii_case("TU")
        || s.eq_ignore_ascii_case("TUE")
        || s.eq_ignore_ascii_case("TUESDAY")
        || s.eq_ignore_ascii_case("WE")
        || s.eq_ignore_ascii_case("WED")
        || s.eq_ignore_ascii_case("WEDNESDAY")
        || s.eq_ignore_ascii_case("TH")
        || s.eq_ignore_ascii_case("THU")
        || s.eq_ignore_ascii_case("THURSDAY")
        || s.eq_ignore_ascii_case("FR")
        || s.eq_ignore_ascii_case("FRI")
        || s.eq_ignore_ascii_case("FRIDAY")
        || s.eq_ignore_ascii_case("SA")
        || s.eq_ignore_ascii_case("SAT")
        || s.eq_ignore_ascii_case("SATURDAY")
        || s.eq_ignore_ascii_case("SU")
        || s.eq_ignore_ascii_case("SUN")
        || s.eq_ignore_ascii_case("SUNDAY")
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion_expr::ReturnFieldArgs;

    #[test]
    fn return_type_is_not_used() {
        let func = SparkNextDay::new();
        let err = func
            .return_type(&[DataType::Date32, DataType::Utf8])
            .unwrap_err();
        assert!(
            err.to_string()
                .contains("return_field_from_args should be used instead")
        );
    }

    #[test]
    fn next_day_nullability_derived_from_inputs() {
        let func = SparkNextDay::new();

        let non_nullable_date = Arc::new(Field::new("date", DataType::Date32, false));
        let non_nullable_weekday = Arc::new(Field::new("weekday", DataType::Utf8, false));

        let field = func
            .return_field_from_args(ReturnFieldArgs {
                arg_fields: &[
                    Arc::clone(&non_nullable_date),
                    Arc::clone(&non_nullable_weekday),
                ],
                scalar_arguments: &[None, None],
            })
            .unwrap();

        assert!(!field.is_nullable());

        let nullable_date = Arc::new(Field::new("date", DataType::Date32, true));

        let field = func
            .return_field_from_args(ReturnFieldArgs {
                arg_fields: &[
                    Arc::clone(&nullable_date),
                    Arc::clone(&non_nullable_weekday),
                ],
                scalar_arguments: &[None, None],
            })
            .unwrap();

        assert!(field.is_nullable());
    }

    #[test]
    fn next_day_valid_scalar_is_not_nullable() {
        let func = SparkNextDay::new();

        let date_field = Arc::new(Field::new("date", DataType::Date32, false));
        let weekday_field = Arc::new(Field::new("weekday", DataType::Utf8, false));

        let scalar = ScalarValue::Utf8(Some("MONDAY".to_string()));

        let field = func
            .return_field_from_args(ReturnFieldArgs {
                arg_fields: &[date_field, weekday_field],
                scalar_arguments: &[None, Some(&scalar)],
            })
            .unwrap();

        assert!(!field.is_nullable());
    }

    #[test]
    fn next_day_invalid_scalar_is_nullable() {
        let func = SparkNextDay::new();

        let date_field = Arc::new(Field::new("date", DataType::Date32, false));
        let weekday_field = Arc::new(Field::new("weekday", DataType::Utf8, false));

        let invalid_scalar = ScalarValue::Utf8(Some("FUNDAY".to_string()));

        let field = func
            .return_field_from_args(ReturnFieldArgs {
                arg_fields: &[date_field, weekday_field],
                scalar_arguments: &[None, Some(&invalid_scalar)],
            })
            .unwrap();

        assert!(field.is_nullable());
    }
}
