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

use arrow::array::AsArray;
use arrow::compute::{DatePart, date_part};
use arrow::datatypes::{DataType, Field, FieldRef, Int32Type};
use datafusion_common::types::{NativeType, logical_date};
use datafusion_common::utils::take_function_args;
use datafusion_common::{Result, ScalarValue, internal_err};
use datafusion_expr::{
    Coercion, ColumnarValue, ReturnFieldArgs, ScalarFunctionArgs, ScalarUDFImpl,
    Signature, TypeSignatureClass, Volatility,
};

/// Spark-compatible `weekday` expression.
/// Returns the day of the week for a date or timestamp as an integer index where
/// Monday = 0, Tuesday = 1, ..., Sunday = 6.
///
/// Note: this differs from `dayofweek`, which is 1-indexed with Sunday = 1.
///
/// <https://spark.apache.org/docs/latest/api/sql/index.html#weekday>
#[derive(Debug, PartialEq, Eq, Hash)]
pub struct SparkWeekDay {
    signature: Signature,
}

impl Default for SparkWeekDay {
    fn default() -> Self {
        Self::new()
    }
}

impl SparkWeekDay {
    pub fn new() -> Self {
        Self {
            signature: Signature::coercible(
                vec![Coercion::new_implicit(
                    TypeSignatureClass::Native(logical_date()),
                    vec![TypeSignatureClass::Timestamp],
                    NativeType::Date,
                )],
                Volatility::Immutable,
            ),
        }
    }
}

impl ScalarUDFImpl for SparkWeekDay {
    fn name(&self) -> &str {
        "weekday"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        internal_err!("return_field_from_args should be used instead")
    }

    fn return_field_from_args(&self, args: ReturnFieldArgs) -> Result<FieldRef> {
        let nullable = args.arg_fields.iter().any(|f| f.is_nullable());
        Ok(Arc::new(Field::new(self.name(), DataType::Int32, nullable)))
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let [arg] = take_function_args(self.name(), args.args)?;
        match arg {
            ColumnarValue::Scalar(scalar) => {
                if scalar.is_null() {
                    return Ok(ColumnarValue::Scalar(ScalarValue::Int32(None)));
                }
                let arr = scalar.to_array_of_size(1)?;
                // `DayOfWeekMonday0` returns 0..=6 with Monday = 0, which
                // matches Spark `weekday` semantics exactly.
                let weekday_arr = date_part(&arr, DatePart::DayOfWeekMonday0)?;
                let value = weekday_arr.as_primitive::<Int32Type>().value(0);
                Ok(ColumnarValue::Scalar(ScalarValue::Int32(Some(value))))
            }
            ColumnarValue::Array(arr) => {
                let weekday_arr = date_part(&arr, DatePart::DayOfWeekMonday0)?;
                Ok(ColumnarValue::Array(weekday_arr))
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Date32Array, Int32Array};

    #[test]
    fn test_weekday_return_field_nullability_matches_input() {
        let func = SparkWeekDay::new();

        let non_nullable_arg = Arc::new(Field::new("arg", DataType::Date32, false));
        let nullable_arg = Arc::new(Field::new("arg", DataType::Date32, true));

        let non_nullable_out = func
            .return_field_from_args(ReturnFieldArgs {
                arg_fields: &[Arc::clone(&non_nullable_arg)],
                scalar_arguments: &[None],
            })
            .expect("non-nullable arg should succeed");
        assert_eq!(non_nullable_out.data_type(), &DataType::Int32);
        assert!(!non_nullable_out.is_nullable());

        let nullable_out = func
            .return_field_from_args(ReturnFieldArgs {
                arg_fields: &[Arc::clone(&nullable_arg)],
                scalar_arguments: &[None],
            })
            .expect("nullable arg should succeed");
        assert_eq!(nullable_out.data_type(), &DataType::Int32);
        assert!(nullable_out.is_nullable());
    }

    #[test]
    fn test_weekday_scalar() -> Result<()> {
        let func = SparkWeekDay::new();

        // 2024-03-15 is a Friday -> Spark weekday = 4 (Mon=0).
        let result = func.invoke_with_args(ScalarFunctionArgs {
            args: vec![ColumnarValue::Scalar(ScalarValue::Date32(Some(19797)))],
            arg_fields: vec![Arc::new(Field::new("arg", DataType::Date32, true))],
            number_rows: 1,
            return_field: Arc::new(Field::new("weekday", DataType::Int32, true)),
            config_options: Arc::new(Default::default()),
        })?;
        match result {
            ColumnarValue::Scalar(ScalarValue::Int32(Some(v))) => assert_eq!(v, 4),
            other => panic!("unexpected result: {other:?}"),
        }

        // NULL input -> NULL output.
        let result = func.invoke_with_args(ScalarFunctionArgs {
            args: vec![ColumnarValue::Scalar(ScalarValue::Date32(None))],
            arg_fields: vec![Arc::new(Field::new("arg", DataType::Date32, true))],
            number_rows: 1,
            return_field: Arc::new(Field::new("weekday", DataType::Int32, true)),
            config_options: Arc::new(Default::default()),
        })?;
        match result {
            ColumnarValue::Scalar(ScalarValue::Int32(None)) => {}
            other => panic!("unexpected result: {other:?}"),
        }

        Ok(())
    }

    #[test]
    fn test_weekday_array() -> Result<()> {
        let func = SparkWeekDay::new();

        // 2024-01-01 Mon(0), 2024-01-06 Sat(5), 2024-01-07 Sun(6), NULL.
        let input = Date32Array::from(vec![Some(19723), Some(19728), Some(19729), None]);
        let result = func.invoke_with_args(ScalarFunctionArgs {
            args: vec![ColumnarValue::Array(Arc::new(input))],
            arg_fields: vec![Arc::new(Field::new("arg", DataType::Date32, true))],
            number_rows: 4,
            return_field: Arc::new(Field::new("weekday", DataType::Int32, true)),
            config_options: Arc::new(Default::default()),
        })?;
        match result {
            ColumnarValue::Array(arr) => {
                let expected = Int32Array::from(vec![Some(0), Some(5), Some(6), None]);
                assert_eq!(arr.as_primitive::<Int32Type>(), &expected);
            }
            other => panic!("unexpected result: {other:?}"),
        }

        Ok(())
    }
}
