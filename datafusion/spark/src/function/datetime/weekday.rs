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

use arrow::array::ArrayRef;
use arrow::compute::{DatePart, date_part};
use arrow::datatypes::{DataType, Field, FieldRef};
use datafusion_common::types::{NativeType, logical_date};
use datafusion_common::utils::take_function_args;
use datafusion_common::{Result, internal_err};
use datafusion_expr::{
    Coercion, ColumnarValue, ReturnFieldArgs, ScalarFunctionArgs, ScalarUDFImpl,
    Signature, TypeSignatureClass, Volatility,
};
use datafusion_functions::utils::make_scalar_function;

/// Spark-compatible `weekday` expression.
///
/// <https://spark.apache.org/docs/latest/api/sql/index.html#weekday>
#[derive(Debug, PartialEq, Eq, Hash)]
pub struct SparkWeekday {
    signature: Signature,
}

impl Default for SparkWeekday {
    fn default() -> Self {
        Self::new()
    }
}

impl SparkWeekday {
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

impl ScalarUDFImpl for SparkWeekday {
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
        make_scalar_function(spark_weekday, vec![])(&args.args)
    }
}

fn spark_weekday(args: &[ArrayRef]) -> Result<ArrayRef> {
    let [date_arg] = take_function_args("weekday", args)?;
    Ok(date_part(date_arg.as_ref(), DatePart::DayOfWeekMonday0)?)
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Array, Date32Array, Int32Array};

    #[test]
    fn test_weekday_array() {
        let date_array = Date32Array::from(vec![
            Some(4),  // 1970-01-05, Monday
            Some(5),  // 1970-01-06, Tuesday
            Some(6),  // 1970-01-07, Wednesday
            Some(7),  // 1970-01-08, Thursday
            Some(8),  // 1970-01-09, Friday
            Some(9),  // 1970-01-10, Saturday
            Some(10), // 1970-01-11, Sunday
            None,
        ]);

        let result = spark_weekday(&[Arc::new(date_array)]).unwrap();
        let result = result.as_any().downcast_ref::<Int32Array>().unwrap();

        for (idx, expected) in (0..=6).enumerate() {
            assert_eq!(result.value(idx), expected);
        }
        assert!(result.is_null(7));
    }

    #[test]
    fn test_weekday_nullability_matches_input() {
        let func = SparkWeekday::new();

        let non_nullable_arg = Arc::new(Field::new("arg", DataType::Date32, false));
        let nullable_arg = Arc::new(Field::new("arg", DataType::Date32, true));

        let non_nullable_out = func
            .return_field_from_args(ReturnFieldArgs {
                arg_fields: &[Arc::clone(&non_nullable_arg)],
                scalar_arguments: &[None],
            })
            .unwrap();
        assert_eq!(non_nullable_out.data_type(), &DataType::Int32);
        assert!(!non_nullable_out.is_nullable());

        let nullable_out = func
            .return_field_from_args(ReturnFieldArgs {
                arg_fields: &[Arc::clone(&nullable_arg)],
                scalar_arguments: &[None],
            })
            .unwrap();
        assert_eq!(nullable_out.data_type(), &DataType::Int32);
        assert!(nullable_out.is_nullable());
    }
}
