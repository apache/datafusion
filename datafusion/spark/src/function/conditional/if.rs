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

use arrow::{
    array::{ArrayRef, BooleanArray},
    compute::kernels::zip::zip,
    datatypes::DataType,
};
use datafusion_common::{plan_err, utils::take_function_args, Result};
use datafusion_expr::{
    binary::comparison_coercion_numeric, ColumnarValue, ScalarFunctionArgs,
    ScalarUDFImpl, Signature, Volatility,
};

#[derive(Debug)]
pub struct SparkIf {
    signature: Signature,
}

impl Default for SparkIf {
    fn default() -> Self {
        Self::new()
    }
}

impl SparkIf {
    pub fn new() -> Self {
        Self {
            signature: Signature::user_defined(Volatility::Immutable),
        }
    }
}

impl ScalarUDFImpl for SparkIf {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn name(&self) -> &str {
        "if"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn coerce_types(&self, arg_types: &[DataType]) -> Result<Vec<DataType>> {
        if arg_types.len() != 3 {
            return plan_err!(
                "Function 'if' expects 3 arguments but received {}",
                arg_types.len()
            );
        }
        let Some(target_type) = comparison_coercion_numeric(&arg_types[1], &arg_types[2])
        else {
            return plan_err!(
                "For function 'if' {} and {} is not comparable",
                arg_types[1],
                arg_types[2]
            );
        };
        // Convert null to String type.
        if target_type.is_null() {
            Ok(vec![
                DataType::Boolean,
                DataType::Utf8View,
                DataType::Utf8View,
            ])
        } else {
            Ok(vec![DataType::Boolean, target_type.clone(), target_type])
        }
    }

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        Ok(arg_types[1].clone())
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let args = ColumnarValue::values_to_arrays(&args.args)?;
        let [expr1, expr2, expr3] = take_function_args::<3, ArrayRef>("if", args)?;
        let expr1 = expr1.as_any().downcast_ref::<BooleanArray>().unwrap();
        let result = zip(expr1, &expr2, &expr3)?;
        Ok(ColumnarValue::Array(result))
    }
}

#[cfg(test)]
mod tests {
    use arrow::array::{
        Array, BooleanArray, Float64Array, Int32Array, StringArray,
    };
    use std::sync::Arc;

    use super::*;

    #[test]
    fn test_if_basic() {
        let if_udf = SparkIf::default();

        // Test basic functionality
        let condition = BooleanArray::from(vec![true, false, true, false]);
        let true_value = Int32Array::from(vec![10, 20, 30, 40]);
        let false_value = Int32Array::from(vec![100, 200, 300, 400]);

        let args = vec![
            ColumnarValue::Array(Arc::new(condition)),
            ColumnarValue::Array(Arc::new(true_value)),
            ColumnarValue::Array(Arc::new(false_value)),
        ];

        let arg_fields = args
            .iter()
            .enumerate()
            .map(|(idx, arg)| {
                arrow::datatypes::Field::new(format!("f_{idx}"), arg.data_type(), true)
                    .into()
            })
            .collect::<Vec<_>>();

        let result = if_udf
            .invoke_with_args(ScalarFunctionArgs {
                args,
                arg_fields,
                number_rows: 4,
                return_field: arrow::datatypes::Field::new(
                    "result",
                    DataType::Int32,
                    true,
                )
                .into(),
            })
            .unwrap();

        let result_array = result.into_array(4).unwrap();
        let result_int32 = result_array.as_any().downcast_ref::<Int32Array>().unwrap();

        assert_eq!(result_int32.value(0), 10); // true -> 10
        assert_eq!(result_int32.value(1), 200); // false -> 200
        assert_eq!(result_int32.value(2), 30); // true -> 30
        assert_eq!(result_int32.value(3), 400); // false -> 400
    }

    #[test]
    fn test_if_with_nulls() {
        let if_udf = SparkIf::default();

        // Test with NULL values in condition
        let condition =
            BooleanArray::from(vec![Some(true), None, Some(false), Some(true)]);
        let true_value = Int32Array::from(vec![10, 20, 30, 40]);
        let false_value = Int32Array::from(vec![100, 200, 300, 400]);

        let args = vec![
            ColumnarValue::Array(Arc::new(condition)),
            ColumnarValue::Array(Arc::new(true_value)),
            ColumnarValue::Array(Arc::new(false_value)),
        ];

        let arg_fields = args
            .iter()
            .enumerate()
            .map(|(idx, arg)| {
                arrow::datatypes::Field::new(format!("f_{idx}"), arg.data_type(), true)
                    .into()
            })
            .collect::<Vec<_>>();

        let result = if_udf
            .invoke_with_args(ScalarFunctionArgs {
                args,
                arg_fields,
                number_rows: 4,
                return_field: arrow::datatypes::Field::new(
                    "result",
                    DataType::Int32,
                    true,
                )
                .into(),
            })
            .unwrap();

        let result_array = result.into_array(4).unwrap();
        let result_int32 = result_array.as_any().downcast_ref::<Int32Array>().unwrap();

        assert_eq!(result_int32.value(0), 10); // true -> 10
        assert_eq!(result_int32.value(1), 200); // NULL -> 200
        assert_eq!(result_int32.value(2), 300); // false -> 300
        assert_eq!(result_int32.value(3), 40); // true -> 40
    }

    #[test]
    fn test_if_string_types() {
        let if_udf = SparkIf::default();

        // Test with string types
        let condition = BooleanArray::from(vec![true, false, true]);
        let true_value = StringArray::from(vec!["yes", "yes", "yes"]);
        let false_value = StringArray::from(vec!["no", "maybe", "no"]);

        let args = vec![
            ColumnarValue::Array(Arc::new(condition)),
            ColumnarValue::Array(Arc::new(true_value)),
            ColumnarValue::Array(Arc::new(false_value)),
        ];

        let arg_fields = args
            .iter()
            .enumerate()
            .map(|(idx, arg)| {
                arrow::datatypes::Field::new(format!("f_{idx}"), arg.data_type(), true)
                    .into()
            })
            .collect::<Vec<_>>();

        let result = if_udf
            .invoke_with_args(ScalarFunctionArgs {
                args,
                arg_fields,
                number_rows: 3,
                return_field: arrow::datatypes::Field::new(
                    "result",
                    DataType::Utf8,
                    true,
                )
                .into(),
            })
            .unwrap();

        let result_array = result.into_array(3).unwrap();
        let result_string = result_array.as_any().downcast_ref::<StringArray>().unwrap();

        assert_eq!(result_string.value(0), "yes"); // true -> "yes"
        assert_eq!(result_string.value(1), "maybe"); // false -> "maybe"
        assert_eq!(result_string.value(2), "yes"); // true -> "yes"
    }

    #[test]
    fn test_if_float_types() {
        let if_udf = SparkIf::default();

        // Test with float types
        let condition = BooleanArray::from(vec![true, false, true, false]);
        let true_value = Float64Array::from(vec![1.5, 2.5, 3.5, 4.5]);
        let false_value = Float64Array::from(vec![10.5, 20.5, 30.5, 40.5]);

        let args = vec![
            ColumnarValue::Array(Arc::new(condition)),
            ColumnarValue::Array(Arc::new(true_value)),
            ColumnarValue::Array(Arc::new(false_value)),
        ];

        let arg_fields = args
            .iter()
            .enumerate()
            .map(|(idx, arg)| {
                arrow::datatypes::Field::new(format!("f_{idx}"), arg.data_type(), true)
                    .into()
            })
            .collect::<Vec<_>>();

        let result = if_udf
            .invoke_with_args(ScalarFunctionArgs {
                args,
                arg_fields,
                number_rows: 4,
                return_field: arrow::datatypes::Field::new(
                    "result",
                    DataType::Float64,
                    true,
                )
                .into(),
            })
            .unwrap();

        let result_array = result.into_array(4).unwrap();
        let result_float = result_array
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap();

        assert_eq!(result_float.value(0), 1.5); // true -> 1.5
        assert_eq!(result_float.value(1), 20.5); // false -> 20.5
        assert_eq!(result_float.value(2), 3.5); // true -> 3.5
        assert_eq!(result_float.value(3), 40.5); // false -> 40.5
    }
}
