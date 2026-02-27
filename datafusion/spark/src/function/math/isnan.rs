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

use arrow::array::{Array, ArrayRef, BooleanArray, Float32Array, Float64Array};
use arrow::datatypes::DataType;
use datafusion_common::utils::take_function_args;
use datafusion_common::{Result, ScalarValue, exec_err};
use datafusion_expr::{
    ColumnarValue, ScalarFunctionArgs, ScalarUDFImpl, Signature, TypeSignature,
    Volatility,
};

/// Spark-compatible `isnan` expression
/// <https://spark.apache.org/docs/latest/api/sql/index.html#isnan>
///
/// Differences with standard SQL:
///  - Returns `false` for NULL inputs (not NULL)
#[derive(Debug, PartialEq, Eq, Hash)]
pub struct SparkIsNaN {
    signature: Signature,
}

impl Default for SparkIsNaN {
    fn default() -> Self {
        Self::new()
    }
}

impl SparkIsNaN {
    pub fn new() -> Self {
        Self {
            signature: Signature::one_of(
                vec![
                    TypeSignature::Exact(vec![DataType::Float32]),
                    TypeSignature::Exact(vec![DataType::Float64]),
                ],
                Volatility::Immutable,
            ),
        }
    }
}

impl ScalarUDFImpl for SparkIsNaN {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "isnan"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Boolean)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        spark_isnan(&args.args)
    }
}

fn spark_isnan(args: &[ColumnarValue]) -> Result<ColumnarValue> {
    let [value] = take_function_args("isnan", args)?;

    match value {
        ColumnarValue::Array(array) => match array.data_type() {
            DataType::Float32 => {
                let array = array.as_any().downcast_ref::<Float32Array>().unwrap();
                Ok(ColumnarValue::Array(nulls_to_false(
                    BooleanArray::from_unary(array, |x| x.is_nan()),
                )))
            }
            DataType::Float64 => {
                let array = array.as_any().downcast_ref::<Float64Array>().unwrap();
                Ok(ColumnarValue::Array(nulls_to_false(
                    BooleanArray::from_unary(array, |x| x.is_nan()),
                )))
            }
            other => exec_err!("Unsupported data type {other:?} for function isnan"),
        },
        ColumnarValue::Scalar(sv) => match sv {
            ScalarValue::Float32(v) => Ok(ColumnarValue::Scalar(ScalarValue::Boolean(
                Some(v.is_some_and(|x| x.is_nan())),
            ))),
            ScalarValue::Float64(v) => Ok(ColumnarValue::Scalar(ScalarValue::Boolean(
                Some(v.is_some_and(|x| x.is_nan())),
            ))),
            _ => exec_err!(
                "Unsupported data type {:?} for function isnan",
                sv.data_type()
            ),
        },
    }
}

/// Replaces null values with false in a BooleanArray.
///
/// Spark's `isnan` returns `false` for NULL inputs rather than propagating NULL.
fn nulls_to_false(is_nan: BooleanArray) -> ArrayRef {
    match is_nan.nulls() {
        Some(nulls) => {
            let is_not_null = nulls.inner();
            Arc::new(BooleanArray::new(is_nan.values() & is_not_null, None))
        }
        None => Arc::new(is_nan),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_isnan_float64() {
        let input = Float64Array::from(vec![
            Some(1.0),
            Some(f64::NAN),
            None,
            Some(f64::INFINITY),
            Some(0.0),
        ]);
        let args = vec![ColumnarValue::Array(Arc::new(input))];
        let result = spark_isnan(&args).unwrap();
        let result = match result {
            ColumnarValue::Array(arr) => arr,
            _ => panic!("Expected array"),
        };
        let result = result.as_any().downcast_ref::<BooleanArray>().unwrap();

        // NULL input produces false, not NULL
        assert!(!result.is_null(2));

        let expected = BooleanArray::from(vec![false, true, false, false, false]);
        assert_eq!(result, &expected);
    }

    #[test]
    fn test_isnan_float32() {
        let input = Float32Array::from(vec![
            Some(1.0f32),
            Some(f32::NAN),
            None,
            Some(f32::INFINITY),
            Some(0.0f32),
        ]);
        let args = vec![ColumnarValue::Array(Arc::new(input))];
        let result = spark_isnan(&args).unwrap();
        let result = match result {
            ColumnarValue::Array(arr) => arr,
            _ => panic!("Expected array"),
        };
        let result = result.as_any().downcast_ref::<BooleanArray>().unwrap();

        assert!(!result.is_null(2));

        let expected = BooleanArray::from(vec![false, true, false, false, false]);
        assert_eq!(result, &expected);
    }

    #[test]
    fn test_isnan_scalar_nan() {
        let result =
            spark_isnan(&[ColumnarValue::Scalar(ScalarValue::Float64(Some(f64::NAN)))])
                .unwrap();
        match result {
            ColumnarValue::Scalar(ScalarValue::Boolean(Some(v))) => assert!(v),
            _ => panic!("Expected scalar boolean"),
        }
    }

    #[test]
    fn test_isnan_scalar_null() {
        let result =
            spark_isnan(&[ColumnarValue::Scalar(ScalarValue::Float64(None))]).unwrap();
        match result {
            ColumnarValue::Scalar(ScalarValue::Boolean(Some(v))) => assert!(!v),
            _ => panic!("Expected scalar boolean"),
        }
    }

    #[test]
    fn test_isnan_scalar_normal() {
        let result =
            spark_isnan(&[ColumnarValue::Scalar(ScalarValue::Float64(Some(1.0)))])
                .unwrap();
        match result {
            ColumnarValue::Scalar(ScalarValue::Boolean(Some(v))) => assert!(!v),
            _ => panic!("Expected scalar boolean"),
        }
    }

    #[test]
    fn test_isnan_float32_scalar_nan() {
        let result =
            spark_isnan(&[ColumnarValue::Scalar(ScalarValue::Float32(Some(f32::NAN)))])
                .unwrap();
        match result {
            ColumnarValue::Scalar(ScalarValue::Boolean(Some(v))) => assert!(v),
            _ => panic!("Expected scalar boolean"),
        }
    }

    #[test]
    fn test_isnan_float32_scalar_null() {
        let result =
            spark_isnan(&[ColumnarValue::Scalar(ScalarValue::Float32(None))]).unwrap();
        match result {
            ColumnarValue::Scalar(ScalarValue::Boolean(Some(v))) => assert!(!v),
            _ => panic!("Expected scalar boolean"),
        }
    }

    #[test]
    fn test_isnan_float32_scalar_normal() {
        let result =
            spark_isnan(&[ColumnarValue::Scalar(ScalarValue::Float32(Some(1.0f32)))])
                .unwrap();
        match result {
            ColumnarValue::Scalar(ScalarValue::Boolean(Some(v))) => assert!(!v),
            _ => panic!("Expected scalar boolean"),
        }
    }
}
