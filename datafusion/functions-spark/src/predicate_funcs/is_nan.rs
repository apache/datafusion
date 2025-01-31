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

use arrow::array::{Float32Array, Float64Array};
use arrow_array::{Array, BooleanArray};
use arrow_schema::DataType;
use datafusion::physical_plan::ColumnarValue;
use datafusion_common::{DataFusionError, ScalarValue};
use std::sync::Arc;

/// Spark-compatible `isnan` expression
pub fn spark_isnan(args: &[ColumnarValue]) -> Result<ColumnarValue, DataFusionError> {
    fn set_nulls_to_false(is_nan: BooleanArray) -> ColumnarValue {
        match is_nan.nulls() {
            Some(nulls) => {
                let is_not_null = nulls.inner();
                ColumnarValue::Array(Arc::new(BooleanArray::new(
                    is_nan.values() & is_not_null,
                    None,
                )))
            }
            None => ColumnarValue::Array(Arc::new(is_nan)),
        }
    }
    let value = &args[0];
    match value {
        ColumnarValue::Array(array) => match array.data_type() {
            DataType::Float64 => {
                let array = array.as_any().downcast_ref::<Float64Array>().unwrap();
                let is_nan = BooleanArray::from_unary(array, |x| x.is_nan());
                Ok(set_nulls_to_false(is_nan))
            }
            DataType::Float32 => {
                let array = array.as_any().downcast_ref::<Float32Array>().unwrap();
                let is_nan = BooleanArray::from_unary(array, |x| x.is_nan());
                Ok(set_nulls_to_false(is_nan))
            }
            other => Err(DataFusionError::Internal(format!(
                "Unsupported data type {:?} for function isnan",
                other,
            ))),
        },
        ColumnarValue::Scalar(a) => match a {
            ScalarValue::Float64(a) => Ok(ColumnarValue::Scalar(ScalarValue::Boolean(Some(
                a.map(|x| x.is_nan()).unwrap_or(false),
            )))),
            ScalarValue::Float32(a) => Ok(ColumnarValue::Scalar(ScalarValue::Boolean(Some(
                a.map(|x| x.is_nan()).unwrap_or(false),
            )))),
            _ => Err(DataFusionError::Internal(format!(
                "Unsupported data type {:?} for function isnan",
                value.data_type(),
            ))),
        },
    }
}
