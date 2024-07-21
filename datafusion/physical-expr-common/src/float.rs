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
// UnLt required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

use std::sync::Arc;

use arrow::array::{
    Array, ArrayRef, ArrowNativeTypeOp, ArrowNumericType, AsArray, PrimitiveBuilder,
};
use arrow::datatypes::{DataType, Float16Type, Float32Type, Float64Type};
use datafusion_common::{Result, ScalarValue};
use datafusion_expr::ColumnarValue;

/// Given a float [`ColumnarValue`] return a new float [`ColumnarValue`]
/// with zeros (-0.0 and 0.0) normalized to (0.0)
pub fn normalize_floating_zeros(
    value: ColumnarValue,
    dt: &DataType,
) -> Result<ColumnarValue> {
    match dt {
        DataType::Float64 => normalize_floating_zeros_impl::<Float64Type>(value),
        DataType::Float32 => normalize_floating_zeros_impl::<Float32Type>(value),
        DataType::Float16 => normalize_floating_zeros_impl::<Float16Type>(value),
        _ => Ok(value),
    }
}

fn normalize_floating_zeros_impl<T>(value: ColumnarValue) -> Result<ColumnarValue>
where
    T: ArrowNumericType,
{
    Ok(match value {
        ColumnarValue::Array(array) => ColumnarValue::Array(norm_zeros::<T>(array)?),
        ColumnarValue::Scalar(value) => ColumnarValue::Scalar(
            ScalarValue::try_from_array(&norm_zeros::<T>(value.to_array()?)?, 0)?,
        ),
    })
}

// rewrites -0.0 as 0.0
fn norm_zeros<T>(array: ArrayRef) -> Result<ArrayRef>
where
    T: ArrowNumericType,
{
    let mut builder = PrimitiveBuilder::<T>::with_capacity(array.len());

    for i in 0..array.len() {
        if array.is_null(i) {
            builder.append_null()
        } else {
            let value = array.as_primitive::<T>().value(i);
            if value.is_zero() {
                builder.append_value(T::default_value());
            } else {
                builder.append_value(value);
            }
        }
    }

    Ok(Arc::new(builder.finish()))
}
