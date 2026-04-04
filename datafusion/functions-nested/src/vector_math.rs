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

//! Shared vector math primitives used by cosine_distance, inner_product,
//! array_normalize, and related functions.

use arrow::array::{ArrayRef, Float64Array};
use datafusion_common::cast::{
    as_float32_array, as_float64_array, as_int32_array, as_int64_array,
};
use datafusion_common::{Result, exec_err};

/// Converts an array of any numeric type to a Float64Array.
pub fn convert_to_f64_array(array: &ArrayRef) -> Result<Float64Array> {
    match array.data_type() {
        arrow::datatypes::DataType::Float64 => Ok(as_float64_array(array)?.clone()),
        arrow::datatypes::DataType::Float32 => {
            let array = as_float32_array(array)?;
            let converted: Float64Array =
                array.iter().map(|v| v.map(|v| v as f64)).collect();
            Ok(converted)
        }
        arrow::datatypes::DataType::Int64 => {
            let array = as_int64_array(array)?;
            let converted: Float64Array =
                array.iter().map(|v| v.map(|v| v as f64)).collect();
            Ok(converted)
        }
        arrow::datatypes::DataType::Int32 => {
            let array = as_int32_array(array)?;
            let converted: Float64Array =
                array.iter().map(|v| v.map(|v| v as f64)).collect();
            Ok(converted)
        }
        _ => exec_err!("Unsupported array type for conversion to Float64Array"),
    }
}

/// Computes dot product: sum(a[i] * b[i])
pub fn dot_product_f64(a: &Float64Array, b: &Float64Array) -> f64 {
    a.iter()
        .zip(b.iter())
        .map(|(v1, v2)| v1.unwrap_or(0.0) * v2.unwrap_or(0.0))
        .sum()
}

/// Computes sum of squares: sum(a[i]^2)
pub fn sum_of_squares_f64(a: &Float64Array) -> f64 {
    a.iter()
        .map(|v| {
            let val = v.unwrap_or(0.0);
            val * val
        })
        .sum()
}

/// Computes magnitude (L2 norm): sqrt(sum(a[i]^2))
pub fn magnitude_f64(a: &Float64Array) -> f64 {
    sum_of_squares_f64(a).sqrt()
}
