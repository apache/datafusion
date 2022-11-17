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

//! This module provides DataFusion specific casting functions
//! that provide error handling. They are intended to "never fail"
//! but provide an error message rather than a panic, as the corresponding
//! kernels in arrow-rs such as `as_boolean_array` do.

use crate::DataFusionError;
use arrow::array::{
    Array, Date32Array, Decimal128Array, Float32Array, Float64Array, Int32Array,
    Int64Array, StringArray, StructArray,
};

// Downcast ArrayRef to Date32Array
pub fn as_date32_array(array: &dyn Array) -> Result<&Date32Array, DataFusionError> {
    array.as_any().downcast_ref::<Date32Array>().ok_or_else(|| {
        DataFusionError::Internal(format!(
            "Expected a Date32Array, got: {}",
            array.data_type()
        ))
    })
}

// Downcast ArrayRef to StructArray
pub fn as_struct_array(array: &dyn Array) -> Result<&StructArray, DataFusionError> {
    array.as_any().downcast_ref::<StructArray>().ok_or_else(|| {
        DataFusionError::Internal(format!(
            "Expected a StructArray, got: {}",
            array.data_type()
        ))
    })
}

// Downcast ArrayRef to Int32Array
pub fn as_int32_array(array: &dyn Array) -> Result<&Int32Array, DataFusionError> {
    array.as_any().downcast_ref::<Int32Array>().ok_or_else(|| {
        DataFusionError::Internal(format!(
            "Expected a Int32Array, got: {}",
            array.data_type()
        ))
    })
}

// Downcast ArrayRef to Int64Array
pub fn as_int64_array(array: &dyn Array) -> Result<&Int64Array, DataFusionError> {
    array.as_any().downcast_ref::<Int64Array>().ok_or_else(|| {
        DataFusionError::Internal(format!(
            "Expected a Int64Array, got: {}",
            array.data_type()
        ))
    })
}

// Downcast ArrayRef to Decimal128Array
pub fn as_decimal128_array(
    array: &dyn Array,
) -> Result<&Decimal128Array, DataFusionError> {
    array
        .as_any()
        .downcast_ref::<Decimal128Array>()
        .ok_or_else(|| {
            DataFusionError::Internal(format!(
                "Expected a Decimal128Array, got: {}",
                array.data_type()
            ))
        })
}

// Downcast ArrayRef to Float32Array
pub fn as_float32_array(array: &dyn Array) -> Result<&Float32Array, DataFusionError> {
    array
        .as_any()
        .downcast_ref::<Float32Array>()
        .ok_or_else(|| {
            DataFusionError::Internal(format!(
                "Expected a Float32Array, got: {}",
                array.data_type()
            ))
        })
}

// Downcast ArrayRef to Float64Array
pub fn as_float64_array(array: &dyn Array) -> Result<&Float64Array, DataFusionError> {
    array
        .as_any()
        .downcast_ref::<Float64Array>()
        .ok_or_else(|| {
            DataFusionError::Internal(format!(
                "Expected a Float64Array, got: {}",
                array.data_type()
            ))
        })
}

// Downcast ArrayRef to StringArray
pub fn as_string_array(array: &dyn Array) -> Result<&StringArray, DataFusionError> {
    array.as_any().downcast_ref::<StringArray>().ok_or_else(|| {
        DataFusionError::Internal(format!(
            "Expected a StringArray, got: {}",
            array.data_type()
        ))
    })
}
