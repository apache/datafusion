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

use crate::{downcast_value, DataFusionError};
use arrow::array::{
    Array, BooleanArray, Date32Array, Decimal128Array, Float32Array, Float64Array,
    Int32Array, Int64Array, ListArray, StringArray, StructArray, UInt32Array,
    UInt64Array,
};

// Downcast ArrayRef to Date32Array
pub fn as_date32_array(array: &dyn Array) -> Result<&Date32Array, DataFusionError> {
    Ok(downcast_value!(array, Date32Array))
}

// Downcast ArrayRef to StructArray
pub fn as_struct_array(array: &dyn Array) -> Result<&StructArray, DataFusionError> {
    Ok(downcast_value!(array, StructArray))
}

// Downcast ArrayRef to Int32Array
pub fn as_int32_array(array: &dyn Array) -> Result<&Int32Array, DataFusionError> {
    Ok(downcast_value!(array, Int32Array))
}

// Downcast ArrayRef to Int64Array
pub fn as_int64_array(array: &dyn Array) -> Result<&Int64Array, DataFusionError> {
    Ok(downcast_value!(array, Int64Array))
}

// Downcast ArrayRef to Decimal128Array
pub fn as_decimal128_array(
    array: &dyn Array,
) -> Result<&Decimal128Array, DataFusionError> {
    Ok(downcast_value!(array, Decimal128Array))
}

// Downcast ArrayRef to Float32Array
pub fn as_float32_array(array: &dyn Array) -> Result<&Float32Array, DataFusionError> {
    Ok(downcast_value!(array, Float32Array))
}

// Downcast ArrayRef to Float64Array
pub fn as_float64_array(array: &dyn Array) -> Result<&Float64Array, DataFusionError> {
    Ok(downcast_value!(array, Float64Array))
}

// Downcast ArrayRef to StringArray
pub fn as_string_array(array: &dyn Array) -> Result<&StringArray, DataFusionError> {
    Ok(downcast_value!(array, StringArray))
}

// Downcast ArrayRef to UInt32Array
pub fn as_uint32_array(array: &dyn Array) -> Result<&UInt32Array, DataFusionError> {
    Ok(downcast_value!(array, UInt32Array))
}

// Downcast ArrayRef to UInt64Array
pub fn as_uint64_array(array: &dyn Array) -> Result<&UInt64Array, DataFusionError> {
    Ok(downcast_value!(array, UInt64Array))
}

// Downcast ArrayRef to BooleanArray
pub fn as_boolean_array(array: &dyn Array) -> Result<&BooleanArray, DataFusionError> {
    Ok(downcast_value!(array, BooleanArray))
}

// Downcast ArrayRef to ListArray
pub fn as_list_array(array: &dyn Array) -> Result<&ListArray, DataFusionError> {
    Ok(downcast_value!(array, ListArray))
}
