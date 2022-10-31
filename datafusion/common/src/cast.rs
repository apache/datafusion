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
use arrow::array::{Array, Date32Array};

// Downcast ArrayRef to Date32Array
pub fn as_date32_array(array: &dyn Array) -> Result<&Date32Array, DataFusionError> {
    array.as_any().downcast_ref::<Date32Array>().ok_or_else(|| {
        DataFusionError::Internal(format!(
            "Expected a Date32Array, got: {}",
            array.data_type()
        ))
    })
}
