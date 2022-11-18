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

//! Columnar value module contains a set of types that represent a columnar value.

use arrow::array::ArrayRef;
use arrow::array::NullArray;
use arrow::datatypes::DataType;
use datafusion_common::ScalarValue;
use std::sync::Arc;

/// Represents the result of evaluating an expression: either a single
/// `ScalarValue` or an [`ArrayRef`].
///
/// While a [`ColumnarValue`] can always be converted into an array
/// for convenience, it is often much more performant to provide an
/// optimized path for scalar values.
#[derive(Clone, Debug)]
pub enum ColumnarValue {
    /// Array of values
    Array(ArrayRef),
    /// A single value
    Scalar(ScalarValue),
}

impl ColumnarValue {
    pub fn data_type(&self) -> DataType {
        match self {
            ColumnarValue::Array(array_value) => array_value.data_type().clone(),
            ColumnarValue::Scalar(scalar_value) => scalar_value.get_datatype(),
        }
    }

    /// Convert a columnar value into an ArrayRef. [`Self::Scalar`] is
    /// converted by repeating the same scalar multiple times.
    pub fn into_array(self, num_rows: usize) -> ArrayRef {
        match self {
            ColumnarValue::Array(array) => array,
            ColumnarValue::Scalar(scalar) => scalar.to_array_of_size(num_rows),
        }
    }

    /// null columnar values are implemented as a null array in order to pass batch
    /// num_rows
    pub fn create_null_array(num_rows: usize) -> Self {
        ColumnarValue::Array(Arc::new(NullArray::new(num_rows)))
    }
}
