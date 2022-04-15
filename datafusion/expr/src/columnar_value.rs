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
use arrow::record_batch::RecordBatch;
use datafusion_common::ScalarValue;
use std::sync::Arc;

/// Represents the result from an expression
#[derive(Clone)]
pub enum ColumnarValue {
    /// Array of values
    Array(ArrayRef),
    /// A single value
    Scalar(ScalarValue),
    // Table(Vec<ColumnarValue>),
}

impl ColumnarValue {
    pub fn data_type(&self) -> DataType {
        match self {
            ColumnarValue::Array(array_value) => array_value.data_type().clone(),
            ColumnarValue::Scalar(scalar_value) => scalar_value.get_datatype(),
            // ColumnarValue::Table(value) => {
            //     let val = value.get(0);
            //     if val.is_some() {
            //         return val.unwrap().data_type();
            //     } else {
            //         return DataType::Null;
            //     }
            // }
        }
    }

    /// Convert a columnar value into an ArrayRef
    pub fn into_array(self, num_rows: usize) -> ArrayRef {
        match self {
            ColumnarValue::Array(array) => array,
            ColumnarValue::Scalar(scalar) => scalar.to_array_of_size(num_rows),
            // ColumnarValue::Table(..)  => panic!("ColumnarValue::Table.into_array not implemented"),
        }
    }
}

/// null columnar values are implemented as a null array in order to pass batch
/// num_rows
pub type NullColumnarValue = ColumnarValue;

impl From<&RecordBatch> for NullColumnarValue {
    fn from(batch: &RecordBatch) -> Self {
        let num_rows = batch.num_rows();
        ColumnarValue::Array(Arc::new(NullArray::new(num_rows)))
    }
}
