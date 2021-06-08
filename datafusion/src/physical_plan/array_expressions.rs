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

//! Array expressions

use crate::error::{DataFusionError, Result};
use arrow::array::*;
use arrow::compute::concat;
use arrow::datatypes::DataType;
use std::sync::Arc;

use super::ColumnarValue;

fn array_array(arrays: &[&dyn Array]) -> Result<FixedSizeListArray> {
    assert!(!arrays.is_empty());
    let first = arrays[0];
    assert!(arrays.iter().all(|x| x.len() == first.len()));
    assert!(arrays.iter().all(|x| x.data_type() == first.data_type()));

    let size = arrays.len();

    let values = concat::concatenate(arrays)?;
    let data_type = FixedSizeListArray::default_datatype(first.data_type().clone(), size);
    Ok(FixedSizeListArray::from_data(
        data_type,
        values.into(),
        None,
    ))
}

/// put values in an array.
pub fn array(values: &[ColumnarValue]) -> Result<ColumnarValue> {
    let arrays: Vec<&dyn Array> = values
        .iter()
        .map(|value| {
            if let ColumnarValue::Array(value) = value {
                Ok(value.as_ref())
            } else {
                Err(DataFusionError::NotImplemented(
                    "Array is not implemented for scalar values.".to_string(),
                ))
            }
        })
        .collect::<Result<_>>()?;

    Ok(ColumnarValue::Array(array_array(&arrays).map(Arc::new)?))
}

/// Currently supported types by the array function.
/// The order of these types correspond to the order on which coercion applies
/// This should thus be from least informative to most informative
// `array` supports all types, but we do not have a signature to correctly
// coerce them.
pub static SUPPORTED_ARRAY_TYPES: &[DataType] = &[
    DataType::Boolean,
    DataType::UInt8,
    DataType::UInt16,
    DataType::UInt32,
    DataType::UInt64,
    DataType::Int8,
    DataType::Int16,
    DataType::Int32,
    DataType::Int64,
    DataType::Float32,
    DataType::Float64,
    DataType::Utf8,
    DataType::LargeUtf8,
];
