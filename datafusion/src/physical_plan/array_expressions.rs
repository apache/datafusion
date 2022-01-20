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
use arrow::datatypes::DataType;

use super::ColumnarValue;

fn array_array(arrays: &[&dyn Array]) -> Result<ArrayRef> {
    assert!(!arrays.is_empty());
    let first = arrays[0];
    assert!(arrays.iter().all(|x| x.len() == first.len()));
    assert!(arrays.iter().all(|x| x.data_type() == first.data_type()));

    let size = arrays.len();

    macro_rules! array {
        ($PRIMITIVE: ty, $ARRAY: ty, $DATA_TYPE: path) => {{
            let array = MutablePrimitiveArray::<$PRIMITIVE>::with_capacity_from(
                first.len() * size,
                $DATA_TYPE,
            );
            let mut array = MutableFixedSizeListArray::new(array, size);
            array.try_extend(
                // for each entry in the array
                (0..first.len()).map(|idx| {
                    Some(arrays.iter().map(move |arg| {
                        let arg = arg.as_any().downcast_ref::<$ARRAY>().unwrap();
                        if arg.is_null(idx) {
                            None
                        } else {
                            Some(arg.value(idx))
                        }
                    }))
                }),
            )?;
            Ok(array.as_arc())
        }};
    }

    macro_rules! array_string {
        ($OFFSET: ty) => {{
            let array = MutableUtf8Array::<$OFFSET>::with_capacity(first.len() * size);
            let mut array = MutableFixedSizeListArray::new(array, size);
            array.try_extend(
                // for each entry in the array
                (0..first.len()).map(|idx| {
                    Some(arrays.iter().map(move |arg| {
                        let arg =
                            arg.as_any().downcast_ref::<Utf8Array<$OFFSET>>().unwrap();
                        if arg.is_null(idx) {
                            None
                        } else {
                            Some(arg.value(idx))
                        }
                    }))
                }),
            )?;
            Ok(array.as_arc())
        }};
    }

    match first.data_type() {
        DataType::Boolean => {
            let array = MutableBooleanArray::with_capacity(first.len() * size);
            let mut array = MutableFixedSizeListArray::new(array, size);
            array.try_extend(
                // for each entry in the array
                (0..first.len()).map(|idx| {
                    Some(arrays.iter().map(move |arg| {
                        let arg = arg.as_any().downcast_ref::<BooleanArray>().unwrap();
                        if arg.is_null(idx) {
                            None
                        } else {
                            Some(arg.value(idx))
                        }
                    }))
                }),
            )?;
            Ok(array.as_arc())
        }
        DataType::UInt8 => array!(u8, PrimitiveArray<u8>, DataType::UInt8),
        DataType::UInt16 => array!(u16, PrimitiveArray<u16>, DataType::UInt16),
        DataType::UInt32 => array!(u32, PrimitiveArray<u32>, DataType::UInt32),
        DataType::UInt64 => array!(u64, PrimitiveArray<u64>, DataType::UInt64),
        DataType::Int8 => array!(i8, PrimitiveArray<i8>, DataType::Int8),
        DataType::Int16 => array!(i16, PrimitiveArray<i16>, DataType::Int16),
        DataType::Int32 => array!(i32, PrimitiveArray<i32>, DataType::Int32),
        DataType::Int64 => array!(i64, PrimitiveArray<i64>, DataType::Int64),
        DataType::Float32 => array!(f32, PrimitiveArray<f32>, DataType::Float32),
        DataType::Float64 => array!(f64, PrimitiveArray<f64>, DataType::Float64),
        DataType::Utf8 => array_string!(i32),
        DataType::LargeUtf8 => array_string!(i64),
        data_type => Err(DataFusionError::NotImplemented(format!(
            "Array is not implemented for type '{:?}'.",
            data_type
        ))),
    }
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

    Ok(ColumnarValue::Array(array_array(&arrays)?))
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
