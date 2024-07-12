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

use std::any::Any;
use std::collections::VecDeque;
use std::sync::Arc;

use arrow::array::{Array, ArrayData, ArrayRef, MapArray, StructArray};
use arrow::compute::concat;
use arrow::datatypes::{DataType, Field, SchemaBuilder};
use arrow_buffer::{Buffer, ToByteSlice};

use datafusion_common::{exec_err, internal_err, ScalarValue};
use datafusion_common::{not_impl_err, Result};
use datafusion_expr::{ColumnarValue, ScalarUDFImpl, Signature, Volatility};

fn make_map(args: &[ColumnarValue]) -> Result<ColumnarValue> {
    let (key, value): (Vec<_>, Vec<_>) = args
        .chunks_exact(2)
        .map(|chunk| {
            if let ColumnarValue::Array(_) = chunk[0] {
                return not_impl_err!("make_map does not support array keys");
            }
            if let ColumnarValue::Array(_) = chunk[1] {
                return not_impl_err!("make_map does not support array values");
            }
            Ok((chunk[0].clone(), chunk[1].clone()))
        })
        .collect::<Result<Vec<_>>>()?
        .into_iter()
        .unzip();

    let keys = ColumnarValue::values_to_arrays(&key)?;
    let values = ColumnarValue::values_to_arrays(&value)?;

    let keys: Vec<_> = keys.iter().map(|k| k.as_ref()).collect();
    let values: Vec<_> = values.iter().map(|v| v.as_ref()).collect();

    let key = match concat(&keys) {
        Ok(key) => key,
        Err(e) => return internal_err!("Error concatenating keys: {}", e),
    };
    let value = match concat(&values) {
        Ok(value) => value,
        Err(e) => return internal_err!("Error concatenating values: {}", e),
    };
    make_map_batch_internal(key, value)
}

fn make_map_batch(args: &[ColumnarValue]) -> Result<ColumnarValue> {
    if args.len() != 2 {
        return exec_err!(
            "make_map requires exactly 2 arguments, got {} instead",
            args.len()
        );
    }
    let key = get_first_array_ref(&args[0])?;
    let value = get_first_array_ref(&args[1])?;
    make_map_batch_internal(key, value)
}

fn get_first_array_ref(columnar_value: &ColumnarValue) -> Result<ArrayRef> {
    match columnar_value {
        ColumnarValue::Scalar(value) => match value {
            ScalarValue::List(array) => Ok(array.value(0).clone()),
            ScalarValue::LargeList(array) => Ok(array.value(0).clone()),
            ScalarValue::FixedSizeList(array) => Ok(array.value(0).clone()),
            _ => exec_err!("Expected array, got {:?}", value),
        },
        ColumnarValue::Array(array) => exec_err!("Expected scalar, got {:?}", array),
    }
}

fn make_map_batch_internal(keys: ArrayRef, values: ArrayRef) -> Result<ColumnarValue> {
    if keys.null_count() > 0 {
        return exec_err!("map key cannot be null");
    }

    if keys.len() != values.len() {
        return exec_err!("map requires key and value lists to have the same length");
    }

    let key_field = Arc::new(Field::new("key", keys.data_type().clone(), false));
    let value_field = Arc::new(Field::new("value", values.data_type().clone(), true));
    let mut entry_struct_buffer: VecDeque<(Arc<Field>, ArrayRef)> = VecDeque::new();
    let mut entry_offsets_buffer = VecDeque::new();
    entry_offsets_buffer.push_back(0);

    entry_struct_buffer.push_back((Arc::clone(&key_field), Arc::clone(&keys)));
    entry_struct_buffer.push_back((Arc::clone(&value_field), Arc::clone(&values)));
    entry_offsets_buffer.push_back(keys.len() as u32);

    let entry_struct: Vec<(Arc<Field>, ArrayRef)> = entry_struct_buffer.into();
    let entry_struct = StructArray::from(entry_struct);

    let map_data_type = DataType::Map(
        Arc::new(Field::new(
            "entries",
            entry_struct.data_type().clone(),
            false,
        )),
        false,
    );

    let entry_offsets: Vec<u32> = entry_offsets_buffer.into();
    let entry_offsets_buffer = Buffer::from(entry_offsets.to_byte_slice());

    let map_data = ArrayData::builder(map_data_type)
        .len(entry_offsets.len() - 1)
        .add_buffer(entry_offsets_buffer)
        .add_child_data(entry_struct.to_data())
        .build()?;

    Ok(ColumnarValue::Array(Arc::new(MapArray::from(map_data))))
}

#[derive(Debug)]
pub struct MakeMap {
    signature: Signature,
}

impl Default for MakeMap {
    fn default() -> Self {
        Self::new()
    }
}

impl MakeMap {
    pub fn new() -> Self {
        Self {
            signature: Signature::user_defined(Volatility::Immutable),
        }
    }
}

impl ScalarUDFImpl for MakeMap {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "make_map"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn coerce_types(&self, arg_types: &[DataType]) -> Result<Vec<DataType>> {
        if arg_types.is_empty() {
            return exec_err!(
                "make_map requires at least one pair of arguments, got 0 instead"
            );
        }
        if arg_types.len() % 2 != 0 {
            return exec_err!(
                "make_map requires an even number of arguments, got {} instead",
                arg_types.len()
            );
        }

        let key_type = &arg_types[0];
        let mut value_type = &arg_types[1];

        for (i, chunk) in arg_types.chunks_exact(2).enumerate() {
            if chunk[0].is_null() {
                return exec_err!("make_map key cannot be null at position {}", i);
            }
            if &chunk[0] != key_type {
                return exec_err!(
                    "make_map requires all keys to have the same type {}, got {} instead at position {}",
                    key_type,
                    chunk[0],
                    i
                );
            }

            if !chunk[1].is_null() {
                if value_type.is_null() {
                    value_type = &chunk[1];
                } else if &chunk[1] != value_type {
                    return exec_err!(
                        "map requires all values to have the same type {}, got {} instead at position {}",
                        value_type,
                        &chunk[1],
                        i
                    );
                }
            }
        }

        let mut result = Vec::new();
        for _ in 0..arg_types.len() / 2 {
            result.push(key_type.clone());
            result.push(value_type.clone());
        }

        Ok(result)
    }

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        let key_type = &arg_types[0];
        let mut value_type = &arg_types[1];

        for chunk in arg_types.chunks_exact(2) {
            if !chunk[1].is_null() && value_type.is_null() {
                value_type = &chunk[1];
            }
        }

        let mut builder = SchemaBuilder::new();
        builder.push(Field::new("key", key_type.clone(), false));
        builder.push(Field::new("value", value_type.clone(), true));
        let fields = builder.finish().fields;
        Ok(DataType::Map(
            Arc::new(Field::new("entries", DataType::Struct(fields), false)),
            false,
        ))
    }

    fn invoke(&self, args: &[ColumnarValue]) -> Result<ColumnarValue> {
        make_map(args)
    }
}

#[derive(Debug)]
pub struct MapFunc {
    signature: Signature,
}

impl Default for MapFunc {
    fn default() -> Self {
        Self::new()
    }
}

impl MapFunc {
    pub fn new() -> Self {
        Self {
            signature: Signature::variadic_any(Volatility::Immutable),
        }
    }
}

impl ScalarUDFImpl for MapFunc {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "map"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        if arg_types.len() % 2 != 0 {
            return exec_err!(
                "map requires an even number of arguments, got {} instead",
                arg_types.len()
            );
        }
        let mut builder = SchemaBuilder::new();
        builder.push(Field::new(
            "key",
            get_element_type(&arg_types[0])?.clone(),
            false,
        ));
        builder.push(Field::new(
            "value",
            get_element_type(&arg_types[1])?.clone(),
            true,
        ));
        let fields = builder.finish().fields;
        Ok(DataType::Map(
            Arc::new(Field::new("entries", DataType::Struct(fields), false)),
            false,
        ))
    }

    fn invoke(&self, args: &[ColumnarValue]) -> Result<ColumnarValue> {
        make_map_batch(args)
    }
}

fn get_element_type(data_type: &DataType) -> Result<&DataType> {
    match data_type {
        DataType::List(element) => Ok(element.data_type()),
        DataType::LargeList(element) => Ok(element.data_type()),
        DataType::FixedSizeList(element, _) => Ok(element.data_type()),
        _ => exec_err!(
            "Expected list, large_list or fixed_size_list, got {:?}",
            data_type
        ),
    }
}
