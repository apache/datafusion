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

use std::collections::VecDeque;
use std::sync::Arc;

use arrow::array::{new_null_array, Array, ArrayData, ArrayRef, MapArray, StructArray};
use arrow::compute::concat;
use arrow::datatypes::{DataType, Field, SchemaBuilder};
use arrow_buffer::{Buffer, ToByteSlice};

use datafusion_common::Result;
use datafusion_common::{exec_err, internal_err};
use datafusion_expr::{ColumnarValue, ScalarUDFImpl, Signature, Volatility};

fn make_map(args: &[ColumnarValue]) -> Result<ColumnarValue> {
    if args.is_empty() {
        return exec_err!("map requires at least one pair of arguments, got 0 instead");
    }

    if args.len() % 2 != 0 {
        return exec_err!(
            "map requires an even number of arguments, got {} instead",
            args.len()
        );
    }

    let mut value_type = args[1].data_type();

    let (key, value): (Vec<_>, Vec<_>) = args.chunks_exact(2)
        .enumerate()
        .map(|(i, chunk)| {
            if chunk[0].data_type().is_null() {
                return internal_err!("map key cannot be null");
            }
            if !chunk[1].data_type().is_null() {
                if value_type.is_null() {
                    value_type = chunk[1].data_type();
                }
                else if chunk[1].data_type() != value_type {
                    return exec_err!(
                        "map requires all values to have the same type {}, got {} instead at position {}",
                        value_type,
                        chunk[1].data_type(),
                        i
                    );
                }
            }
            Ok((chunk[0].clone(), chunk[1].clone()))
        })
        .collect::<Result<Vec<_>>>()?.into_iter().unzip();

    let key = ColumnarValue::values_to_arrays(&key)?;
    let value = ColumnarValue::values_to_arrays(&value)?;

    let mut keys = Vec::new();
    let mut values = Vec::new();

    // Since a null scalar value be transformed into [NullArray] by ColumnarValue::values_to_arrays,
    // `arrow_select::concat` will panic if we pass a [NullArray] and a non-null array to it.
    // So we need to create a [NullArray] with the same data type as the non-null array.
    let null_array = new_null_array(&value_type, 1);
    for (key, value) in key.iter().zip(value.iter()) {
        keys.push(key.as_ref());
        if value.data_type().is_null() {
            values.push(null_array.as_ref());
        } else {
            values.push(value.as_ref());
        }
    }
    let key = match concat(&keys) {
        Ok(key) => key,
        Err(e) => return internal_err!("Error concatenating keys: {}", e),
    };
    let value = match concat(&values) {
        Ok(value) => value,
        Err(e) => return internal_err!("Error concatenating values: {}", e),
    };
    make_map_batch(key, value)
}

fn make_map_batch(keys: ArrayRef, values: ArrayRef) -> Result<ColumnarValue> {
    if keys.null_count() > 0 {
        return internal_err!("map key cannot be null");
    }
    let key_field = Arc::new(Field::new("key", keys.data_type().clone(), false));
    let value_field = Arc::new(Field::new(
        "value",
        values.data_type().clone(),
        values.null_count() > 0,
    ));
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
            signature: Signature::variadic_any(Volatility::Immutable),
        }
    }
}

impl ScalarUDFImpl for MakeMap {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn name(&self) -> &str {
        "make_map"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        if arg_types.is_empty() {
            return internal_err!(
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
        let mut value_nullable = arg_types[1].is_null();

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

            value_nullable = value_nullable || chunk[1].is_null();
        }

        let mut builder = SchemaBuilder::new();
        builder.push(Field::new("key", key_type.clone(), false));
        builder.push(Field::new("value", value_type.clone(), value_nullable));
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
