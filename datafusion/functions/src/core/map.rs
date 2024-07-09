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

use arrow::array::{Array, ArrayData, ArrayRef, MapArray, StructArray};
use arrow::compute::concat;
use arrow::datatypes::{DataType, Field, SchemaBuilder};
use arrow_buffer::{Buffer, ToByteSlice};

use datafusion_common::{exec_err, internal_err};
use datafusion_common::Result;
use datafusion_expr::{ColumnarValue, ScalarUDFImpl, Signature, Volatility};

fn make_map(args: &[ColumnarValue]) -> Result<ColumnarValue> {
    if args.is_empty() {
        return exec_err!(
            "map requires at least one pair of arguments, got 0 instead"
        );
    }

    if args.len() % 2 != 0 {
        return exec_err!(
            "map requires an even number of arguments, got {} instead",
            args.len()
        );
    }
    let (key_buffer, value_buffer): (Vec<_>, Vec<_>) = args.chunks_exact(2)
        .map(|chunk| (chunk[0].clone(), chunk[1].clone()))
        .unzip();

    let key: Vec<_> = key_buffer.into();
    let value: Vec<_> = value_buffer.into();
    let key = ColumnarValue::values_to_arrays(&key)?;
    let value = ColumnarValue::values_to_arrays(&value)?;

    let mut keys = Vec::new();
    let mut values = Vec::new();

    for (key, value) in key.iter().zip(value.iter()) {
        keys.push(key.as_ref());
        values.push(value.as_ref());
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
    let key_field = Arc::new(Field::new("key", keys.data_type().clone(), keys.null_count() > 0));
    let value_field = Arc::new(Field::new("value", values.data_type().clone(), values.null_count() > 0));
    let mut entry_struct_buffer: VecDeque<(Arc<Field>, ArrayRef)> = VecDeque::new();
    let mut entry_offsets_buffer = VecDeque::new();
    entry_offsets_buffer.push_back(0);

    entry_struct_buffer.push_back((Arc::clone(&key_field), Arc::clone(&keys)));
    entry_struct_buffer.push_back((Arc::clone(&value_field), Arc::clone(&values)));
    entry_offsets_buffer.push_back(keys.len() as u32);

    let entry_struct: Vec<(Arc<Field>, ArrayRef)> = entry_struct_buffer.into();
    let entry_struct = StructArray::from(entry_struct);

    let map_data_type = DataType::Map(
        Arc::new(Field::new("entries",
                            entry_struct.data_type().clone(), false)),
        false);

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

        let key_type = arg_types[0].clone();
        let value_type = arg_types[1].clone();
        let mut key_nullable = arg_types[0].is_null();
        let mut value_nullable = arg_types[1].is_null();

        for (i, chunk) in arg_types.chunks_exact(2).enumerate() {
            if !chunk[0].is_null() && chunk[0] != key_type {
                return exec_err!(
                    "make_map requires all keys to have the same type, got {} instead at position {}",
                    chunk[0],
                    i
                );
            }
            if !chunk[1].is_null() && chunk[1] != value_type {
                return exec_err!(
                    "make_map requires all values to have the same type, got {} instead at position {}",
                    chunk[1],
                    i
                );
            }
            key_nullable = key_nullable || chunk[0].is_null();
            value_nullable = value_nullable || chunk[1].is_null();
        }

        let mut builder = SchemaBuilder::new();
        builder.push(Field::new("key", key_type, key_nullable));
        builder.push(Field::new("value", value_type, value_nullable));
        let fields = builder.finish().fields;
        Ok(DataType::Map(
            Arc::new(Field::new("entries",
                                DataType::Struct(fields),
                                false)),
            false))
    }

    fn invoke(&self, args: &[ColumnarValue]) -> Result<ColumnarValue> {
        make_map(args)
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::time::Instant;

    use arrow::array::{Int32Array, StringArray};
    use rand::random;

    use datafusion_common::{Result, ScalarValue};
    use datafusion_expr::ColumnarValue;

    use super::*;

    #[test]
    fn test_make_map() -> Result<()> {
        // the first run is for warm up.
        for num in vec![1, 2, 4, 8, 10, 50, 100, 1000] {
            let mut buffer= VecDeque::new();
            let mut key_buffer=  VecDeque::new();
            let mut value_buffer = VecDeque::new();

            println!("Generating {} random key-value pairs", num);
            for _ in 0..num {
                let rand: i32 = random();
                let key = format!("key_{}", rand.clone());
                key_buffer.push_back(key.clone());
                value_buffer.push_back(rand.clone());
                buffer.push_back(ColumnarValue::Scalar(ScalarValue::Utf8(Some(key.clone()))));
                buffer.push_back(ColumnarValue::Scalar(ScalarValue::Int32(Some(rand.clone()))));
            }

            let record: Vec<ColumnarValue> = buffer.into();
            let start = Instant::now();
            let map = make_map(&record)?;
            let duration = start.elapsed();

            println!("Time elapsed in make_map_from() is: {:?}", duration);


            let key_vec: Vec<String> = key_buffer.into();
            let value_vec: Vec<i32> = value_buffer.into();
            let key = Arc::new(StringArray::from(key_vec));
            let value = Arc::new(Int32Array::from(value_vec));

            let start = Instant::now();
            let map = make_map_batch(key, value)?;
            let duration = start.elapsed();
            println!("Time elapsed in make_map_batch() is: {:?}", duration);
        }
        Ok(())
    }
}
