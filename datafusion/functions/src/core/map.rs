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

use arrow::array::{Array, ArrayData, ArrayRef, Int32Builder, MapArray, MapBuilder, StringBuilder, StructArray};
use arrow::datatypes::{DataType, Field};
use arrow_buffer::{Buffer, ToByteSlice};
use datafusion_common::{exec_err, ScalarValue};
use datafusion_common::Result;
use datafusion_expr::ColumnarValue;

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

    let string_builder = StringBuilder::new();
    let int_builder = Int32Builder::with_capacity(args.len() / 2);
    let mut builder =
        MapBuilder::new(None, string_builder, int_builder);

    for (_, chunk) in args.chunks_exact(2).enumerate() {
        let key = chunk[0].clone();
        let value = chunk[1].clone();
        match key {
            ColumnarValue::Scalar(ScalarValue::Utf8(Some(key_scalar))) => {
                builder.keys().append_value(key_scalar)
            }
            _ => {}
        }

        match value {
            ColumnarValue::Scalar(ScalarValue::Int32(Some(value_scalar))) => {
                builder.values().append_value(value_scalar)
            }
            _ => {}
        }
    }

    Ok(ColumnarValue::Array(Arc::new(builder.finish())))
}



fn make_map_batch(args: &[ArrayRef]) -> Result<ArrayRef> {
    if args.len() % 2 != 0 {
        return exec_err!("map requires an even number of arguments, got {} instead", args.len());
    }

    let key_field = Arc::new(Field::new("key", args[0].data_type().clone(), args[0].null_count() > 0));
    let value_field = Arc::new(Field::new("value", args[1].data_type().clone(), args[1].null_count() > 0));
    let mut entry_struct_buffer = VecDeque::new();
    let mut entry_offsets_buffer = VecDeque::new();
    entry_offsets_buffer.push_back(0);

    for key_value in args.chunks_exact(2) {
        let key = &key_value[0];
        let value = &key_value[1];
        if key.data_type() != args[0].data_type() {
            return exec_err!("map key type must be consistent, got {:?} and {:?}", key.data_type(), args[0].data_type());
        }
        if value.data_type() != args[1].data_type() {
            return exec_err!("map value type must be consistent, got {:?} and {:?}", value.data_type(), args[1].data_type());
        }
        if key.len() != value.len() {
            return exec_err!("map key and value must have the same length, got {} and {}", key.len(), value.len());
        }

        entry_struct_buffer.push_back((Arc::clone(&key_field), Arc::clone(key)));
        entry_struct_buffer.push_back((Arc::clone(&value_field), Arc::clone(value)));
        entry_offsets_buffer.push_back(key.len() as u32);
    }
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

    Ok(Arc::new(MapArray::from(map_data)))
}


#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow::array::{Int32Array, StringArray};

    use datafusion_common::Result;
    use datafusion_expr::ColumnarValue;
    use std::time::Instant;
    use rand::random;
    use super::*;

    #[test]
    fn test_make_map() -> Result<()> {

        // the first run is for warm up.
        for num in vec![1, 10, 50, 100, 500, 1000] {
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

            println!("Time elapsed in make_map() is: {:?}", duration);


            let key_vec: Vec<String> = key_buffer.into();
            let value_vec: Vec<i32> = value_buffer.into();
            let key = Arc::new(StringArray::from(key_vec));
            let value = Arc::new(Int32Array::from(value_vec));

            let start = Instant::now();
            let map = make_map_batch(&[key, value])?;
            let duration = start.elapsed();
            println!("Time elapsed in make_map_batch() is: {:?}", duration);
        }
        Ok(())
    }
}
