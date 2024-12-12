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

//! String kernels

use std::sync::Arc;

use arrow::{
    array::*,
    buffer::MutableBuffer,
    compute::kernels::substring::{substring as arrow_substring, substring_by_char},
    datatypes::{DataType, Int32Type},
};
use datafusion_common::DataFusionError;

/// Returns an ArrayRef with a string consisting of `length` spaces.
///
/// # Preconditions
///
/// - elements in `length` must not be negative
pub fn string_space(length: &dyn Array) -> Result<ArrayRef, DataFusionError> {
    match length.data_type() {
        DataType::Int32 => {
            let array = length.as_any().downcast_ref::<Int32Array>().unwrap();
            Ok(generic_string_space::<i32>(array))
        }
        DataType::Dictionary(_, _) => {
            let dict = as_dictionary_array::<Int32Type>(length);
            let values = string_space(dict.values())?;
            let result = DictionaryArray::try_new(dict.keys().clone(), values)?;
            Ok(Arc::new(result))
        }
        dt => panic!(
            "Unsupported input type for function 'string_space': {:?}",
            dt
        ),
    }
}

pub fn substring(array: &dyn Array, start: i64, length: u64) -> Result<ArrayRef, DataFusionError> {
    match array.data_type() {
        DataType::LargeUtf8 => substring_by_char(
            array
                .as_any()
                .downcast_ref::<LargeStringArray>()
                .expect("A large string is expected"),
            start,
            Some(length),
        )
        .map_err(|e| e.into())
        .map(|t| make_array(t.into_data())),
        DataType::Utf8 => substring_by_char(
            array
                .as_any()
                .downcast_ref::<StringArray>()
                .expect("A string is expected"),
            start,
            Some(length),
        )
        .map_err(|e| e.into())
        .map(|t| make_array(t.into_data())),
        DataType::Binary | DataType::LargeBinary => {
            arrow_substring(array, start, Some(length)).map_err(|e| e.into())
        }
        DataType::Dictionary(_, _) => {
            let dict = as_dictionary_array::<Int32Type>(array);
            let values = substring(dict.values(), start, length)?;
            let result = DictionaryArray::try_new(dict.keys().clone(), values)?;
            Ok(Arc::new(result))
        }
        dt => panic!("Unsupported input type for function 'substring': {:?}", dt),
    }
}

fn generic_string_space<OffsetSize: OffsetSizeTrait>(length: &Int32Array) -> ArrayRef {
    let array_len = length.len();
    let mut offsets = MutableBuffer::new((array_len + 1) * std::mem::size_of::<OffsetSize>());
    let mut length_so_far = OffsetSize::zero();

    // compute null bitmap (copy)
    let null_bit_buffer = length.to_data().nulls().map(|b| b.buffer().clone());

    // Gets slice of length array to access it directly for performance.
    let length_data = length.to_data();
    let lengths = length_data.buffers()[0].typed_data::<i32>();
    let total = lengths.iter().map(|l| *l as usize).sum::<usize>();
    let mut values = MutableBuffer::new(total);

    offsets.push(length_so_far);

    let blank = " ".as_bytes()[0];
    values.resize(total, blank);

    (0..array_len).for_each(|i| {
        let current_len = lengths[i] as usize;

        length_so_far += OffsetSize::from_usize(current_len).unwrap();
        offsets.push(length_so_far);
    });

    let data = unsafe {
        ArrayData::new_unchecked(
            GenericStringArray::<OffsetSize>::DATA_TYPE,
            array_len,
            None,
            null_bit_buffer,
            0,
            vec![offsets.into(), values.into()],
            vec![],
        )
    };
    make_array(data)
}
