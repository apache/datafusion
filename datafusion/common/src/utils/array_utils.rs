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

//! Array concatenation utilities

use std::sync::Arc;

use crate::cast::as_generic_list_array;
use crate::error::_exec_datafusion_err;
use crate::utils::list_ndims;
use crate::Result;
use arrow::array::{
    Array, ArrayData, ArrayRef, GenericListArray, NullBufferBuilder, OffsetSizeTrait,
};
use arrow::buffer::OffsetBuffer;
use arrow::datatypes::{DataType, Field};

/// Concatenates arrays
pub fn concat_arrays(args: &[ArrayRef]) -> Result<ArrayRef> {
    if args.is_empty() {
        return Err(_exec_datafusion_err!(
            "concat_arrays expects at least one argument"
        ));
    }

    let mut all_null = true;
    let mut large_list = false;
    for arg in args {
        match arg.data_type() {
            DataType::Null => continue,
            DataType::LargeList(_) => large_list = true,
            _ => (),
        }
        if arg.null_count() < arg.len() {
            all_null = false;
        }
    }

    if all_null {
        // Return a null array with the same type as the first non-null-type argument
        let return_type = args
            .iter()
            .map(|arg| arg.data_type())
            .find(|d| !d.is_null())
            .unwrap_or_else(|| args[0].data_type());

        return Ok(arrow::array::make_array(ArrayData::new_null(
            return_type,
            args[0].len(),
        )));
    }

    if large_list {
        concat_arrays_internal::<i64>(args)
    } else {
        concat_arrays_internal::<i32>(args)
    }
}

fn concat_arrays_internal<O: OffsetSizeTrait>(args: &[ArrayRef]) -> Result<ArrayRef> {
    let args = align_array_dimensions::<O>(args.to_vec())?;

    let list_arrays = args
        .iter()
        .map(|arg| as_generic_list_array::<O>(arg))
        .collect::<Result<Vec<_>>>()?;

    // Assume number of rows is the same for all arrays
    let row_count = list_arrays[0].len();

    let mut array_lengths = vec![];
    let mut arrays = vec![];
    let mut valid = NullBufferBuilder::new(row_count);
    for i in 0..row_count {
        let nulls = list_arrays
            .iter()
            .map(|arr| arr.is_null(i))
            .collect::<Vec<_>>();

        // If all the arrays are null, the concatenated array is null
        let is_null = nulls.iter().all(|&x| x);
        if is_null {
            array_lengths.push(0);
            valid.append_null();
        } else {
            // Get all the arrays on i-th row
            let values = list_arrays
                .iter()
                .map(|arr| arr.value(i))
                .collect::<Vec<_>>();

            let elements = values
                .iter()
                .map(|a| a.as_ref())
                .collect::<Vec<&dyn Array>>();

            // Concatenated array on i-th row
            let concatenated_array = arrow::compute::concat(elements.as_slice())?;
            array_lengths.push(concatenated_array.len());
            arrays.push(concatenated_array);
            valid.append_non_null();
        }
    }
    // Assume all arrays have the same data type
    let data_type = list_arrays[0].value_type();

    let elements = arrays
        .iter()
        .map(|a| a.as_ref())
        .collect::<Vec<&dyn Array>>();

    let list_arr = GenericListArray::<O>::new(
        Arc::new(Field::new_list_field(data_type, true)),
        OffsetBuffer::from_lengths(array_lengths),
        Arc::new(arrow::compute::concat(elements.as_slice())?),
        valid.finish(),
    );

    Ok(Arc::new(list_arr))
}

/// Aligns array dimensions
fn align_array_dimensions<O: OffsetSizeTrait>(
    args: Vec<ArrayRef>,
) -> Result<Vec<ArrayRef>> {
    let args_ndim = args
        .iter()
        .map(|arg| list_ndims(arg.data_type()))
        .collect::<Vec<_>>();
    let max_ndim = args_ndim.iter().max().unwrap_or(&0);

    // Align the dimensions of the arrays
    let aligned_args: Result<Vec<ArrayRef>> = args
        .into_iter()
        .zip(args_ndim.iter())
        .map(|(array, ndim)| {
            if ndim < max_ndim {
                let mut aligned_array = Arc::clone(&array);
                for _ in 0..(max_ndim - ndim) {
                    let data_type = aligned_array.data_type().to_owned();
                    let array_lengths = vec![1; aligned_array.len()];
                    let offsets = OffsetBuffer::<O>::from_lengths(array_lengths);

                    let field = Arc::new(Field::new("item", data_type, true));
                    let aligned_array_inner =
                        GenericListArray::<O>::new(field, offsets, aligned_array, None);
                    aligned_array = Arc::new(aligned_array_inner);
                }
                Ok(aligned_array)
            } else {
                Ok(array)
            }
        })
        .collect();

    aligned_args
}
