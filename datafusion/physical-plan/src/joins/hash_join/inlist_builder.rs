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

//! Utilities for building InList expressions from hash join build side data

use std::sync::Arc;

use arrow::array::{ArrayRef, StructArray};
use arrow::datatypes::{Field, FieldRef, Fields};
use arrow::downcast_dictionary_array;
use arrow_schema::DataType;
use datafusion_common::Result;

pub(super) fn build_struct_fields(data_types: &[DataType]) -> Result<Fields> {
    data_types
        .iter()
        .enumerate()
        .map(|(i, dt)| Ok(Field::new(format!("c{i}"), dt.clone(), true)))
        .collect()
}

/// Flattens dictionary-encoded arrays to their underlying value arrays.
/// Non-dictionary arrays are returned as-is.
fn flatten_dictionary_array(array: &ArrayRef) -> ArrayRef {
    downcast_dictionary_array! {
        array => {
            // Recursively flatten in case of nested dictionaries
            flatten_dictionary_array(array.values())
        }
        _ => Arc::clone(array)
    }
}

/// Builds InList values from join key column arrays.
///
/// If `join_key_arrays` is:
/// 1. A single array, let's say Int32, this will produce a flat
///    InList expression where the lookup is expected to be scalar Int32 values,
///    that is: this will produce `IN LIST (1, 2, 3)` expected to be used as `2 IN LIST (1, 2, 3)`.
/// 2. An Int32 array and a Utf8 array, this will produce a Struct InList expression
///    where the lookup is expected to be Struct values with two fields (Int32, Utf8),
///    that is: this will produce `IN LIST ((1, "a"), (2, "b"))` expected to be used as `(2, "b") IN LIST ((1, "a"), (2, "b"))`.
///    The field names of the struct are auto-generated as "c0", "c1", ... and should match the struct expression used in the join keys.
///
/// Note that this will not deduplicate values, that will happen later when building an InList expression from this array.
///
/// Returns `None` if the estimated size exceeds `max_size_bytes`.
/// Performs deduplication to ensure unique values only.
pub(super) fn build_struct_inlist_values(
    join_key_arrays: &[ArrayRef],
    max_size_bytes: usize,
) -> Result<Option<ArrayRef>> {
    if join_key_arrays.is_empty() {
        return Ok(None);
    }

    let num_rows = join_key_arrays[0].len();
    if num_rows == 0 {
        return Ok(None);
    }

    // Flatten any dictionary-encoded arrays
    let flattened_arrays: Vec<ArrayRef> = join_key_arrays
        .iter()
        .map(flatten_dictionary_array)
        .collect();

    // Size check using built-in method
    // This is not 1:1 with the actual size of ScalarValues, but it is a good approximation
    // and at this point is basically "free" to compute since we have the arrays already.
    let estimated_size = flattened_arrays
        .iter()
        .map(|arr| arr.get_array_memory_size())
        .sum::<usize>();

    if estimated_size > max_size_bytes {
        return Ok(None);
    }

    // Build the source array/struct
    let source_array: ArrayRef = if flattened_arrays.len() == 1 {
        // Single column: use directly
        Arc::clone(&flattened_arrays[0])
    } else {
        // Multi-column: build StructArray once from all columns
        let fields = build_struct_fields(
            &flattened_arrays
                .iter()
                .map(|arr| arr.data_type().clone())
                .collect::<Vec<_>>(),
        )?;

        // Build field references with proper Arc wrapping
        let arrays_with_fields: Vec<(FieldRef, ArrayRef)> = fields
            .iter()
            .cloned()
            .zip(flattened_arrays.iter().cloned())
            .collect();

        Arc::new(StructArray::from(arrays_with_fields))
    };

    Ok(Some(source_array))
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Int32Array, StringArray};
    use arrow_schema::DataType;
    use std::sync::Arc;

    #[test]
    fn test_build_single_column_inlist_array() {
        let array = Arc::new(Int32Array::from(vec![1, 2, 3, 2, 1])) as ArrayRef;
        let result =
            build_struct_inlist_values(std::slice::from_ref(&array), 1024 * 1024)
                .unwrap()
                .unwrap();

        assert!(array.eq(&result));
    }

    #[test]
    fn test_build_multi_column_inlist() {
        let array1 = Arc::new(Int32Array::from(vec![1, 2, 3, 2, 1])) as ArrayRef;
        let array2 =
            Arc::new(StringArray::from(vec!["a", "b", "c", "b", "a"])) as ArrayRef;

        let result = build_struct_inlist_values(&[array1, array2], 1024 * 1024)
            .unwrap()
            .unwrap();

        assert_eq!(
            *result.data_type(),
            DataType::Struct(
                build_struct_fields(&[DataType::Int32, DataType::Utf8]).unwrap()
            )
        );
    }

    #[test]
    fn test_size_limit_exceeded() {
        let array = Arc::new(Int32Array::from(vec![1, 2, 3, 4, 5])) as ArrayRef;

        // Set a very small size limit
        let result = build_struct_inlist_values(&[array], 10).unwrap();

        // Should return None due to size limit
        assert!(result.is_none());
    }

    #[test]
    fn test_empty_array() {
        let array = Arc::new(Int32Array::from(vec![] as Vec<i32>)) as ArrayRef;
        let result = build_struct_inlist_values(&[array], 1024).unwrap();

        assert!(result.is_none());
    }
}
