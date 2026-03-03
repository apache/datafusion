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
use arrow::compute::cast;
use arrow::datatypes::{Field, FieldRef, Fields};
use arrow_schema::DataType;
use datafusion_common::Result;

pub(super) fn build_struct_fields(data_types: &[DataType]) -> Result<Fields> {
    data_types
        .iter()
        .enumerate()
        .map(|(i, dt)| Ok(Field::new(format!("c{i}"), dt.clone(), true)))
        .collect()
}

/// Casts dictionary-encoded arrays to their underlying value type, preserving row count.
/// Non-dictionary arrays are returned as-is.
fn flatten_dictionary_array(array: &ArrayRef) -> Result<ArrayRef> {
    match array.data_type() {
        DataType::Dictionary(_, value_type) => {
            let casted = cast(array, value_type)?;
            // Recursively flatten in case of nested dictionaries
            flatten_dictionary_array(&casted)
        }
        _ => Ok(Arc::clone(array)),
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
/// Note that this function does not deduplicate values - deduplication will happen later
/// when building an InList expression from this array via `InListExpr::try_new_from_array`.
///
/// Returns `None` if the estimated size exceeds `max_size_bytes` or if the number of rows
/// exceeds `max_distinct_values`.
pub(super) fn build_struct_inlist_values(
    join_key_arrays: &[ArrayRef],
) -> Result<Option<ArrayRef>> {
    // Flatten any dictionary-encoded arrays
    let flattened_arrays: Vec<ArrayRef> = join_key_arrays
        .iter()
        .map(flatten_dictionary_array)
        .collect::<Result<Vec<_>>>()?;

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
    use arrow::array::{
        DictionaryArray, Int8Array, Int32Array, StringArray, StringDictionaryBuilder,
    };
    use arrow_schema::DataType;
    use std::sync::Arc;

    #[test]
    fn test_build_single_column_inlist_array() {
        let array = Arc::new(Int32Array::from(vec![1, 2, 3, 2, 1])) as ArrayRef;
        let result = build_struct_inlist_values(std::slice::from_ref(&array))
            .unwrap()
            .unwrap();

        assert!(array.eq(&result));
    }

    #[test]
    fn test_build_multi_column_inlist() {
        let array1 = Arc::new(Int32Array::from(vec![1, 2, 3, 2, 1])) as ArrayRef;
        let array2 =
            Arc::new(StringArray::from(vec!["a", "b", "c", "b", "a"])) as ArrayRef;

        let result = build_struct_inlist_values(&[array1, array2])
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
    fn test_build_multi_column_inlist_with_dictionary() {
        let mut builder = StringDictionaryBuilder::<arrow::datatypes::Int8Type>::new();
        builder.append_value("foo");
        builder.append_value("foo");
        builder.append_value("foo");
        let dict_array = Arc::new(builder.finish()) as ArrayRef;

        let int_array = Arc::new(Int32Array::from(vec![1, 2, 3])) as ArrayRef;

        let result = build_struct_inlist_values(&[dict_array, int_array])
            .unwrap()
            .unwrap();

        assert_eq!(result.len(), 3);
        assert_eq!(
            *result.data_type(),
            DataType::Struct(
                build_struct_fields(&[DataType::Utf8, DataType::Int32]).unwrap()
            )
        );
    }

    #[test]
    fn test_build_single_column_dictionary_inlist() {
        let keys = Int8Array::from(vec![0i8, 0, 0]);
        let values = Arc::new(StringArray::from(vec!["foo"]));
        let dict_array = Arc::new(DictionaryArray::new(keys, values)) as ArrayRef;

        let result = build_struct_inlist_values(std::slice::from_ref(&dict_array))
            .unwrap()
            .unwrap();

        assert_eq!(result.len(), 3);
        assert_eq!(*result.data_type(), DataType::Utf8);
    }
}
