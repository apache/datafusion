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

use std::collections::HashSet;
use std::sync::Arc;

use arrow::array::{ArrayRef, StructArray};
use arrow::datatypes::{Field, FieldRef, Fields};
use datafusion_common::{Result, ScalarValue};

/// Builds InList values from join key column arrays.
///
/// For single-column joins, creates a list: `[val1, val2, ...]`
/// For multi-column joins, creates struct values: `[{c0:1, c1:2}, {c0:3, c1:4}, ...]`
///
/// Returns `None` if the estimated size exceeds `max_size_bytes`.
/// Performs deduplication to ensure unique values only.
pub fn build_struct_inlist_values(
    join_key_arrays: &[ArrayRef],
    max_size_bytes: usize,
) -> Result<Option<Vec<ScalarValue>>> {
    if join_key_arrays.is_empty() {
        return Ok(Some(vec![]));
    }

    let num_rows = join_key_arrays[0].len();
    if num_rows == 0 {
        return Ok(Some(vec![]));
    }

    // Size check using built-in method
    let estimated_size = join_key_arrays
        .iter()
        .map(|arr| arr.get_array_memory_size())
        .sum::<usize>();
    if estimated_size > max_size_bytes {
        return Ok(None);
    }

    // Build the source array/struct
    let source_array: ArrayRef = if join_key_arrays.len() == 1 {
        // Single column: use directly
        Arc::clone(&join_key_arrays[0])
    } else {
        // Multi-column: build StructArray once from all columns
        let fields: Fields = join_key_arrays
            .iter()
            .enumerate()
            .map(|(i, arr)| {
                Field::new(
                    format!("c{}", i),
                    arr.data_type().clone(),
                    arr.is_nullable(),
                )
            })
            .collect();

        // Build field references with proper Arc wrapping
        let arrays_with_fields: Vec<(FieldRef, ArrayRef)> = fields
            .iter()
            .cloned()
            .zip(join_key_arrays.iter().cloned())
            .collect();

        Arc::new(StructArray::from(arrays_with_fields))
    };

    // Extract unique ScalarValues row-by-row
    let mut unique_values = HashSet::new();
    let mut result = Vec::new();
    let mut cumulative_size = 0;

    for row_idx in 0..num_rows {
        let scalar = ScalarValue::try_from_array(&source_array, row_idx)?;

        // Only add if unique
        if unique_values.insert(scalar.clone()) {
            cumulative_size += scalar.size();

            // Check size limit during deduplication
            if cumulative_size > max_size_bytes {
                return Ok(None);
            }

            result.push(scalar);
        }
    }

    // Sort for deterministic results and better filter performance
    result.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));

    Ok(Some(result))
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Int32Array, StringArray};
    use std::sync::Arc;

    #[test]
    fn test_build_single_column_inlist() {
        let array = Arc::new(Int32Array::from(vec![1, 2, 3, 2, 1])) as ArrayRef;
        let result = build_struct_inlist_values(&[array], 1024 * 1024).unwrap();

        assert!(result.is_some());
        let values = result.unwrap();

        // Should have 3 unique values
        assert_eq!(values.len(), 3);

        // Should be sorted
        assert_eq!(values[0], ScalarValue::Int32(Some(1)));
        assert_eq!(values[1], ScalarValue::Int32(Some(2)));
        assert_eq!(values[2], ScalarValue::Int32(Some(3)));
    }

    #[test]
    fn test_build_multi_column_inlist() {
        let array1 = Arc::new(Int32Array::from(vec![1, 2, 3, 2, 1])) as ArrayRef;
        let array2 =
            Arc::new(StringArray::from(vec!["a", "b", "c", "b", "a"])) as ArrayRef;

        let result = build_struct_inlist_values(&[array1, array2], 1024 * 1024).unwrap();

        assert!(result.is_some());
        let values = result.unwrap();

        // Should have 3 unique tuples: (1,"a"), (2,"b"), (3,"c")
        assert_eq!(values.len(), 3);

        // All should be struct values
        for value in &values {
            assert!(matches!(value, ScalarValue::Struct(_)));
        }
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

        assert!(result.is_some());
        assert_eq!(result.unwrap().len(), 0);
    }
}
