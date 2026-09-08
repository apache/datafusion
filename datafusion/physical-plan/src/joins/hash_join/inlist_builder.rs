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

use arrow::array::{Array, ArrayRef, StructArray, UInt32Array};
use arrow::compute::take;
use arrow::datatypes::{Field, FieldRef, Fields};
use arrow::row::{Row, RowConverter, SortField};
use arrow_schema::DataType;
use datafusion_common::Result;

pub(super) fn build_struct_fields(data_types: &[DataType]) -> Result<Fields> {
    data_types
        .iter()
        .enumerate()
        .map(|(i, dt)| Ok(Field::new(format!("c{i}"), dt.clone(), true)))
        .collect()
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
/// The returned array is deduplicated (see [`dedup_inlist_values`]): the build side is
/// gated on its *distinct* key count, not its row count, so the raw key arrays routinely
/// contain far more rows than distinct values. An `IN` list is a set, so dropping the
/// duplicates cannot change any result, but it does shrink everything downstream that is
/// sized by the list length: the per-value [`ScalarValue`] literals built by
/// `InListExpr::try_new_from_array`, and — wherever the pushed-down filter reaches a
/// `PruningPredicate` — the `LiteralGuarantee` it materializes per row group.
///
/// [`ScalarValue`]: datafusion_common::ScalarValue
///
/// Returns `None` if the estimated size exceeds `max_size_bytes` or if the number of rows
/// exceeds `max_distinct_values`.
pub(super) fn build_struct_inlist_values(
    join_key_arrays: &[ArrayRef],
) -> Result<Option<ArrayRef>> {
    // Build the source array/struct
    let source_array: ArrayRef = if join_key_arrays.len() == 1 {
        // Single column: use directly
        Arc::clone(&join_key_arrays[0])
    } else {
        // Multi-column: build StructArray once from all columns
        let fields = build_struct_fields(
            &join_key_arrays
                .iter()
                .map(|arr| arr.data_type().clone())
                .collect::<Vec<_>>(),
        )?;

        // Build field references with proper Arc wrapping
        let arrays_with_fields: Vec<(FieldRef, ArrayRef)> = fields
            .iter()
            .cloned()
            .zip(join_key_arrays.iter().cloned())
            .collect();

        Arc::new(StructArray::from(arrays_with_fields))
    };

    Ok(Some(dedup_inlist_values(source_array)?))
}

/// Removes duplicate entries from an `IN` list value array, preserving first-occurrence order.
///
/// Equality is the arrow row-format byte equality produced by [`RowConverter`], which is
/// exactly the equality used elsewhere in DataFusion for grouping. In particular NULLs
/// compare equal to each other, so a list with many NULLs collapses to a single NULL. That
/// is semantics-preserving for `IN`: one NULL in the haystack already yields the same
/// three-valued result as a thousand.
///
/// The input is returned unchanged when it holds fewer than two rows, when it contains no
/// duplicates, or when its type cannot be row-encoded (dedup is an optimization, never a
/// requirement).
fn dedup_inlist_values(values: ArrayRef) -> Result<ArrayRef> {
    if values.len() < 2 {
        return Ok(values);
    }

    let sort_field = SortField::new(values.data_type().clone());
    if !RowConverter::supports_fields(std::slice::from_ref(&sort_field)) {
        return Ok(values);
    }

    let converter = RowConverter::new(vec![sort_field])?;
    let rows = converter.convert_columns(std::slice::from_ref(&values))?;

    let mut seen: HashSet<Row> = HashSet::with_capacity(values.len());
    let mut indices: Vec<u32> = Vec::new();
    for (idx, row) in rows.iter().enumerate() {
        if seen.insert(row) {
            indices.push(idx as u32);
        }
    }

    if indices.len() == values.len() {
        // Nothing to remove: skip the copy.
        return Ok(values);
    }

    Ok(take(values.as_ref(), &UInt32Array::from(indices), None)?)
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{
        AsArray, DictionaryArray, Int8Array, Int32Array, StringArray,
        StringDictionaryBuilder,
    };
    use arrow::datatypes::Int32Type;

    #[test]
    fn test_build_single_column_inlist_array() {
        let array = Arc::new(Int32Array::from(vec![1, 2, 3, 2, 1])) as ArrayRef;
        let result = build_struct_inlist_values(std::slice::from_ref(&array))
            .unwrap()
            .unwrap();

        // Duplicates are dropped, first-occurrence order is preserved.
        let expected = Arc::new(Int32Array::from(vec![1, 2, 3])) as ArrayRef;
        assert!(expected.eq(&result));
    }

    #[test]
    fn test_build_single_column_inlist_array_without_duplicates() {
        let array = Arc::new(Int32Array::from(vec![3, 1, 2])) as ArrayRef;
        let result = build_struct_inlist_values(std::slice::from_ref(&array))
            .unwrap()
            .unwrap();

        assert!(array.eq(&result));
    }

    #[test]
    fn test_build_single_column_inlist_array_with_nulls() {
        let array = Arc::new(Int32Array::from(vec![
            Some(1),
            None,
            Some(2),
            None,
            Some(1),
        ])) as ArrayRef;
        let result = build_struct_inlist_values(std::slice::from_ref(&array))
            .unwrap()
            .unwrap();

        // A single NULL is kept: `IN` is a set, and one NULL in the haystack already
        // produces the same three-valued result as many.
        let expected =
            Arc::new(Int32Array::from(vec![Some(1), None, Some(2)])) as ArrayRef;
        assert!(expected.eq(&result));
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
        // Deduplication is on the whole tuple, not per column.
        assert_eq!(result.len(), 3);
        let struct_array = result.as_struct();
        assert_eq!(
            struct_array.column(0).as_primitive::<Int32Type>().values(),
            &[1, 2, 3]
        );
        assert_eq!(
            struct_array
                .column(1)
                .as_string::<i32>()
                .iter()
                .collect::<Vec<_>>(),
            vec![Some("a"), Some("b"), Some("c")]
        );
    }

    #[test]
    fn test_build_multi_column_inlist_distinct_tuples_from_duplicate_columns() {
        // Each column on its own has duplicates, but every (a, b) tuple is distinct.
        let array1 = Arc::new(Int32Array::from(vec![1, 1, 2, 2])) as ArrayRef;
        let array2 = Arc::new(StringArray::from(vec!["a", "b", "a", "b"])) as ArrayRef;

        let result = build_struct_inlist_values(&[array1, array2])
            .unwrap()
            .unwrap();

        assert_eq!(result.len(), 4);
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
                build_struct_fields(&[
                    DataType::Dictionary(
                        Box::new(DataType::Int8),
                        Box::new(DataType::Utf8)
                    ),
                    DataType::Int32
                ])
                .unwrap()
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

        // All three rows decode to "foo", so the list collapses to one entry while
        // keeping the dictionary encoding.
        assert_eq!(result.len(), 1);
        assert_eq!(result.data_type(), dict_array.data_type());
    }
}
