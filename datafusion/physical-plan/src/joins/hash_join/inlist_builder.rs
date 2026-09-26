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

use arrow::array::{ArrayRef, StructArray, UInt64Array};
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
/// Note that this function does not deduplicate values - deduplication will happen later
/// when building an InList expression from this array via `InListExpr::try_new_from_array`.
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

    Ok(Some(source_array))
}

/// Removes duplicate entries from an `IN` list value array and sorts the
/// remaining entries.
///
/// Equality and order are those of the arrow row format produced by
/// [`RowConverter`] with default [`SortField`] options (ascending, NULLs
/// first). This works for single values, for the struct values of
/// multi-column keys and for dictionaries. NULLs compare equal to each other,
/// so many NULLs collapse into one NULL. That does not change the result of
/// `IN`: one NULL in the list gives the same three-valued result as many.
///
/// Sorting makes the list independent of the order in which build rows
/// arrive, so the displayed filter is deterministic.
///
/// Returns the input unchanged when it has fewer than two entries, or when
/// its type cannot be row-encoded (deduplication is an optimization, not a
/// requirement).
pub(super) fn sorted_distinct_inlist_values(values: ArrayRef) -> Result<ArrayRef> {
    if values.len() < 2 {
        return Ok(values);
    }

    let sort_field = SortField::new(values.data_type().clone());
    if !RowConverter::supports_fields(std::slice::from_ref(&sort_field)) {
        return Ok(values);
    }

    let converter = RowConverter::new(vec![sort_field])?;
    let rows = converter.convert_columns(std::slice::from_ref(&values))?;

    // Select the first entry of each distinct value by index. The entries are
    // taken from the input instead of decoded from the rows, so that the type
    // of the input (for example a dictionary) is kept.
    let mut seen: HashSet<Row> = HashSet::with_capacity(values.len());
    let mut indices: Vec<usize> = (0..rows.num_rows())
        .filter(|&idx| seen.insert(rows.row(idx)))
        .collect();
    indices.sort_unstable_by(|&a, &b| rows.row(a).cmp(&rows.row(b)));

    let indices = UInt64Array::from_iter_values(indices.into_iter().map(|i| i as u64));
    Ok(take(values.as_ref(), &indices, None)?)
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{
        DictionaryArray, Int8Array, Int32Array, StringArray, StringDictionaryBuilder,
    };

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

        assert_eq!(result.len(), 3);
        assert_eq!(result.data_type(), dict_array.data_type());
    }

    #[test]
    fn test_sorted_distinct_inlist_values_keeps_dictionary_type() {
        let keys = Int8Array::from(vec![1i8, 0, 1, 1, 0]);
        let values = Arc::new(StringArray::from(vec!["foo", "bar"]));
        let dict_array = Arc::new(DictionaryArray::new(keys, values)) as ArrayRef;

        let result = sorted_distinct_inlist_values(Arc::clone(&dict_array)).unwrap();

        assert_eq!(result.data_type(), dict_array.data_type());
        assert_eq!(result.len(), 2);
        // "bar" sorts before "foo".
        assert_eq!(
            arrow::util::display::array_value_to_string(&result, 0).unwrap(),
            "bar"
        );
        assert_eq!(
            arrow::util::display::array_value_to_string(&result, 1).unwrap(),
            "foo"
        );
    }
}
