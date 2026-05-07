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

use std::borrow::Cow;
use std::collections::HashMap;
use std::sync::Arc;

use arrow::array::{
    Array, ArrayRef, AsArray, BooleanBuilder, Int32Array, MapArray, StructArray,
};
use arrow::buffer::{NullBuffer, OffsetBuffer};
use arrow::compute::{filter, take};
use arrow::datatypes::{DataType, Field, Fields};
use datafusion_common::{Result, ScalarValue, exec_err};

/// Helper function to get element [`DataType`]
/// from [`List`](DataType::List)/[`LargeList`](DataType::LargeList)/[`FixedSizeList`](DataType::FixedSizeList)<br>
/// [`Null`](DataType::Null) can be coerced to `ListType`([`Null`](DataType::Null)), so [`Null`](DataType::Null) is returned<br>
/// For all other types [`exec_err`] is raised
pub fn get_element_type(data_type: &DataType) -> Result<&DataType> {
    match data_type {
        DataType::Null => Ok(data_type),
        DataType::List(element)
        | DataType::LargeList(element)
        | DataType::FixedSizeList(element, _) => Ok(element.data_type()),
        _ => exec_err!(
            "get_element_type expects List/LargeList/FixedSizeList/Null as argument, got {data_type:?}"
        ),
    }
}

/// Helper function to get [`values`](arrow::array::ListArray::values)
/// from [`ListArray`](arrow::array::ListArray)/[`LargeListArray`](arrow::array::LargeListArray)/[`FixedSizeListArray`](arrow::array::FixedSizeListArray)<br>
/// [`NullArray`](arrow::array::NullArray) can be coerced to `ListType`([`Null`](DataType::Null)), so [`NullArray`](arrow::array::NullArray) is returned<br>
/// For all other types [`exec_err`] is raised
pub fn get_list_values(array: &ArrayRef) -> Result<&ArrayRef> {
    match array.data_type() {
        DataType::Null => Ok(array),
        DataType::List(_) => Ok(array.as_list::<i32>().values()),
        DataType::LargeList(_) => Ok(array.as_list::<i64>().values()),
        DataType::FixedSizeList(..) => Ok(array.as_fixed_size_list().values()),
        wrong_type => exec_err!(
            "get_list_values expects List/LargeList/FixedSizeList/Null as argument, got {wrong_type:?}"
        ),
    }
}

/// Helper function to get [`offsets`](arrow::array::ListArray::offsets)
/// from [`ListArray`](arrow::array::ListArray)/[`LargeListArray`](arrow::array::LargeListArray)/[`FixedSizeListArray`](arrow::array::FixedSizeListArray)<br>
/// For all other types [`exec_err`] is raised
pub fn get_list_offsets(array: &ArrayRef) -> Result<Cow<'_, [i32]>> {
    match array.data_type() {
        DataType::List(_) => Ok(Cow::Borrowed(array.as_list::<i32>().offsets().as_ref())),
        DataType::LargeList(_) => Ok(Cow::Owned(
            array
                .as_list::<i64>()
                .offsets()
                .iter()
                .map(|i| *i as i32)
                .collect::<Vec<_>>(),
        )),
        DataType::FixedSizeList(_, size) => Ok(Cow::Owned(
            (0..=array.len() as i32).map(|i| size * i).collect(),
        )),
        wrong_type => exec_err!(
            "get_list_offsets expects List/LargeList/FixedSizeList as argument, got {wrong_type:?}"
        ),
    }
}

/// Helper function to construct [`MapType<K, V>`](DataType::Map) given K and V DataTypes for keys and values
/// - Map keys are unsorted
/// - Map keys are non-nullable
/// - Map entries are non-nullable
/// - Map values can be null
pub fn map_type_from_key_value_types(
    key_type: &DataType,
    value_type: &DataType,
) -> DataType {
    DataType::Map(
        Arc::new(Field::new(
            "entries",
            DataType::Struct(Fields::from(vec![
                // the key must not be nullable
                Field::new("key", key_type.clone(), false),
                Field::new("value", value_type.clone(), true),
            ])),
            false, // the entry is not nullable
        )),
        false, // the keys are not sorted
    )
}

/// Helper function to construct MapArray from flattened ListArrays and OffsetBuffer
///
/// Logic is close to `datafusion_functions_nested::map::make_map_array_internal`<br>
/// But there are some core differences:
/// 1. Input arrays are not [`ListArrays`](arrow::array::ListArray) itself, but their flattened [`values`](arrow::array::ListArray::values)<br>
///    So the inputs can be [`ListArray`](`arrow::array::ListArray`)/[`LargeListArray`](`arrow::array::LargeListArray`)/[`FixedSizeListArray`](`arrow::array::FixedSizeListArray`)<br>
///    To preserve the row info, [`offsets`](arrow::array::ListArray::offsets) and [`nulls`](arrow::array::ListArray::nulls) for both keys and values need to be provided<br>
///    [`FixedSizeListArray`](`arrow::array::FixedSizeListArray`) has no `offsets`, so they can be generated as a cumulative sum of it's `Size`
/// 2. Duplicate-key handling mirrors Spark's
///    [spark.sql.mapKeyDedupPolicy](https://github.com/apache/spark/blob/cf3a34e19dfcf70e2d679217ff1ba21302212472/sql/catalyst/src/main/scala/org/apache/spark/sql/internal/SQLConf.scala#L4961)
///    and is driven by `last_value_wins`:
///    - `false` (Spark's default `EXCEPTION`): raise `[DUPLICATED_MAP_KEY]` on any duplicate.
///    - `true` (`LAST_WIN`): keep the last occurrence of each duplicate key.
///
///    Callers wire this from `datafusion.spark.map_key_dedup_policy`.
pub fn map_from_keys_values_offsets_nulls(
    flat_keys: &ArrayRef,
    flat_values: &ArrayRef,
    keys_offsets: &[i32],
    values_offsets: &[i32],
    keys_nulls: Option<&NullBuffer>,
    values_nulls: Option<&NullBuffer>,
    last_value_wins: bool,
) -> Result<ArrayRef> {
    let (keys, values, offsets) = map_deduplicate_keys(
        flat_keys,
        flat_values,
        keys_offsets,
        values_offsets,
        keys_nulls,
        values_nulls,
        last_value_wins,
    )?;
    let nulls = NullBuffer::union(keys_nulls, values_nulls);

    let fields = Fields::from(vec![
        Field::new("key", flat_keys.data_type().clone(), false),
        Field::new("value", flat_values.data_type().clone(), true),
    ]);
    let entries = StructArray::try_new(fields.clone(), vec![keys, values], None)?;
    let field = Arc::new(Field::new("entries", DataType::Struct(fields), false));
    Ok(Arc::new(MapArray::try_new(
        field, offsets, entries, nulls, false,
    )?))
}

#[allow(clippy::allow_attributes, clippy::mutable_key_type)] // ScalarValue has interior mutability but is intentionally used as hash key
fn map_deduplicate_keys(
    flat_keys: &ArrayRef,
    flat_values: &ArrayRef,
    keys_offsets: &[i32],
    values_offsets: &[i32],
    keys_nulls: Option<&NullBuffer>,
    values_nulls: Option<&NullBuffer>,
    last_value_wins: bool,
) -> Result<(ArrayRef, ArrayRef, OffsetBuffer<i32>)> {
    let offsets_len = keys_offsets.len();
    let mut new_offsets = Vec::with_capacity(offsets_len);

    let mut cur_keys_offset = keys_offsets
        .first()
        .map(|offset| *offset as usize)
        .unwrap_or(0);
    let mut cur_values_offset = values_offsets
        .first()
        .map(|offset| *offset as usize)
        .unwrap_or(0);

    let mut new_last_offset = 0;
    new_offsets.push(new_last_offset);

    // Mirror Spark's `ArrayBasedMapBuilder`: the first occurrence of a key
    // fixes its position in the output; under LAST_WIN a later duplicate
    // overwrites that slot's value. `keys_mask` selects the first-seen keys,
    // `value_indices` records the source index in `flat_values` to materialize
    // for each output slot (updated in place on overwrite).
    let mut keys_mask_builder = BooleanBuilder::new();
    let mut value_indices: Vec<i32> = Vec::new();
    let mut key_to_output_idx: HashMap<ScalarValue, usize> = HashMap::new();
    for (row_idx, (next_keys_offset, next_values_offset)) in keys_offsets
        .iter()
        .zip(values_offsets.iter())
        .skip(1)
        .enumerate()
    {
        let num_keys_entries = *next_keys_offset as usize - cur_keys_offset;
        let num_values_entries = *next_values_offset as usize - cur_values_offset;

        let key_is_valid = keys_nulls.is_none_or(|buf| buf.is_valid(row_idx));
        let value_is_valid = values_nulls.is_none_or(|buf| buf.is_valid(row_idx));

        if key_is_valid && value_is_valid {
            if num_keys_entries != num_values_entries {
                return exec_err!(
                    "map_deduplicate_keys: keys and values lists in the same row must have equal lengths"
                );
            }
            key_to_output_idx.clear();
            for cur_entry_idx in 0..num_keys_entries {
                let key = ScalarValue::try_from_array(
                    &flat_keys,
                    cur_keys_offset + cur_entry_idx,
                )?
                .compacted();
                let abs_value_idx = (cur_values_offset + cur_entry_idx) as i32;

                if let Some(&output_idx) = key_to_output_idx.get(&key) {
                    if last_value_wins {
                        value_indices[output_idx] = abs_value_idx;
                        keys_mask_builder.append_value(false);
                        continue;
                    }
                    return exec_err!(
                        "[DUPLICATED_MAP_KEY] Duplicate map key {key} was found, \
                         please check the input data. To allow duplicate keys with \
                         last-value-wins semantics, set \
                         `datafusion.spark.map_key_dedup_policy` to `LAST_WIN`."
                    );
                }
                keys_mask_builder.append_value(true);
                key_to_output_idx.insert(key, value_indices.len());
                value_indices.push(abs_value_idx);
                new_last_offset += 1;
            }
        } else {
            // The result entry is NULL — no keys/values emitted. Still pad the
            // mask so it stays aligned with `flat_keys`.
            keys_mask_builder.append_n(num_keys_entries, false);
        }
        new_offsets.push(new_last_offset);
        cur_keys_offset += num_keys_entries;
        cur_values_offset += num_values_entries;
    }
    let keys_mask = keys_mask_builder.finish();
    let needed_keys = filter(&flat_keys, &keys_mask)?;
    let value_indices_array = Int32Array::from(value_indices);
    let needed_values = take(&flat_values, &value_indices_array, None)?;
    let offsets = OffsetBuffer::new(new_offsets.into());
    Ok((needed_keys, needed_values, offsets))
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Int32Array, StringArray};

    fn int32_utf8_inputs(
        keys: Vec<i32>,
        values: Vec<Option<&str>>,
    ) -> (ArrayRef, ArrayRef) {
        let keys: ArrayRef = Arc::new(Int32Array::from(keys));
        let values: ArrayRef = Arc::new(StringArray::from(values));
        (keys, values)
    }

    #[test]
    fn happy_path_two_rows_no_duplicates() {
        let (keys, values) =
            int32_utf8_inputs(vec![1, 2, 3], vec![Some("a"), Some("b"), Some("c")]);
        let offsets = [0i32, 2, 3];

        let result = map_from_keys_values_offsets_nulls(
            &keys, &values, &offsets, &offsets, None, None, false,
        )
        .unwrap();

        let map = result.as_map();
        assert_eq!(map.len(), 2);
        assert_eq!(map.value_offsets(), &[0, 2, 3]);
    }

    #[test]
    fn single_row_duplicate_errors_under_exception() {
        let (keys, values) =
            int32_utf8_inputs(vec![1, 2, 1], vec![Some("a"), Some("b"), Some("c")]);
        let offsets = [0i32, 3];

        let err = map_from_keys_values_offsets_nulls(
            &keys, &values, &offsets, &offsets, None, None, false,
        )
        .unwrap_err()
        .to_string();

        assert!(err.contains("[DUPLICATED_MAP_KEY]"), "{err}");
        assert!(err.contains("map_key_dedup_policy"), "{err}");
    }

    #[test]
    fn last_win_keeps_final_occurrence() {
        let (keys, values) = int32_utf8_inputs(
            vec![1, 2, 1, 3, 2],
            vec![Some("a"), Some("b"), Some("c"), Some("d"), Some("e")],
        );
        let offsets = [0i32, 5];

        let result = map_from_keys_values_offsets_nulls(
            &keys, &values, &offsets, &offsets, None, None, true,
        )
        .unwrap();

        let map = result.as_map();
        assert_eq!(map.len(), 1);
        // 5 entries in, 3 unique keys -> offsets [0, 3]
        assert_eq!(map.value_offsets(), &[0, 3]);
    }

    #[test]
    fn duplicate_in_later_row_still_errors() {
        let (keys, values) = int32_utf8_inputs(
            vec![1, 2, 1, 1],
            vec![Some("a"), Some("b"), Some("x"), Some("y")],
        );
        let offsets = [0i32, 2, 4];

        let err = map_from_keys_values_offsets_nulls(
            &keys, &values, &offsets, &offsets, None, None, false,
        )
        .unwrap_err()
        .to_string();

        assert!(err.contains("[DUPLICATED_MAP_KEY]"), "{err}");
    }

    #[test]
    fn empty_row_does_not_trigger_dedup() {
        let (keys, values) = int32_utf8_inputs(vec![], vec![]);
        let offsets = [0i32, 0];

        let result = map_from_keys_values_offsets_nulls(
            &keys, &values, &offsets, &offsets, None, None, false,
        )
        .unwrap();

        let map = result.as_map();
        assert_eq!(map.len(), 1);
        assert_eq!(map.value_offsets(), &[0, 0]);
    }

    #[test]
    fn null_row_is_skipped_and_not_checked() {
        // Row 0 is NULL (keys null). Its duplicate keys should be ignored;
        // row 1 is a clean row.
        let (keys, values) = int32_utf8_inputs(
            vec![1, 1, 2, 3],
            vec![Some("dup-a"), Some("dup-b"), Some("x"), Some("y")],
        );
        let offsets = [0i32, 2, 4];
        let keys_nulls = NullBuffer::from(vec![false, true]);

        let result = map_from_keys_values_offsets_nulls(
            &keys,
            &values,
            &offsets,
            &offsets,
            Some(&keys_nulls),
            None,
            false,
        )
        .unwrap();

        let map = result.as_map();
        assert_eq!(map.len(), 2);
        // First row is NULL (no entries emitted), second row keeps both entries.
        assert_eq!(map.value_offsets(), &[0, 0, 2]);
        assert!(map.is_null(0));
        assert!(!map.is_null(1));
    }
}
