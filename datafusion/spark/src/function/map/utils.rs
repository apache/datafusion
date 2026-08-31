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
    new_empty_array,
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
    // Round-number heuristics balance retaining space for small rows against
    // resizing after a wide row. HashMap::capacity counts insertable entries,
    // not buckets, so the ratio is an approximate retention limit.
    const MIN_RETAINED_LOOKUP_CAPACITY: usize = 16;
    const MAX_RETAINED_LOOKUP_CAPACITY_RATIO: usize = 4;

    let offsets_len = keys_offsets.len();
    let mut new_offsets = Vec::with_capacity(offsets_len);

    let mut cur_keys_offset = keys_offsets
        .first()
        .map(|offset| *offset as usize)
        .unwrap_or(0);
    let values_start_offset = values_offsets
        .first()
        .map(|offset| *offset as usize)
        .unwrap_or(0);
    let mut cur_values_offset = values_start_offset;

    let mut new_last_offset = 0;
    new_offsets.push(new_last_offset);

    // Mirror Spark's `ArrayBasedMapBuilder`: the first occurrence of a key
    // fixes its position in the output; under LAST_WIN a later duplicate
    // overwrites that slot's value. `keys_mask` selects the first-seen keys.
    // Share the value buffers with a slice only if every value is retained in
    // order. Once a value is skipped or overwritten, use `take`: a filter mask
    // would allocate for skipped spans and can materialize NULL list children.
    let mut keys_mask_builder = BooleanBuilder::new();
    let mut value_indices: Vec<i32> = Vec::new();
    // LargeList offsets can narrow to negative i32 values. Keep take's index
    // handling for those offsets instead of sign-extending them for slice.
    let mut needs_value_take = values_offsets.first().is_some_and(|offset| *offset < 0);
    let mut key_to_output_idx: HashMap<ScalarValue, usize> = HashMap::new();
    for (row_idx, (next_keys_offset, next_values_offset)) in keys_offsets
        .iter()
        .zip(values_offsets.iter())
        .skip(1)
        .enumerate()
    {
        let num_keys_entries = *next_keys_offset as usize - cur_keys_offset;
        let num_values_entries = *next_values_offset as usize - cur_values_offset;
        needs_value_take |= *next_values_offset < 0;

        let key_is_valid = keys_nulls.is_none_or(|buf| buf.is_valid(row_idx));
        let value_is_valid = values_nulls.is_none_or(|buf| buf.is_valid(row_idx));

        if key_is_valid && value_is_valid {
            if num_keys_entries != num_values_entries {
                return exec_err!(
                    "map_deduplicate_keys: keys and values lists in the same row must have equal lengths"
                );
            }
            // Reuse normal-sized tables. Only shrink when a prior row left a
            // table much larger than both the current row and a small floor.
            let target_capacity = num_keys_entries.max(MIN_RETAINED_LOOKUP_CAPACITY);
            key_to_output_idx.clear();
            if key_to_output_idx.capacity()
                > target_capacity.saturating_mul(MAX_RETAINED_LOOKUP_CAPACITY_RATIO)
            {
                key_to_output_idx.shrink_to(target_capacity);
            }
            for cur_entry_idx in 0..num_keys_entries {
                let key = ScalarValue::try_from_array(
                    &flat_keys,
                    cur_keys_offset + cur_entry_idx,
                )?
                .compacted();
                let abs_value_idx = (cur_values_offset + cur_entry_idx) as i32;

                if let Some(&output_idx) = key_to_output_idx.get(&key) {
                    if last_value_wins {
                        needs_value_take = true;
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
            // The result entry is NULL — no keys/values emitted. Keep the key
            // mask aligned, but do not allocate a mask for ignored values.
            keys_mask_builder.append_n(num_keys_entries, false);
            needs_value_take |= num_values_entries != 0;
        }
        new_offsets.push(new_last_offset);
        cur_keys_offset += num_keys_entries;
        cur_values_offset += num_values_entries;
    }
    let keys_mask = keys_mask_builder.finish();
    let needed_keys = filter(&flat_keys, &keys_mask)?;
    let needed_values = if value_indices.is_empty() {
        // An empty slice could retain unused nested child buffers.
        new_empty_array(flat_values.data_type())
    } else if needs_value_take {
        let value_indices_array = Int32Array::from(value_indices);
        take(&flat_values, &value_indices_array, None)?
    } else {
        // No values were skipped or overwritten, so the selected range is
        // contiguous. Its first offset can differ from the keys' first offset.
        flat_values.slice(values_start_offset, value_indices.len())
    };
    let offsets = OffsetBuffer::new(new_offsets.into());
    Ok((needed_keys, needed_values, offsets))
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Int32Array, LargeListArray, ListArray, NullArray, StringArray};

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
    fn distinct_keys_reuse_value_payload_buffer() {
        let (keys, values) =
            int32_utf8_inputs(vec![1, 2, 3], vec![Some("a"), Some("b"), Some("c")]);
        let offsets = [0i32, 3];
        let source_values = values.as_any().downcast_ref::<StringArray>().unwrap();

        for last_value_wins in [false, true] {
            let (_, needed_values, _) = map_deduplicate_keys(
                &keys,
                &values,
                &offsets,
                &offsets,
                None,
                None,
                last_value_wins,
            )
            .unwrap();
            let needed_values = needed_values
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap();

            assert_eq!(
                source_values.value_data().as_ptr(),
                needed_values.value_data().as_ptr(),
                "distinct keys should not copy values under last_value_wins={last_value_wins}"
            );
        }
    }

    #[test]
    fn wide_row_followed_by_small_rows() {
        // The wide row grows the lookup table beyond the retention threshold
        // for the following small and empty rows.
        let wide_len = 512;
        let keys = (0..wide_len).chain([0, 1, 0]).collect();
        let values = std::iter::repeat_n(Some("wide"), wide_len as usize)
            .chain([Some("small"), None, Some("last")])
            .collect();
        let (keys, values) = int32_utf8_inputs(keys, values);
        let offsets = [0, wide_len, wide_len + 2, wide_len + 2, wide_len + 3];

        for last_value_wins in [false, true] {
            let result = map_from_keys_values_offsets_nulls(
                &keys,
                &values,
                &offsets,
                &offsets,
                None,
                None,
                last_value_wins,
            )
            .unwrap();

            let map = result.as_map();
            assert_eq!(map.value_offsets(), &offsets);
            assert_eq!(map.keys().to_data(), keys.to_data());
            assert_eq!(map.values().to_data(), values.to_data());
        }
    }

    #[test]
    fn last_win_after_wide_row_preserves_key_order_and_values() {
        let wide_len = 512;
        let keys = (0..wide_len).chain([1, 0, 1, 2, 0, 1]).collect();
        let values = std::iter::repeat_n(Some("wide"), wide_len as usize)
            .chain([
                Some("old-a"),
                Some("old-b"),
                Some("new-a"),
                Some("c"),
                None,
                Some("next-row"),
            ])
            .collect();
        let (keys, values) = int32_utf8_inputs(keys, values);
        let offsets = [0, wide_len, wide_len + 5, wide_len + 6];

        let result = map_from_keys_values_offsets_nulls(
            &keys, &values, &offsets, &offsets, None, None, true,
        )
        .unwrap();

        let expected_keys = (0..wide_len).chain([1, 0, 2, 1]).collect();
        let expected_values = std::iter::repeat_n(Some("wide"), wide_len as usize)
            .chain([Some("new-a"), None, Some("c"), Some("next-row")])
            .collect();
        let (expected_keys, expected_values) =
            int32_utf8_inputs(expected_keys, expected_values);
        let map = result.as_map();
        assert_eq!(
            map.value_offsets(),
            &[0, wide_len, wide_len + 3, wide_len + 4]
        );
        assert_eq!(map.keys().to_data(), expected_keys.to_data());
        assert_eq!(map.values().to_data(), expected_values.to_data());
    }

    #[test]
    fn distinct_keys_with_nonzero_values_offset_reuse_payload_buffer() {
        let (keys, values) = int32_utf8_inputs(
            vec![1, 2, 3],
            vec![Some("prefix"), Some("a"), None, Some("c"), Some("suffix")],
        );
        let keys_offsets = [0, 2, 3];
        let values_offsets = [1, 3, 4];
        let expected_values = StringArray::from(vec![Some("a"), None, Some("c")]);
        let source_values = values.as_any().downcast_ref::<StringArray>().unwrap();

        for last_value_wins in [false, true] {
            let result = map_from_keys_values_offsets_nulls(
                &keys,
                &values,
                &keys_offsets,
                &values_offsets,
                None,
                None,
                last_value_wins,
            )
            .unwrap();

            let map = result.as_map();
            assert_eq!(map.value_offsets(), &keys_offsets);
            assert_eq!(map.keys().to_data(), keys.to_data());
            let needed_values =
                map.values().as_any().downcast_ref::<StringArray>().unwrap();
            assert_eq!(needed_values, &expected_values);
            assert_eq!(
                needed_values.value_data().as_ptr(),
                source_values.value_data().as_ptr(),
            );
        }
    }

    #[test]
    fn large_list_values_near_i32_offset_limit() {
        let keys: ArrayRef = Arc::new(Int32Array::from(vec![7]));
        keys.to_data().validate_full().unwrap();

        for start in [(1_i64 << 31) - 2, 1_i64 << 31] {
            for row_valid in [true, false] {
                // NullArray represents the large child without a large allocation.
                let values: ArrayRef = Arc::new(LargeListArray::new(
                    Arc::new(Field::new("item", DataType::Null, true)),
                    OffsetBuffer::new(vec![start, start + 1].into()),
                    Arc::new(NullArray::new(start as usize + 1)),
                    Some(NullBuffer::from(vec![row_valid])),
                ));
                values.to_data().validate_full().unwrap();
                let values_offsets = get_list_offsets(&values).unwrap();
                let expected_keys = if row_valid { vec![7] } else { vec![] };

                for last_value_wins in [false, true] {
                    let result = map_from_keys_values_offsets_nulls(
                        &keys,
                        get_list_values(&values).unwrap(),
                        &[0, 1],
                        &values_offsets,
                        None,
                        values.nulls(),
                        last_value_wins,
                    )
                    .unwrap();

                    result.to_data().validate_full().unwrap();
                    let map = result.as_map();
                    assert_eq!(map.len(), 1);
                    assert_eq!(map.is_null(0), !row_valid);
                    assert_eq!(map.value_offsets(), &[0, expected_keys.len() as i32]);
                    assert_eq!(
                        map.keys().to_data(),
                        Int32Array::from(expected_keys.clone()).to_data(),
                    );
                    assert_eq!(
                        map.values().to_data(),
                        NullArray::new(expected_keys.len()).to_data(),
                    );
                }
            }
        }
    }

    #[test]
    fn null_large_list_values_cross_i32_offset_limit() {
        let start = (1_i64 << 31) - 1;
        let keys: ArrayRef = Arc::new(Int32Array::from(vec![7, 8]));
        // The first values row is NULL and crosses the narrowing boundary.
        // The second row must still produce its map entry at the large offset.
        let values: ArrayRef = Arc::new(LargeListArray::new(
            Arc::new(Field::new("item", DataType::Null, true)),
            OffsetBuffer::new(vec![start, start + 1, start + 2].into()),
            Arc::new(NullArray::new(start as usize + 2)),
            Some(NullBuffer::from(vec![false, true])),
        ));
        keys.to_data().validate_full().unwrap();
        values.to_data().validate_full().unwrap();
        let values_offsets = get_list_offsets(&values).unwrap();

        for last_value_wins in [false, true] {
            let result = map_from_keys_values_offsets_nulls(
                &keys,
                get_list_values(&values).unwrap(),
                &[0, 1, 2],
                &values_offsets,
                None,
                values.nulls(),
                last_value_wins,
            )
            .unwrap();

            result.to_data().validate_full().unwrap();
            let map = result.as_map();
            assert_eq!(map.len(), 2);
            assert!(map.is_null(0));
            assert!(map.is_valid(1));
            assert_eq!(map.value_offsets(), &[0, 0, 1]);
            assert_eq!(map.keys().to_data(), Int32Array::from(vec![8]).to_data());
            assert_eq!(map.values().to_data(), NullArray::new(1).to_data());
        }
    }

    #[test]
    fn last_win_with_nonzero_values_offset() {
        let (keys, values) = int32_utf8_inputs(
            vec![1, 2, 1],
            vec![Some("prefix"), Some("a"), Some("b"), None, Some("suffix")],
        );
        let result = map_from_keys_values_offsets_nulls(
            &keys,
            &values,
            &[0, 3],
            &[1, 4],
            None,
            None,
            true,
        )
        .unwrap();

        let map = result.as_map();
        assert_eq!(map.value_offsets(), &[0, 2]);
        assert_eq!(map.keys().to_data(), Int32Array::from(vec![1, 2]).to_data());
        assert_eq!(
            map.values().to_data(),
            StringArray::from(vec![None, Some("b")]).to_data(),
        );
    }

    #[test]
    fn null_row_with_unequal_child_lengths_keeps_values_aligned() {
        let (keys, values) = int32_utf8_inputs(
            vec![1, 9, 9, 3, 4],
            vec![Some("first"), Some("ignored"), Some("last"), None],
        );
        let keys_offsets = [0, 1, 3, 5];
        let values_offsets = [0, 1, 2, 4];
        let nulls = NullBuffer::from(vec![true, false, true]);

        for last_value_wins in [false, true] {
            for (keys_nulls, values_nulls) in [(Some(&nulls), None), (None, Some(&nulls))]
            {
                let result = map_from_keys_values_offsets_nulls(
                    &keys,
                    &values,
                    &keys_offsets,
                    &values_offsets,
                    keys_nulls,
                    values_nulls,
                    last_value_wins,
                )
                .unwrap();

                let map = result.as_map();
                assert_eq!(map.value_offsets(), &[0, 1, 1, 3]);
                assert_eq!(map.nulls(), Some(&nulls));
                assert_eq!(
                    map.keys().to_data(),
                    Int32Array::from(vec![1, 3, 4]).to_data(),
                );
                assert_eq!(
                    map.values().to_data(),
                    StringArray::from(vec![Some("first"), Some("last"), None]).to_data(),
                );
            }
        }
    }

    #[test]
    fn null_rows_with_large_values_spans_are_skipped() {
        // A NullArray has no payload buffer, even when a skipped row spans
        // many values. Selection should only allocate for retained entries.
        let skipped_len = 1 << 20;
        for null_row in 0..3 {
            let nulls =
                NullBuffer::from((0..3).map(|row| row != null_row).collect::<Vec<_>>());
            let mut keys_offsets = vec![0];
            let mut values_offsets = vec![0];
            for row in 0..3 {
                keys_offsets.push(keys_offsets[row] + i32::from(row != null_row));
                values_offsets.push(
                    values_offsets[row] + if row == null_row { skipped_len } else { 1 },
                );
            }

            for null_keys in [false, true] {
                let keys: ArrayRef = Arc::new(ListArray::new(
                    Arc::new(Field::new("item", DataType::Int32, false)),
                    OffsetBuffer::new(keys_offsets.clone().into()),
                    Arc::new(Int32Array::from(vec![7, 8])),
                    null_keys.then(|| nulls.clone()),
                ));
                let values: ArrayRef = Arc::new(ListArray::new(
                    Arc::new(Field::new("item", DataType::Null, true)),
                    OffsetBuffer::new(values_offsets.clone().into()),
                    Arc::new(NullArray::new(skipped_len as usize + 2)),
                    (!null_keys).then(|| nulls.clone()),
                ));
                keys.to_data().validate_full().unwrap();
                values.to_data().validate_full().unwrap();

                for last_value_wins in [false, true] {
                    let result = map_from_keys_values_offsets_nulls(
                        get_list_values(&keys).unwrap(),
                        get_list_values(&values).unwrap(),
                        &get_list_offsets(&keys).unwrap(),
                        &get_list_offsets(&values).unwrap(),
                        keys.nulls(),
                        values.nulls(),
                        last_value_wins,
                    )
                    .unwrap();

                    result.to_data().validate_full().unwrap();
                    let map = result.as_map();
                    assert_eq!(map.value_offsets(), keys_offsets.as_slice());
                    assert_eq!(map.nulls(), Some(&nulls));
                    assert_eq!(
                        map.keys().to_data(),
                        Int32Array::from(vec![7, 8]).to_data(),
                    );
                    assert_eq!(map.values().to_data(), NullArray::new(2).to_data());
                }
            }
        }
    }

    #[test]
    fn partial_selection_does_not_materialize_null_list_children() {
        let child_len = 1 << 16;
        let list_field = Arc::new(Field::new("item", DataType::Null, true));
        let child_values: ArrayRef = Arc::new(NullArray::new(child_len + 1));
        let inner_nulls = NullBuffer::from(vec![false, true]);
        let inner_lists: Vec<ArrayRef> = vec![
            Arc::new(ListArray::new(
                Arc::clone(&list_field),
                OffsetBuffer::new(vec![0, child_len as i32, child_len as i32 + 1].into()),
                Arc::clone(&child_values),
                Some(inner_nulls.clone()),
            )),
            Arc::new(LargeListArray::new(
                list_field,
                OffsetBuffer::new(vec![0, child_len as i64, child_len as i64 + 1].into()),
                Arc::clone(&child_values),
                Some(inner_nulls),
            )),
        ];
        let keys: ArrayRef = Arc::new(Int32Array::from(vec![7, 8]));
        let offsets = [0, 1, 2];
        let outer_nulls = NullBuffer::from(vec![true, false]);

        for values in inner_lists {
            values.to_data().validate_full().unwrap();
            for last_value_wins in [false, true] {
                for (keys_nulls, values_nulls) in
                    [(Some(&outer_nulls), None), (None, Some(&outer_nulls))]
                {
                    let result = map_from_keys_values_offsets_nulls(
                        &keys,
                        &values,
                        &offsets,
                        &offsets,
                        keys_nulls,
                        values_nulls,
                        last_value_wins,
                    )
                    .unwrap();

                    result.to_data().validate_full().unwrap();
                    let map = result.as_map();
                    assert_eq!(map.value_offsets(), &[0, 1, 1]);
                    assert_eq!(map.nulls(), Some(&outer_nulls));
                    assert_eq!(map.keys().to_data(), Int32Array::from(vec![7]).to_data());
                    assert!(map.values().is_null(0));
                    assert_eq!(get_list_values(map.values()).unwrap().len(), 0);
                }

                // Retaining every value should still share the nested child,
                // even when a NULL list has a nonempty child span.
                let result = map_from_keys_values_offsets_nulls(
                    &keys,
                    &values,
                    &offsets,
                    &offsets,
                    None,
                    None,
                    last_value_wins,
                )
                .unwrap();
                result.to_data().validate_full().unwrap();
                assert_eq!(result.as_map().values().to_data(), values.to_data());
                assert!(Arc::ptr_eq(
                    get_list_values(result.as_map().values()).unwrap(),
                    &child_values,
                ));
            }
        }
    }

    #[test]
    fn empty_null_rows_preserve_value_buffer_sharing() {
        let (keys, values) = int32_utf8_inputs(
            vec![1, 2],
            vec![Some("prefix"), Some("a"), Some("b"), Some("suffix")],
        );
        let source_values = values.as_any().downcast_ref::<StringArray>().unwrap();
        let nulls = NullBuffer::from(vec![true, false, true]);
        for last_value_wins in [false, true] {
            for (keys_nulls, values_nulls) in [(Some(&nulls), None), (None, Some(&nulls))]
            {
                let result = map_from_keys_values_offsets_nulls(
                    &keys,
                    &values,
                    &[0, 1, 1, 2],
                    &[1, 2, 2, 3],
                    keys_nulls,
                    values_nulls,
                    last_value_wins,
                )
                .unwrap();
                result.to_data().validate_full().unwrap();
                let map = result.as_map();
                let needed_values =
                    map.values().as_any().downcast_ref::<StringArray>().unwrap();
                assert_eq!(map.value_offsets(), &[0, 1, 1, 2]);
                assert_eq!(map.nulls(), Some(&nulls));
                assert_eq!(needed_values, &StringArray::from(vec!["a", "b"]));
                assert_eq!(
                    needed_values.value_data().as_ptr(),
                    source_values.value_data().as_ptr(),
                );
            }
        }
    }

    #[test]
    fn empty_maps_do_not_retain_nested_values() {
        let keys: ArrayRef = Arc::new(Int32Array::from(Vec::<i32>::new()));
        let values: ArrayRef = Arc::new(ListArray::new(
            Arc::new(Field::new("item", DataType::Null, true)),
            OffsetBuffer::new(vec![0, 1 << 16].into()),
            Arc::new(NullArray::new(1 << 16)),
            None,
        ));
        values.to_data().validate_full().unwrap();
        let nulls = NullBuffer::from(vec![false]);

        for last_value_wins in [false, true] {
            // Cover an empty batch, an empty map, and a NULL map that skips
            // either an empty or a nonempty values row.
            for (keys_offsets, values_offsets, keys_nulls) in [
                (&[0][..], &[0][..], None),
                (&[0, 0][..], &[0, 0][..], None),
                (&[0, 0][..], &[1, 1][..], Some(&nulls)),
                (&[0, 0][..], &[0, 1][..], Some(&nulls)),
            ] {
                let result = map_from_keys_values_offsets_nulls(
                    &keys,
                    &values,
                    keys_offsets,
                    values_offsets,
                    keys_nulls,
                    None,
                    last_value_wins,
                )
                .unwrap();

                result.to_data().validate_full().unwrap();
                let map = result.as_map();
                assert_eq!(map.value_offsets(), keys_offsets);
                assert_eq!(map.nulls(), keys_nulls);
                assert_eq!(map.values().len(), 0);
                assert_eq!(get_list_values(map.values()).unwrap().len(), 0);
            }
        }
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
