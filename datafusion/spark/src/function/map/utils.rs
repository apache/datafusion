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
use std::collections::HashSet;
use std::sync::Arc;

use arrow::array::{Array, ArrayRef, AsArray, BooleanBuilder, MapArray, StructArray};
use arrow::buffer::{NullBuffer, OffsetBuffer};
use arrow::compute::filter;
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
/// 2. Spark provides [spark.sql.mapKeyDedupPolicy](https://github.com/apache/spark/blob/cf3a34e19dfcf70e2d679217ff1ba21302212472/sql/catalyst/src/main/scala/org/apache/spark/sql/internal/SQLConf.scala#L4961)
///    to handle duplicate keys<br>
///    For now, configurable functions are not supported by Datafusion<br>
///    So more permissive `LAST_WIN` option is used in this implementation (instead of `EXCEPTION`)<br>
///    `EXCEPTION` behaviour can still be achieved externally in cost of performance:<br>
///    `when(array_length(array_distinct(keys)) == array_length(keys), constructed_map)`<br>
///    `.otherwise(raise_error("duplicate keys occurred during map construction"))`
pub fn map_from_keys_values_offsets_nulls(
    flat_keys: &ArrayRef,
    flat_values: &ArrayRef,
    keys_offsets: &[i32],
    values_offsets: &[i32],
    keys_nulls: Option<&NullBuffer>,
    values_nulls: Option<&NullBuffer>,
) -> Result<ArrayRef> {
    let (keys, values, offsets) = map_deduplicate_keys(
        flat_keys,
        flat_values,
        keys_offsets,
        values_offsets,
        keys_nulls,
        values_nulls,
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

fn map_deduplicate_keys(
    flat_keys: &ArrayRef,
    flat_values: &ArrayRef,
    keys_offsets: &[i32],
    values_offsets: &[i32],
    keys_nulls: Option<&NullBuffer>,
    values_nulls: Option<&NullBuffer>,
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

    let mut keys_mask_builder = BooleanBuilder::new();
    let mut values_mask_builder = BooleanBuilder::new();
    for (row_idx, (next_keys_offset, next_values_offset)) in keys_offsets
        .iter()
        .zip(values_offsets.iter())
        .skip(1)
        .enumerate()
    {
        let num_keys_entries = *next_keys_offset as usize - cur_keys_offset;
        let num_values_entries = *next_values_offset as usize - cur_values_offset;

        let mut keys_mask_one = [false].repeat(num_keys_entries);
        let mut values_mask_one = [false].repeat(num_values_entries);

        let key_is_valid = keys_nulls.is_none_or(|buf| buf.is_valid(row_idx));
        let value_is_valid = values_nulls.is_none_or(|buf| buf.is_valid(row_idx));

        if key_is_valid && value_is_valid {
            if num_keys_entries != num_values_entries {
                return exec_err!(
                    "map_deduplicate_keys: keys and values lists in the same row must have equal lengths"
                );
            } else if num_keys_entries != 0 {
                let mut seen_keys = HashSet::new();

                for cur_entry_idx in (0..num_keys_entries).rev() {
                    let key = ScalarValue::try_from_array(
                        &flat_keys,
                        cur_keys_offset + cur_entry_idx,
                    )?
                    .compacted();
                    if seen_keys.contains(&key) {
                        // TODO: implement configuration and logic for spark.sql.mapKeyDedupPolicy=EXCEPTION (this is default spark-config)
                        // exec_err!("invalid argument: duplicate keys in map")
                        // https://github.com/apache/spark/blob/cf3a34e19dfcf70e2d679217ff1ba21302212472/sql/catalyst/src/main/scala/org/apache/spark/sql/internal/SQLConf.scala#L4961
                    } else {
                        // This code implements deduplication logic for spark.sql.mapKeyDedupPolicy=LAST_WIN (this is NOT default spark-config)
                        keys_mask_one[cur_entry_idx] = true;
                        values_mask_one[cur_entry_idx] = true;
                        seen_keys.insert(key);
                        new_last_offset += 1;
                    }
                }
            }
        } else {
            // the result entry is NULL
            // both current row offsets are skipped
            // keys or values in the current row are marked false in the masks
        }
        keys_mask_builder.append_array(&keys_mask_one.into());
        values_mask_builder.append_array(&values_mask_one.into());
        new_offsets.push(new_last_offset);
        cur_keys_offset += num_keys_entries;
        cur_values_offset += num_values_entries;
    }
    let keys_mask = keys_mask_builder.finish();
    let values_mask = values_mask_builder.finish();
    let needed_keys = filter(&flat_keys, &keys_mask)?;
    let needed_values = filter(&flat_values, &values_mask)?;
    let offsets = OffsetBuffer::new(new_offsets.into());
    Ok((needed_keys, needed_values, offsets))
}
