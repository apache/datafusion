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

use crate::aggregates::group_values::GroupValues;
use crate::hash_utils::RandomState;
use arrow::array::{
    Array, ArrayRef, DictionaryArray, LargeStringArray, LargeStringBuilder, ListArray,
    ListBuilder, PrimitiveArray, PrimitiveBuilder, StringArray, StringBuilder,
    StringViewArray, StringViewBuilder, UInt64Array,
};
use arrow::datatypes::{ArrowDictionaryKeyType, ArrowNativeType, DataType};
use datafusion_common::DataFusionError::{Internal, NotImplemented};
use datafusion_common::Result;
use datafusion_common::hash_utils::create_hashes;
use datafusion_expr::EmitTo;
use std::borrow::Cow;
use std::collections::HashMap;
use std::marker::PhantomData;
use std::sync::Arc;
macro_rules! decode_list {
    ($raw:expr, $builder_type:ty) => {{
        let mut builder = ListBuilder::new(<$builder_type>::new());
        for raw_bytes in $raw {
            match raw_bytes {
                None => builder.append_null(),
                Some(raw_vector) => {
                    let mut offset = 0;
                    while offset < raw_vector.len() {
                        let len = i64::from_ne_bytes(
                            raw_vector[offset..offset + 8]
                                .try_into()
                                .expect("slice of length 8"),
                        );
                        offset += 8;
                        if len == -1 {
                            builder.values().append_null();
                        } else {
                            let s = std::str::from_utf8(
                                &raw_vector[offset..offset + len as usize],
                            )
                            .map_err(|e| {
                                Internal(format!("Invalid utf8 in list element: {e}"))
                            })?;
                            builder.values().append_value(s);
                            offset += len as usize;
                        }
                    }
                    builder.append(true);
                }
            }
        }
        Ok(Arc::new(builder.finish()) as ArrayRef)
    }};
}
macro_rules! decode_scalar_string {
    ($raw:expr, $builder_type:ty) => {{
        let mut builder = <$builder_type>::new();
        for raw_bytes in $raw {
            match raw_bytes {
                Some(raw_vector) => {
                    let s = std::str::from_utf8(raw_vector).map_err(|e| {
                        Internal(format!("Invalid utf8 in GroupValuesDictionary: {e}"))
                    })?;
                    builder.append_value(s);
                }
                None => builder.append_null(),
            }
        }
        Ok(Arc::new(builder.finish()) as ArrayRef)
    }};
}
type GroupEntry = (usize, Option<Vec<u8>>);
pub struct GroupValuesDictionary<K: ArrowDictionaryKeyType + Send> {
    // stores the order new unique elements are seen for self.emit()
    seen_elements: Vec<Option<Vec<u8>>>,
    value_dt: DataType,
    _phantom: PhantomData<K>,
    // keeps track of which values weve already seen. stored as -> <unique_value_hash:(initial_group_id, raw_bytes)>
    unique_dict_value_mapping: HashMap<u64, Vec<GroupEntry>>,

    random_state: RandomState,

    // cache the group id for nulls since they all map to the same group
    null_group_id: Option<usize>,
    intern_called: bool,
}

impl<K: ArrowDictionaryKeyType + Send> GroupValuesDictionary<K> {
    pub fn new(data_type: &DataType) -> Self {
        Self {
            seen_elements: Vec::new(),
            unique_dict_value_mapping: HashMap::new(),
            value_dt: data_type.clone(),
            _phantom: PhantomData,
            random_state: RandomState::with_seed(0),
            null_group_id: None,
            intern_called: false,
        }
    }
    fn compute_value_hashes(&mut self, values: &ArrayRef) -> Result<Vec<u64>> {
        let mut hashes = vec![0u64; values.len()];
        create_hashes([Arc::clone(values)], &self.random_state, &mut hashes)?;
        Ok(hashes)
    }

    fn get_raw_bytes(values: &ArrayRef, index: usize) -> Cow<'_, [u8]> {
        match values.data_type() {
            DataType::Utf8 => Cow::Borrowed(
                values
                    .as_any()
                    .downcast_ref::<StringArray>()
                    .expect("Expected StringArray")
                    .value(index)
                    .as_bytes(),
            ),
            DataType::LargeUtf8 => Cow::Borrowed(
                values
                    .as_any()
                    .downcast_ref::<LargeStringArray>()
                    .expect("Expected LargeStringArray")
                    .value(index)
                    .as_bytes(),
            ),
            DataType::Utf8View => Cow::Borrowed(
                values
                    .as_any()
                    .downcast_ref::<StringViewArray>()
                    .expect("Expected StringViewArray")
                    .value(index)
                    .as_bytes(),
            ),
            DataType::List(_) => {
                let list_array = values
                    .as_any()
                    .downcast_ref::<ListArray>()
                    .expect("Expected ListArray");

                debug_assert!(!list_array.is_null(index));

                let start = list_array.value_offsets()[index] as usize;
                let end = list_array.value_offsets()[index + 1] as usize;
                let child = list_array.values();

                let mut bytes = Vec::new();
                for i in start..end {
                    if child.is_null(i) {
                        // acts as a marker for transform_into_array to write a null
                        bytes.extend_from_slice(&(-1i64).to_ne_bytes());
                    } else {
                        let raw = Self::get_raw_bytes(child, i);
                        bytes.extend_from_slice(&(raw.len() as i64).to_ne_bytes());
                        bytes.extend_from_slice(&raw);
                    }
                }
                Cow::Owned(bytes)
            }
            other => unimplemented!("get_raw_bytes not implemented for {other:?}"),
        }
    }

    #[inline]
    fn get_null_group_id(&mut self) -> usize {
        if let Some(group_id) = self.null_group_id {
            group_id
        } else {
            let new_group_id = self.seen_elements.len();
            self.seen_elements.push(None);
            self.unique_dict_value_mapping
                .insert((usize::MAX - 1) as u64, vec![(new_group_id, None)]);
            self.null_group_id = Some(new_group_id);
            new_group_id
        }
    }
    fn transform_into_array(&self, raw: &[Option<Vec<u8>>]) -> Result<ArrayRef> {
        match &self.value_dt {
            DataType::Utf8 => decode_scalar_string!(raw, StringBuilder),
            DataType::LargeUtf8 => decode_scalar_string!(raw, LargeStringBuilder),
            DataType::Utf8View => decode_scalar_string!(raw, StringViewBuilder),
            DataType::List(field) => match field.data_type() {
                DataType::Utf8 => decode_list!(raw, StringBuilder),
                DataType::LargeUtf8 => decode_list!(raw, LargeStringBuilder),
                DataType::Utf8View => decode_list!(raw, StringViewBuilder),
                other => Err(NotImplemented(format!(
                    "transform_into_array not implemented for List<{other:?}>"
                ))),
            },
            other => Err(NotImplemented(format!(
                "transform_into_array not implemented for {other:?}"
            ))),
        }
    }
    fn normalize_dict_array(
        values: &ArrayRef,
        key_array: &PrimitiveArray<K>,
    ) -> (ArrayRef, Vec<Option<usize>>) {
        // maps old value index -> new canonical index
        let mut old_to_new: Vec<Option<usize>> = vec![None; values.len()];
        let mut canonical_indices: Vec<usize> = Vec::new();

        for (i, slot) in old_to_new.iter_mut().enumerate() {
            if values.is_null(i) {
                continue;
            }
            let raw = Self::get_raw_bytes(values, i);
            let canonical = canonical_indices
                .iter()
                .position(|&j| Self::get_raw_bytes(values, j) == raw);
            if let Some(idx) = canonical {
                *slot = Some(idx);
            } else {
                *slot = Some(canonical_indices.len());
                canonical_indices.push(i);
            }
        }
        // build new deduplicated values array using take
        let indices = UInt64Array::from(
            canonical_indices
                .iter()
                .map(|&i| i as u64)
                .collect::<Vec<_>>(),
        );
        let new_values = arrow::compute::take(values.as_ref(), &indices, None).unwrap();

        // remap keys
        let new_keys: Vec<Option<usize>> = (0..key_array.len())
            .map(|i| {
                if key_array.is_null(i) {
                    None
                } else {
                    let old_key = key_array.value(i).to_usize().unwrap();
                    old_to_new[old_key]
                }
            })
            .collect();

        (new_values, new_keys)
    }
}

impl<K: ArrowDictionaryKeyType + Send> GroupValues for GroupValuesDictionary<K> {
    // not really sure how to return the size of strings and binary values so this is a best effort approach
    fn size(&self) -> usize {
        size_of::<Self>()
            + self
                .seen_elements
                .iter()
                .filter_map(|opt| opt.as_ref())
                .map(|inner| inner.capacity())
                .sum::<usize>()
            + self.unique_dict_value_mapping.capacity()
                * size_of::<(u64, Vec<(usize, Vec<u8>)>)>()
    }
    fn len(&self) -> usize {
        self.seen_elements.len()
    }
    fn is_empty(&self) -> bool {
        self.seen_elements.is_empty()
    }
    fn intern(&mut self, cols: &[ArrayRef], groups: &mut Vec<usize>) -> Result<()> {
        assert_eq!(
            cols.len(),
            1,
            "GroupValuesDictionary only supports a single column"
        );
        let array = Arc::clone(&cols[0]);
        groups.clear(); // zero out buffer
        let dict_array = array
            .as_any()
            .downcast_ref::<DictionaryArray<K>>()
            .ok_or_else(|| {
                Internal(format!(
                    "GroupValuesDictionary expected DictionaryArray but got {:?}",
                    array.data_type()
                ))
            })?;

        let occupied = dict_array.occupancy().count_set_bits();
        self.seen_elements.reserve(occupied);

        let values = dict_array.values();
        let key_array = dict_array.keys();
        if key_array.is_empty() {
            return Ok(()); // nothing to intern, just return early
        }
        let (values, keys_as_usize) = Self::normalize_dict_array(values, key_array);

        let values = &values;
        let value_hashes = self.compute_value_hashes(values)?;

        let mut key_to_group: Vec<Option<usize>> = vec![None; values.len()];
        if self.intern_called {
            for value_idx in 0..values.len() {
                if values.is_null(value_idx) {
                    // this will be handled in phase 2
                    continue;
                }
                let hash = value_hashes[value_idx];
                if let Some(entries) = self.unique_dict_value_mapping.get(&hash) {
                    let raw = Self::get_raw_bytes(values, value_idx);
                    if let Some((group_id, _)) = entries
                        .iter()
                        .find(|(_, stored_bytes)| raw == stored_bytes.as_deref().unwrap())
                    /* we can safely unwrap here because of the condition 9 lines above. if the value is null its skipped and handled in phase 2*/
                    {
                        key_to_group[value_idx] = Some(*group_id);
                        continue;
                    }
                }
            }
        }
        // iterate keys array (n iterations) -
        // only d insertions at most, repeated work is cached
        for key_opt in &keys_as_usize {
            let group_id = match key_opt {
                None => self.get_null_group_id(),
                Some(key) => {
                    if let Some(group_id) = key_to_group[*key] {
                        group_id
                    } else if values.is_null(*key) {
                        let gid = self.get_null_group_id();
                        key_to_group[*key] = Some(gid);
                        gid
                    } else {
                        let new_group_id = self.seen_elements.len();
                        let raw_bytes = Self::get_raw_bytes(values, *key).to_vec();
                        self.seen_elements.push(Some(raw_bytes.clone()));
                        if let Some(entries) =
                            self.unique_dict_value_mapping.get_mut(&value_hashes[*key])
                        {
                            entries.push((new_group_id, Some(raw_bytes)));
                        } else {
                            self.unique_dict_value_mapping.insert(
                                value_hashes[*key],
                                vec![(new_group_id, Some(raw_bytes))],
                            );
                        }
                        key_to_group[*key] = Some(new_group_id);
                        new_group_id
                    }
                }
            };
            groups.push(group_id);
        }
        self.intern_called = true;
        Ok(())
    }
    fn emit(&mut self, emit_to: EmitTo) -> Result<Vec<ArrayRef>> {
        let (elements_to_emit, null_id) = match emit_to {
            EmitTo::All => {
                let original_null_id = self.null_group_id;
                self.null_group_id = None;
                self.unique_dict_value_mapping.clear();
                (std::mem::take(&mut self.seen_elements), original_null_id)
            }
            EmitTo::First(n) => {
                let n = n.min(self.seen_elements.len());
                let first_n = self.seen_elements.drain(..n).collect::<Vec<_>>();
                let original_null_id = self.null_group_id.filter(|&id| id < n);
                // update null_group_id if the null group was in the first n
                if let Some(null_id) = self.null_group_id {
                    if null_id < n {
                        self.null_group_id = None;
                    } else {
                        self.null_group_id = Some(null_id - n);
                    }
                }
                // shift all remaining group indices down by n in the map
                self.unique_dict_value_mapping.retain(|_, entries| {
                    entries.retain_mut(|(group_id, _)| {
                        if *group_id < n {
                            false
                        } else {
                            *group_id -= n;
                            true
                        }
                    });
                    !entries.is_empty()
                });
                (first_n, original_null_id)
            }
        };

        let n = elements_to_emit.len();
        let values_array = self.transform_into_array(&elements_to_emit)?;

        let mut keys_builder = PrimitiveBuilder::<K>::with_capacity(n);
        if let Some(null_id) = null_id {
            for i in 0..n {
                if i == null_id {
                    keys_builder.append_null();
                } else {
                    keys_builder.append_value(K::Native::usize_as(i));
                }
            }
        } else {
            for i in 0..n {
                keys_builder.append_value(K::Native::usize_as(i));
            }
        }
        let dict_array =
            DictionaryArray::<K>::try_new(keys_builder.finish(), values_array)?;
        Ok(vec![Arc::new(dict_array)])
    }
    fn clear_shrink(&mut self, num_rows: usize) {
        self.seen_elements.clear();
        self.seen_elements.shrink_to(num_rows);
        self.null_group_id = None;
        self.unique_dict_value_mapping.clear();
        self.unique_dict_value_mapping.shrink_to(num_rows);
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use arrow::datatypes::{Field, Int32Type};
    fn create_list_utf8_dict_array(
        lists: Vec<Option<Vec<&str>>>,
        keys: Vec<Option<usize>>,
    ) -> (ArrayRef, ArrayRef) {
        let mut list_builder = ListBuilder::new(StringBuilder::new());

        for list_items in lists {
            match list_items {
                Some(items) => {
                    for item in items {
                        list_builder.values().append_value(item);
                    }
                    list_builder.append(true);
                }
                None => list_builder.append_null(),
            }
        }

        let values_array = Arc::new(list_builder.finish()) as ArrayRef;

        let mut keys_builder = PrimitiveBuilder::<Int32Type>::with_capacity(keys.len());
        for key_opt in keys {
            match key_opt {
                Some(k) => keys_builder.append_value(k as i32),
                None => keys_builder.append_null(),
            }
        }
        let keys_array = keys_builder.finish();

        let dict_array =
            DictionaryArray::<Int32Type>::try_new(keys_array, Arc::clone(&values_array))
                .expect("Failed to create dictionary array");

        (Arc::new(dict_array), values_array)
    }

    fn create_list_large_utf8_dict_array(
        lists: Vec<Option<Vec<&str>>>,
        keys: Vec<Option<usize>>,
    ) -> (ArrayRef, ArrayRef) {
        let mut list_builder = ListBuilder::new(LargeStringBuilder::new());

        for list_items in lists {
            match list_items {
                Some(items) => {
                    for item in items {
                        list_builder.values().append_value(item);
                    }
                    list_builder.append(true);
                }
                None => list_builder.append_null(),
            }
        }

        let values_array = Arc::new(list_builder.finish()) as ArrayRef;

        let mut keys_builder = PrimitiveBuilder::<Int32Type>::with_capacity(keys.len());
        for key_opt in keys {
            match key_opt {
                Some(k) => keys_builder.append_value(k as i32),
                None => keys_builder.append_null(),
            }
        }
        let keys_array = keys_builder.finish();

        let dict_array =
            DictionaryArray::<Int32Type>::try_new(keys_array, Arc::clone(&values_array))
                .expect("Failed to create dictionary array");

        (Arc::new(dict_array), values_array)
    }

    #[cfg(test)]
    mod null_test {
        use super::*;
        #[test]
        fn test_null_keys_and_values_utf8() {
            let mut group_vals = GroupValuesDictionary::<Int32Type>::new(&DataType::Utf8);
            let mut groups = Vec::new();
            // ["x",null,"y"]
            let mut values_builder = StringBuilder::new();
            values_builder.append_value("x");
            values_builder.append_null();
            values_builder.append_value("y");
            let values_array = Arc::new(values_builder.finish()) as ArrayRef;
            // [0,null,1,null,2] => ["x", null, "y", null, "y"] -> groups = [0,1,2,1,2]
            let mut keys_builder = PrimitiveBuilder::<Int32Type>::new();
            keys_builder.append_value(0i32);
            keys_builder.append_null();
            keys_builder.append_value(1i32);
            keys_builder.append_null();
            keys_builder.append_value(2i32);
            let keys_array = keys_builder.finish();

            let dict_array = DictionaryArray::<Int32Type>::try_new(
                keys_array,
                Arc::clone(&values_array),
            )
            .expect("Failed to create dictionary array");

            group_vals
                .intern(&[Arc::new(dict_array)], &mut groups)
                .expect("intern should succeed");

            // Should have 2 values + 1 null value + 1 null key group = 4 groups
            assert_eq!(group_vals.len(), 3);
            // null keys should map to same group
            assert_eq!(groups[1], groups[3]);
            // Verify value groups are different
            assert_ne!(groups[0], groups[4]);
        }

        #[test]
        fn test_null_strings_inside_lists() {
            let mut group_vals = GroupValuesDictionary::<Int32Type>::new(
                &DataType::List(Arc::new(Field::new("item", DataType::Utf8, true))),
            );
            let mut groups = Vec::new();

            let mut list_builder = ListBuilder::new(StringBuilder::new());
            // List 0: ["a", null, "b"]
            list_builder.values().append_value("a");
            list_builder.values().append_null();
            list_builder.values().append_value("b");
            list_builder.append(true);

            // List 1: [null, null]
            list_builder.values().append_null();
            list_builder.values().append_null();
            list_builder.append(true);

            // List 2: ["a", null, "b"] - should match List 0
            list_builder.values().append_value("a");
            list_builder.values().append_null();
            list_builder.values().append_value("b");
            list_builder.append(true);

            let values_array = Arc::new(list_builder.finish()) as ArrayRef;

            let mut keys_builder = PrimitiveBuilder::<Int32Type>::new();
            keys_builder.append_value(0i32);
            keys_builder.append_value(1i32);
            keys_builder.append_value(2i32);
            let keys_array = keys_builder.finish();

            let dict_array = DictionaryArray::<Int32Type>::try_new(
                keys_array,
                Arc::clone(&values_array),
            )
            .expect("Failed to create dictionary array");

            group_vals
                .intern(&[Arc::new(dict_array)], &mut groups)
                .expect("intern should succeed");

            // Lists 0 and 2 are identical, so should have 2 unique lists
            assert_eq!(group_vals.len(), 2);
            assert_eq!(groups[0], groups[2]); // Both map to same group
            assert_ne!(groups[0], groups[1]); // Different from all-nulls list
        }

        #[test]
        fn test_empty_list_vs_null_list() {
            let mut group_vals = GroupValuesDictionary::<Int32Type>::new(
                &DataType::List(Arc::new(Field::new("item", DataType::Utf8, true))),
            );
            let mut groups = Vec::new();

            let (dict_array, _) = create_list_utf8_dict_array(
                vec![
                    Some(vec![]),    // empty list
                    None,            // null list
                    Some(vec![]),    // empty list again
                    Some(vec!["a"]), // non-empty list
                ],
                vec![Some(0), Some(1), Some(2), Some(3)],
            );

            group_vals
                .intern(&[dict_array], &mut groups)
                .expect("intern should succeed");

            // Empty lists (0 and 2) should be treated as same group
            assert_eq!(group_vals.len(), 3);
            assert_eq!(groups[0], groups[2]); // Both empty lists
            assert_ne!(groups[0], groups[1]); // Empty != null
            assert_ne!(groups[1], groups[3]); // Null != non-empty
        }

        #[test]
        fn test_null_group_stable_across_batches() {
            let mut group_vals = GroupValuesDictionary::<Int32Type>::new(
                &DataType::List(Arc::new(Field::new("item", DataType::LargeUtf8, true))),
            );
            let mut groups = Vec::new();

            // Batch 1: Create lists in order ["ab", "cd"]
            let (dict_array_1, _) = create_list_large_utf8_dict_array(
                vec![Some(vec!["ab"]), Some(vec!["cd"])],
                vec![None, Some(0), None, Some(1)], // keys: [null, 0, null, 1]
            );

            group_vals
                .intern(&[dict_array_1], &mut groups)
                .expect("first intern should succeed");

            assert_eq!(group_vals.len(), 3); // null + ["ab"] + ["cd"]
            assert_eq!(groups, vec![0, 1, 0, 2]);

            // Emit all and verify state
            let emitted_1 = group_vals
                .emit(EmitTo::All)
                .expect("first emit should succeed");
            assert_eq!(emitted_1.len(), 1);
            assert_eq!(group_vals.len(), 0);

            // Reset groups and start batch 2
            groups.clear();

            // Batch 2: Same logical values but reordered dict: ["cd", "ab"]
            let (dict_array_2, _) = create_list_large_utf8_dict_array(
                vec![Some(vec!["cd"]), Some(vec!["ab"])],
                vec![Some(0), None, Some(1), None],
            );
            // [["cd"],None,["ab"],None] => [0,1,2,1]
            group_vals
                .intern(&[dict_array_2], &mut groups)
                .expect("second intern should succeed");

            assert_eq!(group_vals.len(), 3);
            assert_eq!(groups, vec![0, 1, 2, 1]); // null group should be stable (group 1)
            assert_eq!(groups[1], groups[3]);
        }
    }
    #[cfg(test)]
    mod data_correctness {
        use super::*;
        // Non-canonicalized dictionary arrays with List<Utf8>
        #[test]
        fn test_non_canonicalized_dict_array_list() {
            let mut group_vals = GroupValuesDictionary::<Int32Type>::new(
                &DataType::List(Arc::new(Field::new("item", DataType::Utf8, true))),
            );

            // Create a non-canonicalized dictionary with duplicate values in the array
            // keys [2, 0, 1, 2, 1] mapping to duplicate lists
            let (dict_array, _) = create_list_utf8_dict_array(
                vec![
                    Some(vec!["alpha"]),
                    Some(vec!["beta"]),
                    Some(vec!["gamma"]),
                    Some(vec!["alpha"]), // duplicate
                    Some(vec!["beta"]),  // duplicate
                ],
                vec![Some(2), Some(0), Some(1), Some(2), Some(1)],
            );
            // values: [["gamma"], ["alpha"], ["beta"], ["gamma"], ["beta"]], keys: [2, 0, 1, 2, 1] => logically [["gamma"], ["alpha"], ["beta"],["gamma"],["beta"]] | groups: [0, 1, 2, 0, 2]
            // sequence of unique values : -> ["gamma"] -> ["alpha"] -> ["beta"] (0,1,2)
            let mut groups = Vec::new();
            group_vals
                .intern(&[dict_array], &mut groups)
                .expect("intern should succeed");

            // Should have 3 unique groups due to deduplication
            assert_eq!(group_vals.len(), 3);
            assert_eq!(groups.len(), 5);

            // gamma appears at rows 0 and 3
            assert_eq!(groups[0], groups[3]);
            // beta appears at rows 2 and 4
            assert_eq!(groups[2], groups[4]);

            let result = group_vals.emit(EmitTo::All).expect("emit should succeed");
            let emitted = result[0]
                .as_any()
                .downcast_ref::<DictionaryArray<Int32Type>>()
                .expect("Expected DictionaryArray");

            let list_array = emitted
                .values()
                .as_any()
                .downcast_ref::<ListArray>()
                .expect("Expected ListArray");

            // [["gamma"], ["alpha"], ["beta"]] - order in which they should be emitted
            assert_eq!(list_array.len(), 3);

            let list_0 = list_array.value(0);
            let strings_0 = list_0
                .as_any()
                .downcast_ref::<StringArray>()
                .expect("Expected StringArray in list 0");
            assert_eq!(strings_0.value(0), "gamma");

            let list_1 = list_array.value(1);
            let strings_1 = list_1
                .as_any()
                .downcast_ref::<StringArray>()
                .expect("Expected StringArray in list 1");
            assert_eq!(strings_1.value(0), "alpha");

            let list_2 = list_array.value(2);
            let strings_2 = list_2
                .as_any()
                .downcast_ref::<StringArray>()
                .expect("Expected StringArray in list 2");
            assert_eq!(strings_2.value(0), "beta");

            assert!(group_vals.is_empty());
        }

        // Verify that duplicate values point to the same group id and are emitted only once
        #[test]
        fn test_duplicate_values_in_values_array_utf8() {
            let mut group_vals = GroupValuesDictionary::<Int32Type>::new(&DataType::Utf8);

            // Create a dictionary where values array has duplicates
            // Keys [0, 1, 2, 1, 0] with values ["x", "y", "x", "y", "x"]
            // "x" appears at indices 0 and 2 in the values array
            // "y" appears at indices 1 and 3
            let mut values_builder = StringBuilder::new();
            values_builder.append_value("x");
            values_builder.append_value("y");
            values_builder.append_value("x"); // duplicate "x"
            values_builder.append_value("y"); // duplicate "y"
            let values_array = Arc::new(values_builder.finish()) as ArrayRef;

            let mut keys_builder = PrimitiveBuilder::<Int32Type>::new();
            keys_builder.append_value(0i32); // "x"
            keys_builder.append_value(1i32); // "y"
            keys_builder.append_value(2i32); // "x" (duplicate)
            keys_builder.append_value(3i32); // "y" (duplicate)
            keys_builder.append_value(0i32); // "x"
            let keys_array = keys_builder.finish();

            let dict_array = DictionaryArray::<Int32Type>::try_new(
                keys_array,
                Arc::clone(&values_array),
            )
            .expect("Failed to create dictionary array");

            let mut groups = Vec::new();
            group_vals
                .intern(&[Arc::new(dict_array)], &mut groups)
                .expect("intern should succeed");

            // Should have only 2 unique groups: "x" and "y"
            // Keys pointing to 0 and 2 (both "x") should map to same group
            // Keys pointing to 1 and 3 (both "y") should map to same group
            assert_eq!(group_vals.len(), 2);
            assert_eq!(groups.len(), 5);

            // rows 0, 2, 4 all point to values "x"
            assert_eq!(groups[0], groups[2]);
            assert_eq!(groups[2], groups[4]);
            // rows 1, 3 point to values "y"
            assert_eq!(groups[1], groups[3]);
            // x and y groups are distinct
            assert_ne!(groups[0], groups[1]);

            let result = group_vals.emit(EmitTo::All).expect("emit should succeed");
            let emitted = result[0]
                .as_any()
                .downcast_ref::<DictionaryArray<Int32Type>>()
                .expect("Expected DictionaryArray");

            let string_values = emitted
                .values()
                .as_any()
                .downcast_ref::<StringArray>()
                .expect("Expected StringArray");

            // Should emit exactly 2 unique values: "x" and "y"
            let values_set: std::collections::HashSet<String> = (0..string_values.len())
                .filter_map(|i| {
                    if string_values.is_null(i) {
                        None
                    } else {
                        Some(string_values.value(i).to_string())
                    }
                })
                .collect();

            assert_eq!(values_set.len(), 2);
            assert!(values_set.contains("x"));
            assert!(values_set.contains("y"));

            assert!(group_vals.is_empty());
        }

        #[test]
        fn test_duplicate_values_in_values_array_list() {
            let mut group_vals = GroupValuesDictionary::<Int32Type>::new(
                &DataType::List(Arc::new(Field::new("item", DataType::Utf8, true))),
            );

            // Create a list dictionary where the values array has duplicate lists
            // values : [["x"],["y"],["x"],["y"]]
            let mut builder = ListBuilder::new(StringBuilder::new());

            // List 0: ["x"]
            builder.values().append_value("x");
            builder.append(true);

            // List 1: ["y"]
            builder.values().append_value("y");
            builder.append(true);

            // List 2: ["x"] - duplicate of list 0
            builder.values().append_value("x");
            builder.append(true);

            // List 3: ["y"] - duplicate of list 1
            builder.values().append_value("y");
            builder.append(true);

            let values_array = Arc::new(builder.finish()) as ArrayRef;
            // keys [0, 1, 2, 3, 0] => logically : ["x", "y", "x", "y", "x"]
            let mut keys_builder = PrimitiveBuilder::<Int32Type>::new();
            keys_builder.append_value(0i32);
            keys_builder.append_value(1i32);
            keys_builder.append_value(2i32);
            keys_builder.append_value(3i32);
            keys_builder.append_value(0i32);
            let keys_array = keys_builder.finish();

            let dict_array = DictionaryArray::<Int32Type>::try_new(
                keys_array,
                Arc::clone(&values_array),
            )
            .expect("Failed to create dictionary array");

            let mut groups = Vec::new();
            group_vals
                .intern(&[Arc::new(dict_array)], &mut groups)
                .expect("intern should succeed");

            // Should have only 2 unique groups
            assert_eq!(group_vals.len(), 2);
            assert_eq!(groups.len(), 5);

            // rows 0, 2, 4 point to ["x"]
            assert_eq!(groups[0], groups[2]);
            assert_eq!(groups[2], groups[4]);
            // rows 1, 3 point to ["y"]
            assert_eq!(groups[1], groups[3]);
            assert_ne!(groups[0], groups[1]);

            let result = group_vals.emit(EmitTo::All).expect("emit should succeed");
            let emitted = result[0]
                .as_any()
                .downcast_ref::<DictionaryArray<Int32Type>>()
                .expect("Expected DictionaryArray");
            let list_array = emitted
                .values()
                .as_any()
                .downcast_ref::<ListArray>()
                .expect("Expected ListArray");

            assert!(group_vals.is_empty());
            // Should have exactly 2 lists emitted
            assert_eq!(list_array.len(), 2);
            // Verify the two values are "x" and "y" in correct order
            let string_array = list_array
                .values()
                .as_any()
                .downcast_ref::<StringArray>()
                .expect("Expected StringArray");
            // validate order
            assert_eq!(string_array.value(0), "x");
            assert_eq!(string_array.value(1), "y");
        }
    }
}
