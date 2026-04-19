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
                        let len = i32::from_ne_bytes(
                            raw_vector[offset..offset + 4]
                                .try_into()
                                .expect("slice of length 4"),
                        );
                        offset += 4;
                        if len == -1 {
                            builder.values().append_null();
                        } else {
                            let s = std::str::from_utf8(
                                &raw_vector[offset..offset + len as usize],
                            )
                            .map_err(|e| {
                                datafusion_common::DataFusionError::Internal(format!(
                                    "Invalid utf8 in list element: {e}"
                                ))
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
                        datafusion_common::DataFusionError::Internal(format!(
                            "Invalid utf8 in GroupValuesDictionary: {e}"
                        ))
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
    seen_elements: Vec<Option<Vec<u8>>>, //  Box<dyn Builder> doesnt provide the flexibility of building partition arrays that wed need to support emit::First(N)
    value_dt: DataType,
    _phantom: PhantomData<K>,
    // keeps track of which values weve already seen. stored as -> <unique_value_hash:(initial_group_id, raw_bytes)>
    unique_dict_value_mapping: HashMap<u64, Vec<GroupEntry>>,

    random_state: RandomState,

    // cache the group id for nulls since they all map to the same group
    null_group_id: Option<usize>,
    // tracks if intern has ever been called. this is used to determine if we can skip phaase 1 of of intern.
    // phrase one is where we build a hash -> group id mapping for all unique values in the dictionary to avoid repeated hashmap lookups + equality checks in the hot loop of phase 2.
    // if intern has never been called, we know for certain that no insertions have been made and we can skip phase 1 entirely since the mapping will be empty and not match any values.
    // after the first call to intern, we know that at least one insertion has been made and we have to do phase 1 on every subsequent call to intern to ensure correctness.
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

                if list_array.is_null(index) {
                    panic!() // this cannot happen. leaving this here as an invariant
                }

                let start = list_array.value_offsets()[index] as usize;
                let end = list_array.value_offsets()[index + 1] as usize;
                let child = list_array.values();

                let mut bytes = Vec::new();
                for i in start..end {
                    if child.is_null(i) {
                        // acts as a marker for transform_into_array to write a null
                        bytes.extend_from_slice(&(-1i32).to_ne_bytes());
                    } else {
                        let raw = Self::get_raw_bytes(child, i);
                        bytes.extend_from_slice(&(raw.len() as i32).to_ne_bytes());
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
            // first time we've seen a null
            let new_group_id = self.seen_elements.len();
            self.seen_elements.push(None);
            self.unique_dict_value_mapping
                .insert((usize::MAX - 1) as u64, vec![(new_group_id, None)]);
            self.null_group_id = Some(new_group_id); // never compute this again
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
                other => Err(datafusion_common::DataFusionError::NotImplemented(
                    format!("transform_into_array not implemented for List<{other:?}>"),
                )),
            },
            other => Err(datafusion_common::DataFusionError::NotImplemented(format!(
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
        if cols.len() != 1 {
            return Err(datafusion_common::DataFusionError::Internal(
                "GroupValuesDictionary only supports single column group by".to_string(),
            ));
        }
        let array = Arc::clone(&cols[0]);
        groups.clear(); // zero out buffer
        let dict_array = array
            .as_any()
            .downcast_ref::<DictionaryArray<K>>()
            .ok_or_else(|| {
                datafusion_common::DataFusionError::Internal(format!(
                    "GroupValuesDictionary expected DictionaryArray but got {:?}",
                    array.data_type()
                ))
            })?;

        // pre-allocate space for seen_elements using occupancy
        // occupancy count gives us the number of truly distinct non-null values in this batch
        let occupied = dict_array.occupancy().count_set_bits();
        self.seen_elements.reserve(occupied);

        let values = dict_array.values();
        let key_array = dict_array.keys();
        if key_array.is_empty() {
            return Ok(()); // nothing to intern, just return early
        }
        // convert key array to Vec<usize> for cheap indexed access
        // avoids repeated .value(i).to_usize() calls in the hot loop
        let (values, keys_as_usize) = Self::normalize_dict_array(values, key_array);
        let values = &values;
        // compute hashes for all values in the values array upfront
        // value_hashes[i] corresponds to values[i]
        let value_hashes = self.compute_value_hashes(values)?;

        // Pass 1: iterate values array (d iterations) - build a mapping of value hash -> group id for all unique values in the dictionary
        // this allows us to do a single hashmap lookup per key in the hot loop instead of doing a hashmap lookup + equality check for every key
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
        // Pass 2: iterate keys array (n iterations) -
        // only d insertions at most, repeated work is cached
        for key_opt in &keys_as_usize {
            let group_id = match key_opt {
                None => self.get_null_group_id(),
                Some(key) => {
                    if let Some(group_id) = key_to_group[*key] {
                        group_id
                    } else if values.is_null(*key) {
                        let gid = self.get_null_group_id();
                        key_to_group[*key] = Some(gid); // cache it for future keys that point to null values
                        gid
                    } else {
                        // new unique value we havent seen before, assign a new group id and store it in the map
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
        self.intern_called = true; // set this flag after the first call to intern so that we know to do phase 1 on subsequent calls. 
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

        // reconstruct dictionary keys 0..n
        let mut keys_builder = PrimitiveBuilder::<K>::with_capacity(n);
        // if no nulls exist in this emit batch, use a tighter loop as opposed to having a
        // conditional branch
        if let Some(null_id) = null_id {
            for i in 0..n {
                if i == null_id {
                    // TODO: should keys contain nulls?
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

    fn create_utf8_dict_array(
        values: Vec<&str>,
        keys: Vec<Option<usize>>,
    ) -> (ArrayRef, ArrayRef) {
        let mut values_builder = StringBuilder::new();
        for val in values {
            values_builder.append_value(val);
        }
        let values_array = Arc::new(values_builder.finish()) as ArrayRef;

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

    /// Helper function to assert dictionary array values match expected strings
    /// Supports Utf8, Utf8View, and LargeUtf8 data types
    fn assert_dict_array_values(
        dict_array: &DictionaryArray<Int32Type>,
        expected: &[Option<&str>],
        data_type: &DataType,
    ) {
        let values = dict_array.values();

        assert_eq!(values.len(), expected.len(), "Mismatch in array length");

        match data_type {
            DataType::Utf8 => {
                let value_array = values
                    .as_any()
                    .downcast_ref::<StringArray>()
                    .expect("Expected StringArray for Utf8");
                for (i, exp) in expected.iter().enumerate() {
                    if let Some(exp_val) = exp {
                        assert_eq!(&value_array.value(i), exp_val);
                    } else {
                        assert!(value_array.is_null(i));
                    }
                }
            }
            DataType::Utf8View => {
                let value_array = values
                    .as_any()
                    .downcast_ref::<StringViewArray>()
                    .expect("Expected StringViewArray for Utf8View");
                for (i, exp) in expected.iter().enumerate() {
                    if let Some(exp_val) = exp {
                        assert_eq!(&value_array.value(i), exp_val);
                    } else {
                        assert!(value_array.is_null(i));
                    }
                }
            }
            DataType::LargeUtf8 => {
                let value_array = values
                    .as_any()
                    .downcast_ref::<LargeStringArray>()
                    .expect("Expected LargeStringArray for LargeUtf8");
                for (i, exp) in expected.iter().enumerate() {
                    if let Some(exp_val) = exp {
                        assert_eq!(&value_array.value(i), exp_val);
                    } else {
                        assert!(value_array.is_null(i));
                    }
                }
            }
            _ => panic!(
                "Unsupported data type for assert_dict_array_values: {data_type:?}",
            ),
        }
    }

    /// Helper function to create a List<Utf8> dictionary array for testing.
    /// Each list contains a sequence of strings. Use Option::None for null lists.
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

    /// Helper function to create a List<LargeUtf8> dictionary array for testing.
    /// Each list contains a sequence of strings. Use Option::None for null lists.
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

    /// Helper function to create a List<Utf8View> dictionary array for testing.
    /// Each list contains a sequence of strings. Use Option::None for null lists.
    fn create_list_utf8view_dict_array(
        lists: Vec<Option<Vec<&str>>>,
        keys: Vec<Option<usize>>,
    ) -> (ArrayRef, ArrayRef) {
        let mut list_builder = ListBuilder::new(StringViewBuilder::new());

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
    mod basic_functionality {
        use super::*;
        // 1. basic functionality test
        // * call intern with all data types we support and verify the group ids are correct
        #[test]
        fn test_intern_all_supported_data_types() {
            // UTF8
            {
                let (dict_array, _) = create_utf8_dict_array(
                    vec!["alice", "bob", "charlie"],
                    vec![Some(0), Some(1), Some(2), Some(0), Some(1)],
                );
                let mut group_vals =
                    GroupValuesDictionary::<Int32Type>::new(&DataType::Utf8);
                let mut groups = Vec::new();

                group_vals
                    .intern(&[dict_array], &mut groups)
                    .expect("intern should succeed");

                // We should have 3 unique values for 5 keys
                assert_eq!(group_vals.len(), 3);
                // First key points to "alice" (group 0), second to "bob" (group 1), etc.
                assert_eq!(groups, vec![0, 1, 2, 0, 1]);
            }

            // LargeUtf8
            {
                let mut values_builder = LargeStringBuilder::new();
                values_builder.append_value("david");
                values_builder.append_value("eve");
                let values_array = Arc::new(values_builder.finish()) as ArrayRef;

                let mut keys_builder = PrimitiveBuilder::<Int32Type>::new();
                keys_builder.append_value(0i32);
                keys_builder.append_value(1i32);
                keys_builder.append_value(0i32);
                let keys_array = keys_builder.finish();

                let dict_array = DictionaryArray::<Int32Type>::try_new(
                    keys_array,
                    Arc::clone(&values_array),
                )
                .expect("Failed to create dictionary array");

                let mut group_vals =
                    GroupValuesDictionary::<Int32Type>::new(&DataType::LargeUtf8);
                let mut groups = Vec::new();

                group_vals
                    .intern(&[Arc::new(dict_array)], &mut groups)
                    .expect("intern should succeed");

                assert_eq!(group_vals.len(), 2);
                assert_eq!(groups, vec![0, 1, 0]);
            }

            // Utf8View
            {
                let mut values_builder = StringViewBuilder::new();
                values_builder.append_value("frank");
                values_builder.append_value("grace");
                let values_array = Arc::new(values_builder.finish()) as ArrayRef;

                let mut keys_builder = PrimitiveBuilder::<Int32Type>::new();
                keys_builder.append_value(0i32);
                keys_builder.append_value(1i32);
                keys_builder.append_value(0i32);
                let keys_array = keys_builder.finish();

                let dict_array = DictionaryArray::<Int32Type>::try_new(
                    keys_array,
                    Arc::clone(&values_array),
                )
                .expect("Failed to create dictionary array");

                let mut group_vals =
                    GroupValuesDictionary::<Int32Type>::new(&DataType::Utf8View);
                let mut groups = Vec::new();

                group_vals
                    .intern(&[Arc::new(dict_array)], &mut groups)
                    .expect("intern should succeed");

                assert_eq!(group_vals.len(), 2);
                assert_eq!(groups, vec![0, 1, 0]);
            }

            // List<Utf8>
            {
                let (dict_array, _) = create_list_utf8_dict_array(
                    vec![
                        Some(vec!["a", "b"]),
                        Some(vec!["c", "d"]),
                        Some(vec!["a", "b"]),
                    ],
                    vec![Some(0), Some(1), Some(0), Some(2)],
                );
                let mut group_vals = GroupValuesDictionary::<Int32Type>::new(
                    &DataType::List(Arc::new(Field::new("item", DataType::Utf8, true))),
                );
                let mut groups = Vec::new();

                group_vals
                    .intern(&[dict_array], &mut groups)
                    .expect("intern should succeed");

                // We should have 2 unique lists: ["a", "b"] and ["c", "d"]
                assert_eq!(group_vals.len(), 2);
                assert_eq!(groups, vec![0, 1, 0, 0]);
            }

            // List<LargeUtf8>
            {
                let (dict_array, _) = create_list_large_utf8_dict_array(
                    vec![Some(vec!["x", "y"]), Some(vec!["p", "q"])],
                    vec![Some(0), Some(1), Some(0)],
                );
                let mut group_vals =
                    GroupValuesDictionary::<Int32Type>::new(&DataType::List(Arc::new(
                        Field::new("item", DataType::LargeUtf8, true),
                    )));
                let mut groups = Vec::new();

                group_vals
                    .intern(&[dict_array], &mut groups)
                    .expect("intern should succeed");

                assert_eq!(group_vals.len(), 2);
                assert_eq!(groups, vec![0, 1, 0]);
            }

            // List<Utf8View>
            {
                let (dict_array, _) = create_list_utf8view_dict_array(
                    vec![Some(vec!["m", "n"]), Some(vec!["r", "s"])],
                    vec![Some(0), Some(1), Some(0)],
                );
                let mut group_vals =
                    GroupValuesDictionary::<Int32Type>::new(&DataType::List(Arc::new(
                        Field::new("item", DataType::Utf8View, true),
                    )));
                let mut groups = Vec::new();

                group_vals
                    .intern(&[dict_array], &mut groups)
                    .expect("intern should succeed");

                assert_eq!(group_vals.len(), 2);
                assert_eq!(groups, vec![0, 1, 0]);
            }
        }

        // * call intern multiple times and verify behavior is correct [with same columns , with different columns]
        #[test]
        fn test_intern_multiple_times_same_and_different_columns() {
            // Test with Utf8
            {
                let mut group_vals =
                    GroupValuesDictionary::<Int32Type>::new(&DataType::Utf8);
                let mut groups = Vec::new();

                // First call with ["alice", "bob", "charlie"]
                let (dict_array_1, _) = create_utf8_dict_array(
                    vec!["alice", "bob", "charlie"],
                    vec![Some(0), Some(1), Some(2)],
                );
                group_vals
                    .intern(&[dict_array_1], &mut groups)
                    .expect("first intern should succeed");
                assert_eq!(group_vals.len(), 3);
                assert_eq!(groups, vec![0, 1, 2]);

                // Reset groups and call again with same values but different order
                groups.clear();
                let (dict_array_2, _) = create_utf8_dict_array(
                    vec!["alice", "bob", "charlie"],
                    vec![Some(2), Some(1), Some(0)],
                );
                group_vals
                    .intern(&[dict_array_2], &mut groups)
                    .expect("second intern should succeed");
                // No new unique values, so length should still be 3
                assert_eq!(group_vals.len(), 3);
                assert_eq!(groups, vec![2, 1, 0]);

                // Reset groups and call with some new values
                groups.clear();
                let (dict_array_3, _) = create_utf8_dict_array(
                    vec!["alice", "bob", "charlie", "david"],
                    vec![Some(0), Some(3)],
                );
                group_vals
                    .intern(&[dict_array_3], &mut groups)
                    .expect("third intern should succeed");
                // One new unique value "david"
                assert_eq!(group_vals.len(), 4);
                assert_eq!(groups, vec![0, 3]);
            }

            // Test with List<LargeUtf8>
            {
                let mut group_vals =
                    GroupValuesDictionary::<Int32Type>::new(&DataType::List(Arc::new(
                        Field::new("item", DataType::LargeUtf8, true),
                    )));
                let mut groups = Vec::new();

                // First call with [["x", "y"], ["p", "q"], ["x", "y"]]
                let (dict_array_1, _) = create_list_large_utf8_dict_array(
                    vec![
                        Some(vec!["x", "y"]),
                        Some(vec!["p", "q"]),
                        Some(vec!["x", "y"]),
                    ],
                    vec![Some(0), Some(1), Some(2)],
                );
                group_vals
                    .intern(&[dict_array_1], &mut groups)
                    .expect("first intern should succeed");
                assert_eq!(group_vals.len(), 2);
                assert_eq!(groups, vec![0, 1, 0]);

                // Reset groups and call again with same values but different order
                groups.clear();
                let (dict_array_2, _) = create_list_large_utf8_dict_array(
                    vec![
                        Some(vec!["x", "y"]),
                        Some(vec!["p", "q"]),
                        Some(vec!["x", "y"]),
                    ],
                    vec![Some(2), Some(1), Some(0)],
                );
                group_vals
                    .intern(&[dict_array_2], &mut groups)
                    .expect("second intern should succeed");
                // No new unique values, so length should still be 2
                assert_eq!(group_vals.len(), 2);
                assert_eq!(groups, vec![0, 1, 0]);

                // Reset groups and call with some new values
                groups.clear();
                let (dict_array_3, _) = create_list_large_utf8_dict_array(
                    vec![
                        Some(vec!["x", "y"]),
                        Some(vec!["p", "q"]),
                        Some(vec!["x", "y"]),
                        Some(vec!["a", "b"]),
                    ],
                    vec![Some(0), Some(3)],
                );
                group_vals
                    .intern(&[dict_array_3], &mut groups)
                    .expect("third intern should succeed");
                // One new unique list ["a", "b"]
                assert_eq!(group_vals.len(), 3);
                assert_eq!(groups, vec![0, 2]);
            }
        }

        // * call intern multiple times, then call emit and verify that the emitted arrays are correct and that subsequent calls to intern and emit behave correctly with the updated state after emit
        #[test]
        fn test_intern_multiple_times_then_emit() {
            // Test with Utf8View
            {
                let mut group_vals =
                    GroupValuesDictionary::<Int32Type>::new(&DataType::Utf8View);
                let mut groups = Vec::new();

                // First intern call
                let mut values_builder = StringViewBuilder::new();
                values_builder.append_value("alice");
                values_builder.append_value("bob");
                let values_array = Arc::new(values_builder.finish()) as ArrayRef;

                let mut keys_builder = PrimitiveBuilder::<Int32Type>::new();
                keys_builder.append_value(0i32);
                keys_builder.append_value(1i32);
                keys_builder.append_value(0i32);
                let keys_array = keys_builder.finish();

                let dict_array_1 = DictionaryArray::<Int32Type>::try_new(
                    keys_array,
                    Arc::clone(&values_array),
                )
                .expect("Failed to create dictionary array");

                group_vals
                    .intern(&[Arc::new(dict_array_1)], &mut groups)
                    .expect("first intern should succeed");
                assert_eq!(group_vals.len(), 2);

                // Second intern call with new values
                groups.clear();
                let mut values_builder = StringViewBuilder::new();
                values_builder.append_value("alice");
                values_builder.append_value("bob");
                values_builder.append_value("charlie");
                let values_array = Arc::new(values_builder.finish()) as ArrayRef;

                let mut keys_builder = PrimitiveBuilder::<Int32Type>::new();
                keys_builder.append_value(2i32);
                keys_builder.append_value(1i32);
                let keys_array = keys_builder.finish();

                let dict_array_2 = DictionaryArray::<Int32Type>::try_new(
                    keys_array,
                    Arc::clone(&values_array),
                )
                .expect("Failed to create dictionary array");

                group_vals
                    .intern(&[Arc::new(dict_array_2)], &mut groups)
                    .expect("second intern should succeed");
                assert_eq!(group_vals.len(), 3);

                // Emit first group
                let emitted = group_vals
                    .emit(EmitTo::First(1))
                    .expect("emit should succeed");
                assert_eq!(emitted.len(), 1);
                assert_eq!(group_vals.len(), 2);

                let dict_array = emitted[0]
                    .as_any()
                    .downcast_ref::<DictionaryArray<Int32Type>>()
                    .expect("Expected DictionaryArray");
                assert_dict_array_values(
                    dict_array,
                    &[Some("alice")],
                    &DataType::Utf8View,
                );

                // Intern again and verify that new groups start from 0
                groups.clear();
                let mut values_builder = StringViewBuilder::new();
                values_builder.append_value("alice");
                values_builder.append_value("bob");
                values_builder.append_value("charlie");
                values_builder.append_value("david");
                let values_array = Arc::new(values_builder.finish()) as ArrayRef;

                let mut keys_builder = PrimitiveBuilder::<Int32Type>::new();
                keys_builder.append_value(0i32);
                keys_builder.append_value(3i32);
                let keys_array = keys_builder.finish();

                let dict_array_3 = DictionaryArray::<Int32Type>::try_new(
                    keys_array,
                    Arc::clone(&values_array),
                )
                .expect("Failed to create dictionary array");

                group_vals
                    .intern(&[Arc::new(dict_array_3)], &mut groups)
                    .expect("third intern should succeed");
                // Groups from previous emit are renumbered: "bob" is now 0, "charlie" is 1, "david" is 2
                // So "alice" and "david" should get new group ids
                assert_eq!(groups.len(), 2);
            }

            // Test with List<Utf8View>
            {
                let mut group_vals =
                    GroupValuesDictionary::<Int32Type>::new(&DataType::List(Arc::new(
                        Field::new("item", DataType::Utf8View, true),
                    )));
                let mut groups = Vec::new();

                // First intern call
                let (dict_array_1, _) = create_list_utf8view_dict_array(
                    vec![Some(vec!["alice", "bob"]), Some(vec!["charlie"])],
                    vec![Some(0), Some(1), Some(0)],
                );
                group_vals
                    .intern(&[dict_array_1], &mut groups)
                    .expect("first intern should succeed");
                assert_eq!(group_vals.len(), 2);

                // Second intern call with new values
                groups.clear();
                let (dict_array_2, _) = create_list_utf8view_dict_array(
                    vec![
                        Some(vec!["alice", "bob"]),
                        Some(vec!["charlie"]),
                        Some(vec!["david", "eve"]),
                    ],
                    vec![Some(2), Some(1)],
                );
                group_vals
                    .intern(&[dict_array_2], &mut groups)
                    .expect("second intern should succeed");
                assert_eq!(group_vals.len(), 3);

                // Emit first group
                let emitted = group_vals
                    .emit(EmitTo::First(1))
                    .expect("emit should succeed");
                assert_eq!(emitted.len(), 1);
                assert_eq!(group_vals.len(), 2);

                // Intern again and verify that new groups start from 0
                groups.clear();
                let (dict_array_3, _) = create_list_utf8view_dict_array(
                    vec![
                        Some(vec!["alice", "bob"]),
                        Some(vec!["charlie"]),
                        Some(vec!["david", "eve"]),
                        Some(vec!["frank"]),
                    ],
                    vec![Some(0), Some(3)],
                );
                group_vals
                    .intern(&[dict_array_3], &mut groups)
                    .expect("third intern should succeed");
                // Groups from previous emit are renumbered
                assert_eq!(groups.len(), 2);
            }
        }

        // * call intern multiple times, then call emit multiple times , verify its correct and then verify the once drained the len is 0
        #[test]
        fn test_intern_multiple_times_emit_multiple_times() {
            // Test with Utf8
            {
                let mut group_vals =
                    GroupValuesDictionary::<Int32Type>::new(&DataType::Utf8);
                let mut groups = Vec::new();

                // Intern 4 unique values
                let (dict_array_1, _) = create_utf8_dict_array(
                    vec!["a", "b", "c", "d"],
                    vec![Some(0), Some(1), Some(2), Some(3)],
                );
                group_vals
                    .intern(&[dict_array_1], &mut groups)
                    .expect("intern should succeed");
                assert_eq!(group_vals.len(), 4);

                // Emit first 2 values
                let emitted_1 = group_vals
                    .emit(EmitTo::First(2))
                    .expect("first emit should succeed");
                assert_eq!(emitted_1.len(), 1);
                assert_eq!(group_vals.len(), 2);

                let dict_array_1 = emitted_1[0]
                    .as_any()
                    .downcast_ref::<DictionaryArray<Int32Type>>()
                    .expect("Expected DictionaryArray");
                assert_dict_array_values(
                    dict_array_1,
                    &[Some("a"), Some("b")],
                    &DataType::Utf8,
                );

                // Emit all remaining
                let emitted_2 = group_vals
                    .emit(EmitTo::All)
                    .expect("second emit should succeed");
                assert_eq!(emitted_2.len(), 1);
                assert_eq!(group_vals.len(), 0);

                let dict_array_2 = emitted_2[0]
                    .as_any()
                    .downcast_ref::<DictionaryArray<Int32Type>>()
                    .expect("Expected DictionaryArray");
                assert_dict_array_values(
                    dict_array_2,
                    &[Some("c"), Some("d")],
                    &DataType::Utf8,
                );
            }

            // Test with List<Utf8View>
            {
                let mut group_vals =
                    GroupValuesDictionary::<Int32Type>::new(&DataType::List(Arc::new(
                        Field::new("item", DataType::Utf8View, true),
                    )));
                let mut groups = Vec::new();

                // Intern 4 unique lists
                let (dict_array_1, _) = create_list_utf8view_dict_array(
                    vec![
                        Some(vec!["a"]),
                        Some(vec!["b"]),
                        Some(vec!["c"]),
                        Some(vec!["d"]),
                    ],
                    vec![Some(0), Some(1), Some(2), Some(3)],
                );
                group_vals
                    .intern(&[dict_array_1], &mut groups)
                    .expect("intern should succeed");
                assert_eq!(group_vals.len(), 4);

                // Emit first 2 lists
                let emitted_1 = group_vals
                    .emit(EmitTo::First(2))
                    .expect("first emit should succeed");
                assert_eq!(emitted_1.len(), 1);
                assert_eq!(group_vals.len(), 2);

                // Emit all remaining
                let emitted_2 = group_vals
                    .emit(EmitTo::All)
                    .expect("second emit should succeed");
                assert_eq!(emitted_2.len(), 1);
                assert_eq!(group_vals.len(), 0);
                let _ = emitted_2[0]
                    .as_any()
                    .downcast_ref::<DictionaryArray<Int32Type>>()
                    .unwrap();
            }
        }

        // * call intern and emit with null values and verify that nulls are handled correctly
        #[test]
        fn test_intern_and_emit_with_nulls() {
            // Test with Utf8 simple nulls
            {
                let mut group_vals =
                    GroupValuesDictionary::<Int32Type>::new(&DataType::Utf8);
                let mut groups = Vec::new();

                let (dict_array, _) = create_utf8_dict_array(
                    // unique -> alice,null,bob,                 0,1,2 -> groups:[0,1,2,1,0]
                    vec!["alice", "bob"],
                    vec![Some(0), None, Some(1), None, Some(0)],
                );
                group_vals
                    .intern(&[dict_array], &mut groups)
                    .expect("intern should succeed");
                // 2 unique values + 1 null group = 3 groups
                assert_eq!(group_vals.len(), 3);
                // groups should be: [0, 2, 1, 2, 0] (null maps to group 2)
                assert_eq!(groups[0], 0); // "alice"
                assert_eq!(groups[1], groups[3]); // both nulls map to same group
                assert_eq!(groups[2], 2); // "bob"

                let _null_group_id = groups[1];

                // Emit first 2 groups
                let emitted = group_vals
                    .emit(EmitTo::First(2))
                    .expect("emit should succeed");

                let dict_array = emitted[0]
                    .as_any()
                    .downcast_ref::<DictionaryArray<Int32Type>>()
                    .expect("Expected DictionaryArray");

                // Check that we have correct values and a null
                let values = dict_array.values();
                let value_array = values
                    .as_any()
                    .downcast_ref::<StringArray>()
                    .expect("Expected StringArray");

                assert_eq!(value_array.len(), 2);
                // The keys should map values correctly, one being null
                let keys = dict_array.keys();
                assert_eq!(keys.len(), 2);

                // Verify state after emit
                assert_eq!(group_vals.len(), 1);
            }

            // Test with List<Utf8View> complex null cases: nulls in keys and null lists
            {
                let mut group_vals =
                    GroupValuesDictionary::<Int32Type>::new(&DataType::List(Arc::new(
                        Field::new("item", DataType::Utf8View, true),
                    )));
                let mut groups = Vec::new();

                // Create a list dictionary with:
                // - Some valid lists: ["a", "b"], ["c"], ["x", "y"]
                // - Some null keys (None entries in the keys vector)
                // - Some null lists (None in the values)
                let (dict_array, _) = create_list_utf8view_dict_array(
                    vec![
                        Some(vec!["a", "b"]),
                        Some(vec!["c"]),
                        Some(vec!["x", "y"]),
                        None, // null list at index 3
                    ],
                    // 0,1,2,3,1,4,0
                    vec![
                        Some(0),
                        None, // null key at position 1
                        Some(1),
                        Some(2),
                        None, // null key at position 4
                        Some(3),
                        Some(0),
                    ],
                );
                group_vals
                    .intern(&[dict_array], &mut groups)
                    .expect("intern should succeed");

                // Should have: 3 valid lists + 1 null list
                assert_eq!(group_vals.len(), 4);
                // Verify null keys map to same group
                assert_eq!(groups[1], groups[4]); // both null keys map to same group

                // groups should be: [0, null_group_id, 1, 2, null_group_id, 3, 0]
                let _null_key_group = groups[1];
                let _null_list_group = groups[4]; // the null list at index 3 points to a specific group

                // First intern: [group_0, null_grp, group_1, group_2, null_grp, group_3, group_0]
                // where group_3 is the null list

                // Emit first 2 groups (should include some values and maybe null groups)
                let emitted_1 = group_vals
                    .emit(EmitTo::First(1))
                    .expect("first emit should succeed");
                assert_eq!(emitted_1.len(), 1);
                let remaining_after_first_emit = group_vals.len();
                assert_eq!(remaining_after_first_emit, 3);

                // Intern again with different keys, including another null list
                groups.clear();
                let (dict_array_2, _) = create_list_utf8view_dict_array(
                    vec![Some(vec!["a", "b"]), Some(vec!["m", "n"]), None], // another null list
                    vec![Some(0), None, Some(1), Some(2)],
                );
                let pre_len = group_vals.len();
                group_vals
                    .intern(&[dict_array_2], &mut groups)
                    .expect("second intern should succeed");

                // ["a","b"] was emitted in first emit, so it should get a new group id. "m","n" is new and should get a new group id. The null list should map to the same null list group as before.
                assert_eq!(group_vals.len(), pre_len + 2);
                // Verify null keys still map to same group even after second intern

                // Emit all remaining
                let emitted_2 = group_vals
                    .emit(EmitTo::All)
                    .expect("second emit should succeed");
                assert_eq!(emitted_2.len(), 1);
                assert_eq!(group_vals.len(), 0);
                let values = emitted_2[0]
                    .as_any()
                    .downcast_ref::<DictionaryArray<Int32Type>>()
                    .unwrap();
                let dict_values = values.values();
                //[null,["c"],["x","y"],["a","b"],["m","n"]]
                // dict_values should be a ListArray containing StringViewArray
                let list_array = dict_values
                    .as_any()
                    .downcast_ref::<ListArray>()
                    .expect("Expected ListArray");

                // Verify structure: 5 total lists
                assert_eq!(list_array.len(), 5, "Expected 5 lists total");

                // Verify first element is null (the null list)
                assert!(
                    list_array.is_null(0),
                    "Expected first element to be null list"
                );

                // Get the string values array
                let string_values = list_array.values();
                let string_view_array = string_values
                    .as_any()
                    .downcast_ref::<StringViewArray>()
                    .expect("Expected StringViewArray");

                // Verify content: ["c"], ["x","y"], ["a","b"], ["m","n"]
                // Total strings: 1 + 2 + 2 + 2 = 7
                assert_eq!(
                    string_view_array.len(),
                    7,
                    "Expected 7 total string elements"
                );
                assert_eq!(string_view_array.value(0), "c");
                assert_eq!(string_view_array.value(1), "x");
                assert_eq!(string_view_array.value(2), "y");
                assert_eq!(string_view_array.value(3), "a");
                assert_eq!(string_view_array.value(4), "b");
                assert_eq!(string_view_array.value(5), "m");
                assert_eq!(string_view_array.value(6), "n");
            }
        }

        #[test]
        fn test_intern_and_emit_intertwined() {
            // Test with Utf8
            {
                let mut group_vals =
                    GroupValuesDictionary::<Int32Type>::new(&DataType::Utf8);
                let mut groups = Vec::new();

                // Intern batch 1
                let (dict_array_1, _) =
                    create_utf8_dict_array(vec!["alice", "bob"], vec![Some(0), Some(1)]);
                group_vals
                    .intern(&[dict_array_1], &mut groups)
                    .expect("intern should succeed");
                assert_eq!(group_vals.len(), 2);
                assert_eq!(groups, vec![0, 1]);

                // Emit first
                let emitted_1 = group_vals
                    .emit(EmitTo::First(1))
                    .expect("first emit should succeed");
                assert_eq!(group_vals.len(), 1);
                assert_eq!(emitted_1.len(), 1);

                // Verify the first emitted value is "alice"
                let dict_array = emitted_1[0]
                    .as_any()
                    .downcast_ref::<DictionaryArray<Int32Type>>()
                    .expect("Expected DictionaryArray");
                assert_dict_array_values(dict_array, &[Some("alice")], &DataType::Utf8);

                // Intern batch 2
                groups.clear();
                let (dict_array_2, _) = create_utf8_dict_array(
                    vec!["alice", "bob", "charlie"],
                    vec![Some(1), Some(2)],
                );
                group_vals
                    .intern(&[dict_array_2], &mut groups)
                    .expect("second intern should succeed");
                // "bob" is already at group 0, "charlie" is new at group 1
                assert_eq!(group_vals.len(), 2);

                // Emit all
                let emitted_2 = group_vals
                    .emit(EmitTo::All)
                    .expect("second emit should succeed");
                assert_eq!(group_vals.len(), 0);

                // Verify the second emitted values contain "bob" and "charlie"
                assert_eq!(emitted_2.len(), 1);
                let dict_array = emitted_2[0]
                    .as_any()
                    .downcast_ref::<DictionaryArray<Int32Type>>()
                    .expect("Expected DictionaryArray");
                assert_dict_array_values(
                    dict_array,
                    &[Some("bob"), Some("charlie")],
                    &DataType::Utf8,
                );
            }

            // Test with List<LargeUtf8> with nulls intertwined
            {
                let mut group_vals =
                    GroupValuesDictionary::<Int32Type>::new(&DataType::List(Arc::new(
                        Field::new("item", DataType::LargeUtf8, true),
                    )));
                let mut groups = Vec::new();

                // Intern batch 1: [["a", "b"], ["c"]]
                let (dict_array_1, _) = create_list_large_utf8_dict_array(
                    vec![Some(vec!["a", "b"]), Some(vec!["c"])],
                    vec![Some(0), Some(1)],
                );
                group_vals
                    .intern(&[dict_array_1], &mut groups)
                    .expect("first intern should succeed");
                assert_eq!(group_vals.len(), 2);
                assert_eq!(groups, vec![0, 1]);

                // Emit first: should emit [["a", "b"]]
                let emitted_1 = group_vals
                    .emit(EmitTo::First(1))
                    .expect("first emit should succeed");
                assert_eq!(group_vals.len(), 1);
                assert_eq!(emitted_1.len(), 1);

                // Verify the emitted value is a DictionaryArray with ListArray values containing LargeStringArray
                let dict_array = emitted_1[0]
                    .as_any()
                    .downcast_ref::<DictionaryArray<Int32Type>>()
                    .expect("Expected DictionaryArray");
                let list_array = dict_array
                    .values()
                    .as_any()
                    .downcast_ref::<ListArray>()
                    .expect("Expected ListArray");
                assert_eq!(list_array.len(), 1); // One list element
                let values_array = list_array.values();
                let large_string_array = values_array
                    .as_any()
                    .downcast_ref::<LargeStringArray>()
                    .expect("Expected LargeStringArray");
                assert!(large_string_array.len() > 0); // Should have elements

                // Intern batch 2: [["a", "b"], ["c"], ["x", "y"], None]
                groups.clear();
                let (dict_array_2, _) = create_list_large_utf8_dict_array(
                    vec![
                        Some(vec!["a", "b"]),
                        Some(vec!["c"]),
                        Some(vec!["x", "y"]),
                        None,
                    ],
                    vec![Some(1), Some(2), Some(3)],
                );
                group_vals
                    .intern(&[dict_array_2], &mut groups)
                    .expect("second intern should succeed");
                // "c" is already at group 0, add "x", "y" and null list
                assert_eq!(group_vals.len(), 3);

                // Emit all remaining
                let emitted_2 = group_vals
                    .emit(EmitTo::All)
                    .expect("second emit should succeed");
                assert_eq!(group_vals.len(), 0);

                // Verify the second emitted values contain nested ListArray with LargeStringArray
                assert_eq!(emitted_2.len(), 1);
                let dict_array = emitted_2[0]
                    .as_any()
                    .downcast_ref::<DictionaryArray<Int32Type>>()
                    .expect("Expected DictionaryArray");
                let list_array = dict_array
                    .values()
                    .as_any()
                    .downcast_ref::<ListArray>()
                    .expect("Expected ListArray");
                assert_eq!(list_array.len(), 3); // Should have 3 elements: ["c"], ["x", "y"], null list
                let values_array = list_array.values();
                let large_string_array = values_array
                    .as_any()
                    .downcast_ref::<LargeStringArray>()
                    .expect("Expected LargeStringArray");
                // The values array should contain the strings from both ["c"] and ["x", "y"]
                assert!(large_string_array.len() >= 3); // At least "c", "x", "y"
            }
        }

        // * call emit with EmitTo::All() and validate that all contents are drained and emitted correctly + len is correct
        #[test]
        fn test_emit_all_drains_completely() {
            // Test with Utf8
            {
                let mut group_vals =
                    GroupValuesDictionary::<Int32Type>::new(&DataType::Utf8);
                let mut groups = Vec::new();

                // Intern 5 unique values
                let (dict_array, _) = create_utf8_dict_array(
                    vec!["a", "b", "c", "d", "e"],
                    vec![Some(0), Some(1), Some(2), Some(3), Some(4)],
                );
                group_vals
                    .intern(&[dict_array], &mut groups)
                    .expect("intern should succeed");
                assert_eq!(group_vals.len(), 5);

                // Emit all
                let emitted = group_vals.emit(EmitTo::All).expect("emit should succeed");

                // Verify all values were emitted
                assert_eq!(emitted.len(), 1);
                let dict_array = emitted[0]
                    .as_any()
                    .downcast_ref::<DictionaryArray<Int32Type>>()
                    .expect("Expected DictionaryArray");

                let values = dict_array.values();
                let value_array = values
                    .as_any()
                    .downcast_ref::<StringArray>()
                    .expect("Expected StringArray");
                assert_eq!(value_array.len(), 5);

                // Verify len is 0
                assert_eq!(group_vals.len(), 0);

                // Verify subsequent intern starts with fresh group ids
                groups.clear();
                let (dict_array_2, _) =
                    create_utf8_dict_array(vec!["a", "f"], vec![Some(0), Some(1)]);
                group_vals
                    .intern(&[dict_array_2], &mut groups)
                    .expect("subsequent intern should succeed");
                // Both "a" and "f" should get new group ids starting from 0
                assert_eq!(groups, vec![0, 1]);
                assert_eq!(group_vals.len(), 2);
            }

            // Test with List<Utf8View>
            {
                let mut group_vals =
                    GroupValuesDictionary::<Int32Type>::new(&DataType::List(Arc::new(
                        Field::new("item", DataType::Utf8View, true),
                    )));
                let mut groups = Vec::new();

                // Intern 5 unique lists
                let (dict_array, _) = create_list_utf8view_dict_array(
                    vec![
                        Some(vec!["a"]),
                        Some(vec!["b", "b"]),
                        Some(vec!["c", "c", "c"]),
                        Some(vec!["d", "d", "d", "d"]),
                        Some(vec!["e"]),
                    ],
                    vec![Some(0), Some(1), Some(2), Some(3), Some(4)],
                );
                group_vals
                    .intern(&[dict_array], &mut groups)
                    .expect("intern should succeed");
                assert_eq!(group_vals.len(), 5);
                assert_eq!(groups, vec![0, 1, 2, 3, 4]);

                // Emit all
                let emitted = group_vals.emit(EmitTo::All).expect("emit should succeed");

                // Verify structure and type
                assert_eq!(emitted.len(), 1);
                let dict_array = emitted[0]
                    .as_any()
                    .downcast_ref::<DictionaryArray<Int32Type>>()
                    .expect("Expected DictionaryArray");

                // Verify it's a ListArray containing StringViewArray
                let list_array = dict_array
                    .values()
                    .as_any()
                    .downcast_ref::<ListArray>()
                    .expect("Expected ListArray");
                assert_eq!(list_array.len(), 5); // All 5 lists emitted

                let values_array = list_array.values();
                let string_view_array = values_array
                    .as_any()
                    .downcast_ref::<StringViewArray>()
                    .expect("Expected StringViewArray");

                // Verify content is in correct order
                // The StringViewArray should contain all the string elements
                // ["a"], ["b", "b"], ["c", "c", "c"], ["d", "d", "d", "d"], ["e"]
                // = 1 + 2 + 3 + 4 + 1 = 11 elements total
                assert_eq!(string_view_array.len(), 11);

                // Verify individual elements in order
                assert_eq!(string_view_array.value(0), "a");
                assert_eq!(string_view_array.value(1), "b");
                assert_eq!(string_view_array.value(2), "b");
                assert_eq!(string_view_array.value(3), "c");
                assert_eq!(string_view_array.value(4), "c");
                assert_eq!(string_view_array.value(5), "c");
                assert_eq!(string_view_array.value(6), "d");
                assert_eq!(string_view_array.value(7), "d");
                assert_eq!(string_view_array.value(8), "d");
                assert_eq!(string_view_array.value(9), "d");
                assert_eq!(string_view_array.value(10), "e");

                // Verify len is 0
                assert_eq!(group_vals.len(), 0);

                // Verify subsequent intern starts with fresh group ids
                groups.clear();
                let (dict_array_2, _) = create_list_utf8view_dict_array(
                    vec![Some(vec!["a"]), Some(vec!["x", "y"])],
                    vec![Some(0), Some(1)],
                );
                group_vals
                    .intern(&[dict_array_2], &mut groups)
                    .expect("subsequent intern should succeed");
                // Both lists should get new group ids starting from 0
                assert_eq!(groups, vec![0, 1]);
                assert_eq!(group_vals.len(), 2);
            }
        }
    }

    #[cfg(test)]
    mod trivial_test {
        use super::*;

        #[test]
        fn test_edge_cases() {
            // Test 1: empty array - intern with empty dictionary array
            {
                let mut group_vals =
                    GroupValuesDictionary::<Int32Type>::new(&DataType::Utf8);
                let mut groups = Vec::new();

                let mut values_builder = StringBuilder::new();
                let values_array = Arc::new(values_builder.finish()) as ArrayRef;

                let mut keys_builder = PrimitiveBuilder::<Int32Type>::new();
                let keys_array = keys_builder.finish();

                let dict_array = DictionaryArray::<Int32Type>::try_new(
                    keys_array,
                    Arc::clone(&values_array),
                )
                .expect("Failed to create dictionary array");

                let result = group_vals.intern(&[Arc::new(dict_array)], &mut groups);
                assert!(result.is_ok(), "intern with empty array should succeed");
                assert_eq!(groups.len(), 0, "groups should be empty for empty array");
                assert_eq!(
                    group_vals.len(),
                    0,
                    "group_vals should be empty after interning empty array"
                );
            }

            // Test 2: multiple columns (2+) - intern should return error
            {
                let mut group_vals =
                    GroupValuesDictionary::<Int32Type>::new(&DataType::Utf8);
                let mut groups = Vec::new();

                let (dict_array_1, _) =
                    create_utf8_dict_array(vec!["alice", "bob"], vec![Some(0), Some(1)]);
                let (dict_array_2, _) =
                    create_utf8_dict_array(vec!["charlie"], vec![Some(0)]);

                let result =
                    group_vals.intern(&[dict_array_1, dict_array_2], &mut groups);
                assert!(
                    result.is_err(),
                    "intern with 2+ columns should return an error"
                );
            }

            // Test 3: array with all nulls
            {
                let mut group_vals =
                    GroupValuesDictionary::<Int32Type>::new(&DataType::Utf8);
                let mut groups = Vec::new();

                let (dict_array, _) =
                    create_utf8_dict_array(vec!["alice"], vec![None, None, None, None]);
                group_vals
                    .intern(&[dict_array], &mut groups)
                    .expect("intern with all nulls should succeed");

                // All nulls should map to a single null group
                assert_eq!(
                    group_vals.len(),
                    1,
                    "should have 1 group for all null values"
                );
                // All groups should be the same
                assert_eq!(groups[0], groups[1]);
                assert_eq!(groups[1], groups[2]);
                assert_eq!(groups[2], groups[3]);
            }

            // Test 4: array with all identical values
            {
                let mut group_vals =
                    GroupValuesDictionary::<Int32Type>::new(&DataType::Utf8);
                let mut groups = Vec::new();

                let (dict_array, _) = create_utf8_dict_array(
                    vec!["alice"],
                    vec![Some(0), Some(0), Some(0), Some(0)],
                );
                group_vals
                    .intern(&[dict_array], &mut groups)
                    .expect("intern with identical values should succeed");

                // All identical values should map to the same group
                assert_eq!(
                    group_vals.len(),
                    1,
                    "should have 1 group for all identical values"
                );
                // All groups should be the same
                assert_eq!(groups, vec![0, 0, 0, 0]);
            }

            // Test 5: emit with First(n) where n > number of seen elements
            {
                let mut group_vals =
                    GroupValuesDictionary::<Int32Type>::new(&DataType::Utf8);
                let mut groups = Vec::new();

                // Intern 3 unique values
                let (dict_array, _) = create_utf8_dict_array(
                    vec!["a", "b", "c"],
                    vec![Some(0), Some(1), Some(2)],
                );
                group_vals
                    .intern(&[dict_array], &mut groups)
                    .expect("intern should succeed");
                assert_eq!(group_vals.len(), 3, "should have 3 unique values");

                // Try to emit First(10) where 10 > 3 - should emit all 3
                let emitted = group_vals
                    .emit(EmitTo::First(10))
                    .expect("emit First(n) with n > len should succeed");

                // Should emit all remaining values
                assert_eq!(emitted.len(), 1);
                let dict_array = emitted[0]
                    .as_any()
                    .downcast_ref::<DictionaryArray<Int32Type>>()
                    .expect("Expected DictionaryArray");
                assert_dict_array_values(
                    dict_array,
                    &[Some("a"), Some("b"), Some("c")],
                    &DataType::Utf8,
                );
                // After emitting, all elements should be drained
                assert_eq!(
                    group_vals.len(),
                    0,
                    "all elements should be drained after emit First(n) with n > len"
                );
            }
        }
    }

    #[cfg(test)]
    mod state_management {
        use super::*;

        #[test]
        fn test_state_management() {
            // Test 1: len() for empty is correct
            {
                let group_vals = GroupValuesDictionary::<Int32Type>::new(&DataType::Utf8);
                assert_eq!(
                    group_vals.len(),
                    0,
                    "empty GroupValuesDictionary should have len 0"
                );
            }

            // Test 2: size and len increase as we intern more unique values
            {
                let mut group_vals =
                    GroupValuesDictionary::<Int32Type>::new(&DataType::Utf8);
                let mut groups = Vec::new();

                let initial_size = group_vals.size();
                let initial_len = group_vals.len();

                // Intern first batch
                let (dict_array_1, _) =
                    create_utf8_dict_array(vec!["alice", "bob"], vec![Some(0), Some(1)]);
                group_vals
                    .intern(&[dict_array_1], &mut groups)
                    .expect("intern should succeed");
                let size_after_1 = group_vals.size();
                let len_after_1 = group_vals.len();

                assert!(
                    size_after_1 > initial_size,
                    "size should increase after interning"
                );
                assert!(
                    len_after_1 > initial_len,
                    "len should increase after interning"
                );
                assert_eq!(len_after_1, 2, "should have 2 unique values");

                // Intern second batch with more values
                groups.clear();
                let (dict_array_2, _) = create_utf8_dict_array(
                    vec!["alice", "bob", "charlie", "david"],
                    vec![Some(0), Some(1), Some(2), Some(3)],
                );
                group_vals
                    .intern(&[dict_array_2], &mut groups)
                    .expect("intern should succeed");
                let size_after_2 = group_vals.size();
                let len_after_2 = group_vals.len();

                assert!(
                    size_after_2 > size_after_1,
                    "size should continue to increase"
                );
                assert!(len_after_2 > len_after_1, "len should continue to increase");
                assert_eq!(len_after_2, 4, "should have 4 unique values total");
            }

            // Test 3: len() after multiple intern calls is correct (List<Utf8>)
            {
                let mut group_vals = GroupValuesDictionary::<Int32Type>::new(
                    &DataType::List(Arc::new(Field::new("item", DataType::Utf8, true))),
                );
                let mut groups = Vec::new();

                // First intern: 3 unique lists
                let (dict_array_1, _) = create_list_utf8_dict_array(
                    vec![Some(vec!["a"]), Some(vec!["b"]), Some(vec!["c"])],
                    vec![Some(0), Some(1), Some(2)],
                );
                group_vals
                    .intern(&[dict_array_1], &mut groups)
                    .expect("first intern should succeed");
                assert_eq!(group_vals.len(), 3);

                // Second intern: 2 new lists
                groups.clear();
                let (dict_array_2, _) = create_list_utf8_dict_array(
                    vec![
                        Some(vec!["a"]),
                        Some(vec!["b"]),
                        Some(vec!["c"]),
                        Some(vec!["d"]),
                        Some(vec!["e"]),
                    ],
                    vec![Some(2), Some(3), Some(4)],
                );
                group_vals
                    .intern(&[dict_array_2], &mut groups)
                    .expect("second intern should succeed");
                assert_eq!(group_vals.len(), 5);

                // Third intern: all existing values, no new
                groups.clear();
                let (dict_array_3, _) = create_list_utf8_dict_array(
                    vec![
                        Some(vec!["a"]),
                        Some(vec!["b"]),
                        Some(vec!["c"]),
                        Some(vec!["d"]),
                        Some(vec!["e"]),
                    ],
                    vec![Some(0), Some(1)],
                );
                group_vals
                    .intern(&[dict_array_3], &mut groups)
                    .expect("third intern should succeed");
                assert_eq!(group_vals.len(), 5, "len should remain 5, no new values");
            }

            // Test 4: len() after intern + emit is correct
            {
                let mut group_vals =
                    GroupValuesDictionary::<Int32Type>::new(&DataType::Utf8);
                let mut groups = Vec::new();

                // Intern 5 unique values
                let (dict_array, _) = create_utf8_dict_array(
                    vec!["a", "b", "c", "d", "e"],
                    vec![Some(0), Some(1), Some(2), Some(3), Some(4)],
                );
                group_vals
                    .intern(&[dict_array], &mut groups)
                    .expect("intern should succeed");
                assert_eq!(group_vals.len(), 5);

                // Emit first 2
                let _ = group_vals
                    .emit(EmitTo::First(2))
                    .expect("first emit should succeed");
                assert_eq!(
                    group_vals.len(),
                    3,
                    "should have 3 remaining after emitting 2"
                );

                // Emit all remaining
                let _ = group_vals
                    .emit(EmitTo::All)
                    .expect("second emit should succeed");
                assert_eq!(group_vals.len(), 0, "should be empty after emitting all");
            }

            // Test 5: size() returns non-zero value that increases as we intern more (List<Utf8>)
            {
                let mut group_vals = GroupValuesDictionary::<Int32Type>::new(
                    &DataType::List(Arc::new(Field::new("item", DataType::Utf8, true))),
                );
                let mut groups = Vec::new();

                let initial_size = group_vals.size();

                // Intern first batch
                let (dict_array_1, _) = create_list_utf8_dict_array(
                    vec![
                        Some(vec!["alice"]),
                        Some(vec!["bob"]),
                        Some(vec!["charlie"]),
                    ],
                    vec![Some(0), Some(1), Some(2)],
                );
                group_vals
                    .intern(&[dict_array_1], &mut groups)
                    .expect("intern should succeed");
                let size_1 = group_vals.size();
                assert!(size_1 > 0, "size should be non-zero");
                assert!(size_1 > initial_size, "size should increase");

                // Intern second batch with more data
                groups.clear();
                let (dict_array_2, _) = create_list_utf8_dict_array(
                    vec![
                        Some(vec!["alice"]),
                        Some(vec!["bob"]),
                        Some(vec!["charlie"]),
                        Some(vec!["david"]),
                        Some(vec!["eve"]),
                        Some(vec!["frank"]),
                    ],
                    vec![Some(0), Some(1), Some(2), Some(3), Some(4), Some(5)],
                );
                group_vals
                    .intern(&[dict_array_2], &mut groups)
                    .expect("second intern should succeed");
                let size_2 = group_vals.size();
                assert!(size_2 > size_1, "size should continue to increase");
            }

            // Test 6: shrink behavior of clear_shrink with smaller num_rows
            {
                let mut group_vals =
                    GroupValuesDictionary::<Int32Type>::new(&DataType::Utf8);
                let mut groups = Vec::new();

                // Intern a large number of unique values
                let large_values: Vec<&str> = vec![
                    "v0", "v1", "v2", "v3", "v4", "v5", "v6", "v7", "v8", "v9", "v10",
                    "v11", "v12", "v13", "v14", "v15", "v16", "v17", "v18", "v19", "v20",
                    "v21", "v22", "v23", "v24", "v25", "v26", "v27", "v28", "v29", "v30",
                    "v31", "v32", "v33", "v34", "v35", "v36", "v37", "v38", "v39", "v40",
                    "v41", "v42", "v43", "v44", "v45", "v46", "v47", "v48", "v49",
                ];
                let large_keys: Vec<Option<usize>> = (0..50).map(Some).collect();

                let (dict_array, _) = create_utf8_dict_array(large_values, large_keys);
                group_vals
                    .intern(&[dict_array], &mut groups)
                    .expect("intern should succeed");
                assert_eq!(group_vals.len(), 50, "should have 50 unique values");
                let size_before_shrink = group_vals.size();

                // Clear and shrink with a smaller num_rows
                group_vals.clear_shrink(10);
                let size_after_shrink = group_vals.size();
                assert_eq!(group_vals.len(), 0, "len should be 0 after clear_shrink");
                assert!(
                    size_after_shrink < size_before_shrink,
                    "size should decrease after shrink"
                );
            }

            // Test 7: emit updates internal state correctly (List<Utf8>)
            {
                let mut group_vals = GroupValuesDictionary::<Int32Type>::new(
                    &DataType::List(Arc::new(Field::new("item", DataType::Utf8, true))),
                );
                let mut groups = Vec::new();

                // Intern lists with a null
                let (dict_array, _) = create_list_utf8_dict_array(
                    vec![Some(vec!["a"]), Some(vec!["b"])],
                    vec![Some(0), None, Some(1)],
                );
                group_vals
                    .intern(&[dict_array], &mut groups)
                    .expect("intern should succeed");
                assert_eq!(group_vals.len(), 3, "should have 2 lists + 1 null group");

                // Emit first list (non-null)
                let _ = group_vals
                    .emit(EmitTo::First(1))
                    .expect("first emit should succeed");
                assert_eq!(group_vals.len(), 2, "should have 2 remaining");

                // Intern again - ["a"] should get new group id since it was emitted
                groups.clear();
                let (dict_array_2, _) = create_list_utf8_dict_array(
                    vec![Some(vec!["a"]), Some(vec!["b"])],
                    vec![Some(0), Some(1)],
                );
                group_vals
                    .intern(&[dict_array_2], &mut groups)
                    .expect("second intern should succeed");
                // ["b"] was at group 0, ["a"] is new (since it was emitted), null remains
                // So we should have 3 groups still
                assert_eq!(group_vals.len(), 3, "state should be correctly updated");

                // Emit all and verify clean state
                let _ = group_vals
                    .emit(EmitTo::All)
                    .expect("final emit should succeed");
                assert_eq!(group_vals.len(), 0, "state should be completely cleared");
            }
        }
    }

    #[cfg(test)]
    mod null_test {
        use super::*;

        // 3.A: Null keys with Utf8
        #[test]
        fn test_null_keys_utf8() {
            let mut group_vals = GroupValuesDictionary::<Int32Type>::new(&DataType::Utf8);
            let mut groups = Vec::new();

            // Create dictionary with multiple null keys interspersed with valid keys
            let (dict_array, _) = create_utf8_dict_array(
                vec!["a", "b", "c"],
                vec![Some(0), None, Some(1), None, Some(2), None],
            );
            group_vals
                .intern(&[dict_array], &mut groups)
                .expect("intern should succeed");

            // Should have 3 unique values + 1 null group = 4 groups
            assert_eq!(group_vals.len(), 4);
            // All null keys should map to same group
            assert_eq!(groups[1], groups[3]);
            assert_eq!(groups[3], groups[5]);
            // Verify non-null keys map to different groups
            assert_ne!(groups[0], groups[2]);
            assert_ne!(groups[2], groups[4]);
        }

        // 3.B: Null values in the values array (Utf8)
        #[test]
        fn test_null_values_utf8() {
            let mut group_vals = GroupValuesDictionary::<Int32Type>::new(&DataType::Utf8);
            let mut groups = Vec::new();

            let mut values_builder = StringBuilder::new();
            values_builder.append_value("alice");
            values_builder.append_null(); // null value at index 1
            values_builder.append_value("bob");
            let values_array = Arc::new(values_builder.finish()) as ArrayRef;

            let mut keys_builder = PrimitiveBuilder::<Int32Type>::new();
            keys_builder.append_value(0i32); // points to "alice"
            keys_builder.append_value(1i32); // points to null value
            keys_builder.append_value(2i32); // points to "bob"
            keys_builder.append_value(1i32); // points to null value again
            let keys_array = keys_builder.finish();

            let dict_array = DictionaryArray::<Int32Type>::try_new(
                keys_array,
                Arc::clone(&values_array),
            )
            .expect("Failed to create dictionary array");

            group_vals
                .intern(&[Arc::new(dict_array)], &mut groups)
                .expect("intern should succeed");

            // Should have 2 unique meaningful values + 1 null value = 3 groups
            assert_eq!(group_vals.len(), 3);
            // Verify null value references get same group
            assert_eq!(groups[1], groups[3]);
        }

        // 3.C: Both null keys and null values
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

        // 3.D.1: Null values nested in List<Utf8> - null strings inside lists
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

        // 3.D.2: Null lists combined with null dictionary keys
        #[test]
        fn test_null_lists_with_null_keys() {
            let mut group_vals = GroupValuesDictionary::<Int32Type>::new(
                &DataType::List(Arc::new(Field::new("item", DataType::Utf8, true))),
            );
            let mut groups = Vec::new();
            // [["a"],None,None,["b"],None,None]  => [0,1,2]
            let (dict_array, _) = create_list_utf8_dict_array(
                vec![
                    Some(vec!["a"]),
                    None, // null list
                    Some(vec!["b"]),
                    None, // another null list
                ],
                vec![Some(0), None, Some(1), Some(2), None, Some(3)],
            );
            group_vals
                .intern(&[dict_array], &mut groups)
                .expect("intern should succeed");
            assert_eq!(group_vals.len(), 3);
            // All null keys should map to same group
            assert_eq!(groups[1], groups[4]);
            assert_eq!(groups, vec![0, 1, 1, 2, 1, 1])
        }

        // 3.D.3: Edge case - emit and re-intern with null values
        #[test]
        fn test_emit_and_reintern_with_nulls_list() {
            let mut group_vals = GroupValuesDictionary::<Int32Type>::new(
                &DataType::List(Arc::new(Field::new("item", DataType::Utf8, true))),
            );
            let mut groups = Vec::new();

            // Intern lists including nulls
            // [["x"],None,["y"]] => [0,1,2]
            let (dict_array_1, _) = create_list_utf8_dict_array(
                vec![Some(vec!["x"]), None, Some(vec!["y"])],
                vec![Some(0), Some(1), Some(2)],
            );
            group_vals
                .intern(&[dict_array_1], &mut groups)
                .expect("first intern should succeed");
            assert_eq!(group_vals.len(), 3);

            // Emit first group (the valid list ["x"])
            let emitted_1 = group_vals
                .emit(EmitTo::First(1))
                .expect("first emit should succeed");
            assert_eq!(group_vals.len(), 2);

            // Verify emitted data
            let dict_array = emitted_1[0]
                .as_any()
                .downcast_ref::<DictionaryArray<Int32Type>>()
                .expect("Expected DictionaryArray");
            let list_array = dict_array
                .values()
                .as_any()
                .downcast_ref::<ListArray>()
                .expect("Expected ListArray");
            assert_eq!(list_array.len(), 1);
            // (internally) [None,[y]]
            // Re-intern with same nulls - they should get new group ids
            groups.clear();
            let (dict_array_2, _) = create_list_utf8_dict_array(
                vec![Some(vec!["x"]), None, Some(vec!["z"])],
                vec![Some(0), Some(1), Some(2)],
            );
            group_vals
                .intern(&[dict_array_2], &mut groups)
                .expect("second intern should succeed");
            // (internally) [none,[y],[x],[z]]
            // ["x"] was emitted, so it's new (group 2)
            // null list maps to same null list group (group 0)
            // ["z"] is new (group 3)
            assert_eq!(group_vals.len(), 4);

            // Emit all and validate the structure
            let emitted_2 = group_vals
                .emit(EmitTo::All)
                .expect("second emit should succeed");
            assert_eq!(emitted_2.len(), 1);

            // Validate the emitted array structure: [null, ["y"], ["x"], ["z"]]
            let dict_array = emitted_2[0]
                .as_any()
                .downcast_ref::<DictionaryArray<Int32Type>>()
                .expect("Expected DictionaryArray");
            let list_array = dict_array
                .values()
                .as_any()
                .downcast_ref::<ListArray>()
                .expect("Expected ListArray");

            // Should have 4 lists: null, ["y"], ["x"], ["z"]
            assert_eq!(list_array.len(), 4);

            // First element should be null list
            assert!(list_array.is_null(0), "First element should be null list");

            // Get the string values from the list
            let string_values = list_array.values();
            let string_array = string_values
                .as_any()
                .downcast_ref::<StringArray>()
                .expect("Expected StringArray");

            // Should have 3 strings total: "y", "x", "z"
            assert_eq!(string_array.len(), 3);
            assert_eq!(string_array.value(0), "y");
            assert_eq!(string_array.value(1), "x");
            assert_eq!(string_array.value(2), "z");
        }

        // 3.D.4: Complex nested edge case - multiple emit/intern cycles with nulls
        // TODO: broken
        #[test]
        fn test_multiple_nulls_in_nested_structure() {
            let mut group_vals = GroupValuesDictionary::<Int32Type>::new(
                &DataType::List(Arc::new(Field::new("item", DataType::Utf8, true))),
            );
            let mut groups = Vec::new();

            // Intern batch 1: mix of nulls and valid lists
            // [["a","b"],None,None,["c"],None] => [0,1,1,2,1]
            let (dict_array_1, _) = create_list_utf8_dict_array(
                vec![Some(vec!["a", "b"]), None, Some(vec!["c"])],
                vec![
                    Some(0),
                    None, // null key
                    Some(1),
                    Some(2),
                    None, // null key
                ],
            );
            group_vals
                .intern(&[dict_array_1], &mut groups)
                .expect("first intern should succeed");
            assert_eq!(group_vals.len(), 3);

            // Emit first 2 groups to remove [["a","b"],Null] and null list
            let _ = group_vals
                .emit(EmitTo::First(2))
                .expect("first emit should succeed");
            assert_eq!(group_vals.len(), 1); // ["c"] remains
            let pre_new_batch = group_vals.len();

            // Intern batch 2: bring back nulls and add new data
            groups.clear();
            let (dict_array_2, _) = create_list_utf8_dict_array(
                vec![
                    Some(vec!["a", "b"]), // was emitted, so new
                    None,                 // was emitted, so new
                    Some(vec!["d"]),      // new list
                ],
                vec![
                    Some(0),
                    None, // null key
                    Some(1),
                    Some(2),
                ],
            );
            group_vals
                .intern(&[dict_array_2], &mut groups)
                .expect("second intern should succeed");
            // group count should increase by exactly 3  (["a","b"],Null,["d"]) are all new
            assert_eq!(pre_new_batch + 3, group_vals.len());
            // Should have: ["c"] (old) +  ["a","b"] (new), null (new), ["d"] (new)
            assert_eq!(group_vals.len(), 4);

            // Emit all and verify state is clean
            let emitted = group_vals
                .emit(EmitTo::All)
                .expect("final emit should succeed");
            assert_eq!(emitted.len(), 1);
            assert_eq!(group_vals.len(), 0);

            // Validate the emitted array structure: [["c"], ["a","b"], null, ["d"]]
            let dict_array = emitted[0]
                .as_any()
                .downcast_ref::<DictionaryArray<Int32Type>>()
                .expect("Expected DictionaryArray");
            let list_array = dict_array
                .values()
                .as_any()
                .downcast_ref::<ListArray>()
                .expect("Expected ListArray");

            // Should have 4 lists: ["c"], ["a","b"], null, ["d"]
            assert_eq!(list_array.len(), 4, "Should have 4 lists total");

            // Third element (index 2) should be null list
            assert!(list_array.is_null(2), "Third element should be null list");

            // Get the string values from the list
            let string_values = list_array.values();
            let string_array = string_values
                .as_any()
                .downcast_ref::<StringArray>()
                .expect("Expected StringArray");

            // Should have 4 strings total: "c", "a", "b", "d"
            assert_eq!(string_array.len(), 4, "Should have 4 string elements");
            assert_eq!(string_array.value(0), "c", "First string should be 'c'");
            assert_eq!(string_array.value(1), "a", "Second string should be 'a'");
            assert_eq!(string_array.value(2), "b", "Third string should be 'b'");
            assert_eq!(string_array.value(3), "d", "Fourth string should be 'd'");
        }

        // 3.D.5: All nulls in list with null keys
        #[test]
        fn test_all_nulls_keys_and_empty_lists() {
            let mut group_vals = GroupValuesDictionary::<Int32Type>::new(
                &DataType::List(Arc::new(Field::new("item", DataType::Utf8, true))),
            );
            let mut groups = Vec::new();

            // All dictionary keys are null
            let (dict_array, _) = create_list_utf8_dict_array(
                vec![Some(vec!["a"]), None],
                vec![None, None, None],
            );

            group_vals
                .intern(&[dict_array], &mut groups)
                .expect("intern should succeed");

            // Should have 2 unique list values + 1 null key group = 3 groups
            // But all 3 keys are null, so they all map to null key group
            assert_eq!(
                group_vals.len(),
                1,
                "should have 1 group since all keys are null"
            );
        }

        // 3.D.6: Empty list vs null list distinction
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
            // Null list should be different group
            // Non-empty list is another group
            assert_eq!(group_vals.len(), 3);
            assert_eq!(groups[0], groups[2]); // Both empty lists
            assert_ne!(groups[0], groups[1]); // Empty != null
            assert_ne!(groups[1], groups[3]); // Null != non-empty
        }

        // 3.D.7: Null values in values array (Utf8)
        #[test]
        fn test_null_values_in_values_array() {
            let mut group_vals = GroupValuesDictionary::<Int32Type>::new(&DataType::Utf8);

            let mut values_builder = StringBuilder::new();
            values_builder.append_null(); // index 0: null
            values_builder.append_value("abc"); // index 1: "abc"
            values_builder.append_value("def"); // index 2: "def"
            values_builder.append_null(); // index 3: null
            let values_array = Arc::new(values_builder.finish()) as ArrayRef;

            let mut keys_builder = PrimitiveBuilder::<Int32Type>::new();
            keys_builder.append_value(0i32); // points to null at index 0
            keys_builder.append_value(1i32); // points to "abc"
            keys_builder.append_value(2i32); // points to "def"
            keys_builder.append_value(0i32); // points to null at index 0
            keys_builder.append_value(1i32); // points to "abc"
            keys_builder.append_value(3i32); // points to null at index 3
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

            // Should have 3 groups: null, "abc", "def"
            assert_eq!(group_vals.len(), 3);
            // Verify null keys map to same group
            assert_eq!(groups[0], groups[3]);
            assert_eq!(groups[0], groups[5]);
            // Verify non-null keys map correctly
            assert_eq!(groups[1], groups[4]); // both "abc"
            assert_ne!(groups[1], groups[2]); // "abc" != "def"

            // Emit and verify output
            let result = group_vals.emit(EmitTo::All).expect("emit should succeed");
            assert_eq!(result.len(), 1);

            let emitted = result[0]
                .as_any()
                .downcast_ref::<DictionaryArray<Int32Type>>()
                .expect("Expected DictionaryArray");

            // Should have 3 entries in values array
            assert_eq!(emitted.values().len(), 3);

            // Verify the values
            let string_values = emitted
                .values()
                .as_any()
                .downcast_ref::<StringArray>()
                .expect("Expected StringArray");
            assert!(string_values.is_null(groups[0])); // null group
            assert_eq!(string_values.value(groups[1]), "abc");
            assert_eq!(string_values.value(groups[2]), "def");

            // State should be empty after emit
            assert!(group_vals.is_empty());
        }

        // 3.D.8: Null group stable across batches with reordered dict (List<LargeUtf8>)
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

            // All values are new since state was cleared
            // Should have 3 groups again
            assert_eq!(group_vals.len(), 3);
            assert_eq!(groups, vec![0, 1, 2, 1]); // null group should be stable (group 1)
            // Verify null keys still map to same group
            assert_eq!(groups[1], groups[3]);
        }

        // 3.D.9: Combined null keys and null values in dictionary (Utf8)
        #[test]
        fn test_null_keys_and_values_in_dictionary() {
            let mut group_vals = GroupValuesDictionary::<Int32Type>::new(&DataType::Utf8);

            // Create values with some nulls
            // ["a", null, "b"]
            let mut values_builder = StringBuilder::new();
            values_builder.append_value("a"); // index 0
            values_builder.append_null(); // index 1
            values_builder.append_value("b"); // index 2
            let values_array = Arc::new(values_builder.finish()) as ArrayRef;

            // Create keys with some nulls: [0, 1, 2, 1, 0, null, null] => ["a", null, "b", null, "a", null, null]
            let mut keys_builder = PrimitiveBuilder::<Int32Type>::new();
            keys_builder.append_value(0i32);
            keys_builder.append_value(1i32);
            keys_builder.append_value(2i32);
            keys_builder.append_value(1i32);
            keys_builder.append_value(0i32);
            keys_builder.append_null();
            keys_builder.append_null();
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

            assert_eq!(group_vals.len(), 3);
            // Rows pointing to null value (index 1 and 3) should map to same group
            assert_eq!(groups[1], groups[3]);
            // Null keys (index 5 and 6) should map to same group
            assert_eq!(groups[5], groups[6]);
            // Non-null rows should map correctly
            assert_eq!(groups[0], groups[4]); // both "a"
            assert_ne!(groups[0], groups[2]); // "a" != "b"

            // Emit all to reset state
            let emitted = group_vals.emit(EmitTo::All).expect("emit should succeed");
            assert_eq!(emitted.len(), 1);
            assert_eq!(group_vals.len(), 0);

            // Second batch with different structure
            groups.clear();
            let mut values_builder_2 = StringBuilder::new();
            values_builder_2.append_value("x");
            values_builder_2.append_null();
            let values_array_2 = Arc::new(values_builder_2.finish()) as ArrayRef;

            let mut keys_builder_2 = PrimitiveBuilder::<Int32Type>::new();
            keys_builder_2.append_value(0i32);
            keys_builder_2.append_null();
            keys_builder_2.append_value(1i32);
            let keys_array_2 = keys_builder_2.finish();

            let dict_array_2 =
                DictionaryArray::<Int32Type>::try_new(keys_array_2, values_array_2)
                    .expect("Failed to create dictionary array");

            group_vals
                .intern(&[Arc::new(dict_array_2)], &mut groups)
                .expect("second intern should succeed");

            assert_eq!(group_vals.len(), 2);
            assert_eq!(groups[1], groups[2]);
        }
    }
    #[cfg(test)]
    mod data_correctness {
        use super::*;

        // Regression test for COUNT DISTINCT with mixed null and non-null dictionary values (Utf8)
        #[test]
        fn test_count_distinct_mixed_nulls_utf8() {
            let mut group_vals = GroupValuesDictionary::<Int32Type>::new(&DataType::Utf8);

            let mut values_builder = StringBuilder::new();
            values_builder.append_null(); // index 0
            values_builder.append_value("abc"); // index 1
            values_builder.append_value("def"); // index 2
            values_builder.append_null(); // index 3
            let values_array = Arc::new(values_builder.finish()) as ArrayRef;

            let mut keys_builder = PrimitiveBuilder::<Int32Type>::new();
            keys_builder.append_value(0i32);
            keys_builder.append_value(1i32);
            keys_builder.append_value(2i32);
            keys_builder.append_value(0i32);
            keys_builder.append_value(1i32);
            keys_builder.append_value(3i32);
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

            // 3 groups: null, "abc", "def"
            assert_eq!(group_vals.len(), 3);
            assert_eq!(groups.len(), 6);

            // null group - rows 0, 3, 5 all map to same group
            assert_eq!(groups[0], groups[3]);
            assert_eq!(groups[0], groups[5]);
            // "abc" group - rows 1 and 4
            assert_eq!(groups[1], groups[4]);
            // all three groups are distinct
            assert_ne!(groups[0], groups[1]);
            assert_ne!(groups[1], groups[2]);
            assert_ne!(groups[0], groups[2]);

            // emit and verify null is correctly represented
            let result = group_vals.emit(EmitTo::All).expect("emit should succeed");
            assert_eq!(result.len(), 1);

            let emitted = result[0]
                .as_any()
                .downcast_ref::<DictionaryArray<Int32Type>>()
                .expect("Expected DictionaryArray");

            let string_values = emitted
                .values()
                .as_any()
                .downcast_ref::<StringArray>()
                .expect("Expected StringArray");

            // null group should have null value
            let null_key = emitted.keys().value(groups[0]);
            assert!(string_values.is_null(null_key as usize));

            // non-null groups should have non-null values
            let abc_key = emitted.keys().value(groups[1]);
            assert!(!string_values.is_null(abc_key as usize));
            assert_eq!(string_values.value(abc_key as usize), "abc");

            let def_key = emitted.keys().value(groups[2]);
            assert!(!string_values.is_null(def_key as usize));
            assert_eq!(string_values.value(def_key as usize), "def");

            assert!(group_vals.is_empty());
        }

        // Regression test for COUNT DISTINCT with mixed null and non-null dictionary values (List<Utf8>)
        #[test]
        fn test_count_distinct_mixed_nulls_list() {
            let mut group_vals = GroupValuesDictionary::<Int32Type>::new(
                &DataType::List(Arc::new(Field::new("item", DataType::Utf8, true))),
            );
            let mut groups = Vec::new();

            let (dict_array, _) = create_list_utf8_dict_array(
                vec![None, Some(vec!["abc"]), Some(vec!["def"]), None],
                vec![Some(0), Some(1), Some(2), Some(0), Some(1), Some(3)],
            );

            group_vals
                .intern(&[dict_array], &mut groups)
                .expect("intern should succeed");

            // 3 groups: null, ["abc"], ["def"]
            assert_eq!(group_vals.len(), 3);
            assert_eq!(groups.len(), 6);

            // null group - rows 0, 3, 5 all map to same group
            assert_eq!(groups[0], groups[3]);
            assert_eq!(groups[0], groups[5]);
            // ["abc"] group - rows 1 and 4
            assert_eq!(groups[1], groups[4]);
            // all three groups are distinct
            assert_ne!(groups[0], groups[1]);
            assert_ne!(groups[1], groups[2]);

            let result = group_vals.emit(EmitTo::All).expect("emit should succeed");
            assert_eq!(result.len(), 1);

            let emitted = result[0]
                .as_any()
                .downcast_ref::<DictionaryArray<Int32Type>>()
                .expect("Expected DictionaryArray");

            let list_array = emitted
                .values()
                .as_any()
                .downcast_ref::<ListArray>()
                .expect("Expected ListArray");

            // null group should have null list
            let null_key = emitted.keys().value(groups[0]);
            assert!(list_array.is_null(null_key as usize));

            assert!(group_vals.is_empty());
        }

        // Regression test for GROUP BY with null keys in dictionary (Utf8)
        #[test]
        fn test_group_by_null_keys_utf8() {
            let mut group_vals = GroupValuesDictionary::<Int32Type>::new(&DataType::Utf8);

            let mut values_builder = StringBuilder::new();
            values_builder.append_value("group_a");
            values_builder.append_value("group_b");
            let values_array = Arc::new(values_builder.finish()) as ArrayRef;

            let mut keys_builder = PrimitiveBuilder::<Int32Type>::new();
            keys_builder.append_value(0i32);
            keys_builder.append_null();
            keys_builder.append_value(1i32);
            keys_builder.append_null();
            keys_builder.append_value(0i32);
            let keys_array = keys_builder.finish();
            // values: ["group_a", "group_b"] , keys: [0, null, 1, null, 0] => ["group_a", null, "group_b", null, "group_a"] => groups: [0, 1, 2, 1, 0]
            let dict_array = DictionaryArray::<Int32Type>::try_new(
                keys_array,
                Arc::clone(&values_array),
            )
            .expect("Failed to create dictionary array");

            let mut groups = Vec::new();
            group_vals
                .intern(&[Arc::new(dict_array)], &mut groups)
                .expect("intern should succeed");

            // 3 groups: "group_a", "group_b", null
            assert_eq!(group_vals.len(), 3);
            assert_eq!(groups.len(), 5);

            // null keys map to same group
            assert_eq!(groups[1], groups[3]);
            // "group_a" rows map to same group
            assert_eq!(groups[0], groups[4]);
            // all three groups are distinct
            assert_ne!(groups[0], groups[1]);
            assert_ne!(groups[0], groups[2]);
            assert_ne!(groups[1], groups[2]);

            let result = group_vals.emit(EmitTo::All).expect("emit should succeed");
            assert!(group_vals.is_empty());
            assert_eq!(result.len(), 1);

            let emitted = result[0]
                .as_any()
                .downcast_ref::<DictionaryArray<Int32Type>>()
                .expect("Expected DictionaryArray");
            let string_values = emitted
                .values()
                .as_any()
                .downcast_ref::<StringArray>()
                .expect("Expected StringArray");

            // ["group_a", null, "group_b"] - order in which they should be emitted
            assert_eq!(string_values.len(), 3);
            assert_eq!(string_values.value(0), "group_a");
            assert!(string_values.is_null(1));
            assert_eq!(string_values.value(2), "group_b");

            assert!(group_vals.is_empty());
        }

        // Regression test for GROUP BY with null keys in dictionary (List<Utf8>)
        #[test]
        fn test_group_by_null_keys_list() {
            let mut group_vals = GroupValuesDictionary::<Int32Type>::new(
                &DataType::List(Arc::new(Field::new("item", DataType::Utf8, true))),
            );
            let mut groups = Vec::new();
            // values: [["group_a"], ["group_b"]], keys: [0, null, 1, null, 0] => [["group_a"], null, ["group_b"], null, ["group_a"]] => groups: [0, 1, 2, 1, 0]
            let (dict_array, _) = create_list_utf8_dict_array(
                vec![Some(vec!["group_a"]), Some(vec!["group_b"])],
                vec![Some(0), None, Some(1), None, Some(0)],
            );

            group_vals
                .intern(&[dict_array], &mut groups)
                .expect("intern should succeed");

            // 3 groups: ["group_a"], ["group_b"], null
            assert_eq!(group_vals.len(), 3);

            // null keys map to same group
            assert_eq!(groups[1], groups[3]);
            // ["group_a"] rows map to same group
            assert_eq!(groups[0], groups[4]);

            let result = group_vals.emit(EmitTo::All).expect("emit should succeed");
            assert_eq!(result.len(), 1);

            let emitted = result[0]
                .as_any()
                .downcast_ref::<DictionaryArray<Int32Type>>()
                .expect("Expected DictionaryArray");
            let list_array = emitted
                .values()
                .as_any()
                .downcast_ref::<ListArray>()
                .expect("Expected ListArray");

            // [["group_a"], null, ["group_b"]] - order in which they should be emitted
            assert_eq!(list_array.len(), 3);

            // Validate index 0: ["group_a"]
            assert!(!list_array.is_null(0));
            let list_0 = list_array.value(0);
            let strings_0 = list_0
                .as_any()
                .downcast_ref::<StringArray>()
                .expect("Expected StringArray in list 0");
            assert_eq!(strings_0.value(0), "group_a");

            // Validate index 1: null
            assert!(list_array.is_null(1));

            // Validate index 2: ["group_b"]
            assert!(!list_array.is_null(2));
            let list_2 = list_array.value(2);
            let strings_2 = list_2
                .as_any()
                .downcast_ref::<StringArray>()
                .expect("Expected StringArray in list 2");
            assert_eq!(strings_2.value(0), "group_b");

            assert!(group_vals.is_empty());
        }

        // Regression test for GROUP BY with null values in dictionary values array (Utf8)
        #[test]
        fn test_group_by_null_values_in_dict_utf8() {
            let mut group_vals = GroupValuesDictionary::<Int32Type>::new(&DataType::Utf8);

            let mut values_builder = StringBuilder::new();
            values_builder.append_value("val_x");
            values_builder.append_null();
            values_builder.append_value("val_y");
            let values_array = Arc::new(values_builder.finish()) as ArrayRef;

            let mut keys_builder = PrimitiveBuilder::<Int32Type>::new();
            keys_builder.append_value(0i32);
            keys_builder.append_value(1i32);
            keys_builder.append_value(2i32);
            keys_builder.append_value(1i32);
            keys_builder.append_value(0i32);
            let keys_array = keys_builder.finish();
            // values: ["val_x", null, "val_y"], keys: [0, 1, 2, 1, 0] => ["val_x", null, "val_y", null, "val_x"] => groups: [0, 1, 2, 1, 0]
            let dict_array = DictionaryArray::<Int32Type>::try_new(
                keys_array,
                Arc::clone(&values_array),
            )
            .expect("Failed to create dictionary array");

            let mut groups = Vec::new();
            group_vals
                .intern(&[Arc::new(dict_array)], &mut groups)
                .expect("intern should succeed");

            // 3 groups: "val_x", null, "val_y"
            assert_eq!(group_vals.len(), 3);

            // rows pointing to null value map to same group
            assert_eq!(groups[1], groups[3]);
            // "val_x" rows map to same group
            assert_eq!(groups[0], groups[4]);
            // all three groups are distinct
            assert_ne!(groups[0], groups[1]);
            assert_ne!(groups[1], groups[2]);

            let result = group_vals.emit(EmitTo::All).expect("emit should succeed");
            assert_eq!(result.len(), 1);

            let emitted = result[0]
                .as_any()
                .downcast_ref::<DictionaryArray<Int32Type>>()
                .expect("Expected DictionaryArray");

            let string_values = emitted
                .values()
                .as_any()
                .downcast_ref::<StringArray>()
                .expect("Expected StringArray");

            // ["val_x", null, "val_y"] - order in which they should be emitted
            assert_eq!(string_values.len(), 3);
            assert_eq!(string_values.value(0), "val_x");
            assert!(string_values.is_null(1));
            assert_eq!(string_values.value(2), "val_y");

            assert!(group_vals.is_empty());
        }

        // Regression test for GROUP BY with null values in dictionary values array (List<LargeUtf8>)
        #[test]
        fn test_group_by_null_values_in_dict_list() {
            let mut group_vals = GroupValuesDictionary::<Int32Type>::new(
                &DataType::List(Arc::new(Field::new("item", DataType::LargeUtf8, true))),
            );
            let mut groups = Vec::new();

            let (dict_array, _) = create_list_large_utf8_dict_array(
                vec![Some(vec!["val_x"]), None, Some(vec!["val_y"])],
                vec![Some(0), Some(1), Some(2), Some(1), Some(0)],
            );
            // values: [["val_x"], null, ["val_y"]], keys: [0, 1, 2, 1, 0] => [["val_x"], null, ["val_y"], null, ["val_x"]] => groups: [0, 1, 2, 1, 0]
            group_vals
                .intern(&[dict_array], &mut groups)
                .expect("intern should succeed");

            // 3 groups: ["val_x"], null, ["val_y"]
            assert_eq!(group_vals.len(), 3);

            // rows pointing to null value map to same group
            assert_eq!(groups[1], groups[3]);
            // ["val_x"] rows map to same group
            assert_eq!(groups[0], groups[4]);

            let result = group_vals.emit(EmitTo::All).expect("emit should succeed");
            assert_eq!(result.len(), 1);

            let emitted = result[0]
                .as_any()
                .downcast_ref::<DictionaryArray<Int32Type>>()
                .expect("Expected DictionaryArray");

            let list_array = emitted
                .values()
                .as_any()
                .downcast_ref::<ListArray>()
                .expect("Expected ListArray");

            // [["val_x"], null, ["val_y"]] - order in which they should be emitted
            assert_eq!(list_array.len(), 3);
            assert!(!list_array.is_null(0));
            assert!(list_array.is_null(1));
            assert!(!list_array.is_null(2));

            assert!(group_vals.is_empty());
        }

        // Advanced scenario: Non-canonicalized dictionary arrays
        // Test that intern with a dictionary array where values aren't canonicalized works correctly
        // Validates that normalize_dict_array and get_raw_bytes handle raw byte representation correctly
        // for different data types including nested types like List of strings
        #[test]
        fn test_non_canonicalized_dict_array_utf8() {
            let mut group_vals = GroupValuesDictionary::<Int32Type>::new(&DataType::Utf8);

            // Create a non-canonicalized dictionary where keys don't start at 0
            // and are not in order: keys are [2, 0, 1, 2, 1]
            let mut values_builder = StringBuilder::new();
            values_builder.append_value("alpha");
            values_builder.append_value("beta");
            values_builder.append_value("gamma");
            let values_array = Arc::new(values_builder.finish()) as ArrayRef;

            let mut keys_builder = PrimitiveBuilder::<Int32Type>::new();
            keys_builder.append_value(2i32); // gamma
            keys_builder.append_value(0i32); // alpha
            keys_builder.append_value(1i32); // beta
            keys_builder.append_value(2i32); // gamma
            keys_builder.append_value(1i32); // beta
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

            // Should have 3 unique groups: alpha, beta, gamma
            assert_eq!(group_vals.len(), 3);
            assert_eq!(groups.len(), 5);

            // gamma appears at rows 0 and 3
            assert_eq!(groups[0], groups[3]);
            // alpha appears only at row 1
            // beta appears at rows 2 and 4
            assert_eq!(groups[2], groups[4]);
            // all three should be distinct
            assert_ne!(groups[0], groups[1]);
            assert_ne!(groups[0], groups[2]);
            assert_ne!(groups[1], groups[2]);

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

            // Verify emitted values are correct
            let values_set: std::collections::HashSet<String> = (0..string_values.len())
                .filter_map(|i| {
                    if string_values.is_null(i) {
                        None
                    } else {
                        Some(string_values.value(i).to_string())
                    }
                })
                .collect();

            assert_eq!(values_set.len(), 3);
            assert!(values_set.contains("alpha"));
            assert!(values_set.contains("beta"));
            assert!(values_set.contains("gamma"));

            assert!(group_vals.is_empty());
        }

        // Advanced scenario: Non-canonicalized dictionary arrays with List<Utf8>
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

        // Advanced scenario: Duplicate values in values array
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

        // Advanced scenario: Duplicate values in values array with List<Utf8>
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

        // Advanced scenario: Null key sequence validation
        #[test]
        fn test_null_key_sequence_partial_emit_utf8() {
            let mut group_vals = GroupValuesDictionary::<Int32Type>::new(&DataType::Utf8);

            // First batch: keys with null
            let mut values_builder = StringBuilder::new();
            values_builder.append_value("value_a");
            values_builder.append_value("value_b");
            let values_array = Arc::new(values_builder.finish()) as ArrayRef;

            let mut keys_builder = PrimitiveBuilder::<Int32Type>::new();
            keys_builder.append_value(0i32); // "value_a"
            keys_builder.append_null(); // NULL
            keys_builder.append_value(1i32); // "value_b"
            keys_builder.append_null(); // NULL
            let keys_array = keys_builder.finish();
            // keys: [0,null,1,null] values: ["value_a", "value_b"] | logically [ "value_a", null, "value_b", null] | groups: [0,1,2,1] where group 1 is null group
            let dict_array = DictionaryArray::<Int32Type>::try_new(
                keys_array,
                Arc::clone(&values_array),
            )
            .expect("Failed to create dictionary array");

            let mut groups = Vec::new();
            group_vals
                .intern(&[Arc::new(dict_array)], &mut groups)
                .expect("intern should succeed");

            // Should have 3 groups: "value_a", "value_b", null
            assert_eq!(group_vals.len(), 3);

            // Remember the null groups
            assert_eq!(groups[1], groups[3]);

            // Emit only first 2 groups, which are value_a and null
            let result = group_vals
                .emit(EmitTo::First(2))
                .expect("emit should succeed");
            assert!(!result.is_empty());

            // After partial emit, length depends on how many were emitted
            // but null key should still be in the system if it wasn't emitted
            let len_after_partial = group_vals.len();

            // Now intern the null key again in a second batch
            let mut values_builder2 = StringBuilder::new();
            values_builder2.append_value("value_c");
            values_builder2.append_null(); // NULL, was  emitted so it should be treated as a new group
            let values_array2 = Arc::new(values_builder2.finish()) as ArrayRef;

            let mut keys_builder2 = PrimitiveBuilder::<Int32Type>::new();
            keys_builder2.append_value(0i32); // "value_c"
            keys_builder2.append_null(); // NULL again
            let keys_array2 = keys_builder2.finish();

            let dict_array2 = DictionaryArray::<Int32Type>::try_new(
                keys_array2,
                Arc::clone(&values_array2),
            )
            .expect("Failed to create dictionary array");

            let mut groups2 = Vec::new();
            group_vals
                .intern(&[Arc::new(dict_array2)], &mut groups2)
                .expect("intern should succeed");

            assert_eq!(group_vals.len(), len_after_partial + 2); // should add 2 new groups: "value_c" and null

            let result_final = group_vals.emit(EmitTo::All).expect("emit should succeed");
            assert!(!result_final.is_empty());

            let emitted_final = result_final[0]
                .as_any()
                .downcast_ref::<DictionaryArray<Int32Type>>()
                .expect("Expected DictionaryArray");

            let string_values_final = emitted_final
                .values()
                .as_any()
                .downcast_ref::<StringArray>()
                .expect("Expected StringArray");

            // Validate that output contains value_b, value_c, null in correct order
            assert_eq!(string_values_final.len(), 3);
            assert_eq!(string_values_final.value(0), "value_b");
            assert_eq!(string_values_final.value(1), "value_c");
            assert!(string_values_final.is_null(2));

            assert!(group_vals.is_empty());
        }

        // Advanced scenario: Null key sequence with List<Utf8>
        #[test]
        fn test_null_key_sequence_partial_emit_list() {
            let mut group_vals = GroupValuesDictionary::<Int32Type>::new(
                &DataType::List(Arc::new(Field::new("item", DataType::Utf8, true))),
            );

            // First batch with null key
            let (dict_array, _) = create_list_utf8_dict_array(
                vec![Some(vec!["value_a"]), Some(vec!["value_b"])],
                vec![Some(0), None, Some(1), None],
            );

            let mut groups = Vec::new();
            group_vals
                .intern(&[dict_array], &mut groups)
                .expect("intern should succeed");

            assert_eq!(group_vals.len(), 3); // 2 values + 1 null

            let null_group_id_first = groups[1];

            // Partial emit
            let result = group_vals
                .emit(EmitTo::First(2))
                .expect("emit should succeed");
            assert!(!result.is_empty());

            let len_after_partial = group_vals.len();

            // Second batch with same null key
            let (dict_array2, _) =
                create_list_utf8_dict_array(vec![Some(vec!["value_c"])], vec![None]);

            let mut groups2 = Vec::new();
            group_vals
                .intern(&[dict_array2], &mut groups2)
                .expect("intern should succeed");

            // If null wasn't fully emitted, it should reuse the group id
            if len_after_partial > 0 {
                assert_eq!(groups2[0], null_group_id_first);
            }

            let result_final = group_vals.emit(EmitTo::All).expect("emit should succeed");
            assert!(!result_final.is_empty());
            assert!(group_vals.is_empty());
        }
    }
}
