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

pub struct GroupValuesDictionary<K: ArrowDictionaryKeyType + Send> {
    // stores the order new unique elements are seen for self.emit()
    seen_elements: Vec<Option<Vec<u8>>>, //  Box<dyn Builder> doesnt provide the flexibility of building partition arrays that wed need to support emit::First(N)
    value_dt: DataType,
    _phantom: PhantomData<K>,
    // keeps track of which values weve already seen. stored as -> <unique_value_hash:(initial_group_id, raw_bytes)>
    unique_dict_value_mapping: HashMap<u64, Vec<(usize, Option<Vec<u8>>)>>,
    // fixed seeds ensure consistent hashing across GroupValuesDictionary instances
    // this is critical for correct behavior in multi-partition aggregation where
    // partial phase emits are re-interned by the final phase
    random_state: RandomState,
    // cached components
    //
    null_group_id: Option<usize>, // cache the group id for nulls since they all map to the same group
    intern_called: bool, // tracks if intern has ever been called. this is used to determine if we can skip phaase 1 of of intern.
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
        let (values, keys_as_usize) = Self::normalize_dict_array(values, key_array);
        let values = &values;
        // compute hashes for all values in the values array upfront
        // value_hashes[i] corresponds to values[i]
        let value_hashes = self.compute_value_hashes(values)?;

        // convert key array to Vec<usize> for cheap indexed access
        // avoids repeated .value(i).to_usize() calls in the hot loop
        // TODO: add a field to Self that keeps track of if this is the first iteration, pass 1 just
        // waste cpu on the first iteration if no insertions have already been made.

        // Pass 1: iterate values array (d iterations) - build a mapping of value hash -> group id for all unique values in the dictionary
        // this allows us to do a single hashmap lookup per key in the hot loop instead of
        let mut key_to_group: Vec<Option<usize>> = vec![None; values.len()];
        if !self.intern_called {
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
                    /* we can safely unwrap here because of the condition 9 lines above. if the value is even null its skipped and handled in phase 2*/
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
        Ok(())
    }
    fn emit(&mut self, emit_to: EmitTo) -> Result<Vec<ArrayRef>> {
        let (elements_to_emit, null_id) = match emit_to {
            EmitTo::All => {
                let original_null_id = self.null_group_id.clone();
                self.null_group_id = None;
                self.unique_dict_value_mapping.clear();
                (std::mem::take(&mut self.seen_elements), original_null_id)
            }
            EmitTo::First(n) => {
                let first_n = self.seen_elements.drain(..n).collect::<Vec<_>>();
                let orignal_null_id = self.null_group_id.filter(|&id| id < n);
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
                (first_n, orignal_null_id)
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
