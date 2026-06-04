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
    Array, ArrayRef, AsArray, DictionaryArray, LargeStringArray, LargeStringBuilder,
    ListArray, ListBuilder, PrimitiveArray, PrimitiveBuilder, StringArray, StringBuilder,
    StringViewArray, StringViewBuilder,
};
use arrow::datatypes::{ArrowDictionaryKeyType, ArrowNativeType, DataType};
use datafusion_common::DataFusionError::{Internal, NotImplemented};
use datafusion_common::Result;
use datafusion_common::hash_utils::create_hashes;
use datafusion_expr::EmitTo;
use hashbrown::HashTable;
use hashbrown::hash_table::Entry as HashTableEntry;
use std::borrow::Cow;
use std::marker::PhantomData;
use std::sync::Arc;

/// Heuristic for sizing the values buffer of string builders during emit:
/// dictionary-encoded values are short by design (categorical strings, short
/// identifiers), so 16 B/item avoids the realloc-doubling chain in the common
/// case while keeping over-allocation cheap when values are smaller.
const AVG_BYTES_PER_DICT_VALUE: usize = 16;
const INITIAL_PRE_ALLOCATION: usize = 8 * 1024; // avoid re-allocation`s for small-medium groups

macro_rules! decode_list {
    ($raw:expr, $builder:expr) => {{
        let mut builder = $builder;
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
                            let s = unsafe {
                                std::str::from_utf8_unchecked(
                                    &raw_vector[offset..offset + len as usize],
                                )
                            };
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
    ($raw:expr, $builder:expr) => {{
        let mut builder = $builder;
        for raw_bytes in $raw {
            match raw_bytes {
                Some(raw_vector) => {
                    let s = unsafe { std::str::from_utf8_unchecked(raw_vector) };
                    builder.append_value(s);
                }
                None => builder.append_null(),
            }
        }
        Ok(Arc::new(builder.finish()) as ArrayRef)
    }};
}
/// Entry stored inside `unique_dict_value_mapping`. Caching `hash` on the entry
struct DictEntry {
    hash: u64,
    group_id: usize,
    bytes: Vec<u8>,
}

pub struct GroupValuesDictionary<K: ArrowDictionaryKeyType + Send> {
    // stores the order new unique elements are seen for self.emit()
    seen_elements: Vec<Option<Vec<u8>>>,
    value_dt: DataType,
    _phantom: PhantomData<K>,
    // keeps track of which values weve already seen, keyed by raw value hash.
    unique_dict_value_mapping: HashTable<DictEntry>,

    random_state: RandomState,

    // cache the group id for nulls since they all map to the same group
    null_group_id: Option<usize>,
    intern_called: bool,
    // 0. cache pointer of arrays, this avoids having to re-compute hashing for arrays weve already seen on past iterations
    // 1. avoid re-allocating buffer inbetween calls, instead of allocating a new vector each time re-use inbetween calls
    values_cache: (Option<ArrayRef>, Vec<u64>),
}

impl<K: ArrowDictionaryKeyType + Send> GroupValuesDictionary<K> {
    pub fn new(data_type: &DataType) -> Self {
        Self {
            seen_elements: Vec::new(),
            unique_dict_value_mapping: HashTable::with_capacity(INITIAL_PRE_ALLOCATION),
            value_dt: data_type.clone(),
            _phantom: PhantomData,
            random_state: RandomState::with_seed(0),
            null_group_id: None,
            intern_called: false,
            values_cache: (None, Vec::new()),
        }
    }

    /// Returns the existing `group_id` for the value with this hash and bytes,
    /// or inserts a new entry and returns the freshly assigned group_id.
    fn lookup_or_insert_in_table(&mut self, hash: u64, raw: &[u8]) -> usize {
        match self
            .unique_dict_value_mapping
            .entry(hash, |e| e.bytes == raw, |e| e.hash)
        {
            HashTableEntry::Occupied(o) => o.get().group_id,
            HashTableEntry::Vacant(v) => {
                let new_group_id = self.seen_elements.len();
                self.seen_elements.push(Some(raw.to_vec())); // replace this with raw buffer to avoid this double copy #TODO see https://github.com/apache/datafusion/issues/22078
                v.insert(DictEntry {
                    hash,
                    group_id: new_group_id,
                    bytes: raw.to_vec(),
                });
                new_group_id
            }
        }
    }
    fn compute_value_hashes(&mut self, values: &ArrayRef) -> Result<()> {
        self.values_cache.1.clear();
        self.values_cache.1.resize(values.len(), 0);
        create_hashes(
            [Arc::clone(values)],
            &self.random_state,
            &mut self.values_cache.1,
        )?;
        Ok(())
        //Ok(hashes)
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
            self.null_group_id = Some(new_group_id);
            new_group_id
        }
    }
    fn transform_into_array(&self, raw: &[Option<Vec<u8>>]) -> Result<ArrayRef> {
        let item_capacity = raw.len();
        let data_capacity = item_capacity * AVG_BYTES_PER_DICT_VALUE;
        match &self.value_dt {
            DataType::Utf8 => decode_scalar_string!(
                raw,
                StringBuilder::with_capacity(item_capacity, data_capacity)
            ),
            DataType::LargeUtf8 => decode_scalar_string!(
                raw,
                LargeStringBuilder::with_capacity(item_capacity, data_capacity)
            ),
            DataType::Utf8View => decode_scalar_string!(
                raw,
                StringViewBuilder::with_capacity(item_capacity)
            ),
            DataType::List(field) => match field.data_type() {
                DataType::Utf8 => decode_list!(
                    raw,
                    ListBuilder::with_capacity(
                        StringBuilder::with_capacity(item_capacity, data_capacity),
                        item_capacity,
                    )
                ),
                DataType::LargeUtf8 => decode_list!(
                    raw,
                    ListBuilder::with_capacity(
                        LargeStringBuilder::with_capacity(item_capacity, data_capacity),
                        item_capacity,
                    )
                ),
                DataType::Utf8View => decode_list!(
                    raw,
                    ListBuilder::with_capacity(
                        StringViewBuilder::with_capacity(item_capacity),
                        item_capacity,
                    )
                ),
                other => Err(NotImplemented(format!(
                    "transform_into_array not implemented for List<{other:?}>"
                ))),
            },
            other => Err(NotImplemented(format!(
                "transform_into_array not implemented for {other:?}"
            ))),
        }
    }
}

impl<K: ArrowDictionaryKeyType + Send> GroupValues for GroupValuesDictionary<K> {
    fn size(&self) -> usize {
        let seen_elements_size = self.seen_elements.capacity()
            * size_of::<Option<Vec<u8>>>()
            + self
                .seen_elements
                .iter()
                .filter_map(|opt| opt.as_ref())
                .map(|inner| inner.capacity())
                .sum::<usize>();

        let unique_mapping_size = self.unique_dict_value_mapping.capacity()
            * size_of::<DictEntry>()
            + self
                .unique_dict_value_mapping
                .iter()
                .map(|e| e.bytes.capacity())
                .sum::<usize>();

        let values_cache_size = self.values_cache.1.capacity() * size_of::<u64>()
            + self
                .values_cache
                .0
                .as_ref()
                .map(|a| a.to_data().get_array_memory_size())
                .unwrap_or(0);

        size_of::<Self>() + seen_elements_size + unique_mapping_size + values_cache_size
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
            .as_dictionary_opt()
            .map_or_else(|| Err(Internal("Expected dictionary array".into())), Ok)?;

        let values = dict_array.values();
        let key_array: &PrimitiveArray<K> = dict_array.keys();
        if key_array.is_empty() {
            return Ok(());
        }

        let cache_hit = self
            .values_cache
            .0
            .as_ref()
            .map(|cached| Arc::ptr_eq(cached, values))
            .unwrap_or(false);

        if !cache_hit {
            // values array changed since last batch - recompute hashes and update cached pointer
            self.compute_value_hashes(values)?;
            self.values_cache.0 = Some(Arc::clone(values));
        }
        let mut key_to_group: Vec<Option<usize>> = vec![None; values.len()];
        // For values already seen in prior batches, pre fill `key_to_group`
        // so the main loop below can skip hash table lookups for those keys.
        // This pre scan is skipped on the first batch since the hash table is empty.
        if self.intern_called {
            for i in 0..key_array.len() {
                if key_array.is_null(i) {
                    continue;
                }
                let original_key = key_array.value(i).to_usize().unwrap();
                if values.is_null(original_key) {
                    continue;
                }
                if key_to_group[original_key].is_some() {
                    continue;
                }

                let hash = self.values_cache.1[original_key];
                let raw = Self::get_raw_bytes(values, original_key);
                if let Some(entry) = self
                    .unique_dict_value_mapping
                    .find(hash, |e| e.bytes == raw.as_ref())
                {
                    key_to_group[original_key] = Some(entry.group_id);
                }
            }
        }
        // iterate keys array (n iterations)
        // only d insertions at most, repeated work is cached
        for i in 0..key_array.len() {
            let group_id = if key_array.is_null(i) {
                self.get_null_group_id()
            } else {
                let key = key_array.value(i).to_usize().unwrap();
                if let Some(group_id) = key_to_group[key] {
                    group_id
                } else if values.is_null(key) {
                    let gid = self.get_null_group_id();
                    key_to_group[key] = Some(gid);
                    gid
                } else {
                    let hash = self.values_cache.1[key];
                    let raw = Self::get_raw_bytes(values, key);
                    let gid = self.lookup_or_insert_in_table(hash, raw.as_ref());
                    key_to_group[key] = Some(gid);
                    gid
                }
            };
            groups.push(group_id);
        }
        // set flag to true to repeated calls to intern will pre-fill key_to_group cache
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
                let first_n = self.seen_elements.drain(..n).collect::<Vec<_>>(); // again read from raw buffer directly
                let original_null_id = self.null_group_id.filter(|&id| id < n);
                // update null_group_id if the null group was in the first n
                if let Some(null_id) = self.null_group_id {
                    if null_id < n {
                        self.null_group_id = None;
                    } else {
                        self.null_group_id = Some(null_id - n);
                    }
                }
                // shift all remaining group indices down by n in the table
                self.unique_dict_value_mapping.retain(|e| {
                    if e.group_id < n {
                        false
                    } else {
                        e.group_id -= n;
                        true
                    }
                });
                (first_n, original_null_id)
            }
        };

        let n = elements_to_emit.len();
        let values_array = self.transform_into_array(&elements_to_emit)?; // ingest a buffer + n then produce an array. should also update the len+offset of other entries #TODO see https://github.com/apache/datafusion/issues/22078

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
        self.unique_dict_value_mapping
            .shrink_to(num_rows, |e| e.hash);
        self.intern_called = false;
        self.values_cache.0 = None;
        self.values_cache.1.clear();
        self.values_cache.1.shrink_to(num_rows);
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use arrow::datatypes::{Field, Int32Type};

    fn make_dict_array(values: ArrayRef, keys: Vec<Option<i32>>) -> ArrayRef {
        let mut kb = PrimitiveBuilder::<Int32Type>::with_capacity(keys.len());
        for k in keys {
            match k {
                Some(v) => kb.append_value(v),
                None => kb.append_null(),
            }
        }
        Arc::new(
            DictionaryArray::<Int32Type>::try_new(kb.finish(), values)
                .expect("Failed to create dictionary array"),
        )
    }

    fn make_utf8_values(values: Vec<Option<&str>>) -> ArrayRef {
        let mut b = StringBuilder::new();
        for v in values {
            match v {
                Some(s) => b.append_value(s),
                None => b.append_null(),
            }
        }
        Arc::new(b.finish())
    }

    fn make_list_utf8_values(lists: Vec<Option<Vec<Option<&str>>>>) -> ArrayRef {
        let mut b = ListBuilder::new(StringBuilder::new());
        for l in lists {
            match l {
                Some(items) => {
                    for s in items {
                        match s {
                            Some(s) => b.values().append_value(s),
                            None => b.values().append_null(),
                        }
                    }
                    b.append(true);
                }
                None => b.append_null(),
            }
        }
        Arc::new(b.finish())
    }

    fn make_list_large_utf8_values(lists: Vec<Option<Vec<Option<&str>>>>) -> ArrayRef {
        let mut b = ListBuilder::new(LargeStringBuilder::new());
        for l in lists {
            match l {
                Some(items) => {
                    for s in items {
                        match s {
                            Some(s) => b.values().append_value(s),
                            None => b.values().append_null(),
                        }
                    }
                    b.append(true);
                }
                None => b.append_null(),
            }
        }
        Arc::new(b.finish())
    }

    fn list_utf8_dt() -> DataType {
        DataType::List(Arc::new(Field::new("item", DataType::Utf8, true)))
    }

    fn list_large_utf8_dt() -> DataType {
        DataType::List(Arc::new(Field::new("item", DataType::LargeUtf8, true)))
    }

    fn run_intern(
        data_type: DataType,
        values: ArrayRef,
        keys: Vec<Option<i32>>,
    ) -> (GroupValuesDictionary<Int32Type>, Vec<usize>) {
        let mut gv = GroupValuesDictionary::<Int32Type>::new(&data_type);
        let dict = make_dict_array(values, keys);
        let mut groups = Vec::new();
        gv.intern(&[dict], &mut groups)
            .expect("intern should succeed");
        (gv, groups)
    }

    mod null_test {
        use super::*;

        #[test]
        fn test_null_keys_and_values_utf8() {
            let values = make_utf8_values(vec![Some("x"), None, Some("y")]);
            let (gv, groups) = run_intern(
                DataType::Utf8,
                values,
                vec![Some(0), None, Some(1), None, Some(2)],
            );
            assert_eq!(gv.len(), 3);
            assert_eq!(groups[1], groups[3]);
            assert_ne!(groups[0], groups[4]);
        }

        #[test]
        fn test_null_strings_inside_lists() {
            let values = make_list_utf8_values(vec![
                Some(vec![Some("a"), None, Some("b")]),
                Some(vec![None, None]),
                Some(vec![Some("a"), None, Some("b")]),
            ]);
            let (gv, groups) =
                run_intern(list_utf8_dt(), values, vec![Some(0), Some(1), Some(2)]);
            assert_eq!(gv.len(), 2);
            assert_eq!(groups[0], groups[2]);
            assert_ne!(groups[0], groups[1]);
        }

        #[test]
        fn test_empty_list_vs_null_list() {
            let values = make_list_utf8_values(vec![
                Some(vec![]),
                None,
                Some(vec![]),
                Some(vec![Some("a")]),
            ]);
            let (gv, groups) = run_intern(
                list_utf8_dt(),
                values,
                vec![Some(0), Some(1), Some(2), Some(3)],
            );
            assert_eq!(gv.len(), 3);
            assert_eq!(groups[0], groups[2]);
            assert_ne!(groups[0], groups[1]);
            assert_ne!(groups[1], groups[3]);
        }

        #[test]
        fn test_null_group_stable_across_batches() {
            let mut gv = GroupValuesDictionary::<Int32Type>::new(&list_large_utf8_dt());
            let mut groups = Vec::new();

            let v1 = make_list_large_utf8_values(vec![
                Some(vec![Some("ab")]),
                Some(vec![Some("cd")]),
            ]);
            let dict1 = make_dict_array(v1, vec![None, Some(0), None, Some(1)]);
            gv.intern(&[dict1], &mut groups)
                .expect("first intern should succeed");
            assert_eq!(gv.len(), 3);
            assert_eq!(groups, vec![0, 1, 0, 2]);

            let emitted_1 = gv.emit(EmitTo::All).expect("first emit should succeed");
            assert_eq!(emitted_1.len(), 1);
            assert_eq!(gv.len(), 0);

            groups.clear();

            // Batch 2 reorders the dict to ["cd", "ab"]; null group should remain stable
            let v2 = make_list_large_utf8_values(vec![
                Some(vec![Some("cd")]),
                Some(vec![Some("ab")]),
            ]);
            let dict2 = make_dict_array(v2, vec![Some(0), None, Some(1), None]);
            gv.intern(&[dict2], &mut groups)
                .expect("second intern should succeed");
            assert_eq!(gv.len(), 3);
            assert_eq!(groups, vec![0, 1, 2, 1]);
            assert_eq!(groups[1], groups[3]);
        }
    }

    mod data_correctness {
        use super::*;

        #[test]
        fn test_non_canonicalized_dict_array_list() {
            let values = make_list_utf8_values(vec![
                Some(vec![Some("alpha")]),
                Some(vec![Some("beta")]),
                Some(vec![Some("gamma")]),
                Some(vec![Some("alpha")]),
                Some(vec![Some("beta")]),
            ]);
            let (mut gv, groups) = run_intern(
                list_utf8_dt(),
                values,
                vec![Some(2), Some(0), Some(1), Some(2), Some(1)],
            );

            assert_eq!(gv.len(), 3);
            assert_eq!(groups.len(), 5);
            assert_eq!(groups[0], groups[3]);
            assert_eq!(groups[2], groups[4]);

            let result = gv.emit(EmitTo::All).expect("emit should succeed");
            let emitted = result[0]
                .as_any()
                .downcast_ref::<DictionaryArray<Int32Type>>()
                .expect("Expected DictionaryArray");
            let list_array = emitted
                .values()
                .as_any()
                .downcast_ref::<ListArray>()
                .expect("Expected ListArray");
            assert_eq!(list_array.len(), 3);
            for (i, expected) in ["gamma", "alpha", "beta"].iter().enumerate() {
                let list = list_array.value(i);
                let strings = list
                    .as_any()
                    .downcast_ref::<StringArray>()
                    .expect("Expected StringArray");
                assert_eq!(strings.value(0), *expected);
            }
            assert!(gv.is_empty());
        }

        #[test]
        fn test_duplicate_values_in_values_array_utf8() {
            let values =
                make_utf8_values(vec![Some("x"), Some("y"), Some("x"), Some("y")]);
            let (mut gv, groups) = run_intern(
                DataType::Utf8,
                values,
                vec![Some(0), Some(1), Some(2), Some(3), Some(0)],
            );

            assert_eq!(gv.len(), 2);
            assert_eq!(groups.len(), 5);
            assert_eq!(groups[0], groups[2]);
            assert_eq!(groups[2], groups[4]);
            assert_eq!(groups[1], groups[3]);
            assert_ne!(groups[0], groups[1]);

            let result = gv.emit(EmitTo::All).expect("emit should succeed");
            let emitted = result[0]
                .as_any()
                .downcast_ref::<DictionaryArray<Int32Type>>()
                .expect("Expected DictionaryArray");
            let string_values = emitted
                .values()
                .as_any()
                .downcast_ref::<StringArray>()
                .expect("Expected StringArray");

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
            assert!(gv.is_empty());
        }

        #[test]
        fn test_duplicate_values_in_values_array_list() {
            let values = make_list_utf8_values(vec![
                Some(vec![Some("x")]),
                Some(vec![Some("y")]),
                Some(vec![Some("x")]),
                Some(vec![Some("y")]),
            ]);
            let (mut gv, groups) = run_intern(
                list_utf8_dt(),
                values,
                vec![Some(0), Some(1), Some(2), Some(3), Some(0)],
            );

            assert_eq!(gv.len(), 2);
            assert_eq!(groups.len(), 5);
            assert_eq!(groups[0], groups[2]);
            assert_eq!(groups[2], groups[4]);
            assert_eq!(groups[1], groups[3]);
            assert_ne!(groups[0], groups[1]);

            let result = gv.emit(EmitTo::All).expect("emit should succeed");
            let emitted = result[0]
                .as_any()
                .downcast_ref::<DictionaryArray<Int32Type>>()
                .expect("Expected DictionaryArray");
            let list_array = emitted
                .values()
                .as_any()
                .downcast_ref::<ListArray>()
                .expect("Expected ListArray");
            assert!(gv.is_empty());
            assert_eq!(list_array.len(), 2);
            let string_array = list_array
                .values()
                .as_any()
                .downcast_ref::<StringArray>()
                .expect("Expected StringArray");
            assert_eq!(string_array.value(0), "x");
            assert_eq!(string_array.value(1), "y");
        }
    }
}
