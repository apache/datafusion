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

//! A wrapper around `hashbrown::HashTable` that allows entries to be tracked by index

use crate::aggregates::group_values::HashValue;
use crate::aggregates::topk::heap::Comparable;
use arrow::array::types::{IntervalDayTime, IntervalMonthDayNano};
use arrow::array::{
    Array, ArrayRef, ArrowPrimitiveType, LargeStringArray, PrimitiveArray, StringArray,
    StringViewArray, builder::PrimitiveBuilder, cast::AsArray, downcast_primitive,
};
use arrow::datatypes::{DataType, i256};
use datafusion_common::Result;
use datafusion_common::exec_datafusion_err;
use datafusion_common::hash_utils::RandomState;
use half::f16;
use hashbrown::hash_table::HashTable;
use std::fmt::Debug;
use std::hash::BuildHasher;
use std::sync::Arc;

/// A "type alias" for Keys which are stored in our map
pub trait KeyType: Clone + Comparable + Debug {}

impl<T> KeyType for T where T: Clone + Comparable + Debug {}

/// `heap_idx` assigned to groups whose aggregate values are all NULL. Such
/// groups are tracked in the hash table only (they never enter the heap), so
/// they can be emitted with a NULL aggregate value at the end.
const NULL_HEAP_IDX: usize = usize::MAX;

/// An entry in our hash table that:
/// 1. memoizes the hash
/// 2. contains the key (ID)
/// 3. contains the value (heap_idx - an index into the corresponding heap)
pub struct HashTableItem<ID: KeyType> {
    hash: u64,
    pub id: ID,
    pub heap_idx: usize,
}

/// A custom wrapper around `hashbrown::HashTable` that:
/// 1. limits the number of entries to the top K
/// 2. Allocates a capacity greater than top K to maintain a low-fill factor and prevent resizing
/// 3. Tracks indexes to allow corresponding heap to refer to entries by index vs hash
struct TopKHashTable<ID: KeyType> {
    map: HashTable<usize>,
    // Store the actual items separately to allow for index-based access
    store: Vec<Option<HashTableItem<ID>>>,
    // Free indexes in the store for reuse
    free_indices: Vec<usize>,
    // The maximum number of entries allowed
    limit: usize,
    // Number of entries registered as all-NULL (heap_idx == NULL_HEAP_IDX)
    null_count: usize,
}

/// Outcome of [`ArrowHashTable::find_or_insert`], letting the caller keep its
/// own all-NULL group accounting in sync without an extra lookup.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum InsertKind {
    /// The group already existed as a valued group
    Existing,
    /// The group was newly inserted as a valued group
    New,
    /// The group was registered as all-NULL and has now been converted into a
    /// valued group
    ReplacedNull,
}

/// An interface to hide the generic type signature of TopKHashTable behind arrow arrays
pub trait ArrowHashTable {
    fn set_batch(&mut self, ids: ArrayRef);
    fn len(&self) -> usize;
    fn update_heap_idx(&mut self, mapper: &[(usize, usize)]);
    fn heap_idx_at(&self, map_idx: usize) -> usize;
    fn take_all(&mut self, indexes: Vec<usize>) -> ArrayRef;
    fn find_or_insert(
        &mut self,
        row_idx: usize,
        replace_idx: usize,
    ) -> (usize, InsertKind);
    /// Register the group at `row_idx` as all-NULL. Returns true if it was
    /// newly registered; false if the group is already tracked or the NULL
    /// group limit has been reached.
    fn insert_null(&mut self, row_idx: usize) -> bool;
    /// Remove the group at `row_idx` if it is registered as all-NULL. Returns
    /// true if a NULL registration was removed.
    fn remove_if_null(&mut self, row_idx: usize) -> bool;
    /// Store indexes of all groups registered as all-NULL
    fn null_map_idxs(&self) -> Vec<usize>;
}

/// Returns true if the given data type can be used as a top-K aggregation hash key.
///
/// Supported types include Arrow primitives (integers, floats, decimals, intervals)
/// and UTF-8 strings (`Utf8`, `LargeUtf8`, `Utf8View`). This is used internally by
/// `PriorityMap::supports()` to validate grouping key type compatibility.
pub fn is_supported_hash_key_type(kt: &DataType) -> bool {
    kt.is_primitive()
        || matches!(
            kt,
            DataType::Utf8 | DataType::Utf8View | DataType::LargeUtf8
        )
}

// An implementation of ArrowHashTable for String keys
pub struct StringHashTable {
    owned: ArrayRef,
    map: TopKHashTable<Option<String>>,
    rnd: RandomState,
    data_type: DataType,
}

// An implementation of ArrowHashTable for any `ArrowPrimitiveType` key
struct PrimitiveHashTable<VAL: ArrowPrimitiveType>
where
    Option<<VAL as ArrowPrimitiveType>::Native>: Comparable,
{
    owned: ArrayRef,
    map: TopKHashTable<Option<VAL::Native>>,
    rnd: RandomState,
    kt: DataType,
}

impl StringHashTable {
    pub fn new(limit: usize, data_type: DataType) -> Self {
        let vals: Vec<&str> = Vec::new();
        let owned: ArrayRef = match data_type {
            DataType::Utf8 => Arc::new(StringArray::from(vals)),
            DataType::Utf8View => Arc::new(StringViewArray::from(vals)),
            DataType::LargeUtf8 => Arc::new(LargeStringArray::from(vals)),
            _ => panic!("Unsupported data type"),
        };

        Self {
            owned,
            map: TopKHashTable::new(limit, limit * 10),
            rnd: RandomState::default(),
            data_type,
        }
    }

    /// Extracts the string value at the given row index, handling nulls and different string types.
    ///
    /// Returns `None` if the value is null, otherwise `Some(value.to_string())`.
    fn extract_string_value(&self, row_idx: usize) -> Option<String> {
        let is_null_and_value = match self.data_type {
            DataType::Utf8 => {
                let arr = self.owned.as_string::<i32>();
                (arr.is_null(row_idx), arr.value(row_idx))
            }
            DataType::LargeUtf8 => {
                let arr = self.owned.as_string::<i64>();
                (arr.is_null(row_idx), arr.value(row_idx))
            }
            DataType::Utf8View => {
                let arr = self.owned.as_string_view();
                (arr.is_null(row_idx), arr.value(row_idx))
            }
            _ => panic!("Unsupported data type"),
        };

        let (is_null, value) = is_null_and_value;
        if is_null {
            None
        } else {
            Some(value.to_string())
        }
    }

    /// Computes the id and its hash for the given row, for hash table lookups
    fn id_and_hash(&self, row_idx: usize) -> (Option<String>, u64) {
        let id = self.extract_string_value(row_idx);
        let hash = self.rnd.hash_one(id.as_deref());
        (id, hash)
    }
}

impl ArrowHashTable for StringHashTable {
    fn set_batch(&mut self, ids: ArrayRef) {
        self.owned = ids;
    }

    fn len(&self) -> usize {
        self.map.len()
    }

    fn update_heap_idx(&mut self, mapper: &[(usize, usize)]) {
        self.map.update_heap_idx(mapper);
    }

    fn heap_idx_at(&self, map_idx: usize) -> usize {
        self.map.heap_idx_at(map_idx)
    }

    fn take_all(&mut self, indexes: Vec<usize>) -> ArrayRef {
        let ids = self.map.take_all(indexes);
        match self.data_type {
            DataType::Utf8 => Arc::new(StringArray::from(ids)),
            DataType::LargeUtf8 => Arc::new(LargeStringArray::from(ids)),
            DataType::Utf8View => Arc::new(StringViewArray::from(ids)),
            _ => unreachable!(),
        }
    }

    fn find_or_insert(
        &mut self,
        row_idx: usize,
        replace_idx: usize,
    ) -> (usize, InsertKind) {
        let id = self.extract_string_value(row_idx);

        // Compute hash and create equality closure for hash table lookup.
        let hash = self.rnd.hash_one(id.as_deref());
        let id_for_eq = id.clone();
        let eq = move |mi: &Option<String>| id_for_eq.as_deref() == mi.as_deref();

        // Use entry API to avoid double lookup
        self.map.find_or_insert(hash, id, replace_idx, eq)
    }

    fn insert_null(&mut self, row_idx: usize) -> bool {
        let (id, hash) = self.id_and_hash(row_idx);
        let id_for_eq = id.clone();
        let eq = move |mi: &Option<String>| id_for_eq.as_deref() == mi.as_deref();
        self.map.insert_null(hash, id, eq)
    }

    fn remove_if_null(&mut self, row_idx: usize) -> bool {
        let (id, hash) = self.id_and_hash(row_idx);
        let eq = move |mi: &Option<String>| id.as_deref() == mi.as_deref();
        self.map.remove_if_null(hash, eq)
    }

    fn null_map_idxs(&self) -> Vec<usize> {
        self.map.null_map_idxs()
    }
}

impl<VAL: ArrowPrimitiveType> PrimitiveHashTable<VAL>
where
    Option<<VAL as ArrowPrimitiveType>::Native>: Comparable,
    Option<<VAL as ArrowPrimitiveType>::Native>: HashValue,
{
    pub fn new(limit: usize, kt: DataType) -> Self {
        let owned = Arc::new(
            PrimitiveArray::<VAL>::builder(0)
                .with_data_type(kt.clone())
                .finish(),
        );
        Self {
            owned,
            map: TopKHashTable::new(limit, limit * 10),
            rnd: RandomState::default(),
            kt,
        }
    }

    /// Computes the id and its hash for the given row, for hash table lookups
    fn id_and_hash(&self, row_idx: usize) -> (Option<VAL::Native>, u64) {
        let ids = self.owned.as_primitive::<VAL>();
        let id: Option<VAL::Native> = if ids.is_null(row_idx) {
            None
        } else {
            Some(ids.value(row_idx))
        };
        let hash: u64 = id.hash(&self.rnd);
        (id, hash)
    }
}

impl<VAL: ArrowPrimitiveType> ArrowHashTable for PrimitiveHashTable<VAL>
where
    Option<<VAL as ArrowPrimitiveType>::Native>: Comparable,
    Option<<VAL as ArrowPrimitiveType>::Native>: HashValue,
{
    fn set_batch(&mut self, ids: ArrayRef) {
        self.owned = ids;
    }

    fn len(&self) -> usize {
        self.map.len()
    }

    fn update_heap_idx(&mut self, mapper: &[(usize, usize)]) {
        self.map.update_heap_idx(mapper);
    }

    fn heap_idx_at(&self, map_idx: usize) -> usize {
        self.map.heap_idx_at(map_idx)
    }

    fn take_all(&mut self, indexes: Vec<usize>) -> ArrayRef {
        let ids = self.map.take_all(indexes);
        let mut builder: PrimitiveBuilder<VAL> =
            PrimitiveArray::builder(ids.len()).with_data_type(self.kt.clone());
        for id in ids.into_iter() {
            match id {
                None => builder.append_null(),
                Some(id) => builder.append_value(id),
            }
        }
        let ids = builder.finish();
        Arc::new(ids)
    }

    fn find_or_insert(
        &mut self,
        row_idx: usize,
        replace_idx: usize,
    ) -> (usize, InsertKind) {
        let ids = self.owned.as_primitive::<VAL>();
        let id: Option<VAL::Native> = if ids.is_null(row_idx) {
            None
        } else {
            Some(ids.value(row_idx))
        };
        // Compute hash and create equality closure for hash table lookup.
        let hash: u64 = id.hash(&self.rnd);
        let eq = |mi: &Option<VAL::Native>| id == *mi;

        // Use entry API to avoid double lookup
        self.map.find_or_insert(hash, id, replace_idx, eq)
    }

    fn insert_null(&mut self, row_idx: usize) -> bool {
        let (id, hash) = self.id_and_hash(row_idx);
        let eq = move |mi: &Option<VAL::Native>| id == *mi;
        self.map.insert_null(hash, id, eq)
    }

    fn remove_if_null(&mut self, row_idx: usize) -> bool {
        let (id, hash) = self.id_and_hash(row_idx);
        let eq = move |mi: &Option<VAL::Native>| id == *mi;
        self.map.remove_if_null(hash, eq)
    }

    fn null_map_idxs(&self) -> Vec<usize> {
        self.map.null_map_idxs()
    }
}

use hashbrown::hash_table::Entry;
impl<ID: KeyType + PartialEq> TopKHashTable<ID> {
    pub fn new(limit: usize, capacity: usize) -> Self {
        Self {
            map: HashTable::with_capacity(capacity),
            store: Vec::with_capacity(capacity),
            free_indices: Vec::new(),
            limit,
            null_count: 0,
        }
    }

    pub fn heap_idx_at(&self, map_idx: usize) -> usize {
        self.store[map_idx].as_ref().unwrap().heap_idx
    }

    /// Remove the entry stored at `map_idx`, freeing its store slot for reuse
    fn remove_at(&mut self, map_idx: usize) {
        let item_to_remove = self.store[map_idx].as_ref().unwrap();
        let hash = item_to_remove.hash;
        let id_to_remove = &item_to_remove.id;

        let eq = |&idx: &usize| self.store[idx].as_ref().unwrap().id == *id_to_remove;
        let hasher = |idx: &usize| self.store[*idx].as_ref().unwrap().hash;
        match self.map.entry(hash, eq, hasher) {
            Entry::Occupied(entry) => {
                let (removed_idx, _) = entry.remove();
                self.store[removed_idx] = None;
                self.free_indices.push(removed_idx);
            }
            Entry::Vacant(_) => unreachable!(),
        }
    }

    pub fn remove_if_full(&mut self, replace_idx: usize) -> usize {
        // All-NULL groups are tracked outside the heap, so only valued
        // groups count towards the limit here
        let valued_len = self.map.len() - self.null_count;
        if valued_len >= self.limit {
            self.remove_at(replace_idx);
            0 // if full, always replace top node
        } else {
            valued_len // if we're not full, always append to end
        }
    }

    fn update_heap_idx(&mut self, mapper: &[(usize, usize)]) {
        for (m, h) in mapper {
            self.store[*m].as_mut().unwrap().heap_idx = *h;
        }
    }

    /// Find an existing entry or insert a new one, avoiding double hash table lookup.
    /// Returns (map_idx, kind) where kind describes whether the group already
    /// existed, was newly inserted, or was converted from an all-NULL group.
    /// If inserting a new entry and the table is full, replaces the entry at replace_idx.
    pub fn find_or_insert(
        &mut self,
        hash: u64,
        id: ID,
        replace_idx: usize,
        mut eq: impl FnMut(&ID) -> bool,
    ) -> (usize, InsertKind) {
        // Check if entry exists - this is the only hash table lookup
        let mut replaced_null = false;
        {
            let eq_fn = |idx: &usize| eq(&self.store[*idx].as_ref().unwrap().id);
            if let Some(&map_idx) = self.map.find(hash, eq_fn) {
                if self.store[map_idx].as_ref().unwrap().heap_idx == NULL_HEAP_IDX {
                    // This group was registered as all-NULL but now produced a
                    // value: unregister it so it is inserted as a valued group
                    self.remove_at(map_idx);
                    self.null_count -= 1;
                    replaced_null = true;
                } else {
                    return (map_idx, InsertKind::Existing);
                }
            }
        }

        // Entry doesn't exist - compute heap_idx and prepare item
        let heap_idx = self.remove_if_full(replace_idx);
        let mi = HashTableItem::new(hash, id, heap_idx);
        let store_idx = if let Some(idx) = self.free_indices.pop() {
            self.store[idx] = Some(mi);
            idx
        } else {
            self.store.push(Some(mi));
            self.store.len() - 1
        };

        // Reserve space if needed
        let hasher = |idx: &usize| self.store[*idx].as_ref().unwrap().hash;
        if self.map.len() == self.map.capacity() {
            self.map.reserve(self.limit, hasher);
        }

        // Insert without checking again since we already confirmed it doesn't exist
        self.map.insert_unique(hash, store_idx, hasher);
        let kind = if replaced_null {
            InsertKind::ReplacedNull
        } else {
            InsertKind::New
        };
        (store_idx, kind)
    }

    /// Register a group whose aggregate values are all NULL, unless it is
    /// already tracked. NULL groups are stored with a sentinel `heap_idx` and
    /// never enter the heap. At most `limit` NULL groups are tracked: they all
    /// tie on the sort key, so any `limit` of them is a valid top-k superset.
    /// Returns true if the group was newly registered.
    pub fn insert_null(
        &mut self,
        hash: u64,
        id: ID,
        mut eq: impl FnMut(&ID) -> bool,
    ) -> bool {
        {
            let eq_fn = |idx: &usize| eq(&self.store[*idx].as_ref().unwrap().id);
            if self.map.find(hash, eq_fn).is_some() {
                return false;
            }
        }
        if self.null_count >= self.limit {
            return false;
        }

        let mi = HashTableItem::new(hash, id, NULL_HEAP_IDX);
        let store_idx = if let Some(idx) = self.free_indices.pop() {
            self.store[idx] = Some(mi);
            idx
        } else {
            self.store.push(Some(mi));
            self.store.len() - 1
        };

        let hasher = |idx: &usize| self.store[*idx].as_ref().unwrap().hash;
        if self.map.len() == self.map.capacity() {
            self.map.reserve(self.limit, hasher);
        }
        self.map.insert_unique(hash, store_idx, hasher);
        self.null_count += 1;
        true
    }

    /// Remove the given group if it is registered as all-NULL. Used when an
    /// all-NULL group produces a value that loses to the current top-k: the
    /// group can no longer reach the top-k, but it must not be emitted with a
    /// NULL value either. Returns true if a NULL registration was removed.
    pub fn remove_if_null(&mut self, hash: u64, mut eq: impl FnMut(&ID) -> bool) -> bool {
        let eq_fn = |idx: &usize| eq(&self.store[*idx].as_ref().unwrap().id);
        if let Some(&map_idx) = self.map.find(hash, eq_fn)
            && self.store[map_idx].as_ref().unwrap().heap_idx == NULL_HEAP_IDX
        {
            self.remove_at(map_idx);
            self.null_count -= 1;
            return true;
        }
        false
    }

    /// Store indexes of all groups registered as all-NULL
    pub fn null_map_idxs(&self) -> Vec<usize> {
        self.store
            .iter()
            .enumerate()
            .filter_map(|(idx, item)| {
                item.as_ref()
                    .filter(|item| item.heap_idx == NULL_HEAP_IDX)
                    .map(|_| idx)
            })
            .collect()
    }

    pub fn len(&self) -> usize {
        self.map.len()
    }

    pub fn take_all(&mut self, idxs: Vec<usize>) -> Vec<ID> {
        let ids = idxs
            .into_iter()
            .map(|idx| self.store[idx].take().unwrap().id)
            .collect();
        self.map.clear();
        self.store.clear();
        self.free_indices.clear();
        self.null_count = 0;
        ids
    }
}

impl<ID: KeyType> HashTableItem<ID> {
    pub fn new(hash: u64, id: ID, heap_idx: usize) -> Self {
        Self { hash, id, heap_idx }
    }
}

impl HashValue for Option<String> {
    fn hash(&self, state: &RandomState) -> u64 {
        state.hash_one(self)
    }
}

macro_rules! hash_float {
    ($($t:ty),+) => {
        $(impl HashValue for Option<$t> {
            fn hash(&self, state: &RandomState) -> u64 {
                self.map(|me| me.hash(state)).unwrap_or(0)
            }
        })+
    };
}

macro_rules! has_integer {
    ($($t:ty),+) => {
        $(impl HashValue for Option<$t> {
            fn hash(&self, state: &RandomState) -> u64 {
                self.map(|me| me.hash(state)).unwrap_or(0)
            }
        })+
    };
}

has_integer!(i8, i16, i32, i64, i128, i256);
has_integer!(u8, u16, u32, u64);
has_integer!(IntervalDayTime, IntervalMonthDayNano);
hash_float!(f16, f32, f64);

pub fn new_hash_table(
    limit: usize,
    kt: DataType,
) -> Result<Box<dyn ArrowHashTable + Send>> {
    macro_rules! downcast_helper {
        ($kt:ty, $d:ident) => {
            return Ok(Box::new(PrimitiveHashTable::<$kt>::new(limit, kt)))
        };
    }

    downcast_primitive! {
        kt => (downcast_helper, kt),
        DataType::Utf8 => return Ok(Box::new(StringHashTable::new(limit, DataType::Utf8))),
        DataType::LargeUtf8 => return Ok(Box::new(StringHashTable::new(limit, DataType::LargeUtf8))),
        DataType::Utf8View => return Ok(Box::new(StringHashTable::new(limit, DataType::Utf8View))),
        _ => {}
    }

    Err(exec_datafusion_err!(
        "Can't create HashTable for type: {kt:?}"
    ))
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::TimestampMillisecondArray;
    use arrow_schema::TimeUnit;
    use std::collections::BTreeMap;

    #[test]
    fn should_emit_correct_type() -> Result<()> {
        let ids =
            TimestampMillisecondArray::from(vec![1000]).with_timezone("UTC".to_string());
        let dt = DataType::Timestamp(TimeUnit::Millisecond, Some("UTC".into()));
        let mut ht = new_hash_table(1, dt.clone())?;
        ht.set_batch(Arc::new(ids));
        ht.find_or_insert(0, 0);
        let ids = ht.take_all(vec![0]);
        assert_eq!(ids.data_type(), &dt);

        Ok(())
    }

    #[test]
    fn should_resize_properly() -> Result<()> {
        let mut heap_to_map = BTreeMap::<usize, usize>::new();
        // Create TopKHashTable with limit=5 and capacity=3 to force resizing
        let mut map = TopKHashTable::<Option<String>>::new(5, 3);

        // Insert 5 entries, tracking the heap-to-map index mapping
        for (heap_idx, id) in ["1", "2", "3", "4", "5"].iter().enumerate() {
            let value = Some(id.to_string());
            let hash = heap_idx as u64;
            let (map_idx, kind) =
                map.find_or_insert(hash, value.clone(), heap_idx, |v| *v == value);
            assert_eq!(kind, InsertKind::New, "Entry should be new");
            heap_to_map.insert(heap_idx, map_idx);
        }

        // Verify all 5 entries are present
        assert_eq!(map.len(), 5);

        // Verify that the hash table resized properly (capacity should have grown beyond 3)
        // This is implicit - if it didn't resize, insertions would have failed or been slow

        // Drain all values in heap order
        let (_heap_idxs, map_idxs): (Vec<_>, Vec<_>) = heap_to_map.into_iter().unzip();
        let ids = map.take_all(map_idxs);

        assert_eq!(
            format!("{ids:?}"),
            r#"[Some("1"), Some("2"), Some("3"), Some("4"), Some("5")]"#
        );
        assert_eq!(map.len(), 0, "Map should have been cleared!");

        Ok(())
    }

    #[test]
    fn should_track_null_groups() -> Result<()> {
        let mut map = TopKHashTable::<Option<String>>::new(2, 10);

        let a = Some("a".to_string());
        let b = Some("b".to_string());
        let c = Some("c".to_string());

        // register two all-NULL groups; the third exceeds the NULL group limit
        assert!(map.insert_null(100, a.clone(), |v| *v == a));
        assert!(map.insert_null(200, b.clone(), |v| *v == b));
        assert!(!map.insert_null(300, c.clone(), |v| *v == c));
        // re-registering an existing NULL group is a no-op
        assert!(!map.insert_null(100, a.clone(), |v| *v == a));
        assert_eq!(map.null_count, 2);
        assert_eq!(map.null_map_idxs(), vec![0, 1]);

        // a valued insert for a NULL group converts it to a valued group
        let (map_idx, kind) = map.find_or_insert(200, b.clone(), 0, |v| *v == b);
        assert_eq!(kind, InsertKind::ReplacedNull, "NULL group should convert");
        assert_eq!(map.heap_idx_at(map_idx), 0, "Heap should append at 0");
        assert_eq!(map.null_count, 1);
        assert_eq!(map.null_map_idxs(), vec![0]);

        // remove the remaining NULL group; removing twice is a no-op
        map.remove_if_null(100, |v| *v == a);
        assert_eq!(map.null_count, 0);
        assert!(map.null_map_idxs().is_empty());
        map.remove_if_null(100, |v| *v == a);
        // removing a valued group via remove_if_null is a no-op
        map.remove_if_null(200, |v| *v == b);
        assert_eq!(map.len(), 1);

        Ok(())
    }

    #[test]
    fn should_reuse_all_freed_store_slots() -> Result<()> {
        let mut map = TopKHashTable::<Option<String>>::new(1, 10);

        let a = Some("a".to_string());
        let b = Some("b".to_string());
        let c = Some("c".to_string());

        let (b_idx, kind) = map.find_or_insert(100, b.clone(), 0, |v| *v == b);
        assert_eq!(kind, InsertKind::New);
        assert!(map.insert_null(200, a.clone(), |v| *v == a));

        // Converting a NULL group while the valued heap is full frees two
        // slots: the NULL registration and the evicted valued group.
        let (_, kind) = map.find_or_insert(200, a.clone(), b_idx, |v| *v == a);
        assert_eq!(kind, InsertKind::ReplacedNull);

        // Both freed slots must remain reusable. Otherwise repeated
        // conversions make the backing store grow without bound.
        assert!(map.insert_null(300, c.clone(), |v| *v == c));
        assert_eq!(map.store.len(), 2);

        Ok(())
    }
}
