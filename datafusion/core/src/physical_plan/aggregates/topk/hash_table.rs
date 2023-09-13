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

//! A wrapper around `hashbrown::RawTable` that allows entries to be tracked by index

use crate::physical_plan::aggregates::group_values::primitive::HashValue;
use crate::physical_plan::aggregates::topk::heap::Comparable;
use ahash::RandomState;
use arrow::datatypes::i256;
use arrow_array::builder::PrimitiveBuilder;
use arrow_array::cast::AsArray;
use arrow_array::{
    downcast_primitive, Array, ArrayRef, ArrowPrimitiveType, PrimitiveArray, StringArray,
};
use arrow_schema::DataType;
use datafusion_common::DataFusionError;
use datafusion_common::Result;
use half::f16;
use hashbrown::raw::RawTable;
use std::fmt::Debug;
use std::sync::Arc;

/// A "type alias" for Keys which are stored in our map
pub trait KeyType: Clone + Comparable + Debug {}

impl<T> KeyType for T where T: Clone + Comparable + Debug {}

/// An entry in our hash table that:
/// 1. memoizes the hash
/// 2. contains the key (ID)
/// 3. contains the value (heap_idx - an index into the corresponding heap)
pub struct HashTableItem<ID: KeyType> {
    hash: u64,
    pub id: ID,
    pub heap_idx: usize,
}

/// A custom wrapper around `hashbrown::RawTable` that:
/// 1. limits the number of entries to the top K
/// 2. Allocates a capacity greater than top K to maintain a low-fill factor and prevent resizing
/// 3. Tracks indexes to allow corresponding heap to refer to entries by index vs hash
/// 4. Catches resize events to allow the corresponding heap to update it's indexes
struct TopKHashTable<ID: KeyType> {
    map: RawTable<HashTableItem<ID>>,
    limit: usize,
}

/// An interface to hide the generic type signature of TopKHashTable behind arrow arrays
pub trait ArrowHashTable {
    fn set_batch(&mut self, ids: ArrayRef);
    fn len(&self) -> usize;
    // JUSTIFICATION
    //  Benefit:  ~15% speedup + required to index into RawTable from binary heap
    //  Soundness: the caller must provide valid indexes
    unsafe fn update_heap_idx(&mut self, mapper: &[(usize, usize)]);
    // JUSTIFICATION
    //  Benefit:  ~15% speedup + required to index into RawTable from binary heap
    //  Soundness: the caller must provide a valid index
    unsafe fn heap_idx_at(&self, map_idx: usize) -> usize;
    fn drain(&mut self) -> (ArrayRef, Vec<usize>);

    // JUSTIFICATION
    //  Benefit:  ~15% speedup + required to index into RawTable from binary heap
    //  Soundness: the caller must provide valid indexes
    unsafe fn find_or_insert(
        &mut self,
        row_idx: usize,
        replace_idx: usize,
        map: &mut Vec<(usize, usize)>,
    ) -> (usize, bool);
}

// An implementation of ArrowHashTable for String keys
pub struct StringHashTable {
    owned: ArrayRef,
    map: TopKHashTable<Option<String>>,
    rnd: RandomState,
}

// An implementation of ArrowHashTable for any `ArrowPrimitiveType` key
struct PrimitiveHashTable<VAL: ArrowPrimitiveType>
where
    Option<<VAL as ArrowPrimitiveType>::Native>: Comparable,
{
    owned: ArrayRef,
    map: TopKHashTable<Option<VAL::Native>>,
    rnd: RandomState,
}

impl StringHashTable {
    pub fn new(limit: usize) -> Self {
        let vals: Vec<&str> = Vec::new();
        let owned = Arc::new(StringArray::from(vals));
        Self {
            owned,
            map: TopKHashTable::new(limit, limit * 10),
            rnd: ahash::RandomState::default(),
        }
    }
}

impl ArrowHashTable for StringHashTable {
    fn set_batch(&mut self, ids: ArrayRef) {
        self.owned = ids;
    }

    fn len(&self) -> usize {
        self.map.len()
    }

    unsafe fn update_heap_idx(&mut self, mapper: &[(usize, usize)]) {
        self.map.update_heap_idx(mapper);
    }

    unsafe fn heap_idx_at(&self, map_idx: usize) -> usize {
        self.map.heap_idx_at(map_idx)
    }

    fn drain(&mut self) -> (ArrayRef, Vec<usize>) {
        let mut rows = self.map.drain();
        rows.sort_by(|a, b| a.0.comp(&b.0));
        let (ids, heap_idxs): (Vec<_>, Vec<_>) = rows.into_iter().unzip();
        let ids = Arc::new(StringArray::from(ids));
        (ids, heap_idxs)
    }

    unsafe fn find_or_insert(
        &mut self,
        row_idx: usize,
        replace_idx: usize,
        mapper: &mut Vec<(usize, usize)>,
    ) -> (usize, bool) {
        let ids = self
            .owned
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("StringArray required");
        let id = if ids.is_null(row_idx) {
            None
        } else {
            Some(ids.value(row_idx))
        };

        let hash = self.rnd.hash_one(id);
        if let Some(map_idx) = self
            .map
            .find(hash, |mi| id == mi.as_ref().map(|id| id.as_str()))
        {
            return (map_idx, false);
        }

        // we're full and this is a better value, so remove the worst
        let heap_idx = self.map.remove_if_full(replace_idx);

        // add the new group
        let id = id.map(|id| id.to_string());
        let map_idx = self.map.insert(hash, id, heap_idx, mapper);
        (map_idx, true)
    }
}

impl<VAL: ArrowPrimitiveType> PrimitiveHashTable<VAL>
where
    Option<<VAL as ArrowPrimitiveType>::Native>: Comparable,
    Option<<VAL as ArrowPrimitiveType>::Native>: HashValue,
{
    pub fn new(limit: usize) -> Self {
        let owned = Arc::new(PrimitiveArray::<VAL>::builder(0).finish());
        Self {
            owned,
            map: TopKHashTable::new(limit, limit * 10),
            rnd: ahash::RandomState::default(),
        }
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

    unsafe fn update_heap_idx(&mut self, mapper: &[(usize, usize)]) {
        self.map.update_heap_idx(mapper);
    }

    unsafe fn heap_idx_at(&self, map_idx: usize) -> usize {
        self.map.heap_idx_at(map_idx)
    }

    fn drain(&mut self) -> (ArrayRef, Vec<usize>) {
        let mut rows = self.map.drain();
        rows.sort_by(|a, b| a.0.comp(&b.0));
        let (ids, heap_idxs): (Vec<_>, Vec<_>) = rows.into_iter().unzip();
        let mut builder: PrimitiveBuilder<VAL> = PrimitiveArray::builder(ids.len());
        for id in ids.into_iter() {
            match id {
                None => builder.append_null(),
                Some(id) => builder.append_value(id),
            }
        }
        let ids = Arc::new(builder.finish());
        (ids, heap_idxs)
    }

    unsafe fn find_or_insert(
        &mut self,
        row_idx: usize,
        replace_idx: usize,
        mapper: &mut Vec<(usize, usize)>,
    ) -> (usize, bool) {
        let ids = self.owned.as_primitive::<VAL>();
        let id: Option<VAL::Native> = if ids.is_null(row_idx) {
            None
        } else {
            Some(ids.value(row_idx))
        };

        let hash: u64 = id.hash(&self.rnd);
        if let Some(map_idx) = self.map.find(hash, |mi| id == *mi) {
            return (map_idx, false);
        }

        // we're full and this is a better value, so remove the worst
        let heap_idx = self.map.remove_if_full(replace_idx);

        // add the new group
        let map_idx = self.map.insert(hash, id, heap_idx, mapper);
        (map_idx, true)
    }
}

impl<ID: KeyType> TopKHashTable<ID> {
    pub fn new(limit: usize, capacity: usize) -> Self {
        Self {
            map: RawTable::with_capacity(capacity),
            limit,
        }
    }

    pub fn find(&self, hash: u64, mut eq: impl FnMut(&ID) -> bool) -> Option<usize> {
        let bucket = self.map.find(hash, |mi| eq(&mi.id))?;
        // JUSTIFICATION
        //  Benefit:  ~15% speedup + required to index into RawTable from binary heap
        //  Soundness: getting the index of a bucket we just found
        let idx = unsafe { self.map.bucket_index(&bucket) };
        Some(idx)
    }

    pub unsafe fn heap_idx_at(&self, map_idx: usize) -> usize {
        let bucket = unsafe { self.map.bucket(map_idx) };
        bucket.as_ref().heap_idx
    }

    pub unsafe fn remove_if_full(&mut self, replace_idx: usize) -> usize {
        if self.map.len() >= self.limit {
            self.map.erase(self.map.bucket(replace_idx));
            0 // if full, always replace top node
        } else {
            self.map.len() // if we're not full, always append to end
        }
    }

    unsafe fn update_heap_idx(&mut self, mapper: &[(usize, usize)]) {
        for (m, h) in mapper {
            self.map.bucket(*m).as_mut().heap_idx = *h
        }
    }

    pub fn insert(
        &mut self,
        hash: u64,
        id: ID,
        heap_idx: usize,
        mapper: &mut Vec<(usize, usize)>,
    ) -> usize {
        let mi = HashTableItem::new(hash, id, heap_idx);
        let bucket = self.map.try_insert_no_grow(hash, mi);
        let bucket = match bucket {
            Ok(bucket) => bucket,
            Err(new_item) => {
                let bucket = self.map.insert(hash, new_item, |mi| mi.hash);
                // JUSTIFICATION
                //  Benefit:  ~15% speedup + required to index into RawTable from binary heap
                //  Soundness: we're getting indexes of buckets, not dereferencing them
                unsafe {
                    for bucket in self.map.iter() {
                        let heap_idx = bucket.as_ref().heap_idx;
                        let map_idx = self.map.bucket_index(&bucket);
                        mapper.push((heap_idx, map_idx));
                    }
                }
                bucket
            }
        };
        // JUSTIFICATION
        //  Benefit:  ~15% speedup + required to index into RawTable from binary heap
        //  Soundness: we're getting indexes of buckets, not dereferencing them
        unsafe { self.map.bucket_index(&bucket) }
    }

    pub fn len(&self) -> usize {
        self.map.len()
    }

    pub fn drain(&mut self) -> Vec<(ID, usize)> {
        self.map.drain().map(|mi| (mi.id, mi.heap_idx)).collect()
    }
}

impl<ID: KeyType> HashTableItem<ID> {
    pub fn new(hash: u64, id: ID, heap_idx: usize) -> Self {
        Self { hash, id, heap_idx }
    }
}

#[cfg(test)]
fn map_print<ID: KeyType>(map: &RawTable<HashTableItem<ID>>) {
    use itertools::Itertools;
    // JUSTIFICATION
    //  Benefit:  ~15% speedup + required to index into RawTable from binary heap
    //  Soundness: iterator is safe as long as we don't hold onto it past this stack frame
    unsafe {
        let mut indexes = vec![];
        for mi in map.iter() {
            let mi = mi.as_ref();
            println!("id={:?} heap_idx={}", mi.id, mi.heap_idx);
            indexes.push(mi.heap_idx);
        }
        let indexes: Vec<_> = indexes.iter().unique().collect();
        if indexes.len() != map.len() {
            panic!("{} indexes and {} keys", indexes.len(), map.len());
        }
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
hash_float!(f16, f32, f64);

pub fn new_hash_table(limit: usize, kt: DataType) -> Result<Box<dyn ArrowHashTable>> {
    macro_rules! downcast_helper {
        ($kt:ty, $d:ident) => {
            return Ok(Box::new(PrimitiveHashTable::<$kt>::new(limit)))
        };
    }

    downcast_primitive! {
        kt => (downcast_helper, kt),
        DataType::Utf8 => return Ok(Box::new(StringHashTable::new(limit))),
        _ => {}
    }

    Err(DataFusionError::Execution(format!(
        "Can't create HashTable for type: {kt:?}"
    )))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::error::Result;

    #[test]
    fn should_resize_properly() -> Result<()> {
        let mut map = TopKHashTable::<Option<String>>::new(5, 3);
        for (idx, id) in vec!["1", "2", "3", "4", "5"].into_iter().enumerate() {
            let mut mapper = vec![];
            map.insert(idx as u64, Some(id.to_string()), idx, &mut mapper);
            if idx == 3 {
                assert_eq!(
                    mapper,
                    vec![(0, 0), (1, 1), (2, 2), (3, 3)],
                    "Pass {idx} resized incorrectly!"
                );
            } else {
                assert_eq!(mapper, vec![], "Pass {idx} resized!");
            }
        }

        let (ids, indexes): (Vec<_>, Vec<_>) = map.drain().into_iter().unzip();
        assert_eq!(
            format!("{:?}", ids),
            r#"[Some("1"), Some("2"), Some("3"), Some("4"), Some("5")]"#
        );
        assert_eq!(indexes, vec![0, 1, 2, 3, 4]);

        Ok(())
    }
}
