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

//! Immutable set for fixed-width values.

use std::hash::{BuildHasher, Hash};

use datafusion_common::{Result, exec_datafusion_err};
use hashbrown::{DefaultHashBuilder, HashTable};

/// Computes the keyed hash used to place values in a [`FrozenSet`].
pub(super) trait FrozenSetHash<V> {
    fn hash_one(&self, value: V) -> u64;
}

impl<V: Hash> FrozenSetHash<V> for DefaultHashBuilder {
    #[inline(always)]
    fn hash_one(&self, value: V) -> u64 {
        BuildHasher::hash_one(self, value)
    }
}

/// Immutable set optimized for repeated membership tests.
///
/// Each hash bucket holds two values directly. Values that collide beyond those
/// two slots use a `HashTable`, which keeps the common lookup path to one hash
/// and two bucket comparisons without sacrificing collision handling.
///
/// The first member is used as the empty-slot sentinel and handled before
/// lookup, so primary slots store `V` directly without an `Option<V>` wrapper.
pub(super) struct FrozenSet<V, H = DefaultHashBuilder> {
    hash_builder: H,
    sentinel: Option<V>,
    buckets: Box<[[V; 2]]>,
    overflowed: Box<[bool]>,
    overflow: HashTable<V>,
}

impl<V> FrozenSet<V>
where
    V: Copy + Eq + Hash,
{
    pub(super) fn try_new(values: &[V]) -> Result<Self> {
        Self::try_new_with_hasher(values, DefaultHashBuilder::default())
    }
}

impl<V, H> FrozenSet<V, H>
where
    V: Copy + Eq,
    H: FrozenSetHash<V>,
{
    pub(super) fn try_new_with_hasher(values: &[V], hash_builder: H) -> Result<Self> {
        let Some((&sentinel, values)) = values.split_first() else {
            return Ok(Self {
                hash_builder,
                sentinel: None,
                buckets: Box::default(),
                overflowed: Box::default(),
                overflow: HashTable::new(),
            });
        };

        if values.is_empty() {
            return Ok(Self {
                hash_builder,
                sentinel: Some(sentinel),
                buckets: Box::default(),
                overflowed: Box::default(),
                overflow: HashTable::new(),
            });
        }

        let bucket_count = values
            .len()
            .checked_add(1)
            .ok_or_else(|| exec_datafusion_err!("FrozenSet capacity overflow"))?;
        bucket_count
            .checked_mul(2)
            .ok_or_else(|| exec_datafusion_err!("FrozenSet capacity overflow"))?;
        let mut set = Self {
            hash_builder,
            sentinel: Some(sentinel),
            buckets: vec![[sentinel; 2]; bucket_count].into_boxed_slice(),
            overflowed: vec![false; bucket_count].into_boxed_slice(),
            overflow: HashTable::with_capacity(bucket_count / 8),
        };
        values.iter().copied().for_each(|value| set.insert(value));
        Ok(set)
    }

    fn insert(&mut self, value: V) {
        let sentinel = self.sentinel.expect("non-empty frozen set");
        if value == sentinel {
            return;
        }

        let hash = self.hash_builder.hash_one(value);
        let bucket = reduce_hash(hash, self.buckets.len());
        let slots = &mut self.buckets[bucket];
        if slots[0] == sentinel {
            slots[0] = value;
            return;
        }
        if slots[0] == value {
            return;
        }
        if slots[1] == sentinel {
            slots[1] = value;
            return;
        }
        if slots[1] == value {
            return;
        }

        let hash_builder = &self.hash_builder;
        self.overflow
            .entry(
                hash,
                |stored| *stored == value,
                |stored| hash_builder.hash_one(*stored),
            )
            .or_insert(value);
        self.overflowed[bucket] = true;
    }

    #[inline(always)]
    pub(super) fn contains(&self, value: V) -> bool {
        let Some(sentinel) = self.sentinel else {
            return false;
        };
        if value == sentinel {
            return true;
        }
        if self.buckets.is_empty() {
            return false;
        }

        let hash = self.hash_builder.hash_one(value);
        let bucket = reduce_hash(hash, self.buckets.len());
        // SAFETY: `reduce_hash` returns an index in `0..buckets.len()`.
        let slots = unsafe { self.buckets.get_unchecked(bucket) };
        if (slots[0] == value) | (slots[1] == value) {
            return true;
        }

        // SAFETY: `overflowed` has exactly one entry per bucket.
        (unsafe { *self.overflowed.get_unchecked(bucket) })
            && self
                .overflow
                .find(hash, |stored| *stored == value)
                .is_some()
    }
}

#[inline(always)]
fn reduce_hash(hash: u64, len: usize) -> usize {
    #[cfg(target_pointer_width = "64")]
    return ((hash as u128 * len as u128) >> 64) as usize;

    #[cfg(target_pointer_width = "32")]
    return (((hash as u32) as u64 * len as u64) >> 32) as usize;
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::hash::{Hash, Hasher};

    #[derive(Clone, Copy, Debug, Eq, PartialEq)]
    struct Colliding(u64);

    struct ConstantHash;

    impl FrozenSetHash<u64> for ConstantHash {
        fn hash_one(&self, _value: u64) -> u64 {
            0
        }
    }

    impl Hash for Colliding {
        fn hash<H: Hasher>(&self, state: &mut H) {
            0_u8.hash(state);
        }
    }

    #[test]
    fn handles_empty_duplicates_and_sentinel_member() {
        let empty = FrozenSet::<u64>::try_new(&[]).unwrap();
        assert!(!empty.contains(0));

        let singleton = FrozenSet::try_new(&[42]).unwrap();
        assert!(singleton.contains(42));
        assert!(!singleton.contains(7));

        let values = [42, 7, 42, 9, 11, 7];
        let set = FrozenSet::try_new(&values).unwrap();
        for value in [42, 7, 9, 11] {
            assert!(set.contains(value));
        }
        for value in [0, 8, 10, 12] {
            assert!(!set.contains(value));
        }
    }

    #[test]
    fn handles_many_values() {
        let values = (0_u64..10_000).collect::<Vec<_>>();
        let set = FrozenSet::try_new(&values).unwrap();
        assert!(values.iter().all(|&value| set.contains(value)));
        assert!((10_000..20_000).all(|value| !set.contains(value)));
    }

    #[test]
    fn handles_u128_values() {
        let values = [1_u128, (1_u128 << 64) | 1, (2_u128 << 96) | 7, u128::MAX];
        let set = FrozenSet::try_new(&values).unwrap();
        assert!(values.iter().all(|&value| set.contains(value)));
        assert!(!set.contains(1_u128 << 96));
    }

    #[test]
    fn handles_collisions() {
        let values = (0..128).map(Colliding).collect::<Vec<_>>();
        let set = FrozenSet::try_new(&values).unwrap();
        assert!(values.iter().all(|&value| set.contains(value)));
        assert!((128..256).all(|value| !set.contains(Colliding(value))));
    }

    #[test]
    fn handles_custom_hash_collisions() {
        let values = (0_u64..128).collect::<Vec<_>>();
        let set = FrozenSet::try_new_with_hasher(&values, ConstantHash).unwrap();
        assert!(values.iter().all(|&value| set.contains(value)));
        assert!((128..256).all(|value| !set.contains(value)));
    }
}
