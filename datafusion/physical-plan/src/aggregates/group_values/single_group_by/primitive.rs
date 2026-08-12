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
use arrow::array::types::{IntervalDayTime, IntervalMonthDayNano};
use arrow::array::{
    Array, ArrayRef, ArrowNativeTypeOp, ArrowPrimitiveType, NullBufferBuilder,
    PrimitiveArray, cast::AsArray,
};
use arrow::datatypes::{DataType, i256};
use datafusion_common::Result;
use datafusion_common::hash_utils::RandomState;
use datafusion_common::utils::split_vec_min_alloc;
use datafusion_execution::memory_pool::proxy::VecAllocExt;
use datafusion_expr::EmitTo;
use half::f16;
use hashbrown::hash_table::HashTable;
#[cfg(not(feature = "force_hash_collisions"))]
use std::hash::BuildHasher;
use std::mem::size_of;
use std::sync::Arc;

/// A trait to allow hashing of floating point numbers
pub trait HashValue {
    fn hash(&self, state: &RandomState) -> u64;

    /// Return a canonical representative whose bit pattern is identical for
    /// all values that should be grouped together. Default is the identity;
    /// floats override this to fold `-0.0` into `+0.0` so the bit-equal
    /// `is_eq` check used during insertion treats them as the same group.
    /// NaN payload bits are preserved.
    #[inline]
    fn canonicalize(self) -> Self
    where
        Self: Sized,
    {
        self
    }
}

macro_rules! hash_integer {
    ($($t:ty),+) => {
        $(impl HashValue for $t {
            #[cfg(not(feature = "force_hash_collisions"))]
            fn hash(&self, state: &RandomState) -> u64 {
                state.hash_one(self)
            }

            #[cfg(feature = "force_hash_collisions")]
            fn hash(&self, _state: &RandomState) -> u64 {
                0
            }
        })+
    };
}
hash_integer!(i8, i16, i32, i64, i128, i256);
hash_integer!(u8, u16, u32, u64);
hash_integer!(IntervalDayTime, IntervalMonthDayNano);

macro_rules! hash_float {
    ($($t:ty),+) => {
        $(impl HashValue for $t {
            #[cfg(not(feature = "force_hash_collisions"))]
            fn hash(&self, state: &RandomState) -> u64 {
                state.hash_one(self.canonicalize().to_bits())
            }

            #[cfg(feature = "force_hash_collisions")]
            fn hash(&self, _state: &RandomState) -> u64 {
                0
            }

            #[inline]
            fn canonicalize(self) -> Self {
                let bits = self.to_bits();
                let bits = if bits << 1 == 0 { 0 } else { bits };
                Self::from_bits(bits)
            }
        })+
    };
}

hash_float!(f16, f32, f64);

/// A trait to allow direct mapped ("dense") lookup of integer like values
///
/// Values that map to a dense key can be interned by indexing a vector of
/// group indices with `key - min`, which avoids hashing entirely
pub trait DenseKey: Copy {
    /// The dense key of this value, or `None` if this type does not support
    /// direct mapped lookup
    ///
    /// This is `i128` so that the range of every supported type fits without
    /// overflow (`i64::MIN..i64::MAX` as well as `0..u64::MAX`). It is only
    /// used to size the table, never per row.
    fn dense_key(self) -> Option<i128>;

    /// The value mapped into `u64` such that the order of, and distance
    /// between, values is preserved
    ///
    /// Slot lookup is `biased(value) - biased(min)`, which is a single
    /// wrapping subtraction: values below `min` wrap around to a large number
    /// and are rejected by the same bounds check that catches values above the
    /// end of the table.
    fn biased(self) -> u64;
}

macro_rules! dense_key_signed {
    ($($t:ty),+) => {
        $(impl DenseKey for $t {
            #[inline]
            fn dense_key(self) -> Option<i128> {
                Some(self as i128)
            }

            #[inline]
            fn biased(self) -> u64 {
                (self as i64 as u64) ^ (1 << 63)
            }
        })+
    };
}
dense_key_signed!(i8, i16, i32, i64);

macro_rules! dense_key_unsigned {
    ($($t:ty),+) => {
        $(impl DenseKey for $t {
            #[inline]
            fn dense_key(self) -> Option<i128> {
                Some(self as i128)
            }

            #[inline]
            fn biased(self) -> u64 {
                self as u64
            }
        })+
    };
}
dense_key_unsigned!(u8, u16, u32, u64);

macro_rules! dense_key_unsupported {
    ($($t:ty),+) => {
        $(impl DenseKey for $t {
            #[inline]
            fn dense_key(self) -> Option<i128> {
                None
            }

            #[inline]
            fn biased(self) -> u64 {
                0
            }
        })+
    };
}
dense_key_unsupported!(i128, i256, IntervalDayTime, IntervalMonthDayNano);
dense_key_unsupported!(f16, f32, f64);

/// Marks an unused slot in the direct mapped table
const DENSE_EMPTY: u32 = u32::MAX;

/// The most slots a direct mapped table may have (8MiB at 4 bytes per slot).
/// Ranges wider than this fall back to hashing
const DENSE_MAX_SLOTS: usize = 2 * 1024 * 1024;

/// A direct mapped table larger than [`DENSE_SMALL_SLOTS`] is only built once
/// the groups fill at least this fraction (1/8) of the range they span, so
/// that sparse values are not given a mostly empty slot each
const DENSE_MIN_FILL_DENOM: usize = 8;

/// A direct mapped table of at most this many slots (256KiB at 4 bytes each)
/// is built whatever the fill rate: even entirely empty it wastes little, and
/// waiting for a fill rate would give up the win on small group by keys
const DENSE_SMALL_SLOTS: usize = 64 * 1024;

/// How the group index of each non null value is looked up
enum GroupStore {
    /// Stores the `(group_index, hash)` based on the hash of its value
    ///
    /// We also store `hash` is for reducing cost of rehashing. Such cost
    /// is obvious in high cardinality group by situation.
    /// More details can see:
    /// <https://github.com/apache/datafusion/issues/15961>
    Hash(HashTable<(usize, u64)>),
    /// Direct mapped lookup, where `group_ids[biased(value) - min_biased]` is
    /// the group index of `value`, or [`DENSE_EMPTY`] if it has not been seen
    ///
    /// `min` is the same value as `min_biased`, kept as an `i128` for the
    /// range arithmetic done once per batch
    Dense {
        min: i128,
        min_biased: u64,
        group_ids: Vec<u32>,
    },
}

/// The result of interning a batch with the direct mapped table
enum DenseOutcome {
    /// Every value of the batch was interned
    Interned,
    /// A value fell outside the range covered by the table
    OutOfRange,
}

/// The range of dense keys seen, and the biased form (see [`DenseKey::biased`])
/// of its minimum, which is what slot lookup subtracts
#[derive(Clone, Copy)]
struct DenseRange {
    min: i128,
    max: i128,
    min_biased: u64,
}

impl DenseRange {
    /// Widen the range to also cover `key`, whose biased form is `biased`
    fn extend(self, key: i128, biased: u64) -> Self {
        if key < self.min {
            Self {
                min: key,
                max: self.max,
                min_biased: biased,
            }
        } else {
            Self {
                max: self.max.max(key),
                ..self
            }
        }
    }

    /// Widen the range to also cover `other`
    fn merge(self, other: Self) -> Self {
        let (min, min_biased) = if other.min < self.min {
            (other.min, other.min_biased)
        } else {
            (self.min, self.min_biased)
        };
        Self {
            min,
            max: self.max.max(other.max),
            min_biased,
        }
    }

    /// Number of slots needed to cover the range
    fn len(self) -> Option<usize> {
        usize::try_from(self.max.checked_sub(self.min)?.checked_add(1)?).ok()
    }
}

/// A [`GroupValues`] storing a single column of primitive values
///
/// This specialization is significantly faster than using the more general
/// purpose `Row`s format
pub struct GroupValuesPrimitive<T: ArrowPrimitiveType> {
    /// The data type of the output array
    data_type: DataType,
    /// How the group index of each non null value is looked up
    store: GroupStore,
    /// The group index of the null value if any
    null_group: Option<usize>,
    /// The values for each group index
    values: Vec<T::Native>,
    /// The random state used to generate hashes
    random_state: RandomState,
    /// Set once the values have been found not to be dense, so that the
    /// direct mapped table is not tried again
    dense_disabled: bool,
    /// The range of dense keys seen so far, used to decide whether a direct
    /// mapped table is worth building
    observed_range: Option<DenseRange>,
}

impl<T: ArrowPrimitiveType> GroupValuesPrimitive<T> {
    pub fn new(data_type: DataType) -> Self {
        assert!(PrimitiveArray::<T>::is_compatible(&data_type));
        Self {
            data_type,
            store: GroupStore::Hash(HashTable::with_capacity(128)),
            values: Vec::with_capacity(128),
            null_group: None,
            random_state: crate::aggregates::AGGREGATION_HASH_SEED,
            dense_disabled: false,
            observed_range: None,
        }
    }
}

impl<T: ArrowPrimitiveType> GroupValuesPrimitive<T>
where
    T::Native: HashValue + DenseKey,
{
    /// The range of dense keys in `array`, or `None` if the values do not
    /// support direct mapped lookup or are all null
    fn dense_range(array: &PrimitiveArray<T>) -> Option<DenseRange> {
        let mut range: Option<DenseRange> = None;
        for value in array.iter().flatten() {
            let key = value.dense_key()?;
            range = Some(match range {
                Some(seen) => seen.extend(key, value.biased()),
                None => DenseRange {
                    min: key,
                    max: key,
                    min_biased: value.biased(),
                },
            });
        }
        range
    }

    /// Migrate the groups interned so far into a direct mapped table, if the
    /// values seen so far are dense enough to be worth a slot each
    ///
    /// The decision is deliberately not made from the first batch alone: a
    /// batch of a dense column looks sparse simply because it only holds a
    /// fraction of the column's values, and a batch of a sparse column can
    /// look narrow enough to be worth it. Waiting until enough groups have
    /// accumulated makes the fill rate meaningful, and costs only the range
    /// tracking done while hashing.
    fn try_migrate_to_dense(&mut self) {
        debug_assert!(!self.dense_disabled);
        let Some(range) = self.observed_range else {
            return;
        };
        let Some(len) = range.len().filter(|len| *len <= DENSE_MAX_SLOTS) else {
            // Too wide a range to be worth a slot per value. It can only get
            // wider, so stop considering it
            self.dense_disabled = true;
            return;
        };

        // A small table is always worth it. A larger one only once the values
        // fill enough of the range they span; until then keep hashing, since
        // the groups seen so far may just be a fraction of a dense column.
        let groups = self.values.len() - usize::from(self.null_group.is_some());
        if len > DENSE_SMALL_SLOTS && groups * DENSE_MIN_FILL_DENOM < len {
            return;
        }

        let mut group_ids = vec![DENSE_EMPTY; len];
        for (group_idx, &value) in self.values.iter().enumerate() {
            if Some(group_idx) == self.null_group {
                continue;
            }
            let offset = value.biased().wrapping_sub(range.min_biased);
            debug_assert!(offset < len as u64);
            group_ids[offset as usize] = group_idx as u32;
        }

        self.store = GroupStore::Dense {
            min: range.min,
            min_biased: range.min_biased,
            group_ids,
        };
    }

    /// Track the range of dense keys seen, which decides whether a direct
    /// mapped table is worth building
    fn observe_range(&mut self, array: &PrimitiveArray<T>) {
        if self.dense_disabled {
            return;
        }
        let Some(range) = Self::dense_range(array) else {
            // All null, or a type that cannot be direct mapped
            if array.null_count() != array.len() {
                self.dense_disabled = true;
            }
            return;
        };
        self.observed_range = Some(match self.observed_range {
            Some(seen) => seen.merge(range),
            None => range,
        });
    }

    /// Grow the direct mapped table so that it also covers `array`, returning
    /// false if that would need more than [`DENSE_MAX_SLOTS`] slots
    fn grow_dense(&mut self, array: &PrimitiveArray<T>) -> bool {
        let groups = self.values.len() - usize::from(self.null_group.is_some());
        let GroupStore::Dense {
            min,
            min_biased,
            group_ids,
        } = &mut self.store
        else {
            return false;
        };
        let Some(batch) = Self::dense_range(array) else {
            return false;
        };

        // The range the table covers today, widened to also cover the batch
        let Some(covered_max) = (*min)
            .checked_add(group_ids.len() as i128)
            .map(|end| end - 1)
        else {
            return false;
        };
        let grown = DenseRange {
            min: *min,
            max: covered_max,
            min_biased: *min_biased,
        }
        .merge(batch);
        let Some(len) = grown.len().filter(|len| *len <= DENSE_MAX_SLOTS) else {
            return false;
        };
        let new_min = grown.min;
        // Growing must not leave the table mostly empty
        if len > DENSE_SMALL_SLOTS && groups * DENSE_MIN_FILL_DENOM < len {
            return false;
        }

        if new_min < *min {
            // Values below the current range: shift the existing slots up
            let prefix = (*min - new_min) as usize;
            let mut shifted = vec![DENSE_EMPTY; len];
            shifted[prefix..prefix + group_ids.len()].copy_from_slice(group_ids);
            *group_ids = shifted;
            *min = new_min;
            *min_biased = grown.min_biased;
        } else {
            group_ids.resize(len, DENSE_EMPTY);
        }
        true
    }

    /// Intern `array` with the direct mapped table
    ///
    /// Interning is idempotent, so a batch that reports [`DenseOutcome::OutOfRange`]
    /// can simply be interned again once the table covers it
    fn intern_dense(
        &mut self,
        array: &PrimitiveArray<T>,
        groups: &mut Vec<usize>,
    ) -> DenseOutcome {
        let Self {
            store,
            values,
            null_group,
            ..
        } = self;
        let GroupStore::Dense {
            min_biased,
            group_ids,
            ..
        } = store
        else {
            return DenseOutcome::OutOfRange;
        };
        let min_biased = *min_biased;
        let len = group_ids.len() as u64;

        for v in array {
            let group_id = match v {
                None => *null_group.get_or_insert_with(|| {
                    let group_id = values.len();
                    values.push(Default::default());
                    group_id
                }),
                Some(key) => {
                    let offset = key.biased().wrapping_sub(min_biased);
                    if offset >= len {
                        return DenseOutcome::OutOfRange;
                    }

                    let slot = &mut group_ids[offset as usize];
                    if *slot == DENSE_EMPTY {
                        let group_id = values.len();
                        *slot = group_id as u32;
                        values.push(key);
                        group_id
                    } else {
                        *slot as usize
                    }
                }
            };
            groups.push(group_id);
        }

        DenseOutcome::Interned
    }

    /// Rebuild the group values as a hash table, which is possible at any
    /// point because [`Self::values`] holds every group value in group index
    /// order
    fn convert_dense_to_hash(&mut self) {
        let state = &self.random_state;
        let mut map = HashTable::with_capacity(self.values.len());
        for (group_idx, &value) in self.values.iter().enumerate() {
            if Some(group_idx) == self.null_group {
                continue;
            }
            let hash = value.hash(state);
            map.insert_unique(hash, (group_idx, hash), |&(_, hash)| hash);
        }
        self.store = GroupStore::Hash(map);
    }

    fn intern_hash(&mut self, array: &PrimitiveArray<T>, groups: &mut Vec<usize>) {
        let Self {
            store,
            values,
            null_group,
            random_state,
            ..
        } = self;
        let GroupStore::Hash(map) = store else {
            unreachable!("group values are not stored in a hash table")
        };

        for v in array {
            let group_id = match v {
                None => *null_group.get_or_insert_with(|| {
                    let group_id = values.len();
                    values.push(Default::default());
                    group_id
                }),
                Some(key) => {
                    // Fold equivalence-class duplicates (e.g. `-0.0` → `+0.0`)
                    // so the bit-equal `is_eq` matches and the stored value is
                    // the canonical representative.
                    let key = key.canonicalize();
                    let hash = key.hash(random_state);
                    let insert = map.entry(
                        hash,
                        |&(g, h)| unsafe {
                            hash == h && values.get_unchecked(g).is_eq(key)
                        },
                        |&(_, h)| h,
                    );

                    match insert {
                        hashbrown::hash_table::Entry::Occupied(o) => o.get().0,
                        hashbrown::hash_table::Entry::Vacant(v) => {
                            let g = values.len();
                            v.insert((g, hash));
                            values.push(key);
                            g
                        }
                    }
                }
            };
            groups.push(group_id)
        }
    }
}

impl<T: ArrowPrimitiveType> GroupValues for GroupValuesPrimitive<T>
where
    T::Native: HashValue + DenseKey,
{
    fn intern(&mut self, cols: &[ArrayRef], groups: &mut Vec<usize>) -> Result<()> {
        assert_eq!(cols.len(), 1);
        let array = cols[0].as_primitive::<T>();
        groups.clear();

        // Only while hashing: once the values are direct mapped the range is
        // maintained by the table itself, and once they are known not to be
        // dense there is nothing left to decide
        if !self.dense_disabled && matches!(self.store, GroupStore::Hash(_)) {
            self.observe_range(array);
            self.try_migrate_to_dense();
        }

        if matches!(self.store, GroupStore::Dense { .. }) {
            if let DenseOutcome::Interned = self.intern_dense(array, groups) {
                return Ok(());
            }

            // Values outside the range the table covers: grow it if that is
            // still worthwhile, otherwise fall back to hashing for good
            groups.clear();
            if self.grow_dense(array) {
                if let DenseOutcome::Interned = self.intern_dense(array, groups) {
                    return Ok(());
                }
                groups.clear();
            }
            self.convert_dense_to_hash();
            self.dense_disabled = true;
        }

        self.intern_hash(array, groups);
        Ok(())
    }

    fn size(&self) -> usize {
        let store = match &self.store {
            GroupStore::Hash(map) => map.capacity() * size_of::<(usize, u64)>(),
            GroupStore::Dense { group_ids, .. } => {
                group_ids.capacity() * size_of::<u32>()
            }
        };
        store + self.values.allocated_size()
    }

    fn is_empty(&self) -> bool {
        self.values.is_empty()
    }

    fn len(&self) -> usize {
        self.values.len()
    }

    fn emit(&mut self, emit_to: EmitTo) -> Result<Vec<ArrayRef>> {
        fn build_primitive<T: ArrowPrimitiveType>(
            values: Vec<T::Native>,
            null_idx: Option<usize>,
        ) -> PrimitiveArray<T> {
            let nulls = null_idx.map(|null_idx| {
                let mut buffer = NullBufferBuilder::new(values.len());
                buffer.append_n_non_nulls(null_idx);
                buffer.append_null();
                buffer.append_n_non_nulls(values.len() - null_idx - 1);
                // NOTE: The inner builder must be constructed as there is at least one null
                buffer.finish().unwrap()
            });
            PrimitiveArray::<T>::new(values.into(), nulls)
        }

        let array: PrimitiveArray<T> = match emit_to {
            EmitTo::All => {
                self.store = GroupStore::Hash(HashTable::with_capacity(0));
                self.observed_range = None;
                build_primitive(std::mem::take(&mut self.values), self.null_group.take())
            }
            EmitTo::First(n) => {
                match &mut self.store {
                    GroupStore::Hash(map) => {
                        map.retain(|entry| {
                            // Decrement group index by n
                            let group_idx = entry.0;
                            match group_idx.checked_sub(n) {
                                // Group index was >= n, shift value down
                                Some(sub) => {
                                    entry.0 = sub;
                                    true
                                }
                                // Group index was < n, so remove from table
                                None => false,
                            }
                        });
                    }
                    GroupStore::Dense { group_ids, .. } => {
                        for slot in group_ids.iter_mut() {
                            if *slot == DENSE_EMPTY {
                                continue;
                            }
                            match (*slot as usize).checked_sub(n) {
                                // Group index was >= n, shift value down
                                Some(sub) => *slot = sub as u32,
                                // Group index was < n, so free the slot
                                None => *slot = DENSE_EMPTY,
                            }
                        }
                    }
                }
                let null_group = match &mut self.null_group {
                    Some(v) if *v >= n => {
                        *v -= n;
                        None
                    }
                    Some(_) => self.null_group.take(),
                    None => None,
                };
                build_primitive(split_vec_min_alloc(&mut self.values, n), null_group)
            }
        };

        Ok(vec![Arc::new(array.with_data_type(self.data_type.clone()))])
    }

    fn clear_shrink(&mut self, num_rows: usize) {
        self.values.clear();
        self.values.shrink_to(num_rows);
        self.null_group = None;
        self.observed_range = None;
        match &mut self.store {
            GroupStore::Hash(map) => {
                map.clear();
                map.shrink_to(num_rows, |_| 0); // hasher does not matter since the map is cleared
            }
            GroupStore::Dense { .. } => {
                self.store = GroupStore::Hash(HashTable::with_capacity(num_rows));
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Array, Int64Array};
    use arrow::datatypes::Int64Type;

    fn is_dense(gv: &GroupValuesPrimitive<Int64Type>) -> bool {
        matches!(gv.store, GroupStore::Dense { .. })
    }

    fn array(values: &[Option<i64>]) -> ArrayRef {
        Arc::new(Int64Array::from(values.to_vec())) as ArrayRef
    }

    /// State of the group values at the point everything was interned, before
    /// emitting drains it
    struct Interned {
        dense: bool,
        num_groups: usize,
    }

    /// Intern every batch, then emit, and check that every input row maps to a
    /// group holding exactly that row's value. This holds no matter which
    /// store the values ended up in.
    fn check_intern(batches: &[Vec<Option<i64>>]) -> Interned {
        let mut gv = GroupValuesPrimitive::<Int64Type>::new(DataType::Int64);
        let mut all_rows = vec![];
        let mut all_groups = vec![];
        let mut groups = vec![];

        for batch in batches {
            gv.intern(&[array(batch)], &mut groups).unwrap();
            assert_eq!(groups.len(), batch.len());
            all_rows.extend(batch.iter().copied());
            all_groups.extend(groups.iter().copied());
        }

        let interned = Interned {
            dense: is_dense(&gv),
            num_groups: gv.len(),
        };

        let emitted = gv.emit(EmitTo::All).unwrap();
        let emitted = emitted[0].as_primitive::<Int64Type>();
        assert_eq!(emitted.len(), all_groups.iter().max().map_or(0, |m| m + 1));

        for (row, (value, group)) in all_rows.iter().zip(&all_groups).enumerate() {
            let got = (!emitted.is_null(*group)).then(|| emitted.value(*group));
            assert_eq!(
                got, *value,
                "row {row} interned to group {group} holding {got:?}, expected {value:?}"
            );
        }

        // The same value must always map to the same group
        let mut seen = std::collections::HashMap::new();
        for (value, group) in all_rows.iter().zip(&all_groups) {
            let first = seen.entry(*value).or_insert(*group);
            assert_eq!(first, group, "value {value:?} mapped to two groups");
        }

        interned
    }

    #[test]
    fn dense_keys_use_direct_mapped_table() {
        assert!(check_intern(&[vec![Some(10), Some(11), Some(12), Some(10)]]).dense);
    }

    #[test]
    fn dense_table_grows_for_ascending_keys() {
        // Each batch extends the range upwards, so the table has to grow
        let batches = (0..8)
            .map(|batch| (0..1000).map(|v| Some(batch * 1000 + v)).collect())
            .collect::<Vec<_>>();
        let interned = check_intern(&batches);
        assert!(interned.dense);
        assert_eq!(interned.num_groups, 8000);
    }

    #[test]
    fn dense_table_grows_downwards_for_descending_keys() {
        let batches = (0..8)
            .map(|batch| (0..1000).map(|v| Some(-(batch * 1000 + v))).collect())
            .collect::<Vec<_>>();
        let interned = check_intern(&batches);
        assert!(interned.dense);
        assert_eq!(interned.num_groups, 8000);
    }

    /// Values spread thinly over a wide range are not worth a slot each
    #[test]
    fn sparse_values_keep_hashing() {
        let batches = (0..8)
            .map(|batch| {
                (0..1000)
                    .map(|v| Some((batch * 1000 + v) * 64))
                    .collect::<Vec<_>>()
            })
            .collect::<Vec<_>>();
        let interned = check_intern(&batches);
        assert!(!interned.dense);
        assert_eq!(interned.num_groups, 8000);
    }

    /// A column whose values only look sparse until enough of them have been
    /// seen still ends up direct mapped
    #[test]
    fn values_dense_over_a_wide_range_migrate() {
        // 100k values scattered over a 100k range, 1000 at a time, so that no
        // single batch fills much of the range
        let batches = (0..100)
            .map(|batch| {
                (0..1000)
                    .map(|v| Some((v * 100 + batch) % 100_000))
                    .collect::<Vec<_>>()
            })
            .collect::<Vec<_>>();
        let interned = check_intern(&batches);
        assert!(interned.dense);
        assert_eq!(interned.num_groups, 100_000);
    }

    #[test]
    fn wide_range_falls_back_to_hashing() {
        assert!(!check_intern(&[vec![Some(0), Some(i64::MAX), Some(i64::MIN)]]).dense);
    }

    /// A batch that starts dense but later sees a value far outside the range
    /// must fall back without losing or renumbering the groups it handed out
    #[test]
    fn fallback_after_dense_start_keeps_groups() {
        let interned = check_intern(&[
            vec![Some(5), Some(6), Some(7)],
            vec![Some(6), Some(50_000_000), Some(5)],
        ]);
        assert!(!interned.dense);
        assert_eq!(interned.num_groups, 4);
    }

    #[test]
    fn nulls_get_their_own_group_in_dense_mode() {
        let interned = check_intern(&[vec![None, Some(3), None, Some(4), Some(3)]]);
        assert!(interned.dense);
        assert_eq!(interned.num_groups, 3);
    }

    #[test]
    fn emit_first_reindexes_dense_groups() -> Result<()> {
        let mut gv = GroupValuesPrimitive::<Int64Type>::new(DataType::Int64);
        let mut groups = vec![];

        gv.intern(&[array(&[Some(10), None, Some(11), Some(12)])], &mut groups)?;
        assert_eq!(groups, vec![0, 1, 2, 3]);
        assert!(is_dense(&gv));

        let emitted = gv.emit(EmitTo::First(2))?;
        let emitted = emitted[0].as_primitive::<Int64Type>();
        assert_eq!(emitted.len(), 2);
        assert_eq!(emitted.value(0), 10);
        assert!(emitted.is_null(1));
        assert_eq!(gv.len(), 2);

        // 11 and 12 keep their (shifted) groups, 10 was emitted so it is new
        gv.intern(&[array(&[Some(11), Some(12), Some(10)])], &mut groups)?;
        assert_eq!(groups, vec![0, 1, 2]);

        let emitted = gv.emit(EmitTo::All)?;
        let emitted = emitted[0].as_primitive::<Int64Type>();
        assert_eq!(emitted.values(), &[11, 12, 10]);

        Ok(())
    }

    #[test]
    fn clear_shrink_resets_dense_table() -> Result<()> {
        let mut gv = GroupValuesPrimitive::<Int64Type>::new(DataType::Int64);
        let mut groups = vec![];

        gv.intern(&[array(&[None, Some(1)])], &mut groups)?;
        gv.clear_shrink(0);
        assert!(gv.is_empty());

        gv.intern(&[array(&[Some(5), None])], &mut groups)?;
        assert_eq!(groups, vec![0, 1]);

        let emitted = gv.emit(EmitTo::All)?;
        let emitted = emitted[0].as_primitive::<Int64Type>();
        assert_eq!(emitted.value(0), 5);
        assert!(emitted.is_null(1));

        Ok(())
    }
}
