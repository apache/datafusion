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
    ArrayRef, ArrowNativeTypeOp, ArrowPrimitiveType, NullBufferBuilder, PrimitiveArray,
    cast::AsArray,
};
use arrow::datatypes::{ArrowNativeType, DataType, i256};
use datafusion_common::Result;
use datafusion_common::hash_utils::RandomState;
use datafusion_common::utils::split_vec_min_alloc;
use datafusion_execution::memory_pool::proxy::VecAllocExt;
use datafusion_expr::{EmitTo, GroupSelection};
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

/// A trait to allow direct mapped ("dense") lookup of integer values, by
/// indexing a vector of group indices with `value - min` instead of hashing.
/// The join side maps keys the same way, see `joins::array_map::ArrayMap`
pub trait DenseKey: Copy {
    /// Whether this type can be direct mapped at all
    const DENSE: bool;

    /// The value as a `u64`, a plain cast for the supported types
    ///
    /// Slot lookup is the wrapping `index(value) - index(min)`, so values
    /// below `min` wrap to a large number and are rejected by the same bounds
    /// check as values above the table. The cast preserves distances, so this
    /// holds for signed values too.
    fn index(self) -> u64;
}

macro_rules! dense_key {
    ($($t:ty),+) => {
        $(impl DenseKey for $t {
            const DENSE: bool = true;

            #[inline]
            fn index(self) -> u64 {
                self as u64
            }
        })+
    };
}
dense_key!(i8, i16, i32, i64, u8, u16, u32, u64);

/// Types too wide, or not integers, to be direct mapped
macro_rules! sparse_key {
    ($($t:ty),+) => {
        $(impl DenseKey for $t {
            const DENSE: bool = false;

            #[inline]
            fn index(self) -> u64 {
                0
            }
        })+
    };
}
sparse_key!(i128, i256, f16, f32, f64);
sparse_key!(IntervalDayTime, IntervalMonthDayNano);

/// Marks an unused slot in the direct mapped table
const DENSE_EMPTY: u32 = u32::MAX;

/// Memory budget for one table: 2M slots, 8MiB at 4 bytes each. Wider ranges
/// are hashed instead
const DENSE_MAX_SLOTS: usize = 2 * 1024 * 1024;

/// Minimum fill (1/8) of its range for a table above [`DENSE_SMALL_SLOTS`], so
/// that sparse values are not given a mostly empty slot each. At 4 bytes a slot
/// against 16 a bucket this is also where it stops using less memory than the
/// hash table it replaces. The join side admits keys the same way, see
/// `perfect_hash_join_min_key_density`
const DENSE_MIN_FILL_DENOM: usize = 8;

/// Tables this small (256KiB) are built whatever the fill rate: even empty they
/// waste little
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
    /// Direct mapped lookup, where `group_ids[index(value) - min]` is the
    /// group index of `value`, or [`DENSE_EMPTY`] if it has not been seen
    Dense { min: u64, group_ids: Vec<u32> },
}

/// The range of values seen, which decides whether a direct mapped table is
/// worth building
#[derive(Clone, Copy)]
struct DenseRange<N> {
    min: N,
    max: N,
}

impl<N: ArrowNativeTypeOp + DenseKey> DenseRange<N> {
    /// Widen the range to also cover `value`
    fn extend(&mut self, value: N) {
        if value.is_lt(self.min) {
            self.min = value;
        } else if value.is_gt(self.max) {
            self.max = value;
        }
    }

    /// Number of slots needed to cover the range
    fn len(self) -> Option<usize> {
        let span = self.max.index().wrapping_sub(self.min.index());
        usize::try_from(span as u128 + 1).ok()
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
    /// Set once the values are known not to be dense, so that the direct
    /// mapped table is not tried again until the groups are drained
    dense_disabled: bool,
    /// The range of the group values, maintained while hashing
    observed_range: Option<DenseRange<T::Native>>,
}

impl<T: ArrowPrimitiveType> GroupValuesPrimitive<T>
where
    T::Native: HashValue + DenseKey,
{
    pub fn new(data_type: DataType) -> Self {
        assert!(PrimitiveArray::<T>::is_compatible(&data_type));
        Self {
            data_type,
            store: GroupStore::Hash(HashTable::with_capacity(128)),
            values: Vec::with_capacity(128),
            null_group: None,
            random_state: crate::aggregates::AGGREGATION_HASH_SEED,
            dense_disabled: !T::Native::DENSE,
            observed_range: None,
        }
    }
}

impl<T: ArrowPrimitiveType> GroupValuesPrimitive<T>
where
    T::Native: HashValue + DenseKey,
{
    /// Number of groups holding a value, i.e. excluding the null group
    fn value_groups(&self) -> usize {
        self.values.len() - usize::from(self.null_group.is_some())
    }

    /// The value of each group holding one, in group index order
    fn value_groups_iter(&self) -> impl Iterator<Item = (usize, T::Native)> + '_ {
        self.values
            .iter()
            .enumerate()
            .filter(|(group_idx, _)| Some(*group_idx) != self.null_group)
            .map(|(group_idx, value)| (group_idx, *value))
    }

    /// Build a direct mapped table over `range` if it is worth a slot per
    /// value, returning whether the group values are now direct mapped
    ///
    /// Judging this on the groups accumulated so far rather than on one batch
    /// matters: a batch of a dense column looks sparse simply because it holds
    /// a fraction of the values, and a batch of a sparse one can look narrow.
    fn try_build_dense(
        &mut self,
        mut range: DenseRange<T::Native>,
        slack: usize,
    ) -> bool {
        let Some(needed) = range.len().filter(|len| *len <= DENSE_MAX_SLOTS) else {
            // Too wide to be worth a slot per value, and it can only get wider
            self.dense_disabled = true;
            return false;
        };

        // A small table is always worth it, a larger one only once the values
        // fill enough of the range they span. Judged on the range the values
        // need, not on any slack added below.
        if needed > DENSE_SMALL_SLOTS
            && self.value_groups() * DENSE_MIN_FILL_DENOM < needed
        {
            return false;
        }

        // Slack keeps repeated growth amortized. It widens the range itself so
        // that the table always covers exactly the range, which is what lets
        // any later rebuild place every group.
        let len = needed.max(slack).min(DENSE_MAX_SLOTS);
        range.max = range.max.add_wrapping(T::Native::usize_as(len - needed));
        self.observed_range = Some(range);
        let min = range.min.index();
        let mut group_ids = vec![DENSE_EMPTY; len];
        for (group_idx, value) in self.value_groups_iter() {
            let offset = value.index().wrapping_sub(min);
            debug_assert!(offset < len as u64);
            group_ids[offset as usize] = group_idx as u32;
        }

        self.store = GroupStore::Dense { min, group_ids };
        true
    }

    /// Rebuild the direct mapped table so that it also covers `array`, which
    /// holds the rows that fell outside it
    fn try_widen_dense(&mut self, array: &PrimitiveArray<T>) -> bool {
        let GroupStore::Dense { group_ids, .. } = &self.store else {
            return false;
        };
        let slack = group_ids.len().saturating_mul(2);

        // The range covered so far, widened to cover the rows left over
        let (Some(low), Some(high)) =
            (arrow::compute::min(array), arrow::compute::max(array))
        else {
            return false;
        };
        let mut range = self.observed_range.expect("dense table without a range");
        range.extend(low);
        range.extend(high);

        self.try_build_dense(range, slack)
    }

    /// Intern `array`, returning the row holding the first value that fell
    /// outside the table, if any. Earlier rows are interned, so the caller
    /// resumes from that row once the table covers the rest
    fn intern_dense(
        &mut self,
        array: &PrimitiveArray<T>,
        groups: &mut Vec<usize>,
    ) -> Option<usize> {
        let Self {
            store,
            values,
            null_group,
            ..
        } = self;
        let GroupStore::Dense { min, group_ids } = store else {
            unreachable!("group values are not direct mapped")
        };
        let min = *min;
        let len = group_ids.len() as u64;

        for (row, v) in array.iter().enumerate() {
            let group_id = match v {
                None => *null_group.get_or_insert_with(|| {
                    let group_id = values.len();
                    values.push(Default::default());
                    group_id
                }),
                Some(key) => {
                    let offset = key.index().wrapping_sub(min);
                    if offset >= len {
                        return Some(row);
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

        None
    }

    /// Rebuild the group values as a hash table, which is possible at any
    /// point because [`Self::values`] holds every group value in group index
    /// order
    fn convert_dense_to_hash(&mut self) {
        let state = &self.random_state;
        let mut map = HashTable::with_capacity(self.values.len());
        for (group_idx, value) in self.value_groups_iter() {
            let hash = value.hash(state);
            map.insert_unique(hash, (group_idx, hash), |&(_, hash)| hash);
        }
        self.store = GroupStore::Hash(map);
    }

    /// Intern `array` by hashing, tracking the range of the groups it creates
    /// so [`Self::try_build_dense`] can judge the fill rate without a scan
    fn intern_hash(&mut self, array: &PrimitiveArray<T>, groups: &mut Vec<usize>) {
        let Self {
            store,
            values,
            null_group,
            random_state,
            observed_range,
            dense_disabled,
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
                            // Only new groups can widen the range
                            if !*dense_disabled {
                                match observed_range {
                                    Some(range) => range.extend(key),
                                    None => {
                                        *observed_range =
                                            Some(DenseRange { min: key, max: key })
                                    }
                                }
                            }
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

        if matches!(self.store, GroupStore::Dense { .. }) {
            let Some(row) = self.intern_dense(array, groups) else {
                return Ok(());
            };

            // Rebuild over the wider range if still worthwhile, else hash
            let rest = array.slice(row, array.len() - row);
            if self.try_widen_dense(&rest) {
                let interned = self.intern_dense(&rest, groups);
                debug_assert!(interned.is_none(), "widened table left a value out");
                return Ok(());
            }
            self.convert_dense_to_hash();
            self.dense_disabled = true;
            self.intern_hash(&rest, groups);
            return Ok(());
        }

        self.intern_hash(array, groups);
        if !self.dense_disabled
            && let Some(range) = self.observed_range
        {
            self.try_build_dense(range, 0);
        }
        Ok(())
    }

    fn size(&self) -> usize {
        let store = match &self.store {
            GroupStore::Hash(map) => map.capacity() * size_of::<(usize, u64)>(),
            GroupStore::Dense { group_ids, .. } => group_ids.allocated_size(),
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
                match &mut self.store {
                    // Cleared rather than dropped, so that the capacity built
                    // up for these groups is reused by the next ones
                    GroupStore::Hash(map) => map.clear(),
                    GroupStore::Dense { .. } => {
                        self.store = GroupStore::Hash(HashTable::with_capacity(0))
                    }
                }
                self.observed_range = None;
                // The next values may be dense even if these were not
                self.dense_disabled = !T::Native::DENSE;
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
                    GroupStore::Dense { min, group_ids } => {
                        // Driven by the groups rather than the slots, of which
                        // there can be many more
                        for (group_idx, value) in
                            self.values.iter().enumerate().filter(|(group_idx, _)| {
                                Some(*group_idx) != self.null_group
                            })
                        {
                            let offset = value.index().wrapping_sub(*min) as usize;
                            group_ids[offset] = match group_idx.checked_sub(n) {
                                // Group index was >= n, shift value down
                                Some(sub) => sub as u32,
                                // Group index was < n, so free the slot
                                None => DENSE_EMPTY,
                            };
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

    fn values_preserving(
        &mut self,
        selection: GroupSelection<'_>,
    ) -> Result<Vec<ArrayRef>> {
        selection.validate_num_groups(self.values.len())?;
        let values: Vec<T::Native> =
            selection.iter().map(|index| self.values[index]).collect();
        let nulls = if let Some(null_group) = self.null_group {
            let mut nulls = NullBufferBuilder::new(values.len());
            for index in selection.iter() {
                if index == null_group {
                    nulls.append_null();
                } else {
                    nulls.append_non_null();
                }
            }
            nulls.finish()
        } else {
            None
        };
        let array = PrimitiveArray::<T>::new(values.into(), nulls)
            .with_data_type(self.data_type.clone());
        Ok(vec![Arc::new(array)])
    }

    fn supports_values_preserving(&self) -> bool {
        true
    }

    fn clear_shrink(&mut self, num_rows: usize) {
        self.values.clear();
        self.values.shrink_to(num_rows);
        self.null_group = None;
        self.observed_range = None;
        // Only called when spilling. A direct mapped table is sized by the
        // range of the keys, which spilling does not shrink, so rebuilding one
        // would keep the operator at the memory it just tried to give back.
        self.dense_disabled = true;
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
    use arrow::array::{Array, Float64Array, Int32Array, Int64Array};
    use arrow::datatypes::{Float64Type, Int32Type, Int64Type};

    fn is_dense(gv: &GroupValuesPrimitive<Int64Type>) -> bool {
        matches!(gv.store, GroupStore::Dense { .. })
    }

    /// Mirror of the `EmitTo::take_needed` regression test, applied to the
    /// concrete `GroupValuesPrimitive` accumulator.
    ///
    /// When `n` is small, the old `split_off(n) + swap` pattern used inside
    /// `emit(EmitTo::First(n))` left `self.values` with a small fresh allocation
    /// and returned the emitted prefix carrying the original large backing.
    ///
    /// With `split_vec_min_alloc` and `n * 2 <= len`, the drain branch is taken:
    /// the emitted prefix gets a compact allocation and `self.values` retains the
    /// original large one.
    #[test]
    fn emit_first_small_n_allocates_minimally() -> Result<()> {
        let mut gv = GroupValuesPrimitive::<Int32Type>::new(DataType::Int32);

        // Intern 20 distinct values; `new()` pre-allocates capacity 128 for `values`.
        let arr: ArrayRef = Arc::new(Int32Array::from_iter_values(0..20i32));
        let mut groups = vec![];
        gv.intern(&[arr], &mut groups)?;
        let capacity_before = gv.values.capacity(); // 128

        // n=4, n*2=8 <= len=20 -> drain branch
        let emitted = gv.emit(EmitTo::First(4))?;

        assert_eq!(emitted[0].len(), 4);

        // `self.values` must retain its original large allocation.
        // Old split_off+swap left it with a fresh small allocation (~16).
        assert_eq!(
            gv.values.capacity(),
            capacity_before,
            "self.values capacity {} should equal original {} after small First(n) emit",
            gv.values.capacity(),
            capacity_before,
        );

        Ok(())
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

    /// Emitting every group starts the decision again, since the next values
    /// may be dense even if these were not
    #[test]
    fn emitting_all_groups_reconsiders_dense() -> Result<()> {
        let mut gv = GroupValuesPrimitive::<Int64Type>::new(DataType::Int64);
        let mut groups = vec![];

        // Too wide a range to be worth a slot per value
        gv.intern(&[array(&[Some(0), Some(i64::MAX)])], &mut groups)?;
        assert!(!is_dense(&gv));

        gv.emit(EmitTo::All)?;
        gv.intern(&[array(&[Some(10), Some(11), Some(12)])], &mut groups)?;
        assert!(is_dense(&gv));

        Ok(())
    }

    /// Spilling does not shrink the range of the keys, so a direct mapped
    /// table would come straight back at the size the operator just gave up
    #[test]
    fn spilling_keeps_the_groups_hashed() -> Result<()> {
        let mut gv = GroupValuesPrimitive::<Int64Type>::new(DataType::Int64);
        let mut groups = vec![];

        gv.intern(&[array(&[Some(10), Some(11), Some(12)])], &mut groups)?;
        assert!(is_dense(&gv));

        gv.clear_shrink(0);
        gv.intern(&[array(&[Some(20), Some(21), Some(22)])], &mut groups)?;
        assert!(!is_dense(&gv));

        Ok(())
    }

    /// A table is grown with slack, so groups can be created above every value
    /// used to size it. Rebuilding it must still place those groups rather
    /// than index past the end of the new table.
    #[test]
    fn groups_above_the_observed_range_survive_a_rebuild() -> Result<()> {
        let mut gv = GroupValuesPrimitive::<Int64Type>::new(DataType::Int64);
        let mut groups = vec![];

        // Covers 1000..=1099
        let values = (1000..1100).map(Some).collect::<Vec<_>>();
        gv.intern(&[array(&values)], &mut groups)?;
        assert!(is_dense(&gv));

        // Grows to 1000..=1199, of which only 1000..=1100 was used to size it
        gv.intern(&[array(&[Some(1100)])], &mut groups)?;
        // A group in the slack, above every value the table was sized from
        gv.intern(&[array(&[Some(1199)])], &mut groups)?;

        // Forces a rebuild from far below: the range must still reach 1199
        gv.intern(&[array(&[Some(-5000)])], &mut groups)?;

        let emitted = gv.emit(EmitTo::All)?;
        let emitted = emitted[0].as_primitive::<Int64Type>();
        assert_eq!(emitted.len(), 103);
        assert_eq!(emitted.value(100), 1100);
        assert_eq!(emitted.value(101), 1199);
        assert_eq!(emitted.value(102), -5000);

        Ok(())
    }

    /// Types that cannot be direct mapped at all are ruled out while their
    /// range is tracked, which must not be mistaken for a table worth building
    #[test]
    fn float_values_keep_hashing() -> Result<()> {
        let mut gv = GroupValuesPrimitive::<Float64Type>::new(DataType::Float64);
        let mut groups = vec![];

        for batch in 0..3usize {
            let values = (0..4).map(|v| Some((batch * 4 + v) as f64));
            let input = Arc::new(Float64Array::from_iter(values)) as ArrayRef;
            gv.intern(&[input], &mut groups)?;
            // Each batch holds four values not seen before
            let first = batch * 4;
            assert_eq!(groups, (first..first + 4).collect::<Vec<_>>());
        }

        assert!(matches!(gv.store, GroupStore::Hash(_)));

        let emitted = gv.emit(EmitTo::All)?;
        assert_eq!(emitted[0].len(), 12);

        Ok(())
    }
}
