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
use datafusion_expr::EmitTo;
use half::f16;
use hashbrown::hash_table::HashTable;
#[cfg(not(feature = "force_hash_collisions"))]
use std::hash::BuildHasher;
use std::mem::size_of;
use std::sync::Arc;

const DENSE_LOOKUP_MAX_BYTES: usize = 2 * 1024 * 1024;
const DENSE_LOOKUP_MIN_FILL_NUMERATOR: usize = 1;
const DENSE_LOOKUP_MIN_FILL_DENOMINATOR: usize = 5;
const DENSE_EMPTY_GROUP: usize = usize::MAX;

/// A trait to allow hashing of floating point numbers
pub(crate) trait HashValue {
    fn hash(&self, state: &RandomState) -> u64;
}

/// A trait for values that can be used with the dense group lookup table.
pub(crate) trait DenseRangeValue: Copy {
    fn to_dense_i128(self) -> Option<i128>;
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
                state.hash_one(self.to_bits())
            }

            #[cfg(feature = "force_hash_collisions")]
            fn hash(&self, _state: &RandomState) -> u64 {
                0
            }
        })+
    };
}

hash_float!(f16, f32, f64);

macro_rules! dense_integer {
    ($($t:ty),+) => {
        $(impl DenseRangeValue for $t {
            #[inline]
            fn to_dense_i128(self) -> Option<i128> {
                Some(self as i128)
            }
        })+
    };
}

dense_integer!(i8, i16, i32, i64, i128);
dense_integer!(u8, u16, u32, u64);

macro_rules! dense_unsupported {
    ($($t:ty),+) => {
        $(impl DenseRangeValue for $t {
            #[inline]
            fn to_dense_i128(self) -> Option<i128> {
                None
            }
        })+
    };
}

dense_unsupported!(i256, IntervalDayTime, IntervalMonthDayNano);
dense_unsupported!(f16, f32, f64);

enum PrimitiveGroupStore<T> {
    Hash(HashTable<(usize, T)>),
    Dense(DenseGroupValues<T>),
}

struct DenseGroupValues<T> {
    min: i128,
    group_ids: Vec<usize>,
    values: Vec<T>,
    occupied_slots: usize,
}

struct DenseCandidate<T> {
    groups: Vec<usize>,
    null_group: Option<usize>,
    num_groups: usize,
    dense: DenseGroupValues<T>,
}

/// A [`GroupValues`] storing a single column of primitive values
///
/// This specialization is significantly faster than using the more general
/// purpose `Row`s format
pub struct GroupValuesPrimitive<T: ArrowPrimitiveType> {
    /// The data type of the output array
    data_type: DataType,
    /// Stores the group index and value
    store: PrimitiveGroupStore<T::Native>,
    /// The group index of the null value if any
    null_group: Option<usize>,
    /// The number of groups, including the null group if present
    num_groups: usize,
    /// The random state used to generate hashes
    random_state: RandomState,
    /// Set once observed values are not suitable for dense lookup
    dense_lookup_disabled: bool,
}

impl<T: ArrowPrimitiveType> GroupValuesPrimitive<T> {
    pub fn new(data_type: DataType) -> Self {
        assert!(PrimitiveArray::<T>::is_compatible(&data_type));
        Self {
            data_type,
            store: PrimitiveGroupStore::Hash(HashTable::with_capacity(128)),
            num_groups: 0,
            null_group: None,
            random_state: crate::aggregates::AGGREGATION_HASH_SEED,
            dense_lookup_disabled: false,
        }
    }
}

impl<T: ArrowPrimitiveType> GroupValuesPrimitive<T>
where
    T::Native: DenseRangeValue + HashValue,
{
    fn dense_offset(min: i128, key: T::Native) -> Option<usize> {
        key.to_dense_i128()?.checked_sub(min)?.try_into().ok()
    }

    fn dense_range_len(min: i128, max: i128) -> Option<usize> {
        let len: usize = max.checked_sub(min)?.checked_add(1)?.try_into().ok()?;
        let bytes = len.checked_mul(size_of::<usize>())?;
        (bytes <= DENSE_LOOKUP_MAX_BYTES).then_some(len)
    }

    fn intern_hash_key(
        map: &mut HashTable<(usize, T::Native)>,
        num_groups: &mut usize,
        state: &RandomState,
        key: T::Native,
    ) -> usize {
        let hash = key.hash(state);
        let insert = map.entry(
            hash,
            |&(_, value)| value.is_eq(key),
            |&(_, value)| value.hash(state),
        );

        match insert {
            hashbrown::hash_table::Entry::Occupied(o) => o.get().0,
            hashbrown::hash_table::Entry::Vacant(v) => {
                let g = *num_groups;
                v.insert((g, key));
                *num_groups += 1;
                g
            }
        }
    }

    fn dense_has_minimum_fill(dense: &DenseGroupValues<T::Native>) -> bool {
        if dense.group_ids.is_empty() {
            return false;
        }

        dense.occupied_slots * DENSE_LOOKUP_MIN_FILL_DENOMINATOR
            > dense.group_ids.len() * DENSE_LOOKUP_MIN_FILL_NUMERATOR
    }

    fn dense_can_have_minimum_fill(occupied_slots: usize, range_len: usize) -> bool {
        occupied_slots * DENSE_LOOKUP_MIN_FILL_DENOMINATOR
            > range_len * DENSE_LOOKUP_MIN_FILL_NUMERATOR
    }

    fn try_expand_dense_for_values(
        dense: &mut DenseGroupValues<T::Native>,
        values: &PrimitiveArray<T>,
    ) -> bool {
        let Some(current_max) = dense
            .min
            .checked_add(dense.group_ids.len() as i128)
            .and_then(|max| max.checked_sub(1))
        else {
            return false;
        };

        let mut new_min = dense.min;
        let mut new_max = current_max;
        let mut non_null_rows = 0usize;
        for key in values.iter().flatten() {
            let Some(key) = key.to_dense_i128() else {
                return false;
            };
            new_min = new_min.min(key);
            new_max = new_max.max(key);
            non_null_rows += 1;
        }

        if new_min == dense.min && new_max == current_max {
            return true;
        }

        let Some(new_range_len) = Self::dense_range_len(new_min, new_max) else {
            return false;
        };

        if !Self::dense_can_have_minimum_fill(
            dense.occupied_slots + non_null_rows,
            new_range_len,
        ) {
            return false;
        }

        if new_min < dense.min {
            let prefix_len = dense
                .min
                .checked_sub(new_min)
                .and_then(|v| v.try_into().ok())
                .unwrap();
            let mut group_ids = vec![DENSE_EMPTY_GROUP; new_range_len];
            group_ids[prefix_len..prefix_len + dense.group_ids.len()]
                .copy_from_slice(&dense.group_ids);
            dense.group_ids = group_ids;
            dense.min = new_min;
        } else {
            dense.group_ids.resize(new_range_len, DENSE_EMPTY_GROUP);
        }

        true
    }

    fn intern_existing_dense(
        &mut self,
        values: &PrimitiveArray<T>,
        groups: &mut Vec<usize>,
    ) -> bool {
        let PrimitiveGroupStore::Dense(dense) = &mut self.store else {
            return false;
        };

        if !Self::dense_has_minimum_fill(dense) {
            return false;
        }

        groups.clear();
        let mut null_group = self.null_group;
        let mut num_groups = self.num_groups;
        let mut mutated = false;

        for v in values {
            let group_id = match v {
                None => match null_group {
                    Some(group_id) => group_id,
                    None => {
                        let group_id = num_groups;
                        num_groups += 1;
                        null_group = Some(group_id);
                        dense.values.push(T::default_value());
                        mutated = true;
                        group_id
                    }
                },
                Some(key) => {
                    let Some(offset) = Self::dense_offset(dense.min, key)
                        .filter(|&offset| offset < dense.group_ids.len())
                    else {
                        self.null_group = null_group;
                        self.num_groups = num_groups;
                        if mutated {
                            self.dense_lookup_disabled = true;
                        } else if Self::try_expand_dense_for_values(dense, values) {
                            return self.intern_existing_dense(values, groups);
                        } else {
                            self.dense_lookup_disabled = true;
                        }
                        groups.clear();
                        return false;
                    };
                    let group_id = dense.group_ids[offset];
                    if group_id == DENSE_EMPTY_GROUP {
                        let group_id = num_groups;
                        num_groups += 1;
                        dense.group_ids[offset] = group_id;
                        dense.values.push(key);
                        dense.occupied_slots += 1;
                        mutated = true;
                        group_id
                    } else {
                        group_id
                    }
                }
            };
            groups.push(group_id);
        }

        self.null_group = null_group;
        self.num_groups = num_groups;
        if !Self::dense_has_minimum_fill(dense) {
            self.dense_lookup_disabled = true;
            groups.clear();
            return false;
        }
        true
    }

    fn insert_existing_dense_value(
        dense: &mut DenseGroupValues<T::Native>,
        group_idx: usize,
        value: T::Native,
    ) -> Option<()> {
        let offset = Self::dense_offset(dense.min, value)?;
        if dense.group_ids[offset] == DENSE_EMPTY_GROUP {
            dense.group_ids[offset] = group_idx;
            dense.occupied_slots += 1;
        }
        dense.values[group_idx] = value;
        Some(())
    }

    fn try_build_dense_candidate(
        &self,
        values: &PrimitiveArray<T>,
    ) -> Option<DenseCandidate<T::Native>> {
        let mut min = None::<i128>;
        let mut max = None::<i128>;

        let mut update_range = |value: T::Native| {
            let value = value.to_dense_i128()?;
            min = Some(min.map_or(value, |min| min.min(value)));
            max = Some(max.map_or(value, |max| max.max(value)));
            Some(())
        };

        match &self.store {
            PrimitiveGroupStore::Hash(map) => {
                for &(_, value) in map {
                    update_range(value)?;
                }
            }
            PrimitiveGroupStore::Dense(dense) => {
                for (group_idx, &value) in dense.values.iter().enumerate() {
                    if Some(group_idx) != self.null_group {
                        update_range(value)?;
                    }
                }
            }
        }

        for key in values.iter().flatten() {
            update_range(key)?;
        }

        let min = min?;
        let range_len = Self::dense_range_len(min, max?)?;
        let mut dense = DenseGroupValues {
            min,
            group_ids: vec![DENSE_EMPTY_GROUP; range_len],
            values: vec![T::default_value(); self.num_groups],
            occupied_slots: 0,
        };

        match &self.store {
            PrimitiveGroupStore::Hash(map) => {
                for &(group_idx, value) in map {
                    Self::insert_existing_dense_value(&mut dense, group_idx, value)?;
                }
            }
            PrimitiveGroupStore::Dense(existing) => {
                for (group_idx, &value) in existing.values.iter().enumerate() {
                    if Some(group_idx) != self.null_group {
                        Self::insert_existing_dense_value(&mut dense, group_idx, value)?;
                    }
                }
            }
        }

        let mut groups = Vec::with_capacity(values.len());
        let mut null_group = self.null_group;
        let mut num_groups = self.num_groups;

        for v in values {
            let group_id = match v {
                None => match null_group {
                    Some(group_id) => group_id,
                    None => {
                        let group_id = num_groups;
                        num_groups += 1;
                        null_group = Some(group_id);
                        dense.values.push(T::default_value());
                        group_id
                    }
                },
                Some(key) => {
                    let offset = Self::dense_offset(dense.min, key)?;
                    let group_id = dense.group_ids[offset];
                    if group_id == DENSE_EMPTY_GROUP {
                        let group_id = num_groups;
                        num_groups += 1;
                        dense.group_ids[offset] = group_id;
                        dense.values.push(key);
                        dense.occupied_slots += 1;
                        group_id
                    } else {
                        group_id
                    }
                }
            };
            groups.push(group_id);
        }

        Self::dense_has_minimum_fill(&dense).then_some(DenseCandidate {
            groups,
            null_group,
            num_groups,
            dense,
        })
    }

    fn convert_dense_to_hash(&mut self) {
        if matches!(&self.store, PrimitiveGroupStore::Hash(_)) {
            return;
        }

        let PrimitiveGroupStore::Dense(dense) = std::mem::replace(
            &mut self.store,
            PrimitiveGroupStore::Hash(HashTable::new()),
        ) else {
            return;
        };

        let non_null_groups = self.num_groups - usize::from(self.null_group.is_some());
        let mut map = HashTable::with_capacity(non_null_groups);
        let state = &self.random_state;

        for (group_idx, value) in dense.values.into_iter().enumerate() {
            if Some(group_idx) == self.null_group {
                continue;
            }

            let hash = value.hash(state);
            map.insert_unique(hash, (group_idx, value), |&(_, value)| value.hash(state));
        }

        self.store = PrimitiveGroupStore::Hash(map);
    }

    fn intern_hash(&mut self, values: &PrimitiveArray<T>, groups: &mut Vec<usize>) {
        self.convert_dense_to_hash();

        let PrimitiveGroupStore::Hash(map) = &mut self.store else {
            unreachable!();
        };

        groups.clear();
        let state = &self.random_state;
        let num_groups = &mut self.num_groups;
        let null_group = &mut self.null_group;

        if values.null_count() == 0 {
            groups.extend(
                values
                    .values()
                    .iter()
                    .map(|&key| Self::intern_hash_key(map, num_groups, state, key)),
            );
        } else {
            for v in values {
                let group_id = match v {
                    None => *null_group.get_or_insert_with(|| {
                        let group_id = *num_groups;
                        *num_groups += 1;
                        group_id
                    }),
                    Some(key) => Self::intern_hash_key(map, num_groups, state, key),
                };
                groups.push(group_id)
            }
        }
    }

    fn has_non_null_values(&self, values: &PrimitiveArray<T>) -> bool {
        self.num_groups > usize::from(self.null_group.is_some())
            || values.null_count() != values.len()
    }
}

impl<T: ArrowPrimitiveType> GroupValues for GroupValuesPrimitive<T>
where
    T::Native: DenseRangeValue + HashValue,
{
    fn intern(&mut self, cols: &[ArrayRef], groups: &mut Vec<usize>) -> Result<()> {
        assert_eq!(cols.len(), 1);

        let values = cols[0].as_primitive::<T>();

        if self.intern_existing_dense(values, groups) {
            return Ok(());
        }

        if !self.dense_lookup_disabled {
            if let Some(candidate) = self.try_build_dense_candidate(values) {
                *groups = candidate.groups;
                self.null_group = candidate.null_group;
                self.num_groups = candidate.num_groups;
                self.store = PrimitiveGroupStore::Dense(candidate.dense);
                return Ok(());
            } else if self.has_non_null_values(values) {
                self.dense_lookup_disabled = true;
            }
        }

        self.intern_hash(values, groups);

        Ok(())
    }

    fn size(&self) -> usize {
        match &self.store {
            PrimitiveGroupStore::Hash(map) => {
                map.capacity() * size_of::<(usize, T::Native)>()
            }
            PrimitiveGroupStore::Dense(dense) => {
                dense.group_ids.capacity() * size_of::<usize>()
                    + dense.values.capacity() * size_of::<T::Native>()
            }
        }
    }

    fn is_empty(&self) -> bool {
        self.num_groups == 0
    }

    fn len(&self) -> usize {
        self.num_groups
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
                let values = match std::mem::replace(
                    &mut self.store,
                    PrimitiveGroupStore::Hash(HashTable::new()),
                ) {
                    PrimitiveGroupStore::Hash(map) => {
                        let mut values = vec![T::default_value(); self.num_groups];
                        for (group_idx, value) in map {
                            values[group_idx] = value;
                        }
                        values
                    }
                    PrimitiveGroupStore::Dense(dense) => dense.values,
                };
                self.num_groups = 0;
                self.dense_lookup_disabled = false;

                build_primitive(values, self.null_group.take())
            }
            EmitTo::First(n) => {
                let mut dense_below_minimum_fill = false;
                let values = match &mut self.store {
                    PrimitiveGroupStore::Hash(map) => {
                        let mut values = vec![T::default_value(); n];

                        map.retain(|(group_idx, value)| {
                            // Decrement group index by n
                            match group_idx.checked_sub(n) {
                                // Group index was >= n, shift value down
                                Some(sub) => {
                                    *group_idx = sub;
                                    true
                                }
                                // Group index was < n, so remove from table
                                None => {
                                    values[*group_idx] = *value;
                                    false
                                }
                            }
                        });
                        values
                    }
                    PrimitiveGroupStore::Dense(dense) => {
                        let values = dense.values.drain(..n).collect::<Vec<_>>();
                        for group_id in &mut dense.group_ids {
                            if *group_id == DENSE_EMPTY_GROUP {
                                continue;
                            }
                            match group_id.checked_sub(n) {
                                Some(shifted) => *group_id = shifted,
                                None => {
                                    *group_id = DENSE_EMPTY_GROUP;
                                    dense.occupied_slots -= 1;
                                }
                            }
                        }
                        dense_below_minimum_fill = !Self::dense_has_minimum_fill(dense);
                        values
                    }
                };
                let null_group = match &mut self.null_group {
                    Some(v) if *v >= n => {
                        *v -= n;
                        None
                    }
                    Some(_) => self.null_group.take(),
                    None => None,
                };
                self.num_groups -= n;
                if dense_below_minimum_fill {
                    self.dense_lookup_disabled = true;
                    self.convert_dense_to_hash();
                }
                build_primitive(values, null_group)
            }
        };

        Ok(vec![Arc::new(array.with_data_type(self.data_type.clone()))])
    }

    fn clear_shrink(&mut self, num_rows: usize) {
        self.num_groups = 0;
        self.null_group = None;
        self.dense_lookup_disabled = false;
        match &mut self.store {
            PrimitiveGroupStore::Hash(map) => {
                map.clear();
                map.shrink_to(num_rows, |_| 0); // hasher does not matter since the map is cleared
            }
            PrimitiveGroupStore::Dense(_) => {
                self.store =
                    PrimitiveGroupStore::Hash(HashTable::with_capacity(num_rows));
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Array, UInt64Array};
    use arrow::datatypes::UInt64Type;

    fn values(array: &ArrayRef) -> Vec<Option<u64>> {
        let array = array.as_primitive::<UInt64Type>();
        (0..array.len())
            .map(|idx| {
                if array.is_null(idx) {
                    None
                } else {
                    Some(array.value(idx))
                }
            })
            .collect()
    }

    fn is_dense(group_values: &GroupValuesPrimitive<UInt64Type>) -> bool {
        matches!(group_values.store, PrimitiveGroupStore::Dense(_))
    }

    #[test]
    fn primitive_emit_all_reconstructs_group_order() -> Result<()> {
        let input = Arc::new(UInt64Array::from(vec![Some(10), Some(20), None, Some(10)]))
            as ArrayRef;
        let mut group_values = GroupValuesPrimitive::<UInt64Type>::new(DataType::UInt64);
        let mut groups = vec![];

        group_values.intern(&[input], &mut groups)?;
        assert_eq!(groups, vec![0, 1, 2, 0]);

        let output = group_values.emit(EmitTo::All)?;
        assert_eq!(values(&output[0]), vec![Some(10), Some(20), None]);
        assert!(group_values.is_empty());

        Ok(())
    }

    #[test]
    fn primitive_emit_first_reindexes_remaining_groups() -> Result<()> {
        let input = Arc::new(UInt64Array::from(vec![
            Some(10),
            None,
            Some(20),
            Some(30),
            Some(10),
        ])) as ArrayRef;
        let mut group_values = GroupValuesPrimitive::<UInt64Type>::new(DataType::UInt64);
        let mut groups = vec![];

        group_values.intern(&[input], &mut groups)?;
        assert_eq!(groups, vec![0, 1, 2, 3, 0]);

        let output = group_values.emit(EmitTo::First(2))?;
        assert_eq!(values(&output[0]), vec![Some(10), None]);
        assert_eq!(group_values.len(), 2);

        let input = Arc::new(UInt64Array::from(vec![Some(20), Some(40)])) as ArrayRef;
        group_values.intern(&[input], &mut groups)?;
        assert_eq!(groups, vec![0, 2]);

        let output = group_values.emit(EmitTo::All)?;
        assert_eq!(values(&output[0]), vec![Some(20), Some(30), Some(40)]);

        Ok(())
    }

    #[test]
    fn primitive_dense_lookup_used_for_compact_range() -> Result<()> {
        let input = Arc::new(UInt64Array::from(vec![
            Some(10),
            Some(11),
            Some(12),
            Some(10),
        ])) as ArrayRef;
        let mut group_values = GroupValuesPrimitive::<UInt64Type>::new(DataType::UInt64);
        let mut groups = vec![];

        group_values.intern(&[input], &mut groups)?;
        assert_eq!(groups, vec![0, 1, 2, 0]);
        assert!(is_dense(&group_values));

        Ok(())
    }

    #[test]
    fn primitive_dense_lookup_rejected_for_low_fill_rate() -> Result<()> {
        let input = Arc::new(UInt64Array::from(vec![Some(0), Some(999)])) as ArrayRef;
        let mut group_values = GroupValuesPrimitive::<UInt64Type>::new(DataType::UInt64);
        let mut groups = vec![];

        group_values.intern(&[input], &mut groups)?;
        assert_eq!(groups, vec![0, 1]);
        assert!(!is_dense(&group_values));

        Ok(())
    }

    #[test]
    fn primitive_dense_lookup_rejected_for_large_range() -> Result<()> {
        let input =
            Arc::new(UInt64Array::from(vec![Some(0), Some(1_000_000)])) as ArrayRef;
        let mut group_values = GroupValuesPrimitive::<UInt64Type>::new(DataType::UInt64);
        let mut groups = vec![];

        group_values.intern(&[input], &mut groups)?;
        assert_eq!(groups, vec![0, 1]);
        assert!(!is_dense(&group_values));

        Ok(())
    }

    #[test]
    fn primitive_dense_lookup_converts_to_hash_when_range_expands() -> Result<()> {
        let input =
            Arc::new(UInt64Array::from(vec![Some(0), Some(1), Some(2)])) as ArrayRef;
        let mut group_values = GroupValuesPrimitive::<UInt64Type>::new(DataType::UInt64);
        let mut groups = vec![];

        group_values.intern(&[input], &mut groups)?;
        assert_eq!(groups, vec![0, 1, 2]);
        assert!(is_dense(&group_values));

        let input =
            Arc::new(UInt64Array::from(vec![Some(1_000_000), Some(1)])) as ArrayRef;
        group_values.intern(&[input], &mut groups)?;
        assert_eq!(groups, vec![3, 1]);
        assert!(!is_dense(&group_values));

        let output = group_values.emit(EmitTo::All)?;
        assert_eq!(
            values(&output[0]),
            vec![Some(0), Some(1), Some(2), Some(1_000_000)]
        );

        Ok(())
    }

    #[test]
    fn primitive_dense_lookup_converts_to_hash_when_emit_makes_range_sparse() -> Result<()>
    {
        let input = Arc::new(UInt64Array::from_iter_values(0..10)) as ArrayRef;
        let mut group_values = GroupValuesPrimitive::<UInt64Type>::new(DataType::UInt64);
        let mut groups = vec![];

        group_values.intern(&[input], &mut groups)?;
        assert_eq!(groups, (0..10).collect::<Vec<_>>());
        assert!(is_dense(&group_values));

        let output = group_values.emit(EmitTo::First(9))?;
        assert_eq!(values(&output[0]), (0..9).map(Some).collect::<Vec<_>>());
        assert_eq!(group_values.len(), 1);
        assert!(!is_dense(&group_values));

        Ok(())
    }
}
