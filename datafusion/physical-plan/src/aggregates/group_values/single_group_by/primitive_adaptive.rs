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

//! A [`GroupValues`] implementation that adaptively switches between
//! flat (direct-indexed) and hash-based group tracking for a single
//! primitive column.

use super::primitive::{GroupValuesPrimitive, HashValue};
use super::primitive_flat::FlatIndex;
use crate::aggregates::group_values::GroupValues;
use arrow::array::{
    Array, ArrayRef, ArrowPrimitiveType, NullBufferBuilder, PrimitiveArray, cast::AsArray,
};
use arrow::datatypes::DataType;
use datafusion_common::Result;
use datafusion_expr::EmitTo;
use std::mem::size_of;
use std::sync::Arc;

/// Sentinel value indicating no group has been assigned to a slot.
const NO_GROUP: usize = usize::MAX;

/// Maximum flat map size for the adaptive builder.
/// At 8 bytes per `usize` slot, 16,777,216 slots = 128 MB.
const MAX_ADAPTIVE_FLAT_RANGE: usize = 16_777_216;

/// A [`GroupValues`] for a single primitive column that adaptively chooses
/// between flat (direct-indexed) and hash-based group tracking at runtime.
///
/// Unlike the statistics-based [`GroupValuesPrimitiveFlat`], this does NOT
/// require column statistics. Instead it observes the actual data:
///
/// 1. On the first batch, computes min/max and starts in flat mode if the
///    range fits within [`MAX_ADAPTIVE_FLAT_RANGE`].
/// 2. If a later batch expands the range beyond the threshold, all existing
///    groups are migrated to a [`GroupValuesPrimitive`] hash table and
///    subsequent lookups use hashing.
///
/// [`GroupValuesPrimitiveFlat`]: super::primitive_flat::GroupValuesPrimitiveFlat
pub struct GroupValuesPrimitiveAdaptive<T: ArrowPrimitiveType>
where
    T::Native: FlatIndex + HashValue + Ord,
{
    data_type: DataType,
    state: AdaptiveState<T>,
}

enum AdaptiveState<T: ArrowPrimitiveType>
where
    T::Native: FlatIndex + HashValue + Ord,
{
    /// Haven't seen data yet; will decide on first batch.
    Initial,
    /// Using flat direct indexing (may grow or migrate to Hash).
    Flat {
        map: Vec<usize>,
        min: T::Native,
        null_group: Option<usize>,
        values: Vec<T::Native>,
    },
    /// Hash-based indexing (terminal state).
    Hash(GroupValuesPrimitive<T>),
}

impl<T: ArrowPrimitiveType> GroupValuesPrimitiveAdaptive<T>
where
    T::Native: FlatIndex + HashValue + Ord,
{
    pub fn new(data_type: DataType) -> Self {
        assert!(PrimitiveArray::<T>::is_compatible(&data_type));
        Self {
            data_type,
            state: AdaptiveState::Initial,
        }
    }

    /// Compute min and max of non-null values in the array.
    /// Returns `None` if there are no non-null values.
    #[inline]
    fn compute_min_max(arr: &PrimitiveArray<T>) -> Option<(T::Native, T::Native)> {
        if arr.len() == 0 || arr.null_count() == arr.len() {
            return None;
        }

        let values = arr.values();
        if arr.null_count() == 0 {
            let mut min = values[0];
            let mut max = values[0];
            for &v in values.iter().skip(1) {
                if v < min {
                    min = v;
                }
                if v > max {
                    max = v;
                }
            }
            Some((min, max))
        } else {
            let mut result: Option<(T::Native, T::Native)> = None;
            for v in arr.iter().flatten() {
                match result {
                    None => result = Some((v, v)),
                    Some((mn, mx)) => {
                        result = Some((
                            if v < mn { v } else { mn },
                            if v > mx { v } else { mx },
                        ));
                    }
                }
            }
            result
        }
    }

    /// Migrate existing flat groups into a fresh [`GroupValuesPrimitive`].
    ///
    /// The values are interned in group-id order so that the new hash table
    /// assigns the same group ids as the old flat map.
    fn migrate_flat_to_hash(
        data_type: &DataType,
        values: Vec<T::Native>,
        null_group: Option<usize>,
    ) -> GroupValuesPrimitive<T> {
        let mut hash = GroupValuesPrimitive::new(data_type.clone());

        if values.is_empty() {
            return hash;
        }

        // Build an array from the existing group values, marking the null
        // group slot as null so that `intern` assigns it via the null path.
        let null_buffer = null_group.map(|idx| {
            let mut builder = NullBufferBuilder::new(values.len());
            builder.append_n_non_nulls(idx);
            builder.append_null();
            builder.append_n_non_nulls(values.len() - idx - 1);
            builder.finish().unwrap()
        });
        let array: ArrayRef = Arc::new(
            PrimitiveArray::<T>::new(values.into(), null_buffer)
                .with_data_type(data_type.clone()),
        );

        let mut groups = Vec::new();
        hash.intern(&[array], &mut groups).unwrap();
        debug_assert!(groups.iter().enumerate().all(|(i, &g)| g == i));

        hash
    }

    /// Ensure the flat map can hold values in `[new_min, new_max]`.
    /// Returns `false` if the resulting range would exceed the threshold,
    /// meaning the caller should migrate to hash instead.
    fn ensure_flat_range(
        map: &mut Vec<usize>,
        min: &mut T::Native,
        new_min: T::Native,
        new_max: T::Native,
    ) -> bool {
        let new_range = new_max.index_from(new_min).saturating_add(1);
        if new_range > MAX_ADAPTIVE_FLAT_RANGE {
            return false;
        }

        let current_range = map.len();

        if current_range == 0 {
            // Empty map (e.g. initial all-nulls) — initialise from scratch.
            *map = vec![NO_GROUP; new_range];
            *min = new_min;
        } else if new_min < *min {
            // Prepend slots: shift existing entries right.
            let shift = (*min).index_from(new_min);
            let mut new_map = vec![NO_GROUP; new_range];
            new_map[shift..shift + current_range].copy_from_slice(map);
            *map = new_map;
            *min = new_min;
        } else if new_range > current_range {
            // Extend the map at the end.
            map.resize(new_range, NO_GROUP);
        }

        true
    }

    /// Intern a batch while in flat mode.  Assumes the map already covers all
    /// values in `arr`.
    fn intern_flat(
        map: &mut [usize],
        min: T::Native,
        null_group: &mut Option<usize>,
        values: &mut Vec<T::Native>,
        arr: &PrimitiveArray<T>,
        groups: &mut Vec<usize>,
    ) {
        groups.clear();

        if arr.null_count() == 0 {
            for &key in arr.values().iter() {
                let index = key.index_from(min);
                // SAFETY: ensure_flat_range guarantees the map covers all values.
                let slot = unsafe { map.get_unchecked_mut(index) };
                if *slot == NO_GROUP {
                    let group_id = values.len();
                    *slot = group_id;
                    values.push(key);
                    groups.push(group_id);
                } else {
                    groups.push(*slot);
                }
            }
        } else {
            for v in arr.iter() {
                let group_id = match v {
                    None => *null_group.get_or_insert_with(|| {
                        let group_id = values.len();
                        values.push(Default::default());
                        group_id
                    }),
                    Some(key) => {
                        let index = key.index_from(min);
                        let slot = unsafe { map.get_unchecked_mut(index) };
                        if *slot == NO_GROUP {
                            let group_id = values.len();
                            *slot = group_id;
                            values.push(key);
                            group_id
                        } else {
                            *slot
                        }
                    }
                };
                groups.push(group_id);
            }
        }
    }
}

impl<T: ArrowPrimitiveType> GroupValues for GroupValuesPrimitiveAdaptive<T>
where
    T::Native: FlatIndex + HashValue + Ord,
{
    fn intern(&mut self, cols: &[ArrayRef], groups: &mut Vec<usize>) -> Result<()> {
        assert_eq!(cols.len(), 1);
        let arr = cols[0].as_primitive::<T>();

        match &mut self.state {
            AdaptiveState::Initial => {
                if let Some((batch_min, batch_max)) = Self::compute_min_max(arr) {
                    let range = batch_max.index_from(batch_min).saturating_add(1);
                    if range <= MAX_ADAPTIVE_FLAT_RANGE {
                        self.state = AdaptiveState::Flat {
                            map: vec![NO_GROUP; range],
                            min: batch_min,
                            null_group: None,
                            values: Vec::with_capacity(128),
                        };
                    } else {
                        let mut hash = GroupValuesPrimitive::new(self.data_type.clone());
                        hash.intern(cols, groups)?;
                        self.state = AdaptiveState::Hash(hash);
                        return Ok(());
                    }
                } else {
                    // All nulls or empty: start flat with an empty map.
                    self.state = AdaptiveState::Flat {
                        map: Vec::new(),
                        min: T::Native::default(),
                        null_group: None,
                        values: Vec::with_capacity(128),
                    };
                }
                // Fall through to process the batch in the new flat state.
                self.intern(cols, groups)
            }

            AdaptiveState::Flat {
                map,
                min,
                null_group,
                values,
            } => {
                if let Some((batch_min, batch_max)) = Self::compute_min_max(arr) {
                    let current_range = map.len();
                    let current_min = *min;

                    let needs_resize = if current_range == 0 {
                        true
                    } else {
                        batch_min < current_min
                            || batch_max.index_from(current_min) >= current_range
                    };

                    if needs_resize {
                        let (new_min, new_max) = if current_range == 0 {
                            (batch_min, batch_max)
                        } else {
                            let current_max =
                                T::Native::from_index(current_range - 1, current_min);
                            (
                                std::cmp::min(current_min, batch_min),
                                std::cmp::max(current_max, batch_max),
                            )
                        };

                        if !Self::ensure_flat_range(map, min, new_min, new_max) {
                            // Range too large – migrate to hash.
                            let mut hash = Self::migrate_flat_to_hash(
                                &self.data_type,
                                std::mem::take(values),
                                *null_group,
                            );
                            hash.intern(cols, groups)?;
                            self.state = AdaptiveState::Hash(hash);
                            return Ok(());
                        }
                    }
                }

                Self::intern_flat(map, *min, null_group, values, arr, groups);
                Ok(())
            }

            AdaptiveState::Hash(hash) => hash.intern(cols, groups),
        }
    }

    fn size(&self) -> usize {
        match &self.state {
            AdaptiveState::Initial => 0,
            AdaptiveState::Flat { map, values, .. } => {
                map.len() * size_of::<usize>()
                    + values.capacity() * size_of::<T::Native>()
            }
            AdaptiveState::Hash(hash) => hash.size(),
        }
    }

    fn is_empty(&self) -> bool {
        match &self.state {
            AdaptiveState::Initial => true,
            AdaptiveState::Flat { values, .. } => values.is_empty(),
            AdaptiveState::Hash(hash) => hash.is_empty(),
        }
    }

    fn len(&self) -> usize {
        match &self.state {
            AdaptiveState::Initial => 0,
            AdaptiveState::Flat { values, .. } => values.len(),
            AdaptiveState::Hash(hash) => hash.len(),
        }
    }

    fn emit(&mut self, emit_to: EmitTo) -> Result<Vec<ArrayRef>> {
        match &mut self.state {
            AdaptiveState::Initial => {
                let empty = PrimitiveArray::<T>::builder(0)
                    .finish()
                    .with_data_type(self.data_type.clone());
                Ok(vec![Arc::new(empty)])
            }
            AdaptiveState::Flat {
                map,
                values,
                null_group,
                ..
            } => {
                let array: PrimitiveArray<T> = match emit_to {
                    EmitTo::All => {
                        map.fill(NO_GROUP);
                        let nulls = null_group.take().map(|null_idx| {
                            let vals = &*values;
                            let mut buf = NullBufferBuilder::new(vals.len());
                            buf.append_n_non_nulls(null_idx);
                            buf.append_null();
                            buf.append_n_non_nulls(vals.len() - null_idx - 1);
                            buf.finish().unwrap()
                        });
                        PrimitiveArray::<T>::new(std::mem::take(values).into(), nulls)
                    }
                    EmitTo::First(n) => {
                        for slot in map.iter_mut() {
                            if *slot != NO_GROUP {
                                match slot.checked_sub(n) {
                                    Some(new_idx) => *slot = new_idx,
                                    None => *slot = NO_GROUP,
                                }
                            }
                        }
                        let ng = match null_group {
                            Some(v) if *v >= n => {
                                *v -= n;
                                None
                            }
                            Some(_) => null_group.take(),
                            None => None,
                        };
                        let mut split = values.split_off(n);
                        std::mem::swap(values, &mut split);
                        let nulls = ng.map(|null_idx| {
                            let mut buf = NullBufferBuilder::new(split.len());
                            buf.append_n_non_nulls(null_idx);
                            buf.append_null();
                            buf.append_n_non_nulls(split.len() - null_idx - 1);
                            buf.finish().unwrap()
                        });
                        PrimitiveArray::<T>::new(split.into(), nulls)
                    }
                };
                Ok(vec![Arc::new(array.with_data_type(self.data_type.clone()))])
            }
            AdaptiveState::Hash(hash) => hash.emit(emit_to),
        }
    }

    fn clear_shrink(&mut self, num_rows: usize) {
        match &mut self.state {
            AdaptiveState::Initial => {}
            AdaptiveState::Flat {
                map,
                values,
                null_group,
                ..
            } => {
                values.clear();
                values.shrink_to(num_rows);
                map.fill(NO_GROUP);
                *null_group = None;
            }
            AdaptiveState::Hash(hash) => hash.clear_shrink(num_rows),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::types::{Int32Type, Int64Type};
    use arrow::array::{Int32Array, Int64Array};

    #[test]
    fn test_adaptive_starts_flat() {
        let mut gv = GroupValuesPrimitiveAdaptive::<Int32Type>::new(DataType::Int32);
        let mut groups = vec![];

        // Small range → should stay flat
        let arr: ArrayRef = Arc::new(Int32Array::from(vec![10, 20, 10, 30]));
        gv.intern(&[arr], &mut groups).unwrap();
        assert_eq!(groups, vec![0, 1, 0, 2]);
        assert_eq!(gv.len(), 3);
        assert!(matches!(gv.state, AdaptiveState::Flat { .. }));
    }

    #[test]
    fn test_adaptive_grows_flat() {
        let mut gv = GroupValuesPrimitiveAdaptive::<Int32Type>::new(DataType::Int32);
        let mut groups = vec![];

        let arr: ArrayRef = Arc::new(Int32Array::from(vec![100, 200]));
        gv.intern(&[arr], &mut groups).unwrap();
        assert_eq!(groups, vec![0, 1]);

        // New batch with values outside initial range (below min)
        let arr: ArrayRef = Arc::new(Int32Array::from(vec![50, 100, 300]));
        gv.intern(&[arr], &mut groups).unwrap();
        assert_eq!(groups, vec![2, 0, 3]); // 50 is new, 100 is group 0, 300 is new

        assert!(matches!(gv.state, AdaptiveState::Flat { .. }));
        assert_eq!(gv.len(), 4);
    }

    #[test]
    fn test_adaptive_migrates_to_hash() {
        let mut gv = GroupValuesPrimitiveAdaptive::<Int64Type>::new(DataType::Int64);
        let mut groups = vec![];

        // First batch: small range → flat
        let arr: ArrayRef = Arc::new(Int64Array::from(vec![1, 2, 3]));
        gv.intern(&[arr], &mut groups).unwrap();
        assert_eq!(groups, vec![0, 1, 2]);
        assert!(matches!(gv.state, AdaptiveState::Flat { .. }));

        // Second batch: huge range → triggers migration to hash
        let arr: ArrayRef = Arc::new(Int64Array::from(vec![1, i64::MAX, 2]));
        gv.intern(&[arr], &mut groups).unwrap();
        assert!(matches!(gv.state, AdaptiveState::Hash(_)));
        // Old groups preserved: 1→0, iMAX→3(new), 2→1
        assert_eq!(groups, vec![0, 3, 1]);
    }

    #[test]
    fn test_adaptive_null_handling() {
        let mut gv = GroupValuesPrimitiveAdaptive::<Int32Type>::new(DataType::Int32);
        let mut groups = vec![];

        let arr: ArrayRef =
            Arc::new(Int32Array::from(vec![Some(1), None, Some(1), None]));
        gv.intern(&[arr], &mut groups).unwrap();
        assert_eq!(groups, vec![0, 1, 0, 1]);
        assert_eq!(gv.len(), 2);

        let result = gv.emit(EmitTo::All).unwrap();
        let values = result[0].as_primitive::<Int32Type>();
        assert_eq!(values.len(), 2);
        assert!(values.is_valid(0));
        assert!(!values.is_valid(1));
    }

    #[test]
    fn test_adaptive_emit_first() {
        let mut gv = GroupValuesPrimitiveAdaptive::<Int32Type>::new(DataType::Int32);
        let mut groups = vec![];

        let arr: ArrayRef = Arc::new(Int32Array::from(vec![10, 20, 30]));
        gv.intern(&[arr], &mut groups).unwrap();
        assert_eq!(groups, vec![0, 1, 2]);

        let result = gv.emit(EmitTo::First(2)).unwrap();
        let values = result[0].as_primitive::<Int32Type>();
        assert_eq!(values.values().as_ref(), &[10, 20]);
        assert_eq!(gv.len(), 1);

        let arr: ArrayRef = Arc::new(Int32Array::from(vec![30, 40]));
        gv.intern(&[arr], &mut groups).unwrap();
        assert_eq!(groups, vec![0, 1]);
    }

    #[test]
    fn test_adaptive_all_nulls_initial() {
        let mut gv = GroupValuesPrimitiveAdaptive::<Int32Type>::new(DataType::Int32);
        let mut groups = vec![];

        let arr: ArrayRef = Arc::new(Int32Array::from(vec![None, None]));
        gv.intern(&[arr], &mut groups).unwrap();
        assert_eq!(groups, vec![0, 0]);
        assert!(matches!(gv.state, AdaptiveState::Flat { .. }));

        // Subsequent batch with non-null values
        let arr: ArrayRef = Arc::new(Int32Array::from(vec![Some(5), None]));
        gv.intern(&[arr], &mut groups).unwrap();
        assert_eq!(groups, vec![1, 0]);
    }

    #[test]
    fn test_adaptive_migrate_preserves_nulls() {
        let mut gv = GroupValuesPrimitiveAdaptive::<Int64Type>::new(DataType::Int64);
        let mut groups = vec![];

        let arr: ArrayRef = Arc::new(Int64Array::from(vec![Some(1), None, Some(2)]));
        gv.intern(&[arr], &mut groups).unwrap();
        assert_eq!(groups, vec![0, 1, 2]);

        // Trigger migration
        let arr: ArrayRef =
            Arc::new(Int64Array::from(vec![Some(i64::MAX), None, Some(1)]));
        gv.intern(&[arr], &mut groups).unwrap();
        assert!(matches!(gv.state, AdaptiveState::Hash(_)));
        // null should be same group (1), 1→0, iMAX→3(new)
        assert_eq!(groups, vec![3, 1, 0]);
    }

    #[test]
    fn test_adaptive_direct_hash_for_huge_range() {
        let mut gv = GroupValuesPrimitiveAdaptive::<Int64Type>::new(DataType::Int64);
        let mut groups = vec![];

        // First batch has huge range → goes directly to hash
        let arr: ArrayRef = Arc::new(Int64Array::from(vec![i64::MIN, i64::MAX]));
        gv.intern(&[arr], &mut groups).unwrap();
        assert!(matches!(gv.state, AdaptiveState::Hash(_)));
        assert_eq!(groups, vec![0, 1]);
    }
}
