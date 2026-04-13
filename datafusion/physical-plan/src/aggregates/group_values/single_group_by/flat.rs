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

//! Adaptive flat/hash group values for integer-typed columns.
//!
//! For integer GROUP BY columns, this uses a flat direct-address array when the
//! observed value range is small, providing O(1) lookups with no hashing. When
//! the range exceeds a threshold, it falls back to [`GroupValuesPrimitive`] with
//! zero per-value overhead on subsequent batches.
//!
//! Inspired by DuckDB's "perfect hash aggregate" approach.

use super::primitive::{GroupValuesPrimitive, HashValue};
use crate::aggregates::group_values::GroupValues;
use arrow::array::{
    Array, ArrayRef, ArrowPrimitiveType, NullBufferBuilder, PrimitiveArray, cast::AsArray,
};
use arrow::datatypes::DataType;
use datafusion_common::Result;
use datafusion_common::utils::proxy::VecAllocExt;
use datafusion_expr::EmitTo;
use std::sync::Arc;

/// Maximum number of slots in the flat lookup array before falling back to a
/// hash table. 100K slots × 4 bytes = 400 KB.
const MAX_FLAT_RANGE: usize = 100_000;

/// Sentinel value indicating no group is assigned to a flat-map slot.
const EMPTY_GROUP: u32 = u32::MAX;

/// Integer native types that support range-based flat lookups.
///
/// Provides a wider signed type (`Wide`) for safe range arithmetic that avoids
/// i128 overhead.  For 8/16-bit natives the wide type is `i32`; for 32-bit
/// it is `i64`; for 64-bit it is `i128`.
pub(crate) trait FlatMappable: Copy + Default + Ord {
    /// A signed integer type wide enough to hold `max - min` for any two
    /// values of `Self` without overflow.
    type Wide: Copy
        + Default
        + Ord
        + Send
        + std::ops::Sub<Output = Self::Wide>
        + std::ops::Add<Output = Self::Wide>
        + TryInto<usize>;

    /// Widen this value for range arithmetic.
    fn to_wide(self) -> Self::Wide;

    /// Convert a `usize` back to a `Wide` value (for base storage).
    fn wide_from_usize(v: usize) -> Self::Wide;
}

macro_rules! impl_flat_mappable {
    ($native:ty, $wide:ty) => {
        impl FlatMappable for $native {
            type Wide = $wide;

            #[inline(always)]
            fn to_wide(self) -> $wide {
                self as $wide
            }

            #[inline(always)]
            fn wide_from_usize(v: usize) -> $wide {
                v as $wide
            }
        }
    };
}

impl_flat_mappable!(i8, i32);
impl_flat_mappable!(u8, i32);
impl_flat_mappable!(i16, i32);
impl_flat_mappable!(u16, i32);
impl_flat_mappable!(i32, i64);
impl_flat_mappable!(u32, i64);
impl_flat_mappable!(i64, i128);
impl_flat_mappable!(u64, i128);
impl_flat_mappable!(i128, i128);

// ---------------------------------------------------------------------------
// Inner enum: flat state vs. delegated GroupValuesPrimitive
// ---------------------------------------------------------------------------

enum Inner<T: ArrowPrimitiveType>
where
    T::Native: HashValue + FlatMappable,
{
    Flat(FlatState<T>),
    /// Fallen back to hash — full delegation, zero per-value overhead.
    Hash(GroupValuesPrimitive<T>),
}

/// The flat direct-address state.
struct FlatState<T: ArrowPrimitiveType>
where
    T::Native: FlatMappable,
{
    data_type: DataType,
    /// `values[group_index]` = the native value for that group.
    values: Vec<T::Native>,
    /// Group index for NULL, if encountered.
    null_group: Option<usize>,
    /// `None` = not yet initialised (no non-null value seen).
    /// `Some((map, base))` = active flat map.
    map_and_base: Option<(Vec<u32>, <T::Native as FlatMappable>::Wide)>,
}

impl<T: ArrowPrimitiveType> FlatState<T>
where
    T::Native: FlatMappable,
{
    fn new(data_type: DataType) -> Self {
        Self {
            data_type,
            values: Vec::with_capacity(128),
            null_group: None,
            map_and_base: None,
        }
    }

    /// Ensure the flat map covers `[batch_min, batch_max]`.
    ///
    /// Returns `true` if the range fits (map has been expanded if needed),
    /// `false` if the range exceeds [`MAX_FLAT_RANGE`] (caller should
    /// convert to hash).
    fn ensure_range(&mut self, batch_min: T::Native, batch_max: T::Native) -> bool {
        let min_wide = batch_min.to_wide();
        let max_wide = batch_max.to_wide();

        if let Some((map, base)) = &mut self.map_and_base {
            let current_max =
                *base + <T::Native as FlatMappable>::wide_from_usize(map.len() - 1);

            // Already in range — nothing to do.
            if min_wide >= *base && max_wide <= current_max {
                return true;
            }

            let new_min = if min_wide < *base { min_wide } else { *base };
            let new_max = if max_wide > current_max {
                max_wide
            } else {
                current_max
            };
            let new_range_wide = new_max - new_min;
            let Ok(new_range_minus_one) = new_range_wide.try_into() else {
                return false;
            };
            let new_range: usize = new_range_minus_one;
            let new_range = new_range + 1;

            if new_range > MAX_FLAT_RANGE {
                return false;
            }

            if min_wide < *base {
                let Ok(shift) = (*base - new_min).try_into() else {
                    return false;
                };
                let shift: usize = shift;
                let old_len = map.len();
                map.resize(new_range, EMPTY_GROUP);
                map.copy_within(0..old_len, shift);
                map[..shift].fill(EMPTY_GROUP);
                *base = new_min;
            } else {
                map.resize(new_range, EMPTY_GROUP);
            }
            true
        } else {
            // First non-null values — initialise.
            let range_wide = max_wide - min_wide;
            let Ok(range_minus_one) = range_wide.try_into() else {
                return false;
            };
            let range: usize = range_minus_one;
            let range = range + 1;

            if range > MAX_FLAT_RANGE {
                return false;
            }

            self.map_and_base = Some((vec![EMPTY_GROUP; range], min_wide));
            true
        }
    }

    /// Intern an entire batch assuming the flat map already covers the value
    /// range.  No range checks are performed in the inner loop.
    fn intern_batch_in_range(
        &mut self,
        array: &PrimitiveArray<T>,
        groups: &mut Vec<usize>,
    ) {
        // Destructure to get independent mutable borrows of each field.
        let Self {
            values,
            null_group,
            map_and_base,
            ..
        } = self;

        let Some((map, base)) = map_and_base else {
            // No flat map (entire batch is null).
            let g = *null_group.get_or_insert_with(|| {
                let g = values.len();
                values.push(Default::default());
                g
            });
            groups.resize(array.len(), g);
            return;
        };

        let raw_values = array.values();

        if array.null_count() == 0 {
            // Tight loop — no null checks.
            for &key in raw_values.iter() {
                let idx: usize = (key.to_wide() - *base).try_into().unwrap_or(0);
                let slot = &mut map[idx];
                if *slot == EMPTY_GROUP {
                    let g = values.len();
                    *slot = g as u32;
                    values.push(key);
                    groups.push(g);
                } else {
                    groups.push(*slot as usize);
                }
            }
        } else {
            for i in 0..array.len() {
                if array.is_null(i) {
                    let g = *null_group.get_or_insert_with(|| {
                        let g = values.len();
                        values.push(Default::default());
                        g
                    });
                    groups.push(g);
                } else {
                    let key = raw_values[i];
                    let idx: usize = (key.to_wide() - *base).try_into().unwrap_or(0);
                    let slot = &mut map[idx];
                    if *slot == EMPTY_GROUP {
                        let g = values.len();
                        *slot = g as u32;
                        values.push(key);
                        groups.push(g);
                    } else {
                        groups.push(*slot as usize);
                    }
                }
            }
        }
    }

    /// Convert this flat state into a [`GroupValuesPrimitive`] by re-interning
    /// existing values.
    fn into_primitive(self) -> GroupValuesPrimitive<T>
    where
        T::Native: HashValue,
    {
        let mut primitive = GroupValuesPrimitive::<T>::new(self.data_type.clone());

        if self.values.is_empty() {
            return primitive;
        }

        let len = self.values.len();
        let nulls = self.null_group.map(|idx| {
            let mut builder = NullBufferBuilder::new(len);
            builder.append_n_non_nulls(idx);
            builder.append_null();
            builder.append_n_non_nulls(len - idx - 1);
            builder.finish().unwrap()
        });

        let array = PrimitiveArray::<T>::new(self.values.into(), nulls)
            .with_data_type(self.data_type);
        let array_ref: ArrayRef = Arc::new(array);

        let mut groups = vec![];
        primitive.intern(&[array_ref], &mut groups).unwrap();

        primitive
    }

    fn size(&self) -> usize {
        let map_size = self
            .map_and_base
            .as_ref()
            .map(|(m, _)| m.allocated_size())
            .unwrap_or(0);
        map_size + self.values.allocated_size()
    }
}

// ---------------------------------------------------------------------------
// Public wrapper: GroupValuesFlatMap
// ---------------------------------------------------------------------------

/// Adaptive [`GroupValues`] for integer-typed columns.
///
/// On the first batch the value range is observed and a flat direct-address
/// array is allocated.  Subsequent batches may expand the array dynamically.
/// If the range ever exceeds [`MAX_FLAT_RANGE`], the state is converted to a
/// [`GroupValuesPrimitive`] and all future operations are delegated to it with
/// **zero per-value overhead** (the mode is checked once per batch).
pub struct GroupValuesFlatMap<T: ArrowPrimitiveType>
where
    T::Native: HashValue + FlatMappable,
{
    data_type: DataType,
    inner: Inner<T>,
}

impl<T: ArrowPrimitiveType> GroupValuesFlatMap<T>
where
    T::Native: HashValue + FlatMappable,
{
    pub fn new(data_type: DataType) -> Self {
        assert!(PrimitiveArray::<T>::is_compatible(&data_type));
        Self {
            data_type: data_type.clone(),
            inner: Inner::Flat(FlatState::new(data_type)),
        }
    }

    /// Convert from flat mode to hash mode, delegating to
    /// [`GroupValuesPrimitive`].
    fn convert_flat_to_hash(&mut self) {
        let old = std::mem::replace(
            &mut self.inner,
            Inner::Flat(FlatState::new(self.data_type.clone())),
        );
        let Inner::Flat(flat) = old else {
            unreachable!()
        };
        self.inner = Inner::Hash(flat.into_primitive());
    }
}

impl<T: ArrowPrimitiveType> GroupValues for GroupValuesFlatMap<T>
where
    T::Native: HashValue + FlatMappable,
{
    fn intern(&mut self, cols: &[ArrayRef], groups: &mut Vec<usize>) -> Result<()> {
        // ---- Per-batch mode check (once, not per-value) ----
        if let Inner::Hash(primitive) = &mut self.inner {
            return primitive.intern(cols, groups);
        }

        // ---- Flat mode: batch-oriented processing ----
        assert_eq!(cols.len(), 1);
        groups.clear();
        let array = cols[0].as_primitive::<T>();
        groups.reserve(array.len());

        // 1. Compute min/max of non-null values (single pass).
        let min_max = {
            let raw = array.values();
            let mut iter_positions = (0..array.len()).filter(|&i| !array.is_null(i));
            iter_positions.next().map(|first| {
                let first_val = raw[first];
                iter_positions.fold((first_val, first_val), |(mn, mx), i| {
                    let v = raw[i];
                    (if v < mn { v } else { mn }, if v > mx { v } else { mx })
                })
            })
        };

        // 2. Ensure the flat map covers the batch range (expand once).
        if let Some((batch_min, batch_max)) = min_max {
            let range_ok = {
                let Inner::Flat(flat) = &mut self.inner else {
                    unreachable!()
                };
                flat.ensure_range(batch_min, batch_max)
            };

            if !range_ok {
                // Range too large — convert to hash and delegate entire batch.
                self.convert_flat_to_hash();
                let Inner::Hash(primitive) = &mut self.inner else {
                    unreachable!()
                };
                return primitive.intern(cols, groups);
            }
        }

        // 3. Tight inner loop — no range checks, no per-value function calls.
        let Inner::Flat(flat) = &mut self.inner else {
            unreachable!()
        };
        flat.intern_batch_in_range(array, groups);

        Ok(())
    }

    fn size(&self) -> usize {
        match &self.inner {
            Inner::Flat(flat) => flat.size(),
            Inner::Hash(primitive) => primitive.size(),
        }
    }

    fn is_empty(&self) -> bool {
        match &self.inner {
            Inner::Flat(flat) => flat.values.is_empty(),
            Inner::Hash(primitive) => primitive.is_empty(),
        }
    }

    fn len(&self) -> usize {
        match &self.inner {
            Inner::Flat(flat) => flat.values.len(),
            Inner::Hash(primitive) => primitive.len(),
        }
    }

    fn emit(&mut self, emit_to: EmitTo) -> Result<Vec<ArrayRef>> {
        match &mut self.inner {
            Inner::Hash(primitive) => primitive.emit(emit_to),
            Inner::Flat(flat) => {
                fn build_primitive<T: ArrowPrimitiveType>(
                    values: Vec<T::Native>,
                    null_idx: Option<usize>,
                ) -> PrimitiveArray<T> {
                    let nulls = null_idx.map(|null_idx| {
                        let mut buffer = NullBufferBuilder::new(values.len());
                        buffer.append_n_non_nulls(null_idx);
                        buffer.append_null();
                        buffer.append_n_non_nulls(values.len() - null_idx - 1);
                        buffer.finish().unwrap()
                    });
                    PrimitiveArray::<T>::new(values.into(), nulls)
                }

                let array: PrimitiveArray<T> = match emit_to {
                    EmitTo::All => {
                        flat.map_and_base = None;
                        build_primitive(
                            std::mem::take(&mut flat.values),
                            flat.null_group.take(),
                        )
                    }
                    EmitTo::First(n) => {
                        if let Some((map, _)) = &mut flat.map_and_base {
                            for entry in map.iter_mut() {
                                if *entry != EMPTY_GROUP {
                                    let g = *entry as usize;
                                    if g < n {
                                        *entry = EMPTY_GROUP;
                                    } else {
                                        *entry = (g - n) as u32;
                                    }
                                }
                            }
                        }

                        let null_group = match &mut flat.null_group {
                            Some(v) if *v >= n => {
                                *v -= n;
                                None
                            }
                            Some(_) => flat.null_group.take(),
                            None => None,
                        };

                        let mut remaining = flat.values.split_off(n);
                        std::mem::swap(&mut flat.values, &mut remaining);
                        build_primitive(remaining, null_group)
                    }
                };

                Ok(vec![Arc::new(array.with_data_type(flat.data_type.clone()))])
            }
        }
    }

    fn clear_shrink(&mut self, num_rows: usize) {
        match &mut self.inner {
            Inner::Hash(primitive) => primitive.clear_shrink(num_rows),
            Inner::Flat(flat) => {
                flat.map_and_base = None;
                flat.values.clear();
                flat.values.shrink_to(num_rows);
                flat.null_group = None;
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Array, Int8Array, Int32Array, UInt8Array};
    use arrow::datatypes::{Int8Type, Int32Type, UInt8Type};

    #[test]
    fn test_uint8_basic() {
        let mut gv = GroupValuesFlatMap::<UInt8Type>::new(DataType::UInt8);
        let mut groups = vec![];

        let arr: ArrayRef = Arc::new(UInt8Array::from(vec![5, 10, 5, 10, 255]));
        gv.intern(&[arr], &mut groups).unwrap();
        assert_eq!(groups, vec![0, 1, 0, 1, 2]);
        assert_eq!(gv.len(), 3);
    }

    #[test]
    fn test_int8_with_negatives() {
        let mut gv = GroupValuesFlatMap::<Int8Type>::new(DataType::Int8);
        let mut groups = vec![];

        let arr: ArrayRef = Arc::new(Int8Array::from(vec![
            Some(-128),
            Some(127),
            None,
            Some(-128),
        ]));
        gv.intern(&[arr], &mut groups).unwrap();
        assert_eq!(groups, vec![0, 1, 2, 0]);
        assert_eq!(gv.len(), 3);
    }

    #[test]
    fn test_int32_low_cardinality_stays_flat() {
        let mut gv = GroupValuesFlatMap::<Int32Type>::new(DataType::Int32);
        let mut groups = vec![];

        let arr: ArrayRef = Arc::new(Int32Array::from(vec![100, 200, 100, 300, 200]));
        gv.intern(&[arr], &mut groups).unwrap();
        assert_eq!(groups, vec![0, 1, 0, 2, 1]);
        assert_eq!(gv.len(), 3);
        assert!(matches!(gv.inner, Inner::Flat(_)));
    }

    #[test]
    fn test_int32_high_cardinality_falls_back_to_hash() {
        let mut gv = GroupValuesFlatMap::<Int32Type>::new(DataType::Int32);
        let mut groups = vec![];

        // Values spanning a huge range → should fall back to hash
        let arr: ArrayRef = Arc::new(Int32Array::from(vec![0, i32::MAX, i32::MIN]));
        gv.intern(&[arr], &mut groups).unwrap();
        assert_eq!(groups, vec![0, 1, 2]);
        assert_eq!(gv.len(), 3);
        assert!(matches!(gv.inner, Inner::Hash(_)));
    }

    #[test]
    fn test_flat_expands_left() {
        let mut gv = GroupValuesFlatMap::<Int32Type>::new(DataType::Int32);
        let mut groups = vec![];

        let arr1: ArrayRef = Arc::new(Int32Array::from(vec![100, 101]));
        gv.intern(&[arr1], &mut groups).unwrap();
        assert_eq!(groups, vec![0, 1]);

        let arr2: ArrayRef = Arc::new(Int32Array::from(vec![95, 100]));
        gv.intern(&[arr2], &mut groups).unwrap();
        assert_eq!(groups, vec![2, 0]);
        assert!(matches!(gv.inner, Inner::Flat(_)));
    }

    #[test]
    fn test_flat_expands_right() {
        let mut gv = GroupValuesFlatMap::<Int32Type>::new(DataType::Int32);
        let mut groups = vec![];

        let arr1: ArrayRef = Arc::new(Int32Array::from(vec![10]));
        gv.intern(&[arr1], &mut groups).unwrap();

        let arr2: ArrayRef = Arc::new(Int32Array::from(vec![10, 500]));
        gv.intern(&[arr2], &mut groups).unwrap();
        assert_eq!(groups, vec![0, 1]);
        assert!(matches!(gv.inner, Inner::Flat(_)));
    }

    #[test]
    fn test_emit_all() {
        let mut gv = GroupValuesFlatMap::<UInt8Type>::new(DataType::UInt8);
        let mut groups = vec![];

        let arr: ArrayRef = Arc::new(UInt8Array::from(vec![Some(1), None, Some(2)]));
        gv.intern(&[arr], &mut groups).unwrap();
        assert_eq!(groups, vec![0, 1, 2]);

        let emitted = gv.emit(EmitTo::All).unwrap();
        assert_eq!(emitted.len(), 1);
        let result = emitted[0].as_primitive::<UInt8Type>();
        assert_eq!(result.value(0), 1);
        assert!(result.is_null(1));
        assert_eq!(result.value(2), 2);
        assert!(gv.is_empty());
    }

    #[test]
    fn test_emit_first() {
        let mut gv = GroupValuesFlatMap::<Int32Type>::new(DataType::Int32);
        let mut groups = vec![];

        let arr: ArrayRef = Arc::new(Int32Array::from(vec![10, 20, 30]));
        gv.intern(&[arr], &mut groups).unwrap();
        assert_eq!(groups, vec![0, 1, 2]);

        let emitted = gv.emit(EmitTo::First(2)).unwrap();
        let result = emitted[0].as_primitive::<Int32Type>();
        assert_eq!(result.len(), 2);
        assert_eq!(result.value(0), 10);
        assert_eq!(result.value(1), 20);
        assert_eq!(gv.len(), 1);

        let arr2: ArrayRef = Arc::new(Int32Array::from(vec![30, 40]));
        gv.intern(&[arr2], &mut groups).unwrap();
        assert_eq!(groups, vec![0, 1]);
        assert_eq!(gv.len(), 2);
    }

    #[test]
    fn test_emit_all_resets_for_reuse() {
        let mut gv = GroupValuesFlatMap::<Int32Type>::new(DataType::Int32);
        let mut groups = vec![];

        let arr: ArrayRef = Arc::new(Int32Array::from(vec![1, 2, 3]));
        gv.intern(&[arr], &mut groups).unwrap();

        gv.emit(EmitTo::All).unwrap();
        assert!(gv.is_empty());

        let arr2: ArrayRef = Arc::new(Int32Array::from(vec![1000, 1001]));
        gv.intern(&[arr2], &mut groups).unwrap();
        assert_eq!(groups, vec![0, 1]);
    }

    #[test]
    fn test_clear_shrink() {
        let mut gv = GroupValuesFlatMap::<UInt8Type>::new(DataType::UInt8);
        let mut groups = vec![];

        let arr: ArrayRef = Arc::new(UInt8Array::from(vec![1u8, 2, 3]));
        gv.intern(&[arr], &mut groups).unwrap();
        assert_eq!(gv.len(), 3);

        gv.clear_shrink(0);
        assert!(gv.is_empty());

        let arr2: ArrayRef = Arc::new(UInt8Array::from(vec![1u8]));
        gv.intern(&[arr2], &mut groups).unwrap();
        assert_eq!(groups, vec![0]);
    }

    #[test]
    fn test_hash_fallback_still_correct() {
        let mut gv = GroupValuesFlatMap::<Int32Type>::new(DataType::Int32);
        let mut groups = vec![];

        let arr: ArrayRef =
            Arc::new(Int32Array::from(vec![0, i32::MAX, i32::MIN, 0, i32::MAX]));
        gv.intern(&[arr], &mut groups).unwrap();
        assert_eq!(groups, vec![0, 1, 2, 0, 1]);
        assert!(matches!(gv.inner, Inner::Hash(_)));

        let arr2: ArrayRef = Arc::new(Int32Array::from(vec![42, 0]));
        gv.intern(&[arr2], &mut groups).unwrap();
        assert_eq!(groups, vec![3, 0]);
        assert_eq!(gv.len(), 4);
    }

    #[test]
    fn test_mid_batch_conversion() {
        let mut gv = GroupValuesFlatMap::<Int32Type>::new(DataType::Int32);
        let mut groups = vec![];

        // Start with a small range, then blow it out mid-batch
        let arr: ArrayRef = Arc::new(Int32Array::from(vec![1, 2, i32::MAX]));
        gv.intern(&[arr], &mut groups).unwrap();
        assert_eq!(groups, vec![0, 1, 2]);
        assert!(matches!(gv.inner, Inner::Hash(_)));
        assert_eq!(gv.len(), 3);

        // Subsequent batch is pure hash delegation
        let arr2: ArrayRef = Arc::new(Int32Array::from(vec![1, 99]));
        gv.intern(&[arr2], &mut groups).unwrap();
        assert_eq!(groups, vec![0, 3]);
    }

    #[test]
    fn test_null_only() {
        let mut gv = GroupValuesFlatMap::<Int32Type>::new(DataType::Int32);
        let mut groups = vec![];

        let arr: ArrayRef = Arc::new(Int32Array::from(vec![None, None]));
        gv.intern(&[arr], &mut groups).unwrap();
        assert_eq!(groups, vec![0, 0]);
        assert_eq!(gv.len(), 1);
        assert!(matches!(gv.inner, Inner::Flat(_)));
    }
}
