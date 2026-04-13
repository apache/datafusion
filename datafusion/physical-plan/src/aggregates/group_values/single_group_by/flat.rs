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
//! the range exceeds a threshold, it automatically falls back to a hash table.
//!
//! This is inspired by DuckDB's "perfect hash aggregate" approach.

use super::primitive::HashValue;
use crate::aggregates::group_values::GroupValues;
use arrow::array::{
    ArrayRef, ArrowNativeTypeOp, ArrowPrimitiveType, NullBufferBuilder, PrimitiveArray,
    cast::AsArray,
};
use arrow::datatypes::DataType;
use datafusion_common::Result;
use datafusion_common::hash_utils::RandomState;
use datafusion_common::utils::proxy::VecAllocExt;
use datafusion_expr::EmitTo;
use hashbrown::hash_table::HashTable;
use std::mem::size_of;
use std::sync::Arc;

/// Maximum number of slots in the flat lookup array before falling back to a
/// hash table. 100K slots × 4 bytes = 400 KB.
const MAX_FLAT_RANGE: usize = 100_000;

/// Sentinel value indicating no group is assigned to a flat-map slot.
const EMPTY_GROUP: u32 = u32::MAX;

/// Integer native types that can be converted to `i128` for range computation.
///
/// All Arrow integer native types (i8–i128, u8–u64) implement this.
pub(crate) trait FlatMappable: Copy + Default {
    fn to_i128(self) -> i128;
}

macro_rules! impl_flat_mappable {
    ($($t:ty),+) => {
        $(impl FlatMappable for $t {
            #[inline(always)]
            fn to_i128(self) -> i128 { self as i128 }
        })+
    };
}

impl_flat_mappable!(i8, i16, i32, i64, i128, u8, u16, u32, u64);

/// Internal lookup strategy.
enum Strategy {
    /// No non-null values seen yet.
    Initial,
    /// Flat direct-address lookup.
    /// `map[(to_i128(value) - base) as usize]` → group_index.
    Flat { map: Vec<u32>, base: i128 },
    /// Hash table fallback (same approach as [`GroupValuesPrimitive`]).
    Hash { map: HashTable<(usize, u64)> },
}

/// Adaptive [`GroupValues`] for integer-typed columns.
///
/// On the first batch of data the value range is observed and a flat
/// direct-address array is allocated. Subsequent batches may expand the array
/// dynamically. If the range ever exceeds [`MAX_FLAT_RANGE`], all existing
/// groups are migrated into a hash table and all future lookups use hashing.
///
/// For low-cardinality data this provides:
/// - **No hash computation**
/// - **No hash collisions**
/// - **Excellent cache locality**
///
/// For high-cardinality data it seamlessly degrades to the same hash-table
/// approach used by [`super::primitive::GroupValuesPrimitive`].
pub struct GroupValuesFlatMap<T: ArrowPrimitiveType>
where
    T::Native: HashValue + FlatMappable,
{
    /// Output data type (preserves timezone, decimal params, etc.).
    data_type: DataType,
    /// `values[group_index]` = the native value for that group.
    values: Vec<T::Native>,
    /// Group index for NULL, if encountered.
    null_group: Option<usize>,
    /// Random state for hashing (used only after fallback to Hash mode).
    random_state: RandomState,
    /// Current lookup strategy.
    strategy: Strategy,
}

impl<T: ArrowPrimitiveType> GroupValuesFlatMap<T>
where
    T::Native: HashValue + FlatMappable,
{
    pub fn new(data_type: DataType) -> Self {
        assert!(PrimitiveArray::<T>::is_compatible(&data_type));
        Self {
            data_type,
            values: Vec::with_capacity(128),
            null_group: None,
            random_state: crate::aggregates::AGGREGATION_HASH_SEED,
            strategy: Strategy::Initial,
        }
    }

    /// Intern a single non-null value, returning its group index.
    ///
    /// The fast path (flat-mode, value in range) is inlined for performance.
    #[inline(always)]
    fn intern_value(&mut self, key: T::Native) -> usize {
        // Fast path: flat mode and value is within the current range.
        if let Strategy::Flat { map, base } = &mut self.strategy {
            let offset = key.to_i128() - *base;
            if offset >= 0 {
                let idx = offset as usize;
                if idx < map.len() {
                    let existing = map[idx];
                    if existing != EMPTY_GROUP {
                        return existing as usize;
                    }
                    let g = self.values.len();
                    map[idx] = g as u32;
                    self.values.push(key);
                    return g;
                }
            }
        }
        // Slow path: initialization, expansion, or hash lookup.
        self.intern_value_slow(key)
    }

    /// Slow path for [`intern_value`]: handles first-value initialization,
    /// flat-map expansion, flat→hash conversion, and hash-mode interning.
    #[cold]
    fn intern_value_slow(&mut self, key: T::Native) -> usize {
        loop {
            match &mut self.strategy {
                Strategy::Initial => {
                    let g = self.values.len();
                    self.values.push(key);
                    self.strategy = Strategy::Flat {
                        map: vec![g as u32; 1],
                        base: key.to_i128(),
                    };
                    return g;
                }
                Strategy::Flat { map, base } => {
                    let key_i128 = key.to_i128();
                    let offset = key_i128 - *base;

                    // In-range but EMPTY_GROUP (we got here from the fast path)
                    if offset >= 0 && (offset as usize) < map.len() {
                        let idx = offset as usize;
                        let g = self.values.len();
                        map[idx] = g as u32;
                        self.values.push(key);
                        return g;
                    }

                    // Compute the expanded range
                    let current_max = *base + map.len() as i128 - 1;
                    let new_min = (*base).min(key_i128);
                    let new_max = current_max.max(key_i128);
                    let new_range = (new_max - new_min + 1) as usize;

                    if new_range <= MAX_FLAT_RANGE {
                        if key_i128 < *base {
                            // Expand left: shift existing entries right
                            let shift = (*base - new_min) as usize;
                            let old_len = map.len();
                            map.resize(new_range, EMPTY_GROUP);
                            map.copy_within(0..old_len, shift);
                            map[..shift].fill(EMPTY_GROUP);
                            *base = new_min;
                        } else {
                            // Expand right
                            map.resize(new_range, EMPTY_GROUP);
                        }

                        let idx = (key_i128 - *base) as usize;
                        let g = self.values.len();
                        map[idx] = g as u32;
                        self.values.push(key);
                        return g;
                    }

                    // Range too large — fall through to convert to hash.
                }
                Strategy::Hash { map } => {
                    return Self::intern_hash(
                        map,
                        &mut self.values,
                        &self.random_state,
                        key,
                    );
                }
            }

            // Reached only when Flat range exceeded MAX_FLAT_RANGE.
            self.convert_to_hash();
        }
    }

    /// Rebuild a hash table from the existing `values` vector.
    fn convert_to_hash(&mut self) {
        let mut hash_map = HashTable::with_capacity(self.values.len());
        for (group_idx, &value) in self.values.iter().enumerate() {
            if self.null_group == Some(group_idx) {
                continue; // skip the null placeholder
            }
            let hash = value.hash(&self.random_state);
            hash_map.insert_unique(hash, (group_idx, hash), |&(_, h)| h);
        }
        self.strategy = Strategy::Hash { map: hash_map };
    }

    /// Intern a value using the hash table (mirrors `GroupValuesPrimitive`).
    fn intern_hash(
        map: &mut HashTable<(usize, u64)>,
        values: &mut Vec<T::Native>,
        random_state: &RandomState,
        key: T::Native,
    ) -> usize {
        let hash = key.hash(random_state);
        let entry = map.entry(
            hash,
            |&(g, h)| unsafe { hash == h && values.get_unchecked(g).is_eq(key) },
            |&(_, h)| h,
        );

        match entry {
            hashbrown::hash_table::Entry::Occupied(o) => o.get().0,
            hashbrown::hash_table::Entry::Vacant(v) => {
                let g = values.len();
                v.insert((g, hash));
                values.push(key);
                g
            }
        }
    }
}

impl<T: ArrowPrimitiveType> GroupValues for GroupValuesFlatMap<T>
where
    T::Native: HashValue + FlatMappable,
{
    fn intern(&mut self, cols: &[ArrayRef], groups: &mut Vec<usize>) -> Result<()> {
        assert_eq!(cols.len(), 1);
        groups.clear();

        let array = cols[0].as_primitive::<T>();

        for v in array.iter() {
            let group_id = match v {
                None => *self.null_group.get_or_insert_with(|| {
                    let g = self.values.len();
                    self.values.push(Default::default());
                    g
                }),
                Some(key) => self.intern_value(key),
            };
            groups.push(group_id);
        }
        Ok(())
    }

    fn size(&self) -> usize {
        let strategy_size = match &self.strategy {
            Strategy::Initial => 0,
            Strategy::Flat { map, .. } => map.allocated_size(),
            Strategy::Hash { map } => map.capacity() * size_of::<(usize, u64)>(),
        };
        strategy_size + self.values.allocated_size()
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
                buffer.finish().unwrap()
            });
            PrimitiveArray::<T>::new(values.into(), nulls)
        }

        let array: PrimitiveArray<T> = match emit_to {
            EmitTo::All => {
                self.strategy = Strategy::Initial;
                build_primitive(std::mem::take(&mut self.values), self.null_group.take())
            }
            EmitTo::First(n) => {
                match &mut self.strategy {
                    Strategy::Initial => {}
                    Strategy::Flat { map, .. } => {
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
                    Strategy::Hash { map } => {
                        map.retain(|entry| match entry.0.checked_sub(n) {
                            Some(sub) => {
                                entry.0 = sub;
                                true
                            }
                            None => false,
                        });
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

                let mut remaining = self.values.split_off(n);
                std::mem::swap(&mut self.values, &mut remaining);
                build_primitive(remaining, null_group)
            }
        };

        Ok(vec![Arc::new(array.with_data_type(self.data_type.clone()))])
    }

    fn clear_shrink(&mut self, num_rows: usize) {
        self.strategy = Strategy::Initial;
        self.values.clear();
        self.values.shrink_to(num_rows);
        self.null_group = None;
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

        // Values in a small range → stays in flat mode
        let arr: ArrayRef = Arc::new(Int32Array::from(vec![100, 200, 100, 300, 200]));
        gv.intern(&[arr], &mut groups).unwrap();
        assert_eq!(groups, vec![0, 1, 0, 2, 1]);
        assert_eq!(gv.len(), 3);
        assert!(matches!(gv.strategy, Strategy::Flat { .. }));
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
        assert!(matches!(gv.strategy, Strategy::Hash { .. }));
    }

    #[test]
    fn test_flat_expands_left() {
        let mut gv = GroupValuesFlatMap::<Int32Type>::new(DataType::Int32);
        let mut groups = vec![];

        // First batch: base = 100
        let arr1: ArrayRef = Arc::new(Int32Array::from(vec![100, 101]));
        gv.intern(&[arr1], &mut groups).unwrap();
        assert_eq!(groups, vec![0, 1]);

        // Second batch: value 95 is below base → expand left
        let arr2: ArrayRef = Arc::new(Int32Array::from(vec![95, 100]));
        gv.intern(&[arr2], &mut groups).unwrap();
        assert_eq!(groups, vec![2, 0]);
        assert!(matches!(gv.strategy, Strategy::Flat { .. }));
    }

    #[test]
    fn test_flat_expands_right() {
        let mut gv = GroupValuesFlatMap::<Int32Type>::new(DataType::Int32);
        let mut groups = vec![];

        let arr1: ArrayRef = Arc::new(Int32Array::from(vec![10]));
        gv.intern(&[arr1], &mut groups).unwrap();
        assert_eq!(groups, vec![0]);

        let arr2: ArrayRef = Arc::new(Int32Array::from(vec![10, 500]));
        gv.intern(&[arr2], &mut groups).unwrap();
        assert_eq!(groups, vec![0, 1]);
        assert!(matches!(gv.strategy, Strategy::Flat { .. }));
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

        // Emit first 2 groups
        let emitted = gv.emit(EmitTo::First(2)).unwrap();
        let result = emitted[0].as_primitive::<Int32Type>();
        assert_eq!(result.len(), 2);
        assert_eq!(result.value(0), 10);
        assert_eq!(result.value(1), 20);

        // Remaining group (30) should now be at index 0
        assert_eq!(gv.len(), 1);

        // Interning the same value should find the shifted group
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
        assert_eq!(gv.len(), 3);

        gv.emit(EmitTo::All).unwrap();
        assert!(gv.is_empty());
        assert!(matches!(gv.strategy, Strategy::Initial));

        // Reuse with different values
        let arr2: ArrayRef = Arc::new(Int32Array::from(vec![1000, 1001]));
        gv.intern(&[arr2], &mut groups).unwrap();
        assert_eq!(groups, vec![0, 1]);
        assert_eq!(gv.len(), 2);
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
        assert_eq!(gv.len(), 1);
    }

    #[test]
    fn test_hash_fallback_still_correct() {
        let mut gv = GroupValuesFlatMap::<Int32Type>::new(DataType::Int32);
        let mut groups = vec![];

        // Force hash fallback with wide-range values
        let arr: ArrayRef =
            Arc::new(Int32Array::from(vec![0, i32::MAX, i32::MIN, 0, i32::MAX]));
        gv.intern(&[arr], &mut groups).unwrap();
        assert_eq!(groups, vec![0, 1, 2, 0, 1]);
        assert!(matches!(gv.strategy, Strategy::Hash { .. }));

        // Continue interning in hash mode
        let arr2: ArrayRef = Arc::new(Int32Array::from(vec![42, 0]));
        gv.intern(&[arr2], &mut groups).unwrap();
        assert_eq!(groups, vec![3, 0]);
        assert_eq!(gv.len(), 4);
    }

    #[test]
    fn test_null_only() {
        let mut gv = GroupValuesFlatMap::<Int32Type>::new(DataType::Int32);
        let mut groups = vec![];

        let arr: ArrayRef = Arc::new(Int32Array::from(vec![None, None]));
        gv.intern(&[arr], &mut groups).unwrap();
        assert_eq!(groups, vec![0, 0]);
        assert_eq!(gv.len(), 1);
        assert!(matches!(gv.strategy, Strategy::Initial));
    }
}
