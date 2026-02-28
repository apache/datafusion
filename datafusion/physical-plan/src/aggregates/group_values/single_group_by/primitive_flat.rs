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

//! A [`GroupValues`] implementation that uses direct indexing for primitive
//! types with a small value range.

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

/// A [`GroupValues`] storing a single column of primitive values using direct
/// indexing rather than a hash table.
///
/// This is faster than the hash-table-based [`GroupValuesPrimitive`] when the
/// range of values (`max - min + 1`) is small enough for a direct-indexed vec.
/// Each possible value maps to a slot in the vec via `(value - min)`, giving
/// O(1) group lookups with no hashing overhead.
///
/// Used automatically for:
/// - `Int8`/`UInt8` (range ≤ 256, always worth it)
/// - `Int16`/`UInt16` (range ≤ 65536, always worth it)
/// - Larger integer types when column statistics indicate a small value range
///
/// [`GroupValuesPrimitive`]: super::primitive::GroupValuesPrimitive
pub struct GroupValuesPrimitiveFlat<T: ArrowPrimitiveType>
where
    T::Native: FlatIndex,
{
    /// The data type of the output array
    data_type: DataType,
    /// Direct mapping from value (as offset from min) to group index.
    /// `NO_GROUP` (usize::MAX) indicates no group has been assigned.
    map: Vec<usize>,
    /// The minimum value (used as offset for indexing into `map`)
    min: T::Native,
    /// The group index of the null value if any
    null_group: Option<usize>,
    /// The values for each group index (group_id → value), needed for emit
    values: Vec<T::Native>,
}

impl<T: ArrowPrimitiveType> GroupValuesPrimitiveFlat<T>
where
    T::Native: FlatIndex,
{
    /// Create a new `GroupValuesPrimitiveFlat`.
    ///
    /// * `data_type` - The Arrow data type for the output array
    /// * `min` - The minimum possible value (used as index offset)
    /// * `range` - Number of possible distinct values (`max - min + 1`)
    pub fn new(data_type: DataType, min: T::Native, range: usize) -> Self {
        assert!(PrimitiveArray::<T>::is_compatible(&data_type));
        Self {
            data_type,
            map: vec![NO_GROUP; range],
            min,
            null_group: None,
            values: Vec::with_capacity(128),
        }
    }
}

impl<T: ArrowPrimitiveType> GroupValues for GroupValuesPrimitiveFlat<T>
where
    T::Native: FlatIndex,
{
    fn intern(&mut self, cols: &[ArrayRef], groups: &mut Vec<usize>) -> Result<()> {
        assert_eq!(cols.len(), 1);
        groups.clear();
        let arr = cols[0].as_primitive::<T>();

        // Fast path: no nulls in the array
        if arr.null_count() == 0 {
            for &key in arr.values().iter() {
                let index = key.index_from(self.min);
                // SAFETY: values are guaranteed within range for types that
                // use full-range flat indexing (i8/u8/i16/u16), and verified
                // via exact statistics for larger types.
                let slot = unsafe { self.map.get_unchecked_mut(index) };
                if *slot == NO_GROUP {
                    let group_id = self.values.len();
                    *slot = group_id;
                    self.values.push(key);
                    groups.push(group_id);
                } else {
                    groups.push(*slot);
                }
            }
        } else {
            for v in arr.iter() {
                let group_id = match v {
                    None => *self.null_group.get_or_insert_with(|| {
                        let group_id = self.values.len();
                        self.values.push(Default::default());
                        group_id
                    }),
                    Some(key) => {
                        let index = key.index_from(self.min);
                        let slot = unsafe { self.map.get_unchecked_mut(index) };
                        if *slot == NO_GROUP {
                            let group_id = self.values.len();
                            *slot = group_id;
                            self.values.push(key);
                            group_id
                        } else {
                            *slot
                        }
                    }
                };
                groups.push(group_id);
            }
        }

        Ok(())
    }

    fn size(&self) -> usize {
        self.map.len() * size_of::<usize>()
            + self.values.capacity() * size_of::<T::Native>()
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
                self.map.fill(NO_GROUP);
                build_primitive(std::mem::take(&mut self.values), self.null_group.take())
            }
            EmitTo::First(n) => {
                // Update the flat map: shift group indices down by n, remove < n
                for slot in self.map.iter_mut() {
                    if *slot != NO_GROUP {
                        match slot.checked_sub(n) {
                            Some(new_idx) => *slot = new_idx,
                            None => *slot = NO_GROUP,
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
                let mut split = self.values.split_off(n);
                std::mem::swap(&mut self.values, &mut split);
                build_primitive(split, null_group)
            }
        };

        Ok(vec![Arc::new(array.with_data_type(self.data_type.clone()))])
    }

    fn clear_shrink(&mut self, _num_rows: usize) {
        self.values.clear();
        // Don't deallocate the map - its size is fixed based on the value range
        self.map.fill(NO_GROUP);
    }
}

/// Trait for primitive native types that can be used as direct (flat) indices.
///
/// Converts between a native value and a zero-based index, given a known
/// minimum value. This enables O(1) group lookups via `value - min`.
pub(crate) trait FlatIndex: Copy + Default + Send + Sync + 'static {
    /// Convert this value to a zero-based index: `self - min` as usize.
    fn index_from(self, min: Self) -> usize;
}

macro_rules! impl_flat_index_signed {
    ($signed:ty, $unsigned:ty) => {
        impl FlatIndex for $signed {
            #[inline]
            fn index_from(self, min: Self) -> usize {
                self.wrapping_sub(min) as $unsigned as usize
            }
        }
    };
}

macro_rules! impl_flat_index_unsigned {
    ($unsigned:ty) => {
        impl FlatIndex for $unsigned {
            #[inline]
            fn index_from(self, min: Self) -> usize {
                (self.wrapping_sub(min)) as usize
            }
        }
    };
}

impl_flat_index_signed!(i8, u8);
impl_flat_index_signed!(i16, u16);
impl_flat_index_signed!(i32, u32);
impl_flat_index_signed!(i64, u64);
impl_flat_index_unsigned!(u8);
impl_flat_index_unsigned!(u16);
impl_flat_index_unsigned!(u32);
impl_flat_index_unsigned!(u64);

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::types::{Int8Type, Int32Type, UInt8Type};
    use arrow::array::{Int8Array, Int32Array, UInt8Array};

    #[test]
    fn test_flat_index_signed() {
        // i8: -128 is min, 127 is max
        assert_eq!((-128i8).index_from(-128i8), 0);
        assert_eq!((-127i8).index_from(-128i8), 1);
        assert_eq!(0i8.index_from(-128i8), 128);
        assert_eq!(127i8.index_from(-128i8), 255);

        // i32 with small range
        assert_eq!(10i32.index_from(5i32), 5);
        assert_eq!(5i32.index_from(5i32), 0);
        assert_eq!(100i32.index_from(5i32), 95);
    }

    #[test]
    fn test_flat_index_unsigned() {
        assert_eq!(0u8.index_from(0u8), 0);
        assert_eq!(255u8.index_from(0u8), 255);
        assert_eq!(10u8.index_from(5u8), 5);
    }

    #[test]
    fn test_basic_intern_emit() {
        let mut gv =
            GroupValuesPrimitiveFlat::<Int8Type>::new(DataType::Int8, i8::MIN, 256);
        let mut groups = vec![];

        // First batch
        let arr: ArrayRef = Arc::new(Int8Array::from(vec![1, 2, 1, 3]));
        gv.intern(&[arr], &mut groups).unwrap();
        assert_eq!(groups, vec![0, 1, 0, 2]);
        assert_eq!(gv.len(), 3);

        // Second batch with mix of old and new values
        let arr: ArrayRef = Arc::new(Int8Array::from(vec![3, 4, 1]));
        gv.intern(&[arr], &mut groups).unwrap();
        assert_eq!(groups, vec![2, 3, 0]);
        assert_eq!(gv.len(), 4);

        // Emit all
        let result = gv.emit(EmitTo::All).unwrap();
        assert_eq!(result.len(), 1);
        let values = result[0].as_primitive::<Int8Type>();
        assert_eq!(values.values().as_ref(), &[1, 2, 3, 4]);
        assert_eq!(values.null_count(), 0);
        assert!(gv.is_empty());
    }

    #[test]
    fn test_null_handling() {
        let mut gv =
            GroupValuesPrimitiveFlat::<Int8Type>::new(DataType::Int8, i8::MIN, 256);
        let mut groups = vec![];

        let arr: ArrayRef = Arc::new(Int8Array::from(vec![Some(1), None, Some(1), None]));
        gv.intern(&[arr], &mut groups).unwrap();
        assert_eq!(groups, vec![0, 1, 0, 1]);
        assert_eq!(gv.len(), 2);

        let result = gv.emit(EmitTo::All).unwrap();
        let values = result[0].as_primitive::<Int8Type>();
        assert_eq!(values.len(), 2);
        assert!(values.is_valid(0)); // value 1
        assert!(!values.is_valid(1)); // null
    }

    #[test]
    fn test_emit_first() {
        let mut gv =
            GroupValuesPrimitiveFlat::<Int8Type>::new(DataType::Int8, i8::MIN, 256);
        let mut groups = vec![];

        let arr: ArrayRef = Arc::new(Int8Array::from(vec![10, 20, 30]));
        gv.intern(&[arr], &mut groups).unwrap();
        assert_eq!(groups, vec![0, 1, 2]);

        // Emit first 2 groups
        let result = gv.emit(EmitTo::First(2)).unwrap();
        let values = result[0].as_primitive::<Int8Type>();
        assert_eq!(values.values().as_ref(), &[10, 20]);
        assert_eq!(gv.len(), 1);

        // Remaining group (30) should now be at index 0
        let arr: ArrayRef = Arc::new(Int8Array::from(vec![30, 40]));
        gv.intern(&[arr], &mut groups).unwrap();
        assert_eq!(groups, vec![0, 1]); // 30 is now group 0, 40 is new group 1
    }

    #[test]
    fn test_boundary_values() {
        let mut gv =
            GroupValuesPrimitiveFlat::<Int8Type>::new(DataType::Int8, i8::MIN, 256);
        let mut groups = vec![];

        let arr: ArrayRef = Arc::new(Int8Array::from(vec![-128, 0, 127]));
        gv.intern(&[arr], &mut groups).unwrap();
        assert_eq!(groups, vec![0, 1, 2]);

        let result = gv.emit(EmitTo::All).unwrap();
        let values = result[0].as_primitive::<Int8Type>();
        assert_eq!(values.values().as_ref(), &[-128, 0, 127]);
    }

    #[test]
    fn test_uint8_full_range() {
        let mut gv =
            GroupValuesPrimitiveFlat::<UInt8Type>::new(DataType::UInt8, 0u8, 256);
        let mut groups = vec![];

        let arr: ArrayRef = Arc::new(UInt8Array::from(vec![0, 128, 255]));
        gv.intern(&[arr], &mut groups).unwrap();
        assert_eq!(groups, vec![0, 1, 2]);
    }

    #[test]
    fn test_i32_with_stats_range() {
        // Simulate stats-based flat indexing for i32 with range [100, 200]
        let mut gv =
            GroupValuesPrimitiveFlat::<Int32Type>::new(DataType::Int32, 100, 101);
        let mut groups = vec![];

        let arr: ArrayRef = Arc::new(Int32Array::from(vec![100, 150, 200, 100]));
        gv.intern(&[arr], &mut groups).unwrap();
        assert_eq!(groups, vec![0, 1, 2, 0]);

        let result = gv.emit(EmitTo::All).unwrap();
        let values = result[0].as_primitive::<Int32Type>();
        assert_eq!(values.values().as_ref(), &[100, 150, 200]);
    }

    #[test]
    fn test_clear_shrink() {
        let mut gv =
            GroupValuesPrimitiveFlat::<Int8Type>::new(DataType::Int8, i8::MIN, 256);
        let mut groups = vec![];

        let arr: ArrayRef = Arc::new(Int8Array::from(vec![1, 2, 3]));
        gv.intern(&[arr], &mut groups).unwrap();
        assert_eq!(gv.len(), 3);

        gv.clear_shrink(0);
        assert!(gv.is_empty());

        // Should work again after clear
        let arr: ArrayRef = Arc::new(Int8Array::from(vec![1, 2]));
        gv.intern(&[arr], &mut groups).unwrap();
        assert_eq!(groups, vec![0, 1]);
    }
}
