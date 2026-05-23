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
use arrow::array::{
    Array, ArrayRef, ArrowPrimitiveType, NullBufferBuilder, PrimitiveArray, cast::AsArray,
};
use arrow::datatypes::DataType;
use datafusion_common::Result;
use datafusion_expr::EmitTo;
use num_traits::AsPrimitive;
use std::mem::size_of;
use std::sync::Arc;

/// Sentinel value indicating an unoccupied slot in the flat array.
const EMPTY: u32 = u32::MAX;

/// A [`GroupValues`] implementation using direct array indexing for integer-typed
/// GROUP BY columns with a known, bounded value range.
///
/// Instead of hashing each key and probing a hash table, this computes
/// `index = key_as_u64.wrapping_sub(offset)` to directly index into a flat array,
/// yielding O(1) group lookups with no hashing, no collisions, and excellent
/// cache locality for small ranges.
///
/// Inspired by the `ArrayMap` used for perfect hash joins (see `joins/array_map.rs`).
pub struct GroupValuesFlatPrimitive<T: ArrowPrimitiveType>
where
    T::Native: AsPrimitive<u64>,
{
    data_type: DataType,
    /// Maps `key - offset` -> group_id (u32). EMPTY means no group assigned yet.
    slots: Vec<u32>,
    /// The minimum value (as u64 via two's complement) used as the base offset.
    offset: u64,
    /// The group index assigned to NULL values, if any.
    null_group: Option<usize>,
    /// Ordered group values for emit. `values[group_id]` is the original key.
    values: Vec<T::Native>,
}

impl<T: ArrowPrimitiveType> GroupValuesFlatPrimitive<T>
where
    T::Native: AsPrimitive<u64>,
{
    /// Creates a new flat-indexed GroupValues.
    ///
    /// `min_val` and `max_val` define the key range (as u64 via two's complement cast).
    /// The allocated flat array has `max_val - min_val + 1` slots.
    pub fn new(data_type: DataType, min_val: u64, max_val: u64) -> Self {
        assert!(PrimitiveArray::<T>::is_compatible(&data_type));
        let range = max_val.wrapping_sub(min_val);
        let size = (range as usize) + 1;
        Self {
            data_type,
            slots: vec![EMPTY; size],
            offset: min_val,
            null_group: None,
            values: Vec::new(),
        }
    }

    /// Returns the range (number of slots) of this flat map.
    pub fn range(&self) -> usize {
        self.slots.len()
    }
}

impl<T: ArrowPrimitiveType> GroupValues for GroupValuesFlatPrimitive<T>
where
    T::Native: AsPrimitive<u64> + Default + Copy + Send,
{
    fn intern(&mut self, cols: &[ArrayRef], groups: &mut Vec<usize>) -> Result<()> {
        assert_eq!(cols.len(), 1);
        groups.clear();

        let arr = cols[0].as_primitive::<T>();

        if arr.null_count() == 0 {
            for i in 0..arr.len() {
                // SAFETY: null_count == 0 guarantees all values are valid
                let key: u64 = unsafe { arr.value_unchecked(i) }.as_();
                let idx = key.wrapping_sub(self.offset) as usize;
                debug_assert!(idx < self.slots.len());

                let slot = unsafe { self.slots.get_unchecked_mut(idx) };
                let group_id = if *slot == EMPTY {
                    let g = self.values.len() as u32;
                    *slot = g;
                    self.values.push(unsafe { arr.value_unchecked(i) });
                    g as usize
                } else {
                    *slot as usize
                };
                groups.push(group_id);
            }
        } else {
            for i in 0..arr.len() {
                let group_id = if arr.is_null(i) {
                    *self.null_group.get_or_insert_with(|| {
                        let g = self.values.len();
                        self.values.push(Default::default());
                        g
                    })
                } else {
                    let key: u64 = unsafe { arr.value_unchecked(i) }.as_();
                    let idx = key.wrapping_sub(self.offset) as usize;
                    debug_assert!(idx < self.slots.len());

                    let slot = unsafe { self.slots.get_unchecked_mut(idx) };
                    if *slot == EMPTY {
                        let g = self.values.len() as u32;
                        *slot = g;
                        self.values.push(unsafe { arr.value_unchecked(i) });
                        g as usize
                    } else {
                        *slot as usize
                    }
                };
                groups.push(group_id);
            }
        }
        Ok(())
    }

    fn size(&self) -> usize {
        self.slots.len() * size_of::<u32>()
            + self.values.capacity() * size_of::<T::Native>()
    }

    fn is_empty(&self) -> bool {
        self.values.is_empty()
    }

    fn len(&self) -> usize {
        self.values.len()
    }

    fn emit(&mut self, emit_to: EmitTo) -> Result<Vec<ArrayRef>> {
        let array: PrimitiveArray<T> = match emit_to {
            EmitTo::All => {
                self.slots.fill(EMPTY);
                let null_idx = self.null_group.take();
                let values = std::mem::take(&mut self.values);
                build_primitive::<T>(values, null_idx)
            }
            EmitTo::First(n) => {
                // Shift all slot references down by n, remove those < n
                for slot in self.slots.iter_mut() {
                    if *slot != EMPTY {
                        let g = *slot as usize;
                        if g < n {
                            *slot = EMPTY;
                        } else {
                            *slot = (g - n) as u32;
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
                build_primitive::<T>(split, null_group)
            }
        };

        Ok(vec![Arc::new(array.with_data_type(self.data_type.clone()))])
    }

    fn clear_shrink(&mut self, _num_rows: usize) {
        self.slots.fill(EMPTY);
        self.values.clear();
        self.null_group = None;
    }
}

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

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Int32Array, UInt64Array};
    use arrow::datatypes::{Int32Type, UInt64Type};

    #[test]
    fn test_basic_interning() {
        let mut gv = GroupValuesFlatPrimitive::<Int32Type>::new(DataType::Int32, 0, 9);
        let arr: ArrayRef = Arc::new(Int32Array::from(vec![3, 1, 3, 7, 1]));
        let mut groups = Vec::new();
        gv.intern(&[arr], &mut groups).unwrap();
        // First time seeing 3 -> group 0, first 1 -> group 1, 3 again -> 0, 7 -> group 2, 1 again -> 1
        assert_eq!(groups, vec![0, 1, 0, 2, 1]);
        assert_eq!(gv.len(), 3);
    }

    #[test]
    fn test_with_nulls() {
        let mut gv = GroupValuesFlatPrimitive::<Int32Type>::new(DataType::Int32, 0, 9);
        let arr: ArrayRef = Arc::new(Int32Array::from(vec![
            Some(2),
            None,
            Some(2),
            None,
            Some(5),
        ]));
        let mut groups = Vec::new();
        gv.intern(&[arr], &mut groups).unwrap();
        // 2 -> group 0, null -> group 1, 2 -> 0, null -> 1, 5 -> group 2
        assert_eq!(groups, vec![0, 1, 0, 1, 2]);
        assert_eq!(gv.len(), 3);
    }

    #[test]
    fn test_negative_range() {
        // Range [-5, 5] using wrapping arithmetic
        let min_val = (-5_i32) as u64;
        let max_val = 5_i32 as u64;
        let mut gv =
            GroupValuesFlatPrimitive::<Int32Type>::new(DataType::Int32, min_val, max_val);
        let arr: ArrayRef = Arc::new(Int32Array::from(vec![-5, 0, 5, -5, 0]));
        let mut groups = Vec::new();
        gv.intern(&[arr], &mut groups).unwrap();
        assert_eq!(groups, vec![0, 1, 2, 0, 1]);
        assert_eq!(gv.len(), 3);
    }

    #[test]
    fn test_emit_all() {
        let mut gv = GroupValuesFlatPrimitive::<UInt64Type>::new(DataType::UInt64, 0, 9);
        let arr: ArrayRef = Arc::new(UInt64Array::from(vec![3, 1, 7]));
        let mut groups = Vec::new();
        gv.intern(&[arr], &mut groups).unwrap();

        let emitted = gv.emit(EmitTo::All).unwrap();
        let result = emitted[0].as_primitive::<UInt64Type>();
        assert_eq!(result.values().as_ref(), &[3, 1, 7]);
        assert_eq!(gv.len(), 0);
    }

    #[test]
    fn test_emit_first() {
        let mut gv = GroupValuesFlatPrimitive::<Int32Type>::new(DataType::Int32, 0, 9);
        let arr: ArrayRef = Arc::new(Int32Array::from(vec![3, 1, 7, 5]));
        let mut groups = Vec::new();
        gv.intern(&[arr], &mut groups).unwrap();
        assert_eq!(groups, vec![0, 1, 2, 3]);

        let emitted = gv.emit(EmitTo::First(2)).unwrap();
        let result = emitted[0].as_primitive::<Int32Type>();
        assert_eq!(result.values().as_ref(), &[3, 1]);
        assert_eq!(gv.len(), 2);

        // Intern more - existing groups should still work
        let arr2: ArrayRef = Arc::new(Int32Array::from(vec![7, 5, 9]));
        let mut groups2 = Vec::new();
        gv.intern(&[arr2], &mut groups2).unwrap();
        // 7 is now group 0 (was 2, shifted by 2), 5 is group 1, 9 is new -> group 2
        assert_eq!(groups2, vec![0, 1, 2]);
    }
}
