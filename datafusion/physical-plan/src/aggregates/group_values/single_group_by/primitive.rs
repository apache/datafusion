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
use ahash::RandomState;
use arrow::array::types::{IntervalDayTime, IntervalMonthDayNano};
use arrow::array::{
    ArrayRef, ArrowNativeTypeOp, ArrowPrimitiveType, NullBufferBuilder, PrimitiveArray,
    cast::AsArray,
};
use arrow::datatypes::{DataType, i256};
use datafusion_common::Result;
use datafusion_execution::memory_pool::proxy::VecAllocExt;
use datafusion_expr::EmitTo;
use half::f16;
use hashbrown::hash_table::HashTable;
use std::mem::size_of;
use std::sync::Arc;

/// A trait to allow hashing of floating point numbers
pub(crate) trait HashValue {
    fn hash(&self, state: &RandomState) -> u64;
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

pub(crate) trait SmallValue {
    fn to_index(&self) -> usize;
    const MAX_INDEX: usize;
}

impl SmallValue for u8 {
    fn to_index(&self) -> usize {
        *self as usize
    }
    const MAX_INDEX: usize = 255;
}

impl SmallValue for i8 {
    fn to_index(&self) -> usize {
        (*self as i16 + 128) as usize
    }
    const MAX_INDEX: usize = 255;
}

impl SmallValue for u16 {
    fn to_index(&self) -> usize {
        *self as usize
    }
    const MAX_INDEX: usize = 65535;
}

impl SmallValue for i16 {
    fn to_index(&self) -> usize {
        (*self as i32 + 32768) as usize
    }
    const MAX_INDEX: usize = 65535;
}

impl SmallValue for f16 {
    fn to_index(&self) -> usize {
        self.to_bits() as usize
    }
    const MAX_INDEX: usize = 65535;
}

/// A [`GroupValues`] storing a single column of primitive values
///
/// This specialization is significantly faster than using the more general
/// purpose `Row`s format
pub struct GroupValuesPrimitive<T: ArrowPrimitiveType> {
    /// The data type of the output array
    data_type: DataType,
    /// Stores the `(group_index, hash)` based on the hash of its value
    ///
    /// We also store `hash` is for reducing cost of rehashing. Such cost
    /// is obvious in high cardinality group by situation.
    /// More details can see:
    /// <https://github.com/apache/datafusion/issues/15961>
    map: HashTable<(usize, u64)>,
    /// The group index of the null value if any
    null_group: Option<usize>,
    /// The values for each group index
    values: Vec<T::Native>,
    /// The random state used to generate hashes
    random_state: RandomState,
}

impl<T: ArrowPrimitiveType> GroupValuesPrimitive<T> {
    pub fn new(data_type: DataType) -> Self {
        assert!(PrimitiveArray::<T>::is_compatible(&data_type));
        Self {
            data_type,
            map: HashTable::with_capacity(128),
            values: Vec::with_capacity(128),
            null_group: None,
            random_state: crate::aggregates::AGGREGATION_HASH_SEED,
        }
    }
}

impl<T: ArrowPrimitiveType> GroupValues for GroupValuesPrimitive<T>
where
    T::Native: HashValue,
{
    fn intern(&mut self, cols: &[ArrayRef], groups: &mut Vec<usize>) -> Result<()> {
        assert_eq!(cols.len(), 1);
        groups.clear();

        for v in cols[0].as_primitive::<T>() {
            let group_id = match v {
                None => *self.null_group.get_or_insert_with(|| {
                    let group_id = self.values.len();
                    self.values.push(Default::default());
                    group_id
                }),
                Some(key) => {
                    let state = &self.random_state;
                    let hash = key.hash(state);
                    let insert = self.map.entry(
                        hash,
                        |&(g, _)| unsafe { self.values.get_unchecked(g).is_eq(key) },
                        |&(_, h)| h,
                    );

                    match insert {
                        hashbrown::hash_table::Entry::Occupied(o) => o.get().0,
                        hashbrown::hash_table::Entry::Vacant(v) => {
                            let g = self.values.len();
                            v.insert((g, hash));
                            self.values.push(key);
                            g
                        }
                    }
                }
            };
            groups.push(group_id)
        }
        Ok(())
    }

    fn size(&self) -> usize {
        self.map.capacity() * size_of::<(usize, u64)>() + self.values.allocated_size()
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
                self.map.clear();
                build_primitive::<T>(std::mem::take(&mut self.values), self.null_group.take())
            }
            EmitTo::First(n) => {
                self.map.retain(|entry| {
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

    fn clear_shrink(&mut self, num_rows: usize) {
        self.values.clear();
        self.values.shrink_to(num_rows);
        self.map.clear();
        self.map.shrink_to(num_rows, |_| 0); // hasher does not matter since the map is cleared
    }
}

/// A [`GroupValues`] storing a single column of small primitive values (<16 bits recommended)
///
/// This specialization uses a flat `Vec` as a lookup table instead of a `HashTable`
pub struct GroupValuesSmallPrimitive<T: ArrowPrimitiveType> {
    /// The data type of the output array
    data_type: DataType,
    /// Stores `group_index + 1` for each possible value of the primitive type.
    /// 0 means the value has not been seen yet.
    map: Vec<usize>,
    /// The group index of the null value if any
    null_group: Option<usize>,
    /// The values for each group index
    values: Vec<T::Native>,
}

impl<T: ArrowPrimitiveType> GroupValuesSmallPrimitive<T>
where
    T::Native: SmallValue,
{
    pub fn new(data_type: DataType) -> Self {
        assert!(PrimitiveArray::<T>::is_compatible(&data_type));
        Self {
            data_type,
            map: vec![0; T::Native::MAX_INDEX + 1],
            values: Vec::with_capacity(128),
            null_group: None,
        }
    }
}

impl<T: ArrowPrimitiveType> GroupValues for GroupValuesSmallPrimitive<T>
where
    T::Native: SmallValue,
{
    fn intern(&mut self, cols: &[ArrayRef], groups: &mut Vec<usize>) -> Result<()> {
        assert_eq!(cols.len(), 1);
        groups.clear();

        for v in cols[0].as_primitive::<T>() {
            let group_id = match v {
                None => *self.null_group.get_or_insert_with(|| {
                    let group_id = self.values.len();
                    self.values.push(Default::default());
                    group_id
                }),
                Some(key) => {
                    let index = key.to_index();
                    let entry = unsafe { self.map.get_unchecked_mut(index) };
                    if *entry == 0 {
                        let g = self.values.len();
                        self.values.push(key);
                        *entry = g + 1;
                        g
                    } else {
                        *entry - 1
                    }
                }
            };
            groups.push(group_id)
        }
        Ok(())
    }

    fn size(&self) -> usize {
        self.map.allocated_size() + self.values.allocated_size()
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
                self.map.fill(0);
                build_primitive::<T>(std::mem::take(&mut self.values), self.null_group.take())
            }
            EmitTo::First(n) => {
                for entry in self.map.iter_mut() {
                    if *entry == 0 {
                        continue;
                    }
                    let group_idx = *entry - 1;
                    match group_idx.checked_sub(n) {
                        Some(sub) => {
                            *entry = sub + 1;
                        }
                        None => {
                            *entry = 0;
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

    fn clear_shrink(&mut self, num_rows: usize) {
        self.values.clear();
        self.values.shrink_to(num_rows);
        self.map.fill(0);
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
        // NOTE: The inner builder must be constructed as there is at least one null
        buffer.finish().unwrap()
    });
    PrimitiveArray::<T>::new(values.into(), nulls)
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::types::Int8Type;
    use arrow::array::{Array, Int8Array};

    #[test]
    fn test_intern_int8() {
        let mut group_values = GroupValuesSmallPrimitive::<Int8Type>::new(DataType::Int8);
        let array = Arc::new(Int8Array::from(vec![Some(1), Some(2), Some(1), None, Some(3)])) as ArrayRef;
        let mut groups = vec![];
        group_values.intern(&[array], &mut groups).unwrap();

        assert_eq!(groups, vec![0, 1, 0, 2, 3]);
        assert_eq!(group_values.len(), 4);

        let emitted = group_values.emit(EmitTo::All).unwrap();
        let emitted_array = emitted[0].as_primitive::<Int8Type>();
        
        // Group 0: 1, Group 1: 2, Group 2: None, Group 3: 3
        assert_eq!(emitted_array.len(), 4);
        assert_eq!(emitted_array.value(0), 1);
        assert_eq!(emitted_array.value(1), 2);
        assert!(emitted_array.is_null(2));
        assert_eq!(emitted_array.value(3), 3);
    }
    
    #[test]
    fn test_emit_first_int8() {
        let mut group_values = GroupValuesSmallPrimitive::<Int8Type>::new(DataType::Int8);
        let array = Arc::new(Int8Array::from(vec![Some(10), Some(20), Some(10), None])) as ArrayRef;
        let mut groups = vec![];
        group_values.intern(&[array], &mut groups).unwrap();
        assert_eq!(groups, vec![0, 1, 0, 2]);

        // Emit first 2 groups (10 and 20)
        let emitted = group_values.emit(EmitTo::First(2)).unwrap();
        let emitted_array = emitted[0].as_primitive::<Int8Type>();
        assert_eq!(emitted_array.len(), 2);
        assert_eq!(emitted_array.value(0), 10);
        assert_eq!(emitted_array.value(1), 20);

        // Remaining should be just the null group at index 0
        assert_eq!(group_values.len(), 1);
        let array2 = Arc::new(Int8Array::from(vec![Some(10), None, Some(30)])) as ArrayRef;
        group_values.intern(&[array2], &mut groups).unwrap();
        
        // 10 is new (index 1), None is old (index 0), 30 is new (index 2)
        assert_eq!(groups, vec![1, 0, 2]);
        assert_eq!(group_values.len(), 3);
    }
}
