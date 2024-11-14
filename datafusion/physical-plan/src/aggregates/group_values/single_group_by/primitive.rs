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
use arrow::array::BooleanBufferBuilder;
use arrow::buffer::NullBuffer;
use arrow::datatypes::i256;
use arrow::record_batch::RecordBatch;
use arrow_array::cast::AsArray;
use arrow_array::{ArrayRef, ArrowNativeTypeOp, ArrowPrimitiveType, PrimitiveArray};
use arrow_buffer::{IntervalDayTime, IntervalMonthDayNano};
use arrow_schema::DataType;
use datafusion_common::Result;
use datafusion_execution::memory_pool::proxy::VecAllocExt;
use datafusion_expr::EmitTo;
use half::f16;
use hashbrown::raw::RawTable;
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

/// A [`GroupValues`] storing a single column of primitive values
///
/// This specialization is significantly faster than using the more general
/// purpose `Row`s format
pub struct GroupValuesPrimitive<T: ArrowPrimitiveType> {
    /// The data type of the output array
    data_type: DataType,
    /// Stores the group index based on the hash of its value
    ///
    /// We don't store the hashes as hashing fixed width primitives
    /// is fast enough for this not to benefit performance
    map: RawTable<usize>,
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
            map: RawTable::with_capacity(128),
            values: Vec::with_capacity(128),
            null_group: None,
            random_state: Default::default(),
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
                    let insert = self.map.find_or_find_insert_slot(
                        hash,
                        |g| unsafe { self.values.get_unchecked(*g).is_eq(key) },
                        |g| unsafe { self.values.get_unchecked(*g).hash(state) },
                    );

                    // SAFETY: No mutation occurred since find_or_find_insert_slot
                    unsafe {
                        match insert {
                            Ok(v) => *v.as_ref(),
                            Err(slot) => {
                                let g = self.values.len();
                                self.map.insert_in_slot(hash, slot, g);
                                self.values.push(key);
                                g
                            }
                        }
                    }
                }
            };
            groups.push(group_id)
        }
        Ok(())
    }

    fn size(&self) -> usize {
        self.map.capacity() * size_of::<usize>() + self.values.allocated_size()
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
                let mut buffer = BooleanBufferBuilder::new(values.len());
                buffer.append_n(values.len(), true);
                buffer.set_bit(null_idx, false);
                unsafe { NullBuffer::new_unchecked(buffer.finish(), 1) }
            });
            PrimitiveArray::<T>::new(values.into(), nulls)
        }

        let array: PrimitiveArray<T> = match emit_to {
            EmitTo::All => {
                self.map.clear();
                build_primitive(std::mem::take(&mut self.values), self.null_group.take())
            }
            EmitTo::First(n) => {
                // SAFETY: self.map outlives iterator and is not modified concurrently
                unsafe {
                    for bucket in self.map.iter() {
                        // Decrement group index by n
                        match bucket.as_ref().checked_sub(n) {
                            // Group index was >= n, shift value down
                            Some(sub) => *bucket.as_mut() = sub,
                            // Group index was < n, so remove from table
                            None => self.map.erase(bucket),
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

    fn clear_shrink(&mut self, batch: &RecordBatch) {
        let count = batch.num_rows();
        self.values.clear();
        self.values.shrink_to(count);
        self.map.clear();
        self.map.shrink_to(count, |_| 0); // hasher does not matter since the map is cleared
    }
}
