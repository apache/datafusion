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
    cast::AsArray, ArrayRef, ArrowNativeTypeOp, ArrowPrimitiveType, NullBufferBuilder,
    PrimitiveArray,
};
use arrow::datatypes::{i256, DataType};
use arrow::record_batch::RecordBatch;
use datafusion_common::Result;
use datafusion_execution::memory_pool::proxy::VecAllocExt;
use datafusion_expr::EmitTo;
use half::f16;
use hashbrown::hash_table::HashTable;
use std::mem::size_of;
use std::sync::Arc;

const BLOCK_SIZE: usize = 4096;
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
    map: HashTable<usize>,
    /// The group index of the null value if any
    null_group: Option<usize>,
    /// The values for each group index, stored in blocks of 1024
    values: Vec<Vec<T::Native>>,
    /// The random state used to generate hashes
    random_state: RandomState,
}

impl<T: ArrowPrimitiveType> GroupValuesPrimitive<T> {
    pub fn new(data_type: DataType) -> Self {
        assert!(PrimitiveArray::<T>::is_compatible(&data_type));
        Self {
            data_type,
            map: HashTable::with_capacity(128),
            values: vec![],
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
                    let group_id = self.values.len().saturating_sub(1) * BLOCK_SIZE
                        + self.values.last().map_or(0, |v| v.len());
                    let block = group_id / BLOCK_SIZE;
                    self.values
                        .resize_with(block + 1, || Vec::with_capacity(BLOCK_SIZE));
                    self.values[block].push(Default::default());
                    group_id
                }),
                Some(key) => {
                    let state = &self.random_state;
                    let hash = key.hash(state);

                    let insert = self.map.entry(
                        hash,
                        |g| unsafe {
                            let block = g / BLOCK_SIZE;
                            let index = g % BLOCK_SIZE;

                            self.values
                                .get_unchecked(block)
                                .get_unchecked(index)
                                .is_eq(key)
                        },
                        |g| unsafe {
                            let block = g / BLOCK_SIZE;
                            let index = g % BLOCK_SIZE;

                            self.values
                                .get_unchecked(block)
                                .get_unchecked(index)
                                .hash(state)
                        },
                    );

                    match insert {
                        hashbrown::hash_table::Entry::Occupied(o) => *o.get(),
                        hashbrown::hash_table::Entry::Vacant(v) => {
                            let g = self.values.len().saturating_sub(1) * BLOCK_SIZE
                                + self.values.last().map_or(0, |v| v.len());
                            let block = g / BLOCK_SIZE;
                            self.values
                                .resize_with(block + 1, || Vec::with_capacity(1024));

                            v.insert(g);
                            self.values[block].push(key);
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
        self.map.capacity() * size_of::<usize>() + self.values.allocated_size()
    }

    fn is_empty(&self) -> bool {
        self.values.is_empty()
    }

    fn len(&self) -> usize {
        self.values.len().saturating_sub(1) * BLOCK_SIZE
            + self.values.last().map_or(0, |v| v.len())
    }

    fn emit(&mut self, emit_to: EmitTo) -> Result<Vec<ArrayRef>> {
        fn build_primitive<T: ArrowPrimitiveType>(
            values: Vec<Vec<T::Native>>,
            null_idx: Option<usize>,
        ) -> PrimitiveArray<T> {
            let nulls = null_idx.map(|null_idx| {
                let mut buffer = NullBufferBuilder::new(values.len());
                buffer.append_n_non_nulls(null_idx);
                buffer.append_null();
                buffer.append_n_non_nulls(
                    values.len().saturating_sub(1) * BLOCK_SIZE
                        + values.last().map_or(0, |v| v.len())
                        - null_idx
                        - 1,
                );
                // NOTE: The inner builder must be constructed as there is at least one null
                buffer.finish().unwrap()
            });
            // TODO: optimize
            PrimitiveArray::<T>::from_iter_values_with_nulls(
                values.into_iter().flatten(),
                nulls,
            )
        }

        let array: PrimitiveArray<T> = match emit_to {
            EmitTo::All => {
                self.map.clear();
                build_primitive(std::mem::take(&mut self.values), self.null_group.take())
            }
            EmitTo::First(n) => {
                self.map.retain(|group_idx| {
                    // Decrement group index by n
                    match group_idx.checked_sub(n) {
                        // Group index was >= n, shift value down
                        Some(sub) => {
                            *group_idx = sub;
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
                let num_blocks = n / BLOCK_SIZE;

                let num_values = n % BLOCK_SIZE;
                // get num_blocks - 1 from t (unchanged)
                let mut split = vec![];
                if num_blocks > 0 {
                    split = self.values.split_off(num_blocks - 1);
                }

                let mut t_v = self.values[0].split_off(num_values);
                std::mem::swap(&mut self.values[0], &mut t_v);

                split.push(t_v);

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
