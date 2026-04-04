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
    Array, ArrayRef, ArrowNativeTypeOp, ArrowPrimitiveType, NullBufferBuilder, PrimitiveArray,
    cast::AsArray,
};
use arrow::datatypes::{DataType, i256};
use datafusion_common::Result;
use datafusion_common::hash_utils::{RandomState, create_hashes};
use datafusion_expr::EmitTo;
use half::f16;
use hashbrown::hash_table::HashTable;
#[cfg(not(feature = "force_hash_collisions"))]
use std::hash::BuildHasher;
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
/// purpose `Row`s format.
///
/// Values are stored inline in the hash table entries `(group_index, value)`
/// and hashes are not stored (recomputed on rehash) to minimize memory per entry.
/// Hashes are computed vectorized per batch using `create_hashes`.
pub struct GroupValuesPrimitive<T: ArrowPrimitiveType> {
    /// The data type of the output array
    data_type: DataType,
    /// Hash table storing (group_index, value) inline.
    map: HashTable<(usize, T::Native)>,
    /// The group index of the null value if any
    null_group: Option<usize>,
    /// Total number of distinct groups
    num_groups: usize,
    /// The random state used to generate hashes
    random_state: RandomState,
    /// Reusable buffer for vectorized hash computation
    hashes_buffer: Vec<u64>,
}

impl<T: ArrowPrimitiveType> GroupValuesPrimitive<T> {
    pub fn new(data_type: DataType) -> Self {
        assert!(PrimitiveArray::<T>::is_compatible(&data_type));
        Self {
            data_type,
            map: HashTable::with_capacity(128),
            null_group: None,
            num_groups: 0,
            random_state: crate::aggregates::AGGREGATION_HASH_SEED,
            hashes_buffer: Vec::new(),
        }
    }
}

impl<T: ArrowPrimitiveType> GroupValuesPrimitive<T>
where
    T::Native: HashValue,
{
    #[inline(always)]
    fn find_or_insert(&mut self, key: T::Native, hash: u64) -> usize {
        let state = &self.random_state;
        let entry = self.map.entry(
            hash,
            |&(_, v)| v.is_eq(key),
            |&(_, v)| v.hash(state),
        );

        match entry {
            hashbrown::hash_table::Entry::Occupied(o) => o.get().0,
            hashbrown::hash_table::Entry::Vacant(v) => {
                let g = self.num_groups;
                self.num_groups += 1;
                v.insert((g, key));
                g
            }
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

        let array = cols[0].as_primitive::<T>();
        let len = array.len();

        // Vectorized hash computation
        let mut hashes_buffer = std::mem::take(&mut self.hashes_buffer);
        hashes_buffer.clear();
        hashes_buffer.resize(len, 0);
        create_hashes(cols, &self.random_state, &mut hashes_buffer)?;

        groups.reserve(len);

        if array.null_count() == 0 {
            let values = array.values();
            for i in 0..len {
                let key = values[i];
                let hash = hashes_buffer[i];
                groups.push(self.find_or_insert(key, hash));
            }
        } else {
            for i in 0..len {
                let group_id = if array.is_null(i) {
                    *self.null_group.get_or_insert_with(|| {
                        let g = self.num_groups;
                        self.num_groups += 1;
                        g
                    })
                } else {
                    let key = unsafe { array.value_unchecked(i) };
                    let hash = hashes_buffer[i];
                    self.find_or_insert(key, hash)
                };
                groups.push(group_id);
            }
        }

        self.hashes_buffer = hashes_buffer;
        Ok(())
    }

    fn size(&self) -> usize {
        self.map.capacity() * size_of::<(usize, T::Native)>()
            + self.hashes_buffer.capacity() * size_of::<u64>()
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
                let mut values = vec![T::Native::default(); self.num_groups];
                for &(g, v) in self.map.iter() {
                    values[g] = v;
                }
                self.map.clear();
                let null_group = self.null_group.take();
                self.num_groups = 0;
                build_primitive(values, null_group)
            }
            EmitTo::First(n) => {
                let mut values = vec![T::Native::default(); n];

                self.map.retain(|entry| {
                    if entry.0 < n {
                        values[entry.0] = entry.1;
                        false
                    } else {
                        entry.0 -= n;
                        true
                    }
                });

                self.num_groups -= n;

                let null_group = match &mut self.null_group {
                    Some(v) if *v >= n => {
                        *v -= n;
                        None
                    }
                    Some(_) => self.null_group.take(),
                    None => None,
                };

                build_primitive(values, null_group)
            }
        };

        Ok(vec![Arc::new(array.with_data_type(self.data_type.clone()))])
    }

    fn clear_shrink(&mut self, num_rows: usize) {
        self.map.clear();
        self.map.shrink_to(num_rows, |_| 0);
        self.null_group = None;
        self.num_groups = 0;
    }
}
