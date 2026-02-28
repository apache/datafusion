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

/// A [`GroupValues`] storing a single column of primitive values
///
/// This specialization is significantly faster than using the more general
/// purpose `Row`s format
pub struct GroupValuesPrimitive<T: ArrowPrimitiveType> {
    /// The data type of the output array
    data_type: DataType,
    /// Stores (value, group_index) pairs directly.
    /// Values are compared directly during probing without indirection.
    map: HashTable<(T::Native, usize)>,
    /// The group index of the null value if any
    null_group: Option<usize>,
    /// The random state used to generate hashes
    random_state: RandomState,
}

impl<T: ArrowPrimitiveType> GroupValuesPrimitive<T> {
    pub fn new(data_type: DataType) -> Self {
        assert!(PrimitiveArray::<T>::is_compatible(&data_type));
        Self {
            data_type,
            map: HashTable::with_capacity(128),
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
                    let group_id = self.len();
                    group_id
                }),
                Some(key) => {
                    let state = &self.random_state;
                    let hash = key.hash(state);
                    let insert = self.map.entry(
                        hash,
                        |entry| entry.0.is_eq(key),
                        |entry| entry.0.hash(state),
                    );

                    match insert {
                        hashbrown::hash_table::Entry::Occupied(o) => o.get().1,
                        hashbrown::hash_table::Entry::Vacant(v) => {
                            let g = self.len();
                            v.insert((key, g));
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
        self.map.capacity() * size_of::<(T::Native, usize)>()
    }

    fn is_empty(&self) -> bool {
        self.map.is_empty() && self.null_group.is_none()
    }

    fn len(&self) -> usize {
        self.map.len() + self.null_group.is_some() as usize
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
                let mut values = vec![T::Native::default(); self.len()];
                for &(value, group_idx) in self.map.iter() {
                    values[group_idx] = value;
                }
                self.map.clear();
                let null_group = self.null_group.take();
                build_primitive(values, null_group)
            }
            EmitTo::First(n) => {
                let mut values = vec![T::Native::default(); n];
                self.map.retain(|entry| {
                    if entry.1 < n {
                        values[entry.1] = entry.0;
                        false
                    } else {
                        entry.1 -= n;
                        true
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
                build_primitive(values, null_group)
            }
        };

        Ok(vec![Arc::new(array.with_data_type(self.data_type.clone()))])
    }

    fn clear_shrink(&mut self, count: usize) {
        self.null_group = None;
        self.map.clear();
        self.map.shrink_to(count, |_| 0); // hasher does not matter since the map is cleared
    }
}
