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
    ArrayRef, ArrowNativeTypeOp, ArrowPrimitiveType, NullBufferBuilder, PrimitiveArray,
    cast::AsArray,
};
use arrow::datatypes::{DataType, i256};
use datafusion_common::Result;
use datafusion_common::hash_utils::RandomState;
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
/// purpose `Row`s format
pub struct GroupValuesPrimitive<T: ArrowPrimitiveType> {
    /// The data type of the output array
    data_type: DataType,
    /// Stores the group index and value, keyed by the hash of the value
    map: HashTable<(usize, T::Native)>,
    /// The group index of the null value if any
    null_group: Option<usize>,
    /// The number of groups, including the null group if present
    num_groups: usize,
    /// The random state used to generate hashes
    random_state: RandomState,
}

impl<T: ArrowPrimitiveType> GroupValuesPrimitive<T> {
    pub fn new(data_type: DataType) -> Self {
        assert!(PrimitiveArray::<T>::is_compatible(&data_type));
        Self {
            data_type,
            map: HashTable::with_capacity(128),
            num_groups: 0,
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
                    let group_id = self.num_groups;
                    self.num_groups += 1;
                    group_id
                }),
                Some(key) => {
                    let state = &self.random_state;
                    let hash = key.hash(state);
                    let insert = self.map.entry(
                        hash,
                        |&(_, value)| value.is_eq(key),
                        |&(_, value)| value.hash(state),
                    );

                    match insert {
                        hashbrown::hash_table::Entry::Occupied(o) => o.get().0,
                        hashbrown::hash_table::Entry::Vacant(v) => {
                            let g = self.num_groups;
                            v.insert((g, key));
                            self.num_groups += 1;
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
        self.map.capacity() * size_of::<(usize, T::Native)>()
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
                let mut values = vec![T::default_value(); self.num_groups];
                for (group_idx, value) in std::mem::take(&mut self.map) {
                    values[group_idx] = value;
                }
                self.num_groups = 0;

                build_primitive(values, self.null_group.take())
            }
            EmitTo::First(n) => {
                let mut values = vec![T::default_value(); n];

                self.map.retain(|(group_idx, value)| {
                    // Decrement group index by n
                    match group_idx.checked_sub(n) {
                        // Group index was >= n, shift value down
                        Some(sub) => {
                            *group_idx = sub;
                            true
                        }
                        // Group index was < n, so remove from table
                        None => {
                            values[*group_idx] = *value;
                            false
                        }
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
                self.num_groups -= n;
                build_primitive(values, null_group)
            }
        };

        Ok(vec![Arc::new(array.with_data_type(self.data_type.clone()))])
    }

    fn clear_shrink(&mut self, num_rows: usize) {
        self.num_groups = 0;
        self.null_group = None;
        self.map.clear();
        self.map.shrink_to(num_rows, |_| 0); // hasher does not matter since the map is cleared
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Array, UInt64Array};
    use arrow::datatypes::UInt64Type;

    fn values(array: &ArrayRef) -> Vec<Option<u64>> {
        let array = array.as_primitive::<UInt64Type>();
        (0..array.len())
            .map(|idx| {
                if array.is_null(idx) {
                    None
                } else {
                    Some(array.value(idx))
                }
            })
            .collect()
    }

    #[test]
    fn primitive_emit_all_reconstructs_group_order() -> Result<()> {
        let input = Arc::new(UInt64Array::from(vec![Some(10), Some(20), None, Some(10)]))
            as ArrayRef;
        let mut group_values = GroupValuesPrimitive::<UInt64Type>::new(DataType::UInt64);
        let mut groups = vec![];

        group_values.intern(&[input], &mut groups)?;
        assert_eq!(groups, vec![0, 1, 2, 0]);

        let output = group_values.emit(EmitTo::All)?;
        assert_eq!(values(&output[0]), vec![Some(10), Some(20), None]);
        assert!(group_values.is_empty());

        Ok(())
    }

    #[test]
    fn primitive_emit_first_reindexes_remaining_groups() -> Result<()> {
        let input = Arc::new(UInt64Array::from(vec![
            Some(10),
            None,
            Some(20),
            Some(30),
            Some(10),
        ])) as ArrayRef;
        let mut group_values = GroupValuesPrimitive::<UInt64Type>::new(DataType::UInt64);
        let mut groups = vec![];

        group_values.intern(&[input], &mut groups)?;
        assert_eq!(groups, vec![0, 1, 2, 3, 0]);

        let output = group_values.emit(EmitTo::First(2))?;
        assert_eq!(values(&output[0]), vec![Some(10), None]);
        assert_eq!(group_values.len(), 2);

        let input = Arc::new(UInt64Array::from(vec![Some(20), Some(40)])) as ArrayRef;
        group_values.intern(&[input], &mut groups)?;
        assert_eq!(groups, vec![0, 2]);

        let output = group_values.emit(EmitTo::All)?;
        assert_eq!(values(&output[0]), vec![Some(20), Some(30), Some(40)]);

        Ok(())
    }
}
