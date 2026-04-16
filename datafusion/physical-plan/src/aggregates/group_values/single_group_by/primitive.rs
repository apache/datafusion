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
use datafusion_execution::memory_pool::proxy::VecAllocExt;
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
enum GroupValuesPrimitiveState<T: ArrowPrimitiveType> {
    GroupIds {
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
    },
    DistinctOnly {
        /// Stores the distinct primitive values.
        map: HashTable<T::Native>,
        has_null: bool,
    },
}

pub struct GroupValuesPrimitive<T: ArrowPrimitiveType> {
    /// The data type of the output array
    data_type: DataType,
    state: GroupValuesPrimitiveState<T>,
    /// The random state used to generate hashes
    random_state: RandomState,
}

impl<T: ArrowPrimitiveType> GroupValuesPrimitive<T>
where
    T::Native: HashValue,
{
    pub fn new(data_type: DataType, track_group_ids: bool) -> Self {
        assert!(PrimitiveArray::<T>::is_compatible(&data_type));
        let state = if track_group_ids {
            GroupValuesPrimitiveState::GroupIds {
                map: HashTable::with_capacity(128),
                values: Vec::with_capacity(128),
                null_group: None,
            }
        } else {
            GroupValuesPrimitiveState::DistinctOnly {
                map: HashTable::with_capacity(128),
                has_null: false,
            }
        };
        Self {
            data_type,
            state,
            random_state: crate::aggregates::AGGREGATION_HASH_SEED,
        }
    }

    fn build_primitive(
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

    fn ensure_group_id_tracking(&mut self) {
        if matches!(self.state, GroupValuesPrimitiveState::GroupIds { .. }) {
            return;
        }

        let GroupValuesPrimitiveState::DistinctOnly { map, has_null } = std::mem::replace(
            &mut self.state,
            GroupValuesPrimitiveState::GroupIds {
                map: HashTable::with_capacity(128),
                null_group: None,
                values: Vec::with_capacity(128),
            },
        ) else {
            unreachable!();
        };

        let mut values = Vec::with_capacity(map.len() + usize::from(has_null));
        let null_group = has_null.then(|| {
            values.push(Default::default());
            0
        });
        let mut group_map = HashTable::with_capacity(map.len());
        for value in map {
            let group_idx = values.len();
            values.push(value);
            let hash = value.hash(&self.random_state);
            group_map
                .insert_unique(hash, (group_idx, hash), |&(_, stored_hash)| stored_hash);
        }
        self.state = GroupValuesPrimitiveState::GroupIds {
            map: group_map,
            null_group,
            values,
        };
    }

    fn insert_group_id(
        random_state: &RandomState,
        map: &mut HashTable<(usize, u64)>,
        values: &mut Vec<T::Native>,
        null_group: &mut Option<usize>,
        value: Option<T::Native>,
    ) -> usize {
        match value {
            None => *null_group.get_or_insert_with(|| {
                let group_id = values.len();
                values.push(Default::default());
                group_id
            }),
            Some(key) => {
                let hash = key.hash(random_state);
                let insert = map.entry(
                    hash,
                    |&(g, h)| unsafe { hash == h && values.get_unchecked(g).is_eq(key) },
                    |&(_, h)| h,
                );

                match insert {
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
    }

    fn insert_distinct_only(
        random_state: &RandomState,
        map: &mut HashTable<T::Native>,
        has_null: &mut bool,
        value: Option<T::Native>,
    ) {
        match value {
            None => {
                *has_null = true;
            }
            Some(key) => {
                Self::insert_distinct_only_hashed(
                    random_state,
                    map,
                    key,
                    key.hash(random_state),
                );
            }
        }
    }

    fn insert_distinct_only_hashed(
        random_state: &RandomState,
        map: &mut HashTable<T::Native>,
        key: T::Native,
        hash: u64,
    ) {
        let insert = map.entry(
            hash,
            |stored| stored.is_eq(key),
            |stored| stored.hash(random_state),
        );

        if let hashbrown::hash_table::Entry::Vacant(v) = insert {
            v.insert(key);
        }
    }

    fn emit_group_ids(
        data_type: &DataType,
        map: &mut HashTable<(usize, u64)>,
        values: &mut Vec<T::Native>,
        null_group: &mut Option<usize>,
        emit_to: EmitTo,
    ) -> ArrayRef {
        let array: PrimitiveArray<T> = match emit_to {
            EmitTo::All => {
                map.clear();
                Self::build_primitive(std::mem::take(values), null_group.take())
            }
            EmitTo::First(n) => {
                map.retain(|entry| {
                    let group_idx = entry.0;
                    match group_idx.checked_sub(n) {
                        Some(sub) => {
                            entry.0 = sub;
                            true
                        }
                        None => false,
                    }
                });
                let null_idx = match null_group {
                    Some(v) if *v >= n => {
                        *v -= n;
                        None
                    }
                    Some(_) => null_group.take(),
                    None => None,
                };
                let mut split = values.split_off(n);
                std::mem::swap(values, &mut split);
                Self::build_primitive(split, null_idx)
            }
        };

        Arc::new(array.with_data_type(data_type.clone()))
    }

    fn emit_distinct_only(
        data_type: &DataType,
        random_state: &RandomState,
        map: &mut HashTable<T::Native>,
        has_null: &mut bool,
        emit_to: EmitTo,
    ) -> ArrayRef {
        let total_len = map.len() + usize::from(*has_null);
        let mut values = Vec::with_capacity(total_len);
        if *has_null {
            values.push(Default::default());
        }
        values.extend(map.iter().copied());
        map.clear();

        let (emitted_values, emitted_null_idx, remaining_values, remaining_has_null) =
            match emit_to {
                EmitTo::All => (values, (*has_null).then_some(0), Vec::new(), false),
                EmitTo::First(n) if n >= total_len => {
                    (values, (*has_null).then_some(0), Vec::new(), false)
                }
                EmitTo::First(n) => {
                    let mut remaining_values = values.split_off(n);
                    let emitted_values = values;
                    let emitted_null_idx = (*has_null && n > 0).then_some(0);
                    let remaining_has_null = *has_null && n == 0;
                    if remaining_has_null {
                        remaining_values.remove(0);
                    }
                    (
                        emitted_values,
                        emitted_null_idx,
                        remaining_values,
                        remaining_has_null,
                    )
                }
            };

        *has_null = remaining_has_null;
        for value in remaining_values {
            Self::insert_distinct_only(random_state, map, has_null, Some(value));
        }

        Arc::new(
            Self::build_primitive(emitted_values, emitted_null_idx)
                .with_data_type(data_type.clone()),
        )
    }
}

impl<T: ArrowPrimitiveType> GroupValues for GroupValuesPrimitive<T>
where
    T::Native: HashValue,
{
    fn intern(&mut self, cols: &[ArrayRef], groups: &mut Vec<usize>) -> Result<()> {
        self.ensure_group_id_tracking();
        assert_eq!(cols.len(), 1);
        groups.clear();
        let GroupValuesPrimitiveState::GroupIds {
            map,
            null_group,
            values,
        } = &mut self.state
        else {
            unreachable!();
        };

        for v in cols[0].as_primitive::<T>() {
            let group_id =
                Self::insert_group_id(&self.random_state, map, values, null_group, v);
            groups.push(group_id)
        }
        Ok(())
    }

    fn intern_no_group_ids(&mut self, cols: &[ArrayRef]) -> Result<()> {
        assert_eq!(cols.len(), 1);

        match &mut self.state {
            GroupValuesPrimitiveState::GroupIds {
                map,
                null_group,
                values,
            } => {
                for v in cols[0].as_primitive::<T>() {
                    let _ = Self::insert_group_id(
                        &self.random_state,
                        map,
                        values,
                        null_group,
                        v,
                    );
                }
            }
            GroupValuesPrimitiveState::DistinctOnly { map, has_null } => {
                for v in cols[0].as_primitive::<T>() {
                    Self::insert_distinct_only(&self.random_state, map, has_null, v);
                }
            }
        }
        Ok(())
    }

    fn size(&self) -> usize {
        size_of::<Self>()
            + match &self.state {
                GroupValuesPrimitiveState::GroupIds { map, values, .. } => {
                    map.capacity() * size_of::<(usize, u64)>() + values.allocated_size()
                }
                GroupValuesPrimitiveState::DistinctOnly { map, .. } => {
                    map.capacity() * size_of::<T::Native>()
                }
            }
    }

    fn is_empty(&self) -> bool {
        self.len() == 0
    }

    fn len(&self) -> usize {
        match &self.state {
            GroupValuesPrimitiveState::GroupIds { values, .. } => values.len(),
            GroupValuesPrimitiveState::DistinctOnly { map, has_null, .. } => {
                map.len() + usize::from(*has_null)
            }
        }
    }

    fn emit(&mut self, emit_to: EmitTo) -> Result<Vec<ArrayRef>> {
        let array = match &mut self.state {
            GroupValuesPrimitiveState::GroupIds {
                map,
                null_group,
                values,
            } => Self::emit_group_ids(&self.data_type, map, values, null_group, emit_to),
            GroupValuesPrimitiveState::DistinctOnly { map, has_null, .. } => {
                Self::emit_distinct_only(
                    &self.data_type,
                    &self.random_state,
                    map,
                    has_null,
                    emit_to,
                )
            }
        };

        Ok(vec![array])
    }

    fn clear_shrink(&mut self, num_rows: usize) {
        match &mut self.state {
            GroupValuesPrimitiveState::GroupIds {
                map,
                null_group,
                values,
            } => {
                *null_group = None;
                values.clear();
                values.shrink_to(num_rows);
                map.clear();
                map.shrink_to(num_rows, |_| 0);
            }
            GroupValuesPrimitiveState::DistinctOnly { map, has_null } => {
                *has_null = false;
                map.clear();
                map.shrink_to(num_rows, |_| 0);
            }
        }
    }
}
