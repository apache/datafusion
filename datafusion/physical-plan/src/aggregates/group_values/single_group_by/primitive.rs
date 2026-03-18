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
    Array, ArrayRef, ArrowNativeTypeOp, ArrowPrimitiveType, NullBufferBuilder,
    PrimitiveArray, cast::AsArray,
};
use arrow::buffer::NullBuffer;
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

impl<T: ArrowPrimitiveType> GroupValuesPrimitive<T>
where
    T::Native: HashValue,
{
    /// Find the group index for `key` if it already exists in the map.
    #[inline(always)]
    fn find_group(
        map: &HashTable<(usize, u64)>,
        values: &[T::Native],
        key: T::Native,
        hash: u64,
    ) -> Option<usize> {
        // SAFETY: `g` is always a valid index into `values` because it was set
        // to `values.len()` at insertion time and values are never removed
        // (only via emit, which also clears or adjusts the map).
        map.find(hash, |&(g, h)| unsafe {
            hash == h && values.get_unchecked(g).is_eq(key)
        })
        .map(|&(g, _)| g)
    }

    /// Insert a new group for `key` that is known not to exist yet.
    #[inline(always)]
    fn insert_new_group(
        map: &mut HashTable<(usize, u64)>,
        values: &mut Vec<T::Native>,
        key: T::Native,
        hash: u64,
    ) -> usize {
        let g = values.len();
        values.push(key);
        map.insert_unique(hash, (g, hash), |&(_, h)| h);
        g
    }

    /// Find an existing group or insert a new one.
    #[inline(always)]
    fn lookup_or_insert(
        map: &mut HashTable<(usize, u64)>,
        values: &mut Vec<T::Native>,
        key: T::Native,
        hash: u64,
    ) -> usize {
        if let Some(g) = Self::find_group(map, values, key, hash) {
            g
        } else {
            Self::insert_new_group(map, values, key, hash)
        }
    }

    /// Get or create the null group index.
    #[inline(always)]
    fn get_or_create_null_group(
        null_group: &mut Option<usize>,
        values: &mut Vec<T::Native>,
    ) -> usize {
        *null_group.get_or_insert_with(|| {
            let g = values.len();
            values.push(Default::default());
            g
        })
    }
}

impl<T: ArrowPrimitiveType> GroupValues for GroupValuesPrimitive<T>
where
    T::Native: HashValue,
{
    fn intern(&mut self, cols: &[ArrayRef], groups: &mut Vec<usize>) -> Result<()> {
        assert_eq!(cols.len(), 1);
        let array = cols[0].as_primitive::<T>();
        let len = array.len();
        groups.clear();
        groups.reserve(len);

        if len == 0 {
            return Ok(());
        }

        let values = array.values().as_ref();
        let nulls: Option<&NullBuffer> = array.nulls();

        // Step 1: Batch compute all hashes using thread-local buffer
        // (avoids per-call allocation and separates hashing from hash table ops
        // for better vectorization / instruction pipelining)
        datafusion_common::hash_utils::with_hashes(cols, &self.random_state, |hashes| {
            // Step 2: Process in chunks of 4 for better ILP and local dedup
            let value_chunks = values.chunks_exact(4);
            let hash_chunks = hashes.chunks_exact(4);
            let rem_len = value_chunks.remainder().len();

            for (chunk_idx, (vs, hs)) in value_chunks.zip(hash_chunks).enumerate() {
                let base = chunk_idx * 4;
                let mut gids = [0usize; 4];

                // Local dedup within the chunk: dedup[i] = index of first
                // equivalent entry in this chunk (i itself if unique).
                // Compare only values (not hashes) since for 4 elements
                // the value comparison is cheap enough.
                let mut dedup = [0u8, 1, 2, 3];

                if let Some(nulls) = nulls {
                    let valid = [
                        nulls.is_valid(base),
                        nulls.is_valid(base + 1),
                        nulls.is_valid(base + 2),
                        nulls.is_valid(base + 3),
                    ];

                    for i in 1..4 {
                        for j in 0..i {
                            if (!valid[i] && !valid[j])
                                || (valid[i] && valid[j] && vs[i].is_eq(vs[j]))
                            {
                                dedup[i] = dedup[j];
                                break;
                            }
                        }
                    }

                    // Phase 1: Batch find - lookup all unique entries
                    let mut found = [None; 4];
                    for i in 0..4 {
                        if dedup[i] as usize != i {
                            continue;
                        }
                        if !valid[i] {
                            found[i] = self.null_group;
                        } else {
                            found[i] =
                                Self::find_group(&self.map, &self.values, vs[i], hs[i]);
                        }
                    }

                    // Phase 2: Insert entries not found
                    for i in 0..4 {
                        if dedup[i] as usize != i {
                            gids[i] = gids[dedup[i] as usize];
                            continue;
                        }
                        if let Some(g) = found[i] {
                            gids[i] = g;
                        } else if !valid[i] {
                            gids[i] = Self::get_or_create_null_group(
                                &mut self.null_group,
                                &mut self.values,
                            );
                        } else {
                            gids[i] = Self::insert_new_group(
                                &mut self.map,
                                &mut self.values,
                                vs[i],
                                hs[i],
                            );
                        }
                    }
                } else {
                    // Fast path: no nulls
                    for i in 1..4 {
                        for j in 0..i {
                            if vs[i].is_eq(vs[j]) {
                                dedup[i] = dedup[j];
                                break;
                            }
                        }
                    }

                    // Phase 1: Batch find - lookup all unique entries
                    let mut found = [None; 4];
                    for i in 0..4 {
                        if dedup[i] as usize != i {
                            continue;
                        }
                        found[i] =
                            Self::find_group(&self.map, &self.values, vs[i], hs[i]);
                    }

                    // Phase 2: Insert entries not found
                    for i in 0..4 {
                        if dedup[i] as usize != i {
                            gids[i] = gids[dedup[i] as usize];
                        } else if let Some(g) = found[i] {
                            gids[i] = g;
                        } else {
                            gids[i] = Self::insert_new_group(
                                &mut self.map,
                                &mut self.values,
                                vs[i],
                                hs[i],
                            );
                        }
                    }
                }

                groups.extend_from_slice(&gids);
            }

            // Handle remainder (0-3 elements)
            let rem_start = len - rem_len;
            for i in 0..rem_len {
                let idx = rem_start + i;
                let is_valid = nulls.is_none_or(|n: &NullBuffer| n.is_valid(idx));

                let group_id = if !is_valid {
                    Self::get_or_create_null_group(&mut self.null_group, &mut self.values)
                } else {
                    Self::lookup_or_insert(
                        &mut self.map,
                        &mut self.values,
                        values[idx],
                        hashes[idx],
                    )
                };
                groups.push(group_id);
            }

            Ok(())
        })
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
                self.map.clear();
                build_primitive(std::mem::take(&mut self.values), self.null_group.take())
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
                build_primitive(split, null_group)
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
