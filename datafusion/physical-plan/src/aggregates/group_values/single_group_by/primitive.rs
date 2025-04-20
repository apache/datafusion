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
use datafusion_functions_aggregate_common::aggregate::groups_accumulator::{
    BlockedGroupIndexOperations, FlatGroupIndexOperations, GroupIndexOperations,
};
use half::f16;
use hashbrown::hash_table::HashTable;
use std::collections::VecDeque;
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
    map: HashTable<u64>,

    /// The group index of the null value if any
    null_group: Option<u64>,

    /// The values for each group index
    values: VecDeque<Vec<T::Native>>,

    /// The random state used to generate hashes
    random_state: RandomState,

    /// Block size of current `GroupValues` if exist:
    ///   - If `None`, it means block optimization is disabled,
    ///     all `group values`` will be stored in a single `Vec`
    ///
    ///   - If `Some(blk_size)`, it means block optimization is enabled,
    ///     `group values` will be stored in multiple `Vec`s, and each
    ///     `Vec` if of `blk_size` len, and we call it a `block`
    ///
    block_size: Option<usize>,
}

impl<T: ArrowPrimitiveType> GroupValuesPrimitive<T> {
    pub fn new(data_type: DataType) -> Self {
        assert!(PrimitiveArray::<T>::is_compatible(&data_type));
        let mut values = VecDeque::new();
        values.push_back(Vec::new());

        Self {
            data_type,
            map: HashTable::with_capacity(128),
            values,
            null_group: None,
            random_state: Default::default(),
            block_size: None,
        }
    }
}

impl<T: ArrowPrimitiveType> GroupValues for GroupValuesPrimitive<T>
where
    T::Native: HashValue,
{
    fn intern(&mut self, cols: &[ArrayRef], groups: &mut Vec<usize>) -> Result<()> {
        if let Some(block_size) = self.block_size {
            let before_add_group = |group_values: &mut VecDeque<Vec<T::Native>>| {
                if group_values.is_empty()
                    || group_values.back().unwrap().len() == block_size
                {
                    let new_block = Vec::with_capacity(block_size);
                    group_values.push_back(new_block);
                }
            };
            self.get_or_create_groups::<_, BlockedGroupIndexOperations>(
                cols,
                groups,
                before_add_group,
            )
        } else {
            self.get_or_create_groups::<_, FlatGroupIndexOperations>(
                cols,
                groups,
                |_: &mut VecDeque<Vec<T::Native>>| {},
            )
        }
    }

    fn size(&self) -> usize {
        self.map.capacity() * size_of::<usize>()
            + self
                .values
                .iter()
                .map(|blk| blk.len() * blk.allocated_size())
                .sum::<usize>()
    }

    fn is_empty(&self) -> bool {
        self.len() == 0
    }

    fn len(&self) -> usize {
        self.map.len() + self.null_group.map(|_| 1).unwrap_or_default()
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
            // ===============================================
            // Emitting in flat mode
            // ===============================================
            EmitTo::All => {
                assert!(
                    self.block_size.is_none(),
                    "only support EmitTo::All in flat mode"
                );

                self.map.clear();
                build_primitive(
                    std::mem::take(self.values.back_mut().unwrap()),
                    self.null_group.take().map(|idx| idx as usize),
                )
            }

            EmitTo::First(n) => {
                assert!(
                    self.block_size.is_none(),
                    "only support EmitTo::First in flat mode"
                );

                let n = n as u64;
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

                let single_block = self.values.back_mut().unwrap();
                let mut split = single_block.split_off(n as usize);
                std::mem::swap(single_block, &mut split);
                build_primitive(split, null_group.map(|idx| idx as usize))
            }

            // ===============================================
            // Emitting in blocked mode
            // ===============================================
            // TODO: we should consider if it is necessary to support indices modifying
            // in `EmitTo::NextBlock`. It is only used in spilling case, maybe we can
            // always emit all in blocked mode. So, we just need to clear the map rather
            // than doing expansive modification for each buck in it.
            EmitTo::NextBlock(true) => {
                assert!(
                    self.block_size.is_some(),
                    "only support EmitTo::Next in blocked group values"
                );

                // We only emit the first block(`block_id == 0`),
                // so erase the entries with `block_id == 0`, and decrease entries with `block_id > 0`
                self.map.retain(|packed_idx| {
                    let old_blk_id =
                        BlockedGroupIndexOperations::get_block_id(*packed_idx);
                    match old_blk_id.checked_sub(1) {
                        // `block_id > 0`, shift `block_id` down
                        Some(new_blk_id) => {
                            let blk_offset =
                                BlockedGroupIndexOperations::get_block_offset(
                                    *packed_idx,
                                );
                            let new_packed_idx = BlockedGroupIndexOperations::pack_index(
                                new_blk_id as u32,
                                blk_offset,
                            );
                            *packed_idx = new_packed_idx;

                            true
                        }

                        // `block_id == 0`, so remove from table
                        None => false,
                    }
                });

                // Similar as `non-nulls`, if `block_id > 0` we decrease, and if `block_id == 0` we erase
                let null_block_pair_opt = self.null_group.map(|packed_idx| {
                    (
                        BlockedGroupIndexOperations::get_block_id(packed_idx),
                        BlockedGroupIndexOperations::get_block_offset(packed_idx),
                    )
                });
                let null_idx = match null_block_pair_opt {
                    Some((blk_id, blk_offset)) if blk_id > 0 => {
                        let new_blk_id = blk_id - 1;
                        let new_packed_idx = BlockedGroupIndexOperations::pack_index(
                            new_blk_id, blk_offset,
                        );
                        self.null_group = Some(new_packed_idx);
                        None
                    }
                    Some((_, blk_offset)) => {
                        self.null_group = None;
                        Some(blk_offset as usize)
                    }
                    None => None,
                };

                let emit_blk = self.values.pop_front().unwrap();
                build_primitive(emit_blk, null_idx)
            }

            EmitTo::NextBlock(false) => {
                assert!(
                    self.block_size.is_some(),
                    "only support EmitTo::Next in blocked group values"
                );

                let null_block_pair_opt = self.null_group.map(|packed_idx| {
                    (
                        BlockedGroupIndexOperations::get_block_id(packed_idx),
                        BlockedGroupIndexOperations::get_block_offset(packed_idx),
                    )
                });
                let null_idx = match null_block_pair_opt {
                    Some((blk_id, blk_offset)) if blk_id > 0 => {
                        let new_blk_id = blk_id - 1;
                        let new_packed_idx = BlockedGroupIndexOperations::pack_index(
                            new_blk_id, blk_offset,
                        );
                        self.null_group = Some(new_packed_idx);
                        None
                    }
                    Some((_, blk_offset)) => {
                        self.null_group = None;
                        Some(blk_offset as usize)
                    }
                    None => None,
                };

                let emit_blk = self.values.pop_front().unwrap();
                build_primitive(emit_blk, null_idx)
            }
        };

        Ok(vec![Arc::new(array.with_data_type(self.data_type.clone()))])
    }

    fn clear_shrink(&mut self, batch: &RecordBatch) {
        let count = batch.num_rows();

        // TODO: Only reserve room of values in `flat mode` currently,
        // we may need to consider it again when supporting spilling
        // for `blocked mode`.
        if self.block_size.is_none() {
            let single_block = self.values.back_mut().unwrap();
            single_block.clear();
            single_block.shrink_to(count);
        }

        self.map.clear();
        self.map.shrink_to(count, |_| 0); // hasher does not matter since the map is cleared
    }
}

impl<T: ArrowPrimitiveType> GroupValuesPrimitive<T>
where
    T::Native: HashValue,
{
    fn get_or_create_groups<F, O>(
        &mut self,
        cols: &[ArrayRef],
        groups: &mut Vec<usize>,
        mut before_add_group: F,
    ) -> Result<()>
    where
        F: FnMut(&mut VecDeque<Vec<T::Native>>),
        O: GroupIndexOperations,
    {
        assert_eq!(cols.len(), 1);
        groups.clear();

        for v in cols[0].as_primitive::<T>() {
            let group_index = match v {
                None => *self.null_group.get_or_insert_with(|| {
                    // Actions before add new group like checking if room is enough
                    before_add_group(&mut self.values);

                    // Get block infos and update block,
                    // we need `current block` and `next offset in block`
                    let block_id = self.values.len() as u32 - 1;
                    let current_block = self.values.back_mut().unwrap();
                    let block_offset = current_block.len() as u64;
                    current_block.push(Default::default());

                    // Get group index and finish actions needed it
                    O::pack_index(block_id, block_offset)
                }),
                Some(key) => {
                    let state = &self.random_state;
                    let hash = key.hash(state);
                    let insert = self.map.entry(
                        hash,
                        |g| unsafe {
                            let block_id = O::get_block_id(*g);
                            let block_offset = O::get_block_offset(*g);
                            self.values
                                .get(block_id as usize)
                                .unwrap()
                                .get_unchecked(block_offset as usize)
                                .is_eq(key)
                        },
                        |g| unsafe {
                            let block_id = O::get_block_id(*g);
                            let block_offset = O::get_block_offset(*g);
                            self.values
                                .get(block_id as usize)
                                .unwrap()
                                .get_unchecked(block_offset as usize)
                                .hash(state)
                        },
                    );

                    match insert {
                        hashbrown::hash_table::Entry::Occupied(o) => *o.get(),
                        hashbrown::hash_table::Entry::Vacant(v) => {
                            // Actions before add new group like checking if room is enough
                            before_add_group(&mut self.values);

                            // Get block infos and update block,
                            // we need `current block` and `next offset in block`
                            let block_id = self.values.len() as u32 - 1;
                            let current_block = self.values.back_mut().unwrap();
                            let block_offset = current_block.len() as u64;
                            current_block.push(key);

                            // Get group index and finish actions needed it
                            let packed_index = O::pack_index(block_id, block_offset);
                            v.insert(packed_index);
                            packed_index
                        }
                    }
                }
            };
            groups.push(group_index as usize)
        }
        Ok(())
    }
}
