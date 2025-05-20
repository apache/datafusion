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
use datafusion_common::{internal_datafusion_err, internal_err, Result};
use datafusion_execution::memory_pool::proxy::VecAllocExt;
use datafusion_expr::EmitTo;
use datafusion_functions_aggregate_common::aggregate::groups_accumulator::blocks::EmitBlocksState;
use datafusion_functions_aggregate_common::aggregate::groups_accumulator::group_index_operations::{
    BlockedGroupIndexOperations, FlatGroupIndexOperations, GroupIndexOperations,
};
use half::f16;
use hashbrown::hash_table::HashTable;
use std::mem::{self, size_of};
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
    ///
    map: HashTable<(usize, u64)>,

    /// The group index of the null value if any
    null_group: Option<usize>,

    /// The values for each group index
    values: Vec<Vec<T::Native>>,

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

    /// Number of current storing groups
    ///
    /// We maintain it to avoid the expansive dynamic computation of
    /// `groups number` and `target group index` in `blocked approach`
    ///
    /// Especially the computation of `target group index`, we need to
    /// perform it on `row-level`, it is actually very very expansive...
    ///
    /// So Even it will introduce some complexity of maintain, it still
    /// be worthy to do that.
    ///
    total_num_groups: usize,

    /// Flag used in emitting in `blocked approach`
    /// Mark if it is during blocks emitting, if so states can't
    /// be updated until all blocks are emitted
    emit_state: EmitBlocksState<Vec<Vec<T::Native>>>,
}

impl<T: ArrowPrimitiveType> GroupValuesPrimitive<T> {
    pub fn new(data_type: DataType) -> Self {
        assert!(PrimitiveArray::<T>::is_compatible(&data_type));

        // As a optimization, we ensure the `single block` always exist
        // in flat mode, it can eliminate an expansive row-level empty checking
        Self {
            data_type,
            map: HashTable::with_capacity(128),
            values: vec![vec![]],
            null_group: None,
            random_state: Default::default(),
            block_size: None,
            total_num_groups: 0,
            emit_state: EmitBlocksState::Init,
        }
    }

    #[inline]
    fn is_emitting(&self) -> bool {
        self.emit_state.is_emitting()
    }
}

impl<T: ArrowPrimitiveType> GroupValues for GroupValuesPrimitive<T>
where
    T::Native: HashValue,
{
    fn intern(&mut self, cols: &[ArrayRef], groups: &mut Vec<usize>) -> Result<()> {
        if self.is_emitting() {
            return internal_err!("can not update groups during blocks emitting");
        }

        if let Some(block_size) = self.block_size {
            let before_add_group = |group_values: &mut Vec<Vec<T::Native>>| {
                if group_values.is_empty()
                    || group_values.last().unwrap().len() == block_size
                {
                    let new_block = Vec::with_capacity(block_size);
                    group_values.push(new_block);
                }
            };

            let group_index_operation = BlockedGroupIndexOperations::new(block_size);
            self.get_or_create_groups(
                cols,
                groups,
                before_add_group,
                group_index_operation,
            )
        } else {
            let group_index_operation = FlatGroupIndexOperations;
            self.get_or_create_groups(
                cols,
                groups,
                |_: &mut Vec<Vec<T::Native>>| {},
                group_index_operation,
            )
        }
    }

    fn size(&self) -> usize {
        let map_size = self.map.capacity() * size_of::<(u64, u64)>();
        let values_size = if !self.values.is_empty() {
            // Last block may be non-full, so we compute size with two steps:
            //   - Compute size of first `n - 1` full blocks
            //   - Add the size of last block
            let num_blocks = self.values.len();
            let full_blocks_size = (num_blocks - 1)
                * self
                    .values
                    .first()
                    .map(|blk| blk.len() * blk.allocated_size())
                    .unwrap_or_default();
            let last_block_size = self
                .values
                .last()
                .map(|blk| blk.len() * blk.allocated_size())
                .unwrap_or_default();
            full_blocks_size + last_block_size
        } else {
            0
        };

        map_size + values_size
    }

    fn is_empty(&self) -> bool {
        self.len() == 0
    }

    fn len(&self) -> usize {
        self.total_num_groups
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
                    mem::take(self.values.last_mut().unwrap()),
                    self.null_group.take(),
                )
            }

            EmitTo::First(n) => {
                assert!(
                    self.block_size.is_none(),
                    "only support EmitTo::First in flat mode"
                );

                self.map.retain(|bucket| {
                    // Decrement group index by n
                    let group_idx = bucket.0;
                    match group_idx.checked_sub(n) {
                        // Group index was >= n, shift value down
                        Some(sub) => {
                            bucket.0 = sub;
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

                let single_block = self.values.last_mut().unwrap();
                let mut split = single_block.split_off(n);
                mem::swap(single_block, &mut split);
                build_primitive(split, null_group)
            }

            // ===============================================
            // Emitting in blocked mode
            // ===============================================
            EmitTo::NextBlock => {
                let (total_num_groups, block_size) = if !self.is_emitting() {
                    // Similar as `EmitTo:All`, we will clear the old index infos(like `map`)
                    // TODO: I think, we should set `total_num_groups` to 0 when blocks
                    // emitting starts. because it is used to represent number of `exist groups`,
                    // and I think `emitting groups` actually not exist anymore.
                    // We can't do it due to we use `total_num_groups(len)` to judge when we
                    // have emitted all blocks now. Actually we should judge it by checking
                    // if `None` is return from `emit`.
                    self.map.clear();
                    (
                        self.total_num_groups,
                        self.block_size
                            .expect("only support EmitTo::Next in blocked group values"),
                    )
                } else {
                    (0, 0)
                };

                let init_block_builder = || mem::take(&mut self.values);
                let emit_block = self
                    .emit_state
                    .emit_block(total_num_groups, block_size, init_block_builder)
                    .ok_or_else(|| {
                        internal_datafusion_err!("try to evaluate empty group values")
                    })?;

                // Check if `null` is in current block:
                //   - If so, we take it
                //   - If not, we shift down the group index
                let block_size = self.block_size.unwrap();
                let null_group = match &mut self.null_group {
                    Some(v) if *v >= block_size => {
                        *v -= block_size;
                        None
                    }
                    Some(_) => self.null_group.take(),
                    None => None,
                };

                let null_idx = null_group.map(|group_idx| {
                    let group_index_operation =
                        BlockedGroupIndexOperations::new(block_size);
                    group_index_operation.get_block_offset(group_idx)
                });

                build_primitive(emit_block, null_idx)
            }
        };

        self.total_num_groups -= array.len();

        Ok(vec![Arc::new(array.with_data_type(self.data_type.clone()))])
    }

    fn clear_shrink(&mut self, batch: &RecordBatch) {
        let count = batch.num_rows();

        // Clear values
        // TODO: Only reserve room of values in `flat mode` currently,
        // we may need to consider it again when supporting spilling
        // for `blocked mode`.
        if self.block_size.is_none() {
            let single_block = self.values.last_mut().unwrap();
            single_block.clear();
            single_block.shrink_to(count);
        } else {
            self.values.clear();
        }

        // Clear mappings
        self.map.clear();
        self.map.shrink_to(count, |_| 0); // hasher does not matter since the map is cleared
        self.null_group = None;

        // Clear helping structures
        self.emit_state = EmitBlocksState::Init;
        self.total_num_groups = 0;
    }

    fn supports_blocked_groups(&self) -> bool {
        true
    }

    fn alter_block_size(&mut self, block_size: Option<usize>) -> Result<()> {
        block_size
            .as_ref()
            .map(|blk_size| assert!(blk_size.is_power_of_two()));

        // Clear values
        self.values.clear();

        // Clear mappings
        self.map.clear();
        self.null_group = None;

        // Clear helping structures
        self.emit_state = EmitBlocksState::Init;
        self.total_num_groups = 0;

        // As mentioned above, we ensure the `single block` always exist
        // in `flat mode`
        if block_size.is_none() {
            self.values.push(Vec::new());
        }
        self.block_size = block_size;

        Ok(())
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
        group_index_operation: O,
    ) -> Result<()>
    where
        F: FnMut(&mut Vec<Vec<T::Native>>),
        O: GroupIndexOperations,
    {
        assert_eq!(cols.len(), 1);
        groups.clear();

        let block_size = self.block_size.unwrap_or_default();
        for v in cols[0].as_primitive::<T>() {
            let group_index = match v {
                None => *self.null_group.get_or_insert_with(|| {
                    // Actions before add new group like checking if room is enough
                    before_add_group(&mut self.values);

                    // Get block infos and update block,
                    // we need `current block` and `next offset in block`
                    let current_block = self.values.last_mut().unwrap();
                    current_block.push(Default::default());

                    // Compute group index
                    let group_index = self.total_num_groups;
                    self.total_num_groups += 1;

                    // Get group index and finish actions needed it
                    group_index
                }),
                Some(key) => {
                    let state = &self.random_state;
                    let hash = key.hash(state);
                    let insert = self.map.entry(
                        hash,
                        |g| unsafe {
                            g.1 == hash && {
                                let block_id = group_index_operation.get_block_id(g.0);
                                let block_offset =
                                    group_index_operation.get_block_offset(g.0);
                                self.values
                                    .get_unchecked(block_id)
                                    .get_unchecked(block_offset)
                                    .is_eq(key)
                            }
                        },
                        |g| g.1,
                    );

                    match insert {
                        hashbrown::hash_table::Entry::Occupied(o) => o.get().0,
                        hashbrown::hash_table::Entry::Vacant(v) => {
                            // Actions before add new group like checking if room is enough
                            before_add_group(&mut self.values);

                            // Get block infos and update block,
                            // we need `current block` and `next offset in block`
                            let current_block = self.values.last_mut().unwrap();
                            current_block.push(key);

                            // Compute group index
                            let group_index = self.total_num_groups;
                            self.total_num_groups += 1;

                            v.insert((group_index, hash));
                            group_index
                        }
                    }
                }
            };

            groups.push(group_index)
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;
    use std::sync::Arc;

    use crate::aggregates::group_values::single_group_by::primitive::GroupValuesPrimitive;
    use crate::aggregates::group_values::GroupValues;
    use arrow::array::{Array, AsArray, UInt32Array};
    use arrow::datatypes::{DataType, UInt32Type};
    use datafusion_expr::EmitTo;

    #[test]
    fn test_flat_primitive_group_values() {
        // Will cover such insert cases:
        //   1.1 Non-null row + distinct
        //   1.2 Null row + distinct
        //   1.3 Non-null row + non-distinct
        //   1.4 Null row + non-distinct
        //
        // Will cover such emit cases:
        //   2.1 Emit first n
        //   2.2 Emit all
        //   2.3 Insert again + emit
        //

        let mut group_values = GroupValuesPrimitive::<UInt32Type>::new(DataType::UInt32);
        assert_eq!(group_values.len(), 0);
        assert!(group_values.is_empty());

        let mut group_indices = vec![];
        let data0 = Arc::new(UInt32Array::from(vec![
            Some(1),
            None,
            Some(1),
            None,
            Some(2),
            Some(3),
        ]));
        let data1 = Arc::new(UInt32Array::from(vec![
            Some(3),
            Some(3),
            None,
            Some(4),
            Some(5),
        ]));

        // 1. Insert case 1.1, 1.3, 1.4 + Emit case 2.1, 2.2

        // 1.1 Insert data0, contexts to check:
        //   - Exist num groups: 4
        //   - Exist groups empty: false
        group_values
            .intern(&[Arc::clone(&data0) as _], &mut group_indices)
            .unwrap();
        assert_eq!(group_values.len(), 4);
        assert!(!group_values.is_empty());

        // 1.2 Emit first 3 groups, contexts to check:
        //   - Exist num groups: 1
        //   - Exist groups empty: false
        //   - Emitted groups are top 3 data sorted by their before group indices
        let mut expected = BTreeMap::new();
        for (&group_index, value) in group_indices.iter().zip(data0.iter()) {
            expected.insert(group_index, value);
        }
        let mut expected = expected.into_iter().collect::<Vec<_>>();
        let last_group_index = expected.len() - 1;
        let last_value = expected.last().unwrap().1;
        expected.pop();

        let emit_result = group_values.emit(EmitTo::First(3)).unwrap();
        assert_eq!(group_values.len(), 1);
        assert!(!group_values.is_empty());
        let actual = emit_result[0]
            .as_primitive::<UInt32Type>()
            .iter()
            .enumerate()
            .map(|(group_idx, val)| {
                assert!(group_idx < last_group_index);
                (group_idx, val)
            })
            .collect::<Vec<_>>();

        assert_eq!(expected, actual);

        // 1.3 Emit last 1 group(by emitting all), contexts to check:
        //   - Exist num groups: 0
        //   - Exist groups empty: true
        //   - Emitted group equal to `last_value`
        let emit_result = group_values.emit(EmitTo::All).unwrap();
        assert_eq!(group_values.len(), 0);
        assert!(group_values.is_empty());
        let emit_col = emit_result[0].as_primitive::<UInt32Type>();
        let actual = emit_col.is_valid(0).then(|| emit_col.value(0));
        assert_eq!(actual, last_value);

        // 2. Insert case 1.1, 1.2, 1.3 + Emit case 2.1, 2.2, 2.3

        // 2.1 Insert data1, contexts to check:
        //   - Exist num groups: 4
        //   - Exist groups empty: false
        group_values
            .intern(&[Arc::clone(&data1) as _], &mut group_indices)
            .unwrap();
        assert_eq!(group_values.len(), 4);
        assert!(!group_values.is_empty());

        // 2.2 Emit first 2 groups, contexts to check:
        //   - Exist num groups: 2
        //   - Exist groups empty: false
        //   - Emitted groups are top 2 data sorted by their before group indices
        let mut expected = BTreeMap::new();
        for (&group_index, value) in group_indices.iter().zip(data1.iter()) {
            expected.insert(group_index, value);
        }
        let mut expected = expected.into_iter().collect::<Vec<_>>();
        let mut last_twos = Vec::new();
        let last_value0 = expected.pop().unwrap().1;
        let last_value1 = expected.pop().unwrap().1;
        last_twos.extend([(0, last_value1), (1, last_value0)]);

        let emit_result = group_values.emit(EmitTo::First(2)).unwrap();
        assert_eq!(group_values.len(), 2);
        assert!(!group_values.is_empty());
        let actual = emit_result[0]
            .as_primitive::<UInt32Type>()
            .iter()
            .enumerate()
            .map(|(group_idx, val)| {
                assert!(group_idx < last_group_index);
                (group_idx, val)
            })
            .collect::<Vec<_>>();

        assert_eq!(actual, expected);

        // 2.3 Emit last 2 group(by emitting all), contexts to check:
        //   - Exist num groups: 0
        //   - Exist groups empty: true
        //   - Emitted groups equal to `last_twos`
        let emit_result = group_values.emit(EmitTo::All).unwrap();
        assert_eq!(group_values.len(), 0);
        assert!(group_values.is_empty());
        let actual = emit_result[0]
            .as_primitive::<UInt32Type>()
            .iter()
            .enumerate()
            .map(|(group_idx, val)| {
                assert!(group_idx < last_group_index);
                (group_idx, val)
            })
            .collect::<Vec<_>>();

        assert_eq!(actual, last_twos);
    }

    #[test]
    fn test_blocked_primitive_group_values() {
        // Will cover such insert cases:
        //   1.1 Non-null row + distinct
        //   1.2 Null row + distinct
        //   1.3 Non-null row + non-distinct
        //   1.4 Null row + non-distinct
        //
        // Will cover such emit cases:
        //   2.1 Emit block + `num_groups % block_size == 0`
        //   2.2 Insert again + emit block + `num_groups % block_size != 0`
        //

        let mut group_values = GroupValuesPrimitive::<UInt32Type>::new(DataType::UInt32);
        let block_size = 2;
        group_values.alter_block_size(Some(block_size)).unwrap();
        assert_eq!(group_values.len(), 0);
        assert!(group_values.is_empty());

        let mut group_indices = vec![];
        let data0 = Arc::new(UInt32Array::from(vec![
            Some(1),
            None,
            Some(1),
            None,
            Some(2),
            Some(3),
        ]));
        let data1 = Arc::new(UInt32Array::from(vec![Some(3), None, Some(4)]));

        // 1. Insert case 1.1, 1.3, 1.4 + Emit case 2.1

        // 1.1 Insert data0, contexts to check:
        //   - Exist num groups: 4
        //   - Exist groups empty: false
        group_values
            .intern(&[Arc::clone(&data0) as _], &mut group_indices)
            .unwrap();
        assert_eq!(group_values.len(), 4);
        assert!(!group_values.is_empty());

        // 1.2 Emit blocks, contexts to check:
        //   - Exist num groups: 0
        //   - Exist groups empty: true
        //   - Emitting flag check,
        //   - Emitted block len check
        //   - Emitted groups are equal to data sorted by their before group indices
        let mut expected = BTreeMap::new();
        for (&group_index, value) in group_indices.iter().zip(data0.iter()) {
            expected.insert(group_index, value);
        }
        let expected = expected.into_iter().collect::<Vec<_>>();

        let emit_result0 = group_values.emit(EmitTo::NextBlock).unwrap();
        assert_eq!(group_values.len(), 2);
        assert!(!group_values.is_empty());
        assert!(group_values.is_emitting());
        assert_eq!(emit_result0[0].len(), block_size);

        let emit_result1 = group_values.emit(EmitTo::NextBlock).unwrap();
        assert_eq!(group_values.len(), 0);
        assert!(group_values.is_empty());
        assert!(!group_values.is_emitting());
        assert_eq!(emit_result1[0].len(), block_size);

        let iter0 = emit_result0[0].as_primitive::<UInt32Type>().iter();
        let iter1 = emit_result1[0].as_primitive::<UInt32Type>().iter();
        let actual = iter0.chain(iter1).enumerate().collect::<Vec<_>>();
        assert_eq!(actual, expected);

        // 2. Insert case 1.1, 1.2 + Emit case 2.2

        // 2.1 Insert data0, contexts to check:
        //   - Exist num groups: 3
        //   - Exist groups empty: false
        group_values
            .intern(&[Arc::clone(&data1) as _], &mut group_indices)
            .unwrap();
        assert_eq!(group_values.len(), 3);
        assert!(!group_values.is_empty());

        // 1.2 Emit blocks, contexts to check:
        //   - Exist num groups: 0
        //   - Exist groups empty: true
        //   - Emitting flag check,
        //   - Emitted block len check
        //   - Emitted groups are equal to data sorted by their before group indices
        let mut expected = BTreeMap::new();
        for (&group_index, value) in group_indices.iter().zip(data1.iter()) {
            expected.insert(group_index, value);
        }
        let expected = expected.into_iter().collect::<Vec<_>>();

        let emit_result0 = group_values.emit(EmitTo::NextBlock).unwrap();
        assert_eq!(group_values.len(), 1);
        assert!(!group_values.is_empty());
        assert!(group_values.is_emitting());
        assert_eq!(emit_result0[0].len(), block_size);

        let emit_result1 = group_values.emit(EmitTo::NextBlock).unwrap();
        assert_eq!(group_values.len(), 0);
        assert!(group_values.is_empty());
        assert!(!group_values.is_emitting());
        assert_eq!(emit_result1[0].len(), 1);

        let iter0 = emit_result0[0].as_primitive::<UInt32Type>().iter();
        let iter1 = emit_result1[0].as_primitive::<UInt32Type>().iter();
        let actual = iter0.chain(iter1).enumerate().collect::<Vec<_>>();
        assert_eq!(actual, expected);
    }
}
