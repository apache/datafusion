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
use datafusion_functions_aggregate_common::aggregate::groups_accumulator::block_store::{
    BlockedBlockStore, FlatBlockStore, VecBlockStore,
};
use datafusion_functions_aggregate_common::aggregate::groups_accumulator::group_index_operations::{
    BlockedGroupIndexOperations, FlatGroupIndexOperations, GroupIndexOperations,
};
use half::f16;
use hashbrown::hash_table::HashTable;
#[cfg(not(feature = "force_hash_collisions"))]
use std::hash::BuildHasher;
use std::marker::PhantomData;
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
    map: HashTable<(u64, u64)>,
    /// The group index of the null value if any
    null_group: Option<u64>,

    /// The values for each group index, stored according to the current group mode.
    state: GroupValuesPrimitiveStateAdapter<T::Native>,

    /// The random state used to generate hashes
    random_state: RandomState,
}

impl<T: ArrowPrimitiveType> GroupValuesPrimitive<T> {
    pub fn new(data_type: DataType) -> Self {
        assert!(PrimitiveArray::<T>::is_compatible(&data_type));

        Self {
            data_type,
            map: HashTable::with_capacity(128),
            state: GroupValuesPrimitiveStateAdapter::new_flat(),
            null_group: None,
            random_state: crate::aggregates::AGGREGATION_HASH_SEED,
        }
    }
}

#[derive(Debug)]
struct GroupValuesPrimitiveState<V, VB, O>
where
    V: Clone + std::fmt::Debug,
    VB: VecBlockStore<V> + Send,
    O: GroupIndexOperations,
{
    values: VB,
    _phantom: PhantomData<(V, O)>,
}

impl<V, VB, O> GroupValuesPrimitiveState<V, VB, O>
where
    V: Clone + std::fmt::Debug,
    VB: VecBlockStore<V> + Send,
    O: GroupIndexOperations,
{
    fn new(values: VB) -> Self {
        Self {
            values,
            _phantom: PhantomData,
        }
    }

    fn intern<T>(
        &mut self,
        map: &mut HashTable<(u64, u64)>,
        null_group: &mut Option<u64>,
        random_state: &RandomState,
        cols: &[ArrayRef],
        groups: &mut Vec<usize>,
    ) -> Result<()>
    where
        T: ArrowPrimitiveType<Native = V>,
        V: ArrowNativeTypeOp + Default + HashValue + Send,
    {
        assert_eq!(cols.len(), 1);
        groups.clear();

        for v in cols[0].as_primitive::<T>() {
            let packed_index = match v {
                None => *null_group
                    .get_or_insert_with(|| self.push_new_group(Default::default())),
                Some(key) => {
                    let hash = key.hash(random_state);
                    let insert = map.entry(
                        hash,
                        |&(idx, h)| unsafe {
                            if hash != h {
                                return false;
                            }

                            let block_id = O::get_block_id(idx);
                            let block_offset = O::get_block_offset(idx);
                            self.values[block_id as usize]
                                .get_unchecked(block_offset as usize)
                                .is_eq(key)
                        },
                        |&(_, h)| h,
                    );

                    match insert {
                        hashbrown::hash_table::Entry::Occupied(o) => {
                            let (idx, _) = *o.get();
                            idx
                        }
                        hashbrown::hash_table::Entry::Vacant(v) => {
                            let packed_index = self.push_new_group(key);
                            v.insert((packed_index, hash));
                            packed_index
                        }
                    }
                }
            };
            groups.push(packed_index as usize)
        }
        Ok(())
    }

    fn push_new_group(&mut self, value: V) -> u64 {
        self.values.allocate_block();

        let block_id = self.values.num_blocks().saturating_sub(1);
        let current_block = &mut self.values[block_id];
        let block_offset = current_block.len() as u64;
        current_block.push(value);
        O::pack_index(block_id as u32, block_offset)
    }

    fn emit<T>(
        &mut self,
        map: &mut HashTable<(u64, u64)>,
        null_group: &mut Option<u64>,
        data_type: &DataType,
        emit_to: EmitTo,
    ) -> Result<Vec<ArrayRef>>
    where
        T: ArrowPrimitiveType<Native = V>,
        V: Send,
    {
        fn build_primitive<T: ArrowPrimitiveType>(
            values: Vec<T::Native>,
            null_idx: Option<usize>,
        ) -> PrimitiveArray<T> {
            let nulls = null_idx.map(|null_idx| {
                let mut buffer = NullBufferBuilder::new(values.len());
                buffer.append_n_non_nulls(null_idx);
                buffer.append_null();
                buffer.append_n_non_nulls(values.len() - null_idx - 1);
                buffer.finish().unwrap()
            });
            PrimitiveArray::<T>::new(values.into(), nulls)
        }

        let array: PrimitiveArray<T> = match emit_to {
            EmitTo::All => {
                map.clear();
                let values = self.values.emit(emit_to)?;
                let null_group_opt = null_group.take().map(|packed_index| {
                    let blk_offset = O::get_block_offset(packed_index);
                    blk_offset as usize
                });
                build_primitive(values, null_group_opt)
            }
            EmitTo::First(n) => {
                let n = n as u64;
                map.retain(|entry| {
                    let packed_index = entry.0;
                    let blk_offset = O::get_block_offset(packed_index);
                    match blk_offset.checked_sub(n) {
                        Some(sub) => {
                            let packed_index = O::pack_index(0, sub);
                            entry.0 = packed_index;
                            true
                        }
                        None => false,
                    }
                });

                let null_group_opt = match null_group {
                    Some(v) if *v >= n => {
                        let mut blk_offset = O::get_block_offset(*v);
                        blk_offset -= n;
                        *v = O::pack_index(0, blk_offset);
                        None
                    }
                    Some(_) => null_group.take().map(|packed_index| {
                        let blk_offset = O::get_block_offset(packed_index);
                        blk_offset as usize
                    }),
                    None => None,
                };

                let split = self.values.emit(emit_to)?;
                build_primitive(split, null_group_opt)
            }
            EmitTo::NextBlock => {
                map.clear();

                let null_block_pair_opt = null_group.map(|packed_index| {
                    (
                        O::get_block_id(packed_index),
                        O::get_block_offset(packed_index),
                    )
                });
                let null_idx = match null_block_pair_opt {
                    Some((blk_id, blk_offset)) if blk_id > 0 => {
                        let new_blk_id = blk_id - 1;
                        let new_packed_index = O::pack_index(new_blk_id, blk_offset);
                        *null_group = Some(new_packed_index);
                        None
                    }
                    Some((_, blk_offset)) => {
                        *null_group = None;
                        Some(blk_offset as usize)
                    }
                    None => None,
                };

                let emit_blk = self.values.emit(emit_to)?;
                build_primitive(emit_blk, null_idx)
            }
        };

        Ok(vec![Arc::new(array.with_data_type(data_type.clone()))])
    }

    fn size(&self) -> usize {
        (0..self.values.num_blocks())
            .map(|block_id| {
                let block = &self.values[block_id];
                block.len() * block.allocated_size()
            })
            .sum::<usize>()
    }

    fn len(&self) -> usize {
        if self.values.is_empty() {
            return 0;
        }

        (0..self.values.num_blocks())
            .map(|block_id| self.values[block_id].len())
            .sum::<usize>()
    }
}

type FlatGroupValuesPrimitiveState<V> =
    GroupValuesPrimitiveState<V, FlatBlockStore<Vec<V>>, FlatGroupIndexOperations>;
type BlockedGroupValuesPrimitiveState<V> =
    GroupValuesPrimitiveState<V, BlockedBlockStore<Vec<V>>, BlockedGroupIndexOperations>;

#[derive(Debug)]
enum GroupValuesPrimitiveStateAdapter<V: Clone + std::fmt::Debug + Send> {
    Flat(FlatGroupValuesPrimitiveState<V>),
    Blocked(BlockedGroupValuesPrimitiveState<V>),
}

impl<V: Clone + std::fmt::Debug + Send> GroupValuesPrimitiveStateAdapter<V> {
    fn new_flat() -> Self {
        Self::Flat(GroupValuesPrimitiveState::new(FlatBlockStore::new()))
    }

    fn new_blocked(block_size: usize) -> Self {
        Self::Blocked(GroupValuesPrimitiveState::new(BlockedBlockStore::new(
            block_size,
        )))
    }

    fn size(&self) -> usize {
        match self {
            Self::Flat(state) => state.size(),
            Self::Blocked(state) => state.size(),
        }
    }

    fn len(&self) -> usize {
        match self {
            Self::Flat(state) => state.len(),
            Self::Blocked(state) => state.len(),
        }
    }

    fn emit<T: ArrowPrimitiveType<Native = V>>(
        &mut self,
        map: &mut HashTable<(u64, u64)>,
        null_group: &mut Option<u64>,
        data_type: &DataType,
        emit_to: EmitTo,
    ) -> Result<Vec<ArrayRef>>
    where
        V: Send,
    {
        match self {
            Self::Flat(state) => {
                state.emit::<T>(map, null_group, data_type, emit_to)
            }
            Self::Blocked(state) => {
                state.emit::<T>(map, null_group, data_type, emit_to)
            }
        }
    }

    fn clear_shrink(&mut self, num_rows: usize) {
        if let Self::Flat(state) = self {
            let single_block = &mut state.values[0];
            single_block.clear();
            single_block.shrink_to(num_rows);
        }
    }
}

impl<T: ArrowPrimitiveType> GroupValues for GroupValuesPrimitive<T>
where
    T::Native: HashValue + Send + std::fmt::Debug,
{
    fn intern(&mut self, cols: &[ArrayRef], groups: &mut Vec<usize>) -> Result<()> {
        match &mut self.state {
            GroupValuesPrimitiveStateAdapter::Flat(state) => state
                .intern::<T>(
                    &mut self.map,
                    &mut self.null_group,
                    &self.random_state,
                    cols,
                    groups,
                ),
            GroupValuesPrimitiveStateAdapter::Blocked(state) => state
                .intern::<T>(
                    &mut self.map,
                    &mut self.null_group,
                    &self.random_state,
                    cols,
                    groups,
                ),
        }
    }

    fn size(&self) -> usize {
        self.map.capacity() * size_of::<(usize, u64)>() + self.state.size()
    }

    fn is_empty(&self) -> bool {
        self.len() == 0
    }

    fn len(&self) -> usize {
        self.state.len()
    }

    fn emit(&mut self, emit_to: EmitTo) -> Result<Vec<ArrayRef>> {
        self.state.emit::<T>(
            &mut self.map,
            &mut self.null_group,
            &self.data_type,
            emit_to,
        )
    }

    fn clear_shrink(&mut self, num_rows: usize) {
        self.state.clear_shrink(num_rows);
        self.map.clear();
        self.map.shrink_to(num_rows, |_| 0);
    }

    fn supports_blocked_groups(&self) -> bool {
        true
    }

    fn alter_block_size(&mut self, block_size: Option<usize>) -> Result<()> {
        self.map.clear();
        self.null_group = None;
        self.state = if let Some(block_size) = block_size {
            GroupValuesPrimitiveStateAdapter::new_blocked(block_size)
        } else {
            GroupValuesPrimitiveStateAdapter::new_flat()
        };

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;
    use std::sync::Arc;

    use crate::aggregates::group_values::single_group_by::primitive::GroupValuesPrimitive;
    use crate::aggregates::group_values::GroupValues;
    use arrow::array::{AsArray, UInt32Array};
    use arrow::datatypes::{DataType, UInt32Type};
    use datafusion_expr::EmitTo;
    use datafusion_functions_aggregate_common::aggregate::groups_accumulator::group_index_operations::{
        BlockedGroupIndexOperations, GroupIndexOperations,
    };

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
        let mut group_values = GroupValuesPrimitive::<UInt32Type>::new(DataType::UInt32);
        let mut group_indices = vec![];

        let data1 = Arc::new(UInt32Array::from(vec![
            Some(1),
            None,
            Some(1),
            None,
            Some(2),
            Some(3),
        ]));
        let data2 = Arc::new(UInt32Array::from(vec![Some(3), None, Some(4), Some(5)]));

        // Insert case 1.1, 1.3, 1.4 + Emit case 2.1
        group_values
            .intern(&[Arc::clone(&data1) as _], &mut group_indices)
            .unwrap();

        let mut expected = BTreeMap::new();
        for (&group_index, value) in group_indices.iter().zip(data1.iter()) {
            expected.insert(group_index, value);
        }
        let mut expected = expected.into_iter().collect::<Vec<_>>();
        let last_group_index = expected.len() - 1;
        let last_value = expected.last().unwrap().1;
        expected.pop();

        let emit_result = group_values.emit(EmitTo::First(3)).unwrap();
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

        // Insert case 1.1~1.3 + Emit case 2.2~2.3
        group_values
            .intern(&[Arc::clone(&data2) as _], &mut group_indices)
            .unwrap();

        let mut expected = BTreeMap::new();
        for (&group_index, value) in group_indices.iter().zip(data2.iter()) {
            if group_index == 0 {
                assert_eq!(last_value, value);
            }
            expected.insert(group_index, value);
        }
        let expected = expected.into_iter().collect::<Vec<_>>();

        let emit_result = group_values.emit(EmitTo::All).unwrap();
        let actual = emit_result[0]
            .as_primitive::<UInt32Type>()
            .iter()
            .enumerate()
            .collect::<Vec<_>>();

        assert_eq!(expected, actual);
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
        //   2.1 Emit block
        //   2.2 Insert again + emit block
        //
        let mut group_values = GroupValuesPrimitive::<UInt32Type>::new(DataType::UInt32);
        let block_size = 2;
        group_values.alter_block_size(Some(block_size)).unwrap();
        let mut group_indices = vec![];

        let data1 = Arc::new(UInt32Array::from(vec![
            Some(1),
            None,
            Some(1),
            None,
            Some(2),
            Some(3),
        ]));
        let data2 = Arc::new(UInt32Array::from(vec![Some(3), None, Some(4)]));

        // Insert case 1.1, 1.3, 1.4 + Emit case 2.1
        group_values
            .intern(&[Arc::clone(&data1) as _], &mut group_indices)
            .unwrap();

        let mut expected = BTreeMap::new();
        for (&packed_index, value) in group_indices.iter().zip(data1.iter()) {
            let block_id = BlockedGroupIndexOperations::get_block_id(packed_index as u64);
            let block_offset =
                BlockedGroupIndexOperations::get_block_offset(packed_index as u64);
            let flatten_index = block_id as usize * block_size + block_offset as usize;
            expected.insert(flatten_index, value);
        }
        let expected = expected.into_iter().collect::<Vec<_>>();

        let emit_result1 = group_values.emit(EmitTo::NextBlock).unwrap();
        assert_eq!(emit_result1[0].len(), block_size);
        let emit_result2 = group_values.emit(EmitTo::NextBlock).unwrap();
        assert_eq!(emit_result2[0].len(), block_size);
        let iter1 = emit_result1[0].as_primitive::<UInt32Type>().iter();
        let iter2 = emit_result2[0].as_primitive::<UInt32Type>().iter();
        let actual = iter1.chain(iter2).enumerate().collect::<Vec<_>>();

        assert_eq!(actual, expected);

        // Insert case 1.1~1.2 + Emit case 2.2
        group_values
            .intern(&[Arc::clone(&data2) as _], &mut group_indices)
            .unwrap();

        let mut expected = BTreeMap::new();
        for (&packed_index, value) in group_indices.iter().zip(data2.iter()) {
            let block_id = BlockedGroupIndexOperations::get_block_id(packed_index as u64);
            let block_offset =
                BlockedGroupIndexOperations::get_block_offset(packed_index as u64);
            let flatten_index = block_id as usize * block_size + block_offset as usize;
            expected.insert(flatten_index, value);
        }
        let expected = expected.into_iter().collect::<Vec<_>>();

        let emit_result1 = group_values.emit(EmitTo::NextBlock).unwrap();
        assert_eq!(emit_result1[0].len(), block_size);
        let emit_result2 = group_values.emit(EmitTo::NextBlock).unwrap();
        assert_eq!(emit_result2[0].len(), 1);
        let iter1 = emit_result1[0].as_primitive::<UInt32Type>().iter();
        let iter2 = emit_result2[0].as_primitive::<UInt32Type>().iter();
        let actual = iter1.chain(iter2).enumerate().collect::<Vec<_>>();

        assert_eq!(actual, expected);
    }
}
