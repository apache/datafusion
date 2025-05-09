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
use datafusion_expr::groups_accumulator::EmitBlocksContext;
use datafusion_expr::EmitTo;
use datafusion_functions_aggregate_common::aggregate::groups_accumulator::group_index_operations::{
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
    num_groups: usize,

    /// Context used in emitting in `blocked approach`
    emit_blocks_ctx: EmitBlocksContext,
}

impl<T: ArrowPrimitiveType> GroupValuesPrimitive<T> {
    pub fn new(data_type: DataType) -> Self {
        assert!(PrimitiveArray::<T>::is_compatible(&data_type));

        // As a optimization, we ensure the `single block` always exist
        // in flat mode, it can eliminate an expansive row-level empty checking
        let mut values = Vec::new();
        values.push(Vec::new());

        Self {
            data_type,
            map: HashTable::with_capacity(128),
            values,
            null_group: None,
            random_state: Default::default(),
            block_size: None,
            num_groups: 0,
            emit_blocks_ctx: EmitBlocksContext::new(),
        }
    }
}

impl<T: ArrowPrimitiveType> GroupValues for GroupValuesPrimitive<T>
where
    T::Native: HashValue,
{
    fn intern(&mut self, cols: &[ArrayRef], groups: &mut Vec<usize>) -> Result<()> {
        assert!(
            !self.emit_blocks_ctx.emitting(),
            "can not update groups during emitting"
        );

        if let Some(block_size) = self.block_size {
            let before_add_group = |group_values: &mut Vec<Vec<T::Native>>| {
                if group_values.is_empty()
                    || group_values.last().unwrap().len() == block_size
                {
                    let new_block = Vec::with_capacity(block_size);
                    group_values.push(new_block);
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
                |_: &mut Vec<Vec<T::Native>>| {},
            )
        }
    }

    fn size(&self) -> usize {
        self.map.capacity() * size_of::<(u64, u64)>()
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
                    std::mem::take(self.values.last_mut().unwrap()),
                    self.null_group.take().map(|idx| idx as usize),
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
                let mut split = single_block.split_off(n as usize);
                std::mem::swap(single_block, &mut split);
                build_primitive(split, null_group.map(|idx| idx as usize))
            }

            // ===============================================
            // Emitting in blocked mode
            // ===============================================
            EmitTo::NextBlock => {
                let block_size = self
                    .block_size
                    .expect("only support EmitTo::Next in blocked group values");

                // To mark the emitting has already started, and prevent new updates
                if !self.emit_blocks_ctx.emitting() {
                    let num_blocks = self.values.len();
                    self.emit_blocks_ctx.start_emit(num_blocks);
                }

                // Similar as `EmitTo:All`, we will clear the old index infos both
                // in `map` and `null_group`
                self.map.clear();

                // Get current emit block idx firstly
                let emit_block_id = self.emit_blocks_ctx.cur_emit_block();
                let emit_blk = std::mem::take(&mut self.values[emit_block_id]);
                // And then we advance the block idx
                self.emit_blocks_ctx.advance_emit_block();
                // Finally we check if all blocks emitted, if so, we reset the
                // emit context to allow new updates
                if self.emit_blocks_ctx.all_emitted() {
                    self.emit_blocks_ctx.reset();
                    self.values.clear();
                }

                // Check if `null` is in current block
                let null_block_pair_opt = self.null_group.map(|group_index| {
                    (
                        BlockedGroupIndexOperations::get_block_id(
                            group_index,
                            block_size,
                        ),
                        BlockedGroupIndexOperations::get_block_offset(
                            group_index,
                            block_size,
                        ),
                    )
                });
                let null_idx = match null_block_pair_opt {
                    Some((blk_id, blk_offset)) if blk_id as usize == emit_block_id => {
                        // Clear `null_group` mapping
                        self.null_group = None;
                        Some(blk_offset as usize)
                    }
                    _ => None,
                };

                build_primitive(emit_blk, null_idx)
            }
        };

        // Maintain number of groups
        self.num_groups -= array.len();

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
        self.emit_blocks_ctx.reset();
        self.num_groups = 0;
    }

    fn supports_blocked_groups(&self) -> bool {
        true
    }

    fn alter_block_size(&mut self, block_size: Option<usize>) -> Result<()> {
        // Clear values
        self.values.clear();

        // Clear mappings
        self.map.clear();
        self.null_group = None;

        // Clear helping structures
        self.emit_blocks_ctx.reset();
        self.num_groups = 0;

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
                    let group_index = self.num_groups;
                    self.num_groups += 1;

                    // Get group index and finish actions needed it
                    group_index
                }),
                Some(key) => {
                    let state = &self.random_state;
                    let hash = key.hash(state);
                    let insert = self.map.entry(
                        hash,
                        |g| unsafe {
                            let block_id = O::get_block_id(g.0, block_size);
                            let block_offset = O::get_block_offset(g.0, block_size);
                            self.values
                                .get_unchecked(block_id)
                                .get_unchecked(block_offset)
                                .is_eq(key)
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
                            let group_index = self.num_groups;
                            self.num_groups += 1;

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
    use arrow::array::{AsArray, RecordBatch, UInt32Array};
    use arrow::datatypes::{DataType, UInt32Type};
    use arrow_schema::Schema;
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
        for (&group_index, value) in group_indices.iter().zip(data1.iter()) {
            let block_id =
                BlockedGroupIndexOperations::get_block_id(group_index, block_size);
            let block_offset =
                BlockedGroupIndexOperations::get_block_offset(group_index, block_size);
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
        for (&group_index, value) in group_indices.iter().zip(data2.iter()) {
            let block_id =
                BlockedGroupIndexOperations::get_block_id(group_index, block_size);
            let block_offset =
                BlockedGroupIndexOperations::get_block_offset(group_index, block_size);
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
