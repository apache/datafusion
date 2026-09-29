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

use std::mem::size_of;
use std::sync::Arc;

use arrow::array::{
    ArrayRef, ArrowNativeTypeOp, ArrowPrimitiveType, NullBufferBuilder, PrimitiveArray,
    cast::AsArray,
};
use arrow::datatypes::DataType;
use datafusion_common::Result;
use datafusion_common::hash_utils::RandomState;
use datafusion_expr::{BlockedEmitTo, BlocksIndex};
use datafusion_functions_aggregate_common::aggregate::groups_accumulator::blocked_vec::BlockedVec;
use hashbrown::hash_table::HashTable;

use super::BlockedGroupValues;
use crate::aggregates::group_values::HashValue;

/// A [`BlockedGroupValues`] storing a single column of primitive values.
///
/// Like `GroupValuesPrimitive`, but the values are stored in a [`BlockedVec`]
/// and the hash table entries hold `(group_index, value)` instead of
/// `(group_index, hash)`, so probing never reads the (blocked) values.
pub(crate) struct BlockedGroupValuesPrimitive<T: ArrowPrimitiveType> {
    /// The data type of the output array
    data_type: DataType,
    /// Stores `(group_index, value)` based on the hash of the value
    ///
    /// Storing the value instead of its hash means comparing keys never goes
    /// through `values`, and rehashing is cheap for primitive values.
    map: HashTable<(BlocksIndex, T::Native)>,
    /// The group index of the null value if any
    null_group: Option<BlocksIndex>,
    /// The values for each group index
    values: BlockedVec<T::Native>,
    /// The random state used to generate hashes
    random_state: RandomState,
}

impl<T: ArrowPrimitiveType> BlockedGroupValuesPrimitive<T> {
    pub(crate) fn new(data_type: DataType, block_size: usize) -> Self {
        assert!(PrimitiveArray::<T>::is_compatible(&data_type));
        Self {
            data_type,
            map: HashTable::with_capacity(128),
            values: BlockedVec::new(block_size),
            null_group: None,
            random_state: crate::aggregates::AGGREGATION_HASH_SEED,
        }
    }

    /// Builds the output array of one block whose first group is `start`
    fn build_block(&self, values: Vec<T::Native>, start: usize) -> ArrayRef {
        let len = values.len();
        let block_size = self.values.block_size();
        let nulls = self
            .null_group
            .map(|null_group| null_group.flat(block_size))
            .filter(|null_group| (start..start + len).contains(null_group))
            .map(|null_group| {
                let null_idx = null_group - start;
                let mut buffer = NullBufferBuilder::new(len);
                buffer.append_n_non_nulls(null_idx);
                buffer.append_null();
                buffer.append_n_non_nulls(len - null_idx - 1);
                // NOTE: The inner builder must be constructed as there is at least one null
                buffer.finish().unwrap()
            });
        Arc::new(
            PrimitiveArray::<T>::new(values.into(), nulls)
                .with_data_type(self.data_type.clone()),
        )
    }

    /// Updates the null group after the first `n` groups were emitted
    fn shift_null_group(&mut self, n: usize) {
        let block_size = self.values.block_size();
        self.null_group = self
            .null_group
            .and_then(|null_group| null_group.flat(block_size).checked_sub(n))
            .map(|flat| BlocksIndex::from_flat(flat, block_size));
    }
}

impl<T: ArrowPrimitiveType> BlockedGroupValues for BlockedGroupValuesPrimitive<T>
where
    T::Native: HashValue,
{
    fn intern(&mut self, cols: &[ArrayRef], groups: &mut Vec<BlocksIndex>) -> Result<()> {
        assert_eq!(cols.len(), 1);
        groups.clear();

        for v in cols[0].as_primitive::<T>() {
            let group_id = match v {
                None => *self
                    .null_group
                    .get_or_insert_with(|| self.values.push(Default::default())),
                Some(key) => {
                    // Fold equivalence-class duplicates (e.g. `-0.0` → `+0.0`)
                    // so the bit-equal `is_eq` matches and the stored value is
                    // the canonical representative.
                    let key = key.canonicalize();
                    let state = &self.random_state;
                    let hash = key.hash(state);
                    let insert = self.map.entry(
                        hash,
                        |&(_, k)| k.is_eq(key),
                        |&(_, k)| k.hash(state),
                    );

                    match insert {
                        hashbrown::hash_table::Entry::Occupied(o) => o.get().0,
                        hashbrown::hash_table::Entry::Vacant(v) => {
                            let g = self.values.push(key);
                            v.insert((g, key));
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
        self.map.capacity() * size_of::<(BlocksIndex, T::Native)>()
            + self.values.allocated_size()
    }

    fn is_empty(&self) -> bool {
        self.values.is_empty()
    }

    fn len(&self) -> usize {
        self.values.len()
    }

    fn emit(&mut self, emit_to: BlockedEmitTo) -> Result<Vec<Vec<ArrayRef>>> {
        let blocks = match emit_to {
            BlockedEmitTo::All => {
                self.map.clear();
                let mut start = 0;
                let blocks = self
                    .values
                    .take_all()
                    .into_iter()
                    .map(|block| {
                        let len = block.len();
                        let array = self.build_block(block, start);
                        start += len;
                        vec![array]
                    })
                    .collect();
                self.null_group = None;
                blocks
            }
            BlockedEmitTo::NextBlock => {
                self.map.retain(|entry| match entry.0.block_index() {
                    // Group was in the emitted block, so remove from table
                    0 => false,
                    // Group moves down by one block
                    block_index => {
                        entry.0 =
                            BlocksIndex::new(block_index - 1, entry.0.index_in_block());
                        true
                    }
                });
                match self.values.take_next_block() {
                    Some(block) => {
                        let len = block.len();
                        let array = self.build_block(block, 0);
                        self.shift_null_group(len);
                        vec![vec![array]]
                    }
                    None => vec![],
                }
            }
            BlockedEmitTo::First(n) => {
                let block_size = self.values.block_size();
                self.map.retain(|entry| {
                    // Decrement group index by n
                    match entry.0.flat(block_size).checked_sub(n) {
                        // Group index was >= n, shift value down
                        Some(sub) => {
                            entry.0 = BlocksIndex::from_flat(sub, block_size);
                            true
                        }
                        // Group index was < n, so remove from table
                        None => false,
                    }
                });
                let block = self.values.take_first(n);
                let array = self.build_block(block, 0);
                self.shift_null_group(n);
                vec![vec![array]]
            }
        };
        Ok(blocks)
    }

    fn clear_shrink(&mut self, num_rows: usize) {
        self.values = BlockedVec::new(self.values.block_size());
        self.null_group = None;
        self.map.clear();
        self.map.shrink_to(num_rows, |_| 0); // hasher does not matter since the map is cleared
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Array, Float64Array, Int32Array};
    use arrow::datatypes::{Float64Type, Int32Type};

    fn intern(
        gv: &mut BlockedGroupValuesPrimitive<Int32Type>,
        v: Vec<Option<i32>>,
    ) -> Vec<usize> {
        let mut groups = vec![];
        gv.intern(&[Arc::new(Int32Array::from(v)) as ArrayRef], &mut groups)
            .unwrap();
        groups.into_iter().map(|g| g.flat(4)).collect()
    }

    #[test]
    fn intern_and_emit_blocks_in_order() -> Result<()> {
        let mut gv = BlockedGroupValuesPrimitive::<Int32Type>::new(DataType::Int32, 4);
        assert_eq!(
            intern(&mut gv, (0..6).map(Some).collect()),
            vec![0, 1, 2, 3, 4, 5]
        );
        assert_eq!(intern(&mut gv, vec![Some(5), Some(0)]), vec![5, 0]);
        let first = gv.emit(BlockedEmitTo::NextBlock)?;
        assert_eq!(
            first[0][0].as_primitive::<Int32Type>().values(),
            &[0, 1, 2, 3]
        );
        let rest = gv.emit(BlockedEmitTo::All)?;
        assert_eq!(rest.len(), 1);
        assert_eq!(rest[0][0].as_primitive::<Int32Type>().values(), &[4, 5]);
        assert!(gv.is_empty());
        Ok(())
    }

    #[test]
    fn null_group_in_second_block() -> Result<()> {
        let mut gv = BlockedGroupValuesPrimitive::<Int32Type>::new(DataType::Int32, 4);
        let mut v: Vec<Option<i32>> = (0..5).map(Some).collect();
        v.push(None);
        assert_eq!(intern(&mut gv, v), vec![0, 1, 2, 3, 4, 5]);
        let first = gv.emit(BlockedEmitTo::NextBlock)?;
        assert_eq!(first[0][0].null_count(), 0);
        let second = gv.emit(BlockedEmitTo::NextBlock)?;
        let arr = second[0][0].as_primitive::<Int32Type>();
        assert_eq!(arr.len(), 2);
        assert!(arr.is_null(1));
        Ok(())
    }

    #[test]
    fn first_n_renumbers_remaining_groups() -> Result<()> {
        let mut gv = BlockedGroupValuesPrimitive::<Int32Type>::new(DataType::Int32, 4);
        intern(&mut gv, (0..6).map(Some).collect());
        let out = gv.emit(BlockedEmitTo::First(2))?;
        assert_eq!(out[0][0].as_primitive::<Int32Type>().values(), &[0, 1]);
        assert_eq!(
            intern(&mut gv, vec![Some(5), Some(2), Some(9)]),
            vec![3, 0, 4]
        );
        Ok(())
    }

    #[test]
    fn first_n_shifts_null_group() -> Result<()> {
        let mut gv = BlockedGroupValuesPrimitive::<Int32Type>::new(DataType::Int32, 4);
        intern(&mut gv, vec![Some(0), None, Some(2), Some(3), Some(4)]);
        // Null group emitted
        let out = gv.emit(BlockedEmitTo::First(2))?;
        assert!(out[0][0].is_null(1));
        // A new null group is created after the emit
        assert_eq!(intern(&mut gv, vec![None, Some(4)]), vec![3, 2]);
        // Null group kept and shifted down
        let out = gv.emit(BlockedEmitTo::First(1))?;
        assert_eq!(out[0][0].null_count(), 0);
        assert_eq!(intern(&mut gv, vec![None]), vec![2]);
        Ok(())
    }

    #[test]
    fn all_with_null_group_in_last_block() -> Result<()> {
        let mut gv = BlockedGroupValuesPrimitive::<Int32Type>::new(DataType::Int32, 4);
        let mut v: Vec<Option<i32>> = (0..9).map(Some).collect();
        v.push(None);
        intern(&mut gv, v);
        let blocks = gv.emit(BlockedEmitTo::All)?;
        assert_eq!(blocks.len(), 3);
        assert_eq!(
            blocks.iter().map(|b| b[0].len()).collect::<Vec<_>>(),
            vec![4, 4, 2]
        );
        assert_eq!(blocks[0][0].null_count(), 0);
        assert_eq!(blocks[1][0].null_count(), 0);
        let last = blocks[2][0].as_primitive::<Int32Type>();
        assert_eq!(last.value(0), 8);
        assert!(last.is_null(1));
        assert!(gv.is_empty());
        // State fully reset: a new null group starts again at 0
        assert_eq!(intern(&mut gv, vec![None, Some(8)]), vec![0, 1]);
        Ok(())
    }

    #[test]
    fn float_negative_and_positive_zero_are_one_group() -> Result<()> {
        let mut gv =
            BlockedGroupValuesPrimitive::<Float64Type>::new(DataType::Float64, 4);
        let mut groups = vec![];
        let input = Arc::new(Float64Array::from(vec![-0.0, 0.0, 1.0, -0.0]));
        gv.intern(&[input as ArrayRef], &mut groups)?;
        let groups: Vec<usize> = groups.into_iter().map(|g| g.flat(4)).collect();
        assert_eq!(groups, vec![0, 0, 1, 0]);
        let out = gv.emit(BlockedEmitTo::All)?;
        let values = out[0][0].as_primitive::<Float64Type>();
        // Stored value is the canonical +0.0
        assert_eq!(values.value(0).to_bits(), 0.0f64.to_bits());
        assert_eq!(values.value(1), 1.0);
        Ok(())
    }

    #[test]
    fn keeps_output_data_type() -> Result<()> {
        let data_type = DataType::Timestamp(
            arrow::datatypes::TimeUnit::Nanosecond,
            Some("UTC".into()),
        );
        let mut gv = BlockedGroupValuesPrimitive::<
            arrow::datatypes::TimestampNanosecondType,
        >::new(data_type.clone(), 4);
        let input = Arc::new(
            arrow::array::TimestampNanosecondArray::from(vec![1, 2, 3, 4, 5])
                .with_data_type(data_type.clone()),
        );
        gv.intern(&[input as ArrayRef], &mut vec![])?;
        let blocks = gv.emit(BlockedEmitTo::All)?;
        assert_eq!(blocks.len(), 2);
        assert!(blocks.iter().all(|b| b[0].data_type() == &data_type));
        Ok(())
    }

    #[test]
    fn clear_shrink_resets_state() -> Result<()> {
        let mut gv = BlockedGroupValuesPrimitive::<Int32Type>::new(DataType::Int32, 4);
        intern(&mut gv, vec![Some(1), None, Some(2), Some(3), Some(4)]);
        gv.clear_shrink(2);
        assert!(gv.is_empty());
        assert_eq!(gv.len(), 0);
        assert_eq!(intern(&mut gv, vec![Some(3), None, Some(3)]), vec![0, 1, 0]);
        Ok(())
    }

    #[test]
    fn next_block_keeps_interning_working() -> Result<()> {
        let mut gv = BlockedGroupValuesPrimitive::<Int32Type>::new(DataType::Int32, 4);
        intern(&mut gv, (0..6).map(Some).collect());
        let first = gv.emit(BlockedEmitTo::NextBlock)?;
        assert_eq!(
            first[0][0].as_primitive::<Int32Type>().values(),
            &[0, 1, 2, 3]
        );
        // 4 and 5 moved down one block; 0 was emitted so it is a new group
        assert_eq!(
            intern(&mut gv, vec![Some(5), Some(4), Some(0)]),
            vec![1, 0, 2]
        );
        let rest = gv.emit(BlockedEmitTo::All)?;
        assert_eq!(rest[0][0].as_primitive::<Int32Type>().values(), &[4, 5, 0]);
        Ok(())
    }

    #[test]
    fn size_decreases_while_draining() -> Result<()> {
        let mut gv = BlockedGroupValuesPrimitive::<Int32Type>::new(DataType::Int32, 4);
        intern(&mut gv, (0..10).map(Some).collect());
        let mut previous = gv.size();
        assert!(previous >= 10 * size_of::<i32>());
        while !gv.is_empty() {
            previous = gv.size();
            assert_eq!(gv.emit(BlockedEmitTo::NextBlock)?.len(), 1);
            assert!(gv.size() < previous);
        }
        assert!(gv.emit(BlockedEmitTo::NextBlock)?.is_empty());
        // Only the (empty) outer `Vec` of blocks of `BlockedVec` is left
        assert!(gv.values.allocated_size() <= 4 * size_of::<Vec<i32>>());
        Ok(())
    }
}
