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

use crate::aggregates::group_values::{BlockedGroupValues, HashValue};
use arrow::array::{
    ArrayRef, ArrowNativeTypeOp, ArrowPrimitiveType, NullBufferBuilder, PrimitiveArray,
    cast::AsArray,
};
use arrow::buffer::ScalarBuffer;
use arrow::datatypes::DataType;
use datafusion_common::Result;
use datafusion_common::hash_utils::RandomState;
use datafusion_expr_common::blocked_groups_accumulator::BlocksIndex;
use datafusion_expr_common::blocked_helpers::BlockedVec;
use hashbrown::hash_table::HashTable;
use std::mem::size_of;
use std::sync::Arc;

/// A [`GroupValues`] storing a single column of primitive values
///
/// This specialization is significantly faster than using the more general
/// purpose `Row`s format
pub struct BlockedGroupValuesPrimitive<T: ArrowPrimitiveType> {
    block_size: usize,
    /// The data type of the output array
    data_type: DataType,
    /// Stores the `(group_index, hash)` based on the hash of its value
    ///
    /// We also store `hash` is for reducing cost of rehashing. Such cost
    /// is obvious in high cardinality group by situation.
    /// More details can see:
    /// <https://github.com/apache/datafusion/issues/15961>
    map: HashTable<(BlocksIndex, u64)>,
    /// The group index of the null value if any
    null_group: Option<BlocksIndex>,
    /// The values for each group index
    values: BlockedVec<T::Native>,
    /// The random state used to generate hashes
    random_state: RandomState,
}

impl<T: ArrowPrimitiveType> BlockedGroupValuesPrimitive<T> {
    pub fn new(data_type: DataType, block_size: usize) -> Self {
        assert!(PrimitiveArray::<T>::is_compatible(&data_type));
        Self {
            block_size,
            data_type,
            map: HashTable::with_capacity(128),
            values: BlockedVec::new(block_size),
            null_group: None,
            random_state: crate::aggregates::AGGREGATION_HASH_SEED,
        }
    }

    fn build_primitive(
        values: impl Into<ScalarBuffer<T::Native>>,
        null_idx: Option<usize>,
    ) -> PrimitiveArray<T> {
        let values = values.into();
        let nulls = null_idx.map(|null_idx| {
            let mut buffer = NullBufferBuilder::new(values.len());
            buffer.append_n_non_nulls(null_idx);
            buffer.append_null();
            buffer.append_n_non_nulls(values.len() - null_idx - 1);
            // NOTE: The inner builder must be constructed as there is at least one null
            buffer.finish().unwrap()
        });
        PrimitiveArray::<T>::new(values, nulls)
    }

    fn build_no_nulls_primitive_arc(
        values: impl Into<ScalarBuffer<T::Native>>,
        data_type: DataType,
    ) -> ArrayRef {
        Arc::new(PrimitiveArray::<T>::new(values.into(), None).with_data_type(data_type))
    }

    fn build_with_nulls_primitive_arc(
        values: impl Into<ScalarBuffer<T::Native>>,
        null_idx: usize,
        data_type: DataType,
    ) -> ArrayRef {
        let values = values.into();
        let nulls = {
            let mut buffer = NullBufferBuilder::new(values.len());
            buffer.append_n_non_nulls(null_idx);
            buffer.append_null();
            buffer.append_n_non_nulls(values.len() - null_idx - 1);
            // NOTE: The inner builder must be constructed as there is at least one null
            buffer.finish().unwrap()
        };
        Arc::new(
            PrimitiveArray::<T>::new(values, Some(nulls))
                .with_data_type(data_type),
        )
    }
}

impl<T: ArrowPrimitiveType> BlockedGroupValues for BlockedGroupValuesPrimitive<T>
where
    T::Native: HashValue,
{
    fn block_size(&self) -> usize {
        self.block_size
    }

    fn intern(&mut self, cols: &[ArrayRef], groups: &mut Vec<BlocksIndex>) -> Result<()> {
        assert_eq!(cols.len(), 1);
        groups.clear();

        for v in cols[0].as_primitive::<T>() {
            let group_id = match v {
                None => *self.null_group.get_or_insert_with(|| {
                    let group_id = self.values.len();
                    self.values.push(Default::default());
                    BlocksIndex::from_index_in_fixed_block_size(group_id, self.block_size)
                }),
                Some(key) => {
                    // Fold equivalence-class duplicates (e.g. `-0.0` → `+0.0`)
                    // so the bit-equal `is_eq` matches and the stored value is
                    // the canonical representative.
                    let key = key.canonicalize();
                    let state = &self.random_state;
                    let hash = key.hash(state);
                    let insert = self.map.entry(
                        hash,
                        |&(g, h)| {
                            // SAFETY: every group in the map was appended to `values`
                            hash == h
                                && unsafe { self.values.get_unchecked(g) }.is_eq(key)
                        },
                        |&(_, h)| h,
                    );

                    match insert {
                        hashbrown::hash_table::Entry::Occupied(o) => o.get().0,
                        hashbrown::hash_table::Entry::Vacant(v) => {
                            let g = BlocksIndex::from_index_in_fixed_block_size(
                                self.values.len(),
                                self.block_size,
                            );
                            v.insert((g, hash));
                            self.values.push(key);
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
        self.map.capacity() * size_of::<(BlocksIndex, u64)>()
            + self.values.allocated_size()
    }

    fn is_empty(&self) -> bool {
        self.values.is_empty()
    }

    fn len(&self) -> usize {
        self.values.len()
    }

    fn emit_all(&mut self) -> Result<Vec<Vec<ArrayRef>>> {
        self.map = HashTable::with_capacity(128);

        let all = self.values.take_all();
        let mut blocks = Vec::with_capacity(all.len());

        let mut into_iter = all.into_iter();

        if let Some(null_idx) = self.null_group.take() {
            for values in into_iter.by_ref().take(null_idx.block_index()) {
                blocks.push(vec![Self::build_no_nulls_primitive_arc(
                    values,
                    self.data_type.clone(),
                )]);
            }

            let block_with_nulls = into_iter.next().expect("must have block for nulls");
            blocks.push(vec![Self::build_with_nulls_primitive_arc(
                block_with_nulls,
                null_idx.index_in_block(),
                self.data_type.clone(),
            )]);
        }

        for values in into_iter {
            blocks.push(vec![Self::build_no_nulls_primitive_arc(
                values,
                self.data_type.clone(),
            )]);
        }

        Ok(blocks)
    }

    fn emit_block(&mut self) -> Result<Option<Vec<ArrayRef>>> {
        let Some(values) = self.values.take_block() else {
            return Ok(None);
        };

        // If nothing left
        let null_group = if self.values.is_empty() {
            self.map.clear();

            assert_eq!(self.null_group.map_or(0, |index| index.block_index()), 0);

            self.null_group.take()
        } else {
            self.map.retain(|entry| {
                // Decrement group index by n
                let group_idx = entry.0;
                match group_idx.prev_block_checked() {
                    Some(new_block_index) => {
                        entry.0 = new_block_index;
                        true
                    }
                    None => false,
                }
            });

            match &mut self.null_group {
                Some(v) if v.block_index() != 0 => {
                    *v = v.prev_block();
                    None
                }
                Some(_) => self.null_group.take(),
                None => None,
            }
        };

        let array = Self::build_primitive(values, null_group.map(|i| i.index_in_block()));

        Ok(Some(vec![Arc::new(
            array.with_data_type(self.data_type.clone()),
        )]))
    }

    fn emit_first_n(&mut self, n: usize) -> Result<Vec<ArrayRef>> {
        let block_size = self.block_size;
        let array: PrimitiveArray<T> = {
            self.map.retain(|entry| {
                // Decrement group index by n
                let group_idx = entry.0;
                match group_idx.sub_flat_checked(n, block_size) {
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
                Some(v) if v.gte_flat(n, block_size) => {
                    *v = v.sub_flat(n, block_size);
                    None
                }
                Some(_) => self.null_group.take().map(|g| g.index_in_block()),
                None => None,
            };
            let first_values = self.values.take_n(n);
            Self::build_primitive(first_values, null_group)
        };

        Ok(vec![Arc::new(array.with_data_type(self.data_type.clone()))])
    }

    fn clear_shrink(&mut self, num_rows: usize) {
        // self.values.clear();
        // self.values.shrink_to(num_rows);
        self.values.reset();
        self.map.clear();
        self.map.shrink_to(num_rows, |_| 0); // hasher does not matter since the map is cleared
    }
}
