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
use crate::aggregates::group_values::single_group_by::primitive::HashValue;

use arrow::array::{
    ArrayRef, ArrowNativeTypeOp, ArrowPrimitiveType, NullBufferBuilder, PrimitiveArray,
    cast::AsArray,
};
use arrow::datatypes::DataType;
use datafusion_common::Result;
use datafusion_common::hash_utils::RandomState;
use datafusion_execution::memory_pool::proxy::VecAllocExt;
use datafusion_expr::EmitTo;
use hashbrown::hash_table::HashTable;
use std::mem::size_of;
use std::sync::Arc;

/// A single-column primitive [`GroupValues`] that stores group keys in fixed
/// sized blocks. Group ids are still logically contiguous:
///
/// ```text
/// group_id = block_index * block_size + offset_in_block
/// ```
///
/// `EmitTo::Block` consumes exactly one stored block, so callers that configure
/// accumulators with the same block size can build output batches without first
/// materializing one large contiguous values vector.
pub struct GroupValuesPrimitiveBlock<T: ArrowPrimitiveType> {
    data_type: DataType,
    map: HashTable<(usize, u64)>,
    null_group: Option<usize>,
    blocks: Vec<Box<[T::Native]>>,
    len: usize,
    block_size: usize,
    random_state: RandomState,
}

impl<T: ArrowPrimitiveType> GroupValuesPrimitiveBlock<T> {
    pub fn new(data_type: DataType, block_size: usize) -> Self {
        assert!(block_size > 0);
        assert!(PrimitiveArray::<T>::is_compatible(&data_type));
        Self {
            data_type,
            map: HashTable::with_capacity(128),
            blocks: Vec::with_capacity(1),
            null_group: None,
            len: 0,
            block_size,
            random_state: crate::aggregates::AGGREGATION_HASH_SEED,
        }
    }

    fn push_value(&mut self, value: T::Native) -> usize
    where
        T::Native: Default + Copy,
    {
        let group_id = self.len;
        if group_id.is_multiple_of(self.block_size) {
            self.blocks
                .push(vec![T::Native::default(); self.block_size].into_boxed_slice());
        }

        let block_idx = group_id / self.block_size;
        let value_idx = group_id % self.block_size;
        self.blocks[block_idx][value_idx] = value;
        self.len += 1;
        group_id
    }

    fn release_map(&mut self) {
        self.map.clear();
        self.map.shrink_to(0, |_| 0);
    }

    fn nulls_for(
        null_idx: Option<usize>,
        len: usize,
    ) -> Option<arrow::buffer::NullBuffer> {
        null_idx.map(|null_idx| {
            let mut buffer = NullBufferBuilder::new(len);
            buffer.append_n_non_nulls(null_idx);
            buffer.append_null();
            buffer.append_n_non_nulls(len - null_idx - 1);
            buffer.finish().unwrap()
        })
    }

    fn build_array(
        data_type: &DataType,
        values: Vec<T::Native>,
        null_idx: Option<usize>,
    ) -> ArrayRef {
        let nulls = Self::nulls_for(null_idx, values.len());
        Arc::new(
            PrimitiveArray::<T>::new(values.into(), nulls)
                .with_data_type(data_type.clone()),
        )
    }

    fn take_null_for_emit(&mut self, emit_len: usize) -> Option<usize> {
        match self.null_group {
            Some(null_group) if null_group < emit_len => {
                self.null_group = None;
                Some(null_group)
            }
            Some(null_group) => {
                self.null_group = Some(null_group - emit_len);
                None
            }
            None => None,
        }
    }

    fn values_range(&self, start: usize, len: usize) -> Vec<T::Native>
    where
        T::Native: Copy,
    {
        let mut output = Vec::with_capacity(len);
        let mut remaining = len;
        let mut group_id = start;

        while remaining > 0 {
            let block_idx = group_id / self.block_size;
            let offset = group_id % self.block_size;
            let take = remaining.min(self.block_size - offset);
            output.extend_from_slice(&self.blocks[block_idx][offset..offset + take]);
            remaining -= take;
            group_id += take;
        }

        output
    }

    fn rebuild_from_values(&mut self, values: &[T::Native])
    where
        T::Native: Default + Copy,
    {
        self.blocks.clear();
        self.len = 0;

        for chunk in values.chunks(self.block_size) {
            let mut block =
                vec![T::Native::default(); self.block_size].into_boxed_slice();
            block[..chunk.len()].copy_from_slice(chunk);
            self.blocks.push(block);
            self.len += chunk.len();
        }
    }

    fn take_all_values(&mut self) -> Vec<T::Native>
    where
        T::Native: Copy,
    {
        let output = self.values_range(0, self.len);
        self.blocks.clear();
        self.len = 0;
        output
    }

    fn take_block_values(&mut self) -> (Vec<T::Native>, Option<usize>) {
        self.release_map();

        let emit_len = self.len.min(self.block_size);
        let block = self.blocks.remove(0);
        let mut values = block.into_vec();
        values.truncate(emit_len);
        self.len -= emit_len;
        let null_idx = self.take_null_for_emit(emit_len);

        (values, null_idx)
    }
}

impl<T> GroupValues for GroupValuesPrimitiveBlock<T>
where
    T: ArrowPrimitiveType,
    T::Native: HashValue + Default + Copy + ArrowNativeTypeOp,
{
    fn intern(&mut self, cols: &[ArrayRef], groups: &mut Vec<usize>) -> Result<()> {
        assert_eq!(cols.len(), 1);
        groups.clear();

        for value in cols[0].as_primitive::<T>() {
            let group_id = match value {
                None => match self.null_group {
                    Some(group_id) => group_id,
                    None => {
                        let group_id = self.push_value(T::Native::default());
                        self.null_group = Some(group_id);
                        group_id
                    }
                },
                Some(key) => {
                    let state = &self.random_state;
                    let hash = key.hash(state);
                    let blocks = self.blocks.as_ptr();
                    let block_size = self.block_size;
                    let insert = self.map.entry(
                        hash,
                        |&(group_id, existing_hash)| {
                            // SAFETY: entries in `map` are created from group ids
                            // already stored in `blocks`, and `block_size` is fixed.
                            unsafe {
                                let block = &*blocks.add(group_id / block_size);
                                hash == existing_hash
                                    && block
                                        .get_unchecked(group_id % block_size)
                                        .is_eq(key)
                            }
                        },
                        |&(_, existing_hash)| existing_hash,
                    );

                    match insert {
                        hashbrown::hash_table::Entry::Occupied(entry) => entry.get().0,
                        hashbrown::hash_table::Entry::Vacant(entry) => {
                            let group_id = self.len;
                            entry.insert((group_id, hash));
                            if group_id.is_multiple_of(self.block_size) {
                                self.blocks.push(
                                    vec![T::Native::default(); self.block_size]
                                        .into_boxed_slice(),
                                );
                            }

                            let block_idx = group_id / self.block_size;
                            let value_idx = group_id % self.block_size;
                            self.blocks[block_idx][value_idx] = key;
                            self.len += 1;
                            group_id
                        }
                    }
                }
            };
            groups.push(group_id);
        }

        Ok(())
    }

    fn size(&self) -> usize {
        self.map.capacity() * size_of::<(usize, u64)>()
            + self.blocks.allocated_size()
            + self.blocks.len() * self.block_size * size_of::<T::Native>()
    }

    fn is_empty(&self) -> bool {
        self.len == 0
    }

    fn len(&self) -> usize {
        self.len
    }

    fn emit(&mut self, emit_to: EmitTo) -> Result<Vec<ArrayRef>> {
        let data_type = self.data_type.clone();
        let output = match emit_to {
            EmitTo::All => {
                self.release_map();
                let null_idx = self.null_group.take();
                let values = self.take_all_values();
                Self::build_array(&data_type, values, null_idx)
            }
            EmitTo::Block => {
                let (values, null_idx) = self.take_block_values();
                Self::build_array(&data_type, values, null_idx)
            }
            EmitTo::First(n) => {
                let n = n.min(self.len);
                let null_idx = self.take_null_for_emit(n);
                let output = self.values_range(0, n);
                let remaining = self.values_range(n, self.len - n);

                self.map.retain(|entry| match entry.0.checked_sub(n) {
                    Some(group_id) => {
                        entry.0 = group_id;
                        true
                    }
                    None => false,
                });
                self.rebuild_from_values(&remaining);

                Self::build_array(&data_type, output, null_idx)
            }
        };

        Ok(vec![output])
    }

    fn clear_shrink(&mut self, num_rows: usize) {
        self.blocks.clear();
        self.blocks.shrink_to(num_rows.div_ceil(self.block_size));
        self.len = 0;
        self.null_group = None;
        self.map.clear();
        self.map.shrink_to(num_rows, |_| 0);
    }
}
