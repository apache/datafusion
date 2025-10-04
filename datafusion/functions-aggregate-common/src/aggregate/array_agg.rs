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

//! Utilities for implementing GroupsAccumulator
//! Adapter that makes [`GroupsAccumulator`] out of [`Accumulator`]

use std::iter::repeat_n;
use std::sync::Arc;

use arrow::array::{new_empty_array, Array, GenericListArray};
use arrow::array::{ArrayRef, AsArray, BooleanArray};
use arrow::buffer::OffsetBuffer;
use arrow::compute::kernels::{self};
use arrow::datatypes::Field;
use datafusion_common::utils::proxy::VecAllocExt;
use datafusion_common::{internal_datafusion_err, Result};
use datafusion_expr_common::groups_accumulator::{EmitTo, GroupsAccumulator};

#[derive(Default)]
pub struct AggGroupAccumulator {
    // [1,2,3] [4,5,6]
    stacked_batches: Vec<ArrayRef>,
    // address items of each group within the stacked_batches
    // this is maintained to perform kernel::interleave
    stacked_group_indices: Vec<Vec<(usize, usize)>>,

    /// Current memory usage, in bytes.
    allocation_bytes: usize,
}

impl AggGroupAccumulator {
    /// Create a new adapter that will create a new [`Accumulator`]
    /// for each group, using the specified factory function
    pub fn new() -> Self {
        Self {
            stacked_batches: vec![],
            stacked_group_indices: vec![],
            allocation_bytes: 0,
        }
    }
    fn consume_stacked_batches(
        &mut self,
        emit_to: EmitTo,
    ) -> Result<GenericListArray<i32>> {
        let stacked_batches = self
            .stacked_batches
            .iter()
            .map(|arr| arr.as_ref())
            .collect::<Vec<_>>();

        let group_indices = emit_to.take_needed(&mut self.stacked_group_indices);
        let mut reduced_size = 0;
        let lengths = group_indices.iter().map(|v| {
            reduced_size += v.len() * size_of::<usize>() * 2;
            v.len()
        });

        let offsets_buffer = OffsetBuffer::from_lengths(lengths);

        self.allocation_bytes += reduced_size;

        // group indices like [1,1,1,2,2,2]
        // backend_array like [a,b,c,d,e,f]
        // offsets should be: [0,3,6]
        // then result should be [a,b,c], [d,e,f]

        // backend_array is a flatten list of individual values before aggregation
        let backend_array = kernels::interleave::interleave(
            &stacked_batches,
            group_indices
                .into_iter()
                .flatten()
                .collect::<Vec<_>>()
                .as_slice(),
        )?;
        let dt = backend_array.data_type();
        let field = Arc::new(Field::new_list_field(dt.clone(), true));

        let arr =
            GenericListArray::<i32>::new(field, offsets_buffer, backend_array, None);
        Ok(arr)
    }
}

impl GroupsAccumulator for AggGroupAccumulator {
    // given the stacked_batch as:
    // - batch1 [1,4,5,6,7]
    // - batch2 [5,1,1,1,1]

    // and group_indices as
    // indices g1: [(0,0), (1,1), (1,2) ...]
    // indices g2: []
    // indices g3: []
    // indices g4: [(0,1)]
    // each tuple represents (batch_index, and offset within the batch index)
    // for example
    // - (0,0) means the 0th item inside batch1, which is `1`
    // - (1,1) means the 1th item inside batch2, which is `1`
    fn update_batch(
        &mut self,
        values: &[ArrayRef],
        group_indices: &[usize],
        opt_filter: Option<&BooleanArray>,
        total_num_groups: usize,
    ) -> Result<()> {
        if opt_filter.is_some() {
            panic!("not implemented");
        }

        let singular_col = values
            .first()
            .ok_or(internal_datafusion_err!("invalid agg input"))?;
        if self.stacked_group_indices.len() < total_num_groups {
            self.stacked_group_indices
                .resize(total_num_groups, Vec::new());
        }
        // null value is handled

        self.stacked_batches.push(Arc::clone(singular_col));
        let batch_index = self.stacked_batches.len() - 1;
        for (array_offset, group_index) in group_indices.iter().enumerate() {
            self.stacked_group_indices[*group_index].push((batch_index, array_offset));
        }

        self.allocation_bytes += size_of::<usize>() * 2 * group_indices.len();

        Ok(())
    }

    fn evaluate(&mut self, emit_to: EmitTo) -> Result<ArrayRef> {
        let arr = self.consume_stacked_batches(emit_to)?;
        Ok(Arc::new(arr) as ArrayRef)
    }

    // filtered_null_mask(opt_filter, &values);
    fn state(&mut self, emit_to: EmitTo) -> Result<Vec<ArrayRef>> {
        Ok(vec![self.evaluate(emit_to)?])
    }

    fn merge_batch(
        &mut self,
        values: &[ArrayRef],
        group_indices: &[usize],
        opt_filter: Option<&BooleanArray>,
        total_num_groups: usize,
    ) -> Result<()> {
        // TODO: all the reference to this function always result into this opt_filter as none
        assert!(opt_filter.is_none());
        let singular_col = values
            .first()
            .ok_or(internal_datafusion_err!("invalid agg input"))?;
        let list_arr = singular_col.as_list::<i32>();
        let backed_arr = list_arr.values();
        let flatten_group_index = group_indices
            .iter()
            .enumerate()
            .flat_map(|(row, group_index)| {
                let row_length = list_arr.value_length(row);
                repeat_n(*group_index, row_length as usize)
            })
            .collect::<Vec<usize>>();
        self.update_batch(
            std::slice::from_ref(backed_arr),
            &flatten_group_index,
            None,
            total_num_groups,
        )
    }

    fn size(&self) -> usize {
        // all batched array's underlying memory is borrowed, and thus not counted
        self.stacked_batches.allocated_size() + self.allocation_bytes
        // self.allocation_bytes
    }

    fn convert_to_state(
        &self,
        values: &[ArrayRef],
        opt_filter: Option<&BooleanArray>,
    ) -> Result<Vec<ArrayRef>> {
        assert!(opt_filter.is_none());
        assert!(values.len() == 1);
        let col_array = values
            .first()
            .ok_or(internal_datafusion_err!("invalid state for array agg"))?;

        let num_rows = col_array.len();
        // If there are no rows, return empty arrays
        if num_rows == 0 {
            return Ok(vec![new_empty_array(col_array.data_type())]);
        }
        let dt = col_array.data_type();

        let offsets = OffsetBuffer::from_lengths(repeat_n(1, num_rows));
        let field = Arc::new(Field::new_list_field(dt.clone(), true));

        let arr = GenericListArray::<i32>::new(
            field,
            OffsetBuffer::new(offsets.into()),
            Arc::clone(col_array),
            None,
        );
        Ok(vec![Arc::new(arr) as Arc<dyn Array>])
    }

    fn supports_convert_to_state(&self) -> bool {
        true
    }
}
