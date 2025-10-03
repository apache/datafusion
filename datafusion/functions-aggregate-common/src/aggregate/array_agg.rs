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

use std::iter::repeat;
use std::mem::{size_of, size_of_val, take};
use std::sync::Arc;

use arrow::array::{new_empty_array, Array, GenericListArray, ListArray, StructArray};
use arrow::buffer::{OffsetBuffer, ScalarBuffer};
use arrow::compute::kernels::{self, concat};
use arrow::datatypes::{DataType, Field};
use arrow::{
    array::{ArrayRef, AsArray, BooleanArray, PrimitiveArray},
    compute,
    compute::take_arrays,
    datatypes::UInt32Type,
};
use datafusion_common::utils::SingleRowListArrayBuilder;
use datafusion_common::{
    arrow_datafusion_err, internal_datafusion_err, DataFusionError, Result, ScalarValue,
};
use datafusion_expr_common::accumulator::Accumulator;
use datafusion_expr_common::groups_accumulator::{EmitTo, GroupsAccumulator};

use crate::accumulator::AccumulatorArgs;
use crate::aggregate::groups_accumulator::accumulate::NullState;

pub struct AggGroupAccumulator {
    // [1,2,3] [4,5,6]
    stacked_batches: Vec<ArrayRef>,
    // address items of each group within the stacked_batches
    // this is maintained to perform kernel::interleave
    stacked_group_indices: Vec<Vec<(usize, usize)>>,

    // similar to the previous two fields, but these for states merging
    stacked_states: Vec<ArrayRef>,
    stacked_states_group_indices: Vec<Vec<(usize, usize)>>,
    // TODO: document me
    // for each group index, total accumulated length
    stacked_states_group_length: Vec<i32>,
    // merged_states: Vec<
    ns: NullState,

    // factory: Box<dyn Fn() -> Result<Box<dyn Accumulator>> + Send>,
    /// Current memory usage, in bytes.
    ///
    /// Note this is incrementally updated with deltas to avoid the
    /// call to size() being a bottleneck. We saw size() being a
    /// bottleneck in earlier implementations when there were many
    /// distinct groups.
    allocation_bytes: usize,
}

impl AggGroupAccumulator {
    /// Create a new adapter that will create a new [`Accumulator`]
    /// for each group, using the specified factory function
    pub fn new<F>(f: F) -> Self
    where
        F: Fn() -> Result<Box<dyn Accumulator>> + Send + 'static,
    {
        Self {
            stacked_batches: vec![],
            stacked_group_indices: vec![],
            stacked_states: vec![],
            stacked_states_group_indices: vec![],
            stacked_states_group_length: vec![],
            ns: NullState::new(),
            allocation_bytes: 0,
        }
    }
    fn consume_stacked_batches(&mut self) -> Result<GenericListArray<i32>> {
        let stacked = take(&mut self.stacked_batches);
        let stack2 = stacked.iter().map(|arr| arr.as_ref()).collect::<Vec<_>>();

        let group_indices = take(&mut self.stacked_group_indices);
        let offsets = group_indices.iter().map(|v| v.len()).scan(0, |state, len| {
            *state += len;
            Some(*state as i32)
        });

        let offsets_buffer = OffsetBuffer::new(ScalarBuffer::from_iter(offsets));
        // group indices like [1,1,1,2,2,2]
        // backend_array like [a,b,c,d,e,f]
        // offsets should be: [0,3,6]
        // then result should be [a,b,c], [d,e,f]

        // backend_array is a flatten list of individual values before aggregation
        let backend_array = kernels::interleave::interleave(
            &stack2,
            group_indices
                .into_iter()
                .flatten()
                .collect::<Vec<_>>()
                .as_slice(),
        )?;
        let dt = backend_array.data_type();
        let field = Arc::new(Field::new_list_field(dt.clone(), false));

        let arr =
            GenericListArray::<i32>::new(field, offsets_buffer, backend_array, None);
        return Ok(arr);
    }

    fn consume_stacked_states(&mut self) -> Result<GenericListArray<i32>> {
        let stacked = take(&mut self.stacked_states);
        let stacked2 = stacked.iter().map(|arr| arr.as_ref()).collect::<Vec<_>>();

        let group_indices = take(&mut self.stacked_states_group_indices);
        let group_length = take(&mut self.stacked_states_group_length);

        let offsets: Vec<i32> = group_length
            .iter()
            .scan(0, |state, len| {
                *state += len;
                Some(*state)
            })
            .collect();

        let offsets_buffer = OffsetBuffer::new(ScalarBuffer::from_iter(offsets));
        // group indices like [1,1,2,2]
        // interleave result like [[a,b],[c,d],[e,f], [g]]
        // backend array like [a,b,c,d,e,f,g]
        // offsets should be: [0,4,7]
        // then result should be [a,b,c,d], [e,f, g]
        let list_arr = kernels::interleave::interleave(
            &stacked2,
            group_indices
                .into_iter()
                .flatten()
                .collect::<Vec<_>>()
                .as_slice(),
        )?;
        let backend_array = list_arr.as_list::<i32>().values();
        let dt = backend_array.data_type();
        let field = Arc::new(Field::new_list_field(dt.clone(), false));

        let arr = GenericListArray::<i32>::new(
            field,
            offsets_buffer,
            backend_array.clone(),
            None,
        );
        return Ok(arr);
    }
}

impl GroupsAccumulator for AggGroupAccumulator {
    // batch1 [1,4,5,6,7]
    // batch2 [5,1,1,1,1]

    // indices g1: [(0,0), (1,1), (1,2) ...]
    // indices g2: []
    // indices g3: []
    // indices g4: [(0,1)]
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
            .get(0)
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

        Ok(())
    }

    fn evaluate(&mut self, emit_to: EmitTo) -> Result<ArrayRef> {
        if matches!(emit_to, EmitTo::First(_)) {
            return Err(internal_datafusion_err!("unimpl eimit to first"));
        }
        let arr = self.consume_stacked_batches()?;
        return Ok(Arc::new(arr) as ArrayRef);
        // only batch stacked no states interleaved
        // if !self.stacked_batches.is_empty()
        //     && self.stacked_states_group_indices.is_empty()
        // {
        //     let arr = self.consume_stacked_batches()?;
        //     return Ok(Arc::new(arr) as ArrayRef);
        // }
        // // only stacked states, no stacked batches interleave
        // if self.stacked_batches.is_empty()
        //     && !self.stacked_states_group_indices.is_empty()
        // {
        //     let arr = self.consume_stacked_states()?;
        //     return Ok(Arc::new(arr) as ArrayRef);
        // }
        // let stacked = take(&mut self.stacked_batches);
        // let stacked_indices = take(&mut self.stacked_group_indices);

        // let stacked_states = take(&mut self.stacked_states);
        // let staced_state_indices = take(&mut self.stacked_states_group_indices);
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
            .get(0)
            .ok_or(internal_datafusion_err!("invalid agg input"))?;
        let list_arr = singular_col.as_list::<i32>();
        let backed_arr = list_arr.values();
        let flatten_group_index = group_indices
            .iter()
            .enumerate()
            .map(|(row, group_index)| {
                let row_length = list_arr.value_length(row);
                repeat(*group_index).take(row_length as usize)
            })
            .flatten()
            .collect::<Vec<usize>>();
        return self.update_batch(
            &[backed_arr.clone()],
            &flatten_group_index,
            None,
            total_num_groups,
        );
        // if self.stacked_states.len() < total_num_groups {
        //     self.stacked_states_group_indices
        //         .resize(total_num_groups, Vec::new());
        //     self.stacked_states_group_length.resize(total_num_groups, 0);
        // }

        // let batch_index = self.stacked_states.len();
        // for (array_offset, group_index) in group_indices.iter().enumerate() {
        //     self.stacked_states_group_indices[*group_index]
        //         .push((batch_index, array_offset));
        //     self.stacked_states_group_length[*group_index] +=
        //         singular_col.as_list::<i32>().value_length(array_offset)
        // }

        // self.stacked_states.push(Arc::clone(singular_col));
        Ok(())
    }

    fn size(&self) -> usize {
        1000
        // self.allocation_bytes
    }

    fn convert_to_state(
        &self,
        values: &[ArrayRef],
        opt_filter: Option<&BooleanArray>,
    ) -> Result<Vec<ArrayRef>> {
        assert!(opt_filter.is_none());
        let col_array = values
            .get(0)
            .ok_or(internal_datafusion_err!("invalid state for array agg"))?;

        let num_rows = col_array.len();
        // If there are no rows, return empty arrays
        if num_rows == 0 {
            return Ok(vec![new_empty_array(col_array.data_type())]);
        }
        let dt = col_array.data_type();

        let offsets = OffsetBuffer::from_lengths(std::iter::repeat_n(1, num_rows));
        let field = Arc::new(Field::new_list_field(dt.clone(), false));

        let arr = GenericListArray::<i32>::new(
            field,
            OffsetBuffer::new(offsets.into()),
            col_array.clone(),
            None,
        );
        return Ok(vec![Arc::new(arr) as Arc<dyn Array>]);
    }

    fn supports_convert_to_state(&self) -> bool {
        true
    }
}

/// Extension trait for [`Vec`] to account for allocations.
pub trait VecAllocExt {
    /// Item type.
    type T;
    /// Return the amount of memory allocated by this Vec (not
    /// recursively counting any heap allocations contained within the
    /// structure). Does not include the size of `self`
    fn allocated_size(&self) -> usize;
}

impl<T> VecAllocExt for Vec<T> {
    type T = T;
    fn allocated_size(&self) -> usize {
        size_of::<T>() * self.capacity()
    }
}

fn get_filter_at_indices(
    opt_filter: Option<&BooleanArray>,
    indices: &PrimitiveArray<UInt32Type>,
) -> Result<Option<ArrayRef>> {
    opt_filter
        .map(|filter| {
            compute::take(
                &filter, indices, None, // None: no index check
            )
        })
        .transpose()
        .map_err(|e| arrow_datafusion_err!(e))
}

// Copied from physical-plan
pub(crate) fn slice_and_maybe_filter(
    aggr_array: &[ArrayRef],
    filter_opt: Option<&BooleanArray>,
    offsets: &[usize],
) -> Result<Vec<ArrayRef>> {
    let (offset, length) = (offsets[0], offsets[1] - offsets[0]);
    let sliced_arrays: Vec<ArrayRef> = aggr_array
        .iter()
        .map(|array| array.slice(offset, length))
        .collect();

    if let Some(f) = filter_opt {
        let filter = f.slice(offset, length);

        sliced_arrays
            .iter()
            .map(|array| {
                compute::filter(&array, &filter).map_err(|e| arrow_datafusion_err!(e))
            })
            .collect()
    } else {
        Ok(sliced_arrays)
    }
}

fn inner_datatype_from_list(dt: &DataType) -> DataType {
    match dt {
        DataType::List(f) | DataType::FixedSizeList(f, _) | DataType::LargeList(f) => {
            f.data_type().clone()
        }
        _ => dt.clone(),
    }
}

fn try_unnest(a: &ScalarValue) -> Option<Arc<ListArray>> {
    match a {
        ScalarValue::List(l) => Some(l.clone()),
        _ => None,
    }
}
