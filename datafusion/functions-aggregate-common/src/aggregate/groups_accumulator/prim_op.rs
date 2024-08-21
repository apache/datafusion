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

use std::sync::Arc;

use arrow::array::{ArrayRef, AsArray, BooleanArray, PrimitiveArray};
use arrow::buffer::NullBuffer;
use arrow::compute;
use arrow::datatypes::ArrowPrimitiveType;
use arrow::datatypes::DataType;
use datafusion_common::{internal_datafusion_err, DataFusionError, Result};
use datafusion_expr_common::groups_accumulator::{
    EmitTo, GroupStatesMode, GroupsAccumulator,
};

use crate::aggregate::groups_accumulator::accumulate::BlockedNullState;
use crate::aggregate::groups_accumulator::{
    ensure_enough_room_for_blocked_values, ensure_enough_room_for_flat_values,
    BlockedGroupIndex, Blocks, EmitToExt, VecBlocks,
};

/// An accumulator that implements a single operation over
/// [`ArrowPrimitiveType`] where the accumulated state is the same as
/// the input type (such as `Sum`)
///
/// F: The function to apply to two elements. The first argument is
/// the existing value and should be updated with the second value
/// (e.g. [`BitAndAssign`] style).
///
/// [`BitAndAssign`]: std::ops::BitAndAssign
#[derive(Debug)]
pub struct PrimitiveGroupsAccumulator<T, F>
where
    T: ArrowPrimitiveType + Send,
    F: Fn(&mut T::Native, T::Native) + Send + Sync,
{
    /// values per group, stored as the native type
    values_blocks: VecBlocks<T::Native>,

    /// The output type (needed for Decimal precision and scale)
    data_type: DataType,

    /// The starting value for new groups
    starting_value: T::Native,

    /// Track nulls in the input / filters
    null_state: BlockedNullState,

    /// Function that computes the primitive result
    prim_fn: F,

    mode: GroupStatesMode,
}

impl<T, F> PrimitiveGroupsAccumulator<T, F>
where
    T: ArrowPrimitiveType + Send,
    F: Fn(&mut T::Native, T::Native) + Send + Sync,
{
    pub fn new(data_type: &DataType, prim_fn: F) -> Self {
        Self {
            values_blocks: Blocks::new(),
            data_type: data_type.clone(),
            null_state: BlockedNullState::new(GroupStatesMode::Flat),
            starting_value: T::default_value(),
            prim_fn,
            mode: GroupStatesMode::Flat,
        }
    }

    /// Set the starting values for new groups
    pub fn with_starting_value(mut self, starting_value: T::Native) -> Self {
        self.starting_value = starting_value;
        self
    }
}

impl<T, F> GroupsAccumulator for PrimitiveGroupsAccumulator<T, F>
where
    T: ArrowPrimitiveType + Send,
    F: Fn(&mut T::Native, T::Native) + Send + Sync,
{
    fn update_batch(
        &mut self,
        values: &[ArrayRef],
        group_indices: &[usize],
        opt_filter: Option<&BooleanArray>,
        total_num_groups: usize,
    ) -> Result<()> {
        if total_num_groups == 0 {
            return Ok(());
        }

        assert_eq!(values.len(), 1, "single argument to update_batch");
        let values = values[0].as_primitive::<T>();

        // NullState dispatches / handles tracking nulls and groups that saw no values
        match self.mode {
            GroupStatesMode::Flat => {
                // Ensure enough room in values
                ensure_enough_room_for_flat_values(
                    &mut self.values_blocks,
                    total_num_groups,
                    self.starting_value,
                );

                let block = self.values_blocks.current_mut().unwrap();
                self.null_state.accumulate_for_flat(
                    group_indices,
                    values,
                    opt_filter,
                    total_num_groups,
                    |group_index, new_value| {
                        let value = &mut block[group_index];
                        (self.prim_fn)(value, new_value);
                    },
                );
            }
            GroupStatesMode::Blocked(blk_size) => {
                // Ensure enough room in values
                ensure_enough_room_for_blocked_values(
                    &mut self.values_blocks,
                    total_num_groups,
                    blk_size,
                    self.starting_value,
                );

                self.null_state.accumulate_for_blocked(
                    group_indices,
                    values,
                    opt_filter,
                    total_num_groups,
                    blk_size,
                    |group_index, new_value| {
                        let blocked_index = BlockedGroupIndex::new(group_index);
                        let value = &mut self.values_blocks[blocked_index.block_id]
                            [blocked_index.block_offset];
                        (self.prim_fn)(value, new_value);
                    },
                );
            }
        }

        Ok(())
    }

    fn evaluate(&mut self, emit_to: EmitTo) -> Result<ArrayRef> {
        let values = emit_to.take_needed_from_blocks(&mut self.values_blocks, self.mode);
        let nulls = self.null_state.build(emit_to);
        let values = PrimitiveArray::<T>::new(values.into(), Some(nulls)) // no copy
            .with_data_type(self.data_type.clone());

        Ok(Arc::new(values))
    }

    fn state(&mut self, emit_to: EmitTo) -> Result<Vec<ArrayRef>> {
        self.evaluate(emit_to).map(|arr| vec![arr])
    }

    fn merge_batch(
        &mut self,
        values: &[ArrayRef],
        group_indices: &[usize],
        opt_filter: Option<&BooleanArray>,
        total_num_groups: usize,
    ) -> Result<()> {
        // update / merge are the same
        self.update_batch(values, group_indices, opt_filter, total_num_groups)
    }

    /// Converts an input batch directly to a state batch
    ///
    /// The state is:
    /// - self.prim_fn for all non null, non filtered values
    /// - null otherwise
    ///
    fn convert_to_state(
        &self,
        values: &[ArrayRef],
        opt_filter: Option<&BooleanArray>,
    ) -> Result<Vec<ArrayRef>> {
        let values = values[0].as_primitive::<T>().clone();

        // Initializing state with starting values
        let initial_state =
            PrimitiveArray::<T>::from_value(self.starting_value, values.len());

        // Recalculating values in case there is filter
        let values = match opt_filter {
            None => values,
            Some(filter) => {
                let (filter_values, filter_nulls) = filter.clone().into_parts();
                // Calculating filter mask as a result of bitand of filter, and converting it to null buffer
                let filter_bool = match filter_nulls {
                    Some(filter_nulls) => filter_nulls.inner() & &filter_values,
                    None => filter_values,
                };
                let filter_nulls = NullBuffer::from(filter_bool);

                // Rebuilding input values with a new nulls mask, which is equal to
                // the union of original nulls and filter mask
                let (dt, values_buf, original_nulls) = values.clone().into_parts();
                let nulls_buf =
                    NullBuffer::union(original_nulls.as_ref(), Some(&filter_nulls));
                PrimitiveArray::<T>::new(values_buf, nulls_buf).with_data_type(dt)
            }
        };

        let state_values = compute::binary_mut(initial_state, &values, |mut x, y| {
            (self.prim_fn)(&mut x, y);
            x
        });
        let state_values = state_values
            .map_err(|_| {
                internal_datafusion_err!(
                    "initial_values underlying buffer must not be shared"
                )
            })?
            .map_err(DataFusionError::from)?
            .with_data_type(self.data_type.clone());

        Ok(vec![Arc::new(state_values)])
    }

    fn supports_convert_to_state(&self) -> bool {
        true
    }

    fn size(&self) -> usize {
        self.values_blocks.capacity() * std::mem::size_of::<T::Native>()
            + self.null_state.size()
    }

    fn supports_blocked_mode(&self) -> bool {
        true
    }

    fn switch_to_mode(&mut self, mode: GroupStatesMode) -> Result<()> {
        self.values_blocks.clear();
        self.null_state = BlockedNullState::new(mode);
        self.mode = mode;

        Ok(())
    }
}
