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
use datafusion_common::{DataFusionError, Result, internal_datafusion_err};
use datafusion_expr_common::blocked_groups_accumulator::{
    BlockedEmitTo, BlockedGroupSelection, BlockedGroupsAccumulator, BlocksIndex,
};
use datafusion_expr_common::blocked_helpers::BlockedVec;

use super::accumulate::BlockedNullState;

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
pub struct BlockedPrimitiveGroupsAccumulator<T, F>
where
    T: ArrowPrimitiveType + Send,
    F: Fn(&mut T::Native, T::Native) + Send + Sync + 'static,
{
    /// values per group, stored as the native type
    values: BlockedVec<T::Native>,

    /// The output type (needed for Decimal precision and scale)
    data_type: DataType,

    /// The starting value for new groups
    starting_value: T::Native,

    /// Track nulls in the input / filters
    null_state: BlockedNullState,

    /// Function that computes the primitive result
    prim_fn: F,
}

impl<T, F> BlockedPrimitiveGroupsAccumulator<T, F>
where
    T: ArrowPrimitiveType + Send,
    F: Fn(&mut T::Native, T::Native) + Send + Sync + 'static,
{
    pub fn new(data_type: &DataType, block_size: usize, prim_fn: F) -> Self {
        Self {
            values: BlockedVec::new(block_size),
            data_type: data_type.clone(),
            null_state: BlockedNullState::new(block_size),
            starting_value: T::default_value(),
            prim_fn,
        }
    }

    /// Set the starting values for new groups
    pub fn with_starting_value(mut self, starting_value: T::Native) -> Self {
        self.starting_value = starting_value;
        self
    }
}

impl<T, F> BlockedGroupsAccumulator for BlockedPrimitiveGroupsAccumulator<T, F>
where
    T: ArrowPrimitiveType + Send,
    F: Fn(&mut T::Native, T::Native) + Send + Sync + 'static,
{
    fn batch_size(&self) -> usize {
        self.values.block_size()
    }

    fn update_batch(
        &mut self,
        values: &[ArrayRef],
        group_indices: &[BlocksIndex],
        opt_filter: Option<&BooleanArray>,
        total_num_groups: usize,
    ) -> Result<()> {
        assert_eq!(values.len(), 1, "single argument to update_batch");
        let values = values[0].as_primitive::<T>();

        // update values
        self.values
            .push_value_n_to_len(self.starting_value, total_num_groups);

        // NullState dispatches / handles tracking nulls and groups that saw no values
        self.null_state.accumulate(
            group_indices,
            values,
            opt_filter,
            total_num_groups,
            |group_index, new_value| {
                // SAFETY: group_index is guaranteed to be in bounds
                let value = unsafe { self.values.get_unchecked_mut(group_index) };
                (self.prim_fn)(value, new_value);
            },
        );

        Ok(())
    }

    fn evaluate(&mut self, emit_to: BlockedEmitTo) -> Result<Vec<ArrayRef>> {
        let values = self.values.emit(emit_to);
        let nulls = self.null_state.build(emit_to);

        let blocks = values
            .into_iter()
            .zip(nulls)
            .map(|(values, nulls)| {
                let values = PrimitiveArray::<T>::new(values.into(), nulls) // no copy
                    .with_data_type(self.data_type.clone());
                Arc::new(values) as ArrayRef
            })
            .collect::<Vec<_>>();

        Ok(blocks)
    }

    fn evaluate_preserving(
        &mut self,
        selection: BlockedGroupSelection<'_>,
    ) -> Result<ArrayRef> {
        selection.validate_num_groups(self.values.len())?;
        let values: Vec<T::Native> =
            selection.iter().map(|index| self.values[index]).collect();
        let nulls = self.null_state.build_preserving(selection)?;
        let values = PrimitiveArray::<T>::new(values.into(), nulls)
            .with_data_type(self.data_type.clone());
        Ok(Arc::new(values))
    }

    fn supports_evaluate_preserving(&self) -> bool {
        true
    }

    fn state(&mut self, emit_to: BlockedEmitTo) -> Result<Vec<Vec<ArrayRef>>> {
        let blocks = self.evaluate(emit_to)?;

        Ok(blocks
            .into_iter()
            // Convert each block into a Vec<ArrayRef> to match the return type
            .map(|arr| vec![arr])
            .collect::<Vec<_>>())
    }

    fn state_preserving(
        &mut self,
        selection: BlockedGroupSelection<'_>,
    ) -> Result<Vec<ArrayRef>> {
        self.evaluate_preserving(selection).map(|arr| vec![arr])
    }

    fn supports_state_preserving(&self) -> bool {
        true
    }

    fn merge_batch(
        &mut self,
        values: &[ArrayRef],
        group_indices: &[BlocksIndex],
        total_num_groups: usize,
    ) -> Result<()> {
        // update / merge are the same
        self.update_batch(values, group_indices, None, total_num_groups)
    }

    /// Converts an input batch directly to a state batch
    ///
    /// The state is:
    /// - self.prim_fn for all non null, non filtered values
    /// - null otherwise
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
                let (dt, values_buf, original_nulls) = values.into_parts();
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

    fn size(&self) -> usize {
        self.values.allocated_size() + self.null_state.size()
    }
}
