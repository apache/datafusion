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

use std::fmt::Debug;
use std::mem::size_of;
use std::sync::Arc;

use arrow::array::{ArrayRef, AsArray, BooleanArray, PrimitiveArray};
use arrow::buffer::NullBuffer;
use arrow::compute;
use arrow::datatypes::ArrowPrimitiveType;
use arrow::datatypes::DataType;
use datafusion_common::{DataFusionError, Result, internal_datafusion_err};
use datafusion_expr_common::groups_accumulator::{EmitTo, GroupsAccumulator};

use crate::aggregate::groups_accumulator::accumulate::{
    BlockedNullState, FlatNullState, NullState, SeenValueStore,
};
use crate::aggregate::groups_accumulator::block_store::{
    BlockStore, FlatBlockStore, VecValues, VecValuesBlockStore,
};
use crate::aggregate::groups_accumulator::blocks::Blocks;
use crate::aggregate::groups_accumulator::group_index_operations::{
    BlockedGroupIndexOperations, FlatGroupIndexOperations, GroupIndexOperations,
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
    F: Fn(&mut T::Native, T::Native) + Send + Sync + 'static,
{
    /// Values and null state per group, stored according to the current group mode.
    state: PrimitiveGroupsState<T::Native>,

    /// The output type (needed for Decimal precision and scale)
    data_type: DataType,

    /// The starting value for new groups
    starting_value: T::Native,

    /// Function that computes the primitive result
    prim_fn: F,
}

#[derive(Debug)]
enum PrimitiveGroupsState<V: Clone + Debug> {
    Flat {
        values: FlatBlockStore<VecValues<V>>,
        null_state: FlatNullState,
    },
    Blocked {
        values: Blocks<VecValues<V>>,
        null_state: BlockedNullState,
    },
}

impl<T, F> PrimitiveGroupsAccumulator<T, F>
where
    T: ArrowPrimitiveType + Send,
    F: Fn(&mut T::Native, T::Native) + Send + Sync + 'static,
{
    pub fn new(data_type: &DataType, prim_fn: F) -> Self {
        Self {
            state: PrimitiveGroupsState::Flat {
                values: FlatBlockStore::new(),
                null_state: FlatNullState::new(None),
            },
            data_type: data_type.clone(),
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

#[derive(Clone, Copy)]
struct UpdateBatchInput<'a, T: ArrowPrimitiveType> {
    values: &'a PrimitiveArray<T>,
    group_indices: &'a [usize],
    opt_filter: Option<&'a BooleanArray>,
    total_num_groups: usize,
}

fn update_batch_for<T, F, O, V, N>(
    values_store: &mut V,
    null_state: &mut NullState<O, N>,
    input: &UpdateBatchInput<'_, T>,
    starting_value: T::Native,
    prim_fn: &F,
) where
    T: ArrowPrimitiveType + Send,
    T::Native: Debug,
    F: Fn(&mut T::Native, T::Native) + Send + Sync + 'static,
    O: GroupIndexOperations,
    V: BlockStore<VecValues<T::Native>> + Send,
    N: SeenValueStore + Send,
{
    // Expand to ensure values are large enough
    let new_block = |block_size: Option<usize>| {
        // In blocked mode, pre-allocate the full block capacity.
        // In flat mode (block_size=None), start with an empty Vec
        // and let `resize` grow it to exactly `total_num_groups`,
        // matching the standard Vec growth behavior.
        match block_size {
            Some(cap) => VecValues::with_capacity(cap),
            None => VecValues::default(),
        }
    };
    values_store.resize(input.total_num_groups, new_block, starting_value);

    null_state.accumulate(
        input.group_indices,
        input.values,
        input.opt_filter,
        input.total_num_groups,
        |block_id, block_offset, new_value| {
            // SAFETY: `block_id` and `block_offset` are guaranteed to be in bounds
            let value = unsafe {
                values_store[block_id as usize].get_unchecked_mut(block_offset as usize)
            };
            prim_fn(value, new_value);
        },
    );
}

fn values_size<T: Clone + Debug, V: BlockStore<VecValues<T>>>(values: &V) -> usize {
    if values.is_empty() {
        return 0;
    }
    values.num_blocks() * values[0].capacity() * size_of::<T>()
}

impl<T, F> GroupsAccumulator for PrimitiveGroupsAccumulator<T, F>
where
    T: ArrowPrimitiveType + Send,
    T::Native: Debug,
    F: Fn(&mut T::Native, T::Native) + Send + Sync + 'static,
{
    fn update_batch(
        &mut self,
        values: &[ArrayRef],
        group_indices: &[usize],
        opt_filter: Option<&BooleanArray>,
        total_num_groups: usize,
    ) -> Result<()> {
        assert_eq!(values.len(), 1, "single argument to update_batch");
        let input_values = values[0].as_primitive::<T>();

        match &mut self.state {
            PrimitiveGroupsState::Flat { values, null_state } => {
                update_batch_for::<T, F, FlatGroupIndexOperations, _, _>(
                    values,
                    null_state,
                    &UpdateBatchInput {
                        values: input_values,
                        group_indices,
                        opt_filter,
                        total_num_groups,
                    },
                    self.starting_value,
                    &self.prim_fn,
                );
            }
            PrimitiveGroupsState::Blocked { values, null_state } => {
                update_batch_for::<T, F, BlockedGroupIndexOperations, _, _>(
                    values,
                    null_state,
                    &UpdateBatchInput {
                        values: input_values,
                        group_indices,
                        opt_filter,
                        total_num_groups,
                    },
                    self.starting_value,
                    &self.prim_fn,
                );
            }
        }

        Ok(())
    }

    fn evaluate(&mut self, emit_to: EmitTo) -> Result<ArrayRef> {
        let (values, nulls): (Vec<T::Native>, _) = match &mut self.state {
            PrimitiveGroupsState::Flat { values, null_state } => (
                VecValuesBlockStore::emit(values, emit_to)?,
                null_state.build(emit_to),
            ),
            PrimitiveGroupsState::Blocked { values, null_state } => (
                VecValuesBlockStore::emit(values, emit_to)?,
                null_state.build(emit_to),
            ),
        };
        let values = PrimitiveArray::<T>::new(values.into(), nulls) // no copy
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

    fn supports_convert_to_state(&self) -> bool {
        true
    }

    fn size(&self) -> usize {
        match &self.state {
            PrimitiveGroupsState::Flat { values, null_state } => {
                if values.is_empty() {
                    return 0;
                }
                values_size::<T::Native, _>(values) + null_state.size()
            }
            PrimitiveGroupsState::Blocked { values, null_state } => {
                if values.is_empty() {
                    return 0;
                }
                values_size::<T::Native, _>(values) + null_state.size()
            }
        }
    }

    fn supports_blocked_groups(&self) -> bool {
        true
    }

    fn alter_block_size(&mut self, block_size: Option<usize>) -> Result<()> {
        self.state = if let Some(block_size) = block_size {
            PrimitiveGroupsState::Blocked {
                values: Blocks::new(Some(block_size)),
                null_state: BlockedNullState::new(Some(block_size)),
            }
        } else {
            PrimitiveGroupsState::Flat {
                values: FlatBlockStore::new(),
                null_state: FlatNullState::new(None),
            }
        };

        Ok(())
    }
}
