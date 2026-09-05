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

use std::option::IntoIter;
use std::sync::Arc;

use super::accumulate::{BlockedNullState, NullState};
use crate::aggregate::groups_accumulator::nulls::filtered_null_mask;
use arrow::array::{ArrayRef, AsArray, BooleanArray, BooleanBufferBuilder, PrimitiveArray};
use arrow::buffer::BooleanBuffer;
use datafusion_common::{Result, internal_err};
use datafusion_expr_common::blocked_helpers::BlockedBooleanBuilder;
use datafusion_expr_common::groups_accumulator::{BlockedEmitTo, BlockedGroupSelection, BlockedGroupsAccumulator, BlocksIndex, EmitTo, GroupSelection, GroupsAccumulator};

/// An accumulator that implements a single operation over a
/// [`BooleanArray`] where the accumulated state is also boolean (such
/// as [`BitAndAssign`])
///
/// F: The function to apply to two elements. The first argument is
/// the existing value and should be updated with the second value
/// (e.g. [`BitAndAssign`] style).
///
/// [`BitAndAssign`]: std::ops::BitAndAssign
#[derive(Debug)]
pub struct BlockedBooleanGroupsAccumulator<F>
where
    F: Fn(bool, bool) -> bool + Send + Sync + 'static,
{
    /// values per group
    values: BlockedBooleanBuilder<true>,

    /// Track nulls in the input / filters
    null_state: BlockedNullState,

    /// Function that computes the output
    bool_fn: F,

    /// The identity element for the boolean operation.
    /// Any value combined with this returns the original value.
    identity: bool,
}

impl<F> BlockedBooleanGroupsAccumulator<F>
where
    F: Fn(bool, bool) -> bool + Send + Sync + 'static,
{
    pub fn new(bool_fn: F, identity: bool, block_size: usize) -> Self {
        Self {
            values: BlockedBooleanBuilder::new(block_size),
            null_state: BlockedNullState::new(block_size),
            bool_fn,
            identity,
        }
    }

    fn emit_block(&mut self) -> Result<Option<ArrayRef>> {
        let Some(values) = self
          .values
          .take_block() else {
            return Ok(None);
        };

        let nulls = self.null_state.build();
        let values = BooleanArray::new(values, nulls);
        Ok(Some(Arc::new(values)))
    }
}

impl<F> BlockedGroupsAccumulator for BlockedBooleanGroupsAccumulator<F>
where
    F: Fn(bool, bool) -> bool + Send + Sync + 'static,
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
        let values = values[0].as_boolean();

        if self.values.len() < total_num_groups {
            let new_groups = total_num_groups - self.values.len();
            // Fill with the identity element, so that when the first non-null value is encountered,
            // it will combine with the identity and the result will be the first non-null value itself.
            self.values.append_n(new_groups, self.identity);
        }

        // NullState dispatches / handles tracking nulls and groups that saw no values
        self.null_state.accumulate_boolean(
            group_indices,
            values,
            opt_filter,
            total_num_groups,
            |group_index, new_value| {
                let current_value = self.values.get_bit(group_index);
                let value = (self.bool_fn)(current_value, new_value);
                self.values.set_bit(group_index, value);
            },
        );

        Ok(())
    }

    fn evaluate(&mut self, emit_to: BlockedEmitTo) -> Result<Vec<ArrayRef>> {
        match emit_to {
            BlockedEmitTo::All => {
                let mut blocks = vec![];

                while let Some(block) = self.emit_block()? {
                    blocks.push(block);
                }

                Ok(blocks)
            }
            BlockedEmitTo::NextBlock => {
                let block = self.emit_block()?;

                Ok(block.into_iter().collect::<Vec<_>>())
            }
            BlockedEmitTo::First(n) => {
                assert!(n < self.batch_size(), "n ({n}) must be less than block size ({})", self.batch_size());

                let values = self
                  .values
                  .take_n(n, None::<IntoIter<usize>>);

                let nulls = self.null_state.take_n(n);
                let values = BooleanArray::new(values, nulls);
                Ok(vec![Arc::new(values)])
            }
        }
    }

    fn evaluate_preserving(&mut self, selection: BlockedGroupSelection<'_>) -> Result<ArrayRef> {
        selection.validate_num_groups(self.values.len())?;
        let mut values = BooleanBufferBuilder::new(selection.len());
        let block_size = self.values.block_size();
        for index in selection.iter() {
            values.append(self.values[index.into_index_in_fixed_block_size(block_size)]);
        }
        let nulls = self.null_state.build_preserving(selection)?;
        Ok(Arc::new(BooleanArray::new(values.finish(), nulls)))
    }

    fn supports_evaluate_preserving(&self) -> bool {
        true
    }

    fn state(&mut self, emit_to: BlockedEmitTo) -> Result<Vec<Vec<ArrayRef>>> {
        Ok(self.evaluate(emit_to)?.into_iter().map(|col_in_block| vec![col_in_block]).collect::<Vec<_>>())
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

    fn size(&self) -> usize {
        // capacity is in bits, so convert to bytes
        self.values.allocated_size() + self.null_state.size()
    }

    fn convert_to_state(
        &self,
        values: &[ArrayRef],
        opt_filter: Option<&BooleanArray>,
    ) -> Result<Vec<ArrayRef>> {
        let values = values[0].as_boolean().clone();

        let values_null_buffer_filtered = filtered_null_mask(opt_filter, &values);
        let (values_buf, _) = values.into_parts();
        let values_filtered = BooleanArray::new(values_buf, values_null_buffer_filtered);

        Ok(vec![Arc::new(values_filtered)])
    }
}
