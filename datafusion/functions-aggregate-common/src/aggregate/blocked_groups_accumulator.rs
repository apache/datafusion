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

use arrow::{
    array::{ArrayRef, AsArray, BooleanArray, PrimitiveArray},
    compute,
    compute::take_arrays,
    datatypes::UInt32Type,
};
use datafusion_common::{Result, ScalarValue, arrow_datafusion_err};
use datafusion_expr_common::accumulator::Accumulator;
use datafusion_expr_common::groups_accumulator::{BlockedEmitTo, BlockedGroupSelection, BlockedGroupsAccumulator, BlocksIndex, EmitTo, GroupSelection, GroupsAccumulator};

/// An adapter that implements [`GroupsAccumulator`] for any [`Accumulator`]
///
/// While [`Accumulator`] are simpler to implement and can support
/// more general calculations (like retractable window functions),
/// they are not as fast as a specialized `GroupsAccumulator`. This
/// interface bridges the gap so the group by operator only operates
/// in terms of [`Accumulator`].
///
/// Internally, this adapter creates a new [`Accumulator`] for each group which
/// stores the state for that group. This both requires an allocation for each
/// Accumulator, internal indices, as well as whatever internal allocations the
/// Accumulator itself requires.
///
/// For example, a `MinAccumulator` that computes the minimum string value with
/// a [`ScalarValue::Utf8`]. That will require at least two allocations per group
/// (one for the `MinAccumulator` and one for the `ScalarValue::Utf8`).
///
/// ```text
///                       ┌─────────────────────────────────┐
///                       │MinAccumulator {                 │
///                ┌─────▶│ min: ScalarValue::Utf8("A")     │───────┐
///                │      │}                                │       │
///                │      └─────────────────────────────────┘       └───────▶   "A"
///    ┌─────┐     │      ┌─────────────────────────────────┐
///    │  0  │─────┘      │MinAccumulator {                 │
///    ├─────┤     ┌─────▶│ min: ScalarValue::Utf8("Z")     │───────────────▶   "Z"
///    │  1  │─────┘      │}                                │
///    └─────┘            └─────────────────────────────────┘                   ...
///      ...                 ...
///    ┌─────┐            ┌────────────────────────────────┐
///    │ N-2 │            │MinAccumulator {                │
///    ├─────┤            │  min: ScalarValue::Utf8("A")   │────────────────▶   "A"
///    │ N-1 │─────┐      │}                               │
///    └─────┘     │      └────────────────────────────────┘
///                │      ┌────────────────────────────────┐        ┌───────▶   "Q"
///                │      │MinAccumulator {                │        │
///                └─────▶│  min: ScalarValue::Utf8("Q")   │────────┘
///                       │}                               │
///                       └────────────────────────────────┘
///
///
///  Logical group         Current Min/Max value for that group stored
///     number             as a ScalarValue which points to an
///                        individually allocated String
/// ```
///
/// # Optimizations
///
/// The adapter minimizes the number of calls to [`Accumulator::update_batch`]
/// by first collecting the input rows for each group into a contiguous array
/// using [`compute::take`]
pub struct BlockedGroupsAccumulatorAdapter {
    inner: Box<dyn GroupsAccumulator + Send>,
    batch_size: usize,
    number_of_groups: usize,
}

impl BlockedGroupsAccumulatorAdapter {
    /// Create a new adapter that will create a new [`Accumulator`]
    /// for each group, using the specified factory function
    pub fn new(inner: Box<dyn GroupsAccumulator + Send>, batch_size: usize) -> Self {
        Self {
            batch_size,
            inner,
            number_of_groups: 0,
        }
    }

    fn update_number_of_groups_after_emit(&mut self, emit_to: BlockedEmitTo) {
        match emit_to {
            BlockedEmitTo::All => {
                self.number_of_groups = 0;
            }
            BlockedEmitTo::NextBlock => {
                self.number_of_groups = self.number_of_groups.saturating_sub(self.batch_size);
            }
            BlockedEmitTo::First(n) => {
                self.number_of_groups = self.number_of_groups.saturating_sub(n);
            }
        }
    }
}

impl BlockedGroupsAccumulator for BlockedGroupsAccumulatorAdapter {
    fn batch_size(&self) -> usize {
        self.batch_size
    }

    fn update_batch(&mut self, values: &[ArrayRef], group_indices: &[BlocksIndex], opt_filter: Option<&BooleanArray>, total_num_groups: usize) -> Result<()> {
        let group_indices_flatten = group_indices.iter().map(|index| index.into_index_in_fixed_block_size(self.batch_size)).collect::<Vec<_>>();
        self.number_of_groups = total_num_groups;
        self.inner.update_batch(values, &group_indices_flatten, opt_filter, total_num_groups)
    }

    fn evaluate(&mut self, emit_to: BlockedEmitTo) -> Result<Vec<ArrayRef>> {
        let iter = emit_to.into_emit_to(self.number_of_groups, self.batch_size)?;

        let mut output = vec![];
        for mapped_emit_to in iter {
            let item = self.inner.evaluate(mapped_emit_to)?;
            output.push(item);
        }

        self.update_number_of_groups_after_emit(emit_to);

        Ok(output)
    }

    fn supports_evaluate_preserving(&self) -> bool {
        self.inner.supports_evaluate_preserving()
    }

    fn evaluate_preserving(&mut self, selection: BlockedGroupSelection<'_>) -> Result<ArrayRef> {
        match selection.indices() {
            None => {
                let selection = GroupSelection::all(selection.total_num_groups());

                self.inner.evaluate_preserving(selection)
            }
            Some(indices) => {
                let indices_flatten = indices.iter().map(|index| index.into_index_in_fixed_block_size(self.batch_size)).collect::<Vec<_>>();

                let selection = GroupSelection::try_from_indices(&indices_flatten, selection.total_num_groups())?;

                self.inner.evaluate_preserving(selection)
            }
        }
    }

    fn state(&mut self, emit_to: BlockedEmitTo) -> Result<Vec<Vec<ArrayRef>>> {
        let iter = emit_to.into_emit_to(self.number_of_groups, self.batch_size)?;

        let mut output = vec![];
        for mapped_emit_to in iter {
            let item = self.inner.state(mapped_emit_to)?;
            output.push(item);
        }

        self.update_number_of_groups_after_emit(emit_to);

        Ok(output)
    }

    fn supports_state_preserving(&self) -> bool {
        self.inner.supports_state_preserving()
    }

    fn state_preserving(&mut self, selection: BlockedGroupSelection<'_>) -> Result<Vec<ArrayRef>> {
        match selection.indices() {
            None => {
                let selection = GroupSelection::all(selection.total_num_groups());

                self.inner.state_preserving(selection)
            }
            Some(indices) => {
                let indices_flatten = indices.iter().map(|index| index.into_index_in_fixed_block_size(self.batch_size)).collect::<Vec<_>>();

                let selection = GroupSelection::try_from_indices(&indices_flatten, selection.total_num_groups())?;

                self.inner.state_preserving(selection)
            }
        }
    }

    fn merge_batch(&mut self, values: &[ArrayRef], group_indices: &[BlocksIndex], total_num_groups: usize) -> Result<()> {
        let group_indices_flatten = group_indices.iter().map(|index| index.into_index_in_fixed_block_size(self.batch_size)).collect::<Vec<_>>();
        self.number_of_groups = total_num_groups;
        self.inner.merge_batch(values, &group_indices_flatten, total_num_groups)
    }

    fn convert_to_state(&self, values: &[ArrayRef], opt_filter: Option<&BooleanArray>) -> Result<Vec<ArrayRef>> {
        self.inner.convert_to_state(values, opt_filter)
    }

    fn size(&self) -> usize {
        self.inner.size()
    }
}
