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

use arrow::datatypes::SchemaRef;
use arrow::record_batch::RecordBatch;
use datafusion_common::Result;

use crate::aggregates::group_values::AccumulatorPhase;
use crate::aggregates::{AggregateExec, AggregateMode};

use super::common::{
    AggregateHashTable, AggregateHashTableState, FinalMarker, HashAggregateAccumulator,
};

/// Implementation specific to final aggregation, where the table stores partial
/// aggregate states and the input rows are also partial states.
///
/// Example: `AVG(x) GROUP BY k`
///
/// - Aggregate table stores: `k, sum(x), count(x)`
/// - Input rows: `k, sum(x), count(x)`
impl AggregateHashTable<FinalMarker> {
    pub(in crate::aggregates) fn new(
        agg: &AggregateExec,
        partition: usize,
        output_schema: SchemaRef,
        batch_size: usize,
    ) -> Result<Self> {
        Self::new_with_filters(
            agg,
            partition,
            output_schema,
            Arc::clone(&agg.input().schema()),
            batch_size,
            vec![None; agg.aggr_expr().len()],
        )
    }

    /// A table that merges partial state rows of `state_schema` for `agg`,
    /// which does not have to be a final aggregation itself: a single stage
    /// aggregation passes a copy of itself whose `group_by` refers to the
    /// state columns (see `PhysicalGroupBy::as_final`), as it does to replay
    /// its spills.
    /// `borrow_group_values`: the table keeps the data buffers of the batches
    /// it aggregates instead of copying every new group value out of them, see
    /// `new_group_values_with_borrow`. Only for a table that is dropped before
    /// those batches are, which holds for the table of one bucket.
    pub(in crate::aggregates) fn new_over_state(
        agg: &AggregateExec,
        state_schema: &SchemaRef,
        partition: usize,
        output_schema: SchemaRef,
        batch_size: usize,
        borrow_group_values: bool,
    ) -> Result<Self> {
        Self::new_for_input(
            agg,
            Arc::clone(state_schema),
            &AggregateMode::Final,
            partition,
            output_schema,
            Arc::clone(state_schema),
            batch_size,
            vec![None; agg.aggr_expr().len()],
            borrow_group_values,
        )
    }

    /// Emits the next batch of aggregated group keys and final aggregate values.
    ///
    /// The output batch size is determined by `self.batch_size`.
    ///
    /// Returns `Some(batch)` for each emitted batch, `None` when output is
    /// exhausted, and an internal error if polled in the `Building` state.
    pub(in crate::aggregates) fn next_output_batch(
        &mut self,
    ) -> Result<Option<RecordBatch>> {
        self.next_output_batch_inner(
            HashAggregateAccumulator::evaluate_to_columns,
            AccumulatorPhase::Evaluate,
        )
    }

    /// Final aggregation consumes partial aggregate states and merges them into
    /// the table's partial-state accumulators.
    pub(in crate::aggregates) fn aggregate_batch(
        &mut self,
        batch: &RecordBatch,
    ) -> Result<()> {
        self.aggregate_batch_inner(
            batch,
            HashAggregateAccumulator::merge_batch,
            AccumulatorPhase::Merge,
        )
    }

    /// Makes the table reusable through [`Self::restart`].
    pub(in crate::aggregates) fn with_restart(mut self) -> Self {
        self.recycle_buffer = true;
        self
    }

    /// After all output has been taken, goes back to aggregating a new set of
    /// groups with the allocations of the previous one: a fresh table grows
    /// from a few entries, rehashing every group it holds each time it
    /// doubles. Returns false if the table cannot be reused.
    pub(in crate::aggregates) fn restart(&mut self) -> bool {
        match (&self.state, self.recycled_buffer.take()) {
            (AggregateHashTableState::Done, Some(buffer)) => {
                self.state = AggregateHashTableState::Building(buffer);
                true
            }
            (_, buffer) => {
                self.recycled_buffer = buffer;
                false
            }
        }
    }

    pub(in crate::aggregates) fn start_output(&mut self) -> Result<()> {
        self.start_outputting();
        Ok(())
    }
}
