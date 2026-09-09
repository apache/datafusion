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

use std::marker::PhantomData;
use std::sync::Arc;

use arrow::datatypes::SchemaRef;
use arrow::record_batch::RecordBatch;
use datafusion_common::{Result, assert_eq_or_internal_err};

use crate::aggregates::AggregateExec;
use crate::aggregates::group_values::{AccumulatorPhase, new_group_values};
use crate::aggregates::order::GroupOrdering;

use super::common::{
    AggregateHashTable, AggregateHashTableBuffer, AggregateHashTableState,
    HashAggregateAccumulator, PartialMarker, PartialSkipMarker,
};

/// Implementation specific to partial aggregation, where the table stores
/// partial aggregate states and the input rows are raw rows.
///
/// Example: `AVG(x) GROUP BY k`
///
/// - Aggregate table stores: `k, sum(x), count(x)`
/// - Input rows: `k, x`
impl AggregateHashTable<PartialMarker> {
    pub(in crate::aggregates) fn new(
        agg: &AggregateExec,
        partition: usize,
        output_schema: SchemaRef,
        batch_size: usize,
    ) -> Result<Self> {
        Self::new_with_filters(
            agg,
            partition,
            Arc::clone(&output_schema),
            output_schema,
            batch_size,
            agg.filter_expr.iter().cloned().collect(),
        )
    }

    /// Emits the next batch of aggregated group keys and aggregate states.
    ///
    /// The output batch size is determined by `self.batch_size`.
    ///
    /// Returns `Some(batch)` for each emitted batch, `None` when output is
    /// exhausted, and an internal error if polled in the `Building` state.
    pub(in crate::aggregates) fn next_output_batch(
        &mut self,
    ) -> Result<Option<RecordBatch>> {
        self.next_output_batch_inner(
            HashAggregateAccumulator::state,
            AccumulatorPhase::State,
        )
    }

    /// In skip-partial-aggregation optimization, when a decision has been made to skip
    /// partial stage, build a typed hash table only for aggregation state conversion
    /// row-by-row.
    pub(in crate::aggregates) fn partial_skip_table(
        &self,
    ) -> Result<AggregateHashTable<PartialSkipMarker>> {
        let state = self.state.building();
        let group_schema = state.group_by.group_schema(&self.input_schema)?;
        let group_values = new_group_values(group_schema, &GroupOrdering::None)?;
        let accumulators = state
            .accumulators
            .iter()
            .map(HashAggregateAccumulator::empty_like)
            .collect::<Result<Vec<_>>>()?;

        Ok(AggregateHashTable {
            group_by_metrics: self.group_by_metrics.clone(),
            aggregate_argument_metrics: self.aggregate_argument_metrics.clone(),
            aggregate_accumulator_metrics: Arc::clone(
                &self.aggregate_accumulator_metrics,
            ),
            input_schema: Arc::clone(&self.input_schema),
            output_schema: Arc::clone(&self.output_schema),
            state_schema: Arc::clone(&self.state_schema),
            batch_size: self.batch_size,
            state: AggregateHashTableState::Building(AggregateHashTableBuffer {
                group_by: Arc::clone(&state.group_by),
                group_values,
                batch_group_indices: Default::default(),
                accumulators,
            }),
            _mode: PhantomData,
        })
    }

    /// Partial aggregation consumes raw input rows and updates the table's
    /// partial-state accumulators.
    pub(in crate::aggregates) fn aggregate_batch(
        &mut self,
        batch: &RecordBatch,
    ) -> Result<()> {
        self.aggregate_batch_inner(
            batch,
            HashAggregateAccumulator::update_batch,
            AccumulatorPhase::Update,
        )
    }

    pub(in crate::aggregates) fn start_output(&mut self) -> Result<()> {
        self.init_empty_grouping_sets()?;
        self.start_outputting();
        Ok(())
    }
}

impl AggregateHashTable<PartialSkipMarker> {
    pub(in crate::aggregates) fn convert_batch_to_state(
        &mut self,
        batch: &RecordBatch,
    ) -> Result<RecordBatch> {
        let evaluated_batch = self.evaluate_batch(batch)?;

        assert_eq_or_internal_err!(
            evaluated_batch.grouping_set_args.len(),
            1,
            "group_values expected to have single element"
        );
        let mut output = evaluated_batch
            .grouping_set_args
            .into_iter()
            .next()
            .unwrap_or_default();

        let accumulator_metrics = Arc::clone(&self.aggregate_accumulator_metrics);
        let state = self.state.building_mut();
        for (idx, (acc, values)) in state
            .accumulators
            .iter_mut()
            .zip(evaluated_batch.accumulator_args.iter())
            .enumerate()
        {
            output.extend(accumulator_metrics.time(
                idx,
                AccumulatorPhase::ConvertToState,
                || acc.convert_to_state(values),
            )?);
        }

        Ok(RecordBatch::try_new(
            Arc::clone(&self.output_schema),
            output,
        )?)
    }
}
