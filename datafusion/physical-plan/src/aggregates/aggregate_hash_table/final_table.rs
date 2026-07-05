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
use datafusion_common::{Result, assert_eq_or_internal_err, internal_err};
use datafusion_expr::EmitTo;

use crate::aggregates::AggregateExec;

use super::common::{
    AggregateHashTable, AggregateHashTableBuffer, AggregateHashTableState,
    EvaluatedAggregateBatch, FinalMarker, MaterializedAggregateOutput,
};

/// Methods specific to the aggregate hash table used in the final aggregation stage.
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
            batch_size,
            vec![None; agg.aggr_expr.len()],
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
        let output_schema = Arc::clone(&self.output_schema);
        let batch_size = self.batch_size;
        // Take ownership of the output state. `emit_next_materialized_batch`
        // restores `self.state` to `OutputtingMaterialized` or `Done`.
        match std::mem::replace(&mut self.state, AggregateHashTableState::Done) {
            AggregateHashTableState::Outputting(state) => {
                if state.group_values.is_empty() {
                    return Ok(None);
                }

                let output = self.materialize_final_output(state, output_schema)?;
                Ok(self.emit_next_materialized_batch(output, batch_size))
            }
            AggregateHashTableState::OutputtingMaterialized(output) => {
                Ok(self.emit_next_materialized_batch(output, batch_size))
            }
            AggregateHashTableState::Done => Ok(None),
            AggregateHashTableState::Building(_) => {
                internal_err!("next_output_batch must be called in the outputting state")
            }
        }
    }

    fn materialize_final_output(
        &self,
        mut state: AggregateHashTableBuffer,
        output_schema: SchemaRef,
    ) -> Result<MaterializedAggregateOutput> {
        // Final aggregate evaluation consumes accumulator state. Evaluate all
        // groups once, then slice the materialized batch on subsequent polls.
        let emit_to = EmitTo::All;
        let timer = self.group_by_metrics.emitting_time.timer();
        let mut output = state.group_values.emit(emit_to)?;

        for acc in state.accumulators.iter_mut() {
            output.push(acc.evaluate(emit_to)?);
        }
        drop(timer);

        let batch = RecordBatch::try_new(output_schema, output)?;
        debug_assert!(batch.num_rows() > 0);

        let num_rows = batch.num_rows();
        state.group_values.clear_shrink(num_rows);
        state.batch_group_indices.clear();
        state.batch_group_indices.shrink_to(num_rows);

        Ok(MaterializedAggregateOutput::new_with_reusable_buffer(
            batch, state,
        ))
    }

    fn emit_next_materialized_batch(
        &mut self,
        mut output: MaterializedAggregateOutput,
        batch_size: usize,
    ) -> Option<RecordBatch> {
        let batch = output.next_batch(batch_size);
        if output.is_exhausted() {
            self.state = output
                .take_reusable_buffer()
                .map(AggregateHashTableState::Building)
                .unwrap_or(AggregateHashTableState::Done);
        } else {
            self.state = AggregateHashTableState::OutputtingMaterialized(output);
        }
        batch
    }

    pub(in crate::aggregates) fn aggregate_batch(
        &mut self,
        batch: &RecordBatch,
    ) -> Result<()> {
        let evaluated_batch = self.evaluate_batch(batch)?;
        let state = self.state.building_mut();

        let timer = self.group_by_metrics.aggregation_time.timer();
        for group_values in &evaluated_batch.grouping_set_args {
            state
                .group_values
                .intern(group_values, &mut state.batch_group_indices)?;
            let group_indices = &state.batch_group_indices;
            let total_num_groups = state.group_values.len();

            for (acc, values) in state
                .accumulators
                .iter_mut()
                .zip(evaluated_batch.accumulator_args.iter())
            {
                acc.merge_batch(values, group_indices, total_num_groups)?;
            }
        }
        drop(timer);

        Ok(())
    }

    pub(in crate::aggregates) fn create_group_hashes_indices(
        &self,
        evaluated_batch: &EvaluatedAggregateBatch,
        partition_indices: &[usize],
        hashes: &mut Vec<u64>,
    ) -> Result<()> {
        assert_eq_or_internal_err!(
            evaluated_batch.grouping_set_args.len(),
            1,
            "final partition replay expects a single grouping set"
        );

        let state = self.state.building();
        let group_values = &evaluated_batch.grouping_set_args[0];
        let timer = self.group_by_metrics.time_calculating_group_ids.timer();
        state.group_values.create_hashes_indices(
            group_values,
            partition_indices,
            hashes,
        )?;
        drop(timer);
        Ok(())
    }

    pub(in crate::aggregates) fn aggregate_partitioned_evaluated_batch(
        &mut self,
        evaluated_batch: &EvaluatedAggregateBatch,
        group_hashes: &[u64],
        partition_indices: &[usize],
    ) -> Result<()> {
        assert_eq_or_internal_err!(
            evaluated_batch.grouping_set_args.len(),
            1,
            "final partition replay expects a single grouping set"
        );

        let state = self.state.building_mut();
        let group_values = &evaluated_batch.grouping_set_args[0];

        let timer = self.group_by_metrics.aggregation_time.timer();
        state.group_values.intern_partitioned(
            group_values,
            group_hashes,
            &mut state.batch_group_indices,
            partition_indices,
        )?;
        let group_indices = &state.batch_group_indices;
        let total_num_groups = state.group_values.len();

        for (acc, values) in state
            .accumulators
            .iter_mut()
            .zip(evaluated_batch.accumulator_args.iter())
        {
            acc.merge_partitioned_batch(
                values,
                group_indices,
                partition_indices,
                total_num_groups,
            )?;
        }
        drop(timer);

        Ok(())
    }

    pub(in crate::aggregates) fn supports_partitioned_merge(&self) -> bool {
        let state = self.state.building();
        state.group_values.supports_intern_partitioned()
            && state
                .accumulators
                .iter()
                .all(|acc| acc.supports_merge_partitioned_batch())
    }
    pub(in crate::aggregates) fn start_output(&mut self) -> Result<()> {
        self.start_outputting();
        Ok(())
    }
}
