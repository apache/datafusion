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
use datafusion_common::{Result, internal_err};
use datafusion_expr::EmitTo;

use crate::aggregates::AggregateExec;
use crate::aggregates::group_values::AccumulatorPhase;

use super::common::{
    AggregateHashTable, AggregateHashTableBuffer, AggregateHashTableState, FinalMarker,
    HashAggregateAccumulator, MaterializedAggregateOutput,
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
        let emit_to = EmitTo::All;
        let accumulator_metrics = Arc::clone(&self.aggregate_accumulator_metrics);
        let output = self.group_by_metrics.time_emitting(|| {
            let mut output = state.group_values.emit(emit_to)?;
            for (idx, accumulator) in state.accumulators.iter_mut().enumerate() {
                output.extend(accumulator_metrics.time(
                    idx,
                    AccumulatorPhase::Evaluate,
                    || accumulator.evaluate_to_columns(emit_to),
                )?);
            }
            Ok::<_, datafusion_common::DataFusionError>(output)
        })?;
        let batch = RecordBatch::try_new(output_schema, output)?;
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

    pub(in crate::aggregates) fn start_output(&mut self) -> Result<()> {
        self.start_outputting();
        Ok(())
    }
}
