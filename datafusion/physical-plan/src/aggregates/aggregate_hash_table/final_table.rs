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

use crate::aggregates::AggregateExec;

use super::common::{
    AggregateHashTable, AggregateHashTableState, FinalMarker, emit_to_for_batch_size,
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
        match &mut self.state {
            AggregateHashTableState::Outputting(state) => {
                if state.group_values.is_empty() {
                    self.state = AggregateHashTableState::Done;
                    return Ok(None);
                }

                let emit_to =
                    emit_to_for_batch_size(batch_size, state.group_values.len());
                let timer = self.group_by_metrics.emitting_time.timer();
                let mut output = state.group_values.emit(emit_to)?;

                for acc in state.accumulators.iter_mut() {
                    output.push(acc.evaluate(emit_to)?);
                }
                let done = state.group_values.is_empty();
                drop(timer);

                let batch = RecordBatch::try_new(output_schema, output)?;
                debug_assert!(batch.num_rows() > 0);
                if done {
                    self.state = AggregateHashTableState::Done;
                }
                Ok(Some(batch))
            }
            AggregateHashTableState::Done => Ok(None),
            AggregateHashTableState::Building(_) => {
                internal_err!("next_output_batch must be called in the outputting state")
            }
        }
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

    pub(in crate::aggregates) fn start_output(&mut self) -> Result<()> {
        self.start_outputting();
        Ok(())
    }
}
