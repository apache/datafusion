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

use super::common::{
    AggregateHashTable, AggregateHashTableBuffer, AggregateHashTableState, FinalMarker,
    MaterializedFinalOutput,
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
        match std::mem::replace(&mut self.state, AggregateHashTableState::Done) {
            AggregateHashTableState::Outputting(state) => {
                if state.group_values.is_empty() {
                    return Ok(None);
                }

                let output = self.materialize_final_output(state, output_schema)?;
                Ok(self.emit_next_materialized_batch(output, batch_size))
            }
            AggregateHashTableState::OutputtingMaterializedFinal(output) => {
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
    ) -> Result<MaterializedFinalOutput> {
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
        Ok(MaterializedFinalOutput::new(batch))
    }

    fn emit_next_materialized_batch(
        &mut self,
        mut output: MaterializedFinalOutput,
        batch_size: usize,
    ) -> Option<RecordBatch> {
        let batch = output.next_batch(batch_size);
        if output.is_exhausted() {
            self.state = AggregateHashTableState::Done;
        } else {
            self.state = AggregateHashTableState::OutputtingMaterializedFinal(output);
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

    pub(in crate::aggregates) fn start_output(&mut self) -> Result<()> {
        self.start_outputting();
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow::array::{Int32Array, Int64Array};
    use arrow::datatypes::{DataType, Field, Schema};
    use datafusion_functions_aggregate::count::count_udaf;
    use datafusion_physical_expr::aggregate::AggregateExprBuilder;
    use datafusion_physical_expr::expressions::col;

    use crate::aggregates::aggregate_hash_table::common::PartialMarker;
    use crate::aggregates::{AggregateMode, PhysicalGroupBy};
    use crate::execution_plan::ExecutionPlan;
    use crate::test::TestMemoryExec;

    use super::*;

    #[test]
    fn final_output_materializes_once_then_slices() -> Result<()> {
        let schema = Arc::new(Schema::new(vec![Field::new(
            "group_col",
            DataType::Int32,
            false,
        )]));
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![Arc::new(Int32Array::from(vec![1, 1, 2, 3, 3, 3]))],
        )?;
        let group_by = PhysicalGroupBy::new_single(vec![(
            col("group_col", &schema)?,
            "group_col".to_string(),
        )]);
        let aggregate = Arc::new(
            AggregateExprBuilder::new(count_udaf(), vec![col("group_col", &schema)?])
                .schema(Arc::clone(&schema))
                .alias("COUNT(group_col)")
                .build()?,
        );

        let partial_input = TestMemoryExec::try_new_exec(
            &[vec![batch.clone()]],
            Arc::clone(&schema),
            None,
        )?;
        let partial_exec = AggregateExec::try_new(
            AggregateMode::Partial,
            group_by.clone(),
            vec![Arc::clone(&aggregate)],
            vec![None],
            partial_input,
            Arc::clone(&schema),
        )?;
        let mut partial_hash_table = AggregateHashTable::<PartialMarker>::new(
            &partial_exec,
            0,
            partial_exec.schema(),
            1024,
        )?;
        partial_hash_table.aggregate_batch(&batch)?;
        partial_hash_table.start_output()?;
        let partial_batch = partial_hash_table.next_output_batch()?.unwrap();
        assert!(partial_hash_table.next_output_batch()?.is_none());

        let final_input = TestMemoryExec::try_new_exec(
            &[vec![partial_batch.clone()]],
            partial_exec.schema(),
            None,
        )?;
        let aggregate_exec = AggregateExec::try_new(
            AggregateMode::FinalPartitioned,
            group_by.as_final(),
            vec![aggregate],
            vec![None],
            final_input,
            partial_exec.schema(),
        )?;

        let mut hash_table = AggregateHashTable::<FinalMarker>::new(
            &aggregate_exec,
            0,
            aggregate_exec.schema(),
            2,
        )?;
        hash_table.aggregate_batch(&partial_batch)?;
        hash_table.start_output()?;

        assert!(matches!(
            hash_table.state,
            AggregateHashTableState::Outputting(_)
        ));
        let batch = hash_table.next_output_batch()?.unwrap();
        assert_eq!(batch.num_rows(), 2);
        assert!(matches!(
            hash_table.state,
            AggregateHashTableState::OutputtingMaterializedFinal(_)
        ));
        let mut output = group_counts(&batch);

        let batch = hash_table.next_output_batch()?.unwrap();
        assert_eq!(batch.num_rows(), 1);
        output.extend(group_counts(&batch));
        output.sort();
        assert_eq!(output, vec![(1, 2), (2, 1), (3, 3)]);

        assert!(hash_table.next_output_batch()?.is_none());
        assert!(hash_table.is_done());

        Ok(())
    }

    fn group_counts(batch: &RecordBatch) -> Vec<(i32, i64)> {
        let groups = batch
            .column(0)
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        let counts = batch
            .column(1)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();

        (0..batch.num_rows())
            .map(|idx| (groups.value(idx), counts.value(idx)))
            .collect()
    }
}
