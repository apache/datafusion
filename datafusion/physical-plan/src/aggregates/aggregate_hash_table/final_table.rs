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
    AggregateHashTable, AggregateHashTableState, FinalMarker, MaterializedOutput,
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
            AggregateHashTableState::Outputting(mut state) => {
                if state.group_values.is_empty() {
                    return Ok(None);
                }

                let emit_to = EmitTo::All;
                let timer = self.group_by_metrics.emitting_time.timer();
                let mut output = state.group_values.emit(emit_to)?;

                for acc in state.accumulators.iter_mut() {
                    output.push(acc.evaluate(emit_to)?);
                }
                drop(timer);

                let batch = RecordBatch::try_new(output_schema, output)?;
                debug_assert!(batch.num_rows() > 0);

                let mut output = MaterializedOutput::new(batch);
                let batch = output.next_batch(batch_size);
                if output.is_exhausted() {
                    self.state = AggregateHashTableState::Done;
                } else {
                    self.state = AggregateHashTableState::OutputtingMaterialized(output);
                }
                Ok(batch)
            }
            AggregateHashTableState::OutputtingMaterialized(mut output) => {
                let batch = output.next_batch(batch_size);
                if output.is_exhausted() {
                    self.state = AggregateHashTableState::Done;
                } else {
                    self.state = AggregateHashTableState::OutputtingMaterialized(output);
                }
                Ok(batch)
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

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow::array::Int32Array;
    use arrow::datatypes::{DataType, Field, Schema};

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
            vec![Arc::new(Int32Array::from(vec![1, 2, 3, 4, 5]))],
        )?;
        let input = TestMemoryExec::try_new_exec(
            &[vec![batch.clone()]],
            Arc::clone(&schema),
            None,
        )?;
        let group_by = PhysicalGroupBy::new_single(vec![(
            datafusion_physical_expr::expressions::col("group_col", &schema)?,
            "group_col".to_string(),
        )]);
        let aggregate_exec = AggregateExec::try_new(
            AggregateMode::FinalPartitioned,
            group_by,
            vec![],
            vec![],
            input,
            Arc::clone(&schema),
        )?;

        let mut hash_table = AggregateHashTable::<FinalMarker>::new(
            &aggregate_exec,
            0,
            aggregate_exec.schema(),
            2,
        )?;
        hash_table.aggregate_batch(&batch)?;
        hash_table.start_output()?;

        assert!(matches!(
            hash_table.state,
            AggregateHashTableState::Outputting(_)
        ));
        assert_eq!(hash_table.next_output_batch()?.unwrap().num_rows(), 2);
        assert!(matches!(
            hash_table.state,
            AggregateHashTableState::OutputtingMaterialized(_)
        ));
        assert_eq!(hash_table.next_output_batch()?.unwrap().num_rows(), 2);
        assert_eq!(hash_table.next_output_batch()?.unwrap().num_rows(), 1);
        assert!(hash_table.next_output_batch()?.is_none());
        assert!(hash_table.is_done());

        Ok(())
    }
}
