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

use std::collections::HashMap;
use std::marker::PhantomData;
use std::sync::Arc;

use arrow::array::{ArrayRef, BooleanArray, new_null_array};
use arrow::datatypes::SchemaRef;
use arrow::record_batch::RecordBatch;
use datafusion_common::{Result, assert_eq_or_internal_err, internal_err};
use datafusion_expr::EmitTo;

use crate::aggregates::group_values::new_group_values;
use crate::aggregates::order::GroupOrdering;
use crate::aggregates::{AggregateExec, group_id_array, max_duplicate_ordinal};

use super::common::{
    AggregateHashTable, AggregateHashTableBuffer, AggregateHashTableState,
    EvaluatedAccumulatorArgs, HashAggregateAccumulator, MaterializedAggregateOutput,
    PartialMarker, PartialSkipMarker,
};

/// Methods specific to the aggregate hash table used in the partial aggregation stage.
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
        let output_schema = Arc::clone(&self.output_schema);
        let batch_size = self.batch_size;
        // Take ownership of the output state. `emit_next_materialized_batch`
        // restores `self.state` to `OutputtingMaterialized` or `Done`.
        match std::mem::replace(&mut self.state, AggregateHashTableState::Done) {
            AggregateHashTableState::Outputting(state) => {
                if state.group_values.is_empty() {
                    return Ok(None);
                }

                let output = self.materialize_partial_output(state, output_schema)?;
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

    fn materialize_partial_output(
        &self,
        mut state: AggregateHashTableBuffer,
        output_schema: SchemaRef,
    ) -> Result<MaterializedAggregateOutput> {
        let emit_to = EmitTo::All;
        let timer = self.group_by_metrics.emitting_time.timer();
        let mut output = state.group_values.emit(emit_to)?;

        for acc in state.accumulators.iter_mut() {
            output.extend(acc.state(emit_to)?);
        }
        drop(timer);

        let batch = RecordBatch::try_new(output_schema, output)?;
        debug_assert!(batch.num_rows() > 0);
        Ok(MaterializedAggregateOutput::new(batch))
    }

    fn emit_next_materialized_batch(
        &mut self,
        mut output: MaterializedAggregateOutput,
        batch_size: usize,
    ) -> Option<RecordBatch> {
        let batch = output.next_batch(batch_size);
        if output.is_exhausted() {
            self.state = AggregateHashTableState::Done;
        } else {
            self.state = AggregateHashTableState::OutputtingMaterialized(output);
        }
        batch
    }

    pub(in crate::aggregates) fn can_skip_aggregation(&self) -> bool {
        self.state
            .building()
            .accumulators
            .iter()
            .all(|acc| acc.supports_convert_to_state())
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
            input_schema: Arc::clone(&self.input_schema),
            output_schema: Arc::clone(&self.output_schema),
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

    pub(in crate::aggregates) fn aggregate_batch(
        &mut self,
        batch: &RecordBatch,
    ) -> Result<()> {
        let evaluated_batch = self.evaluate_batch(batch)?;
        let state = self.state.building_mut();

        let _timer = self.group_by_metrics.aggregation_time.timer();
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
                acc.update_batch(values, group_indices, total_num_groups)?;
            }
        }

        Ok(())
    }

    pub(in crate::aggregates) fn start_output(&mut self) -> Result<()> {
        self.init_empty_grouping_sets()?;
        self.start_outputting();
        Ok(())
    }

    /// Creates the required empty grouping-set rows when the input is empty.
    ///
    /// For example, this query must still produce one grand-total group even if
    /// `t` has no rows:
    ///
    /// ```sql
    /// SELECT COUNT(v)
    /// FROM t
    /// GROUP BY GROUPING SETS (());
    /// ```
    ///
    /// The synthetic row is filtered out before accumulator update so aggregates
    /// see the same state they would see for an empty input, rather than a real
    /// null-valued row.
    fn init_empty_grouping_sets(&mut self) -> Result<()> {
        let state = self.state.building_mut();
        if !state.group_by.has_grouping_set() || !state.group_values.is_empty() {
            return Ok(());
        }

        let max_ordinal = max_duplicate_ordinal(state.group_by.groups());
        let mut ordinals: HashMap<&[bool], usize> = HashMap::new();
        let group_schema = state.group_by.group_schema(&self.input_schema)?;
        let n_expr = state.group_by.expr().len();
        let mut any_interned = false;

        for group in state.group_by.groups() {
            let ordinal = {
                let entry = ordinals.entry(group.as_slice()).or_insert(0);
                let ordinal = *entry;
                *entry += 1;
                ordinal
            };

            if !group.iter().all(|&is_null| is_null) {
                continue;
            }

            let mut cols: Vec<ArrayRef> = group_schema
                .fields()
                .iter()
                .take(n_expr)
                .map(|field| new_null_array(field.data_type(), 1))
                .collect();
            cols.push(group_id_array(group, ordinal, max_ordinal, 1)?);

            state
                .group_values
                .intern(&cols, &mut state.batch_group_indices)?;
            any_interned = true;
        }

        if any_interned {
            let total_groups = state.group_values.len();
            let false_filter = BooleanArray::from(vec![false]);
            for acc in state.accumulators.iter_mut() {
                let null_args = acc.null_arguments(&self.input_schema)?;
                let values = EvaluatedAccumulatorArgs {
                    arguments: null_args,
                    filter: Some(Arc::new(false_filter.clone())),
                };
                acc.update_batch(&values, &[0], total_groups)?;
            }
        }

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

        let state = self.state.building_mut();
        for (acc, values) in state
            .accumulators
            .iter_mut()
            .zip(evaluated_batch.accumulator_args.iter())
        {
            output.extend(acc.convert_to_state(values)?);
        }

        Ok(RecordBatch::try_new(
            Arc::clone(&self.output_schema),
            output,
        )?)
    }
}
