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

//! Aggregate table for final aggregation when partial-state input is ordered.
//!
//! See comments in [`super::ordered_partial_table`] for details.

use std::sync::Arc;

use arrow::datatypes::SchemaRef;
use arrow::record_batch::RecordBatch;
use datafusion_common::Result;

use crate::InputOrderMode;
use crate::aggregates::{AggregateExec, AggregateMode};

use super::common_ordered::{
    OrderedAggregateTable, OrderedFinalMarker, remove_emitted_groups,
};

/// Implementation specific to final aggregation, where the table stores partial
/// aggregate states and the input rows are also partial states.
///
/// Example: `AVG(x) GROUP BY k`
///
/// - Aggregate table stores: `k, sum(x), count(x)`
/// - Input rows: `k, sum(x), count(x)`
impl OrderedAggregateTable<OrderedFinalMarker> {
    pub(in crate::aggregates) fn new_with_input_order(
        agg: &AggregateExec,
        partition: usize,
        input_schema: &SchemaRef,
        output_schema: SchemaRef,
        input_order_mode: &InputOrderMode,
    ) -> Result<Self> {
        Self::new_for_mode(
            agg,
            partition,
            input_schema,
            output_schema,
            input_order_mode,
            &AggregateMode::Final,
            vec![None; agg.aggr_expr.len()],
        )
    }

    /// Merges one partial-state input batch and updates ordering information for
    /// any newly observed groups.
    pub(in crate::aggregates) fn aggregate_batch(
        &mut self,
        batch: &RecordBatch,
    ) -> Result<()> {
        let evaluated_batch = self.evaluate_batch(batch)?;
        // `PhysicalGroupBy::as_final()` ensures it removes grouping set when
        // planning final aggregate, so it's safe to reuse here.
        debug_assert_eq!(evaluated_batch.grouping_set_args.len(), 1);

        for group_values in &evaluated_batch.grouping_set_args {
            let starting_num_groups = self.buffer.group_values.len();
            self.buffer
                .group_values
                .intern(group_values, &mut self.buffer.group_indices)?;
            let total_num_groups = self.buffer.group_values.len();
            if total_num_groups > starting_num_groups {
                self.buffer.group_ordering.new_groups(
                    group_values,
                    &self.buffer.group_indices,
                    total_num_groups,
                )?;
            }

            let timer = self.group_by_metrics.aggregation_time.timer();
            for (acc, values) in self
                .buffer
                .accumulators
                .iter_mut()
                .zip(evaluated_batch.accumulator_args.iter())
            {
                acc.merge_batch(values, &self.buffer.group_indices, total_num_groups)?;
            }
            drop(timer);
        }

        Ok(())
    }

    /// See comments in `ordered_partial_stream::next_output_batch`
    pub(in crate::aggregates) fn next_output_batch(
        &mut self,
    ) -> Result<Option<RecordBatch>> {
        if self.buffer.group_values.is_empty() {
            return Ok(None);
        }

        let Some(emit_to) = self.buffer.group_ordering.emit_to() else {
            return Ok(None);
        };

        let timer = self.group_by_metrics.emitting_time.timer();
        let mut output = self.buffer.group_values.emit(emit_to)?;
        remove_emitted_groups(&mut self.buffer.group_ordering, emit_to);

        for acc in &mut self.buffer.accumulators {
            output.push(acc.evaluate(emit_to)?);
        }
        drop(timer);

        let batch = RecordBatch::try_new(Arc::clone(&self.output_schema), output)?;
        debug_assert!(batch.num_rows() > 0);

        Ok(Some(batch))
    }
}
