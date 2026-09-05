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

//! Aggregate table for single aggregation when raw input is ordered.
//!
//! See comments in [`super::ordered_partial_table`] for details.

use arrow::datatypes::SchemaRef;
use arrow::record_batch::RecordBatch;
use datafusion_common::Result;

use crate::aggregates::aggregate_hash_table::SingleMarker;
use crate::aggregates::{AggregateExec, AggregateMode, group_values::AccumulatorPhase};

use super::common::HashAggregateAccumulator;
use super::common_ordered::{OrderedAggregateTable, OrderedAggregateTableMetrics};

/// Implementation specific to single aggregation, where the table stores final
/// aggregate values and the input rows are raw rows.
///
/// Example: `AVG(x) GROUP BY k`
///
/// - Aggregate table stores: `k, avg(x)`
/// - Input rows: `k, x`
///
/// See comments at [`OrderedAggregateTable`] for details.
impl OrderedAggregateTable<SingleMarker> {
    pub(in crate::aggregates) fn new(
        agg: &AggregateExec,
        partition: usize,
        output_schema: SchemaRef,
        state_schema: SchemaRef,
        batch_size: usize,
    ) -> Result<Self> {
        debug_assert!(matches!(
            agg.mode,
            AggregateMode::Single | AggregateMode::SinglePartitioned
        ));

        let input_schema = agg.input().schema();
        let metrics = OrderedAggregateTableMetrics::new(agg, partition);
        Self::new_for_mode(
            agg,
            &input_schema,
            output_schema,
            state_schema,
            batch_size,
            &agg.group_completion_mode,
            &agg.mode,
            agg.filter_expr.iter().cloned().collect(),
            metrics,
        )
    }

    /// Aggregates one raw input batch and updates ordering information for any
    /// newly observed groups.
    pub(in crate::aggregates) fn aggregate_batch(
        &mut self,
        batch: &RecordBatch,
    ) -> Result<()> {
        let evaluated_batch = self.evaluate_batch(batch)?;
        self.aggregate_evaluated_batch(
            &evaluated_batch,
            HashAggregateAccumulator::update_batch,
            AccumulatorPhase::Update,
        )
    }

    /// Emits the next batch of final aggregate values for groups proven complete
    /// by the input ordering.
    pub(in crate::aggregates) fn next_output_batch(
        &mut self,
    ) -> Result<Option<RecordBatch>> {
        self.next_output_batch_inner(
            HashAggregateAccumulator::evaluate_to_columns,
            AccumulatorPhase::Evaluate,
        )
    }
}
