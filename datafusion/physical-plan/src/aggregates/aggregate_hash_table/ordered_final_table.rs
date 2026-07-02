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

use arrow::datatypes::SchemaRef;
use arrow::record_batch::RecordBatch;
use datafusion_common::Result;

use crate::InputOrderMode;
use crate::aggregates::aggregate_hash_table::FinalMarker;
use crate::aggregates::{AggregateExec, AggregateMode};

use super::common_ordered::OrderedAggregateTable;

/// Implementation specific to final aggregation, where the table stores partial
/// aggregate states and the input rows are also partial states.
///
/// Example: `AVG(x) GROUP BY k`
///
/// - Aggregate table stores: `k, sum(x), count(x)`
/// - Input rows: `k, sum(x), count(x)`
///
/// See comments at [`OrderedAggregateTable`] for details.
impl OrderedAggregateTable<FinalMarker> {
    pub(in crate::aggregates) fn new_with_input_order(
        agg: &AggregateExec,
        partition: usize,
        input_schema: &SchemaRef,
        output_schema: SchemaRef,
        batch_size: usize,
        input_order_mode: &InputOrderMode,
    ) -> Result<Self> {
        Self::new_for_mode(
            agg,
            partition,
            input_schema,
            output_schema,
            batch_size,
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
        // `PhysicalGroupBy::as_final()` removes grouping sets while planning
        // final aggregation, so final ordered aggregation sees one grouping.
        debug_assert_eq!(evaluated_batch.grouping_set_args.len(), 1);
        self.aggregate_evaluated_batch(&evaluated_batch, true)
    }

    /// See comments in `ordered_partial_stream::next_output_batch`
    pub(in crate::aggregates) fn next_output_batch(
        &mut self,
    ) -> Result<Option<RecordBatch>> {
        self.next_output_batch_for_mode(true)
    }
}
