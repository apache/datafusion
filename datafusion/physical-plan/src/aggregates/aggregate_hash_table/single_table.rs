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

use arrow::datatypes::SchemaRef;
use arrow::record_batch::RecordBatch;
use datafusion_common::Result;

use crate::aggregates::AggregateExec;

use super::common::{AggregateHashTable, HashAggregateAccumulator, SingleMarker};

/// Implementation specific to single aggregation, where the table stores final
/// aggregate values and the input rows are raw rows.
///
/// Example: `AVG(x) GROUP BY k`
///
/// - Aggregate table stores: `k, avg(x)`
/// - Input rows: `k, x`
impl AggregateHashTable<SingleMarker> {
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

    /// Emits the next batch of aggregated group keys and final aggregate values.
    ///
    /// The output batch size is determined by `self.batch_size`.
    ///
    /// Returns `Some(batch)` for each emitted batch, `None` when output is
    /// exhausted, and an internal error if polled in the `Building` state.
    pub(in crate::aggregates) fn next_output_batch(
        &mut self,
    ) -> Result<Option<RecordBatch>> {
        self.next_output_batch_inner(HashAggregateAccumulator::evaluate_to_columns)
    }

    /// Single aggregation consumes raw input rows and updates the table's
    /// final-value accumulators.
    pub(in crate::aggregates) fn aggregate_batch(
        &mut self,
        batch: &RecordBatch,
    ) -> Result<()> {
        self.aggregate_batch_inner(batch, HashAggregateAccumulator::update_batch)
    }

    pub(in crate::aggregates) fn start_output(&mut self) -> Result<()> {
        self.start_outputting();
        Ok(())
    }
}
