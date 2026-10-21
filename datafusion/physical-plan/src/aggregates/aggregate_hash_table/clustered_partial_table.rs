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

//! Aggregate table for partial aggregation when input is clustered by group keys.
//!
//! See the [`super::common_clustered`] comments for the high-level ideas.
//!
//! Ordering can establish either group-clustering mode:
//! - Full: `GROUP BY a, b`, input is `ORDER BY a, b`
//! - Partial: `GROUP BY a, b`, input is `ORDER BY a`
//!
//! When a group key combination is exhausted, this table eagerly flushes the
//! completed groups to improve memory efficiency.
//!
//! The implementation is separated from other aggregate tables because this
//! execution path is likely to be optimized further in the future.

use std::sync::Arc;

use arrow::datatypes::SchemaRef;
use arrow::record_batch::RecordBatch;
use datafusion_common::Result;

use crate::aggregates::{
    AggregateExec, AggregateMode, aggregate_hash_table::PartialMarker,
    group_values::AccumulatorPhase,
};

use super::common::HashAggregateAccumulator;
use super::common_clustered::{ClusteredAggregateTable, ClusteredAggregateTableMetrics};

/// Implementation specific to partial aggregation, where the table stores
/// partial aggregate states and the input rows are raw rows.
///
/// Example: `AVG(x) GROUP BY k`
///
/// - Aggregate table stores: `k, sum(x), count(x)`
/// - Input rows: `k, x`
///
/// See comments at [`ClusteredAggregateTable`] for details.
impl ClusteredAggregateTable<PartialMarker> {
    pub(in crate::aggregates) fn new(
        agg: &AggregateExec,
        partition: usize,
        output_schema: SchemaRef,
    ) -> Result<Self> {
        let input_schema = agg.input().schema();
        let state_schema = Arc::clone(&output_schema);
        let metrics = ClusteredAggregateTableMetrics::new(agg, partition);
        Self::new_for_mode(
            agg,
            &input_schema,
            output_schema,
            state_schema,
            &agg.group_clustering_mode,
            &AggregateMode::Partial,
            agg.filter_expr().to_vec(),
            metrics,
        )
    }

    /// Aggregates one raw input batch and updates completion state for any
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

    /// Materializes all completed groups, leaving
    /// the active contiguous-key range in the table.
    pub(in crate::aggregates) fn take_completed_state_batch(
        &mut self,
    ) -> Result<Option<RecordBatch>> {
        if self.is_empty() {
            return Ok(None);
        }
        let Some(emit_to) = self.group_clustering().emit_to() else {
            return Ok(None);
        };
        self.materialize_groups(
            emit_to,
            HashAggregateAccumulator::state,
            AccumulatorPhase::State,
        )
        .map(Some)
    }
}
