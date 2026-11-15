// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: See the NOTICE file distributed with this work for additional information regarding copyright ownership
// SPDX-FileContributor: Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements

//! Aggregate table for final aggregation when partial-state input is clustered.
//!
//! See comments in [`super::clustered_partial_table`] for details.

use std::sync::Arc;

use arrow::datatypes::SchemaRef;
use arrow::record_batch::RecordBatch;
use datafusion_common::Result;

use crate::aggregates::aggregate_hash_table::FinalMarker;
use crate::aggregates::order::GroupClusteringMode;
use crate::aggregates::{AggregateExec, AggregateMode, group_values::AccumulatorPhase};

use super::common::HashAggregateAccumulator;
use super::common_clustered::{ClusteredAggregateTable, ClusteredAggregateTableMetrics};

/// Implementation specific to final aggregation, where the table stores partial
/// aggregate states and the input rows are also partial states.
///
/// Example: `AVG(x) GROUP BY k`
///
/// - Aggregate table stores: `k, sum(x), count(x)`
/// - Input rows: `k, sum(x), count(x)`
///
/// See comments at [`ClusteredAggregateTable`] for details.
impl ClusteredAggregateTable<FinalMarker> {
    pub(in crate::aggregates) fn new_with_group_clustering(
        agg: &AggregateExec,
        input_schema: &SchemaRef,
        output_schema: SchemaRef,
        group_clustering_mode: &GroupClusteringMode,
        metrics: ClusteredAggregateTableMetrics,
    ) -> Result<Self> {
        Self::new_for_mode(
            agg,
            input_schema,
            output_schema,
            Arc::clone(input_schema),
            group_clustering_mode,
            &AggregateMode::Final,
            vec![None; agg.aggr_expr().len()],
            metrics,
        )
    }

    /// Merges one partial-state input batch and updates completion state for
    /// any newly observed groups.
    pub(in crate::aggregates) fn aggregate_batch(
        &mut self,
        batch: &RecordBatch,
    ) -> Result<()> {
        let evaluated_batch = self.evaluate_batch(batch)?;
        // `PhysicalGroupBy::as_final()` removes grouping sets while planning
        // final aggregation, so clustered final aggregation sees one grouping.
        debug_assert_eq!(evaluated_batch.grouping_set_args.len(), 1);
        self.aggregate_evaluated_batch(
            &evaluated_batch,
            HashAggregateAccumulator::merge_batch,
            AccumulatorPhase::Merge,
        )
    }

    /// Materializes final results for all completed groups, leaving
    /// the active contiguous-key range in the table.
    ///
    /// Returns None if there are no completed groups.
    pub(in crate::aggregates) fn take_completed_result_batch(
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
            HashAggregateAccumulator::evaluate_to_columns,
            AccumulatorPhase::Evaluate,
        )
        .map(Some)
    }
}
