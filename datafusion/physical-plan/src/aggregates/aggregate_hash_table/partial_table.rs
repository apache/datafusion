// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: See the NOTICE file distributed with this work for additional information regarding copyright ownership
// SPDX-FileContributor: Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements

use std::sync::Arc;

use crate::aggregates::group_values::AccumulatorPhase;
use crate::aggregates::{AggregateExec, evaluate_group_by};
use arrow::datatypes::SchemaRef;
use arrow::record_batch::RecordBatch;
use datafusion_common::{Result, assert_eq_or_internal_err};

use super::common::{AggregateHashTable, HashAggregateAccumulator, PartialMarker};

/// Implementation specific to partial aggregation, where the table stores
/// partial aggregate states and the input rows are raw rows.
///
/// Example: `AVG(x) GROUP BY k`
///
/// - Aggregate table stores: `k, sum(x), count(x)`
/// - Input rows: `k, x`
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
            Arc::clone(&output_schema),
            output_schema,
            batch_size,
            agg.filter_expr().to_vec(),
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
        self.next_output_batch_inner(
            HashAggregateAccumulator::state,
            AccumulatorPhase::State,
        )
    }

    /// Partial aggregation consumes raw input rows and updates the table's
    /// partial-state accumulators.
    pub(in crate::aggregates) fn aggregate_batch(
        &mut self,
        batch: &RecordBatch,
    ) -> Result<()> {
        self.aggregate_batch_inner(
            batch,
            HashAggregateAccumulator::update_batch,
            AccumulatorPhase::Update,
        )
    }

    pub(in crate::aggregates) fn start_output(&mut self) -> Result<()> {
        self.init_empty_grouping_sets()?;
        self.start_outputting();
        Ok(())
    }

    /// Converts raw rows to partial states without inserting them into the table.
    /// All accumulated groups must have been emitted before bypassing aggregation.
    pub(in crate::aggregates) fn convert_batch_to_state(
        &mut self,
        batch: &RecordBatch,
    ) -> Result<RecordBatch> {
        let state = self.state.building_mut();
        debug_assert!(state.group_values.is_empty());
        let grouping_set_args = self
            .group_by_metrics
            .time_group_key_preparation(|| evaluate_group_by(&state.group_by, batch))?;

        assert_eq_or_internal_err!(
            grouping_set_args.len(),
            1,
            "group_values expected to have single element"
        );
        let mut output = grouping_set_args.into_iter().next().unwrap_or_default();

        let accumulator_metrics = Arc::clone(&self.aggregate_accumulator_metrics);
        for (idx, acc) in state.accumulators.iter_mut().enumerate() {
            let values = self.group_by_metrics.time_aggregate_arguments(|| {
                self.aggregate_argument_metrics
                    .time(idx, || acc.evaluate_row_aligned_args(batch))
            })?;

            output.extend(accumulator_metrics.time(
                idx,
                AccumulatorPhase::ConvertToState,
                || acc.convert_to_state(&values),
            )?);
        }

        Ok(RecordBatch::try_new(
            Arc::clone(&self.output_schema),
            output,
        )?)
    }
}
