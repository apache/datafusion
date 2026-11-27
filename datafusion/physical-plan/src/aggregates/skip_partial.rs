// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: See the NOTICE file distributed with this work for additional information regarding copyright ownership
// SPDX-FileContributor: Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements

use arrow::record_batch::RecordBatch;

use crate::metrics;

/// Tracks if the aggregate should skip partial aggregations
///
/// Used by [`crate::aggregates::hash_stream::PartialHashAggregateStream`].
pub(super) struct SkipAggregationProbe {
    // ========================================================================
    // PROPERTIES:
    // These fields are initialized at the start and remain constant throughout
    // the execution.
    // ========================================================================
    /// Aggregation ratio check performed when the number of input rows exceeds
    /// this threshold (from `SessionConfig`)
    probe_rows_threshold: usize,
    /// Maximum ratio of `num_groups` to `input_rows` for continuing aggregation
    /// (from `SessionConfig`). If the ratio exceeds this value, aggregation
    /// is skipped and input rows are directly converted to output
    probe_ratio_threshold: f64,

    // ========================================================================
    // STATES:
    // Fields changes during execution. Can be buffer, or state flags that
    // influence the execution in the parent aggregate stream
    // ========================================================================
    /// Number of processed input rows (updated during probing)
    input_rows: usize,
    /// Number of groups created for `input_rows` (updated during probing).
    /// A key that returns after an early emit is counted again, so this is the
    /// number of rows the stream outputs for `input_rows`.
    num_groups: usize,

    /// Flag indicating further data aggregation may be skipped (decision made
    /// when probing complete)
    should_skip: bool,
    /// Flag indicating further updates of `SkipAggregationProbe` state won't
    /// make any effect (set either while probing or on probing completion)
    is_locked: bool,

    // ========================================================================
    // METRICS:
    // ========================================================================
    /// Number of rows where state was output without aggregation.
    ///
    /// * If 0, all input rows were aggregated (should_skip was always false)
    ///
    /// * if greater than zero, the number of rows which were output directly
    ///   without aggregation
    skipped_aggregation_rows: metrics::Count,
}

impl SkipAggregationProbe {
    pub(super) fn new(
        probe_rows_threshold: usize,
        probe_ratio_threshold: f64,
        skipped_aggregation_rows: metrics::Count,
    ) -> Self {
        Self {
            input_rows: 0,
            num_groups: 0,
            probe_rows_threshold,
            probe_ratio_threshold,
            should_skip: false,
            is_locked: false,
            skipped_aggregation_rows,
        }
    }

    /// Updates `SkipAggregationProbe` state:
    /// - adds the input rows of a batch and the groups it created
    /// - on `probe_rows_threshold` exceeded calculates
    ///   aggregation ratio and sets `should_skip` flag
    /// - if `should_skip` is set, locks further state updates
    pub(super) fn update_state(&mut self, input_rows: usize, new_groups: usize) {
        if self.is_locked {
            return;
        }
        self.input_rows += input_rows;
        self.num_groups += new_groups;
        if self.input_rows >= self.probe_rows_threshold {
            self.should_skip = self.num_groups as f64 / self.input_rows as f64
                > self.probe_ratio_threshold;
            // Set is_locked to true only if we have decided to skip, otherwise we can try to skip
            // during processing the next record_batch.
            self.is_locked = self.should_skip;
        }
    }

    pub(super) fn should_skip(&self) -> bool {
        self.should_skip
    }

    /// Record the number of rows that were output directly without aggregation
    pub(super) fn record_skipped(&mut self, batch: &RecordBatch) {
        self.skipped_aggregation_rows.add(batch.num_rows());
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_skip_aggregation_probe_equality_does_not_skip() {
        // When num_groups / input_rows == probe_ratio_threshold, the `>` boundary
        // means we must NOT skip: equality is not sufficient to trigger skip.
        let threshold_ratio = 0.5_f64;
        let threshold_rows = 10_usize;
        let mut probe = SkipAggregationProbe::new(
            threshold_rows,
            threshold_ratio,
            metrics::Count::new(),
        );

        // 10 rows, 5 groups: ratio = 5/10 = 0.5 exactly equals threshold
        probe.update_state(10, 5);

        assert!(
            !probe.should_skip(),
            "ratio == threshold should not trigger skip (boundary is exclusive)"
        );
    }
}
