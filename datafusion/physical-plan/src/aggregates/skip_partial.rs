// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: See the NOTICE file distributed with this work for additional information regarding copyright ownership
// SPDX-FileContributor: Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements

use arrow::record_batch::RecordBatch;

use crate::metrics;

// Retry promptly at first, then limit repeated work on ineffective aggregation
// without allowing an indefinitely growing delay before conditions are checked.
const INITIAL_REPROBE_SKIP_MULTIPLIER: usize = 4;
const MAX_REPROBE_SKIP_MULTIPLIER: usize = 16;

/// Tracks if the aggregate should skip partial aggregations
///
/// After an ineffective probe, skip four times the larger of the row threshold
/// and the triggering batch. Consecutive ineffective probes double this interval
/// up to sixteen times; a useful decision resets the multiplier to four.
/// Probe counters include groups recreated after early emission and remain
/// cumulative until a skip interval ends.
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
    state: ProbeState,
    /// Multiplier for the next skip window; reset after a useful probe.
    skip_multiplier: usize,

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

enum ProbeState {
    Probing {
        input_rows: usize,
        num_groups: usize,
    },
    Skipping {
        rows_left: usize,
    },
}

impl SkipAggregationProbe {
    pub(super) fn new(
        probe_rows_threshold: usize,
        probe_ratio_threshold: f64,
        skipped_aggregation_rows: metrics::Count,
    ) -> Self {
        Self {
            probe_rows_threshold,
            probe_ratio_threshold,
            state: ProbeState::Probing {
                input_rows: 0,
                num_groups: 0,
            },
            skip_multiplier: INITIAL_REPROBE_SKIP_MULTIPLIER,
            skipped_aggregation_rows,
        }
    }

    /// Adds a batch's input rows and newly created groups to the active probe.
    pub(super) fn update_state(&mut self, input_rows: usize, new_groups: usize) {
        let ProbeState::Probing {
            input_rows: total_rows,
            num_groups,
        } = &mut self.state
        else {
            return;
        };
        *total_rows += input_rows;
        *num_groups += new_groups;
        if *total_rows > 0 && *total_rows >= self.probe_rows_threshold {
            if *num_groups as f64 / *total_rows as f64 > self.probe_ratio_threshold {
                // A long useful phase must not delay recovery after a later skip.
                // Account for batches larger than the configured probe window.
                self.state = ProbeState::Skipping {
                    rows_left: self
                        .probe_rows_threshold
                        .max(input_rows)
                        .saturating_mul(self.skip_multiplier),
                };
                self.skip_multiplier = self
                    .skip_multiplier
                    .saturating_mul(2)
                    .min(MAX_REPROBE_SKIP_MULTIPLIER);
            } else {
                self.skip_multiplier = INITIAL_REPROBE_SKIP_MULTIPLIER;
            }
        }
    }

    pub(super) fn should_skip(&self) -> bool {
        matches!(self.state, ProbeState::Skipping { .. })
    }

    /// Record the number of rows that were output directly without aggregation
    pub(super) fn record_skipped(&mut self, batch: &RecordBatch) {
        self.skipped_aggregation_rows.add(batch.num_rows());
        // No non-empty probe can satisfy a non-positive ratio threshold.
        if self.probe_ratio_threshold <= 0.0 {
            return;
        }
        let ProbeState::Skipping { rows_left } = &mut self.state else {
            return;
        };
        *rows_left = rows_left.saturating_sub(batch.num_rows());
        if *rows_left == 0 {
            self.state = ProbeState::Probing {
                input_rows: 0,
                num_groups: 0,
            };
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::aggregates::grouped_hash_stream::GroupedHashAggregateStream;
    use crate::aggregates::{AggregateExec, AggregateMode, PhysicalGroupBy};
    use crate::execution_plan::ExecutionPlan;
    use crate::test::TestMemoryExec;

    use std::sync::Arc;

    use arrow::array::Int32Array;
    use arrow::datatypes::{DataType, Field, Schema};
    use datafusion_common::Result;
    use datafusion_execution::TaskContext;
    use datafusion_execution::runtime_env::RuntimeEnvBuilder;
    use datafusion_functions_aggregate::count::count_udaf;
    use datafusion_physical_expr::aggregate::AggregateExprBuilder;
    use datafusion_physical_expr::expressions::col;
    use futures::StreamExt;

    fn skip_rows(probe: &mut SkipAggregationProbe, rows: usize) {
        let batch = RecordBatch::try_new(
            Arc::new(Schema::new(vec![Field::new("key", DataType::Int32, false)])),
            vec![Arc::new(Int32Array::from(vec![0; rows]))],
        )
        .unwrap();
        probe.record_skipped(&batch);
    }

    #[rstest::rstest]
    #[case(0)]
    #[case(8)]
    fn test_reprobe_backoff_is_bounded(#[case] probe_rows_threshold: usize) {
        let mut probe =
            SkipAggregationProbe::new(probe_rows_threshold, 0.8, metrics::Count::new());
        for multiplier in [4, 8, 16, 16, 16] {
            probe.update_state(8, 8);
            assert!(probe.should_skip());
            skip_rows(&mut probe, 8 * multiplier - 1);
            assert!(probe.should_skip());
            skip_rows(&mut probe, 0);
            assert!(probe.should_skip());
            skip_rows(&mut probe, 1);
            assert!(!probe.should_skip());
            probe.update_state(0, 0);
            assert!(!probe.should_skip());
        }
    }

    #[test]
    fn test_reprobe_backoff_resets_after_useful_window() {
        let mut probe = SkipAggregationProbe::new(8, 0.8, metrics::Count::new());
        for multiplier in [4, 8, 16] {
            probe.update_state(8, 8);
            skip_rows(&mut probe, 8 * multiplier);
            assert!(!probe.should_skip());
        }

        // A short useful prefix does not establish that the probe is useful.
        probe.update_state(4, 2);
        probe.update_state(12, 12);
        assert!(probe.should_skip());
        skip_rows(&mut probe, 16 * 12 - 1);
        assert!(probe.should_skip());
        skip_rows(&mut probe, 1);
        assert!(!probe.should_skip());

        probe.update_state(8, 4);
        assert!(!probe.should_skip());
        probe.update_state(32, 32);
        assert!(probe.should_skip());
        skip_rows(&mut probe, 4 * 32 - 1);
        assert!(probe.should_skip());
        skip_rows(&mut probe, 1);
        assert!(!probe.should_skip());
    }

    // Migrated to PartialHashAggregateStream coverage in hash_stream.rs;
    // kept here for the legacy GroupedHashAggregateStream implementation.
    #[tokio::test]
    async fn test_skip_aggregation_probe_not_locked_until_skip() -> Result<()> {
        // Test that the probe is not locked until we actually decide to skip.
        // This allows us to continue evaluating the skip condition across multiple batches.
        //
        // Scenario:
        // - Batch 1: Hits rows threshold but NOT ratio threshold (low cardinality) -> don't skip
        // - Batch 2: Now hits ratio threshold (high cardinality) -> skip
        //
        // Without the fix, the probe would be locked after batch 1, preventing the skip
        // decision from being made on batch 2.

        let schema = Arc::new(Schema::new(vec![
            Field::new("group_col", DataType::Int32, false),
            Field::new("value_col", DataType::Int32, false),
        ]));

        // Configure thresholds:
        // - probe_rows_threshold: 100 rows
        // - probe_ratio_threshold: 0.8 (80%)
        let probe_rows_threshold = 100;
        let probe_ratio_threshold = 0.8;

        // Batch 1: 100 rows with only 10 unique groups
        // Ratio: 10/100 = 0.1 (10%) < 0.8 -> should NOT skip
        // This will hit the rows threshold but not the ratio threshold
        let batch1_rows = 100;
        let batch1_groups = 10;
        let mut group_ids_batch1 = Vec::new();
        for i in 0..batch1_rows {
            group_ids_batch1.push((i % batch1_groups) as i32);
        }
        let values_batch1: Vec<i32> = vec![1; batch1_rows];

        let batch1 = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(Int32Array::from(group_ids_batch1)),
                Arc::new(Int32Array::from(values_batch1)),
            ],
        )?;

        // Batch 2: 360 rows with 360 unique NEW groups (starting from group 10)
        // After batch 2, total: 460 rows, 370 groups
        // Ratio: 370/460 is about 0.804 (80.4%) > 0.8 -> SHOULD decide to skip
        let batch2_rows = 360;
        let batch2_groups = 360;
        let group_ids_batch2: Vec<i32> = (batch1_groups..(batch1_groups + batch2_groups))
            .map(|x| x as i32)
            .collect();
        let values_batch2: Vec<i32> = vec![1; batch2_rows];

        let batch2 = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(Int32Array::from(group_ids_batch2)),
                Arc::new(Int32Array::from(values_batch2)),
            ],
        )?;

        // Batch 3: This batch should be skipped since we decided to skip after batch 2
        // 100 rows with 100 unique groups (continuing from where batch 2 left off)
        let batch3_rows = 100;
        let batch3_groups = 100;
        let batch3_start_group = batch1_groups + batch2_groups;
        let group_ids_batch3: Vec<i32> = (batch3_start_group
            ..(batch3_start_group + batch3_groups))
            .map(|x| x as i32)
            .collect();
        let values_batch3: Vec<i32> = vec![1; batch3_rows];

        let batch3 = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(Int32Array::from(group_ids_batch3)),
                Arc::new(Int32Array::from(values_batch3)),
            ],
        )?;

        let input_partitions = vec![vec![batch1, batch2, batch3]];

        let runtime = RuntimeEnvBuilder::default().build_arc()?;
        let mut task_ctx = TaskContext::default().with_runtime(runtime);

        // Configure skip aggregation settings
        let mut session_config = task_ctx.session_config().clone();
        session_config = session_config.set(
            "datafusion.execution.skip_partial_aggregation_probe_rows_threshold",
            &datafusion_common::ScalarValue::UInt64(Some(probe_rows_threshold)),
        );
        session_config = session_config.set(
            "datafusion.execution.skip_partial_aggregation_probe_ratio_threshold",
            &datafusion_common::ScalarValue::Float64(Some(probe_ratio_threshold)),
        );
        task_ctx = task_ctx.with_session_config(session_config);
        let task_ctx = Arc::new(task_ctx);

        // Create aggregate: COUNT(*) GROUP BY group_col
        let group_expr = vec![(col("group_col", &schema)?, "group_col".to_string())];
        let aggr_expr = vec![Arc::new(
            AggregateExprBuilder::new(count_udaf(), vec![col("value_col", &schema)?])
                .schema(Arc::clone(&schema))
                .alias("count_value")
                .build()?,
        )];

        let exec = TestMemoryExec::try_new(&input_partitions, Arc::clone(&schema), None)?;
        let exec = Arc::new(TestMemoryExec::update_cache(&Arc::new(exec)));

        // Use Partial mode
        let aggregate_exec = AggregateExec::try_new(
            AggregateMode::Partial,
            PhysicalGroupBy::new_single(group_expr),
            aggr_expr,
            vec![None],
            exec,
            Arc::clone(&schema),
        )?;

        // Execute and collect results
        let mut stream =
            GroupedHashAggregateStream::new(&aggregate_exec, &Arc::clone(&task_ctx), 0)?;
        let mut results = Vec::new();

        while let Some(result) = stream.next().await {
            let batch = result?;
            results.push(batch);
        }

        // Check that skip aggregation actually happened.
        // The key metric is skipped_aggregation_rows.
        let metrics = aggregate_exec.metrics().unwrap();
        let skipped_rows = metrics
            .sum_by_name("skipped_aggregation_rows")
            .map(|m| m.as_usize())
            .unwrap_or(0);

        // We expect batch 3's rows to be skipped (100 rows)
        assert_eq!(
            skipped_rows, batch3_rows,
            "Expected batch 3's rows ({batch3_rows}) to be skipped",
        );

        Ok(())
    }

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
