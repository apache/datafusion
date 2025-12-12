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

//! Defines the merge plan for executing partitions in parallel and then merging the results
//! into a single partition

use std::any::Any;
use std::sync::Arc;

use super::metrics::{BaselineMetrics, ExecutionPlanMetricsSet, MetricsSet};
use super::stream::{ObservedStream, RecordBatchReceiverStream};
use super::{
    DisplayAs, ExecutionPlanProperties, PlanProperties, SendableRecordBatchStream,
    Statistics,
};
use crate::execution_plan::{CardinalityEffect, EvaluationType, SchedulingType};
use crate::filter_pushdown::{FilterDescription, FilterPushdownPhase};
use crate::projection::{make_with_child, ProjectionExec};
use crate::{DisplayFormatType, ExecutionPlan, Partitioning};

use datafusion_common::config::ConfigOptions;
use datafusion_common::{assert_eq_or_internal_err, internal_err, Result};
use datafusion_execution::TaskContext;
use datafusion_physical_expr::PhysicalExpr;

/// Merge execution plan executes partitions in parallel and combines them into a single
/// partition. No guarantees are made about the order of the resulting partition.
#[derive(Debug, Clone)]
pub struct CoalescePartitionsExec {
    /// Input execution plan
    input: Arc<dyn ExecutionPlan>,
    /// Execution metrics
    metrics: ExecutionPlanMetricsSet,
    cache: PlanProperties,
    /// Optional number of rows to fetch. Stops producing rows after this fetch
    pub(crate) fetch: Option<usize>,
}

impl CoalescePartitionsExec {
    /// Create a new CoalescePartitionsExec
    pub fn new(input: Arc<dyn ExecutionPlan>) -> Self {
        let cache = Self::compute_properties(&input);
        CoalescePartitionsExec {
            input,
            metrics: ExecutionPlanMetricsSet::new(),
            cache,
            fetch: None,
        }
    }

    /// Update fetch with the argument
    pub fn with_fetch(mut self, fetch: Option<usize>) -> Self {
        self.fetch = fetch;
        self
    }

    /// Input execution plan
    pub fn input(&self) -> &Arc<dyn ExecutionPlan> {
        &self.input
    }

    /// This function creates the cache object that stores the plan properties such as schema, equivalence properties, ordering, partitioning, etc.
    fn compute_properties(input: &Arc<dyn ExecutionPlan>) -> PlanProperties {
        let input_partitions = input.output_partitioning().partition_count();
        let (drive, scheduling) = if input_partitions > 1 {
            (EvaluationType::Eager, SchedulingType::Cooperative)
        } else {
            (
                input.properties().evaluation_type,
                input.properties().scheduling_type,
            )
        };

        // Coalescing partitions loses existing orderings:
        let mut eq_properties = input.equivalence_properties().clone();
        eq_properties.clear_orderings();
        eq_properties.clear_per_partition_constants();
        PlanProperties::new(
            eq_properties,                        // Equivalence Properties
            Partitioning::UnknownPartitioning(1), // Output Partitioning
            input.pipeline_behavior(),
            input.boundedness(),
        )
        .with_evaluation_type(drive)
        .with_scheduling_type(scheduling)
    }
}

impl DisplayAs for CoalescePartitionsExec {
    fn fmt_as(
        &self,
        t: DisplayFormatType,
        f: &mut std::fmt::Formatter,
    ) -> std::fmt::Result {
        match t {
            DisplayFormatType::Default | DisplayFormatType::Verbose => match self.fetch {
                Some(fetch) => {
                    write!(f, "CoalescePartitionsExec: fetch={fetch}")
                }
                None => write!(f, "CoalescePartitionsExec"),
            },
            DisplayFormatType::TreeRender => match self.fetch {
                Some(fetch) => {
                    write!(f, "limit: {fetch}")
                }
                None => write!(f, ""),
            },
        }
    }
}

impl ExecutionPlan for CoalescePartitionsExec {
    fn name(&self) -> &'static str {
        "CoalescePartitionsExec"
    }

    /// Return a reference to Any that can be used for downcasting
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn properties(&self) -> &PlanProperties {
        &self.cache
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.input]
    }

    fn benefits_from_input_partitioning(&self) -> Vec<bool> {
        vec![false]
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let mut plan = CoalescePartitionsExec::new(Arc::clone(&children[0]));
        plan.fetch = self.fetch;
        Ok(Arc::new(plan))
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        // CoalescePartitionsExec produces a single partition
        assert_eq_or_internal_err!(
            partition,
            0,
            "CoalescePartitionsExec invalid partition {partition}"
        );

        let input_partitions = self.input.output_partitioning().partition_count();
        match input_partitions {
            0 => internal_err!(
                "CoalescePartitionsExec requires at least one input partition"
            ),
            1 => {
                // single-partition path: execute child directly, but ensure fetch is respected
                // (wrap with ObservedStream only if fetch is present so we don't add overhead otherwise)
                let child_stream = self.input.execute(0, context)?;
                if self.fetch.is_some() {
                    let baseline_metrics = BaselineMetrics::new(&self.metrics, partition);
                    return Ok(Box::pin(ObservedStream::new(
                        child_stream,
                        baseline_metrics,
                        self.fetch,
                    )));
                }
                Ok(child_stream)
            }
            _ => {
                let baseline_metrics = BaselineMetrics::new(&self.metrics, partition);
                // record the (very) minimal work done so that
                // elapsed_compute is not reported as 0
                let elapsed_compute = baseline_metrics.elapsed_compute().clone();
                let _timer = elapsed_compute.timer();

                // use a stream that allows each sender to put in at
                // least one result in an attempt to maximize
                // parallelism.
                let mut builder =
                    RecordBatchReceiverStream::builder(self.schema(), input_partitions);

                // spawn independent tasks whose resulting streams (of batches)
                // are sent to the channel for consumption.
                for part_i in 0..input_partitions {
                    builder.run_input(
                        Arc::clone(&self.input),
                        part_i,
                        Arc::clone(&context),
                    );
                }

                let stream = builder.build();
                Ok(Box::pin(ObservedStream::new(
                    stream,
                    baseline_metrics,
                    self.fetch,
                )))
            }
        }
    }

    fn metrics(&self) -> Option<MetricsSet> {
        Some(self.metrics.clone_inner())
    }

    fn statistics(&self) -> Result<Statistics> {
        self.partition_statistics(None)
    }

    fn partition_statistics(&self, _partition: Option<usize>) -> Result<Statistics> {
        self.input
            .partition_statistics(None)?
            .with_fetch(self.fetch, 0, 1)
    }

    fn supports_limit_pushdown(&self) -> bool {
        true
    }

    fn cardinality_effect(&self) -> CardinalityEffect {
        CardinalityEffect::Equal
    }

    /// Tries to swap `projection` with its input, which is known to be a
    /// [`CoalescePartitionsExec`]. If possible, performs the swap and returns
    /// [`CoalescePartitionsExec`] as the top plan. Otherwise, returns `None`.
    fn try_swapping_with_projection(
        &self,
        projection: &ProjectionExec,
    ) -> Result<Option<Arc<dyn ExecutionPlan>>> {
        // If the projection does not narrow the schema, we should not try to push it down:
        if projection.expr().len() >= projection.input().schema().fields().len() {
            return Ok(None);
        }
        // CoalescePartitionsExec always has a single child, so zero indexing is safe.
        make_with_child(projection, projection.input().children()[0]).map(|e| {
            if self.fetch.is_some() {
                let mut plan = CoalescePartitionsExec::new(e);
                plan.fetch = self.fetch;
                Some(Arc::new(plan) as _)
            } else {
                Some(Arc::new(CoalescePartitionsExec::new(e)) as _)
            }
        })
    }

    fn fetch(&self) -> Option<usize> {
        self.fetch
    }

    fn with_fetch(&self, limit: Option<usize>) -> Option<Arc<dyn ExecutionPlan>> {
        Some(Arc::new(CoalescePartitionsExec {
            input: Arc::clone(&self.input),
            fetch: limit,
            metrics: self.metrics.clone(),
            cache: self.cache.clone(),
        }))
    }

    fn gather_filters_for_pushdown(
        &self,
        _phase: FilterPushdownPhase,
        parent_filters: Vec<Arc<dyn PhysicalExpr>>,
        _config: &ConfigOptions,
    ) -> Result<FilterDescription> {
        FilterDescription::from_children(parent_filters, &self.children())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test::exec::{
        assert_strong_count_converges_to_zero, BlockingExec, PanicExec,
    };
    use crate::test::{self, assert_is_pending};
    use crate::{collect, common};

    use arrow::datatypes::{DataType, Field, Schema};

    use futures::FutureExt;

    #[tokio::test]
    async fn merge() -> Result<()> {
        let task_ctx = Arc::new(TaskContext::default());

        let num_partitions = 4;
        let csv = test::scan_partitioned(num_partitions);

        // input should have 4 partitions
        assert_eq!(csv.output_partitioning().partition_count(), num_partitions);

        let merge = CoalescePartitionsExec::new(csv);

        // output of CoalescePartitionsExec should have a single partition
        assert_eq!(
            merge.properties().output_partitioning().partition_count(),
            1
        );

        // the result should contain 4 batches (one per input partition)
        let iter = merge.execute(0, task_ctx)?;
        let batches = common::collect(iter).await?;
        assert_eq!(batches.len(), num_partitions);

        // there should be a total of 400 rows (100 per each partition)
        let row_count: usize = batches.iter().map(|batch| batch.num_rows()).sum();
        assert_eq!(row_count, 400);

        Ok(())
    }

    #[tokio::test]
    async fn test_drop_cancel() -> Result<()> {
        let task_ctx = Arc::new(TaskContext::default());
        let schema =
            Arc::new(Schema::new(vec![Field::new("a", DataType::Float32, true)]));

        let blocking_exec = Arc::new(BlockingExec::new(Arc::clone(&schema), 2));
        let refs = blocking_exec.refs();
        let coalesce_partitions_exec =
            Arc::new(CoalescePartitionsExec::new(blocking_exec));

        let fut = collect(coalesce_partitions_exec, task_ctx);
        let mut fut = fut.boxed();

        assert_is_pending(&mut fut);
        drop(fut);
        assert_strong_count_converges_to_zero(refs).await;

        Ok(())
    }

    #[tokio::test]
    #[should_panic(expected = "PanickingStream did panic")]
    async fn test_panic() {
        let task_ctx = Arc::new(TaskContext::default());
        let schema =
            Arc::new(Schema::new(vec![Field::new("a", DataType::Float32, true)]));

        let panicking_exec = Arc::new(PanicExec::new(Arc::clone(&schema), 2));
        let coalesce_partitions_exec =
            Arc::new(CoalescePartitionsExec::new(panicking_exec));

        collect(coalesce_partitions_exec, task_ctx).await.unwrap();
    }

    #[tokio::test]
    async fn test_single_partition_with_fetch() -> Result<()> {
        let task_ctx = Arc::new(TaskContext::default());

        // Use existing scan_partitioned with 1 partition (returns 100 rows per partition)
        let input = test::scan_partitioned(1);

        // Test with fetch=3
        let coalesce = CoalescePartitionsExec::new(input).with_fetch(Some(3));

        let stream = coalesce.execute(0, task_ctx)?;
        let batches = common::collect(stream).await?;

        let row_count: usize = batches.iter().map(|batch| batch.num_rows()).sum();
        assert_eq!(row_count, 3, "Should only return 3 rows due to fetch=3");

        Ok(())
    }

    #[tokio::test]
    async fn test_multi_partition_with_fetch_one() -> Result<()> {
        let task_ctx = Arc::new(TaskContext::default());

        // Create 4 partitions, each with 100 rows
        // This simulates the real-world scenario where each partition has data
        let input = test::scan_partitioned(4);

        // Test with fetch=1 (the original bug: was returning multiple rows instead of 1)
        let coalesce = CoalescePartitionsExec::new(input).with_fetch(Some(1));

        let stream = coalesce.execute(0, task_ctx)?;
        let batches = common::collect(stream).await?;

        let row_count: usize = batches.iter().map(|batch| batch.num_rows()).sum();
        assert_eq!(
            row_count, 1,
            "Should only return 1 row due to fetch=1, not one per partition"
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_single_partition_without_fetch() -> Result<()> {
        let task_ctx = Arc::new(TaskContext::default());

        // Use scan_partitioned with 1 partition
        let input = test::scan_partitioned(1);

        // Test without fetch (should return all rows)
        let coalesce = CoalescePartitionsExec::new(input);

        let stream = coalesce.execute(0, task_ctx)?;
        let batches = common::collect(stream).await?;

        let row_count: usize = batches.iter().map(|batch| batch.num_rows()).sum();
        assert_eq!(
            row_count, 100,
            "Should return all 100 rows when fetch is None"
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_single_partition_fetch_larger_than_batch() -> Result<()> {
        let task_ctx = Arc::new(TaskContext::default());

        // Use scan_partitioned with 1 partition (returns 100 rows)
        let input = test::scan_partitioned(1);

        // Test with fetch larger than available rows
        let coalesce = CoalescePartitionsExec::new(input).with_fetch(Some(200));

        let stream = coalesce.execute(0, task_ctx)?;
        let batches = common::collect(stream).await?;

        let row_count: usize = batches.iter().map(|batch| batch.num_rows()).sum();
        assert_eq!(
            row_count, 100,
            "Should return all available rows (100) when fetch (200) is larger"
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_multi_partition_fetch_exact_match() -> Result<()> {
        let task_ctx = Arc::new(TaskContext::default());

        // Create 4 partitions, each with 100 rows
        let num_partitions = 4;
        let csv = test::scan_partitioned(num_partitions);

        // Test with fetch=400 (exactly all rows)
        let coalesce = CoalescePartitionsExec::new(csv).with_fetch(Some(400));

        let stream = coalesce.execute(0, task_ctx)?;
        let batches = common::collect(stream).await?;

        let row_count: usize = batches.iter().map(|batch| batch.num_rows()).sum();
        assert_eq!(row_count, 400, "Should return exactly 400 rows");

        Ok(())
    }
}
