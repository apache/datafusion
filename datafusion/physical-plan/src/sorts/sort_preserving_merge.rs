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

//! [`SortPreservingMergeExec`] merges multiple sorted streams into one sorted stream.

use std::any::Any;
use std::sync::Arc;

use crate::common::spawn_buffered;
use crate::limit::LimitStream;
use crate::metrics::{BaselineMetrics, ExecutionPlanMetricsSet, MetricsSet};
use crate::projection::{make_with_child, update_ordering, ProjectionExec};
use crate::sorts::streaming_merge::StreamingMergeBuilder;
use crate::{
    DisplayAs, DisplayFormatType, Distribution, ExecutionPlan, ExecutionPlanProperties,
    Partitioning, PlanProperties, SendableRecordBatchStream, Statistics,
};

use datafusion_common::{assert_eq_or_internal_err, internal_err, Result};
use datafusion_execution::memory_pool::MemoryConsumer;
use datafusion_execution::TaskContext;
use datafusion_physical_expr_common::sort_expr::{LexOrdering, OrderingRequirements};

use crate::execution_plan::{EvaluationType, SchedulingType};
use log::{debug, trace};

/// Sort preserving merge execution plan
///
/// # Overview
///
/// This operator implements a K-way merge. It is used to merge multiple sorted
/// streams into a single sorted stream and is highly optimized.
///
/// ## Inputs:
///
/// 1. A list of sort expressions
/// 2. An input plan, where each partition is sorted with respect to
///    these sort expressions.
///
/// ## Output:
///
/// 1. A single partition that is also sorted with respect to the expressions
///
/// ## Diagram
///
/// ```text
/// ┌─────────────────────────┐
/// │ ┌───┬───┬───┬───┐       │
/// │ │ A │ B │ C │ D │ ...   │──┐
/// │ └───┴───┴───┴───┘       │  │
/// └─────────────────────────┘  │  ┌───────────────────┐    ┌───────────────────────────────┐
///   Stream 1                   │  │                   │    │ ┌───┬───╦═══╦───┬───╦═══╗     │
///                              ├─▶│SortPreservingMerge│───▶│ │ A │ B ║ B ║ C │ D ║ E ║ ... │
///                              │  │                   │    │ └───┴─▲─╩═══╩───┴───╩═══╝     │
/// ┌─────────────────────────┐  │  └───────────────────┘    └─┬─────┴───────────────────────┘
/// │ ╔═══╦═══╗               │  │
/// │ ║ B ║ E ║     ...       │──┘                             │
/// │ ╚═══╩═══╝               │              Stable sort if `enable_round_robin_repartition=false`:
/// └─────────────────────────┘              the merged stream places equal rows from stream 1
///   Stream 2
///
///
///  Input Partitions                                          Output Partition
///    (sorted)                                                  (sorted)
/// ```
///
/// # Error Handling
///
/// If any of the input partitions return an error, the error is propagated to
/// the output and inputs are not polled again.
#[derive(Debug, Clone)]
pub struct SortPreservingMergeExec {
    /// Input plan with sorted partitions
    input: Arc<dyn ExecutionPlan>,
    /// Sort expressions
    expr: LexOrdering,
    /// Execution metrics
    metrics: ExecutionPlanMetricsSet,
    /// Optional number of rows to fetch. Stops producing rows after this fetch
    fetch: Option<usize>,
    /// Cache holding plan properties like equivalences, output partitioning etc.
    cache: PlanProperties,
    /// Use round-robin selection of tied winners of loser tree
    ///
    /// See [`Self::with_round_robin_repartition`] for more information.
    enable_round_robin_repartition: bool,
}

impl SortPreservingMergeExec {
    /// Create a new sort execution plan
    pub fn new(expr: LexOrdering, input: Arc<dyn ExecutionPlan>) -> Self {
        let cache = Self::compute_properties(&input, expr.clone());
        Self {
            input,
            expr,
            metrics: ExecutionPlanMetricsSet::new(),
            fetch: None,
            cache,
            enable_round_robin_repartition: true,
        }
    }

    /// Sets the number of rows to fetch
    pub fn with_fetch(mut self, fetch: Option<usize>) -> Self {
        self.fetch = fetch;
        self
    }

    /// Sets the selection strategy of tied winners of the loser tree algorithm
    ///
    /// If true (the default) equal output rows are placed in the merged stream
    /// in round robin fashion. This approach consumes input streams at more
    /// even rates when there are many rows with the same sort key.
    ///
    /// If false, equal output rows are always placed in the merged stream in
    /// the order of the inputs, resulting in potentially slower execution but a
    /// stable output order.
    pub fn with_round_robin_repartition(
        mut self,
        enable_round_robin_repartition: bool,
    ) -> Self {
        self.enable_round_robin_repartition = enable_round_robin_repartition;
        self
    }

    /// Input schema
    pub fn input(&self) -> &Arc<dyn ExecutionPlan> {
        &self.input
    }

    /// Sort expressions
    pub fn expr(&self) -> &LexOrdering {
        &self.expr
    }

    /// Fetch
    pub fn fetch(&self) -> Option<usize> {
        self.fetch
    }

    /// Creates the cache object that stores the plan properties
    /// such as schema, equivalence properties, ordering, partitioning, etc.
    fn compute_properties(
        input: &Arc<dyn ExecutionPlan>,
        ordering: LexOrdering,
    ) -> PlanProperties {
        let input_partitions = input.output_partitioning().partition_count();
        let (drive, scheduling) = if input_partitions > 1 {
            (EvaluationType::Eager, SchedulingType::Cooperative)
        } else {
            (
                input.properties().evaluation_type,
                input.properties().scheduling_type,
            )
        };

        let mut eq_properties = input.equivalence_properties().clone();
        eq_properties.clear_per_partition_constants();
        eq_properties.add_ordering(ordering);
        PlanProperties::new(
            eq_properties,                        // Equivalence Properties
            Partitioning::UnknownPartitioning(1), // Output Partitioning
            input.pipeline_behavior(),            // Pipeline Behavior
            input.boundedness(),                  // Boundedness
        )
        .with_evaluation_type(drive)
        .with_scheduling_type(scheduling)
    }
}

impl DisplayAs for SortPreservingMergeExec {
    fn fmt_as(
        &self,
        t: DisplayFormatType,
        f: &mut std::fmt::Formatter,
    ) -> std::fmt::Result {
        match t {
            DisplayFormatType::Default | DisplayFormatType::Verbose => {
                write!(f, "SortPreservingMergeExec: [{}]", self.expr)?;
                if let Some(fetch) = self.fetch {
                    write!(f, ", fetch={fetch}")?;
                };

                Ok(())
            }
            DisplayFormatType::TreeRender => {
                if let Some(fetch) = self.fetch {
                    writeln!(f, "limit={fetch}")?;
                };

                for (i, e) in self.expr().iter().enumerate() {
                    e.fmt_sql(f)?;
                    if i != self.expr().len() - 1 {
                        write!(f, ", ")?;
                    }
                }

                Ok(())
            }
        }
    }
}

impl ExecutionPlan for SortPreservingMergeExec {
    fn name(&self) -> &'static str {
        "SortPreservingMergeExec"
    }

    /// Return a reference to Any that can be used for downcasting
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn properties(&self) -> &PlanProperties {
        &self.cache
    }

    fn fetch(&self) -> Option<usize> {
        self.fetch
    }

    /// Sets the number of rows to fetch
    fn with_fetch(&self, limit: Option<usize>) -> Option<Arc<dyn ExecutionPlan>> {
        Some(Arc::new(Self {
            input: Arc::clone(&self.input),
            expr: self.expr.clone(),
            metrics: self.metrics.clone(),
            fetch: limit,
            cache: self.cache.clone(),
            enable_round_robin_repartition: true,
        }))
    }

    fn required_input_distribution(&self) -> Vec<Distribution> {
        vec![Distribution::UnspecifiedDistribution]
    }

    fn benefits_from_input_partitioning(&self) -> Vec<bool> {
        vec![false]
    }

    fn required_input_ordering(&self) -> Vec<Option<OrderingRequirements>> {
        vec![Some(OrderingRequirements::from(self.expr.clone()))]
    }

    fn maintains_input_order(&self) -> Vec<bool> {
        vec![true]
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.input]
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        Ok(Arc::new(
            SortPreservingMergeExec::new(self.expr.clone(), Arc::clone(&children[0]))
                .with_fetch(self.fetch),
        ))
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        trace!("Start SortPreservingMergeExec::execute for partition: {partition}");
        assert_eq_or_internal_err!(
            partition,
            0,
            "SortPreservingMergeExec invalid partition {partition}"
        );

        let input_partitions = self.input.output_partitioning().partition_count();
        trace!(
            "Number of input partitions of  SortPreservingMergeExec::execute: {input_partitions}"
        );
        let schema = self.schema();

        let reservation =
            MemoryConsumer::new(format!("SortPreservingMergeExec[{partition}]"))
                .register(&context.runtime_env().memory_pool);

        match input_partitions {
            0 => internal_err!(
                "SortPreservingMergeExec requires at least one input partition"
            ),
            1 => match self.fetch {
                Some(fetch) => {
                    let stream = self.input.execute(0, context)?;
                    debug!("Done getting stream for SortPreservingMergeExec::execute with 1 input with {fetch}");
                    Ok(Box::pin(LimitStream::new(
                        stream,
                        0,
                        Some(fetch),
                        BaselineMetrics::new(&self.metrics, partition),
                    )))
                }
                None => {
                    let stream = self.input.execute(0, context);
                    debug!("Done getting stream for SortPreservingMergeExec::execute with 1 input without fetch");
                    stream
                }
            },
            _ => {
                let receivers = (0..input_partitions)
                    .map(|partition| {
                        let stream =
                            self.input.execute(partition, Arc::clone(&context))?;
                        Ok(spawn_buffered(stream, 1))
                    })
                    .collect::<Result<_>>()?;

                debug!("Done setting up sender-receiver for SortPreservingMergeExec::execute");

                let result = StreamingMergeBuilder::new()
                    .with_streams(receivers)
                    .with_schema(schema)
                    .with_expressions(&self.expr)
                    .with_metrics(BaselineMetrics::new(&self.metrics, partition))
                    .with_batch_size(context.session_config().batch_size())
                    .with_fetch(self.fetch)
                    .with_reservation(reservation)
                    .with_round_robin_tie_breaker(self.enable_round_robin_repartition)
                    .build()?;

                debug!("Got stream result from SortPreservingMergeStream::new_from_receivers");

                Ok(result)
            }
        }
    }

    fn metrics(&self) -> Option<MetricsSet> {
        Some(self.metrics.clone_inner())
    }

    fn statistics(&self) -> Result<Statistics> {
        self.input.partition_statistics(None)
    }

    fn partition_statistics(&self, _partition: Option<usize>) -> Result<Statistics> {
        self.input.partition_statistics(None)
    }

    fn supports_limit_pushdown(&self) -> bool {
        true
    }

    /// Tries to swap the projection with its input [`SortPreservingMergeExec`].
    /// If this is possible, it returns the new [`SortPreservingMergeExec`] whose
    /// child is a projection. Otherwise, it returns None.
    fn try_swapping_with_projection(
        &self,
        projection: &ProjectionExec,
    ) -> Result<Option<Arc<dyn ExecutionPlan>>> {
        // If the projection does not narrow the schema, we should not try to push it down.
        if projection.expr().len() >= projection.input().schema().fields().len() {
            return Ok(None);
        }

        let Some(updated_exprs) = update_ordering(self.expr.clone(), projection.expr())?
        else {
            return Ok(None);
        };

        Ok(Some(Arc::new(
            SortPreservingMergeExec::new(
                updated_exprs,
                make_with_child(projection, self.input())?,
            )
            .with_fetch(self.fetch()),
        )))
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashSet;
    use std::fmt::Formatter;
    use std::pin::Pin;
    use std::sync::Mutex;
    use std::task::{ready, Context, Poll, Waker};
    use std::time::Duration;

    use super::*;
    use crate::coalesce_batches::CoalesceBatchesExec;
    use crate::coalesce_partitions::CoalescePartitionsExec;
    use crate::execution_plan::{Boundedness, EmissionType};
    use crate::expressions::col;
    use crate::metrics::{MetricValue, Timestamp};
    use crate::repartition::RepartitionExec;
    use crate::sorts::sort::SortExec;
    use crate::stream::RecordBatchReceiverStream;
    use crate::test::exec::{assert_strong_count_converges_to_zero, BlockingExec};
    use crate::test::TestMemoryExec;
    use crate::test::{self, assert_is_pending, make_partition};
    use crate::{collect, common};

    use arrow::array::{
        ArrayRef, Int32Array, Int64Array, RecordBatch, StringArray,
        TimestampNanosecondArray,
    };
    use arrow::compute::SortOptions;
    use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
    use datafusion_common::test_util::batches_to_string;
    use datafusion_common::{assert_batches_eq, exec_err};
    use datafusion_common_runtime::SpawnedTask;
    use datafusion_execution::config::SessionConfig;
    use datafusion_execution::runtime_env::RuntimeEnvBuilder;
    use datafusion_execution::RecordBatchStream;
    use datafusion_physical_expr::expressions::Column;
    use datafusion_physical_expr::EquivalenceProperties;
    use datafusion_physical_expr_common::physical_expr::PhysicalExpr;
    use datafusion_physical_expr_common::sort_expr::PhysicalSortExpr;

    use futures::{FutureExt, Stream, StreamExt};
    use insta::assert_snapshot;
    use tokio::time::timeout;

    // The number in the function is highly related to the memory limit we are testing
    // any change of the constant should be aware of
    fn generate_task_ctx_for_round_robin_tie_breaker() -> Result<Arc<TaskContext>> {
        let runtime = RuntimeEnvBuilder::new()
            .with_memory_limit(20_000_000, 1.0)
            .build_arc()?;
        let config = SessionConfig::new();
        let task_ctx = TaskContext::default()
            .with_runtime(runtime)
            .with_session_config(config);
        Ok(Arc::new(task_ctx))
    }
    // The number in the function is highly related to the memory limit we are testing,
    // any change of the constant should be aware of
    fn generate_spm_for_round_robin_tie_breaker(
        enable_round_robin_repartition: bool,
    ) -> Result<Arc<SortPreservingMergeExec>> {
        let target_batch_size = 12500;
        let row_size = 12500;
        let a: ArrayRef = Arc::new(Int32Array::from(vec![1; row_size]));
        let b: ArrayRef = Arc::new(StringArray::from_iter(vec![Some("a"); row_size]));
        let c: ArrayRef = Arc::new(Int64Array::from_iter(vec![0; row_size]));
        let rb = RecordBatch::try_from_iter(vec![("a", a), ("b", b), ("c", c)])?;

        let rbs = (0..1024).map(|_| rb.clone()).collect::<Vec<_>>();

        let schema = rb.schema();
        let sort = [
            PhysicalSortExpr {
                expr: col("b", &schema)?,
                options: Default::default(),
            },
            PhysicalSortExpr {
                expr: col("c", &schema)?,
                options: Default::default(),
            },
        ]
        .into();

        let repartition_exec = RepartitionExec::try_new(
            TestMemoryExec::try_new_exec(&[rbs], schema, None)?,
            Partitioning::RoundRobinBatch(2),
        )?;
        let coalesce_batches_exec =
            CoalesceBatchesExec::new(Arc::new(repartition_exec), target_batch_size);
        let spm = SortPreservingMergeExec::new(sort, Arc::new(coalesce_batches_exec))
            .with_round_robin_repartition(enable_round_robin_repartition);
        Ok(Arc::new(spm))
    }

    /// This test verifies that memory usage stays within limits when the tie breaker is enabled.
    /// Any errors here could indicate unintended changes in tie breaker logic.
    ///
    /// Note: If you adjust constants in this test, ensure that memory usage differs
    /// based on whether the tie breaker is enabled or disabled.
    #[tokio::test(flavor = "multi_thread")]
    async fn test_round_robin_tie_breaker_success() -> Result<()> {
        let task_ctx = generate_task_ctx_for_round_robin_tie_breaker()?;
        let spm = generate_spm_for_round_robin_tie_breaker(true)?;
        let _collected = collect(spm, task_ctx).await?;
        Ok(())
    }

    /// This test verifies that memory usage stays within limits when the tie breaker is enabled.
    /// Any errors here could indicate unintended changes in tie breaker logic.
    ///
    /// Note: If you adjust constants in this test, ensure that memory usage differs
    /// based on whether the tie breaker is enabled or disabled.
    #[tokio::test(flavor = "multi_thread")]
    async fn test_round_robin_tie_breaker_fail() -> Result<()> {
        let task_ctx = generate_task_ctx_for_round_robin_tie_breaker()?;
        let spm = generate_spm_for_round_robin_tie_breaker(false)?;
        let _err = collect(spm, task_ctx).await.unwrap_err();
        Ok(())
    }

    #[tokio::test]
    async fn test_merge_interleave() {
        let task_ctx = Arc::new(TaskContext::default());
        let a: ArrayRef = Arc::new(Int32Array::from(vec![1, 2, 7, 9, 3]));
        let b: ArrayRef = Arc::new(StringArray::from_iter(vec![
            Some("a"),
            Some("c"),
            Some("e"),
            Some("g"),
            Some("j"),
        ]));
        let c: ArrayRef = Arc::new(TimestampNanosecondArray::from(vec![8, 7, 6, 5, 8]));
        let b1 = RecordBatch::try_from_iter(vec![("a", a), ("b", b), ("c", c)]).unwrap();

        let a: ArrayRef = Arc::new(Int32Array::from(vec![10, 20, 70, 90, 30]));
        let b: ArrayRef = Arc::new(StringArray::from_iter(vec![
            Some("b"),
            Some("d"),
            Some("f"),
            Some("h"),
            Some("j"),
        ]));
        let c: ArrayRef = Arc::new(TimestampNanosecondArray::from(vec![4, 6, 2, 2, 6]));
        let b2 = RecordBatch::try_from_iter(vec![("a", a), ("b", b), ("c", c)]).unwrap();

        _test_merge(
            &[vec![b1], vec![b2]],
            &[
                "+----+---+-------------------------------+",
                "| a  | b | c                             |",
                "+----+---+-------------------------------+",
                "| 1  | a | 1970-01-01T00:00:00.000000008 |",
                "| 10 | b | 1970-01-01T00:00:00.000000004 |",
                "| 2  | c | 1970-01-01T00:00:00.000000007 |",
                "| 20 | d | 1970-01-01T00:00:00.000000006 |",
                "| 7  | e | 1970-01-01T00:00:00.000000006 |",
                "| 70 | f | 1970-01-01T00:00:00.000000002 |",
                "| 9  | g | 1970-01-01T00:00:00.000000005 |",
                "| 90 | h | 1970-01-01T00:00:00.000000002 |",
                "| 30 | j | 1970-01-01T00:00:00.000000006 |", // input b2 before b1
                "| 3  | j | 1970-01-01T00:00:00.000000008 |",
                "+----+---+-------------------------------+",
            ],
            task_ctx,
        )
        .await;
    }

    #[tokio::test]
    async fn test_merge_some_overlap() {
        let task_ctx = Arc::new(TaskContext::default());
        let a: ArrayRef = Arc::new(Int32Array::from(vec![1, 2, 7, 9, 3]));
        let b: ArrayRef = Arc::new(StringArray::from_iter(vec![
            Some("a"),
            Some("b"),
            Some("c"),
            Some("d"),
            Some("e"),
        ]));
        let c: ArrayRef = Arc::new(TimestampNanosecondArray::from(vec![8, 7, 6, 5, 8]));
        let b1 = RecordBatch::try_from_iter(vec![("a", a), ("b", b), ("c", c)]).unwrap();

        let a: ArrayRef = Arc::new(Int32Array::from(vec![70, 90, 30, 100, 110]));
        let b: ArrayRef = Arc::new(StringArray::from_iter(vec![
            Some("c"),
            Some("d"),
            Some("e"),
            Some("f"),
            Some("g"),
        ]));
        let c: ArrayRef = Arc::new(TimestampNanosecondArray::from(vec![4, 6, 2, 2, 6]));
        let b2 = RecordBatch::try_from_iter(vec![("a", a), ("b", b), ("c", c)]).unwrap();

        _test_merge(
            &[vec![b1], vec![b2]],
            &[
                "+-----+---+-------------------------------+",
                "| a   | b | c                             |",
                "+-----+---+-------------------------------+",
                "| 1   | a | 1970-01-01T00:00:00.000000008 |",
                "| 2   | b | 1970-01-01T00:00:00.000000007 |",
                "| 70  | c | 1970-01-01T00:00:00.000000004 |",
                "| 7   | c | 1970-01-01T00:00:00.000000006 |",
                "| 9   | d | 1970-01-01T00:00:00.000000005 |",
                "| 90  | d | 1970-01-01T00:00:00.000000006 |",
                "| 30  | e | 1970-01-01T00:00:00.000000002 |",
                "| 3   | e | 1970-01-01T00:00:00.000000008 |",
                "| 100 | f | 1970-01-01T00:00:00.000000002 |",
                "| 110 | g | 1970-01-01T00:00:00.000000006 |",
                "+-----+---+-------------------------------+",
            ],
            task_ctx,
        )
        .await;
    }

    #[tokio::test]
    async fn test_merge_no_overlap() {
        let task_ctx = Arc::new(TaskContext::default());
        let a: ArrayRef = Arc::new(Int32Array::from(vec![1, 2, 7, 9, 3]));
        let b: ArrayRef = Arc::new(StringArray::from_iter(vec![
            Some("a"),
            Some("b"),
            Some("c"),
            Some("d"),
            Some("e"),
        ]));
        let c: ArrayRef = Arc::new(TimestampNanosecondArray::from(vec![8, 7, 6, 5, 8]));
        let b1 = RecordBatch::try_from_iter(vec![("a", a), ("b", b), ("c", c)]).unwrap();

        let a: ArrayRef = Arc::new(Int32Array::from(vec![10, 20, 70, 90, 30]));
        let b: ArrayRef = Arc::new(StringArray::from_iter(vec![
            Some("f"),
            Some("g"),
            Some("h"),
            Some("i"),
            Some("j"),
        ]));
        let c: ArrayRef = Arc::new(TimestampNanosecondArray::from(vec![4, 6, 2, 2, 6]));
        let b2 = RecordBatch::try_from_iter(vec![("a", a), ("b", b), ("c", c)]).unwrap();

        _test_merge(
            &[vec![b1], vec![b2]],
            &[
                "+----+---+-------------------------------+",
                "| a  | b | c                             |",
                "+----+---+-------------------------------+",
                "| 1  | a | 1970-01-01T00:00:00.000000008 |",
                "| 2  | b | 1970-01-01T00:00:00.000000007 |",
                "| 7  | c | 1970-01-01T00:00:00.000000006 |",
                "| 9  | d | 1970-01-01T00:00:00.000000005 |",
                "| 3  | e | 1970-01-01T00:00:00.000000008 |",
                "| 10 | f | 1970-01-01T00:00:00.000000004 |",
                "| 20 | g | 1970-01-01T00:00:00.000000006 |",
                "| 70 | h | 1970-01-01T00:00:00.000000002 |",
                "| 90 | i | 1970-01-01T00:00:00.000000002 |",
                "| 30 | j | 1970-01-01T00:00:00.000000006 |",
                "+----+---+-------------------------------+",
            ],
            task_ctx,
        )
        .await;
    }

    #[tokio::test]
    async fn test_merge_three_partitions() {
        let task_ctx = Arc::new(TaskContext::default());
        let a: ArrayRef = Arc::new(Int32Array::from(vec![1, 2, 7, 9, 3]));
        let b: ArrayRef = Arc::new(StringArray::from_iter(vec![
            Some("a"),
            Some("b"),
            Some("c"),
            Some("d"),
            Some("f"),
        ]));
        let c: ArrayRef = Arc::new(TimestampNanosecondArray::from(vec![8, 7, 6, 5, 8]));
        let b1 = RecordBatch::try_from_iter(vec![("a", a), ("b", b), ("c", c)]).unwrap();

        let a: ArrayRef = Arc::new(Int32Array::from(vec![10, 20, 70, 90, 30]));
        let b: ArrayRef = Arc::new(StringArray::from_iter(vec![
            Some("e"),
            Some("g"),
            Some("h"),
            Some("i"),
            Some("j"),
        ]));
        let c: ArrayRef =
            Arc::new(TimestampNanosecondArray::from(vec![40, 60, 20, 20, 60]));
        let b2 = RecordBatch::try_from_iter(vec![("a", a), ("b", b), ("c", c)]).unwrap();

        let a: ArrayRef = Arc::new(Int32Array::from(vec![100, 200, 700, 900, 300]));
        let b: ArrayRef = Arc::new(StringArray::from_iter(vec![
            Some("f"),
            Some("g"),
            Some("h"),
            Some("i"),
            Some("j"),
        ]));
        let c: ArrayRef = Arc::new(TimestampNanosecondArray::from(vec![4, 6, 2, 2, 6]));
        let b3 = RecordBatch::try_from_iter(vec![("a", a), ("b", b), ("c", c)]).unwrap();

        _test_merge(
            &[vec![b1], vec![b2], vec![b3]],
            &[
                "+-----+---+-------------------------------+",
                "| a   | b | c                             |",
                "+-----+---+-------------------------------+",
                "| 1   | a | 1970-01-01T00:00:00.000000008 |",
                "| 2   | b | 1970-01-01T00:00:00.000000007 |",
                "| 7   | c | 1970-01-01T00:00:00.000000006 |",
                "| 9   | d | 1970-01-01T00:00:00.000000005 |",
                "| 10  | e | 1970-01-01T00:00:00.000000040 |",
                "| 100 | f | 1970-01-01T00:00:00.000000004 |",
                "| 3   | f | 1970-01-01T00:00:00.000000008 |",
                "| 200 | g | 1970-01-01T00:00:00.000000006 |",
                "| 20  | g | 1970-01-01T00:00:00.000000060 |",
                "| 700 | h | 1970-01-01T00:00:00.000000002 |",
                "| 70  | h | 1970-01-01T00:00:00.000000020 |",
                "| 900 | i | 1970-01-01T00:00:00.000000002 |",
                "| 90  | i | 1970-01-01T00:00:00.000000020 |",
                "| 300 | j | 1970-01-01T00:00:00.000000006 |",
                "| 30  | j | 1970-01-01T00:00:00.000000060 |",
                "+-----+---+-------------------------------+",
            ],
            task_ctx,
        )
        .await;
    }

    async fn _test_merge(
        partitions: &[Vec<RecordBatch>],
        exp: &[&str],
        context: Arc<TaskContext>,
    ) {
        let schema = partitions[0][0].schema();
        let sort = [
            PhysicalSortExpr {
                expr: col("b", &schema).unwrap(),
                options: Default::default(),
            },
            PhysicalSortExpr {
                expr: col("c", &schema).unwrap(),
                options: Default::default(),
            },
        ]
        .into();
        let exec = TestMemoryExec::try_new_exec(partitions, schema, None).unwrap();
        let merge = Arc::new(SortPreservingMergeExec::new(sort, exec));

        let collected = collect(merge, context).await.unwrap();
        assert_batches_eq!(exp, collected.as_slice());
    }

    async fn sorted_merge(
        input: Arc<dyn ExecutionPlan>,
        sort: LexOrdering,
        context: Arc<TaskContext>,
    ) -> RecordBatch {
        let merge = Arc::new(SortPreservingMergeExec::new(sort, input));
        let mut result = collect(merge, context).await.unwrap();
        assert_eq!(result.len(), 1);
        result.remove(0)
    }

    async fn partition_sort(
        input: Arc<dyn ExecutionPlan>,
        sort: LexOrdering,
        context: Arc<TaskContext>,
    ) -> RecordBatch {
        let sort_exec =
            Arc::new(SortExec::new(sort.clone(), input).with_preserve_partitioning(true));
        sorted_merge(sort_exec, sort, context).await
    }

    async fn basic_sort(
        src: Arc<dyn ExecutionPlan>,
        sort: LexOrdering,
        context: Arc<TaskContext>,
    ) -> RecordBatch {
        let merge = Arc::new(CoalescePartitionsExec::new(src));
        let sort_exec = Arc::new(SortExec::new(sort, merge));
        let mut result = collect(sort_exec, context).await.unwrap();
        assert_eq!(result.len(), 1);
        result.remove(0)
    }

    #[tokio::test]
    async fn test_partition_sort() -> Result<()> {
        let task_ctx = Arc::new(TaskContext::default());
        let partitions = 4;
        let csv = test::scan_partitioned(partitions);
        let schema = csv.schema();

        let sort: LexOrdering = [PhysicalSortExpr {
            expr: col("i", &schema)?,
            options: SortOptions {
                descending: true,
                nulls_first: true,
            },
        }]
        .into();

        let basic =
            basic_sort(Arc::clone(&csv), sort.clone(), Arc::clone(&task_ctx)).await;
        let partition = partition_sort(csv, sort, Arc::clone(&task_ctx)).await;

        let basic = arrow::util::pretty::pretty_format_batches(&[basic])
            .unwrap()
            .to_string();
        let partition = arrow::util::pretty::pretty_format_batches(&[partition])
            .unwrap()
            .to_string();

        assert_eq!(
            basic, partition,
            "basic:\n\n{basic}\n\npartition:\n\n{partition}\n\n"
        );

        Ok(())
    }

    // Split the provided record batch into multiple batch_size record batches
    fn split_batch(sorted: &RecordBatch, batch_size: usize) -> Vec<RecordBatch> {
        let batches = sorted.num_rows().div_ceil(batch_size);

        // Split the sorted RecordBatch into multiple
        (0..batches)
            .map(|batch_idx| {
                let columns = (0..sorted.num_columns())
                    .map(|column_idx| {
                        let length =
                            batch_size.min(sorted.num_rows() - batch_idx * batch_size);

                        sorted
                            .column(column_idx)
                            .slice(batch_idx * batch_size, length)
                    })
                    .collect();

                RecordBatch::try_new(sorted.schema(), columns).unwrap()
            })
            .collect()
    }

    async fn sorted_partitioned_input(
        sort: LexOrdering,
        sizes: &[usize],
        context: Arc<TaskContext>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let partitions = 4;
        let csv = test::scan_partitioned(partitions);

        let sorted = basic_sort(csv, sort, context).await;
        let split: Vec<_> = sizes.iter().map(|x| split_batch(&sorted, *x)).collect();

        TestMemoryExec::try_new_exec(&split, sorted.schema(), None).map(|e| e as _)
    }

    #[tokio::test]
    async fn test_partition_sort_streaming_input() -> Result<()> {
        let task_ctx = Arc::new(TaskContext::default());
        let schema = make_partition(11).schema();
        let sort: LexOrdering = [PhysicalSortExpr {
            expr: col("i", &schema)?,
            options: Default::default(),
        }]
        .into();

        let input =
            sorted_partitioned_input(sort.clone(), &[10, 3, 11], Arc::clone(&task_ctx))
                .await?;
        let basic =
            basic_sort(Arc::clone(&input), sort.clone(), Arc::clone(&task_ctx)).await;
        let partition = sorted_merge(input, sort, Arc::clone(&task_ctx)).await;

        assert_eq!(basic.num_rows(), 1200);
        assert_eq!(partition.num_rows(), 1200);

        let basic = arrow::util::pretty::pretty_format_batches(&[basic])?.to_string();
        let partition =
            arrow::util::pretty::pretty_format_batches(&[partition])?.to_string();

        assert_eq!(basic, partition);

        Ok(())
    }

    #[tokio::test]
    async fn test_partition_sort_streaming_input_output() -> Result<()> {
        let schema = make_partition(11).schema();
        let sort: LexOrdering = [PhysicalSortExpr {
            expr: col("i", &schema)?,
            options: Default::default(),
        }]
        .into();

        // Test streaming with default batch size
        let task_ctx = Arc::new(TaskContext::default());
        let input =
            sorted_partitioned_input(sort.clone(), &[10, 5, 13], Arc::clone(&task_ctx))
                .await?;
        let basic = basic_sort(Arc::clone(&input), sort.clone(), task_ctx).await;

        // batch size of 23
        let task_ctx = TaskContext::default()
            .with_session_config(SessionConfig::new().with_batch_size(23));
        let task_ctx = Arc::new(task_ctx);

        let merge = Arc::new(SortPreservingMergeExec::new(sort, input));
        let merged = collect(merge, task_ctx).await?;

        assert_eq!(merged.len(), 53);
        assert_eq!(basic.num_rows(), 1200);
        assert_eq!(merged.iter().map(|x| x.num_rows()).sum::<usize>(), 1200);

        let basic = arrow::util::pretty::pretty_format_batches(&[basic])?.to_string();
        let partition = arrow::util::pretty::pretty_format_batches(&merged)?.to_string();

        assert_eq!(basic, partition);

        Ok(())
    }

    #[tokio::test]
    async fn test_nulls() {
        let task_ctx = Arc::new(TaskContext::default());
        let a: ArrayRef = Arc::new(Int32Array::from(vec![1, 2, 7, 9, 3]));
        let b: ArrayRef = Arc::new(StringArray::from_iter(vec![
            None,
            Some("a"),
            Some("b"),
            Some("d"),
            Some("e"),
        ]));
        let c: ArrayRef = Arc::new(TimestampNanosecondArray::from(vec![
            Some(8),
            None,
            Some(6),
            None,
            Some(4),
        ]));
        let b1 = RecordBatch::try_from_iter(vec![("a", a), ("b", b), ("c", c)]).unwrap();

        let a: ArrayRef = Arc::new(Int32Array::from(vec![1, 2, 3, 4, 5]));
        let b: ArrayRef = Arc::new(StringArray::from_iter(vec![
            None,
            Some("b"),
            Some("g"),
            Some("h"),
            Some("i"),
        ]));
        let c: ArrayRef = Arc::new(TimestampNanosecondArray::from(vec![
            Some(8),
            None,
            Some(5),
            None,
            Some(4),
        ]));
        let b2 = RecordBatch::try_from_iter(vec![("a", a), ("b", b), ("c", c)]).unwrap();
        let schema = b1.schema();

        let sort = [
            PhysicalSortExpr {
                expr: col("b", &schema).unwrap(),
                options: SortOptions {
                    descending: false,
                    nulls_first: true,
                },
            },
            PhysicalSortExpr {
                expr: col("c", &schema).unwrap(),
                options: SortOptions {
                    descending: false,
                    nulls_first: false,
                },
            },
        ]
        .into();
        let exec =
            TestMemoryExec::try_new_exec(&[vec![b1], vec![b2]], schema, None).unwrap();
        let merge = Arc::new(SortPreservingMergeExec::new(sort, exec));

        let collected = collect(merge, task_ctx).await.unwrap();
        assert_eq!(collected.len(), 1);

        assert_snapshot!(batches_to_string(collected.as_slice()), @r#"
            +---+---+-------------------------------+
            | a | b | c                             |
            +---+---+-------------------------------+
            | 1 |   | 1970-01-01T00:00:00.000000008 |
            | 1 |   | 1970-01-01T00:00:00.000000008 |
            | 2 | a |                               |
            | 7 | b | 1970-01-01T00:00:00.000000006 |
            | 2 | b |                               |
            | 9 | d |                               |
            | 3 | e | 1970-01-01T00:00:00.000000004 |
            | 3 | g | 1970-01-01T00:00:00.000000005 |
            | 4 | h |                               |
            | 5 | i | 1970-01-01T00:00:00.000000004 |
            +---+---+-------------------------------+
            "#);
    }

    #[tokio::test]
    async fn test_sort_merge_single_partition_with_fetch() {
        let task_ctx = Arc::new(TaskContext::default());
        let a: ArrayRef = Arc::new(Int32Array::from(vec![1, 2, 7, 9, 3]));
        let b: ArrayRef = Arc::new(StringArray::from(vec!["a", "b", "c", "d", "e"]));
        let batch = RecordBatch::try_from_iter(vec![("a", a), ("b", b)]).unwrap();
        let schema = batch.schema();

        let sort = [PhysicalSortExpr {
            expr: col("b", &schema).unwrap(),
            options: SortOptions {
                descending: false,
                nulls_first: true,
            },
        }]
        .into();
        let exec = TestMemoryExec::try_new_exec(&[vec![batch]], schema, None).unwrap();
        let merge =
            Arc::new(SortPreservingMergeExec::new(sort, exec).with_fetch(Some(2)));

        let collected = collect(merge, task_ctx).await.unwrap();
        assert_eq!(collected.len(), 1);

        assert_snapshot!(batches_to_string(collected.as_slice()), @r#"
            +---+---+
            | a | b |
            +---+---+
            | 1 | a |
            | 2 | b |
            +---+---+
            "#);
    }

    #[tokio::test]
    async fn test_sort_merge_single_partition_without_fetch() {
        let task_ctx = Arc::new(TaskContext::default());
        let a: ArrayRef = Arc::new(Int32Array::from(vec![1, 2, 7, 9, 3]));
        let b: ArrayRef = Arc::new(StringArray::from(vec!["a", "b", "c", "d", "e"]));
        let batch = RecordBatch::try_from_iter(vec![("a", a), ("b", b)]).unwrap();
        let schema = batch.schema();

        let sort = [PhysicalSortExpr {
            expr: col("b", &schema).unwrap(),
            options: SortOptions {
                descending: false,
                nulls_first: true,
            },
        }]
        .into();
        let exec = TestMemoryExec::try_new_exec(&[vec![batch]], schema, None).unwrap();
        let merge = Arc::new(SortPreservingMergeExec::new(sort, exec));

        let collected = collect(merge, task_ctx).await.unwrap();
        assert_eq!(collected.len(), 1);

        assert_snapshot!(batches_to_string(collected.as_slice()), @r#"
            +---+---+
            | a | b |
            +---+---+
            | 1 | a |
            | 2 | b |
            | 7 | c |
            | 9 | d |
            | 3 | e |
            +---+---+
            "#);
    }

    #[tokio::test]
    async fn test_async() -> Result<()> {
        let task_ctx = Arc::new(TaskContext::default());
        let schema = make_partition(11).schema();
        let sort: LexOrdering = [PhysicalSortExpr {
            expr: col("i", &schema).unwrap(),
            options: SortOptions::default(),
        }]
        .into();

        let batches =
            sorted_partitioned_input(sort.clone(), &[5, 7, 3], Arc::clone(&task_ctx))
                .await?;

        let partition_count = batches.output_partitioning().partition_count();
        let mut streams = Vec::with_capacity(partition_count);

        for partition in 0..partition_count {
            let mut builder = RecordBatchReceiverStream::builder(Arc::clone(&schema), 1);

            let sender = builder.tx();

            let mut stream = batches.execute(partition, Arc::clone(&task_ctx)).unwrap();
            builder.spawn(async move {
                while let Some(batch) = stream.next().await {
                    sender.send(batch).await.unwrap();
                    // This causes the MergeStream to wait for more input
                    tokio::time::sleep(Duration::from_millis(10)).await;
                }

                Ok(())
            });

            streams.push(builder.build());
        }

        let metrics = ExecutionPlanMetricsSet::new();
        let reservation =
            MemoryConsumer::new("test").register(&task_ctx.runtime_env().memory_pool);

        let fetch = None;
        let merge_stream = StreamingMergeBuilder::new()
            .with_streams(streams)
            .with_schema(batches.schema())
            .with_expressions(&sort)
            .with_metrics(BaselineMetrics::new(&metrics, 0))
            .with_batch_size(task_ctx.session_config().batch_size())
            .with_fetch(fetch)
            .with_reservation(reservation)
            .build()?;

        let mut merged = common::collect(merge_stream).await.unwrap();

        assert_eq!(merged.len(), 1);
        let merged = merged.remove(0);
        let basic = basic_sort(batches, sort.clone(), Arc::clone(&task_ctx)).await;

        let basic = arrow::util::pretty::pretty_format_batches(&[basic])
            .unwrap()
            .to_string();
        let partition = arrow::util::pretty::pretty_format_batches(&[merged])
            .unwrap()
            .to_string();

        assert_eq!(
            basic, partition,
            "basic:\n\n{basic}\n\npartition:\n\n{partition}\n\n"
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_merge_metrics() {
        let task_ctx = Arc::new(TaskContext::default());
        let a: ArrayRef = Arc::new(Int32Array::from(vec![1, 2]));
        let b: ArrayRef = Arc::new(StringArray::from_iter(vec![Some("a"), Some("c")]));
        let b1 = RecordBatch::try_from_iter(vec![("a", a), ("b", b)]).unwrap();

        let a: ArrayRef = Arc::new(Int32Array::from(vec![10, 20]));
        let b: ArrayRef = Arc::new(StringArray::from_iter(vec![Some("b"), Some("d")]));
        let b2 = RecordBatch::try_from_iter(vec![("a", a), ("b", b)]).unwrap();

        let schema = b1.schema();
        let sort = [PhysicalSortExpr {
            expr: col("b", &schema).unwrap(),
            options: Default::default(),
        }]
        .into();
        let exec =
            TestMemoryExec::try_new_exec(&[vec![b1], vec![b2]], schema, None).unwrap();
        let merge = Arc::new(SortPreservingMergeExec::new(sort, exec));

        let collected = collect(Arc::clone(&merge) as Arc<dyn ExecutionPlan>, task_ctx)
            .await
            .unwrap();
        assert_snapshot!(batches_to_string(collected.as_slice()), @r#"
            +----+---+
            | a  | b |
            +----+---+
            | 1  | a |
            | 10 | b |
            | 2  | c |
            | 20 | d |
            +----+---+
            "#);

        // Now, validate metrics
        let metrics = merge.metrics().unwrap();

        assert_eq!(metrics.output_rows().unwrap(), 4);
        assert!(metrics.elapsed_compute().unwrap() > 0);

        let mut saw_start = false;
        let mut saw_end = false;
        metrics.iter().for_each(|m| match m.value() {
            MetricValue::StartTimestamp(ts) => {
                saw_start = true;
                assert!(nanos_from_timestamp(ts) > 0);
            }
            MetricValue::EndTimestamp(ts) => {
                saw_end = true;
                assert!(nanos_from_timestamp(ts) > 0);
            }
            _ => {}
        });

        assert!(saw_start);
        assert!(saw_end);
    }

    fn nanos_from_timestamp(ts: &Timestamp) -> i64 {
        ts.value().unwrap().timestamp_nanos_opt().unwrap()
    }

    #[tokio::test]
    async fn test_drop_cancel() -> Result<()> {
        let task_ctx = Arc::new(TaskContext::default());
        let schema =
            Arc::new(Schema::new(vec![Field::new("a", DataType::Float32, true)]));

        let blocking_exec = Arc::new(BlockingExec::new(Arc::clone(&schema), 2));
        let refs = blocking_exec.refs();
        let sort_preserving_merge_exec = Arc::new(SortPreservingMergeExec::new(
            [PhysicalSortExpr {
                expr: col("a", &schema)?,
                options: SortOptions::default(),
            }]
            .into(),
            blocking_exec,
        ));

        let fut = collect(sort_preserving_merge_exec, task_ctx);
        let mut fut = fut.boxed();

        assert_is_pending(&mut fut);
        drop(fut);
        assert_strong_count_converges_to_zero(refs).await;

        Ok(())
    }

    #[tokio::test]
    async fn test_stable_sort() {
        let task_ctx = Arc::new(TaskContext::default());

        // Create record batches like:
        // batch_number |value
        // -------------+------
        //    1         | A
        //    1         | B
        //
        // Ensure that the output is in the same order the batches were fed
        let partitions: Vec<Vec<RecordBatch>> = (0..10)
            .map(|batch_number| {
                let batch_number: Int32Array =
                    vec![Some(batch_number), Some(batch_number)]
                        .into_iter()
                        .collect();
                let value: StringArray = vec![Some("A"), Some("B")].into_iter().collect();

                let batch = RecordBatch::try_from_iter(vec![
                    ("batch_number", Arc::new(batch_number) as ArrayRef),
                    ("value", Arc::new(value) as ArrayRef),
                ])
                .unwrap();

                vec![batch]
            })
            .collect();

        let schema = partitions[0][0].schema();

        let sort = [PhysicalSortExpr {
            expr: col("value", &schema).unwrap(),
            options: SortOptions {
                descending: false,
                nulls_first: true,
            },
        }]
        .into();

        let exec = TestMemoryExec::try_new_exec(&partitions, schema, None).unwrap();
        let merge = Arc::new(SortPreservingMergeExec::new(sort, exec));

        let collected = collect(merge, task_ctx).await.unwrap();
        assert_eq!(collected.len(), 1);

        // Expect the data to be sorted first by "batch_number" (because
        // that was the order it was fed in, even though only "value"
        // is in the sort key)
        assert_snapshot!(batches_to_string(collected.as_slice()), @r#"
                +--------------+-------+
                | batch_number | value |
                +--------------+-------+
                | 0            | A     |
                | 1            | A     |
                | 2            | A     |
                | 3            | A     |
                | 4            | A     |
                | 5            | A     |
                | 6            | A     |
                | 7            | A     |
                | 8            | A     |
                | 9            | A     |
                | 0            | B     |
                | 1            | B     |
                | 2            | B     |
                | 3            | B     |
                | 4            | B     |
                | 5            | B     |
                | 6            | B     |
                | 7            | B     |
                | 8            | B     |
                | 9            | B     |
                +--------------+-------+
            "#);
    }

    #[derive(Debug)]
    struct CongestionState {
        wakers: Vec<Waker>,
        unpolled_partitions: HashSet<usize>,
    }

    #[derive(Debug)]
    struct Congestion {
        congestion_state: Mutex<CongestionState>,
    }

    impl Congestion {
        fn new(partition_count: usize) -> Self {
            Congestion {
                congestion_state: Mutex::new(CongestionState {
                    wakers: vec![],
                    unpolled_partitions: (0usize..partition_count).collect(),
                }),
            }
        }

        fn check_congested(&self, partition: usize, cx: &mut Context<'_>) -> Poll<()> {
            let mut state = self.congestion_state.lock().unwrap();

            state.unpolled_partitions.remove(&partition);

            if state.unpolled_partitions.is_empty() {
                state.wakers.iter().for_each(|w| w.wake_by_ref());
                state.wakers.clear();
                Poll::Ready(())
            } else {
                state.wakers.push(cx.waker().clone());
                Poll::Pending
            }
        }
    }

    /// It returns pending for the 2nd partition until the 3rd partition is polled. The 1st
    /// partition is exhausted from the start, and if it is polled more than one, it panics.
    #[derive(Debug, Clone)]
    struct CongestedExec {
        schema: Schema,
        cache: PlanProperties,
        congestion: Arc<Congestion>,
    }

    impl CongestedExec {
        fn compute_properties(schema: SchemaRef) -> PlanProperties {
            let columns = schema
                .fields
                .iter()
                .enumerate()
                .map(|(i, f)| Arc::new(Column::new(f.name(), i)) as Arc<dyn PhysicalExpr>)
                .collect::<Vec<_>>();
            let mut eq_properties = EquivalenceProperties::new(schema);
            eq_properties.add_ordering(
                columns
                    .iter()
                    .map(|expr| PhysicalSortExpr::new_default(Arc::clone(expr))),
            );
            PlanProperties::new(
                eq_properties,
                Partitioning::Hash(columns, 3),
                EmissionType::Incremental,
                Boundedness::Unbounded {
                    requires_infinite_memory: false,
                },
            )
        }
    }

    impl ExecutionPlan for CongestedExec {
        fn name(&self) -> &'static str {
            Self::static_name()
        }
        fn as_any(&self) -> &dyn Any {
            self
        }
        fn properties(&self) -> &PlanProperties {
            &self.cache
        }
        fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
            vec![]
        }
        fn with_new_children(
            self: Arc<Self>,
            _: Vec<Arc<dyn ExecutionPlan>>,
        ) -> Result<Arc<dyn ExecutionPlan>> {
            Ok(self)
        }
        fn execute(
            &self,
            partition: usize,
            _context: Arc<TaskContext>,
        ) -> Result<SendableRecordBatchStream> {
            Ok(Box::pin(CongestedStream {
                schema: Arc::new(self.schema.clone()),
                none_polled_once: false,
                congestion: Arc::clone(&self.congestion),
                partition,
            }))
        }
    }

    impl DisplayAs for CongestedExec {
        fn fmt_as(&self, t: DisplayFormatType, f: &mut Formatter) -> std::fmt::Result {
            match t {
                DisplayFormatType::Default | DisplayFormatType::Verbose => {
                    write!(f, "CongestedExec",).unwrap()
                }
                DisplayFormatType::TreeRender => {
                    // TODO: collect info
                    write!(f, "").unwrap()
                }
            }
            Ok(())
        }
    }

    /// It returns pending for the 2nd partition until the 3rd partition is polled. The 1st
    /// partition is exhausted from the start, and if it is polled more than once, it panics.
    #[derive(Debug)]
    pub struct CongestedStream {
        schema: SchemaRef,
        none_polled_once: bool,
        congestion: Arc<Congestion>,
        partition: usize,
    }

    impl Stream for CongestedStream {
        type Item = Result<RecordBatch>;
        fn poll_next(
            mut self: Pin<&mut Self>,
            cx: &mut Context<'_>,
        ) -> Poll<Option<Self::Item>> {
            match self.partition {
                0 => {
                    let _ = self.congestion.check_congested(self.partition, cx);
                    if self.none_polled_once {
                        panic!("Exhausted stream is polled more than once")
                    } else {
                        self.none_polled_once = true;
                        Poll::Ready(None)
                    }
                }
                _ => {
                    ready!(self.congestion.check_congested(self.partition, cx));
                    Poll::Ready(None)
                }
            }
        }
    }

    impl RecordBatchStream for CongestedStream {
        fn schema(&self) -> SchemaRef {
            Arc::clone(&self.schema)
        }
    }

    #[tokio::test]
    async fn test_spm_congestion() -> Result<()> {
        let task_ctx = Arc::new(TaskContext::default());
        let schema = Schema::new(vec![Field::new("c1", DataType::UInt64, false)]);
        let properties = CongestedExec::compute_properties(Arc::new(schema.clone()));
        let &partition_count = match properties.output_partitioning() {
            Partitioning::RoundRobinBatch(partitions) => partitions,
            Partitioning::Hash(_, partitions) => partitions,
            Partitioning::UnknownPartitioning(partitions) => partitions,
        };
        let source = CongestedExec {
            schema: schema.clone(),
            cache: properties,
            congestion: Arc::new(Congestion::new(partition_count)),
        };
        let spm = SortPreservingMergeExec::new(
            [PhysicalSortExpr::new_default(Arc::new(Column::new(
                "c1", 0,
            )))]
            .into(),
            Arc::new(source),
        );
        let spm_task = SpawnedTask::spawn(collect(Arc::new(spm), task_ctx));

        let result = timeout(Duration::from_secs(3), spm_task.join()).await;
        match result {
            Ok(Ok(Ok(_batches))) => Ok(()),
            Ok(Ok(Err(e))) => Err(e),
            Ok(Err(_)) => exec_err!("SortPreservingMerge task panicked or was cancelled"),
            Err(_) => exec_err!("SortPreservingMerge caused a deadlock"),
        }
    }
}
