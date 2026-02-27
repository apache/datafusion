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

//! MergePartitionsExec maps N input work items to M output partitions using
//! a shared work queue for work-stealing. Each output stream pops the next
//! work item, executes it (which may produce additional items), yields all
//! batches, then pops the next. Supports lazy morselization: file-level items
//! can be expanded into row-group-level items at execution time.

use std::any::Any;
use std::collections::VecDeque;
use std::fmt;
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll};

use super::metrics::{BaselineMetrics, ExecutionPlanMetricsSet, MetricsSet};
use super::{
    DisplayAs, ExecutionPlanProperties, PlanProperties, SendableRecordBatchStream,
    Statistics,
};
use crate::execution_plan::{CardinalityEffect, EvaluationType};
use crate::filter_pushdown::{FilterDescription, FilterPushdownPhase};
use crate::sort_pushdown::SortOrderPushdownResult;
use crate::{DisplayFormatType, ExecutionPlan, Partitioning, check_if_same_properties};
use datafusion_physical_expr_common::sort_expr::PhysicalSortExpr;

use arrow::datatypes::SchemaRef;
use arrow::record_batch::RecordBatch;
use datafusion_common::Result;
use datafusion_common::config::ConfigOptions;
use datafusion_execution::{RecordBatchStream, TaskContext};
use datafusion_physical_expr::PhysicalExpr;

use futures::future::BoxFuture;
use futures::stream::BoxStream;
use futures::{Stream, StreamExt, ready};

// ─────────────────────────────────────────────────────────────────────────────
// Morselizer trait and types
// ─────────────────────────────────────────────────────────────────────────────

/// An opaque unit of work that a [`Morselizer`] can execute.
///
/// Work items are type-erased so that [`MergePartitionsExec`] stays generic.
/// The [`Morselizer`] implementation knows how to interpret them.
pub struct WorkItem {
    data: Box<dyn Any + Send + Sync>,
}

impl WorkItem {
    /// Create a new work item wrapping any `Send + Sync + 'static` value.
    pub fn new<T: Any + Send + Sync + 'static>(value: T) -> Self {
        Self {
            data: Box::new(value),
        }
    }

    /// Downcast to a concrete type reference.
    pub fn downcast_ref<T: Any>(&self) -> Option<&T> {
        self.data.downcast_ref()
    }

    /// Consume and downcast to a concrete type.
    pub fn downcast<T: Any>(self) -> Result<T, Box<dyn Any + Send + Sync>> {
        match self.data.downcast::<T>() {
            Ok(val) => Ok(*val),
            Err(boxed) => Err(boxed),
        }
    }
}

impl fmt::Debug for WorkItem {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("WorkItem").finish_non_exhaustive()
    }
}

/// The result of executing a [`WorkItem`] via a [`Morselizer`].
pub struct MorselResult {
    /// A stream of record batches for this morsel.
    pub stream: BoxStream<'static, Result<RecordBatch>>,
    /// Additional work items discovered during execution (e.g., row groups
    /// discovered after reading file metadata). These are pushed into the
    /// shared queue for other output streams to steal.
    pub additional_items: Vec<WorkItem>,
}

/// Provides work items and executes them for [`MergePartitionsExec`].
///
/// This trait enables lazy morselization: a [`Morselizer`] can start with
/// coarse-grained items (e.g., files) and expand them into fine-grained
/// items (e.g., row groups) at execution time, after reading metadata.
pub trait Morselizer: Send + Sync + fmt::Debug {
    /// Return the initial set of work items (called once per execute).
    fn initial_items(&self) -> Vec<WorkItem>;

    /// Execute a work item. Returns a future that resolves to a stream
    /// of record batches plus any additional work items discovered.
    ///
    /// For example, executing a file item might read parquet metadata,
    /// return a stream for the first row group, and return additional
    /// items for the remaining row groups.
    fn execute_item(
        &self,
        item: WorkItem,
        context: Arc<TaskContext>,
    ) -> Result<BoxFuture<'static, Result<MorselResult>>>;

    /// The output schema.
    fn schema(&self) -> SchemaRef;
}

// ─────────────────────────────────────────────────────────────────────────────
// PartitionMorselizer — default impl wrapping an ExecutionPlan
// ─────────────────────────────────────────────────────────────────────────────

/// Default [`Morselizer`] that provides one work item per input partition.
///
/// This gives the same behavior as the original atomic-counter approach:
/// each output stream claims whole input partitions.
#[derive(Debug)]
struct PartitionMorselizer {
    input: Arc<dyn ExecutionPlan>,
}

impl Morselizer for PartitionMorselizer {
    fn initial_items(&self) -> Vec<WorkItem> {
        let count = self.input.output_partitioning().partition_count();
        (0..count).map(|i| WorkItem::new(i as usize)).collect()
    }

    fn execute_item(
        &self,
        item: WorkItem,
        context: Arc<TaskContext>,
    ) -> Result<BoxFuture<'static, Result<MorselResult>>> {
        let partition_idx: usize = *item
            .downcast_ref()
            .expect("PartitionMorselizer work item should be usize");
        let stream = self.input.execute(partition_idx, context)?;
        Ok(Box::pin(async move {
            Ok(MorselResult {
                stream: stream.boxed(),
                additional_items: vec![],
            })
        }))
    }

    fn schema(&self) -> SchemaRef {
        self.input.schema()
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// MergePartitionsExec
// ─────────────────────────────────────────────────────────────────────────────

/// Maps N input work items to M output partitions using work-stealing.
///
/// Each output stream pops the next work item from a shared queue, executes
/// it via the [`Morselizer`], yields all batches, then pops the next item.
/// Executing an item may produce additional items (e.g., row groups discovered
/// after reading file metadata), which are pushed back into the queue for
/// other streams to steal.
///
/// This provides natural load balancing without channels or background tasks.
#[derive(Debug)]
pub struct MergePartitionsExec {
    /// The input plan (used for plan-tree operations: children, with_new_children, etc.)
    input: Arc<dyn ExecutionPlan>,
    /// Number of output partitions
    output_partitions: usize,
    /// Provides and executes work items
    morselizer: Arc<dyn Morselizer>,
    /// Shared work queue populated during execute()
    queue: Arc<Mutex<VecDeque<WorkItem>>>,
    /// Whether the queue has been initialized
    queue_initialized: Arc<Mutex<bool>>,
    metrics: ExecutionPlanMetricsSet,
    cache: Arc<PlanProperties>,
}

impl Clone for MergePartitionsExec {
    fn clone(&self) -> Self {
        Self {
            input: Arc::clone(&self.input),
            output_partitions: self.output_partitions,
            morselizer: Arc::clone(&self.morselizer),
            queue: Arc::new(Mutex::new(VecDeque::new())),
            queue_initialized: Arc::new(Mutex::new(false)),
            metrics: ExecutionPlanMetricsSet::new(),
            cache: Arc::clone(&self.cache),
        }
    }
}

impl MergePartitionsExec {
    /// Create a new MergePartitionsExec that maps the input's partitions
    /// to `output_partitions` output partitions using partition-level
    /// work stealing (one work item per input partition).
    pub fn new(input: Arc<dyn ExecutionPlan>, output_partitions: usize) -> Self {
        let morselizer = Arc::new(PartitionMorselizer {
            input: Arc::clone(&input),
        });
        Self::new_with_morselizer(input, output_partitions, morselizer)
    }

    /// Create a new MergePartitionsExec with a custom [`Morselizer`].
    ///
    /// The morselizer controls the granularity of work stealing. For example,
    /// a parquet morselizer can expand file-level items into row-group-level
    /// items for finer-grained load balancing.
    pub fn new_with_morselizer(
        input: Arc<dyn ExecutionPlan>,
        output_partitions: usize,
        morselizer: Arc<dyn Morselizer>,
    ) -> Self {
        let cache = Self::compute_properties(&input, output_partitions);
        MergePartitionsExec {
            input,
            output_partitions,
            morselizer,
            queue: Arc::new(Mutex::new(VecDeque::new())),
            queue_initialized: Arc::new(Mutex::new(false)),
            metrics: ExecutionPlanMetricsSet::new(),
            cache: Arc::new(cache),
        }
    }

    /// Input execution plan
    pub fn input(&self) -> &Arc<dyn ExecutionPlan> {
        &self.input
    }

    /// Number of output partitions
    pub fn output_partitions(&self) -> usize {
        self.output_partitions
    }

    fn with_new_children_and_same_properties(
        &self,
        mut children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Self {
        let morselizer = Arc::new(PartitionMorselizer {
            input: Arc::clone(&children[0]),
        });
        Self {
            input: children.swap_remove(0),
            morselizer,
            queue: Arc::new(Mutex::new(VecDeque::new())),
            queue_initialized: Arc::new(Mutex::new(false)),
            metrics: ExecutionPlanMetricsSet::new(),
            ..Self::clone(self)
        }
    }

    fn compute_properties(
        input: &Arc<dyn ExecutionPlan>,
        output_partitions: usize,
    ) -> PlanProperties {
        let mut eq_properties = input.equivalence_properties().clone();
        eq_properties.clear_orderings();
        eq_properties.clear_per_partition_constants();
        PlanProperties::new(
            eq_properties,
            Partitioning::UnknownPartitioning(output_partitions),
            input.pipeline_behavior(),
            input.boundedness(),
        )
        .with_evaluation_type(EvaluationType::Lazy)
        .with_scheduling_type(input.properties().scheduling_type)
    }
}

impl DisplayAs for MergePartitionsExec {
    fn fmt_as(&self, t: DisplayFormatType, f: &mut fmt::Formatter) -> fmt::Result {
        let input_partitions = self.input.output_partitioning().partition_count();
        match t {
            DisplayFormatType::Default | DisplayFormatType::Verbose => {
                write!(
                    f,
                    "MergePartitionsExec: partitions={input_partitions}→{}",
                    self.output_partitions
                )
            }
            DisplayFormatType::TreeRender => {
                write!(
                    f,
                    "partitions: {input_partitions}→{}",
                    self.output_partitions
                )
            }
        }
    }
}

impl ExecutionPlan for MergePartitionsExec {
    fn name(&self) -> &'static str {
        "MergePartitionsExec"
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn properties(&self) -> &Arc<PlanProperties> {
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
        mut children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        check_if_same_properties!(self, children);
        Ok(Arc::new(MergePartitionsExec::new(
            children.swap_remove(0),
            self.output_partitions,
        )))
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        // Initialize the shared queue exactly once (first call to execute).
        {
            let mut initialized = self.queue_initialized.lock().unwrap();
            if !*initialized {
                let items = self.morselizer.initial_items();
                let mut queue = self.queue.lock().unwrap();
                for item in items {
                    queue.push_back(item);
                }
                *initialized = true;
            }
        }

        let baseline_metrics = BaselineMetrics::new(&self.metrics, partition);

        Ok(Box::pin(MergeStream {
            morselizer: Arc::clone(&self.morselizer),
            context,
            queue: Arc::clone(&self.queue),
            state: MergeStreamState::Idle,
            schema: self.schema(),
            baseline_metrics,
        }))
    }

    fn metrics(&self) -> Option<MetricsSet> {
        Some(self.metrics.clone_inner())
    }

    fn partition_statistics(&self, _partition: Option<usize>) -> Result<Statistics> {
        Ok(Statistics::new_unknown(&self.schema()))
    }

    fn cardinality_effect(&self) -> CardinalityEffect {
        CardinalityEffect::Equal
    }

    fn gather_filters_for_pushdown(
        &self,
        _phase: FilterPushdownPhase,
        parent_filters: Vec<Arc<dyn PhysicalExpr>>,
        _config: &ConfigOptions,
    ) -> Result<FilterDescription> {
        FilterDescription::from_children(parent_filters, &self.children())
    }

    fn try_pushdown_sort(
        &self,
        order: &[PhysicalSortExpr],
    ) -> Result<SortOrderPushdownResult<Arc<dyn ExecutionPlan>>> {
        let result = self.input.try_pushdown_sort(order)?;

        let has_multiple_partitions =
            self.input.output_partitioning().partition_count() > 1;

        result
            .try_map(|new_input| {
                Ok(
                    Arc::new(MergePartitionsExec::new(new_input, self.output_partitions))
                        as Arc<dyn ExecutionPlan>,
                )
            })
            .map(|r| {
                if has_multiple_partitions {
                    r.into_inexact()
                } else {
                    r
                }
            })
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// MergeStream — the output stream
// ─────────────────────────────────────────────────────────────────────────────

enum MergeStreamState {
    /// Ready to pop the next work item from the queue.
    Idle,
    /// Waiting for a morsel future to complete (e.g., reading metadata).
    Opening {
        future: BoxFuture<'static, Result<MorselResult>>,
    },
    /// Yielding batches from a morsel's stream.
    Scanning {
        stream: BoxStream<'static, Result<RecordBatch>>,
    },
    /// Terminal state.
    Done,
}

struct MergeStream {
    morselizer: Arc<dyn Morselizer>,
    context: Arc<TaskContext>,
    queue: Arc<Mutex<VecDeque<WorkItem>>>,
    state: MergeStreamState,
    schema: SchemaRef,
    baseline_metrics: BaselineMetrics,
}

impl Stream for MergeStream {
    type Item = Result<RecordBatch>;

    fn poll_next(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Self::Item>> {
        let this = &mut *self;

        loop {
            match &mut this.state {
                MergeStreamState::Idle => {
                    // Pop next work item from the shared queue
                    let item = this.queue.lock().unwrap().pop_front();
                    match item {
                        Some(item) => {
                            match this
                                .morselizer
                                .execute_item(item, Arc::clone(&this.context))
                            {
                                Ok(future) => {
                                    this.state = MergeStreamState::Opening { future };
                                }
                                Err(e) => {
                                    this.state = MergeStreamState::Done;
                                    return Poll::Ready(Some(Err(e)));
                                }
                            }
                        }
                        None => {
                            this.state = MergeStreamState::Done;
                            return Poll::Ready(None);
                        }
                    }
                }

                MergeStreamState::Opening { future } => {
                    match ready!(future.as_mut().poll(cx)) {
                        Ok(result) => {
                            // Push additional items to the shared queue
                            if !result.additional_items.is_empty() {
                                let mut queue = this.queue.lock().unwrap();
                                for item in result.additional_items {
                                    queue.push_back(item);
                                }
                            }
                            this.state = MergeStreamState::Scanning {
                                stream: result.stream,
                            };
                        }
                        Err(e) => {
                            this.state = MergeStreamState::Done;
                            return Poll::Ready(Some(Err(e)));
                        }
                    }
                }

                MergeStreamState::Scanning { stream } => {
                    match ready!(stream.poll_next_unpin(cx)) {
                        Some(Ok(batch)) => {
                            this.baseline_metrics.record_output(batch.num_rows());
                            return Poll::Ready(Some(Ok(batch)));
                        }
                        Some(Err(e)) => {
                            this.state = MergeStreamState::Done;
                            return Poll::Ready(Some(Err(e)));
                        }
                        None => {
                            // Current morsel exhausted, get next
                            this.state = MergeStreamState::Idle;
                        }
                    }
                }

                MergeStreamState::Done => {
                    return Poll::Ready(None);
                }
            }
        }
    }
}

impl RecordBatchStream for MergeStream {
    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::common;
    use crate::test;

    #[tokio::test]
    async fn test_merge_10_to_3() -> Result<()> {
        let task_ctx = Arc::new(TaskContext::default());

        let num_input_partitions = 10;
        let num_output_partitions = 3;
        let input = test::scan_partitioned(num_input_partitions);

        assert_eq!(
            input.output_partitioning().partition_count(),
            num_input_partitions
        );

        let merge = MergePartitionsExec::new(input, num_output_partitions);

        assert_eq!(
            merge.properties().output_partitioning().partition_count(),
            num_output_partitions
        );

        // Collect all output partitions
        let mut total_rows = 0;
        for partition in 0..num_output_partitions {
            let stream = merge.execute(partition, Arc::clone(&task_ctx))?;
            let batches = common::collect(stream).await?;
            let rows: usize = batches.iter().map(|batch| batch.num_rows()).sum();
            total_rows += rows;
        }

        // 10 partitions x 100 rows each = 1000 total rows
        assert_eq!(total_rows, 1000);

        Ok(())
    }

    #[tokio::test]
    async fn test_merge_output_ge_input() -> Result<()> {
        let task_ctx = Arc::new(TaskContext::default());

        let num_input_partitions = 3;
        let num_output_partitions = 5;
        let input = test::scan_partitioned(num_input_partitions);

        let merge = MergePartitionsExec::new(input, num_output_partitions);

        assert_eq!(
            merge.properties().output_partitioning().partition_count(),
            num_output_partitions
        );

        let mut total_rows = 0;
        for partition in 0..num_output_partitions {
            let stream = merge.execute(partition, Arc::clone(&task_ctx))?;
            let batches = common::collect(stream).await?;
            let rows: usize = batches.iter().map(|batch| batch.num_rows()).sum();
            total_rows += rows;
        }

        // 3 partitions x 100 rows each = 300 total rows
        assert_eq!(total_rows, 300);

        Ok(())
    }

    #[tokio::test]
    async fn test_schema_preserved() -> Result<()> {
        let task_ctx = Arc::new(TaskContext::default());

        let input = test::scan_partitioned(4);
        let expected_schema = input.schema();

        let merge = MergePartitionsExec::new(input, 2);

        // Verify the operator schema matches input
        assert_eq!(merge.schema(), expected_schema);

        // Verify stream schema matches
        let stream = merge.execute(0, task_ctx)?;
        assert_eq!(stream.schema(), expected_schema);

        Ok(())
    }
}
