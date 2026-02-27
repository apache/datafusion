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

//! MergePartitionsExec maps N input partitions to M output partitions using
//! an atomic counter for work-stealing. Each output stream atomically claims
//! the next input partition, executes it, yields all batches, then claims
//! the next. No channels needed.

use std::any::Any;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
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

use futures::{Stream, ready};

/// Maps N input partitions to M output partitions using work-stealing.
///
/// Each output stream atomically claims the next unclaimed input partition,
/// executes it to completion (yielding all batches), then claims the next.
/// This provides natural load balancing via Tokio's task scheduler without
/// requiring channels or background tasks.
#[derive(Debug)]
pub struct MergePartitionsExec {
    input: Arc<dyn ExecutionPlan>,
    output_partitions: usize,
    next_partition: Arc<AtomicUsize>,
    metrics: ExecutionPlanMetricsSet,
    cache: Arc<PlanProperties>,
}

impl Clone for MergePartitionsExec {
    fn clone(&self) -> Self {
        Self {
            input: Arc::clone(&self.input),
            output_partitions: self.output_partitions,
            next_partition: Arc::new(AtomicUsize::new(0)),
            metrics: ExecutionPlanMetricsSet::new(),
            cache: Arc::clone(&self.cache),
        }
    }
}

impl MergePartitionsExec {
    /// Create a new MergePartitionsExec that maps the input's partitions
    /// to `output_partitions` output partitions.
    pub fn new(input: Arc<dyn ExecutionPlan>, output_partitions: usize) -> Self {
        let cache = Self::compute_properties(&input, output_partitions);
        MergePartitionsExec {
            input,
            output_partitions,
            next_partition: Arc::new(AtomicUsize::new(0)),
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

    fn with_new_children_and_same_properties(
        &self,
        mut children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Self {
        Self {
            input: children.swap_remove(0),
            metrics: ExecutionPlanMetricsSet::new(),
            next_partition: Arc::new(AtomicUsize::new(0)),
            ..Self::clone(self)
        }
    }
}

impl DisplayAs for MergePartitionsExec {
    fn fmt_as(
        &self,
        t: DisplayFormatType,
        f: &mut std::fmt::Formatter,
    ) -> std::fmt::Result {
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
        let total_input_partitions = self.input.output_partitioning().partition_count();
        let baseline_metrics = BaselineMetrics::new(&self.metrics, partition);

        Ok(Box::pin(MergeStream {
            input: Arc::clone(&self.input),
            context,
            next_partition: Arc::clone(&self.next_partition),
            total_input_partitions,
            current_stream: None,
            schema: self.schema(),
            baseline_metrics,
            done: false,
        }))
    }

    fn metrics(&self) -> Option<MetricsSet> {
        Some(self.metrics.clone_inner())
    }

    fn partition_statistics(&self, _partition: Option<usize>) -> Result<Statistics> {
        // Can't predict which output partition gets which input data
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

struct MergeStream {
    input: Arc<dyn ExecutionPlan>,
    context: Arc<TaskContext>,
    next_partition: Arc<AtomicUsize>,
    total_input_partitions: usize,
    current_stream: Option<SendableRecordBatchStream>,
    schema: SchemaRef,
    baseline_metrics: BaselineMetrics,
    done: bool,
}

impl Stream for MergeStream {
    type Item = Result<RecordBatch>;

    fn poll_next(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Self::Item>> {
        let this = &mut *self;

        if this.done {
            return Poll::Ready(None);
        }

        loop {
            // If we have a current stream, poll it
            if let Some(stream) = &mut this.current_stream {
                let poll = stream.as_mut().poll_next(cx);
                match ready!(poll) {
                    Some(Ok(batch)) => {
                        this.baseline_metrics.record_output(batch.num_rows());
                        return Poll::Ready(Some(Ok(batch)));
                    }
                    Some(Err(e)) => {
                        this.done = true;
                        return Poll::Ready(Some(Err(e)));
                    }
                    None => {
                        // Current stream exhausted, drop it and try next
                        this.current_stream = None;
                    }
                }
            }

            // Claim next input partition atomically
            let partition_idx = this.next_partition.fetch_add(1, Ordering::Relaxed);
            if partition_idx >= this.total_input_partitions {
                this.done = true;
                return Poll::Ready(None);
            }

            // Start new input stream (synchronous, lazy — actual I/O on poll)
            match this.input.execute(partition_idx, Arc::clone(&this.context)) {
                Ok(stream) => {
                    this.current_stream = Some(stream);
                    // Loop back to poll the new stream
                }
                Err(e) => {
                    this.done = true;
                    return Poll::Ready(Some(Err(e)));
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

        // 10 partitions × 100 rows each = 1000 total rows
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

        // 3 partitions × 100 rows each = 300 total rows
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
