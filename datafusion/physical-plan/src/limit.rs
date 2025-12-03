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

//! Defines the LIMIT plan

use std::any::Any;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use super::metrics::{BaselineMetrics, ExecutionPlanMetricsSet, MetricsSet};
use super::{
    DisplayAs, ExecutionPlanProperties, PlanProperties, RecordBatchStream,
    SendableRecordBatchStream, Statistics,
};
use crate::execution_plan::{Boundedness, CardinalityEffect};
use crate::{DisplayFormatType, Distribution, ExecutionPlan, Partitioning};

use arrow::datatypes::SchemaRef;
use arrow::record_batch::RecordBatch;
use datafusion_common::{assert_eq_or_internal_err, internal_err, Result};
use datafusion_execution::TaskContext;

use futures::stream::{Stream, StreamExt};
use log::trace;

/// Limit execution plan
#[derive(Debug, Clone)]
pub struct GlobalLimitExec {
    /// Input execution plan
    input: Arc<dyn ExecutionPlan>,
    /// Number of rows to skip before fetch
    skip: usize,
    /// Maximum number of rows to fetch,
    /// `None` means fetching all rows
    fetch: Option<usize>,
    /// Execution metrics
    metrics: ExecutionPlanMetricsSet,
    cache: PlanProperties,
}

impl GlobalLimitExec {
    /// Create a new GlobalLimitExec
    pub fn new(input: Arc<dyn ExecutionPlan>, skip: usize, fetch: Option<usize>) -> Self {
        let cache = Self::compute_properties(&input);
        GlobalLimitExec {
            input,
            skip,
            fetch,
            metrics: ExecutionPlanMetricsSet::new(),
            cache,
        }
    }

    /// Input execution plan
    pub fn input(&self) -> &Arc<dyn ExecutionPlan> {
        &self.input
    }

    /// Number of rows to skip before fetch
    pub fn skip(&self) -> usize {
        self.skip
    }

    /// Maximum number of rows to fetch
    pub fn fetch(&self) -> Option<usize> {
        self.fetch
    }

    /// This function creates the cache object that stores the plan properties such as schema, equivalence properties, ordering, partitioning, etc.
    fn compute_properties(input: &Arc<dyn ExecutionPlan>) -> PlanProperties {
        PlanProperties::new(
            input.equivalence_properties().clone(), // Equivalence Properties
            Partitioning::UnknownPartitioning(1),   // Output Partitioning
            input.pipeline_behavior(),
            // Limit operations are always bounded since they output a finite number of rows
            Boundedness::Bounded,
        )
    }
}

impl DisplayAs for GlobalLimitExec {
    fn fmt_as(
        &self,
        t: DisplayFormatType,
        f: &mut std::fmt::Formatter,
    ) -> std::fmt::Result {
        match t {
            DisplayFormatType::Default | DisplayFormatType::Verbose => {
                write!(
                    f,
                    "GlobalLimitExec: skip={}, fetch={}",
                    self.skip,
                    self.fetch
                        .map_or_else(|| "None".to_string(), |x| x.to_string())
                )
            }
            DisplayFormatType::TreeRender => {
                if let Some(fetch) = self.fetch {
                    writeln!(f, "limit={fetch}")?;
                }
                write!(f, "skip={}", self.skip)
            }
        }
    }
}

impl ExecutionPlan for GlobalLimitExec {
    fn name(&self) -> &'static str {
        "GlobalLimitExec"
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

    fn required_input_distribution(&self) -> Vec<Distribution> {
        vec![Distribution::SinglePartition]
    }

    fn maintains_input_order(&self) -> Vec<bool> {
        vec![true]
    }

    fn benefits_from_input_partitioning(&self) -> Vec<bool> {
        vec![false]
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        Ok(Arc::new(GlobalLimitExec::new(
            Arc::clone(&children[0]),
            self.skip,
            self.fetch,
        )))
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        trace!("Start GlobalLimitExec::execute for partition: {partition}");
        // GlobalLimitExec has a single output partition
        assert_eq_or_internal_err!(
            partition,
            0,
            "GlobalLimitExec invalid partition {partition}"
        );

        // GlobalLimitExec requires a single input partition
        assert_eq_or_internal_err!(
            self.input.output_partitioning().partition_count(),
            1,
            "GlobalLimitExec requires a single input partition"
        );

        let baseline_metrics = BaselineMetrics::new(&self.metrics, partition);
        let stream = self.input.execute(0, context)?;
        Ok(Box::pin(LimitStream::new(
            stream,
            self.skip,
            self.fetch,
            baseline_metrics,
        )))
    }

    fn metrics(&self) -> Option<MetricsSet> {
        Some(self.metrics.clone_inner())
    }

    fn statistics(&self) -> Result<Statistics> {
        self.partition_statistics(None)
    }

    fn partition_statistics(&self, partition: Option<usize>) -> Result<Statistics> {
        self.input
            .partition_statistics(partition)?
            .with_fetch(self.fetch, self.skip, 1)
    }

    fn fetch(&self) -> Option<usize> {
        self.fetch
    }

    fn supports_limit_pushdown(&self) -> bool {
        true
    }
}

/// LocalLimitExec applies a limit to a single partition
#[derive(Debug)]
pub struct LocalLimitExec {
    /// Input execution plan
    input: Arc<dyn ExecutionPlan>,
    /// Maximum number of rows to return
    fetch: usize,
    /// Execution metrics
    metrics: ExecutionPlanMetricsSet,
    cache: PlanProperties,
}

impl LocalLimitExec {
    /// Create a new LocalLimitExec partition
    pub fn new(input: Arc<dyn ExecutionPlan>, fetch: usize) -> Self {
        let cache = Self::compute_properties(&input);
        Self {
            input,
            fetch,
            metrics: ExecutionPlanMetricsSet::new(),
            cache,
        }
    }

    /// Input execution plan
    pub fn input(&self) -> &Arc<dyn ExecutionPlan> {
        &self.input
    }

    /// Maximum number of rows to fetch
    pub fn fetch(&self) -> usize {
        self.fetch
    }

    /// This function creates the cache object that stores the plan properties such as schema, equivalence properties, ordering, partitioning, etc.
    fn compute_properties(input: &Arc<dyn ExecutionPlan>) -> PlanProperties {
        PlanProperties::new(
            input.equivalence_properties().clone(), // Equivalence Properties
            input.output_partitioning().clone(),    // Output Partitioning
            input.pipeline_behavior(),
            // Limit operations are always bounded since they output a finite number of rows
            Boundedness::Bounded,
        )
    }
}

impl DisplayAs for LocalLimitExec {
    fn fmt_as(
        &self,
        t: DisplayFormatType,
        f: &mut std::fmt::Formatter,
    ) -> std::fmt::Result {
        match t {
            DisplayFormatType::Default | DisplayFormatType::Verbose => {
                write!(f, "LocalLimitExec: fetch={}", self.fetch)
            }
            DisplayFormatType::TreeRender => {
                write!(f, "limit={}", self.fetch)
            }
        }
    }
}

impl ExecutionPlan for LocalLimitExec {
    fn name(&self) -> &'static str {
        "LocalLimitExec"
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

    fn maintains_input_order(&self) -> Vec<bool> {
        vec![true]
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        match children.len() {
            1 => Ok(Arc::new(LocalLimitExec::new(
                Arc::clone(&children[0]),
                self.fetch,
            ))),
            _ => internal_err!("LocalLimitExec wrong number of children"),
        }
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        trace!("Start LocalLimitExec::execute for partition {} of context session_id {} and task_id {:?}", partition, context.session_id(), context.task_id());
        let baseline_metrics = BaselineMetrics::new(&self.metrics, partition);
        let stream = self.input.execute(partition, context)?;
        Ok(Box::pin(LimitStream::new(
            stream,
            0,
            Some(self.fetch),
            baseline_metrics,
        )))
    }

    fn metrics(&self) -> Option<MetricsSet> {
        Some(self.metrics.clone_inner())
    }

    fn statistics(&self) -> Result<Statistics> {
        self.partition_statistics(None)
    }

    fn partition_statistics(&self, partition: Option<usize>) -> Result<Statistics> {
        self.input
            .partition_statistics(partition)?
            .with_fetch(Some(self.fetch), 0, 1)
    }

    fn fetch(&self) -> Option<usize> {
        Some(self.fetch)
    }

    fn supports_limit_pushdown(&self) -> bool {
        true
    }

    fn cardinality_effect(&self) -> CardinalityEffect {
        CardinalityEffect::LowerEqual
    }
}

/// A Limit stream skips `skip` rows, and then fetch up to `fetch` rows.
pub struct LimitStream {
    /// The remaining number of rows to skip
    skip: usize,
    /// The remaining number of rows to produce
    fetch: usize,
    /// The input to read from. This is set to None once the limit is
    /// reached to enable early termination
    input: Option<SendableRecordBatchStream>,
    /// Copy of the input schema
    schema: SchemaRef,
    /// Execution time metrics
    baseline_metrics: BaselineMetrics,
}

impl LimitStream {
    pub fn new(
        input: SendableRecordBatchStream,
        skip: usize,
        fetch: Option<usize>,
        baseline_metrics: BaselineMetrics,
    ) -> Self {
        let schema = input.schema();
        Self {
            skip,
            fetch: fetch.unwrap_or(usize::MAX),
            input: Some(input),
            schema,
            baseline_metrics,
        }
    }

    fn poll_and_skip(
        &mut self,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Result<RecordBatch>>> {
        let input = self.input.as_mut().unwrap();
        loop {
            let poll = input.poll_next_unpin(cx);
            let poll = poll.map_ok(|batch| {
                if batch.num_rows() <= self.skip {
                    self.skip -= batch.num_rows();
                    RecordBatch::new_empty(input.schema())
                } else {
                    let new_batch = batch.slice(self.skip, batch.num_rows() - self.skip);
                    self.skip = 0;
                    new_batch
                }
            });

            match &poll {
                Poll::Ready(Some(Ok(batch))) => {
                    if batch.num_rows() > 0 {
                        break poll;
                    } else {
                        // Continue to poll input stream
                    }
                }
                Poll::Ready(Some(Err(_e))) => break poll,
                Poll::Ready(None) => break poll,
                Poll::Pending => break poll,
            }
        }
    }

    /// Fetches from the batch
    fn stream_limit(&mut self, batch: RecordBatch) -> Option<RecordBatch> {
        // records time on drop
        let _timer = self.baseline_metrics.elapsed_compute().timer();
        if self.fetch == 0 {
            self.input = None; // Clear input so it can be dropped early
            None
        } else if batch.num_rows() < self.fetch {
            //
            self.fetch -= batch.num_rows();
            Some(batch)
        } else if batch.num_rows() >= self.fetch {
            let batch_rows = self.fetch;
            self.fetch = 0;
            self.input = None; // Clear input so it can be dropped early

            // It is guaranteed that batch_rows is <= batch.num_rows
            Some(batch.slice(0, batch_rows))
        } else {
            unreachable!()
        }
    }
}

impl Stream for LimitStream {
    type Item = Result<RecordBatch>;

    fn poll_next(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Self::Item>> {
        let fetch_started = self.skip == 0;
        let poll = match &mut self.input {
            Some(input) => {
                let poll = if fetch_started {
                    input.poll_next_unpin(cx)
                } else {
                    self.poll_and_skip(cx)
                };

                poll.map(|x| match x {
                    Some(Ok(batch)) => Ok(self.stream_limit(batch)).transpose(),
                    other => other,
                })
            }
            // Input has been cleared
            None => Poll::Ready(None),
        };

        self.baseline_metrics.record_poll(poll)
    }
}

impl RecordBatchStream for LimitStream {
    /// Get the schema
    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::coalesce_partitions::CoalescePartitionsExec;
    use crate::common::collect;
    use crate::test;

    use crate::aggregates::{AggregateExec, AggregateMode, PhysicalGroupBy};
    use arrow::array::RecordBatchOptions;
    use arrow::datatypes::Schema;
    use datafusion_common::stats::Precision;
    use datafusion_physical_expr::expressions::col;
    use datafusion_physical_expr::PhysicalExpr;

    #[tokio::test]
    async fn limit() -> Result<()> {
        let task_ctx = Arc::new(TaskContext::default());

        let num_partitions = 4;
        let csv = test::scan_partitioned(num_partitions);

        // Input should have 4 partitions
        assert_eq!(csv.output_partitioning().partition_count(), num_partitions);

        let limit =
            GlobalLimitExec::new(Arc::new(CoalescePartitionsExec::new(csv)), 0, Some(7));

        // The result should contain 4 batches (one per input partition)
        let iter = limit.execute(0, task_ctx)?;
        let batches = collect(iter).await?;

        // There should be a total of 100 rows
        let row_count: usize = batches.iter().map(|batch| batch.num_rows()).sum();
        assert_eq!(row_count, 7);

        Ok(())
    }

    #[tokio::test]
    async fn limit_early_shutdown() -> Result<()> {
        let batches = vec![
            test::make_partition(5),
            test::make_partition(10),
            test::make_partition(15),
            test::make_partition(20),
            test::make_partition(25),
        ];
        let input = test::exec::TestStream::new(batches);

        let index = input.index();
        assert_eq!(index.value(), 0);

        // Limit of six needs to consume the entire first record batch
        // (5 rows) and 1 row from the second (1 row)
        let baseline_metrics = BaselineMetrics::new(&ExecutionPlanMetricsSet::new(), 0);
        let limit_stream =
            LimitStream::new(Box::pin(input), 0, Some(6), baseline_metrics);
        assert_eq!(index.value(), 0);

        let results = collect(Box::pin(limit_stream)).await.unwrap();
        let num_rows: usize = results.into_iter().map(|b| b.num_rows()).sum();
        // Only 6 rows should have been produced
        assert_eq!(num_rows, 6);

        // Only the first two batches should be consumed
        assert_eq!(index.value(), 2);

        Ok(())
    }

    #[tokio::test]
    async fn limit_equals_batch_size() -> Result<()> {
        let batches = vec![
            test::make_partition(6),
            test::make_partition(6),
            test::make_partition(6),
        ];
        let input = test::exec::TestStream::new(batches);

        let index = input.index();
        assert_eq!(index.value(), 0);

        // Limit of six needs to consume the entire first record batch
        // (6 rows) and stop immediately
        let baseline_metrics = BaselineMetrics::new(&ExecutionPlanMetricsSet::new(), 0);
        let limit_stream =
            LimitStream::new(Box::pin(input), 0, Some(6), baseline_metrics);
        assert_eq!(index.value(), 0);

        let results = collect(Box::pin(limit_stream)).await.unwrap();
        let num_rows: usize = results.into_iter().map(|b| b.num_rows()).sum();
        // Only 6 rows should have been produced
        assert_eq!(num_rows, 6);

        // Only the first batch should be consumed
        assert_eq!(index.value(), 1);

        Ok(())
    }

    #[tokio::test]
    async fn limit_no_column() -> Result<()> {
        let batches = vec![
            make_batch_no_column(6),
            make_batch_no_column(6),
            make_batch_no_column(6),
        ];
        let input = test::exec::TestStream::new(batches);

        let index = input.index();
        assert_eq!(index.value(), 0);

        // Limit of six needs to consume the entire first record batch
        // (6 rows) and stop immediately
        let baseline_metrics = BaselineMetrics::new(&ExecutionPlanMetricsSet::new(), 0);
        let limit_stream =
            LimitStream::new(Box::pin(input), 0, Some(6), baseline_metrics);
        assert_eq!(index.value(), 0);

        let results = collect(Box::pin(limit_stream)).await.unwrap();
        let num_rows: usize = results.into_iter().map(|b| b.num_rows()).sum();
        // Only 6 rows should have been produced
        assert_eq!(num_rows, 6);

        // Only the first batch should be consumed
        assert_eq!(index.value(), 1);

        Ok(())
    }

    // Test cases for "skip"
    async fn skip_and_fetch(skip: usize, fetch: Option<usize>) -> Result<usize> {
        let task_ctx = Arc::new(TaskContext::default());

        // 4 partitions @ 100 rows apiece
        let num_partitions = 4;
        let csv = test::scan_partitioned(num_partitions);

        assert_eq!(csv.output_partitioning().partition_count(), num_partitions);

        let offset =
            GlobalLimitExec::new(Arc::new(CoalescePartitionsExec::new(csv)), skip, fetch);

        // The result should contain 4 batches (one per input partition)
        let iter = offset.execute(0, task_ctx)?;
        let batches = collect(iter).await?;
        Ok(batches.iter().map(|batch| batch.num_rows()).sum())
    }

    #[tokio::test]
    async fn skip_none_fetch_none() -> Result<()> {
        let row_count = skip_and_fetch(0, None).await?;
        assert_eq!(row_count, 400);
        Ok(())
    }

    #[tokio::test]
    async fn skip_none_fetch_50() -> Result<()> {
        let row_count = skip_and_fetch(0, Some(50)).await?;
        assert_eq!(row_count, 50);
        Ok(())
    }

    #[tokio::test]
    async fn skip_3_fetch_none() -> Result<()> {
        // There are total of 400 rows, we skipped 3 rows (offset = 3)
        let row_count = skip_and_fetch(3, None).await?;
        assert_eq!(row_count, 397);
        Ok(())
    }

    #[tokio::test]
    async fn skip_3_fetch_10_stats() -> Result<()> {
        // There are total of 100 rows, we skipped 3 rows (offset = 3)
        let row_count = skip_and_fetch(3, Some(10)).await?;
        assert_eq!(row_count, 10);
        Ok(())
    }

    #[tokio::test]
    async fn skip_400_fetch_none() -> Result<()> {
        let row_count = skip_and_fetch(400, None).await?;
        assert_eq!(row_count, 0);
        Ok(())
    }

    #[tokio::test]
    async fn skip_400_fetch_1() -> Result<()> {
        // There are a total of 400 rows
        let row_count = skip_and_fetch(400, Some(1)).await?;
        assert_eq!(row_count, 0);
        Ok(())
    }

    #[tokio::test]
    async fn skip_401_fetch_none() -> Result<()> {
        // There are total of 400 rows, we skipped 401 rows (offset = 3)
        let row_count = skip_and_fetch(401, None).await?;
        assert_eq!(row_count, 0);
        Ok(())
    }

    #[tokio::test]
    async fn test_row_number_statistics_for_global_limit() -> Result<()> {
        let row_count = row_number_statistics_for_global_limit(0, Some(10)).await?;
        assert_eq!(row_count, Precision::Exact(10));

        let row_count = row_number_statistics_for_global_limit(5, Some(10)).await?;
        assert_eq!(row_count, Precision::Exact(10));

        let row_count = row_number_statistics_for_global_limit(400, Some(10)).await?;
        assert_eq!(row_count, Precision::Exact(0));

        let row_count = row_number_statistics_for_global_limit(398, Some(10)).await?;
        assert_eq!(row_count, Precision::Exact(2));

        let row_count = row_number_statistics_for_global_limit(398, Some(1)).await?;
        assert_eq!(row_count, Precision::Exact(1));

        let row_count = row_number_statistics_for_global_limit(398, None).await?;
        assert_eq!(row_count, Precision::Exact(2));

        let row_count =
            row_number_statistics_for_global_limit(0, Some(usize::MAX)).await?;
        assert_eq!(row_count, Precision::Exact(400));

        let row_count =
            row_number_statistics_for_global_limit(398, Some(usize::MAX)).await?;
        assert_eq!(row_count, Precision::Exact(2));

        let row_count =
            row_number_inexact_statistics_for_global_limit(0, Some(10)).await?;
        assert_eq!(row_count, Precision::Inexact(10));

        let row_count =
            row_number_inexact_statistics_for_global_limit(5, Some(10)).await?;
        assert_eq!(row_count, Precision::Inexact(10));

        let row_count =
            row_number_inexact_statistics_for_global_limit(400, Some(10)).await?;
        assert_eq!(row_count, Precision::Exact(0));

        let row_count =
            row_number_inexact_statistics_for_global_limit(398, Some(10)).await?;
        assert_eq!(row_count, Precision::Inexact(2));

        let row_count =
            row_number_inexact_statistics_for_global_limit(398, Some(1)).await?;
        assert_eq!(row_count, Precision::Inexact(1));

        let row_count = row_number_inexact_statistics_for_global_limit(398, None).await?;
        assert_eq!(row_count, Precision::Inexact(2));

        let row_count =
            row_number_inexact_statistics_for_global_limit(0, Some(usize::MAX)).await?;
        assert_eq!(row_count, Precision::Inexact(400));

        let row_count =
            row_number_inexact_statistics_for_global_limit(398, Some(usize::MAX)).await?;
        assert_eq!(row_count, Precision::Inexact(2));

        Ok(())
    }

    #[tokio::test]
    async fn test_row_number_statistics_for_local_limit() -> Result<()> {
        let row_count = row_number_statistics_for_local_limit(4, 10).await?;
        assert_eq!(row_count, Precision::Exact(10));

        Ok(())
    }

    async fn row_number_statistics_for_global_limit(
        skip: usize,
        fetch: Option<usize>,
    ) -> Result<Precision<usize>> {
        let num_partitions = 4;
        let csv = test::scan_partitioned(num_partitions);

        assert_eq!(csv.output_partitioning().partition_count(), num_partitions);

        let offset =
            GlobalLimitExec::new(Arc::new(CoalescePartitionsExec::new(csv)), skip, fetch);

        Ok(offset.partition_statistics(None)?.num_rows)
    }

    pub fn build_group_by(
        input_schema: &SchemaRef,
        columns: Vec<String>,
    ) -> PhysicalGroupBy {
        let mut group_by_expr: Vec<(Arc<dyn PhysicalExpr>, String)> = vec![];
        for column in columns.iter() {
            group_by_expr.push((col(column, input_schema).unwrap(), column.to_string()));
        }
        PhysicalGroupBy::new_single(group_by_expr.clone())
    }

    async fn row_number_inexact_statistics_for_global_limit(
        skip: usize,
        fetch: Option<usize>,
    ) -> Result<Precision<usize>> {
        let num_partitions = 4;
        let csv = test::scan_partitioned(num_partitions);

        assert_eq!(csv.output_partitioning().partition_count(), num_partitions);

        // Adding a "GROUP BY i" changes the input stats from Exact to Inexact.
        let agg = AggregateExec::try_new(
            AggregateMode::Final,
            build_group_by(&csv.schema(), vec!["i".to_string()]),
            vec![],
            vec![],
            Arc::clone(&csv),
            Arc::clone(&csv.schema()),
        )?;
        let agg_exec: Arc<dyn ExecutionPlan> = Arc::new(agg);

        let offset = GlobalLimitExec::new(
            Arc::new(CoalescePartitionsExec::new(agg_exec)),
            skip,
            fetch,
        );

        Ok(offset.partition_statistics(None)?.num_rows)
    }

    async fn row_number_statistics_for_local_limit(
        num_partitions: usize,
        fetch: usize,
    ) -> Result<Precision<usize>> {
        let csv = test::scan_partitioned(num_partitions);

        assert_eq!(csv.output_partitioning().partition_count(), num_partitions);

        let offset = LocalLimitExec::new(csv, fetch);

        Ok(offset.partition_statistics(None)?.num_rows)
    }

    /// Return a RecordBatch with a single array with row_count sz
    fn make_batch_no_column(sz: usize) -> RecordBatch {
        let schema = Arc::new(Schema::empty());

        let options = RecordBatchOptions::new().with_row_count(Option::from(sz));
        RecordBatch::try_new_with_options(schema, vec![], &options).unwrap()
    }
}
