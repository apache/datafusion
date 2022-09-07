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

use futures::stream::Stream;
use futures::stream::StreamExt;
use log::debug;
use std::any::Any;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use crate::error::{DataFusionError, Result};
use crate::physical_plan::{
    DisplayFormatType, Distribution, ExecutionPlan, Partitioning,
};
use arrow::array::ArrayRef;
use arrow::compute::limit;
use arrow::datatypes::SchemaRef;
use arrow::error::Result as ArrowResult;
use arrow::record_batch::RecordBatch;

use super::expressions::PhysicalSortExpr;
use super::{
    metrics::{BaselineMetrics, ExecutionPlanMetricsSet, MetricsSet},
    RecordBatchStream, SendableRecordBatchStream, Statistics,
};

use crate::execution::context::TaskContext;

/// Limit execution plan
#[derive(Debug)]
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
}

impl GlobalLimitExec {
    /// Create a new GlobalLimitExec
    pub fn new(input: Arc<dyn ExecutionPlan>, skip: usize, fetch: Option<usize>) -> Self {
        GlobalLimitExec {
            input,
            skip,
            fetch,
            metrics: ExecutionPlanMetricsSet::new(),
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
    pub fn fetch(&self) -> Option<&usize> {
        self.fetch.as_ref()
    }
}

impl ExecutionPlan for GlobalLimitExec {
    /// Return a reference to Any that can be used for downcasting
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.input.schema()
    }

    fn children(&self) -> Vec<Arc<dyn ExecutionPlan>> {
        vec![self.input.clone()]
    }

    fn required_child_distribution(&self) -> Distribution {
        Distribution::SinglePartition
    }

    /// Get the output partitioning of this plan
    fn output_partitioning(&self) -> Partitioning {
        Partitioning::UnknownPartitioning(1)
    }

    fn relies_on_input_order(&self) -> bool {
        self.input.output_ordering().is_some()
    }

    fn maintains_input_order(&self) -> bool {
        true
    }

    fn benefits_from_input_partitioning(&self) -> bool {
        false
    }

    fn output_ordering(&self) -> Option<&[PhysicalSortExpr]> {
        self.input.output_ordering()
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        Ok(Arc::new(GlobalLimitExec::new(
            children[0].clone(),
            self.skip,
            self.fetch,
        )))
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        debug!(
            "Start GlobalLimitExec::execute for partition: {}",
            partition
        );
        // GlobalLimitExec has a single output partition
        if 0 != partition {
            return Err(DataFusionError::Internal(format!(
                "GlobalLimitExec invalid partition {}",
                partition
            )));
        }

        // GlobalLimitExec requires a single input partition
        if 1 != self.input.output_partitioning().partition_count() {
            return Err(DataFusionError::Internal(
                "GlobalLimitExec requires a single input partition".to_owned(),
            ));
        }

        let baseline_metrics = BaselineMetrics::new(&self.metrics, partition);
        let stream = self.input.execute(0, context)?;
        Ok(Box::pin(LimitStream::new(
            stream,
            self.skip,
            self.fetch,
            baseline_metrics,
        )))
    }

    fn fmt_as(
        &self,
        t: DisplayFormatType,
        f: &mut std::fmt::Formatter,
    ) -> std::fmt::Result {
        match t {
            DisplayFormatType::Default => {
                write!(
                    f,
                    "GlobalLimitExec: skip={}, fetch={}",
                    self.skip,
                    self.fetch.map_or("None".to_string(), |x| x.to_string())
                )
            }
        }
    }

    fn metrics(&self) -> Option<MetricsSet> {
        Some(self.metrics.clone_inner())
    }

    fn statistics(&self) -> Statistics {
        let input_stats = self.input.statistics();
        let skip = self.skip;
        match input_stats {
            Statistics {
                num_rows: Some(nr), ..
            } => {
                if nr <= skip {
                    // if all input data will be skipped, return 0
                    Statistics {
                        num_rows: Some(0),
                        is_exact: input_stats.is_exact,
                        ..Default::default()
                    }
                } else if nr - skip <= self.fetch.unwrap_or(usize::MAX) {
                    // if the input does not reach the "fetch" globally, return input stats
                    input_stats
                } else if nr - skip > self.fetch.unwrap_or(usize::MAX) {
                    // if the input is greater than the "fetch", the num_row will be the "fetch",
                    // but we won't be able to predict the other statistics
                    Statistics {
                        num_rows: self.fetch,
                        is_exact: input_stats.is_exact,
                        ..Default::default()
                    }
                } else {
                    Statistics::default()
                }
            }
            _ => Statistics::default(),
        }
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
}

impl LocalLimitExec {
    /// Create a new LocalLimitExec partition
    pub fn new(input: Arc<dyn ExecutionPlan>, fetch: usize) -> Self {
        Self {
            input,
            fetch,
            metrics: ExecutionPlanMetricsSet::new(),
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
}

impl ExecutionPlan for LocalLimitExec {
    /// Return a reference to Any that can be used for downcasting
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.input.schema()
    }

    fn children(&self) -> Vec<Arc<dyn ExecutionPlan>> {
        vec![self.input.clone()]
    }

    fn output_partitioning(&self) -> Partitioning {
        self.input.output_partitioning()
    }

    fn relies_on_input_order(&self) -> bool {
        self.input.output_ordering().is_some()
    }

    fn benefits_from_input_partitioning(&self) -> bool {
        false
    }

    // Local limit does not make any attempt to maintain the input
    // sortedness (if there is more than one partition)
    fn output_ordering(&self) -> Option<&[PhysicalSortExpr]> {
        if self.output_partitioning().partition_count() == 1 {
            self.input.output_ordering()
        } else {
            None
        }
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        match children.len() {
            1 => Ok(Arc::new(LocalLimitExec::new(
                children[0].clone(),
                self.fetch,
            ))),
            _ => Err(DataFusionError::Internal(
                "LocalLimitExec wrong number of children".to_string(),
            )),
        }
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        debug!("Start LocalLimitExec::execute for partition {} of context session_id {} and task_id {:?}", partition, context.session_id(), context.task_id());
        let baseline_metrics = BaselineMetrics::new(&self.metrics, partition);
        let stream = self.input.execute(partition, context)?;
        Ok(Box::pin(LimitStream::new(
            stream,
            0,
            Some(self.fetch),
            baseline_metrics,
        )))
    }

    fn fmt_as(
        &self,
        t: DisplayFormatType,
        f: &mut std::fmt::Formatter,
    ) -> std::fmt::Result {
        match t {
            DisplayFormatType::Default => {
                write!(f, "LocalLimitExec: fetch={}", self.fetch)
            }
        }
    }

    fn metrics(&self) -> Option<MetricsSet> {
        Some(self.metrics.clone_inner())
    }

    fn statistics(&self) -> Statistics {
        let input_stats = self.input.statistics();
        match input_stats {
            // if the input does not reach the limit globally, return input stats
            Statistics {
                num_rows: Some(nr), ..
            } if nr <= self.fetch => input_stats,
            // if the input is greater than the limit, the num_row will be greater
            // than the limit because the partitions will be limited separatly
            // the statistic
            Statistics {
                num_rows: Some(nr), ..
            } if nr > self.fetch => Statistics {
                num_rows: Some(self.fetch),
                // this is not actually exact, but will be when GlobalLimit is applied
                // TODO stats: find a more explicit way to vehiculate this information
                is_exact: input_stats.is_exact,
                ..Default::default()
            },
            // if we don't know the input size, we can't predict the limit's behaviour
            _ => Statistics::default(),
        }
    }
}

/// Truncate a RecordBatch to maximum of n rows
pub fn truncate_batch(batch: &RecordBatch, n: usize) -> RecordBatch {
    let limited_columns: Vec<ArrayRef> = (0..batch.num_columns())
        .map(|i| limit(batch.column(i), n))
        .collect();

    RecordBatch::try_new(batch.schema(), limited_columns).unwrap()
}

/// A Limit stream skips `skip` rows, and then fetch up to `fetch` rows.
struct LimitStream {
    /// The number of rows to skip
    skip: usize,
    /// The maximum number of rows to produce, after `skip` are skipped
    fetch: usize,
    /// The input to read from. This is set to None once the limit is
    /// reached to enable early termination
    input: Option<SendableRecordBatchStream>,
    /// Copy of the input schema
    schema: SchemaRef,
    /// Number of rows have already skipped
    current_skipped: usize,
    /// the current number of rows which have been produced
    current_fetched: usize,
    /// Execution time metrics
    baseline_metrics: BaselineMetrics,
}

impl LimitStream {
    fn new(
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
            current_skipped: 0,
            current_fetched: 0,
            baseline_metrics,
        }
    }

    fn poll_and_skip(
        &mut self,
        cx: &mut Context<'_>,
    ) -> Poll<Option<ArrowResult<RecordBatch>>> {
        let input = self.input.as_mut().unwrap();
        loop {
            let poll = input.poll_next_unpin(cx);
            let poll = poll.map_ok(|batch| {
                if batch.num_rows() + self.current_skipped <= self.skip {
                    self.current_skipped += batch.num_rows();
                    RecordBatch::new_empty(input.schema())
                } else {
                    let offset = self.skip - self.current_skipped;
                    let new_batch = batch.slice(offset, batch.num_rows() - offset);
                    self.current_skipped = self.skip;
                    new_batch
                }
            });

            match &poll {
                Poll::Ready(Some(Ok(batch)))
                    if batch.num_rows() > 0 && self.current_skipped == self.skip =>
                {
                    break poll
                }
                Poll::Ready(Some(Err(_e))) => break poll,
                Poll::Ready(None) => break poll,
                Poll::Pending => break poll,
                _ => {
                    // continue to poll input stream
                }
            }
        }
    }

    fn stream_limit(&mut self, batch: RecordBatch) -> Option<RecordBatch> {
        // records time on drop
        let _timer = self.baseline_metrics.elapsed_compute().timer();
        if self.current_fetched == self.fetch {
            self.input = None; // clear input so it can be dropped early
            None
        } else if self.current_fetched + batch.num_rows() <= self.fetch {
            self.current_fetched += batch.num_rows();
            Some(batch)
        } else {
            let batch_rows = self.fetch - self.current_fetched;
            self.current_fetched = self.fetch;
            self.input = None; // clear input so it can be dropped early
            Some(truncate_batch(&batch, batch_rows))
        }
    }
}

impl Stream for LimitStream {
    type Item = ArrowResult<RecordBatch>;

    fn poll_next(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Self::Item>> {
        let fetch_started = self.current_skipped == self.skip;
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
            // input has been cleared
            None => Poll::Ready(None),
        };

        self.baseline_metrics.record_poll(poll)
    }
}

impl RecordBatchStream for LimitStream {
    /// Get the schema
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }
}

#[cfg(test)]
mod tests {

    use common::collect;

    use super::*;
    use crate::physical_plan::coalesce_partitions::CoalescePartitionsExec;
    use crate::physical_plan::common;
    use crate::prelude::SessionContext;
    use crate::test;

    #[tokio::test]
    async fn limit() -> Result<()> {
        let session_ctx = SessionContext::new();
        let task_ctx = session_ctx.task_ctx();

        let num_partitions = 4;
        let csv = test::scan_partitioned_csv(num_partitions)?;

        // input should have 4 partitions
        assert_eq!(csv.output_partitioning().partition_count(), num_partitions);

        let limit =
            GlobalLimitExec::new(Arc::new(CoalescePartitionsExec::new(csv)), 0, Some(7));

        // the result should contain 4 batches (one per input partition)
        let iter = limit.execute(0, task_ctx)?;
        let batches = common::collect(iter).await?;

        // there should be a total of 100 rows
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

        // limit of six needs to consume the entire first record batch
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

    // test cases for "skip"
    async fn skip_and_fetch(skip: usize, fetch: Option<usize>) -> Result<usize> {
        let session_ctx = SessionContext::new();
        let task_ctx = session_ctx.task_ctx();

        let num_partitions = 4;
        let csv = test::scan_partitioned_csv(num_partitions)?;

        assert_eq!(csv.output_partitioning().partition_count(), num_partitions);

        let offset =
            GlobalLimitExec::new(Arc::new(CoalescePartitionsExec::new(csv)), skip, fetch);

        // the result should contain 4 batches (one per input partition)
        let iter = offset.execute(0, task_ctx)?;
        let batches = common::collect(iter).await?;
        Ok(batches.iter().map(|batch| batch.num_rows()).sum())
    }

    #[tokio::test]
    async fn skip_none_fetch_none() -> Result<()> {
        let row_count = skip_and_fetch(0, None).await?;
        assert_eq!(row_count, 100);
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
        // there are total of 100 rows, we skipped 3 rows (offset = 3)
        let row_count = skip_and_fetch(3, None).await?;
        assert_eq!(row_count, 97);
        Ok(())
    }

    #[tokio::test]
    async fn skip_3_fetch_10() -> Result<()> {
        // there are total of 100 rows, we skipped 3 rows (offset = 3)
        let row_count = skip_and_fetch(3, Some(10)).await?;
        assert_eq!(row_count, 10);
        Ok(())
    }

    #[tokio::test]
    async fn skip_100_fetch_none() -> Result<()> {
        let row_count = skip_and_fetch(100, None).await?;
        assert_eq!(row_count, 0);
        Ok(())
    }

    #[tokio::test]
    async fn skip_100_fetch_1() -> Result<()> {
        let row_count = skip_and_fetch(100, Some(1)).await?;
        assert_eq!(row_count, 0);
        Ok(())
    }

    #[tokio::test]
    async fn skip_101_fetch_none() -> Result<()> {
        // there are total of 100 rows, we skipped 101 rows (offset = 3)
        let row_count = skip_and_fetch(101, None).await?;
        assert_eq!(row_count, 0);
        Ok(())
    }
}
