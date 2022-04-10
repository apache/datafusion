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

use futures::stream::Stream;
use futures::stream::StreamExt;

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
use async_trait::async_trait;

/// Limit execution plan
#[derive(Debug)]
pub struct GlobalLimitExec {
    /// Input execution plan
    input: Arc<dyn ExecutionPlan>,
    /// Maximum number of rows to return
    limit: usize,
    /// Execution metrics
    metrics: ExecutionPlanMetricsSet,
}

impl GlobalLimitExec {
    /// Create a new GlobalLimitExec
    pub fn new(input: Arc<dyn ExecutionPlan>, limit: usize) -> Self {
        GlobalLimitExec {
            input,
            limit,
            metrics: ExecutionPlanMetricsSet::new(),
        }
    }

    /// Input execution plan
    pub fn input(&self) -> &Arc<dyn ExecutionPlan> {
        &self.input
    }

    /// Maximum number of rows to return
    pub fn limit(&self) -> usize {
        self.limit
    }
}

#[async_trait]
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
            self.limit,
        )))
    }

    async fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
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
        let stream = self.input.execute(0, context).await?;
        Ok(Box::pin(LimitStream::new(
            stream,
            self.limit,
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
                write!(f, "GlobalLimitExec: limit={}", self.limit)
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
            } if nr <= self.limit => input_stats,
            // if the input is greater than the limit, the num_row will be the limit
            // but we won't be able to predict the other statistics
            Statistics {
                num_rows: Some(nr), ..
            } if nr > self.limit => Statistics {
                num_rows: Some(self.limit),
                is_exact: input_stats.is_exact,
                ..Default::default()
            },
            // if we don't know the input size, we can't predict the limit's behaviour
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
    limit: usize,
    /// Execution metrics
    metrics: ExecutionPlanMetricsSet,
}

impl LocalLimitExec {
    /// Create a new LocalLimitExec partition
    pub fn new(input: Arc<dyn ExecutionPlan>, limit: usize) -> Self {
        Self {
            input,
            limit,
            metrics: ExecutionPlanMetricsSet::new(),
        }
    }

    /// Input execution plan
    pub fn input(&self) -> &Arc<dyn ExecutionPlan> {
        &self.input
    }

    /// Maximum number of rows to return
    pub fn limit(&self) -> usize {
        self.limit
    }
}

#[async_trait]
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
                self.limit,
            ))),
            _ => Err(DataFusionError::Internal(
                "LocalLimitExec wrong number of children".to_string(),
            )),
        }
    }

    async fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        let baseline_metrics = BaselineMetrics::new(&self.metrics, partition);
        let stream = self.input.execute(partition, context).await?;
        Ok(Box::pin(LimitStream::new(
            stream,
            self.limit,
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
                write!(f, "LocalLimitExec: limit={}", self.limit)
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
            } if nr <= self.limit => input_stats,
            // if the input is greater than the limit, the num_row will be greater
            // than the limit because the partitions will be limited separatly
            // the statistic
            Statistics {
                num_rows: Some(nr), ..
            } if nr > self.limit => Statistics {
                num_rows: Some(self.limit),
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

/// A Limit stream limits the stream to up to `limit` rows.
struct LimitStream {
    /// The maximum number of rows to produce
    limit: usize,
    /// The input to read from. This is set to None once the limit is
    /// reached to enable early termination
    input: Option<SendableRecordBatchStream>,
    /// Copy of the input schema
    schema: SchemaRef,
    // the current number of rows which have been produced
    current_len: usize,
    /// Execution time metrics
    baseline_metrics: BaselineMetrics,
}

impl LimitStream {
    fn new(
        input: SendableRecordBatchStream,
        limit: usize,
        baseline_metrics: BaselineMetrics,
    ) -> Self {
        let schema = input.schema();
        Self {
            limit,
            input: Some(input),
            schema,
            current_len: 0,
            baseline_metrics,
        }
    }

    fn stream_limit(&mut self, batch: RecordBatch) -> Option<RecordBatch> {
        // records time on drop
        let _timer = self.baseline_metrics.elapsed_compute().timer();
        if self.current_len == self.limit {
            self.input = None; // clear input so it can be dropped early
            None
        } else if self.current_len + batch.num_rows() <= self.limit {
            self.current_len += batch.num_rows();
            Some(batch)
        } else {
            let batch_rows = self.limit - self.current_len;
            self.current_len = self.limit;
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
        let poll = match &mut self.input {
            Some(input) => input.poll_next_unpin(cx).map(|x| match x {
                Some(Ok(batch)) => Ok(self.stream_limit(batch)).transpose(),
                other => other,
            }),
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
    use crate::datafusion_data_access::object_store::local::LocalFileSystem;
    use crate::physical_plan::coalesce_partitions::CoalescePartitionsExec;
    use crate::physical_plan::common;
    use crate::physical_plan::file_format::{CsvExec, FileScanConfig};
    use crate::prelude::SessionContext;
    use crate::{test, test_util};

    #[tokio::test]
    async fn limit() -> Result<()> {
        let session_ctx = SessionContext::new();
        let task_ctx = session_ctx.task_ctx();
        let schema = test_util::aggr_test_schema();

        let num_partitions = 4;
        let (_, files) =
            test::create_partitioned_csv("aggregate_test_100.csv", num_partitions)?;

        let csv = CsvExec::new(
            FileScanConfig {
                object_store: Arc::new(LocalFileSystem {}),
                file_schema: schema,
                file_groups: files,
                statistics: Statistics::default(),
                projection: None,
                limit: None,
                table_partition_cols: vec![],
            },
            true,
            b',',
        );

        // input should have 4 partitions
        assert_eq!(csv.output_partitioning().partition_count(), num_partitions);

        let limit =
            GlobalLimitExec::new(Arc::new(CoalescePartitionsExec::new(Arc::new(csv))), 7);

        // the result should contain 4 batches (one per input partition)
        let iter = limit.execute(0, task_ctx).await?;
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
        let limit_stream = LimitStream::new(Box::pin(input), 6, baseline_metrics);
        assert_eq!(index.value(), 0);

        let results = collect(Box::pin(limit_stream)).await.unwrap();
        let num_rows: usize = results.into_iter().map(|b| b.num_rows()).sum();
        // Only 6 rows should have been produced
        assert_eq!(num_rows, 6);

        // Only the first two batches should be consumed
        assert_eq!(index.value(), 2);

        Ok(())
    }
}
