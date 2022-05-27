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

//! Defines the OFFSET plan

use crate::execution::context::TaskContext;
use arrow::array::ArrayRef;
use arrow::array::UInt64Array;
use arrow::compute::take;
use arrow::datatypes::SchemaRef;
use arrow::error::Result as ArrowResult;
use arrow::record_batch::RecordBatch;

use datafusion_physical_expr::PhysicalSortExpr;
use std::any::Any;
use std::fmt::Formatter;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use futures::stream::{Stream, StreamExt};
use log::debug;

use super::metrics::ExecutionPlanMetricsSet;
use crate::error::{DataFusionError, Result};
use crate::physical_plan::metrics::{BaselineMetrics, MetricsSet};
use crate::physical_plan::{
    DisplayFormatType, Distribution, ExecutionPlan, Partitioning, RecordBatchStream,
    SendableRecordBatchStream, Statistics,
};

/// Offset execution plan
#[derive(Debug)]
pub struct OffsetExec {
    /// Input execution plan
    input: Arc<dyn ExecutionPlan>,
    /// Number of rows to skip
    offset: usize,
    /// Execution metrics
    metrics: ExecutionPlanMetricsSet,
}

impl OffsetExec {
    /// Create a new OffsetExec
    pub fn new(input: Arc<dyn ExecutionPlan>, offset: usize) -> Self {
        OffsetExec {
            input,
            offset,
            metrics: ExecutionPlanMetricsSet::new(),
        }
    }

    /// Input execution plan
    pub fn input(&self) -> &Arc<dyn ExecutionPlan> {
        &self.input
    }

    /// Number of rows to ignore
    pub fn offset(&self) -> usize {
        self.offset
    }
}

impl ExecutionPlan for OffsetExec {
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
        Ok(Arc::new(OffsetExec::new(children[0].clone(), self.offset)))
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        debug!("Start OffsetExec::execute for partition: {}", partition);

        if 0 != partition {
            return Err(DataFusionError::Internal(format!(
                "OffsetExec invalid partition {}",
                partition
            )));
        }

        if 1 != self.input.output_partitioning().partition_count() {
            return Err(DataFusionError::Internal(
                "OffsetExec requires a single input partition".to_owned(),
            ));
        }

        let baseline_metrics = BaselineMetrics::new(&self.metrics, partition);
        let stream = self.input.execute(0, context)?;
        Ok(Box::pin(OffsetStream::new(
            stream,
            self.offset,
            baseline_metrics,
        )))
    }

    fn fmt_as(&self, t: DisplayFormatType, f: &mut Formatter) -> std::fmt::Result {
        match t {
            DisplayFormatType::Default => {
                write!(f, "OffsetExec: offset={}", self.offset)
            }
        }
    }

    fn metrics(&self) -> Option<MetricsSet> {
        Some(self.metrics.clone_inner())
    }

    fn statistics(&self) -> Statistics {
        let input_stats = self.input.statistics();
        match input_stats {
            Statistics {
                num_rows: Some(nr), ..
            } => Statistics {
                num_rows: Some(nr - self.offset),
                is_exact: input_stats.is_exact,
                ..Default::default()
            },
            _ => Statistics::default(),
        }
    }
}

/// An Offset stream skip the input stream's data up to `offset` row.
pub struct OffsetStream {
    /// Number of rows to skip, starts with 1.
    offset: usize,
    input: SendableRecordBatchStream,
    schema: SchemaRef,
    /// Number of rows have already skipped.
    skipped: usize,
    /// Execution time metrics
    baseline_metrics: BaselineMetrics,
}

impl OffsetStream {
    fn new(
        input: SendableRecordBatchStream,
        offset: usize,
        baseline_metrics: BaselineMetrics,
    ) -> Self {
        let schema = input.schema();
        Self {
            offset,
            input,
            schema,
            skipped: 0,
            baseline_metrics,
        }
    }

    fn poll_and_skip(
        &mut self,
        cx: &mut Context<'_>,
    ) -> Poll<Option<ArrowResult<RecordBatch>>> {
        loop {
            let poll = self.input.poll_next_unpin(cx);
            let poll = poll.map_ok(|batch| {
                if batch.num_rows() + self.skipped <= self.offset {
                    self.skipped += batch.num_rows();
                    RecordBatch::new_empty(self.input.schema())
                } else {
                    let new_batch = cut_batch(&batch, self.offset - self.skipped);
                    self.skipped = self.offset;
                    new_batch
                }
            });

            match &poll {
                Poll::Ready(Some(Ok(batch)))
                    if batch.num_rows() > 0 && self.skipped == self.offset =>
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
}

/// Remove a RecordBatch's first n rows
pub fn cut_batch(batch: &RecordBatch, n: usize) -> RecordBatch {
    // todo
    // cast
    // unwrap
    let indices = UInt64Array::from_iter_values(n as u64..batch.num_rows() as u64);
    let columns: Vec<ArrayRef> = (0..batch.num_columns())
        .map(|i| take(batch.column(i), &indices, None).unwrap())
        .collect();
    RecordBatch::try_new(batch.schema(), columns).unwrap()
}

impl Stream for OffsetStream {
    type Item = ArrowResult<RecordBatch>;

    fn poll_next(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Self::Item>> {
        if self.skipped == self.offset {
            let poll = self.input.poll_next_unpin(cx);
            return self.baseline_metrics.record_poll(poll);
        }

        let loop_pool = self.poll_and_skip(cx);
        self.baseline_metrics.record_poll(loop_pool)
    }
}

impl RecordBatchStream for OffsetStream {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::physical_plan::coalesce_partitions::CoalescePartitionsExec;
    use crate::physical_plan::common;
    use crate::prelude::SessionContext;
    use crate::test;

    async fn offset_with_value(offset: usize) -> Result<usize> {
        let session_ctx = SessionContext::new();
        let task_ctx = session_ctx.task_ctx();

        let num_partitions = 4;
        let csv = test::scan_partitioned_csv(num_partitions)?;

        assert_eq!(csv.output_partitioning().partition_count(), num_partitions);

        let offset = OffsetExec::new(Arc::new(CoalescePartitionsExec::new(csv)), offset);

        // the result should contain 4 batches (one per input partition)
        let iter = offset.execute(0, task_ctx)?;
        let batches = common::collect(iter).await?;
        Ok(batches.iter().map(|batch| batch.num_rows()).sum())
    }

    #[tokio::test]
    async fn enough_to_skip() -> Result<()> {
        // there are total of 100 rows, we skipped 3 rows (offset = 3)
        let row_count = offset_with_value(3).await?;
        assert_eq!(row_count, 97);
        Ok(())
    }

    #[tokio::test]
    async fn not_enough_to_skip() -> Result<()> {
        // there are total of 100 rows, we skipped 101 rows (offset = 3)
        let row_count = offset_with_value(101).await?;
        assert_eq!(row_count, 0);
        Ok(())
    }
}
