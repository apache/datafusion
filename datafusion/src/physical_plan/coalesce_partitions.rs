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
use std::task::Poll;

use futures::channel::mpsc;
use futures::Stream;

use async_trait::async_trait;

use arrow::record_batch::RecordBatch;
use arrow::{datatypes::SchemaRef, error::Result as ArrowResult};

use super::metrics::{BaselineMetrics, ExecutionPlanMetricsSet, MetricsSet};
use super::{RecordBatchStream, Statistics};
use crate::error::{DataFusionError, Result};
use crate::physical_plan::{DisplayFormatType, ExecutionPlan, Partitioning};

use super::SendableRecordBatchStream;
use crate::physical_plan::common::spawn_execution;
use pin_project_lite::pin_project;

/// Merge execution plan executes partitions in parallel and combines them into a single
/// partition. No guarantees are made about the order of the resulting partition.
#[derive(Debug)]
pub struct CoalescePartitionsExec {
    /// Input execution plan
    input: Arc<dyn ExecutionPlan>,
    /// Execution metrics
    metrics: ExecutionPlanMetricsSet,
}

impl CoalescePartitionsExec {
    /// Create a new CoalescePartitionsExec
    pub fn new(input: Arc<dyn ExecutionPlan>) -> Self {
        CoalescePartitionsExec {
            input,
            metrics: ExecutionPlanMetricsSet::new(),
        }
    }

    /// Input execution plan
    pub fn input(&self) -> &Arc<dyn ExecutionPlan> {
        &self.input
    }
}

#[async_trait]
impl ExecutionPlan for CoalescePartitionsExec {
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

    /// Get the output partitioning of this plan
    fn output_partitioning(&self) -> Partitioning {
        Partitioning::UnknownPartitioning(1)
    }

    fn with_new_children(
        &self,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        match children.len() {
            1 => Ok(Arc::new(CoalescePartitionsExec::new(children[0].clone()))),
            _ => Err(DataFusionError::Internal(
                "CoalescePartitionsExec wrong number of children".to_string(),
            )),
        }
    }

    async fn execute(&self, partition: usize) -> Result<SendableRecordBatchStream> {
        let baseline_metrics = BaselineMetrics::new(&self.metrics, partition);

        // CoalescePartitionsExec produces a single partition
        if 0 != partition {
            return Err(DataFusionError::Internal(format!(
                "CoalescePartitionsExec invalid partition {}",
                partition
            )));
        }

        let input_partitions = self.input.output_partitioning().partition_count();
        match input_partitions {
            0 => Err(DataFusionError::Internal(
                "CoalescePartitionsExec requires at least one input partition".to_owned(),
            )),
            1 => {
                // bypass any threading if there is a single partition
                self.input.execute(0).await
            }
            _ => {
                // use a stream that allows each sender to put in at
                // least one result in an attempt to maximize
                // parallelism.
                let (sender, receiver) =
                    mpsc::channel::<ArrowResult<RecordBatch>>(input_partitions);

                // spawn independent tasks whose resulting streams (of batches)
                // are sent to the channel for consumption.
                for part_i in 0..input_partitions {
                    spawn_execution(self.input.clone(), sender.clone(), part_i);
                }

                Ok(Box::pin(MergeStream {
                    input: receiver,
                    schema: self.schema(),
                    baseline_metrics,
                }))
            }
        }
    }

    fn fmt_as(
        &self,
        t: DisplayFormatType,
        f: &mut std::fmt::Formatter,
    ) -> std::fmt::Result {
        match t {
            DisplayFormatType::Default => {
                write!(f, "CoalescePartitionsExec")
            }
        }
    }

    fn metrics(&self) -> Option<MetricsSet> {
        Some(self.metrics.clone_inner())
    }

    fn statistics(&self) -> Statistics {
        self.input.statistics()
    }
}

pin_project! {
    struct MergeStream {
        schema: SchemaRef,
        #[pin]
        input: mpsc::Receiver<ArrowResult<RecordBatch>>,
        baseline_metrics: BaselineMetrics
    }
}

impl Stream for MergeStream {
    type Item = ArrowResult<RecordBatch>;

    fn poll_next(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> Poll<Option<Self::Item>> {
        let this = self.project();
        let poll = this.input.poll_next(cx);
        this.baseline_metrics.record_poll(poll)
    }
}

impl RecordBatchStream for MergeStream {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }
}

#[cfg(test)]
mod tests {

    use super::*;
    use crate::physical_plan::common;
    use crate::physical_plan::csv::{CsvExec, CsvReadOptions};
    use crate::test;

    #[tokio::test]
    async fn merge() -> Result<()> {
        let schema = test::aggr_test_schema();

        let num_partitions = 4;
        let path =
            test::create_partitioned_csv("aggregate_test_100.csv", num_partitions)?;

        let csv = CsvExec::try_new(
            &path,
            CsvReadOptions::new().schema(&schema),
            None,
            1024,
            None,
        )?;

        // input should have 4 partitions
        assert_eq!(csv.output_partitioning().partition_count(), num_partitions);

        let merge = CoalescePartitionsExec::new(Arc::new(csv));

        // output of CoalescePartitionsExec should have a single partition
        assert_eq!(merge.output_partitioning().partition_count(), 1);

        // the result should contain 4 batches (one per input partition)
        let iter = merge.execute(0).await?;
        let batches = common::collect(iter).await?;
        assert_eq!(batches.len(), num_partitions);

        // there should be a total of 100 rows
        let row_count: usize = batches.iter().map(|batch| batch.num_rows()).sum();
        assert_eq!(row_count, 100);

        Ok(())
    }
}
