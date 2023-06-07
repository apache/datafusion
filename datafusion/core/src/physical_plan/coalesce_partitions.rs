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

use arrow::datatypes::SchemaRef;

use super::expressions::PhysicalSortExpr;
use super::metrics::{BaselineMetrics, ExecutionPlanMetricsSet, MetricsSet};
use super::stream::{ObservedStream, RecordBatchReceiverStream};
use super::Statistics;
use crate::error::{DataFusionError, Result};
use crate::physical_plan::{
    DisplayFormatType, EquivalenceProperties, ExecutionPlan, Partitioning,
};

use super::SendableRecordBatchStream;
use datafusion_execution::TaskContext;

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

    /// Specifies whether this plan generates an infinite stream of records.
    /// If the plan does not support pipelining, but its input(s) are
    /// infinite, returns an error to indicate this.
    fn unbounded_output(&self, children: &[bool]) -> Result<bool> {
        Ok(children[0])
    }
    /// Get the output partitioning of this plan
    fn output_partitioning(&self) -> Partitioning {
        Partitioning::UnknownPartitioning(1)
    }

    fn output_ordering(&self) -> Option<&[PhysicalSortExpr]> {
        None
    }

    fn equivalence_properties(&self) -> EquivalenceProperties {
        self.input.equivalence_properties()
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        Ok(Arc::new(CoalescePartitionsExec::new(children[0].clone())))
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        // CoalescePartitionsExec produces a single partition
        if 0 != partition {
            return Err(DataFusionError::Internal(format!(
                "CoalescePartitionsExec invalid partition {partition}"
            )));
        }

        let input_partitions = self.input.output_partitioning().partition_count();
        match input_partitions {
            0 => Err(DataFusionError::Internal(
                "CoalescePartitionsExec requires at least one input partition".to_owned(),
            )),
            1 => {
                // bypass any threading / metrics if there is a single partition
                self.input.execute(0, context)
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
                    builder.run_input(self.input.clone(), part_i, context.clone());
                }

                let stream = builder.build();
                Ok(Box::pin(ObservedStream::new(stream, baseline_metrics)))
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

#[cfg(test)]
mod tests {

    use arrow::datatypes::{DataType, Field, Schema};
    use futures::FutureExt;

    use super::*;
    use crate::physical_plan::{collect, common};
    use crate::prelude::SessionContext;
    use crate::test::exec::{
        assert_strong_count_converges_to_zero, BlockingExec, PanicExec,
    };
    use crate::test::{self, assert_is_pending};

    #[tokio::test]
    async fn merge() -> Result<()> {
        let session_ctx = SessionContext::new();
        let task_ctx = session_ctx.task_ctx();

        let num_partitions = 4;
        let csv = test::scan_partitioned_csv(num_partitions)?;

        // input should have 4 partitions
        assert_eq!(csv.output_partitioning().partition_count(), num_partitions);

        let merge = CoalescePartitionsExec::new(csv);

        // output of CoalescePartitionsExec should have a single partition
        assert_eq!(merge.output_partitioning().partition_count(), 1);

        // the result should contain 4 batches (one per input partition)
        let iter = merge.execute(0, task_ctx)?;
        let batches = common::collect(iter).await?;
        assert_eq!(batches.len(), num_partitions);

        // there should be a total of 100 rows
        let row_count: usize = batches.iter().map(|batch| batch.num_rows()).sum();
        assert_eq!(row_count, 100);

        Ok(())
    }

    #[tokio::test]
    async fn test_drop_cancel() -> Result<()> {
        let session_ctx = SessionContext::new();
        let task_ctx = session_ctx.task_ctx();
        let schema =
            Arc::new(Schema::new(vec![Field::new("a", DataType::Float32, true)]));

        let blocking_exec = Arc::new(BlockingExec::new(Arc::clone(&schema), 2));
        let refs = blocking_exec.refs();
        let coaelesce_partitions_exec =
            Arc::new(CoalescePartitionsExec::new(blocking_exec));

        let fut = collect(coaelesce_partitions_exec, task_ctx);
        let mut fut = fut.boxed();

        assert_is_pending(&mut fut);
        drop(fut);
        assert_strong_count_converges_to_zero(refs).await;

        Ok(())
    }

    #[tokio::test]
    #[should_panic(expected = "PanickingStream did panic")]
    async fn test_panic() {
        let session_ctx = SessionContext::new();
        let task_ctx = session_ctx.task_ctx();
        let schema =
            Arc::new(Schema::new(vec![Field::new("a", DataType::Float32, true)]));

        let panicking_exec = Arc::new(PanicExec::new(Arc::clone(&schema), 2));
        let coalesce_partitions_exec =
            Arc::new(CoalescePartitionsExec::new(panicking_exec));

        collect(coalesce_partitions_exec, task_ctx).await.unwrap();
    }
}
