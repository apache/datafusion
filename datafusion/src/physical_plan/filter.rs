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

//! FilterExec evaluates a boolean predicate against all input batches to determine which rows to
//! include in its output batches.

use std::any::Any;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use super::{RecordBatchStream, SendableRecordBatchStream, Statistics};
use crate::error::{DataFusionError, Result};
use crate::physical_plan::{
    metrics::{BaselineMetrics, ExecutionPlanMetricsSet, MetricsSet},
    DisplayFormatType, ExecutionPlan, Partitioning, PhysicalExpr,
};
use arrow::array::BooleanArray;
use arrow::compute::filter_record_batch;
use arrow::datatypes::{DataType, SchemaRef};
use arrow::error::Result as ArrowResult;
use arrow::record_batch::RecordBatch;

use async_trait::async_trait;

use crate::execution::runtime_env::RuntimeEnv;
use futures::stream::{Stream, StreamExt};

/// FilterExec evaluates a boolean predicate against all input batches to determine which rows to
/// include in its output batches.
#[derive(Debug)]
pub struct FilterExec {
    /// The expression to filter on. This expression must evaluate to a boolean value.
    predicate: Arc<dyn PhysicalExpr>,
    /// The input plan
    input: Arc<dyn ExecutionPlan>,
    /// Execution metrics
    metrics: ExecutionPlanMetricsSet,
}

impl FilterExec {
    /// Create a FilterExec on an input
    pub fn try_new(
        predicate: Arc<dyn PhysicalExpr>,
        input: Arc<dyn ExecutionPlan>,
    ) -> Result<Self> {
        match predicate.data_type(input.schema().as_ref())? {
            DataType::Boolean => Ok(Self {
                predicate,
                input: input.clone(),
                metrics: ExecutionPlanMetricsSet::new(),
            }),
            other => Err(DataFusionError::Plan(format!(
                "Filter predicate must return boolean values, not {:?}",
                other
            ))),
        }
    }

    /// The expression to filter on. This expression must evaluate to a boolean value.
    pub fn predicate(&self) -> &Arc<dyn PhysicalExpr> {
        &self.predicate
    }

    /// The input plan
    pub fn input(&self) -> &Arc<dyn ExecutionPlan> {
        &self.input
    }
}

#[async_trait]
impl ExecutionPlan for FilterExec {
    /// Return a reference to Any that can be used for downcasting
    fn as_any(&self) -> &dyn Any {
        self
    }

    /// Get the schema for this execution plan
    fn schema(&self) -> SchemaRef {
        // The filter operator does not make any changes to the schema of its input
        self.input.schema()
    }

    fn children(&self) -> Vec<Arc<dyn ExecutionPlan>> {
        vec![self.input.clone()]
    }

    /// Get the output partitioning of this plan
    fn output_partitioning(&self) -> Partitioning {
        self.input.output_partitioning()
    }

    fn with_new_children(
        &self,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        match children.len() {
            1 => Ok(Arc::new(FilterExec::try_new(
                self.predicate.clone(),
                children[0].clone(),
            )?)),
            _ => Err(DataFusionError::Internal(
                "FilterExec wrong number of children".to_string(),
            )),
        }
    }

    async fn execute(
        &self,
        partition: usize,
        runtime: Arc<RuntimeEnv>,
    ) -> Result<SendableRecordBatchStream> {
        let baseline_metrics = BaselineMetrics::new(&self.metrics, partition);

        Ok(Box::pin(FilterExecStream {
            schema: self.input.schema().clone(),
            predicate: self.predicate.clone(),
            input: self.input.execute(partition, runtime).await?,
            baseline_metrics,
        }))
    }

    fn fmt_as(
        &self,
        t: DisplayFormatType,
        f: &mut std::fmt::Formatter,
    ) -> std::fmt::Result {
        match t {
            DisplayFormatType::Default => {
                write!(f, "FilterExec: {}", self.predicate)
            }
        }
    }

    fn metrics(&self) -> Option<MetricsSet> {
        Some(self.metrics.clone_inner())
    }

    /// The output statistics of a filtering operation are unknown
    fn statistics(&self) -> Statistics {
        Statistics::default()
    }
}

/// The FilterExec streams wraps the input iterator and applies the predicate expression to
/// determine which rows to include in its output batches
struct FilterExecStream {
    /// Output schema, which is the same as the input schema for this operator
    schema: SchemaRef,
    /// The expression to filter on. This expression must evaluate to a boolean value.
    predicate: Arc<dyn PhysicalExpr>,
    /// The input partition to filter.
    input: SendableRecordBatchStream,
    /// runtime metrics recording
    baseline_metrics: BaselineMetrics,
}

fn batch_filter(
    batch: &RecordBatch,
    predicate: &Arc<dyn PhysicalExpr>,
) -> ArrowResult<RecordBatch> {
    predicate
        .evaluate(batch)
        .map(|v| v.into_array(batch.num_rows()))
        .map_err(DataFusionError::into_arrow_external_error)
        .and_then(|array| {
            array
                .as_any()
                .downcast_ref::<BooleanArray>()
                .ok_or_else(|| {
                    DataFusionError::Internal(
                        "Filter predicate evaluated to non-boolean value".to_string(),
                    )
                    .into_arrow_external_error()
                })
                // apply filter array to record batch
                .and_then(|filter_array| filter_record_batch(batch, filter_array))
        })
}

impl Stream for FilterExecStream {
    type Item = ArrowResult<RecordBatch>;

    fn poll_next(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Self::Item>> {
        let poll = self.input.poll_next_unpin(cx).map(|x| match x {
            Some(Ok(batch)) => {
                let timer = self.baseline_metrics.elapsed_compute().timer();
                let filtered_batch = batch_filter(&batch, &self.predicate);
                timer.done();
                Some(filtered_batch)
            }
            other => other,
        });
        self.baseline_metrics.record_poll(poll)
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        // same number of record batches
        self.input.size_hint()
    }
}

impl RecordBatchStream for FilterExecStream {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }
}

#[cfg(test)]
mod tests {

    use super::*;
    use crate::datasource::object_store::local::LocalFileSystem;
    use crate::physical_plan::expressions::*;
    use crate::physical_plan::file_format::{CsvExec, PhysicalPlanConfig};
    use crate::physical_plan::ExecutionPlan;
    use crate::scalar::ScalarValue;
    use crate::test;
    use crate::test_util;
    use crate::{logical_plan::Operator, physical_plan::collect};
    use std::iter::Iterator;

    #[tokio::test]
    async fn simple_predicate() -> Result<()> {
        let runtime = Arc::new(RuntimeEnv::default());
        let schema = test_util::aggr_test_schema();

        let partitions = 4;
        let (_, files) =
            test::create_partitioned_csv("aggregate_test_100.csv", partitions)?;

        let csv = CsvExec::new(
            PhysicalPlanConfig {
                object_store: Arc::new(LocalFileSystem {}),
                file_schema: Arc::clone(&schema),
                file_groups: files,
                statistics: Statistics::default(),
                projection: None,
                batch_size: 1024,
                limit: None,
                table_partition_cols: vec![],
            },
            true,
            b',',
        );

        let predicate: Arc<dyn PhysicalExpr> = binary(
            binary(
                col("c2", &schema)?,
                Operator::Gt,
                lit(ScalarValue::from(1u32)),
                &schema,
            )?,
            Operator::And,
            binary(
                col("c2", &schema)?,
                Operator::Lt,
                lit(ScalarValue::from(4u32)),
                &schema,
            )?,
            &schema,
        )?;

        let filter: Arc<dyn ExecutionPlan> =
            Arc::new(FilterExec::try_new(predicate, Arc::new(csv))?);

        let results = collect(filter, runtime).await?;

        results
            .iter()
            .for_each(|batch| assert_eq!(13, batch.num_columns()));
        let row_count: usize = results.iter().map(|batch| batch.num_rows()).sum();
        assert_eq!(41, row_count);

        Ok(())
    }
}
