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

// Some of these functions reference the Postgres documentation
// or implementation to ensure compatibility and are subject to
// the Postgres license.

//! The Union operator combines multiple inputs with the same schema

use std::{any::Any, sync::Arc};

use arrow::{datatypes::SchemaRef, record_batch::RecordBatch};
use futures::StreamExt;

use super::{
    expressions::PhysicalSortExpr,
    metrics::{ExecutionPlanMetricsSet, MetricsSet},
    ColumnStatistics, DisplayFormatType, ExecutionPlan, Partitioning, RecordBatchStream,
    SendableRecordBatchStream, Statistics,
};
use crate::execution::context::TaskContext;
use crate::{
    error::Result,
    physical_plan::{expressions, metrics::BaselineMetrics},
};
use async_trait::async_trait;

/// UNION ALL execution plan
#[derive(Debug)]
pub struct UnionExec {
    /// Input execution plan
    inputs: Vec<Arc<dyn ExecutionPlan>>,
    /// Execution metrics
    metrics: ExecutionPlanMetricsSet,
}

impl UnionExec {
    /// Create a new UnionExec
    pub fn new(inputs: Vec<Arc<dyn ExecutionPlan>>) -> Self {
        UnionExec {
            inputs,
            metrics: ExecutionPlanMetricsSet::new(),
        }
    }

    /// Get inputs of the execution plan
    pub fn inputs(&self) -> &Vec<Arc<dyn ExecutionPlan>> {
        &self.inputs
    }
}

#[async_trait]
impl ExecutionPlan for UnionExec {
    /// Return a reference to Any that can be used for downcasting
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.inputs[0].schema()
    }

    fn children(&self) -> Vec<Arc<dyn ExecutionPlan>> {
        self.inputs.clone()
    }

    /// Output of the union is the combination of all output partitions of the inputs
    fn output_partitioning(&self) -> Partitioning {
        // Sums all the output partitions
        let num_partitions = self
            .inputs
            .iter()
            .map(|plan| plan.output_partitioning().partition_count())
            .sum();
        // TODO: this loses partitioning info in case of same partitioning scheme (for example `Partitioning::Hash`)
        // https://issues.apache.org/jira/browse/ARROW-11991
        Partitioning::UnknownPartitioning(num_partitions)
    }

    fn output_ordering(&self) -> Option<&[PhysicalSortExpr]> {
        None
    }

    fn relies_on_input_order(&self) -> bool {
        false
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        Ok(Arc::new(UnionExec::new(children)))
    }

    async fn execute(
        &self,
        mut partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        let baseline_metrics = BaselineMetrics::new(&self.metrics, partition);
        // record the tiny amount of work done in this function so
        // elapsed_compute is reported as non zero
        let elapsed_compute = baseline_metrics.elapsed_compute().clone();
        let _timer = elapsed_compute.timer(); // record on drop

        // find partition to execute
        for input in self.inputs.iter() {
            // Calculate whether partition belongs to the current partition
            if partition < input.output_partitioning().partition_count() {
                let stream = input.execute(partition, context.clone()).await?;
                return Ok(Box::pin(ObservedStream::new(stream, baseline_metrics)));
            } else {
                partition -= input.output_partitioning().partition_count();
            }
        }

        Err(crate::error::DataFusionError::Execution(format!(
            "Partition {} not found in Union",
            partition
        )))
    }

    fn fmt_as(
        &self,
        t: DisplayFormatType,
        f: &mut std::fmt::Formatter,
    ) -> std::fmt::Result {
        match t {
            DisplayFormatType::Default => {
                write!(f, "UnionExec")
            }
        }
    }

    fn metrics(&self) -> Option<MetricsSet> {
        Some(self.metrics.clone_inner())
    }

    fn statistics(&self) -> Statistics {
        self.inputs
            .iter()
            .map(|ep| ep.statistics())
            .reduce(stats_union)
            .unwrap_or_default()
    }

    fn benefits_from_input_partitioning(&self) -> bool {
        false
    }
}

/// Stream wrapper that records `BaselineMetrics` for a particular
/// partition
struct ObservedStream {
    inner: SendableRecordBatchStream,
    baseline_metrics: BaselineMetrics,
}

impl ObservedStream {
    fn new(inner: SendableRecordBatchStream, baseline_metrics: BaselineMetrics) -> Self {
        Self {
            inner,
            baseline_metrics,
        }
    }
}

impl RecordBatchStream for ObservedStream {
    fn schema(&self) -> arrow::datatypes::SchemaRef {
        self.inner.schema()
    }
}

impl futures::Stream for ObservedStream {
    type Item = arrow::error::Result<RecordBatch>;

    fn poll_next(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Self::Item>> {
        let poll = self.inner.poll_next_unpin(cx);
        self.baseline_metrics.record_poll(poll)
    }
}

fn col_stats_union(
    mut left: ColumnStatistics,
    right: ColumnStatistics,
) -> ColumnStatistics {
    left.distinct_count = None;
    left.min_value = left
        .min_value
        .zip(right.min_value)
        .map(|(a, b)| expressions::helpers::min(&a, &b))
        .and_then(Result::ok);
    left.max_value = left
        .max_value
        .zip(right.max_value)
        .map(|(a, b)| expressions::helpers::max(&a, &b))
        .and_then(Result::ok);
    left.null_count = left.null_count.zip(right.null_count).map(|(a, b)| a + b);

    left
}

fn stats_union(mut left: Statistics, right: Statistics) -> Statistics {
    left.is_exact = left.is_exact && right.is_exact;
    left.num_rows = left.num_rows.zip(right.num_rows).map(|(a, b)| a + b);
    left.total_byte_size = left
        .total_byte_size
        .zip(right.total_byte_size)
        .map(|(a, b)| a + b);
    left.column_statistics =
        left.column_statistics
            .zip(right.column_statistics)
            .map(|(a, b)| {
                a.into_iter()
                    .zip(b)
                    .map(|(ca, cb)| col_stats_union(ca, cb))
                    .collect()
            });
    left
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::datafusion_data_access::object_store::{
        local::LocalFileSystem, ObjectStore,
    };
    use crate::{test, test_util};

    use crate::prelude::SessionContext;
    use crate::{
        physical_plan::{
            collect,
            file_format::{CsvExec, FileScanConfig},
        },
        scalar::ScalarValue,
    };
    use arrow::record_batch::RecordBatch;

    #[tokio::test]
    async fn test_union_partitions() -> Result<()> {
        let session_ctx = SessionContext::new();
        let task_ctx = session_ctx.task_ctx();
        let schema = test_util::aggr_test_schema();
        let fs: Arc<dyn ObjectStore> = Arc::new(LocalFileSystem {});

        // Create csv's with different partitioning
        let (_, files) = test::create_partitioned_csv("aggregate_test_100.csv", 4)?;
        let (_, files2) = test::create_partitioned_csv("aggregate_test_100.csv", 5)?;

        let csv = CsvExec::new(
            FileScanConfig {
                object_store: Arc::clone(&fs),
                file_schema: Arc::clone(&schema),
                file_groups: files,
                statistics: Statistics::default(),
                projection: None,
                limit: None,
                table_partition_cols: vec![],
            },
            true,
            b',',
        );

        let csv2 = CsvExec::new(
            FileScanConfig {
                object_store: Arc::clone(&fs),
                file_schema: Arc::clone(&schema),
                file_groups: files2,
                statistics: Statistics::default(),
                projection: None,
                limit: None,
                table_partition_cols: vec![],
            },
            true,
            b',',
        );

        let union_exec = Arc::new(UnionExec::new(vec![Arc::new(csv), Arc::new(csv2)]));

        // Should have 9 partitions and 9 output batches
        assert_eq!(union_exec.output_partitioning().partition_count(), 9);

        let result: Vec<RecordBatch> = collect(union_exec, task_ctx).await?;
        assert_eq!(result.len(), 9);

        Ok(())
    }

    #[tokio::test]
    async fn test_stats_union() {
        let left = Statistics {
            is_exact: true,
            num_rows: Some(5),
            total_byte_size: Some(23),
            column_statistics: Some(vec![
                ColumnStatistics {
                    distinct_count: Some(5),
                    max_value: Some(ScalarValue::Int64(Some(21))),
                    min_value: Some(ScalarValue::Int64(Some(-4))),
                    null_count: Some(0),
                },
                ColumnStatistics {
                    distinct_count: Some(1),
                    max_value: Some(ScalarValue::Utf8(Some(String::from("x")))),
                    min_value: Some(ScalarValue::Utf8(Some(String::from("a")))),
                    null_count: Some(3),
                },
                ColumnStatistics {
                    distinct_count: None,
                    max_value: Some(ScalarValue::Float32(Some(1.1))),
                    min_value: Some(ScalarValue::Float32(Some(0.1))),
                    null_count: None,
                },
            ]),
        };

        let right = Statistics {
            is_exact: true,
            num_rows: Some(7),
            total_byte_size: Some(29),
            column_statistics: Some(vec![
                ColumnStatistics {
                    distinct_count: Some(3),
                    max_value: Some(ScalarValue::Int64(Some(34))),
                    min_value: Some(ScalarValue::Int64(Some(1))),
                    null_count: Some(1),
                },
                ColumnStatistics {
                    distinct_count: None,
                    max_value: Some(ScalarValue::Utf8(Some(String::from("c")))),
                    min_value: Some(ScalarValue::Utf8(Some(String::from("b")))),
                    null_count: None,
                },
                ColumnStatistics {
                    distinct_count: None,
                    max_value: None,
                    min_value: None,
                    null_count: None,
                },
            ]),
        };

        let result = stats_union(left, right);
        let expected = Statistics {
            is_exact: true,
            num_rows: Some(12),
            total_byte_size: Some(52),
            column_statistics: Some(vec![
                ColumnStatistics {
                    distinct_count: None,
                    max_value: Some(ScalarValue::Int64(Some(34))),
                    min_value: Some(ScalarValue::Int64(Some(-4))),
                    null_count: Some(1),
                },
                ColumnStatistics {
                    distinct_count: None,
                    max_value: Some(ScalarValue::Utf8(Some(String::from("x")))),
                    min_value: Some(ScalarValue::Utf8(Some(String::from("a")))),
                    null_count: None,
                },
                ColumnStatistics {
                    distinct_count: None,
                    max_value: None,
                    min_value: None,
                    null_count: None,
                },
            ]),
        };

        assert_eq!(result, expected);
    }
}
