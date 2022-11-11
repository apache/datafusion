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

use std::pin::Pin;
use std::task::{Context, Poll};
use std::{any::Any, sync::Arc};

use arrow::error::Result as ArrowResult;
use arrow::{
    datatypes::{Field, Schema, SchemaRef},
    record_batch::RecordBatch,
};
use futures::{Stream, StreamExt};
use itertools::Itertools;
use log::debug;
use log::warn;

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
use datafusion_physical_expr::sort_expr_list_eq_strict_order;
use tokio::macros::support::thread_rng_n;

/// UNION ALL execution plan
#[derive(Debug)]
pub struct UnionExec {
    /// Input execution plan
    inputs: Vec<Arc<dyn ExecutionPlan>>,
    /// Execution metrics
    metrics: ExecutionPlanMetricsSet,
    /// Schema of Union
    schema: SchemaRef,
    /// Partition aware Union
    partition_aware: bool,
}

impl UnionExec {
    /// Create a new UnionExec
    pub fn new(inputs: Vec<Arc<dyn ExecutionPlan>>) -> Self {
        let fields: Vec<Field> = (0..inputs[0].schema().fields().len())
            .map(|i| {
                inputs
                    .iter()
                    .filter_map(|input| {
                        if input.schema().fields().len() > i {
                            Some(input.schema().field(i).clone())
                        } else {
                            None
                        }
                    })
                    .find_or_first(|f| f.is_nullable())
                    .unwrap()
            })
            .collect();

        let schema = Arc::new(Schema::new_with_metadata(
            fields,
            inputs[0].schema().metadata().clone(),
        ));

        // If all the input partitions have the same Hash partition spec with the first_input_partition
        // The UnionExec is partition aware.
        //
        // It might be too strict here in the case that the input partition specs are compatible but not exactly the same.
        // For example one input partition has the partition spec Hash('a','b','c') and
        // other has the partition spec Hash('a'), It is safe to derive the out partition with the spec Hash('a','b','c').
        let first_input_partition = inputs[0].output_partitioning();
        let partition_aware = matches!(first_input_partition, Partitioning::Hash(_, _))
            && inputs
                .iter()
                .map(|plan| plan.output_partitioning())
                .all(|partition| partition == first_input_partition);

        UnionExec {
            inputs,
            metrics: ExecutionPlanMetricsSet::new(),
            schema,
            partition_aware,
        }
    }

    /// Get inputs of the execution plan
    pub fn inputs(&self) -> &Vec<Arc<dyn ExecutionPlan>> {
        &self.inputs
    }
}

impl ExecutionPlan for UnionExec {
    /// Return a reference to Any that can be used for downcasting
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    fn children(&self) -> Vec<Arc<dyn ExecutionPlan>> {
        self.inputs.clone()
    }

    /// Output of the union is the combination of all output partitions of the inputs
    fn output_partitioning(&self) -> Partitioning {
        if self.partition_aware {
            self.inputs[0].output_partitioning()
        } else {
            // Output the combination of all output partitions of the inputs if the Union is not partition aware
            let num_partitions = self
                .inputs
                .iter()
                .map(|plan| plan.output_partitioning().partition_count())
                .sum();

            Partitioning::UnknownPartitioning(num_partitions)
        }
    }

    fn output_ordering(&self) -> Option<&[PhysicalSortExpr]> {
        let first_input_ordering = self.inputs[0].output_ordering();
        // If the Union is not partition aware and all the input ordering spec strictly equal with the first_input_ordering
        // Return the first_input_ordering as the output_ordering
        //
        // It might be too strict here in the case that the input ordering are compatible but not exactly the same.
        // For example one input ordering has the ordering spec SortExpr('a','b','c') and the other has the ordering
        // spec SortExpr('a'), It is safe to derive the out ordering with the spec SortExpr('a').
        if !self.partition_aware
            && first_input_ordering.is_some()
            && self
                .inputs
                .iter()
                .map(|plan| plan.output_ordering())
                .all(|ordering| {
                    ordering.is_some()
                        && sort_expr_list_eq_strict_order(
                            ordering.unwrap(),
                            first_input_ordering.unwrap(),
                        )
                })
        {
            first_input_ordering
        } else {
            None
        }
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        Ok(Arc::new(UnionExec::new(children)))
    }

    fn execute(
        &self,
        mut partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        debug!("Start UnionExec::execute for partition {} of context session_id {} and task_id {:?}", partition, context.session_id(), context.task_id());
        let baseline_metrics = BaselineMetrics::new(&self.metrics, partition);
        // record the tiny amount of work done in this function so
        // elapsed_compute is reported as non zero
        let elapsed_compute = baseline_metrics.elapsed_compute().clone();
        let _timer = elapsed_compute.timer(); // record on drop

        if self.partition_aware {
            let mut input_stream_vec = vec![];
            for input in self.inputs.iter() {
                if partition < input.output_partitioning().partition_count() {
                    input_stream_vec.push(input.execute(partition, context.clone())?);
                } else {
                    // Do not find a partition to execute
                    break;
                }
            }
            if input_stream_vec.len() == self.inputs.len() {
                let stream = Box::pin(CombinedRecordBatchStream::new(
                    self.schema(),
                    input_stream_vec,
                ));
                return Ok(Box::pin(ObservedStream::new(stream, baseline_metrics)));
            }
        } else {
            // find partition to execute
            for input in self.inputs.iter() {
                // Calculate whether partition belongs to the current partition
                if partition < input.output_partitioning().partition_count() {
                    let stream = input.execute(partition, context)?;
                    debug!("Found a Union partition to execute");
                    return Ok(Box::pin(ObservedStream::new(stream, baseline_metrics)));
                } else {
                    partition -= input.output_partitioning().partition_count();
                }
            }
        }

        warn!("Error in Union: Partition {} not found", partition);

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

/// CombinedRecordBatchStream can be used to combine a Vec of SendableRecordBatchStreams into one
pub struct CombinedRecordBatchStream {
    /// Schema wrapped by Arc
    schema: SchemaRef,
    /// Stream entries
    entries: Vec<SendableRecordBatchStream>,
}

impl CombinedRecordBatchStream {
    /// Create an CombinedRecordBatchStream
    pub fn new(schema: SchemaRef, entries: Vec<SendableRecordBatchStream>) -> Self {
        Self { schema, entries }
    }
}

impl RecordBatchStream for CombinedRecordBatchStream {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }
}

impl Stream for CombinedRecordBatchStream {
    type Item = ArrowResult<RecordBatch>;

    fn poll_next(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Self::Item>> {
        use Poll::*;

        let start = thread_rng_n(self.entries.len() as u32) as usize;
        let mut idx = start;

        for _ in 0..self.entries.len() {
            let stream = self.entries.get_mut(idx).unwrap();

            match Pin::new(stream).poll_next(cx) {
                Ready(Some(val)) => return Ready(Some(val)),
                Ready(None) => {
                    // Remove the entry
                    self.entries.swap_remove(idx);

                    // Check if this was the last entry, if so the cursor needs
                    // to wrap
                    if idx == self.entries.len() {
                        idx = 0;
                    } else if idx < start && start <= self.entries.len() {
                        // The stream being swapped into the current index has
                        // already been polled, so skip it.
                        idx = idx.wrapping_add(1) % self.entries.len();
                    }
                }
                Pending => {
                    idx = idx.wrapping_add(1) % self.entries.len();
                }
            }
        }

        // If the map is empty, then the stream is complete.
        if self.entries.is_empty() {
            Ready(None)
        } else {
            Pending
        }
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
    use crate::test;

    use crate::prelude::SessionContext;
    use crate::{physical_plan::collect, scalar::ScalarValue};
    use arrow::record_batch::RecordBatch;

    #[tokio::test]
    async fn test_union_partitions() -> Result<()> {
        let session_ctx = SessionContext::new();
        let task_ctx = session_ctx.task_ctx();

        // Create csv's with different partitioning
        let csv = test::scan_partitioned_csv(4)?;
        let csv2 = test::scan_partitioned_csv(5)?;

        let union_exec = Arc::new(UnionExec::new(vec![csv, csv2]));

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
