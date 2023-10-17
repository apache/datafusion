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

use arrow::{
    datatypes::{Field, Schema, SchemaRef},
    record_batch::RecordBatch,
};
use datafusion_common::{exec_err, internal_err, DFSchemaRef, DataFusionError};
use futures::Stream;
use itertools::Itertools;
use log::{debug, trace, warn};

use super::DisplayAs;
use super::{
    expressions::PhysicalSortExpr,
    metrics::{ExecutionPlanMetricsSet, MetricsSet},
    ColumnStatistics, DisplayFormatType, ExecutionPlan, Partitioning, RecordBatchStream,
    SendableRecordBatchStream, Statistics,
};
use crate::common::get_meet_of_orderings;
use crate::stream::ObservedStream;
use crate::{expressions, metrics::BaselineMetrics};
use datafusion_common::Result;
use datafusion_execution::TaskContext;
use datafusion_physical_expr::SchemaProperties;
use tokio::macros::support::thread_rng_n;

/// `UnionExec`: `UNION ALL` execution plan.
///
/// `UnionExec` combines multiple inputs with the same schema by
/// concatenating the partitions.  It does not mix or copy data within
/// or across partitions. Thus if the input partitions are sorted, the
/// output partitions of the union are also sorted.
///
/// For example, given a `UnionExec` of two inputs, with `N`
/// partitions, and `M` partitions, there will be `N+M` output
/// partitions. The first `N` output partitions are from Input 1
/// partitions, and then next `M` output partitions are from Input 2.
///
/// ```text
///                       ▲       ▲           ▲         ▲
///                       │       │           │         │
///     Output            │  ...  │           │         │
///   Partitions          │0      │N-1        │ N       │N+M-1
///(passes through   ┌────┴───────┴───────────┴─────────┴───┐
/// the N+M input    │              UnionExec               │
///  partitions)     │                                      │
///                  └──────────────────────────────────────┘
///                                      ▲
///                                      │
///                                      │
///       Input           ┌────────┬─────┴────┬──────────┐
///     Partitions        │ ...    │          │     ...  │
///                    0  │        │ N-1      │ 0        │  M-1
///                  ┌────┴────────┴───┐  ┌───┴──────────┴───┐
///                  │                 │  │                  │
///                  │                 │  │                  │
///                  │                 │  │                  │
///                  │                 │  │                  │
///                  │                 │  │                  │
///                  │                 │  │                  │
///                  │Input 1          │  │Input 2           │
///                  └─────────────────┘  └──────────────────┘
/// ```
#[derive(Debug)]
pub struct UnionExec {
    /// Input execution plan
    inputs: Vec<Arc<dyn ExecutionPlan>>,
    /// Execution metrics
    metrics: ExecutionPlanMetricsSet,
    /// Schema of Union
    schema: SchemaRef,
}

impl UnionExec {
    /// Create a new UnionExec with specified schema.
    /// The `schema` should always be a subset of the schema of `inputs`,
    /// otherwise, an error will be returned.
    pub fn try_new_with_schema(
        inputs: Vec<Arc<dyn ExecutionPlan>>,
        schema: DFSchemaRef,
    ) -> Result<Self> {
        let mut exec = Self::new(inputs);
        let exec_schema = exec.schema();
        let fields = schema
            .fields()
            .iter()
            .map(|dff| {
                exec_schema
                    .field_with_name(dff.name())
                    .cloned()
                    .map_err(|_| {
                        DataFusionError::Internal(format!(
                            "Cannot find the field {:?} in child schema",
                            dff.name()
                        ))
                    })
            })
            .collect::<Result<Vec<Field>>>()?;
        let schema = Arc::new(Schema::new_with_metadata(
            fields,
            exec.schema().metadata().clone(),
        ));
        exec.schema = schema;
        Ok(exec)
    }

    /// Create a new UnionExec
    pub fn new(inputs: Vec<Arc<dyn ExecutionPlan>>) -> Self {
        let schema = union_schema(&inputs);

        UnionExec {
            inputs,
            metrics: ExecutionPlanMetricsSet::new(),
            schema,
        }
    }

    /// Get inputs of the execution plan
    pub fn inputs(&self) -> &Vec<Arc<dyn ExecutionPlan>> {
        &self.inputs
    }
}

impl DisplayAs for UnionExec {
    fn fmt_as(
        &self,
        t: DisplayFormatType,
        f: &mut std::fmt::Formatter,
    ) -> std::fmt::Result {
        match t {
            DisplayFormatType::Default | DisplayFormatType::Verbose => {
                write!(f, "UnionExec")
            }
        }
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

    /// Specifies whether this plan generates an infinite stream of records.
    /// If the plan does not support pipelining, but its input(s) are
    /// infinite, returns an error to indicate this.
    fn unbounded_output(&self, children: &[bool]) -> Result<bool> {
        Ok(children.iter().any(|x| *x))
    }

    fn children(&self) -> Vec<Arc<dyn ExecutionPlan>> {
        self.inputs.clone()
    }

    /// Output of the union is the combination of all output partitions of the inputs
    fn output_partitioning(&self) -> Partitioning {
        // Output the combination of all output partitions of the inputs if the Union is not partition aware
        let num_partitions = self
            .inputs
            .iter()
            .map(|plan| plan.output_partitioning().partition_count())
            .sum();

        Partitioning::UnknownPartitioning(num_partitions)
    }

    fn output_ordering(&self) -> Option<&[PhysicalSortExpr]> {
        // The output ordering is the "meet" of its input orderings.
        // The meet is the finest ordering that satisfied by all the input
        // orderings, see https://en.wikipedia.org/wiki/Join_and_meet.
        get_meet_of_orderings(&self.inputs)
    }

    fn maintains_input_order(&self) -> Vec<bool> {
        // If the Union has an output ordering, it maintains at least one
        // child's ordering (i.e. the meet).
        // For instance, assume that the first child is SortExpr('a','b','c'),
        // the second child is SortExpr('a','b') and the third child is
        // SortExpr('a','b'). The output ordering would be SortExpr('a','b'),
        // which is the "meet" of all input orderings. In this example, this
        // function will return vec![false, true, true], indicating that we
        // preserve the orderings for the 2nd and the 3rd children.
        if let Some(output_ordering) = self.output_ordering() {
            self.inputs()
                .iter()
                .map(|child| {
                    if let Some(child_ordering) = child.output_ordering() {
                        output_ordering.len() == child_ordering.len()
                    } else {
                        false
                    }
                })
                .collect()
        } else {
            vec![false; self.inputs().len()]
        }
    }

    fn schema_properties(&self) -> SchemaProperties {
        let child_oeqs = self
            .inputs
            .iter()
            .map(|child| child.schema_properties())
            .collect::<Vec<_>>();
        let mut union_oeq = SchemaProperties::new(self.schema());
        // Iterate ordering equivalent group of first child
        for elem in child_oeqs[0].oeq_group().iter() {
            // Seed for the meet.
            let mut meet = Some(elem.clone());
            child_oeqs.iter().for_each(|child_oeq| {
                if let Some(meet_vec) = &meet {
                    let res = child_oeq
                        .oeq_group()
                        .iter()
                        .filter_map(|ordering| {
                            child_oeq.get_meet_ordering(ordering, meet_vec)
                        })
                        .collect::<Vec<_>>();
                    if let Some(new_meet) = res.first() {
                        meet = Some(new_meet.to_vec());
                    } else {
                        // If none of the child doesn't have a meet
                        // There is no meet.
                        meet = None;
                    }
                }
            });
            // All of the children have a common meet ordering.
            // This ordering can be propagated in Union.
            if let Some(meet) = meet {
                union_oeq.add_new_orderings(&[meet])
            }
        }
        union_oeq
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
        trace!("Start UnionExec::execute for partition {} of context session_id {} and task_id {:?}", partition, context.session_id(), context.task_id());
        let baseline_metrics = BaselineMetrics::new(&self.metrics, partition);
        // record the tiny amount of work done in this function so
        // elapsed_compute is reported as non zero
        let elapsed_compute = baseline_metrics.elapsed_compute().clone();
        let _timer = elapsed_compute.timer(); // record on drop

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

        warn!("Error in Union: Partition {} not found", partition);

        exec_err!("Partition {partition} not found in Union")
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

    fn benefits_from_input_partitioning(&self) -> Vec<bool> {
        vec![false; self.children().len()]
    }
}

/// Combines multiple input streams by interleaving them.
///
/// This only works if all inputs have the same hash-partitioning.
///
/// # Data Flow
/// ```text
/// +---------+
/// |         |---+
/// | Input 1 |   |
/// |         |-------------+
/// +---------+   |         |
///               |         |         +---------+
///               +------------------>|         |
///                 +---------------->| Combine |-->
///                 | +-------------->|         |
///                 | |     |         +---------+
/// +---------+     | |     |
/// |         |-----+ |     |
/// | Input 2 |       |     |
/// |         |---------------+
/// +---------+       |     | |
///                   |     | |       +---------+
///                   |     +-------->|         |
///                   |       +------>| Combine |-->
///                   |         +---->|         |
///                   |         |     +---------+
/// +---------+       |         |
/// |         |-------+         |
/// | Input 3 |                 |
/// |         |-----------------+
/// +---------+
/// ```
#[derive(Debug)]
pub struct InterleaveExec {
    /// Input execution plan
    inputs: Vec<Arc<dyn ExecutionPlan>>,
    /// Execution metrics
    metrics: ExecutionPlanMetricsSet,
    /// Schema of Interleave
    schema: SchemaRef,
}

impl InterleaveExec {
    /// Create a new InterleaveExec
    pub fn try_new(inputs: Vec<Arc<dyn ExecutionPlan>>) -> Result<Self> {
        let schema = union_schema(&inputs);

        if !can_interleave(&inputs) {
            return internal_err!(
                "Not all InterleaveExec children have a consistent hash partitioning"
            );
        }

        Ok(InterleaveExec {
            inputs,
            metrics: ExecutionPlanMetricsSet::new(),
            schema,
        })
    }

    /// Get inputs of the execution plan
    pub fn inputs(&self) -> &Vec<Arc<dyn ExecutionPlan>> {
        &self.inputs
    }
}

impl DisplayAs for InterleaveExec {
    fn fmt_as(
        &self,
        t: DisplayFormatType,
        f: &mut std::fmt::Formatter,
    ) -> std::fmt::Result {
        match t {
            DisplayFormatType::Default | DisplayFormatType::Verbose => {
                write!(f, "InterleaveExec")
            }
        }
    }
}

impl ExecutionPlan for InterleaveExec {
    /// Return a reference to Any that can be used for downcasting
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    /// Specifies whether this plan generates an infinite stream of records.
    /// If the plan does not support pipelining, but its input(s) are
    /// infinite, returns an error to indicate this.
    fn unbounded_output(&self, children: &[bool]) -> Result<bool> {
        Ok(children.iter().any(|x| *x))
    }

    fn children(&self) -> Vec<Arc<dyn ExecutionPlan>> {
        self.inputs.clone()
    }

    /// All inputs must have the same partitioning. The output partioning of InterleaveExec is the same as the inputs
    /// (NOT combined). E.g. if there are 10 inputs where each is `Hash(3)`-partitioned, InterleaveExec is also
    /// `Hash(3)`-partitioned.
    fn output_partitioning(&self) -> Partitioning {
        self.inputs[0].output_partitioning()
    }

    fn output_ordering(&self) -> Option<&[PhysicalSortExpr]> {
        None
    }

    fn maintains_input_order(&self) -> Vec<bool> {
        vec![false; self.inputs().len()]
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        Ok(Arc::new(InterleaveExec::try_new(children)?))
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        trace!("Start InterleaveExec::execute for partition {} of context session_id {} and task_id {:?}", partition, context.session_id(), context.task_id());
        let baseline_metrics = BaselineMetrics::new(&self.metrics, partition);
        // record the tiny amount of work done in this function so
        // elapsed_compute is reported as non zero
        let elapsed_compute = baseline_metrics.elapsed_compute().clone();
        let _timer = elapsed_compute.timer(); // record on drop

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

        warn!("Error in InterleaveExec: Partition {} not found", partition);

        exec_err!("Partition {partition} not found in InterleaveExec")
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

    fn benefits_from_input_partitioning(&self) -> Vec<bool> {
        vec![false; self.children().len()]
    }
}

/// If all the input partitions have the same Hash partition spec with the first_input_partition
/// The InterleaveExec is partition aware.
///
/// It might be too strict here in the case that the input partition specs are compatible but not exactly the same.
/// For example one input partition has the partition spec Hash('a','b','c') and
/// other has the partition spec Hash('a'), It is safe to derive the out partition with the spec Hash('a','b','c').
pub fn can_interleave(inputs: &[Arc<dyn ExecutionPlan>]) -> bool {
    if inputs.is_empty() {
        return false;
    }

    let first_input_partition = inputs[0].output_partitioning();
    matches!(first_input_partition, Partitioning::Hash(_, _))
        && inputs
            .iter()
            .map(|plan| plan.output_partitioning())
            .all(|partition| partition == first_input_partition)
}

fn union_schema(inputs: &[Arc<dyn ExecutionPlan>]) -> SchemaRef {
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

    Arc::new(Schema::new_with_metadata(
        fields,
        inputs[0].schema().metadata().clone(),
    ))
}

/// CombinedRecordBatchStream can be used to combine a Vec of SendableRecordBatchStreams into one
struct CombinedRecordBatchStream {
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
    type Item = Result<RecordBatch>;

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

    use crate::collect;
    use crate::memory::MemoryExec;
    use arrow::record_batch::RecordBatch;
    use arrow_schema::{DataType, SortOptions};
    use datafusion_common::ScalarValue;
    use datafusion_physical_expr::expressions::col;

    // Generate a schema which consists of 7 columns (a, b, c, d, e, f, g)
    fn create_test_schema() -> Result<SchemaRef> {
        let a = Field::new("a", DataType::Int32, true);
        let b = Field::new("b", DataType::Int32, true);
        let c = Field::new("c", DataType::Int32, true);
        let d = Field::new("d", DataType::Int32, true);
        let e = Field::new("e", DataType::Int32, true);
        let f = Field::new("f", DataType::Int32, true);
        let g = Field::new("g", DataType::Int32, true);
        let schema = Arc::new(Schema::new(vec![a, b, c, d, e, f, g]));

        Ok(schema)
    }

    #[tokio::test]
    async fn test_union_partitions() -> Result<()> {
        let task_ctx = Arc::new(TaskContext::default());

        // Create inputs with different partitioning
        let csv = test::scan_partitioned(4);
        let csv2 = test::scan_partitioned(5);

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

    #[tokio::test]
    async fn test_union_schema_properties() -> Result<()> {
        let schema = create_test_schema()?;
        let col_a = &col("a", &schema)?;
        let col_b = &col("b", &schema)?;
        let col_c = &col("c", &schema)?;
        let col_d = &col("d", &schema)?;
        let col_e = &col("e", &schema)?;
        let col_f = &col("f", &schema)?;
        let options = SortOptions::default();
        // [a ASC, b ASC, f ASC], [d ASC]
        let orderings = vec![
            vec![
                PhysicalSortExpr {
                    expr: col_a.clone(),
                    options,
                },
                PhysicalSortExpr {
                    expr: col_b.clone(),
                    options,
                },
                PhysicalSortExpr {
                    expr: col_f.clone(),
                    options,
                },
            ],
            vec![PhysicalSortExpr {
                expr: col_d.clone(),
                options,
            }],
        ];
        let child1 = Arc::new(
            MemoryExec::try_new(&[], schema.clone(), None)?
                .with_sort_information(orderings),
        );

        // [a ASC, b ASC, c ASC], [e ASC]
        let orderings = vec![
            vec![
                PhysicalSortExpr {
                    expr: col_a.clone(),
                    options,
                },
                PhysicalSortExpr {
                    expr: col_b.clone(),
                    options,
                },
                PhysicalSortExpr {
                    expr: col_c.clone(),
                    options,
                },
            ],
            vec![PhysicalSortExpr {
                expr: col_e.clone(),
                options,
            }],
        ];
        let child2 = Arc::new(
            MemoryExec::try_new(&[], schema, None)?.with_sort_information(orderings),
        );

        let union = UnionExec::new(vec![child1, child2]);
        // Expects union to have [a ASC, b ASC] (e.g meet of inout orderings)
        let union_schema_properties = union.schema_properties();
        let union_orderings = union_schema_properties.oeq_group();
        println!("union_orderings:{:?}", union_orderings);
        assert!(union_orderings.contains(&vec![
            PhysicalSortExpr {
                expr: col_a.clone(),
                options
            },
            PhysicalSortExpr {
                expr: col_b.clone(),
                options
            }
        ]));
        Ok(())
    }
}
