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

//! Defines the cross join plan for loading the left side of the cross join
//! and producing batches in parallel for the right partitions

use std::{any::Any, sync::Arc, task::Poll};

use super::utils::{
    adjust_right_output_partitioning, BuildProbeJoinMetrics, OnceAsync, OnceFut,
};
use crate::metrics::{ExecutionPlanMetricsSet, MetricsSet};
use crate::DisplayAs;
use crate::{
    coalesce_batches::concat_batches, coalesce_partitions::CoalescePartitionsExec,
    ColumnStatistics, DisplayFormatType, Distribution, ExecutionPlan, Partitioning,
    PhysicalSortExpr, RecordBatchStream, SendableRecordBatchStream, Statistics,
};

use arrow::datatypes::{Fields, Schema, SchemaRef};
use arrow::record_batch::RecordBatch;
use arrow_array::RecordBatchOptions;
use datafusion_common::stats::Precision;
use datafusion_common::{plan_err, DataFusionError, JoinType, Result, ScalarValue};
use datafusion_execution::memory_pool::{MemoryConsumer, MemoryReservation};
use datafusion_execution::TaskContext;
use datafusion_physical_expr::equivalence::join_equivalence_properties;
use datafusion_physical_expr::EquivalenceProperties;

use async_trait::async_trait;
use futures::{ready, Stream, StreamExt, TryStreamExt};

/// Data of the left side
type JoinLeftData = (RecordBatch, MemoryReservation);

/// executes partitions in parallel and combines them into a set of
/// partitions by combining all values from the left with all values on the right
#[derive(Debug)]
pub struct CrossJoinExec {
    /// left (build) side which gets loaded in memory
    pub left: Arc<dyn ExecutionPlan>,
    /// right (probe) side which are combined with left side
    pub right: Arc<dyn ExecutionPlan>,
    /// The schema once the join is applied
    schema: SchemaRef,
    /// Build-side data
    left_fut: OnceAsync<JoinLeftData>,
    /// Execution plan metrics
    metrics: ExecutionPlanMetricsSet,
}

impl CrossJoinExec {
    /// Create a new [CrossJoinExec].
    pub fn new(left: Arc<dyn ExecutionPlan>, right: Arc<dyn ExecutionPlan>) -> Self {
        // left then right
        let all_columns: Fields = {
            let left_schema = left.schema();
            let right_schema = right.schema();
            let left_fields = left_schema.fields().iter();
            let right_fields = right_schema.fields().iter();
            left_fields.chain(right_fields).cloned().collect()
        };

        let schema = Arc::new(Schema::new(all_columns));

        CrossJoinExec {
            left,
            right,
            schema,
            left_fut: Default::default(),
            metrics: ExecutionPlanMetricsSet::default(),
        }
    }

    /// left (build) side which gets loaded in memory
    pub fn left(&self) -> &Arc<dyn ExecutionPlan> {
        &self.left
    }

    /// right side which gets combined with left side
    pub fn right(&self) -> &Arc<dyn ExecutionPlan> {
        &self.right
    }
}

/// Asynchronously collect the result of the left child
async fn load_left_input(
    left: Arc<dyn ExecutionPlan>,
    context: Arc<TaskContext>,
    metrics: BuildProbeJoinMetrics,
    reservation: MemoryReservation,
) -> Result<JoinLeftData> {
    // merge all left parts into a single stream
    let left_schema = left.schema();
    let merge = if left.output_partitioning().partition_count() != 1 {
        Arc::new(CoalescePartitionsExec::new(left))
    } else {
        left
    };
    let stream = merge.execute(0, context)?;

    // Load all batches and count the rows
    let (batches, num_rows, _, reservation) = stream
        .try_fold(
            (Vec::new(), 0usize, metrics, reservation),
            |mut acc, batch| async {
                let batch_size = batch.get_array_memory_size();
                // Reserve memory for incoming batch
                acc.3.try_grow(batch_size)?;
                // Update metrics
                acc.2.build_mem_used.add(batch_size);
                acc.2.build_input_batches.add(1);
                acc.2.build_input_rows.add(batch.num_rows());
                // Update rowcount
                acc.1 += batch.num_rows();
                // Push batch to output
                acc.0.push(batch);
                Ok(acc)
            },
        )
        .await?;

    let merged_batch = concat_batches(&left_schema, &batches, num_rows)?;

    Ok((merged_batch, reservation))
}

impl DisplayAs for CrossJoinExec {
    fn fmt_as(
        &self,
        t: DisplayFormatType,
        f: &mut std::fmt::Formatter,
    ) -> std::fmt::Result {
        match t {
            DisplayFormatType::Default | DisplayFormatType::Verbose => {
                write!(f, "CrossJoinExec")
            }
        }
    }
}

impl ExecutionPlan for CrossJoinExec {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    fn children(&self) -> Vec<Arc<dyn ExecutionPlan>> {
        vec![self.left.clone(), self.right.clone()]
    }

    fn metrics(&self) -> Option<MetricsSet> {
        Some(self.metrics.clone_inner())
    }

    /// Specifies whether this plan generates an infinite stream of records.
    /// If the plan does not support pipelining, but its input(s) are
    /// infinite, returns an error to indicate this.
    fn unbounded_output(&self, children: &[bool]) -> Result<bool> {
        if children[0] || children[1] {
            plan_err!(
                "Cross Join Error: Cross join is not supported for the unbounded inputs."
            )
        } else {
            Ok(false)
        }
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        Ok(Arc::new(CrossJoinExec::new(
            children[0].clone(),
            children[1].clone(),
        )))
    }

    fn required_input_distribution(&self) -> Vec<Distribution> {
        vec![
            Distribution::SinglePartition,
            Distribution::UnspecifiedDistribution,
        ]
    }

    // TODO optimize CrossJoin implementation to generate M * N partitions
    fn output_partitioning(&self) -> Partitioning {
        let left_columns_len = self.left.schema().fields.len();
        adjust_right_output_partitioning(
            self.right.output_partitioning(),
            left_columns_len,
        )
    }

    // TODO check the output ordering of CrossJoin
    fn output_ordering(&self) -> Option<&[PhysicalSortExpr]> {
        None
    }

    fn equivalence_properties(&self) -> EquivalenceProperties {
        join_equivalence_properties(
            self.left.equivalence_properties(),
            self.right.equivalence_properties(),
            &JoinType::Full,
            self.schema(),
            &[false, false],
            None,
            &[],
        )
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        let stream = self.right.execute(partition, context.clone())?;

        let join_metrics = BuildProbeJoinMetrics::new(partition, &self.metrics);

        // Initialization of operator-level reservation
        let reservation =
            MemoryConsumer::new("CrossJoinExec").register(context.memory_pool());

        let left_fut = self.left_fut.once(|| {
            load_left_input(
                self.left.clone(),
                context,
                join_metrics.clone(),
                reservation,
            )
        });

        Ok(Box::pin(CrossJoinStream {
            schema: self.schema.clone(),
            left_fut,
            right: stream,
            right_batch: Arc::new(parking_lot::Mutex::new(None)),
            left_index: 0,
            join_metrics,
        }))
    }

    fn statistics(&self) -> Result<Statistics> {
        Ok(stats_cartesian_product(
            self.left.statistics()?,
            self.right.statistics()?,
        ))
    }
}

/// [left/right]_col_count are required in case the column statistics are None
fn stats_cartesian_product(
    left_stats: Statistics,
    right_stats: Statistics,
) -> Statistics {
    let left_row_count = left_stats.num_rows;
    let right_row_count = right_stats.num_rows;

    // calculate global stats
    let num_rows = left_row_count.multiply(&right_row_count);
    // the result size is two times a*b because you have the columns of both left and right
    let total_byte_size = left_stats
        .total_byte_size
        .multiply(&right_stats.total_byte_size)
        .multiply(&Precision::Exact(2));

    let left_col_stats = left_stats.column_statistics;
    let right_col_stats = right_stats.column_statistics;

    // the null counts must be multiplied by the row counts of the other side (if defined)
    // Min, max and distinct_count on the other hand are invariants.
    let cross_join_stats = left_col_stats
        .into_iter()
        .map(|s| ColumnStatistics {
            null_count: s.null_count.multiply(&right_row_count),
            distinct_count: s.distinct_count,
            min_value: s.min_value,
            max_value: s.max_value,
        })
        .chain(right_col_stats.into_iter().map(|s| ColumnStatistics {
            null_count: s.null_count.multiply(&left_row_count),
            distinct_count: s.distinct_count,
            min_value: s.min_value,
            max_value: s.max_value,
        }))
        .collect();

    Statistics {
        num_rows,
        total_byte_size,
        column_statistics: cross_join_stats,
    }
}

/// A stream that issues [RecordBatch]es as they arrive from the right  of the join.
struct CrossJoinStream {
    /// Input schema
    schema: Arc<Schema>,
    /// future for data from left side
    left_fut: OnceFut<JoinLeftData>,
    /// right
    right: SendableRecordBatchStream,
    /// Current value on the left
    left_index: usize,
    /// Current batch being processed from the right side
    right_batch: Arc<parking_lot::Mutex<Option<RecordBatch>>>,
    /// join execution metrics
    join_metrics: BuildProbeJoinMetrics,
}

impl RecordBatchStream for CrossJoinStream {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }
}

fn build_batch(
    left_index: usize,
    batch: &RecordBatch,
    left_data: &RecordBatch,
    schema: &Schema,
) -> Result<RecordBatch> {
    // Repeat value on the left n times
    let arrays = left_data
        .columns()
        .iter()
        .map(|arr| {
            let scalar = ScalarValue::try_from_array(arr, left_index)?;
            scalar.to_array_of_size(batch.num_rows())
        })
        .collect::<Result<Vec<_>>>()?;

    RecordBatch::try_new_with_options(
        Arc::new(schema.clone()),
        arrays
            .iter()
            .chain(batch.columns().iter())
            .cloned()
            .collect(),
        &RecordBatchOptions::new().with_row_count(Some(batch.num_rows())),
    )
    .map_err(Into::into)
}

#[async_trait]
impl Stream for CrossJoinStream {
    type Item = Result<RecordBatch>;

    fn poll_next(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Self::Item>> {
        self.poll_next_impl(cx)
    }
}

impl CrossJoinStream {
    /// Separate implementation function that unpins the [`CrossJoinStream`] so
    /// that partial borrows work correctly
    fn poll_next_impl(
        &mut self,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Result<RecordBatch>>> {
        let build_timer = self.join_metrics.build_time.timer();
        let (left_data, _) = match ready!(self.left_fut.get(cx)) {
            Ok(left_data) => left_data,
            Err(e) => return Poll::Ready(Some(Err(e))),
        };
        build_timer.done();

        if left_data.num_rows() == 0 {
            return Poll::Ready(None);
        }

        if self.left_index > 0 && self.left_index < left_data.num_rows() {
            let join_timer = self.join_metrics.join_time.timer();
            let right_batch = {
                let right_batch = self.right_batch.lock();
                right_batch.clone().unwrap()
            };
            let result =
                build_batch(self.left_index, &right_batch, left_data, &self.schema);
            self.join_metrics.input_rows.add(right_batch.num_rows());
            if let Ok(ref batch) = result {
                join_timer.done();
                self.join_metrics.output_batches.add(1);
                self.join_metrics.output_rows.add(batch.num_rows());
            }
            self.left_index += 1;
            return Poll::Ready(Some(result));
        }
        self.left_index = 0;
        self.right
            .poll_next_unpin(cx)
            .map(|maybe_batch| match maybe_batch {
                Some(Ok(batch)) => {
                    let join_timer = self.join_metrics.join_time.timer();
                    let result =
                        build_batch(self.left_index, &batch, left_data, &self.schema);
                    self.join_metrics.input_batches.add(1);
                    self.join_metrics.input_rows.add(batch.num_rows());
                    if let Ok(ref batch) = result {
                        join_timer.done();
                        self.join_metrics.output_batches.add(1);
                        self.join_metrics.output_rows.add(batch.num_rows());
                    }
                    self.left_index = 1;

                    let mut right_batch = self.right_batch.lock();
                    *right_batch = Some(batch);

                    Some(result)
                }
                other => other,
            })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::common;
    use crate::test::build_table_scan_i32;

    use datafusion_common::{assert_batches_sorted_eq, assert_contains};
    use datafusion_execution::runtime_env::{RuntimeConfig, RuntimeEnv};

    async fn join_collect(
        left: Arc<dyn ExecutionPlan>,
        right: Arc<dyn ExecutionPlan>,
        context: Arc<TaskContext>,
    ) -> Result<(Vec<String>, Vec<RecordBatch>)> {
        let join = CrossJoinExec::new(left, right);
        let columns_header = columns(&join.schema());

        let stream = join.execute(0, context)?;
        let batches = common::collect(stream).await?;

        Ok((columns_header, batches))
    }

    #[tokio::test]
    async fn test_stats_cartesian_product() {
        let left_row_count = 11;
        let left_bytes = 23;
        let right_row_count = 7;
        let right_bytes = 27;

        let left = Statistics {
            num_rows: Precision::Exact(left_row_count),
            total_byte_size: Precision::Exact(left_bytes),
            column_statistics: vec![
                ColumnStatistics {
                    distinct_count: Precision::Exact(5),
                    max_value: Precision::Exact(ScalarValue::Int64(Some(21))),
                    min_value: Precision::Exact(ScalarValue::Int64(Some(-4))),
                    null_count: Precision::Exact(0),
                },
                ColumnStatistics {
                    distinct_count: Precision::Exact(1),
                    max_value: Precision::Exact(ScalarValue::from("x")),
                    min_value: Precision::Exact(ScalarValue::from("a")),
                    null_count: Precision::Exact(3),
                },
            ],
        };

        let right = Statistics {
            num_rows: Precision::Exact(right_row_count),
            total_byte_size: Precision::Exact(right_bytes),
            column_statistics: vec![ColumnStatistics {
                distinct_count: Precision::Exact(3),
                max_value: Precision::Exact(ScalarValue::Int64(Some(12))),
                min_value: Precision::Exact(ScalarValue::Int64(Some(0))),
                null_count: Precision::Exact(2),
            }],
        };

        let result = stats_cartesian_product(left, right);

        let expected = Statistics {
            num_rows: Precision::Exact(left_row_count * right_row_count),
            total_byte_size: Precision::Exact(2 * left_bytes * right_bytes),
            column_statistics: vec![
                ColumnStatistics {
                    distinct_count: Precision::Exact(5),
                    max_value: Precision::Exact(ScalarValue::Int64(Some(21))),
                    min_value: Precision::Exact(ScalarValue::Int64(Some(-4))),
                    null_count: Precision::Exact(0),
                },
                ColumnStatistics {
                    distinct_count: Precision::Exact(1),
                    max_value: Precision::Exact(ScalarValue::from("x")),
                    min_value: Precision::Exact(ScalarValue::from("a")),
                    null_count: Precision::Exact(3 * right_row_count),
                },
                ColumnStatistics {
                    distinct_count: Precision::Exact(3),
                    max_value: Precision::Exact(ScalarValue::Int64(Some(12))),
                    min_value: Precision::Exact(ScalarValue::Int64(Some(0))),
                    null_count: Precision::Exact(2 * left_row_count),
                },
            ],
        };

        assert_eq!(result, expected);
    }

    #[tokio::test]
    async fn test_stats_cartesian_product_with_unknwon_size() {
        let left_row_count = 11;

        let left = Statistics {
            num_rows: Precision::Exact(left_row_count),
            total_byte_size: Precision::Exact(23),
            column_statistics: vec![
                ColumnStatistics {
                    distinct_count: Precision::Exact(5),
                    max_value: Precision::Exact(ScalarValue::Int64(Some(21))),
                    min_value: Precision::Exact(ScalarValue::Int64(Some(-4))),
                    null_count: Precision::Exact(0),
                },
                ColumnStatistics {
                    distinct_count: Precision::Exact(1),
                    max_value: Precision::Exact(ScalarValue::from("x")),
                    min_value: Precision::Exact(ScalarValue::from("a")),
                    null_count: Precision::Exact(3),
                },
            ],
        };

        let right = Statistics {
            num_rows: Precision::Absent,
            total_byte_size: Precision::Absent,
            column_statistics: vec![ColumnStatistics {
                distinct_count: Precision::Exact(3),
                max_value: Precision::Exact(ScalarValue::Int64(Some(12))),
                min_value: Precision::Exact(ScalarValue::Int64(Some(0))),
                null_count: Precision::Exact(2),
            }],
        };

        let result = stats_cartesian_product(left, right);

        let expected = Statistics {
            num_rows: Precision::Absent,
            total_byte_size: Precision::Absent,
            column_statistics: vec![
                ColumnStatistics {
                    distinct_count: Precision::Exact(5),
                    max_value: Precision::Exact(ScalarValue::Int64(Some(21))),
                    min_value: Precision::Exact(ScalarValue::Int64(Some(-4))),
                    null_count: Precision::Absent, // we don't know the row count on the right
                },
                ColumnStatistics {
                    distinct_count: Precision::Exact(1),
                    max_value: Precision::Exact(ScalarValue::from("x")),
                    min_value: Precision::Exact(ScalarValue::from("a")),
                    null_count: Precision::Absent, // we don't know the row count on the right
                },
                ColumnStatistics {
                    distinct_count: Precision::Exact(3),
                    max_value: Precision::Exact(ScalarValue::Int64(Some(12))),
                    min_value: Precision::Exact(ScalarValue::Int64(Some(0))),
                    null_count: Precision::Exact(2 * left_row_count),
                },
            ],
        };

        assert_eq!(result, expected);
    }

    #[tokio::test]
    async fn test_join() -> Result<()> {
        let task_ctx = Arc::new(TaskContext::default());

        let left = build_table_scan_i32(
            ("a1", &vec![1, 2, 3]),
            ("b1", &vec![4, 5, 6]),
            ("c1", &vec![7, 8, 9]),
        );
        let right = build_table_scan_i32(
            ("a2", &vec![10, 11]),
            ("b2", &vec![12, 13]),
            ("c2", &vec![14, 15]),
        );

        let (columns, batches) = join_collect(left, right, task_ctx).await?;

        assert_eq!(columns, vec!["a1", "b1", "c1", "a2", "b2", "c2"]);
        let expected = [
            "+----+----+----+----+----+----+",
            "| a1 | b1 | c1 | a2 | b2 | c2 |",
            "+----+----+----+----+----+----+",
            "| 1  | 4  | 7  | 10 | 12 | 14 |",
            "| 1  | 4  | 7  | 11 | 13 | 15 |",
            "| 2  | 5  | 8  | 10 | 12 | 14 |",
            "| 2  | 5  | 8  | 11 | 13 | 15 |",
            "| 3  | 6  | 9  | 10 | 12 | 14 |",
            "| 3  | 6  | 9  | 11 | 13 | 15 |",
            "+----+----+----+----+----+----+",
        ];

        assert_batches_sorted_eq!(expected, &batches);

        Ok(())
    }

    #[tokio::test]
    async fn test_overallocation() -> Result<()> {
        let runtime_config = RuntimeConfig::new().with_memory_limit(100, 1.0);
        let runtime = Arc::new(RuntimeEnv::new(runtime_config)?);
        let task_ctx = TaskContext::default().with_runtime(runtime);
        let task_ctx = Arc::new(task_ctx);

        let left = build_table_scan_i32(
            ("a1", &vec![1, 2, 3, 4, 5, 6, 7, 8, 9, 0]),
            ("b1", &vec![1, 2, 3, 4, 5, 6, 7, 8, 9, 0]),
            ("c1", &vec![1, 2, 3, 4, 5, 6, 7, 8, 9, 0]),
        );
        let right = build_table_scan_i32(
            ("a2", &vec![10, 11]),
            ("b2", &vec![12, 13]),
            ("c2", &vec![14, 15]),
        );

        let err = join_collect(left, right, task_ctx).await.unwrap_err();

        assert_contains!(
            err.to_string(),
            "External error: Resources exhausted: Failed to allocate additional"
        );
        assert_contains!(err.to_string(), "CrossJoinExec");

        Ok(())
    }

    /// Returns the column names on the schema
    fn columns(schema: &Schema) -> Vec<String> {
        schema.fields().iter().map(|f| f.name().clone()).collect()
    }
}
