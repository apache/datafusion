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
    adjust_right_output_partitioning, reorder_output_after_swap, BatchSplitter,
    BatchTransformer, BuildProbeJoinMetrics, NoopBatchTransformer, OnceAsync, OnceFut,
    StatefulStreamResult,
};
use crate::execution_plan::{boundedness_from_children, EmissionType};
use crate::metrics::{ExecutionPlanMetricsSet, MetricsSet};
use crate::projection::{
    join_allows_pushdown, join_table_borders, new_join_children,
    physical_to_column_exprs, ProjectionExec,
};
use crate::{
    handle_state, ColumnStatistics, DisplayAs, DisplayFormatType, Distribution,
    ExecutionPlan, ExecutionPlanProperties, PlanProperties, RecordBatchStream,
    SendableRecordBatchStream, Statistics,
};

use arrow::array::{RecordBatch, RecordBatchOptions};
use arrow::compute::concat_batches;
use arrow::datatypes::{Fields, Schema, SchemaRef};
use datafusion_common::stats::Precision;
use datafusion_common::{
    assert_eq_or_internal_err, internal_err, JoinType, Result, ScalarValue,
};
use datafusion_execution::memory_pool::{MemoryConsumer, MemoryReservation};
use datafusion_execution::TaskContext;
use datafusion_physical_expr::equivalence::join_equivalence_properties;

use async_trait::async_trait;
use futures::{ready, Stream, StreamExt, TryStreamExt};

/// Data of the left side that is buffered into memory
#[derive(Debug)]
struct JoinLeftData {
    /// Single RecordBatch with all rows from the left side
    merged_batch: RecordBatch,
    /// Track memory reservation for merged_batch. Relies on drop
    /// semantics to release reservation when JoinLeftData is dropped.
    _reservation: MemoryReservation,
}

#[expect(rustdoc::private_intra_doc_links)]
/// Cross Join Execution Plan
///
/// This operator is used when there are no predicates between two tables and
/// returns the Cartesian product of the two tables.
///
/// Buffers the left input into memory and then streams batches from each
/// partition on the right input combining them with the buffered left input
/// to generate the output.
///
/// # Clone / Shared State
///
/// Note this structure includes a [`OnceAsync`] that is used to coordinate the
/// loading of the left side with the processing in each output stream.
/// Therefore it can not be [`Clone`]
#[derive(Debug)]
pub struct CrossJoinExec {
    /// left (build) side which gets loaded in memory
    pub left: Arc<dyn ExecutionPlan>,
    /// right (probe) side which are combined with left side
    pub right: Arc<dyn ExecutionPlan>,
    /// The schema once the join is applied
    schema: SchemaRef,
    /// Buffered copy of left (build) side in memory.
    ///
    /// This structure is *shared* across all output streams.
    ///
    /// Each output stream waits on the `OnceAsync` to signal the completion of
    /// the left side loading.
    left_fut: OnceAsync<JoinLeftData>,
    /// Execution plan metrics
    metrics: ExecutionPlanMetricsSet,
    /// Properties such as schema, equivalence properties, ordering, partitioning, etc.
    cache: PlanProperties,
}

impl CrossJoinExec {
    /// Create a new [CrossJoinExec].
    pub fn new(left: Arc<dyn ExecutionPlan>, right: Arc<dyn ExecutionPlan>) -> Self {
        // left then right
        let (all_columns, metadata) = {
            let left_schema = left.schema();
            let right_schema = right.schema();
            let left_fields = left_schema.fields().iter();
            let right_fields = right_schema.fields().iter();

            let mut metadata = left_schema.metadata().clone();
            metadata.extend(right_schema.metadata().clone());

            (
                left_fields.chain(right_fields).cloned().collect::<Fields>(),
                metadata,
            )
        };

        let schema = Arc::new(Schema::new(all_columns).with_metadata(metadata));
        let cache = Self::compute_properties(&left, &right, Arc::clone(&schema)).unwrap();

        CrossJoinExec {
            left,
            right,
            schema,
            left_fut: Default::default(),
            metrics: ExecutionPlanMetricsSet::default(),
            cache,
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

    /// This function creates the cache object that stores the plan properties such as schema, equivalence properties, ordering, partitioning, etc.
    fn compute_properties(
        left: &Arc<dyn ExecutionPlan>,
        right: &Arc<dyn ExecutionPlan>,
        schema: SchemaRef,
    ) -> Result<PlanProperties> {
        // Calculate equivalence properties
        // TODO: Check equivalence properties of cross join, it may preserve
        //       ordering in some cases.
        let eq_properties = join_equivalence_properties(
            left.equivalence_properties().clone(),
            right.equivalence_properties().clone(),
            &JoinType::Full,
            schema,
            &[false, false],
            None,
            &[],
        )?;

        // Get output partitioning:
        // TODO: Optimize the cross join implementation to generate M * N
        //       partitions.
        let output_partitioning = adjust_right_output_partitioning(
            right.output_partitioning(),
            left.schema().fields.len(),
        )?;

        Ok(PlanProperties::new(
            eq_properties,
            output_partitioning,
            EmissionType::Final,
            boundedness_from_children([left, right]),
        ))
    }

    /// Returns a new `ExecutionPlan` that computes the same join as this one,
    /// with the left and right inputs swapped using the  specified
    /// `partition_mode`.
    ///
    /// # Notes:
    ///
    /// This function should be called BEFORE inserting any repartitioning
    /// operators on the join's children. Check [`super::HashJoinExec::swap_inputs`]
    /// for more details.
    pub fn swap_inputs(&self) -> Result<Arc<dyn ExecutionPlan>> {
        let new_join =
            CrossJoinExec::new(Arc::clone(&self.right), Arc::clone(&self.left));
        reorder_output_after_swap(
            Arc::new(new_join),
            &self.left.schema(),
            &self.right.schema(),
        )
    }
}

/// Asynchronously collect the result of the left child
async fn load_left_input(
    stream: SendableRecordBatchStream,
    metrics: BuildProbeJoinMetrics,
    reservation: MemoryReservation,
) -> Result<JoinLeftData> {
    let left_schema = stream.schema();

    // Load all batches and count the rows
    let (batches, _metrics, reservation) = stream
        .try_fold(
            (Vec::new(), metrics, reservation),
            |(mut batches, metrics, mut reservation), batch| async {
                let batch_size = batch.get_array_memory_size();
                // Reserve memory for incoming batch
                reservation.try_grow(batch_size)?;
                // Update metrics
                metrics.build_mem_used.add(batch_size);
                metrics.build_input_batches.add(1);
                metrics.build_input_rows.add(batch.num_rows());
                // Push batch to output
                batches.push(batch);
                Ok((batches, metrics, reservation))
            },
        )
        .await?;

    let merged_batch = concat_batches(&left_schema, &batches)?;

    Ok(JoinLeftData {
        merged_batch,
        _reservation: reservation,
    })
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
            DisplayFormatType::TreeRender => {
                // no extra info to display
                Ok(())
            }
        }
    }
}

impl ExecutionPlan for CrossJoinExec {
    fn name(&self) -> &'static str {
        "CrossJoinExec"
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn properties(&self) -> &PlanProperties {
        &self.cache
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.left, &self.right]
    }

    fn metrics(&self) -> Option<MetricsSet> {
        Some(self.metrics.clone_inner())
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        Ok(Arc::new(CrossJoinExec::new(
            Arc::clone(&children[0]),
            Arc::clone(&children[1]),
        )))
    }

    fn reset_state(self: Arc<Self>) -> Result<Arc<dyn ExecutionPlan>> {
        let new_exec = CrossJoinExec {
            left: Arc::clone(&self.left),
            right: Arc::clone(&self.right),
            schema: Arc::clone(&self.schema),
            left_fut: Default::default(), // reset the build side!
            metrics: ExecutionPlanMetricsSet::default(),
            cache: self.cache.clone(),
        };
        Ok(Arc::new(new_exec))
    }

    fn required_input_distribution(&self) -> Vec<Distribution> {
        vec![
            Distribution::SinglePartition,
            Distribution::UnspecifiedDistribution,
        ]
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        assert_eq_or_internal_err!(
            self.left.output_partitioning().partition_count(),
            1,
            "Invalid CrossJoinExec, the output partition count of the left child must be 1,\
                 consider using CoalescePartitionsExec or the EnforceDistribution rule"
        );

        let stream = self.right.execute(partition, Arc::clone(&context))?;

        let join_metrics = BuildProbeJoinMetrics::new(partition, &self.metrics);

        // Initialization of operator-level reservation
        let reservation =
            MemoryConsumer::new("CrossJoinExec").register(context.memory_pool());

        let batch_size = context.session_config().batch_size();
        let enforce_batch_size_in_joins =
            context.session_config().enforce_batch_size_in_joins();

        let left_fut = self.left_fut.try_once(|| {
            let left_stream = self.left.execute(0, context)?;

            Ok(load_left_input(
                left_stream,
                join_metrics.clone(),
                reservation,
            ))
        })?;

        if enforce_batch_size_in_joins {
            Ok(Box::pin(CrossJoinStream {
                schema: Arc::clone(&self.schema),
                left_fut,
                right: stream,
                left_index: 0,
                join_metrics,
                state: CrossJoinStreamState::WaitBuildSide,
                left_data: RecordBatch::new_empty(self.left().schema()),
                batch_transformer: BatchSplitter::new(batch_size),
            }))
        } else {
            Ok(Box::pin(CrossJoinStream {
                schema: Arc::clone(&self.schema),
                left_fut,
                right: stream,
                left_index: 0,
                join_metrics,
                state: CrossJoinStreamState::WaitBuildSide,
                left_data: RecordBatch::new_empty(self.left().schema()),
                batch_transformer: NoopBatchTransformer::new(),
            }))
        }
    }

    fn statistics(&self) -> Result<Statistics> {
        self.partition_statistics(None)
    }

    fn partition_statistics(&self, partition: Option<usize>) -> Result<Statistics> {
        // Get the all partitions statistics of the left
        let left_stats = self.left.partition_statistics(None)?;
        let right_stats = self.right.partition_statistics(partition)?;

        Ok(stats_cartesian_product(left_stats, right_stats))
    }

    /// Tries to swap the projection with its input [`CrossJoinExec`]. If it can be done,
    /// it returns the new swapped version having the [`CrossJoinExec`] as the top plan.
    /// Otherwise, it returns None.
    fn try_swapping_with_projection(
        &self,
        projection: &ProjectionExec,
    ) -> Result<Option<Arc<dyn ExecutionPlan>>> {
        // Convert projected PhysicalExpr's to columns. If not possible, we cannot proceed.
        let Some(projection_as_columns) = physical_to_column_exprs(projection.expr())
        else {
            return Ok(None);
        };

        let (far_right_left_col_ind, far_left_right_col_ind) = join_table_borders(
            self.left().schema().fields().len(),
            &projection_as_columns,
        );

        if !join_allows_pushdown(
            &projection_as_columns,
            &self.schema(),
            far_right_left_col_ind,
            far_left_right_col_ind,
        ) {
            return Ok(None);
        }

        let (new_left, new_right) = new_join_children(
            &projection_as_columns,
            far_right_left_col_ind,
            far_left_right_col_ind,
            self.left(),
            self.right(),
        )?;

        Ok(Some(Arc::new(CrossJoinExec::new(
            Arc::new(new_left),
            Arc::new(new_right),
        ))))
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
            sum_value: s
                .sum_value
                .get_value()
                // Cast the row count into the same type as any existing sum value
                .and_then(|v| {
                    Precision::<ScalarValue>::from(right_row_count)
                        .cast_to(&v.data_type())
                        .ok()
                })
                .map(|row_count| s.sum_value.multiply(&row_count))
                .unwrap_or(Precision::Absent),
        })
        .chain(right_col_stats.into_iter().map(|s| {
            ColumnStatistics {
                null_count: s.null_count.multiply(&left_row_count),
                distinct_count: s.distinct_count,
                min_value: s.min_value,
                max_value: s.max_value,
                sum_value: s
                    .sum_value
                    .get_value()
                    // Cast the row count into the same type as any existing sum value
                    .and_then(|v| {
                        Precision::<ScalarValue>::from(left_row_count)
                            .cast_to(&v.data_type())
                            .ok()
                    })
                    .map(|row_count| s.sum_value.multiply(&row_count))
                    .unwrap_or(Precision::Absent),
            }
        }))
        .collect();

    Statistics {
        num_rows,
        total_byte_size,
        column_statistics: cross_join_stats,
    }
}

/// A stream that issues [RecordBatch]es as they arrive from the right  of the join.
struct CrossJoinStream<T> {
    /// Input schema
    schema: Arc<Schema>,
    /// Future for data from left side
    left_fut: OnceFut<JoinLeftData>,
    /// Right side stream
    right: SendableRecordBatchStream,
    /// Current value on the left
    left_index: usize,
    /// Join execution metrics
    join_metrics: BuildProbeJoinMetrics,
    /// State of the stream
    state: CrossJoinStreamState,
    /// Left data (copy of the entire buffered left side)
    left_data: RecordBatch,
    /// Batch transformer
    batch_transformer: T,
}

impl<T: BatchTransformer + Unpin + Send> RecordBatchStream for CrossJoinStream<T> {
    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }
}

/// Represents states of CrossJoinStream
enum CrossJoinStreamState {
    WaitBuildSide,
    FetchProbeBatch,
    /// Holds the currently processed right side batch
    BuildBatches(RecordBatch),
}

impl CrossJoinStreamState {
    /// Tries to extract RecordBatch from CrossJoinStreamState enum.
    /// Returns an error if state is not BuildBatches state.
    fn try_as_record_batch(&mut self) -> Result<&RecordBatch> {
        match self {
            CrossJoinStreamState::BuildBatches(rb) => Ok(rb),
            _ => internal_err!("Expected RecordBatch in BuildBatches state"),
        }
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
impl<T: BatchTransformer + Unpin + Send> Stream for CrossJoinStream<T> {
    type Item = Result<RecordBatch>;

    fn poll_next(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> Poll<Option<Self::Item>> {
        self.poll_next_impl(cx)
    }
}

impl<T: BatchTransformer> CrossJoinStream<T> {
    /// Separate implementation function that unpins the [`CrossJoinStream`] so
    /// that partial borrows work correctly
    fn poll_next_impl(
        &mut self,
        cx: &mut std::task::Context<'_>,
    ) -> Poll<Option<Result<RecordBatch>>> {
        loop {
            return match self.state {
                CrossJoinStreamState::WaitBuildSide => {
                    handle_state!(ready!(self.collect_build_side(cx)))
                }
                CrossJoinStreamState::FetchProbeBatch => {
                    handle_state!(ready!(self.fetch_probe_batch(cx)))
                }
                CrossJoinStreamState::BuildBatches(_) => {
                    let poll = handle_state!(self.build_batches());
                    self.join_metrics.baseline.record_poll(poll)
                }
            };
        }
    }

    /// Collects build (left) side of the join into the state. In case of an empty build batch,
    /// the execution terminates. Otherwise, the state is updated to fetch probe (right) batch.
    fn collect_build_side(
        &mut self,
        cx: &mut std::task::Context<'_>,
    ) -> Poll<Result<StatefulStreamResult<Option<RecordBatch>>>> {
        let build_timer = self.join_metrics.build_time.timer();
        let left_data = match ready!(self.left_fut.get(cx)) {
            Ok(left_data) => left_data,
            Err(e) => return Poll::Ready(Err(e)),
        };
        build_timer.done();

        let left_data = left_data.merged_batch.clone();
        let result = if left_data.num_rows() == 0 {
            StatefulStreamResult::Ready(None)
        } else {
            self.left_data = left_data;
            self.state = CrossJoinStreamState::FetchProbeBatch;
            StatefulStreamResult::Continue
        };
        Poll::Ready(Ok(result))
    }

    /// Fetches the probe (right) batch, updates the metrics, and save the batch in the state.
    /// Then, the state is updated to build result batches.
    fn fetch_probe_batch(
        &mut self,
        cx: &mut std::task::Context<'_>,
    ) -> Poll<Result<StatefulStreamResult<Option<RecordBatch>>>> {
        self.left_index = 0;
        let right_data = match ready!(self.right.poll_next_unpin(cx)) {
            Some(Ok(right_data)) => right_data,
            Some(Err(e)) => return Poll::Ready(Err(e)),
            None => return Poll::Ready(Ok(StatefulStreamResult::Ready(None))),
        };
        self.join_metrics.input_batches.add(1);
        self.join_metrics.input_rows.add(right_data.num_rows());

        self.state = CrossJoinStreamState::BuildBatches(right_data);
        Poll::Ready(Ok(StatefulStreamResult::Continue))
    }

    /// Joins the indexed row of left data with the current probe batch.
    /// If all the results are produced, the state is set to fetch new probe batch.
    fn build_batches(&mut self) -> Result<StatefulStreamResult<Option<RecordBatch>>> {
        let right_batch = self.state.try_as_record_batch()?;
        if self.left_index < self.left_data.num_rows() {
            match self.batch_transformer.next() {
                None => {
                    let join_timer = self.join_metrics.join_time.timer();
                    let result = build_batch(
                        self.left_index,
                        right_batch,
                        &self.left_data,
                        &self.schema,
                    );
                    join_timer.done();

                    self.batch_transformer.set_batch(result?);
                }
                Some((batch, last)) => {
                    if last {
                        self.left_index += 1;
                    }

                    return Ok(StatefulStreamResult::Ready(Some(batch)));
                }
            }
        } else {
            self.state = CrossJoinStreamState::FetchProbeBatch;
        }
        Ok(StatefulStreamResult::Continue)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::common;
    use crate::test::{assert_join_metrics, build_table_scan_i32};

    use datafusion_common::{assert_contains, test_util::batches_to_sort_string};
    use datafusion_execution::runtime_env::RuntimeEnvBuilder;
    use insta::assert_snapshot;

    async fn join_collect(
        left: Arc<dyn ExecutionPlan>,
        right: Arc<dyn ExecutionPlan>,
        context: Arc<TaskContext>,
    ) -> Result<(Vec<String>, Vec<RecordBatch>, MetricsSet)> {
        let join = CrossJoinExec::new(left, right);
        let columns_header = columns(&join.schema());

        let stream = join.execute(0, context)?;
        let batches = common::collect(stream).await?;
        let metrics = join.metrics().unwrap();

        Ok((columns_header, batches, metrics))
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
                    sum_value: Precision::Exact(ScalarValue::Int64(Some(42))),
                    null_count: Precision::Exact(0),
                },
                ColumnStatistics {
                    distinct_count: Precision::Exact(1),
                    max_value: Precision::Exact(ScalarValue::from("x")),
                    min_value: Precision::Exact(ScalarValue::from("a")),
                    sum_value: Precision::Absent,
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
                sum_value: Precision::Exact(ScalarValue::Int64(Some(20))),
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
                    sum_value: Precision::Exact(ScalarValue::Int64(Some(
                        42 * right_row_count as i64,
                    ))),
                    null_count: Precision::Exact(0),
                },
                ColumnStatistics {
                    distinct_count: Precision::Exact(1),
                    max_value: Precision::Exact(ScalarValue::from("x")),
                    min_value: Precision::Exact(ScalarValue::from("a")),
                    sum_value: Precision::Absent,
                    null_count: Precision::Exact(3 * right_row_count),
                },
                ColumnStatistics {
                    distinct_count: Precision::Exact(3),
                    max_value: Precision::Exact(ScalarValue::Int64(Some(12))),
                    min_value: Precision::Exact(ScalarValue::Int64(Some(0))),
                    sum_value: Precision::Exact(ScalarValue::Int64(Some(
                        20 * left_row_count as i64,
                    ))),
                    null_count: Precision::Exact(2 * left_row_count),
                },
            ],
        };

        assert_eq!(result, expected);
    }

    #[tokio::test]
    async fn test_stats_cartesian_product_with_unknown_size() {
        let left_row_count = 11;

        let left = Statistics {
            num_rows: Precision::Exact(left_row_count),
            total_byte_size: Precision::Exact(23),
            column_statistics: vec![
                ColumnStatistics {
                    distinct_count: Precision::Exact(5),
                    max_value: Precision::Exact(ScalarValue::Int64(Some(21))),
                    min_value: Precision::Exact(ScalarValue::Int64(Some(-4))),
                    sum_value: Precision::Exact(ScalarValue::Int64(Some(42))),
                    null_count: Precision::Exact(0),
                },
                ColumnStatistics {
                    distinct_count: Precision::Exact(1),
                    max_value: Precision::Exact(ScalarValue::from("x")),
                    min_value: Precision::Exact(ScalarValue::from("a")),
                    sum_value: Precision::Absent,
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
                sum_value: Precision::Exact(ScalarValue::Int64(Some(20))),
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
                    sum_value: Precision::Absent, // we don't know the row count on the right
                    null_count: Precision::Absent, // we don't know the row count on the right
                },
                ColumnStatistics {
                    distinct_count: Precision::Exact(1),
                    max_value: Precision::Exact(ScalarValue::from("x")),
                    min_value: Precision::Exact(ScalarValue::from("a")),
                    sum_value: Precision::Absent,
                    null_count: Precision::Absent, // we don't know the row count on the right
                },
                ColumnStatistics {
                    distinct_count: Precision::Exact(3),
                    max_value: Precision::Exact(ScalarValue::Int64(Some(12))),
                    min_value: Precision::Exact(ScalarValue::Int64(Some(0))),
                    sum_value: Precision::Exact(ScalarValue::Int64(Some(
                        20 * left_row_count as i64,
                    ))),
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

        let (columns, batches, metrics) = join_collect(left, right, task_ctx).await?;

        assert_eq!(columns, vec!["a1", "b1", "c1", "a2", "b2", "c2"]);

        assert_snapshot!(batches_to_sort_string(&batches), @r#"
            +----+----+----+----+----+----+
            | a1 | b1 | c1 | a2 | b2 | c2 |
            +----+----+----+----+----+----+
            | 1  | 4  | 7  | 10 | 12 | 14 |
            | 1  | 4  | 7  | 11 | 13 | 15 |
            | 2  | 5  | 8  | 10 | 12 | 14 |
            | 2  | 5  | 8  | 11 | 13 | 15 |
            | 3  | 6  | 9  | 10 | 12 | 14 |
            | 3  | 6  | 9  | 11 | 13 | 15 |
            +----+----+----+----+----+----+
            "#);

        assert_join_metrics!(metrics, 6);

        Ok(())
    }

    #[tokio::test]
    async fn test_overallocation() -> Result<()> {
        let runtime = RuntimeEnvBuilder::new()
            .with_memory_limit(100, 1.0)
            .build_arc()?;
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
            "Resources exhausted: Additional allocation failed for CrossJoinExec with top memory consumers (across reservations) as:\n  CrossJoinExec"
        );

        Ok(())
    }

    /// Returns the column names on the schema
    fn columns(schema: &Schema) -> Vec<String> {
        schema.fields().iter().map(|f| f.name().clone()).collect()
    }
}
