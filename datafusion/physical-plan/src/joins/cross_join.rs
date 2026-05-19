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

//! Defines the cross join plan for loading both sides of the cross join
//! and producing M×N output partitions in parallel.

use std::{sync::Arc, task::Poll};

use super::utils::{
    BatchSplitter, BatchTransformer, BuildProbeJoinMetrics, NoopBatchTransformer,
    OnceAsync, OnceFut, StatefulStreamResult, reorder_output_after_swap,
};
use crate::execution_plan::{EmissionType, boundedness_from_children};
use crate::metrics::{ExecutionPlanMetricsSet, MetricsSet};
use crate::projection::{
    ProjectionExec, join_allows_pushdown, join_table_borders, new_join_children,
    physical_to_column_exprs,
};
use crate::{
    ColumnStatistics, DisplayAs, DisplayFormatType, Distribution, ExecutionPlan,
    ExecutionPlanProperties, PlanProperties, RecordBatchStream,
    SendableRecordBatchStream, Statistics, check_if_same_properties, handle_state,
};

use arrow::array::{RecordBatch, RecordBatchOptions};
use arrow::compute::concat_batches;
use arrow::datatypes::{Fields, Schema, SchemaRef};
use datafusion_common::stats::Precision;
use datafusion_common::tree_node::TreeNodeRecursion;
use datafusion_common::{JoinType, Result, ScalarValue, internal_err};
use datafusion_execution::TaskContext;
use datafusion_execution::memory_pool::{MemoryConsumer, MemoryReservation};
use datafusion_physical_expr::Partitioning;
use datafusion_physical_expr::PhysicalExpr;
use datafusion_physical_expr::equivalence::join_equivalence_properties;

use async_trait::async_trait;
use futures::{Stream, TryStreamExt, ready};

/// Data of the left side that is buffered into memory
#[derive(Debug)]
struct JoinLeftData {
    /// Single RecordBatch with all rows from the left side
    merged_batch: RecordBatch,
    /// Track memory reservation for merged_batch. Relies on drop
    /// semantics to release reservation when JoinLeftData is dropped.
    _reservation: MemoryReservation,
}

/// Data of the right side that is buffered into memory
#[derive(Debug)]
struct JoinRightData {
    /// Individual batches from the right side (not merged, to avoid allocation spike)
    batches: Arc<Vec<RecordBatch>>,
    /// Track memory reservation. Relies on drop semantics to release.
    _reservation: MemoryReservation,
}

#[expect(rustdoc::private_intra_doc_links)]
/// Cross Join Execution Plan
///
/// This operator is used when there are no predicates between two tables and
/// returns the Cartesian product of the two tables.
///
/// Produces M×N output partitions where M and N are the partition counts of
/// the left and right inputs respectively. Each output partition (i, j)
/// computes the cartesian product of left partition i with right partition j.
///
/// Both sides are buffered via [`OnceAsync`]: tasks sharing the same left
/// (or right) partition index share a single buffered copy through `Arc`.
///
/// # Clone / Shared State
///
/// Note this structure includes [`OnceAsync`] vectors that coordinate the
/// loading of both sides with the processing in each output stream.
/// Therefore it can not be [`Clone`]
#[derive(Debug)]
pub struct CrossJoinExec {
    /// left (build) side which gets loaded in memory
    pub left: Arc<dyn ExecutionPlan>,
    /// right (probe) side which are combined with left side
    pub right: Arc<dyn ExecutionPlan>,
    /// The schema once the join is applied
    schema: SchemaRef,
    /// Buffered left partitions. One entry per left partition (M entries).
    /// Tasks sharing the same left partition index share the buffer.
    left_futs: Vec<OnceAsync<JoinLeftData>>,
    /// Buffered right partitions. One entry per right partition (N entries).
    /// Tasks sharing the same right partition index share the buffer.
    right_futs: Vec<OnceAsync<JoinRightData>>,
    /// Execution plan metrics
    metrics: ExecutionPlanMetricsSet,
    /// Properties such as schema, equivalence properties, ordering, partitioning, etc.
    cache: Arc<PlanProperties>,
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

        let left_count = left.output_partitioning().partition_count();
        let right_count = right.output_partitioning().partition_count();

        CrossJoinExec {
            left,
            right,
            schema,
            left_futs: (0..left_count).map(|_| OnceAsync::default()).collect(),
            right_futs: (0..right_count).map(|_| OnceAsync::default()).collect(),
            metrics: ExecutionPlanMetricsSet::default(),
            cache: Arc::new(cache),
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

        // M×N output partitions: each (left_i, right_j) pair is independent.
        let left_count = left.output_partitioning().partition_count();
        let right_count = right.output_partitioning().partition_count();
        let output_partitioning =
            Partitioning::UnknownPartitioning(left_count * right_count);

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

    fn with_new_children_and_same_properties(
        &self,
        mut children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Self {
        let left = children.swap_remove(0);
        let right = children.swap_remove(0);

        let left_count = left.output_partitioning().partition_count();
        let right_count = right.output_partitioning().partition_count();

        Self {
            left,
            right,
            metrics: ExecutionPlanMetricsSet::new(),
            left_futs: (0..left_count).map(|_| OnceAsync::default()).collect(),
            right_futs: (0..right_count).map(|_| OnceAsync::default()).collect(),
            cache: Arc::clone(&self.cache),
            schema: Arc::clone(&self.schema),
        }
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
            |(mut batches, metrics, reservation), batch| async {
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

/// Asynchronously collect the result of the right child into buffered batches.
/// Unlike load_left_input, we do NOT concat into a single batch — right batches
/// are accessed whole during the cross product, so no merge is needed.
async fn load_right_input(
    stream: SendableRecordBatchStream,
    metrics: BuildProbeJoinMetrics,
    reservation: MemoryReservation,
) -> Result<JoinRightData> {
    let (batches, _metrics, reservation) = stream
        .try_fold(
            (Vec::new(), metrics, reservation),
            |(mut batches, metrics, reservation), batch| async {
                let batch_size = batch.get_array_memory_size();
                reservation.try_grow(batch_size)?;
                metrics.input_batches.add(1);
                metrics.input_rows.add(batch.num_rows());
                batches.push(batch);
                Ok((batches, metrics, reservation))
            },
        )
        .await?;

    Ok(JoinRightData {
        batches: Arc::new(batches),
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

    fn properties(&self) -> &Arc<PlanProperties> {
        &self.cache
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.left, &self.right]
    }

    fn metrics(&self) -> Option<MetricsSet> {
        Some(self.metrics.clone_inner())
    }

    fn apply_expressions(
        &self,
        _f: &mut dyn FnMut(&dyn PhysicalExpr) -> Result<TreeNodeRecursion>,
    ) -> Result<TreeNodeRecursion> {
        // CrossJoin has no join conditions or expressions
        Ok(TreeNodeRecursion::Continue)
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        check_if_same_properties!(self, children);
        Ok(Arc::new(CrossJoinExec::new(
            Arc::clone(&children[0]),
            Arc::clone(&children[1]),
        )))
    }

    fn reset_state(self: Arc<Self>) -> Result<Arc<dyn ExecutionPlan>> {
        let left_count = self.left.output_partitioning().partition_count();
        let right_count = self.right.output_partitioning().partition_count();
        let new_exec = CrossJoinExec {
            left: Arc::clone(&self.left),
            right: Arc::clone(&self.right),
            schema: Arc::clone(&self.schema),
            left_futs: (0..left_count).map(|_| OnceAsync::default()).collect(),
            right_futs: (0..right_count).map(|_| OnceAsync::default()).collect(),
            metrics: ExecutionPlanMetricsSet::default(),
            cache: Arc::clone(&self.cache),
        };
        Ok(Arc::new(new_exec))
    }

    fn required_input_distribution(&self) -> Vec<Distribution> {
        vec![
            Distribution::UnspecifiedDistribution,
            Distribution::UnspecifiedDistribution,
        ]
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        let right_count = self.right.output_partitioning().partition_count();
        let left_partition = partition / right_count;
        let right_partition = partition % right_count;

        let join_metrics = BuildProbeJoinMetrics::new(partition, &self.metrics);

        let batch_size = context.session_config().batch_size();
        let enforce_batch_size_in_joins =
            context.session_config().enforce_batch_size_in_joins();

        let left_fut = self.left_futs[left_partition].try_once(|| {
            let reservation = MemoryConsumer::new("CrossJoinExec[left]")
                .register(context.memory_pool());
            let left_stream = self.left.execute(left_partition, Arc::clone(&context))?;
            Ok(load_left_input(
                left_stream,
                join_metrics.clone(),
                reservation,
            ))
        })?;

        let right_fut = self.right_futs[right_partition].try_once(|| {
            let reservation = MemoryConsumer::new("CrossJoinExec[right]")
                .register(context.memory_pool());
            let right_stream =
                self.right.execute(right_partition, Arc::clone(&context))?;
            Ok(load_right_input(
                right_stream,
                join_metrics.clone(),
                reservation,
            ))
        })?;

        if enforce_batch_size_in_joins {
            Ok(Box::pin(CrossJoinStream {
                schema: Arc::clone(&self.schema),
                left_fut,
                right_fut,
                left_index: 0,
                right_batch_index: 0,
                join_metrics,
                state: CrossJoinStreamState::WaitBuildSide,
                left_data: RecordBatch::new_empty(self.left().schema()),
                right_data: Arc::new(vec![]),
                batch_transformer: BatchSplitter::new(batch_size),
            }))
        } else {
            Ok(Box::pin(CrossJoinStream {
                schema: Arc::clone(&self.schema),
                left_fut,
                right_fut,
                left_index: 0,
                right_batch_index: 0,
                join_metrics,
                state: CrossJoinStreamState::WaitBuildSide,
                left_data: RecordBatch::new_empty(self.left().schema()),
                right_data: Arc::new(vec![]),
                batch_transformer: NoopBatchTransformer::new(),
            }))
        }
    }

    fn partition_statistics(&self, partition: Option<usize>) -> Result<Arc<Statistics>> {
        let (left_part, right_part) = match partition {
            Some(p) => {
                let right_count = self.right.output_partitioning().partition_count();
                (Some(p / right_count), Some(p % right_count))
            }
            None => (None, None),
        };
        let left_stats = Arc::unwrap_or_clone(self.left.partition_statistics(left_part)?);
        let right_stats =
            Arc::unwrap_or_clone(self.right.partition_statistics(right_part)?);

        Ok(Arc::new(stats_cartesian_product(left_stats, right_stats)))
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
        .map(|s| {
            let widened_sum = s.sum_value.cast_to_sum_type();
            ColumnStatistics {
                null_count: s.null_count.multiply(&right_row_count),
                distinct_count: s.distinct_count,
                min_value: s.min_value,
                max_value: s.max_value,
                sum_value: widened_sum
                    .get_value()
                    // Cast the row count into the same type as any existing sum value
                    .and_then(|v| {
                        Precision::<ScalarValue>::from(right_row_count)
                            .cast_to(&v.data_type())
                            .ok()
                    })
                    .map(|row_count| widened_sum.multiply(&row_count))
                    .unwrap_or(Precision::Absent),
                byte_size: Precision::Absent,
            }
        })
        .chain(right_col_stats.into_iter().map(|s| {
            let widened_sum = s.sum_value.cast_to_sum_type();
            ColumnStatistics {
                null_count: s.null_count.multiply(&left_row_count),
                distinct_count: s.distinct_count,
                min_value: s.min_value,
                max_value: s.max_value,
                sum_value: widened_sum
                    .get_value()
                    // Cast the row count into the same type as any existing sum value
                    .and_then(|v| {
                        Precision::<ScalarValue>::from(left_row_count)
                            .cast_to(&v.data_type())
                            .ok()
                    })
                    .map(|row_count| widened_sum.multiply(&row_count))
                    .unwrap_or(Precision::Absent),
                byte_size: Precision::Absent,
            }
        }))
        .collect();

    Statistics {
        num_rows,
        total_byte_size,
        column_statistics: cross_join_stats,
    }
}

/// A stream that produces the cartesian product of one left partition with
/// one right partition. Both sides are pre-buffered via OnceAsync.
struct CrossJoinStream<T> {
    /// Input schema
    schema: Arc<Schema>,
    /// Future for data from left side
    left_fut: OnceFut<JoinLeftData>,
    /// Future for data from right side (buffered)
    right_fut: OnceFut<JoinRightData>,
    /// Current row index within the left batch
    left_index: usize,
    /// Current batch index within the buffered right batches
    right_batch_index: usize,
    /// Join execution metrics
    join_metrics: BuildProbeJoinMetrics,
    /// State of the stream
    state: CrossJoinStreamState,
    /// Left data (merged batch for row-level access)
    left_data: RecordBatch,
    /// Right data (buffered batches for batch-level iteration)
    right_data: Arc<Vec<RecordBatch>>,
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
                    handle_state!(self.fetch_probe_batch())
                }
                CrossJoinStreamState::BuildBatches(_) => {
                    let poll = handle_state!(self.build_batches());
                    self.join_metrics.baseline.record_poll(poll)
                }
            };
        }
    }

    /// Collects both left and right sides into the state. Both futures are
    /// polled on every call to ensure they make progress in parallel.
    fn collect_build_side(
        &mut self,
        cx: &mut std::task::Context<'_>,
    ) -> Poll<Result<StatefulStreamResult<Option<RecordBatch>>>> {
        let build_timer = self.join_metrics.build_time.timer();

        // Poll both futures every time to drive them forward concurrently.
        let left_poll = self.left_fut.get(cx);
        let right_poll = self.right_fut.get(cx);

        let left_data = match left_poll {
            Poll::Ready(Ok(data)) => data,
            Poll::Ready(Err(e)) => return Poll::Ready(Err(e)),
            Poll::Pending => return Poll::Pending,
        };

        let right_data = match right_poll {
            Poll::Ready(Ok(data)) => data,
            Poll::Ready(Err(e)) => return Poll::Ready(Err(e)),
            Poll::Pending => return Poll::Pending,
        };
        build_timer.done();

        let left_batch = left_data.merged_batch.clone();
        if left_batch.num_rows() == 0 || right_data.batches.is_empty() {
            return Poll::Ready(Ok(StatefulStreamResult::Ready(None)));
        }

        self.left_data = left_batch;
        self.right_data = Arc::clone(&right_data.batches);
        self.state = CrossJoinStreamState::FetchProbeBatch;
        Poll::Ready(Ok(StatefulStreamResult::Continue))
    }

    /// Advances to the next buffered right batch. This is synchronous since
    /// right data is already fully loaded in memory.
    fn fetch_probe_batch(&mut self) -> Result<StatefulStreamResult<Option<RecordBatch>>> {
        self.left_index = 0;
        if self.right_batch_index < self.right_data.len() {
            let right_batch = self.right_data[self.right_batch_index].clone();
            self.right_batch_index += 1;
            self.state = CrossJoinStreamState::BuildBatches(right_batch);
            Ok(StatefulStreamResult::Continue)
        } else {
            Ok(StatefulStreamResult::Ready(None))
        }
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
                    byte_size: Precision::Absent,
                },
                ColumnStatistics {
                    distinct_count: Precision::Exact(1),
                    max_value: Precision::Exact(ScalarValue::from("x")),
                    min_value: Precision::Exact(ScalarValue::from("a")),
                    sum_value: Precision::Absent,
                    null_count: Precision::Exact(3),
                    byte_size: Precision::Absent,
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
                byte_size: Precision::Absent,
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
                    byte_size: Precision::Absent,
                },
                ColumnStatistics {
                    distinct_count: Precision::Exact(1),
                    max_value: Precision::Exact(ScalarValue::from("x")),
                    min_value: Precision::Exact(ScalarValue::from("a")),
                    sum_value: Precision::Absent,
                    null_count: Precision::Exact(3 * right_row_count),
                    byte_size: Precision::Absent,
                },
                ColumnStatistics {
                    distinct_count: Precision::Exact(3),
                    max_value: Precision::Exact(ScalarValue::Int64(Some(12))),
                    min_value: Precision::Exact(ScalarValue::Int64(Some(0))),
                    sum_value: Precision::Exact(ScalarValue::Int64(Some(
                        20 * left_row_count as i64,
                    ))),
                    null_count: Precision::Exact(2 * left_row_count),
                    byte_size: Precision::Absent,
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
                    byte_size: Precision::Absent,
                },
                ColumnStatistics {
                    distinct_count: Precision::Exact(1),
                    max_value: Precision::Exact(ScalarValue::from("x")),
                    min_value: Precision::Exact(ScalarValue::from("a")),
                    sum_value: Precision::Absent,
                    null_count: Precision::Exact(3),
                    byte_size: Precision::Absent,
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
                byte_size: Precision::Absent,
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
                    byte_size: Precision::Absent,
                },
                ColumnStatistics {
                    distinct_count: Precision::Exact(1),
                    max_value: Precision::Exact(ScalarValue::from("x")),
                    min_value: Precision::Exact(ScalarValue::from("a")),
                    sum_value: Precision::Absent,
                    null_count: Precision::Absent, // we don't know the row count on the right
                    byte_size: Precision::Absent,
                },
                ColumnStatistics {
                    distinct_count: Precision::Exact(3),
                    max_value: Precision::Exact(ScalarValue::Int64(Some(12))),
                    min_value: Precision::Exact(ScalarValue::Int64(Some(0))),
                    sum_value: Precision::Exact(ScalarValue::Int64(Some(
                        20 * left_row_count as i64,
                    ))),
                    null_count: Precision::Exact(2 * left_row_count),
                    byte_size: Precision::Absent,
                },
            ],
        };

        assert_eq!(result, expected);
    }

    #[tokio::test]
    async fn test_stats_cartesian_product_unsigned_sum_widens_to_u64() {
        let left_row_count = 2;
        let right_row_count = 3;

        let left = Statistics {
            num_rows: Precision::Exact(left_row_count),
            total_byte_size: Precision::Exact(10),
            column_statistics: vec![ColumnStatistics {
                distinct_count: Precision::Exact(2),
                max_value: Precision::Exact(ScalarValue::UInt32(Some(10))),
                min_value: Precision::Exact(ScalarValue::UInt32(Some(1))),
                sum_value: Precision::Exact(ScalarValue::UInt32(Some(7))),
                null_count: Precision::Exact(0),
                byte_size: Precision::Absent,
            }],
        };

        let right = Statistics {
            num_rows: Precision::Exact(right_row_count),
            total_byte_size: Precision::Exact(10),
            column_statistics: vec![ColumnStatistics {
                distinct_count: Precision::Exact(3),
                max_value: Precision::Exact(ScalarValue::UInt32(Some(12))),
                min_value: Precision::Exact(ScalarValue::UInt32(Some(0))),
                sum_value: Precision::Exact(ScalarValue::UInt32(Some(11))),
                null_count: Precision::Exact(0),
                byte_size: Precision::Absent,
            }],
        };

        let result = stats_cartesian_product(left, right);

        assert_eq!(
            result.column_statistics[0].sum_value,
            Precision::Exact(ScalarValue::UInt64(Some(21)))
        );
        assert_eq!(
            result.column_statistics[1].sum_value,
            Precision::Exact(ScalarValue::UInt64(Some(22)))
        );
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

        assert_snapshot!(batches_to_sort_string(&batches), @r"
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
        ");

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
            "Resources exhausted: Additional allocation failed for CrossJoinExec[left] with top memory consumers (across reservations) as:\n  CrossJoinExec[left]"
        );

        Ok(())
    }

    /// Returns the column names on the schema
    fn columns(schema: &Schema) -> Vec<String> {
        schema.fields().iter().map(|f| f.name().clone()).collect()
    }

    /// Helper to create a multi-partition exec from separate row groups
    fn build_multi_partition_i32(
        col_name: &str,
        partitions_data: Vec<Vec<i32>>,
    ) -> Arc<dyn ExecutionPlan> {
        use arrow::array::Int32Array;
        use arrow::datatypes::{DataType, Field};

        let schema = Arc::new(Schema::new(vec![Field::new(
            col_name,
            DataType::Int32,
            false,
        )]));
        let partitions: Vec<Vec<RecordBatch>> = partitions_data
            .into_iter()
            .map(|values| {
                let arr = Arc::new(Int32Array::from(values)) as arrow::array::ArrayRef;
                vec![RecordBatch::try_new(Arc::clone(&schema), vec![arr]).unwrap()]
            })
            .collect();
        crate::test::TestMemoryExec::try_new_exec(&partitions, schema, None).unwrap()
    }

    #[tokio::test]
    async fn test_multi_partition_cross_join() -> Result<()> {
        let task_ctx = Arc::new(TaskContext::default());

        // Left: M=2 partitions, 3 rows total
        let left = build_multi_partition_i32("a", vec![vec![1, 2], vec![3]]);

        // Right: N=3 partitions, 3 rows total
        let right = build_multi_partition_i32("b", vec![vec![10], vec![20], vec![30]]);

        let join = CrossJoinExec::new(left, right);

        // Verify M×N = 6 output partitions
        assert_eq!(join.properties().output_partitioning().partition_count(), 6);

        // Collect all 6 partitions
        let mut all_batches = vec![];
        for i in 0..6 {
            let stream = join.execute(i, Arc::clone(&task_ctx))?;
            let batches = common::collect(stream).await?;
            all_batches.extend(batches);
        }

        // Should produce 3×3 = 9 rows total
        assert_snapshot!(batches_to_sort_string(&all_batches), @r"
        +---+----+
        | a | b  |
        +---+----+
        | 1 | 10 |
        | 1 | 20 |
        | 1 | 30 |
        | 2 | 10 |
        | 2 | 20 |
        | 2 | 30 |
        | 3 | 10 |
        | 3 | 20 |
        | 3 | 30 |
        +---+----+
        ");

        Ok(())
    }

    #[tokio::test]
    async fn test_empty_left_terminates() -> Result<()> {
        let task_ctx = Arc::new(TaskContext::default());

        // Left: empty (0 rows, 1 partition)
        let left = build_multi_partition_i32("a", vec![vec![]]);

        // Right: non-empty
        let right = build_multi_partition_i32("b", vec![vec![10, 20]]);

        let join = CrossJoinExec::new(left, right);
        let stream = join.execute(0, Arc::clone(&task_ctx))?;
        let batches = common::collect(stream).await?;

        // Cross join with empty left produces 0 rows
        let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total_rows, 0);

        Ok(())
    }

    #[tokio::test]
    async fn test_empty_right_terminates() -> Result<()> {
        let task_ctx = Arc::new(TaskContext::default());

        // Left: non-empty
        let left = build_multi_partition_i32("a", vec![vec![1, 2, 3]]);

        // Right: empty (0 rows, 1 partition)
        let right = build_multi_partition_i32("b", vec![vec![]]);

        let join = CrossJoinExec::new(left, right);
        let stream = join.execute(0, Arc::clone(&task_ctx))?;
        let batches = common::collect(stream).await?;

        // Cross join with empty right produces 0 rows
        let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total_rows, 0);

        Ok(())
    }

    #[tokio::test]
    async fn test_right_memory_reservation() -> Result<()> {
        use arrow::array::{Array, Int32Array};
        use datafusion_execution::memory_pool::{GreedyMemoryPool, MemoryPool};

        let pool = Arc::new(GreedyMemoryPool::new(10 * 1024 * 1024));
        let runtime = RuntimeEnvBuilder::new()
            .with_memory_pool(Arc::clone(&pool) as _)
            .build_arc()?;
        let task_ctx = Arc::new(TaskContext::default().with_runtime(runtime));

        let left = build_multi_partition_i32("a", vec![vec![1, 2, 3]]);
        let right = build_multi_partition_i32("b", vec![vec![10, 20, 30, 40, 50]]);

        let left_size = Int32Array::from(vec![1, 2, 3]).get_array_memory_size();
        let right_size =
            Int32Array::from(vec![10, 20, 30, 40, 50]).get_array_memory_size();

        let join = CrossJoinExec::new(left, right);
        let stream = join.execute(0, Arc::clone(&task_ctx))?;
        let _batches = common::collect(stream).await?;

        // While OnceAsync buffers are alive, pool must reflect both sides
        let reserved = pool.reserved();
        assert!(
            reserved >= left_size + right_size,
            "pool.reserved() ({reserved}) should be >= left ({left_size}) + right ({right_size})"
        );

        // Drop join to release OnceAsync buffers
        drop(join);

        assert_eq!(
            pool.reserved(),
            0,
            "All memory should be released after join is dropped"
        );

        Ok(())
    }
}
