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

//! Defines the nested loop join plan, it supports all [`JoinType`].
//! The nested loop join can execute in parallel by partitions and it is
//! determined by the [`JoinType`].

use std::any::Any;
use std::fmt::Formatter;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::task::Poll;

use super::utils::{
    asymmetric_join_output_partitioning, need_produce_result_in_final, BatchSplitter,
    BatchTransformer, NoopBatchTransformer, StatefulStreamResult,
};
use crate::coalesce_partitions::CoalescePartitionsExec;
use crate::joins::utils::{
    adjust_indices_by_join_type, apply_join_filter_to_indices, build_batch_from_indices,
    build_join_schema, check_join_is_valid, estimate_join_statistics,
    get_final_indices_from_bit_map, BuildProbeJoinMetrics, ColumnIndex, JoinFilter,
    OnceAsync, OnceFut,
};
use crate::metrics::{ExecutionPlanMetricsSet, MetricsSet};
use crate::{
    execution_mode_from_children, handle_state, DisplayAs, DisplayFormatType,
    Distribution, ExecutionMode, ExecutionPlan, ExecutionPlanProperties, PlanProperties,
    RecordBatchStream, SendableRecordBatchStream,
};

use arrow::array::{BooleanBufferBuilder, UInt32Array, UInt64Array};
use arrow::compute::concat_batches;
use arrow::datatypes::{Schema, SchemaRef};
use arrow::record_batch::RecordBatch;
use arrow::util::bit_util;
use datafusion_common::{
    exec_datafusion_err, internal_err, JoinSide, Result, Statistics,
};
use datafusion_execution::memory_pool::{MemoryConsumer, MemoryReservation};
use datafusion_execution::TaskContext;
use datafusion_expr::JoinType;
use datafusion_physical_expr::equivalence::join_equivalence_properties;

use futures::{ready, Stream, StreamExt, TryStreamExt};
use parking_lot::Mutex;

/// Shared bitmap for visited left-side indices
type SharedBitmapBuilder = Mutex<BooleanBufferBuilder>;
/// Left (build-side) data
struct JoinLeftData {
    /// Build-side data collected to single batch
    batch: RecordBatch,
    /// Shared bitmap builder for visited left indices
    bitmap: SharedBitmapBuilder,
    /// Counter of running probe-threads, potentially able to update `bitmap`
    probe_threads_counter: AtomicUsize,
    /// Memory reservation for tracking batch and bitmap
    /// Cleared on `JoinLeftData` drop
    #[allow(dead_code)]
    reservation: MemoryReservation,
}

impl JoinLeftData {
    fn new(
        batch: RecordBatch,
        bitmap: SharedBitmapBuilder,
        probe_threads_counter: AtomicUsize,
        reservation: MemoryReservation,
    ) -> Self {
        Self {
            batch,
            bitmap,
            probe_threads_counter,
            reservation,
        }
    }

    fn batch(&self) -> &RecordBatch {
        &self.batch
    }

    fn bitmap(&self) -> &SharedBitmapBuilder {
        &self.bitmap
    }

    /// Decrements counter of running threads, and returns `true`
    /// if caller is the last running thread
    fn report_probe_completed(&self) -> bool {
        self.probe_threads_counter.fetch_sub(1, Ordering::Relaxed) == 1
    }
}

/// NestedLoopJoinExec is build-probe join operator, whose main task is to
/// perform joins without any equijoin conditions in `ON` clause.
///
/// Execution consists of following phases:
///
/// #### 1. Build phase
/// Collecting build-side data in memory, by polling all available data from build-side input.
/// Due to the absence of equijoin conditions, it's not possible to partition build-side data
/// across multiple threads of the operator, so build-side is always collected in a single
/// batch shared across all threads.
/// The operator always considers LEFT input as build-side input, so it's crucial to adjust
/// smaller input to be the LEFT one. Normally this selection is handled by physical optimizer.
///
/// #### 2. Probe phase
/// Sequentially polling batches from the probe-side input and processing them according to the
/// following logic:
/// - apply join filter (`ON` clause) to Cartesian product of probe batch and build side data
///   -- filter evaluation is executed once per build-side data row
/// - update shared bitmap of joined ("visited") build-side row indices, if required -- allows
///   to produce unmatched build-side data in case of e.g. LEFT/FULL JOIN after probing phase
///   completed
/// - perform join index alignment is required -- depending on `JoinType`
/// - produce output join batch
///
/// Probing phase is executed in parallel, according to probe-side input partitioning -- one
/// thread per partition. After probe input is exhausted, each thread **ATTEMPTS** to produce
/// unmatched build-side data.
///
/// #### 3. Producing unmatched build-side data
/// Producing unmatched build-side data as an output batch, after probe input is exhausted.
/// This step is also executed in parallel (once per probe input partition), and to avoid
/// duplicate output of unmatched data (due to shared nature build-side data), each thread
/// "reports" about probe phase completion (which means that "visited" bitmap won't be
/// updated anymore), and only the last thread, reporting about completion, will return output.
///
#[derive(Debug)]
pub struct NestedLoopJoinExec {
    /// left side
    pub(crate) left: Arc<dyn ExecutionPlan>,
    /// right side
    pub(crate) right: Arc<dyn ExecutionPlan>,
    /// Filters which are applied while finding matching rows
    pub(crate) filter: Option<JoinFilter>,
    /// How the join is performed
    pub(crate) join_type: JoinType,
    /// The schema once the join is applied
    schema: SchemaRef,
    /// Build-side data
    inner_table: OnceAsync<JoinLeftData>,
    /// Information of index and left / right placement of columns
    column_indices: Vec<ColumnIndex>,
    /// Execution metrics
    metrics: ExecutionPlanMetricsSet,
    /// Cache holding plan properties like equivalences, output partitioning etc.
    cache: PlanProperties,
}

impl NestedLoopJoinExec {
    /// Try to create a new [`NestedLoopJoinExec`]
    pub fn try_new(
        left: Arc<dyn ExecutionPlan>,
        right: Arc<dyn ExecutionPlan>,
        filter: Option<JoinFilter>,
        join_type: &JoinType,
    ) -> Result<Self> {
        let left_schema = left.schema();
        let right_schema = right.schema();
        check_join_is_valid(&left_schema, &right_schema, &[])?;
        let (schema, column_indices) =
            build_join_schema(&left_schema, &right_schema, join_type);
        let schema = Arc::new(schema);
        let cache =
            Self::compute_properties(&left, &right, Arc::clone(&schema), *join_type);

        Ok(NestedLoopJoinExec {
            left,
            right,
            filter,
            join_type: *join_type,
            schema,
            inner_table: Default::default(),
            column_indices,
            metrics: Default::default(),
            cache,
        })
    }

    /// left side
    pub fn left(&self) -> &Arc<dyn ExecutionPlan> {
        &self.left
    }

    /// right side
    pub fn right(&self) -> &Arc<dyn ExecutionPlan> {
        &self.right
    }

    /// Filters applied before join output
    pub fn filter(&self) -> Option<&JoinFilter> {
        self.filter.as_ref()
    }

    /// How the join is performed
    pub fn join_type(&self) -> &JoinType {
        &self.join_type
    }

    /// This function creates the cache object that stores the plan properties such as schema, equivalence properties, ordering, partitioning, etc.
    fn compute_properties(
        left: &Arc<dyn ExecutionPlan>,
        right: &Arc<dyn ExecutionPlan>,
        schema: SchemaRef,
        join_type: JoinType,
    ) -> PlanProperties {
        // Calculate equivalence properties:
        let eq_properties = join_equivalence_properties(
            left.equivalence_properties().clone(),
            right.equivalence_properties().clone(),
            &join_type,
            schema,
            &Self::maintains_input_order(join_type),
            None,
            // No on columns in nested loop join
            &[],
        );

        let output_partitioning =
            asymmetric_join_output_partitioning(left, right, &join_type);

        // Determine execution mode:
        let mode = if left.execution_mode().is_unbounded() {
            ExecutionMode::PipelineBreaking
        } else {
            execution_mode_from_children([left, right])
        };

        PlanProperties::new(eq_properties, output_partitioning, mode)
    }

    /// Returns a vector indicating whether the left and right inputs maintain their order.
    /// The first element corresponds to the left input, and the second to the right.
    ///
    /// The left (build-side) input's order may change, but the right (probe-side) input's
    /// order is maintained for INNER, RIGHT, RIGHT ANTI, and RIGHT SEMI joins.
    ///
    /// Maintaining the right input's order helps optimize the nodes down the pipeline
    /// (See [`ExecutionPlan::maintains_input_order`]).
    ///
    /// This is a separate method because it is also called when computing properties, before
    /// a [`NestedLoopJoinExec`] is created. It also takes [`JoinType`] as an argument, as
    /// opposed to `Self`, for the same reason.
    fn maintains_input_order(join_type: JoinType) -> Vec<bool> {
        vec![
            false,
            matches!(
                join_type,
                JoinType::Inner
                    | JoinType::Right
                    | JoinType::RightAnti
                    | JoinType::RightSemi
            ),
        ]
    }
}

impl DisplayAs for NestedLoopJoinExec {
    fn fmt_as(&self, t: DisplayFormatType, f: &mut Formatter) -> std::fmt::Result {
        match t {
            DisplayFormatType::Default | DisplayFormatType::Verbose => {
                let display_filter = self.filter.as_ref().map_or_else(
                    || "".to_string(),
                    |f| format!(", filter={}", f.expression()),
                );
                write!(
                    f,
                    "NestedLoopJoinExec: join_type={:?}{}",
                    self.join_type, display_filter
                )
            }
        }
    }
}

impl ExecutionPlan for NestedLoopJoinExec {
    fn name(&self) -> &'static str {
        "NestedLoopJoinExec"
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn properties(&self) -> &PlanProperties {
        &self.cache
    }

    fn required_input_distribution(&self) -> Vec<Distribution> {
        vec![
            Distribution::SinglePartition,
            Distribution::UnspecifiedDistribution,
        ]
    }

    fn maintains_input_order(&self) -> Vec<bool> {
        Self::maintains_input_order(self.join_type)
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.left, &self.right]
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        Ok(Arc::new(NestedLoopJoinExec::try_new(
            Arc::clone(&children[0]),
            Arc::clone(&children[1]),
            self.filter.clone(),
            &self.join_type,
        )?))
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        let join_metrics = BuildProbeJoinMetrics::new(partition, &self.metrics);

        // Initialization reservation for load of inner table
        let load_reservation =
            MemoryConsumer::new(format!("NestedLoopJoinLoad[{partition}]"))
                .register(context.memory_pool());

        let inner_table = self.inner_table.once(|| {
            collect_left_input(
                Arc::clone(&self.left),
                Arc::clone(&context),
                join_metrics.clone(),
                load_reservation,
                need_produce_result_in_final(self.join_type),
                self.right().output_partitioning().partition_count(),
            )
        });

        let batch_size = context.session_config().batch_size();
        let enforce_batch_size_in_joins =
            context.session_config().enforce_batch_size_in_joins();

        let outer_table = self.right.execute(partition, context)?;

        let indices_cache = (UInt64Array::new_null(0), UInt32Array::new_null(0));

        // Right side has an order and it is maintained during operation.
        let right_side_ordered =
            self.maintains_input_order()[1] && self.right.output_ordering().is_some();

        if enforce_batch_size_in_joins {
            Ok(Box::pin(NestedLoopJoinStream {
                schema: Arc::clone(&self.schema),
                filter: self.filter.clone(),
                join_type: self.join_type,
                outer_table,
                inner_table,
                column_indices: self.column_indices.clone(),
                join_metrics,
                indices_cache,
                right_side_ordered,
                state: NestedLoopJoinStreamState::WaitBuildSide,
                batch_transformer: BatchSplitter::new(batch_size),
                left_data: None,
            }))
        } else {
            Ok(Box::pin(NestedLoopJoinStream {
                schema: Arc::clone(&self.schema),
                filter: self.filter.clone(),
                join_type: self.join_type,
                outer_table,
                inner_table,
                column_indices: self.column_indices.clone(),
                join_metrics,
                indices_cache,
                right_side_ordered,
                state: NestedLoopJoinStreamState::WaitBuildSide,
                batch_transformer: NoopBatchTransformer::new(),
                left_data: None,
            }))
        }
    }

    fn metrics(&self) -> Option<MetricsSet> {
        Some(self.metrics.clone_inner())
    }

    fn statistics(&self) -> Result<Statistics> {
        estimate_join_statistics(
            Arc::clone(&self.left),
            Arc::clone(&self.right),
            vec![],
            &self.join_type,
            &self.schema,
        )
    }
}

/// Asynchronously collect input into a single batch, and creates `JoinLeftData` from it
async fn collect_left_input(
    input: Arc<dyn ExecutionPlan>,
    context: Arc<TaskContext>,
    join_metrics: BuildProbeJoinMetrics,
    reservation: MemoryReservation,
    with_visited_left_side: bool,
    probe_threads_count: usize,
) -> Result<JoinLeftData> {
    let schema = input.schema();
    let merge = if input.output_partitioning().partition_count() != 1 {
        Arc::new(CoalescePartitionsExec::new(input))
    } else {
        input
    };
    let stream = merge.execute(0, context)?;

    // Load all batches and count the rows
    let (batches, metrics, mut reservation) = stream
        .try_fold(
            (Vec::new(), join_metrics, reservation),
            |mut acc, batch| async {
                let batch_size = batch.get_array_memory_size();
                // Reserve memory for incoming batch
                acc.2.try_grow(batch_size)?;
                // Update metrics
                acc.1.build_mem_used.add(batch_size);
                acc.1.build_input_batches.add(1);
                acc.1.build_input_rows.add(batch.num_rows());
                // Push batch to output
                acc.0.push(batch);
                Ok(acc)
            },
        )
        .await?;

    let merged_batch = concat_batches(&schema, &batches)?;

    // Reserve memory for visited_left_side bitmap if required by join type
    let visited_left_side = if with_visited_left_side {
        // TODO: Replace `ceil` wrapper with stable `div_cell` after
        // https://github.com/rust-lang/rust/issues/88581
        let buffer_size = bit_util::ceil(merged_batch.num_rows(), 8);
        reservation.try_grow(buffer_size)?;
        metrics.build_mem_used.add(buffer_size);

        let mut buffer = BooleanBufferBuilder::new(merged_batch.num_rows());
        buffer.append_n(merged_batch.num_rows(), false);
        buffer
    } else {
        BooleanBufferBuilder::new(0)
    };

    Ok(JoinLeftData::new(
        merged_batch,
        Mutex::new(visited_left_side),
        AtomicUsize::new(probe_threads_count),
        reservation,
    ))
}

/// This enumeration represents various states of the nested loop join algorithm.
#[derive(Debug, Clone)]
enum NestedLoopJoinStreamState {
    /// The initial state, indicating that build-side data not collected yet
    WaitBuildSide,
    /// Indicates that build-side has been collected, and stream is ready for
    /// fetching probe-side
    FetchProbeBatch,
    /// Indicates that a non-empty batch has been fetched from probe-side, and
    /// is ready to be processed
    ProcessProbeBatch(RecordBatch),
    /// Indicates that probe-side has been fully processed
    ExhaustedProbeSide,
    /// Indicates that NestedLoopJoinStream execution is completed
    Completed,
}

impl NestedLoopJoinStreamState {
    /// Tries to extract a `ProcessProbeBatchState` from the
    /// `NestedLoopJoinStreamState` enum. Returns an error if state is not
    /// `ProcessProbeBatchState`.
    fn try_as_process_probe_batch(&mut self) -> Result<&RecordBatch> {
        match self {
            NestedLoopJoinStreamState::ProcessProbeBatch(state) => Ok(state),
            _ => internal_err!("Expected join stream in ProcessProbeBatch state"),
        }
    }
}

/// A stream that issues [RecordBatch]es as they arrive from the right  of the join.
struct NestedLoopJoinStream<T> {
    /// Input schema
    schema: Arc<Schema>,
    /// join filter
    filter: Option<JoinFilter>,
    /// type of the join
    join_type: JoinType,
    /// the outer table data of the nested loop join
    outer_table: SendableRecordBatchStream,
    /// the inner table data of the nested loop join
    inner_table: OnceFut<JoinLeftData>,
    /// Information of index and left / right placement of columns
    column_indices: Vec<ColumnIndex>,
    // TODO: support null aware equal
    // null_equals_null: bool
    /// Join execution metrics
    join_metrics: BuildProbeJoinMetrics,
    /// Cache for join indices calculations
    indices_cache: (UInt64Array, UInt32Array),
    /// Whether the right side is ordered
    right_side_ordered: bool,
    /// Current state of the stream
    state: NestedLoopJoinStreamState,
    /// Transforms the output batch before returning.
    batch_transformer: T,
    /// Result of the left data future
    left_data: Option<Arc<JoinLeftData>>,
}

/// Creates a Cartesian product of two input batches, preserving the order of the right batch,
/// and applying a join filter if provided.
///
/// # Example
/// Input:
/// left = [0, 1], right = [0, 1, 2]
///
/// Output:
/// left_indices = [0, 1, 0, 1, 0, 1], right_indices = [0, 0, 1, 1, 2, 2]
///
/// Input:
/// left = [0, 1, 2], right = [0, 1, 2, 3], filter = left.a != right.a
///
/// Output:
/// left_indices = [1, 2, 0, 2, 0, 1, 0, 1, 2], right_indices = [0, 0, 1, 1, 2, 2, 3, 3, 3]
fn build_join_indices(
    left_batch: &RecordBatch,
    right_batch: &RecordBatch,
    filter: Option<&JoinFilter>,
    indices_cache: &mut (UInt64Array, UInt32Array),
) -> Result<(UInt64Array, UInt32Array)> {
    let left_row_count = left_batch.num_rows();
    let right_row_count = right_batch.num_rows();
    let output_row_count = left_row_count * right_row_count;

    // We always use the same indices before applying the filter, so we can cache them
    let (left_indices_cache, right_indices_cache) = indices_cache;
    let cached_output_row_count = left_indices_cache.len();

    let (left_indices, right_indices) =
        match output_row_count.cmp(&cached_output_row_count) {
            std::cmp::Ordering::Equal => {
                // Reuse the cached indices
                (left_indices_cache.clone(), right_indices_cache.clone())
            }
            std::cmp::Ordering::Less => {
                // Left_row_count never changes because it's the build side. The changes to the
                // right_row_count can be handled trivially by taking the first output_row_count
                // elements of the cache because of how the indices are generated.
                // (See the Ordering::Greater match arm)
                (
                    left_indices_cache.slice(0, output_row_count),
                    right_indices_cache.slice(0, output_row_count),
                )
            }
            std::cmp::Ordering::Greater => {
                // Rebuild the indices cache

                // Produces 0, 1, 2, 0, 1, 2, 0, 1, 2, ...
                *left_indices_cache = UInt64Array::from_iter_values(
                    (0..output_row_count as u64).map(|i| i % left_row_count as u64),
                );

                // Produces 0, 0, 0, 1, 1, 1, 2, 2, 2, ...
                *right_indices_cache = UInt32Array::from_iter_values(
                    (0..output_row_count as u32).map(|i| i / left_row_count as u32),
                );

                (left_indices_cache.clone(), right_indices_cache.clone())
            }
        };

    if let Some(filter) = filter {
        apply_join_filter_to_indices(
            left_batch,
            right_batch,
            left_indices,
            right_indices,
            filter,
            JoinSide::Left,
        )
    } else {
        Ok((left_indices, right_indices))
    }
}

impl<T: BatchTransformer> NestedLoopJoinStream<T> {
    fn poll_next_impl(
        &mut self,
        cx: &mut std::task::Context<'_>,
    ) -> Poll<Option<Result<RecordBatch>>> {
        loop {
            return match self.state {
                NestedLoopJoinStreamState::WaitBuildSide => {
                    handle_state!(ready!(self.collect_build_side(cx)))
                }
                NestedLoopJoinStreamState::FetchProbeBatch => {
                    handle_state!(ready!(self.fetch_probe_batch(cx)))
                }
                NestedLoopJoinStreamState::ProcessProbeBatch(_) => {
                    handle_state!(self.process_probe_batch())
                }
                NestedLoopJoinStreamState::ExhaustedProbeSide => {
                    handle_state!(self.process_unmatched_build_batch())
                }
                NestedLoopJoinStreamState::Completed => Poll::Ready(None),
            };
        }
    }

    fn collect_build_side(
        &mut self,
        cx: &mut std::task::Context<'_>,
    ) -> Poll<Result<StatefulStreamResult<Option<RecordBatch>>>> {
        let build_timer = self.join_metrics.build_time.timer();
        // build hash table from left (build) side, if not yet done
        self.left_data = Some(ready!(self.inner_table.get_shared(cx))?);
        build_timer.done();

        self.state = NestedLoopJoinStreamState::FetchProbeBatch;

        Poll::Ready(Ok(StatefulStreamResult::Continue))
    }

    /// Fetches next batch from probe-side
    ///
    /// If a non-empty batch has been fetched, updates state to
    /// `ProcessProbeBatchState`, otherwise updates state to `ExhaustedProbeSide`.
    fn fetch_probe_batch(
        &mut self,
        cx: &mut std::task::Context<'_>,
    ) -> Poll<Result<StatefulStreamResult<Option<RecordBatch>>>> {
        match ready!(self.outer_table.poll_next_unpin(cx)) {
            None => {
                self.state = NestedLoopJoinStreamState::ExhaustedProbeSide;
            }
            Some(Ok(right_batch)) => {
                self.state = NestedLoopJoinStreamState::ProcessProbeBatch(right_batch);
            }
            Some(Err(err)) => return Poll::Ready(Err(err)),
        };

        Poll::Ready(Ok(StatefulStreamResult::Continue))
    }

    /// Joins current probe batch with build-side data and produces batch with
    /// matched output, updates state to `FetchProbeBatch`.
    fn process_probe_batch(
        &mut self,
    ) -> Result<StatefulStreamResult<Option<RecordBatch>>> {
        let Some(left_data) = self.left_data.clone() else {
            return internal_err!(
                "Expected left_data to be Some in ProcessProbeBatch state"
            );
        };
        let visited_left_side = left_data.bitmap();
        let batch = self.state.try_as_process_probe_batch()?;

        match self.batch_transformer.next() {
            None => {
                // Setting up timer & updating input metrics
                self.join_metrics.input_batches.add(1);
                self.join_metrics.input_rows.add(batch.num_rows());
                let timer = self.join_metrics.join_time.timer();

                let result = join_left_and_right_batch(
                    left_data.batch(),
                    batch,
                    self.join_type,
                    self.filter.as_ref(),
                    &self.column_indices,
                    &self.schema,
                    visited_left_side,
                    &mut self.indices_cache,
                    self.right_side_ordered,
                );
                timer.done();

                self.batch_transformer.set_batch(result?);
                Ok(StatefulStreamResult::Continue)
            }
            Some((batch, last)) => {
                if last {
                    self.state = NestedLoopJoinStreamState::FetchProbeBatch;
                }

                self.join_metrics.output_batches.add(1);
                self.join_metrics.output_rows.add(batch.num_rows());
                Ok(StatefulStreamResult::Ready(Some(batch)))
            }
        }
    }

    /// Processes unmatched build-side rows for certain join types and produces
    /// output batch, updates state to `Completed`.
    fn process_unmatched_build_batch(
        &mut self,
    ) -> Result<StatefulStreamResult<Option<RecordBatch>>> {
        let Some(left_data) = self.left_data.clone() else {
            return internal_err!(
                "Expected left_data to be Some in ExhaustedProbeSide state"
            );
        };
        let visited_left_side = left_data.bitmap();
        if need_produce_result_in_final(self.join_type) {
            // At this stage `visited_left_side` won't be updated, so it's
            // safe to report about probe completion.
            //
            // Setting `is_exhausted` / returning None will prevent from
            // multiple calls of `report_probe_completed()`
            if !left_data.report_probe_completed() {
                self.state = NestedLoopJoinStreamState::Completed;
                return Ok(StatefulStreamResult::Ready(None));
            };

            // Only setting up timer, input is exhausted
            let timer = self.join_metrics.join_time.timer();
            // use the global left bitmap to produce the left indices and right indices
            let (left_side, right_side) =
                get_final_indices_from_shared_bitmap(visited_left_side, self.join_type);
            let empty_right_batch = RecordBatch::new_empty(self.outer_table.schema());
            // use the left and right indices to produce the batch result
            let result = build_batch_from_indices(
                &self.schema,
                left_data.batch(),
                &empty_right_batch,
                &left_side,
                &right_side,
                &self.column_indices,
                JoinSide::Left,
            );
            self.state = NestedLoopJoinStreamState::Completed;

            // Recording time
            if result.is_ok() {
                timer.done();
            }

            Ok(StatefulStreamResult::Ready(Some(result?)))
        } else {
            // end of the join loop
            self.state = NestedLoopJoinStreamState::Completed;
            Ok(StatefulStreamResult::Ready(None))
        }
    }
}

#[allow(clippy::too_many_arguments)]
fn join_left_and_right_batch(
    left_batch: &RecordBatch,
    right_batch: &RecordBatch,
    join_type: JoinType,
    filter: Option<&JoinFilter>,
    column_indices: &[ColumnIndex],
    schema: &Schema,
    visited_left_side: &SharedBitmapBuilder,
    indices_cache: &mut (UInt64Array, UInt32Array),
    right_side_ordered: bool,
) -> Result<RecordBatch> {
    let (left_side, right_side) =
        build_join_indices(left_batch, right_batch, filter, indices_cache).map_err(
            |e| {
                exec_datafusion_err!(
                    "Fail to build join indices in NestedLoopJoinExec, error: {e}"
                )
            },
        )?;

    // set the left bitmap
    // and only full join need the left bitmap
    if need_produce_result_in_final(join_type) {
        let mut bitmap = visited_left_side.lock();
        left_side.values().iter().for_each(|x| {
            bitmap.set_bit(*x as usize, true);
        });
    }
    // adjust the two side indices base on the join type
    let (left_side, right_side) = adjust_indices_by_join_type(
        left_side,
        right_side,
        0..right_batch.num_rows(),
        join_type,
        right_side_ordered,
    )?;

    build_batch_from_indices(
        schema,
        left_batch,
        right_batch,
        &left_side,
        &right_side,
        column_indices,
        JoinSide::Left,
    )
}

fn get_final_indices_from_shared_bitmap(
    shared_bitmap: &SharedBitmapBuilder,
    join_type: JoinType,
) -> (UInt64Array, UInt32Array) {
    let bitmap = shared_bitmap.lock();
    get_final_indices_from_bit_map(&bitmap, join_type)
}

impl<T: BatchTransformer + Unpin + Send> Stream for NestedLoopJoinStream<T> {
    type Item = Result<RecordBatch>;

    fn poll_next(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> Poll<Option<Self::Item>> {
        self.poll_next_impl(cx)
    }
}

impl<T: BatchTransformer + Unpin + Send> RecordBatchStream for NestedLoopJoinStream<T> {
    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }
}

#[cfg(test)]
pub(crate) mod tests {
    use super::*;
    use crate::{
        common, expressions::Column, memory::MemoryExec, repartition::RepartitionExec,
        test::build_table_i32,
    };

    use arrow::datatypes::{DataType, Field};
    use arrow_array::Int32Array;
    use arrow_schema::SortOptions;
    use datafusion_common::{assert_batches_sorted_eq, assert_contains, ScalarValue};
    use datafusion_execution::runtime_env::RuntimeEnvBuilder;
    use datafusion_expr::Operator;
    use datafusion_physical_expr::expressions::{BinaryExpr, Literal};
    use datafusion_physical_expr::{Partitioning, PhysicalExpr};
    use datafusion_physical_expr_common::sort_expr::PhysicalSortExpr;

    use rstest::rstest;

    fn build_table(
        a: (&str, &Vec<i32>),
        b: (&str, &Vec<i32>),
        c: (&str, &Vec<i32>),
        batch_size: Option<usize>,
        sorted_column_names: Vec<&str>,
    ) -> Arc<dyn ExecutionPlan> {
        let batch = build_table_i32(a, b, c);
        let schema = batch.schema();

        let batches = if let Some(batch_size) = batch_size {
            let num_batches = batch.num_rows().div_ceil(batch_size);
            (0..num_batches)
                .map(|i| {
                    let start = i * batch_size;
                    let remaining_rows = batch.num_rows() - start;
                    batch.slice(start, batch_size.min(remaining_rows))
                })
                .collect::<Vec<_>>()
        } else {
            vec![batch]
        };

        let mut exec =
            MemoryExec::try_new(&[batches], Arc::clone(&schema), None).unwrap();
        if !sorted_column_names.is_empty() {
            let mut sort_info = Vec::new();
            for name in sorted_column_names {
                let index = schema.index_of(name).unwrap();
                let sort_expr = PhysicalSortExpr {
                    expr: Arc::new(Column::new(name, index)),
                    options: SortOptions {
                        descending: false,
                        nulls_first: false,
                    },
                };
                sort_info.push(sort_expr);
            }
            exec = exec.try_with_sort_information(vec![sort_info]).unwrap();
        }

        Arc::new(exec)
    }

    fn build_left_table() -> Arc<dyn ExecutionPlan> {
        build_table(
            ("a1", &vec![5, 9, 11]),
            ("b1", &vec![5, 8, 8]),
            ("c1", &vec![50, 90, 110]),
            None,
            Vec::new(),
        )
    }

    fn build_right_table() -> Arc<dyn ExecutionPlan> {
        build_table(
            ("a2", &vec![12, 2, 10]),
            ("b2", &vec![10, 2, 10]),
            ("c2", &vec![40, 80, 100]),
            None,
            Vec::new(),
        )
    }

    fn prepare_join_filter() -> JoinFilter {
        let column_indices = vec![
            ColumnIndex {
                index: 1,
                side: JoinSide::Left,
            },
            ColumnIndex {
                index: 1,
                side: JoinSide::Right,
            },
        ];
        let intermediate_schema = Schema::new(vec![
            Field::new("x", DataType::Int32, true),
            Field::new("x", DataType::Int32, true),
        ]);
        // left.b1!=8
        let left_filter = Arc::new(BinaryExpr::new(
            Arc::new(Column::new("x", 0)),
            Operator::NotEq,
            Arc::new(Literal::new(ScalarValue::Int32(Some(8)))),
        )) as Arc<dyn PhysicalExpr>;
        // right.b2!=10
        let right_filter = Arc::new(BinaryExpr::new(
            Arc::new(Column::new("x", 1)),
            Operator::NotEq,
            Arc::new(Literal::new(ScalarValue::Int32(Some(10)))),
        )) as Arc<dyn PhysicalExpr>;
        // filter = left.b1!=8 and right.b2!=10
        // after filter:
        // left table:
        // ("a1", &vec![5]),
        // ("b1", &vec![5]),
        // ("c1", &vec![50]),
        // right table:
        // ("a2", &vec![12, 2]),
        // ("b2", &vec![10, 2]),
        // ("c2", &vec![40, 80]),
        let filter_expression =
            Arc::new(BinaryExpr::new(left_filter, Operator::And, right_filter))
                as Arc<dyn PhysicalExpr>;

        JoinFilter::new(filter_expression, column_indices, intermediate_schema)
    }

    pub(crate) async fn multi_partitioned_join_collect(
        left: Arc<dyn ExecutionPlan>,
        right: Arc<dyn ExecutionPlan>,
        join_type: &JoinType,
        join_filter: Option<JoinFilter>,
        context: Arc<TaskContext>,
    ) -> Result<(Vec<String>, Vec<RecordBatch>)> {
        let partition_count = 4;

        // Redistributing right input
        let right = Arc::new(RepartitionExec::try_new(
            right,
            Partitioning::RoundRobinBatch(partition_count),
        )?) as Arc<dyn ExecutionPlan>;

        // Use the required distribution for nested loop join to test partition data
        let nested_loop_join =
            NestedLoopJoinExec::try_new(left, right, join_filter, join_type)?;
        let columns = columns(&nested_loop_join.schema());
        let mut batches = vec![];
        for i in 0..partition_count {
            let stream = nested_loop_join.execute(i, Arc::clone(&context))?;
            let more_batches = common::collect(stream).await?;
            batches.extend(
                more_batches
                    .into_iter()
                    .filter(|b| b.num_rows() > 0)
                    .collect::<Vec<_>>(),
            );
        }
        Ok((columns, batches))
    }

    #[tokio::test]
    async fn join_inner_with_filter() -> Result<()> {
        let task_ctx = Arc::new(TaskContext::default());
        let left = build_left_table();
        let right = build_right_table();
        let filter = prepare_join_filter();
        let (columns, batches) = multi_partitioned_join_collect(
            left,
            right,
            &JoinType::Inner,
            Some(filter),
            task_ctx,
        )
        .await?;
        assert_eq!(columns, vec!["a1", "b1", "c1", "a2", "b2", "c2"]);
        let expected = [
            "+----+----+----+----+----+----+",
            "| a1 | b1 | c1 | a2 | b2 | c2 |",
            "+----+----+----+----+----+----+",
            "| 5  | 5  | 50 | 2  | 2  | 80 |",
            "+----+----+----+----+----+----+",
        ];

        assert_batches_sorted_eq!(expected, &batches);

        Ok(())
    }

    #[tokio::test]
    async fn join_left_with_filter() -> Result<()> {
        let task_ctx = Arc::new(TaskContext::default());
        let left = build_left_table();
        let right = build_right_table();

        let filter = prepare_join_filter();
        let (columns, batches) = multi_partitioned_join_collect(
            left,
            right,
            &JoinType::Left,
            Some(filter),
            task_ctx,
        )
        .await?;
        assert_eq!(columns, vec!["a1", "b1", "c1", "a2", "b2", "c2"]);
        let expected = [
            "+----+----+-----+----+----+----+",
            "| a1 | b1 | c1  | a2 | b2 | c2 |",
            "+----+----+-----+----+----+----+",
            "| 11 | 8  | 110 |    |    |    |",
            "| 5  | 5  | 50  | 2  | 2  | 80 |",
            "| 9  | 8  | 90  |    |    |    |",
            "+----+----+-----+----+----+----+",
        ];

        assert_batches_sorted_eq!(expected, &batches);

        Ok(())
    }

    #[tokio::test]
    async fn join_right_with_filter() -> Result<()> {
        let task_ctx = Arc::new(TaskContext::default());
        let left = build_left_table();
        let right = build_right_table();

        let filter = prepare_join_filter();
        let (columns, batches) = multi_partitioned_join_collect(
            left,
            right,
            &JoinType::Right,
            Some(filter),
            task_ctx,
        )
        .await?;
        assert_eq!(columns, vec!["a1", "b1", "c1", "a2", "b2", "c2"]);
        let expected = [
            "+----+----+----+----+----+-----+",
            "| a1 | b1 | c1 | a2 | b2 | c2  |",
            "+----+----+----+----+----+-----+",
            "|    |    |    | 10 | 10 | 100 |",
            "|    |    |    | 12 | 10 | 40  |",
            "| 5  | 5  | 50 | 2  | 2  | 80  |",
            "+----+----+----+----+----+-----+",
        ];

        assert_batches_sorted_eq!(expected, &batches);

        Ok(())
    }

    #[tokio::test]
    async fn join_full_with_filter() -> Result<()> {
        let task_ctx = Arc::new(TaskContext::default());
        let left = build_left_table();
        let right = build_right_table();

        let filter = prepare_join_filter();
        let (columns, batches) = multi_partitioned_join_collect(
            left,
            right,
            &JoinType::Full,
            Some(filter),
            task_ctx,
        )
        .await?;
        assert_eq!(columns, vec!["a1", "b1", "c1", "a2", "b2", "c2"]);
        let expected = [
            "+----+----+-----+----+----+-----+",
            "| a1 | b1 | c1  | a2 | b2 | c2  |",
            "+----+----+-----+----+----+-----+",
            "|    |    |     | 10 | 10 | 100 |",
            "|    |    |     | 12 | 10 | 40  |",
            "| 11 | 8  | 110 |    |    |     |",
            "| 5  | 5  | 50  | 2  | 2  | 80  |",
            "| 9  | 8  | 90  |    |    |     |",
            "+----+----+-----+----+----+-----+",
        ];

        assert_batches_sorted_eq!(expected, &batches);

        Ok(())
    }

    #[tokio::test]
    async fn join_left_semi_with_filter() -> Result<()> {
        let task_ctx = Arc::new(TaskContext::default());
        let left = build_left_table();
        let right = build_right_table();

        let filter = prepare_join_filter();
        let (columns, batches) = multi_partitioned_join_collect(
            left,
            right,
            &JoinType::LeftSemi,
            Some(filter),
            task_ctx,
        )
        .await?;
        assert_eq!(columns, vec!["a1", "b1", "c1"]);
        let expected = [
            "+----+----+----+",
            "| a1 | b1 | c1 |",
            "+----+----+----+",
            "| 5  | 5  | 50 |",
            "+----+----+----+",
        ];

        assert_batches_sorted_eq!(expected, &batches);

        Ok(())
    }

    #[tokio::test]
    async fn join_left_anti_with_filter() -> Result<()> {
        let task_ctx = Arc::new(TaskContext::default());
        let left = build_left_table();
        let right = build_right_table();

        let filter = prepare_join_filter();
        let (columns, batches) = multi_partitioned_join_collect(
            left,
            right,
            &JoinType::LeftAnti,
            Some(filter),
            task_ctx,
        )
        .await?;
        assert_eq!(columns, vec!["a1", "b1", "c1"]);
        let expected = [
            "+----+----+-----+",
            "| a1 | b1 | c1  |",
            "+----+----+-----+",
            "| 11 | 8  | 110 |",
            "| 9  | 8  | 90  |",
            "+----+----+-----+",
        ];

        assert_batches_sorted_eq!(expected, &batches);

        Ok(())
    }

    #[tokio::test]
    async fn join_right_semi_with_filter() -> Result<()> {
        let task_ctx = Arc::new(TaskContext::default());
        let left = build_left_table();
        let right = build_right_table();

        let filter = prepare_join_filter();
        let (columns, batches) = multi_partitioned_join_collect(
            left,
            right,
            &JoinType::RightSemi,
            Some(filter),
            task_ctx,
        )
        .await?;
        assert_eq!(columns, vec!["a2", "b2", "c2"]);
        let expected = [
            "+----+----+----+",
            "| a2 | b2 | c2 |",
            "+----+----+----+",
            "| 2  | 2  | 80 |",
            "+----+----+----+",
        ];

        assert_batches_sorted_eq!(expected, &batches);

        Ok(())
    }

    #[tokio::test]
    async fn join_right_anti_with_filter() -> Result<()> {
        let task_ctx = Arc::new(TaskContext::default());
        let left = build_left_table();
        let right = build_right_table();

        let filter = prepare_join_filter();
        let (columns, batches) = multi_partitioned_join_collect(
            left,
            right,
            &JoinType::RightAnti,
            Some(filter),
            task_ctx,
        )
        .await?;
        assert_eq!(columns, vec!["a2", "b2", "c2"]);
        let expected = [
            "+----+----+-----+",
            "| a2 | b2 | c2  |",
            "+----+----+-----+",
            "| 10 | 10 | 100 |",
            "| 12 | 10 | 40  |",
            "+----+----+-----+",
        ];

        assert_batches_sorted_eq!(expected, &batches);

        Ok(())
    }

    #[tokio::test]
    async fn test_overallocation() -> Result<()> {
        let left = build_table(
            ("a1", &vec![1, 2, 3, 4, 5, 6, 7, 8, 9, 0]),
            ("b1", &vec![1, 2, 3, 4, 5, 6, 7, 8, 9, 0]),
            ("c1", &vec![1, 2, 3, 4, 5, 6, 7, 8, 9, 0]),
            None,
            Vec::new(),
        );
        let right = build_table(
            ("a2", &vec![10, 11]),
            ("b2", &vec![12, 13]),
            ("c2", &vec![14, 15]),
            None,
            Vec::new(),
        );
        let filter = prepare_join_filter();

        let join_types = vec![
            JoinType::Inner,
            JoinType::Left,
            JoinType::Right,
            JoinType::Full,
            JoinType::LeftSemi,
            JoinType::LeftAnti,
            JoinType::RightSemi,
            JoinType::RightAnti,
        ];

        for join_type in join_types {
            let runtime = RuntimeEnvBuilder::new()
                .with_memory_limit(100, 1.0)
                .build_arc()?;
            let task_ctx = TaskContext::default().with_runtime(runtime);
            let task_ctx = Arc::new(task_ctx);

            let err = multi_partitioned_join_collect(
                Arc::clone(&left),
                Arc::clone(&right),
                &join_type,
                Some(filter.clone()),
                task_ctx,
            )
            .await
            .unwrap_err();

            assert_contains!(
                err.to_string(),
                "External error: Resources exhausted: Additional allocation failed with top memory consumers (across reservations) as: NestedLoopJoinLoad[0]"
            );
        }

        Ok(())
    }

    fn prepare_mod_join_filter() -> JoinFilter {
        let column_indices = vec![
            ColumnIndex {
                index: 1,
                side: JoinSide::Left,
            },
            ColumnIndex {
                index: 1,
                side: JoinSide::Right,
            },
        ];
        let intermediate_schema = Schema::new(vec![
            Field::new("x", DataType::Int32, true),
            Field::new("x", DataType::Int32, true),
        ]);

        // left.b1 % 3
        let left_mod = Arc::new(BinaryExpr::new(
            Arc::new(Column::new("x", 0)),
            Operator::Modulo,
            Arc::new(Literal::new(ScalarValue::Int32(Some(3)))),
        )) as Arc<dyn PhysicalExpr>;
        // left.b1 % 3 != 0
        let left_filter = Arc::new(BinaryExpr::new(
            left_mod,
            Operator::NotEq,
            Arc::new(Literal::new(ScalarValue::Int32(Some(0)))),
        )) as Arc<dyn PhysicalExpr>;

        // right.b2 % 5
        let right_mod = Arc::new(BinaryExpr::new(
            Arc::new(Column::new("x", 1)),
            Operator::Modulo,
            Arc::new(Literal::new(ScalarValue::Int32(Some(5)))),
        )) as Arc<dyn PhysicalExpr>;
        // right.b2 % 5 != 0
        let right_filter = Arc::new(BinaryExpr::new(
            right_mod,
            Operator::NotEq,
            Arc::new(Literal::new(ScalarValue::Int32(Some(0)))),
        )) as Arc<dyn PhysicalExpr>;
        // filter = left.b1 % 3 != 0 and right.b2 % 5 != 0
        let filter_expression =
            Arc::new(BinaryExpr::new(left_filter, Operator::And, right_filter))
                as Arc<dyn PhysicalExpr>;

        JoinFilter::new(filter_expression, column_indices, intermediate_schema)
    }

    fn generate_columns(num_columns: usize, num_rows: usize) -> Vec<Vec<i32>> {
        let column = (1..=num_rows).map(|x| x as i32).collect();
        vec![column; num_columns]
    }

    #[rstest]
    #[tokio::test]
    async fn join_maintains_right_order(
        #[values(
            JoinType::Inner,
            JoinType::Right,
            JoinType::RightAnti,
            JoinType::RightSemi
        )]
        join_type: JoinType,
        #[values(1, 100, 1000)] left_batch_size: usize,
        #[values(1, 100, 1000)] right_batch_size: usize,
    ) -> Result<()> {
        let left_columns = generate_columns(3, 1000);
        let left = build_table(
            ("a1", &left_columns[0]),
            ("b1", &left_columns[1]),
            ("c1", &left_columns[2]),
            Some(left_batch_size),
            Vec::new(),
        );

        let right_columns = generate_columns(3, 1000);
        let right = build_table(
            ("a2", &right_columns[0]),
            ("b2", &right_columns[1]),
            ("c2", &right_columns[2]),
            Some(right_batch_size),
            vec!["a2", "b2", "c2"],
        );

        let filter = prepare_mod_join_filter();

        let nested_loop_join = Arc::new(NestedLoopJoinExec::try_new(
            left,
            Arc::clone(&right),
            Some(filter),
            &join_type,
        )?) as Arc<dyn ExecutionPlan>;
        assert_eq!(nested_loop_join.maintains_input_order(), vec![false, true]);

        let right_column_indices = match join_type {
            JoinType::Inner | JoinType::Right => vec![3, 4, 5],
            JoinType::RightAnti | JoinType::RightSemi => vec![0, 1, 2],
            _ => unreachable!(),
        };

        let right_ordering = right.output_ordering().unwrap();
        let join_ordering = nested_loop_join.output_ordering().unwrap();
        for (right, join) in right_ordering.iter().zip(join_ordering.iter()) {
            let right_column = right.expr.as_any().downcast_ref::<Column>().unwrap();
            let join_column = join.expr.as_any().downcast_ref::<Column>().unwrap();
            assert_eq!(join_column.name(), join_column.name());
            assert_eq!(
                right_column_indices[right_column.index()],
                join_column.index()
            );
            assert_eq!(right.options, join.options);
        }

        let batches = nested_loop_join
            .execute(0, Arc::new(TaskContext::default()))?
            .try_collect::<Vec<_>>()
            .await?;

        // Make sure that the order of the right side is maintained
        let mut prev_values = [i32::MIN, i32::MIN, i32::MIN];

        for (batch_index, batch) in batches.iter().enumerate() {
            let columns: Vec<_> = right_column_indices
                .iter()
                .map(|&i| {
                    batch
                        .column(i)
                        .as_any()
                        .downcast_ref::<Int32Array>()
                        .unwrap()
                })
                .collect();

            for row in 0..batch.num_rows() {
                let current_values = [
                    columns[0].value(row),
                    columns[1].value(row),
                    columns[2].value(row),
                ];
                assert!(
                    current_values
                        .into_iter()
                        .zip(prev_values)
                        .all(|(current, prev)| current >= prev),
                    "batch_index: {} row: {} current: {:?}, prev: {:?}",
                    batch_index,
                    row,
                    current_values,
                    prev_values
                );
                prev_values = current_values;
            }
        }

        Ok(())
    }

    /// Returns the column names on the schema
    fn columns(schema: &Schema) -> Vec<String> {
        schema.fields().iter().map(|f| f.name().clone()).collect()
    }
}
