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

//! Stream and channel implementations for window function expressions.

use std::collections::VecDeque;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use super::tile::{CompletedWindowTile, WindowTileCoalescer, WindowTileMode};
use super::utils::create_schema;
use crate::execution_plan::{CardinalityEffect, EmissionType};
use crate::metrics::{BaselineMetrics, ExecutionPlanMetricsSet, MetricsSet};
use crate::stream::EmptyRecordBatchStream;
use crate::windows::{
    calc_requirements, get_ordered_partition_by_indices, get_partition_by_sort_exprs,
    window_equivalence_properties,
};
use crate::{
    ColumnStatistics, DisplayAs, DisplayFormatType, Distribution, ExecutionPlan,
    ExecutionPlanProperties, PhysicalExpr, PlanProperties, RecordBatchStream,
    SendableRecordBatchStream, Statistics, WindowExpr, check_if_same_properties,
};

use arrow::array::ArrayRef;
use arrow::compute::{concat, concat_batches};
use arrow::datatypes::SchemaRef;
use arrow::error::ArrowError;
use arrow::record_batch::RecordBatch;
use datafusion_common::stats::Precision;
use datafusion_common::utils::{evaluate_partition_ranges, transpose};
use datafusion_common::{
    Result, ScalarValue, assert_eq_or_internal_err, exec_datafusion_err, internal_err,
};
use datafusion_common_runtime::SpawnedTask;
use datafusion_execution::TaskContext;
use datafusion_expr::{WindowFrame, WindowFrameBound, WindowFrameUnits};
use datafusion_physical_expr::window::ParallelSlidingAggregateWindowExpr;
use datafusion_physical_expr_common::sort_expr::{
    OrderingRequirements, PhysicalSortExpr,
};

use futures::{Stream, StreamExt, ready};

type TileWorkerGroup = Pin<Box<dyn Future<Output = Result<Vec<RecordBatch>>> + Send>>;

/// Window execution plan
#[derive(Debug, Clone)]
pub struct WindowAggExec {
    /// Input plan
    pub(crate) input: Arc<dyn ExecutionPlan>,
    /// Window function expression
    window_expr: Vec<Arc<dyn WindowExpr>>,
    /// Schema after the window is run
    schema: SchemaRef,
    /// Execution metrics
    metrics: ExecutionPlanMetricsSet,
    /// Partition by indices that defines preset for existing ordering
    // see `get_ordered_partition_by_indices` for more details.
    ordered_partition_by_indices: Vec<usize>,
    /// Cache holding plan properties like equivalences, output partitioning etc.
    cache: Arc<PlanProperties>,
    /// If `can_partition` is false, partition_keys is always empty.
    can_repartition: bool,
}

impl WindowAggExec {
    /// Create a new execution plan for window aggregates
    pub fn try_new(
        window_expr: Vec<Arc<dyn WindowExpr>>,
        input: Arc<dyn ExecutionPlan>,
        can_repartition: bool,
    ) -> Result<Self> {
        let schema = create_schema(&input.schema(), &window_expr)?;
        let schema = Arc::new(schema);

        let ordered_partition_by_indices =
            get_ordered_partition_by_indices(window_expr[0].partition_by(), &input)?;
        let cache = Self::compute_properties(&schema, &input, &window_expr)?;
        Ok(Self {
            input,
            window_expr,
            schema,
            metrics: ExecutionPlanMetricsSet::new(),
            ordered_partition_by_indices,
            cache: Arc::new(cache),
            can_repartition,
        })
    }

    /// Window expressions
    pub fn window_expr(&self) -> &[Arc<dyn WindowExpr>] {
        &self.window_expr
    }

    /// Input plan
    pub fn input(&self) -> &Arc<dyn ExecutionPlan> {
        &self.input
    }

    /// Return the output sort order of partition keys: For example
    /// OVER(PARTITION BY a, ORDER BY b) -> would give sorting of the column a
    // We are sure that partition by columns are always at the beginning of sort_keys
    // Hence returned `PhysicalSortExpr` corresponding to `PARTITION BY` columns can be used safely
    // to calculate partition separation points
    pub fn partition_by_sort_keys(&self) -> Result<Vec<PhysicalSortExpr>> {
        let partition_by = self.window_expr()[0].partition_by();
        get_partition_by_sort_exprs(
            &self.input,
            partition_by,
            &self.ordered_partition_by_indices,
        )
    }

    /// This function creates the cache object that stores the plan properties such as schema, equivalence properties, ordering, partitioning, etc.
    fn compute_properties(
        schema: &SchemaRef,
        input: &Arc<dyn ExecutionPlan>,
        window_exprs: &[Arc<dyn WindowExpr>],
    ) -> Result<PlanProperties> {
        // Calculate equivalence properties:
        let eq_properties = window_equivalence_properties(schema, input, window_exprs)?;

        // Get output partitioning:
        // Because we can have repartitioning using the partition keys this
        // would be either 1 or more than 1 depending on the presence of repartitioning.
        let output_partitioning = input.output_partitioning().clone();

        // Construct properties cache:
        Ok(PlanProperties::new(
            eq_properties,
            output_partitioning,
            // TODO: Emission type and boundedness information can be enhanced here
            EmissionType::Final,
            input.boundedness(),
        ))
    }

    pub fn partition_keys(&self) -> Vec<Arc<dyn PhysicalExpr>> {
        if !self.can_repartition {
            vec![]
        } else {
            let all_partition_keys = self
                .window_expr()
                .iter()
                .map(|expr| expr.partition_by().to_vec())
                .collect::<Vec<_>>();

            all_partition_keys
                .into_iter()
                .min_by_key(|s| s.len())
                .unwrap_or_else(Vec::new)
        }
    }

    fn with_new_children_and_same_properties(
        &self,
        mut children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Self {
        Self {
            input: children.swap_remove(0),
            metrics: ExecutionPlanMetricsSet::new(),
            ..Self::clone(self)
        }
    }
}

impl DisplayAs for WindowAggExec {
    fn fmt_as(
        &self,
        t: DisplayFormatType,
        f: &mut std::fmt::Formatter,
    ) -> std::fmt::Result {
        match t {
            DisplayFormatType::Default | DisplayFormatType::Verbose => {
                write!(f, "WindowAggExec: ")?;
                let g: Vec<String> = self
                    .window_expr
                    .iter()
                    .map(|e| {
                        format!(
                            "{}: {:?}, frame: {:?}",
                            e.name().to_owned(),
                            e.field(),
                            e.get_window_frame()
                        )
                    })
                    .collect();
                write!(f, "wdw=[{}]", g.join(", "))?;
                if let Some(stream_name) = self.stream_name_for_display() {
                    write!(f, ", stream=[{stream_name}]")?;
                }
            }
            DisplayFormatType::TreeRender => {
                let g: Vec<String> = self
                    .window_expr
                    .iter()
                    .map(|e| e.name().to_owned().to_string())
                    .collect();
                writeln!(f, "select_list={}", g.join(", "))?;
                if let Some(stream_name) = self.stream_name_for_display() {
                    writeln!(f, "stream={stream_name}")?;
                }
            }
        }
        Ok(())
    }
}

impl ExecutionPlan for WindowAggExec {
    fn name(&self) -> &'static str {
        "WindowAggExec"
    }

    /// Return a reference to Any that can be used for downcasting
    fn properties(&self) -> &Arc<PlanProperties> {
        &self.cache
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.input]
    }

    fn maintains_input_order(&self) -> Vec<bool> {
        vec![true]
    }

    fn required_input_ordering(&self) -> Vec<Option<OrderingRequirements>> {
        let partition_bys = self.window_expr()[0].partition_by();
        let order_keys = self.window_expr()[0].order_by();
        if self.ordered_partition_by_indices.len() < partition_bys.len() {
            vec![calc_requirements(partition_bys, order_keys)]
        } else {
            let partition_bys = self
                .ordered_partition_by_indices
                .iter()
                .map(|idx| &partition_bys[*idx]);
            vec![calc_requirements(partition_bys, order_keys)]
        }
    }

    fn required_input_distribution(&self) -> Vec<Distribution> {
        if self.partition_keys().is_empty() {
            vec![Distribution::SinglePartition]
        } else {
            vec![Distribution::HashPartitioned(self.partition_keys())]
        }
    }

    fn with_new_children(
        self: Arc<Self>,
        mut children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        check_if_same_properties!(self, children);
        Ok(Arc::new(WindowAggExec::try_new(
            self.window_expr.clone(),
            children.swap_remove(0),
            true,
        )?))
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        let parallel_config = ParallelWindowStreamConfig::new(
            context.session_config().target_partitions(),
            context.session_config().batch_size(),
        );
        let input = self.input.execute(partition, context)?;
        let baseline_metrics = BaselineMetrics::new(&self.metrics, partition);
        let partition_by_sort_keys = self.partition_by_sort_keys()?;
        let stream: SendableRecordBatchStream =
            match choose_window_stream(&self.window_expr)? {
                WindowStreamChoice::Parallel => Box::pin(ParallelWindowStream::new(
                    Arc::clone(&self.schema),
                    self.window_expr.clone(),
                    input,
                    baseline_metrics,
                    partition_by_sort_keys,
                    &self.ordered_partition_by_indices,
                    parallel_config,
                )?),
                WindowStreamChoice::Default => Box::pin(WindowAggStream::new(
                    Arc::clone(&self.schema),
                    self.window_expr.clone(),
                    input,
                    baseline_metrics,
                    partition_by_sort_keys,
                    self.ordered_partition_by_indices.clone(),
                )?),
            };
        Ok(stream)
    }

    fn metrics(&self) -> Option<MetricsSet> {
        Some(self.metrics.clone_inner())
    }

    fn partition_statistics(&self, partition: Option<usize>) -> Result<Arc<Statistics>> {
        let input_stat =
            Arc::unwrap_or_clone(self.input.partition_statistics(partition)?);
        let win_cols = self.window_expr.len();
        let input_cols = self.input.schema().fields().len();
        // TODO stats: some windowing function will maintain invariants such as min, max...
        let mut column_statistics = Vec::with_capacity(win_cols + input_cols);
        // copy stats of the input to the beginning of the schema.
        column_statistics.extend(input_stat.column_statistics);
        for _ in 0..win_cols {
            column_statistics.push(ColumnStatistics::new_unknown())
        }
        Ok(Arc::new(Statistics {
            num_rows: input_stat.num_rows,
            column_statistics,
            total_byte_size: Precision::Absent,
        }))
    }

    fn cardinality_effect(&self) -> CardinalityEffect {
        CardinalityEffect::Equal
    }
}

impl WindowAggExec {
    fn stream_name_for_display(&self) -> Option<&'static str> {
        match choose_window_stream(&self.window_expr) {
            Ok(WindowStreamChoice::Parallel) => Some("ParallelWindowStream"),
            Ok(WindowStreamChoice::Default) | Err(_) => None,
        }
    }
}

/// Compute the window aggregate columns
fn compute_window_aggregates(
    window_expr: &[Arc<dyn WindowExpr>],
    batch: &RecordBatch,
) -> Result<Vec<ArrayRef>> {
    window_expr
        .iter()
        .map(|window_expr| window_expr.evaluate(batch))
        .collect()
}

enum WindowStreamChoice {
    Parallel,
    Default,
}

fn choose_window_stream(
    window_expr: &[Arc<dyn WindowExpr>],
) -> Result<WindowStreamChoice> {
    Ok(if can_use_parallel_window_stream(window_expr)? {
        WindowStreamChoice::Parallel
    } else {
        WindowStreamChoice::Default
    })
}

fn can_use_parallel_window_stream(window_expr: &[Arc<dyn WindowExpr>]) -> Result<bool> {
    if window_expr.is_empty() {
        return Ok(false);
    }

    window_expr
        .iter()
        .map(|window_expr| can_evaluate_in_parallel(window_expr.as_ref()))
        .try_fold(true, |acc, can_evaluate| Ok(acc && can_evaluate?))
}

fn can_evaluate_in_parallel(window_expr: &dyn WindowExpr) -> Result<bool> {
    if fixed_rows_window_halo(window_expr.get_window_frame()).is_none() {
        return Ok(false);
    }

    let Some(window_expr) = window_expr
        .as_any()
        .downcast_ref::<ParallelSlidingAggregateWindowExpr>()
    else {
        return Ok(false);
    };

    let accumulator = window_expr
        .get_aggregate_expr()
        .create_sliding_accumulator()?;
    Ok(accumulator.supports_window_batch())
}

fn fixed_rows_window_halo(window_frame: &WindowFrame) -> Option<(usize, usize)> {
    if window_frame.units != WindowFrameUnits::Rows {
        return None;
    }

    let start = fixed_rows_bound_halo(&window_frame.start_bound)?;
    let end = fixed_rows_bound_halo(&window_frame.end_bound)?;
    Some((start.0.max(end.0), start.1.max(end.1)))
}

fn fixed_rows_bound_halo(bound: &WindowFrameBound) -> Option<(usize, usize)> {
    match bound {
        WindowFrameBound::Preceding(ScalarValue::UInt64(Some(offset))) => {
            Some((usize::try_from(*offset).ok()?, 0))
        }
        WindowFrameBound::CurrentRow => Some((0, 0)),
        WindowFrameBound::Following(ScalarValue::UInt64(Some(offset))) => {
            Some((0, usize::try_from(*offset).ok()?))
        }
        _ => None,
    }
}

/// Controls the coarse-grained parallelism inside [`ParallelWindowStream`].
#[derive(Debug, Clone, Copy)]
pub struct ParallelWindowStreamConfig {
    worker_count: usize,
    target_tile_len: usize,
}

impl ParallelWindowStreamConfig {
    pub fn new(worker_count: usize, target_tile_len: usize) -> Self {
        Self {
            worker_count: worker_count.max(1),
            target_tile_len: target_tile_len.max(1),
        }
    }
}

/// Stream for window aggregation plans that can evaluate fixed-size ROWS tiles
/// independently.
///
/// The scheduling model is:
///
/// 1. Read input until the tiler has up to `worker_count` completed tiles.
/// 2. Start one worker per tile.
/// 3. Wait for all workers in that group to finish.
/// 4. Emit their output batches in tile order.
///
/// For example, with two workers, this stream reads enough input for tiles
/// `T0` and `T1`, evaluates both concurrently, emits `T0` then `T1`, and only
/// then starts reading for `T2` and `T3`. This preserves output order with a
/// small amount of coordination state. A later scheduler can make this more
/// responsive by emitting the ready head tile and immediately refilling one
/// worker slot.
pub struct ParallelWindowStream {
    schema: SchemaRef,
    input: SendableRecordBatchStream,
    /// Converts input batches into halo-padded output tiles.
    tiler: WindowTileCoalescer,
    /// Completed tiles collected for the next worker group. This is retained
    /// across `Poll::Pending` while the stream is still reading enough input to
    /// fill the group.
    cached_tiles: Vec<CompletedWindowTile>,
    /// Finished worker results that are ready to be returned to the caller.
    pending_output_batches: VecDeque<RecordBatch>,
    /// Active worker group. It completes only when every tile in the group has
    /// produced its output batch.
    worker_group: Option<TileWorkerGroup>,
    input_finished: bool,
    finished: bool,
    window_expr: Vec<Arc<dyn WindowExpr>>,
    baseline_metrics: BaselineMetrics,
    config: ParallelWindowStreamConfig,
}

impl ParallelWindowStream {
    /// Create a new ParallelWindowStream.
    pub fn new(
        schema: SchemaRef,
        window_expr: Vec<Arc<dyn WindowExpr>>,
        input: SendableRecordBatchStream,
        baseline_metrics: BaselineMetrics,
        partition_by_sort_keys: Vec<PhysicalSortExpr>,
        ordered_partition_by_indices: &[usize],
        config: ParallelWindowStreamConfig,
    ) -> Result<Self> {
        // In WindowAggExec all partition by columns should be ordered.
        assert_eq_or_internal_err!(
            window_expr[0].partition_by().len(),
            ordered_partition_by_indices.len(),
            "All partition by columns should have an ordering"
        );

        // Each tile must include enough halo rows for every expression it
        // evaluates. The widest frame wins, so a single tiler can serve the
        // whole expression list.
        let (preceding_halo_len, following_halo_len) =
            max_rows_window_halo(&window_expr)?;
        let tiler = WindowTileCoalescer::new_with_mode(
            partition_by_sort_keys,
            WindowTileMode::FixedSize {
                target_tile_len: config.target_tile_len,
                preceding_halo_len,
                following_halo_len,
            },
        );

        Ok(Self {
            schema,
            input,
            tiler,
            cached_tiles: Vec::with_capacity(config.worker_count),
            pending_output_batches: VecDeque::new(),
            worker_group: None,
            input_finished: false,
            finished: false,
            window_expr,
            baseline_metrics,
            config,
        })
    }
}

impl Stream for ParallelWindowStream {
    type Item = Result<RecordBatch>;

    fn poll_next(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Self::Item>> {
        let poll = self.poll_next_inner(cx);
        self.baseline_metrics.record_poll(poll)
    }
}

impl ParallelWindowStream {
    #[inline]
    fn poll_next_inner(
        &mut self,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Result<RecordBatch>>> {
        if self.finished {
            return Poll::Ready(None);
        }

        loop {
            if let Some(batch) = self.pending_output_batches.pop_front() {
                debug_assert!(batch.num_rows() > 0);
                return Poll::Ready(Some(Ok(batch)));
            }

            if let Some(worker_group) = self.worker_group.as_mut() {
                let result = ready!(worker_group.as_mut().poll(cx));
                self.worker_group = None;
                match result {
                    Ok(batches) => {
                        // Barrier between worker groups: only expose results
                        // after every tile in the current group has finished.
                        self.pending_output_batches.extend(
                            batches.into_iter().filter(|batch| batch.num_rows() > 0),
                        );
                        continue;
                    }
                    Err(e) => return Poll::Ready(Some(Err(e))),
                };
            }

            // Buffer window tiles for workers to handle concurrently
            let tiles = ready!(self.poll_next_tile_batch(cx))?;
            if tiles.is_empty() {
                self.finished = true;
                return Poll::Ready(None);
            }

            // Spawn workers for intra-operator parallelism
            self.worker_group = Some(spawn_parallel_tile_workers(
                &self.schema,
                &self.window_expr,
                tiles,
            ));
        }
    }

    fn poll_next_tile_batch(
        &mut self,
        cx: &mut Context<'_>,
    ) -> Poll<Result<Vec<CompletedWindowTile>>> {
        loop {
            // Drain already-completed tiles before reading more input. If this
            // poll later returns `Pending`, `cached_tiles` keeps the partial
            // group so the next poll resumes from the same point.
            while self.cached_tiles.len() < self.config.worker_count {
                let Some(tile) = self.tiler.next_completed_tile() else {
                    break;
                };
                self.cached_tiles.push(tile);
            }

            // Start a worker group when it is full, or flush the final partial
            // group once the input has ended and the tiler has been finished.
            if self.cached_tiles.len() == self.config.worker_count || self.input_finished
            {
                return Poll::Ready(Ok(std::mem::take(&mut self.cached_tiles)));
            }

            match ready!(self.input.poll_next_unpin(cx)) {
                Some(Ok(batch)) => {
                    self.tiler.push_batch(batch)?;
                }
                Some(Err(e)) => return Poll::Ready(Err(e)),
                None => {
                    self.input_finished = true;
                    self.tiler.finish()?;
                }
            }
        }
    }
}

impl RecordBatchStream for ParallelWindowStream {
    /// Get the schema
    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }
}

fn max_rows_window_halo(window_expr: &[Arc<dyn WindowExpr>]) -> Result<(usize, usize)> {
    window_expr
        .iter()
        .map(|window_expr| {
            fixed_rows_window_halo(window_expr.get_window_frame()).ok_or_else(|| {
                exec_datafusion_err!(
                    "ParallelWindowStream requires fixed ROWS window frames"
                )
            })
        })
        .try_fold((0, 0), |(max_preceding, max_following), halo| {
            let (preceding, following) = halo?;
            Ok((max_preceding.max(preceding), max_following.max(following)))
        })
}

fn spawn_parallel_tile_workers(
    schema: &SchemaRef,
    window_expr: &[Arc<dyn WindowExpr>],
    tiles: Vec<CompletedWindowTile>,
) -> TileWorkerGroup {
    let mut tasks = Vec::with_capacity(tiles.len());
    let window_expr: Arc<[Arc<dyn WindowExpr>]> = Arc::from(window_expr.to_vec());
    for tile in tiles {
        let schema = Arc::clone(schema);
        let window_expr = Arc::clone(&window_expr);
        tasks.push(SpawnedTask::spawn(async move {
            build_parallel_window_tile(schema, window_expr.as_ref(), &tile)
        }));
    }

    Box::pin(async move {
        let mut batches = Vec::with_capacity(tasks.len());
        for task in tasks {
            batches.push(task.await.map_err(|e| {
                exec_datafusion_err!("parallel window worker failed: {e}")
            })??);
        }
        Ok(batches)
    })
}

fn build_parallel_window_tile(
    schema: SchemaRef,
    window_expr: &[Arc<dyn WindowExpr>],
    tile: &CompletedWindowTile,
) -> Result<RecordBatch> {
    let output_len = tile.output_end - tile.output_start;
    let mut batch_columns = tile
        .batch
        .columns()
        .iter()
        .map(|column| column.slice(tile.output_start, output_len))
        .collect::<Vec<_>>();

    for window_expr in window_expr {
        let Some(window_expr) = window_expr
            .as_any()
            .downcast_ref::<ParallelSlidingAggregateWindowExpr>()
        else {
            return internal_err!(
                "ParallelWindowStream requires ParallelSlidingAggregateWindowExpr"
            );
        };
        batch_columns.push(window_expr.evaluate_window_batch(
            &tile.batch,
            tile.output_start,
            tile.output_end,
        )?);
    }

    Ok(RecordBatch::try_new(schema, batch_columns)?)
}

/// stream for window aggregation plan
pub struct WindowAggStream {
    schema: SchemaRef,
    input: SendableRecordBatchStream,
    batches: Vec<RecordBatch>,
    finished: bool,
    window_expr: Vec<Arc<dyn WindowExpr>>,
    partition_by_sort_keys: Vec<PhysicalSortExpr>,
    baseline_metrics: BaselineMetrics,
    ordered_partition_by_indices: Vec<usize>,
}

impl WindowAggStream {
    /// Create a new WindowAggStream
    pub fn new(
        schema: SchemaRef,
        window_expr: Vec<Arc<dyn WindowExpr>>,
        input: SendableRecordBatchStream,
        baseline_metrics: BaselineMetrics,
        partition_by_sort_keys: Vec<PhysicalSortExpr>,
        ordered_partition_by_indices: Vec<usize>,
    ) -> Result<Self> {
        // In WindowAggExec all partition by columns should be ordered.
        assert_eq_or_internal_err!(
            window_expr[0].partition_by().len(),
            ordered_partition_by_indices.len(),
            "All partition by columns should have an ordering"
        );
        Ok(Self {
            schema,
            input,
            batches: vec![],
            finished: false,
            window_expr,
            baseline_metrics,
            partition_by_sort_keys,
            ordered_partition_by_indices,
        })
    }

    fn compute_aggregates(&self) -> Result<Option<RecordBatch>> {
        // record compute time on drop
        let _timer = self.baseline_metrics.elapsed_compute().timer();

        let batch = concat_batches(&self.input.schema(), &self.batches)?;
        if batch.num_rows() == 0 {
            return Ok(None);
        }

        let partition_by_sort_keys = self
            .ordered_partition_by_indices
            .iter()
            .map(|idx| self.partition_by_sort_keys[*idx].evaluate_to_sort_column(&batch))
            .collect::<Result<Vec<_>>>()?;
        let partition_points =
            evaluate_partition_ranges(batch.num_rows(), &partition_by_sort_keys)?;

        let mut partition_results = vec![];
        // Calculate window cols
        for partition_point in partition_points {
            let length = partition_point.end - partition_point.start;
            partition_results.push(compute_window_aggregates(
                &self.window_expr,
                &batch.slice(partition_point.start, length),
            )?)
        }
        let columns = transpose(partition_results)
            .iter()
            .map(|elems| concat(&elems.iter().map(|x| x.as_ref()).collect::<Vec<_>>()))
            .collect::<Vec<_>>()
            .into_iter()
            .collect::<Result<Vec<ArrayRef>, ArrowError>>()?;

        // combine with the original cols
        // note the setup of window aggregates is that they newly calculated window
        // expression results are always appended to the columns
        let mut batch_columns = batch.columns().to_vec();
        // calculate window cols
        batch_columns.extend_from_slice(&columns);
        Ok(Some(RecordBatch::try_new(
            Arc::clone(&self.schema),
            batch_columns,
        )?))
    }
}

impl Stream for WindowAggStream {
    type Item = Result<RecordBatch>;

    fn poll_next(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Self::Item>> {
        let poll = self.poll_next_inner(cx);
        self.baseline_metrics.record_poll(poll)
    }
}

impl WindowAggStream {
    #[inline]
    fn poll_next_inner(
        &mut self,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Result<RecordBatch>>> {
        if self.finished {
            return Poll::Ready(None);
        }

        loop {
            return Poll::Ready(Some(match ready!(self.input.poll_next_unpin(cx)) {
                Some(Ok(batch)) => {
                    self.batches.push(batch);
                    continue;
                }
                Some(Err(e)) => Err(e),
                None => {
                    // Release the input pipeline's resources before computing
                    // the final aggregates.
                    let input_schema = self.input.schema();
                    self.input = Box::pin(EmptyRecordBatchStream::new(input_schema));
                    let Some(result) = self.compute_aggregates()? else {
                        return Poll::Ready(None);
                    };
                    self.finished = true;
                    // Empty record batches should not be emitted.
                    // They need to be treated as  [`Option<RecordBatch>`]es and handled separately
                    debug_assert!(result.num_rows() > 0);
                    Ok(result)
                }
            }));
        }
    }
}

impl RecordBatchStream for WindowAggStream {
    /// Get the schema
    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::displayable;
    use crate::stream::RecordBatchStreamAdapter;
    use crate::test::TestMemoryExec;
    use crate::windows::create_window_expr;
    use arrow::array::Float64Array;
    use arrow::compute::concat_batches;
    use arrow::datatypes::{DataType, Field, Schema};
    use datafusion_common::ScalarValue;
    use datafusion_common::cast::as_float64_array;
    use datafusion_expr::{
        WindowFrame, WindowFrameBound, WindowFrameUnits, WindowFunctionDefinition,
    };
    use datafusion_functions_aggregate::average::avg_udaf;
    use datafusion_functions_aggregate::count::count_udaf;
    use datafusion_physical_expr::aggregate::AggregateExprBuilder;
    use futures::stream;

    #[test]
    fn test_window_agg_cardinality_effect() -> Result<()> {
        let schema = Arc::new(Schema::new(vec![Field::new("a", DataType::Int64, true)]));
        let input: Arc<dyn ExecutionPlan> =
            Arc::new(TestMemoryExec::try_new(&[], Arc::clone(&schema), None)?);
        let args = vec![crate::expressions::col("a", &schema)?];
        let window_expr = create_window_expr(
            &WindowFunctionDefinition::AggregateUDF(count_udaf()),
            "count(a)".to_string(),
            &args,
            &[],
            &[],
            Arc::new(WindowFrame::new_bounds(
                WindowFrameUnits::Rows,
                WindowFrameBound::Preceding(ScalarValue::UInt64(None)),
                WindowFrameBound::CurrentRow,
            )),
            Arc::clone(&schema),
            false,
            false,
            None,
        )?;

        let window = WindowAggExec::try_new(vec![window_expr], input, true)?;
        assert!(matches!(
            window.cardinality_effect(),
            CardinalityEffect::Equal
        ));
        Ok(())
    }

    #[test]
    fn parallel_window_stream_planning_requires_window_batch_accumulator() -> Result<()> {
        let schema =
            Arc::new(Schema::new(vec![Field::new("v", DataType::Float64, false)]));
        let value = crate::expressions::col("v", &schema)?;
        let aggregate = Arc::new(
            AggregateExprBuilder::new(avg_udaf(), vec![Arc::clone(&value)])
                .schema(Arc::clone(&schema))
                .alias("moving_avg")
                .build()?,
        );
        let order_by = vec![PhysicalSortExpr::new_default(value)];
        let window_expr: Arc<dyn WindowExpr> =
            Arc::new(ParallelSlidingAggregateWindowExpr::try_new(
                aggregate,
                &[],
                &order_by,
                Arc::new(WindowFrame::new_bounds(
                    WindowFrameUnits::Rows,
                    WindowFrameBound::Preceding(ScalarValue::UInt64(Some(1))),
                    WindowFrameBound::CurrentRow,
                )),
                None,
            )?);

        assert!(can_use_parallel_window_stream(&[window_expr])?);
        Ok(())
    }

    #[test]
    fn window_agg_display_shows_parallel_window_stream_choice() -> Result<()> {
        let schema =
            Arc::new(Schema::new(vec![Field::new("v", DataType::Float64, false)]));
        let input: Arc<dyn ExecutionPlan> =
            Arc::new(TestMemoryExec::try_new(&[], Arc::clone(&schema), None)?);
        let value = crate::expressions::col("v", &schema)?;
        let aggregate = Arc::new(
            AggregateExprBuilder::new(avg_udaf(), vec![Arc::clone(&value)])
                .schema(Arc::clone(&schema))
                .alias("moving_avg")
                .build()?,
        );
        let order_by = vec![PhysicalSortExpr::new_default(value)];
        let window_expr: Arc<dyn WindowExpr> =
            Arc::new(ParallelSlidingAggregateWindowExpr::try_new(
                aggregate,
                &[],
                &order_by,
                Arc::new(WindowFrame::new_bounds(
                    WindowFrameUnits::Rows,
                    WindowFrameBound::Preceding(ScalarValue::UInt64(Some(1))),
                    WindowFrameBound::CurrentRow,
                )),
                None,
            )?);

        let window = WindowAggExec::try_new(vec![window_expr], input, true)?;
        let display = displayable(&window).indent(false).to_string();
        assert!(display.contains("stream=[ParallelWindowStream]"));
        Ok(())
    }

    #[tokio::test]
    async fn parallel_window_stream_outputs_tiles_in_order() -> Result<()> {
        let schema =
            Arc::new(Schema::new(vec![Field::new("v", DataType::Float64, false)]));
        let batch1 = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![Arc::new(Float64Array::from(vec![1.0, 2.0]))],
        )?;
        let batch2 = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![Arc::new(Float64Array::from(vec![3.0, 4.0, 5.0]))],
        )?;

        let value = crate::expressions::col("v", &schema)?;
        let aggregate = Arc::new(
            AggregateExprBuilder::new(avg_udaf(), vec![Arc::clone(&value)])
                .schema(Arc::clone(&schema))
                .alias("moving_avg")
                .build()?,
        );
        let order_by = vec![PhysicalSortExpr::new_default(value)];
        let window_expr: Arc<dyn WindowExpr> =
            Arc::new(ParallelSlidingAggregateWindowExpr::try_new(
                aggregate,
                &[],
                &order_by,
                Arc::new(WindowFrame::new_bounds(
                    WindowFrameUnits::Rows,
                    WindowFrameBound::Preceding(ScalarValue::UInt64(Some(1))),
                    WindowFrameBound::CurrentRow,
                )),
                None,
            )?);

        let output_schema =
            Arc::new(create_schema(&schema, &[Arc::clone(&window_expr)])?);
        let input: SendableRecordBatchStream = Box::pin(RecordBatchStreamAdapter::new(
            Arc::clone(&schema),
            stream::iter(vec![Ok(batch1), Ok(batch2)]),
        ));
        let metrics = ExecutionPlanMetricsSet::new();
        let stream: SendableRecordBatchStream = Box::pin(ParallelWindowStream::new(
            Arc::clone(&output_schema),
            vec![window_expr],
            input,
            BaselineMetrics::new(&metrics, 0),
            vec![],
            &[],
            ParallelWindowStreamConfig::new(2, 2),
        )?);

        let batches = crate::common::collect(stream).await?;
        assert_eq!(batches.len(), 2);
        let batch = concat_batches(&output_schema, &batches)?;
        assert_eq!(
            as_float64_array(batch.column(0))?.values(),
            &[1.0, 2.0, 3.0, 4.0, 5.0]
        );
        assert_eq!(
            as_float64_array(batch.column(1))?.values(),
            &[1.0, 1.5, 2.5, 3.5, 4.5]
        );
        Ok(())
    }
}
