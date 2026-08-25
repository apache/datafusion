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

//! Nested loop join stream specifically for semi, anti, and mark joins
//! Instantiated by [`NestedLoopJoinExec`](crate::joins::nested_loop_join::NestedLoopJoinExec)
//! when the join type is `LeftSemi`, `LeftAnti`, `RightSemi`, `RightAnti`,
//! `LeftMark`, or `RightMark`.

use std::future::poll_fn;
use std::sync::Arc;
use std::sync::atomic::AtomicUsize;

use super::materializing_stream::{
    JoinLeftData, NestedLoopJoinMetrics, SpillState, SpillStateActive,
};
use crate::SendableRecordBatchStream;
use crate::joins::nested_loop_join::materializing_stream::LeftSpillData;
use crate::joins::utils::{
    ColumnIndex, JoinFilter, OnceFut, need_produce_result_in_final,
};
use crate::spill::replayable_spill_input::ReplayableStreamSource;
use crate::spill::spill_manager::SpillManager;
use crate::stream::{ObservedStream, RecordBatchStreamAdapter};

use arrow::array::{BooleanArray, BooleanBufferBuilder};
use arrow::buffer::BooleanBuffer;
use arrow::compute::{BatchCoalescer, concat_batches};
use arrow::datatypes::Schema;
use arrow::record_batch::RecordBatch;
use datafusion_common::{DataFusionError, JoinSide, Result, internal_err};
use datafusion_execution::memory_pool::MemoryConsumer;
use datafusion_execution::{TryEmitter, async_try_stream};
use datafusion_expr::JoinType;
use futures::StreamExt;
use log::debug;
use parking_lot::Mutex;

/// Note that we're using the explicit state management even with the async generator pattern
/// The motivation behind this is due to the complexity for NLJ
///
/// State transition graph:
/// ============================
///
/// (start) --> BufferingLeft
/// ----------------------------
/// BufferingLeft → FetchingRight
///
/// FetchingRight → ProbeRight (if right batch available)
/// FetchingRight → ProbeEnd (if right exhausted)
///
/// ProbeRight → ProbeRight (next left row or after emitting output)
/// ProbeRight → EmitRightResult (for right joins)
/// ProbeRight → FetchingRight (done with the current right batch)
///
/// EmitRightResult → FetchingRight
///
/// ProbeEnd → EmitLeftResult (records whether this partition is responsible for
/// left side output, then always continues to EmitLeftResult)
///
/// EmitLeftResult → EmitLeftResult (only process 1 chunk for each
/// iteration)
/// EmitLeftResult -> EmitGlobalRightResult (if all chunks are doe and this is spilled right join)
///
/// EmitLeftResult → Done (if finished)
/// ----------------------------
/// Done → (end)

#[derive(Debug, Clone, Copy)]
enum SAMNLJState {
    BufferingLeft,
    FetchingRight,
    ProbeRight,
    /// Entered exactly once per left chunk, when the probe (right) side is
    /// exhausted and probing for the current chunk is finished. This state
    /// owns the single [`JoinLeftData::report_probe_completed`] call that
    /// decrements the shared probe-threads counter.
    ProbeEnd,
    EmitLeftResult,
    EmitRightResult,
    /// Emit rows using the global bitmap accumulated across all left chunks.
    /// Only used in memory-limited mode for join types that require
    /// tracking right-side matches in the final output (RIGHT SEMI/ANTI/MARK)
    EmitGlobalRightResult,
    Done,
}

/// Nested loop join stream for Semi/Anti/Mark joins.
///
/// Evaluates the join predicate for every relevant left/right combination but unlike `materializing_stream`,
/// this does not emit `(left, right)` pairs - instead we accumulate a Boolean value for each row
/// on the output side to check for any match
///
/// For left joins:
///     - matches accumulate in the shared left bitmap
///     - every right partition must finish probing
/// For right joins:
///     - matches accumulate for each right batch
///     - result can be emitted once batch has been compared with all buffered left rows (without spill)
#[expect(dead_code)]
pub(crate) struct SemiAntiMarkNestedLoopJoinStream {
    // ========================================================================
    // PROPERTIES:
    // Operator's properties that remain constant
    //
    // Note: The implementation uses the terms left/build-side table and
    // right/probe-side table interchangeably. Treating the left side as the
    // build side is a convention in DataFusion: the planner always tries to
    // swap the smaller table to the left side.
    // ========================================================================
    /// Output schema
    output_schema: Arc<Schema>,
    /// join filter
    join_filter: Option<JoinFilter>,
    /// type of the join
    join_type: JoinType,
    /// output side of the join
    join_side: JoinSide,
    /// the probe-side(right) table data of the nested loop join
    /// `Option` is used because memory-limited path requires resetting it.
    right_data: Option<SendableRecordBatchStream>,
    /// the build-side table data of the nested loop join
    left_data: OnceFut<JoinLeftData>,

    /// Projection to construct the output schema from the left and right tables.
    /// Example:
    /// - output_schema: ['a', 'c']
    /// - left_schema: ['a', 'b']
    /// - right_schema: ['c']
    ///
    /// The column indices would be [(left, 0), (right, 0)] -- taking the left
    /// 0th column and right 0th column can construct the output schema.
    ///
    /// Note there are other columns ('b' in the example) still kept after
    /// projection pushdown; this is because they might be used to evaluate
    /// the join filter (e.g., `JOIN ON (b+c)>0`).
    column_indices: Vec<ColumnIndex>,
    /// Join execution metrics
    metrics: NestedLoopJoinMetrics,

    /// `batch_size` from configuration
    batch_size: usize,

    // ========================================================================
    // STATE FLAGS/BUFFERS:
    // Fields that hold intermediate data/flags during execution
    // ========================================================================
    /// State Tracking
    state: SAMNLJState,
    /// Output buffer holds the join result to output. It will emit eagerly when
    /// the threshold is reached.
    output_buffer: Box<BatchCoalescer>,

    /// Memory-limited spill fallback state. See [`SpillState`] for details.
    spill_state: SpillState,
}

impl SemiAntiMarkNestedLoopJoinStream {
    #[expect(clippy::too_many_arguments)]
    // TODO: fix later
    pub(crate) fn new(
        schema: Arc<Schema>,
        filter: Option<JoinFilter>,
        join_type: JoinType,
        right_data: SendableRecordBatchStream,
        left_data: OnceFut<JoinLeftData>,
        column_indices: Vec<ColumnIndex>,
        metrics: NestedLoopJoinMetrics,
        batch_size: usize,
        spill_state: SpillState,
    ) -> Result<SendableRecordBatchStream> {
        debug_assert!(
            matches!(
                join_type,
                JoinType::LeftSemi
                    | JoinType::RightSemi
                    | JoinType::LeftAnti
                    | JoinType::RightAnti
                    | JoinType::LeftMark
                    | JoinType::RightMark
            ),
            "SemiAntiMarkNestedLoopJoinStream does not handle {join_type:?}"
        );

        let join_side = match join_type {
            JoinType::LeftSemi | JoinType::LeftAnti | JoinType::LeftMark => {
                JoinSide::Left
            }
            _ => JoinSide::Right,
        };

        let state = Self {
            output_schema: Arc::clone(&schema),
            join_filter: filter,
            join_type,
            join_side,
            right_data: Some(right_data),
            column_indices,
            left_data,
            metrics,
            output_buffer: Box::new(BatchCoalescer::new(schema, batch_size)),
            batch_size,
            state: SAMNLJState::BufferingLeft,
            spill_state,
        };

        let stream = async_try_stream(|mut emitter| async move {
            state.start_join_time();
            let result = state.join(&mut emitter).await;
            state.stop_join_time();
            result
        });
        // ObservedStream records the baseline metrics (output rows/batches,
        // end time) exactly as the former hand-written poll_next did.
        Ok(Box::pin(ObservedStream::new(
            Box::pin(RecordBatchStreamAdapter::new(schema, stream)),
            baseline_metrics,
            None,
        )))
    }

    /// Start (resume) the `join_time` clock.
    fn start_join_time(&mut self) {
        // debug_assert!(self.join_time_start.is_none(), "join_time already running");
        // self.join_time_start = Some(Instant::now());
    }

    /// Stop (pause) the `join_time` clock, accumulating the elapsed span.
    ///
    /// Called around awaits whose duration is not the join's own work: the
    /// child input streams' `next()` and `emitter.emit()` (where the
    /// consumer processes the batch). The join's own spill read-back is NOT
    /// excluded — that time is join work.
    fn stop_join_time(&mut self) {
        // if let Some(start) = self.join_time_start.take() {
        //     self.join_time.add_elapsed(start);
        // }
    }

    /// Main loop - TODO describe further
    async fn join(
        &mut self,
        emitter: &mut TryEmitter<RecordBatch, DataFusionError>,
    ) -> Result<()> {
        let mut left_chunk: Option<Arc<JoinLeftData>> = None;
        let mut is_last_chunk: bool = false;

        let mut right_batch: Option<RecordBatch> = None;
        let right_batch_matched: Option<BooleanArray> = None;
        loop {
            match self.state {
                // # SAMNLJState transitions
                // --> FetchingRight
                // This state will prepare the left side batches, next state
                // `FetchingRight` is responsible for preparing a single probe
                // side batch, before start joining.
                SAMNLJState::BufferingLeft => {
                    debug!("[SAMNLJState] Entering: {:?}", self.state);
                    // inside `collect_left_input` (the routine to buffer build
                    // -side batches), related metrics except build time will be
                    // updated.
                    // stop on drop
                    let build_metric = self.metrics.join_metrics.build_time.clone();
                    let _build_timer = build_metric.timer();

                    let (curr_left_chunk, curr_is_last_chunk) =
                        self.handle_buffering_left().await?;
                    left_chunk = Some(curr_left_chunk);
                    is_last_chunk = curr_is_last_chunk;
                    self.state = SAMNLJState::FetchingRight;
                }

                // # SAMNLJState transitions:
                // 1. --> ProbeRight
                //    Start processing the join for the newly fetched right
                //    batch.
                // 2. --> ProbeEnd: When the right side input is exhausted,
                //    probing for the current left chunk is finished.
                //
                // After fetching a new batch from the right side, it will
                // process all rows from the buffered left data:
                // ```text
                // for batch in right_side:
                //     for row in left_buffer:
                //         join(batch, row)
                // ```
                // Note: the implementation does this step incrementally,
                // instead of materializing all intermediate Cartesian products
                // at once in memory.
                //
                // So after the right side input is exhausted, the join phase
                // for the current buffered left data is finished. We go to the
                // `ProbeEnd` state, which records probe completion before the
                // `EmitLeftUnmatched` phase checks if there is any special
                // handling (e.g., in cases like left join).
                SAMNLJState::FetchingRight => {
                    debug!("[SAMNLJState] Entering: {:?}", self.state);
                    // stop on drop
                    let join_metric = self.metrics.join_metrics.join_time.clone();
                    let _join_timer = join_metric.timer();

                    if let Some(curr_right_batch) = self.handle_fetching_right().await? {
                        right_batch = Some(curr_right_batch);
                        self.state = SAMNLJState::ProbeRight;
                        // Prepare right bitmap
                        if self.join_side == JoinSide::Right {
                            let zeroed_buf =
                                BooleanBuffer::new_unset(curr_right_batch.num_rows());
                            right_batch_matched =
                                Some(BooleanArray::new(zeroed_buf, None));
                        }
                    } else {
                        self.state = SAMNLJState::ProbeEnd;
                    }
                }

                // SAMNLJState transitions:
                // 1. --> ProbeRight(1)
                //    If we have already buffered enough output to yield, it
                //    will first give back control to the parent state machine,
                //    then resume at the same place.
                // 2. --> ProbeRight(2)
                //    After probing one right batch, and evaluating the
                //    join filter on (left-row x right-batch), it will advance
                //    to the next left row, then re-enter the current state and
                //    continue joining.
                // 3. --> FetchRight
                //    After it has done with the current right batch (to join
                //    with all rows in the left buffer), it will go to
                //    FetchRight state to check what to do next.
                SAMNLJState::ProbeRight => {
                    debug!("[SAMNLJState] Entering: {:?}", self.state);

                    // stop on drop
                    let join_metric = self.metrics.join_metrics.join_time.clone();
                    let _join_timer = join_metric.timer();

                    match self.handle_probe_right() {
                        ControlFlow::Continue(()) => continue,
                        ControlFlow::Break(poll) => {
                            return self.metrics.join_metrics.baseline.record_poll(poll);
                        }
                    }
                }

                // In the `current_right_batch_matched` bitmap, all trues mean
                // it has been output by the join. In this state we have to
                // output unmatched rows for current right batch (with null
                // padding for left relation)
                // Precondition: we have checked the join type so that it's
                // possible to output right unmatched (e.g. it's right join)
                SAMNLJState::EmitRightResult => {
                    debug!("[SAMNLJState] Entering: {:?}", self.state);

                    // stop on drop
                    let join_metric = self.metrics.join_metrics.join_time.clone();
                    let _join_timer = join_metric.timer();

                    match self.handle_emit_right_unmatched() {
                        ControlFlow::Continue(()) => continue,
                        ControlFlow::Break(poll) => {
                            return self.metrics.join_metrics.baseline.record_poll(poll);
                        }
                    }
                }

                // SAMNLJState transitions:
                // 1. --> EmitLeftUnmatched
                //    Probing for the current left chunk is finished. Report
                //    probe completion exactly once (decrementing the shared
                //    probe-threads counter) and record whether this stream is
                //    the unmatched-left emitter, then always advance to
                //    `EmitLeftUnmatched`.
                SAMNLJState::ProbeEnd => {
                    debug!("[SAMNLJState] Entering: {:?}", self.state);

                    // stop on drop
                    let join_metric = self.metrics.join_metrics.join_time.clone();
                    let _join_timer = join_metric.timer();

                    match self.handle_probe_end() {
                        ControlFlow::Continue(()) => continue,
                        ControlFlow::Break(poll) => {
                            return self.metrics.join_metrics.baseline.record_poll(poll);
                        }
                    }
                }

                // SAMNLJState transitions:
                // 1. --> EmitLeftUnmatched(1)
                //    If we have already buffered enough output to yield, it
                //    will first give back control to the parent state machine,
                //    then resume at the same place.
                // 2. --> EmitLeftUnmatched(2)
                //    After processing some unmatched rows, it will re-enter
                //    the same state, to check if there are any more final
                //    results to output.
                // 3. --> Done
                //    It has processed all data, go to the final state and ready
                //    to exit.
                // 4. --> BufferingLeft (memory-limited mode only)
                //    When left data was loaded in chunks and more chunks remain,
                //    go back to BufferingLeft to load the next chunk.
                SAMNLJState::EmitLeftResult => {
                    debug!("[SAMNLJState] Entering: {:?}", self.state);

                    // stop on drop
                    let join_metric = self.metrics.join_metrics.join_time.clone();
                    let _join_timer = join_metric.timer();

                    match self.handle_emit_left_unmatched() {
                        ControlFlow::Continue(()) => continue,
                        ControlFlow::Break(poll) => {
                            return self.metrics.join_metrics.baseline.record_poll(poll);
                        }
                    }
                }

                // Replay all right batches from spill and emit unmatched
                // right rows using the global bitmap accumulated across all
                // left chunks. Only entered in memory-limited mode for join
                // types where `should_track_unmatched_right` is true
                // (RIGHT, FULL, RIGHT SEMI, RIGHT ANTI, RIGHT MARK).
                SAMNLJState::EmitGlobalRightResult => {
                    debug!("[SAMNLJState] Entering: {:?}", self.state);

                    let join_metric = self.metrics.join_metrics.join_time.clone();
                    let _join_timer = join_metric.timer();

                    match self.handle_emit_global_right_unmatched(cx) {
                        ControlFlow::Continue(()) => continue,
                        ControlFlow::Break(poll) => {
                            return self.metrics.join_metrics.baseline.record_poll(poll);
                        }
                    }
                }

                // The final state and the exit point
                SAMNLJState::Done => {
                    debug!("[SAMNLJState] Entering: {:?}", self.state);

                    // stop on drop
                    let join_metric = self.metrics.join_metrics.join_time.clone();
                    let _join_timer = join_metric.timer();
                    // counting it in join timer due to there might be some
                    // final resout batches to output in this state

                    let poll = self.handle_done();
                    return self.metrics.join_metrics.baseline.record_poll(poll);
                }
            }
        }
    }

    // ========================================================================
    // Functions for the BufferingLeft state
    // ========================================================================

    /// Handle BufferingLeft state - prepare left side batches.
    ///
    /// In standard mode, uses OnceFut to load all left data at once.
    /// In memory-limited mode, incrementally buffers left batches until the
    /// memory budget is reached or the left stream is exhausted.
    ///
    /// Returns a two-tuple of the (left chunk, boolean indicating whether this is the last chunk)
    async fn handle_buffering_left(&mut self) -> Result<(Arc<JoinLeftData>, bool)> {
        loop {
            if self.is_memory_limited() {
                return self.handle_buffering_left_memory_limited().await;
            }

            // Standard path: use OnceFut
            let left_data_result = poll_fn(|cx| self.left_data.get_shared(cx)).await;
            match left_data_result {
                Ok(left_data) => return Ok((left_data, true)),
                Err(e) => {
                    if self.can_fallback_to_spill(&e) {
                        debug!(
                            "NestedLoopJoin: OnceFut failed with OOM, \
                             falling back to memory-limited mode"
                        );
                        self.initiate_fallback()?;
                    } else {
                        return Err(e);
                    }
                }
            }
        }
    }

    /// Memory-limited path for handle_buffering_left.
    ///
    /// Incrementally polls the left stream and accumulates batches until:
    /// - Memory reservation fails (chunk is full, more data remains)
    /// - Left stream is exhausted (this is the last/only chunk)
    async fn handle_buffering_left_memory_limited(
        &mut self,
    ) -> Result<(Arc<JoinLeftData>, bool)> {
        let SpillState::Active(active) = &mut self.spill_state else {
            unreachable!(
                "handle_buffering_left_memory_limited called without Active spill state"
            );
        };

        // On first entry (or after re-entry for a new chunk pass when
        // left_stream was consumed), wait for the shared left spill
        // future to resolve and then open a stream from the spill file.
        if active.left_stream.is_none() {
            let spill_data = poll_fn(|cx| active.left_spill_fut.get_shared(cx)).await?;

            let stream = spill_data
                .spill_manager
                .read_spill_as_stream(Arc::clone(&spill_data.spill_file), None)?;
            active.left_schema = Some(Arc::clone(&spill_data.schema));
            active.left_stream = Some(stream);
        }

        let left_stream = active
            .left_stream
            .as_mut()
            .expect("left_stream must be set after spill future resolves");

        let is_last_chunk;

        // Poll left stream for more batches.
        // Note: pending_batches may already contain a batch from the
        // previous chunk iteration (the batch that triggered the memory limit).
        loop {
            match left_stream.next().await {
                Some(Ok(batch)) => {
                    if batch.num_rows() == 0 {
                        continue;
                    }
                    let batch_rows = batch.num_rows();
                    let batch_size = batch.get_array_memory_size();
                    let can_grow = active.reservation.try_grow(batch_size).is_ok();

                    if !can_grow && !active.pending_batches.is_empty() {
                        // Memory limit reached and we already have data.
                        // Push this batch into pending (it's already in memory)
                        // and stop buffering for this chunk.
                        active.pending_batches.push(batch);
                        is_last_chunk = false;
                        break;
                    } else if !can_grow {
                        // No pending batches yet — we must accept this batch
                        // to make progress, even if it exceeds the budget.
                        active.reservation.grow(batch_size);
                    }

                    self.metrics.join_metrics.build_mem_used.add(batch_size);
                    self.metrics.join_metrics.build_input_batches.add(1);
                    self.metrics.join_metrics.build_input_rows.add(batch_rows);
                    active.pending_batches.push(batch);
                }
                Some(Err(e)) => return Err(e),
                None => {
                    // Left stream exhausted
                    is_last_chunk = true;
                    break;
                }
            }
        }

        // If the left stream is fully exhausted, release its resources so the
        // upstream pipeline can be torn down before we move on to probing.
        if is_last_chunk {
            active.left_stream = None;
        }

        if active.pending_batches.is_empty() {
            // No data at all — go directly to Done
            return internal_err!("Left spill stream produced no data");
        }

        let merged_batch = concat_batches(
            active
                .left_schema
                .as_ref()
                .expect("left_schema must be set"),
            &active.pending_batches,
        )?;
        active.pending_batches.clear();

        // Build visited bitmap if needed for this join type
        let with_visited = need_produce_result_in_final(self.join_type);
        let n_rows = merged_batch.num_rows();
        let visited_left_side = if with_visited {
            let buffer_size = n_rows.div_ceil(8);
            // Use infallible grow for bitmap — it's small
            active.reservation.grow(buffer_size);
            self.metrics.join_metrics.build_mem_used.add(buffer_size);
            let mut buffer = BooleanBufferBuilder::new(n_rows);
            buffer.append_n(n_rows, false);
            buffer
        } else {
            BooleanBufferBuilder::new(0)
        };

        // Create an empty reservation for JoinLeftData's RAII field.
        // The actual memory tracking is managed by the Active state's reservation.
        let dummy_reservation = active.reservation.new_empty();

        let left_data = JoinLeftData::new(
            merged_batch,
            Mutex::new(visited_left_side),
            // In memory-limited mode, only 1 probe thread per chunk
            AtomicUsize::new(1),
            dummy_reservation,
        );

        active.right_batch_index = 0;
        self.right_data = Some(active.right_input.open_pass()?);

        Ok((Arc::new(left_data), is_last_chunk))
    }

    /// Returns true if this stream is operating in memory-limited mode
    fn is_memory_limited(&self) -> bool {
        matches!(self.spill_state, SpillState::Active(_))
    }

    /// Check if we can fall back to memory-limited mode on this error.
    fn can_fallback_to_spill(&self, error: &datafusion_common::DataFusionError) -> bool {
        matches!(self.spill_state, SpillState::Pending { .. })
            && matches!(
                error.find_root(),
                datafusion_common::DataFusionError::ResourcesExhausted(_)
            )
    }

    /// Switch from the standard OnceFut path to memory-limited mode.
    ///
    /// Uses the shared `left_spill_data` OnceAsync so that only the first
    /// partition to reach this point re-executes the left child and spills
    /// it to disk. Other partitions share the same spill file.
    fn initiate_fallback(&mut self) -> Result<()> {
        // Take ownership of Pending state
        let SpillState::Pending {
            left_plan,
            task_context: context,
            left_spill_data,
        } = std::mem::replace(&mut self.spill_state, SpillState::Disabled)
        else {
            return internal_err!("initiate_fallback called in non-Pending spill state");
        };

        // Use OnceAsync to ensure only the first partition spills the left
        // side. Other partitions will get the same OnceFut that resolves
        // to the shared spill file.
        let left_spill_fut = left_spill_data.try_once(|| {
            let plan = Arc::clone(&left_plan);
            let ctx = Arc::clone(&context);
            let spill_metrics = self.metrics.spill_metrics.clone();
            Ok(async move {
                let mut stream = plan.execute(0, Arc::clone(&ctx))?;
                let schema = stream.schema();
                let left_spill_manager = SpillManager::new(
                    ctx.runtime_env(),
                    spill_metrics,
                    Arc::clone(&schema),
                )
                .with_compression_type(ctx.session_config().spill_compression());

                let result = left_spill_manager
                    .spill_record_batch_stream_and_return_max_batch_memory(
                        &mut stream,
                        "NestedLoopJoin left spill",
                    )
                    .await?;

                match result {
                    Some((file, _max_batch_memory)) => Ok(LeftSpillData {
                        spill_manager: left_spill_manager,
                        spill_file: file,
                        schema,
                    }),
                    None => {
                        internal_err!("Left side produced no data to spill")
                    }
                }
            })
        })?;

        // Create reservation with can_spill for fair memory allocation
        let reservation = MemoryConsumer::new("NestedLoopJoinLoad[fallback]".to_string())
            .with_can_spill(true)
            .register(context.memory_pool());

        // Separate reservation for the global right bitmaps. These buffers
        // persist across all left chunks, whereas `reservation` is reset
        // between chunks via `resize(0)`.
        let global_right_bitmaps_reservation =
            MemoryConsumer::new("NestedLoopJoinGlobalRightBitmaps".to_string())
                .register(context.memory_pool());

        // Create SpillManager for right-side spilling
        let right_schema = self
            .right_data
            .as_ref()
            .expect("right_data must be present before fallback")
            .schema();
        let right_data = self
            .right_data
            .take()
            .expect("right_data must be present before fallback");
        let right_spill_manager = SpillManager::new(
            context.runtime_env(),
            self.metrics.spill_metrics.clone(),
            right_schema,
        )
        .with_compression_type(context.session_config().spill_compression());

        self.spill_state = SpillState::Active(Box::new(SpillStateActive {
            left_spill_fut,
            left_stream: None,
            left_schema: None,
            reservation,
            pending_batches: Vec::new(),
            right_input: ReplayableStreamSource::new(
                right_data,
                right_spill_manager,
                "NestedLoopJoin right spill",
            ),
            global_right_bitmaps: Vec::new(),
            global_right_bitmaps_reservation,
            right_batch_index: 0,
        }));

        // State stays BufferingLeft — next iteration will enter
        // handle_buffering_left_memory_limited via is_memory_limited() check
        self.state = SAMNLJState::BufferingLeft;

        Ok(())
    }

    // ========================================================================
    // Functions for the FetchingRight state
    // ========================================================================
    /// Handle FetchingRight state - fetch next right batch and prepare for processing.
    ///
    /// In memory-limited mode during the first pass, each right batch is also
    /// written to a spill file so it can be re-read on subsequent passes.
    async fn handle_fetching_right(&mut self) -> Result<Option<RecordBatch>> {
        loop {
            match self
                .right_data
                .as_mut()
                .expect("right_data must be present while fetching right")
                .next()
                .await
            {
                Some(Ok(right_batch)) => {
                    // Update metrics
                    let right_batch_rows = right_batch.num_rows();
                    self.metrics.join_metrics.input_rows.add(right_batch_rows);
                    self.metrics.join_metrics.input_batches.add(1);

                    // Skip the empty batch
                    if right_batch_rows == 0 {
                        continue;
                    }

                    return Ok(Some(right_batch));
                }
                Some(Err(e)) => return Err(e),
                None => {
                    // Right side exhausted: probing for the current left chunk
                    // is finished.
                    return Ok(None);
                }
            }
        }
    }
}
