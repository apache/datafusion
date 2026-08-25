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

//! Materializing nested loop join stream.

use std::ops::{BitOr, ControlFlow};
use std::sync::Arc;
use std::task::Poll;

use super::shared::{
    JoinLeftData, LeftBufferBatchDecision, NestedLoopJoinMetrics, SpillState,
    apply_filter_to_row_join_batch, boolean_mask_from_filter, buffer_left_batch_in_chunk,
    build_global_right_result_batch, build_row_join_batch, build_unmatched_batch,
    create_record_batch_with_empty_schema, finalize_buffered_left_chunk,
    initiate_spill_fallback,
};
use crate::joins::utils::need_produce_result_in_final;
use crate::joins::utils::{
    ColumnIndex, JoinFilter, OnceFut, need_produce_right_in_final,
};
use crate::metrics::Count;
use crate::{RecordBatchStream, SendableRecordBatchStream};

use arrow::array::{Array, BooleanArray, BooleanBufferBuilder, UInt32Array};
use arrow::buffer::BooleanBuffer;
use arrow::compute::{BatchCoalescer, filter_record_batch, take};
use arrow::datatypes::{Schema, SchemaRef};
use arrow::record_batch::RecordBatch;
use datafusion_common::cast::as_boolean_array;
use datafusion_common::{
    JoinSide, Result, arrow_err, internal_datafusion_err, unwrap_or_internal_err,
};
use datafusion_expr::JoinType;

use futures::{Stream, StreamExt};
use log::debug;

/// States for join processing. See `poll_next()` comment for more details about
/// state transitions.
#[derive(Debug, Clone, Copy)]
pub(super) enum NLJState {
    BufferingLeft,
    FetchingRight,
    ProbeRight,
    EmitRightUnmatched,
    /// Entered exactly once per left chunk, when the probe (right) side is
    /// exhausted and probing for the current chunk is finished. This state
    /// owns the single [`JoinLeftData::report_probe_completed`] call that
    /// decrements the shared probe-threads counter, and records in
    /// `is_unmatched_left_emitter` whether this stream is the one responsible
    /// for emitting unmatched-left rows. Splitting this decision out of
    /// `EmitLeftUnmatched` makes "decrement exactly once" a structural
    /// property of the state graph, so the (re-enterable) emit state no longer
    /// has to guard against decrementing twice.
    ProbeEnd,
    EmitLeftUnmatched,
    /// Emit unmatched right rows using the global bitmap accumulated across
    /// all left chunks. Only used in memory-limited mode for join types that
    /// require tracking right-side matches in the final output (RIGHT, FULL,
    /// RIGHT SEMI, RIGHT ANTI, RIGHT MARK).
    EmitGlobalRightUnmatched,
    Done,
}

pub(crate) struct NestedLoopJoinStream {
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
    pub(crate) output_schema: Arc<Schema>,
    /// join filter
    pub(crate) join_filter: Option<JoinFilter>,
    /// type of the join
    pub(crate) join_type: JoinType,
    /// the probe-side(right) table data of the nested loop join
    /// `Option` is used because memory-limited path requires resetting it.
    pub(crate) right_data: Option<SendableRecordBatchStream>,
    /// the build-side table data of the nested loop join
    pub(crate) left_data: OnceFut<JoinLeftData>,
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
    pub(crate) column_indices: Vec<ColumnIndex>,
    /// Join execution metrics
    pub(crate) metrics: NestedLoopJoinMetrics,

    /// `batch_size` from configuration
    batch_size: usize,

    /// See comments in [`need_produce_right_in_final`] for more detail
    should_track_unmatched_right: bool,

    // ========================================================================
    // STATE FLAGS/BUFFERS:
    // Fields that hold intermediate data/flags during execution
    // ========================================================================
    /// State Tracking
    state: NLJState,
    /// Output buffer holds the join result to output. It will emit eagerly when
    /// the threshold is reached.
    output_buffer: Box<BatchCoalescer>,
    /// See comments in [`NLJState::Done`] for its purpose
    handled_empty_output: bool,

    // Buffer(left) side
    // -----------------
    /// The current buffered left data to join
    buffered_left_data: Option<Arc<JoinLeftData>>,
    /// Index into the left buffered batch. Used in `ProbeRight` state
    left_probe_idx: usize,
    /// Index into the left buffered batch. Used in `EmitLeftUnmatched` state
    left_emit_idx: usize,
    /// Should we go back to `BufferingLeft` state again after `EmitLeftUnmatched`
    /// state is over.
    left_exhausted: bool,
    /// If we can buffer all left data in one pass (false means memory-limited multi-pass)
    left_buffered_in_one_pass: bool,

    // Probe(right) side
    // -----------------
    /// The current probe batch to process
    current_right_batch: Option<RecordBatch>,
    // For right join, keep track of matched rows in `current_right_batch`
    // Constructed when fetching each new incoming right batch in `FetchingRight` state.
    current_right_batch_matched: Option<BooleanArray>,

    /// Memory-limited spill fallback state. See [`SpillState`] for details.
    spill_state: SpillState,

    /// Whether this stream is the one responsible for emitting unmatched-left
    /// rows for the current left chunk. Set in the [`NLJState::ProbeEnd`] state,
    /// which is entered exactly once per chunk and owns the single
    /// [`JoinLeftData::report_probe_completed`] call: the stream that drives the
    /// shared probe-threads counter to zero (the last to finish probing) becomes
    /// the emitter. Because the decrement happens once in `ProbeEnd` rather than
    /// in the re-enterable `EmitLeftUnmatched` state, the counter can never be
    /// decremented twice, so it cannot reach zero before all partitions finish
    /// probing (which would otherwise let a partition emit spurious NULL-padded
    /// unmatched-left rows early).
    is_unmatched_left_emitter: bool,
}

impl Stream for NestedLoopJoinStream {
    type Item = Result<RecordBatch>;

    /// See the comments [`NestedLoopJoinExec`] for high-level design ideas.
    ///
    /// # Implementation
    ///
    /// This function is the entry point of NLJ operator's state machine
    /// transitions. The rough state transition graph is as follow, for more
    /// details see the comment in each state's matching arm.
    ///
    /// ============================
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
    /// ProbeRight → ProbeRight (next left row or after yielding output)
    /// ProbeRight → EmitRightUnmatched (for special join types like right join)
    /// ProbeRight → FetchingRight (done with the current right batch)
    ///
    /// EmitRightUnmatched → FetchingRight
    ///
    /// ProbeEnd → EmitLeftUnmatched (records whether this stream is the
    /// unmatched-left emitter, then always continues to EmitLeftUnmatched)
    ///
    /// EmitLeftUnmatched → EmitLeftUnmatched (only process 1 chunk for each
    /// iteration)
    /// EmitLeftUnmatched → Done (if finished)
    /// ----------------------------
    /// Done → (end)
    fn poll_next(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> Poll<Option<Self::Item>> {
        loop {
            match self.state {
                // # NLJState transitions
                // --> FetchingRight
                // This state will prepare the left side batches, next state
                // `FetchingRight` is responsible for preparing a single probe
                // side batch, before start joining.
                NLJState::BufferingLeft => {
                    debug!("[NLJState] Entering: {:?}", self.state);
                    // inside `collect_left_input` (the routine to buffer build
                    // -side batches), related metrics except build time will be
                    // updated.
                    // stop on drop
                    let build_metric = self.metrics.join_metrics.build_time.clone();
                    let _build_timer = build_metric.timer();

                    match self.handle_buffering_left(cx) {
                        ControlFlow::Continue(()) => continue,
                        ControlFlow::Break(poll) => return poll,
                    }
                }

                // # NLJState transitions:
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
                NLJState::FetchingRight => {
                    debug!("[NLJState] Entering: {:?}", self.state);
                    // stop on drop
                    let join_metric = self.metrics.join_metrics.join_time.clone();
                    let _join_timer = join_metric.timer();

                    match self.handle_fetching_right(cx) {
                        ControlFlow::Continue(()) => continue,
                        ControlFlow::Break(poll) => return poll,
                    }
                }

                // NLJState transitions:
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
                NLJState::ProbeRight => {
                    debug!("[NLJState] Entering: {:?}", self.state);

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
                NLJState::EmitRightUnmatched => {
                    debug!("[NLJState] Entering: {:?}", self.state);

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

                // NLJState transitions:
                // 1. --> EmitLeftUnmatched
                //    Probing for the current left chunk is finished. Report
                //    probe completion exactly once (decrementing the shared
                //    probe-threads counter) and record whether this stream is
                //    the unmatched-left emitter, then always advance to
                //    `EmitLeftUnmatched`.
                NLJState::ProbeEnd => {
                    debug!("[NLJState] Entering: {:?}", self.state);

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

                // NLJState transitions:
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
                NLJState::EmitLeftUnmatched => {
                    debug!("[NLJState] Entering: {:?}", self.state);

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
                NLJState::EmitGlobalRightUnmatched => {
                    debug!("[NLJState] Entering: {:?}", self.state);

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
                NLJState::Done => {
                    debug!("[NLJState] Entering: {:?}", self.state);

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
}

impl RecordBatchStream for NestedLoopJoinStream {
    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.output_schema)
    }
}

impl NestedLoopJoinStream {
    #[expect(clippy::too_many_arguments)]
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
    ) -> Self {
        Self {
            output_schema: Arc::clone(&schema),
            join_filter: filter,
            join_type,
            right_data: Some(right_data),
            column_indices,
            left_data,
            metrics,
            buffered_left_data: None,
            output_buffer: Box::new(BatchCoalescer::new(schema, batch_size)),
            batch_size,
            current_right_batch: None,
            current_right_batch_matched: None,
            state: NLJState::BufferingLeft,
            left_probe_idx: 0,
            left_emit_idx: 0,
            left_exhausted: false,
            left_buffered_in_one_pass: true,
            handled_empty_output: false,
            should_track_unmatched_right: need_produce_right_in_final(join_type),
            spill_state,
            is_unmatched_left_emitter: false,
        }
    }

    // ==== State handler functions ====

    /// Handle BufferingLeft state - prepare left side batches.
    ///
    /// In standard mode, uses OnceFut to load all left data at once.
    /// In memory-limited mode, incrementally buffers left batches until the
    /// memory budget is reached or the left stream is exhausted.
    fn handle_buffering_left(
        &mut self,
        cx: &mut std::task::Context<'_>,
    ) -> ControlFlow<Poll<Option<Result<RecordBatch>>>> {
        if self.spill_state.is_active() {
            self.handle_buffering_left_memory_limited(cx)
        } else {
            // Standard path: use OnceFut
            match self.left_data.get_shared(cx) {
                Poll::Ready(Ok(left_data)) => {
                    self.buffered_left_data = Some(left_data);
                    self.left_exhausted = true;
                    self.state = NLJState::FetchingRight;
                    ControlFlow::Continue(())
                }
                Poll::Ready(Err(e)) => {
                    if self.spill_state.can_fallback_to_spill(&e) {
                        debug!(
                            "NestedLoopJoin: OnceFut failed with OOM, \
                             falling back to memory-limited mode"
                        );
                        match initiate_spill_fallback(
                            &mut self.spill_state,
                            &self.metrics,
                            &mut self.right_data,
                        ) {
                            Ok(()) => {
                                // State stays BufferingLeft — next poll will enter
                                // handle_buffering_left_memory_limited via
                                // SpillState::Active check
                                self.state = NLJState::BufferingLeft;
                                ControlFlow::Continue(())
                            }
                            Err(fallback_err) => {
                                ControlFlow::Break(Poll::Ready(Some(Err(fallback_err))))
                            }
                        }
                    } else {
                        ControlFlow::Break(Poll::Ready(Some(Err(e))))
                    }
                }
                Poll::Pending => ControlFlow::Break(Poll::Pending),
            }
        }
    }

    /// Memory-limited path for handle_buffering_left.
    ///
    /// Incrementally polls the left stream and accumulates batches until:
    /// - Memory reservation fails (chunk is full, more data remains)
    /// - Left stream is exhausted (this is the last/only chunk)
    fn handle_buffering_left_memory_limited(
        &mut self,
        cx: &mut std::task::Context<'_>,
    ) -> ControlFlow<Poll<Option<Result<RecordBatch>>>> {
        let SpillState::Active(active) = &mut self.spill_state else {
            unreachable!(
                "handle_buffering_left_memory_limited called without Active spill state"
            );
        };

        // On first entry (or after re-entry for a new chunk pass when
        // left_stream was consumed), wait for the shared left spill
        // future to resolve and then open a stream from the spill file.
        if active.left_stream.is_none() {
            match active.left_spill_fut.get_shared(cx) {
                Poll::Ready(Ok(spill_data)) => {
                    if let Err(e) = active.set_left_spill_data(&spill_data) {
                        return ControlFlow::Break(Poll::Ready(Some(Err(e))));
                    }
                }
                Poll::Ready(Err(e)) => {
                    return ControlFlow::Break(Poll::Ready(Some(Err(e))));
                }
                Poll::Pending => {
                    return ControlFlow::Break(Poll::Pending);
                }
            }
        }

        let left_stream = active
            .left_stream
            .as_mut()
            .expect("left_stream must be set after spill future resolves");

        // Poll left stream for more batches.
        // Note: pending_batches may already contain a batch from the
        // previous chunk iteration (the batch that triggered the memory limit).
        loop {
            match left_stream.poll_next_unpin(cx) {
                Poll::Ready(Some(Ok(batch))) => {
                    if batch.num_rows() == 0 {
                        continue;
                    }
                    match buffer_left_batch_in_chunk(
                        &mut active.reservation,
                        &mut active.pending_batches,
                        &self.metrics.join_metrics,
                        batch,
                    ) {
                        LeftBufferBatchDecision::Continue => {}
                        LeftBufferBatchDecision::ChunkFull => {
                            self.left_exhausted = false;
                            self.left_buffered_in_one_pass = false;
                            break;
                        }
                    }
                }
                Poll::Ready(Some(Err(e))) => {
                    return ControlFlow::Break(Poll::Ready(Some(Err(e))));
                }
                Poll::Ready(None) => {
                    // Left stream exhausted
                    self.left_exhausted = true;
                    break;
                }
                Poll::Pending => {
                    return ControlFlow::Break(Poll::Pending);
                }
            }
        }

        // If the left stream is fully exhausted, release its resources so the
        // upstream pipeline can be torn down before we move on to probing.
        if self.left_exhausted {
            active.left_stream = None;
        }

        if active.pending_batches.is_empty() {
            // No data at all — go directly to Done
            self.left_exhausted = true;
            self.state = NLJState::Done;
            return ControlFlow::Continue(());
        }

        let finalized = match finalize_buffered_left_chunk(
            active,
            &self.metrics.join_metrics,
            need_produce_result_in_final(self.join_type),
        ) {
            Ok(finalized) => finalized,
            Err(e) => {
                return ControlFlow::Break(Poll::Ready(Some(Err(e))));
            }
        };

        self.buffered_left_data = Some(Arc::new(finalized.left_data));
        self.right_data = Some(finalized.right_pass);

        self.state = NLJState::FetchingRight;
        ControlFlow::Continue(())
    }

    /// Handle FetchingRight state - fetch next right batch and prepare for processing.
    ///
    /// In memory-limited mode during the first pass, each right batch is also
    /// written to a spill file so it can be re-read on subsequent passes.
    fn handle_fetching_right(
        &mut self,
        cx: &mut std::task::Context<'_>,
    ) -> ControlFlow<Poll<Option<Result<RecordBatch>>>> {
        match self
            .right_data
            .as_mut()
            .expect("right_data must be present while fetching right")
            .poll_next_unpin(cx)
        {
            Poll::Ready(result) => match result {
                Some(Ok(right_batch)) => {
                    // Update metrics
                    let right_batch_rows = right_batch.num_rows();
                    self.metrics.join_metrics.input_rows.add(right_batch_rows);
                    self.metrics.join_metrics.input_batches.add(1);

                    // Skip the empty batch
                    if right_batch_rows == 0 {
                        return ControlFlow::Continue(());
                    }

                    self.current_right_batch = Some(right_batch);

                    // Prepare right bitmap
                    if self.should_track_unmatched_right {
                        let zeroed_buf = BooleanBuffer::new_unset(right_batch_rows);
                        self.current_right_batch_matched =
                            Some(BooleanArray::new(zeroed_buf, None));
                    }

                    self.left_probe_idx = 0;
                    self.state = NLJState::ProbeRight;
                    ControlFlow::Continue(())
                }
                Some(Err(e)) => ControlFlow::Break(Poll::Ready(Some(Err(e)))),
                None => {
                    // Right side exhausted: probing for the current left chunk
                    // is finished. `ProbeEnd` reports probe completion before
                    // emitting unmatched-left rows.
                    self.state = NLJState::ProbeEnd;
                    ControlFlow::Continue(())
                }
            },
            Poll::Pending => ControlFlow::Break(Poll::Pending),
        }
    }

    /// Handle ProbeRight state - process current probe batch
    fn handle_probe_right(&mut self) -> ControlFlow<Poll<Option<Result<RecordBatch>>>> {
        // Return any completed batches first
        if let Some(poll) = self.maybe_flush_ready_batch() {
            return ControlFlow::Break(poll);
        }

        // Process current probe state
        match self.process_probe_batch() {
            // State unchanged (ProbeRight)
            // Continue probing until we have done joining the
            // current right batch with all buffered left rows.
            Ok(true) => ControlFlow::Continue(()),
            // To next FetchRightState
            // We have finished joining
            // (cur_right_batch x buffered_left_batches)
            Ok(false) => {
                // Left exhausted, transition to FetchingRight
                self.left_probe_idx = 0;

                // Selectivity Metric: Update total possibilities for the batch (left_rows * right_rows)
                // If memory-limited execution is implemented, this logic must be updated accordingly.
                if let (Ok(left_data), Some(right_batch)) =
                    (self.get_left_data(), self.current_right_batch.as_ref())
                {
                    let left_rows = left_data.batch().num_rows();
                    let right_rows = right_batch.num_rows();
                    self.metrics.selectivity.add_total(left_rows * right_rows);
                }

                if self.should_track_unmatched_right {
                    debug_assert!(
                        self.current_right_batch_matched.is_some(),
                        "If it's required to track matched rows in the right input, the right bitmap must be present"
                    );
                    self.state = NLJState::EmitRightUnmatched;
                } else {
                    self.current_right_batch = None;
                    self.state = NLJState::FetchingRight;
                }
                ControlFlow::Continue(())
            }
            Err(e) => ControlFlow::Break(Poll::Ready(Some(Err(e)))),
        }
    }

    /// Handle EmitRightUnmatched state - emit unmatched right rows.
    ///
    /// In memory-limited mode, instead of emitting unmatched right rows
    /// per-batch (which would be incorrect since more left chunks may
    /// match those rows), we merge the bitmap into the global accumulator
    /// and defer emission to `EmitGlobalRightUnmatched`.
    fn handle_emit_right_unmatched(
        &mut self,
    ) -> ControlFlow<Poll<Option<Result<RecordBatch>>>> {
        // In memory-limited mode, merge bitmap into global and move on
        if self.spill_state.is_active() {
            debug_assert!(
                self.current_right_batch_matched.is_some(),
                "right bitmap must be present"
            );
            let bitmap = std::mem::take(&mut self.current_right_batch_matched)
                .expect("right bitmap should be available");

            if let SpillState::Active(ref mut active) = self.spill_state {
                active.handoff_completed_right_bitmap(bitmap);
            }

            self.current_right_batch = None;
            self.state = NLJState::FetchingRight;
            return ControlFlow::Continue(());
        }

        // Standard (single-pass) mode: emit unmatched right rows immediately
        // Return any completed batches first
        if let Some(poll) = self.maybe_flush_ready_batch() {
            return ControlFlow::Break(poll);
        }

        debug_assert!(
            self.current_right_batch_matched.is_some()
                && self.current_right_batch.is_some(),
            "This state is yielding output for unmatched rows in the current right batch, so both the right batch and the bitmap must be present"
        );
        match self.process_right_unmatched() {
            Ok(Some(batch)) => match self.output_buffer.push_batch(batch) {
                Ok(()) => {
                    debug_assert!(self.current_right_batch.is_none());
                    self.state = NLJState::FetchingRight;
                    ControlFlow::Continue(())
                }
                Err(e) => ControlFlow::Break(Poll::Ready(Some(arrow_err!(e)))),
            },
            Ok(None) => {
                debug_assert!(self.current_right_batch.is_none());
                self.state = NLJState::FetchingRight;
                ControlFlow::Continue(())
            }
            Err(e) => ControlFlow::Break(Poll::Ready(Some(Err(e)))),
        }
    }

    /// Handle ProbeEnd state - record probe completion for the current chunk.
    ///
    /// Entered exactly once per left chunk, when the right side is exhausted.
    /// This is the single place that decrements the shared probe-threads counter
    /// via [`JoinLeftData::report_probe_completed`]: the stream that drives the
    /// counter to zero (the last to finish probing) is the one responsible for
    /// emitting unmatched-left rows, recorded in `is_unmatched_left_emitter`.
    ///
    /// Owning the decrement here — rather than in the re-enterable
    /// `EmitLeftUnmatched` state — makes "decrement exactly once per stream" a
    /// structural property of the state graph, so the counter cannot reach zero
    /// before all partitions finish probing (which would let a partition emit
    /// spurious NULL-padded unmatched-left rows early).
    ///
    /// Always transitions to `EmitLeftUnmatched`.
    fn handle_probe_end(&mut self) -> ControlFlow<Poll<Option<Result<RecordBatch>>>> {
        // Decrement the shared counter exactly once for this stream/chunk. The
        // last stream to finish probing (the one that drives the counter to
        // zero) becomes the unmatched-left emitter.
        let is_emitter = match self.get_left_data() {
            Ok(left_data) => left_data.report_probe_completed(),
            Err(e) => return ControlFlow::Break(Poll::Ready(Some(Err(e)))),
        };
        self.is_unmatched_left_emitter = is_emitter;
        self.state = NLJState::EmitLeftUnmatched;
        ControlFlow::Continue(())
    }

    /// Handle EmitLeftUnmatched state - emit unmatched left rows.
    ///
    /// In memory-limited mode, after processing all unmatched rows for the
    /// current left chunk, transitions back to `BufferingLeft` to load the
    /// next chunk (if the left stream is not yet exhausted).
    fn handle_emit_left_unmatched(
        &mut self,
    ) -> ControlFlow<Poll<Option<Result<RecordBatch>>>> {
        // Return any completed batches first
        if let Some(poll) = self.maybe_flush_ready_batch() {
            return ControlFlow::Break(poll);
        }

        // Process current unmatched state
        match self.process_left_unmatched() {
            // State unchanged (EmitLeftUnmatched)
            // Continue processing until we have processed all unmatched rows
            Ok(true) => ControlFlow::Continue(()),
            // We have finished processing all unmatched rows for this chunk
            Ok(false) => match self.output_buffer.finish_buffered_batch() {
                Ok(()) => {
                    // Flush any completed batch before transitioning.
                    // This is critical for the memory-limited path: the
                    // ProbeRight results must be emitted before we discard
                    // the current chunk and load the next one.
                    if let Some(poll) = self.maybe_flush_ready_batch() {
                        return ControlFlow::Break(poll);
                    }

                    if !self.left_exhausted && self.spill_state.is_active() {
                        // More left data to process — free current chunk and
                        // go back to BufferingLeft for the next chunk
                        if let SpillState::Active(ref active) = self.spill_state {
                            active.reservation.resize(0);
                        }
                        self.buffered_left_data = None;
                        self.left_probe_idx = 0;
                        self.left_emit_idx = 0;
                        // Each memory-limited chunk gets a fresh per-chunk
                        // `JoinLeftData`/counter; `is_unmatched_left_emitter` is
                        // recomputed when `ProbeEnd` is re-entered for the next
                        // chunk, so it does not need to be reset here.
                        self.state = NLJState::BufferingLeft;
                    } else if self.spill_state.is_active()
                        && self.should_track_unmatched_right
                    {
                        // All left chunks done — emit global right unmatched.
                        // Drop the exhausted right stream so that
                        // EmitGlobalRightUnmatched opens a fresh replay pass
                        // from the spill file. (process_left_unmatched_range
                        // already ran with right_data still set, so its
                        // schema access is not affected.)
                        self.right_data = None;
                        self.state = NLJState::EmitGlobalRightUnmatched;
                    } else {
                        self.state = NLJState::Done;
                    }
                    ControlFlow::Continue(())
                }
                Err(e) => ControlFlow::Break(Poll::Ready(Some(arrow_err!(e)))),
            },
            Err(e) => ControlFlow::Break(Poll::Ready(Some(Err(e)))),
        }
    }

    /// Handle EmitGlobalRightUnmatched state.
    ///
    /// Replays all right batches from the spill file and emits unmatched
    /// right rows using the global bitmap accumulated across all left chunks.
    fn handle_emit_global_right_unmatched(
        &mut self,
        cx: &mut std::task::Context<'_>,
    ) -> ControlFlow<Poll<Option<Result<RecordBatch>>>> {
        // Flush any completed batches first
        if let Some(poll) = self.maybe_flush_ready_batch() {
            return ControlFlow::Break(poll);
        }

        // On first entry, open a new replay pass on the right input
        if self.right_data.is_none() {
            let SpillState::Active(ref mut active) = self.spill_state else {
                unreachable!("EmitGlobalRightUnmatched without Active spill state");
            };
            match active.open_right_pass() {
                Ok(stream) => {
                    self.right_data = Some(stream);
                }
                Err(e) => {
                    return ControlFlow::Break(Poll::Ready(Some(Err(e))));
                }
            }
        }

        // Poll the replay stream for the next right batch
        match self
            .right_data
            .as_mut()
            .expect("right_data must be present")
            .poll_next_unpin(cx)
        {
            Poll::Ready(Some(Ok(right_batch))) => {
                if right_batch.num_rows() == 0 {
                    return ControlFlow::Continue(());
                }

                let SpillState::Active(ref mut active) = self.spill_state else {
                    unreachable!();
                };
                match build_global_right_result_batch(
                    active,
                    &self.output_schema,
                    &right_batch,
                    &self.column_indices,
                    self.join_type,
                ) {
                    Ok(Some(batch)) => match self.output_buffer.push_batch(batch) {
                        Ok(()) => ControlFlow::Continue(()),
                        Err(e) => ControlFlow::Break(Poll::Ready(Some(arrow_err!(e)))),
                    },
                    Ok(None) => ControlFlow::Continue(()),
                    Err(e) => ControlFlow::Break(Poll::Ready(Some(Err(e)))),
                }
            }
            Poll::Ready(Some(Err(e))) => ControlFlow::Break(Poll::Ready(Some(Err(e)))),
            Poll::Ready(None) => {
                // All right batches replayed
                match self.output_buffer.finish_buffered_batch() {
                    Ok(()) => {
                        self.state = NLJState::Done;
                        ControlFlow::Continue(())
                    }
                    Err(e) => ControlFlow::Break(Poll::Ready(Some(arrow_err!(e)))),
                }
            }
            Poll::Pending => ControlFlow::Break(Poll::Pending),
        }
    }

    /// Handle Done state - final state processing
    fn handle_done(&mut self) -> Poll<Option<Result<RecordBatch>>> {
        // Return any remaining completed batches before final termination
        if let Some(poll) = self.maybe_flush_ready_batch() {
            return poll;
        }

        // HACK for the doc test in https://github.com/apache/datafusion/blob/main/datafusion/core/src/dataframe/mod.rs#L1265
        // If this operator directly return `Poll::Ready(None)`
        // for empty result, the final result will become an empty
        // batch with empty schema, however the expected result
        // should be with the expected schema for this operator
        if !self.handled_empty_output {
            let zero_count = Count::new();
            if *self.metrics.join_metrics.baseline.output_rows() == zero_count {
                let empty_batch = RecordBatch::new_empty(Arc::clone(&self.output_schema));
                self.handled_empty_output = true;
                return Poll::Ready(Some(Ok(empty_batch)));
            }
        }

        Poll::Ready(None)
    }

    // ==== Core logic handling for each state ====

    /// Returns bool to indicate should it continue probing
    /// true -> continue in the same ProbeRight state
    /// false -> It has done with the (buffered_left x cur_right_batch), go to
    /// next state (ProbeRight)
    fn process_probe_batch(&mut self) -> Result<bool> {
        let left_data = Arc::clone(self.get_left_data()?);
        let right_batch = self
            .current_right_batch
            .as_ref()
            .ok_or_else(|| internal_datafusion_err!("Right batch should be available"))?
            .clone();

        // stop probing, the caller will go to the next state
        if self.left_probe_idx >= left_data.batch().num_rows() {
            return Ok(false);
        }

        // ========
        // Join (l_row x right_batch)
        // and push the result into output_buffer
        // ========

        // Special case:
        // When the right batch is very small, join with multiple left rows at once,
        //
        // The regular implementation is not efficient if the plan's right child is
        // very small (e.g. 1 row total), because inside the inner loop of NLJ, it's
        // handling one input right batch at once, if it's not large enough, the
        // overheads like filter evaluation can't be amortized through vectorization.
        debug_assert_ne!(
            right_batch.num_rows(),
            0,
            "When fetching the right batch, empty batches will be skipped"
        );

        let l_row_cnt_ratio = self.batch_size / right_batch.num_rows();
        if l_row_cnt_ratio > 10 {
            // Calculate max left rows to handle at once. This operator tries to handle
            // up to `datafusion.execution.batch_size` rows at once in the intermediate
            // batch.
            let l_row_count = std::cmp::min(
                l_row_cnt_ratio,
                left_data.batch().num_rows() - self.left_probe_idx,
            );

            debug_assert!(
                l_row_count != 0,
                "This function should only be entered when there are remaining left rows to process"
            );
            let joined_batch = self.process_left_range_join(
                &left_data,
                &right_batch,
                self.left_probe_idx,
                l_row_count,
            )?;

            if let Some(batch) = joined_batch {
                self.output_buffer.push_batch(batch)?;
            }

            self.left_probe_idx += l_row_count;

            return Ok(true);
        }

        let l_idx = self.left_probe_idx;
        let joined_batch =
            self.process_single_left_row_join(&left_data, &right_batch, l_idx)?;

        if let Some(batch) = joined_batch {
            self.output_buffer.push_batch(batch)?;
        }

        // ==== Prepare for the next iteration ====

        // Advance left cursor
        self.left_probe_idx += 1;

        // Return true to continue probing
        Ok(true)
    }

    /// Process [l_start_index, l_start_index + l_count) JOIN right_batch
    /// Returns a RecordBatch containing the join results (None if empty)
    ///
    /// Side Effect: If the join type requires, left or right side matched bitmap
    /// will be set for matched indices.
    fn process_left_range_join(
        &mut self,
        left_data: &JoinLeftData,
        right_batch: &RecordBatch,
        l_start_index: usize,
        l_row_count: usize,
    ) -> Result<Option<RecordBatch>> {
        // Construct the Cartesian product between the specified range of left rows
        // and the entire right_batch. First, it calculates the index vectors, then
        // materializes the intermediate batch, and finally applies the join filter
        // to it.
        // -----------------------------------------------------------
        let right_rows = right_batch.num_rows();
        let total_rows = l_row_count * right_rows;

        // Build index arrays for cartesian product: left_range X right_batch
        let left_indices: UInt32Array =
            UInt32Array::from_iter_values((0..l_row_count).flat_map(|i| {
                std::iter::repeat_n((l_start_index + i) as u32, right_rows)
            }));
        let right_indices: UInt32Array = UInt32Array::from_iter_values(
            (0..l_row_count).flat_map(|_| 0..right_rows as u32),
        );

        debug_assert!(
            left_indices.len() == right_indices.len()
                && right_indices.len() == total_rows,
            "The length or cartesian product should be (left_size * right_size)",
        );

        // Evaluate the join filter (if any) over an intermediate batch built
        // using the filter's own schema/column indices.
        let bitmap_combined = if let Some(filter) = &self.join_filter {
            // Build the intermediate batch for filter evaluation
            let intermediate_batch = if filter.schema.fields().is_empty() {
                // Constant predicate (e.g., TRUE/FALSE). Use an empty schema with row_count
                create_record_batch_with_empty_schema(
                    Arc::new((*filter.schema).clone()),
                    total_rows,
                )?
            } else {
                let mut filter_columns: Vec<Arc<dyn Array>> =
                    Vec::with_capacity(filter.column_indices().len());
                for column_index in filter.column_indices() {
                    let array = if column_index.side == JoinSide::Left {
                        let col = left_data.batch().column(column_index.index);
                        take(col.as_ref(), &left_indices, None)?
                    } else {
                        let col = right_batch.column(column_index.index);
                        take(col.as_ref(), &right_indices, None)?
                    };
                    filter_columns.push(array);
                }

                RecordBatch::try_new(Arc::new((*filter.schema).clone()), filter_columns)?
            };

            let filter_result = filter
                .expression()
                .evaluate(&intermediate_batch)?
                .into_array(intermediate_batch.num_rows())?;
            let filter_arr = as_boolean_array(&filter_result)?;

            // Combine with null bitmap to get a unified mask
            boolean_mask_from_filter(filter_arr)
        } else {
            // No filter: all pairs match
            BooleanArray::from(vec![true; total_rows])
        };

        // Update the global left or right bitmap for matched indices
        // -----------------------------------------------------------

        // None means we don't have to update left bitmap for this join type
        let mut left_bitmap = if need_produce_result_in_final(self.join_type) {
            Some(left_data.bitmap().lock())
        } else {
            None
        };

        // 'local' meaning: we want to collect 'is_matched' flag for the current
        // right batch, after it has joining all of the left buffer, here it's only
        // the partial result for joining given left range
        let mut local_right_bitmap = if self.should_track_unmatched_right {
            let mut current_right_batch_bitmap = BooleanBufferBuilder::new(right_rows);
            // Ensure builder has logical length so set_bit is in-bounds
            current_right_batch_bitmap.append_n(right_rows, false);
            Some(current_right_batch_bitmap)
        } else {
            None
        };

        // Set the matched bit for left and right side bitmap
        for (i, is_matched) in bitmap_combined.iter().enumerate() {
            let is_matched = is_matched.ok_or_else(|| {
                internal_datafusion_err!("Must be Some after the previous combining step")
            })?;

            let l_index = l_start_index + i / right_rows;
            let r_index = i % right_rows;

            if let Some(bitmap) = left_bitmap.as_mut()
                && is_matched
            {
                // Map local index back to absolute left index within the batch
                bitmap.set_bit(l_index, true);
            }

            if let Some(bitmap) = local_right_bitmap.as_mut()
                && is_matched
            {
                bitmap.set_bit(r_index, true);
            }
        }

        // Apply the local right bitmap to the global bitmap
        if self.should_track_unmatched_right {
            // Remember to put it back after update
            let global_right_bitmap =
                std::mem::take(&mut self.current_right_batch_matched).ok_or_else(
                    || internal_datafusion_err!("right batch's bitmap should be present"),
                )?;
            let (buf, nulls) = global_right_bitmap.into_parts();
            debug_assert!(nulls.is_none());

            let current_right_bitmap = local_right_bitmap
                .ok_or_else(|| {
                    internal_datafusion_err!(
                        "Should be Some if the current join type requires right bitmap"
                    )
                })?
                .finish();
            let updated_global_right_bitmap = buf.bitor(&current_right_bitmap);

            self.current_right_batch_matched =
                Some(BooleanArray::new(updated_global_right_bitmap, None));
        }

        // For the following join types: only bitmaps are updated; do not emit rows now
        if matches!(
            self.join_type,
            JoinType::LeftAnti
                | JoinType::LeftSemi
                | JoinType::LeftMark
                | JoinType::RightAnti
                | JoinType::RightMark
                | JoinType::RightSemi
        ) {
            return Ok(None);
        }

        // Build the projected output batch (using output schema/column_indices),
        // then apply the bitmap filter to it.
        if self.output_schema.fields().is_empty() {
            // Empty projection: only row count matters
            let row_count = bitmap_combined.true_count();
            return Ok(Some(create_record_batch_with_empty_schema(
                Arc::clone(&self.output_schema),
                row_count,
            )?));
        }

        let mut out_columns: Vec<Arc<dyn Array>> =
            Vec::with_capacity(self.output_schema.fields().len());
        for column_index in &self.column_indices {
            let array = if column_index.side == JoinSide::Left {
                let col = left_data.batch().column(column_index.index);
                take(col.as_ref(), &left_indices, None)?
            } else {
                let col = right_batch.column(column_index.index);
                take(col.as_ref(), &right_indices, None)?
            };
            out_columns.push(array);
        }
        let pre_filtered =
            RecordBatch::try_new(Arc::clone(&self.output_schema), out_columns)?;
        let filtered = filter_record_batch(&pre_filtered, &bitmap_combined)?;
        Ok(Some(filtered))
    }

    /// Process a single left row join with the current right batch.
    /// Returns a RecordBatch containing the join results (None if empty)
    ///
    /// Side Effect: If the join type requires, left or right side matched bitmap
    /// will be set for matched indices.
    fn process_single_left_row_join(
        &mut self,
        left_data: &JoinLeftData,
        right_batch: &RecordBatch,
        l_index: usize,
    ) -> Result<Option<RecordBatch>> {
        let right_row_count = right_batch.num_rows();
        if right_row_count == 0 {
            return Ok(None);
        }

        let cur_right_bitmap = if let Some(filter) = &self.join_filter {
            apply_filter_to_row_join_batch(
                left_data.batch(),
                l_index,
                right_batch,
                filter,
            )?
        } else {
            BooleanArray::from(vec![true; right_row_count])
        };

        self.update_matched_bitmap(l_index, &cur_right_bitmap)?;

        // For the following join types: here we only have to set the left/right
        // bitmap, and no need to output result
        if matches!(
            self.join_type,
            JoinType::LeftAnti
                | JoinType::LeftSemi
                | JoinType::LeftMark
                | JoinType::RightAnti
                | JoinType::RightMark
                | JoinType::RightSemi
        ) {
            return Ok(None);
        }

        if !cur_right_bitmap.has_true() {
            // If none of the pairs has passed the join predicate/filter
            Ok(None)
        } else {
            // Use the optimized approach similar to build_intermediate_batch_for_single_left_row
            let join_batch = build_row_join_batch(
                &self.output_schema,
                left_data.batch(),
                l_index,
                right_batch,
                Some(cur_right_bitmap),
                &self.column_indices,
                JoinSide::Left,
            )?;
            Ok(join_batch)
        }
    }

    /// Returns bool to indicate should it continue processing unmatched rows
    /// true -> continue in the same EmitLeftUnmatched state
    /// false -> next state (Done)
    fn process_left_unmatched(&mut self) -> Result<bool> {
        let left_data = self.get_left_data()?;
        let left_batch = left_data.batch();

        // ========
        // Check early return conditions
        // ========

        // Early return if join type can't have unmatched rows
        let join_type_no_produce_left = !need_produce_result_in_final(self.join_type);
        // Stop processing unmatched rows, the caller will go to the next state
        let finished = self.left_emit_idx >= left_batch.num_rows();

        // `ProbeEnd` already recorded whether this stream emits unmatched-left
        // rows. Every probe partition passes through this state, but only the
        // one that finished probing last is the emitter, so this flag is false
        // for the others.
        if join_type_no_produce_left || !self.is_unmatched_left_emitter || finished {
            return Ok(false);
        }

        // ========
        // Process unmatched rows and push the result into output_buffer
        // Each time, the number to process is up to batch size
        // ========
        let start_idx = self.left_emit_idx;
        let end_idx = std::cmp::min(start_idx + self.batch_size, left_batch.num_rows());

        if let Some(batch) =
            self.process_left_unmatched_range(left_data, start_idx, end_idx)?
        {
            self.output_buffer.push_batch(batch)?;
        }

        // ==== Prepare for the next iteration ====
        self.left_emit_idx = end_idx;

        // Return true to continue processing unmatched rows
        Ok(true)
    }

    /// Process unmatched rows from the left data within the specified range.
    /// Returns a RecordBatch containing the unmatched rows (None if empty).
    ///
    /// # Arguments
    /// * `left_data` - The left side data containing the batch and bitmap
    /// * `start_idx` - Start index (inclusive) of the range to process
    /// * `end_idx` - End index (exclusive) of the range to process
    ///
    /// # Safety
    /// The caller is responsible for ensuring that `start_idx` and `end_idx` are
    /// within valid bounds of the left batch. This function does not perform
    /// bounds checking.
    fn process_left_unmatched_range(
        &self,
        left_data: &JoinLeftData,
        start_idx: usize,
        end_idx: usize,
    ) -> Result<Option<RecordBatch>> {
        if start_idx == end_idx {
            return Ok(None);
        }

        // Slice both left batch, and bitmap to range [start_idx, end_idx)
        // The range is bit index (not byte)
        let (left_batch_sliced, bitmap_sliced) =
            left_data.slice_batch_and_bitmap(start_idx, end_idx);

        let right_schema = self
            .right_data
            .as_ref()
            .expect("right_data must be present when building unmatched batch")
            .schema();
        build_unmatched_batch(
            &self.output_schema,
            &left_batch_sliced,
            bitmap_sliced,
            &right_schema,
            &self.column_indices,
            self.join_type,
            JoinSide::Left,
        )
    }

    /// Process unmatched rows from the current right batch and reset the bitmap.
    /// Returns a RecordBatch containing the unmatched right rows (None if empty).
    fn process_right_unmatched(&mut self) -> Result<Option<RecordBatch>> {
        // ==== Take current right batch and its bitmap ====
        let right_batch_bitmap: BooleanArray =
            std::mem::take(&mut self.current_right_batch_matched).ok_or_else(|| {
                internal_datafusion_err!("right bitmap should be available")
            })?;

        let right_batch = self.current_right_batch.take();
        let cur_right_batch = unwrap_or_internal_err!(right_batch);

        let left_data = self.get_left_data()?;
        let left_schema = left_data.batch().schema();

        let res = build_unmatched_batch(
            &self.output_schema,
            &cur_right_batch,
            right_batch_bitmap,
            &left_schema,
            &self.column_indices,
            self.join_type,
            JoinSide::Right,
        );

        // ==== Clean-up ====
        self.current_right_batch_matched = None;

        res
    }

    // ==== Utilities ====

    /// Get the build-side data of the left input, errors if it's None
    fn get_left_data(&self) -> Result<&Arc<JoinLeftData>> {
        self.buffered_left_data
            .as_ref()
            .ok_or_else(|| internal_datafusion_err!("LeftData should be available"))
    }

    /// Flush the `output_buffer` if there are batches ready to output
    /// None if no result batch ready.
    fn maybe_flush_ready_batch(&mut self) -> Option<Poll<Option<Result<RecordBatch>>>> {
        if self.output_buffer.has_completed_batch()
            && let Some(batch) = self.output_buffer.next_completed_batch()
        {
            // Update output rows for selectivity metric
            let output_rows = batch.num_rows();
            self.metrics.selectivity.add_part(output_rows);

            return Some(Poll::Ready(Some(Ok(batch))));
        }

        None
    }

    /// After joining (l_index@left_buffer x current_right_batch), it will result
    /// in a bitmap (the same length as current_right_batch) as the join match
    /// result. Use this bitmap to update the global bitmap, for special join
    /// types like full joins.
    ///
    /// Example:
    /// After joining l_index=1 (1-indexed row in the left buffer), and the
    /// current right batch with 3 elements, this function will be called with
    /// arguments: l_index = 1, r_matched = [false, false, true]
    /// - If the join type is FullJoin, the 1-index in the left bitmap will be
    ///   set to true, and also the right bitmap will be bitwise-ORed with the
    ///   input r_matched bitmap.
    /// - For join types that don't require output unmatched rows, this
    ///   function can be a no-op. For inner joins, this function is a no-op; for left
    ///   joins, only the left bitmap may be updated.
    fn update_matched_bitmap(
        &mut self,
        l_index: usize,
        r_matched_bitmap: &BooleanArray,
    ) -> Result<()> {
        let left_data = self.get_left_data()?;

        // 1. Maybe update the left bitmap
        if need_produce_result_in_final(self.join_type) && r_matched_bitmap.has_true() {
            let mut bitmap = left_data.bitmap().lock();
            bitmap.set_bit(l_index, true);
        }

        // 2. Maybe update the right bitmap
        if self.should_track_unmatched_right {
            debug_assert!(self.current_right_batch_matched.is_some());
            // after bit-wise or, it will be put back
            let right_bitmap = std::mem::take(&mut self.current_right_batch_matched)
                .ok_or_else(|| {
                    internal_datafusion_err!("right batch's bitmap should be present")
                })?;
            let (buf, nulls) = right_bitmap.into_parts();
            debug_assert!(nulls.is_none());
            let updated_right_bitmap = buf.bitor(r_matched_bitmap.values());

            self.current_right_batch_matched =
                Some(BooleanArray::new(updated_right_bitmap, None));
        }

        Ok(())
    }
}
