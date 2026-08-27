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
//!
//! # Algorithm
//!
//! For each buffered left chunk:
//! ```text
//! for right_batch in right_side:
//!     for left_row in left_chunk:          // bitmap-only probing
//!         update left/right match bitmaps
//!     emit/accumulate right-side SAM result
//! report_probe_completed() once
//! emit left-side SAM result (emitter partition only)
//! ```
//!
//! In memory-limited mode, left chunks are loaded incrementally and right-side
//! match bitmaps are accumulated globally; after all chunks, the right input
//! is replayed from spill for final right-side emission.

use std::future::poll_fn;
use std::sync::Arc;

use super::shared::{
    JoinLeftData, LeftBufferBatchDecision, NestedLoopJoinMetrics, SpillState,
    apply_filter_to_row_join_batch, buffer_left_batch_in_chunk,
    build_global_right_result_batch, build_unmatched_batch, finalize_buffered_left_chunk,
    initiate_spill_fallback, probe_sam_left_range, update_sam_matched_bitmaps,
};
use crate::SendableRecordBatchStream;
use crate::joins::utils::{ColumnIndex, JoinFilter, OnceFut};
use crate::stream::{ObservedStream, RecordBatchStreamAdapter};

use arrow::array::BooleanArray;
use arrow::buffer::BooleanBuffer;
use arrow::compute::BatchCoalescer;
use arrow::datatypes::{Schema, SchemaRef};
use arrow::record_batch::RecordBatch;
use datafusion_common::instant::Instant;
use datafusion_common::{DataFusionError, JoinSide, Result, internal_datafusion_err};
use datafusion_execution::{TryEmitter, async_try_stream};
use datafusion_expr::JoinType;
use futures::StreamExt;
use log::debug;

/// Nested loop join stream for Semi/Anti/Mark joins.
///
/// Evaluates the join predicate for every relevant left/right combination but
/// does not emit `(left, right)` pairs. Instead it accumulates a Boolean value
/// for each row on the output side to check for any match.
pub(super) struct SemiAntiMarkNestedLoopJoinStream {
    /// Output schema after applying the join projection.
    output_schema: Arc<Schema>,
    /// Optional non-equality join predicate.
    join_filter: Option<JoinFilter>,
    /// Semi, anti, or mark join type handled by this stream.
    join_type: JoinType,
    /// Side whose rows are produced by the join.
    join_side: JoinSide,
    /// Current probe-side input. Replaced by each replay pass after spilling.
    right_data: Option<SendableRecordBatchStream>,
    /// Shared future that collects the build side for the standard path.
    left_data: OnceFut<JoinLeftData>,
    /// Projection used to construct output columns from the input sides.
    column_indices: Vec<ColumnIndex>,
    /// Join, spill, and selectivity metrics.
    metrics: NestedLoopJoinMetrics,
    /// Target output batch size and probe range size.
    batch_size: usize,
    /// Coalesces result batches before yielding them to the consumer.
    output_buffer: Box<BatchCoalescer>,
    /// Disabled, pending, or active memory-limited spill execution.
    spill_state: SpillState,
    /// Start of the current join-time interval; `None` while paused.
    join_time_start: Option<Instant>,
    /// Number of right-side passes opened for buffered left chunks.
    right_pass_count: usize,
    /// Whether this stream has emitted at least one output row.
    emitted_rows: bool,
}

impl SemiAntiMarkNestedLoopJoinStream {
    /// Create the SAM stream and wrap its generator with baseline observation.
    #[expect(clippy::too_many_arguments)]
    pub(super) fn try_new(
        schema: SchemaRef,
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
                    | JoinType::LeftAnti
                    | JoinType::RightSemi
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

        let baseline_metrics = metrics.join_metrics.baseline.clone();

        let mut state = Self {
            output_schema: Arc::clone(&schema),
            join_filter: filter,
            join_type,
            join_side,
            right_data: Some(right_data),
            column_indices,
            left_data,
            metrics,
            output_buffer: Box::new(BatchCoalescer::new(Arc::clone(&schema), batch_size)),
            batch_size,
            spill_state,
            join_time_start: None,
            right_pass_count: 0,
            emitted_rows: false,
        };

        let stream = async_try_stream(|mut emitter| async move {
            state.start_join_time();
            let result = state.join(&mut emitter).await;
            state.stop_join_time();
            result
        });
        Ok(Box::pin(ObservedStream::new(
            Box::pin(RecordBatchStreamAdapter::new(schema, stream)),
            baseline_metrics,
            None,
        )))
    }

    /// Resume measuring time spent by this join itself
    fn start_join_time(&mut self) {
        debug_assert!(self.join_time_start.is_none(), "join_time already running");
        self.join_time_start = Some(Instant::now());
    }

    /// Pause join-time measurement and record the completed interval
    fn stop_join_time(&mut self) {
        if let Some(start) = self.join_time_start.take() {
            self.metrics.join_metrics.join_time.add_elapsed(start);
        }
    }

    /// Run the join as nested loops over left chunks and right batches
    async fn join(
        &mut self,
        emitter: &mut TryEmitter<RecordBatch, DataFusionError>,
    ) -> Result<()> {
        let track_right_matches = self.join_side == JoinSide::Right;

        // Outer loop: fetching current the left chunk
        loop {
            let Some((left_chunk, is_last_chunk)) = self.buffer_left_chunk().await?
            else {
                break;
            };

            // Inner loop: scan all right batches against the current left chunk
            loop {
                let Some(right_batch) = self.fetch_next_right_batch().await? else {
                    break;
                };

                let right_batch_matched =
                    self.probe_right_batch(&left_chunk, &right_batch)?;

                if track_right_matches {
                    self.emit_or_accumulate_right_result(
                        &left_chunk,
                        right_batch,
                        right_batch_matched,
                        emitter,
                    )
                    .await?;
                }
            }

            // Right exhausted for this chunk: decrement the shared probe counter once
            let is_left_result_emitter = left_chunk.report_probe_completed();

            if !track_right_matches && is_left_result_emitter {
                self.emit_left_result(&left_chunk, emitter).await?;
            }

            // Flush before releasing the chunk
            self.output_buffer.finish_buffered_batch()?;
            self.drain_output_coalescer(emitter).await?;

            if !is_last_chunk && self.spill_state.is_active() {
                if let SpillState::Active(ref active) = self.spill_state {
                    active.reservation.resize(0);
                }
                continue;
            }
            break;
        }

        // Replay spilled right input using globally accumulated bitmaps
        if self.spill_state.is_active() && track_right_matches {
            self.emit_global_right_result(emitter).await?;
        }

        self.finish_output(emitter).await
    }

    /// Probe every left row against one right batch, updating match bitmaps only.
    fn probe_right_batch(
        &mut self,
        left_chunk: &JoinLeftData,
        right_batch: &RecordBatch,
    ) -> Result<Option<BooleanArray>> {
        let right_rows = right_batch.num_rows();
        debug_assert_ne!(
            right_rows, 0,
            "Empty right batches are skipped when fetching"
        );

        let track_left_matches = self.join_side == JoinSide::Left;
        let mut right_batch_matched = (self.join_side == JoinSide::Right)
            .then(|| BooleanArray::new(BooleanBuffer::new_unset(right_rows), None));

        let left_rows = left_chunk.batch().num_rows();
        let mut left_probe_idx = 0;
        while left_probe_idx < left_rows {
            let left_rows_per_range = self.batch_size / right_rows;
            if left_rows_per_range > 10 {
                let left_row_count =
                    std::cmp::min(left_rows_per_range, left_rows - left_probe_idx);
                probe_sam_left_range(
                    left_chunk,
                    right_batch,
                    left_probe_idx,
                    left_row_count,
                    self.join_filter.as_ref(),
                    track_left_matches,
                    &mut right_batch_matched,
                )?;
                left_probe_idx += left_row_count;
            } else {
                let row_filter = if let Some(filter) = &self.join_filter {
                    apply_filter_to_row_join_batch(
                        left_chunk.batch(),
                        left_probe_idx,
                        right_batch,
                        filter,
                    )?
                } else {
                    BooleanArray::from(vec![true; right_rows])
                };
                update_sam_matched_bitmaps(
                    left_chunk,
                    left_probe_idx,
                    &row_filter,
                    track_left_matches,
                    &mut right_batch_matched,
                )?;
                left_probe_idx += 1;
            }
        }

        self.metrics.selectivity.add_total(left_rows * right_rows);
        Ok(right_batch_matched)
    }

    /// Emit all left-side SAM results for the current chunk
    async fn emit_left_result(
        &mut self,
        left_chunk: &JoinLeftData,
        emitter: &mut TryEmitter<RecordBatch, DataFusionError>,
    ) -> Result<()> {
        let left_batch = left_chunk.batch();
        let right_schema = self
            .right_data
            .as_ref()
            .expect("right_data must be present when building left-side result")
            .schema();

        let mut start_idx = 0;
        while start_idx < left_batch.num_rows() {
            let end_idx =
                std::cmp::min(start_idx + self.batch_size, left_batch.num_rows());
            let (left_batch_sliced, bitmap_sliced) =
                left_chunk.slice_batch_and_bitmap(start_idx, end_idx);

            if let Some(batch) = build_unmatched_batch(
                &self.output_schema,
                &left_batch_sliced,
                bitmap_sliced,
                &right_schema,
                &self.column_indices,
                self.join_type,
                JoinSide::Left,
            )? {
                self.output_buffer.push_batch(batch)?;
            }

            start_idx = end_idx;
            self.drain_output_coalescer(emitter).await?;
        }
        Ok(())
    }

    /// Replay the right input and emit results from global spill bitmaps.
    ///
    /// Each bitmap contains the OR of matches found across every left chunk.
    /// Empty batches must be skipped to keep replay batch indices aligned with
    /// the stored bitmap indices.
    async fn emit_global_right_result(
        &mut self,
        emitter: &mut TryEmitter<RecordBatch, DataFusionError>,
    ) -> Result<()> {
        let SpillState::Active(ref mut active) = self.spill_state else {
            unreachable!("global right replay without Active spill state");
        };
        self.right_data = Some(active.open_right_pass()?);

        while let Some(item) = self
            .right_data
            .as_mut()
            .expect("right_data must be present")
            .next()
            .await
        {
            let right_batch = item?;
            if right_batch.num_rows() == 0 {
                continue;
            }

            let SpillState::Active(ref mut active) = self.spill_state else {
                unreachable!();
            };
            if let Some(batch) = build_global_right_result_batch(
                active,
                &self.output_schema,
                &right_batch,
                &self.column_indices,
                self.join_type,
            )? {
                self.output_buffer.push_batch(batch)?;
            }
            self.drain_output_coalescer(emitter).await?;
        }

        self.output_buffer.finish_buffered_batch()?;
        self.drain_output_coalescer(emitter).await
    }

    /// Flush final output and preserve the schema when the result is empty.
    ///
    /// `ObservedStream` records yielded rows, but downstream consumers still
    /// require one zero-row batch carrying this operator's output schema.
    async fn finish_output(
        &mut self,
        emitter: &mut TryEmitter<RecordBatch, DataFusionError>,
    ) -> Result<()> {
        self.output_buffer.finish_buffered_batch()?;
        self.drain_output_coalescer(emitter).await?;

        if !self.emitted_rows {
            // HACK for the doc test in https://github.com/apache/datafusion/blob/main/datafusion/core/src/dataframe/mod.rs#L1265
            self.stop_join_time();
            emitter
                .emit(RecordBatch::new_empty(Arc::clone(&self.output_schema)))
                .await;
            self.start_join_time();
        }
        Ok(())
    }

    /// Buffer the next left chunk.
    /// Returns two-tuple representing the left data (`Ok(None)` when no data remaining) & boolean that's truthy if this is the last chunk
    async fn buffer_left_chunk(&mut self) -> Result<Option<(Arc<JoinLeftData>, bool)>> {
        loop {
            if self.spill_state.is_active() {
                return self.buffer_left_chunk_memory_limited().await;
            }

            self.stop_join_time();
            let build_time = self.metrics.join_metrics.build_time.clone();
            let left_data_result = poll_fn(|cx| {
                let _build_timer = build_time.timer();
                self.left_data.get_shared(cx)
            })
            .await;

            match left_data_result {
                Ok(left_data) => {
                    self.start_join_time();
                    return Ok(Some((left_data, true)));
                }
                Err(e) => {
                    if self.spill_state.can_fallback_to_spill(&e) {
                        debug!(
                            "NestedLoopJoin: OnceFut failed with OOM, \
                             falling back to memory-limited mode"
                        );
                        let _build_timer = build_time.timer();
                        initiate_spill_fallback(
                            &mut self.spill_state,
                            &self.metrics,
                            &mut self.right_data,
                        )?;
                    } else {
                        return Err(e);
                    }
                }
            }
        }
    }

    /// Load one left chunk from spill within the current memory reservation.
    ///
    /// The first call opens the shared left spill file. Batches are then read
    /// until the reservation is full or the stream is exhausted. Finalization
    /// concatenates the buffered batches, creates the left match bitmap when
    /// needed, and opens the corresponding right-side pass
    async fn buffer_left_chunk_memory_limited(
        &mut self,
    ) -> Result<Option<(Arc<JoinLeftData>, bool)>> {
        let need_left_stream = matches!(
            &self.spill_state,
            SpillState::Active(active) if active.left_stream.is_none()
        );

        if need_left_stream {
            // Every partition waits on the same spill future; only the first
            // poll executes and spills the left child.
            self.stop_join_time();
            let build_time = self.metrics.join_metrics.build_time.clone();
            let spill_data = {
                let SpillState::Active(active) = &mut self.spill_state else {
                    unreachable!(
                        "buffer_left_chunk_memory_limited called without Active spill state"
                    );
                };
                poll_fn(|cx| {
                    let _build_timer = build_time.timer();
                    active.left_spill_fut.get_shared(cx)
                })
                .await?
            };

            let _build_timer = build_time.timer();
            let SpillState::Active(active) = &mut self.spill_state else {
                unreachable!(
                    "buffer_left_chunk_memory_limited called without Active spill state"
                );
            };
            active.set_left_spill_data(&spill_data)?;
        }

        let is_last_chunk;
        // Keep reading until the next batch would exceed the chunk reservation
        // or until the shared left spill stream is exhausted.
        loop {
            self.stop_join_time();
            let build_time = self.metrics.join_metrics.build_time.clone();
            let next_item = poll_fn(|cx| {
                let _build_timer = build_time.timer();
                let SpillState::Active(active) = &mut self.spill_state else {
                    unreachable!(
                        "buffer_left_chunk_memory_limited called without Active spill state"
                    );
                };
                active
                    .left_stream
                    .as_mut()
                    .expect("left_stream must be set after spill future resolves")
                    .poll_next_unpin(cx)
            })
            .await;
            let _build_timer = build_time.timer();

            match next_item {
                Some(Ok(batch)) => {
                    if batch.num_rows() == 0 {
                        continue;
                    }
                    let SpillState::Active(active) = &mut self.spill_state else {
                        unreachable!();
                    };
                    match buffer_left_batch_in_chunk(
                        &mut active.reservation,
                        &mut active.pending_batches,
                        &self.metrics.join_metrics,
                        batch,
                    ) {
                        LeftBufferBatchDecision::Continue => {}
                        LeftBufferBatchDecision::ChunkFull => {
                            // The batch that reached the limit remains pending,
                            // so this chunk still makes forward progress.
                            is_last_chunk = false;
                            break;
                        }
                    }
                }
                Some(Err(e)) => return Err(e),
                None => {
                    is_last_chunk = true;
                    break;
                }
            }
        }

        let build_time = self.metrics.join_metrics.build_time.clone();
        let build_timer = build_time.timer();
        let SpillState::Active(active) = &mut self.spill_state else {
            unreachable!();
        };

        if is_last_chunk {
            // Release the exhausted stream before probing so upstream spill
            // resources are not retained through result production.
            active.left_stream = None;
        }

        if active.pending_batches.is_empty() {
            drop(build_timer);
            self.start_join_time();
            return Ok(None);
        }

        // Concatenate this chunk, allocate its match bitmap if this is a
        // left-side SAM join, and open the matching right-side pass
        let finalized = finalize_buffered_left_chunk(
            active,
            &self.metrics.join_metrics,
            self.join_side == JoinSide::Left,
        )?;
        self.right_pass_count += 1;
        self.right_data = Some(finalized.right_pass);

        drop(build_timer);
        self.start_join_time();
        Ok(Some((Arc::new(finalized.left_data), is_last_chunk)))
    }

    /// Fetch the next non-empty right batch for the current pass
    async fn fetch_next_right_batch(&mut self) -> Result<Option<RecordBatch>> {
        loop {
            let await_child_input = self.right_pass_count <= 1;
            if await_child_input {
                self.stop_join_time();
            }
            let item = self
                .right_data
                .as_mut()
                .expect("right_data must be present while fetching right")
                .next()
                .await;
            if await_child_input {
                self.start_join_time();
            }

            match item {
                Some(Ok(right_batch)) => {
                    // Preserve input metrics for every pass, matching the
                    // materializing NLJ implementation.
                    let right_batch_rows = right_batch.num_rows();
                    self.metrics.join_metrics.input_rows.add(right_batch_rows);
                    self.metrics.join_metrics.input_batches.add(1);
                    if right_batch_rows == 0 {
                        continue;
                    }
                    return Ok(Some(right_batch));
                }
                Some(Err(e)) => return Err(e),
                None => return Ok(None),
            }
        }
    }

    /// Emit a right-side SAM result or accumulate it for spill replay.
    ///
    /// Standard execution can emit as soon as the batch has seen every left
    /// row. Memory-limited execution ORs the bitmap into the global accumulator
    /// because later left chunks may add more matches.
    async fn emit_or_accumulate_right_result(
        &mut self,
        left_chunk: &JoinLeftData,
        right_batch: RecordBatch,
        right_batch_matched: Option<BooleanArray>,
        emitter: &mut TryEmitter<RecordBatch, DataFusionError>,
    ) -> Result<()> {
        if self.spill_state.is_active() {
            debug_assert!(
                right_batch_matched.is_some(),
                "right bitmap must be present"
            );
            let bitmap = right_batch_matched.ok_or_else(|| {
                internal_datafusion_err!("right bitmap should be available")
            })?;
            if let SpillState::Active(ref mut active) = self.spill_state {
                active.handoff_completed_right_bitmap(bitmap);
            }
            return Ok(());
        }

        self.drain_output_coalescer(emitter).await?;

        let right_batch_bitmap = right_batch_matched.ok_or_else(|| {
            internal_datafusion_err!("right bitmap should be available")
        })?;
        let left_schema = left_chunk.batch().schema();

        if let Some(batch) = build_unmatched_batch(
            &self.output_schema,
            &right_batch,
            right_batch_bitmap,
            &left_schema,
            &self.column_indices,
            self.join_type,
            JoinSide::Right,
        )? {
            self.output_buffer.push_batch(batch)?;
        }

        self.drain_output_coalescer(emitter).await?;
        Ok(())
    }

    /// Yield every completed coalesced batch and update output metrics
    async fn drain_output_coalescer(
        &mut self,
        emitter: &mut TryEmitter<RecordBatch, DataFusionError>,
    ) -> Result<()> {
        while let Some(batch) = self.output_buffer.next_completed_batch() {
            let output_rows = batch.num_rows();
            if output_rows > 0 {
                self.emitted_rows = true;
            }
            self.metrics.selectivity.add_part(output_rows);
            self.stop_join_time();
            emitter.emit(batch).await;
            self.start_join_time();
        }
        Ok(())
    }
}
