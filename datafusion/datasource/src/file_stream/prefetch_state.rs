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

use std::collections::VecDeque;
use std::task::{Context, Poll};

use crate::morsel::{Morsel, MorselPlanner, Morselizer, PendingMorselPlanner};
use arrow::record_batch::RecordBatch;
use datafusion_common::Result;
use datafusion_common::instant::Instant;
use futures::stream::BoxStream;
use futures::{FutureExt as _, StreamExt as _};

use super::scan_state::ScanAndReturn;
use super::work_source::WorkSource;
use super::{FileStreamMetrics, OnError};

/// Maximum number of morsel-producing work items that may be prefetched ahead
/// of the active reader.
///
/// This caps the total amount of in-flight morsel-producing work across:
/// pending planner I/O, CPU-ready planners, decoded-but-unread morsels, and
/// the active reader. Once the cap is reached no new files are morselized
/// until work drains.
pub(super) const MAX_PREFETCH_MORSELS: usize = 20;

/// State [`FileStreamState::Prefetch`].
///
/// Like [`ScanState`], but allows many concurrent planner I/O operations to
/// prefetch up to [`MAX_PREFETCH_MORSELS`] morsels ahead of the reader.
///
/// # I/O
///
/// Unlike [`ScanState`], which keeps at most one planner I/O outstanding,
/// `PrefetchState` may have multiple pending planners in flight at once
/// (bounded by [`MAX_PREFETCH_MORSELS`]). A new file is morselized whenever
/// the active reader is idle or blocked and the prefetch cap allows it, so
/// I/O for upcoming files overlaps with CPU decoding of the current file.
///
/// # State Transitions
///
/// ```text
///          work_source
///             |
///             v
/// morselizer.plan_file(file)
///             |
///             v
///        ready_planners ----> plan() ----> ready_morsels --> into_stream() --> reader --> RecordBatches
///             ^                  |
///             |                  v
///             |            pending_planners (many concurrent)
///             |                  |
///             +------- poll until each resolves
/// ```
///
/// [`ScanState`]: super::scan_state::ScanState
/// [`FileStreamState::Prefetch`]: super::FileStreamState::Prefetch
pub(super) struct PrefetchState {
    /// Unopened files that still need to be planned for this stream.
    work_source: WorkSource,
    /// Remaining row limit, if any.
    remain: Option<usize>,
    /// The morselizer used to plan files.
    morselizer: Box<dyn Morselizer>,
    /// Behavior if opening or scanning a file fails.
    on_error: OnError,
    /// CPU-ready planners awaiting CPU planning.
    ready_planners: VecDeque<Box<dyn MorselPlanner>>,
    /// Prefetched morsels waiting for the reader.
    ready_morsels: VecDeque<Box<dyn Morsel>>,
    /// The active reader, if any.
    reader: Option<BoxStream<'static, Result<RecordBatch>>>,
    /// Planner I/O in flight. Multiple planners may be pending concurrently.
    pending_planners: Vec<PendingMorselPlanner>,
    /// Metrics for the active scan queues.
    metrics: FileStreamMetrics,
}

impl PrefetchState {
    pub(super) fn new(
        work_source: WorkSource,
        remain: Option<usize>,
        morselizer: Box<dyn Morselizer>,
        on_error: OnError,
        metrics: FileStreamMetrics,
    ) -> Self {
        Self {
            work_source,
            remain,
            morselizer,
            on_error,
            ready_planners: VecDeque::new(),
            ready_morsels: VecDeque::new(),
            reader: None,
            pending_planners: Vec::new(),
            metrics,
        }
    }

    /// Updates how scan errors are handled while the stream is still active.
    pub(super) fn set_on_error(&mut self, on_error: OnError) {
        self.on_error = on_error;
    }

    /// Total in-flight morsel-producing work counted against
    /// [`MAX_PREFETCH_MORSELS`].
    fn in_flight(&self) -> usize {
        self.pending_planners.len()
            + self.ready_planners.len()
            + self.ready_morsels.len()
            + usize::from(self.reader.is_some())
    }

    /// True if we have room to morselize another file.
    fn has_prefetch_capacity(&self) -> bool {
        self.in_flight() < MAX_PREFETCH_MORSELS
    }

    /// Drives one iteration of the active prefetch state.
    ///
    /// Work is attempted in this order:
    /// 1. poll all pending planner I/O concurrently
    /// 2. poll the active reader (return a batch if available)
    /// 3. run CPU planning on a ready planner
    /// 4. promote a ready morsel into the active reader
    /// 5. morselize the next unopened file (if under the prefetch cap)
    ///
    /// The reader takes priority over prefetching so user-visible latency is
    /// not delayed by opening new files. Prefetching kicks in exactly when
    /// the reader has no batch ready, which is the point at which overlapping
    /// I/O for the next file with decoding of the current file is most
    /// beneficial.
    pub(super) fn poll_scan(&mut self, cx: &mut Context<'_>) -> ScanAndReturn {
        let processing_start = Instant::now();
        let action = self.poll_scan_inner(cx);
        self.metrics.time_processing.add_elapsed(processing_start);
        action
    }

    fn poll_scan_inner(&mut self, cx: &mut Context<'_>) -> ScanAndReturn {
        // 1) Drive all in-flight I/O forward concurrently.
        if let Some(action) = self.poll_pending_planners(cx) {
            return action;
        }

        // 2) Return a batch from the active reader, if any.
        if let Some(action) = self.poll_reader(cx) {
            return action;
        }

        // 3) Run CPU planning on a ready planner if any.
        if let Some(planner) = self.ready_planners.pop_front() {
            return self.run_plan(planner);
        }

        // 4) Promote a ready morsel to the active reader.
        if let Some(morsel) = self.ready_morsels.pop_front() {
            if self.metrics.time_opening.start.is_some() {
                self.metrics.time_opening.stop();
            }
            self.metrics.time_scanning_until_data.start();
            self.metrics.time_scanning_total.start();
            self.reader = Some(morsel.into_stream());
            return ScanAndReturn::Continue;
        }

        // 5) Morselize the next unopened file if the prefetch cap allows.
        if self.has_prefetch_capacity() {
            match self.work_source.pop_front() {
                Some(part_file) => return self.morselize(part_file),
                None => {
                    // No more files and nothing in flight → done.
                    if self.in_flight() == 0 {
                        return ScanAndReturn::Done(None);
                    }
                }
            }
        }

        // Outstanding work remains (pending I/O) but there is nothing to do
        // this iteration. Yield Pending so the wakers registered on the
        // pending planners can resume us.
        ScanAndReturn::Return(Poll::Pending)
    }

    /// Poll every in-flight planner I/O once, moving any resolved planners to
    /// `ready_planners`. Returns a terminal action if an I/O error surfaces
    /// under [`OnError::Fail`].
    fn poll_pending_planners(&mut self, cx: &mut Context<'_>) -> Option<ScanAndReturn> {
        let mut i = 0;
        while i < self.pending_planners.len() {
            match self.pending_planners[i].poll_unpin(cx) {
                Poll::Pending => {
                    i += 1;
                }
                Poll::Ready(Ok(planner)) => {
                    self.pending_planners.swap_remove(i);
                    self.ready_planners.push_back(planner);
                    // swap_remove put the last element at i, so re-check it.
                }
                Poll::Ready(Err(err)) => {
                    self.pending_planners.swap_remove(i);
                    self.metrics.file_open_errors.add(1);
                    if self.in_flight() == 0 {
                        self.metrics.time_opening.stop();
                    }
                    return Some(match self.on_error {
                        OnError::Skip => {
                            self.metrics.files_processed.add(1);
                            ScanAndReturn::Continue
                        }
                        OnError::Fail => ScanAndReturn::Error(err),
                    });
                }
            }
        }
        None
    }

    /// Drive the active reader. Returns a terminal action whenever the reader
    /// yields a batch, finishes, or errors.
    fn poll_reader(&mut self, cx: &mut Context<'_>) -> Option<ScanAndReturn> {
        let reader = self.reader.as_mut()?;
        match reader.poll_next_unpin(cx) {
            // Morsels should ideally only expose ready-to-decode streams,
            // but tolerate pending readers here by returning Pending so the
            // outer loop can re-check on the next wakeup.
            Poll::Pending => Some(ScanAndReturn::Return(Poll::Pending)),
            Poll::Ready(Some(Ok(batch))) => {
                self.metrics.time_scanning_until_data.stop();
                self.metrics.time_scanning_total.stop();
                // Apply any remaining row limit.
                let (batch, finished) = match &mut self.remain {
                    Some(remain) => {
                        if *remain > batch.num_rows() {
                            *remain -= batch.num_rows();
                            self.metrics.time_scanning_total.start();
                            (batch, false)
                        } else {
                            let batch = batch.slice(0, *remain);
                            // Count the active reader plus anything queued
                            // or still in flight as "processed" for metrics.
                            let done = 1
                                + self.work_source.len()
                                + self.ready_morsels.len()
                                + self.ready_planners.len()
                                + self.pending_planners.len();
                            self.metrics.files_processed.add(done);
                            *remain = 0;
                            (batch, true)
                        }
                    }
                    None => {
                        self.metrics.time_scanning_total.start();
                        (batch, false)
                    }
                };
                Some(if finished {
                    ScanAndReturn::Done(Some(Ok(batch)))
                } else {
                    ScanAndReturn::Return(Poll::Ready(Some(Ok(batch))))
                })
            }
            Poll::Ready(Some(Err(err))) => {
                self.reader = None;
                self.metrics.file_scan_errors.add(1);
                self.metrics.time_scanning_until_data.stop();
                self.metrics.time_scanning_total.stop();
                Some(match self.on_error {
                    OnError::Skip => {
                        self.metrics.files_processed.add(1);
                        ScanAndReturn::Continue
                    }
                    OnError::Fail => ScanAndReturn::Error(err),
                })
            }
            Poll::Ready(None) => {
                self.reader = None;
                self.metrics.files_processed.add(1);
                self.metrics.time_scanning_until_data.stop();
                self.metrics.time_scanning_total.stop();
                Some(ScanAndReturn::Continue)
            }
        }
    }

    /// Run CPU planning on one ready planner and queue the resulting morsels,
    /// child planners and optional pending I/O.
    fn run_plan(&mut self, planner: Box<dyn MorselPlanner>) -> ScanAndReturn {
        match planner.plan() {
            Ok(Some(mut plan)) => {
                self.ready_morsels.extend(plan.take_morsels());
                self.ready_planners.extend(plan.take_ready_planners());
                if let Some(pending_planner) = plan.take_pending_planner() {
                    self.pending_planners.push(pending_planner);
                }
                ScanAndReturn::Continue
            }
            Ok(None) => {
                self.metrics.files_processed.add(1);
                if self.in_flight() == 0 {
                    self.metrics.time_opening.stop();
                }
                ScanAndReturn::Continue
            }
            Err(err) => {
                self.metrics.file_open_errors.add(1);
                if self.in_flight() == 0 {
                    self.metrics.time_opening.stop();
                }
                match self.on_error {
                    OnError::Skip => {
                        self.metrics.files_processed.add(1);
                        ScanAndReturn::Continue
                    }
                    OnError::Fail => ScanAndReturn::Error(err),
                }
            }
        }
    }

    /// Morselize one file, emitting the initial planner or handling open
    /// errors according to [`OnError`].
    fn morselize(&mut self, part_file: crate::PartitionedFile) -> ScanAndReturn {
        if self.metrics.time_opening.start.is_none() {
            self.metrics.time_opening.start();
        }
        match self.morselizer.plan_file(part_file) {
            Ok(planner) => {
                self.metrics.files_opened.add(1);
                self.ready_planners.push_back(planner);
                ScanAndReturn::Continue
            }
            Err(err) => match self.on_error {
                OnError::Skip => {
                    self.metrics.file_open_errors.add(1);
                    self.metrics.files_processed.add(1);
                    if self.in_flight() == 0 {
                        self.metrics.time_opening.stop();
                    }
                    ScanAndReturn::Continue
                }
                OnError::Fail => ScanAndReturn::Error(err),
            },
        }
    }
}
