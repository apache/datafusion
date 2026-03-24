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

//! A generic stream over file format readers that can be used by
//! any file format that read its files from start to end.
//!
//! Note: Most traits here need to be marked `Sync + Send` to be
//! compliant with the `SendableRecordBatchStream` trait.

pub mod shared_state;
mod trace;

pub use shared_state::{
    FileStreamId, OutstandingIoPermit, SharedFileStreamMode, SharedFileStreamState,
};

use std::collections::VecDeque;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use crate::PartitionedFile;
use crate::file_scan_config::FileScanConfig;
use arrow::datatypes::SchemaRef;
use datafusion_common::{Result, internal_datafusion_err};
use datafusion_execution::RecordBatchStream;
use datafusion_physical_plan::metrics::{
    BaselineMetrics, Count, ExecutionPlanMetricsSet, MetricBuilder, Time,
};

use arrow::record_batch::RecordBatch;
use datafusion_common::instant::Instant;

use crate::morsel::{FileOpenerMorselizer, Morsel, MorselPlanner, Morselizer};
use datafusion_common_runtime::SpawnedTask;
use futures::future::BoxFuture;
use futures::stream::BoxStream;
use futures::{FutureExt, Stream, StreamExt as _};
use trace::{ReadTrace, file_stream_trace_enabled};

const DEFAULT_OUTSTANDING_IOS_PER_PARTITION: usize = 2;

/// Keep at most this many morsels buffered before pausing additional planning.
///
/// TODO make this a config option
fn max_buffered_morsels() -> usize {
    2
}

/// Resolve the shared outstanding-I/O budget for one `DataSourceExec`.
///
/// This is temporary wiring until the datasource layer constructs and passes
/// the shared state directly into each sibling `FileStream`.
fn target_datasource_outstanding_ios(num_partitions: usize) -> usize {
    std::env::var("DATAFUSION_DATASOURCE_OUTSTANDING_IOS")
        .ok()
        .and_then(|value| value.parse::<usize>().ok())
        .filter(|value| *value > 0)
        .unwrap_or_else(|| {
            DEFAULT_OUTSTANDING_IOS_PER_PARTITION * std::cmp::max(num_partitions, 1)
        })
}

/// Build a default shared state object for streams created directly from a
/// `FileScanConfig`.
pub(crate) fn shared_file_stream_state_for(
    config: &FileScanConfig,
) -> SharedFileStreamState {
    SharedFileStreamState::new(
        target_datasource_outstanding_ios(config.file_groups.len()),
        if config.preserve_order {
            SharedFileStreamMode::PreserveOrder
        } else {
            SharedFileStreamMode::Unordered
        },
    )
}

/// A stream that iterates record batch by record batch, file over file.
///
/// When running, a FileStream has some number of waiting planners (that are
/// waiting on IO) and some number of read_planners that are waiting on CPU.
///
/// When the next batch is requested, the FileStream will first poll any
/// outstanding io_requests to ensure I/O is making progress in parallel with
/// batch processing.
///
/// It then tries to prioritize processing data it has in its cache by read from
/// the active stream, if any. If that is not ready, it will use the CPU to
/// prepare more morsels or discover new IO before launching the next morsel.
///
/// Sibling `FileStream`s created for the same `DataSourceExec` may also share a
/// [`SharedFileStreamState`]. That shared state coordinates resources such as
/// the total number of outstanding planner I/O operations across all sibling
/// streams. Each `FileStream` registers itself with the shared state during
/// construction (via `with_shared_state`) and must acquire a shared permit
/// before advancing a ready planner toward its next I/O phase. If the planner
/// actually issues an `io_future`, the permit remains attached to that waiting
/// planner until the future resolves.
///
/// Future feature:
///  Other FileStreams may steal morsels from this stream to increase parallelism and resource utilization.
pub struct FileStream {
    /// Local file/planner/morsel queues owned by this stream.
    queues: MorselQueue,
    /// The current reader, if any
    reader: Option<BoxStream<'static, Result<RecordBatch>>>,
    /// The stream schema (file schema including partition columns and after
    /// projection).
    projected_schema: SchemaRef,
    /// The remaining number of records to parse until limit is reached, None if no limit
    remain: Option<usize>,
    /// A type specific [`Morselizer`] that examines the input files and produces a stream of `Morsels`
    morselizer: Box<dyn Morselizer>,
    /// File stream specific metrics
    file_stream_metrics: FileStreamMetrics,
    /// runtime baseline metrics
    baseline_metrics: BaselineMetrics,
    /// Describes the behavior of the `FileStream` if file opening or scanning fails
    on_error: OnError,
    /// Preserve the logical planner/morsel order defined by the
    /// [`MorselPlan`] API?
    ///
    /// If false (the default) morsels will be produced in the order
    /// that they are ready to be run.
    ///
    /// If true, Morsels will be produced in the logical order defined on
    /// [`MorselPlan`]
    ///
    /// [`MorselPlan`]: crate::morsel::MorselPlan
    preserve_order: bool,
    /// Preserve output partition boundaries.
    ///
    /// If false (the default), morsels may be run by a sibling `FileStream`
    ///
    /// If true, morsels will be produced by the same stream that created
    /// planned them.
    preserve_partitions: bool,
    /// Shared scheduling state across all sibling `FileStream`s for the same
    /// `DataSourceExec`.
    ///
    /// This shared state enforces the cross-stream outstanding-I/O budget and
    /// wakes blocked sibling streams when capacity becomes available again.
    shared_file_stream_state: SharedFileStreamState,
    /// This stream's identity within `shared_file_stream_state`.
    ///
    /// The id is assigned when the stream registers itself with the shared
    /// state and is cleared once the stream unregisters after reaching a
    /// terminal state.
    stream_id: Option<FileStreamId>,
    /// Optional runtime trace for observing scheduler behavior.
    trace: ReadTrace,
    /// Is the stream complete?
    state: StreamState,
}

enum StreamState {
    /// Stream can make progress when polled
    Active,
    /// Stream is done
    Done,
    /// Stream is done, and errord
    Error,
}

/// Queues for a file/planner/morsel
///
/// The flow is:
/// 1. Read each file  and turn it into morsels (potentially in parallel)
/// 2. Read each morsel individually and produce `RecordBatch`es for processing
#[derive(Debug)]
pub(super) struct MorselQueue {
    /// Input files that have not yet been morselized.
    file_iter: VecDeque<PartitionedFile>,
    /// Planners that are currently waiting on an outstanding I/O phase.
    waiting_planners: VecDeque<WaitingPlanner>,
    /// Planners that are CPU-ready and may be advanced by calling `plan()`.
    ready_planners: VecDeque<Box<dyn MorselPlanner>>,
    /// Morsels that are ready to be scanned into `RecordBatch`es.
    morsels: VecDeque<Box<dyn Morsel>>,
}

impl MorselQueue {
    /// Create an empty queue set for one file group.
    pub(super) fn new(file_iter: VecDeque<PartitionedFile>) -> Self {
        Self {
            file_iter,
            waiting_planners: VecDeque::new(),
            ready_planners: VecDeque::new(),
            morsels: VecDeque::new(),
        }
    }

    /// Clear all planner and morsel work currently owned by this stream.
    pub(super) fn clear(&mut self) {
        self.waiting_planners.clear();
        self.ready_planners.clear();
        self.morsels.clear();
    }

    /// Return true if the stream has no remaining queued file or morsel work.
    pub(super) fn is_empty(&self) -> bool {
        self.file_iter.is_empty()
            && self.waiting_planners.is_empty()
            && self.ready_planners.is_empty()
            && self.morsels.is_empty()
    }

    /// Return the number of queued ready morsels.
    pub(super) fn morsel_len(&self) -> usize {
        self.morsels.len()
    }

    /// Return true if there is at least one queued ready morsel.
    pub(super) fn has_morsels(&self) -> bool {
        !self.morsels.is_empty()
    }

    /// Return true if there is at least one queued ready planner.
    pub(super) fn has_ready_planners(&self) -> bool {
        !self.ready_planners.is_empty()
    }

    /// Return true if there is at least one queued waiting planner.
    pub(super) fn has_waiting_planners(&self) -> bool {
        !self.waiting_planners.is_empty()
    }

    /// Return the total number of queued ready and waiting planners.
    pub(super) fn planner_count(&self) -> usize {
        self.waiting_planners.len() + self.ready_planners.len()
    }

    /// Push one CPU-ready planner into the local queue.
    pub(super) fn push_ready_planner(&mut self, planner: Box<dyn MorselPlanner>) {
        self.ready_planners.push_back(planner);
    }

    /// Extend the local queue with CPU-ready planners.
    pub(super) fn extend_ready_planners(
        &mut self,
        planners: impl IntoIterator<Item = Box<dyn MorselPlanner>>,
    ) {
        self.ready_planners.extend(planners);
    }

    /// Pop the next CPU-ready planner from the local queue.
    pub(super) fn pop_ready_planner(&mut self) -> Option<Box<dyn MorselPlanner>> {
        self.ready_planners.pop_front()
    }

    /// Push one waiting planner into the local queue.
    fn push_waiting_planner(&mut self, planner: WaitingPlanner) {
        self.waiting_planners.push_back(planner);
    }

    /// Drain all waiting planners from the queue.
    fn take_waiting_planners(&mut self) -> VecDeque<WaitingPlanner> {
        std::mem::take(&mut self.waiting_planners)
    }

    /// Push one ready morsel into the local queue.
    pub(super) fn push_morsel(&mut self, morsel: Box<dyn Morsel>) {
        self.morsels.push_back(morsel);
    }

    /// Extend the local queue with ready morsels.
    pub(super) fn extend_morsels(
        &mut self,
        morsels: impl IntoIterator<Item = Box<dyn Morsel>>,
    ) {
        self.morsels.extend(morsels);
    }

    /// Pop the next ready morsel from the local queue.
    pub(super) fn pop_morsel(&mut self) -> Option<Box<dyn Morsel>> {
        self.morsels.pop_front()
    }

    /// Pop the next input file from the local queue.
    pub(super) fn pop_file(&mut self) -> Option<PartitionedFile> {
        self.file_iter.pop_front()
    }
}

/// Builder for constructing a [`FileStream`].
pub struct FileStreamBuilder<'a> {
    config: &'a FileScanConfig,
    partition: usize,
    morselizer: Box<dyn Morselizer>,
    metrics: &'a ExecutionPlanMetricsSet,
    on_error: OnError,
    preserve_order: bool,
    preserve_partitions: bool,
    shared_file_stream_state: Option<SharedFileStreamState>,
}

impl<'a> FileStreamBuilder<'a> {
    /// Create a new builder using a legacy [`FileOpener`].
    pub fn new(
        config: &'a FileScanConfig,
        partition: usize,
        file_opener: Arc<dyn FileOpener>,
        metrics: &'a ExecutionPlanMetricsSet,
    ) -> Self {
        Self::new_with_morselizer(
            config,
            partition,
            Box::new(FileOpenerMorselizer::new(file_opener)),
            metrics,
        )
    }

    /// Create a new builder using a [`Morselizer`].
    pub fn new_with_morselizer(
        config: &'a FileScanConfig,
        partition: usize,
        morselizer: Box<dyn Morselizer>,
        metrics: &'a ExecutionPlanMetricsSet,
    ) -> Self {
        Self {
            config,
            partition,
            morselizer,
            on_error: OnError::Fail,
            preserve_order: config.preserve_order,
            preserve_partitions: config.partitioned_by_file_group,
            metrics,
            shared_file_stream_state: None,
        }
    }

    /// Configure the behavior when opening or scanning a file fails.
    pub fn with_on_error(mut self, on_error: OnError) -> Self {
        self.on_error = on_error;
        self
    }

    /// Configure whether this stream should preserve logical planner order.
    pub fn with_preserve_order(mut self, preserve_order: bool) -> Self {
        self.preserve_order = preserve_order;
        self
    }

    /// Use the provided shared scheduler state instead of the default one.
    pub fn with_shared_state(
        mut self,
        shared_file_stream_state: SharedFileStreamState,
    ) -> Self {
        self.shared_file_stream_state = Some(shared_file_stream_state);
        self
    }

    /// Build the configured [`FileStream`].
    pub fn build(self) -> Result<FileStream> {
        let shared_file_stream_state = self
            .shared_file_stream_state
            .unwrap_or_else(|| shared_file_stream_state_for(self.config));
        let projected_schema = self.config.projected_schema()?;
        let file_group = self.config.file_groups[self.partition].clone();
        let stream_id = shared_file_stream_state.register_stream();
        let trace =
            ReadTrace::new(file_stream_trace_enabled(), self.partition, stream_id);

        Ok(FileStream {
            queues: MorselQueue::new(file_group.into_inner().into_iter().collect()),
            reader: None,
            projected_schema,
            remain: self.config.limit,
            morselizer: self.morselizer,
            file_stream_metrics: FileStreamMetrics::new(self.metrics, self.partition),
            baseline_metrics: BaselineMetrics::new(self.metrics, self.partition),
            on_error: self.on_error,
            preserve_order: self.preserve_order,
            preserve_partitions: self.preserve_partitions,
            shared_file_stream_state,
            stream_id: Some(stream_id),
            trace,
            state: StreamState::Active,
        })
    }
}

impl FileStream {
    /// Return true if this stream may publish and steal ready work from the
    /// shared queue.
    ///
    /// The shared queue permits output to be reordered, so only do this when
    /// stream does not need to preserve ordering within the stream or across
    /// partitions.
    fn can_share_ready_work(&self) -> bool {
        !self.preserve_order
            && !self.preserve_partitions
            && self.shared_file_stream_state.registered_stream_count() > 1
    }

    /// Enqueue ready planners either locally or into the shared queue.
    fn push_ready_planners(
        &mut self,
        planners: impl IntoIterator<Item = Box<dyn MorselPlanner>>,
    ) {
        let planners: Vec<_> = planners.into_iter().collect();
        if planners.is_empty() {
            return;
        }
        if self.can_share_ready_work() {
            self.trace.planners_ready(planners.len(), true);
            for planner in planners {
                self.shared_file_stream_state.push_ready_planner(planner);
            }
        } else {
            self.trace.planners_ready(planners.len(), false);
            self.queues.extend_ready_planners(planners);
        }
    }

    /// Enqueue ready morsels either locally or into the shared queue.
    fn push_ready_morsels(&mut self, morsels: impl IntoIterator<Item = Box<dyn Morsel>>) {
        let morsels: Vec<_> = morsels.into_iter().collect();
        if morsels.is_empty() {
            return;
        }
        if self.can_share_ready_work() {
            self.trace.morsels_ready(morsels.len(), true);
            for morsel in morsels {
                self.shared_file_stream_state.push_ready_morsel(morsel);
            }
        } else {
            self.trace.morsels_ready(morsels.len(), false);
            self.queues.extend_morsels(morsels);
        }
    }

    /// Try to steal one ready morsel or planner from the shared queue and place
    /// them in the local queue.
    ///
    /// Morsels are preferred because they are already fully prepared CPU work.
    fn try_steal_ready_work(&mut self) -> bool {
        if !self.can_share_ready_work()
            || self.reader.is_some()
            || self.queues.has_morsels()
            || self.queues.has_ready_planners()
        {
            return false;
        }

        if let Some(morsel) = self.shared_file_stream_state.pop_ready_morsel() {
            self.queues.push_morsel(morsel);
            self.trace.stole_work("morsel");
            return true;
        }

        if let Some(planner) = self.shared_file_stream_state.pop_ready_planner() {
            self.queues.push_ready_planner(planner);
            self.trace.stole_work("planner");
            return true;
        }

        false
    }

    /// Create a new [`FileStream`] using a legacy [`FileOpener`].
    ///
    /// Prefer [`FileStreamBuilder`] for new code.
    #[deprecated(since = "52.3.0", note = "use FileStreamBuilder instead")]
    pub fn new(
        config: &FileScanConfig,
        partition: usize,
        file_opener: Arc<dyn FileOpener>,
        metrics: &ExecutionPlanMetricsSet,
    ) -> Result<Self> {
        FileStreamBuilder::new(config, partition, file_opener, metrics).build()
    }

    /// Return this stream's registered shared-state id.
    fn stream_id(&self) -> Result<FileStreamId> {
        self.stream_id.ok_or_else(|| {
            internal_datafusion_err!("file stream is not registered with shared state")
        })
    }

    /// Unregister this stream from the shared scheduler once it reaches a
    /// terminal state.
    fn unregister_stream_if_needed(&mut self) {
        if let Some(stream_id) = self.stream_id.take() {
            self.shared_file_stream_state.unregister_stream(stream_id);
        }
    }

    /// Run a planner on CPU until it either needs I/O or fully completes.
    ///
    /// Any morsels produced along the way are appended to `self.morsels`. If
    /// the planner needs more I/O, it is moved to `waiting_planners`.
    fn plan_morsels(
        &mut self,
        mut planner: Box<dyn MorselPlanner>,
        io_permit: OutstandingIoPermit,
    ) -> Result<()> {
        let max_buffered_morsels = max_buffered_morsels();
        let mut io_permit = Some(io_permit);
        while let Some(mut plan) = planner.plan()? {
            let morsels = plan.take_morsels();
            let planners = plan.take_planners();
            let io_future = plan.take_io_future();
            self.trace
                .plan_result(morsels.len(), planners.len(), io_future.is_some());
            self.push_ready_morsels(morsels);
            self.push_ready_planners(planners);
            if let Some(io_future) = io_future {
                self.queues.push_waiting_planner(WaitingPlanner::new(
                    planner,
                    io_future,
                    io_permit
                        .take()
                        .expect("planner I/O permit should be available"),
                ));
                self.trace.io_scheduled(self.queues.waiting_planners.len());
                break;
            }

            if self.queues.morsel_len() >= max_buffered_morsels {
                self.push_ready_planners(std::iter::once(planner));
                break;
            }
        }
        Ok(())
    }

    /// Turn one file into one or more planners and immediately drive each of
    /// them into the ready queue.
    ///
    /// The actual `plan()` calls happen in `poll_inner` once the stream has
    /// acquired a shared permit to potentially issue another outstanding I/O.
    fn morselize_next_file(&mut self, file: PartitionedFile) -> Result<()> {
        self.trace.file_opened(&file);
        for planner in self.morselizer.morselize(file)? {
            self.push_ready_planners(std::iter::once(planner));
        }
        Ok(())
    }

    /// Pull additional files into the planner pipeline until the configured
    /// planner concurrency target is reached.
    ///
    /// This is where new file-level work enters the stream. Formats that do all
    /// of their planning synchronously may immediately populate `self.morsels`,
    /// while formats that need metadata I/O will populate `waiting_planners`.
    fn start_next_files(&mut self) -> Result<()> {
        let max_buffered_morsels = max_buffered_morsels();
        // Keep local file admission bounded per stream. The shared state
        // controls the total outstanding I/O budget across sibling streams,
        // but using that global budget as a per-stream admission target causes
        // each stream to eagerly admit far too many files and planners.
        let local_planner_target = DEFAULT_OUTSTANDING_IOS_PER_PARTITION.max(1);
        while self.queues.planner_count() < local_planner_target {
            // In ordered mode, do not admit later files while there is any
            // earlier file work still buffered, waiting on I/O, or actively
            // being scanned. This keeps file-level planning from introducing
            // later output ahead of earlier files.
            if self.preserve_order
                && (self.reader.is_some()
                    || self.queues.has_morsels()
                    || self.queues.has_ready_planners()
                    || self.queues.has_waiting_planners())
            {
                break;
            }
            if self.queues.morsel_len() >= max_buffered_morsels {
                break;
            }
            let Some(file) = self.queues.pop_file() else {
                break;
            };
            self.morselize_next_file(file)?;
        }
        Ok(())
    }

    /// Poll each waiting planner's outstanding I/O once.
    ///
    /// When a future completes successfully, the planner becomes CPU-ready
    /// again and is moved back to `ready_planners`. Failed futures are handled
    /// according to `OnError`.
    fn check_io(&mut self, cx: &mut Context<'_>) -> Result<()> {
        for mut waiting_planner in self.queues.take_waiting_planners() {
            match waiting_planner.io_task.poll_unpin(cx) {
                Poll::Ready(Ok(Ok(()))) => {
                    self.file_stream_metrics.files_opened.add(1);
                    self.push_ready_planners(std::iter::once(waiting_planner.planner));
                    self.trace.io_completed(self.queues.planner_count());
                }
                Poll::Ready(Ok(Err(e))) => {
                    self.file_stream_metrics.file_open_errors.add(1);
                    match self.on_error {
                        OnError::Skip => {
                            self.file_stream_metrics.files_processed.add(1);
                        }
                        OnError::Fail => return Err(e),
                    }
                }
                Poll::Ready(Err(join_err)) => {
                    self.file_stream_metrics.file_open_errors.add(1);
                    let e =
                        datafusion_common::DataFusionError::External(Box::new(join_err));
                    match self.on_error {
                        OnError::Skip => {
                            self.file_stream_metrics.files_processed.add(1);
                        }
                        OnError::Fail => return Err(e),
                    }
                }
                Poll::Pending => self.queues.push_waiting_planner(waiting_planner),
            }
        }
        Ok(())
    }

    /// Convert the next ready morsel into an active `RecordBatch` reader.
    ///
    /// This only happens when there is no reader currently in flight. The
    /// corresponding scan timers start here because the morsel is now eligible
    /// to produce batches.
    fn start_next_morsel(&mut self) {
        if self.reader.is_none()
            && let Some(morsel) = self.queues.pop_morsel()
        {
            self.reader = Some(morsel.into_stream());
            self.trace.morsel_started(self.queues.morsel_len());
            self.file_stream_metrics.time_scanning_until_data.start();
            self.file_stream_metrics.time_scanning_total.start();
        }
    }

    /// Drive the `FileStream` scheduler forward by one poll.
    ///
    /// The order is important:
    /// 1. Admit more files into the planner pipeline up to the concurrency
    ///    target (ensures I/O are scheduled if needed)
    /// 2. Poll outstanding planner I/O (ensure I/O completes in parallel)
    /// 3. Spend CPU on ready planners only when there is no morsel already ready
    ///    to execute.
    /// 4. Launch and poll the active morsel reader.
    fn poll_inner(&mut self, cx: &mut Context<'_>) -> Poll<Option<Result<RecordBatch>>> {
        loop {
            match self.state {
                StreamState::Active => {}
                StreamState::Done => {
                    self.unregister_stream_if_needed();
                    return Poll::Ready(None);
                }
                StreamState::Error => {
                    self.unregister_stream_if_needed();
                    return Poll::Ready(None);
                }
            }

            if let Err(e) = self.start_next_files() {
                self.queues.clear();
                self.state = StreamState::Error;
                self.unregister_stream_if_needed();
                return Poll::Ready(Some(Err(e)));
            }
            if let Err(e) = self.check_io(cx) {
                self.queues.clear();
                self.state = StreamState::Error;
                self.unregister_stream_if_needed();
                return Poll::Ready(Some(Err(e)));
            }

            // Opportunistically refill the local queues from shared ready work
            // before spending more CPU locally. We intentionally ignore the
            // return value here because this is only a best-effort steal: the
            // normal scheduler flow below will observe any newly queued work.
            let _ = self.try_steal_ready_work();

            // Give ready planners CPU whenever there is buffer space, even if a
            // reader is currently active. This avoids starving planner work
            // behind a reader that is itself waiting on I/O.
            while self.queues.morsel_len() < max_buffered_morsels() {
                // In ordered mode, once an earlier planner has produced a
                // morsel or is blocked on I/O, do not advance later sibling
                // planners yet. This preserves the logical `MorselPlan` order:
                // direct morsels first, then child planners in API order.
                if self.preserve_order
                    && (self.reader.is_some()
                        || self.queues.has_morsels()
                        || self.queues.has_waiting_planners())
                {
                    break;
                }
                let stream_id = match self.stream_id() {
                    Ok(stream_id) => stream_id,
                    Err(e) => {
                        self.queues.clear();
                        self.state = StreamState::Error;
                        self.unregister_stream_if_needed();
                        return Poll::Ready(Some(Err(e)));
                    }
                };
                let Some(io_permit) = self
                    .shared_file_stream_state
                    .try_acquire_io_permit(stream_id)
                else {
                    self.shared_file_stream_state
                        .register_waker(stream_id, cx.waker());
                    break;
                };
                let Some(planner) = self.queues.pop_ready_planner() else {
                    drop(io_permit);
                    break;
                };
                if let Err(e) = self.plan_morsels(planner, io_permit) {
                    self.queues.clear();
                    self.state = StreamState::Error;
                    self.unregister_stream_if_needed();
                    return Poll::Ready(Some(Err(e)));
                }

                // Once a morsel is buffered and a reader is already active,
                // return to the scan side of the scheduler rather than
                // continuing to spend CPU on planning in this poll.
                if self.reader.is_some() && self.queues.has_morsels() {
                    break;
                }
            }

            // Newly planned work may have just discovered fresh I/O. Poll it
            // once now so the future can register the current waker before we
            // return `Pending`; otherwise the stream can stall waiting on an
            // I/O future that has never been polled.
            if let Err(e) = self.check_io(cx) {
                self.queues.clear();
                self.state = StreamState::Error;
                self.unregister_stream_if_needed();
                return Poll::Ready(Some(Err(e)));
            }

            // After polling I/O, see if a sibling published newly ready work.
            // The boolean result is ignored because this is only an
            // opportunistic prefetch into the local queues; the subsequent
            // checks for local planners/morsels will handle any stolen work.
            let _ = self.try_steal_ready_work();

            // The second I/O poll may have completed planner work discovered
            // during this same call to `poll_inner`. Loop back so newly ready
            // planners get CPU time before we consider returning `Pending`.
            if self.queues.has_ready_planners()
                && self.queues.morsel_len() < max_buffered_morsels()
                // In ordered mode, only loop back for more planner CPU when
                // there is no earlier reader, buffered morsel, or waiting I/O
                // that should be drained first. Otherwise, drop to
                // `start_next_morsel()` so output is produced in order.
                && (!self.preserve_order
                    || (self.reader.is_none()
                        && !self.queues.has_morsels()
                        && !self.queues.has_waiting_planners()))
            {
                continue;
            }

            self.start_next_morsel();

            if let Some(reader) = self.reader.as_mut() {
                match reader.poll_next_unpin(cx) {
                    Poll::Ready(Some(Ok(batch))) => {
                        self.file_stream_metrics.time_scanning_until_data.stop();
                        self.file_stream_metrics.time_scanning_total.stop();
                        let batch = match &mut self.remain {
                            Some(remain) => {
                                if batch.num_rows() > *remain {
                                    let batch = batch.slice(0, *remain);
                                    *remain = 0;
                                    self.state = StreamState::Done;
                                    batch
                                } else {
                                    *remain -= batch.num_rows();
                                    batch
                                }
                            }
                            None => batch,
                        };
                        self.file_stream_metrics.time_scanning_total.start();
                        self.trace.batch_emitted(batch.num_rows());
                        return Poll::Ready(Some(Ok(batch)));
                    }
                    Poll::Ready(Some(Err(e))) => {
                        self.reader = None;
                        self.file_stream_metrics.file_scan_errors.add(1);
                        self.file_stream_metrics.time_scanning_until_data.stop();
                        self.file_stream_metrics.time_scanning_total.stop();

                        match self.on_error {
                            OnError::Fail => {
                                self.queues.clear();
                                self.state = StreamState::Error;
                                self.unregister_stream_if_needed();
                                return Poll::Ready(Some(Err(e)));
                            }
                            OnError::Skip => {
                                self.file_stream_metrics.files_processed.add(1);
                                continue;
                            }
                        }
                    }
                    Poll::Ready(None) => {
                        self.reader = None;
                        self.file_stream_metrics.files_processed.add(1);
                        self.file_stream_metrics.time_scanning_until_data.stop();
                        self.file_stream_metrics.time_scanning_total.stop();
                        continue;
                    }
                    Poll::Pending => {}
                }
            }

            if self.reader.is_none() && self.queues.is_empty() {
                self.state = StreamState::Done;
                self.unregister_stream_if_needed();
                return Poll::Ready(None);
            }

            // try and find more work if possible, but if not,wait on the waker
            if !self.try_steal_ready_work()
                && self.can_share_ready_work()
                && self.reader.is_none()
                && !self.queues.has_morsels()
                && !self.queues.has_ready_planners()
            {
                self.trace.waiting("shared_work_or_io");
                let stream_id = match self.stream_id() {
                    Ok(stream_id) => stream_id,
                    Err(e) => {
                        self.queues.clear();
                        self.state = StreamState::Error;
                        self.unregister_stream_if_needed();
                        return Poll::Ready(Some(Err(e)));
                    }
                };
                self.shared_file_stream_state
                    .register_waker(stream_id, cx.waker());
            // If the active reader just returned `Pending`, yield back to the
            // executor instead of looping immediately. Otherwise a reader that
            // needs more I/O can hot-loop inside `poll_inner` as long as there
            // is buffered work behind it, repeatedly polling the same pending
            // reader without giving the executor a chance to wake it.
            } else if self.reader.is_none()
                && (self.queues.has_morsels() || self.queues.has_ready_planners())
            {
                continue;
            }

            self.trace.waiting("reader_or_io");
            return Poll::Pending;
        }
    }
}

/// A planner that has already discovered its next I/O phase.
///
/// The I/O future is spawned onto the tokio runtime so it progresses
/// independently of when this `FileStream` is polled, enabling true
/// parallel prefetch of row-group data.
struct WaitingPlanner {
    planner: Box<dyn MorselPlanner>,
    io_task: SpawnedTask<Result<()>>,
    _io_permit: OutstandingIoPermit,
}

impl WaitingPlanner {
    fn new(
        planner: Box<dyn MorselPlanner>,
        io_future: BoxFuture<'static, Result<()>>,
        io_permit: OutstandingIoPermit,
    ) -> Self {
        let io_task = SpawnedTask::spawn(io_future);
        Self {
            planner,
            io_task,
            _io_permit: io_permit,
        }
    }
}

impl std::fmt::Debug for WaitingPlanner {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("WaitingPlanner").finish_non_exhaustive()
    }
}

impl Drop for FileStream {
    fn drop(&mut self) {
        // Release any outstanding permits before unregistering this stream
        // from the shared scheduler.
        self.queues.take_waiting_planners();
        self.unregister_stream_if_needed();
    }
}

impl Stream for FileStream {
    type Item = Result<RecordBatch>;

    fn poll_next(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Self::Item>> {
        self.file_stream_metrics.time_processing.start();
        let result = self.poll_inner(cx);
        self.file_stream_metrics.time_processing.stop();
        self.baseline_metrics.record_poll(result)
    }
}

impl RecordBatchStream for FileStream {
    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.projected_schema)
    }
}

/// A fallible future that resolves to a stream of [`RecordBatch`]
///
/// This is typically an `async` function that opens the file, and returns a
/// stream that reads the file and produces `RecordBatch`es.
pub type FileOpenFuture =
    BoxFuture<'static, Result<BoxStream<'static, Result<RecordBatch>>>>;

/// Describes the behavior of the `FileStream` if file opening or scanning fails
#[derive(Default)]
pub enum OnError {
    /// Fail the entire stream and return the underlying error
    #[default]
    Fail,
    /// Continue scanning, ignoring the failed file
    Skip,
}

/// Generic API for opening a file using an [`ObjectStore`] and resolving to a
/// stream of [`RecordBatch`]
///
/// [`ObjectStore`]: object_store::ObjectStore
pub trait FileOpener: Unpin + Send + Sync {
    /// Asynchronously open the specified file and return a stream
    /// of [`RecordBatch`]
    ///
    /// TODO: describe prefetching behavior here, and expectations around IO
    fn open(&self, partitioned_file: PartitionedFile) -> Result<FileOpenFuture>;
}

/// A timer that can be started and stopped.
pub struct StartableTime {
    pub metrics: Time,
    // use for record each part cost time, will eventually add into 'metrics'.
    pub start: Option<Instant>,
}

impl StartableTime {
    pub fn start(&mut self) {
        assert!(self.start.is_none());
        self.start = Some(Instant::now());
    }

    pub fn stop(&mut self) {
        if let Some(start) = self.start.take() {
            self.metrics.add_elapsed(start);
        }
    }
}

/// Metrics for [`FileStream`]
///
/// Note that all of these metrics are in terms of wall clock time
/// (not cpu time) so they include time spent waiting on I/O as well
/// as other operators.
///
/// [`FileStream`]: <https://github.com/apache/datafusion/blob/main/datafusion/datasource/src/file_stream.rs>
pub struct FileStreamMetrics {
    /// Wall clock time elapsed for file opening.
    ///
    /// Time between when [`FileOpener::open`] is called and when the
    /// [`FileStream`] receives a stream for reading.
    ///
    /// If there are multiple files being scanned, the stream
    /// will open the next file in the background while scanning the
    /// current file. This metric will only capture time spent opening
    /// while not also scanning.
    /// [`FileStream`]: <https://github.com/apache/datafusion/blob/main/datafusion/datasource/src/file_stream.rs>
    pub time_opening: StartableTime,
    /// Wall clock time elapsed for file scanning + first record batch of decompression + decoding
    ///
    /// Time between when the [`FileStream`] requests data from the
    /// stream and when the first [`RecordBatch`] is produced.
    /// [`FileStream`]: <https://github.com/apache/datafusion/blob/main/datafusion/datasource/src/file_stream.rs>
    pub time_scanning_until_data: StartableTime,
    /// Total elapsed wall clock time for scanning + record batch decompression / decoding
    ///
    /// Sum of time between when the [`FileStream`] requests data from
    /// the stream and when a [`RecordBatch`] is produced for all
    /// record batches in the stream. Note that this metric also
    /// includes the time of the parent operator's execution.
    pub time_scanning_total: StartableTime,
    /// Wall clock time elapsed for data decompression + decoding
    ///
    /// Time spent waiting for the FileStream's input.
    pub time_processing: StartableTime,
    /// Count of errors opening file.
    ///
    /// If using `OnError::Skip` this will provide a count of the number of files
    /// which were skipped and will not be included in the scan results.
    pub file_open_errors: Count,
    /// Count of errors scanning file
    ///
    /// If using `OnError::Skip` this will provide a count of the number of files
    /// which were skipped and will not be included in the scan results.
    pub file_scan_errors: Count,
    /// Count of files successfully opened or evaluated for processing.
    /// At t=end (completion of a query) this is equal to `files_opened`, and both values are equal
    /// to the total number of files in the query; unless the query itself fails.
    /// This value will always be greater than or equal to `files_open`.
    /// Note that this value does *not* mean the file was actually scanned.
    /// We increment this value for any processing of a file, even if that processing is
    /// discarding it because we hit a `LIMIT` (in this case `files_opened` and `files_processed` are both incremented at the same time).
    pub files_opened: Count,
    /// Count of files completely processed / closed (opened, pruned, or skipped due to limit).
    /// At t=0 (the beginning of a query) this is 0.
    /// At t=end (completion of a query) this is equal to `files_opened`, and both values are equal
    /// to the total number of files in the query; unless the query itself fails.
    /// This value will always be less than or equal to `files_open`.
    /// We increment this value for any processing of a file, even if that processing is
    /// discarding it because we hit a `LIMIT` (in this case `files_opened` and `files_processed` are both incremented at the same time).
    pub files_processed: Count,
}

impl FileStreamMetrics {
    pub fn new(metrics: &ExecutionPlanMetricsSet, partition: usize) -> Self {
        let time_opening = StartableTime {
            metrics: MetricBuilder::new(metrics)
                .subset_time("time_elapsed_opening", partition),
            start: None,
        };

        let time_scanning_until_data = StartableTime {
            metrics: MetricBuilder::new(metrics)
                .subset_time("time_elapsed_scanning_until_data", partition),
            start: None,
        };

        let time_scanning_total = StartableTime {
            metrics: MetricBuilder::new(metrics)
                .subset_time("time_elapsed_scanning_total", partition),
            start: None,
        };

        let time_processing = StartableTime {
            metrics: MetricBuilder::new(metrics)
                .subset_time("time_elapsed_processing", partition),
            start: None,
        };

        let file_open_errors =
            MetricBuilder::new(metrics).counter("file_open_errors", partition);

        let file_scan_errors =
            MetricBuilder::new(metrics).counter("file_scan_errors", partition);

        let files_opened = MetricBuilder::new(metrics).counter("files_opened", partition);

        let files_processed =
            MetricBuilder::new(metrics).counter("files_processed", partition);

        Self {
            time_opening,
            time_scanning_until_data,
            time_scanning_total,
            time_processing,
            file_open_errors,
            file_scan_errors,
            files_opened,
            files_processed,
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::file_groups::FileGroup;
    use crate::file_scan_config::{FileScanConfig, FileScanConfigBuilder};
    use crate::morsel::test_utils::{
        IoFutureId, MockMorselSpec, MockMorselizer, MockPlanner, MorselId,
        MorselObserver, PlannerId, ReturnPlanBuilder,
    };
    use crate::tests::make_partition;
    use crate::{PartitionedFile, TableSchema};
    use arrow::datatypes::Int32Type;
    use datafusion_common::error::Result;
    use datafusion_execution::object_store::ObjectStoreUrl;
    use datafusion_physical_plan::metrics::ExecutionPlanMetricsSet;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};

    use crate::file_stream::{
        FileOpenFuture, FileOpener, FileStream, FileStreamBuilder, OnError,
        SharedFileStreamMode, SharedFileStreamState,
    };
    use crate::test_util::MockSource;
    use arrow::array::{Array, AsArray, RecordBatch};
    use arrow::datatypes::{DataType, Field, Schema};
    use datafusion_common::{assert_batches_eq, exec_err, internal_err};
    use futures::{FutureExt, StreamExt};

    /// Test `FileOpener` which will simulate errors during file opening or scanning
    #[derive(Default)]
    struct TestOpener {
        /// Index in stream of files which should throw an error while opening
        error_opening_idx: Vec<usize>,
        /// Index in stream of files which should throw an error while scanning
        error_scanning_idx: Vec<usize>,
        /// Index of last file in stream
        current_idx: AtomicUsize,
        /// `RecordBatch` to return
        records: Vec<RecordBatch>,
    }

    impl FileOpener for TestOpener {
        fn open(&self, _partitioned_file: PartitionedFile) -> Result<FileOpenFuture> {
            let idx = self.current_idx.fetch_add(1, Ordering::SeqCst);

            if self.error_opening_idx.contains(&idx) {
                Ok(futures::future::ready(internal_err!("error opening")).boxed())
            } else if self.error_scanning_idx.contains(&idx) {
                let error = futures::future::ready(exec_err!("error scanning"));
                let stream = futures::stream::once(error).boxed();
                Ok(futures::future::ready(Ok(stream)).boxed())
            } else {
                let iterator = self.records.clone().into_iter().map(Ok);
                let stream = futures::stream::iter(iterator).boxed();
                Ok(futures::future::ready(Ok(stream)).boxed())
            }
        }
    }

    #[derive(Default)]
    struct FileStreamTest {
        /// Number of files in the stream
        num_files: usize,
        /// Global limit of records emitted by the stream
        limit: Option<usize>,
        /// Error-handling behavior of the stream
        on_error: OnError,
        /// Mock `FileOpener`
        opener: TestOpener,
    }

    impl FileStreamTest {
        pub fn new() -> Self {
            Self::default()
        }

        /// Specify the number of files in the stream
        pub fn with_num_files(mut self, num_files: usize) -> Self {
            self.num_files = num_files;
            self
        }

        /// Specify the limit
        pub fn with_limit(mut self, limit: Option<usize>) -> Self {
            self.limit = limit;
            self
        }

        /// Specify the index of files in the stream which should
        /// throw an error when opening
        pub fn with_open_errors(mut self, idx: Vec<usize>) -> Self {
            self.opener.error_opening_idx = idx;
            self
        }

        /// Specify the index of files in the stream which should
        /// throw an error when scanning
        pub fn with_scan_errors(mut self, idx: Vec<usize>) -> Self {
            self.opener.error_scanning_idx = idx;
            self
        }

        /// Specify the behavior of the stream when an error occurs
        pub fn with_on_error(mut self, on_error: OnError) -> Self {
            self.on_error = on_error;
            self
        }

        /// Specify the record batches that should be returned from each
        /// file that is successfully scanned
        pub fn with_records(mut self, records: Vec<RecordBatch>) -> Self {
            self.opener.records = records;
            self
        }

        /// Collect the results of the `FileStream`
        pub async fn result(self) -> Result<Vec<RecordBatch>> {
            let file_schema = self
                .opener
                .records
                .first()
                .map(|batch| batch.schema())
                .unwrap_or_else(|| Arc::new(Schema::empty()));

            // let ctx = SessionContext::new();
            let mock_files: Vec<(String, u64)> = (0..self.num_files)
                .map(|idx| (format!("mock_file{idx}"), 10_u64))
                .collect();

            // let mock_files_ref: Vec<(&str, u64)> = mock_files
            //     .iter()
            //     .map(|(name, size)| (name.as_str(), *size))
            //     .collect();

            let file_group = mock_files
                .into_iter()
                .map(|(name, size)| PartitionedFile::new(name, size))
                .collect();

            let on_error = self.on_error;

            let table_schema = TableSchema::new(file_schema, vec![]);
            let config = FileScanConfigBuilder::new(
                ObjectStoreUrl::parse("test:///").unwrap(),
                Arc::new(MockSource::new(table_schema)),
            )
            .with_file_group(file_group)
            .with_limit(self.limit)
            .build();
            let metrics_set = ExecutionPlanMetricsSet::new();
            let file_stream =
                FileStreamBuilder::new(&config, 0, Arc::new(self.opener), &metrics_set)
                    .with_on_error(on_error)
                    .build()
                    .unwrap();

            file_stream
                .collect::<Vec<_>>()
                .await
                .into_iter()
                .collect::<Result<Vec<_>>>()
        }
    }

    /// helper that creates a stream of 2 files with the same pair of batches in each ([0,1,2] and [0,1])
    async fn create_and_collect(limit: Option<usize>) -> Vec<RecordBatch> {
        FileStreamTest::new()
            .with_records(vec![make_partition(3), make_partition(2)])
            .with_num_files(2)
            .with_limit(limit)
            .result()
            .await
            .expect("error executing stream")
    }

    /// Helper for morsel-driven `FileStream` tests that bundles the mock
    /// `Morselizer` setup with the corresponding `FileScanConfig`.
    #[derive(Clone)]
    struct MorselTest {
        morselizer: MockMorselizer,
        file_names: Vec<String>,
        preserve_order: bool,
        event_summaries: bool,
    }

    impl MorselTest {
        /// Create an empty morsel-driven test harness.
        fn new() -> Self {
            Self {
                morselizer: MockMorselizer::new(),
                file_names: vec![],
                preserve_order: false,
                event_summaries: false,
            }
        }

        /// Add one file and its root mock planner to the test input.
        fn with_file(mut self, path: impl Into<String>, planner: MockPlanner) -> Self {
            let path = path.into();
            self.morselizer = self.morselizer.with_file(path.clone(), planner);
            self.file_names.push(path);
            self
        }

        /// Run this test harness with ordered output semantics enabled.
        fn with_preserve_order(mut self, preserve_order: bool) -> Self {
            self.preserve_order = preserve_order;
            self
        }

        /// Snapshot only the higher-level scheduler events.
        ///
        /// The full event trace is still useful for detailed tests, but for
        /// more complex tests those lower level events obscure the important
        /// events.
        fn with_event_summaries(mut self) -> Self {
            self.event_summaries = true;
            self
        }

        /// Build the `FileScanConfig` corresponding to the configured mock
        /// file set.
        fn test_config(&self) -> FileScanConfig {
            let file_group = self
                .file_names
                .iter()
                .map(|name| PartitionedFile::new(name, 10))
                .collect();
            let table_schema = TableSchema::new(
                Arc::new(Schema::new(vec![Field::new("i", DataType::Int32, false)])),
                vec![],
            );
            FileScanConfigBuilder::new(
                ObjectStoreUrl::parse("test:///").unwrap(),
                Arc::new(MockSource::new(table_schema)),
            )
            .with_file_group(file_group)
            .with_preserve_order(self.preserve_order)
            .build()
        }

        async fn run(self) -> Result<String> {
            // handle to shared observer
            let observer = self.morselizer.observer().clone();
            // Clear any prior observer events before running the test, so the
            // snapshot only includes events from this run.
            observer.clear();

            let config = self.test_config();
            let metrics_set = ExecutionPlanMetricsSet::new();
            let mut stream = FileStreamBuilder::new_with_morselizer(
                &config,
                0,
                Box::new(self.morselizer),
                &metrics_set,
            )
            .build()?;

            let mut stream_contents = Vec::new();
            while let Some(result) = stream.next().await {
                match result {
                    Ok(batch) => {
                        // Each batch should have a single int32 column with the
                        // mocked batch id, which keeps snapshot output compact.
                        let col = batch.column(0).as_primitive::<Int32Type>();
                        assert_eq!(col.len(), 1);
                        assert!(col.is_valid(0));
                        let batch_id = col.value(0);
                        stream_contents.push(format!("Batch: {batch_id}"));
                    }
                    Err(e) => {
                        stream_contents.push(format!("Error: {e}"));
                    }
                }
            }
            stream_contents.push("Done".to_string());
            let output = stream_contents.join("\n");

            // Snapshot both the produced output and the scheduler trace
            // together. This makes scheduler changes much easier to review than
            // maintaining long hand-written event assertions separately.
            let mut parts = vec!["----- Output Stream -----".to_string(), output];
            parts.push("----- File Stream Events -----".to_string());
            let events = if self.event_summaries {
                observer.format_summary_events()
            } else {
                observer.format_events()
            };
            parts.push(events);
            Ok(parts.join("\n"))
        }
    }

    /// Helper for multi-stream morsel tests that share one
    /// [`SharedFileStreamState`].
    #[derive(Clone)]
    struct MultiStreamMorselTest {
        /// Shared mock morselizer used by all sibling streams in the test.
        morselizer: MockMorselizer,
        /// Per-partition file assignments used to build one sibling
        /// `FileStream` per partition.
        partitions: Vec<Vec<String>>,
        /// The sequence of sibling streams to poll while exercising the
        /// stealing scenario under test.
        reads: Vec<TestStreamId>,
    }

    /// Identifies one sibling stream in a [`MultiStreamMorselTest`].
    #[derive(Debug, Clone, Copy)]
    struct TestStreamId(usize);

    impl MultiStreamMorselTest {
        /// Create a sibling-stream test harness with `num_partitions`
        /// independent `FileStream`s.
        fn new(num_partitions: usize) -> Self {
            Self {
                morselizer: MockMorselizer::new(),
                partitions: vec![vec![]; num_partitions],
                reads: vec![],
            }
        }

        /// Add one file and its root planner to a specific sibling stream.
        ///
        /// This lets tests control which stream owns the original file-local
        /// work before stealing redistributes ready morsels or planners.
        fn with_file_in_partition(
            mut self,
            partition: usize,
            path: impl Into<String>,
            planner: MockPlanner,
        ) -> Self {
            let path = path.into();
            self.morselizer = self.morselizer.with_file(path.clone(), planner);
            self.partitions[partition].push(path);
            self
        }

        /// Configure the order in which sibling streams are polled while the
        /// test scenario is executing.
        fn with_reads(mut self, reads: Vec<TestStreamId>) -> Self {
            self.reads = reads;
            self
        }

        /// Build a multi-partition `FileScanConfig` matching the configured
        /// sibling test layout.
        fn test_config(&self) -> FileScanConfig {
            let table_schema = TableSchema::new(
                Arc::new(Schema::new(vec![Field::new("i", DataType::Int32, false)])),
                vec![],
            );
            let mut builder = FileScanConfigBuilder::new(
                ObjectStoreUrl::parse("test:///").unwrap(),
                Arc::new(MockSource::new(table_schema)),
            );
            for file_group in &self.partitions {
                let file_group = file_group
                    .iter()
                    .map(|name| PartitionedFile::new(name, 10))
                    .collect::<Vec<_>>();
                builder = builder.with_file_group(FileGroup::new(file_group));
            }
            builder.build()
        }

        /// Build one `FileStream` per configured partition, all sharing the
        /// same `SharedFileStreamState`.
        ///
        /// This is the core helper for stealing tests: separate streams have
        /// distinct local queues, but share the same outstanding-I/O budget
        /// and shared ready-work queues.
        fn build_streams(&self) -> Result<(MorselObserver, Vec<FileStream>)> {
            let observer = self.morselizer.observer().clone();
            observer.clear();

            let config = self.test_config();
            let shared_state =
                SharedFileStreamState::new(2, SharedFileStreamMode::Unordered);
            let metrics_set = ExecutionPlanMetricsSet::new();
            let streams = (0..self.partitions.len())
                .map(|partition| {
                    FileStreamBuilder::new_with_morselizer(
                        &config,
                        partition,
                        Box::new(self.morselizer.clone()),
                        &metrics_set,
                    )
                    .with_shared_state(shared_state.clone())
                    .build()
                })
                .collect::<Result<Vec<_>>>()?;

            Ok((observer, streams))
        }

        /// Run the configured poll sequence and format the per-stream outputs
        /// plus shared scheduler events into one snapshot string.
        async fn run(self) -> Result<String> {
            let reads = self.reads.clone();
            let (observer, mut streams) = self.build_streams()?;
            let mut outputs = vec![vec![]; streams.len()];

            for stream_id in reads {
                let batch_id = next_batch_id(&mut streams[stream_id.0]).await?;
                assert!(
                    batch_id.is_some(),
                    "expected stream {stream_id:?} to produce a batch"
                );
                outputs[stream_id.0].push(batch_id.unwrap());
            }

            for stream in &mut streams {
                assert_eq!(next_batch_id(stream).await?, None);
            }

            let mut parts = vec![];
            for (idx, output) in outputs.iter().enumerate() {
                parts.push(format!("----- Stream {idx} Output -----"));
                parts.push(
                    output
                        .iter()
                        .map(|batch_id| format!("Batch: {batch_id}"))
                        .chain(std::iter::once("Done".to_string()))
                        .collect::<Vec<_>>()
                        .join("\n"),
                );
            }
            parts.push("----- File Stream Events -----".to_string());
            parts.push(observer.format_summary_events());
            Ok(parts.join("\n"))
        }
    }

    /// Read the next single-row batch from a test stream and return its batch
    /// id.
    async fn next_batch_id(stream: &mut FileStream) -> Result<Option<i32>> {
        let batch = stream.next().await.transpose()?;
        Ok(batch.map(|batch| {
            let col = batch.column(0).as_primitive::<Int32Type>();
            assert_eq!(col.len(), 1);
            assert!(col.is_valid(0));
            col.value(0)
        }))
    }

    /// Verifies the simplest morsel-driven flow: one planner produces one
    /// morsel immediately, and the morsel is then scanned to completion.
    #[tokio::test]
    async fn morsel_framework_single_morsel_no_io() -> Result<()> {
        let test = MorselTest::new().with_file(
            "file1.parquet",
            MockPlanner::builder()
                .with_id(PlannerId(0))
                .return_morsel(MorselId(10), 42)
                .return_none()
                .build(),
        );

        insta::assert_snapshot!(test.run().await.unwrap(), @r"
        ----- Output Stream -----
        Batch: 42
        Done
        ----- File Stream Events -----
        morselize_file: file1.parquet
        planner_created: PlannerId(0)
        planner_called: PlannerId(0)
        morsel_produced: PlannerId(0), MorselId(10)
        planner_called: PlannerId(0)
        morsel_stream_started: MorselId(10)
        morsel_stream_batch_produced: MorselId(10), BatchId(42)
        morsel_stream_finished: MorselId(10)
        ");

        Ok(())
    }

    /// Verifies that a planner can block on one I/O phase, resume, and only
    /// then produce its morsel.
    #[tokio::test]
    async fn morsel_framework_single_morsel_io() -> Result<()> {
        let test = MorselTest::new().with_file(
            "file1.parquet",
            MockPlanner::builder()
                .with_id(PlannerId(0))
                .return_plan(ReturnPlanBuilder::new().with_io(IoFutureId(100), 1))
                .return_morsel(MorselId(10), 42)
                .return_none()
                .build(),
        );

        insta::assert_snapshot!(test.run().await.unwrap(), @r"
        ----- Output Stream -----
        Batch: 42
        Done
        ----- File Stream Events -----
        morselize_file: file1.parquet
        planner_created: PlannerId(0)
        planner_called: PlannerId(0)
        io_future_created: PlannerId(0), IoFutureId(100)
        io_future_polled: PlannerId(0), IoFutureId(100)
        io_future_polled: PlannerId(0), IoFutureId(100)
        io_future_resolved: PlannerId(0), IoFutureId(100)
        planner_called: PlannerId(0)
        morsel_produced: PlannerId(0), MorselId(10)
        planner_called: PlannerId(0)
        morsel_stream_started: MorselId(10)
        morsel_stream_batch_produced: MorselId(10), BatchId(42)
        morsel_stream_finished: MorselId(10)
        ");

        Ok(())
    }

    /// Verifies that a planner can require multiple CPU-only `plan()` calls
    /// before it discovers any morsels or I/O, matching the staged behavior of
    /// the Parquet morsel planner.
    #[tokio::test]
    async fn morsel_framework_two_cpu_steps_before_morsel() -> Result<()> {
        let test = MorselTest::new().with_file(
            "file1.parquet",
            MockPlanner::builder()
                .with_id(PlannerId(0))
                .return_plan(ReturnPlanBuilder::new())
                .return_plan(ReturnPlanBuilder::new())
                .return_morsel(MorselId(10), 42)
                .return_none()
                .build(),
        );

        insta::assert_snapshot!(test.run().await.unwrap(), @r"
        ----- Output Stream -----
        Batch: 42
        Done
        ----- File Stream Events -----
        morselize_file: file1.parquet
        planner_created: PlannerId(0)
        planner_called: PlannerId(0)
        planner_called: PlannerId(0)
        planner_called: PlannerId(0)
        morsel_produced: PlannerId(0), MorselId(10)
        planner_called: PlannerId(0)
        morsel_stream_started: MorselId(10)
        morsel_stream_batch_produced: MorselId(10), BatchId(42)
        morsel_stream_finished: MorselId(10)
        ");

        Ok(())
    }

    /// Verifies direct morsels returned from a planner are consumed before
    /// batches produced by any returned child planners.
    #[tokio::test]
    async fn morsel_framework_morsels_before_child_planner() -> Result<()> {
        let child_planner = MockPlanner::builder()
            .with_id(PlannerId(1))
            .return_morsel(MorselId(11), 43)
            .return_none()
            .build();

        // planner 0 returns batch 42
        let parent_planner = MockPlanner::builder()
            .with_id(PlannerId(0))
            .return_plan(
                ReturnPlanBuilder::new()
                    .with_morsel(MockMorselSpec::single_batch(MorselId(10), 42))
                    .with_planner(child_planner),
            )
            .return_none()
            .build();

        let test = MorselTest::new()
            .with_file("file1.parquet", parent_planner)
            .with_event_summaries();

        insta::assert_snapshot!(test.run().await.unwrap(), @r"
        ----- Output Stream -----
        Batch: 42
        Batch: 43
        Done
        ----- File Stream Events -----
        morselize_file: file1.parquet
        planner_created: PlannerId(0)
        morsel_produced: PlannerId(0), MorselId(10)
        planner_produced_child: PlannerId(0) -> PlannerId(1)
        morsel_produced: PlannerId(1), MorselId(11)
        morsel_stream_batch_produced: MorselId(10), BatchId(42)
        morsel_stream_batch_produced: MorselId(11), BatchId(43)
        ");

        Ok(())
    }

    /// Verifies the non-ordered behavior for child planners: if the first child
    /// planner blocks on I/O and the second can make progress immediately, the
    /// second planner's batches are emitted first.
    #[tokio::test]
    async fn morsel_framework_child_planner_reorder() -> Result<()> {
        let planner_1 = MockPlanner::builder()
            .with_id(PlannerId(1))
            // Note IO required 2 polls
            .return_plan(ReturnPlanBuilder::new().with_io(IoFutureId(100), 2))
            .return_morsel(MorselId(11), 41)
            .return_none()
            .build();
        let planner_2 = MockPlanner::builder()
            .with_id(PlannerId(2))
            // IO only requires 1 poll, so it will resolve before planner 1's IO
            .return_plan(ReturnPlanBuilder::new().with_io(IoFutureId(101), 1)) // IO returns after 1 poll
            .return_morsel(MorselId(12), 42)
            .return_none()
            .build();

        let parent_planner = MockPlanner::builder()
            .with_id(PlannerId(0))
            .return_plan(
                ReturnPlanBuilder::new()
                    .with_planner(planner_1)
                    .with_planner(planner_2),
            )
            .return_none()
            .build();

        let test = MorselTest::new().with_file("file1.parquet", parent_planner);

        // Expect both futures to be polled, but second planner's (42) batch to be
        // produced first
        insta::assert_snapshot!(test.clone().run().await.unwrap(), @r"
        ----- Output Stream -----
        Batch: 42
        Batch: 41
        Done
        ----- File Stream Events -----
        morselize_file: file1.parquet
        planner_created: PlannerId(0)
        planner_called: PlannerId(0)
        planner_produced_child: PlannerId(0) -> PlannerId(1)
        planner_produced_child: PlannerId(0) -> PlannerId(2)
        planner_called: PlannerId(0)
        planner_called: PlannerId(1)
        io_future_created: PlannerId(1), IoFutureId(100)
        planner_called: PlannerId(2)
        io_future_created: PlannerId(2), IoFutureId(101)
        io_future_polled: PlannerId(1), IoFutureId(100)
        io_future_polled: PlannerId(2), IoFutureId(101)
        io_future_polled: PlannerId(1), IoFutureId(100)
        io_future_polled: PlannerId(2), IoFutureId(101)
        io_future_resolved: PlannerId(2), IoFutureId(101)
        planner_called: PlannerId(2)
        morsel_produced: PlannerId(2), MorselId(12)
        planner_called: PlannerId(2)
        io_future_polled: PlannerId(1), IoFutureId(100)
        io_future_resolved: PlannerId(1), IoFutureId(100)
        planner_called: PlannerId(1)
        morsel_produced: PlannerId(1), MorselId(11)
        morsel_stream_started: MorselId(12)
        morsel_stream_batch_produced: MorselId(12), BatchId(42)
        planner_called: PlannerId(1)
        morsel_stream_finished: MorselId(12)
        morsel_stream_started: MorselId(11)
        morsel_stream_batch_produced: MorselId(11), BatchId(41)
        morsel_stream_finished: MorselId(11)
        ");

        // Run same test using `with_preserve_order(true)`, but expect the first
        // planner's batch (41) to be produced before the second's (42), even
        // though the second planner's I/O resolves first.
        let test = test.with_preserve_order(true);

        insta::assert_snapshot!(test.run().await.unwrap(), @r"
        ----- Output Stream -----
        Batch: 41
        Batch: 42
        Done
        ----- File Stream Events -----
        morselize_file: file1.parquet
        planner_created: PlannerId(0)
        planner_called: PlannerId(0)
        planner_produced_child: PlannerId(0) -> PlannerId(1)
        planner_produced_child: PlannerId(0) -> PlannerId(2)
        planner_called: PlannerId(0)
        planner_called: PlannerId(1)
        io_future_created: PlannerId(1), IoFutureId(100)
        io_future_polled: PlannerId(1), IoFutureId(100)
        io_future_polled: PlannerId(1), IoFutureId(100)
        io_future_polled: PlannerId(1), IoFutureId(100)
        io_future_resolved: PlannerId(1), IoFutureId(100)
        planner_called: PlannerId(2)
        io_future_created: PlannerId(2), IoFutureId(101)
        io_future_polled: PlannerId(2), IoFutureId(101)
        io_future_polled: PlannerId(2), IoFutureId(101)
        io_future_resolved: PlannerId(2), IoFutureId(101)
        planner_called: PlannerId(1)
        morsel_produced: PlannerId(1), MorselId(11)
        planner_called: PlannerId(1)
        morsel_stream_started: MorselId(11)
        morsel_stream_batch_produced: MorselId(11), BatchId(41)
        morsel_stream_finished: MorselId(11)
        planner_called: PlannerId(2)
        morsel_produced: PlannerId(2), MorselId(12)
        planner_called: PlannerId(2)
        morsel_stream_started: MorselId(12)
        morsel_stream_batch_produced: MorselId(12), BatchId(42)
        morsel_stream_finished: MorselId(12)
        ");

        Ok(())
    }

    /// Verifies that child planners still respect the global outstanding-I/O
    /// cap. Even if a parent planner returns three ready children, only two of
    /// them should be allowed to create waiting I/O futures at once.
    #[tokio::test]
    async fn morsel_framework_child_planner_io_respects_global_cap() -> Result<()> {
        let planner_1 = MockPlanner::builder()
            .with_id(PlannerId(1))
            .return_plan(ReturnPlanBuilder::new().with_io(IoFutureId(100), 1))
            .return_morsel(MorselId(11), 41)
            .return_none()
            .build();
        let planner_2 = MockPlanner::builder()
            .with_id(PlannerId(2))
            .return_plan(ReturnPlanBuilder::new().with_io(IoFutureId(101), 3))
            .return_morsel(MorselId(12), 42)
            .return_none()
            .build();
        let planner_3 = MockPlanner::builder()
            .with_id(PlannerId(3))
            .return_plan(ReturnPlanBuilder::new().with_io(IoFutureId(102), 1))
            .return_morsel(MorselId(13), 43)
            .return_none()
            .build();

        let parent_planner = MockPlanner::builder()
            .with_id(PlannerId(0))
            .return_plan(
                ReturnPlanBuilder::new()
                    .with_planner(planner_1)
                    .with_planner(planner_2)
                    .with_planner(planner_3),
            )
            .return_none()
            .build();

        let test = MorselTest::new()
            .with_file("file1.parquet", parent_planner)
            .with_event_summaries();

        // Note that the future for planner 1 must resolve before planner 2 begins
        insta::assert_snapshot!(test.run().await.unwrap(), @r"
        ----- Output Stream -----
        Batch: 41
        Batch: 42
        Batch: 43
        Done
        ----- File Stream Events -----
        morselize_file: file1.parquet
        planner_created: PlannerId(0)
        planner_produced_child: PlannerId(0) -> PlannerId(1)
        planner_produced_child: PlannerId(0) -> PlannerId(2)
        planner_produced_child: PlannerId(0) -> PlannerId(3)
        io_future_created: PlannerId(1), IoFutureId(100)
        io_future_created: PlannerId(2), IoFutureId(101)
        io_future_resolved: PlannerId(1), IoFutureId(100)
        io_future_created: PlannerId(3), IoFutureId(102)
        io_future_resolved: PlannerId(2), IoFutureId(101)
        io_future_resolved: PlannerId(3), IoFutureId(102)
        morsel_produced: PlannerId(1), MorselId(11)
        morsel_produced: PlannerId(2), MorselId(12)
        morsel_stream_batch_produced: MorselId(11), BatchId(41)
        morsel_produced: PlannerId(3), MorselId(13)
        morsel_stream_batch_produced: MorselId(12), BatchId(42)
        morsel_stream_batch_produced: MorselId(13), BatchId(43)
        ");

        Ok(())
    }

    /// Verifies that `FileStream` overlaps planner I/O across multiple files
    /// rather than waiting for the first file to finish before starting the
    /// second.
    #[tokio::test]
    async fn morsel_framework_two_files_overlapping_io() -> Result<()> {
        let test = MorselTest::new()
            .with_file(
                "file1.parquet",
                MockPlanner::builder()
                    .with_id(PlannerId(0))
                    .return_plan(ReturnPlanBuilder::new().with_io(IoFutureId(100), 1))
                    .return_morsel(MorselId(10), 42)
                    .return_none()
                    .build(),
            )
            .with_file(
                "file2.parquet",
                MockPlanner::builder()
                    .with_id(PlannerId(1))
                    .return_plan(ReturnPlanBuilder::new().with_io(IoFutureId(101), 1))
                    .return_morsel(MorselId(11), 43)
                    .return_none()
                    .build(),
            );

        insta::assert_snapshot!(test.run().await.unwrap(), @r"
        ----- Output Stream -----
        Batch: 42
        Batch: 43
        Done
        ----- File Stream Events -----
        morselize_file: file1.parquet
        planner_created: PlannerId(0)
        morselize_file: file2.parquet
        planner_created: PlannerId(1)
        planner_called: PlannerId(0)
        io_future_created: PlannerId(0), IoFutureId(100)
        planner_called: PlannerId(1)
        io_future_created: PlannerId(1), IoFutureId(101)
        io_future_polled: PlannerId(0), IoFutureId(100)
        io_future_polled: PlannerId(1), IoFutureId(101)
        io_future_polled: PlannerId(0), IoFutureId(100)
        io_future_resolved: PlannerId(0), IoFutureId(100)
        io_future_polled: PlannerId(1), IoFutureId(101)
        io_future_resolved: PlannerId(1), IoFutureId(101)
        planner_called: PlannerId(0)
        morsel_produced: PlannerId(0), MorselId(10)
        planner_called: PlannerId(0)
        planner_called: PlannerId(1)
        morsel_produced: PlannerId(1), MorselId(11)
        morsel_stream_started: MorselId(10)
        morsel_stream_batch_produced: MorselId(10), BatchId(42)
        planner_called: PlannerId(1)
        morsel_stream_finished: MorselId(10)
        morsel_stream_started: MorselId(11)
        morsel_stream_batch_produced: MorselId(11), BatchId(43)
        morsel_stream_finished: MorselId(11)
        ");

        Ok(())
    }

    /// Verifies that an idle sibling stream can steal ready morsels even when
    /// it has no local files of its own.
    #[tokio::test]
    async fn morsel_framework_sibling_stream_steals_when_only_one_has_files() -> Result<()>
    {
        let test = MultiStreamMorselTest::new(2)
            .with_file_in_partition(
                0,
                "file1.parquet",
                MockPlanner::builder()
                    .with_id(PlannerId(0))
                    .return_plan(
                        ReturnPlanBuilder::new()
                            .return_morsel(MorselId(10), 41)
                            .return_morsel(MorselId(11), 42),
                    )
                    .return_none()
                    .build(),
            )
            // Poll sibling 0 first so it discovers the file and publishes
            // ready morsels. Poll sibling 1 next: because it has no local
            // files, any batch it returns must have been stolen from sibling 0.
            .with_reads(vec![TestStreamId(0), TestStreamId(1)]);

        insta::assert_snapshot!(
            test.run().await.unwrap(),
            @r"
        ----- Stream 0 Output -----
        Batch: 41
        Done
        ----- Stream 1 Output -----
        Batch: 42
        Done
        ----- File Stream Events -----
        morselize_file: file1.parquet
        planner_created: PlannerId(0)
        morsel_produced: PlannerId(0), MorselId(10)
        morsel_produced: PlannerId(0), MorselId(11)
        morsel_stream_batch_produced: MorselId(10), BatchId(41)
        morsel_stream_batch_produced: MorselId(11), BatchId(42)
        "
        );

        Ok(())
    }

    /// Verifies that a sibling stream waiting on its own file's I/O can steal
    /// ready work from a faster sibling and continue making progress.
    #[tokio::test]
    async fn morsel_framework_sibling_stream_steals_while_own_file_waits_on_io()
    -> Result<()> {
        let test = MultiStreamMorselTest::new(2)
            .with_file_in_partition(
                0,
                "fast.parquet",
                MockPlanner::builder()
                    .with_id(PlannerId(0))
                    .return_plan(
                        ReturnPlanBuilder::new()
                            .return_morsel(MorselId(10), 41)
                            .return_morsel(MorselId(11), 42),
                    )
                    .return_none()
                    .build(),
            )
            .with_file_in_partition(
                1,
                "slow.parquet",
                MockPlanner::builder()
                    .with_id(PlannerId(1))
                    .return_plan(ReturnPlanBuilder::new().with_io(IoFutureId(100), 2))
                    .return_morsel(MorselId(12), 51)
                    .return_none()
                    .build(),
            )
            // Poll sibling 0 first so it publishes one ready morsel from the
            // fast file. Poll sibling 1 next while its own file is still
            // blocked on I/O: the batch it returns at that point must have
            // been stolen from sibling 0. Poll sibling 0 again last so it can
            // finish once sibling 1's local I/O has resolved.
            .with_reads(vec![TestStreamId(0), TestStreamId(1), TestStreamId(0)]);

        insta::assert_snapshot!(
            test.run().await.unwrap(),
            @r"
        ----- Stream 0 Output -----
        Batch: 41
        Batch: 51
        Done
        ----- Stream 1 Output -----
        Batch: 42
        Done
        ----- File Stream Events -----
        morselize_file: fast.parquet
        planner_created: PlannerId(0)
        morsel_produced: PlannerId(0), MorselId(10)
        morsel_produced: PlannerId(0), MorselId(11)
        morsel_stream_batch_produced: MorselId(10), BatchId(41)
        morselize_file: slow.parquet
        planner_created: PlannerId(1)
        morsel_stream_batch_produced: MorselId(11), BatchId(42)
        io_future_created: PlannerId(1), IoFutureId(100)
        io_future_resolved: PlannerId(1), IoFutureId(100)
        morsel_produced: PlannerId(1), MorselId(12)
        morsel_stream_batch_produced: MorselId(12), BatchId(51)
        "
        );

        Ok(())
    }

    #[tokio::test]
    async fn on_error_opening() -> Result<()> {
        let batches = FileStreamTest::new()
            .with_records(vec![make_partition(3), make_partition(2)])
            .with_num_files(2)
            .with_on_error(OnError::Skip)
            .with_open_errors(vec![0])
            .result()
            .await?;

        #[rustfmt::skip]
        assert_batches_eq!(&[
            "+---+",
            "| i |",
            "+---+",
            "| 0 |",
            "| 1 |",
            "| 2 |",
            "| 0 |",
            "| 1 |",
            "+---+",
        ], &batches);

        let batches = FileStreamTest::new()
            .with_records(vec![make_partition(3), make_partition(2)])
            .with_num_files(2)
            .with_on_error(OnError::Skip)
            .with_open_errors(vec![1])
            .result()
            .await?;

        #[rustfmt::skip]
        assert_batches_eq!(&[
            "+---+",
            "| i |",
            "+---+",
            "| 0 |",
            "| 1 |",
            "| 2 |",
            "| 0 |",
            "| 1 |",
            "+---+",
        ], &batches);

        let batches = FileStreamTest::new()
            .with_records(vec![make_partition(3), make_partition(2)])
            .with_num_files(2)
            .with_on_error(OnError::Skip)
            .with_open_errors(vec![0, 1])
            .result()
            .await?;

        #[rustfmt::skip]
        assert_batches_eq!(&[
            "++",
            "++",
        ], &batches);

        Ok(())
    }

    #[tokio::test]
    async fn on_error_scanning_fail() -> Result<()> {
        let result = FileStreamTest::new()
            .with_records(vec![make_partition(3), make_partition(2)])
            .with_num_files(2)
            .with_on_error(OnError::Fail)
            .with_scan_errors(vec![1])
            .result()
            .await;

        assert!(result.is_err());

        Ok(())
    }

    #[tokio::test]
    async fn on_error_opening_fail() -> Result<()> {
        let result = FileStreamTest::new()
            .with_records(vec![make_partition(3), make_partition(2)])
            .with_num_files(2)
            .with_on_error(OnError::Fail)
            .with_open_errors(vec![1])
            .result()
            .await;

        assert!(result.is_err());

        Ok(())
    }

    #[tokio::test]
    async fn on_error_scanning() -> Result<()> {
        let batches = FileStreamTest::new()
            .with_records(vec![make_partition(3), make_partition(2)])
            .with_num_files(2)
            .with_on_error(OnError::Skip)
            .with_scan_errors(vec![0])
            .result()
            .await?;

        #[rustfmt::skip]
        assert_batches_eq!(&[
            "+---+",
            "| i |",
            "+---+",
            "| 0 |",
            "| 1 |",
            "| 2 |",
            "| 0 |",
            "| 1 |",
            "+---+",
        ], &batches);

        let batches = FileStreamTest::new()
            .with_records(vec![make_partition(3), make_partition(2)])
            .with_num_files(2)
            .with_on_error(OnError::Skip)
            .with_scan_errors(vec![1])
            .result()
            .await?;

        #[rustfmt::skip]
        assert_batches_eq!(&[
            "+---+",
            "| i |",
            "+---+",
            "| 0 |",
            "| 1 |",
            "| 2 |",
            "| 0 |",
            "| 1 |",
            "+---+",
        ], &batches);

        let batches = FileStreamTest::new()
            .with_records(vec![make_partition(3), make_partition(2)])
            .with_num_files(2)
            .with_on_error(OnError::Skip)
            .with_scan_errors(vec![0, 1])
            .result()
            .await?;

        #[rustfmt::skip]
        assert_batches_eq!(&[
            "++",
            "++",
        ], &batches);

        Ok(())
    }

    #[tokio::test]
    async fn on_error_mixed() -> Result<()> {
        let batches = FileStreamTest::new()
            .with_records(vec![make_partition(3), make_partition(2)])
            .with_num_files(3)
            .with_on_error(OnError::Skip)
            .with_open_errors(vec![1])
            .with_scan_errors(vec![0])
            .result()
            .await?;

        #[rustfmt::skip]
        assert_batches_eq!(&[
            "+---+",
            "| i |",
            "+---+",
            "| 0 |",
            "| 1 |",
            "| 2 |",
            "| 0 |",
            "| 1 |",
            "+---+",
        ], &batches);

        let batches = FileStreamTest::new()
            .with_records(vec![make_partition(3), make_partition(2)])
            .with_num_files(3)
            .with_on_error(OnError::Skip)
            .with_open_errors(vec![0])
            .with_scan_errors(vec![1])
            .result()
            .await?;

        #[rustfmt::skip]
        assert_batches_eq!(&[
            "+---+",
            "| i |",
            "+---+",
            "| 0 |",
            "| 1 |",
            "| 2 |",
            "| 0 |",
            "| 1 |",
            "+---+",
        ], &batches);

        let batches = FileStreamTest::new()
            .with_records(vec![make_partition(3), make_partition(2)])
            .with_num_files(3)
            .with_on_error(OnError::Skip)
            .with_open_errors(vec![2])
            .with_scan_errors(vec![0, 1])
            .result()
            .await?;

        #[rustfmt::skip]
        assert_batches_eq!(&[
            "++",
            "++",
        ], &batches);

        let batches = FileStreamTest::new()
            .with_records(vec![make_partition(3), make_partition(2)])
            .with_num_files(3)
            .with_on_error(OnError::Skip)
            .with_open_errors(vec![0, 2])
            .with_scan_errors(vec![1])
            .result()
            .await?;

        #[rustfmt::skip]
        assert_batches_eq!(&[
            "++",
            "++",
        ], &batches);

        Ok(())
    }

    #[tokio::test]
    async fn without_limit() -> Result<()> {
        let batches = create_and_collect(None).await;

        #[rustfmt::skip]
        assert_batches_eq!(&[
            "+---+",
            "| i |",
            "+---+",
            "| 0 |",
            "| 1 |",
            "| 2 |",
            "| 0 |",
            "| 1 |",
            "| 0 |",
            "| 1 |",
            "| 2 |",
            "| 0 |",
            "| 1 |",
            "+---+",
        ], &batches);

        Ok(())
    }

    #[tokio::test]
    async fn with_limit_between_files() -> Result<()> {
        let batches = create_and_collect(Some(5)).await;
        #[rustfmt::skip]
        assert_batches_eq!(&[
            "+---+",
            "| i |",
            "+---+",
            "| 0 |",
            "| 1 |",
            "| 2 |",
            "| 0 |",
            "| 1 |",
            "+---+",
        ], &batches);

        Ok(())
    }

    #[tokio::test]
    async fn with_limit_at_middle_of_batch() -> Result<()> {
        let batches = create_and_collect(Some(6)).await;
        #[rustfmt::skip]
        assert_batches_eq!(&[
            "+---+",
            "| i |",
            "+---+",
            "| 0 |",
            "| 1 |",
            "| 2 |",
            "| 0 |",
            "| 1 |",
            "| 0 |",
            "+---+",
        ], &batches);

        Ok(())
    }
}
