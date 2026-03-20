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

pub use shared_state::{
    FileStreamId, OutstandingIoPermit, SharedFileStreamMode, SharedFileStreamState,
};

use std::collections::VecDeque;
use std::mem;
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
use futures::future::BoxFuture;
use futures::stream::BoxStream;
use futures::{FutureExt, Stream, StreamExt as _};

const DEFAULT_OUTSTANDING_IOS_PER_PARTITION: usize = 2;

/// Keep at most this many morsels buffered before pausing additional planning.
///
/// The default is one morsel per available core. The intent is that once work
/// stealing is added, each other core can find at least one morsel to steal
/// without requiring the scan to eagerly buffer an unbounded amount of work.
///
/// TODO make this a config option
fn max_buffered_morsels() -> usize {
    std::thread::available_parallelism()
        .map(usize::from)
        .unwrap_or(1)
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
fn default_shared_file_stream_state(config: &FileScanConfig) -> SharedFileStreamState {
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
/// the active stream, if any. If that is not ready, it will use the CPUto
/// prepare more morsels or discover new IO before launching the next morsel.
///
/// The flow is:
/// 1. Read each file from `file_iter` and turn it into morsels (potentially in parallel)
/// 2. Read each morsel individually and produce `RecordBatch`es for processing
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
    /// An iterator over input files.
    file_iter: VecDeque<PartitionedFile>,
    /// One or more planners are waiting on I/O
    waiting_planners: VecDeque<WaitingPlanner>,
    /// One or more planners that is waiting on CPU time
    ready_planners: VecDeque<Box<dyn MorselPlanner>>,
    /// Morsels which are waiting on CPU for processing
    ///
    /// (TODO steal morsels from other streams)
    morsels: VecDeque<Box<dyn Morsel>>,
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

impl FileStream {
    /// Create a new `FileStream` using the give `FileOpener` to scan underlying files
    pub fn new(
        config: &FileScanConfig,
        partition: usize,
        file_opener: Arc<dyn FileOpener>,
        metrics: &ExecutionPlanMetricsSet,
    ) -> Result<Self> {
        Self::new_with_morselizer(
            config,
            partition,
            Box::new(FileOpenerMorselizer::new(file_opener)),
            metrics,
        )
    }

    /// Create a new FileStream from morsels
    pub fn new_with_morselizer(
        config: &FileScanConfig,
        partition: usize,
        morselizer: Box<dyn Morselizer>,
        metrics: &ExecutionPlanMetricsSet,
    ) -> Result<Self> {
        let shared_file_stream_state = default_shared_file_stream_state(config);
        let projected_schema = config.projected_schema()?;

        let file_group = config.file_groups[partition].clone();

        Ok(Self {
            file_iter: file_group.into_inner().into_iter().collect(),
            ready_planners: VecDeque::new(),
            waiting_planners: VecDeque::new(),
            morsels: VecDeque::new(),
            reader: None,
            projected_schema,
            remain: config.limit,
            morselizer,
            file_stream_metrics: FileStreamMetrics::new(metrics, partition),
            baseline_metrics: BaselineMetrics::new(metrics, partition),
            on_error: OnError::Fail,
            preserve_order: config.preserve_order,
            shared_file_stream_state: shared_file_stream_state.clone(),
            stream_id: None,
            state: StreamState::Active,
        })
        .map(|stream| stream.with_shared_state(shared_file_stream_state))
    }
    /// Specify the behavior when an error occurs opening or scanning a file
    ///
    /// If `OnError::Skip` the stream will skip files which encounter an error and continue
    /// If `OnError:Fail` (default) the stream will fail and stop processing when an error occurs
    pub fn with_on_error(mut self, on_error: OnError) -> Self {
        self.on_error = on_error;
        self
    }

    /// Specify whether this `FileStream` should preserve the logical output
    /// order implied by `MorselPlan`s.
    pub fn with_preserve_order(mut self, preserve_order: bool) -> Self {
        self.preserve_order = preserve_order;
        self
    }

    /// Replace the shared scheduler state used by this stream before it has
    /// been registered with any shared-state instance.
    ///
    /// This is intended for callers such as `DataSourceExec` that want several
    /// sibling streams to coordinate against the same shared I/O budget.
    ///
    /// # Panics
    ///
    /// Panics if the stream has already registered itself with a shared state.
    pub fn with_shared_state(
        mut self,
        shared_file_stream_state: SharedFileStreamState,
    ) -> Self {
        assert!(
            self.stream_id.is_none(),
            "cannot replace shared state after the stream has registered itself"
        );
        self.shared_file_stream_state = shared_file_stream_state;
        self.stream_id = Some(self.shared_file_stream_state.register_stream());
        self
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
            self.morsels.extend(plan.take_morsels());
            self.ready_planners.extend(plan.take_planners());
            if let Some(io_future) = plan.take_io_future() {
                self.waiting_planners.push_back(WaitingPlanner::new(
                    planner,
                    io_future,
                    io_permit
                        .take()
                        .expect("planner I/O permit should be available"),
                ));
                break;
            }

            if self.morsels.len() >= max_buffered_morsels {
                self.ready_planners.push_back(planner);
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
        for planner in self.morselizer.morselize(file)? {
            self.ready_planners.push_back(planner);
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
        let local_planner_target =
            self.shared_file_stream_state.max_outstanding_ios().max(1);
        while (self.waiting_planners.len() + self.ready_planners.len())
            < local_planner_target
        {
            // In ordered mode, do not admit later files while there is any
            // earlier file work still buffered, waiting on I/O, or actively
            // being scanned. This keeps file-level planning from introducing
            // later output ahead of earlier files.
            if self.preserve_order
                && (self.reader.is_some()
                    || !self.morsels.is_empty()
                    || !self.ready_planners.is_empty()
                    || !self.waiting_planners.is_empty())
            {
                break;
            }
            if self.morsels.len() >= max_buffered_morsels {
                break;
            }
            let Some(file) = self.file_iter.pop_front() else {
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
        for mut waiting_planner in mem::take(&mut self.waiting_planners) {
            match waiting_planner.io_future.poll_unpin(cx) {
                Poll::Ready(Ok(())) => {
                    self.file_stream_metrics.files_opened.add(1);
                    self.ready_planners.push_back(waiting_planner.planner);
                }
                Poll::Ready(Err(e)) => {
                    self.file_stream_metrics.file_open_errors.add(1);
                    match self.on_error {
                        OnError::Skip => {
                            self.file_stream_metrics.files_processed.add(1);
                        }
                        OnError::Fail => return Err(e),
                    }
                }
                Poll::Pending => self.waiting_planners.push_back(waiting_planner),
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
            && let Some(morsel) = self.morsels.pop_front()
        {
            self.reader = Some(morsel.into_stream());
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
                self.waiting_planners.clear();
                self.ready_planners.clear();
                self.morsels.clear();
                self.state = StreamState::Error;
                self.unregister_stream_if_needed();
                return Poll::Ready(Some(Err(e)));
            }
            if let Err(e) = self.check_io(cx) {
                self.waiting_planners.clear();
                self.ready_planners.clear();
                self.morsels.clear();
                self.state = StreamState::Error;
                self.unregister_stream_if_needed();
                return Poll::Ready(Some(Err(e)));
            }

            // Give ready planners CPU whenever there is buffer space, even if a
            // reader is currently active. This avoids starving planner work
            // behind a reader that is itself waiting on I/O.
            while self.morsels.len() < max_buffered_morsels() {
                // In ordered mode, once an earlier planner has produced a
                // morsel or is blocked on I/O, do not advance later sibling
                // planners yet. This preserves the logical `MorselPlan` order:
                // direct morsels first, then child planners in API order.
                if self.preserve_order
                    && (self.reader.is_some()
                        || !self.morsels.is_empty()
                        || !self.waiting_planners.is_empty())
                {
                    break;
                }
                let stream_id = match self.stream_id() {
                    Ok(stream_id) => stream_id,
                    Err(e) => {
                        self.waiting_planners.clear();
                        self.ready_planners.clear();
                        self.morsels.clear();
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
                let Some(planner) = self.ready_planners.pop_front() else {
                    drop(io_permit);
                    break;
                };
                if let Err(e) = self.plan_morsels(planner, io_permit) {
                    self.waiting_planners.clear();
                    self.ready_planners.clear();
                    self.morsels.clear();
                    self.state = StreamState::Error;
                    self.unregister_stream_if_needed();
                    return Poll::Ready(Some(Err(e)));
                }

                // Once a morsel is buffered and a reader is already active,
                // return to the scan side of the scheduler rather than
                // continuing to spend CPU on planning in this poll.
                if self.reader.is_some() && !self.morsels.is_empty() {
                    break;
                }
            }

            // Newly planned work may have just discovered fresh I/O. Poll it
            // once now so the future can register the current waker before we
            // return `Pending`; otherwise the stream can stall waiting on an
            // I/O future that has never been polled.
            if let Err(e) = self.check_io(cx) {
                self.waiting_planners.clear();
                self.ready_planners.clear();
                self.morsels.clear();
                self.state = StreamState::Error;
                self.unregister_stream_if_needed();
                return Poll::Ready(Some(Err(e)));
            }

            // The second I/O poll may have completed planner work discovered
            // during this same call to `poll_inner`. Loop back so newly ready
            // planners get CPU time before we consider returning `Pending`.
            if !self.ready_planners.is_empty()
                && self.morsels.len() < max_buffered_morsels()
                // In ordered mode, only loop back for more planner CPU when
                // there is no earlier reader, buffered morsel, or waiting I/O
                // that should be drained first. Otherwise, drop to
                // `start_next_morsel()` so output is produced in order.
                && (!self.preserve_order
                    || (self.reader.is_none()
                        && self.morsels.is_empty()
                        && self.waiting_planners.is_empty()))
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
                        return Poll::Ready(Some(Ok(batch)));
                    }
                    Poll::Ready(Some(Err(e))) => {
                        self.reader = None;
                        self.file_stream_metrics.file_scan_errors.add(1);
                        self.file_stream_metrics.time_scanning_until_data.stop();
                        self.file_stream_metrics.time_scanning_total.stop();

                        match self.on_error {
                            OnError::Fail => {
                                self.waiting_planners.clear();
                                self.ready_planners.clear();
                                self.morsels.clear();
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

            if self.reader.is_none()
                && self.morsels.is_empty()
                && self.ready_planners.is_empty()
                && self.waiting_planners.is_empty()
                && self.file_iter.is_empty()
            {
                self.state = StreamState::Done;
                self.unregister_stream_if_needed();
                return Poll::Ready(None);
            }

            return Poll::Pending;
        }
    }
}

/// A planner that has already discovered its next I/O phase.
struct WaitingPlanner {
    planner: Box<dyn MorselPlanner>,
    io_future: BoxFuture<'static, Result<()>>,
    _io_permit: OutstandingIoPermit,
}

impl WaitingPlanner {
    fn new(
        planner: Box<dyn MorselPlanner>,
        io_future: BoxFuture<'static, Result<()>>,
        io_permit: OutstandingIoPermit,
    ) -> Self {
        Self {
            planner,
            io_future,
            _io_permit: io_permit,
        }
    }
}

impl Drop for FileStream {
    fn drop(&mut self) {
        // Release any outstanding permits before unregistering this stream
        // from the shared scheduler.
        self.waiting_planners.clear();
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
    use crate::file_scan_config::{FileScanConfig, FileScanConfigBuilder};
    use crate::morsel::test_utils::{
        IoFutureId, MockMorselSpec, MockMorselizer, MockPlanner, MorselId, PlannerId,
        ReturnPlanBuilder,
    };
    use crate::tests::make_partition;
    use crate::{PartitionedFile, TableSchema};
    use arrow::datatypes::Int32Type;
    use datafusion_common::error::Result;
    use datafusion_execution::object_store::ObjectStoreUrl;
    use datafusion_physical_plan::metrics::ExecutionPlanMetricsSet;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};

    use crate::file_stream::{FileOpenFuture, FileOpener, FileStream, OnError};
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
                FileStream::new(&config, 0, Arc::new(self.opener), &metrics_set)
                    .unwrap()
                    .with_on_error(on_error);

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
            let mut stream = FileStream::new_with_morselizer(
                &config,
                0,
                Box::new(self.morselizer),
                &metrics_set,
            )?;

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
        planner_called: PlannerId(1)
        morsel_stream_started: MorselId(12)
        morsel_stream_batch_produced: MorselId(12), BatchId(42)
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
        morsel_produced: PlannerId(3), MorselId(13)
        morsel_stream_batch_produced: MorselId(11), BatchId(41)
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
        planner_called: PlannerId(1)
        morsel_stream_started: MorselId(10)
        morsel_stream_batch_produced: MorselId(10), BatchId(42)
        morsel_stream_finished: MorselId(10)
        morsel_stream_started: MorselId(11)
        morsel_stream_batch_produced: MorselId(11), BatchId(43)
        morsel_stream_finished: MorselId(11)
        ");

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
