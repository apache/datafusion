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

use std::collections::VecDeque;
use std::mem;
use std::pin::Pin;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll};
use tokio::sync::Notify;

use crate::PartitionedFile;
use crate::file_scan_config::FileScanConfig;
use arrow::datatypes::SchemaRef;
use datafusion_common::error::Result;
use datafusion_execution::RecordBatchStream;
use datafusion_physical_plan::metrics::{
    BaselineMetrics, Count, ExecutionPlanMetricsSet, MetricBuilder, Time,
};

use arrow::record_batch::RecordBatch;
use datafusion_common::instant::Instant;

use futures::future::BoxFuture;
use futures::stream::BoxStream;
use futures::{FutureExt as _, Stream, StreamExt as _, ready};

/// A guard that decrements the morselizing count when dropped.
struct MorselizingGuard {
    queue: Arc<WorkQueue>,
}

impl Drop for MorselizingGuard {
    fn drop(&mut self) {
        self.queue.stop_morselizing();
    }
}

/// A stream that iterates record batch by record batch, file over file.
pub struct FileStream {
    /// An iterator over input files.
    file_iter: VecDeque<PartitionedFile>,
    /// Shared work queue for morsel-driven execution.
    shared_queue: Option<Arc<WorkQueue>>,
    /// Whether to use morsel-driven execution.
    morsel_driven: bool,
    /// The stream schema (file schema including partition columns and after
    /// projection).
    projected_schema: SchemaRef,
    /// The remaining number of records to parse, None if no limit
    remain: Option<usize>,
    /// A dynamic [`FileOpener`]. Calling `open()` returns a [`FileOpenFuture`],
    /// which can be resolved to a stream of `RecordBatch`.
    file_opener: Arc<dyn FileOpener>,
    /// The stream state
    state: FileStreamState,
    /// File stream specific metrics
    file_stream_metrics: FileStreamMetrics,
    /// runtime baseline metrics
    baseline_metrics: BaselineMetrics,
    /// Describes the behavior of the `FileStream` if file opening or scanning fails
    on_error: OnError,
    /// Guard for morselizing state to ensure counter is decremented on drop
    morsel_guard: Option<MorselizingGuard>,
}

impl FileStream {
    /// Create a new `FileStream` using the give `FileOpener` to scan underlying files
    pub fn new(
        config: &FileScanConfig,
        partition: usize,
        file_opener: Arc<dyn FileOpener>,
        metrics: &ExecutionPlanMetricsSet,
        shared_queue: Option<Arc<WorkQueue>>,
    ) -> Result<Self> {
        let projected_schema = config.projected_schema()?;

        let (file_iter, shared_queue) = if config.morsel_driven {
            (VecDeque::new(), shared_queue)
        } else {
            let file_group = config.file_groups[partition].clone();
            (file_group.into_inner().into_iter().collect(), None)
        };

        Ok(Self {
            file_iter,
            shared_queue,
            morsel_driven: config.morsel_driven,
            projected_schema,
            remain: config.limit,
            file_opener,
            state: FileStreamState::Idle,
            file_stream_metrics: FileStreamMetrics::new(metrics, partition),
            baseline_metrics: BaselineMetrics::new(metrics, partition),
            on_error: OnError::Fail,
            morsel_guard: None,
        })
    }

    /// Specify the behavior when an error occurs opening or scanning a file
    ///
    /// If `OnError::Skip` the stream will skip files which encounter an error and continue
    /// If `OnError:Fail` (default) the stream will fail and stop processing when an error occurs
    pub fn with_on_error(mut self, on_error: OnError) -> Self {
        self.on_error = on_error;
        self
    }

    /// Begin opening the next file in parallel while decoding the current file in FileStream.
    ///
    /// Since file opening is mostly IO (and may involve a
    /// bunch of sequential IO), it can be parallelized with decoding.
    fn start_next_file(&mut self) -> Option<Result<FileOpenFuture>> {
        if self.morsel_driven {
            return None;
        }
        let part_file = self.file_iter.pop_front()?;
        Some(self.file_opener.open(part_file))
    }

    fn poll_inner(&mut self, cx: &mut Context<'_>) -> Poll<Option<Result<RecordBatch>>> {
        loop {
            match &mut self.state {
                FileStreamState::Idle => {
                    self.file_stream_metrics.time_opening.start();

                    if self.morsel_driven {
                        let queue = self.shared_queue.as_ref().expect("shared queue");
                        match queue.pull() {
                            WorkStatus::Work(part_file) => {
                                self.morsel_guard = Some(MorselizingGuard {
                                    queue: Arc::clone(queue),
                                });
                                self.state = FileStreamState::Morselizing {
                                    future: self.file_opener.morselize(*part_file),
                                };
                            }
                            WorkStatus::Wait => {
                                self.file_stream_metrics.time_opening.stop();
                                let queue_captured = Arc::clone(queue);
                                self.state = FileStreamState::Waiting {
                                    future: Box::pin(async move {
                                        let notified = queue_captured.notify.notified();
                                        if !queue_captured.has_work_or_done() {
                                            notified.await;
                                        }
                                    }),
                                };
                            }
                            WorkStatus::Done => {
                                self.file_stream_metrics.time_opening.stop();
                                return Poll::Ready(None);
                            }
                        }
                    } else {
                        match self.start_next_file().transpose() {
                            Ok(Some(future)) => {
                                self.state = FileStreamState::Open { future }
                            }
                            Ok(None) => return Poll::Ready(None),
                            Err(e) => {
                                self.state = FileStreamState::Error;
                                return Poll::Ready(Some(Err(e)));
                            }
                        }
                    }
                }
                FileStreamState::Morselizing { future } => {
                    match ready!(future.poll_unpin(cx)) {
                        Ok(morsels) => {
                            let queue = self.shared_queue.as_ref().expect("shared queue");
                            // Take the guard to decrement morselizing_count
                            let _guard = self.morsel_guard.take();

                            if morsels.len() > 1 {
                                self.file_stream_metrics.time_opening.stop();
                                // Expanded into multiple morsels. Put all back and pull again.
                                queue.push_many(morsels);
                                self.state = FileStreamState::Idle;
                            } else if morsels.len() == 1 {
                                // No further expansion possible. Proceed to open.
                                let morsel = morsels.into_iter().next().unwrap();
                                match self.file_opener.open(morsel) {
                                    Ok(future) => {
                                        self.state = FileStreamState::Open { future }
                                    }
                                    Err(e) => {
                                        self.file_stream_metrics.time_opening.stop();
                                        self.state = FileStreamState::Error;
                                        return Poll::Ready(Some(Err(e)));
                                    }
                                }
                            } else {
                                self.file_stream_metrics.time_opening.stop();
                                // No morsels returned, skip this file
                                self.state = FileStreamState::Idle;
                            }
                        }
                        Err(e) => {
                            let _guard = self.morsel_guard.take();
                            self.file_stream_metrics.time_opening.stop();
                            self.state = FileStreamState::Error;
                            return Poll::Ready(Some(Err(e)));
                        }
                    }
                }
                FileStreamState::Waiting { future } => {
                    ready!(future.poll_unpin(cx));
                    self.state = FileStreamState::Idle;
                }
                FileStreamState::Open { future } => match ready!(future.poll_unpin(cx)) {
                    Ok(reader) => {
                        // include time needed to start opening in `start_next_file`
                        self.file_stream_metrics.time_opening.stop();
                        let next = self.start_next_file().transpose();
                        self.file_stream_metrics.time_scanning_until_data.start();
                        self.file_stream_metrics.time_scanning_total.start();

                        match next {
                            Ok(Some(next_future)) => {
                                self.state = FileStreamState::Scan {
                                    reader,
                                    next: Some(NextOpen::Pending(next_future)),
                                };
                            }
                            Ok(None) => {
                                self.state = FileStreamState::Scan { reader, next: None };
                            }
                            Err(e) => {
                                self.state = FileStreamState::Error;
                                return Poll::Ready(Some(Err(e)));
                            }
                        }
                    }
                    Err(e) => {
                        self.file_stream_metrics.file_open_errors.add(1);
                        match self.on_error {
                            OnError::Skip => {
                                self.file_stream_metrics.time_opening.stop();
                                self.state = FileStreamState::Idle
                            }
                            OnError::Fail => {
                                self.state = FileStreamState::Error;
                                return Poll::Ready(Some(Err(e)));
                            }
                        }
                    }
                },
                FileStreamState::Scan { reader, next } => {
                    // We need to poll the next `FileOpenFuture` here to drive it forward
                    if let Some(next_open_future) = next
                        && let NextOpen::Pending(f) = next_open_future
                        && let Poll::Ready(reader) = f.as_mut().poll(cx)
                    {
                        *next_open_future = NextOpen::Ready(reader);
                    }
                    match ready!(reader.poll_next_unpin(cx)) {
                        Some(Ok(batch)) => {
                            self.file_stream_metrics.time_scanning_until_data.stop();
                            self.file_stream_metrics.time_scanning_total.stop();
                            let batch = match &mut self.remain {
                                Some(remain) => {
                                    if *remain > batch.num_rows() {
                                        *remain -= batch.num_rows();
                                        batch
                                    } else {
                                        let batch = batch.slice(0, *remain);
                                        self.state = FileStreamState::Limit;
                                        *remain = 0;
                                        batch
                                    }
                                }
                                None => batch,
                            };
                            self.file_stream_metrics.time_scanning_total.start();
                            return Poll::Ready(Some(Ok(batch)));
                        }
                        Some(Err(err)) => {
                            self.file_stream_metrics.file_scan_errors.add(1);
                            self.file_stream_metrics.time_scanning_until_data.stop();
                            self.file_stream_metrics.time_scanning_total.stop();

                            match self.on_error {
                                // If `OnError::Skip` we skip the file as soon as we hit the first error
                                OnError::Skip => match mem::take(next) {
                                    Some(future) => {
                                        self.file_stream_metrics.time_opening.start();

                                        match future {
                                            NextOpen::Pending(future) => {
                                                self.state =
                                                    FileStreamState::Open { future }
                                            }
                                            NextOpen::Ready(reader) => {
                                                self.state = FileStreamState::Open {
                                                    future: Box::pin(std::future::ready(
                                                        reader,
                                                    )),
                                                }
                                            }
                                        }
                                    }
                                    None => {
                                        if self.morsel_driven {
                                            self.state = FileStreamState::Idle;
                                        } else {
                                            return Poll::Ready(None);
                                        }
                                    }
                                },
                                OnError::Fail => {
                                    self.state = FileStreamState::Error;
                                    return Poll::Ready(Some(Err(err)));
                                }
                            }
                        }
                        None => {
                            self.file_stream_metrics.time_scanning_until_data.stop();
                            self.file_stream_metrics.time_scanning_total.stop();

                            match mem::take(next) {
                                Some(future) => {
                                    self.file_stream_metrics.time_opening.start();

                                    match future {
                                        NextOpen::Pending(future) => {
                                            self.state = FileStreamState::Open { future }
                                        }
                                        NextOpen::Ready(reader) => {
                                            self.state = FileStreamState::Open {
                                                future: Box::pin(std::future::ready(
                                                    reader,
                                                )),
                                            }
                                        }
                                    }
                                }
                                None => {
                                    if self.morsel_driven {
                                        self.state = FileStreamState::Idle;
                                    } else {
                                        return Poll::Ready(None);
                                    }
                                }
                            }
                        }
                    }
                }
                FileStreamState::Error | FileStreamState::Limit => {
                    return Poll::Ready(None);
                }
            }
        }
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

/// Result of pulling work from the queue
#[derive(Debug)]
pub enum WorkStatus {
    /// A morsel is available
    Work(Box<PartitionedFile>),
    /// No morsel available now, but others are morselizing
    Wait,
    /// No more work available
    Done,
}

/// A shared queue of [`PartitionedFile`] morsels for morsel-driven execution.
#[derive(Debug)]
pub struct WorkQueue {
    queue: Mutex<VecDeque<PartitionedFile>>,
    /// Number of workers currently morselizing a file.
    morselizing_count: AtomicUsize,
    /// Notify waiters when work is added or morselizing finishes.
    notify: Notify,
}

impl WorkQueue {
    /// Create a new `WorkQueue` with the given initial files
    pub fn new(initial_files: Vec<PartitionedFile>) -> Self {
        Self {
            queue: Mutex::new(VecDeque::from(initial_files)),
            morselizing_count: AtomicUsize::new(0),
            notify: Notify::new(),
        }
    }

    /// Pull a file from the queue.
    pub fn pull(&self) -> WorkStatus {
        let mut queue = self.queue.lock().unwrap();
        if let Some(file) = queue.pop_front() {
            self.morselizing_count.fetch_add(1, Ordering::SeqCst);
            WorkStatus::Work(Box::new(file))
        } else if self.morselizing_count.load(Ordering::SeqCst) > 0 {
            WorkStatus::Wait
        } else {
            WorkStatus::Done
        }
    }

    /// Returns true if there is work in the queue or if all morselizing is done.
    pub fn has_work_or_done(&self) -> bool {
        let queue = self.queue.lock().unwrap();
        !queue.is_empty() || self.morselizing_count.load(Ordering::SeqCst) == 0
    }

    /// Push many files back to the queue.
    ///
    /// This is used when a file is expanded into multiple morsels.
    pub fn push_many(&self, files: Vec<PartitionedFile>) {
        if files.is_empty() {
            return;
        }
        self.queue.lock().unwrap().extend(files);
        self.notify.notify_waiters();
    }

    /// Increment the morselizing count.
    pub fn start_morselizing(&self) {
        self.morselizing_count.fetch_add(1, Ordering::SeqCst);
    }

    /// Decrement the morselizing count and notify waiters.
    pub fn stop_morselizing(&self) {
        self.morselizing_count.fetch_sub(1, Ordering::SeqCst);
        self.notify.notify_waiters();
    }

    /// Return true if any worker is currently morselizing.
    pub fn is_morselizing(&self) -> bool {
        self.morselizing_count.load(Ordering::SeqCst) > 0
    }

    /// Return a future that resolves when work is added or morselizing finishes.
    pub async fn wait_for_work(&self) {
        self.notify.notified().await;
    }
}

/// A fallible future that resolves to a stream of [`RecordBatch`]
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
    fn open(&self, partitioned_file: PartitionedFile) -> Result<FileOpenFuture>;

    /// Optional: Split a file into smaller morsels for morsel-driven execution.
    ///
    /// By default, returns the file as a single morsel.
    fn morselize(
        &self,
        file: PartitionedFile,
    ) -> BoxFuture<'static, Result<Vec<PartitionedFile>>> {
        Box::pin(futures::future::ready(Ok(vec![file])))
    }
}

/// Represents the state of the next `FileOpenFuture`. Since we need to poll
/// this future while scanning the current file, we need to store the result if it
/// is ready
pub enum NextOpen {
    Pending(FileOpenFuture),
    Ready(Result<BoxStream<'static, Result<RecordBatch>>>),
}

pub enum FileStreamState {
    /// The idle state, no file is currently being read
    Idle,
    /// Currently performing asynchronous IO to obtain a stream of RecordBatch
    /// for a given file
    Open {
        /// A [`FileOpenFuture`] returned by [`FileOpener::open`]
        future: FileOpenFuture,
    },
    /// Currently splitting a file into smaller morsels.
    Morselizing {
        /// A future that resolves to a list of morsels
        future: BoxFuture<'static, Result<Vec<PartitionedFile>>>,
    },
    /// Waiting for more work to be added to the queue.
    Waiting {
        /// A future that resolves when more work is available
        future: BoxFuture<'static, ()>,
    },
    /// Scanning the [`BoxStream`] returned by the completion of a [`FileOpenFuture`]
    /// returned by [`FileOpener::open`]
    Scan {
        /// The reader instance
        reader: BoxStream<'static, Result<RecordBatch>>,
        /// A [`FileOpenFuture`] for the next file to be processed.
        /// This allows the next file to be opened in parallel while the
        /// current file is read.
        next: Option<NextOpen>,
    },
    /// Encountered an error
    Error,
    /// Reached the row limit
    Limit,
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

        Self {
            time_opening,
            time_scanning_until_data,
            time_scanning_total,
            time_processing,
            file_open_errors,
            file_scan_errors,
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::PartitionedFile;
    use crate::file_scan_config::FileScanConfigBuilder;
    use crate::tests::make_partition;
    use datafusion_common::error::Result;
    use datafusion_execution::object_store::ObjectStoreUrl;
    use datafusion_physical_plan::metrics::ExecutionPlanMetricsSet;
    use futures::{FutureExt as _, StreamExt as _};
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};

    use crate::file_stream::{FileOpenFuture, FileOpener, FileStream, OnError};
    use crate::test_util::MockSource;
    use arrow::array::RecordBatch;
    use arrow::datatypes::Schema;

    use datafusion_common::{assert_batches_eq, exec_err, internal_err};

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

            let table_schema = crate::table_schema::TableSchema::new(file_schema, vec![]);
            let config = FileScanConfigBuilder::new(
                ObjectStoreUrl::parse("test:///").unwrap(),
                Arc::new(MockSource::new(table_schema)),
            )
            .with_file_group(file_group)
            .with_limit(self.limit)
            .build();
            let metrics_set = ExecutionPlanMetricsSet::new();
            let file_stream =
                FileStream::new(&config, 0, Arc::new(self.opener), &metrics_set, None)
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
