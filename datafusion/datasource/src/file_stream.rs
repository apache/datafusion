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

use crate::file_meta::FileMeta;
use datafusion_common::error::Result;
use datafusion_physical_plan::metrics::{
    Count, ExecutionPlanMetricsSet, MetricBuilder, Time,
};

use arrow::error::ArrowError;
use arrow::record_batch::RecordBatch;
use datafusion_common::instant::Instant;
use datafusion_common::ScalarValue;

use futures::future::BoxFuture;
use futures::stream::BoxStream;

/// A fallible future that resolves to a stream of [`RecordBatch`]
pub type FileOpenFuture =
    BoxFuture<'static, Result<BoxStream<'static, Result<RecordBatch, ArrowError>>>>;

/// Describes the behavior of the `FileStream` if file opening or scanning fails
pub enum OnError {
    /// Fail the entire stream and return the underlying error
    Fail,
    /// Continue scanning, ignoring the failed file
    Skip,
}

impl Default for OnError {
    fn default() -> Self {
        Self::Fail
    }
}

/// Generic API for opening a file using an [`ObjectStore`] and resolving to a
/// stream of [`RecordBatch`]
///
/// [`ObjectStore`]: object_store::ObjectStore
pub trait FileOpener: Unpin + Send + Sync {
    /// Asynchronously open the specified file and return a stream
    /// of [`RecordBatch`]
    fn open(&self, file_meta: FileMeta) -> Result<FileOpenFuture>;
}

/// Represents the state of the next `FileOpenFuture`. Since we need to poll
/// this future while scanning the current file, we need to store the result if it
/// is ready
pub enum NextOpen {
    Pending(FileOpenFuture),
    Ready(Result<BoxStream<'static, Result<RecordBatch, ArrowError>>>),
}

pub enum FileStreamState {
    /// The idle state, no file is currently being read
    Idle,
    /// Currently performing asynchronous IO to obtain a stream of RecordBatch
    /// for a given file
    Open {
        /// A [`FileOpenFuture`] returned by [`FileOpener::open`]
        future: FileOpenFuture,
        /// The partition values for this file
        partition_values: Vec<ScalarValue>,
    },
    /// Scanning the [`BoxStream`] returned by the completion of a [`FileOpenFuture`]
    /// returned by [`FileOpener::open`]
    Scan {
        /// Partitioning column values for the current batch_iter
        partition_values: Vec<ScalarValue>,
        /// The reader instance
        reader: BoxStream<'static, Result<RecordBatch, ArrowError>>,
        /// A [`FileOpenFuture`] for the next file to be processed,
        /// and its corresponding partition column values, if any.
        /// This allows the next file to be opened in parallel while the
        /// current file is read.
        next: Option<(NextOpen, Vec<ScalarValue>)>,
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

#[allow(rustdoc::broken_intra_doc_links)]
/// Metrics for [`FileStream`]
///
/// Note that all of these metrics are in terms of wall clock time
/// (not cpu time) so they include time spent waiting on I/O as well
/// as other operators.
///
/// [`FileStream`]: <https://github.com/apache/datafusion/blob/main/datafusion/core/src/datasource/physical_plan/file_stream.rs>
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
    /// [`FileStream`]: <https://github.com/apache/datafusion/blob/main/datafusion/core/src/datasource/physical_plan/file_stream.rs>
    pub time_opening: StartableTime,
    /// Wall clock time elapsed for file scanning + first record batch of decompression + decoding
    ///
    /// Time between when the [`FileStream`] requests data from the
    /// stream and when the first [`RecordBatch`] is produced.
    /// [`FileStream`]: <https://github.com/apache/datafusion/blob/main/datafusion/core/src/datasource/physical_plan/file_stream.rs>
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
