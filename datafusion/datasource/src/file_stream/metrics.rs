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

use datafusion_common::instant::Instant;
use datafusion_physical_plan::metrics::{
    Count, ExecutionPlanMetricsSet, MetricBuilder, MetricCategory, Time,
};

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
    /// [`FileStream`]: crate::file_stream::FileStream
    /// [`FileOpener::open`]: crate::file_stream::FileOpener::open
    pub time_opening: StartableTime,
    /// Wall clock time elapsed for file scanning + first record batch of decompression + decoding
    ///
    /// Time between when the [`FileStream`] requests data from the
    /// stream and when the first [`RecordBatch`] is produced.
    ///
    /// [`FileStream`]: crate::file_stream::FileStream
    /// [`RecordBatch`]: arrow::record_batch::RecordBatch
    pub time_scanning_until_data: StartableTime,
    /// Total elapsed wall clock time for scanning + record batch decompression / decoding
    ///
    /// Sum of time between when the [`FileStream`] requests data from
    /// the stream and when a [`RecordBatch`] is produced for all
    /// record batches in the stream. Note that this metric also
    /// includes the time of the parent operator's execution.
    ///
    /// [`FileStream`]: crate::file_stream::FileStream
    /// [`RecordBatch`]: arrow::record_batch::RecordBatch
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

        let file_open_errors = MetricBuilder::new(metrics)
            .with_category(MetricCategory::Rows)
            .counter("file_open_errors", partition);

        let file_scan_errors = MetricBuilder::new(metrics)
            .with_category(MetricCategory::Rows)
            .counter("file_scan_errors", partition);

        let files_opened = MetricBuilder::new(metrics)
            .with_category(MetricCategory::Rows)
            .counter("files_opened", partition);

        let files_processed = MetricBuilder::new(metrics)
            .with_category(MetricCategory::Rows)
            .counter("files_processed", partition);

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
