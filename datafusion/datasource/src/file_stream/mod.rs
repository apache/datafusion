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

mod builder;
mod metrics;

use std::collections::VecDeque;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use crate::PartitionedFile;
use crate::file_scan_config::FileScanConfig;
use arrow::datatypes::SchemaRef;
use datafusion_common::Result;
use datafusion_execution::RecordBatchStream;
use datafusion_physical_plan::metrics::{BaselineMetrics, ExecutionPlanMetricsSet};

use arrow::record_batch::RecordBatch;

use futures::future::BoxFuture;
use futures::stream::BoxStream;
use futures::{FutureExt as _, Stream, StreamExt as _, ready};

pub use builder::FileStreamBuilder;
pub use metrics::{FileStreamMetrics, StartableTime};

/// A stream that iterates record batch by record batch, file over file.
pub struct FileStream {
    /// An iterator over input files.
    file_iter: VecDeque<PartitionedFile>,
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
}

impl FileStream {
    /// Create a new `FileStream` using the give `FileOpener` to scan underlying files
    #[deprecated(since = "54.0.0", note = "Use FileStreamBuilder instead")]
    pub fn new(
        config: &FileScanConfig,
        partition: usize,
        file_opener: Arc<dyn FileOpener>,
        metrics: &ExecutionPlanMetricsSet,
    ) -> Result<Self> {
        FileStreamBuilder::new(config)
            .with_partition(partition)
            .with_file_opener(file_opener)
            .with_metrics(metrics)
            .build()
    }

    /// Specify the behavior when an error occurs opening or scanning a file
    ///
    /// If `OnError::Skip` the stream will skip files which encounter an error and continue
    /// If `OnError:Fail` (default) the stream will fail and stop processing when an error occurs
    pub fn with_on_error(mut self, on_error: OnError) -> Self {
        self.on_error = on_error;
        self
    }

    fn start_next_file(&mut self) -> Option<Result<FileOpenFuture>> {
        let part_file = self.file_iter.pop_front()?;
        Some(self.file_opener.open(part_file))
    }

    fn poll_inner(&mut self, cx: &mut Context<'_>) -> Poll<Option<Result<RecordBatch>>> {
        loop {
            match &mut self.state {
                FileStreamState::Idle => match self.start_next_file().transpose() {
                    Ok(Some(future)) => {
                        self.file_stream_metrics.time_opening.start();
                        self.state = FileStreamState::Open { future };
                    }
                    Ok(None) => return Poll::Ready(None),
                    Err(e) => {
                        self.state = FileStreamState::Error;
                        return Poll::Ready(Some(Err(e)));
                    }
                },
                FileStreamState::Open { future } => match ready!(future.poll_unpin(cx)) {
                    Ok(reader) => {
                        self.file_stream_metrics.files_opened.add(1);
                        self.file_stream_metrics.time_opening.stop();
                        self.file_stream_metrics.time_scanning_until_data.start();
                        self.file_stream_metrics.time_scanning_total.start();
                        self.state = FileStreamState::Scan { reader };
                    }
                    Err(e) => {
                        self.file_stream_metrics.file_open_errors.add(1);
                        match self.on_error {
                            OnError::Skip => {
                                self.file_stream_metrics.files_processed.add(1);
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
                FileStreamState::Scan { reader } => {
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
                                        // Count this file and all remaining files
                                        // we will never open.
                                        let done = 1 + self.file_iter.len();
                                        self.file_stream_metrics
                                            .files_processed
                                            .add(done);
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
                                OnError::Skip => {
                                    self.file_stream_metrics.files_processed.add(1);
                                    self.state = FileStreamState::Idle;
                                }
                                OnError::Fail => {
                                    self.state = FileStreamState::Error;
                                    return Poll::Ready(Some(Err(err)));
                                }
                            }
                        }
                        None => {
                            self.file_stream_metrics.files_processed.add(1);
                            self.file_stream_metrics.time_scanning_until_data.stop();
                            self.file_stream_metrics.time_scanning_total.stop();
                            self.state = FileStreamState::Idle;
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
    /// Scanning the [`BoxStream`] returned by the completion of a [`FileOpenFuture`]
    /// returned by [`FileOpener::open`]
    Scan {
        /// The reader instance
        reader: BoxStream<'static, Result<RecordBatch>>,
    },
    /// Encountered an error
    Error,
    /// Reached the row limit
    Limit,
}

/// A timer that can be started and stopped.
#[cfg(test)]
mod tests {
    use crate::file_scan_config::{FileScanConfig, FileScanConfigBuilder};
    use crate::tests::make_partition;
    use crate::{PartitionedFile, TableSchema};
    use datafusion_common::error::Result;
    use datafusion_execution::object_store::ObjectStoreUrl;
    use datafusion_physical_plan::metrics::ExecutionPlanMetricsSet;
    use futures::{FutureExt as _, StreamExt as _};
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};

    use crate::file_stream::{FileOpenFuture, FileOpener, FileStreamBuilder, OnError};
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

            let table_schema = TableSchema::new(file_schema, vec![]);
            let config = FileScanConfigBuilder::new(
                ObjectStoreUrl::parse("test:///").unwrap(),
                Arc::new(MockSource::new(table_schema)),
            )
            .with_file_group(file_group)
            .with_limit(self.limit)
            .build();
            let metrics_set = ExecutionPlanMetricsSet::new();
            let file_stream = FileStreamBuilder::new(&config)
                .with_partition(0)
                .with_file_opener(Arc::new(self.opener))
                .with_metrics(&metrics_set)
                .with_on_error(on_error)
                .build()?;

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

    /// Create the smallest valid file scan config for builder validation tests.
    fn builder_test_config() -> FileScanConfig {
        let table_schema = TableSchema::new(Arc::new(Schema::empty()), vec![]);
        FileScanConfigBuilder::new(
            ObjectStoreUrl::parse("test:///").unwrap(),
            Arc::new(MockSource::new(table_schema)),
        )
        .with_file(PartitionedFile::new("mock_file", 10))
        .build()
    }

    /// Convenience helper to keep builder error assertions focused on the
    /// specific missing or invalid input under test.
    fn builder_error(builder: FileStreamBuilder<'_>) -> String {
        builder.build().err().unwrap().to_string()
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

    #[test]
    fn builder_requires_partition_file_opener_and_metrics() {
        let config = builder_test_config();

        let err = builder_error(FileStreamBuilder::new(&config));
        assert!(err.contains("FileStreamBuilder missing required partition"));

        let err = builder_error(FileStreamBuilder::new(&config).with_partition(0));
        assert!(err.contains("FileStreamBuilder missing required file_opener"));

        let err = builder_error(
            FileStreamBuilder::new(&config)
                .with_partition(0)
                .with_file_opener(Arc::new(TestOpener::default())),
        );
        assert!(err.contains("FileStreamBuilder missing required metrics"));
    }

    #[test]
    fn builder_errors_on_invalid_partition() {
        let config = builder_test_config();
        let metrics = ExecutionPlanMetricsSet::new();

        let err = builder_error(
            FileStreamBuilder::new(&config)
                .with_partition(1)
                .with_file_opener(Arc::new(TestOpener::default()))
                .with_metrics(&metrics),
        );
        assert!(err.contains("FileStreamBuilder invalid partition index: 1"));
    }
}
