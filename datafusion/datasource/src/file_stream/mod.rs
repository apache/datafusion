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
mod scan_state;
pub(crate) mod work_source;

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

use futures::Stream;
use futures::future::BoxFuture;
use futures::stream::BoxStream;

use self::scan_state::{ScanAndReturn, ScanState};

pub use builder::FileStreamBuilder;
pub use metrics::{FileStreamMetrics, StartableTime};

/// A stream that iterates record batch by record batch, file over file.
pub struct FileStream {
    /// The stream schema (file schema including partition columns and after
    /// projection).
    projected_schema: SchemaRef,
    /// The stream state
    state: FileStreamState,
    /// runtime baseline metrics
    baseline_metrics: BaselineMetrics,
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
        match &mut self.state {
            FileStreamState::Scan { scan_state } => scan_state.set_on_error(on_error),
            FileStreamState::Error | FileStreamState::Done => {
                // no effect as there are no more files to process
            }
        };
        self
    }

    fn poll_inner(&mut self, cx: &mut Context<'_>) -> Poll<Option<Result<RecordBatch>>> {
        loop {
            match &mut self.state {
                FileStreamState::Scan { scan_state: queue } => {
                    let action = queue.poll_scan(cx);
                    match action {
                        ScanAndReturn::Continue => continue,
                        ScanAndReturn::Done(result) => {
                            self.state = FileStreamState::Done;
                            return Poll::Ready(result);
                        }
                        ScanAndReturn::Error(err) => {
                            self.state = FileStreamState::Error;
                            return Poll::Ready(Some(Err(err)));
                        }
                        ScanAndReturn::Return(result) => return result,
                    }
                }
                FileStreamState::Error | FileStreamState::Done => {
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
        let result = self.poll_inner(cx);
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

enum FileStreamState {
    /// Actively processing readers, ready morsels, and planner work.
    Scan {
        /// The ready queues and active reader for the current file.
        scan_state: Box<ScanState>,
    },
    /// Encountered an error
    Error,
    /// Finished scanning all requested data, possibly because a limit was reached
    Done,
}

#[cfg(test)]
mod tests {
    use crate::file_scan_config::{FileScanConfig, FileScanConfigBuilder};
    use crate::morsel::mocks::{
        IoFutureId, MockMorselizer, MockPlanBuilder, MockPlanner, MorselId,
        PendingPlannerBuilder, PollsToResolve,
    };
    use crate::source::DataSource;
    use crate::tests::make_partition;
    use crate::{PartitionedFile, TableSchema};
    use arrow::array::{AsArray, RecordBatch};
    use arrow::datatypes::{DataType, Field, Int32Type, Schema};
    use datafusion_common::DataFusionError;
    use datafusion_common::error::Result;
    use datafusion_execution::object_store::ObjectStoreUrl;
    use datafusion_physical_plan::metrics::ExecutionPlanMetricsSet;
    use futures::{FutureExt as _, StreamExt as _};
    use std::collections::{BTreeMap, VecDeque};
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};

    use crate::file_stream::{
        FileOpenFuture, FileOpener, FileStream, FileStreamBuilder, OnError,
        work_source::SharedWorkSource,
    };
    use crate::test_util::MockSource;

    use datafusion_common::{assert_batches_eq, exec_err, internal_err};

    /// Test identifier for one `FileStream` partition.
    #[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
    struct PartitionId(usize);

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
        assert!(err.contains("FileStreamBuilder missing required morselizer"));

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

    /// Verifies the simplest morsel-driven flow: one planner produces one
    /// morsel immediately, and that morsel is then scanned to completion.
    #[tokio::test]
    async fn morsel_no_io() -> Result<()> {
        let test = FileStreamMorselTest::new().with_file(
            MockPlanner::builder("file1.parquet")
                .add_plan(MockPlanBuilder::new().with_morsel(MorselId(10), 42))
                .return_none(),
        );

        insta::assert_snapshot!(test.run().await.unwrap(), @r"
        ----- Output Stream -----
        Batch: 42
        Done
        ----- File Stream Events -----
        morselize_file: file1.parquet
        planner_created: file1.parquet
        planner_called: file1.parquet
        morsel_produced: file1.parquet, MorselId(10)
        morsel_stream_started: MorselId(10)
        morsel_stream_batch_produced: MorselId(10), BatchId(42)
        morsel_stream_finished: MorselId(10)
        ");

        Ok(())
    }

    /// Verifies that a planner can block on one I/O phase and then produce a
    /// morsel containing two batches.
    #[tokio::test]
    async fn morsel_single_io_two_batches() -> Result<()> {
        let test = FileStreamMorselTest::new().with_file(
            MockPlanner::builder("file1.parquet")
                .add_plan(
                    PendingPlannerBuilder::new(IoFutureId(1))
                        .with_polls_to_resolve(PollsToResolve(1)),
                )
                .add_plan(
                    MockPlanBuilder::new()
                        .with_morsel_batches(MorselId(10), vec![42, 43]),
                )
                .return_none(),
        );

        insta::assert_snapshot!(test.run().await.unwrap(), @r"
        ----- Output Stream -----
        Batch: 42
        Batch: 43
        Done
        ----- File Stream Events -----
        morselize_file: file1.parquet
        planner_created: file1.parquet
        planner_called: file1.parquet
        io_future_created: file1.parquet, IoFutureId(1)
        io_future_polled: file1.parquet, IoFutureId(1)
        io_future_polled: file1.parquet, IoFutureId(1)
        io_future_resolved: file1.parquet, IoFutureId(1)
        planner_called: file1.parquet
        morsel_produced: file1.parquet, MorselId(10)
        morsel_stream_started: MorselId(10)
        morsel_stream_batch_produced: MorselId(10), BatchId(42)
        morsel_stream_batch_produced: MorselId(10), BatchId(43)
        morsel_stream_finished: MorselId(10)
        ");

        Ok(())
    }

    /// Verifies that a planner can traverse two sequential I/O phases before
    /// producing one batch, similar to Parquet.
    #[tokio::test]
    async fn morsel_two_ios_one_batch() -> Result<()> {
        let test = FileStreamMorselTest::new().with_file(
            MockPlanner::builder("file1.parquet")
                .add_plan(PendingPlannerBuilder::new(IoFutureId(1)))
                .add_plan(PendingPlannerBuilder::new(IoFutureId(2)))
                .add_plan(MockPlanBuilder::new().with_morsel(MorselId(10), 42))
                .return_none(),
        );

        insta::assert_snapshot!(test.run().await.unwrap(), @r"
        ----- Output Stream -----
        Batch: 42
        Done
        ----- File Stream Events -----
        morselize_file: file1.parquet
        planner_created: file1.parquet
        planner_called: file1.parquet
        io_future_created: file1.parquet, IoFutureId(1)
        io_future_polled: file1.parquet, IoFutureId(1)
        io_future_resolved: file1.parquet, IoFutureId(1)
        planner_called: file1.parquet
        io_future_created: file1.parquet, IoFutureId(2)
        io_future_polled: file1.parquet, IoFutureId(2)
        io_future_resolved: file1.parquet, IoFutureId(2)
        planner_called: file1.parquet
        morsel_produced: file1.parquet, MorselId(10)
        morsel_stream_started: MorselId(10)
        morsel_stream_batch_produced: MorselId(10), BatchId(42)
        morsel_stream_finished: MorselId(10)
        ");

        Ok(())
    }

    /// Verifies that a planner I/O future can fail and terminate the stream.
    #[tokio::test]
    async fn morsel_io_error() -> Result<()> {
        let test = FileStreamMorselTest::new().with_file(
            MockPlanner::builder("file1.parquet").add_plan(
                PendingPlannerBuilder::new(IoFutureId(1))
                    .with_error("io failed while opening file"),
            ),
        );

        insta::assert_snapshot!(test.run().await.unwrap(), @r"
        ----- Output Stream -----
        Error: io failed while opening file
        Done
        ----- File Stream Events -----
        morselize_file: file1.parquet
        planner_created: file1.parquet
        planner_called: file1.parquet
        io_future_created: file1.parquet, IoFutureId(1)
        io_future_polled: file1.parquet, IoFutureId(1)
        io_future_errored: file1.parquet, IoFutureId(1), io failed while opening file
        ");

        Ok(())
    }

    /// Verifies that pending planner I/O does not block draining the current
    /// morsel stream.
    #[tokio::test]
    async fn morsel_pending_planner_does_not_block_active_reader() -> Result<()> {
        let test = FileStreamMorselTest::new().with_file(
            MockPlanner::builder("file1.parquet")
                .add_plan(
                    MockPlanBuilder::new()
                        .with_morsel_batches(MorselId(10), vec![41, 42])
                        .with_pending_planner(IoFutureId(1), PollsToResolve(3), Ok(())),
                )
                .add_plan(MockPlanBuilder::new().with_morsel(MorselId(11), 43))
                .return_none(),
        );

        // The key events are:
        // 1. the first `planner_called` produces `MorselId(10)` and creates `IoFutureId(1)`
        // 2. `MorselId(10)` continues yielding both batches while that I/O is pending
        // 3. after the I/O resolves, planning resumes and yields `MorselId(11)`
        insta::assert_snapshot!(test.run().await.unwrap(), @r"
        ----- Output Stream -----
        Batch: 41
        Batch: 42
        Batch: 43
        Done
        ----- File Stream Events -----
        morselize_file: file1.parquet
        planner_created: file1.parquet
        planner_called: file1.parquet
        morsel_produced: file1.parquet, MorselId(10)
        io_future_created: file1.parquet, IoFutureId(1)
        io_future_polled: file1.parquet, IoFutureId(1)
        morsel_stream_started: MorselId(10)
        io_future_polled: file1.parquet, IoFutureId(1)
        morsel_stream_batch_produced: MorselId(10), BatchId(41)
        io_future_polled: file1.parquet, IoFutureId(1)
        morsel_stream_batch_produced: MorselId(10), BatchId(42)
        io_future_polled: file1.parquet, IoFutureId(1)
        io_future_resolved: file1.parquet, IoFutureId(1)
        morsel_stream_finished: MorselId(10)
        planner_called: file1.parquet
        morsel_produced: file1.parquet, MorselId(11)
        morsel_stream_started: MorselId(11)
        morsel_stream_batch_produced: MorselId(11), BatchId(43)
        morsel_stream_finished: MorselId(11)
        ");

        Ok(())
    }

    /// Verifies that one `plan()` call can return a ready child planner, which
    /// is then called to produce the morsel.
    #[tokio::test]
    async fn morsel_ready_child_planner() -> Result<()> {
        let child_planner = MockPlanner::builder("child planner")
            .add_plan(MockPlanBuilder::new().with_morsel(MorselId(10), 42))
            .return_none();

        let test = FileStreamMorselTest::new().with_file(
            MockPlanner::builder("file1.parquet")
                .add_plan(MockPlanBuilder::new().with_ready_planner(child_planner))
                .return_none(),
        );

        insta::assert_snapshot!(test.run().await.unwrap(), @r"
        ----- Output Stream -----
        Batch: 42
        Done
        ----- File Stream Events -----
        morselize_file: file1.parquet
        planner_created: file1.parquet
        planner_called: file1.parquet
        planner_created: child planner
        planner_called: child planner
        morsel_produced: child planner, MorselId(10)
        morsel_stream_started: MorselId(10)
        morsel_stream_batch_produced: MorselId(10), BatchId(42)
        morsel_stream_finished: MorselId(10)
        ");

        Ok(())
    }

    /// Verifies that planning can fail after a successful I/O phase.
    #[tokio::test]
    async fn morsel_plan_error_after_io() -> Result<()> {
        let test = FileStreamMorselTest::new().with_file(
            MockPlanner::builder("file1.parquet")
                .add_plan(PendingPlannerBuilder::new(IoFutureId(1)))
                .return_error("planner failed after io"),
        );

        insta::assert_snapshot!(test.run().await.unwrap(), @r"
        ----- Output Stream -----
        Error: planner failed after io
        Done
        ----- File Stream Events -----
        morselize_file: file1.parquet
        planner_created: file1.parquet
        planner_called: file1.parquet
        io_future_created: file1.parquet, IoFutureId(1)
        io_future_polled: file1.parquet, IoFutureId(1)
        io_future_resolved: file1.parquet, IoFutureId(1)
        planner_called: file1.parquet
        ");

        Ok(())
    }

    /// Verifies that `FileStream` scans multiple files in order.
    #[tokio::test]
    async fn morsel_multiple_files() -> Result<()> {
        let test = FileStreamMorselTest::new()
            .with_file(
                MockPlanner::builder("file1.parquet")
                    .add_plan(MockPlanBuilder::new().with_morsel(MorselId(10), 41))
                    .return_none(),
            )
            .with_file(
                MockPlanner::builder("file2.parquet")
                    .add_plan(MockPlanBuilder::new().with_morsel(MorselId(11), 42))
                    .return_none(),
            );

        insta::assert_snapshot!(test.run().await.unwrap(), @r"
        ----- Output Stream -----
        Batch: 41
        Batch: 42
        Done
        ----- File Stream Events -----
        morselize_file: file1.parquet
        planner_created: file1.parquet
        planner_called: file1.parquet
        morsel_produced: file1.parquet, MorselId(10)
        morsel_stream_started: MorselId(10)
        morsel_stream_batch_produced: MorselId(10), BatchId(41)
        morsel_stream_finished: MorselId(10)
        morselize_file: file2.parquet
        planner_created: file2.parquet
        planner_called: file2.parquet
        morsel_produced: file2.parquet, MorselId(11)
        morsel_stream_started: MorselId(11)
        morsel_stream_batch_produced: MorselId(11), BatchId(42)
        morsel_stream_finished: MorselId(11)
        ");

        Ok(())
    }

    /// Verifies that a global limit can stop the stream before a second file is opened.
    #[tokio::test]
    async fn morsel_limit_prevents_second_file() -> Result<()> {
        let test = FileStreamMorselTest::new()
            .with_file(
                MockPlanner::builder("file1.parquet")
                    .add_plan(
                        MockPlanBuilder::new()
                            .with_morsel_batches(MorselId(10), vec![41, 42]),
                    )
                    .return_none(),
            )
            .with_file(
                MockPlanner::builder("file2.parquet")
                    .add_plan(MockPlanBuilder::new().with_morsel(MorselId(11), 43))
                    .return_none(),
            )
            .with_limit(1);

        // Note the snapshot should not ever see planner id2
        insta::assert_snapshot!(test.run().await.unwrap(), @r"
        ----- Output Stream -----
        Batch: 41
        Done
        ----- File Stream Events -----
        morselize_file: file1.parquet
        planner_created: file1.parquet
        planner_called: file1.parquet
        morsel_produced: file1.parquet, MorselId(10)
        morsel_stream_started: MorselId(10)
        morsel_stream_batch_produced: MorselId(10), BatchId(41)
        ");

        Ok(())
    }

    /// Return a morsel test with two partitions:
    /// Partition 0: file1, file2, file3
    /// Partition 1: file4
    ///
    /// Partition 1 has only 1 file but it polled first 4 times
    fn two_partition_morsel_test() -> FileStreamMorselTest {
        FileStreamMorselTest::new()
            // Partition 0 has three files
            .with_file_in_partition(
                PartitionId(0),
                MockPlanner::builder("file1.parquet")
                    .add_plan(MockPlanBuilder::new().with_morsel(MorselId(10), 101))
                    .return_none(),
            )
            .with_file_in_partition(
                PartitionId(0),
                MockPlanner::builder("file2.parquet")
                    .add_plan(MockPlanBuilder::new().with_morsel(MorselId(11), 102))
                    .return_none(),
            )
            .with_file_in_partition(
                PartitionId(0),
                MockPlanner::builder("file3.parquet")
                    .add_plan(MockPlanBuilder::new().with_morsel(MorselId(12), 103))
                    .return_none(),
            )
            // Partition 1 has only one file, but is polled first
            .with_file_in_partition(
                PartitionId(1),
                MockPlanner::builder("file4.parquet")
                    .add_plan(MockPlanBuilder::new().with_morsel(MorselId(13), 201))
                    .return_none(),
            )
            .with_reads(vec![
                PartitionId(1),
                PartitionId(1),
                PartitionId(1),
                PartitionId(1),
                PartitionId(1),
            ])
    }

    /// Verifies that an idle sibling stream can steal shared files from
    /// another stream once it exhausts its own local work.
    #[tokio::test]
    async fn morsel_shared_files_can_be_stolen() -> Result<()> {
        let test = two_partition_morsel_test().with_file_stream_events(false);

        // Partition 0 starts with 3 files, but Partition 1 is polled first.
        // Since Partition 1 is polled first, it will run all the files even those
        // that were assigned to Partition 0.
        insta::assert_snapshot!(test.run().await.unwrap(), @r"
        ----- Partition 0 -----
        Done
        ----- Partition 1 -----
        Batch: 101
        Batch: 102
        Batch: 103
        Batch: 201
        Done
        ----- File Stream Events -----
        (omitted due to with_file_stream_events(false))
        ");

        Ok(())
    }

    /// Verifies that a stream that must preserve order keeps its files local
    /// and therefore cannot steal from a sibling shared queue.
    #[tokio::test]
    async fn morsel_preserve_order_keeps_files_local() -> Result<()> {
        // same fixture as `morsel_shared_files_can_be_stolen` but marked as
        // preserve-order
        let test = two_partition_morsel_test()
            .with_preserve_order(true)
            .with_file_stream_events(false);

        // Even though that Partition 1 is polled first, it can not steal files
        // from partition 0. The three files originally assigned to Partition 0
        // must be evaluated by Partition 0.
        insta::assert_snapshot!(test.run().await.unwrap(), @r"
        ----- Partition 0 -----
        Batch: 101
        Batch: 102
        Batch: 103
        Done
        ----- Partition 1 -----
        Batch: 201
        Done
        ----- File Stream Events -----
        (omitted due to with_file_stream_events(false))
        ");

        Ok(())
    }

    /// Verifies that `partitioned_by_file_group` disables shared work stealing.
    #[tokio::test]
    async fn morsel_partitioned_by_file_group_keeps_files_local() -> Result<()> {
        // same fixture as `morsel_shared_files_can_be_stolen` but marked as
        // preserve-partitioned
        let test = two_partition_morsel_test()
            .with_partitioned_by_file_group(true)
            .with_file_stream_events(false);

        insta::assert_snapshot!(test.run().await.unwrap(), @r"
        ----- Partition 0 -----
        Batch: 101
        Batch: 102
        Batch: 103
        Done
        ----- Partition 1 -----
        Batch: 201
        Done
        ----- File Stream Events -----
        (omitted due to with_file_stream_events(false))
        ");

        Ok(())
    }

    /// Verifies that an empty sibling can immediately steal shared files when
    /// it is polled before the stream that originally owned them.
    #[tokio::test]
    async fn morsel_empty_sibling_can_steal() -> Result<()> {
        let test = FileStreamMorselTest::new()
            .with_file_in_partition(
                PartitionId(0),
                MockPlanner::builder("file1.parquet")
                    .add_plan(MockPlanBuilder::new().with_morsel(MorselId(10), 101))
                    .return_none(),
            )
            .with_file_in_partition(
                PartitionId(0),
                MockPlanner::builder("file2.parquet")
                    .add_plan(MockPlanBuilder::new().with_morsel(MorselId(11), 102))
                    .return_none(),
            )
            // Poll the empty sibling first so it steals both files.
            .with_reads(vec![PartitionId(1), PartitionId(1), PartitionId(1)])
            .with_file_stream_events(false);

        insta::assert_snapshot!(test.run().await.unwrap(), @r"
        ----- Partition 0 -----
        Done
        ----- Partition 1 -----
        Batch: 101
        Batch: 102
        Done
        ----- File Stream Events -----
        (omitted due to with_file_stream_events(false))
        ");

        Ok(())
    }

    /// Ensures that if a sibling is built and polled
    /// before another sibling has been built and contributed its files to the
    /// shared queue, the first sibling does not finish prematurely.
    #[tokio::test]
    async fn morsel_empty_sibling_can_finish_before_shared_work_exists() -> Result<()> {
        let test = FileStreamMorselTest::new()
            .with_file_in_partition(
                PartitionId(0),
                MockPlanner::builder("file1.parquet")
                    .add_plan(MockPlanBuilder::new().with_morsel(MorselId(10), 101))
                    .return_none(),
            )
            .with_file_in_partition(
                PartitionId(0),
                MockPlanner::builder("file2.parquet")
                    .add_plan(MockPlanBuilder::new().with_morsel(MorselId(11), 102))
                    .return_none(),
            )
            // Build streams lazily so partition 1 can poll the shared queue
            // before partition 0 has contributed its files. Once partition 0
            // is built, a later poll of partition 1 can still steal one of
            // them from the shared queue.
            .with_build_streams_on_first_read(true)
            .with_reads(vec![PartitionId(1), PartitionId(0), PartitionId(1)])
            .with_file_stream_events(false);

        // Partition 1 polls too early once, then later steals one file after
        // partition 0 has populated the shared queue.
        insta::assert_snapshot!(test.run().await.unwrap(), @r"
        ----- Partition 0 -----
        Batch: 102
        Done
        ----- Partition 1 -----
        Batch: 101
        Done
        ----- File Stream Events -----
        (omitted due to with_file_stream_events(false))
        ");

        Ok(())
    }

    /// Verifies that a sibling hitting its limit does not count shared files
    /// left in the queue as already processed by that stream.
    #[tokio::test]
    async fn morsel_shared_limit_does_not_double_count_files_processed() -> Result<()> {
        let test = two_partition_morsel_test();
        let unlimited_config = test.test_config();
        let limited_config = test.clone().with_limit(1).test_config();
        let shared_work_source = limited_config
            .create_sibling_state()
            .and_then(|state| state.as_ref().downcast_ref::<SharedWorkSource>().cloned())
            .expect("shared work source");
        let limited_metrics = ExecutionPlanMetricsSet::new();
        let unlimited_metrics = ExecutionPlanMetricsSet::new();

        let limited_stream = FileStreamBuilder::new(&limited_config)
            .with_partition(1)
            .with_shared_work_source(Some(shared_work_source.clone()))
            .with_morselizer(Box::new(test.morselizer.clone()))
            .with_metrics(&limited_metrics)
            .build()?;

        let unlimited_stream = FileStreamBuilder::new(&unlimited_config)
            .with_partition(0)
            .with_shared_work_source(Some(shared_work_source))
            .with_morselizer(Box::new(test.morselizer))
            .with_metrics(&unlimited_metrics)
            .build()?;

        let limited_output = drain_stream_output(limited_stream).await?;
        let unlimited_output = drain_stream_output(unlimited_stream).await?;

        insta::assert_snapshot!(format!(
            "----- Limited Stream -----\n{limited_output}\n----- Unlimited Stream -----\n{unlimited_output}"
        ), @r"
        ----- Limited Stream -----
        Batch: 101
        ----- Unlimited Stream -----
        Batch: 102
        Batch: 103
        Batch: 201
        ");

        assert_eq!(
            metric_count(&limited_metrics, "files_opened"),
            1,
            "the limited stream should only open the file that produced its output"
        );
        assert_eq!(
            metric_count(&limited_metrics, "files_processed"),
            1,
            "the limited stream should only mark its own file as processed"
        );
        assert_eq!(
            metric_count(&unlimited_metrics, "files_opened"),
            3,
            "the draining stream should open the remaining shared files"
        );
        assert_eq!(
            metric_count(&unlimited_metrics, "files_processed"),
            3,
            "the draining stream should process exactly the files it opened"
        );

        Ok(())
    }

    /// Verifies that one fast sibling can drain shared files that originated
    /// in more than one other partition.
    #[tokio::test]
    async fn morsel_one_sibling_can_drain_multiple_siblings() -> Result<()> {
        let test = FileStreamMorselTest::new()
            .with_file_in_partition(
                PartitionId(0),
                MockPlanner::builder("file1.parquet")
                    .add_plan(MockPlanBuilder::new().with_morsel(MorselId(10), 101))
                    .return_none(),
            )
            // Partition 1 has two files
            .with_file_in_partition(
                PartitionId(1),
                MockPlanner::builder("file2.parquet")
                    .add_plan(MockPlanBuilder::new().with_morsel(MorselId(11), 102))
                    .return_none(),
            )
            .with_file_in_partition(
                PartitionId(1),
                MockPlanner::builder("file3.parquet")
                    .add_plan(MockPlanBuilder::new().with_morsel(MorselId(12), 103))
                    .return_none(),
            )
            // Partition 2 starts empty but is polled first, so it should drain
            // the shared queue across both sibling partitions.
            .with_reads(vec![
                PartitionId(2),
                PartitionId(2),
                PartitionId(1),
                PartitionId(2),
            ])
            .with_file_stream_events(false);

        insta::assert_snapshot!(test.run().await.unwrap(), @r"
        ----- Partition 0 -----
        Done
        ----- Partition 1 -----
        Batch: 103
        Done
        ----- Partition 2 -----
        Batch: 101
        Batch: 102
        Done
        ----- File Stream Events -----
        (omitted due to with_file_stream_events(false))
        ");

        Ok(())
    }

    /// Tests how one or more `FileStream`s consume morselized file work.
    #[derive(Clone)]
    struct FileStreamMorselTest {
        morselizer: MockMorselizer,
        partition_files: BTreeMap<PartitionId, Vec<String>>,
        preserve_order: bool,
        partitioned_by_file_group: bool,
        file_stream_events: bool,
        build_streams_on_first_read: bool,
        reads: Vec<PartitionId>,
        limit: Option<usize>,
    }

    impl FileStreamMorselTest {
        /// Creates an empty test harness.
        fn new() -> Self {
            Self {
                morselizer: MockMorselizer::new(),
                partition_files: BTreeMap::new(),
                preserve_order: false,
                partitioned_by_file_group: false,
                file_stream_events: true,
                build_streams_on_first_read: false,
                reads: vec![],
                limit: None,
            }
        }

        /// Adds one file and its root planner to partition 0.
        fn with_file(self, planner: impl Into<MockPlanner>) -> Self {
            self.with_file_in_partition(PartitionId(0), planner)
        }

        /// Adds one file and its root planner to the specified input partition.
        fn with_file_in_partition(
            mut self,
            partition: PartitionId,
            planner: impl Into<MockPlanner>,
        ) -> Self {
            let planner = planner.into();
            let file_path = planner.file_path().to_string();
            self.morselizer = self.morselizer.with_planner(planner);
            self.partition_files
                .entry(partition)
                .or_default()
                .push(file_path);
            self
        }

        /// Marks the stream (and all partitions) to preserve the specified file
        /// order.
        fn with_preserve_order(mut self, preserve_order: bool) -> Self {
            self.preserve_order = preserve_order;
            self
        }

        /// Marks the test scan as pre-partitioned by file group, which should
        /// force each stream to keep its own files local.
        fn with_partitioned_by_file_group(
            mut self,
            partitioned_by_file_group: bool,
        ) -> Self {
            self.partitioned_by_file_group = partitioned_by_file_group;
            self
        }

        /// Controls whether scheduler events are included in the snapshot.
        ///
        /// When disabled, `run()` still includes the event section header but
        /// replaces the trace with a fixed placeholder so tests can focus only
        /// on the output batches.
        fn with_file_stream_events(mut self, file_stream_events: bool) -> Self {
            self.file_stream_events = file_stream_events;
            self
        }

        /// Controls whether streams are all built up front or lazily on their
        /// first read.
        ///
        /// The default builds all streams before polling begins, which matches
        /// normal execution. Tests may enable lazy creation to model races
        /// where one sibling polls before another has contributed its files to
        /// the shared queue.
        fn with_build_streams_on_first_read(
            mut self,
            build_streams_on_first_read: bool,
        ) -> Self {
            self.build_streams_on_first_read = build_streams_on_first_read;
            self
        }

        /// Sets the partition polling order.
        ///
        /// `run()` polls these partitions in the listed order first. After
        /// those explicit reads are exhausted, it completes to round
        /// robin across all configured partitions, skipping any streams that
        /// have already finished.
        ///
        /// This allows testing early scheduling decisions explicit in a test
        /// while avoiding a fully scripted poll trace for the remainder.
        fn with_reads(mut self, reads: Vec<PartitionId>) -> Self {
            self.reads = reads;
            self
        }

        /// Sets a global output limit for all streams created by this test.
        fn with_limit(mut self, limit: usize) -> Self {
            self.limit = Some(limit);
            self
        }

        /// Runs the test and returns combined stream output and scheduler
        /// trace text.
        async fn run(self) -> Result<String> {
            let observer = self.morselizer.observer().clone();
            observer.clear();

            let metrics_set = ExecutionPlanMetricsSet::new();
            let partition_count = self.num_partitions();

            let mut partitions = (0..partition_count)
                .map(|_| PartitionState::new())
                .collect::<Vec<_>>();

            let mut build_order = Vec::new();
            for partition in self.reads.iter().map(|partition| partition.0) {
                if !build_order.contains(&partition) {
                    build_order.push(partition);
                }
            }
            for partition in 0..partition_count {
                if !build_order.contains(&partition) {
                    build_order.push(partition);
                }
            }

            let config = self.test_config();
            // `DataSourceExec::execute` creates one execution-local shared
            // state object via `create_sibling_state()` and then passes it
            // to `open_with_sibling_state(...)`. These tests build
            // `FileStream`s directly, bypassing `DataSourceExec`, so they must
            // perform the same setup explicitly when exercising sibling-stream
            // work stealing.
            let shared_work_source = config.create_sibling_state().and_then(|state| {
                state.as_ref().downcast_ref::<SharedWorkSource>().cloned()
            });
            if !self.build_streams_on_first_read {
                for partition in build_order {
                    let stream = FileStreamBuilder::new(&config)
                        .with_partition(partition)
                        .with_shared_work_source(shared_work_source.clone())
                        .with_morselizer(Box::new(self.morselizer.clone()))
                        .with_metrics(&metrics_set)
                        .build()?;
                    partitions[partition].set_stream(stream);
                }
            }

            let mut initial_reads: VecDeque<_> = self.reads.into();
            let mut next_round_robin = 0;

            while !initial_reads.is_empty()
                || partitions.iter().any(PartitionState::is_active)
            {
                let partition = if let Some(partition) = initial_reads.pop_front() {
                    partition.0
                } else {
                    let partition = next_round_robin;
                    next_round_robin = (next_round_robin + 1) % partition_count.max(1);
                    partition
                };

                let partition_state = &mut partitions[partition];

                if self.build_streams_on_first_read && !partition_state.built {
                    let stream = FileStreamBuilder::new(&config)
                        .with_partition(partition)
                        .with_shared_work_source(shared_work_source.clone())
                        .with_morselizer(Box::new(self.morselizer.clone()))
                        .with_metrics(&metrics_set)
                        .build()?;
                    partition_state.set_stream(stream);
                }

                let Some(stream) = partition_state.stream.as_mut() else {
                    continue;
                };

                match stream.next().await {
                    Some(result) => partition_state.push_output(format_result(result)),
                    None => partition_state.finish(),
                }
            }

            let output_text = if partition_count == 1 {
                format!(
                    "----- Output Stream -----\n{}",
                    partitions[0].output.join("\n")
                )
            } else {
                partitions
                    .into_iter()
                    .enumerate()
                    .map(|(partition, state)| {
                        format!(
                            "----- Partition {} -----\n{}",
                            partition,
                            state.output.join("\n")
                        )
                    })
                    .collect::<Vec<_>>()
                    .join("\n")
            };

            let file_stream_events = if self.file_stream_events {
                observer.format_events()
            } else {
                "(omitted due to with_file_stream_events(false))".to_string()
            };

            Ok(format!(
                "{output_text}\n----- File Stream Events -----\n{file_stream_events}",
            ))
        }

        /// Returns the number of configured partitions, including empty ones
        /// that appear only in the explicit read schedule.
        fn num_partitions(&self) -> usize {
            self.partition_files
                .keys()
                .map(|partition| partition.0 + 1)
                .chain(self.reads.iter().map(|partition| partition.0 + 1))
                .max()
                .unwrap_or(1)
        }

        /// Builds a `FileScanConfig` covering every configured partition.
        fn test_config(&self) -> FileScanConfig {
            let file_groups = (0..self.num_partitions())
                .map(|partition| {
                    self.partition_files
                        .get(&PartitionId(partition))
                        .into_iter()
                        .flat_map(|files| files.iter())
                        .map(|name| PartitionedFile::new(name, 10))
                        .collect::<Vec<_>>()
                        .into()
                })
                .collect::<Vec<_>>();

            let table_schema = TableSchema::new(
                Arc::new(Schema::new(vec![Field::new("i", DataType::Int32, false)])),
                vec![],
            );
            FileScanConfigBuilder::new(
                ObjectStoreUrl::parse("test:///").unwrap(),
                Arc::new(MockSource::new(table_schema)),
            )
            .with_file_groups(file_groups)
            .with_limit(self.limit)
            .with_preserve_order(self.preserve_order)
            .with_partitioned_by_file_group(self.partitioned_by_file_group)
            .build()
        }
    }

    /// Formats one stream poll result into a stable snapshot line.
    fn format_result(result: Result<RecordBatch>) -> String {
        match result {
            Ok(batch) => {
                let col = batch.column(0).as_primitive::<Int32Type>();
                let batch_id = col.value(0);
                format!("Batch: {batch_id}")
            }
            Err(e) => {
                // Pull the actual message for external errors rather than
                // relying on DataFusionError formatting, which changes if
                // backtraces are enabled, etc.
                let message = if let DataFusionError::External(generic) = e {
                    generic.to_string()
                } else {
                    e.to_string()
                };
                format!("Error: {message}")
            }
        }
    }

    async fn drain_stream_output(stream: FileStream) -> Result<String> {
        let output = stream
            .collect::<Vec<_>>()
            .await
            .into_iter()
            .map(|result| result.map(|batch| format_result(Ok(batch))))
            .collect::<Result<Vec<_>>>()?;
        Ok(output.join("\n"))
    }

    fn metric_count(metrics: &ExecutionPlanMetricsSet, name: &str) -> usize {
        metrics
            .clone_inner()
            .sum_by_name(name)
            .unwrap_or_else(|| panic!("missing metric: {name}"))
            .as_usize()
    }

    /// Test-only state for one stream partition in [`FileStreamMorselTest`].
    struct PartitionState {
        /// Whether the `FileStream` for this partition has been built yet.
        built: bool,
        /// The live stream, if this partition has not finished yet.
        stream: Option<FileStream>,
        /// Snapshot lines produced by this partition.
        output: Vec<String>,
    }

    impl PartitionState {
        /// Create an unbuilt partition with no output yet.
        fn new() -> Self {
            Self {
                built: false,
                stream: None,
                output: vec![],
            }
        }

        /// Returns true if this partition might still produce output.
        fn is_active(&self) -> bool {
            !self.built || self.stream.is_some()
        }

        /// Records that this partition's stream has been built.
        fn set_stream(&mut self, stream: FileStream) {
            self.stream = Some(stream);
            self.built = true;
        }

        /// Records one formatted output line for this partition.
        fn push_output(&mut self, line: String) {
            self.output.push(line);
        }

        /// Marks this partition as finished.
        fn finish(&mut self) {
            self.push_output("Done".to_string());
            self.stream = None;
        }
    }
}
