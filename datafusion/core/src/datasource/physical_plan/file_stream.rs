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
use std::sync::Arc;
use std::task::{Context, Poll};

use crate::datasource::listing::PartitionedFile;
use crate::datasource::physical_plan::file_scan_config::PartitionColumnProjector;
use crate::datasource::physical_plan::{FileMeta, FileScanConfig};
use crate::error::Result;
use crate::physical_plan::metrics::{BaselineMetrics, ExecutionPlanMetricsSet};
use crate::physical_plan::RecordBatchStream;

use arrow::datatypes::SchemaRef;
use arrow::error::ArrowError;
use arrow::record_batch::RecordBatch;
pub use datafusion_catalog_datasource::file_stream::{
    FileOpenFuture, FileOpener, OnError,
};
use datafusion_catalog_datasource::file_stream::{
    FileStreamMetrics, FileStreamState, NextOpen,
};
use datafusion_common::ScalarValue;

use futures::{ready, FutureExt, Stream, StreamExt};

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
    /// The partition column projector
    pc_projector: PartitionColumnProjector,
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
    pub fn new(
        config: &FileScanConfig,
        partition: usize,
        file_opener: Arc<dyn FileOpener>,
        metrics: &ExecutionPlanMetricsSet,
    ) -> Result<Self> {
        let (projected_schema, ..) = config.project();
        let pc_projector = PartitionColumnProjector::new(
            Arc::clone(&projected_schema),
            &config
                .table_partition_cols
                .iter()
                .map(|x| x.name().clone())
                .collect::<Vec<_>>(),
        );

        let files = config.file_groups[partition].clone();

        Ok(Self {
            file_iter: files.into(),
            projected_schema,
            remain: config.limit,
            file_opener,
            pc_projector,
            state: FileStreamState::Idle,
            file_stream_metrics: FileStreamMetrics::new(metrics, partition),
            baseline_metrics: BaselineMetrics::new(metrics, partition),
            on_error: OnError::Fail,
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
    fn start_next_file(&mut self) -> Option<Result<(FileOpenFuture, Vec<ScalarValue>)>> {
        let part_file = self.file_iter.pop_front()?;

        let file_meta = FileMeta {
            object_meta: part_file.object_meta,
            range: part_file.range,
            extensions: part_file.extensions,
            metadata_size_hint: part_file.metadata_size_hint,
        };

        Some(
            self.file_opener
                .open(file_meta)
                .map(|future| (future, part_file.partition_values)),
        )
    }

    fn poll_inner(&mut self, cx: &mut Context<'_>) -> Poll<Option<Result<RecordBatch>>> {
        loop {
            match &mut self.state {
                FileStreamState::Idle => {
                    self.file_stream_metrics.time_opening.start();

                    match self.start_next_file().transpose() {
                        Ok(Some((future, partition_values))) => {
                            self.state = FileStreamState::Open {
                                future,
                                partition_values,
                            }
                        }
                        Ok(None) => return Poll::Ready(None),
                        Err(e) => {
                            self.state = FileStreamState::Error;
                            return Poll::Ready(Some(Err(e)));
                        }
                    }
                }
                FileStreamState::Open {
                    future,
                    partition_values,
                } => match ready!(future.poll_unpin(cx)) {
                    Ok(reader) => {
                        let partition_values = mem::take(partition_values);

                        // include time needed to start opening in `start_next_file`
                        self.file_stream_metrics.time_opening.stop();
                        let next = self.start_next_file().transpose();
                        self.file_stream_metrics.time_scanning_until_data.start();
                        self.file_stream_metrics.time_scanning_total.start();

                        match next {
                            Ok(Some((next_future, next_partition_values))) => {
                                self.state = FileStreamState::Scan {
                                    partition_values,
                                    reader,
                                    next: Some((
                                        NextOpen::Pending(next_future),
                                        next_partition_values,
                                    )),
                                };
                            }
                            Ok(None) => {
                                self.state = FileStreamState::Scan {
                                    reader,
                                    partition_values,
                                    next: None,
                                };
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
                FileStreamState::Scan {
                    reader,
                    partition_values,
                    next,
                } => {
                    // We need to poll the next `FileOpenFuture` here to drive it forward
                    if let Some((next_open_future, _)) = next {
                        if let NextOpen::Pending(f) = next_open_future {
                            if let Poll::Ready(reader) = f.as_mut().poll(cx) {
                                *next_open_future = NextOpen::Ready(reader);
                            }
                        }
                    }
                    match ready!(reader.poll_next_unpin(cx)) {
                        Some(Ok(batch)) => {
                            self.file_stream_metrics.time_scanning_until_data.stop();
                            self.file_stream_metrics.time_scanning_total.stop();
                            let result = self
                                .pc_projector
                                .project(batch, partition_values)
                                .map_err(|e| ArrowError::ExternalError(e.into()))
                                .map(|batch| match &mut self.remain {
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
                                });

                            if result.is_err() {
                                // If the partition value projection fails, this is not governed by
                                // the `OnError` behavior
                                self.state = FileStreamState::Error
                            }
                            self.file_stream_metrics.time_scanning_total.start();
                            return Poll::Ready(Some(result.map_err(Into::into)));
                        }
                        Some(Err(err)) => {
                            self.file_stream_metrics.file_scan_errors.add(1);
                            self.file_stream_metrics.time_scanning_until_data.stop();
                            self.file_stream_metrics.time_scanning_total.stop();

                            match self.on_error {
                                // If `OnError::Skip` we skip the file as soon as we hit the first error
                                OnError::Skip => match mem::take(next) {
                                    Some((future, partition_values)) => {
                                        self.file_stream_metrics.time_opening.start();

                                        match future {
                                            NextOpen::Pending(future) => {
                                                self.state = FileStreamState::Open {
                                                    future,
                                                    partition_values,
                                                }
                                            }
                                            NextOpen::Ready(reader) => {
                                                self.state = FileStreamState::Open {
                                                    future: Box::pin(std::future::ready(
                                                        reader,
                                                    )),
                                                    partition_values,
                                                }
                                            }
                                        }
                                    }
                                    None => return Poll::Ready(None),
                                },
                                OnError::Fail => {
                                    self.state = FileStreamState::Error;
                                    return Poll::Ready(Some(Err(err.into())));
                                }
                            }
                        }
                        None => {
                            self.file_stream_metrics.time_scanning_until_data.stop();
                            self.file_stream_metrics.time_scanning_total.stop();

                            match mem::take(next) {
                                Some((future, partition_values)) => {
                                    self.file_stream_metrics.time_opening.start();

                                    match future {
                                        NextOpen::Pending(future) => {
                                            self.state = FileStreamState::Open {
                                                future,
                                                partition_values,
                                            }
                                        }
                                        NextOpen::Ready(reader) => {
                                            self.state = FileStreamState::Open {
                                                future: Box::pin(std::future::ready(
                                                    reader,
                                                )),
                                                partition_values,
                                            }
                                        }
                                    }
                                }
                                None => return Poll::Ready(None),
                            }
                        }
                    }
                }
                FileStreamState::Error | FileStreamState::Limit => {
                    return Poll::Ready(None)
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

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Arc;

    use super::*;
    use crate::datasource::object_store::ObjectStoreUrl;
    use crate::prelude::SessionContext;
    use crate::test::{make_partition, object_store::register_test_store};

    use crate::datasource::physical_plan::CsvSource;
    use arrow::datatypes::Schema;
    use datafusion_common::internal_err;

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
        fn open(&self, _file_meta: FileMeta) -> Result<FileOpenFuture> {
            let idx = self.current_idx.fetch_add(1, Ordering::SeqCst);

            if self.error_opening_idx.contains(&idx) {
                Ok(futures::future::ready(internal_err!("error opening")).boxed())
            } else if self.error_scanning_idx.contains(&idx) {
                let error = futures::future::ready(Err(ArrowError::IpcError(
                    "error scanning".to_owned(),
                )));
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

            let ctx = SessionContext::new();
            let mock_files: Vec<(String, u64)> = (0..self.num_files)
                .map(|idx| (format!("mock_file{idx}"), 10_u64))
                .collect();

            let mock_files_ref: Vec<(&str, u64)> = mock_files
                .iter()
                .map(|(name, size)| (name.as_str(), *size))
                .collect();

            register_test_store(&ctx, &mock_files_ref);

            let file_group = mock_files
                .into_iter()
                .map(|(name, size)| PartitionedFile::new(name, size))
                .collect();

            let on_error = self.on_error;

            let config = FileScanConfig::new(
                ObjectStoreUrl::parse("test:///").unwrap(),
                file_schema,
                Arc::new(CsvSource::default()),
            )
            .with_file_group(file_group)
            .with_limit(self.limit);
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
        crate::assert_batches_eq!(&[
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
        crate::assert_batches_eq!(&[
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
        crate::assert_batches_eq!(&[
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
        crate::assert_batches_eq!(&[
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
        crate::assert_batches_eq!(&[
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
        crate::assert_batches_eq!(&[
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
        crate::assert_batches_eq!(&[
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
        crate::assert_batches_eq!(&[
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
        crate::assert_batches_eq!(&[
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
        crate::assert_batches_eq!(&[
            "++",
            "++",
        ], &batches);

        Ok(())
    }

    #[tokio::test]
    async fn without_limit() -> Result<()> {
        let batches = create_and_collect(None).await;

        #[rustfmt::skip]
        crate::assert_batches_eq!(&[
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
        crate::assert_batches_eq!(&[
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
        crate::assert_batches_eq!(&[
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
