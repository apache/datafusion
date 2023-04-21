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

//! File writer executor
//!
use crate::datasource::file_format::file_type::FileCompressionType;
use crate::datasource::file_format::BatchSerializer;
use crate::physical_plan::file_format::file_stream::StartableTime;
use crate::physical_plan::file_format::{FileSinkConfig, FileWriterMode};
use crate::physical_plan::metrics::{
    BaselineMetrics, ExecutionPlanMetricsSet, MetricBuilder,
};
use crate::physical_plan::{RecordBatchStream, SendableRecordBatchStream};
use arrow::datatypes::SchemaRef;
use arrow::record_batch::RecordBatch;
use bytes::Bytes;
use datafusion_common::{DataFusionError, Result};
use futures::future::BoxFuture;
use futures::{ready, FutureExt, Stream, StreamExt};
use object_store::ObjectStore;
use std::mem;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};
use tokio::io::{AsyncWrite, AsyncWriteExt};

/// A fallible future that resolves to [`AsyncWrite`]
pub type WriterOpenFuture =
    BoxFuture<'static, Result<Box<dyn AsyncWrite + Unpin + Send>>>;

/// Represents the various states of the `FileSinkStream` struct.
enum FileSinkState {
    /// The writer is being opened asynchronously.
    Open { writer_future: WriterOpenFuture },
    /// The stream is pulling a new RecordBatch from its input.
    Pull,
    /// The stream is serializing the current RecordBatch into bytes.
    Serialize { maybe_batch: Option<RecordBatch> },
    /// The stream is writing the serialized bytes to the output.
    Write { maybe_batch: Option<Bytes> },
    /// The stream is flushing the output writer.
    Flush { exhausted: bool },
    /// The stream is shutting down the output writer.
    Shutdown,
}

/// Metrics for [`FileSinkStream`]
///
/// Note that all of these metrics are in terms of wall clock time
/// (not cpu time) so they include time spent waiting on I/O as well
/// as other operators.
struct FileSinkStreamMetrics {
    /// Time spent waiting for the FileStream's input.
    pub time_processing: StartableTime,
}

impl FileSinkStreamMetrics {
    fn new(metrics: &ExecutionPlanMetricsSet, partition: usize) -> Self {
        let time_processing = StartableTime {
            metrics: MetricBuilder::new(metrics)
                .subset_time("time_elapsed_processing", partition),
            start: None,
        };

        Self { time_processing }
    }
}

/// `FileSinkStream` struct handles writing record batches to a file-like output.
pub struct FileSinkStream<S: BatchSerializer> {
    /// Input stream providing record batches to be written
    input: SendableRecordBatchStream,
    /// Compression type to be applied to the file output
    file_compression: FileCompressionType,
    /// Serializer responsible for converting record batches into a suitable file format
    serializer: S,
    /// Writer to handle asynchronous writing of serialized data
    writer: Option<Box<dyn AsyncWrite + Unpin + Send>>,
    /// Mode in which the file writer operates
    writer_mode: FileWriterMode,
    /// Schema of the record batches being written
    schema: SchemaRef,
    /// The current state of the stream's state machine
    state: FileSinkState,
    /// File sink stream specific metrics for monitoring performance
    file_stream_metrics: FileSinkStreamMetrics,
    /// Runtime baseline metrics for monitoring performance
    baseline_metrics: BaselineMetrics,
}

impl<S: BatchSerializer> FileSinkStream<S> {
    pub fn try_new(
        config: &FileSinkConfig,
        partition: usize,
        store: Arc<dyn ObjectStore>,
        serializer: S,
        inner_stream: SendableRecordBatchStream,
        file_compression: FileCompressionType,
        metrics: &ExecutionPlanMetricsSet,
    ) -> Result<Self> {
        let writer_future = config
            .writer_mode
            .build_async_writer(&config.file_groups[partition].object_meta, &store)?;
        Ok(Self {
            schema: Arc::clone(&config.output_schema),
            input: inner_stream,
            file_compression,
            serializer,
            state: FileSinkState::Open { writer_future },
            writer: None,
            writer_mode: config.writer_mode,
            file_stream_metrics: FileSinkStreamMetrics::new(metrics, partition),
            baseline_metrics: BaselineMetrics::new(metrics, partition),
        })
    }

    fn poll_inner(&mut self, cx: &mut Context<'_>) -> Poll<Option<Result<RecordBatch>>> {
        loop {
            match &mut self.state {
                FileSinkState::Open { writer_future } => {
                    let writer = match ready!(writer_future.poll_unpin(cx)) {
                        Ok(writer) => writer,
                        Err(e) => {
                            return Poll::Ready(Some(Err(e)));
                        }
                    };

                    self.writer = match self.file_compression.convert_async_writer(writer)
                    {
                        Ok(writer) => Some(writer),
                        Err(e) => {
                            return Poll::Ready(Some(Err(e)));
                        }
                    };
                    self.state = FileSinkState::Pull;
                }
                FileSinkState::Pull => {
                    let maybe_batch = ready!(self.input.poll_next_unpin(cx));
                    match maybe_batch {
                        None => self.state = FileSinkState::Flush { exhausted: true },
                        Some(Ok(batch)) => {
                            self.state = FileSinkState::Serialize {
                                maybe_batch: Some(batch),
                            }
                        }
                        Some(Err(e)) => return Poll::Ready(Some(Err(e))),
                    }
                }
                FileSinkState::Serialize { maybe_batch } => {
                    let batch = mem::take(maybe_batch).unwrap();
                    let maybe_bytes = ready_poll!(self.serializer.serialize(batch), cx);
                    let bytes = match maybe_bytes {
                        Ok(b) => b,
                        Err(e) => return Poll::Ready(Some(Err(e))),
                    };
                    self.state = FileSinkState::Write {
                        maybe_batch: Some(bytes),
                    }
                }
                FileSinkState::Write { maybe_batch } => {
                    if let Some(writer) = self.writer.as_mut() {
                        if let Some(bytes) = maybe_batch {
                            match ready!(Box::pin(writer.write(bytes)).poll_unpin(cx)) {
                                Ok(_) => {
                                    if !matches!(&self.writer_mode, FileWriterMode::Put) {
                                        self.state =
                                            FileSinkState::Flush { exhausted: false };
                                    } else {
                                        self.state = FileSinkState::Pull;
                                    }
                                }
                                Err(e) => {
                                    return Poll::Ready(Some(Err(
                                        DataFusionError::IoError(e),
                                    )))
                                }
                            }
                        }
                    }
                }
                FileSinkState::Flush { exhausted } => {
                    if let Some(writer) = self.writer.as_mut() {
                        match ready!(Box::pin(writer.flush()).poll_unpin(cx)) {
                            Ok(_) => {
                                if *exhausted {
                                    self.state = FileSinkState::Shutdown;
                                } else {
                                    self.state = FileSinkState::Pull;
                                }
                            }
                            Err(e) => {
                                return Poll::Ready(Some(Err(DataFusionError::IoError(
                                    e,
                                ))))
                            }
                        }
                    }
                }
                FileSinkState::Shutdown => {
                    if let Some(writer) = self.writer.as_mut() {
                        return match ready!(Box::pin(writer.shutdown()).poll_unpin(cx)) {
                            Ok(_) => Poll::Ready(None),
                            Err(e) => Poll::Ready(Some(Err(DataFusionError::IoError(e)))),
                        };
                    }
                }
            }
        }
    }
}

impl<S: BatchSerializer> Stream for FileSinkStream<S> {
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

impl<S: BatchSerializer> RecordBatchStream for FileSinkStream<S> {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::datasource::object_store::ObjectStoreUrl;
    use crate::error::Result;
    use crate::physical_plan::stream::RecordBatchStreamAdapter;
    use arrow_schema::{DataType, Field, Schema};
    use async_trait::async_trait;
    use futures::StreamExt;
    use object_store::memory::InMemory;
    use object_store::path::Path;
    use object_store::{GetResult, ObjectMeta};

    struct TestSerializer {
        bytes: Bytes,
    }

    #[async_trait]
    impl BatchSerializer for TestSerializer {
        async fn serialize(&mut self, _batch: RecordBatch) -> Result<Bytes> {
            Ok(self.bytes.clone())
        }
    }

    async fn create_and_collect(total_bytes: usize, mode: FileWriterMode) -> Bytes {
        let file_schema =
            Arc::new(Schema::new(vec![Field::new("i", DataType::Int32, true)]));
        let batch = RecordBatch::new_empty(file_schema.clone());
        let number_of_batches = 10;
        assert_eq!(total_bytes % number_of_batches, 0);
        let batch_serialize_size = total_bytes / number_of_batches;
        let bytes = Bytes::from(vec![0; batch_serialize_size]);
        let serializer = TestSerializer { bytes };
        let inner_stream = Box::pin(RecordBatchStreamAdapter::new(
            file_schema.clone(),
            futures::stream::iter(
                itertools::repeat_n(batch.clone(), number_of_batches)
                    .map(Ok::<_, DataFusionError>),
            ),
        ));

        let memory = Arc::new(InMemory::new());

        let object = ObjectMeta {
            location: Path::parse("mock_file1").unwrap(),
            last_modified: Default::default(),
            size: 0,
        };

        let config = FileSinkConfig {
            object_store_url: ObjectStoreUrl::parse("test:///").unwrap(),
            file_groups: vec![object.clone().into()],
            output_schema: file_schema,
            table_partition_cols: vec![],
            writer_mode: mode,
        };
        let metrics_set = ExecutionPlanMetricsSet::new();
        let file_sink_stream = FileSinkStream::try_new(
            &config,
            0,
            memory.clone(),
            serializer,
            inner_stream,
            FileCompressionType::UNCOMPRESSED,
            &metrics_set,
        )
        .unwrap();

        file_sink_stream
            .map(|b| b.expect("No error expected in stream"))
            .collect::<Vec<_>>()
            .await;

        let mut res = match memory.get(&object.location).await.unwrap() {
            GetResult::Stream(s) => {
                s.map(|b| b.expect("No error expected in stream"))
                    .collect::<Vec<_>>()
                    .await
            }
            GetResult::File(_, _) => unreachable!(),
        };
        res.swap_remove(0)
    }

    #[tokio::test]
    async fn test_put_mode() -> Result<()> {
        let num_bytes = 1000;
        let bytes = create_and_collect(num_bytes, FileWriterMode::Put).await;
        assert_eq!(bytes.len(), num_bytes);
        Ok(())
    }

    #[tokio::test]
    async fn test_put_multipart_mode() -> Result<()> {
        let num_bytes = 1000;
        let bytes = create_and_collect(num_bytes, FileWriterMode::PutMultipart).await;
        assert_eq!(bytes.len(), num_bytes);
        Ok(())
    }
}
