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

//! Module containing helper methods/traits related to
//! orchestrating file serialization, streaming to object store,
//! parallelization, and abort handling

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use super::demux::DemuxedStreamReceiver;
use super::{BatchSerializer, ObjectWriterBuilder};
use crate::file_compression_type::FileCompressionType;
use crate::file_sink_config::FileSinkMetrics;
use datafusion_common::error::Result;
use datafusion_physical_plan::metrics::Time;

use arrow::array::RecordBatch;
use datafusion_common::{
    DataFusionError, exec_datafusion_err, internal_datafusion_err, internal_err,
};
use datafusion_common_runtime::{JoinSet, SpawnedTask};
use datafusion_execution::TaskContext;

use bytes::Bytes;
use futures::join;
use object_store::ObjectStore;
use tokio::io::{AsyncWrite, AsyncWriteExt};
use tokio::sync::mpsc::{self, Receiver};

type WriterType = Box<dyn AsyncWrite + Send + Unpin>;
type SerializerType = Arc<dyn BatchSerializer>;

/// Result of calling [`serialize_rb_stream_to_object_store`]
pub(crate) enum SerializedRecordBatchResult {
    Success {
        /// the writer
        writer: WriterType,

        /// the number of rows successfully written
        row_count: usize,

        /// Counter for the compressed bytes written to the object store writer.
        bytes_written: Arc<AtomicUsize>,
    },
    Failure {
        /// As explained in [`serialize_rb_stream_to_object_store`]:
        /// - If an IO error occurred that involved the ObjectStore writer, then the writer will not be returned to the caller
        /// - Otherwise, the writer is returned to the caller
        writer: Option<WriterType>,

        /// the actual error that occurred
        err: DataFusionError,
    },
}

impl SerializedRecordBatchResult {
    /// Create the success variant
    pub fn success(
        writer: WriterType,
        row_count: usize,
        bytes_written: Arc<AtomicUsize>,
    ) -> Self {
        Self::Success {
            writer,
            row_count,
            bytes_written,
        }
    }

    pub fn failure(writer: Option<WriterType>, err: DataFusionError) -> Self {
        Self::Failure { writer, err }
    }
}

/// Serializes a single data stream in parallel and writes to an ObjectStore concurrently.
/// Data order is preserved.
///
/// In the event of a non-IO error which does not involve the ObjectStore writer,
/// the writer returned to the caller in addition to the error,
/// so that failed writes may be aborted.
///
/// In the event of an IO error involving the ObjectStore writer,
/// the writer is dropped to avoid calling further methods on it which might panic.
pub(crate) async fn serialize_rb_stream_to_object_store(
    mut data_rx: Receiver<RecordBatch>,
    serializer: Arc<dyn BatchSerializer>,
    mut writer: WriterType,
    bytes_written: Arc<AtomicUsize>,
    elapsed_compute: Option<Time>,
) -> SerializedRecordBatchResult {
    let (tx, mut rx) =
        mpsc::channel::<SpawnedTask<Result<(usize, Bytes), DataFusionError>>>(100);
    let serialize_task = SpawnedTask::spawn(async move {
        // Some serializers (like CSV) handle the first batch differently than
        // subsequent batches, so we track that here.
        let mut initial = true;
        while let Some(batch) = data_rx.recv().await {
            let serializer_clone = Arc::clone(&serializer);
            let elapsed_compute = elapsed_compute.clone();

            let task = SpawnedTask::spawn(async move {
                let num_rows = batch.num_rows();

                let bytes = {
                    let _timer = elapsed_compute.as_ref().map(|metric| metric.timer());

                    serializer_clone.serialize(batch, initial)?
                };

                Ok((num_rows, bytes))
            });
            if initial {
                initial = false;
            }
            tx.send(task).await.map_err(|_| {
                internal_datafusion_err!("Unknown error writing to object store")
            })?;
        }
        Ok(())
    });

    let mut row_count = 0;
    while let Some(task) = rx.recv().await {
        match task.join().await {
            Ok(Ok((cnt, bytes))) => {
                match writer.write_all(&bytes).await {
                    Ok(_) => (),
                    Err(e) => {
                        return SerializedRecordBatchResult::failure(
                            None,
                            exec_datafusion_err!("Error writing to object store: {e}"),
                        );
                    }
                }
                row_count += cnt;
            }
            Ok(Err(e)) => {
                // Return the writer along with the error
                return SerializedRecordBatchResult::failure(Some(writer), e);
            }
            Err(e) => {
                // Handle task panic or cancellation
                return SerializedRecordBatchResult::failure(
                    Some(writer),
                    exec_datafusion_err!(
                        "Serialization task panicked or was cancelled: {e}"
                    ),
                );
            }
        }
    }

    match serialize_task.join().await {
        Ok(Ok(_)) => (),
        Ok(Err(e)) => return SerializedRecordBatchResult::failure(Some(writer), e),
        Err(_) => {
            return SerializedRecordBatchResult::failure(
                Some(writer),
                internal_datafusion_err!("Unknown error writing to object store"),
            );
        }
    }
    SerializedRecordBatchResult::success(writer, row_count, bytes_written)
}

type FileWriteBundle = (
    Receiver<RecordBatch>,
    SerializerType,
    WriterType,
    Arc<AtomicUsize>,
);

/// Options for stateless file writer tasks.
pub struct StatelessWriterOptions<'a> {
    compression: FileCompressionType,
    compression_level: Option<u32>,
    metrics: Option<&'a FileSinkMetrics>,
}

impl<'a> StatelessWriterOptions<'a> {
    /// Create options for the specified compression type.
    pub fn new(compression: FileCompressionType) -> Self {
        Self {
            compression,
            compression_level: None,
            metrics: None,
        }
    }

    /// Set the compression level.
    pub fn with_compression_level(mut self, compression_level: Option<u32>) -> Self {
        self.compression_level = compression_level;
        self
    }

    /// Record common file sink metrics while writing.
    pub fn with_metrics(mut self, metrics: &'a FileSinkMetrics) -> Self {
        self.metrics = Some(metrics);
        self
    }
}

/// Contains the common logic for serializing RecordBatches and
/// writing the resulting bytes to an ObjectStore.
/// Serialization is assumed to be stateless, i.e.
/// each RecordBatch can be serialized without any
/// dependency on the RecordBatches before or after.
pub(crate) async fn stateless_serialize_and_write_files(
    mut rx: Receiver<FileWriteBundle>,
    tx: tokio::sync::oneshot::Sender<(u64, usize)>,
    elapsed_compute: Option<Time>,
) -> Result<()> {
    let mut row_count = 0;
    let mut bytes_written = 0;
    // tracks if any writers encountered an error triggering the need to abort
    let mut any_errors = false;
    // tracks the specific error triggering abort
    let mut triggering_error = None;
    // tracks if any errors were encountered in the process of aborting writers.
    // if true, we may not have a guarantee that all written data was cleaned up.
    let mut any_abort_errors = false;
    let mut join_set = JoinSet::new();
    while let Some((data_rx, serializer, writer, byte_counter)) = rx.recv().await {
        let elapsed_compute = elapsed_compute.clone();

        join_set.spawn(async move {
            serialize_rb_stream_to_object_store(
                data_rx,
                serializer,
                writer,
                byte_counter,
                elapsed_compute,
            )
            .await
        });
    }
    let mut finished_writers = Vec::new();
    let mut byte_counters = Vec::new();
    while let Some(result) = join_set.join_next().await {
        match result {
            Ok(res) => match res {
                SerializedRecordBatchResult::Success {
                    writer,
                    row_count: cnt,
                    bytes_written: bytes,
                } => {
                    finished_writers.push(writer);
                    byte_counters.push(bytes);
                    row_count += cnt;
                }
                SerializedRecordBatchResult::Failure { writer, err } => {
                    finished_writers.extend(writer);
                    any_errors = true;
                    triggering_error = Some(err);
                }
            },
            Err(e) => {
                // Don't panic, instead try to clean up as many writers as possible.
                // If we hit this code, ownership of a writer was not joined back to
                // this thread, so we cannot clean it up (hence any_abort_errors is true)
                any_errors = true;
                any_abort_errors = true;
                triggering_error = Some(internal_datafusion_err!(
                    "Unexpected join error while serializing file {e}"
                ));
            }
        }
    }

    // Finalize or abort writers as appropriate
    for mut writer in finished_writers.into_iter() {
        writer.shutdown()
                    .await
                    .map_err(|_| internal_datafusion_err!("Error encountered while finalizing writes! Partial results may have been written to ObjectStore!"))?;
    }

    for counter in byte_counters {
        bytes_written += counter.load(Ordering::Relaxed);
    }

    if any_errors {
        match any_abort_errors {
            true => {
                return internal_err!(
                    "Error encountered during writing to ObjectStore and failed to abort all writers. Partial result may have been written."
                );
            }
            false => match triggering_error {
                Some(e) => return Err(e),
                None => {
                    return internal_err!(
                        "Unknown Error encountered during writing to ObjectStore. All writers successfully aborted."
                    );
                }
            },
        }
    }

    tx.send((row_count as u64, bytes_written)).map_err(|_| {
        internal_datafusion_err!(
            "Error encountered while sending row count back to file sink!"
        )
    })?;
    Ok(())
}

/// Orchestrates multipart put of a dynamic number of output files from a single input stream
/// for any statelessly serialized file type. That is, any file type for which each [RecordBatch]
/// can be serialized independently of all other [RecordBatch]s.
pub async fn spawn_writer_tasks_and_join(
    context: &Arc<TaskContext>,
    serializer: Arc<dyn BatchSerializer>,
    compression: FileCompressionType,
    compression_level: Option<u32>,
    object_store: Arc<dyn ObjectStore>,
    demux_task: SpawnedTask<Result<()>>,
    file_stream_rx: DemuxedStreamReceiver,
) -> Result<u64> {
    spawn_writer_tasks_and_join_with_metrics(
        context,
        serializer,
        StatelessWriterOptions::new(compression)
            .with_compression_level(compression_level),
        object_store,
        demux_task,
        file_stream_rx,
    )
    .await
}

/// Like [`spawn_writer_tasks_and_join`], and also records common file sink metrics.
pub async fn spawn_writer_tasks_and_join_with_metrics(
    context: &Arc<TaskContext>,
    serializer: Arc<dyn BatchSerializer>,
    options: StatelessWriterOptions<'_>,
    object_store: Arc<dyn ObjectStore>,
    demux_task: SpawnedTask<Result<()>>,
    mut file_stream_rx: DemuxedStreamReceiver,
) -> Result<u64> {
    let elapsed_compute = options
        .metrics
        .map(|metrics| metrics.elapsed_compute().clone());
    let rb_buffer_size = &context
        .session_config()
        .options()
        .execution
        .max_buffered_batches_per_output_file;

    let (tx_file_bundle, rx_file_bundle) = mpsc::channel(rb_buffer_size.get() / 2);
    let (tx_row_cnt, rx_row_cnt) = tokio::sync::oneshot::channel();
    let write_coordinator_task = SpawnedTask::spawn(async move {
        stateless_serialize_and_write_files(rx_file_bundle, tx_row_cnt, elapsed_compute)
            .await
    });
    while let Some((location, rb_stream)) = file_stream_rx.recv().await {
        let (writer, bytes_written) = ObjectWriterBuilder::new(
            options.compression,
            &location,
            Arc::clone(&object_store),
        )
        .with_buffer_size(Some(
            context
                .session_config()
                .options()
                .execution
                .objectstore_writer_buffer_size,
        ))
        .with_compression_level(options.compression_level)
        .build_with_byte_counter()?;

        if tx_file_bundle
            .send((rb_stream, Arc::clone(&serializer), writer, bytes_written))
            .await
            .is_err()
        {
            internal_datafusion_err!(
                "Writer receive file bundle channel closed unexpectedly!"
            );
        }
    }

    // Signal to the write coordinator that no more files are coming
    drop(tx_file_bundle);

    let (r1, r2) = join!(
        write_coordinator_task.join_unwind(),
        demux_task.join_unwind()
    );
    r1.map_err(|e| DataFusionError::ExecutionJoin(Box::new(e)))??;
    r2.map_err(|e| DataFusionError::ExecutionJoin(Box::new(e)))??;

    // Return total row count and update optional metrics:
    let (rows_written, bytes_written) = rx_row_cnt.await.map_err(|_| {
        internal_datafusion_err!("Did not receive row count from write coordinator")
    })?;
    if let Some(metrics) = options.metrics {
        metrics.rows_written().add(rows_written as usize);
        metrics.bytes_written().add(bytes_written);
    }
    Ok(rows_written)
}
