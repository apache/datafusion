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

use super::demux::start_demuxer_task;
use super::{create_writer, AbortableWrite, SerializationSchema};
use crate::datasource::file_format::file_compression_type::FileCompressionType;
use crate::datasource::physical_plan::FileSinkConfig;
use crate::error::Result;
use crate::physical_plan::SendableRecordBatchStream;

use arrow_array::RecordBatch;
use datafusion_common::{internal_datafusion_err, internal_err, DataFusionError};
use datafusion_execution::TaskContext;

use bytes::Bytes;
use tokio::io::{AsyncWrite, AsyncWriteExt};
use tokio::sync::mpsc::{self, Receiver};
use tokio::task::{JoinHandle, JoinSet};
use tokio::try_join;

type WriterType = AbortableWrite<Box<dyn AsyncWrite + Send + Unpin>>;
type SerializerType = Arc<dyn SerializationSchema>;

/// Serializes a single data stream in parallel and writes to an ObjectStore
/// concurrently. Data order is preserved. In the event of an error,
/// the ObjectStore writer is returned to the caller in addition to an error,
/// so that the caller may handle aborting failed writes.
pub(crate) async fn serialize_rb_stream_to_object_store(
    mut data_rx: Receiver<RecordBatch>,
    mut serializer: Arc<dyn SerializationSchema>,
    mut writer: AbortableWrite<Box<dyn AsyncWrite + Send + Unpin>>,
) -> std::result::Result<(WriterType, u64), (WriterType, DataFusionError)> {
    let (tx, mut rx) =
        mpsc::channel::<JoinHandle<Result<(usize, Bytes), DataFusionError>>>(100);
    // Initially, has_header can be true for CSV use cases. Then, we must turn
    // it off to maintain the integrity of the writing process.
    let serialize_task = tokio::spawn(async move {
        let mut initial = true;
        while let Some(batch) = data_rx.recv().await {
            let serializer_clone = serializer.clone();
            let handle = tokio::spawn(async move {
                let num_rows = batch.num_rows();
                let bytes = serializer_clone.serialize(batch).await?;
                Ok((num_rows, bytes))
            });
            if initial {
                serializer = serializer.duplicate_headerless();
                initial = false;
            }
            tx.send(handle).await.map_err(|_| {
                internal_datafusion_err!("Unknown error writing to object store")
            })?;
        }
        Ok(())
    });

    let mut row_count = 0;
    while let Some(handle) = rx.recv().await {
        match handle.await {
            Ok(Ok((cnt, bytes))) => {
                match writer.write_all(&bytes).await {
                    Ok(_) => (),
                    Err(e) => {
                        return Err((
                            writer,
                            DataFusionError::Execution(format!(
                                "Error writing to object store: {e}"
                            )),
                        ))
                    }
                };
                row_count += cnt;
            }
            Ok(Err(e)) => {
                // Return the writer along with the error
                return Err((writer, e));
            }
            Err(e) => {
                // Handle task panic or cancellation
                return Err((
                    writer,
                    DataFusionError::Execution(format!(
                        "Serialization task panicked or was cancelled: {e}"
                    )),
                ));
            }
        }
    }

    match serialize_task.await {
        Ok(Ok(_)) => (),
        Ok(Err(e)) => return Err((writer, e)),
        Err(_) => {
            return Err((
                writer,
                internal_datafusion_err!("Unknown error writing to object store"),
            ))
        }
    };
    Ok((writer, row_count as u64))
}

type FileWriteBundle = (Receiver<RecordBatch>, SerializerType, WriterType);
/// Contains the common logic for serializing RecordBatches and
/// writing the resulting bytes to an ObjectStore.
/// Serialization is assumed to be stateless, i.e.
/// each RecordBatch can be serialized without any
/// dependency on the RecordBatches before or after.
pub(crate) async fn stateless_serialize_and_write_files(
    mut rx: Receiver<FileWriteBundle>,
    tx: tokio::sync::oneshot::Sender<u64>,
) -> Result<()> {
    let mut row_count = 0;
    // tracks if any writers encountered an error triggering the need to abort
    let mut any_errors = false;
    // tracks the specific error triggering abort
    let mut triggering_error = None;
    // tracks if any errors were encountered in the process of aborting writers.
    // if true, we may not have a guarentee that all written data was cleaned up.
    let mut any_abort_errors = false;
    let mut join_set = JoinSet::new();
    while let Some((data_rx, serializer, writer)) = rx.recv().await {
        join_set.spawn(async move {
            serialize_rb_stream_to_object_store(data_rx, serializer, writer).await
        });
    }
    let mut finished_writers = Vec::new();
    while let Some(result) = join_set.join_next().await {
        match result {
            Ok(res) => match res {
                Ok((writer, cnt)) => {
                    finished_writers.push(writer);
                    row_count += cnt;
                }
                Err((writer, e)) => {
                    finished_writers.push(writer);
                    any_errors = true;
                    triggering_error = Some(e);
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
        match any_errors {
            true => {
                let abort_result = writer.abort_writer();
                if abort_result.is_err() {
                    any_abort_errors = true;
                }
            }
            false => {
                writer.shutdown()
                    .await
                    .map_err(|_| internal_datafusion_err!("Error encountered while finalizing writes! Partial results may have been written to ObjectStore!"))?;
            }
        }
    }

    if any_errors {
        match any_abort_errors{
            true => return internal_err!("Error encountered during writing to ObjectStore and failed to abort all writers. Partial result may have been written."),
            false => match triggering_error {
                Some(e) => return Err(e),
                None => return internal_err!("Unknown Error encountered during writing to ObjectStore. All writers succesfully aborted.")
            }
        }
    }

    tx.send(row_count).map_err(|_| {
        internal_datafusion_err!(
            "Error encountered while sending row count back to file sink!"
        )
    })?;
    Ok(())
}

/// Orchestrates multipart put of a dynamic number of output files from a single input stream
/// for any statelessly serialized file type. That is, any file type for which each [RecordBatch]
/// can be serialized independently of all other [RecordBatch]s.
pub(crate) async fn stateless_multipart_put(
    data: SendableRecordBatchStream,
    context: &Arc<TaskContext>,
    file_extension: String,
    get_serializer: Box<dyn Fn() -> Arc<dyn SerializationSchema> + Send>,
    config: &FileSinkConfig,
    compression: FileCompressionType,
) -> Result<u64> {
    let object_store = context
        .runtime_env()
        .object_store(&config.object_store_url)?;

    let single_file_output = config.single_file_output;
    let base_output_path = &config.table_paths[0];
    let part_cols = if !config.table_partition_cols.is_empty() {
        Some(config.table_partition_cols.clone())
    } else {
        None
    };

    let (demux_task, mut file_stream_rx) = start_demuxer_task(
        data,
        context,
        part_cols,
        base_output_path.clone(),
        file_extension,
        single_file_output,
    );

    let rb_buffer_size = &context
        .session_config()
        .options()
        .execution
        .max_buffered_batches_per_output_file;

    let (tx_file_bundle, rx_file_bundle) = tokio::sync::mpsc::channel(rb_buffer_size / 2);
    let (tx_row_cnt, rx_row_cnt) = tokio::sync::oneshot::channel();
    let write_coordinater_task = tokio::spawn(async move {
        stateless_serialize_and_write_files(rx_file_bundle, tx_row_cnt).await
    });
    while let Some((location, rb_stream)) = file_stream_rx.recv().await {
        let serializer = get_serializer();
        let writer = create_writer(compression, &location, object_store.clone()).await?;

        tx_file_bundle
            .send((rb_stream, serializer, writer))
            .await
            .map_err(|_| {
                internal_datafusion_err!(
                    "Writer receive file bundle channel closed unexpectedly!"
                )
            })?;
    }

    // Signal to the write coordinater that no more files are coming
    drop(tx_file_bundle);

    match try_join!(write_coordinater_task, demux_task) {
        Ok((r1, r2)) => {
            r1?;
            r2?;
        }
        Err(e) => {
            if e.is_panic() {
                std::panic::resume_unwind(e.into_panic());
            } else {
                unreachable!();
            }
        }
    }

    let total_count = rx_row_cnt.await.map_err(|_| {
        internal_datafusion_err!("Did not receieve row count from write coordinater")
    })?;

    Ok(total_count)
}
