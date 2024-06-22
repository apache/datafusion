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

//! DataFusion distributed planning and execution

use std::fs::File;
use std::io::Error;
use std::pin::Pin;
use std::sync::Arc;
use std::task::Context;
use std::task::Poll;

use arrow::datatypes::DataType;
use arrow::datatypes::Field;
use arrow::ipc::writer::{IpcWriteOptions, StreamWriter};
use arrow::ipc::CompressionType;
use datafusion_common::{DataFusionError, Result};
use datafusion_physical_plan::metrics;
use datafusion_physical_plan::RecordBatchStream;
use futures::future::BoxFuture;
use futures::AsyncWrite;
use futures::StreamExt;
use log::error;
use object_store::path::Path;
use object_store::MultipartId;
use object_store::ObjectStore;
use tokio::task::JoinHandle;

pub mod shuffle_reader;
pub mod shuffle_writer;

#[derive(Debug, Clone)]
pub struct PartitionLocation {
    partition_id: usize,
    partition_stats: PartitionStats,
    path: String,
}

#[derive(Debug, Copy, Clone)]
pub struct PartitionStats {
    num_rows: usize,
    num_batches: usize,
    num_bytes: usize,
}

impl PartitionStats {
    pub fn new(num_rows: usize, num_batches: usize, num_bytes: usize) -> Self {
        Self {
            num_rows,
            num_batches,
            num_bytes,
        }
    }

    pub fn arrow_struct_repr(self) -> Field {
        Field::new(
            "partition_stats",
            DataType::Struct(self.arrow_struct_fields().into()),
            false,
        )
    }

    pub fn arrow_struct_fields(self) -> Vec<Field> {
        vec![
            Field::new("num_rows", DataType::UInt64, false),
            Field::new("num_batches", DataType::UInt64, false),
            Field::new("num_bytes", DataType::UInt64, false),
        ]
    }
}

impl Default for PartitionStats {
    fn default() -> Self {
        Self::new(0, 0, 0)
    }
}

#[derive(Debug)]
pub struct ShuffleWritePartition {
    partition_id: usize,
    path: String,
    num_batches: usize,
    num_rows: usize,
    num_bytes: usize,
}

#[derive(Clone)]
pub(crate) struct MultiPart {
    /// A shared reference to the object store
    store: Arc<dyn ObjectStore>,
    multipart_id: MultipartId,
    location: Path,
}

impl MultiPart {
    /// Create a new `MultiPart`
    pub fn new(
        store: Arc<dyn ObjectStore>,
        multipart_id: MultipartId,
        location: Path,
    ) -> Self {
        Self {
            store,
            multipart_id,
            location,
        }
    }
}

/// A wrapper struct with abort method and writer
pub(crate) struct AbortableWrite<W: AsyncWrite + Unpin + Send> {
    writer: W,
    multipart: MultiPart,
}

impl<W: AsyncWrite + Unpin + Send> AbortableWrite<W> {
    /// Create a new `AbortableWrite` instance with the given writer, and write mode.
    pub(crate) fn new(writer: W, multipart: MultiPart) -> Self {
        Self { writer, multipart }
    }

    /// handling of abort for different write modes
    pub(crate) fn abort_writer(&self) -> Result<BoxFuture<'static, Result<()>>> {
        let multi = self.multipart.clone();
        // Ok(Box::pin(async move {
        //     multi
        //         .store
        //         .putabort_multipart(&multi.location, &multi.multipart_id)
        //         .await
        //         .map_err(DataFusionError::ObjectStore)
        // }))
        todo!()
    }
}

impl<W: AsyncWrite + Unpin + Send> AsyncWrite for AbortableWrite<W> {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<std::result::Result<usize, Error>> {
        Pin::new(&mut self.get_mut().writer).poll_write(cx, buf)
    }

    fn poll_flush(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<std::result::Result<(), Error>> {
        Pin::new(&mut self.get_mut().writer).poll_flush(cx)
    }

    fn poll_close(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<std::result::Result<(), Error>> {
        Pin::new(&mut self.get_mut().writer).poll_close(cx)
    }
}

/// Helper that aborts the given join handles on drop.
///
/// Useful to kill background tasks when the consumer is dropped.
#[derive(Debug)]
pub(crate) struct AbortOnDropMany<T>(pub Vec<JoinHandle<T>>);

impl<T> Drop for AbortOnDropMany<T> {
    fn drop(&mut self) {
        for join_handle in &self.0 {
            join_handle.abort();
        }
    }
}

/// Stream data to disk in Arrow IPC format
pub(crate) async fn write_stream_to_disk(
    stream: &mut Pin<Box<dyn RecordBatchStream + Send>>,
    path: &str,
    disk_write_metric: &metrics::Time,
) -> Result<PartitionStats> {
    let file = File::create(path).map_err(|e| {
        error!("Failed to create partition file at {}: {:?}", path, e);
        DataFusionError::IoError(e)
    })?;

    let mut num_rows = 0;
    let mut num_batches = 0;
    let mut num_bytes = 0;

    let options = IpcWriteOptions::default()
        .try_with_compression(Some(CompressionType::LZ4_FRAME))?;

    let mut writer =
        StreamWriter::try_new_with_options(file, stream.schema().as_ref(), options)?;

    while let Some(result) = stream.next().await {
        let batch = result?;

        let batch_size_bytes: usize = batch.get_array_memory_size();
        num_batches += 1;
        num_rows += batch.num_rows();
        num_bytes += batch_size_bytes;

        let timer = disk_write_metric.timer();
        writer.write(&batch)?;
        timer.done();
    }
    let timer = disk_write_metric.timer();
    writer.finish()?;
    timer.done();
    Ok(PartitionStats::new(num_rows, num_batches, num_bytes))
}
