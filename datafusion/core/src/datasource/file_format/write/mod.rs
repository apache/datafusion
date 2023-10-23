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

//! Module containing helper methods/traits related to enabling
//! write support for the various file formats

use std::io::Error;
use std::mem;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use crate::datasource::file_format::file_compression_type::FileCompressionType;

use crate::datasource::physical_plan::FileMeta;
use crate::error::Result;

use arrow_array::RecordBatch;

use datafusion_common::{exec_err, DataFusionError};

use async_trait::async_trait;
use bytes::Bytes;

use futures::future::BoxFuture;
use futures::ready;
use futures::FutureExt;
use object_store::path::Path;
use object_store::{MultipartId, ObjectMeta, ObjectStore};

use tokio::io::AsyncWrite;

pub(crate) mod demux;
pub(crate) mod orchestration;

/// `AsyncPutWriter` is an object that facilitates asynchronous writing to object stores.
/// It is specifically designed for the `object_store` crate's `put` method and sends
/// whole bytes at once when the buffer is flushed.
pub struct AsyncPutWriter {
    /// Object metadata
    object_meta: ObjectMeta,
    /// A shared reference to the object store
    store: Arc<dyn ObjectStore>,
    /// A buffer that stores the bytes to be sent
    current_buffer: Vec<u8>,
    /// Used for async handling in flush method
    inner_state: AsyncPutState,
}

impl AsyncPutWriter {
    /// Constructor for the `AsyncPutWriter` object
    pub fn new(object_meta: ObjectMeta, store: Arc<dyn ObjectStore>) -> Self {
        Self {
            object_meta,
            store,
            current_buffer: vec![],
            // The writer starts out in buffering mode
            inner_state: AsyncPutState::Buffer,
        }
    }

    /// Separate implementation function that unpins the [`AsyncPutWriter`] so
    /// that partial borrows work correctly
    fn poll_shutdown_inner(
        &mut self,
        cx: &mut Context<'_>,
    ) -> Poll<std::result::Result<(), Error>> {
        loop {
            match &mut self.inner_state {
                AsyncPutState::Buffer => {
                    // Convert the current buffer to bytes and take ownership of it
                    let bytes = Bytes::from(mem::take(&mut self.current_buffer));
                    // Set the inner state to Put variant with the bytes
                    self.inner_state = AsyncPutState::Put { bytes }
                }
                AsyncPutState::Put { bytes } => {
                    // Send the bytes to the object store's put method
                    return Poll::Ready(
                        ready!(self
                            .store
                            .put(&self.object_meta.location, bytes.clone())
                            .poll_unpin(cx))
                        .map_err(Error::from),
                    );
                }
            }
        }
    }
}

/// An enum that represents the inner state of AsyncPut
enum AsyncPutState {
    /// Building Bytes struct in this state
    Buffer,
    /// Data in the buffer is being sent to the object store
    Put { bytes: Bytes },
}

impl AsyncWrite for AsyncPutWriter {
    // Define the implementation of the AsyncWrite trait for the `AsyncPutWriter` struct
    fn poll_write(
        mut self: Pin<&mut Self>,
        _: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<std::result::Result<usize, Error>> {
        // Extend the current buffer with the incoming buffer
        self.current_buffer.extend_from_slice(buf);
        // Return a ready poll with the length of the incoming buffer
        Poll::Ready(Ok(buf.len()))
    }

    fn poll_flush(
        self: Pin<&mut Self>,
        _: &mut Context<'_>,
    ) -> Poll<std::result::Result<(), Error>> {
        // Return a ready poll with an empty result
        Poll::Ready(Ok(()))
    }

    fn poll_shutdown(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<std::result::Result<(), Error>> {
        // Call the poll_shutdown_inner method to handle the actual sending of data to the object store
        self.poll_shutdown_inner(cx)
    }
}

/// Stores data needed during abortion of MultiPart writers
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

pub(crate) enum AbortMode {
    Put,
    Append,
    MultiPart(MultiPart),
}

/// A wrapper struct with abort method and writer
pub(crate) struct AbortableWrite<W: AsyncWrite + Unpin + Send> {
    writer: W,
    mode: AbortMode,
}

impl<W: AsyncWrite + Unpin + Send> AbortableWrite<W> {
    /// Create a new `AbortableWrite` instance with the given writer, and write mode.
    pub(crate) fn new(writer: W, mode: AbortMode) -> Self {
        Self { writer, mode }
    }

    /// handling of abort for different write modes
    pub(crate) fn abort_writer(&self) -> Result<BoxFuture<'static, Result<()>>> {
        match &self.mode {
            AbortMode::Put => Ok(async { Ok(()) }.boxed()),
            AbortMode::Append => exec_err!("Cannot abort in append mode"),
            AbortMode::MultiPart(MultiPart {
                store,
                multipart_id,
                location,
            }) => {
                let location = location.clone();
                let multipart_id = multipart_id.clone();
                let store = store.clone();
                Ok(Box::pin(async move {
                    store
                        .abort_multipart(&location, &multipart_id)
                        .await
                        .map_err(DataFusionError::ObjectStore)
                }))
            }
        }
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

    fn poll_shutdown(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<std::result::Result<(), Error>> {
        Pin::new(&mut self.get_mut().writer).poll_shutdown(cx)
    }
}

/// An enum that defines different file writer modes.
#[derive(Debug, Clone, Copy)]
pub enum FileWriterMode {
    /// Data is appended to an existing file.
    Append,
    /// Data is written to a new file.
    Put,
    /// Data is written to a new file in multiple parts.
    PutMultipart,
}
/// A trait that defines the methods required for a RecordBatch serializer.
#[async_trait]
pub trait BatchSerializer: Unpin + Send {
    /// Asynchronously serializes a `RecordBatch` and returns the serialized bytes.
    async fn serialize(&mut self, batch: RecordBatch) -> Result<Bytes>;
    /// Duplicates self to support serializing multiple batches in parallel on multiple cores
    fn duplicate(&mut self) -> Result<Box<dyn BatchSerializer>> {
        Err(DataFusionError::NotImplemented(
            "Parallel serialization is not implemented for this file type".into(),
        ))
    }
}

/// Returns an [`AbortableWrite`] which writes to the given object store location
/// with the specified compression
pub(crate) async fn create_writer(
    writer_mode: FileWriterMode,
    file_compression_type: FileCompressionType,
    file_meta: FileMeta,
    object_store: Arc<dyn ObjectStore>,
) -> Result<AbortableWrite<Box<dyn AsyncWrite + Send + Unpin>>> {
    let object = &file_meta.object_meta;
    match writer_mode {
        // If the mode is append, call the store's append method and return wrapped in
        // a boxed trait object.
        FileWriterMode::Append => {
            let writer = object_store
                .append(&object.location)
                .await
                .map_err(DataFusionError::ObjectStore)?;
            let writer = AbortableWrite::new(
                file_compression_type.convert_async_writer(writer)?,
                AbortMode::Append,
            );
            Ok(writer)
        }
        // If the mode is put, create a new AsyncPut writer and return it wrapped in
        // a boxed trait object
        FileWriterMode::Put => {
            let writer = Box::new(AsyncPutWriter::new(object.clone(), object_store));
            let writer = AbortableWrite::new(
                file_compression_type.convert_async_writer(writer)?,
                AbortMode::Put,
            );
            Ok(writer)
        }
        // If the mode is put multipart, call the store's put_multipart method and
        // return the writer wrapped in a boxed trait object.
        FileWriterMode::PutMultipart => {
            let (multipart_id, writer) = object_store
                .put_multipart(&object.location)
                .await
                .map_err(DataFusionError::ObjectStore)?;
            Ok(AbortableWrite::new(
                file_compression_type.convert_async_writer(writer)?,
                AbortMode::MultiPart(MultiPart::new(
                    object_store,
                    multipart_id,
                    object.location.clone(),
                )),
            ))
        }
    }
}
