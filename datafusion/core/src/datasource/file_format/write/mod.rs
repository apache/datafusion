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
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use crate::datasource::file_format::file_compression_type::FileCompressionType;

use crate::error::Result;

use arrow_array::RecordBatch;

use datafusion_common::DataFusionError;

use async_trait::async_trait;
use bytes::Bytes;

use futures::future::BoxFuture;
use object_store::path::Path;
use object_store::{MultipartId, ObjectStore};

use tokio::io::AsyncWrite;

pub(crate) mod demux;
pub(crate) mod orchestration;

/// Stores data needed during abortion of MultiPart writers
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
        Ok(Box::pin(async move {
            multi
                .store
                .abort_multipart(&multi.location, &multi.multipart_id)
                .await
                .map_err(DataFusionError::ObjectStore)
        }))
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
    file_compression_type: FileCompressionType,
    location: &Path,
    object_store: Arc<dyn ObjectStore>,
) -> Result<AbortableWrite<Box<dyn AsyncWrite + Send + Unpin>>> {
    let (multipart_id, writer) = object_store
        .put_multipart(location)
        .await
        .map_err(DataFusionError::ObjectStore)?;
    Ok(AbortableWrite::new(
        file_compression_type.convert_async_writer(writer)?,
        MultiPart::new(object_store, multipart_id, location.clone()),
    ))
}
