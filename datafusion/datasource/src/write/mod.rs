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

use std::io::Write;
use std::sync::Arc;

use crate::file_compression_type::FileCompressionType;
use crate::file_sink_config::FileSinkConfig;
use datafusion_common::error::Result;

use arrow::array::RecordBatch;
use arrow::datatypes::Schema;
use bytes::Bytes;
use datafusion_storage::StorageBinding;
use datafusion_storage::path::Path;
use datafusion_storage::{FileAccessContext, WriterOptions};
use tokio::io::AsyncWrite;

pub mod demux;
pub mod orchestration;

/// A buffer with interior mutability shared by the SerializedFileWriter and
/// storage writer
#[derive(Clone)]
pub struct SharedBuffer {
    /// The inner buffer for reading and writing
    ///
    /// The lock is used to obtain internal mutability, so no worry about the
    /// lock contention.
    pub buffer: Arc<futures::lock::Mutex<Vec<u8>>>,
}

impl SharedBuffer {
    pub fn new(capacity: usize) -> Self {
        Self {
            buffer: Arc::new(futures::lock::Mutex::new(Vec::with_capacity(capacity))),
        }
    }
}

impl Write for SharedBuffer {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        let mut buffer = self.buffer.try_lock().unwrap();
        Write::write(&mut *buffer, buf)
    }

    fn flush(&mut self) -> std::io::Result<()> {
        let mut buffer = self.buffer.try_lock().unwrap();
        Write::flush(&mut *buffer)
    }
}

/// A trait that defines the methods required for a RecordBatch serializer.
pub trait BatchSerializer: Sync + Send {
    /// Asynchronously serializes a `RecordBatch` and returns the serialized bytes.
    /// Parameter `initial` signals whether the given batch is the first batch.
    /// This distinction is important for certain serializers (like CSV).
    fn serialize(&self, batch: RecordBatch, initial: bool) -> Result<Bytes>;
}

/// Converts table schema to writer schema, which may differ in the case
/// of hive style partitioning where some columns are removed from the
/// underlying files.
pub fn get_writer_schema(config: &FileSinkConfig) -> Arc<Schema> {
    if !config.table_partition_cols.is_empty() && !config.keep_partition_by_columns {
        let schema = config.output_schema();
        let partition_names: Vec<_> =
            config.table_partition_cols.iter().map(|(s, _)| s).collect();
        Arc::new(Schema::new_with_metadata(
            schema
                .fields()
                .iter()
                .filter(|f| !partition_names.contains(&f.name()))
                .map(|f| (**f).clone())
                .collect::<Vec<_>>(),
            schema.metadata().clone(),
        ))
    } else {
        Arc::clone(config.output_schema())
    }
}

/// A builder for an [`AsyncWrite`] that writes to an object store location.
///
/// This can be used to specify file compression on the writer. The writer
/// will have a default buffer size unless altered. The specific default size
/// is chosen by the storage backend.
///
/// Shutdown commits the output. Dropping it cancels pending I/O without committing;
/// remote cleanup follows the backend's policy.
#[derive(Debug)]
pub struct FileWriterBuilder {
    /// Compression type for object writer.
    file_compression_type: FileCompressionType,
    /// Output path
    location: Path,
    /// The related store that handles the given path
    object_store: Arc<StorageBinding>,
    /// The size of the buffer for the object writer.
    buffer_size: Option<usize>,
    /// The compression level for the object writer.
    compression_level: Option<u32>,
}

impl FileWriterBuilder {
    /// Create a new [`FileWriterBuilder`] for the specified path and compression type.
    pub fn new(
        file_compression_type: FileCompressionType,
        location: &Path,
        object_store: Arc<StorageBinding>,
    ) -> Self {
        Self {
            file_compression_type,
            location: location.clone(),
            object_store,
            buffer_size: None,
            compression_level: None,
        }
    }

    /// Set buffer size in bytes for object writer.
    ///
    /// # Example
    /// ```
    /// # use datafusion_datasource::file_compression_type::FileCompressionType;
    /// # use datafusion_datasource::write::FileWriterBuilder;
    /// # use object_store::memory::InMemory;
    /// # use datafusion_storage::path::Path;
    /// # use std::sync::Arc;
    /// # let compression_type = FileCompressionType::UNCOMPRESSED;
    /// # let location = Path::from("/foo/bar");
    /// # let object_store = test_utils::storage::object_store(Arc::new(InMemory::new()));
    /// let mut builder = FileWriterBuilder::new(compression_type, &location, object_store);
    /// builder.set_buffer_size(Some(20 * 1024 * 1024)); //20 MiB
    /// assert_eq!(
    ///     builder.get_buffer_size(),
    ///     Some(20 * 1024 * 1024),
    ///     "Internal error: Builder buffer size doesn't match"
    /// );
    /// ```
    pub fn set_buffer_size(&mut self, buffer_size: Option<usize>) {
        self.buffer_size = buffer_size;
    }

    /// Set buffer size in bytes for object writer, returning the builder.
    ///
    /// # Example
    /// ```
    /// # use datafusion_datasource::file_compression_type::FileCompressionType;
    /// # use datafusion_datasource::write::FileWriterBuilder;
    /// # use object_store::memory::InMemory;
    /// # use datafusion_storage::path::Path;
    /// # use std::sync::Arc;
    /// # let compression_type = FileCompressionType::UNCOMPRESSED;
    /// # let location = Path::from("/foo/bar");
    /// # let object_store = test_utils::storage::object_store(Arc::new(InMemory::new()));
    /// let builder = FileWriterBuilder::new(compression_type, &location, object_store)
    ///     .with_buffer_size(Some(20 * 1024 * 1024)); //20 MiB
    /// assert_eq!(
    ///     builder.get_buffer_size(),
    ///     Some(20 * 1024 * 1024),
    ///     "Internal error: Builder buffer size doesn't match"
    /// );
    /// ```
    pub fn with_buffer_size(mut self, buffer_size: Option<usize>) -> Self {
        self.buffer_size = buffer_size;
        self
    }

    /// Currently specified buffer size in bytes.
    pub fn get_buffer_size(&self) -> Option<usize> {
        self.buffer_size
    }

    /// Set compression level for object writer.
    pub fn set_compression_level(&mut self, compression_level: Option<u32>) {
        self.compression_level = compression_level;
    }

    /// Set compression level for object writer, returning the builder.
    pub fn with_compression_level(mut self, compression_level: Option<u32>) -> Self {
        self.compression_level = compression_level;
        self
    }

    /// Currently specified compression level.
    pub fn get_compression_level(&self) -> Option<u32> {
        self.compression_level
    }

    /// Return a writer object that writes to the object store location.
    ///
    /// If a buffer size has not been set, the default buffer buffer size will
    /// be used.
    ///
    /// # Errors
    /// If there is an error applying the compression type.
    pub async fn build(self) -> Result<Box<dyn AsyncWrite + Send + Unpin>> {
        let Self {
            file_compression_type,
            location,
            object_store,
            buffer_size,
            compression_level,
        } = self;

        let writer = object_store
            .writer(
                &location,
                WriterOptions { buffer_size },
                FileAccessContext::new("write"),
            )
            .await?;

        file_compression_type.convert_async_writer_with_level(writer, compression_level)
    }
}
