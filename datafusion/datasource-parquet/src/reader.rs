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

//! [`ParquetFileReaderFactory`] for low level control of Parquet file readers.

use crate::ParquetFileMetrics;
use crate::metadata::DFParquetMetadata;
use bytes::Bytes;
use datafusion_common::HashMap;
use datafusion_datasource::PartitionedFile;
use datafusion_execution::cache::cache_manager::FileMetadata;
use datafusion_execution::cache::cache_manager::FileMetadataCache;
use datafusion_physical_plan::metrics::ExecutionPlanMetricsSet;
use datafusion_storage::{FileAccessContext, FileReader, StorageBinding};
use futures::FutureExt;
use futures::future::BoxFuture;
use parquet::arrow::arrow_reader::ArrowReaderOptions;
use parquet::arrow::async_reader::AsyncFileReader;
use parquet::errors::ParquetError;
use parquet::file::metadata::ParquetMetaData;
use std::any::Any;
use std::fmt::Debug;
use std::ops::Range;
use std::sync::Arc;
use tokio::sync::OnceCell;

/// Interface for reading Apache Parquet files.
///
/// The combined implementations of [`ParquetFileReaderFactory`] and
/// [`AsyncFileReader`] can be used to provide custom data access operations
/// such as pre-cached metadata, I/O coalescing, etc.
///
/// See [`DefaultParquetFileReaderFactory`] for the built-in implementation.
pub trait ParquetFileReaderFactory: Debug + Send + Sync + 'static {
    /// Provides an `AsyncFileReader` for reading data from a parquet file specified
    ///
    /// # Notes
    ///
    /// If the resulting [`AsyncFileReader`]  returns `ParquetMetaData` without
    /// page index information, the reader will load it on demand. Thus it is important
    /// to ensure that the returned `ParquetMetaData` has the necessary information
    /// if you wish to avoid a subsequent I/O
    ///
    /// # Arguments
    /// * partition_index - Index of the partition (for reporting metrics)
    /// * file - The file to be read
    /// * metadata_size_hint - If specified, the first IO reads this many bytes from the footer
    /// * metrics - Execution metrics
    fn create_reader(
        &self,
        partition_index: usize,
        partitioned_file: PartitionedFile,
        metadata_size_hint: Option<usize>,
        metrics: &ExecutionPlanMetricsSet,
    ) -> datafusion_common::Result<Box<dyn AsyncFileReader + Send>>;
}

/// Default implementation of [`ParquetFileReaderFactory`]
///
/// This implementation:
/// 1. Reads parquet directly from an underlying [`StorageBinding`] instance.
/// 2. Reads the footer and page metadata on demand.
/// 3. Does not cache metadata or coalesce I/O operations.
#[derive(Debug)]
pub struct DefaultParquetFileReaderFactory {
    store: Arc<StorageBinding>,
    context: FileAccessContext,
}

impl DefaultParquetFileReaderFactory {
    pub fn with_context(mut self, context: FileAccessContext) -> Self {
        self.context = context;
        self
    }

    /// Create a new `DefaultParquetFileReaderFactory`.
    pub fn new(store: Arc<StorageBinding>) -> Self {
        Self {
            store,
            context: FileAccessContext::new("parquet"),
        }
    }
}

impl ParquetFileReaderFactory for DefaultParquetFileReaderFactory {
    fn create_reader(
        &self,
        partition_index: usize,
        partitioned_file: PartitionedFile,
        metadata_size_hint: Option<usize>,
        metrics: &ExecutionPlanMetricsSet,
    ) -> datafusion_common::Result<Box<dyn AsyncFileReader + Send>> {
        let file_metrics = ParquetFileMetrics::new(
            partition_index,
            partitioned_file.object_meta.location.as_ref(),
            metrics,
        );

        let reader = ParquetFileReader::new(
            file_metrics,
            Arc::clone(&self.store),
            partitioned_file,
        )
        .with_context(self.context.clone())
        .with_metadata_hint(metadata_size_hint);
        Ok(Box::new(reader))
    }
}

/// Implementation of [`ParquetFileReaderFactory`] supporting the caching of footer and page
/// metadata. Reads and updates the [`FileMetadataCache`] with the [`ParquetMetaData`] data.
///
/// [`ParquetFileReader::get_metadata`] forwards the [`parquet::file::metadata::PageIndexPolicy`] from
/// [`ArrowReaderOptions`] to [`DFParquetMetadata::fetch_metadata`], so callers such as the
/// parquet opener can skip page-index I/O during the initial metadata load.
#[derive(Debug)]
pub struct CachedParquetFileReaderFactory {
    store: Arc<StorageBinding>,
    context: FileAccessContext,
    metadata_cache: Arc<FileMetadataCache>,
}

impl CachedParquetFileReaderFactory {
    pub fn with_context(mut self, context: FileAccessContext) -> Self {
        self.context = context;
        self
    }

    pub fn new(
        store: Arc<StorageBinding>,
        metadata_cache: Arc<FileMetadataCache>,
    ) -> Self {
        Self {
            store,
            context: FileAccessContext::new("parquet"),
            metadata_cache,
        }
    }
}

impl ParquetFileReaderFactory for CachedParquetFileReaderFactory {
    fn create_reader(
        &self,
        partition_index: usize,
        partitioned_file: PartitionedFile,
        metadata_size_hint: Option<usize>,
        metrics: &ExecutionPlanMetricsSet,
    ) -> datafusion_common::Result<Box<dyn AsyncFileReader + Send>> {
        let file_metrics = ParquetFileMetrics::new(
            partition_index,
            partitioned_file.object_meta.location.as_ref(),
            metrics,
        );

        let reader = ParquetFileReader::new(
            file_metrics,
            Arc::clone(&self.store),
            partitioned_file,
        )
        .with_context(self.context.clone())
        .with_metadata_hint(metadata_size_hint)
        .with_metadata_cache(Some(Arc::clone(&self.metadata_cache)));

        Ok(Box::new(reader))
    }
}

/// Implements [`AsyncFileReader`] for a parquet file in object storage.
///
/// This implementation reads data directly from the underlying [`StorageBinding`]
/// on demand, as required, tracking the number of bytes read.
///
/// When configured via [`Self::with_metadata_cache`], [`Self::get_metadata`]
/// reads footer and page metadata from the cache when available and populates
/// the cache otherwise. Without a cache, metadata is fetched fresh on every call.
///
/// # Notes
///
/// This implementation does not coalesce I/O operations or cache bytes. Such
/// optimizations can be done either at the storage backend level or by providing
/// a custom implementation of [`ParquetFileReaderFactory`].
pub struct ParquetFileReader {
    reader: OnceCell<Arc<dyn FileReader>>,
    file_metrics: ParquetFileMetrics,
    store: Arc<StorageBinding>,
    context: FileAccessContext,
    partitioned_file: PartitionedFile,
    metadata_cache: Option<Arc<FileMetadataCache>>,
    metadata_size_hint: Option<usize>,
}

impl ParquetFileReader {
    fn with_context(mut self, context: FileAccessContext) -> Self {
        self.context = context;
        self
    }
    async fn reader(&self) -> parquet::errors::Result<&Arc<dyn FileReader>> {
        self.reader
            .get_or_try_init(|| {
                self.store
                    .open(&self.partitioned_file.object_meta, self.context.clone())
            })
            .await
            .map_err(|e| ParquetError::External(Box::new(e)))
    }

    /// Create a new `ParquetFileReader`.
    ///
    /// By default the reader has no [`FileMetadataCache`] and no metadata
    /// size hint, so metadata is fetched fresh on every call (as
    /// [`DefaultParquetFileReaderFactory`] does). Use
    /// [`Self::with_metadata_cache`] to read and populate a cache (as
    /// [`CachedParquetFileReaderFactory`] does), and
    /// [`Self::with_metadata_hint`] to set the size hint.
    pub(crate) fn new(
        file_metrics: ParquetFileMetrics,
        store: Arc<StorageBinding>,
        partitioned_file: PartitionedFile,
    ) -> Self {
        Self {
            reader: OnceCell::new(),
            file_metrics,
            store,
            context: FileAccessContext::new("parquet"),
            partitioned_file,
            metadata_cache: None,
            metadata_size_hint: None,
        }
    }

    /// Returns the metrics tracked while reading this file.
    pub fn file_metrics(&self) -> &ParquetFileMetrics {
        &self.file_metrics
    }

    /// Returns the file this reader is reading.
    pub fn partitioned_file(&self) -> &PartitionedFile {
        &self.partitioned_file
    }

    /// Set the [`FileMetadataCache`] for this reader
    pub fn with_metadata_cache(
        mut self,
        metadata_cache: Option<Arc<FileMetadataCache>>,
    ) -> Self {
        self.metadata_cache = metadata_cache;
        self
    }

    /// Set the metadata size hint for this reader.
    ///
    /// See [`DFParquetMetadata::with_metadata_size_hint`] for more details.
    pub fn with_metadata_hint(mut self, metadata_size_hint: Option<usize>) -> Self {
        self.metadata_size_hint = metadata_size_hint;
        self
    }
}

impl AsyncFileReader for ParquetFileReader {
    fn get_bytes(
        &mut self,
        range: Range<u64>,
    ) -> BoxFuture<'_, parquet::errors::Result<Bytes>> {
        let bytes_scanned = range.end - range.start;
        self.file_metrics.bytes_scanned.add(bytes_scanned as usize);
        async move {
            self.reader()
                .await?
                .read_range(range.into())
                .await
                .map_err(|e| ParquetError::External(Box::new(e)))
        }
        .boxed()
    }

    fn get_byte_ranges(
        &mut self,
        ranges: Vec<Range<u64>>,
    ) -> BoxFuture<'_, parquet::errors::Result<Vec<Bytes>>>
    where
        Self: Send,
    {
        let total: u64 = ranges.iter().map(|r| r.end - r.start).sum();
        self.file_metrics.bytes_scanned.add(total as usize);
        async move {
            self.reader()
                .await?
                .read_ranges(ranges)
                .await
                .map_err(|e| ParquetError::External(Box::new(e)))
        }
        .boxed()
    }

    fn get_metadata<'a>(
        &'a mut self,
        options: Option<&'a ArrowReaderOptions>,
    ) -> BoxFuture<'a, parquet::errors::Result<Arc<ParquetMetaData>>> {
        let object_meta = self.partitioned_file.object_meta.clone();
        let metadata_cache = self.metadata_cache.clone();

        async move {
            #[cfg(feature = "parquet_encryption")]
            let file_decryption_properties = options
                .and_then(|o| o.file_decryption_properties())
                .map(Arc::clone);

            #[cfg(not(feature = "parquet_encryption"))]
            let file_decryption_properties = None;

            let page_index_policy = options.map(|o| o.column_index_policy());

            DFParquetMetadata::new(self.reader().await?.as_ref(), &object_meta)
                .with_decryption_properties(file_decryption_properties)
                .with_file_metadata_cache(metadata_cache, self.store.id())
                .with_metadata_size_hint(self.metadata_size_hint)
                .with_page_index_policy(page_index_policy)
                .fetch_metadata()
                .await
                .map_err(|e| {
                    ParquetError::General(format!(
                        "Failed to fetch metadata for file {}: {e}",
                        object_meta.location,
                    ))
                })
        }
        .boxed()
    }
}

impl Drop for ParquetFileReader {
    fn drop(&mut self) {
        self.file_metrics
            .scan_efficiency_ratio
            .add_part(self.file_metrics.bytes_scanned.value());
        // Multiple ParquetFileReaders may run, so we set_total to avoid adding the total multiple times
        self.file_metrics
            .scan_efficiency_ratio
            .set_total(self.partitioned_file.object_meta.size as usize);
    }
}

/// Wrapper to implement [`FileMetadata`] for [`ParquetMetaData`].
pub struct CachedParquetMetaData(Arc<ParquetMetaData>);

impl CachedParquetMetaData {
    pub fn new(metadata: Arc<ParquetMetaData>) -> Self {
        Self(metadata)
    }

    pub fn parquet_metadata(&self) -> &Arc<ParquetMetaData> {
        &self.0
    }
}

impl FileMetadata for CachedParquetMetaData {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn memory_size(&self) -> usize {
        self.0.memory_size()
    }

    fn extra_info(&self) -> HashMap<String, String> {
        let page_index =
            self.0.column_index().is_some() && self.0.offset_index().is_some();
        HashMap::from([("page_index".to_owned(), page_index.to_string())])
    }
}
