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

//! [`ParquetFileReaderFactory`] and [`DefaultParquetFileReaderFactory`] for
//! low level control of parquet file readers

use crate::ParquetFileMetrics;
use crate::ParquetAccessPlan;
use crate::metadata::DFParquetMetadata;
use bytes::Bytes;
use datafusion_datasource::PartitionedFile;
use datafusion_execution::cache::cache_manager::FileMetadata;
use datafusion_execution::cache::cache_manager::FileMetadataCache;
use datafusion_physical_plan::metrics::ExecutionPlanMetricsSet;
use futures::FutureExt;
use futures::future::BoxFuture;
use object_store::ObjectStore;
use parquet::arrow::arrow_reader::{ArrowReaderMetadata, ArrowReaderOptions};
use parquet::arrow::async_reader::{AsyncFileReader, ParquetObjectReader};
use parquet::file::metadata::ParquetMetaData;
use std::any::Any;
use std::collections::HashMap;
use std::fmt::Debug;
use std::ops::Range;
use std::sync::{Arc, Mutex};

#[derive(Debug, Clone)]
pub struct SharedParquetMetadata(Arc<Mutex<Option<Arc<ParquetMetaData>>>>);

impl SharedParquetMetadata {
    pub fn new() -> Self {
        Self(Arc::new(Mutex::new(None)))
    }

    pub fn get(&self) -> Option<Arc<ParquetMetaData>> {
        let guard = match self.0.lock() {
            Ok(guard) => guard,
            Err(poisoned) => poisoned.into_inner(),
        };
        guard.clone()
    }

    pub fn set_if_empty(&self, metadata: Arc<ParquetMetaData>) {
        let mut guard = match self.0.lock() {
            Ok(guard) => guard,
            Err(poisoned) => poisoned.into_inner(),
        };

        if guard.is_none() {
            *guard = Some(metadata);
        }
    }
}

#[derive(Debug, Clone)]
pub struct SharedArrowReaderMetadata(Arc<Mutex<Option<ArrowReaderMetadata>>>);

impl SharedArrowReaderMetadata {
    pub fn new() -> Self {
        Self(Arc::new(Mutex::new(None)))
    }

    pub fn get(&self) -> Option<ArrowReaderMetadata> {
        let guard = match self.0.lock() {
            Ok(guard) => guard,
            Err(poisoned) => poisoned.into_inner(),
        };
        guard.clone()
    }

    pub fn set_if_empty(&self, metadata: ArrowReaderMetadata) {
        let mut guard = match self.0.lock() {
            Ok(guard) => guard,
            Err(poisoned) => poisoned.into_inner(),
        };

        if guard.is_none() {
            *guard = Some(metadata);
        }
    }
}

#[derive(Debug, Clone)]
pub struct ParquetFileScanPlan {
    access_plan: Option<ParquetAccessPlan>,
    shared_metadata: SharedParquetMetadata,
    shared_reader_metadata: SharedArrowReaderMetadata,
}

impl ParquetFileScanPlan {
    pub fn new(
        access_plan: Option<ParquetAccessPlan>,
        shared_metadata: SharedParquetMetadata,
        shared_reader_metadata: SharedArrowReaderMetadata,
    ) -> Self {
        Self {
            access_plan,
            shared_metadata,
            shared_reader_metadata,
        }
    }

    pub fn access_plan(&self) -> Option<&ParquetAccessPlan> {
        self.access_plan.as_ref()
    }

    pub fn shared_metadata(&self) -> SharedParquetMetadata {
        self.shared_metadata.clone()
    }

    pub fn shared_reader_metadata(&self) -> SharedArrowReaderMetadata {
        self.shared_reader_metadata.clone()
    }
}

/// Interface for reading parquet files.
///
/// The combined implementations of [`ParquetFileReaderFactory`] and
/// [`AsyncFileReader`] can be used to provide custom data access operations
/// such as pre-cached metadata, I/O coalescing, etc.
///
/// See [`DefaultParquetFileReaderFactory`] for a simple implementation.
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
/// 1. Reads parquet directly from an underlying [`ObjectStore`] instance.
/// 2. Reads the footer and page metadata on demand.
/// 3. Does not cache metadata or coalesce I/O operations.
#[derive(Debug)]
pub struct DefaultParquetFileReaderFactory {
    store: Arc<dyn ObjectStore>,
}

impl DefaultParquetFileReaderFactory {
    /// Create a new `DefaultParquetFileReaderFactory`.
    pub fn new(store: Arc<dyn ObjectStore>) -> Self {
        Self { store }
    }
}

/// Implements [`AsyncFileReader`] for a parquet file in object storage.
///
/// This implementation uses the [`ParquetObjectReader`] to read data from the
/// object store on demand, as required, tracking the number of bytes read.
///
/// This implementation does not coalesce I/O operations or cache bytes. Such
/// optimizations can be done either at the object store level or by providing a
/// custom implementation of [`ParquetFileReaderFactory`].
pub struct ParquetFileReader {
    pub file_metrics: ParquetFileMetrics,
    pub inner: ParquetObjectReader,
    pub partitioned_file: PartitionedFile,
    pub shared_metadata: Option<SharedParquetMetadata>,
}

impl AsyncFileReader for ParquetFileReader {
    fn get_bytes(
        &mut self,
        range: Range<u64>,
    ) -> BoxFuture<'_, parquet::errors::Result<Bytes>> {
        let bytes_scanned = range.end - range.start;
        self.file_metrics.bytes_scanned.add(bytes_scanned as usize);
        self.inner.get_bytes(range)
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
        self.inner.get_byte_ranges(ranges)
    }

    fn get_metadata<'a>(
        &'a mut self,
        options: Option<&'a ArrowReaderOptions>,
    ) -> BoxFuture<'a, parquet::errors::Result<Arc<ParquetMetaData>>> {
        if let Some(shared_metadata) = &self.shared_metadata
            && let Some(metadata) = shared_metadata.get()
        {
            return futures::future::ready(Ok(metadata)).boxed();
        }

        let shared_metadata = self.shared_metadata.clone();
        let fut = self.inner.get_metadata(options);
        async move {
            let metadata = fut.await?;
            if let Some(shared_metadata) = shared_metadata {
                shared_metadata.set_if_empty(Arc::clone(&metadata));
            }
            Ok(metadata)
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
        let store = Arc::clone(&self.store);
        let mut inner = ParquetObjectReader::new(
            store,
            partitioned_file.object_meta.location.clone(),
        )
        .with_file_size(partitioned_file.object_meta.size);

        if let Some(hint) = metadata_size_hint {
            inner = inner.with_footer_size_hint(hint)
        };

        let shared_metadata = partitioned_file
            .extensions
            .as_ref()
            .and_then(|extensions| extensions.downcast_ref::<ParquetFileScanPlan>())
            .map(|plan| plan.shared_metadata());

        Ok(Box::new(ParquetFileReader {
            inner,
            file_metrics,
            partitioned_file,
            shared_metadata,
        }))
    }
}

/// Implementation of [`ParquetFileReaderFactory`] supporting the caching of footer and page
/// metadata. Reads and updates the [`FileMetadataCache`] with the [`ParquetMetaData`] data.
/// This reader always loads the entire metadata (including page index, unless the file is
/// encrypted), even if not required by the current query, to ensure it is always available for
/// those that need it.
#[derive(Debug)]
pub struct CachedParquetFileReaderFactory {
    store: Arc<dyn ObjectStore>,
    metadata_cache: Arc<dyn FileMetadataCache>,
}

impl CachedParquetFileReaderFactory {
    pub fn new(
        store: Arc<dyn ObjectStore>,
        metadata_cache: Arc<dyn FileMetadataCache>,
    ) -> Self {
        Self {
            store,
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
        let store = Arc::clone(&self.store);

        let mut inner = ParquetObjectReader::new(
            store,
            partitioned_file.object_meta.location.clone(),
        )
        .with_file_size(partitioned_file.object_meta.size);

        if let Some(hint) = metadata_size_hint {
            inner = inner.with_footer_size_hint(hint)
        };

        Ok(Box::new(CachedParquetFileReader::new(
            file_metrics,
            Arc::clone(&self.store),
            inner,
            partitioned_file,
            Arc::clone(&self.metadata_cache),
            metadata_size_hint,
        )))
    }
}

/// Implements [`AsyncFileReader`] for a Parquet file in object storage. Reads the file metadata
/// from the [`FileMetadataCache`], if available, otherwise reads it directly from the file and then
/// updates the cache.
pub struct CachedParquetFileReader {
    pub file_metrics: ParquetFileMetrics,
    store: Arc<dyn ObjectStore>,
    pub inner: ParquetObjectReader,
    partitioned_file: PartitionedFile,
    metadata_cache: Arc<dyn FileMetadataCache>,
    metadata_size_hint: Option<usize>,
}

impl CachedParquetFileReader {
    pub fn new(
        file_metrics: ParquetFileMetrics,
        store: Arc<dyn ObjectStore>,
        inner: ParquetObjectReader,
        partitioned_file: PartitionedFile,
        metadata_cache: Arc<dyn FileMetadataCache>,
        metadata_size_hint: Option<usize>,
    ) -> Self {
        Self {
            file_metrics,
            store,
            inner,
            partitioned_file,
            metadata_cache,
            metadata_size_hint,
        }
    }
}

impl AsyncFileReader for CachedParquetFileReader {
    fn get_bytes(
        &mut self,
        range: Range<u64>,
    ) -> BoxFuture<'_, parquet::errors::Result<Bytes>> {
        let bytes_scanned = range.end - range.start;
        self.file_metrics.bytes_scanned.add(bytes_scanned as usize);
        self.inner.get_bytes(range)
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
        self.inner.get_byte_ranges(ranges)
    }

    fn get_metadata<'a>(
        &'a mut self,
        #[cfg_attr(not(feature = "parquet_encryption"), expect(unused_variables))]
        options: Option<&'a ArrowReaderOptions>,
    ) -> BoxFuture<'a, parquet::errors::Result<Arc<ParquetMetaData>>> {
        let object_meta = self.partitioned_file.object_meta.clone();
        let metadata_cache = Arc::clone(&self.metadata_cache);

        async move {
            #[cfg(feature = "parquet_encryption")]
            let file_decryption_properties = options
                .and_then(|o| o.file_decryption_properties())
                .map(Arc::clone);

            #[cfg(not(feature = "parquet_encryption"))]
            let file_decryption_properties = None;

            DFParquetMetadata::new(&self.store, &object_meta)
                .with_decryption_properties(file_decryption_properties)
                .with_file_metadata_cache(Some(Arc::clone(&metadata_cache)))
                .with_metadata_size_hint(self.metadata_size_hint)
                .fetch_metadata()
                .await
                .map_err(|e| {
                    parquet::errors::ParquetError::General(format!(
                        "Failed to fetch metadata for file {}: {e}",
                        object_meta.location,
                    ))
                })
        }
        .boxed()
    }
}

impl Drop for CachedParquetFileReader {
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
