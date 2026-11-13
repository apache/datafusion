// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: See the NOTICE file distributed with this work for additional information regarding copyright ownership
// SPDX-FileContributor: Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements

//! [`ParquetFileReaderFactory`] and [`DefaultParquetFileReaderFactory`] for
//! low level control of parquet file readers

use crate::ParquetFileMetrics;
use crate::metadata::DFParquetMetadata;
use arrow::datatypes::SchemaRef;
use bytes::Bytes;
use datafusion_common::HashMap;
use datafusion_common::not_impl_err;
use datafusion_datasource::PartitionedFile;
use datafusion_execution::cache::cache_manager::FileMetadata;
use datafusion_execution::cache::cache_manager::FileMetadataCache;
use datafusion_physical_plan::metrics::ExecutionPlanMetricsSet;
use futures::FutureExt;
use futures::TryFutureExt;
use futures::future::BoxFuture;
use object_store::{ObjectStore, ObjectStoreExt};
use parquet::arrow::arrow_reader::ArrowReaderOptions;
use parquet::arrow::async_reader::AsyncFileReader;
use parquet::errors::ParquetError;
use parquet::file::metadata::ParquetMetaData;
use parquet::schema::types::SchemaDescriptor;
use std::any::Any;
use std::fmt::Debug;
use std::ops::Range;
use std::sync::Arc;

/// Derives a file's Arrow schema from its physical Parquet schema during opening.
///
/// Install with [`ParquetSource::with_schema_provider`](crate::source::ParquetSource::with_schema_provider).
/// This runs after the footer is loaded, before Arrow schema inference or filter
/// preparation, so files may have different nested layouts without loading their
/// footers during planning. The original Parquet metadata is retained.
pub trait ParquetFileSchemaProvider: Debug + Send + Sync + 'static {
    /// Return the complete Arrow schema for the physical file, excluding partition
    /// and virtual columns. It must be compatible with the Parquet schema under
    /// [`ArrowReaderOptions::with_schema`], which also ignores advisory `ARROW:schema`
    /// metadata. Errors fail the file open.
    ///
    /// An explicit [`PartitionedFile::arrow_schema`] takes precedence and skips
    /// this call. Existing table-schema and INT96 coercions are applied afterwards.
    fn schema(
        &self,
        parquet_schema: &SchemaDescriptor,
    ) -> datafusion_common::Result<SchemaRef>;
}

/// Arguments to [`ParquetFileReaderFactory::create_reader_with_args`].
///
/// Create with [`Self::new`] and set optional arguments with the `with_*`
/// methods. New arguments are added here rather than as new parameters of
/// [`ParquetFileReaderFactory`] methods, so adding one does not change the
/// trait.
#[derive(Debug, Clone)]
pub struct CreateReaderArgs {
    partition_index: usize,
    partitioned_file: PartitionedFile,
    metrics: ExecutionPlanMetricsSet,
    metadata_size_hint: Option<usize>,
    file_metrics: Option<ParquetFileMetrics>,
}

impl CreateReaderArgs {
    /// Create arguments for reading `partitioned_file` in partition
    /// `partition_index`, reporting metrics into `metrics`.
    pub fn new(
        partition_index: usize,
        partitioned_file: PartitionedFile,
        metrics: &ExecutionPlanMetricsSet,
    ) -> Self {
        Self {
            partition_index,
            partitioned_file,
            metrics: metrics.clone(),
            metadata_size_hint: None,
            file_metrics: None,
        }
    }

    /// If specified, the first IO reads this many bytes from the end of the
    /// file, to fetch the footer in a single request.
    pub fn with_metadata_size_hint(mut self, metadata_size_hint: Option<usize>) -> Self {
        self.metadata_size_hint = metadata_size_hint;
        self
    }

    /// The [`ParquetFileMetrics`] the caller has already registered for this
    /// file. The reader should record into this set rather than registering
    /// another one in [`Self::metrics`].
    pub fn with_file_metrics(mut self, file_metrics: ParquetFileMetrics) -> Self {
        self.file_metrics = Some(file_metrics);
        self
    }

    /// Index of the partition the file is read in (for reporting metrics).
    pub fn partition_index(&self) -> usize {
        self.partition_index
    }

    /// The file to be read.
    pub fn partitioned_file(&self) -> &PartitionedFile {
        &self.partitioned_file
    }

    /// Consume the arguments, returning the file to be read.
    pub fn into_partitioned_file(self) -> PartitionedFile {
        self.partitioned_file
    }

    /// The execution metrics of the scan.
    pub fn metrics(&self) -> &ExecutionPlanMetricsSet {
        &self.metrics
    }

    /// See [`Self::with_metadata_size_hint`].
    pub fn metadata_size_hint(&self) -> Option<usize> {
        self.metadata_size_hint
    }

    /// See [`Self::with_file_metrics`].
    pub fn file_metrics(&self) -> Option<&ParquetFileMetrics> {
        self.file_metrics.as_ref()
    }

    /// The file metrics the reader should record into: the set from
    /// [`Self::with_file_metrics`] if the caller provided one, otherwise a new
    /// set registered in [`Self::metrics`].
    pub fn file_metrics_or_register(&self) -> ParquetFileMetrics {
        self.file_metrics.clone().unwrap_or_else(|| {
            ParquetFileMetrics::new(
                self.partition_index,
                self.partitioned_file.object_meta.location.as_ref(),
                &self.metrics,
            )
        })
    }
}

/// Interface for reading Apache Parquet files.
///
/// The combined implementations of [`ParquetFileReaderFactory`] and
/// [`AsyncFileReader`] can be used to provide custom data access operations
/// such as pre-cached metadata, I/O coalescing, etc.
///
/// Implement [`Self::create_reader_with_args`]. See
/// [`DefaultParquetFileReaderFactory`] for a simple implementation.
pub trait ParquetFileReaderFactory: Debug + Send + Sync + 'static {
    /// Provides an `AsyncFileReader` for reading data from a parquet file specified
    ///
    /// # Arguments
    /// * partition_index - Index of the partition (for reporting metrics)
    /// * file - The file to be read
    /// * metadata_size_hint - If specified, the first IO reads this many bytes from the footer
    /// * metrics - Execution metrics
    ///
    /// The default implementation returns a not-implemented error: factories
    /// implement [`Self::create_reader_with_args`] instead.
    #[deprecated(
        since = "56.0.0",
        note = "Implement and call `create_reader_with_args` instead"
    )]
    fn create_reader(
        &self,
        partition_index: usize,
        partitioned_file: PartitionedFile,
        metadata_size_hint: Option<usize>,
        metrics: &ExecutionPlanMetricsSet,
    ) -> datafusion_common::Result<Box<dyn AsyncFileReader + Send>> {
        let _ = (
            partition_index,
            partitioned_file,
            metadata_size_hint,
            metrics,
        );
        not_impl_err!(
            "ParquetFileReaderFactory::create_reader is deprecated; implement create_reader_with_args"
        )
    }

    /// Provides an `AsyncFileReader` for reading data from the parquet file
    /// described by `args`.
    ///
    /// If [`CreateReaderArgs::file_metrics`] is set, the reader should record
    /// into that set rather than registering another one;
    /// [`CreateReaderArgs::file_metrics_or_register`] does this.
    ///
    /// # Notes
    ///
    /// If the resulting [`AsyncFileReader`]  returns `ParquetMetaData` without
    /// page index information, the reader will load it on demand. Thus it is important
    /// to ensure that the returned `ParquetMetaData` has the necessary information
    /// if you wish to avoid a subsequent I/O
    ///
    /// The default implementation calls the deprecated [`Self::create_reader`]
    /// and ignores [`CreateReaderArgs::file_metrics`], so factories written
    /// against that method keep their behavior.
    fn create_reader_with_args(
        &self,
        args: CreateReaderArgs,
    ) -> datafusion_common::Result<Box<dyn AsyncFileReader + Send>> {
        let CreateReaderArgs {
            partition_index,
            partitioned_file,
            metrics,
            metadata_size_hint,
            file_metrics: _,
        } = args;
        #[expect(deprecated)]
        self.create_reader(
            partition_index,
            partitioned_file,
            metadata_size_hint,
            &metrics,
        )
    }
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

impl ParquetFileReaderFactory for DefaultParquetFileReaderFactory {
    // Kept so direct callers of the deprecated method see no change.
    fn create_reader(
        &self,
        partition_index: usize,
        partitioned_file: PartitionedFile,
        metadata_size_hint: Option<usize>,
        metrics: &ExecutionPlanMetricsSet,
    ) -> datafusion_common::Result<Box<dyn AsyncFileReader + Send>> {
        self.create_reader_with_args(
            CreateReaderArgs::new(partition_index, partitioned_file, metrics)
                .with_metadata_size_hint(metadata_size_hint),
        )
    }

    fn create_reader_with_args(
        &self,
        args: CreateReaderArgs,
    ) -> datafusion_common::Result<Box<dyn AsyncFileReader + Send>> {
        let file_metrics = args.file_metrics_or_register();
        let metadata_size_hint = args.metadata_size_hint();
        let reader = ParquetFileReader::new(
            file_metrics,
            Arc::clone(&self.store),
            args.into_partitioned_file(),
        )
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
    store: Arc<dyn ObjectStore>,
    metadata_cache: Arc<FileMetadataCache>,
}

impl CachedParquetFileReaderFactory {
    pub fn new(
        store: Arc<dyn ObjectStore>,
        metadata_cache: Arc<FileMetadataCache>,
    ) -> Self {
        Self {
            store,
            metadata_cache,
        }
    }
}

impl ParquetFileReaderFactory for CachedParquetFileReaderFactory {
    // Kept so direct callers of the deprecated method see no change.
    fn create_reader(
        &self,
        partition_index: usize,
        partitioned_file: PartitionedFile,
        metadata_size_hint: Option<usize>,
        metrics: &ExecutionPlanMetricsSet,
    ) -> datafusion_common::Result<Box<dyn AsyncFileReader + Send>> {
        self.create_reader_with_args(
            CreateReaderArgs::new(partition_index, partitioned_file, metrics)
                .with_metadata_size_hint(metadata_size_hint),
        )
    }

    fn create_reader_with_args(
        &self,
        args: CreateReaderArgs,
    ) -> datafusion_common::Result<Box<dyn AsyncFileReader + Send>> {
        let file_metrics = args.file_metrics_or_register();
        let metadata_size_hint = args.metadata_size_hint();
        let reader = ParquetFileReader::new(
            file_metrics,
            Arc::clone(&self.store),
            args.into_partitioned_file(),
        )
        .with_metadata_hint(metadata_size_hint)
        .with_metadata_cache(Some(Arc::clone(&self.metadata_cache)));

        Ok(Box::new(reader))
    }
}

/// Implements [`AsyncFileReader`] for a parquet file in object storage.
///
/// This implementation reads data directly from the underlying [`ObjectStore`]
/// on demand, as required, tracking the number of bytes read.
///
/// When configured via [`Self::with_metadata_cache`], [`Self::get_metadata`]
/// reads footer and page metadata from the cache when available and populates
/// the cache otherwise. Without a cache, metadata is fetched fresh on every call.
///
/// # Notes
///
/// This implementation does not coalesce I/O operations or cache bytes. Such
/// optimizations can be done either at the object store level or by providing
/// a custom implementation of [`ParquetFileReaderFactory`].
pub struct ParquetFileReader {
    file_metrics: ParquetFileMetrics,
    store: Arc<dyn ObjectStore>,
    partitioned_file: PartitionedFile,
    metadata_cache: Option<Arc<FileMetadataCache>>,
    metadata_size_hint: Option<usize>,
}

impl ParquetFileReader {
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
        store: Arc<dyn ObjectStore>,
        partitioned_file: PartitionedFile,
    ) -> Self {
        Self {
            file_metrics,
            store,
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
        self.store
            .get_range(&self.partitioned_file.object_meta.location, range)
            .map_err(|e| ParquetError::External(Box::new(e)))
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
            self.store
                .get_ranges(&self.partitioned_file.object_meta.location, &ranges)
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

            DFParquetMetadata::new(&self.store, &object_meta)
                .with_decryption_properties(file_decryption_properties)
                .with_file_metadata_cache(metadata_cache)
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
        let page_index = self
            .0
            .page_index()
            .is_some_and(|page_index| page_index.is_complete());
        HashMap::from([("page_index".to_owned(), page_index.to_string())])
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion_common::DataFusionError;
    use datafusion_execution::cache::cache_manager::CachedFileMetadataEntry;
    use datafusion_execution::cache::default_cache::DefaultCache;
    use object_store::memory::InMemory;
    use object_store::path::Path;
    use std::sync::Mutex;

    fn store() -> Arc<dyn ObjectStore> {
        Arc::new(InMemory::new())
    }

    fn file() -> PartitionedFile {
        PartitionedFile::new("file.parquet".to_string(), 100)
    }

    /// How many sets of file metrics are registered in `metrics`.
    fn registered_sets(metrics: &ExecutionPlanMetricsSet) -> usize {
        metrics
            .clone_inner()
            .iter()
            .filter(|metric| metric.value().name() == "bytes_scanned")
            .count()
    }

    /// A factory written against the deprecated `create_reader` only.
    #[derive(Debug)]
    struct LegacyFactory {
        inner: DefaultParquetFileReaderFactory,
        calls: Mutex<Vec<(usize, Option<usize>)>>,
    }

    impl ParquetFileReaderFactory for LegacyFactory {
        fn create_reader(
            &self,
            partition_index: usize,
            partitioned_file: PartitionedFile,
            metadata_size_hint: Option<usize>,
            metrics: &ExecutionPlanMetricsSet,
        ) -> datafusion_common::Result<Box<dyn AsyncFileReader + Send>> {
            self.calls
                .lock()
                .unwrap()
                .push((partition_index, metadata_size_hint));
            #[expect(deprecated)]
            self.inner.create_reader(
                partition_index,
                partitioned_file,
                metadata_size_hint,
                metrics,
            )
        }
    }

    /// A factory implementing only `create_reader_with_args`.
    #[derive(Debug)]
    struct ArgsOnlyFactory(DefaultParquetFileReaderFactory);

    impl ParquetFileReaderFactory for ArgsOnlyFactory {
        fn create_reader_with_args(
            &self,
            args: CreateReaderArgs,
        ) -> datafusion_common::Result<Box<dyn AsyncFileReader + Send>> {
            self.0.create_reader_with_args(args)
        }
    }

    #[test]
    fn create_reader_with_args_falls_back_to_create_reader() {
        let factory = LegacyFactory {
            inner: DefaultParquetFileReaderFactory::new(store()),
            calls: Mutex::new(vec![]),
        };
        let metrics = ExecutionPlanMetricsSet::new();
        let args =
            CreateReaderArgs::new(3, file(), &metrics).with_metadata_size_hint(Some(512));

        factory.create_reader_with_args(args).unwrap();

        assert_eq!(*factory.calls.lock().unwrap(), vec![(3, Some(512))]);
        assert_eq!(registered_sets(&metrics), 1);
    }

    #[test]
    fn deprecated_create_reader_is_not_implemented_by_default() {
        let factory = ArgsOnlyFactory(DefaultParquetFileReaderFactory::new(store()));
        let metrics = ExecutionPlanMetricsSet::new();

        #[expect(deprecated)]
        let result = factory.create_reader(0, file(), None, &metrics);

        assert!(matches!(result, Err(DataFusionError::NotImplemented(_))));
    }

    #[test]
    fn built_in_factories_record_into_the_given_file_metrics() {
        let store = store();
        let factories: Vec<Arc<dyn ParquetFileReaderFactory>> = vec![
            Arc::new(DefaultParquetFileReaderFactory::new(Arc::clone(&store))),
            Arc::new(CachedParquetFileReaderFactory::new(
                Arc::clone(&store),
                Arc::new(DefaultCache::<Path, CachedFileMetadataEntry>::new(
                    1024 * 1024,
                )),
            )),
        ];
        for factory in factories {
            let metrics = ExecutionPlanMetricsSet::new();
            let file_metrics = ParquetFileMetrics::new(0, "file.parquet", &metrics);

            factory
                .create_reader_with_args(
                    CreateReaderArgs::new(0, file(), &metrics)
                        .with_file_metrics(file_metrics),
                )
                .unwrap();
            assert_eq!(
                registered_sets(&metrics),
                1,
                "{factory:?} with file metrics"
            );

            // Without file metrics the factory registers its own set, as
            // `create_reader` always did.
            factory
                .create_reader_with_args(CreateReaderArgs::new(0, file(), &metrics))
                .unwrap();
            assert_eq!(registered_sets(&metrics), 2, "{factory:?} without");

            #[expect(deprecated)]
            factory.create_reader(0, file(), None, &metrics).unwrap();
            assert_eq!(registered_sets(&metrics), 3, "{factory:?} create_reader");
        }
    }
}
