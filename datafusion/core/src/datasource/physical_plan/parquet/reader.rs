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

use crate::datasource::physical_plan::{FileMeta, ParquetFileMetrics};
use bytes::Bytes;
use dashmap::DashMap;
use datafusion_physical_plan::metrics::ExecutionPlanMetricsSet;
use futures::future::{BoxFuture, Either};
use futures::FutureExt;
use object_store::ObjectStore;
use parquet::arrow::async_reader::{AsyncFileReader, ParquetObjectReader};
use parquet::file::metadata::ParquetMetaData;
use std::fmt::Debug;
use std::ops::Range;
use std::sync::{Arc, OnceLock};

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
    /// * file_meta - The file to be read
    /// * metadata_size_hint - If specified, the first IO reads this many bytes from the footer
    /// * metrics - Execution metrics
    fn create_reader(
        &self,
        partition_index: usize,
        file_meta: FileMeta,
        metadata_size_hint: Option<usize>,
        metrics: &ExecutionPlanMetricsSet,
    ) -> datafusion_common::Result<Box<dyn AsyncFileReader + Send>>;
}

/// Default implementation of [`ParquetFileReaderFactory`]
///
/// This implementation:
/// 1. Reads parquet directly from an underlying [`ObjectStore`] instance.
/// 2. Reads the footer and page metadata on demand.
/// 3. Does not cache metadata
/// 4. Does not coalesce I/O operations.
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

/// Caching implementation of [`ParquetFileReaderFactory`]
///
/// This implementation:
/// 1. Reads parquet directly from an underlying [`ObjectStore`] instance.
/// 2. Reads the footer and page metadata on demand (but may cache them in the future).
/// 3. Ches metadata
/// 4. Does not coalesce I/O operations.
#[derive(Debug)]
pub struct CachedParquetFileReaderFactory {
    store: Arc<dyn ObjectStore>,
    /// The parquet metadata for each file in the index, keyed by the file name
    /// (e.g. `file1.parquet`).
    ///
    /// There are two layers of Arc. The outer one allows sharing the lock while a future is
    /// executing, while the inner one shares the metadata between readers once it is cached
    metadata: DashMap<String, Arc<OnceLock<Arc<ParquetMetaData>>>>,
}

impl CachedParquetFileReaderFactory {
    /// Create a new `CachedParquetFileReaderFactory`.
    pub fn new(store: Arc<dyn ObjectStore>) -> Self {
        Self {
            store,
            metadata: DashMap::new(),
        }
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
///
/// It will cache file metadata if `metadata_cache` is set.
pub(crate) struct ParquetFileReader {
    pub file_metrics: ParquetFileMetrics,
    pub inner: ParquetObjectReader,
    pub metadata_cache: Option<Arc<OnceLock<Arc<ParquetMetaData>>>>,
}

impl AsyncFileReader for ParquetFileReader {
    fn get_bytes(
        &mut self,
        range: Range<usize>,
    ) -> BoxFuture<'_, parquet::errors::Result<Bytes>> {
        self.file_metrics.bytes_scanned.add(range.end - range.start);
        self.inner.get_bytes(range)
    }

    fn get_byte_ranges(
        &mut self,
        ranges: Vec<Range<usize>>,
    ) -> BoxFuture<'_, parquet::errors::Result<Vec<Bytes>>>
    where
        Self: Send,
    {
        let total = ranges.iter().map(|r| r.end - r.start).sum();
        self.file_metrics.bytes_scanned.add(total);
        self.inner.get_byte_ranges(ranges)
    }

    fn get_metadata(
        &mut self,
    ) -> BoxFuture<'_, parquet::errors::Result<Arc<ParquetMetaData>>> {
        Box::pin(match &self.metadata_cache {
            None => Either::Left(self.inner.get_metadata()),
            Some(metadata_cache) => Either::Right(match metadata_cache.get() {
                Some(metadata) => {
                    Either::Left(std::future::ready(Ok(Arc::clone(metadata))))
                }
                None => {
                    let metadata_cache = Arc::clone(&metadata_cache);
                    Either::Right(self.inner.get_metadata().inspect(move |metadata| {
                        if let Ok(metadata) = metadata {
                            // TODO: use metadata.try_insert when
                            // https://github.com/rust-lang/rust/issues/116693 is stabilized
                            metadata_cache.get_or_init(|| Arc::clone(metadata));
                        }
                    }))
                }
            }),
        })
    }
}

impl ParquetFileReaderFactory for DefaultParquetFileReaderFactory {
    fn create_reader(
        &self,
        partition_index: usize,
        file_meta: FileMeta,
        metadata_size_hint: Option<usize>,
        metrics: &ExecutionPlanMetricsSet,
    ) -> datafusion_common::Result<Box<dyn AsyncFileReader + Send>> {
        let file_metrics = ParquetFileMetrics::new(
            partition_index,
            file_meta.location().as_ref(),
            metrics,
        );
        let store = Arc::clone(&self.store);
        let mut inner = ParquetObjectReader::new(store, file_meta.object_meta);

        if let Some(hint) = metadata_size_hint {
            inner = inner.with_footer_size_hint(hint)
        };

        Ok(Box::new(ParquetFileReader {
            inner,
            file_metrics,
            metadata_cache: None,
        }))
    }
}

impl ParquetFileReaderFactory for CachedParquetFileReaderFactory {
    fn create_reader(
        &self,
        partition_index: usize,
        file_meta: FileMeta,
        metadata_size_hint: Option<usize>,
        metrics: &ExecutionPlanMetricsSet,
    ) -> datafusion_common::Result<Box<dyn AsyncFileReader + Send>> {
        let filename = file_meta
            .location()
            .parts()
            .last()
            .expect("No path in location")
            .as_ref()
            .to_string();

        // TODO: cache metrics?
        let file_metrics = ParquetFileMetrics::new(
            partition_index,
            file_meta.location().as_ref(),
            metrics,
        );
        let object_store = Arc::clone(&self.store);
        let mut inner = ParquetObjectReader::new(object_store, file_meta.object_meta);

        if let Some(hint) = metadata_size_hint {
            inner = inner.with_footer_size_hint(hint)
        };

        let metadata = Arc::clone(
            self.metadata
                .entry(filename)
                .or_insert_with(|| Arc::new(OnceLock::new()))
                .value(),
        );
        Ok(Box::new(ParquetFileReader {
            inner,
            file_metrics,
            metadata_cache: Some(metadata),
        }))
    }
}
