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

use crate::cache::CacheAccessor;
use crate::cache::cache_unit::DefaultFilesMetadataCache;
use datafusion_common::{Result, Statistics};
use object_store::ObjectMeta;
use object_store::path::Path;
use std::any::Any;
use std::collections::HashMap;
use std::fmt::{Debug, Formatter};
use std::sync::Arc;

/// A cache for [`Statistics`].
///
/// If enabled via [`CacheManagerConfig::with_files_statistics_cache`] this
/// cache avoids inferring the same file statistics repeatedly during the
/// session lifetime.
///
/// See [`crate::runtime_env::RuntimeEnv`] for more details
pub type FileStatisticsCache =
    Arc<dyn CacheAccessor<Path, Arc<Statistics>, Extra = ObjectMeta>>;

/// Cache for storing the [`ObjectMeta`]s that result from listing a path
///
/// Listing a path means doing an object store "list" operation or `ls`
/// command on the local filesystem. This operation can be expensive,
/// especially when done over remote object stores.
///
/// See [`crate::runtime_env::RuntimeEnv`] for more details
pub type ListFilesCache =
    Arc<dyn CacheAccessor<Path, Arc<Vec<ObjectMeta>>, Extra = ObjectMeta>>;

/// Generic file-embedded metadata used with [`FileMetadataCache`].
///
/// For example, Parquet footers and page metadata can be represented
/// using this trait.
///
/// See [`crate::runtime_env::RuntimeEnv`] for more details
pub trait FileMetadata: Any + Send + Sync {
    /// Returns the file metadata as [`Any`] so that it can be downcast to a specific
    /// implementation.
    fn as_any(&self) -> &dyn Any;

    /// Returns the size of the metadata in bytes.
    fn memory_size(&self) -> usize;

    /// Returns extra information about this entry (used by [`FileMetadataCache::list_entries`]).
    fn extra_info(&self) -> HashMap<String, String>;
}

/// Cache for file-embedded metadata.
///
/// This cache stores per-file metadata in the form of [`FileMetadata`],
///
/// For example, the built in [`ListingTable`] uses this cache to avoid parsing
/// Parquet footers multiple times for the same file.
///
/// DataFusion provides a default implementation, [`DefaultFilesMetadataCache`],
/// and users can also provide their own implementations to implement custom
/// caching strategies.
///
/// See [`crate::runtime_env::RuntimeEnv`] for more details.
///
/// [`ListingTable`]: https://docs.rs/datafusion/latest/datafusion/datasource/listing/struct.ListingTable.html
pub trait FileMetadataCache:
    CacheAccessor<ObjectMeta, Arc<dyn FileMetadata>, Extra = ObjectMeta>
{
    /// Returns the cache's memory limit in bytes.
    fn cache_limit(&self) -> usize;

    /// Updates the cache with a new memory limit in bytes.
    fn update_cache_limit(&self, limit: usize);

    /// Retrieves the information about the entries currently cached.
    fn list_entries(&self) -> HashMap<Path, FileMetadataCacheEntry>;
}

#[derive(Debug, Clone, PartialEq, Eq)]
/// Represents information about a cached metadata entry.
/// This is used to expose the metadata cache contents to outside modules.
pub struct FileMetadataCacheEntry {
    pub object_meta: ObjectMeta,
    /// Size of the cached metadata, in bytes.
    pub size_bytes: usize,
    /// Number of times this entry was retrieved.
    pub hits: usize,
    /// Additional object-specific information.
    pub extra: HashMap<String, String>,
}

impl Debug for dyn CacheAccessor<Path, Arc<Statistics>, Extra = ObjectMeta> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "Cache name: {} with length: {}", self.name(), self.len())
    }
}

impl Debug for dyn CacheAccessor<Path, Arc<Vec<ObjectMeta>>, Extra = ObjectMeta> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "Cache name: {} with length: {}", self.name(), self.len())
    }
}

impl Debug for dyn FileMetadataCache {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "Cache name: {} with length: {}", self.name(), self.len())
    }
}

/// Manages various caches used in DataFusion.
///
/// Following DataFusion design principles, DataFusion provides default cache
/// implementations, while also allowing users to provide their own custom cache
/// implementations by implementing the relevant traits.
///
/// See [`CacheManagerConfig`] for configuration options.
#[derive(Debug)]
pub struct CacheManager {
    file_statistic_cache: Option<FileStatisticsCache>,
    list_files_cache: Option<ListFilesCache>,
    file_metadata_cache: Arc<dyn FileMetadataCache>,
}

impl CacheManager {
    pub fn try_new(config: &CacheManagerConfig) -> Result<Arc<Self>> {
        let file_statistic_cache =
            config.table_files_statistics_cache.as_ref().map(Arc::clone);

        let list_files_cache = config.list_files_cache.as_ref().map(Arc::clone);

        let file_metadata_cache = config
            .file_metadata_cache
            .as_ref()
            .map(Arc::clone)
            .unwrap_or_else(|| {
                Arc::new(DefaultFilesMetadataCache::new(config.metadata_cache_limit))
            });

        // the cache memory limit might have changed, ensure the limit is updated
        file_metadata_cache.update_cache_limit(config.metadata_cache_limit);

        Ok(Arc::new(CacheManager {
            file_statistic_cache,
            list_files_cache,
            file_metadata_cache,
        }))
    }

    /// Get the cache of listing files statistics.
    pub fn get_file_statistic_cache(&self) -> Option<FileStatisticsCache> {
        self.file_statistic_cache.clone()
    }

    /// Get the cache for storing the result of listing [`ObjectMeta`]s under the same path.
    pub fn get_list_files_cache(&self) -> Option<ListFilesCache> {
        self.list_files_cache.clone()
    }

    /// Get the file embedded metadata cache.
    pub fn get_file_metadata_cache(&self) -> Arc<dyn FileMetadataCache> {
        Arc::clone(&self.file_metadata_cache)
    }

    /// Get the limit of the file embedded metadata cache.
    pub fn get_metadata_cache_limit(&self) -> usize {
        self.file_metadata_cache.cache_limit()
    }
}

pub const DEFAULT_METADATA_CACHE_LIMIT: usize = 50 * 1024 * 1024; // 50M

#[derive(Clone)]
pub struct CacheManagerConfig {
    /// Enable cache of files statistics when listing files.
    /// Avoid get same file statistics repeatedly in same datafusion session.
    /// Default is disable. Fow now only supports Parquet files.
    pub table_files_statistics_cache: Option<FileStatisticsCache>,
    /// Enable cache of file metadata when listing files.
    /// This setting avoids listing file meta of the same path repeatedly
    /// in same session, which may be expensive in certain situations (e.g. remote object storage).
    /// Note that if this option is enabled, DataFusion will not see any updates to the underlying
    /// location.  
    /// Default is disable.
    pub list_files_cache: Option<ListFilesCache>,
    /// Cache of file-embedded metadata, used to avoid reading it multiple times when processing a
    /// data file (e.g., Parquet footer and page metadata).
    /// If not provided, the [`CacheManager`] will create a [`DefaultFilesMetadataCache`].
    pub file_metadata_cache: Option<Arc<dyn FileMetadataCache>>,
    /// Limit of the file-embedded metadata cache, in bytes.
    pub metadata_cache_limit: usize,
}

impl Default for CacheManagerConfig {
    fn default() -> Self {
        Self {
            table_files_statistics_cache: Default::default(),
            list_files_cache: Default::default(),
            file_metadata_cache: Default::default(),
            metadata_cache_limit: DEFAULT_METADATA_CACHE_LIMIT,
        }
    }
}

impl CacheManagerConfig {
    /// Set the cache for files statistics.
    ///
    /// Default is `None` (disabled).
    pub fn with_files_statistics_cache(
        mut self,
        cache: Option<FileStatisticsCache>,
    ) -> Self {
        self.table_files_statistics_cache = cache;
        self
    }

    /// Set the cache for listing files.
    ///     
    /// Default is `None` (disabled).
    pub fn with_list_files_cache(mut self, cache: Option<ListFilesCache>) -> Self {
        self.list_files_cache = cache;
        self
    }

    /// Sets the cache for file-embedded metadata.
    ///
    /// Default is a [`DefaultFilesMetadataCache`].
    pub fn with_file_metadata_cache(
        mut self,
        cache: Option<Arc<dyn FileMetadataCache>>,
    ) -> Self {
        self.file_metadata_cache = cache;
        self
    }

    /// Sets the limit of the file-embedded metadata cache, in bytes.
    pub fn with_metadata_cache_limit(mut self, limit: usize) -> Self {
        self.metadata_cache_limit = limit;
        self
    }
}
