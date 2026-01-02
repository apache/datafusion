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

use crate::cache::cache_unit::DefaultFilesMetadataCache;
use crate::cache::{CacheAccessor, DefaultListFilesCache};
use datafusion_common::stats::Precision;
use datafusion_common::{Result, Statistics};
use object_store::ObjectMeta;
use object_store::path::Path;
use std::any::Any;
use std::collections::HashMap;
use std::fmt::{Debug, Formatter};
use std::sync::Arc;
use std::time::Duration;

pub use super::list_files_cache::{
    DEFAULT_LIST_FILES_CACHE_MEMORY_LIMIT, DEFAULT_LIST_FILES_CACHE_TTL,
};

/// A cache for [`Statistics`].
///
/// If enabled via [`CacheManagerConfig::with_files_statistics_cache`] this
/// cache avoids inferring the same file statistics repeatedly during the
/// session lifetime.
///
/// See [`crate::runtime_env::RuntimeEnv`] for more details
pub trait FileStatisticsCache:
    CacheAccessor<Path, Arc<Statistics>, Extra = ObjectMeta>
{
    /// Retrieves the information about the entries currently cached.
    fn list_entries(&self) -> HashMap<Path, FileStatisticsCacheEntry>;
}

/// Represents information about a cached statistics entry.
/// This is used to expose the statistics cache contents to outside modules.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FileStatisticsCacheEntry {
    pub object_meta: ObjectMeta,
    /// Number of table rows.
    pub num_rows: Precision<usize>,
    /// Number of table columns.
    pub num_columns: usize,
    /// Total table size, in bytes.
    pub table_size_bytes: Precision<usize>,
    /// Size of the statistics entry, in bytes.
    pub statistics_size_bytes: usize,
}

/// Cache for storing the [`ObjectMeta`]s that result from listing a path
///
/// Listing a path means doing an object store "list" operation or `ls`
/// command on the local filesystem. This operation can be expensive,
/// especially when done over remote object stores.
///
/// The cache key is always the table's base path, ensuring a stable cache key.
/// The `Extra` type is `Option<Path>`, representing an optional prefix filter
/// (relative to the table base path) for partition-aware lookups.
///
/// When `get_with_extra(key, Some(prefix))` is called:
/// - The cache entry for `key` (table base path) is fetched
/// - Results are filtered to only include files matching `key/prefix`
/// - Filtered results are returned without making a storage call
///
/// This enables efficient partition pruning: a single cached listing of the
/// full table can serve queries for any partition subset.
///
/// See [`crate::runtime_env::RuntimeEnv`] for more details.
pub trait ListFilesCache:
    CacheAccessor<Path, Arc<Vec<ObjectMeta>>, Extra = Option<Path>>
{
    /// Returns the cache's memory limit in bytes.
    fn cache_limit(&self) -> usize;

    /// Returns the TTL (time-to-live) for cache entries, if configured.
    fn cache_ttl(&self) -> Option<Duration>;

    /// Updates the cache with a new memory limit in bytes.
    fn update_cache_limit(&self, limit: usize);

    /// Updates the cache with a new TTL (time-to-live).
    fn update_cache_ttl(&self, ttl: Option<Duration>);
}

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

impl Debug for dyn FileStatisticsCache {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "Cache name: {} with length: {}", self.name(), self.len())
    }
}

impl Debug for dyn ListFilesCache {
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
    file_statistic_cache: Option<Arc<dyn FileStatisticsCache>>,
    list_files_cache: Option<Arc<dyn ListFilesCache>>,
    file_metadata_cache: Arc<dyn FileMetadataCache>,
}

impl CacheManager {
    pub fn try_new(config: &CacheManagerConfig) -> Result<Arc<Self>> {
        let file_statistic_cache =
            config.table_files_statistics_cache.as_ref().map(Arc::clone);

        let list_files_cache = match &config.list_files_cache {
            Some(lfc) if config.list_files_cache_limit > 0 => {
                // the cache memory limit or ttl might have changed, ensure they are updated
                lfc.update_cache_limit(config.list_files_cache_limit);
                // Only update TTL if explicitly set in config, otherwise preserve the cache's existing TTL
                if let Some(ttl) = config.list_files_cache_ttl {
                    lfc.update_cache_ttl(Some(ttl));
                }
                Some(Arc::clone(lfc))
            }
            None if config.list_files_cache_limit > 0 => {
                let lfc: Arc<dyn ListFilesCache> = Arc::new(DefaultListFilesCache::new(
                    config.list_files_cache_limit,
                    config.list_files_cache_ttl,
                ));
                Some(lfc)
            }
            _ => None,
        };

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
    pub fn get_file_statistic_cache(&self) -> Option<Arc<dyn FileStatisticsCache>> {
        self.file_statistic_cache.clone()
    }

    /// Get the cache for storing the result of listing [`ObjectMeta`]s under the same path.
    pub fn get_list_files_cache(&self) -> Option<Arc<dyn ListFilesCache>> {
        self.list_files_cache.clone()
    }

    /// Get the memory limit of the list files cache.
    pub fn get_list_files_cache_limit(&self) -> usize {
        self.list_files_cache
            .as_ref()
            .map_or(0, |c| c.cache_limit())
    }

    /// Get the TTL (time-to-live) of the list files cache.
    pub fn get_list_files_cache_ttl(&self) -> Option<Duration> {
        self.list_files_cache.as_ref().and_then(|c| c.cache_ttl())
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
    /// Enable caching of file statistics when listing files.
    /// Enabling the cache avoids repeatedly reading file statistics in a DataFusion session.
    /// Default is disabled. Currently only Parquet files are supported.
    pub table_files_statistics_cache: Option<Arc<dyn FileStatisticsCache>>,
    /// Enable caching of file metadata when listing files.
    /// Enabling the cache avoids repeat list and object metadata fetch operations, which may be
    /// expensive in certain situations (e.g. remote object storage), for objects under paths that
    /// are cached.
    /// Note that if this option is enabled, DataFusion will not see any updates to the underlying
    /// storage for at least `list_files_cache_ttl` duration.
    /// Default is disabled.
    pub list_files_cache: Option<Arc<dyn ListFilesCache>>,
    /// Limit of the `list_files_cache`, in bytes. Default: 1MiB.
    pub list_files_cache_limit: usize,
    /// The duration the list files cache will consider an entry valid after insertion. Note that
    /// changes to the underlying storage system, such as adding or removing data, will not be
    /// visible until an entry expires. Default: None (infinite).
    pub list_files_cache_ttl: Option<Duration>,
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
            list_files_cache_limit: DEFAULT_LIST_FILES_CACHE_MEMORY_LIMIT,
            list_files_cache_ttl: DEFAULT_LIST_FILES_CACHE_TTL,
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
        cache: Option<Arc<dyn FileStatisticsCache>>,
    ) -> Self {
        self.table_files_statistics_cache = cache;
        self
    }

    /// Set the cache for listing files.
    ///
    /// Default is `None` (disabled).
    pub fn with_list_files_cache(
        mut self,
        cache: Option<Arc<dyn ListFilesCache>>,
    ) -> Self {
        self.list_files_cache = cache;
        self
    }

    /// Sets the limit of the list files cache, in bytes.
    ///
    /// Default: 1MiB (1,048,576 bytes).
    pub fn with_list_files_cache_limit(mut self, limit: usize) -> Self {
        self.list_files_cache_limit = limit;
        self
    }

    /// Sets the TTL (time-to-live) for entries in the list files cache.
    ///
    /// Default: None (infinite).
    pub fn with_list_files_cache_ttl(mut self, ttl: Option<Duration>) -> Self {
        self.list_files_cache_ttl = ttl;
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cache::DefaultListFilesCache;

    /// Test to verify that TTL is preserved when not explicitly set in config.
    /// This fixes issue #19396 where TTL was being unset from DefaultListFilesCache
    /// when CacheManagerConfig::list_files_cache_ttl was not set explicitly.
    #[test]
    fn test_ttl_preserved_when_not_set_in_config() {
        use std::time::Duration;

        // Create a cache with TTL = 1 second
        let list_file_cache =
            DefaultListFilesCache::new(1024, Some(Duration::from_secs(1)));

        // Verify the cache has TTL set initially
        assert_eq!(
            list_file_cache.cache_ttl(),
            Some(Duration::from_secs(1)),
            "Cache should have TTL = 1 second initially"
        );

        // Put cache in config WITHOUT setting list_files_cache_ttl
        let config = CacheManagerConfig::default()
            .with_list_files_cache(Some(Arc::new(list_file_cache)));

        // Create CacheManager from config
        let cache_manager = CacheManager::try_new(&config).unwrap();

        // Verify TTL is preserved (not unset)
        let cache_ttl = cache_manager.get_list_files_cache().unwrap().cache_ttl();

        assert!(
            cache_ttl.is_some(),
            "TTL should be preserved when not set in config. Expected Some(Duration::from_secs(1)), got {cache_ttl:?}"
        );

        // Verify it's the correct TTL value
        assert_eq!(
            cache_ttl,
            Some(Duration::from_secs(1)),
            "TTL should be exactly 1 second"
        );
    }

    /// Test to verify that TTL can still be overridden when explicitly set in config.
    #[test]
    fn test_ttl_overridden_when_set_in_config() {
        use std::time::Duration;

        // Create a cache with TTL = 1 second
        let list_file_cache =
            DefaultListFilesCache::new(1024, Some(Duration::from_secs(1)));

        // Put cache in config WITH a different TTL set
        let config = CacheManagerConfig::default()
            .with_list_files_cache(Some(Arc::new(list_file_cache)))
            .with_list_files_cache_ttl(Some(Duration::from_secs(60)));

        // Create CacheManager from config
        let cache_manager = CacheManager::try_new(&config).unwrap();

        // Verify TTL is overridden to the config value
        let cache_ttl = cache_manager.get_list_files_cache().unwrap().cache_ttl();

        assert_eq!(
            cache_ttl,
            Some(Duration::from_secs(60)),
            "TTL should be overridden to 60 seconds when set in config"
        );
    }
}
