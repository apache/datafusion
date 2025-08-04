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
use crate::cache::CacheAccessor;
use datafusion_common::{Result, Statistics};
use object_store::path::Path;
use object_store::ObjectMeta;
use std::any::Any;
use std::fmt::{Debug, Formatter};
use std::sync::Arc;

/// The cache of listing files statistics.
/// if set [`CacheManagerConfig::with_files_statistics_cache`]
/// Will avoid infer same file statistics repeatedly during the session lifetime,
/// this cache will store in [`crate::runtime_env::RuntimeEnv`].
pub type FileStatisticsCache =
    Arc<dyn CacheAccessor<Path, Arc<Statistics>, Extra = ObjectMeta>>;

pub type ListFilesCache =
    Arc<dyn CacheAccessor<Path, Arc<Vec<ObjectMeta>>, Extra = ObjectMeta>>;

/// Represents generic file-embedded metadata.
pub trait FileMetadata: Any + Send + Sync {
    /// Returns the file metadata as [`Any`] so that it can be downcasted to a specific
    /// implementation.
    fn as_any(&self) -> &dyn Any;

    /// Returns the size of the metadata in bytes.
    fn memory_size(&self) -> usize;
}

/// Cache to store file-embedded metadata.
pub trait FileMetadataCache:
    CacheAccessor<ObjectMeta, Arc<dyn FileMetadata>, Extra = ObjectMeta>
{
    // Returns the cache's memory limit in bytes, or `None` for no limit.
    fn cache_limit(&self) -> Option<usize>;

    // Updates the cache with a new memory limit in bytes, or `None` for no limit.
    fn update_cache_limit(&self, limit: Option<usize>);
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
                Arc::new(DefaultFilesMetadataCache::new(
                    config.file_metadata_cache_limit,
                ))
            });

        // the cache memory limit might have changed, ensure the limit is updated
        file_metadata_cache.update_cache_limit(config.file_metadata_cache_limit);

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

    /// Get the cache of objectMeta under same path.
    pub fn get_list_files_cache(&self) -> Option<ListFilesCache> {
        self.list_files_cache.clone()
    }

    /// Get the file embedded metadata cache.
    pub fn get_file_metadata_cache(&self) -> Arc<dyn FileMetadataCache> {
        Arc::clone(&self.file_metadata_cache)
    }

    /// Get the limit of the file embedded metadata cache.
    pub fn get_file_metadata_cache_limit(&self) -> Option<usize> {
        self.file_metadata_cache.cache_limit()
    }
}

const DEFAULT_FILE_METADATA_CACHE_LIMIT: usize = 1024 * 1024 * 1024; // 1G

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
    pub file_metadata_cache_limit: Option<usize>,
}

impl Default for CacheManagerConfig {
    fn default() -> Self {
        Self {
            table_files_statistics_cache: Default::default(),
            list_files_cache: Default::default(),
            file_metadata_cache: Default::default(),
            file_metadata_cache_limit: Some(DEFAULT_FILE_METADATA_CACHE_LIMIT),
        }
    }
}

impl CacheManagerConfig {
    pub fn with_files_statistics_cache(
        mut self,
        cache: Option<FileStatisticsCache>,
    ) -> Self {
        self.table_files_statistics_cache = cache;
        self
    }

    pub fn with_list_files_cache(mut self, cache: Option<ListFilesCache>) -> Self {
        self.list_files_cache = cache;
        self
    }

    pub fn with_file_metadata_cache(
        mut self,
        cache: Option<Arc<dyn FileMetadataCache>>,
    ) -> Self {
        self.file_metadata_cache = cache;
        self
    }

    pub fn with_file_metadata_cache_limit(mut self, limit: Option<usize>) -> Self {
        self.file_metadata_cache_limit = limit;
        self
    }
}
