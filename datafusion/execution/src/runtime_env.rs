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

//! Execution [`RuntimeEnv`] environment that manages access to object
//! store, memory manager, disk manager.

#[expect(deprecated)]
use crate::disk_manager::{DiskManagerConfig, SpillingProgress};
use crate::{
    disk_manager::{DiskManager, DiskManagerBuilder, DiskManagerMode},
    memory_pool::{
        GreedyMemoryPool, MemoryPool, TrackConsumersPool, UnboundedMemoryPool,
    },
    object_store::{DefaultObjectStoreRegistry, ObjectStoreRegistry},
};

use crate::cache::cache_manager::{CacheManager, CacheManagerConfig};
#[cfg(feature = "parquet_encryption")]
use crate::parquet_encryption::{EncryptionFactory, EncryptionFactoryRegistry};
use datafusion_common::{Result, config::ConfigEntry};
use object_store::ObjectStore;
use std::sync::Arc;
use std::{
    fmt::{Debug, Formatter},
    num::NonZeroUsize,
};
use std::{path::PathBuf, time::Duration};
use url::Url;

#[derive(Clone)]
/// Execution runtime environment that manages system resources such
/// as memory, disk, cache and storage.
///
/// A [`RuntimeEnv`] can be created using [`RuntimeEnvBuilder`] and has the
/// following resource management functionality:
///
/// * [`MemoryPool`]: Manage memory
/// * [`DiskManager`]: Manage temporary files on local disk
/// * [`CacheManager`]: Manage temporary cache data during the session lifetime
/// * [`ObjectStoreRegistry`]: Manage mapping URLs to object store instances
///
/// # Example: Create default `RuntimeEnv`
/// ```
/// # use datafusion_execution::runtime_env::RuntimeEnv;
/// let runtime_env = RuntimeEnv::default();
/// ```
///
/// # Example: Create a `RuntimeEnv` from [`RuntimeEnvBuilder`] with a new memory pool
/// ```
/// # use std::sync::Arc;
/// # use datafusion_execution::memory_pool::GreedyMemoryPool;
/// # use datafusion_execution::runtime_env::{RuntimeEnv, RuntimeEnvBuilder};
/// // restrict to using at most 100MB of memory
/// let pool_size = 100 * 1024 * 1024;
/// let runtime_env = RuntimeEnvBuilder::new()
///     .with_memory_pool(Arc::new(GreedyMemoryPool::new(pool_size)))
///     .build()
///     .unwrap();
/// ```
pub struct RuntimeEnv {
    /// Runtime memory management
    pub memory_pool: Arc<dyn MemoryPool>,
    /// Manage temporary files during query execution
    pub disk_manager: Arc<DiskManager>,
    /// Manage temporary cache during query execution
    pub cache_manager: Arc<CacheManager>,
    /// Object Store Registry
    pub object_store_registry: Arc<dyn ObjectStoreRegistry>,
    /// Parquet encryption factory registry
    #[cfg(feature = "parquet_encryption")]
    pub parquet_encryption_factory_registry: Arc<EncryptionFactoryRegistry>,
}

impl Debug for RuntimeEnv {
    fn fmt(&self, f: &mut Formatter) -> std::fmt::Result {
        write!(f, "RuntimeEnv")
    }
}

/// Creates runtime configuration entries with the provided values
///
/// This helper function defines the structure and metadata for all runtime configuration
/// entries to avoid duplication between `RuntimeEnv::config_entries()` and
/// `RuntimeEnvBuilder::entries()`.
fn create_runtime_config_entries(
    memory_limit: Option<String>,
    max_temp_directory_size: Option<String>,
    temp_directory: Option<String>,
    metadata_cache_limit: Option<String>,
    list_files_cache_limit: Option<String>,
    list_files_cache_ttl: Option<String>,
) -> Vec<ConfigEntry> {
    vec![
        ConfigEntry {
            key: "datafusion.runtime.memory_limit".to_string(),
            value: memory_limit,
            description: "Maximum memory limit for query execution. Supports suffixes K (kilobytes), M (megabytes), and G (gigabytes). Example: '2G' for 2 gigabytes.",
        },
        ConfigEntry {
            key: "datafusion.runtime.max_temp_directory_size".to_string(),
            value: max_temp_directory_size,
            description: "Maximum temporary file directory size. Supports suffixes K (kilobytes), M (megabytes), and G (gigabytes). Example: '2G' for 2 gigabytes.",
        },
        ConfigEntry {
            key: "datafusion.runtime.temp_directory".to_string(),
            value: temp_directory,
            description: "The path to the temporary file directory.",
        },
        ConfigEntry {
            key: "datafusion.runtime.metadata_cache_limit".to_string(),
            value: metadata_cache_limit,
            description: "Maximum memory to use for file metadata cache such as Parquet metadata. Supports suffixes K (kilobytes), M (megabytes), and G (gigabytes). Example: '2G' for 2 gigabytes.",
        },
        ConfigEntry {
            key: "datafusion.runtime.list_files_cache_limit".to_string(),
            value: list_files_cache_limit,
            description: "Maximum memory to use for list files cache. Supports suffixes K (kilobytes), M (megabytes), and G (gigabytes). Example: '2G' for 2 gigabytes.",
        },
        ConfigEntry {
            key: "datafusion.runtime.list_files_cache_ttl".to_string(),
            value: list_files_cache_ttl,
            description: "TTL (time-to-live) of the entries in the list file cache. Supports units m (minutes), and s (seconds). Example: '2m' for 2 minutes.",
        },
    ]
}

impl RuntimeEnv {
    /// Registers a custom `ObjectStore` to be used with a specific url.
    /// This allows DataFusion to create external tables from urls that do not have
    /// built in support such as `hdfs://namenode:port/...`.
    ///
    /// Returns the [`ObjectStore`] previously registered for this
    /// scheme, if any.
    ///
    /// See [`ObjectStoreRegistry`] for more details
    ///
    /// # Example: Register local file system object store
    /// ```
    /// # use std::sync::Arc;
    /// # use url::Url;
    /// # use datafusion_execution::runtime_env::RuntimeEnv;
    /// # let runtime_env = RuntimeEnv::default();
    /// let url = Url::try_from("file://").unwrap();
    /// let object_store = object_store::local::LocalFileSystem::new();
    /// // register the object store with the runtime environment
    /// runtime_env.register_object_store(&url, Arc::new(object_store));
    /// ```
    ///
    /// # Example: Register remote URL object store like [Github](https://github.com)
    /// ```
    /// # use std::sync::Arc;
    /// # use url::Url;
    /// # use datafusion_execution::runtime_env::RuntimeEnv;
    /// # let runtime_env = RuntimeEnv::default();
    /// # // use local store for example as http feature is not enabled
    /// # let http_store = object_store::local::LocalFileSystem::new();
    /// // create a new object store via object_store::http::HttpBuilder;
    /// let base_url = Url::parse("https://github.com").unwrap();
    /// // (note this example can't depend on the http feature)
    /// // let http_store = HttpBuilder::new()
    /// //    .with_url(base_url.clone())
    /// //    .build()
    /// //    .unwrap();
    /// // register the object store with the runtime environment
    /// runtime_env.register_object_store(&base_url, Arc::new(http_store));
    /// ```
    pub fn register_object_store(
        &self,
        url: &Url,
        object_store: Arc<dyn ObjectStore>,
    ) -> Option<Arc<dyn ObjectStore>> {
        self.object_store_registry.register_store(url, object_store)
    }

    /// Deregisters a custom `ObjectStore` previously registered for a specific url.
    /// See [`ObjectStoreRegistry::deregister_store`] for more details.
    pub fn deregister_object_store(&self, url: &Url) -> Result<Arc<dyn ObjectStore>> {
        self.object_store_registry.deregister_store(url)
    }

    /// Retrieves a `ObjectStore` instance for a url by consulting the
    /// registry. See [`ObjectStoreRegistry::get_store`] for more
    /// details.
    pub fn object_store(&self, url: impl AsRef<Url>) -> Result<Arc<dyn ObjectStore>> {
        self.object_store_registry.get_store(url.as_ref())
    }

    /// Returns the current spilling progress
    pub fn spilling_progress(&self) -> SpillingProgress {
        self.disk_manager.spilling_progress()
    }

    /// Register an [`EncryptionFactory`] with an associated identifier that can be later
    /// used to configure encryption when reading or writing Parquet.
    /// If an encryption factory with the same identifier was already registered, it is replaced and returned.
    #[cfg(feature = "parquet_encryption")]
    pub fn register_parquet_encryption_factory(
        &self,
        id: &str,
        encryption_factory: Arc<dyn EncryptionFactory>,
    ) -> Option<Arc<dyn EncryptionFactory>> {
        self.parquet_encryption_factory_registry
            .register_factory(id, encryption_factory)
    }

    /// Retrieve an [`EncryptionFactory`] by its identifier
    #[cfg(feature = "parquet_encryption")]
    pub fn parquet_encryption_factory(
        &self,
        id: &str,
    ) -> Result<Arc<dyn EncryptionFactory>> {
        self.parquet_encryption_factory_registry.get_factory(id)
    }

    /// Returns the current runtime configuration entries
    pub fn config_entries(&self) -> Vec<ConfigEntry> {
        use crate::memory_pool::MemoryLimit;

        /// Convert bytes to a human-readable format
        fn format_byte_size(size: u64) -> String {
            const GB: u64 = 1024 * 1024 * 1024;
            const MB: u64 = 1024 * 1024;
            const KB: u64 = 1024;

            match size {
                s if s >= GB => format!("{}G", s / GB),
                s if s >= MB => format!("{}M", s / MB),
                s if s >= KB => format!("{}K", s / KB),
                s => format!("{s}"),
            }
        }

        fn format_duration(duration: Duration) -> String {
            let total = duration.as_secs();
            let mins = total / 60;
            let secs = total % 60;

            format!("{mins}m{secs}s")
        }

        let memory_limit_value = match self.memory_pool.memory_limit() {
            MemoryLimit::Finite(size) => Some(format_byte_size(
                size.try_into()
                    .expect("Memory limit size conversion failed"),
            )),
            MemoryLimit::Infinite => Some("unlimited".to_string()),
            MemoryLimit::Unknown => None,
        };

        let max_temp_dir_size = self.disk_manager.max_temp_directory_size();
        let max_temp_dir_value = format_byte_size(max_temp_dir_size);

        let temp_paths = self.disk_manager.temp_dir_paths();
        let temp_dir_value = if temp_paths.is_empty() {
            None
        } else {
            Some(
                temp_paths
                    .iter()
                    .map(|p| p.display().to_string())
                    .collect::<Vec<_>>()
                    .join(","),
            )
        };

        let metadata_cache_limit = self.cache_manager.get_metadata_cache_limit();
        let metadata_cache_value = format_byte_size(
            metadata_cache_limit
                .try_into()
                .expect("Metadata cache size conversion failed"),
        );

        let list_files_cache_limit = self.cache_manager.get_list_files_cache_limit();
        let list_files_cache_value = format_byte_size(
            list_files_cache_limit
                .try_into()
                .expect("List files cache size conversion failed"),
        );

        let list_files_cache_ttl = self
            .cache_manager
            .get_list_files_cache_ttl()
            .map(format_duration);

        create_runtime_config_entries(
            memory_limit_value,
            Some(max_temp_dir_value),
            temp_dir_value,
            Some(metadata_cache_value),
            Some(list_files_cache_value),
            list_files_cache_ttl,
        )
    }
}

impl Default for RuntimeEnv {
    fn default() -> Self {
        RuntimeEnvBuilder::new().build().unwrap()
    }
}

/// Execution runtime configuration builder.
///
/// See example on [`RuntimeEnv`]
#[derive(Clone)]
pub struct RuntimeEnvBuilder {
    #[expect(deprecated)]
    /// DiskManager to manage temporary disk file usage
    pub disk_manager: DiskManagerConfig,
    /// DiskManager builder to manager temporary disk file usage
    pub disk_manager_builder: Option<DiskManagerBuilder>,
    /// [`MemoryPool`] from which to allocate memory
    ///
    /// Defaults to using an [`UnboundedMemoryPool`] if `None`
    pub memory_pool: Option<Arc<dyn MemoryPool>>,
    /// CacheManager to manage cache data
    pub cache_manager: CacheManagerConfig,
    /// ObjectStoreRegistry to get object store based on url
    pub object_store_registry: Arc<dyn ObjectStoreRegistry>,
    /// Parquet encryption factory registry
    #[cfg(feature = "parquet_encryption")]
    pub parquet_encryption_factory_registry: Arc<EncryptionFactoryRegistry>,
}

impl Default for RuntimeEnvBuilder {
    fn default() -> Self {
        Self::new()
    }
}

impl RuntimeEnvBuilder {
    /// New with default values
    pub fn new() -> Self {
        Self {
            disk_manager: Default::default(),
            disk_manager_builder: Default::default(),
            memory_pool: Default::default(),
            cache_manager: Default::default(),
            object_store_registry: Arc::new(DefaultObjectStoreRegistry::default()),
            #[cfg(feature = "parquet_encryption")]
            parquet_encryption_factory_registry: Default::default(),
        }
    }

    #[expect(deprecated)]
    #[deprecated(since = "48.0.0", note = "Use with_disk_manager_builder instead")]
    /// Customize disk manager
    pub fn with_disk_manager(mut self, disk_manager: DiskManagerConfig) -> Self {
        self.disk_manager = disk_manager;
        self
    }

    /// Customize the disk manager builder
    pub fn with_disk_manager_builder(mut self, disk_manager: DiskManagerBuilder) -> Self {
        self.disk_manager_builder = Some(disk_manager);
        self
    }

    /// Customize memory policy
    pub fn with_memory_pool(mut self, memory_pool: Arc<dyn MemoryPool>) -> Self {
        self.memory_pool = Some(memory_pool);
        self
    }

    /// Customize cache policy
    pub fn with_cache_manager(mut self, cache_manager: CacheManagerConfig) -> Self {
        self.cache_manager = cache_manager;
        self
    }

    /// Customize object store registry
    pub fn with_object_store_registry(
        mut self,
        object_store_registry: Arc<dyn ObjectStoreRegistry>,
    ) -> Self {
        self.object_store_registry = object_store_registry;
        self
    }

    /// Specify the total memory to use while running the DataFusion
    /// plan to `max_memory * memory_fraction` in bytes.
    ///
    /// This defaults to using [`GreedyMemoryPool`] wrapped in the
    /// [`TrackConsumersPool`] with a maximum of 5 consumers.
    ///
    /// Note DataFusion does not yet respect this limit in all cases.
    pub fn with_memory_limit(self, max_memory: usize, memory_fraction: f64) -> Self {
        let pool_size = (max_memory as f64 * memory_fraction) as usize;
        self.with_memory_pool(Arc::new(TrackConsumersPool::new(
            GreedyMemoryPool::new(pool_size),
            NonZeroUsize::new(5).unwrap(),
        )))
    }

    /// Use the specified path to create any needed temporary files
    pub fn with_temp_file_path(mut self, path: impl Into<PathBuf>) -> Self {
        let builder = self.disk_manager_builder.take().unwrap_or_default();
        self.with_disk_manager_builder(
            builder.with_mode(DiskManagerMode::Directories(vec![path.into()])),
        )
    }

    /// Specify a limit on the size of the temporary file directory in bytes
    pub fn with_max_temp_directory_size(mut self, size: u64) -> Self {
        let builder = self.disk_manager_builder.take().unwrap_or_default();
        self.with_disk_manager_builder(builder.with_max_temp_directory_size(size))
    }

    /// Specify the limit of the file-embedded metadata cache, in bytes.
    pub fn with_metadata_cache_limit(mut self, limit: usize) -> Self {
        self.cache_manager = self.cache_manager.with_metadata_cache_limit(limit);
        self
    }

    /// Specifies the memory limit for the object list cache, in bytes.
    pub fn with_object_list_cache_limit(mut self, limit: usize) -> Self {
        self.cache_manager = self.cache_manager.with_list_files_cache_limit(limit);
        self
    }

    /// Specifies the duration entries in the object list cache will be considered valid.
    pub fn with_object_list_cache_ttl(mut self, ttl: Option<Duration>) -> Self {
        self.cache_manager = self.cache_manager.with_list_files_cache_ttl(ttl);
        self
    }

    /// Build a RuntimeEnv
    pub fn build(self) -> Result<RuntimeEnv> {
        let Self {
            disk_manager,
            disk_manager_builder,
            memory_pool,
            cache_manager,
            object_store_registry,
            #[cfg(feature = "parquet_encryption")]
            parquet_encryption_factory_registry,
        } = self;
        let memory_pool =
            memory_pool.unwrap_or_else(|| Arc::new(UnboundedMemoryPool::default()));

        Ok(RuntimeEnv {
            memory_pool,
            disk_manager: if let Some(builder) = disk_manager_builder {
                Arc::new(builder.build()?)
            } else {
                #[expect(deprecated)]
                DiskManager::try_new(disk_manager)?
            },
            cache_manager: CacheManager::try_new(&cache_manager)?,
            object_store_registry,
            #[cfg(feature = "parquet_encryption")]
            parquet_encryption_factory_registry,
        })
    }

    /// Convenience method to create a new `Arc<RuntimeEnv>`
    pub fn build_arc(self) -> Result<Arc<RuntimeEnv>> {
        self.build().map(Arc::new)
    }

    /// Create a new RuntimeEnvBuilder from an existing RuntimeEnv
    pub fn from_runtime_env(runtime_env: &RuntimeEnv) -> Self {
        let cache_config = CacheManagerConfig {
            table_files_statistics_cache: runtime_env
                .cache_manager
                .get_file_statistic_cache(),
            list_files_cache: runtime_env.cache_manager.get_list_files_cache(),
            list_files_cache_limit: runtime_env
                .cache_manager
                .get_list_files_cache_limit(),
            list_files_cache_ttl: runtime_env.cache_manager.get_list_files_cache_ttl(),
            file_metadata_cache: Some(
                runtime_env.cache_manager.get_file_metadata_cache(),
            ),
            metadata_cache_limit: runtime_env.cache_manager.get_metadata_cache_limit(),
        };

        Self {
            #[expect(deprecated)]
            disk_manager: DiskManagerConfig::Existing(Arc::clone(
                &runtime_env.disk_manager,
            )),
            disk_manager_builder: None,
            memory_pool: Some(Arc::clone(&runtime_env.memory_pool)),
            cache_manager: cache_config,
            object_store_registry: Arc::clone(&runtime_env.object_store_registry),
            #[cfg(feature = "parquet_encryption")]
            parquet_encryption_factory_registry: Arc::clone(
                &runtime_env.parquet_encryption_factory_registry,
            ),
        }
    }

    /// Returns a list of all available runtime configurations with their current values and descriptions
    pub fn entries(&self) -> Vec<ConfigEntry> {
        create_runtime_config_entries(
            None,
            Some("100G".to_string()),
            None,
            Some("50M".to_owned()),
            Some("1M".to_owned()),
            None,
        )
    }

    /// Generate documentation that can be included in the user guide
    pub fn generate_config_markdown() -> String {
        use std::fmt::Write as _;

        let s = Self::default();

        let mut docs = "| key | default | description |\n".to_string();
        docs += "|-----|---------|-------------|\n";
        let mut entries = s.entries();
        entries.sort_unstable_by(|a, b| a.key.cmp(&b.key));

        for entry in &entries {
            let _ = writeln!(
                &mut docs,
                "| {} | {} | {} |",
                entry.key,
                entry.value.as_deref().unwrap_or("NULL"),
                entry.description
            );
        }
        docs
    }
}
