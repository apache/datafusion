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

use crate::{
    disk_manager::{DiskManager, DiskManagerConfig},
    memory_pool::{
        GreedyMemoryPool, MemoryPool, TrackConsumersPool, UnboundedMemoryPool,
    },
    object_store::{DefaultObjectStoreRegistry, ObjectStoreRegistry},
};

use crate::cache::cache_manager::{CacheManager, CacheManagerConfig};
use datafusion_common::{DataFusionError, Result};
use object_store::ObjectStore;
use std::path::PathBuf;
use std::sync::Arc;
use std::{
    fmt::{Debug, Formatter},
    num::NonZeroUsize,
};
use url::Url;

#[derive(Clone)]
/// Execution runtime environment that manages system resources such
/// as memory, disk, cache and storage.
///
/// A [`RuntimeEnv`] is created from a [`RuntimeEnvBuilder`] and has the
/// following resource management functionality:
///
/// * [`MemoryPool`]: Manage memory
/// * [`DiskManager`]: Manage temporary files on local disk
/// * [`CacheManager`]: Manage temporary cache data during the session lifetime
/// * [`ObjectStoreRegistry`]: Manage mapping URLs to object store instances
pub struct RuntimeEnv {
    /// Runtime memory management
    pub memory_pool: Arc<dyn MemoryPool>,
    /// Manage temporary files during query execution
    pub disk_manager: Arc<DiskManager>,
    /// Manage temporary cache during query execution
    pub cache_manager: Arc<CacheManager>,
    /// Object Store Registry
    pub object_store_registry: Arc<dyn ObjectStoreRegistry>,
}

impl Debug for RuntimeEnv {
    fn fmt(&self, f: &mut Formatter) -> std::fmt::Result {
        write!(f, "RuntimeEnv")
    }
}

impl RuntimeEnv {
    #[deprecated(note = "please use `try_new` instead")]
    pub fn new(config: RuntimeConfig) -> Result<Self> {
        Self::try_new(config)
    }
    /// Create env based on configuration
    pub fn try_new(config: RuntimeConfig) -> Result<Self> {
        let RuntimeConfig {
            memory_pool,
            disk_manager,
            cache_manager,
            object_store_registry,
        } = config;

        let memory_pool =
            memory_pool.unwrap_or_else(|| Arc::new(UnboundedMemoryPool::default()));

        Ok(Self {
            memory_pool,
            disk_manager: DiskManager::try_new(disk_manager)?,
            cache_manager: CacheManager::try_new(&cache_manager)?,
            object_store_registry,
        })
    }

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
    /// # let runtime_env = RuntimeEnv::try_new(Default::default()).unwrap();
    /// let url = Url::try_from("file://").unwrap();
    /// let object_store = object_store::local::LocalFileSystem::new();
    /// // register the object store with the runtime environment
    /// runtime_env.register_object_store(&url, Arc::new(object_store));
    /// ```
    ///
    /// # Example: Register local file system object store
    ///
    /// To register reading from urls such as <https://github.com>`
    ///
    /// ```
    /// # use std::sync::Arc;
    /// # use url::Url;
    /// # use datafusion_execution::runtime_env::RuntimeEnv;
    /// # let runtime_env = RuntimeEnv::try_new(Default::default()).unwrap();
    /// # // use local store for example as http feature is not enabled
    /// # let http_store = object_store::local::LocalFileSystem::new();
    /// // create a new object store via object_store::http::HttpBuilder;
    /// let base_url = Url::parse("https://github.com").unwrap();
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

    /// Retrieves a `ObjectStore` instance for a url by consulting the
    /// registry. See [`ObjectStoreRegistry::get_store`] for more
    /// details.
    pub fn object_store(&self, url: impl AsRef<Url>) -> Result<Arc<dyn ObjectStore>> {
        self.object_store_registry
            .get_store(url.as_ref())
            .map_err(DataFusionError::from)
    }
}

impl Default for RuntimeEnv {
    fn default() -> Self {
        RuntimeEnvBuilder::new().build().unwrap()
    }
}

/// Please see: <https://github.com/apache/datafusion/issues/12156>
/// This a type alias for backwards compatibility.
pub type RuntimeConfig = RuntimeEnvBuilder;

#[derive(Clone)]
/// Execution runtime configuration
pub struct RuntimeEnvBuilder {
    /// DiskManager to manage temporary disk file usage
    pub disk_manager: DiskManagerConfig,
    /// [`MemoryPool`] from which to allocate memory
    ///
    /// Defaults to using an [`UnboundedMemoryPool`] if `None`
    pub memory_pool: Option<Arc<dyn MemoryPool>>,
    /// CacheManager to manage cache data
    pub cache_manager: CacheManagerConfig,
    /// ObjectStoreRegistry to get object store based on url
    pub object_store_registry: Arc<dyn ObjectStoreRegistry>,
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
            memory_pool: Default::default(),
            cache_manager: Default::default(),
            object_store_registry: Arc::new(DefaultObjectStoreRegistry::default()),
        }
    }

    /// Customize disk manager
    pub fn with_disk_manager(mut self, disk_manager: DiskManagerConfig) -> Self {
        self.disk_manager = disk_manager;
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
    /// This defaults to using [`GreedyMemoryPool`]
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
    pub fn with_temp_file_path(self, path: impl Into<PathBuf>) -> Self {
        self.with_disk_manager(DiskManagerConfig::new_specified(vec![path.into()]))
    }

    /// Build a RuntimeEnv
    pub fn build(self) -> Result<RuntimeEnv> {
        let memory_pool = self
            .memory_pool
            .unwrap_or_else(|| Arc::new(UnboundedMemoryPool::default()));

        Ok(RuntimeEnv {
            memory_pool,
            disk_manager: DiskManager::try_new(self.disk_manager)?,
            cache_manager: CacheManager::try_new(&self.cache_manager)?,
            object_store_registry: self.object_store_registry,
        })
    }

    /// Convenience method to create a new `Arc<RuntimeEnv>`
    pub fn build_arc(self) -> Result<Arc<RuntimeEnv>> {
        self.build().map(Arc::new)
    }
}
