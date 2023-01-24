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

//! Execution runtime environment that holds object Store, memory manager, disk manager
//! and various system level components that are used during physical plan execution.

use crate::{
    error::Result,
    execution::disk_manager::{DiskManager, DiskManagerConfig},
};
use std::collections::HashMap;

use crate::datasource::datasource::TableProviderFactory;
use crate::datasource::listing_table_factory::ListingTableFactory;
use crate::datasource::object_store::ObjectStoreRegistry;
use crate::execution::memory_pool::{GreedyMemoryPool, MemoryPool, UnboundedMemoryPool};
use datafusion_common::DataFusionError;
use object_store::ObjectStore;
use std::fmt::{Debug, Formatter};
use std::path::PathBuf;
use std::sync::Arc;
use url::Url;

#[derive(Clone)]
/// Execution runtime environment.
pub struct RuntimeEnv {
    /// Runtime memory management
    pub memory_pool: Arc<dyn MemoryPool>,
    /// Manage temporary files during query execution
    pub disk_manager: Arc<DiskManager>,
    /// Object Store Registry
    pub object_store_registry: Arc<ObjectStoreRegistry>,
    /// TableProviderFactories
    pub table_factories: HashMap<String, Arc<dyn TableProviderFactory>>,
}

impl Debug for RuntimeEnv {
    fn fmt(&self, f: &mut Formatter) -> std::fmt::Result {
        write!(f, "RuntimeEnv")
    }
}

impl RuntimeEnv {
    /// Create env based on configuration
    pub fn new(config: RuntimeConfig) -> Result<Self> {
        let RuntimeConfig {
            memory_pool,
            disk_manager,
            object_store_registry,
            table_factories,
        } = config;

        let memory_pool =
            memory_pool.unwrap_or_else(|| Arc::new(UnboundedMemoryPool::default()));

        Ok(Self {
            memory_pool,
            disk_manager: DiskManager::try_new(disk_manager)?,
            object_store_registry,
            table_factories,
        })
    }

    /// Registers a custom `ObjectStore` to be used when accessing a
    /// specific scheme and host. This allows DataFusion to create
    /// external tables from urls that do not have built in support
    /// such as `hdfs://...`.
    ///
    /// Returns the [`ObjectStore`] previously registered for this
    /// scheme, if any.
    ///
    /// See [`ObjectStoreRegistry`] for more details
    pub fn register_object_store(
        &self,
        scheme: impl AsRef<str>,
        host: impl AsRef<str>,
        object_store: Arc<dyn ObjectStore>,
    ) -> Option<Arc<dyn ObjectStore>> {
        self.object_store_registry
            .register_store(scheme, host, object_store)
    }

    /// Registers TableFactories
    pub fn register_table_factories(
        &mut self,
        table_factories: HashMap<String, Arc<dyn TableProviderFactory>>,
    ) {
        self.table_factories.extend(table_factories)
    }

    /// Retrieves a `ObjectStore` instance for a url by consulting the
    /// registery. See [`ObjectStoreRegistry::get_by_url`] for more
    /// details.
    pub fn object_store(&self, url: impl AsRef<Url>) -> Result<Arc<dyn ObjectStore>> {
        self.object_store_registry
            .get_by_url(url)
            .map_err(DataFusionError::from)
    }
}

impl Default for RuntimeEnv {
    fn default() -> Self {
        RuntimeEnv::new(RuntimeConfig::new()).unwrap()
    }
}

#[derive(Clone, Default)]
/// Execution runtime configuration
pub struct RuntimeConfig {
    /// DiskManager to manage temporary disk file usage
    pub disk_manager: DiskManagerConfig,
    /// [`MemoryPool`] from which to allocate memory
    ///
    /// Defaults to using an [`UnboundedMemoryPool`] if `None`
    pub memory_pool: Option<Arc<dyn MemoryPool>>,
    /// ObjectStoreRegistry to get object store based on url
    pub object_store_registry: Arc<ObjectStoreRegistry>,
    /// Custom table factories for things like deltalake that are not part of core datafusion
    pub table_factories: HashMap<String, Arc<dyn TableProviderFactory>>,
}

impl RuntimeConfig {
    /// New with default values
    pub fn new() -> Self {
        let mut table_factories: HashMap<String, Arc<dyn TableProviderFactory>> =
            HashMap::new();
        table_factories.insert("PARQUET".into(), Arc::new(ListingTableFactory::new()));
        table_factories.insert("CSV".into(), Arc::new(ListingTableFactory::new()));
        table_factories.insert("JSON".into(), Arc::new(ListingTableFactory::new()));
        table_factories.insert("NDJSON".into(), Arc::new(ListingTableFactory::new()));
        table_factories.insert("AVRO".into(), Arc::new(ListingTableFactory::new()));
        Self {
            table_factories,
            ..Default::default()
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

    /// Customize object store registry
    pub fn with_object_store_registry(
        mut self,
        object_store_registry: Arc<ObjectStoreRegistry>,
    ) -> Self {
        self.object_store_registry = object_store_registry;
        self
    }

    /// Customize object store registry
    pub fn with_table_factories(
        mut self,
        table_factories: HashMap<String, Arc<dyn TableProviderFactory>>,
    ) -> Self {
        self.table_factories = table_factories;
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
        self.with_memory_pool(Arc::new(GreedyMemoryPool::new(pool_size)))
    }

    /// Use the specified path to create any needed temporary files
    pub fn with_temp_file_path(self, path: impl Into<PathBuf>) -> Self {
        self.with_disk_manager(DiskManagerConfig::new_specified(vec![path.into()]))
    }
}
