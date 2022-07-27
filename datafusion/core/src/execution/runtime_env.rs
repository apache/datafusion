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
    execution::{
        disk_manager::{DiskManager, DiskManagerConfig},
        memory_manager::{MemoryConsumerId, MemoryManager, MemoryManagerConfig},
    },
};

use crate::datasource::object_store::ObjectStoreRegistry;
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
    pub memory_manager: Arc<MemoryManager>,
    /// Manage temporary files during query execution
    pub disk_manager: Arc<DiskManager>,
    /// Object Store Registry
    pub object_store_registry: Arc<ObjectStoreRegistry>,
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
            memory_manager,
            disk_manager,
            object_store_registry,
        } = config;

        Ok(Self {
            memory_manager: MemoryManager::new(memory_manager),
            disk_manager: DiskManager::try_new(disk_manager)?,
            object_store_registry,
        })
    }

    /// Register the consumer to get it tracked
    pub fn register_requester(&self, id: &MemoryConsumerId) {
        self.memory_manager.register_requester(id);
    }

    /// Drop the consumer from get tracked, reclaim memory
    pub fn drop_consumer(&self, id: &MemoryConsumerId, mem_used: usize) {
        self.memory_manager.drop_consumer(id, mem_used)
    }

    /// Grow tracker memory of `delta`
    pub fn grow_tracker_usage(&self, delta: usize) {
        self.memory_manager.grow_tracker_usage(delta)
    }

    /// Shrink tracker memory of `delta`
    pub fn shrink_tracker_usage(&self, delta: usize) {
        self.memory_manager.shrink_tracker_usage(delta)
    }

    /// Registers a object store with scheme using a custom `ObjectStore` so that
    /// an external file system or object storage system could be used against this context.
    ///
    /// Returns the `ObjectStore` previously registered for this scheme, if any
    pub fn register_object_store(
        &self,
        scheme: impl AsRef<str>,
        host: impl AsRef<str>,
        object_store: Arc<dyn ObjectStore>,
    ) -> Option<Arc<dyn ObjectStore>> {
        self.object_store_registry
            .register_store(scheme, host, object_store)
    }

    /// Retrieves a `ObjectStore` instance for a url
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
    /// MemoryManager to limit access to memory
    pub memory_manager: MemoryManagerConfig,
    /// ObjectStoreRegistry to get object store based on url
    pub object_store_registry: Arc<ObjectStoreRegistry>,
}

impl RuntimeConfig {
    /// New with default values
    pub fn new() -> Self {
        Default::default()
    }

    /// Customize disk manager
    pub fn with_disk_manager(mut self, disk_manager: DiskManagerConfig) -> Self {
        self.disk_manager = disk_manager;
        self
    }

    /// Customize memory manager
    pub fn with_memory_manager(mut self, memory_manager: MemoryManagerConfig) -> Self {
        self.memory_manager = memory_manager;
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

    /// Specify the total memory to use while running the DataFusion
    /// plan to `max_memory * memory_fraction` in bytes.
    ///
    /// Note DataFusion does not yet respect this limit in all cases.
    pub fn with_memory_limit(self, max_memory: usize, memory_fraction: f64) -> Self {
        self.with_memory_manager(
            MemoryManagerConfig::try_new_limit(max_memory, memory_fraction).unwrap(),
        )
    }

    /// Use the specified path to create any needed temporary files
    pub fn with_temp_file_path(self, path: impl Into<PathBuf>) -> Self {
        self.with_disk_manager(DiskManagerConfig::new_specified(vec![path.into()]))
    }
}
