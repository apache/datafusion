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

//! Execution runtime environment that tracks memory, disk and various configurations
//! that are used during physical plan execution.

use crate::{
    error::Result,
    execution::{
        disk_manager::{DiskManager, DiskManagerConfig},
        memory_manager::{MemoryConsumerId, MemoryManager, MemoryManagerConfig},
    },
};

use std::fmt::{Debug, Formatter};
use std::sync::Arc;

#[derive(Clone)]
/// Execution runtime environment. This structure is passed to the
/// physical plans when they are run.
pub struct RuntimeEnv {
    /// Default batch size while creating new batches
    pub batch_size: usize,
    /// Runtime memory management
    pub memory_manager: Arc<MemoryManager>,
    /// Manage temporary files during query execution
    pub disk_manager: Arc<DiskManager>,
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
            batch_size,
            memory_manager,
            disk_manager,
        } = config;

        Ok(Self {
            batch_size,
            memory_manager: MemoryManager::new(memory_manager),
            disk_manager: DiskManager::try_new(disk_manager)?,
        })
    }

    /// Get execution batch size based on config
    pub fn batch_size(&self) -> usize {
        self.batch_size
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
}

impl Default for RuntimeEnv {
    fn default() -> Self {
        RuntimeEnv::new(RuntimeConfig::new()).unwrap()
    }
}

#[derive(Clone)]
/// Execution runtime configuration
pub struct RuntimeConfig {
    /// Default batch size while creating new batches, it's especially useful
    /// for buffer-in-memory batches since creating tiny batches would results
    /// in too much metadata memory consumption.
    pub batch_size: usize,
    /// DiskManager to manage temporary disk file usage
    pub disk_manager: DiskManagerConfig,
    /// MemoryManager to limit access to memory
    pub memory_manager: MemoryManagerConfig,
}

impl RuntimeConfig {
    /// New with default values
    pub fn new() -> Self {
        Default::default()
    }

    /// Customize batch size
    pub fn with_batch_size(mut self, n: usize) -> Self {
        // batch size must be greater than zero
        assert!(n > 0);
        self.batch_size = n;
        self
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
}

impl Default for RuntimeConfig {
    fn default() -> Self {
        Self {
            batch_size: 8192,
            disk_manager: DiskManagerConfig::default(),
            memory_manager: MemoryManagerConfig::default(),
        }
    }
}
