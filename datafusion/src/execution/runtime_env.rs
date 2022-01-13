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

use crate::error::Result;
use crate::execution::disk_manager::DiskManager;
use crate::execution::memory_manager::{MemoryConsumer, MemoryConsumerId, MemoryManager};
use std::fmt::{Debug, Formatter};
use std::sync::Arc;

#[derive(Clone)]
/// Execution runtime environment
pub struct RuntimeEnv {
    /// Runtime configuration
    pub config: RuntimeConfig,
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
        let memory_manager = Arc::new(MemoryManager::new(
            (config.max_memory as f64 * config.memory_fraction) as usize,
        ));
        let disk_manager = Arc::new(DiskManager::new(&config.local_dirs)?);
        Ok(Self {
            config,
            memory_manager,
            disk_manager,
        })
    }

    /// Get execution batch size based on config
    pub fn batch_size(&self) -> usize {
        self.config.batch_size
    }

    /// Register the consumer to get it tracked
    pub fn register_consumer(&self, memory_consumer: &Arc<dyn MemoryConsumer>) {
        self.memory_manager.register_consumer(memory_consumer);
    }

    /// Drop the consumer from get tracked
    pub fn drop_consumer(&self, id: &MemoryConsumerId) {
        self.memory_manager.drop_consumer(id)
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
    /// Max execution memory allowed for DataFusion.
    /// Defaults to `usize::MAX`
    pub max_memory: usize,
    /// The fraction of total memory used for execution.
    /// The purpose of this config is to set aside memory for untracked data structures,
    /// and imprecise size estimation during memory acquisition.
    /// Defaults to 0.7
    pub memory_fraction: f64,
    /// Local dirs to store temporary files during execution.
    pub local_dirs: Vec<String>,
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

    /// Customize exec size
    pub fn with_max_execution_memory(mut self, max_memory: usize) -> Self {
        assert!(max_memory > 0);
        self.max_memory = max_memory;
        self
    }

    /// Customize exec memory fraction
    pub fn with_memory_fraction(mut self, fraction: f64) -> Self {
        assert!(fraction > 0f64 && fraction <= 1f64);
        self.memory_fraction = fraction;
        self
    }

    /// Customize exec size
    pub fn with_local_dirs(mut self, local_dirs: Vec<String>) -> Self {
        assert!(!local_dirs.is_empty());
        self.local_dirs = local_dirs;
        self
    }
}

impl Default for RuntimeConfig {
    fn default() -> Self {
        let tmp_dir = tempfile::tempdir().unwrap();
        let path = tmp_dir.path().to_str().unwrap().to_string();
        std::mem::forget(tmp_dir);

        Self {
            batch_size: 8192,
            // Effectively "no limit"
            max_memory: usize::MAX,
            memory_fraction: 0.7,
            local_dirs: vec![path],
        }
    }
}
