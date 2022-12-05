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

//! Manages all available memory during query execution

use crate::error::{DataFusionError, Result};
use log::debug;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

pub mod proxy;

#[derive(Debug, Clone)]
/// Configuration information for memory management
pub enum MemoryManagerConfig {
    /// Use the existing [MemoryManager]
    Existing(Arc<MemoryManager>),

    /// Create a new [MemoryManager] that will use up to some
    /// fraction of total system memory.
    New {
        /// Max execution memory allowed for DataFusion.  Defaults to
        /// `usize::MAX`, which will not attempt to limit the memory
        /// used during plan execution.
        max_memory: usize,

        /// The fraction of `max_memory` that the memory manager will
        /// use for execution.
        ///
        /// The purpose of this config is to set aside memory for
        /// untracked data structures, and imprecise size estimation
        /// during memory acquisition.  Defaults to 0.7
        memory_fraction: f64,
    },
}

impl Default for MemoryManagerConfig {
    fn default() -> Self {
        Self::New {
            max_memory: usize::MAX,
            memory_fraction: 0.7,
        }
    }
}

impl MemoryManagerConfig {
    /// Create a new memory [MemoryManager] with no limit on the
    /// memory used
    pub fn new() -> Self {
        Default::default()
    }

    /// Create a configuration based on an existing [MemoryManager]
    pub fn new_existing(existing: Arc<MemoryManager>) -> Self {
        Self::Existing(existing)
    }

    /// Create a new [MemoryManager] with a `max_memory` and `fraction`
    pub fn try_new_limit(max_memory: usize, memory_fraction: f64) -> Result<Self> {
        if max_memory == 0 {
            return Err(DataFusionError::Plan(format!(
                "invalid max_memory. Expected greater than 0, got {}",
                max_memory
            )));
        }
        if !(memory_fraction > 0f64 && memory_fraction <= 1f64) {
            return Err(DataFusionError::Plan(format!(
                "invalid fraction. Expected greater than 0 and less than 1.0, got {}",
                memory_fraction
            )));
        }

        Ok(Self::New {
            max_memory,
            memory_fraction,
        })
    }
}

/// The memory manager maintains a fixed size pool of memory
/// from which portions can be allocated
#[derive(Debug)]
pub struct MemoryManager {
    state: Arc<MemoryManagerState>,
}

impl MemoryManager {
    /// Create new memory manager based on the configuration
    pub fn new(config: MemoryManagerConfig) -> Arc<Self> {
        match config {
            MemoryManagerConfig::Existing(manager) => manager,
            MemoryManagerConfig::New {
                max_memory,
                memory_fraction,
            } => {
                let pool_size = (max_memory as f64 * memory_fraction) as usize;
                debug!(
                    "Creating memory manager with initial size {}",
                    human_readable_size(pool_size)
                );

                Arc::new(Self {
                    state: Arc::new(MemoryManagerState {
                        pool_size,
                        used: AtomicUsize::new(0),
                    }),
                })
            }
        }
    }

    /// Returns the maximum pool size
    ///
    /// Note: this can be less than the amount allocated as a result of [`MemoryManager::allocate`]
    pub fn pool_size(&self) -> usize {
        self.state.pool_size
    }

    /// Returns the number of allocated bytes
    ///
    /// Note: this can exceed the pool size as a result of [`MemoryManager::allocate`]
    pub fn allocated(&self) -> usize {
        self.state.used.load(Ordering::Relaxed)
    }

    /// Returns a new empty allocation identified by `name`
    pub fn new_tracked_allocation(&self, name: String) -> TrackedAllocation {
        TrackedAllocation::new_empty(name, Arc::clone(&self.state))
    }
}

impl std::fmt::Display for MemoryManager {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(
            f,
            "MemoryManager(capacity: {}, allocated: {})",
            human_readable_size(self.state.pool_size),
            human_readable_size(self.allocated()),
        )
    }
}

#[derive(Debug)]
struct MemoryManagerState {
    pool_size: usize,
    used: AtomicUsize,
}

/// A [`TrackedAllocation`] tracks a reservation of memory in a [`MemoryManager`]
/// that is freed back to the memory pool on drop
#[derive(Debug)]
pub struct TrackedAllocation {
    name: String,
    size: usize,
    state: Arc<MemoryManagerState>,
}

impl TrackedAllocation {
    fn new_empty(name: String, state: Arc<MemoryManagerState>) -> Self {
        Self {
            name,
            size: 0,
            state,
        }
    }

    /// Returns the size of this [`TrackedAllocation`] in bytes
    pub fn size(&self) -> usize {
        self.size
    }

    /// Frees all bytes from this allocation returning the number of bytes freed
    pub fn free(&mut self) -> usize {
        let size = self.size;
        if size != 0 {
            self.shrink(size)
        }
        size
    }

    /// Frees `capacity` bytes from this allocation
    ///
    /// # Panics
    ///
    /// Panics if `capacity` exceeds [`Self::size`]
    pub fn shrink(&mut self, capacity: usize) {
        let new_size = self.size.checked_sub(capacity).unwrap();
        self.state.used.fetch_sub(capacity, Ordering::SeqCst);
        self.size = new_size
    }

    /// Sets the size of this allocation to `capacity`
    pub fn resize(&mut self, capacity: usize) {
        use std::cmp::Ordering;
        match capacity.cmp(&self.size) {
            Ordering::Greater => self.grow(capacity - self.size),
            Ordering::Less => self.shrink(self.size - capacity),
            _ => {}
        }
    }

    /// Increase the size of this by `capacity` bytes
    pub fn grow(&mut self, capacity: usize) {
        self.state.used.fetch_add(capacity, Ordering::SeqCst);
        self.size += capacity;
    }

    /// Try to increase the size of this [`TrackedAllocation`] by `capacity` bytes
    pub fn try_grow(&mut self, capacity: usize) -> Result<()> {
        let pool_size = self.state.pool_size;
        self.state
            .used
            .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |used| {
                let new_used = used + capacity;
                (new_used <= pool_size).then_some(new_used)
            })
            .map_err(|used| DataFusionError::ResourcesExhausted(format!("Failed to allocate additional {} bytes for {} with {} bytes already allocated - maximum available is {}", capacity, self.name, self.size, used)))?;
        self.size += capacity;
        Ok(())
    }
}

impl Drop for TrackedAllocation {
    fn drop(&mut self) {
        self.free();
    }
}

const TB: u64 = 1 << 40;
const GB: u64 = 1 << 30;
const MB: u64 = 1 << 20;
const KB: u64 = 1 << 10;

/// Present size in human readable form
pub fn human_readable_size(size: usize) -> String {
    let size = size as u64;
    let (value, unit) = {
        if size >= 2 * TB {
            (size as f64 / TB as f64, "TB")
        } else if size >= 2 * GB {
            (size as f64 / GB as f64, "GB")
        } else if size >= 2 * MB {
            (size as f64 / MB as f64, "MB")
        } else if size >= 2 * KB {
            (size as f64 / KB as f64, "KB")
        } else {
            (size as f64, "B")
        }
    };
    format!("{:.1} {}", value, unit)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    #[should_panic(expected = "invalid max_memory. Expected greater than 0, got 0")]
    fn test_try_new_with_limit_0() {
        MemoryManagerConfig::try_new_limit(0, 1.0).unwrap();
    }

    #[test]
    #[should_panic(
        expected = "invalid fraction. Expected greater than 0 and less than 1.0, got -9.6"
    )]
    fn test_try_new_with_limit_neg_fraction() {
        MemoryManagerConfig::try_new_limit(100, -9.6).unwrap();
    }

    #[test]
    #[should_panic(
        expected = "invalid fraction. Expected greater than 0 and less than 1.0, got 9.6"
    )]
    fn test_try_new_with_limit_too_large() {
        MemoryManagerConfig::try_new_limit(100, 9.6).unwrap();
    }

    #[test]
    fn test_try_new_with_limit_pool_size() {
        let config = MemoryManagerConfig::try_new_limit(100, 0.5).unwrap();
        let manager = MemoryManager::new(config);
        assert_eq!(manager.pool_size(), 50);

        let config = MemoryManagerConfig::try_new_limit(100000, 0.1).unwrap();
        let manager = MemoryManager::new(config);
        assert_eq!(manager.pool_size(), 10000);
    }

    #[test]
    fn test_memory_manager_underflow() {
        let config = MemoryManagerConfig::try_new_limit(100, 0.5).unwrap();
        let manager = MemoryManager::new(config);
        let mut a1 = manager.new_tracked_allocation("a1".to_string());
        assert_eq!(manager.allocated(), 0);

        a1.grow(100);
        assert_eq!(manager.allocated(), 100);

        assert_eq!(a1.free(), 100);
        assert_eq!(manager.allocated(), 0);

        a1.try_grow(100).unwrap_err();
        assert_eq!(manager.allocated(), 0);

        a1.try_grow(30).unwrap();
        assert_eq!(manager.allocated(), 30);

        let mut a2 = manager.new_tracked_allocation("a2".to_string());
        a2.try_grow(25).unwrap_err();
        assert_eq!(manager.allocated(), 30);

        drop(a1);
        assert_eq!(manager.allocated(), 0);

        a2.try_grow(25).unwrap();
        assert_eq!(manager.allocated(), 25);
    }
}
