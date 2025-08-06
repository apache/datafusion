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

//! # Memory Tracker

use parking_lot::{Mutex, Mutex as StdMutex};
use std::{
    collections::HashMap,
    sync::atomic::{AtomicBool, Ordering},
    sync::{Arc, LazyLock},
};
#[derive(Default, Debug)]
pub struct MemoryMetrics {
    entries: HashMap<String, usize>,
}

impl MemoryMetrics {
    pub fn record(&mut self, operator: &str, bytes: usize) {
        *self.entries.entry(operator.to_string()).or_insert(0) += bytes;
    }

    pub fn snapshot(&self) -> HashMap<String, usize> {
        self.entries.clone()
    }

    pub fn clear(&mut self) {
        self.entries.clear();
    }
}

#[derive(Debug)]
pub struct MemoryTracker {
    enabled: AtomicBool,
    metrics: Arc<Mutex<MemoryMetrics>>,
}

impl MemoryTracker {
    pub fn new() -> Self {
        Self {
            enabled: AtomicBool::new(false),
            metrics: Arc::new(Mutex::new(MemoryMetrics::default())),
        }
    }

    pub fn enable(&self) {
        self.enabled.store(true, Ordering::Relaxed);
        self.metrics.lock().clear();
    }

    pub fn disable(&self) {
        self.enabled.store(false, Ordering::Relaxed);
    }

    pub fn record_memory(&self, operator: &str, bytes: usize) {
        if !self.enabled.load(Ordering::Relaxed) {
            return;
        }
        self.metrics.lock().record(operator, bytes);
    }

    pub fn metrics(&self) -> HashMap<String, usize> {
        self.metrics.lock().snapshot()
    }

    pub fn reset(&self) {
        self.metrics.lock().clear();
    }
}

// Add Default impl to satisfy clippy new_without_default lint
impl Default for MemoryTracker {
    fn default() -> Self {
        Self::new()
    }
}

static GLOBAL_TRACKER: LazyLock<StdMutex<Option<Arc<MemoryTracker>>>> =
    // std::sync::Mutex is used as contention is low; switch to parking_lot if performance issues arise
    LazyLock::new(|| StdMutex::new(None));

/// Set or clear the global memory tracker used for automatic instrumentation
pub fn set_global_memory_tracker(tracker: Option<Arc<MemoryTracker>>) {
    *GLOBAL_TRACKER.lock() = tracker;
}

/// Get the currently configured global memory tracker
pub fn global_memory_tracker() -> Option<Arc<MemoryTracker>> {
    GLOBAL_TRACKER.lock().clone()
}
