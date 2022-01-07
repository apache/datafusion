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

use crate::error::Result;
use async_trait::async_trait;
use futures::lock::Mutex;
use futures::stream::iter;
use futures::StreamExt;
use hashbrown::HashMap;
use log::info;
use std::fmt;
use std::fmt::{Debug, Display, Formatter};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tokio::runtime::Handle;
use tokio::task;
use tokio::task::JoinHandle;

static mut CONSUMER_ID: AtomicUsize = AtomicUsize::new(0);

fn next_id() -> usize {
    unsafe { CONSUMER_ID.fetch_add(1, Ordering::SeqCst) }
}

/// Type of the memory consumer
pub enum ConsumerType {
    /// consumers that can grow or shrink its memory usage during execution
    /// such as spillable sorter, spillable hashmap, etc.
    Controlling,
    /// consumers that are not spillable, counting in for only tracking purpose.
    Tracking,
}

#[derive(Clone, Debug, Hash, Eq, PartialEq)]
/// Id that uniquely identifies a Memory Consumer
pub struct MemoryConsumerId {
    /// partition the consumer belongs to
    pub partition_id: usize,
    /// unique id
    pub id: usize,
}

impl MemoryConsumerId {
    /// Auto incremented new Id
    pub fn new(partition_id: usize) -> Self {
        let id = next_id();
        Self { partition_id, id }
    }
}

impl Display for MemoryConsumerId {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "{}:{}", self.partition_id, self.id)
    }
}

#[async_trait]
/// A memory consumer that either takes up memory (of type `ConsumerType::Tracking`)
/// or grows/shrinks memory usage based on available memory (of type `ConsumerType::Controlling`).
pub trait MemoryConsumer: Send + Sync + Debug {
    /// Display name of the consumer
    fn name(&self) -> String;

    /// Unique id of the consumer
    fn id(&self) -> &MemoryConsumerId;

    /// Ptr to MemoryManager
    fn memory_manager(&self) -> Arc<MemoryManager>;

    /// Partition that the consumer belongs to
    fn partition_id(&self) -> usize {
        self.id().partition_id
    }

    /// Type of the consumer
    fn type_(&self) -> &ConsumerType;

    /// Grow memory by `required` to buffer more data in memory,
    /// this may trigger spill before grow when the memory threshold is
    /// reached for this consumer.
    async fn try_grow(&self, required: usize) -> Result<()> {
        let current_usage = self.mem_used().await;
        let can_grow = self
            .memory_manager()
            .try_grow(required, current_usage, self.id())
            .await;
        if !can_grow {
            info!(
                "Failed to grow memory of {} from {}, spilling...",
                human_readable_size(required),
                self.id()
            );
            self.spill().await?;
        }
        Ok(())
    }

    /// Spill in-memory buffers to disk, free memory
    async fn spill(&self) -> Result<()>;

    /// Current memory used by this consumer
    async fn mem_used(&self) -> usize;

    /// Current status of the consumer
    async fn str_repr(&self) -> String {
        let mem = self.mem_used().await;
        format!(
            "{}[{}]: {}",
            self.name(),
            self.id(),
            human_readable_size(mem)
        )
    }

    /// Memory usage for display / debug purpose
    fn mem_used_sync(&self) -> usize {
        Handle::current().block_on(async { self.mem_used().await })
    }
}

/// Manage memory usage during physical plan execution
pub struct MemoryManager {
    consumers: Arc<Mutex<HashMap<MemoryConsumerId, Arc<dyn MemoryConsumer>>>>,
    trackers: Arc<Mutex<HashMap<MemoryConsumerId, Arc<dyn MemoryConsumer>>>>,
    trackers_total_usage: AtomicUsize,
    pool_size: usize,
    join_handle: Arc<Mutex<Option<JoinHandle<()>>>>,
}

impl MemoryManager {
    /// Create new memory manager based on max available pool_size
    pub fn new(pool_size: usize) -> Self {
        info!(
            "Creating memory manager with initial size {}",
            human_readable_size(pool_size)
        );
        Self {
            consumers: Arc::new(Mutex::new(HashMap::new())),
            trackers: Arc::new(Mutex::new(HashMap::new())),
            trackers_total_usage: AtomicUsize::new(0),
            pool_size,
            join_handle: Arc::new(Mutex::new(None)),
        }
    }

    async fn update_tracker_total(self: &Arc<Self>) {
        let trackers = self.trackers.lock().await;
        if trackers.len() > 0 {
            let sum = iter(trackers.values())
                .fold(0usize, |acc, y| async move { acc + y.mem_used().await })
                .await;
            drop(trackers);
            self.trackers_total_usage.store(sum, Ordering::SeqCst);
        }
    }

    /// Initialize
    pub(crate) async fn initialize(self: &Arc<Self>) {
        let manager = self.clone();
        let handle = task::spawn(async move {
            let mut interval_timer = tokio::time::interval(Duration::from_secs(60));
            loop {
                interval_timer.tick().await;
                manager.update_tracker_total().await;
            }
        });
        let _ = self.join_handle.lock().await.insert(handle);
    }

    /// Register a new memory consumer for memory usage tracking
    pub(crate) async fn register_consumer(
        self: &Arc<Self>,
        consumer: Arc<dyn MemoryConsumer>,
    ) {
        let id = consumer.id().clone();
        match consumer.type_() {
            ConsumerType::Controlling => {
                let mut consumers = self.consumers.lock().await;
                consumers.insert(id, consumer);
            }
            ConsumerType::Tracking => {
                let mut trackers = self.trackers.lock().await;
                trackers.insert(id, consumer);
            }
        }
    }

    /// Grow memory attempt from a consumer, return if we could grant that much to it
    async fn try_grow(
        self: &Arc<Self>,
        required: usize,
        current: usize,
        consumer_id: &MemoryConsumerId,
    ) -> bool {
        let max_per_op = {
            let total_available =
                self.pool_size - self.trackers_total_usage.load(Ordering::SeqCst);
            let ops = self.consumers.lock().await.len();
            (total_available / ops) as usize
        };
        let granted = required + current < max_per_op;
        info!(
            "trying to acquire {} whiling holding {} from {}, got: {}",
            human_readable_size(required),
            human_readable_size(current),
            consumer_id,
            granted,
        );
        granted
    }

    /// Drop a memory consumer from memory usage tracking
    pub(crate) async fn drop_consumer(self: &Arc<Self>, id: &MemoryConsumerId) {
        // find in consumers first
        {
            let mut consumers = self.consumers.lock().await;
            if consumers.contains_key(id) {
                consumers.remove(id);
                return;
            }
        }
        {
            let mut trackers = self.trackers.lock().await;
            if trackers.contains_key(id) {
                let removed = trackers.remove(id);
                match removed {
                    None => {}
                    Some(tracker) => {
                        let usage = tracker.mem_used().await;
                        self.trackers_total_usage.fetch_sub(usage, Ordering::SeqCst);
                    }
                }
            }
        }
    }

    /// Shutdown
    #[allow(dead_code)]
    pub(crate) async fn shutdown(self: &Arc<Self>) {
        let maybe_handle = self.join_handle.lock().await.take();
        match maybe_handle {
            None => {}
            Some(handle) => handle.abort(),
        }
    }

    #[allow(dead_code)]
    pub(crate) async fn print_memory_usage(self: &Arc<Self>) {
        let consumers = iter(self.consumers.lock().await.values())
            .fold(vec![], |mut acc, y| async move {
                acc.push(y.str_repr().await);
                acc
            })
            .await;
        let tracker_mem = self.trackers_total_usage.load(Ordering::SeqCst);
        info!("Memory usage statistics: total {}, tracker used {}, total {} consumers detail: \n {},",
            human_readable_size(self.pool_size),
            human_readable_size(tracker_mem),
            &consumers.len(),
            consumers.join("\n"));
    }
}

const TB: u64 = 1 << 40;
const GB: u64 = 1 << 30;
const MB: u64 = 1 << 20;
const KB: u64 = 1 << 10;

fn human_readable_size(size: usize) -> String {
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
