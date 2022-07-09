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
use async_trait::async_trait;
use hashbrown::HashSet;
use log::{debug, warn};
use parking_lot::{Condvar, Mutex};
use std::fmt;
use std::fmt::{Debug, Display, Formatter};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

static CONSUMER_ID: AtomicUsize = AtomicUsize::new(0);

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

    /// return the maximum size of the memory, in bytes, this config will allow
    fn pool_size(&self) -> usize {
        match self {
            MemoryManagerConfig::Existing(existing) => existing.pool_size,
            MemoryManagerConfig::New {
                max_memory,
                memory_fraction,
            } => (*max_memory as f64 * *memory_fraction) as usize,
        }
    }
}

fn next_id() -> usize {
    CONSUMER_ID.fetch_add(1, Ordering::SeqCst)
}

/// Type of the memory consumer
pub enum ConsumerType {
    /// consumers that can grow its memory usage by requesting more from the memory manager or
    /// shrinks its memory usage when we can no more assign available memory to it.
    /// Examples are spillable sorter, spillable hashmap, etc.
    Requesting,
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
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        write!(f, "{}:{}", self.partition_id, self.id)
    }
}

#[async_trait]
/// A memory consumer that either takes up memory (of type `ConsumerType::Tracking`)
/// or grows/shrinks memory usage based on available memory (of type `ConsumerType::Requesting`).
pub trait MemoryConsumer: Send + Sync {
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
        let current = self.mem_used();
        debug!(
            "trying to acquire {} whiling holding {} from consumer {}",
            human_readable_size(required),
            human_readable_size(current),
            self.id(),
        );

        let can_grow_directly = self
            .memory_manager()
            .can_grow_directly(required, current)
            .await;
        if !can_grow_directly {
            debug!(
                "Failed to grow memory of {} directly from consumer {}, spilling first ...",
                human_readable_size(required),
                self.id()
            );
            let freed = self.spill().await?;
            self.memory_manager()
                .record_free_then_acquire(freed, required);
        }
        Ok(())
    }

    /// Return `freed` memory to the memory manager,
    /// may wake up other requesters waiting for their minimum memory quota.
    fn shrink(&self, freed: usize) {
        self.memory_manager().record_free(freed);
    }

    /// Spill in-memory buffers to disk, free memory, return the previous used
    async fn spill(&self) -> Result<usize>;

    /// Current memory used by this consumer
    fn mem_used(&self) -> usize;
}

impl Debug for dyn MemoryConsumer {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        write!(
            f,
            "{}[{}]: {}",
            self.name(),
            self.id(),
            human_readable_size(self.mem_used())
        )
    }
}

impl Display for dyn MemoryConsumer {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        write!(f, "{}[{}]", self.name(), self.id(),)
    }
}

/*
The memory management architecture is the following:

1. User designates max execution memory by setting RuntimeConfig.max_memory and RuntimeConfig.memory_fraction (float64 between 0..1).
   The actual max memory DataFusion could use `pool_size =  max_memory * memory_fraction`.
2. The entities that take up memory during its execution are called 'Memory Consumers'. Operators or others are encouraged to
   register themselves to the memory manager and report its usage through `mem_used()`.
3. There are two kinds of consumers:
   - 'Requesting' consumers that would acquire memory during its execution and release memory through `spill` if no more memory is available.
   - 'Tracking' consumers that exist for reporting purposes to provide a more accurate memory usage estimation for memory consumers.
4. Requesting and tracking consumers share the pool. Each controlling consumer could acquire a maximum of
   (pool_size - all_tracking_used) / active_num_controlling_consumers.

            Memory Space for the DataFusion Lib / Process of `pool_size`
   ┌──────────────────────────────────────────────z─────────────────────────────┐
   │                                              z                             │
   │                                              z                             │
   │               Requesting                     z          Tracking           │
   │            Memory Consumers                  z       Memory Consumers      │
   │                                              z                             │
   │                                              z                             │
   └──────────────────────────────────────────────z─────────────────────────────┘
*/

/// Manage memory usage during physical plan execution
#[derive(Debug)]
pub struct MemoryManager {
    requesters: Arc<Mutex<HashSet<MemoryConsumerId>>>,
    pool_size: usize,
    requesters_total: Arc<Mutex<usize>>,
    trackers_total: AtomicUsize,
    cv: Condvar,
}

impl MemoryManager {
    /// Create new memory manager based on the configuration
    #[allow(clippy::mutex_atomic)]
    pub fn new(config: MemoryManagerConfig) -> Arc<Self> {
        let pool_size = config.pool_size();

        match config {
            MemoryManagerConfig::Existing(manager) => manager,
            MemoryManagerConfig::New { .. } => {
                debug!(
                    "Creating memory manager with initial size {}",
                    human_readable_size(pool_size)
                );

                Arc::new(Self {
                    requesters: Arc::new(Mutex::new(HashSet::new())),
                    pool_size,
                    requesters_total: Arc::new(Mutex::new(0)),
                    trackers_total: AtomicUsize::new(0),
                    cv: Condvar::new(),
                })
            }
        }
    }

    fn get_tracker_total(&self) -> usize {
        self.trackers_total.load(Ordering::SeqCst)
    }

    pub(crate) fn grow_tracker_usage(&self, delta: usize) {
        self.trackers_total.fetch_add(delta, Ordering::SeqCst);
    }

    pub(crate) fn shrink_tracker_usage(&self, delta: usize) {
        let update =
            self.trackers_total
                .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |x| {
                    if x >= delta {
                        Some(x - delta)
                    } else {
                        None
                    }
                });
        update.unwrap_or_else(|_| {
            panic!(
                "Tracker total memory shrink by {} underflow, current value is ",
                delta
            )
        });
    }

    /// Return the total memory usage for all requesters
    pub fn get_requester_total(&self) -> usize {
        *self.requesters_total.lock()
    }

    /// Register a new memory requester
    pub(crate) fn register_requester(&self, requester_id: &MemoryConsumerId) {
        self.requesters.lock().insert(requester_id.clone());
    }

    fn max_mem_for_requesters(&self) -> usize {
        let trk_total = self.get_tracker_total();
        self.pool_size - trk_total
    }

    /// Grow memory attempt from a consumer, return if we could grant that much to it
    async fn can_grow_directly(&self, required: usize, current: usize) -> bool {
        let num_rqt = self.requesters.lock().len();
        let mut rqt_current_used = self.requesters_total.lock();
        let mut rqt_max = self.max_mem_for_requesters();

        let granted;
        loop {
            let max_per_rqt = rqt_max / num_rqt;
            let min_per_rqt = max_per_rqt / 2;

            if required + current >= max_per_rqt {
                granted = false;
                break;
            }

            let remaining = rqt_max.checked_sub(*rqt_current_used).unwrap_or_default();
            if remaining >= required {
                granted = true;
                *rqt_current_used += required;
                break;
            } else if current < min_per_rqt {
                // if we cannot acquire at lease 1/2n memory, just wait for others
                // to spill instead spill self frequently with limited total mem
                debug!(
                    "Cannot acquire a minimum amount of {} memory from the manager of total {}, waiting for others to spill ...",
                    human_readable_size(min_per_rqt), human_readable_size(self.pool_size));
                let now = Instant::now();
                self.cv.wait(&mut rqt_current_used);
                let elapsed = now.elapsed();
                if elapsed > Duration::from_secs(10) {
                    warn!("Elapsed on waiting for spilling: {:.2?}", elapsed);
                }
            } else {
                granted = false;
                break;
            }

            rqt_max = self.max_mem_for_requesters();
        }

        granted
    }

    fn record_free_then_acquire(&self, freed: usize, acquired: usize) {
        let mut requesters_total = self.requesters_total.lock();
        debug!(
            "free_then_acquire: total {}, freed {}, acquired {}",
            human_readable_size(*requesters_total),
            human_readable_size(freed),
            human_readable_size(acquired)
        );
        assert!(*requesters_total >= freed);
        *requesters_total -= freed;
        *requesters_total += acquired;
        self.cv.notify_all();
    }

    fn record_free(&self, freed: usize) {
        let mut requesters_total = self.requesters_total.lock();
        debug!(
            "free: total {}, freed {}",
            human_readable_size(*requesters_total),
            human_readable_size(freed)
        );
        assert!(*requesters_total >= freed);
        *requesters_total -= freed;
        self.cv.notify_all();
    }

    /// Drop a memory consumer and reclaim the memory
    pub(crate) fn drop_consumer(&self, id: &MemoryConsumerId, mem_used: usize) {
        // find in requesters first
        {
            let mut requesters = self.requesters.lock();
            if requesters.remove(id) {
                let mut total = self.requesters_total.lock();
                assert!(*total >= mem_used);
                *total -= mem_used;
                self.cv.notify_all();
                return;
            }
        }
        self.shrink_tracker_usage(mem_used);
        self.cv.notify_all();
    }
}

impl Display for MemoryManager {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        write!(f,
               "MemoryManager usage statistics: total {}, trackers used {}, total {} requesters used: {}",
               human_readable_size(self.pool_size),
               human_readable_size(self.get_tracker_total()),
               self.requesters.lock().len(),
               human_readable_size(self.get_requester_total()),
        )
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
    use crate::error::Result;
    use crate::execution::runtime_env::{RuntimeConfig, RuntimeEnv};
    use crate::execution::MemoryConsumer;
    use crate::physical_plan::metrics::{ExecutionPlanMetricsSet, MemTrackingMetrics};
    use async_trait::async_trait;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Arc;

    struct DummyRequester {
        id: MemoryConsumerId,
        runtime: Arc<RuntimeEnv>,
        spills: AtomicUsize,
        mem_used: AtomicUsize,
    }

    impl DummyRequester {
        fn new(partition: usize, runtime: Arc<RuntimeEnv>) -> Self {
            Self {
                id: MemoryConsumerId::new(partition),
                runtime,
                spills: AtomicUsize::new(0),
                mem_used: AtomicUsize::new(0),
            }
        }

        async fn do_with_mem(&self, grow: usize) -> Result<()> {
            self.try_grow(grow).await?;
            self.mem_used.fetch_add(grow, Ordering::SeqCst);
            Ok(())
        }

        fn get_spills(&self) -> usize {
            self.spills.load(Ordering::SeqCst)
        }
    }

    #[async_trait]
    impl MemoryConsumer for DummyRequester {
        fn name(&self) -> String {
            "dummy".to_owned()
        }

        fn id(&self) -> &MemoryConsumerId {
            &self.id
        }

        fn memory_manager(&self) -> Arc<MemoryManager> {
            self.runtime.memory_manager.clone()
        }

        fn type_(&self) -> &ConsumerType {
            &ConsumerType::Requesting
        }

        async fn spill(&self) -> Result<usize> {
            self.spills.fetch_add(1, Ordering::SeqCst);
            let used = self.mem_used.swap(0, Ordering::SeqCst);
            Ok(used)
        }

        fn mem_used(&self) -> usize {
            self.mem_used.load(Ordering::SeqCst)
        }
    }

    struct DummyTracker {
        id: MemoryConsumerId,
        runtime: Arc<RuntimeEnv>,
        mem_used: usize,
    }

    impl DummyTracker {
        fn new(partition: usize, runtime: Arc<RuntimeEnv>, mem_used: usize) -> Self {
            runtime.grow_tracker_usage(mem_used);
            Self {
                id: MemoryConsumerId::new(partition),
                runtime,
                mem_used,
            }
        }
    }

    #[async_trait]
    impl MemoryConsumer for DummyTracker {
        fn name(&self) -> String {
            "dummy".to_owned()
        }

        fn id(&self) -> &MemoryConsumerId {
            &self.id
        }

        fn memory_manager(&self) -> Arc<MemoryManager> {
            self.runtime.memory_manager.clone()
        }

        fn type_(&self) -> &ConsumerType {
            &ConsumerType::Tracking
        }

        async fn spill(&self) -> Result<usize> {
            Ok(0)
        }

        fn mem_used(&self) -> usize {
            self.mem_used
        }
    }

    #[tokio::test]
    async fn basic_functionalities() {
        let config = RuntimeConfig::new()
            .with_memory_manager(MemoryManagerConfig::try_new_limit(100, 1.0).unwrap());
        let runtime = Arc::new(RuntimeEnv::new(config).unwrap());

        DummyTracker::new(0, runtime.clone(), 5);
        assert_eq!(runtime.memory_manager.get_tracker_total(), 5);

        let tracker1 = DummyTracker::new(0, runtime.clone(), 10);
        assert_eq!(runtime.memory_manager.get_tracker_total(), 15);

        DummyTracker::new(0, runtime.clone(), 15);
        assert_eq!(runtime.memory_manager.get_tracker_total(), 30);

        runtime.drop_consumer(tracker1.id(), tracker1.mem_used);
        assert_eq!(runtime.memory_manager.get_tracker_total(), 20);

        // MemTrackingMetrics as an easy way to track memory
        let ms = ExecutionPlanMetricsSet::new();
        let tracking_metric = MemTrackingMetrics::new_with_rt(&ms, 0, runtime.clone());
        tracking_metric.init_mem_used(15);
        assert_eq!(runtime.memory_manager.get_tracker_total(), 35);

        drop(tracking_metric);
        assert_eq!(runtime.memory_manager.get_tracker_total(), 20);

        let requester1 = DummyRequester::new(0, runtime.clone());
        runtime.register_requester(requester1.id());

        // first requester entered, should be able to use any of the remaining 80
        requester1.do_with_mem(40).await.unwrap();
        requester1.do_with_mem(10).await.unwrap();
        assert_eq!(requester1.get_spills(), 0);
        assert_eq!(requester1.mem_used(), 50);
        assert_eq!(*runtime.memory_manager.requesters_total.lock(), 50);

        let requester2 = DummyRequester::new(0, runtime.clone());
        runtime.register_requester(requester2.id());

        requester2.do_with_mem(20).await.unwrap();
        requester2.do_with_mem(30).await.unwrap();
        assert_eq!(requester2.get_spills(), 1);
        assert_eq!(requester2.mem_used(), 30);

        requester1.do_with_mem(10).await.unwrap();
        assert_eq!(requester1.get_spills(), 1);
        assert_eq!(requester1.mem_used(), 10);

        assert_eq!(*runtime.memory_manager.requesters_total.lock(), 40);
    }

    #[tokio::test]
    #[should_panic(expected = "invalid max_memory. Expected greater than 0, got 0")]
    async fn test_try_new_with_limit_0() {
        MemoryManagerConfig::try_new_limit(0, 1.0).unwrap();
    }

    #[tokio::test]
    #[should_panic(
        expected = "invalid fraction. Expected greater than 0 and less than 1.0, got -9.6"
    )]
    async fn test_try_new_with_limit_neg_fraction() {
        MemoryManagerConfig::try_new_limit(100, -9.6).unwrap();
    }

    #[tokio::test]
    #[should_panic(
        expected = "invalid fraction. Expected greater than 0 and less than 1.0, got 9.6"
    )]
    async fn test_try_new_with_limit_too_large() {
        MemoryManagerConfig::try_new_limit(100, 9.6).unwrap();
    }

    #[tokio::test]
    async fn test_try_new_with_limit_pool_size() {
        let config = MemoryManagerConfig::try_new_limit(100, 0.5).unwrap();
        assert_eq!(config.pool_size(), 50);

        let config = MemoryManagerConfig::try_new_limit(100000, 0.1).unwrap();
        assert_eq!(config.pool_size(), 10000);
    }
}
