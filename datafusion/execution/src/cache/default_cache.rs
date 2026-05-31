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

use std::sync::{Arc, Mutex};
use std::time::Duration;

use datafusion_common::TableReference;
use datafusion_common::instant::Instant;
use datafusion_common::{HashMap, Result};

use crate::cache::lru_queue::LruQueue;
use crate::cache::{Cache, CacheEntryInfo, CacheKey, CacheValue};

/// Source of the current time used by a [`DefaultCache`] when applying TTLs.
pub trait TimeProvider: Send + Sync {
    /// Return the current instant.
    fn now(&self) -> Instant;
}

/// [`TimeProvider`] backed by [`Instant::now`].
///
/// This is the default time source used by [`DefaultCache`]
#[derive(Debug, Default)]
pub struct SystemTimeProvider;

impl TimeProvider for SystemTimeProvider {
    fn now(&self) -> Instant {
        Instant::now()
    }
}

#[derive(Clone)]
struct ValueEntry<V: CacheValue> {
    value: V,
    expires: Option<Instant>,
}

struct DefaultCacheState<K: CacheKey, V: CacheValue> {
    lru_queue: LruQueue<K, ValueEntry<V>>,
    hits: HashMap<K, usize>,
    memory_limit: usize,
    memory_used: usize,
    ttl: Option<Duration>,
}

impl<K: CacheKey, V: CacheValue> DefaultCacheState<K, V> {
    fn new(memory_limit: usize, ttl: Option<Duration>) -> Self {
        Self {
            lru_queue: LruQueue::new(),
            hits: HashMap::new(),
            memory_limit,
            memory_used: 0,
            ttl,
        }
    }

    fn get(&mut self, key: &K, now: Instant) -> Option<V> {
        let entry = self.lru_queue.get(key)?;
        if let Some(exp) = entry.expires
            && now > exp
        {
            self.remove(key);
            return None;
        }
        let value = entry.value.clone();
        *self.hits.entry(key.clone()).or_insert(0) += 1;
        Some(value)
    }

    fn contains_key(&mut self, key: &K, now: Instant) -> bool {
        let Some(entry) = self.lru_queue.peek(key) else {
            return false;
        };
        match entry.expires {
            Some(exp) if now > exp => {
                self.remove(key);
                false
            }
            _ => true,
        }
    }

    fn put(&mut self, key: &K, value: V, now: Instant) -> Option<V> {
        let value_size = value.size();

        if value_size == 0 {
            return None;
        }

        let key_size = key.size();
        let total_size = key_size + value_size;

        if total_size > self.memory_limit {
            // Remove potential stale entry
            let result = self.remove(key);
            if let Some(stale_entry) = &result {
                self.memory_used -= key_size;
                self.memory_used -= stale_entry.size();
            }
            return result;
        }

        let expires = self.ttl.map(|ttl| now + ttl);
        let entry = ValueEntry { value, expires };

        self.memory_used += total_size;
        self.hits.insert(key.clone(), 0);
        let old = self.lru_queue.put(key.clone(), entry);
        if let Some(old_entry) = &old {
            self.memory_used -= key_size;
            self.memory_used -= old_entry.value.size();
        }

        self.evict_entries();

        old.map(|v| v.value)
    }

    fn remove(&mut self, key: &K) -> Option<V> {
        let entry = self.lru_queue.remove(key)?;
        self.memory_used -= key.size();
        self.memory_used -= entry.value.size();
        self.hits.remove(key);
        Some(entry.value)
    }

    fn evict_entries(&mut self) {
        while self.memory_used > self.memory_limit {
            let Some((evicted_key, evicted)) = self.lru_queue.pop() else {
                // cache is empty while memory_used > memory_limit, cannot happen
                log::error!(
                    "DefaultCache memory accounting bug: memory_used={} but cache is empty",
                    self.memory_used
                );
                debug_assert!(false, "memory_used > limit with empty cache");
                self.memory_used = 0;
                return;
            };
            self.memory_used -= evicted_key.size();
            self.memory_used -= evicted.value.size();
            self.hits.remove(&evicted_key);
        }
    }

    fn clear(&mut self) {
        self.lru_queue.clear();
        self.hits.clear();
        self.memory_used = 0;
    }
}

/// In-memory [`Cache`] with an LRU eviction policy, byte-based memory limit,
/// and optional per-entry TTL.
///
/// Entries are evicted in least-recently-used order whenever an insert would
/// push `memory_used` above `memory_limit`. Inserts whose own size exceeds the
/// limit are rejected (and any prior entry under the same key is removed).
/// When a TTL is configured, the expiration is stamped onto each entry at
/// insertion time and checked lazily on access.
pub struct DefaultCache<K: CacheKey, V: CacheValue> {
    state: Mutex<DefaultCacheState<K, V>>,
    time_provider: Arc<dyn TimeProvider>,
    name: String,
}

impl<K: CacheKey, V: CacheValue> DefaultCache<K, V> {
    /// Create a cache with the given memory budget in bytes and no TTL.
    pub fn new(memory_limit: usize) -> Self {
        Self::new_with_ttl(memory_limit, None)
    }

    /// Create a cache with the given memory budget in bytes and an optional
    /// TTL applied to every newly inserted entry.
    pub fn new_with_ttl(memory_limit: usize, ttl: Option<Duration>) -> Self {
        Self {
            state: Mutex::new(DefaultCacheState::new(memory_limit, ttl)),
            time_provider: Arc::new(SystemTimeProvider),
            name: "DefaultCache".to_string(),
        }
    }

    /// Override the cache name.
    pub fn with_name(mut self, name: impl Into<String>) -> Self {
        self.name = name.into();
        self
    }

    /// Override the time source used to stamp and check TTLs.
    pub fn with_time_provider(mut self, provider: Arc<dyn TimeProvider>) -> Self {
        self.time_provider = provider;
        self
    }

    /// Number of bytes currently accounted for by live entries.
    pub fn memory_used(&self) -> usize {
        self.state.lock().unwrap().memory_used
    }
}

impl<K: CacheKey, V: CacheValue> Cache<K, V> for DefaultCache<K, V> {
    fn get(&self, key: &K) -> Option<V> {
        let now = self.time_provider.now();
        let mut state = self.state.lock().unwrap();
        state.get(key, now)
    }

    fn put(&self, key: &K, value: V) -> Option<V> {
        let now = self.time_provider.now();
        let mut state = self.state.lock().unwrap();
        state.put(key, value, now)
    }

    fn remove(&self, k: &K) -> Option<V> {
        let mut state = self.state.lock().unwrap();
        state.remove(k)
    }

    fn contains_key(&self, k: &K) -> bool {
        let now = self.time_provider.now();
        let mut state = self.state.lock().unwrap();
        state.contains_key(k, now)
    }

    fn len(&self) -> usize {
        self.state.lock().unwrap().lru_queue.len()
    }

    fn clear(&self) {
        let mut state = self.state.lock().unwrap();
        state.clear();
    }

    fn name(&self) -> String {
        self.name.clone()
    }
    fn cache_limit(&self) -> usize {
        self.state.lock().unwrap().memory_limit
    }

    fn update_cache_limit(&self, limit: usize) {
        let mut state = self.state.lock().unwrap();
        state.memory_limit = limit;
        state.evict_entries();
    }

    fn cache_ttl(&self) -> Option<Duration> {
        self.state.lock().unwrap().ttl
    }

    fn update_cache_ttl(&self, ttl: Option<Duration>) {
        let mut state = self.state.lock().unwrap();
        state.ttl = ttl;
    }

    fn drop_table_entries(&self, table_ref: &Option<TableReference>) -> Result<()> {
        let mut state = self.state.lock().unwrap();
        let to_remove: Vec<K> = state
            .lru_queue
            .list_entries()
            .keys()
            .flat_map(|k| {
                let matches = match (k.table_ref(), table_ref.as_ref()) {
                    (Some(a), Some(b)) => a == b,
                    (None, None) => true,
                    _ => false,
                };
                matches.then(|| (*k).clone())
            })
            .collect();
        for k in &to_remove {
            state.remove(k);
        }
        Ok(())
    }

    fn list_entries(&self) -> HashMap<K, CacheEntryInfo<V>> {
        let state = self.state.lock().unwrap();
        state
            .lru_queue
            .list_entries()
            .into_iter()
            .map(|(k, entry)| {
                let hits = state.hits.get(k).copied().unwrap_or(0);
                let info = CacheEntryInfo {
                    value: entry.value.clone(),
                    size_bytes: entry.value.size(),
                    hits,
                    expires: entry.expires,
                };
                (k.clone(), info)
            })
            .collect()
    }
}
