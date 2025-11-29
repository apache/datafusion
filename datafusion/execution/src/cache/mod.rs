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

pub mod cache_manager;
pub mod cache_unit;
pub mod lru_queue;

/// The cache accessor, users usually working on this interface while manipulating caches.
/// This interface does not get `mut` references and thus has to handle its own
/// locking via internal mutability. It can be accessed via multiple concurrent queries
/// during planning and execution.
pub trait CacheAccessor<K, V>: Send + Sync {
    // Extra info but not part of the cache key or cache value.
    type Extra: Clone;

    /// Get value from cache.
    fn get(&self, k: &K) -> Option<V>;
    /// Get value from cache.
    fn get_with_extra(&self, k: &K, e: &Self::Extra) -> Option<V>;
    /// Put value into cache. Returns the old value associated with the key if there was one.
    fn put(&self, key: &K, value: V) -> Option<V>;
    /// Put value into cache. Returns the old value associated with the key if there was one.
    fn put_with_extra(&self, key: &K, value: V, e: &Self::Extra) -> Option<V>;
    /// Remove an entry from the cache, returning value if they existed in the map.
    fn remove(&self, k: &K) -> Option<V>;
    /// Check if the cache contains a specific key.
    fn contains_key(&self, k: &K) -> bool;
    /// Fetch the total number of cache entries.
    fn len(&self) -> usize;
    /// Check if the Cache collection is empty or not.
    fn is_empty(&self) -> bool {
        self.len() == 0
    }
    /// Remove all entries from the cache.
    fn clear(&self);
    /// Return the cache name.
    fn name(&self) -> String;
}
