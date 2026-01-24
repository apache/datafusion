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

mod file_metadata_cache;
mod list_files_cache;

pub use file_metadata_cache::DefaultFilesMetadataCache;
pub use list_files_cache::DefaultListFilesCache;
pub use list_files_cache::ListFilesEntry;
pub use list_files_cache::TableScopedPath;

/// Base trait for cache implementations with common operations.
///
/// This trait provides the fundamental cache operations (`get`, `put`, `remove`, etc.)
/// that all cache types share. Specific cache traits like [`cache_manager::FileStatisticsCache`],
/// [`cache_manager::ListFilesCache`], and [`cache_manager::FileMetadataCache`] extend this
/// trait with their specialized methods.
///
/// ## Thread Safety
///
/// Implementations must handle their own locking via internal mutability, as methods do not
/// take mutable references and may be accessed by multiple concurrent queries.
///
/// ## Validation Pattern
///
/// Validation metadata (e.g., file size, last modified time) should be embedded in the
/// value type `V`. The typical usage pattern is:
/// 1. Call `get(key)` to check for cached value
/// 2. If `Some(cached)`, validate with `cached.is_valid_for(&current_meta)`
/// 3. If invalid or missing, compute new value and call `put(key, new_value)`
pub trait CacheAccessor<K, V>: Send + Sync {
    /// Get a cached entry if it exists.
    ///
    /// Returns the cached value without any validation. The caller should
    /// validate the returned value if freshness matters.
    fn get(&self, key: &K) -> Option<V>;

    /// Store a value in the cache.
    ///
    /// Returns the previous value if one existed.
    fn put(&self, key: &K, value: V) -> Option<V>;

    /// Remove an entry from the cache, returning the value if it existed.
    fn remove(&self, k: &K) -> Option<V>;

    /// Check if the cache contains a specific key.
    fn contains_key(&self, k: &K) -> bool;

    /// Fetch the total number of cache entries.
    fn len(&self) -> usize;

    /// Check if the cache collection is empty.
    fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Remove all entries from the cache.
    fn clear(&self);

    /// Return the cache name.
    fn name(&self) -> String;
}
