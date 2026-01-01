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

use std::future::Future;

pub mod cache_manager;
pub mod cache_unit;
pub mod lru_queue;

mod file_metadata_cache;
mod list_files_cache;

pub use file_metadata_cache::DefaultFilesMetadataCache;
pub use list_files_cache::DefaultListFilesCache;

/// A trait that can be implemented to provide custom cache behavior for the caches managed by
/// [`cache_manager::CacheManager`].
///
/// This trait provides a single atomic `get_or_update_with` method that handles both
/// cache retrieval and population in one operation. The closure receives the existing
/// cached value (if any) and returns the value to cache.
///
/// ## Note on dyn-compatibility
///
/// This trait is NOT dyn-compatible due to the generic type parameters on `get_or_update_with`.
/// For dyn-compatible cache traits, use the specific cache traits like [`cache_manager::FileStatisticsCache`],
/// [`cache_manager::ListFilesCache`], or [`cache_manager::FileMetadataCache`] which use `async_trait`.
///
/// ## Validation
///
/// Validation metadata (e.g., file size, last modified time) should be embedded in the
/// value type `V`. The closure is responsible for validating stale entries by comparing
/// the cached metadata against the current state.
///
/// ## Thread Safety
///
/// Implementations must handle their own locking via internal mutability, as methods do not
/// take mutable references and may be accessed by multiple concurrent queries.
///
/// ## Async Safety with DashMap
///
/// When implementing with DashMap, ensure locks are NOT held across await points to avoid
/// [potential deadlocks](https://dev.to/acter/beware-of-the-dashmap-deadlock-lij).
/// The correct pattern is:
/// 1. Get existing value (lock acquired and released immediately)
/// 2. Await the closure (no lock held)
/// 3. Insert result (lock acquired and released immediately)
pub trait CacheAccessor<K, V>: Send + Sync {
    /// Atomically get or update a cache entry.
    ///
    /// The closure receives `Option<V>`:
    /// - `Some(cached_value)` if something is cached (may be stale - closure should validate)
    /// - `None` if not cached
    ///
    /// The closure should:
    /// - Validate the existing value against current state (if applicable)
    /// - Return the value to cache (can return existing unchanged, or compute new)
    ///
    /// Returns the value from the closure, which is also stored in the cache.
    ///
    /// # Example
    ///
    /// ```ignore
    /// let result = cache.get_or_update_with(&key, |existing| async move {
    ///     // Check if cached and still valid
    ///     if let Some(cached) = existing.filter(|c| c.is_valid_for(&current_meta)) {
    ///         return Ok(cached);
    ///     }
    ///     // Cache miss or invalid - compute new value
    ///     let new_value = compute_value().await?;
    ///     Ok(new_value)
    /// }).await?;
    /// ```
    fn get_or_update_with<F, Fut, E>(
        &self,
        key: &K,
        f: F,
    ) -> impl Future<Output = Result<V, E>> + Send
    where
        F: FnOnce(Option<V>) -> Fut + Send,
        Fut: Future<Output = Result<V, E>> + Send,
        E: Send + 'static,
        V: Clone + Send;

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
