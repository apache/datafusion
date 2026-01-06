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

use std::mem::size_of;
use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
    time::Duration,
};

use datafusion_common::instant::Instant;
use object_store::{ObjectMeta, path::Path};

use crate::cache::{CacheAccessor, cache_manager::ListFilesCache, lru_queue::LruQueue};

pub trait TimeProvider: Send + Sync + 'static {
    fn now(&self) -> Instant;
}

#[derive(Debug, Default)]
pub struct SystemTimeProvider;

impl TimeProvider for SystemTimeProvider {
    fn now(&self) -> Instant {
        Instant::now()
    }
}

/// Default implementation of [`ListFilesCache`]
///
/// Caches file metadata for file listing operations.
///
/// # Internal details
///
/// The `memory_limit` parameter controls the maximum size of the cache, which uses a Least
/// Recently Used eviction algorithm. When adding a new entry, if the total number of entries in
/// the cache exceeds `memory_limit`, the least recently used entries are evicted until the total
/// size is lower than the `memory_limit`.
///
/// # `Extra` Handling
///
/// Users should use the [`Self::get`] and [`Self::put`] methods. The
/// [`Self::get_with_extra`] and [`Self::put_with_extra`] methods simply call
/// `get` and `put`, respectively.
pub struct DefaultListFilesCache {
    state: Mutex<DefaultListFilesCacheState>,
    time_provider: Arc<dyn TimeProvider>,
}

impl Default for DefaultListFilesCache {
    fn default() -> Self {
        Self::new(DEFAULT_LIST_FILES_CACHE_MEMORY_LIMIT, None)
    }
}

impl DefaultListFilesCache {
    /// Creates a new instance of [`DefaultListFilesCache`].
    ///
    /// # Arguments
    /// * `memory_limit` - The maximum size of the cache, in bytes.
    /// * `ttl` - The TTL (time-to-live) of entries in the cache.
    pub fn new(memory_limit: usize, ttl: Option<Duration>) -> Self {
        Self {
            state: Mutex::new(DefaultListFilesCacheState::new(memory_limit, ttl)),
            time_provider: Arc::new(SystemTimeProvider),
        }
    }

    #[cfg(test)]
    pub(crate) fn with_time_provider(mut self, provider: Arc<dyn TimeProvider>) -> Self {
        self.time_provider = provider;
        self
    }

    /// Returns the cache's memory limit in bytes.
    pub fn cache_limit(&self) -> usize {
        self.state.lock().unwrap().memory_limit
    }

    /// Updates the cache with a new memory limit in bytes.
    pub fn update_cache_limit(&self, limit: usize) {
        let mut state = self.state.lock().unwrap();
        state.memory_limit = limit;
        state.evict_entries();
    }

    /// Returns the TTL (time-to-live) applied to cache entries.
    pub fn cache_ttl(&self) -> Option<Duration> {
        self.state.lock().unwrap().ttl
    }
}

#[derive(Clone, PartialEq, Debug)]
pub struct ListFilesEntry {
    pub metas: Arc<Vec<ObjectMeta>>,
    pub size_bytes: usize,
    pub expires: Option<Instant>,
}

impl ListFilesEntry {
    fn try_new(
        metas: Arc<Vec<ObjectMeta>>,
        ttl: Option<Duration>,
        now: Instant,
    ) -> Option<Self> {
        let size_bytes = (metas.capacity() * size_of::<ObjectMeta>())
            + metas.iter().map(meta_heap_bytes).reduce(|acc, b| acc + b)?;

        Some(Self {
            metas,
            size_bytes,
            expires: ttl.map(|t| now + t),
        })
    }
}

/// Calculates the number of bytes an [`ObjectMeta`] occupies in the heap.
fn meta_heap_bytes(object_meta: &ObjectMeta) -> usize {
    let mut size = object_meta.location.as_ref().len();

    if let Some(e) = &object_meta.e_tag {
        size += e.len();
    }
    if let Some(v) = &object_meta.version {
        size += v.len();
    }

    size
}

/// The default memory limit for the [`DefaultListFilesCache`]
pub const DEFAULT_LIST_FILES_CACHE_MEMORY_LIMIT: usize = 1024 * 1024; // 1MiB

/// The default cache TTL for the [`DefaultListFilesCache`]
pub const DEFAULT_LIST_FILES_CACHE_TTL: Option<Duration> = None; // Infinite

/// Handles the inner state of the [`DefaultListFilesCache`] struct.
pub struct DefaultListFilesCacheState {
    lru_queue: LruQueue<Path, ListFilesEntry>,
    memory_limit: usize,
    memory_used: usize,
    ttl: Option<Duration>,
}

impl Default for DefaultListFilesCacheState {
    fn default() -> Self {
        Self {
            lru_queue: LruQueue::new(),
            memory_limit: DEFAULT_LIST_FILES_CACHE_MEMORY_LIMIT,
            memory_used: 0,
            ttl: DEFAULT_LIST_FILES_CACHE_TTL,
        }
    }
}

impl DefaultListFilesCacheState {
    fn new(memory_limit: usize, ttl: Option<Duration>) -> Self {
        Self {
            lru_queue: LruQueue::new(),
            memory_limit,
            memory_used: 0,
            ttl,
        }
    }

    /// Performs a prefix-aware cache lookup.
    ///
    /// # Arguments
    /// * `table_base` - The table's base path (the cache key)
    /// * `prefix` - Optional prefix filter relative to the table base path
    /// * `now` - Current time for expiration checking
    ///
    /// # Behavior
    /// - Fetches the cache entry for `table_base`
    /// - If `prefix` is `Some`, filters results to only files matching `table_base/prefix`
    /// - Returns the (potentially filtered) results
    ///
    /// # Example
    /// ```text
    /// get_with_prefix("my_table", Some("a=1"), now)
    ///   → Fetch cache entry for "my_table"
    ///   → Filter to files matching "my_table/a=1/*"
    ///   → Return filtered results
    /// ```
    fn get_with_prefix(
        &mut self,
        table_base: &Path,
        prefix: Option<&Path>,
        now: Instant,
    ) -> Option<Arc<Vec<ObjectMeta>>> {
        let entry = self.lru_queue.get(table_base)?;

        // Check expiration
        if let Some(exp) = entry.expires
            && now > exp
        {
            self.remove(table_base);
            return None;
        }

        // Early return if no prefix filter - return all files
        let Some(prefix) = prefix else {
            return Some(Arc::clone(&entry.metas));
        };

        // Build the full prefix path: table_base/prefix
        let mut parts: Vec<_> = table_base.parts().collect();
        parts.extend(prefix.parts());
        let full_prefix = Path::from_iter(parts);
        let full_prefix_str = full_prefix.as_ref();

        // Filter files to only those matching the prefix
        let filtered: Vec<ObjectMeta> = entry
            .metas
            .iter()
            .filter(|meta| meta.location.as_ref().starts_with(full_prefix_str))
            .cloned()
            .collect();

        if filtered.is_empty() {
            None
        } else {
            Some(Arc::new(filtered))
        }
    }

    /// Checks if the respective entry is currently cached.
    ///
    /// If the entry has expired by `now` it is removed from the cache.
    ///
    /// The LRU queue is not updated.
    fn contains_key(&mut self, k: &Path, now: Instant) -> bool {
        let Some(entry) = self.lru_queue.peek(k) else {
            return false;
        };

        match entry.expires {
            Some(exp) if now > exp => {
                self.remove(k);
                false
            }
            _ => true,
        }
    }

    /// Adds a new key-value pair to cache expiring at `now` + the TTL.
    ///
    /// This means that LRU entries might be evicted if required.
    /// If the key is already in the cache, the previous entry is returned.
    /// If the size of the entry is greater than the `memory_limit`, the value is not inserted.
    fn put(
        &mut self,
        key: &Path,
        value: Arc<Vec<ObjectMeta>>,
        now: Instant,
    ) -> Option<Arc<Vec<ObjectMeta>>> {
        let entry = ListFilesEntry::try_new(value, self.ttl, now)?;
        let entry_size = entry.size_bytes;

        // no point in trying to add this value to the cache if it cannot fit entirely
        if entry_size > self.memory_limit {
            return None;
        }

        // if the key is already in the cache, the old value is removed
        let old_value = self.lru_queue.put(key.clone(), entry);
        self.memory_used += entry_size;

        if let Some(entry) = &old_value {
            self.memory_used -= entry.size_bytes;
        }

        self.evict_entries();

        old_value.map(|v| v.metas)
    }

    /// Evicts entries from the LRU cache until `memory_used` is lower than `memory_limit`.
    fn evict_entries(&mut self) {
        while self.memory_used > self.memory_limit {
            if let Some(removed) = self.lru_queue.pop() {
                self.memory_used -= removed.1.size_bytes;
            } else {
                // cache is empty while memory_used > memory_limit, cannot happen
                debug_assert!(
                    false,
                    "cache is empty while memory_used > memory_limit, cannot happen"
                );
                return;
            }
        }
    }

    /// Removes an entry from the cache and returns it, if it exists.
    fn remove(&mut self, k: &Path) -> Option<Arc<Vec<ObjectMeta>>> {
        if let Some(entry) = self.lru_queue.remove(k) {
            self.memory_used -= entry.size_bytes;
            Some(entry.metas)
        } else {
            None
        }
    }

    /// Returns the number of entries currently cached.
    fn len(&self) -> usize {
        self.lru_queue.len()
    }

    /// Removes all entries from the cache.
    fn clear(&mut self) {
        self.lru_queue.clear();
        self.memory_used = 0;
    }
}

impl ListFilesCache for DefaultListFilesCache {
    fn cache_limit(&self) -> usize {
        let state = self.state.lock().unwrap();
        state.memory_limit
    }

    fn cache_ttl(&self) -> Option<Duration> {
        let state = self.state.lock().unwrap();
        state.ttl
    }

    fn update_cache_limit(&self, limit: usize) {
        let mut state = self.state.lock().unwrap();
        state.memory_limit = limit;
        state.evict_entries();
    }

    fn update_cache_ttl(&self, ttl: Option<Duration>) {
        let mut state = self.state.lock().unwrap();
        state.ttl = ttl;
        state.evict_entries();
    }

    fn list_entries(&self) -> HashMap<Path, ListFilesEntry> {
        let state = self.state.lock().unwrap();
        let mut entries = HashMap::<Path, ListFilesEntry>::new();
        for (path, entry) in state.lru_queue.list_entries() {
            entries.insert(path.clone(), entry.clone());
        }
        entries
    }
}

impl CacheAccessor<Path, Arc<Vec<ObjectMeta>>> for DefaultListFilesCache {
    type Extra = Option<Path>;

    /// Gets all files for the given table base path.
    ///
    /// This is equivalent to calling `get_with_extra(k, &None)`.
    fn get(&self, k: &Path) -> Option<Arc<Vec<ObjectMeta>>> {
        self.get_with_extra(k, &None)
    }

    /// Performs a prefix-aware cache lookup.
    ///
    /// # Arguments
    /// * `table_base` - The table's base path (the cache key)
    /// * `prefix` - Optional prefix filter (relative to table base) for partition filtering
    ///
    /// # Behavior
    /// - Fetches the cache entry for `table_base`
    /// - If `prefix` is `Some`, filters results to only files matching `table_base/prefix`
    /// - Returns the (potentially filtered) results
    ///
    /// This enables efficient partition pruning - a single cached listing of the full table
    /// can serve queries for any partition subset without additional storage calls.
    fn get_with_extra(
        &self,
        table_base: &Path,
        prefix: &Self::Extra,
    ) -> Option<Arc<Vec<ObjectMeta>>> {
        let mut state = self.state.lock().unwrap();
        let now = self.time_provider.now();
        state.get_with_prefix(table_base, prefix.as_ref(), now)
    }

    fn put(
        &self,
        key: &Path,
        value: Arc<Vec<ObjectMeta>>,
    ) -> Option<Arc<Vec<ObjectMeta>>> {
        let mut state = self.state.lock().unwrap();
        let now = self.time_provider.now();
        state.put(key, value, now)
    }

    fn put_with_extra(
        &self,
        key: &Path,
        value: Arc<Vec<ObjectMeta>>,
        _e: &Self::Extra,
    ) -> Option<Arc<Vec<ObjectMeta>>> {
        self.put(key, value)
    }

    fn remove(&self, k: &Path) -> Option<Arc<Vec<ObjectMeta>>> {
        let mut state = self.state.lock().unwrap();
        state.remove(k)
    }

    fn contains_key(&self, k: &Path) -> bool {
        let mut state = self.state.lock().unwrap();
        let now = self.time_provider.now();
        state.contains_key(k, now)
    }

    fn len(&self) -> usize {
        let state = self.state.lock().unwrap();
        state.len()
    }

    fn clear(&self) {
        let mut state = self.state.lock().unwrap();
        state.clear();
    }

    fn name(&self) -> String {
        String::from("DefaultListFilesCache")
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::DateTime;

    struct MockTimeProvider {
        base: Instant,
        offset: Mutex<Duration>,
    }

    impl MockTimeProvider {
        fn new() -> Self {
            Self {
                base: Instant::now(),
                offset: Mutex::new(Duration::ZERO),
            }
        }

        fn inc(&self, duration: Duration) {
            let mut offset = self.offset.lock().unwrap();
            *offset += duration;
        }
    }

    impl TimeProvider for MockTimeProvider {
        fn now(&self) -> Instant {
            self.base + *self.offset.lock().unwrap()
        }
    }

    /// Helper function to create a test ObjectMeta with a specific path and location string size
    fn create_test_object_meta(path: &str, location_size: usize) -> ObjectMeta {
        // Create a location string of the desired size by padding with zeros
        let location_str = if location_size > path.len() {
            format!("{}{}", path, "0".repeat(location_size - path.len()))
        } else {
            path.to_string()
        };

        ObjectMeta {
            location: Path::from(location_str),
            last_modified: DateTime::parse_from_rfc3339("2022-09-27T22:36:00+02:00")
                .unwrap()
                .into(),
            size: 1024,
            e_tag: None,
            version: None,
        }
    }

    /// Helper function to create a vector of ObjectMeta with at least meta_size bytes
    fn create_test_list_files_entry(
        path: &str,
        count: usize,
        meta_size: usize,
    ) -> (Path, Arc<Vec<ObjectMeta>>, usize) {
        let metas: Vec<ObjectMeta> = (0..count)
            .map(|i| create_test_object_meta(&format!("file{i}"), meta_size))
            .collect();
        let metas = Arc::new(metas);

        // Calculate actual size using the same logic as ListFilesEntry::try_new
        let size = (metas.capacity() * size_of::<ObjectMeta>())
            + metas.iter().map(meta_heap_bytes).sum::<usize>();

        (Path::from(path), metas, size)
    }

    #[test]
    fn test_basic_operations() {
        let cache = DefaultListFilesCache::default();
        let path = Path::from("test_path");

        // Initially cache is empty
        assert!(cache.get(&path).is_none());
        assert!(!cache.contains_key(&path));
        assert_eq!(cache.len(), 0);

        // Put an entry
        let meta = create_test_object_meta("file1", 50);
        let value = Arc::new(vec![meta.clone()]);
        cache.put(&path, Arc::clone(&value));

        // Entry should be retrievable
        assert!(cache.contains_key(&path));
        assert_eq!(cache.len(), 1);
        let retrieved = cache.get(&path).unwrap();
        assert_eq!(retrieved.len(), 1);
        assert_eq!(retrieved[0].location, meta.location);

        // Remove the entry
        let removed = cache.remove(&path).unwrap();
        assert_eq!(removed.len(), 1);
        assert!(!cache.contains_key(&path));
        assert_eq!(cache.len(), 0);

        // Put multiple entries
        let (path1, value1, size1) = create_test_list_files_entry("path1", 2, 50);
        let (path2, value2, size2) = create_test_list_files_entry("path2", 3, 50);
        cache.put(&path1, Arc::clone(&value1));
        cache.put(&path2, Arc::clone(&value2));
        assert_eq!(cache.len(), 2);

        // List cache entries
        assert_eq!(
            cache.list_entries(),
            HashMap::from([
                (
                    path1.clone(),
                    ListFilesEntry {
                        metas: value1,
                        size_bytes: size1,
                        expires: None,
                    }
                ),
                (
                    path2.clone(),
                    ListFilesEntry {
                        metas: value2,
                        size_bytes: size2,
                        expires: None,
                    }
                )
            ])
        );

        // Clear all entries
        cache.clear();
        assert_eq!(cache.len(), 0);
        assert!(!cache.contains_key(&path1));
        assert!(!cache.contains_key(&path2));
    }

    #[test]
    fn test_lru_eviction_basic() {
        let (path1, value1, size) = create_test_list_files_entry("path1", 1, 100);
        let (path2, value2, _) = create_test_list_files_entry("path2", 1, 100);
        let (path3, value3, _) = create_test_list_files_entry("path3", 1, 100);

        // Set cache limit to exactly fit all three entries
        let cache = DefaultListFilesCache::new(size * 3, None);

        // All three entries should fit
        cache.put(&path1, value1);
        cache.put(&path2, value2);
        cache.put(&path3, value3);
        assert_eq!(cache.len(), 3);
        assert!(cache.contains_key(&path1));
        assert!(cache.contains_key(&path2));
        assert!(cache.contains_key(&path3));

        // Adding a new entry should evict path1 (LRU)
        let (path4, value4, _) = create_test_list_files_entry("path4", 1, 100);
        cache.put(&path4, value4);

        assert_eq!(cache.len(), 3);
        assert!(!cache.contains_key(&path1)); // Evicted
        assert!(cache.contains_key(&path2));
        assert!(cache.contains_key(&path3));
        assert!(cache.contains_key(&path4));
    }

    #[test]
    fn test_lru_ordering_after_access() {
        let (path1, value1, size) = create_test_list_files_entry("path1", 1, 100);
        let (path2, value2, _) = create_test_list_files_entry("path2", 1, 100);
        let (path3, value3, _) = create_test_list_files_entry("path3", 1, 100);

        // Set cache limit to fit exactly three entries
        let cache = DefaultListFilesCache::new(size * 3, None);

        cache.put(&path1, value1);
        cache.put(&path2, value2);
        cache.put(&path3, value3);
        assert_eq!(cache.len(), 3);

        // Access path1 to move it to front (MRU)
        // Order is now: path2 (LRU), path3, path1 (MRU)
        cache.get(&path1);

        // Adding a new entry should evict path2 (the LRU)
        let (path4, value4, _) = create_test_list_files_entry("path4", 1, 100);
        cache.put(&path4, value4);

        assert_eq!(cache.len(), 3);
        assert!(cache.contains_key(&path1)); // Still present (recently accessed)
        assert!(!cache.contains_key(&path2)); // Evicted (was LRU)
        assert!(cache.contains_key(&path3));
        assert!(cache.contains_key(&path4));
    }

    #[test]
    fn test_reject_too_large() {
        let (path1, value1, size) = create_test_list_files_entry("path1", 1, 100);
        let (path2, value2, _) = create_test_list_files_entry("path2", 1, 100);

        // Set cache limit to fit both entries
        let cache = DefaultListFilesCache::new(size * 2, None);

        cache.put(&path1, value1);
        cache.put(&path2, value2);
        assert_eq!(cache.len(), 2);

        // Try to add an entry that's too large to fit in the cache
        let (path_large, value_large, _) = create_test_list_files_entry("large", 1, 1000);
        cache.put(&path_large, value_large);

        // Large entry should not be added
        assert!(!cache.contains_key(&path_large));
        assert_eq!(cache.len(), 2);
        assert!(cache.contains_key(&path1));
        assert!(cache.contains_key(&path2));
    }

    #[test]
    fn test_multiple_evictions() {
        let (path1, value1, size) = create_test_list_files_entry("path1", 1, 100);
        let (path2, value2, _) = create_test_list_files_entry("path2", 1, 100);
        let (path3, value3, _) = create_test_list_files_entry("path3", 1, 100);

        // Set cache limit for exactly 3 entries
        let cache = DefaultListFilesCache::new(size * 3, None);

        cache.put(&path1, value1);
        cache.put(&path2, value2);
        cache.put(&path3, value3);
        assert_eq!(cache.len(), 3);

        // Add a large entry that requires evicting 2 entries
        let (path_large, value_large, _) = create_test_list_files_entry("large", 1, 200);
        cache.put(&path_large, value_large);

        // path1 and path2 should be evicted (both LRU), path3 and path_large remain
        assert_eq!(cache.len(), 2);
        assert!(!cache.contains_key(&path1)); // Evicted
        assert!(!cache.contains_key(&path2)); // Evicted
        assert!(cache.contains_key(&path3));
        assert!(cache.contains_key(&path_large));
    }

    #[test]
    fn test_cache_limit_resize() {
        let (path1, value1, size) = create_test_list_files_entry("path1", 1, 100);
        let (path2, value2, _) = create_test_list_files_entry("path2", 1, 100);
        let (path3, value3, _) = create_test_list_files_entry("path3", 1, 100);

        let cache = DefaultListFilesCache::new(size * 3, None);

        // Add three entries
        cache.put(&path1, value1);
        cache.put(&path2, value2);
        cache.put(&path3, value3);
        assert_eq!(cache.len(), 3);

        // Resize cache to only fit one entry
        cache.update_cache_limit(size);

        // Should keep only the most recent entry (path3, the MRU)
        assert_eq!(cache.len(), 1);
        assert!(cache.contains_key(&path3));
        // Earlier entries (LRU) should be evicted
        assert!(!cache.contains_key(&path1));
        assert!(!cache.contains_key(&path2));
    }

    #[test]
    fn test_entry_update_with_size_change() {
        let (path1, value1, size) = create_test_list_files_entry("path1", 1, 100);
        let (path2, value2, size2) = create_test_list_files_entry("path2", 1, 100);
        let (path3, value3_v1, _) = create_test_list_files_entry("path3", 1, 100);

        let cache = DefaultListFilesCache::new(size * 3, None);

        // Add three entries
        cache.put(&path1, value1);
        cache.put(&path2, Arc::clone(&value2));
        cache.put(&path3, value3_v1);
        assert_eq!(cache.len(), 3);

        // Update path3 with same size - should not cause eviction
        let (_, value3_v2, _) = create_test_list_files_entry("path3", 1, 100);
        cache.put(&path3, value3_v2);

        assert_eq!(cache.len(), 3);
        assert!(cache.contains_key(&path1));
        assert!(cache.contains_key(&path2));
        assert!(cache.contains_key(&path3));

        // Update path3 with larger size that requires evicting path1 (LRU)
        let (_, value3_v3, size3_v3) = create_test_list_files_entry("path3", 1, 200);
        cache.put(&path3, Arc::clone(&value3_v3));

        assert_eq!(cache.len(), 2);
        assert!(!cache.contains_key(&path1));

        // List cache entries
        assert_eq!(
            cache.list_entries(),
            HashMap::from([
                (
                    path2,
                    ListFilesEntry {
                        metas: value2,
                        size_bytes: size2,
                        expires: None,
                    }
                ),
                (
                    path3,
                    ListFilesEntry {
                        metas: value3_v3,
                        size_bytes: size3_v3,
                        expires: None,
                    }
                )
            ])
        );
    }

    #[test]
    fn test_cache_with_ttl() {
        let ttl = Duration::from_millis(100);

        let mock_time = Arc::new(MockTimeProvider::new());
        let cache = DefaultListFilesCache::new(10000, Some(ttl))
            .with_time_provider(Arc::clone(&mock_time) as Arc<dyn TimeProvider>);

        let (path1, value1, size1) = create_test_list_files_entry("path1", 2, 50);
        let (path2, value2, size2) = create_test_list_files_entry("path2", 2, 50);

        cache.put(&path1, Arc::clone(&value1));
        cache.put(&path2, Arc::clone(&value2));

        // Entries should be accessible immediately
        assert!(cache.get(&path1).is_some());
        assert!(cache.get(&path2).is_some());
        // List cache entries
        assert_eq!(
            cache.list_entries(),
            HashMap::from([
                (
                    path1.clone(),
                    ListFilesEntry {
                        metas: value1,
                        size_bytes: size1,
                        expires: mock_time.now().checked_add(ttl),
                    }
                ),
                (
                    path2.clone(),
                    ListFilesEntry {
                        metas: value2,
                        size_bytes: size2,
                        expires: mock_time.now().checked_add(ttl),
                    }
                )
            ])
        );
        // Wait for TTL to expire
        mock_time.inc(Duration::from_millis(150));

        // Entries should now return None and be removed when observed through get or contains_key
        assert!(cache.get(&path1).is_none());
        assert_eq!(cache.len(), 1); // path1 was removed by get()
        assert!(!cache.contains_key(&path2));
        assert_eq!(cache.len(), 0); // path2 was removed by contains_key()
    }

    #[test]
    fn test_cache_with_ttl_and_lru() {
        let ttl = Duration::from_millis(200);

        let mock_time = Arc::new(MockTimeProvider::new());
        let cache = DefaultListFilesCache::new(1000, Some(ttl))
            .with_time_provider(Arc::clone(&mock_time) as Arc<dyn TimeProvider>);

        let (path1, value1, _) = create_test_list_files_entry("path1", 1, 400);
        let (path2, value2, _) = create_test_list_files_entry("path2", 1, 400);
        let (path3, value3, _) = create_test_list_files_entry("path3", 1, 400);

        cache.put(&path1, value1);
        mock_time.inc(Duration::from_millis(50));
        cache.put(&path2, value2);
        mock_time.inc(Duration::from_millis(50));

        // path3 should evict path1 due to size limit
        cache.put(&path3, value3);
        assert!(!cache.contains_key(&path1)); // Evicted by LRU
        assert!(cache.contains_key(&path2));
        assert!(cache.contains_key(&path3));

        mock_time.inc(Duration::from_millis(151));

        assert!(!cache.contains_key(&path2)); // Expired
        assert!(cache.contains_key(&path3)); // Still valid 
    }

    #[test]
    fn test_meta_heap_bytes_calculation() {
        // Test with minimal ObjectMeta (no e_tag, no version)
        let meta1 = ObjectMeta {
            location: Path::from("test"),
            last_modified: chrono::Utc::now(),
            size: 100,
            e_tag: None,
            version: None,
        };
        assert_eq!(meta_heap_bytes(&meta1), 4); // Just the location string "test"

        // Test with e_tag
        let meta2 = ObjectMeta {
            location: Path::from("test"),
            last_modified: chrono::Utc::now(),
            size: 100,
            e_tag: Some("etag123".to_string()),
            version: None,
        };
        assert_eq!(meta_heap_bytes(&meta2), 4 + 7); // location (4) + e_tag (7)

        // Test with version
        let meta3 = ObjectMeta {
            location: Path::from("test"),
            last_modified: chrono::Utc::now(),
            size: 100,
            e_tag: None,
            version: Some("v1.0".to_string()),
        };
        assert_eq!(meta_heap_bytes(&meta3), 4 + 4); // location (4) + version (4)

        // Test with both e_tag and version
        let meta4 = ObjectMeta {
            location: Path::from("test"),
            last_modified: chrono::Utc::now(),
            size: 100,
            e_tag: Some("tag".to_string()),
            version: Some("ver".to_string()),
        };
        assert_eq!(meta_heap_bytes(&meta4), 4 + 3 + 3); // location (4) + e_tag (3) + version (3)
    }

    #[test]
    fn test_entry_creation() {
        // Test with empty vector
        let empty_vec: Arc<Vec<ObjectMeta>> = Arc::new(vec![]);
        let now = Instant::now();
        let entry = ListFilesEntry::try_new(empty_vec, None, now);
        assert!(entry.is_none());

        // Validate entry size
        let metas: Vec<ObjectMeta> = (0..5)
            .map(|i| create_test_object_meta(&format!("file{i}"), 30))
            .collect();
        let metas = Arc::new(metas);
        let entry = ListFilesEntry::try_new(metas, None, now).unwrap();
        assert_eq!(entry.metas.len(), 5);
        // Size should be: capacity * sizeof(ObjectMeta) + (5 * 30) for heap bytes
        let expected_size =
            (entry.metas.capacity() * size_of::<ObjectMeta>()) + (entry.metas.len() * 30);
        assert_eq!(entry.size_bytes, expected_size);

        // Test with TTL
        let meta = create_test_object_meta("file", 50);
        let ttl = Duration::from_secs(10);
        let entry =
            ListFilesEntry::try_new(Arc::new(vec![meta]), Some(ttl), now).unwrap();
        assert!(entry.expires.unwrap() > now);
    }

    #[test]
    fn test_memory_tracking() {
        let cache = DefaultListFilesCache::new(1000, None);

        // Verify cache starts with 0 memory used
        {
            let state = cache.state.lock().unwrap();
            assert_eq!(state.memory_used, 0);
        }

        // Add entry and verify memory tracking
        let (path1, value1, size1) = create_test_list_files_entry("path1", 1, 100);
        cache.put(&path1, value1);
        {
            let state = cache.state.lock().unwrap();
            assert_eq!(state.memory_used, size1);
        }

        // Add another entry
        let (path2, value2, size2) = create_test_list_files_entry("path2", 1, 200);
        cache.put(&path2, value2);
        {
            let state = cache.state.lock().unwrap();
            assert_eq!(state.memory_used, size1 + size2);
        }

        // Remove first entry and verify memory decreases
        cache.remove(&path1);
        {
            let state = cache.state.lock().unwrap();
            assert_eq!(state.memory_used, size2);
        }

        // Clear and verify memory is 0
        cache.clear();
        {
            let state = cache.state.lock().unwrap();
            assert_eq!(state.memory_used, 0);
        }
    }

    // Prefix-aware cache tests

    /// Helper function to create ObjectMeta with a specific location path
    fn create_object_meta_with_path(location: &str) -> ObjectMeta {
        ObjectMeta {
            location: Path::from(location),
            last_modified: DateTime::parse_from_rfc3339("2022-09-27T22:36:00+02:00")
                .unwrap()
                .into(),
            size: 1024,
            e_tag: None,
            version: None,
        }
    }

    #[test]
    fn test_prefix_aware_cache_hit() {
        // Scenario: Cache has full table listing, query for partition returns filtered results
        let cache = DefaultListFilesCache::new(100000, None);

        // Create files for a partitioned table
        let table_base = Path::from("my_table");
        let files = Arc::new(vec![
            create_object_meta_with_path("my_table/a=1/file1.parquet"),
            create_object_meta_with_path("my_table/a=1/file2.parquet"),
            create_object_meta_with_path("my_table/a=2/file3.parquet"),
            create_object_meta_with_path("my_table/a=2/file4.parquet"),
        ]);

        // Cache the full table listing
        cache.put(&table_base, files);

        // Query for partition a=1 using get_with_extra
        // New API: get_with_extra(table_base, Some(relative_prefix))
        let prefix_a1 = Some(Path::from("a=1"));
        let result = cache.get_with_extra(&table_base, &prefix_a1);

        // Should return filtered results (only files from a=1)
        assert!(result.is_some());
        let filtered = result.unwrap();
        assert_eq!(filtered.len(), 2);
        assert!(
            filtered
                .iter()
                .all(|m| m.location.as_ref().starts_with("my_table/a=1"))
        );

        // Query for partition a=2
        let prefix_a2 = Some(Path::from("a=2"));
        let result_2 = cache.get_with_extra(&table_base, &prefix_a2);

        assert!(result_2.is_some());
        let filtered_2 = result_2.unwrap();
        assert_eq!(filtered_2.len(), 2);
        assert!(
            filtered_2
                .iter()
                .all(|m| m.location.as_ref().starts_with("my_table/a=2"))
        );
    }

    #[test]
    fn test_prefix_aware_cache_no_filter_returns_all() {
        // Scenario: Query with no prefix filter should return all files
        let cache = DefaultListFilesCache::new(100000, None);

        let table_base = Path::from("my_table");

        // Cache full table listing with 4 files
        let full_files = Arc::new(vec![
            create_object_meta_with_path("my_table/a=1/file1.parquet"),
            create_object_meta_with_path("my_table/a=1/file2.parquet"),
            create_object_meta_with_path("my_table/a=2/file3.parquet"),
            create_object_meta_with_path("my_table/a=2/file4.parquet"),
        ]);
        cache.put(&table_base, full_files);

        // Query with no prefix filter (None) should return all 4 files
        let result = cache.get_with_extra(&table_base, &None);
        assert!(result.is_some());
        let files = result.unwrap();
        assert_eq!(files.len(), 4);

        // Also test using get() which delegates to get_with_extra(&None)
        let result_get = cache.get(&table_base);
        assert!(result_get.is_some());
        assert_eq!(result_get.unwrap().len(), 4);
    }

    #[test]
    fn test_prefix_aware_cache_miss_no_entry() {
        // Scenario: Table not cached, query should miss
        let cache = DefaultListFilesCache::new(100000, None);

        let table_base = Path::from("my_table");

        // Query for full table should miss (nothing cached)
        let result = cache.get_with_extra(&table_base, &None);
        assert!(result.is_none());

        // Query with prefix should also miss
        let prefix = Some(Path::from("a=1"));
        let result_2 = cache.get_with_extra(&table_base, &prefix);
        assert!(result_2.is_none());
    }

    #[test]
    fn test_prefix_aware_cache_no_matching_files() {
        // Scenario: Cache has table listing but no files match the requested partition
        let cache = DefaultListFilesCache::new(100000, None);

        let table_base = Path::from("my_table");
        let files = Arc::new(vec![
            create_object_meta_with_path("my_table/a=1/file1.parquet"),
            create_object_meta_with_path("my_table/a=2/file2.parquet"),
        ]);
        cache.put(&table_base, files);

        // Query for partition a=3 which doesn't exist
        let prefix_a3 = Some(Path::from("a=3"));
        let result = cache.get_with_extra(&table_base, &prefix_a3);

        // Should return None since no files match
        assert!(result.is_none());
    }

    #[test]
    fn test_prefix_aware_nested_partitions() {
        // Scenario: Table with multiple partition levels (e.g., year/month/day)
        let cache = DefaultListFilesCache::new(100000, None);

        let table_base = Path::from("events");
        let files = Arc::new(vec![
            create_object_meta_with_path(
                "events/year=2024/month=01/day=01/file1.parquet",
            ),
            create_object_meta_with_path(
                "events/year=2024/month=01/day=02/file2.parquet",
            ),
            create_object_meta_with_path(
                "events/year=2024/month=02/day=01/file3.parquet",
            ),
            create_object_meta_with_path(
                "events/year=2025/month=01/day=01/file4.parquet",
            ),
        ]);
        cache.put(&table_base, files);

        // Query for year=2024/month=01 (should get 2 files)
        let prefix_month = Some(Path::from("year=2024/month=01"));
        let result = cache.get_with_extra(&table_base, &prefix_month);
        assert!(result.is_some());
        assert_eq!(result.unwrap().len(), 2);

        // Query for year=2024 (should get 3 files)
        let prefix_year = Some(Path::from("year=2024"));
        let result_year = cache.get_with_extra(&table_base, &prefix_year);
        assert!(result_year.is_some());
        assert_eq!(result_year.unwrap().len(), 3);

        // Query for specific day (should get 1 file)
        let prefix_day = Some(Path::from("year=2024/month=01/day=01"));
        let result_day = cache.get_with_extra(&table_base, &prefix_day);
        assert!(result_day.is_some());
        assert_eq!(result_day.unwrap().len(), 1);
    }

    #[test]
    fn test_prefix_aware_different_tables() {
        // Scenario: Multiple tables cached, queries should not cross-contaminate
        let cache = DefaultListFilesCache::new(100000, None);

        let table_a = Path::from("table_a");
        let table_b = Path::from("table_b");

        let files_a = Arc::new(vec![create_object_meta_with_path(
            "table_a/part=1/file1.parquet",
        )]);
        let files_b = Arc::new(vec![
            create_object_meta_with_path("table_b/part=1/file1.parquet"),
            create_object_meta_with_path("table_b/part=2/file2.parquet"),
        ]);

        cache.put(&table_a, files_a);
        cache.put(&table_b, files_b);

        // Query table_a should only return table_a files
        let result_a = cache.get(&table_a);
        assert!(result_a.is_some());
        assert_eq!(result_a.unwrap().len(), 1);

        // Query table_b with prefix should only return matching table_b files
        let prefix = Some(Path::from("part=1"));
        let result_b = cache.get_with_extra(&table_b, &prefix);
        assert!(result_b.is_some());
        assert_eq!(result_b.unwrap().len(), 1);
    }
}
