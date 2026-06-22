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
            return self.remove(key);
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
/// insertion time and checked lazily on access. Entries with size 0 are rejected.
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

    fn drop_table_entries(&self, table_ref: &TableReference) -> Result<()> {
        let mut state = self.state.lock().unwrap();
        let to_remove: Vec<K> = state
            .lru_queue
            .keys()
            .filter(|k| k.table_ref() == Some(table_ref))
            .cloned()
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

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use crate::cache::TableScopedPath;
    use crate::cache::cache_manager::{
        CachedFileList, DEFAULT_LIST_FILES_CACHE_MEMORY_LIMIT, meta_heap_bytes,
    };
    use crate::cache::cache_manager::{
        CachedFileMetadata, DEFAULT_FILE_STATISTICS_MEMORY_LIMIT,
    };
    use crate::cache::cache_manager::{CachedFileMetadataEntry, FileMetadata};
    use crate::cache::default_cache::DefaultCache;
    use crate::cache::default_cache::TimeProvider;
    use crate::cache::{Cache, CacheEntryInfo};
    use crate::cache::{CacheKey, CacheValue};
    use arrow::array::{Int32Array, ListArray, RecordBatch};
    use arrow::buffer::{OffsetBuffer, ScalarBuffer};
    use arrow::datatypes::{DataType, Field, Schema, TimeUnit};
    use chrono::DateTime;
    use datafusion_common::HashMap;
    use datafusion_common::TableReference;
    use datafusion_common::heap_size::{DFHeapSize, DFHeapSizeCtx};
    use datafusion_common::instant::Instant;
    use datafusion_common::stats::Precision;
    use datafusion_common::{ColumnStatistics, ScalarValue, Statistics};
    use datafusion_expr::ColumnarValue;
    use datafusion_physical_expr_common::physical_expr::PhysicalExpr;
    use datafusion_physical_expr_common::sort_expr::{LexOrdering, PhysicalSortExpr};
    use object_store::ObjectMeta;
    use object_store::path::Path;
    use std::sync::Mutex;
    use std::thread;
    use std::time::Duration;

    pub struct TestFileMetadata {
        metadata: String,
    }

    impl FileMetadata for TestFileMetadata {
        fn as_any(&self) -> &dyn std::any::Any {
            self
        }

        fn memory_size(&self) -> usize {
            self.metadata.len()
        }

        fn extra_info(&self) -> HashMap<String, String> {
            HashMap::from([("extra_info".to_owned(), "abc".to_owned())])
        }
    }

    impl PartialEq for CachedFileMetadataEntry {
        fn eq(&self, other: &Self) -> bool {
            self.meta == other.meta
        }
    }

    fn create_test_object_meta(path: &str, size: usize) -> ObjectMeta {
        ObjectMeta {
            location: Path::from(path),
            last_modified: DateTime::parse_from_rfc3339("2025-07-29T12:12:12+00:00")
                .unwrap()
                .into(),
            size: size as u64,
            e_tag: None,
            version: None,
        }
    }

    #[test]
    fn test_default_file_metadata_cache() {
        let object_meta = create_test_object_meta("test", 1024);

        let metadata: Arc<dyn FileMetadata> = Arc::new(TestFileMetadata {
            metadata: "retrieved_metadata".to_owned(),
        });

        let cache = DefaultCache::new(1024 * 1024);

        // Cache miss
        assert!(cache.get(&object_meta.location).is_none());

        // Put a value
        let cached_entry =
            CachedFileMetadataEntry::new(object_meta.clone(), Arc::clone(&metadata));
        cache.put(&object_meta.location, cached_entry);

        // Verify the cached value
        assert!(cache.contains_key(&object_meta.location));
        let result = cache.get(&object_meta.location).unwrap();
        let test_file_metadata = Arc::downcast::<TestFileMetadata>(result.file_metadata);
        assert!(test_file_metadata.is_ok());
        assert_eq!(test_file_metadata.unwrap().metadata, "retrieved_metadata");

        // Cache hit - check validation
        let result2 = cache.get(&object_meta.location).unwrap();
        assert!(result2.is_valid_for(&object_meta));

        // File size changed - closure should detect invalidity
        let object_meta2 = create_test_object_meta("test", 2048);
        let result3 = cache.get(&object_meta2.location).unwrap();
        // Cached entry should NOT be valid for new meta
        assert!(!result3.is_valid_for(&object_meta2));

        // Return new entry
        let new_entry =
            CachedFileMetadataEntry::new(object_meta2.clone(), Arc::clone(&metadata));
        cache.put(&object_meta2.location, new_entry);

        let result4 = cache.get(&object_meta2.location).unwrap();
        assert_eq!(result4.meta.size, 2048);

        // remove
        cache.remove(&object_meta.location);
        assert!(!cache.contains_key(&object_meta.location));

        // len and clear
        let object_meta3 = create_test_object_meta("test3", 100);
        cache.put(
            &object_meta.location,
            CachedFileMetadataEntry::new(object_meta.clone(), Arc::clone(&metadata)),
        );
        cache.put(
            &object_meta3.location,
            CachedFileMetadataEntry::new(object_meta3.clone(), Arc::clone(&metadata)),
        );
        assert_eq!(cache.len(), 2);
        cache.clear();
        assert_eq!(cache.len(), 0);
    }

    fn generate_test_metadata_with_size(
        path: &str,
        size: usize,
    ) -> (ObjectMeta, Arc<dyn FileMetadata>) {
        let object_meta = ObjectMeta {
            location: Path::from(path),
            last_modified: chrono::Utc::now(),
            size: size as u64,
            e_tag: None,
            version: None,
        };
        let metadata = "a".repeat(size);
        let metadata: Arc<dyn FileMetadata> = Arc::new(TestFileMetadata { metadata });

        (object_meta, metadata)
    }

    #[test]
    fn test_default_file_metadata_cache_with_limit() {
        // Create a cache with 1000 bytes capacity + 4 keys each key 2 bytes
        let cache = DefaultCache::new(1000 + 4 * 2);

        let (object_meta1, metadata1) = generate_test_metadata_with_size("01", 100);
        let (object_meta2, metadata2) = generate_test_metadata_with_size("02", 500);
        let (object_meta3, metadata3) = generate_test_metadata_with_size("03", 300);

        cache.put(
            &object_meta1.location,
            CachedFileMetadataEntry::new(object_meta1.clone(), metadata1),
        );
        cache.put(
            &object_meta2.location,
            CachedFileMetadataEntry::new(object_meta2.clone(), metadata2),
        );
        cache.put(
            &object_meta3.location,
            CachedFileMetadataEntry::new(object_meta3.clone(), metadata3),
        );

        // all entries will fit
        assert_eq!(cache.len(), 3);
        assert_eq!(cache.memory_used(), 906);
        assert!(cache.contains_key(&object_meta1.location));
        assert!(cache.contains_key(&object_meta2.location));
        assert!(cache.contains_key(&object_meta3.location));

        // add a new entry which will remove the least recently used ("1")
        let (object_meta4, metadata4) = generate_test_metadata_with_size("04", 200);
        cache.put(
            &object_meta4.location,
            CachedFileMetadataEntry::new(object_meta4.clone(), metadata4),
        );
        assert_eq!(cache.len(), 3);
        assert_eq!(cache.memory_used(), 1006);
        assert!(!cache.contains_key(&object_meta1.location));
        assert!(cache.contains_key(&object_meta4.location));

        // get entry "2", which will move it to the top of the queue, and add a new one which will
        // remove the new least recently used ("3")
        let _ = cache.get(&object_meta2.location);
        let (object_meta5, metadata5) = generate_test_metadata_with_size("05", 100);
        cache.put(
            &object_meta5.location,
            CachedFileMetadataEntry::new(object_meta5.clone(), metadata5),
        );
        assert_eq!(cache.len(), 3);
        assert_eq!(cache.memory_used(), 806);
        assert!(!cache.contains_key(&object_meta3.location));
        assert!(cache.contains_key(&object_meta5.location));

        // new entry which will not be able to fit in the 1000 bytes allocated
        let (object_meta6, metadata6) = generate_test_metadata_with_size("06", 1200);
        cache.put(
            &object_meta6.location,
            CachedFileMetadataEntry::new(object_meta6.clone(), metadata6),
        );
        assert_eq!(cache.len(), 3);
        assert_eq!(cache.memory_used(), 806);
        assert!(!cache.contains_key(&object_meta6.location));

        // new entry which is able to fit without removing any entry
        let (object_meta7, metadata7) = generate_test_metadata_with_size("07", 200);
        cache.put(
            &object_meta7.location,
            CachedFileMetadataEntry::new(object_meta7.clone(), metadata7),
        );
        assert_eq!(cache.len(), 4);
        assert_eq!(cache.memory_used(), 1008);
        assert!(cache.contains_key(&object_meta7.location));

        // new entry which will remove all other entries
        let (object_meta8, metadata8) = generate_test_metadata_with_size("08", 999);
        cache.put(
            &object_meta8.location,
            CachedFileMetadataEntry::new(object_meta8.clone(), metadata8),
        );
        assert_eq!(cache.len(), 1);
        assert_eq!(cache.memory_used(), 1001);
        assert!(cache.contains_key(&object_meta8.location));

        // when updating an entry, the previous ones are not unnecessarily removed
        let (object_meta9, metadata9) = generate_test_metadata_with_size("09", 300);
        let (object_meta10, metadata10) = generate_test_metadata_with_size("10", 200);
        let (object_meta11_v1, metadata11_v1) =
            generate_test_metadata_with_size("11", 400);
        cache.put(
            &object_meta9.location,
            CachedFileMetadataEntry::new(object_meta9.clone(), metadata9),
        );
        cache.put(
            &object_meta10.location,
            CachedFileMetadataEntry::new(object_meta10.clone(), metadata10),
        );
        cache.put(
            &object_meta11_v1.location,
            CachedFileMetadataEntry::new(object_meta11_v1.clone(), metadata11_v1),
        );
        assert_eq!(cache.memory_used(), 906);
        assert_eq!(cache.len(), 3);
        let (object_meta11_v2, metadata11_v2) =
            generate_test_metadata_with_size("11", 500);
        cache.put(
            &object_meta11_v2.location,
            CachedFileMetadataEntry::new(object_meta11_v2.clone(), metadata11_v2),
        );
        assert_eq!(cache.memory_used(), 1006);
        assert_eq!(cache.len(), 3);
        assert!(cache.contains_key(&object_meta9.location));
        assert!(cache.contains_key(&object_meta10.location));
        assert!(cache.contains_key(&object_meta11_v2.location));

        // when updating an entry that now exceeds the limit, the LRU ("09") needs to be removed
        let (object_meta11_v3, metadata11_v3) =
            generate_test_metadata_with_size("11", 510);
        cache.put(
            &object_meta11_v3.location,
            CachedFileMetadataEntry::new(object_meta11_v3.clone(), metadata11_v3),
        );
        assert_eq!(cache.memory_used(), 714);
        assert_eq!(cache.len(), 2);
        assert!(cache.contains_key(&object_meta10.location));
        assert!(cache.contains_key(&object_meta11_v3.location));

        // manually removing an entry that is not the LRU
        cache.remove(&object_meta11_v3.location);
        assert_eq!(cache.len(), 1);
        assert_eq!(cache.memory_used(), 202);
        assert!(cache.contains_key(&object_meta10.location));
        assert!(!cache.contains_key(&object_meta11_v3.location));

        // clear
        cache.clear();
        assert_eq!(cache.len(), 0);
        assert_eq!(cache.memory_used(), 0);

        // resizing the cache should clear the extra entries
        let (object_meta12, metadata12) = generate_test_metadata_with_size("12", 300);
        let (object_meta13, metadata13) = generate_test_metadata_with_size("13", 200);
        let (object_meta14, metadata14) = generate_test_metadata_with_size("14", 500);
        cache.put(
            &object_meta12.location,
            CachedFileMetadataEntry::new(object_meta12.clone(), metadata12),
        );
        cache.put(
            &object_meta13.location,
            CachedFileMetadataEntry::new(object_meta13.clone(), metadata13),
        );
        cache.put(
            &object_meta14.location,
            CachedFileMetadataEntry::new(object_meta14.clone(), metadata14),
        );
        assert_eq!(cache.len(), 3);
        assert_eq!(cache.memory_used(), 1006);
        cache.update_cache_limit(600);
        assert_eq!(cache.len(), 1);
        assert_eq!(cache.memory_used(), 502);
        assert!(!cache.contains_key(&object_meta12.location));
        assert!(!cache.contains_key(&object_meta13.location));
        assert!(cache.contains_key(&object_meta14.location));
    }

    #[test]
    fn test_default_file_metadata_cache_entries_info() {
        // Create a cache with 1000 bytes + 4 bytes for 4 keys each key 1 byte
        let cache = DefaultCache::new(1000 + 4);

        let (object_meta1, metadata1) = generate_test_metadata_with_size("1", 100);
        let (object_meta2, metadata2) = generate_test_metadata_with_size("2", 200);
        let (object_meta3, metadata3) = generate_test_metadata_with_size("3", 300);

        // initial entries, all will have hits = 0
        let entry_1 = CachedFileMetadataEntry::new(object_meta1.clone(), metadata1);
        let entry_2 = CachedFileMetadataEntry::new(object_meta2.clone(), metadata2);
        let entry_3 = CachedFileMetadataEntry::new(object_meta3.clone(), metadata3);

        // Build a cache which fits exactly these 3 entries

        cache.put(&object_meta1.location, entry_1.clone());
        cache.put(&object_meta2.location, entry_2.clone());
        cache.put(&object_meta3.location, entry_3.clone());
        let entries = cache.list_entries();

        assert_eq!(
            entries,
            HashMap::from([
                (
                    Path::from("1"),
                    CacheEntryInfo {
                        value: entry_1.clone(),
                        size_bytes: 100,
                        hits: 0,
                        expires: None,
                    }
                ),
                (
                    Path::from("2"),
                    CacheEntryInfo {
                        value: entry_2.clone(),
                        size_bytes: 200,
                        hits: 0,
                        expires: None,
                    }
                ),
                (
                    Path::from("3"),
                    CacheEntryInfo {
                        value: entry_3.clone(),
                        size_bytes: 300,
                        hits: 0,
                        expires: None,
                    }
                )
            ])
        );

        // new hit on "1"
        let _ = cache.get(&object_meta1.location);
        assert_eq!(
            cache.list_entries(),
            HashMap::from([
                (
                    Path::from("1"),
                    CacheEntryInfo {
                        value: entry_1.clone(),
                        size_bytes: 100,
                        hits: 1,
                        expires: None,
                    }
                ),
                (
                    Path::from("2"),
                    CacheEntryInfo {
                        value: entry_2.clone(),
                        size_bytes: 200,
                        hits: 0,
                        expires: None,
                    }
                ),
                (
                    Path::from("3"),
                    CacheEntryInfo {
                        value: entry_3.clone(),
                        size_bytes: 300,
                        hits: 0,
                        expires: None,
                    }
                )
            ])
        );

        // new entry, will evict "2"
        let (object_meta4, metadata4) = generate_test_metadata_with_size("4", 600);
        let entry_4 = CachedFileMetadataEntry::new(object_meta4.clone(), metadata4);
        cache.put(&object_meta4.location, entry_4.clone());
        assert_eq!(
            cache.list_entries(),
            HashMap::from([
                (
                    Path::from("1"),
                    CacheEntryInfo {
                        value: entry_1.clone(),
                        size_bytes: 100,
                        hits: 1,
                        expires: None,
                    }
                ),
                (
                    Path::from("3"),
                    CacheEntryInfo {
                        value: entry_3.clone(),
                        size_bytes: 300,
                        hits: 0,
                        expires: None,
                    }
                ),
                (
                    Path::from("4"),
                    CacheEntryInfo {
                        value: entry_4.clone(),
                        size_bytes: 600,
                        hits: 0,
                        expires: None,
                    }
                )
            ])
        );

        // replace entry "1"
        let (object_meta1_new, metadata1_new) = generate_test_metadata_with_size("1", 50);
        let entry_1 =
            CachedFileMetadataEntry::new(object_meta1_new.clone(), metadata1_new);
        cache.put(&object_meta1_new.location, entry_1.clone());
        assert_eq!(
            cache.list_entries(),
            HashMap::from([
                (
                    Path::from("1"),
                    CacheEntryInfo {
                        value: entry_1.clone(),
                        size_bytes: 50,
                        hits: 0,
                        expires: None,
                    }
                ),
                (
                    Path::from("3"),
                    CacheEntryInfo {
                        value: entry_3.clone(),
                        size_bytes: 300,
                        hits: 0,
                        expires: None,
                    }
                ),
                (
                    Path::from("4"),
                    CacheEntryInfo {
                        value: entry_4.clone(),
                        size_bytes: 600,
                        hits: 0,
                        expires: None,
                    }
                )
            ])
        );

        // remove entry "4"
        cache.remove(&object_meta4.location);
        assert_eq!(
            cache.list_entries(),
            HashMap::from([
                (
                    Path::from("1"),
                    CacheEntryInfo {
                        value: entry_1.clone(),
                        size_bytes: 50,
                        hits: 0,
                        expires: None,
                    }
                ),
                (
                    Path::from("3"),
                    CacheEntryInfo {
                        value: entry_3.clone(),
                        size_bytes: 300,
                        hits: 0,
                        expires: None,
                    }
                )
            ])
        );

        // clear
        cache.clear();
        assert_eq!(cache.list_entries(), HashMap::from([]));
    }

    fn create_test_meta(path: &str, size: u64) -> ObjectMeta {
        ObjectMeta {
            location: Path::from(path),
            last_modified: DateTime::parse_from_rfc3339("2022-09-27T22:36:00+02:00")
                .unwrap()
                .into(),
            size,
            e_tag: None,
            version: None,
        }
    }

    #[test]
    fn test_statistics_cache() {
        let meta = create_test_meta("test", 1024);
        let cache = DefaultCache::new(DEFAULT_FILE_STATISTICS_MEMORY_LIMIT);

        let schema = Schema::new(vec![Field::new(
            "test_column",
            DataType::Timestamp(TimeUnit::Second, None),
            false,
        )]);

        let path = TableScopedPath {
            path: meta.location.clone(),
            table: None,
        };

        // Cache miss
        assert!(cache.get(&path).is_none());

        // Put a value
        let cached_value = CachedFileMetadata::new(
            meta.clone(),
            Arc::new(Statistics::new_unknown(&schema)),
            None,
        );
        cache.put(&path, cached_value);

        // Cache hit
        let result = cache.get(&path);
        assert!(result.is_some());

        let cached = result.unwrap();
        assert!(cached.is_valid_for(&meta));

        // File size changed - validation should fail
        let meta2 = create_test_meta("test", 2048);

        let path_2 = TableScopedPath {
            path: meta2.location.clone(),
            table: None,
        };

        let cached = cache.get(&path_2).unwrap();
        assert!(!cached.is_valid_for(&meta2));

        // Update with new value
        let cached_value2 = CachedFileMetadata::new(
            meta2.clone(),
            Arc::new(Statistics::new_unknown(&schema)),
            None,
        );
        cache.put(&path_2, cached_value2);

        // Test list_entries
        let entries = cache.list_entries();
        assert_eq!(entries.len(), 1);

        let path_3 = TableScopedPath {
            path: Path::from("test"),
            table: None,
        };

        let entry = entries.get(&path_3).unwrap();
        assert_eq!(entry.value.meta.size, 2048); // Should be updated value
    }

    #[derive(Clone, Debug, PartialEq, Eq, Hash)]
    struct MockExpr {}

    impl std::fmt::Display for MockExpr {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            write!(f, "MockExpr")
        }
    }

    impl PhysicalExpr for MockExpr {
        fn data_type(
            &self,
            _input_schema: &Schema,
        ) -> datafusion_common::Result<DataType> {
            Ok(DataType::Int32)
        }

        fn nullable(&self, _input_schema: &Schema) -> datafusion_common::Result<bool> {
            Ok(false)
        }

        fn evaluate(
            &self,
            _batch: &RecordBatch,
        ) -> datafusion_common::Result<ColumnarValue> {
            unimplemented!()
        }

        fn children(&self) -> Vec<&Arc<dyn PhysicalExpr>> {
            vec![]
        }

        fn with_new_children(
            self: Arc<Self>,
            children: Vec<Arc<dyn PhysicalExpr>>,
        ) -> datafusion_common::Result<Arc<dyn PhysicalExpr>> {
            assert!(children.is_empty());
            Ok(self)
        }

        fn fmt_sql(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            write!(f, "MockExpr")
        }
    }

    fn ordering() -> LexOrdering {
        let expr = Arc::new(MockExpr {}) as Arc<dyn PhysicalExpr>;
        LexOrdering::new(vec![PhysicalSortExpr::new_default(expr)]).unwrap()
    }

    #[test]
    fn test_ordering_cache() {
        let meta = create_test_meta("test.parquet", 100);
        let cache = DefaultCache::new(DEFAULT_FILE_STATISTICS_MEMORY_LIMIT);

        let schema = Schema::new(vec![Field::new("a", DataType::Int32, false)]);

        // Cache statistics with no ordering
        let cached_value = CachedFileMetadata::new(
            meta.clone(),
            Arc::new(Statistics::new_unknown(&schema)),
            None, // No ordering yet
        );

        let path = TableScopedPath {
            path: meta.location.clone(),
            table: None,
        };

        cache.put(&path, cached_value);

        let result = cache.get(&path).unwrap();
        assert!(result.ordering.is_none());

        // Update to add ordering
        let mut cached = cache.get(&path).unwrap();
        if cached.is_valid_for(&meta) && cached.ordering.is_none() {
            cached.ordering = Some(ordering());
        }
        cache.put(&path, cached);

        let result2 = cache.get(&path).unwrap();
        assert!(result2.ordering.is_some());

        // Verify list_entries shows has_ordering = true
        let entries = cache.list_entries();
        assert_eq!(entries.len(), 1);
        assert!(entries.get(&path).unwrap().value.ordering.is_some());
    }

    #[test]
    fn test_cache_invalidation_on_file_modification() {
        let cache = DefaultCache::new(DEFAULT_FILE_STATISTICS_MEMORY_LIMIT);
        let path = TableScopedPath {
            path: Path::from("test.parquet"),
            table: None,
        };
        let schema = Schema::new(vec![Field::new("a", DataType::Int32, false)]);

        let meta_v1 = create_test_meta("test.parquet", 100);

        // Cache initial value
        let cached_value = CachedFileMetadata::new(
            meta_v1.clone(),
            Arc::new(Statistics::new_unknown(&schema)),
            None,
        );
        cache.put(&path, cached_value);

        // File modified (size changed)
        let meta_v2 = create_test_meta("test.parquet", 200);

        let cached = cache.get(&path).unwrap();
        // Should not be valid for new meta
        assert!(!cached.is_valid_for(&meta_v2));

        // Compute new value and update
        let new_cached = CachedFileMetadata::new(
            meta_v2.clone(),
            Arc::new(Statistics::new_unknown(&schema)),
            None,
        );
        cache.put(&path, new_cached);

        // Should have new metadata
        let result = cache.get(&path).unwrap();
        assert_eq!(result.meta.size, 200);
    }

    #[test]
    fn test_ordering_cache_invalidation_on_file_modification() {
        let cache = DefaultCache::new(DEFAULT_FILE_STATISTICS_MEMORY_LIMIT);
        let path = TableScopedPath {
            path: Path::from("test.parquet"),
            table: None,
        };
        let schema = Schema::new(vec![Field::new("a", DataType::Int32, false)]);

        // Cache with original metadata and ordering
        let meta_v1 = ObjectMeta {
            location: path.path.clone(),
            last_modified: DateTime::parse_from_rfc3339("2022-09-27T22:36:00+02:00")
                .unwrap()
                .into(),
            size: 100,
            e_tag: None,
            version: None,
        };
        let ordering_v1 = ordering();
        let cached_v1 = CachedFileMetadata::new(
            meta_v1.clone(),
            Arc::new(Statistics::new_unknown(&schema)),
            Some(ordering_v1),
        );
        cache.put(&path, cached_v1);

        // Verify cached ordering is valid
        let cached = cache.get(&path).unwrap();
        assert!(cached.is_valid_for(&meta_v1));
        assert!(cached.ordering.is_some());

        // File modified (size changed)
        let meta_v2 = ObjectMeta {
            location: path.path.clone(),
            last_modified: DateTime::parse_from_rfc3339("2022-09-28T10:00:00+02:00")
                .unwrap()
                .into(),
            size: 200, // Changed
            e_tag: None,
            version: None,
        };

        // Cache entry exists but should be invalid for new metadata
        let cached = cache.get(&path).unwrap();
        assert!(!cached.is_valid_for(&meta_v2));

        // Cache new version with different ordering
        let ordering_v2 = ordering(); // New ordering instance
        let cached_v2 = CachedFileMetadata::new(
            meta_v2.clone(),
            Arc::new(Statistics::new_unknown(&schema)),
            Some(ordering_v2),
        );
        cache.put(&path, cached_v2);

        // Old metadata should be invalid
        let cached = cache.get(&path).unwrap();
        assert!(!cached.is_valid_for(&meta_v1));

        // New metadata should be valid
        assert!(cached.is_valid_for(&meta_v2));
        assert!(cached.ordering.is_some());
    }

    #[test]
    fn test_list_entries() {
        let cache = DefaultCache::new(DEFAULT_FILE_STATISTICS_MEMORY_LIMIT);
        let schema = Schema::new(vec![Field::new("a", DataType::Int32, false)]);

        let meta1 = create_test_meta("test1.parquet", 100);

        let cached_value_1 = CachedFileMetadata::new(
            meta1.clone(),
            Arc::new(Statistics::new_unknown(&schema)),
            None,
        );

        let path_1 = TableScopedPath {
            path: meta1.location.clone(),
            table: None,
        };

        cache.put(&path_1, cached_value_1.clone());
        let meta2 = create_test_meta("test2.parquet", 200);
        let cached_value_2 = CachedFileMetadata::new(
            meta2.clone(),
            Arc::new(Statistics::new_unknown(&schema)),
            Some(ordering()),
        );

        let path_2 = TableScopedPath {
            path: meta2.location.clone(),
            table: None,
        };

        cache.put(&path_2, cached_value_2.clone());

        let entries = cache.list_entries();
        assert_eq!(
            entries,
            HashMap::from([
                (
                    path_1,
                    CacheEntryInfo {
                        value: cached_value_1,
                        hits: 0,
                        size_bytes: 373,
                        expires: None,
                    }
                ),
                (
                    path_2,
                    CacheEntryInfo {
                        value: cached_value_2,
                        hits: 0,
                        size_bytes: 373,
                        expires: None,
                    }
                ),
            ])
        );
    }

    #[test]
    fn test_cache_entry_added_when_entries_are_within_cache_limit() {
        let (meta_1, value_1) =
            create_cached_file_metadata_with_stats("test1.parquet", 10);
        let (meta_2, value_2) =
            create_cached_file_metadata_with_stats("test2.parquet", 10);
        let (meta_3, value_3) =
            create_cached_file_metadata_with_stats("test3.parquet", 10);

        let mut ctx = DFHeapSizeCtx::default();

        let limit_for_2_entries = meta_1.location.as_ref().heap_size(&mut ctx)
            + value_1.heap_size(&mut ctx)
            + meta_2.location.as_ref().heap_size(&mut ctx)
            + value_2.heap_size(&mut ctx);

        // create a cache with a limit which fits exactly 2 entries
        let cache = DefaultCache::new(limit_for_2_entries);
        let path_1 = TableScopedPath {
            path: meta_1.location.clone(),
            table: None,
        };

        let path_2 = TableScopedPath {
            path: meta_2.location.clone(),
            table: None,
        };

        cache.put(&path_1, value_1.clone());
        cache.put(&path_2, value_2.clone());

        assert_eq!(cache.len(), 2);
        assert_eq!(cache.memory_used(), limit_for_2_entries);

        let result_1 = cache.get(&path_1);
        let result_2 = cache.get(&path_2);
        assert_eq!(result_1.unwrap(), value_1);
        assert_eq!(result_2.unwrap(), value_2);

        let path_3 = TableScopedPath {
            path: meta_3.location.clone(),
            table: None,
        };

        // adding the third entry evicts the first entry
        cache.put(&path_3, value_3.clone());
        assert_eq!(cache.len(), 2);
        assert_eq!(cache.memory_used(), limit_for_2_entries);

        let result_1 = cache.get(&path_1);
        assert!(result_1.is_none());

        let result_2 = cache.get(&path_2);
        let result_3 = cache.get(&path_3);

        assert_eq!(result_2.unwrap(), value_2);
        assert_eq!(result_3.unwrap(), value_3);

        // add the third entry again, making sure memory usage remains the same
        cache.put(&path_3, value_3.clone());
        assert_eq!(cache.memory_used(), limit_for_2_entries);
        cache.put(&path_3, value_3.clone());
        assert_eq!(cache.memory_used(), limit_for_2_entries);

        let mut ctx = DFHeapSizeCtx::default();
        cache.remove(&path_2);
        assert_eq!(cache.len(), 1);
        assert_eq!(
            cache.memory_used(),
            meta_3.location.as_ref().heap_size(&mut ctx) + value_3.heap_size(&mut ctx)
        );

        cache.clear();
        assert_eq!(cache.len(), 0);
        assert_eq!(cache.memory_used(), 0);
    }

    #[test]
    fn test_cache_rejects_entry_which_is_too_large() {
        let (meta, value_too_large) =
            create_cached_file_metadata_with_stats("test1.parquet", 10);
        let mut ctx = DFHeapSizeCtx::default();
        let limit_less_than_the_entry = value_too_large.clone().heap_size(&mut ctx) - 1;

        // create a cache with a size less than the entry
        let cache = DefaultCache::new(limit_less_than_the_entry);

        let path_1 = TableScopedPath {
            path: meta.location.clone(),
            table: None,
        };

        cache.put(&path_1, value_too_large.clone());

        assert_eq!(cache.len(), 0);
        assert_eq!(cache.memory_used(), 0);

        // Test stale entry is removed when oversized entry is added
        let (_, value_fits) = create_cached_file_metadata_with_stats("test1.parquet", 7);
        cache.put(&path_1, value_fits.clone());

        assert_eq!(cache.len(), 1);
        assert_eq!(cache.memory_used(), 1514);

        // now add an entry which is over the limit and make sure the old stale entry is removed
        let stale_entry = cache.put(&path_1, value_too_large.clone());
        assert_eq!(stale_entry, Some(value_fits));

        assert_eq!(cache.len(), 0);
        assert_eq!(cache.memory_used(), 0);
    }

    fn create_cached_file_metadata_with_stats(
        file_name: &str,
        series_size: i32,
    ) -> (ObjectMeta, CachedFileMetadata) {
        let series: Vec<i32> = (0..=series_size).collect();
        let values = Int32Array::from(series);
        let offsets = OffsetBuffer::new(ScalarBuffer::from(vec![0, series_size + 1]));
        let field = Arc::new(Field::new_list_field(DataType::Int32, false));
        let list_array = ListArray::new(field, offsets, Arc::new(values), None);

        let column_statistics = ColumnStatistics {
            null_count: Precision::Exact(1),
            max_value: Precision::Exact(ScalarValue::List(Arc::new(list_array.clone()))),
            min_value: Precision::Exact(ScalarValue::List(Arc::new(list_array.clone()))),
            sum_value: Precision::Exact(ScalarValue::List(Arc::new(list_array.clone()))),
            distinct_count: Precision::Exact(10),
            byte_size: Precision::Absent,
        };

        let stats = Statistics {
            num_rows: Precision::Exact(100),
            total_byte_size: Precision::Exact(100),
            column_statistics: vec![column_statistics.clone()],
        };
        let mut ctx = DFHeapSizeCtx::default();
        let object_meta = create_test_meta(file_name, stats.heap_size(&mut ctx) as u64);
        let value =
            CachedFileMetadata::new(object_meta.clone(), Arc::new(stats.clone()), None);
        (object_meta, value)
    }

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
    fn create_object_meta(path: &str, location_size: usize) -> ObjectMeta {
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

    /// Helper function to create a TableScopedPath and a CachedFileList with at least meta_size bytes
    fn create_test_list_files_entry(
        path: &str,
        count: usize,
        meta_size: usize,
        table: Option<TableReference>,
    ) -> (TableScopedPath, CachedFileList) {
        let key = TableScopedPath {
            table,
            path: Path::from(path),
        };
        let metas: Vec<ObjectMeta> = (0..count)
            .map(|i| create_object_meta(&format!("file{i}"), meta_size))
            .collect();
        let value = CachedFileList::new(metas);
        (key, value)
    }

    #[test]
    fn test_basic_operations() {
        let cache = DefaultCache::new(DEFAULT_LIST_FILES_CACHE_MEMORY_LIMIT);
        let table_ref = Some(TableReference::from("table"));
        let path = Path::from("test_path");
        let key = TableScopedPath {
            table: table_ref.clone(),
            path,
        };

        // Initially cache is empty
        assert!(!cache.contains_key(&key));
        assert_eq!(cache.len(), 0);

        // Cache miss - get returns None
        assert!(cache.get(&key).is_none());

        // Put a value
        let meta = create_test_object_meta("file1", 50);
        cache.put(&key, CachedFileList::new(vec![meta]));

        // Entry should be cached
        assert!(cache.contains_key(&key));
        assert_eq!(cache.len(), 1);
        let result = cache.get(&key).unwrap();
        assert_eq!(result.files.len(), 1);

        // Remove the entry
        let removed = cache.remove(&key).unwrap();
        assert_eq!(removed.files.len(), 1);
        assert!(!cache.contains_key(&key));
        assert_eq!(cache.len(), 0);

        // Put multiple entries
        let (key1, value1) =
            create_test_list_files_entry("path1", 2, 50, table_ref.clone());
        let (key2, value2) = create_test_list_files_entry("path2", 3, 50, table_ref);
        cache.put(&key1, value1.clone());
        cache.put(&key2, value2.clone());
        assert_eq!(cache.len(), 2);

        // List cache entries
        assert_eq!(
            cache.list_entries(),
            HashMap::from([
                (
                    key1.clone(),
                    CacheEntryInfo {
                        value: value1.clone(),
                        size_bytes: value1.size(),
                        hits: 0,
                        expires: None,
                    }
                ),
                (
                    key2.clone(),
                    CacheEntryInfo {
                        value: value2.clone(),
                        size_bytes: value2.size(),
                        hits: 0,
                        expires: None,
                    }
                )
            ])
        );

        // Clear all entries
        cache.clear();
        assert_eq!(cache.len(), 0);
        assert!(!cache.contains_key(&key1));
        assert!(!cache.contains_key(&key2));
    }

    #[test]
    fn test_lru_eviction_basic() {
        let table_ref = Some(TableReference::from("table"));
        let (key1, value1) =
            create_test_list_files_entry("path1", 1, 100, table_ref.clone());
        let (key2, value2) =
            create_test_list_files_entry("path2", 1, 100, table_ref.clone());
        let (key3, value3) =
            create_test_list_files_entry("path3", 1, 100, table_ref.clone());

        let entry_size = key1.size() + value1.size();

        // Set cache limit to exactly fit all 3 entries
        let cache = DefaultCache::new(entry_size * 3);

        // All three entries should fit
        cache.put(&key1, value1);
        cache.put(&key2, value2);
        cache.put(&key3, value3);
        assert_eq!(cache.len(), 3);
        assert!(cache.contains_key(&key1));
        assert!(cache.contains_key(&key2));
        assert!(cache.contains_key(&key3));

        // Adding a new entry should evict path1 (LRU)
        let (key4, value4) = create_test_list_files_entry("path4", 1, 100, table_ref);
        cache.put(&key4, value4);

        assert_eq!(cache.len(), 3);
        assert!(!cache.contains_key(&key1)); // Evicted
        assert!(cache.contains_key(&key2));
        assert!(cache.contains_key(&key3));
        assert!(cache.contains_key(&key4));
    }

    #[test]
    fn test_lru_ordering_after_access() {
        let table_ref = Some(TableReference::from("table"));
        let (key1, value1) =
            create_test_list_files_entry("path1", 1, 100, table_ref.clone());
        let (key2, value2) =
            create_test_list_files_entry("path2", 1, 100, table_ref.clone());
        let (key3, value3) =
            create_test_list_files_entry("path3", 1, 100, table_ref.clone());

        // Set cache limit to fit exactly three entries
        let cache = DefaultCache::new((key1.size() + value1.size()) * 3);

        cache.put(&key1, value1);
        cache.put(&key2, value2);
        cache.put(&key3, value3);
        assert_eq!(cache.len(), 3);

        // Access path1 to move it to front (MRU)
        // Order is now: path2 (LRU), path3, path1 (MRU)
        let _ = cache.get(&key1);

        // Adding a new entry should evict path2 (the LRU)
        let (key4, value4) = create_test_list_files_entry("path4", 1, 100, table_ref);
        cache.put(&key4, value4);

        assert_eq!(cache.len(), 3);
        assert!(cache.contains_key(&key1)); // Still present (recently accessed)
        assert!(!cache.contains_key(&key2)); // Evicted (was LRU)
        assert!(cache.contains_key(&key3));
        assert!(cache.contains_key(&key4));
    }

    #[test]
    fn test_reject_too_large() {
        let table_ref = Some(TableReference::from("table"));
        let (key1, value1) =
            create_test_list_files_entry("path1", 1, 100, table_ref.clone());
        let (key2, value2) =
            create_test_list_files_entry("path2", 1, 100, table_ref.clone());

        // Set cache limit to fit both entries
        let cache = DefaultCache::new((key1.size() + value1.size()) * 2);

        cache.put(&key1, value1);
        cache.put(&key2, value2);
        assert_eq!(cache.len(), 2);

        // Try to add an entry that's too large to fit in the cache
        // The entry is not stored (too large)
        let (key_large, value_large) =
            create_test_list_files_entry("large", 1, 1000, table_ref);
        cache.put(&key_large, value_large);

        // Large entry should not be added
        assert!(!cache.contains_key(&key_large));
        assert_eq!(cache.len(), 2);
        assert!(cache.contains_key(&key1));
        assert!(cache.contains_key(&key2));
    }

    #[test]
    fn test_multiple_evictions() {
        let table_ref = Some(TableReference::from("table"));
        let (key1, value1) =
            create_test_list_files_entry("path1", 1, 100, table_ref.clone());
        let (key2, value2) =
            create_test_list_files_entry("path2", 1, 100, table_ref.clone());
        let (key3, value3) =
            create_test_list_files_entry("path3", 1, 100, table_ref.clone());

        let entry_size = key1.size() + value1.size();

        // Set cache limit for exactly 3 entries
        let cache = DefaultCache::new(entry_size * 3);

        cache.put(&key1, value1);
        cache.put(&key2, value2);
        cache.put(&key3, value3);
        assert_eq!(cache.len(), 3);

        // Add a large entry that requires evicting 2 entries
        let (key_large, value_large) =
            create_test_list_files_entry("large", 1, 200, table_ref);
        cache.put(&key_large, value_large);

        // path1 and path2 should be evicted (both LRU), path3 and path_large remain
        assert_eq!(cache.len(), 2);
        assert!(!cache.contains_key(&key1)); // Evicted
        assert!(!cache.contains_key(&key2)); // Evicted
        assert!(cache.contains_key(&key3));
        assert!(cache.contains_key(&key_large));
    }

    #[test]
    fn test_cache_limit_resize() {
        let table_ref = Some(TableReference::from("table"));
        let (key1, value1) =
            create_test_list_files_entry("path1", 1, 100, table_ref.clone());
        let (key2, value2) =
            create_test_list_files_entry("path2", 1, 100, table_ref.clone());
        let (key3, value3) = create_test_list_files_entry("path3", 1, 100, table_ref);

        let entry_size = key1.size() + value1.size();

        let cache = DefaultCache::new(entry_size * 3);

        // Add three entries
        cache.put(&key1, value1);
        cache.put(&key2, value2);
        cache.put(&key3, value3);
        assert_eq!(cache.len(), 3);

        // Resize cache to only fit one entry
        cache.update_cache_limit(entry_size);

        // Should keep only the most recent entry (path3, the MRU)
        assert_eq!(cache.len(), 1);
        assert!(cache.contains_key(&key3));
        // Earlier entries (LRU) should be evicted
        assert!(!cache.contains_key(&key1));
        assert!(!cache.contains_key(&key2));
    }

    #[test]
    fn test_entry_update_with_size_change() {
        let table_ref = Some(TableReference::from("table"));
        let (key1, value1) =
            create_test_list_files_entry("path1", 1, 100, table_ref.clone());
        let (key2, value2) =
            create_test_list_files_entry("path2", 1, 100, table_ref.clone());
        let (key3, value3_v1) =
            create_test_list_files_entry("path3", 1, 100, table_ref.clone());

        let entry_size = key1.size() + value1.size();

        let cache = DefaultCache::new(entry_size * 3);

        // Add three entries
        cache.put(&key1, value1);
        cache.put(&key2, value2.clone());
        cache.put(&key3, value3_v1);
        assert_eq!(cache.len(), 3);

        // Update path3 with same size - should not cause eviction
        let (_, value3_v2) =
            create_test_list_files_entry("path3", 1, 100, table_ref.clone());
        cache.put(&key3, value3_v2);

        assert_eq!(cache.len(), 3);
        assert!(cache.contains_key(&key1));
        assert!(cache.contains_key(&key2));
        assert!(cache.contains_key(&key3));

        // Update path3 with larger size that requires evicting path1 (LRU)
        let (_, value3_v3) = create_test_list_files_entry("path3", 1, 200, table_ref);
        cache.put(&key3, value3_v3.clone());

        assert_eq!(cache.len(), 2);
        assert!(!cache.contains_key(&key1)); // Evicted (was LRU)
        assert!(cache.contains_key(&key2));
        assert!(cache.contains_key(&key3));

        // List cache entries
        assert_eq!(
            cache.list_entries(),
            HashMap::from([
                (
                    key2,
                    CacheEntryInfo {
                        value: value2.clone(),
                        size_bytes: value2.size(),
                        hits: 0,
                        expires: None,
                    }
                ),
                (
                    key3,
                    CacheEntryInfo {
                        value: value3_v3.clone(),
                        size_bytes: value3_v3.size(),
                        hits: 0,
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
        let cache = DefaultCache::new_with_ttl(10000, Some(ttl))
            .with_time_provider(Arc::clone(&mock_time) as Arc<dyn TimeProvider>);

        let table_ref = Some(TableReference::from("table"));
        let (key1, value1) =
            create_test_list_files_entry("path1", 2, 50, table_ref.clone());
        let (key2, value2) = create_test_list_files_entry("path2", 2, 50, table_ref);
        cache.put(&key1, value1.clone());
        cache.put(&key2, value2.clone());

        // Entries should be accessible immediately
        assert!(cache.get(&key1).is_some());
        assert!(cache.get(&key2).is_some());
        // List cache entries
        assert_eq!(
            cache.list_entries(),
            HashMap::from([
                (
                    key1.clone(),
                    CacheEntryInfo {
                        value: value1.clone(),
                        size_bytes: value1.size(),
                        hits: 1,
                        expires: mock_time.now().checked_add(ttl),
                    }
                ),
                (
                    key2.clone(),
                    CacheEntryInfo {
                        value: value2.clone(),
                        size_bytes: value2.size(),
                        hits: 1,
                        expires: mock_time.now().checked_add(ttl),
                    }
                )
            ])
        );
        // Wait for TTL to expire
        mock_time.inc(Duration::from_millis(150));

        // Entries should now return None when observed through contains_key
        assert!(!cache.contains_key(&key1));
        assert_eq!(cache.len(), 1); // key1 was removed by contains_key()
        assert!(!cache.contains_key(&key2));
        assert_eq!(cache.len(), 0); // key2 was removed by contains_key()
    }

    #[test]
    fn test_cache_with_ttl_and_lru() {
        let ttl = Duration::from_millis(200);

        let mock_time = Arc::new(MockTimeProvider::new());
        let cache = DefaultCache::new_with_ttl(1100, Some(ttl))
            .with_time_provider(Arc::clone(&mock_time) as Arc<dyn TimeProvider>);

        let table_ref = Some(TableReference::from("table"));
        let (key1, value1) =
            create_test_list_files_entry("path1", 1, 400, table_ref.clone());
        let (key2, value2) =
            create_test_list_files_entry("path2", 1, 400, table_ref.clone());

        let (key3, value3) = create_test_list_files_entry("path3", 1, 400, table_ref);
        cache.put(&key1, value1);
        mock_time.inc(Duration::from_millis(50));
        cache.put(&key2, value2);
        mock_time.inc(Duration::from_millis(50));

        // path3 should evict path1 due to size limit
        cache.put(&key3, value3);
        assert!(!cache.contains_key(&key1)); // Evicted by LRU
        assert!(cache.contains_key(&key2));
        assert!(cache.contains_key(&key3));

        mock_time.inc(Duration::from_millis(151));

        assert!(!cache.contains_key(&key2)); // Expired
        assert!(cache.contains_key(&key3)); // Still valid
    }

    #[test]
    fn test_ttl_expiration_in_get() {
        let ttl = Duration::from_millis(100);
        let cache = DefaultCache::new_with_ttl(10000, Some(ttl));

        let table_ref = Some(TableReference::from("table"));
        let (key, value) = create_test_list_files_entry("path", 2, 50, table_ref);

        // Cache the entry
        cache.put(&key, value.clone());

        // Entry should be accessible immediately
        let result = cache.get(&key);
        assert!(result.is_some());
        assert_eq!(result.unwrap().files.len(), 2);

        // Wait for TTL to expire
        thread::sleep(Duration::from_millis(150));

        // Get should return None because entry expired
        let result2 = cache.get(&key);
        assert!(result2.is_none());
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
    fn test_memory_tracking() {
        let cache = DefaultCache::new(1000);

        // Verify cache starts with 0 memory used
        {
            assert_eq!(cache.memory_used(), 0);
        }

        // Add entry and verify memory tracking
        let table_ref = Some(TableReference::from("table"));
        let (key1, value1) =
            create_test_list_files_entry("path1", 1, 100, table_ref.clone());
        cache.put(&key1, value1.clone());
        let entry_size_1 = key1.size() + value1.size();
        {
            assert_eq!(cache.memory_used(), entry_size_1);
        }

        // Add another entry
        let (key2, value2) =
            create_test_list_files_entry("path2", 1, 200, table_ref.clone());
        cache.put(&key2, value2.clone());
        let entry_size_2 = key2.size() + value2.size();

        {
            assert_eq!(cache.memory_used(), entry_size_1 + entry_size_2);
        }

        // Remove first entry and verify memory decreases
        cache.remove(&key1);
        {
            assert_eq!(cache.memory_used(), entry_size_2);
        }

        // Clear and verify memory is 0
        cache.clear();
        {
            assert_eq!(cache.memory_used(), 0);
        }
    }

    // Prefix filtering tests using CachedFileList::filter_by_prefix

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
    fn test_prefix_filtering() {
        let cache = DefaultCache::new(100000);

        // Create files for a partitioned table
        let table_base = Path::from("my_table");
        let files = vec![
            create_object_meta_with_path("my_table/a=1/file1.parquet"),
            create_object_meta_with_path("my_table/a=1/file2.parquet"),
            create_object_meta_with_path("my_table/a=2/file3.parquet"),
            create_object_meta_with_path("my_table/a=2/file4.parquet"),
        ];

        // Cache the full table listing
        let table_ref = Some(TableReference::from("table"));
        let key = TableScopedPath {
            table: table_ref,
            path: table_base,
        };
        cache.put(&key, CachedFileList::new(files));

        let result = cache.get(&key).unwrap();

        // Filter for partition a=1
        let prefix_a1 = Some(Path::from("my_table/a=1"));
        let filtered = result.files_matching_prefix(&prefix_a1);
        assert_eq!(filtered.len(), 2);
        assert!(
            filtered
                .iter()
                .all(|m| m.location.as_ref().starts_with("my_table/a=1"))
        );

        // Filter for partition a=2
        let prefix_a2 = Some(Path::from("my_table/a=2"));
        let filtered_2 = result.files_matching_prefix(&prefix_a2);
        assert_eq!(filtered_2.len(), 2);
        assert!(
            filtered_2
                .iter()
                .all(|m| m.location.as_ref().starts_with("my_table/a=2"))
        );

        // No filter returns all
        let all = result.files_matching_prefix(&None);
        assert_eq!(all.len(), 4);
    }

    #[test]
    fn test_prefix_no_matching_files() {
        let cache = DefaultCache::new(100000);

        let table_base = Path::from("my_table");
        let files = vec![
            create_object_meta_with_path("my_table/a=1/file1.parquet"),
            create_object_meta_with_path("my_table/a=2/file2.parquet"),
        ];

        let table_ref = Some(TableReference::from("table"));
        let key = TableScopedPath {
            table: table_ref,
            path: table_base,
        };
        cache.put(&key, CachedFileList::new(files));
        let result = cache.get(&key).unwrap();

        // Query for partition a=3 which doesn't exist
        let prefix_a3 = Some(Path::from("my_table/a=3"));
        let filtered = result.files_matching_prefix(&prefix_a3);
        assert!(filtered.is_empty());
    }

    #[test]
    fn test_nested_partitions() {
        let cache = DefaultCache::new(100000);

        let table_base = Path::from("events");
        let files = vec![
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
        ];

        let table_ref = Some(TableReference::from("table"));
        let key = TableScopedPath {
            table: table_ref,
            path: table_base,
        };
        cache.put(&key, CachedFileList::new(files));
        let result = cache.get(&key).unwrap();

        // Filter for year=2024/month=01
        let prefix_month = Some(Path::from("events/year=2024/month=01"));
        let filtered = result.files_matching_prefix(&prefix_month);
        assert_eq!(filtered.len(), 2);

        // Filter for year=2024
        let prefix_year = Some(Path::from("events/year=2024"));
        let filtered_year = result.files_matching_prefix(&prefix_year);
        assert_eq!(filtered_year.len(), 3);
    }

    #[test]
    fn test_drop_table_entries() {
        let cache = DefaultCache::new(DEFAULT_LIST_FILES_CACHE_MEMORY_LIMIT);

        let table_ref1 = TableReference::from("table1");
        let table_ref2 = TableReference::from("table2");
        let (key1, value1) =
            create_test_list_files_entry("path1", 1, 100, Some(table_ref1.clone()));
        let (key2, value2) =
            create_test_list_files_entry("path2", 1, 100, Some(table_ref1.clone()));
        let (key3, value3) =
            create_test_list_files_entry("path3", 1, 100, Some(table_ref2.clone()));

        cache.put(&key1, value1);
        cache.put(&key2, value2);
        cache.put(&key3, value3);

        cache.drop_table_entries(&table_ref1).unwrap();

        assert!(!cache.contains_key(&key1));
        assert!(!cache.contains_key(&key2));
        assert!(cache.contains_key(&key3));
    }
}
