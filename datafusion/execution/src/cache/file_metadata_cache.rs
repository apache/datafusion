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

use std::{collections::HashMap, sync::Mutex};

use object_store::path::Path;

use crate::cache::{
    CacheAccessor,
    cache_manager::{CachedFileMetadataEntry, FileMetadataCache, FileMetadataCacheEntry},
    lru_queue::LruQueue,
};

/// Handles the inner state of the [`DefaultFilesMetadataCache`] struct.
struct DefaultFilesMetadataCacheState {
    lru_queue: LruQueue<Path, CachedFileMetadataEntry>,
    memory_limit: usize,
    memory_used: usize,
    cache_hits: HashMap<Path, usize>,
}

impl DefaultFilesMetadataCacheState {
    fn new(memory_limit: usize) -> Self {
        Self {
            lru_queue: LruQueue::new(),
            memory_limit,
            memory_used: 0,
            cache_hits: HashMap::new(),
        }
    }

    /// Returns the respective entry from the cache, if it exists.
    /// If the entry exists, it becomes the most recently used.
    fn get(&mut self, k: &Path) -> Option<CachedFileMetadataEntry> {
        self.lru_queue.get(k).cloned().inspect(|_| {
            *self.cache_hits.entry(k.clone()).or_insert(0) += 1;
        })
    }

    /// Checks if the metadata is currently cached.
    /// The LRU queue is not updated.
    fn contains_key(&self, k: &Path) -> bool {
        self.lru_queue.peek(k).is_some()
    }

    /// Adds a new key-value pair to cache, meaning LRU entries might be evicted if required.
    /// If the key is already in the cache, the previous metadata is returned.
    /// If the size of the metadata is greater than the `memory_limit`, the value is not inserted.
    fn put(
        &mut self,
        key: Path,
        value: CachedFileMetadataEntry,
    ) -> Option<CachedFileMetadataEntry> {
        let value_size = value.file_metadata.memory_size();

        // no point in trying to add this value to the cache if it cannot fit entirely
        if value_size > self.memory_limit {
            return None;
        }

        self.cache_hits.insert(key.clone(), 0);
        // if the key is already in the cache, the old value is removed
        let old_value = self.lru_queue.put(key, value);
        self.memory_used += value_size;
        if let Some(ref old_entry) = old_value {
            self.memory_used -= old_entry.file_metadata.memory_size();
        }

        self.evict_entries();

        old_value
    }

    /// Evicts entries from the LRU cache until `memory_used` is lower than `memory_limit`.
    fn evict_entries(&mut self) {
        while self.memory_used > self.memory_limit {
            if let Some(removed) = self.lru_queue.pop() {
                self.memory_used -= removed.1.file_metadata.memory_size();
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
    fn remove(&mut self, k: &Path) -> Option<CachedFileMetadataEntry> {
        if let Some(old_entry) = self.lru_queue.remove(k) {
            self.memory_used -= old_entry.file_metadata.memory_size();
            self.cache_hits.remove(k);
            Some(old_entry)
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
        self.cache_hits.clear();
    }
}

/// Default implementation of [`FileMetadataCache`]
///
/// Collected file embedded metadata cache.
///
/// The metadata for each file is validated by comparing the cached [`ObjectMeta`]
/// (size and last_modified) against the current file state using `cached.is_valid_for(&current_meta)`.
///
/// # Internal details
///
/// The `memory_limit` controls the maximum size of the cache, which uses a
/// Least Recently Used eviction algorithm. When adding a new entry, if the total
/// size of the cached entries exceeds `memory_limit`, the least recently used entries
/// are evicted until the total size is lower than `memory_limit`.
///
/// [`ObjectMeta`]: object_store::ObjectMeta
pub struct DefaultFilesMetadataCache {
    // the state is wrapped in a Mutex to ensure the operations are atomic
    state: Mutex<DefaultFilesMetadataCacheState>,
}

impl DefaultFilesMetadataCache {
    /// Create a new instance of [`DefaultFilesMetadataCache`].
    ///
    /// # Arguments
    /// `memory_limit`:  the maximum size of the cache, in bytes
    //
    pub fn new(memory_limit: usize) -> Self {
        Self {
            state: Mutex::new(DefaultFilesMetadataCacheState::new(memory_limit)),
        }
    }

    /// Returns the size of the cached memory, in bytes.
    pub fn memory_used(&self) -> usize {
        let state = self.state.lock().unwrap();
        state.memory_used
    }
}

impl CacheAccessor<Path, CachedFileMetadataEntry> for DefaultFilesMetadataCache {
    fn get(&self, key: &Path) -> Option<CachedFileMetadataEntry> {
        let mut state = self.state.lock().unwrap();
        state.get(key)
    }

    fn put(
        &self,
        key: &Path,
        value: CachedFileMetadataEntry,
    ) -> Option<CachedFileMetadataEntry> {
        let mut state = self.state.lock().unwrap();
        state.put(key.clone(), value)
    }

    fn remove(&self, k: &Path) -> Option<CachedFileMetadataEntry> {
        let mut state = self.state.lock().unwrap();
        state.remove(k)
    }

    fn contains_key(&self, k: &Path) -> bool {
        let state = self.state.lock().unwrap();
        state.contains_key(k)
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
        "DefaultFilesMetadataCache".to_string()
    }
}

impl FileMetadataCache for DefaultFilesMetadataCache {
    fn cache_limit(&self) -> usize {
        let state = self.state.lock().unwrap();
        state.memory_limit
    }

    fn update_cache_limit(&self, limit: usize) {
        let mut state = self.state.lock().unwrap();
        state.memory_limit = limit;
        state.evict_entries();
    }

    fn list_entries(&self) -> HashMap<Path, FileMetadataCacheEntry> {
        let state = self.state.lock().unwrap();
        let mut entries = HashMap::<Path, FileMetadataCacheEntry>::new();

        for (path, entry) in state.lru_queue.list_entries() {
            entries.insert(
                path.clone(),
                FileMetadataCacheEntry {
                    object_meta: entry.meta.clone(),
                    size_bytes: entry.file_metadata.memory_size(),
                    hits: *state.cache_hits.get(path).expect("entry must exist"),
                    extra: entry.file_metadata.extra_info(),
                },
            );
        }

        entries
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::sync::Arc;

    use crate::cache::CacheAccessor;
    use crate::cache::cache_manager::{
        CachedFileMetadataEntry, FileMetadata, FileMetadataCache, FileMetadataCacheEntry,
    };
    use crate::cache::file_metadata_cache::DefaultFilesMetadataCache;
    use object_store::ObjectMeta;
    use object_store::path::Path;

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

    fn create_test_object_meta(path: &str, size: usize) -> ObjectMeta {
        ObjectMeta {
            location: Path::from(path),
            last_modified: chrono::DateTime::parse_from_rfc3339(
                "2025-07-29T12:12:12+00:00",
            )
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

        let cache = DefaultFilesMetadataCache::new(1024 * 1024);

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
        let metadata: Arc<dyn FileMetadata> = Arc::new(TestFileMetadata {
            metadata: "a".repeat(size),
        });

        (object_meta, metadata)
    }

    #[test]
    fn test_default_file_metadata_cache_with_limit() {
        let cache = DefaultFilesMetadataCache::new(1000);
        let (object_meta1, metadata1) = generate_test_metadata_with_size("1", 100);
        let (object_meta2, metadata2) = generate_test_metadata_with_size("2", 500);
        let (object_meta3, metadata3) = generate_test_metadata_with_size("3", 300);

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
        assert_eq!(cache.memory_used(), 900);
        assert!(cache.contains_key(&object_meta1.location));
        assert!(cache.contains_key(&object_meta2.location));
        assert!(cache.contains_key(&object_meta3.location));

        // add a new entry which will remove the least recently used ("1")
        let (object_meta4, metadata4) = generate_test_metadata_with_size("4", 200);
        cache.put(
            &object_meta4.location,
            CachedFileMetadataEntry::new(object_meta4.clone(), metadata4),
        );
        assert_eq!(cache.len(), 3);
        assert_eq!(cache.memory_used(), 1000);
        assert!(!cache.contains_key(&object_meta1.location));
        assert!(cache.contains_key(&object_meta4.location));

        // get entry "2", which will move it to the top of the queue, and add a new one which will
        // remove the new least recently used ("3")
        let _ = cache.get(&object_meta2.location);
        let (object_meta5, metadata5) = generate_test_metadata_with_size("5", 100);
        cache.put(
            &object_meta5.location,
            CachedFileMetadataEntry::new(object_meta5.clone(), metadata5),
        );
        assert_eq!(cache.len(), 3);
        assert_eq!(cache.memory_used(), 800);
        assert!(!cache.contains_key(&object_meta3.location));
        assert!(cache.contains_key(&object_meta5.location));

        // new entry which will not be able to fit in the 1000 bytes allocated
        let (object_meta6, metadata6) = generate_test_metadata_with_size("6", 1200);
        cache.put(
            &object_meta6.location,
            CachedFileMetadataEntry::new(object_meta6.clone(), metadata6),
        );
        assert_eq!(cache.len(), 3);
        assert_eq!(cache.memory_used(), 800);
        assert!(!cache.contains_key(&object_meta6.location));

        // new entry which is able to fit without removing any entry
        let (object_meta7, metadata7) = generate_test_metadata_with_size("7", 200);
        cache.put(
            &object_meta7.location,
            CachedFileMetadataEntry::new(object_meta7.clone(), metadata7),
        );
        assert_eq!(cache.len(), 4);
        assert_eq!(cache.memory_used(), 1000);
        assert!(cache.contains_key(&object_meta7.location));

        // new entry which will remove all other entries
        let (object_meta8, metadata8) = generate_test_metadata_with_size("8", 999);
        cache.put(
            &object_meta8.location,
            CachedFileMetadataEntry::new(object_meta8.clone(), metadata8),
        );
        assert_eq!(cache.len(), 1);
        assert_eq!(cache.memory_used(), 999);
        assert!(cache.contains_key(&object_meta8.location));

        // when updating an entry, the previous ones are not unnecessarily removed
        let (object_meta9, metadata9) = generate_test_metadata_with_size("9", 300);
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
        assert_eq!(cache.memory_used(), 900);
        assert_eq!(cache.len(), 3);
        let (object_meta11_v2, metadata11_v2) =
            generate_test_metadata_with_size("11", 500);
        cache.put(
            &object_meta11_v2.location,
            CachedFileMetadataEntry::new(object_meta11_v2.clone(), metadata11_v2),
        );
        assert_eq!(cache.memory_used(), 1000);
        assert_eq!(cache.len(), 3);
        assert!(cache.contains_key(&object_meta9.location));
        assert!(cache.contains_key(&object_meta10.location));
        assert!(cache.contains_key(&object_meta11_v2.location));

        // when updating an entry that now exceeds the limit, the LRU ("9") needs to be removed
        let (object_meta11_v3, metadata11_v3) =
            generate_test_metadata_with_size("11", 501);
        cache.put(
            &object_meta11_v3.location,
            CachedFileMetadataEntry::new(object_meta11_v3.clone(), metadata11_v3),
        );
        assert_eq!(cache.memory_used(), 701);
        assert_eq!(cache.len(), 2);
        assert!(cache.contains_key(&object_meta10.location));
        assert!(cache.contains_key(&object_meta11_v3.location));

        // manually removing an entry that is not the LRU
        cache.remove(&object_meta11_v3.location);
        assert_eq!(cache.len(), 1);
        assert_eq!(cache.memory_used(), 200);
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
        assert_eq!(cache.memory_used(), 1000);
        cache.update_cache_limit(600);
        assert_eq!(cache.len(), 1);
        assert_eq!(cache.memory_used(), 500);
        assert!(!cache.contains_key(&object_meta12.location));
        assert!(!cache.contains_key(&object_meta13.location));
        assert!(cache.contains_key(&object_meta14.location));
    }

    #[test]
    fn test_default_file_metadata_cache_entries_info() {
        let cache = DefaultFilesMetadataCache::new(1000);
        let (object_meta1, metadata1) = generate_test_metadata_with_size("1", 100);
        let (object_meta2, metadata2) = generate_test_metadata_with_size("2", 200);
        let (object_meta3, metadata3) = generate_test_metadata_with_size("3", 300);

        // initial entries, all will have hits = 0
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
        assert_eq!(
            cache.list_entries(),
            HashMap::from([
                (
                    Path::from("1"),
                    FileMetadataCacheEntry {
                        object_meta: object_meta1.clone(),
                        size_bytes: 100,
                        hits: 0,
                        extra: HashMap::from([(
                            "extra_info".to_owned(),
                            "abc".to_owned()
                        )]),
                    }
                ),
                (
                    Path::from("2"),
                    FileMetadataCacheEntry {
                        object_meta: object_meta2.clone(),
                        size_bytes: 200,
                        hits: 0,
                        extra: HashMap::from([(
                            "extra_info".to_owned(),
                            "abc".to_owned()
                        )]),
                    }
                ),
                (
                    Path::from("3"),
                    FileMetadataCacheEntry {
                        object_meta: object_meta3.clone(),
                        size_bytes: 300,
                        hits: 0,
                        extra: HashMap::from([(
                            "extra_info".to_owned(),
                            "abc".to_owned()
                        )]),
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
                    FileMetadataCacheEntry {
                        object_meta: object_meta1.clone(),
                        size_bytes: 100,
                        hits: 1,
                        extra: HashMap::from([(
                            "extra_info".to_owned(),
                            "abc".to_owned()
                        )]),
                    }
                ),
                (
                    Path::from("2"),
                    FileMetadataCacheEntry {
                        object_meta: object_meta2.clone(),
                        size_bytes: 200,
                        hits: 0,
                        extra: HashMap::from([(
                            "extra_info".to_owned(),
                            "abc".to_owned()
                        )]),
                    }
                ),
                (
                    Path::from("3"),
                    FileMetadataCacheEntry {
                        object_meta: object_meta3.clone(),
                        size_bytes: 300,
                        hits: 0,
                        extra: HashMap::from([(
                            "extra_info".to_owned(),
                            "abc".to_owned()
                        )]),
                    }
                )
            ])
        );

        // new entry, will evict "2"
        let (object_meta4, metadata4) = generate_test_metadata_with_size("4", 600);
        cache.put(
            &object_meta4.location,
            CachedFileMetadataEntry::new(object_meta4.clone(), metadata4),
        );
        assert_eq!(
            cache.list_entries(),
            HashMap::from([
                (
                    Path::from("1"),
                    FileMetadataCacheEntry {
                        object_meta: object_meta1.clone(),
                        size_bytes: 100,
                        hits: 1,
                        extra: HashMap::from([(
                            "extra_info".to_owned(),
                            "abc".to_owned()
                        )]),
                    }
                ),
                (
                    Path::from("3"),
                    FileMetadataCacheEntry {
                        object_meta: object_meta3.clone(),
                        size_bytes: 300,
                        hits: 0,
                        extra: HashMap::from([(
                            "extra_info".to_owned(),
                            "abc".to_owned()
                        )]),
                    }
                ),
                (
                    Path::from("4"),
                    FileMetadataCacheEntry {
                        object_meta: object_meta4.clone(),
                        size_bytes: 600,
                        hits: 0,
                        extra: HashMap::from([(
                            "extra_info".to_owned(),
                            "abc".to_owned()
                        )]),
                    }
                )
            ])
        );

        // replace entry "1"
        let (object_meta1_new, metadata1_new) = generate_test_metadata_with_size("1", 50);
        cache.put(
            &object_meta1_new.location,
            CachedFileMetadataEntry::new(object_meta1_new.clone(), metadata1_new),
        );
        assert_eq!(
            cache.list_entries(),
            HashMap::from([
                (
                    Path::from("1"),
                    FileMetadataCacheEntry {
                        object_meta: object_meta1_new.clone(),
                        size_bytes: 50,
                        hits: 0,
                        extra: HashMap::from([(
                            "extra_info".to_owned(),
                            "abc".to_owned()
                        )]),
                    }
                ),
                (
                    Path::from("3"),
                    FileMetadataCacheEntry {
                        object_meta: object_meta3.clone(),
                        size_bytes: 300,
                        hits: 0,
                        extra: HashMap::from([(
                            "extra_info".to_owned(),
                            "abc".to_owned()
                        )]),
                    }
                ),
                (
                    Path::from("4"),
                    FileMetadataCacheEntry {
                        object_meta: object_meta4.clone(),
                        size_bytes: 600,
                        hits: 0,
                        extra: HashMap::from([(
                            "extra_info".to_owned(),
                            "abc".to_owned()
                        )]),
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
                    FileMetadataCacheEntry {
                        object_meta: object_meta1_new.clone(),
                        size_bytes: 50,
                        hits: 0,
                        extra: HashMap::from([(
                            "extra_info".to_owned(),
                            "abc".to_owned()
                        )]),
                    }
                ),
                (
                    Path::from("3"),
                    FileMetadataCacheEntry {
                        object_meta: object_meta3.clone(),
                        size_bytes: 300,
                        hits: 0,
                        extra: HashMap::from([(
                            "extra_info".to_owned(),
                            "abc".to_owned()
                        )]),
                    }
                )
            ])
        );

        // clear
        cache.clear();
        assert_eq!(cache.list_entries(), HashMap::from([]));
    }
}
