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

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use crate::cache::cache_manager::{
    FileMetadata, FileMetadataCache, FileMetadataCacheEntry, FileStatisticsCache,
    FileStatisticsCacheEntry,
};
use crate::cache::lru_queue::LruQueue;
use crate::cache::CacheAccessor;

use datafusion_common::Statistics;

use dashmap::DashMap;
use object_store::path::Path;
use object_store::ObjectMeta;

/// Default implementation of [`FileStatisticsCache`]
///
/// Stores collected statistics for files
///
/// Cache is invalided when file size or last modification has changed
///
/// [`FileStatisticsCache`]: crate::cache::cache_manager::FileStatisticsCache
#[derive(Default)]
pub struct DefaultFileStatisticsCache {
    statistics: DashMap<Path, (ObjectMeta, Arc<Statistics>)>,
}

impl FileStatisticsCache for DefaultFileStatisticsCache {
    fn list_entries(&self) -> HashMap<Path, FileStatisticsCacheEntry> {
        let mut entries = HashMap::<Path, FileStatisticsCacheEntry>::new();

        for entry in &self.statistics {
            let path = entry.key();
            let (object_meta, stats) = entry.value();
            entries.insert(
                path.clone(),
                FileStatisticsCacheEntry {
                    object_meta: object_meta.clone(),
                    num_rows: stats.num_rows,
                    num_columns: stats.column_statistics.len(),
                    table_size_bytes: stats.total_byte_size,
                    statistics_size_bytes: 0, // TODO: set to the real size in the future
                },
            );
        }

        entries
    }
}

impl CacheAccessor<Path, Arc<Statistics>> for DefaultFileStatisticsCache {
    type Extra = ObjectMeta;

    /// Get `Statistics` for file location.
    fn get(&self, k: &Path) -> Option<Arc<Statistics>> {
        self.statistics
            .get(k)
            .map(|s| Some(Arc::clone(&s.value().1)))
            .unwrap_or(None)
    }

    /// Get `Statistics` for file location. Returns None if file has changed or not found.
    fn get_with_extra(&self, k: &Path, e: &Self::Extra) -> Option<Arc<Statistics>> {
        self.statistics
            .get(k)
            .map(|s| {
                let (saved_meta, statistics) = s.value();
                if saved_meta.size != e.size
                    || saved_meta.last_modified != e.last_modified
                {
                    // file has changed
                    None
                } else {
                    Some(Arc::clone(statistics))
                }
            })
            .unwrap_or(None)
    }

    /// Save collected file statistics
    fn put(&self, _key: &Path, _value: Arc<Statistics>) -> Option<Arc<Statistics>> {
        panic!("Put cache in DefaultFileStatisticsCache without Extra not supported.")
    }

    fn put_with_extra(
        &self,
        key: &Path,
        value: Arc<Statistics>,
        e: &Self::Extra,
    ) -> Option<Arc<Statistics>> {
        self.statistics
            .insert(key.clone(), (e.clone(), value))
            .map(|x| x.1)
    }

    fn remove(&self, k: &Path) -> Option<Arc<Statistics>> {
        self.statistics.remove(k).map(|x| x.1 .1)
    }

    fn contains_key(&self, k: &Path) -> bool {
        self.statistics.contains_key(k)
    }

    fn len(&self) -> usize {
        self.statistics.len()
    }

    fn clear(&self) {
        self.statistics.clear()
    }
    fn name(&self) -> String {
        "DefaultFileStatisticsCache".to_string()
    }
}

/// Default implementation of [`ListFilesCache`]
///
/// Collected files metadata for listing files.
///
/// Cache is not invalided until user calls [`Self::remove`] or [`Self::clear`].
///
/// [`ListFilesCache`]: crate::cache::cache_manager::ListFilesCache
#[derive(Default)]
pub struct DefaultListFilesCache {
    statistics: DashMap<Path, Arc<Vec<ObjectMeta>>>,
}

impl CacheAccessor<Path, Arc<Vec<ObjectMeta>>> for DefaultListFilesCache {
    type Extra = ObjectMeta;

    fn get(&self, k: &Path) -> Option<Arc<Vec<ObjectMeta>>> {
        self.statistics.get(k).map(|x| Arc::clone(x.value()))
    }

    fn get_with_extra(
        &self,
        _k: &Path,
        _e: &Self::Extra,
    ) -> Option<Arc<Vec<ObjectMeta>>> {
        panic!("Not supported DefaultListFilesCache get_with_extra")
    }

    fn put(
        &self,
        key: &Path,
        value: Arc<Vec<ObjectMeta>>,
    ) -> Option<Arc<Vec<ObjectMeta>>> {
        self.statistics.insert(key.clone(), value)
    }

    fn put_with_extra(
        &self,
        _key: &Path,
        _value: Arc<Vec<ObjectMeta>>,
        _e: &Self::Extra,
    ) -> Option<Arc<Vec<ObjectMeta>>> {
        panic!("Not supported DefaultListFilesCache put_with_extra")
    }

    fn remove(&self, k: &Path) -> Option<Arc<Vec<ObjectMeta>>> {
        self.statistics.remove(k).map(|x| x.1)
    }

    fn contains_key(&self, k: &Path) -> bool {
        self.statistics.contains_key(k)
    }

    fn len(&self) -> usize {
        self.statistics.len()
    }

    fn clear(&self) {
        self.statistics.clear()
    }

    fn name(&self) -> String {
        "DefaultListFilesCache".to_string()
    }
}

/// Handles the inner state of the [`DefaultFilesMetadataCache`] struct.
struct DefaultFilesMetadataCacheState {
    lru_queue: LruQueue<Path, (ObjectMeta, Arc<dyn FileMetadata>)>,
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

    /// Returns the respective entry from the cache, if it exists and the `size` and `last_modified`
    /// properties from [`ObjectMeta`] match.
    /// If the entry exists, it becomes the most recently used.
    fn get(&mut self, k: &ObjectMeta) -> Option<Arc<dyn FileMetadata>> {
        self.lru_queue
            .get(&k.location)
            .map(|(object_meta, metadata)| {
                if object_meta.size != k.size
                    || object_meta.last_modified != k.last_modified
                {
                    None
                } else {
                    *self.cache_hits.entry(k.location.clone()).or_insert(0) += 1;
                    Some(Arc::clone(metadata))
                }
            })
            .unwrap_or(None)
    }

    /// Checks if the metadata is currently cached (entry exists and the `size` and `last_modified`
    /// properties of [`ObjectMeta`] match).
    /// The LRU queue is not updated.
    fn contains_key(&self, k: &ObjectMeta) -> bool {
        self.lru_queue
            .peek(&k.location)
            .map(|(object_meta, _)| {
                object_meta.size == k.size && object_meta.last_modified == k.last_modified
            })
            .unwrap_or(false)
    }

    /// Adds a new key-value pair to cache, meaning LRU entries might be evicted if required.
    /// If the key is already in the cache, the previous metadata is returned.
    /// If the size of the metadata is greater than the `memory_limit`, the value is not inserted.
    fn put(
        &mut self,
        key: ObjectMeta,
        value: Arc<dyn FileMetadata>,
    ) -> Option<Arc<dyn FileMetadata>> {
        let value_size = value.memory_size();

        // no point in trying to add this value to the cache if it cannot fit entirely
        if value_size > self.memory_limit {
            return None;
        }

        self.cache_hits.insert(key.location.clone(), 0);
        // if the key is already in the cache, the old value is removed
        let old_value = self.lru_queue.put(key.location.clone(), (key, value));
        self.memory_used += value_size;
        if let Some((_, ref old_metadata)) = old_value {
            self.memory_used -= old_metadata.memory_size();
        }

        self.evict_entries();

        old_value.map(|v| v.1)
    }

    /// Evicts entries from the LRU cache until `memory_used` is lower than `memory_limit`.
    fn evict_entries(&mut self) {
        while self.memory_used > self.memory_limit {
            if let Some(removed) = self.lru_queue.pop() {
                let metadata: Arc<dyn FileMetadata> = removed.1 .1;
                self.memory_used -= metadata.memory_size();
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
    fn remove(&mut self, k: &ObjectMeta) -> Option<Arc<dyn FileMetadata>> {
        if let Some((_, old_metadata)) = self.lru_queue.remove(&k.location) {
            self.memory_used -= old_metadata.memory_size();
            self.cache_hits.remove(&k.location);
            Some(old_metadata)
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
/// The metadata for each file is invalidated when the file size or last
/// modification time have been changed.
///
/// # Internal details
///
/// The `memory_limit` controls the maximum size of the cache, which uses a
/// Least Recently Used eviction algorithm. When adding a new entry, if the total
/// size of the cached entries exceeds `memory_limit`, the least recently used entries
/// are evicted until the total size is lower than `memory_limit`.
///
/// # `Extra` Handling
///
/// Users should use the [`Self::get`] and [`Self::put`] methods. The
/// [`Self::get_with_extra`] and [`Self::put_with_extra`] methods simply call
/// `get` and `put`, respectively.
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

        for (path, (object_meta, metadata)) in state.lru_queue.list_entries() {
            entries.insert(
                path.clone(),
                FileMetadataCacheEntry {
                    object_meta: object_meta.clone(),
                    size_bytes: metadata.memory_size(),
                    hits: *state.cache_hits.get(path).expect("entry must exist"),
                    extra: metadata.extra_info(),
                },
            );
        }

        entries
    }
}

impl CacheAccessor<ObjectMeta, Arc<dyn FileMetadata>> for DefaultFilesMetadataCache {
    type Extra = ObjectMeta;

    fn get(&self, k: &ObjectMeta) -> Option<Arc<dyn FileMetadata>> {
        let mut state = self.state.lock().unwrap();
        state.get(k)
    }

    fn get_with_extra(
        &self,
        k: &ObjectMeta,
        _e: &Self::Extra,
    ) -> Option<Arc<dyn FileMetadata>> {
        self.get(k)
    }

    fn put(
        &self,
        key: &ObjectMeta,
        value: Arc<dyn FileMetadata>,
    ) -> Option<Arc<dyn FileMetadata>> {
        let mut state = self.state.lock().unwrap();
        state.put(key.clone(), value)
    }

    fn put_with_extra(
        &self,
        key: &ObjectMeta,
        value: Arc<dyn FileMetadata>,
        _e: &Self::Extra,
    ) -> Option<Arc<dyn FileMetadata>> {
        self.put(key, value)
    }

    fn remove(&self, k: &ObjectMeta) -> Option<Arc<dyn FileMetadata>> {
        let mut state = self.state.lock().unwrap();
        state.remove(k)
    }

    fn contains_key(&self, k: &ObjectMeta) -> bool {
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

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::sync::Arc;

    use crate::cache::cache_manager::{
        FileMetadata, FileMetadataCache, FileMetadataCacheEntry, FileStatisticsCache,
        FileStatisticsCacheEntry,
    };
    use crate::cache::cache_unit::{
        DefaultFileStatisticsCache, DefaultFilesMetadataCache, DefaultListFilesCache,
    };
    use crate::cache::CacheAccessor;
    use arrow::datatypes::{DataType, Field, Schema, TimeUnit};
    use chrono::DateTime;
    use datafusion_common::stats::Precision;
    use datafusion_common::Statistics;
    use object_store::path::Path;
    use object_store::ObjectMeta;

    #[test]
    fn test_statistics_cache() {
        let meta = ObjectMeta {
            location: Path::from("test"),
            last_modified: DateTime::parse_from_rfc3339("2022-09-27T22:36:00+02:00")
                .unwrap()
                .into(),
            size: 1024,
            e_tag: None,
            version: None,
        };
        let cache = DefaultFileStatisticsCache::default();
        assert!(cache.get_with_extra(&meta.location, &meta).is_none());

        cache.put_with_extra(
            &meta.location,
            Statistics::new_unknown(&Schema::new(vec![Field::new(
                "test_column",
                DataType::Timestamp(TimeUnit::Second, None),
                false,
            )]))
            .into(),
            &meta,
        );
        assert!(cache.get_with_extra(&meta.location, &meta).is_some());

        // file size changed
        let mut meta2 = meta.clone();
        meta2.size = 2048;
        assert!(cache.get_with_extra(&meta2.location, &meta2).is_none());

        // file last_modified changed
        let mut meta2 = meta.clone();
        meta2.last_modified = DateTime::parse_from_rfc3339("2022-09-27T22:40:00+02:00")
            .unwrap()
            .into();
        assert!(cache.get_with_extra(&meta2.location, &meta2).is_none());

        // different file
        let mut meta2 = meta.clone();
        meta2.location = Path::from("test2");
        assert!(cache.get_with_extra(&meta2.location, &meta2).is_none());

        // test the list_entries method
        let entries = cache.list_entries();
        assert_eq!(
            entries,
            HashMap::from([(
                Path::from("test"),
                FileStatisticsCacheEntry {
                    object_meta: meta.clone(),
                    num_rows: Precision::Absent,
                    num_columns: 1,
                    table_size_bytes: Precision::Absent,
                    statistics_size_bytes: 0,
                }
            )])
        );
    }

    #[test]
    fn test_list_file_cache() {
        let meta = ObjectMeta {
            location: Path::from("test"),
            last_modified: DateTime::parse_from_rfc3339("2022-09-27T22:36:00+02:00")
                .unwrap()
                .into(),
            size: 1024,
            e_tag: None,
            version: None,
        };

        let cache = DefaultListFilesCache::default();
        assert!(cache.get(&meta.location).is_none());

        cache.put(&meta.location, vec![meta.clone()].into());
        assert_eq!(
            cache.get(&meta.location).unwrap().first().unwrap().clone(),
            meta.clone()
        );
    }

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

    #[test]
    fn test_default_file_metadata_cache() {
        let object_meta = ObjectMeta {
            location: Path::from("test"),
            last_modified: DateTime::parse_from_rfc3339("2025-07-29T12:12:12+00:00")
                .unwrap()
                .into(),
            size: 1024,
            e_tag: None,
            version: None,
        };

        let metadata: Arc<dyn FileMetadata> = Arc::new(TestFileMetadata {
            metadata: "retrieved_metadata".to_owned(),
        });

        let cache = DefaultFilesMetadataCache::new(1024 * 1024);
        assert!(cache.get(&object_meta).is_none());

        // put
        cache.put(&object_meta, Arc::clone(&metadata));

        // get and contains of a valid entry
        assert!(cache.contains_key(&object_meta));
        let value = cache.get(&object_meta);
        assert!(value.is_some());
        let test_file_metadata = Arc::downcast::<TestFileMetadata>(value.unwrap());
        assert!(test_file_metadata.is_ok());
        assert_eq!(test_file_metadata.unwrap().metadata, "retrieved_metadata");

        // file size changed
        let mut object_meta2 = object_meta.clone();
        object_meta2.size = 2048;
        assert!(cache.get(&object_meta2).is_none());
        assert!(!cache.contains_key(&object_meta2));

        // file last_modified changed
        let mut object_meta2 = object_meta.clone();
        object_meta2.last_modified =
            DateTime::parse_from_rfc3339("2025-07-29T13:13:13+00:00")
                .unwrap()
                .into();
        assert!(cache.get(&object_meta2).is_none());
        assert!(!cache.contains_key(&object_meta2));

        // different file
        let mut object_meta2 = object_meta.clone();
        object_meta2.location = Path::from("test2");
        assert!(cache.get(&object_meta2).is_none());
        assert!(!cache.contains_key(&object_meta2));

        // remove
        cache.remove(&object_meta);
        assert!(cache.get(&object_meta).is_none());
        assert!(!cache.contains_key(&object_meta));

        // len and clear
        cache.put(&object_meta, Arc::clone(&metadata));
        cache.put(&object_meta2, metadata);
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

        cache.put(&object_meta1, metadata1);
        cache.put(&object_meta2, metadata2);
        cache.put(&object_meta3, metadata3);

        // all entries will fit
        assert_eq!(cache.len(), 3);
        assert_eq!(cache.memory_used(), 900);
        assert!(cache.contains_key(&object_meta1));
        assert!(cache.contains_key(&object_meta2));
        assert!(cache.contains_key(&object_meta3));

        // add a new entry which will remove the least recently used ("1")
        let (object_meta4, metadata4) = generate_test_metadata_with_size("4", 200);
        cache.put(&object_meta4, metadata4);
        assert_eq!(cache.len(), 3);
        assert_eq!(cache.memory_used(), 1000);
        assert!(!cache.contains_key(&object_meta1));
        assert!(cache.contains_key(&object_meta4));

        // get entry "2", which will move it to the top of the queue, and add a new one which will
        // remove the new least recently used ("3")
        cache.get(&object_meta2);
        let (object_meta5, metadata5) = generate_test_metadata_with_size("5", 100);
        cache.put(&object_meta5, metadata5);
        assert_eq!(cache.len(), 3);
        assert_eq!(cache.memory_used(), 800);
        assert!(!cache.contains_key(&object_meta3));
        assert!(cache.contains_key(&object_meta5));

        // new entry which will not be able to fit in the 1000 bytes allocated
        let (object_meta6, metadata6) = generate_test_metadata_with_size("6", 1200);
        cache.put(&object_meta6, metadata6);
        assert_eq!(cache.len(), 3);
        assert_eq!(cache.memory_used(), 800);
        assert!(!cache.contains_key(&object_meta6));

        // new entry which is able to fit without removing any entry
        let (object_meta7, metadata7) = generate_test_metadata_with_size("7", 200);
        cache.put(&object_meta7, metadata7);
        assert_eq!(cache.len(), 4);
        assert_eq!(cache.memory_used(), 1000);
        assert!(cache.contains_key(&object_meta7));

        // new entry which will remove all other entries
        let (object_meta8, metadata8) = generate_test_metadata_with_size("8", 999);
        cache.put(&object_meta8, metadata8);
        assert_eq!(cache.len(), 1);
        assert_eq!(cache.memory_used(), 999);
        assert!(cache.contains_key(&object_meta8));

        // when updating an entry, the previous ones are not unnecessarily removed
        let (object_meta9, metadata9) = generate_test_metadata_with_size("9", 300);
        let (object_meta10, metadata10) = generate_test_metadata_with_size("10", 200);
        let (object_meta11_v1, metadata11_v1) =
            generate_test_metadata_with_size("11", 400);
        cache.put(&object_meta9, metadata9);
        cache.put(&object_meta10, metadata10);
        cache.put(&object_meta11_v1, metadata11_v1);
        assert_eq!(cache.memory_used(), 900);
        assert_eq!(cache.len(), 3);
        let (object_meta11_v2, metadata11_v2) =
            generate_test_metadata_with_size("11", 500);
        cache.put(&object_meta11_v2, metadata11_v2);
        assert_eq!(cache.memory_used(), 1000);
        assert_eq!(cache.len(), 3);
        assert!(cache.contains_key(&object_meta9));
        assert!(cache.contains_key(&object_meta10));
        assert!(cache.contains_key(&object_meta11_v2));
        assert!(!cache.contains_key(&object_meta11_v1));

        // when updating an entry that now exceeds the limit, the LRU ("9") needs to be removed
        let (object_meta11_v3, metadata11_v3) =
            generate_test_metadata_with_size("11", 501);
        cache.put(&object_meta11_v3, metadata11_v3);
        assert_eq!(cache.memory_used(), 701);
        assert_eq!(cache.len(), 2);
        assert!(cache.contains_key(&object_meta10));
        assert!(cache.contains_key(&object_meta11_v3));
        assert!(!cache.contains_key(&object_meta11_v2));

        // manually removing an entry that is not the LRU
        cache.remove(&object_meta11_v3);
        assert_eq!(cache.len(), 1);
        assert_eq!(cache.memory_used(), 200);
        assert!(cache.contains_key(&object_meta10));
        assert!(!cache.contains_key(&object_meta11_v3));

        // clear
        cache.clear();
        assert_eq!(cache.len(), 0);
        assert_eq!(cache.memory_used(), 0);

        // resizing the cache should clear the extra entries
        let (object_meta12, metadata12) = generate_test_metadata_with_size("12", 300);
        let (object_meta13, metadata13) = generate_test_metadata_with_size("13", 200);
        let (object_meta14, metadata14) = generate_test_metadata_with_size("14", 500);
        cache.put(&object_meta12, metadata12);
        cache.put(&object_meta13, metadata13);
        cache.put(&object_meta14, metadata14);
        assert_eq!(cache.len(), 3);
        assert_eq!(cache.memory_used(), 1000);
        cache.update_cache_limit(600);
        assert_eq!(cache.len(), 1);
        assert_eq!(cache.memory_used(), 500);
        assert!(!cache.contains_key(&object_meta12));
        assert!(!cache.contains_key(&object_meta13));
        assert!(cache.contains_key(&object_meta14));
    }

    #[test]
    fn test_default_file_metadata_cache_entries_info() {
        let cache = DefaultFilesMetadataCache::new(1000);
        let (object_meta1, metadata1) = generate_test_metadata_with_size("1", 100);
        let (object_meta2, metadata2) = generate_test_metadata_with_size("2", 200);
        let (object_meta3, metadata3) = generate_test_metadata_with_size("3", 300);

        // initial entries, all will have hits = 0
        cache.put(&object_meta1, metadata1);
        cache.put(&object_meta2, metadata2);
        cache.put(&object_meta3, metadata3);
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
        cache.get(&object_meta1);
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
        cache.put(&object_meta4, metadata4);
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
        cache.put(&object_meta1_new, metadata1_new);
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
        cache.remove(&object_meta4);
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
