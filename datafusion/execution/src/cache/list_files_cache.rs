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

#[cfg(test)]
mod tests {
    use crate::cache::cache_manager::{
        CachedFileList, DEFAULT_LIST_FILES_CACHE_MEMORY_LIMIT, TableScopedPath,
        meta_heap_bytes,
    };
    use crate::cache::default_cache::{DefaultCache, TimeProvider};
    use crate::cache::{Cache, CacheEntryInfo, CacheKey, CacheValue};
    use chrono::DateTime;
    use datafusion_common::HashMap;
    use datafusion_common::TableReference;
    use datafusion_common::instant::Instant;
    use object_store::{ObjectMeta, path::Path};
    use std::sync::{Arc, Mutex};
    use std::thread;
    use std::time::Duration;

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
            .map(|i| create_test_object_meta(&format!("file{i}"), meta_size))
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
        let cache = DefaultCache::with_ttl(10000, Some(ttl))
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
        let cache = DefaultCache::with_ttl(1100, Some(ttl))
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
        let cache = DefaultCache::with_ttl(1000, Some(ttl));

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

        let table_ref1 = Some(TableReference::from("table1"));
        let table_ref2 = Some(TableReference::from("table2"));
        let (key1, value1) =
            create_test_list_files_entry("path1", 1, 100, table_ref1.clone());
        let (key2, value2) =
            create_test_list_files_entry("path2", 1, 100, table_ref1.clone());
        let (key3, value3) =
            create_test_list_files_entry("path3", 1, 100, table_ref2.clone());

        cache.put(&key1, value1);
        cache.put(&key2, value2);
        cache.put(&key3, value3);

        cache.drop_table_entries(&table_ref1).unwrap();

        assert!(!cache.contains_key(&key1));
        assert!(!cache.contains_key(&key2));
        assert!(cache.contains_key(&key3));
    }
}
