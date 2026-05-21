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
    use std::sync::Arc;

    use crate::cache::cache_manager::{CachedFileMetadataEntry, FileMetadata};
    use crate::cache::default_cache::DefaultCache;
    use crate::cache::{Cache, CacheAccessor, CacheEntryInfo};
    use datafusion_common::HashMap;
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

    impl PartialEq for CachedFileMetadataEntry {
        fn eq(&self, other: &Self) -> bool {
            self.meta == other.meta
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
}
