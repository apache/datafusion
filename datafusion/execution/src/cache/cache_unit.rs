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

use std::sync::Arc;

use crate::cache::cache_manager::{FileMetadata, FileMetadataCache};
use crate::cache::CacheAccessor;

use datafusion_common::Statistics;

use dashmap::DashMap;
use object_store::path::Path;
use object_store::ObjectMeta;

/// Collected statistics for files
/// Cache is invalided when file size or last modification has changed
#[derive(Default)]
pub struct DefaultFileStatisticsCache {
    statistics: DashMap<Path, (ObjectMeta, Arc<Statistics>)>,
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

    fn remove(&mut self, k: &Path) -> Option<Arc<Statistics>> {
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

/// Collected files metadata for listing files.
/// Cache will not invalided until user call remove or clear.
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

    fn remove(&mut self, k: &Path) -> Option<Arc<Vec<ObjectMeta>>> {
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

/// Collected file embedded metadata cache.
/// The metadata for some file is invalided when the file size or last modification time have been
/// changed.
/// Users should use the `get` and `put` methods. The `get_with_extra` and `put_with_extra` methods
/// simply call `get` and `put`, respectively.
#[derive(Default)]
pub struct DefaultFilesMetadataCache {
    metadata: DashMap<Path, (ObjectMeta, Arc<dyn FileMetadata>)>,
}

impl FileMetadataCache for DefaultFilesMetadataCache {}

impl CacheAccessor<ObjectMeta, Arc<dyn FileMetadata>> for DefaultFilesMetadataCache {
    type Extra = ObjectMeta;

    fn get(&self, k: &ObjectMeta) -> Option<Arc<dyn FileMetadata>> {
        self.metadata
            .get(&k.location)
            .map(|s| {
                let (extra, metadata) = s.value();
                if extra.size != k.size || extra.last_modified != k.last_modified {
                    None
                } else {
                    Some(Arc::clone(metadata))
                }
            })
            .unwrap_or(None)
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
        self.metadata
            .insert(key.location.clone(), (key.clone(), value))
            .map(|x| x.1)
    }

    fn put_with_extra(
        &self,
        key: &ObjectMeta,
        value: Arc<dyn FileMetadata>,
        _e: &Self::Extra,
    ) -> Option<Arc<dyn FileMetadata>> {
        self.put(key, value)
    }

    fn remove(&mut self, k: &ObjectMeta) -> Option<Arc<dyn FileMetadata>> {
        self.metadata.remove(&k.location).map(|x| x.1 .1)
    }

    fn contains_key(&self, k: &ObjectMeta) -> bool {
        self.metadata
            .get(&k.location)
            .map(|s| {
                let (extra, _) = s.value();
                extra.size == k.size && extra.last_modified == k.last_modified
            })
            .unwrap_or(false)
    }

    fn len(&self) -> usize {
        self.metadata.len()
    }

    fn clear(&self) {
        self.metadata.clear();
    }

    fn name(&self) -> String {
        "DefaultFilesMetadataCache".to_string()
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use crate::cache::cache_manager::FileMetadata;
    use crate::cache::cache_unit::{
        DefaultFileStatisticsCache, DefaultFilesMetadataCache, DefaultListFilesCache,
    };
    use crate::cache::CacheAccessor;
    use arrow::datatypes::{DataType, Field, Schema, TimeUnit};
    use chrono::DateTime;
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
        let mut meta2 = meta;
        meta2.location = Path::from("test2");
        assert!(cache.get_with_extra(&meta2.location, &meta2).is_none());
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
    }

    #[test]
    fn test_file_metadata_cache() {
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

        let mut cache = DefaultFilesMetadataCache::default();
        assert!(cache.get(&object_meta).is_none());

        // put
        cache.put(&object_meta, metadata);

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
    }
}
