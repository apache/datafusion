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

use crate::cache::CacheAccessor;
use core::panic;
use dashmap::DashMap;
use datafusion_common::Statistics;
use object_store::path::Path;
use object_store::ObjectMeta;
use parking_lot::Mutex;
use std::collections::VecDeque;
use std::sync::Arc;

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
            .map(|s| Some(s.value().1.clone()))
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
                    Some(statistics.clone())
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
        self.statistics.get(k).map(|x| x.value().clone())
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
pub struct LruMetaCache {
    // the actual hashmap
    stores: DashMap<Path, Arc<Vec<ObjectMeta>>>,
    // number of object meta we should store
    capacity: usize,
    // order in linked list
    order: Mutex<VecDeque<Path>>,
    // position of the actual path
    key_position: DashMap<Path, usize>,
}
impl LruMetaCache {
    fn update_order(&self, k: &Path) {
        let mut order = self.order.lock();
        if let Some(pos) = self.key_position.get(k).map(|x| *x.value()) {
            order.remove(pos);
            order.push_back(k.clone());
            self.key_position.insert(k.clone(), order.len() - 1);
        }
    }
}
impl Default for LruMetaCache {
    fn default() -> Self {
        Self {
            capacity: 50,
            stores: DashMap::new(),
            order: Mutex::new(VecDeque::new()),
            key_position: DashMap::new(),
        }
    }
}
impl CacheAccessor<Path, Arc<Vec<ObjectMeta>>> for LruMetaCache {
    type Extra = ObjectMeta;

    fn get(&self, k: &Path) -> Option<Arc<Vec<ObjectMeta>>> {
        let value = self.stores.get(k).map(|x| x.clone());
        if value.is_some() {
            self.update_order(k);
        }
        value
    }

    fn get_with_extra(&self, k: &Path, e: &Self::Extra) -> Option<Arc<Vec<ObjectMeta>>> {
        panic!("Put cache in LruMetaCache without Extra not supported.")
    }

    fn put(
        &self,
        key: &Path,
        value: Arc<Vec<ObjectMeta>>,
    ) -> Option<Arc<Vec<ObjectMeta>>> {
        let mut order = self.order.lock();
        if order.len() == self.capacity {
            let oldest = order.pop_front().unwrap();
            self.stores.remove(&oldest);
            self.key_position.remove(&oldest);
        }
        let res = self.stores.insert(key.clone(), value);
        order.push_back(key.clone());
        self.key_position.insert(key.clone(), order.len() - 1);
        res
    }

    fn put_with_extra(
        &self,
        _key: &Path,
        _value: Arc<Vec<ObjectMeta>>,
        _e: &Self::Extra,
    ) -> Option<Arc<Vec<ObjectMeta>>> {
        panic!("Put cache in LruMetaCache without Extra not supported.")
    }

    fn remove(&mut self, k: &Path) -> Option<Arc<Vec<ObjectMeta>>> {
        let removed_value = self.stores.remove(k);
        if let Some(value) = removed_value {
            let mut order = self.order.lock();
            if let Some(pos) = self.key_position.get(k).map(|x| *x.value()) {
                order.remove(pos);
                self.key_position.remove(k);
            }
            Some(value.1)
        } else {
            None
        }
    }

    fn contains_key(&self, k: &Path) -> bool {
        self.stores.contains_key(k)
    }

    fn len(&self) -> usize {
        self.stores.len()
    }

    fn clear(&self) {
        self.stores.clear();
        let mut order = self.order.lock();
        order.clear();
        self.key_position.clear();
    }

    fn name(&self) -> String {
        "LruMetaCache".to_string()
    }
}
#[cfg(test)]
mod tests {
    use crate::cache::cache_unit::{
        DefaultFileStatisticsCache, DefaultListFilesCache, LruMetaCache,
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
    #[test]
    fn test_lru_cache_single() {
        let meta = ObjectMeta {
            location: Path::from("test"),
            last_modified: DateTime::parse_from_rfc3339("2022-09-27T22:36:00+02:00")
                .unwrap()
                .into(),
            size: 1024,
            e_tag: None,
            version: None,
        };

        let cache = LruMetaCache::default();
        assert!(cache.get(&meta.location).is_none());

        cache.put(&meta.location, vec![meta.clone()].into());
        assert_eq!(
            cache.get(&meta.location).unwrap().first().unwrap().clone(),
            meta.clone()
        );
    }
    use std::sync::Arc;
    use std::thread;
    #[test]
    fn test_lru_cache_concurreny() {
        let meta = ObjectMeta {
            location: Path::from("test"),
            last_modified: DateTime::parse_from_rfc3339("2022-09-27T22:36:00+02:00")
                .unwrap()
                .into(),
            size: 1024,
            e_tag: None,
            version: None,
        };

        let cache = Arc::new(LruMetaCache::default());
        assert!(cache.get(&meta.location).is_none());

        let cache_clone = cache.clone();
        let meta_clone = meta.clone();
        let handle_put = thread::spawn(move || {
            cache_clone.put(&meta_clone.location, Arc::new(vec![meta_clone.clone()]));
        });

        handle_put.join().unwrap();

        let mut handles = vec![];
        for _ in 0..10 {
            let cache_clone = cache.clone();
            let meta_clone = meta.clone();
            handles.push(thread::spawn(move || {
                let retrieved_meta = cache_clone.get(&meta_clone.location).unwrap();
                assert_eq!(retrieved_meta.first().unwrap().clone(), meta_clone);
            }));
        }

        for handle in handles {
            handle.join().unwrap();
        }
    }
}
