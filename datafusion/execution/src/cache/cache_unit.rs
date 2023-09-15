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

use dashmap::DashMap;
use datafusion_common::Statistics;
use object_store::path::Path;
use object_store::ObjectMeta;
use crate::cache::CacheAccessor;

/// Collected statistics for files
/// Cache is invalided when file size or last modification has changed
#[derive(Default)]
pub struct FileStatisticsCache {
    statistics: DashMap<Path, (ObjectMeta, Statistics)>,
}

impl CacheAccessor<Path, Statistics> for FileStatisticsCache {
    type Extra = ObjectMeta;

    /// Get `Statistics` for file location.
    fn get(&self, k: &Path) -> Option<Statistics> {
        self.statistics
            .get(&k)
            .map(|s| {
                Some(s.value().1.clone())
            })
            .unwrap_or(None)
    }

    /// Get `Statistics` for file location. Returns None if file has changed or not found.
    fn get_with_extra(&self, k: &Path, e: &Self::Extra) -> Option<Statistics> {
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
    fn put(&self, _key: &Path, _value: Statistics) -> Option<Statistics> {
        panic!("Put cache in FileStatisticsCache without Extra not supported.")
    }

    fn put_with_extra(&self, key: &Path, value: Statistics, e: &Self::Extra) -> Option<Statistics> {
        self.statistics
            .insert(key.clone(), (e.clone(), value)).and_then(|x| Some(x.1))
    }

    fn evict(&self, k: &Path) -> bool {
        self.statistics.remove(&k).is_some()
    }

    fn contains_key(&self, k: &Path) -> bool {
        self.statistics.contains_key(&k)
    }

    fn len(&self) -> usize {
        self.statistics.len()
    }
}

#[cfg(test)]
mod tests {
    use chrono::DateTime;
    use datafusion_common::Statistics;
    use object_store::path::Path;
    use object_store::ObjectMeta;
    use crate::cache::cache_unit::FileStatisticsCache;
    use crate::cache::CacheAccessor;

    #[test]
    fn test_statistics_cache() {
        let meta = ObjectMeta {
            location: Path::from("test"),
            last_modified: DateTime::parse_from_rfc3339("2022-09-27T22:36:00+02:00")
                .unwrap()
                .into(),
            size: 1024,
            e_tag: None,
        };

        let cache = FileStatisticsCache::default();
        assert!(cache.get_with_extra(&meta.location, &meta).is_none());

        cache.put_with_extra(&meta.location, Statistics::default(), &meta);
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
}