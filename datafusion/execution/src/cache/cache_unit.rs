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

use crate::cache::CacheAccessor;

use datafusion_common::Statistics;

use dashmap::DashMap;
use object_store::ObjectMeta;
use object_store::path::Path;

pub use crate::cache::DefaultFilesMetadataCache;

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
        self.statistics.remove(k).map(|x| x.1.1)
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

#[cfg(test)]
mod tests {
    use crate::cache::CacheAccessor;
    use crate::cache::cache_unit::DefaultFileStatisticsCache;
    use arrow::datatypes::{DataType, Field, Schema, TimeUnit};
    use chrono::DateTime;
    use datafusion_common::Statistics;
    use object_store::ObjectMeta;
    use object_store::path::Path;

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
}
