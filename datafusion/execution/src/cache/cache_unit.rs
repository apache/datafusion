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

use crate::cache::CacheAccessor;
use crate::cache::cache_manager::{
    CachedFileMetadata, FileStatisticsCache, FileStatisticsCacheEntry,
};

use dashmap::DashMap;
use object_store::path::Path;

pub use crate::cache::DefaultFilesMetadataCache;

/// Default implementation of [`FileStatisticsCache`]
///
/// Stores cached file metadata (statistics and orderings) for files.
///
/// The typical usage pattern is:
/// 1. Call `get(path)` to check for cached value
/// 2. If `Some(cached)`, validate with `cached.is_valid_for(&current_meta)`
/// 3. If invalid or missing, compute new value and call `put(path, new_value)`
///
/// Uses DashMap for lock-free concurrent access.
///
/// [`FileStatisticsCache`]: crate::cache::cache_manager::FileStatisticsCache
#[derive(Default)]
pub struct DefaultFileStatisticsCache {
    cache: DashMap<Path, CachedFileMetadata>,
}

impl CacheAccessor<Path, CachedFileMetadata> for DefaultFileStatisticsCache {
    fn get(&self, key: &Path) -> Option<CachedFileMetadata> {
        self.cache.get(key).map(|entry| entry.value().clone())
    }

    fn put(&self, key: &Path, value: CachedFileMetadata) -> Option<CachedFileMetadata> {
        self.cache.insert(key.clone(), value)
    }

    fn remove(&self, k: &Path) -> Option<CachedFileMetadata> {
        self.cache.remove(k).map(|(_, entry)| entry)
    }

    fn contains_key(&self, k: &Path) -> bool {
        self.cache.contains_key(k)
    }

    fn len(&self) -> usize {
        self.cache.len()
    }

    fn clear(&self) {
        self.cache.clear();
    }

    fn name(&self) -> String {
        "DefaultFileStatisticsCache".to_string()
    }
}

impl FileStatisticsCache for DefaultFileStatisticsCache {
    fn list_entries(&self) -> HashMap<Path, FileStatisticsCacheEntry> {
        let mut entries = HashMap::<Path, FileStatisticsCacheEntry>::new();

        for entry in self.cache.iter() {
            let path = entry.key();
            let cached = entry.value();
            entries.insert(
                path.clone(),
                FileStatisticsCacheEntry {
                    object_meta: cached.meta.clone(),
                    num_rows: cached.statistics.num_rows,
                    num_columns: cached.statistics.column_statistics.len(),
                    table_size_bytes: cached.statistics.total_byte_size,
                    statistics_size_bytes: 0, // TODO: set to the real size in the future
                    has_ordering: cached.ordering.is_some(),
                },
            );
        }

        entries
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cache::CacheAccessor;
    use crate::cache::cache_manager::{
        CachedFileMetadata, FileStatisticsCache, FileStatisticsCacheEntry,
    };
    use arrow::array::RecordBatch;
    use arrow::datatypes::{DataType, Field, Schema, TimeUnit};
    use chrono::DateTime;
    use datafusion_common::Statistics;
    use datafusion_common::stats::Precision;
    use datafusion_expr::ColumnarValue;
    use datafusion_physical_expr_common::physical_expr::PhysicalExpr;
    use datafusion_physical_expr_common::sort_expr::{LexOrdering, PhysicalSortExpr};
    use object_store::ObjectMeta;
    use object_store::path::Path;
    use std::sync::Arc;

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
        let cache = DefaultFileStatisticsCache::default();

        let schema = Schema::new(vec![Field::new(
            "test_column",
            DataType::Timestamp(TimeUnit::Second, None),
            false,
        )]);

        // Cache miss
        assert!(cache.get(&meta.location).is_none());

        // Put a value
        let cached_value = CachedFileMetadata::new(
            meta.clone(),
            Arc::new(Statistics::new_unknown(&schema)),
            None,
        );
        cache.put(&meta.location, cached_value);

        // Cache hit
        let result = cache.get(&meta.location);
        assert!(result.is_some());
        let cached = result.unwrap();
        assert!(cached.is_valid_for(&meta));

        // File size changed - validation should fail
        let meta2 = create_test_meta("test", 2048);
        let cached = cache.get(&meta2.location).unwrap();
        assert!(!cached.is_valid_for(&meta2));

        // Update with new value
        let cached_value2 = CachedFileMetadata::new(
            meta2.clone(),
            Arc::new(Statistics::new_unknown(&schema)),
            None,
        );
        cache.put(&meta2.location, cached_value2);

        // Test list_entries
        let entries = cache.list_entries();
        assert_eq!(entries.len(), 1);
        let entry = entries.get(&Path::from("test")).unwrap();
        assert_eq!(entry.object_meta.size, 2048); // Should be updated value
    }

    #[derive(Clone, Debug, PartialEq, Eq, Hash)]
    struct MockExpr {}

    impl std::fmt::Display for MockExpr {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            write!(f, "MockExpr")
        }
    }

    impl PhysicalExpr for MockExpr {
        fn as_any(&self) -> &dyn std::any::Any {
            self
        }

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
        let cache = DefaultFileStatisticsCache::default();

        let schema = Schema::new(vec![Field::new("a", DataType::Int32, false)]);

        // Cache statistics with no ordering
        let cached_value = CachedFileMetadata::new(
            meta.clone(),
            Arc::new(Statistics::new_unknown(&schema)),
            None, // No ordering yet
        );
        cache.put(&meta.location, cached_value);

        let result = cache.get(&meta.location).unwrap();
        assert!(result.ordering.is_none());

        // Update to add ordering
        let mut cached = cache.get(&meta.location).unwrap();
        if cached.is_valid_for(&meta) && cached.ordering.is_none() {
            cached.ordering = Some(ordering());
        }
        cache.put(&meta.location, cached);

        let result2 = cache.get(&meta.location).unwrap();
        assert!(result2.ordering.is_some());

        // Verify list_entries shows has_ordering = true
        let entries = cache.list_entries();
        assert_eq!(entries.len(), 1);
        assert!(entries.get(&meta.location).unwrap().has_ordering);
    }

    #[test]
    fn test_cache_invalidation_on_file_modification() {
        let cache = DefaultFileStatisticsCache::default();
        let path = Path::from("test.parquet");
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
    fn test_list_entries() {
        let cache = DefaultFileStatisticsCache::default();
        let schema = Schema::new(vec![Field::new("a", DataType::Int32, false)]);

        let meta1 = create_test_meta("test1.parquet", 100);

        let cached_value = CachedFileMetadata::new(
            meta1.clone(),
            Arc::new(Statistics::new_unknown(&schema)),
            None,
        );
        cache.put(&meta1.location, cached_value);
        let meta2 = create_test_meta("test2.parquet", 200);
        let cached_value = CachedFileMetadata::new(
            meta2.clone(),
            Arc::new(Statistics::new_unknown(&schema)),
            Some(ordering()),
        );
        cache.put(&meta2.location, cached_value);

        let entries = cache.list_entries();
        assert_eq!(
            entries,
            HashMap::from([
                (
                    Path::from("test1.parquet"),
                    FileStatisticsCacheEntry {
                        object_meta: meta1,
                        num_rows: Precision::Absent,
                        num_columns: 1,
                        table_size_bytes: Precision::Absent,
                        statistics_size_bytes: 0,
                        has_ordering: false,
                    }
                ),
                (
                    Path::from("test2.parquet"),
                    FileStatisticsCacheEntry {
                        object_meta: meta2,
                        num_rows: Precision::Absent,
                        num_columns: 1,
                        table_size_bytes: Precision::Absent,
                        statistics_size_bytes: 0,
                        has_ordering: true,
                    }
                ),
            ])
        );
    }
}
