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

use crate::cache::cache_manager::{
    CachedFileMetadata, FileStatisticsCache, FileStatisticsCacheEntry,
};
use crate::cache::{CacheAccessor, TableScopedPath};
use std::collections::HashMap;
use std::sync::Mutex;

pub use crate::cache::DefaultFilesMetadataCache;
use crate::cache::lru_queue::LruQueue;
use datafusion_common::TableReference;
use datafusion_common::heap_size::{DFHeapSize, DFHeapSizeCtx};

/// Default implementation of [`FileStatisticsCache`]
///
/// Stores cached file metadata (statistics and orderings) for files.
///
/// The typical usage pattern is:
/// 1. Call `get(path)` to check for cached value
/// 2. If `Some(cached)`, validate with `cached.is_valid_for(&current_meta)`
/// 3. If invalid or missing, compute new value and call `put(path, new_value)`
///
/// # Internal details
///
/// The `memory_limit` controls the maximum size of the cache, which uses a
/// Least Recently Used eviction algorithm. When adding a new entry, if the total
/// size of the cached entries exceeds `memory_limit`, the least recently used entries
/// are evicted until the total size is lower than `memory_limit`.
///
///
/// [`FileStatisticsCache`]: crate::cache::cache_manager::FileStatisticsCache
#[derive(Default)]
pub struct DefaultFileStatisticsCache {
    state: Mutex<DefaultFileStatisticsCacheState>,
}

impl DefaultFileStatisticsCache {
    pub fn new(memory_limit: usize) -> Self {
        Self {
            state: Mutex::new(DefaultFileStatisticsCacheState::new(memory_limit)),
        }
    }

    /// Returns the size of the cached memory, in bytes.
    pub fn memory_used(&self) -> usize {
        let state = self.state.lock().unwrap();
        state.memory_used
    }
}

struct DefaultFileStatisticsCacheState {
    lru_queue: LruQueue<TableScopedPath, CachedFileMetadata>,
    memory_limit: usize,
    memory_used: usize,
}

pub const DEFAULT_FILE_STATISTICS_MEMORY_LIMIT: usize = 20 * 1024 * 1024; // 20MiB

impl Default for DefaultFileStatisticsCacheState {
    fn default() -> Self {
        Self {
            lru_queue: LruQueue::new(),
            memory_limit: DEFAULT_FILE_STATISTICS_MEMORY_LIMIT,
            memory_used: 0,
        }
    }
}

impl DefaultFileStatisticsCacheState {
    fn new(memory_limit: usize) -> Self {
        Self {
            lru_queue: LruQueue::new(),
            memory_limit,
            memory_used: 0,
        }
    }
    fn get(&mut self, key: &TableScopedPath) -> Option<CachedFileMetadata> {
        self.lru_queue.get(key).cloned()
    }

    fn put(
        &mut self,
        key: &TableScopedPath,
        value: CachedFileMetadata,
    ) -> Option<CachedFileMetadata> {
        let mut ctx = DFHeapSizeCtx::default();
        let key_size = key.heap_size(&mut ctx);
        let entry_size = value.heap_size(&mut ctx);

        if entry_size + key_size > self.memory_limit {
            // Remove potential stale entry
            self.remove(key);
            return None;
        }

        self.memory_used += entry_size;
        self.memory_used += key_size;

        let old_value = self.lru_queue.put(key.clone(), value);
        if let Some(old_entry) = &old_value {
            let mut ctx = DFHeapSizeCtx::default();
            self.memory_used -= old_entry.heap_size(&mut ctx);
            self.memory_used -= key_size;
        }

        self.evict_entries();

        old_value
    }

    fn remove(&mut self, k: &TableScopedPath) -> Option<CachedFileMetadata> {
        if let Some(old_entry) = self.lru_queue.remove(k) {
            let mut ctx = DFHeapSizeCtx::default();
            self.memory_used -= k.heap_size(&mut ctx);
            self.memory_used -= old_entry.heap_size(&mut ctx);
            Some(old_entry)
        } else {
            None
        }
    }

    fn contains_key(&self, k: &TableScopedPath) -> bool {
        self.lru_queue.contains_key(k)
    }

    fn len(&self) -> usize {
        self.lru_queue.len()
    }

    fn clear(&mut self) {
        self.lru_queue.clear();
        self.memory_used = 0;
    }

    fn evict_entries(&mut self) {
        while self.memory_used > self.memory_limit {
            if let Some(removed) = self.lru_queue.pop() {
                let mut ctx = DFHeapSizeCtx::default();
                self.memory_used -= removed.0.heap_size(&mut ctx);
                self.memory_used -= removed.1.heap_size(&mut ctx);
            } else {
                // cache is empty while memory_used > memory_limit, cannot happen
                log::error!(
                    "File statistics cache memory accounting bug: memory_used={} but cache is empty. \
                     Please report this to the Apache DataFusion developers.",
                    self.memory_used
                );
                debug_assert!(
                    false,
                    "memory_used={} but cache is empty",
                    self.memory_used
                );
                self.memory_used = 0;
                return;
            }
        }
    }
}
impl CacheAccessor<TableScopedPath, CachedFileMetadata> for DefaultFileStatisticsCache {
    fn get(&self, key: &TableScopedPath) -> Option<CachedFileMetadata> {
        let mut state = self.state.lock().unwrap();
        state.get(key)
    }

    fn put(
        &self,
        key: &TableScopedPath,
        value: CachedFileMetadata,
    ) -> Option<CachedFileMetadata> {
        let mut state = self.state.lock().unwrap();
        state.put(key, value)
    }

    fn remove(&self, key: &TableScopedPath) -> Option<CachedFileMetadata> {
        let mut state = self.state.lock().unwrap();
        state.remove(key)
    }

    fn contains_key(&self, k: &TableScopedPath) -> bool {
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
        "DefaultFileStatisticsCache".to_string()
    }
}

impl FileStatisticsCache for DefaultFileStatisticsCache {
    fn cache_limit(&self) -> usize {
        let state = self.state.lock().unwrap();
        state.memory_limit
    }

    fn update_cache_limit(&self, limit: usize) {
        let mut state = self.state.lock().unwrap();
        state.memory_limit = limit;
        state.evict_entries();
    }

    fn list_entries(&self) -> HashMap<TableScopedPath, FileStatisticsCacheEntry> {
        let mut entries = HashMap::<TableScopedPath, FileStatisticsCacheEntry>::new();
        let mut ctx = DFHeapSizeCtx::default();
        for entry in self.state.lock().unwrap().lru_queue.list_entries() {
            let path = entry.0.clone();
            let cached = entry.1;
            entries.insert(
                path,
                FileStatisticsCacheEntry {
                    object_meta: cached.meta.clone(),
                    num_rows: cached.statistics.num_rows,
                    num_columns: cached.statistics.column_statistics.len(),
                    table_size_bytes: cached.statistics.total_byte_size,
                    statistics_size_bytes: cached.statistics.heap_size(&mut ctx),
                    has_ordering: cached.ordering.is_some(),
                },
            );
        }

        entries
    }

    fn drop_table_entries(
        &self,
        table_ref: &Option<TableReference>,
    ) -> datafusion_common::Result<()> {
        let mut state = self.state.lock().unwrap();
        let mut table_paths = vec![];
        for (path, _) in state.lru_queue.list_entries() {
            if path.table == *table_ref {
                table_paths.push(path.clone());
            }
        }
        for path in table_paths {
            state.remove(&path);
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cache::cache_manager::{
        CachedFileMetadata, FileStatisticsCache, FileStatisticsCacheEntry,
    };
    use arrow::array::{Int32Array, ListArray, RecordBatch};
    use arrow::buffer::{OffsetBuffer, ScalarBuffer};
    use arrow::datatypes::{DataType, Field, Schema, TimeUnit};
    use chrono::DateTime;
    use datafusion_common::heap_size::DFHeapSizeCtx;
    use datafusion_common::stats::Precision;
    use datafusion_common::{ColumnStatistics, ScalarValue, Statistics};
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
        assert!(entries.get(&path).unwrap().has_ordering);
    }

    #[test]
    fn test_cache_invalidation_on_file_modification() {
        let cache = DefaultFileStatisticsCache::default();
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
        let cache = DefaultFileStatisticsCache::default();
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
        let cache = DefaultFileStatisticsCache::default();
        let schema = Schema::new(vec![Field::new("a", DataType::Int32, false)]);

        let meta1 = create_test_meta("test1.parquet", 100);

        let cached_value = CachedFileMetadata::new(
            meta1.clone(),
            Arc::new(Statistics::new_unknown(&schema)),
            None,
        );

        let path_1 = TableScopedPath {
            path: meta1.location.clone(),
            table: None,
        };

        cache.put(&path_1, cached_value);
        let meta2 = create_test_meta("test2.parquet", 200);
        let cached_value = CachedFileMetadata::new(
            meta2.clone(),
            Arc::new(Statistics::new_unknown(&schema)),
            Some(ordering()),
        );

        let path_2 = TableScopedPath {
            path: meta2.location.clone(),
            table: None,
        };

        cache.put(&path_2, cached_value);

        let entries = cache.list_entries();
        assert_eq!(
            entries,
            HashMap::from([
                (
                    path_1,
                    FileStatisticsCacheEntry {
                        object_meta: meta1,
                        num_rows: Precision::Absent,
                        num_columns: 1,
                        table_size_bytes: Precision::Absent,
                        statistics_size_bytes: 304,
                        has_ordering: false,
                    }
                ),
                (
                    path_2,
                    FileStatisticsCacheEntry {
                        object_meta: meta2,
                        num_rows: Precision::Absent,
                        num_columns: 1,
                        table_size_bytes: Precision::Absent,
                        statistics_size_bytes: 304,
                        has_ordering: true,
                    }
                ),
            ])
        );
    }

    #[test]
    fn test_cache_entry_added_when_entries_are_within_cache_limit() {
        let (meta_1, value_1) = create_cached_file_metadata_with_stats("test1.parquet");
        let (meta_2, value_2) = create_cached_file_metadata_with_stats("test2.parquet");
        let (meta_3, value_3) = create_cached_file_metadata_with_stats("test3.parquet");

        let mut ctx = DFHeapSizeCtx::default();

        let limit_for_2_entries = meta_1.location.as_ref().heap_size(&mut ctx)
            + value_1.heap_size(&mut ctx)
            + meta_2.location.as_ref().heap_size(&mut ctx)
            + value_2.heap_size(&mut ctx);

        // create a cache with a limit which fits exactly 2 entries
        let cache = DefaultFileStatisticsCache::new(limit_for_2_entries);
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
        let (meta, value) = create_cached_file_metadata_with_stats("test1.parquet");
        let mut ctx = DFHeapSizeCtx::default();
        let limit_less_than_the_entry = value.heap_size(&mut ctx) - 1;

        // create a cache with a size less than the entry
        let cache = DefaultFileStatisticsCache::new(limit_less_than_the_entry);

        let path_1 = TableScopedPath {
            path: meta.location.clone(),
            table: None,
        };

        cache.put(&path_1, value);

        assert_eq!(cache.len(), 0);
        assert_eq!(cache.memory_used(), 0);
    }

    fn create_cached_file_metadata_with_stats(
        file_name: &str,
    ) -> (ObjectMeta, CachedFileMetadata) {
        let series: Vec<i32> = (0..=10).collect();
        let values = Int32Array::from(series);
        let offsets = OffsetBuffer::new(ScalarBuffer::from(vec![0, 11]));
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
}
