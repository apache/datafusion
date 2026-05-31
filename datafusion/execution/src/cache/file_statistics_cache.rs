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
        CachedFileMetadata, DEFAULT_FILE_STATISTICS_MEMORY_LIMIT,
    };
    use crate::cache::default_cache::DefaultCache;
    use crate::cache::{Cache, CacheEntryInfo, TableScopedPath};
    use arrow::array::{Int32Array, ListArray, RecordBatch};
    use arrow::buffer::{OffsetBuffer, ScalarBuffer};
    use arrow::datatypes::{DataType, Field, Schema, TimeUnit};
    use chrono::DateTime;
    use datafusion_common::heap_size::{DFHeapSize, DFHeapSizeCtx};
    use datafusion_common::stats::Precision;
    use datafusion_common::{ColumnStatistics, HashMap, ScalarValue, Statistics};
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
        let cache = DefaultCache::new(DEFAULT_FILE_STATISTICS_MEMORY_LIMIT);

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
        assert_eq!(entry.value.meta.size, 2048); // Should be updated value
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
        let cache = DefaultCache::new(DEFAULT_FILE_STATISTICS_MEMORY_LIMIT);

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
        assert!(entries.get(&path).unwrap().value.ordering.is_some());
    }

    #[test]
    fn test_cache_invalidation_on_file_modification() {
        let cache = DefaultCache::new(DEFAULT_FILE_STATISTICS_MEMORY_LIMIT);
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
        let cache = DefaultCache::new(DEFAULT_FILE_STATISTICS_MEMORY_LIMIT);
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
        let cache = DefaultCache::new(DEFAULT_FILE_STATISTICS_MEMORY_LIMIT);
        let schema = Schema::new(vec![Field::new("a", DataType::Int32, false)]);

        let meta1 = create_test_meta("test1.parquet", 100);

        let cached_value_1 = CachedFileMetadata::new(
            meta1.clone(),
            Arc::new(Statistics::new_unknown(&schema)),
            None,
        );

        let path_1 = TableScopedPath {
            path: meta1.location.clone(),
            table: None,
        };

        cache.put(&path_1, cached_value_1.clone());
        let meta2 = create_test_meta("test2.parquet", 200);
        let cached_value_2 = CachedFileMetadata::new(
            meta2.clone(),
            Arc::new(Statistics::new_unknown(&schema)),
            Some(ordering()),
        );

        let path_2 = TableScopedPath {
            path: meta2.location.clone(),
            table: None,
        };

        cache.put(&path_2, cached_value_2.clone());

        let entries = cache.list_entries();
        assert_eq!(
            entries,
            HashMap::from([
                (
                    path_1,
                    CacheEntryInfo {
                        value: cached_value_1,
                        hits: 0,
                        size_bytes: 373,
                        expires: None,
                    }
                ),
                (
                    path_2,
                    CacheEntryInfo {
                        value: cached_value_2,
                        hits: 0,
                        size_bytes: 373,
                        expires: None,
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
        let cache = DefaultCache::new(limit_for_2_entries);
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
        let cache = DefaultCache::new(limit_less_than_the_entry);

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
