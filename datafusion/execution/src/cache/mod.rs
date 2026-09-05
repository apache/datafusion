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

pub mod cache_manager;
pub mod lru_queue;

pub mod default_cache;

use datafusion_common::arrow::datatypes::{DataType, Schema};
use datafusion_common::heap_size::{DFHeapSize, DFHeapSizeCtx};
use datafusion_common::instant::Instant;
use datafusion_common::{HashMap, TableReference};
use object_store::path::Path;
use std::collections::hash_map::DefaultHasher;
use std::fmt::{Debug, Display, Formatter};
use std::hash::{Hash, Hasher};
use std::time::Duration;

/// Base trait for cache implementations with common operations.
///
/// This trait provides the fundamental cache operations (`get`, `put`, `remove`, etc.)
/// that all cache types share.
///
/// ## Thread Safety
///
/// Implementations must handle their own locking via internal mutability, as methods do not
/// take mutable references and may be accessed by multiple concurrent queries.
///
pub trait Cache<K: CacheKey, V: CacheValue>: Send + Sync {
    /// Get a cached entry if it exists.
    fn get(&self, key: &K) -> Option<V>;

    /// Store a value in the cache.
    ///
    /// Returns the previous value if one existed.
    fn put(&self, key: &K, value: V) -> Option<V>;

    /// Remove an entry from the cache, returning the value if it existed.
    fn remove(&self, k: &K) -> Option<V>;

    /// Check if the cache contains a specific key.
    fn contains_key(&self, k: &K) -> bool;

    /// Fetch the total number of cache entries.
    fn len(&self) -> usize;

    /// Check if the cache collection is empty.
    fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Remove all entries from the cache.
    fn clear(&self);

    /// Return the cache name.
    fn name(&self) -> String;

    /// Current memory budget, in bytes.
    fn cache_limit(&self) -> usize;

    /// Change the memory budget in bytes.
    fn update_cache_limit(&self, limit: usize);

    /// Time-to-live applied to newly inserted entries, or `None` if entries
    /// never expire on their own.
    fn cache_ttl(&self) -> Option<Duration>;

    /// Change the TTL applied to subsequent inserts.
    fn update_cache_ttl(&self, _ttl: Option<Duration>);

    /// Invalidate every entry associated with `table_ref`.
    fn drop_table_entries(
        &self,
        table_ref: &TableReference,
    ) -> datafusion_common::Result<()>;

    /// Snapshot of all current entries with per-entry metadata (size, hits,
    /// expiration) for diagnostics and observability.
    fn list_entries(&self) -> HashMap<K, CacheEntryInfo<V>>;

    /// Aggregate counters describing how this cache has behaved so far, or
    /// `None` if the implementation does not track them.
    ///
    /// Unlike [`Cache::list_entries`], which only describes entries that are
    /// currently resident, these counters cover the whole lifetime of the cache
    /// and therefore survive eviction. They are what makes a thrashing cache
    /// (0% hit rate because the working set does not fit the byte budget)
    /// diagnosable.
    ///
    /// The default implementation returns `None` so that existing external
    /// implementations of this trait keep compiling; `None` means
    /// "not instrumented", which a reporting tool can distinguish from an
    /// instrumented cache that is genuinely all zeros.
    fn statistics(&self) -> Option<CacheStatistics> {
        None
    }
}

/// Aggregate, lifetime counters for a [`Cache`].
///
/// Obtained via [`Cache::statistics`]. All counters are monotonic for the
/// lifetime of the cache: they are not reset by [`Cache::clear`].
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct CacheStatistics {
    /// Lookups ([`Cache::get`]) that returned a value.
    pub hits: u64,
    /// Lookups ([`Cache::get`]) that did not return a value, either because the
    /// key was absent or because the entry had expired.
    pub misses: u64,
    /// Entries dropped to stay within the memory budget.
    ///
    /// This counts only capacity-driven eviction (including eviction triggered
    /// by lowering the limit via [`Cache::update_cache_limit`]). Explicit
    /// invalidation through [`Cache::remove`], [`Cache::clear`] or
    /// [`Cache::drop_table_entries`], and replacing an entry with a new value
    /// for the same key, are not evictions.
    pub evictions: u64,
    /// Total size, in bytes, of the entries counted by `evictions`.
    pub bytes_evicted: u64,
    /// Inserts rejected because the entry on its own was larger than the whole
    /// cache limit.
    ///
    /// A non-zero value means the cache can never hold that entry, no matter
    /// how much of it is free — a distinct failure mode from ordinary eviction
    /// pressure, and one that is worth surfacing on its own.
    pub inserts_rejected_too_large: u64,
}

impl CacheStatistics {
    /// Total number of lookups, i.e. `hits + misses`.
    pub fn lookups(&self) -> u64 {
        self.hits.saturating_add(self.misses)
    }

    /// Fraction of lookups that were hits, or `None` if there were no lookups.
    pub fn hit_rate(&self) -> Option<f64> {
        let lookups = self.lookups();
        (lookups > 0).then(|| self.hits as f64 / lookups as f64)
    }
}

/// Key type for entries stored in a [`Cache`].
pub trait CacheKey: Clone + Eq + Hash + Send + Sync + Debug {
    /// Size of the key in bytes, used for cache memory accounting.
    fn size(&self) -> usize;

    /// Table this key is associated with, or `None` if the key is not
    /// table-scoped.
    fn table_ref(&self) -> Option<&TableReference>;
}

/// Value type for entries stored in a [`Cache`].
pub trait CacheValue: Clone + Send + Sync {
    /// Size of the value in bytes used for cache memory accounting.
    fn size(&self) -> usize;
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CacheEntryInfo<V> {
    pub value: V,
    pub size_bytes: usize,
    pub hits: usize,
    pub expires: Option<Instant>,
}

impl<K: CacheKey, V: CacheValue> Debug for dyn Cache<K, V> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "Cache name: {} with length: {}", self.name(), self.len())
    }
}

impl CacheKey for Path {
    fn size(&self) -> usize {
        self.as_ref().heap_size(&mut DFHeapSizeCtx::default())
    }

    fn table_ref(&self) -> Option<&TableReference> {
        None
    }
}

impl CacheKey for TableScopedPath {
    fn size(&self) -> usize {
        DFHeapSize::heap_size(self, &mut DFHeapSizeCtx::default())
    }

    fn table_ref(&self) -> Option<&TableReference> {
        self.table.as_ref()
    }
}

/// Each entry is scoped to its use within a specific table so that the cache
/// can differentiate between identical paths in different tables, and
/// table-level cache invalidation.
#[derive(PartialEq, Eq, Hash, Clone, Debug)]
pub struct TableScopedPath {
    pub table: Option<TableReference>,
    pub path: Path,
}

impl DFHeapSize for TableScopedPath {
    fn heap_size(&self, ctx: &mut DFHeapSizeCtx) -> usize {
        self.path.as_ref().heap_size(ctx) + self.table.heap_size(ctx)
    }
}

impl Display for TableScopedPath {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        if let Some(table) = &self.table {
            write!(f, "{}, {}", self.path, table)
        } else {
            write!(f, "{}", self.path)
        }
    }
}

/// A fingerprint of the `file_schema` used to compute a file's statistics.
///
/// Captures exactly the attributes that determine the layout and meaning of
/// `Statistics::column_statistics`: each column's name, data type and
/// nullability, in order. It deliberately excludes field/schema metadata, which
/// cannot affect statistics — including it would needlessly fragment the cache.
#[derive(Clone, Debug)]
pub struct SchemaFingerprint {
    columns: Vec<(String, DataType, bool)>,
    /// Precomputed hash of `columns`, so hashing a key on every cache lookup is
    /// O(1) rather than O(schema width). Computed once in `from_schema` with a
    /// fixed-seed hasher so it is stable across keys; `PartialEq` still compares
    /// `columns` exactly, so a hash collision can never make two different
    /// schemas share a cache entry.
    hash: u64,
}

impl SchemaFingerprint {
    /// Builds a fingerprint from the `file_schema` used to compute statistics
    /// (the schema of the columns physically read, not the full table schema —
    /// partition columns and their statistics are handled separately).
    pub fn from_schema(file_schema: &Schema) -> Self {
        let columns: Vec<(String, DataType, bool)> = file_schema
            .fields()
            .iter()
            .map(|f| (f.name().clone(), f.data_type().clone(), f.is_nullable()))
            .collect();
        let mut hasher = DefaultHasher::new();
        columns.hash(&mut hasher);
        Self {
            columns,
            hash: hasher.finish(),
        }
    }
}

impl PartialEq for SchemaFingerprint {
    fn eq(&self, other: &Self) -> bool {
        // Cheap hash gate first, then an exact comparison so collisions are safe.
        self.hash == other.hash && self.columns == other.columns
    }
}

impl Eq for SchemaFingerprint {}

impl Hash for SchemaFingerprint {
    fn hash<H: Hasher>(&self, state: &mut H) {
        state.write_u64(self.hash);
    }
}

impl DFHeapSize for SchemaFingerprint {
    fn heap_size(&self, ctx: &mut DFHeapSizeCtx) -> usize {
        self.columns.heap_size(ctx)
    }
}

#[cfg(test)]
mod cache_statistics_tests {
    use super::*;

    #[derive(Clone)]
    struct Unit;

    impl CacheValue for Unit {
        fn size(&self) -> usize {
            0
        }
    }

    /// A [`Cache`] implementation that does not override [`Cache::statistics`],
    /// standing in for an external implementation written before the counters
    /// existed. It must keep compiling, and report that it is uninstrumented.
    struct UninstrumentedCache;

    impl Cache<Path, Unit> for UninstrumentedCache {
        fn get(&self, _key: &Path) -> Option<Unit> {
            None
        }
        fn put(&self, _key: &Path, _value: Unit) -> Option<Unit> {
            None
        }
        fn remove(&self, _k: &Path) -> Option<Unit> {
            None
        }
        fn contains_key(&self, _k: &Path) -> bool {
            false
        }
        fn len(&self) -> usize {
            0
        }
        fn clear(&self) {}
        fn name(&self) -> String {
            "UninstrumentedCache".to_string()
        }
        fn cache_limit(&self) -> usize {
            0
        }
        fn update_cache_limit(&self, _limit: usize) {}
        fn cache_ttl(&self) -> Option<Duration> {
            None
        }
        fn update_cache_ttl(&self, _ttl: Option<Duration>) {}
        fn drop_table_entries(
            &self,
            _table_ref: &TableReference,
        ) -> datafusion_common::Result<()> {
            Ok(())
        }
        fn list_entries(&self) -> HashMap<Path, CacheEntryInfo<Unit>> {
            HashMap::new()
        }
    }

    #[test]
    fn uninstrumented_cache_reports_no_statistics() {
        assert_eq!(UninstrumentedCache.statistics(), None);
    }

    #[test]
    fn hit_rate_and_lookups() {
        assert_eq!(CacheStatistics::default().hit_rate(), None);
        assert_eq!(CacheStatistics::default().lookups(), 0);

        let stats = CacheStatistics {
            hits: 1,
            misses: 3,
            ..Default::default()
        };
        assert_eq!(stats.lookups(), 4);
        assert_eq!(stats.hit_rate(), Some(0.25));
    }
}

#[cfg(test)]
mod schema_fingerprint_tests {
    use super::*;
    use datafusion_common::arrow::datatypes::Field;

    fn fp(fields: Vec<Field>) -> SchemaFingerprint {
        SchemaFingerprint::from_schema(&Schema::new(fields))
    }

    /// `from_schema` must capture nullability and field order — the two
    /// attributes most easily dropped by a wrong implementation.
    #[test]
    fn fingerprint_captures_nullability_and_order() {
        assert_ne!(
            fp(vec![Field::new("id", DataType::Int64, false)]),
            fp(vec![Field::new("id", DataType::Int64, true)]),
            "nullability must affect the fingerprint",
        );

        let ab = fp(vec![
            Field::new("a", DataType::Int64, false),
            Field::new("b", DataType::Utf8, true),
        ]);
        let ba = fp(vec![
            Field::new("b", DataType::Utf8, true),
            Field::new("a", DataType::Int64, false),
        ]);
        assert_ne!(ab, ba, "field order must affect the fingerprint");
    }

    /// Metadata must NOT affect the fingerprint: it cannot change column
    /// statistics, so including it would needlessly fragment the cache.
    #[test]
    fn fingerprint_ignores_metadata() {
        let plain = fp(vec![Field::new("id", DataType::Int64, false)]);

        let field_md = SchemaFingerprint::from_schema(&Schema::new(vec![
            Field::new("id", DataType::Int64, false)
                .with_metadata([("note".to_string(), "x".to_string())].into()),
        ]));
        assert_eq!(plain, field_md, "field metadata must be ignored");

        let schema_md = SchemaFingerprint::from_schema(
            &Schema::new(vec![Field::new("id", DataType::Int64, false)])
                .with_metadata([("k".to_string(), "v".to_string())].into()),
        );
        assert_eq!(plain, schema_md, "schema metadata must be ignored");
    }
}
