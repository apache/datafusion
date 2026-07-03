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

use datafusion_common::heap_size::{DFHeapSize, DFHeapSizeCtx};
use datafusion_common::instant::Instant;
use datafusion_common::{HashMap, TableReference};
use object_store::path::Path;
use std::fmt::{Debug, Display, Formatter};
use std::hash::Hash;
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
