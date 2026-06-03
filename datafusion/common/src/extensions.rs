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

//! A type-keyed map of opaque, `Arc`'d objects.
//!
//! Used as the backing store for the various `extensions` fields throughout
//! DataFusion (e.g. [`SessionConfig`], [`ExtendedStatistics`],
//! [`PartitionedFile`]) so that independent components can each attach
//! their own data without conflict, each keyed by its concrete Rust type.
//!
//! [`SessionConfig`]: https://docs.rs/datafusion-execution/latest/datafusion_execution/config/struct.SessionConfig.html
//! [`ExtendedStatistics`]: https://docs.rs/datafusion-physical-plan/latest/datafusion_physical_plan/operator_statistics/struct.ExtendedStatistics.html
//! [`PartitionedFile`]: https://docs.rs/datafusion-datasource/latest/datafusion_datasource/struct.PartitionedFile.html

use std::any::{Any, TypeId};
use std::collections::HashMap;
use std::hash::{BuildHasherDefault, Hasher};
use std::sync::Arc;

/// A type-keyed map of opaque `Arc`'d values. Each Rust type `T` occupies
/// its own slot, so independent components can each attach their own data
/// without conflict.
///
/// Cloning is cheap: the backing values are reference-counted.
///
/// # Example
///
/// ```
/// # use std::sync::Arc;
/// # use datafusion_common::extensions::Extensions;
/// struct MyData(u32);
/// struct OtherData(&'static str);
///
/// let mut ext = Extensions::new();
/// ext.insert(MyData(42));
/// ext.insert_arc(Arc::new(OtherData("hello")));
///
/// assert_eq!(ext.get::<MyData>().unwrap().0, 42);
/// assert_eq!(ext.get::<OtherData>().unwrap().0, "hello");
/// ```
#[derive(Debug, Clone, Default)]
pub struct Extensions {
    inner: HashMap<TypeId, Arc<dyn Any + Send + Sync>, BuildHasherDefault<IdHasher>>,
}

impl Extensions {
    /// Create an empty map.
    pub fn new() -> Self {
        Self::default()
    }

    /// Returns true if no extensions are set.
    pub fn is_empty(&self) -> bool {
        self.inner.is_empty()
    }

    /// Number of extensions set.
    pub fn len(&self) -> usize {
        self.inner.len()
    }

    /// Insert an extension keyed by its concrete type `T`. Returns the
    /// previous value of that type, if any.
    ///
    /// The value is wrapped in an [`Arc`] internally. If the caller already
    /// has an `Arc<T>` and wants to avoid an extra allocation, use
    /// [`Self::insert_arc`].
    pub fn insert<T: Any + Send + Sync>(&mut self, value: T) -> Option<Arc<T>> {
        self.insert_arc(Arc::new(value))
    }

    /// Insert an extension keyed by its concrete type `T`, taking an
    /// already-allocated [`Arc<T>`]. Returns the previous value of that type,
    /// if any.
    pub fn insert_arc<T: Any + Send + Sync>(&mut self, value: Arc<T>) -> Option<Arc<T>> {
        self.inner
            .insert(TypeId::of::<T>(), value)
            .map(|p| Arc::downcast::<T>(p).expect("TypeId matches T"))
    }

    /// Insert an already-type-erased value, keyed by its dynamic
    /// [`TypeId`]. Used internally to support APIs that accept
    /// `Arc<dyn Any + Send + Sync>` for backwards compatibility and need
    /// to recover the concrete type for keying.
    ///
    /// New code should use [`Self::insert`] or [`Self::insert_arc`], which
    /// preserve the concrete type at the call site.
    #[deprecated(
        since = "54.0.0",
        note = "use `insert` or `insert_arc`; only retained to support the deprecated `PartitionedFile::with_extensions` shim"
    )]
    pub fn insert_dyn(
        &mut self,
        value: Arc<dyn Any + Send + Sync>,
    ) -> Option<Arc<dyn Any + Send + Sync>> {
        let id = (*value).type_id();
        self.inner.insert(id, value)
    }

    /// Borrow the extension of type `T`, if set.
    pub fn get<T: Any + Send + Sync>(&self) -> Option<&T> {
        self.inner
            .get(&TypeId::of::<T>())
            .and_then(|a| a.downcast_ref::<T>())
    }

    /// Get a cloned `Arc<T>` of the extension, if set.
    pub fn get_arc<T: Any + Send + Sync>(&self) -> Option<Arc<T>> {
        self.inner
            .get(&TypeId::of::<T>())
            .map(|a| Arc::downcast::<T>(Arc::clone(a)).expect("TypeId matches T"))
    }

    /// Returns true if an extension of type `T` is set.
    pub fn contains<T: Any + Send + Sync>(&self) -> bool {
        self.inner.contains_key(&TypeId::of::<T>())
    }

    /// Merge entries from `other` into `self`. Entries in `other` take
    /// precedence over existing entries with the same type.
    pub fn merge(&mut self, other: &Extensions) {
        for (id, ext) in &other.inner {
            self.inner.insert(*id, Arc::clone(ext));
        }
    }
}

/// Hasher specialized for [`TypeId`] keys. Since `TypeId` is already a
/// hash produced by the compiler, we don't need to hash it again — we
/// just store the `u64` it writes and return it unchanged.
#[derive(Default)]
struct IdHasher(u64);

impl Hasher for IdHasher {
    fn write(&mut self, _: &[u8]) {
        unreachable!("TypeId calls write_u64");
    }

    #[inline]
    fn write_u64(&mut self, id: u64) {
        self.0 = id;
    }

    #[inline]
    fn finish(&self) -> u64 {
        self.0
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[derive(Debug, PartialEq)]
    struct A(u32);

    #[derive(Debug, PartialEq)]
    struct B(&'static str);

    #[test]
    fn insert_get_replace() {
        let mut ext = Extensions::new();
        assert!(ext.is_empty());

        ext.insert(A(1));
        ext.insert_arc(Arc::new(B("x")));
        assert_eq!(ext.len(), 2);
        assert_eq!(ext.get::<A>(), Some(&A(1)));
        assert_eq!(ext.get::<B>(), Some(&B("x")));
        assert!(ext.contains::<A>());

        let prev = ext.insert(A(2));
        assert_eq!(prev.as_deref(), Some(&A(1)));
        assert_eq!(ext.get::<A>(), Some(&A(2)));
    }

    #[test]
    #[expect(deprecated)]
    fn insert_dyn_keys_by_concrete_type() {
        let mut ext = Extensions::new();
        let erased: Arc<dyn Any + Send + Sync> = Arc::new(A(7));
        ext.insert_dyn(erased);
        assert_eq!(ext.get::<A>(), Some(&A(7)));
    }

    #[test]
    fn merge_other_wins() {
        let mut a = Extensions::new();
        a.insert(A(1));
        let mut b = Extensions::new();
        b.insert(A(2));
        b.insert(B("hi"));
        a.merge(&b);
        assert_eq!(a.get::<A>(), Some(&A(2)));
        assert_eq!(a.get::<B>(), Some(&B("hi")));
    }
}
