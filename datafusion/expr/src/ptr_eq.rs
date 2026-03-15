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

use std::fmt::Debug;
use std::hash::{Hash, Hasher};
use std::ops::Deref;
use std::sync::Arc;

/// Compares two `Arc` pointers for equality based on their underlying pointers values.
/// This is not equivalent to [`Arc::ptr_eq`] for fat pointers, see that method
/// for more information.
pub fn arc_ptr_eq<T: ?Sized>(a: &Arc<T>, b: &Arc<T>) -> bool {
    std::ptr::eq(Arc::as_ptr(a), Arc::as_ptr(b))
}

/// Hashes an `Arc` pointer based on its underlying pointer value.
/// The general contract for this function is that if [`arc_ptr_eq`] returns `true`
/// for two `Arc`s, then this function should return the same hash value for both.
pub fn arc_ptr_hash<T: ?Sized>(a: &Arc<T>, hasher: &mut impl Hasher) {
    std::ptr::hash(Arc::as_ptr(a), hasher)
}

/// A wrapper around a pointer that implements `Eq` and `Hash` comparing
/// the underlying pointer address.
///
/// If you have pointers to a `dyn UDF impl` consider using [`super::udf_eq::UdfEq`].
#[derive(Clone)]
#[expect(private_bounds)] // This is so that PtrEq can only be used with allowed pointer types (e.g. Arc), without allowing misuse.
pub struct PtrEq<Ptr: PointerType>(Ptr);

impl<T> PartialEq for PtrEq<Arc<T>>
where
    T: ?Sized,
{
    fn eq(&self, other: &Self) -> bool {
        arc_ptr_eq(&self.0, &other.0)
    }
}
impl<T> Eq for PtrEq<Arc<T>> where T: ?Sized {}

impl<T> Hash for PtrEq<Arc<T>>
where
    T: ?Sized,
{
    fn hash<H: Hasher>(&self, state: &mut H) {
        arc_ptr_hash(&self.0, state);
    }
}

impl<Ptr> From<Ptr> for PtrEq<Ptr>
where
    Ptr: PointerType,
{
    fn from(ptr: Ptr) -> Self {
        PtrEq(ptr)
    }
}

impl<T> From<PtrEq<Arc<T>>> for Arc<T>
where
    T: ?Sized,
{
    fn from(wrapper: PtrEq<Arc<T>>) -> Self {
        wrapper.0
    }
}

impl<Ptr> Debug for PtrEq<Ptr>
where
    Ptr: PointerType + Debug,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.0.fmt(f)
    }
}

impl<Ptr> Deref for PtrEq<Ptr>
where
    Ptr: PointerType,
{
    type Target = Ptr;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

trait PointerType {}
impl<T> PointerType for Arc<T> where T: ?Sized {}

#[cfg(test)]
mod tests {
    use super::*;
    use std::hash::DefaultHasher;

    #[test]
    pub fn test_ptr_eq_wrapper() {
        let a = Arc::new("Hello".to_string());
        let b = Arc::new(a.deref().clone());
        let c = Arc::new("world".to_string());

        let wrapper = PtrEq(Arc::clone(&a));
        assert_eq!(wrapper, wrapper);

        // same address (equal)
        assert_eq!(PtrEq(Arc::clone(&a)), PtrEq(Arc::clone(&a)));
        assert_eq!(hash(PtrEq(Arc::clone(&a))), hash(PtrEq(Arc::clone(&a))));

        // different address, same content (not equal)
        assert_ne!(PtrEq(Arc::clone(&a)), PtrEq(Arc::clone(&b)));

        // different address, different content (not equal)
        assert_ne!(PtrEq(Arc::clone(&a)), PtrEq(Arc::clone(&c)));
    }

    fn hash<T: Hash>(value: T) -> u64 {
        let hasher = &mut DefaultHasher::new();
        value.hash(hasher);
        hasher.finish()
    }
}
