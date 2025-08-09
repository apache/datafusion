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

use std::any::Any;
use std::hash::{Hash, Hasher};

/// A dyn-compatible version of [`Eq`] trait.
/// The implementation constraints for this trait are the same as for [`Eq`]:
/// the implementation must be reflexive, symmetric, and transitive.
/// Additionally, if two values can be compared with [`DynEq`] and [`PartialEq`] then
/// they must be [`DynEq`]-equal if and only if they are [`PartialEq`]-equal.
/// It is therefore strongly discouraged to implement this trait for types
/// that implement `PartialEq<Other>` or `Eq<Other>` for any type `Other` other than `Self`.
///
/// Note: This trait should not be implemented directly. Implement `Eq` and `Any` and use
/// the blanket implementation.
pub trait DynEq {
    fn dyn_eq(&self, other: &dyn Any) -> bool;

    fn i_did_not_implement_the_trait_directly_but_using_the_blanked_impl_instead()
    where
        Self: Sized;
}

impl<T: Eq + Any> DynEq for T {
    fn dyn_eq(&self, other: &dyn Any) -> bool {
        other.downcast_ref::<Self>() == Some(self)
    }

    fn i_did_not_implement_the_trait_directly_but_using_the_blanked_impl_instead() {}
}

/// A dyn-compatible version of [`Hash`] trait.
/// If two values are equal according to [`DynEq`], they must produce the same hash value.
///
/// Note: This trait should not be implemented directly. Implement `Hash` and `Any` and use
/// the blanket implementation.
pub trait DynHash {
    fn dyn_hash(&self, _state: &mut dyn Hasher);

    fn i_did_not_implement_the_trait_directly_but_using_the_blanked_impl_instead()
    where
        Self: Sized;
}

impl<T: Hash + Any> DynHash for T {
    fn dyn_hash(&self, mut state: &mut dyn Hasher) {
        self.type_id().hash(&mut state);
        self.hash(&mut state)
    }

    fn i_did_not_implement_the_trait_directly_but_using_the_blanked_impl_instead() {}
}
