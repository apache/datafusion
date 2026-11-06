// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: See the NOTICE file distributed with this work for additional information regarding copyright ownership
// SPDX-FileContributor: Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements

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
#[expect(private_bounds)]
pub trait DynEq: private::EqSealed {
    fn dyn_eq(&self, other: &dyn Any) -> bool;
}

impl<T: Eq + Any> private::EqSealed for T {}
impl<T: Eq + Any> DynEq for T {
    fn dyn_eq(&self, other: &dyn Any) -> bool {
        other.downcast_ref::<Self>() == Some(self)
    }
}

/// A dyn-compatible version of [`Hash`] trait.
/// If two values are equal according to [`DynEq`], they must produce the same hash value.
///
/// Note: This trait should not be implemented directly. Implement `Hash` and `Any` and use
/// the blanket implementation.
#[expect(private_bounds)]
pub trait DynHash: private::HashSealed {
    fn dyn_hash(&self, _state: &mut dyn Hasher);
}

impl<T: Hash + Any> private::HashSealed for T {}
impl<T: Hash + Any> DynHash for T {
    fn dyn_hash(&self, mut state: &mut dyn Hasher) {
        self.type_id().hash(&mut state);
        self.hash(&mut state)
    }
}

mod private {
    pub(super) trait EqSealed {}
    pub(super) trait HashSealed {}
}
