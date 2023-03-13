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

use std::{
    any::{Any, TypeId},
    collections::HashMap,
    hash::{BuildHasherDefault, Hasher},
    sync::Arc,
};

/// Map that holds opaque objects indexed by their type.
///
/// Data is wrapped into an [`Arc`] to enable [`Clone`] while still being [object safe].
///
/// [object safe]: https://doc.rust-lang.org/reference/items/traits.html#object-safety
pub type AnyMap =
    HashMap<TypeId, Arc<dyn Any + Send + Sync + 'static>, BuildHasherDefault<IdHasher>>;

/// Hasher for [`AnyMap`].
///
/// With [`TypeId`]s as keys, there's no need to hash them. They are already hashes themselves, coming from the compiler.
/// The [`IdHasher`] just holds the [`u64`] of the [`TypeId`], and then returns it, instead of doing any bit fiddling.
#[derive(Default)]
pub struct IdHasher(u64);

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
