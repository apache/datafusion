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

//! Utilities that help with tracking of memory allocations.

use hashbrown::raw::{Bucket, RawTable};

/// Extension trait for [`Vec`] to account for allocations.
pub trait VecAllocExt {
    /// Item type.
    type T;

    /// [Push](Vec::push) new element to vector and store additional allocated bytes in `accounting` (additive).
    fn push_accounted(&mut self, x: Self::T, accounting: &mut usize);
}

impl<T> VecAllocExt for Vec<T> {
    type T = T;

    fn push_accounted(&mut self, x: Self::T, accounting: &mut usize) {
        if self.capacity() == self.len() {
            // allocate more

            // growth factor: 2, but at least 2 elements
            let bump_elements = (self.capacity() * 2).max(2);
            let bump_size = std::mem::size_of::<u32>() * bump_elements;
            self.reserve(bump_elements);
            *accounting = (*accounting).checked_add(bump_size).expect("overflow");
        }

        self.push(x);
    }
}

/// Extension trait for [`RawTable`] to account for allocations.
pub trait RawTableAllocExt {
    /// Item type.
    type T;

    /// [Insert](RawTable::insert) new element into table and store additional allocated bytes in `accounting` (additive).
    fn insert_accounted(
        &mut self,
        x: Self::T,
        hasher: impl Fn(&Self::T) -> u64,
        accounting: &mut usize,
    ) -> Bucket<Self::T>;
}

impl<T> RawTableAllocExt for RawTable<T> {
    type T = T;

    fn insert_accounted(
        &mut self,
        x: Self::T,
        hasher: impl Fn(&Self::T) -> u64,
        accounting: &mut usize,
    ) -> Bucket<Self::T> {
        let hash = hasher(&x);

        match self.try_insert_no_grow(hash, x) {
            Ok(bucket) => bucket,
            Err(x) => {
                // need to request more memory

                let bump_elements = (self.capacity() * 2).max(16);
                let bump_size = bump_elements * std::mem::size_of::<T>();
                *accounting = (*accounting).checked_add(bump_size).expect("overflow");

                self.reserve(bump_elements, hasher);

                // still need to insert the element since first try failed
                // Note: cannot use `.expect` here because `T` may not implement `Debug`
                match self.try_insert_no_grow(hash, x) {
                    Ok(bucket) => bucket,
                    Err(_) => panic!("just grew the container"),
                }
            }
        }
    }
}
