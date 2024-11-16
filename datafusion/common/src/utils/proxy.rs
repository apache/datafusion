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

//! [`VecAllocExt`] and [`RawTableAllocExt`] to help tracking of memory allocations

use hashbrown::raw::{Bucket, RawTable};
use std::mem::size_of;

/// Extension trait for [`Vec`] to account for allocations.
pub trait VecAllocExt {
    /// Item type.
    type T;

    /// [Push](Vec::push) new element to vector and increase
    /// `accounting` by any newly allocated bytes.
    ///
    /// Note that allocation counts  capacity, not size
    ///
    /// # Example:
    /// ```
    /// # use datafusion_common::utils::proxy::VecAllocExt;
    /// // use allocated to incrementally track how much memory is allocated in the vec
    /// let mut allocated = 0;
    /// let mut vec = Vec::new();
    /// // Push data into the vec and the accounting will be updated to reflect
    /// // memory allocation
    /// vec.push_accounted(1, &mut allocated);
    /// assert_eq!(allocated, 16); // space for 4 u32s
    /// vec.push_accounted(1, &mut allocated);
    /// assert_eq!(allocated, 16); // no new allocation needed
    ///
    /// // push more data into the vec
    /// for _ in 0..10 { vec.push_accounted(1, &mut allocated); }
    /// assert_eq!(allocated, 64); // underlying vec has space for 10 u32s
    /// assert_eq!(vec.allocated_size(), 64);
    /// ```
    /// # Example with other allocations:
    /// ```
    /// # use datafusion_common::utils::proxy::VecAllocExt;
    /// // You can use the same allocated size to track memory allocated by
    /// // another source. For example
    /// let mut allocated = 27;
    /// let mut vec = Vec::new();
    /// vec.push_accounted(1, &mut allocated); // allocates 16 bytes for vec
    /// assert_eq!(allocated, 43); // 16 bytes for vec, 27 bytes for other
    /// ```
    fn push_accounted(&mut self, x: Self::T, accounting: &mut usize);

    /// Return the amount of memory allocated by this Vec to store elements
    /// (`size_of<T> * capacity`).
    ///
    /// Note this calculation is not recursive, and does not include any heap
    /// allocations contained within the Vec's elements. Does not include the
    /// size of `self`
    ///
    /// # Example:
    /// ```
    /// # use datafusion_common::utils::proxy::VecAllocExt;
    /// let mut vec = Vec::new();
    /// // Push data into the vec and the accounting will be updated to reflect
    /// // memory allocation
    /// vec.push(1);
    /// assert_eq!(vec.allocated_size(), 16); // space for 4 u32s
    /// vec.push(1);
    /// assert_eq!(vec.allocated_size(), 16); // no new allocation needed
    ///
    /// // push more data into the vec
    /// for _ in 0..10 { vec.push(1); }
    /// assert_eq!(vec.allocated_size(), 64); // space for 64 now
    /// ```
    fn allocated_size(&self) -> usize;
}

impl<T> VecAllocExt for Vec<T> {
    type T = T;

    fn push_accounted(&mut self, x: Self::T, accounting: &mut usize) {
        let prev_capacty = self.capacity();
        self.push(x);
        let new_capacity = self.capacity();
        if new_capacity > prev_capacty {
            // capacity changed, so we allocated more
            let bump_size = (new_capacity - prev_capacty) * size_of::<T>();
            // Note multiplication should never overflow because `push` would
            // have panic'd first, but the checked_add could potentially
            // overflow since accounting could be tracking additional values, and
            // could be greater than what is stored in the Vec
            *accounting = (*accounting).checked_add(bump_size).expect("overflow");
        }
    }
    fn allocated_size(&self) -> usize {
        size_of::<T>() * self.capacity()
    }
}

/// Extension trait for hash browns [`RawTable`] to account for allocations.
pub trait RawTableAllocExt {
    /// Item type.
    type T;

    /// [Insert](RawTable::insert) new element into table and increase
    /// `accounting` by any newly allocated bytes.
    ///
    /// Returns the bucket where the element was inserted.
    /// Note that allocation counts capacity, not size.
    ///
    /// # Example:
    /// ```
    /// # use datafusion_common::utils::proxy::RawTableAllocExt;
    /// # use hashbrown::raw::RawTable;
    /// let mut table = RawTable::new();
    /// let mut allocated = 0;
    /// let hash_fn = |x: &u32| (*x as u64) % 1000;
    /// // pretend 0x3117 is the hash value for 1
    /// table.insert_accounted(1, hash_fn, &mut allocated);
    /// assert_eq!(allocated, 64);
    ///
    /// // insert more values
    /// for i in 0..100 { table.insert_accounted(i, hash_fn, &mut allocated); }
    /// assert_eq!(allocated, 400);
    /// ```
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

                let bump_elements = self.capacity().max(16);
                let bump_size = bump_elements * size_of::<T>();
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
