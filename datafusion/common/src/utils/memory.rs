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

//! This module provides a function to estimate the memory size of a HashTable prior to allocation

use crate::Result;
use crate::error::_exec_datafusion_err;
use std::mem::size_of;

/// Estimates the memory size required for a hash table prior to allocation.
///
/// # Parameters
/// - `num_elements`: The number of elements expected in the hash table.
/// - `fixed_size`: A fixed overhead size associated with the collection
///   (e.g., HashSet or HashTable).
/// - `T`: The type of elements stored in the hash table.
///
/// # Details
/// This function calculates the estimated memory size by considering:
/// - An overestimation of buckets to keep approximately 1/8 of them empty.
/// - The total memory size is computed as:
///   - The size of each entry (`T`) multiplied by the estimated number of
///     buckets.
///   - One byte overhead for each bucket.
///   - The fixed size overhead of the collection.
/// - If the estimation overflows, we return a [`crate::error::DataFusionError`]
///
/// # Examples
/// ---
///
/// ## From within a struct
///
/// ```rust
/// # use datafusion_common::utils::memory::estimate_memory_size;
/// # use datafusion_common::Result;
///
/// struct MyStruct<T> {
///     values: Vec<T>,
///     other_data: usize,
/// }
///
/// impl<T> MyStruct<T> {
///     fn size(&self) -> Result<usize> {
///         let num_elements = self.values.len();
///         let fixed_size =
///             std::mem::size_of_val(self) + std::mem::size_of_val(&self.values);
///
///         estimate_memory_size::<T>(num_elements, fixed_size)
///     }
/// }
/// ```
/// ---
/// ## With a simple collection
///
/// ```rust
/// # use datafusion_common::utils::memory::estimate_memory_size;
/// # use std::collections::HashMap;
///
/// let num_rows = 100;
/// let fixed_size = std::mem::size_of::<HashMap<u64, u64>>();
/// let estimated_hashtable_size =
///     estimate_memory_size::<(u64, u64)>(num_rows, fixed_size)
///         .expect("Size estimation failed");
/// ```
pub fn estimate_memory_size<T>(num_elements: usize, fixed_size: usize) -> Result<usize> {
    // For the majority of cases hashbrown overestimates the bucket quantity
    // to keep ~1/8 of them empty. We take this factor into account by
    // multiplying the number of elements with a fixed ratio of 8/7 (~1.14).
    // This formula leads to over-allocation for small tables (< 8 elements)
    // but should be fine overall.
    num_elements
        .checked_mul(8)
        .and_then(|overestimate| {
            let estimated_buckets = (overestimate / 7).next_power_of_two();
            // + size of entry * number of buckets
            // + 1 byte for each bucket
            // + fixed size of collection (HashSet/HashTable)
            size_of::<T>()
                .checked_mul(estimated_buckets)?
                .checked_add(estimated_buckets)?
                .checked_add(fixed_size)
        })
        .ok_or_else(|| {
            _exec_datafusion_err!("usize overflow while estimating the number of buckets")
        })
}

#[cfg(test)]
mod tests {
    use std::{collections::HashSet, mem::size_of};

    use super::estimate_memory_size;

    #[test]
    fn test_estimate_memory() {
        // size (bytes): 48
        let fixed_size = size_of::<HashSet<u32>>();

        // estimated buckets: 16 = (8 * 8 / 7).next_power_of_two()
        let num_elements = 8;
        // size (bytes): 128 = 16 * 4 + 16 + 48
        let estimated = estimate_memory_size::<u32>(num_elements, fixed_size).unwrap();
        assert_eq!(estimated, 128);

        // estimated buckets: 64 = (40 * 8 / 7).next_power_of_two()
        let num_elements = 40;
        // size (bytes): 368 = 64 * 4 + 64 + 48
        let estimated = estimate_memory_size::<u32>(num_elements, fixed_size).unwrap();
        assert_eq!(estimated, 368);
    }

    #[test]
    fn test_estimate_memory_overflow() {
        let num_elements = usize::MAX;
        let fixed_size = size_of::<HashSet<u32>>();
        let estimated = estimate_memory_size::<u32>(num_elements, fixed_size);

        assert!(estimated.is_err());
    }
}
