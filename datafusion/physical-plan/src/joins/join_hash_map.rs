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

//! This file contains the implementation of the `JoinHashMap` struct, which
//! is used to store the mapping between hash values based on the build side
//! ["on" values] to a list of indices with this key's value.

use std::fmt::{self, Debug};
use std::ops::Sub;

use hashbrown::hash_table::Entry::{Occupied, Vacant};
use hashbrown::HashTable;

/// Maps a `u64` hash value based on the build side ["on" values] to a list of indices with this key's value.
///
/// By allocating a `HashMap` with capacity for *at least* the number of rows for entries at the build side,
/// we make sure that we don't have to re-hash the hashmap, which needs access to the key (the hash in this case) value.
///
/// E.g. 1 -> [3, 6, 8] indicates that the column values map to rows 3, 6 and 8 for hash value 1
/// As the key is a hash value, we need to check possible hash collisions in the probe stage
/// During this stage it might be the case that a row is contained the same hashmap value,
/// but the values don't match. Those are checked in the `equal_rows_arr` method.
///
/// The indices (values) are stored in a separate chained list stored as `Vec<u32>` or `Vec<u64>`.
///
/// The first value (+1) is stored in the hashmap, whereas the next value is stored in array at the position value.
///
/// The chain can be followed until the value "0" has been reached, meaning the end of the list.
/// Also see chapter 5.3 of [Balancing vectorized query execution with bandwidth-optimized storage](https://dare.uva.nl/search?identifier=5ccbb60a-38b8-4eeb-858a-e7735dd37487)
///
/// # Example
///
/// ``` text
/// See the example below:
///
/// Insert (10,1)            <-- insert hash value 10 with row index 1
/// map:
/// ----------
/// | 10 | 2 |
/// ----------
/// next:
/// ---------------------
/// | 0 | 0 | 0 | 0 | 0 |
/// ---------------------
/// Insert (20,2)
/// map:
/// ----------
/// | 10 | 2 |
/// | 20 | 3 |
/// ----------
/// next:
/// ---------------------
/// | 0 | 0 | 0 | 0 | 0 |
/// ---------------------
/// Insert (10,3)           <-- collision! row index 3 has a hash value of 10 as well
/// map:
/// ----------
/// | 10 | 4 |
/// | 20 | 3 |
/// ----------
/// next:
/// ---------------------
/// | 0 | 0 | 0 | 2 | 0 |  <--- hash value 10 maps to 4,2 (which means indices values 3,1)
/// ---------------------
/// Insert (10,4)          <-- another collision! row index 4 ALSO has a hash value of 10
/// map:
/// ---------
/// | 10 | 5 |
/// | 20 | 3 |
/// ---------
/// next:
/// ---------------------
/// | 0 | 0 | 0 | 2 | 4 | <--- hash value 10 maps to 5,4,2 (which means indices values 4,3,1)
/// ---------------------
/// ```
///
/// Here we have an option between creating a `JoinHashMapType` using `u32` or `u64` indices
/// based on how many rows were being used for indices.
///
/// At runtime we choose between using `JoinHashMapU32` and `JoinHashMapU64` which oth implement
/// `JoinHashMapType`.
pub trait JoinHashMapType: Send + Sync {
    fn extend_zero(&mut self, len: usize);

    fn update_from_iter<'a>(
        &mut self,
        iter: Box<dyn Iterator<Item = (usize, &'a u64)> + Send + 'a>,
        deleted_offset: usize,
    );

    fn get_matched_indices<'a>(
        &self,
        iter: Box<dyn Iterator<Item = (usize, &'a u64)> + 'a>,
        deleted_offset: Option<usize>,
    ) -> (Vec<u32>, Vec<u64>);

    fn get_matched_indices_with_limit_offset(
        &self,
        hash_values: &[u64],
        limit: usize,
        offset: JoinHashMapOffset,
    ) -> (Vec<u32>, Vec<u64>, Option<JoinHashMapOffset>);

    /// Returns `true` if the join hash map contains no entries.
    fn is_empty(&self) -> bool;
}

pub struct JoinHashMapU32 {
    // Stores hash value to last row index
    map: HashTable<(u64, u32)>,
    // Stores indices in chained list data structure
    next: Vec<u32>,
}

impl JoinHashMapU32 {
    #[cfg(test)]
    pub(crate) fn new(map: HashTable<(u64, u32)>, next: Vec<u32>) -> Self {
        Self { map, next }
    }

    pub fn with_capacity(cap: usize) -> Self {
        Self {
            map: HashTable::with_capacity(cap),
            next: vec![0; cap],
        }
    }
}

impl Debug for JoinHashMapU32 {
    fn fmt(&self, _f: &mut fmt::Formatter) -> fmt::Result {
        Ok(())
    }
}

impl JoinHashMapType for JoinHashMapU32 {
    fn extend_zero(&mut self, _: usize) {}

    fn update_from_iter<'a>(
        &mut self,
        iter: Box<dyn Iterator<Item = (usize, &'a u64)> + Send + 'a>,
        deleted_offset: usize,
    ) {
        update_from_iter::<u32>(&mut self.map, &mut self.next, iter, deleted_offset);
    }

    fn get_matched_indices<'a>(
        &self,
        iter: Box<dyn Iterator<Item = (usize, &'a u64)> + 'a>,
        deleted_offset: Option<usize>,
    ) -> (Vec<u32>, Vec<u64>) {
        get_matched_indices::<u32>(&self.map, &self.next, iter, deleted_offset)
    }

    fn get_matched_indices_with_limit_offset(
        &self,
        hash_values: &[u64],
        limit: usize,
        offset: JoinHashMapOffset,
    ) -> (Vec<u32>, Vec<u64>, Option<JoinHashMapOffset>) {
        get_matched_indices_with_limit_offset::<u32>(
            &self.map,
            &self.next,
            hash_values,
            limit,
            offset,
        )
    }

    fn is_empty(&self) -> bool {
        self.map.is_empty()
    }
}

pub struct JoinHashMapU64 {
    // Stores hash value to last row index
    map: HashTable<(u64, u64)>,
    // Stores indices in chained list data structure
    next: Vec<u64>,
}

impl JoinHashMapU64 {
    #[cfg(test)]
    pub(crate) fn new(map: HashTable<(u64, u64)>, next: Vec<u64>) -> Self {
        Self { map, next }
    }

    pub fn with_capacity(cap: usize) -> Self {
        Self {
            map: HashTable::with_capacity(cap),
            next: vec![0; cap],
        }
    }
}

impl Debug for JoinHashMapU64 {
    fn fmt(&self, _f: &mut fmt::Formatter) -> fmt::Result {
        Ok(())
    }
}

impl JoinHashMapType for JoinHashMapU64 {
    fn extend_zero(&mut self, _: usize) {}

    fn update_from_iter<'a>(
        &mut self,
        iter: Box<dyn Iterator<Item = (usize, &'a u64)> + Send + 'a>,
        deleted_offset: usize,
    ) {
        update_from_iter::<u64>(&mut self.map, &mut self.next, iter, deleted_offset);
    }

    fn get_matched_indices<'a>(
        &self,
        iter: Box<dyn Iterator<Item = (usize, &'a u64)> + 'a>,
        deleted_offset: Option<usize>,
    ) -> (Vec<u32>, Vec<u64>) {
        get_matched_indices::<u64>(&self.map, &self.next, iter, deleted_offset)
    }

    fn get_matched_indices_with_limit_offset(
        &self,
        hash_values: &[u64],
        limit: usize,
        offset: JoinHashMapOffset,
    ) -> (Vec<u32>, Vec<u64>, Option<JoinHashMapOffset>) {
        get_matched_indices_with_limit_offset::<u64>(
            &self.map,
            &self.next,
            hash_values,
            limit,
            offset,
        )
    }

    fn is_empty(&self) -> bool {
        self.map.is_empty()
    }
}

// Type of offsets for obtaining indices from JoinHashMap.
pub(crate) type JoinHashMapOffset = (usize, Option<u64>);

// Macro for traversing chained values with limit.
// Early returns in case of reaching output tuples limit.
macro_rules! chain_traverse {
    (
        $input_indices:ident, $match_indices:ident,
        $hash_values:ident, $next_chain:ident,
        $input_idx:ident, $chain_idx:ident, $remaining_output:ident, $one:ident, $zero:ident
    ) => {{
        // now `one` and `zero` are in scope from the outer function
        let mut match_row_idx = $chain_idx - $one;
        loop {
            $match_indices.push(match_row_idx.into());
            $input_indices.push($input_idx as u32);
            $remaining_output -= 1;

            let next = $next_chain[match_row_idx.into() as usize];

            if $remaining_output == 0 {
                // we compare against `zero` (of type T) here too
                let next_offset = if $input_idx == $hash_values.len() - 1 && next == $zero
                {
                    None
                } else {
                    Some(($input_idx, Some(next.into())))
                };
                return ($input_indices, $match_indices, next_offset);
            }
            if next == $zero {
                break;
            }
            match_row_idx = next - $one;
        }
    }};
}

pub fn update_from_iter<'a, T>(
    map: &mut HashTable<(u64, T)>,
    next: &mut [T],
    iter: Box<dyn Iterator<Item = (usize, &'a u64)> + Send + 'a>,
    deleted_offset: usize,
) where
    T: Copy + TryFrom<usize> + PartialOrd,
    <T as TryFrom<usize>>::Error: Debug,
{
    for (row, &hash_value) in iter {
        let entry = map.entry(
            hash_value,
            |&(hash, _)| hash_value == hash,
            |&(hash, _)| hash,
        );

        match entry {
            Occupied(mut occupied_entry) => {
                // Already exists: add index to next array
                let (_, index) = occupied_entry.get_mut();
                let prev_index = *index;
                // Store new value inside hashmap
                *index = T::try_from(row + 1).unwrap();
                // Update chained Vec at `row` with previous value
                next[row - deleted_offset] = prev_index;
            }
            Vacant(vacant_entry) => {
                vacant_entry.insert((hash_value, T::try_from(row + 1).unwrap()));
            }
        }
    }
}

pub fn get_matched_indices<'a, T>(
    map: &HashTable<(u64, T)>,
    next: &[T],
    iter: Box<dyn Iterator<Item = (usize, &'a u64)> + 'a>,
    deleted_offset: Option<usize>,
) -> (Vec<u32>, Vec<u64>)
where
    T: Copy + TryFrom<usize> + PartialOrd + Into<u64> + Sub<Output = T>,
    <T as TryFrom<usize>>::Error: Debug,
{
    let mut input_indices = vec![];
    let mut match_indices = vec![];
    let zero = T::try_from(0).unwrap();
    let one = T::try_from(1).unwrap();

    for (row_idx, hash_value) in iter {
        // Get the hash and find it in the index
        if let Some((_, index)) = map.find(*hash_value, |(hash, _)| *hash_value == *hash)
        {
            let mut i = *index - one;
            loop {
                let match_row_idx = if let Some(offset) = deleted_offset {
                    let offset = T::try_from(offset).unwrap();
                    // This arguments means that we prune the next index way before here.
                    if i < offset {
                        // End of the list due to pruning
                        break;
                    }
                    i - offset
                } else {
                    i
                };
                match_indices.push(match_row_idx.into());
                input_indices.push(row_idx as u32);
                // Follow the chain to get the next index value
                let next_chain = next[match_row_idx.into() as usize];
                if next_chain == zero {
                    // end of list
                    break;
                }
                i = next_chain - one;
            }
        }
    }

    (input_indices, match_indices)
}

pub fn get_matched_indices_with_limit_offset<T>(
    map: &HashTable<(u64, T)>,
    next_chain: &[T],
    hash_values: &[u64],
    limit: usize,
    offset: JoinHashMapOffset,
) -> (Vec<u32>, Vec<u64>, Option<JoinHashMapOffset>)
where
    T: Copy + TryFrom<usize> + PartialOrd + Into<u64> + Sub<Output = T>,
    <T as TryFrom<usize>>::Error: Debug,
{
    let mut input_indices = Vec::with_capacity(limit);
    let mut match_indices = Vec::with_capacity(limit);
    let zero = T::try_from(0).unwrap();
    let one = T::try_from(1).unwrap();

    // Check if hashmap consists of unique values
    // If so, we can skip the chain traversal
    if map.len() == next_chain.len() {
        let start = offset.0;
        let end = (start + limit).min(hash_values.len());
        for (i, &hash) in hash_values[start..end].iter().enumerate() {
            if let Some((_, idx)) = map.find(hash, |(h, _)| hash == *h) {
                input_indices.push(start as u32 + i as u32);
                match_indices.push((*idx - one).into());
            }
        }
        let next_off = if end == hash_values.len() {
            None
        } else {
            Some((end, None))
        };
        return (input_indices, match_indices, next_off);
    }

    let mut remaining_output = limit;

    // Calculate initial `hash_values` index before iterating
    let to_skip = match offset {
        // None `initial_next_idx` indicates that `initial_idx` processing has'n been started
        (idx, None) => idx,
        // Zero `initial_next_idx` indicates that `initial_idx` has been processed during
        // previous iteration, and it should be skipped
        (idx, Some(0)) => idx + 1,
        // Otherwise, process remaining `initial_idx` matches by traversing `next_chain`,
        // to start with the next index
        (idx, Some(next_idx)) => {
            let next_idx: T = T::try_from(next_idx as usize).unwrap();
            chain_traverse!(
                input_indices,
                match_indices,
                hash_values,
                next_chain,
                idx,
                next_idx,
                remaining_output,
                one,
                zero
            );
            idx + 1
        }
    };

    let mut row_idx = to_skip;
    for &hash in &hash_values[to_skip..] {
        if let Some((_, idx)) = map.find(hash, |(h, _)| hash == *h) {
            let idx: T = *idx;
            chain_traverse!(
                input_indices,
                match_indices,
                hash_values,
                next_chain,
                row_idx,
                idx,
                remaining_output,
                one,
                zero
            );
        }
        row_idx += 1;
    }
    (input_indices, match_indices, None)
}
