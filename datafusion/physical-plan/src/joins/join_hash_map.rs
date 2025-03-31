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
use std::ops::IndexMut;

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
/// The indices (values) are stored in a separate chained list stored in the `Vec<u64>`.
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
pub struct JoinHashMap {
    // Stores hash value to last row index
    map: HashTable<(u64, u64)>,
    // Stores indices in chained list data structure
    next: Vec<u64>,
}

impl JoinHashMap {
    #[cfg(test)]
    pub(crate) fn new(map: HashTable<(u64, u64)>, next: Vec<u64>) -> Self {
        Self { map, next }
    }

    pub(crate) fn with_capacity(capacity: usize) -> Self {
        JoinHashMap {
            map: HashTable::with_capacity(capacity),
            next: vec![0; capacity],
        }
    }
}

// Type of offsets for obtaining indices from JoinHashMap.
pub(crate) type JoinHashMapOffset = (usize, Option<u64>);

// Macro for traversing chained values with limit.
// Early returns in case of reaching output tuples limit.
macro_rules! chain_traverse {
    (
        $input_indices:ident, $match_indices:ident, $hash_values:ident, $next_chain:ident,
        $input_idx:ident, $chain_idx:ident, $deleted_offset:ident, $remaining_output:ident
    ) => {
        let mut i = $chain_idx - 1;
        loop {
            let match_row_idx = if let Some(offset) = $deleted_offset {
                // This arguments means that we prune the next index way before here.
                if i < offset as u64 {
                    // End of the list due to pruning
                    break;
                }
                i - offset as u64
            } else {
                i
            };
            $match_indices.push(match_row_idx);
            $input_indices.push($input_idx as u32);
            $remaining_output -= 1;
            // Follow the chain to get the next index value
            let next = $next_chain[match_row_idx as usize];

            if $remaining_output == 0 {
                // In case current input index is the last, and no more chain values left
                // returning None as whole input has been scanned
                let next_offset = if $input_idx == $hash_values.len() - 1 && next == 0 {
                    None
                } else {
                    Some(($input_idx, Some(next)))
                };
                return ($input_indices, $match_indices, next_offset);
            }
            if next == 0 {
                // end of list
                break;
            }
            i = next - 1;
        }
    };
}

// Trait defining methods that must be implemented by a hash map type to be used for joins.
pub trait JoinHashMapType {
    /// The type of list used to store the next list
    type NextType: IndexMut<usize, Output = u64>;
    /// Extend with zero
    fn extend_zero(&mut self, len: usize);
    /// Returns mutable references to the hash map and the next.
    fn get_mut(&mut self) -> (&mut HashTable<(u64, u64)>, &mut Self::NextType);
    /// Returns a reference to the hash map.
    fn get_map(&self) -> &HashTable<(u64, u64)>;
    /// Returns a reference to the next.
    fn get_list(&self) -> &Self::NextType;

    /// Updates hashmap from iterator of row indices & row hashes pairs.
    fn update_from_iter<'a>(
        &mut self,
        iter: impl Iterator<Item = (usize, &'a u64)>,
        deleted_offset: usize,
    ) {
        let (mut_map, mut_list) = self.get_mut();
        for (row, &hash_value) in iter {
            let entry = mut_map.entry(
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
                    *index = (row + 1) as u64;
                    // Update chained Vec at `row` with previous value
                    mut_list[row - deleted_offset] = prev_index;
                }
                Vacant(vacant_entry) => {
                    vacant_entry.insert((hash_value, (row + 1) as u64));
                    // chained list at `row` is already initialized with 0
                    // meaning end of list
                }
            }
        }
    }

    /// Returns all pairs of row indices matched by hash.
    ///
    /// This method only compares hashes, so additional further check for actual values
    /// equality may be required.
    fn get_matched_indices<'a>(
        &self,
        iter: impl Iterator<Item = (usize, &'a u64)>,
        deleted_offset: Option<usize>,
    ) -> (Vec<u32>, Vec<u64>) {
        let mut input_indices = vec![];
        let mut match_indices = vec![];

        let hash_map = self.get_map();
        let next_chain = self.get_list();
        for (row_idx, hash_value) in iter {
            // Get the hash and find it in the index
            if let Some((_, index)) =
                hash_map.find(*hash_value, |(hash, _)| *hash_value == *hash)
            {
                let mut i = *index - 1;
                loop {
                    let match_row_idx = if let Some(offset) = deleted_offset {
                        // This arguments means that we prune the next index way before here.
                        if i < offset as u64 {
                            // End of the list due to pruning
                            break;
                        }
                        i - offset as u64
                    } else {
                        i
                    };
                    match_indices.push(match_row_idx);
                    input_indices.push(row_idx as u32);
                    // Follow the chain to get the next index value
                    let next = next_chain[match_row_idx as usize];
                    if next == 0 {
                        // end of list
                        break;
                    }
                    i = next - 1;
                }
            }
        }

        (input_indices, match_indices)
    }

    /// Matches hashes with taking limit and offset into account.
    /// Returns pairs of matched indices along with the starting point for next
    /// matching iteration (`None` if limit has not been reached).
    ///
    /// This method only compares hashes, so additional further check for actual values
    /// equality may be required.
    fn get_matched_indices_with_limit_offset(
        &self,
        hash_values: &[u64],
        deleted_offset: Option<usize>,
        limit: usize,
        offset: JoinHashMapOffset,
    ) -> (Vec<u32>, Vec<u64>, Option<JoinHashMapOffset>) {
        let mut input_indices = vec![];
        let mut match_indices = vec![];

        let mut remaining_output = limit;

        let hash_map: &HashTable<(u64, u64)> = self.get_map();
        let next_chain = self.get_list();

        // Calculate initial `hash_values` index before iterating
        let to_skip = match offset {
            // None `initial_next_idx` indicates that `initial_idx` processing has'n been started
            (initial_idx, None) => initial_idx,
            // Zero `initial_next_idx` indicates that `initial_idx` has been processed during
            // previous iteration, and it should be skipped
            (initial_idx, Some(0)) => initial_idx + 1,
            // Otherwise, process remaining `initial_idx` matches by traversing `next_chain`,
            // to start with the next index
            (initial_idx, Some(initial_next_idx)) => {
                chain_traverse!(
                    input_indices,
                    match_indices,
                    hash_values,
                    next_chain,
                    initial_idx,
                    initial_next_idx,
                    deleted_offset,
                    remaining_output
                );

                initial_idx + 1
            }
        };

        let mut row_idx = to_skip;
        for hash_value in &hash_values[to_skip..] {
            if let Some((_, index)) =
                hash_map.find(*hash_value, |(hash, _)| *hash_value == *hash)
            {
                chain_traverse!(
                    input_indices,
                    match_indices,
                    hash_values,
                    next_chain,
                    row_idx,
                    index,
                    deleted_offset,
                    remaining_output
                );
            }
            row_idx += 1;
        }

        (input_indices, match_indices, None)
    }
}

/// Implementation of `JoinHashMapType` for `JoinHashMap`.
impl JoinHashMapType for JoinHashMap {
    type NextType = Vec<u64>;

    // Void implementation
    fn extend_zero(&mut self, _: usize) {}

    /// Get mutable references to the hash map and the next.
    fn get_mut(&mut self) -> (&mut HashTable<(u64, u64)>, &mut Self::NextType) {
        (&mut self.map, &mut self.next)
    }

    /// Get a reference to the hash map.
    fn get_map(&self) -> &HashTable<(u64, u64)> {
        &self.map
    }

    /// Get a reference to the next.
    fn get_list(&self) -> &Self::NextType {
        &self.next
    }
}

impl Debug for JoinHashMap {
    fn fmt(&self, _f: &mut fmt::Formatter) -> fmt::Result {
        Ok(())
    }
}
