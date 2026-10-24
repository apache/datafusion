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

use arrow::array::BooleanArray;
use arrow::buffer::{BooleanBuffer, NullBuffer};
use arrow::datatypes::ArrowNativeType;
use hashbrown::HashTable;
use hashbrown::hash_table::Entry::{Occupied, Vacant};

/// Resume point of a chunked lookup, see
/// [`JoinHashMapType::get_matched_indices_with_limit_offset`].
///
/// A lookup emits at most `limit` matches. If the limit is reached before
/// the probe batch is exhausted, it returns the position to resume from;
/// once the batch is exhausted it returns `None`.
///
/// Each position has a single encoding: once a probe row's chain has been
/// fully traversed, the next offset is the following row, `AtRow { row: row + 1 }`.
/// Given `limit > 0`, a returned offset is therefore never `AtRow { row: 0 }`,
/// which marks the first lookup of a probe batch.
#[derive(Debug, PartialEq, Eq, Clone, Copy)]
pub enum ProbeOffset {
    /// Resume at probe row `row`, from the head of its chain.
    AtRow { row: usize },
    /// Resume in the middle of probe row `row`'s chain, where `next` is the
    /// 1-based index of the next build row in the chain. `next` is never `0`,
    /// the end-of-chain marker.
    MidChain { row: usize, next: u64 },
}

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
/// Once every build row has been inserted, [`JoinHashMapType::group_rows`] can replace the
/// chain with the rows of each hash value stored next to each other, so that a probe row reads
/// its matches in order instead of loading each next index from the previous one. The map then
/// holds the position of each hash value's group, which starts with its number of rows:
///
/// ``` text
/// map:                 groups:
/// ----------           -------------------------
/// | 10 | 0 |           | 3 | 4 | 3 | 1 | 1 | 2 |
/// | 20 | 4 |           -------------------------
/// ----------             ^ hash value 10: 3 rows (4, 3, 1)
///                                        ^ hash value 20: 1 row (2)
/// ```
///
/// Here we have an option between creating a `JoinHashMapType` using `u32` or `u64` indices
/// based on how many rows were being used for indices.
///
/// At runtime we choose between using `JoinHashMapU32` and `JoinHashMapU64` which oth implement
/// `JoinHashMapType`.
///
/// For example, we may compare references to `JoinHashMapType` implementations by pointer equality
/// rather than deep equality of contents, as deep equality would be expensive and in our usage
/// patterns it is impossible for two different hash maps to have identical contents in a practical sense.
///
/// # Public Only for Internal Use:
///
/// This is not a public API and is for internal use only; see [API policy] for details.
///
/// [API policy]: https://datafusion.apache.org/contributor-guide/api-health.html#internal-public-apis
#[doc(hidden)]
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

    /// Probe rows marked NULL in `valid_keys` are skipped without a lookup:
    /// their key contains a NULL, which cannot match any build row under
    /// `NullEquality::NullEqualsNothing`. Pass `None` when every probe key is
    /// matchable.
    fn get_matched_indices_with_limit_offset(
        &self,
        hash_values: &[u64],
        valid_keys: Option<&NullBuffer>,
        limit: usize,
        offset: ProbeOffset,
        input_indices: &mut Vec<u32>,
        match_indices: &mut Vec<u64>,
    ) -> Option<ProbeOffset>;

    /// Returns a BooleanArray indicating which of the provided hashes exist in the map.
    fn contain_hashes(&self, hash_values: &[u64]) -> BooleanArray;

    /// Returns `true` if the join hash map contains no entries.
    fn is_empty(&self) -> bool;

    /// Returns the number of entries in the join hash map.
    fn len(&self) -> usize;

    /// Returns the number of bytes that [`Self::group_rows`] would allocate,
    /// or `None` if it would not group the rows: when the hash values have
    /// fewer than two build rows each on average, the chains are too short for
    /// grouping to pay for itself.
    fn grouped_rows_size(&self) -> Option<usize> {
        None
    }

    /// Stores the build rows of each hash value next to each other, in chain
    /// order, and drops the chain. A probe row then reads its matches from one
    /// place, instead of following the chain with a dependent load for every
    /// match, which misses the cache when the build side is large. Call this
    /// once every build row has been inserted. Returns the number of bytes
    /// freed.
    fn group_rows(&mut self) -> usize {
        0
    }
}

pub struct JoinHashMapU32 {
    // Stores hash value to last row index, or to its group in `groups`
    map: HashTable<(u64, u32)>,
    // Stores indices in chained list data structure
    next: Vec<u32>,
    // Whether any hash value maps to more than one row
    chained: bool,
    // The build rows of each hash value once `group_rows` has grouped them,
    // after which `next` is empty
    groups: Vec<u32>,
}

impl JoinHashMapU32 {
    #[cfg(test)]
    pub(crate) fn new(map: HashTable<(u64, u32)>, next: Vec<u32>) -> Self {
        let chained = map.len() != next.len();
        Self {
            map,
            next,
            chained,
            groups: vec![],
        }
    }

    pub fn with_capacity(cap: usize) -> Self {
        Self {
            map: HashTable::with_capacity(cap),
            next: vec![0; cap],
            chained: false,
            groups: vec![],
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
        self.chained |=
            update_from_iter::<u32>(&mut self.map, &mut self.next, iter, deleted_offset);
    }

    fn get_matched_indices<'a>(
        &self,
        iter: Box<dyn Iterator<Item = (usize, &'a u64)> + 'a>,
        deleted_offset: Option<usize>,
    ) -> (Vec<u32>, Vec<u64>) {
        get_matched_indices::<u32>(
            &self.map,
            &self.next,
            &self.groups,
            iter,
            deleted_offset,
        )
    }

    fn get_matched_indices_with_limit_offset(
        &self,
        hash_values: &[u64],
        valid_keys: Option<&NullBuffer>,
        limit: usize,
        offset: ProbeOffset,
        input_indices: &mut Vec<u32>,
        match_indices: &mut Vec<u64>,
    ) -> Option<ProbeOffset> {
        get_matched_indices_with_limit_offset::<u32>(
            &self.map,
            &self.next,
            &self.groups,
            !self.chained,
            hash_values,
            valid_keys,
            limit,
            offset,
            input_indices,
            match_indices,
        )
    }

    fn contain_hashes(&self, hash_values: &[u64]) -> BooleanArray {
        contain_hashes(&self.map, hash_values)
    }

    fn is_empty(&self) -> bool {
        self.map.is_empty()
    }

    fn len(&self) -> usize {
        self.map.len()
    }

    fn grouped_rows_size(&self) -> Option<usize> {
        grouped_rows_size(&self.map, &self.next, self.chained)
    }

    fn group_rows(&mut self) -> usize {
        if self.grouped_rows_size().is_none() {
            return 0;
        }
        let freed = self.next.capacity() * size_of::<u32>();
        self.groups = group_rows(&mut self.map, &self.next);
        self.next = vec![];
        freed
    }
}

pub struct JoinHashMapU64 {
    // Stores hash value to last row index, or to its group in `groups`
    map: HashTable<(u64, u64)>,
    // Stores indices in chained list data structure
    next: Vec<u64>,
    // Whether any hash value maps to more than one row
    chained: bool,
    // The build rows of each hash value once `group_rows` has grouped them,
    // after which `next` is empty
    groups: Vec<u64>,
}

impl JoinHashMapU64 {
    #[cfg(test)]
    pub(crate) fn new(map: HashTable<(u64, u64)>, next: Vec<u64>) -> Self {
        let chained = map.len() != next.len();
        Self {
            map,
            next,
            chained,
            groups: vec![],
        }
    }

    pub fn with_capacity(cap: usize) -> Self {
        Self {
            map: HashTable::with_capacity(cap),
            next: vec![0; cap],
            chained: false,
            groups: vec![],
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
        self.chained |=
            update_from_iter::<u64>(&mut self.map, &mut self.next, iter, deleted_offset);
    }

    fn get_matched_indices<'a>(
        &self,
        iter: Box<dyn Iterator<Item = (usize, &'a u64)> + 'a>,
        deleted_offset: Option<usize>,
    ) -> (Vec<u32>, Vec<u64>) {
        get_matched_indices::<u64>(
            &self.map,
            &self.next,
            &self.groups,
            iter,
            deleted_offset,
        )
    }

    fn get_matched_indices_with_limit_offset(
        &self,
        hash_values: &[u64],
        valid_keys: Option<&NullBuffer>,
        limit: usize,
        offset: ProbeOffset,
        input_indices: &mut Vec<u32>,
        match_indices: &mut Vec<u64>,
    ) -> Option<ProbeOffset> {
        get_matched_indices_with_limit_offset::<u64>(
            &self.map,
            &self.next,
            &self.groups,
            !self.chained,
            hash_values,
            valid_keys,
            limit,
            offset,
            input_indices,
            match_indices,
        )
    }

    fn contain_hashes(&self, hash_values: &[u64]) -> BooleanArray {
        contain_hashes(&self.map, hash_values)
    }

    fn is_empty(&self) -> bool {
        self.map.is_empty()
    }

    fn len(&self) -> usize {
        self.map.len()
    }

    fn grouped_rows_size(&self) -> Option<usize> {
        grouped_rows_size(&self.map, &self.next, self.chained)
    }

    fn group_rows(&mut self) -> usize {
        if self.grouped_rows_size().is_none() {
            return 0;
        }
        let freed = self.next.capacity() * size_of::<u64>();
        self.groups = group_rows(&mut self.map, &self.next);
        self.next = vec![];
        freed
    }
}

use crate::joins::chain::{emit_grouped_rows, traverse_chain};
use crate::joins::join_hash_map::ProbeOffset::{AtRow, MidChain};

/// Returns the number of bytes that [`group_rows`] allocates for `map` and
/// `next`, or `None` if the hash values have fewer than two rows each on
/// average, or if a position in the groups would not fit in `T`.
fn grouped_rows_size<T>(
    map: &HashTable<(u64, T)>,
    next: &[T],
    chained: bool,
) -> Option<usize>
where
    T: ArrowNativeType,
{
    // Following a chain costs a cache miss for each row after the first, which
    // only adds up when the average chain is long enough. `next.len()` counts
    // the rows left out of the map, such as NULL keys, too.
    if !chained || next.len() < map.len().saturating_mul(2) {
        return None;
    }
    // Each group holds its length and then its rows.
    let len = next.len().checked_add(map.len())?;
    T::from_usize(len).map(|_| len * size_of::<T>())
}

/// Lays out the rows of each hash value in `map` next to each other, in the
/// order of their chain in `next`, and points each map entry at its group: at
/// `pos` the group holds its number of rows, followed by the rows (0-based).
/// Returns the groups.
fn group_rows<T>(map: &mut HashTable<(u64, T)>, next: &[T]) -> Vec<T>
where
    T: ArrowNativeType + Sub<Output = T> + PartialEq,
{
    /// Chains followed at once. Each step of a chain loads its next index
    /// from the previous one, so following one chain at a time would wait for
    /// a cache miss at every step; the steps of different chains overlap.
    const LANES: usize = 16;
    /// Rows of a chain held until its end, when they are appended to the
    /// groups. A longer chain is only measured, then placed in a second pass,
    /// so that what the lanes hold stays small.
    const MAX_HELD: usize = 4096;

    /// A chain being followed: its map entry, its first and next index
    /// (1-based), its length so far, and its rows unless it is too long.
    struct Lane<'a, T> {
        value: &'a mut T,
        head: T,
        index: T,
        len: usize,
        rows: Vec<T>,
    }

    let zero = T::usize_as(0);
    let one = T::usize_as(1);
    let mut groups = Vec::with_capacity(next.len() + map.len());
    let mut entries = map.iter_mut().map(|(_, value)| value);
    let mut lanes: Vec<Lane<'_, T>> = entries
        .by_ref()
        .take(LANES)
        .map(|value| Lane {
            head: *value,
            index: *value,
            value,
            len: 0,
            rows: vec![],
        })
        .collect();
    // The chains longer than `MAX_HELD`: their map entry, first index and length
    let mut long_chains = vec![];

    while !lanes.is_empty() {
        let mut i = 0;
        while i < lanes.len() {
            let lane = &mut lanes[i];
            let row = lane.index - one;
            lane.len += 1;
            if lane.len <= MAX_HELD {
                lane.rows.push(row);
            }
            lane.index = next[row.as_usize()];
            if lane.index == zero {
                // The end of the chain: append its group, or place it later
                let long = lane.len > MAX_HELD;
                if !long {
                    let pos = groups.len();
                    groups.push(T::usize_as(lane.len));
                    groups.extend_from_slice(&lane.rows);
                    *lane.value = T::usize_as(pos);
                }
                lane.rows.clear();
                // Follow the chain of the next entry in this lane, or retire it
                let (done, retired) = if let Some(value) = entries.next() {
                    let rows = std::mem::take(&mut lane.rows);
                    let lane = Lane {
                        head: *value,
                        index: *value,
                        value,
                        len: 0,
                        rows,
                    };
                    (std::mem::replace(&mut lanes[i], lane), false)
                } else {
                    (lanes.swap_remove(i), true)
                };
                if long {
                    long_chains.push((done.value, done.head, done.len));
                }
                if retired {
                    continue;
                }
            }
            i += 1;
        }
    }

    // Place each long chain at the end of the groups and fill its rows,
    // following several of them at once again: the next index and the
    // position of the next row of each.
    let mut long_chains = long_chains.into_iter();
    let mut place = |groups: &mut Vec<T>| {
        long_chains.next().map(|(value, head, len)| {
            let pos = groups.len();
            groups.push(T::usize_as(len));
            groups.resize(pos + 1 + len, zero);
            *value = T::usize_as(pos);
            (head, pos + 1)
        })
    };
    let mut fills: Vec<(T, usize)> =
        (0..LANES).map_while(|_| place(&mut groups)).collect();
    while !fills.is_empty() {
        let mut i = 0;
        while i < fills.len() {
            let (index, pos) = &mut fills[i];
            let row = *index - one;
            groups[*pos] = row;
            *pos += 1;
            *index = next[row.as_usize()];
            if *index == zero {
                match place(&mut groups) {
                    Some(fill) => fills[i] = fill,
                    None => {
                        fills.swap_remove(i);
                        continue;
                    }
                }
            }
            i += 1;
        }
    }
    groups
}

/// Inserts the rows of `iter` into `map` and `next`. Returns `true` if any row
/// was chained onto an existing hash value.
pub fn update_from_iter<'a, T>(
    map: &mut HashTable<(u64, T)>,
    next: &mut [T],
    iter: Box<dyn Iterator<Item = (usize, &'a u64)> + Send + 'a>,
    deleted_offset: usize,
) -> bool
where
    T: Copy + TryFrom<usize> + PartialOrd,
    <T as TryFrom<usize>>::Error: Debug,
{
    let mut chained = false;
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
                chained = true;
            }
            Vacant(vacant_entry) => {
                vacant_entry.insert((hash_value, T::try_from(row + 1).unwrap()));
            }
        }
    }
    chained
}

/// `groups` holds the rows grouped by [`JoinHashMapType::group_rows`], or is empty if the rows
/// are in the chain in `next`.
pub fn get_matched_indices<'a, T>(
    map: &HashTable<(u64, T)>,
    next: &[T],
    groups: &[T],
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

    if !groups.is_empty() {
        for (row_idx, hash_value) in iter {
            if let Some((_, pos)) =
                map.find(*hash_value, |(hash, _)| *hash_value == *hash)
            {
                let pos: u64 = (*pos).into();
                let pos = pos as usize;
                let len: u64 = groups[pos].into();
                for &row in &groups[pos + 1..=pos + len as usize] {
                    let match_row_idx = if let Some(offset) = deleted_offset {
                        let offset = T::try_from(offset).unwrap();
                        if row < offset {
                            // End of the list due to pruning, as for the chain
                            break;
                        }
                        row - offset
                    } else {
                        row
                    };
                    match_indices.push(match_row_idx.into());
                    input_indices.push(row_idx as u32);
                }
            }
        }
        return (input_indices, match_indices);
    }

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

/// `unique` must be `true` only if every hash value in `map` maps to a single
/// row; the chain in `next_chain` is then not traversed. `groups` holds the
/// rows grouped by [`JoinHashMapType::group_rows`], or is empty if the rows are in the chain.
#[expect(clippy::too_many_arguments)]
pub fn get_matched_indices_with_limit_offset<T>(
    map: &HashTable<(u64, T)>,
    next_chain: &[T],
    groups: &[T],
    unique: bool,
    hash_values: &[u64],
    valid_keys: Option<&NullBuffer>,
    limit: usize,
    offset: ProbeOffset,
    input_indices: &mut Vec<u32>,
    match_indices: &mut Vec<u64>,
) -> Option<ProbeOffset>
where
    T: Copy + TryFrom<usize> + PartialOrd + Into<u64> + Sub<Output = T>,
    <T as TryFrom<usize>>::Error: Debug,
    T: ArrowNativeType,
{
    // Clear the buffer before producing new results
    input_indices.clear();
    match_indices.clear();
    let one = T::try_from(1).unwrap();

    // Check if hashmap consists of unique values
    // If so, we can skip the chain traversal
    if unique {
        debug_assert!(
            matches!(offset, AtRow { .. }),
            "unique keys cannot have mid-chain"
        );
        let start = match offset {
            AtRow { row } => row,
            MidChain { row, .. } => row,
        };
        let end = (start + limit).min(hash_values.len());
        for (i, &hash) in hash_values[start..end].iter().enumerate() {
            // NULL keys cannot match any build row
            if valid_keys.is_some_and(|valid| valid.is_null(start + i)) {
                continue;
            }
            if let Some((_, idx)) = map.find(hash, |(h, _)| hash == *h) {
                input_indices.push(start as u32 + i as u32);
                match_indices.push((*idx - one).into());
            }
        }
        return if end == hash_values.len() {
            None
        } else {
            Some(AtRow { row: end })
        };
    }

    let mut remaining_output = limit;

    if !groups.is_empty() {
        return get_grouped_matched_indices(
            map,
            groups,
            hash_values,
            valid_keys,
            offset,
            &mut remaining_output,
            input_indices,
            match_indices,
        );
    }

    // Calculate initial `hash_values` index before iterating
    let to_skip = match offset {
        // `AtRow` indicates that `idx` processing hasn't been started
        AtRow { row: idx } => idx,
        // Otherwise, process remaining `idx` matches by traversing `next_chain`,
        // to start with the next index
        MidChain {
            row: idx,
            next: next_idx,
        } => {
            let next_idx: T = T::usize_as(next_idx as usize);
            let is_last = idx == hash_values.len() - 1;
            if let Some(next_offset) = traverse_chain(
                next_chain,
                idx,
                next_idx,
                &mut remaining_output,
                input_indices,
                match_indices,
                is_last,
            ) {
                return Some(next_offset);
            }
            idx + 1
        }
    };

    let hash_values_len = hash_values.len();
    for (i, &hash) in hash_values[to_skip..].iter().enumerate() {
        let row_idx = to_skip + i;
        // NULL keys cannot match any build row
        if valid_keys.is_some_and(|valid| valid.is_null(row_idx)) {
            continue;
        }
        if let Some((_, idx)) = map.find(hash, |(h, _)| hash == *h) {
            let idx: T = *idx;
            let is_last = row_idx == hash_values_len - 1;
            if let Some(next_offset) = traverse_chain(
                next_chain,
                row_idx,
                idx,
                &mut remaining_output,
                input_indices,
                match_indices,
                is_last,
            ) {
                return Some(next_offset);
            }
        }
    }
    None
}

/// The lookup of [`get_matched_indices_with_limit_offset`] once the rows are
/// grouped: the matches of a probe row are the rows of its group.
#[expect(clippy::too_many_arguments)]
fn get_grouped_matched_indices<T>(
    map: &HashTable<(u64, T)>,
    groups: &[T],
    hash_values: &[u64],
    valid_keys: Option<&NullBuffer>,
    offset: ProbeOffset,
    remaining_output: &mut usize,
    input_indices: &mut Vec<u32>,
    match_indices: &mut Vec<u64>,
) -> Option<ProbeOffset>
where
    T: Copy + Into<u64>,
{
    // The position of the group of `hash`, and the end of its rows
    let find_group = |hash: u64| {
        map.find(hash, |(h, _)| hash == *h).map(|(_, pos)| {
            let pos: u64 = (*pos).into();
            let pos = pos as usize;
            let len: u64 = groups[pos].into();
            (pos, pos + 1 + len as usize)
        })
    };
    let hash_values_len = hash_values.len();

    let to_skip = match offset {
        AtRow { row: idx } => idx,
        // Resume at position `next` of the group of probe row `idx`
        MidChain {
            row: idx,
            next: next_pos,
        } => {
            let (_, end) = find_group(hash_values[idx])
                .expect("a probe row resumed mid-group has a group");
            let is_last = idx == hash_values_len - 1;
            if let Some(next_offset) = emit_grouped_rows(
                groups,
                next_pos as usize,
                end,
                idx,
                remaining_output,
                input_indices,
                match_indices,
                is_last,
            ) {
                return Some(next_offset);
            }
            idx + 1
        }
    };

    for (i, &hash) in hash_values[to_skip..].iter().enumerate() {
        let row_idx = to_skip + i;
        // NULL keys cannot match any build row
        if valid_keys.is_some_and(|valid| valid.is_null(row_idx)) {
            continue;
        }
        if let Some((pos, end)) = find_group(hash) {
            let is_last = row_idx == hash_values_len - 1;
            if let Some(next_offset) = emit_grouped_rows(
                groups,
                pos + 1,
                end,
                row_idx,
                remaining_output,
                input_indices,
                match_indices,
                is_last,
            ) {
                return Some(next_offset);
            }
        }
    }
    None
}

pub fn contain_hashes<T>(map: &HashTable<(u64, T)>, hash_values: &[u64]) -> BooleanArray {
    let buffer = BooleanBuffer::collect_bool(hash_values.len(), |i| {
        let hash = hash_values[i];
        map.find(hash, |(h, _)| hash == *h).is_some()
    });
    BooleanArray::new(buffer, None)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_contain_hashes() {
        let mut hash_map = JoinHashMapU32::with_capacity(10);
        hash_map.update_from_iter(Box::new([10u64, 20u64, 30u64].iter().enumerate()), 0);

        let probe_hashes = vec![10, 11, 20, 21, 30, 31];
        let array = hash_map.contain_hashes(&probe_hashes);

        assert_eq!(array.len(), probe_hashes.len());

        for (i, &hash) in probe_hashes.iter().enumerate() {
            if matches!(hash, 10 | 20 | 30) {
                assert!(array.value(i), "Hash {hash} should exist in the map");
            } else {
                assert!(!array.value(i), "Hash {hash} should NOT exist in the map");
            }
        }
    }

    #[test]
    fn test_get_matched_indices_skips_invalid_keys() {
        let mut hash_map = JoinHashMapU32::with_capacity(3);
        hash_map.update_from_iter(Box::new([10u64, 20u64, 30u64].iter().enumerate()), 0);

        let probe_hashes = vec![10, 20, 30];
        // The probe row for hash 20 has a NULL key and must not match.
        let valid_keys = NullBuffer::from(vec![true, false, true]);

        let mut input_indices = vec![];
        let mut match_indices = vec![];
        let next_offset = hash_map.get_matched_indices_with_limit_offset(
            &probe_hashes,
            Some(&valid_keys),
            8192,
            AtRow { row: 0 },
            &mut input_indices,
            &mut match_indices,
        );

        assert_eq!(next_offset, None);
        assert_eq!(input_indices, vec![0, 2]);
        assert_eq!(match_indices, vec![0, 2]);
    }

    #[test]
    fn test_get_matched_indices_skips_invalid_keys_with_duplicates() {
        // Duplicate build keys chain multiple rows under one hash value.
        let mut hash_map = JoinHashMapU32::with_capacity(4);
        hash_map.update_from_iter(
            Box::new([10u64, 20u64, 10u64, 20u64].iter().enumerate()),
            0,
        );

        let probe_hashes = vec![10, 20];
        // The probe row for hash 10 has a NULL key: none of the build rows in
        // its chain may match, while the valid probe row for hash 20 must
        // still match its entire chain.
        let valid_keys = NullBuffer::from(vec![false, true]);

        let mut input_indices = vec![];
        let mut match_indices = vec![];
        let next_offset = hash_map.get_matched_indices_with_limit_offset(
            &probe_hashes,
            Some(&valid_keys),
            8192,
            AtRow { row: 0 },
            &mut input_indices,
            &mut match_indices,
        );

        assert_eq!(next_offset, None);
        assert_eq!(input_indices, vec![1, 1]);
        assert_eq!(match_indices, vec![3, 1]);
    }

    #[test]
    fn test_unique_fast_path_with_uninserted_row() {
        // Row 1 is left out, as `update_hash` does for a NULL key under
        // `NullEquality::NullEqualsNothing`. Every hash value still maps to a
        // single row, so the probe takes the unique-key fast path.
        let build_hashes = [10u64, 99, 20, 30];
        let mut hash_map = JoinHashMapU32::with_capacity(build_hashes.len());
        hash_map.update_from_iter(
            Box::new(build_hashes.iter().enumerate().filter(|(row, _)| *row != 1)),
            0,
        );

        // Probe row 1 has no match.
        let probe_hashes = vec![10, 40, 20, 30];
        let mut input_indices = vec![];
        let mut match_indices = vec![];
        let next_offset = hash_map.get_matched_indices_with_limit_offset(
            &probe_hashes,
            None,
            2,
            AtRow { row: 0 },
            &mut input_indices,
            &mut match_indices,
        );
        // The fast path limits by probe rows and resumes at probe row 2 after a
        // single match; the chain walk limits by matches and would also emit
        // probe row 2's match, resuming at probe row 3.
        assert_eq!(next_offset, Some(AtRow { row: 2 }));
        assert_eq!(input_indices, vec![0]);
        assert_eq!(match_indices, vec![0]);

        let next_offset = hash_map.get_matched_indices_with_limit_offset(
            &probe_hashes,
            None,
            2,
            AtRow { row: 2 },
            &mut input_indices,
            &mut match_indices,
        );
        assert_eq!(next_offset, None);
        assert_eq!(input_indices, vec![2, 3]);
        assert_eq!(match_indices, vec![2, 3]);
    }

    #[test]
    fn test_chain_from_earlier_update_disables_fast_path() {
        // Rows 0 and 1 share a hash value; a later update without a chain must
        // not re-enable the unique-key fast path.
        let mut hash_map = JoinHashMapU32::with_capacity(3);
        hash_map.update_from_iter(Box::new([10u64, 10u64].iter().enumerate()), 0);
        hash_map.update_from_iter(Box::new([(2, &20u64)].into_iter()), 0);

        let mut input_indices = vec![];
        let mut match_indices = vec![];
        let next_offset = hash_map.get_matched_indices_with_limit_offset(
            &[10],
            None,
            8192,
            AtRow { row: 0 },
            &mut input_indices,
            &mut match_indices,
        );

        assert_eq!(next_offset, None);
        assert_eq!(input_indices, vec![0, 0]);
        assert_eq!(match_indices, vec![1, 0]);
    }

    /// Collects every match of `hash_values`, `limit` at a time.
    fn all_matches(
        hash_map: &dyn JoinHashMapType,
        hash_values: &[u64],
        valid_keys: Option<&NullBuffer>,
        limit: usize,
    ) -> Vec<(u32, u64)> {
        let mut matches = vec![];
        let mut input_indices = vec![];
        let mut match_indices = vec![];
        let mut offset = AtRow { row: 0 };
        loop {
            let next_offset = hash_map.get_matched_indices_with_limit_offset(
                hash_values,
                valid_keys,
                limit,
                offset,
                &mut input_indices,
                &mut match_indices,
            );
            assert!(input_indices.len() <= limit);
            assert_eq!(input_indices.len(), match_indices.len());
            matches.extend(
                input_indices
                    .iter()
                    .copied()
                    .zip(match_indices.iter().copied()),
            );
            match next_offset {
                Some(next) => offset = next,
                None => return matches,
            }
        }
    }

    /// Builds two maps from the same rows, inserted batch by batch and last
    /// to first as `update_hash` does, with row 7 left out as a NULL key would
    /// be, and groups the rows of the second.
    fn chained_and_grouped<M: JoinHashMapType>(
        new_map: impl Fn(usize) -> M,
        build_hashes: &[u64],
    ) -> (M, M) {
        let insert = |hash_map: &mut M| {
            for (batch_idx, hashes) in build_hashes.chunks(64).enumerate().rev() {
                let offset = batch_idx * 64;
                hash_map.update_from_iter(
                    Box::new(
                        hashes
                            .iter()
                            .enumerate()
                            .map(move |(i, hash)| (offset + i, hash))
                            .filter(|(row, _)| *row != 7)
                            .rev(),
                    ),
                    0,
                );
            }
        };
        let mut chained = new_map(build_hashes.len());
        insert(&mut chained);
        let mut grouped = new_map(build_hashes.len());
        insert(&mut grouped);
        assert!(grouped.grouped_rows_size().is_some());
        assert!(grouped.group_rows() > 0);
        (chained, grouped)
    }

    #[test]
    fn test_grouped_rows_match_the_chain() {
        // 13 hash values over 300 build rows, and probe rows that repeat,
        // miss (13 to 16) or have a NULL key.
        let build_hashes: Vec<u64> = (0..300u64).map(|i| (i * 7919) % 13).collect();
        let probe_hashes: Vec<u64> = (0..50u64).map(|i| (i * 31) % 17).collect();
        let valid_keys =
            NullBuffer::from((0..50).map(|i| i % 9 != 4).collect::<Vec<_>>());

        let (chained, grouped) =
            chained_and_grouped(JoinHashMapU32::with_capacity, &build_hashes);
        let (chained_u64, grouped_u64) =
            chained_and_grouped(JoinHashMapU64::with_capacity, &build_hashes);
        for limit in [1, 2, 3, 5, 23, 64, 8192] {
            for valid in [None, Some(&valid_keys)] {
                let expected = all_matches(&chained, &probe_hashes, valid, limit);
                assert!(!expected.is_empty());
                assert_eq!(all_matches(&grouped, &probe_hashes, valid, limit), expected);
                assert_eq!(
                    all_matches(&chained_u64, &probe_hashes, valid, limit),
                    expected
                );
                assert_eq!(
                    all_matches(&grouped_u64, &probe_hashes, valid, limit),
                    expected
                );
            }
        }

        let iter = || Box::new(probe_hashes.iter().enumerate());
        let expected = chained.get_matched_indices(iter(), None);
        assert_eq!(grouped.get_matched_indices(iter(), None), expected);
        assert_eq!(grouped_u64.get_matched_indices(iter(), None), expected);
        assert_eq!(
            grouped.contain_hashes(&probe_hashes),
            chained.contain_hashes(&probe_hashes)
        );
    }

    #[test]
    fn test_grouped_rows_match_the_chain_for_long_chains() {
        // Hash values 0 and 1 have chains too long for grouping to hold their
        // rows until their end, among 2,000 short chains.
        let build_hashes: Vec<u64> = (0..30_000u64)
            .map(|i| if i % 3 == 0 { i % 2 } else { 2 + i % 2_000 })
            .collect();
        let probe_hashes: Vec<u64> = vec![0, 5, 1, 7, 2_001, 3_000, 0];

        let (chained, grouped) =
            chained_and_grouped(JoinHashMapU32::with_capacity, &build_hashes);
        for limit in [1, 7, 4_096, 5_000, 8_192, 100_000] {
            let expected = all_matches(&chained, &probe_hashes, None, limit);
            assert_eq!(expected.len(), 2 * 5_000 + 5_000 + 3 * 10);
            assert_eq!(all_matches(&grouped, &probe_hashes, None, limit), expected);
        }
    }

    #[test]
    fn test_unique_hash_values_are_not_grouped() {
        let mut hash_map = JoinHashMapU32::with_capacity(3);
        hash_map.update_from_iter(Box::new([10u64, 20u64, 30u64].iter().enumerate()), 0);
        assert_eq!(hash_map.grouped_rows_size(), None);
        assert_eq!(hash_map.group_rows(), 0);
        assert_eq!(
            all_matches(&hash_map, &[30, 10, 40], None, 8192),
            vec![(0, 2), (1, 0)]
        );
    }

    #[test]
    fn test_short_chains_are_not_grouped() {
        // Four rows over three hash values: fewer than two rows each on average
        let mut hash_map = JoinHashMapU32::with_capacity(4);
        hash_map.update_from_iter(
            Box::new([10u64, 20u64, 30u64, 10u64].iter().enumerate().rev()),
            0,
        );
        assert_eq!(hash_map.grouped_rows_size(), None);
        assert_eq!(hash_map.group_rows(), 0);
        assert_eq!(
            all_matches(&hash_map, &[10, 30], None, 8192),
            vec![(0, 0), (0, 3), (1, 2)]
        );
    }

    #[test]
    fn test_group_rows_replaces_the_chain() {
        let mut hash_map = JoinHashMapU64::with_capacity(4);
        hash_map.update_from_iter(
            Box::new([10u64, 20u64, 10u64, 10u64].iter().enumerate().rev()),
            0,
        );
        // A length and the rows for each of the two hash values
        assert_eq!(hash_map.grouped_rows_size(), Some(6 * size_of::<u64>()));
        assert_eq!(hash_map.group_rows(), 4 * size_of::<u64>());
        assert!(hash_map.next.is_empty());
        assert_eq!(hash_map.groups.len(), 6);
        // The rows of a hash value keep the chain's order, earliest first
        assert_eq!(
            all_matches(&hash_map, &[10, 20], None, 8192),
            vec![(0, 0), (0, 2), (0, 3), (1, 1)]
        );
    }

    #[test]
    fn test_chain_created_by_later_update_disables_fast_path() {
        assert_chain_created_by_later_update(JoinHashMapU32::with_capacity(3));
        assert_chain_created_by_later_update(JoinHashMapU64::with_capacity(3));
    }

    /// Rows 0 and 2 share a hash value but arrive in separate updates, and row
    /// 1 is left out as a NULL key would be. The first update alone is unique;
    /// the second creates the chain. Probing with `limit = 1` must resume
    /// through the chain and return each build row exactly once.
    fn assert_chain_created_by_later_update(mut hash_map: impl JoinHashMapType) {
        hash_map.update_from_iter(Box::new([(0, &10u64)].into_iter()), 0);
        hash_map.update_from_iter(Box::new([(2, &10u64)].into_iter()), 0);

        let mut input_indices = vec![];
        let mut match_indices = vec![];
        let mut matched = vec![];
        let mut offset = AtRow { row: 0 };
        for _ in 0..3 {
            let next_offset = hash_map.get_matched_indices_with_limit_offset(
                &[10],
                None,
                1,
                offset,
                &mut input_indices,
                &mut match_indices,
            );
            assert_eq!(input_indices, vec![0]);
            matched.extend_from_slice(&match_indices);
            match next_offset {
                Some(next) => offset = next,
                None => break,
            }
        }

        matched.sort_unstable();
        assert_eq!(matched, vec![0, 2]);
    }
}
