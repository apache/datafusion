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
use std::mem::{size_of, size_of_val};
use std::ops::Sub;

use arrow::array::BooleanArray;
use arrow::buffer::{BooleanBuffer, NullBuffer};
use arrow::datatypes::ArrowNativeType;
use datafusion_common::{DataFusionError, Result, internal_datafusion_err};
use datafusion_execution::memory_pool::MemoryReservation;
use hashbrown::HashTable;
use hashbrown::hash_table::Entry::{Occupied, Vacant};

/// Initial capacity of the generic join lookup index.
///
/// The lookup index only stores one entry per distinct build hash, while the
/// row-index chain stores one slot per build row. Sizing the lookup index from
/// the total number of build rows therefore wastes bucket memory whenever many
/// rows share the same join key (e.g. a low-cardinality state-code column), so
/// the index starts this small and grows only as distinct hashes are inserted.
const INITIAL_LOOKUP_CAPACITY: usize = 64;

/// Maximum control-group width used by hashbrown 0.17 (SSE2 / wasm SIMD).
/// Scalar and NEON groups are smaller, so this is also an upper bound there.
const MAX_CONTROL_GROUP_WIDTH: usize = 16;

/// Includes hashbrown's trailing control group, which the general-purpose
/// `estimate_memory_size` helper does not account for. Join entries contain a
/// u64 hash and a u32/u64 index, so their size is a multiple of every supported
/// control-group alignment.
fn lookup_allocation_size<T>(capacity: usize) -> Result<usize> {
    if capacity == 0 {
        return Ok(0);
    }
    let overflow =
        || internal_datafusion_err!("overflow while estimating join hash map size");
    let buckets = if capacity < 8 {
        if capacity < 4 { 4 } else { 8 }
    } else {
        (capacity.checked_mul(8).ok_or_else(overflow)? / 7)
            .checked_next_power_of_two()
            .ok_or_else(overflow)?
    };
    buckets
        .checked_mul(size_of::<(u64, T)>() + 1)
        .and_then(|size| size.checked_add(MAX_CONTROL_GROUP_WIDTH))
        .ok_or_else(overflow)
}

/// Charges `reservation` for the temporary peak reached while the lookup index
/// grows.
///
/// `hashbrown` allocates the new, larger bucket array before releasing the old
/// one, so both allocations are live at the same time. Reserving `old + new`
/// before the growth keeps the pool accounting honest, allowing a bounded pool
/// to reject the join before the allocation is made.
fn reserve_growth_peak<T>(
    map: &HashTable<(u64, T)>,
    chain_bytes: usize,
    reservation: &MemoryReservation,
    capacity: usize,
) -> Result<()> {
    let old_table = map.allocation_size();
    let new_table = lookup_allocation_size::<T>(capacity)?;
    let peak = old_table
        .checked_add(new_table)
        .and_then(|peak| peak.checked_add(chain_bytes))
        .ok_or_else(|| {
            internal_datafusion_err!("overflow while estimating join hash map size")
        })?;
    reservation.try_resize(peak)
}

/// Avoid repeated rehashing for batches whose sample has no
/// repeated hashes. Sampling uses bounded stack space and is spread across
/// the batch, rather than trusting a potentially unrepresentative prefix.
/// This is only a hint: if the pool rejects it, normal incremental growth
/// remains available. Later batches may have a different distribution.
fn reserve_from_batch<T>(
    map: &mut HashTable<(u64, T)>,
    num_rows: usize,
    hashes: &[u64],
    reservation: Option<&MemoryReservation>,
) -> Result<()> {
    const SAMPLE_SIZE: usize = 1024;
    let Some(reservation) = reservation else {
        return Ok(());
    };
    if hashes.len() < SAMPLE_SIZE || num_rows <= map.capacity() {
        return Ok(());
    }
    let mut sample = [0; SAMPLE_SIZE];
    // Choose one row from each disjoint stratum, using the hashes already
    // computed to jitter the position. A fixed stride can miss repetitions
    // in periodic inputs, while sampling with replacement repeats rows even
    // when every key is unique. These products are bounded by hashes.len().
    let width = hashes.len() / SAMPLE_SIZE;
    let remainder = hashes.len() % SAMPLE_SIZE;
    for (i, value) in sample.iter_mut().enumerate() {
        let start = i * width + i.min(remainder);
        let len = width + usize::from(i < remainder);
        *value = hashes[start + hashes[start] as usize % len];
        if map.find(*value, |&(hash, _)| hash == *value).is_some() {
            return Ok(());
        }
    }
    sample.sort_unstable();
    if sample.windows(2).any(|pair| pair[0] == pair[1]) {
        return Ok(());
    }
    let chain_bytes = num_rows * size_of::<T>();
    // The first batch only earns capacity for itself. A later batch with
    // distinct, previously unseen samples can justify reserving the rest.
    // Repeated batches therefore do not size the index from total row count.
    let capacity = if map.is_empty() {
        hashes.len().min(num_rows)
    } else {
        num_rows
    };
    if capacity <= map.capacity() {
        return Ok(());
    }
    match reserve_growth_peak(map, chain_bytes, reservation, capacity) {
        Ok(()) => {
            map.reserve(capacity - map.len(), |&(hash, _)| hash);
            settle_accounting(map, chain_bytes, reservation)
        }
        Err(DataFusionError::ResourcesExhausted(_)) => Ok(()),
        Err(error) => Err(error),
    }
}

/// Charges `reservation` for the lookup index and row-index chain after a
/// growth (or a no-op insertion). This releases the resize peak reserved by
/// [`reserve_growth_peak`] once the old bucket array is freed.
fn settle_accounting<T>(
    map: &HashTable<(u64, T)>,
    chain_bytes: usize,
    reservation: &MemoryReservation,
) -> Result<()> {
    let size = map
        .allocation_size()
        .checked_add(chain_bytes)
        .ok_or_else(|| {
            internal_datafusion_err!("overflow while estimating join hash map size")
        })?;
    reservation.try_resize(size)
}

/// Maps a `u64` hash value based on the build side ["on" values] to a list of indices with this key's value.
///
/// The row-index chain is sized for *every* build row so that join
/// multiplicity is preserved, but the lookup index is only sized for the
/// number of *distinct* hashes and therefore starts small and grows on demand.
/// This avoids reserving a hash bucket for every build row when many rows
/// share the same join key.
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
///
/// ## Note on use of this trait as a public API
/// This is currently a public trait but is mainly intended for internal use within DataFusion.
/// For example, we may compare references to `JoinHashMapType` implementations by pointer equality
/// rather than deep equality of contents, as deep equality would be expensive and in our usage
/// patterns it is impossible for two different hash maps to have identical contents in a practical sense.
pub trait JoinHashMapType: Send + Sync {
    fn extend_zero(&mut self, len: usize);

    /// Optionally reserve lookup capacity from a batch of matchable hashes.
    fn reserve_from_batch(&mut self, _hashes: &[u64]) -> Result<()> {
        Ok(())
    }

    /// Inserts or updates the entries yielded by `iter`.
    ///
    /// The lookup index grows on demand as distinct hashes are inserted; a
    /// bounded memory pool may therefore reject the join here if the index
    /// cannot be grown further.
    fn update_from_iter<'a>(
        &mut self,
        iter: Box<dyn Iterator<Item = (usize, &'a u64)> + Send + 'a>,
        deleted_offset: usize,
    ) -> Result<()>;

    /// Returns the number of bytes allocated by the map: the lookup index (the
    /// hash table bucket array) plus the row-index chain.
    fn size(&self) -> usize;

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
        offset: MapOffset,
        input_indices: &mut Vec<u32>,
        match_indices: &mut Vec<u64>,
    ) -> Option<MapOffset>;

    /// Returns a BooleanArray indicating which of the provided hashes exist in the map.
    fn contain_hashes(&self, hash_values: &[u64]) -> BooleanArray;

    /// Returns `true` if the join hash map contains no entries.
    fn is_empty(&self) -> bool;

    /// Returns the number of entries in the join hash map.
    fn len(&self) -> usize;
}

pub struct JoinHashMapU32 {
    // Stores hash value to last row index
    map: HashTable<(u64, u32)>,
    // Stores indices in chained list data structure
    next: Vec<u32>,
    // Reservation owned by this map, grown as the lookup index expands. `None`
    // for maps built without memory accounting (e.g. in tests).
    reservation: Option<MemoryReservation>,
}

impl JoinHashMapU32 {
    #[cfg(test)]
    pub(crate) fn new(map: HashTable<(u64, u32)>, next: Vec<u32>) -> Self {
        Self {
            map,
            next,
            reservation: None,
        }
    }

    pub fn with_capacity(cap: usize) -> Self {
        Self {
            map: HashTable::with_capacity(cap),
            next: vec![0; cap],
            reservation: None,
        }
    }

    /// Creates a map whose row-index chain is sized for every one of `num_rows`
    /// build rows, but whose lookup index starts small and grows on demand.
    ///
    /// `reservation` is charged for the chain and the small initial index; a
    /// private handle to that reservation is retained by the map and grown as
    /// the index expands.
    pub(crate) fn with_capacity_and_reservation(
        num_rows: usize,
        reservation: &mut MemoryReservation,
    ) -> Result<Self> {
        let capacity = num_rows.min(INITIAL_LOOKUP_CAPACITY);
        let chain_bytes = num_rows.checked_mul(size_of::<u32>()).ok_or_else(|| {
            internal_datafusion_err!("overflow while estimating join hash map size")
        })?;
        let initial = lookup_allocation_size::<u32>(capacity)?
            .checked_add(chain_bytes)
            .ok_or_else(|| {
                internal_datafusion_err!("overflow while estimating join hash map size")
            })?;
        reservation.try_grow(initial)?;
        let reservation = reservation.split(initial);
        let map = HashTable::with_capacity(capacity);
        let next = vec![0; num_rows];
        settle_accounting(&map, chain_bytes, &reservation)?;
        Ok(Self {
            map,
            next,
            reservation: Some(reservation),
        })
    }
}

impl Debug for JoinHashMapU32 {
    fn fmt(&self, _f: &mut fmt::Formatter) -> fmt::Result {
        Ok(())
    }
}

impl JoinHashMapType for JoinHashMapU32 {
    fn extend_zero(&mut self, _: usize) {}

    fn reserve_from_batch(&mut self, hashes: &[u64]) -> Result<()> {
        reserve_from_batch(
            &mut self.map,
            self.next.len(),
            hashes,
            self.reservation.as_ref(),
        )
    }

    fn update_from_iter<'a>(
        &mut self,
        iter: Box<dyn Iterator<Item = (usize, &'a u64)> + Send + 'a>,
        deleted_offset: usize,
    ) -> Result<()> {
        update_from_iter::<u32>(
            &mut self.map,
            &mut self.next,
            iter,
            deleted_offset,
            self.reservation.as_ref(),
        )
    }

    fn size(&self) -> usize {
        self.map.allocation_size() + self.next.len() * size_of::<u32>()
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
        valid_keys: Option<&NullBuffer>,
        limit: usize,
        offset: MapOffset,
        input_indices: &mut Vec<u32>,
        match_indices: &mut Vec<u64>,
    ) -> Option<MapOffset> {
        get_matched_indices_with_limit_offset::<u32>(
            &self.map,
            &self.next,
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
}

pub struct JoinHashMapU64 {
    // Stores hash value to last row index
    map: HashTable<(u64, u64)>,
    // Stores indices in chained list data structure
    next: Vec<u64>,
    // Reservation owned by this map, grown as the lookup index expands. `None`
    // for maps built without memory accounting (e.g. in tests).
    reservation: Option<MemoryReservation>,
}

impl JoinHashMapU64 {
    #[cfg(test)]
    pub(crate) fn new(map: HashTable<(u64, u64)>, next: Vec<u64>) -> Self {
        Self {
            map,
            next,
            reservation: None,
        }
    }

    pub fn with_capacity(cap: usize) -> Self {
        Self {
            map: HashTable::with_capacity(cap),
            next: vec![0; cap],
            reservation: None,
        }
    }

    /// Creates a map whose row-index chain is sized for every one of `num_rows`
    /// build rows, but whose lookup index starts small and grows on demand.
    ///
    /// `reservation` is charged for the chain and the small initial index; a
    /// private handle to that reservation is retained by the map and grown as
    /// the index expands.
    pub(crate) fn with_capacity_and_reservation(
        num_rows: usize,
        reservation: &mut MemoryReservation,
    ) -> Result<Self> {
        let capacity = num_rows.min(INITIAL_LOOKUP_CAPACITY);
        let chain_bytes = num_rows.checked_mul(size_of::<u64>()).ok_or_else(|| {
            internal_datafusion_err!("overflow while estimating join hash map size")
        })?;
        let initial = lookup_allocation_size::<u64>(capacity)?
            .checked_add(chain_bytes)
            .ok_or_else(|| {
                internal_datafusion_err!("overflow while estimating join hash map size")
            })?;
        reservation.try_grow(initial)?;
        let reservation = reservation.split(initial);
        let map = HashTable::with_capacity(capacity);
        let next = vec![0; num_rows];
        settle_accounting(&map, chain_bytes, &reservation)?;
        Ok(Self {
            map,
            next,
            reservation: Some(reservation),
        })
    }
}

impl Debug for JoinHashMapU64 {
    fn fmt(&self, _f: &mut fmt::Formatter) -> fmt::Result {
        Ok(())
    }
}

impl JoinHashMapType for JoinHashMapU64 {
    fn extend_zero(&mut self, _: usize) {}

    fn reserve_from_batch(&mut self, hashes: &[u64]) -> Result<()> {
        reserve_from_batch(
            &mut self.map,
            self.next.len(),
            hashes,
            self.reservation.as_ref(),
        )
    }

    fn update_from_iter<'a>(
        &mut self,
        iter: Box<dyn Iterator<Item = (usize, &'a u64)> + Send + 'a>,
        deleted_offset: usize,
    ) -> Result<()> {
        update_from_iter::<u64>(
            &mut self.map,
            &mut self.next,
            iter,
            deleted_offset,
            self.reservation.as_ref(),
        )
    }

    fn size(&self) -> usize {
        self.map.allocation_size() + self.next.len() * size_of::<u64>()
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
        valid_keys: Option<&NullBuffer>,
        limit: usize,
        offset: MapOffset,
        input_indices: &mut Vec<u32>,
        match_indices: &mut Vec<u64>,
    ) -> Option<MapOffset> {
        get_matched_indices_with_limit_offset::<u64>(
            &self.map,
            &self.next,
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
}

use crate::joins::MapOffset;
use crate::joins::chain::traverse_chain;

pub fn update_from_iter<'a, T>(
    map: &mut HashTable<(u64, T)>,
    next: &mut [T],
    iter: Box<dyn Iterator<Item = (usize, &'a u64)> + Send + 'a>,
    deleted_offset: usize,
    reservation: Option<&MemoryReservation>,
) -> Result<()>
where
    T: Copy + TryFrom<usize> + PartialOrd,
    <T as TryFrom<usize>>::Error: Debug,
{
    // The row-index chain is allocated once and never resized, so its size is
    // constant and included in every accounting update below.
    let chain_bytes = size_of_val(next);

    let mut capacity = map.capacity();

    for (row, &hash_value) in iter {
        // If the next insertion grows the lookup index, reserve the transient
        // resize peak before allocating so a bounded pool fails fast.
        if let Some(reservation) = reservation
            && map.len() == capacity
        {
            // An existing hash only updates its chain, even when all buckets
            // are occupied. Do not require any additional pool capacity.
            if let Some((_, index)) =
                map.find_mut(hash_value, |&(hash, _)| hash == hash_value)
            {
                next[row - deleted_offset] = *index;
                *index = T::try_from(row + 1).unwrap();
                continue;
            }
            reserve_growth_peak(map, chain_bytes, reservation, map.len() + 1)?;
            map.reserve(1, |&(hash, _)| hash);
            capacity = map.capacity();
            settle_accounting(map, chain_bytes, reservation)?;
        }

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

    Ok(())
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

#[expect(clippy::too_many_arguments)]
pub fn get_matched_indices_with_limit_offset<T>(
    map: &HashTable<(u64, T)>,
    next_chain: &[T],
    hash_values: &[u64],
    valid_keys: Option<&NullBuffer>,
    limit: usize,
    offset: MapOffset,
    input_indices: &mut Vec<u32>,
    match_indices: &mut Vec<u64>,
) -> Option<MapOffset>
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
    if map.len() == next_chain.len() {
        let start = offset.0;
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
            Some((end, None))
        };
    }

    let mut remaining_output = limit;

    // Calculate initial `hash_values` index before iterating
    let to_skip = match offset {
        // None `initial_next_idx` indicates that `initial_idx` processing hasn't been started
        (idx, None) => idx,
        // Zero `initial_next_idx` indicates that `initial_idx` has been processed during
        // previous iteration, and it should be skipped
        (idx, Some(0)) => idx + 1,
        // Otherwise, process remaining `initial_idx` matches by traversing `next_chain`,
        // to start with the next index
        (idx, Some(next_idx)) => {
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

    use datafusion_common::utils::memory::estimate_memory_size;

    use datafusion_execution::memory_pool::{
        GreedyMemoryPool, MemoryConsumer, MemoryPool, UnboundedMemoryPool,
    };
    use std::sync::Arc;

    /// Builds a reservation-backed `JoinHashMapU32` and returns the pool, the
    /// caller's reservation handle and the map.
    fn reservation_backed_map(
        num_rows: usize,
    ) -> (Arc<dyn MemoryPool>, MemoryReservation, JoinHashMapU32) {
        let pool: Arc<dyn MemoryPool> = Arc::new(UnboundedMemoryPool::default());
        let mut reservation = MemoryConsumer::new("join-hash-map-test").register(&pool);
        let map =
            JoinHashMapU32::with_capacity_and_reservation(num_rows, &mut reservation)
                .unwrap();
        (pool, reservation, map)
    }

    #[test]
    fn test_contain_hashes() {
        let mut hash_map = JoinHashMapU32::with_capacity(10);
        hash_map
            .update_from_iter(Box::new([10u64, 20u64, 30u64].iter().enumerate()), 0)
            .unwrap();

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
        hash_map
            .update_from_iter(Box::new([10u64, 20u64, 30u64].iter().enumerate()), 0)
            .unwrap();

        let probe_hashes = vec![10, 20, 30];
        // The probe row for hash 20 has a NULL key and must not match.
        let valid_keys = NullBuffer::from(vec![true, false, true]);

        let mut input_indices = vec![];
        let mut match_indices = vec![];
        let next_offset = hash_map.get_matched_indices_with_limit_offset(
            &probe_hashes,
            Some(&valid_keys),
            8192,
            (0, None),
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
        hash_map
            .update_from_iter(
                Box::new([10u64, 20u64, 10u64, 20u64].iter().enumerate()),
                0,
            )
            .unwrap();

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
            (0, None),
            &mut input_indices,
            &mut match_indices,
        );

        assert_eq!(next_offset, None);
        assert_eq!(input_indices, vec![1, 1]);
        assert_eq!(match_indices, vec![3, 1]);
    }

    #[test]
    fn test_low_cardinality_lookup_index_stays_small() {
        let num_rows = 10_000;
        let (pool, _reservation, mut hash_map) = reservation_backed_map(num_rows);

        // Every build row shares the same hash value.
        let hashes = vec![7u64; num_rows];
        hash_map
            .update_from_iter(Box::new(hashes.iter().enumerate()), 0)
            .unwrap();

        assert_eq!(hash_map.len(), 1);
        // The row-index chain keeps a slot for every build row ...
        assert_eq!(hash_map.next.len(), num_rows);
        // ... but the lookup index is not sized for every build row.
        assert!(
            hash_map.map.capacity() < num_rows,
            "lookup index capacity {} should stay below the row count {num_rows}",
            hash_map.map.capacity()
        );
        // The reservation tracks the actual allocation.
        assert_eq!(pool.reserved(), hash_map.size());
    }

    #[test]
    fn test_reservation_tracks_lookup_index_growth() {
        let num_rows = 5_000;
        let (pool, _reservation, mut hash_map) = reservation_backed_map(num_rows);

        // Mostly-unique hashes force the lookup index to grow past its small
        // initial capacity.
        let hashes: Vec<u64> = (0..num_rows as u64).collect();
        hash_map
            .update_from_iter(Box::new(hashes.iter().enumerate()), 0)
            .unwrap();

        assert_eq!(hash_map.len(), num_rows);
        assert!(hash_map.map.capacity() >= num_rows);
        assert_eq!(pool.reserved(), hash_map.size());
    }

    #[test]
    fn test_low_cardinality_fits_bounded_pool() {
        let num_rows = 100_000;
        // Memory that sizing the lookup index from every build row would need.
        let row_sized =
            estimate_memory_size::<(u32, u64)>(num_rows, size_of::<JoinHashMapU32>())
                .unwrap();
        let limit = row_sized / 4;
        let pool: Arc<dyn MemoryPool> = Arc::new(GreedyMemoryPool::new(limit));
        let mut reservation = MemoryConsumer::new("bounded-join").register(&pool);

        let mut hash_map =
            JoinHashMapU32::with_capacity_and_reservation(num_rows, &mut reservation)
                .unwrap();
        let hashes = vec![123u64; num_rows];
        hash_map
            .update_from_iter(Box::new(hashes.iter().enumerate()), 0)
            .unwrap();

        assert_eq!(hash_map.len(), 1);
        assert!(pool.reserved() <= limit);
        assert!(
            pool.reserved() < row_sized,
            "reserved {} should be far below the row-sized estimate {row_sized}",
            pool.reserved()
        );
    }

    #[test]
    fn test_unique_keys_use_full_lookup_index() {
        let num_rows = 1_000;
        let (_pool, _reservation, mut hash_map) = reservation_backed_map(num_rows);

        let hashes: Vec<u64> = (0..num_rows as u64).map(|i| i * 2).collect();
        hash_map
            .update_from_iter(Box::new(hashes.iter().enumerate()), 0)
            .unwrap();

        // `map.len() == next.len()` selects the unique-key fast path.
        let probe = hashes.clone();
        let mut input_indices = vec![];
        let mut match_indices = vec![];
        let next = hash_map.get_matched_indices_with_limit_offset(
            &probe,
            None,
            8192,
            (0, None),
            &mut input_indices,
            &mut match_indices,
        );

        assert_eq!(next, None);
        assert_eq!(input_indices, (0..num_rows as u32).collect::<Vec<_>>());
        assert_eq!(match_indices, (0..num_rows as u64).collect::<Vec<_>>());
    }

    fn bounded_map(
        rows: usize,
        limit: usize,
        wide: bool,
    ) -> (Arc<dyn MemoryPool>, Box<dyn JoinHashMapType>) {
        let pool: Arc<dyn MemoryPool> = Arc::new(GreedyMemoryPool::new(limit));
        let mut reservation = MemoryConsumer::new("bounded-map").register(&pool);
        let map: Box<dyn JoinHashMapType> = if wide {
            Box::new(
                JoinHashMapU64::with_capacity_and_reservation(rows, &mut reservation)
                    .unwrap(),
            )
        } else {
            Box::new(
                JoinHashMapU32::with_capacity_and_reservation(rows, &mut reservation)
                    .unwrap(),
            )
        };
        (pool, map)
    }

    #[test]
    fn test_full_lookup_accepts_duplicate_without_extra_memory() {
        for wide in [false, true] {
            let table = HashTable::<(u64, u64)>::with_capacity(INITIAL_LOOKUP_CAPACITY);
            let capacity = table.capacity();
            let rows = capacity + 1;
            let chain_bytes = rows * if wide { 8 } else { 4 };
            let limit = lookup_allocation_size::<u64>(INITIAL_LOOKUP_CAPACITY).unwrap()
                + chain_bytes;
            let (pool, mut map) = bounded_map(rows, limit, wide);
            // The initial estimate is conservative on platforms with smaller
            // control groups. Fill that slack so insertion has no spare budget.
            let slack = MemoryConsumer::new("unused-control-group-space").register(&pool);
            slack.try_grow(limit - map.size()).unwrap();
            let hashes: Vec<_> = (0..capacity as u64).chain(std::iter::once(0)).collect();
            map.update_from_iter(Box::new(hashes.iter().enumerate()), 0)
                .unwrap();
            assert_eq!(map.len(), capacity);
            assert_eq!(pool.reserved(), limit);
            let (input, matched) =
                map.get_matched_indices(Box::new(std::iter::once(&0).enumerate()), None);
            assert_eq!(input, vec![0, 0]);
            assert_eq!(matched, vec![capacity as u64, 0]);
            drop(map);
            assert_eq!(pool.reserved(), slack.size());
            drop(slack);
            assert_eq!(pool.reserved(), 0);
        }
    }

    #[test]
    fn test_initial_reservation_precedes_chain_allocation() {
        let pool: Arc<dyn MemoryPool> = Arc::new(GreedyMemoryPool::new(0));
        let mut reservation =
            MemoryConsumer::new("reject-before-allocation").register(&pool);
        // These lengths exceed Vec's isize::MAX byte limit. Allocating first
        // would panic; the pool must reject them without reaching Vec.
        let rows = isize::MAX as usize / size_of::<u32>() + 1;
        assert!(matches!(
            JoinHashMapU32::with_capacity_and_reservation(rows, &mut reservation),
            Err(DataFusionError::ResourcesExhausted(_))
        ));
        let rows = isize::MAX as usize / size_of::<u64>() + 1;
        assert!(matches!(
            JoinHashMapU64::with_capacity_and_reservation(rows, &mut reservation),
            Err(DataFusionError::ResourcesExhausted(_))
        ));
        assert_eq!(pool.reserved(), 0);
    }

    #[test]
    fn test_lookup_estimate_covers_hashbrown_allocations() {
        for capacity in (0..130).chain([255, 256, 257, 1023, 1024, 8192, 65536]) {
            assert!(
                lookup_allocation_size::<u32>(capacity).unwrap()
                    >= HashTable::<(u64, u32)>::with_capacity(capacity).allocation_size()
            );
            assert!(
                lookup_allocation_size::<u64>(capacity).unwrap()
                    >= HashTable::<(u64, u64)>::with_capacity(capacity).allocation_size()
            );
        }
        assert!(lookup_allocation_size::<u64>(usize::MAX).is_err());
    }

    #[test]
    fn test_growth_requires_old_and_new_allocations() {
        for wide in [false, true] {
            let table = HashTable::<(u64, u64)>::with_capacity(INITIAL_LOOKUP_CAPACITY);
            let capacity = table.capacity();
            let rows = capacity + 1;
            let chain_bytes = rows * if wide { 8 } else { 4 };
            let new_size = lookup_allocation_size::<u64>(rows).unwrap();
            let peak = chain_bytes + table.allocation_size() + new_size;
            let hashes: Vec<_> = (0..rows as u64).collect();
            let (pool, mut map) = bounded_map(rows, peak - 1, wide);
            let initial = map.size();
            assert!(matches!(
                map.update_from_iter(Box::new(hashes.iter().enumerate()), 0),
                Err(DataFusionError::ResourcesExhausted(_))
            ));
            assert_eq!(map.len(), capacity);
            assert_eq!(map.size(), initial);
            assert_eq!(pool.reserved(), initial);
            drop(map);
            assert_eq!(pool.reserved(), 0);

            let (pool, mut map) = bounded_map(rows, peak, wide);
            map.update_from_iter(Box::new(hashes.iter().enumerate()), 0)
                .unwrap();
            assert_eq!(map.len(), rows);
            assert_eq!(pool.reserved(), map.size());
            assert!(pool.reserved() < peak);
            drop(map);
            assert_eq!(pool.reserved(), 0);
        }
    }

    #[test]
    fn test_unique_sample_presizes_index() {
        let rows = 8192;
        let (_, _, mut map) = reservation_backed_map(rows);
        let hashes: Vec<_> = (0..rows as u64).collect();
        map.reserve_from_batch(&hashes).unwrap();
        assert!(map.map.capacity() >= rows);
        let size = map.size();
        map.update_from_iter(Box::new(hashes.iter().enumerate()), 0)
            .unwrap();
        assert_eq!(map.size(), size);
    }

    #[test]
    fn test_repeated_sample_keeps_small_index() {
        let rows = 8192;
        let (_, _, mut map) = reservation_backed_map(rows);
        let hashes: Vec<_> = (0..rows as u64).map(|i| i % 8).collect();
        let size = map.size();
        map.reserve_from_batch(&hashes).unwrap();
        map.update_from_iter(Box::new(hashes.iter().enumerate()), 0)
            .unwrap();
        assert_eq!(map.size(), size);
        assert_eq!(map.len(), 8);
    }

    #[test]
    fn test_rejected_sample_hint_falls_back_to_incremental_growth() {
        for wide in [false, true] {
            let rows = 100_000;
            let chain_bytes = rows * if wide { 8 } else { 4 };
            let (pool, mut map) = bounded_map(rows, chain_bytes + 120_000, wide);
            let hashes: Vec<_> = (0..rows as u64).map(|i| i % 2048).collect();
            // Two distinct batches suggest high cardinality, but the rest
            // repeat them. Rejecting the full-size hint must not fail the join.
            for (batch, chunk) in hashes.chunks(1024).enumerate() {
                map.reserve_from_batch(chunk).unwrap();
                map.update_from_iter(
                    Box::new(
                        chunk
                            .iter()
                            .enumerate()
                            .map(move |(row, hash)| (batch * 1024 + row, hash)),
                    ),
                    0,
                )
                .unwrap();
            }
            assert_eq!(map.len(), 2048);
            assert_eq!(pool.reserved(), map.size());
            drop(map);
            assert_eq!(pool.reserved(), 0);
        }
    }

    #[test]
    fn test_repeated_batches_do_not_reserve_for_total_rows() {
        let rows = 100_000;
        let (_, _, mut map) = reservation_backed_map(rows);
        let hashes: Vec<_> = (0..rows as u64).map(|i| i % 8192).collect();
        for (batch, chunk) in hashes.chunks(8192).enumerate() {
            map.reserve_from_batch(chunk).unwrap();
            map.update_from_iter(
                Box::new(
                    chunk
                        .iter()
                        .enumerate()
                        .map(move |(row, hash)| (batch * 8192 + row, hash)),
                ),
                0,
            )
            .unwrap();
        }
        assert_eq!(map.len(), 8192);
        assert!(map.map.capacity() < rows / 2);
    }

    #[test]
    fn test_periodic_batch_does_not_look_unique() {
        use std::hash::{BuildHasher, BuildHasherDefault, DefaultHasher};
        let rows = 200_000;
        let (_, _, mut map) = reservation_backed_map(rows);
        let state = BuildHasherDefault::<DefaultHasher>::default();
        let hashes: Vec<_> = (0..rows).map(|i| state.hash_one(i % 8192)).collect();
        let initial = map.size();
        map.reserve_from_batch(&hashes).unwrap();
        assert_eq!(map.size(), initial);
        map.update_from_iter(Box::new(hashes.iter().enumerate()), 0)
            .unwrap();
        assert_eq!(map.len(), 8192);
        assert!(map.map.capacity() < rows / 2);
    }
}
