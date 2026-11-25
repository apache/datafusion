// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: See the NOTICE file distributed with this work for additional information regarding copyright ownership
// SPDX-FileContributor: Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements

//! Immutable join index, independent of payload storage.
//!
//! Lookup resolves exact keys; the join operator handles output batching and
//! residual predicates. For build keys `[A, B, A]`, the directory maps `A` to
//! group `[0, 2]` and `B` to row `1`. An all-unique build returns verified row
//! pairs, avoiding handle-to-pair conversion during output.
//!
//! `directory` owns layouts and construction, `keys` owns hashing and exact
//! comparison, and `matches` owns grouped row IDs and lookup-result memory.

mod directory;
mod keys;
mod matches;

use std::mem::size_of;
use std::ops::Deref;

use arrow::array::{ArrayRef, BooleanArray};
use datafusion_common::hash_utils::RandomState;
use datafusion_common::{NullEquality, Result};
use datafusion_execution::memory_pool::MemoryReservation;

use directory::IndexWidth;
pub(crate) use keys::LookupScratch;
pub(crate) use matches::{
    GroupRows, LookupResult, MatchHandle, MatchHandleArray, MatchHandles, RowId,
    UniqueMatches,
};

/// Maps exact join keys (one or more columns) to matching build-row IDs.
/// Lookup returns row pairs or group handles for the join processor to consume.
/// See [`directory::JoinHashIndex`] for the storage design and a small example.
#[derive(Debug)]
pub(crate) struct JoinIndexMap {
    /// Index implementation specialized on row-id width.
    index: IndexWidth,
    random_state: RandomState,
    /// See [`NullEquality`] for semantics.
    ///
    /// For implementation:
    /// - `NullEqualsNothing`: skip NULL-key rows during build and probe.
    /// - `NullEqualsNull`: hash and compare NULL keys
    null_equality: NullEquality,
}

/// Keeps the index charged while processors or dynamic filters retain it.
pub(crate) struct ReservedJoinIndex {
    index: JoinIndexMap,
    // Drop the index before releasing its reservation.
    reservation: MemoryReservation,
}

impl ReservedJoinIndex {
    pub(crate) fn new(index: JoinIndexMap, reservation: MemoryReservation) -> Self {
        Self { index, reservation }
    }

    pub(crate) fn reserved_bytes(&self) -> usize {
        self.reservation.size()
    }
}

impl Deref for ReservedJoinIndex {
    type Target = JoinIndexMap;

    fn deref(&self) -> &Self::Target {
        &self.index
    }
}

impl JoinIndexMap {
    /// Build a join index.
    ///
    /// # Arguments
    ///
    /// - `keys`: evaluated build-side join keys.
    /// - `random_state`: hashing state shared by build and probe.
    /// - `null_equality`: NULL matching semantics for the join.
    /// - `dense_range`: `Some((min, max))` enables direct indexing; `None` keeps
    ///   hashing. See the Specializations section below.
    /// - `reservation`: memory budget for index storage and build scratch.
    ///
    /// # Specializations
    ///
    /// The index uses the following specializations:
    /// - If `dense_range` is provided for a single integer key column, its
    ///   inclusive `(min, max)` bounds must cover all non-NULL build keys.
    ///   Use one array slot per value in that range, including gaps:
    ///   `Some((10, 12))` allocates three slots; key `12` maps to slot `2`.
    /// - Otherwise, if `keys` contains a single integer column, keep a hash table
    ///   but store each join key inline instead of its cached hash.
    pub(crate) fn try_new(
        keys: &[ArrayRef],
        random_state: &RandomState,
        null_equality: NullEquality,
        dense_range: Option<(u64, u64)>,
        reservation: &MemoryReservation,
    ) -> Result<Self> {
        Ok(Self {
            index: IndexWidth::build(
                keys,
                random_state,
                null_equality,
                dense_range,
                reservation,
            )?,
            random_state: random_state.clone(),
            null_equality,
        })
    }

    /// Resolve a probe batch to verified row pairs or row/group handles.
    ///
    /// See comments on [`JoinIndexMap`] for the explanations for 'group handles' and
    /// more details.
    pub(crate) fn lookup_batch(
        &self,
        build_keys: &[ArrayRef],
        probe_keys: &[ArrayRef],
        scratch: &mut LookupScratch,
    ) -> Result<LookupResult> {
        self.index.lookup_batch(
            build_keys,
            probe_keys,
            self.null_equality,
            &self.random_state,
            scratch,
        )
    }

    /// Borrow a duplicate group's row IDs in their original build order.
    #[inline]
    pub(crate) fn group_rows(&self, group: u64) -> GroupRows<'_> {
        self.index.group_rows(group)
    }

    pub(crate) fn is_dense(&self) -> bool {
        self.index.is_dense()
    }

    pub(crate) fn num_of_distinct_key(&self) -> usize {
        self.index.num_of_distinct_key()
    }

    pub(crate) fn is_empty(&self) -> bool {
        self.num_of_distinct_key() == 0
    }

    /// Hash-based counterpart to [`Self::contain_keys`]; collisions may produce
    /// false positives.
    pub(crate) fn contain_hashes(&self, hashes: &[u64]) -> BooleanArray {
        self.index.contain_hashes(hashes, &self.random_state)
    }

    /// Return a mask indicating which keys exist in the dense integer directory.
    ///
    /// Dynamic filters call this through [`HashTableLookupExpr`] to discard
    /// unmatched probe rows before the join.
    ///
    /// [`HashTableLookupExpr`]: crate::joins::hash_join::HashTableLookupExpr
    pub(crate) fn contain_keys(&self, keys: &[ArrayRef]) -> Result<BooleanArray> {
        self.index.contain_keys(keys)
    }
}

/// Grow reusable probe buffers geometrically, charging memory before allocating.
fn reserve<T>(
    buffer: &mut Vec<T>,
    needed: usize,
    reservation: &MemoryReservation,
) -> Result<()> {
    if needed > buffer.capacity() {
        let capacity = needed.max(buffer.capacity().saturating_mul(2)).max(8);
        reservation.try_grow((capacity - buffer.capacity()) * size_of::<T>())?;
        buffer.reserve_exact(capacity - buffer.len());
    }
    Ok(())
}
