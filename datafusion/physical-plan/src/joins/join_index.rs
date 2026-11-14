// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: See the NOTICE file distributed with this work for additional information regarding copyright ownership
// SPDX-FileContributor: Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements

//! Immutable, partition-local join index, independent of payload storage.
//!
//! Lookup resolves exact keys; the join operator handles output batching and
//! residual predicates. For build keys `[A, B, A]`, the directory maps `A` to
//! group `[0, 2]` and `B` to row `1`. An all-unique build returns verified row
//! pairs directly, without allocating or decoding group handles.
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

/// Conceptually, a hash map from exact join keys (one or more columns) to sets of
/// row IDs in the build-side batch. For example:
///
/// Probe side row has join keys`(A, tag1)`, it maps to build rows `[3, 5, 100]`.
///
/// # Physical Design
///
/// A matching key identifies either one build row or a group of build rows:
///
/// ```text
/// (B, tag2) -> One(8)                 -> build row 8
/// (A, tag1) -> Group(0)               -> build rows [3, 5, 100]
/// ```
///
/// Lookup returns row IDs or group handles, not materialized joined rows.
/// For example, if one probe row matches one million build rows, lookup returns
/// a single group handle without expanding those matches into row pairs.
///
/// The join processor, `BatchedJoinProcessor`, incrementally expands these handles
/// into bounded chunks of row pairs, applies the residual join filter, and
/// materializes the surviving rows.
///
/// ## Implementation
///
/// Hash-based directories use [`hashbrown::HashTable`]. Grouped lookup results
/// are stored in [`MatchHandleArray`].
#[derive(Debug)]
pub(crate) struct JoinIndexMap {
    index: IndexWidth,
    random_state: RandomState,
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
    /// - `keys`: evaluated join keys
    /// - `null_equality`: NULL matching semantics for the join.
    /// - `dense_range`: See the Specializations section below.
    ///
    /// # Specializations
    ///
    /// The index uses the following specializations:
    /// - If `dense_range` is provided for a single integer key column, use a
    ///   direct array map. For example, `Some((0, 100))` specifies the inclusive
    ///   range `[0, 100]` containing all non-NULL build keys.
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

    /// Resolve a whole probe batch to exact-equality-matched group handles.
    ///
    /// See coments on [`JoinIndexMap`] for the explanations for 'group handles' and
    /// more detials.
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

    pub(crate) fn contain_hashes(&self, hashes: &[u64]) -> BooleanArray {
        self.index.contain_hashes(hashes, &self.random_state)
    }

    pub(crate) fn contain_keys(&self, keys: &[ArrayRef]) -> Result<BooleanArray> {
        self.index.contain_keys(keys)
    }
}

/// Grow before allocating, including the construction-only buffers. Geometric
/// growth avoids a reservation and reallocation for every duplicate.
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
