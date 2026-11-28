// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: See the NOTICE file distributed with this work for additional information regarding copyright ownership
// SPDX-FileContributor: Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements

//! General-key directory with batched comparison of candidate representatives.
//!
//! A hash match is only a candidate. Compare representatives column by column,
//! once per key rather than per duplicate. If `A` and `B` share a hash, rejecting
//! representative `A` must retry the directory: another entry may hold `B`.
//! Both paths resolve candidates into handles. Unique builds convert these to
//! row pairs at return; duplicate keys retain handles for later expansion.

use std::mem::size_of;
use std::sync::Arc;

use arrow::array::ArrayRef;
use arrow::buffer::NullBuffer;
use arrow::compute::SortOptions;
use datafusion_common::float_semantics::{has_float_leaf, normalize};
use datafusion_common::hash_utils::RandomState;
use datafusion_common::utils::memory::estimate_memory_size;
use datafusion_common::{NullEquality, Result};
use datafusion_execution::memory_pool::MemoryReservation;
use hashbrown::{HashTable, hash_table::Entry};

use super::super::keys::{
    LookupScratch, compare_column, for_each_hash_chunk, hash_probe_keys,
};
use super::super::matches::{
    HandleBuilder, JoinHashLookup, MatchHandle, MatchedGroupsBuilder, RowId,
};
use super::super::reserve;
use super::Probe;
use crate::joins::utils::{JoinKeyComparator, matchable_join_keys};

/// One directory entry per equal join key(s).
///
/// See [`JoinHashIndex`](super::JoinHashIndex) for the design overview.
///
/// # Example
///
/// Build keys `[A, B, A]`, with zero-based row IDs, produce:
///
/// ```text
/// key | hash | representative_row | group_plus_one | matches()
/// ----+------+--------------------+----------------+------------------------
/// A   | h(A) | 0                  | 1              | Group(0) -> rows [0, 2]
/// B   | h(B) | 1                  | 0              | One(1)   -> row 1
/// ```
///
/// `h(key)` is the key's hash. `representative_row` indexes the concatenated
/// build batch, letting lookup read the actual key to verify equality.
/// Duplicate members are stored separately; `group_plus_one = 1` refers to
/// group 0, while zero means the representative is the only matching row.
#[derive(Clone, Copy, Debug)]
struct KeyEntry<T> {
    hash: u64,
    representative_row: T,
    // Zero means One(representative_row); otherwise this is Group(id - 1).
    group_plus_one: T,
}

impl<T: RowId> KeyEntry<T> {
    fn matches(&self) -> MatchHandle<T> {
        match self.group_plus_one.as_index() {
            0 => MatchHandle::One(self.representative_row),
            group => MatchHandle::Group(T::from_index(group - 1)),
        }
    }
}

/// Hash candidates identify representative build rows whose keys establish equality.
///
/// See [`JoinHashIndex`](super::JoinHashIndex) for the design overview.
#[derive(Debug)]
pub(super) struct GeneralDirectory<T> {
    table: HashTable<KeyEntry<T>>,
    /// Every matchable build key has at most one row, including NULL.
    build_unique: bool,
}

impl<T: RowId> GeneralDirectory<T> {
    /// Only exact matches share a group, even when different keys have the same hash.
    pub(super) fn build(
        keys: &[ArrayRef],
        random_state: &RandomState,
        hash_chunk_size: usize,
        valid: Option<&NullBuffer>,
        reservation: &MemoryReservation,
        groups: &mut MatchedGroupsBuilder<T>,
    ) -> Result<Self> {
        reservation.try_grow(estimate_memory_size::<KeyEntry<T>>(
            keys[0].len(),
            size_of::<HashTable<KeyEntry<T>>>(),
        )?)?;
        let mut table: HashTable<KeyEntry<T>> = HashTable::with_capacity(keys[0].len());
        // NULL != NULL rows are excluded before comparison. Using NULL
        // equality here avoids rebuilding dictionary logical-null masks;
        // it does not change equality of nested children.
        let comparator = JoinKeyComparator::new_with_normalized_left(
            keys,
            keys,
            &vec![SortOptions::default(); keys.len()],
            NullEquality::NullEqualsNull,
        )?;
        for_each_hash_chunk(keys, random_state, hash_chunk_size, |start, hashes| {
            for (offset, &hash) in hashes.iter().enumerate() {
                let row = start + offset;
                if valid.is_some_and(|valid| valid.is_null(row)) {
                    continue;
                }
                match table.entry(
                    hash,
                    |entry| {
                        entry.hash == hash
                            && comparator
                                .is_equal(entry.representative_row.as_index(), row)
                    },
                    |entry| entry.hash,
                ) {
                    Entry::Occupied(mut entry) => {
                        let entry = entry.get_mut();
                        let mut handle = entry.matches();
                        groups.add_duplicate(&mut handle, T::from_index(row));
                        let MatchHandle::Group(group) = handle else {
                            unreachable!()
                        };
                        entry.group_plus_one = T::from_index(group.as_index() + 1);
                    }
                    Entry::Vacant(entry) => {
                        let row = T::from_index(row);
                        entry.insert(KeyEntry {
                            hash,
                            representative_row: row,
                            group_plus_one: T::default(),
                        });
                    }
                }
            }
        })?;
        Ok(Self {
            table,
            build_unique: !groups.has_groups(),
        })
    }

    /// Find exact join-key matches for the current probe batch.
    ///
    /// # Implementation
    ///
    /// - Fast pre-filter: collect hash-only candidates, then compare keys column by
    ///   column.
    /// - Retry rejected candidates with full key equality.
    /// - Return all match handles (with unique build-side specialization).
    ///
    /// # Example
    ///
    /// With build keys `[A, B, A]` and probe keys `[B, X, A]` (no collisions):
    ///
    /// ```text
    /// candidate (probe row, build representative): [(0, 1), (2, 0)]
    /// verified handles, indexed by probe row:     [One(1), miss, Group(0)]
    /// ```
    ///
    /// Group 0 expands to build rows `[0, 2]` later. If all build keys are unique,
    /// the handles are converted to row pairs using the candidate buffers.
    pub(super) fn lookup_batch(
        &self,
        probe: &Probe<'_>,
        scratch: &mut LookupScratch,
    ) -> Result<JoinHashLookup<T>> {
        // step1: Prepare probe scratch and reserve space for lookup results.
        let len = probe.probe_keys[0].len();
        prepare_probe_scratch(probe, scratch)?;

        let mut handles = HandleBuilder::new(len, &scratch.reservation)?;
        // step2: Find candidates using full hash equality, not actual join keys.
        // Record at most one candidate per eligible probe row in scratch:
        //
        // Build batch
        // row  key  hash
        //   0   A     7
        //   1   B     7
        //   2   C     9
        //
        // Probe batch
        // row  key  hash
        //   0   B     7
        //   1   C     9
        //
        // If the hash-7 lookup finds A first:
        //   probe_candidate_rows = [0, 1] // indices into the current probe batch
        //   representatives      = [0, 2] // indices into the concatenated build batch
        // These pairs mean (B, A) and (C, C); equality is still unverified.
        for row in 0..len {
            if scratch
                .probe_valid
                .as_ref()
                .is_some_and(|valid| valid.is_null(row))
            {
                continue;
            }
            let hash = scratch.probe_hashes[row];
            if let Some(entry) = self.table.find(hash, |entry| entry.hash == hash) {
                scratch.probe_candidate_rows.push(row as u32);
                scratch
                    .representatives
                    .push(entry.representative_row.as_u64());
                handles.set(row, Some(entry.matches()));
            }
        }

        // step3: Compare actual keys column by column, updating equal per candidate slot.
        // For the pairs above: equal starts [true, true], then becomes [false, true].
        //
        // A rejected candidate may still match another entry with the same hash;
        // step4 retries those candidates with full join-key equality.
        scratch
            .equal
            .resize(scratch.probe_candidate_rows.len(), true);
        for column in 0..scratch.build_keys.len() {
            compare_column(scratch, column)?;
        }

        // step4: Keep verified handles; retry rejected candidates to replace or clear theirs.
        // In the example, probing B first found A; retry replaces One(0) with One(1).
        for position in 0..scratch.probe_candidate_rows.len() {
            let row = scratch.probe_candidate_rows[position] as usize;
            if scratch.equal[position] {
                continue;
            }
            if scratch.comparator.is_none() {
                scratch.comparator = Some(JoinKeyComparator::new_with_normalized_left(
                    &scratch.build_keys,
                    &scratch.probe_keys,
                    &vec![SortOptions::default(); scratch.build_keys.len()],
                    // Both sides already passed matchable-key filtering.
                    NullEquality::NullEqualsNull,
                )?);
            }
            let hash = scratch.probe_hashes[row];
            let comparator = scratch.comparator.as_ref().unwrap();
            // A different exact key can share the first candidate's
            // full hash. Retry all entries with that hash on rejection.
            let entry = self.table.find(hash, |entry| {
                entry.hash == hash
                    && comparator.is_equal(entry.representative_row.as_index(), row)
            });
            handles.set(row, entry.map(KeyEntry::matches));
        }

        // step5: Return handles, or convert them once to compact pairs for unique builds.
        let handles = handles.finish();
        if self.build_unique {
            Ok(JoinHashLookup::Unique(
                scratch.take_unique_matches(&handles),
            ))
        } else {
            Ok(JoinHashLookup::Groups(handles))
        }
    }

    pub(super) fn len(&self) -> usize {
        self.table.len()
    }

    pub(super) fn contains_hash(&self, hash: u64) -> bool {
        self.table.find(hash, |entry| entry.hash == hash).is_some()
    }
}

/// Set up [`LookupScratch`] and update the memory reservation for the current probe
/// batch.
///
/// The caller resets scratch first and releases batch state afterward, even on error.
///
/// # Example
///
/// For one string key under `NullEqualsNothing`, [`LookupScratch`] will be initialized
/// like below:
///
/// ```text
/// probe_keys[0]:        [B,    NULL,  A]
/// probe_valid:          [true, false, true]
/// probe_hashes:         [h(B), *,     h(A)] // * is unused: NULL row is skipped
/// probe_candidate_rows: []                 // capacity for at least 3 rows
/// representatives:     []                 // filled by directory lookup next
/// ```
///
/// Eligibility alone does not imply a match; no build keys have been probed yet.
fn prepare_probe_scratch(probe: &Probe<'_>, scratch: &mut LookupScratch) -> Result<()> {
    let len = probe.probe_keys[0].len();
    scratch.null_equality = probe.null_equality;
    let normalize_budget: usize = probe
        .probe_keys
        .iter()
        .filter(|array| has_float_leaf(array.data_type()))
        .map(|array| array.get_array_memory_size())
        .sum();
    let metadata_budget =
        probe.probe_keys.len() * 512 + len.div_ceil(8) * (probe.probe_keys.len() + 1);
    // Reserve an upper bound before normalization allocates; reconcile below.
    scratch
        .batch_reservation
        .try_grow(normalize_budget + metadata_budget)?;
    // These Vec capacities survive release_batch and stay charged to the
    // reusable scratch, separately from temporary arrays and comparators.
    reserve(
        &mut scratch.probe_keys,
        probe.probe_keys.len(),
        &scratch.reservation,
    )?;
    reserve(
        &mut scratch.build_keys,
        probe.build_keys.len(),
        &scratch.reservation,
    )?;
    reserve(
        &mut scratch.column_comparators,
        probe.build_keys.len(),
        &scratch.reservation,
    )?;
    let mut normalized_bytes = 0;
    for key in probe.probe_keys {
        let normalized = normalize(Arc::clone(key));
        if !Arc::ptr_eq(key, &normalized) {
            normalized_bytes += normalized.get_array_memory_size();
        }
        scratch.probe_keys.push(normalized);
    }
    scratch
        .batch_reservation
        .try_resize(normalized_bytes + metadata_budget)?;
    scratch.build_keys.extend_from_slice(probe.build_keys);
    scratch
        .column_comparators
        .resize_with(probe.build_keys.len(), || None);
    scratch.probe_valid = matchable_join_keys(probe.probe_keys, probe.null_equality);
    hash_probe_keys(probe.probe_keys, probe.random_state, scratch)?;
    reserve(&mut scratch.probe_candidate_rows, len, &scratch.reservation)?;
    reserve(&mut scratch.representatives, len, &scratch.reservation)?;
    reserve(&mut scratch.equal, len, &scratch.reservation)?;
    Ok(())
}
