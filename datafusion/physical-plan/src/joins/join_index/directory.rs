// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: See the NOTICE file distributed with this work for additional information regarding copyright ownership
// SPDX-FileContributor: Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements

//! Exact-key directory layouts, construction, and typed integer lookup.
//!
//! Dense integers use an array slot: keys `[10, 11, 12]` live at `key - 10`.
//! Sparse integers such as `[10, 1_000_000]` use SwissTable with the actual key
//! in each entry. General keys use a cached hash and a representative build row
//! whose Arrow values establish equality. All three identify exact keys and
//! return the same singleton/group representation; only row IDs are grouped.

use std::mem::size_of;

use arrow::array::{ArrayRef, BooleanArray};
use arrow::buffer::BooleanBuffer;
use datafusion_common::hash_utils::{RandomState, create_hashes};
use datafusion_common::{NullEquality, Result, internal_err};
use datafusion_execution::memory_pool::MemoryReservation;

use super::keys::{LookupScratch, hash_scratch_bytes};
use super::matches::{
    GroupRows, JoinHashLookup, LookupResult, MatchHandles, MatchedGroups,
    MatchedGroupsBuilder, RowId,
};
use crate::joins::array_map::ArrayMap;
use crate::joins::utils::matchable_join_keys;

mod dense;
mod general;
mod integer;

use dense::DenseIntegerDirectory;
use general::GeneralDirectory;
use integer::{IntegerDirectory, integer_hasher};

/// See [`JoinHashIndex`] for the design overview.
struct Probe<'a> {
    build_keys: &'a [ArrayRef],
    probe_keys: &'a [ArrayRef],
    null_equality: NullEquality,
    random_state: &'a RandomState,
}

/// See [`JoinHashIndex`] for the design overview.
#[derive(Debug)]
pub(super) enum IndexWidth {
    U32(JoinHashIndex<u32>),
    U64(JoinHashIndex<u64>),
}

impl IndexWidth {
    /// See [`JoinHashIndex::build`] and
    /// [`JoinIndexMap::try_new`](super::JoinIndexMap::try_new) for shared argument documentation.
    pub(super) fn build(
        keys: &[ArrayRef],
        random_state: &RandomState,
        null_equality: NullEquality,
        dense_range: Option<(u64, u64)>,
        reservation: &MemoryReservation,
    ) -> Result<Self> {
        let Some(first) = keys.first() else {
            return internal_err!("A join index requires at least one key column");
        };
        let num_rows = first.len();
        let scratch = reservation.new_empty();
        // Keep validity unions and per-column metadata charged for every layout.
        // Dense keys are read directly, so only hash-based directories need hashes.
        scratch.try_grow(num_rows.div_ceil(8) * (keys.len() + 1) + keys.len() * 256)?;
        let hashes = if dense_range.is_none() {
            scratch.try_grow(num_rows * size_of::<u64>() + hash_scratch_bytes(keys))?;
            let mut hashes = vec![0; num_rows];
            create_hashes(keys, random_state, &mut hashes)?;
            hashes
        } else {
            Vec::new()
        };
        let before = reservation.size();
        let result = if u32::try_from(num_rows).is_ok() {
            JoinHashIndex::<u32>::build(
                keys,
                &hashes,
                null_equality,
                dense_range,
                reservation,
                random_state,
            )
            .map(IndexWidth::U32)
        } else {
            JoinHashIndex::<u64>::build(
                keys,
                &hashes,
                null_equality,
                dense_range,
                reservation,
                random_state,
            )
            .map(IndexWidth::U64)
        };
        if result.is_err() {
            reservation.shrink(reservation.size() - before);
        }
        result
    }

    /// See [`KeyDirectory::lookup_batch`].
    pub(super) fn lookup_batch(
        &self,
        build_keys: &[ArrayRef],
        probe_keys: &[ArrayRef],
        null_equality: NullEquality,
        random_state: &RandomState,
        scratch: &mut LookupScratch,
    ) -> Result<LookupResult> {
        if build_keys.is_empty() || build_keys.len() != probe_keys.len() {
            return internal_err!(
                "Join index probe key columns do not match the build keys"
            );
        }
        if probe_keys[0].len() > u32::MAX as usize {
            return internal_err!("Join index probe batch exceeds u32 row indices");
        }
        scratch.reset();
        let probe = Probe {
            build_keys,
            probe_keys,
            null_equality,
            random_state,
        };
        let result =
            match self {
                IndexWidth::U32(index) => index
                    .directory
                    .lookup_batch(&probe, scratch)
                    .map(|result| match result {
                        JoinHashLookup::Unique(matches) => LookupResult::Unique(matches),
                        JoinHashLookup::Groups(handles) => {
                            LookupResult::Groups(MatchHandles::U32(handles))
                        }
                    }),
                IndexWidth::U64(index) => index
                    .directory
                    .lookup_batch(&probe, scratch)
                    .map(|result| match result {
                        JoinHashLookup::Unique(matches) => LookupResult::Unique(matches),
                        JoinHashLookup::Groups(handles) => {
                            LookupResult::Groups(MatchHandles::U64(handles))
                        }
                    }),
            };
        scratch.release_batch();
        result
    }

    #[inline]
    pub(super) fn group_rows(&self, group: u64) -> GroupRows<'_> {
        let group = group as usize;
        match self {
            IndexWidth::U32(index) => GroupRows::U32(index.groups.group_rows(group)),
            IndexWidth::U64(index) => GroupRows::U64(index.groups.group_rows(group)),
        }
    }

    pub(super) fn is_dense(&self) -> bool {
        match self {
            IndexWidth::U32(index) => matches!(index.directory, KeyDirectory::Dense(_)),
            IndexWidth::U64(index) => matches!(index.directory, KeyDirectory::Dense(_)),
        }
    }

    pub(super) fn num_of_distinct_key(&self) -> usize {
        match self {
            IndexWidth::U32(index) => index.len(),
            IndexWidth::U64(index) => index.len(),
        }
    }

    pub(super) fn contain_hashes(
        &self,
        hashes: &[u64],
        random_state: &RandomState,
    ) -> BooleanArray {
        let values = BooleanBuffer::collect_bool(hashes.len(), |row| match self {
            IndexWidth::U32(index) => index.contains_hash(hashes[row], random_state),
            IndexWidth::U64(index) => index.contains_hash(hashes[row], random_state),
        });
        BooleanArray::new(values, None)
    }

    pub(super) fn contain_keys(&self, keys: &[ArrayRef]) -> Result<BooleanArray> {
        if keys.len() != 1 {
            return internal_err!("Dense join index lookup requires one integer key");
        }
        match self {
            IndexWidth::U32(JoinHashIndex {
                directory: KeyDirectory::Dense(dense),
                ..
            }) => dense.contain_keys(&keys[0]),
            IndexWidth::U64(JoinHashIndex {
                directory: KeyDirectory::Dense(dense),
                ..
            }) => dense.contain_keys(&keys[0]),
            _ => internal_err!("Dense join index lookup requires an integer directory"),
        }
    }
}

/// See [`JoinHashIndex`] for the design overview.
#[derive(Debug)]
enum KeyDirectory<T> {
    General(GeneralDirectory<T>),
    Integer(IntegerDirectory<T>),
    Dense(DenseIntegerDirectory<T>),
}

impl<T: RowId> KeyDirectory<T> {
    fn lookup_batch(
        &self,
        probe: &Probe<'_>,
        scratch: &mut LookupScratch,
    ) -> Result<JoinHashLookup<T>> {
        match self {
            Self::General(index) => index.lookup_batch(probe, scratch),
            Self::Integer(index) => index.lookup_batch(probe, scratch),
            Self::Dense(index) => index.lookup_batch(probe, scratch),
        }
    }
}

/// Equi-join index using SwissTable and contiguous storage for matching row IDs.
///
/// # Motivation
///
/// - Make large matches efficient to materialize (e.g. one probe row matches 1M build rows).
/// - Make existence joins easy to short-circuit (e.g. one probe row has 1M matches,
///   but the 1st match can prove existence).
///
/// # Implementation
///
/// 1. **Contiguous storage:** Duplicate keys' row IDs occupy one slice in
///    [`MatchedGroups`], avoiding pointer chasing through duplicate chains.
/// 2. **Hash Directory:** Hash-based variants use [`hashbrown::HashTable`] (SwissTable)
///    to find candidates, then check full key equality. Each distinct key maps
///    to a row or group handle.
///    See [SwissTable internals (video)](https://www.youtube.com/watch?v=ncHmEUmJZf4).
/// 3. **Lazy expansion:** [`MatchHandleArray`](super::matches::MatchHandleArray)
///    holds one row/group handle or miss per probe row. The join processor expands
///    groups into bounded chunks for memory efficiency.
///
/// ## Hash directory specializations
///
/// For a single integer join key:
///
/// - [`DenseIntegerDirectory`]: Map a compact key range directly to array slots,
///   avoiding hashing.
///   e.g. if build keys only have the range `[1,100]`, it builds an array of length 100 to store
///   matches directly.
/// - [`IntegerDirectory`]: Keep keys inside hash entries, so equality needs no
///   build-array lookup.
///
/// Both reuse the same row/group handles and contiguous [`MatchedGroups`].
/// Other key types and multiple key columns use [`GeneralDirectory`].
///
/// ## Example
///
/// ```text
/// Build keys: [A, B, A]
///
/// Hash Directory:
///   A -> Group(0)
///   B -> One(1)
///
/// Groups:     offsets = [0, 2], rows = [0, 2]
///
/// Probe keys: [B, A, X]
/// Handles:    [One(1), Group(0), miss]
/// Expansion: probe 0 -> build [1]
///            probe 1 -> build [0, 2]    // read group 0's slice on demand
/// ```
///
/// All-unique builds return compact row pairs directly.
///
/// # Row ID width
///
/// [`IndexWidth`] selects `T`, the row/group ID type: `u32` for fewer than 2^32
/// build rows to save memory, otherwise `u64`. Directory entries, match handles,
/// and grouped row IDs all use this width.
#[derive(Debug)]
pub(super) struct JoinHashIndex<T> {
    directory: KeyDirectory<T>,
    /// Duplicate-key members addressed by the directory's group handles.
    groups: MatchedGroups<T>,
}

impl<T: RowId> JoinHashIndex<T> {
    /// Build the hash index and choose the specialization.
    ///
    /// See [`JoinIndexMap::try_new`](super::JoinIndexMap::try_new) for shared argument documentation.
    fn build(
        keys: &[ArrayRef],
        hashes: &[u64],
        null_equality: NullEquality,
        dense_range: Option<(u64, u64)>,
        reservation: &MemoryReservation,
        random_state: &RandomState,
    ) -> Result<Self> {
        let mut groups = MatchedGroupsBuilder::new(keys[0].len(), reservation)?;
        let valid = matchable_join_keys(keys, null_equality);
        let directory = if let Some(range) = dense_range {
            let key = match keys {
                [key] if ArrayMap::is_supported_type(key.data_type()) => key,
                _ => return internal_err!("Dense join index requires one integer key"),
            };
            DenseIntegerDirectory::build(
                key,
                valid.as_ref(),
                range,
                reservation,
                &mut groups,
            )
            .map(KeyDirectory::Dense)?
        } else if let Some(hash_key) = (keys.len() == 1)
            .then(|| integer_hasher(keys[0].data_type()))
            .flatten()
        {
            IntegerDirectory::build(
                &keys[0],
                hashes,
                null_equality,
                hash_key,
                random_state,
                reservation,
                &mut groups,
            )
            .map(KeyDirectory::Integer)?
        } else {
            GeneralDirectory::build(
                keys,
                hashes,
                valid.as_ref(),
                reservation,
                &mut groups,
            )
            .map(KeyDirectory::General)?
        };
        Ok(Self {
            directory,
            groups: groups.finish(reservation)?,
        })
    }

    fn len(&self) -> usize {
        match &self.directory {
            KeyDirectory::General(general) => general.len(),
            KeyDirectory::Integer(integer) => integer.len(),
            KeyDirectory::Dense(dense) => dense.len(),
        }
    }

    fn contains_hash(&self, hash: u64, random_state: &RandomState) -> bool {
        match &self.directory {
            KeyDirectory::General(general) => general.contains_hash(hash),
            KeyDirectory::Integer(integer) => integer.contains_hash(hash, random_state),
            // A hash-only filter cannot reject values from an integer directory.
            KeyDirectory::Dense(_) => true,
        }
    }
}
