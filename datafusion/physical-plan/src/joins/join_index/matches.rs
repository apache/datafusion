// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: See the NOTICE file distributed with this work for additional information regarding copyright ownership
// SPDX-FileContributor: Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements

//! Groups of build rows with equal keys, plus compact lookup results.
//!
//! Build keys `[A, B, A]` give `A -> Group(0)` and `B -> One(1)`. Only rows with
//! duplicate keys occupy [`MatchedGroups`]: `offsets = [0, 2]`, `rows = [0, 2]`.
//! Payload columns stay untouched. [`MatchHandles`] records only successful
//! lookups, pairing each original probe position with a build row or group ID.
//! Missing keys occupy no entries. All-singleton results need no group bitmap.

use std::fmt::Debug;
use std::mem::size_of;

use arrow::array::BooleanBufferBuilder;
use arrow::buffer::{BooleanBuffer, ScalarBuffer};
use arrow::datatypes::ArrowNativeType;
use datafusion_common::{Result, exec_datafusion_err};
use datafusion_execution::memory_pool::MemoryReservation;

/// See [`JoinHashIndex`](super::directory::JoinHashIndex) for the design overview.
pub(crate) trait RowId: ArrowNativeType + Copy + Debug + Default {
    fn from_index(value: usize) -> Self;
    fn as_index(self) -> usize;
    fn as_u64(self) -> u64;
    fn into_lookup_result(matches: MatchHandles<Self>) -> LookupResult;
}

macro_rules! row_id {
    ($type:ty, $variant:ident) => {
        impl RowId for $type {
            #[inline]
            fn from_index(value: usize) -> Self {
                Self::try_from(value).expect("row ID fits the selected index width")
            }
            #[inline]
            fn as_index(self) -> usize {
                self as usize
            }
            #[inline]
            fn as_u64(self) -> u64 {
                self as u64
            }
            fn into_lookup_result(matches: MatchHandles<Self>) -> LookupResult {
                LookupResult::$variant(matches)
            }
        }
    };
}
row_id!(u32, U32);
row_id!(u64, U64);

/// A build-row ID or a group ID.
///
/// See [`JoinHashIndex`](super::directory::JoinHashIndex) for the design overview.
#[derive(Clone, Copy, Debug)]
pub(crate) enum MatchHandle<T> {
    One(T),
    Group(T),
}

/// Build-row IDs grouped by exact join key. Only keys with multiple build rows
/// have groups; singleton rows are represented directly by directory handles.
///
/// See [`JoinHashIndex`](super::directory::JoinHashIndex) for the design overview.
#[derive(Debug)]
pub(super) struct MatchedGroups<T> {
    pub(super) offsets: Vec<usize>,
    pub(super) rows: Vec<T>,
}

impl<T> MatchedGroups<T> {
    #[inline]
    pub(super) fn group_rows(&self, group: usize) -> &[T] {
        &self.rows[self.offsets[group]..self.offsets[group + 1]]
    }
}

/// Builds [`MatchedGroups`].
///
/// See [`JoinHashIndex`](super::directory::JoinHashIndex) for the design overview.
///
/// # Building process
///
/// Each group needs a contiguous slice, but its size is unknown until all build
/// rows have been seen. The builder therefore uses two passes:
///
/// 1. While scanning build rows, [`Self::add_duplicate`] counts each group's
///    rows and buffers their IDs. A group is created only when its key repeats.
/// 2. [`Self::finish`] uses the counts to lay out one slice per group and writes
///    each group's first row. It then walks the buffered IDs to fill the rest
///    of each slice in input order.
///
/// This algorithm is amenable to vectorization.
///
/// ## Example
///
/// For build keys `[A, B, A, B, A, C]`:
///
/// Pass 1: Assign groups 0 and 1 to `A` and `B`, count their rows, and
/// buffer later `(group, row)` pairs:
///
/// ```text
/// first_rows: [0, 1]
/// counts: [3, 2]
/// pending: [(0, 2), (1, 3), (0, 4)]
/// ```
///
/// `C` stays `One(5)` in the directory.
///
/// Pass 2: [`Self::finish`] computes offsets and packs each group's rows:
///
/// ```text
/// offsets: [0, 3, 5]
/// rows: [0, 2, 4, 1, 3]
/// ```
pub(super) struct MatchedGroupsBuilder<T> {
    /// First build-row ID per group, indexed by group ID.
    first_rows: Vec<T>,
    /// Total rows per group; reused as write cursors in `finish`.
    counts: Vec<usize>,
    /// Subsequent `(group_id, build_row_id)` pairs in input order.
    pending: Vec<(T, T)>,
    /// Memory charge for the temporary buffers.
    _reservation: MemoryReservation,
}

impl<T: RowId> MatchedGroupsBuilder<T> {
    pub(super) fn new(num_rows: usize, reservation: &MemoryReservation) -> Result<Self> {
        // Estimate the memory and update the reservation. The conservative estimation
        // is on purpose for simplicity.
        // Each group needs at least two rows; each key's first row is not pending.
        // E.g. [A, A, A] has one group and two pending rows.
        let max_groups = num_rows / 2;
        let max_pending = num_rows.saturating_sub(1);
        let bytes = max_groups
            .checked_mul(size_of::<T>() + size_of::<usize>())
            .and_then(|bytes| {
                let pending_bytes = max_pending.checked_mul(size_of::<(T, T)>())?;
                bytes.checked_add(pending_bytes)
            })
            .ok_or_else(|| {
                exec_datafusion_err!("Join group construction size overflow")
            })?;
        let reservation = reservation.new_empty();
        reservation.try_grow(bytes)?;
        Ok(Self {
            first_rows: Vec::with_capacity(max_groups),
            counts: Vec::with_capacity(max_groups),
            pending: Vec::with_capacity(max_pending),
            _reservation: reservation,
        })
    }

    /// Whether any key has been promoted from a single row to a group.
    pub(super) fn has_groups(&self) -> bool {
        !self.counts.is_empty()
    }

    /// Add a duplicate row; each build row must be added at most once.
    pub(super) fn add_duplicate(&mut self, handle: &mut MatchHandle<T>, row: T) {
        let group = match *handle {
            MatchHandle::One(first) => {
                let group = T::from_index(self.counts.len());
                self.first_rows.push(first);
                self.counts.push(1);
                *handle = MatchHandle::Group(group);
                group
            }
            MatchHandle::Group(group) => group,
        };
        self.counts[group.as_index()] += 1;
        self.pending.push((group, row));
    }

    /// Pack the collected rows into contiguous [`MatchedGroups`].
    pub(super) fn finish(
        mut self,
        reservation: &MemoryReservation,
    ) -> Result<MatchedGroups<T>> {
        if self.counts.is_empty() {
            return Ok(MatchedGroups {
                offsets: vec![],
                rows: vec![],
            });
        }
        let num_rows = self.pending.len() + self.counts.len();
        reservation.try_grow(
            (self.counts.len() + 1) * size_of::<usize>() + num_rows * size_of::<T>(),
        )?;
        let mut offsets = Vec::with_capacity(self.counts.len() + 1);
        let mut rows = vec![T::default(); num_rows];
        offsets.push(0);
        for (first, count) in self.first_rows.iter().zip(&mut self.counts) {
            let start = *offsets.last().unwrap();
            offsets.push(start + *count);
            rows[start] = *first;
            // Counts become the next write position; no extra scatter cursor.
            *count = start + 1;
        }
        for (group, row) in self.pending {
            let position = &mut self.counts[group.as_index()];
            rows[*position] = row;
            *position += 1;
        }
        Ok(MatchedGroups { offsets, rows })
    }
}

/// Successful key lookups, in original probe order.
///
/// Each entry identifies one build row or a group of equal-key build rows.
/// Missing keys occupy no entries; a repeated probe key has its own entry.
///
/// - `probe_indices`: original positions in the probe batch, strictly increasing.
/// - `ids`: build-row or group IDs, aligned with `probe_indices`.
/// - `is_group`: true for group IDs; absent when every entry is a single row.
///   This depends on the returned matches, even if the build has duplicate keys.
///
/// For build keys `[A, B, A]` and probe keys `[X, B, A, X, A]`:
///
/// ```text
/// probe_indices: [1, 2, 4]
/// ids: [1, 0, 0]
/// is_group: [false, true, true]
/// group 0: [0, 2]
/// ```
///
/// Key equality is already verified. Residual ON predicates are evaluated after
/// expansion and may reject some or all members of a group. The original probe
/// batch remains with the join processor so outer joins can emit missing rows.
/// These buffers are bounded by one probe batch and are not charged to the
/// memory pool; see [`MemoryPool`](datafusion_execution::memory_pool::MemoryPool).
#[derive(Debug)]
pub(crate) struct MatchHandles<T: RowId> {
    ids: ScalarBuffer<T>,
    probe_indices: ScalarBuffer<u32>,
    is_group: Option<BooleanBuffer>,
}

impl<T: RowId> MatchHandles<T> {
    /// Resolve each input row, retaining only successful lookups.
    #[inline]
    pub(super) fn from_lookup(
        len: usize,
        mut lookup: impl FnMut(usize) -> Option<MatchHandle<T>>,
    ) -> Self {
        let mut handles = HandleBuilder::new(len);
        for row in 0..len {
            if let Some(handle) = lookup(row) {
                handles.push(row as u32, handle);
            }
        }
        handles.finish()
    }

    /// Resolve singleton keys without group-bitmap bookkeeping.
    #[inline]
    pub(super) fn from_singleton_lookup(
        len: usize,
        mut lookup: impl FnMut(usize) -> Option<T>,
    ) -> Self {
        let mut ids = Vec::with_capacity(len);
        let mut probe_indices = Vec::with_capacity(len);
        for row in 0..len {
            if let Some(id) = lookup(row) {
                ids.push(id);
                probe_indices.push(row as u32);
            }
        }
        Self::from_buffers(ids, probe_indices, None)
    }

    #[inline]
    pub(super) fn from_buffers(
        ids: Vec<T>,
        probe_indices: Vec<u32>,
        is_group: Option<BooleanBuffer>,
    ) -> Self {
        debug_assert_eq!(ids.len(), probe_indices.len());
        debug_assert!(
            is_group
                .as_ref()
                .is_none_or(|flags| flags.len() == ids.len())
        );
        Self {
            ids: ids.into(),
            probe_indices: probe_indices.into(),
            is_group,
        }
    }

    #[inline]
    pub(crate) fn len(&self) -> usize {
        self.ids.len()
    }

    #[inline]
    pub(crate) fn ids(&self) -> &[T] {
        &self.ids
    }

    #[inline]
    pub(crate) fn ids_buffer(&self) -> &ScalarBuffer<T> {
        &self.ids
    }

    #[inline]
    pub(crate) fn probe_indices(&self) -> &ScalarBuffer<u32> {
        &self.probe_indices
    }

    #[inline]
    pub(crate) fn has_groups(&self) -> bool {
        self.is_group.is_some()
    }

    #[inline]
    pub(crate) fn get(&self, position: usize) -> MatchHandle<T> {
        if self
            .is_group
            .as_ref()
            .is_some_and(|flags| flags.value(position))
        {
            MatchHandle::Group(self.ids[position])
        } else {
            MatchHandle::One(self.ids[position])
        }
    }
}

/// Dispatch the lookup ID width once per output chunk.
#[derive(Debug)]
pub(crate) enum LookupResult {
    U32(MatchHandles<u32>),
    U64(MatchHandles<u64>),
}

impl LookupResult {
    pub(crate) fn len(&self) -> usize {
        match self {
            Self::U32(handles) => handles.len(),
            Self::U64(handles) => handles.len(),
        }
    }
}

/// Borrowed row IDs from one group in [`MatchedGroups`], in original build order.
///
/// See [`JoinHashIndex`](super::directory::JoinHashIndex) for the design overview.
pub(crate) enum GroupRows<'a> {
    U32(&'a [u32]),
    U64(&'a [u64]),
}

impl GroupRows<'_> {
    pub(crate) fn len(&self) -> usize {
        match self {
            Self::U32(rows) => rows.len(),
            Self::U64(rows) => rows.len(),
        }
    }
}

pub(super) fn bitmap_bytes(len: usize) -> usize {
    // Arrow rounds a bitmap allocation to 64 bytes.
    len.div_ceil(512).saturating_mul(64)
}

/// Collect compact handles; general-key lookup may replace or reject candidates
/// after checking actual keys. See [`MatchHandles`] for the returned layout.
pub(super) struct HandleBuilder<T: RowId> {
    ids: Vec<T>,
    probe_indices: Vec<u32>,
    is_group: Option<BooleanBufferBuilder>,
    capacity: usize,
}

impl<T: RowId> HandleBuilder<T> {
    #[inline]
    pub(super) fn new(capacity: usize) -> Self {
        Self {
            ids: Vec::with_capacity(capacity),
            probe_indices: Vec::with_capacity(capacity),
            is_group: None,
            capacity,
        }
    }

    #[inline]
    pub(super) fn push(&mut self, probe_index: u32, handle: MatchHandle<T>) {
        let (id, is_group) = match handle {
            MatchHandle::One(id) => (id, false),
            MatchHandle::Group(id) => (id, true),
        };
        if is_group && self.is_group.is_none() {
            let mut flags = BooleanBufferBuilder::new(self.capacity);
            flags.append_n(self.ids.len(), false);
            self.is_group = Some(flags);
        }
        self.ids.push(id);
        self.probe_indices.push(probe_index);
        if let Some(flags) = &mut self.is_group {
            flags.append(is_group);
        }
    }

    #[inline]
    pub(super) fn set(&mut self, position: usize, handle: MatchHandle<T>) {
        let (id, is_group) = match handle {
            MatchHandle::One(id) => (id, false),
            MatchHandle::Group(id) => (id, true),
        };
        self.ids[position] = id;
        if is_group && self.is_group.is_none() {
            let mut flags = BooleanBufferBuilder::new(self.capacity);
            flags.append_n(self.ids.len(), false);
            self.is_group = Some(flags);
        }
        if let Some(flags) = &mut self.is_group {
            flags.set_bit(position, is_group);
        }
    }

    /// Remove rejected hash candidates without changing successful row order.
    pub(super) fn retain(&mut self, keep: &[bool]) {
        debug_assert_eq!(self.ids.len(), keep.len());
        let mut matched = 0;
        for (position, &keep) in keep.iter().enumerate() {
            if keep {
                self.ids[matched] = self.ids[position];
                self.probe_indices[matched] = self.probe_indices[position];
                if let Some(flags) = &mut self.is_group {
                    flags.set_bit(matched, flags.get_bit(position));
                }
                matched += 1;
            }
        }
        self.ids.truncate(matched);
        self.probe_indices.truncate(matched);
        if let Some(flags) = &mut self.is_group {
            flags.truncate(matched);
        }
    }

    #[inline]
    pub(super) fn finish(self) -> MatchHandles<T> {
        let is_group = self.is_group.and_then(|mut flags| {
            let flags = flags.finish();
            flags.has_true().then_some(flags)
        });
        MatchHandles::from_buffers(self.ids, self.probe_indices, is_group)
    }
}
