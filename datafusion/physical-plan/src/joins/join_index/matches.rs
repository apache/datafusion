// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: See the NOTICE file distributed with this work for additional information regarding copyright ownership
// SPDX-FileContributor: Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements

//! Groups of build rows with equal keys, plus lookup results and their memory
//! reservations.
//!
//! Build keys `[A, B, A]` give `A -> Group(0)` and `B -> One(1)`. Only rows with
//! duplicate keys occupy [`MatchedGroups`]: `offsets = [0, 2]`, `rows = [0, 2]`.
//! Payload columns stay untouched. Batch handles keep IDs separate from nullable
//! flags: true means group, false means singleton, and NULL means lookup miss.
//!
//! If the entire build is unique, results are already compact row pairs and
//! need neither flags nor group expansion. Both forms own their memory charge.

use std::fmt::Debug;
use std::mem::size_of;

use arrow::array::{Array, BooleanArray, BooleanBufferBuilder};
use arrow::buffer::{BooleanBuffer, NullBuffer, ScalarBuffer};
use arrow::datatypes::ArrowNativeType;
use datafusion_common::{Result, exec_datafusion_err};
use datafusion_execution::memory_pool::MemoryReservation;

use super::reserve;

pub(crate) trait RowId: ArrowNativeType + Copy + Debug + Default {
    fn from_index(value: usize) -> Self;
    fn as_index(self) -> usize;
    fn as_u64(self) -> u64;
}

macro_rules! row_id {
    ($type:ty) => {
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
        }
    };
}
row_id!(u32);
row_id!(u64);

/// A row ID or a group ID, both using the partition's index width.
#[derive(Clone, Copy, Debug)]
pub(crate) enum MatchHandle<T> {
    One(T),
    Group(T),
}

/// Build-row IDs grouped by exact join key. Only keys with multiple build rows
/// have groups; singleton rows are represented directly by directory handles.
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

/// Temporary buffers for constructing [`MatchedGroups`]. Unique keys allocate
/// no member storage. The second occurrence creates a group and records its
/// first row; later occurrences record just (group, row), in input order.
pub(super) struct GroupBuilder<T> {
    first_rows: Vec<T>,
    counts: Vec<usize>,
    pending: Vec<(T, T)>,
    reservation: MemoryReservation,
}

impl<T: RowId> GroupBuilder<T> {
    pub(super) fn new(reservation: &MemoryReservation) -> Self {
        Self {
            first_rows: vec![],
            counts: vec![],
            pending: vec![],
            reservation: reservation.new_empty(),
        }
    }

    pub(super) fn add(&mut self, handle: &mut MatchHandle<T>, row: T) -> Result<()> {
        let group = match *handle {
            MatchHandle::One(first) => {
                let group = T::from_index(self.counts.len());
                reserve(
                    &mut self.first_rows,
                    self.counts.len() + 1,
                    &self.reservation,
                )?;
                reserve(
                    &mut self.counts,
                    self.first_rows.len() + 1,
                    &self.reservation,
                )?;
                self.first_rows.push(first);
                self.counts.push(1);
                *handle = MatchHandle::Group(group);
                group
            }
            MatchHandle::Group(group) => group,
        };
        self.counts[group.as_index()] += 1;
        let needed = self.pending.len() + 1;
        reserve(&mut self.pending, needed, &self.reservation)?;
        self.pending.push((group, row));
        Ok(())
    }

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

pub(super) enum PartitionLookup<T: RowId> {
    Unique(UniqueMatches),
    Groups(MatchHandleArray<T>),
}

/// Unique build keys can return verified pairs directly. Misses occupy no
/// slots; `probe_len` still records the full input for outer-join completion.
#[derive(Debug)]
pub(crate) struct UniqueMatches {
    pub(crate) build_rows: ScalarBuffer<u64>,
    pub(crate) probe_rows: ScalarBuffer<u32>,
    pub(crate) probe_len: usize,
    pub(super) _reservation: MemoryReservation,
}

impl UniqueMatches {
    // Keep typed lookup constants visible across the directory/matches boundary.
    #[inline]
    pub(super) fn from_lookup<T: RowId>(
        len: usize,
        reservation: &MemoryReservation,
        mut lookup: impl FnMut(usize) -> Option<T>,
    ) -> Result<Self> {
        let reservation = reservation.new_empty();
        reservation.try_grow(len.checked_mul(12).ok_or_else(|| {
            exec_datafusion_err!("Unique join pair array size overflow")
        })?)?;
        let mut build_rows = Vec::with_capacity(len);
        let mut probe_rows = Vec::with_capacity(len);
        for row in 0..len {
            if let Some(build) = lookup(row) {
                build_rows.push(build.as_u64());
                probe_rows.push(row as u32);
            }
        }
        Ok(Self {
            build_rows: build_rows.into(),
            probe_rows: probe_rows.into(),
            probe_len: len,
            _reservation: reservation,
        })
    }
}

#[derive(Debug)]
pub(crate) enum LookupResult {
    Unique(UniqueMatches),
    Groups(MatchHandles),
}

impl LookupResult {
    #[cfg(test)]
    pub(crate) fn widen(self) -> Result<Self> {
        match self {
            Self::Unique(_) => Ok(self),
            Self::Groups(handles) => handles.widen().map(Self::Groups),
        }
    }
}

/// One physical match handle per probe row. NULL in `is_group` means a miss;
/// otherwise the corresponding non-null ID is a build row or a group ID.
#[derive(Debug)]
pub(crate) struct MatchHandleArray<T: RowId> {
    ids: ScalarBuffer<T>,
    is_group: BooleanArray,
    has_groups: bool,
    // Keep the charge with the returned buffers, after the allocations it owns.
    pub(super) _reservation: MemoryReservation,
}

impl<T: RowId> MatchHandleArray<T> {
    /// Sequential exact lookups pack each bitmap word in a register. A row
    /// writes its ID once, and 64 rows share one write to each bitmap.
    #[inline]
    pub(super) fn from_lookup(
        len: usize,
        reservation: &MemoryReservation,
        mut lookup: impl FnMut(usize) -> Option<MatchHandle<T>>,
    ) -> Result<Self> {
        let reservation = reservation.new_empty();
        reservation.try_grow(handle_bytes::<T>(len)?)?;
        let mut ids = Vec::with_capacity(len);
        let mut group_words = Vec::with_capacity(len.div_ceil(64));
        let mut valid_words = Vec::with_capacity(len.div_ceil(64));
        for start in (0..len).step_by(64) {
            let mut groups = 0_u64;
            let mut valid = 0_u64;
            for bit in 0..(len - start).min(64) {
                match lookup(start + bit) {
                    None => ids.push(T::default()),
                    Some(MatchHandle::One(id)) => {
                        ids.push(id);
                        valid |= 1 << bit;
                    }
                    Some(MatchHandle::Group(id)) => {
                        ids.push(id);
                        valid |= 1 << bit;
                        groups |= 1 << bit;
                    }
                }
            }
            group_words.push(groups.to_le());
            valid_words.push(valid.to_le());
        }
        Ok(Self::from_buffers(
            ids,
            BooleanBuffer::new(group_words.into(), 0, len),
            BooleanBuffer::new(valid_words.into(), 0, len),
            reservation,
        ))
    }

    fn from_buffers(
        ids: Vec<T>,
        is_group: BooleanBuffer,
        valid: BooleanBuffer,
        reservation: MemoryReservation,
    ) -> Self {
        let has_groups = is_group.has_true();
        let nulls = NullBuffer::new(valid);
        let nulls = if nulls.null_count() == 0 {
            drop(nulls);
            reservation.shrink(bitmap_bytes(ids.len()));
            None
        } else {
            Some(nulls)
        };
        Self {
            ids: ids.into(),
            is_group: BooleanArray::new(is_group, nulls),
            has_groups,
            _reservation: reservation,
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
    pub(crate) fn validity(&self) -> Option<&NullBuffer> {
        self.is_group.nulls()
    }

    #[inline]
    pub(crate) fn has_groups(&self) -> bool {
        self.has_groups
    }

    #[inline]
    pub(crate) fn get(&self, row: usize) -> Option<MatchHandle<T>> {
        if self.is_group.is_null(row) {
            None
        } else if self.is_group.value(row) {
            Some(MatchHandle::Group(self.ids[row]))
        } else {
            Some(MatchHandle::One(self.ids[row]))
        }
    }
}

#[derive(Debug)]
pub(crate) enum MatchHandles {
    U32(MatchHandleArray<u32>),
    U64(MatchHandleArray<u64>),
}

impl MatchHandles {
    pub(crate) fn len(&self) -> usize {
        match self {
            Self::U32(handles) => handles.len(),
            Self::U64(handles) => handles.len(),
        }
    }

    #[cfg(test)]
    pub(crate) fn widen(self) -> Result<Self> {
        match self {
            Self::U64(_) => Ok(self),
            Self::U32(MatchHandleArray {
                ids,
                is_group,
                has_groups,
                _reservation: reservation,
            }) => {
                let wide_reservation = reservation.new_empty();
                wide_reservation.try_grow(handle_bytes::<u64>(ids.len())?)?;
                let wide_ids: Vec<u64> = ids.iter().map(|&id| u64::from(id)).collect();
                drop(ids);
                drop(reservation);
                Ok(Self::U64(MatchHandleArray {
                    ids: wide_ids.into(),
                    is_group,
                    has_groups,
                    _reservation: wide_reservation,
                }))
            }
        }
    }
}

/// Borrowed row IDs from one group in [`MatchedGroups`], in original build order.
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

fn handle_bytes<T: RowId>(len: usize) -> Result<usize> {
    len.checked_mul(size_of::<T>())
        .and_then(|bytes| bytes.checked_add(bitmap_bytes(len).saturating_mul(2)))
        .ok_or_else(|| exec_datafusion_err!("Join match handle array size overflow"))
}

pub(super) struct HandleBuilder<T: RowId> {
    ids: Vec<T>,
    is_group: BooleanBufferBuilder,
    valid: BooleanBufferBuilder,
    reservation: MemoryReservation,
}

impl<T: RowId> HandleBuilder<T> {
    pub(super) fn new(len: usize, reservation: &MemoryReservation) -> Result<Self> {
        let reservation = reservation.new_empty();
        reservation.try_grow(handle_bytes::<T>(len)?)?;
        let mut is_group = BooleanBufferBuilder::new(len);
        is_group.append_n(len, false);
        let mut valid = BooleanBufferBuilder::new(len);
        valid.append_n(len, false);
        Ok(Self {
            ids: vec![T::default(); len],
            is_group,
            valid,
            reservation,
        })
    }

    pub(super) fn set(&mut self, row: usize, handle: Option<MatchHandle<T>>) {
        match handle {
            None => self.valid.set_bit(row, false),
            Some(MatchHandle::One(id)) => {
                self.ids[row] = id;
                self.is_group.set_bit(row, false);
                self.valid.set_bit(row, true);
            }
            Some(MatchHandle::Group(id)) => {
                self.ids[row] = id;
                self.is_group.set_bit(row, true);
                self.valid.set_bit(row, true);
            }
        }
    }

    pub(super) fn finish(mut self) -> MatchHandleArray<T> {
        MatchHandleArray::from_buffers(
            self.ids,
            self.is_group.finish(),
            self.valid.finish(),
            self.reservation,
        )
    }
}
