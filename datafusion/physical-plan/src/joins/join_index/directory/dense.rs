// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: See the NOTICE file distributed with this work for additional information regarding copyright ownership
// SPDX-FileContributor: Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements

//! Dense integer directory addressed directly by key offsets.

use std::mem::size_of;

use arrow::array::*;
use arrow::buffer::{BooleanBuffer, NullBuffer};
use arrow::datatypes::{ArrowPrimitiveType, DataType};
use datafusion_common::{Result, exec_datafusion_err, internal_err};
use datafusion_execution::memory_pool::MemoryReservation;
use num_traits::AsPrimitive;

use super::super::keys::LookupScratch;
use super::super::matches::{
    JoinHashLookup, MatchHandle, MatchHandleArray, MatchedGroupsBuilder, RowId,
    UniqueMatches, bitmap_bytes,
};
use super::Probe;
use crate::joins::array_map::ArrayMap;

/// Direct lookup for a single integer join key in a compact range.
///
/// # Overview
///
/// See [`JoinHashIndex`](super::JoinHashIndex) for the design overview.
///
/// # Dense integer specialization
///
/// Use `key - min` as an array index. [`Self::build`] fills the slots;
/// [`Self::lookup_batch`] probes them directly, without hashing.
///
/// ```text
/// Build keys: [10, 12, 10]     range: 10..=12
///
/// Key   Slot (key - min)   Match
/// 10    0                  Group(0) -> build rows [0, 2]
/// 11    1                  miss
/// 12    2                  One(1)   -> build row 1
/// ```
///
/// Groups still use [`MatchedGroups`](super::super::matches::MatchedGroups)
/// for lazy expansion. Gaps consume slots too, so compact ranges work best.
#[derive(Debug)]
pub(super) struct DenseIntegerDirectory<T> {
    /// Minimum key, represented by slot 0.
    offset: u64,
    /// Zero means absent; otherwise stores a row/group ID plus one.
    slots: Vec<T>,
    /// For occupied slots: `true` = group ID, `false` = row ID.
    /// `None` means no bitmap: all occupied slots hold row IDs.
    /// Bits cannot be NULL; `slots[i] == 0` marks a miss.
    is_group: Option<BooleanBuffer>,
    /// Separate handle for build-side NULLs when NULL equals NULL.
    /// `None` if no build key is NULL or NULLs cannot match.
    null: Option<MatchHandle<T>>,
    /// Distinct matchable build keys, counting NULL once if stored.
    distinct: usize,
    /// Every matchable build key has at most one row, including NULL.
    build_unique: bool,
}

impl<T: RowId> DenseIntegerDirectory<T> {
    /// Map dense integer keys directly to slots at `key - min`, promoting
    /// repeated keys to group handles.
    ///
    /// # Arguments
    ///
    /// See [`JoinIndexMap::try_new`](super::super::JoinIndexMap::try_new) for
    /// shared argument documentation.
    ///
    /// - `key_array`: the build-side integer join-key column, including NULLs.
    ///   Read values directly, casting non-NULL keys to `u64` for slot addressing.
    pub(super) fn build(
        key_array: &ArrayRef,
        valid: Option<&NullBuffer>,
        range: (u64, u64),
        reservation: &MemoryReservation,
        groups: &mut MatchedGroupsBuilder<T>,
    ) -> Result<Self> {
        // Dispatch once per column; the typed build reads values without a copy.
        downcast_integer_array!(
            key_array => {
                Self::build_typed(key_array, valid, range, reservation, groups)
            }
            _ => internal_err!("Dense join index requires an integer key"),
        )
    }

    /// See [`Self::build`] for shared argument documentation.
    fn build_typed<K: ArrowPrimitiveType>(
        key_array: &PrimitiveArray<K>,
        valid: Option<&NullBuffer>,
        (min, max): (u64, u64),
        reservation: &MemoryReservation,
        groups: &mut MatchedGroupsBuilder<T>,
    ) -> Result<Self>
    where
        K::Native: AsPrimitive<u64>,
    {
        // Step 1: Sanity check
        let range = ArrayMap::calculate_range(min, max);
        if range >= usize::MAX as u64 {
            return internal_err!("Dense join index key range is too large");
        }

        // Step 2: update memory reservation
        let len = range as usize + 1;
        let bytes = len
            .checked_mul(size_of::<T>())
            .ok_or_else(|| exec_datafusion_err!("Dense join index size overflow"))?;
        reservation.try_grow(bytes)?;

        // Step 3: initialize flat array directory
        let mut dense = Self {
            offset: min,
            slots: vec![T::default(); len],
            is_group: None,
            null: None,
            distinct: 0,
            build_unique: true,
        };
        let mut group_flags: Option<BooleanBufferBuilder> = None;

        // Step 4: build the index
        for (row, &key) in key_array.values().iter().enumerate() {
            if valid.is_some_and(|valid| valid.is_null(row)) {
                continue;
            }
            if key_array.is_null(row) {
                match &mut dense.null {
                    Some(handle) => groups.add_duplicate(handle, T::from_index(row)),
                    slot @ None => {
                        *slot = Some(MatchHandle::One(T::from_index(row)));
                        dense.distinct += 1;
                    }
                }
                continue;
            }

            let key: u64 = key.as_();
            let position = key.wrapping_sub(min);
            if position >= len as u64 {
                return internal_err!("Build key is outside the dense join index range");
            }
            let position = position as usize;
            let slot = dense.slots[position].as_index();

            if slot == 0 {
                dense.slots[position] = T::from_index(row + 1);
                dense.distinct += 1;
                continue;
            }
            // E.g. key 10 at rows 0 and 2: One(0) becomes Group(0) -> [0, 2].
            let already_group = group_flags
                .as_ref()
                .is_some_and(|flags| flags.get_bit(position));
            let mut handle = if already_group {
                MatchHandle::Group(T::from_index(slot - 1))
            } else {
                MatchHandle::One(T::from_index(slot - 1))
            };
            groups.add_duplicate(&mut handle, T::from_index(row));
            let MatchHandle::Group(group) = handle else {
                unreachable!()
            };
            // Allocate group flags only when a non-NULL key repeats.
            if group_flags.is_none() {
                reservation.try_grow(bitmap_bytes(len))?;
                let mut flags = BooleanBufferBuilder::new(len);
                flags.append_n(len, false);
                group_flags = Some(flags);
            }
            group_flags.as_mut().unwrap().set_bit(position, true);
            dense.slots[position] = T::from_index(group.as_index() + 1);
        }

        // Step 5: Freeze the group bitmap; without grouped slots it stays None.
        dense.is_group = group_flags.map(|mut flags| flags.finish());
        // Include NULL groups, which do not appear in the slot bitmap.
        dense.build_unique = !groups.has_groups();
        Ok(dense)
    }

    pub(super) fn lookup_batch(
        &self,
        probe: &Probe<'_>,
        scratch: &mut LookupScratch,
    ) -> Result<JoinHashLookup<T>> {
        let [array] = probe.probe_keys else {
            return internal_err!("Integer join index requires one integer key");
        };
        downcast_integer_array!(
            array => {
                self.lookup(array, &scratch.reservation)
            }
            _ => internal_err!("Integer join index requires integer keys"),
        )
    }

    #[inline]
    fn get(&self, key: u64) -> Option<MatchHandle<T>> {
        let position = key.wrapping_sub(self.offset);
        if position >= self.slots.len() as u64 {
            return None;
        }
        let position = position as usize;
        let slot = self.slots[position].as_index();
        if slot == 0 {
            None
        } else {
            let id = T::from_index(slot - 1);
            Some(
                if self
                    .is_group
                    .as_ref()
                    .is_some_and(|flags| flags.value(position))
                {
                    MatchHandle::Group(id)
                } else {
                    MatchHandle::One(id)
                },
            )
        }
    }

    fn lookup<K: ArrowPrimitiveType>(
        &self,
        array: &PrimitiveArray<K>,
        reservation: &MemoryReservation,
    ) -> Result<JoinHashLookup<T>>
    where
        K::Native: AsPrimitive<u64>,
    {
        let has_nulls = array.null_count() > 0;
        if self.build_unique {
            let null = match self.null {
                Some(MatchHandle::One(row)) => Some(row),
                None => None,
                Some(MatchHandle::Group(_)) => unreachable!("unique index has no groups"),
            };
            return UniqueMatches::from_lookup(array.len(), reservation, |row| {
                if has_nulls && array.is_null(row) {
                    null
                } else {
                    let key: u64 = array.value(row).as_();
                    let position = key.wrapping_sub(self.offset);
                    if position >= self.slots.len() as u64 {
                        return None;
                    }
                    let slot = self.slots[position as usize].as_index();
                    (slot != 0).then(|| T::from_index(slot - 1))
                }
            })
            .map(JoinHashLookup::Unique);
        }
        MatchHandleArray::from_lookup(array.len(), reservation, |row| {
            if has_nulls && array.is_null(row) {
                self.null
            } else {
                self.get(array.value(row).as_())
            }
        })
        .map(JoinHashLookup::Groups)
    }

    /// Dynamic filters need membership only. Read the typed keys directly, so
    /// filtering allocates only its Boolean output, with no integer-key copy.
    pub(super) fn contain_keys(&self, array: &ArrayRef) -> Result<BooleanArray> {
        macro_rules! contains {
            ($type:ty) => {{
                let array = array.as_primitive::<$type>();
                let has_nulls = array.null_count() > 0;
                let values = BooleanBuffer::collect_bool(array.len(), |row| {
                    if has_nulls && array.is_null(row) {
                        self.null.is_some()
                    } else {
                        self.get(array.value(row).as_()).is_some()
                    }
                });
                Ok(BooleanArray::new(values, None))
            }};
        }
        match array.data_type() {
            DataType::Int8 => contains!(arrow::datatypes::Int8Type),
            DataType::Int16 => contains!(arrow::datatypes::Int16Type),
            DataType::Int32 => contains!(arrow::datatypes::Int32Type),
            DataType::Int64 => contains!(arrow::datatypes::Int64Type),
            DataType::UInt8 => contains!(arrow::datatypes::UInt8Type),
            DataType::UInt16 => contains!(arrow::datatypes::UInt16Type),
            DataType::UInt32 => contains!(arrow::datatypes::UInt32Type),
            DataType::UInt64 => contains!(arrow::datatypes::UInt64Type),
            _ => internal_err!("Dense join index requires integer keys"),
        }
    }

    pub(super) fn len(&self) -> usize {
        self.distinct
    }
}
