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

use super::super::matches::{
    LookupResult, MatchHandle, MatchHandles, MatchedGroups, RowId, bitmap_bytes,
};
use super::Probe;
use crate::joins::array_map::ArrayMap;

// Plain loops let the build body inline and give NULL-free arrays a separate path.
macro_rules! for_each_valid {
    ($len:expr, $valid:expr, |$row:ident| $body:block) => {
        match $valid {
            Some(valid) => {
                for $row in valid.valid_indices() $body
            }
            None => {
                for $row in 0..$len $body
            }
        }
    };
}

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
    /// Distinct matchable build keys.
    distinct: usize,
    /// Every matchable build key has at most one row.
    build_unique: bool,
}

impl<T: RowId> DenseIntegerDirectory<T> {
    /// Count dense integer keys at `key - min`, then scatter row IDs directly
    /// into contiguous groups without buffering duplicate rows.
    ///
    /// # Arguments
    ///
    /// See [`JoinIndexMap::try_new`](super::super::JoinIndexMap::try_new) for
    /// shared argument documentation.
    ///
    /// - `key_array`: the build-side integer join-key column. Any NULLs must
    ///   be excluded by `valid`; matchable NULLs use a hash-based directory.
    ///   Read values directly, casting non-NULL keys to `u64` for slot addressing.
    pub(super) fn build(
        key_array: &ArrayRef,
        valid: Option<&NullBuffer>,
        range: (u64, u64),
        reservation: &MemoryReservation,
    ) -> Result<(Self, MatchedGroups<T>)> {
        // Dispatch once per column; the typed build reads values without a copy.
        downcast_integer_array!(
            key_array => Self::build_typed(key_array, valid, range, reservation),
            _ => internal_err!("Dense join index requires an integer key"),
        )
    }

    /// See [`Self::build`] for shared argument documentation.
    fn build_typed<K: ArrowPrimitiveType>(
        key_array: &PrimitiveArray<K>,
        valid: Option<&NullBuffer>,
        (min, max): (u64, u64),
        reservation: &MemoryReservation,
    ) -> Result<(Self, MatchedGroups<T>)>
    where
        K::Native: AsPrimitive<u64>,
    {
        let range = ArrayMap::calculate_range(min, max);
        if range >= usize::MAX as u64 {
            return internal_err!("Dense join index key range is too large");
        }
        // `array_map_key_range` excludes builds with matchable NULL keys.
        if valid.is_none() && key_array.null_count() > 0 {
            return internal_err!("Dense join index cannot store NULL keys");
        }
        let len = range as usize + 1;
        let bytes = len
            .checked_mul(size_of::<T>())
            .ok_or_else(|| exec_datafusion_err!("Dense join index size overflow"))?;
        reservation.try_grow(bytes)?;
        let values = key_array.values();

        // Counts and write cursors fit T: IndexWidth selects it for the row count.
        let mut slots = vec![T::default(); len];
        for_each_valid!(values.len(), valid, |row| {
            let position = values[row].as_().wrapping_sub(min);
            if position >= len as u64 {
                return internal_err!("Build key is outside the dense join index range");
            }
            let slot = &mut slots[position as usize];
            *slot = T::usize_as(slot.as_usize() + 1);
        });

        let (mut distinct, mut num_groups, mut grouped_rows) = (0, 0, 0);
        for slot in &slots {
            let count = slot.as_usize();
            distinct += usize::from(count != 0);
            if count > 1 {
                num_groups += 1;
                grouped_rows += count;
            }
        }
        let mut is_group = None;
        let mut offsets = Vec::new();
        let mut rows = Vec::new();
        if num_groups > 0 {
            reservation.try_grow(
                bitmap_bytes(len)
                    + (num_groups + 1) * size_of::<usize>()
                    + grouped_rows * size_of::<T>(),
            )?;
            // Reuse each group's slot as its write cursor into the final rows.
            let mut flags = BooleanBufferBuilder::new(len);
            flags.append_n(len, false);
            offsets.reserve_exact(num_groups + 1);
            offsets.push(0);
            for (position, slot) in slots.iter_mut().enumerate() {
                let count = slot.as_usize();
                if count > 1 {
                    let start = offsets[offsets.len() - 1];
                    *slot = T::usize_as(start);
                    offsets.push(start + count);
                    flags.set_bit(position, true);
                }
            }
            let flags = flags.finish();
            rows = vec![T::default(); grouped_rows];

            // The forward scan preserves build order within each group.
            // The counting pass already checked every slot position.
            for_each_valid!(values.len(), valid, |row| {
                let position = values[row].as_().wrapping_sub(min) as usize;
                let slot = &mut slots[position];
                if flags.value(position) {
                    rows[slot.as_usize()] = T::usize_as(row);
                    *slot = T::usize_as(slot.as_usize() + 1);
                } else {
                    *slot = T::usize_as(row + 1);
                }
            });

            // Probing needs group handles, not the completed write cursors.
            for (group, position) in flags.set_indices().enumerate() {
                slots[position] = T::usize_as(group + 1);
            }
            is_group = Some(flags);
        } else {
            for_each_valid!(values.len(), valid, |row| {
                let position = values[row].as_().wrapping_sub(min) as usize;
                slots[position] = T::usize_as(row + 1);
            });
        }

        let dense = Self {
            offset: min,
            slots,
            is_group,
            distinct,
            build_unique: num_groups == 0,
        };
        Ok((dense, MatchedGroups { offsets, rows }))
    }

    pub(super) fn lookup_batch(&self, probe: &Probe<'_>) -> Result<LookupResult> {
        let [array] = probe.probe_keys else {
            return internal_err!("Integer join index requires one integer key");
        };
        downcast_integer_array!(
            array => {
                Ok(self.lookup(array))
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

    fn lookup<K: ArrowPrimitiveType>(&self, array: &PrimitiveArray<K>) -> LookupResult
    where
        K::Native: AsPrimitive<u64>,
    {
        let has_nulls = array.null_count() > 0;
        if self.build_unique {
            return LookupResult::U64(MatchHandles::<u64>::from_singleton_lookup(
                array.len(),
                |row| {
                    if has_nulls && array.is_null(row) {
                        None
                    } else {
                        let key: u64 = array.value(row).as_();
                        let position = key.wrapping_sub(self.offset);
                        if position >= self.slots.len() as u64 {
                            return None;
                        }
                        let slot = self.slots[position as usize].as_index();
                        (slot != 0).then(|| (slot - 1) as u64)
                    }
                },
            ));
        }
        T::into_lookup_result(MatchHandles::<T>::from_lookup(array.len(), |row| {
            if has_nulls && array.is_null(row) {
                None
            } else {
                self.get(array.value(row).as_())
            }
        }))
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
                        false
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
