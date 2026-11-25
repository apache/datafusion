// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: See the NOTICE file distributed with this work for additional information regarding copyright ownership
// SPDX-FileContributor: Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements

//! Single-integer-key directory with exact keys stored in hash entries.

#[cfg(not(feature = "force_hash_collisions"))]
use std::hash::BuildHasher;
use std::mem::size_of;

use arrow::array::*;
use arrow::datatypes::{ArrowPrimitiveType, DataType};
use datafusion_common::hash_utils::RandomState;
use datafusion_common::utils::memory::estimate_memory_size;
use datafusion_common::{NullEquality, Result, internal_err};
use datafusion_execution::memory_pool::MemoryReservation;
use hashbrown::{HashTable, hash_table::Entry};
use num_traits::AsPrimitive;

use super::super::keys::{LookupScratch, hash_probe_keys};
use super::super::matches::{
    JoinHashLookup, MatchHandle, MatchHandleArray, MatchedGroupsBuilder, RowId,
    UniqueMatches,
};
use super::Probe;

/// Stores the exact integer join key in place of the general directory's cached
/// hash, avoiding an indirection into the build arrays on the lookup hot path.
///
/// See [`JoinHashIndex`](super::JoinHashIndex) for the design overview.
///
/// # Example
///
/// Build keys `[10, 20, 10]`, with zero-based row IDs, produce:
///
/// ```text
/// key | representative_row | group_plus_one | matches()
/// ----+--------------------+----------------+------------------------
/// 10  | 0                  | 1              | Group(0) -> rows [0, 2]
/// 20  | 1                  | 0              | One(1)   -> row 1
/// ```
///
/// Lookup hashes the probe key to find candidates, then compares `key` directly.
///
/// - `representative_row`: the first build-row ID for this key.
/// - `group_plus_one`: zero for a single row; otherwise the group ID plus one.
///   The group's row IDs occupy a contiguous slice of
///   [`MatchedGroups::rows`](super::super::matches::MatchedGroups::rows).
#[derive(Clone, Copy, Debug)]
struct IntegerKeyEntry<T> {
    key: u64,
    representative_row: T,
    group_plus_one: T,
}

/// Hash directory for a single integer join key, comparing keys inline in
/// [`IntegerKeyEntry`].
///
/// See [`JoinHashIndex`](super::JoinHashIndex) for the design overview.
#[derive(Debug)]
pub(super) struct IntegerDirectory<T> {
    table: HashTable<IntegerKeyEntry<T>>,
    null: Option<MatchHandle<T>>,
    // Reconstruct the native signedness and width before hashing. This matches
    // create_hashes and keeps hash-only dynamic filters selective.
    hash_key: fn(u64, &RandomState) -> u64,
    /// Every matchable build key has at most one row, including NULL.
    build_unique: bool,
}

pub(super) fn integer_hasher(
    data_type: &DataType,
) -> Option<fn(u64, &RandomState) -> u64> {
    macro_rules! hasher {
        ($type:ty) => {{
            #[cfg(not(feature = "force_hash_collisions"))]
            let hash: fn(u64, &RandomState) -> u64 =
                |key, state| state.hash_one(key as $type);
            #[cfg(feature = "force_hash_collisions")]
            let hash: fn(u64, &RandomState) -> u64 = |_, _| 0;
            Some(hash)
        }};
    }
    match data_type {
        DataType::Int8 => hasher!(i8),
        DataType::Int16 => hasher!(i16),
        DataType::Int32 => hasher!(i32),
        DataType::Int64 => hasher!(i64),
        DataType::UInt8 => hasher!(u8),
        DataType::UInt16 => hasher!(u16),
        DataType::UInt32 => hasher!(u32),
        DataType::UInt64 => hasher!(u64),
        _ => None,
    }
}

impl<T: RowId> IntegerKeyEntry<T> {
    fn matches(&self) -> MatchHandle<T> {
        match self.group_plus_one.as_index() {
            0 => MatchHandle::One(self.representative_row),
            group => MatchHandle::Group(T::from_index(group - 1)),
        }
    }
}

impl<T: RowId> IntegerDirectory<T> {
    /// Store integer keys inline in hash entries for exact comparison;
    /// repeated keys and matchable NULLs accumulate in [`MatchedGroupsBuilder`].
    pub(super) fn build(
        key_array: &ArrayRef,
        hashes: &[u64],
        null_equality: NullEquality,
        hash_key: fn(u64, &RandomState) -> u64,
        random_state: &RandomState,
        reservation: &MemoryReservation,
        groups: &mut MatchedGroupsBuilder<T>,
    ) -> Result<Self> {
        reservation.try_grow(estimate_memory_size::<IntegerKeyEntry<T>>(
            hashes.len(),
            size_of::<HashTable<IntegerKeyEntry<T>>>(),
        )?)?;
        let mut directory = Self {
            table: HashTable::with_capacity(hashes.len()),
            null: None,
            hash_key,
            build_unique: true,
        };
        macro_rules! build_integer {
            ($type:ty) => {{
                let array = key_array.as_primitive::<$type>();
                for (row, &hash) in hashes.iter().enumerate() {
                    if array.is_null(row) {
                        if null_equality == NullEquality::NullEqualsNull {
                            match &mut directory.null {
                                Some(handle) => {
                                    groups.add_duplicate(handle, T::from_index(row))
                                }
                                slot @ None => {
                                    *slot = Some(MatchHandle::One(T::from_index(row)))
                                }
                            }
                        }
                        continue;
                    }
                    let key = array.value(row).as_();
                    match directory.table.entry(
                        hash,
                        |entry| entry.key == key,
                        |entry| hash_key(entry.key, random_state),
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
                            entry.insert(IntegerKeyEntry {
                                key,
                                representative_row: T::from_index(row),
                                group_plus_one: T::default(),
                            });
                        }
                    }
                }
            }};
        }
        match key_array.data_type() {
            DataType::Int8 => build_integer!(arrow::datatypes::Int8Type),
            DataType::Int16 => build_integer!(arrow::datatypes::Int16Type),
            DataType::Int32 => build_integer!(arrow::datatypes::Int32Type),
            DataType::Int64 => build_integer!(arrow::datatypes::Int64Type),
            DataType::UInt8 => build_integer!(arrow::datatypes::UInt8Type),
            DataType::UInt16 => build_integer!(arrow::datatypes::UInt16Type),
            DataType::UInt32 => build_integer!(arrow::datatypes::UInt32Type),
            DataType::UInt64 => build_integer!(arrow::datatypes::UInt64Type),
            _ => unreachable!("integer_hasher checked the key type"),
        }
        directory.build_unique = !groups.has_groups();
        Ok(directory)
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
                hash_probe_keys(probe.probe_keys, probe.random_state, scratch)?;
                self.lookup(array, &scratch.probe_hashes, &scratch.reservation)
            }
            _ => internal_err!("Integer join index requires integer keys"),
        )
    }

    fn lookup<K: ArrowPrimitiveType>(
        &self,
        array: &PrimitiveArray<K>,
        hashes: &[u64],
        reservation: &MemoryReservation,
    ) -> Result<JoinHashLookup<T>>
    where
        K::Native: AsPrimitive<u64>,
    {
        let has_nulls = array.null_count() > 0;
        let find = |row: usize| {
            let key: u64 = array.value(row).as_();
            self.table.find(hashes[row], |entry| entry.key == key)
        };
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
                    find(row).map(|entry| entry.representative_row)
                }
            })
            .map(JoinHashLookup::Unique);
        }
        MatchHandleArray::from_lookup(array.len(), reservation, |row| {
            if has_nulls && array.is_null(row) {
                self.null
            } else {
                find(row).map(IntegerKeyEntry::matches)
            }
        })
        .map(JoinHashLookup::Groups)
    }

    pub(super) fn len(&self) -> usize {
        self.table.len() + usize::from(self.null.is_some())
    }

    pub(super) fn contains_hash(&self, hash: u64, random_state: &RandomState) -> bool {
        (hash == 0 && self.null.is_some())
            || self
                .table
                .find(hash, |entry| {
                    (self.hash_key)(entry.key, random_state) == hash
                })
                .is_some()
    }
}
