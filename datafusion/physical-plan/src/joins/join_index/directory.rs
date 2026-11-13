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

#[cfg(not(feature = "force_hash_collisions"))]
use std::hash::BuildHasher;
use std::mem::size_of;

use arrow::array::*;
use arrow::buffer::{BooleanBuffer, NullBuffer};
use arrow::compute::SortOptions;
use arrow::datatypes::{ArrowPrimitiveType, DataType};
use datafusion_common::hash_utils::{RandomState, create_hashes};
use datafusion_common::utils::memory::estimate_memory_size;
use datafusion_common::{NullEquality, Result, exec_datafusion_err, internal_err};
use datafusion_execution::memory_pool::MemoryReservation;
use hashbrown::{HashTable, hash_table::Entry};
use num_traits::AsPrimitive;

use super::keys::{LookupMode, LookupScratch, hash_scratch_bytes, integer_values};
use super::matches::{
    GroupBuilder, GroupRows, LookupResult, MatchHandle, MatchHandleArray, MatchHandles,
    MatchedGroups, PartitionLookup, RowId, UniqueMatches, bitmap_bytes,
};
use crate::joins::array_map::ArrayMap;
use crate::joins::utils::{JoinKeyComparator, matchable_join_keys};

/// Use `u32` row and group IDs to save memory for builds with fewer than 2^32
/// rows , and `u64` for larger builds.
#[derive(Debug)]
pub(super) enum IndexWidth {
    U32(PartitionIndex<u32>),
    U64(PartitionIndex<u64>),
}

impl IndexWidth {
    /// See [`PartitionIndex::build`]
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
        // Hashes (or the dense directory's integer keys), validity unions, and
        // per-column comparator metadata are all construction scratch.
        scratch.try_grow(
            num_rows * size_of::<u64>()
                + num_rows.div_ceil(8) * (keys.len() + 1)
                + keys.len() * 256
                + hash_scratch_bytes(keys),
        )?;
        let mut hashes = vec![0; num_rows];
        if dense_range.is_some() {
            integer_values(first, &mut hashes)?;
        } else {
            create_hashes(keys, random_state, &mut hashes)?;
        }
        let before = reservation.size();
        let result = if u32::try_from(num_rows).is_ok() {
            PartitionIndex::<u32>::build(
                keys,
                &hashes,
                null_equality,
                dense_range,
                reservation,
                random_state,
            )
            .map(IndexWidth::U32)
        } else {
            PartitionIndex::<u64>::build(
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

    /// See [`PartitionIndex::lookup`]
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
        scratch.prepare(
            build_keys,
            probe_keys,
            null_equality,
            random_state,
            self.mode(),
        )?;
        let result = match self {
            IndexWidth::U32(index) => index.lookup(scratch).map(|result| match result {
                PartitionLookup::Unique(matches) => LookupResult::Unique(matches),
                PartitionLookup::Groups(handles) => {
                    LookupResult::Groups(MatchHandles::U32(handles))
                }
            }),
            IndexWidth::U64(index) => index.lookup(scratch).map(|result| match result {
                PartitionLookup::Unique(matches) => LookupResult::Unique(matches),
                PartitionLookup::Groups(handles) => {
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
            IndexWidth::U32(PartitionIndex {
                directory: KeyDirectory::Dense(dense),
                ..
            }) => dense.contain_keys(&keys[0]),
            IndexWidth::U64(PartitionIndex {
                directory: KeyDirectory::Dense(dense),
                ..
            }) => dense.contain_keys(&keys[0]),
            _ => internal_err!("Dense join index lookup requires an integer directory"),
        }
    }

    fn mode(&self) -> LookupMode {
        match self {
            Self::U32(index) => index.mode(),
            Self::U64(index) => index.mode(),
        }
    }
}

#[derive(Clone, Copy, Debug)]
pub(super) struct KeyEntry<T> {
    pub(super) hash: u64,
    pub(super) representative_row: T,
    // Zero means One(representative_row); otherwise this is Group(id - 1).
    group_plus_one: T,
}

/// Single integer keys replace the cached hash with the actual key, so equality
/// never gathers a representative value from the build arrays.
#[derive(Clone, Copy, Debug)]
struct IntegerKeyEntry<T> {
    key: u64,
    representative_row: T,
    group_plus_one: T,
}

#[derive(Debug)]
struct IntegerDirectory<T> {
    table: HashTable<IntegerKeyEntry<T>>,
    null: Option<MatchHandle<T>>,
    // Reconstruct the native signedness and width before hashing. This matches
    // create_hashes and keeps hash-only dynamic filters selective.
    hash_key: fn(u64, &RandomState) -> u64,
}

fn integer_hasher(data_type: &DataType) -> Option<fn(u64, &RandomState) -> u64> {
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

#[derive(Debug)]
struct DenseIntegerDirectory<T> {
    offset: u64,
    // Zero is absent. The group bitmap chooses whether slot - 1 is a row or group.
    slots: Vec<T>,
    is_group: Option<BooleanBuffer>,
    null: Option<MatchHandle<T>>,
    distinct: usize,
}

impl<T: RowId> DenseIntegerDirectory<T> {
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
        unique: bool,
    ) -> Result<PartitionLookup<T>>
    where
        K::Native: AsPrimitive<u64>,
    {
        let has_nulls = array.null_count() > 0;
        if unique {
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
            .map(PartitionLookup::Unique);
        }
        MatchHandleArray::from_lookup(array.len(), reservation, |row| {
            if has_nulls && array.is_null(row) {
                self.null
            } else {
                self.get(array.value(row).as_())
            }
        })
        .map(PartitionLookup::Groups)
    }

    /// Dynamic filters need membership only. Read the typed keys directly, so
    /// filtering allocates only its Boolean output, with no integer-key copy.
    fn contain_keys(&self, array: &ArrayRef) -> Result<BooleanArray> {
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
}

#[derive(Debug)]
enum KeyDirectory<T> {
    Swiss(HashTable<KeyEntry<T>>),
    Integer(IntegerDirectory<T>),
    Dense(DenseIntegerDirectory<T>),
}

#[derive(Debug)]
pub(super) struct PartitionIndex<T> {
    directory: KeyDirectory<T>,
    /// Duplicate-key members addressed by the directory's group handles.
    groups: MatchedGroups<T>,
}

impl<T: RowId> KeyEntry<T> {
    pub(super) fn matches(&self) -> MatchHandle<T> {
        match self.group_plus_one.as_index() {
            0 => MatchHandle::One(self.representative_row),
            group => MatchHandle::Group(T::from_index(group - 1)),
        }
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
    fn lookup<K: ArrowPrimitiveType>(
        &self,
        array: &PrimitiveArray<K>,
        hashes: &[u64],
        reservation: &MemoryReservation,
        unique: bool,
    ) -> Result<PartitionLookup<T>>
    where
        K::Native: AsPrimitive<u64>,
    {
        let has_nulls = array.null_count() > 0;
        let find = |row: usize| {
            let key: u64 = array.value(row).as_();
            self.table.find(hashes[row], |entry| entry.key == key)
        };
        if unique {
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
            .map(PartitionLookup::Unique);
        }
        MatchHandleArray::from_lookup(array.len(), reservation, |row| {
            if has_nulls && array.is_null(row) {
                self.null
            } else {
                find(row).map(IntegerKeyEntry::matches)
            }
        })
        .map(PartitionLookup::Groups)
    }
}

impl<T: RowId> PartitionIndex<T> {
    fn mode(&self) -> LookupMode {
        match self.directory {
            KeyDirectory::Dense(_) => LookupMode::Dense,
            KeyDirectory::Integer(_) => LookupMode::Integer,
            KeyDirectory::Swiss(_) => LookupMode::General,
        }
    }

    /// Select a directory layout, then pack the duplicate groups it discovers.
    fn build(
        keys: &[ArrayRef],
        hashes: &[u64],
        null_equality: NullEquality,
        dense_range: Option<(u64, u64)>,
        reservation: &MemoryReservation,
        random_state: &RandomState,
    ) -> Result<Self> {
        let mut groups = GroupBuilder::new(reservation);
        let valid = matchable_join_keys(keys, null_equality);
        let directory = if let Some(range) = dense_range {
            Self::build_dense_directory(
                keys,
                hashes,
                valid.as_ref(),
                range,
                reservation,
                &mut groups,
            )?
        } else if let Some(hash_key) = (keys.len() == 1)
            .then(|| integer_hasher(keys[0].data_type()))
            .flatten()
        {
            Self::build_integer_directory(
                &keys[0],
                hashes,
                null_equality,
                hash_key,
                random_state,
                reservation,
                &mut groups,
            )?
        } else {
            Self::build_general_directory(
                keys,
                hashes,
                valid.as_ref(),
                reservation,
                &mut groups,
            )?
        };
        Ok(Self {
            directory,
            groups: groups.finish(reservation)?,
        })
    }

    /// Map dense integer keys directly to slots at `key - min`, promoting
    /// repeated keys to group handles. `values` contains integer values, not hashes.
    fn build_dense_directory(
        keys: &[ArrayRef],
        values: &[u64],
        valid: Option<&NullBuffer>,
        (min, max): (u64, u64),
        reservation: &MemoryReservation,
        groups: &mut GroupBuilder<T>,
    ) -> Result<KeyDirectory<T>> {
        if keys.len() != 1 || !ArrayMap::is_supported_type(keys[0].data_type()) {
            return internal_err!("Dense join index requires one integer key");
        }
        let range = ArrayMap::calculate_range(min, max);
        if range >= usize::MAX as u64 {
            return internal_err!("Dense join index key range is too large");
        }
        let len = range as usize + 1;
        let bytes = len
            .checked_mul(size_of::<T>())
            .ok_or_else(|| exec_datafusion_err!("Dense join index size overflow"))?;
        reservation.try_grow(bytes)?;
        let mut dense = DenseIntegerDirectory {
            offset: min,
            slots: vec![T::default(); len],
            is_group: None,
            null: None,
            distinct: 0,
        };
        let mut group_flags: Option<BooleanBufferBuilder> = None;
        for (row, &key) in values.iter().enumerate() {
            if valid.is_some_and(|valid| valid.is_null(row)) {
                continue;
            }
            // Matchable NULL keys share a separate handle.
            if keys[0].is_null(row) {
                match &mut dense.null {
                    Some(handle) => groups.add(handle, T::from_index(row))?,
                    slot @ None => {
                        *slot = Some(MatchHandle::One(T::from_index(row)));
                        dense.distinct += 1;
                    }
                }
                continue;
            }
            let position = key.wrapping_sub(min);
            if position >= len as u64 {
                return internal_err!("Build key is outside the dense join index range");
            }
            let position = position as usize;
            let slot = dense.slots[position].as_index();
            // Keep singleton row IDs inline; repeated keys use group handles.
            if slot == 0 {
                dense.slots[position] = T::from_index(row + 1);
                dense.distinct += 1;
                continue;
            }
            let already_group = group_flags
                .as_ref()
                .is_some_and(|flags| flags.get_bit(position));
            let mut handle = if already_group {
                MatchHandle::Group(T::from_index(slot - 1))
            } else {
                MatchHandle::One(T::from_index(slot - 1))
            };
            groups.add(&mut handle, T::from_index(row))?;
            let MatchHandle::Group(group) = handle else {
                unreachable!()
            };
            if group_flags.is_none() {
                reservation.try_grow(bitmap_bytes(len))?;
                let mut flags = BooleanBufferBuilder::new(len);
                flags.append_n(len, false);
                group_flags = Some(flags);
            }
            group_flags.as_mut().unwrap().set_bit(position, true);
            dense.slots[position] = T::from_index(group.as_index() + 1);
        }
        dense.is_group = group_flags.map(|mut flags| flags.finish());
        Ok(KeyDirectory::Dense(dense))
    }

    /// Store integer keys inline in hash entries for exact comparison;
    /// repeated keys and matchable NULLs accumulate in the shared group builder.
    fn build_integer_directory(
        key_array: &ArrayRef,
        hashes: &[u64],
        null_equality: NullEquality,
        hash_key: fn(u64, &RandomState) -> u64,
        random_state: &RandomState,
        reservation: &MemoryReservation,
        groups: &mut GroupBuilder<T>,
    ) -> Result<KeyDirectory<T>> {
        reservation.try_grow(estimate_memory_size::<IntegerKeyEntry<T>>(
            hashes.len(),
            size_of::<HashTable<IntegerKeyEntry<T>>>(),
        )?)?;
        let mut directory = IntegerDirectory {
            table: HashTable::with_capacity(hashes.len()),
            null: None,
            hash_key,
        };
        macro_rules! build_integer {
            ($type:ty) => {{
                let array = key_array.as_primitive::<$type>();
                for (row, &hash) in hashes.iter().enumerate() {
                    if array.is_null(row) {
                        if null_equality == NullEquality::NullEqualsNull {
                            match &mut directory.null {
                                Some(handle) => groups.add(handle, T::from_index(row))?,
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
                            groups.add(&mut handle, T::from_index(row))?;
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
        Ok(KeyDirectory::Integer(directory))
    }

    /// Find general keys by hash, then compare against a representative build row
    /// so only exact matches share a group, even when different keys have the same hash.
    fn build_general_directory(
        keys: &[ArrayRef],
        hashes: &[u64],
        valid: Option<&NullBuffer>,
        reservation: &MemoryReservation,
        groups: &mut GroupBuilder<T>,
    ) -> Result<KeyDirectory<T>> {
        reservation.try_grow(estimate_memory_size::<KeyEntry<T>>(
            hashes.len(),
            size_of::<HashTable<KeyEntry<T>>>(),
        )?)?;
        let mut table: HashTable<KeyEntry<T>> = HashTable::with_capacity(hashes.len());
        // NULL != NULL rows are excluded before comparison. Using NULL
        // equality here avoids rebuilding dictionary logical-null masks;
        // it does not change equality of nested children.
        let comparator = JoinKeyComparator::new_with_normalized_left(
            keys,
            keys,
            &vec![SortOptions::default(); keys.len()],
            NullEquality::NullEqualsNull,
        )?;
        for (row, &hash) in hashes.iter().enumerate() {
            if valid.is_some_and(|valid| valid.is_null(row)) {
                continue;
            }
            match table.entry(
                hash,
                |entry| {
                    entry.hash == hash
                        && comparator.is_equal(entry.representative_row.as_index(), row)
                },
                |entry| entry.hash,
            ) {
                Entry::Occupied(mut entry) => {
                    let entry = entry.get_mut();
                    let mut handle = entry.matches();
                    groups.add(&mut handle, T::from_index(row))?;
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
        Ok(KeyDirectory::Swiss(table))
    }

    fn len(&self) -> usize {
        match &self.directory {
            KeyDirectory::Swiss(table) => table.len(),
            KeyDirectory::Integer(integer) => {
                integer.table.len() + usize::from(integer.null.is_some())
            }
            KeyDirectory::Dense(dense) => dense.distinct,
        }
    }

    fn contains_hash(&self, hash: u64, random_state: &RandomState) -> bool {
        match &self.directory {
            KeyDirectory::Swiss(table) => {
                table.find(hash, |entry| entry.hash == hash).is_some()
            }
            KeyDirectory::Integer(integer) => {
                (hash == 0 && integer.null.is_some())
                    || integer
                        .table
                        .find(hash, |entry| {
                            (integer.hash_key)(entry.key, random_state) == hash
                        })
                        .is_some()
            }
            // A hash-only filter cannot reject values from an integer directory.
            KeyDirectory::Dense(_) => true,
        }
    }

    fn lookup(&self, scratch: &mut LookupScratch) -> Result<PartitionLookup<T>> {
        let unique = self.groups.rows.is_empty();
        let table = match &self.directory {
            KeyDirectory::Dense(_) | KeyDirectory::Integer(_) => {
                macro_rules! lookup {
                    ($type:ty) => {{
                        let array = scratch.probe_keys[0].as_primitive::<$type>();
                        return match &self.directory {
                            KeyDirectory::Dense(dense) => {
                                dense.lookup(array, &scratch.reservation, unique)
                            }
                            KeyDirectory::Integer(integer) => integer.lookup(
                                array,
                                &scratch.hashes,
                                &scratch.reservation,
                                unique,
                            ),
                            KeyDirectory::Swiss(_) => unreachable!(),
                        };
                    }};
                }
                match scratch.probe_keys[0].data_type() {
                    DataType::Int8 => lookup!(arrow::datatypes::Int8Type),
                    DataType::Int16 => lookup!(arrow::datatypes::Int16Type),
                    DataType::Int32 => lookup!(arrow::datatypes::Int32Type),
                    DataType::Int64 => lookup!(arrow::datatypes::Int64Type),
                    DataType::UInt8 => lookup!(arrow::datatypes::UInt8Type),
                    DataType::UInt16 => lookup!(arrow::datatypes::UInt16Type),
                    DataType::UInt32 => lookup!(arrow::datatypes::UInt32Type),
                    DataType::UInt64 => lookup!(arrow::datatypes::UInt64Type),
                    _ => {
                        return internal_err!("Integer join index requires integer keys");
                    }
                }
            }
            KeyDirectory::Swiss(table) => table,
        };
        scratch.lookup_swiss(table, unique)
    }
}
