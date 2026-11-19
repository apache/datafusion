// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: See the NOTICE file distributed with this work for additional information regarding copyright ownership
// SPDX-FileContributor: Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements

//! Hashing, normalization, and exact comparison of candidate key pairs.
//!
//! A hash match is only a candidate. General Swiss lookup compares candidate
//! representatives column by column, once per key rather than per duplicate.
//! If `A` and `B` share a hash, rejecting representative `A` must retry the
//! directory: another entry may hold `B`. Verified unique candidates become
//! output pairs in place; duplicate keys retain handles for later expansion.

use std::fmt::{self, Debug};
use std::mem::size_of;
use std::sync::Arc;

use arrow::array::*;
use arrow::buffer::NullBuffer;
use arrow::compute::SortOptions;
use arrow::datatypes::{DataType, TimeUnit};
use datafusion_common::float_semantics::{has_float_leaf, normalize};
use datafusion_common::hash_utils::{RandomState, create_hashes};
use datafusion_common::{NullEquality, Result, internal_err};
use datafusion_execution::memory_pool::MemoryReservation;
use hashbrown::HashTable;

use super::directory::KeyEntry;
use super::matches::{HandleBuilder, PartitionLookup, RowId, UniqueMatches};
use super::reserve;
use crate::joins::array_map::ArrayMap;
use crate::joins::utils::{JoinKeyComparator, matchable_join_keys};

#[derive(Clone, Copy, PartialEq, Eq)]
pub(super) enum LookupMode {
    Dense,
    Integer,
    General,
}

/// Reusable keys and candidates; output continuation belongs to the join operator.
/// Key arrays and comparators are retained only for the duration of lookup.
pub(crate) struct LookupScratch {
    pub(super) hashes: Vec<u64>,
    pub(super) candidate_rows: Vec<u32>,
    pub(super) representatives: Vec<u64>,
    equal: Vec<bool>,
    valid: Option<NullBuffer>,
    pub(super) build_keys: Vec<ArrayRef>,
    pub(super) probe_keys: Vec<ArrayRef>,
    comparator: Option<JoinKeyComparator>,
    column_comparators: Vec<Option<JoinKeyComparator>>,
    null_equality: NullEquality,
    batch_reservation: MemoryReservation,
    pub(super) reservation: MemoryReservation,
}

impl Debug for LookupScratch {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("LookupScratch")
            .field("hash_capacity", &self.hashes.capacity())
            .finish_non_exhaustive()
    }
}

impl LookupScratch {
    pub(crate) fn new(reservation: &MemoryReservation) -> Self {
        Self {
            hashes: vec![],
            candidate_rows: vec![],
            representatives: vec![],
            equal: vec![],
            valid: None,
            build_keys: vec![],
            probe_keys: vec![],
            comparator: None,
            column_comparators: vec![],
            null_equality: NullEquality::NullEqualsNothing,
            batch_reservation: reservation.new_empty(),
            reservation: reservation.new_empty(),
        }
    }

    pub(super) fn release_batch(&mut self) {
        self.valid = None;
        self.comparator = None;
        self.column_comparators.clear();
        self.build_keys.clear();
        self.probe_keys.clear();
        self.batch_reservation.free();
    }

    pub(super) fn prepare(
        &mut self,
        build_keys: &[ArrayRef],
        probe_keys: &[ArrayRef],
        null_equality: NullEquality,
        random_state: &RandomState,
        mode: LookupMode,
    ) -> Result<()> {
        self.release_batch();
        self.candidate_rows.clear();
        self.representatives.clear();
        self.equal.clear();
        self.null_equality = null_equality;
        let len = probe_keys[0].len();
        if matches!(mode, LookupMode::Dense | LookupMode::Integer) {
            if probe_keys.len() != 1
                || !ArrayMap::is_supported_type(probe_keys[0].data_type())
            {
                return internal_err!("Dense join index requires one integer key");
            }
            reserve(&mut self.probe_keys, 1, &self.reservation)?;
            self.probe_keys.push(Arc::clone(&probe_keys[0]));
            if mode == LookupMode::Dense {
                return Ok(());
            }
        } else {
            let normalize_budget: usize = probe_keys
                .iter()
                .filter(|array| has_float_leaf(array.data_type()))
                .map(|array| array.get_array_memory_size())
                .sum();
            let metadata_budget =
                probe_keys.len() * 512 + len.div_ceil(8) * (probe_keys.len() + 1);
            self.batch_reservation
                .try_grow(normalize_budget + metadata_budget)?;
            // These Vec capacities survive release_batch and stay charged to the
            // reusable scratch, separately from temporary arrays and comparators.
            reserve(&mut self.probe_keys, probe_keys.len(), &self.reservation)?;
            reserve(&mut self.build_keys, build_keys.len(), &self.reservation)?;
            reserve(
                &mut self.column_comparators,
                build_keys.len(),
                &self.reservation,
            )?;
            let mut normalized_bytes = 0;
            for key in probe_keys {
                let normalized = normalize(Arc::clone(key));
                if !Arc::ptr_eq(key, &normalized) {
                    normalized_bytes += normalized.get_array_memory_size();
                }
                self.probe_keys.push(normalized);
            }
            self.batch_reservation
                .try_resize(normalized_bytes + metadata_budget)?;
            self.build_keys.extend_from_slice(build_keys);
            self.column_comparators
                .resize_with(build_keys.len(), || None);
            self.valid = matchable_join_keys(probe_keys, null_equality);
        }
        reserve(&mut self.hashes, len, &self.reservation)?;
        self.hashes.resize(len, 0);
        // Hash kernels can leave NULL slots untouched in this reusable buffer.
        self.hashes.fill(0);
        let hash_scratch = self.reservation.new_empty();
        hash_scratch.try_grow(hash_scratch_bytes(probe_keys))?;
        create_hashes(probe_keys, random_state, &mut self.hashes)?;
        Ok(())
    }

    /// Verify one representative per candidate key, retry hash collisions, then
    /// retain group handles or turn unique candidates directly into row pairs.
    pub(super) fn lookup_swiss<T: RowId>(
        &mut self,
        table: &HashTable<KeyEntry<T>>,
        unique: bool,
    ) -> Result<PartitionLookup<T>> {
        let len = self.probe_keys[0].len();
        let mut grouped = if unique {
            None
        } else {
            Some(HandleBuilder::new(len, &self.reservation)?)
        };
        reserve(&mut self.candidate_rows, len, &self.reservation)?;
        reserve(&mut self.representatives, len, &self.reservation)?;
        reserve(&mut self.equal, len, &self.reservation)?;
        for row in 0..len {
            if self.valid.as_ref().is_some_and(|valid| valid.is_null(row)) {
                continue;
            }
            let hash = self.hashes[row];
            if let Some(entry) = table.find(hash, |entry| entry.hash == hash) {
                self.candidate_rows.push(row as u32);
                self.representatives.push(entry.representative_row.as_u64());
                if let Some(output) = &mut grouped {
                    output.set(row, Some(entry.matches()));
                }
            }
        }
        self.equal.resize(self.candidate_rows.len(), true);
        for column in 0..self.build_keys.len() {
            compare_column(self, column)?;
        }
        // The candidate pair buffers become the unique result. Compact rejected
        // candidates in place instead of allocating another pair array.
        let mut matched = 0;
        for position in 0..self.candidate_rows.len() {
            let row = self.candidate_rows[position] as usize;
            if self.equal[position] {
                if unique {
                    self.representatives[matched] = self.representatives[position];
                    self.candidate_rows[matched] = row as u32;
                    matched += 1;
                }
                continue;
            }
            if self.comparator.is_none() {
                self.comparator = Some(JoinKeyComparator::new_with_normalized_left(
                    &self.build_keys,
                    &self.probe_keys,
                    &vec![SortOptions::default(); self.build_keys.len()],
                    // Both sides already passed matchable-key filtering.
                    NullEquality::NullEqualsNull,
                )?);
            }
            let hash = self.hashes[row];
            let comparator = self.comparator.as_ref().unwrap();
            // A different exact key can share the first candidate's
            // full hash. Retry all entries with that hash on rejection.
            let entry = table.find(hash, |entry| {
                entry.hash == hash
                    && comparator.is_equal(entry.representative_row.as_index(), row)
            });
            if unique {
                if let Some(entry) = entry {
                    self.representatives[matched] = entry.representative_row.as_u64();
                    self.candidate_rows[matched] = row as u32;
                    matched += 1;
                }
            } else if let Some(output) = &mut grouped {
                output.set(row, entry.map(KeyEntry::matches));
            }
        }
        if unique {
            self.representatives.truncate(matched);
            self.candidate_rows.truncate(matched);
            let bytes = self.representatives.capacity() * size_of::<u64>()
                + self.candidate_rows.capacity() * size_of::<u32>();
            let reservation = self.reservation.split(bytes);
            Ok(PartitionLookup::Unique(UniqueMatches {
                build_rows: std::mem::take(&mut self.representatives).into(),
                probe_rows: std::mem::take(&mut self.candidate_rows).into(),
                probe_len: len,
                _reservation: reservation,
            }))
        } else {
            Ok(PartitionLookup::Groups(grouped.unwrap().finish()))
        }
    }
}

/// Reuse ArrayMap's wrapping signed-integer representation. Dispatch once per
/// array; NULL slots are ignored separately by the directory.
pub(super) fn integer_values(array: &ArrayRef, output: &mut [u64]) -> Result<()> {
    macro_rules! values {
        ($type:ty) => {{
            let array = array.as_any().downcast_ref::<$type>().unwrap();
            for (output, value) in output.iter_mut().zip(array.values()) {
                *output = *value as u64;
            }
        }};
    }
    match array.data_type() {
        DataType::Int8 => values!(Int8Array),
        DataType::Int16 => values!(Int16Array),
        DataType::Int32 => values!(Int32Array),
        DataType::Int64 => values!(Int64Array),
        DataType::UInt8 => values!(UInt8Array),
        DataType::UInt16 => values!(UInt16Array),
        DataType::UInt32 => values!(UInt32Array),
        DataType::UInt64 => values!(UInt64Array),
        _ => return internal_err!("Dense join index requires integer keys"),
    }
    Ok(())
}

/// Typed indexed equality, with reusable candidate flags. Native float equality
/// is deliberately excluded: identical NaN bits match and signed zero has been
/// normalized by the same comparator machinery used by the join operators.
fn compare_column(state: &mut LookupScratch, column: usize) -> Result<()> {
    let left = state.build_keys[column].as_ref();
    let right = state.probe_keys[column].as_ref();
    let null_equals_null = state.null_equality == NullEquality::NullEqualsNull;
    macro_rules! compare {
        ($type:ty) => {
            if let (Some(left), Some(right)) = (
                left.as_any().downcast_ref::<$type>(),
                right.as_any().downcast_ref::<$type>(),
            ) {
                for ((equal, &build), &probe) in state
                    .equal
                    .iter_mut()
                    .zip(&state.representatives)
                    .zip(&state.candidate_rows)
                {
                    if !*equal {
                        continue;
                    }
                    let build = build as usize;
                    let probe = probe as usize;
                    *equal = match (left.is_null(build), right.is_null(probe)) {
                        (false, false) => left.value(build) == right.value(probe),
                        (true, true) => null_equals_null,
                        _ => false,
                    };
                }
                return Ok(());
            }
        };
    }
    match left.data_type() {
        DataType::Boolean => compare!(BooleanArray),
        DataType::Int8 => compare!(Int8Array),
        DataType::Int16 => compare!(Int16Array),
        DataType::Int32 => compare!(Int32Array),
        DataType::Int64 => compare!(Int64Array),
        DataType::UInt8 => compare!(UInt8Array),
        DataType::UInt16 => compare!(UInt16Array),
        DataType::UInt32 => compare!(UInt32Array),
        DataType::UInt64 => compare!(UInt64Array),
        DataType::Decimal128(..) => compare!(Decimal128Array),
        DataType::Binary => compare!(BinaryArray),
        DataType::LargeBinary => compare!(LargeBinaryArray),
        DataType::BinaryView => compare!(BinaryViewArray),
        DataType::FixedSizeBinary(_) => compare!(FixedSizeBinaryArray),
        DataType::Utf8 => compare!(StringArray),
        DataType::LargeUtf8 => compare!(LargeStringArray),
        DataType::Utf8View => compare!(StringViewArray),
        DataType::Date32 => compare!(Date32Array),
        DataType::Date64 => compare!(Date64Array),
        DataType::Timestamp(unit, _) => match unit {
            TimeUnit::Second => compare!(TimestampSecondArray),
            TimeUnit::Millisecond => compare!(TimestampMillisecondArray),
            TimeUnit::Microsecond => compare!(TimestampMicrosecondArray),
            TimeUnit::Nanosecond => compare!(TimestampNanosecondArray),
        },
        _ => {}
    }
    if state.column_comparators[column].is_none() {
        // Unmatchable rows were excluded before staging, so this comparator
        // does not need a second logical-NULL mask for either side.
        state.column_comparators[column] =
            Some(JoinKeyComparator::new_with_normalized_left(
                &state.build_keys[column..=column],
                &state.probe_keys[column..=column],
                &[SortOptions::default()],
                NullEquality::NullEqualsNull,
            )?);
    }
    let comparator = state.column_comparators[column].as_ref().unwrap();
    for ((equal, &build), &probe) in state
        .equal
        .iter_mut()
        .zip(&state.representatives)
        .zip(&state.candidate_rows)
    {
        *equal = *equal && comparator.is_equal(build as usize, probe as usize);
    }
    Ok(())
}

/// Upper bound for the temporary child hashes allocated inside `create_hashes`.
/// Arrays are hashed one column at a time. A parent buffer remains live while
/// hashing its children; dictionaries and nested lists can have many more child
/// values than parent rows. Union hashing can additionally materialize a taken
/// child, so its bound includes the child buffers as well.
pub(super) fn hash_scratch_bytes(keys: &[ArrayRef]) -> usize {
    fn nested(data: &ArrayData) -> usize {
        let children = data.child_data();
        let child_scratch = children.iter().map(nested).max().unwrap_or(0);
        let child_hashes =
            |child: &ArrayData| child.len().saturating_mul(size_of::<u64>());
        let own = match data.data_type() {
            DataType::Dictionary(..) => children.first().map(child_hashes).unwrap_or(0),
            DataType::Struct(_) => data.len().saturating_mul(size_of::<u64>()),
            DataType::List(_)
            | DataType::LargeList(_)
            | DataType::ListView(_)
            | DataType::LargeListView(_)
            | DataType::FixedSizeList(..)
            | DataType::Map(..) => children.first().map(child_hashes).unwrap_or(0),
            DataType::RunEndEncoded(..) => children.get(1).map(child_hashes).unwrap_or(0),
            DataType::Union(..) => {
                let child_buffers = children.iter().fold(0usize, |total, child| {
                    total
                        .saturating_add(child_hashes(child))
                        .saturating_add(child.get_array_memory_size())
                });
                // Sparse unions group row IDs and may keep both full-length
                // and taken child hashes; dense unions retain all child hashes.
                child_buffers
                    .saturating_add(data.len().saturating_mul(24))
                    .saturating_add(128 * size_of::<Vec<u64>>())
            }
            _ => return 0,
        };
        own.saturating_add(child_scratch)
            .saturating_add(children.len().saturating_mul(256))
    }
    keys.iter()
        .map(|key| match key.data_type() {
            DataType::Dictionary(..)
            | DataType::Struct(_)
            | DataType::List(_)
            | DataType::LargeList(_)
            | DataType::ListView(_)
            | DataType::LargeListView(_)
            | DataType::FixedSizeList(..)
            | DataType::Map(..)
            | DataType::RunEndEncoded(..)
            | DataType::Union(..) => nested(&key.to_data()),
            _ => 0,
        })
        .max()
        .unwrap_or(0)
}
