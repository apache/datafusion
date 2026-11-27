// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: See the NOTICE file distributed with this work for additional information regarding copyright ownership
// SPDX-FileContributor: Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements

//! Reusable lookup buffers, hashing, and exact key comparison utilities.

use std::fmt::{self, Debug};
use std::mem::size_of;

use arrow::array::*;
use arrow::buffer::NullBuffer;
use arrow::compute::SortOptions;
use arrow::datatypes::{DataType, TimeUnit};
use datafusion_common::hash_utils::{RandomState, create_hashes};
use datafusion_common::{NullEquality, Result};
use datafusion_execution::memory_pool::MemoryReservation;

use super::matches::MatchHandles;
use super::reserve;
use crate::joins::utils::JoinKeyComparator;

/// Probe-side workspace reused by one join processor across input batches.
///
/// See [`JoinHashIndex`](super::directory::JoinHashIndex) for the design overview.
///
/// Each new probe batch calls [`super::JoinIndexMap::lookup_batch`], which resets
/// this scratch with the new probe batch.
///
/// # Example
///
/// For one string key, assuming no hash collisions, general lookup produces the
/// following scratch after candidate verification. Row IDs are zero-based;
/// `h(key)` denotes the hash of that key.
///
/// ```text
/// build_keys[0]:  [A, B, A, C]       // all build rows
/// probe_keys[0]:  [B, X, A]          // current probe batch
///
/// probe_hashes:         [h(B), h(X), h(A)] // indexed by probe row
/// // The values below mean (probe-rowid, build-rowid) pairs (0,1), (2,0). Both passed the
/// // exact equality check.
/// probe_candidate_rows: [0, 2]            // probe row IDs; X has no candidate
/// representatives:      [1, 0]            // corresponding build row IDs
/// equal:                [true, true]      // exact equality of each candidate pair
/// ```
pub(crate) struct LookupScratch {
    // Evaluated build/probe keys.
    pub(super) build_keys: Vec<ArrayRef>,
    pub(super) probe_keys: Vec<ArrayRef>,
    pub(super) null_equality: NullEquality,

    /// One hash per row of the current probe batch, combining all join-key columns.
    pub(super) probe_hashes: Vec<u64>,
    /// Probe rows eligible for lookup under NULL rules; None means all rows.
    ///
    /// For two nullable integer keys, with zero-based row IDs:
    ///
    /// ```text
    /// row | key1 | key2
    /// ----+------+-----
    ///  0  |  1   | NULL
    ///  1  | NULL |  1
    ///  2  |  1   |  1
    ///  3  | NULL | NULL
    /// ```
    ///
    /// - [`NullEquality::NullEqualsNothing`]: valid indices are `[2]`;
    ///   every key must be non-NULL.
    /// - [`NullEquality::NullEqualsNull`]: valid indices are `[0, 1, 2, 3]`,
    ///   represented by `None` since no rows are excluded.
    ///
    /// Eligible rows still need matching build keys.
    pub(super) probe_valid: Option<NullBuffer>,

    // Probe scratches.
    // See example on this struct for details.
    pub(super) probe_candidate_rows: Vec<u32>,
    pub(super) representatives: Vec<u64>,
    pub(super) equal: Vec<bool>,

    /// Lazy per-column fallback when direct typed equality is unavailable.
    pub(super) column_comparators: Vec<Option<JoinKeyComparator>>,
    /// Lazy whole-key comparator for retrying rejected hash candidates.
    pub(super) comparator: Option<JoinKeyComparator>,

    // Keep reservations after the allocations they account for.
    /// Temporary normalized arrays and metadata, released after lookup.
    pub(super) batch_reservation: MemoryReservation,
    /// Reusable vector capacity.
    pub(super) reservation: MemoryReservation,
}

impl Debug for LookupScratch {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("LookupScratch")
            .field("probe_hash_capacity", &self.probe_hashes.capacity())
            .finish_non_exhaustive()
    }
}

impl LookupScratch {
    pub(crate) fn new(reservation: &MemoryReservation) -> Self {
        Self {
            build_keys: vec![],
            probe_keys: vec![],
            null_equality: NullEquality::NullEqualsNothing,
            probe_hashes: vec![],
            probe_valid: None,
            probe_candidate_rows: vec![],
            representatives: vec![],
            equal: vec![],
            column_comparators: vec![],
            comparator: None,
            batch_reservation: reservation.new_empty(),
            reservation: reservation.new_empty(),
        }
    }

    /// Release this lookup's arrays and helpers while retaining vector capacity.
    pub(super) fn release_batch(&mut self) {
        self.probe_valid = None;
        self.comparator = None;
        self.column_comparators.clear();
        self.build_keys.clear();
        self.probe_keys.clear();
        self.batch_reservation.free();
    }

    /// Start lookup for a new probe batch by clearing the previous candidate state.
    pub(super) fn reset(&mut self) {
        self.release_batch();
        self.probe_candidate_rows.clear();
        self.representatives.clear();
        self.equal.clear();
    }

    /// Return verified singleton candidates in output-ready buffers.
    /// Rejected candidates are removed in place, preserving probe order.
    pub(super) fn take_unique_matches(&mut self) -> MatchHandles<u64> {
        let mut matched = 0;
        for (position, &equal) in self.equal.iter().enumerate() {
            if equal {
                self.representatives[matched] = self.representatives[position];
                self.probe_candidate_rows[matched] = self.probe_candidate_rows[position];
                matched += 1;
            }
        }
        self.representatives.truncate(matched);
        self.probe_candidate_rows.truncate(matched);
        let bytes = self.representatives.capacity() * size_of::<u64>()
            + self.probe_candidate_rows.capacity() * size_of::<u32>();
        self.reservation.shrink(bytes);
        MatchHandles::from_buffers(
            std::mem::take(&mut self.representatives),
            std::mem::take(&mut self.probe_candidate_rows),
            None,
        )
    }
}

pub(super) fn hash_probe_keys(
    probe_keys: &[ArrayRef],
    random_state: &RandomState,
    scratch: &mut LookupScratch,
) -> Result<()> {
    let len = probe_keys[0].len();
    reserve(&mut scratch.probe_hashes, len, &scratch.reservation)?;
    scratch.probe_hashes.resize(len, 0);
    // Hash kernels can leave NULL slots untouched in this reusable buffer.
    scratch.probe_hashes.fill(0);
    let hash_scratch = scratch.reservation.new_empty();
    hash_scratch.try_grow(hash_scratch_bytes(probe_keys))?;
    create_hashes(probe_keys, random_state, &mut scratch.probe_hashes)?;
    Ok(())
}

/// Hash build keys in forward slices with a reusable buffer.
///
/// It avoids allocating hash buffers for all keys.
pub(super) fn for_each_hash_chunk(
    keys: &[ArrayRef],
    random_state: &RandomState,
    // slice size
    hash_chunk_size: usize,
    mut insert: impl FnMut(usize, &[u64]),
) -> Result<()> {
    let num_rows = keys[0].len();
    if num_rows == 0 {
        return Ok(());
    }
    let hash_chunk_size = hash_chunk_size.max(1).min(num_rows);
    let mut hashes = vec![0; hash_chunk_size];
    for offset in (0..num_rows).step_by(hash_chunk_size) {
        let len = hash_chunk_size.min(num_rows - offset);
        let chunk: Vec<_> = keys.iter().map(|key| key.slice(offset, len)).collect();
        hashes.truncate(len);
        // Hash kernels can leave NULL slots untouched in this reusable buffer.
        hashes.fill(0);
        create_hashes(&chunk, random_state, &mut hashes)?;
        insert(offset, &hashes);
    }
    Ok(())
}

/// Check one join-key column for every candidate pair, updating `scratch.equal`.
///
/// # Example
///
/// ```text
/// build_keys[column]:   [A, B, C]
/// probe_keys[column]:   [B, C]
/// probe_candidate_rows: [0, 1]
/// representatives:      [0, 2]
/// comparisons:          [B == A, C == C]
/// equal:                [true, true] -> [false, true]
/// ```
///
/// This rejects the first candidate pair, not necessarily the probe row: lookup
/// may retry another entry with the same hash afterward.
pub(super) fn compare_column(scratch: &mut LookupScratch, column: usize) -> Result<()> {
    let left = scratch.build_keys[column].as_ref();
    let right = scratch.probe_keys[column].as_ref();
    let null_equals_null = scratch.null_equality == NullEquality::NullEqualsNull;
    macro_rules! compare {
        ($type:ty) => {
            if let (Some(left), Some(right)) = (
                left.as_any().downcast_ref::<$type>(),
                right.as_any().downcast_ref::<$type>(),
            ) {
                for ((equal, &build), &probe) in scratch
                    .equal
                    .iter_mut()
                    .zip(&scratch.representatives)
                    .zip(&scratch.probe_candidate_rows)
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
    if scratch.column_comparators[column].is_none() {
        // Unmatchable rows were excluded before staging, so this comparator
        // does not need a second logical-NULL mask for either side.
        scratch.column_comparators[column] =
            Some(JoinKeyComparator::new_with_normalized_left(
                &scratch.build_keys[column..=column],
                &scratch.probe_keys[column..=column],
                &[SortOptions::default()],
                NullEquality::NullEqualsNull,
            )?);
    }
    let comparator = scratch.column_comparators[column].as_ref().unwrap();
    for ((equal, &build), &probe) in scratch
        .equal
        .iter_mut()
        .zip(&scratch.representatives)
        .zip(&scratch.probe_candidate_rows)
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
