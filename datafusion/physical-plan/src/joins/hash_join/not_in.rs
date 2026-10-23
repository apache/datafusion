// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

//! Null-aware (`NOT IN`) hash join: the correlated marking pass, which
//! records the rows whose `NOT IN` is UNKNOWN, and its helpers.

use crate::hash_utils::create_hashes;
use crate::joins::SharedBitmapBuilder;
use crate::joins::hash_join::exec::JoinLeftData;
use crate::joins::hash_join::stream::lookup_join_hashmap;
use crate::joins::join_hash_map::ProbeOffset::AtRow;
use crate::joins::utils::{JoinFilter, JoinHashMapType, apply_join_filter_to_indices};

use arrow::array::{Array, ArrayRef, AsArray, BooleanArray, UInt32Array, UInt64Array};
use arrow::buffer::{BooleanBuffer, NullBuffer};
use arrow::compute::{filter, take};
use arrow::record_batch::RecordBatch;
use datafusion_common::hash_utils::RandomState;
use datafusion_common::{JoinSide, JoinType, NullEquality, Result};
use datafusion_expr::{ColumnarValue, Operator};
use datafusion_physical_expr_common::datum::apply_cmp;

/// The build rows with a NULL `NOT IN` value key (in any element of a
/// multi-column key), used by correlated null-aware joins (see
/// [`NullAwareMode`](super::exec::NullAwareMode)).
///
/// Such rows make `NOT IN` UNKNOWN whenever *any* probe row in their
/// correlation scope passes the join filter (unless another element is a
/// definite mismatch), so every probe row must be tested against them.
/// Restricting this lookup to the NULL-valued build rows keeps it
/// proportional to the number of NULLs instead of enumerating every scope
/// match of every probe row.
pub(super) struct NullValueBuildRows {
    /// Hash table keyed by the correlation scope values of the NULL-valued
    /// build rows. Stored positions index into `scope_values`/`build_indices`,
    /// not the full build batch. `None` when the join has no correlation
    /// scope keys, so every probe row is in scope.
    pub(super) scope_map: Option<Box<dyn JoinHashMapType>>,
    /// Correlation scope key values of the NULL-valued build rows.
    pub(super) scope_values: Vec<ArrayRef>,
    /// Maps positions in `map`/`scope_values` back to row indices in the full
    /// build batch.
    pub(super) build_indices: UInt64Array,
}

/// Returns the rows where at least one of `value_keys` is NULL, or `None` when
/// there is no such row.
pub(super) fn null_value_key_mask(value_keys: &[ArrayRef]) -> Option<BooleanArray> {
    let logical_nulls: Vec<_> = value_keys.iter().map(|v| v.logical_nulls()).collect();
    let valid = NullBuffer::union_many(logical_nulls.iter().map(Option::as_ref))?;
    (valid.null_count() > 0).then(|| BooleanArray::new(!valid.inner(), None))
}

/// Where a correlated null-aware join records the rows whose `NOT IN` is
/// UNKNOWN, see [`mark_null_candidates_for_probe_batch`].
#[derive(Clone, Copy)]
pub(super) enum UnknownMarks<'a> {
    /// `LeftAnti` and `LeftMark` mark build rows, in the null-indices bitmap
    /// shared by all probe partitions.
    Build(&'a SharedBitmapBuilder),
    /// `RightAnti` marks the rows of the current probe batch.
    Probe(&'a SharedBitmapBuilder),
}

impl<'a> UnknownMarks<'a> {
    fn bitmap(self) -> &'a SharedBitmapBuilder {
        match self {
            Self::Build(bitmap) | Self::Probe(bitmap) => bitmap,
        }
    }

    /// The row of a (build, probe) candidate pair whose `NOT IN` the pair
    /// decides, and that is marked when the pair makes it UNKNOWN.
    fn unknown_row(self, build_idx: u64, probe_idx: u32) -> usize {
        match self {
            Self::Build(_) => build_idx as usize,
            Self::Probe(_) => probe_idx as usize,
        }
    }
}

/// Records which rows of a correlated null-aware join are UNKNOWN candidates
/// for this probe batch: the build rows for `LeftAnti` and `LeftMark`, the
/// probe rows for `RightAnti` (see [`UnknownMarks`]).
///
/// Key layout: `on[..V]` are the `NOT IN` value keys, `on[V..]` the (possibly
/// empty) correlation scope keys (see `HashJoinExec::null_aware`). A row is
/// NULL-valued when any of its value keys is NULL. Each (build, probe) pair
/// decides one of its rows (see [`UnknownMarks::unknown_row`]), and makes that
/// row's `NOT IN` UNKNOWN instead of TRUE (a `LeftMark` mark NULL instead of
/// FALSE) when either:
/// 1. the build row is NULL-valued, or
/// 2. the probe row is NULL-valued,
///
/// the two rows are in the same correlation scope, the pair passes the join
/// filter, and, for a multi-column value key, the two rows' value tuples are
/// not a definite mismatch: every element pair is equal or involves a NULL.
/// With a single value key a NULL on either side already rules out a mismatch.
/// An exact match, which makes `NOT IN` FALSE, is found by the main hash lookup
/// instead and takes precedence over these marks.
///
/// Case 1 pairs the NULL-valued build rows with all probe rows; case 2 pairs
/// all build rows with the NULL-valued probe rows. Scope keys narrow these
/// pairs through a hash lookup; without scope keys every pair is a candidate.
/// The value tuples and then the join filter, if any, decide which candidates
/// count.
///
/// A row stays UNKNOWN once it is marked, so pairs that decide a row already
/// known to be UNKNOWN are skipped, and the join filter is not evaluated for
/// them. Without scope keys this also ends the pairing as soon as every row
/// that can be decided is UNKNOWN.
#[expect(clippy::too_many_arguments)]
pub(super) fn mark_null_candidates_for_probe_batch(
    left_data: &JoinLeftData,
    probe_batch: &RecordBatch,
    probe_values: &[ArrayRef],
    num_value_keys: usize,
    filter: Option<&JoinFilter>,
    join_type: JoinType,
    marks: UnknownMarks<'_>,
    random_state: &RandomState,
    batch_size: usize,
    hashes_buffer: &mut Vec<u64>,
    probe_indices_buffer: &mut Vec<u32>,
    build_indices_buffer: &mut Vec<u64>,
) -> Result<()> {
    let null_value_build_rows = left_data.null_value_build_rows();
    let probe_null_value_mask = null_value_key_mask(&probe_values[..num_value_keys]);
    if null_value_build_rows.is_none() && probe_null_value_mask.is_none() {
        return Ok(());
    }

    debug_assert_eq!(
        left_data.values().len(),
        probe_values.len(),
        "build/probe key counts must match"
    );
    let (build_value_keys, build_scope_values) =
        left_data.values().split_at(num_value_keys);
    let (probe_value_keys, probe_scope_values) = probe_values.split_at(num_value_keys);

    // Keeps the candidate pairs whose value tuples are not a definite mismatch
    // and that pass the join filter, and marks their rows as UNKNOWN.
    let mut mark = |build_indices: UInt64Array, probe_indices: UInt32Array| {
        let (build_indices, probe_indices) =
            retain_unmarked(marks, build_indices, probe_indices);
        let (build_indices, probe_indices) = if num_value_keys > 1 {
            retain_value_mismatch_free(
                build_value_keys,
                probe_value_keys,
                build_indices,
                probe_indices,
            )?
        } else {
            (build_indices, probe_indices)
        };
        if build_indices.is_empty() {
            return Ok(());
        }
        let (build_indices, probe_indices) = match filter {
            Some(filter) => apply_join_filter_to_indices(
                left_data.batch(),
                probe_batch,
                build_indices,
                probe_indices,
                filter,
                JoinSide::Left,
                None,
                join_type,
            )?,
            None => (build_indices, probe_indices),
        };
        if !build_indices.is_empty() {
            let mut bitmap = marks.bitmap().lock();
            for (build_idx, probe_idx) in
                build_indices.values().iter().zip(probe_indices.values())
            {
                bitmap.set_bit(marks.unknown_row(*build_idx, *probe_idx), true);
            }
        }
        Ok(())
    };

    // Case 1: NULL-valued build rows paired with the probe rows in their
    // correlation scope.
    if let Some(null_rows) = null_value_build_rows {
        match &null_rows.scope_map {
            Some(scope_map) => {
                hashes_buffer.clear();
                hashes_buffer.resize(probe_batch.num_rows(), 0);
                create_hashes(probe_scope_values, random_state, hashes_buffer)?;

                for_each_scope_match(
                    scope_map.as_ref(),
                    &null_rows.scope_values,
                    probe_scope_values,
                    hashes_buffer,
                    batch_size,
                    probe_indices_buffer,
                    build_indices_buffer,
                    |positions, probe_indices| {
                        // The map indexes only the NULL-valued build rows;
                        // translate its positions back to build row indices.
                        let build_indices = UInt64Array::from_iter_values(
                            positions
                                .values()
                                .iter()
                                .map(|p| null_rows.build_indices.value(*p as usize)),
                        );
                        mark(build_indices, probe_indices)
                    },
                )?;
            }
            None => {
                for_each_unmarked_cross_product(
                    marks,
                    null_rows.build_indices.values().iter().copied(),
                    0..probe_batch.num_rows() as u32,
                    batch_size,
                    &mut mark,
                )?;
            }
        }
    }

    // Case 2: NULL-valued probe rows paired with the build rows in their
    // correlation scope.
    if let Some(null_mask) = probe_null_value_mask {
        let null_probe_rows = UInt32Array::from_iter_values(
            null_mask.values().set_indices().map(|i| i as u32),
        );

        match left_data.null_aware_scope_map() {
            Some(scope_map) => {
                let probe_null_scope_values = probe_scope_values
                    .iter()
                    .map(|values| {
                        Ok(arrow::compute::filter(values.as_ref(), &null_mask)?)
                    })
                    .collect::<Result<Vec<_>>>()?;

                hashes_buffer.clear();
                hashes_buffer.resize(null_probe_rows.len(), 0);
                create_hashes(&probe_null_scope_values, random_state, hashes_buffer)?;

                for_each_scope_match(
                    scope_map,
                    build_scope_values,
                    &probe_null_scope_values,
                    hashes_buffer,
                    batch_size,
                    probe_indices_buffer,
                    build_indices_buffer,
                    |build_indices, positions| {
                        // The lookup ran over only the NULL-valued probe rows;
                        // translate its positions back to probe row indices.
                        let probe_indices = UInt32Array::from_iter_values(
                            positions
                                .values()
                                .iter()
                                .map(|p| null_probe_rows.value(*p as usize)),
                        );
                        mark(build_indices, probe_indices)
                    },
                )?;
            }
            None => {
                for_each_unmarked_cross_product(
                    marks,
                    0..left_data.batch().num_rows() as u64,
                    null_probe_rows.values().iter().copied(),
                    batch_size,
                    &mut mark,
                )?;
            }
        }
    }

    Ok(())
}

/// Keeps the candidate pairs whose multi-column `NOT IN` value tuples are not a
/// definite mismatch, i.e. every element pair is equal or involves a NULL.
///
/// Such a pair compares UNKNOWN when some element is NULL (TRUE pairs, with no
/// NULL, are found by the hash lookup instead), whereas a pair with a definite
/// mismatch compares FALSE whatever its NULLs are: `(NULL, 1) = (2, 3)` is
/// `UNKNOWN AND FALSE`, which is FALSE.
///
/// Elements are compared with SQL `=` (see [`apply_cmp`]), which, like the
/// join's own key equality, treats `-0.0` and `+0.0` as equal.
fn retain_value_mismatch_free(
    build_value_keys: &[ArrayRef],
    probe_value_keys: &[ArrayRef],
    build_indices: UInt64Array,
    probe_indices: UInt32Array,
) -> Result<(UInt64Array, UInt32Array)> {
    if build_indices.is_empty() {
        return Ok((build_indices, probe_indices));
    }
    let mut keep: Option<BooleanBuffer> = None;
    for (build, probe) in build_value_keys.iter().zip(probe_value_keys) {
        let build = take(build.as_ref(), &build_indices, None)?;
        let probe = take(probe.as_ref(), &probe_indices, None)?;
        let eq = apply_cmp(
            Operator::Eq,
            &ColumnarValue::Array(build),
            &ColumnarValue::Array(probe),
        )?
        .into_array(build_indices.len())?;
        let eq = eq.as_boolean();
        // A NULL comparison (an element involving NULL) is not a mismatch.
        let not_mismatch = match eq.nulls() {
            Some(nulls) => eq.values() | &!nulls.inner(),
            None => eq.values().clone(),
        };
        keep = Some(match keep {
            Some(keep) => &keep & &not_mismatch,
            None => not_mismatch,
        });
    }
    match keep {
        Some(keep) if keep.count_set_bits() < keep.len() => {
            let keep = BooleanArray::new(keep, None);
            Ok((
                filter(&build_indices, &keep)?.as_primitive().clone(),
                filter(&probe_indices, &keep)?.as_primitive().clone(),
            ))
        }
        _ => Ok((build_indices, probe_indices)),
    }
}

/// Calls `f` with all correlation-scope matches between `build_scope_values`
/// and `probe_scope_values`, as chunks of at most `batch_size` pairs of
/// (position in `build_scope_values`, position in `probe_scope_values`).
/// `build_scope_values` must already be normalized, as for
/// [`lookup_join_hashmap`].
#[expect(clippy::too_many_arguments)]
fn for_each_scope_match(
    scope_map: &dyn JoinHashMapType,
    build_scope_values: &[ArrayRef],
    probe_scope_values: &[ArrayRef],
    hashes_buffer: &[u64],
    batch_size: usize,
    probe_indices_buffer: &mut Vec<u32>,
    build_indices_buffer: &mut Vec<u64>,
    mut f: impl FnMut(UInt64Array, UInt32Array) -> Result<()>,
) -> Result<()> {
    let mut offset = AtRow { row: 0 };
    let mut key_comparator = None;
    loop {
        let (build_indices, probe_indices, next_offset) = lookup_join_hashmap(
            scope_map,
            build_scope_values,
            probe_scope_values,
            NullEquality::NullEqualsNothing,
            hashes_buffer,
            None,
            batch_size,
            offset,
            probe_indices_buffer,
            build_indices_buffer,
            &mut key_comparator,
        )?;

        if !build_indices.is_empty() {
            f(build_indices, probe_indices)?;
        }

        let Some(next_offset) = next_offset else {
            break;
        };
        offset = next_offset;
    }

    Ok(())
}

/// Removes the candidate pairs that decide a row already marked UNKNOWN.
fn retain_unmarked(
    marks: UnknownMarks<'_>,
    build_indices: UInt64Array,
    probe_indices: UInt32Array,
) -> (UInt64Array, UInt32Array) {
    let bitmap = marks.bitmap().lock();
    let is_unmarked = |(build_idx, probe_idx): (&u64, &u32)| {
        !bitmap.get_bit(marks.unknown_row(*build_idx, *probe_idx))
    };
    if build_indices
        .values()
        .iter()
        .zip(probe_indices.values().iter())
        .all(is_unmarked)
    {
        return (build_indices, probe_indices);
    }
    let (build, probe): (Vec<u64>, Vec<u32>) = build_indices
        .values()
        .iter()
        .zip(probe_indices.values().iter())
        .filter(|pair| is_unmarked(*pair))
        .unzip();
    (build.into(), probe.into())
}

/// Calls `f` with the pairs of `build_rows` x `probe_rows` that decide a row
/// (see [`UnknownMarks::unknown_row`]) not yet marked UNKNOWN, as chunks of at
/// most `batch_size` pairs.
fn for_each_unmarked_cross_product(
    marks: UnknownMarks<'_>,
    build_rows: impl Iterator<Item = u64>,
    probe_rows: impl Iterator<Item = u32>,
    batch_size: usize,
    mut f: impl FnMut(UInt64Array, UInt32Array) -> Result<()>,
) -> Result<()> {
    match marks {
        UnknownMarks::Build(bitmap) => for_each_unmarked_pair(
            bitmap,
            build_rows.collect(),
            probe_rows,
            batch_size,
            |build, probe| f(build.into(), probe.into()),
        ),
        UnknownMarks::Probe(bitmap) => for_each_unmarked_pair(
            bitmap,
            probe_rows.collect(),
            build_rows,
            batch_size,
            |probe, build| f(build.into(), probe.into()),
        ),
    }
}

/// Calls `f` with the pairs of `decided_rows` x `other_rows` whose row in
/// `decided_rows` is not set in `bitmap`, as chunks of at most `batch_size`
/// pairs.
///
/// `f` sets bits in `bitmap`, so the unset rows are found again after each
/// chunk. The pairing stops when every row of `decided_rows` is set.
fn for_each_unmarked_pair<D: Copy + Into<u64>, O: Copy>(
    bitmap: &SharedBitmapBuilder,
    mut decided_rows: Vec<D>,
    other_rows: impl Iterator<Item = O>,
    batch_size: usize,
    mut f: impl FnMut(Vec<D>, Vec<O>) -> Result<()>,
) -> Result<()> {
    let retain_undecided_rows = |rows: &mut Vec<D>| {
        let bitmap = bitmap.lock();
        rows.retain(|idx| !bitmap.get_bit((*idx).into() as usize));
    };

    retain_undecided_rows(&mut decided_rows);

    let mut decided_chunk = Vec::with_capacity(batch_size);
    let mut other_chunk = Vec::with_capacity(batch_size);
    let mut marks_since_refresh = false;
    for other_row in other_rows {
        // Refresh only after a chunk was sent, so the cost of the refresh
        // stays proportional to the pairs already evaluated.
        if marks_since_refresh {
            retain_undecided_rows(&mut decided_rows);
            marks_since_refresh = false;
        }
        if decided_rows.is_empty() {
            break;
        }
        for decided_row in &decided_rows {
            decided_chunk.push(*decided_row);
            other_chunk.push(other_row);
            if decided_chunk.len() == batch_size {
                f(
                    std::mem::replace(&mut decided_chunk, Vec::with_capacity(batch_size)),
                    std::mem::replace(&mut other_chunk, Vec::with_capacity(batch_size)),
                )?;
                marks_since_refresh = true;
            }
        }
    }
    if !decided_chunk.is_empty() {
        f(decided_chunk, other_chunk)?;
    }
    Ok(())
}
