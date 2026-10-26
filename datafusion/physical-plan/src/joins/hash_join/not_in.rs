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

//! `NOT IN` (null-aware) semantics of the hash join.
//!
//! `x NOT IN (subquery)` is `NOT (x IN (subquery))` under SQL three-valued
//! logic: `x IN (..)` is TRUE when some subquery value equals `x`, UNKNOWN
//! (NULL) when none does but a NULL on either side might, and FALSE
//! otherwise. `NOT IN` keeps a row only where `IN` is FALSE.
//!
//! The regular hash join finds the equal pairs; what NULLs change in the
//! rows a `NOT IN` emits is here. The join reaches it through:
//! - [`NotIn::try_new`] when the plan is built or executed: is this join
//!   legal, and which form is it;
//! - [`NotInBuild::try_new`] once per build, from `collect_left_input`;
//! - [`NotInProbe::before_lookup`] and [`NotInProbe::after_lookup`] around
//!   each probe batch's lookup, or [`NotInProbe::after_empty_build`] when
//!   the build map is empty and there is no lookup;
//! - [`NotInBuild::final_chunk`] on each chunk of build rows emitted after
//!   the probe side is exhausted.

use std::sync::Arc;

use parking_lot::Mutex;

use arrow::array::{
    Array, ArrayRef, AsArray, BooleanArray, BooleanBufferBuilder, RecordBatch,
    UInt32Array, UInt64Array,
};
use arrow::buffer::{BooleanBuffer, NullBuffer};
use arrow::compute::{filter, filter_record_batch, take};
use arrow::util::bit_util;
use datafusion_common::hash_utils::{RandomState, create_hashes};
use datafusion_common::{JoinSide, JoinType, NullEquality, Result, plan_err};
use datafusion_execution::memory_pool::MemoryReservation;
use datafusion_expr::{ColumnarValue, Operator};
use datafusion_physical_expr::PhysicalExprRef;
use datafusion_physical_expr_common::datum::apply_cmp;

use crate::joins::PartitionMode;
use crate::joins::SharedBitmapBuilder;
use crate::joins::hash_join::exec::{JoinLeftData, new_join_hashmap};
use crate::joins::join_hash_map::JoinHashMapType;
use crate::joins::join_hash_map::ProbeOffset::AtRow;
use crate::joins::probe_completion::ProbeSideSummary;
use crate::joins::utils::{
    BuildProbeJoinMetrics, JoinFilter, apply_join_filter_to_indices,
    equal_rows_arr_with_normalized_left, matchable_join_keys, update_hash,
};

/// Which `NOT IN` a null-aware join computes, derived from
/// `HashJoinExec::null_aware`, the join type, the keys and the filter.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum NotIn {
    /// `build.key NOT IN (probe.key)`: the final stage gives every build row
    /// its `IN` mark, from the visited bitmap and the NULLs seen.
    BuildRows {
        scope: NotInScope,
        output: NotInOutput,
    },
    /// `probe.key NOT IN (build.key)`, `CollectLeft` (`RightAnti`): emits the
    /// probe rows whose `IN` is FALSE, decided while streaming because every
    /// probe row sees the whole build side.
    ProbeRows { scope: NotInScope },
}

/// Which rows of the other side a NULL can make a row's `IN` UNKNOWN for.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum NotInScope {
    /// One value key, no correlation keys, no filter: a NULL anywhere on the
    /// other side makes every unmatched row UNKNOWN. [`NotIn::BuildRows`]
    /// probe partitions share one summary of what they saw
    /// (`ProbeCompletion`); [`NotIn::ProbeRows`] emits nothing once any build
    /// key is NULL, and never a probe row whose key is NULL.
    Uncorrelated,
    /// Correlation scope keys (`on[value_keys..]`), a multi-column value key
    /// (`on[..value_keys]`), or a filter: only the rows in the same scope
    /// count, so UNKNOWN is decided per row (see
    /// [`mark_null_candidates_for_probe_batch`]).
    Correlated { value_keys: usize },
}

/// What a [`NotIn::BuildRows`] join emits.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum NotInOutput {
    /// `LeftAnti`: the build rows whose mark is FALSE.
    Rows,
    /// `LeftMark`: every build row with its mark, a nullable Boolean column.
    Mark,
}

impl NotIn {
    /// Validates that `null_aware` may be set for this join and returns its
    /// form. The errors are those of the legality table this replaced.
    pub(super) fn try_new(
        join_type: JoinType,
        partition_mode: PartitionMode,
        num_keys: usize,
        num_value_keys: usize,
        has_filter: bool,
    ) -> Result<Self> {
        if num_value_keys == 0 || num_value_keys > num_keys {
            return plan_err!(
                "null_aware {join_type} join needs between 1 and {num_keys} `NOT IN` value keys, got {num_value_keys}"
            );
        }
        // `num_keys > 1` also covers a multi-column value key without any
        // correlation: a NULL in one tuple element leaves the comparison FALSE
        // whenever another element is a definite mismatch, so it too must be
        // decided per row.
        let scope = if num_keys > 1 || has_filter {
            NotInScope::Correlated {
                value_keys: num_value_keys,
            }
        } else {
            NotInScope::Uncorrelated
        };
        let output = match (join_type, partition_mode) {
            (JoinType::LeftAnti, _) => NotInOutput::Rows,
            (JoinType::LeftMark, _) => NotInOutput::Mark,
            // `PartitionMode::CollectLeft` is safe because `RightAnti` is
            // probe-driven: every probe row sees the whole build side.
            (JoinType::RightAnti, PartitionMode::CollectLeft) => {
                return Ok(Self::ProbeRows { scope });
            }
            _ => {
                return plan_err!(
                    "null_aware can only be true for LeftAnti joins and RightAnti joins with `CollectLeft` `PartitionMode`, or LeftMark joins, got {join_type} with {partition_mode}"
                );
            }
        };
        // Build-driven joins decide in the final stage from state shared
        // across the probe partitions: the per-build-row null bitmap for
        // the correlated forms, and the probe-side NULL flag for the
        // uncorrelated ones. `Partitioned` gives each output partition its
        // own build side, so neither is global there.
        if partition_mode == PartitionMode::Partitioned {
            return plan_err!(
                "null_aware joins require PartitionMode::CollectLeft, got PartitionMode::Partitioned"
            );
        }
        Ok(Self::BuildRows { scope, output })
    }

    fn scope(self) -> NotInScope {
        match self {
            Self::BuildRows { scope, .. } | Self::ProbeRows { scope } => scope,
        }
    }

    /// Number of leading keys that are `NOT IN` value keys; the keys after
    /// them are correlation scope keys.
    pub(super) fn value_keys(self) -> usize {
        match self.scope() {
            NotInScope::Correlated { value_keys } => value_keys,
            NotInScope::Uncorrelated => 1,
        }
    }
}

/// What a build side keeps for its `NOT IN`, one variant per form.
pub(super) enum NotInBuild {
    /// Correlated [`NotIn::BuildRows`]: UNKNOWN is recorded per build row.
    BuildRowsCorrelated {
        value_keys: usize,
        output: NotInOutput,
        /// One bit per build row, set once its mark is UNKNOWN; shared by
        /// every probe partition, and never cleared.
        unknown: SharedBitmapBuilder,
        /// Hash table over the scope keys (`on[value_keys..]`) of all build
        /// rows, probed with NULL-valued probe rows. `None` without scope keys.
        scope_map: Option<Box<dyn JoinHashMapType>>,
        /// The build rows with a NULL value key, `None` when there are none.
        null_value_rows: Option<NullValueBuildRows>,
    },
    /// Uncorrelated [`NotIn::BuildRows`]: what every probe partition saw
    /// decides every unmatched mark.
    BuildRowsUncorrelated {
        output: NotInOutput,
        /// Logical NULLs of the value key, `None` when it has none. A NULL
        /// build key's mark is UNKNOWN once the probe side has a row.
        key_nulls: Option<NullBuffer>,
    },
    /// Correlated [`NotIn::ProbeRows`]: UNKNOWN is decided per probe row,
    /// within its probe batch, so nothing is shared across probe partitions.
    /// The fields are those of [`Self::BuildRowsCorrelated`].
    ProbeRowsCorrelated {
        value_keys: usize,
        scope_map: Option<Box<dyn JoinHashMapType>>,
        null_value_rows: Option<NullValueBuildRows>,
    },
    /// Uncorrelated [`NotIn::ProbeRows`].
    ProbeRowsUncorrelated {
        /// Whether a build key is NULL: then no probe row is emitted.
        build_has_null: bool,
    },
}

impl NotInBuild {
    /// Builds the `NOT IN` state of a collected build side and charges it to
    /// `reservation`. `keys` are the build keys as evaluated, `normalized`
    /// with `-0.0` rewritten (see `JoinLeftData::values`).
    #[expect(clippy::too_many_arguments)]
    pub(super) fn try_new(
        not_in: NotIn,
        on_left: &[PhysicalExprRef],
        batch: &RecordBatch,
        keys: &[ArrayRef],
        normalized: &[ArrayRef],
        random_state: &RandomState,
        reservation: &mut MemoryReservation,
        metrics: &BuildProbeJoinMetrics,
    ) -> Result<Self> {
        let num_rows = batch.num_rows();
        let correlated_row_state = |value_keys, reservation: &mut MemoryReservation| {
            correlated_row_state(
                on_left,
                batch,
                keys,
                normalized,
                value_keys,
                random_state,
                reservation,
                metrics,
            )
        };
        match not_in {
            NotIn::ProbeRows {
                scope: NotInScope::Uncorrelated,
            } => Ok(Self::ProbeRowsUncorrelated {
                build_has_null: keys[0].logical_null_count() > 0,
            }),
            NotIn::ProbeRows {
                scope: NotInScope::Correlated { value_keys },
            } => {
                let (scope_map, null_value_rows) =
                    correlated_row_state(value_keys, reservation)?;
                Ok(Self::ProbeRowsCorrelated {
                    value_keys,
                    scope_map,
                    null_value_rows,
                })
            }
            NotIn::BuildRows {
                scope: NotInScope::Uncorrelated,
                output,
            } => {
                let key_nulls = keys[0].logical_nulls().filter(|n| n.null_count() > 0);
                if let Some(nulls) = &key_nulls {
                    // `logical_nulls` reuses an array's physical null buffer
                    // when it can, which the batch reservation already holds.
                    // Charge only a newly materialized one (dictionaries).
                    let reuses_input_buffer = keys[0]
                        .nulls()
                        .is_some_and(|input| input.inner().ptr_eq(nulls.inner()));
                    if !reuses_input_buffer {
                        let size = nulls.buffer().capacity();
                        reservation.try_grow(size)?;
                        metrics.build_mem_used.add(size);
                    }
                }
                Ok(Self::BuildRowsUncorrelated { output, key_nulls })
            }
            NotIn::BuildRows {
                scope: NotInScope::Correlated { value_keys },
                output,
            } => {
                let bitmap_size = bit_util::ceil(num_rows, 8);
                reservation.try_grow(bitmap_size)?;
                metrics.build_mem_used.add(bitmap_size);
                let mut unknown = BooleanBufferBuilder::new(num_rows);
                unknown.append_n(num_rows, false);

                let (scope_map, null_value_rows) =
                    correlated_row_state(value_keys, reservation)?;
                Ok(Self::BuildRowsCorrelated {
                    value_keys,
                    output,
                    unknown: Mutex::new(unknown),
                    scope_map,
                    null_value_rows,
                })
            }
        }
    }

    /// The rows to emit for one chunk of final build rows, once every probe
    /// partition has finished (`summary`), with the mark column for
    /// [`NotInOutput::Mark`].
    ///
    /// A matched build row's `IN` mark is TRUE. An unmatched one's is UNKNOWN
    /// (NULL) or FALSE (the "null bucket" and `hadNull` of Neumann, Leis and
    /// Kemper, "The Complete Story of Joins (in HyPer)", Section 5.6):
    /// - correlated: UNKNOWN where the bitmap recorded it;
    /// - uncorrelated, a probe key was NULL: UNKNOWN;
    /// - uncorrelated, the probe side was empty: FALSE (`x IN (empty)`);
    /// - uncorrelated otherwise: UNKNOWN exactly for a NULL build key.
    pub(super) fn final_chunk(
        &self,
        summary: ProbeSideSummary,
        left_side: UInt64Array,
        right_side: UInt32Array,
    ) -> (UInt64Array, UInt32Array, Option<ArrayRef>) {
        debug_assert!(
            !summary.has_null || summary.non_empty,
            "a probe batch with a NULL key has rows"
        );
        match self {
            Self::ProbeRowsCorrelated { .. } | Self::ProbeRowsUncorrelated { .. } => {
                (left_side, right_side, None)
            }
            Self::BuildRowsCorrelated {
                output, unknown, ..
            } => {
                let unknown = unknown.lock();
                emit(*output, left_side, right_side, |row| unknown.get_bit(row))
            }
            // A probe batch with a NULL key also has rows: `has_null` implies
            // `non_empty`.
            Self::BuildRowsUncorrelated { output, .. } if summary.has_null => {
                emit_uniform(*output, left_side, right_side, true)
            }
            Self::BuildRowsUncorrelated { output, .. } if !summary.non_empty => {
                emit_uniform(*output, left_side, right_side, false)
            }
            Self::BuildRowsUncorrelated { output, key_nulls } => match key_nulls {
                Some(nulls) => {
                    emit(*output, left_side, right_side, |row| nulls.is_null(row))
                }
                None => emit_uniform(*output, left_side, right_side, false),
            },
        }
    }
}

/// Emits a chunk of final build rows given which unmatched rows have an
/// UNKNOWN mark (see [`NotInBuild::final_chunk`]). `LeftAnti` chunks hold
/// only unmatched rows and keep those whose mark is FALSE; `LeftMark` chunks
/// hold every row, a valid probe index marking a matched one.
fn emit(
    output: NotInOutput,
    left_side: UInt64Array,
    right_side: UInt32Array,
    is_unknown: impl Fn(usize) -> bool,
) -> (UInt64Array, UInt32Array, Option<ArrayRef>) {
    match output {
        NotInOutput::Rows => {
            let kept = UInt64Array::from_iter_values(
                left_side
                    .values()
                    .iter()
                    .copied()
                    .filter(|row| !is_unknown(*row as usize)),
            );
            let right_side = UInt32Array::new_null(kept.len());
            (kept, right_side, None)
        }
        NotInOutput::Mark => {
            let marks: BooleanArray = left_side
                .values()
                .iter()
                .zip(right_side.iter())
                .map(|(row, probe)| {
                    if probe.is_some() {
                        Some(true)
                    } else {
                        (!is_unknown(*row as usize)).then_some(false)
                    }
                })
                .collect();
            (left_side, right_side, Some(Arc::new(marks)))
        }
    }
}

/// [`emit`] when every unmatched row's mark is the same: an anti join then
/// keeps all of the chunk or none of it.
fn emit_uniform(
    output: NotInOutput,
    left_side: UInt64Array,
    right_side: UInt32Array,
    unknown: bool,
) -> (UInt64Array, UInt32Array, Option<ArrayRef>) {
    match (output, unknown) {
        (NotInOutput::Rows, true) => {
            (UInt64Array::new_null(0), UInt32Array::new_null(0), None)
        }
        (NotInOutput::Rows, false) => (left_side, right_side, None),
        (NotInOutput::Mark, _) => emit(output, left_side, right_side, |_| unknown),
    }
}

/// One probe stream's `NOT IN` work, with scratch space for the correlated
/// marking pass, reused across probe batches.
#[derive(Debug, Default)]
pub(super) struct NotInProbe {
    hashes: Vec<u64>,
    lookup: LookupBuffers,
    /// For [`NotInBuild::ProbeRowsCorrelated`], the rows of the current probe
    /// batch whose `NOT IN` is UNKNOWN, set by [`Self::before_lookup`] on the
    /// batch's first lookup; they are never emitted. `None` when there is no
    /// such row.
    unknown_probe_rows: Option<BooleanBuffer>,
}

impl NotInProbe {
    /// Before a lookup of a probe batch. `first_chunk` is whether this is the
    /// batch's first lookup: a batch may resume lookup several times, but
    /// what it contributes to `NOT IN` is recorded once.
    ///
    /// - Uncorrelated [`NotIn::ProbeRows`] emits probe rows, so a NULL build
    ///   key skips every batch.
    /// - Uncorrelated [`NotIn::BuildRows`]: record what the batch saw for the
    ///   final stage. The anti join skips once any partition saw a NULL probe
    ///   key: every unmatched mark is then UNKNOWN. This is a best-effort
    ///   early exit, checked on every resumed lookup; the final stage
    ///   re-checks.
    /// - Correlated: mark the rows this batch makes UNKNOWN, build rows for
    ///   [`NotIn::BuildRows`] and this batch's probe rows for
    ///   [`NotIn::ProbeRows`]. Must also run when the build map is empty: an
    ///   all-NULL-key build side still needs its marks.
    ///
    /// Returns whether the batch cannot contribute any output row, so its
    /// lookup is skipped. A no-op returning `false` for other joins.
    #[expect(clippy::too_many_arguments)]
    pub(super) fn before_lookup(
        &mut self,
        left_data: &JoinLeftData,
        probe_batch: &RecordBatch,
        probe_values: &[ArrayRef],
        first_chunk: bool,
        filter: Option<&JoinFilter>,
        random_state: &RandomState,
        batch_size: usize,
    ) -> Result<bool> {
        let Some(not_in) = left_data.not_in() else {
            return Ok(false);
        };
        match not_in {
            NotInBuild::ProbeRowsUncorrelated { build_has_null } => Ok(*build_has_null),
            NotInBuild::BuildRowsUncorrelated { output, .. } => {
                if first_chunk {
                    // `on[0]` is the `NOT IN` value key. Only batches with rows
                    // count: `NULL NOT IN (empty)` is TRUE.
                    let probe_has_null = probe_values[0].logical_null_count() > 0;
                    left_data
                        .record_probe_batch(probe_batch.num_rows() > 0, probe_has_null);
                }
                Ok(*output == NotInOutput::Rows && left_data.probe_side_has_null_hint())
            }
            NotInBuild::BuildRowsCorrelated {
                value_keys,
                output,
                unknown,
                scope_map,
                null_value_rows,
            } => {
                // The join filter needs the join type the output implies.
                let join_type = match output {
                    NotInOutput::Rows => JoinType::LeftAnti,
                    NotInOutput::Mark => JoinType::LeftMark,
                };
                if first_chunk {
                    mark_null_candidates_for_probe_batch(
                        left_data,
                        UnknownMarks::Build(unknown),
                        scope_map.as_deref(),
                        null_value_rows.as_ref(),
                        probe_batch,
                        probe_values,
                        *value_keys,
                        filter,
                        join_type,
                        random_state,
                        batch_size,
                        &mut self.hashes,
                        &mut self.lookup,
                    )?;
                }
                Ok(false)
            }
            NotInBuild::ProbeRowsCorrelated {
                value_keys,
                scope_map,
                null_value_rows,
            } => {
                if !first_chunk {
                    return Ok(false);
                }
                self.unknown_probe_rows = None;
                // Without a NULL-valued row on either side no probe row can be
                // UNKNOWN, so the bitmap is only allocated when one exists.
                let may_be_unknown = null_value_rows.is_some()
                    || probe_values[..*value_keys]
                        .iter()
                        .any(|values| values.logical_null_count() > 0);
                if may_be_unknown {
                    let num_rows = probe_batch.num_rows();
                    let mut unknown = BooleanBufferBuilder::new(num_rows);
                    unknown.append_n(num_rows, false);
                    let unknown = Mutex::new(unknown);
                    mark_null_candidates_for_probe_batch(
                        left_data,
                        UnknownMarks::Probe(&unknown),
                        scope_map.as_deref(),
                        null_value_rows.as_ref(),
                        probe_batch,
                        probe_values,
                        *value_keys,
                        filter,
                        JoinType::RightAnti,
                        random_state,
                        batch_size,
                        &mut self.hashes,
                        &mut self.lookup,
                    )?;
                    let unknown = unknown.into_inner().finish();
                    self.unknown_probe_rows =
                        (unknown.count_set_bits() > 0).then_some(unknown);
                }
                Ok(false)
            }
        }
    }

    /// After a lookup, drops the probe rows a [`NotIn::ProbeRows`] join must
    /// not emit: when uncorrelated, those whose key is NULL, since
    /// `NULL NOT IN (..)` is never TRUE; when correlated, those that
    /// [`Self::before_lookup`] marked UNKNOWN. `valid_keys` is the probe keys'
    /// validity (`matchable_join_keys`), when the batch has one.
    pub(super) fn after_lookup(
        &self,
        not_in: &NotInBuild,
        probe_values: &[ArrayRef],
        valid_keys: Option<&NullBuffer>,
        right_indices: UInt32Array,
    ) -> UInt32Array {
        match not_in {
            NotInBuild::ProbeRowsUncorrelated { .. } => {
                let mask = valid_keys
                    .cloned()
                    .or_else(|| probe_values[0].logical_nulls());
                match mask.filter(|m| m.null_count() > 0) {
                    Some(mask) => {
                        retain_probe_rows(&right_indices, |idx| !mask.is_null(idx))
                    }
                    None => right_indices,
                }
            }
            NotInBuild::ProbeRowsCorrelated { .. } => match &self.unknown_probe_rows {
                Some(unknown) => {
                    retain_probe_rows(&right_indices, |idx| !unknown.value(idx))
                }
                None => right_indices,
            },
            NotInBuild::BuildRowsCorrelated { .. }
            | NotInBuild::BuildRowsUncorrelated { .. } => right_indices,
        }
    }

    /// With an empty build map the lookup is skipped and `output` holds the
    /// probe batch as the join type emits it. Drops the probe rows that
    /// [`Self::before_lookup`] marked UNKNOWN, as [`Self::after_lookup`] does.
    pub(super) fn after_empty_build(&self, output: RecordBatch) -> Result<RecordBatch> {
        match &self.unknown_probe_rows {
            Some(unknown) => Ok(filter_record_batch(
                &output,
                &BooleanArray::new(!unknown, None),
            )?),
            None => Ok(output),
        }
    }
}

/// Keeps the probe rows of `right_indices` for which `keep` returns true.
fn retain_probe_rows(
    right_indices: &UInt32Array,
    keep: impl Fn(usize) -> bool,
) -> UInt32Array {
    UInt32Array::from_iter_values(
        right_indices
            .values()
            .iter()
            .copied()
            .filter(|idx| keep(*idx as usize)),
    )
}

/// The scope map and NULL-valued build rows of a correlated `NOT IN`.
#[expect(clippy::too_many_arguments, clippy::type_complexity)]
fn correlated_row_state(
    on_left: &[PhysicalExprRef],
    batch: &RecordBatch,
    keys: &[ArrayRef],
    normalized: &[ArrayRef],
    value_keys: usize,
    random_state: &RandomState,
    reservation: &mut MemoryReservation,
    metrics: &BuildProbeJoinMetrics,
) -> Result<(Option<Box<dyn JoinHashMapType>>, Option<NullValueBuildRows>)> {
    let num_rows = batch.num_rows();
    // `on_left[..V]` are the value keys and `on_left[V..]` the (possibly
    // empty) correlation scope keys.
    let scope_keys = &on_left[value_keys..];
    let scope_map = if scope_keys.is_empty() {
        None
    } else {
        // Scope-only NULL marking uses a HashMap (the primary join map may
        // use ArrayMap for full-key matches, but scope keys have arbitrary
        // shape).
        let mut scope_map = new_join_hashmap(num_rows, reservation, metrics)?;
        let mut hashes_buffer = vec![0; num_rows];
        update_hash(
            scope_keys,
            batch,
            &mut *scope_map,
            0,
            random_state,
            &mut hashes_buffer,
            0,
            true,
            NullEquality::NullEqualsNothing,
        )?;
        Some(scope_map)
    };

    let Some(null_mask) = null_value_key_mask(&keys[..value_keys]) else {
        return Ok((scope_map, None));
    };
    let build_indices =
        UInt64Array::from_iter_values(null_mask.values().set_indices().map(|i| i as u64));
    // `build_indices` is retained for the lifetime of the join, so charge it
    // like the maps and bitmaps.
    let retained_size = build_indices.get_array_memory_size();
    reservation.try_grow(retained_size)?;
    metrics.build_mem_used.add(retained_size);

    let null_scope_map = if scope_keys.is_empty() {
        None
    } else {
        let scope_values = normalized[value_keys..]
            .iter()
            .map(|values| Ok(filter(values.as_ref(), &null_mask)?))
            .collect::<Result<Vec<_>>>()?;
        let null_rows = build_indices.len();
        let mut map = new_join_hashmap(null_rows, reservation, metrics)?;
        let mut hashes_buffer = vec![0; null_rows];
        create_hashes(&scope_values, random_state, &mut hashes_buffer)?;
        map.update_from_iter(Box::new(hashes_buffer.iter().enumerate().rev()), 0);
        Some(map)
    };
    Ok((
        scope_map,
        Some(NullValueBuildRows {
            scope_map: null_scope_map,
            build_indices,
        }),
    ))
}

/// The build rows with a NULL `NOT IN` value key (in any element of a
/// multi-column key), used by correlated `NOT IN` (see [`NotInScope::Correlated`]).
///
/// Such rows make `NOT IN` UNKNOWN whenever *any* probe row in their
/// correlation scope passes the join filter (unless another element is a
/// definite mismatch), so every probe row must be tested against them.
/// Restricting this lookup to the NULL-valued build rows keeps it
/// proportional to the number of NULLs instead of enumerating every scope
/// match of every probe row.
pub(super) struct NullValueBuildRows {
    /// Hash table keyed by the correlation scope values of the NULL-valued
    /// build rows. Stored positions index into `build_indices`, not the full
    /// build batch. `None` when the join has no correlation scope keys, so
    /// every probe row is in scope.
    scope_map: Option<Box<dyn JoinHashMapType>>,
    /// Maps positions in `scope_map` back to row indices in the full build
    /// batch.
    build_indices: UInt64Array,
}

/// Returns the rows where at least one of `value_keys` is NULL, or `None` when
/// there is no such row.
fn null_value_key_mask(value_keys: &[ArrayRef]) -> Option<BooleanArray> {
    let logical_nulls: Vec<_> = value_keys.iter().map(|v| v.logical_nulls()).collect();
    let valid = NullBuffer::union_many(logical_nulls.iter().map(Option::as_ref))?;
    (valid.null_count() > 0).then(|| BooleanArray::new(!valid.inner(), None))
}

/// Where a correlated null-aware join records the rows whose `NOT IN` is
/// UNKNOWN, see [`mark_null_candidates_for_probe_batch`].
#[derive(Clone, Copy)]
enum UnknownMarks<'a> {
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
/// empty) correlation scope keys (see [`NotInScope::Correlated`]). A row is
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
/// that can be decided is UNKNOWN. See [`UnmarkedPairs`].
#[expect(clippy::too_many_arguments)]
fn mark_null_candidates_for_probe_batch(
    left_data: &JoinLeftData,
    marks: UnknownMarks<'_>,
    scope_map: Option<&dyn JoinHashMapType>,
    null_value_build_rows: Option<&NullValueBuildRows>,
    probe_batch: &RecordBatch,
    probe_values: &[ArrayRef],
    num_value_keys: usize,
    filter: Option<&JoinFilter>,
    join_type: JoinType,
    random_state: &RandomState,
    batch_size: usize,
    hashes_buffer: &mut Vec<u64>,
    lookup: &mut LookupBuffers,
) -> Result<()> {
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
    let mark = |build_indices: UInt64Array, probe_indices: UInt32Array| {
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

    // The scope maps pair rows by the hash of their scope keys; keep only the
    // pairs whose scope keys are equal.
    let mut key_comparator = None;
    let mut mark_scope_matches =
        |build_indices: UInt64Array, probe_indices: UInt32Array| {
            let (build_indices, probe_indices) = equal_rows_arr_with_normalized_left(
                &build_indices,
                &probe_indices,
                build_scope_values,
                probe_scope_values,
                NullEquality::NullEqualsNothing,
                &mut key_comparator,
            )?;
            mark(build_indices, probe_indices)
        };

    // With scope keys, the probe rows whose scope keys hold a NULL are in no
    // scope. The others are looked up by the hash of their scope keys.
    let probe_rows_in_scope = if probe_scope_values.is_empty() {
        None
    } else {
        hashes_buffer.clear();
        hashes_buffer.resize(probe_batch.num_rows(), 0);
        create_hashes(probe_scope_values, random_state, hashes_buffer)?;
        matchable_join_keys(probe_scope_values, NullEquality::NullEqualsNothing)
    };
    let in_scope = |probe_row: &u32| match &probe_rows_in_scope {
        Some(valid) => valid.is_valid(*probe_row as usize),
        None => true,
    };

    // Case 1: NULL-valued build rows paired with the probe rows in their
    // correlation scope.
    if let Some(null_rows) = null_value_build_rows {
        let probe_rows = (0..probe_batch.num_rows() as u32).filter(in_scope);
        match &null_rows.scope_map {
            Some(scope_map) => {
                let mut pairs =
                    UnmarkedPairs::new(marks, batch_size, &mut mark_scope_matches);
                // The map indexes only the NULL-valued build rows; translate
                // its positions back to build row indices.
                pairs.scope_matches(
                    scope_map.as_ref(),
                    |position| null_rows.build_indices.value(position as usize),
                    probe_rows,
                    hashes_buffer,
                    lookup,
                )?;
                pairs.finish()?;
            }
            None => {
                let mut pairs = UnmarkedPairs::new(marks, batch_size, &mark);
                pairs.cross_product(
                    null_rows.build_indices.values().iter().copied(),
                    probe_rows,
                )?;
                pairs.finish()?;
            }
        }
    }

    // Case 2: NULL-valued probe rows paired with the build rows in their
    // correlation scope.
    if let Some(null_mask) = probe_null_value_mask {
        let null_probe_rows = null_mask
            .values()
            .set_indices()
            .map(|i| i as u32)
            .filter(in_scope);

        match scope_map {
            Some(scope_map) => {
                let mut pairs =
                    UnmarkedPairs::new(marks, batch_size, &mut mark_scope_matches);
                pairs.scope_matches(
                    scope_map,
                    |build_row| build_row,
                    null_probe_rows,
                    hashes_buffer,
                    lookup,
                )?;
                pairs.finish()?;
            }
            None => {
                let mut pairs = UnmarkedPairs::new(marks, batch_size, &mark);
                pairs.cross_product(
                    0..left_data.batch().num_rows() as u64,
                    null_probe_rows,
                )?;
                pairs.finish()?;
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

/// Reused buffers for the matches of a scope map lookup.
#[derive(Debug, Default)]
pub(super) struct LookupBuffers {
    probe_indices: Vec<u32>,
    build_indices: Vec<u64>,
}

/// Pairs build rows with probe rows for
/// [`mark_null_candidates_for_probe_batch`], and calls `f` with the pairs, as
/// chunks of at most `batch_size` pairs.
///
/// `f` marks rows (see [`UnknownMarks`]), and a marked row cannot become
/// unmarked. So a cross product drops the marked rows again after each chunk.
struct UnmarkedPairs<'a, F> {
    marks: UnknownMarks<'a>,
    batch_size: usize,
    build_chunk: Vec<u64>,
    probe_chunk: Vec<u32>,
    /// Whether the rows that the pairs decide may hold rows marked since they
    /// were last refreshed.
    needs_refresh: bool,
    /// All the matches that the scope map lookups returned.
    #[cfg(test)]
    looked_up_matches: usize,
    f: F,
}

impl<'a, F> UnmarkedPairs<'a, F>
where
    F: FnMut(UInt64Array, UInt32Array) -> Result<()>,
{
    fn new(marks: UnknownMarks<'a>, batch_size: usize, f: F) -> Self {
        Self {
            marks,
            batch_size,
            build_chunk: Vec::with_capacity(batch_size),
            probe_chunk: Vec::with_capacity(batch_size),
            needs_refresh: false,
            #[cfg(test)]
            looked_up_matches: 0,
            f,
        }
    }

    /// Pairs each of `probe_rows` with each of `build_rows`. The pairing stops
    /// when every row that the pairs decide is marked.
    fn cross_product(
        &mut self,
        build_rows: impl Iterator<Item = u64>,
        probe_rows: impl Iterator<Item = u32>,
    ) -> Result<()> {
        // Drop the rows that are already marked before the first pair.
        self.needs_refresh = true;
        match self.marks {
            UnknownMarks::Build(_) => {
                self.pair_unmarked(&mut build_rows.collect(), probe_rows, |b, p| (b, p))
            }
            UnknownMarks::Probe(_) => {
                self.pair_unmarked(&mut probe_rows.collect(), build_rows, |p, b| (b, p))
            }
        }
    }

    /// Pairs each of `probe_rows` with the build rows that share the hash of
    /// its correlation scope keys in `scope_map`. `probe_hashes` holds the
    /// hashes of all rows of the probe batch. `to_build_row` translates the
    /// positions stored in `scope_map` to build row indices. Rows with equal
    /// hashes can have different scope keys, so `f` must still check that the
    /// scope keys are equal.
    ///
    fn scope_matches(
        &mut self,
        scope_map: &dyn JoinHashMapType,
        to_build_row: impl Fn(u64) -> u64,
        probe_rows: impl Iterator<Item = u32>,
        probe_hashes: &[u64],
        lookup: &mut LookupBuffers,
    ) -> Result<()> {
        let probe_rows: Vec<u32> = probe_rows.collect();
        let hashes: Vec<u64> = probe_rows
            .iter()
            .map(|row| probe_hashes[*row as usize])
            .collect();
        let mut offset = AtRow { row: 0 };
        loop {
            let next_offset = scope_map.get_matched_indices_with_limit_offset(
                &hashes,
                None,
                self.batch_size,
                offset,
                &mut lookup.probe_indices,
                &mut lookup.build_indices,
            );
            #[cfg(test)]
            {
                self.looked_up_matches += lookup.build_indices.len();
            }
            for i in 0..lookup.build_indices.len() {
                self.push(
                    to_build_row(lookup.build_indices[i]),
                    probe_rows[lookup.probe_indices[i] as usize],
                )?;
            }
            let Some(next_offset) = next_offset else {
                return Ok(());
            };
            offset = next_offset;
        }
    }

    /// Sends the pairs that are left.
    fn finish(mut self) -> Result<()> {
        self.send()
    }

    /// Pairs each of `decided_rows` with each of `other_rows`. `to_pair`
    /// orders a pair as (build row, probe row), and `decided_rows` are the
    /// rows that the pairs decide (see [`UnknownMarks::unknown_row`]).
    /// `decided_rows` must hold no row that was marked before the last chunk
    /// was sent, unless `needs_refresh` is set.
    fn pair_unmarked<D: Copy + Into<u64>, O: Copy>(
        &mut self,
        decided_rows: &mut Vec<D>,
        other_rows: impl Iterator<Item = O>,
        to_pair: impl Fn(D, O) -> (u64, u32),
    ) -> Result<()> {
        for other_row in other_rows {
            // Refresh only after a chunk was sent, so the cost of the refresh
            // stays proportional to the pairs already evaluated.
            if self.needs_refresh {
                let bitmap = self.marks.bitmap().lock();
                decided_rows.retain(|row| !bitmap.get_bit((*row).into() as usize));
                self.needs_refresh = false;
            }
            if decided_rows.is_empty() {
                break;
            }
            for decided_row in decided_rows.iter() {
                let (build_row, probe_row) = to_pair(*decided_row, other_row);
                self.push(build_row, probe_row)?;
            }
        }
        Ok(())
    }

    fn push(&mut self, build_row: u64, probe_row: u32) -> Result<()> {
        self.build_chunk.push(build_row);
        self.probe_chunk.push(probe_row);
        if self.build_chunk.len() == self.batch_size {
            self.send()?;
        }
        Ok(())
    }

    fn send(&mut self) -> Result<()> {
        if self.build_chunk.is_empty() {
            return Ok(());
        }
        let build_chunk =
            std::mem::replace(&mut self.build_chunk, Vec::with_capacity(self.batch_size));
        let probe_chunk =
            std::mem::replace(&mut self.probe_chunk, Vec::with_capacity(self.batch_size));
        (self.f)(build_chunk.into(), probe_chunk.into())?;
        self.needs_refresh = true;
        Ok(())
    }
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::joins::join_hash_map::JoinHashMapU64;
    use rand::rngs::StdRng;
    use rand::{Rng, SeedableRng};

    /// The input of one run of [`UnmarkedPairs`]. Each row has a scope key,
    /// and the hash of a key is `key % hash_modulus`, so different scopes can
    /// share a hash.
    #[derive(Debug)]
    struct Case {
        mark_build_rows: bool,
        /// Whether to pair through a scope map, or as a cross product.
        through_scope_map: bool,
        batch_size: usize,
        hash_modulus: u64,
        /// The scope key of each build row.
        build_keys: Vec<u64>,
        /// The scope key of each probe row.
        probe_keys: Vec<u64>,
        /// The probe rows to pair, a subset of all the probe rows.
        probe_rows: Vec<u32>,
        /// The build rows to pair, a subset of all the build rows. The scope
        /// map stores positions in this list, as for the NULL-valued build
        /// rows.
        build_rows: Vec<u64>,
        /// The rows marked before the pairing starts, as by earlier probe
        /// batches.
        marked_before: Vec<bool>,
        /// Seed of the pairs that pass the join filter.
        filter_seed: u64,
        /// One in this many pairs passes the join filter. `u64::MAX` passes
        /// none.
        filter_one_in: u64,
        /// Whether each chunk of pairs also marks a random build row, as
        /// another probe partition can while this one pairs.
        foreign_marks: bool,
    }

    impl Case {
        fn random(rng: &mut StdRng) -> Self {
            let mark_build_rows = rng.random_bool(0.5);
            let num_build = rng.random_range(0..80);
            let num_probe = rng.random_range(0..300);
            // Few keys give large scopes, which make the pairing switch to
            // groups.
            let num_keys = rng.random_range(1..12);
            let build_keys: Vec<u64> = (0..num_build)
                .map(|_| rng.random_range(0..num_keys))
                .collect();
            let probe_keys: Vec<u64> = (0..num_probe)
                .map(|_| rng.random_range(0..num_keys))
                .collect();
            let probe_rows = (0..num_probe as u32)
                .filter(|_| rng.random_bool(0.9))
                .collect();
            let build_rows = (0..num_build as u64)
                .filter(|_| rng.random_bool(0.8))
                .collect();
            let num_marked = if mark_build_rows {
                num_build
            } else {
                num_probe
            };
            let marked_before = (0..num_marked).map(|_| rng.random_bool(0.1)).collect();
            Self {
                mark_build_rows,
                through_scope_map: rng.random_bool(0.8),
                batch_size: [1, 2, 3, 7, 64, 8192][rng.random_range(0..6)],
                hash_modulus: rng.random_range(1..=num_keys),
                build_keys,
                probe_keys,
                probe_rows,
                build_rows,
                marked_before,
                filter_seed: rng.random(),
                filter_one_in: [1, 2, 5, 50, u64::MAX][rng.random_range(0..5)],
                foreign_marks: mark_build_rows && rng.random_bool(0.3),
            }
        }

        fn passes_filter(&self, build_row: u64, probe_row: u32) -> bool {
            let mixed = (build_row ^ self.filter_seed)
                .wrapping_mul(0x9E37_79B9_7F4A_7C15)
                .wrapping_add(probe_row as u64)
                .wrapping_mul(0xC2B2_AE3D_27D4_EB4F);
            self.filter_one_in != u64::MAX
                && (mixed >> 7).is_multiple_of(self.filter_one_in)
        }

        fn decided_row(&self, build_row: u64, probe_row: u32) -> usize {
            if self.mark_build_rows {
                build_row as usize
            } else {
                probe_row as usize
            }
        }

        /// The marks after every pair of the same scope that passes the
        /// filter has marked its row.
        fn expected_marks(&self) -> Vec<bool> {
            let mut marks = self.marked_before.clone();
            for build_row in &self.build_rows {
                for probe_row in &self.probe_rows {
                    let same_scope = self.build_keys[*build_row as usize]
                        == self.probe_keys[*probe_row as usize];
                    let paired = !self.through_scope_map || same_scope;
                    if paired && self.passes_filter(*build_row, *probe_row) {
                        marks[self.decided_row(*build_row, *probe_row)] = true;
                    }
                }
            }
            marks
        }

        /// Runs [`UnmarkedPairs`] like [`mark_null_candidates_for_probe_batch`]
        /// does, and returns the marks and the pairing.
        fn run(&self) -> Result<(Vec<bool>, Stats)> {
            let mut bitmap = BooleanBufferBuilder::new(self.marked_before.len());
            for marked in &self.marked_before {
                bitmap.append(*marked);
            }
            let bitmap = Mutex::new(bitmap);
            let marks = if self.mark_build_rows {
                UnknownMarks::Build(&bitmap)
            } else {
                UnknownMarks::Probe(&bitmap)
            };
            let hash = |key: u64| key % self.hash_modulus;

            let mut foreign_marks = vec![];
            let mark = |build: UInt64Array, probe: UInt32Array| {
                assert!(build.len() <= self.batch_size);
                if self.foreign_marks {
                    let row = (self.filter_seed as usize)
                        .wrapping_add(foreign_marks.len().wrapping_mul(0x9E37_79B9))
                        % self.marked_before.len();
                    bitmap.lock().set_bit(row, true);
                    foreign_marks.push(row);
                }
                // Like `mark_scope_matches` and `mark`: drop the pairs of
                // different scopes, then apply the filter.
                let mut bitmap = bitmap.lock();
                for (build_row, probe_row) in build.values().iter().zip(probe.values()) {
                    let build_key = self.build_keys[*build_row as usize];
                    let probe_key = self.probe_keys[*probe_row as usize];
                    if self.through_scope_map {
                        assert_eq!(hash(build_key), hash(probe_key));
                        if build_key != probe_key {
                            continue;
                        }
                    }
                    if self.passes_filter(*build_row, *probe_row) {
                        bitmap.set_bit(self.decided_row(*build_row, *probe_row), true);
                    }
                }
                Ok(())
            };

            let mut pairs = UnmarkedPairs::new(marks, self.batch_size, mark);
            if self.through_scope_map {
                let mut scope_map = JoinHashMapU64::with_capacity(self.build_rows.len());
                let build_hashes: Vec<u64> = self
                    .build_rows
                    .iter()
                    .map(|row| hash(self.build_keys[*row as usize]))
                    .collect();
                scope_map
                    .update_from_iter(Box::new(build_hashes.iter().enumerate().rev()), 0);
                let probe_hashes: Vec<u64> =
                    self.probe_keys.iter().map(|key| hash(*key)).collect();
                pairs.scope_matches(
                    &scope_map,
                    |position| self.build_rows[position as usize],
                    self.probe_rows.iter().copied(),
                    &probe_hashes,
                    &mut LookupBuffers::default(),
                )?;
            } else {
                pairs.cross_product(
                    self.build_rows.iter().copied(),
                    self.probe_rows.iter().copied(),
                )?;
            }
            let looked_up_matches = pairs.looked_up_matches;
            pairs.finish()?;
            let bitmap = bitmap.into_inner().finish();
            let marks = (0..bitmap.len()).map(|row| bitmap.value(row)).collect();
            Ok((
                marks,
                Stats {
                    looked_up_matches,
                    foreign_marks,
                },
            ))
        }
    }

    #[derive(Debug)]
    struct Stats {
        looked_up_matches: usize,
        /// The rows that `foreign_marks` marked.
        foreign_marks: Vec<usize>,
    }

    /// Compares the marks of [`UnmarkedPairs`] with those of every pair, on
    /// random inputs: hash collisions between scopes, rows marked before or
    /// meanwhile by other partitions, filters that pass none to all of the
    /// pairs, marks on build or probe rows, and lookups that switch to groups
    /// or not.
    #[test]
    fn unmarked_pairs_match_brute_force() -> Result<()> {
        let mut rng = StdRng::seed_from_u64(0x5C0BE);
        for i in 0..3000 {
            let case = Case::random(&mut rng);
            let (marks, stats) = case.run()?;
            let mut expected = case.expected_marks();
            for row in &stats.foreign_marks {
                expected[*row] = true;
            }
            assert_eq!(marks, expected, "case {i}: {case:?}");
        }
        Ok(())
    }

    /// One scope holds every row, and every pair passes. Returns the matches
    /// that the scope map lookups walked.
    fn walked_matches_in_one_scope(
        mark_build_rows: bool,
        num_build: usize,
        num_probe: usize,
        batch_size: usize,
    ) -> Result<usize> {
        let num_marked = if mark_build_rows {
            num_build
        } else {
            num_probe
        };
        let case = Case {
            mark_build_rows,
            through_scope_map: true,
            batch_size,
            hash_modulus: 1,
            build_keys: vec![0; num_build],
            probe_keys: vec![0; num_probe],
            probe_rows: (0..num_probe as u32).collect(),
            build_rows: (0..num_build as u64).collect(),
            marked_before: vec![false; num_marked],
            filter_seed: 0,
            filter_one_in: 1,
            foreign_marks: false,
        };
        let (marks, stats) = case.run()?;
        assert!(marks.iter().all(|marked| *marked));
        Ok(stats.looked_up_matches)
    }

    /// With marks on the probe rows, a scope with far more build rows than
    /// probe rows. A lookup per probe row walks the whole hash chain for
    /// every probe row, even after the probe row is marked.
    #[test]
    fn marked_probe_rows_walk_whole_chains() -> Result<()> {
        for batch_size in [16, 8192] {
            let (num_build, num_probe) = (50_000, 500);
            let walked =
                walked_matches_in_one_scope(false, num_build, num_probe, batch_size)?;
            assert_eq!(walked, num_build * num_probe, "batch_size {batch_size}");
        }
        Ok(())
    }

    /// One scope with many rows, where every pair passes. A lookup per probe
    /// row walks every build row of the scope for every probe row, even after
    /// the rows are marked. So the scope map lookups walk one match per pair
    /// of rows.
    #[test]
    fn scope_lookups_walk_every_pair() -> Result<()> {
        for mark_build_rows in [true, false] {
            for batch_size in [1, 16, 8192] {
                let (num_build, num_probe) = (2_000, 2_000);
                let walked = walked_matches_in_one_scope(
                    mark_build_rows,
                    num_build,
                    num_probe,
                    batch_size,
                )?;
                assert_eq!(
                    walked,
                    num_build * num_probe,
                    "marks on build rows: {mark_build_rows}, batch_size {batch_size}"
                );
            }
        }
        Ok(())
    }
}
