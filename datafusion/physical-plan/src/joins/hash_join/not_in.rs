// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: See the NOTICE file distributed with this work for additional information regarding copyright ownership
// SPDX-FileContributor: Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements

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
use crate::joins::join_hash_map::ProbeOffset::{AtRow, MidChain};
use crate::joins::join_hash_map::{JoinHashMapType, ProbeOffset};
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
/// A row stays UNKNOWN once it is marked. So the pairing drops most pairs that
/// decide a row already known to be UNKNOWN before the join filter is
/// evaluated. Without scope keys this also ends the pairing as soon as every
/// row that can be decided is UNKNOWN. See [`UnmarkedPairs`], and
/// [`UnmarkedPairs::lookup_unmarked`] for the checks it skips.
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
/// unmarked. So the pairing drops most pairs that decide a marked row before
/// it sends them, and drops the marked rows again after each chunk. Some
/// pairs of a marked row still reach `f` (see [`Self::lookup_unmarked`]).
/// Such a pair only sets a mark that is already set.
struct UnmarkedPairs<'a, F> {
    marks: UnknownMarks<'a>,
    batch_size: usize,
    build_chunk: Vec<u64>,
    probe_chunk: Vec<u32>,
    /// Whether the rows that the pairs decide may hold rows marked since they
    /// were last refreshed.
    needs_refresh: bool,
    /// The lookups to skip the check of the build row marks for, after the
    /// last check (see [`Self::lookup_unmarked`]).
    unchecked_lookups: usize,
    /// The lookups left before the next check of the build row marks.
    lookups_until_check: usize,
    /// The matches of the current [`Self::scope_matches`] that a lookup per
    /// probe row pays for without need (see there).
    wasted_matches: usize,
    /// All the matches that the scope map lookups returned.
    #[cfg(test)]
    looked_up_matches: usize,
    /// Whether a [`Self::scope_matches`] wasted enough matches to consider
    /// groups.
    #[cfg(test)]
    considered_groups: bool,
    /// Whether a [`Self::scope_matches`] switched to groups.
    #[cfg(test)]
    grouped: bool,
    /// The lookups with marks on the build rows.
    #[cfg(test)]
    build_lookups: usize,
    /// The lookups that checked the build row marks.
    #[cfg(test)]
    mark_checks: usize,
    f: F,
}

/// A sort costs about as much as this many wasted matches per probe row (see
/// [`UnmarkedPairs::scope_matches`]).
const WASTED_MATCHES_PER_SORTED_ROW: usize = 8;

/// The most lookups that skip the check of the build row marks in a row (see
/// [`UnmarkedPairs::lookup_unmarked`]).
const MAX_UNCHECKED_LOOKUPS: usize = 32;

/// The probe rows sampled to estimate the groups of equal hashes before a
/// sort (see [`UnmarkedPairs::scope_matches`]).
const GROUP_SAMPLE_ROWS: usize = 256;

/// Estimates how many different values `hashes` holds, from a sample of at
/// most [`GROUP_SAMPLE_ROWS`] of them. The count is exact when `hashes` fits
/// in the sample.
fn estimate_groups(hashes: &[u64]) -> usize {
    let mut sample: Vec<u64> = if hashes.len() <= GROUP_SAMPLE_ROWS {
        hashes.to_vec()
    } else {
        // Pseudo-random positions, so that the sample sees a hash twice about
        // as often as a random sample would. A fixed stride follows the
        // period of keys such as `value % n`.
        (0..GROUP_SAMPLE_ROWS as u64)
            .map(|i| hashes[(splitmix64(i) % hashes.len() as u64) as usize])
            .collect()
    };
    sample.sort_unstable();
    let mut seen = 0;
    let mut seen_once: usize = 0;
    let mut seen_twice = 0;
    for run in sample.chunk_by(|a, b| a == b) {
        seen += 1;
        match run.len() {
            1 => seen_once += 1,
            2 => seen_twice += 1,
            _ => {}
        }
    }
    if hashes.len() <= GROUP_SAMPLE_ROWS {
        return seen;
    }
    // The bias-corrected Chao1 estimate: the hashes that the sample saw once
    // suggest how many it did not see.
    let unseen = seen_once * seen_once.saturating_sub(1) / (2 * (seen_twice + 1));
    (seen + unseen).min(hashes.len())
}

/// The SplitMix64 mix of `x`, a fixed pseudo-random sequence.
fn splitmix64(x: u64) -> u64 {
    let x = x.wrapping_add(0x9E37_79B9_7F4A_7C15);
    let x = (x ^ (x >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
    let x = (x ^ (x >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
    x ^ (x >> 31)
}

/// Whether `rows` probe rows in `groups` groups of equal hashes save work when
/// each group is paired at once (see [`UnmarkedPairs::scope_matches`]).
/// `matches_per_row` estimates the build rows of a hash.
fn groups_save_work(
    mark_build_rows: bool,
    rows: usize,
    groups: usize,
    matches_per_row: usize,
    batch_size: usize,
) -> bool {
    if mark_build_rows {
        rows >= 2 * groups
    } else {
        rows * matches_per_row > 2 * batch_size * groups
    }
}

/// The lookups to skip the check of the build row marks for, after a check
/// that followed `unchecked` skipped lookups, and that dropped a match or not
/// (see [`UnmarkedPairs::lookup_unmarked`]).
fn next_unchecked_lookups(unchecked: usize, dropped: bool) -> usize {
    if dropped {
        0
    } else {
        (2 * unchecked).clamp(1, MAX_UNCHECKED_LOOKUPS)
    }
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
            unchecked_lookups: 0,
            lookups_until_check: 0,
            wasted_matches: 0,
            #[cfg(test)]
            looked_up_matches: 0,
            #[cfg(test)]
            considered_groups: false,
            #[cfg(test)]
            grouped: false,
            #[cfg(test)]
            build_lookups: 0,
            #[cfg(test)]
            mark_checks: 0,
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
    /// The pairing starts with a lookup per probe row, like the join itself.
    /// But each probe row finds all the build rows in its scope:
    /// - When the marks are on the build rows, a marked build row costs a
    ///   little for every later probe row in its scope.
    /// - When the marks are on the probe rows, a probe row is paired with all
    ///   the build rows in its scope, even when its first pair marks it.
    ///
    /// When those costs add up to more than a sort of the probe rows, the
    /// join considers groups of the rest of the probe rows with equal hashes.
    /// A sample of the hashes estimates the groups first. If the groups can
    /// save work, the join sorts the rest of the probe rows by hash. Each hash
    /// is then looked up once, and its group is paired like a cross product,
    /// which drops the rows as they are marked. A group saves work when:
    /// - with marks on the build rows, it holds more than one probe row, which
    ///   then share one walk of the hash chain;
    /// - with marks on the probe rows, its pairs fill more than a chunk.
    ///   Within a chunk every pair is sent, so only the later chunks can skip
    ///   the marked probe rows.
    ///
    /// Otherwise the lookup per probe row goes on.
    fn scope_matches(
        &mut self,
        scope_map: &dyn JoinHashMapType,
        to_build_row: impl Fn(u64) -> u64,
        probe_rows: impl Iterator<Item = u32>,
        probe_hashes: &[u64],
        lookup: &mut LookupBuffers,
    ) -> Result<()> {
        let probe_rows: Vec<u32> = probe_rows.collect();
        // `probe_rows` is increasing, so it holds every row exactly when it is
        // as long as the batch. Then the positions are the probe rows.
        let all_rows = probe_rows.len() == probe_hashes.len();
        let gathered: Vec<u64>;
        let hashes = if all_rows {
            probe_hashes
        } else {
            gathered = probe_rows
                .iter()
                .map(|row| probe_hashes[*row as usize])
                .collect();
            &gathered
        };

        self.wasted_matches = 0;
        let max_wasted_matches = WASTED_MATCHES_PER_SORTED_ROW * probe_rows.len();
        let Some((offset, matches)) = self.lookup_each_row(
            scope_map,
            hashes,
            (!all_rows).then_some(probe_rows.as_slice()),
            AtRow { row: 0 },
            &to_build_row,
            max_wasted_matches,
            lookup,
        )?
        else {
            return Ok(());
        };
        // Group the probe rows from the one being looked up. Its build rows
        // that were already paired are paired again, which changes no mark.
        let group_from = match offset {
            AtRow { row } | MidChain { row, .. } => row,
        };

        #[cfg(test)]
        {
            self.considered_groups = true;
        }
        let mark_build_rows = matches!(self.marks, UnknownMarks::Build(_));
        // The matches per probe row so far estimate the build rows of a hash.
        let matches_per_row = matches / group_from.max(1);
        let saves_work = |rows: usize, groups: usize| {
            groups_save_work(
                mark_build_rows,
                rows,
                groups,
                matches_per_row,
                self.batch_size,
            )
        };
        let rest_hashes = &hashes[group_from..];
        // Estimate the groups from a sample first, because a sort of all the
        // rest that finds no group worth it costs as much as the wasted
        // matches did.
        let worth_sorting = saves_work(rest_hashes.len(), estimate_groups(rest_hashes));
        let mut rest: Vec<(u64, u32)> = vec![];
        let mut groups = vec![];
        if worth_sorting {
            rest = rest_hashes
                .iter()
                .copied()
                .zip(probe_rows[group_from..].iter().copied())
                .collect();
            rest.sort_unstable_by_key(|(hash, _)| *hash);
            groups = rest.chunk_by(|a, b| a.0 == b.0).collect();
        }
        if !worth_sorting || !saves_work(rest.len(), groups.len()) {
            self.lookup_each_row(
                scope_map,
                hashes,
                (!all_rows).then_some(probe_rows.as_slice()),
                offset,
                &to_build_row,
                usize::MAX,
                lookup,
            )?;
            return Ok(());
        }

        #[cfg(test)]
        {
            self.grouped = true;
        }
        // Send the pairs that are left, so the lookups below see their marks.
        self.send()?;
        let group_hashes: Vec<u64> = groups.iter().map(|group| group[0].0).collect();
        match self.marks {
            UnknownMarks::Build(_) => self.pair_build_groups(
                scope_map,
                &groups,
                &group_hashes,
                &to_build_row,
                lookup,
            ),
            UnknownMarks::Probe(_) => {
                for (group, hash) in groups.iter().zip(&group_hashes) {
                    self.pair_probe_group(
                        scope_map,
                        group,
                        *hash,
                        &to_build_row,
                        lookup,
                    )?;
                }
                Ok(())
            }
        }
    }

    /// Looks up each of `probe_rows`, whose hashes are `hashes`, from `offset`,
    /// and pairs it with the unmarked build rows of its hash. `None` stands
    /// for every row of the probe batch. Stops once more
    /// than `max_wasted_matches` matches are wasted, and then returns where
    /// the lookup stopped and how many matches it walked.
    #[expect(clippy::too_many_arguments)]
    fn lookup_each_row(
        &mut self,
        scope_map: &dyn JoinHashMapType,
        hashes: &[u64],
        probe_rows: Option<&[u32]>,
        mut offset: ProbeOffset,
        to_build_row: impl Fn(u64) -> u64,
        max_wasted_matches: usize,
        lookup: &mut LookupBuffers,
    ) -> Result<Option<(ProbeOffset, usize)>> {
        let mut matches = 0;
        loop {
            let next_offset = self.lookup_unmarked(
                scope_map,
                hashes,
                |position| match probe_rows {
                    Some(rows) => rows[position as usize],
                    None => position,
                },
                offset,
                &to_build_row,
                &mut matches,
                lookup,
            );
            self.push_all(&lookup.build_indices, &lookup.probe_indices)?;
            let Some(next_offset) = next_offset else {
                return Ok(None);
            };
            offset = next_offset;
            if self.wasted_matches > max_wasted_matches {
                return Ok(Some((offset, matches)));
            }
        }
    }

    /// Pairs each group of probe rows with the build rows of its hash, when
    /// the marks are on the build rows. The groups are looked up together,
    /// and the build rows of a group are dropped as they are marked.
    fn pair_build_groups(
        &mut self,
        scope_map: &dyn JoinHashMapType,
        groups: &[&[(u64, u32)]],
        group_hashes: &[u64],
        to_build_row: impl Fn(u64) -> u64,
        lookup: &mut LookupBuffers,
    ) -> Result<()> {
        let mut build_rows = vec![];
        let mut offset = AtRow { row: 0 };
        loop {
            // A lookup chunk holds the build rows of one or more groups, in
            // group order.
            let next_offset = self.lookup_unmarked(
                scope_map,
                group_hashes,
                |group| group,
                offset,
                &to_build_row,
                &mut 0,
                lookup,
            );
            let mut matches = lookup.build_indices.as_slice();
            for group in lookup.probe_indices.chunk_by(|a, b| a == b) {
                let group_matches;
                (group_matches, matches) = matches.split_at(group.len());
                build_rows.clear();
                build_rows.extend_from_slice(group_matches);
                // A build row belongs to one group only, so the marks made
                // for the earlier groups do not concern these build rows.
                self.needs_refresh = false;
                self.pair_unmarked(
                    &mut build_rows,
                    groups[group[0] as usize].iter().map(|(_, row)| *row),
                    |b, p| (b, p),
                )?;
            }
            let Some(next_offset) = next_offset else {
                return Ok(());
            };
            offset = next_offset;
        }
    }

    /// Pairs one group of probe rows with the build rows of its hash, when
    /// the marks are on the probe rows. The probe rows of the group are
    /// dropped as they are marked, and the walk of the hash chain stops when
    /// none is left.
    fn pair_probe_group(
        &mut self,
        scope_map: &dyn JoinHashMapType,
        group: &[(u64, u32)],
        hash: u64,
        to_build_row: impl Fn(u64) -> u64,
        lookup: &mut LookupBuffers,
    ) -> Result<()> {
        let mut probe_rows: Vec<u32> = group.iter().map(|(_, row)| *row).collect();
        let mut build_rows = vec![];
        let mut offset = AtRow { row: 0 };
        loop {
            let next_offset = scope_map.get_matched_indices_with_limit_offset(
                &[hash],
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
            build_rows.clear();
            build_rows.extend(lookup.build_indices.iter().map(|row| to_build_row(*row)));
            // Drop the probe rows that the earlier chunks marked.
            self.needs_refresh = true;
            self.pair_unmarked(&mut probe_rows, build_rows.iter().copied(), |p, b| {
                (b, p)
            })?;
            if probe_rows.is_empty() {
                return Ok(());
            }
            let Some(next_offset) = next_offset else {
                return Ok(());
            };
            offset = next_offset;
        }
    }

    /// Looks up the next chunk of matches of `hashes` in `scope_map`, from
    /// `offset`, into the buffers: the position in `hashes` translated with
    /// `to_probe_row`, and the build row translated with `to_build_row`. Drops
    /// the matches that decide a marked row, counts the wasted matches (see
    /// [`Self::scope_matches`]), and adds the matches walked to `matches`.
    #[expect(clippy::too_many_arguments)]
    fn lookup_unmarked(
        &mut self,
        scope_map: &dyn JoinHashMapType,
        hashes: &[u64],
        to_probe_row: impl Fn(u32) -> u32,
        offset: ProbeOffset,
        to_build_row: impl Fn(u64) -> u64,
        matches: &mut usize,
        lookup: &mut LookupBuffers,
    ) -> Option<ProbeOffset> {
        let LookupBuffers {
            probe_indices,
            build_indices,
        } = lookup;
        let next_offset = scope_map.get_matched_indices_with_limit_offset(
            hashes,
            None,
            self.batch_size,
            offset,
            probe_indices,
            build_indices,
        );
        for build_row in build_indices.iter_mut() {
            *build_row = to_build_row(*build_row);
        }
        for probe_row in probe_indices.iter_mut() {
            *probe_row = to_probe_row(*probe_row);
        }
        let len = build_indices.len();
        // With marks on the probe rows, only the lookup per probe row comes
        // here, and the matches of a probe row are all pushed before any of
        // them is sent. So the check could drop only the rest of a hash chain
        // that spans lookup chunks, and it is skipped.
        //
        // With marks on the build rows, a check that drops nothing makes the
        // next checks less frequent, up to one in `MAX_UNCHECKED_LOOKUPS + 1`
        // lookups. The pairs of a marked build row that a skipped check lets
        // through only cost a filter evaluation, and change no mark.
        if let UnknownMarks::Build(bitmap) = self.marks {
            #[cfg(test)]
            {
                self.build_lookups += 1;
            }
            if self.lookups_until_check > 0 {
                self.lookups_until_check -= 1;
            } else {
                let bitmap = bitmap.lock();
                let mut kept = 0;
                for i in 0..len {
                    let (build, probe) = (build_indices[i], probe_indices[i]);
                    if !bitmap.get_bit(build as usize) {
                        build_indices[kept] = build;
                        probe_indices[kept] = probe;
                        kept += 1;
                    }
                }
                build_indices.truncate(kept);
                probe_indices.truncate(kept);
                self.unchecked_lookups =
                    next_unchecked_lookups(self.unchecked_lookups, kept < len);
                #[cfg(test)]
                {
                    self.mark_checks += 1;
                }
                self.lookups_until_check = self.unchecked_lookups;
            }
        }
        *matches += len;
        #[cfg(test)]
        {
            self.looked_up_matches += len;
        }
        self.wasted_matches += match self.marks {
            // The matches whose build row is marked.
            UnknownMarks::Build(_) => len - build_indices.len(),
            // Every match pairs its probe row again, unless it is the first
            // match of that probe row. Counting all matches overcounts by at
            // most one per probe row.
            UnknownMarks::Probe(_) => len,
        };
        next_offset
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

    /// Pushes the pairs of `build_rows` and `probe_rows`.
    fn push_all(&mut self, mut build_rows: &[u64], mut probe_rows: &[u32]) -> Result<()> {
        while !build_rows.is_empty() {
            let n = build_rows
                .len()
                .min(self.batch_size - self.build_chunk.len());
            let (build_now, build_later) = build_rows.split_at(n);
            let (probe_now, probe_later) = probe_rows.split_at(n);
            self.build_chunk.extend_from_slice(build_now);
            self.probe_chunk.extend_from_slice(probe_now);
            (build_rows, probe_rows) = (build_later, probe_later);
            if self.build_chunk.len() == self.batch_size {
                self.send()?;
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
            let (considered_groups, grouped) = (pairs.considered_groups, pairs.grouped);
            let looked_up_matches = pairs.looked_up_matches;
            let (build_lookups, mark_checks) = (pairs.build_lookups, pairs.mark_checks);
            pairs.finish()?;
            let bitmap = bitmap.into_inner().finish();
            let marks = (0..bitmap.len()).map(|row| bitmap.value(row)).collect();
            Ok((
                marks,
                Stats {
                    considered_groups,
                    grouped,
                    looked_up_matches,
                    build_lookups,
                    mark_checks,
                    foreign_marks,
                },
            ))
        }
    }

    #[derive(Debug)]
    struct Stats {
        considered_groups: bool,
        grouped: bool,
        looked_up_matches: usize,
        build_lookups: usize,
        mark_checks: usize,
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
        let mut grouped = [0; 2];
        for i in 0..3000 {
            let case = Case::random(&mut rng);
            let (marks, stats) = case.run()?;
            let mut expected = case.expected_marks();
            for row in &stats.foreign_marks {
                expected[*row] = true;
            }
            assert_eq!(marks, expected, "case {i}: {case:?}");
            if stats.grouped {
                grouped[case.mark_build_rows as usize] += 1;
            }
        }
        // Both kinds of marks reach the grouped pairing.
        assert!(grouped.iter().all(|n| *n > 100), "{grouped:?}");
        Ok(())
    }

    /// Every scope holds `rows_per_scope` build rows and as many probe rows.
    /// Returns the pairing of all of them.
    fn equal_scopes(
        mark_build_rows: bool,
        rows_per_scope: usize,
        num_scopes: usize,
        filter_one_in: u64,
    ) -> Result<Stats> {
        let num_rows = rows_per_scope * num_scopes;
        let keys: Vec<u64> = (0..num_rows as u64)
            .map(|row| row % num_scopes as u64)
            .collect();
        let case = Case {
            mark_build_rows,
            through_scope_map: true,
            batch_size: 8192,
            hash_modulus: num_scopes as u64,
            build_keys: keys.clone(),
            probe_keys: keys,
            probe_rows: (0..num_rows as u32).collect(),
            build_rows: (0..num_rows as u64).collect(),
            marked_before: vec![false; num_rows],
            filter_seed: 0,
            filter_one_in,
            foreign_marks: false,
        };
        let (marks, stats) = case.run()?;
        assert_eq!(marks, case.expected_marks());
        Ok(stats)
    }

    /// Scopes of a few rows cost less to look up per probe row than to sort.
    /// So the pairing must not switch to groups for them, whether every pair
    /// marks its row or none does.
    #[test]
    fn small_scopes_are_not_grouped() -> Result<()> {
        for (mark_build_rows, rows_per_scope, filter_one_in) in [
            (true, 1, 1),
            (true, 5, u64::MAX),
            (false, 1, 1),
            (false, 5, 1),
            (false, 5, u64::MAX),
        ] {
            let stats =
                equal_scopes(mark_build_rows, rows_per_scope, 400, filter_one_in)?;
            assert!(
                !stats.considered_groups,
                "marks on build rows: {mark_build_rows}, {rows_per_scope} rows per scope, one in {filter_one_in} pairs passes"
            );
        }
        // Probe rows marked by their first pair waste the other matches of
        // their scope. With 16 build rows per scope that is enough to consider
        // groups, but a group of 16 probe rows fills no chunk.
        let stats = equal_scopes(false, 16, 400, 1)?;
        assert!(stats.considered_groups && !stats.grouped);
        // A large scope does switch.
        for mark_build_rows in [true, false] {
            assert!(equal_scopes(mark_build_rows, 400, 1, 1)?.grouped);
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

    #[test]
    fn estimate_groups_from_a_sample() {
        let estimate = |hashes: Vec<u64>| estimate_groups(&hashes);
        // Exact when the rows fit in the sample.
        assert_eq!(estimate(vec![]), 0);
        assert_eq!(estimate((0..100).collect()), 100);
        assert_eq!(estimate((0..256).map(|row| row % 7).collect()), 7);
        // A sample that sees every hash several times.
        assert_eq!(estimate(vec![7; 8192]), 1);
        assert_eq!(estimate((0..8192).map(|row| row % 4).collect()), 4);
        // The sample is fixed, so the estimates are too.
        let periodic = |rows: u64, groups: u64| {
            estimate((0..rows).map(|row| row % groups).collect())
        };
        assert_eq!(periodic(1000, 300), 262);
        assert_eq!(periodic(8192, 1000), 698);
        assert_eq!(periodic(8192, 100), 100);
        assert_eq!(periodic(8192, 8192), 6377);
        // Between those, and with periodic keys as from `value % n`, the
        // estimate stays within a factor of two.
        for (rows, groups) in [
            (1000, 300),
            (8192, 1000),
            (8192, 100),
            (8192, 32),
            (8192, 8192),
        ] {
            let estimated = estimate((0..rows).map(|row| row % groups).collect());
            assert!(
                (groups / 2..=2 * groups).contains(&(estimated as u64)),
                "{rows} rows, {groups} groups: {estimated}"
            );
        }
    }

    #[test]
    fn groups_save_work_when_they_share_walks_or_chunks() {
        // Marks on build rows: a group shares one walk of the hash chain.
        assert!(groups_save_work(true, 10, 5, 0, 8192));
        assert!(!groups_save_work(true, 9, 5, 1_000, 8192));
        // Marks on probe rows: the pairs of a group must fill more than a
        // chunk, here 4 groups of 2048 probe rows.
        assert!(!groups_save_work(false, 8192, 4, 8, 8192));
        assert!(groups_save_work(false, 8192, 4, 9, 8192));
        assert!(groups_save_work(false, 8192, 4, 2, 1024));
        assert!(!groups_save_work(false, 8192, 4, 2, 4096));
    }

    #[test]
    fn mark_checks_back_off_while_they_drop_nothing() {
        assert_eq!(next_unchecked_lookups(0, false), 1);
        assert_eq!(next_unchecked_lookups(1, false), 2);
        assert_eq!(next_unchecked_lookups(5, false), 10);
        assert_eq!(next_unchecked_lookups(20, false), MAX_UNCHECKED_LOOKUPS);
        assert_eq!(
            next_unchecked_lookups(MAX_UNCHECKED_LOOKUPS, false),
            MAX_UNCHECKED_LOOKUPS
        );
        assert_eq!(next_unchecked_lookups(MAX_UNCHECKED_LOOKUPS, true), 0);
        assert_eq!(next_unchecked_lookups(0, true), 0);
    }

    /// With marks on the build rows and a filter that no pair passes, no
    /// build row is ever marked. So the lookups must skip most checks of the
    /// marks.
    #[test]
    fn lookups_skip_checks_while_nothing_is_marked() -> Result<()> {
        let stats = equal_scopes(true, 2_000, 1, u64::MAX)?;
        let (checks, lookups) = (stats.mark_checks, stats.build_lookups);
        assert!(lookups > 100, "{lookups} lookups");
        assert!(checks > 0);
        assert!(
            checks * MAX_UNCHECKED_LOOKUPS / 2 <= lookups,
            "{checks} checks in {lookups} lookups"
        );
        Ok(())
    }

    /// With marks on the probe rows, a scope with far more build rows than
    /// probe rows: once every probe row is marked, the walk of the hash chain
    /// must stop. So the walk stays within the matches before the switch to
    /// groups and a few lookup chunks, whatever the number of build rows.
    #[test]
    fn marked_probe_groups_stop_walking() -> Result<()> {
        for batch_size in [16, 8192] {
            let (num_build, num_probe) = (50_000, 500);
            let walked =
                walked_matches_in_one_scope(false, num_build, num_probe, batch_size)?;
            let bound = WASTED_MATCHES_PER_SORTED_ROW * num_probe + 4 * batch_size;
            assert!(
                walked <= bound,
                "batch_size {batch_size}: {walked} matches walked, bound {bound}"
            );
        }
        Ok(())
    }

    /// One scope with many rows, where every pair passes: once a row is
    /// marked, it must not be paired again. So the scope map lookups walk
    /// about as many matches as there are rows, not one per pair of rows. A
    /// lookup per probe row walks every build row of the scope for every
    /// probe row.
    #[test]
    fn scope_lookups_stay_linear_in_rows() -> Result<()> {
        for mark_build_rows in [true, false] {
            for batch_size in [1, 16, 8192] {
                let (num_build, num_probe) = (2_000, 2_000);
                let walked = walked_matches_in_one_scope(
                    mark_build_rows,
                    num_build,
                    num_probe,
                    batch_size,
                )?;
                // The checks of the build row marks that are skipped can
                // delay the switch to groups by that many lookups.
                let bound = WASTED_MATCHES_PER_SORTED_ROW * num_probe
                    + 2 * num_build
                    + (MAX_UNCHECKED_LOOKUPS + 2) * batch_size;
                // Every build row is found at least once.
                assert!(walked >= num_build, "{walked} matches walked");
                assert!(
                    walked <= bound,
                    "marks on build rows: {mark_build_rows}, batch_size {batch_size}: {walked} matches walked, bound {bound}"
                );
            }
        }
        Ok(())
    }
}
