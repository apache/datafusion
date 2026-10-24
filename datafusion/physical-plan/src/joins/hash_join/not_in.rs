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
use crate::joins::hash_join::probe_completion::ProbeSideSummary;
use crate::joins::hash_join::stream::lookup_join_hashmap;
use crate::joins::join_hash_map::JoinHashMapType;
use crate::joins::join_hash_map::ProbeOffset::AtRow;
use crate::joins::utils::{
    BuildProbeJoinMetrics, JoinFilter, apply_join_filter_to_indices, update_hash,
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
    probe_indices: Vec<u32>,
    build_indices: Vec<u64>,
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
                        &mut self.probe_indices,
                        &mut self.build_indices,
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
                        &mut self.probe_indices,
                        &mut self.build_indices,
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
    let scope_values = normalized[value_keys..]
        .iter()
        .map(|values| Ok(filter(values.as_ref(), &null_mask)?))
        .collect::<Result<Vec<_>>>()?;
    // `scope_values` and `build_indices` are retained for the lifetime of the
    // join, so charge them like the maps and bitmaps.
    let retained_size = build_indices.get_array_memory_size()
        + scope_values
            .iter()
            .map(|values| values.get_array_memory_size())
            .sum::<usize>();
    reservation.try_grow(retained_size)?;
    metrics.build_mem_used.add(retained_size);

    let null_scope_map = if scope_values.is_empty() {
        None
    } else {
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
            scope_values,
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
    /// build rows. Stored positions index into `scope_values`/`build_indices`,
    /// not the full build batch. `None` when the join has no correlation
    /// scope keys, so every probe row is in scope.
    scope_map: Option<Box<dyn JoinHashMapType>>,
    /// Correlation scope key values of the NULL-valued build rows.
    scope_values: Vec<ArrayRef>,
    /// Maps positions in `map`/`scope_values` back to row indices in the full
    /// build batch.
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
/// that can be decided is UNKNOWN.
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
    probe_indices_buffer: &mut Vec<u32>,
    build_indices_buffer: &mut Vec<u64>,
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

        match scope_map {
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
