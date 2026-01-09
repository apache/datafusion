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

// Pruner Common Structs/Utilities

//! This is the top-level comment for pruning via statistics propagation.
//!
//! TODO: This is a concise draft; it should be polished for readers with less
//! prior background.
//!
//! # Introduction
//!
//! This module helps skip scanning data micro-partitions by evaluating predicates
//! against container-level statistics.
//!
//! It supports pruning for complex and nested predicates through statistics
//! propagation.
//!
//! For examples of pruning nested predicates via statistics propagation, see:
//! <https://github.com/apache/datafusion/issues/19487>
//!
//!
//!
//! # Vectorized pruning intermediate representation
//!
//! Source statistics and intermediate pruning results are stored in Arrow arrays,
//! enabling vectorized evaluation across many containers.
//!
//!
//!
//! # Difference from [`super::PhysicalExpr::evaluate_bounds`]
//!
//! `evaluate_bounds()` derives per-column statistics for a single plan, aimed at
//! tasks like cardinality estimation and other planner fast paths. It reasons
//! about one container and may track richer distribution details.
//! Pruning must reason about *all* containers (potentially thousands) to decide
//! which to skip, so it favors a vectorized, array-backed representation with
//! lighter-weight stats. These are intentionally separate interfaces.
//!
//!
//!
//! # Core API/Data Structures
//!
//! The key structures involved in pruning are:
//! - [`PruningStatistics`]: the input source statistics for all containers
//! - [`super::PhysicalExpr::evaluate_pruning()`]: evaluates pruning behavior for predicates
//! - [`PruningIntermediate`]: the intermediate result produced during statistics propagation for pruning. Its internal representation uses Arrow Arrays, enabling vectorized evaluation for performance.

use std::{iter::repeat_n, sync::Arc};

use arrow::array::{Array, ArrayRef, BooleanArray, BooleanBuilder, UInt64Array};
use arrow::compute::kernels::boolean::and_kleene;
use datafusion_common::pruning::PruningStatistics;
use datafusion_common::{Result, ScalarValue, assert_eq_or_internal_err};
use datafusion_expr_common::columnar_value::ColumnarValue;

/// Physical representation of pruning outcomes for each container:
/// `true` = KeepAll, `false` = SkipAll, `null` = Unknown.
///
/// Use `BooleanArray` so the propagation steps can use existing Arrow kernels for
/// both simplicity and performance.
///
/// # Pruning results
/// - KeepAll: The pruning predicate evaluates to true for all rows within a micro
///   partition. Future filter evaluation can be skipped for that partition.
/// - SkipAll: The pruning predicate evaluates to false for all rows within a micro
///   partition. The partition can be skipped at scan time.
/// - UnknownOrMixed: The statistics are insufficient to prove KeepAll/SkipAll, or
///   the predicate is mixed. The predicate must be evaluated row-wise.
///
/// Example (`SELECT * FROM t WHERE x >= 0`):
/// - micro_partition_a(min=0, max=10): KeepAll — can pass through `FilterExec`
///   without re-evaluating `x >= 0`.
/// - micro_partition_b(min=-10, max=-1): SkipAll — skip the partition entirely.
/// - micro_partition_c(min=-5, max=5): Unknown — must evaluate the predicate on rows.
///
/// `PruningOutcome` provides utilities to convert between this semantic
/// representation and its tri-state boolean encoding.
///
/// # Important invariants
/// Pruning results must be sound, but need not be complete:
/// - If a container is labeled `KeepAll` or `SkipAll`, that label must be correct.
/// - If a container is labeled `Unknown` but is actually `KeepAll`/`SkipAll`,
///   correctness is still preserved; it just means pruning was conservative.
///
/// Propagation implementation can be refined to reduce `Unknown` cases to improve
/// pruning effectiveness.
#[derive(Debug, Clone)]
pub struct PruningResults {
    results: Option<BooleanArray>,
    /// Number of containers. Needed to infer result if all stats types are `None`.
    pub num_containers: usize,
}

/// Semantic representation for items inside `PruningResults::results`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PruningOutcome {
    KeepAll,
    SkipAll,
    UnknownOrMixed,
}

impl PruningResults {
    pub fn new(array: Option<BooleanArray>, num_containers: usize) -> Self {
        debug_assert_eq!(
            array.as_ref().map(|a| a.len()).unwrap_or(num_containers),
            num_containers
        );
        Self {
            results: array,
            num_containers,
        }
    }

    pub fn none(num_containers: usize) -> Self {
        Self::new(None, num_containers)
    }

    pub fn as_ref(&self) -> Option<&BooleanArray> {
        self.results.as_ref()
    }

    pub fn into_inner(self) -> Option<BooleanArray> {
        self.results
    }

    pub fn len(&self) -> usize {
        self.results
            .as_ref()
            .map(|a| a.len())
            .unwrap_or(self.num_containers)
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

impl PruningOutcome {
    /// Convert to/from the tri-state boolean encoding stored in `PruningResults`.
    /// - Some(true)=KeepAll
    /// - Some(false)=SkipAll
    /// - None=(Unknown/mixed)
    pub fn from_result_item(result_item: Option<bool>) -> Self {
        match result_item {
            Some(true) => PruningOutcome::KeepAll,
            Some(false) => PruningOutcome::SkipAll,
            None => PruningOutcome::UnknownOrMixed,
        }
    }

    pub fn to_result_item(&self) -> Option<bool> {
        match self {
            PruningOutcome::KeepAll => Some(true),
            PruningOutcome::SkipAll => Some(false),
            PruningOutcome::UnknownOrMixed => None,
        }
    }
}

impl From<BooleanArray> for PruningResults {
    fn from(array: BooleanArray) -> Self {
        let len = array.len();
        PruningResults::new(Some(array), len)
    }
}

#[derive(Debug, Clone)]
pub struct RangeStats {
    /// Ranges for all containers in columnar form.
    /// - If `mins`/`maxs` are `None`, all containers have unknown statistics.
    /// - Each entry (per-container) may be a bound or null. Null means missing or
    ///   unbounded (null in `mins` = -inf; treating missing/unbounded the same
    ///   does not change pruning results).
    /// - Use `ColumnarValue::Scalar` to represent a uniform literal value across
    ///   all containers.
    pub mins: Option<ColumnarValue>,
    pub maxs: Option<ColumnarValue>,
    pub length: usize,
}

/// Null-related statistics for each container stored as a BooleanArray:
/// `true` = NoNull, `false` = AllNull, `null` = Unknown/mixed.
///
/// Use `BooleanArray` so the propagation steps can use existing Arrow kernels for
/// both simplicity and performance.
/// `NullPresence` provides utility to convert between its semantics representation
/// and physical encoding.
#[derive(Debug, Clone)]
pub struct NullStats {
    presence: BooleanArray,
}

/// Semantic representation for items inside `NullStats::presence`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NullPresence {
    NoNull,
    AllNull,
    UnknownOrMixed,
}

impl NullPresence {
    /// Convert to/from the tri-state boolean encoding stored in `NullStats.presence`
    /// - Some(true)=NoNull
    /// - Some(false)=AllNull
    /// - None=(Unknown/mixed)
    pub fn from_presence_item(presence_item: Option<bool>) -> Self {
        match presence_item {
            Some(true) => NullPresence::NoNull,
            Some(false) => NullPresence::AllNull,
            None => NullPresence::UnknownOrMixed,
        }
    }

    pub fn to_presence_item(&self) -> Option<bool> {
        match self {
            NullPresence::NoNull => Some(true),
            NullPresence::AllNull => Some(false),
            NullPresence::UnknownOrMixed => None,
        }
    }
}

/// Column statistics that propagate through the `PhysicalExpr` tree nodes
///
/// # Important invariants
/// Non-null stats (e.g., ranges) describe only the value bounds for non-null
/// rows; they DO NOT include nulls. For example, a partition with `min=0,
/// max=10` may still contain nulls outside that range. Predicate pruning must
/// combine decisions from non-null stats with null stats to derive the final
/// outcome.
#[derive(Debug, Clone)]
pub struct ColumnStats {
    pub range_stats: Option<RangeStats>,
    pub null_stats: Option<NullStats>,
    /// Number of containers. Needed to infer result if all stats types are `None`.
    pub num_containers: usize,
}

impl RangeStats {
    pub fn new(
        mins: Option<ArrayRef>,
        maxs: Option<ArrayRef>,
        length: usize,
    ) -> Result<Self> {
        if let Some(ref mins) = mins {
            assert_eq_or_internal_err!(
                mins.len(),
                length,
                "Range mins length mismatch for pruning statistics"
            );
        }
        if let Some(ref maxs) = maxs {
            assert_eq_or_internal_err!(
                maxs.len(),
                length,
                "Range maxs length mismatch for pruning statistics"
            );
        }
        Ok(Self {
            mins: mins.map(ColumnarValue::Array),
            maxs: maxs.map(ColumnarValue::Array),
            length,
        })
    }

    /// Create range stats for a constant literal across all containers.
    pub fn new_constant(value: ScalarValue, length: usize) -> Result<Self> {
        let value = ColumnarValue::Scalar(value);
        Ok(Self {
            mins: Some(value.clone()),
            maxs: Some(value),
            length,
        })
    }

    pub fn len(&self) -> usize {
        self.length
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Normalize into concrete min/max arrays.
    ///
    /// For `ColumnarValue::Array`, returns cloned mins/maxs (which may be `None`).
    /// For `ColumnarValue::Scalar`, expands the scalar to arrays of length `length`.
    pub fn normalize_to_arrays(&self) -> Result<(Option<ArrayRef>, Option<ArrayRef>)> {
        let mins = match self.mins.as_ref() {
            Some(mins) => Some(mins.to_array_of_size(self.length)?),
            None => None,
        };
        let maxs = match self.maxs.as_ref() {
            Some(maxs) => Some(maxs.to_array_of_size(self.length)?),
            None => None,
        };
        Ok((mins, maxs))
    }
}

pub struct PruningContext {
    stats: Arc<dyn PruningStatistics + Send + Sync>,
}

impl PruningContext {
    pub fn new(stats: Arc<dyn PruningStatistics + Send + Sync>) -> Self {
        Self { stats }
    }

    pub fn statistics(&self) -> &Arc<dyn PruningStatistics + Send + Sync> {
        &self.stats
    }
}

impl NullStats {
    /// Build `NullStats` from per-container null and row counts.
    ///
    /// # Arguments
    /// - `null_counts`: All containers' null counts in a single `Array`, or `None` if missing.
    /// - `row_counts`: All containers' row counts in a single `Array`, or `None` if missing.
    ///
    /// # Return
    /// `Some(NullStats)` when both inputs are present and aligned; `None` when either input is missing/unknown.
    ///
    /// # Examples (per-container outcomes)
    /// - `null_counts=[3, 0, 1]`, `row_counts=[3, 5, 10]` →
    ///   presence = [false, true, null] (AllNull, NoNull, Mixed).
    ///
    /// # Errors
    /// - Internal error if inputs have inconsistent lengths.
    pub fn new(
        null_counts: Option<&UInt64Array>,
        row_counts: Option<&UInt64Array>,
    ) -> Result<Option<Self>> {
        // If either input is absent, we can't derive null stats for all containers.
        let (Some(null_counts), Some(row_counts)) = (null_counts, row_counts) else {
            return Ok(None);
        };

        let length = null_counts.len();
        assert_eq_or_internal_err!(
            row_counts.len(),
            length,
            "Row counts length mismatch for pruning statistics"
        );

        let mut presence = BooleanBuilder::with_capacity(length);
        for idx in 0..length {
            let nulls = (!null_counts.is_null(idx)).then(|| null_counts.value(idx));
            let rows = (!row_counts.is_null(idx)).then(|| row_counts.value(idx));

            // See `NullStats` for encoding semantics
            match (nulls, rows) {
                (Some(0), Some(_)) | (Some(0), None) => presence.append_value(true),
                (Some(n), Some(r)) if n == r => presence.append_value(false),
                _ => presence.append_null(),
            }
        }

        Ok(Some(Self {
            presence: presence.finish(),
        }))
    }

    /// Create a `NullStats` with a uniform `presence` repeated `num_containers` times.
    /// See `NullStats` docs for `presence` semantics.
    ///
    /// Used to create pruning statistics literal/scalar values.
    pub fn from_uniform_presence(presence: NullPresence, num_containers: usize) -> Self {
        let presence_item = match presence {
            NullPresence::NoNull => Some(true),
            NullPresence::AllNull => Some(false),
            NullPresence::UnknownOrMixed => None,
        };
        NullStats {
            presence: BooleanArray::from_iter(repeat_n(presence_item, num_containers)),
        }
    }

    /// Combine two null-stat arrays for a comparison (`=, !=, <, >, <=, >=`).
    ///
    /// None means all containers' null stats are missing, otherwise for each container:
    /// - If either side is `AllNull` → result is `AllNull` (all comparisons are null).
    /// - If both sides are `NoNull`   → result is `NoNull`.
    /// - Otherwise                    → result is `UnknownOrMixed`.
    ///
    /// # Errors
    /// Returns internal error if left and right side has inconsistent container length
    pub fn combine_for_cmp(
        left: Option<&Self>,
        right: Option<&Self>,
    ) -> Result<Option<Self>> {
        let (left, right) = match (left, right) {
            (Some(l), Some(r)) => (l, r),
            (_, _) => {
                return Ok(None);
            }
        };

        let len = left.len();
        assert_eq_or_internal_err!(
            len,
            right.len(),
            "Null stats length mismatch for comparison pruning"
        );

        // The function comments specified the semantics behavior, and given the
        // physical encoding:
        // `true` = NoNull, `false` = AllNull, `null` = Unknown/mixed.
        // The implementation can be simplified to a kleene(null-aware) 'AND'
        Ok(Some(NullStats {
            presence: and_kleene(left.presence(), right.presence())?,
        }))
    }

    pub fn len(&self) -> usize {
        self.presence.len()
    }

    pub fn is_empty(&self) -> bool {
        self.presence.is_empty()
    }

    pub fn presence(&self) -> &BooleanArray {
        &self.presence
    }
}

impl ColumnStats {
    pub fn new(
        range_stats: Option<RangeStats>,
        null_stats: Option<NullStats>,
        num_containers: usize,
    ) -> Self {
        debug_assert_eq!(
            range_stats
                .as_ref()
                .map(|r| r.len())
                .unwrap_or(num_containers),
            num_containers
        );
        debug_assert_eq!(
            null_stats
                .as_ref()
                .map(|n| n.len())
                .unwrap_or(num_containers),
            num_containers
        );
        Self {
            range_stats,
            null_stats,
            num_containers,
        }
    }

    pub fn range_stats(&self) -> Option<&RangeStats> {
        self.range_stats.as_ref()
    }

    pub fn null_stats(&self) -> Option<&NullStats> {
        self.null_stats.as_ref()
    }

    pub fn len(&self) -> usize {
        self.num_containers
    }

    pub fn is_empty(&self) -> bool {
        self.num_containers == 0
    }
}

/// Pruning intermediate type propagated through `PhysicalExpr` nodes.
/// Holds intermediate results for multiple input micro-partitions/containers,
/// stored in a vectorized Arrow array form.
#[derive(Debug, Clone)]
pub enum PruningIntermediate {
    IntermediateStats(ColumnStats),
    IntermediateResult(PruningResults),
}

impl PruningIntermediate {
    /// Create an `IntermediateStats` variant with no range or null statistics.
    pub fn empty_stats() -> Self {
        Self::IntermediateStats(ColumnStats::new(None, None, 0))
    }

    /// Returns the number of containers inside the current `PruningIntermediate`
    pub fn len(&self) -> usize {
        match self {
            PruningIntermediate::IntermediateStats(column_stats) => column_stats.len(),
            PruningIntermediate::IntermediateResult(pruning_results) => {
                pruning_results.len()
            }
        }
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

#[cfg(test)]
mod tests {
    use super::PruningOutcome;
    use arrow::array::BooleanArray;

    #[test]
    fn pruning_outcome_semantics_round_trip() {
        let arr = BooleanArray::from(vec![Some(true), Some(false), None]);

        let decoded: Vec<PruningOutcome> =
            arr.iter().map(PruningOutcome::from_result_item).collect();
        assert_eq!(
            decoded,
            vec![
                PruningOutcome::KeepAll,
                PruningOutcome::SkipAll,
                PruningOutcome::UnknownOrMixed
            ]
        );

        let encoded: Vec<Option<bool>> =
            decoded.iter().map(PruningOutcome::to_result_item).collect();
        assert_eq!(encoded, vec![Some(true), Some(false), None]);
    }
}
