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

use std::collections::HashSet;
use std::sync::Arc;

use arrow::array::{Array, ArrayRef};
use datafusion_common::pruning::PruningStatistics;
use datafusion_common::{Result, ScalarValue, assert_eq_or_internal_err};

// Pruner Common
/// e.g. for x > 5
/// bucket 1 has stat [10,15] -> KeepAll
/// bucket 2 has stat [0,5] -> SkipAll (prune it)
/// bucket 3 has stat [0,10] -> Unknown
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PruningResult {
    KeepAll,
    SkipAll,
    Unknown,
}

#[derive(Debug, Clone)]
pub enum RangeStats {
    Values {
        mins: Option<ArrayRef>,
        maxs: Option<ArrayRef>,
        length: usize,
    },
    /// Represents a uniform literal value across all containers.
    /// This variant make it easy to compare between literals and normal ranges representing
    /// each containers' value range.
    ///
    /// TODO: remove length -- seems redundant
    Scalar { value: ScalarValue, length: usize },
}

/// Summaries about whether a container has nulls.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NullPresence {
    /// Every row in the container is null.
    AllNull,
    /// No rows in the container are null.
    NoNull,
    /// Mixed values or insufficient information to decide.
    Unknown,
}

/// Null-related statistics for each container.
#[derive(Debug, Clone)]
pub struct NullStats {
    presence: Vec<NullPresence>,
}

#[derive(Debug, Clone)]
pub struct SetStats {
    sets: Vec<Option<HashSet<ScalarValue>>>,
}

#[derive(Debug, Clone)]
pub struct ColumnStats {
    pub range_stats: Option<RangeStats>,
    pub null_stats: Option<NullStats>,
    pub set_stats: Option<SetStats>,
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
        Ok(Self::Values { mins, maxs, length })
    }

    /// Create range stats for a constant literal across all containers.
    ///
    pub fn new_scalar(value: ScalarValue, length: usize) -> Result<Self> {
        Ok(Self::Scalar { value, length })
    }

    pub fn len(&self) -> usize {
        match self {
            RangeStats::Values { length, .. } | RangeStats::Scalar { length, .. } => {
                *length
            }
        }
    }
}

pub struct PruningContext {
    stats: Arc<dyn PruningStatistics>,
}

impl PruningContext {
    pub fn new(stats: Arc<dyn PruningStatistics>) -> Self {
        Self { stats }
    }

    pub fn statistics(&self) -> &Arc<dyn PruningStatistics> {
        &self.stats
    }
}

impl NullStats {
    pub fn new(
        null_counts: Option<ArrayRef>,
        row_counts: Option<ArrayRef>,
        length: usize,
    ) -> Result<Self> {
        if let Some(ref null_counts) = null_counts {
            assert_eq_or_internal_err!(
                null_counts.len(),
                length,
                "Null counts length mismatch for pruning statistics"
            );
        }
        if let Some(ref row_counts) = row_counts {
            assert_eq_or_internal_err!(
                row_counts.len(),
                length,
                "Row counts length mismatch for pruning statistics"
            );
        }

        let null_counts = null_counts
            .as_ref()
            .and_then(|counts| counts.as_any().downcast_ref::<arrow::array::UInt64Array>());
        let row_counts = row_counts
            .as_ref()
            .and_then(|counts| counts.as_any().downcast_ref::<arrow::array::UInt64Array>());

        let mut presence = Vec::with_capacity(length);
        for idx in 0..length {
            let nulls = null_counts
                .and_then(|counts| (!counts.is_null(idx)).then(|| counts.value(idx)));
            let rows = row_counts
                .and_then(|counts| (!counts.is_null(idx)).then(|| counts.value(idx)));

            let state = match (nulls, rows) {
                (Some(0), Some(_)) => NullPresence::NoNull,
                (Some(n), Some(r)) if n == r => NullPresence::AllNull,
                (Some(0), None) => NullPresence::NoNull,
                _ => NullPresence::Unknown,
            };

            presence.push(state);
        }

        Ok(Self { presence })
    }

    pub fn len(&self) -> usize {
        self.presence.len()
    }

    pub fn presence(&self) -> &[NullPresence] {
        &self.presence
    }
}

impl SetStats {
    pub fn new(sets: Vec<Option<HashSet<ScalarValue>>>, length: usize) -> Result<Self> {
        assert_eq_or_internal_err!(
            sets.len(),
            length,
            "Set stats length mismatch for pruning statistics"
        );
        Ok(Self { sets })
    }

    pub fn len(&self) -> usize {
        self.sets.len()
    }

    pub fn value_sets(&self) -> &[Option<HashSet<ScalarValue>>] {
        &self.sets
    }
}

impl ColumnStats {
    pub fn new(range_stats: Option<RangeStats>, null_stats: Option<NullStats>) -> Self {
        Self::new_with_set_stats(range_stats, null_stats, None)
    }

    pub fn new_with_set_stats(
        range_stats: Option<RangeStats>,
        null_stats: Option<NullStats>,
        set_stats: Option<SetStats>,
    ) -> Self {
        Self {
            range_stats,
            null_stats,
            set_stats,
        }
    }

    pub fn range_stats(&self) -> Option<&RangeStats> {
        self.range_stats.as_ref()
    }

    pub fn null_stats(&self) -> Option<&NullStats> {
        self.null_stats.as_ref()
    }

    pub fn set_stats(&self) -> Option<&SetStats> {
        self.set_stats.as_ref()
    }
}

// TODO: should include length (container count)
#[derive(Debug, Clone)]
pub enum PruningIntermediate {
    IntermediateStats(ColumnStats),
    IntermediateResult(Vec<PruningResult>),
}

impl PruningIntermediate {
    /// Create an `IntermediateStats` variant with no range or null statistics.
    pub fn empty_stats() -> Self {
        Self::IntermediateStats(ColumnStats::new(None, None))
    }
}
