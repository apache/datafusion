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

//! Measurements that the placement decisions use. All partitions and files
//! of one scan share them.

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use arrow::array::{Array, BooleanArray};
use datafusion_physical_expr::PhysicalExpr;
use datafusion_physical_expr::expressions::OptionalFilterPhysicalExpr;
use datafusion_physical_expr::filter_stats::duration_nanos;
use datafusion_physical_expr::utils::is_optional_filter;
use parking_lot::Mutex;

use crate::optional_filter::DEFAULT_DECODE_NS_PER_BYTE;

/// Number of rows in one window of [`skippable_rows`].
///
/// The Parquet decoder skips the rows that a row filter removes only when
/// the removed rows make long runs. Its default selection policy
/// (`RowSelectionPolicy::Auto { threshold: 32 }`) decodes all rows and then
/// filters them when the runs are shorter than 32 rows on average. A window
/// of 64 rows is one `u64` of the filter result.
pub(crate) const SKIP_WINDOW_ROWS: usize = 64;

/// Rows of `result` in windows of [`SKIP_WINDOW_ROWS`] rows where no row
/// passes (a `null` does not pass). These are the rows that a row filter
/// lets the decoder skip for the other columns. A partial window at the end
/// is counted if no row in it passes.
pub(crate) fn skippable_rows(result: &BooleanArray) -> usize {
    let values = result.values();
    let passed = match result.nulls() {
        Some(nulls) => values & nulls.inner(),
        None => values.clone(),
    };
    let chunks = passed.bit_chunks();
    let full = chunks.iter().filter(|chunk| *chunk == 0).count() * SKIP_WINDOW_ROWS;
    let remainder = if chunks.remainder_len() > 0 && chunks.remainder_bits() == 0 {
        chunks.remainder_len()
    } else {
        0
    };
    full + remainder
}

/// What the scan measured for one conjunct: in the row filter or in the
/// post-scan filter, and in the row group statistics pruning.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub(crate) struct Observation {
    /// Rows that the conjunct was evaluated on.
    pub(crate) rows_in: u64,
    /// Rows that passed the conjunct.
    pub(crate) rows_out: u64,
    /// Rows in windows where no row passed, see [`skippable_rows`].
    pub(crate) skippable_rows: u64,
    /// Row groups that the conjunct alone pruned with statistics.
    pub(crate) row_groups_pruned: u64,
    /// Row groups that the conjunct alone did not prune with statistics.
    pub(crate) row_groups_kept: u64,
}

impl Observation {
    /// Fraction of the evaluated rows that the decoder can skip, or `None`
    /// if the conjunct was not evaluated on any row.
    pub(crate) fn skippable_fraction(&self) -> Option<f64> {
        (self.rows_in > 0).then(|| self.skippable_rows as f64 / self.rows_in as f64)
    }

    /// Fraction of the row groups that the conjunct pruned with statistics,
    /// or `None` if there is no statistics pruning result.
    pub(crate) fn pruned_fraction(&self) -> Option<f64> {
        let total = self.row_groups_pruned + self.row_groups_kept;
        (total > 0).then(|| self.row_groups_pruned as f64 / total as f64)
    }
}

/// The pooled [`Observation`] of one conjunct. Lock-free.
#[derive(Debug, Default)]
pub(crate) struct ConjunctStats {
    rows_in: AtomicU64,
    rows_out: AtomicU64,
    skippable_rows: AtomicU64,
    row_groups_pruned: AtomicU64,
    row_groups_kept: AtomicU64,
}

impl ConjunctStats {
    /// Records one evaluation of the conjunct. `result` has one value for
    /// each evaluated row.
    pub(crate) fn record_evaluation(&self, result: &BooleanArray) {
        let rows_in = result.len() as u64;
        if rows_in == 0 {
            return;
        }
        // `true_count` does not count nulls.
        let rows_out = result.true_count() as u64;
        let skippable = if rows_out == 0 {
            rows_in
        } else if rows_out == rows_in {
            0
        } else {
            skippable_rows(result) as u64
        };
        self.rows_in.fetch_add(rows_in, Ordering::Relaxed);
        self.rows_out.fetch_add(rows_out, Ordering::Relaxed);
        self.skippable_rows.fetch_add(skippable, Ordering::Relaxed);
    }

    /// The pooled values. The values are not read at the same instant.
    pub(crate) fn observation(&self) -> Observation {
        Observation {
            rows_in: self.rows_in.load(Ordering::Relaxed),
            rows_out: self.rows_out.load(Ordering::Relaxed),
            skippable_rows: self.skippable_rows.load(Ordering::Relaxed),
            row_groups_pruned: self.row_groups_pruned.load(Ordering::Relaxed),
            row_groups_kept: self.row_groups_kept.load(Ordering::Relaxed),
        }
    }
}

/// The measured decode speed of the columns that the decoder produces, for
/// one scan. Lock-free.
#[derive(Debug, Default)]
pub(crate) struct DecodeSpeed {
    nanos: AtomicU64,
    bytes: AtomicU64,
}

impl DecodeSpeed {
    /// Records that the decoder produced `bytes` compressed bytes (estimated
    /// from the footer) in `elapsed`.
    pub(crate) fn record(&self, elapsed: Duration, bytes: u64) {
        self.nanos
            .fetch_add(duration_nanos(elapsed), Ordering::Relaxed);
        self.bytes.fetch_add(bytes, Ordering::Relaxed);
    }

    /// Decode time for each compressed byte. Before the first measurement,
    /// the same default as the optional filter gates.
    pub(crate) fn ns_per_byte(&self) -> f64 {
        let bytes = self.bytes.load(Ordering::Relaxed);
        if bytes == 0 {
            return DEFAULT_DECODE_NS_PER_BYTE;
        }
        self.nanos.load(Ordering::Relaxed) as f64 / bytes as f64
    }
}

/// The mean latency of the fetches (`AsyncFileReader::get_byte_ranges`) of
/// one scan. Lock-free.
#[derive(Debug, Default)]
pub(crate) struct FetchCost {
    nanos: AtomicU64,
    fetches: AtomicU64,
}

impl FetchCost {
    /// Records one fetch that took `elapsed`.
    pub(crate) fn record(&self, elapsed: Duration) {
        self.nanos
            .fetch_add(duration_nanos(elapsed), Ordering::Relaxed);
        self.fetches.fetch_add(1, Ordering::Relaxed);
    }

    /// Mean latency of one fetch in nanoseconds, 0 before the first fetch.
    pub(crate) fn mean_nanos(&self) -> f64 {
        let fetches = self.fetches.load(Ordering::Relaxed);
        if fetches == 0 {
            return 0.0;
        }
        self.nanos.load(Ordering::Relaxed) as f64 / fetches as f64
    }
}

/// Identifies one conjunct of the scan predicate in the rewritten predicate
/// of each file.
///
/// The scan rewrites its predicate for each file (schema adaptation,
/// partition values, simplification):
///
/// * A conjunct with a [`PhysicalExpr::expression_id`] (for example a
///   `DynamicFilterPhysicalExpr`) uses that id. `with_new_children` keeps
///   the id, thus all rewrites keep it.
/// * Other conjuncts use their position in the root `AND` chain. A rewrite
///   does not reorder the conjuncts, but the simplifier can fold a conjunct
///   to a literal. Thus the position is used only when the file predicate
///   has as many conjuncts as the scan predicate.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
enum ConjunctKey {
    ExpressionId(u64),
    Position(usize),
}

/// The pooled measurements of one scan, shared by all partitions and files
/// of that scan. `ParquetSource` makes a new one each time its predicate
/// changes.
#[derive(Debug, Default)]
pub(crate) struct PlacementSites {
    conjuncts: Mutex<HashMap<ConjunctKey, Arc<ConjunctStats>>>,
    fetch: FetchCost,
    decode: DecodeSpeed,
}

impl PlacementSites {
    /// The fetch latency of the scan.
    pub(crate) fn fetch(&self) -> &FetchCost {
        &self.fetch
    }

    /// The decode speed of the scan.
    pub(crate) fn decode(&self) -> &DecodeSpeed {
        &self.decode
    }

    /// The statistics of the conjunct at `position` in `conjuncts` (the
    /// conjuncts of the root `AND` chain of a file predicate). The scan
    /// predicate has `scan_count` conjuncts. See [`ConjunctSiteKey`].
    pub(crate) fn stats_for(
        &self,
        conjuncts: &[&Arc<dyn PhysicalExpr>],
        position: usize,
        scan_count: usize,
    ) -> Arc<ConjunctStats> {
        let conjunct = conjuncts[position];
        // An optional filter keeps the id of the filter that it wraps.
        let expr = conjunct
            .downcast_ref::<OptionalFilterPhysicalExpr>()
            .map_or(conjunct, |optional| optional.inner());
        debug_assert!(!is_optional_filter(expr));
        let key = match expr.expression_id() {
            Some(id) => Some(ConjunctKey::ExpressionId(id)),
            None => {
                (conjuncts.len() == scan_count).then_some(ConjunctKey::Position(position))
            }
        };
        match key {
            Some(key) => Arc::clone(self.conjuncts.lock().entry(key).or_default()),
            // The conjunct cannot be identified: do not share its statistics.
            None => Arc::new(ConjunctStats::default()),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn bools(values: impl IntoIterator<Item = Option<bool>>) -> BooleanArray {
        values.into_iter().collect()
    }

    #[test]
    fn skippable_rows_counts_empty_windows() {
        // Window 0 (rows 0..64) has one passing row, window 1 has none, the
        // partial window (rows 128..160) has none.
        let mut values = vec![Some(false); 160];
        values[10] = Some(true);
        assert_eq!(skippable_rows(&bools(values.clone())), 64 + 32);

        // A null does not pass.
        values[70] = None;
        assert_eq!(skippable_rows(&bools(values.clone())), 64 + 32);

        // One passing row in the partial window.
        values[150] = Some(true);
        assert_eq!(skippable_rows(&bools(values)), 64);

        // Scattered passing rows: no window is empty.
        let scattered = (0..640).map(|i| Some(i % 50 == 0));
        assert_eq!(skippable_rows(&bools(scattered)), 0);
    }

    #[test]
    fn conjunct_stats_accumulate() {
        let stats = ConjunctStats::default();
        let mut values = vec![Some(false); 128];
        values[0] = Some(true);
        stats.record_evaluation(&bools(values));
        stats.record_evaluation(&bools(vec![Some(true); 64]));
        stats.record_evaluation(&bools(vec![None; 64]));
        let observation = stats.observation();
        assert_eq!(
            observation,
            Observation {
                rows_in: 256,
                rows_out: 65,
                skippable_rows: 128,
                row_groups_pruned: 0,
                row_groups_kept: 0,
            }
        );
        assert_eq!(observation.skippable_fraction(), Some(0.5));
        assert_eq!(Observation::default().skippable_fraction(), None);
        assert_eq!(Observation::default().pruned_fraction(), None);
    }

    #[test]
    fn decode_speed() {
        let decode = DecodeSpeed::default();
        assert_eq!(decode.ns_per_byte(), DEFAULT_DECODE_NS_PER_BYTE);
        decode.record(Duration::from_nanos(300), 100);
        decode.record(Duration::from_nanos(100), 100);
        assert_eq!(decode.ns_per_byte(), 2.0);
    }

    #[test]
    fn stats_are_shared_by_position_or_id() {
        use arrow::datatypes::{DataType, Field, Schema};
        use datafusion_physical_expr::expressions::{
            DynamicFilterPhysicalExpr, col, lit,
        };

        let schema = Schema::new(vec![Field::new("a", DataType::Boolean, true)]);
        let a = col("a", &schema).unwrap();
        let dynamic: Arc<dyn PhysicalExpr> = Arc::new(DynamicFilterPhysicalExpr::new(
            vec![Arc::clone(&a)],
            lit(true),
        ));
        let sites = PlacementSites::default();
        let file = [&a, &dynamic];
        // Same position and count: shared.
        assert!(Arc::ptr_eq(
            &sites.stats_for(&file, 0, 2),
            &sites.stats_for(&file, 0, 2)
        ));
        // Another count: not shared.
        assert!(!Arc::ptr_eq(
            &sites.stats_for(&file, 0, 2),
            &sites.stats_for(&file, 0, 3)
        ));
        // An expression id: shared at all positions and counts.
        let other_file = [&dynamic];
        assert!(Arc::ptr_eq(
            &sites.stats_for(&file, 1, 5),
            &sites.stats_for(&other_file, 0, 7)
        ));
    }

    #[test]
    fn fetch_cost_mean() {
        let fetch = FetchCost::default();
        assert_eq!(fetch.mean_nanos(), 0.0);
        fetch.record(Duration::from_micros(1));
        fetch.record(Duration::from_micros(3));
        assert_eq!(fetch.mean_nanos(), 2_000.0);
    }
}
