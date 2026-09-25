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

use arrow::array::BooleanArray;
use datafusion_physical_expr::PhysicalExpr;
use datafusion_physical_expr::expressions::OptionalFilterPhysicalExpr;
use datafusion_physical_expr::filter_stats::duration_nanos;
use datafusion_physical_expr::utils::is_optional_filter;
use parking_lot::Mutex;

use crate::optional_filter::DEFAULT_DECODE_NS_PER_BYTE;
use crate::row_filter_cost::{SKIP_WINDOW_ROWS, skippable_in, skippable_rows};

/// What the scan measured for one conjunct: in the row filter or in the
/// post-scan filter, and in the row group statistics pruning.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub(crate) struct Observation {
    /// Rows that the conjunct was evaluated on.
    pub(crate) rows_in: u64,
    /// Rows that passed the conjunct.
    pub(crate) rows_out: u64,
    /// Rows in windows where no row passed the conjunct, see
    /// [`skippable_rows`](crate::row_filter_cost::skippable_rows) and [`StageSelection`].
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

    /// Fraction of the evaluated rows that passed, or `None` if the
    /// conjunct was not evaluated on any row.
    pub(crate) fn pass_ratio(&self) -> Option<f64> {
        (self.rows_in > 0).then(|| self.rows_out as f64 / self.rows_in as f64)
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
    /// Records one evaluation of the conjunct as a row filter predicate.
    /// `result` has one value for each evaluated row.
    ///
    /// The decoder evaluates a row filter predicate only on the rows that
    /// the earlier predicates let pass, thus the windows of a predicate
    /// that is not the first one are windows of these rows, not of the
    /// rows of the file. This is an approximation: an empty window then
    /// spans 64 or more rows of the file, thus the skippable rows are
    /// counted too low. The first predicate is measured exactly. See
    /// [`StageSelection`] for the post-scan filter.
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

    /// Records one evaluation of the conjunct on `rows_in` rows, of which
    /// `rows_out` passed and which made `skippable` rows skippable.
    pub(crate) fn record(&self, rows_in: usize, rows_out: usize, skippable: usize) {
        if rows_in == 0 {
            return;
        }
        self.rows_in.fetch_add(rows_in as u64, Ordering::Relaxed);
        self.rows_out.fetch_add(rows_out as u64, Ordering::Relaxed);
        self.skippable_rows
            .fetch_add(skippable as u64, Ordering::Relaxed);
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

/// Measures the conjuncts of one batch of the post-scan filter in the
/// positions of its input batch.
///
/// The post-scan filter evaluates its conjuncts one after the other and
/// compacts the working batch to the rows that are still live (see
/// `PostScanFilter`). Windows of the compacted rows are not windows of the
/// decoded rows, thus a conjunct after a compaction must not count its
/// skippable rows on its own result.
///
/// A conjunct that moves to the row filter runs after the row filter
/// predicates and before all post-scan conjuncts: its input is the input
/// batch of the post-scan filter, not the rows that the earlier post-scan
/// conjuncts let pass. Thus each conjunct is measured on the input batch.
/// Before the first compaction it was evaluated on all rows of the input
/// batch and the measurement is exact. After a compaction, it was not
/// evaluated on the rows that the compaction removed; these rows count as
/// passing. This counts too few skippable rows (a conservative
/// approximation: the conjunct is a row filter only if its measured saving
/// pays for it). The first conjunct is always exact.
///
/// Only a window whose rows are all in the working batch can be skippable.
/// Thus this keeps these windows and their first row in the working batch,
/// and the cost is O(rows / 64) for each conjunct and compaction.
#[derive(Debug)]
pub(crate) struct StageSelection {
    input_rows: usize,
    /// The windows of the input batch whose rows are all in the working
    /// batch, as (rows of the window, first row in the working batch).
    /// `None` if the working batch is the input batch (not compacted).
    complete_windows: Option<Vec<(usize, usize)>>,
}

impl StageSelection {
    /// For an input batch of `input_rows` rows.
    pub(crate) fn new(input_rows: usize) -> Self {
        Self {
            input_rows,
            complete_windows: None,
        }
    }

    /// Records in `stats` the evaluation of a conjunct with the result
    /// `passed` (without nulls) for each row of the working batch.
    pub(crate) fn record(&self, stats: &ConjunctStats, passed: &BooleanArray) {
        let values = passed.values();
        match &self.complete_windows {
            None => stats.record(
                self.input_rows,
                values.count_set_bits(),
                skippable_in(values),
            ),
            Some(windows) => {
                // The rows that the conjunct did not see pass.
                let unseen = self.input_rows - values.len();
                let skippable = windows
                    .iter()
                    .filter(|(len, start)| {
                        values.slice(*start, *len).count_set_bits() == 0
                    })
                    .map(|(len, _)| len)
                    .sum();
                stats.record(
                    self.input_rows,
                    unseen + values.count_set_bits(),
                    skippable,
                );
            }
        }
    }

    /// The working batch is compacted to the rows of `live` (a selection of
    /// the rows of the working batch, without nulls).
    pub(crate) fn compact(&mut self, live: &BooleanArray) {
        let live = live.values();
        let windows: Vec<(usize, usize)> = match self.complete_windows.take() {
            Some(windows) => windows,
            None => (0..self.input_rows)
                .step_by(SKIP_WINDOW_ROWS)
                .map(|start| (SKIP_WINDOW_ROWS.min(self.input_rows - start), start))
                .collect(),
        };
        // `rank` is the number of live rows before `position`: the row of
        // the new working batch at `position`.
        let mut rank = 0;
        let mut position = 0;
        let mut complete = Vec::with_capacity(windows.len());
        for (len, start) in windows {
            rank += live.slice(position, start - position).count_set_bits();
            position = start;
            if live.slice(start, len).count_set_bits() == len {
                complete.push((len, rank));
            }
        }
        self.complete_windows = Some(complete);
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
    /// Sites with a decode speed of `decode_ns_per_byte` and a fetch
    /// latency of `fetch_ns`, weighted so that the measurements of a test
    /// do not change them, for deterministic placement decisions.
    #[cfg(test)]
    pub(crate) fn with_fixed_costs(decode_ns_per_byte: f64, fetch_ns: f64) -> Self {
        const WEIGHT: u64 = 1 << 40;
        let sites = Self::default();
        let weighted = |ns: f64| (ns * WEIGHT as f64) as u64;
        sites
            .decode
            .nanos
            .store(weighted(decode_ns_per_byte), Ordering::Relaxed);
        sites.decode.bytes.store(WEIGHT, Ordering::Relaxed);
        sites
            .fetch
            .nanos
            .store(weighted(fetch_ns), Ordering::Relaxed);
        sites.fetch.fetches.store(WEIGHT, Ordering::Relaxed);
        sites
    }

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

    /// A conjunct after a compaction is measured in the positions of the
    /// input batch, with the rows that it did not see as passing.
    #[test]
    fn stage_selection_measures_on_input_positions() {
        let rows = 256;
        let mut selection = StageSelection::new(rows);
        // Conjunct 1 keeps every fourth row: no empty window. Exact.
        let first: BooleanArray = (0..rows).map(|i| Some(i % 4 == 0)).collect();
        let first_stats = ConjunctStats::default();
        selection.record(&first_stats, &first);
        assert_eq!(
            first_stats.observation(),
            Observation {
                rows_in: 256,
                rows_out: 64,
                skippable_rows: 0,
                ..Default::default()
            }
        );
        // The working batch is compacted to the 64 live rows.
        selection.compact(&first);
        // Conjunct 2 fails the live rows of input rows 0..128 (the first 32
        // working rows). The rows that conjunct 1 removed count as passing,
        // thus no window is empty.
        let second: BooleanArray = (0..64).map(|i| Some(i >= 32)).collect();
        let second_stats = ConjunctStats::default();
        selection.record(&second_stats, &second);
        assert_eq!(
            second_stats.observation(),
            Observation {
                rows_in: 256,
                rows_out: 256 - 32,
                skippable_rows: 0,
                ..Default::default()
            }
        );
    }

    /// Before a compaction, a conjunct is measured exactly on all rows,
    /// also after an earlier conjunct.
    #[test]
    fn stage_selection_is_exact_before_compaction() {
        let selection = StageSelection::new(256);
        // Fails input rows 0..128: two empty windows.
        let passed: BooleanArray = (0..256).map(|i| Some(i >= 128)).collect();
        let stats = ConjunctStats::default();
        selection.record(&stats, &passed);
        assert_eq!(
            stats.observation(),
            Observation {
                rows_in: 256,
                rows_out: 128,
                skippable_rows: 128,
                ..Default::default()
            }
        );
    }

    /// A window whose rows are all in the working batch after a compaction
    /// is measured on the rows of the working batch.
    #[test]
    fn stage_selection_keeps_complete_windows() {
        let mut selection = StageSelection::new(256);
        // Removes input rows 0..64 and the odd rows of 64..128.
        let first: BooleanArray = (0..256)
            .map(|i| Some(i >= 128 || (i >= 64 && i % 2 == 0)))
            .collect();
        selection.compact(&first);
        // 160 working rows: 32 rows of window 1, then windows 2 and 3
        // (working rows 32..96 and 96..160). Fails working rows 32..96 and
        // one row of window 3.
        let second: BooleanArray = (0..160)
            .map(|i| Some(!(32..96).contains(&i) && i != 100))
            .collect();
        let stats = ConjunctStats::default();
        selection.record(&stats, &second);
        assert_eq!(
            stats.observation(),
            Observation {
                rows_in: 256,
                rows_out: 256 - 65,
                skippable_rows: 64,
                ..Default::default()
            }
        );
        // Compact again: window 2 has no live row and window 3 lost a row,
        // thus no window is complete. Nothing is skippable any more.
        selection.compact(&second);
        let third: BooleanArray = (0..95).map(|i| Some(i >= 64)).collect();
        let stats = ConjunctStats::default();
        selection.record(&stats, &third);
        assert_eq!(stats.observation().skippable_rows, 0);
        assert_eq!(stats.observation().rows_out, 256 - 64);
    }

    /// Two compactions: the second maps through the first.
    #[test]
    fn stage_selection_composes_compactions() {
        let mut selection = StageSelection::new(256);
        // Keep the even rows, then (of those) the ones in the upper half.
        let even: BooleanArray = (0..256).map(|i| Some(i % 2 == 0)).collect();
        selection.compact(&even);
        let upper: BooleanArray = (0..128).map(|i| Some(i >= 64)).collect();
        selection.compact(&upper);
        // 64 working rows: the even rows of 128..256. A conjunct that fails
        // all of them leaves the odd rows (not seen) passing.
        let none: BooleanArray = (0..64).map(|_| Some(false)).collect();
        let stats = ConjunctStats::default();
        selection.record(&stats, &none);
        assert_eq!(
            stats.observation(),
            Observation {
                rows_in: 256,
                rows_out: 192,
                skippable_rows: 0,
                ..Default::default()
            }
        );
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
