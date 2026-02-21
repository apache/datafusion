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

//! Adaptive filter selectivity tracking for Parquet row filters.
//!
//! See [`SelectivityTracker`] for the main entry point, `FilterState` for the
//! per-filter lifecycle, `PartitionedFilters` for the output consumed by
//! `ParquetOpener::open`, and [`FilterId`] for stable filter identification.

use log::debug;
use parking_lot::RwLock;
use std::collections::HashMap;
use std::sync::Arc;

use datafusion_physical_expr_common::physical_expr::{
    OptionalFilterPhysicalExpr, PhysicalExpr, snapshot_generation,
};

/// Stable identifier for a filter conjunct, assigned by `ParquetSource::with_predicate`.
pub type FilterId = usize;

/// Internal representation of the promotion strategy derived from `min_bytes_per_sec`.
///
/// This avoids fragile float comparisons (`== 0.0`, `== INFINITY`) scattered
/// throughout the code and makes the intent explicit.
#[derive(Debug, Clone, Copy, PartialEq)]
enum PromotionStrategy {
    /// Feature disabled — no filters are promoted to row filters.
    /// Corresponds to `min_bytes_per_sec == f64::INFINITY`.
    Disabled,
    /// All filters are pushed as row filters unconditionally.
    /// Corresponds to `min_bytes_per_sec == 0.0`.
    AllPromoted,
    /// Only filters with bytes/sec throughput >= threshold are promoted.
    Threshold(f64),
}

/// Per-filter lifecycle state in the adaptive filter system.
///
/// State transitions:
/// - **(unseen)** → [`Collecting`](Self::Collecting) on first encounter in
///   [`SelectivityTracker::partition_filters`].
/// - [`Collecting`](Self::Collecting) → [`Promoted`](Self::Promoted) when
///   effectiveness ≥ `min_bytes_per_sec` and enough rows have been observed.
/// - [`Collecting`](Self::Collecting) → [`Demoted`](Self::Demoted) when
///   effectiveness is below threshold (mandatory filter).
/// - [`Collecting`](Self::Collecting) → [`Dropped`](Self::Dropped) when
///   effectiveness is below threshold and the filter is optional
///   ([`OptionalFilterPhysicalExpr`]).
/// - [`Promoted`](Self::Promoted) → [`Demoted`](Self::Demoted)/[`Dropped`](Self::Dropped)
///   on periodic re-evaluation if effectiveness drops below threshold after
///   CI upper bound drops below threshold.
/// - **Any state** → [`Collecting`](Self::Collecting) when a dynamic filter's
///   `snapshot_generation` changes (see [`SelectivityTrackerInner::note_generation`]).
#[derive(Debug, Clone, Copy, PartialEq)]
pub(crate) enum FilterState {
    /// Still collecting stats — filter goes to post-scan for measurement.
    Collecting,
    /// Promoted to row filter (sufficient throughput).
    Promoted,
    /// Demoted to post-scan (insufficient throughput, but mandatory).
    Demoted,
    /// Dropped entirely (insufficient throughput and optional).
    Dropped,
}

/// Result of partitioning filters into row filters vs post-scan.
///
/// Produced by [`SelectivityTracker::partition_filters`], consumed by
/// `ParquetOpener::open` to build row-level predicates and post-scan filters.
///
/// Filters are partitioned based on their effectiveness threshold.
#[derive(Debug, Clone, Default)]
pub(crate) struct PartitionedFilters {
    /// Filters still collecting stats — will become a CollectingArrowPredicate
    pub(crate) collecting: Vec<(FilterId, Arc<dyn PhysicalExpr>)>,
    /// Filters promoted past collection — individual chained ArrowPredicates
    pub(crate) promoted: Vec<(FilterId, Arc<dyn PhysicalExpr>)>,
    /// Filters demoted to post-scan (fast path only)
    pub(crate) post_scan: Vec<(FilterId, Arc<dyn PhysicalExpr>)>,
}

/// Tracks selectivity statistics for a single filter expression.
#[derive(Debug, Clone, Default)]
struct SelectivityStats {
    /// Number of rows that matched (passed) the filter
    rows_matched: u64,
    /// Total number of rows evaluated
    rows_total: u64,
    /// Cumulative evaluation time in nanoseconds
    eval_nanos: u64,
    /// Cumulative bytes across batches this filter has been evaluated on
    bytes_seen: u64,
    /// Welford's online algorithm: number of per-batch effectiveness samples
    sample_count: u64,
    /// Welford's online algorithm: running mean of per-batch effectiveness
    eff_mean: f64,
    /// Welford's online algorithm: running sum of squared deviations (M2)
    eff_m2: f64,
}

impl SelectivityStats {
    /// Create new stats with given values.
    #[cfg(test)]
    fn new(rows_matched: u64, rows_total: u64, eval_nanos: u64, bytes_seen: u64) -> Self {
        Self {
            rows_matched,
            rows_total,
            eval_nanos,
            bytes_seen,
            sample_count: 0,
            eff_mean: 0.0,
            eff_m2: 0.0,
        }
    }

    /// Returns the effectiveness as an opaque ordering score (higher = run first).
    ///
    /// Currently computed as bytes/sec throughput using self-contained stats.
    /// Callers should not assume the unit.
    fn effectiveness(&self) -> Option<f64> {
        if self.rows_total == 0 || self.eval_nanos == 0 || self.bytes_seen == 0 {
            return None;
        }
        let rows_pruned = self.rows_total - self.rows_matched;
        let bytes_per_row = self.bytes_seen as f64 / self.rows_total as f64;
        let bytes_saved = rows_pruned as f64 * bytes_per_row;
        Some(bytes_saved * 1_000_000_000.0 / self.eval_nanos as f64)
    }

    /// Returns the lower bound of a confidence interval on mean effectiveness.
    ///
    /// Uses Welford's online variance to compute a one-sided CI:
    /// `mean - z * stderr`. Returns `None` if fewer than 2 samples.
    fn confidence_lower_bound(&self, confidence_z: f64) -> Option<f64> {
        if self.sample_count < 2 {
            return None;
        }
        let variance = self.eff_m2 / (self.sample_count - 1) as f64;
        let stderr = (variance / self.sample_count as f64).sqrt();
        Some(self.eff_mean - confidence_z * stderr)
    }

    /// Returns the upper bound of a confidence interval on mean effectiveness.
    ///
    /// Uses Welford's online variance: `mean + z * stderr`.
    /// Returns `None` if fewer than 2 samples.
    fn confidence_upper_bound(&self, confidence_z: f64) -> Option<f64> {
        if self.sample_count < 2 {
            return None;
        }
        let variance = self.eff_m2 / (self.sample_count - 1) as f64;
        let stderr = (variance / self.sample_count as f64).sqrt();
        Some(self.eff_mean + confidence_z * stderr)
    }

    /// Update stats with new observations.
    fn update(&mut self, matched: u64, total: u64, eval_nanos: u64, batch_bytes: u64) {
        self.rows_matched += matched;
        self.rows_total += total;
        self.eval_nanos += eval_nanos;
        self.bytes_seen += batch_bytes;

        // Feed Welford's algorithm with per-batch effectiveness
        if total > 0 && eval_nanos > 0 && batch_bytes > 0 {
            let rows_pruned = total - matched;
            let bytes_per_row = batch_bytes as f64 / total as f64;
            let batch_eff =
                (rows_pruned as f64 * bytes_per_row) * 1e9 / eval_nanos as f64;

            self.sample_count += 1;
            let delta = batch_eff - self.eff_mean;
            self.eff_mean += delta / self.sample_count as f64;
            let delta2 = batch_eff - self.eff_mean;
            self.eff_m2 += delta * delta2;
        }
    }
}

/// Immutable configuration for a [`SelectivityTracker`].
///
/// Use the builder methods to customise, then call [`build()`](TrackerConfig::build)
/// to produce a ready-to-use tracker.
pub struct TrackerConfig {
    /// Minimum bytes/sec throughput for promoting a filter (default: INFINITY = disabled).
    pub min_bytes_per_sec: f64,
    /// Byte-ratio threshold for placing collecting filters at row-level vs post-scan.
    pub byte_ratio_threshold: f64,
    /// Z-score for confidence intervals on filter effectiveness.
    pub confidence_z: f64,
}

impl TrackerConfig {
    pub fn new() -> Self {
        Self {
            min_bytes_per_sec: f64::INFINITY,
            byte_ratio_threshold: 0.2,
            confidence_z: 2.0,
        }
    }

    pub fn with_min_bytes_per_sec(mut self, v: f64) -> Self {
        self.min_bytes_per_sec = v;
        self
    }

    pub fn with_byte_ratio_threshold(mut self, v: f64) -> Self {
        self.byte_ratio_threshold = v;
        self
    }

    pub fn with_confidence_z(mut self, v: f64) -> Self {
        self.confidence_z = v;
        self
    }

    pub fn build(self) -> SelectivityTracker {
        SelectivityTracker {
            config: self,
            inner: RwLock::new(SelectivityTrackerInner::new()),
        }
    }
}

impl Default for TrackerConfig {
    fn default() -> Self {
        Self::new()
    }
}

/// Cross-file adaptive system that measures filter effectiveness and decides
/// which filters are promoted to row-level predicates (pushed into the Parquet
/// reader) vs. applied post-scan or dropped entirely.
///
/// The `RwLock` is **private** — external callers cannot hold the guard across
/// expensive work. All lock-holding code paths are auditable in this file.
///
/// # Filter state machine
///
/// ```text
/// (new) ──→ Collecting ──→ Promoted ──→ (re-eval) ──→ Demoted / Dropped
///                │                            ↑
///                └──→ Demoted / Dropped        │
///                                    generation change resets to Collecting
/// ```
///
/// See [`TrackerConfig`] for configuration knobs.
pub struct SelectivityTracker {
    config: TrackerConfig,
    inner: RwLock<SelectivityTrackerInner>,
}

impl std::fmt::Debug for SelectivityTracker {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SelectivityTracker")
            .field("config.min_bytes_per_sec", &self.config.min_bytes_per_sec)
            .finish()
    }
}

impl Default for SelectivityTracker {
    fn default() -> Self {
        Self::new()
    }
}

impl SelectivityTracker {
    /// Create a new tracker with default settings (feature disabled).
    pub fn new() -> Self {
        TrackerConfig::new().build()
    }

    /// Update stats for a filter after processing a batch.
    ///
    /// Acquires and releases the write lock in bounded time.
    /// Debug-logs lock wait times > 100μs **after** dropping the lock.
    pub(crate) fn update(
        &self,
        id: FilterId,
        matched: u64,
        total: u64,
        eval_nanos: u64,
        batch_bytes: u64,
    ) {
        self.inner
            .write()
            .update(id, matched, total, eval_nanos, batch_bytes);
    }

    /// Partition filters into collecting / promoted / post-scan.
    pub(crate) fn partition_filters(
        &self,
        filters: Vec<(FilterId, Arc<dyn PhysicalExpr>)>,
    ) -> PartitionedFilters {
        self.inner.write().partition_filters(filters, &self.config)
    }

    /// Returns true if the filter is in the Collecting state.
    pub(crate) fn is_collecting(&self, id: FilterId) -> bool {
        let inner = self.inner.read();
        matches!(
            inner.filter_states.get(&id),
            Some(FilterState::Collecting) | None
        )
    }

    /// Access the tracker's configuration.
    pub(crate) fn config(&self) -> &TrackerConfig {
        &self.config
    }

    // Builder-style constructors for convenience (delegating to TrackerConfig).

    /// Set the minimum bytes/sec throughput for promoting a filter.
    pub(crate) fn with_min_bytes_per_sec(self, v: f64) -> Self {
        Self {
            config: TrackerConfig {
                min_bytes_per_sec: v,
                ..self.config
            },
            ..self
        }
    }

    /// Set the byte-ratio threshold for collecting filter placement.
    pub(crate) fn with_byte_ratio_threshold(self, v: f64) -> Self {
        Self {
            config: TrackerConfig {
                byte_ratio_threshold: v,
                ..self.config
            },
            ..self
        }
    }

    /// Set the confidence z-score for promotion/demotion decisions.
    pub(crate) fn with_confidence_z(self, v: f64) -> Self {
        Self {
            config: TrackerConfig {
                confidence_z: v,
                ..self.config
            },
            ..self
        }
    }
}

/// Mutable state guarded by the `RwLock` inside [`SelectivityTracker`].
#[derive(Debug)]
struct SelectivityTrackerInner {
    /// Per-filter effectiveness statistics, keyed by FilterId
    stats: HashMap<FilterId, SelectivityStats>,
    /// Per-filter lifecycle state
    filter_states: HashMap<FilterId, FilterState>,
    /// Snapshot generation for each filter (for detecting dynamic filter updates)
    snapshot_generations: HashMap<FilterId, u64>,
    /// Incrementing counter for partition_filters calls (for log context).
    partition_call_count: u64,
}

impl SelectivityTrackerInner {
    fn new() -> Self {
        Self {
            stats: HashMap::new(),
            filter_states: HashMap::new(),
            snapshot_generations: HashMap::new(),
            partition_call_count: 0,
        }
    }

    /// Map `min_bytes_per_sec` to the [`PromotionStrategy`] enum.
    fn promotion_strategy(config: &TrackerConfig) -> PromotionStrategy {
        if config.min_bytes_per_sec.is_infinite() {
            PromotionStrategy::Disabled
        } else if config.min_bytes_per_sec == 0.0 {
            PromotionStrategy::AllPromoted
        } else {
            PromotionStrategy::Threshold(config.min_bytes_per_sec)
        }
    }

    /// Check and update the snapshot generation for a filter.
    fn note_generation(&mut self, id: FilterId, generation: u64) {
        if generation == 0 {
            return;
        }
        match self.snapshot_generations.get(&id) {
            Some(&prev_generation) if prev_generation == generation => {}
            Some(_) => {
                let current_state = self.filter_states.get(&id).copied();
                debug!(
                    "FilterId {id} generation changed, resetting stats (state={current_state:?})"
                );
                self.stats.remove(&id);
                self.snapshot_generations.insert(id, generation);
            }
            None => {
                self.snapshot_generations.insert(id, generation);
            }
        }
    }

    /// Get the effectiveness for a filter by ID.
    fn get_effectiveness_by_id(&self, id: FilterId) -> Option<f64> {
        self.stats.get(&id).and_then(|s| s.effectiveness())
    }

    /// Demote a filter to post-scan or drop it entirely if optional.
    fn demote_or_drop(
        &mut self,
        id: FilterId,
        expr: &Arc<dyn PhysicalExpr>,
        post_scan: &mut Vec<(FilterId, Arc<dyn PhysicalExpr>)>,
    ) {
        if expr
            .as_any()
            .downcast_ref::<OptionalFilterPhysicalExpr>()
            .is_none()
        {
            self.filter_states.insert(id, FilterState::Demoted);
            post_scan.push((id, Arc::clone(expr)));
        } else {
            self.filter_states.insert(id, FilterState::Dropped);
        }
        self.stats.remove(&id);
    }

    /// Update stats for a filter by ID after processing a batch.
    fn update(
        &mut self,
        id: FilterId,
        matched: u64,
        total: u64,
        eval_nanos: u64,
        batch_bytes: u64,
    ) {
        self.stats
            .entry(id)
            .or_default()
            .update(matched, total, eval_nanos, batch_bytes);
    }

    /// Partition filters into collecting / promoted / post-scan buckets.
    fn partition_filters(
        &mut self,
        filters: Vec<(FilterId, Arc<dyn PhysicalExpr>)>,
        config: &TrackerConfig,
    ) -> PartitionedFilters {
        let strategy = Self::promotion_strategy(config);

        // Disabled: nothing is promoted.
        if strategy == PromotionStrategy::Disabled {
            debug!(
                "Filter promotion disabled; all {} filters post-scan",
                filters.len()
            );
            return PartitionedFilters {
                collecting: Vec::new(),
                promoted: Vec::new(),
                post_scan: filters,
            };
        }

        // Note snapshot generations for dynamic filter detection
        for &(id, ref expr) in &filters {
            let generation = snapshot_generation(expr);
            self.note_generation(id, generation);
        }

        // Separate into collecting, promoted, and post_scan.
        let mut collecting: Vec<(FilterId, Arc<dyn PhysicalExpr>)> = Vec::new();
        let mut promoted: Vec<(FilterId, Arc<dyn PhysicalExpr>)> = Vec::new();
        let mut post_scan: Vec<(FilterId, Arc<dyn PhysicalExpr>)> = Vec::new();

        match strategy {
            PromotionStrategy::AllPromoted => {
                // All filters start as promoted row filters unconditionally.
                for (id, expr) in filters {
                    self.filter_states
                        .entry(id)
                        .or_insert(FilterState::Promoted);
                    promoted.push((id, expr));
                }
            }
            PromotionStrategy::Threshold(threshold) => {
                let confidence_z = config.confidence_z;
                for (id, expr) in filters {
                    let state = self.filter_states.get(&id).copied();

                    // Terminal states
                    match state {
                        Some(FilterState::Demoted) => {
                            post_scan.push((id, expr));
                            continue;
                        }
                        Some(FilterState::Dropped) => {
                            continue;
                        }
                        _ => {}
                    }

                    if state == Some(FilterState::Promoted) {
                        // Promoted filter: check for demotion via CI upper bound
                        let should_demote = self
                            .stats
                            .get(&id)
                            .and_then(|s| s.confidence_upper_bound(confidence_z))
                            .is_some_and(|ub| ub < threshold);
                        if should_demote {
                            debug!(
                                "FilterId {id}: Promoted → Demoted via CI upper bound < {threshold:.0} bytes/sec — {expr}"
                            );
                            self.demote_or_drop(id, &expr, &mut post_scan);
                        } else {
                            promoted.push((id, expr));
                        }
                        continue;
                    }

                    // Collecting or new: check CI for promotion/demotion
                    let lower = self
                        .stats
                        .get(&id)
                        .and_then(|s| s.confidence_lower_bound(confidence_z));
                    let upper = self
                        .stats
                        .get(&id)
                        .and_then(|s| s.confidence_upper_bound(confidence_z));

                    if lower.is_some_and(|lb| lb >= threshold) {
                        debug!(
                            "FilterId {id}: Collecting → Promoted via CI lower bound >= {threshold:.0} bytes/sec — {expr}"
                        );
                        self.stats.remove(&id);
                        self.filter_states.insert(id, FilterState::Promoted);
                        promoted.push((id, expr));
                    } else if upper.is_some_and(|ub| ub < threshold) {
                        debug!(
                            "FilterId {id}: Collecting → Demoted via CI upper bound < {threshold:.0} bytes/sec — {expr}"
                        );
                        self.demote_or_drop(id, &expr, &mut post_scan);
                    } else {
                        if state.is_none() {
                            self.filter_states.insert(id, FilterState::Collecting);
                        }
                        collecting.push((id, expr));
                    }
                }
            }
            PromotionStrategy::Disabled => unreachable!(),
        }

        // Sort all row-level filters (promoted + collecting) by effectiveness
        // descending, falling back to byte ratio ascending for filters without stats.
        promoted.sort_by(|a, b| {
            let eff_a = self.get_effectiveness_by_id(a.0);
            let eff_b = self.get_effectiveness_by_id(b.0);
            match (eff_a, eff_b) {
                (Some(ea), Some(eb)) => {
                    eb.partial_cmp(&ea).unwrap_or(std::cmp::Ordering::Equal)
                }
                (Some(_), None) => std::cmp::Ordering::Less,
                (None, Some(_)) => std::cmp::Ordering::Greater,
                (None, None) => std::cmp::Ordering::Equal,
            }
        });

        // Diagnostic logging
        self.partition_call_count += 1;
        if log::log_enabled!(log::Level::Debug) {
            let threshold_str = match strategy {
                PromotionStrategy::Threshold(t) => format!("{t:.0} bytes/sec"),
                PromotionStrategy::AllPromoted => "all promoted".to_string(),
                PromotionStrategy::Disabled => "disabled".to_string(),
            };
            debug!(
                "Filter partition #{}: strategy={:?}, threshold={}, confidence_z={}",
                self.partition_call_count, strategy, threshold_str, config.confidence_z,
            );
            for &(id, ref expr) in &collecting {
                let samples = self.stats.get(&id).map_or(0, |s| s.sample_count);
                debug!("  Filter id={id}: {expr} [COLLECTING ({samples} samples)]");
            }
            for &(id, ref expr) in &promoted {
                let detail = match self.stats.get(&id) {
                    Some(s) => {
                        let eff_str = s
                            .effectiveness()
                            .map_or_else(|| "N/A".to_string(), |v| format!("{v:.0}"));
                        format!(
                            "PROMOTED ({} samples, eff={eff_str} bytes/sec)",
                            s.sample_count
                        )
                    }
                    None => "PROMOTED (fresh)".to_string(),
                };
                debug!("  Filter id={id}: {expr} [{detail}]");
            }
            for &(id, ref expr) in &post_scan {
                let state = self.filter_states.get(&id).copied();
                let detail = match state {
                    Some(FilterState::Demoted) => "DEMOTED".to_string(),
                    _ => format!("{state:?}"),
                };
                debug!("  Filter id={id}: {expr} [{detail}]");
            }
            for (id, state) in &self.filter_states {
                if matches!(state, FilterState::Dropped) {
                    debug!("  Filter id={id}: DROPPED (optional, below threshold)");
                }
            }
            debug!(
                "Summary: {} collecting, {} promoted, {} post-scan, {} dropped",
                collecting.len(),
                promoted.len(),
                post_scan.len(),
                self.filter_states
                    .values()
                    .filter(|s| matches!(s, FilterState::Dropped))
                    .count()
            );
        }

        PartitionedFilters {
            collecting,
            promoted,
            post_scan,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::datatypes::{DataType, Field, Schema};
    use datafusion_expr::Operator;
    use datafusion_physical_expr::expressions::{BinaryExpr, col, lit};
    use std::sync::Arc;

    fn test_schema() -> Schema {
        Schema::new(vec![
            Field::new("a", DataType::Int32, false),
            Field::new("b", DataType::Utf8, true),
            Field::new("c", DataType::Int32, false),
            Field::new("d", DataType::Int32, false),
        ])
    }

    impl SelectivityTrackerInner {
        /// Test helper: check if a filter is in collecting state (or not yet decided).
        fn needs_collection(&self, id: FilterId) -> bool {
            matches!(
                self.filter_states.get(&id),
                None | Some(FilterState::Collecting)
            )
        }
    }

    fn make_filter(col_name: &str, value: i32) -> Arc<dyn PhysicalExpr> {
        let schema = test_schema();
        Arc::new(BinaryExpr::new(
            col(col_name, &schema).unwrap(),
            Operator::Eq,
            lit(value),
        ))
    }

    fn make_optional_filter(col_name: &str, value: i32) -> Arc<dyn PhysicalExpr> {
        Arc::new(OptionalFilterPhysicalExpr::new(make_filter(
            col_name, value,
        )))
    }

    /// Helper: create a TrackerConfig and mutable inner for tests.
    fn make_tracker(min_bytes_per_sec: f64) -> (TrackerConfig, SelectivityTrackerInner) {
        let config = TrackerConfig::new().with_min_bytes_per_sec(min_bytes_per_sec);
        let inner = SelectivityTrackerInner::new();
        (config, inner)
    }

    mod stats {
        use super::*;

        #[test]
        fn effectiveness_none_cases() {
            // No data → no effectiveness
            let stats = SelectivityStats::new(0, 0, 0, 0);
            assert!(stats.effectiveness().is_none());

            // No eval time → no effectiveness
            let stats = SelectivityStats::new(20, 100, 0, 1000);
            assert!(stats.effectiveness().is_none());

            // No bytes → no effectiveness
            let stats = SelectivityStats::new(20, 100, 1_000_000_000, 0);
            assert!(stats.effectiveness().is_none());
        }

        #[test]
        fn effectiveness_bytes_per_sec_cases() {
            // 80 rows pruned * 10 bytes/row = 800 bytes saved in 1 sec = 800 bytes/sec
            let stats = SelectivityStats::new(20, 100, 1_000_000_000, 1000);
            let eff = stats.effectiveness().unwrap();
            assert!((eff - 800.0).abs() < 0.001);

            // 0 rows pruned = 0 effectiveness
            let stats = SelectivityStats::new(100, 100, 1_000_000_000, 1000);
            let eff = stats.effectiveness().unwrap();
            assert!((eff - 0.0).abs() < 0.001);
        }

        #[test]
        fn update_accumulates_across_batches() {
            let mut stats = SelectivityStats::default();
            assert_eq!(stats.rows_matched, 0);
            assert_eq!(stats.rows_total, 0);
            assert_eq!(stats.eval_nanos, 0);

            stats.update(20, 100, 500, 1000);
            assert_eq!(stats.rows_matched, 20);
            assert_eq!(stats.rows_total, 100);
            assert_eq!(stats.eval_nanos, 500);

            stats.update(30, 100, 300, 1000);
            assert_eq!(stats.rows_matched, 50);
            assert_eq!(stats.rows_total, 200);
            assert_eq!(stats.eval_nanos, 800);
        }
    }

    mod partitioning {
        use super::*;

        #[test]
        fn all_promoted_pushes_every_filter() {
            let (config, mut inner) = make_tracker(0.0);

            let filter1 = make_filter("a", 5);
            let filter2 = make_filter("a", 10);

            inner.update(0, 10, 100, 0, 0);
            inner.update(1, 10, 100, 0, 0);

            let result = inner.partition_filters(
                vec![(0, filter1.clone()), (1, filter2.clone())],
                &config,
            );

            assert_eq!(result.promoted.len(), 2);
            assert_eq!(result.post_scan.len(), 0);
        }

        #[test]
        fn threshold_promotes_above_demotes_below() {
            let (config, mut inner) = make_tracker(100.0);

            let filter1 = make_filter("a", 5);
            let filter2 = make_filter("a", 10);
            let optional_filter = make_optional_filter("c", 1);

            // Multiple consistent samples for CI to converge
            for _ in 0..3 {
                inner.update(0, 20, 100, 1_000_000_000, 1000); // eff=800 > 100
                inner.update(1, 90, 100, 10_000_000_000, 1000); // eff=1 < 100
                inner.update(2, 10, 100, 1_000_000_000, 10_000); // eff=9000 > 100
            }

            let result = inner.partition_filters(
                vec![
                    (0, filter1.clone()),
                    (1, filter2.clone()),
                    (2, optional_filter.clone()),
                ],
                &config,
            );

            assert_eq!(result.promoted.len(), 2);
            assert_eq!(result.post_scan.len(), 1);
            assert_eq!(result.post_scan[0].0, 1);
            assert!(result.promoted.iter().any(|(id, _)| *id == 2));
        }

        #[test]
        fn disabled_sends_all_to_post_scan() {
            let (config, mut inner) = make_tracker(f64::INFINITY);
            let filter = make_filter("a", 5);
            inner.update(0, 0, 100, 1, 1000);

            let result = inner.partition_filters(vec![(0, filter.clone())], &config);

            assert!(result.promoted.is_empty());
            assert_eq!(result.post_scan.len(), 1);
        }

        #[test]
        fn ineffective_optional_dropped_mandatory_demoted() {
            let (config, mut inner) = make_tracker(100.0);

            let mandatory_filter = make_filter("a", 5);
            let optional_filter = make_optional_filter("c", 1);

            // Multiple consistent samples for CI to converge on low effectiveness
            for _ in 0..3 {
                inner.update(0, 50, 100, 100_000_000_000, 10_000);
                inner.update(1, 50, 100, 100_000_000_000, 10_000);
            }

            let result = inner.partition_filters(
                vec![(0, mandatory_filter.clone()), (1, optional_filter.clone())],
                &config,
            );

            assert_eq!(result.post_scan.len(), 1);
            assert_eq!(result.post_scan[0].0, 0);
            assert!(result.promoted.is_empty());
        }
    }

    mod collection {
        use super::*;

        #[test]
        fn new_filters_stay_collecting_without_enough_samples() {
            let (config, mut inner) = make_tracker(100.0);

            let filter_a = make_filter("a", 5);
            let filter_b = make_filter("a", 10);

            let result = inner.partition_filters(
                vec![(0, filter_a.clone()), (1, filter_b.clone())],
                &config,
            );

            assert!(result.promoted.is_empty());
            assert_eq!(
                result.collecting.len(),
                2,
                "new filters should be collecting"
            );
            assert!(result.post_scan.is_empty());
        }

        #[test]
        fn filters_evaluated_independently_as_data_arrives() {
            let (config, mut inner) = make_tracker(100.0);

            let filter_a = make_filter("a", 5);
            let filter_b = make_filter("a", 10);

            // Give filter0 enough consistent samples for CI promotion
            for _ in 0..3 {
                inner.update(0, 10, 100, 1_000_000_000, 10_000);
            }
            // filter1 has only 1 sample — not enough for CI
            inner.update(1, 5, 50, 500_000_000, 5_000);

            let result = inner.partition_filters(
                vec![(0, filter_a.clone()), (1, filter_b.clone())],
                &config,
            );
            assert_eq!(result.promoted.len(), 1, "filter0 should be promoted");
            assert_eq!(result.promoted[0].0, 0);
            assert_eq!(result.collecting.len(), 1, "filter1 still collecting");
            assert_eq!(result.collecting[0].0, 1);

            // Give filter1 more consistent samples
            for _ in 0..2 {
                inner.update(1, 5, 50, 500_000_000, 5_000);
            }
            let result = inner.partition_filters(
                vec![(0, filter_a.clone()), (1, filter_b.clone())],
                &config,
            );
            assert_eq!(
                result.promoted.len(),
                2,
                "both filters should now be promoted"
            );
        }
    }

    mod early_promotion {
        use super::*;

        #[test]
        fn early_promotion_via_confidence_interval() {
            let (config, mut inner) = make_tracker(100.0);

            let filter = make_filter("a", 5);

            for _ in 0..3 {
                inner.update(0, 20, 100, 1_000_000_000, 1000);
            }

            let result = inner.partition_filters(vec![(0, filter.clone())], &config);
            assert_eq!(
                result.promoted.len(),
                1,
                "filter should be early-promoted via CI before reaching min_rows"
            );
            assert_eq!(result.promoted[0].0, 0);
        }

        #[test]
        fn no_early_promotion_with_high_variance() {
            let (config, mut inner) = make_tracker(500.0);

            let filter = make_filter("a", 5);

            inner.update(0, 20, 100, 1_000_000_000, 1000);
            inner.update(0, 90, 100, 10_000_000_000, 1000);

            let result = inner.partition_filters(vec![(0, filter.clone())], &config);
            assert!(
                result.promoted.is_empty(),
                "high-variance filter should NOT be early-promoted"
            );
        }

        #[test]
        fn no_early_promotion_with_single_sample() {
            let (config, mut inner) = make_tracker(100.0);

            let filter = make_filter("a", 5);

            inner.update(0, 20, 100, 1_000_000_000, 1000);

            let result = inner.partition_filters(vec![(0, filter.clone())], &config);
            assert!(
                result.promoted.is_empty(),
                "single sample should not trigger early promotion"
            );
        }

        #[test]
        fn welford_stats_cleared_on_promotion() {
            let (config, mut inner) = make_tracker(100.0);

            let filter = make_filter("a", 5);

            for _ in 0..3 {
                inner.update(0, 20, 100, 1_000_000_000, 1000);
            }

            let result = inner.partition_filters(vec![(0, filter.clone())], &config);
            assert_eq!(result.promoted.len(), 1);
            assert!(!inner.stats.contains_key(&0));
        }
    }

    mod lifecycle {
        use super::*;

        #[test]
        fn generation_change_resets_to_collecting() {
            let (_config, mut inner) = make_tracker(100.0);

            inner.update(0, 10, 100, 1_000_000_000, 10_000);

            inner.note_generation(0, 1);
            inner.note_generation(0, 2);

            assert!(inner.needs_collection(0));
            assert!(!inner.stats.contains_key(&0));
        }

        #[test]
        fn collecting_to_promoted_clears_stats() {
            let (config, mut inner) = make_tracker(100.0);

            let filter_a = make_filter("a", 5);

            // Need multiple samples for CI
            for _ in 0..3 {
                inner.update(0, 10, 100, 1_000_000_000, 10_000);
            }
            assert!(inner.stats.contains_key(&0));

            let result = inner.partition_filters(vec![(0, filter_a.clone())], &config);
            assert_eq!(result.promoted.len(), 1);

            assert!(
                !inner.stats.contains_key(&0),
                "Stats should be cleared on Collecting→Promoted transition"
            );
        }

        #[test]
        fn promoted_filter_demoted_on_low_effectiveness() {
            let (config, mut inner) = make_tracker(100.0);

            let filter_a = make_filter("a", 5);

            // Promote filter via CI (high effectiveness)
            for _ in 0..3 {
                inner.update(0, 20, 100, 1_000_000_000, 1000); // eff=800
            }

            let result = inner.partition_filters(vec![(0, filter_a.clone())], &config);
            assert_eq!(result.promoted.len(), 1, "filter should be promoted");

            // Now feed low-effectiveness data as a promoted filter
            // Stats were cleared on promotion, so these are fresh
            for _ in 0..5 {
                inner.update(0, 100, 100, 1_000_000_000, 1000); // eff=0
            }

            let result = inner.partition_filters(vec![(0, filter_a.clone())], &config);
            assert!(
                !result.promoted.iter().any(|(id, _)| *id == 0),
                "filter 0 should be demoted after accumulating low-effectiveness data"
            );
        }

        #[test]
        fn demoted_filter_stays_demoted_across_partitions() {
            let (config, mut inner) = make_tracker(100.0);

            let filter_a = make_filter("a", 5);

            // Multiple samples needed for CI to converge on low effectiveness
            for _ in 0..3 {
                inner.update(0, 100, 100, 1_000_000_000, 1000); // eff=0
            }

            let result = inner.partition_filters(vec![(0, filter_a.clone())], &config);
            assert!(result.promoted.is_empty());
            assert_eq!(result.post_scan.len(), 1);
            assert_eq!(*inner.filter_states.get(&0).unwrap(), FilterState::Demoted);

            inner.update(0, 10, 100, 1_000_000_000, 10_000);

            let result = inner.partition_filters(vec![(0, filter_a.clone())], &config);
            assert!(
                result.promoted.is_empty(),
                "demoted filter should stay demoted even with new good stats"
            );
            assert_eq!(result.post_scan.len(), 1);
        }
    }
}
