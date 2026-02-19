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

use std::collections::HashMap;
use std::sync::Arc;

use datafusion_common::instant::Instant;
use log::debug;
use parking_lot::RwLock;

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
///   accumulating `effective_min_rows` new rows.
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
/// Correlated filters are deduplicated: only the most effective filter
/// from each correlated group is promoted; the rest are demoted to post-scan.
#[derive(Debug, Clone, Default)]
pub(crate) struct PartitionedFilters {
    /// Filters still collecting stats — will become a CollectingArrowPredicate
    pub(crate) collecting: Vec<(FilterId, Arc<dyn PhysicalExpr>)>,
    /// Filters promoted past collection — individual chained ArrowPredicates
    pub(crate) promoted: Vec<(FilterId, Arc<dyn PhysicalExpr>)>,
    /// Filters demoted to post-scan (fast path only)
    pub(crate) post_scan: Vec<(FilterId, Arc<dyn PhysicalExpr>)>,
}

/// Joint selectivity statistics for a pair of filters A and B.
#[derive(Debug, Clone, Default)]
struct CorrelationStats {
    rows_both_passed: u64,
    rows_total: u64,
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

    /// Number of rows that matched (passed) the filter.
    fn rows_matched(&self) -> u64 {
        self.rows_matched
    }

    /// Total number of rows evaluated.
    fn rows_total(&self) -> u64 {
        self.rows_total
    }

    /// Cumulative evaluation time in nanoseconds.
    fn eval_nanos(&self) -> u64 {
        self.eval_nanos
    }

    /// Returns the selectivity (fraction of rows filtered out).
    ///
    /// - 1.0 = perfect filter (all rows filtered out)
    /// - 0.0 = useless filter (no rows filtered out)
    ///
    /// Returns 0.0 if no rows have been evaluated (unknown selectivity).
    fn selectivity(&self) -> f64 {
        if self.rows_total == 0 {
            0.0 // Unknown, assume no filtering
        } else {
            1.0 - (self.rows_matched as f64 / self.rows_total as f64)
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

/// Canonical pair key for correlation: always (min, max).
#[derive(Debug, Clone, Copy, Hash, PartialEq, Eq)]
struct PairKey(FilterId, FilterId);

impl PairKey {
    fn new(a: FilterId, b: FilterId) -> Self {
        if a <= b { Self(a, b) } else { Self(b, a) }
    }
}

/// Immutable configuration for a [`SelectivityTracker`].
///
/// Use the builder methods to customise, then call [`build()`](TrackerConfig::build)
/// to produce a ready-to-use tracker.
pub struct TrackerConfig {
    /// Minimum bytes/sec throughput for promoting a filter (default: INFINITY = disabled).
    pub min_bytes_per_sec: f64,
    /// Lift ratio above which two filters are considered correlated (default: 1.5).
    pub correlation_threshold: f64,
    /// Floor for the collection / re-evaluation window (default: 10 000).
    pub min_rows_for_collection: u64,
    /// Fraction of total dataset rows for the collection window (default: 0.0).
    pub collection_fraction: f64,
    /// Cap on collection / re-evaluation window size (default: 0 = no cap).
    pub max_rows_for_collection: u64,
}

impl TrackerConfig {
    pub fn new() -> Self {
        Self {
            min_bytes_per_sec: f64::INFINITY,
            correlation_threshold: 1.5,
            min_rows_for_collection: 10_000,
            collection_fraction: 0.0,
            max_rows_for_collection: 0,
        }
    }

    pub fn with_min_bytes_per_sec(mut self, v: f64) -> Self {
        self.min_bytes_per_sec = v;
        self
    }

    pub fn with_correlation_threshold(mut self, v: f64) -> Self {
        self.correlation_threshold = v;
        self
    }

    pub fn with_min_rows_for_collection(mut self, v: u64) -> Self {
        self.min_rows_for_collection = v;
        self
    }

    pub fn with_collection_fraction(mut self, v: f64) -> Self {
        self.collection_fraction = v;
        self
    }

    pub fn with_max_rows_for_collection(mut self, v: u64) -> Self {
        self.max_rows_for_collection = v;
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
        let lock_wait_us = {
            let lock_start = Instant::now();
            let mut inner = self.inner.write();
            let wait = lock_start.elapsed().as_micros();
            inner.update(id, matched, total, eval_nanos, batch_bytes);
            wait
        }; // lock dropped here
        if lock_wait_us > 100 {
            debug!("FilterId {id}: selectivity write lock wait={lock_wait_us}μs");
        }
    }

    /// Partition filters into collecting / promoted / post-scan.
    pub(crate) fn partition_filters(
        &self,
        filters: Vec<(FilterId, Arc<dyn PhysicalExpr>)>,
    ) -> PartitionedFilters {
        self.inner.write().partition_filters(filters, &self.config)
    }

    /// Returns the number of times `partition_filters` has been called.
    pub(crate) fn partition_call_count(&self) -> u64 {
        self.inner.read().partition_call_count
    }

    /// Resolve the fraction-based collection threshold from dataset statistics.
    pub(crate) fn notify_dataset_rows(&self, total_rows: u64) {
        self.inner
            .write()
            .notify_dataset_rows(total_rows, &self.config);
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

    /// Set the lift ratio above which two filters are considered correlated.
    pub(crate) fn with_correlation_threshold(self, v: f64) -> Self {
        Self {
            config: TrackerConfig {
                correlation_threshold: v,
                ..self.config
            },
            ..self
        }
    }

    /// Set the floor for the collection / re-evaluation window.
    pub(crate) fn with_min_rows_for_collection(self, v: u64) -> Self {
        Self {
            config: TrackerConfig {
                min_rows_for_collection: v,
                ..self.config
            },
            ..self
        }
    }

    /// Set the fraction of total dataset rows used for the collection window.
    pub(crate) fn with_collection_fraction(self, v: f64) -> Self {
        Self {
            config: TrackerConfig {
                collection_fraction: v,
                ..self.config
            },
            ..self
        }
    }

    /// Set the cap on collection / re-evaluation window size.
    pub(crate) fn with_max_rows_for_collection(self, v: u64) -> Self {
        Self {
            config: TrackerConfig {
                max_rows_for_collection: v,
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
    /// Pairwise correlation statistics between filter pairs
    correlations: HashMap<PairKey, CorrelationStats>,
    /// Snapshot generation for each filter (for detecting dynamic filter updates)
    snapshot_generations: HashMap<FilterId, u64>,
    /// Incrementing counter for partition_filters calls (for log context).
    partition_call_count: u64,
    /// Resolved minimum rows after notify_dataset_rows() is called.
    resolved_min_rows: Option<u64>,
}

impl SelectivityTrackerInner {
    fn new() -> Self {
        Self {
            stats: HashMap::new(),
            filter_states: HashMap::new(),
            correlations: HashMap::new(),
            snapshot_generations: HashMap::new(),
            partition_call_count: 0,
            resolved_min_rows: None,
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

    /// Compute the effective collection / re-evaluation window size.
    fn effective_min_rows(&self, config: &TrackerConfig) -> u64 {
        let base = self
            .resolved_min_rows
            .unwrap_or(config.min_rows_for_collection);
        if config.max_rows_for_collection > 0 {
            base.min(config.max_rows_for_collection)
        } else {
            base
        }
    }

    /// Resolve the fraction-based collection threshold from dataset statistics.
    fn notify_dataset_rows(&mut self, total_rows: u64, config: &TrackerConfig) {
        if config.collection_fraction > 0.0 {
            let fraction_rows = (config.collection_fraction * total_rows as f64) as u64;
            self.resolved_min_rows =
                Some(config.min_rows_for_collection.max(fraction_rows));
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
                self.correlations.retain(|pk, _| pk.0 != id && pk.1 != id);
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

    /// Partition filters using correlation-based deduplication.
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
                promoted = filters;
            }
            PromotionStrategy::Threshold(threshold) => {
                let min_rows = self.effective_min_rows(config);
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

                    // Active states: Collecting, Promoted, or new (None)
                    let has_enough = self
                        .stats
                        .get(&id)
                        .is_some_and(|s| s.rows_total >= min_rows);

                    if has_enough {
                        match self.stats.get(&id).and_then(|s| s.effectiveness()) {
                            Some(eff) if eff >= threshold => {
                                // Clear stats if transitioning from Collecting/new
                                if matches!(state, None | Some(FilterState::Collecting)) {
                                    debug!(
                                        "FilterId {id}: Collecting → Promoted at {eff:.0} bytes/sec — {expr}"
                                    );
                                    self.stats.remove(&id);
                                }
                                self.filter_states.insert(id, FilterState::Promoted);
                                promoted.push((id, expr));
                            }
                            _ => {
                                self.demote_or_drop(id, &expr, &mut post_scan);
                            }
                        }
                    } else {
                        // Not enough rows yet
                        if state == Some(FilterState::Promoted) {
                            // Grace period: keep promoted
                            promoted.push((id, expr));
                        } else {
                            // Early promotion via confidence interval
                            let early_promote = self
                                .stats
                                .get(&id)
                                .and_then(|s| s.confidence_lower_bound(2.0))
                                .is_some_and(|lb| lb >= threshold);

                            if early_promote {
                                debug!(
                                    "FilterId {id}: early promotion via CI — lower bound >= {threshold:.0} bytes/sec — {expr}"
                                );
                                self.stats.remove(&id);
                                self.filter_states.insert(id, FilterState::Promoted);
                                promoted.push((id, expr));
                            } else {
                                if state.is_none() {
                                    self.filter_states
                                        .insert(id, FilterState::Collecting);
                                }
                                collecting.push((id, expr));
                            }
                        }
                    }
                }
            }
            PromotionStrategy::Disabled => unreachable!(),
        }

        // Deduplicate correlated filters among promoted only
        let groups = self.group_by_correlation_id(&promoted, config);

        let mut deduped_promoted: Vec<(FilterId, Arc<dyn PhysicalExpr>)> = Vec::new();
        for indices in groups {
            if indices.len() == 1 {
                deduped_promoted.push(promoted[indices[0]].clone());
            } else {
                let best_idx = indices
                    .iter()
                    .copied()
                    .max_by(|&a, &b| {
                        let eff_a =
                            self.get_effectiveness_by_id(promoted[a].0).unwrap_or(0.0);
                        let eff_b =
                            self.get_effectiveness_by_id(promoted[b].0).unwrap_or(0.0);
                        eff_a
                            .partial_cmp(&eff_b)
                            .unwrap_or(std::cmp::Ordering::Equal)
                    })
                    .unwrap();
                for &i in &indices {
                    if i == best_idx {
                        deduped_promoted.push(promoted[i].clone());
                    } else {
                        let (id, ref expr) = promoted[i];
                        if expr
                            .as_any()
                            .downcast_ref::<OptionalFilterPhysicalExpr>()
                            .is_none()
                        {
                            self.filter_states.insert(id, FilterState::Demoted);
                            post_scan.push(promoted[i].clone());
                        } else {
                            self.stats.remove(&id);
                            self.filter_states.insert(id, FilterState::Dropped);
                        }
                    }
                }
            }
        }
        let promoted = deduped_promoted;

        // Sort promoted by effectiveness descending
        let mut promoted = promoted;
        promoted.sort_by(|a, b| {
            let eff_a = self.get_effectiveness_by_id(a.0).unwrap_or(0.0);
            let eff_b = self.get_effectiveness_by_id(b.0).unwrap_or(0.0);
            eff_b
                .partial_cmp(&eff_a)
                .unwrap_or(std::cmp::Ordering::Equal)
        });

        // Diagnostic logging
        self.partition_call_count += 1;
        if log::log_enabled!(log::Level::Debug) {
            let min_rows = self.effective_min_rows(config);
            let threshold_str = match strategy {
                PromotionStrategy::Threshold(t) => format!("{t:.0} bytes/sec"),
                PromotionStrategy::AllPromoted => "all promoted".to_string(),
                PromotionStrategy::Disabled => "disabled".to_string(),
            };
            debug!(
                "Filter partition #{}: strategy={:?}, threshold={}, collection_min_rows={}",
                self.partition_call_count, strategy, threshold_str, min_rows,
            );
            for &(id, ref expr) in &collecting {
                let rows_so_far = self.stats.get(&id).map_or(0, |s| s.rows_total());
                let remaining = min_rows.saturating_sub(rows_so_far);
                debug!(
                    "  Filter id={id}: {expr} [COLLECTING as row-filter (need {remaining} more rows)]"
                );
            }
            for &(id, ref expr) in &promoted {
                let detail = match self.stats.get(&id) {
                    Some(s) if s.rows_total() >= min_rows => {
                        let eff = s.effectiveness();
                        let eval_ms = s.eval_nanos() as f64 / 1_000_000.0;
                        let eff_str =
                            eff.map_or_else(|| "N/A".to_string(), |v| format!("{v:.0}"));
                        format!(
                            "rows={}/{} matched, selectivity={:.4}, eval_time={eval_ms:.2}ms, bytes_seen={}, effectiveness={eff_str} bytes/sec → PROMOTED (re-confirmed, >= {threshold_str})",
                            s.rows_matched(),
                            s.rows_total(),
                            s.selectivity(),
                            s.bytes_seen,
                        )
                    }
                    _ => {
                        let rows_so_far =
                            self.stats.get(&id).map_or(0, |s| s.rows_total());
                        let remaining = min_rows.saturating_sub(rows_so_far);
                        format!(
                            "PROMOTED (grace period, re-eval after {remaining} more rows)"
                        )
                    }
                };
                debug!("  Filter id={id}: {expr} [{detail}]");
            }
            for &(id, ref expr) in &post_scan {
                let state = self.filter_states.get(&id).copied();
                let detail = match state {
                    Some(FilterState::Collecting) | None => {
                        let rows_so_far =
                            self.stats.get(&id).map_or(0, |s| s.rows_total());
                        let remaining = min_rows.saturating_sub(rows_so_far);
                        let label = if state.is_none() {
                            "COLLECTING (new)"
                        } else {
                            "COLLECTING"
                        };
                        match self.stats.get(&id) {
                            Some(s) => {
                                let eff = s.effectiveness();
                                let eval_ms = s.eval_nanos() as f64 / 1_000_000.0;
                                let eff_str = eff.map_or_else(
                                    || "N/A".to_string(),
                                    |v| format!("{v:.0}"),
                                );
                                format!(
                                    "rows={}/{} matched, selectivity={:.4}, eval_time={eval_ms:.2}ms, bytes_seen={}, effectiveness={eff_str} bytes/sec → {label} (need {remaining} more rows)",
                                    s.rows_matched(),
                                    s.rows_total(),
                                    s.selectivity(),
                                    s.bytes_seen,
                                )
                            }
                            None => format!("{label} (need {remaining} more rows)"),
                        }
                    }
                    Some(FilterState::Demoted) => match self.stats.get(&id) {
                        Some(s) => {
                            let eff = s.effectiveness();
                            let eval_ms = s.eval_nanos() as f64 / 1_000_000.0;
                            let eff_str = eff
                                .map_or_else(|| "N/A".to_string(), |v| format!("{v:.0}"));
                            format!(
                                "rows={}/{} matched, selectivity={:.4}, eval_time={eval_ms:.2}ms, bytes_seen={}, effectiveness={eff_str} bytes/sec → DEMOTED (below {threshold_str})",
                                s.rows_matched(),
                                s.rows_total(),
                                s.selectivity(),
                                s.bytes_seen,
                            )
                        }
                        None => format!("DEMOTED (below {threshold_str})"),
                    },
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

    /// Compute the correlation ratio for two filters by ID.
    fn correlation_ratio_by_id(
        &self,
        a: FilterId,
        b: FilterId,
        config: &TrackerConfig,
    ) -> Option<f64> {
        let stats_a = self.stats.get(&a)?;
        let stats_b = self.stats.get(&b)?;

        let min_rows = self.effective_min_rows(config);
        if stats_a.rows_total < min_rows || stats_b.rows_total < min_rows {
            return None;
        }

        let pair_key = PairKey::new(a, b);
        let pair_stats = self.correlations.get(&pair_key)?;

        if pair_stats.rows_total < min_rows {
            return None;
        }

        let p_a = stats_a.rows_matched as f64 / stats_a.rows_total as f64;
        let p_b = stats_b.rows_matched as f64 / stats_b.rows_total as f64;

        let p_a_times_p_b = p_a * p_b;
        if p_a_times_p_b < 1e-10 {
            return None;
        }

        let p_ab = pair_stats.rows_both_passed as f64 / pair_stats.rows_total as f64;

        Some(p_ab / p_a_times_p_b)
    }

    /// Group effective filters by correlation using union-find.
    fn group_by_correlation_id(
        &self,
        filters: &[(FilterId, Arc<dyn PhysicalExpr>)],
        config: &TrackerConfig,
    ) -> Vec<Vec<usize>> {
        let n = filters.len();
        if n <= 1 {
            return (0..n).map(|i| vec![i]).collect();
        }

        let mut parent: Vec<usize> = (0..n).collect();
        let mut rank: Vec<usize> = vec![0; n];

        for i in 0..n {
            for j in (i + 1)..n {
                if let Some(ratio) =
                    self.correlation_ratio_by_id(filters[i].0, filters[j].0, config)
                    && ratio > config.correlation_threshold
                {
                    union(&mut parent, &mut rank, i, j);
                }
            }
        }

        let mut components: HashMap<usize, Vec<usize>> = HashMap::new();
        for i in 0..n {
            let root = find(&mut parent, i);
            components.entry(root).or_default().push(i);
        }

        components.into_values().collect()
    }
}

/// Union-find: find with path compression
fn find(parent: &mut [usize], i: usize) -> usize {
    if parent[i] != i {
        parent[i] = find(parent, parent[i]);
    }
    parent[i]
}

/// Union-find: union by rank
fn union(parent: &mut [usize], rank: &mut [usize], a: usize, b: usize) {
    let root_a = find(parent, a);
    let root_b = find(parent, b);
    if root_a == root_b {
        return;
    }
    match rank[root_a].cmp(&rank[root_b]) {
        std::cmp::Ordering::Less => parent[root_a] = root_b,
        std::cmp::Ordering::Greater => parent[root_b] = root_a,
        std::cmp::Ordering::Equal => {
            parent[root_b] = root_a;
            rank[root_a] += 1;
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
        /// Test helper: directly insert correlation stats for a pair of filters.
        fn update_correlation(
            &mut self,
            a: FilterId,
            b: FilterId,
            both_passed: u64,
            total: u64,
        ) {
            let pair_key = PairKey::new(a, b);
            let entry = self.correlations.entry(pair_key).or_default();
            entry.rows_both_passed += both_passed;
            entry.rows_total += total;
        }

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
    fn make_tracker(
        min_bytes_per_sec: f64,
        min_rows: u64,
    ) -> (TrackerConfig, SelectivityTrackerInner) {
        let config = TrackerConfig::new()
            .with_min_bytes_per_sec(min_bytes_per_sec)
            .with_min_rows_for_collection(min_rows);
        let inner = SelectivityTrackerInner::new();
        (config, inner)
    }

    mod stats {
        use super::*;

        #[test]
        fn selectivity_fraction_cases() {
            let stats = SelectivityStats::new(0, 0, 0, 0);
            assert_eq!(stats.selectivity(), 0.0);

            let stats = SelectivityStats::new(100, 100, 0, 0);
            assert_eq!(stats.selectivity(), 0.0);

            let stats = SelectivityStats::new(0, 100, 0, 0);
            assert_eq!(stats.selectivity(), 1.0);

            let stats = SelectivityStats::new(20, 100, 0, 0);
            assert_eq!(stats.selectivity(), 0.8);

            let stats = SelectivityStats::new(50, 100, 0, 0);
            assert_eq!(stats.selectivity(), 0.5);
        }

        #[test]
        fn effectiveness_bytes_per_sec_cases() {
            let stats = SelectivityStats::new(0, 0, 0, 0);
            assert!(stats.effectiveness().is_none());

            let stats = SelectivityStats::new(20, 100, 0, 1000);
            assert!(stats.effectiveness().is_none());

            let stats = SelectivityStats::new(20, 100, 1_000_000_000, 0);
            assert!(stats.effectiveness().is_none());

            let stats = SelectivityStats::new(20, 100, 1_000_000_000, 1000);
            let eff = stats.effectiveness().unwrap();
            assert!((eff - 800.0).abs() < 0.001);

            let stats = SelectivityStats::new(100, 100, 1_000_000_000, 1000);
            let eff = stats.effectiveness().unwrap();
            assert!((eff - 0.0).abs() < 0.001);
        }

        #[test]
        fn update_accumulates_across_batches() {
            let mut stats = SelectivityStats::default();
            assert_eq!(stats.rows_matched(), 0);
            assert_eq!(stats.rows_total(), 0);
            assert_eq!(stats.eval_nanos(), 0);

            stats.update(20, 100, 500, 1000);
            assert_eq!(stats.rows_matched(), 20);
            assert_eq!(stats.rows_total(), 100);
            assert_eq!(stats.eval_nanos(), 500);

            stats.update(30, 100, 300, 1000);
            assert_eq!(stats.rows_matched(), 50);
            assert_eq!(stats.rows_total(), 200);
            assert_eq!(stats.eval_nanos(), 800);
            assert_eq!(stats.selectivity(), 0.75);
        }
    }

    mod partitioning {
        use super::*;

        #[test]
        fn all_promoted_pushes_every_filter() {
            let (config, mut inner) = make_tracker(0.0, 100);

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
            let (config, mut inner) = make_tracker(100.0, 100);

            let filter1 = make_filter("a", 5);
            let filter2 = make_filter("a", 10);
            let optional_filter = make_optional_filter("c", 1);

            inner.update(0, 20, 100, 1_000_000_000, 1000);
            inner.update(1, 90, 100, 10_000_000_000, 1000);
            inner.update(2, 10, 100, 1_000_000_000, 10_000);

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
            let (config, mut inner) = make_tracker(f64::INFINITY, 100);
            let filter = make_filter("a", 5);
            inner.update(0, 0, 100, 1, 1000);

            let result = inner.partition_filters(vec![(0, filter.clone())], &config);

            assert!(result.promoted.is_empty());
            assert_eq!(result.post_scan.len(), 1);
        }

        #[test]
        fn correlated_pair_keeps_best_demotes_other() {
            let (config, mut inner) = make_tracker(0.0, 100);

            let filter_a = make_filter("a", 5);
            let filter_b = make_filter("a", 10);

            inner.update(0, 30, 100, 0, 0);
            inner.update(1, 40, 100, 0, 0);
            inner.update_correlation(0, 1, 25, 100);

            let result = inner.partition_filters(
                vec![(0, filter_a.clone()), (1, filter_b.clone())],
                &config,
            );

            assert_eq!(result.promoted.len(), 1);
            assert_eq!(result.post_scan.len(), 1);
        }

        #[test]
        fn two_correlated_groups_each_keep_best() {
            let (config, mut inner) = make_tracker(0.0, 100);

            let filter_a = make_filter("a", 5);
            let filter_b = make_filter("a", 10);
            let filter_c = make_filter("c", 1);
            let filter_d = make_filter("d", 2);

            inner.update(0, 30, 100, 0, 0);
            inner.update(1, 40, 100, 0, 0);
            inner.update(2, 20, 100, 0, 0);
            inner.update(3, 35, 100, 0, 0);

            inner.update_correlation(0, 1, 25, 100);
            inner.update_correlation(2, 3, 15, 100);
            inner.update_correlation(0, 2, 6, 100);
            inner.update_correlation(1, 2, 8, 100);
            inner.update_correlation(0, 3, 10, 100);
            inner.update_correlation(1, 3, 14, 100);

            let result = inner.partition_filters(
                vec![
                    (0, filter_a.clone()),
                    (1, filter_b.clone()),
                    (2, filter_c.clone()),
                    (3, filter_d.clone()),
                ],
                &config,
            );

            assert_eq!(result.promoted.len(), 2);
            assert_eq!(result.post_scan.len(), 2);
        }

        #[test]
        fn ineffective_optional_dropped_mandatory_demoted() {
            let (config, mut inner) = make_tracker(100.0, 100);

            let mandatory_filter = make_filter("a", 5);
            let optional_filter = make_optional_filter("c", 1);

            inner.update(0, 50, 100, 100_000_000_000, 10_000);
            inner.update(1, 50, 100, 100_000_000_000, 10_000);

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
        fn new_filters_stay_collecting_until_min_rows() {
            let (config, mut inner) = make_tracker(100.0, 10_000);

            let filter_a = make_filter("a", 5);
            let filter_b = make_filter("a", 10);

            let result = inner.partition_filters(
                vec![(0, filter_a.clone()), (1, filter_b.clone())],
                &config,
            );

            assert!(result.promoted.is_empty());
            assert_eq!(result.collecting.len(), 2);
            assert!(result.post_scan.is_empty());
        }

        #[test]
        fn effective_min_rows_respects_fraction_floor_and_cap() {
            // Fraction increases effective_min_rows above the floor
            let config = TrackerConfig::new()
                .with_min_bytes_per_sec(0.0)
                .with_min_rows_for_collection(100)
                .with_collection_fraction(0.05);
            let mut inner = SelectivityTrackerInner::new();
            assert_eq!(inner.effective_min_rows(&config), 100);
            inner.notify_dataset_rows(10_000, &config);
            assert_eq!(inner.effective_min_rows(&config), 500);

            // Floor wins when fraction result is smaller
            let config2 = TrackerConfig::new()
                .with_min_bytes_per_sec(0.0)
                .with_min_rows_for_collection(1000)
                .with_collection_fraction(0.05);
            let mut inner2 = SelectivityTrackerInner::new();
            inner2.notify_dataset_rows(10_000, &config2);
            assert_eq!(inner2.effective_min_rows(&config2), 1000);

            // Cap limits effective_min_rows
            let config3 = TrackerConfig::new()
                .with_min_bytes_per_sec(0.0)
                .with_min_rows_for_collection(100)
                .with_collection_fraction(0.05)
                .with_max_rows_for_collection(500);
            let mut inner3 = SelectivityTrackerInner::new();
            inner3.notify_dataset_rows(1_000_000, &config3);
            assert_eq!(inner3.effective_min_rows(&config3), 500);
        }

        #[test]
        fn filters_evaluated_independently_as_data_arrives() {
            let (config, mut inner) = make_tracker(100.0, 100);

            let filter_a = make_filter("a", 5);
            let filter_b = make_filter("a", 10);

            inner.update(0, 10, 100, 1_000_000_000, 10_000);
            inner.update(1, 5, 50, 500_000_000, 5_000);

            let result = inner.partition_filters(
                vec![(0, filter_a.clone()), (1, filter_b.clone())],
                &config,
            );
            assert_eq!(result.promoted.len(), 1, "filter0 should be promoted");
            assert_eq!(result.promoted[0].0, 0);
            assert_eq!(result.collecting.len(), 1, "filter1 still collecting");
            assert_eq!(result.collecting[0].0, 1);

            inner.update(1, 5, 50, 500_000_000, 5_000);
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
            let (config, mut inner) = make_tracker(100.0, 10_000);

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
            let (config, mut inner) = make_tracker(500.0, 10_000);

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
            let (config, mut inner) = make_tracker(100.0, 10_000);

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
            let (config, mut inner) = make_tracker(100.0, 10_000);

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
            let (_config, mut inner) = make_tracker(100.0, 100);

            inner.update(0, 10, 100, 1_000_000_000, 10_000);

            inner.note_generation(0, 1);
            inner.note_generation(0, 2);

            assert!(inner.needs_collection(0));
            assert!(!inner.stats.contains_key(&0));
        }

        #[test]
        fn collecting_to_promoted_clears_stats() {
            let (config, mut inner) = make_tracker(100.0, 100);

            let filter_a = make_filter("a", 5);

            inner.update(0, 10, 100, 1_000_000_000, 10_000);
            assert!(inner.stats.contains_key(&0));
            assert_eq!(inner.stats.get(&0).unwrap().rows_total(), 100);

            let result = inner.partition_filters(vec![(0, filter_a.clone())], &config);
            assert_eq!(result.promoted.len(), 1);

            assert!(
                !inner.stats.contains_key(&0),
                "Stats should be cleared on Collecting→Promoted transition"
            );
        }

        #[test]
        fn grace_period_then_demotion_on_low_effectiveness() {
            let (config, mut inner) = make_tracker(100.0, 100);

            let filter_a = make_filter("a", 5);
            let filter_b = make_filter("a", 10);

            inner.update(0, 20, 100, 1_000_000_000, 1000);

            let result = inner.partition_filters(
                vec![(0, filter_a.clone()), (1, filter_b.clone())],
                &config,
            );
            assert_eq!(result.promoted.len(), 1);
            assert_eq!(result.promoted[0].0, 0);

            let result = inner.partition_filters(
                vec![(0, filter_a.clone()), (1, filter_b.clone())],
                &config,
            );
            assert!(
                result.promoted.iter().any(|(id, _)| *id == 0),
                "filter 0 should remain in row_filters during grace period"
            );

            for _ in 0..5 {
                inner.update(0, 100, 100, 1_000_000_000, 1000);
            }

            let result = inner.partition_filters(
                vec![(0, filter_a.clone()), (1, filter_b.clone())],
                &config,
            );
            assert!(
                !result.promoted.iter().any(|(id, _)| *id == 0),
                "filter 0 should be demoted after accumulating enough low-effectiveness data"
            );
        }

        #[test]
        fn demoted_filter_stays_demoted_across_partitions() {
            let (config, mut inner) = make_tracker(100.0, 100);

            let filter_a = make_filter("a", 5);

            inner.update(0, 100, 100, 1_000_000_000, 1000);

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

        #[test]
        fn three_filters_transitive_correlation_grouping() {
            let (config, mut inner) = make_tracker(0.0, 100);

            let filter_a = make_filter("a", 5);
            let filter_b = make_filter("a", 10);
            let filter_c = make_filter("c", 1);

            inner.update(0, 30, 100, 1_000_000_000, 10_000);
            inner.update(1, 50, 100, 500_000_000, 10_000);
            inner.update(2, 40, 100, 1_000_000_000, 10_000);

            inner.update_correlation(0, 1, 28, 100);
            inner.update_correlation(1, 2, 38, 100);
            inner.update_correlation(0, 2, 12, 100);

            let result = inner.partition_filters(
                vec![
                    (0, filter_a.clone()),
                    (1, filter_b.clone()),
                    (2, filter_c.clone()),
                ],
                &config,
            );

            assert_eq!(
                result.promoted.len(),
                1,
                "transitive correlation should group all three, promoting only the best"
            );
            assert_eq!(
                result.post_scan.len(),
                2,
                "the other two should be demoted to post-scan"
            );
        }
    }
}
