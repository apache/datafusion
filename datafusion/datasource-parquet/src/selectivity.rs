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
//! This module provides infrastructure to track filter effectiveness across files
//! and adaptively decide which filters should be pushed down as row filters vs.
//! applied post-scan.
//!
//! Filters are identified by a stable `FilterId` (usize index) assigned when the
//! predicate is first split into conjuncts in `ParquetSource::with_predicate`.
//! This avoids ExprKey mismatch issues when expressions are rebased or simplified.

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use log::debug;

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

/// Per-filter lifecycle state.
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
/// Correlated filters are deduplicated: only the most effective filter
/// from each correlated group is promoted; the rest are demoted to post-scan.
#[derive(Debug, Clone, Default)]
pub(crate) struct PartitionedFilters {
    /// Filters to push down as row filters (one ArrowPredicate each).
    pub(crate) row_filters: Vec<(FilterId, Arc<dyn PhysicalExpr>)>,
    /// Filters to apply post-scan (ineffective or in collection phase)
    pub(crate) post_scan: Vec<(FilterId, Arc<dyn PhysicalExpr>)>,
}

/// Cached partitioning decision to avoid re-evaluating promotion/demotion
/// on every call to `partition_filters`.
///
/// The decision is reused until `effective_min_rows()` additional rows have
/// been observed, at which point a fresh evaluation is triggered.
#[derive(Debug)]
struct CachedDecision {
    /// FilterIds promoted to row filters
    promoted: HashSet<FilterId>,
    /// FilterIds explicitly sent to post_scan
    demoted: HashSet<FilterId>,
    /// `total_file_rows` when the decision was made
    decided_at_rows: u64,
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

    /// Update stats with new observations.
    fn update(&mut self, matched: u64, total: u64, eval_nanos: u64, batch_bytes: u64) {
        self.rows_matched += matched;
        self.rows_total += total;
        self.eval_nanos += eval_nanos;
        self.bytes_seen += batch_bytes;
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

/// Cross-file selectivity tracker for adaptive filter ordering.
///
/// Filters are identified by stable `FilterId` indices assigned at predicate
/// construction time. This avoids ExprKey mismatch when expressions are
/// rebased or simplified per-file.
#[derive(Debug)]
pub(crate) struct SelectivityTracker {
    /// Per-filter effectiveness statistics, keyed by FilterId
    stats: HashMap<FilterId, SelectivityStats>,
    /// Per-filter lifecycle state
    filter_states: HashMap<FilterId, FilterState>,
    /// Pairwise correlation statistics between filter pairs
    correlations: HashMap<PairKey, CorrelationStats>,
    /// Snapshot generation for each filter (for detecting dynamic filter updates)
    snapshot_generations: HashMap<FilterId, u64>,
    /// Minimum bytes/sec throughput for promoting a filter to row filter.
    min_bytes_per_sec: f64,
    /// Correlation ratio threshold for grouping filters.
    correlation_threshold: f64,
    /// Minimum rows that must be observed before collection phase ends.
    min_rows_for_collection: u64,
    /// Fraction of total dataset rows for collection phase.
    collection_fraction: f64,
    /// Maximum rows for collection phase (0 = no cap).
    max_rows_for_collection: u64,
    /// Resolved minimum rows after notify_dataset_rows() is called.
    resolved_min_rows: Option<u64>,
    /// Monotonic counter of total rows observed (for cache invalidation).
    total_rows_observed: u64,
    /// Cached partitioning decision.
    cached_decision: Option<CachedDecision>,
}

impl Default for SelectivityTracker {
    fn default() -> Self {
        Self::new()
    }
}

impl SelectivityTracker {
    /// Create a new tracker with default settings (feature disabled).
    pub(crate) fn new() -> Self {
        Self {
            stats: HashMap::new(),
            filter_states: HashMap::new(),
            correlations: HashMap::new(),
            snapshot_generations: HashMap::new(),
            min_bytes_per_sec: f64::INFINITY,
            correlation_threshold: 1.5,
            min_rows_for_collection: 10_000,
            collection_fraction: 0.0,
            max_rows_for_collection: 0,
            resolved_min_rows: None,
            total_rows_observed: 0,
            cached_decision: None,
        }
    }

    pub(crate) fn with_min_bytes_per_sec(mut self, min_bytes_per_sec: f64) -> Self {
        self.min_bytes_per_sec = min_bytes_per_sec;
        self
    }

    pub(crate) fn with_correlation_threshold(
        mut self,
        correlation_threshold: f64,
    ) -> Self {
        self.correlation_threshold = correlation_threshold;
        self
    }

    pub(crate) fn with_min_rows_for_collection(mut self, min_rows: u64) -> Self {
        self.min_rows_for_collection = min_rows;
        self
    }

    pub(crate) fn with_collection_fraction(mut self, fraction: f64) -> Self {
        self.collection_fraction = fraction;
        self
    }

    pub(crate) fn with_max_rows_for_collection(mut self, max_rows: u64) -> Self {
        self.max_rows_for_collection = max_rows;
        self
    }

    fn promotion_strategy(&self) -> PromotionStrategy {
        if self.min_bytes_per_sec.is_infinite() {
            PromotionStrategy::Disabled
        } else if self.min_bytes_per_sec == 0.0 {
            PromotionStrategy::AllPromoted
        } else {
            PromotionStrategy::Threshold(self.min_bytes_per_sec)
        }
    }

    fn effective_min_rows(&self) -> u64 {
        let base = self
            .resolved_min_rows
            .unwrap_or(self.min_rows_for_collection);
        if self.max_rows_for_collection > 0 {
            base.min(self.max_rows_for_collection)
        } else {
            base
        }
    }

    pub(crate) fn notify_dataset_rows(&mut self, total_rows: u64) {
        if self.collection_fraction > 0.0 {
            let fraction_rows = (self.collection_fraction * total_rows as f64) as u64;
            self.resolved_min_rows =
                Some(self.min_rows_for_collection.max(fraction_rows));
        }
    }

    /// Returns true if we need to collect stats for a given filter.
    pub(crate) fn needs_collection(&self, id: FilterId) -> bool {
        matches!(
            self.filter_states.get(&id),
            None | Some(FilterState::Collecting)
        )
    }

    /// Check and update the snapshot generation for a filter.
    /// If the generation changed (dynamic filter updated)
    /// move from post-scan to collecting if not already a row filter.
    /// Dynamic filters may be refined as the scan progresses,
    /// almost always to a more selective filter.
    /// Thus it may be a good candidate to promote from a post-scan filter
    /// to a
    fn note_generation(&mut self, id: FilterId, generation: u64) {
        if generation == 0 {
            // Static expression — no generation tracking needed
            return;
        }
        match self.snapshot_generations.get(&id) {
            Some(&prev_generation) if prev_generation == generation => {
                // No change
            }
            Some(_) => {
                // Generation changed — dynamic filter updated
                debug!("FilterId {id} generation changed, resetting to Collecting");
                self.stats.remove(&id);
                self.filter_states.insert(id, FilterState::Collecting);
                self.snapshot_generations.insert(id, generation);
                // Invalidate cached decision
                self.cached_decision = None;
                // Clear correlations involving this filter
                self.correlations.retain(|pk, _| pk.0 != id && pk.1 != id);
            }
            None => {
                // First time seeing this filter
                self.snapshot_generations.insert(id, generation);
            }
        }
    }

    /// Get the effectiveness (opaque ordering score) for a filter by ID.
    fn get_effectiveness_by_id(&self, id: FilterId) -> Option<f64> {
        self.stats.get(&id).and_then(|s| s.effectiveness())
    }

    /// Returns true if we're still in the collection phase for any filter
    /// that is in the Collecting state.
    fn in_collection_phase(&self) -> bool {
        let min_rows = self.effective_min_rows();
        if min_rows == 0 {
            return false;
        }
        // Only consider filters that are still in Collecting state (or new/unknown)
        let collecting_ids: Vec<FilterId> = self
            .filter_states
            .iter()
            .filter(|(_, state)| matches!(state, FilterState::Collecting))
            .map(|(&id, _)| id)
            .collect();
        // If no filters are registered yet but stats are empty, we're in collection
        if self.filter_states.is_empty() && self.stats.is_empty() {
            return true;
        }
        // Check if any collecting filter still needs more rows
        collecting_ids
            .iter()
            .any(|id| self.stats.get(id).is_none_or(|s| s.rows_total < min_rows))
    }

    /// Partition filters using correlation-based deduplication.
    ///
    /// Filters are identified by their stable FilterId. Correlated filters
    /// are deduplicated: only the most effective from each correlated group
    /// is promoted; the rest are demoted to post-scan.
    pub(crate) fn partition_filters(
        &mut self,
        filters: Vec<(FilterId, Arc<dyn PhysicalExpr>)>,
    ) -> PartitionedFilters {
        let strategy = self.promotion_strategy();

        // Disabled: nothing is promoted.
        if strategy == PromotionStrategy::Disabled {
            debug!(
                "Filter promotion disabled; all {} filters post-scan",
                filters.len()
            );
            return PartitionedFilters {
                row_filters: Vec::new(),
                post_scan: filters,
            };
        }

        // Note snapshot generations for dynamic filter detection
        for &(id, ref expr) in &filters {
            let generation = snapshot_generation(expr);
            self.note_generation(id, generation);
        }

        // During collection phase (only for Threshold strategy), all filters go
        // to post-scan for accurate marginal and joint measurement.
        if strategy != PromotionStrategy::AllPromoted && self.in_collection_phase() {
            debug!(
                "In collection phase ({} rows so far, need {}); all {} filters post-scan",
                self.total_rows_observed,
                self.effective_min_rows(),
                filters.len()
            );
            return PartitionedFilters {
                row_filters: Vec::new(),
                post_scan: filters,
            };
        }

        // Replay cached decision if not enough new rows have arrived.
        if let Some(ref cached) = self.cached_decision {
            let rows_since = self
                .total_rows_observed
                .saturating_sub(cached.decided_at_rows);
            if rows_since < self.effective_min_rows() {
                return self.apply_cached_decision(cached, filters);
            }
        }

        // Separate effective vs ineffective filters.
        let mut effective: Vec<(FilterId, Arc<dyn PhysicalExpr>)> = Vec::new();
        let mut post_scan: Vec<(FilterId, Arc<dyn PhysicalExpr>)> = Vec::new();

        // Track previous states for clearing stats on transition
        let prev_states: HashMap<FilterId, FilterState> = filters
            .iter()
            .filter_map(|&(id, _)| self.filter_states.get(&id).map(|&s| (id, s)))
            .collect();

        match strategy {
            PromotionStrategy::AllPromoted => {
                effective = filters;
            }
            PromotionStrategy::Threshold(threshold) => {
                for (id, expr) in filters {
                    // Check if filter has stats and meets threshold
                    match self.stats.get(&id).and_then(|s| s.effectiveness()) {
                        Some(eff) if eff >= threshold => {
                            // Clear stats if transitioning from Collecting
                            if matches!(
                                prev_states.get(&id),
                                None | Some(FilterState::Collecting)
                            ) {
                                self.stats.remove(&id);
                            }
                            self.filter_states.insert(id, FilterState::Promoted);
                            effective.push((id, expr));
                        }
                        _ => {
                            // Below threshold or no stats: drop optional filters entirely,
                            // demote mandatory filters to post-scan.
                            if expr
                                .as_any()
                                .downcast_ref::<OptionalFilterPhysicalExpr>()
                                .is_none()
                            {
                                if matches!(
                                    prev_states.get(&id),
                                    None | Some(FilterState::Collecting)
                                ) {
                                    self.stats.remove(&id);
                                }
                                self.filter_states.insert(id, FilterState::Demoted);
                                post_scan.push((id, expr));
                            } else {
                                self.stats.remove(&id);
                                self.filter_states.insert(id, FilterState::Dropped);
                            }
                        }
                    }
                }
            }
            PromotionStrategy::Disabled => unreachable!(),
        }

        // Deduplicate correlated filters: for each correlated group with >1 filter,
        // keep only the most effective, demote the rest to post-scan.
        let groups = self.group_by_correlation_id(&effective);

        let mut row_filters: Vec<(FilterId, Arc<dyn PhysicalExpr>)> = Vec::new();
        for indices in groups {
            if indices.len() == 1 {
                row_filters.push(effective[indices[0]].clone());
            } else {
                // Find the most effective filter in the group
                let best_idx = indices
                    .iter()
                    .copied()
                    .max_by(|&a, &b| {
                        let eff_a =
                            self.get_effectiveness_by_id(effective[a].0).unwrap_or(0.0);
                        let eff_b =
                            self.get_effectiveness_by_id(effective[b].0).unwrap_or(0.0);
                        eff_a
                            .partial_cmp(&eff_b)
                            .unwrap_or(std::cmp::Ordering::Equal)
                    })
                    .unwrap();
                // Promote the best, demote the rest
                for &i in &indices {
                    if i == best_idx {
                        row_filters.push(effective[i].clone());
                    } else {
                        let (id, ref expr) = effective[i];
                        if expr
                            .as_any()
                            .downcast_ref::<OptionalFilterPhysicalExpr>()
                            .is_none()
                        {
                            self.filter_states.insert(id, FilterState::Demoted);
                            post_scan.push(effective[i].clone());
                        } else {
                            self.stats.remove(&id);
                            self.filter_states.insert(id, FilterState::Dropped);
                        }
                    }
                }
            }
        }

        // Sort row_filters by effectiveness descending
        row_filters.sort_by(|a, b| {
            let eff_a = self.get_effectiveness_by_id(a.0).unwrap_or(0.0);
            let eff_b = self.get_effectiveness_by_id(b.0).unwrap_or(0.0);
            eff_b
                .partial_cmp(&eff_a)
                .unwrap_or(std::cmp::Ordering::Equal)
        });

        // Cache the decision for replay
        let promoted: HashSet<FilterId> = row_filters.iter().map(|&(id, _)| id).collect();
        let demoted: HashSet<FilterId> = post_scan.iter().map(|&(id, _)| id).collect();
        self.cached_decision = Some(CachedDecision {
            promoted,
            demoted,
            decided_at_rows: self.total_rows_observed,
        });

        // Diagnostic logging
        if log::log_enabled!(log::Level::Debug) {
            let threshold_str = match strategy {
                PromotionStrategy::Threshold(t) => format!("{t:.0} bytes/sec"),
                PromotionStrategy::AllPromoted => "all promoted".to_string(),
                PromotionStrategy::Disabled => "disabled".to_string(),
            };
            debug!(
                "Filter partition decision: strategy={:?}, threshold={}, rows_observed={}, collection_min_rows={}",
                strategy,
                threshold_str,
                self.total_rows_observed,
                self.effective_min_rows()
            );
            for &(id, ref expr) in &row_filters {
                let detail = match self.stats.get(&id) {
                    Some(s) => {
                        let eff = s.effectiveness();
                        let eval_ms = s.eval_nanos() as f64 / 1_000_000.0;
                        let eff_str =
                            eff.map_or_else(|| "N/A".to_string(), |v| format!("{v:.0}"));
                        format!(
                            "rows={}/{} matched, selectivity={:.4}, eval_time={eval_ms:.2}ms, bytes_seen={}, effectiveness={eff_str} bytes/sec → PROMOTED (>= {threshold_str})",
                            s.rows_matched(),
                            s.rows_total(),
                            s.selectivity(),
                            s.bytes_seen,
                        )
                    }
                    None => "no stats (cleared on transition) → PROMOTED".to_string(),
                };
                debug!("  Filter id={id}: {expr} [{detail}]");
            }
            for &(id, ref expr) in &post_scan {
                let detail = match self.stats.get(&id) {
                    Some(s) => {
                        let eff = s.effectiveness();
                        let eval_ms = s.eval_nanos() as f64 / 1_000_000.0;
                        let eff_str =
                            eff.map_or_else(|| "N/A".to_string(), |v| format!("{v:.0}"));
                        format!(
                            "rows={}/{} matched, selectivity={:.4}, eval_time={eval_ms:.2}ms, bytes_seen={}, effectiveness={eff_str} bytes/sec → DEMOTED (below {threshold_str})",
                            s.rows_matched(),
                            s.rows_total(),
                            s.selectivity(),
                            s.bytes_seen,
                        )
                    }
                    None => "no stats collected → DEMOTED".to_string(),
                };
                debug!("  Filter id={id}: {expr} [{detail}]");
            }
            // Log any dropped filters
            for (id, state) in &self.filter_states {
                if matches!(state, FilterState::Dropped) {
                    debug!("  Filter id={id}: DROPPED (optional, below threshold)");
                }
            }
            debug!(
                "Summary: {} promoted, {} post-scan, {} dropped",
                row_filters.len(),
                post_scan.len(),
                self.filter_states
                    .values()
                    .filter(|s| matches!(s, FilterState::Dropped))
                    .count()
            );
        }

        PartitionedFilters {
            row_filters,
            post_scan,
        }
    }

    /// Replay a cached partitioning decision without re-evaluating promotion.
    fn apply_cached_decision(
        &self,
        cached: &CachedDecision,
        filters: Vec<(FilterId, Arc<dyn PhysicalExpr>)>,
    ) -> PartitionedFilters {
        let mut row_filters = Vec::new();
        let mut post_scan = Vec::new();

        for (id, expr) in filters {
            if cached.promoted.contains(&id) {
                row_filters.push((id, expr));
            } else if cached.demoted.contains(&id) {
                if expr
                    .as_any()
                    .downcast_ref::<OptionalFilterPhysicalExpr>()
                    .is_none()
                {
                    post_scan.push((id, expr));
                }
            } else {
                // Unknown filter — promote
                row_filters.push((id, expr));
            }
        }

        row_filters.sort_by(|a, b| {
            let eff_a = self.get_effectiveness_by_id(a.0).unwrap_or(0.0);
            let eff_b = self.get_effectiveness_by_id(b.0).unwrap_or(0.0);
            eff_b
                .partial_cmp(&eff_a)
                .unwrap_or(std::cmp::Ordering::Equal)
        });

        PartitionedFilters {
            row_filters,
            post_scan,
        }
    }

    /// Update stats for a filter by ID after processing a batch.
    pub(crate) fn update(
        &mut self,
        id: FilterId,
        matched: u64,
        total: u64,
        eval_nanos: u64,
        batch_bytes: u64,
    ) {
        self.total_rows_observed += total;
        self.stats
            .entry(id)
            .or_default()
            .update(matched, total, eval_nanos, batch_bytes);
    }

    /// Update pairwise correlation statistics for two filters by ID.
    pub(crate) fn update_correlation(
        &mut self,
        a: FilterId,
        b: FilterId,
        both_passed: u64,
        total: u64,
    ) {
        let pair_key = PairKey::new(a, b);
        let stats = self.correlations.entry(pair_key).or_default();
        stats.rows_both_passed += both_passed;
        stats.rows_total += total;
    }

    /// Compute the correlation ratio for two filters by ID.
    fn correlation_ratio_by_id(&self, a: FilterId, b: FilterId) -> Option<f64> {
        let stats_a = self.stats.get(&a)?;
        let stats_b = self.stats.get(&b)?;

        let min_rows = self.effective_min_rows();
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
                    self.correlation_ratio_by_id(filters[i].0, filters[j].0)
                    && ratio > self.correlation_threshold
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

    fn make_filter(col_name: &str, value: i32) -> Arc<dyn PhysicalExpr> {
        let schema = test_schema();
        Arc::new(BinaryExpr::new(
            col(col_name, &schema).unwrap(),
            Operator::Eq,
            lit(value),
        ))
    }

    #[test]
    fn test_selectivity_stats_selectivity() {
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
    fn test_selectivity_stats_effectiveness() {
        // No data → None
        let stats = SelectivityStats::new(0, 0, 0, 0);
        assert!(stats.effectiveness().is_none());

        // No eval time → None
        let stats = SelectivityStats::new(20, 100, 0, 1000);
        assert!(stats.effectiveness().is_none());

        // No bytes → None
        let stats = SelectivityStats::new(20, 100, 1_000_000_000, 0);
        assert!(stats.effectiveness().is_none());

        // 80 rows pruned, 10 bytes/row, 1 sec → 800 bytes/sec
        let stats = SelectivityStats::new(20, 100, 1_000_000_000, 1000);
        let eff = stats.effectiveness().unwrap();
        assert!((eff - 800.0).abs() < 0.001);

        // 0 rows pruned → 0 bytes/sec
        let stats = SelectivityStats::new(100, 100, 1_000_000_000, 1000);
        let eff = stats.effectiveness().unwrap();
        assert!((eff - 0.0).abs() < 0.001);
    }

    #[test]
    fn test_selectivity_stats_update() {
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

    #[test]
    fn test_tracker_partition_threshold_zero_pushes_all() {
        let mut tracker = SelectivityTracker::new()
            .with_min_bytes_per_sec(0.0)
            .with_min_rows_for_collection(100);

        let filter1 = make_filter("a", 5);
        let filter2 = make_filter("a", 10);

        tracker.update(0, 10, 100, 0, 0);
        tracker.update(1, 10, 100, 0, 0);

        let result =
            tracker.partition_filters(vec![(0, filter1.clone()), (1, filter2.clone())]);

        assert_eq!(result.row_filters.len(), 2);
        assert_eq!(result.post_scan.len(), 0);
    }

    #[test]
    fn test_tracker_partition_promotes_high_throughput_filter() {
        let mut tracker = SelectivityTracker::new()
            .with_min_bytes_per_sec(100.0)
            .with_min_rows_for_collection(100);

        let filter1 = make_filter("a", 5);
        let filter2 = make_filter("a", 10);

        // filter1: 80 rows pruned * 10 bytes/row = 800 bytes saved in 1 sec → 800 bytes/sec
        tracker.update(0, 20, 100, 1_000_000_000, 1000);
        // filter2: 10 rows pruned * 10 bytes/row = 100 bytes saved in 10 sec → 10 bytes/sec
        tracker.update(1, 90, 100, 10_000_000_000, 1000);

        let result =
            tracker.partition_filters(vec![(0, filter1.clone()), (1, filter2.clone())]);

        assert_eq!(result.row_filters.len(), 1);
        assert_eq!(result.post_scan.len(), 1);
        assert_eq!(result.row_filters[0].0, 0);
        assert_eq!(result.post_scan[0].0, 1);
    }

    #[test]
    fn test_tracker_partition_infinity_disables_promotion() {
        let mut tracker = SelectivityTracker::new();
        let filter = make_filter("a", 5);
        tracker.update(0, 0, 100, 1, 1000);

        let result = tracker.partition_filters(vec![(0, filter.clone())]);

        assert!(result.row_filters.is_empty());
        assert_eq!(result.post_scan.len(), 1);
    }

    #[test]
    fn test_partition_filters_collection_phase() {
        let mut tracker = SelectivityTracker::new()
            .with_min_bytes_per_sec(100.0)
            .with_min_rows_for_collection(10_000);

        let filter_a = make_filter("a", 5);
        let filter_b = make_filter("a", 10);

        let result =
            tracker.partition_filters(vec![(0, filter_a.clone()), (1, filter_b.clone())]);

        assert!(result.row_filters.is_empty());
        assert_eq!(result.post_scan.len(), 2);
    }

    #[test]
    fn test_partition_filters_all_independent() {
        let mut tracker = SelectivityTracker::new()
            .with_min_bytes_per_sec(0.0)
            .with_min_rows_for_collection(100);

        let filter_a = make_filter("a", 5);
        let filter_b = make_filter("a", 10);

        tracker.update(0, 10, 100, 0, 0);
        tracker.update(1, 10, 100, 0, 0);
        tracker.update_correlation(0, 1, 1, 100);

        let result =
            tracker.partition_filters(vec![(0, filter_a.clone()), (1, filter_b.clone())]);

        assert_eq!(result.row_filters.len(), 2);
        assert_eq!(result.post_scan.len(), 0);
    }

    #[test]
    fn test_partition_filters_correlated() {
        let mut tracker = SelectivityTracker::new()
            .with_min_bytes_per_sec(0.0)
            .with_min_rows_for_collection(100);

        let filter_a = make_filter("a", 5);
        let filter_b = make_filter("a", 10);

        // filter_a: selectivity 0.7 (30/100 matched), filter_b: selectivity 0.6 (40/100 matched)
        // With AllPromoted strategy and no effectiveness data (no eval_nanos/bytes),
        // both are effective but correlated. The best (by effectiveness) is promoted,
        // the other is demoted. Since effectiveness is None for both (no timing data),
        // both default to 0.0 — one is kept, one demoted.
        tracker.update(0, 30, 100, 0, 0);
        tracker.update(1, 40, 100, 0, 0);
        tracker.update_correlation(0, 1, 25, 100);

        let result =
            tracker.partition_filters(vec![(0, filter_a.clone()), (1, filter_b.clone())]);

        // Best filter promoted, other demoted to post_scan
        assert_eq!(result.row_filters.len(), 1);
        assert_eq!(result.post_scan.len(), 1);
    }

    #[test]
    fn test_partition_filters_mixed() {
        let mut tracker = SelectivityTracker::new()
            .with_min_bytes_per_sec(0.0)
            .with_min_rows_for_collection(100);

        let filter_a = make_filter("a", 5);
        let filter_b = make_filter("a", 10);
        let filter_c = make_filter("c", 1);
        let filter_d = make_filter("d", 2);

        tracker.update(0, 30, 100, 0, 0);
        tracker.update(1, 40, 100, 0, 0);
        tracker.update(2, 20, 100, 0, 0);
        tracker.update(3, 35, 100, 0, 0);

        // A-B correlated
        tracker.update_correlation(0, 1, 25, 100);
        // C-D correlated
        tracker.update_correlation(2, 3, 15, 100);
        // A-C independent
        tracker.update_correlation(0, 2, 6, 100);
        tracker.update_correlation(1, 2, 8, 100);
        tracker.update_correlation(0, 3, 10, 100);
        tracker.update_correlation(1, 3, 14, 100);

        let result = tracker.partition_filters(vec![
            (0, filter_a.clone()),
            (1, filter_b.clone()),
            (2, filter_c.clone()),
            (3, filter_d.clone()),
        ]);

        // One filter promoted per correlated pair, the other demoted
        assert_eq!(result.row_filters.len(), 2);
        assert_eq!(result.post_scan.len(), 2);
    }

    #[test]
    fn test_partition_filters_single_filter() {
        let mut tracker = SelectivityTracker::new()
            .with_min_bytes_per_sec(0.0)
            .with_min_rows_for_collection(100);

        let filter_a = make_filter("a", 5);
        tracker.update(0, 10, 100, 0, 0);

        let result = tracker.partition_filters(vec![(0, filter_a.clone())]);

        assert_eq!(result.row_filters.len(), 1);
        assert_eq!(result.post_scan.len(), 0);
    }

    #[test]
    fn test_partition_filters_with_low_throughput() {
        let mut tracker = SelectivityTracker::new()
            .with_min_bytes_per_sec(100.0)
            .with_min_rows_for_collection(100);

        let filter_a = make_filter("a", 5);
        let filter_b = make_filter("a", 10);

        // filter0: 90 pruned * 100 bpr = 9000 bytes saved in 1s → 9000 bps
        tracker.update(0, 10, 100, 1_000_000_000, 10_000);
        // filter1: 50 pruned * 100 bpr = 5000 bytes saved in 100s → 50 bps (below threshold)
        tracker.update(1, 50, 100, 100_000_000_000, 10_000);
        tracker.update_correlation(0, 1, 8, 100);

        let result =
            tracker.partition_filters(vec![(0, filter_a.clone()), (1, filter_b.clone())]);

        assert_eq!(result.row_filters.len(), 1);
        assert_eq!(result.post_scan.len(), 1);
    }

    #[test]
    fn test_notify_dataset_rows_resolves_fraction() {
        let mut tracker = SelectivityTracker::new()
            .with_min_bytes_per_sec(0.0)
            .with_min_rows_for_collection(100)
            .with_collection_fraction(0.05);
        assert_eq!(tracker.effective_min_rows(), 100);

        tracker.notify_dataset_rows(10_000);
        assert_eq!(tracker.effective_min_rows(), 500);
    }

    #[test]
    fn test_notify_dataset_rows_floor_behavior() {
        let mut tracker = SelectivityTracker::new()
            .with_min_bytes_per_sec(0.0)
            .with_min_rows_for_collection(1000)
            .with_collection_fraction(0.05);
        tracker.notify_dataset_rows(10_000);
        assert_eq!(tracker.effective_min_rows(), 1000);
    }

    #[test]
    fn test_max_rows_caps_effective_min_rows() {
        let mut tracker = SelectivityTracker::new()
            .with_min_bytes_per_sec(0.0)
            .with_min_rows_for_collection(100)
            .with_collection_fraction(0.05)
            .with_max_rows_for_collection(500);
        tracker.notify_dataset_rows(1_000_000);
        assert_eq!(tracker.effective_min_rows(), 500);
    }

    #[test]
    fn test_optional_filter_dropped_when_ineffective() {
        let mut tracker = SelectivityTracker::new()
            .with_min_bytes_per_sec(100.0)
            .with_min_rows_for_collection(100);

        let mandatory_filter = make_filter("a", 5);
        let inner_optional = make_filter("c", 1);
        let optional_filter: Arc<dyn PhysicalExpr> =
            Arc::new(OptionalFilterPhysicalExpr::new(inner_optional));

        // Both filters: 50 pruned * 100 bpr = 5000 bytes saved in 100s → 50 bps (below 100)
        tracker.update(0, 50, 100, 100_000_000_000, 10_000);
        tracker.update(1, 50, 100, 100_000_000_000, 10_000);

        let result = tracker.partition_filters(vec![
            (0, mandatory_filter.clone()),
            (1, optional_filter.clone()),
        ]);

        assert_eq!(result.post_scan.len(), 1);
        assert_eq!(result.post_scan[0].0, 0); // mandatory demoted
        assert!(result.row_filters.is_empty());
    }

    #[test]
    fn test_optional_filter_promoted_when_effective() {
        let mut tracker = SelectivityTracker::new()
            .with_min_bytes_per_sec(100.0)
            .with_min_rows_for_collection(100);

        let inner_optional = make_filter("a", 5);
        let optional_filter: Arc<dyn PhysicalExpr> =
            Arc::new(OptionalFilterPhysicalExpr::new(inner_optional));

        // 90 pruned * 100 bpr = 9000 bytes saved in 1s → 9000 bps (above 100)
        tracker.update(0, 10, 100, 1_000_000_000, 10_000);

        let result = tracker.partition_filters(vec![(0, optional_filter.clone())]);

        assert_eq!(result.row_filters.len(), 1);
        assert_eq!(result.post_scan.len(), 0);
    }

    #[test]
    fn test_cached_decision_reuse_and_reevaluation() {
        let mut tracker = SelectivityTracker::new()
            .with_min_bytes_per_sec(100.0)
            .with_min_rows_for_collection(100);

        let filter_a = make_filter("a", 5);
        let filter_b = make_filter("a", 10);

        // filter0: high throughput, filter1: low throughput
        tracker.update(0, 10, 100, 1_000_000_000, 10_000);
        tracker.update(1, 50, 100, 100_000_000_000, 10_000);

        let result =
            tracker.partition_filters(vec![(0, filter_a.clone()), (1, filter_b.clone())]);
        assert_eq!(result.row_filters.len(), 1);
        assert_eq!(result.post_scan.len(), 1);
        let decided_at = tracker.cached_decision.as_ref().unwrap().decided_at_rows;

        // Add small number of rows (not enough to trigger re-evaluation)
        tracker.update(1, 10, 50, 1_000_000_000, 5_000);

        // Should replay cached
        let result =
            tracker.partition_filters(vec![(0, filter_a.clone()), (1, filter_b.clone())]);
        assert_eq!(result.row_filters.len(), 1);
        assert_eq!(result.post_scan.len(), 1);

        // Add enough rows to trigger re-evaluation, for both filters
        tracker.update(0, 10, 100, 1_000_000_000, 10_000);
        tracker.update(1, 10, 10000, 1_000_000_000, 1_000_000);
        let result =
            tracker.partition_filters(vec![(0, filter_a.clone()), (1, filter_b.clone())]);
        assert_eq!(result.row_filters.len(), 2);
        assert!(tracker.cached_decision.as_ref().unwrap().decided_at_rows > decided_at);
    }

    #[test]
    fn test_dynamic_filter_generation_reset() {
        let mut tracker = SelectivityTracker::new()
            .with_min_bytes_per_sec(100.0)
            .with_min_rows_for_collection(100);

        tracker.update(0, 10, 100, 1_000_000_000, 10_000);

        // Simulate generation change
        tracker.note_generation(0, 1);
        tracker.note_generation(0, 2); // changed!

        assert!(tracker.needs_collection(0));
        assert!(!tracker.stats.contains_key(&0));
    }

    #[test]
    fn test_stats_cleared_on_state_transition() {
        let mut tracker = SelectivityTracker::new()
            .with_min_bytes_per_sec(100.0)
            .with_min_rows_for_collection(100);

        let filter_a = make_filter("a", 5);

        // Collect stats during collection phase
        tracker.update(0, 10, 100, 1_000_000_000, 10_000);
        assert!(tracker.stats.contains_key(&0));
        assert_eq!(tracker.stats.get(&0).unwrap().rows_total(), 100);

        // Promote the filter — stats should be cleared
        let result = tracker.partition_filters(vec![(0, filter_a.clone())]);
        assert_eq!(result.row_filters.len(), 1);

        // Stats were cleared on Collecting→Promoted transition
        // (stats.remove was called, but filter is still in row_filters)
        assert!(
            !tracker.stats.contains_key(&0),
            "Stats should be cleared on Collecting→Promoted transition"
        );
    }
}
