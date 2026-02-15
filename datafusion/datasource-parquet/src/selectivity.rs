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
//! The key insight is that filters with low effectiveness (those that don't filter
//! out many rows) may not be worth the I/O cost of late materialization. By tracking
//! effectiveness across files, we can learn which filters are worth pushing down.

use std::collections::{HashMap, HashSet};
use std::hash::{Hash, Hasher};
use std::sync::Arc;

use datafusion_physical_expr::utils::conjunction;
use datafusion_physical_expr_common::physical_expr::{
    OptionalFilterPhysicalExpr, PhysicalExpr,
};

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

/// Result of partitioning filters based on their effectiveness.
///
/// Filters are split into two groups:
/// - `row_filters`: Filters that should be pushed down as row filters
/// - `post_scan`: Filters that should be applied after scanning
#[derive(Debug, Clone, Default)]
pub struct PartitionedFilters {
    /// Filters to push down as row filters (effective or unknown effectiveness)
    pub row_filters: Vec<Arc<dyn PhysicalExpr>>,
    /// Filters to apply post-scan (known to be ineffective)
    pub post_scan: Vec<Arc<dyn PhysicalExpr>>,
}

/// Result of partitioning filters with correlation-based grouping.
///
/// Effective filters are grouped by correlation into compound predicates.
/// Each group becomes a single ArrowPredicate via conjunction.
#[derive(Debug, Clone, Default)]
pub struct PartitionedFiltersGrouped {
    /// Groups of correlated filters to push down as compound row filters.
    /// Each inner Vec is one group that will be combined with AND into a single ArrowPredicate.
    pub row_filter_groups: Vec<Vec<Arc<dyn PhysicalExpr>>>,
    /// Filters to apply post-scan (ineffective or in collection phase)
    pub post_scan: Vec<Arc<dyn PhysicalExpr>>,
}

/// Cached partitioning decision to avoid re-evaluating promotion/demotion
/// on every call to `partition_filters_grouped`.
///
/// The decision is reused until `effective_min_rows()` additional rows have
/// been observed, at which point a fresh evaluation is triggered.
#[derive(Debug)]
struct CachedDecision {
    /// Maps each promoted filter's ExprKey → group index
    promoted: HashMap<ExprKey, usize>,
    /// Filters explicitly sent to post_scan
    demoted: HashSet<ExprKey>,
    /// Number of groups
    num_groups: usize,
    /// `total_file_rows` when the decision was made
    decided_at_rows: u64,
}

/// Canonical pair key: always stores (lesser, greater) by hash for (A,B)==(B,A).
///
/// Canonicalization ensures that `update_correlation(A, B, ...)` and
/// `update_correlation(B, A, ...)` update the same entry.
#[derive(Clone, Debug)]
struct PairKey(ExprKey, ExprKey);

impl PairKey {
    fn new(a: &ExprKey, b: &ExprKey) -> Self {
        use std::collections::hash_map::DefaultHasher;
        let hash_a = {
            let mut h = DefaultHasher::new();
            a.hash(&mut h);
            h.finish()
        };
        let hash_b = {
            let mut h = DefaultHasher::new();
            b.hash(&mut h);
            h.finish()
        };
        if hash_a <= hash_b {
            Self(a.clone(), b.clone())
        } else {
            Self(b.clone(), a.clone())
        }
    }
}

impl Hash for PairKey {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.0.hash(state);
        self.1.hash(state);
    }
}

impl PartialEq for PairKey {
    fn eq(&self, other: &Self) -> bool {
        self.0 == other.0 && self.1 == other.1
    }
}

impl Eq for PairKey {}

/// Joint selectivity statistics for a pair of filters A and B.
///
/// Tracks the number of rows where BOTH filters pass simultaneously.
/// This allows computing:
///   `P(A ∧ B) = rows_both_passed / rows_total`
///
/// Combined with marginal stats from [`SelectivityStats`]:
///   `P(A) = A.rows_matched / A.rows_total`
///   `P(B) = B.rows_matched / B.rows_total`
///
/// We derive the correlation ratio:
///   `ratio = P(A ∧ B) / (P(A) * P(B))`
///
/// - `ratio > 1` → positively correlated (should group)
/// - `ratio ≈ 1` → independent (keep separate for late materialization)
/// - `ratio < 1` → negatively correlated (keep separate; sequential benefits)
#[derive(Debug, Clone, Default)]
struct CorrelationStats {
    rows_both_passed: u64,
    rows_total: u64,
}

/// Wrapper for `Arc<dyn PhysicalExpr>` that uses structural Hash/Eq.
///
/// This is needed because `Arc<dyn PhysicalExpr>` uses pointer equality by default,
/// but we want to use the structural equality provided by `DynEq` and `DynHash`.
///
/// For dynamic expressions (like `DynamicFilterPhysicalExpr`), we use the snapshot
/// of the expression to ensure stable hash/eq values even as the dynamic expression
/// updates. This is critical for HashMap correctness.
#[derive(Clone, Debug)]
pub(crate) struct ExprKey(Arc<dyn PhysicalExpr>);

impl ExprKey {
    /// Create a new ExprKey from an expression.
    ///
    /// For dynamic expressions, this takes a snapshot to ensure stable hash/eq.
    pub(crate) fn new(expr: &Arc<dyn PhysicalExpr>) -> Self {
        // Try to get a snapshot; if available, use it for stable hash/eq
        let stable_expr = expr
            .snapshot()
            .ok()
            .flatten()
            .unwrap_or_else(|| Arc::clone(expr));
        Self(stable_expr)
    }
}

impl Hash for ExprKey {
    fn hash<H: Hasher>(&self, state: &mut H) {
        // dyn PhysicalExpr implements Hash, which delegates to dyn_hash
        self.0.as_ref().hash(state);
    }
}

impl PartialEq for ExprKey {
    fn eq(&self, other: &Self) -> bool {
        self.0.as_ref() == other.0.as_ref()
    }
}

impl Eq for ExprKey {}

/// Tracks selectivity statistics for a single filter expression.
#[derive(Debug, Clone, Default)]
pub struct SelectivityStats {
    /// Number of rows that matched (passed) the filter
    rows_matched: u64,
    /// Total number of rows evaluated
    rows_total: u64,
    /// Cumulative evaluation time in nanoseconds
    eval_nanos: u64,
}

impl SelectivityStats {
    /// Create new stats with given values.
    pub fn new(rows_matched: u64, rows_total: u64, eval_nanos: u64) -> Self {
        Self {
            rows_matched,
            rows_total,
            eval_nanos,
        }
    }

    /// Number of rows that matched (passed) the filter.
    pub fn rows_matched(&self) -> u64 {
        self.rows_matched
    }

    /// Total number of rows evaluated.
    pub fn rows_total(&self) -> u64 {
        self.rows_total
    }

    /// Cumulative evaluation time in nanoseconds.
    pub fn eval_nanos(&self) -> u64 {
        self.eval_nanos
    }

    /// Returns the filter effectiveness (fraction of rows filtered out).
    ///
    /// - 1.0 = perfect filter (all rows filtered out)
    /// - 0.0 = useless filter (no rows filtered out)
    ///
    /// Returns 0.0 if no rows have been evaluated (unknown effectiveness).
    pub fn effectiveness(&self) -> f64 {
        if self.rows_total == 0 {
            0.0 // Unknown, assume ineffective
        } else {
            1.0 - (self.rows_matched as f64 / self.rows_total as f64)
        }
    }

    /// Returns the bytes/sec throughput of this filter.
    ///
    /// `bytes_per_sec = bytes_saved / eval_time`
    /// where `bytes_saved = rows_pruned * bytes_per_row`
    ///
    /// Returns `None` if no rows or no timing data is available.
    pub fn bytes_per_sec(&self, bytes_per_row: f64) -> Option<f64> {
        if self.rows_total == 0 || self.eval_nanos == 0 {
            return None;
        }
        let rows_pruned = self.rows_total - self.rows_matched;
        let bytes_saved = rows_pruned as f64 * bytes_per_row;
        // Single division for better floating-point precision
        Some(bytes_saved * 1_000_000_000.0 / self.eval_nanos as f64)
    }

    /// Update stats with new observations.
    pub fn update(&mut self, matched: u64, total: u64, eval_nanos: u64) {
        self.rows_matched += matched;
        self.rows_total += total;
        self.eval_nanos += eval_nanos;
    }
}

/// Cross-file selectivity tracker for adaptive filter ordering.
///
/// This tracker maintains effectiveness statistics for filter expressions
/// across multiple files, allowing the system to learn which filters are
/// worth pushing down as row filters.
///
/// Filters start as post-scan to collect clean, uncorrelated stats. A filter
/// is promoted to row filter only when its measured throughput (bytes_saved / eval_time)
/// exceeds `min_bytes_per_sec`. Once promoted, row filter stats are correlated
/// and are NOT used for adaptive decisions.
///
/// Additionally tracks pairwise correlation between filters to enable
/// grouping correlated filters into compound ArrowPredicates.
#[derive(Debug)]
pub struct SelectivityTracker {
    /// Per-expression effectiveness statistics
    stats: HashMap<ExprKey, SelectivityStats>,
    /// Pairwise correlation statistics between filter pairs
    correlations: HashMap<PairKey, CorrelationStats>,
    /// Minimum bytes/sec throughput for promoting a filter to row filter.
    /// - `f64::INFINITY` (default) = no filter promoted (feature disabled)
    /// - `0.0` = all filters pushed as row filters (skip adaptive logic)
    min_bytes_per_sec: f64,
    /// Correlation ratio threshold for grouping filters.
    /// Pairs with correlation_ratio > this value are grouped together.
    /// Default: 1.5
    correlation_threshold: f64,
    /// Minimum rows that must be observed before collection phase ends.
    /// During collection, all filters go to post-scan for accurate measurement.
    /// Default: 10_000
    min_rows_for_collection: u64,
    /// Fraction of total dataset rows for collection phase (0.0 = disabled).
    /// When > 0 and dataset size is known, effective threshold =
    /// max(min_rows_for_collection, (fraction * total_rows) as u64).
    collection_fraction: f64,
    /// Resolved minimum rows after notify_dataset_rows() is called.
    /// None = not yet resolved (use min_rows_for_collection as-is).
    resolved_min_rows: Option<u64>,
    /// Cumulative compressed bytes across all files (for avg bytes_per_row).
    total_file_bytes: f64,
    /// Cumulative rows across all files (for avg bytes_per_row).
    total_file_rows: u64,
    /// Cached partitioning decision; replayed until enough new rows arrive.
    cached_decision: Option<CachedDecision>,
}

impl Default for SelectivityTracker {
    fn default() -> Self {
        Self::new(f64::INFINITY)
    }
}

impl SelectivityTracker {
    /// Create a new tracker with the given min bytes/sec threshold.
    ///
    /// # Arguments
    /// * `min_bytes_per_sec` - Minimum bytes/sec throughput to promote a filter.
    ///   `f64::INFINITY` = feature disabled (no filters promoted).
    ///   `0.0` = all filters pushed as row filters.
    pub fn new(min_bytes_per_sec: f64) -> Self {
        Self {
            stats: HashMap::new(),
            correlations: HashMap::new(),
            min_bytes_per_sec,
            correlation_threshold: 1.5,
            min_rows_for_collection: 10_000,
            collection_fraction: 0.0,
            resolved_min_rows: None,
            total_file_bytes: 0.0,
            total_file_rows: 0,
            cached_decision: None,
        }
    }

    /// Create a new tracker with all configurable parameters.
    pub fn new_with_config(
        min_bytes_per_sec: f64,
        correlation_threshold: f64,
        min_rows_for_collection: u64,
        collection_fraction: f64,
    ) -> Self {
        Self {
            stats: HashMap::new(),
            correlations: HashMap::new(),
            min_bytes_per_sec,
            correlation_threshold,
            min_rows_for_collection,
            collection_fraction,
            resolved_min_rows: None,
            total_file_bytes: 0.0,
            total_file_rows: 0,
            cached_decision: None,
        }
    }

    /// Get the min bytes/sec threshold.
    pub fn min_bytes_per_sec(&self) -> f64 {
        self.min_bytes_per_sec
    }

    /// Derive the promotion strategy from the configured `min_bytes_per_sec`.
    fn promotion_strategy(&self) -> PromotionStrategy {
        if self.min_bytes_per_sec.is_infinite() {
            PromotionStrategy::Disabled
        } else if self.min_bytes_per_sec == 0.0 {
            PromotionStrategy::AllPromoted
        } else {
            PromotionStrategy::Threshold(self.min_bytes_per_sec)
        }
    }

    /// Returns the effective minimum rows for collection, taking into account
    /// the fraction-based threshold if it has been resolved.
    fn effective_min_rows(&self) -> u64 {
        self.resolved_min_rows
            .unwrap_or(self.min_rows_for_collection)
    }

    /// Notify the tracker of the total dataset row count so the fraction-based
    /// threshold can be resolved.
    ///
    /// When `collection_fraction > 0`, computes:
    /// `resolved_min_rows = max(min_rows_for_collection, (fraction * total_rows) as u64)`
    pub fn notify_dataset_rows(&mut self, total_rows: u64) {
        if self.collection_fraction > 0.0 {
            let fraction_rows = (self.collection_fraction * total_rows as f64) as u64;
            self.resolved_min_rows =
                Some(self.min_rows_for_collection.max(fraction_rows));
        }
    }

    /// Record the bytes_per_row and row count for a file, contributing to the
    /// running average used by `partition_filters_grouped`.
    pub fn update_bytes_per_row(&mut self, bytes_per_row: f64, file_rows: u64) {
        self.total_file_bytes += bytes_per_row * file_rows as f64;
        self.total_file_rows += file_rows;
    }

    /// Returns the running-average bytes per row across all files seen so far,
    /// or `None` if no files have been recorded.
    pub fn avg_bytes_per_row(&self) -> Option<f64> {
        if self.total_file_rows == 0 {
            None
        } else {
            Some(self.total_file_bytes / self.total_file_rows as f64)
        }
    }

    /// Get the effectiveness for a filter expression, if known.
    pub fn get_effectiveness(&self, expr: &Arc<dyn PhysicalExpr>) -> Option<f64> {
        let key = ExprKey::new(expr);
        self.stats.get(&key).map(|s| s.effectiveness())
    }

    /// Returns true if we're still in the collection phase.
    ///
    /// During collection, all filters should be evaluated as post-scan so
    /// they all see the same input rows, enabling accurate marginal and
    /// joint selectivity measurement.
    ///
    /// Collection ends when ALL known filters have >= `min_rows_for_collection` rows.
    /// Returns false if no stats exist yet (no filters registered) or if
    /// min_rows_for_collection is 0 (collection disabled).
    pub fn in_collection_phase(&self) -> bool {
        let min_rows = self.effective_min_rows();
        if min_rows == 0 {
            return false;
        }
        if self.stats.is_empty() {
            // No filters registered yet - treat as collection phase
            // so the first file's filters go to post-scan for measurement
            return true;
        }
        self.stats.values().any(|s| s.rows_total < min_rows)
    }

    /// Partition filters into row_filters and post_scan based on bytes/sec throughput.
    ///
    /// This is the non-grouped variant used primarily for testing. For production
    /// use, prefer [`Self::partition_filters_grouped`] which also handles correlation-based
    /// grouping.
    ///
    /// - `AllPromoted` (0.0): all filters pushed as row filters
    /// - `Disabled` (INFINITY): no filter promoted
    /// - `Threshold`: filters meeting threshold → row_filters, others → post_scan
    /// - Unknown filters (no stats) → post-scan (to collect clean stats)
    pub fn partition_filters(
        &self,
        filters: Vec<Arc<dyn PhysicalExpr>>,
        bytes_per_row: Option<f64>,
    ) -> PartitionedFilters {
        match self.promotion_strategy() {
            PromotionStrategy::AllPromoted => {
                return PartitionedFilters {
                    row_filters: filters,
                    post_scan: Vec::new(),
                };
            }
            PromotionStrategy::Disabled => {
                return PartitionedFilters {
                    row_filters: Vec::new(),
                    post_scan: filters,
                };
            }
            PromotionStrategy::Threshold(_) => {}
        }

        let threshold = self.min_bytes_per_sec;
        let mut row_filters = Vec::new();
        let mut post_scan = Vec::new();

        for filter in filters {
            let key = ExprKey::new(&filter);
            match (self.stats.get(&key), bytes_per_row) {
                (Some(stats), Some(bpr)) => {
                    match stats.bytes_per_sec(bpr) {
                        Some(bps) if bps >= threshold => row_filters.push(filter),
                        _ => post_scan.push(filter), // below threshold or no timing → stay post-scan
                    }
                }
                _ => post_scan.push(filter), // unknown → post-scan to learn
            }
        }

        PartitionedFilters {
            row_filters,
            post_scan,
        }
    }

    /// Partition filters with correlation-based grouping.
    ///
    /// **Two-phase strategy:**
    /// 1. **Collection phase** (first N rows): All filters go to post_scan so they
    ///    all see the same input rows for accurate marginal and joint measurement.
    /// 2. **Optimized phase** (after collection): Use bytes/sec throughput and
    ///    correlation data to group correlated filters. Each group becomes one
    ///    compound ArrowPredicate. Independent filters remain as separate predicates.
    ///    Filters below the throughput threshold stay post-scan.
    pub fn partition_filters_grouped(
        &mut self,
        filters: Vec<Arc<dyn PhysicalExpr>>,
    ) -> PartitionedFiltersGrouped {
        let strategy = self.promotion_strategy();

        // Disabled: nothing is promoted.
        if strategy == PromotionStrategy::Disabled {
            return PartitionedFiltersGrouped {
                row_filter_groups: Vec::new(),
                post_scan: filters,
            };
        }

        let bytes_per_row = self.avg_bytes_per_row();

        // During collection phase (only for Threshold strategy), all filters go
        // to post-scan for accurate marginal and joint measurement.
        // AllPromoted skips collection — filters are always promoted.
        if strategy != PromotionStrategy::AllPromoted && self.in_collection_phase() {
            return PartitionedFiltersGrouped {
                row_filter_groups: Vec::new(),
                post_scan: filters,
            };
        }

        // Replay cached decision if not enough new rows have arrived.
        if let Some(ref cached) = self.cached_decision {
            let rows_since = self.total_file_rows.saturating_sub(cached.decided_at_rows);
            if rows_since < self.effective_min_rows() {
                return self.apply_cached_decision(cached, filters);
            }
        }

        // Separate effective vs ineffective filters.
        // AllPromoted: all filters are effective (skip threshold check).
        // Threshold: only filters exceeding the throughput threshold are effective.
        let mut effective = Vec::new();
        let mut post_scan = Vec::new();

        match strategy {
            PromotionStrategy::AllPromoted => {
                effective = filters;
            }
            PromotionStrategy::Threshold(threshold) => {
                for filter in filters {
                    let key = ExprKey::new(&filter);
                    match (self.stats.get(&key), bytes_per_row) {
                        (Some(stats), Some(bpr)) => {
                            match stats.bytes_per_sec(bpr) {
                                Some(bps) if bps >= threshold => {
                                    effective.push(filter);
                                }
                                _ => {
                                    // Below threshold: drop optional filters entirely,
                                    // demote mandatory filters to post-scan.
                                    if filter
                                        .as_any()
                                        .downcast_ref::<OptionalFilterPhysicalExpr>()
                                        .is_none()
                                    {
                                        post_scan.push(filter);
                                    }
                                }
                            }
                        }
                        _ => post_scan.push(filter), // unknown → post-scan to learn
                    }
                }
            }
            PromotionStrategy::Disabled => unreachable!(),
        }

        // Group effective filters by correlation using union-find
        let groups = self.group_by_correlation(&effective);

        // Build groups and check compound demotion
        let mut row_filter_groups: Vec<Vec<Arc<dyn PhysicalExpr>>> = Vec::new();
        for indices in groups {
            let group: Vec<Arc<dyn PhysicalExpr>> = indices
                .into_iter()
                .map(|i| Arc::clone(&effective[i]))
                .collect();

            if group.len() > 1
                && let PromotionStrategy::Threshold(threshold) = strategy
            {
                let combined = conjunction(group.iter().map(Arc::clone));
                let key = ExprKey::new(&combined);
                if let (Some(stats), Some(bpr)) = (self.stats.get(&key), bytes_per_row)
                    && let Some(bps) = stats.bytes_per_sec(bpr)
                    && bps < threshold
                {
                    for expr in group {
                        if expr
                            .as_any()
                            .downcast_ref::<OptionalFilterPhysicalExpr>()
                            .is_none()
                        {
                            post_scan.push(expr);
                        }
                    }
                    continue;
                }
            }

            row_filter_groups.push(group);
        }

        row_filter_groups.sort_by(|a, b| {
            let eff_a = self.group_effectiveness(a);
            let eff_b = self.group_effectiveness(b);
            eff_b
                .partial_cmp(&eff_a)
                .unwrap_or(std::cmp::Ordering::Equal)
        });

        // Cache the decision for replay until enough new rows arrive.
        let mut promoted = HashMap::new();
        for (group_idx, group) in row_filter_groups.iter().enumerate() {
            for filter in group {
                promoted.insert(ExprKey::new(filter), group_idx);
            }
        }
        let demoted: HashSet<ExprKey> =
            post_scan.iter().map(|f| ExprKey::new(f)).collect();
        self.cached_decision = Some(CachedDecision {
            promoted,
            demoted,
            num_groups: row_filter_groups.len(),
            decided_at_rows: self.total_file_rows,
        });

        PartitionedFiltersGrouped {
            row_filter_groups,
            post_scan,
        }
    }

    /// Replay a cached partitioning decision without re-evaluating promotion.
    fn apply_cached_decision(
        &self,
        cached: &CachedDecision,
        filters: Vec<Arc<dyn PhysicalExpr>>,
    ) -> PartitionedFiltersGrouped {
        let mut groups: Vec<Vec<Arc<dyn PhysicalExpr>>> =
            (0..cached.num_groups).map(|_| Vec::new()).collect();
        let mut post_scan = Vec::new();

        for filter in filters {
            let key = ExprKey::new(&filter);
            if let Some(&group_idx) = cached.promoted.get(&key) {
                groups[group_idx].push(filter);
            } else if cached.demoted.contains(&key) {
                // Explicitly demoted: drop optional, keep mandatory as post-scan
                if filter
                    .as_any()
                    .downcast_ref::<OptionalFilterPhysicalExpr>()
                    .is_none()
                {
                    post_scan.push(filter);
                }
            } else {
                // Unknown filter (likely unbuildable in previous file).
                // Promote as its own group — build_row_filter_from_groups will
                // handle it if still unbuildable in this file's schema.
                groups.push(vec![filter]);
            }
        }

        groups.retain(|g| !g.is_empty());

        // Re-sort by effectiveness (stats are still updating between decisions)
        groups.sort_by(|a, b| {
            let eff_a = self.group_effectiveness(a);
            let eff_b = self.group_effectiveness(b);
            eff_b
                .partial_cmp(&eff_a)
                .unwrap_or(std::cmp::Ordering::Equal)
        });

        PartitionedFiltersGrouped {
            row_filter_groups: groups,
            post_scan,
        }
    }

    /// Update stats for a filter expression after processing a batch.
    pub fn update(
        &mut self,
        expr: &Arc<dyn PhysicalExpr>,
        matched: u64,
        total: u64,
        eval_nanos: u64,
    ) {
        let key = ExprKey::new(expr);
        self.stats
            .entry(key)
            .or_default()
            .update(matched, total, eval_nanos);
    }

    /// Update pairwise correlation statistics for two filter expressions.
    ///
    /// `both_passed` is the number of rows where BOTH filters passed.
    /// `total` is the total number of rows both filters were evaluated on.
    pub fn update_correlation(
        &mut self,
        a: &Arc<dyn PhysicalExpr>,
        b: &Arc<dyn PhysicalExpr>,
        both_passed: u64,
        total: u64,
    ) {
        let key_a = ExprKey::new(a);
        let key_b = ExprKey::new(b);
        let pair_key = PairKey::new(&key_a, &key_b);
        let stats = self.correlations.entry(pair_key).or_default();
        stats.rows_both_passed += both_passed;
        stats.rows_total += total;
    }

    /// Compute the correlation ratio for two filter expressions.
    ///
    /// `ratio = P(A ∧ B) / (P(A) * P(B))`
    ///
    /// Returns `None` if insufficient data (< min_rows_for_collection)
    /// or if `P(A) * P(B) ≈ 0` (one filter almost never passes).
    pub fn correlation_ratio(
        &self,
        a: &Arc<dyn PhysicalExpr>,
        b: &Arc<dyn PhysicalExpr>,
    ) -> Option<f64> {
        let key_a = ExprKey::new(a);
        let key_b = ExprKey::new(b);

        let stats_a = self.stats.get(&key_a)?;
        let stats_b = self.stats.get(&key_b)?;

        // Need sufficient data
        let min_rows = self.effective_min_rows();
        if stats_a.rows_total < min_rows || stats_b.rows_total < min_rows {
            return None;
        }

        let pair_key = PairKey::new(&key_a, &key_b);
        let pair_stats = self.correlations.get(&pair_key)?;

        if pair_stats.rows_total < min_rows {
            return None;
        }

        // P(A) = marginal pass rate
        let p_a = stats_a.rows_matched as f64 / stats_a.rows_total as f64;
        // P(B) = marginal pass rate
        let p_b = stats_b.rows_matched as f64 / stats_b.rows_total as f64;

        let p_a_times_p_b = p_a * p_b;

        // Avoid division by near-zero
        if p_a_times_p_b < 1e-10 {
            return None;
        }

        // P(A ∧ B) = joint pass rate
        let p_ab = pair_stats.rows_both_passed as f64 / pair_stats.rows_total as f64;

        Some(p_ab / p_a_times_p_b)
    }

    /// Get the current stats for a filter expression, if any.
    pub fn get_stats(&self, expr: &Arc<dyn PhysicalExpr>) -> Option<&SelectivityStats> {
        let key = ExprKey::new(expr);
        self.stats.get(&key)
    }

    /// Iterate all known selectivities.
    pub fn iter(
        &self,
    ) -> impl Iterator<Item = (&Arc<dyn PhysicalExpr>, &SelectivityStats)> {
        self.stats.iter().map(|(key, stats)| (&key.0, stats))
    }

    /// Group effective filters by correlation using union-find.
    ///
    /// Returns groups as vectors of indices into the `filters` slice.
    fn group_by_correlation(&self, filters: &[Arc<dyn PhysicalExpr>]) -> Vec<Vec<usize>> {
        let n = filters.len();
        if n <= 1 {
            return (0..n).map(|i| vec![i]).collect();
        }

        // Union-find data structure
        let mut parent: Vec<usize> = (0..n).collect();
        let mut rank: Vec<usize> = vec![0; n];

        // Check all pairs for correlation
        for i in 0..n {
            for j in (i + 1)..n {
                if let Some(ratio) = self.correlation_ratio(&filters[i], &filters[j])
                    && ratio > self.correlation_threshold
                {
                    union(&mut parent, &mut rank, i, j);
                }
            }
        }

        // Collect connected components
        let mut components: HashMap<usize, Vec<usize>> = HashMap::new();
        for i in 0..n {
            let root = find(&mut parent, i);
            components.entry(root).or_default().push(i);
        }

        components.into_values().collect()
    }

    /// Compute the combined effectiveness of a group of filters.
    /// Uses the maximum effectiveness of any filter in the group as the sort key.
    fn group_effectiveness(&self, group: &[Arc<dyn PhysicalExpr>]) -> f64 {
        group
            .iter()
            .filter_map(|f| self.get_effectiveness(f))
            .fold(0.0_f64, f64::max)
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
    use datafusion_physical_expr::utils::conjunction;
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
    fn test_expr_key_equality() {
        let filter1 = make_filter("a", 5);
        let filter2 = make_filter("a", 5);
        let filter3 = make_filter("a", 10);

        let key1 = ExprKey::new(&filter1);
        let key2 = ExprKey::new(&filter2);
        let key3 = ExprKey::new(&filter3);

        // Same expression structure should be equal
        assert_eq!(key1, key2);
        // Different value should not be equal
        assert_ne!(key1, key3);
    }

    #[test]
    fn test_expr_key_hash() {
        use std::collections::hash_map::DefaultHasher;

        let filter1 = make_filter("a", 5);
        let filter2 = make_filter("a", 5);

        let key1 = ExprKey::new(&filter1);
        let key2 = ExprKey::new(&filter2);

        let mut hasher1 = DefaultHasher::new();
        let mut hasher2 = DefaultHasher::new();
        key1.hash(&mut hasher1);
        key2.hash(&mut hasher2);

        assert_eq!(hasher1.finish(), hasher2.finish());
    }

    #[test]
    fn test_selectivity_stats_effectiveness() {
        // No data - unknown
        let stats = SelectivityStats::new(0, 0, 0);
        assert_eq!(stats.effectiveness(), 0.0);

        // All rows pass - useless filter
        let stats = SelectivityStats::new(100, 100, 0);
        assert_eq!(stats.effectiveness(), 0.0);

        // No rows pass - perfect filter
        let stats = SelectivityStats::new(0, 100, 0);
        assert_eq!(stats.effectiveness(), 1.0);

        // 20% pass = 80% filtered = 0.8 effectiveness
        let stats = SelectivityStats::new(20, 100, 0);
        assert_eq!(stats.effectiveness(), 0.8);

        // 50% pass = 50% filtered = 0.5 effectiveness
        let stats = SelectivityStats::new(50, 100, 0);
        assert_eq!(stats.effectiveness(), 0.5);
    }

    #[test]
    fn test_selectivity_stats_bytes_per_sec() {
        // No data → None
        let stats = SelectivityStats::new(0, 0, 0);
        assert!(stats.bytes_per_sec(10.0).is_none());

        // No timing → None
        let stats = SelectivityStats::new(20, 100, 0);
        assert!(stats.bytes_per_sec(10.0).is_none());

        // 80 rows pruned * 10 bytes/row = 800 bytes saved
        // 1_000_000_000 nanos = 1 sec → 800 bytes/sec
        let stats = SelectivityStats::new(20, 100, 1_000_000_000);
        let bps = stats.bytes_per_sec(10.0).unwrap();
        assert!((bps - 800.0).abs() < 0.001);

        // All rows pass (0 pruned) → 0 bytes/sec
        let stats = SelectivityStats::new(100, 100, 1_000_000_000);
        let bps = stats.bytes_per_sec(10.0).unwrap();
        assert!((bps - 0.0).abs() < 0.001);
    }

    #[test]
    fn test_selectivity_stats_update() {
        let mut stats = SelectivityStats::default();
        assert_eq!(stats.rows_matched(), 0);
        assert_eq!(stats.rows_total(), 0);
        assert_eq!(stats.eval_nanos(), 0);

        stats.update(20, 100, 500);
        assert_eq!(stats.rows_matched(), 20);
        assert_eq!(stats.rows_total(), 100);
        assert_eq!(stats.eval_nanos(), 500);

        stats.update(30, 100, 300);
        assert_eq!(stats.rows_matched(), 50);
        assert_eq!(stats.rows_total(), 200);
        assert_eq!(stats.eval_nanos(), 800);
        assert_eq!(stats.effectiveness(), 0.75); // 150/200 filtered = 0.75
    }

    #[test]
    fn test_tracker_partition_unknown_filters() {
        // With bytes/sec metric, unknown filters go to post-scan to collect stats
        let tracker = SelectivityTracker::new(100_000_000.0);

        let filter1 = make_filter("a", 5);
        let filter2 = make_filter("a", 10);

        let PartitionedFilters {
            row_filters,
            post_scan,
        } = tracker.partition_filters(vec![filter1.clone(), filter2.clone()], Some(10.0));

        // Unknown filters → post-scan (to collect clean stats)
        assert_eq!(row_filters.len(), 0);
        assert_eq!(post_scan.len(), 2);
    }

    #[test]
    fn test_tracker_partition_threshold_zero_pushes_all() {
        // 0.0 threshold = push all filters as row filters
        let tracker = SelectivityTracker::new(0.0);

        let filter1 = make_filter("a", 5);
        let filter2 = make_filter("a", 10);

        let PartitionedFilters {
            row_filters,
            post_scan,
        } = tracker.partition_filters(vec![filter1.clone(), filter2.clone()], Some(10.0));

        assert_eq!(row_filters.len(), 2);
        assert_eq!(post_scan.len(), 0);
    }

    #[test]
    fn test_tracker_partition_promotes_high_throughput_filter() {
        let mut tracker = SelectivityTracker::new(100.0); // 100 bytes/sec threshold

        let filter1 = make_filter("a", 5);
        let filter2 = make_filter("a", 10);

        // filter1: 80 rows pruned * 10 bytes/row = 800 bytes saved in 1 sec = 800 bytes/sec
        tracker.update(&filter1, 20, 100, 1_000_000_000);
        // filter2: 10 rows pruned * 10 bytes/row = 100 bytes saved in 10 sec = 10 bytes/sec
        tracker.update(&filter2, 90, 100, 10_000_000_000);

        let PartitionedFilters {
            row_filters,
            post_scan,
        } = tracker.partition_filters(vec![filter1.clone(), filter2.clone()], Some(10.0));

        // filter1 at 800 bytes/sec >= 100 → promoted
        // filter2 at 10 bytes/sec < 100 → stays post-scan
        assert_eq!(row_filters.len(), 1);
        assert_eq!(post_scan.len(), 1);

        assert!(
            row_filters
                .iter()
                .any(|f| ExprKey::new(f) == ExprKey::new(&filter1))
        );
        assert!(
            post_scan
                .iter()
                .any(|f| ExprKey::new(f) == ExprKey::new(&filter2))
        );
    }

    #[test]
    fn test_tracker_partition_no_bytes_per_row() {
        let mut tracker = SelectivityTracker::new(100.0);
        let filter = make_filter("a", 5);
        tracker.update(&filter, 20, 100, 1_000_000_000);

        // No bytes_per_row → can't compute throughput → post-scan
        let PartitionedFilters {
            row_filters,
            post_scan,
        } = tracker.partition_filters(vec![filter.clone()], None);

        assert_eq!(row_filters.len(), 0);
        assert_eq!(post_scan.len(), 1);
    }

    #[test]
    fn test_tracker_partition_infinity_disables_promotion() {
        let mut tracker = SelectivityTracker::new(f64::INFINITY);
        let filter = make_filter("a", 5);
        // Very high throughput but threshold is infinity
        tracker.update(&filter, 0, 100, 1); // 100 rows pruned, 1 ns

        let PartitionedFilters {
            row_filters,
            post_scan,
        } = tracker.partition_filters(vec![filter.clone()], Some(10.0));

        // f64::INFINITY threshold → no filter can reach it → stays post-scan
        assert_eq!(row_filters.len(), 0);
        assert_eq!(post_scan.len(), 1);
    }

    #[test]
    fn test_tracker_partition_at_threshold_boundary() {
        let mut tracker = SelectivityTracker::new(800.0);
        let filter = make_filter("a", 5);
        // Exactly 800 bytes/sec: 80 pruned * 10 bpr = 800 bytes / 1 sec
        tracker.update(&filter, 20, 100, 1_000_000_000);

        let PartitionedFilters {
            row_filters,
            post_scan,
        } = tracker.partition_filters(vec![filter.clone()], Some(10.0));

        // At threshold boundary (>=) → promoted
        assert_eq!(row_filters.len(), 1);
        assert_eq!(post_scan.len(), 0);
    }

    // ---- Correlation-based grouping tests ----

    #[test]
    fn test_pair_key_canonical_ordering() {
        let filter_a = make_filter("a", 5);
        let filter_b = make_filter("a", 10);

        let key_a = ExprKey::new(&filter_a);
        let key_b = ExprKey::new(&filter_b);

        let pair1 = PairKey::new(&key_a, &key_b);
        let pair2 = PairKey::new(&key_b, &key_a);

        // (A,B) and (B,A) should be equal
        assert_eq!(pair1, pair2);

        // And hash the same
        use std::collections::hash_map::DefaultHasher;
        let mut h1 = DefaultHasher::new();
        let mut h2 = DefaultHasher::new();
        pair1.hash(&mut h1);
        pair2.hash(&mut h2);
        assert_eq!(h1.finish(), h2.finish());
    }

    #[test]
    fn test_correlation_stats_update() {
        let mut tracker = SelectivityTracker::new_with_config(0.0, 1.5, 100, 0.0);

        let filter_a = make_filter("a", 5);
        let filter_b = make_filter("a", 10);

        // Update marginal stats
        tracker.update(&filter_a, 30, 100, 0);
        tracker.update(&filter_b, 40, 100, 0);

        // Update correlation
        tracker.update_correlation(&filter_a, &filter_b, 25, 100);

        // Check correlation ratio
        // P(A) = 30/100 = 0.3, P(B) = 40/100 = 0.4
        // P(A ∧ B) = 25/100 = 0.25
        // ratio = 0.25 / (0.3 * 0.4) = 0.25 / 0.12 ≈ 2.083
        let ratio = tracker.correlation_ratio(&filter_a, &filter_b).unwrap();
        assert!((ratio - 2.083).abs() < 0.01);

        // Reverse order should give same result
        let ratio_rev = tracker.correlation_ratio(&filter_b, &filter_a).unwrap();
        assert!((ratio - ratio_rev).abs() < 0.001);
    }

    #[test]
    fn test_correlation_ratio_independent() {
        let mut tracker = SelectivityTracker::new_with_config(0.0, 1.5, 100, 0.0);

        let filter_a = make_filter("a", 5);
        let filter_b = make_filter("a", 10);

        // Independent filters: P(A)=0.5, P(B)=0.5, P(A∧B)=0.25 (= P(A)*P(B))
        tracker.update(&filter_a, 50, 100, 0);
        tracker.update(&filter_b, 50, 100, 0);
        tracker.update_correlation(&filter_a, &filter_b, 25, 100);

        let ratio = tracker.correlation_ratio(&filter_a, &filter_b).unwrap();
        assert!((ratio - 1.0).abs() < 0.01);
    }

    #[test]
    fn test_correlation_ratio_insufficient_data() {
        let mut tracker = SelectivityTracker::new_with_config(0.0, 1.5, 1000, 0.0);

        let filter_a = make_filter("a", 5);
        let filter_b = make_filter("a", 10);

        // Only 100 rows < min_rows_for_collection (1000)
        tracker.update(&filter_a, 30, 100, 0);
        tracker.update(&filter_b, 40, 100, 0);
        tracker.update_correlation(&filter_a, &filter_b, 25, 100);

        assert!(tracker.correlation_ratio(&filter_a, &filter_b).is_none());
    }

    #[test]
    fn test_in_collection_phase() {
        let mut tracker = SelectivityTracker::new_with_config(100.0, 1.5, 1000, 0.0);

        // No stats yet - in collection phase
        assert!(tracker.in_collection_phase());

        let filter = make_filter("a", 5);
        tracker.update(&filter, 50, 500, 0);

        // 500 < 1000, still collecting
        assert!(tracker.in_collection_phase());

        tracker.update(&filter, 50, 500, 0);

        // Now at 1000, collection done
        assert!(!tracker.in_collection_phase());
    }

    #[test]
    fn test_in_collection_phase_disabled() {
        let tracker = SelectivityTracker::new_with_config(100.0, 1.5, 0, 0.0);

        // min_rows = 0 means collection is disabled
        assert!(!tracker.in_collection_phase());
    }

    #[test]
    fn test_partition_filters_grouped_collection_phase() {
        let mut tracker = SelectivityTracker::new_with_config(100.0, 1.5, 10_000, 0.0);

        let filter_a = make_filter("a", 5);
        let filter_b = make_filter("a", 10);

        // No stats → collection phase → all post_scan
        let result =
            tracker.partition_filters_grouped(vec![filter_a.clone(), filter_b.clone()]);

        assert!(result.row_filter_groups.is_empty());
        assert_eq!(result.post_scan.len(), 2);
    }

    #[test]
    fn test_partition_filters_grouped_all_independent() {
        let mut tracker = SelectivityTracker::new_with_config(0.0, 1.5, 100, 0.0);

        let filter_a = make_filter("a", 5);
        let filter_b = make_filter("a", 10);

        // Effective filters with independent correlation
        tracker.update(&filter_a, 10, 100, 0);
        tracker.update(&filter_b, 10, 100, 0);
        // P(A)=0.1, P(B)=0.1, P(A∧B)=0.01 (independent)
        tracker.update_correlation(&filter_a, &filter_b, 1, 100);

        let result =
            tracker.partition_filters_grouped(vec![filter_a.clone(), filter_b.clone()]);

        // Independent: each in own group
        assert_eq!(result.row_filter_groups.len(), 2);
        assert_eq!(result.post_scan.len(), 0);
        // Each group has 1 filter
        for group in &result.row_filter_groups {
            assert_eq!(group.len(), 1);
        }
    }

    #[test]
    fn test_partition_filters_grouped_correlated() {
        let mut tracker = SelectivityTracker::new_with_config(0.0, 1.5, 100, 0.0);

        let filter_a = make_filter("a", 5);
        let filter_b = make_filter("a", 10);

        // Effective filters with high correlation
        tracker.update(&filter_a, 30, 100, 0);
        tracker.update(&filter_b, 40, 100, 0);
        // P(A)=0.3, P(B)=0.4, P(A∧B)=0.25
        // ratio = 0.25 / (0.3*0.4) = 2.08 > 1.5
        tracker.update_correlation(&filter_a, &filter_b, 25, 100);

        let result =
            tracker.partition_filters_grouped(vec![filter_a.clone(), filter_b.clone()]);

        // Correlated: both in one group
        assert_eq!(result.row_filter_groups.len(), 1);
        assert_eq!(result.row_filter_groups[0].len(), 2);
        assert_eq!(result.post_scan.len(), 0);
    }

    #[test]
    fn test_partition_filters_grouped_mixed() {
        let mut tracker = SelectivityTracker::new_with_config(0.0, 1.5, 100, 0.0);

        let filter_a = make_filter("a", 5);
        let filter_b = make_filter("a", 10);
        let filter_c = make_filter("c", 1);
        let filter_d = make_filter("d", 2);

        // All effective
        tracker.update(&filter_a, 30, 100, 0);
        tracker.update(&filter_b, 40, 100, 0);
        tracker.update(&filter_c, 20, 100, 0);
        tracker.update(&filter_d, 35, 100, 0);

        // A-B correlated (ratio > 1.5)
        tracker.update_correlation(&filter_a, &filter_b, 25, 100);
        // C-D correlated (ratio > 1.5)
        // P(C)=0.2, P(D)=0.35, P(C∧D)=0.15
        // ratio = 0.15 / (0.2*0.35) = 0.15/0.07 = 2.14
        tracker.update_correlation(&filter_c, &filter_d, 15, 100);
        // A-C independent
        // P(A)=0.3, P(C)=0.2, P(A∧C)=0.06
        // ratio = 0.06 / (0.3*0.2) = 0.06/0.06 = 1.0
        tracker.update_correlation(&filter_a, &filter_c, 6, 100);
        // B-C independent
        tracker.update_correlation(&filter_b, &filter_c, 8, 100);
        // A-D independent
        tracker.update_correlation(&filter_a, &filter_d, 10, 100);
        // B-D independent
        tracker.update_correlation(&filter_b, &filter_d, 14, 100);

        let result = tracker.partition_filters_grouped(vec![
            filter_a.clone(),
            filter_b.clone(),
            filter_c.clone(),
            filter_d.clone(),
        ]);

        // Should get 2 groups: {A,B} and {C,D}
        assert_eq!(result.row_filter_groups.len(), 2);
        assert_eq!(result.post_scan.len(), 0);

        // Each group should have 2 filters
        let mut group_sizes: Vec<usize> =
            result.row_filter_groups.iter().map(|g| g.len()).collect();
        group_sizes.sort();
        assert_eq!(group_sizes, vec![2, 2]);
    }

    #[test]
    fn test_partition_filters_grouped_single_filter() {
        let mut tracker = SelectivityTracker::new_with_config(0.0, 1.5, 100, 0.0);

        let filter_a = make_filter("a", 5);
        tracker.update(&filter_a, 10, 100, 0);

        let result = tracker.partition_filters_grouped(vec![filter_a.clone()]);

        // Single filter: one group of one
        assert_eq!(result.row_filter_groups.len(), 1);
        assert_eq!(result.row_filter_groups[0].len(), 1);
        assert_eq!(result.post_scan.len(), 0);
    }

    #[test]
    fn test_partition_filters_grouped_with_low_throughput() {
        // Use a bytes/sec threshold: 100 bytes/sec
        let mut tracker = SelectivityTracker::new_with_config(100.0, 1.5, 100, 0.0);

        let filter_a = make_filter("a", 5);
        let filter_b = make_filter("a", 10);

        // Feed bytes_per_row into tracker (10 bytes/row)
        tracker.update_bytes_per_row(10.0, 1000);

        // A has high throughput: 90 rows pruned * 10 bpr = 900 bytes saved / 1sec = 900 bps
        tracker.update(&filter_a, 10, 100, 1_000_000_000);
        // B has low throughput: 50 rows pruned * 10 bpr = 500 bytes saved / 100sec = 5 bps
        tracker.update(&filter_b, 50, 100, 100_000_000_000);

        // Correlation doesn't matter since B is below threshold
        tracker.update_correlation(&filter_a, &filter_b, 8, 100);

        let result =
            tracker.partition_filters_grouped(vec![filter_a.clone(), filter_b.clone()]);

        // A promoted (900 >= 100), B stays post-scan (5 < 100)
        assert_eq!(result.row_filter_groups.len(), 1);
        assert_eq!(result.row_filter_groups[0].len(), 1);
        assert_eq!(result.post_scan.len(), 1);
    }

    #[test]
    fn test_notify_dataset_rows_resolves_fraction() {
        let mut tracker = SelectivityTracker::new_with_config(0.0, 1.5, 100, 0.05);
        // Before notify, effective_min_rows = min_rows_for_collection
        assert_eq!(tracker.effective_min_rows(), 100);

        // 5% of 10_000 = 500, which is > 100
        tracker.notify_dataset_rows(10_000);
        assert_eq!(tracker.effective_min_rows(), 500);
    }

    #[test]
    fn test_notify_dataset_rows_floor_behavior() {
        let mut tracker = SelectivityTracker::new_with_config(0.0, 1.5, 1000, 0.05);
        // 5% of 10_000 = 500, but min_rows = 1000 is larger
        tracker.notify_dataset_rows(10_000);
        assert_eq!(tracker.effective_min_rows(), 1000);
    }

    #[test]
    fn test_notify_dataset_rows_fraction_disabled() {
        let mut tracker = SelectivityTracker::new_with_config(0.0, 1.5, 100, 0.0);
        tracker.notify_dataset_rows(1_000_000);
        // fraction = 0.0, so resolved_min_rows stays None
        assert_eq!(tracker.effective_min_rows(), 100);
    }

    #[test]
    fn test_collection_phase_with_fraction() {
        let filter = make_filter("a", 5);

        let mut tracker = SelectivityTracker::new_with_config(0.0, 1.5, 100, 0.05);
        // 5% of 100_000 = 5000
        tracker.notify_dataset_rows(100_000);
        assert_eq!(tracker.effective_min_rows(), 5000);

        // Record 200 rows — still in collection (200 < 5000)
        tracker.update(&filter, 80, 200, 10_000_000);
        assert!(tracker.in_collection_phase());

        // Record enough to pass threshold
        tracker.update(&filter, 1000, 5000, 50_000_000);
        assert!(!tracker.in_collection_phase());
    }

    // ---- Optional filter drop tests ----

    #[test]
    fn test_optional_filter_dropped_when_ineffective() {
        // Threshold 100 bytes/sec
        let mut tracker = SelectivityTracker::new_with_config(100.0, 1.5, 100, 0.0);

        let mandatory_filter = make_filter("a", 5);
        let inner_optional = make_filter("c", 1);
        let optional_filter: Arc<dyn PhysicalExpr> =
            Arc::new(OptionalFilterPhysicalExpr::new(inner_optional));

        // Feed bytes_per_row into tracker (10 bytes/row)
        tracker.update_bytes_per_row(10.0, 1000);

        // Both have low throughput: 5 bps (50 pruned * 10 bpr = 500 bytes / 100s)
        tracker.update(&mandatory_filter, 50, 100, 100_000_000_000);
        tracker.update(&optional_filter, 50, 100, 100_000_000_000);

        let result = tracker.partition_filters_grouped(vec![
            mandatory_filter.clone(),
            optional_filter.clone(),
        ]);

        // Mandatory filter demoted to post_scan (not dropped)
        assert_eq!(result.post_scan.len(), 1);
        assert!(
            result
                .post_scan
                .iter()
                .any(|f| ExprKey::new(f) == ExprKey::new(&mandatory_filter))
        );
        // Optional filter dropped entirely — not in row_filter_groups or post_scan
        assert!(result.row_filter_groups.is_empty());
        assert!(
            !result
                .post_scan
                .iter()
                .any(|f| ExprKey::new(f) == ExprKey::new(&optional_filter))
        );
    }

    #[test]
    fn test_optional_filter_promoted_when_effective() {
        // Threshold 100 bytes/sec
        let mut tracker = SelectivityTracker::new_with_config(100.0, 1.5, 100, 0.0);

        let inner_optional = make_filter("a", 5);
        let optional_filter: Arc<dyn PhysicalExpr> =
            Arc::new(OptionalFilterPhysicalExpr::new(inner_optional));

        // Feed bytes_per_row into tracker (10 bytes/row)
        tracker.update_bytes_per_row(10.0, 1000);

        // High throughput: 900 bps (90 pruned * 10 bpr = 900 bytes / 1s)
        tracker.update(&optional_filter, 10, 100, 1_000_000_000);

        let result = tracker.partition_filters_grouped(vec![optional_filter.clone()]);

        // Optional filter promoted to row_filter_groups when effective
        assert_eq!(result.row_filter_groups.len(), 1);
        assert_eq!(result.post_scan.len(), 0);
    }

    #[test]
    fn test_mandatory_filter_demoted_not_dropped() {
        // Threshold 100 bytes/sec
        let mut tracker = SelectivityTracker::new_with_config(100.0, 1.5, 100, 0.0);

        let mandatory_filter = make_filter("a", 5);

        // Feed bytes_per_row into tracker (10 bytes/row)
        tracker.update_bytes_per_row(10.0, 1000);

        // Low throughput: 5 bps
        tracker.update(&mandatory_filter, 50, 100, 100_000_000_000);

        let result = tracker.partition_filters_grouped(vec![mandatory_filter.clone()]);

        // Mandatory filter demoted to post_scan, NOT dropped
        assert!(result.row_filter_groups.is_empty());
        assert_eq!(result.post_scan.len(), 1);
    }

    #[test]
    fn test_mixed_optional_mandatory_scenario() {
        // Threshold 100 bytes/sec
        let mut tracker = SelectivityTracker::new_with_config(100.0, 1.5, 100, 0.0);

        let mandatory_effective = make_filter("a", 5);
        let mandatory_ineffective = make_filter("a", 10);
        let optional_effective_inner = make_filter("c", 1);
        let optional_effective: Arc<dyn PhysicalExpr> =
            Arc::new(OptionalFilterPhysicalExpr::new(optional_effective_inner));
        let optional_ineffective_inner = make_filter("d", 2);
        let optional_ineffective: Arc<dyn PhysicalExpr> =
            Arc::new(OptionalFilterPhysicalExpr::new(optional_ineffective_inner));

        // Feed bytes_per_row into tracker (10 bytes/row)
        tracker.update_bytes_per_row(10.0, 1000);

        // mandatory_effective: 900 bps → promoted
        tracker.update(&mandatory_effective, 10, 100, 1_000_000_000);
        // mandatory_ineffective: 5 bps → post_scan
        tracker.update(&mandatory_ineffective, 50, 100, 100_000_000_000);
        // optional_effective: 900 bps → promoted
        tracker.update(&optional_effective, 10, 100, 1_000_000_000);
        // optional_ineffective: 5 bps → DROPPED
        tracker.update(&optional_ineffective, 50, 100, 100_000_000_000);

        let result = tracker.partition_filters_grouped(vec![
            mandatory_effective.clone(),
            mandatory_ineffective.clone(),
            optional_effective.clone(),
            optional_ineffective.clone(),
        ]);

        // 2 effective filters promoted (mandatory_effective + optional_effective)
        let total_promoted: usize =
            result.row_filter_groups.iter().map(|g| g.len()).sum();
        assert_eq!(total_promoted, 2);

        // Only mandatory_ineffective in post_scan (optional_ineffective dropped)
        assert_eq!(result.post_scan.len(), 1);
        assert!(
            result
                .post_scan
                .iter()
                .any(|f| ExprKey::new(f) == ExprKey::new(&mandatory_ineffective))
        );
    }

    // ---- avg_bytes_per_row accumulation tests ----

    #[test]
    fn test_avg_bytes_per_row_empty() {
        let tracker = SelectivityTracker::new(100.0);
        assert!(tracker.avg_bytes_per_row().is_none());
    }

    #[test]
    fn test_avg_bytes_per_row_single_file() {
        let mut tracker = SelectivityTracker::new(100.0);
        // 10 bytes/row, 1000 rows
        tracker.update_bytes_per_row(10.0, 1000);
        let avg = tracker.avg_bytes_per_row().unwrap();
        assert!((avg - 10.0).abs() < 0.001);
    }

    #[test]
    fn test_avg_bytes_per_row_multiple_files() {
        let mut tracker = SelectivityTracker::new(100.0);
        // File 1: 10 bytes/row, 1000 rows → 10_000 total bytes
        tracker.update_bytes_per_row(10.0, 1000);
        // File 2: 20 bytes/row, 3000 rows → 60_000 total bytes
        tracker.update_bytes_per_row(20.0, 3000);
        // Weighted average: 70_000 / 4000 = 17.5
        let avg = tracker.avg_bytes_per_row().unwrap();
        assert!((avg - 17.5).abs() < 0.001);
    }

    #[test]
    fn test_partition_filters_grouped_uses_avg_bytes_per_row() {
        // Verify that partition_filters_grouped uses the internal avg_bytes_per_row
        let mut tracker = SelectivityTracker::new_with_config(100.0, 1.5, 100, 0.0);

        let filter = make_filter("a", 5);

        // No bytes_per_row recorded → can't compute throughput → post_scan
        tracker.update(&filter, 10, 100, 1_000_000_000);
        let result = tracker.partition_filters_grouped(vec![filter.clone()]);
        assert!(result.row_filter_groups.is_empty());
        assert_eq!(result.post_scan.len(), 1);

        // Now feed bytes_per_row → throughput can be computed
        tracker.update_bytes_per_row(10.0, 1000);
        // 90 pruned * 10 bpr = 900 bytes / 1s = 900 bps >= 100 → promoted
        let result = tracker.partition_filters_grouped(vec![filter.clone()]);
        assert_eq!(result.row_filter_groups.len(), 1);
        assert_eq!(result.post_scan.len(), 0);
    }

    #[test]
    fn test_cached_decision_reuse_and_reevaluation() {
        // min_rows_for_collection = 100, so effective_min_rows() = 100
        let mut tracker = SelectivityTracker::new_with_config(100.0, 1.5, 100, 0.0);

        let filter_a = make_filter("a", 5);
        let filter_b = make_filter("a", 10);

        // Feed bytes_per_row
        tracker.update_bytes_per_row(10.0, 1000);

        // filter_a: high throughput (900 bps) → promoted
        tracker.update(&filter_a, 10, 100, 1_000_000_000);
        // filter_b: low throughput (5 bps) → post_scan
        tracker.update(&filter_b, 50, 100, 100_000_000_000);

        // First call after collection: computes fresh decision and caches it.
        let result =
            tracker.partition_filters_grouped(vec![filter_a.clone(), filter_b.clone()]);
        assert_eq!(result.row_filter_groups.len(), 1); // filter_a promoted
        assert_eq!(result.post_scan.len(), 1); // filter_b demoted
        assert!(tracker.cached_decision.is_some());
        let decided_at = tracker.cached_decision.as_ref().unwrap().decided_at_rows;

        // Add a small number of rows (< effective_min_rows = 100)
        tracker.update_bytes_per_row(10.0, 50);

        // Meanwhile make filter_b effective by adding enough high-throughput data
        // to overcome the initial poor stats.
        // After: matched=60, total=10100, nanos=101_000_000_000
        //   pruned=10040, bytes_saved=100400, time=101s → ~994 bps >= 100
        tracker.update(&filter_b, 10, 10000, 1_000_000_000);

        // Second call: should replay cached decision (only 50 rows since last decision).
        let result =
            tracker.partition_filters_grouped(vec![filter_a.clone(), filter_b.clone()]);
        // Still cached: filter_b stays demoted even though its stats improved
        assert_eq!(result.row_filter_groups.len(), 1);
        assert_eq!(result.post_scan.len(), 1);
        // decided_at_rows unchanged
        assert_eq!(
            tracker.cached_decision.as_ref().unwrap().decided_at_rows,
            decided_at
        );

        // Now add enough rows to trigger re-evaluation (>= 100 total since decision)
        tracker.update_bytes_per_row(10.0, 50); // total 100 new rows

        // Third call: re-evaluates because rows_since >= effective_min_rows
        let result =
            tracker.partition_filters_grouped(vec![filter_a.clone(), filter_b.clone()]);
        // Now filter_b should also be promoted
        let total_promoted: usize =
            result.row_filter_groups.iter().map(|g| g.len()).sum();
        assert_eq!(total_promoted, 2);
        assert_eq!(result.post_scan.len(), 0);
        // decided_at_rows updated
        assert!(tracker.cached_decision.as_ref().unwrap().decided_at_rows > decided_at);
    }

    #[test]
    fn test_partition_filters_grouped_compound_demotion() {
        // Threshold 100 bytes/sec
        let mut tracker = SelectivityTracker::new_with_config(100.0, 1.5, 100, 0.0);

        let filter_a = make_filter("a", 5);
        let filter_b = make_filter("a", 10);

        // Feed bytes_per_row (10 bytes/row)
        tracker.update_bytes_per_row(10.0, 1000);

        // Both individual filters have high throughput so they pass individual threshold
        // A: 90 pruned * 10 bpr = 900 bytes / 1s = 900 bps
        tracker.update(&filter_a, 10, 100, 1_000_000_000);
        // B: 90 pruned * 10 bpr = 900 bytes / 1s = 900 bps
        tracker.update(&filter_b, 10, 100, 1_000_000_000);

        // Make A and B correlated so they group together
        // P(A)=0.1, P(B)=0.1, P(A∧B)=0.08
        // ratio = 0.08 / (0.1*0.1) = 8.0 > 1.5
        tracker.update_correlation(&filter_a, &filter_b, 8, 100);

        // Now feed low-throughput stats for the conjunction of A AND B.
        // This simulates a compound filter that is slow overall.
        let combined = conjunction([Arc::clone(&filter_a), Arc::clone(&filter_b)]);
        // 50 pruned * 10 bpr = 500 bytes / 100s = 5 bps (well below 100)
        tracker.update(&combined, 50, 100, 100_000_000_000);

        let result =
            tracker.partition_filters_grouped(vec![filter_a.clone(), filter_b.clone()]);

        // The compound group should be demoted: both filters move to post_scan
        assert!(
            result.row_filter_groups.is_empty(),
            "expected no promoted groups, got {}",
            result.row_filter_groups.len()
        );
        assert_eq!(result.post_scan.len(), 2);
    }
}
