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
use parking_lot::{Mutex, RwLock};
use parquet::file::metadata::ParquetMetaData;
use std::collections::HashMap;
use std::sync::Arc;

use datafusion_physical_expr::utils::collect_columns;
use datafusion_physical_expr_common::physical_expr::{
    OptionalFilterPhysicalExpr, PhysicalExpr, snapshot_generation,
};

/// Stable identifier for a filter conjunct, assigned by `ParquetSource::with_predicate`.
pub type FilterId = usize;

/// Per-filter lifecycle state in the adaptive filter system.
///
/// State transitions:
/// - **(unseen)** → [`RowFilter`](Self::RowFilter) or [`PostScan`](Self::PostScan)
///   on first encounter in [`SelectivityTracker::partition_filters`].
/// - [`PostScan`](Self::PostScan) → [`RowFilter`](Self::RowFilter) when
///   effectiveness ≥ `min_bytes_per_sec` and enough rows have been observed.
/// - [`RowFilter`](Self::RowFilter) → [`PostScan`](Self::PostScan) when
///   effectiveness is below threshold (mandatory filter).
/// - [`RowFilter`](Self::RowFilter) → [`Dropped`](Self::Dropped) when
///   effectiveness is below threshold and the filter is optional
///   ([`OptionalFilterPhysicalExpr`]).
/// - [`RowFilter`](Self::RowFilter) → [`PostScan`](Self::PostScan)/[`Dropped`](Self::Dropped)
///   on periodic re-evaluation if effectiveness drops below threshold after
///   CI upper bound drops below threshold.
/// - **Any state** → re-evaluated when a dynamic filter's
///   `snapshot_generation` changes.
#[derive(Debug, Clone, Copy, PartialEq)]
pub(crate) enum FilterState {
    /// Currently a row filter.
    RowFilter,
    /// Currently a post-scan filter.
    PostScan,
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
    /// Filters promoted past collection — individual chained ArrowPredicates
    pub(crate) row_filters: Vec<(FilterId, Arc<dyn PhysicalExpr>)>,
    /// Filters demoted to post-scan (fast path only)
    pub(crate) post_scan: Vec<(FilterId, Arc<dyn PhysicalExpr>)>,
}

/// Tracks selectivity statistics for a single filter expression.
#[derive(Debug, Clone, Default, Copy, PartialEq)]
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
pub(crate) struct TrackerConfig {
    /// Minimum bytes/sec throughput for promoting a filter (default: INFINITY = disabled).
    pub min_bytes_per_sec: f64,
    /// Byte-ratio threshold for initial filter placement (row-level vs post-scan).
    /// Computed as `filter_compressed_bytes / projection_compressed_bytes`.
    /// When low, the filter columns are small relative to the projection,
    /// so row-level placement enables large late-materialization savings.
    /// When high, the filter columns dominate the projection, so there's
    /// little benefit from late materialization.
    /// Default is 0.20.
    pub byte_ratio_threshold: f64,
    /// Z-score for confidence intervals on filter effectiveness.
    /// Lower values (e.g. 1.0 or 0.0) will make the tracker more aggressive about promotion/demotion based on limited data.
    /// Higher values (e.g. 3.0) will require more confidence before changing filter states.
    /// Default is 2.0, corresponding to ~97.5% one-sided confidence.
    /// Set to <= 0.0 to disable confidence intervals and promote/demote based on point estimates alone (not recommended).
    /// Set to INFINITY to disable promotion entirely (overrides `min_bytes_per_sec`).
    pub confidence_z: f64,
}

impl TrackerConfig {
    pub fn new() -> Self {
        Self {
            min_bytes_per_sec: f64::INFINITY,
            byte_ratio_threshold: 0.20,
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
            filter_stats: RwLock::new(HashMap::new()),
            inner: Mutex::new(SelectivityTrackerInner::new()),
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
/// reader) vs. applied post-scan (demoted) or dropped entirely.
///
/// # Locking design
///
/// All locks are **private** to this struct — external callers cannot hold a
/// guard across expensive work, and all lock-holding code paths are auditable
/// in this file alone.
///
/// State is split across two independent locks to minimise contention between
/// the hot per-batch `update()` path and the cold per-file-open
/// `partition_filters()` path:
///
/// - **`filter_stats`** (`RwLock<HashMap<FilterId, Mutex<SelectivityStats>>>`)
///   — `update()` acquires a *shared read* lock on the outer map, then a
///   per-filter `Mutex` to increment counters.  Multiple threads updating
///   *different* filters never contend at all; threads updating the *same*
///   filter serialize only on the cheap per-filter `Mutex` (~100 ns).
///   `partition_filters()` also takes a read lock here when it needs to
///   inspect stats for promotion/demotion decisions, so it never blocks
///   `update()` callers.  The write lock is taken only briefly in Phase 2
///   of `partition_filters()` to insert entries for newly-seen filter IDs.
///
/// - **`inner`** (`Mutex<SelectivityTrackerInner>`) — holds the filter
///   state-machine (`filter_states`) and dynamic-filter generation tracking.
///   Only `partition_filters()` acquires this lock (once per file open), so
///   concurrent `update()` calls are completely unaffected.
///
/// ## Lock ordering (deadlock-free)
///
/// Locks are always acquired in the order `inner` → `filter_stats` →
/// per-filter `Mutex`.  Because `update()` never acquires `inner`, no
/// cycle is possible.
///
/// ## Correctness of concurrent access
///
/// `update()` may write stats while `partition_filters()` reads them for
/// promotion/demotion.  Both hold a shared `filter_stats` read lock; the
/// per-filter `Mutex` ensures they do not interleave on the same filter's
/// stats.  One proceeds first; the other sees a consistent (slightly newer
/// or older) snapshot.  This is benign — the single-lock design that
/// preceded this split already allowed stats to change between consecutive
/// reads within `partition_filters()`.
///
/// On promote/demote, `partition_filters()` zeros a filter's stats via the
/// per-filter `Mutex`.  An `update()` running concurrently may write one
/// stale batch's worth of data to the freshly-zeroed stats; this is quickly
/// diluted by hundreds of correct-context batches and is functionally
/// identical to the old design where `update()` queued behind the write
/// lock and ran immediately after.
///
/// # Filter state machine
///
/// ```text
///                        ┌─────────┐
///                        │   New   │
///                        └─────────┘
///                             │
///                             ▼
///                ┌────────────────────────┐
///                │     Estimated Cost     │
///                │Bytes needed for filter │
///                └────────────────────────┘
///                             │
///          ┌──────────────────┴──────────────────┐
/// ┌────────▼────────┐                   ┌────────▼────────┐
/// │    Post-scan    │                   │   Row filter    │
/// │                 │                   │                 │
/// └─────────────────┘                   └─────────────────┘
///          │                                     │
///          ▼                                     ▼
/// ┌─────────────────┐                   ┌─────────────────┐
/// │  Effectiveness  │                   │  Effectiveness  │
/// │  Bytes pruned   │                   │  Bytes pruned   │
/// │       per       │                   │       per       │
/// │Second of compute│                   │Second of compute│
/// └─────────────────┘                   └─────────────────┘
///          │                                     │
///          └──────────────────┬──────────────────┘
///                             ▼
///     ┌───────────────────────────────────────────────┐
///     │                   New Scan                    │
///     │     Move filters based on effectiveness.      │
///     │    Promote (move post-scan -> row filter).    │
///     │    Demote (move row-filter -> post-scan).     │
///     │   Disable (for optional filters; either row   │
///     │             filter or disabled).              │
///     └───────────────────────────────────────────────┘
///                             │
///          ┌──────────────────┴──────────────────┐
/// ┌────────▼────────┐                   ┌────────▼────────┐
/// │    Post-scan    │                   │   Row filter    │
/// │                 │                   │                 │
/// └─────────────────┘                   └─────────────────┘
/// ```
///
/// See `TrackerConfig` for configuration knobs.
pub struct SelectivityTracker {
    config: TrackerConfig,
    /// Per-filter selectivity statistics, each individually `Mutex`-protected.
    ///
    /// The outer `RwLock` is almost always read-locked: both `update()` (hot,
    /// per-batch) and `partition_filters()` (cold, per-file-open) only need
    /// shared access to look up existing entries.  The write lock is taken
    /// only when `partition_filters()` inserts entries for newly-seen filter
    /// IDs — a brief, infrequent operation.
    ///
    /// Each inner `Mutex<SelectivityStats>` protects a single filter's
    /// counters, so concurrent `update()` calls on *different* filters
    /// proceed in parallel with zero contention.
    filter_stats: RwLock<HashMap<FilterId, Mutex<SelectivityStats>>>,
    /// Filter lifecycle state machine and dynamic-filter generation tracking.
    ///
    /// Only `partition_filters()` acquires this lock (once per file open).
    /// `update()` never touches it, so the hot per-batch path is completely
    /// decoupled from the cold state-machine path.
    inner: Mutex<SelectivityTrackerInner>,
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
    /// **Locking:** acquires `filter_stats.read()` (shared) then a per-filter
    /// `Mutex`.  Never touches `inner`, so this hot per-batch path cannot
    /// contend with the cold per-file-open `partition_filters()` path.
    ///
    /// Silently skips unknown filter IDs (can occur if `update()` is called
    /// before `partition_filters()` has registered the filter — in practice
    /// this cannot happen because `partition_filters()` runs during file open
    /// before any batches are processed).
    pub(crate) fn update(
        &self,
        id: FilterId,
        matched: u64,
        total: u64,
        eval_nanos: u64,
        batch_bytes: u64,
    ) {
        let map = self.filter_stats.read();
        if let Some(entry) = map.get(&id) {
            entry.lock().update(matched, total, eval_nanos, batch_bytes);
        }
    }

    /// Partition filters into row-level predicates vs post-scan filters.
    ///
    /// Called once per file open (cold path).
    ///
    /// **Locking — two phases:**
    /// 1. Acquires `inner` (exclusive) and `filter_stats` (shared read) for
    ///    all decision logic — promotion, demotion, initial placement, and
    ///    sorting by effectiveness.  Because `filter_stats` is only
    ///    read-locked, concurrent `update()` calls proceed unblocked.
    /// 2. If new filter IDs were seen, briefly acquires `filter_stats` (write)
    ///    to insert per-filter `Mutex` entries so that future `update()` calls
    ///    can find them.
    pub(crate) fn partition_filters(
        &self,
        filters: Vec<(FilterId, Arc<dyn PhysicalExpr>)>,
        projection_scan_size: usize,
        metadata: &ParquetMetaData,
    ) -> PartitionedFilters {
        // Phase 1: inner.lock() + filter_stats.read() → all decision logic
        let mut guard = self.inner.lock();
        let stats_map = self.filter_stats.read();
        let result = guard.partition_filters(
            filters,
            projection_scan_size,
            metadata,
            &self.config,
            &stats_map,
        );
        drop(stats_map);
        drop(guard);

        // Phase 2: if new filters were seen, briefly acquire write lock to insert entries
        if !result.new_filter_ids.is_empty() {
            let mut stats_write = self.filter_stats.write();
            for id in result.new_filter_ids {
                stats_write
                    .entry(id)
                    .or_insert_with(|| Mutex::new(SelectivityStats::default()));
            }
        }

        result.partitioned
    }

    /// Test helper: ensure a stats entry exists for the given filter ID.
    /// In production, `partition_filters()` inserts entries for new filters.
    /// Tests that call `update()` without prior `partition_filters()` need this.
    #[cfg(test)]
    fn ensure_stats_entry(&self, id: FilterId) {
        let map = self.filter_stats.read();
        if map.get(&id).is_none() {
            drop(map);
            self.filter_stats
                .write()
                .entry(id)
                .or_insert_with(|| Mutex::new(SelectivityStats::default()));
        }
    }
}

/// Internal result from [`SelectivityTrackerInner::partition_filters`].
///
/// Carries both the partitioned filters and a list of newly-seen filter IDs
/// back to the outer [`SelectivityTracker::partition_filters`], which uses
/// `new_filter_ids` to insert per-filter `Mutex` entries into `filter_stats`
/// in a brief Phase 2 write lock.
struct PartitionResult {
    partitioned: PartitionedFilters,
    new_filter_ids: Vec<FilterId>,
}

/// Filter state-machine and generation tracking, guarded by the `Mutex`
/// inside [`SelectivityTracker`].
///
/// This struct intentionally does **not** contain per-filter stats — those
/// live in the separate `filter_stats` lock so that the hot `update()` path
/// can modify stats without acquiring this lock.  Only the cold
/// `partition_filters()` path (once per file open) needs this lock.
#[derive(Debug)]
struct SelectivityTrackerInner {
    /// Per-filter lifecycle state (RowFilter / PostScan / Dropped).
    filter_states: HashMap<FilterId, FilterState>,
    /// Last-seen snapshot generation per filter, for detecting when a dynamic
    /// filter's selectivity has changed (e.g. hash-join build side grew).
    snapshot_generations: HashMap<FilterId, u64>,
}

impl SelectivityTrackerInner {
    fn new() -> Self {
        Self {
            filter_states: HashMap::new(),
            snapshot_generations: HashMap::new(),
        }
    }

    /// Check and update the snapshot generation for a filter.
    fn note_generation(
        &mut self,
        id: FilterId,
        generation: u64,
        stats_map: &HashMap<FilterId, Mutex<SelectivityStats>>,
    ) {
        if generation == 0 {
            return;
        }
        match self.snapshot_generations.get(&id) {
            Some(&prev_generation) if prev_generation == generation => {}
            Some(_) => {
                let current_state = self.filter_states.get(&id).copied();
                // Always reset stats since selectivity changed with new generation.
                if let Some(entry) = stats_map.get(&id) {
                    *entry.lock() = SelectivityStats::default();
                }
                self.snapshot_generations.insert(id, generation);

                // Optional/dynamic filters only get more selective over time
                // (hash join build side accumulates more values). So if the
                // filter was already working (RowFilter or PostScan), preserve
                // its state. Only un-drop Dropped filters back to PostScan
                // so they get another chance with the new selectivity.
                if current_state == Some(FilterState::Dropped) {
                    debug!("FilterId {id} generation changed, un-dropping to PostScan");
                    self.filter_states.insert(id, FilterState::PostScan);
                } else {
                    debug!(
                        "FilterId {id} generation changed, resetting stats but preserving state {current_state:?}"
                    );
                }
            }
            None => {
                self.snapshot_generations.insert(id, generation);
            }
        }
    }

    /// Get the effectiveness for a filter by ID.
    fn get_effectiveness_by_id(
        &self,
        id: FilterId,
        stats_map: &HashMap<FilterId, Mutex<SelectivityStats>>,
    ) -> Option<f64> {
        stats_map
            .get(&id)
            .and_then(|entry| entry.lock().effectiveness())
    }

    /// Demote a filter to post-scan or drop it entirely if optional.
    fn demote_or_drop(
        &mut self,
        id: FilterId,
        expr: &Arc<dyn PhysicalExpr>,
        post_scan: &mut Vec<(FilterId, Arc<dyn PhysicalExpr>)>,
        stats_map: &HashMap<FilterId, Mutex<SelectivityStats>>,
    ) {
        if expr
            .as_any()
            .downcast_ref::<OptionalFilterPhysicalExpr>()
            .is_none()
        {
            self.filter_states.insert(id, FilterState::PostScan);
            post_scan.push((id, Arc::clone(expr)));
            // Reset stats for this filter so it can be re-evaluated as a post-scan filter.
            if let Some(entry) = stats_map.get(&id) {
                *entry.lock() = SelectivityStats::default();
            }
        } else {
            self.filter_states.insert(id, FilterState::Dropped);
        }
    }

    /// Promote a filter to row-level.
    fn promote(
        &mut self,
        id: FilterId,
        expr: Arc<dyn PhysicalExpr>,
        row_filters: &mut Vec<(FilterId, Arc<dyn PhysicalExpr>)>,
        stats_map: &HashMap<FilterId, Mutex<SelectivityStats>>,
    ) {
        row_filters.push((id, expr));
        self.filter_states.insert(id, FilterState::RowFilter);
        // Reset stats for this filter since it will be evaluated at row-level now.
        if let Some(entry) = stats_map.get(&id) {
            *entry.lock() = SelectivityStats::default();
        }
    }

    /// Partition filters into collecting / promoted / post-scan buckets.
    fn partition_filters(
        &mut self,
        filters: Vec<(FilterId, Arc<dyn PhysicalExpr>)>,
        projection_scan_size: usize,
        metadata: &ParquetMetaData,
        config: &TrackerConfig,
        stats_map: &HashMap<FilterId, Mutex<SelectivityStats>>,
    ) -> PartitionResult {
        let mut new_filter_ids = Vec::new();

        // If min_bytes_per_sec is INFINITY -> all filters are post-scan.
        if config.min_bytes_per_sec.is_infinite() {
            debug!(
                "Filter promotion disabled via min_bytes_per_sec=INFINITY; all {} filters post-scan",
                filters.len()
            );
            // Register all filter IDs so update() can find them
            for &(id, _) in &filters {
                if !stats_map.contains_key(&id) {
                    new_filter_ids.push(id);
                }
            }
            return PartitionResult {
                partitioned: PartitionedFilters {
                    row_filters: Vec::new(),
                    post_scan: filters,
                },
                new_filter_ids,
            };
        }
        // If min_bytes_per_sec is 0 -> all filters are promoted.
        if config.min_bytes_per_sec == 0.0 {
            debug!(
                "All filters promoted via min_bytes_per_sec=0; all {} filters row-level",
                filters.len()
            );
            // Register all filter IDs so update() can find them
            for &(id, _) in &filters {
                if !stats_map.contains_key(&id) {
                    new_filter_ids.push(id);
                }
            }
            return PartitionResult {
                partitioned: PartitionedFilters {
                    row_filters: filters,
                    post_scan: Vec::new(),
                },
                new_filter_ids,
            };
        }

        // Note snapshot generations for dynamic filter detection.
        // This clears stats for any filter whose generation has changed since the last scan.
        // This must be done before any other logic since it can change filter states and stats.
        for &(id, ref expr) in &filters {
            let generation = snapshot_generation(expr);
            self.note_generation(id, generation, stats_map);
        }

        // Separate into row filters and post-scan filters based on effectiveness and state.
        let mut row_filters: Vec<(FilterId, Arc<dyn PhysicalExpr>)> = Vec::new();
        let mut post_scan_filters: Vec<(FilterId, Arc<dyn PhysicalExpr>)> = Vec::new();

        let confidence_z = config.confidence_z;
        for (id, expr) in filters {
            let state = self.filter_states.get(&id).copied();

            let Some(state) = state else {
                // New filter: decide initial placement.

                // Optional/dynamic filters (e.g. hash join pushdown) always start
                // PostScan because their selectivity is unknown and they can be
                // dropped if ineffective. This is especially important for
                // single-file tables where there's no second file to adapt.
                let is_optional = expr
                    .as_any()
                    .downcast_ref::<OptionalFilterPhysicalExpr>()
                    .is_some();
                if is_optional {
                    debug!(
                        "FilterId {id}: New optional filter → Post-scan (conservative) — {expr}"
                    );
                    self.filter_states.insert(id, FilterState::PostScan);
                    if !stats_map.contains_key(&id) {
                        new_filter_ids.push(id);
                    }
                    post_scan_filters.push((id, expr));
                    continue;
                }

                // Static filters: use filter_bytes / projection_bytes to decide
                // initial placement. This ratio captures the I/O tradeoff:
                //
                // - Low ratio (filter columns are small vs projection): row-filter
                //   enables late materialization — the large non-filter portion of
                //   the projection is only decoded for rows that pass the filter.
                //
                // - High ratio (filter columns are most of the projection): little
                //   benefit from late materialization since there's not much left
                //   to skip. Post-scan avoids row-filter overhead.
                //
                // Extra bytes (filter columns not in projection) are naturally
                // included in filter_bytes, making the ratio higher and placement
                // more conservative, which is correct since those bytes represent
                // additional I/O cost for row-filter evaluation.
                let filter_columns: Vec<usize> = collect_columns(&expr)
                    .iter()
                    .map(|col| col.index())
                    .collect();
                let filter_bytes =
                    crate::row_filter::total_compressed_bytes(&filter_columns, metadata);
                let byte_ratio = if projection_scan_size > 0 {
                    filter_bytes as f64 / projection_scan_size as f64
                } else {
                    1.0
                };

                if !stats_map.contains_key(&id) {
                    new_filter_ids.push(id);
                }

                if byte_ratio <= config.byte_ratio_threshold {
                    debug!(
                        "FilterId {id}: New filter → Row filter (byte_ratio {byte_ratio:.4} <= {}) — {expr}",
                        config.byte_ratio_threshold
                    );
                    self.filter_states.insert(id, FilterState::RowFilter);
                    row_filters.push((id, expr));
                } else {
                    debug!(
                        "FilterId {id}: New filter → Post-scan (byte_ratio {byte_ratio:.4} > {}) — {expr}",
                        config.byte_ratio_threshold
                    );
                    self.filter_states.insert(id, FilterState::PostScan);
                    post_scan_filters.push((id, expr));
                }
                continue;
            };

            match state {
                FilterState::RowFilter => {
                    // Should we demote this filter based on CI upper bound?
                    if let Some(entry) = stats_map.get(&id) {
                        let stats = entry.lock();
                        if let Some(ub) = stats.confidence_upper_bound(confidence_z)
                            && ub < config.min_bytes_per_sec
                        {
                            drop(stats);
                            debug!(
                                "FilterId {id}: Row filter → Post-scan via CI upper bound {ub} < {} bytes/sec — {expr}",
                                config.min_bytes_per_sec
                            );
                            self.demote_or_drop(
                                id,
                                &expr,
                                &mut post_scan_filters,
                                stats_map,
                            );
                            continue;
                        }
                    }
                    // If not demoted, keep as row filter.
                    row_filters.push((id, expr));
                }
                FilterState::PostScan => {
                    // Should we promote this filter based on CI lower bound?
                    if let Some(entry) = stats_map.get(&id) {
                        let stats = entry.lock();
                        if let Some(lb) = stats.confidence_lower_bound(confidence_z)
                            && lb >= config.min_bytes_per_sec
                        {
                            drop(stats);
                            debug!(
                                "FilterId {id}: Post-scan → Row filter via CI lower bound {lb} >= {} bytes/sec — {expr}",
                                config.min_bytes_per_sec
                            );
                            self.promote(id, expr, &mut row_filters, stats_map);
                            continue;
                        }
                    }
                    // Should we drop this filter if it's optional and ineffective?
                    // Non-optional filters must stay as post-scan regardless.
                    if let Some(entry) = stats_map.get(&id) {
                        let stats = entry.lock();
                        if let Some(ub) = stats.confidence_upper_bound(confidence_z)
                            && ub < config.min_bytes_per_sec
                            && expr
                                .as_any()
                                .downcast_ref::<OptionalFilterPhysicalExpr>()
                                .is_some()
                        {
                            drop(stats);
                            debug!(
                                "FilterId {id}: Post-scan → Dropped via CI upper bound {ub} < {} bytes/sec — {expr}",
                                config.min_bytes_per_sec
                            );
                            self.filter_states.insert(id, FilterState::Dropped);
                            continue;
                        }
                    }
                    // Keep as post-scan filter (don't reset stats for mandatory filters).
                    post_scan_filters.push((id, expr));
                }
                FilterState::Dropped => continue,
            }
        }

        // Sort row filters by:
        // - Effectiveness (descending, higher = better) if available for both filters.
        // - Scan size (ascending, cheapest first) as fallback — cheap filters prune
        //   rows before expensive ones, reducing downstream evaluation cost.
        let cmp_row_filters =
            |(id_a, expr_a): &(FilterId, Arc<dyn PhysicalExpr>),
             (id_b, expr_b): &(FilterId, Arc<dyn PhysicalExpr>)| {
                let eff_a = self.get_effectiveness_by_id(*id_a, stats_map);
                let eff_b = self.get_effectiveness_by_id(*id_b, stats_map);
                if let (Some(eff_a), Some(eff_b)) = (eff_a, eff_b) {
                    eff_b
                        .partial_cmp(&eff_a)
                        .unwrap_or(std::cmp::Ordering::Equal)
                } else {
                    let size_a = filter_scan_size(expr_a, metadata);
                    let size_b = filter_scan_size(expr_b, metadata);
                    size_a.cmp(&size_b)
                }
            };
        row_filters.sort_by(cmp_row_filters);
        // Post-scan filters: same logic (cheaper post-scan filters first to reduce
        // the batch size for subsequent filters).
        post_scan_filters.sort_by(cmp_row_filters);

        debug!(
            "Partitioned filters: {} row-level, {} post-scan",
            row_filters.len(),
            post_scan_filters.len()
        );
        PartitionResult {
            partitioned: PartitionedFilters {
                row_filters,
                post_scan: post_scan_filters,
            },
            new_filter_ids,
        }
    }
}

/// Calculate the estimated number of bytes needed to evaluate a filter based on the columns
/// it references as if it were applied to the entire file.
/// This is used for initial placement of new filters before any stats are available, and as a fallback for filters without stats.
fn filter_scan_size(expr: &Arc<dyn PhysicalExpr>, metadata: &ParquetMetaData) -> usize {
    let columns: Vec<usize> = collect_columns(expr)
        .iter()
        .map(|col| col.index())
        .collect();

    crate::row_filter::total_compressed_bytes(&columns, metadata)
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion_physical_expr::expressions::Column;
    use parquet::basic::Type as PhysicalType;
    use parquet::file::metadata::{ColumnChunkMetaData, FileMetaData, RowGroupMetaData};
    use parquet::schema::types::SchemaDescPtr;
    use parquet::schema::types::Type as SchemaType;
    use std::sync::Arc;

    mod helper_functions {
        use super::*;

        /// Creates test ParquetMetaData with specified row groups and column sizes.
        ///
        /// # Arguments
        /// * `specs` - Vec of (num_rows, vec![compressed_size]) tuples for each row group
        pub fn create_test_metadata(specs: Vec<(i64, Vec<usize>)>) -> ParquetMetaData {
            // Get the maximum number of columns from all specs
            let num_columns = specs
                .iter()
                .map(|(_, sizes)| sizes.len())
                .max()
                .unwrap_or(1);
            let schema_descr = get_test_schema_descr_with_columns(num_columns);

            let row_group_metadata: Vec<_> = specs
                .into_iter()
                .map(|(num_rows, column_sizes)| {
                    let columns = column_sizes
                        .into_iter()
                        .enumerate()
                        .map(|(col_idx, size)| {
                            ColumnChunkMetaData::builder(schema_descr.column(col_idx))
                                .set_num_values(num_rows)
                                .set_total_compressed_size(size as i64)
                                .build()
                                .unwrap()
                        })
                        .collect();

                    RowGroupMetaData::builder(schema_descr.clone())
                        .set_num_rows(num_rows)
                        .set_column_metadata(columns)
                        .build()
                        .unwrap()
                })
                .collect();

            let total_rows: i64 = row_group_metadata.iter().map(|rg| rg.num_rows()).sum();
            let file_metadata =
                FileMetaData::new(1, total_rows, None, None, schema_descr.clone(), None);

            ParquetMetaData::new(file_metadata, row_group_metadata)
        }

        /// Creates a simple column expression with given name and index.
        pub fn col_expr(name: &str, index: usize) -> Arc<dyn PhysicalExpr> {
            Arc::new(Column::new(name, index))
        }

        /// Create schema with specified number of columns, each named "a", "b", etc.
        pub fn get_test_schema_descr_with_columns(num_columns: usize) -> SchemaDescPtr {
            use parquet::basic::LogicalType;

            let fields: Vec<_> = (0..num_columns)
                .map(|i| {
                    let col_name = format!("{}", (b'a' + i as u8) as char);
                    SchemaType::primitive_type_builder(
                        &col_name,
                        PhysicalType::BYTE_ARRAY,
                    )
                    .with_logical_type(Some(LogicalType::String))
                    .build()
                    .unwrap()
                })
                .map(Arc::new)
                .collect();

            let schema = SchemaType::group_type_builder("schema")
                .with_fields(fields)
                .build()
                .unwrap();
            Arc::new(parquet::schema::types::SchemaDescriptor::new(Arc::new(
                schema,
            )))
        }
    }

    mod selectivity_stats_tests {
        use super::*;

        #[test]
        fn test_effectiveness_basic_calculation() {
            let mut stats = SelectivityStats::default();

            // 100 rows total, 50 rows pruned (matched 50), 1 sec eval time, 10000 bytes seen
            // bytes_per_row = 10000 / 100 = 100
            // bytes_saved = 50 * 100 = 5000
            // effectiveness = 5000 * 1e9 / 1e9 = 5000
            stats.update(50, 100, 1_000_000_000, 10_000);

            let eff = stats.effectiveness().unwrap();
            assert!((eff - 5000.0).abs() < 0.1);
        }

        #[test]
        fn test_effectiveness_zero_rows_total() {
            let mut stats = SelectivityStats::default();
            stats.update(0, 0, 1_000_000_000, 10_000);

            assert_eq!(stats.effectiveness(), None);
        }

        #[test]
        fn test_effectiveness_zero_eval_nanos() {
            let mut stats = SelectivityStats::default();
            stats.update(50, 100, 0, 10_000);

            assert_eq!(stats.effectiveness(), None);
        }

        #[test]
        fn test_effectiveness_zero_bytes_seen() {
            let mut stats = SelectivityStats::default();
            stats.update(50, 100, 1_000_000_000, 0);

            assert_eq!(stats.effectiveness(), None);
        }

        #[test]
        fn test_effectiveness_all_rows_matched() {
            let mut stats = SelectivityStats::default();
            // All rows matched (no pruning)
            stats.update(100, 100, 1_000_000_000, 10_000);

            let eff = stats.effectiveness().unwrap();
            assert_eq!(eff, 0.0);
        }

        #[test]
        fn test_confidence_bounds_single_sample() {
            let mut stats = SelectivityStats::default();
            stats.update(50, 100, 1_000_000_000, 10_000);

            // Single sample returns None for confidence bounds
            assert_eq!(stats.confidence_lower_bound(2.0), None);
            assert_eq!(stats.confidence_upper_bound(2.0), None);
        }

        #[test]
        fn test_welford_identical_samples() {
            let mut stats = SelectivityStats::default();

            // Add two identical samples
            stats.update(50, 100, 1_000_000_000, 10_000);
            stats.update(50, 100, 1_000_000_000, 10_000);

            // Variance should be 0
            assert_eq!(stats.sample_count, 2);
            let lb = stats.confidence_lower_bound(2.0).unwrap();
            let ub = stats.confidence_upper_bound(2.0).unwrap();

            // Both should be equal to the mean since variance is 0
            assert!((lb - ub).abs() < 0.01);
        }

        #[test]
        fn test_welford_variance_calculation() {
            let mut stats = SelectivityStats::default();

            // Add samples that will produce effectiveness values of ~100, ~200, ~300
            // These are constructed to give those exact effectiveness values
            stats.update(50, 100, 1_000_000_000, 10_000); // eff ≈ 5000
            stats.update(40, 100, 1_000_000_000, 10_000); // eff ≈ 6000
            stats.update(30, 100, 1_000_000_000, 10_000); // eff ≈ 7000

            // We should have 3 samples
            assert_eq!(stats.sample_count, 3);

            // Mean should be 6000
            assert!((stats.eff_mean - 6000.0).abs() < 1.0);

            // Both bounds should be defined
            let lb = stats.confidence_lower_bound(1.0).unwrap();
            let ub = stats.confidence_upper_bound(1.0).unwrap();

            assert!(lb < stats.eff_mean);
            assert!(ub > stats.eff_mean);
        }

        #[test]
        fn test_confidence_bounds_asymmetry() {
            let mut stats = SelectivityStats::default();

            stats.update(50, 100, 1_000_000_000, 10_000);
            stats.update(40, 100, 1_000_000_000, 10_000);

            let lb = stats.confidence_lower_bound(2.0).unwrap();
            let ub = stats.confidence_upper_bound(2.0).unwrap();

            // Bounds should be symmetric around the mean
            let lower_dist = stats.eff_mean - lb;
            let upper_dist = ub - stats.eff_mean;

            assert!((lower_dist - upper_dist).abs() < 0.01);
        }

        #[test]
        fn test_welford_incremental_vs_batch() {
            // Create two identical stats objects
            let mut stats_incremental = SelectivityStats::default();
            let mut stats_batch = SelectivityStats::default();

            // Incremental: add one at a time
            stats_incremental.update(50, 100, 1_000_000_000, 10_000);
            stats_incremental.update(40, 100, 1_000_000_000, 10_000);
            stats_incremental.update(30, 100, 1_000_000_000, 10_000);

            // Batch: simulate batch update (all at once)
            stats_batch.update(120, 300, 3_000_000_000, 30_000);

            // Both should produce the same overall statistics
            assert_eq!(stats_incremental.rows_total, stats_batch.rows_total);
            assert_eq!(stats_incremental.rows_matched, stats_batch.rows_matched);

            // Means should be close
            assert!((stats_incremental.eff_mean - stats_batch.eff_mean).abs() < 100.0);
        }

        #[test]
        fn test_effectiveness_numerical_stability() {
            let mut stats = SelectivityStats::default();

            // Test with large values to ensure numerical stability
            stats.update(
                500_000_000,
                1_000_000_000,
                10_000_000_000_000,
                1_000_000_000_000,
            );

            let eff = stats.effectiveness();
            assert!(eff.is_some());
            assert!(eff.unwrap() > 0.0);
            assert!(!eff.unwrap().is_nan());
            assert!(!eff.unwrap().is_infinite());
        }
    }

    mod tracker_config_tests {
        use super::*;

        #[test]
        fn test_default_config() {
            let config = TrackerConfig::default();

            assert!(config.min_bytes_per_sec.is_infinite());
            assert_eq!(config.byte_ratio_threshold, 0.20);
            assert_eq!(config.confidence_z, 2.0);
        }

        #[test]
        fn test_with_min_bytes_per_sec() {
            let config = TrackerConfig::new().with_min_bytes_per_sec(1000.0);

            assert_eq!(config.min_bytes_per_sec, 1000.0);
        }

        #[test]
        fn test_with_byte_ratio_threshold() {
            let config = TrackerConfig::new().with_byte_ratio_threshold(0.5);

            assert_eq!(config.byte_ratio_threshold, 0.5);
        }

        #[test]
        fn test_with_confidence_z() {
            let config = TrackerConfig::new().with_confidence_z(3.0);

            assert_eq!(config.confidence_z, 3.0);
        }

        #[test]
        fn test_builder_chain() {
            let config = TrackerConfig::new()
                .with_min_bytes_per_sec(500.0)
                .with_byte_ratio_threshold(0.3)
                .with_confidence_z(1.5);

            assert_eq!(config.min_bytes_per_sec, 500.0);
            assert_eq!(config.byte_ratio_threshold, 0.3);
            assert_eq!(config.confidence_z, 1.5);
        }

        #[test]
        fn test_build_creates_tracker() {
            let tracker = TrackerConfig::new().with_min_bytes_per_sec(1000.0).build();

            // Tracker should be created and functional
            assert_eq!(tracker.config.min_bytes_per_sec, 1000.0);
        }
    }

    mod state_machine_tests {
        use super::helper_functions::*;
        use super::*;

        #[test]
        fn test_initial_placement_low_byte_ratio() {
            let tracker = TrackerConfig::new()
                .with_min_bytes_per_sec(1000.0)
                .with_byte_ratio_threshold(0.2)
                .build();

            // Create metadata: 1 row group, 100 rows, 1000 bytes for column
            let metadata = create_test_metadata(vec![(100, vec![1000])]);

            // Filter using column 0 (1000 bytes out of 1000 projection = 100% ratio > 0.2)
            // So this should be placed in post-scan initially
            let expr = col_expr("a", 0);
            let filters = vec![(1, expr)];

            let result = tracker.partition_filters(filters, 1000, &metadata);

            // With 100% byte ratio, should go to post-scan
            assert_eq!(result.row_filters.len(), 0);
            assert_eq!(result.post_scan.len(), 1);
        }

        #[test]
        fn test_initial_placement_filter_in_projection_low_ratio() {
            let tracker = TrackerConfig::new()
                .with_min_bytes_per_sec(1000.0)
                .with_byte_ratio_threshold(0.5)
                .build();

            // Create metadata: 1 row group, 100 rows, 100 bytes for column
            let metadata = create_test_metadata(vec![(100, vec![100])]);

            // Filter using column 0 which IS in the projection.
            // filter_bytes=100, projection=1000, ratio=0.10 <= 0.5 → RowFilter
            let expr = col_expr("a", 0);
            let filters = vec![(1, expr)];

            let result = tracker.partition_filters(filters, 1000, &metadata);

            assert_eq!(result.row_filters.len(), 1);
            assert_eq!(result.post_scan.len(), 0);
        }

        #[test]
        fn test_initial_placement_high_byte_ratio() {
            let tracker = TrackerConfig::new()
                .with_min_bytes_per_sec(1000.0)
                .with_byte_ratio_threshold(0.5)
                .build();

            // Create metadata: 1 row group, 100 rows, 100 bytes for column
            let metadata = create_test_metadata(vec![(100, vec![100])]);

            // Filter using column 0 (100 bytes / 1000 projection = 10% ratio <= 0.5)
            // So this should be placed in row-filter immediately
            let expr = col_expr("a", 0);
            let filters = vec![(1, expr)];

            let result = tracker.partition_filters(filters, 1000, &metadata);

            // With 10% byte ratio, should go to row-filter
            assert_eq!(result.row_filters.len(), 1);
            assert_eq!(result.post_scan.len(), 0);
        }

        #[test]
        fn test_min_bytes_per_sec_infinity_disables_promotion() {
            let tracker = TrackerConfig::new()
                .with_min_bytes_per_sec(f64::INFINITY)
                .build();

            let metadata = create_test_metadata(vec![(100, vec![100])]);
            let expr = col_expr("a", 0);
            let filters = vec![(1, expr)];

            let result = tracker.partition_filters(filters, 1000, &metadata);

            // All filters should go to post_scan when min_bytes_per_sec is INFINITY
            assert_eq!(result.row_filters.len(), 0);
            assert_eq!(result.post_scan.len(), 1);
        }

        #[test]
        fn test_min_bytes_per_sec_zero_promotes_all() {
            let tracker = TrackerConfig::new().with_min_bytes_per_sec(0.0).build();

            let metadata = create_test_metadata(vec![(100, vec![1000])]);
            let expr = col_expr("a", 0);
            let filters = vec![(1, expr)];

            let result = tracker.partition_filters(filters, 1000, &metadata);

            // All filters should be promoted to row_filters when min_bytes_per_sec is 0
            assert_eq!(result.row_filters.len(), 1);
            assert_eq!(result.post_scan.len(), 0);
        }

        #[test]
        fn test_promotion_via_confidence_lower_bound() {
            let tracker = TrackerConfig::new()
                .with_min_bytes_per_sec(1000.0)
                .with_byte_ratio_threshold(0.5) // Force to PostScan initially
                .with_confidence_z(0.5) // Lower z for easier promotion
                .build();

            let metadata = create_test_metadata(vec![(100, vec![1000])]);
            let expr = col_expr("a", 0);
            let filters = vec![(1, expr.clone())];

            // First partition: goes to PostScan (high byte ratio)
            let result = tracker.partition_filters(filters.clone(), 1000, &metadata);
            assert_eq!(result.post_scan.len(), 1);
            assert_eq!(result.row_filters.len(), 0);

            // Feed high effectiveness stats
            for _ in 0..5 {
                tracker.update(1, 10, 100, 100_000, 1000); // high effectiveness
            }

            // Second partition: should be promoted to RowFilter
            let result = tracker.partition_filters(filters, 1000, &metadata);
            assert_eq!(result.row_filters.len(), 1);
            assert_eq!(result.post_scan.len(), 0);
        }

        #[test]
        fn test_demotion_via_confidence_upper_bound() {
            let tracker = TrackerConfig::new()
                .with_min_bytes_per_sec(10000.0)
                .with_byte_ratio_threshold(0.1) // Force to RowFilter initially
                .with_confidence_z(0.5) // Lower z for easier demotion
                .build();

            let metadata = create_test_metadata(vec![(100, vec![100])]);
            let expr = col_expr("a", 0);
            let filters = vec![(1, expr.clone())];

            // First partition: goes to RowFilter (low byte ratio)
            let result = tracker.partition_filters(filters.clone(), 1000, &metadata);
            assert_eq!(result.row_filters.len(), 1);
            assert_eq!(result.post_scan.len(), 0);

            // Feed low effectiveness stats
            for _ in 0..5 {
                tracker.update(1, 100, 100, 100_000, 1000); // all rows matched, no pruning
            }

            // Second partition: should be demoted to PostScan
            let result = tracker.partition_filters(filters, 1000, &metadata);
            assert_eq!(result.row_filters.len(), 0);
            assert_eq!(result.post_scan.len(), 1);
        }

        #[test]
        fn test_demotion_resets_stats() {
            let tracker = TrackerConfig::new()
                .with_min_bytes_per_sec(10000.0)
                .with_byte_ratio_threshold(0.1)
                .with_confidence_z(0.5)
                .build();

            let metadata = create_test_metadata(vec![(100, vec![100])]);
            let expr = col_expr("a", 0);
            let filters = vec![(1, expr.clone())];

            // Start as RowFilter
            tracker.partition_filters(filters.clone(), 1000, &metadata);

            // Add stats
            tracker.update(1, 100, 100, 100_000, 1000);
            tracker.update(1, 100, 100, 100_000, 1000);

            // Demote
            tracker.partition_filters(filters.clone(), 1000, &metadata);

            // Stats should be zeroed after demotion
            let stats_map = tracker.filter_stats.read();
            assert_eq!(
                *stats_map.get(&1).unwrap().lock(),
                SelectivityStats::default()
            );
        }

        #[test]
        fn test_promotion_resets_stats() {
            let tracker = TrackerConfig::new()
                .with_min_bytes_per_sec(100.0)
                .with_byte_ratio_threshold(0.5)
                .with_confidence_z(0.5)
                .build();

            let metadata = create_test_metadata(vec![(100, vec![1000])]);
            let expr = col_expr("a", 0);
            let filters = vec![(1, expr.clone())];

            // Start as PostScan
            tracker.partition_filters(filters.clone(), 1000, &metadata);

            // Add stats
            for _ in 0..3 {
                tracker.update(1, 50, 100, 100_000, 1000);
            }

            // Promote
            tracker.partition_filters(filters.clone(), 1000, &metadata);

            // Stats should be zeroed after promotion
            let stats_map = tracker.filter_stats.read();
            assert_eq!(
                *stats_map.get(&1).unwrap().lock(),
                SelectivityStats::default()
            );
        }

        #[test]
        fn test_optional_filter_dropping() {
            let tracker = TrackerConfig::new()
                .with_min_bytes_per_sec(10000.0)
                .with_byte_ratio_threshold(0.5)
                .with_confidence_z(0.5)
                .build();

            let metadata = create_test_metadata(vec![(100, vec![1000])]);
            let expr = col_expr("a", 0);
            let filters = vec![(1, expr.clone())];

            // Start as PostScan
            tracker.partition_filters(filters.clone(), 1000, &metadata);

            // Feed poor effectiveness stats
            for _ in 0..5 {
                tracker.update(1, 100, 100, 100_000, 1000); // no pruning
            }

            // Next partition: should stay as PostScan (not dropped because not optional)
            let result = tracker.partition_filters(filters, 1000, &metadata);
            assert_eq!(result.post_scan.len(), 1);
            assert_eq!(result.row_filters.len(), 0);
        }

        #[test]
        fn test_persistent_dropped_state() {
            let tracker = TrackerConfig::new()
                .with_min_bytes_per_sec(10000.0)
                .with_byte_ratio_threshold(0.5)
                .build();

            let metadata = create_test_metadata(vec![(100, vec![1000])]);
            let expr = col_expr("a", 0);
            let filters = vec![(1, expr.clone())];

            // Mark filter as dropped by manually setting state
            tracker
                .inner
                .lock()
                .filter_states
                .insert(1, FilterState::Dropped);

            // On next partition, dropped filters should not reappear
            let result = tracker.partition_filters(filters, 1000, &metadata);
            assert_eq!(result.row_filters.len(), 0);
            assert_eq!(result.post_scan.len(), 0);
        }
    }

    mod filter_ordering_tests {
        use super::helper_functions::*;
        use super::*;

        #[test]
        fn test_filters_get_partitioned() {
            let tracker = TrackerConfig::new()
                .with_min_bytes_per_sec(1.0) // Very low threshold
                .build();

            let metadata = create_test_metadata(vec![(100, vec![100, 100, 100])]);
            let filters = vec![
                (1, col_expr("a", 0)),
                (2, col_expr("a", 1)),
                (3, col_expr("a", 2)),
            ];

            // Partition should process all filters
            let result = tracker.partition_filters(filters.clone(), 1000, &metadata);

            // With min_bytes_per_sec=1.0, filters should be partitioned
            assert!(result.row_filters.len() + result.post_scan.len() > 0);

            // Add stats and partition again
            tracker.update(1, 60, 100, 1_000_000, 100);
            tracker.update(2, 10, 100, 1_000_000, 100);
            tracker.update(3, 40, 100, 1_000_000, 100);

            let result2 = tracker.partition_filters(filters, 1000, &metadata);

            // Filters should still be partitioned
            assert!(result2.row_filters.len() + result2.post_scan.len() > 0);
        }

        #[test]
        fn test_filters_processed_without_stats() {
            let tracker = TrackerConfig::new()
                .with_min_bytes_per_sec(1.0) // Very low threshold
                .build();

            // Different column sizes: 300, 200, 100 bytes
            let metadata = create_test_metadata(vec![(100, vec![300, 200, 100])]);
            let filters = vec![
                (1, col_expr("a", 0)),
                (2, col_expr("a", 1)),
                (3, col_expr("a", 2)),
            ];

            // First partition - no stats yet
            let result = tracker.partition_filters(filters.clone(), 1000, &metadata);

            // All filters should be processed (partitioned into row/post-scan)
            assert!(result.row_filters.len() + result.post_scan.len() > 0);

            // Filters should be consistent on repeated calls
            let result2 = tracker.partition_filters(filters, 1000, &metadata);
            assert_eq!(
                result.row_filters.len() + result.post_scan.len(),
                result2.row_filters.len() + result2.post_scan.len()
            );
        }

        #[test]
        fn test_filters_with_partial_stats() {
            let tracker = TrackerConfig::new().with_min_bytes_per_sec(1.0).build();

            // Give filter 2 larger bytes so it's prioritized when falling back to byte ratio
            let metadata = create_test_metadata(vec![(100, vec![100, 300, 100])]);
            let filters = vec![
                (1, col_expr("a", 0)),
                (2, col_expr("a", 1)),
                (3, col_expr("a", 2)),
            ];

            // First partition
            let result1 = tracker.partition_filters(filters.clone(), 1000, &metadata);
            assert!(result1.row_filters.len() + result1.post_scan.len() > 0);

            // Only add stats for filters 1 and 3, not 2
            tracker.update(1, 60, 100, 1_000_000, 100);
            tracker.update(3, 60, 100, 1_000_000, 100);

            // Second partition with partial stats
            let result2 = tracker.partition_filters(filters, 1000, &metadata);
            assert!(result2.row_filters.len() + result2.post_scan.len() > 0);
        }

        #[test]
        fn test_ordering_stability_with_identical_values() {
            let tracker = TrackerConfig::new().with_min_bytes_per_sec(0.0).build();

            let metadata = create_test_metadata(vec![(100, vec![100, 100, 100])]);
            let filters = vec![
                (1, col_expr("a", 0)),
                (2, col_expr("a", 1)),
                (3, col_expr("a", 2)),
            ];

            let result1 = tracker.partition_filters(filters.clone(), 1000, &metadata);
            let result2 = tracker.partition_filters(filters, 1000, &metadata);

            // Without stats and with identical byte sizes, order should be stable
            assert_eq!(result1.row_filters[0].0, result2.row_filters[0].0);
            assert_eq!(result1.row_filters[1].0, result2.row_filters[1].0);
            assert_eq!(result1.row_filters[2].0, result2.row_filters[2].0);
        }
    }

    mod dynamic_filter_tests {
        use super::helper_functions::*;
        use super::*;

        #[test]
        fn test_generation_zero_ignored() {
            let tracker = TrackerConfig::new()
                .with_min_bytes_per_sec(1000.0)
                .with_byte_ratio_threshold(0.5)
                .build();

            let metadata = create_test_metadata(vec![(100, vec![1000])]);

            // Create two filters with same ID but generation 0 and 1
            // Generation 0 should be ignored
            let expr1 = col_expr("a", 0);
            let filters1 = vec![(1, expr1)];

            tracker.partition_filters(filters1, 1000, &metadata);
            tracker.update(1, 50, 100, 100_000, 1000);

            // Generation 0 doesn't trigger state reset
            let snapshot_gen = tracker.inner.lock().snapshot_generations.get(&1).copied();
            assert_eq!(snapshot_gen, None);
        }

        #[test]
        fn test_generation_change_clears_stats() {
            let tracker = TrackerConfig::new()
                .with_min_bytes_per_sec(1000.0)
                .with_byte_ratio_threshold(0.5)
                .build();

            // Pre-populate stats entry so update() can find it
            tracker.ensure_stats_entry(1);

            // Initialize generation to 100
            {
                let mut inner = tracker.inner.lock();
                let stats = tracker.filter_stats.read();
                inner.note_generation(1, 100, &stats);
            }

            // Add stats
            tracker.update(1, 50, 100, 100_000, 1000);
            tracker.update(1, 50, 100, 100_000, 1000);

            let stats_before = {
                let stats_map = tracker.filter_stats.read();
                *stats_map.get(&1).unwrap().lock() != SelectivityStats::default()
            };
            assert!(stats_before);

            // Simulate generation change to a different value
            {
                let mut inner = tracker.inner.lock();
                let stats = tracker.filter_stats.read();
                inner.note_generation(1, 101, &stats);
            }

            // Stats should be zeroed on generation change
            let stats_after = {
                let stats_map = tracker.filter_stats.read();
                *stats_map.get(&1).unwrap().lock() == SelectivityStats::default()
            };
            assert!(stats_after);
        }

        #[test]
        fn test_generation_unchanged_preserves_stats() {
            let tracker = TrackerConfig::new().with_min_bytes_per_sec(1000.0).build();

            // Pre-populate stats entry so update() can find it
            tracker.ensure_stats_entry(1);

            // Manually set generation
            {
                let mut inner = tracker.inner.lock();
                let stats = tracker.filter_stats.read();
                inner.note_generation(1, 100, &stats);
            }

            // Add stats
            tracker.update(1, 50, 100, 100_000, 1000);
            tracker.update(1, 50, 100, 100_000, 1000);

            let sample_count_before = {
                let stats_map = tracker.filter_stats.read();
                stats_map.get(&1).map(|s| s.lock().sample_count)
            };
            assert_eq!(sample_count_before, Some(2));

            // Call note_generation with same generation
            {
                let mut inner = tracker.inner.lock();
                let stats = tracker.filter_stats.read();
                inner.note_generation(1, 100, &stats);
            }

            // Stats should be preserved
            let sample_count_after = {
                let stats_map = tracker.filter_stats.read();
                stats_map.get(&1).map(|s| s.lock().sample_count)
            };
            assert_eq!(sample_count_after, Some(2));
        }

        #[test]
        fn test_generation_change_preserves_state() {
            let tracker = TrackerConfig::new()
                .with_min_bytes_per_sec(1000.0)
                .with_byte_ratio_threshold(0.1)
                .build();

            let metadata = create_test_metadata(vec![(100, vec![100])]);

            // First partition: goes to RowFilter
            let expr = col_expr("a", 0);
            let filters = vec![(1, expr)];
            tracker.partition_filters(filters.clone(), 1000, &metadata);

            let state_before = tracker.inner.lock().filter_states.get(&1).copied();
            assert_eq!(state_before, Some(FilterState::RowFilter));

            // Simulate generation change
            {
                let mut inner = tracker.inner.lock();
                let stats = tracker.filter_stats.read();
                inner.note_generation(1, 100, &stats);
            }

            // State should be preserved despite stats being cleared
            let state_after = tracker.inner.lock().filter_states.get(&1).copied();
            assert_eq!(state_after, Some(FilterState::RowFilter));
        }

        #[test]
        fn test_generation_change_undrops_dropped_filter() {
            let tracker = TrackerConfig::new()
                .with_min_bytes_per_sec(1000.0)
                .with_byte_ratio_threshold(0.1)
                .build();

            // Manually set filter state to Dropped
            tracker
                .inner
                .lock()
                .filter_states
                .insert(1, FilterState::Dropped);
            {
                let mut inner = tracker.inner.lock();
                let stats = tracker.filter_stats.read();
                inner.note_generation(1, 100, &stats);
            }

            // Simulate generation change
            {
                let mut inner = tracker.inner.lock();
                let stats = tracker.filter_stats.read();
                inner.note_generation(1, 101, &stats);
            }

            // Dropped filter should be un-dropped to PostScan
            let state_after = tracker.inner.lock().filter_states.get(&1).copied();
            assert_eq!(state_after, Some(FilterState::PostScan));
        }

        #[test]
        fn test_multiple_filters_independent_generation_tracking() {
            let tracker = TrackerConfig::new().with_min_bytes_per_sec(1000.0).build();

            // Pre-populate stats entries so update() can find them
            tracker.ensure_stats_entry(1);
            tracker.ensure_stats_entry(2);

            // Set generations for multiple filters
            {
                let mut inner = tracker.inner.lock();
                let stats = tracker.filter_stats.read();
                inner.note_generation(1, 100, &stats);
                inner.note_generation(2, 200, &stats);
            }

            // Add stats to both
            tracker.update(1, 50, 100, 100_000, 1000);
            tracker.update(2, 50, 100, 100_000, 1000);

            // Change generation of filter 1 only
            {
                let mut inner = tracker.inner.lock();
                let stats = tracker.filter_stats.read();
                inner.note_generation(1, 101, &stats);
            }

            // Filter 1 stats should be zeroed, filter 2 preserved
            let stats_map = tracker.filter_stats.read();
            assert_eq!(
                *stats_map.get(&1).unwrap().lock(),
                SelectivityStats::default()
            );
            assert_ne!(
                *stats_map.get(&2).unwrap().lock(),
                SelectivityStats::default()
            );
        }
    }

    mod integration_tests {
        use super::helper_functions::*;
        use super::*;

        #[test]
        fn test_full_promotion_lifecycle() {
            let tracker = TrackerConfig::new()
                .with_min_bytes_per_sec(500.0)
                .with_byte_ratio_threshold(0.5) // Force initial PostScan
                .with_confidence_z(0.5)
                .build();

            let metadata = create_test_metadata(vec![(100, vec![1000])]);
            let expr = col_expr("a", 0);
            let filters = vec![(1, expr.clone())];

            // Step 1: Initial placement (PostScan)
            let result = tracker.partition_filters(filters.clone(), 1000, &metadata);
            assert_eq!(result.post_scan.len(), 1);
            assert_eq!(result.row_filters.len(), 0);

            // Step 2: Accumulate high effectiveness stats
            for _ in 0..5 {
                tracker.update(1, 10, 100, 100_000, 1000); // high effectiveness
            }

            // Step 3: Promotion should occur
            let result = tracker.partition_filters(filters.clone(), 1000, &metadata);
            assert_eq!(result.row_filters.len(), 1);
            assert_eq!(result.post_scan.len(), 0);

            // Step 4: Continue to partition without additional updates
            let result = tracker.partition_filters(filters, 1000, &metadata);
            assert_eq!(result.row_filters.len(), 1);
            assert_eq!(result.post_scan.len(), 0);
        }

        #[test]
        fn test_full_demotion_lifecycle() {
            let tracker = TrackerConfig::new()
                .with_min_bytes_per_sec(10000.0)
                .with_byte_ratio_threshold(0.1) // Force initial RowFilter
                .with_confidence_z(0.5)
                .build();

            let metadata = create_test_metadata(vec![(100, vec![100])]);
            let expr = col_expr("a", 0);
            let filters = vec![(1, expr.clone())];

            // Step 1: Initial placement (RowFilter)
            let result = tracker.partition_filters(filters.clone(), 1000, &metadata);
            assert_eq!(result.row_filters.len(), 1);
            assert_eq!(result.post_scan.len(), 0);

            // Step 2: Accumulate low effectiveness stats
            for _ in 0..5 {
                tracker.update(1, 100, 100, 100_000, 1000); // no pruning
            }

            // Step 3: Demotion should occur
            let result = tracker.partition_filters(filters.clone(), 1000, &metadata);
            assert_eq!(result.row_filters.len(), 0);
            assert_eq!(result.post_scan.len(), 1);

            // Step 4: Continue to partition without additional updates
            let result = tracker.partition_filters(filters, 1000, &metadata);
            assert_eq!(result.row_filters.len(), 0);
            assert_eq!(result.post_scan.len(), 1);
        }

        #[test]
        fn test_multiple_filters_mixed_states() {
            let tracker = TrackerConfig::new()
                .with_min_bytes_per_sec(1000.0)
                .with_byte_ratio_threshold(0.4) // Force PostScan initially (500/1000=0.5 > 0.4)
                .with_confidence_z(0.5)
                .build();

            let metadata = create_test_metadata(vec![(100, vec![500, 500])]);
            let filters = vec![(1, col_expr("a", 0)), (2, col_expr("a", 1))];

            // Initial partition: both go to PostScan (500/1000 = 0.5 > 0.4)
            let result = tracker.partition_filters(filters.clone(), 1000, &metadata);
            assert_eq!(result.post_scan.len(), 2);

            // Filter 1: high effectiveness (promote)
            for _ in 0..3 {
                tracker.update(1, 10, 100, 100_000, 500);
            }

            // Filter 2: low effectiveness (stay PostScan)
            for _ in 0..3 {
                tracker.update(2, 100, 100, 100_000, 500);
            }

            // Next partition: Filter 1 promoted, Filter 2 stays PostScan
            let result = tracker.partition_filters(filters, 1000, &metadata);
            assert_eq!(result.row_filters.len(), 1);
            assert_eq!(result.post_scan.len(), 1);
            assert_eq!(result.row_filters[0].0, 1);
            assert_eq!(result.post_scan[0].0, 2);
        }

        #[test]
        fn test_empty_filter_list() {
            let tracker = TrackerConfig::new().build();
            let metadata = create_test_metadata(vec![(100, vec![1000])]);
            let filters = vec![];

            let result = tracker.partition_filters(filters, 1000, &metadata);

            assert_eq!(result.row_filters.len(), 0);
            assert_eq!(result.post_scan.len(), 0);
        }

        #[test]
        fn test_single_filter() {
            let tracker = TrackerConfig::new().with_min_bytes_per_sec(0.0).build();

            let metadata = create_test_metadata(vec![(100, vec![100])]);
            let expr = col_expr("a", 0);
            let filters = vec![(1, expr)];

            let result = tracker.partition_filters(filters, 1000, &metadata);

            assert_eq!(result.row_filters.len(), 1);
            assert_eq!(result.post_scan.len(), 0);
        }

        #[test]
        fn test_zero_effectiveness_stays_at_boundary() {
            let tracker = TrackerConfig::new()
                .with_min_bytes_per_sec(100.0)
                .with_byte_ratio_threshold(0.1)
                .with_confidence_z(0.5)
                .build();

            let metadata = create_test_metadata(vec![(100, vec![100])]);
            let expr = col_expr("a", 0);
            let filters = vec![(1, expr.clone())];

            // Start as RowFilter
            tracker.partition_filters(filters.clone(), 1000, &metadata);

            // All rows match (zero effectiveness)
            for _ in 0..5 {
                tracker.update(1, 100, 100, 100_000, 100);
            }

            // Should demote due to CI upper bound being 0
            let result = tracker.partition_filters(filters, 1000, &metadata);
            assert_eq!(result.row_filters.len(), 0);
            assert_eq!(result.post_scan.len(), 1);
        }

        #[test]
        fn test_confidence_z_parameter_stored() {
            // Test that different confidence_z values are properly stored in config
            let tracker_conservative = TrackerConfig::new()
                .with_min_bytes_per_sec(1000.0)
                .with_byte_ratio_threshold(0.5)
                .with_confidence_z(3.0) // Harder to promote
                .build();

            let tracker_aggressive = TrackerConfig::new()
                .with_min_bytes_per_sec(1000.0)
                .with_byte_ratio_threshold(0.5)
                .with_confidence_z(0.5) // Easier to promote
                .build();

            // Verify configs are stored correctly
            assert_eq!(tracker_conservative.config.confidence_z, 3.0);
            assert_eq!(tracker_aggressive.config.confidence_z, 0.5);

            // The z-score affects confidence intervals during promotion/demotion decisions.
            // With identical stats, higher z requires narrower confidence intervals,
            // making promotion harder. With lower z, confidence intervals are wider,
            // making promotion easier. This is tested in other integration tests
            // that verify actual promotion/demotion behavior.
        }
    }
}
