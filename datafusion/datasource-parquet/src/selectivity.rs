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

use arrow::array::BooleanArray;
use arrow::datatypes::SchemaRef;
use log::debug;
use parking_lot::{Mutex, RwLock};
use parquet::file::metadata::ParquetMetaData;
use parquet::schema::types::SchemaDescriptor;
use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};

use datafusion_physical_expr::utils::collect_columns;
use datafusion_physical_expr_common::physical_expr::{
    OptionalFilterPhysicalExpr, PhysicalExpr, snapshot_generation,
};

/// Window size for the per-batch scatter analysis fed to
/// [`count_skippable_bytes`]. Approximates a parquet data page so that
/// "windows with zero survivors" tracks "pages a row-level decoder
/// could skip". Hardcoded for now; making this configurable (or
/// deriving it from per-row-group page metadata) is a natural follow-up.
pub(crate) const SKIP_WINDOW_ROWS: usize = 8192;

/// Compute the bytes that late-materialization can plausibly skip for a
/// batch given the predicate output `bool_arr` and the total non-filter
/// projection bytes for that batch.
///
/// Splits `bool_arr` into [`SKIP_WINDOW_ROWS`]-sized windows; each window
/// with zero survivors represents a page-sized chunk whose
/// other-projection columns the row-level decoder can skip outright.
/// Returns `total_other_bytes × (empty_windows / total_windows)` —
/// scatter-discounted skippable bytes.
///
/// Interpretation depends on which side calls this:
///
/// - **Post-scan path**: a *prediction* of bytes-saved-per-sec the
///   row-level path would achieve. The bool_arr we see is over the wide
///   batch in the same row order the decoder would emit, so for single-
///   predicate filters the prediction is faithful (modulo `W` matching
///   the actual parquet page size).
///
/// - **Row-level path**: a conservative *measurement* of what the
///   decoder actually skipped — within-window RowSelection narrowing is
///   an additional uncounted bonus. So at row-level this is a *lower
///   bound* of real savings, which is the safe direction for the
///   demote-or-not decision.
pub(crate) fn count_skippable_bytes(
    bool_arr: &BooleanArray,
    total_other_bytes: u64,
) -> u64 {
    let n = bool_arr.len();
    if n == 0 || total_other_bytes == 0 {
        return 0;
    }
    // Short-circuit on the two extremes: avoids a redundant per-window
    // SIMD scan over the same buffer when the answer is already
    // determined by the batch-level total. The whole helper otherwise
    // costs ~2× per-batch `true_count` for nothing.
    let total_matched = bool_arr.true_count();
    if total_matched == 0 {
        // Every window empty: full skippable.
        return total_other_bytes;
    }
    if total_matched == n {
        // No window empty: nothing skippable.
        return 0;
    }
    let total_windows = n.div_ceil(SKIP_WINDOW_ROWS);
    if total_windows == 1 {
        // One-window batch with mixed matches → not skippable. Avoids
        // a wasted slice+`true_count`.
        return 0;
    }
    let mut empty_windows: u64 = 0;
    for i in 0..total_windows {
        let start = i * SKIP_WINDOW_ROWS;
        let len = SKIP_WINDOW_ROWS.min(n - start);
        if bool_arr.slice(start, len).true_count() == 0 {
            empty_windows += 1;
        }
    }
    ((total_other_bytes as f64 * empty_windows as f64) / total_windows as f64) as u64
}

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
///
/// This type is `pub` to support the [selectivity tracker benchmark
/// harness](../../benches/selectivity_tracker.rs); treat the layout as
/// unstable from outside the crate.
#[derive(Debug, Clone, Default)]
#[doc(hidden)]
pub struct PartitionedFilters {
    /// Filters promoted past collection — individual chained ArrowPredicates
    pub row_filters: Vec<(FilterId, Arc<dyn PhysicalExpr>)>,
    /// Filters demoted to post-scan (fast path only)
    pub post_scan: Vec<(FilterId, Arc<dyn PhysicalExpr>)>,
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
    /// Whether the underlying expression is wrapped in
    /// `OptionalFilterPhysicalExpr`. Cached here (rather than re-checked
    /// via [`is_optional_filter`] on every batch) so the per-batch hot
    /// path in [`SelectivityTracker::update`] can skip the
    /// SKIP_FLAG/CI-bound work entirely for non-optional filters with a
    /// single field load on the already-held stats lock — no extra
    /// HashMap or `RwLock::read()` per batch.
    is_optional: bool,
}

impl SelectivityStats {
    /// Returns the cumulative effectiveness as an opaque ordering score
    /// (higher = run first).
    ///
    /// Computed from `eff_mean` so it matches the Welford-tracked metric
    /// fed to CI bounds: per-batch scatter-aware bytes-saved-per-second.
    /// Callers should not assume the unit.
    fn effectiveness(&self) -> Option<f64> {
        if self.sample_count == 0 {
            return None;
        }
        Some(self.eff_mean)
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
    ///
    /// `skippable_bytes` is the caller's already-computed estimate of
    /// non-filter projection bytes that late-materialization would
    /// actually save for this batch — see [`count_skippable_bytes`] for
    /// the windowed scatter calculation. The Welford accumulator tracks
    /// `skippable_bytes × 1e9 / eval_nanos` (= scatter-aware
    /// bytes-saved-per-second), which is what the promote/demote
    /// gates compare against `min_bytes_per_sec`.
    fn update(
        &mut self,
        matched: u64,
        total: u64,
        eval_nanos: u64,
        skippable_bytes: u64,
    ) {
        self.rows_matched += matched;
        self.rows_total += total;
        self.eval_nanos += eval_nanos;
        self.bytes_seen += skippable_bytes;

        if total > 0 && eval_nanos > 0 {
            let batch_eff = skippable_bytes as f64 * 1e9 / eval_nanos as f64;

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
#[doc(hidden)]
pub struct TrackerConfig {
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
    /// Initial-placement prior threshold: if per-conjunct row-group
    /// statistics pruning prunes ≥ this fraction of the file's row
    /// groups, place the filter at row-level on first encounter. Set
    /// to >1.0 to disable the prior. Default 0.5.
    pub prior_promote_threshold: f64,
    /// Initial-placement prior threshold: if per-conjunct row-group
    /// statistics pruning prunes ≤ this fraction of the file's row
    /// groups, place the filter at post-scan on first encounter. Set
    /// to <0.0 to disable the prior. Default 0.05.
    pub prior_demote_threshold: f64,
    /// Per-fetch latency baseline in milliseconds — at this average
    /// per-fetch RTT the tracker uses the unmodified `confidence_z`.
    /// Above this, `confidence_z` is shrunk proportionally so the
    /// tracker becomes more aggressive about state changes when
    /// per-request cost is high. 0.0 disables. Default 5.0.
    pub latency_z_baseline_ms: f64,
    /// Maximum scale factor for the latency-aware z shrink. Default 8.0.
    pub latency_z_max_scale: f64,
}

impl TrackerConfig {
    pub fn new() -> Self {
        Self {
            min_bytes_per_sec: f64::INFINITY,
            byte_ratio_threshold: 0.20,
            confidence_z: 2.0,
            prior_promote_threshold: 0.5,
            prior_demote_threshold: 0.05,
            latency_z_baseline_ms: 5.0,
            latency_z_max_scale: 8.0,
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

    pub fn with_prior_promote_threshold(mut self, v: f64) -> Self {
        self.prior_promote_threshold = v;
        self
    }

    pub fn with_prior_demote_threshold(mut self, v: f64) -> Self {
        self.prior_demote_threshold = v;
        self
    }

    pub fn with_latency_z_baseline_ms(mut self, v: f64) -> Self {
        self.latency_z_baseline_ms = v;
        self
    }

    pub fn with_latency_z_max_scale(mut self, v: f64) -> Self {
        self.latency_z_max_scale = v;
        self
    }

    pub fn build(self) -> SelectivityTracker {
        SelectivityTracker {
            config: self,
            filter_stats: RwLock::new(HashMap::new()),
            skip_flags: RwLock::new(HashMap::new()),
            inner: Mutex::new(SelectivityTrackerInner::new()),
            total_fetch_ns: AtomicU64::new(0),
            total_fetches: AtomicU64::new(0),
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
    /// Cumulative wall time spent inside `AsyncFileReader::get_byte_ranges`
    /// across all openers using this tracker.
    total_fetch_ns: AtomicU64,
    /// Number of byte-range fetches recorded.
    total_fetches: AtomicU64,
    filter_stats: RwLock<HashMap<FilterId, Mutex<SelectivityStats>>>,
    /// Per-filter "skip" flags — when set, the corresponding filter is
    /// treated as a no-op by both the row-filter
    /// (`DatafusionArrowPredicate::evaluate`) and the post-scan path
    /// (`apply_post_scan_filters_with_stats`). This is the mid-stream
    /// equivalent of dropping an optional filter: once the per-batch
    /// `update()` path proves an `OptionalFilterPhysicalExpr` is
    /// CPU-dominated and ineffective, it flips the flag and subsequent
    /// batches stop paying the evaluation cost. The decoder still decodes
    /// the filter columns (we cannot rebuild it mid-scan), so I/O is not
    /// reclaimed; only the predicate evaluation is skipped.
    ///
    /// Only ever set for filters whose `is_optional` flag (cached on the
    /// per-filter [`SelectivityStats`]) is `true` — mandatory filters
    /// must always execute or queries return wrong rows.
    skip_flags: RwLock<HashMap<FilterId, Arc<AtomicBool>>>,
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

    /// Record one batch of `get_byte_ranges` activity (latency-aware z input).
    pub fn record_fetch(&self, ranges: usize, elapsed_ns: u64) {
        if ranges == 0 || elapsed_ns == 0 {
            return;
        }
        self.total_fetch_ns.fetch_add(elapsed_ns, Ordering::Relaxed);
        self.total_fetches
            .fetch_add(ranges as u64, Ordering::Relaxed);
    }

    fn avg_fetch_ms(&self) -> f64 {
        let fetches = self.total_fetches.load(Ordering::Relaxed);
        if fetches == 0 {
            return 0.0;
        }
        let ns = self.total_fetch_ns.load(Ordering::Relaxed) as f64;
        ns / fetches as f64 / 1_000_000.0
    }

    fn effective_z(&self) -> f64 {
        let z = self.config.confidence_z;
        if self.config.latency_z_baseline_ms <= 0.0 {
            return z;
        }
        let avg = self.avg_fetch_ms();
        if avg <= self.config.latency_z_baseline_ms {
            return z;
        }
        let factor = (avg / self.config.latency_z_baseline_ms)
            .clamp(1.0, self.config.latency_z_max_scale);
        z / factor
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
    ///
    /// **Mid-stream drop:** after every `SKIP_FLAG_CHECK_INTERVAL`'th batch
    /// we evaluate the CI upper bound; if it falls below
    /// `min_bytes_per_sec` and the filter is wrapped in
    /// `OptionalFilterPhysicalExpr`, we set the per-filter skip flag.
    /// Subsequent calls to `DatafusionArrowPredicate::evaluate` (row-level)
    /// and `apply_post_scan_filters_with_stats` (post-scan) observe the
    /// flag and short-circuit their work for that filter. Mandatory
    /// filters are never flagged because doing so would change the result
    /// set.
    #[doc(hidden)]
    pub fn update(
        &self,
        id: FilterId,
        matched: u64,
        total: u64,
        eval_nanos: u64,
        batch_bytes: u64,
    ) {
        let stats_map = self.filter_stats.read();
        let Some(entry) = stats_map.get(&id) else {
            return;
        };
        let mut stats = entry.lock();
        stats.update(matched, total, eval_nanos, batch_bytes);

        // Fast path for non-optional filters: nothing else to do. The
        // SKIP_FLAG mid-stream drop only applies to
        // `OptionalFilterPhysicalExpr`-wrapped filters (hash-join /
        // TopK dynamic), and `is_optional` is cached inline on
        // `SelectivityStats` at filter registration so this is a single
        // field load on the already-held lock.
        if !stats.is_optional {
            return;
        }

        // Optional filter: do the SKIP_FLAG check every batch — there's
        // no SKIP_FLAG_CHECK_INTERVAL gate here on purpose. We want
        // join/TopK skip flags to fire as soon as stats prove the
        // filter's selectivity has collapsed, even mid-row-group. The
        // CI-bound calc is cheap arithmetic on already-locked stats.
        if !self.config.min_bytes_per_sec.is_finite() {
            return;
        }
        let z = self.effective_z();
        let Some(ub) = stats.confidence_upper_bound(z) else {
            return;
        };
        if ub >= self.config.min_bytes_per_sec {
            return;
        }
        drop(stats);
        drop(stats_map);

        if let Some(flag) = self.skip_flags.read().get(&id)
            && !flag.swap(true, Ordering::Release)
        {
            debug!(
                "FilterId {id}: mid-stream skip — CI upper bound {ub} < {} bytes/sec",
                self.config.min_bytes_per_sec
            );
        }
    }

    /// Returns the shared skip flag for `id`, creating one if absent.
    ///
    /// Cloned into [`crate::row_filter::DatafusionArrowPredicate`] so the
    /// row-filter path can short-circuit when the per-batch update path
    /// decides the filter has stopped pulling its weight. The post-scan
    /// path uses [`Self::is_filter_skipped`] instead — it does not need a
    /// long-lived handle.
    pub(crate) fn skip_flag(&self, id: FilterId) -> Arc<AtomicBool> {
        if let Some(existing) = self.skip_flags.read().get(&id) {
            return Arc::clone(existing);
        }
        let mut write = self.skip_flags.write();
        Arc::clone(
            write
                .entry(id)
                .or_insert_with(|| Arc::new(AtomicBool::new(false))),
        )
    }

    /// Returns `true` when `id` has been mid-stream-dropped by the tracker.
    ///
    /// Cheap: a single `RwLock::read` plus an atomic load. Called from the
    /// post-scan filter loop in `apply_post_scan_filters_with_stats`.
    pub(crate) fn is_filter_skipped(&self, id: FilterId) -> bool {
        self.skip_flags
            .read()
            .get(&id)
            .is_some_and(|f| f.load(Ordering::Acquire))
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
    #[doc(hidden)]
    #[expect(clippy::too_many_arguments)]
    pub fn partition_filters(
        &self,
        filters: Vec<(FilterId, Arc<dyn PhysicalExpr>)>,
        projection_columns: &std::collections::HashSet<usize>,
        projection_scan_size: usize,
        metadata: &ParquetMetaData,
        arrow_schema: &SchemaRef,
        parquet_schema: &SchemaDescriptor,
        page_pruning_rates: &HashMap<FilterId, f64>,
    ) -> PartitionedFilters {
        // Phase 1: inner.lock() + filter_stats.read() → all decision logic
        let z_eff = self.effective_z();
        let mut guard = self.inner.lock();
        let stats_map = self.filter_stats.read();
        let result = guard.partition_filters(
            filters,
            projection_columns,
            projection_scan_size,
            metadata,
            arrow_schema,
            parquet_schema,
            &self.config,
            z_eff,
            page_pruning_rates,
            &stats_map,
        );
        drop(stats_map);
        drop(guard);

        // Phase 2: if new filters were seen, briefly acquire write locks
        // to insert per-filter `Mutex<SelectivityStats>` (with
        // `is_optional` cached inline so the per-batch `update()` hot
        // path can fast-return for mandatory filters) and an
        // `AtomicBool` skip-flag (only consulted for optional filters).
        if !result.new_optional_flags.is_empty() {
            let mut stats_write = self.filter_stats.write();
            let mut skip_write = self.skip_flags.write();
            for (id, is_optional) in result.new_optional_flags {
                stats_write.entry(id).or_insert_with(|| {
                    Mutex::new(SelectivityStats {
                        is_optional,
                        ..Default::default()
                    })
                });
                skip_write
                    .entry(id)
                    .or_insert_with(|| Arc::new(AtomicBool::new(false)));
            }
        }

        result.partitioned
    }

    /// Test-only convenience that derives `arrow_schema` / `parquet_schema`
    /// from the parquet metadata and forwards to the public
    /// [`Self::partition_filters`]. Lets test code keep its existing call
    /// sites without threading two more arguments through every test.
    #[doc(hidden)]
    pub fn partition_filters_for_test(
        &self,
        filters: Vec<(FilterId, Arc<dyn PhysicalExpr>)>,
        projection_columns: &std::collections::HashSet<usize>,
        projection_scan_size: usize,
        metadata: &ParquetMetaData,
    ) -> PartitionedFilters {
        let parquet_schema = metadata.file_metadata().schema_descr_ptr();
        let arrow_schema: SchemaRef = match parquet::arrow::parquet_to_arrow_schema(
            parquet_schema.as_ref(),
            None,
        ) {
            Ok(s) => Arc::new(s),
            Err(_) => Arc::new(arrow::datatypes::Schema::empty()),
        };
        self.partition_filters(
            filters,
            projection_columns,
            projection_scan_size,
            metadata,
            &arrow_schema,
            parquet_schema.as_ref(),
            &HashMap::new(),
        )
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
/// Carries both the partitioned filters and the `(FilterId, is_optional)`
/// entries seen for the first time, so the outer
/// [`SelectivityTracker::partition_filters`] can insert per-filter
/// `Mutex<SelectivityStats>` entries (with `is_optional` cached inline)
/// in a brief Phase 2 write lock.
struct PartitionResult {
    partitioned: PartitionedFilters,
    /// `(FilterId, is_optional)` entries observed for the first time in
    /// this `partition_filters` call.
    new_optional_flags: Vec<(FilterId, bool)>,
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
        if expr.downcast_ref::<OptionalFilterPhysicalExpr>().is_none() {
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
    #[expect(clippy::too_many_arguments)]
    fn partition_filters(
        &mut self,
        filters: Vec<(FilterId, Arc<dyn PhysicalExpr>)>,
        projection_columns: &std::collections::HashSet<usize>,
        projection_scan_size: usize,
        metadata: &ParquetMetaData,
        arrow_schema: &SchemaRef,
        parquet_schema: &SchemaDescriptor,
        config: &TrackerConfig,
        z_eff: f64,
        page_pruning_rates: &HashMap<FilterId, f64>,
        stats_map: &HashMap<FilterId, Mutex<SelectivityStats>>,
    ) -> PartitionResult {
        let mut new_optional_flags: Vec<(FilterId, bool)> = Vec::new();

        // If min_bytes_per_sec is INFINITY -> all filters are post-scan.
        if config.min_bytes_per_sec.is_infinite() {
            debug!(
                "Filter promotion disabled via min_bytes_per_sec=INFINITY; all {} filters post-scan",
                filters.len()
            );
            // Register all filter IDs so update() can find them
            for (id, expr) in &filters {
                if !stats_map.contains_key(id) {
                    new_optional_flags.push((*id, is_optional_filter(expr)));
                }
            }
            return PartitionResult {
                partitioned: PartitionedFilters {
                    row_filters: Vec::new(),
                    post_scan: filters,
                },
                new_optional_flags,
            };
        }
        // If min_bytes_per_sec is 0 -> all filters are promoted.
        if config.min_bytes_per_sec == 0.0 {
            debug!(
                "All filters promoted via min_bytes_per_sec=0; all {} filters row-level",
                filters.len()
            );
            // Register all filter IDs so update() can find them
            for (id, expr) in &filters {
                if !stats_map.contains_key(id) {
                    new_optional_flags.push((*id, is_optional_filter(expr)));
                }
            }
            return PartitionResult {
                partitioned: PartitionedFilters {
                    row_filters: filters,
                    post_scan: Vec::new(),
                },
                new_optional_flags,
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

        // Use the latency-aware effective z (clamped to <= config.confidence_z).
        let confidence_z = z_eff;
        for (id, expr) in filters {
            let state = self.filter_states.get(&id).copied();

            let Some(state) = state else {
                // New filter: decide initial placement.
                //
                // We start at row-level only when the filter pulls in a
                // small amount of *extra* I/O — bytes for filter columns
                // **not already in the user projection** — relative to the
                // projection. These are the cases where the row-level
                // I/O cost is bounded and late materialization on a
                // selective filter is a clear win (think a small int
                // column predicate against a heavy string projection).
                //
                // Two cases default to post-scan instead, with the
                // tracker free to promote later if measured
                // bytes-saved-per-sec exceeds `min_bytes_per_sec`:
                //
                // - `extra_bytes == 0`: filter cols are entirely in the
                //   projection (e.g. `WHERE col <> '' GROUP BY col`).
                //   There's no I/O to save; the only payoff is late
                //   materialization on the *non*-filter projection
                //   columns, which depends on selectivity we don't know
                //   yet. Empirically (ClickBench Q10/11/13/14/26)
                //   defaulting these to row-level loses to post-scan
                //   because predicate-cache eviction on heavy string
                //   columns means the filter column is decoded twice.
                //
                // - `byte_ratio > byte_ratio_threshold`: extra I/O is
                //   too high to justify before we have evidence the
                //   filter is selective.
                //
                // Pre-existing snapshot-generation handling
                // ([`SelectivityTrackerInner::note_generation`]) keeps
                // dynamic filters (hash-join, TopK) at post-scan when
                // they re-arm with new values — those rely on row-group
                // statistics pruning rather than row-level I/O savings,
                // so post-scan is correct for them too.
                let filter_columns: Vec<usize> = collect_columns(&expr)
                    .iter()
                    .map(|col| col.index())
                    .collect();
                let extra_columns: Vec<usize> = filter_columns
                    .iter()
                    .copied()
                    .filter(|c| !projection_columns.contains(c))
                    .collect();
                let extra_bytes =
                    crate::row_filter::total_compressed_bytes(&extra_columns, metadata);
                let byte_ratio = if projection_scan_size > 0 {
                    extra_bytes as f64 / projection_scan_size as f64
                } else {
                    1.0
                };

                if !stats_map.contains_key(&id) {
                    new_optional_flags.push((id, is_optional_filter(&expr)));
                }

                // Selectivity prior from page-index pruning that the
                // opener already ran on this file (see
                // `PagePruningAccessPlanFilter::prune_plan_with_per_conjunct_stats`).
                // No extra pruning work is done here — we just look up
                // this filter's per-conjunct rate. When no rate is
                // available (page index disabled, predicate not
                // single-column, or schema mismatch), we fall back to
                // the existing byte-ratio heuristic.
                //
                // **Dynamic-filter refresh**: when this conjunct is a
                // populated DynamicFilter (snapshot_generation > 0)
                // we evaluate a per-conjunct `PruningPredicate` against
                // the file's row-group stats *now*, because the
                // side-effect rates captured at file open were taken
                // when the filter was still a placeholder. This is
                // targeted re-evaluation — only for dynamic conjuncts
                // that have updated since file open — so it doesn't
                // count as an "extra pruning run" on the static path.
                let dynamic_rate = if snapshot_generation(&expr) > 0 {
                    fresh_rate_for_dynamic_conjunct(
                        &expr,
                        arrow_schema,
                        parquet_schema,
                        metadata,
                    )
                } else {
                    None
                };
                let prior = dynamic_rate.or_else(|| page_pruning_rates.get(&id).copied());

                let row_level = match prior {
                    Some(p) if p >= config.prior_promote_threshold => {
                        debug!(
                            "FilterId {id}: New filter → Row filter via page-prior (pruned_rate={p:.3} >= {}) — {expr}",
                            config.prior_promote_threshold
                        );
                        true
                    }
                    Some(p) if p <= config.prior_demote_threshold => {
                        debug!(
                            "FilterId {id}: New filter → Post-scan via page-prior (pruned_rate={p:.3} <= {}) — {expr}",
                            config.prior_demote_threshold
                        );
                        false
                    }
                    _ => {
                        let r =
                            extra_bytes > 0 && byte_ratio <= config.byte_ratio_threshold;
                        debug!(
                            "FilterId {id}: New filter → {} via byte_ratio (byte_ratio={byte_ratio:.4}, extra_bytes={extra_bytes}, prior={prior:?}) — {expr}",
                            if r { "Row filter" } else { "Post-scan" }
                        );
                        r
                    }
                };

                if row_level {
                    self.filter_states.insert(id, FilterState::RowFilter);
                    row_filters.push((id, expr));
                } else {
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
                    // Single gate: scatter-aware CI lower bound on
                    // bytes-saved-per-sec ≥ `min_bytes_per_sec`.
                    //
                    // The metric (see [`SelectivityStats::update`])
                    // counts only sub-batch windows the filter empties
                    // out, so a 50% uniform filter scores ~0 and stays
                    // at post-scan; a TopK / hash-join / `Title LIKE`
                    // style filter where most batches drop entirely
                    // blows past the threshold.
                    //
                    // Earlier revisions also required `prune_rate ≥ 99%`
                    // on the theory that arrow-rs's row-level path
                    // double-decoded heavy string columns when the
                    // filter and projection overlapped. EXPLAIN ANALYZE
                    // on the ClickBench Q23 workload (URL LIKE
                    // `%google%`) showed the predicate cache is in fact
                    // active (`predicate_cache_inner_records=8.76M`)
                    // and the filter column is decoded once. The gate
                    // was removed; the residual ClickBench regressions
                    // we attributed to it (Q26 / Q31) trace to a
                    // different cause: post-scan filtering inside the
                    // opener changes batch-arrival order at downstream
                    // TopK, shifting the convergence point of TopK's
                    // dynamic filter and slightly weakening file-stats
                    // pruning. That has nothing to do with the
                    // promotion decision.
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
                            && expr.downcast_ref::<OptionalFilterPhysicalExpr>().is_some()
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
            new_optional_flags,
        }
    }
}

/// Returns `true` if `expr` is wrapped in [`OptionalFilterPhysicalExpr`].
fn is_optional_filter(expr: &Arc<dyn PhysicalExpr>) -> bool {
    expr.downcast_ref::<OptionalFilterPhysicalExpr>().is_some()
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

// (Per-conjunct page-pruning rates are now extracted as a side-effect
// of the opener's existing page-index pruning pass — see
// `PagePruningAccessPlanFilter::prune_plan_with_per_conjunct_stats`.
// `partition_filters` reads them through its `page_pruning_rates`
// parameter; no extra pruning runs happen on the static path.)

/// Compute a fresh row-group pruning rate for a single dynamic
/// conjunct, evaluated against the file's row-group statistics
/// *now*. Used by `partition_filters` to refresh the prior for
/// dynamic filters that were placeholders when the side-effect
/// rates were captured at file open and have since been populated
/// by the join build side.
///
/// Returns `None` when the conjunct doesn't translate into a
/// usable pruning predicate (e.g. always-true after rewriting,
/// references columns missing from the schema, contains
/// hash_lookup-style nodes the rewriter can't handle).
fn fresh_rate_for_dynamic_conjunct(
    expr: &Arc<dyn PhysicalExpr>,
    arrow_schema: &SchemaRef,
    parquet_schema: &SchemaDescriptor,
    metadata: &ParquetMetaData,
) -> Option<f64> {
    use datafusion_pruning::PruningPredicate;
    // Unwrap OptionalFilterPhysicalExpr — pruning should evaluate
    // the underlying predicate, not the marker.
    let inner = if let Some(opt) = expr.downcast_ref::<OptionalFilterPhysicalExpr>() {
        opt.inner()
    } else {
        Arc::clone(expr)
    };
    let groups = metadata.row_groups();
    if groups.is_empty() {
        return None;
    }
    let stats = crate::row_group_filter::RowGroupPruningStatistics {
        parquet_schema,
        row_group_metadatas: groups.iter().collect(),
        arrow_schema: arrow_schema.as_ref(),
        missing_null_counts_as_zero: false,
    };

    // First try: build a PruningPredicate from the whole conjunct.
    if let Ok(pp) =
        PruningPredicate::try_new(Arc::clone(&inner), Arc::clone(arrow_schema))
        && !pp.always_true()
        && let Ok(kept) = pp.prune(&stats)
        && !kept.is_empty()
    {
        let total = kept.len();
        let pruned = total - kept.iter().filter(|b| **b).count();
        return Some(pruned as f64 / total as f64);
    }

    // Second try (the AND-with-hash-lookup case): snapshot the
    // dynamic filter to materialize its current inner expression,
    // then split the AND inside. `split_conjunction` doesn't descend
    // into DynamicFilterPhysicalExpr wrappers, so without this step
    // the split would return `[dynamic_filter]` and miss the
    // prunable parts inside. We take the *max* pruning rate across
    // sub-parts as a *promote* signal — if any sub-conjunct prunes
    // a high fraction, the whole AND prunes at least that much. We
    // deliberately do NOT use this as a demote signal.
    let snapshot_result =
        datafusion_physical_expr_common::physical_expr::snapshot_physical_expr_opt(
            Arc::clone(&inner),
        )
        .ok()?;
    let snapshotted = snapshot_result.data;
    let parts = datafusion_physical_expr::split_conjunction(&snapshotted);
    if parts.len() < 2 {
        return None;
    }
    let mut max_rate: Option<f64> = None;
    for part in parts {
        let Ok(pp) =
            PruningPredicate::try_new(Arc::clone(part), Arc::clone(arrow_schema))
        else {
            continue;
        };
        if pp.always_true() {
            continue;
        }
        let Ok(kept) = pp.prune(&stats) else { continue };
        if kept.is_empty() {
            continue;
        }
        let total = kept.len();
        let pruned = total - kept.iter().filter(|b| **b).count();
        let rate = pruned as f64 / total as f64;
        max_rate = Some(max_rate.map_or(rate, |m| m.max(rate)));
    }
    // Promote-only semantics: only return when the partial-AND rate
    // is high enough to be a confident promote signal. Below that we
    // return None and let the standard prior / byte-ratio fallback
    // run, which won't be misled by an undercounted rate.
    max_rate.filter(|&r| r >= 0.5)
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
            Arc::new(SchemaDescriptor::new(Arc::new(schema)))
        }
    }

    mod selectivity_stats_tests {
        use super::*;

        #[test]
        fn test_effectiveness_basic_calculation() {
            let mut stats = SelectivityStats::default();

            // skippable_bytes is now caller-computed (= rows_pruned *
            // bytes_per_row in the simple case), so passing 5000 directly
            // models the same scenario the old test described:
            // "100 rows total, 50 pruned, 100 bytes/row → 5000 saved".
            stats.update(50, 100, 1_000_000_000, 5_000);

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
            // A batch with zero skippable_bytes is a legitimate sample
            // ("filter ran, late-mat had nothing to save") — Welford
            // records it as eff=0 rather than discarding it, so the
            // demotion path can see "CPU spent, no payoff."
            let mut stats = SelectivityStats::default();
            stats.update(50, 100, 1_000_000_000, 0);

            assert_eq!(stats.effectiveness(), Some(0.0));
        }

        #[test]
        fn test_effectiveness_all_rows_matched() {
            let mut stats = SelectivityStats::default();
            // All rows matched (no pruning) — caller computes
            // skippable_bytes = rows_pruned * bytes_per_row = 0.
            stats.update(100, 100, 1_000_000_000, 0);

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

            // Add samples that produce effectiveness values 5000, 6000, 7000
            // (caller-computed skippable_bytes is the lever now).
            stats.update(50, 100, 1_000_000_000, 5_000); // eff = 5000
            stats.update(40, 100, 1_000_000_000, 6_000); // eff = 6000
            stats.update(30, 100, 1_000_000_000, 7_000); // eff = 7000

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

            let result = tracker.partition_filters_for_test(
                filters,
                &std::collections::HashSet::new(),
                1000,
                &metadata,
            );

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

            let result = tracker.partition_filters_for_test(
                filters,
                &std::collections::HashSet::new(),
                1000,
                &metadata,
            );

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

            let result = tracker.partition_filters_for_test(
                filters,
                &std::collections::HashSet::new(),
                1000,
                &metadata,
            );

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

            let result = tracker.partition_filters_for_test(
                filters,
                &std::collections::HashSet::new(),
                1000,
                &metadata,
            );

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

            let result = tracker.partition_filters_for_test(
                filters,
                &std::collections::HashSet::new(),
                1000,
                &metadata,
            );

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
            let result = tracker.partition_filters_for_test(
                filters.clone(),
                &std::collections::HashSet::new(),
                1000,
                &metadata,
            );
            assert_eq!(result.post_scan.len(), 1);
            assert_eq!(result.row_filters.len(), 0);

            // Feed high effectiveness stats
            for _ in 0..5 {
                tracker.update(1, 1, 100, 100_000, 1000); // high effectiveness
            }

            // Second partition: should be promoted to RowFilter
            let result = tracker.partition_filters_for_test(
                filters,
                &std::collections::HashSet::new(),
                1000,
                &metadata,
            );
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
            let result = tracker.partition_filters_for_test(
                filters.clone(),
                &std::collections::HashSet::new(),
                1000,
                &metadata,
            );
            assert_eq!(result.row_filters.len(), 1);
            assert_eq!(result.post_scan.len(), 0);

            // Feed low effectiveness stats — all rows matched, no rows
            // pruned, so caller-computed skippable_bytes is 0.
            for _ in 0..5 {
                tracker.update(1, 100, 100, 100_000, 0);
            }

            // Second partition: should be demoted to PostScan
            let result = tracker.partition_filters_for_test(
                filters,
                &std::collections::HashSet::new(),
                1000,
                &metadata,
            );
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
            tracker.partition_filters_for_test(
                filters.clone(),
                &std::collections::HashSet::new(),
                1000,
                &metadata,
            );

            // Add stats — no pruning, so skippable_bytes = 0
            tracker.update(1, 100, 100, 100_000, 0);
            tracker.update(1, 100, 100, 100_000, 0);

            // Demote
            tracker.partition_filters_for_test(
                filters.clone(),
                &std::collections::HashSet::new(),
                1000,
                &metadata,
            );

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
            tracker.partition_filters_for_test(
                filters.clone(),
                &std::collections::HashSet::new(),
                1000,
                &metadata,
            );

            // Add stats with high prune_rate so the selectivity gate
            // (>= 0.99) lets the promotion fire.
            for _ in 0..3 {
                tracker.update(1, 1, 100, 100_000, 1000);
            }

            // Promote
            tracker.partition_filters_for_test(
                filters.clone(),
                &std::collections::HashSet::new(),
                1000,
                &metadata,
            );

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
            tracker.partition_filters_for_test(
                filters.clone(),
                &std::collections::HashSet::new(),
                1000,
                &metadata,
            );

            // Feed poor effectiveness stats — no pruning, no skippable_bytes
            for _ in 0..5 {
                tracker.update(1, 100, 100, 100_000, 0);
            }

            // Next partition: should stay as PostScan (not dropped because not optional)
            let result = tracker.partition_filters_for_test(
                filters,
                &std::collections::HashSet::new(),
                1000,
                &metadata,
            );
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
            let result = tracker.partition_filters_for_test(
                filters,
                &std::collections::HashSet::new(),
                1000,
                &metadata,
            );
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
            let result = tracker.partition_filters_for_test(
                filters.clone(),
                &std::collections::HashSet::new(),
                1000,
                &metadata,
            );

            // With min_bytes_per_sec=1.0, filters should be partitioned
            assert!(result.row_filters.len() + result.post_scan.len() > 0);

            // Add stats and partition again
            tracker.update(1, 60, 100, 1_000_000, 100);
            tracker.update(2, 1, 100, 1_000_000, 100);
            tracker.update(3, 40, 100, 1_000_000, 100);

            let result2 = tracker.partition_filters_for_test(
                filters,
                &std::collections::HashSet::new(),
                1000,
                &metadata,
            );

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
            let result = tracker.partition_filters_for_test(
                filters.clone(),
                &std::collections::HashSet::new(),
                1000,
                &metadata,
            );

            // All filters should be processed (partitioned into row/post-scan)
            assert!(result.row_filters.len() + result.post_scan.len() > 0);

            // Filters should be consistent on repeated calls
            let result2 = tracker.partition_filters_for_test(
                filters,
                &std::collections::HashSet::new(),
                1000,
                &metadata,
            );
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
            let result1 = tracker.partition_filters_for_test(
                filters.clone(),
                &std::collections::HashSet::new(),
                1000,
                &metadata,
            );
            assert!(result1.row_filters.len() + result1.post_scan.len() > 0);

            // Only add stats for filters 1 and 3, not 2
            tracker.update(1, 60, 100, 1_000_000, 100);
            tracker.update(3, 60, 100, 1_000_000, 100);

            // Second partition with partial stats
            let result2 = tracker.partition_filters_for_test(
                filters,
                &std::collections::HashSet::new(),
                1000,
                &metadata,
            );
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

            let result1 = tracker.partition_filters_for_test(
                filters.clone(),
                &std::collections::HashSet::new(),
                1000,
                &metadata,
            );
            let result2 = tracker.partition_filters_for_test(
                filters,
                &std::collections::HashSet::new(),
                1000,
                &metadata,
            );

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

            tracker.partition_filters_for_test(
                filters1,
                &std::collections::HashSet::new(),
                1000,
                &metadata,
            );
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
            tracker.partition_filters_for_test(
                filters.clone(),
                &std::collections::HashSet::new(),
                1000,
                &metadata,
            );

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
            let result = tracker.partition_filters_for_test(
                filters.clone(),
                &std::collections::HashSet::new(),
                1000,
                &metadata,
            );
            assert_eq!(result.post_scan.len(), 1);
            assert_eq!(result.row_filters.len(), 0);

            // Step 2: Accumulate high effectiveness stats
            for _ in 0..5 {
                tracker.update(1, 1, 100, 100_000, 1000); // high effectiveness
            }

            // Step 3: Promotion should occur
            let result = tracker.partition_filters_for_test(
                filters.clone(),
                &std::collections::HashSet::new(),
                1000,
                &metadata,
            );
            assert_eq!(result.row_filters.len(), 1);
            assert_eq!(result.post_scan.len(), 0);

            // Step 4: Continue to partition without additional updates
            let result = tracker.partition_filters_for_test(
                filters,
                &std::collections::HashSet::new(),
                1000,
                &metadata,
            );
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
            let result = tracker.partition_filters_for_test(
                filters.clone(),
                &std::collections::HashSet::new(),
                1000,
                &metadata,
            );
            assert_eq!(result.row_filters.len(), 1);
            assert_eq!(result.post_scan.len(), 0);

            // Step 2: Accumulate low effectiveness stats — no pruning,
            // so skippable_bytes = 0
            for _ in 0..5 {
                tracker.update(1, 100, 100, 100_000, 0);
            }

            // Step 3: Demotion should occur
            let result = tracker.partition_filters_for_test(
                filters.clone(),
                &std::collections::HashSet::new(),
                1000,
                &metadata,
            );
            assert_eq!(result.row_filters.len(), 0);
            assert_eq!(result.post_scan.len(), 1);

            // Step 4: Continue to partition without additional updates
            let result = tracker.partition_filters_for_test(
                filters,
                &std::collections::HashSet::new(),
                1000,
                &metadata,
            );
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
            let result = tracker.partition_filters_for_test(
                filters.clone(),
                &std::collections::HashSet::new(),
                1000,
                &metadata,
            );
            assert_eq!(result.post_scan.len(), 2);

            // Filter 1: high effectiveness — 99/100 rows pruned out of
            // 500 batch bytes ≈ 495 skippable bytes
            for _ in 0..3 {
                tracker.update(1, 1, 100, 100_000, 495);
            }

            // Filter 2: low effectiveness — no rows pruned, so 0 skippable
            for _ in 0..3 {
                tracker.update(2, 100, 100, 100_000, 0);
            }

            // Next partition: Filter 1 promoted, Filter 2 stays PostScan
            let result = tracker.partition_filters_for_test(
                filters,
                &std::collections::HashSet::new(),
                1000,
                &metadata,
            );
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

            let result = tracker.partition_filters_for_test(
                filters,
                &std::collections::HashSet::new(),
                1000,
                &metadata,
            );

            assert_eq!(result.row_filters.len(), 0);
            assert_eq!(result.post_scan.len(), 0);
        }

        #[test]
        fn test_single_filter() {
            let tracker = TrackerConfig::new().with_min_bytes_per_sec(0.0).build();

            let metadata = create_test_metadata(vec![(100, vec![100])]);
            let expr = col_expr("a", 0);
            let filters = vec![(1, expr)];

            let result = tracker.partition_filters_for_test(
                filters,
                &std::collections::HashSet::new(),
                1000,
                &metadata,
            );

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
            tracker.partition_filters_for_test(
                filters.clone(),
                &std::collections::HashSet::new(),
                1000,
                &metadata,
            );

            // All rows match (zero effectiveness) — no rows pruned, so
            // skippable_bytes = 0
            for _ in 0..5 {
                tracker.update(1, 100, 100, 100_000, 0);
            }

            // Should demote due to CI upper bound being 0
            let result = tracker.partition_filters_for_test(
                filters,
                &std::collections::HashSet::new(),
                1000,
                &metadata,
            );
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
