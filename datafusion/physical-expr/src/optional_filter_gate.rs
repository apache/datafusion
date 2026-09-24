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

//! A runtime gate that pauses optional filters that remove too few rows, or
//! that cost more than they save.
//!
//! An *optional filter* is a filter that is not needed for correctness, for
//! example a dynamic filter that a hash join or a TopK pushes down into a
//! scan. An operator can skip such a filter and still produce correct
//! results. When the filter removes few rows, or when it is expensive (for
//! example a hash table lookup with many columns), the cost to evaluate it
//! can be larger than the benefit.
//!
//! [`OptionalFilterGate`] decides, batch by batch, if a stream evaluates the
//! filter or skips it. Each stream has its own gate. All gates of one plan
//! site (one operator and one optional filter) share one
//! [`OptionalFilterSiteStats`].
//!
//! # State machine
//!
//! ```text
//!                 keep (see "Decision")
//!              (reset backoff, new window)
//!                     +-------+
//!                     |       |
//!                     v       |
//!               +-------------+-+           pause                 +---------------------+
//!  start ------>|   Evaluate    |-------------------------------->|       Paused        |
//!               | (window of    |   (pause for `backoff` batches, | (skip `remaining`   |
//!               | sample_batches|    then double `backoff`)       |  batches)           |
//!               | batches)      |<--------------------------------|                     |
//!               +---------------+   pause ends: probe with a      +---------------------+
//!                                   fresh window
//! ```
//!
//! * In `Evaluate`, the gate collects the rows in, the rows out and the
//!   evaluation time of `sample_batches` batches (a *window*). Then it
//!   decides (see below). To pause, it goes to `Paused` for `backoff` batches
//!   and doubles `backoff` (up to `max_pause_batches`). To keep the filter,
//!   it stays in `Evaluate`, sets `backoff` to `initial_pause_batches` and
//!   starts a new window.
//! * In `Paused`, the gate skips batches. It does not change counters or
//!   `backoff` for skipped batches. When the pause ends, the gate evaluates
//!   a new window (a *probe*).
//! * Before each batch the gate checks if the filter changed (for example a
//!   dynamic filter got new bounds). If so, the gate goes to `Evaluate` with
//!   an empty window and sets `backoff` to `initial_pause_batches`.
//!
//! # Decision
//!
//! At the end of each window, the gate pauses the filter if one of these
//! checks is true. The first check is cheap and does not use time.
//!
//! 1. *Pass ratio*: more than `max_pass_ratio` of the rows passed.
//! 2. *Cost*: the evaluation time of the window (`cost_ns`) is larger than
//!    the work that the removed rows save (`saving_ns`):
//!
//!    ```text
//!    saving_ns = (rows_in - rows_out) * saving_ns_per_row
//!    saving_ns_per_row = min_saving_ns_per_row + measured saving
//!    ```
//!
//!    `min_saving_ns_per_row` comes from the configuration. It is the work
//!    that a removed row saves after the filter, for example a hash table
//!    probe in a join. The *measured saving* is optional: a consumer that
//!    can measure more work that a removed row saves (the Parquet scan
//!    measures the decode time of the columns that the filter does not read)
//!    gives it in a shared [`MeasuredRowSaving`] and updates it at any time.
//!
//!    To prevent a filter from switching on and off when the cost and the
//!    saving are almost equal, the cost check has a margin: a running filter
//!    is paused only if `cost_ns > saving_ns * 1.1`, and a probe after a
//!    pause turns the filter on again only if `cost_ns < saving_ns * 0.9`.
//!
//! Thus a filter that removes most rows but is expensive is paused, and a
//! cheap filter stays on also when it removes only some of the rows.
//!
//! The gate measures time with a [`Clock`]. Tests use a [`ManualClock`], so
//! that the decisions are deterministic.
//!
//! [`ManualClock`]: crate::filter_stats::ManualClock
//!
//! # Change detection
//!
//! The gate walks the filter one time, when it is created, with
//! [`DynamicFilterTracking::classify`]. The walk subscribes to each
//! [`DynamicFilterPhysicalExpr`] in the filter that is not complete. Before
//! each batch, the gate polls these subscriptions with
//! [`DynamicFilterTracker::changed`]. When nothing changed, this is one
//! atomic load for each subscription. The tracker drops a subscription when
//! its filter is complete. A filter without dynamic filters, or with only
//! complete dynamic filters, is never polled and never resets the gate.
//!
//! The gate identifies the current version of the filter with a
//! *generation tag*: the sum of the generations of the dynamic filters in
//! the filter. The tag comes from the subscriptions and from the walk at
//! creation, thus the gate does not walk the filter again to get it.
//!
//! [`DynamicFilterPhysicalExpr`]: crate::expressions::DynamicFilterPhysicalExpr
//! [`DynamicFilterTracker::changed`]: crate::expressions::DynamicFilterTracker::changed
//!
//! # Pooled statistics
//!
//! When a gate makes a decision, it adds its window counts and time to the
//! shared [`OptionalFilterSiteStats`] and publishes its verdict (paused or
//! not) and its pause length. A new gate reads the pooled verdict one time,
//! when it is created: if the pooled verdict is "paused" for the current
//! generation tag of the filter, the new gate starts in `Paused`. Thus a new
//! file or partition does not have to learn again that the filter is not
//! worth its cost. The pooled verdict never changes the state of a gate that
//! is running.

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use arrow::array::{BooleanArray, RecordBatch};
use datafusion_common::Result;
use datafusion_common::cast::as_boolean_array;
use datafusion_common::config::ExecutionOptions;
use datafusion_physical_expr_common::physical_expr::PhysicalExpr;

use crate::expressions::DynamicFilterTracking;
use crate::filter_stats::{Clock, FilterCost, SystemClock, duration_nanos};

/// A running filter is paused by the cost check only if its cost is larger
/// than this multiple of its saving. See the [module documentation](self).
const PAUSE_COST_MARGIN: f64 = 1.1;

/// A probe turns a paused filter on again (by the cost check) only if its
/// cost is smaller than this multiple of its saving.
const RESUME_COST_MARGIN: f64 = 0.9;

/// Configuration of an [`OptionalFilterGate`].
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct OptionalFilterGateConfig {
    /// Pause the filter when the fraction of rows that pass it is larger than
    /// this value.
    pub max_pass_ratio: f64,
    /// Number of evaluated batches in one window. The gate makes a decision
    /// at the end of each window. Values smaller than 1 are used as 1.
    pub sample_batches: usize,
    /// Number of batches to skip at the first pause, and after the filter
    /// was selective again. Values smaller than 1 are used as 1.
    pub initial_pause_batches: usize,
    /// Maximum number of batches to skip in one pause. Values smaller than
    /// `initial_pause_batches` are used as `initial_pause_batches`.
    pub max_pause_batches: usize,
    /// Work, in nanoseconds, that each row removed by the filter saves after
    /// the filter, at the least. The gate adds the saving that the consumer
    /// measures (see [`MeasuredRowSaving`]). The gate pauses a filter whose
    /// evaluation time is larger than the saving of the rows that it
    /// removes. Negative values are used as 0.
    pub min_saving_ns_per_row: f64,
}

impl Default for OptionalFilterGateConfig {
    fn default() -> Self {
        Self {
            max_pass_ratio: 0.8,
            sample_batches: 2,
            initial_pause_batches: 4,
            max_pause_batches: 32,
            min_saving_ns_per_row: 20.0,
        }
    }
}

impl From<&ExecutionOptions> for OptionalFilterGateConfig {
    /// Uses `optional_filter_max_pass_ratio` and
    /// `optional_filter_min_saving_ns_per_row` from `options`, and the default
    /// values for the other fields.
    fn from(options: &ExecutionOptions) -> Self {
        Self {
            max_pass_ratio: options.optional_filter_max_pass_ratio,
            min_saving_ns_per_row: options.optional_filter_min_saving_ns_per_row,
            ..Default::default()
        }
    }
}

impl OptionalFilterGateConfig {
    /// Returns a copy with the documented minimum values applied.
    fn normalized(self) -> Self {
        let sample_batches = self.sample_batches.max(1);
        let initial_pause_batches = self.initial_pause_batches.max(1);
        let max_pause_batches = self.max_pause_batches.max(initial_pause_batches);
        Self {
            max_pass_ratio: self.max_pass_ratio,
            sample_batches,
            initial_pause_batches,
            max_pause_batches,
            min_saving_ns_per_row: self.min_saving_ns_per_row.max(0.0),
        }
    }
}

/// The decision of an [`OptionalFilterGate`] for one batch.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum GateDecision {
    /// Evaluate the filter on this batch, then call
    /// [`OptionalFilterGate::record`] with the row counts and the time.
    Evaluate,
    /// Do not evaluate the filter on this batch. Let all rows pass.
    Skip,
}

/// Work, in nanoseconds, that each row removed by an optional filter saves,
/// as measured by the consumer of the filter. The gate adds it to
/// [`OptionalFilterGateConfig::min_saving_ns_per_row`].
///
/// The consumer creates one value, gives a clone of the [`Arc`] to the gate
/// with [`OptionalFilterGate::with_measured_saving`], and updates it at any
/// time with [`Self::set_ns_per_row`]. The gate reads it at each decision.
/// For example, the Parquet scan sets it to the time to decode the columns
/// that the filter does not read, for each row.
///
/// The value is an `f64` in an [`AtomicU64`], thus reads and updates are
/// cheap and lock-free.
#[derive(Debug, Default)]
pub struct MeasuredRowSaving {
    /// The bits of the `f64` value.
    ns_per_row_bits: AtomicU64,
}

impl MeasuredRowSaving {
    /// Creates a value of 0 ns.
    pub fn new() -> Self {
        Self::default()
    }

    /// Sets the measured saving for each removed row, in nanoseconds.
    /// Values that are negative or not finite are used as 0.
    pub fn set_ns_per_row(&self, ns_per_row: f64) {
        let ns_per_row = if ns_per_row.is_finite() {
            ns_per_row.max(0.0)
        } else {
            0.0
        };
        self.ns_per_row_bits
            .store(ns_per_row.to_bits(), Ordering::Relaxed);
    }

    /// The measured saving for each removed row, in nanoseconds.
    pub fn ns_per_row(&self) -> f64 {
        f64::from_bits(self.ns_per_row_bits.load(Ordering::Relaxed))
    }
}

/// Statistics of one optional filter at one plan site, shared by all the
/// [`OptionalFilterGate`]s of that site.
///
/// Create one value for each operator and each optional filter, wrap it in
/// an [`Arc`], and give a clone to each partition, stream or file of that
/// operator.
///
/// # Implementation
///
/// All fields are atomics, thus the type is lock-free. Gates write to it only
/// at decision points (one time for each `sample_batches` evaluated batches)
/// and read the verdict only when they are created.
///
/// The verdict, the pause length and the generation tag of the filter are
/// packed into one `u64`, so that a reader always sees a verdict together
/// with the generation it belongs to. The tag is the sum of the generations
/// of the dynamic filters in the filter (0 for a static filter), and it is
/// stored as its low 32 bits. Generations only increase, thus two tags of
/// one filter have the same low 32 bits only after 2^32 updates of that
/// filter. The worst result of such a collision is a wrong initial state for
/// a new gate, which the gate then corrects.
#[derive(Debug, Default)]
pub struct OptionalFilterSiteStats {
    /// Rows that went into the filter, for the current generation.
    rows_in: AtomicU64,
    /// Rows that passed the filter, for the current generation.
    rows_out: AtomicU64,
    /// Evaluation time in nanoseconds, for the current generation.
    nanos: AtomicU64,
    /// Packed verdict, see [`PackedVerdict`].
    verdict: AtomicU64,
}

/// The verdict of the last decision at a plan site.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct PackedVerdict {
    /// Low 32 bits of the generation tag of the filter.
    generation_tag: u32,
    /// True if the last decision paused the filter.
    paused: bool,
    /// Length (in batches) of the pause that the last decision started.
    pause_batches: u32,
}

impl PackedVerdict {
    const PAUSED_BIT: u64 = 1 << 63;
    const MAX_PAUSE_BATCHES: u32 = (1 << 31) - 1;

    fn new(generation: u64, paused: bool, pause_batches: usize) -> Self {
        Self {
            generation_tag: generation as u32,
            paused,
            pause_batches: u32::try_from(pause_batches)
                .unwrap_or(u32::MAX)
                .min(Self::MAX_PAUSE_BATCHES),
        }
    }

    fn pack(self) -> u64 {
        let paused = if self.paused { Self::PAUSED_BIT } else { 0 };
        paused | (u64::from(self.pause_batches) << 32) | u64::from(self.generation_tag)
    }

    fn unpack(word: u64) -> Self {
        Self {
            generation_tag: word as u32,
            paused: word & Self::PAUSED_BIT != 0,
            pause_batches: ((word & !Self::PAUSED_BIT) >> 32) as u32,
        }
    }

    fn matches(self, generation: u64) -> bool {
        self.generation_tag == generation as u32
    }
}

impl OptionalFilterSiteStats {
    /// Creates empty statistics.
    pub fn new() -> Self {
        Self::default()
    }

    /// Rows that went into the filter at this site, for the generation of
    /// the last decision. Skipped batches are not counted.
    pub fn rows_in(&self) -> u64 {
        self.rows_in.load(Ordering::Relaxed)
    }

    /// Rows that passed the filter at this site, for the generation of the
    /// last decision. Skipped batches are not counted.
    pub fn rows_out(&self) -> u64 {
        self.rows_out.load(Ordering::Relaxed)
    }

    /// Evaluation time in nanoseconds at this site, for the generation of
    /// the last decision. Skipped batches are not counted.
    pub fn nanos(&self) -> u64 {
        self.nanos.load(Ordering::Relaxed)
    }

    /// The pooled rows in, rows out and time, for the generation of the
    /// last decision. The three values are not read at the same instant.
    pub fn cost(&self) -> FilterCost {
        FilterCost {
            rows_in: self.rows_in(),
            rows_out: self.rows_out(),
            nanos: self.nanos(),
        }
    }

    /// Returns the pause length of the pooled verdict if the verdict is
    /// "paused" for `generation`.
    fn paused_for(&self, generation: u64) -> Option<usize> {
        let verdict = PackedVerdict::unpack(self.verdict.load(Ordering::Acquire));
        (verdict.paused && verdict.matches(generation) && verdict.pause_batches > 0)
            .then_some(verdict.pause_batches as usize)
    }

    /// Clears the counters and the verdict if they belong to a generation
    /// other than `generation`.
    fn reset_for_generation(&self, generation: u64) {
        let mut current = self.verdict.load(Ordering::Acquire);
        loop {
            if PackedVerdict::unpack(current).matches(generation) {
                return;
            }
            let new = PackedVerdict::new(generation, false, 0).pack();
            match self.verdict.compare_exchange_weak(
                current,
                new,
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                Ok(_) => {
                    self.rows_in.store(0, Ordering::Relaxed);
                    self.rows_out.store(0, Ordering::Relaxed);
                    self.nanos.store(0, Ordering::Relaxed);
                    return;
                }
                Err(actual) => current = actual,
            }
        }
    }

    /// Adds the counts and the time of one window and publishes the verdict
    /// of the decision made for that window.
    fn publish(&self, verdict: PackedVerdict, window: FilterCost) {
        let previous =
            PackedVerdict::unpack(self.verdict.swap(verdict.pack(), Ordering::AcqRel));
        if previous.generation_tag == verdict.generation_tag {
            self.rows_in.fetch_add(window.rows_in, Ordering::Relaxed);
            self.rows_out.fetch_add(window.rows_out, Ordering::Relaxed);
            self.nanos.fetch_add(window.nanos, Ordering::Relaxed);
        } else {
            self.rows_in.store(window.rows_in, Ordering::Relaxed);
            self.rows_out.store(window.rows_out, Ordering::Relaxed);
            self.nanos.store(window.nanos, Ordering::Relaxed);
        }
    }
}

/// The state of an [`OptionalFilterGate`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum GateState {
    /// Evaluate the filter and collect counts and time for the current
    /// window.
    Evaluate {
        window: FilterCost,
        batches_in_window: usize,
    },
    /// Skip the filter for `remaining_batches` more batches.
    Paused { remaining_batches: usize },
}

impl GateState {
    const fn new_window() -> Self {
        Self::Evaluate {
            window: FilterCost {
                rows_in: 0,
                rows_out: 0,
                nanos: 0,
            },
            batches_in_window: 0,
        }
    }
}

/// Decides, batch by batch, if one stream evaluates an optional filter.
///
/// See the [module documentation](self) for the state machine. A gate is
/// for one stream only. Do not share it between streams: share the
/// [`OptionalFilterSiteStats`] instead.
///
/// There are two ways to use a gate:
///
/// * Call [`Self::evaluate`] with each batch. The gate evaluates the filter
///   or skips it, and measures the time with its [`Clock`].
/// * If the caller evaluates the filter itself (for example a Parquet
///   `ArrowPredicate`), call [`Self::begin_batch`] before each batch. If it
///   returns [`GateDecision::Evaluate`], evaluate the filter and then call
///   [`Self::record`] with the row counts and the evaluation time. The
///   caller can measure the time with [`Self::clock`].
#[derive(Debug)]
pub struct OptionalFilterGate {
    filter: Arc<dyn PhysicalExpr>,
    /// The dynamic filters in `filter` that can still change.
    tracking: DynamicFilterTracking,
    site: Arc<OptionalFilterSiteStats>,
    config: OptionalFilterGateConfig,
    /// The clock of [`Self::evaluate`].
    clock: Arc<dyn Clock>,
    /// The saving that the consumer measures, added to
    /// `config.min_saving_ns_per_row`.
    measured_saving: Option<Arc<MeasuredRowSaving>>,
    state: GateState,
    /// True while the current window is a probe after a pause. The cost
    /// check then uses [`RESUME_COST_MARGIN`].
    probing: bool,
    /// Length of the next pause, in batches.
    backoff: usize,
    /// Generation tag of `filter` at the last observed change.
    last_generation: u64,
    /// True after `begin_batch` returned `Evaluate` and before `record`.
    awaiting_record: bool,
    rows_skipped: usize,
    pauses: usize,
    batches_evaluated: usize,
}

impl OptionalFilterGate {
    /// Creates a gate for `filter`, a boolean expression.
    ///
    /// If the pooled verdict in `site` is "paused" for the current generation
    /// tag of `filter`, the gate starts in the paused state. Otherwise it
    /// starts to evaluate the filter.
    ///
    /// The gate uses a [`SystemClock`] and no measured saving. See
    /// [`Self::with_clock`] and [`Self::with_measured_saving`].
    ///
    /// This walks `filter` one time to find its dynamic filters.
    pub fn new(
        filter: Arc<dyn PhysicalExpr>,
        site: Arc<OptionalFilterSiteStats>,
        config: OptionalFilterGateConfig,
    ) -> Self {
        let config = config.normalized();
        let (tracking, generation) =
            DynamicFilterTracking::classify_with_generation_tag(&filter);
        let mut gate = Self {
            filter,
            tracking,
            site,
            config,
            clock: SystemClock::shared(),
            measured_saving: None,
            state: GateState::new_window(),
            probing: false,
            backoff: config.initial_pause_batches,
            last_generation: generation,
            awaiting_record: false,
            rows_skipped: 0,
            pauses: 0,
            batches_evaluated: 0,
        };
        if let Some(pause_batches) = gate.site.paused_for(generation) {
            gate.start_pause(pause_batches);
        }
        gate
    }

    /// Uses `clock` to measure the evaluation time in [`Self::evaluate`].
    /// Consumers that call [`Self::record`] can use it too, see
    /// [`Self::clock`].
    pub fn with_clock(mut self, clock: Arc<dyn Clock>) -> Self {
        self.clock = clock;
        self
    }

    /// Adds `saving` to [`OptionalFilterGateConfig::min_saving_ns_per_row`]
    /// at each decision. See [`MeasuredRowSaving`].
    pub fn with_measured_saving(mut self, saving: Arc<MeasuredRowSaving>) -> Self {
        self.measured_saving = Some(saving);
        self
    }

    /// The filter of this gate.
    pub fn filter(&self) -> &Arc<dyn PhysicalExpr> {
        &self.filter
    }

    /// The clock of this gate.
    pub fn clock(&self) -> &Arc<dyn Clock> {
        &self.clock
    }

    /// The work, in nanoseconds, that the gate assumes each removed row
    /// saves now: the configured minimum plus the measured saving.
    pub fn saving_ns_per_row(&self) -> f64 {
        let measured = self
            .measured_saving
            .as_ref()
            .map_or(0.0, |saving| saving.ns_per_row());
        self.config.min_saving_ns_per_row + measured
    }

    /// The shared statistics of this gate.
    pub fn site(&self) -> &Arc<OptionalFilterSiteStats> {
        &self.site
    }

    /// Call before each batch. Returns if the caller must evaluate the
    /// filter on the batch or skip it.
    ///
    /// `num_rows` is the number of rows in the batch. The gate uses it only
    /// for [`Self::rows_skipped`].
    ///
    /// If the result is [`GateDecision::Evaluate`], call [`Self::record`]
    /// after the evaluation.
    pub fn begin_batch(&mut self, num_rows: usize) -> GateDecision {
        // Only a filter with dynamic filters that are not complete can
        // change. When nothing changed, this is one atomic load for each
        // such dynamic filter.
        if let Some(tracker) = self.tracking.watcher()
            && tracker.changed()
        {
            let generation = tracker.generation_tag();
            self.last_generation = generation;
            self.state = GateState::new_window();
            self.probing = false;
            self.backoff = self.config.initial_pause_batches;
            self.site.reset_for_generation(generation);
        }

        match &mut self.state {
            GateState::Paused { remaining_batches } => {
                // Only count down. Counters and backoff change only at
                // decision points.
                *remaining_batches = remaining_batches.saturating_sub(1);
                if *remaining_batches == 0 {
                    // The next batch is a probe.
                    self.state = GateState::new_window();
                    self.probing = true;
                }
                self.rows_skipped += num_rows;
                self.awaiting_record = false;
                GateDecision::Skip
            }
            GateState::Evaluate { .. } => {
                self.batches_evaluated += 1;
                self.awaiting_record = true;
                GateDecision::Evaluate
            }
        }
    }

    /// Records the result of an evaluation that [`Self::begin_batch`]
    /// requested. `rows_in` is the number of rows evaluated, `rows_out`
    /// the number of rows that passed (a null result does not pass) and
    /// `elapsed` the evaluation time.
    ///
    /// Calls without a matching `begin_batch` that returned
    /// [`GateDecision::Evaluate`] are ignored.
    pub fn record(&mut self, rows_in: usize, rows_out: usize, elapsed: Duration) {
        if !std::mem::take(&mut self.awaiting_record) {
            return;
        }
        let GateState::Evaluate {
            window,
            batches_in_window,
        } = &mut self.state
        else {
            return;
        };
        window.add(rows_in as u64, rows_out as u64, duration_nanos(elapsed));
        *batches_in_window += 1;
        if *batches_in_window >= self.config.sample_batches {
            let window = *window;
            self.decide(window);
        }
    }

    /// Evaluates the filter on `batch`, or skips it.
    ///
    /// Returns `None` if the gate skipped the filter: all rows pass.
    /// Otherwise returns the result of the filter. A null value in the
    /// result means that the row does not pass.
    pub fn evaluate(&mut self, batch: &RecordBatch) -> Result<Option<BooleanArray>> {
        let num_rows = batch.num_rows();
        if self.begin_batch(num_rows) == GateDecision::Skip {
            return Ok(None);
        }
        let start = self.clock.now_nanos();
        let result = self.filter.evaluate(batch)?.into_array(num_rows)?;
        let result = as_boolean_array(&result)?.clone();
        let elapsed = self.clock.now_nanos().saturating_sub(start);
        // `true_count` does not count nulls.
        self.record(num_rows, result.true_count(), Duration::from_nanos(elapsed));
        Ok(Some(result))
    }

    /// Number of rows in the batches that the gate skipped.
    pub fn rows_skipped(&self) -> usize {
        self.rows_skipped
    }

    /// Number of times the gate paused the filter, including a paused start
    /// from the pooled verdict.
    pub fn pauses(&self) -> usize {
        self.pauses
    }

    /// Number of batches for which the gate requested an evaluation.
    pub fn batches_evaluated(&self) -> usize {
        self.batches_evaluated
    }

    /// True if the gate skips the next batch, unless the filter changes
    /// before it.
    pub fn is_paused(&self) -> bool {
        matches!(self.state, GateState::Paused { .. })
    }

    /// Length, in batches, of the next pause.
    pub fn next_pause_batches(&self) -> usize {
        self.backoff
    }

    /// Makes a decision at the end of a window.
    fn decide(&mut self, window: FilterCost) {
        let Some(pass_ratio) = window.pass_ratio() else {
            // No rows, thus no information. Start a new window.
            self.state = GateState::new_window();
            return;
        };
        let verdict = if self.should_pause(pass_ratio, &window) {
            let pause_batches = self.backoff;
            self.start_pause(pause_batches);
            PackedVerdict::new(self.last_generation, true, pause_batches)
        } else {
            self.state = GateState::new_window();
            self.probing = false;
            self.backoff = self.config.initial_pause_batches;
            PackedVerdict::new(self.last_generation, false, self.backoff)
        };
        self.site.publish(verdict, window);
    }

    /// The decision rule, see the [module documentation](self).
    fn should_pause(&self, pass_ratio: f64, window: &FilterCost) -> bool {
        // The cheap check first: the filter removes too few rows.
        if pass_ratio > self.config.max_pass_ratio {
            return true;
        }
        // The cost check: the filter costs more than it saves.
        let cost_ns = window.nanos as f64;
        let saving_ns = window.rows_removed() as f64 * self.saving_ns_per_row();
        if self.probing {
            // Turn the filter on again only if it is clearly worth its cost.
            cost_ns >= saving_ns * RESUME_COST_MARGIN
        } else {
            cost_ns > saving_ns * PAUSE_COST_MARGIN
        }
    }

    /// Goes to `Paused` for `pause_batches` batches and doubles the backoff.
    fn start_pause(&mut self, pause_batches: usize) {
        let pause_batches = pause_batches.max(1);
        self.probing = false;
        self.state = GateState::Paused {
            remaining_batches: pause_batches,
        };
        self.backoff = pause_batches
            .saturating_mul(2)
            .min(self.config.max_pause_batches);
        self.pauses += 1;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::expressions::{BinaryExpr, Column, DynamicFilterPhysicalExpr, col, lit};
    use crate::filter_stats::ManualClock;
    use arrow::array::{Array, Int32Array};
    use arrow::datatypes::{DataType, Field, Schema};
    use datafusion_expr::Operator;

    const ROWS: usize = 1000;

    fn static_filter() -> Arc<dyn PhysicalExpr> {
        lit(true)
    }

    /// A gate with the default configuration and a clock that does not
    /// move: the evaluation time is 0, thus only the pass ratio check can
    /// pause the filter.
    fn gate_with(
        filter: Arc<dyn PhysicalExpr>,
        site: &Arc<OptionalFilterSiteStats>,
    ) -> OptionalFilterGate {
        OptionalFilterGate::new(
            filter,
            Arc::clone(site),
            OptionalFilterGateConfig::default(),
        )
        .with_clock(Arc::new(ManualClock::new()))
    }

    fn new_gate() -> OptionalFilterGate {
        gate_with(static_filter(), &Arc::new(OptionalFilterSiteStats::new()))
    }

    /// Feeds one batch with the given pass ratio and an evaluation time of
    /// 0. Returns the decision.
    fn feed(gate: &mut OptionalFilterGate, pass_ratio: f64) -> GateDecision {
        feed_timed(gate, pass_ratio, 0.0)
    }

    /// Feeds one batch with the given pass ratio and an evaluation time of
    /// `ns_per_row` for each row. Returns the decision.
    fn feed_timed(
        gate: &mut OptionalFilterGate,
        pass_ratio: f64,
        ns_per_row: f64,
    ) -> GateDecision {
        let decision = gate.begin_batch(ROWS);
        if decision == GateDecision::Evaluate {
            let elapsed = Duration::from_nanos((ROWS as f64 * ns_per_row) as u64);
            gate.record(ROWS, (ROWS as f64 * pass_ratio) as usize, elapsed);
        }
        decision
    }

    /// Feeds `n` batches and returns how many the gate evaluated.
    fn feed_n(gate: &mut OptionalFilterGate, n: usize, pass_ratio: f64) -> usize {
        (0..n)
            .filter(|_| feed(gate, pass_ratio) == GateDecision::Evaluate)
            .count()
    }

    /// Feeds batches until the gate evaluates one. Returns the number of
    /// skipped batches.
    fn skip_until_probe(gate: &mut OptionalFilterGate, pass_ratio: f64) -> usize {
        let mut skipped = 0;
        while feed(gate, pass_ratio) == GateDecision::Skip {
            skipped += 1;
            assert!(skipped < 10_000, "gate never probes");
        }
        skipped
    }

    #[test]
    fn pauses_non_selective_filter() {
        let mut gate = new_gate();
        assert_eq!(feed(&mut gate, 0.95), GateDecision::Evaluate);
        assert!(!gate.is_paused());
        assert_eq!(feed(&mut gate, 0.95), GateDecision::Evaluate);
        assert!(gate.is_paused());
        assert_eq!(gate.pauses(), 1);

        // Pauses for `initial_pause_batches` batches.
        for _ in 0..4 {
            assert_eq!(feed(&mut gate, 0.95), GateDecision::Skip);
        }
        assert_eq!(gate.rows_skipped(), 4 * ROWS);
        assert_eq!(gate.batches_evaluated(), 2);
        // Then it probes.
        assert_eq!(feed(&mut gate, 0.95), GateDecision::Evaluate);
    }

    #[test]
    fn keeps_selective_filter() {
        let mut gate = new_gate();
        assert_eq!(feed_n(&mut gate, 100, 0.5), 100);
        assert_eq!(gate.pauses(), 0);
        assert_eq!(gate.rows_skipped(), 0);
        // A pass ratio equal to the maximum does not pause.
        assert_eq!(feed_n(&mut gate, 10, 0.8), 10);
        assert_eq!(gate.pauses(), 0);
    }

    #[test]
    fn backoff_doubles_up_to_cap_and_resets() {
        let mut gate = new_gate();
        let mut observed = vec![];
        assert_eq!(feed_n(&mut gate, 2, 1.0), 2);
        for _ in 0..6 {
            observed.push(skip_until_probe(&mut gate, 1.0));
            // `skip_until_probe` evaluated the first batch of the probe
            // window. One more batch closes the window.
            assert_eq!(feed(&mut gate, 1.0), GateDecision::Evaluate);
        }
        assert_eq!(observed, vec![4, 8, 16, 32, 32, 32]);
        assert_eq!(gate.pauses(), 7);

        // A selective probe resets the backoff.
        assert!(gate.is_paused());
        let skipped = skip_until_probe(&mut gate, 0.1);
        assert_eq!(skipped, 32);
        assert_eq!(feed(&mut gate, 0.1), GateDecision::Evaluate);
        assert!(!gate.is_paused());
        assert_eq!(gate.next_pause_batches(), 4);

        // The next pause is short again.
        assert_eq!(feed_n(&mut gate, 2, 1.0), 2);
        assert_eq!(skip_until_probe(&mut gate, 1.0), 4);
    }

    fn dynamic_filter() -> (Arc<DynamicFilterPhysicalExpr>, Arc<dyn PhysicalExpr>) {
        let column: Arc<dyn PhysicalExpr> = Arc::new(Column::new("a", 0));
        let dynamic = Arc::new(DynamicFilterPhysicalExpr::new(vec![column], lit(true)));
        let filter = Arc::clone(&dynamic) as Arc<dyn PhysicalExpr>;
        (dynamic, filter)
    }

    fn a_gt(value: i32) -> Arc<dyn PhysicalExpr> {
        Arc::new(BinaryExpr::new(
            Arc::new(Column::new("a", 0)),
            Operator::Gt,
            lit(value),
        ))
    }

    #[test]
    fn generation_change_restarts_evaluation() {
        let (dynamic, filter) = dynamic_filter();
        let site = Arc::new(OptionalFilterSiteStats::new());
        let mut gate = gate_with(filter, &site);

        // Pause two times: the backoff grows to 16.
        assert_eq!(feed_n(&mut gate, 2, 1.0), 2);
        skip_until_probe(&mut gate, 1.0);
        feed(&mut gate, 1.0);
        assert!(gate.is_paused());
        assert_eq!(gate.next_pause_batches(), 16);
        assert!(site.rows_in() > 0);

        // A new generation of the filter ends the pause at once.
        dynamic.update(a_gt(10)).unwrap();
        assert_eq!(feed(&mut gate, 1.0), GateDecision::Evaluate);
        assert!(!gate.is_paused());
        assert_eq!(gate.next_pause_batches(), 4);
        // The pooled stats were reset for the new generation.
        assert_eq!(site.rows_in(), 0);
        assert_eq!(site.rows_out(), 0);

        // The new window has an empty history: one more batch decides.
        assert_eq!(feed(&mut gate, 1.0), GateDecision::Evaluate);
        assert!(gate.is_paused());
        assert_eq!(site.rows_in(), 2 * ROWS as u64);
        assert_eq!(skip_until_probe(&mut gate, 1.0), 4);
    }

    #[test]
    fn static_filter_is_never_watched() {
        // A filter without dynamic filters, and a filter whose dynamic
        // filters are all complete, can not change: the gate never polls
        // them and never resets.
        let (dynamic, complete) = dynamic_filter();
        dynamic.mark_complete();
        for (filter, expected_complete) in [(static_filter(), false), (complete, true)] {
            let site = Arc::new(OptionalFilterSiteStats::new());
            let mut gate = gate_with(filter, &site);
            match (&gate.tracking, expected_complete) {
                (DynamicFilterTracking::Static, false)
                | (DynamicFilterTracking::AllComplete, true) => {}
                (other, _) => panic!("unexpected tracking {other:?}"),
            }
            assert!(gate.tracking.watcher().is_none());

            assert_eq!(feed_n(&mut gate, 2, 1.0), 2);
            assert!(gate.is_paused());
            assert_eq!(skip_until_probe(&mut gate, 1.0), 4);
            assert_eq!(feed(&mut gate, 1.0), GateDecision::Evaluate);
            assert_eq!(skip_until_probe(&mut gate, 1.0), 8);
            assert_eq!(gate.pauses(), 2);
        }
    }

    #[test]
    fn completed_filter_stops_being_watched() {
        let (dynamic, filter) = dynamic_filter();
        let site = Arc::new(OptionalFilterSiteStats::new());
        let mut gate = gate_with(Arc::clone(&filter), &site);
        assert!(gate.tracking.watcher().is_some());
        assert_eq!(feed_n(&mut gate, 2, 1.0), 2);
        assert!(gate.is_paused());

        // The final update restarts the evaluation one time.
        dynamic.update(a_gt(10)).unwrap();
        dynamic.mark_complete();
        assert_eq!(feed(&mut gate, 1.0), GateDecision::Evaluate);
        assert!(!gate.is_paused());
        assert_eq!(site.rows_in(), 0);
        // The tracker dropped the subscription of the complete filter.
        assert!(gate.tracking.watcher().unwrap().is_exhausted());

        // No more resets: the gate pauses and backs off as usual.
        assert_eq!(feed(&mut gate, 1.0), GateDecision::Evaluate);
        assert!(gate.is_paused());
        assert_eq!(skip_until_probe(&mut gate, 1.0), 4);
        assert_eq!(feed(&mut gate, 1.0), GateDecision::Evaluate);
        assert_eq!(skip_until_probe(&mut gate, 1.0), 8);

        // A gate created after completion does not watch the filter, and it
        // reuses the pooled verdict of the final generation.
        let seeded = gate_with(filter, &site);
        assert!(matches!(
            seeded.tracking,
            DynamicFilterTracking::AllComplete
        ));
        assert!(seeded.is_paused());
    }

    #[test]
    fn topk_like_filter_stays_on() {
        // The filter gets tighter over time and changes often, like a TopK
        // dynamic filter. At the start it removes almost no rows.
        let (dynamic, filter) = dynamic_filter();
        let site = Arc::new(OptionalFilterSiteStats::new());
        let mut gate = gate_with(filter, &site);
        for i in 0..100 {
            if i % 2 == 0 {
                dynamic.update(a_gt(i)).unwrap();
            }
            let pass_ratio = 1.0 - (i as f64 / 100.0);
            assert_eq!(
                feed(&mut gate, pass_ratio),
                GateDecision::Evaluate,
                "batch {i}"
            );
        }
        assert_eq!(gate.rows_skipped(), 0);
        assert_eq!(gate.batches_evaluated(), 100);
    }

    #[test]
    fn skewed_input_probe_re_enables_filter() {
        let mut gate = new_gate();
        // Non-selective data first: the gate pauses with growing backoff.
        assert_eq!(feed_n(&mut gate, 2, 1.0), 2);
        for _ in 0..3 {
            skip_until_probe(&mut gate, 1.0);
            feed(&mut gate, 1.0);
        }
        assert!(gate.is_paused());

        // Then the data becomes selective. A probe finds it.
        let skipped = skip_until_probe(&mut gate, 0.05);
        assert!(skipped <= 32);
        assert_eq!(feed(&mut gate, 0.05), GateDecision::Evaluate);
        assert!(!gate.is_paused());
        // The filter stays on for the rest of the input.
        assert_eq!(feed_n(&mut gate, 50, 0.05), 50);
    }

    #[test]
    fn pooled_verdict_seeds_new_gates() {
        let site = Arc::new(OptionalFilterSiteStats::new());
        let mut first = gate_with(static_filter(), &site);
        // A second gate created before the first one decides: it evaluates.
        let mut running = gate_with(static_filter(), &site);
        assert!(!running.is_paused());

        assert_eq!(feed_n(&mut first, 2, 1.0), 2);
        assert!(first.is_paused());
        assert_eq!(site.rows_in(), 2 * ROWS as u64);
        assert_eq!(site.rows_out(), 2 * ROWS as u64);

        // A gate created now starts paused, with the pooled pause length.
        let mut seeded = gate_with(static_filter(), &site);
        assert!(seeded.is_paused());
        assert_eq!(seeded.pauses(), 1);
        assert_eq!(skip_until_probe(&mut seeded, 1.0), 4);
        assert_eq!(seeded.next_pause_batches(), 8);

        // The running gate is not flipped by the pooled verdict.
        assert_eq!(feed(&mut running, 0.1), GateDecision::Evaluate);
        assert_eq!(feed(&mut running, 0.1), GateDecision::Evaluate);
        assert!(!running.is_paused());
        // Its selective verdict is now the pooled one: new gates evaluate.
        let fresh = gate_with(static_filter(), &site);
        assert!(!fresh.is_paused());
    }

    #[test]
    fn pooled_verdict_ignored_for_other_generation() {
        let (dynamic, filter) = dynamic_filter();
        let site = Arc::new(OptionalFilterSiteStats::new());
        let mut first = gate_with(Arc::clone(&filter), &site);
        assert_eq!(feed_n(&mut first, 2, 1.0), 2);
        assert!(first.is_paused());

        dynamic.update(a_gt(5)).unwrap();
        let second = gate_with(filter, &site);
        assert!(!second.is_paused());
    }

    /// Regression test: a paused gate must not change its backoff or its
    /// counters for each skipped batch. Only decisions change them.
    #[test]
    fn paused_gate_does_not_grow_backoff_while_skipping() {
        let site = Arc::new(OptionalFilterSiteStats::new());
        let mut gate = gate_with(static_filter(), &site);
        assert_eq!(feed_n(&mut gate, 2, 1.0), 2);
        assert!(gate.is_paused());
        let backoff = gate.next_pause_batches();
        let pauses = gate.pauses();
        let evaluated = gate.batches_evaluated();
        let (site_in, site_out) = (site.rows_in(), site.rows_out());

        for _ in 0..3 {
            assert_eq!(gate.begin_batch(ROWS), GateDecision::Skip);
            // A stray `record` during a pause is ignored.
            gate.record(ROWS, 0, Duration::from_secs(1));
            assert_eq!(gate.next_pause_batches(), backoff);
            assert_eq!(gate.pauses(), pauses);
            assert_eq!(gate.batches_evaluated(), evaluated);
            assert_eq!((site.rows_in(), site.rows_out()), (site_in, site_out));
        }
        assert_eq!(gate.begin_batch(ROWS), GateDecision::Skip);
        assert_eq!(gate.next_pause_batches(), backoff);
        // The pause is over: the next batch is evaluated.
        assert_eq!(gate.begin_batch(ROWS), GateDecision::Evaluate);
    }

    #[test]
    fn empty_window_makes_no_decision() {
        let mut gate = new_gate();
        for _ in 0..10 {
            assert_eq!(gate.begin_batch(0), GateDecision::Evaluate);
            gate.record(0, 0, Duration::from_micros(1));
        }
        assert_eq!(gate.pauses(), 0);
    }

    fn batch(values: Vec<Option<i32>>) -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![Field::new("a", DataType::Int32, true)]));
        RecordBatch::try_new(schema, vec![Arc::new(Int32Array::from(values))]).unwrap()
    }

    fn col_gt(value: i32) -> Arc<dyn PhysicalExpr> {
        let schema = Schema::new(vec![Field::new("a", DataType::Int32, true)]);
        Arc::new(BinaryExpr::new(
            col("a", &schema).unwrap(),
            Operator::Gt,
            lit(value),
        ))
    }

    #[test]
    fn evaluate_selective_filter() {
        let site = Arc::new(OptionalFilterSiteStats::new());
        let mut gate = gate_with(col_gt(8), &site);
        let input = batch((0..10).map(Some).collect());
        for _ in 0..10 {
            let result = gate.evaluate(&input).unwrap().expect("evaluated");
            assert_eq!(result.true_count(), 1);
        }
        assert_eq!(gate.batches_evaluated(), 10);
        assert_eq!(gate.pauses(), 0);
        // The pooled stats hold all windows of the generation.
        assert_eq!(site.rows_in(), 100);
        assert_eq!(site.rows_out(), 10);
    }

    #[test]
    fn evaluate_non_selective_filter_skips() {
        let site = Arc::new(OptionalFilterSiteStats::new());
        let mut gate = gate_with(col_gt(0), &site);
        let input = batch((1..=10).map(Some).collect());
        assert!(gate.evaluate(&input).unwrap().is_some());
        assert!(gate.evaluate(&input).unwrap().is_some());
        for _ in 0..4 {
            assert!(gate.evaluate(&input).unwrap().is_none());
        }
        assert_eq!(gate.rows_skipped(), 40);
        assert!(gate.evaluate(&input).unwrap().is_some());
    }

    #[test]
    fn evaluate_counts_null_as_not_passing() {
        let site = Arc::new(OptionalFilterSiteStats::new());
        let mut gate = gate_with(col_gt(0), &site);
        // 9 of 10 rows are null: the filter is selective.
        let mut values = vec![None; 9];
        values.push(Some(5));
        let input = batch(values);
        for _ in 0..10 {
            let result = gate.evaluate(&input).unwrap().expect("evaluated");
            assert_eq!(result.null_count(), 9);
        }
        assert_eq!(gate.pauses(), 0);
        assert_eq!(site.rows_in(), 100);
        assert_eq!(site.rows_out(), 10);
    }

    #[test]
    fn config_from_execution_options() {
        let mut options = ExecutionOptions::default();
        assert_eq!(
            OptionalFilterGateConfig::from(&options),
            OptionalFilterGateConfig::default()
        );
        options.optional_filter_max_pass_ratio = 0.5;
        options.optional_filter_min_saving_ns_per_row = 7.5;
        let config = OptionalFilterGateConfig::from(&options);
        assert_eq!(config.max_pass_ratio, 0.5);
        assert_eq!(config.min_saving_ns_per_row, 7.5);
        assert_eq!(config.sample_batches, 2);
    }

    // The tests below use the default `min_saving_ns_per_row` of 20 ns. For
    // a window of 1000-row batches with pass ratio `p` and cost `c` ns for
    // each row, the gate compares `c` with `(1 - p) * 20` ns for each row.

    /// Like the dynamic filter of a hash join with a multi-column key: it
    /// removes 93% of the rows, but costs 70 ns for each row. Each removed
    /// row saves only 20 ns, thus 18.6 ns for each evaluated row.
    #[test]
    fn selective_but_expensive_filter_pauses() {
        let site = Arc::new(OptionalFilterSiteStats::new());
        let mut gate = gate_with(static_filter(), &site);
        assert_eq!(feed_timed(&mut gate, 0.07, 70.0), GateDecision::Evaluate);
        assert_eq!(feed_timed(&mut gate, 0.07, 70.0), GateDecision::Evaluate);
        assert!(gate.is_paused());
        assert_eq!(gate.pauses(), 1);
        // The pooled statistics hold the time.
        assert_eq!(site.nanos(), 2 * 70 * ROWS as u64);
        assert_eq!(site.cost().nanos_per_row(), Some(70.0));

        // The probes find the same cost: the pauses get longer.
        assert_eq!(skip_until_probe(&mut gate, 0.07), 4);
        assert_eq!(feed_timed(&mut gate, 0.07, 70.0), GateDecision::Evaluate);
        // `skip_until_probe` evaluated the first batch with no time. The
        // window costs 70 µs and saves 1860 * 20 ns = 37.2 µs: still too
        // much.
        assert!(gate.is_paused());
        assert_eq!(skip_until_probe(&mut gate, 0.07), 8);

        // A new gate for the site starts paused.
        let seeded = gate_with(static_filter(), &site);
        assert!(seeded.is_paused());
    }

    /// A bound check that costs 2 ns for each row and removes 30% of the
    /// rows: it saves 6 ns for each row, thus it stays on.
    #[test]
    fn cheap_weakly_selective_filter_stays_on() {
        let mut gate = new_gate();
        for _ in 0..100 {
            assert_eq!(feed_timed(&mut gate, 0.7, 2.0), GateDecision::Evaluate);
        }
        assert_eq!(gate.pauses(), 0);
    }

    /// Like the dynamic filter of a TopK: a cheap comparison that removes
    /// almost all rows. It stays on.
    #[test]
    fn cheap_very_selective_filter_stays_on() {
        let mut gate = new_gate();
        for _ in 0..100 {
            assert_eq!(feed_timed(&mut gate, 0.001, 3.0), GateDecision::Evaluate);
        }
        assert_eq!(gate.pauses(), 0);
    }

    /// The pass ratio check comes first: a free filter that removes too few
    /// rows is paused.
    #[test]
    fn pass_ratio_check_pauses_cheap_non_selective_filter() {
        let mut gate = new_gate();
        assert_eq!(feed_timed(&mut gate, 0.9, 0.0), GateDecision::Evaluate);
        assert_eq!(feed_timed(&mut gate, 0.9, 0.0), GateDecision::Evaluate);
        assert!(gate.is_paused());
    }

    /// The consumer measures a larger saving (for example the Parquet scan
    /// measures the decode time of the columns that the filter does not
    /// read). The next probe turns the expensive filter on again.
    #[test]
    fn measured_saving_re_enables_filter_at_next_probe() {
        let saving = Arc::new(MeasuredRowSaving::new());
        let mut gate = new_gate().with_measured_saving(Arc::clone(&saving));
        assert_eq!(gate.saving_ns_per_row(), 20.0);
        assert_eq!(feed_timed(&mut gate, 0.07, 70.0), GateDecision::Evaluate);
        assert_eq!(feed_timed(&mut gate, 0.07, 70.0), GateDecision::Evaluate);
        assert!(gate.is_paused());

        // Each removed row now saves 20 + 80 = 100 ns: 93 ns for each
        // evaluated row, more than the cost of 70 ns with the margin.
        saving.set_ns_per_row(80.0);
        assert_eq!(gate.saving_ns_per_row(), 100.0);
        // The pause does not end early.
        for _ in 0..4 {
            assert_eq!(feed_timed(&mut gate, 0.07, 70.0), GateDecision::Skip);
        }
        assert_eq!(feed_timed(&mut gate, 0.07, 70.0), GateDecision::Evaluate);
        assert_eq!(feed_timed(&mut gate, 0.07, 70.0), GateDecision::Evaluate);
        assert!(!gate.is_paused());
        assert_eq!(gate.next_pause_batches(), 4);
        for _ in 0..20 {
            assert_eq!(feed_timed(&mut gate, 0.07, 70.0), GateDecision::Evaluate);
        }

        // Invalid measured values are used as 0.
        saving.set_ns_per_row(f64::NAN);
        assert_eq!(gate.saving_ns_per_row(), 20.0);
        saving.set_ns_per_row(-5.0);
        assert_eq!(gate.saving_ns_per_row(), 20.0);
    }

    /// When the cost is near the saving, the gate keeps its state: a running
    /// filter stays on and a paused filter stays paused.
    #[test]
    fn cost_check_has_hysteresis() {
        // The filter removes 50% of the rows: the saving is 10 ns for each
        // evaluated row. A cost of 10.5 ns is between 0.9 and 1.1 times the
        // saving.
        let mut gate = new_gate();
        for _ in 0..20 {
            assert_eq!(feed_timed(&mut gate, 0.5, 10.5), GateDecision::Evaluate);
        }
        assert!(!gate.is_paused());

        // Pause it with a larger cost.
        assert_eq!(feed_timed(&mut gate, 0.5, 30.0), GateDecision::Evaluate);
        assert_eq!(feed_timed(&mut gate, 0.5, 30.0), GateDecision::Evaluate);
        assert!(gate.is_paused());
        // A probe with the cost near the saving does not turn it on.
        for _ in 0..4 {
            assert_eq!(feed_timed(&mut gate, 0.5, 10.5), GateDecision::Skip);
        }
        assert_eq!(feed_timed(&mut gate, 0.5, 10.5), GateDecision::Evaluate);
        assert_eq!(feed_timed(&mut gate, 0.5, 10.5), GateDecision::Evaluate);
        assert!(gate.is_paused());
        assert_eq!(gate.pauses(), 2);
        // A probe with a clearly lower cost turns it on.
        for _ in 0..8 {
            assert_eq!(feed_timed(&mut gate, 0.5, 8.5), GateDecision::Skip);
        }
        assert_eq!(feed_timed(&mut gate, 0.5, 8.5), GateDecision::Evaluate);
        assert_eq!(feed_timed(&mut gate, 0.5, 8.5), GateDecision::Evaluate);
        assert!(!gate.is_paused());
    }

    /// A clock that moves by a fixed step each time it is read.
    #[derive(Debug)]
    struct SteppingClock {
        now: AtomicU64,
        step: u64,
    }

    impl Clock for SteppingClock {
        fn now_nanos(&self) -> u64 {
            self.now.fetch_add(self.step, Ordering::Relaxed)
        }
    }

    /// `evaluate` measures the time with the clock of the gate.
    #[test]
    fn evaluate_measures_time_with_gate_clock() {
        let site = Arc::new(OptionalFilterSiteStats::new());
        // Each evaluation takes 10 µs for 10 rows: 1000 ns for each row.
        let clock = Arc::new(SteppingClock {
            now: AtomicU64::new(0),
            step: 10_000,
        });
        let mut gate = OptionalFilterGate::new(
            col_gt(8),
            Arc::clone(&site),
            OptionalFilterGateConfig::default(),
        )
        .with_clock(clock);
        let input = batch((0..10).map(Some).collect());
        // The filter removes 90% of the rows, but it costs 1000 ns for each
        // row, and each removed row saves 20 ns.
        assert!(gate.evaluate(&input).unwrap().is_some());
        assert!(gate.evaluate(&input).unwrap().is_some());
        assert!(gate.is_paused());
        assert_eq!(site.nanos(), 20_000);
        assert!(gate.evaluate(&input).unwrap().is_none());
    }
}
