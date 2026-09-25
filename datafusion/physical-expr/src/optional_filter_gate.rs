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

//! A runtime gate that pauses optional filters that cost more than they
//! save.
//!
//! An *optional filter* is a filter that is not needed for correctness, for
//! example a dynamic filter that a hash join or a TopK pushes down into a
//! scan. An operator can skip such a filter and still produce correct
//! results. When the filter removes few rows, or when it is expensive (for
//! example a hash table lookup with many columns), the cost to evaluate it
//! can be larger than the benefit.
//!
//! [`OptionalFilterGate`] decides, batch by batch, if a stream evaluates the
//! filter or skips it. Each stream has its own gate. The gates of one plan
//! site (for example all files and partitions of one scan) can share their
//! pauses, see "Shared verdict".
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
//! rules is true:
//!
//! 1. The filter removed no rows in the window.
//! 2. The cost of the window (`cost_ns`) is larger than the work that the
//!    removed rows save (`saving_ns`):
//!
//!    ```text
//!    cost_ns   = evaluation time + rows_in * measured overhead
//!    saving_ns = (rows_in - rows_out) * saving_ns_per_row
//!    saving_ns_per_row = min_saving_ns_per_row + measured saving
//!    ```
//!
//!    `min_saving_ns_per_row` comes from the configuration. It is the work
//!    that a removed row saves after the filter, for example a hash table
//!    probe in a join. The *measured saving* and the *measured overhead* are
//!    optional: a consumer that can measure more work that a removed row
//!    saves (the Parquet scan measures the decode time of the columns that
//!    the filter does not read), or a fixed cost for each evaluated row in
//!    addition to the evaluation time (the Parquet scan has a cost for each
//!    row filter stage), gives them in a shared [`MeasuredRowSaving`] and
//!    updates them at any time.
//!
//!    To prevent a filter from switching on and off when the cost and the
//!    saving are almost equal, this rule has a margin: a running filter is
//!    paused only if `cost_ns > saving_ns * 1.1`, and a probe after a pause
//!    turns the filter on again only if `cost_ns < saving_ns * 0.9`.
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
//! [`DynamicFilterPhysicalExpr`]: crate::expressions::DynamicFilterPhysicalExpr
//! [`DynamicFilterTracker::changed`]: crate::expressions::DynamicFilterTracker::changed
//!
//! # Shared verdict
//!
//! Each gate pays for at least one window before its first decision. A scan
//! opens many files at the same time, and a filter that removes no rows
//! (for example a hash join filter on a column that has only matching
//! values) then costs one window in each file. Also each gate probes on its
//! own after each pause.
//!
//! Thus the gates of one plan site can share a [`SharedGateVerdict`] (see
//! [`OptionalFilterGate::with_shared_verdict`]). Each gate publishes its
//! pauses and the end of its pauses there. A gate that has no evidence of
//! its own that the filter is worth its cost (before its first decision,
//! after a change of the filter, and after a decision to pause) uses a
//! pause that another gate published after the last shared verdict that it
//! saw:
//!
//! * A new gate starts paused if the shared verdict is a pause.
//! * A gate in its first window, or in a probe window after a pause, stops
//!   the window and pauses. Thus after a pause of all gates, usually only
//!   the first gate at the end of its pause probes the filter.
//!
//! A gate that keeps the filter does not use the pauses of other gates:
//! with skewed data the filter can be worth its cost for some files only.
//! A gate that sees a change of the filter clears a shared pause, because
//! it was measured on the old filter, and starts again without evidence.

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use datafusion_common::config::ExecutionOptions;
use datafusion_physical_expr_common::physical_expr::PhysicalExpr;

use crate::expressions::DynamicFilterTracking;
use crate::filter_stats::{Clock, FilterCost, SystemClock, duration_nanos};

/// A running filter is paused by the cost rule only if its cost is larger
/// than this multiple of its saving. See the [module documentation](self).
const PAUSE_COST_MARGIN: f64 = 1.1;

/// A probe turns a paused filter on again (by the cost rule) only if its
/// cost is smaller than this multiple of its saving.
const RESUME_COST_MARGIN: f64 = 0.9;

/// Configuration of an [`OptionalFilterGate`].
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct OptionalFilterGateConfig {
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
            sample_batches: 2,
            initial_pause_batches: 4,
            max_pause_batches: 32,
            min_saving_ns_per_row: 20.0,
        }
    }
}

impl From<&ExecutionOptions> for OptionalFilterGateConfig {
    /// Uses `optional_filter_min_saving_ns_per_row` from `options`, and the
    /// default values for the other fields.
    fn from(options: &ExecutionOptions) -> Self {
        Self {
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

/// The terms of the cost rule that the consumer of an optional filter
/// measures:
///
/// * The *saving*: work, in nanoseconds, that each row removed by the filter
///   saves. The gate adds it to
///   [`OptionalFilterGateConfig::min_saving_ns_per_row`]. For example, the
///   Parquet scan sets it to the time to decode the columns that the filter
///   does not read, for each removed row that the decoder can skip.
/// * The *overhead*: work, in nanoseconds, that the consumer does for each
///   evaluated row because it evaluates the filter, in addition to the
///   evaluation time. The gate adds it to the cost of each window. For
///   example, the Parquet scan sets it to the fixed cost of a row filter
///   stage when the filter is a row filter predicate.
///
/// The consumer creates one value, gives a clone of the [`Arc`] to the gate
/// with [`OptionalFilterGate::with_measured_saving`], and updates it at any
/// time with [`Self::set_ns_per_row`] and [`Self::set_overhead_ns_per_row`].
/// The gate reads it at each decision.
///
/// Each value is an `f64` in an [`AtomicU64`], thus reads and updates are
/// cheap and lock-free.
#[derive(Debug, Default)]
pub struct MeasuredRowSaving {
    /// The bits of the `f64` saving for each removed row.
    ns_per_row_bits: AtomicU64,
    /// The bits of the `f64` overhead for each evaluated row.
    overhead_ns_per_row_bits: AtomicU64,
}

/// `value` if it is finite and not negative, else 0.
fn non_negative(value: f64) -> f64 {
    if value.is_finite() {
        value.max(0.0)
    } else {
        0.0
    }
}

impl MeasuredRowSaving {
    /// Creates a value of 0 ns.
    pub fn new() -> Self {
        Self::default()
    }

    /// Sets the measured saving for each removed row, in nanoseconds.
    /// Values that are negative or not finite are used as 0.
    pub fn set_ns_per_row(&self, ns_per_row: f64) {
        self.ns_per_row_bits
            .store(non_negative(ns_per_row).to_bits(), Ordering::Relaxed);
    }

    /// The measured saving for each removed row, in nanoseconds.
    pub fn ns_per_row(&self) -> f64 {
        f64::from_bits(self.ns_per_row_bits.load(Ordering::Relaxed))
    }

    /// Sets the measured overhead for each evaluated row, in nanoseconds.
    /// Values that are negative or not finite are used as 0.
    pub fn set_overhead_ns_per_row(&self, ns_per_row: f64) {
        self.overhead_ns_per_row_bits
            .store(non_negative(ns_per_row).to_bits(), Ordering::Relaxed);
    }

    /// The measured overhead for each evaluated row, in nanoseconds.
    pub fn overhead_ns_per_row(&self) -> f64 {
        f64::from_bits(self.overhead_ns_per_row_bits.load(Ordering::Relaxed))
    }
}

/// The last pause (or end of a pause) that the gates of one plan site
/// published, see "Shared verdict" in the [module documentation](self).
///
/// Create one value for each plan site (for example each optional filter of
/// one scan), and give a clone of the [`Arc`] to each gate of the site with
/// [`OptionalFilterGate::with_shared_verdict`].
///
/// The value is one [`AtomicU64`], thus it is lock-free. Gates write it only
/// at decisions that pause the filter or end a pause, and read it before a
/// batch only while they have no evidence of their own.
#[derive(Debug, Default)]
pub struct SharedGateVerdict {
    /// A [`Verdict`] and its sequence number, see [`Verdict::pack`].
    word: AtomicU64,
}

/// One published verdict of a [`SharedGateVerdict`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Verdict {
    /// The filter is not paused (or nothing was published yet).
    Keep,
    /// A gate paused the filter for this number of batches.
    Pause(usize),
}

impl Verdict {
    /// Largest pause length that fits in the packed word.
    const MAX_BATCHES: usize = u32::MAX as usize;

    /// Packs the verdict with the sequence number `seq`: the sequence
    /// number in the low 32 bits, and the pause length (0 for
    /// [`Verdict::Keep`]) in the high 32 bits.
    fn pack(self, seq: u32) -> u64 {
        let batches = match self {
            Self::Keep => 0,
            Self::Pause(batches) => batches.clamp(1, Self::MAX_BATCHES) as u64,
        };
        (batches << 32) | u64::from(seq)
    }

    /// The verdict and the sequence number in `word`.
    fn unpack(word: u64) -> (Self, u32) {
        let verdict = match (word >> 32) as usize {
            0 => Self::Keep,
            batches => Self::Pause(batches),
        };
        (verdict, word as u32)
    }
}

impl SharedGateVerdict {
    /// Creates a shared verdict without any published pause.
    pub fn new() -> Self {
        Self::default()
    }

    /// True if the last published verdict is a pause.
    pub fn is_paused(&self) -> bool {
        matches!(self.load().0, Verdict::Pause(_))
    }

    /// The current verdict and its sequence number.
    fn load(&self) -> (Verdict, u32) {
        Verdict::unpack(self.word.load(Ordering::Acquire))
    }

    /// Publishes `verdict` if `replace` returns true for the current
    /// verdict. Returns the sequence number of the published verdict.
    fn publish_if(
        &self,
        verdict: Verdict,
        replace: impl Fn(Verdict) -> bool,
    ) -> Option<u32> {
        self.word
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |word| {
                let (current, seq) = Verdict::unpack(word);
                replace(current).then(|| verdict.pack(seq.wrapping_add(1)))
            })
            .ok()
            .map(|previous| (previous as u32).wrapping_add(1))
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
/// for one stream only. Do not share it between streams.
///
/// Call [`Self::begin_batch`] before each batch. If it returns
/// [`GateDecision::Evaluate`], evaluate [`Self::filter`] on the batch and
/// then call [`Self::record`] with the row counts and the evaluation time.
/// Measure the time with [`Self::clock`], so that tests can replace it.
#[derive(Debug)]
pub struct OptionalFilterGate {
    filter: Arc<dyn PhysicalExpr>,
    /// The dynamic filters in `filter` that can still change.
    tracking: DynamicFilterTracking,
    config: OptionalFilterGateConfig,
    /// The clock that consumers use to measure the evaluation time.
    clock: Arc<dyn Clock>,
    /// The saving that the consumer measures, added to
    /// `config.min_saving_ns_per_row`.
    measured_saving: Option<Arc<MeasuredRowSaving>>,
    state: GateState,
    /// True while the current window is a probe after a pause. The cost
    /// rule then uses [`RESUME_COST_MARGIN`].
    probing: bool,
    /// Length of the next pause, in batches.
    backoff: usize,
    /// True after `begin_batch` returned `Evaluate` and before `record`.
    awaiting_record: bool,
    pauses: usize,
    /// The verdict that this gate shares with the other gates of its plan
    /// site, if any.
    shared: Option<Arc<SharedGateVerdict>>,
    /// Sequence number of the last shared verdict that this gate published
    /// or saw.
    shared_seq: u32,
    /// True while this gate has no evidence of its own that the filter is
    /// worth its cost: before its first decision, after a change of the
    /// filter, and after a decision that paused the filter. Then it uses
    /// the shared pauses of the other gates.
    uses_shared_pauses: bool,
}

impl OptionalFilterGate {
    /// Creates a gate for `filter`, a boolean expression. The gate starts to
    /// evaluate the filter.
    ///
    /// The gate uses a [`SystemClock`] and no measured saving. See
    /// [`Self::with_clock`] and [`Self::with_measured_saving`].
    ///
    /// This walks `filter` one time to find its dynamic filters.
    pub fn new(filter: Arc<dyn PhysicalExpr>, config: OptionalFilterGateConfig) -> Self {
        let config = config.normalized();
        let tracking = DynamicFilterTracking::classify(&filter);
        Self {
            filter,
            tracking,
            config,
            clock: SystemClock::shared(),
            measured_saving: None,
            state: GateState::new_window(),
            probing: false,
            backoff: config.initial_pause_batches,
            awaiting_record: false,
            pauses: 0,
            shared: None,
            shared_seq: 0,
            uses_shared_pauses: true,
        }
    }

    /// Shares the pauses of this gate with the other gates of the same plan
    /// site, see "Shared verdict" in the [module documentation](self). If
    /// the shared verdict is a pause, the gate starts paused.
    pub fn with_shared_verdict(mut self, shared: Arc<SharedGateVerdict>) -> Self {
        let (verdict, seq) = shared.load();
        self.shared_seq = seq;
        if let Verdict::Pause(batches) = verdict {
            self.start_pause(batches);
        }
        self.shared = Some(shared);
        self
    }

    /// Uses `clock` as the clock of this gate, see [`Self::clock`].
    pub fn with_clock(mut self, clock: Arc<dyn Clock>) -> Self {
        self.clock = clock;
        self
    }

    /// Adds the saving of `saving` to
    /// [`OptionalFilterGateConfig::min_saving_ns_per_row`] and its overhead
    /// to the cost at each decision. See [`MeasuredRowSaving`].
    pub fn with_measured_saving(mut self, saving: Arc<MeasuredRowSaving>) -> Self {
        self.measured_saving = Some(saving);
        self
    }

    /// The filter of this gate.
    pub fn filter(&self) -> &Arc<dyn PhysicalExpr> {
        &self.filter
    }

    /// The clock to measure the evaluation time that is given to
    /// [`Self::record`].
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

    /// The work, in nanoseconds for each evaluated row, that the gate adds
    /// to the evaluation time now: the measured overhead.
    pub fn overhead_ns_per_row(&self) -> f64 {
        self.measured_saving
            .as_ref()
            .map_or(0.0, |saving| saving.overhead_ns_per_row())
    }

    /// Call before each batch. Returns if the caller must evaluate the
    /// filter on the batch or skip it.
    ///
    /// If the result is [`GateDecision::Evaluate`], call [`Self::record`]
    /// after the evaluation.
    pub fn begin_batch(&mut self) -> GateDecision {
        // Only a filter with dynamic filters that are not complete can
        // change. When nothing changed, this is one atomic load for each
        // such dynamic filter.
        if let Some(tracker) = self.tracking.watcher()
            && tracker.changed()
        {
            self.state = GateState::new_window();
            self.probing = false;
            self.backoff = self.config.initial_pause_batches;
            self.uses_shared_pauses = true;
            if let Some(shared) = &self.shared {
                // A shared pause was measured on the old filter.
                let paused = |verdict| matches!(verdict, Verdict::Pause(_));
                if let Some(seq) = shared.publish_if(Verdict::Keep, paused) {
                    self.shared_seq = seq;
                }
            }
        }
        if !self.is_paused() {
            self.use_shared_pause();
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
                self.awaiting_record = false;
                GateDecision::Skip
            }
            GateState::Evaluate { .. } => {
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

    /// Number of times the gate paused the filter.
    pub fn pauses(&self) -> usize {
        self.pauses
    }

    /// True if the gate skips the next batch, unless the filter changes
    /// before it.
    pub fn is_paused(&self) -> bool {
        matches!(self.state, GateState::Paused { .. })
    }

    /// Makes a decision at the end of a window.
    fn decide(&mut self, window: FilterCost) {
        if window.rows_in == 0 {
            // No rows, thus no information. Start a new window.
            self.state = GateState::new_window();
            return;
        }
        if self.should_pause(&window) {
            let batches = self.backoff;
            self.start_pause(batches);
            self.uses_shared_pauses = true;
            self.publish(Verdict::Pause(batches));
        } else {
            self.state = GateState::new_window();
            self.probing = false;
            self.backoff = self.config.initial_pause_batches;
            if std::mem::take(&mut self.uses_shared_pauses) {
                // The first decision, or the end of a pause.
                self.publish(Verdict::Keep);
            }
        }
    }

    /// Before a batch that the gate would evaluate: if this gate uses the
    /// shared pauses and another gate published a pause after the last
    /// shared verdict that this gate saw, pauses the filter for the same
    /// length. The current window is dropped.
    fn use_shared_pause(&mut self) {
        if !self.uses_shared_pauses {
            return;
        }
        let Some(shared) = &self.shared else {
            return;
        };
        let (verdict, seq) = shared.load();
        if seq == self.shared_seq {
            return;
        }
        self.shared_seq = seq;
        if let Verdict::Pause(batches) = verdict {
            self.start_pause(batches);
        }
    }

    /// Publishes `verdict` to the shared verdict, if any.
    fn publish(&mut self, verdict: Verdict) {
        if let Some(shared) = &self.shared {
            self.shared_seq = shared
                .publish_if(verdict, |_| true)
                .expect("an unconditional update always succeeds");
        }
    }

    /// The decision rules, see the [module documentation](self).
    fn should_pause(&self, window: &FilterCost) -> bool {
        let rows_removed = window.rows_removed();
        if rows_removed == 0 {
            return true;
        }
        // The filter costs more than it saves.
        let cost_ns =
            window.nanos as f64 + window.rows_in as f64 * self.overhead_ns_per_row();
        let saving_ns = rows_removed as f64 * self.saving_ns_per_row();
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
    use arrow::array::{Array, BooleanArray, Int32Array, RecordBatch};
    use arrow::datatypes::{DataType, Field, Schema};
    use datafusion_common::cast::as_boolean_array;
    use datafusion_expr::Operator;

    const ROWS: usize = 1000;

    fn static_filter() -> Arc<dyn PhysicalExpr> {
        lit(true)
    }

    /// A gate with the default configuration and a clock that does not
    /// move: the evaluation time is 0, thus only a window that removes no
    /// rows pauses the filter.
    fn gate_with(filter: Arc<dyn PhysicalExpr>) -> OptionalFilterGate {
        OptionalFilterGate::new(filter, OptionalFilterGateConfig::default())
            .with_clock(Arc::new(ManualClock::new()))
    }

    fn new_gate() -> OptionalFilterGate {
        gate_with(static_filter())
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
        let decision = gate.begin_batch();
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

    /// Evaluates the filter of `gate` on `batch` like a consumer does, or
    /// skips it. Returns `None` if the gate skipped the filter.
    fn evaluate(
        gate: &mut OptionalFilterGate,
        batch: &RecordBatch,
    ) -> Option<BooleanArray> {
        if gate.begin_batch() == GateDecision::Skip {
            return None;
        }
        let num_rows = batch.num_rows();
        let start = gate.clock().now_nanos();
        let result = gate
            .filter()
            .evaluate(batch)
            .unwrap()
            .into_array(num_rows)
            .unwrap();
        let result = as_boolean_array(&result).unwrap().clone();
        let elapsed = gate.clock().now_nanos().saturating_sub(start);
        // `true_count` does not count nulls.
        gate.record(num_rows, result.true_count(), Duration::from_nanos(elapsed));
        Some(result)
    }

    #[test]
    fn pauses_filter_that_removes_no_rows() {
        let mut gate = new_gate();
        assert_eq!(feed(&mut gate, 1.0), GateDecision::Evaluate);
        assert!(!gate.is_paused());
        assert_eq!(feed(&mut gate, 1.0), GateDecision::Evaluate);
        assert!(gate.is_paused());
        assert_eq!(gate.pauses(), 1);

        // Pauses for `initial_pause_batches` batches.
        for _ in 0..4 {
            assert_eq!(feed(&mut gate, 1.0), GateDecision::Skip);
        }
        // Then it probes.
        assert_eq!(feed(&mut gate, 1.0), GateDecision::Evaluate);
    }

    #[test]
    fn keeps_free_filter_that_removes_rows() {
        let mut gate = new_gate();
        assert_eq!(feed_n(&mut gate, 100, 0.5), 100);
        assert_eq!(gate.pauses(), 0);
        // A free filter that removes only 1% of the rows also stays on.
        assert_eq!(feed_n(&mut gate, 10, 0.99), 10);
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
        assert_eq!(gate.backoff, 4);

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
        let mut gate = gate_with(filter);

        // Pause two times: the backoff grows to 16.
        assert_eq!(feed_n(&mut gate, 2, 1.0), 2);
        skip_until_probe(&mut gate, 1.0);
        feed(&mut gate, 1.0);
        assert!(gate.is_paused());
        assert_eq!(gate.backoff, 16);

        // A new generation of the filter ends the pause at once.
        dynamic.update(a_gt(10)).unwrap();
        assert_eq!(feed(&mut gate, 1.0), GateDecision::Evaluate);
        assert!(!gate.is_paused());
        assert_eq!(gate.backoff, 4);

        // The new window has an empty history: one more batch decides.
        assert_eq!(feed(&mut gate, 1.0), GateDecision::Evaluate);
        assert!(gate.is_paused());
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
            let mut gate = gate_with(filter);
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
        let mut gate = gate_with(filter);
        assert!(gate.tracking.watcher().is_some());
        assert_eq!(feed_n(&mut gate, 2, 1.0), 2);
        assert!(gate.is_paused());

        // The final update restarts the evaluation one time.
        dynamic.update(a_gt(10)).unwrap();
        dynamic.mark_complete();
        assert_eq!(feed(&mut gate, 1.0), GateDecision::Evaluate);
        assert!(!gate.is_paused());
        // The tracker dropped the subscription of the complete filter.
        assert!(gate.tracking.watcher().unwrap().is_exhausted());

        // No more resets: the gate pauses and backs off as usual.
        assert_eq!(feed(&mut gate, 1.0), GateDecision::Evaluate);
        assert!(gate.is_paused());
        assert_eq!(skip_until_probe(&mut gate, 1.0), 4);
        assert_eq!(feed(&mut gate, 1.0), GateDecision::Evaluate);
        assert_eq!(skip_until_probe(&mut gate, 1.0), 8);
    }

    #[test]
    fn topk_like_filter_stays_on() {
        // The filter gets tighter over time and changes often, like a TopK
        // dynamic filter. At the start it removes almost no rows.
        let (dynamic, filter) = dynamic_filter();
        let mut gate = gate_with(filter);
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
        assert_eq!(gate.pauses(), 0);
    }

    #[test]
    fn skewed_input_probe_re_enables_filter() {
        let mut gate = new_gate();
        // Data that the filter does not remove first: the gate pauses with
        // growing backoff.
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

    /// Regression test: a paused gate must not change its backoff or its
    /// counters for each skipped batch. Only decisions change them.
    #[test]
    fn paused_gate_does_not_grow_backoff_while_skipping() {
        let mut gate = new_gate();
        assert_eq!(feed_n(&mut gate, 2, 1.0), 2);
        assert!(gate.is_paused());
        let backoff = gate.backoff;
        let pauses = gate.pauses();

        for _ in 0..3 {
            assert_eq!(gate.begin_batch(), GateDecision::Skip);
            // A stray `record` during a pause is ignored.
            gate.record(ROWS, 0, Duration::from_secs(1));
            assert_eq!(gate.backoff, backoff);
            assert_eq!(gate.pauses(), pauses);
        }
        assert_eq!(gate.begin_batch(), GateDecision::Skip);
        assert_eq!(gate.backoff, backoff);
        // The pause is over: the next batch is evaluated.
        assert_eq!(gate.begin_batch(), GateDecision::Evaluate);
    }

    #[test]
    fn empty_window_makes_no_decision() {
        let mut gate = new_gate();
        for _ in 0..10 {
            assert_eq!(gate.begin_batch(), GateDecision::Evaluate);
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
        let mut gate = gate_with(col_gt(8));
        let input = batch((0..10).map(Some).collect());
        for _ in 0..10 {
            let result = evaluate(&mut gate, &input).expect("evaluated");
            assert_eq!(result.true_count(), 1);
        }
        assert_eq!(gate.pauses(), 0);
    }

    #[test]
    fn evaluate_filter_that_removes_no_rows_skips() {
        let mut gate = gate_with(col_gt(0));
        let input = batch((1..=10).map(Some).collect());
        assert!(evaluate(&mut gate, &input).is_some());
        assert!(evaluate(&mut gate, &input).is_some());
        for _ in 0..4 {
            assert!(evaluate(&mut gate, &input).is_none());
        }
        assert!(evaluate(&mut gate, &input).is_some());
    }

    #[test]
    fn evaluate_counts_null_as_not_passing() {
        let mut gate = gate_with(col_gt(0));
        // 9 of 10 rows are null: the filter removes them.
        let mut values = vec![None; 9];
        values.push(Some(5));
        let input = batch(values);
        for _ in 0..10 {
            let result = evaluate(&mut gate, &input).expect("evaluated");
            assert_eq!(result.null_count(), 9);
        }
        assert_eq!(gate.pauses(), 0);
    }

    #[test]
    fn config_from_execution_options() {
        let mut options = ExecutionOptions::default();
        assert_eq!(
            OptionalFilterGateConfig::from(&options),
            OptionalFilterGateConfig::default()
        );
        options.optional_filter_min_saving_ns_per_row = 7.5;
        let config = OptionalFilterGateConfig::from(&options);
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
        let mut gate = new_gate();
        assert_eq!(feed_timed(&mut gate, 0.07, 70.0), GateDecision::Evaluate);
        assert_eq!(feed_timed(&mut gate, 0.07, 70.0), GateDecision::Evaluate);
        assert!(gate.is_paused());
        assert_eq!(gate.pauses(), 1);

        // The probes find the same cost: the pauses get longer.
        assert_eq!(skip_until_probe(&mut gate, 0.07), 4);
        assert_eq!(feed_timed(&mut gate, 0.07, 70.0), GateDecision::Evaluate);
        // `skip_until_probe` evaluated the first batch with no time. The
        // window costs 70 µs and saves 1860 * 20 ns = 37.2 µs: still too
        // much.
        assert!(gate.is_paused());
        assert_eq!(skip_until_probe(&mut gate, 0.07), 8);
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

    /// Cheap bounds that cost 1 ns for each row and remove 12% of the rows:
    /// they save 2.4 ns for each row, thus they stay on. There is no
    /// separate rule for the fraction of rows that pass.
    #[test]
    fn cheap_bounds_that_remove_few_rows_stay_on() {
        let mut gate = new_gate();
        for _ in 0..100 {
            assert_eq!(feed_timed(&mut gate, 0.88, 1.0), GateDecision::Evaluate);
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
        assert_eq!(gate.backoff, 4);
        for _ in 0..20 {
            assert_eq!(feed_timed(&mut gate, 0.07, 70.0), GateDecision::Evaluate);
        }

        // Invalid measured values are used as 0.
        saving.set_ns_per_row(f64::NAN);
        assert_eq!(gate.saving_ns_per_row(), 20.0);
        saving.set_ns_per_row(-5.0);
        assert_eq!(gate.saving_ns_per_row(), 20.0);
    }

    /// The consumer measures an overhead for each evaluated row (for example
    /// the fixed cost of a Parquet row filter stage). A filter that is worth
    /// its evaluation time alone is paused when the overhead is added.
    #[test]
    fn measured_overhead_adds_to_cost() {
        let saving = Arc::new(MeasuredRowSaving::new());
        let mut gate = new_gate().with_measured_saving(Arc::clone(&saving));
        // Removes 50% of the rows: saves 10 ns for each evaluated row. The
        // evaluation costs 5 ns for each row.
        for _ in 0..10 {
            assert_eq!(feed_timed(&mut gate, 0.5, 5.0), GateDecision::Evaluate);
        }
        assert!(!gate.is_paused());

        // With 8 ns of overhead, the cost is 13 ns for each row.
        saving.set_overhead_ns_per_row(8.0);
        assert_eq!(gate.overhead_ns_per_row(), 8.0);
        assert_eq!(feed_timed(&mut gate, 0.5, 5.0), GateDecision::Evaluate);
        assert_eq!(feed_timed(&mut gate, 0.5, 5.0), GateDecision::Evaluate);
        assert!(gate.is_paused());

        // Invalid measured values are used as 0.
        saving.set_overhead_ns_per_row(f64::INFINITY);
        assert_eq!(gate.overhead_ns_per_row(), 0.0);
        saving.set_overhead_ns_per_row(-1.0);
        assert_eq!(gate.overhead_ns_per_row(), 0.0);
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

    fn shared_gate(
        filter: Arc<dyn PhysicalExpr>,
        shared: &Arc<SharedGateVerdict>,
    ) -> OptionalFilterGate {
        gate_with(filter).with_shared_verdict(Arc::clone(shared))
    }

    #[test]
    fn verdict_pack_round_trip() {
        for verdict in [Verdict::Keep, Verdict::Pause(1), Verdict::Pause(32)] {
            assert_eq!(Verdict::unpack(verdict.pack(7)), (verdict, 7));
            assert_eq!(Verdict::unpack(verdict.pack(u32::MAX)), (verdict, u32::MAX));
        }
        // Pause lengths are at least 1 and at most `MAX_BATCHES`.
        assert_eq!(
            Verdict::unpack(Verdict::Pause(0).pack(1)).0,
            Verdict::Pause(1)
        );
        assert_eq!(
            Verdict::unpack(Verdict::Pause(usize::MAX).pack(1)).0,
            Verdict::Pause(Verdict::MAX_BATCHES)
        );
    }

    /// A new gate starts from the shared pause of the other gates.
    #[test]
    fn new_gate_starts_with_shared_pause() {
        let shared = Arc::new(SharedGateVerdict::new());
        let mut first = shared_gate(static_filter(), &shared);
        assert!(!first.is_paused());
        assert_eq!(feed_n(&mut first, 2, 1.0), 2);
        assert!(first.is_paused());
        assert!(shared.is_paused());

        // A new gate starts paused for the same length, then probes.
        let mut second = shared_gate(static_filter(), &shared);
        assert!(second.is_paused());
        assert_eq!(second.pauses(), 1);
        assert_eq!(skip_until_probe(&mut second, 0.5), 4);
        // The probe keeps the filter: new gates evaluate again.
        assert_eq!(feed(&mut second, 0.5), GateDecision::Evaluate);
        assert!(!second.is_paused());
        assert!(!shared.is_paused());
        assert!(!shared_gate(static_filter(), &shared).is_paused());
    }

    /// Gates that start at the same time: a gate in its first window uses
    /// the pause that another gate published, instead of the rest of its
    /// own window, and only one gate probes after the pause.
    #[test]
    fn first_window_and_probe_use_shared_pause() {
        let shared = Arc::new(SharedGateVerdict::new());
        let mut gates: Vec<_> = (0..4)
            .map(|_| shared_gate(static_filter(), &shared))
            .collect();
        // All gates evaluate their first batch.
        for gate in &mut gates {
            assert_eq!(feed(gate, 1.0), GateDecision::Evaluate);
        }
        // The first gate completes its window and pauses.
        assert_eq!(feed(&mut gates[0], 1.0), GateDecision::Evaluate);
        assert!(gates[0].is_paused());
        // The others do not evaluate the second batch of their window.
        for gate in &mut gates[1..] {
            assert_eq!(feed(gate, 1.0), GateDecision::Skip);
            assert!(gate.is_paused());
        }

        // Only the first gate probes at the end of its pause. The others
        // use its verdict: a pause of 8 batches.
        assert_eq!(skip_until_probe(&mut gates[0], 1.0), 4);
        assert_eq!(feed(&mut gates[0], 1.0), GateDecision::Evaluate);
        assert!(gates[0].is_paused());
        for gate in &mut gates[1..] {
            let skipped = (0..20)
                .take_while(|_| feed(gate, 1.0) == GateDecision::Skip)
                .count();
            // 3 more batches of the first pause, then 8.
            assert_eq!(skipped, 3 + 8);
            assert_eq!(gate.pauses(), 2);
        }
    }

    /// A gate that keeps the filter does not use the pauses of other gates
    /// (skewed data).
    #[test]
    fn running_gate_ignores_shared_pause() {
        let shared = Arc::new(SharedGateVerdict::new());
        let mut selective = shared_gate(static_filter(), &shared);
        assert_eq!(feed_n(&mut selective, 2, 0.1), 2);
        assert!(!selective.is_paused());

        let mut other = shared_gate(static_filter(), &shared);
        assert_eq!(feed_n(&mut other, 2, 1.0), 2);
        assert!(other.is_paused());
        assert!(shared.is_paused());

        assert_eq!(feed_n(&mut selective, 20, 0.1), 20);
        assert_eq!(selective.pauses(), 0);
    }

    /// A change of the filter clears the shared pause: it was measured on
    /// the old filter.
    #[test]
    fn change_clears_shared_pause() {
        let (dynamic, filter) = dynamic_filter();
        let shared = Arc::new(SharedGateVerdict::new());
        let mut gate = shared_gate(Arc::clone(&filter), &shared);
        assert_eq!(feed_n(&mut gate, 2, 1.0), 2);
        assert!(shared.is_paused());

        dynamic.update(a_gt(1)).unwrap();
        // The gate sees the change at its next batch.
        assert_eq!(feed(&mut gate, 0.5), GateDecision::Evaluate);
        assert!(!shared.is_paused());
        assert!(!shared_gate(filter, &shared).is_paused());
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

    /// A consumer measures the time with the clock of the gate.
    #[test]
    fn consumer_measures_time_with_gate_clock() {
        // Each evaluation takes 10 µs for 10 rows: 1000 ns for each row.
        let clock = Arc::new(SteppingClock {
            now: AtomicU64::new(0),
            step: 10_000,
        });
        let mut gate =
            OptionalFilterGate::new(col_gt(8), OptionalFilterGateConfig::default())
                .with_clock(clock);
        let input = batch((0..10).map(Some).collect());
        // The filter removes 90% of the rows, but it costs 1000 ns for each
        // row, and each removed row saves 20 ns.
        assert!(evaluate(&mut gate, &input).is_some());
        assert!(evaluate(&mut gate, &input).is_some());
        assert!(gate.is_paused());
        assert!(evaluate(&mut gate, &input).is_none());
    }
}
