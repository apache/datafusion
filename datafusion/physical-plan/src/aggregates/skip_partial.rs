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

//! Cost-aware partial-aggregation skip probe.
//!
//! Rebased from #22518.
//!
//! The classic partial-aggregation skip check is a single fixed ratio:
//! if `num_groups / input_rows` exceeds a threshold (default `0.8`)
//! after the first `probe_rows_threshold` rows, skip further partial
//! aggregation. That catches the no-reduction case but misses the
//! medium-ratio band (~0.5-0.7) where partial aggregation is still
//! net-negative because the per-row cost is high (variable-length
//! keys, complex aggregates, ...).
//!
//! [`SkipAggregationProbe`] extends the check with a short A/B
//! sampling window that measures both the partial-agg per-row cost
//! and the passthrough per-row cost on real input, then makes a
//! cost-aware skip decision:
//!
//! ```text
//! skip <=> ratio > passthrough_ns_per_row / partial_ns_per_row
//! ```
//!
//! Derived from `cost_keep_partial = partial * N + final * N * ratio`
//! vs `cost_skip = passthrough * N + final * N`, assuming
//! `final ~= partial` (same hash-table mechanics).
//!
//! Set `skip_partial_aggregation_use_cost_model = false` to fall back
//! to the plain ratio check.

use arrow::record_batch::RecordBatch;

use crate::metrics;
use crate::metrics::{MetricBuilder, MetricCategory};

/// Three phases of the cost-aware skip decision.
///
/// 1. [`ProbePhase::Partial`] — accumulate input through the hash
///    table (normal partial-agg path), measuring `partial_ns/row` and
///    the `num_groups/input_rows` ratio over the first
///    `probe_rows_threshold` rows.
/// 2. [`ProbePhase::AbSampling`] — route the next `ab_sampling_rows`
///    of input through the passthrough path (`transform_to_states`)
///    to measure `passthrough_ns/row`. The hash table built so far is
///    kept; nothing is emitted yet.
/// 3. [`ProbePhase::Locked`] — final decision. Skip when
///    `ratio > passthrough_ns/row / partial_ns/row` (the cost-aware
///    crossover); otherwise revert to partial agg for the rest of the
///    stream.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ProbePhase {
    Partial,
    AbSampling,
    Locked { should_skip: bool },
}

/// Tracks if the aggregate should skip partial aggregations.
///
/// See "partial aggregation" discussion on
/// [`crate::aggregates::grouped_hash_stream::GroupedHashAggregateStream`].
///
/// The probe runs a short A/B sampling window that measures both the
/// partial-agg per-row cost and the passthrough per-row cost on real
/// input, then makes a cost-based skip decision without relying on a
/// hardcoded ratio cutoff. `use_cost_model = false` falls back to the
/// original behaviour: a single bare ratio check at probe close.
pub(super) struct SkipAggregationProbe {
    // ========================================================================
    // PROPERTIES (immutable for the stream's lifetime)
    // ========================================================================
    probe_rows_threshold: usize,
    probe_ratio_threshold: f64,
    use_cost_model: bool,
    ab_sampling_rows: usize,

    // ========================================================================
    // STATE
    // ========================================================================
    phase: ProbePhase,
    /// Rows processed in the `Partial` phase.
    input_rows: usize,
    /// Latest `group_values.len()` reported in the `Partial` phase.
    num_groups: usize,
    /// Rows processed in the `AbSampling` phase.
    ab_rows: usize,
    /// `elapsed_compute.value()` snapshot at probe construction.
    elapsed_compute_at_probe_start: usize,
    /// `elapsed_compute.value()` snapshot at the `Partial`→`AbSampling`
    /// transition. `None` before that transition.
    elapsed_compute_at_ab_start: Option<usize>,

    /// Convenience mirror of `phase == Locked { should_skip: true }`.
    /// Kept so the hot path (`should_skip()`) is a single field load.
    should_skip: bool,
    /// Convenience mirror of `matches!(phase, Locked { .. })`. Once
    /// locked the probe stops observing.
    is_locked: bool,

    // ========================================================================
    // METRICS / DIAGNOSTICS
    // ========================================================================
    /// Number of rows emitted directly (skipping partial agg).
    skipped_aggregation_rows: metrics::Count,

    /// Shared `elapsed_compute` gauge — used to derive per-row cost.
    elapsed_compute: metrics::Time,

    /// `ExecutionPlanMetricsSet` for lazily registering diagnostic gauges
    /// on the first partial batch that reaches this probe.
    agg_metrics: metrics::ExecutionPlanMetricsSet,
    /// Partition id — used when the gauges are registered.
    partition: usize,

    // Lazily-registered diagnostic gauges. Registered on the first call
    // to `observe_partial_batch` so small queries that never hit the
    // partial probe don't have to display `...=0` noise.
    probe_partial_ns_per_row: Option<metrics::Gauge>,
    probe_passthrough_ns_per_row: Option<metrics::Gauge>,
    probe_ratio_per_mille: Option<metrics::Gauge>,
    probe_cost_decision_skip: Option<metrics::Gauge>,
}

impl SkipAggregationProbe {
    #[expect(clippy::too_many_arguments)]
    pub(super) fn new(
        probe_rows_threshold: usize,
        probe_ratio_threshold: f64,
        use_cost_model: bool,
        ab_sampling_rows: usize,
        skipped_aggregation_rows: metrics::Count,
        elapsed_compute: metrics::Time,
        agg_metrics: metrics::ExecutionPlanMetricsSet,
        partition: usize,
    ) -> Self {
        let elapsed_compute_at_probe_start = elapsed_compute.value();
        Self {
            probe_rows_threshold,
            probe_ratio_threshold,
            use_cost_model,
            ab_sampling_rows,
            phase: ProbePhase::Partial,
            input_rows: 0,
            num_groups: 0,
            ab_rows: 0,
            elapsed_compute_at_probe_start,
            elapsed_compute_at_ab_start: None,
            should_skip: false,
            is_locked: false,
            skipped_aggregation_rows,
            elapsed_compute,
            agg_metrics,
            partition,
            probe_partial_ns_per_row: None,
            probe_passthrough_ns_per_row: None,
            probe_ratio_per_mille: None,
            probe_cost_decision_skip: None,
        }
    }

    /// Lazily register the diagnostic gauges on first partial-probe
    /// observation. Doing this in `new()` would attach `...=0` noise
    /// to every aggregation, even ones that finish before the probe
    /// window closes.
    fn ensure_probe_gauges(&mut self) {
        if self.probe_partial_ns_per_row.is_some() {
            return;
        }
        let partition = self.partition;
        let register = |name: &'static str| -> metrics::Gauge {
            MetricBuilder::new(&self.agg_metrics)
                .with_category(MetricCategory::Rows)
                .gauge(name, partition)
        };
        self.probe_partial_ns_per_row =
            Some(register("partial_agg_probe_partial_ns_per_row"));
        self.probe_passthrough_ns_per_row =
            Some(register("partial_agg_probe_passthrough_ns_per_row"));
        self.probe_ratio_per_mille =
            Some(register("partial_agg_probe_ratio_per_mille"));
        self.probe_cost_decision_skip =
            Some(register("partial_agg_probe_cost_decision_skip"));
    }

    /// Observe a partial-agg batch. Accumulates `input_rows` / tracks
    /// `num_groups`, and when the `probe_rows_threshold` is reached,
    /// drives the phase transition.
    ///
    /// Replaces the pre-#22518 `update_state`. When
    /// `use_cost_model == false`, behaviour is identical to that
    /// original ratio check.
    pub(super) fn observe_partial_batch(
        &mut self,
        input_rows: usize,
        num_groups: usize,
    ) {
        if self.phase != ProbePhase::Partial {
            return;
        }
        self.ensure_probe_gauges();

        self.input_rows += input_rows;
        self.num_groups = num_groups;
        if self.input_rows < self.probe_rows_threshold {
            return;
        }

        // Probe window closed — publish the observed ratio.
        let ratio = if self.input_rows == 0 {
            0.0
        } else {
            self.num_groups as f64 / self.input_rows as f64
        };
        if let Some(g) = self.probe_ratio_per_mille.as_ref() {
            g.set((ratio * 1000.0) as usize);
        }

        // Rule 1: bare-ratio short-circuit. Preserved so obvious
        // no-reduction cases don't burn the A/B window.
        if ratio >= self.probe_ratio_threshold {
            self.commit_skip();
            return;
        }

        // With the cost model disabled we replicate the original
        // behaviour: don't lock the phase — subsequent batches may push
        // the ratio over the threshold and trigger `commit_skip` above.
        // The probe naturally locks itself once `should_skip` becomes
        // true; until then, keep re-checking on every partial batch.
        if !self.use_cost_model {
            return;
        }

        // Record `partial_ns/row` for the closed-form cost comparison.
        let elapsed_now = self.elapsed_compute.value();
        let partial_elapsed =
            elapsed_now.saturating_sub(self.elapsed_compute_at_probe_start);
        let partial_ns_per_row = if self.input_rows == 0 {
            0
        } else {
            partial_elapsed / self.input_rows
        };
        if let Some(g) = self.probe_partial_ns_per_row.as_ref() {
            g.set(partial_ns_per_row);
        }

        // Transition to A/B. The stream will now route input through
        // the passthrough path until `ab_sampling_rows` have been
        // observed, at which point `finalize_ab_decision` runs the
        // cost-based comparison.
        self.elapsed_compute_at_ab_start = Some(elapsed_now);
        self.phase = ProbePhase::AbSampling;
    }

    /// `true` iff the stream should route the next batch through
    /// `transform_to_states` so the probe can measure
    /// `passthrough_ns/row`.
    pub(super) fn wants_passthrough_sample(&self) -> bool {
        matches!(self.phase, ProbePhase::AbSampling)
    }

    /// Observe a passthrough (A/B-sampling) batch. Accumulates
    /// `ab_rows`, and when the window is full, triggers the
    /// cost-aware decision.
    pub(super) fn observe_ab_batch(&mut self, input_rows: usize) {
        if self.phase != ProbePhase::AbSampling {
            return;
        }
        self.ab_rows += input_rows;
        if self.ab_rows >= self.ab_sampling_rows {
            self.finalize_ab_decision();
        }
    }

    /// Compare the two measured per-row costs and lock the phase to
    /// either "skip partial" or "keep partial". Fires the diagnostic
    /// gauges before locking. If either measurement is 0 (extremely
    /// small window or degenerate input) we default to keeping
    /// partial.
    fn finalize_ab_decision(&mut self) {
        // Gauges were registered when we entered the partial probe;
        // they should exist here (we reached the partial threshold
        // before transitioning to `AbSampling`).
        let ab_start = self
            .elapsed_compute_at_ab_start
            .expect("A/B start snapshot must be set when entering AbSampling");
        let ab_elapsed = self
            .elapsed_compute
            .value()
            .saturating_sub(ab_start);
        let passthrough_ns_per_row = if self.ab_rows == 0 {
            0
        } else {
            ab_elapsed / self.ab_rows
        };
        if let Some(g) = self.probe_passthrough_ns_per_row.as_ref() {
            g.set(passthrough_ns_per_row);
        }

        let partial_ns_per_row = self
            .probe_partial_ns_per_row
            .as_ref()
            .map(|g| g.value())
            .unwrap_or(0);
        let ratio = if self.input_rows == 0 {
            0.0
        } else {
            self.num_groups as f64 / self.input_rows as f64
        };
        // skip <=> ratio > passthrough / partial. Fall back to "keep"
        // if we don't have both measurements — otherwise a degenerate
        // 0-ns partial would always tip the decision toward skip.
        let should_skip = partial_ns_per_row > 0
            && passthrough_ns_per_row > 0
            && ratio * (partial_ns_per_row as f64) > passthrough_ns_per_row as f64;

        if let Some(g) = self.probe_cost_decision_skip.as_ref() {
            g.set(if should_skip { 1 } else { 0 });
        }
        if should_skip {
            self.commit_skip();
        } else {
            self.phase = ProbePhase::Locked { should_skip: false };
            self.is_locked = true;
        }
    }

    /// Transition to the terminal `Locked { should_skip: true }`
    /// state. Used by both Rule 1 (fixed-ratio short-circuit) and the
    /// cost-aware path so the rest of the operator can rely on a
    /// single `should_skip` flag.
    fn commit_skip(&mut self) {
        self.should_skip = true;
        self.is_locked = true;
        self.phase = ProbePhase::Locked { should_skip: true };
    }

    pub(super) fn should_skip(&self) -> bool {
        self.should_skip
    }

    /// `true` once the probe has made its terminal decision (either
    /// via the bare-ratio short-circuit or the cost-aware A/B window).
    /// Used by the stream to distinguish "still measuring in A/B" from
    /// "decision finalised, revert to partial".
    pub(super) fn is_locked(&self) -> bool {
        self.is_locked
    }

    /// Record the number of rows that were output directly without
    /// aggregation.
    pub(super) fn record_skipped(&mut self, batch: &RecordBatch) {
        self.skipped_aggregation_rows.add(batch.num_rows());
    }
}
