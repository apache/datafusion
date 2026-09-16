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

//! Runtime-adaptive evaluation of a conjunctive (`AND`) predicate in
//! [`FilterExec`](crate::filter::FilterExec).
//!
//! Evaluation order matters: a selective conjunct run first gates the work of
//! the conjuncts after it. Two mechanisms already order and gate conjuncts
//! before this module sees them, and both decide statically:
//!
//! - the logical optimizer's `reorder_predicates` pass sorts the conjuncts
//!   cheap-before-expensive by a static cost class
//!   (<https://github.com/apache/datafusion/pull/22343>). It is blind to
//!   selectivity, so a cheap-but-unselective conjunct still sorts ahead of an
//!   expensive-but-very-selective one, and conjuncts in the same cost class
//!   keep the order they were written in. That is the order this module calls
//!   the *written order* and measures against.
//! - [`BinaryExpr`]'s `AND` pre-selects: when the conjuncts evaluated so far
//!   keep at most 20% of the rows and produce no nulls, it filters the batch
//!   down to those rows before evaluating the next conjunct. It can only gate
//!   a conjunct on the conjuncts written *before* it, never on a more
//!   selective one written after it.
//!
//! This module measures each conjunct's selectivity and cost at runtime and
//! reorders them accordingly, so that pre-selection fires on the conjunct that
//! discards the most rows. Whether it runs at all is controlled by
//! `datafusion.execution.adaptive_filter_reordering`. For example:
//!
//! ```sql
//! WHERE regexp_like(s,'a') AND regexp_like(s,'b') AND regexp_like(s,'rare')
//! ```
//!
//! All three conjuncts are equally expensive to the static cost class, so they
//! reach `FilterExec` as written, and the first two each keep most rows, so
//! `AND` pre-selection never fires. Once the warm-up has measured the three,
//! the selective one is promoted and the batch is compacted behind it:
//!
//! ```text
//! before:  regexp_like(s,'a')     evaluated on every row
//!          regexp_like(s,'b')     evaluated on every row
//!          regexp_like(s,'rare')  evaluated on every row
//!
//! after:   regexp_like(s,'rare')  every row, keeps ~1% -> batch compacted
//!          regexp_like(s,'a')     evaluated on those survivors only
//!          regexp_like(s,'b')     evaluated on those survivors only
//! ```
//!
//! This module contains no evaluation logic of its own. While the order is
//! being learned, the written predicate is handed to [`BinaryExpr`] with every
//! conjunct *leaf* wrapped in a [`MeasuredConjunct`] and the `AND` tree left
//! exactly as written; `BinaryExpr` evaluates and pre-selects as it would for
//! the plain predicate, so each conjunct is measured on the population it
//! would really see in that position.
//!
//! If the warm-up finds nothing materially better, the written predicate is
//! handed back as written — the same `Arc`, not an equivalent rebuilt from its
//! conjuncts. Reassociating an `AND` tree changes where pre-selection fires
//! even when the conjunct sequence is unchanged, so rebuilding it would let
//! the flag alter evaluation, and the side effects of fallible conjuncts with
//! it, without any reorder having been adopted.
//!
//! An adopted reorder *is* materialised, as a right-nested `AND` chain,
//! `(c_first AND (c_second AND (... AND c_last)))`. Right-nesting is what makes
//! it pay: pre-selection filters the batch an `AND` is handed before evaluating
//! its right-hand side, so the survivors of the first conjunct stay compacted
//! for the rest of the chain, where a left-nested chain — what
//! [`conjunction`](datafusion_physical_expr::utils::conjunction) builds — would
//! re-filter the original batch at every level.
//!
//! The ranking key is rows discarded per nanosecond
//! ([`effectiveness`](ConjunctStats::effectiveness)), and the ranking is
//! adopted only if it is materially cheaper than the written order
//! ([`TIE_COST_FRACTION`]), so a conjunction that does not benefit carries none
//! of this machinery past the warm-up. The decision then stays fixed.
//!
//! Cost is counted the way `AND` actually behaves rather than by pass rate
//! alone: a conjunct shortens the work after it only on a batch where
//! `BinaryExpr` pre-selects, which it does only when the conjunct produced no
//! nulls and kept at most
//! [`PRE_SELECTION_THRESHOLD`](datafusion_physical_expr::expressions::PRE_SELECTION_THRESHOLD)
//! of the rows. Conjuncts keeping 30% and 90% therefore both leave the next
//! one facing the whole batch and are credited alike, and a conjunct that
//! looks selective only because it produced nulls is credited with nothing.
//! Which of these happened is recorded per batch through
//! [`and_rhs_evaluation`], the same function `BinaryExpr` decides by, so the
//! model cannot drift away from the behaviour it models.
//!
//! A `FilterExec` is split across many partition streams, each seeing only a
//! slice of the data, so measurements are pooled into a shared
//! [`AdaptiveFilterShared`] and the streams learn as one: the first stream with
//! enough samples settles the order for all of them, and the rest adopt it on
//! their next batch instead of each re-paying the warm-up. Only unsettled
//! streams take the shared lock.
//!
//! ## Known limitations
//!
//! - Results never change (a conjunction's value does not depend on evaluation
//!   order), but the side effects of fallible predicates can, in either
//!   direction: a conjunct evaluated after a pre-selection sees only the rows
//!   that survived, so an error the written order raises can disappear and one
//!   it avoided can appear. Volatile predicates are never reordered.
//! - Measurements are conditional on the written order and, after a
//!   pre-selection, taken on small batches whose per-row cost is inflated by
//!   fixed overheads. Correlated conjuncts can be misjudged; the material-win
//!   guard only makes adoption conservative.
//! - A conjunct is only ever observed where it was written, so whether it
//!   would pre-select somewhere else in the order is projected from what it
//!   did in its own position, not measured.
//! - The decision is one-shot: a misjudged reorder, or drifting data, is kept
//!   for the rest of the query.
//! - The pooled state lives on the `FilterExec` node, not on one execution of
//!   it. Executing the same plan again, or concurrently, reuses the earlier
//!   measurements and the settled decision; only
//!   [`reset_state`](crate::ExecutionPlan::reset_state) — which the execution
//!   API does not promise to call — starts over. Results are unaffected, but a
//!   second run can differ from the first in speed, in `adaptive_reorders`,
//!   and in the side effects of a fallible conjunct.
//!
//! See <https://github.com/apache/datafusion/pull/22698>.

use std::fmt;
use std::fmt::Formatter;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::atomic::{AtomicU64, Ordering::Relaxed};

use crate::metrics::Count;
use arrow::array::{Array, ArrayRef};
use arrow::datatypes::{DataType, Schema};
use arrow::record_batch::RecordBatch;
use datafusion_common::Result;
use datafusion_common::cast::as_boolean_array;
use datafusion_common::instant::Instant;
use datafusion_expr::{ColumnarValue, Operator};
use datafusion_physical_expr::PhysicalExpr;
use datafusion_physical_expr::expressions::{
    AndRhsEvaluation, BinaryExpr, and_rhs_evaluation,
};
use datafusion_physical_expr::utils::split_conjunction;
use datafusion_physical_expr_common::physical_expr::is_volatile;

/// Batches measured before the order is settled.
const WARMUP_BATCHES: u64 = 8;

/// A candidate order is adopted only if its expected cost is below
/// `(1 - TIE_COST_FRACTION)` of the written order's.
const TIE_COST_FRACTION: f64 = 0.05;

/// Per-conjunct counts over the warm-up, on exactly the rows that reached it.
///
/// Besides the totals, the counts are split by what [`BinaryExpr`]'s `AND`
/// actually did with the conjuncts *after* this one on each batch
/// ([`and_rhs_evaluation`]), because that — not the raw pass rate — is what
/// decides how much work this conjunct saves them. See
/// [`downstream_weight`](Self::downstream_weight).
#[derive(Debug, Default, Clone)]
struct ConjunctStats {
    /// Total rows the conjunct was evaluated on.
    rows: u64,
    /// Rows that passed (non-null `true`, matching SQL filter semantics).
    matched: u64,
    /// Total evaluation time, nanoseconds.
    nanos: u64,
    /// Of `rows`, those in batches where the result let `AND` pre-select: the
    /// conjuncts after this one saw only the matching rows.
    gated_rows: u64,
    /// Of `gated_rows`, the rows that passed — what the conjuncts after this
    /// one were actually handed.
    gated_matched: u64,
    /// Of `rows`, those in batches where the result was all `false`: the
    /// conjuncts after this one were not evaluated at all.
    skipped_rows: u64,
}

impl ConjunctStats {
    /// Counts for a conjunct that saw `rows` rows across null-free batches
    /// that all had the same shape, keeping `matched` of them in `nanos`
    /// nanoseconds. Used by tests and by seeding; real measurements come from
    /// [`MeasuredConjunct`] batch by batch.
    #[cfg(test)]
    fn from_null_free_batches(rows: u64, matched: u64, nanos: u64) -> Self {
        let mut stats = Self {
            rows,
            matched,
            nanos,
            ..Default::default()
        };
        match and_rhs_evaluation(matched as usize, 0, rows as usize) {
            AndRhsEvaluation::Skipped => stats.skipped_rows = rows,
            AndRhsEvaluation::PreSelected => {
                stats.gated_rows = rows;
                stats.gated_matched = matched;
            }
            AndRhsEvaluation::FullBatch => {}
        }
        stats
    }

    /// Pool another stream's counts into this one.
    fn merge(&mut self, other: &Self) {
        self.rows += other.rows;
        self.matched += other.matched;
        self.nanos += other.nanos;
        self.gated_rows += other.gated_rows;
        self.gated_matched += other.gated_matched;
        self.skipped_rows += other.skipped_rows;
    }

    /// Per-row cost in nanoseconds, or `None` if never evaluated. Time is
    /// clamped to 1ns so "too cheap to measure" ranks as very cheap.
    fn cost_per_row(&self) -> Option<f64> {
        (self.rows > 0).then(|| self.nanos.max(1) as f64 / self.rows as f64)
    }

    /// Rows the conjuncts after this one were handed, per row this one saw.
    ///
    /// This is *not* the pass rate. `AND` only narrows what follows when it
    /// pre-selects, which it does on a null-free batch keeping at most
    /// [`PRE_SELECTION_THRESHOLD`] of the rows; otherwise the conjuncts after
    /// it see the whole batch however many rows this one rejected. So a
    /// conjunct keeping 30% and one keeping 90% both leave a weight of 1, and
    /// a conjunct that looks selective only because it produced nulls also
    /// leaves 1, since nulls disable pre-selection entirely.
    ///
    /// Unmeasured conjuncts weigh 1: they are assumed to narrow nothing.
    ///
    /// [`PRE_SELECTION_THRESHOLD`]: datafusion_physical_expr::expressions::PRE_SELECTION_THRESHOLD
    fn downstream_weight(&self) -> f64 {
        if self.rows == 0 {
            return 1.0;
        }
        // Batches that pre-selected pass on their matching rows; batches that
        // skipped pass on nothing; the rest pass on everything they saw.
        let full_batch_rows = self
            .rows
            .saturating_sub(self.gated_rows)
            .saturating_sub(self.skipped_rows);
        (self.gated_matched + full_batch_rows) as f64 / self.rows as f64
    }

    /// Ranking key: rows discarded per nanosecond, `(1 + rows_in - rows_out) /
    /// time` — the reciprocal of the score Velox sorts its filters by
    /// (<https://www.vldb.org/pvldb/vol15/p3372-pedreira.pdf>), so maximising it
    /// minimises time per discarded row. `None` when unmeasured, so such
    /// conjuncts sort last.
    fn effectiveness(&self) -> Option<f64> {
        (self.rows > 0)
            .then(|| (1 + self.rows - self.matched) as f64 / self.nanos.max(1) as f64)
    }
}

/// Measurements pooled across the partition streams of one `FilterExec`, and
/// the decision the first stream to fill the warm-up makes for all of them.
#[derive(Debug, Default)]
pub(crate) struct AdaptiveFilterShared {
    inner: Mutex<SharedInner>,
}

#[derive(Debug, Default)]
struct SharedInner {
    /// Pooled per-conjunct counts, sized by the first measured batch.
    stats: Vec<ConjunctStats>,
    /// Measured batches contributed by all streams so far.
    measured_batches: u64,
    /// The settled decision, once made; `None` while learning.
    settled: Option<Settled>,
}

/// The settled outcome of the warm-up.
#[derive(Debug, Clone)]
struct Settled {
    /// The settled order as a right-nested `AND` chain.
    predicate: Arc<dyn PhysicalExpr>,
    /// Whether that order reorders the written conjuncts.
    reordered: bool,
}

impl AdaptiveFilterShared {
    /// The settled decision, or `None` if the streams are still learning.
    #[cfg(test)]
    fn settled(&self) -> Option<Settled> {
        self.inner.lock().expect("poisoned").settled.clone()
    }

    /// Whether nothing has been measured or settled yet.
    #[cfg(test)]
    pub(crate) fn is_pristine(&self) -> bool {
        let inner = self.inner.lock().expect("poisoned");
        inner.stats.is_empty() && inner.measured_batches == 0 && inner.settled.is_none()
    }

    /// Seed `(rows, matched, nanos)` per conjunct one batch short of the
    /// warm-up, so the next measured batch settles on the seeded decision
    /// regardless of real timings.
    #[cfg(test)]
    pub(crate) fn seed_one_batch_short_of_warmup(
        &self,
        per_conjunct: &[(u64, u64, u64)],
    ) {
        let mut inner = self.inner.lock().expect("poisoned");
        inner.stats = per_conjunct
            .iter()
            .map(|&(rows, matched, nanos)| {
                ConjunctStats::from_null_free_batches(rows, matched, nanos)
            })
            .collect();
        inner.measured_batches = WARMUP_BATCHES - 1;
    }
}

/// A conjunct that records the rows it was handed, the rows it kept, the time
/// it took and what `AND` then did with the conjuncts after it, returning its
/// result unchanged (nulls included). Everything else delegates to the wrapped
/// conjunct.
#[derive(Debug)]
struct MeasuredConjunct {
    inner: Arc<dyn PhysicalExpr>,
    /// Rows handed to the conjunct since the last [`take`](Self::take).
    rows: AtomicU64,
    /// Of those, the non-null `true`s.
    matched: AtomicU64,
    /// Time spent inside the conjunct over those rows, in nanoseconds.
    nanos: AtomicU64,
    /// Rows in batches whose result let `AND` pre-select.
    gated_rows: AtomicU64,
    /// Of `gated_rows`, the rows that passed.
    gated_matched: AtomicU64,
    /// Rows in batches whose result was all `false`.
    skipped_rows: AtomicU64,
}

impl MeasuredConjunct {
    fn new(inner: Arc<dyn PhysicalExpr>) -> Self {
        Self {
            inner,
            rows: AtomicU64::new(0),
            matched: AtomicU64::new(0),
            nanos: AtomicU64::new(0),
            gated_rows: AtomicU64::new(0),
            gated_matched: AtomicU64::new(0),
            skipped_rows: AtomicU64::new(0),
        }
    }

    /// Drain the counters (per stream and uncontended, hence `Relaxed`).
    fn take(&self) -> ConjunctStats {
        ConjunctStats {
            rows: self.rows.swap(0, Relaxed),
            matched: self.matched.swap(0, Relaxed),
            nanos: self.nanos.swap(0, Relaxed),
            gated_rows: self.gated_rows.swap(0, Relaxed),
            gated_matched: self.gated_matched.swap(0, Relaxed),
            skipped_rows: self.skipped_rows.swap(0, Relaxed),
        }
    }
}

impl PartialEq for MeasuredConjunct {
    fn eq(&self, other: &Self) -> bool {
        self.inner.eq(&other.inner)
    }
}

impl Eq for MeasuredConjunct {}

impl std::hash::Hash for MeasuredConjunct {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.inner.hash(state);
    }
}

impl fmt::Display for MeasuredConjunct {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.inner)
    }
}

impl PhysicalExpr for MeasuredConjunct {
    fn data_type(&self, input_schema: &Schema) -> Result<DataType> {
        self.inner.data_type(input_schema)
    }

    fn nullable(&self, input_schema: &Schema) -> Result<bool> {
        self.inner.nullable(input_schema)
    }

    fn evaluate(&self, batch: &RecordBatch) -> Result<ColumnarValue> {
        let rows = batch.num_rows();
        let timer = Instant::now();
        let array = self.inner.evaluate(batch)?.into_array(rows)?;
        let nanos = timer.elapsed().as_nanos() as u64;
        let bools = as_boolean_array(&array)?;
        let matched = bools.true_count() as u64;

        self.rows.fetch_add(rows as u64, Relaxed);
        self.matched.fetch_add(matched, Relaxed);
        self.nanos.fetch_add(nanos, Relaxed);

        // Record what this result lets `AND` do with the conjuncts after it,
        // by the same rule evaluation uses, rather than inferring it from the
        // pass rate afterwards.
        match and_rhs_evaluation(matched as usize, bools.null_count(), rows) {
            AndRhsEvaluation::Skipped => {
                self.skipped_rows.fetch_add(rows as u64, Relaxed);
            }
            AndRhsEvaluation::PreSelected => {
                self.gated_rows.fetch_add(rows as u64, Relaxed);
                self.gated_matched.fetch_add(matched, Relaxed);
            }
            AndRhsEvaluation::FullBatch => {}
        }

        Ok(ColumnarValue::Array(array))
    }

    fn children(&self) -> Vec<&Arc<dyn PhysicalExpr>> {
        vec![&self.inner]
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn PhysicalExpr>>,
    ) -> Result<Arc<dyn PhysicalExpr>> {
        Ok(Arc::new(Self::new(Arc::clone(&children[0]))))
    }

    fn fmt_sql(&self, f: &mut Formatter<'_>) -> fmt::Result {
        self.inner.fmt_sql(f)
    }
}

/// Adaptive evaluator for a single conjunctive predicate, owned per partition
/// stream. Measurements are pooled into the shared [`AdaptiveFilterShared`];
/// the per-stream state is just the chain this stream currently evaluates.
#[derive(Debug)]
pub(crate) struct AdaptiveConjunction {
    /// The predicate exactly as written, kept so that a warm-up that finds
    /// nothing better hands back the very tree `FilterExec` would have run.
    written: Arc<dyn PhysicalExpr>,
    /// The split conjuncts, in written order.
    conjuncts: Vec<Arc<dyn PhysicalExpr>>,
    /// Measurements and the settled decision, shared by every partition stream.
    shared: Arc<AdaptiveFilterShared>,
    /// The written tree with every conjunct leaf wrapped in a
    /// [`MeasuredConjunct`] — same shape, same evaluation, plus counters.
    warmup_predicate: Arc<dyn PhysicalExpr>,
    /// The wrappers inside `warmup_predicate`, in written order.
    measured: Vec<Arc<MeasuredConjunct>>,
    /// The settled order as a right-nested `AND` chain; the warm-up chain until then.
    settled_predicate: Arc<dyn PhysicalExpr>,
    /// Whether the settled decision reordered the conjuncts.
    reordered: bool,
    /// Whether the order is settled: this stream no longer measures.
    settled: bool,
    /// Incremented once, if and when this stream adopts a *reordered* decision.
    adaptive_reorders: Option<Count>,
}

impl AdaptiveConjunction {
    /// Whether `predicate` has at least two `AND` conjuncts, none volatile.
    /// (Whether the feature is enabled is the caller's business.)
    pub(crate) fn applies(predicate: &Arc<dyn PhysicalExpr>) -> bool {
        let conjuncts = split_conjunction(predicate);
        conjuncts.len() >= 2 && !conjuncts.iter().any(|c| is_volatile(c))
    }

    /// `None` if adaptive reordering does not [apply](Self::applies).
    /// `adaptive_reorders` is bumped if this stream adopts a reorder.
    pub(crate) fn try_new(
        predicate: &Arc<dyn PhysicalExpr>,
        shared: Arc<AdaptiveFilterShared>,
        adaptive_reorders: Option<Count>,
    ) -> Option<Self> {
        if !Self::applies(predicate) {
            return None;
        }
        let conjuncts: Vec<Arc<dyn PhysicalExpr>> = split_conjunction(predicate)
            .into_iter()
            .map(Arc::clone)
            .collect();
        let mut measured = Vec::with_capacity(conjuncts.len());
        let warmup_predicate = wrap_conjuncts_in_place(predicate, &mut measured);
        debug_assert_eq!(measured.len(), conjuncts.len());
        Some(Self {
            written: Arc::clone(predicate),
            conjuncts,
            shared,
            settled_predicate: Arc::clone(&warmup_predicate),
            warmup_predicate,
            measured,
            reordered: false,
            settled: false,
            adaptive_reorders,
        })
    }

    /// The boolean mask of rows passing every conjunct. Until the order
    /// settles, each batch is measured and its counts pooled.
    pub(crate) fn evaluate(&mut self, batch: &RecordBatch) -> Result<ArrayRef> {
        if self.settled {
            return self.evaluate_settled(batch);
        }

        // Empty batches measure nothing and must not consume the warm-up.
        if batch.num_rows() == 0 {
            let mask = self.evaluate_warmup(batch)?;
            self.take_measurements();
            return Ok(mask);
        }

        let result = self.evaluate_warmup(batch)?;
        let local = self.take_measurements();
        self.pool_and_maybe_settle(&local);
        Ok(result)
    }

    /// Evaluate the written order through the wrappers.
    fn evaluate_warmup(&self, batch: &RecordBatch) -> Result<ArrayRef> {
        self.warmup_predicate
            .evaluate(batch)?
            .into_array(batch.num_rows())
    }

    /// Drain the wrappers, indexed by written position.
    fn take_measurements(&self) -> Vec<ConjunctStats> {
        self.measured.iter().map(|m| m.take()).collect()
    }

    /// Evaluate the settled chain, uninstrumented.
    fn evaluate_settled(&self, batch: &RecordBatch) -> Result<ArrayRef> {
        self.settled_predicate
            .evaluate(batch)?
            .into_array(batch.num_rows())
    }

    fn adopt(&mut self, decision: Settled) {
        self.settled_predicate = decision.predicate;
        self.reordered = decision.reordered;
        self.settled = true;
        if self.reordered
            && let Some(count) = &self.adaptive_reorders
        {
            count.add(1);
        }
    }

    /// Pool this batch's counts and settle once the warm-up is full.
    fn pool_and_maybe_settle(&mut self, local: &[ConjunctStats]) {
        let mut inner = self.shared.inner.lock().expect("poisoned");
        // Another stream settled meanwhile: take its decision and drop this
        // batch's counts. Checking here rather than before evaluating keeps the
        // lock off the path until there is something to pool.
        if let Some(decision) = inner.settled.clone() {
            drop(inner);
            self.adopt(decision);
            return;
        }
        if inner.stats.is_empty() {
            inner.stats = vec![ConjunctStats::default(); local.len()];
        }
        // One `AdaptiveFilterShared` only ever backs one predicate.
        debug_assert_eq!(inner.stats.len(), local.len());
        for (s, l) in inner.stats.iter_mut().zip(local) {
            s.merge(l);
        }
        inner.measured_batches += 1;
        if inner.measured_batches < WARMUP_BATCHES {
            return;
        }
        let decision = settle(&inner.stats, &self.conjuncts, &self.written);
        inner.settled = Some(decision.clone());
        drop(inner);
        self.adopt(decision);
    }
}

/// Rank by effectiveness and adopt the ranking as a right-nested `AND` chain,
/// but only if it is materially cheaper than the written order. Otherwise hand
/// back `written` untouched: rebuilding it would reassociate the `AND` tree,
/// which changes where pre-selection fires and so what a fallible conjunct
/// sees, even though the conjunct sequence is unchanged.
fn settle(
    stats: &[ConjunctStats],
    conjuncts: &[Arc<dyn PhysicalExpr>],
    written: &Arc<dyn PhysicalExpr>,
) -> Settled {
    let identity: Vec<usize> = (0..stats.len()).collect();
    let candidate = rank_by_effectiveness(stats);
    if candidate != identity
        && expected_cost_per_row(stats, &candidate)
            < (1.0 - TIE_COST_FRACTION) * expected_cost_per_row(stats, &identity)
    {
        Settled {
            predicate: right_nested_conjunction(conjuncts, &candidate),
            reordered: true,
        }
    } else {
        Settled {
            predicate: Arc::clone(written),
            reordered: false,
        }
    }
}

/// The `AND` tree of `predicate` with every conjunct leaf replaced by a
/// [`MeasuredConjunct`] around it, collected into `measured`.
///
/// The tree keeps its shape, so the warm-up evaluates exactly what the written
/// predicate would — same nesting, same pre-selection points — and only adds
/// the counters. Leaves are collected left to right, the order
/// [`split_conjunction`] yields them in, so `measured[i]` is the wrapper for
/// conjunct `i`.
fn wrap_conjuncts_in_place(
    predicate: &Arc<dyn PhysicalExpr>,
    measured: &mut Vec<Arc<MeasuredConjunct>>,
) -> Arc<dyn PhysicalExpr> {
    if let Some(binary) = predicate.downcast_ref::<BinaryExpr>()
        && *binary.op() == Operator::And
    {
        let left = wrap_conjuncts_in_place(binary.left(), measured);
        let right = wrap_conjuncts_in_place(binary.right(), measured);
        return Arc::new(BinaryExpr::new(left, Operator::And, right)) as _;
    }
    let wrapper = Arc::new(MeasuredConjunct::new(Arc::clone(predicate)));
    measured.push(Arc::clone(&wrapper));
    wrapper as _
}

/// `conjuncts` in `order` as `(c_first AND (c_second AND (... AND c_last)))`.
/// Right-nesting lets [`BinaryExpr`]'s pre-selection keep the first conjunct's
/// survivors compacted for the rest of the chain. `order` must be non-empty.
fn right_nested_conjunction(
    conjuncts: &[Arc<dyn PhysicalExpr>],
    order: &[usize],
) -> Arc<dyn PhysicalExpr> {
    let (&last, rest) = order.split_last().expect("a non-empty order");
    rest.iter()
        .rev()
        .fold(Arc::clone(&conjuncts[last]), |acc, &id| {
            Arc::new(BinaryExpr::new(
                Arc::clone(&conjuncts[id]),
                Operator::And,
                acc,
            )) as _
        })
}

/// Rank conjunct ids by effectiveness (discards per nanosecond) descending;
/// ids without measurements sort last. Stable, so equal ids keep their order.
fn rank_by_effectiveness(stats: &[ConjunctStats]) -> Vec<usize> {
    let mut ids: Vec<usize> = (0..stats.len()).collect();
    ids.sort_by(
        |&a, &b| match (stats[a].effectiveness(), stats[b].effectiveness()) {
            (Some(x), Some(y)) => y.partial_cmp(&x).unwrap_or(std::cmp::Ordering::Equal),
            (Some(_), None) => std::cmp::Ordering::Less,
            (None, Some(_)) => std::cmp::Ordering::Greater,
            (None, None) => std::cmp::Ordering::Equal,
        },
    );
    ids
}

/// Expected nanoseconds per input row for `order`: each conjunct's per-row
/// cost weighted by the product of the
/// [`downstream_weight`](ConjunctStats::downstream_weight)s of the conjuncts
/// before it (assumed independent). Unmeasured conjuncts contribute no cost
/// and narrow nothing.
///
/// The weights are what `AND` really hands on — a conjunct only shrinks the
/// work after it when it pre-selects — so an order is credited for a discard
/// only where evaluation would act on it.
fn expected_cost_per_row(stats: &[ConjunctStats], order: &[usize]) -> f64 {
    let mut weight = 1.0_f64;
    let mut total = 0.0_f64;
    for &id in order {
        let Some(cost) = stats[id].cost_per_row() else {
            continue;
        };
        total += weight * cost;
        weight *= stats[id].downstream_weight();
    }
    total
}

#[cfg(test)]
mod tests {
    use super::*;

    use arrow::array::{Array, Int32Array, Int64Array};
    use arrow::datatypes::{DataType, Field, Schema};
    use datafusion_physical_expr::expressions::{binary, col, lit};

    fn schema() -> Arc<Schema> {
        Arc::new(Schema::new(vec![
            Field::new("a", DataType::Int32, false),
            Field::new("b", DataType::Int32, false),
        ]))
    }

    fn batch(schema: &Arc<Schema>, a: Vec<i32>, b: Vec<i32>) -> RecordBatch {
        RecordBatch::try_new(
            Arc::clone(schema),
            vec![Arc::new(Int32Array::from(a)), Arc::new(Int32Array::from(b))],
        )
        .unwrap()
    }

    /// `a > 2 AND b < 5`
    fn predicate(schema: &Arc<Schema>) -> Arc<dyn PhysicalExpr> {
        let left =
            binary(col("a", schema).unwrap(), Operator::Gt, lit(2i32), schema).unwrap();
        let right =
            binary(col("b", schema).unwrap(), Operator::Lt, lit(5i32), schema).unwrap();
        binary(left, Operator::And, right, schema).unwrap()
    }

    /// The conjuncts of `predicate`, pointer-equal to the ones inside it.
    fn split(predicate: &Arc<dyn PhysicalExpr>) -> Vec<Arc<dyn PhysicalExpr>> {
        split_conjunction(predicate)
            .into_iter()
            .map(Arc::clone)
            .collect()
    }

    /// Assert `chain` is the right-nested `AND` of `conjuncts` in `order`.
    fn assert_chain(
        chain: &Arc<dyn PhysicalExpr>,
        conjuncts: &[Arc<dyn PhysicalExpr>],
        order: &[usize],
    ) {
        let (&last, rest) = order.split_last().expect("a non-empty order");
        let mut node = Arc::clone(chain);
        for (depth, &id) in rest.iter().enumerate() {
            let and = node
                .downcast_ref::<BinaryExpr>()
                .unwrap_or_else(|| panic!("an AND at depth {depth}"));
            assert_eq!(*and.op(), Operator::And);
            assert!(
                Arc::ptr_eq(and.left(), &conjuncts[id]),
                "conjunct {id} at depth {depth}"
            );
            node = Arc::clone(and.right());
        }
        assert!(Arc::ptr_eq(&node, &conjuncts[last]), "last conjunct {last}");
    }

    fn passing_rows(mask: &ArrayRef) -> Vec<usize> {
        let mask = as_boolean_array(mask).unwrap();
        (0..mask.len())
            .filter(|&i| !mask.is_null(i) && mask.value(i))
            .collect()
    }

    /// Counts as if measured over null-free batches of a uniform shape.
    fn stats(rows: u64, matched: u64, nanos: u64) -> ConjunctStats {
        ConjunctStats::from_null_free_batches(rows, matched, nanos)
    }

    /// `try_new` with a fresh, unshared registry and no metric.
    fn try_new(predicate: &Arc<dyn PhysicalExpr>) -> Option<AdaptiveConjunction> {
        AdaptiveConjunction::try_new(
            predicate,
            Arc::new(AdaptiveFilterShared::default()),
            None,
        )
    }

    #[test]
    fn single_conjunct_is_not_adaptive() {
        let schema = schema();
        let p =
            binary(col("a", &schema).unwrap(), Operator::Gt, lit(2i32), &schema).unwrap();
        assert!(try_new(&p).is_none());
    }

    #[test]
    fn two_conjuncts_are_adaptive() {
        let schema = schema();
        let adaptive = try_new(&predicate(&schema)).unwrap();
        assert_eq!(adaptive.conjuncts.len(), 2);
        assert!(!adaptive.settled);
        assert!(!adaptive.reordered);
    }

    #[test]
    fn ranks_by_discards_per_nanosecond() {
        // id 0: cheap (1ns/row), unselective (pass 0.9):      eff = 0.1 / 1  = 0.1
        // id 1: expensive (5ns/row), selective (pass 0.01):   eff = 0.99 / 5 = 0.198 (first)
        // id 2: cheap (1ns/row), very unselective (pass 0.95): eff = 0.05 / 1 = 0.05  (last)
        let s = vec![
            stats(1000, 900, 1000),
            stats(1000, 10, 5000),
            stats(1000, 950, 1000),
        ];
        assert_eq!(rank_by_effectiveness(&s), vec![1, 0, 2]);
    }

    #[test]
    fn unmeasured_conjuncts_sort_last() {
        let s = vec![
            stats(0, 0, 0),         // unmeasured -> last
            stats(1000, 10, 1000),  // selective
            stats(1000, 900, 1000), // unselective
        ];
        assert_eq!(rank_by_effectiveness(&s), vec![1, 2, 0]);
    }

    #[test]
    fn zero_nanos_ranks_as_very_cheap() {
        // A conjunct evaluated faster than the timer's resolution must rank as
        // very cheap (its cost clamps to 1ns total), not drop out of the
        // ranking as unmeasured (which would sort it last — backwards).
        let s = vec![
            stats(1000, 500, 0),    // immeasurably cheap, somewhat selective
            stats(1000, 500, 1000), // same selectivity, 1ns/row
        ];
        assert_eq!(rank_by_effectiveness(&s), vec![0, 1]);
    }

    /// The wrapper counts rows in and non-null trues, returns the array
    /// untouched, and drains to zero. Elapsed time is not asserted: five rows
    /// can legitimately measure 0ns on a coarse timer.
    #[test]
    fn measured_conjunct_counts_rows_and_matches() {
        let schema = Arc::new(Schema::new(vec![Field::new("a", DataType::Int32, true)]));
        let rb = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![Arc::new(Int32Array::from(vec![
                Some(1),
                None,
                Some(5),
                Some(7),
                None,
            ]))],
        )
        .unwrap();
        let inner =
            binary(col("a", &schema).unwrap(), Operator::Gt, lit(2i32), &schema).unwrap();
        let measured = Arc::new(MeasuredConjunct::new(Arc::clone(&inner)));

        let got = measured
            .evaluate(&rb)
            .unwrap()
            .into_array(rb.num_rows())
            .unwrap();
        let want = inner
            .evaluate(&rb)
            .unwrap()
            .into_array(rb.num_rows())
            .unwrap();
        assert_eq!(&got, &want, "the conjunct's own result, unchanged");
        assert_eq!(
            as_boolean_array(&got).unwrap().null_count(),
            2,
            "nulls are left for `BinaryExpr` to interpret"
        );

        let s = measured.take();
        assert_eq!((s.rows, s.matched), (5, 2));
        let s = measured.take();
        assert_eq!((s.rows, s.matched, s.nanos), (0, 0, 0));
    }

    /// Empty batches must not consume the warm-up.
    #[test]
    fn empty_batches_do_not_consume_warmup() {
        let schema = schema();
        let mut adaptive = try_new(&predicate(&schema)).unwrap();

        for _ in 0..(2 * WARMUP_BATCHES) {
            let rb = batch(&schema, vec![], vec![]);
            let got = adaptive.evaluate(&rb).unwrap();
            assert_eq!(got.len(), 0);
        }
        assert!(!adaptive.settled);
        assert_eq!(adaptive.shared.inner.lock().unwrap().measured_batches, 0);
    }

    fn close(got: f64, want: f64) -> bool {
        (got - want).abs() < 1e-9
    }

    /// A conjunct narrows the work after it only where `AND` pre-selects.
    /// Below the threshold the weight is the pass rate; above it, and for an
    /// all-`true` conjunct, the conjuncts after it still see every row.
    #[test]
    fn downstream_weight_follows_the_pre_selection_threshold() {
        // Just below the 20% threshold: pre-selects, so the weight is the
        // pass rate.
        assert!(close(stats(1000, 190, 1000).downstream_weight(), 0.19));
        // Exactly at it: `check_short_circuit` uses `<=`, so it pre-selects.
        assert!(close(stats(1000, 200, 1000).downstream_weight(), 0.20));
        // Just above it: no pre-selection, so the full batch carries on.
        assert!(close(stats(1000, 210, 1000).downstream_weight(), 1.0));
        // Well above it: keeping 30% and keeping 90% both leave the conjuncts
        // after them facing every row, so both weigh the same.
        assert!(close(stats(1000, 300, 1000).downstream_weight(), 1.0));
        assert!(close(stats(1000, 900, 1000).downstream_weight(), 1.0));
        // All rows passing: likewise the full batch.
        assert!(close(stats(1000, 1000, 1000).downstream_weight(), 1.0));
        // All rows rejected: nothing after it is evaluated at all.
        assert!(close(stats(1000, 0, 1000).downstream_weight(), 0.0));
        // Never evaluated: assumed to narrow nothing.
        assert!(close(stats(0, 0, 0).downstream_weight(), 1.0));
    }

    /// The cost model charges a conjunct's followers for the rows `AND` really
    /// hands them, not for its pass rate.
    #[test]
    fn expected_cost_weights_by_what_and_hands_on() {
        // Both keep half the rows, so neither pre-selects and the second
        // conjunct is charged for the whole batch either way.
        let s = vec![stats(1000, 500, 1000), stats(1000, 500, 10_000)];
        assert!(close(expected_cost_per_row(&s, &[0, 1]), 11.0));
        assert!(close(expected_cost_per_row(&s, &[1, 0]), 11.0));

        // Below the threshold conjunct 0 does narrow the batch, and running it
        // first pays: 1 + 0.1 * 10, against 10 + 1 the other way round.
        let s = vec![stats(1000, 100, 1000), stats(1000, 500, 10_000)];
        assert!(close(expected_cost_per_row(&s, &[0, 1]), 2.0));
        assert!(close(expected_cost_per_row(&s, &[1, 0]), 11.0));
    }

    /// The reviewed case: a cheap conjunct keeping 30% ranks ahead of an
    /// expensive one keeping 90%, but neither can pre-select, so promoting it
    /// saves nothing and the reorder must not be adopted.
    #[test]
    fn no_reorder_when_the_better_ranked_conjunct_cannot_pre_select() {
        let schema = schema();
        let p = predicate(&schema);
        let cs = split(&p);
        // id 0: 10ns/row, keeps 90% ; id 1: 1ns/row, keeps 30%.
        let s = vec![stats(1000, 900, 10_000), stats(1000, 300, 1000)];
        assert_eq!(rank_by_effectiveness(&s), vec![1, 0], "id 1 ranks first");
        // Both orders cost 10 + 1: neither conjunct narrows the other.
        assert!(close(expected_cost_per_row(&s, &[0, 1]), 11.0));
        assert!(close(expected_cost_per_row(&s, &[1, 0]), 11.0));

        let d = settle(&s, &cs, &p);
        assert!(!d.reordered, "a reorder that cannot pay must be rejected");
        assert!(Arc::ptr_eq(&d.predicate, &p));
    }

    /// Nulls disable pre-selection entirely, so a conjunct that looks
    /// selective only because it produced them narrows nothing. Measured
    /// through the wrapper, against the same conjunct on a null-free batch.
    #[test]
    fn nulls_disable_the_downstream_discount() {
        let nullable =
            Arc::new(Schema::new(vec![Field::new("a", DataType::Int32, true)]));
        let inner = binary(
            col("a", &nullable).unwrap(),
            Operator::Gt,
            lit(8i32),
            &nullable,
        )
        .unwrap();

        // 1 true, 1 null, 8 false: a 10% pass rate that cannot pre-select.
        let measured = Arc::new(MeasuredConjunct::new(Arc::clone(&inner)));
        let mut with_null: Vec<Option<i32>> = (0..10).map(Some).collect();
        with_null[0] = None;
        let rb = RecordBatch::try_new(
            Arc::clone(&nullable),
            vec![Arc::new(Int32Array::from(with_null))],
        )
        .unwrap();
        measured.evaluate(&rb).unwrap();
        let s = measured.take();
        assert_eq!((s.rows, s.matched), (10, 1));
        assert_eq!(s.gated_rows, 0, "a null batch never pre-selects");
        assert!(close(s.downstream_weight(), 1.0));

        // The same conjunct and the same pass rate without the null does.
        let measured = Arc::new(MeasuredConjunct::new(inner));
        let rb = RecordBatch::try_new(
            Arc::clone(&nullable),
            vec![Arc::new(Int32Array::from((0..10).collect::<Vec<i32>>()))],
        )
        .unwrap();
        measured.evaluate(&rb).unwrap();
        let s = measured.take();
        assert_eq!(
            (s.rows, s.matched, s.gated_rows, s.gated_matched),
            (10, 1, 10, 1)
        );
        assert!(close(s.downstream_weight(), 0.1));
    }

    /// The mask equals the written predicate's before and after settling.
    #[test]
    fn evaluate_matches_predicate_across_warmup() {
        let schema = schema();
        let p = predicate(&schema);
        let mut adaptive = try_new(&p).unwrap();

        for round in 0..(WARMUP_BATCHES as i32 + 4) {
            let base = round * 10;
            let a: Vec<i32> = (base..base + 10).collect();
            let b: Vec<i32> = (base..base + 10).map(|x| x.rem_euclid(9)).collect();
            let rb = batch(&schema, a, b);

            let got = adaptive.evaluate(&rb).unwrap();
            let want = p.evaluate(&rb).unwrap().into_array(rb.num_rows()).unwrap();
            assert_eq!(
                passing_rows(&got),
                passing_rows(&want),
                "mismatch on round {round}"
            );
        }
        assert!(adaptive.settled);
    }

    /// Null-producing conjuncts match the written predicate on every batch.
    #[test]
    fn nullable_conjuncts_match_the_plain_predicate_across_warmup() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("a", DataType::Int32, true),
            Field::new("b", DataType::Int32, true),
        ]));
        let p = predicate(&schema);
        let mut adaptive = try_new(&p).unwrap();

        for round in 0..(WARMUP_BATCHES as i32 + 4) {
            let base = round * 10;
            let a: Vec<Option<i32>> = (base..base + 10)
                .map(|x| (x.rem_euclid(3) != 0).then_some(x))
                .collect();
            let b: Vec<Option<i32>> = (base..base + 10)
                .map(|x| (x.rem_euclid(4) != 0).then_some(x.rem_euclid(9)))
                .collect();
            let rb = RecordBatch::try_new(
                Arc::clone(&schema),
                vec![Arc::new(Int32Array::from(a)), Arc::new(Int32Array::from(b))],
            )
            .unwrap();

            let got = adaptive.evaluate(&rb).unwrap();
            let want = p.evaluate(&rb).unwrap().into_array(rb.num_rows()).unwrap();
            assert_eq!(
                passing_rows(&got),
                passing_rows(&want),
                "mismatch on round {round}"
            );
        }
        assert!(adaptive.settled);
    }

    /// An already-good order is kept as the written expression itself, not
    /// rebuilt: reassociating it would change where pre-selection fires.
    #[test]
    fn settle_keeps_the_written_expression_when_not_materially_better() {
        let schema = schema();
        let p = predicate(&schema);
        // Equally cheap and selective: swapping cannot help.
        let s = vec![stats(1000, 500, 1000), stats(1000, 500, 1000)];
        let cs = split(&p);
        let d = settle(&s, &cs, &p);
        assert!(!d.reordered);
        assert!(
            Arc::ptr_eq(&d.predicate, &p),
            "the written predicate is handed back untouched"
        );
    }

    #[test]
    fn settle_adopts_materially_cheaper_order() {
        let schema = schema();
        let p = predicate(&schema);
        let cs = split(&p);
        // id 1 is far more selective at equal cost: it moves first.
        let s = vec![stats(1000, 900, 1000), stats(1000, 10, 1000)];
        let d = settle(&s, &cs, &p);
        assert!(d.reordered);
        assert_chain(&d.predicate, &cs, &[1, 0]);
    }

    /// The adopted order is a right-nested chain.
    #[test]
    fn adopted_order_is_a_right_nested_and_chain() {
        let schema = schema();
        // Three conjuncts: `a > 2 AND b < 5 AND a < 90`.
        let p = binary(
            predicate(&schema),
            Operator::And,
            binary(
                col("a", &schema).unwrap(),
                Operator::Lt,
                lit(90i32),
                &schema,
            )
            .unwrap(),
            &schema,
        )
        .unwrap();
        let cs = split(&p);
        assert_eq!(cs.len(), 3);

        // Equal cost, decreasing pass rate: the ranking reverses the order.
        let s = vec![
            stats(1000, 900, 1000),
            stats(1000, 500, 1000),
            stats(1000, 10, 1000),
        ];
        let d = settle(&s, &cs, &p);
        assert!(d.reordered);

        // `(cs[2] AND (cs[1] AND cs[0]))`.
        let outer = d.predicate.downcast_ref::<BinaryExpr>().expect("an AND");
        assert_eq!(*outer.op(), Operator::And);
        assert!(
            Arc::ptr_eq(outer.left(), &cs[2]),
            "first conjunct is outermost"
        );
        let inner = outer
            .right()
            .downcast_ref::<BinaryExpr>()
            .expect("the tail is itself an AND");
        assert_eq!(*inner.op(), Operator::And);
        assert!(Arc::ptr_eq(inner.left(), &cs[1]));
        assert!(Arc::ptr_eq(inner.right(), &cs[0]));
    }

    /// A kept written order runs the written expression itself. Real timings
    /// are used on purpose: neither conjunct pre-selects, so both downstream
    /// weights are 1 and no order can be cheaper than another.
    #[test]
    fn no_reorder_evaluates_plain_predicate() {
        let schema = schema();
        let left =
            binary(col("a", &schema).unwrap(), Operator::Gt, lit(2i32), &schema).unwrap();
        let right =
            binary(col("b", &schema).unwrap(), Operator::Gt, lit(2i32), &schema).unwrap();
        let p = binary(left, Operator::And, right, &schema).unwrap();
        let mut adaptive = try_new(&p).unwrap();

        for round in 0..(WARMUP_BATCHES as i32 + 2) {
            let base = round * 10;
            let a: Vec<i32> = (base..base + 10).collect();
            let b: Vec<i32> = (base..base + 10).collect();
            let rb = batch(&schema, a, b);
            let got = adaptive.evaluate(&rb).unwrap();
            let want = p.evaluate(&rb).unwrap().into_array(rb.num_rows()).unwrap();
            assert_eq!(passing_rows(&got), passing_rows(&want));
        }
        assert!(adaptive.settled);
        assert!(
            !adaptive.reordered,
            "interchangeable conjuncts keep the written order"
        );
        assert!(
            Arc::ptr_eq(&adaptive.settled_predicate, &p),
            "and run the written expression itself"
        );
    }

    /// A left-nested three-conjunct predicate, `((a > 2 AND b < 5) AND a < 90)`.
    fn left_nested_predicate(schema: &Arc<Schema>) -> Arc<dyn PhysicalExpr> {
        let third =
            binary(col("a", schema).unwrap(), Operator::Lt, lit(90i32), schema).unwrap();
        binary(predicate(schema), Operator::And, third, schema).unwrap()
    }

    /// The warm-up wraps the conjunct leaves without reshaping the `AND` tree,
    /// so it evaluates exactly what the written predicate would — same
    /// nesting, so the same pre-selection points.
    #[test]
    fn warmup_preserves_the_written_tree_shape() {
        let schema = schema();
        let p = left_nested_predicate(&schema);
        let cs = split(&p);
        assert_eq!(cs.len(), 3);

        let adaptive = try_new(&p).unwrap();

        // `((M(cs[0]) AND M(cs[1])) AND M(cs[2]))`: left-nested, as written.
        let outer = adaptive
            .warmup_predicate
            .downcast_ref::<BinaryExpr>()
            .expect("an AND");
        assert_eq!(*outer.op(), Operator::And);
        let inner = outer
            .left()
            .downcast_ref::<BinaryExpr>()
            .expect("the written tree nests to the left");
        assert_eq!(*inner.op(), Operator::And);

        // Every leaf is the wrapper for the conjunct written in that position.
        for (leaf, id) in [(inner.left(), 0), (inner.right(), 1), (outer.right(), 2)] {
            let wrapper = leaf
                .downcast_ref::<MeasuredConjunct>()
                .unwrap_or_else(|| panic!("conjunct {id} is wrapped"));
            assert!(Arc::ptr_eq(&wrapper.inner, &cs[id]), "conjunct {id}");
            assert!(
                Arc::ptr_eq(&adaptive.measured[id].inner, &cs[id]),
                "measured[{id}] is the wrapper in written position {id}"
            );
        }
    }

    /// Until a reorder is adopted the flag must be inert, side effects
    /// included.
    ///
    /// `((a < 50 AND b < 3) AND 1 / z > 2)` over 100 rows: `a < 50` keeps 50%
    /// and `b < 3` keeps 30%, so neither pre-selects on its own, but their
    /// conjunction keeps 15% and the outer `AND` does — which is the only
    /// reason `1 / z` never meets the zeros. Rebuilding the same conjuncts
    /// right-nested would gate `1 / z` on `b < 3` alone, at 30%, and divide by
    /// zero.
    #[test]
    fn keeping_the_written_order_keeps_its_side_effects() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("a", DataType::Int64, false),
            Field::new("b", DataType::Int64, false),
            Field::new("z", DataType::Int64, false),
        ]));
        let lt = |name: &str, v: i64| {
            binary(col(name, &schema).unwrap(), Operator::Lt, lit(v), &schema).unwrap()
        };
        let divide = || {
            binary(
                binary(
                    lit(1i64),
                    Operator::Divide,
                    col("z", &schema).unwrap(),
                    &schema,
                )
                .unwrap(),
                Operator::Gt,
                lit(2i64),
                &schema,
            )
            .unwrap()
        };
        // Written left-nested, as `conjunction` and the parser build it.
        let p = binary(
            binary(lt("a", 50), Operator::And, lt("b", 3), &schema).unwrap(),
            Operator::And,
            divide(),
            &schema,
        )
        .unwrap();

        // `a < 50` on 50 rows, `b < 3` on 30 spread across them: 15 together.
        let a: Vec<i64> = (0..100).collect();
        let b: Vec<i64> = (0..100).map(|i| i % 10).collect();
        let z: Vec<i64> = (0..100).map(|i| i64::from(i < 50 && i % 10 < 3)).collect();
        let rb = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(Int64Array::from(a)),
                Arc::new(Int64Array::from(b)),
                Arc::new(Int64Array::from(z)),
            ],
        )
        .unwrap();

        // Flag off: pre-selection keeps `1 / z` away from the zeros.
        let want = p.evaluate(&rb).unwrap().into_array(rb.num_rows()).unwrap();
        assert_eq!(passing_rows(&want).len(), 0, "1 / 1 > 2 is false");

        // The same conjuncts in the same order, but right-nested, gate
        // `1 / z` on `b < 3` alone and raise. This is what settling on the
        // written order must not do.
        let right_nested = binary(
            lt("a", 50),
            Operator::And,
            binary(lt("b", 3), Operator::And, divide(), &schema).unwrap(),
            &schema,
        )
        .unwrap();
        let err = right_nested.evaluate(&rb).unwrap_err().to_string();
        assert!(err.contains("Divide by zero"), "unexpected error: {err}");

        // Flag on, settling on the written order: identical counts rank as a
        // tie, so no candidate can be materially cheaper.
        let shared = Arc::new(AdaptiveFilterShared::default());
        shared.seed_one_batch_short_of_warmup(&[
            (70_000_000, 35_000_000, 70_000_000),
            (70_000_000, 35_000_000, 70_000_000),
            (70_000_000, 35_000_000, 70_000_000),
        ]);
        let mut adaptive =
            AdaptiveConjunction::try_new(&p, Arc::clone(&shared), None).unwrap();

        // The settling batch runs through the wrappers ...
        let got = adaptive.evaluate(&rb).unwrap();
        assert_eq!(passing_rows(&got), passing_rows(&want));
        assert!(adaptive.settled && !adaptive.reordered);
        assert!(
            Arc::ptr_eq(&adaptive.settled_predicate, &p),
            "the written expression is run, not a rebuilt one"
        );

        // ... and every batch after it runs the written expression.
        let got = adaptive.evaluate(&rb).unwrap();
        assert_eq!(passing_rows(&got), passing_rows(&want));
    }

    /// Two streams share one pool: the warm-up is pooled across both, and the
    /// second adopts the first's decision on its next batch.
    #[test]
    fn streams_pool_measurements_and_share_settled_order() {
        let schema = schema();
        let p = predicate(&schema); // `a > 2 AND b < 5`, written order [0, 1]
        let cs = split(&p);
        let shared = Arc::new(AdaptiveFilterShared::default());
        shared.seed_one_batch_short_of_warmup(&[
            (70_000_000, 63_000_000, 70_000_000), // pass 0.9, ~1ns/row
            (70_000_000, 700_000, 350_000_000),   // pass 0.01, ~5ns/row
        ]);
        let mut s1 = AdaptiveConjunction::try_new(&p, Arc::clone(&shared), None).unwrap();
        let mut s2 = AdaptiveConjunction::try_new(&p, Arc::clone(&shared), None).unwrap();

        let mk = |round: i32| {
            let base = round * 100;
            let a: Vec<i32> = (base..base + 100).collect();
            let b: Vec<i32> = (base..base + 100).map(|x| x.rem_euclid(25)).collect();
            batch(&schema, a, b)
        };

        for round in 0..(WARMUP_BATCHES as i32) {
            let rb = mk(round);
            for s in [&mut s1, &mut s2] {
                let got = s.evaluate(&rb).unwrap();
                let want = p.evaluate(&rb).unwrap().into_array(rb.num_rows()).unwrap();
                assert_eq!(passing_rows(&got), passing_rows(&want));
            }
        }

        assert!(shared.settled().is_some());
        assert!(s1.settled && s2.settled);
        assert!(s1.reordered && s2.reordered);
        assert_chain(&s1.settled_predicate, &cs, &[1, 0]);
        assert_chain(&s2.settled_predicate, &cs, &[1, 0]);
    }

    /// `adaptive_reorders` is bumped once per stream: by the settler and by a
    /// stream that later takes up its decision.
    #[test]
    fn adopted_reorder_signals_once_per_stream() {
        let schema = schema();
        let p = predicate(&schema); // `a > 2 AND b < 5`, written order [0, 1]
        let shared = Arc::new(AdaptiveFilterShared::default());
        shared.seed_one_batch_short_of_warmup(&[
            (70_000_000, 63_000_000, 70_000_000), // pass 0.9, ~1ns/row
            (70_000_000, 700_000, 350_000_000),   // pass 0.01, ~5ns/row
        ]);
        let settler_count = Count::new();
        let adopter_count = Count::new();
        let mut settler = AdaptiveConjunction::try_new(
            &p,
            Arc::clone(&shared),
            Some(settler_count.clone()),
        )
        .unwrap();
        let mut adopter = AdaptiveConjunction::try_new(
            &p,
            Arc::clone(&shared),
            Some(adopter_count.clone()),
        )
        .unwrap();

        let a: Vec<i32> = (0..100).collect();
        let b: Vec<i32> = a.iter().map(|x| x.rem_euclid(25)).collect();
        let rb = batch(&schema, a, b);

        assert_eq!(settler_count.value(), 0);

        // This batch completes the warm-up.
        settler.evaluate(&rb).unwrap();
        assert!(settler.reordered);
        assert_eq!(settler_count.value(), 1);
        settler.evaluate(&rb).unwrap();
        assert_eq!(settler_count.value(), 1);

        adopter.evaluate(&rb).unwrap();
        assert!(adopter.reordered);
        assert_eq!(adopter_count.value(), 1);
        adopter.evaluate(&rb).unwrap();
        assert_eq!(adopter_count.value(), 1);
    }

    /// Keeping the written order is not a reorder.
    #[test]
    fn settling_without_reorder_signals_nothing() {
        let schema = schema();
        let p = predicate(&schema);
        let shared = Arc::new(AdaptiveFilterShared::default());
        // Identical cost and selectivity: no order can be materially cheaper.
        shared.seed_one_batch_short_of_warmup(&[
            (70_000_000, 35_000_000, 70_000_000),
            (70_000_000, 35_000_000, 70_000_000),
        ]);
        let count = Count::new();
        let mut adaptive =
            AdaptiveConjunction::try_new(&p, Arc::clone(&shared), Some(count.clone()))
                .unwrap();

        let a: Vec<i32> = (0..100).collect();
        let rb = batch(&schema, a.clone(), a);
        adaptive.evaluate(&rb).unwrap();
        assert!(adaptive.settled && !adaptive.reordered);
        assert_eq!(count.value(), 0);
    }

    /// Feed batches, settle on a reorder, stay there.
    #[test]
    fn scenario_measure_batches_then_settle_on_reorder() {
        let schema = schema();
        let p = predicate(&schema); // `a > 2 AND b < 5`, written order [0, 1]
        let cs = split(&p);
        let shared = Arc::new(AdaptiveFilterShared::default());
        shared.seed_one_batch_short_of_warmup(&[
            (70_000_000, 63_000_000, 70_000_000), // pass 0.9, ~1ns/row
            (70_000_000, 700_000, 350_000_000),   // pass 0.01, ~5ns/row
        ]);
        let mut adaptive =
            AdaptiveConjunction::try_new(&p, Arc::clone(&shared), None).unwrap();
        assert!(!adaptive.settled, "the first batch is still measured");

        for round in 0..3 {
            let base = round * 100;
            let a: Vec<i32> = (base..base + 100).collect();
            let b: Vec<i32> = (base..base + 100).map(|x| x.rem_euclid(25)).collect();
            let rb = batch(&schema, a, b);
            let got = adaptive.evaluate(&rb).unwrap();
            let want = p.evaluate(&rb).unwrap().into_array(rb.num_rows()).unwrap();
            assert_eq!(passing_rows(&got), passing_rows(&want), "round {round}");
            assert!(adaptive.settled, "settled after round {round}");
            assert!(adaptive.reordered, "reordered after round {round}");
            assert_chain(&adaptive.settled_predicate, &cs, &[1, 0]);
        }
    }

    /// Interchangeable conjuncts settle on the written order.
    #[test]
    fn scenario_measure_batches_then_settle_on_written_order() {
        let schema = schema();
        let p = predicate(&schema);
        let shared = Arc::new(AdaptiveFilterShared::default());
        // Identical cost and selectivity: no order can be materially cheaper.
        shared.seed_one_batch_short_of_warmup(&[
            (70_000_000, 35_000_000, 70_000_000),
            (70_000_000, 35_000_000, 70_000_000),
        ]);
        let mut adaptive =
            AdaptiveConjunction::try_new(&p, Arc::clone(&shared), None).unwrap();
        assert!(!adaptive.settled, "the first batch is still measured");

        for round in 0..3 {
            let base = round * 100;
            let a: Vec<i32> = (base..base + 100).collect();
            let b: Vec<i32> = (base..base + 100).collect();
            let rb = batch(&schema, a, b);
            let got = adaptive.evaluate(&rb).unwrap();
            let want = p.evaluate(&rb).unwrap().into_array(rb.num_rows()).unwrap();
            assert_eq!(passing_rows(&got), passing_rows(&want), "round {round}");
            assert!(adaptive.settled, "settled after round {round}");
            assert!(!adaptive.reordered, "not reordered after round {round}");
            assert!(Arc::ptr_eq(&adaptive.settled_predicate, &p));
        }
    }

    /// The pool is sized by the first measured batch and receives its counts.
    #[test]
    fn first_measured_batch_initialises_the_shared_pool() {
        let schema = schema();
        let p = predicate(&schema); // `a > 2 AND b < 5`
        let shared = Arc::new(AdaptiveFilterShared::default());
        assert!(shared.inner.lock().unwrap().stats.is_empty());
        let mut adaptive =
            AdaptiveConjunction::try_new(&p, Arc::clone(&shared), None).unwrap();
        adaptive.evaluate(&batch(&schema, vec![], vec![])).unwrap();
        assert!(shared.inner.lock().unwrap().stats.is_empty());

        let a: Vec<i32> = (0..10).collect();
        adaptive.evaluate(&batch(&schema, a.clone(), a)).unwrap();

        let inner = shared.inner.lock().unwrap();
        assert_eq!(inner.stats.len(), 2, "sized to the conjunct count");
        assert_eq!(inner.measured_batches, 1);
        // `a > 2` keeps 7 of 10 (no pre-selection), so `b < 5` sees all 10.
        assert_eq!((inner.stats[0].rows, inner.stats[0].matched), (10, 7));
        assert_eq!((inner.stats[1].rows, inner.stats[1].matched), (10, 5));
    }

    /// `Int64` schema for the divide-by-zero side-effect tests below.
    fn int64_schema() -> Arc<Schema> {
        Arc::new(Schema::new(vec![
            Field::new("a", DataType::Int64, false),
            Field::new("b", DataType::Int64, false),
        ]))
    }

    fn int64_batch(schema: &Arc<Schema>, a: Vec<i64>, b: Vec<i64>) -> RecordBatch {
        RecordBatch::try_new(
            Arc::clone(schema),
            vec![Arc::new(Int64Array::from(a)), Arc::new(Int64Array::from(b))],
        )
        .unwrap()
    }

    /// The two conjuncts `b <> 0` and `1 / b > 2`.
    fn divide_by_zero_conjuncts(
        schema: &Arc<Schema>,
    ) -> (Arc<dyn PhysicalExpr>, Arc<dyn PhysicalExpr>) {
        let non_zero = binary(
            col("b", schema).unwrap(),
            Operator::NotEq,
            lit(0i64),
            schema,
        )
        .unwrap();
        let divide = binary(
            binary(
                lit(1i64),
                Operator::Divide,
                col("b", schema).unwrap(),
                schema,
            )
            .unwrap(),
            Operator::Gt,
            lit(2i64),
            schema,
        )
        .unwrap();
        (non_zero, divide)
    }

    /// The side effect the config doc warns about: a reorder can introduce an
    /// error the written order avoided. `b <> 0` holds on 15% of rows, so the
    /// written `AND` pre-selects and `1 / b` never sees a zero; reordered,
    /// `1 / b > 2` runs first on every row.
    #[test]
    fn adopted_reorder_can_introduce_a_divide_by_zero() {
        let schema = int64_schema();
        let (non_zero, divide) = divide_by_zero_conjuncts(&schema);
        let p = binary(non_zero, Operator::And, divide, &schema).unwrap();
        let cs = split(&p);
        let a: Vec<i64> = (0..100).collect();
        let b: Vec<i64> = (0..100).map(|i| i64::from(i < 15)).collect();
        let rb = int64_batch(&schema, a, b);

        // Flag off: succeeds.
        assert!(p.evaluate(&rb).is_ok(), "flag-off evaluation must succeed");

        let shared = Arc::new(AdaptiveFilterShared::default());
        shared.seed_one_batch_short_of_warmup(&[
            (70_000_000, 63_000_000, 70_000_000), // pass 0.9, ~1ns/row
            (70_000_000, 700_000, 70_000_000),    // pass 0.01, ~1ns/row
        ]);
        let mut adaptive =
            AdaptiveConjunction::try_new(&p, Arc::clone(&shared), None).unwrap();

        // The settling batch still runs in the written order.
        adaptive.evaluate(&rb).unwrap();
        assert!(adaptive.reordered);
        assert_chain(&adaptive.settled_predicate, &cs, &[1, 0]);

        // The next batch runs the reorder.
        let err = adaptive.evaluate(&rb).unwrap_err().to_string();
        assert!(err.contains("Divide by zero"), "unexpected error: {err}");
    }

    /// The mirror: `1 / b > 2 AND a < 10` with `b = 0` exactly where `a < 10`
    /// discards. The written order divides by zero; reordered, `a < 10` keeps
    /// 10% of rows, so pre-selection keeps `1 / b` away from the zeros.
    #[test]
    fn adopted_reorder_can_avoid_a_divide_by_zero_the_written_order_raises() {
        let schema = int64_schema();
        let (_, divide) = divide_by_zero_conjuncts(&schema);
        let selective = binary(
            col("a", &schema).unwrap(),
            Operator::Lt,
            lit(10i64),
            &schema,
        )
        .unwrap();
        let p = binary(divide, Operator::And, selective, &schema).unwrap();
        let cs = split(&p);

        let shared = Arc::new(AdaptiveFilterShared::default());
        shared.seed_one_batch_short_of_warmup(&[
            (70_000_000, 63_000_000, 350_000_000), // pass 0.9, ~5ns/row
            (70_000_000, 700_000, 70_000_000),     // pass 0.01, ~1ns/row
        ]);
        let mut adaptive =
            AdaptiveConjunction::try_new(&p, Arc::clone(&shared), None).unwrap();

        // Settle on a batch with no zeros, so the warm-up cannot error.
        let a: Vec<i64> = (0..100).collect();
        adaptive
            .evaluate(&int64_batch(&schema, a.clone(), vec![1; 100]))
            .unwrap();
        assert!(adaptive.reordered);
        assert_chain(&adaptive.settled_predicate, &cs, &[1, 0]);

        let b: Vec<i64> = (0..100).map(|i| i64::from(i < 10)).collect();
        let rb = int64_batch(&schema, a, b);

        let err = p.evaluate(&rb).unwrap_err().to_string();
        assert!(err.contains("Divide by zero"), "unexpected error: {err}");
        let got = adaptive.evaluate(&rb).unwrap();
        assert!(passing_rows(&got).is_empty(), "1 / 1 > 2 is false");
    }
}
