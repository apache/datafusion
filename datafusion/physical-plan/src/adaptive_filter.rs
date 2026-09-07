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
//! Predicate evaluation order matters: a selective predicate run first gates
//! the work of the predicates after it. DataFusion's `BinaryExpr` `AND`
//! short-circuit only gates on the *leftmost* conjunct, so a conjunction whose
//! selective member is written last (e.g.
//! `regexp_like(s,'a') AND … AND regexp_like(s,'rare')`) evaluates every
//! predicate against ~every row.
//!
//! ## How it evaluates: the compact-once loop
//!
//! The conjuncts are evaluated sequentially, combining their boolean results
//! with `AND`. The working batch is physically compacted to the surviving rows
//! once the accumulated mask becomes selective enough — so a run of
//! non-selective conjuncts costs only cheap bitwise `AND`s, while a selective
//! conjunct shrinks the batch the conjuncts after it must decode. This
//! compaction is what makes ordering pay off (and is itself a win even without
//! reordering): a left-deep fused `BinaryExpr` `AND` does *not* compact between
//! conjuncts, so it evaluates ~every conjunct on ~every row regardless of order.
//!
//! ## How it orders
//!
//! Each conjunct is timed and counted on exactly the rows it evaluated, giving
//! its marginal selectivity and per-row cost. After a short warm-up the
//! conjuncts are ranked by rows discarded per nanosecond
//! (`(1 - pass_rate) / cost_per_row`, the classic optimal ordering key for
//! independent conjuncts), and if the ranked order is *materially* cheaper than
//! the written one it is adopted. The order then stays fixed.
//!
//! Compact-once is used **only in service of a reorder**: if the warm-up does
//! not reorder the conjuncts (e.g. they are interchangeable), the written
//! predicate is evaluated as-is, so once settled a conjunction that does not
//! benefit from reordering pays no compact-once overhead and evaluates exactly
//! as it would with the feature off. During the warm-up itself the conjuncts
//! are necessarily evaluated individually (with compaction) so they can be
//! measured — see the side-effects caveat below.
//!
//! ## How it shares
//!
//! A `FilterExec` is split across many partition streams, each seeing only a
//! slice of the data. Measurements are pooled into a shared
//! [`AdaptiveFilterShared`] so the streams learn as one: the first stream to
//! accumulate enough samples settles the order and publishes it, and the others
//! adopt it on their next batch without each re-paying the warm-up — which is
//! what makes the win materialise when each stream is only a handful of batches
//! long. Only unsettled streams touch the shared mutex, and only to pool a
//! batch's counts or pick up a published decision; a settled stream never
//! locks it again.
//!
//! It is **off by default**
//! (`datafusion.execution.adaptive_filter_reordering`) and never changes query
//! results: a conjunction's value is independent of evaluation order. Predicates
//! containing volatile expressions are never reordered (their observable side
//! effects depend on order).
//!
//! Observable *side effects* of fallible predicates can change even when no
//! reorder is adopted: whenever conjuncts are evaluated individually (during
//! warm-up, or settled with a reorder), a conjunct after a compaction sees only
//! the surviving rows, so an error a fused evaluation would have raised on an
//! already-filtered row (e.g. `b <> 0 AND 1/b > 2` with the fused `AND`
//! evaluating `1/b` on every row) may not occur.
//!
//! ## Known limitations
//!
//! The statistics are *conditional*: each conjunct is measured on the rows that
//! survived the conjuncts before it in written order, and (after a compaction)
//! on small survivor batches whose per-row cost is inflated by fixed overheads.
//! Correlated conjuncts can therefore look more selective in a late position
//! than they would be up front, and the settle decision is one-shot: once
//! adopted, the order is never re-measured, so a misjudged reorder (or drifting
//! data) is kept for the stream's lifetime. The material-win guard
//! ([`TIE_COST_FRACTION`]) makes adoption conservative but cannot detect
//! correlation. Further policies (drift re-measurement, confidence-interval
//! statistics, A/B-validated adoption for the cases a cost model cannot
//! separate) can build on top of this core.

use std::sync::Arc;
use std::sync::Mutex;

use arrow::array::{Array, ArrayRef, BooleanArray, BooleanBufferBuilder, UInt32Array};
use arrow::buffer::BooleanBuffer;
use arrow::compute::kernels::boolean::and;
use arrow::compute::{filter, filter_record_batch, prep_null_mask_filter};
use arrow::record_batch::RecordBatch;
use datafusion_common::cast::as_boolean_array;
use datafusion_common::instant::Instant;
use datafusion_common::{Result, internal_err};
use datafusion_physical_expr::PhysicalExpr;
use datafusion_physical_expr::utils::split_conjunction;
use datafusion_physical_expr_common::physical_expr::is_volatile;

/// Batches measured before the order is settled.
const WARMUP_BATCHES: u64 = 8;

/// Fraction of the conjunction's expected per-row cost below which a reorder is
/// immaterial. A candidate order is adopted only if it is expected to cost less
/// than `(1 - TIE_COST_FRACTION)` of the written order, so interchangeable
/// conjuncts never trigger a reorder.
const TIE_COST_FRACTION: f64 = 0.05;

/// Physically compact the working batch to the surviving rows only when the
/// accumulated mask keeps at most this fraction of them. Above this, the cost
/// of materializing a barely-smaller batch is not repaid, so we keep evaluating
/// against the full working batch and just `AND` the boolean masks.
const COMPACTION_SELECTIVITY_THRESHOLD: f64 = 0.2;

/// Per-conjunct measurement: marginal pass rate and per-row evaluation cost,
/// accumulated over the warm-up window on exactly the rows that reached the
/// conjunct.
#[derive(Debug, Default, Clone)]
struct ConjunctStats {
    /// Total rows the conjunct was evaluated on.
    rows: u64,
    /// Rows that passed (non-null `true`, matching SQL filter semantics).
    matched: u64,
    /// Total evaluation time, nanoseconds.
    nanos: u64,
}

impl ConjunctStats {
    fn record(&mut self, matched: u64, rows: u64, nanos: u64) {
        self.rows += rows;
        self.matched += matched;
        self.nanos += nanos;
    }

    /// Fold another accumulator's counts into this one (they are plain sums, so
    /// merging is addition). Used to pool measurements across partition streams.
    fn merge(&mut self, other: &Self) {
        self.rows += other.rows;
        self.matched += other.matched;
        self.nanos += other.nanos;
    }

    /// Fraction of rows that pass, or `None` if never evaluated on any row.
    fn pass_rate(&self) -> Option<f64> {
        (self.rows > 0).then(|| self.matched as f64 / self.rows as f64)
    }

    /// Per-row evaluation cost in nanoseconds, or `None` if the conjunct was
    /// never evaluated on any row. An evaluation faster than the timer's
    /// resolution is clamped to one nanosecond total: "too cheap to measure"
    /// must rank as very cheap, not drop out of the ranking as unmeasured.
    fn cost_per_row(&self) -> Option<f64> {
        (self.rows > 0).then(|| self.nanos.max(1) as f64 / self.rows as f64)
    }

    /// Ranking key: rows discarded per nanosecond of evaluation
    /// (`(1 - pass_rate) / cost_per_row`). Maximising this is exactly
    /// minimising `cost_per_row / (1 - pass_rate)`, the classic optimal
    /// ordering key for independent conjuncts — so a selective-but-expensive
    /// predicate correctly sorts ahead of a cheap-but-unselective one.
    /// `None` when unmeasured, so such conjuncts sort last.
    fn effectiveness(&self) -> Option<f64> {
        let cost = self.cost_per_row()?;
        let pass = self.pass_rate()?;
        Some((1.0 - pass) / cost)
    }
}

/// State shared by every partition stream of one `FilterExec`, so the streams
/// learn as one: per-conjunct measurements are pooled across streams and the
/// first stream to accumulate enough samples settles the order for all of them.
///
/// This matters because a `FilterExec` is split across many partition streams,
/// each seeing only a slice of the data. Without sharing, every stream pays its
/// own warm-up — and when each stream is only a handful of batches long, that
/// warm-up is most of its work, so the reordering win never materialises. With
/// sharing the warm-up is paid roughly once per query, not once per stream.
#[derive(Debug, Default)]
pub(crate) struct AdaptiveFilterShared {
    inner: Mutex<SharedInner>,
}

#[derive(Debug, Default)]
struct SharedInner {
    /// Per-conjunct counts pooled across all streams (indexed by conjunct
    /// position). Empty until the first measured batch sizes it.
    stats: Vec<ConjunctStats>,
    /// Measured batches contributed by all streams so far.
    measured_batches: u64,
    /// The settled decision, once made; `None` while learning.
    settled: Option<Settled>,
}

/// How one batch was evaluated. Reported by
/// [`AdaptiveConjunction::evaluate_traced`] so the evaluator's behaviour is
/// observable batch by batch (its input/output contract is exercised by the
/// `scenario_*` tests). Borrows the adopted order rather than cloning it, so
/// reporting costs nothing on the per-batch path.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum BatchStrategy<'a> {
    /// Still learning: the written order through the compact-once loop, each
    /// conjunct instrumented and its counts pooled. (Empty batches are
    /// evaluated but measure nothing and do not consume the warm-up.)
    Measure,
    /// Settled without a reorder: the written fused predicate, exactly as if
    /// the feature were off.
    Fused,
    /// Settled on an adopted reorder, evaluated through the compact-once
    /// loop. The payload is the adopted evaluation order: positions in the
    /// written conjunct list, first-evaluated first.
    Reordered(&'a [usize]),
}

/// The settled outcome of the warm-up: the evaluation order, and whether to run
/// it through the compact-once loop or the plain predicate.
#[derive(Debug, Clone)]
struct Settled {
    /// Evaluation order: indices into the conjunct list.
    order: Vec<usize>,
    /// `true` to run `order` through the compact-once loop; `false` to evaluate
    /// the written predicate as-is (see [`settle`]).
    compact: bool,
}

impl AdaptiveFilterShared {
    pub(crate) fn new() -> Self {
        Self::default()
    }

    /// The published settled decision, or `None` if streams are still learning.
    fn settled(&self) -> Option<Settled> {
        self.inner.lock().expect("poisoned").settled.clone()
    }

    /// Test-only: seed the pooled measurements with `(rows, matched, nanos)`
    /// per conjunct and leave the pool exactly one batch short of the warm-up,
    /// so the next measured batch settles on the seeded decision.
    ///
    /// The stand-in for a mocked clock: it lets a test pin the settle decision
    /// instead of depending on real timer values, which on a shared CI runner
    /// can be perturbed by a scheduling hiccup far larger than the evaluation
    /// being measured. Used by `FilterExec`'s end-to-end tests; the tests in
    /// this module use the equivalent `tests::seed`.
    #[cfg(test)]
    pub(crate) fn seed_one_batch_short_of_warmup(
        &self,
        per_conjunct: &[(u64, u64, u64)],
    ) {
        let mut inner = self.inner.lock().expect("poisoned");
        inner.stats = per_conjunct
            .iter()
            .map(|&(rows, matched, nanos)| ConjunctStats {
                rows,
                matched,
                nanos,
            })
            .collect();
        inner.measured_batches = WARMUP_BATCHES - 1;
    }
}

/// Adaptive evaluator for a single conjunctive predicate, owned per partition
/// stream. Measurements are pooled into the shared [`AdaptiveFilterShared`];
/// the per-stream state is just the current order and how far it has caught up.
#[derive(Debug)]
pub(crate) struct AdaptiveConjunction {
    /// The split conjuncts. `order` indices refer to positions here.
    conjuncts: Vec<Arc<dyn PhysicalExpr>>,
    /// The written predicate, evaluated as-is when the settled order does not
    /// reorder it (so a settled non-reorder costs exactly what the flag-off path
    /// costs — no compact-once overhead).
    predicate: Arc<dyn PhysicalExpr>,
    /// Measurements and the settled decision, shared by every partition stream.
    shared: Arc<AdaptiveFilterShared>,
    /// Evaluation order: indices into `conjuncts`. The written order until a
    /// settled order is adopted.
    order: Vec<usize>,
    /// Whether the settled order runs through the compact-once loop; `false`
    /// means evaluate [`predicate`](Self::predicate) directly.
    compact: bool,
    /// Whether the order is settled (frozen): this stream no longer measures.
    settled: bool,
    /// Set when this stream adopts a *reordered* decision, and cleared by
    /// [`take_adopted_reorder`](Self::take_adopted_reorder) — a one-shot
    /// transition signal so the owner (`FilterExec`'s stream) can report that
    /// the reorder happened without this type knowing about metrics.
    adopted_reorder: bool,
}

impl AdaptiveConjunction {
    /// Build an adaptive evaluator for `predicate`, or `None` if adaptive
    /// reordering does not structurally apply:
    ///
    /// - the predicate has fewer than two `AND` conjuncts (nothing to reorder);
    /// - any conjunct is volatile (reordering could change side effects).
    ///
    /// Whether adaptive reordering is *enabled* is the caller's policy (the
    /// config flag lives with `FilterExec`); this constructor only answers
    /// whether the predicate is a reorderable conjunction. `shared` is the
    /// state common to all partition streams of the owning `FilterExec`.
    pub(crate) fn try_new(
        predicate: &Arc<dyn PhysicalExpr>,
        shared: Arc<AdaptiveFilterShared>,
    ) -> Option<Self> {
        let conjuncts: Vec<Arc<dyn PhysicalExpr>> = split_conjunction(predicate)
            .into_iter()
            .map(Arc::clone)
            .collect();
        if conjuncts.len() < 2 || conjuncts.iter().any(is_volatile) {
            return None;
        }
        let order = (0..conjuncts.len()).collect();
        Some(Self {
            conjuncts,
            predicate: Arc::clone(predicate),
            shared,
            order,
            compact: false,
            settled: false,
            adopted_reorder: false,
        })
    }

    /// Whether this stream has just adopted a reordered evaluation order,
    /// clearing the signal.
    ///
    /// Fires exactly once per stream, on the batch at which the stream settles
    /// on a reorder — whether it settled the order itself or picked up one
    /// another stream published. A stream that settles on the written order
    /// (no reorder) never fires.
    pub(crate) fn take_adopted_reorder(&mut self) -> bool {
        std::mem::take(&mut self.adopted_reorder)
    }

    /// Evaluate the conjunction against `batch`, returning the boolean mask
    /// (over the batch's rows) of rows that passed every conjunct.
    ///
    /// Until the order settles, each batch is measured and its counts pooled
    /// into the shared registry; a stream adopts the settled order another
    /// stream published on its next batch.
    pub(crate) fn evaluate(&mut self, batch: &RecordBatch) -> Result<ArrayRef> {
        self.evaluate_traced(batch).map(|(mask, _)| mask)
    }

    /// [`evaluate`](Self::evaluate), additionally reporting the
    /// [`BatchStrategy`] used for this batch, so the evaluator's behaviour is
    /// observable batch by batch (see the scenario tests).
    fn evaluate_traced(
        &mut self,
        batch: &RecordBatch,
    ) -> Result<(ArrayRef, BatchStrategy<'_>)> {
        // Adopt a settled order another stream published since our last batch.
        // An unsettled stream already locks the shared state once per measured
        // batch to pool its counts, so this brief extra lock is in the same
        // cost class; a settled stream never touches it again.
        if !self.settled
            && let Some(decision) = self.shared.settled()
        {
            self.adopt(decision);
        }
        if self.settled {
            let mask = self.evaluate_settled(batch)?;
            let strategy = if self.compact {
                BatchStrategy::Reordered(&self.order)
            } else {
                BatchStrategy::Fused
            };
            return Ok((mask, strategy));
        }

        // An empty batch measures nothing; evaluating it must not consume the
        // warm-up (a run of empty batches would otherwise settle the written
        // order on no evidence, permanently).
        if batch.num_rows() == 0 {
            let mask = eval_conjuncts(&self.conjuncts, &self.order, batch, None)?;
            return Ok((mask, BatchStrategy::Measure));
        }

        // Measure this batch into a local accumulator, then pool it.
        let mut local = vec![ConjunctStats::default(); self.conjuncts.len()];
        let result =
            eval_conjuncts(&self.conjuncts, &self.order, batch, Some(&mut local))?;
        self.pool_and_maybe_settle(&local);
        Ok((result, BatchStrategy::Measure))
    }

    /// Evaluate the settled arrangement with no instrumentation: the
    /// compact-once loop when the order was reordered, or the written predicate
    /// directly otherwise (identical to the feature being off).
    fn evaluate_settled(&self, batch: &RecordBatch) -> Result<ArrayRef> {
        if self.compact {
            eval_conjuncts(&self.conjuncts, &self.order, batch, None)
        } else {
            self.predicate.evaluate(batch)?.into_array(batch.num_rows())
        }
    }

    fn adopt(&mut self, decision: Settled) {
        self.order = decision.order;
        self.compact = decision.compact;
        self.settled = true;
        // Only a genuine reorder is worth reporting; settling on the written
        // order is indistinguishable from the feature being off.
        self.adopted_reorder = self.compact;
    }

    /// Merge this batch's measurements into the shared pool and, once enough
    /// batches have accrued across all streams, decide and publish the order.
    fn pool_and_maybe_settle(&mut self, local: &[ConjunctStats]) {
        let mut inner = self.shared.inner.lock().expect("poisoned");
        if inner.stats.is_empty() {
            inner.stats = vec![ConjunctStats::default(); local.len()];
        }
        // One `AdaptiveFilterShared` only ever backs one predicate: the builder,
        // predicate rewrites and `reset_state` each allocate a fresh instance,
        // and the paths that share one (`Clone`, `with_fetch`,
        // `with_batch_size`) keep the same predicate.
        debug_assert_eq!(inner.stats.len(), local.len());
        for (s, l) in inner.stats.iter_mut().zip(local) {
            s.merge(l);
        }
        inner.measured_batches += 1;
        // Another stream settled between our two lock acquisitions: adopt its
        // decision rather than measuring on.
        if let Some(decision) = inner.settled.clone() {
            drop(inner);
            self.adopt(decision);
            return;
        }
        if inner.measured_batches < WARMUP_BATCHES {
            return;
        }
        let decision = settle(&inner.stats);
        inner.settled = Some(decision.clone());
        drop(inner);
        self.adopt(decision);
    }
}

/// Decide the settled arrangement from the pooled measurements.
///
/// Rank the conjuncts by effectiveness and adopt the ranking only if it is
/// materially cheaper than the written order. A genuine reorder runs through the
/// compact-once loop (the source of the win); otherwise the written predicate is
/// kept and evaluated as-is. This is the guard: compact-once is only ever used
/// in service of a reorder, so a conjunction that does not benefit from
/// reordering (interchangeable conjuncts, e.g. several equally expensive
/// unselective predicates) pays no compact-once overhead and behaves exactly as
/// it would with the feature off.
fn settle(stats: &[ConjunctStats]) -> Settled {
    let identity: Vec<usize> = (0..stats.len()).collect();
    let candidate = rank_by_effectiveness(stats);
    if candidate != identity
        && expected_cost_per_row(stats, &candidate)
            < (1.0 - TIE_COST_FRACTION) * expected_cost_per_row(stats, &identity)
    {
        Settled {
            order: candidate,
            compact: true,
        }
    } else {
        Settled {
            order: identity,
            compact: false,
        }
    }
}

/// Evaluate `conjuncts` in `order` against `batch` via the compact-once loop,
/// returning the boolean mask (over the batch's original rows) of rows that
/// passed every conjunct. With `stats`, each conjunct is additionally timed and
/// counted on exactly the rows it evaluated (its marginal selectivity and cost
/// on the current working population).
///
/// The working batch is physically compacted to the surviving rows only once
/// the accumulated mask becomes selective enough (see
/// [`COMPACTION_SELECTIVITY_THRESHOLD`]); until then masks are combined with a
/// cheap bitwise `AND`, so a run of non-selective conjuncts pays no
/// materialization cost. Unlike a fused `BinaryExpr` chain, survivors stay
/// compacted across the remaining conjuncts instead of being re-evaluated on
/// every row.
fn eval_conjuncts(
    conjuncts: &[Arc<dyn PhysicalExpr>],
    order: &[usize],
    batch: &RecordBatch,
    mut stats: Option<&mut [ConjunctStats]>,
) -> Result<ArrayRef> {
    let num_rows = batch.num_rows();
    if num_rows == 0 {
        return Ok(Arc::new(BooleanArray::from(Vec::<bool>::new())));
    }
    // Live-row indices are tracked as `u32` (arrow's `filter`/`take` index
    // space); a larger batch would silently wrap the indices, so refuse it.
    if num_rows > u32::MAX as usize {
        return internal_err!("adaptive filter: batch exceeds u32::MAX rows");
    }

    // `working` is the batch conjuncts are evaluated against. `acc` is the
    // accumulated (`AND`-combined, null-free) result over `working`'s rows since
    // the last compaction; `None` means all of them are still live. `live` maps
    // `working`'s rows back to original row indices; `None` until a compaction
    // first drops rows.
    let mut working = batch.clone();
    let mut acc: Option<BooleanArray> = None;
    let mut live: Option<ArrayRef> = None;

    for &id in order {
        let rows_in = working.num_rows();

        let timer = stats.is_some().then(Instant::now);
        let array = conjuncts[id].evaluate(&working)?.into_array(rows_in)?;
        let mask = as_boolean_array(&array)?;
        // `matched` counts non-null trues (SQL filter semantics).
        let matched = mask.true_count() as u64;

        if let (Some(stats), Some(timer)) = (stats.as_deref_mut(), timer) {
            let eval_nanos = timer.elapsed().as_nanos() as u64;
            stats[id].record(matched, rows_in as u64, eval_nanos);
        }

        // An all-true mask leaves the accumulated result untouched.
        if matched == rows_in as u64 && mask.null_count() == 0 {
            continue;
        }

        // Fold this conjunct into the accumulated mask (null -> false).
        let mask = if mask.null_count() > 0 {
            prep_null_mask_filter(mask)
        } else {
            mask.clone()
        };
        let folded = match &acc {
            None => mask,
            Some(prev) => and(prev, &mask)?,
        };

        let alive = folded.true_count();
        if alive == 0 {
            // Nothing survives; the result is all-false over the original rows.
            return Ok(Arc::new(BooleanArray::new(
                BooleanBuffer::new_unset(num_rows),
                None,
            )));
        }
        // Compact only when the survivors are a small fraction of the working
        // batch — otherwise the copy is not worth it.
        if (alive as f64) <= COMPACTION_SELECTIVITY_THRESHOLD * rows_in as f64 {
            working = filter_record_batch(&working, &folded)?;
            let indices = live.take().unwrap_or_else(|| {
                Arc::new(UInt32Array::from_iter_values(0..num_rows as u32))
            });
            live = Some(filter(&indices, &folded)?);
            acc = None;
        } else {
            acc = Some(folded);
        }
    }

    match live {
        // Never compacted: `acc` (or all-true) already covers the original rows.
        None => Ok(match acc {
            Some(acc) => Arc::new(acc),
            None => Arc::new(BooleanArray::new(BooleanBuffer::new_set(num_rows), None)),
        }),
        // Compacted at least once: scatter the surviving original indices
        // (`live`, narrowed by any residual `acc`) into a full-length mask.
        Some(indices) => {
            let indices = match acc {
                Some(acc) => filter(&indices, &acc)?,
                None => indices,
            };
            let Some(indices) = indices.as_any().downcast_ref::<UInt32Array>() else {
                return internal_err!(
                    "adaptive filter: live row indices are not a UInt32Array"
                );
            };
            let mut builder = BooleanBufferBuilder::new(num_rows);
            builder.append_n(num_rows, false);
            for &idx in indices.values() {
                builder.set_bit(idx as usize, true);
            }
            Ok(Arc::new(BooleanArray::new(builder.finish(), None)))
        }
    }
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

/// Expected cost of evaluating the conjuncts in `order`, in nanoseconds per
/// input row: each conjunct's measured per-row cost weighted by the fraction of
/// rows expected to reach it (the product of the pass rates of the conjuncts
/// before it, treated as independent). Unmeasured conjuncts contribute nothing.
fn expected_cost_per_row(stats: &[ConjunctStats], order: &[usize]) -> f64 {
    let mut weight = 1.0_f64;
    let mut total = 0.0_f64;
    for &id in order {
        let (Some(cost), Some(pass)) = (stats[id].cost_per_row(), stats[id].pass_rate())
        else {
            continue;
        };
        total += weight * cost;
        weight *= pass;
    }
    total
}

#[cfg(test)]
mod tests {
    use super::*;

    use arrow::array::{Int32Array, Int64Array};
    use arrow::datatypes::{DataType, Field, Schema};
    use datafusion_expr::Operator;
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

    fn conjuncts(schema: &Arc<Schema>) -> Vec<Arc<dyn PhysicalExpr>> {
        split_conjunction(&predicate(schema))
            .into_iter()
            .map(Arc::clone)
            .collect()
    }

    fn passing_rows(mask: &ArrayRef) -> Vec<usize> {
        let mask = as_boolean_array(mask).unwrap();
        (0..mask.len())
            .filter(|&i| !mask.is_null(i) && mask.value(i))
            .collect()
    }

    fn stats(rows: u64, matched: u64, nanos: u64) -> ConjunctStats {
        ConjunctStats {
            rows,
            matched,
            nanos,
        }
    }

    /// `try_new` with a fresh, unshared registry.
    fn try_new(predicate: &Arc<dyn PhysicalExpr>) -> Option<AdaptiveConjunction> {
        AdaptiveConjunction::try_new(predicate, Arc::new(AdaptiveFilterShared::new()))
    }

    /// Seed the shared pool as if `batches` instrumented batches had already
    /// recorded `stats` — a stand-in for a mocked clock, giving scenario tests
    /// deterministic control over each conjunct's measured cost and
    /// selectivity.
    fn seed(shared: &AdaptiveFilterShared, stats: Vec<ConjunctStats>, batches: u64) {
        let mut inner = shared.inner.lock().unwrap();
        inner.stats = stats;
        inner.measured_batches = batches;
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
        assert_eq!(adaptive.order, vec![0, 1]);
        assert!(!adaptive.settled);
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

    /// Empty batches measure nothing, so they must not consume the warm-up:
    /// a stream fed only empty batches keeps learning instead of settling the
    /// written order on no evidence.
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

    #[test]
    fn expected_cost_weights_by_upstream_pass_rate() {
        // a: cost 1, pass 0.5 ; b: cost 10, pass 0.5
        let s = vec![stats(1000, 500, 1000), stats(1000, 500, 10_000)];
        // order [0,1]: 1 + 0.5*10 = 6
        assert!((expected_cost_per_row(&s, &[0, 1]) - 6.0).abs() < 1e-9);
        // order [1,0]: 10 + 0.5*1 = 10.5
        assert!((expected_cost_per_row(&s, &[1, 0]) - 10.5).abs() < 1e-9);
    }

    /// The compact-once loop returns exactly the rows the plain predicate
    /// keeps, in any order and whether or not compaction triggers.
    #[test]
    fn eval_conjuncts_matches_predicate_in_any_order() {
        let schema = schema();
        let cs = conjuncts(&schema);
        let p = predicate(&schema);

        // A batch where `b < 5` is rare (forces a compaction) and one where it
        // is common (no compaction).
        for b in [
            (0..100).map(|x| x % 50).collect::<Vec<_>>(), // b<5 rare
            (0..100).map(|x| x % 3).collect::<Vec<_>>(),  // b<5 common
        ] {
            let a: Vec<i32> = (0..100).collect();
            let rb = batch(&schema, a, b);
            let want = p.evaluate(&rb).unwrap().into_array(rb.num_rows()).unwrap();
            for order in [vec![0, 1], vec![1, 0]] {
                let got = eval_conjuncts(&cs, &order, &rb, None).unwrap();
                assert_eq!(passing_rows(&got), passing_rows(&want), "order {order:?}");
            }
        }
    }

    /// Across the warm-up boundary the mask must always equal the plain
    /// predicate's, before and after the order settles.
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

    /// A reorder is adopted only when materially cheaper; an already-good order
    /// is left untouched and runs the plain predicate (no compact-once).
    #[test]
    fn settle_keeps_order_when_not_materially_better() {
        // Two equally cheap, equally selective conjuncts: swapping cannot help,
        // so the written order stands and compact-once is not used.
        let s = vec![stats(1000, 500, 1000), stats(1000, 500, 1000)];
        let d = settle(&s);
        assert_eq!(d.order, vec![0, 1]);
        assert!(!d.compact);
    }

    #[test]
    fn settle_adopts_materially_cheaper_order_with_compaction() {
        // id 1 is far more selective and equally cheap: it should move first and
        // run through the compact-once loop.
        let s = vec![stats(1000, 900, 1000), stats(1000, 10, 1000)];
        let d = settle(&s);
        assert_eq!(d.order, vec![1, 0]);
        assert!(d.compact);
    }

    /// When the order does not change, the settled evaluator runs the plain
    /// predicate (compact-once is only used in service of a reorder), so an
    /// interchangeable conjunction costs exactly what the flag-off path costs.
    ///
    /// This one measures real timings on purpose and is still deterministic:
    /// both conjuncts pass the same ~96% of rows, and with a pass rate `p`
    /// above `1 - TIE_COST_FRACTION` the material-win guard
    /// (`c1 + p*c0 < 0.95 * (c0 + p*c1)`) cannot hold for any positive costs,
    /// so no timing can produce a reorder here.
    #[test]
    fn no_reorder_evaluates_plain_predicate() {
        let schema = schema();
        // Both conjuncts equally cheap and selective: nothing to reorder.
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
            !adaptive.compact,
            "interchangeable conjuncts stay on the plain predicate"
        );
    }

    /// Two streams sharing one registry settle the order together: the pooled
    /// warm-up is `WARMUP_BATCHES` total across both streams, and once one
    /// stream publishes the order the other adopts it on its next batch.
    ///
    /// The pool is seeded (see [`seed`]) two batches short of the warm-up, so
    /// the two real measured batches that complete it cannot move the ranking:
    /// their counts are several orders of magnitude smaller than the seeded
    /// ones. Deriving the ranking from the real `Instant` timings of two
    /// hundred-row batches instead would make the assertions below depend on
    /// the measured cost ratio staying inside the material-win guard, which a
    /// scheduling hiccup on a shared runner can flip.
    #[test]
    fn streams_pool_measurements_and_share_settled_order() {
        let schema = schema();
        let p = predicate(&schema); // `a > 2 AND b < 5`, written order [0, 1]
        let shared = Arc::new(AdaptiveFilterShared::new());
        // Conjunct 1 is far more selective, so promoting it is materially
        // cheaper. Two batches short of the warm-up: the two streams below
        // pool one measured batch each to complete it.
        seed(
            &shared,
            vec![
                stats(70_000_000, 63_000_000, 70_000_000), // pass 0.9, ~1ns/row
                stats(70_000_000, 700_000, 350_000_000),   // pass 0.01, ~5ns/row
            ],
            WARMUP_BATCHES - 2,
        );
        let mut s1 = AdaptiveConjunction::try_new(&p, Arc::clone(&shared)).unwrap();
        let mut s2 = AdaptiveConjunction::try_new(&p, Arc::clone(&shared)).unwrap();

        // `b < 5` (conjunct 1) is the selective one; drive both streams with
        // batches where it keeps 5 rows in 25 (20%, exactly the compact-once
        // threshold).
        let mk = |round: i32| {
            let base = round * 100;
            let a: Vec<i32> = (base..base + 100).collect();
            let b: Vec<i32> = (base..base + 100).map(|x| x.rem_euclid(25)).collect();
            batch(&schema, a, b)
        };

        // Alternate the two streams for `WARMUP_BATCHES` pooled batches; the
        // order settles partway through and both streams must end settled.
        for round in 0..(WARMUP_BATCHES as i32) {
            let rb = mk(round);
            for s in [&mut s1, &mut s2] {
                let got = s.evaluate(&rb).unwrap();
                let want = p.evaluate(&rb).unwrap().into_array(rb.num_rows()).unwrap();
                assert_eq!(passing_rows(&got), passing_rows(&want));
            }
        }

        assert!(shared.settled().is_some());
        // One more batch each lets a not-yet-settled stream adopt the decision.
        s1.evaluate(&mk(99)).unwrap();
        s2.evaluate(&mk(99)).unwrap();
        assert!(s1.settled && s2.settled);
        // The selective conjunct was promoted to the front for both, and the
        // reorder runs through the compact-once loop.
        assert_eq!(s1.order, vec![1, 0]);
        assert_eq!(s2.order, vec![1, 0]);
        assert!(s1.compact && s2.compact);
    }

    /// The reorder-adoption signal (`take_adopted_reorder`, what `FilterExec`
    /// counts into its `adaptive_reorders` metric) fires exactly once per
    /// stream: once for the stream that settles the order, and once for a
    /// stream that later picks up the decision that stream published.
    ///
    /// The per-conjunct costs are seeded (see [`seed`]) so the settle decision
    /// is a reorder deterministically, regardless of real timer values.
    #[test]
    fn adopted_reorder_signals_once_per_stream() {
        let schema = schema();
        let p = predicate(&schema); // `a > 2 AND b < 5`, written order [0, 1]
        let shared = Arc::new(AdaptiveFilterShared::new());
        // Conjunct 1 is far more selective; promoting it is materially cheaper,
        // so the warm-up settles on a reorder. One batch short of the warm-up.
        seed(
            &shared,
            vec![
                stats(70_000_000, 63_000_000, 70_000_000), // pass 0.9, ~1ns/row
                stats(70_000_000, 700_000, 350_000_000),   // pass 0.01, ~5ns/row
            ],
            WARMUP_BATCHES - 1,
        );
        let mut settler = AdaptiveConjunction::try_new(&p, Arc::clone(&shared)).unwrap();
        let mut adopter = AdaptiveConjunction::try_new(&p, Arc::clone(&shared)).unwrap();

        let a: Vec<i32> = (0..100).collect();
        let b: Vec<i32> = a.iter().map(|x| x.rem_euclid(25)).collect();
        let rb = batch(&schema, a, b);

        // Nothing adopted yet.
        assert!(!settler.take_adopted_reorder());

        // This batch completes the warm-up: `settler` settles on the reorder
        // and signals it, exactly once.
        settler.evaluate(&rb).unwrap();
        assert!(settler.compact);
        assert!(settler.take_adopted_reorder());
        settler.evaluate(&rb).unwrap();
        assert!(!settler.take_adopted_reorder());

        // `adopter` never measured its way to a decision: it picks up the
        // published one on its next batch, and signals that once too.
        adopter.evaluate(&rb).unwrap();
        assert!(adopter.compact);
        assert!(adopter.take_adopted_reorder());
        adopter.evaluate(&rb).unwrap();
        assert!(!adopter.take_adopted_reorder());
    }

    /// Settling on the written order is indistinguishable from the feature
    /// being off, so it must not signal a reorder.
    #[test]
    fn settling_without_reorder_signals_nothing() {
        let schema = schema();
        let p = predicate(&schema);
        let shared = Arc::new(AdaptiveFilterShared::new());
        // Identical cost and selectivity: no order can be materially cheaper.
        seed(
            &shared,
            vec![
                stats(70_000_000, 35_000_000, 70_000_000),
                stats(70_000_000, 35_000_000, 70_000_000),
            ],
            WARMUP_BATCHES - 1,
        );
        let mut adaptive = AdaptiveConjunction::try_new(&p, Arc::clone(&shared)).unwrap();

        let a: Vec<i32> = (0..100).collect();
        let rb = batch(&schema, a.clone(), a);
        adaptive.evaluate(&rb).unwrap();
        assert!(adaptive.settled && !adaptive.compact);
        assert!(!adaptive.take_adopted_reorder());
    }

    /// End-to-end input/output-contract scenario: feed batches, observe the
    /// strategy used for each one alongside the masks.
    ///
    /// Per-conjunct costs are injected by seeding the shared pool with
    /// synthetic measurements (the stand-in for a mocked clock): conjunct 0 is
    /// cheap but unselective, conjunct 1 is expensive but very selective, so
    /// the warm-up must settle on promoting conjunct 1 and run the reorder
    /// through the compact-once loop. The seeded magnitudes dominate the one
    /// real measured batch, so the decision is deterministic regardless of
    /// real timer values.
    #[test]
    fn scenario_measure_batches_then_settle_on_reorder() {
        let schema = schema();
        let p = predicate(&schema); // `a > 2 AND b < 5`, written order [0, 1]
        let shared = Arc::new(AdaptiveFilterShared::new());
        // One batch short of the warm-up: the next measured batch settles.
        seed(
            &shared,
            vec![
                stats(70_000_000, 63_000_000, 70_000_000), // pass 0.9, ~1ns/row
                stats(70_000_000, 700_000, 350_000_000),   // pass 0.01, ~5ns/row
            ],
            WARMUP_BATCHES - 1,
        );
        let mut adaptive = AdaptiveConjunction::try_new(&p, Arc::clone(&shared)).unwrap();

        let mut trace = vec![];
        for round in 0..3 {
            let base = round * 100;
            let a: Vec<i32> = (base..base + 100).collect();
            let b: Vec<i32> = (base..base + 100).map(|x| x.rem_euclid(25)).collect();
            let rb = batch(&schema, a, b);
            let (got, strategy) = adaptive.evaluate_traced(&rb).unwrap();
            trace.push(format!("{strategy:?}"));
            let want = p.evaluate(&rb).unwrap().into_array(rb.num_rows()).unwrap();
            assert_eq!(passing_rows(&got), passing_rows(&want), "round {round}");
        }

        // Batch 1 completes the warm-up and settles; batches 2+ run the
        // adopted reorder (selective conjunct promoted to the front) through
        // the compact-once loop.
        assert_eq!(
            trace,
            vec!["Measure", "Reordered([1, 0])", "Reordered([1, 0])"]
        );
    }

    /// Contract scenario for the no-win case: interchangeable conjuncts settle
    /// on the written fused predicate (as if the feature were off), never the
    /// compact-once loop.
    #[test]
    fn scenario_measure_batches_then_settle_on_fused() {
        let schema = schema();
        let p = predicate(&schema);
        let shared = Arc::new(AdaptiveFilterShared::new());
        // Identical cost and selectivity: no order can be materially cheaper.
        // The seeded magnitudes dominate the one real measured batch, so even
        // if real timings nudge the ranking, the 5% material-win guard holds.
        seed(
            &shared,
            vec![
                stats(70_000_000, 35_000_000, 70_000_000),
                stats(70_000_000, 35_000_000, 70_000_000),
            ],
            WARMUP_BATCHES - 1,
        );
        let mut adaptive = AdaptiveConjunction::try_new(&p, Arc::clone(&shared)).unwrap();

        let mut trace = vec![];
        for round in 0..3 {
            let base = round * 100;
            let a: Vec<i32> = (base..base + 100).collect();
            let b: Vec<i32> = (base..base + 100).collect();
            let rb = batch(&schema, a, b);
            let (got, strategy) = adaptive.evaluate_traced(&rb).unwrap();
            trace.push(format!("{strategy:?}"));
            let want = p.evaluate(&rb).unwrap().into_array(rb.num_rows()).unwrap();
            assert_eq!(passing_rows(&got), passing_rows(&want), "round {round}");
        }

        assert_eq!(trace, vec!["Measure", "Fused", "Fused"]);
        assert_eq!(adaptive.order, vec![0, 1]);
    }

    /// The pooled registry is sized lazily by the first measured batch (the
    /// conjunct count is not known to `AdaptiveFilterShared`, which is built
    /// before the predicate is split), and the counts of that first batch land
    /// in it.
    #[test]
    fn first_measured_batch_initialises_the_shared_pool() {
        let schema = schema();
        let p = predicate(&schema); // `a > 2 AND b < 5`
        let shared = Arc::new(AdaptiveFilterShared::new());
        assert!(shared.inner.lock().unwrap().stats.is_empty());
        let mut adaptive = AdaptiveConjunction::try_new(&p, Arc::clone(&shared)).unwrap();
        // Empty batches measure nothing, so the pool is still unsized after one.
        adaptive.evaluate(&batch(&schema, vec![], vec![])).unwrap();
        assert!(shared.inner.lock().unwrap().stats.is_empty());

        let a: Vec<i32> = (0..10).collect();
        adaptive.evaluate(&batch(&schema, a.clone(), a)).unwrap();

        let inner = shared.inner.lock().unwrap();
        assert_eq!(inner.stats.len(), 2, "sized to the conjunct count");
        assert_eq!(inner.measured_batches, 1);
        // `a > 2` keeps 7 of 10 rows: too many to compact, so `b < 5` is
        // evaluated on all 10 rows too and keeps 5 of them.
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

    /// This is the behaviour the config option's doc warns about: adopting a
    /// reorder can introduce an error the written order avoided.
    ///
    /// `b <> 0 AND 1 / b > 2` on data where `b <> 0` holds for 15% of the rows.
    /// The written fused `BinaryExpr` `AND` pre-selects (its own threshold is
    /// also 20%), so `1 / b` never sees a zero and the flag-off query succeeds.
    /// Once the conjuncts are reordered, `1 / b > 2` runs first — on every row,
    /// zeros included — and integer division by zero is an error.
    #[test]
    fn adopted_reorder_can_introduce_a_divide_by_zero() {
        let schema = int64_schema();
        let (non_zero, divide) = divide_by_zero_conjuncts(&schema);
        // Written order [0, 1] = [`b <> 0`, `1 / b > 2`].
        let p = binary(non_zero, Operator::And, divide, &schema).unwrap();
        let a: Vec<i64> = (0..100).collect();
        let b: Vec<i64> = (0..100).map(|i| i64::from(i < 15)).collect();
        let rb = int64_batch(&schema, a, b);

        // Flag off: the fused predicate pre-selects on `b <> 0` (15 of 100
        // rows, within its 20% threshold) and succeeds.
        assert!(p.evaluate(&rb).is_ok(), "flag-off evaluation must succeed");

        let shared = Arc::new(AdaptiveFilterShared::new());
        // `1 / b > 2` (conjunct 1) seeded as cheap and very selective, so the
        // warm-up settles on promoting it. One batch short of the warm-up.
        seed(
            &shared,
            vec![
                stats(70_000_000, 63_000_000, 70_000_000), // pass 0.9, ~1ns/row
                stats(70_000_000, 700_000, 70_000_000),    // pass 0.01, ~1ns/row
            ],
            WARMUP_BATCHES - 1,
        );
        let mut adaptive = AdaptiveConjunction::try_new(&p, Arc::clone(&shared)).unwrap();

        // The settling batch is still measured in the written order, whose
        // compaction on `b <> 0` also keeps `1 / b` away from the zeros.
        adaptive.evaluate(&rb).unwrap();
        assert_eq!(adaptive.order, vec![1, 0]);
        assert!(adaptive.compact);

        // The next batch runs the adopted reorder, and errors.
        let err = adaptive.evaluate(&rb).unwrap_err().to_string();
        assert!(err.contains("Divide by zero"), "unexpected error: {err}");
    }

    /// The mirror of the case above: an error the written fused order *does*
    /// raise, which the reordered compact-once evaluation avoids.
    ///
    /// `1 / b > 2 AND a < 10`, with `b = 0` on exactly the rows `a < 10`
    /// discards. The fused `AND` evaluates its left side on every row and
    /// errors; the adopted order runs `a < 10` first, compacts to its 10
    /// survivors (all with `b = 1`), and never divides by zero.
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
        // Written order [0, 1] = [`1 / b > 2`, `a < 10`].
        let p = binary(divide, Operator::And, selective, &schema).unwrap();

        let shared = Arc::new(AdaptiveFilterShared::new());
        // Conjunct 1 (`a < 10`) seeded as cheap and very selective, conjunct 0
        // as expensive and unselective, so the warm-up promotes conjunct 1.
        seed(
            &shared,
            vec![
                stats(70_000_000, 63_000_000, 350_000_000), // pass 0.9, ~5ns/row
                stats(70_000_000, 700_000, 70_000_000),     // pass 0.01, ~1ns/row
            ],
            WARMUP_BATCHES - 1,
        );
        let mut adaptive = AdaptiveConjunction::try_new(&p, Arc::clone(&shared)).unwrap();

        // Settle on a batch with no zeros at all, so the warm-up itself (which
        // evaluates in the written order) cannot hit the error.
        let a: Vec<i64> = (0..100).collect();
        adaptive
            .evaluate(&int64_batch(&schema, a.clone(), vec![1; 100]))
            .unwrap();
        assert_eq!(adaptive.order, vec![1, 0]);

        // Now a batch whose `b` is zero on every row `a < 10` discards.
        let b: Vec<i64> = (0..100).map(|i| i64::from(i < 10)).collect();
        let rb = int64_batch(&schema, a, b);

        // The written fused order divides by zero...
        let err = p.evaluate(&rb).unwrap_err().to_string();
        assert!(err.contains("Divide by zero"), "unexpected error: {err}");
        // ...while the adopted order compacts `1 / b > 2` down to the rows
        // `a < 10` kept, none of which is zero.
        let got = adaptive.evaluate(&rb).unwrap();
        assert!(passing_rows(&got).is_empty(), "1 / 1 > 2 is false");
    }
}
