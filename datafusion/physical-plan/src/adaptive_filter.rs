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
//! predicate is evaluated as-is, so a conjunction that does not benefit from
//! reordering pays no compact-once overhead and behaves exactly as it would
//! with the feature off.
//!
//! ## How it shares
//!
//! A `FilterExec` is split across many partition streams, each seeing only a
//! slice of the data. Measurements are pooled into a shared
//! [`AdaptiveFilterShared`] so the streams learn as one: the first stream to
//! accumulate enough samples settles the order and publishes it, and the others
//! adopt it (one relaxed atomic load per batch) without each re-paying the
//! warm-up — which is what makes the win materialise when each stream is only a
//! handful of batches long.
//!
//! It is **off by default**
//! (`datafusion.execution.adaptive_filter_reordering`) and never changes query
//! results: a conjunction's value is independent of evaluation order. Predicates
//! containing volatile expressions are never reordered (their observable side
//! effects depend on order).
//!
//! This is the core of the adaptive evaluator. Further policies (drift
//! re-measurement, confidence-interval statistics, A/B-validated adoption for
//! the cases a cost model cannot separate) can build on top of it.

use std::sync::Arc;
use std::sync::Mutex;
use std::sync::atomic::{AtomicU64, Ordering};

use arrow::array::{Array, ArrayRef, BooleanArray, BooleanBufferBuilder, UInt32Array};
use arrow::buffer::BooleanBuffer;
use arrow::compute::kernels::boolean::and;
use arrow::compute::{filter, filter_record_batch, prep_null_mask_filter};
use arrow::record_batch::RecordBatch;
use datafusion_common::Result;
use datafusion_common::cast::as_boolean_array;
use datafusion_common::instant::Instant;
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

    /// Per-row evaluation cost in nanoseconds, or `None` if unmeasured.
    fn cost_per_row(&self) -> Option<f64> {
        (self.rows > 0 && self.nanos > 0).then(|| self.nanos as f64 / self.rows as f64)
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
    /// `0` until an order is published; bumped once when the first stream
    /// settles. Streams poll it with one relaxed atomic load per batch.
    epoch: AtomicU64,
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
    /// Shared epoch this stream has caught up to.
    epoch_seen: u64,
    /// Whether the order is settled (frozen): this stream no longer measures.
    settled: bool,
}

impl AdaptiveConjunction {
    /// Build an adaptive evaluator for `predicate`, or `None` if adaptive
    /// reordering does not apply:
    ///
    /// - `enabled` is false (the config flag is off);
    /// - the predicate has fewer than two `AND` conjuncts (nothing to reorder);
    /// - any conjunct is volatile (reordering could change side effects).
    ///
    /// `shared` is the state common to all partition streams of the owning
    /// `FilterExec`.
    pub(crate) fn try_new(
        predicate: &Arc<dyn PhysicalExpr>,
        enabled: bool,
        shared: Arc<AdaptiveFilterShared>,
    ) -> Option<Self> {
        if !enabled {
            return None;
        }
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
            epoch_seen: 0,
            settled: false,
        })
    }

    /// Evaluate the conjunction against `batch`, returning the boolean mask
    /// (over the batch's rows) of rows that passed every conjunct.
    ///
    /// Until the order settles, each batch is measured and its counts pooled
    /// into the shared registry; a stream adopts the settled order another
    /// stream published as soon as it sees the epoch advance.
    pub(crate) fn evaluate(&mut self, batch: &RecordBatch) -> Result<ArrayRef> {
        // Adopt a settled order another stream published since we last looked:
        // one relaxed atomic load per batch, a lock only on the transition.
        if !self.settled {
            let epoch = self.shared.epoch.load(Ordering::Acquire);
            if epoch != self.epoch_seen {
                self.epoch_seen = epoch;
                if let Some(decision) = self.shared.settled() {
                    self.adopt(decision);
                }
            }
        }
        if self.settled {
            return self.evaluate_settled(batch);
        }

        // Measure this batch into a local accumulator, then pool it.
        let mut local = vec![ConjunctStats::default(); self.conjuncts.len()];
        let result =
            eval_conjuncts(&self.conjuncts, &self.order, batch, Some(&mut local))?;
        self.pool_and_maybe_settle(&local);
        Ok(result)
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
    }

    /// Merge this batch's measurements into the shared pool and, once enough
    /// batches have accrued across all streams, decide and publish the order.
    fn pool_and_maybe_settle(&mut self, local: &[ConjunctStats]) {
        let mut inner = self.shared.inner.lock().expect("poisoned");
        if inner.stats.len() != local.len() {
            inner.stats = vec![ConjunctStats::default(); local.len()];
        }
        for (s, l) in inner.stats.iter_mut().zip(local) {
            s.merge(l);
        }
        inner.measured_batches += 1;
        if inner.settled.is_some() || inner.measured_batches < WARMUP_BATCHES {
            return;
        }
        let decision = settle(&inner.stats);
        inner.settled = Some(decision.clone());
        drop(inner);
        self.adopt(decision);
        self.shared.epoch.fetch_add(1, Ordering::Release);
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
            let indices = indices
                .as_any()
                .downcast_ref::<UInt32Array>()
                .expect("u32 live");
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

    use arrow::array::Int32Array;
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
    fn try_new(
        predicate: &Arc<dyn PhysicalExpr>,
        enabled: bool,
    ) -> Option<AdaptiveConjunction> {
        AdaptiveConjunction::try_new(
            predicate,
            enabled,
            Arc::new(AdaptiveFilterShared::new()),
        )
    }

    #[test]
    fn disabled_is_not_adaptive() {
        let schema = schema();
        assert!(try_new(&predicate(&schema), false).is_none());
    }

    #[test]
    fn single_conjunct_is_not_adaptive() {
        let schema = schema();
        let p =
            binary(col("a", &schema).unwrap(), Operator::Gt, lit(2i32), &schema).unwrap();
        assert!(try_new(&p, true).is_none());
    }

    #[test]
    fn two_conjuncts_are_adaptive() {
        let schema = schema();
        let adaptive = try_new(&predicate(&schema), true).unwrap();
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
        let mut adaptive = try_new(&p, true).unwrap();

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
    #[test]
    fn no_reorder_evaluates_plain_predicate() {
        let schema = schema();
        // Both conjuncts equally cheap and selective: nothing to reorder.
        let left =
            binary(col("a", &schema).unwrap(), Operator::Gt, lit(2i32), &schema).unwrap();
        let right =
            binary(col("b", &schema).unwrap(), Operator::Gt, lit(2i32), &schema).unwrap();
        let p = binary(left, Operator::And, right, &schema).unwrap();
        let mut adaptive = try_new(&p, true).unwrap();

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
    #[test]
    fn streams_pool_measurements_and_share_settled_order() {
        let schema = schema();
        let p = predicate(&schema);
        let shared = Arc::new(AdaptiveFilterShared::new());
        let mut s1 = AdaptiveConjunction::try_new(&p, true, Arc::clone(&shared)).unwrap();
        let mut s2 = AdaptiveConjunction::try_new(&p, true, Arc::clone(&shared)).unwrap();

        // `b < 5` (conjunct 1) is the selective one; drive both streams with
        // batches where it keeps ~1 row in 25.
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
        // One more batch each lets a not-yet-settled stream adopt the epoch.
        s1.evaluate(&mk(99)).unwrap();
        s2.evaluate(&mk(99)).unwrap();
        assert!(s1.settled && s2.settled);
        // The selective conjunct was promoted to the front for both, and the
        // reorder runs through the compact-once loop.
        assert_eq!(s1.order, vec![1, 0]);
        assert_eq!(s2.order, vec![1, 0]);
        assert!(s1.compact && s2.compact);
    }
}
