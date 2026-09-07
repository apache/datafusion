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
//! being learned, the written order is handed to [`BinaryExpr`] with every
//! conjunct wrapped in a [`MeasuredConjunct`]; `BinaryExpr` evaluates and
//! pre-selects as it would for the plain predicate, so each conjunct is
//! measured on the population it would really see in that position.
//!
//! Once the order settles the wrappers are gone: the settled order — the
//! written one if the warm-up found nothing materially better, otherwise the
//! learned one — is materialised once as a right-nested `AND` chain,
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
//! - The decision is one-shot: a misjudged reorder, or drifting data, is kept
//!   for the rest of the query.
//!
//! See <https://github.com/apache/datafusion/pull/22698>.

use std::fmt;
use std::fmt::Formatter;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::atomic::{AtomicU64, Ordering::Relaxed};

use crate::metrics::Count;
use arrow::array::ArrayRef;
use arrow::datatypes::{DataType, Schema};
use arrow::record_batch::RecordBatch;
use datafusion_common::Result;
use datafusion_common::cast::as_boolean_array;
use datafusion_common::instant::Instant;
use datafusion_expr::{ColumnarValue, Operator};
use datafusion_physical_expr::PhysicalExpr;
use datafusion_physical_expr::expressions::BinaryExpr;
use datafusion_physical_expr::utils::split_conjunction;
use datafusion_physical_expr_common::physical_expr::is_volatile;

/// Batches measured before the order is settled.
const WARMUP_BATCHES: u64 = 8;

/// A candidate order is adopted only if its expected cost is below
/// `(1 - TIE_COST_FRACTION)` of the written order's.
const TIE_COST_FRACTION: f64 = 0.05;

/// Per-conjunct counts over the warm-up, on exactly the rows that reached it.
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
    /// Pool another stream's counts into this one.
    fn merge(&mut self, other: &Self) {
        self.rows += other.rows;
        self.matched += other.matched;
        self.nanos += other.nanos;
    }

    /// Fraction of rows that pass, or `None` if never evaluated on any row.
    fn pass_rate(&self) -> Option<f64> {
        (self.rows > 0).then(|| self.matched as f64 / self.rows as f64)
    }

    /// Per-row cost in nanoseconds, or `None` if never evaluated. Time is
    /// clamped to 1ns so "too cheap to measure" ranks as very cheap.
    fn cost_per_row(&self) -> Option<f64> {
        (self.rows > 0).then(|| self.nanos.max(1) as f64 / self.rows as f64)
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
            .map(|&(rows, matched, nanos)| ConjunctStats {
                rows,
                matched,
                nanos,
            })
            .collect();
        inner.measured_batches = WARMUP_BATCHES - 1;
    }
}

/// A conjunct that records the rows it was handed, the rows it kept and the
/// time it took, returning its result unchanged (nulls included). Everything
/// else delegates to the wrapped conjunct.
#[derive(Debug)]
struct MeasuredConjunct {
    inner: Arc<dyn PhysicalExpr>,
    /// Rows handed to the conjunct since the last [`take`](Self::take).
    rows: AtomicU64,
    /// Of those, the non-null `true`s.
    matched: AtomicU64,
    /// Time spent inside the conjunct over those rows, in nanoseconds.
    nanos: AtomicU64,
}

impl MeasuredConjunct {
    fn new(inner: Arc<dyn PhysicalExpr>) -> Self {
        Self {
            inner,
            rows: AtomicU64::new(0),
            matched: AtomicU64::new(0),
            nanos: AtomicU64::new(0),
        }
    }

    /// Drain the counters (per stream and uncontended, hence `Relaxed`).
    fn take(&self) -> ConjunctStats {
        ConjunctStats {
            rows: self.rows.swap(0, Relaxed),
            matched: self.matched.swap(0, Relaxed),
            nanos: self.nanos.swap(0, Relaxed),
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
        let matched = as_boolean_array(&array)?.true_count() as u64;

        self.rows.fetch_add(rows as u64, Relaxed);
        self.matched.fetch_add(matched, Relaxed);
        self.nanos.fetch_add(nanos, Relaxed);

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
    /// The split conjuncts, in written order.
    conjuncts: Vec<Arc<dyn PhysicalExpr>>,
    /// Measurements and the settled decision, shared by every partition stream.
    shared: Arc<AdaptiveFilterShared>,
    /// The written order as a right-nested `AND` chain over the wrappers.
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
        let order: Vec<usize> = (0..conjuncts.len()).collect();
        let measured: Vec<Arc<MeasuredConjunct>> = conjuncts
            .iter()
            .map(|c| Arc::new(MeasuredConjunct::new(Arc::clone(c))))
            .collect();
        let wrapped: Vec<Arc<dyn PhysicalExpr>> = measured
            .iter()
            .map(|m| Arc::clone(m) as Arc<dyn PhysicalExpr>)
            .collect();
        let warmup_predicate = right_nested_conjunction(&wrapped, &order);
        Some(Self {
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
        let decision = settle(&inner.stats, &self.conjuncts);
        inner.settled = Some(decision.clone());
        drop(inner);
        self.adopt(decision);
    }
}

/// Rank by effectiveness and adopt the ranking only if it is materially
/// cheaper than the written order; either way, build the result as a
/// right-nested `AND` chain.
fn settle(stats: &[ConjunctStats], conjuncts: &[Arc<dyn PhysicalExpr>]) -> Settled {
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
            predicate: right_nested_conjunction(conjuncts, &identity),
            reordered: false,
        }
    }
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
/// cost weighted by the product of the pass rates before it (assumed
/// independent). Unmeasured conjuncts contribute nothing.
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

    fn stats(rows: u64, matched: u64, nanos: u64) -> ConjunctStats {
        ConjunctStats {
            rows,
            matched,
            nanos,
        }
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

    #[test]
    fn expected_cost_weights_by_upstream_pass_rate() {
        // a: cost 1, pass 0.5 ; b: cost 10, pass 0.5
        let s = vec![stats(1000, 500, 1000), stats(1000, 500, 10_000)];
        // order [0,1]: 1 + 0.5*10 = 6
        assert!((expected_cost_per_row(&s, &[0, 1]) - 6.0).abs() < 1e-9);
        // order [1,0]: 10 + 0.5*1 = 10.5
        assert!((expected_cost_per_row(&s, &[1, 0]) - 10.5).abs() < 1e-9);
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

    /// An already-good order is kept, as a right-nested chain.
    #[test]
    fn settle_keeps_order_when_not_materially_better() {
        let schema = schema();
        let p = predicate(&schema);
        // Equally cheap and selective: swapping cannot help.
        let s = vec![stats(1000, 500, 1000), stats(1000, 500, 1000)];
        let cs = split(&p);
        let d = settle(&s, &cs);
        assert!(!d.reordered);
        assert_chain(&d.predicate, &cs, &[0, 1]);
    }

    #[test]
    fn settle_adopts_materially_cheaper_order() {
        let schema = schema();
        let p = predicate(&schema);
        let cs = split(&p);
        // id 1 is far more selective at equal cost: it moves first.
        let s = vec![stats(1000, 900, 1000), stats(1000, 10, 1000)];
        let d = settle(&s, &cs);
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
        let d = settle(&s, &cs);
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

    /// A kept written order runs as a right-nested chain. Real timings are
    /// used on purpose: with both pass rates above `1 - TIE_COST_FRACTION` the
    /// guard `c1 + p*c0 < 0.95 * (c0 + p*c1)` cannot hold for any costs.
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
        assert_chain(&adaptive.settled_predicate, &split(&p), &[0, 1]);
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
        let cs = split(&p);
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
            assert_chain(&adaptive.settled_predicate, &cs, &[0, 1]);
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
