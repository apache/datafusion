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

//! Adaptive conjunct ordering for [`FilterExec`], controlled by
//! `datafusion.execution.adaptive_filter_reordering`.
//!
//! The order of the conjuncts of an `AND` predicate is important. The `AND`
//! of [`BinaryExpr`] evaluates its right side only on the rows that its left
//! side keeps, if the left side keeps few rows (see
//! [`PRE_SELECTION_THRESHOLD`]). For example, in
//! `regexp_like(s, 'a') AND regexp_like(s, 'rare')`, the second conjunct
//! removes most rows. If it is first, the first conjunct is evaluated only on
//! the few rows that remain.
//!
//! [`ConjunctOrder`] finds a better order for one stream:
//!
//! 1. For the first [`WARMUP_BATCHES`] batches, it evaluates the predicate as
//!    written. Each conjunct is wrapped in a [`MeasuredConjunct`] that counts
//!    the rows in, the rows out and the evaluation time (a [`FilterCost`],
//!    measured with a [`Clock`]). The shape of the `AND` tree does not
//!    change, thus each
//!    conjunct sees the same rows as without the measurement.
//! 2. Then it ranks the conjuncts by rows removed per nanosecond, and
//!    estimates the cost of the new order and of the written order. It uses
//!    the new order only if its cost is at least [`MIN_GAIN`] smaller.
//! 3. It evaluates the new order as an ordinary right-nested `AND` chain of
//!    [`BinaryExpr`], or the written predicate unchanged.
//!
//! The decision is made one time. The state is local to the stream, thus
//! executions and partitions do not share it.
//!
//! [`FilterExec`]: crate::filter::FilterExec

use std::fmt::{self, Debug, Display, Formatter};
use std::hash::{Hash, Hasher};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering::Relaxed};

use arrow::array::{Array, RecordBatch};
use arrow::datatypes::{FieldRef, Schema};
use datafusion_common::Result;
use datafusion_common::cast::as_boolean_array;
use datafusion_expr::{ColumnarValue, Operator};
use datafusion_physical_expr::PhysicalExpr;
use datafusion_physical_expr::expressions::{BinaryExpr, PRE_SELECTION_THRESHOLD};
use datafusion_physical_expr::filter_stats::{Clock, FilterCost};
use datafusion_physical_expr::split_conjunction;
use datafusion_physical_expr_common::physical_expr::is_volatile;

/// Number of non-empty batches that a stream measures before it decides.
const WARMUP_BATCHES: usize = 8;

/// A new order must be at least this fraction cheaper than the written order.
const MIN_GAIN: f64 = 0.05;

/// The measurements of one conjunct.
#[derive(Debug, Default, Clone, Copy, PartialEq)]
struct ConjunctStats {
    /// Rows in, rows out (`true`; `null` does not pass) and evaluation time.
    cost: FilterCost,
    /// Rows that `AND` gives to the conjuncts after this one, if this one
    /// is the left side. See [`rows_passed_on`].
    rows_passed_on: u64,
}

impl ConjunctStats {
    /// Nanoseconds for each row, or `None` if the conjunct was not
    /// evaluated.
    fn cost_per_row(&self) -> Option<f64> {
        self.cost.nanos_per_row()
    }

    /// The rank key: removed rows per nanosecond, see
    /// [`FilterCost::rows_removed_per_nano`]. `None` if the conjunct was not
    /// evaluated.
    fn rank_key(&self) -> Option<f64> {
        self.cost.rows_removed_per_nano()
    }

    /// Fraction of the rows that the conjuncts after this one see.
    fn passed_on_fraction(&self) -> f64 {
        if self.cost.rows_in == 0 {
            1.0
        } else {
            self.rows_passed_on as f64 / self.cost.rows_in as f64
        }
    }
}

/// Rows that the `AND` of [`BinaryExpr`] gives to its right side, when its
/// left side returns `rows_out` passing rows and `null_count` nulls out of
/// `rows_in` rows. This is the same rule as `check_short_circuit` in
/// `binary.rs`: a left side with nulls never pre-selects, a left side with
/// no passing row stops the evaluation, and a left side that keeps at most
/// [`PRE_SELECTION_THRESHOLD`] of the rows pre-selects them.
fn rows_passed_on(rows_in: usize, rows_out: usize, null_count: usize) -> usize {
    if rows_in == 0 || null_count > 0 {
        rows_in
    } else if rows_out == 0 {
        0
    } else if rows_out as f32 / rows_in as f32 <= PRE_SELECTION_THRESHOLD {
        rows_out
    } else {
        rows_in
    }
}

/// Wraps a conjunct and records [`ConjunctStats`] for it. The result of the
/// conjunct does not change.
#[derive(Debug)]
struct MeasuredConjunct {
    inner: Arc<dyn PhysicalExpr>,
    clock: Arc<dyn Clock>,
    rows_in: AtomicU64,
    rows_out: AtomicU64,
    rows_passed_on: AtomicU64,
    nanos: AtomicU64,
}

impl MeasuredConjunct {
    fn new(inner: Arc<dyn PhysicalExpr>, clock: Arc<dyn Clock>) -> Self {
        Self {
            inner,
            clock,
            rows_in: AtomicU64::new(0),
            rows_out: AtomicU64::new(0),
            rows_passed_on: AtomicU64::new(0),
            nanos: AtomicU64::new(0),
        }
    }

    fn stats(&self) -> ConjunctStats {
        ConjunctStats {
            cost: FilterCost {
                rows_in: self.rows_in.load(Relaxed),
                rows_out: self.rows_out.load(Relaxed),
                nanos: self.nanos.load(Relaxed),
            },
            rows_passed_on: self.rows_passed_on.load(Relaxed),
        }
    }
}

impl PartialEq for MeasuredConjunct {
    fn eq(&self, other: &Self) -> bool {
        self.inner.eq(&other.inner)
    }
}

impl Eq for MeasuredConjunct {}

impl Hash for MeasuredConjunct {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.inner.hash(state);
    }
}

impl Display for MeasuredConjunct {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.inner)
    }
}

impl PhysicalExpr for MeasuredConjunct {
    fn return_field(&self, input_schema: &Schema) -> Result<FieldRef> {
        self.inner.return_field(input_schema)
    }

    fn evaluate(&self, batch: &RecordBatch) -> Result<ColumnarValue> {
        let rows_in = batch.num_rows();
        let start = self.clock.now_nanos();
        let result = self.inner.evaluate(batch)?.into_array(rows_in)?;
        let nanos = self.clock.now_nanos().saturating_sub(start);
        let mask = as_boolean_array(&result)?;
        let rows_out = mask.true_count();
        let passed_on = rows_passed_on(rows_in, rows_out, mask.null_count());
        self.rows_in.fetch_add(rows_in as u64, Relaxed);
        self.rows_out.fetch_add(rows_out as u64, Relaxed);
        self.rows_passed_on.fetch_add(passed_on as u64, Relaxed);
        self.nanos.fetch_add(nanos, Relaxed);
        Ok(ColumnarValue::Array(result))
    }

    fn children(&self) -> Vec<&Arc<dyn PhysicalExpr>> {
        vec![&self.inner]
    }

    fn with_new_children(
        self: Arc<Self>,
        mut children: Vec<Arc<dyn PhysicalExpr>>,
    ) -> Result<Arc<dyn PhysicalExpr>> {
        datafusion_common::assert_eq_or_internal_err!(
            children.len(),
            1,
            "MeasuredConjunct: expected 1 child"
        );
        Ok(Arc::new(Self::new(
            children.remove(0),
            Arc::clone(&self.clock),
        )))
    }

    fn fmt_sql(&self, f: &mut Formatter<'_>) -> fmt::Result {
        self.inner.fmt_sql(f)
    }
}

/// The state of a [`ConjunctOrder`].
#[derive(Debug)]
enum State {
    /// Evaluates the written predicate with measured conjuncts.
    Measuring {
        /// The written predicate, with each conjunct wrapped.
        predicate: Arc<dyn PhysicalExpr>,
        /// The wrapped conjuncts, in the order of [`split_conjunction`].
        measured: Vec<Arc<MeasuredConjunct>>,
        /// Non-empty batches measured so far.
        batches: usize,
    },
    /// The decision is made.
    Decided {
        predicate: Arc<dyn PhysicalExpr>,
        /// The new order (indexes into the written conjuncts), or `None` if
        /// the written predicate is kept.
        order: Option<Vec<usize>>,
    },
}

/// Evaluates an `AND` predicate for one stream, and changes the order of its
/// conjuncts when that is clearly faster. See the [module
/// documentation](self).
#[derive(Debug)]
pub(crate) struct ConjunctOrder {
    written: Arc<dyn PhysicalExpr>,
    state: State,
}

impl ConjunctOrder {
    /// Returns `None` if the order of the conjuncts of `predicate` cannot
    /// change: `predicate` has fewer than two conjuncts, or a conjunct is
    /// volatile.
    pub(crate) fn try_new(
        predicate: &Arc<dyn PhysicalExpr>,
        clock: &Arc<dyn Clock>,
    ) -> Option<Self> {
        let conjuncts = split_conjunction(predicate);
        if conjuncts.len() < 2 || conjuncts.iter().any(|c| is_volatile(c)) {
            return None;
        }
        let mut measured = Vec::with_capacity(conjuncts.len());
        let wrapped = wrap_conjuncts(predicate, clock, &mut measured);
        Some(Self {
            written: Arc::clone(predicate),
            state: State::Measuring {
                predicate: wrapped,
                measured,
                batches: 0,
            },
        })
    }

    /// Evaluates the predicate on `batch`.
    pub(crate) fn evaluate(&mut self, batch: &RecordBatch) -> Result<ColumnarValue> {
        match &mut self.state {
            State::Decided { predicate, .. } => predicate.evaluate(batch),
            State::Measuring {
                predicate,
                measured,
                batches,
            } => {
                let result = predicate.evaluate(batch)?;
                if batch.num_rows() > 0 {
                    *batches += 1;
                }
                if *batches >= WARMUP_BATCHES {
                    let stats: Vec<_> = measured.iter().map(|m| m.stats()).collect();
                    self.state = decide(&self.written, &stats);
                }
                Ok(result)
            }
        }
    }

    /// The new order, if the decision is made and it changed the order.
    pub(crate) fn new_order(&self) -> Option<&[usize]> {
        match &self.state {
            State::Decided { order, .. } => order.as_deref(),
            State::Measuring { .. } => None,
        }
    }

    /// True while the stream measures the conjuncts.
    pub(crate) fn is_measuring(&self) -> bool {
        matches!(self.state, State::Measuring { .. })
    }
}

/// Returns `predicate` with each conjunct of its root `AND` chain wrapped in
/// a [`MeasuredConjunct`]. The shape of the `AND` tree does not change. The
/// wrappers are added to `measured` in the order of [`split_conjunction`].
fn wrap_conjuncts(
    predicate: &Arc<dyn PhysicalExpr>,
    clock: &Arc<dyn Clock>,
    measured: &mut Vec<Arc<MeasuredConjunct>>,
) -> Arc<dyn PhysicalExpr> {
    if let Some(binary) = predicate.downcast_ref::<BinaryExpr>()
        && *binary.op() == Operator::And
    {
        let left = wrap_conjuncts(binary.left(), clock, measured);
        let right = wrap_conjuncts(binary.right(), clock, measured);
        return Arc::new(BinaryExpr::new(left, Operator::And, right));
    }
    let wrapper = Arc::new(MeasuredConjunct::new(
        Arc::clone(predicate),
        Arc::clone(clock),
    ));
    measured.push(Arc::clone(&wrapper));
    wrapper
}

/// Decides the order from the measurements.
fn decide(written: &Arc<dyn PhysicalExpr>, stats: &[ConjunctStats]) -> State {
    let written_order: Vec<usize> = (0..stats.len()).collect();
    let order = rank(stats);
    let is_better = order != written_order
        && expected_cost(stats, &order)
            < (1.0 - MIN_GAIN) * expected_cost(stats, &written_order);
    let conjuncts = split_conjunction(written);
    match right_nested_and(&conjuncts, &order) {
        Some(predicate) if is_better => State::Decided {
            predicate,
            order: Some(order),
        },
        // Keep the written tree. A new tree with the same order can change
        // where `AND` pre-selects, thus which rows a conjunct sees.
        _ => State::Decided {
            predicate: Arc::clone(written),
            order: None,
        },
    }
}

/// Indexes of the conjuncts, by rank key from high to low. Conjuncts that were
/// not evaluated are last. The sort is stable.
fn rank(stats: &[ConjunctStats]) -> Vec<usize> {
    let mut order: Vec<usize> = (0..stats.len()).collect();
    order.sort_by(|&a, &b| match (stats[a].rank_key(), stats[b].rank_key()) {
        (Some(a), Some(b)) => b.total_cmp(&a),
        (Some(_), None) => std::cmp::Ordering::Less,
        (None, Some(_)) => std::cmp::Ordering::Greater,
        (None, None) => std::cmp::Ordering::Equal,
    });
    order
}

/// Estimated nanoseconds for each input row when the conjuncts are
/// evaluated in `order` as a right-nested `AND` chain. Each conjunct costs
/// its cost for each row, times the fraction of the rows that the conjuncts
/// before it pass on (see [`rows_passed_on`]). The conjuncts are assumed to
/// be independent. A conjunct that was not evaluated costs nothing.
fn expected_cost(stats: &[ConjunctStats], order: &[usize]) -> f64 {
    let mut fraction = 1.0;
    let mut cost = 0.0;
    for stats in order.iter().map(|&i| &stats[i]) {
        if let Some(cost_per_row) = stats.cost_per_row() {
            cost += fraction * cost_per_row;
            fraction *= stats.passed_on_fraction();
        }
    }
    cost
}

/// `c[o0] AND (c[o1] AND (... AND c[on]))`, or `None` if `order` is empty.
/// A right-nested chain keeps the rows that the `AND` pre-selects for all
/// the conjuncts after it.
fn right_nested_and(
    conjuncts: &[&Arc<dyn PhysicalExpr>],
    order: &[usize],
) -> Option<Arc<dyn PhysicalExpr>> {
    order
        .iter()
        .rev()
        .map(|&i| Arc::clone(conjuncts[i]))
        .reduce(|right, left| Arc::new(BinaryExpr::new(left, Operator::And, right)))
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{BooleanArray, Int32Array};
    use arrow::datatypes::{DataType, Field};
    use datafusion_physical_expr::expressions::{binary, col, lit};
    use datafusion_physical_expr::filter_stats::ManualClock;

    /// A conjunct that moves the manual clock by `nanos_per_row` for each row
    /// that it evaluates. The clock does not move otherwise.
    #[derive(Debug)]
    struct Costly {
        inner: Arc<dyn PhysicalExpr>,
        clock: Arc<ManualClock>,
        nanos_per_row: u64,
        volatile: bool,
    }

    impl PartialEq for Costly {
        fn eq(&self, other: &Self) -> bool {
            self.inner.eq(&other.inner) && self.nanos_per_row == other.nanos_per_row
        }
    }

    impl Eq for Costly {}

    impl Hash for Costly {
        fn hash<H: Hasher>(&self, state: &mut H) {
            self.inner.hash(state);
        }
    }

    impl Display for Costly {
        fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
            write!(f, "{}", self.inner)
        }
    }

    impl PhysicalExpr for Costly {
        fn return_field(&self, input_schema: &Schema) -> Result<FieldRef> {
            self.inner.return_field(input_schema)
        }

        fn evaluate(&self, batch: &RecordBatch) -> Result<ColumnarValue> {
            self.clock
                .advance(self.nanos_per_row * batch.num_rows() as u64);
            self.inner.evaluate(batch)
        }

        fn children(&self) -> Vec<&Arc<dyn PhysicalExpr>> {
            vec![&self.inner]
        }

        fn with_new_children(
            self: Arc<Self>,
            _children: Vec<Arc<dyn PhysicalExpr>>,
        ) -> Result<Arc<dyn PhysicalExpr>> {
            Ok(self)
        }

        fn fmt_sql(&self, f: &mut Formatter<'_>) -> fmt::Result {
            self.inner.fmt_sql(f)
        }

        fn is_volatile_node(&self) -> bool {
            self.volatile
        }
    }

    const ROWS: i32 = 100;

    /// Column `a` is `0..100`. Column `b` is `a` in the even rows and null
    /// in the odd rows.
    fn schema() -> Schema {
        Schema::new(vec![
            Field::new("a", DataType::Int32, false),
            Field::new("b", DataType::Int32, true),
        ])
    }

    fn batch(rows: i32) -> RecordBatch {
        RecordBatch::try_new(
            Arc::new(schema()),
            vec![
                Arc::new(Int32Array::from_iter_values(0..rows)),
                Arc::new(Int32Array::from_iter(
                    (0..rows).map(|v| (v % 2 == 0).then_some(v)),
                )),
            ],
        )
        .unwrap()
    }

    struct Scenario {
        clock: Arc<ManualClock>,
    }

    impl Scenario {
        fn new() -> Self {
            Self {
                clock: Arc::new(ManualClock::new()),
            }
        }

        /// `column op value`, which costs `nanos_per_row` for each row.
        fn conjunct(
            &self,
            column: &str,
            op: Operator,
            value: i32,
            nanos_per_row: u64,
        ) -> Arc<dyn PhysicalExpr> {
            let schema = schema();
            let inner =
                binary(col(column, &schema).unwrap(), op, lit(value), &schema).unwrap();
            Arc::new(Costly {
                inner,
                clock: Arc::clone(&self.clock),
                nanos_per_row,
                volatile: false,
            })
        }

        fn order(&self, predicate: &Arc<dyn PhysicalExpr>) -> ConjunctOrder {
            let clock = Arc::clone(&self.clock) as Arc<dyn Clock>;
            ConjunctOrder::try_new(predicate, &clock).expect("can reorder")
        }
    }

    fn and(
        left: Arc<dyn PhysicalExpr>,
        right: Arc<dyn PhysicalExpr>,
    ) -> Arc<dyn PhysicalExpr> {
        Arc::new(BinaryExpr::new(left, Operator::And, right))
    }

    fn mask(value: ColumnarValue) -> BooleanArray {
        as_boolean_array(&value.into_array(ROWS as usize).unwrap())
            .unwrap()
            .clone()
    }

    /// Evaluates `batches` batches. Returns, for each batch, the strategy
    /// that was used: `measure`, `written` or `order [..]`. Also checks that
    /// each result is the same as the result of `predicate`.
    fn run(
        order: &mut ConjunctOrder,
        predicate: &Arc<dyn PhysicalExpr>,
        batches: usize,
    ) -> Vec<String> {
        (0..batches)
            .map(|_| {
                let strategy = if order.is_measuring() {
                    "measure".to_string()
                } else if let Some(new_order) = order.new_order() {
                    format!("order {new_order:?}")
                } else {
                    "written".to_string()
                };
                let batch = batch(ROWS);
                let got = mask(order.evaluate(&batch).unwrap());
                let want = mask(predicate.evaluate(&batch).unwrap());
                assert_eq!(got, want);
                strategy
            })
            .collect()
    }

    /// `measure` for the warm-up batches, then `then` for 2 batches.
    fn trace(then: &str) -> Vec<String> {
        let mut trace = vec!["measure".to_string(); WARMUP_BATCHES];
        trace.extend([then.to_string(), then.to_string()]);
        trace
    }

    fn decided_predicate(order: &ConjunctOrder) -> &Arc<dyn PhysicalExpr> {
        match &order.state {
            State::Decided { predicate, .. } => predicate,
            State::Measuring { .. } => panic!("still measuring"),
        }
    }

    /// `a >= 0` keeps all rows and `a < 5` keeps 5% of the rows, at the
    /// same cost. `a < 5` goes first.
    #[test]
    fn selective_conjunct_moves_first() {
        let s = Scenario::new();
        let predicate = and(
            s.conjunct("a", Operator::GtEq, 0, 10),
            s.conjunct("a", Operator::Lt, 5, 10),
        );
        let mut order = s.order(&predicate);
        assert_eq!(run(&mut order, &predicate, 10), trace("order [1, 0]"));
        assert_eq!(
            decided_predicate(&order).to_string(),
            "a@0 < 5 AND a@0 >= 0"
        );
    }

    /// The written order is already the best: the written predicate is kept
    /// unchanged (the same `Arc`).
    #[test]
    fn best_written_order_is_kept() {
        let s = Scenario::new();
        let predicate = and(
            s.conjunct("a", Operator::Lt, 5, 10),
            s.conjunct("a", Operator::GtEq, 0, 10),
        );
        let mut order = s.order(&predicate);
        assert_eq!(run(&mut order, &predicate, 10), trace("written"));
        assert!(Arc::ptr_eq(decided_predicate(&order), &predicate));
    }

    /// `a >= 10` is cheap (1 ns for each row) and removes 10% of the rows.
    /// `a < 20` is expensive (100 ns for each row) and removes 80%. The
    /// cheap conjunct removes more rows per nanosecond, thus it stays first.
    #[test]
    fn cheap_conjunct_stays_before_expensive_selective_conjunct() {
        let s = Scenario::new();
        let predicate = and(
            s.conjunct("a", Operator::GtEq, 10, 1),
            s.conjunct("a", Operator::Lt, 20, 100),
        );
        let mut order = s.order(&predicate);
        assert_eq!(run(&mut order, &predicate, 10), trace("written"));
    }

    /// `a < 30` keeps 30% of the rows. This is more than
    /// `PRE_SELECTION_THRESHOLD`, thus `AND` evaluates the next conjunct on
    /// all rows. The new order is not cheaper, thus it is not used.
    #[test]
    fn no_reorder_when_and_cannot_pre_select() {
        let s = Scenario::new();
        let predicate = and(
            s.conjunct("a", Operator::GtEq, 10, 10),
            s.conjunct("a", Operator::Lt, 30, 10),
        );
        let mut order = s.order(&predicate);
        assert_eq!(run(&mut order, &predicate, 10), trace("written"));

        // With `a < 10` (10% of the rows), `AND` pre-selects.
        let predicate = and(
            s.conjunct("a", Operator::GtEq, 10, 10),
            s.conjunct("a", Operator::Lt, 10, 10),
        );
        let mut order = s.order(&predicate);
        assert_eq!(run(&mut order, &predicate, 10), trace("order [1, 0]"));
    }

    /// `b < 10` keeps 5% of the rows, but it returns nulls. `AND` never
    /// pre-selects with nulls, thus the new order is not cheaper.
    #[test]
    fn nulls_prevent_reorder() {
        let s = Scenario::new();
        let predicate = and(
            s.conjunct("a", Operator::GtEq, 0, 10),
            s.conjunct("b", Operator::Lt, 10, 10),
        );
        let mut order = s.order(&predicate);
        assert_eq!(run(&mut order, &predicate, 10), trace("written"));
    }

    /// The new order is a right-nested `AND` chain.
    #[test]
    fn new_order_is_right_nested() {
        let s = Scenario::new();
        let predicate = and(
            and(
                s.conjunct("a", Operator::GtEq, 0, 10),
                s.conjunct("a", Operator::GtEq, 10, 10),
            ),
            s.conjunct("a", Operator::Lt, 5, 10),
        );
        let mut order = s.order(&predicate);
        assert_eq!(run(&mut order, &predicate, 10), trace("order [2, 1, 0]"));
        let root = decided_predicate(&order)
            .downcast_ref::<BinaryExpr>()
            .unwrap();
        assert_eq!(root.left().to_string(), "a@0 < 5");
        let right = root.right().downcast_ref::<BinaryExpr>().unwrap();
        assert_eq!(right.left().to_string(), "a@0 >= 10");
        assert_eq!(right.right().to_string(), "a@0 >= 0");
    }

    /// Empty batches do not count as measured batches.
    #[test]
    fn empty_batches_are_not_measured() {
        let s = Scenario::new();
        let predicate = and(
            s.conjunct("a", Operator::GtEq, 0, 10),
            s.conjunct("a", Operator::Lt, 5, 10),
        );
        let mut order = s.order(&predicate);
        for _ in 0..2 * WARMUP_BATCHES {
            order.evaluate(&batch(0)).unwrap();
        }
        assert!(order.is_measuring());
    }

    /// A predicate with fewer than two conjuncts, or with a volatile
    /// conjunct, is not reordered.
    #[test]
    fn some_predicates_are_not_reordered() {
        let s = Scenario::new();
        let clock = || Arc::clone(&s.clock) as Arc<dyn Clock>;
        let single = s.conjunct("a", Operator::Lt, 5, 10);
        assert!(ConjunctOrder::try_new(&single, &clock()).is_none());

        let volatile: Arc<dyn PhysicalExpr> = Arc::new(Costly {
            inner: s.conjunct("a", Operator::Lt, 5, 10),
            clock: Arc::clone(&s.clock),
            nanos_per_row: 10,
            volatile: true,
        });
        let predicate = and(s.conjunct("a", Operator::GtEq, 0, 10), volatile);
        assert!(ConjunctOrder::try_new(&predicate, &clock()).is_none());
    }

    #[test]
    fn rows_passed_on_matches_binary_expr() {
        // No passing row: the evaluation stops.
        assert_eq!(rows_passed_on(100, 0, 0), 0);
        // At or below the threshold: pre-selection.
        assert_eq!(rows_passed_on(100, 20, 0), 20);
        // Above the threshold: all rows.
        assert_eq!(rows_passed_on(100, 21, 0), 100);
        // All rows pass.
        assert_eq!(rows_passed_on(100, 100, 0), 100);
        // Nulls: never pre-selects.
        assert_eq!(rows_passed_on(100, 5, 1), 100);
        assert_eq!(rows_passed_on(100, 0, 1), 100);
    }
}
