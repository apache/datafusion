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

//! A conjunction (`AND`) of tagged [`PruningPredicate`] leaves with
//! per-leaf observation.
//!
//! [`PruningConjunction`] evaluates each leaf against statistics and
//! ANDs the results, invoking a [`PruningObserver`] each time a leaf
//! is actually evaluated. The AND short-circuits once every container
//! is pruned, and leaves cut off that way are *not* observed — so
//! per-conjunct stats reflect only what was evaluated.
//!
//! ## Why only `AND` (no `OR` / `NOT`)?
//!
//! `OR` and `NOT` are handled *inside* a leaf via
//! [`PruningPredicate::try_new`], not as structural nodes here, for
//! two reasons:
//!
//! * **Better pruning.** `try_new(a < 5 OR b > 100)` reasons across
//!   both branches; ORing two separate leaf masks is strictly more
//!   conservative.
//! * **Sound stats.** The per-leaf "containers pruned" count is only
//!   meaningful when pruning is monotonic, i.e. under `AND` (any leaf
//!   pruning a container prunes the whole expression). Under `OR` a
//!   leaf only helps where every sibling also prunes, so a per-leaf
//!   count would mislead a scheduler. Negating a conservative mask is
//!   likewise unsound.
//!
//! Per-branch `OR` short-circuit *is* useful during row-filter decode,
//! where masks are exact (not conservative) — that belongs to a
//! separate decode-path tree, not here.

use std::collections::HashMap;
use std::sync::Arc;

use arrow::datatypes::SchemaRef;
use datafusion_common::Result;
use datafusion_common::pruning::PruningStatistics;
use datafusion_physical_plan::PhysicalExpr;
use log::debug;

use crate::pruning_predicate::PruningPredicate;

/// Caller-supplied identifier attached to a leaf so consumers can
/// correlate per-leaf observations back to the original conjunct
/// (e.g. an adaptive filter scheduler keys this by `FilterId`).
pub type Tag = u64;

#[derive(Debug, Clone)]
struct TaggedLeaf {
    tag: Option<Tag>,
    predicate: Arc<PruningPredicate>,
}

/// A conjunction of tagged [`PruningPredicate`] leaves.
///
/// See the module-level docs for evaluation and short-circuit
/// semantics. Build one with [`PruningConjunction::builder`], or wrap
/// a single existing predicate with [`PruningConjunction::single`].
#[derive(Debug, Clone)]
pub struct PruningConjunction {
    schema: SchemaRef,
    leaves: Vec<TaggedLeaf>,
}

/// Observer invoked once per leaf that is actually evaluated against
/// statistics. Leaves skipped by AND short-circuit do not fire it.
///
/// The default method does nothing, so a no-op observer is just
/// [`NoopObserver`].
pub trait PruningObserver {
    /// Called after a leaf has been evaluated. `mask[i] = true` means
    /// container `i` may match this leaf (cannot be pruned); `false`
    /// means the leaf alone proves the container can be skipped.
    fn on_leaf(&mut self, _tag: Option<Tag>, _mask: &[bool]) {}
}

/// Zero-cost no-op observer used by the plain `prune()` path.
#[derive(Debug, Default, Clone, Copy)]
pub struct NoopObserver;

impl PruningObserver for NoopObserver {}

/// Per-leaf pruning rate accumulated by [`ConjunctStatsObserver`].
///
/// `containers_seen` counts the containers a leaf was evaluated
/// against; `containers_pruned` counts containers the leaf alone
/// proved skippable. Leaves cut off by AND short-circuit are not
/// counted (no event fires for them).
#[derive(Debug, Default, Clone, Copy)]
pub struct PerConjunctPruneStats {
    pub tag: Tag,
    pub containers_seen: usize,
    pub containers_pruned: usize,
}

impl PerConjunctPruneStats {
    /// Pruning rate as a fraction in `[0.0, 1.0]`, or `None` when the
    /// leaf has not been observed yet.
    pub fn pruning_rate(&self) -> Option<f64> {
        if self.containers_seen == 0 {
            return None;
        }
        Some(self.containers_pruned as f64 / self.containers_seen as f64)
    }
}

/// Built-in observer accumulating [`PerConjunctPruneStats`] keyed by
/// [`Tag`]. Untagged leaves are ignored. Repeated observations for the
/// same tag (e.g. the same conjunction re-run across files) accumulate
/// additively.
#[derive(Debug, Default)]
pub struct ConjunctStatsObserver {
    stats: HashMap<Tag, PerConjunctPruneStats>,
}

impl ConjunctStatsObserver {
    pub fn new() -> Self {
        Self::default()
    }

    /// Take ownership of the accumulated stats, sorted by tag for
    /// deterministic output. The observer is empty afterwards.
    pub fn take(&mut self) -> Vec<PerConjunctPruneStats> {
        let mut out: Vec<_> = self.stats.drain().map(|(_, v)| v).collect();
        out.sort_by_key(|s| s.tag);
        out
    }

    /// Borrow the accumulated stats without consuming.
    pub fn stats(&self) -> impl Iterator<Item = &PerConjunctPruneStats> {
        self.stats.values()
    }
}

impl PruningObserver for ConjunctStatsObserver {
    fn on_leaf(&mut self, tag: Option<Tag>, mask: &[bool]) {
        let Some(tag) = tag else {
            return;
        };
        let entry = self.stats.entry(tag).or_insert(PerConjunctPruneStats {
            tag,
            containers_seen: 0,
            containers_pruned: 0,
        });
        entry.containers_seen += mask.len();
        entry.containers_pruned += mask.iter().filter(|b| !**b).count();
    }
}

impl PruningConjunction {
    /// Wrap a single already-built [`PruningPredicate`] as an untagged
    /// one-leaf conjunction. Used by the standard (non-adaptive)
    /// pruning path.
    pub fn single(predicate: Arc<PruningPredicate>) -> Self {
        let schema = Arc::clone(predicate.schema());
        Self {
            schema,
            leaves: vec![TaggedLeaf {
                tag: None,
                predicate,
            }],
        }
    }

    /// Start building a conjunction whose leaves are all evaluated
    /// against `schema`.
    pub fn builder(schema: SchemaRef) -> PruningConjunctionBuilder {
        PruningConjunctionBuilder {
            schema,
            leaves: Vec::new(),
        }
    }

    /// Schema all leaves were built against.
    pub fn schema(&self) -> &SchemaRef {
        &self.schema
    }

    /// Number of leaves.
    pub fn num_leaves(&self) -> usize {
        self.leaves.len()
    }

    /// Evaluate the conjunction against `statistics`. Equivalent to
    /// `prune_with_observer(statistics, &mut NoopObserver)`.
    pub fn prune<S: PruningStatistics + ?Sized>(
        &self,
        statistics: &S,
    ) -> Result<Vec<bool>> {
        self.prune_with_observer(statistics, &mut NoopObserver)
    }

    /// Evaluate the conjunction against `statistics`, invoking
    /// `observer.on_leaf(tag, mask)` once per leaf actually evaluated.
    /// Short-circuits once every container is pruned.
    pub fn prune_with_observer<S: PruningStatistics + ?Sized>(
        &self,
        statistics: &S,
        observer: &mut dyn PruningObserver,
    ) -> Result<Vec<bool>> {
        let n = statistics.num_containers();
        let mut combined = vec![true; n];
        for leaf in &self.leaves {
            if combined.iter().all(|b| !b) {
                // Every container already pruned; remaining leaves
                // cannot change the result and are intentionally not
                // observed.
                break;
            }
            let mask = leaf.predicate.prune(statistics)?;
            observer.on_leaf(leaf.tag, &mask);
            for (c, v) in combined.iter_mut().zip(mask.iter()) {
                *c = *c && *v;
            }
        }
        Ok(combined)
    }

    /// The conjunction's combined original expression — the `AND` of
    /// each leaf's [`PruningPredicate::orig_expr`]. Callers that need
    /// the *whole* predicate (e.g. to invert it for fully-matched
    /// detection) use this rather than re-deriving it, so the
    /// conjunction stays the single source of truth.
    pub fn combined_orig_expr(&self) -> Arc<dyn PhysicalExpr> {
        datafusion_physical_expr::conjunction(
            self.leaves
                .iter()
                .map(|l| Arc::clone(l.predicate.orig_expr()))
                .collect::<Vec<_>>(),
        )
    }
}

/// Builder for [`PruningConjunction`]. Each [`push`](Self::push) runs
/// [`PruningPredicate::try_new`]; conjuncts that error or simplify to
/// always-true are skipped (their tags will not appear in observer
/// output). [`build`](Self::build) returns `None` when no non-trivial
/// leaf survived.
#[derive(Debug)]
pub struct PruningConjunctionBuilder {
    schema: SchemaRef,
    leaves: Vec<TaggedLeaf>,
}

impl PruningConjunctionBuilder {
    /// Add a tagged conjunct. Mutates and returns `&mut self`, so this
    /// works both in chains and in loops:
    ///
    /// ```ignore
    /// let mut b = PruningConjunction::builder(schema);
    /// for (id, expr) in filters {
    ///     b.push(id, expr);
    /// }
    /// let conj = b.build();
    /// ```
    pub fn push(&mut self, tag: Tag, expr: Arc<dyn PhysicalExpr>) -> &mut Self {
        match PruningPredicate::try_new(expr, Arc::clone(&self.schema)) {
            Ok(pp) if !pp.always_true() => {
                self.leaves.push(TaggedLeaf {
                    tag: Some(tag),
                    predicate: Arc::new(pp),
                });
            }
            Ok(_) => {
                // always-true: contributes nothing; tag intentionally dropped.
            }
            Err(e) => {
                debug!("PruningConjunctionBuilder: skipping conjunct tag={tag}: {e}");
            }
        }
        self
    }

    /// Finish building. Returns `None` if no non-trivial leaf was added.
    pub fn build(self) -> Option<PruningConjunction> {
        if self.leaves.is_empty() {
            return None;
        }
        Some(PruningConjunction {
            schema: self.schema,
            leaves: self.leaves,
        })
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow::array::{ArrayRef, Int32Array};
    use arrow::datatypes::{DataType, Field, Schema};
    use datafusion_common::pruning::PruningStatistics;
    use datafusion_common::{Column, ScalarValue};
    use datafusion_expr_common::operator::Operator;
    use datafusion_physical_expr::expressions::{BinaryExpr, Column as PhysCol, Literal};
    use datafusion_physical_plan::PhysicalExpr;

    use crate::pruning_predicate::PruningPredicate;

    use super::*;

    /// 3-container statistics: container i sees `x` in [mins[i], maxes[i]].
    struct TestStats {
        mins: Vec<i32>,
        maxes: Vec<i32>,
    }
    impl PruningStatistics for TestStats {
        fn min_values(&self, _column: &Column) -> Option<ArrayRef> {
            Some(Arc::new(Int32Array::from(self.mins.clone())))
        }
        fn max_values(&self, _column: &Column) -> Option<ArrayRef> {
            Some(Arc::new(Int32Array::from(self.maxes.clone())))
        }
        fn num_containers(&self) -> usize {
            self.mins.len()
        }
        fn null_counts(&self, _column: &Column) -> Option<ArrayRef> {
            None
        }
        fn row_counts(&self) -> Option<ArrayRef> {
            None
        }
        fn contained(
            &self,
            _column: &Column,
            _values: &std::collections::HashSet<ScalarValue>,
        ) -> Option<arrow::array::BooleanArray> {
            None
        }
    }

    fn schema_x() -> SchemaRef {
        Arc::new(Schema::new(vec![Field::new("x", DataType::Int32, true)]))
    }

    fn expr(op: Operator, rhs: i32) -> Arc<dyn PhysicalExpr> {
        Arc::new(BinaryExpr::new(
            Arc::new(PhysCol::new("x", 0)),
            op,
            Arc::new(Literal::new(ScalarValue::Int32(Some(rhs)))),
        ))
    }

    fn pp(op: Operator, rhs: i32, schema: &SchemaRef) -> Arc<PruningPredicate> {
        Arc::new(PruningPredicate::try_new(expr(op, rhs), Arc::clone(schema)).unwrap())
    }

    #[derive(Default)]
    struct RecordingObserver {
        events: Vec<(Option<Tag>, Vec<bool>)>,
    }
    impl PruningObserver for RecordingObserver {
        fn on_leaf(&mut self, tag: Option<Tag>, mask: &[bool]) {
            self.events.push((tag, mask.to_vec()));
        }
    }

    #[test]
    fn single_behaves_like_plain_pruning_predicate() {
        let schema = schema_x();
        let predicate = pp(Operator::Gt, 5, &schema); // x > 5
        let conj = PruningConjunction::single(Arc::clone(&predicate));
        let stats = TestStats {
            mins: vec![0, 6, 10],
            maxes: vec![3, 10, 20],
        };
        assert_eq!(
            conj.prune(&stats).unwrap(),
            predicate.prune(&stats).unwrap()
        );
    }

    #[test]
    fn builder_accumulates_per_conjunct_stats() {
        // (x > 5) AND (x < 100): p1 prunes container 0, p2 prunes none.
        let schema = schema_x();
        let mut b = PruningConjunction::builder(Arc::clone(&schema));
        b.push(101, expr(Operator::Gt, 5))
            .push(202, expr(Operator::Lt, 100));
        let conj = b.build().expect("non-trivial");

        let stats = TestStats {
            mins: vec![0, 6, 10],
            maxes: vec![3, 10, 20],
        };
        let mut obs = ConjunctStatsObserver::new();
        let mask = conj.prune_with_observer(&stats, &mut obs).unwrap();
        assert_eq!(mask, vec![false, true, true]);

        let stats = obs.take();
        assert_eq!(stats.len(), 2);
        let s101 = stats.iter().find(|s| s.tag == 101).unwrap();
        assert_eq!((s101.containers_seen, s101.containers_pruned), (3, 1));
        let s202 = stats.iter().find(|s| s.tag == 202).unwrap();
        assert_eq!((s202.containers_seen, s202.containers_pruned), (3, 0));
    }

    #[test]
    fn and_short_circuit_keeps_stats_unbiased() {
        // p1 (x > 100) prunes everything alone; p2 (x < 50) must NOT be
        // observed — it never ran.
        let schema = schema_x();
        let mut b = PruningConjunction::builder(Arc::clone(&schema));
        b.push(1, expr(Operator::Gt, 100))
            .push(2, expr(Operator::Lt, 50));
        let conj = b.build().unwrap();

        let stats = TestStats {
            mins: vec![0, 0, 0],
            maxes: vec![3, 10, 20],
        };
        let mut obs = RecordingObserver::default();
        let mask = conj.prune_with_observer(&stats, &mut obs).unwrap();
        assert_eq!(mask, vec![false, false, false]);
        assert_eq!(obs.events.len(), 1);
        assert_eq!(obs.events[0].0, Some(1));
    }

    #[test]
    fn builder_drops_always_true() {
        let schema = schema_x();
        let always_true: Arc<dyn PhysicalExpr> =
            Arc::new(Literal::new(ScalarValue::Boolean(Some(true))));
        let mut b = PruningConjunction::builder(Arc::clone(&schema));
        b.push(1, always_true).push(2, expr(Operator::Gt, 5));
        let conj = b.build().expect("one non-trivial leaf");
        assert_eq!(conj.num_leaves(), 1);
    }

    #[test]
    fn builder_all_trivial_returns_none() {
        let schema = schema_x();
        let always_true: Arc<dyn PhysicalExpr> =
            Arc::new(Literal::new(ScalarValue::Boolean(Some(true))));
        let mut b = PruningConjunction::builder(schema);
        b.push(1, always_true);
        assert!(b.build().is_none());
    }
}
