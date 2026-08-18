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

//! Tracking changes to the dynamic filters inside a predicate.
//!
//! Several operators (Parquet file/row-group pruning, remote execution, ...)
//! hold a predicate that *may* contain one or more
//! [`DynamicFilterPhysicalExpr`] nodes which are updated during execution
//! (e.g. a `TopK` tightening its threshold, or a `HashJoinExec` publishing the
//! build-side bounds). These consumers repeatedly ask two questions:
//!
//! 1. *"Does this predicate contain anything that can still change?"* — to
//!    decide whether it is worth setting up runtime re-pruning at all.
//! 2. *"Has it changed since I last looked?"* — to decide whether to rebuild an
//!    expensive derived artifact (e.g. a `PruningPredicate`).
//!
//! Historically each call site answered these by recursively folding
//! [`PhysicalExpr::snapshot_generation`] over the whole tree on *every* check
//! and diffing the resulting `u64`. [`DynamicFilterTracker`] replaces that with
//! a single up-front walk that subscribes to each still-incomplete dynamic
//! filter; subsequent checks only poll the (shrinking) set of subscriptions,
//! each of which is a cheap atomic load in the common "nothing changed" case.
//!
//! [`PhysicalExpr::snapshot_generation`]: crate::PhysicalExpr::snapshot_generation

use std::sync::Arc;

use datafusion_common::tree_node::{TreeNode, TreeNodeRecursion};

use crate::PhysicalExpr;

use super::{DynamicFilterPhysicalExpr, DynamicFilterSubscription};

/// Classification of a predicate according to the dynamic filters it contains.
///
/// Produced by [`DynamicFilterTracking::classify`] with a single tree walk so
/// callers can answer both "is it worth pruning at all?" and "do I need to keep
/// watching?" without traversing the predicate twice.
#[derive(Debug)]
pub enum DynamicFilterTracking {
    /// The predicate contains no [`DynamicFilterPhysicalExpr`] at all. It is
    /// fully static and will never change.
    Static,
    /// The predicate contains one or more dynamic filters, but all of them have
    /// already been marked complete. Their *current* values may differ from
    /// what was known at planning time (so a one-shot prune is still
    /// worthwhile), but they will not change again — there is nothing to watch.
    AllComplete,
    /// The predicate contains at least one dynamic filter that can still change.
    /// The embedded [`DynamicFilterTracker`] should be polled to detect updates.
    Watching(DynamicFilterTracker),
}

impl DynamicFilterTracking {
    /// Walk `predicate` once and classify its dynamic-filter content,
    /// subscribing to every filter that is not yet complete.
    pub fn classify(predicate: &Arc<dyn PhysicalExpr>) -> Self {
        let mut subscriptions = Vec::new();
        let mut found_any = false;
        predicate
            .apply(|expr| {
                if let Some(filter) = expr.downcast_ref::<DynamicFilterPhysicalExpr>() {
                    found_any = true;
                    // Already-complete filters can never change again, so there
                    // is no point subscribing to them.
                    if !filter.is_complete() {
                        subscriptions.push(filter.subscribe());
                    }
                }
                Ok(TreeNodeRecursion::Continue)
            })
            .expect("traversal closure is infallible");

        if !found_any {
            DynamicFilterTracking::Static
        } else if subscriptions.is_empty() {
            DynamicFilterTracking::AllComplete
        } else {
            DynamicFilterTracking::Watching(DynamicFilterTracker { subscriptions })
        }
    }

    /// `true` if the predicate contains any dynamic filter (complete or not),
    /// i.e. its value may differ from what was known at planning time and is
    /// therefore worth re-evaluating at least once.
    pub fn contains_dynamic_filter(&self) -> bool {
        !matches!(self, DynamicFilterTracking::Static)
    }

    /// Mutable access to the underlying tracker when there is still something to
    /// watch.
    pub fn watcher(&mut self) -> Option<&mut DynamicFilterTracker> {
        match self {
            DynamicFilterTracking::Watching(tracker) => Some(tracker),
            _ => None,
        }
    }
}

/// Watches every still-incomplete [`DynamicFilterPhysicalExpr`] reachable from a
/// predicate and reports, cheaply, whether any of them has been updated since
/// the last check.
///
/// Obtain one from [`DynamicFilterTracking::classify`] via
/// [`DynamicFilterTracking::watcher`]; the `Watching` variant carries it only
/// when there is at least one dynamic filter that can still change.
#[derive(Debug)]
pub struct DynamicFilterTracker {
    /// Subscriptions to the not-yet-complete dynamic filters. Entries are
    /// dropped as their filters complete, so the set only shrinks.
    subscriptions: Vec<DynamicFilterSubscription>,
}

impl DynamicFilterTracker {
    /// Returns `true` if any watched filter's expression has advanced since the
    /// previous call.
    ///
    /// Filters that have completed are dropped from the watch set as they are
    /// observed; once every filter has completed this is a no-op that always
    /// returns `false`.
    pub fn changed(&mut self) -> bool {
        let mut changed = false;
        self.subscriptions.retain_mut(|subscription| {
            let change = subscription.observe();
            changed |= change.changed;
            // Keep the subscription only while the filter can still change.
            !change.complete
        });
        changed
    }
}

#[cfg(test)]
impl DynamicFilterTracker {
    /// Build a tracker directly, or `None` if `predicate` has no dynamic filter
    /// that can still change. Test-only; production builds a tracker via
    /// [`DynamicFilterTracking::classify`] + [`DynamicFilterTracking::watcher`].
    fn try_new(predicate: &Arc<dyn PhysicalExpr>) -> Option<Self> {
        match DynamicFilterTracking::classify(predicate) {
            DynamicFilterTracking::Watching(tracker) => Some(tracker),
            DynamicFilterTracking::Static | DynamicFilterTracking::AllComplete => None,
        }
    }

    /// `true` once every watched filter has completed and been dropped.
    fn is_exhausted(&self) -> bool {
        self.subscriptions.is_empty()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use crate::expressions::{BinaryExpr, col, lit};
    use arrow::datatypes::{DataType, Field, Schema};
    use datafusion_expr::Operator;

    /// `col > <dynamic>` where the dynamic filter starts as `lit(true)`.
    fn dynamic_predicate() -> (Arc<dyn PhysicalExpr>, Arc<DynamicFilterPhysicalExpr>) {
        let schema = Schema::new(vec![Field::new("a", DataType::Int32, false)]);
        let column = col("a", &schema).unwrap();
        let filter = Arc::new(DynamicFilterPhysicalExpr::new(
            vec![Arc::clone(&column)],
            lit(true),
        ));
        let predicate = Arc::new(BinaryExpr::new(
            column,
            Operator::Gt,
            Arc::clone(&filter) as Arc<dyn PhysicalExpr>,
        )) as Arc<dyn PhysicalExpr>;
        (predicate, filter)
    }

    #[test]
    fn static_predicate_is_not_watched() {
        let predicate = lit(true);
        assert!(matches!(
            DynamicFilterTracking::classify(&predicate),
            DynamicFilterTracking::Static
        ));
        assert!(DynamicFilterTracker::try_new(&predicate).is_none());
    }

    #[test]
    fn already_complete_filter_is_not_watched() {
        let (predicate, filter) = dynamic_predicate();
        filter.mark_complete();

        match DynamicFilterTracking::classify(&predicate) {
            DynamicFilterTracking::AllComplete => {}
            other => panic!("expected AllComplete, got {other:?}"),
        }
        // Still reported as dynamic (worth a one-shot prune)...
        assert!(DynamicFilterTracking::classify(&predicate).contains_dynamic_filter());
        // ...but there is nothing to watch.
        assert!(DynamicFilterTracker::try_new(&predicate).is_none());
    }

    #[test]
    fn detects_update_exactly_once() {
        let (predicate, filter) = dynamic_predicate();
        let mut tracker = DynamicFilterTracker::try_new(&predicate)
            .expect("predicate has an incomplete dynamic filter");

        // No update yet.
        assert!(!tracker.changed());

        filter.update(lit(false)).unwrap();
        // The update is reported once...
        assert!(tracker.changed());
        // ...and not repeatedly.
        assert!(!tracker.changed());
    }

    #[test]
    fn update_before_subscribe_is_not_reported() {
        let (predicate, filter) = dynamic_predicate();

        // An update that happens *before* the tracker subscribes must not be
        // reported on the first poll: `subscribe()` snapshots the current
        // generation via `borrow_and_update()`, so only post-subscription
        // updates count.
        filter.update(lit(false)).unwrap();

        let mut tracker = DynamicFilterTracker::try_new(&predicate)
            .expect("predicate has an incomplete dynamic filter");
        assert!(!tracker.changed());

        // A subsequent update is still reported.
        filter.update(lit(true)).unwrap();
        assert!(tracker.changed());
    }

    #[test]
    fn mark_complete_does_not_count_as_a_change() {
        let (predicate, filter) = dynamic_predicate();
        let mut tracker = DynamicFilterTracker::try_new(&predicate).unwrap();

        filter.update(lit(false)).unwrap();
        assert!(tracker.changed());

        // `mark_complete()` re-broadcasts the current generation without
        // changing the expression: it must not trigger a spurious rebuild.
        filter.mark_complete();
        assert!(!tracker.changed());
        // The filter has completed, so the tracker drains itself.
        assert!(tracker.is_exhausted());
    }

    #[test]
    fn coalesced_update_then_complete_is_one_change() {
        let (predicate, filter) = dynamic_predicate();
        let mut tracker = DynamicFilterTracker::try_new(&predicate).unwrap();

        // Update and complete before the tracker gets a chance to observe.
        // The watch channel only retains the latest value, so the tracker sees
        // `Complete` directly; it must still report the (final) change once.
        filter.update(lit(false)).unwrap();
        filter.mark_complete();

        assert!(tracker.changed());
        assert!(tracker.is_exhausted());
        assert!(!tracker.changed());
    }

    #[test]
    fn watches_multiple_filters_independently() {
        let schema = Schema::new(vec![
            Field::new("a", DataType::Int32, false),
            Field::new("b", DataType::Int32, false),
        ]);
        let col_a = col("a", &schema).unwrap();
        let col_b = col("b", &schema).unwrap();
        let filter_a = Arc::new(DynamicFilterPhysicalExpr::new(
            vec![Arc::clone(&col_a)],
            lit(true),
        ));
        let filter_b = Arc::new(DynamicFilterPhysicalExpr::new(
            vec![Arc::clone(&col_b)],
            lit(true),
        ));
        let predicate = Arc::new(BinaryExpr::new(
            Arc::new(BinaryExpr::new(
                col_a,
                Operator::Gt,
                Arc::clone(&filter_a) as Arc<dyn PhysicalExpr>,
            )),
            Operator::And,
            Arc::new(BinaryExpr::new(
                col_b,
                Operator::Lt,
                Arc::clone(&filter_b) as Arc<dyn PhysicalExpr>,
            )),
        )) as Arc<dyn PhysicalExpr>;

        let mut tracker = DynamicFilterTracker::try_new(&predicate).unwrap();
        assert!(!tracker.changed());

        filter_a.update(lit(false)).unwrap();
        assert!(tracker.changed());
        assert!(!tracker.changed());

        filter_b.update(lit(false)).unwrap();
        assert!(tracker.changed());
        assert!(!tracker.changed());

        // Completing one filter leaves the other still watched.
        filter_a.mark_complete();
        assert!(!tracker.changed());
        assert!(!tracker.is_exhausted());

        filter_b.mark_complete();
        assert!(!tracker.changed());
        assert!(tracker.is_exhausted());
    }
}
