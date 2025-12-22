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

use std::{
    any::Any,
    sync::{Arc, OnceLock, Weak},
};

use arrow::array::RecordBatch;
use datafusion_common::{
    Result, internal_err,
    tree_node::{Transformed, TreeNode},
};
use datafusion_execution::{
    RecordBatchStream, SendableRecordBatchStream, metrics::MetricsSet,
};
use datafusion_physical_expr::{
    PhysicalExpr,
    expressions::{DynamicFilterPhysicalExpr, PlannedDynamicFilterPhysicalExpr},
};
use futures::{Stream, StreamExt};
use parking_lot::Mutex;

use crate::{ExecutionPlan, WorkTable, metrics::ExecutionPlanMetricsSet};

/// [`PlanStateNode`] contains the state required during plan execution.
/// It is maintained so that each partition of a given plan receives the
/// same state node during a single query execution.
///
/// A [`PlanStateNode`] tree, built during the [`ExecutionPlan::execute`] stage,
/// mirrors the structure of the [`ExecutionPlan`] tree itself. It is designed
/// to store plan state that is created during execution and must later be
/// associated with the corresponding plan nodes—for example, metrics.
///
/// Each plan can store its state in the corresponding state node by implementing
/// [`PlanState`] for its specific state structure.
///
/// # Passing data to child nodes
///
/// State tree allows to pass state from some plan to children, for example,
/// work tables for recursive queries or dynamic filters. For the details,
/// see [`PlanState`] trait.
///
/// [`ExecutionPlan`]: crate::ExecutionPlan
/// [`ExecutionPlan::execute`]: crate::ExecutionPlan::execute
///
pub struct PlanStateNode {
    /// Corresponding plan node.
    pub plan_node: Arc<dyn ExecutionPlan>,
    /// Parent of the current node if exists.
    /// [`None`] if node is root.
    parent_node: Option<Weak<PlanStateNode>>,
    /// Plan specific state initialized once per execution.
    pub(super) plan_state: OnceLock<Arc<dyn PlanState>>,
    /// Metrics associated with this plan node.
    pub metrics: ExecutionPlanMetricsSet,
    /// State for each plan child lazily initialized.
    children_state: Mutex<Box<[OnceLock<Arc<PlanStateNode>>]>>,
}

impl PlanStateNode {
    /// Make a new [`PlanStateNode`].
    fn new(
        plan_node: Arc<dyn ExecutionPlan>,
        parent_node: Option<Weak<PlanStateNode>>,
    ) -> Self {
        let num_children = plan_node.children().len();
        Self {
            plan_node,
            parent_node,
            plan_state: OnceLock::default(),
            metrics: ExecutionPlanMetricsSet::new(),
            children_state: Mutex::new(
                (0..num_children).map(|_| OnceLock::default()).collect(),
            ),
        }
    }

    /// Make a new arced [`PlanStateNode`].
    fn new_arc(
        plan_node: Arc<dyn ExecutionPlan>,
        parent_node: Option<Weak<PlanStateNode>>,
    ) -> Arc<Self> {
        Arc::new(Self::new(plan_node, parent_node))
    }

    /// Make a new [`PlanStateNode`] for a root plan node.
    pub fn new_root(plan_node: Arc<dyn ExecutionPlan>) -> Self {
        Self::new(plan_node, None)
    }

    /// Make a new arced [`PlanStateNode`] for a root plan node.
    pub fn new_root_arc(plan_node: Arc<dyn ExecutionPlan>) -> Arc<Self> {
        Arc::new(Self::new_root(plan_node))
    }

    /// Find metrics of the plan within state tree. Returns [`None`] if the
    /// `plan` is not found among state tree plan nodes.
    pub fn metrics_of(
        self: &Arc<Self>,
        plan: &Arc<dyn ExecutionPlan>,
    ) -> Option<MetricsSet> {
        let mut metrics = None;
        accept_state(self, &mut |state: &Arc<PlanStateNode>| -> Result<bool> {
            if Arc::ptr_eq(&state.plan_node, plan) {
                metrics = Some(state.metrics.clone_inner());
                Ok(false)
            } else {
                Ok(true)
            }
        })
        .unwrap();
        metrics
    }

    /// Get or init state using passed `f`. Returns a reference to the state.
    ///
    /// # Panics
    ///
    /// State is already initialized and cannot be downcast to `S`.
    ///
    pub fn get_or_init_state<S: PlanState>(&self, f: impl FnOnce() -> S) -> &S {
        self.plan_state
            .get_or_init(|| Arc::new(f()))
            .as_any()
            .downcast_ref::<S>()
            .unwrap()
    }

    /// Make a child state node if it not initialized and return it.
    ///
    /// # Panics
    ///
    /// `child_idx` is more than the corresponding plan children number.
    ///
    pub fn child_state(self: &Arc<Self>, child_idx: usize) -> Arc<PlanStateNode> {
        Arc::clone(self.children_state.lock()[child_idx].get_or_init(|| {
            Self::new_arc(
                Arc::clone(self.plan_node.children()[child_idx]),
                // Set node parent.
                Some(Arc::downgrade(self)),
            )
        }))
    }

    /// Lookup for the last [`WorkTable`] owner node over the path from root
    /// to the current node.
    ///
    /// This function is intended  to be called by plan node that should operate
    /// with a work table during [`ExecutionPlan::execute`] call to find table set
    /// by work table owner, typically it is a [`RecursiveQueryExec`].
    ///
    /// [`RecursiveQueryExec`]: crate::recursive_query::RecursiveQueryExec
    ///
    pub fn work_table(&self) -> Option<Arc<WorkTable>> {
        self.inspect_root_path(|node| {
            node.plan_state.get().and_then(|state| state.work_table())
        })
    }

    /// Replace all planned dynamic filters in the given expression,
    /// converting them into executable versions by deriving shared
    /// state from the filter owner.
    ///
    /// This function is intended to be called by a plan node that supports
    /// dynamic filters during [`ExecutionPlan::execute`]. It converts stored
    /// planning-time filters into execution-time filters by looking up the
    /// filters stored in one of the parent nodes along the path to the
    /// state tree root.
    ///
    pub fn planned_dynamic_filter_to_executable(
        &self,
        expr: Arc<dyn PhysicalExpr>,
    ) -> Result<Arc<dyn PhysicalExpr>> {
        expr.transform_up(|expr| {
            let Some(dynamic_filter) = expr
                .as_any()
                .downcast_ref::<PlannedDynamicFilterPhysicalExpr>()
            else {
                return Ok(Transformed::no(expr));
            };
            let filter = match self.dynamic_filter_for(dynamic_filter) {
                None => {
                    return internal_err!(
                        "dynamic filter cannot be resolved to executable"
                    );
                }
                Some(filter) => filter,
            };
            Ok(Transformed::yes(filter as _))
        })
        .map(|tnr| tnr.data)
    }

    /// Lookup for the execution time dynamic filter by its origin.
    fn dynamic_filter_for(
        &self,
        origin: &PlannedDynamicFilterPhysicalExpr,
    ) -> Option<Arc<DynamicFilterPhysicalExpr>> {
        self.inspect_root_path(|node| {
            if let Some(state) = node.plan_state.get() {
                for filter in state.dynamic_filters() {
                    if let Some(res) = filter.as_dynamic_for(origin) {
                        return Some(res);
                    }
                }
            }
            None
        })
    }

    fn inspect_root_path<T>(&self, f: impl Fn(&PlanStateNode) -> Option<T>) -> Option<T> {
        if let Some(res) = f(self) {
            return Some(res);
        }
        let mut current_node = self.parent_node.as_ref().and_then(|p| p.upgrade());
        while let Some(node) = current_node {
            if let Some(res) = f(&node) {
                return Some(res);
            }
            current_node = node.parent_node.as_ref().and_then(|p| p.upgrade());
        }
        None
    }
}

/// Generic execution stage plan state.
pub trait PlanState: Send + Sync + 'static {
    /// Returns the state as [`Any`] so that it can be downcast to
    /// a specific implementation.
    fn as_any(&self) -> &dyn Any;

    /// Return dynamic filters maintained by this node.
    ///
    /// This function is used to push shared mutable dynamic filters
    /// from an owner to a child that accepted the filter during the
    /// planning stage via filter push-down optimization.
    ///
    fn dynamic_filters(&self) -> Vec<Arc<DynamicFilterPhysicalExpr>> {
        vec![]
    }

    /// Return [`WorkTable`] maintained by this node.
    fn work_table(&self) -> Option<Arc<WorkTable>> {
        None
    }
}

/// Describes a data associated with a [`PlanStateNode`].
pub struct WithPlanStateNode<T> {
    inner: T,
    state: Arc<PlanStateNode>,
}

impl<T> WithPlanStateNode<T> {
    /// Make a new [`WithPlanStateNode`].
    pub fn new(inner: T, state: Arc<PlanStateNode>) -> Self {
        Self { inner, state }
    }

    /// Project an inner data.
    pub fn as_inner(&self) -> &T {
        &self.inner
    }

    /// Project an inner mutable data.
    pub fn as_inner_mut(&mut self) -> &mut T {
        &mut self.inner
    }

    /// Project a state.
    pub fn state(&self) -> &Arc<PlanStateNode> {
        &self.state
    }

    /// Borrow an inner data, preserving the state node.
    pub fn as_ref(&self) -> WithPlanStateNode<&T> {
        WithPlanStateNode {
            inner: &self.inner,
            state: Arc::clone(&self.state),
        }
    }

    /// Map an inner data, preserving the state node.
    pub fn map<V>(self, f: impl FnOnce(T) -> V) -> WithPlanStateNode<V> {
        WithPlanStateNode {
            inner: f(self.inner),
            state: self.state,
        }
    }

    /// Try to map an inner data, preserving the state node.
    pub fn try_map<V>(
        self,
        f: impl FnOnce(T) -> Result<V>,
    ) -> Result<WithPlanStateNode<V>> {
        let inner = f(self.inner)?;
        Ok(WithPlanStateNode {
            inner,
            state: self.state,
        })
    }

    /// Try to apply async map `f`, preserving the state node.
    pub async fn try_map_async<V, Fut>(
        self,
        f: impl FnOnce(T) -> Fut,
    ) -> Result<WithPlanStateNode<V>>
    where
        Fut: Future<Output = Result<V>>,
    {
        let inner = f(self.inner).await?;
        Ok(WithPlanStateNode {
            inner,
            state: self.state,
        })
    }

    /// Consume `self` and convert into inner.
    pub fn into_inner(self) -> T {
        self.inner
    }

    /// Consume `self` and convert into node state.
    pub fn into_state(self) -> Arc<PlanStateNode> {
        self.state
    }

    /// Return stored parts.
    pub fn into_parts(self) -> (T, Arc<PlanStateNode>) {
        (self.inner, self.state)
    }
}

impl<T> WithPlanStateNode<Result<T>> {
    /// Represent self as a result.
    pub fn as_result(self) -> Result<WithPlanStateNode<T>> {
        self.inner.map(|inner| WithPlanStateNode {
            inner,
            state: self.state,
        })
    }
}

impl Stream for WithPlanStateNode<SendableRecordBatchStream> {
    type Item = Result<RecordBatch>;

    fn poll_next(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Self::Item>> {
        self.as_inner_mut().poll_next_unpin(cx)
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        self.as_inner().size_hint()
    }
}

impl RecordBatchStream for WithPlanStateNode<SendableRecordBatchStream> {
    fn schema(&self) -> arrow_schema::SchemaRef {
        self.as_inner().schema()
    }
}

/// Visit all children of this state using passing `visitor`.
pub fn accept_state<V: ExecutionPlanStateVisitor>(
    state: &Arc<PlanStateNode>,
    visitor: &mut V,
) -> Result<bool, V::Error> {
    if !visitor.pre_visit(state)? {
        return Ok(false);
    };
    for i in 0..state.plan_node.children().len() {
        if !accept_state(&state.child_state(i), visitor)? {
            return Ok(false);
        }
    }
    if !visitor.post_visit(state)? {
        return Ok(false);
    };
    Ok(true)
}

/// Trait that implements the [Visitor
/// pattern](https://en.wikipedia.org/wiki/Visitor_pattern) for a
/// depth first walk of [`PlanStateNode`] nodes. `pre_visit` is called
/// before any children are visited, and then `post_visit` is called
/// after all children have been visited.
pub trait ExecutionPlanStateVisitor {
    /// The type of error returned by this visitor.
    type Error;

    /// Invoked on an [`PlanStateNode`] before any of its child have
    /// been visited. If Ok(true) is returned, the recursion continues.
    /// If Err(..) or Ok(false) are returned, the recursion stops immediately
    /// and the error, if any, is returned.
    fn pre_visit(&mut self, state: &Arc<PlanStateNode>) -> Result<bool, Self::Error>;

    /// Invoked on an [`PlanStateNode`] plan *after* all of its child
    /// inputs have been visited. The return value is handled the same
    /// as the return value of `pre_visit`.
    fn post_visit(&mut self, _state: &Arc<PlanStateNode>) -> Result<bool, Self::Error> {
        Ok(true)
    }
}

impl<E, F> ExecutionPlanStateVisitor for F
where
    F: FnMut(&Arc<PlanStateNode>) -> Result<bool, E>,
{
    type Error = E;

    fn pre_visit(&mut self, state: &Arc<PlanStateNode>) -> Result<bool, Self::Error> {
        (self)(state)
    }
}
