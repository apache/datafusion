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

//! The join graph: a subtree of joins as relations plus the predicates between them.
//!
//! Extraction flattens a subtree into a [`JoinGraph`], which is what
//! [`JoinCostModel`](super::JoinCostModel) estimates over and what the rebuilt tree is
//! assembled from.

use std::sync::Arc;

use datafusion_common::error::Result;
use datafusion_common::{JoinSide, JoinType, NullEquality, Statistics, internal_err};
use datafusion_physical_expr::PhysicalExprRef;
use datafusion_physical_expr::expressions::Column;
use datafusion_physical_plan::joins::utils::{
    ColumnIndex, JoinFilter, max_distinct_count,
};
use datafusion_physical_plan::joins::{
    CrossJoinExec, HashJoinExec, NestedLoopJoinExec, SortMergeJoinExec,
};
use datafusion_physical_plan::projection::{ProjectionExec, all_alias_free_columns};
use datafusion_physical_plan::{ExecutionPlan, ExecutionPlanProperties};

/// Hard upper bound on the relations in one join graph. The search allocates `2^n` and
/// visits `3^n`, so larger graphs keep the planner's order regardless of the limit.
pub(crate) const MAX_RELATIONS: usize = 16;

/// Computes a plan node's statistics, shared with the rest of `JoinSelection`.
pub(crate) type StatsFn<'a> =
    dyn FnMut(&dyn ExecutionPlan) -> Result<Arc<Statistics>> + 'a;

/// A bitmask over relation indices.
pub type RelSet = u64;

/// The set holding `rel` alone.
pub fn bit(rel: usize) -> RelSet {
    1u64 << rel
}

/// The relations in `mask`, lowest index first.
pub fn iter_rels(mask: RelSet) -> impl Iterator<Item = usize> {
    std::iter::successors(Some(mask), |m| Some(m & m.wrapping_sub(1)))
        .take_while(|m| *m != 0)
        .map(|m| m.trailing_zeros() as usize)
}

/// Whether `mask` holds every relation in `required`.
pub fn covers(mask: RelSet, required: RelSet) -> bool {
    required & !mask == 0
}

/// One column of one relation, tracked instead of a plain index because reordering
/// moves columns to other positions.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub struct ColRef {
    /// Index into [`JoinGraph::relations`].
    pub rel: usize,
    /// Index into that relation's schema.
    pub col: usize,
}

/// What a relation contributes to the join.
#[derive(Debug)]
pub enum Role {
    /// An ordinary input, contributing its columns.
    Output,
    /// The quantified side of a semi or anti join, which filters instead of
    /// contributing columns.
    Reducer(Reducer),
}

/// The quantified side of a semi or anti join, as what it does to the side it filters.
#[derive(Debug)]
pub struct Reducer {
    /// `true` for an anti join, which keeps the rows that do *not* match.
    pub anti: bool,
    /// Keys, as `(column of the filtered side, column index here)`.
    pub keys: Vec<(ColRef, usize)>,
    /// Relations the keys reference; this reducer applies only to a set covering them.
    pub required: RelSet,
}

/// One leaf of the join graph: a subplan the enumerator does not look inside.
#[derive(Debug)]
pub struct Relation {
    /// The subplan this relation stands for.
    pub plan: Arc<dyn ExecutionPlan>,
    /// Estimated row count, clamped to at least 1.
    pub rows: f64,
    /// Estimated bytes per row, when the input reports a size.
    pub width: Option<f64>,
    /// Per-column distinct value estimate, clamped to `[1, rows]`.
    pub ndv: Vec<f64>,
    /// What it contributes to the join.
    pub role: Role,
}

/// An equi-join predicate `left = right` between two distinct relations.
#[derive(Clone, Copy, Debug)]
pub struct Edge {
    /// The column on one side.
    pub left: ColRef,
    /// The column it is compared against.
    pub right: ColRef,
}

/// A non-equi join predicate, moved along with its column references rewritten.
#[derive(Debug)]
pub struct Filter {
    /// The predicate itself.
    pub filter: JoinFilter,
    /// The column each entry of the filter's intermediate schema comes from.
    pub columns: Vec<ColRef>,
    /// The relations those columns belong to.
    pub required: RelSet,
}

/// A connected set of joins as relations plus the predicates between them.
#[derive(Debug)]
pub struct JoinGraph {
    /// The leaves, addressed by index throughout.
    pub relations: Vec<Relation>,
    /// The equi-join predicates between them.
    pub edges: Vec<Edge>,
    /// The non-equi predicates between them.
    pub filters: Vec<Filter>,
    /// Columns the original subtree emitted; the rebuilt one reproduces this exactly.
    pub output: Vec<ColRef>,
    /// Null handling shared by the subtree's joins. A join that differs becomes a
    /// relation instead.
    pub null_equality: Option<NullEquality>,
    /// The original tree's internal nodes as `(node, one child)`, children first, so
    /// the planner's shape can be scored under the same formula as the alternatives.
    pub original_nodes: Vec<(RelSet, RelSet)>,
    /// The relations that are reducers rather than ordinary inputs.
    pub reducers: RelSet,
    /// Which join operator the subtree used, and which the rebuild emits.
    pub kind: Option<JoinKind>,
}

impl JoinGraph {
    /// Distinct values estimated for one column.
    pub fn ndv(&self, col: ColRef) -> f64 {
        self.relations[col.rel].ndv[col.col]
    }

    /// Every relation in the graph.
    pub fn all(&self) -> RelSet {
        (0..self.relations.len()).fold(0, |mask, rel| mask | bit(rel))
    }

    /// How `rel` reduces the side it filters, if it is a reducer at all.
    pub fn reducer(&self, rel: usize) -> Option<&Reducer> {
        match &self.relations[rel].role {
            Role::Reducer(reducer) => Some(reducer),
            Role::Output => None,
        }
    }

    /// The operator the rebuild emits, defaulting to a hash join.
    pub fn kind(&self) -> JoinKind {
        self.kind.unwrap_or(JoinKind::Hash)
    }

    /// The null handling the rebuild emits, defaulting to `NullEqualsNothing`.
    pub fn null_equality(&self) -> NullEquality {
        self.null_equality
            .unwrap_or(NullEquality::NullEqualsNothing)
    }
}

/// The join operators the rule can flatten and rebuild.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum JoinKind {
    /// Rebuilt as a [`HashJoinExec`].
    Hash,
    /// Rebuilt as a [`SortMergeJoinExec`], which has no built-in projection, so the
    /// subtree gets one projection on top instead.
    SortMerge,
}

/// One join, seen the same way whichever operator implements it. `kind` and
/// `null_equality` are `None` for operators without equi-join keys.
struct JoinView<'a> {
    kind: Option<JoinKind>,
    role: JoinRole,
    left: &'a Arc<dyn ExecutionPlan>,
    right: &'a Arc<dyn ExecutionPlan>,
    on: &'a [(PhysicalExprRef, PhysicalExprRef)],
    filter: Option<&'a JoinFilter>,
    null_equality: Option<NullEquality>,
    projection: Option<&'a [usize]>,
}

fn join_view(plan: &Arc<dyn ExecutionPlan>) -> Option<JoinView<'_>> {
    if let Some(join) = plan.downcast_ref::<HashJoinExec>() {
        if join.null_aware || join.fetch().is_some() || join.on().is_empty() {
            return None;
        }
        return Some(JoinView {
            kind: Some(JoinKind::Hash),
            role: join_role(join.join_type(), join.filter().is_some())?,
            left: join.left(),
            right: join.right(),
            on: join.on(),
            filter: join.filter(),
            null_equality: Some(join.null_equality),
            projection: join.projection.as_deref(),
        });
    }
    if let Some(join) = plan.downcast_ref::<SortMergeJoinExec>() {
        if join.on().is_empty() {
            return None;
        }
        return Some(JoinView {
            kind: Some(JoinKind::SortMerge),
            role: join_role(&join.join_type(), join.filter().is_some())?,
            left: join.left(),
            right: join.right(),
            on: join.on(),
            filter: join.filter().as_ref(),
            null_equality: Some(join.null_equality()),
            projection: None,
        });
    }
    if let Some(join) = plan.downcast_ref::<NestedLoopJoinExec>() {
        // A semi or anti variant has no keys to model as a reducer.
        if *join.join_type() != JoinType::Inner {
            return None;
        }
        return Some(JoinView {
            kind: None,
            role: JoinRole::Inner,
            left: join.left(),
            right: join.right(),
            on: &[],
            filter: join.filter(),
            null_equality: None,
            projection: join.projection().as_deref(),
        });
    }
    let join = plan.downcast_ref::<CrossJoinExec>()?;
    Some(JoinView {
        kind: None,
        role: JoinRole::Inner,
        left: join.left(),
        right: join.right(),
        on: &[],
        filter: None,
        null_equality: None,
        projection: None,
    })
}

/// How a join takes part in enumeration, if at all.
#[derive(Clone, Copy, Debug)]
enum JoinRole {
    Inner,
    /// A semi or anti join; `output` names the side whose rows survive.
    Reducing {
        anti: bool,
        output: JoinSide,
    },
}

/// Classifies a join. Outer and mark joins are excluded because they do not just filter
/// their inputs, and semi and anti joins with a filter because it is part of their test.
fn join_role(join_type: &JoinType, has_filter: bool) -> Option<JoinRole> {
    let role = match join_type {
        JoinType::Inner => JoinRole::Inner,
        JoinType::LeftSemi => JoinRole::Reducing {
            anti: false,
            output: JoinSide::Left,
        },
        JoinType::RightSemi => JoinRole::Reducing {
            anti: false,
            output: JoinSide::Right,
        },
        JoinType::LeftAnti => JoinRole::Reducing {
            anti: true,
            output: JoinSide::Left,
        },
        JoinType::RightAnti => JoinRole::Reducing {
            anti: true,
            output: JoinSide::Right,
        },
        _ => return None,
    };
    if has_filter && !matches!(role, JoinRole::Inner) {
        return None;
    }
    Some(role)
}

/// Whether a graph-wide choice and one join's are compatible.
fn agree<T: PartialEq>(graph: Option<T>, join: Option<T>) -> bool {
    match (graph, join) {
        (Some(graph), Some(join)) => graph == join,
        _ => true,
    }
}

fn as_column(expr: &PhysicalExprRef) -> Option<usize> {
    expr.downcast_ref::<Column>().map(|col| col.index())
}

/// Extracts the maximal reorderable subtree at `plan`. `None` covers every bail-out: an
/// unmodelled join feature, a non-column key, missing row counts, too few or many inputs.
pub(crate) fn extract(
    plan: &Arc<dyn ExecutionPlan>,
    stats: &mut StatsFn,
) -> Result<Option<JoinGraph>> {
    // Start at a join, or at the pruning projection usually above one, so its column list
    // becomes the top join's projection instead of a `ProjectionExec` above a wider join.
    let is_root = join_view(plan).is_some()
        || plan
            .downcast_ref::<ProjectionExec>()
            .is_some_and(|projection| all_alias_free_columns(projection.expr()));
    if !is_root {
        return Ok(None);
    }
    Extractor::new(stats).extract(plan)
}

struct Extractor<'a, 's> {
    graph: JoinGraph,
    stats: &'s mut StatsFn<'a>,
}

impl<'a, 's> Extractor<'a, 's> {
    fn new(stats: &'s mut StatsFn<'a>) -> Self {
        Self {
            graph: JoinGraph {
                relations: vec![],
                edges: vec![],
                filters: vec![],
                output: vec![],
                null_equality: None,
                original_nodes: vec![],
                reducers: 0,
                kind: None,
            },
            stats,
        }
    }

    fn extract(mut self, plan: &Arc<dyn ExecutionPlan>) -> Result<Option<JoinGraph>> {
        let Some((output, _)) = self.visit(plan)? else {
            return Ok(None);
        };
        let mut graph = self.graph;
        graph.output = output;

        if graph.relations.len() < 3 {
            return Ok(None);
        }
        // A filter over one relation has no join to sit at; the node would be a leaf.
        if graph
            .filters
            .iter()
            .any(|filter| filter.required.count_ones() < 2)
        {
            return Ok(None);
        }
        Ok(Some(graph))
    }

    fn visit(
        &mut self,
        plan: &Arc<dyn ExecutionPlan>,
    ) -> Result<Option<(Vec<ColRef>, RelSet)>> {
        if let Some(view) = join_view(plan)
            && agree(self.graph.null_equality, view.null_equality)
            && agree(self.graph.kind, view.kind)
        {
            self.graph.null_equality = self.graph.null_equality.or(view.null_equality);
            self.graph.kind = self.graph.kind.or(view.kind);
            let visited = match view.role {
                JoinRole::Inner => self.visit_inner(&view)?,
                JoinRole::Reducing { anti, output } => {
                    self.visit_reducing(&view, anti, output)?
                }
            };
            let Some((columns, mask)) = visited else {
                return Ok(None);
            };

            // For a semi or anti join the projection selects from the output side alone.
            let columns = match view.projection {
                Some(projection) => projection.iter().map(|idx| columns[*idx]).collect(),
                None => columns,
            };
            Ok(Some((columns, mask)))
        } else if let Some(projection) = plan.downcast_ref::<ProjectionExec>()
            && all_alias_free_columns(projection.expr())
        {
            // Looking through pruning projections lets the enumerator see a whole chain,
            // since `ProjectionPushdown` has not folded them into the joins yet.
            let Some((child, mask)) = self.visit(projection.input())? else {
                return Ok(None);
            };
            let columns = projection
                .expr()
                .iter()
                .map(|proj| {
                    proj.expr
                        .downcast_ref::<Column>()
                        .map(|col| child[col.index()])
                })
                .collect::<Option<Vec<_>>>();
            Ok(columns.map(|columns| (columns, mask)))
        } else {
            let Some(rel) = self.push_relation(plan, Role::Output)? else {
                return Ok(None);
            };
            Ok(Some((
                (0..plan.schema().fields().len())
                    .map(|col| ColRef { rel, col })
                    .collect(),
                bit(rel),
            )))
        }
    }

    /// Flattens an inner join: both sides join the graph, predicates become edges
    /// and filters.
    fn visit_inner(&mut self, view: &JoinView) -> Result<Option<(Vec<ColRef>, RelSet)>> {
        let Some((left, left_mask)) = self.visit(view.left)? else {
            return Ok(None);
        };
        let Some((right, right_mask)) = self.visit(view.right)? else {
            return Ok(None);
        };

        for (left_key, right_key) in view.on {
            let (Some(left_key), Some(right_key)) =
                (as_column(left_key), as_column(right_key))
            else {
                // A key like `cast(a) = b` would need re-deriving against a different schema.
                return Ok(None);
            };
            let edge = Edge {
                left: left[left_key],
                right: right[right_key],
            };
            // Duplicates would be double counted by the cost model.
            if !self
                .graph
                .edges
                .iter()
                .any(|e| (e.left, e.right) == (edge.left, edge.right))
            {
                self.graph.edges.push(edge);
            }
        }

        if let Some(filter) = view.filter {
            let columns = filter
                .column_indices()
                .iter()
                .map(|ColumnIndex { index, side }| match side {
                    JoinSide::Left => Some(left[*index]),
                    JoinSide::Right => Some(right[*index]),
                    JoinSide::None => None,
                })
                .collect::<Option<Vec<_>>>();
            let Some(columns) = columns else {
                return Ok(None);
            };
            let required = columns.iter().fold(0, |mask, col| mask | bit(col.rel));
            self.graph.filters.push(Filter {
                filter: filter.clone(),
                columns,
                required,
            });
        }

        let mut columns = left;
        columns.extend(right);
        self.graph
            .original_nodes
            .push((left_mask | right_mask, left_mask));
        Ok(Some((columns, left_mask | right_mask)))
    }

    /// Flattens a semi or anti join: its output side joins the graph, its quantified
    /// side becomes a reducer.
    fn visit_reducing(
        &mut self,
        view: &JoinView,
        anti: bool,
        output: JoinSide,
    ) -> Result<Option<(Vec<ColRef>, RelSet)>> {
        let (output_plan, reducer_plan) = match output {
            JoinSide::Left => (view.left, view.right),
            JoinSide::Right => (view.right, view.left),
            JoinSide::None => return internal_err!("semi join with no output side"),
        };

        let Some((columns, mask)) = self.visit(output_plan)? else {
            return Ok(None);
        };

        // Keys resolve against the output side, so they precede the reducer relation.
        let mut keys = Vec::with_capacity(view.on.len());
        let mut required = 0;
        for (left_key, right_key) in view.on {
            let (output_key, reducer_key) = match output {
                JoinSide::Left => (left_key, right_key),
                _ => (right_key, left_key),
            };
            let (Some(output_key), Some(reducer_key)) =
                (as_column(output_key), as_column(reducer_key))
            else {
                return Ok(None);
            };
            let column = columns[output_key];
            required |= bit(column.rel);
            keys.push((column, reducer_key));
        }

        let role = Role::Reducer(Reducer {
            anti,
            keys,
            required,
        });
        let Some(rel) = self.push_relation(reducer_plan, role)? else {
            return Ok(None);
        };
        self.graph.original_nodes.push((mask | bit(rel), bit(rel)));
        Ok(Some((columns, mask | bit(rel))))
    }

    fn push_relation(
        &mut self,
        plan: &Arc<dyn ExecutionPlan>,
        role: Role,
    ) -> Result<Option<usize>> {
        if plan.boundedness().is_unbounded() {
            // Reordering could break the pipeline properties the other subrules establish.
            return Ok(None);
        }
        if self.graph.relations.len() >= MAX_RELATIONS {
            return Ok(None);
        }
        let statistics = (self.stats)(plan.as_ref())?;
        let Some(rows) = statistics.num_rows.get_value().copied() else {
            return Ok(None);
        };
        let rows = (rows as f64).max(1.0);
        let ndv = statistics
            .column_statistics
            .iter()
            .map(|col| {
                max_distinct_count(&statistics.num_rows, col)
                    .get_value()
                    .map(|ndv| (*ndv as f64).clamp(1.0, rows))
                    .unwrap_or(rows)
            })
            .collect();

        let rel = self.graph.relations.len();
        if matches!(role, Role::Reducer(_)) {
            self.graph.reducers |= bit(rel);
        }
        self.graph.relations.push(Relation {
            plan: Arc::clone(plan),
            rows,
            width: statistics
                .total_byte_size
                .get_value()
                .map(|bytes| *bytes as f64 / rows),
            ndv,
            role,
        });
        Ok(Some(rel))
    }
}
