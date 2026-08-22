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
use datafusion_physical_expr::expressions::{CastExpr, Column, TryCastExpr};
use datafusion_physical_plan::joins::utils::{
    ColumnIndex, JoinFilter, max_distinct_count,
};
use datafusion_physical_plan::joins::{
    CrossJoinExec, HashJoinExec, NestedLoopJoinExec, SortMergeJoinExec,
};
use datafusion_physical_plan::projection::{ProjectionExec, all_alias_free_columns};
use datafusion_physical_plan::{ExecutionPlan, ExecutionPlanProperties};

/// Hard upper bound on the relations in one join graph. The search allocates `2^n`
/// subsets, so larger graphs keep the planner's order regardless of the limit.
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
    /// A side applied to the relations its keys come from rather than joined freely:
    /// the quantified side of a semi or anti join, or the null-supplying side of an
    /// outer join.
    Applied(Applied),
}

/// A side of one join that is applied to the relations its keys come from, as what
/// applying it does to them.
#[derive(Debug)]
pub struct Applied {
    /// What it does to the side it is applied to.
    pub kind: AppliedKind,
    /// What it is joined by.
    pub keys: Vec<AppliedKey>,
    /// Relations the keys reference; it applies only to a set covering them.
    pub required: RelSet,
}

/// One key of an applied join. The relation is emitted whole, so its own side keeps the
/// expression it had; the other side's moves with its column.
#[derive(Debug)]
pub struct AppliedKey {
    /// The key on the side this relation is applied to.
    pub other: Key,
    /// The column this side reads, for its distinct count.
    pub column: usize,
    /// The expression this side compares, as it stood.
    pub expr: PhysicalExprRef,
}

/// The column a mark join's flag is addressed by. It is not a column of the relation the
/// flag comes from but a boolean the join itself produces, so it has no statistics and
/// nothing may use it as a key.
pub const MARK_COLUMN: usize = usize::MAX;

/// What applying one side of a join does to the other.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum AppliedKind {
    /// A semi join: keeps the rows of the other side that match.
    Semi,
    /// An anti join: keeps the rows that do not match.
    Anti,
    /// An outer join: keeps every row of the other side, adding this side's columns and
    /// nulls where nothing matched.
    Outer,
    /// A mark join: keeps every row of the other side, adding one boolean saying whether
    /// anything matched.
    Mark,
}

impl AppliedKind {
    /// The columns this side contributes to the join's output, given how many its plan
    /// has. A semi or anti join contributes none: it emits the rows it kept and nothing
    /// of the side that filtered them.
    pub fn emitted(self, rel: usize, fields: usize) -> Vec<ColRef> {
        match self {
            AppliedKind::Semi | AppliedKind::Anti => vec![],
            AppliedKind::Outer => (0..fields).map(|col| ColRef { rel, col }).collect(),
            AppliedKind::Mark => vec![ColRef {
                rel,
                col: MARK_COLUMN,
            }],
        }
    }

    /// Whether the rows it is applied to carry this side's own columns afterwards, which
    /// is what decides how wide they are. A mark join adds one boolean, not a row.
    pub fn widens(self) -> bool {
        matches!(self, AppliedKind::Outer)
    }

    /// Whether what this side emits depends on where its join lands: an outer join's
    /// nulls and a mark join's flag both do.
    pub fn depends_on_placement(self) -> bool {
        matches!(self, AppliedKind::Outer | AppliedKind::Mark)
    }
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

/// One side of an equi-join predicate: the column it reads, and the expression around
/// that column when the predicate reads more than the column itself. Coercing two sides
/// to one type puts a cast there.
#[derive(Clone, Debug)]
pub struct Key {
    /// The column it reads.
    pub column: ColRef,
    /// `None` when the key is that column itself.
    pub wrapper: Option<PhysicalExprRef>,
}

impl Key {
    fn bare(column: ColRef) -> Self {
        Self {
            column,
            wrapper: None,
        }
    }

    /// This key over `column` instead, when nothing has to be composed to say so.
    fn wrapped(&self, wrapper: &PhysicalExprRef) -> Option<Self> {
        self.wrapper.is_none().then(|| Self {
            column: self.column,
            wrapper: Some(Arc::clone(wrapper)),
        })
    }
}

/// An equi-join predicate `left = right` between two distinct relations.
#[derive(Clone, Debug)]
pub struct Edge {
    /// The key on one side.
    pub left: Key,
    /// The key it is compared against.
    pub right: Key,
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
    /// The relations that are applied to others rather than joined freely.
    pub applied: RelSet,
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

    /// How `rel` applies to the side it is applied to, if it is applied at all.
    pub fn applied(&self, rel: usize) -> Option<&Applied> {
        match &self.relations[rel].role {
            Role::Applied(applied) => Some(applied),
            Role::Output => None,
        }
    }

    /// The relations whose emitted columns depend on where their join lands: an outer
    /// join's nulls and a mark join's flag.
    pub fn placement_dependent(&self) -> RelSet {
        (0..self.relations.len())
            .filter(|rel| {
                self.applied(*rel)
                    .is_some_and(|applied| applied.kind.depends_on_placement())
            })
            .fold(0, |mask, rel| mask | bit(rel))
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
        // A semi, anti or outer variant has no keys to apply it by.
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
    /// One side applied to the other; `output` names the side whose rows survive, and
    /// the other becomes an applied relation.
    Applying {
        kind: AppliedKind,
        output: JoinSide,
    },
}

/// Classifies a join. A full outer join is excluded: it leaves neither side's rows as
/// they were, so neither side can be the one that is applied. A semi, anti, outer or mark
/// join carrying a filter is excluded too, because the filter is part of what the join
/// tests and would have to move with it.
fn join_role(join_type: &JoinType, has_filter: bool) -> Option<JoinRole> {
    let applying = |kind, output| JoinRole::Applying { kind, output };
    let role = match join_type {
        JoinType::Inner => JoinRole::Inner,
        JoinType::LeftSemi => applying(AppliedKind::Semi, JoinSide::Left),
        JoinType::RightSemi => applying(AppliedKind::Semi, JoinSide::Right),
        JoinType::LeftAnti => applying(AppliedKind::Anti, JoinSide::Left),
        JoinType::RightAnti => applying(AppliedKind::Anti, JoinSide::Right),
        // A left outer join keeps its left side's rows and supplies its right side.
        JoinType::Left => applying(AppliedKind::Outer, JoinSide::Left),
        JoinType::Right => applying(AppliedKind::Outer, JoinSide::Right),
        // A left mark join keeps its left side's rows and marks them.
        JoinType::LeftMark => applying(AppliedKind::Mark, JoinSide::Left),
        JoinType::RightMark => applying(AppliedKind::Mark, JoinSide::Right),
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

/// The one column an expression reads, if it reads exactly one and only casts it. A cast
/// is what coercion puts around a join key whose two sides had different types.
fn key_column(expr: &PhysicalExprRef) -> Option<usize> {
    if let Some(column) = expr.downcast_ref::<Column>() {
        return Some(column.index());
    }
    if let Some(cast) = expr.downcast_ref::<CastExpr>() {
        return key_column(cast.expr());
    }
    key_column(expr.downcast_ref::<TryCastExpr>()?.expr())
}

/// What one side of a join predicate reads, in terms of the relations under it.
fn join_key(expr: &PhysicalExprRef, columns: &[Key]) -> Option<Key> {
    let base = columns.get(key_column(expr)?)?;
    if expr.downcast_ref::<Column>().is_some() {
        return Some(base.clone());
    }
    base.wrapped(expr)
}

/// How each column a projection emits maps to its input: a column of it, or a cast of
/// one, which is how a join key coerced to another type is computed.
fn projected(
    projection: &ProjectionExec,
) -> Option<Vec<(usize, Option<PhysicalExprRef>)>> {
    projection
        .expr()
        .iter()
        .map(|proj| {
            if let Some(column) = proj.expr.downcast_ref::<Column>() {
                // A rename would not survive a rebuild that names columns after the
                // relations they come from.
                return (column.name() == proj.alias).then_some((column.index(), None));
            }
            Some((key_column(&proj.expr)?, Some(Arc::clone(&proj.expr))))
        })
        .collect()
}

/// Extracts the maximal reorderable subtree at `plan`. `None` covers every bail-out: an
/// unmodelled join feature, a key over more than one column, missing row counts, too few
/// or too many inputs.
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
                applied: 0,
                kind: None,
            },
            stats,
        }
    }

    fn extract(mut self, plan: &Arc<dyn ExecutionPlan>) -> Result<Option<JoinGraph>> {
        let Some((output, _)) = self.visit(plan)? else {
            return Ok(None);
        };
        // A derived column would have to be computed again above the rebuilt tree.
        let Some(output) = output
            .iter()
            .map(|key| key.wrapper.is_none().then_some(key.column))
            .collect::<Option<Vec<_>>>()
        else {
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
        // Which rows of a null-supplied relation are null, and which rows a mark join
        // marks, depend on where that join lands, so a predicate over one of those
        // columns has to be evaluated there and cannot be moved. Rather than track that,
        // leave the subtree alone: another predicate over such a relation, including
        // another outer or mark join's keys, is a graph this rule does not reorder.
        let derived = graph.placement_dependent();
        let reaches_nulls = |mask: RelSet| mask & derived != 0;
        let reordering_would_move_a_predicate = graph.edges.iter().any(|edge| {
            reaches_nulls(bit(edge.left.column.rel) | bit(edge.right.column.rel))
        }) || graph
            .filters
            .iter()
            .any(|filter| reaches_nulls(filter.required))
            || (0..graph.relations.len()).any(|rel| {
                graph
                    .applied(rel)
                    .is_some_and(|applied| reaches_nulls(applied.required))
            });
        if reordering_would_move_a_predicate {
            return Ok(None);
        }
        Ok(Some(graph))
    }

    fn visit(
        &mut self,
        plan: &Arc<dyn ExecutionPlan>,
    ) -> Result<Option<(Vec<Key>, RelSet)>> {
        if let Some(view) = join_view(plan)
            && agree(self.graph.null_equality, view.null_equality)
            && agree(self.graph.kind, view.kind)
        {
            self.graph.null_equality = self.graph.null_equality.or(view.null_equality);
            self.graph.kind = self.graph.kind.or(view.kind);
            let visited = match view.role {
                JoinRole::Inner => self.visit_inner(&view)?,
                JoinRole::Applying { kind, output } => {
                    self.visit_applying(&view, kind, output)?
                }
            };
            let Some((columns, mask)) = visited else {
                return Ok(None);
            };

            // For a semi or anti join the projection selects from the output side alone.
            let columns = match view.projection {
                Some(projection) => {
                    projection.iter().map(|idx| columns[*idx].clone()).collect()
                }
                None => columns,
            };
            Ok(Some((columns, mask)))
        } else if let Some(projection) = plan.downcast_ref::<ProjectionExec>()
            && let Some(slots) = projected(projection)
        {
            // Looking through the projections between joins lets the enumerator see a
            // whole chain, since `ProjectionPushdown` has not folded them into the joins
            // yet, and the ones holding a coerced join key are how casts reach it.
            let Some((child, mask)) = self.visit(projection.input())? else {
                return Ok(None);
            };
            let columns = slots
                .iter()
                .map(|(index, wrapper)| {
                    let base = child.get(*index)?;
                    match wrapper {
                        None => Some(base.clone()),
                        Some(wrapper) => base.wrapped(wrapper),
                    }
                })
                .collect::<Option<Vec<_>>>();
            Ok(columns.map(|columns| (columns, mask)))
        } else {
            let Some(rel) = self.push_relation(plan, Role::Output)? else {
                return Ok(None);
            };
            Ok(Some((
                (0..plan.schema().fields().len())
                    .map(|col| Key::bare(ColRef { rel, col }))
                    .collect(),
                bit(rel),
            )))
        }
    }

    /// Flattens an inner join: both sides join the graph, predicates become edges
    /// and filters.
    fn visit_inner(&mut self, view: &JoinView) -> Result<Option<(Vec<Key>, RelSet)>> {
        let Some((left, left_mask)) = self.visit(view.left)? else {
            return Ok(None);
        };
        let Some((right, right_mask)) = self.visit(view.right)? else {
            return Ok(None);
        };

        for (left_key, right_key) in view.on {
            let (Some(left), Some(right)) =
                (join_key(left_key, &left), join_key(right_key, &right))
            else {
                return Ok(None);
            };
            let edge = Edge { left, right };
            // Duplicates would be double counted by the cost model.
            if !self.graph.edges.iter().any(|other| {
                (other.left.column, other.right.column)
                    == (edge.left.column, edge.right.column)
            }) {
                self.graph.edges.push(edge);
            }
        }

        if let Some(filter) = view.filter {
            let columns = filter
                .column_indices()
                .iter()
                .map(|ColumnIndex { index, side }| {
                    let key = match side {
                        JoinSide::Left => left.get(*index),
                        JoinSide::Right => right.get(*index),
                        JoinSide::None => None,
                    }?;
                    // A derived column would have to be computed to test the filter.
                    key.wrapper.is_none().then_some(key.column)
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

    /// Flattens a join that applies one side to the other: the surviving side joins the
    /// graph, and the side applied to it becomes one applied relation.
    fn visit_applying(
        &mut self,
        view: &JoinView,
        kind: AppliedKind,
        output: JoinSide,
    ) -> Result<Option<(Vec<Key>, RelSet)>> {
        let (output_plan, applied_plan) = match output {
            JoinSide::Left => (view.left, view.right),
            JoinSide::Right => (view.right, view.left),
            JoinSide::None => return internal_err!("applying join with no output side"),
        };

        let Some((columns, mask)) = self.visit(output_plan)? else {
            return Ok(None);
        };

        // Keys resolve against the surviving side, so they precede the applied relation.
        let mut keys = Vec::with_capacity(view.on.len());
        let mut required = 0;
        for (left_key, right_key) in view.on {
            let (output_key, applied_key) = match output {
                JoinSide::Left => (left_key, right_key),
                _ => (right_key, left_key),
            };
            let (Some(other), Some(column)) =
                (join_key(output_key, &columns), key_column(applied_key))
            else {
                return Ok(None);
            };
            required |= bit(other.column.rel);
            keys.push(AppliedKey {
                other,
                column,
                expr: Arc::clone(applied_key),
            });
        }

        let fields = applied_plan.schema().fields().len();
        let role = Role::Applied(Applied {
            kind,
            keys,
            required,
        });
        let Some(rel) = self.push_relation(applied_plan, role)? else {
            return Ok(None);
        };
        self.graph.original_nodes.push((mask | bit(rel), bit(rel)));

        // An outer join emits both of its sides, in the order its inputs were in; a mark
        // join emits the side it marked and then the flag.
        let mine = kind.emitted(rel, fields).into_iter().map(Key::bare);
        // A mark join's flag comes last whichever side it marked; an outer join's columns
        // keep the position its input had.
        let appended =
            output == JoinSide::Left || kind == AppliedKind::Mark || mine.len() == 0;
        let columns = if appended {
            columns.into_iter().chain(mine).collect()
        } else {
            mine.chain(columns).collect()
        };
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
        if matches!(role, Role::Applied(_)) {
            self.graph.applied |= bit(rel);
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
