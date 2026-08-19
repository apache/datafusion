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

//! Cost-based join order enumeration.
//!
//! A subtree of joins is flattened into relations plus the predicates between
//! them, a dynamic program searches the orders (bushy as well as left-deep) under
//! a `C_out` cost model, and the subtree is rebuilt if the winner is cheaper by a
//! clear margin. `JoinSelection` then picks each join's build side and partition
//! mode against the shape chosen here.
//!
//! Reordering is sound because a tree of inner joins equals the cross product of
//! its relations filtered by all its predicates: any tree applying every
//! predicate exactly once, where the columns it needs are available, computes the
//! same rows. Semi and anti joins take part as reducers, since they filter their
//! output side rather than contributing columns of their own.

use std::collections::HashMap;
use std::sync::Arc;

use crate::PhysicalOptimizerRule;
use crate::optimizer::{ConfigOnlyContext, PhysicalOptimizerContext};

use arrow::compute::SortOptions;
use arrow::datatypes::{FieldRef, Schema};
use datafusion_common::config::ConfigOptions;
use datafusion_common::error::Result;
use datafusion_common::tree_node::{Transformed, TransformedResult, TreeNode};
use datafusion_common::{JoinSide, JoinType, NullEquality, Statistics, internal_err};
use datafusion_expr_common::operator::Operator;
use datafusion_physical_expr::PhysicalExprRef;
use datafusion_physical_expr::expressions::{BinaryExpr, Column};
use datafusion_physical_expr::projection::ProjectionExpr;
use datafusion_physical_plan::execution_plan::replace_children_if_necessary;
use datafusion_physical_plan::joins::utils::{
    ColumnIndex, JoinFilter, max_distinct_count,
};
use datafusion_physical_plan::joins::{
    HashJoinExec, HashJoinExecBuilder, PartitionMode, SortMergeJoinExec,
};
use datafusion_physical_plan::operator_statistics::StatisticsRegistry;
use datafusion_physical_plan::projection::{ProjectionExec, all_alias_free_columns};
use datafusion_physical_plan::statistics::{StatisticsArgs, StatisticsContext};
use datafusion_physical_plan::{ExecutionPlan, ExecutionPlanProperties};

/// Chooses the shape of the join tree, before [`JoinSelection`] decides how each
/// join runs.
///
/// [`JoinSelection`]: crate::join_selection::JoinSelection
#[derive(Default, Debug)]
pub struct JoinEnumeration {}

impl JoinEnumeration {
    #[expect(missing_docs)]
    pub fn new() -> Self {
        Self {}
    }
}

impl PhysicalOptimizerRule for JoinEnumeration {
    fn optimize(
        &self,
        plan: Arc<dyn ExecutionPlan>,
        config: &ConfigOptions,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        self.optimize_with_context(plan, &ConfigOnlyContext::new(config))
    }

    fn optimize_with_context(
        &self,
        plan: Arc<dyn ExecutionPlan>,
        context: &dyn PhysicalOptimizerContext,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let config = context.config_options();
        if !config.optimizer.join_enumeration {
            return Ok(plan);
        }
        let mut default_registry = None;
        let registry: Option<&StatisticsRegistry> =
            if config.optimizer.use_statistics_registry {
                Some(context.statistics_registry().unwrap_or_else(|| {
                    default_registry
                        .insert(StatisticsRegistry::default_with_builtin_providers())
                }))
            } else {
                None
            };
        let mut stats = |plan: &dyn ExecutionPlan| {
            if let Some(registry) = registry {
                registry
                    .compute(plan)
                    .map(|s| Arc::<Statistics>::clone(s.base_arc()))
            } else {
                StatisticsContext::new().compute(plan, &StatisticsArgs::new())
            }
        };
        Ok(enumerate_join_order(&plan, config, &mut stats)?.unwrap_or(plan))
    }

    fn name(&self) -> &str {
        "join_enumeration"
    }

    fn schema_check(&self) -> bool {
        true
    }
}

/// Hard upper bound on the relations in one join graph, and on the exhaustive
/// search, which allocates `2^n` and visits `3^n`. Larger graphs keep the
/// planner's order however high `join_enumeration_limit` is set.
const MAX_RELATIONS: usize = 16;

/// Computes a plan node's statistics, shared with the rest of `JoinSelection`.
pub(crate) type StatsFn<'a> =
    dyn FnMut(&dyn ExecutionPlan) -> Result<Arc<Statistics>> + 'a;

/// A bitmask over relation indices.
type RelSet = u64;

fn bit(rel: usize) -> RelSet {
    1u64 << rel
}

fn iter_rels(mask: RelSet) -> impl Iterator<Item = usize> {
    std::iter::successors(Some(mask), |m| Some(m & m.wrapping_sub(1)))
        .take_while(|m| *m != 0)
        .map(|m| m.trailing_zeros() as usize)
}

fn covers(mask: RelSet, required: RelSet) -> bool {
    required & !mask == 0
}

/// One column of one relation. Reordering moves columns, so plumbing uses these
/// rather than indices.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
struct ColRef {
    rel: usize,
    col: usize,
}

/// What a relation contributes to the join.
#[derive(Debug)]
enum Role {
    Output,
    /// The quantified side of a semi or anti join, which filters instead of
    /// contributing columns.
    Reducer(Reducer),
}

#[derive(Debug)]
struct Reducer {
    /// `true` for an anti join, which keeps the rows that do *not* match.
    anti: bool,
    /// Keys, as `(column of the filtered side, column index here)`.
    keys: Vec<(ColRef, usize)>,
    /// Relations the keys reference; this reducer applies only to a set covering them.
    required: RelSet,
}

/// One leaf of the join graph: a subplan the enumerator does not look inside.
#[derive(Debug)]
struct Relation {
    plan: Arc<dyn ExecutionPlan>,
    /// Estimated row count, clamped to at least 1.
    rows: f64,
    /// Per-column distinct value estimate, clamped to `[1, rows]`.
    ndv: Vec<f64>,
    role: Role,
}

/// An equi-join predicate `left = right` between two distinct relations.
#[derive(Clone, Copy, Debug)]
struct Edge {
    left: ColRef,
    right: ColRef,
}

/// A non-equi join predicate, moved along with its column references rewritten.
#[derive(Debug)]
struct Filter {
    filter: JoinFilter,
    /// The column each entry of the filter's intermediate schema comes from.
    columns: Vec<ColRef>,
    /// The relations those columns belong to.
    required: RelSet,
}

/// A connected set of joins as relations plus the predicates between them.
#[derive(Debug)]
struct JoinGraph {
    relations: Vec<Relation>,
    edges: Vec<Edge>,
    filters: Vec<Filter>,
    /// Columns the original subtree emitted; the rebuilt one reproduces this exactly.
    output: Vec<ColRef>,
    /// Null handling shared by the subtree's joins. A join that differs becomes a
    /// relation instead.
    null_equality: Option<NullEquality>,
    /// The original tree's internal nodes, for scoring it against the alternatives.
    original_nodes: Vec<RelSet>,
    /// The relations that are reducers rather than ordinary inputs.
    reducers: RelSet,
    /// Which join operator the subtree used, and which the rebuild emits.
    kind: Option<JoinKind>,
}

impl JoinGraph {
    fn ndv(&self, col: ColRef) -> f64 {
        self.relations[col.rel].ndv[col.col]
    }

    fn all(&self) -> RelSet {
        (0..self.relations.len()).fold(0, |mask, rel| mask | bit(rel))
    }

    fn reducer(&self, rel: usize) -> Option<&Reducer> {
        match &self.relations[rel].role {
            Role::Reducer(reducer) => Some(reducer),
            Role::Output => None,
        }
    }

    fn kind(&self) -> JoinKind {
        self.kind.unwrap_or(JoinKind::Hash)
    }

    fn null_equality(&self) -> NullEquality {
        self.null_equality
            .unwrap_or(NullEquality::NullEqualsNothing)
    }
}

/// A valid way of combining two relation sets.
#[derive(Clone, Copy, Debug)]
enum Combine {
    Inner,
    /// A semi or anti join applying `reducer` to the opposite set.
    Reducer {
        reducer: usize,
    },
}

/// Cardinality and cost estimates over the subsets of a [`JoinGraph`].
struct CostModel<'a> {
    graph: &'a JoinGraph,
    /// Aggregated selectivity per connected relation pair, as
    /// `(rel_a, rel_b, selectivity)` with `rel_a < rel_b`.
    pair_selectivity: Vec<(usize, usize, f64)>,
    /// Neighbours of each relation. Reducers neighbour nothing: they are applied.
    adjacency: Vec<RelSet>,
    /// Fraction of its filtered side each reducer keeps; `1.0` for non-reducers.
    reducer_selectivity: Vec<f64>,
    /// Selectivity of each non-equi filter, with the relations it needs.
    filter_selectivity: Vec<(RelSet, f64)>,
}

impl<'a> CostModel<'a> {
    fn new(graph: &'a JoinGraph, config: &ConfigOptions) -> Self {
        // Denominate each relation pair by its most selective key, as
        // `estimate_inner_join_cardinality` does, so the two models agree.
        let mut denominators: HashMap<(usize, usize), f64> = HashMap::new();
        for edge in &graph.edges {
            let (a, b) = (edge.left.rel, edge.right.rel);
            let key = if a < b { (a, b) } else { (b, a) };
            let denominator = graph.ndv(edge.left).max(graph.ndv(edge.right)).max(1.0);
            denominators
                .entry(key)
                .and_modify(|current| *current = current.max(denominator))
                .or_insert(denominator);
        }

        let mut adjacency = vec![0; graph.relations.len()];
        let mut pair_selectivity = Vec::with_capacity(denominators.len());
        for ((a, b), denominator) in denominators {
            adjacency[a] |= bit(b);
            adjacency[b] |= bit(a);
            pair_selectivity.push((a, b, 1.0 / denominator));
        }
        // `HashMap` iteration order is not deterministic, but plans must be.
        pair_selectivity.sort_unstable_by_key(|(a, b, _)| (*a, *b));

        let reducer_selectivity = (0..graph.relations.len())
            .map(|rel| match graph.reducer(rel) {
                None => 1.0,
                Some(reducer) => {
                    // Fraction of the filtered side's key values the reducer covers.
                    let matched = reducer
                        .keys
                        .iter()
                        .map(|(filtered, col)| {
                            let reducer_ndv = graph.ndv(ColRef { rel, col: *col });
                            (reducer_ndv / graph.ndv(*filtered)).clamp(0.0, 1.0)
                        })
                        .fold(1.0f64, f64::min);
                    if reducer.anti { 1.0 - matched } else { matched }
                }
            })
            .collect();

        // A non-equi filter has no statistics, so it takes the optimizer's default.
        let default_selectivity =
            f64::from(config.optimizer.default_filter_selectivity) / 100.0;
        let filter_selectivity = graph
            .filters
            .iter()
            .map(|filter| (filter.required, default_selectivity))
            .collect();

        Self {
            graph,
            pair_selectivity,
            adjacency,
            reducer_selectivity,
            filter_selectivity,
        }
    }

    /// Estimated rows from joining every relation in `mask`: the product of the
    /// relation sizes scaled by the predicates that apply within it. Depending only
    /// on the *set*, not the tree shape, is what makes the dynamic program valid.
    fn cardinality(&self, mask: RelSet) -> f64 {
        let mut rows = 1.0;
        for rel in iter_rels(mask) {
            // A reducer contributes a selectivity, not rows.
            rows *= match self.graph.reducer(rel) {
                Some(_) => self.reducer_selectivity[rel],
                None => self.graph.relations[rel].rows,
            };
        }
        for (a, b, selectivity) in &self.pair_selectivity {
            if mask & bit(*a) != 0 && mask & bit(*b) != 0 {
                rows *= selectivity;
            }
        }
        for (required, selectivity) in &self.filter_selectivity {
            if covers(mask, *required) {
                rows *= selectivity;
            }
        }
        rows.max(1.0)
    }

    fn connected(&self, left: RelSet, right: RelSet) -> bool {
        iter_rels(left).any(|rel| self.adjacency[rel] & right != 0)
    }

    fn combine(&self, left: RelSet, right: RelSet) -> Option<Combine> {
        let reducers = self.graph.reducers;
        // A lone reducer is applied to the other side, which must supply its keys.
        for (reducer_side, filtered) in [(right, left), (left, right)] {
            if reducer_side.is_power_of_two() && reducer_side & reducers != 0 {
                let reducer = reducer_side.trailing_zeros() as usize;
                let required = self.graph.reducer(reducer)?.required;
                return covers(filtered, required)
                    .then_some(Combine::Reducer { reducer });
            }
        }
        // Otherwise an inner join: both sides contribute columns and a predicate must
        // connect them. Cross products are never introduced.
        if left & !reducers == 0 || right & !reducers == 0 {
            return None;
        }
        self.connected(left, right).then_some(Combine::Inner)
    }

    /// `C_out`: the sum of the internal nodes' cardinalities. Leaves are excluded as
    /// every candidate reads the same relations.
    fn tree_cost(&self, nodes: &[RelSet]) -> f64 {
        nodes
            .iter()
            .filter(|mask| mask.count_ones() > 1)
            .map(|mask| self.cardinality(*mask))
            .sum()
    }
}

struct Solution {
    splits: HashMap<RelSet, RelSet>,
    cost: f64,
}

/// Exhaustive dynamic programming over connected relation subsets, costing each
/// once by trying every split, so bushy shapes are considered too. `O(3^n)`.
fn solve_dp(model: &CostModel) -> Option<Solution> {
    let n = model.graph.relations.len();
    let full: RelSet = model.graph.all();
    let size = 1usize << n;

    // `INFINITY` marks a subset that cannot be built: disconnected, or holding a
    // reducer whose columns it does not cover.
    let mut cost = vec![f64::INFINITY; size];
    let mut split = vec![0 as RelSet; size];
    for rel in 0..n {
        cost[bit(rel) as usize] = 0.0;
    }

    for mask in 1..=full {
        if mask.count_ones() < 2 {
            continue;
        }
        let cardinality = model.cardinality(mask);
        // Subsets containing the lowest set bit, so each pair of halves is seen once.
        let lowest = mask & mask.wrapping_neg();
        let mut left = mask;
        let mut best = f64::INFINITY;
        let mut best_left = 0;
        while left != 0 {
            left = (left - 1) & mask;
            if left & lowest == 0 {
                continue;
            }
            let right = mask ^ left;
            if right == 0 {
                continue;
            }
            let (left_cost, right_cost) = (cost[left as usize], cost[right as usize]);
            if !left_cost.is_finite()
                || !right_cost.is_finite()
                || model.combine(left, right).is_none()
            {
                continue;
            }
            let candidate = left_cost + right_cost + cardinality;
            if candidate < best {
                best = candidate;
                best_left = left;
            }
        }
        if best.is_finite() {
            cost[mask as usize] = best;
            split[mask as usize] = best_left;
        }
    }

    if !cost[full as usize].is_finite() {
        return None;
    }

    let mut splits = HashMap::new();
    let mut stack = vec![full];
    while let Some(mask) = stack.pop() {
        if mask.count_ones() < 2 {
            continue;
        }
        let left = split[mask as usize];
        splits.insert(mask, left);
        stack.push(left);
        stack.push(mask ^ left);
    }

    Some(Solution {
        splits,
        cost: cost[full as usize],
    })
}

/// The join operators the rule can flatten and rebuild.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum JoinKind {
    Hash,
    /// Emits every column, having no built-in projection; the subtree gets one
    /// projection on top instead.
    SortMerge,
}

/// One join, seen the same way whichever operator implements it.
struct JoinView<'a> {
    kind: JoinKind,
    role: JoinRole,
    left: &'a Arc<dyn ExecutionPlan>,
    right: &'a Arc<dyn ExecutionPlan>,
    on: &'a [(PhysicalExprRef, PhysicalExprRef)],
    filter: Option<&'a JoinFilter>,
    null_equality: NullEquality,
    projection: Option<&'a [usize]>,
}

fn join_view(plan: &Arc<dyn ExecutionPlan>) -> Option<JoinView<'_>> {
    if let Some(join) = plan.downcast_ref::<HashJoinExec>() {
        if join.null_aware || join.fetch().is_some() {
            return None;
        }
        return Some(JoinView {
            kind: JoinKind::Hash,
            role: join_role(join.join_type(), join.filter().is_some(), join.on())?,
            left: join.left(),
            right: join.right(),
            on: join.on(),
            filter: join.filter(),
            null_equality: join.null_equality,
            projection: join.projection.as_deref(),
        });
    }
    let join = plan.downcast_ref::<SortMergeJoinExec>()?;
    Some(JoinView {
        kind: JoinKind::SortMerge,
        role: join_role(&join.join_type(), join.filter().is_some(), join.on())?,
        left: join.left(),
        right: join.right(),
        on: join.on(),
        filter: join.filter().as_ref(),
        null_equality: join.null_equality(),
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

/// Classifies a join. Outer and mark joins are excluded (not filters on their
/// inputs, or they add a column), as are `null_aware` anti joins, joins with a
/// limit, and semi joins whose filter is part of their existential test.
fn join_role(
    join_type: &JoinType,
    has_filter: bool,
    on: &[(PhysicalExprRef, PhysicalExprRef)],
) -> Option<JoinRole> {
    if on.is_empty() {
        return None;
    }
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

fn as_column(expr: &PhysicalExprRef) -> Option<usize> {
    expr.downcast_ref::<Column>().map(|col| col.index())
}

fn position(columns: &[ColRef], col: ColRef) -> Option<usize> {
    columns.iter().position(|candidate| *candidate == col)
}

fn push_unique(columns: &mut Vec<ColRef>, col: ColRef) {
    if position(columns, col).is_none() {
        columns.push(col);
    }
}

fn extend_required(columns: &mut Vec<ColRef>, wanted: &[ColRef], side: RelSet) {
    for col in wanted.iter().filter(|col| side & bit(col.rel) != 0) {
        push_unique(columns, *col);
    }
}

/// Extracts the maximal reorderable subtree at `plan`. `None` covers every
/// bail-out: an unmodelled join feature, a key that is not a plain column,
/// missing row counts, too few or too many relations.
fn extract(
    plan: &Arc<dyn ExecutionPlan>,
    stats: &mut StatsFn,
) -> Result<Option<JoinGraph>> {
    // Start at a join, or at the column pruning projection usually sitting above
    // one: rooting there lets its column list become the top join's projection
    // instead of stranding a `ProjectionExec` above a wider join.
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
            && self
                .graph
                .null_equality
                .is_none_or(|null_equality| null_equality == view.null_equality)
            && self.graph.kind.is_none_or(|kind| kind == view.kind)
        {
            self.graph.null_equality = Some(view.null_equality);
            self.graph.kind = Some(view.kind);
            let visited = match view.role {
                JoinRole::Inner => self.visit_inner(&view)?,
                JoinRole::Reducing { anti, output } => {
                    self.visit_reducing(&view, anti, output)?
                }
            };
            let Some((columns, mask)) = visited else {
                return Ok(None);
            };

            self.graph.original_nodes.push(mask);
            // For a semi or anti join the projection selects from the output side alone.
            let columns = match view.projection {
                Some(projection) => projection.iter().map(|idx| columns[*idx]).collect(),
                None => columns,
            };
            Ok(Some((columns, mask)))
        } else if let Some(projection) = plan.downcast_ref::<ProjectionExec>()
            && all_alias_free_columns(projection.expr())
        {
            // Looking through pruning projections is what lets the enumerator see a whole
            // chain, since `ProjectionPushdown` has not folded them into the joins yet.
            // `all_alias_free_columns` rules out renaming, so dropping them is safe.
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
            ndv,
            role,
        });
        Ok(Some(rel))
    }
}

struct Rebuilder<'a> {
    graph: &'a JoinGraph,
    model: &'a CostModel<'a>,
    solution: &'a Solution,
    /// Each relation's plan, already rewritten if it held a join subtree.
    relations: &'a [Arc<dyn ExecutionPlan>],
}

impl Rebuilder<'_> {
    fn node(
        &self,
        mask: RelSet,
        required: &[ColRef],
    ) -> Result<(Arc<dyn ExecutionPlan>, Vec<ColRef>)> {
        if mask.is_power_of_two() {
            // A single relation, opaque here; narrowing is `ProjectionPushdown`'s job.
            let rel = mask.trailing_zeros() as usize;
            let plan = Arc::clone(&self.relations[rel]);
            let columns = (0..plan.schema().fields().len())
                .map(|col| ColRef { rel, col })
                .collect();
            return Ok((plan, columns));
        }

        let Some(split) = self.solution.splits.get(&mask).copied() else {
            return internal_err!("join enumeration produced no split for {mask:b}");
        };
        let other = mask ^ split;
        match self.model.combine(split, other) {
            None => internal_err!("join enumeration produced an invalid join"),
            Some(Combine::Reducer { reducer }) => self.reducing(mask, required, reducer),
            Some(Combine::Inner) => {
                // Cheaper side on the left; `JoinSelection`'s swap may still revise it.
                let (left, right) =
                    if self.model.cardinality(split) <= self.model.cardinality(other) {
                        (split, other)
                    } else {
                        (other, split)
                    };
                self.inner(required, left, right)
            }
        }
    }

    /// Builds one join of the kind the subtree used.
    fn build_join(
        &self,
        left: Built,
        right: Built,
        on: Vec<(PhysicalExprRef, PhysicalExprRef)>,
        join_type: JoinType,
        filter: Option<JoinFilter>,
        required: &[ColRef],
    ) -> Result<(Arc<dyn ExecutionPlan>, Vec<ColRef>)> {
        // A semi or anti join emits one side, and a reducer's own columns are never
        // emitted, so its `Built` carries none.
        let natural = match join_type {
            JoinType::RightSemi | JoinType::RightAnti => right.columns.clone(),
            JoinType::LeftSemi | JoinType::LeftAnti => left.columns.clone(),
            _ => {
                let mut natural = left.columns.clone();
                natural.extend(right.columns.clone());
                natural
            }
        };
        let keys = on.len();
        match self.graph.kind() {
            JoinKind::Hash => {
                let join = HashJoinExecBuilder::new(left.plan, right.plan, on, join_type)
                    .with_filter(filter)
                    .with_null_equality(self.graph.null_equality())
                    // Build side and partition mode are picked by the
                    // statistical subrule after.
                    .with_partition_mode(PartitionMode::Auto)
                    .with_projection(projection_for(required, &natural)?)
                    .build()?;
                Ok((Arc::new(join), required.to_vec()))
            }
            JoinKind::SortMerge => {
                let join = SortMergeJoinExec::try_new(
                    left.plan,
                    right.plan,
                    on,
                    filter,
                    join_type,
                    vec![SortOptions::default(); keys],
                    self.graph.null_equality(),
                )?;
                Ok((Arc::new(join), natural))
            }
        }
    }

    fn inner(
        &self,
        required: &[ColRef],
        left_mask: RelSet,
        right_mask: RelSet,
    ) -> Result<(Arc<dyn ExecutionPlan>, Vec<ColRef>)> {
        // Each edge crosses exactly one cut, at the lowest node holding both endpoints.
        let mut keys: Vec<(ColRef, ColRef)> = vec![];
        for edge in &self.graph.edges {
            let (left, right) = (edge.left, edge.right);
            if left_mask & bit(left.rel) != 0 && right_mask & bit(right.rel) != 0 {
                keys.push((left, right));
            } else if left_mask & bit(right.rel) != 0 && right_mask & bit(left.rel) != 0 {
                keys.push((right, left));
            }
        }
        if keys.is_empty() {
            return internal_err!("join enumeration produced a cross product");
        }

        // Filters land at their lowest common ancestor: covered here, by neither input.
        let mask = left_mask | right_mask;
        let filters: Vec<&Filter> = self
            .graph
            .filters
            .iter()
            .filter(|filter| {
                covers(mask, filter.required)
                    && !covers(left_mask, filter.required)
                    && !covers(right_mask, filter.required)
            })
            .collect();

        // Each side emits this join's keys, its filters' columns, and what is asked
        // for above.
        let child_required = |side: RelSet, take_left: bool| {
            let mut columns: Vec<ColRef> = vec![];
            for (left, right) in &keys {
                push_unique(&mut columns, if take_left { *left } else { *right });
            }
            for filter in &filters {
                extend_required(&mut columns, &filter.columns, side);
            }
            extend_required(&mut columns, required, side);
            columns
        };

        let (left_plan, left_columns) =
            self.node(left_mask, &child_required(left_mask, true))?;
        let (right_plan, right_columns) =
            self.node(right_mask, &child_required(right_mask, false))?;

        let on = keys
            .iter()
            .map(|(left, right)| {
                Ok((
                    key_expr(&left_columns, &left_plan, *left)?,
                    key_expr(&right_columns, &right_plan, *right)?,
                ))
            })
            .collect::<Result<Vec<_>>>()?;
        let filter = rebuild_filters(&filters, &left_columns, &right_columns)?;

        self.build_join(
            Built {
                plan: left_plan,
                columns: left_columns,
            },
            Built {
                plan: right_plan,
                columns: right_columns,
            },
            on,
            JoinType::Inner,
            filter,
            required,
        )
    }

    fn reducing(
        &self,
        mask: RelSet,
        required: &[ColRef],
        reducer: usize,
    ) -> Result<(Arc<dyn ExecutionPlan>, Vec<ColRef>)> {
        let Some(info) = self.graph.reducer(reducer) else {
            return internal_err!("relation {reducer} is not a reducer");
        };
        let filtered_mask = mask & !bit(reducer);

        // Filters never land here: they reference output relations only, so their
        // lowest covering node is always inside the filtered side.
        let mut filtered_required: Vec<ColRef> = vec![];
        for (column, _) in &info.keys {
            push_unique(&mut filtered_required, *column);
        }
        extend_required(&mut filtered_required, required, filtered_mask);

        let (filtered_plan, filtered_columns) =
            self.node(filtered_mask, &filtered_required)?;
        let reducer_plan = Arc::clone(&self.relations[reducer]);

        // Reducer on the build side so the filtered side streams: `RightSemi` and
        // `RightAnti` emit rows of their right input.
        let reducer_schema = reducer_plan.schema();
        let on = info
            .keys
            .iter()
            .map(|(column, reducer_col)| {
                let reducer_key = Arc::new(Column::new(
                    reducer_schema.field(*reducer_col).name(),
                    *reducer_col,
                )) as _;
                Ok((
                    reducer_key,
                    key_expr(&filtered_columns, &filtered_plan, *column)?,
                ))
            })
            .collect::<Result<Vec<_>>>()?;

        let join_type = if info.anti {
            JoinType::RightAnti
        } else {
            JoinType::RightSemi
        };

        self.build_join(
            Built {
                plan: reducer_plan,
                columns: vec![],
            },
            Built {
                plan: filtered_plan,
                columns: filtered_columns,
            },
            on,
            join_type,
            None,
            required,
        )
    }
}

fn key_expr(
    columns: &[ColRef],
    plan: &Arc<dyn ExecutionPlan>,
    col: ColRef,
) -> Result<PhysicalExprRef> {
    let Some(index) = position(columns, col) else {
        return internal_err!("join enumeration lost column {col:?}");
    };
    Ok(Arc::new(Column::new(plan.schema().field(index).name(), index)) as _)
}

fn projection_for(required: &[ColRef], emitted: &[ColRef]) -> Result<Option<Vec<usize>>> {
    let mut projection = Vec::with_capacity(required.len());
    for col in required {
        let Some(index) = position(emitted, *col) else {
            return internal_err!("join enumeration lost column {col:?}");
        };
        projection.push(index);
    }
    let identity = projection.len() == emitted.len()
        && projection.iter().enumerate().all(|(idx, col)| idx == *col);
    Ok((!identity).then_some(projection))
}

/// One built input and the columns it emits.
struct Built {
    plan: Arc<dyn ExecutionPlan>,
    columns: Vec<ColRef>,
}

/// Rebuilds the non-equi filters applied at one join as one conjunction. A
/// [`JoinFilter`] addresses an intermediate batch by index, so merging means
/// concatenating those schemas and shifting all but the first filter's indices.
fn rebuild_filters(
    filters: &[&Filter],
    left_columns: &[ColRef],
    right_columns: &[ColRef],
) -> Result<Option<JoinFilter>> {
    let Some((first, rest)) = filters.split_first() else {
        return Ok(None);
    };

    let mut expression = Arc::clone(first.filter.expression());
    let mut fields: Vec<FieldRef> =
        first.filter.schema().fields().iter().cloned().collect();
    let mut columns: Vec<ColRef> = first.columns.clone();
    for filter in rest {
        let shifted =
            shift_columns(Arc::clone(filter.filter.expression()), fields.len())?;
        expression = Arc::new(BinaryExpr::new(expression, Operator::And, shifted)) as _;
        fields.extend(filter.filter.schema().fields().iter().cloned());
        columns.extend_from_slice(&filter.columns);
    }

    let column_indices = columns
        .iter()
        .map(|col| {
            if let Some(index) = position(left_columns, *col) {
                Ok(ColumnIndex {
                    index,
                    side: JoinSide::Left,
                })
            } else if let Some(index) = position(right_columns, *col) {
                Ok(ColumnIndex {
                    index,
                    side: JoinSide::Right,
                })
            } else {
                internal_err!("join enumeration lost filter column {col:?}")
            }
        })
        .collect::<Result<Vec<_>>>()?;

    // Intermediate columns go left side first: a hash join reads them positionally,
    // but a sort merge join rebuilds the batch as all left then all right columns,
    // so interleaved sides would evaluate against the wrong columns.
    let mut order: Vec<usize> = (0..column_indices.len()).collect();
    order.sort_by_key(|i| column_indices[*i].side == JoinSide::Right);
    let mut moved = vec![0; order.len()];
    for (to, from) in order.iter().enumerate() {
        moved[*from] = to;
    }

    Ok(Some(JoinFilter::new(
        remap_columns(expression, &moved)?,
        order.iter().map(|i| column_indices[*i].clone()).collect(),
        Arc::new(Schema::new(
            order
                .iter()
                .map(|i| Arc::clone(&fields[*i]))
                .collect::<Vec<_>>(),
        )),
    )))
}

fn shift_columns(expression: PhysicalExprRef, offset: usize) -> Result<PhysicalExprRef> {
    if offset == 0 {
        return Ok(expression);
    }
    rewrite_columns(expression, &|index| index + offset)
}

fn remap_columns(
    expression: PhysicalExprRef,
    moved: &[usize],
) -> Result<PhysicalExprRef> {
    if moved.iter().enumerate().all(|(from, to)| from == *to) {
        return Ok(expression);
    }
    rewrite_columns(expression, &|index| moved[index])
}

fn rewrite_columns(
    expression: PhysicalExprRef,
    index: &dyn Fn(usize) -> usize,
) -> Result<PhysicalExprRef> {
    expression
        .transform(|expr| {
            Ok(match expr.downcast_ref::<Column>() {
                Some(column) => Transformed::yes(Arc::new(Column::new(
                    column.name(),
                    index(column.index()),
                )) as _),
                None => Transformed::no(expr),
            })
        })
        .data()
}

type RewrittenRelation = (Arc<dyn ExecutionPlan>, Arc<dyn ExecutionPlan>);

/// Substitutes rewritten relations into a subtree, leaving its shape untouched.
/// The relations are the exact `Arc`s taken from it, so pointer identity finds
/// them, and a match is not descended into.
fn replace_relations(
    plan: &Arc<dyn ExecutionPlan>,
    rewritten: &[RewrittenRelation],
) -> Result<Option<Arc<dyn ExecutionPlan>>> {
    if rewritten.is_empty() {
        return Ok(None);
    }
    if let Some((_, new)) = rewritten
        .iter()
        .find(|(original, _)| Arc::ptr_eq(original, plan))
    {
        return Ok(Some(Arc::clone(new)));
    }
    let mut changed = false;
    let children = plan
        .children()
        .into_iter()
        .map(|child| match replace_relations(child, rewritten)? {
            Some(new_child) => {
                changed = true;
                Ok(new_child)
            }
            None => Ok(Arc::clone(child)),
        })
        .collect::<Result<Vec<_>>>()?;
    if changed {
        Ok(Some(replace_children_if_necessary(
            Arc::clone(plan),
            children,
        )?))
    } else {
        Ok(None)
    }
}

pub(crate) fn enumerate_join_order(
    plan: &Arc<dyn ExecutionPlan>,
    config: &ConfigOptions,
    stats: &mut StatsFn,
) -> Result<Option<Arc<dyn ExecutionPlan>>> {
    if let Some(graph) = extract(plan, stats)? {
        if let Some(reordered) = reorder(&graph, config, stats)? {
            return Ok(Some(reordered));
        }
        // Rejected, so descend into the relations, not the children: re-extracting here
        // would re-search costed subsets, and would weigh a gain against a subtree's own
        // cost rather than the whole graph's, readmitting what the margin rejected.
        let mut rewritten: Vec<RewrittenRelation> = vec![];
        for relation in &graph.relations {
            if let Some(new) = enumerate_join_order(&relation.plan, config, stats)? {
                rewritten.push((Arc::clone(&relation.plan), new));
            }
        }
        return replace_relations(plan, &rewritten);
    }

    let mut changed = false;
    let children = plan
        .children()
        .into_iter()
        .map(|child| match enumerate_join_order(child, config, stats)? {
            Some(new_child) => {
                changed = true;
                Ok(new_child)
            }
            None => Ok(Arc::clone(child)),
        })
        .collect::<Result<Vec<_>>>()?;
    if changed {
        Ok(Some(replace_children_if_necessary(
            Arc::clone(plan),
            children,
        )?))
    } else {
        Ok(None)
    }
}

fn reorder(
    graph: &JoinGraph,
    config: &ConfigOptions,
    stats: &mut StatsFn,
) -> Result<Option<Arc<dyn ExecutionPlan>>> {
    let model = CostModel::new(graph, config);
    let limit = config.optimizer.join_enumeration_limit.min(MAX_RELATIONS);
    if graph.relations.len() > limit {
        return Ok(None);
    }
    let Some(solution) = solve_dp(&model) else {
        return Ok(None);
    };

    // Keep the planner's order unless the winner is clearly cheaper. Where estimates
    // cannot tell orders apart every candidate looks alike and the winner is
    // arbitrary, which cost TPC-DS q6 37% on an estimated gain under 1%.
    let margin = f64::from(config.optimizer.join_enumeration_min_improvement) / 100.0;
    if solution.cost >= model.tree_cost(&graph.original_nodes) * (1.0 - margin) {
        return Ok(None);
    }

    let mut relations = Vec::with_capacity(graph.relations.len());
    for relation in &graph.relations {
        relations.push(
            enumerate_join_order(&relation.plan, config, stats)?
                .unwrap_or_else(|| Arc::clone(&relation.plan)),
        );
    }

    let rebuilder = Rebuilder {
        graph,
        model: &model,
        solution: &solution,
        relations: &relations,
    };
    let (plan, columns) = rebuilder.node(graph.all(), &graph.output)?;
    if columns == graph.output {
        return Ok(Some(plan));
    }
    // Only a sort merge subtree reaches here, having no projection of its own.
    let schema = plan.schema();
    let exprs = graph
        .output
        .iter()
        .map(|col| {
            let Some(index) = position(&columns, *col) else {
                return internal_err!("join enumeration lost column {col:?}");
            };
            let name = schema.field(index).name();
            Ok(ProjectionExpr {
                expr: Arc::new(Column::new(name, index)),
                alias: name.clone(),
            })
        })
        .collect::<Result<Vec<_>>>()?;
    Ok(Some(Arc::new(ProjectionExec::try_new(exprs, plan)?)))
}
