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

//! Cost-based join order enumeration for [`JoinSelection`].
//!
//! [`JoinSelection`] on its own only makes *local* decisions: for a single join
//! it picks the build side and the partition mode. The shape of the join tree is
//! whatever the logical planner produced, which for a query written as a flat
//! list of relations is a left-deep tree in `FROM`-clause order. That order is
//! frequently far from the cheapest one, because it ignores how much each join
//! reduces or inflates its inputs.
//!
//! This module adds the missing global step. It
//!
//! 1. **extracts** a maximal connected subtree of reorderable joins into a
//!    [`JoinGraph`] of opaque *relations* (the subtree's leaves) plus the
//!    predicates between them,
//! 2. **enumerates** join trees over that graph with a dynamic programming
//!    search ([`solve_dp`]) that considers bushy shapes as well as left-deep
//!    ones, scoring each candidate with the cardinality model in [`CostModel`],
//!    and falling back to a greedy search ([`solve_greedy`]) for graphs too
//!    large to enumerate exhaustively, and
//! 3. **rebuilds** the subtree from the winning plan ([`build_tree`]),
//!    re-deriving every join key and filter against the new schemas and
//!    inserting join projections so intermediate results stay as narrow as they
//!    were before.
//!
//! The rewrite only replaces the original subtree when the winning plan is
//! strictly cheaper than the shape the planner produced, so plans that are
//! already optimal are left untouched.
//!
//! # Why reordering is sound, and what a relation set means
//!
//! A tree of inner joins is equivalent to the cross product of its relations
//! filtered by the conjunction of all its predicates. So *any* tree that applies
//! every predicate exactly once, at a node where the columns that predicate needs
//! are available, computes the same rows. Three kinds of predicate take part:
//!
//! * **Equi-join edges** ([`Edge`]) connect two relations. Each is applied at the
//!   one node whose two inputs separate its endpoints.
//! * **Non-equi join filters** ([`Filter`]) may reference any number of
//!   relations. Each is applied at its lowest common ancestor: the deepest node
//!   whose inputs together, but neither alone, cover everything it references.
//! * **Semi and anti joins** ([`Reducer`]) are *filters on their output side*:
//!   they keep or drop rows of it and contribute no columns of their own. Their
//!   quantified side therefore becomes a relation that may be applied at any node
//!   covering the columns its keys reference, which is what lets a selective
//!   `EXISTS` run before the joins it used to sit above.
//!
//! [`JoinSelection`]: crate::join_selection::JoinSelection

use std::collections::HashMap;
use std::sync::Arc;

use arrow::datatypes::{FieldRef, Schema};
use datafusion_common::config::ConfigOptions;
use datafusion_common::error::Result;
use datafusion_common::tree_node::{Transformed, TransformedResult, TreeNode};
use datafusion_common::{JoinSide, JoinType, NullEquality, Statistics, internal_err};
use datafusion_expr_common::operator::Operator;
use datafusion_physical_expr::PhysicalExprRef;
use datafusion_physical_expr::expressions::{BinaryExpr, Column};
use datafusion_physical_plan::execution_plan::replace_children_if_necessary;
use datafusion_physical_plan::joins::utils::{
    ColumnIndex, JoinFilter, max_distinct_count,
};
use datafusion_physical_plan::joins::{HashJoinExec, HashJoinExecBuilder, PartitionMode};
use datafusion_physical_plan::projection::{ProjectionExec, all_alias_free_columns};
use datafusion_physical_plan::{ExecutionPlan, ExecutionPlanProperties};

/// Hard upper bound on the number of relations in one join graph.
///
/// Relation sets are bitmasks in a `u64` and the greedy search is cubic in the
/// number of relations, so very large join graphs are left alone.
const MAX_RELATIONS: usize = 32;

/// Hard upper bound on the relations handed to the exhaustive search, whatever
/// `join_enumeration_limit` says.
///
/// [`solve_dp`] allocates `2^n` entries and visits `3^n` splits, so an unclamped
/// configuration value would ask for absurd amounts of memory and time. Graphs
/// above this bound use [`solve_greedy`] instead.
const MAX_DP_RELATIONS: usize = 16;

/// Computes the statistics of a plan node.
///
/// `JoinSelection` supplies this so the enumerator sees the same estimates as
/// the rest of the rule, including the pluggable [`StatisticsRegistry`] when that
/// is enabled.
///
/// [`StatisticsRegistry`]: datafusion_physical_plan::operator_statistics::StatisticsRegistry
pub(crate) type StatsFn<'a> =
    dyn FnMut(&dyn ExecutionPlan) -> Result<Arc<Statistics>> + 'a;

/// A bitmask over relation indices.
type RelSet = u64;

fn bit(rel: usize) -> RelSet {
    1u64 << rel
}

/// Iterates the relation indices contained in `mask`.
fn iter_rels(mask: RelSet) -> impl Iterator<Item = usize> {
    std::iter::successors(Some(mask), |m| Some(m & m.wrapping_sub(1)))
        .take_while(|m| *m != 0)
        .map(|m| m.trailing_zeros() as usize)
}

/// Whether `mask` contains every relation in `required`.
fn covers(mask: RelSet, required: RelSet) -> bool {
    required & !mask == 0
}

/// Reference to one column of one relation of a [`JoinGraph`].
///
/// Column plumbing is done in terms of these rather than column indices, since
/// reordering the tree changes the index any given column sits at.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
struct ColRef {
    /// Index into [`JoinGraph::relations`].
    rel: usize,
    /// Column index within that relation's output schema.
    col: usize,
}

/// What a relation contributes to the join.
#[derive(Debug)]
enum Role {
    /// An ordinary input: its rows and columns flow into the output.
    Output,
    /// The quantified side of a semi or anti join: it filters other relations and
    /// contributes no columns.
    Reducer(Reducer),
}

/// The quantified side of a semi or anti join.
#[derive(Debug)]
struct Reducer {
    /// `true` for an anti join, which keeps the rows that do *not* match.
    anti: bool,
    /// Join keys, as `(column of the filtered side, column index in this
    /// relation)`.
    keys: Vec<(ColRef, usize)>,
    /// The relations the keys reference. This reducer can only be applied to a
    /// set of relations covering all of them.
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

/// A non-equi join predicate, carried along unchanged apart from having its
/// column references rewritten for wherever it ends up.
#[derive(Debug)]
struct Filter {
    filter: JoinFilter,
    /// The column each entry of the filter's intermediate schema comes from.
    columns: Vec<ColRef>,
    /// The relations those columns belong to.
    required: RelSet,
}

/// A connected set of joins, flattened into relations plus the predicates
/// between them.
#[derive(Debug)]
struct JoinGraph {
    relations: Vec<Relation>,
    edges: Vec<Edge>,
    filters: Vec<Filter>,
    /// The columns the original subtree emitted, in order. The rebuilt subtree
    /// reproduces exactly this list, so the plan above it stays valid.
    output: Vec<ColRef>,
    /// Null handling shared by every join in the subtree, taken from the first
    /// join seen. A join that handles nulls differently becomes a relation
    /// instead of part of the graph.
    null_equality: Option<NullEquality>,
    /// Relation sets of the original tree's internal nodes, used to score the
    /// shape the planner produced against the enumerated alternatives.
    original_nodes: Vec<RelSet>,
    /// The relations that are reducers rather than ordinary inputs.
    reducers: RelSet,
}

impl JoinGraph {
    /// Distinct value estimate for a column.
    fn ndv(&self, col: ColRef) -> f64 {
        self.relations[col.rel].ndv[col.col]
    }

    /// The set of all relations in the graph.
    fn all(&self) -> RelSet {
        (0..self.relations.len()).fold(0, |mask, rel| mask | bit(rel))
    }

    fn reducer(&self, rel: usize) -> Option<&Reducer> {
        match &self.relations[rel].role {
            Role::Reducer(reducer) => Some(reducer),
            Role::Output => None,
        }
    }

    fn null_equality(&self) -> NullEquality {
        self.null_equality
            .unwrap_or(NullEquality::NullEqualsNothing)
    }
}

/// A valid way of combining two relation sets into one node.
#[derive(Clone, Copy, Debug)]
enum Combine {
    /// An inner join of two sets that a predicate connects.
    Inner,
    /// A semi or anti join applying the reducer relation `reducer` to the
    /// opposite set.
    Reducer { reducer: usize },
}

/// Cardinality and cost estimates over the subsets of a [`JoinGraph`].
struct CostModel<'a> {
    graph: &'a JoinGraph,
    /// Aggregated selectivity per connected relation pair, as
    /// `(rel_a, rel_b, selectivity)` with `rel_a < rel_b`.
    pair_selectivity: Vec<(usize, usize, f64)>,
    /// Neighbours of each relation, as a bitmask. Reducers neighbour nothing:
    /// they are applied, not joined.
    adjacency: Vec<RelSet>,
    /// The fraction of its filtered side's rows each reducer keeps, indexed by
    /// relation. `1.0` for relations that are not reducers.
    reducer_selectivity: Vec<f64>,
    /// Selectivity of each non-equi filter, with the relations it needs.
    filter_selectivity: Vec<(RelSet, f64)>,
}

impl<'a> CostModel<'a> {
    fn new(graph: &'a JoinGraph, config: &ConfigOptions) -> Self {
        // Aggregate the predicates of a relation pair the way
        // `estimate_inner_join_cardinality` does: a multi-key join is estimated
        // from its single most selective key rather than by multiplying the keys
        // together. Keeping the two models consistent matters, because the
        // statistics the rest of `JoinSelection` reads off the rebuilt plan come
        // from that function.
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
        pair_selectivity.sort_unstable_by(|l, r| (l.0, l.1).cmp(&(r.0, r.1)));

        let reducer_selectivity = (0..graph.relations.len())
            .map(|rel| match graph.reducer(rel) {
                None => 1.0,
                Some(reducer) => {
                    // The fraction of the filtered side's key values that the
                    // reducer covers, from its most selective key.
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

        // A non-equi filter has no statistics to go on, so it gets the same
        // default the rest of the optimizer uses for an opaque predicate.
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

    /// Estimated number of rows produced by joining every relation in `mask`.
    ///
    /// This is the textbook estimate: the product of the relation sizes scaled
    /// down by the selectivity of every predicate that applies within `mask`. It
    /// depends only on the *set* of relations and not on the shape of the tree
    /// that joins them, which is what makes the dynamic program below valid.
    fn cardinality(&self, mask: RelSet) -> f64 {
        let mut rows = 1.0;
        for rel in iter_rels(mask) {
            // A reducer contributes a selectivity rather than rows of its own.
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

    /// Whether at least one equi-join predicate connects `left` and `right`.
    fn connected(&self, left: RelSet, right: RelSet) -> bool {
        iter_rels(left).any(|rel| self.adjacency[rel] & right != 0)
    }

    /// How `left` and `right` may be combined, if at all.
    fn combine(&self, left: RelSet, right: RelSet) -> Option<Combine> {
        let reducers = self.graph.reducers;
        // A side that is a lone reducer is applied to the other side, which must
        // supply every column the reducer's keys reference.
        for (reducer_side, filtered) in [(right, left), (left, right)] {
            if reducer_side.count_ones() == 1 && reducer_side & reducers != 0 {
                let reducer = reducer_side.trailing_zeros() as usize;
                let required = self.graph.reducer(reducer)?.required;
                return covers(filtered, required)
                    .then_some(Combine::Reducer { reducer });
            }
        }
        // Otherwise this is an inner join, so both sides must contribute columns
        // and a predicate must connect them. Cross products are never introduced:
        // a plan needing one is left in the planner's own order.
        if left & !reducers == 0 || right & !reducers == 0 {
            return None;
        }
        self.connected(left, right).then_some(Combine::Inner)
    }

    /// Cost of a join tree: the sum of the cardinalities of its internal nodes
    /// (`C_out`). Leaves are excluded because every candidate tree reads the same
    /// relations, making their cost a constant.
    fn tree_cost(&self, nodes: &[RelSet]) -> f64 {
        nodes
            .iter()
            .filter(|mask| mask.count_ones() > 1)
            .map(|mask| self.cardinality(*mask))
            .sum()
    }
}

/// The winning join tree: for every internal node, the relation set of one of
/// its two inputs. The other input is the rest of that node's relation set.
struct Solution {
    splits: HashMap<RelSet, RelSet>,
    cost: f64,
}

/// Exhaustive dynamic programming over connected relation subsets.
///
/// Every subset is costed once by trying all the ways of splitting it into two
/// halves, so bushy shapes are considered alongside left-deep ones. The search is
/// `O(3^n)` in the number of relations, which is why the caller bounds `n`.
fn solve_dp(model: &CostModel) -> Option<Solution> {
    let n = model.graph.relations.len();
    let full: RelSet = model.graph.all();
    let size = 1usize << n;

    // `f64::INFINITY` marks a subset that cannot be built at all, either because
    // it is disconnected or because it holds a reducer whose columns it does not
    // cover.
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
        // Enumerate the subsets of `mask` containing its lowest set bit, so each
        // unordered pair of halves is visited exactly once.
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

    // Walk the winning tree, keeping only the splits it actually uses.
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

/// Greedy fallback for join graphs too large for [`solve_dp`].
///
/// Repeatedly combines the pair of subtrees whose result is smallest. Cubic in
/// the number of relations, and it can lose to the planner's original order --
/// which the caller checks for.
fn solve_greedy(model: &CostModel) -> Option<Solution> {
    let n = model.graph.relations.len();
    // (relation set, accumulated cost of the subtree built for it)
    let mut components: Vec<(RelSet, f64)> = (0..n).map(|rel| (bit(rel), 0.0)).collect();
    let mut splits = HashMap::new();

    while components.len() > 1 {
        let mut best: Option<(usize, usize, f64, f64)> = None;
        for i in 0..components.len() {
            for j in (i + 1)..components.len() {
                let (left, left_cost) = components[i];
                let (right, right_cost) = components[j];
                if model.combine(left, right).is_none() {
                    continue;
                }
                let cardinality = model.cardinality(left | right);
                let cost = left_cost + right_cost + cardinality;
                if best.is_none_or(|(_, _, best_cardinality, _)| {
                    cardinality < best_cardinality
                }) {
                    best = Some((i, j, cardinality, cost));
                }
            }
        }
        // Nothing left to combine without a cross product; leave the graph alone.
        let (i, j, _, cost) = best?;
        let (left, _) = components[i];
        let (right, _) = components[j];
        splits.insert(left | right, left);
        components[i] = (left | right, cost);
        components.swap_remove(j);
    }

    let (_, cost) = components[0];
    Some(Solution { splits, cost })
}

/// How a join takes part in enumeration, if at all.
#[derive(Clone, Copy, Debug)]
enum JoinRole {
    /// An inner join: both inputs are part of the graph.
    Inner,
    /// A semi or anti join: `output` names the side whose rows survive, and the
    /// other side becomes a [`Reducer`].
    Reducing { anti: bool, output: JoinSide },
}

/// Classifies a join for enumeration.
///
/// Outer and mark joins are excluded: an outer join is not a filter on its inputs
/// and so cannot be moved past one, and a mark join adds a column that the column
/// plumbing here does not model. `null_aware` anti joins are excluded because they
/// carry `NOT IN` semantics that depend on the whole probe side, and joins with a
/// limit because the limit belongs to one particular tree shape. A semi or anti
/// join with a non-equi filter is excluded too: that filter is part of an
/// existential test, not a conjunct that can move on its own.
fn join_role(join: &HashJoinExec) -> Option<JoinRole> {
    if join.null_aware || join.fetch().is_some() || join.on().is_empty() {
        return None;
    }
    let role = match join.join_type() {
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
    if join.filter().is_some() && !matches!(role, JoinRole::Inner) {
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

/// Appends `col` unless it is already there.
fn push_unique(columns: &mut Vec<ColRef>, col: ColRef) {
    if position(columns, col).is_none() {
        columns.push(col);
    }
}

/// Appends the columns of `wanted` that belong to `side`, keeping their order and
/// dropping duplicates.
fn extend_required(columns: &mut Vec<ColRef>, wanted: &[ColRef], side: RelSet) {
    for col in wanted.iter().filter(|col| side & bit(col.rel) != 0) {
        push_unique(columns, *col);
    }
}

/// Extracts the maximal reorderable join subtree rooted at `plan`.
///
/// Returns `None` when `plan` does not root a subtree worth enumerating, which
/// covers every bail-out condition: a join feature the enumerator does not model,
/// a join key that is not a plain column, missing row count statistics, or too
/// few or too many relations.
fn extract(
    plan: &Arc<dyn ExecutionPlan>,
    stats: &mut StatsFn,
) -> Result<Option<JoinGraph>> {
    // Start either at a join, or at the column pruning projection that usually
    // sits directly above one. Rooting the graph at the projection lets its
    // column list become the top join's own projection, instead of leaving a
    // `ProjectionExec` stranded above a join emitting more columns than the query
    // needs. Anything else is not a subtree root, and bailing out here keeps
    // whole plans from being walked for nothing.
    let is_root = match plan.downcast_ref::<HashJoinExec>() {
        Some(join) => join_role(join).is_some(),
        None => plan
            .downcast_ref::<ProjectionExec>()
            .is_some_and(|projection| all_alias_free_columns(projection.expr())),
    };
    if !is_root {
        return Ok(None);
    }
    Extractor::new(stats).extract(plan)
}

/// Flattens a join subtree into a [`JoinGraph`].
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
            // A single join has nothing to reorder: `JoinSelection`'s build side
            // swap already covers it.
            return Ok(None);
        }
        // A filter referencing a single relation has no join to be applied at,
        // since the deepest node covering it would be a leaf.
        if graph
            .filters
            .iter()
            .any(|filter| filter.required.count_ones() < 2)
        {
            return Ok(None);
        }
        Ok(Some(graph))
    }

    /// Recursively flattens `plan`, returning the columns it emits and the set of
    /// relations it covers.
    ///
    /// `None` means the subtree cannot be reordered and the caller must give up.
    fn visit(
        &mut self,
        plan: &Arc<dyn ExecutionPlan>,
    ) -> Result<Option<(Vec<ColRef>, RelSet)>> {
        if let Some(join) = plan.downcast_ref::<HashJoinExec>()
            && let Some(role) = join_role(join)
            && self
                .graph
                .null_equality
                .is_none_or(|null_equality| null_equality == join.null_equality)
        {
            self.graph.null_equality = Some(join.null_equality);
            let visited = match role {
                JoinRole::Inner => self.visit_inner(join)?,
                JoinRole::Reducing { anti, output } => {
                    self.visit_reducing(join, anti, output)?
                }
            };
            let Some((columns, mask)) = visited else {
                return Ok(None);
            };

            self.graph.original_nodes.push(mask);
            // The join's projection selects from the columns it emits, which for a
            // semi or anti join are only those of its output side.
            let columns = match &join.projection {
                Some(projection) => projection.iter().map(|idx| columns[*idx]).collect(),
                None => columns,
            };
            Ok(Some((columns, mask)))
        } else if let Some(projection) = plan.downcast_ref::<ProjectionExec>()
            && all_alias_free_columns(projection.expr())
        {
            // A pure column pruning projection. Looking through these is what lets
            // the enumerator see a whole join chain: at this point in the
            // optimizer the planner has left one between every pair of joins, and
            // `ProjectionPushdown`, which folds them into the joins, has not run
            // yet. `all_alias_free_columns` also rules out renaming, so dropping
            // the projection cannot change the subtree's output field names.
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
            // A leaf: opaque to the enumerator, but it needs its statistics.
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

    /// Flattens an inner join: both sides join the graph, and its predicates
    /// become edges and filters.
    fn visit_inner(
        &mut self,
        join: &HashJoinExec,
    ) -> Result<Option<(Vec<ColRef>, RelSet)>> {
        let Some((left, left_mask)) = self.visit(join.left())? else {
            return Ok(None);
        };
        let Some((right, right_mask)) = self.visit(join.right())? else {
            return Ok(None);
        };

        for (left_key, right_key) in join.on() {
            let (Some(left_key), Some(right_key)) =
                (as_column(left_key), as_column(right_key))
            else {
                // A key such as `cast(a) = b` would have to be re-derived against
                // a different schema; not worth the complexity.
                return Ok(None);
            };
            let edge = Edge {
                left: left[left_key],
                right: right[right_key],
            };
            // Duplicate predicates would be double counted by the cost model.
            if !self
                .graph
                .edges
                .iter()
                .any(|e| (e.left, e.right) == (edge.left, edge.right))
            {
                self.graph.edges.push(edge);
            }
        }

        if let Some(filter) = join.filter() {
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

    /// Flattens a semi or anti join: its output side joins the graph, and its
    /// quantified side becomes a reducer relation.
    fn visit_reducing(
        &mut self,
        join: &HashJoinExec,
        anti: bool,
        output: JoinSide,
    ) -> Result<Option<(Vec<ColRef>, RelSet)>> {
        let (output_plan, reducer_plan) = match output {
            JoinSide::Left => (join.left(), join.right()),
            JoinSide::Right => (join.right(), join.left()),
            JoinSide::None => return internal_err!("semi join with no output side"),
        };

        let Some((columns, mask)) = self.visit(output_plan)? else {
            return Ok(None);
        };

        // The keys are resolved against the output side's columns, so they have to
        // be collected before the reducer relation that owns them exists.
        let mut keys = Vec::with_capacity(join.on().len());
        let mut required = 0;
        for (left_key, right_key) in join.on() {
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

    /// Adds `plan` to the graph as a relation, returning its index.
    fn push_relation(
        &mut self,
        plan: &Arc<dyn ExecutionPlan>,
        role: Role,
    ) -> Result<Option<usize>> {
        if plan.boundedness().is_unbounded() {
            // Reordering could break the pipeline properties the other
            // `JoinSelection` subrules establish.
            return Ok(None);
        }
        if self.graph.relations.len() >= MAX_RELATIONS {
            return Ok(None);
        }
        let statistics = (self.stats)(plan.as_ref())?;
        let Some(rows) = statistics.num_rows.get_value().copied() else {
            // Without a row count there is no basis for reordering anything.
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

/// Rebuilds a join subtree from the tree a search picked.
struct Rebuilder<'a> {
    graph: &'a JoinGraph,
    model: &'a CostModel<'a>,
    solution: &'a Solution,
    /// The plan of each relation, already rewritten if it held a join subtree of
    /// its own.
    relations: &'a [Arc<dyn ExecutionPlan>],
}

impl Rebuilder<'_> {
    /// Builds the node joining every relation in `mask`, emitting `required` in
    /// that order.
    fn node(
        &self,
        mask: RelSet,
        required: &[ColRef],
    ) -> Result<(Arc<dyn ExecutionPlan>, Vec<ColRef>)> {
        if mask.count_ones() == 1 {
            // Relations are opaque, so they are emitted as they are. Narrowing
            // them is `ProjectionPushdown`'s job and it runs later.
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
                // Put the cheaper side on the left. `JoinSelection`'s build side
                // swap runs after this and may revise the choice from the rebuilt
                // plan's statistics, but starting from the smaller side keeps the
                // two decisions consistent.
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

    /// Builds an inner join of `left_mask` and `right_mask`.
    fn inner(
        &self,
        required: &[ColRef],
        left_mask: RelSet,
        right_mask: RelSet,
    ) -> Result<(Arc<dyn ExecutionPlan>, Vec<ColRef>)> {
        // Every equi-join predicate crossing this cut is applied here, and only
        // here: each edge crosses exactly one cut of the tree, at the lowest node
        // whose relation set contains both of its endpoints.
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

        // Non-equi filters are applied at their lowest common ancestor: this node
        // covers everything the filter references, and neither input does alone.
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

        // Each side must emit this join's key columns, the columns its filters
        // reference, and whatever the nodes above asked for.
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

        // The join's natural output, before its projection.
        let mut joined = left_columns;
        joined.extend(right_columns);
        let projection = projection_for(required, &joined)?;

        let join = HashJoinExecBuilder::new(left_plan, right_plan, on, JoinType::Inner)
            .with_filter(filter)
            .with_null_equality(self.graph.null_equality())
            // The build side and the partition mode are picked by
            // `statistical_join_selection_subrule`, which runs after enumeration.
            .with_partition_mode(PartitionMode::Auto)
            .with_projection(projection)
            .build()?;
        Ok((Arc::new(join), required.to_vec()))
    }

    /// Builds the semi or anti join applying `reducer` to the rest of `mask`.
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

        // The filtered side must emit the columns the keys compare, plus whatever
        // the nodes above asked for. Non-equi filters never land here: they only
        // reference output relations, so the deepest node covering one is always
        // inside the filtered side.
        let mut filtered_required: Vec<ColRef> = vec![];
        for (column, _) in &info.keys {
            push_unique(&mut filtered_required, *column);
        }
        extend_required(&mut filtered_required, required, filtered_mask);

        let (filtered_plan, filtered_columns) =
            self.node(filtered_mask, &filtered_required)?;
        let reducer_plan = Arc::clone(&self.relations[reducer]);

        // The reducer goes on the build side so the filtered side can be
        // streamed: `RightSemi` and `RightAnti` emit rows of their right input,
        // which is exactly the filtered side.
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

        // A semi or anti join emits only its output side, so the projection
        // selects from the filtered side's columns alone.
        let projection = projection_for(required, &filtered_columns)?;
        let join_type = if info.anti {
            JoinType::RightAnti
        } else {
            JoinType::RightSemi
        };

        let join = HashJoinExecBuilder::new(reducer_plan, filtered_plan, on, join_type)
            .with_null_equality(self.graph.null_equality())
            .with_partition_mode(PartitionMode::Auto)
            .with_projection(projection)
            .build()?;
        Ok((Arc::new(join), required.to_vec()))
    }
}

/// Builds a `Column` expression for `col`, given the columns a plan emits.
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

/// The projection selecting `required` out of `emitted`, or `None` when the node
/// already emits exactly that.
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

/// Rebuilds the non-equi filters applied at one join as a single conjunction over
/// the columns its inputs now emit.
///
/// A [`JoinFilter`] evaluates its expression against an intermediate batch whose
/// columns are described by `column_indices`, and the expression addresses that
/// batch by index. So combining filters means concatenating their intermediate
/// schemas and shifting the column indices of all but the first, while the mapping
/// from intermediate column to input column is rebuilt from scratch.
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

    Ok(Some(JoinFilter::new(
        expression,
        column_indices,
        Arc::new(Schema::new(fields)),
    )))
}

/// Shifts every column index in `expression` by `offset`, for a filter whose
/// intermediate columns have been appended after another filter's.
fn shift_columns(expression: PhysicalExprRef, offset: usize) -> Result<PhysicalExprRef> {
    if offset == 0 {
        return Ok(expression);
    }
    expression
        .transform(|expr| {
            Ok(match expr.downcast_ref::<Column>() {
                Some(column) => Transformed::yes(Arc::new(Column::new(
                    column.name(),
                    column.index() + offset,
                )) as _),
                None => Transformed::no(expr),
            })
        })
        .data()
}

/// Enumerates join orders throughout `plan`, returning `None` if nothing changed.
pub(crate) fn enumerate_join_order(
    plan: &Arc<dyn ExecutionPlan>,
    config: &ConfigOptions,
    stats: &mut StatsFn,
) -> Result<Option<Arc<dyn ExecutionPlan>>> {
    if let Some(graph) = extract(plan, stats)?
        && let Some(reordered) = reorder(&graph, config, stats)?
    {
        return Ok(Some(reordered));
    }

    // Not a subtree that could be reordered: recurse into the children.
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

/// Enumerates orders for one extracted graph, rebuilding it if a cheaper order
/// exists.
fn reorder(
    graph: &JoinGraph,
    config: &ConfigOptions,
    stats: &mut StatsFn,
) -> Result<Option<Arc<dyn ExecutionPlan>>> {
    let model = CostModel::new(graph, config);
    let limit = config
        .optimizer
        .join_enumeration_limit
        .min(MAX_DP_RELATIONS);
    let solution = if graph.relations.len() <= limit {
        solve_dp(&model)
    } else {
        solve_greedy(&model)
    };
    let Some(solution) = solution else {
        return Ok(None);
    };

    // Keep the planner's order unless the winner is strictly cheaper. The dynamic
    // program considers the original shape as well, so this only rejects ties
    // there -- but the greedy search can genuinely lose, and either way it keeps
    // already-optimal plans byte identical.
    if solution.cost >= model.tree_cost(&graph.original_nodes) {
        return Ok(None);
    }

    // Reorder inside the relations before assembling them.
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
    let (plan, _) = rebuilder.node(graph.all(), &graph.output)?;
    Ok(Some(plan))
}
