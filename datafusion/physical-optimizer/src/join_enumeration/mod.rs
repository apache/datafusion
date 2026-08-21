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
//! A subtree of joins is flattened into relations plus the predicates between them, a
//! dynamic program searches the orders (bushy as well as left-deep) under a `C_out` cost
//! model, and the subtree is rebuilt if the winner is clearly cheaper.
//!
//! The estimates come from a [`JoinCostModel`], so a different one can be plugged in
//! with [`JoinEnumeration::with_cost_model`].
//!
//! The graph itself, and how a subtree of joins is flattened into one, is in [`graph`].
//!
//! Reordering is sound because a tree of inner joins equals the cross product of its
//! relations filtered by all its predicates. Semi and anti joins take part as reducers:
//! they filter their output side rather than contributing columns.

pub mod graph;

use std::collections::HashMap;
use std::sync::Arc;

use crate::PhysicalOptimizerRule;
use crate::optimizer::{ConfigOnlyContext, PhysicalOptimizerContext};

use arrow::compute::SortOptions;
use arrow::datatypes::{FieldRef, Schema};
use datafusion_common::config::ConfigOptions;
use datafusion_common::error::Result;
use datafusion_common::tree_node::{Transformed, TransformedResult, TreeNode};
use datafusion_common::{JoinSide, JoinType, Statistics, internal_err};
use datafusion_expr_common::operator::Operator;
use datafusion_physical_expr::PhysicalExprRef;
use datafusion_physical_expr::expressions::{BinaryExpr, Column};
use datafusion_physical_expr::projection::ProjectionExpr;
use datafusion_physical_plan::ExecutionPlan;
use datafusion_physical_plan::execution_plan::replace_children_if_necessary;
use datafusion_physical_plan::joins::utils::{ColumnIndex, JoinFilter};
use datafusion_physical_plan::joins::{
    CrossJoinExec, HashJoinExecBuilder, NestedLoopJoinExec, PartitionMode,
    SortMergeJoinExec,
};
use datafusion_physical_plan::operator_statistics::StatisticsRegistry;
use datafusion_physical_plan::projection::ProjectionExec;
use datafusion_physical_plan::statistics::{StatisticsArgs, StatisticsContext};

use graph::{
    ColRef, Edge, Filter, JoinGraph, JoinKind, MAX_RELATIONS, RelSet, StatsFn, bit,
    covers, extract, iter_rels,
};

/// Chooses the shape of the join tree, before [`JoinSelection`] decides how each
/// join runs.
///
/// [`JoinSelection`]: crate::join_selection::JoinSelection
#[derive(Debug)]
pub struct JoinEnumeration {
    cost_model: Arc<dyn JoinCostModelFactory>,
}

impl JoinEnumeration {
    #[expect(missing_docs)]
    pub fn new() -> Self {
        Self {
            cost_model: Arc::new(DefaultJoinCostModelFactory {}),
        }
    }

    /// Searches with `cost_model` in place of [`DefaultJoinCostModel`].
    pub fn with_cost_model(mut self, cost_model: Arc<dyn JoinCostModelFactory>) -> Self {
        self.cost_model = cost_model;
        self
    }
}

impl Default for JoinEnumeration {
    fn default() -> Self {
        Self::new()
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
        Ok(
            enumerate_join_order(&plan, config, &mut stats, self.cost_model.as_ref())?
                .unwrap_or(plan),
        )
    }

    fn name(&self) -> &str {
        "join_enumeration"
    }

    fn schema_check(&self) -> bool {
        true
    }
}

/// A hash partitioning, as the set of key classes it is partitioned on. Zero means
/// not hash partitioned, which is where every scan starts.
pub type PartSet = u32;

/// A valid way of combining two relation sets.
#[derive(Clone, Copy, Debug)]
pub enum Combine {
    /// An inner join, both sides contributing their columns.
    Inner,
    /// A semi or anti join applying `reducer` to the opposite set.
    Reducer {
        /// The reducing relation, which is the whole of its side.
        reducer: usize,
    },
}

/// One way of exchanging a join's inputs: what the exchange costs, the partitioning it
/// leaves the output in, the side that builds, and the mode the join runs in.
#[derive(Clone, Copy, Debug)]
pub struct Exchange {
    /// Cost of moving the inputs, in the unit [`JoinCostModel::cardinality`] counts in.
    pub cost: f64,
    /// The partitioning the join's output is left in, for a join above to reuse.
    pub partitioning: PartSet,
    /// The side that builds: what `CollectLeft` collects, or which side is hashed.
    pub build: RelSet,
    /// The mode the join is costed under, which is the one the rebuild emits.
    pub mode: PartitionMode,
}

/// Cardinality and cost estimates over the subsets of a [`JoinGraph`], which is all the
/// search knows about the data.
///
/// [`DefaultJoinCostModel`] derives them from the statistics the subtree's inputs report.
/// Implement this trait, and hand it to [`JoinEnumeration::with_cost_model`], to search
/// under other estimates: statistics kept outside the plan, a different cost function, or
/// knowledge of the data the plan has no way to carry.
pub trait JoinCostModel {
    /// Estimated rows from joining every relation in `mask`.
    ///
    /// Must depend on the set alone and not on the order its relations were joined in:
    /// that is what makes the dynamic program valid. Should stay at or above `1.0`, since
    /// the search multiplies cardinalities.
    fn cardinality(&self, mask: RelSet) -> f64;

    /// Whether `left` and `right` can be combined, and how: as an inner join, or with one
    /// side applying as a reducer. `None` prunes the pair from the search, which is what
    /// keeps a cut no predicate spans, or a reducer whose keys the other side does not
    /// supply, out of the plan.
    fn combine(&self, left: RelSet, right: RelSet) -> Option<Combine>;

    /// Cost of joining `left` (partitioned as `left_part`) with `right` (`right_part`),
    /// one entry per way of exchanging the inputs. Returning several lets the search carry
    /// each partitioning forward and keep whichever pays off at the joins above; an empty
    /// result prunes the pair.
    ///
    /// `collect_only` is the side that must build, when the combination forces one.
    fn exchanges(
        &self,
        left: RelSet,
        right: RelSet,
        left_part: PartSet,
        right_part: PartSet,
        collect_only: Option<RelSet>,
    ) -> Vec<Exchange>;

    /// The side that must build, when one of them is a reducer: the reducer is hashed so
    /// the side it filters can stream.
    fn reducer_side(&self, left: RelSet, right: RelSet) -> Option<RelSet> {
        match self.combine(left, right) {
            Some(Combine::Reducer { reducer }) => Some(bit(reducer)),
            _ => None,
        }
    }

    /// Cost of a whole tree, given its internal nodes as `(node, one child)`, children
    /// first. The search scores its own candidates this way, so the shape the planner
    /// produced can be compared against them.
    fn tree_cost(&self, nodes: &[(RelSet, RelSet)]) -> f64 {
        let mut parts: HashMap<RelSet, PartSet> = HashMap::new();
        let mut total = 0.0;
        for (mask, child) in nodes {
            if mask.count_ones() < 2 {
                continue;
            }
            let other = mask ^ child;
            let (child_part, other_part) = (
                parts.get(child).copied().unwrap_or(0),
                parts.get(&other).copied().unwrap_or(0),
            );
            let collect_only = self.reducer_side(*child, other);
            let best = self
                .exchanges(*child, other, child_part, other_part, collect_only)
                .into_iter()
                .min_by(|a, b| a.cost.total_cmp(&b.cost));
            let (exchange, part) =
                best.map_or((0.0, 0), |best| (best.cost, best.partitioning));
            total += self.cardinality(*mask) + exchange;
            parts.insert(*mask, part);
        }
        total
    }
}

/// Builds the [`JoinCostModel`] for one join subtree.
pub trait JoinCostModelFactory: std::fmt::Debug + Send + Sync {
    /// Creates a cost model over `graph`.
    fn create<'graph>(
        &self,
        graph: &'graph JoinGraph,
        config: &ConfigOptions,
    ) -> Result<Box<dyn JoinCostModel + 'graph>>;
}

/// Hands out [`DefaultJoinCostModel`], which the rule searches with unless it was given
/// another factory.
#[derive(Debug, Default)]
pub struct DefaultJoinCostModelFactory {}

impl JoinCostModelFactory for DefaultJoinCostModelFactory {
    fn create<'graph>(
        &self,
        graph: &'graph JoinGraph,
        config: &ConfigOptions,
    ) -> Result<Box<dyn JoinCostModel + 'graph>> {
        Ok(Box::new(DefaultJoinCostModel::new(graph, config)))
    }
}

/// The built-in [`JoinCostModel`]: a `C_out` model over the row counts, distinct counts
/// and widths the subtree's inputs report.
pub struct DefaultJoinCostModel<'a> {
    graph: &'a JoinGraph,
    /// Aggregated selectivity per connected relation pair, as
    /// `(rel_a, rel_b, selectivity)` with `rel_a < rel_b`.
    pair_selectivity: Vec<(usize, usize, f64)>,
    /// Neighbours of each relation, over equi-join keys and non-equi filters alike.
    /// Reducers neighbour nothing: they are applied.
    adjacency: Vec<RelSet>,
    /// The relations of each connected component of `adjacency`.
    components: Vec<RelSet>,
    /// Fraction of its filtered side each reducer keeps; `1.0` for non-reducers.
    reducer_selectivity: Vec<f64>,
    /// Selectivity of each non-equi filter, with the relations it needs.
    filter_selectivity: Vec<(RelSet, f64)>,
    /// The key class of each edge, as a bit position. Joins on the same class can
    /// reuse each other's hash partitioning.
    edge_class: Vec<u32>,
    /// The size a build side must stay under for `JoinSelection` to broadcast it.
    broadcast_bytes: f64,
    /// The row count it must stay under when no byte estimate is available.
    broadcast_rows: f64,
}

impl<'a> DefaultJoinCostModel<'a> {
    /// Precomputes the selectivities and connectivity of `graph`.
    pub fn new(graph: &'a JoinGraph, config: &ConfigOptions) -> Self {
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

        // A filter links what it references, so a pair joined only by one counts as
        // connected and its cuts prune like any other.
        for filter in &graph.filters {
            for rel in iter_rels(filter.required) {
                adjacency[rel] |= filter.required & !bit(rel);
            }
        }

        let mut components: Vec<RelSet> = vec![];
        let mut seen: RelSet = 0;
        for rel in 0..graph.relations.len() {
            if seen & bit(rel) != 0 {
                continue;
            }
            let mut component = bit(rel);
            loop {
                let grown = iter_rels(component)
                    .fold(component, |mask, rel| mask | adjacency[rel]);
                if grown == component {
                    break;
                }
                component = grown;
            }
            seen |= component;
            components.push(component);
        }

        Self {
            graph,
            pair_selectivity,
            adjacency,
            components,
            edge_class: key_classes(&graph.edges),
            broadcast_bytes: config.optimizer.hash_join_single_partition_threshold as f64,
            broadcast_rows: config.optimizer.hash_join_single_partition_threshold_rows
                as f64,
            reducer_selectivity,
            filter_selectivity,
        }
    }

    fn connected(&self, left: RelSet, right: RelSet) -> bool {
        iter_rels(left).any(|rel| self.adjacency[rel] & right != 0)
    }

    /// The key classes joining `left` to `right`, which a partitioned join hashes both
    /// sides on.
    fn crossing_classes(&self, left: RelSet, right: RelSet) -> PartSet {
        let mut classes = 0;
        for (index, edge) in self.graph.edges.iter().enumerate() {
            let (a, b) = (bit(edge.left.rel), bit(edge.right.rel));
            if (left & a != 0 && right & b != 0) || (left & b != 0 && right & a != 0) {
                classes |= 1 << self.edge_class[index];
            }
        }
        classes
    }

    /// Whether `JoinSelection` will broadcast this side rather than partition it, by the
    /// same bytes-or-rows test it uses. A sort merge join has no mode that collects.
    fn broadcasts(&self, side: RelSet) -> bool {
        if self.graph.kind() == JoinKind::SortMerge {
            return false;
        }
        let mut width = 0.0;
        for rel in iter_rels(side) {
            if self.graph.reducer(rel).is_some() {
                continue;
            }
            match self.graph.relations[rel].width {
                Some(bytes) => width += bytes,
                None => return self.cardinality(side) < self.broadcast_rows,
            }
        }
        self.cardinality(side) * width < self.broadcast_bytes
    }
}

impl JoinCostModel for DefaultJoinCostModel<'_> {
    /// Rows are the product of the relations' row counts, cut by the selectivity of
    /// every predicate the set closes over.
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
        // Otherwise both sides must contribute columns. An unconnected cut is a cross
        // product, allowed only between whole components, which is the only way to join
        // a disconnected graph.
        if left & !reducers == 0 || right & !reducers == 0 {
            return None;
        }
        let separates_components = self
            .components
            .iter()
            .all(|component| component & left == 0 || component & right == 0);
        (self.connected(left, right) || separates_components).then_some(Combine::Inner)
    }

    /// A side already hashed on the join key is not moved again, so the cost of an
    /// exchange depends on what the joins below left behind.
    fn exchanges(
        &self,
        left: RelSet,
        right: RelSet,
        left_part: PartSet,
        right_part: PartSet,
        collect_only: Option<RelSet>,
    ) -> Vec<Exchange> {
        let classes = self.crossing_classes(left, right);
        // Without a key every pair is examined, which the output cardinality does not
        // show: a filter estimated to keep few rows still compares all of them.
        let pairs = if classes == 0 {
            self.cardinality(left) * self.cardinality(right)
        } else {
            0.0
        };
        let mut options = vec![];
        for (build, probe_part) in [(left, right_part), (right, left_part)] {
            if collect_only.is_some_and(|only| only != build) {
                continue;
            }
            // With no key there is nothing to hash on, so a side must be collected.
            if classes == 0 || self.broadcasts(build) {
                options.push(Exchange {
                    cost: pairs + self.cardinality(build),
                    partitioning: probe_part,
                    build,
                    mode: PartitionMode::CollectLeft,
                });
            }
        }
        if classes != 0 {
            let mut moved = 0.0;
            if left_part != classes {
                moved += self.cardinality(left);
            }
            if right_part != classes {
                moved += self.cardinality(right);
            }
            let build = collect_only.unwrap_or_else(|| {
                if self.cardinality(left) <= self.cardinality(right) {
                    left
                } else {
                    right
                }
            });
            options.push(Exchange {
                cost: moved,
                partitioning: classes,
                build,
                mode: PartitionMode::Partitioned,
            });
        }
        options
    }
}

/// Groups equi-join predicates that share a column into key classes, so two joins hashing
/// on the same class can reuse one partitioning. Returns each edge's class as a bit
/// position, with classes past the bitmask's width collapsed into the last.
fn key_classes(edges: &[Edge]) -> Vec<u32> {
    let mut ids: Vec<usize> = (0..edges.len()).collect();
    let shares = |a: &Edge, b: &Edge| {
        [a.left, a.right]
            .iter()
            .any(|col| *col == b.left || *col == b.right)
    };
    loop {
        let mut merged = false;
        for i in 0..edges.len() {
            for j in (i + 1)..edges.len() {
                if ids[i] != ids[j] && shares(&edges[i], &edges[j]) {
                    let (keep, drop) = (ids[i].min(ids[j]), ids[i].max(ids[j]));
                    ids.iter_mut()
                        .filter(|id| **id == drop)
                        .for_each(|id| *id = keep);
                    merged = true;
                }
            }
        }
        if !merged {
            break;
        }
    }
    let mut compact = ids.clone();
    compact.sort_unstable();
    compact.dedup();
    ids.iter()
        .map(|id| {
            let position = compact.iter().position(|c| c == id).unwrap_or(0);
            position.min(PartSet::BITS as usize - 1) as u32
        })
        .collect()
}

/// The winning join tree: per internal node, which input goes on the left and how the
/// join exchanges its data.
struct Solution {
    nodes: HashMap<RelSet, (RelSet, PartitionMode)>,
    cost: f64,
}

/// Exhaustive dynamic programming over connected relation subsets, each paired with the
/// partitioning its plan leaves behind. Carrying the partitioning lets a later join reuse
/// an earlier one's exchange instead of paying for another.
fn solve_dp(graph: &JoinGraph, model: &dyn JoinCostModel) -> Option<Solution> {
    let n = graph.relations.len();
    let full: RelSet = graph.all();

    // Per subset, the cheapest plan for each partitioning it can be left in, and the
    // choice that got there. A scan arrives hash partitioned on nothing.
    let mut best: Vec<HashMap<PartSet, f64>> = vec![HashMap::new(); 1usize << n];
    let mut choice: Vec<HashMap<PartSet, (RelSet, RelSet, PartitionMode)>> =
        vec![HashMap::new(); 1usize << n];
    for rel in 0..n {
        best[bit(rel) as usize].insert(0, 0.0);
    }

    for mask in 1..=full {
        if mask.count_ones() < 2 {
            continue;
        }
        let cardinality = model.cardinality(mask);
        // Subsets containing the lowest set bit, so each pair of halves is seen once.
        let lowest = mask & mask.wrapping_neg();
        let mut left = mask;
        while left != 0 {
            left = (left - 1) & mask;
            if left & lowest == 0 {
                continue;
            }
            let right = mask ^ left;
            if right == 0 || model.combine(left, right).is_none() {
                continue;
            }
            let collect_only = model.reducer_side(left, right);
            for (left_part, left_cost) in best[left as usize].clone() {
                for (right_part, right_cost) in best[right as usize].clone() {
                    let below = left_cost + right_cost + cardinality;
                    for exchange in
                        model.exchanges(left, right, left_part, right_part, collect_only)
                    {
                        let candidate = below + exchange.cost;
                        let entry = best[mask as usize]
                            .entry(exchange.partitioning)
                            .or_insert(f64::MAX);
                        if candidate < *entry {
                            *entry = candidate;
                            choice[mask as usize].insert(
                                exchange.partitioning,
                                (left, exchange.build, exchange.mode),
                            );
                        }
                    }
                }
            }
        }
    }

    let (&winning_part, &cost) = best[full as usize]
        .iter()
        .min_by(|a, b| a.1.total_cmp(b.1))?;

    // Walk the winning tree, keeping the left input and mode of each node it uses.
    let mut nodes = HashMap::new();
    let mut stack = vec![(full, winning_part)];
    while let Some((mask, part)) = stack.pop() {
        if mask.count_ones() < 2 {
            continue;
        }
        let Some(&(split, build, mode)) = choice[mask as usize].get(&part) else {
            continue;
        };
        let other = mask ^ split;
        // The build side goes on the left: that is the side `CollectLeft` gathers.
        nodes.insert(mask, (build, mode));
        for child in [split, other] {
            let child_part = best[child as usize]
                .iter()
                .min_by(|a, b| a.1.total_cmp(b.1))
                .map(|(part, _)| *part)
                .unwrap_or(0);
            stack.push((child, child_part));
        }
    }

    Some(Solution { nodes, cost })
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

struct Rebuilder<'a> {
    graph: &'a JoinGraph,
    model: &'a dyn JoinCostModel,
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

        let Some(&(left, mode)) = self.solution.nodes.get(&mask) else {
            return internal_err!("join enumeration produced no split for {mask:b}");
        };
        let right = mask ^ left;
        match self.model.combine(left, right) {
            None => internal_err!("join enumeration produced an invalid join"),
            Some(Combine::Reducer { reducer }) => {
                self.reducing(mask, required, reducer, mode)
            }
            // The search chose the build side and the mode along with the order, so both
            // are emitted as decided rather than left to `JoinSelection`.
            Some(Combine::Inner) => self.inner(required, left, right, mode),
        }
    }

    /// Builds one join of the kind the subtree used.
    fn build_join(
        &self,
        left: Built,
        right: Built,
        spec: JoinSpec,
        required: &[ColRef],
    ) -> Result<(Arc<dyn ExecutionPlan>, Vec<ColRef>)> {
        let JoinSpec {
            on,
            join_type,
            filter,
            mode,
        } = spec;
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
        if keys == 0 {
            // No keys: with a filter this is a nested loop join, without one a cross
            // join.
            let projection = projection_for(required, &natural)?;
            return match filter {
                Some(filter) => {
                    let join = NestedLoopJoinExec::try_new(
                        left.plan,
                        right.plan,
                        Some(filter),
                        &join_type,
                        projection.clone(),
                    )?;
                    // `projection_for` is `None` only for the identity.
                    let emitted = match projection {
                        Some(_) => required.to_vec(),
                        None => natural,
                    };
                    Ok((Arc::new(join), emitted))
                }
                // A cross join has no projection of its own.
                None => {
                    Ok((Arc::new(CrossJoinExec::new(left.plan, right.plan)), natural))
                }
            };
        }
        match self.graph.kind() {
            JoinKind::Hash => {
                let join = HashJoinExecBuilder::new(left.plan, right.plan, on, join_type)
                    .with_filter(filter)
                    .with_null_equality(self.graph.null_equality())
                    .with_partition_mode(mode)
                    .with_projection(projection_for(required, &natural)?)
                    .build()?;
                Ok((Arc::new(join), required.to_vec()))
            }
            JoinKind::SortMerge => {
                let join: Arc<dyn ExecutionPlan> = Arc::new(SortMergeJoinExec::try_new(
                    left.plan,
                    right.plan,
                    on,
                    filter,
                    join_type,
                    vec![SortOptions::default(); keys],
                    self.graph.null_equality(),
                )?);
                // A sort merge join emits every column, so drop the ones nothing above
                // needs here rather than once above the whole subtree: what this join
                // emits is what the next one sorts. `ProjectionPushdown` cannot do it
                // afterwards, since it only pushes through a join whose columns stay
                // left-then-right, and reordering interleaves them. Reordering alone is
                // left to the parent, which addresses columns by position anyway.
                if required.len() == natural.len() {
                    return Ok((join, natural));
                }
                let Some(projection) = projection_for(required, &natural)? else {
                    return Ok((join, natural));
                };
                Ok((narrow(join, &projection)?, required.to_vec()))
            }
        }
    }

    fn inner(
        &self,
        required: &[ColRef],
        left_mask: RelSet,
        right_mask: RelSet,
        mode: PartitionMode,
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

        // Each side emits this join's keys, its filters' columns, and what is needed
        // above.
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
            JoinSpec {
                on,
                join_type: JoinType::Inner,
                filter,
                mode,
            },
            required,
        )
    }

    fn reducing(
        &self,
        mask: RelSet,
        required: &[ColRef],
        reducer: usize,
        mode: PartitionMode,
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
            JoinSpec {
                on,
                join_type,
                filter: None,
                mode,
            },
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

/// Wraps `plan` in a projection keeping only `indices`, in that order.
fn narrow(
    plan: Arc<dyn ExecutionPlan>,
    indices: &[usize],
) -> Result<Arc<dyn ExecutionPlan>> {
    let schema = plan.schema();
    let exprs: Vec<ProjectionExpr> = indices
        .iter()
        .map(|&index| {
            let name = schema.field(index).name();
            ProjectionExpr {
                expr: Arc::new(Column::new(name, index)),
                alias: name.clone(),
            }
        })
        .collect();
    Ok(Arc::new(ProjectionExec::try_new(exprs, plan)?))
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

/// One join as the search decided it, including the mode it was costed with.
struct JoinSpec {
    on: Vec<(PhysicalExprRef, PhysicalExprRef)>,
    join_type: JoinType,
    filter: Option<JoinFilter>,
    mode: PartitionMode,
}

/// Rebuilds the non-equi filters applied at one join as one conjunction. A [`JoinFilter`]
/// addresses its batch by index, so merging shifts all but the first filter's indices.
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

    // Intermediate columns go left side first: a sort merge join rebuilds the batch as
    // all left then all right columns, so interleaved sides would read wrong columns.
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

/// Substitutes rewritten relations into a subtree, leaving its shape untouched. The
/// relations are the exact `Arc`s taken from it, so pointer identity finds them.
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
    cost_model: &dyn JoinCostModelFactory,
) -> Result<Option<Arc<dyn ExecutionPlan>>> {
    if let Some(graph) = extract(plan, stats)? {
        if let Some(reordered) = reorder(&graph, config, stats, cost_model)? {
            return Ok(Some(reordered));
        }
        // Rejected, so descend into the relations rather than the children: re-extracting
        // here would re-search costed subsets and readmit what the margin turned down.
        let mut rewritten: Vec<RewrittenRelation> = vec![];
        for relation in &graph.relations {
            if let Some(new) =
                enumerate_join_order(&relation.plan, config, stats, cost_model)?
            {
                rewritten.push((Arc::clone(&relation.plan), new));
            }
        }
        return replace_relations(plan, &rewritten);
    }

    let mut changed = false;
    let children = plan
        .children()
        .into_iter()
        .map(
            |child| match enumerate_join_order(child, config, stats, cost_model)? {
                Some(new_child) => {
                    changed = true;
                    Ok(new_child)
                }
                None => Ok(Arc::clone(child)),
            },
        )
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
    cost_model: &dyn JoinCostModelFactory,
) -> Result<Option<Arc<dyn ExecutionPlan>>> {
    let limit = config.optimizer.join_enumeration_limit.min(MAX_RELATIONS);
    if graph.relations.len() > limit {
        return Ok(None);
    }
    let model = cost_model.create(graph, config)?;
    let Some(solution) = solve_dp(graph, model.as_ref()) else {
        return Ok(None);
    };

    // Keep the planner's order unless the winner is clearly cheaper: where estimates
    // cannot tell orders apart, the winner is arbitrary.
    let margin = f64::from(config.optimizer.join_enumeration_min_improvement) / 100.0;
    if solution.cost >= model.tree_cost(&graph.original_nodes) * (1.0 - margin) {
        return Ok(None);
    }

    let mut relations = Vec::with_capacity(graph.relations.len());
    for relation in &graph.relations {
        relations.push(
            enumerate_join_order(&relation.plan, config, stats, cost_model)?
                .unwrap_or_else(|| Arc::clone(&relation.plan)),
        );
    }

    let rebuilder = Rebuilder {
        graph,
        model: model.as_ref(),
        solution: &solution,
        relations: &relations,
    };
    let (plan, columns) = rebuilder.node(graph.all(), &graph.output)?;
    if columns == graph.output {
        return Ok(Some(plan));
    }
    // Only a sort merge subtree reaches here, when its root emits more than the output.
    let Some(projection) = projection_for(&graph.output, &columns)? else {
        return Ok(Some(plan));
    };
    Ok(Some(narrow(plan, &projection)?))
}
