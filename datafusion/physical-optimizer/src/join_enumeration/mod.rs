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
//! model, and the subtree is rebuilt if the winner is cheaper.
//!
//! The estimates come from a [`JoinCostModel`], so a different one can be plugged in
//! with [`JoinEnumeration::with_cost_model`].
//!
//! The graph itself, and how a subtree of joins is flattened into one, is in [`graph`].
//!
//! Reordering is sound because a tree of inner joins equals the cross product of its
//! relations filtered by all its predicates. Semi, anti, outer and mark joins take part
//! as relations applied to the set their keys come from: a semi or anti join filters that
//! side, an outer join extends it, a mark join marks it.

mod dphyp;
pub mod graph;

use std::cell::RefCell;
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
    AppliedKey, AppliedKind, ColRef, Edge, Filter, JoinGraph, JoinKind, Key,
    MAX_RELATIONS, RelSet, StatsFn, bit, covers, extract, iter_rels,
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
    /// A semi, anti or outer join applying `applied` to the opposite set.
    Applied {
        /// The applied relation, which is the whole of its side.
        applied: usize,
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
    /// side applied to the other. `None` prunes the pair from the search, which is what
    /// keeps a cut no predicate spans, or an applied side whose keys the other does not
    /// supply, out of the plan.
    fn combine(&self, left: RelSet, right: RelSet) -> Option<Combine>;

    /// Ways of exchanging the inputs of a join of `left` (partitioned as `left_part`)
    /// with `right` (`right_part`), appended to `into`. Appending several lets the search
    /// carry each partitioning forward and keep whichever pays off at the joins above;
    /// appending none prunes the pair. `into` arrives empty and is reused between calls,
    /// which is why this appends rather than returns.
    ///
    /// `collect_only` is the side that must build, when the combination forces one.
    fn exchanges(
        &self,
        left: RelSet,
        right: RelSet,
        left_part: PartSet,
        right_part: PartSet,
        collect_only: Option<RelSet>,
        into: &mut Vec<Exchange>,
    );

    /// The key classes a join above `mask` could still hash on. A plan left partitioned on
    /// anything else is not worth keeping apart from the cheapest one, since nothing above
    /// can reuse it, and the search then keeps one plan for `mask` instead of several.
    /// Every class, by default.
    fn reusable(&self, _mask: RelSet) -> PartSet {
        PartSet::MAX
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
            let collect_only = collect_only(self.combine(*child, other));
            let mut exchanges = vec![];
            self.exchanges(
                *child,
                other,
                child_part,
                other_part,
                collect_only,
                &mut exchanges,
            );
            let best = exchanges
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

/// The side a combination forces to build: an applied relation is hashed so the side it
/// applies to can stream.
fn collect_only(combine: Option<Combine>) -> Option<RelSet> {
    match combine {
        Some(Combine::Applied { applied }) => Some(bit(applied)),
        _ => None,
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
    /// What each joined relation pair divides the row count by, largest first.
    denominators: Vec<Denominator>,
    /// The relations each equality holds between. A pair may only be counted as long as
    /// its equality has pairs left to spend.
    equalities: Vec<RelSet>,
    /// Neighbours of each relation, over equi-join keys and non-equi filters alike.
    /// An applied relation neighbours nothing: it is applied rather than joined.
    adjacency: Vec<RelSet>,
    /// The relations of each connected component of `adjacency`.
    components: Vec<RelSet>,
    /// What applying a relation does to the row count of the side it applies to: the
    /// fraction a semi or anti join keeps, or the rows an outer join leaves per row.
    /// `1.0` for a relation that is not applied.
    applied_factor: Vec<f64>,
    /// Selectivity of each non-equi filter, with the relations it needs.
    filter_selectivity: Vec<(RelSet, f64)>,
    /// The key class of each edge, as a bit position. Joins on the same class can
    /// reuse each other's hash partitioning.
    edge_class: Vec<u32>,
    /// The relations each key class joins, by that bit position.
    class_relations: Vec<RelSet>,
    /// The size a build side must stay under for `JoinSelection` to broadcast it.
    broadcast_bytes: f64,
    /// The row count it must stay under when no byte estimate is available.
    broadcast_rows: f64,
    /// Row counts already estimated, since `exchanges` asks for its inputs' repeatedly.
    /// `NaN` where not yet estimated.
    estimated: RefCell<Vec<f64>>,
}

impl<'a> DefaultJoinCostModel<'a> {
    /// Precomputes the selectivities and connectivity of `graph`.
    pub fn new(graph: &'a JoinGraph, config: &ConfigOptions) -> Self {
        let mut adjacency = vec![0; graph.relations.len()];
        for edge in &graph.edges {
            let (a, b) = (edge.left.column.rel, edge.right.column.rel);
            adjacency[a] |= bit(b);
            adjacency[b] |= bit(a);
        }
        let (denominators, equalities) = denominators(graph);

        let edge_class: Vec<u32> = key_classes(&graph.edges)
            .iter()
            .map(|class| (*class).min(PartSet::BITS as usize - 1) as u32)
            .collect();
        let mut class_relations = vec![0 as RelSet; PartSet::BITS as usize];
        for (edge, class) in graph.edges.iter().zip(&edge_class) {
            class_relations[*class as usize] |=
                bit(edge.left.column.rel) | bit(edge.right.column.rel);
        }

        let applied_factor = (0..graph.relations.len())
            .map(|rel| {
                let Some(applied) = graph.applied(rel) else {
                    return 1.0;
                };
                let mine = |key: &AppliedKey| {
                    graph.ndv(ColRef {
                        rel,
                        col: key.column,
                    })
                };
                match applied.kind {
                    // A mark join adds a column, not rows.
                    AppliedKind::Mark => 1.0,
                    // The fraction of the other side's key values this one covers.
                    AppliedKind::Semi | AppliedKind::Anti => {
                        let matched = applied
                            .keys
                            .iter()
                            .map(|key| {
                                (mine(key) / graph.ndv(key.other.column)).clamp(0.0, 1.0)
                            })
                            .fold(1.0f64, f64::min);
                        if applied.kind == AppliedKind::Anti {
                            1.0 - matched
                        } else {
                            matched
                        }
                    }
                    // Rows per row of the side it extends, as an inner join would count
                    // them, but never below the one an unmatched row keeps.
                    AppliedKind::Outer => {
                        let denominator = iter_rels(applied.required)
                            .map(|other| {
                                applied
                                    .keys
                                    .iter()
                                    .filter(|key| key.other.column.rel == other)
                                    .map(|key| mine(key).max(graph.ndv(key.other.column)))
                                    .fold(1.0f64, f64::max)
                            })
                            .product::<f64>();
                        (graph.relations[rel].rows / denominator.max(1.0)).max(1.0)
                    }
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
            denominators,
            equalities,
            adjacency,
            components,
            edge_class,
            class_relations,
            broadcast_bytes: config.optimizer.hash_join_single_partition_threshold as f64,
            broadcast_rows: config.optimizer.hash_join_single_partition_threshold_rows
                as f64,
            estimated: RefCell::new(vec![f64::NAN; 1usize << graph.relations.len()]),
            applied_factor,
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
            let (a, b) = (bit(edge.left.column.rel), bit(edge.right.column.rel));
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
            // Only an outer join leaves the rows it is applied to carrying this side's
            // columns, so only its width counts.
            if self
                .graph
                .applied(rel)
                .is_some_and(|applied| !applied.kind.widens())
            {
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
        let known = self.estimated.borrow()[mask as usize];
        if !known.is_nan() {
            return known;
        }
        let rows = self.estimate(mask);
        self.estimated.borrow_mut()[mask as usize] = rows;
        rows
    }
    fn combine(&self, left: RelSet, right: RelSet) -> Option<Combine> {
        let applied_relations = self.graph.applied;
        // A lone applied relation is applied to the other side, which must supply its
        // keys.
        for (side, other) in [(right, left), (left, right)] {
            if side.is_power_of_two() && side & applied_relations != 0 {
                let applied = side.trailing_zeros() as usize;
                let required = self.graph.applied(applied)?.required;
                return covers(other, required).then_some(Combine::Applied { applied });
            }
        }
        // Otherwise both sides must be joinable on their own. An unconnected cut is a
        // cross product, allowed only between whole components, which is the only way to
        // join a disconnected graph.
        if left & !applied_relations == 0 || right & !applied_relations == 0 {
            return None;
        }
        let separates_components = self
            .components
            .iter()
            .all(|component| component & left == 0 || component & right == 0);
        (self.connected(left, right) || separates_components).then_some(Combine::Inner)
    }

    /// A class whose relations are all joined already cannot be hashed on again.
    fn reusable(&self, mask: RelSet) -> PartSet {
        let mut classes = 0;
        for (class, relations) in self.class_relations.iter().enumerate() {
            if relations & !mask != 0 {
                classes |= 1 << class;
            }
        }
        classes
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
        into: &mut Vec<Exchange>,
    ) {
        let classes = self.crossing_classes(left, right);
        // Without a key every pair is examined, which the output cardinality does not
        // show: a filter estimated to keep few rows still compares all of them.
        let pairs = if classes == 0 {
            self.cardinality(left) * self.cardinality(right)
        } else {
            0.0
        };
        for (build, probe_part) in [(left, right_part), (right, left_part)] {
            if collect_only.is_some_and(|only| only != build) {
                continue;
            }
            // With no key there is nothing to hash on, so a side must be collected.
            if classes == 0 || self.broadcasts(build) {
                into.push(Exchange {
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
            into.push(Exchange {
                cost: moved,
                partitioning: classes,
                build,
                mode: PartitionMode::Partitioned,
            });
        }
    }
}

impl DefaultJoinCostModel<'_> {
    /// Rows from joining every relation in `mask`, before memoizing.
    fn estimate(&self, mask: RelSet) -> f64 {
        let mut rows = 1.0;
        for rel in iter_rels(mask) {
            // An applied relation contributes a factor, not its own rows.
            rows *= match self.graph.applied(rel) {
                Some(_) => self.applied_factor[rel],
                None => self.graph.relations[rel].rows,
            };
        }
        // An equality between `k` relations is one predicate however many pairs write it,
        // so it may divide at most `k - 1` times: the largest denominators, since a
        // predicate is at its most selective where it joins the most distinct values.
        let mut budget: Vec<u32> = self
            .equalities
            .iter()
            .map(|relations| (relations & mask).count_ones().saturating_sub(1))
            .collect();
        for denominator in &self.denominators {
            if covers(mask, denominator.pair) && budget[denominator.equality] > 0 {
                budget[denominator.equality] -= 1;
                rows /= denominator.denominator;
            }
        }
        for (required, selectivity) in &self.filter_selectivity {
            if covers(mask, *required) {
                rows *= selectivity;
            }
        }
        rows.max(1.0)
    }
}

/// What one joined relation pair divides the row count by.
struct Denominator {
    /// The two relations, as a mask.
    pair: RelSet,
    /// Their most selective key's distinct count.
    denominator: f64,
    /// Index into the equalities, of the one that key belongs to.
    equality: usize,
}

/// What each joined relation pair divides by, largest first, and the relations each
/// equality holds between. Predicates sharing a column state one equality however many
/// pairs write it, which is what keeps `k` relations joined on one key from dividing
/// `k * (k - 1) / 2` times instead of `k - 1`.
fn denominators(graph: &JoinGraph) -> (Vec<Denominator>, Vec<RelSet>) {
    let classes = key_classes(&graph.edges);
    let mut equalities = vec![0; classes.iter().max().map_or(0, |last| last + 1)];
    // Denominate each pair by its most selective key, as `estimate_inner_join_cardinality`
    // does, so the two models agree on a pair.
    let mut pairs: HashMap<RelSet, (f64, usize)> = HashMap::new();
    for (edge, class) in graph.edges.iter().zip(&classes) {
        let (a, b) = (edge.left.column.rel, edge.right.column.rel);
        equalities[*class] |= bit(a) | bit(b);
        let ndv = graph
            .ndv(edge.left.column)
            .max(graph.ndv(edge.right.column))
            .max(1.0);
        pairs
            .entry(bit(a) | bit(b))
            .and_modify(|current| {
                if ndv > current.0 {
                    *current = (ndv, *class);
                }
            })
            .or_insert((ndv, *class));
    }

    let mut denominators: Vec<Denominator> = pairs
        .into_iter()
        .map(|(pair, (denominator, equality))| Denominator {
            pair,
            denominator,
            equality,
        })
        .collect();
    // `HashMap` iteration order is not deterministic, but plans must be.
    denominators.sort_unstable_by(|a, b| {
        b.denominator
            .total_cmp(&a.denominator)
            .then(a.pair.cmp(&b.pair))
    });
    (denominators, equalities)
}

/// Groups equi-join predicates that share a column, so two joins on the same group state
/// one equality and can reuse one hash partitioning.
fn key_classes(edges: &[Edge]) -> Vec<usize> {
    let mut ids: Vec<usize> = (0..edges.len()).collect();
    let shares = |a: &Edge, b: &Edge| {
        [a.left.column, a.right.column]
            .iter()
            .any(|col| *col == b.left.column || *col == b.right.column)
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
        .map(|id| compact.iter().position(|c| c == id).unwrap_or(0))
        .collect()
}

/// The winning join tree: per internal node, which input goes on the left and how the
/// join exchanges its data.
struct Solution {
    nodes: HashMap<RelSet, (RelSet, PartitionMode)>,
    cost: f64,
}

/// One plan for one relation subset: what it costs, the partitioning it leaves behind,
/// and the join that got there.
#[derive(Clone, Copy)]
struct Plan {
    partitioning: PartSet,
    cost: f64,
    /// The set that goes on the left, and the one that builds.
    left: RelSet,
    build: RelSet,
    mode: PartitionMode,
}

/// The dynamic program's table: per relation subset, the cheapest plan for each
/// partitioning it can be left in. Carrying the partitioning lets a later join reuse an
/// earlier one's exchange instead of paying for another. A subset is left in only a few
/// partitionings, so the plans are scanned rather than hashed.
struct DpTable<'a> {
    model: &'a dyn JoinCostModel,
    plans: Vec<Vec<Plan>>,
    /// Cardinalities already asked for, since many pairs share one union.
    cardinality: Vec<f64>,
    /// The same for the partitionings still worth telling apart, `None` until asked.
    reusable: Vec<Option<PartSet>>,
    /// Reused by every call to [`JoinCostModel::exchanges`].
    exchanges: Vec<Exchange>,
}

impl<'a> DpTable<'a> {
    fn new(model: &'a dyn JoinCostModel, relations: usize) -> Self {
        let subsets = 1usize << relations;
        let mut plans = vec![vec![]; subsets];
        // A scan arrives hash partitioned on nothing.
        for rel in 0..relations {
            plans[bit(rel) as usize].push(Plan {
                partitioning: 0,
                cost: 0.0,
                left: 0,
                build: 0,
                mode: PartitionMode::Auto,
            });
        }
        Self {
            model,
            plans,
            cardinality: vec![f64::NAN; subsets],
            reusable: vec![None; subsets],
            exchanges: vec![],
        }
    }

    /// Whether any plan is known for `mask`.
    fn planned(&self, mask: RelSet) -> bool {
        !self.plans[mask as usize].is_empty()
    }

    fn cardinality(&mut self, mask: RelSet) -> f64 {
        if self.cardinality[mask as usize].is_nan() {
            self.cardinality[mask as usize] = self.model.cardinality(mask);
        }
        self.cardinality[mask as usize]
    }

    fn reusable(&mut self, mask: RelSet) -> PartSet {
        *self.reusable[mask as usize].get_or_insert_with(|| self.model.reusable(mask))
    }

    /// The cheapest plan for `mask`, and among equally cheap ones the same plan every
    /// time, so the tree does not depend on the order the search reached it in.
    fn cheapest(&self, mask: RelSet) -> Option<&Plan> {
        self.plans[mask as usize].iter().min_by(|a, b| {
            a.cost
                .total_cmp(&b.cost)
                .then(a.partitioning.cmp(&b.partitioning))
        })
    }

    /// Costs joining `left` with `right`, keeping the result if it beats their union's.
    fn join(&mut self, left: RelSet, right: RelSet) {
        let combine = self.model.combine(left, right);
        if combine.is_none() {
            return;
        }
        let collect_only = collect_only(combine);
        let mask = left | right;
        let cardinality = self.cardinality(mask);
        let reusable = self.reusable(mask);
        // Taken out so the halves can be read while the union is filled.
        let mut plans = std::mem::take(&mut self.plans[mask as usize]);
        let mut exchanges = std::mem::take(&mut self.exchanges);
        for left_plan in &self.plans[left as usize] {
            for right_plan in &self.plans[right as usize] {
                let below = left_plan.cost + right_plan.cost + cardinality;
                exchanges.clear();
                self.model.exchanges(
                    left,
                    right,
                    left_plan.partitioning,
                    right_plan.partitioning,
                    collect_only,
                    &mut exchanges,
                );
                for exchange in &exchanges {
                    let candidate = Plan {
                        // A partitioning nothing above can match is worth no more than
                        // none at all, and plans then merge instead of multiplying.
                        partitioning: if exchange.partitioning & !reusable == 0 {
                            exchange.partitioning
                        } else {
                            0
                        },
                        cost: below + exchange.cost,
                        left,
                        build: exchange.build,
                        mode: exchange.mode,
                    };
                    match plans
                        .iter_mut()
                        .find(|plan| plan.partitioning == candidate.partitioning)
                    {
                        // Orders often tie; breaking the tie on the sets rather than on
                        // which was reached first keeps the plan independent of that.
                        Some(plan) => {
                            if candidate.cost < plan.cost
                                || (candidate.cost == plan.cost && left > plan.left)
                            {
                                *plan = candidate;
                            }
                        }
                        None => plans.push(candidate),
                    }
                }
            }
        }
        self.exchanges = exchanges;
        self.plans[mask as usize] = plans;
    }

    /// The cheapest tree over every relation, if the search reached one.
    fn solution(&self, full: RelSet) -> Option<Solution> {
        let winner = self.cheapest(full)?;

        // Walk the winning tree, keeping the left input and mode of each node it uses.
        let mut nodes = HashMap::new();
        let mut stack = vec![(full, winner.partitioning)];
        while let Some((mask, partitioning)) = stack.pop() {
            if mask.count_ones() < 2 {
                continue;
            }
            let Some(plan) = self.plans[mask as usize]
                .iter()
                .find(|plan| plan.partitioning == partitioning)
            else {
                continue;
            };
            // The build side goes on the left: that is the side `CollectLeft` gathers.
            nodes.insert(mask, (plan.build, plan.mode));
            for child in [plan.left, mask ^ plan.left] {
                let child_part = self.cheapest(child).map_or(0, |plan| plan.partitioning);
                stack.push((child, child_part));
            }
        }

        Some(Solution {
            nodes,
            cost: winner.cost,
        })
    }
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
            Some(Combine::Applied { applied }) => {
                self.applying(mask, required, applied, mode)
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
        // A semi or anti join emits one side, and the applied side's own columns are
        // not emitted, so its `Built` carries none.
        let natural = match join_type {
            JoinType::RightSemi | JoinType::RightAnti => right.columns.clone(),
            // A mark join emits the side it marked and then the flag, which is the one
            // column the marking side contributes.
            JoinType::RightMark => {
                right.columns.iter().chain(&left.columns).copied().collect()
            }
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
        let mut keys: Vec<(&Key, &Key)> = vec![];
        for edge in &self.graph.edges {
            let (left, right) = (&edge.left, &edge.right);
            if left_mask & bit(left.column.rel) != 0
                && right_mask & bit(right.column.rel) != 0
            {
                keys.push((left, right));
            } else if left_mask & bit(right.column.rel) != 0
                && right_mask & bit(left.column.rel) != 0
            {
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
                push_unique(&mut columns, if take_left { left } else { right }.column);
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
                    key_expr(&left_columns, &left_plan, left)?,
                    key_expr(&right_columns, &right_plan, right)?,
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

    fn applying(
        &self,
        mask: RelSet,
        required: &[ColRef],
        applied: usize,
        mode: PartitionMode,
    ) -> Result<(Arc<dyn ExecutionPlan>, Vec<ColRef>)> {
        let Some(info) = self.graph.applied(applied) else {
            return internal_err!("relation {applied} is not applied to another");
        };
        let other_mask = mask & !bit(applied);

        // Filters never land here: they reference relations this one is applied to, so
        // their lowest covering node is always inside that side.
        let mut other_required: Vec<ColRef> = vec![];
        for key in &info.keys {
            push_unique(&mut other_required, key.other.column);
        }
        extend_required(&mut other_required, required, other_mask);

        let (other_plan, other_columns) = self.node(other_mask, &other_required)?;
        let applied_plan = Arc::clone(&self.relations[applied]);

        // The applied relation builds so the other side streams, which is what the
        // `Right` join types emit the rows of.
        let applied_schema = applied_plan.schema();
        let on = info
            .keys
            .iter()
            .map(|key| {
                Ok((
                    Arc::clone(&key.expr),
                    key_expr(&other_columns, &other_plan, &key.other)?,
                ))
            })
            .collect::<Result<Vec<_>>>()?;

        let join_type = match info.kind {
            AppliedKind::Semi => JoinType::RightSemi,
            AppliedKind::Anti => JoinType::RightAnti,
            AppliedKind::Outer => JoinType::Right,
            AppliedKind::Mark => JoinType::RightMark,
        };
        let applied_columns = info.kind.emitted(applied, applied_schema.fields().len());

        self.build_join(
            Built {
                plan: applied_plan,
                columns: applied_columns,
            },
            Built {
                plan: other_plan,
                columns: other_columns,
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

/// The key again, reading the column where the rebuilt input emits it.
fn key_expr(
    columns: &[ColRef],
    plan: &Arc<dyn ExecutionPlan>,
    key: &Key,
) -> Result<PhysicalExprRef> {
    let Some(index) = position(columns, key.column) else {
        return internal_err!("join enumeration lost column {:?}", key.column);
    };
    let column = Arc::new(Column::new(plan.schema().field(index).name(), index))
        as PhysicalExprRef;
    let Some(wrapper) = &key.wrapper else {
        return Ok(column);
    };
    Arc::clone(wrapper)
        .transform(|expr| {
            Ok(match expr.downcast_ref::<Column>() {
                Some(_) => Transformed::yes(Arc::clone(&column)),
                None => Transformed::no(expr),
            })
        })
        .data()
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
    let Some(solution) = dphyp::solve(graph, model.as_ref()) else {
        return Ok(None);
    };

    // Nothing cheaper found, so leave the plan alone rather than rebuild it into an
    // order the estimates cannot tell apart from the one it has.
    if solution.cost >= model.tree_cost(&graph.original_nodes) {
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
