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

//! Exhaustive join ordering by dynamic programming over connected subgraphs.
//!
//! # The cost being minimized
//!
//! The size of a set of relations is the product of their cardinalities times
//! the selectivity of every edge inside the set, and the cost of a plan is the
//! sum of the sizes of its intermediate results:
//!
//! ```text
//! cost(S) = min over splits (L, R) of S:  cost(L) + cost(R)  +  size(S)
//! ```
//!
//! with single relations free. Both halves of a split must themselves be
//! connected, which is what keeps cross products out of the search. Every
//! bushy order is considered, not just left deep ones, which matters on a
//! helix: the cheapest order there routinely joins two composites together.
//!
//! # Why a diamond's size is an estimate
//!
//! Multiplying the selectivities of the edges around a cycle assumes those
//! predicates are independent when they are not, so a diamond's size is
//! approximate. It is the standard approximation, and it is only an
//! *estimate*: [`rewrite`] applies every predicate exactly once however the
//! search splits a subset, so an inaccurate estimate costs plan quality and
//! never correctness.
//!
//! # Relationship to the reference implementation
//!
//! This is a port of the Python prototype in `reference/`, kept structurally
//! parallel to it. It deliberately does **not** reproduce the reference bit
//! for bit, because the reference has no stable bit pattern to reproduce: its
//! subset size multiplies cardinalities while iterating a `frozenset` of
//! relation names, Python randomizes string hashing per process, and floating
//! point multiplication is not associative. The same input can therefore
//! produce several different last bits across runs of the prototype itself.
//! The tests below compare against it within a relative tolerance and assert
//! the chosen *tree* exactly, which is the part that becomes a plan.
//! Deviations from the reference are marked `Deviation:`.
//!
//! [`rewrite`]: super::rewrite

/// Upper bound on relations in one search.
///
/// The search is `O(3^n)` in time and `O(2^n)` in memory, so this is what
/// keeps planning time bounded:
///
/// ```text
/// relations   subset pairs
///         7          ~2_000
///        10         ~59_000
///        13      ~1_600_000
///        16     ~43_000_000
/// ```
///
/// Thirteen relations covers a four diamond helix and costs a few
/// milliseconds. A larger helix is left alone rather than planned slowly.
pub const MAX_RELATIONS: usize = 13;

/// A subset of relations, one bit per relation index.
type Subset = u64;

/// The bit standing for `relation`.
fn bit(relation: usize) -> Subset {
    1 << relation
}

/// The lowest relation present in `subset`, as a single bit.
fn lowest(subset: Subset) -> Subset {
    subset & subset.wrapping_neg()
}

/// An equijoin edge, with the selectivity of the predicate connecting its two
/// relations.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct Edge {
    /// Index of one relation.
    pub left: usize,
    /// Index of the other.
    pub right: usize,
    /// Fraction of the cross product of the two that survives the join.
    pub selectivity: f64,
}

impl Edge {
    /// Create an edge between two relations.
    pub fn new(left: usize, right: usize, selectivity: f64) -> Self {
        Self {
            left,
            right,
            selectivity,
        }
    }
}

/// A join graph priced for the search: a cardinality per relation and a
/// selectivity per pair.
///
/// Deviation: the reference keeps relations in dictionaries keyed by name.
/// Here they are indices into the caller's own relation list, so the ids in
/// the resulting [`JoinTree`] point straight back at it.
#[derive(Debug, Clone, PartialEq)]
pub struct QueryGraph {
    /// Cardinality per relation.
    weights: Vec<f64>,
    /// `selectivity[i][j]` for `i < j`, and `1.0` where no edge joins the two,
    /// so a subset's size can be formed without consulting an edge list.
    selectivity: Vec<Vec<f64>>,
    /// Neighbors of each relation, as a subset.
    neighbors: Vec<Subset>,
}

impl QueryGraph {
    /// Build a graph, or return `None` if the search cannot draw a conclusion
    /// from it.
    ///
    /// Deviation: the reference takes hand-entered numbers and trusts them.
    /// Statistics-derived inputs are not trustworthy in the same way — an
    /// absent estimate can surface as `NaN`, a zero distinct count as an
    /// infinite selectivity — and feeding those through would produce a
    /// confidently wrong order. They are refused here so the caller can leave
    /// the plan alone.
    ///
    /// Edges must name distinct relations in range, and no pair may appear
    /// twice: several predicates joining the same two relations are one edge
    /// whose selectivities have already been combined.
    pub fn try_new(weights: Vec<f64>, edges: &[Edge]) -> Option<Self> {
        fn usable(value: f64) -> bool {
            value.is_finite() && value >= 0.0
        }

        let count = weights.len();
        if !(2..=MAX_RELATIONS).contains(&count) {
            return None;
        }
        if !weights.iter().copied().all(usable) {
            return None;
        }

        let mut selectivity = vec![vec![1.0; count]; count];
        let mut neighbors = vec![0; count];
        for edge in edges {
            let (low, high) = (edge.left.min(edge.right), edge.left.max(edge.right));
            if low == high || high >= count || !usable(edge.selectivity) {
                return None;
            }
            // Set only if some earlier edge already claimed this pair.
            if neighbors[low] & bit(high) != 0 {
                return None;
            }
            selectivity[low][high] = edge.selectivity;
            neighbors[low] |= bit(high);
            neighbors[high] |= bit(low);
        }

        Some(Self {
            weights,
            selectivity,
            neighbors,
        })
    }

    /// How many relations this graph holds.
    pub fn relation_count(&self) -> usize {
        self.weights.len()
    }

    /// Estimated rows in the result of joining every relation in `subset`.
    ///
    /// The product of their cardinalities and the selectivity of every edge
    /// with both endpoints inside. Cardinalities are taken first and pairs in
    /// ascending order, mirroring the reference's two phases; the grouping is
    /// where the two implementations' last bits part company.
    pub fn subset_size(&self, subset: Subset) -> f64 {
        let mut total = 1.0;

        for (relation, &weight) in self.weights.iter().enumerate() {
            if subset & bit(relation) != 0 {
                total *= weight;
            }
        }

        for (left, row) in self.selectivity.iter().enumerate() {
            if subset & bit(left) == 0 {
                continue;
            }
            for (right, &selectivity) in row.iter().enumerate().skip(left + 1) {
                if subset & bit(right) != 0 {
                    total *= selectivity;
                }
            }
        }

        total
    }

    /// Whether every relation in `subset` is reachable from every other
    /// without leaving it.
    ///
    /// Expands a frontier exactly as the reference does, over subsets rather
    /// than sets.
    fn is_connected(&self, subset: Subset) -> bool {
        if subset == 0 {
            return false;
        }

        let mut reached = lowest(subset);
        let mut frontier = reached;
        while frontier != 0 {
            let mut adjacent = 0;
            let mut remaining = frontier;
            while remaining != 0 {
                let relation = remaining.trailing_zeros() as usize;
                adjacent |= self.neighbors[relation] & subset;
                remaining &= remaining - 1;
            }
            frontier = adjacent & !reached;
            reached |= frontier;
        }

        reached == subset
    }

    /// Whether any edge joins a relation in `left` to one in `right`.
    pub fn crosses(&self, left: Subset, right: Subset) -> bool {
        let mut remaining = left;
        while remaining != 0 {
            let relation = remaining.trailing_zeros() as usize;
            if self.neighbors[relation] & right != 0 {
                return true;
            }
            remaining &= remaining - 1;
        }
        false
    }
}

/// A join order, as a binary tree over relation indices.
///
/// Indices are positions in the caller's relation list, so a tree can be read
/// straight back against the graph it was derived from.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum JoinTree {
    /// A single relation, by index.
    Leaf(usize),
    /// Two subtrees joined on every predicate that crosses between them.
    Join(Box<JoinTree>, Box<JoinTree>),
}

impl JoinTree {
    /// Join two subtrees.
    pub fn join(left: JoinTree, right: JoinTree) -> JoinTree {
        JoinTree::Join(Box::new(left), Box::new(right))
    }

    /// The relations this tree covers, left to right.
    pub fn leaves(&self) -> Vec<usize> {
        let mut leaves = Vec::new();
        self.collect_leaves(&mut leaves);
        leaves
    }

    fn collect_leaves(&self, out: &mut Vec<usize>) {
        match self {
            Self::Leaf(relation) => out.push(*relation),
            Self::Join(left, right) => {
                left.collect_leaves(out);
                right.collect_leaves(out);
            }
        }
    }
}

/// The cheapest join order found, and what it is estimated to cost.
///
/// Deviation: the reference returns only the cost of the best order, which is
/// enough to compare orders but not to build one. Recording which split won at
/// each subset lets the order itself be recovered.
#[derive(Debug, Clone, PartialEq)]
pub struct DpPlan {
    /// Estimated cost, in rows flowing through joins.
    pub cost: f64,
    /// The order itself.
    pub tree: JoinTree,
}

/// Find the cheapest join order for `graph`.
///
/// Returns `None` when the graph is disconnected — every order would then need
/// a cross product, which this does not consider — or when no order has a
/// finite cost, which a chain of large cardinalities can produce by
/// overflowing. Callers should read `None` as "leave the plan alone" rather
/// than as "this plan is free".
pub fn optimal_join_order(graph: &QueryGraph) -> Option<DpPlan> {
    let count = graph.relation_count();
    let all = bit(count) - 1;

    let mut cost = vec![f64::INFINITY; all as usize + 1];
    let mut connected = vec![false; all as usize + 1];
    // The winning left half of each subset, for recovering the tree.
    let mut split: Vec<Subset> = vec![0; all as usize + 1];

    // Ascending order suffices: every proper subset of `subset` is numerically
    // smaller than it, so both halves of any split are already solved.
    for subset in 1..=all {
        if !graph.is_connected(subset) {
            continue;
        }
        connected[subset as usize] = true;

        if subset.count_ones() == 1 {
            cost[subset as usize] = 0.0;
            continue;
        }

        // Pinning the lowest relation to the left half visits each unordered
        // split once instead of twice.
        let anchor = lowest(subset);
        let rest = subset ^ anchor;
        let mut extra: Subset = 0;
        let mut best = f64::INFINITY;
        loop {
            let left = anchor | extra;
            let right = subset ^ left;

            if right != 0 && connected[left as usize] && connected[right as usize] {
                // Implied, not checked: `subset` is connected and these halves
                // partition it, so some edge must run between them. Asserted
                // rather than tested so the reasoning stays visible without
                // paying for it in release builds.
                debug_assert!(graph.crosses(left, right));

                let candidate = cost[left as usize] + cost[right as usize];
                // Strictly less, so the lowest left half wins ties and the
                // chosen order does not depend on enumeration order. This is a
                // different tie break from the reference's, which walks splits
                // in name order; both are arbitrary, and only determinism
                // matters, since the emitted plan is compared against
                // committed expected output.
                if candidate < best {
                    best = candidate;
                    split[subset as usize] = left;
                }
            }

            if extra == rest {
                break;
            }
            // The next subset of `rest`, ascending.
            extra = (extra | !rest).wrapping_add(1) & rest;
        }

        cost[subset as usize] = best + graph.subset_size(subset);
    }

    let total = cost[all as usize];
    total.is_finite().then(|| DpPlan {
        cost: total,
        tree: materialize(all, &split),
    })
}

/// Rebuild the winning order from the recorded splits.
fn materialize(subset: Subset, split: &[Subset]) -> JoinTree {
    if subset.count_ones() == 1 {
        return JoinTree::Leaf(subset.trailing_zeros() as usize);
    }

    let left = split[subset as usize];
    JoinTree::Join(
        Box::new(materialize(left, split)),
        Box::new(materialize(subset ^ left, split)),
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Relative tolerance for comparisons against the reference.
    ///
    /// Reassociating a product of `n` terms moves the result by roughly
    /// `n * f64::EPSILON`, around `1e-15` here, while a genuine difference in
    /// the search shows up as a whole factor. This sits far above the former
    /// and far below the latter, so it neither flakes nor forgives.
    const TOLERANCE: f64 = 1e-9;

    fn assert_close(actual: f64, expected: f64) {
        let allowed = TOLERANCE * expected.abs().max(1.0);
        assert!(
            (actual - expected).abs() <= allowed,
            "expected {expected}, got {actual}, which is outside {allowed}"
        );
    }

    fn leaf(relation: usize) -> JoinTree {
        JoinTree::Leaf(relation)
    }

    fn join(left: JoinTree, right: JoinTree) -> JoinTree {
        JoinTree::join(left, right)
    }

    fn solve(graph: &QueryGraph) -> DpPlan {
        optimal_join_order(graph).expect("a connected graph with usable statistics")
    }

    /// One diamond: `P0` and `P1` linked by both `A0` and `B0`.
    ///
    /// Relations are numbered in the order the reference sorts their names, so
    /// its trees and these read the same way: `A0` 0, `B0` 1, `P0` 2, `P1` 3.
    fn single_diamond() -> QueryGraph {
        QueryGraph::try_new(
            vec![50.0, 200.0, 1000.0, 5000.0],
            &[
                Edge::new(2, 0, 0.01),
                Edge::new(2, 1, 0.005),
                Edge::new(0, 3, 0.002),
                Edge::new(1, 3, 0.001),
            ],
        )
        .expect("a valid graph")
    }

    /// Two diamonds sharing `P1`: `A0` 0, `A1` 1, `B0` 2, `B1` 3, `P0` 4,
    /// `P1` 5, `P2` 6.
    fn two_diamond_helix() -> QueryGraph {
        QueryGraph::try_new(
            vec![50.0, 80.0, 200.0, 300.0, 1000.0, 5000.0, 2000.0],
            &[
                Edge::new(4, 0, 0.01),
                Edge::new(4, 2, 0.005),
                Edge::new(0, 5, 0.002),
                Edge::new(2, 5, 0.001),
                Edge::new(5, 1, 0.02),
                Edge::new(5, 3, 0.004),
                Edge::new(1, 6, 0.0015),
                Edge::new(3, 6, 0.003),
            ],
        )
        .expect("a valid graph")
    }

    /// A four relation path `a - b - c - d`, numbered in that order.
    fn path() -> QueryGraph {
        QueryGraph::try_new(
            vec![100.0, 1000.0, 20000.0, 300.0],
            &[
                Edge::new(0, 1, 0.01),
                Edge::new(1, 2, 0.001),
                Edge::new(2, 3, 0.005),
            ],
        )
        .expect("a valid graph")
    }

    /// The subset a tree covers.
    fn subset_of(tree: &JoinTree) -> Subset {
        tree.leaves()
            .into_iter()
            .fold(0, |subset, relation| subset | bit(relation))
    }

    /// Sum the sizes of a tree's intermediate results, independently of the
    /// search that produced it.
    fn cost_of(graph: &QueryGraph, tree: &JoinTree) -> f64 {
        match tree {
            JoinTree::Leaf(_) => 0.0,
            JoinTree::Join(left, right) => {
                cost_of(graph, left)
                    + cost_of(graph, right)
                    + graph.subset_size(subset_of(tree))
            }
        }
    }

    /// Whether every right hand input is a base relation.
    fn is_left_deep(tree: &JoinTree) -> bool {
        match tree {
            JoinTree::Leaf(_) => true,
            JoinTree::Join(left, right) => {
                matches!(**right, JoinTree::Leaf(_)) && is_left_deep(left)
            }
        }
    }

    #[test]
    fn matches_the_reference_on_a_single_diamond() {
        let plan = solve(&single_diamond());

        assert_close(plan.cost, 605.0);
        // (((A0 P1) B0) P0): the two smallest relations first, then the
        // diamond closes on B0 before P0 is absorbed.
        assert_eq!(
            plan.tree,
            join(join(join(leaf(0), leaf(3)), leaf(1)), leaf(2))
        );
    }

    #[test]
    fn matches_the_reference_on_a_two_diamond_helix() {
        let plan = solve(&two_diamond_helix());

        // The reference prints 552.85439999999994; the digits past this are
        // the reassociation noise the tolerance exists to absorb.
        assert_close(plan.cost, 552.854_4);
        // (((A0 (((A1 P2) B1) P1)) B0) P0)
        assert_eq!(
            plan.tree,
            join(
                join(
                    join(
                        leaf(0),
                        join(join(join(leaf(1), leaf(6)), leaf(3)), leaf(5))
                    ),
                    leaf(2)
                ),
                leaf(4)
            )
        );
    }

    #[test]
    fn matches_the_reference_on_a_path() {
        // Small enough to check by hand: joining `d` last costs
        // 1_000 + 20_000 + 30_000, while joining `a` last costs
        // 30_000 + 30_000 and splitting down the middle costs
        // 1_000 + 30_000 + 30_000.
        let plan = solve(&path());

        assert_close(plan.cost, 51_000.0);
        assert_eq!(
            plan.tree,
            join(join(join(leaf(0), leaf(1)), leaf(2)), leaf(3))
        );
    }

    #[test]
    fn two_relations_have_a_single_order() {
        let graph = QueryGraph::try_new(vec![100.0, 1000.0], &[Edge::new(0, 1, 0.01)])
            .expect("a valid graph");
        let plan = solve(&graph);

        assert_close(plan.cost, 1000.0);
        assert_eq!(plan.tree, join(leaf(0), leaf(1)));
    }

    #[test]
    fn the_chosen_tree_costs_what_the_search_said() {
        // The reference returns only a cost, so the recorded splits are the
        // one piece of this with nothing to compare against. Pricing the tree
        // back independently is what catches a wrong split.
        for graph in [single_diamond(), two_diamond_helix(), path()] {
            let plan = solve(&graph);
            assert_close(cost_of(&graph, &plan.tree), plan.cost);
        }
    }

    #[test]
    fn covers_every_relation_exactly_once() {
        for graph in [single_diamond(), two_diamond_helix(), path()] {
            let mut leaves = solve(&graph).tree.leaves();
            leaves.sort_unstable();

            assert_eq!(leaves, (0..graph.relation_count()).collect::<Vec<_>>());
        }
    }

    #[test]
    fn every_join_has_a_predicate() {
        // A join with no connecting predicate is a cross product: the same
        // columns, catastrophically more rows.
        fn check(graph: &QueryGraph, tree: &JoinTree) {
            let JoinTree::Join(left, right) = tree else {
                return;
            };

            assert!(graph.crosses(subset_of(left), subset_of(right)));
            check(graph, left);
            check(graph, right);
        }

        for graph in [single_diamond(), two_diamond_helix(), path()] {
            check(&graph, &solve(&graph).tree);
        }
    }

    #[test]
    fn finds_a_bushy_order() {
        // The point of searching rather than sorting: on the helix the best
        // order joins two composites together, which no left deep enumeration
        // would reach.
        assert!(!is_left_deep(&solve(&two_diamond_helix()).tree));
        // And it does not do so gratuitously: the path's best order is left
        // deep, and it is found as such.
        assert!(is_left_deep(&solve(&path()).tree));
    }

    #[test]
    fn is_deterministic() {
        let graph = two_diamond_helix();
        let first = solve(&graph);
        let second = solve(&graph);

        assert_eq!(first.cost.to_bits(), second.cost.to_bits());
        assert_eq!(first.tree, second.tree);
    }

    #[test]
    fn declines_a_disconnected_graph() {
        // Two relations with no predicate between them: every order needs a
        // cross product, so there is nothing to choose.
        let graph =
            QueryGraph::try_new(vec![100.0, 1000.0, 20.0], &[Edge::new(0, 1, 0.01)])
                .expect("a valid graph");

        assert_eq!(optimal_join_order(&graph), None);
    }

    #[test]
    fn declines_more_relations_than_the_cap() {
        let weights = vec![10.0; MAX_RELATIONS + 1];
        let edges: Vec<Edge> = (1..weights.len())
            .map(|relation| Edge::new(relation - 1, relation, 0.1))
            .collect();

        assert_eq!(QueryGraph::try_new(weights, &edges), None);
    }

    #[test]
    fn declines_fewer_than_two_relations() {
        assert_eq!(QueryGraph::try_new(vec![100.0], &[]), None);
    }

    #[test]
    fn declines_unusable_numbers() {
        let edge = [Edge::new(0, 1, 0.01)];

        assert_eq!(QueryGraph::try_new(vec![f64::NAN, 1000.0], &edge), None);
        assert_eq!(
            QueryGraph::try_new(vec![f64::INFINITY, 1000.0], &edge),
            None
        );
        assert_eq!(QueryGraph::try_new(vec![-1.0, 1000.0], &edge), None);
        assert_eq!(
            QueryGraph::try_new(vec![100.0, 1000.0], &[Edge::new(0, 1, f64::NAN)]),
            None
        );
    }

    #[test]
    fn declines_a_malformed_edge() {
        let weights = vec![100.0, 1000.0];

        // Out of range.
        assert_eq!(
            QueryGraph::try_new(weights.clone(), &[Edge::new(0, 2, 0.01)]),
            None
        );
        // A relation joined to itself.
        assert_eq!(
            QueryGraph::try_new(weights.clone(), &[Edge::new(1, 1, 0.01)]),
            None
        );
        // The same pair twice: the caller must combine those into one edge,
        // since silently keeping the last would drop a predicate from the
        // estimate.
        assert_eq!(
            QueryGraph::try_new(weights, &[Edge::new(0, 1, 0.01), Edge::new(1, 0, 0.02)]),
            None
        );
    }

    #[test]
    fn declines_when_every_order_overflows() {
        // Cardinalities large enough that the product leaves `f64` range. An
        // infinite cost is not a cheap plan, and must not be read as one.
        let huge = 1e300;
        let graph = QueryGraph::try_new(
            vec![huge, huge, huge],
            &[Edge::new(0, 1, 1.0), Edge::new(1, 2, 1.0)],
        )
        .expect("a valid graph");

        assert_eq!(optimal_join_order(&graph), None);
    }
}
