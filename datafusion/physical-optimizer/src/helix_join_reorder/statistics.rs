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

//! Deriving cost model inputs from a [`JoinGraph`]'s statistics.
//!
//! # Cardinalities
//!
//! A relation's weight is simply its estimated row count. Because a plan that
//! is not a reorderable join becomes an opaque leaf during flattening, a
//! `FilterExec` between two joins reports rows *after* filtering, so the cost
//! model sees post-filter cardinalities without special handling.
//!
//! # Selectivities
//!
//! The System R estimate for an equijoin is
//!
//! ```text
//! |A join B| = |A| * |B| / max(ndv_A, ndv_B)
//! ```
//!
//! Writing `V` for that denominator: assuming values are spread evenly and the
//! smaller side's domain sits inside the larger's, each of the `V` values
//! appears in `|A| / V` rows of A and `|B| / V` rows of B, so the join emits
//! `V * (|A| / V) * (|B| / V)` rows. Matching that against the cost model's
//! `size *= weight; size *= selectivity` gives
//!
//! ```text
//! selectivity = 1 / max(ndv_left, ndv_right)
//! ```
//!
//! NDV comes from [`max_distinct_count`], shared with the cardinality
//! estimation in `joins::utils` so that the two cannot disagree about the same
//! join.
//!
//! # A bias worth knowing
//!
//! When a column has no distinct count, [`max_distinct_count`] falls back to
//! `num_rows - null_count`, i.e. it assumes every non-null value is unique.
//! That is exact for a primary key and wrong for a foreign key, and the error
//! runs one way. For a 1.5M-row fact table whose foreign key has 150k distinct
//! values, joined to a 150k-row dimension table on its primary key, the true
//! result is 1.5M rows; with both distinct counts missing the fact table's
//! inflated NDV wins the `max` and the estimate drops to 150k, a tenfold
//! underestimate. See `star_schema_join_without_ndv_underestimates`.
//!
//! Join ordering compares plans rather than trusting absolute costs, and the
//! bias applies to every edge, so orderings survive it better than the numbers
//! do. Real distinct counts still improve results substantially.
//!
//! # When statistics are unusable
//!
//! Every path here refuses rather than substituting a default. A made-up
//! selectivity is indistinguishable from a measured one once it reaches the
//! cost model, so `None` (leave the plan alone) is the only honest answer when
//! there is nothing to reason from.

use std::sync::Arc;

use datafusion_common::Statistics;
use datafusion_physical_plan::ExecutionPlan;
use datafusion_physical_plan::joins::utils::max_distinct_count;
use datafusion_physical_plan::operator_statistics::StatisticsRegistry;
use datafusion_physical_plan::statistics::{StatisticsArgs, StatisticsContext};

use super::cost_model::{Edge, QueryGraph};
use super::join_graph::{JoinEdge, JoinGraph};

/// Cardinalities and selectivities for one [`JoinGraph`].
///
/// Both are properties of relations and edges rather than of any particular
/// join order, so they are computed once and every order the search considers
/// is then a cheap rearrangement of the same numbers.
#[derive(Debug, Clone, PartialEq)]
pub struct GraphStatistics {
    /// Estimated row count per relation, indexed as in [`JoinGraph::relations`].
    weights: Vec<f64>,
    /// Selectivity per edge, indexed as in [`JoinGraph::edges`].
    selectivities: Vec<f64>,
}

impl GraphStatistics {
    /// Derive cost model inputs for `graph`.
    ///
    /// Returns `None` if any relation has no row count, or any edge has no
    /// usable distinct count on either side.
    pub fn try_new(
        graph: &JoinGraph,
        registry: Option<&StatisticsRegistry>,
    ) -> Option<Self> {
        let statistics = graph
            .relations()
            .iter()
            .map(|relation| relation_statistics(relation.as_ref(), registry))
            .collect::<Option<Vec<_>>>()?;

        let mut weights = Vec::with_capacity(statistics.len());
        for (index, relation) in statistics.iter().enumerate() {
            let Some(&rows) = relation.num_rows.get_value() else {
                log::debug!("helix: relation {index} has no row count estimate");
                return None;
            };
            weights.push(rows as f64);
        }

        let mut selectivities = Vec::with_capacity(graph.edges().len());
        for edge in graph.edges() {
            let Some(denominator) =
                edge_denominator(edge, &statistics[edge.left], &statistics[edge.right])
            else {
                log::debug!(
                    "helix: edge {}-{} has no usable distinct count",
                    edge.left,
                    edge.right
                );
                return None;
            };
            selectivities.push(1.0 / denominator as f64);
        }

        Some(Self {
            weights,
            selectivities,
        })
    }

    /// Hand these numbers to the search, as a priced graph.
    ///
    /// Relation indices are carried through unchanged, so the ids in the
    /// resulting [`JoinTree`] index straight back into [`JoinGraph::relations`].
    ///
    /// `None` when the graph has more relations than the search can afford;
    /// see [`QueryGraph::try_new`].
    ///
    /// [`JoinTree`]: super::cost_model::JoinTree
    pub fn query_graph(&self, graph: &JoinGraph) -> Option<QueryGraph> {
        // `selectivities` is indexed as `graph.edges()`, so the two line up.
        let edges: Vec<Edge> = graph
            .edges()
            .iter()
            .zip(&self.selectivities)
            .map(|(edge, &selectivity)| Edge::new(edge.left, edge.right, selectivity))
            .collect();

        QueryGraph::try_new(self.weights.clone(), &edges)
    }

    /// Estimated row count of a relation.
    pub fn weight(&self, relation: usize) -> f64 {
        self.weights[relation]
    }

    /// Selectivity of the edge joining `a` and `b`, if they are adjacent.
    pub fn selectivity(&self, graph: &JoinGraph, a: usize, b: usize) -> Option<f64> {
        let (left, right) = (a.min(b), a.max(b));
        graph
            .edges()
            .iter()
            .position(|edge| edge.left == left && edge.right == right)
            .map(|index| self.selectivities[index])
    }
}

/// Statistics for one relation, preferring the registry when the session
/// supplies one. Mirrors `join_selection`'s `get_stats`.
fn relation_statistics(
    plan: &dyn ExecutionPlan,
    registry: Option<&StatisticsRegistry>,
) -> Option<Arc<Statistics>> {
    let statistics = match registry {
        Some(registry) => registry
            .compute(plan)
            .map(|extended| Arc::clone(extended.base_arc())),
        None => StatisticsContext::new().compute(plan, &StatisticsArgs::new()),
    };

    statistics
        .inspect_err(|error| log::debug!("helix: statistics unavailable: {error}"))
        .ok()
}

/// The `V` in `|A| * |B| / V` for one edge.
///
/// Composite keys are almost always correlated, so the denominators of the
/// individual keys are not multiplied. Following the same Spark Catalyst rule
/// as `estimate_inner_join_cardinality`, the most selective key wins, which is
/// the largest denominator. Keys whose NDV is unknown are skipped; the edge is
/// only unusable when no key yields one.
fn edge_denominator(
    edge: &JoinEdge,
    left: &Statistics,
    right: &Statistics,
) -> Option<usize> {
    let mut denominator: Option<usize> = None;

    for &(left_column, right_column) in &edge.keys {
        let left_stats = left.column_statistics.get(left_column)?;
        let right_stats = right.column_statistics.get(right_column)?;

        let left_ndv = max_distinct_count(&left.num_rows, left_stats);
        let right_ndv = max_distinct_count(&right.num_rows, right_stats);

        if let Some(&key_ndv) = left_ndv.max(&right_ndv).get_value() {
            denominator =
                Some(denominator.map_or(key_ndv, |current| current.max(key_ndv)));
        }
    }

    // A value domain holds at least one value. This also keeps an empty
    // relation from producing an infinite selectivity: its weight is zero, so
    // a fanout of `0 * 1` correctly collapses everything downstream to zero.
    Some(denominator?.max(1))
}

#[cfg(test)]
mod tests {
    use super::*;

    use crate::helix_join_reorder::test_support::{
        col, col_ndv, inexact_relation, join, relation, relation_without_row_count,
    };
    use std::sync::Arc;

    /// Build a two-relation graph and return its single edge's selectivity.
    fn selectivity_of(
        left: Arc<dyn ExecutionPlan>,
        right: Arc<dyn ExecutionPlan>,
        keys: &[(usize, usize)],
    ) -> Option<f64> {
        let plan = join(left, right, keys);
        let graph = JoinGraph::try_new(&plan).expect("a reorderable clump");
        let statistics = GraphStatistics::try_new(&graph, None)?;
        Some(statistics.selectivities[0])
    }

    #[test]
    fn selectivity_uses_the_larger_distinct_count() {
        // 1 / max(100, 500)
        let selectivity = selectivity_of(
            relation(vec![col_ndv("k", 100)], 1_000),
            relation(vec![col_ndv("k", 500)], 2_000),
            &[(0, 0)],
        );

        assert_eq!(selectivity, Some(1.0 / 500.0));
    }

    #[test]
    fn distinct_count_is_clamped_to_the_row_count() {
        // A 10-row relation cannot hold 900 distinct values, so the claim is
        // capped and the other side's 50 wins.
        let selectivity = selectivity_of(
            relation(vec![col_ndv("k", 900)], 10),
            relation(vec![col_ndv("k", 50)], 100),
            &[(0, 0)],
        );

        assert_eq!(selectivity, Some(1.0 / 50.0));
    }

    #[test]
    fn absent_distinct_count_falls_back_to_non_null_rows() {
        // No distinct counts anywhere, so NDV becomes num_rows - null_count
        // on each side and the larger relation sets the denominator.
        let selectivity = selectivity_of(
            relation(vec![col("k")], 300),
            relation(vec![col("k")], 700),
            &[(0, 0)],
        );

        assert_eq!(selectivity, Some(1.0 / 700.0));
    }

    /// The bias documented in the module docs, pinned as a test so the size of
    /// the error is visible rather than folklore.
    #[test]
    fn star_schema_join_without_ndv_underestimates() {
        let fact_rows = 1_500_000.0;
        let dimension_rows = 150_000.0;

        // With real distinct counts the estimate is exact: every order
        // matches its one customer, so the join emits one row per order.
        let with_ndv = selectivity_of(
            relation(vec![col_ndv("custkey", 150_000)], 1_500_000),
            relation(vec![col_ndv("custkey", 150_000)], 150_000),
            &[(0, 0)],
        )
        .expect("statistics are usable");
        assert_eq!(fact_rows * dimension_rows * with_ndv, 1_500_000.0);

        // Without them the fact table's foreign key is assumed unique, which
        // inflates its NDV to the row count and wins the `max`.
        let without_ndv = selectivity_of(
            relation(vec![col("custkey")], 1_500_000),
            relation(vec![col("custkey")], 150_000),
            &[(0, 0)],
        )
        .expect("statistics are usable");
        assert_eq!(fact_rows * dimension_rows * without_ndv, 150_000.0);
    }

    #[test]
    fn null_count_reduces_the_fallback_ndv() {
        // With no distinct count the fallback is non-null rows, so 400 nulls
        // out of 1000 leaves 600 — which still beats the other side's 500 and
        // sets the denominator.
        let selectivity = selectivity_of(
            relation(vec![col("k").nulls(400)], 1_000),
            relation(vec![col("k")], 500),
            &[(0, 0)],
        );

        assert_eq!(selectivity, Some(1.0 / 600.0));
    }

    #[test]
    fn inexact_statistics_are_usable() {
        // Only `Absent` means "no information". An estimate, such as the row
        // count downstream of a filter, is still worth reasoning from.
        let selectivity = selectivity_of(
            inexact_relation(vec![col_ndv("k", 250)], 900),
            inexact_relation(vec![col_ndv("k", 120)], 700),
            &[(0, 0)],
        );

        assert_eq!(selectivity, Some(1.0 / 250.0));
    }

    #[test]
    fn composite_keys_take_the_most_selective_rather_than_the_product() {
        // Denominators 100 and 40: the product would be 4000, wildly
        // overstating selectivity for keys that are usually correlated.
        let selectivity = selectivity_of(
            relation(vec![col_ndv("a", 100), col_ndv("b", 40)], 1_000),
            relation(vec![col_ndv("a", 20), col_ndv("b", 10)], 1_000),
            &[(0, 0), (1, 1)],
        );

        assert_eq!(selectivity, Some(1.0 / 100.0));
    }

    #[test]
    fn declines_when_a_row_count_is_missing() {
        let plan = join(
            relation(vec![col_ndv("k", 10)], 100),
            relation_without_row_count(&["k"]),
            &[(0, 0)],
        );
        let graph = JoinGraph::try_new(&plan).expect("a reorderable clump");

        assert!(GraphStatistics::try_new(&graph, None).is_none());
    }

    #[test]
    fn an_empty_relation_does_not_produce_an_infinite_selectivity() {
        let selectivity = selectivity_of(
            relation(vec![col_ndv("k", 0)], 0),
            relation(vec![col_ndv("k", 0)], 0),
            &[(0, 0)],
        )
        .expect("zero rows is a usable estimate");

        assert!(selectivity.is_finite());
        assert_eq!(selectivity, 1.0);
    }

    #[test]
    fn builds_a_query_graph_carrying_every_edge() {
        // Sizes are the check: a subset's size is the product of its weights
        // and of the selectivity of every edge inside it, so a dropped or
        // misplaced edge shows up as a size that is off by that factor.
        let hub = relation(vec![col_ndv("k", 1_000), col_ndv("sa", 500)], 1_000);
        let spoke = relation(vec![col_ndv("k", 50)], 50);
        let central = relation(vec![col_ndv("ka", 200), col_ndv("kb", 200)], 200);

        let left = join(hub, spoke, &[(1, 0)]);
        let plan = join(left, central, &[(0, 0)]);

        let graph = JoinGraph::try_new(&plan).expect("a reorderable clump");
        let statistics =
            GraphStatistics::try_new(&graph, None).expect("statistics are usable");
        let query_graph = statistics
            .query_graph(&graph)
            .expect("three relations is within the cap");

        assert_eq!(query_graph.relation_count(), 3);
        // Relation 0 alone.
        assert_eq!(query_graph.subset_size(0b001), 1_000.0);
        // Relations 0 and 1, across the edge with 500 distinct values.
        assert_eq!(query_graph.subset_size(0b011), 1_000.0 * 50.0 / 500.0);
        // Relations 0 and 2, across the edge with 1000.
        assert_eq!(query_graph.subset_size(0b101), 1_000.0 * 200.0 / 1_000.0);
        // All three: both edges apply, and the absent 1-2 pair contributes
        // nothing rather than being treated as a cross product.
        assert_eq!(
            query_graph.subset_size(0b111),
            1_000.0 * 50.0 * 200.0 / 500.0 / 1_000.0
        );
    }

    #[test]
    fn prices_every_edge_of_a_diamond() {
        // A cycle: the closing edge must be priced like any other, since the
        // search multiplies all four selectivities into the full subset.
        let p0 = relation(vec![col_ndv("ka", 100), col_ndv("kb", 100)], 100);
        let a0 = relation(vec![col_ndv("p0", 40), col_ndv("p1", 40)], 40);
        let b0 = relation(vec![col_ndv("p0", 60), col_ndv("p1", 60)], 60);
        let p1 = relation(vec![col_ndv("a", 200), col_ndv("b", 200)], 200);

        let left = join(p0, a0, &[(0, 0)]);
        let left = join(left, b0, &[(1, 0)]);
        let plan = join(left, p1, &[(3, 0), (5, 1)]);

        let graph = JoinGraph::try_new(&plan).expect("a reorderable clump");
        assert_eq!(graph.edges().len(), 4);

        let statistics =
            GraphStatistics::try_new(&graph, None).expect("statistics are usable");

        // Each edge takes the larger of the two distinct counts.
        assert_eq!(statistics.selectivity(&graph, 0, 1), Some(1.0 / 100.0));
        assert_eq!(statistics.selectivity(&graph, 0, 2), Some(1.0 / 100.0));
        assert_eq!(statistics.selectivity(&graph, 1, 3), Some(1.0 / 200.0));
        assert_eq!(statistics.selectivity(&graph, 2, 3), Some(1.0 / 200.0));
        // The diagonal of the cycle is not an edge.
        assert_eq!(statistics.selectivity(&graph, 1, 2), None);
    }
}
