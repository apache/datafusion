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

use super::cost_model::{DoubleStar, Relation, Spoke};
use super::join_graph::{DoubleStarShape, JoinEdge, JoinGraph};

/// Cardinalities and selectivities for one [`JoinGraph`].
///
/// Both are properties of relations and edges rather than of any particular
/// double star reading of the graph, so they are computed once and every
/// candidate shape is then a cheap rearrangement of the same numbers.
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
                log::debug!("double star: relation {index} has no row count estimate");
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
                    "double star: edge {}-{} has no usable distinct count",
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

    /// Attach these numbers to one reading of the graph.
    ///
    /// Relation indices are carried through unchanged, so the ids in the
    /// resulting [`crate::double_star_join_reorder::cost_model::DoubleStarPlan`]
    /// index straight back into [`JoinGraph::relations`].
    pub fn double_star(
        &self,
        graph: &JoinGraph,
        shape: &DoubleStarShape,
    ) -> Option<DoubleStar> {
        Some(DoubleStar {
            hub_a: Relation::new(shape.hub_a, self.weights[shape.hub_a]),
            hub_b: Relation::new(shape.hub_b, self.weights[shape.hub_b]),
            central: Relation::new(shape.central, self.weights[shape.central]),
            sel_a: self.selectivity(graph, shape.hub_a, shape.central)?,
            sel_b: self.selectivity(graph, shape.hub_b, shape.central)?,
            spokes_a: self.spokes(graph, shape.hub_a, &shape.spokes_a)?,
            spokes_b: self.spokes(graph, shape.hub_b, &shape.spokes_b)?,
        })
    }

    /// Estimated row count of a relation.
    pub fn weight(&self, relation: usize) -> f64 {
        self.weights[relation]
    }

    /// Selectivity of the edge joining `a` and `b`, if they are adjacent.
    fn selectivity(&self, graph: &JoinGraph, a: usize, b: usize) -> Option<f64> {
        let (left, right) = (a.min(b), a.max(b));
        graph
            .edges()
            .iter()
            .position(|edge| edge.left == left && edge.right == right)
            .map(|index| self.selectivities[index])
    }

    /// Turn spoke relation indices into cost model spokes against `hub`.
    fn spokes(
        &self,
        graph: &JoinGraph,
        hub: usize,
        spokes: &[usize],
    ) -> Option<Vec<Spoke>> {
        spokes
            .iter()
            .map(|&spoke| {
                Some(Spoke::new(
                    spoke,
                    self.weights[spoke],
                    self.selectivity(graph, hub, spoke)?,
                ))
            })
            .collect()
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
        .inspect_err(|error| log::debug!("double star: statistics unavailable: {error}"))
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

    use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
    use datafusion_common::stats::Precision;
    use datafusion_common::{ColumnStatistics, JoinType, Result};
    use datafusion_execution::{SendableRecordBatchStream, TaskContext};
    use datafusion_physical_expr::expressions::Column;
    use datafusion_physical_expr::{EquivalenceProperties, Partitioning};
    use datafusion_physical_plan::execution_plan::{Boundedness, EmissionType};
    use datafusion_physical_plan::joins::HashJoinExecBuilder;
    use datafusion_physical_plan::{DisplayAs, DisplayFormatType, PlanProperties};
    use std::fmt::Formatter;

    /// A leaf relation with statistics fixed by the test.
    #[derive(Debug)]
    struct FakeRelation {
        schema: SchemaRef,
        statistics: Arc<Statistics>,
        properties: Arc<PlanProperties>,
    }

    impl FakeRelation {
        /// A relation of `rows` rows whose columns carry the given distinct
        /// counts, `None` meaning the statistic is absent.
        fn build(
            columns: &[(&str, Option<usize>)],
            rows: usize,
        ) -> Arc<dyn ExecutionPlan> {
            let schema: SchemaRef = Arc::new(Schema::new(
                columns
                    .iter()
                    .map(|(name, _)| Field::new(*name, DataType::Int32, false))
                    .collect::<Vec<_>>(),
            ));

            let column_statistics = columns
                .iter()
                .map(|(_, distinct)| ColumnStatistics {
                    distinct_count: match distinct {
                        Some(count) => Precision::Exact(*count),
                        None => Precision::Absent,
                    },
                    null_count: Precision::Exact(0),
                    ..ColumnStatistics::new_unknown()
                })
                .collect();

            let properties = Arc::new(PlanProperties::new(
                EquivalenceProperties::new(Arc::clone(&schema)),
                Partitioning::UnknownPartitioning(1),
                EmissionType::Incremental,
                Boundedness::Bounded,
            ));

            Arc::new(Self {
                schema,
                statistics: Arc::new(Statistics {
                    num_rows: Precision::Exact(rows),
                    total_byte_size: Precision::Absent,
                    column_statistics,
                }),
                properties,
            })
        }

        /// A relation whose row count is unknown.
        fn without_row_count(columns: &[&str]) -> Arc<dyn ExecutionPlan> {
            let schema: SchemaRef = Arc::new(Schema::new(
                columns
                    .iter()
                    .map(|name| Field::new(*name, DataType::Int32, false))
                    .collect::<Vec<_>>(),
            ));
            let properties = Arc::new(PlanProperties::new(
                EquivalenceProperties::new(Arc::clone(&schema)),
                Partitioning::UnknownPartitioning(1),
                EmissionType::Incremental,
                Boundedness::Bounded,
            ));

            Arc::new(Self {
                statistics: Arc::new(Statistics::new_unknown(&schema)),
                schema,
                properties,
            })
        }
    }

    impl DisplayAs for FakeRelation {
        fn fmt_as(&self, _: DisplayFormatType, f: &mut Formatter) -> std::fmt::Result {
            write!(f, "FakeRelation")
        }
    }

    impl ExecutionPlan for FakeRelation {
        fn name(&self) -> &str {
            "FakeRelation"
        }

        fn schema(&self) -> SchemaRef {
            Arc::clone(&self.schema)
        }

        fn properties(&self) -> &Arc<PlanProperties> {
            &self.properties
        }

        fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
            vec![]
        }

        fn with_new_children(
            self: Arc<Self>,
            _: Vec<Arc<dyn ExecutionPlan>>,
        ) -> Result<Arc<dyn ExecutionPlan>> {
            Ok(self)
        }

        fn execute(
            &self,
            _: usize,
            _: Arc<TaskContext>,
        ) -> Result<SendableRecordBatchStream> {
            unimplemented!("FakeRelation is only used for planning")
        }

        fn partition_statistics(&self, _: Option<usize>) -> Result<Arc<Statistics>> {
            Ok(Arc::clone(&self.statistics))
        }
    }

    fn join(
        left: Arc<dyn ExecutionPlan>,
        right: Arc<dyn ExecutionPlan>,
        keys: &[(usize, usize)],
    ) -> Arc<dyn ExecutionPlan> {
        let on = keys
            .iter()
            .map(|&(left_index, right_index)| {
                let left_name = left.schema().field(left_index).name().clone();
                let right_name = right.schema().field(right_index).name().clone();
                (
                    Arc::new(Column::new(&left_name, left_index)) as _,
                    Arc::new(Column::new(&right_name, right_index)) as _,
                )
            })
            .collect();

        HashJoinExecBuilder::new(left, right, on, JoinType::Inner)
            .build_exec()
            .expect("valid inner join")
    }

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
            FakeRelation::build(&[("k", Some(100))], 1_000),
            FakeRelation::build(&[("k", Some(500))], 2_000),
            &[(0, 0)],
        );

        assert_eq!(selectivity, Some(1.0 / 500.0));
    }

    #[test]
    fn distinct_count_is_clamped_to_the_row_count() {
        // A 10-row relation cannot hold 900 distinct values, so the claim is
        // capped and the other side's 50 wins.
        let selectivity = selectivity_of(
            FakeRelation::build(&[("k", Some(900))], 10),
            FakeRelation::build(&[("k", Some(50))], 100),
            &[(0, 0)],
        );

        assert_eq!(selectivity, Some(1.0 / 50.0));
    }

    #[test]
    fn absent_distinct_count_falls_back_to_non_null_rows() {
        // No distinct counts anywhere, so NDV becomes num_rows - null_count
        // on each side and the larger relation sets the denominator.
        let selectivity = selectivity_of(
            FakeRelation::build(&[("k", None)], 300),
            FakeRelation::build(&[("k", None)], 700),
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
            FakeRelation::build(&[("custkey", Some(150_000))], 1_500_000),
            FakeRelation::build(&[("custkey", Some(150_000))], 150_000),
            &[(0, 0)],
        )
        .expect("statistics are usable");
        assert_eq!(fact_rows * dimension_rows * with_ndv, 1_500_000.0);

        // Without them the fact table's foreign key is assumed unique, which
        // inflates its NDV to the row count and wins the `max`.
        let without_ndv = selectivity_of(
            FakeRelation::build(&[("custkey", None)], 1_500_000),
            FakeRelation::build(&[("custkey", None)], 150_000),
            &[(0, 0)],
        )
        .expect("statistics are usable");
        assert_eq!(fact_rows * dimension_rows * without_ndv, 150_000.0);
    }

    #[test]
    fn composite_keys_take_the_most_selective_rather_than_the_product() {
        // Denominators 100 and 40: the product would be 4000, wildly
        // overstating selectivity for keys that are usually correlated.
        let selectivity = selectivity_of(
            FakeRelation::build(&[("a", Some(100)), ("b", Some(40))], 1_000),
            FakeRelation::build(&[("a", Some(20)), ("b", Some(10))], 1_000),
            &[(0, 0), (1, 1)],
        );

        assert_eq!(selectivity, Some(1.0 / 100.0));
    }

    #[test]
    fn declines_when_a_row_count_is_missing() {
        let plan = join(
            FakeRelation::build(&[("k", Some(10))], 100),
            FakeRelation::without_row_count(&["k"]),
            &[(0, 0)],
        );
        let graph = JoinGraph::try_new(&plan).expect("a reorderable clump");

        assert!(GraphStatistics::try_new(&graph, None).is_none());
    }

    #[test]
    fn an_empty_relation_does_not_produce_an_infinite_selectivity() {
        let selectivity = selectivity_of(
            FakeRelation::build(&[("k", Some(0))], 0),
            FakeRelation::build(&[("k", Some(0))], 0),
            &[(0, 0)],
        )
        .expect("zero rows is a usable estimate");

        assert!(selectivity.is_finite());
        assert_eq!(selectivity, 1.0);
    }

    #[test]
    fn builds_a_double_star_from_a_bowtie() {
        //   a1        b1
        //    \        /
        //    hub_a - c - hub_b
        let hub_a = FakeRelation::build(&[("k", Some(1_000)), ("sa", Some(500))], 1_000);
        let a1 = FakeRelation::build(&[("k", Some(50))], 50);
        let central = FakeRelation::build(&[("ka", Some(200)), ("kb", Some(200))], 200);
        let hub_b = FakeRelation::build(&[("k", Some(2_000)), ("sb", Some(800))], 2_000);
        let b1 = FakeRelation::build(&[("k", Some(80))], 80);

        let left = join(hub_a, a1, &[(1, 0)]);
        let left = join(left, central, &[(0, 0)]);
        let right = join(hub_b, b1, &[(1, 0)]);
        let plan = join(left, right, &[(4, 0)]);

        let graph = JoinGraph::try_new(&plan).expect("a reorderable clump");
        let shapes = graph.detect_double_stars();
        assert_eq!(shapes.len(), 1);

        let statistics =
            GraphStatistics::try_new(&graph, None).expect("statistics are usable");
        let star = statistics
            .double_star(&graph, &shapes[0])
            .expect("every edge of the shape exists");

        assert_eq!(star.hub_a.weight, 1_000.0);
        assert_eq!(star.hub_b.weight, 2_000.0);
        assert_eq!(star.central.weight, 200.0);
        // hub_a.k has 1000 distinct values against central.ka's 200.
        assert_eq!(star.sel_a, 1.0 / 1_000.0);
        // hub_b.k has 2000 against central.kb's 200.
        assert_eq!(star.sel_b, 1.0 / 2_000.0);

        assert_eq!(star.spokes_a.len(), 1);
        assert_eq!(star.spokes_a[0].weight, 50.0);
        // hub_a.sa has 500 distinct values against a1.k's 50.
        assert_eq!(star.spokes_a[0].selectivity, 1.0 / 500.0);

        assert_eq!(star.spokes_b.len(), 1);
        assert_eq!(star.spokes_b[0].weight, 80.0);
        assert_eq!(star.spokes_b[0].selectivity, 1.0 / 800.0);

        // Ids index straight back into the graph's relations.
        assert_eq!(star.spokes_a[0].id, shapes[0].spokes_a[0]);
    }
}
