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

//! Cost-based reordering of "double star" (bowtie) join graphs.
//!
//! A double star is two hub relations, each surrounded by its own set of
//! single-edge spoke relations, linked to one another through a shared
//! central relation:
//!
//! ```text
//!    a1  a2  a3              b1  b2
//!      \  |  /                 \  /
//!       \ | /                   \/
//!        hubA ----- c ------- hubB
//!                (central)
//! ```
//!
//! This shape shows up in warehouse workloads whenever two fact tables are
//! joined through a bridge table and each brings its own dimension tables.
//!
//! # The pieces
//!
//! Each stage is independently testable, and the split is along what each one
//! is allowed to know:
//!
//! * [`join_graph`] flattens a tree of [`HashJoinExec`]s into relations and
//!   edges, and recognizes the shape. Knows plan structure, no numbers.
//! * [`statistics`] turns cardinalities and distinct counts into weights and
//!   selectivities. Knows numbers.
//! * [`cost_model`] picks the cheapest order. Knows arithmetic, and nothing
//!   about [`ExecutionPlan`]s at all.
//! * [`rewrite`] emits the chosen order, remapping every column index.
//!
//! # When this rule declines
//!
//! Most queries are not double stars, so declining is the common case and
//! every stage above returns `None` rather than guessing: a join type that is
//! not reorderable, a graph that is not a bowtie, absent statistics. Each
//! records a `debug` log line naming the reason, since "the plan did not
//! change" is otherwise hard to diagnose.
//!
//! Disabled by default; enable with
//! `datafusion.optimizer.double_star_join_reorder`.
//!
//! [`ExecutionPlan`]: datafusion_physical_plan::ExecutionPlan
//! [`HashJoinExec`]: datafusion_physical_plan::joins::HashJoinExec

pub mod cost_model;
pub mod join_graph;
pub mod rewrite;
pub mod statistics;
#[cfg(test)]
mod test_support;

use std::sync::Arc;

use crate::PhysicalOptimizerRule;
use crate::optimizer::{ConfigOnlyContext, PhysicalOptimizerContext};
use cost_model::{DoubleStarPlan, optimal_double_star};
use datafusion_common::Result;
use datafusion_common::config::ConfigOptions;
use datafusion_common::tree_node::{
    Transformed, TransformedResult, TreeNode, TreeNodeRecursion,
};
use datafusion_physical_plan::ExecutionPlan;
use datafusion_physical_plan::operator_statistics::StatisticsRegistry;
use join_graph::JoinGraph;
use rewrite::{apply_projection, rebuild};
use statistics::GraphStatistics;

/// Reorders double star join graphs using statistics-driven cost estimates.
///
/// See the [module documentation](self) for the shape this recognizes and the
/// stages involved.
#[derive(Default, Debug)]
pub struct DoubleStarJoinReorder {}

impl DoubleStarJoinReorder {
    #[expect(missing_docs)]
    pub fn new() -> Self {
        Self {}
    }

    /// Reorder the clump rooted at `plan`, or return it unchanged.
    ///
    /// Leaf relations are optimized first and substituted back in. That is
    /// sound because this rule preserves schemas, so an optimized leaf has the
    /// same width and column order and the graph's offsets stay valid. It is
    /// also necessary: the traversal jumps over a rewritten subtree, so a join
    /// tree hidden beneath a leaf would otherwise never be visited. Leaves are
    /// commonly `FilterExec` nodes here, because this rule runs before
    /// `FilterPushdown`.
    fn reorder_clump(
        &self,
        plan: Arc<dyn ExecutionPlan>,
        context: &dyn PhysicalOptimizerContext,
        registry: Option<&StatisticsRegistry>,
    ) -> Result<Transformed<Arc<dyn ExecutionPlan>>> {
        let Some(graph) = JoinGraph::try_new(&plan) else {
            return Ok(Transformed::no(plan));
        };

        let shapes = graph.detect_double_stars();
        if shapes.is_empty() {
            // Report the size too: the commonest reason for no decomposition
            // is a clump split by an intervening operator, leaving too few
            // relations to form a double star at all.
            log::debug!(
                "double star: no valid decomposition of {} relations and {} edges",
                graph.relations().len(),
                graph.edges().len()
            );
            return Ok(Transformed::no(plan));
        }

        let Some(statistics) = GraphStatistics::try_new(&graph, registry) else {
            return Ok(Transformed::no(plan));
        };

        // Statistics are properties of relations and edges, so they are shared
        // across candidate readings of the graph; only the arrangement differs.
        let Some(chosen) = shapes
            .iter()
            .filter_map(|shape| statistics.double_star(&graph, shape))
            .filter_map(|star| optimal_double_star(&star))
            .reduce(cheaper)
        else {
            log::debug!("double star: no candidate order had a finite cost");
            return Ok(Transformed::no(plan));
        };

        // Recurse into the leaves before rebuilding, since the traversal will
        // not descend into what we emit.
        let graph = graph.map_relations(|relation| {
            self.optimize_with_context(Arc::clone(relation), context)
        })?;

        let Some(rewritten) = rebuild(&graph, &chosen) else {
            return Ok(Transformed::no(plan));
        };
        let Some(reordered) = apply_projection(rewritten) else {
            return Ok(Transformed::no(plan));
        };

        log::debug!(
            "double star: reordered {} relations, estimated cost {}",
            graph.relations().len(),
            chosen.cost
        );

        // Jump: the emitted subtree is already optimal, and descending into it
        // would rediscover the same clump.
        Ok(Transformed::new(reordered, true, TreeNodeRecursion::Jump))
    }
}

/// Keep the cheaper order, preferring the earlier candidate on a tie.
///
/// Shapes arrive ordered by central relation, so ties resolve to the lowest
/// one. Determinism matters because the resulting plan is compared against
/// committed expected output.
fn cheaper(best: DoubleStarPlan, candidate: DoubleStarPlan) -> DoubleStarPlan {
    if candidate.cost < best.cost {
        candidate
    } else {
        best
    }
}

impl PhysicalOptimizerRule for DoubleStarJoinReorder {
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
        if !config.optimizer.double_star_join_reorder {
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

        // Top down, so the largest clump is found first. Starting at the
        // bottom would rewrite an inner clump, whose projection would then
        // make it an opaque leaf and split the larger clump above it.
        plan.transform_down(|node| self.reorder_clump(node, context, registry))
            .data()
    }

    fn name(&self) -> &str {
        "double_star_join_reorder"
    }

    /// The rewrite restores the original column order through the top join's
    /// projection, so the schema is unchanged. Opting into the check turns
    /// that invariant into an assertion on every query, not just in tests.
    fn schema_check(&self) -> bool {
        true
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use crate::double_star_join_reorder::test_support::{
        bowtie, bowtie_from, col, join, relation, relation_without_row_count, scan,
        typed_join,
    };
    use datafusion_common::JoinType;
    use datafusion_physical_plan::displayable;

    fn config(enabled: bool) -> ConfigOptions {
        let mut config = ConfigOptions::default();
        config.optimizer.double_star_join_reorder = enabled;
        config
    }

    fn display(plan: &Arc<dyn ExecutionPlan>) -> String {
        displayable(plan.as_ref()).indent(false).to_string()
    }

    #[test]
    fn does_nothing_when_disabled() {
        let plan = bowtie();
        let optimized = DoubleStarJoinReorder::new()
            .optimize(Arc::clone(&plan), &config(false))
            .expect("the rule never fails");

        assert!(Arc::ptr_eq(&plan, &optimized), "expected the same plan");
    }

    #[test]
    fn reorders_when_enabled() {
        let plan = bowtie();
        let optimized = DoubleStarJoinReorder::new()
            .optimize(Arc::clone(&plan), &config(true))
            .expect("the rule never fails");

        assert_ne!(display(&plan), display(&optimized), "expected a reorder");
        // The invariant `schema_check` will also assert in the planner.
        assert_eq!(plan.schema(), optimized.schema());
    }

    #[test]
    fn leaves_a_non_bowtie_alone() {
        // A single star: one hub with three spokes has no bridging relation.
        let hub = scan(&["k", "s1", "s2", "s3"]);
        let plan = join(hub, scan(&["a"]), &[(1, 0)]);
        let plan = join(plan, scan(&["b"]), &[(2, 0)]);
        let plan = join(plan, scan(&["c"]), &[(3, 0)]);

        let optimized = DoubleStarJoinReorder::new()
            .optimize(Arc::clone(&plan), &config(true))
            .expect("the rule never fails");

        assert_eq!(display(&plan), display(&optimized));
    }

    #[test]
    fn leaves_an_outer_join_alone() {
        // The clump root is a left join, which is not reorderable, and the
        // inner joins beneath it are too few to form a double star.
        let left = join(scan(&["k", "s"]), scan(&["a"]), &[(1, 0)]);
        let plan = typed_join(left, scan(&["k"]), &[(0, 0)], JoinType::Left);

        let optimized = DoubleStarJoinReorder::new()
            .optimize(Arc::clone(&plan), &config(true))
            .expect("the rule never fails");

        assert_eq!(display(&plan), display(&optimized));
    }

    #[test]
    fn declines_when_a_relation_has_no_row_count() {
        // The shape matches and every other relation is measured, but one
        // missing row count is enough: the rule refuses rather than filling in
        // a default it cannot distinguish from a real measurement.
        let plan = bowtie_from(
            relation(vec![col("ha_k"), col("ha_s1"), col("ha_s2")], 100),
            relation(vec![col("a1_k")], 10),
            relation(vec![col("a2_k"), col("a2_x"), col("a2_y"), col("a2_z")], 20),
            relation_without_row_count(&["c_ka", "c_kb"]),
            relation(
                vec![
                    col("hb_k"),
                    col("hb_s1"),
                    col("hb_p"),
                    col("hb_q"),
                    col("hb_r"),
                ],
                200,
            ),
            relation(vec![col("b1_k"), col("b1_x")], 30),
        );

        let optimized = DoubleStarJoinReorder::new()
            .optimize(Arc::clone(&plan), &config(true))
            .expect("the rule never fails");

        assert_eq!(display(&plan), display(&optimized));
    }

    #[test]
    fn reorders_a_measured_bowtie() {
        // The counterpart: with every relation measured, the same shape is
        // reordered and the schema survives.
        let plan = bowtie_from(
            relation(vec![col("ha_k"), col("ha_s1"), col("ha_s2")], 100),
            relation(vec![col("a1_k")], 10),
            relation(vec![col("a2_k"), col("a2_x"), col("a2_y"), col("a2_z")], 20),
            relation(vec![col("c_ka"), col("c_kb")], 400),
            relation(
                vec![
                    col("hb_k"),
                    col("hb_s1"),
                    col("hb_p"),
                    col("hb_q"),
                    col("hb_r"),
                ],
                5_000,
            ),
            relation(vec![col("b1_k"), col("b1_x")], 30),
        );

        let optimized = DoubleStarJoinReorder::new()
            .optimize(Arc::clone(&plan), &config(true))
            .expect("the rule never fails");

        assert_ne!(display(&plan), display(&optimized));
        assert_eq!(plan.schema(), optimized.schema());
    }

    #[test]
    fn is_idempotent() {
        let rule = DoubleStarJoinReorder::new();
        let once = rule
            .optimize(bowtie(), &config(true))
            .expect("the rule never fails");
        let twice = rule
            .optimize(Arc::clone(&once), &config(true))
            .expect("the rule never fails");

        assert_eq!(display(&once), display(&twice));
    }
}
