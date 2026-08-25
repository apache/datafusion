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

//! Cost-based reordering of helix join graphs.
//!
//! A helix is a chain of diamonds: consecutive relations linked by two
//! parallel paths rather than one.
//!
//! ```text
//!      a0        a1        a2
//!     /  \      /  \      /  \
//!   p0    p1  p1    p2  p2    p3
//!     \  /      \  /      \  /
//!      b0        b1        b2
//! ```
//!
//! It shows up wherever two entities are related through more than one path at
//! once — a shipment linked to a route by both its origin and its carrier
//! leg, say — repeated down a chain.
//!
//! # Why this shape needs its own rule
//!
//! A helix is not a tree. `p0 → a0 → p1 → b0 → p0` is a cycle, so join
//! reordering rules that require a tree decline it outright, and no ordering
//! heuristic built around one hub and its spokes has anything to say about it.
//! The search here is exhaustive instead: every bushy order of the graph is
//! priced and the cheapest wins. That is affordable only because a helix is
//! small — see [`MAX_RELATIONS`].
//!
//! # The pieces
//!
//! Each stage is independently testable, and the split is along what each one
//! is allowed to know:
//!
//! * [`join_graph`] flattens a tree of [`HashJoinExec`]s into relations and
//!   edges, recognizes the helix, and records the order the joins were already
//!   in. Knows plan structure, no numbers.
//! * [`statistics`] turns cardinalities and distinct counts into weights and
//!   selectivities. Knows numbers.
//! * [`cost_model`] searches every join order and picks the cheapest. Knows
//!   arithmetic, and nothing about [`ExecutionPlan`]s at all.
//! * [`rewrite`] emits the chosen order, remapping every column index.
//!
//! # When this rule declines
//!
//! Most queries are not helixes, so declining is the common case and every
//! stage returns `None` rather than guessing: a join type that is not
//! reorderable, a graph that is not a chain of diamonds, absent statistics, a
//! graph too large to search. Each records a `debug` log line naming the
//! reason, since "the plan did not change" is otherwise hard to diagnose.
//!
//! It also declines when the cheapest order it finds is the one the plan is
//! already in, rather than replacing the plan with a copy of itself.
//!
//! Disabled by default; enable with
//! `datafusion.optimizer.helix_join_reorder`.
//!
//! [`ExecutionPlan`]: datafusion_physical_plan::ExecutionPlan
//! [`HashJoinExec`]: datafusion_physical_plan::joins::HashJoinExec
//! [`MAX_RELATIONS`]: cost_model::MAX_RELATIONS

pub mod cost_model;
pub mod join_graph;
pub mod rewrite;
pub mod statistics;
#[cfg(test)]
mod test_support;

use std::sync::Arc;

use crate::PhysicalOptimizerRule;
use crate::optimizer::{ConfigOnlyContext, PhysicalOptimizerContext};
use cost_model::optimal_join_order;
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

/// Reorders helix join graphs using statistics-driven cost estimates.
///
/// See the [module documentation](self) for the shape this recognizes and the
/// stages involved.
#[derive(Default, Debug)]
pub struct HelixJoinReorder {}

impl HelixJoinReorder {
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

        let Some(shape) = graph.detect_helix() else {
            // Report the size too: the commonest reason a helix is not found
            // is a clump split by an intervening operator, leaving too few
            // relations to form one at all.
            log::debug!(
                "helix: {} relations and {} edges are not a chain of diamonds",
                graph.relations().len(),
                graph.edges().len()
            );
            return Ok(Transformed::no(plan));
        };

        let Some(statistics) = GraphStatistics::try_new(&graph, registry) else {
            return Ok(Transformed::no(plan));
        };
        let Some(query_graph) = statistics.query_graph(&graph) else {
            log::debug!(
                "helix: {} diamonds is more than the search can afford",
                shape.diamonds()
            );
            return Ok(Transformed::no(plan));
        };

        let Some(chosen) = optimal_join_order(&query_graph) else {
            log::debug!("helix: no join order of the graph had a finite cost");
            return Ok(Transformed::no(plan));
        };

        // Rebuilding the order the plan is already in would replace it with a
        // copy of itself and report that as a change. Declining also leaves any
        // projection the clump was flattened through where it was, rather than
        // quietly fusing it into the top join for no gain.
        if chosen.tree == *graph.input_tree() {
            log::debug!(
                "helix: the plan is already in the cheapest order of {} relations",
                graph.relations().len()
            );
            return Ok(Transformed::no(plan));
        }

        // Recurse into the leaves before rebuilding, since the traversal will
        // not descend into what we emit.
        let graph = graph.map_relations(|relation| {
            self.optimize_with_context(Arc::clone(relation), context)
        })?;

        let Some(rewritten) = rebuild(&graph, &chosen.tree) else {
            return Ok(Transformed::no(plan));
        };
        let Some(reordered) = apply_projection(rewritten) else {
            return Ok(Transformed::no(plan));
        };

        log::debug!(
            "helix: reordered {} diamonds over {} relations, estimated cost {}",
            shape.diamonds(),
            graph.relations().len(),
            chosen.cost
        );

        // Jump: the emitted subtree is already optimal, and descending into it
        // would rediscover the same clump.
        Ok(Transformed::new(reordered, true, TreeNodeRecursion::Jump))
    }
}

impl PhysicalOptimizerRule for HelixJoinReorder {
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
        if !config.optimizer.helix_join_reorder {
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
        "helix_join_reorder"
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

    use crate::helix_join_reorder::test_support::{
        col, diamond_from, helix, join, relation, relation_without_row_count, scan,
        typed_join,
    };
    use datafusion_common::JoinType;
    use datafusion_physical_plan::displayable;

    fn config(enabled: bool) -> ConfigOptions {
        let mut config = ConfigOptions::default();
        config.optimizer.helix_join_reorder = enabled;
        config
    }

    fn display(plan: &Arc<dyn ExecutionPlan>) -> String {
        displayable(plan.as_ref()).indent(false).to_string()
    }

    fn optimize(plan: &Arc<dyn ExecutionPlan>, enabled: bool) -> Arc<dyn ExecutionPlan> {
        HelixJoinReorder::new()
            .optimize(Arc::clone(plan), &config(enabled))
            .expect("the rule never fails")
    }

    #[test]
    fn does_nothing_when_disabled() {
        let plan = helix();
        let optimized = optimize(&plan, false);

        assert!(Arc::ptr_eq(&plan, &optimized), "expected the same plan");
    }

    #[test]
    fn reorders_a_helix() {
        let plan = helix();
        let optimized = optimize(&plan, true);

        assert_ne!(display(&plan), display(&optimized), "expected a reorder");
        // The invariant `schema_check` will also assert in the planner.
        assert_eq!(plan.schema(), optimized.schema());
    }

    #[test]
    fn reorders_a_single_diamond() {
        // The smallest helix, and a cycle: a rule that required a tree could
        // not touch this at all.
        let plan = test_support::measured_diamond();
        let optimized = optimize(&plan, true);

        assert_ne!(display(&plan), display(&optimized), "expected a reorder");
        assert_eq!(plan.schema(), optimized.schema());
    }

    #[test]
    fn leaves_a_bowtie_alone() {
        // The double star shape: a tree, so not a helix however the numbers
        // fall out.
        let hub_a = scan(&["ha_k", "ha_s1", "ha_s2"]);
        let plan = join(hub_a, scan(&["a1_k"]), &[(1, 0)]);
        let plan = join(plan, scan(&["a2_k"]), &[(2, 0)]);
        let plan = join(plan, scan(&["c_ka", "c_kb"]), &[(0, 0)]);
        let right = join(scan(&["hb_k", "hb_s"]), scan(&["b1_k"]), &[(1, 0)]);
        let plan = join(plan, right, &[(6, 0)]);

        assert_eq!(display(&plan), display(&optimize(&plan, true)));
    }

    #[test]
    fn leaves_a_chain_alone() {
        // Five relations in a line: connected, small, and orderable in
        // principle, but not a chain of diamonds, so this rule declines it.
        let plan = join(scan(&["a", "ka"]), scan(&["b", "kb"]), &[(1, 0)]);
        let plan = join(plan, scan(&["c", "kc"]), &[(3, 0)]);
        let plan = join(plan, scan(&["d", "kd"]), &[(5, 0)]);
        let plan = join(plan, scan(&["e"]), &[(7, 0)]);

        assert_eq!(display(&plan), display(&optimize(&plan, true)));
    }

    #[test]
    fn leaves_an_outer_join_alone() {
        // The clump root is a left join, which is not reorderable, and the
        // inner joins beneath it are too few to form a helix.
        let left = join(scan(&["k", "s"]), scan(&["a"]), &[(1, 0)]);
        let plan = typed_join(left, scan(&["k"]), &[(0, 0)], JoinType::Left);

        assert_eq!(display(&plan), display(&optimize(&plan, true)));
    }

    #[test]
    fn declines_when_a_relation_has_no_row_count() {
        // The shape matches and every other relation is measured, but one
        // missing row count is enough: the rule refuses rather than filling in
        // a default it cannot distinguish from a real measurement.
        let plan = diamond_from(
            relation(vec![col("p0_ka"), col("p0_kb"), col("p0_x")], 1_000),
            relation(vec![col("a0_p0"), col("a0_p1")], 50),
            relation_without_row_count(&["b0_p0", "b0_p1", "b0_y", "b0_z"]),
            relation(
                vec![
                    col("p1_a"),
                    col("p1_b"),
                    col("p1_c"),
                    col("p1_d"),
                    col("p1_e"),
                ],
                5_000,
            ),
        );

        assert_eq!(display(&plan), display(&optimize(&plan, true)));
    }

    #[test]
    fn keeps_a_plan_that_is_already_in_the_cheapest_order() {
        // The same diamond built right deep. `scan` relations report zero
        // rows, so every order costs the same and the tie break decides:
        // the lowest relation splits off on its own, then the next, which is
        // exactly this plan. The rule must hand back the very same plan rather
        // than an equal copy of it, which is the observable difference between
        // declining and rebuilding.
        let p0 = scan(&["p0_ka", "p0_kb", "p0_x"]);
        let a0 = scan(&["a0_p0", "a0_p1"]);
        let b0 = scan(&["b0_p0", "b0_p1", "b0_y", "b0_z"]);
        let p1 = scan(&["p1_a", "p1_b", "p1_c", "p1_d", "p1_e"]);

        // b0.b0_p1 = p1.p1_b
        let inner = join(b0, p1, &[(1, 1)]);
        // a0.a0_p1 = p1.p1_a, which sits at index 4 of `inner`.
        let inner = join(a0, inner, &[(1, 4)]);
        // p0 reaches both links at once: a0 at index 0, b0 at index 2.
        let plan = join(p0, inner, &[(0, 0), (1, 2)]);

        let graph = JoinGraph::try_new(&plan).expect("a reorderable clump");
        assert!(graph.detect_helix().is_some(), "expected a helix");

        let optimized = optimize(&plan, true);

        assert!(Arc::ptr_eq(&plan, &optimized), "expected the same plan");
    }

    #[test]
    fn is_idempotent() {
        let once = optimize(&helix(), true);
        let twice = optimize(&once, true);

        assert_eq!(display(&once), display(&twice));
    }
}
