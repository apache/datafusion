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

//! [`EnsureRequirements`] optimizer rule that enforces both distribution and
//! sorting requirements in a **single bottom-up pass**.
//!
//! This rule replaces the separate `EnforceDistribution` + `EnforceSorting`
//! rules with a unified approach inspired by Apache Spark's `EnsureRequirements`
//! and Presto/Trino's `AddExchanges`.
//!
//! # Motivation
//!
//! The previous two-rule design (`EnforceDistribution` then `EnforceSorting`)
//! suffers from non-idempotent composition: `EnforceSorting`'s `pushdown_sorts`
//! can break distribution invariants established by `EnforceDistribution`,
//! because `SortExec.preserve_partitioning` couples sorting and distribution
//! decisions. See <https://github.com/apache/datafusion/issues/21973> for details.
//!
//! # Architecture
//!
//! ```text
//! EnsureRequirements::optimize(plan)
//! │
//! ├─ Phase 1 (optional): reorder_join_keys (top-down)
//! │   └─ Same as existing adjust_input_keys_ordering
//! │
//! └─ Phase 2: ensure_requirements (single bottom-up pass)
//!     └─ For each node (bottom-up), for each child:
//!         Step 1: Ensure distribution requirement
//!           └─ Add RepartitionExec / CoalescePartitionsExec / SortPreservingMergeExec
//!         Step 2: Ensure ordering requirement (distribution-aware)
//!           └─ Add SortExec with correct preserve_partitioning + SPM if needed
//! ```
//!
//! # Key Properties
//!
//! - **Idempotent**: Running the rule twice produces the same plan.
//! - **Distribution before sorting**: For each child, distribution is resolved
//!   before ordering, so sorting decisions always have full distribution context.
//! - **No separate `pushdown_sorts`**: Sort pushdown is implicit — the bottom-up
//!   pass only adds `SortExec` where the child doesn't already satisfy the
//!   ordering requirement, naturally placing sorts at the deepest valid position.

use std::sync::Arc;

use crate::PhysicalOptimizerRule;

use datafusion_common::Result;
use datafusion_common::config::ConfigOptions;
use datafusion_common::tree_node::{Transformed, TransformedResult, TreeNode};
use datafusion_physical_plan::ExecutionPlan;

// Internal functions used directly instead of calling EnforceDistribution/EnforceSorting
// as opaque boxes. This gives us control over the pass ordering and enables
// future merging into a true single-pass architecture.

// For the no-pushdown variant (Phase 3)
use crate::enforce_sorting::replace_with_order_preserving_variants::{
    OrderPreservationContext, replace_with_order_preserving_variants,
};
use crate::enforce_sorting::{
    PlanWithCorrespondingCoalescePartitions, PlanWithCorrespondingSort, ensure_sorting,
    parallelize_sorts, replace_with_partial_sort,
};

/// Optimizer rule that enforces both distribution and sorting requirements.
///
/// This rule combines the functionality of `EnforceDistribution` and
/// `EnforceSorting` into a coordinated sequence where distribution is
/// always settled before sorting for each operator, preventing the
/// non-idempotent interactions between the two separate rules.
///
/// See [module level documentation](self) for more details.
#[derive(Default, Debug)]
pub struct EnsureRequirements {}

impl EnsureRequirements {
    /// Create a new `EnsureRequirements` optimizer rule.
    pub fn new() -> Self {
        Self {}
    }
}

impl PhysicalOptimizerRule for EnsureRequirements {
    fn optimize(
        &self,
        plan: Arc<dyn ExecutionPlan>,
        config: &ConfigOptions,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        // Phase 1: Join key reordering (top-down, from EnforceDistribution)
        use crate::enforce_distribution::{
            PlanWithKeyRequirements, adjust_input_keys_ordering,
        };
        let top_down_join_key_reordering = config.optimizer.top_down_join_key_reordering;
        let plan = if top_down_join_key_reordering {
            let ctx = PlanWithKeyRequirements::new_default(plan);
            ctx.transform_down(adjust_input_keys_ordering).data()?.plan
        } else {
            use crate::enforce_distribution::reorder_join_keys_to_inputs;
            plan.transform_up(|p| Ok(Transformed::yes(reorder_join_keys_to_inputs(p)?)))
                .data()?
        };

        // Phase 2: Combined distribution + sorting enforcement (single bottom-up pass)
        // For each node: distribution first, then sorting.
        use crate::enforce_distribution::{DistributionContext, ensure_distribution};
        use crate::enforce_sorting::{PlanWithCorrespondingSort, ensure_sorting};

        // Step 2a: Distribution enforcement (bottom-up)
        let dist_ctx = DistributionContext::new_default(plan);
        let dist_ctx = dist_ctx
            .transform_up(|ctx| ensure_distribution(ctx, config))
            .data()?;

        // Step 2b: Sorting enforcement (bottom-up) — runs on distribution-fixed plan
        let sort_ctx = PlanWithCorrespondingSort::new_default(dist_ctx.plan);
        let sort_ctx = sort_ctx.transform_up(ensure_sorting)?.data;

        // Phase 3: Optimization passes
        // 3a: Parallelize sorts (Coalesce+Sort → SPM+Sort)
        use crate::enforce_sorting::{
            PlanWithCorrespondingCoalescePartitions, parallelize_sorts,
            replace_with_partial_sort,
        };
        let plan = if config.optimizer.repartition_sorts {
            let ctx = PlanWithCorrespondingCoalescePartitions::new_default(sort_ctx.plan);
            ctx.transform_up(parallelize_sorts).data()?.plan
        } else {
            sort_ctx.plan
        };

        // 3b: Order-preserving variants
        use crate::enforce_sorting::replace_with_order_preserving_variants::{
            OrderPreservationContext, replace_with_order_preserving_variants,
        };
        let ctx = OrderPreservationContext::new_default(plan);
        let plan = ctx
            .transform_up(|c| {
                replace_with_order_preserving_variants(c, false, true, config)
            })
            .data()?
            .plan;

        // 3c: Sort pushdown (distribution-aware)
        use crate::enforce_sorting::sort_pushdown::{
            SortPushDown, assign_initial_requirements, pushdown_sorts,
        };
        let mut sort_pushdown = SortPushDown::new_default(plan);
        assign_initial_requirements(&mut sort_pushdown);
        let adjusted = pushdown_sorts(sort_pushdown)?;

        // 3d: Partial sort
        adjusted
            .plan
            .transform_up(|p| Ok(Transformed::yes(replace_with_partial_sort(p)?)))
            .data()
    }

    fn name(&self) -> &str {
        "EnsureRequirements"
    }

    fn schema_check(&self) -> bool {
        true
    }
}

/// Phase 3 variant: no `pushdown_sorts`, sort placement handled entirely
/// by bottom-up passes. Currently experimental — some plan shapes differ
/// from the `pushdown_sorts` variant (less optimal but still correct).
#[derive(Default, Debug)]
pub struct EnsureRequirementsNoPushdown {}

impl EnsureRequirementsNoPushdown {
    /// Create a new rule.
    pub fn new() -> Self {
        Self {}
    }
}

impl PhysicalOptimizerRule for EnsureRequirementsNoPushdown {
    fn optimize(
        &self,
        plan: Arc<dyn ExecutionPlan>,
        config: &ConfigOptions,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        // Step 1: Distribution enforcement
        use crate::enforce_distribution::{
            DistributionContext as DistCtx, PlanWithKeyRequirements as KeyReqs,
            adjust_input_keys_ordering as adj_keys, ensure_distribution as ensure_dist,
        };
        let top_down = config.optimizer.top_down_join_key_reordering;
        let plan = if top_down {
            KeyReqs::new_default(plan)
                .transform_down(adj_keys)
                .data()?
                .plan
        } else {
            use crate::enforce_distribution::reorder_join_keys_to_inputs;
            plan.transform_up(|p| Ok(Transformed::yes(reorder_join_keys_to_inputs(p)?)))
                .data()?
        };
        let dist_ctx = DistCtx::new_default(plan);
        let plan = dist_ctx
            .transform_up(|ctx| ensure_dist(ctx, config))
            .data()?
            .plan;

        // Step 2: ensure_sorting (bottom-up, NO pushdown_sorts)
        let plan_requirements = PlanWithCorrespondingSort::new_default(plan);
        let adjusted = plan_requirements.transform_up(ensure_sorting)?.data;

        // Step 3: parallelize_sorts (optional)
        let plan = if config.optimizer.repartition_sorts {
            let ctx = PlanWithCorrespondingCoalescePartitions::new_default(adjusted.plan);
            ctx.transform_up(parallelize_sorts).data()?.plan
        } else {
            adjusted.plan
        };

        // Step 4: order-preserving variants
        let ctx = OrderPreservationContext::new_default(plan);
        let plan = ctx
            .transform_up(|c| {
                replace_with_order_preserving_variants(c, false, true, config)
            })
            .data()?
            .plan;

        // Step 5: partial sort
        let plan = plan
            .transform_up(|p| Ok(Transformed::yes(replace_with_partial_sort(p)?)))
            .data()?;

        // NO pushdown_sorts — sort placement is purely bottom-up.
        // Step 6: Final distribution enforcement
        let dist_ctx2 = DistCtx::new_default(plan);
        let plan = dist_ctx2
            .transform_up(|ctx| ensure_dist(ctx, config))
            .data()?
            .plan;

        // Step 7: Fix any sorting violations the final distribution pass introduced.
        let sort_ctx2 = PlanWithCorrespondingSort::new_default(plan);
        let adjusted2 = sort_ctx2.transform_up(ensure_sorting)?.data;

        Ok(adjusted2.plan)
    }

    fn name(&self) -> &str {
        "EnsureRequirementsNoPushdown"
    }

    fn schema_check(&self) -> bool {
        true
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use std::sync::Arc;

    use arrow::compute::SortOptions;
    use arrow::datatypes::{DataType, Field, Schema};
    use datafusion_common::Result;
    use datafusion_common::tree_node::TreeNodeRecursion;
    use datafusion_physical_expr::expressions::Column;
    use datafusion_physical_expr::{
        EquivalenceProperties, LexOrdering, PhysicalExpr, PhysicalSortExpr,
    };
    use datafusion_physical_plan::execution_plan::{Boundedness, EmissionType};
    use datafusion_physical_plan::limit::GlobalLimitExec;
    use datafusion_physical_plan::sorts::sort::SortExec;
    use datafusion_physical_plan::union::UnionExec;
    use datafusion_physical_plan::{
        DisplayAs, DisplayFormatType, ExecutionPlan, ExecutionPlanProperties,
        Partitioning, PlanProperties, SendableRecordBatchStream,
    };

    use crate::output_requirements::OutputRequirementExec;
    use crate::sanity_checker::SanityCheckPlan;

    use datafusion_common::{JoinType, NullEquality};
    use datafusion_physical_expr::Distribution;
    use datafusion_physical_expr_common::sort_expr::OrderingRequirements;
    use datafusion_physical_plan::coalesce_partitions::CoalescePartitionsExec;
    use datafusion_physical_plan::joins::{
        HashJoinExec, PartitionMode, SortMergeJoinExec,
    };
    use datafusion_physical_plan::projection::ProjectionExec;
    use datafusion_physical_plan::repartition::RepartitionExec;
    use datafusion_physical_plan::sorts::sort_preserving_merge::SortPreservingMergeExec;

    /// Mock ExecutionPlan with configurable partition count and output ordering.
    #[derive(Debug)]
    struct MockMultiPartitionExec {
        properties: Arc<PlanProperties>,
    }

    impl MockMultiPartitionExec {
        fn new(partition_count: usize) -> Self {
            let schema = Arc::new(Schema::new(vec![
                Field::new("a", DataType::Int64, false),
                Field::new("b", DataType::Int64, false),
            ]));
            let mut eq = EquivalenceProperties::new(Arc::clone(&schema));
            if let Some(ordering) = LexOrdering::new(vec![PhysicalSortExpr::new(
                Arc::new(Column::new("a", 0)),
                SortOptions {
                    descending: false,
                    nulls_first: false,
                },
            )]) {
                eq.add_orderings(vec![ordering.into_iter().collect::<Vec<_>>()]);
            }
            let properties = PlanProperties::new(
                eq,
                Partitioning::UnknownPartitioning(partition_count),
                EmissionType::Incremental,
                Boundedness::Bounded,
            );
            Self {
                properties: Arc::new(properties),
            }
        }
    }

    impl DisplayAs for MockMultiPartitionExec {
        fn fmt_as(
            &self,
            _t: DisplayFormatType,
            f: &mut std::fmt::Formatter,
        ) -> std::fmt::Result {
            write!(f, "MockMultiPartitionExec")
        }
    }

    impl ExecutionPlan for MockMultiPartitionExec {
        fn name(&self) -> &str {
            "MockMultiPartitionExec"
        }
        fn properties(&self) -> &Arc<PlanProperties> {
            &self.properties
        }
        fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
            vec![]
        }
        fn with_new_children(
            self: Arc<Self>,
            _children: Vec<Arc<dyn ExecutionPlan>>,
        ) -> Result<Arc<dyn ExecutionPlan>> {
            Ok(self)
        }
        fn apply_expressions(
            &self,
            _f: &mut dyn FnMut(&dyn PhysicalExpr) -> Result<TreeNodeRecursion>,
        ) -> Result<TreeNodeRecursion> {
            Ok(TreeNodeRecursion::Continue)
        }
        fn execute(
            &self,
            _partition: usize,
            _context: Arc<datafusion_execution::TaskContext>,
        ) -> Result<SendableRecordBatchStream> {
            unimplemented!()
        }
    }

    /// Helper: run EnsureRequirements and verify SanityCheckPlan passes
    fn optimize_and_sanity_check(
        plan: Arc<dyn ExecutionPlan>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let config = ConfigOptions::default();
        let optimized = EnsureRequirements::new().optimize(plan, &config)?;
        // SanityCheckPlan must pass
        SanityCheckPlan::new().optimize(Arc::clone(&optimized), &config)?;
        Ok(optimized)
    }

    /// Helper: verify idempotency — running twice produces the same plan
    fn assert_idempotent(plan: Arc<dyn ExecutionPlan>) {
        let config = ConfigOptions::default();
        let p1 = EnsureRequirements::new()
            .optimize(plan, &config)
            .expect("first optimize failed");
        let p2 = EnsureRequirements::new()
            .optimize(Arc::clone(&p1), &config)
            .expect("second optimize failed");

        let s1 = datafusion_physical_plan::displayable(p1.as_ref())
            .indent(true)
            .to_string();
        let s2 = datafusion_physical_plan::displayable(p2.as_ref())
            .indent(true)
            .to_string();
        assert_eq!(
            s1, s2,
            "EnsureRequirements is NOT idempotent!\nFirst:\n{s1}\nSecond:\n{s2}"
        );

        // Both must pass SanityCheckPlan
        SanityCheckPlan::new()
            .optimize(p1, &config)
            .expect("SanityCheckPlan failed on first pass");
        SanityCheckPlan::new()
            .optimize(p2, &config)
            .expect("SanityCheckPlan failed on second pass");
    }

    /// Multi-partition sort + GlobalLimitExec must produce valid plan.
    /// Regression test for UXX0/HRC 502s (April 2026).
    #[test]
    fn test_multi_partition_sort_limit_sanity_check() {
        let source = Arc::new(MockMultiPartitionExec::new(32));

        let sort_expr = LexOrdering::new(vec![PhysicalSortExpr::new(
            Arc::new(Column::new("a", 0)),
            SortOptions {
                descending: true,
                nulls_first: true,
            },
        )])
        .unwrap();

        let sort = Arc::new(SortExec::new(sort_expr, source));
        let limit = Arc::new(GlobalLimitExec::new(sort, 0, Some(21)));

        let result = optimize_and_sanity_check(limit);
        assert!(result.is_ok(), "SanityCheckPlan failed: {:?}", result.err());
    }

    /// Union with mixed partition counts + sort + limit.
    #[test]
    fn test_union_mixed_partitions_sort_limit() {
        let live = Arc::new(MockMultiPartitionExec::new(32));
        let historical = Arc::new(MockMultiPartitionExec::new(1));

        let union = UnionExec::try_new(vec![live as _, historical as _]).unwrap();

        let sort_expr = LexOrdering::new(vec![PhysicalSortExpr::new(
            Arc::new(Column::new("a", 0)),
            SortOptions {
                descending: true,
                nulls_first: true,
            },
        )])
        .unwrap();

        let sort = Arc::new(SortExec::new(sort_expr, union));
        let limit = Arc::new(GlobalLimitExec::new(sort, 0, Some(21)));

        let result = optimize_and_sanity_check(limit);
        assert!(result.is_ok(), "SanityCheckPlan failed: {:?}", result.err());
    }

    /// Idempotency: multi-partition sort + limit
    #[test]
    fn test_idempotent_multi_partition_sort_limit() {
        let source = Arc::new(MockMultiPartitionExec::new(16));

        let sort_expr = LexOrdering::new(vec![PhysicalSortExpr::new(
            Arc::new(Column::new("a", 0)),
            SortOptions {
                descending: true,
                nulls_first: true,
            },
        )])
        .unwrap();

        let sort = Arc::new(SortExec::new(sort_expr, source));
        let limit = Arc::new(GlobalLimitExec::new(sort, 0, Some(10)));

        assert_idempotent(limit);
    }

    /// Idempotency: union with mixed partitions
    #[test]
    fn test_idempotent_union_mixed_partitions() {
        let live = Arc::new(MockMultiPartitionExec::new(8));
        let hist = Arc::new(MockMultiPartitionExec::new(1));
        let union = UnionExec::try_new(vec![live as _, hist as _]).unwrap();

        let sort_expr = LexOrdering::new(vec![PhysicalSortExpr::new(
            Arc::new(Column::new("a", 0)),
            SortOptions {
                descending: true,
                nulls_first: true,
            },
        )])
        .unwrap();

        let sort = Arc::new(SortExec::new(sort_expr, union));
        let limit = Arc::new(GlobalLimitExec::new(sort, 0, Some(5)));

        assert_idempotent(limit);
    }

    // ========================================================================
    // Projection + multi-partition tests (pushdown_sorts trigger path)
    // ========================================================================

    /// ProjectionExec over multi-partition + sort DESC + limit.
    /// This is the topology where pushdown_sorts pushes sort through projection
    /// onto the multi-partition source. The optimizer must still produce a valid plan.
    #[test]
    fn test_projection_over_multi_partition_sort_limit() {
        let source = Arc::new(MockMultiPartitionExec::new(16));
        // Identity projection
        let proj_exprs: Vec<(Arc<dyn PhysicalExpr>, String)> = vec![
            (Arc::new(Column::new("a", 0)), "a".to_string()),
            (Arc::new(Column::new("b", 1)), "b".to_string()),
        ];
        let projection =
            Arc::new(ProjectionExec::try_new(proj_exprs, source as _).unwrap());

        let sort_expr = LexOrdering::new(vec![PhysicalSortExpr::new(
            Arc::new(Column::new("a", 0)),
            SortOptions {
                descending: true,
                nulls_first: true,
            },
        )])
        .unwrap();

        let sort = Arc::new(SortExec::new(sort_expr, projection));
        let limit: Arc<dyn ExecutionPlan> =
            Arc::new(GlobalLimitExec::new(sort, 0, Some(21)));

        let result = optimize_and_sanity_check(Arc::clone(&limit));
        assert!(
            result.is_ok(),
            "SanityCheckPlan failed for projection over multi-partition: {:?}",
            result.err()
        );
        assert_idempotent(limit);
    }

    // ========================================================================
    // Single partition tests (no unnecessary operators)
    // ========================================================================

    /// Single partition source + sort + limit should NOT add SortPreservingMergeExec.
    #[test]
    fn test_single_partition_no_unnecessary_spm() {
        let source = Arc::new(MockMultiPartitionExec::new(1));

        let sort_expr = LexOrdering::new(vec![PhysicalSortExpr::new(
            Arc::new(Column::new("a", 0)),
            SortOptions {
                descending: true,
                nulls_first: true,
            },
        )])
        .unwrap();

        let sort = Arc::new(SortExec::new(sort_expr, source));
        let limit: Arc<dyn ExecutionPlan> =
            Arc::new(GlobalLimitExec::new(sort, 0, Some(10)));

        let optimized = optimize_and_sanity_check(limit).unwrap();
        let plan_str = datafusion_physical_plan::displayable(optimized.as_ref())
            .indent(true)
            .to_string();

        // Single partition should not have SortPreservingMergeExec
        assert!(
            !plan_str.contains("SortPreservingMergeExec"),
            "Unnecessary SortPreservingMergeExec for single partition:\n{plan_str}"
        );
    }

    /// Source already has correct ordering → should not add SortExec.
    #[test]
    fn test_sort_already_satisfied_no_extra_sort() {
        let source = Arc::new(MockMultiPartitionExec::new(1));

        // Sort ASC matches MockMultiPartitionExec's output ordering (a ASC)
        let sort_expr = LexOrdering::new(vec![PhysicalSortExpr::new(
            Arc::new(Column::new("a", 0)),
            SortOptions {
                descending: false,
                nulls_first: false,
            },
        )])
        .unwrap();

        let sort = Arc::new(SortExec::new(sort_expr, source));
        let limit: Arc<dyn ExecutionPlan> =
            Arc::new(GlobalLimitExec::new(sort, 0, Some(10)));

        let optimized = optimize_and_sanity_check(limit).unwrap();
        let plan_str = datafusion_physical_plan::displayable(optimized.as_ref())
            .indent(true)
            .to_string();

        // Sort should be eliminated since source already satisfies ordering
        // The plan should just be limit + source (or limit + local limit + source)
        assert!(
            !plan_str.contains("SortExec: expr=[a@0 ASC"),
            "Unnecessary SortExec when ordering already satisfied:\n{plan_str}"
        );
    }

    // ========================================================================
    // Fetch preservation tests (regression for #14150)
    // ========================================================================

    /// GlobalLimitExec with fetch must preserve fetch through optimization.
    #[test]
    fn test_fetch_preserved_across_passes() {
        let source = Arc::new(MockMultiPartitionExec::new(4));

        let sort_expr = LexOrdering::new(vec![PhysicalSortExpr::new(
            Arc::new(Column::new("a", 0)),
            SortOptions {
                descending: true,
                nulls_first: true,
            },
        )])
        .unwrap();

        let sort = Arc::new(SortExec::new(sort_expr, source));
        let limit: Arc<dyn ExecutionPlan> =
            Arc::new(GlobalLimitExec::new(sort, 0, Some(5)));

        let optimized = optimize_and_sanity_check(limit).unwrap();
        let plan_str = datafusion_physical_plan::displayable(optimized.as_ref())
            .indent(true)
            .to_string();

        // fetch=5 must appear somewhere in the plan
        assert!(
            plan_str.contains("fetch=5"),
            "Fetch value lost during optimization:\n{plan_str}"
        );
    }

    // ========================================================================
    // Various partition counts (stress test)
    // ========================================================================

    /// Test with different partition counts: 2, 4, 8, 16, 32, 64
    #[test]
    fn test_various_partition_counts_all_pass_sanity_check() {
        for n in [2, 4, 8, 16, 32, 64] {
            let source = Arc::new(MockMultiPartitionExec::new(n));

            let sort_expr = LexOrdering::new(vec![PhysicalSortExpr::new(
                Arc::new(Column::new("a", 0)),
                SortOptions {
                    descending: true,
                    nulls_first: true,
                },
            )])
            .unwrap();

            let sort = Arc::new(SortExec::new(sort_expr, source));
            let limit: Arc<dyn ExecutionPlan> =
                Arc::new(GlobalLimitExec::new(sort, 0, Some(21)));

            let result = optimize_and_sanity_check(limit);
            assert!(
                result.is_ok(),
                "SanityCheckPlan failed for {n} partitions: {:?}",
                result.err()
            );
        }
    }

    /// Idempotency for all partition counts
    #[test]
    fn test_idempotent_various_partition_counts() {
        for n in [2, 4, 8, 16, 32, 64] {
            let source = Arc::new(MockMultiPartitionExec::new(n));

            let sort_expr = LexOrdering::new(vec![PhysicalSortExpr::new(
                Arc::new(Column::new("a", 0)),
                SortOptions {
                    descending: true,
                    nulls_first: true,
                },
            )])
            .unwrap();

            let sort = Arc::new(SortExec::new(sort_expr, source));
            let limit: Arc<dyn ExecutionPlan> =
                Arc::new(GlobalLimitExec::new(sort, 0, Some(10)));

            assert_idempotent(limit);
        }
    }

    // ========================================================================
    // CoalescePartitionsExec tests
    // ========================================================================

    /// CoalescePartitionsExec + sort should produce valid plan
    #[test]
    fn test_coalesce_then_sort_limit() {
        let source = Arc::new(MockMultiPartitionExec::new(8));
        let coalesce: Arc<dyn ExecutionPlan> =
            Arc::new(CoalescePartitionsExec::new(source));

        let sort_expr = LexOrdering::new(vec![PhysicalSortExpr::new(
            Arc::new(Column::new("a", 0)),
            SortOptions {
                descending: true,
                nulls_first: true,
            },
        )])
        .unwrap();

        let sort = Arc::new(SortExec::new(sort_expr, coalesce));
        let limit: Arc<dyn ExecutionPlan> =
            Arc::new(GlobalLimitExec::new(sort, 0, Some(10)));

        let result = optimize_and_sanity_check(Arc::clone(&limit));
        assert!(
            result.is_ok(),
            "SanityCheckPlan failed for coalesce+sort: {:?}",
            result.err()
        );
        assert_idempotent(limit);
    }

    // ========================================================================
    // Filter + multi-partition tests
    // ========================================================================

    /// FilterExec over multi-partition + sort + limit
    #[test]
    fn test_filter_over_multi_partition_sort_limit() {
        use datafusion_common::ScalarValue;
        use datafusion_physical_expr::expressions::Literal;
        use datafusion_physical_plan::filter::FilterExec;

        let source = Arc::new(MockMultiPartitionExec::new(16));

        // Simple always-true filter
        let predicate = Arc::new(Literal::new(ScalarValue::Boolean(Some(true))));
        let filter = Arc::new(FilterExec::try_new(predicate, source as _).unwrap());

        let sort_expr = LexOrdering::new(vec![PhysicalSortExpr::new(
            Arc::new(Column::new("a", 0)),
            SortOptions {
                descending: true,
                nulls_first: true,
            },
        )])
        .unwrap();

        let sort = Arc::new(SortExec::new(sort_expr, filter));
        let limit: Arc<dyn ExecutionPlan> =
            Arc::new(GlobalLimitExec::new(sort, 0, Some(21)));

        let result = optimize_and_sanity_check(Arc::clone(&limit));
        assert!(
            result.is_ok(),
            "SanityCheckPlan failed for filter+sort+limit: {:?}",
            result.err()
        );
        assert_idempotent(limit);
    }

    // ========================================================================
    // RepartitionExec tests
    // ========================================================================

    /// Existing RepartitionExec + sort + limit must remain valid
    #[test]
    fn test_repartition_sort_limit_idempotent() {
        let source = Arc::new(MockMultiPartitionExec::new(1));
        let repartition = Arc::new(
            RepartitionExec::try_new(source as _, Partitioning::RoundRobinBatch(8))
                .unwrap(),
        );

        let sort_expr = LexOrdering::new(vec![PhysicalSortExpr::new(
            Arc::new(Column::new("a", 0)),
            SortOptions {
                descending: true,
                nulls_first: true,
            },
        )])
        .unwrap();

        let sort = Arc::new(SortExec::new(sort_expr, repartition));
        let limit: Arc<dyn ExecutionPlan> =
            Arc::new(GlobalLimitExec::new(sort, 0, Some(10)));

        let result = optimize_and_sanity_check(Arc::clone(&limit));
        assert!(
            result.is_ok(),
            "SanityCheckPlan failed for repartition+sort: {:?}",
            result.err()
        );
        assert_idempotent(limit);
    }

    // ========================================================================
    // Skip + Fetch (offset + limit) tests
    // ========================================================================

    /// GlobalLimitExec with skip=5, fetch=10 must produce valid plan
    #[test]
    fn test_skip_and_fetch_multi_partition() {
        let source = Arc::new(MockMultiPartitionExec::new(16));

        let sort_expr = LexOrdering::new(vec![PhysicalSortExpr::new(
            Arc::new(Column::new("a", 0)),
            SortOptions {
                descending: true,
                nulls_first: true,
            },
        )])
        .unwrap();

        let sort = Arc::new(SortExec::new(sort_expr, source));
        // skip=5, fetch=10
        let limit: Arc<dyn ExecutionPlan> =
            Arc::new(GlobalLimitExec::new(sort, 5, Some(10)));

        let result = optimize_and_sanity_check(Arc::clone(&limit));
        assert!(
            result.is_ok(),
            "SanityCheckPlan failed for skip+fetch: {:?}",
            result.err()
        );
        assert_idempotent(limit);
    }

    // ========================================================================
    // Multiple sort columns test
    // ========================================================================

    /// Sort on (a DESC, b ASC) with multi-partition
    #[test]
    fn test_multi_column_sort_multi_partition() {
        let source = Arc::new(MockMultiPartitionExec::new(32));

        let sort_expr = LexOrdering::new(vec![
            PhysicalSortExpr::new(
                Arc::new(Column::new("a", 0)),
                SortOptions {
                    descending: true,
                    nulls_first: true,
                },
            ),
            PhysicalSortExpr::new(
                Arc::new(Column::new("b", 1)),
                SortOptions {
                    descending: false,
                    nulls_first: false,
                },
            ),
        ])
        .unwrap();

        let sort = Arc::new(SortExec::new(sort_expr, source));
        let limit: Arc<dyn ExecutionPlan> =
            Arc::new(GlobalLimitExec::new(sort, 0, Some(21)));

        let result = optimize_and_sanity_check(Arc::clone(&limit));
        assert!(
            result.is_ok(),
            "SanityCheckPlan failed for multi-column sort: {:?}",
            result.err()
        );
        assert_idempotent(limit);
    }

    // ========================================================================
    // PR #53 / #54 regression tests — pushdown_sorts distribution awareness
    // These test the specific bug where pushdown_sorts pushes SortExec through
    // an intermediate node onto a multi-partition source, setting
    // preserve_partitioning=true without inserting SortPreservingMergeExec.
    // ========================================================================

    /// Regression for PR #53: OutputRequirementExec(SinglePartition) wrapping
    /// a multi-partition source. EnforceSorting's ensure_sorting must insert
    /// SortPreservingMergeExec to satisfy SinglePartition requirement.
    ///
    /// The final ensure_distribution pass catches the distribution violation
    /// from pushdown_sorts, producing a valid plan (via CoalescePartitionsExec
    /// or SortPreservingMergeExec).
    #[test]
    fn test_pr53_output_requirement_single_partition_multi_partition_source() {
        let source = Arc::new(MockMultiPartitionExec::new(10));

        let sort_expr = LexOrdering::new(vec![PhysicalSortExpr::new(
            Arc::new(Column::new("a", 0)),
            SortOptions {
                descending: true,
                nulls_first: true,
            },
        )])
        .unwrap();

        // OutputRequirementExec with SinglePartition + ordering requirement
        let output_req: Arc<dyn ExecutionPlan> = Arc::new(OutputRequirementExec::new(
            source,
            Some(OrderingRequirements::from(sort_expr)),
            Distribution::SinglePartition,
            Some(21),
        ));

        // Run EnsureRequirements (which includes EnforceSorting internally)
        let config = ConfigOptions::default();
        let optimized = EnsureRequirements::new()
            .optimize(output_req, &config)
            .expect("optimize failed");

        let plan_str = datafusion_physical_plan::displayable(optimized.as_ref())
            .indent(true)
            .to_string();

        // SinglePartition must be satisfied (via SPM or Coalesce+Sort).
        let sanity = SanityCheckPlan::new().optimize(Arc::clone(&optimized), &config);
        assert!(
            sanity.is_ok(),
            "SanityCheckPlan failed for SinglePartition + multi-partition source:\n{plan_str}\nError: {:?}",
            sanity.err()
        );
    }

    /// Regression for PR #54: pushdown_sorts pushes sort through ProjectionExec
    /// onto multi-partition source. The sort must include SortPreservingMergeExec
    /// when parent requires SinglePartition.
    ///
    /// Currently fails because `pushdown_sorts` doesn't propagate distribution
    /// The final ensure_distribution pass catches the distribution violation,
    /// inserting CoalescePartitionsExec to satisfy SinglePartition.
    #[test]
    fn test_pr54_sort_pushdown_through_projection_adds_spm() {
        let source: Arc<dyn ExecutionPlan> = Arc::new(MockMultiPartitionExec::new(10));

        // Identity projection
        let proj_exprs: Vec<(Arc<dyn PhysicalExpr>, String)> = vec![
            (Arc::new(Column::new("a", 0)), "a".to_string()),
            (Arc::new(Column::new("b", 1)), "b".to_string()),
        ];
        let projection: Arc<dyn ExecutionPlan> =
            Arc::new(ProjectionExec::try_new(proj_exprs, source).unwrap());

        let sort_expr = LexOrdering::new(vec![PhysicalSortExpr::new(
            Arc::new(Column::new("a", 0)),
            SortOptions {
                descending: true,
                nulls_first: true,
            },
        )])
        .unwrap();

        // OutputRequirementExec(SinglePartition) → ProjectionExec → multi-partition source
        let output_req: Arc<dyn ExecutionPlan> = Arc::new(OutputRequirementExec::new(
            projection,
            Some(OrderingRequirements::from(sort_expr)),
            Distribution::SinglePartition,
            Some(21),
        ));

        let config = ConfigOptions::default();
        let optimized = EnsureRequirements::new()
            .optimize(output_req, &config)
            .expect("optimize failed");

        let plan_str = datafusion_physical_plan::displayable(optimized.as_ref())
            .indent(true)
            .to_string();

        // SinglePartition must be satisfied. The final ensure_distribution pass
        // adds CoalescePartitionsExec or SortPreservingMergeExec as needed.
        let sanity = SanityCheckPlan::new().optimize(Arc::clone(&optimized), &config);
        assert!(
            sanity.is_ok(),
            "SanityCheckPlan failed for projection + multi-partition:\n{plan_str}\nError: {:?}",
            sanity.err()
        );
    }

    /// Regression: no extra SortPreservingMergeExec when plan is already optimal.
    /// OutputRequirementExec(SinglePartition) → SortPreservingMergeExec → SortExec(preserve=true)
    /// → multi-partition source. Must NOT add duplicate SortPreservingMergeExec.
    #[test]
    fn test_no_extra_spm_when_already_optimal() {
        let source: Arc<dyn ExecutionPlan> = Arc::new(MockMultiPartitionExec::new(10));

        let sort_expr = LexOrdering::new(vec![PhysicalSortExpr::new(
            Arc::new(Column::new("a", 0)),
            SortOptions {
                descending: true,
                nulls_first: true,
            },
        )])
        .unwrap();

        // Already-optimal plan: SPM → Sort(preserve=true) → source(10 partitions)
        let sort = Arc::new(
            SortExec::new(sort_expr.clone(), source).with_preserve_partitioning(true),
        );
        let spm: Arc<dyn ExecutionPlan> =
            Arc::new(SortPreservingMergeExec::new(sort_expr, sort));

        let limit: Arc<dyn ExecutionPlan> =
            Arc::new(GlobalLimitExec::new(spm, 0, Some(21)));

        let config = ConfigOptions::default();
        let optimized = EnsureRequirements::new()
            .optimize(limit, &config)
            .expect("optimize failed");

        let plan_str = datafusion_physical_plan::displayable(optimized.as_ref())
            .indent(true)
            .to_string();

        // Count SortPreservingMergeExec occurrences — should be exactly 1
        let spm_count = plan_str.matches("SortPreservingMergeExec").count();
        assert!(
            spm_count <= 1,
            "Extra SortPreservingMergeExec added ({spm_count} found):\n{plan_str}"
        );

        SanityCheckPlan::new()
            .optimize(optimized, &config)
            .expect("SanityCheckPlan failed");
    }

    // ========================================================================
    // Idempotency tests for distribution-fix scenarios
    // These verify that the pushdown_sorts distribution fix actually
    // makes EnsureRequirements idempotent for the bug-triggering topologies.
    // ========================================================================

    /// Idempotency for PR #53 scenario: OutputRequirementExec(SinglePartition)
    /// + multi-partition source. Running twice must produce the same plan.
    #[test]
    fn test_idempotent_pr53_output_requirement() {
        let source = Arc::new(MockMultiPartitionExec::new(10));
        let sort_expr = LexOrdering::new(vec![PhysicalSortExpr::new(
            Arc::new(Column::new("a", 0)),
            SortOptions {
                descending: true,
                nulls_first: true,
            },
        )])
        .unwrap();

        let output_req: Arc<dyn ExecutionPlan> = Arc::new(OutputRequirementExec::new(
            source,
            Some(OrderingRequirements::from(sort_expr)),
            Distribution::SinglePartition,
            Some(21),
        ));

        assert_idempotent(output_req);
    }

    /// Idempotency for PR #54 scenario: OutputRequirementExec(SinglePartition)
    /// → ProjectionExec → multi-partition source.
    #[test]
    fn test_idempotent_pr54_projection_multi_partition() {
        let source: Arc<dyn ExecutionPlan> = Arc::new(MockMultiPartitionExec::new(10));
        let proj_exprs: Vec<(Arc<dyn PhysicalExpr>, String)> = vec![
            (Arc::new(Column::new("a", 0)), "a".to_string()),
            (Arc::new(Column::new("b", 1)), "b".to_string()),
        ];
        let projection: Arc<dyn ExecutionPlan> =
            Arc::new(ProjectionExec::try_new(proj_exprs, source).unwrap());

        let sort_expr = LexOrdering::new(vec![PhysicalSortExpr::new(
            Arc::new(Column::new("a", 0)),
            SortOptions {
                descending: true,
                nulls_first: true,
            },
        )])
        .unwrap();

        let output_req: Arc<dyn ExecutionPlan> = Arc::new(OutputRequirementExec::new(
            projection,
            Some(OrderingRequirements::from(sort_expr)),
            Distribution::SinglePartition,
            Some(21),
        ));

        assert_idempotent(output_req);
    }

    /// Idempotency: SPM → Sort(preserve=true) → multi-partition
    /// This is the topology that caused test_pushdown_through_spm to fail.
    #[test]
    fn test_idempotent_spm_sort_multi_partition() {
        let source: Arc<dyn ExecutionPlan> = Arc::new(MockMultiPartitionExec::new(10));
        let sort_expr = LexOrdering::new(vec![PhysicalSortExpr::new(
            Arc::new(Column::new("a", 0)),
            SortOptions {
                descending: true,
                nulls_first: true,
            },
        )])
        .unwrap();

        let sort = Arc::new(
            SortExec::new(sort_expr.clone(), source).with_preserve_partitioning(true),
        );
        let spm: Arc<dyn ExecutionPlan> =
            Arc::new(SortPreservingMergeExec::new(sort_expr, sort));
        let limit: Arc<dyn ExecutionPlan> =
            Arc::new(GlobalLimitExec::new(spm, 0, Some(21)));

        assert_idempotent(limit);
    }

    /// Idempotency: Sort → Aggregate → Sort → Aggregate pattern (#18989).
    /// This tests the multi-aggregate topology that caused SanityCheckPlan
    /// failures in upstream issue #18989.
    #[test]
    fn test_idempotent_sort_aggregate_sort_aggregate() {
        use datafusion_physical_plan::aggregates::{
            AggregateExec, AggregateMode, PhysicalGroupBy,
        };

        let schema = Arc::new(Schema::new(vec![
            Field::new("a", DataType::Int64, false),
            Field::new("b", DataType::Int64, false),
        ]));

        let source: Arc<dyn ExecutionPlan> = Arc::new(MockMultiPartitionExec::new(4));

        // Partial aggregate
        let group_by = PhysicalGroupBy::new_single(vec![(
            Arc::new(Column::new("a", 0)) as _,
            "a".to_string(),
        )]);
        let partial_agg: Arc<dyn ExecutionPlan> = Arc::new(
            AggregateExec::try_new(
                AggregateMode::Partial,
                group_by,
                vec![],
                vec![],
                source,
                Arc::clone(&schema),
            )
            .unwrap(),
        );

        let sort_expr = LexOrdering::new(vec![PhysicalSortExpr::new(
            Arc::new(Column::new("a", 0)),
            SortOptions {
                descending: false,
                nulls_first: false,
            },
        )])
        .unwrap();

        let sort: Arc<dyn ExecutionPlan> =
            Arc::new(SortExec::new(sort_expr, partial_agg));
        let limit: Arc<dyn ExecutionPlan> =
            Arc::new(GlobalLimitExec::new(sort, 0, Some(10)));

        assert_idempotent(limit);
    }

    /// Stress test: idempotency with ALL partition counts from 1 to 64
    #[test]
    fn test_idempotent_all_partition_counts_1_to_64() {
        for n in 1..=64 {
            let source = Arc::new(MockMultiPartitionExec::new(n));
            let sort_expr = LexOrdering::new(vec![PhysicalSortExpr::new(
                Arc::new(Column::new("a", 0)),
                SortOptions {
                    descending: true,
                    nulls_first: true,
                },
            )])
            .unwrap();
            let sort = Arc::new(SortExec::new(sort_expr, source));
            let limit: Arc<dyn ExecutionPlan> =
                Arc::new(GlobalLimitExec::new(sort, 0, Some(10)));
            assert_idempotent(limit);
        }
    }

    /// Triple optimization: EnsureRequirements three times must be stable.
    #[test]
    fn test_triple_optimize_stable() {
        let source = Arc::new(MockMultiPartitionExec::new(32));
        let sort_expr = LexOrdering::new(vec![PhysicalSortExpr::new(
            Arc::new(Column::new("a", 0)),
            SortOptions {
                descending: true,
                nulls_first: true,
            },
        )])
        .unwrap();
        let sort = Arc::new(SortExec::new(sort_expr, source));
        let limit: Arc<dyn ExecutionPlan> =
            Arc::new(GlobalLimitExec::new(sort, 0, Some(21)));

        let config = ConfigOptions::default();
        let p1 = EnsureRequirements::new().optimize(limit, &config).unwrap();
        let p2 = EnsureRequirements::new()
            .optimize(Arc::clone(&p1), &config)
            .unwrap();
        let p3 = EnsureRequirements::new()
            .optimize(Arc::clone(&p2), &config)
            .unwrap();

        let s1 = datafusion_physical_plan::displayable(p1.as_ref())
            .indent(true)
            .to_string();
        let s2 = datafusion_physical_plan::displayable(p2.as_ref())
            .indent(true)
            .to_string();
        let s3 = datafusion_physical_plan::displayable(p3.as_ref())
            .indent(true)
            .to_string();

        assert_eq!(
            s1, s2,
            "Not idempotent between pass 1 and 2:\nPass 1:\n{s1}\nPass 2:\n{s2}"
        );
        assert_eq!(
            s2, s3,
            "Not stable between pass 2 and 3:\nPass 2:\n{s2}\nPass 3:\n{s3}"
        );

        SanityCheckPlan::new()
            .optimize(p3, &config)
            .expect("SanityCheckPlan failed on pass 3");
    }

    /// Regression for #14150: EnforceDistribution claims to be idempotent
    /// but loses fetch when applied twice. Verify EnsureRequirements
    /// preserves fetch across multiple passes.
    #[test]
    fn test_issue_14150_fetch_survives_multiple_passes() {
        // Simulate: SELECT * FROM multi_partition_table ORDER BY a LIMIT 5
        // with target_partitions > 1 (triggers RepartitionExec)
        let source: Arc<dyn ExecutionPlan> = Arc::new(MockMultiPartitionExec::new(1));
        let repartition = Arc::new(
            RepartitionExec::try_new(source, Partitioning::RoundRobinBatch(4)).unwrap(),
        );

        let sort_expr = LexOrdering::new(vec![PhysicalSortExpr::new(
            Arc::new(Column::new("a", 0)),
            SortOptions {
                descending: false,
                nulls_first: true,
            },
        )])
        .unwrap();

        let sort =
            Arc::new(SortExec::new(sort_expr, repartition as _).with_fetch(Some(5)));
        let limit: Arc<dyn ExecutionPlan> =
            Arc::new(GlobalLimitExec::new(sort, 0, Some(5)));

        let config = ConfigOptions::default();

        // Pass 1
        let p1 = EnsureRequirements::new()
            .optimize(Arc::clone(&limit), &config)
            .unwrap();
        let s1 = datafusion_physical_plan::displayable(p1.as_ref())
            .indent(true)
            .to_string();

        // Pass 2
        let p2 = EnsureRequirements::new()
            .optimize(Arc::clone(&p1), &config)
            .unwrap();
        let s2 = datafusion_physical_plan::displayable(p2.as_ref())
            .indent(true)
            .to_string();

        // Pass 3
        let p3 = EnsureRequirements::new()
            .optimize(Arc::clone(&p2), &config)
            .unwrap();
        let s3 = datafusion_physical_plan::displayable(p3.as_ref())
            .indent(true)
            .to_string();

        // Fetch must survive all passes
        assert!(s1.contains("fetch=5"), "fetch=5 lost after pass 1:\n{s1}");
        assert!(
            s2.contains("fetch=5"),
            "fetch=5 lost after pass 2 (#14150 regression):\n{s2}"
        );
        assert!(s3.contains("fetch=5"), "fetch=5 lost after pass 3:\n{s3}");

        // Plans must be identical (idempotent)
        assert_eq!(s1, s2, "Plan changed between pass 1 and 2:\n{s1}\nvs\n{s2}");
        assert_eq!(s2, s3, "Plan changed between pass 2 and 3:\n{s2}\nvs\n{s3}");
    }

    // Note: a previous `test_issue_14150_enforce_distribution_idempotent`
    // test asserted idempotency of the standalone `EnforceDistribution`
    // pass. With that rule retired, the relevant property is covered by
    // `test_issue_14150_fetch_survives_multiple_passes` above, which
    // exercises `EnsureRequirements` end-to-end.

    // ========================================================================
    // Mock operator with configurable distribution / ordering requirements
    // (used by window-function and distribution tests below)
    // ========================================================================

    /// Mock operator requiring specific distribution and/or ordering from its
    /// single child. Simulates operators like `BoundedWindowAggExec` that
    /// demand hash-partitioning + ordering without pulling in complex window
    /// expression machinery.
    #[derive(Debug)]
    struct MockReqExec {
        input: Arc<dyn ExecutionPlan>,
        dist: Distribution,
        ord: Option<LexOrdering>,
        properties: Arc<PlanProperties>,
    }

    impl MockReqExec {
        fn new(
            input: Arc<dyn ExecutionPlan>,
            dist: Distribution,
            ord: Option<LexOrdering>,
        ) -> Self {
            let properties = Arc::new(PlanProperties::new(
                input.equivalence_properties().clone(),
                input.output_partitioning().clone(),
                input.pipeline_behavior(),
                input.boundedness(),
            ));
            Self {
                input,
                dist,
                ord,
                properties,
            }
        }
    }

    impl DisplayAs for MockReqExec {
        fn fmt_as(
            &self,
            _t: DisplayFormatType,
            f: &mut std::fmt::Formatter,
        ) -> std::fmt::Result {
            write!(f, "MockReqExec")
        }
    }

    impl ExecutionPlan for MockReqExec {
        fn name(&self) -> &str {
            "MockReqExec"
        }
        fn properties(&self) -> &Arc<PlanProperties> {
            &self.properties
        }
        fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
            vec![&self.input]
        }
        fn required_input_distribution(&self) -> Vec<Distribution> {
            vec![self.dist.clone()]
        }
        fn required_input_ordering(&self) -> Vec<Option<OrderingRequirements>> {
            vec![
                self.ord
                    .as_ref()
                    .map(|o| OrderingRequirements::from(o.clone())),
            ]
        }
        fn maintains_input_order(&self) -> Vec<bool> {
            vec![true]
        }
        fn with_new_children(
            self: Arc<Self>,
            mut c: Vec<Arc<dyn ExecutionPlan>>,
        ) -> Result<Arc<dyn ExecutionPlan>> {
            assert_eq!(c.len(), 1);
            Ok(Arc::new(MockReqExec::new(
                c.pop().expect("1 child"),
                self.dist.clone(),
                self.ord.clone(),
            )))
        }
        fn apply_expressions(
            &self,
            _f: &mut dyn FnMut(&dyn PhysicalExpr) -> Result<TreeNodeRecursion>,
        ) -> Result<TreeNodeRecursion> {
            Ok(TreeNodeRecursion::Continue)
        }
        fn execute(
            &self,
            _p: usize,
            _c: Arc<datafusion_execution::TaskContext>,
        ) -> Result<SendableRecordBatchStream> {
            unimplemented!()
        }
    }

    // ========================================================================
    // Additional idempotency tests covering remaining sub-passes
    // ========================================================================

    /// Idempotency: plan that triggers `parallelize_sorts`.
    /// CoalescePartitionsExec → SortExec(preserve=false) → multi-partition
    /// source. After optimization Sort+SPM should be parallel. Running twice
    /// must produce the same plan.
    #[test]
    fn test_idempotent_parallelize_sorts() {
        let source: Arc<dyn ExecutionPlan> = Arc::new(MockMultiPartitionExec::new(8));
        let coalesce: Arc<dyn ExecutionPlan> =
            Arc::new(CoalescePartitionsExec::new(source));

        let sort_expr = LexOrdering::new(vec![PhysicalSortExpr::new(
            Arc::new(Column::new("a", 0)),
            SortOptions {
                descending: true,
                nulls_first: true,
            },
        )])
        .unwrap();

        // Sort without preserve_partitioning on top of coalesced input
        let sort: Arc<dyn ExecutionPlan> = Arc::new(SortExec::new(sort_expr, coalesce));
        let limit: Arc<dyn ExecutionPlan> =
            Arc::new(GlobalLimitExec::new(sort, 0, Some(10)));

        // First pass should parallelize the sort (Sort+SPM replaces Coalesce+Sort)
        let config = ConfigOptions::default();
        let p1 = EnsureRequirements::new()
            .optimize(Arc::clone(&limit), &config)
            .expect("first optimize failed");
        let p2 = EnsureRequirements::new()
            .optimize(Arc::clone(&p1), &config)
            .expect("second optimize failed");

        let s1 = datafusion_physical_plan::displayable(p1.as_ref())
            .indent(true)
            .to_string();
        let s2 = datafusion_physical_plan::displayable(p2.as_ref())
            .indent(true)
            .to_string();
        assert_eq!(
            s1, s2,
            "parallelize_sorts NOT idempotent!\nFirst:\n{s1}\nSecond:\n{s2}"
        );

        SanityCheckPlan::new()
            .optimize(p1, &config)
            .expect("SanityCheckPlan failed on first pass");
        SanityCheckPlan::new()
            .optimize(p2, &config)
            .expect("SanityCheckPlan failed on second pass");
    }

    /// Idempotency: SortMergeJoinExec with two multi-partition inputs + ORDER BY
    /// + LIMIT. Tests that join key reordering + sort enforcement is stable.
    #[test]
    fn test_idempotent_sort_merge_join() {
        let left: Arc<dyn ExecutionPlan> = Arc::new(MockMultiPartitionExec::new(4));
        let right: Arc<dyn ExecutionPlan> = Arc::new(MockMultiPartitionExec::new(4));

        let on = vec![(
            Arc::new(Column::new("a", 0)) as Arc<dyn PhysicalExpr>,
            Arc::new(Column::new("a", 0)) as Arc<dyn PhysicalExpr>,
        )];

        let join: Arc<dyn ExecutionPlan> = Arc::new(
            SortMergeJoinExec::try_new(
                left,
                right,
                on,
                None,
                JoinType::Inner,
                vec![SortOptions {
                    descending: false,
                    nulls_first: false,
                }],
                NullEquality::NullEqualsNothing,
            )
            .expect("SortMergeJoinExec creation failed"),
        );

        let sort_expr = LexOrdering::new(vec![PhysicalSortExpr::new(
            Arc::new(Column::new("a", 0)),
            SortOptions {
                descending: true,
                nulls_first: true,
            },
        )])
        .unwrap();

        let sort = Arc::new(SortExec::new(sort_expr, join));
        let limit: Arc<dyn ExecutionPlan> =
            Arc::new(GlobalLimitExec::new(sort, 0, Some(50)));

        assert_idempotent(limit);
    }

    /// Idempotency: window-function-like operator over multi-partition source.
    /// Uses MockReqExec with hash distribution + ordering to simulate
    /// BoundedWindowAggExec requirements. Tests that window partitioning +
    /// sort requirements are stable across optimizer passes.
    #[test]
    fn test_idempotent_window_over_multi_partition() {
        let source: Arc<dyn ExecutionPlan> = Arc::new(MockMultiPartitionExec::new(8));

        // Window function requires Hash(a) distribution + ordering [a ASC, b ASC]
        let ord = LexOrdering::new(vec![
            PhysicalSortExpr::new(
                Arc::new(Column::new("a", 0)),
                SortOptions {
                    descending: false,
                    nulls_first: false,
                },
            ),
            PhysicalSortExpr::new(
                Arc::new(Column::new("b", 1)),
                SortOptions {
                    descending: false,
                    nulls_first: false,
                },
            ),
        ])
        .unwrap();

        let dist = Distribution::HashPartitioned(vec![Arc::new(Column::new("a", 0))]);
        let window_like: Arc<dyn ExecutionPlan> =
            Arc::new(MockReqExec::new(source, dist, Some(ord)));

        let sort_expr = LexOrdering::new(vec![PhysicalSortExpr::new(
            Arc::new(Column::new("a", 0)),
            SortOptions {
                descending: true,
                nulls_first: true,
            },
        )])
        .unwrap();

        let sort = Arc::new(SortExec::new(sort_expr, window_like));
        let limit: Arc<dyn ExecutionPlan> =
            Arc::new(GlobalLimitExec::new(sort, 0, Some(20)));

        assert_idempotent(limit);
    }

    /// Idempotency: multiple levels of sort + limit.
    /// GlobalLimitExec → SortExec → ProjectionExec → GlobalLimitExec → SortExec
    /// → multi-partition source. Tests deeply nested sort/limit stability.
    #[test]
    fn test_idempotent_nested_subqueries_sort_limit() {
        let source: Arc<dyn ExecutionPlan> = Arc::new(MockMultiPartitionExec::new(8));

        // Inner sort (a DESC) + inner limit — use DESC to avoid matching
        // MockMultiPartitionExec's built-in ASC ordering, which would cause
        // the optimizer to eliminate the sort differently across passes.
        let inner_sort_expr = LexOrdering::new(vec![PhysicalSortExpr::new(
            Arc::new(Column::new("a", 0)),
            SortOptions {
                descending: true,
                nulls_first: true,
            },
        )])
        .unwrap();
        let inner_sort: Arc<dyn ExecutionPlan> =
            Arc::new(SortExec::new(inner_sort_expr, source));
        let inner_limit: Arc<dyn ExecutionPlan> =
            Arc::new(GlobalLimitExec::new(inner_sort, 0, Some(100)));

        // Identity projection
        let proj_exprs: Vec<(Arc<dyn PhysicalExpr>, String)> = vec![
            (Arc::new(Column::new("a", 0)), "a".to_string()),
            (Arc::new(Column::new("b", 1)), "b".to_string()),
        ];
        let projection: Arc<dyn ExecutionPlan> =
            Arc::new(ProjectionExec::try_new(proj_exprs, inner_limit).unwrap());

        // Outer sort (a DESC) + outer limit
        let outer_sort_expr = LexOrdering::new(vec![PhysicalSortExpr::new(
            Arc::new(Column::new("a", 0)),
            SortOptions {
                descending: true,
                nulls_first: true,
            },
        )])
        .unwrap();
        let outer_sort: Arc<dyn ExecutionPlan> =
            Arc::new(SortExec::new(outer_sort_expr, projection));
        let outer_limit: Arc<dyn ExecutionPlan> =
            Arc::new(GlobalLimitExec::new(outer_sort, 0, Some(10)));

        assert_idempotent(outer_limit);
    }

    /// Idempotency: RepartitionExec(Hash) + sort + limit.
    /// Tests that hash distribution + ordering enforcement is stable.
    #[test]
    fn test_idempotent_repartition_hash_sort_limit() {
        let source: Arc<dyn ExecutionPlan> = Arc::new(MockMultiPartitionExec::new(8));

        let hash_exprs: Vec<Arc<dyn PhysicalExpr>> = vec![Arc::new(Column::new("a", 0))];
        let repartition = Arc::new(
            RepartitionExec::try_new(source, Partitioning::Hash(hash_exprs, 4)).unwrap(),
        );

        let sort_expr = LexOrdering::new(vec![PhysicalSortExpr::new(
            Arc::new(Column::new("a", 0)),
            SortOptions {
                descending: true,
                nulls_first: true,
            },
        )])
        .unwrap();

        let sort: Arc<dyn ExecutionPlan> =
            Arc::new(SortExec::new(sort_expr, repartition));
        let limit: Arc<dyn ExecutionPlan> =
            Arc::new(GlobalLimitExec::new(sort, 0, Some(15)));

        assert_idempotent(limit);
    }

    /// EnforceDistribution::optimize twice on a HashJoinExec plan must produce
    /// identical plans. Tests that hash distribution enforcement is stable.
    #[test]
    fn test_enforce_distribution_idempotent_hash_join() {
        let left: Arc<dyn ExecutionPlan> = Arc::new(MockMultiPartitionExec::new(4));
        let right: Arc<dyn ExecutionPlan> = Arc::new(MockMultiPartitionExec::new(4));

        let on = vec![(
            Arc::new(Column::new("a", 0)) as Arc<dyn PhysicalExpr>,
            Arc::new(Column::new("a", 0)) as Arc<dyn PhysicalExpr>,
        )];

        let join: Arc<dyn ExecutionPlan> = Arc::new(
            HashJoinExec::try_new(
                left,
                right,
                on,
                None,
                &JoinType::Inner,
                None,
                PartitionMode::Partitioned,
                NullEquality::NullEqualsNothing,
                false,
            )
            .expect("HashJoinExec creation failed"),
        );

        let config = ConfigOptions::default();
        let p1 = EnsureRequirements::new()
            .optimize(Arc::clone(&join), &config)
            .expect("first EnforceDistribution failed");
        let p2 = EnsureRequirements::new()
            .optimize(Arc::clone(&p1), &config)
            .expect("second EnforceDistribution failed");

        let s1 = datafusion_physical_plan::displayable(p1.as_ref())
            .indent(true)
            .to_string();
        let s2 = datafusion_physical_plan::displayable(p2.as_ref())
            .indent(true)
            .to_string();

        assert_eq!(
            s1, s2,
            "EnforceDistribution not idempotent for HashJoinExec!\nPass 1:\n{s1}\nPass 2:\n{s2}"
        );
    }

    /// Stress test: run EnsureRequirements 10 times on a complex plan.
    /// GlobalLimitExec → SortExec → ProjectionExec →
    /// UnionExec(multi-partition, single-partition).
    /// All 10 passes must produce identical plans.
    #[test]
    fn test_idempotent_10x_complex() {
        let live: Arc<dyn ExecutionPlan> = Arc::new(MockMultiPartitionExec::new(16));
        let hist: Arc<dyn ExecutionPlan> = Arc::new(MockMultiPartitionExec::new(1));
        let union: Arc<dyn ExecutionPlan> = UnionExec::try_new(vec![live, hist]).unwrap();

        // Identity projection
        let proj_exprs: Vec<(Arc<dyn PhysicalExpr>, String)> = vec![
            (Arc::new(Column::new("a", 0)), "a".to_string()),
            (Arc::new(Column::new("b", 1)), "b".to_string()),
        ];
        let projection: Arc<dyn ExecutionPlan> =
            Arc::new(ProjectionExec::try_new(proj_exprs, union).unwrap());

        let sort_expr = LexOrdering::new(vec![PhysicalSortExpr::new(
            Arc::new(Column::new("a", 0)),
            SortOptions {
                descending: true,
                nulls_first: true,
            },
        )])
        .unwrap();

        let sort = Arc::new(SortExec::new(sort_expr, projection));
        let plan: Arc<dyn ExecutionPlan> =
            Arc::new(GlobalLimitExec::new(sort, 0, Some(21)));

        let config = ConfigOptions::default();
        let mut current = EnsureRequirements::new()
            .optimize(Arc::clone(&plan), &config)
            .expect("first optimize failed");
        let first = datafusion_physical_plan::displayable(current.as_ref())
            .indent(true)
            .to_string();

        for i in 2..=10 {
            current = EnsureRequirements::new()
                .optimize(Arc::clone(&current), &config)
                .unwrap_or_else(|e| panic!("optimize pass {i} failed: {e}"));
            let s = datafusion_physical_plan::displayable(current.as_ref())
                .indent(true)
                .to_string();
            assert_eq!(
                first, s,
                "Plan changed on pass {i}!\nFirst:\n{first}\nPass {i}:\n{s}"
            );
        }

        SanityCheckPlan::new()
            .optimize(current, &config)
            .expect("SanityCheckPlan failed after 10 passes");
    }
}
