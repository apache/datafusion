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

//! [`EnsureRequirements`] optimizer rule that enforces distribution and
//! sorting requirements together so that the two never invalidate each other.
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
//! `optimize` runs several tree traversals. The defining property of this
//! rule is **Phase 2**: a single combined bottom-up pass that resolves
//! distribution *and* sorting for each node together. The surrounding phases
//! are independent traversals (top-down join-key reorder, then several
//! follow-up sort/order rewrites). Some of those could be consolidated
//! further in a follow-up.
//!
//! ```text
//! EnsureRequirements::optimize(plan)
//! │
//! ├─ Phase 1: top-down join-key reorder        (adjust_input_keys_ordering)
//! │
//! ├─ Phase 2: combined distribution + sorting  (single bottom-up pass)
//! │   └─ For each node (bottom-up), for each child:
//! │       Step 1: ensure distribution requirement
//! │         └─ insert RepartitionExec / CoalescePartitionsExec /
//! │            SortPreservingMergeExec as needed
//! │       Step 2: ensure ordering requirement (distribution-aware)
//! │         └─ insert SortExec with the correct `preserve_partitioning`,
//! │            with SortPreservingMergeExec on top if needed
//! │
//! └─ Phase 3: small follow-up passes (bottom-up unless noted)
//!     ├─ parallelize_sorts
//!     ├─ replace_with_order_preserving_variants
//!     ├─ pushdown_sorts                         (recursive walk)
//!     └─ replace_with_partial_sort
//! ```
//!
//! # Key Properties
//!
//! - **Idempotent across the whole rule**: Running `EnsureRequirements`
//!   twice produces the same plan. This is the property that fixes
//!   <https://github.com/apache/datafusion/issues/21973>, where the old
//!   two-rule pipeline could regress a parallel sort plan into a serial one
//!   on pass 2.
//! - **Distribution before sorting**: For each child, distribution is
//!   resolved before ordering, so sorting decisions always have full
//!   distribution context.
//! - **Sort pushdown is implicit**: Phase 2 only adds `SortExec` where the
//!   child doesn't already satisfy the ordering requirement, so sorts land
//!   at the deepest valid position without a separate destructive pass.
//!
//! # Behavior: parallelism via repartitioning
//!
//! Phase 2 Step 1 inserts `RepartitionExec` to satisfy distribution
//! requirements. When configuration allows, it also increases parallelism by
//! repartitioning over otherwise-serial inputs. For example, given two
//! 1-partition inputs feeding an operator that can run with more
//! parallelism:
//!
//! ```text
//! ┌─────────────────────────────────┐
//! │          ExecutionPlan          │
//! └─────────────────────────────────┘
//!         ▲                 ▲
//!         │                 │
//!   ┌───────────┐     ┌───────────┐
//!   │  batch A  │     │  batch B  │      Input: 2 partitions
//!   └───────────┘     └───────────┘
//! ```
//!
//! `EnsureRequirements` inserts a `RepartitionExec` so the operator runs
//! with three partitions:
//!
//! ```text
//! ┌─────────────────────────────────┐
//! │          ExecutionPlan          │      Input now has 3 partitions
//! └─────────────────────────────────┘
//!         ▲      ▲       ▲
//!         └──────┼───────┘
//!                │
//! ┌─────────────────────────────────┐
//! │       RepartitionExec(3)        │      batches are repartitioned
//! │           RoundRobin            │
//! └─────────────────────────────────┘
//!         ▲                 ▲
//!   ┌───────────┐     ┌───────────┐
//!   │  batch A  │     │  batch B  │
//!   └───────────┘     └───────────┘
//! ```
//!
//! # Behavior: joint distribution + sorting
//!
//! Resolving distribution and sorting together lets Phase 2 produce a
//! parallel sort plan in cases where the two-rule pipeline historically
//! risked a serial one. Given `Sort(DESC) ← Coalesce ← MultiPartitionSource`,
//! `EnsureRequirements` rewrites it into:
//!
//! ```text
//! SortPreservingMergeExec: [a DESC]            (cheap k-way merge of sorted streams)
//!   SortExec: [a DESC], preserve_partitioning=true   (N sorts run in parallel)
//!     MultiPartitionSource
//! ```
//!
//! Each input partition is sorted in parallel, then a `SortPreservingMergeExec`
//! at the top performs a cheap merge of pre-sorted streams. For TopK queries
//! (`fetch=K`), each parallel sort only keeps K rows per partition, so total
//! memory is `N × K` rather than coalescing the entire stream first.
//!
//! # Behavior: strictest distribution match for joins
//!
//! Distribution requirements are met in the strictest way. For example, a
//! hash join with keys `(a, b, c)` requires `Distribution(a, b, c)`. This
//! can in principle be satisfied by partitioning on any superset of any
//! subset of `(a, b, c)`, but this rule always partitions on the exact key
//! tuple `(a, b, c)`. This is sometimes more aggressive than strictly
//! necessary, but the strictest match helps avoid data skew in joins.

// Internal implementation modules. Re-exported from `crate` root for tests
// in `core/tests/physical_optimizer/{enforce_distribution,enforce_sorting}.rs`.
pub mod enforce_distribution;
pub mod enforce_sorting;

use std::sync::Arc;

use crate::PhysicalOptimizerRule;

use datafusion_common::Result;
use datafusion_common::config::ConfigOptions;
use datafusion_common::tree_node::{Transformed, TransformedResult, TreeNode};
use datafusion_physical_plan::ExecutionPlan;

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
        use super::enforce_distribution::{
            PlanWithKeyRequirements, adjust_input_keys_ordering,
        };
        let top_down_join_key_reordering = config.optimizer.top_down_join_key_reordering;
        let plan = if top_down_join_key_reordering {
            let ctx = PlanWithKeyRequirements::new_default(plan);
            ctx.transform_down(adjust_input_keys_ordering).data()?.plan
        } else {
            use super::enforce_distribution::reorder_join_keys_to_inputs;
            plan.transform_up(|p| Ok(Transformed::yes(reorder_join_keys_to_inputs(p)?)))
                .data()?
        };

        // Phase 2: Combined distribution + sorting enforcement (single bottom-up pass)
        // For each node: distribution first, then sorting.
        use super::enforce_distribution::{DistributionContext, ensure_distribution};
        use super::enforce_sorting::{PlanWithCorrespondingSort, ensure_sorting};

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
        use super::enforce_sorting::{
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
        use super::enforce_sorting::replace_with_order_preserving_variants::{
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
        use super::enforce_sorting::sort_pushdown::{
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

// See tests in datafusion/core/tests/physical_optimizer/ensure_requirements.rs
