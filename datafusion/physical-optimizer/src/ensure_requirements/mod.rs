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

//! Requirement enforcement: makes a plan satisfy the distribution and
//! ordering requirements its operators declare, inspired by Apache Spark's
//! `EnsureRequirements` and Presto/Trino's `AddExchanges`.
//!
//! The default pipeline runs [`EnsureRequirements`] as a `PhysicalAnalyzerRule`,
//! where it enforces distribution (Phases 0-2a) and then ordering (Phase 2b).
//! The Phase 3 sort *optimizations* run later, as the [`OptimizeSorts`]
//! optimizer rule. Registered as a `PhysicalOptimizerRule`, `EnsureRequirements`
//! runs all phases in one pass, as it always has.
//!
//! # Motivation
//!
//! Ordering enforcement used to include sort pushdown, and that pushdown could
//! break the distribution established just before it, because
//! `SortExec.preserve_partitioning` couples sorting and distribution decisions.
//! See <https://github.com/apache/datafusion/issues/21973> for details. The
//! phases here keep them apart: enforcement (Phases 0-2) only inserts what a
//! requirement needs, and sort pushdown lives in the Phase 3 optimizations,
//! which run on an already-valid plan.
//!
//! # Architecture
//!
//! Enforcement is a sequence of tree traversals. Phase 2a settles distribution
//! bottom-up; Phase 2b then settles ordering bottom-up on the
//! distribution-fixed plan, so every sorting decision has full distribution
//! context. Phases 0 and 1 are top-down preparation. Phase 3 is optimization
//! and runs separately, after the optimizer rules that create the operators
//! it parallelizes.
//!
//! ```text
//! enforce_requirements(plan)         EnsureRequirements as an analyzer rule
//! │
//! ├─ Phase 0: top-down Interleave → Union      (replace_interleave_with_union)
//! │
//! ├─ Phase 1: top-down join-key reorder        (adjust_input_keys_ordering)
//! │
//! ├─ Phase 2a: distribution enforcement        (bottom-up)
//! │   └─ insert RepartitionExec / CoalescePartitionsExec /
//! │      SortPreservingMergeExec as each child's requirement needs
//! │
//! └─ Phase 2b: ordering enforcement            (bottom-up, distribution-aware)
//!     └─ insert SortExec with the correct `preserve_partitioning`,
//!        with SortPreservingMergeExec on top if needed
//!
//! optimize_sorts(plan)               OptimizeSorts, later in the optimizer
//! │                                  (EnsureRequirements as an optimizer rule
//! │                                   runs this in the same pass)
//! └─ Phase 3: small follow-up passes (bottom-up unless noted)
//!     ├─ parallelize_sorts
//!     ├─ replace_with_order_preserving_variants
//!     ├─ pushdown_sorts                         (recursive walk)
//!     └─ replace_with_partial_sort
//! ```
//!
//! # Key Properties
//!
//! - **Distribution before sorting**: Phase 2a finishes before Phase 2b
//!   starts, so sorting decisions always have full distribution context.
//! - **Sort pushdown is implicit**: Phase 2b only adds `SortExec` where the
//!   child doesn't already satisfy the ordering requirement, so sorts land
//!   at the deepest valid position without a separate destructive pass.
//!
//! # Behavior: parallelism via repartitioning
//!
//! Phase 2a inserts `RepartitionExec` to satisfy distribution
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
//! Settling distribution before sorting lets Phase 2b produce a
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
use crate::analyzer::PhysicalAnalyzerRule;
use crate::optimizer::{ConfigOnlyContext, PhysicalOptimizerContext};

use datafusion_common::Result;
use datafusion_common::config::ConfigOptions;
use datafusion_common::tree_node::{Transformed, TransformedResult, TreeNode};
use datafusion_physical_plan::ExecutionPlan;
use datafusion_physical_plan::statistics::StatisticsContext;

/// Makes a plan satisfy the distribution and ordering requirements its
/// operators declare.
///
/// As a [`PhysicalAnalyzerRule`], which is how the default pipeline registers
/// it, the rule enforces: distribution via [`enforce_distribution_requirements`],
/// then ordering via [`enforce_sorting_requirements`]. The sort optimizations
/// run later, as the [`OptimizeSorts`] optimizer rule.
///
/// As a [`PhysicalOptimizerRule`] it additionally runs those sort optimizations
/// ([`optimize_sorts`]) in the same pass, so a pipeline that registers it in an
/// optimizer list keeps the behavior it had before the analyzer phase existed.
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

/// Phases 0-2a: make the plan valid with respect to **distribution**
/// requirements only (normalize interleave, join-key reordering, distribution
/// enforcement). Split from ordering enforcement so it can be used on its own:
/// it is the first half of [`enforce_requirements`], and a [`JoinSelection`]
/// registered after enforcement calls it directly to re-establish the
/// partitioning its rewrite disturbed, without touching ordering.
///
/// [`JoinSelection`]: crate::join_selection::JoinSelection
pub fn enforce_distribution_requirements(
    plan: Arc<dyn ExecutionPlan>,
    context: &dyn PhysicalOptimizerContext,
) -> Result<Arc<dyn ExecutionPlan>> {
    let config = context.config_options();
    // Phase 0: Normalize `InterleaveExec` back to `UnionExec` (top-down).
    // Interleaves are distribution artifacts of Phase 2a, which re-derives
    // them from the children's final partitioning. Keeping them would
    // fail as soon as a child loses the partitioning they depend on.
    use super::enforce_distribution::replace_interleave_with_union;
    let plan = plan.transform_down(replace_interleave_with_union).data()?;

    // Phase 1: Join key reordering (top-down)
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

    // Phase 2a: Distribution enforcement (bottom-up)
    use super::enforce_distribution::{
        DistributionContext, ensure_distribution_with_stats,
    };
    let dist_ctx = DistributionContext::new_default(plan);
    // Share one statistics context across the whole distribution pass so each
    // subtree's statistics are computed once instead of once per ancestor.
    // Build it from the session's statistics registry so registered providers
    // are consulted (an empty registry, the default, is unchanged behavior).
    // `StatsCache` is keyed by raw node pointer, so reset it after any node
    // whose plan pointer actually changed: a rewrite can free a cached node
    // and a later allocation could reuse its address. A node that makes no
    // change cannot free anything, so the cache safely persists across the
    // no-op nodes that dominate a deep plan.
    let stats_ctx = match context.statistics_registry() {
        Some(registry) => StatisticsContext::new_with_registry(registry.clone()),
        None => StatisticsContext::new(),
    };
    let dist_ctx = dist_ctx
        .transform_up(|ctx| {
            let before = Arc::clone(&ctx.plan);
            let result = ensure_distribution_with_stats(ctx, config, &stats_ctx)?;
            if !Arc::ptr_eq(&before, &result.data.plan) {
                stats_ctx.reset_cache();
            }
            Ok(result)
        })
        .data()?;
    Ok(dist_ctx.plan)
}

/// Phase 2b: enforce **ordering** requirements by inserting `SortExec`s on a
/// distribution-fixed plan (bottom-up). This is the enforcement half that is
/// *not* idempotent, so in the default pipeline it runs exactly once, as the
/// second half of [`enforce_requirements`].
pub fn enforce_sorting_requirements(
    plan: Arc<dyn ExecutionPlan>,
) -> Result<Arc<dyn ExecutionPlan>> {
    use super::enforce_sorting::{PlanWithCorrespondingSort, ensure_sorting};
    let sort_ctx = PlanWithCorrespondingSort::new_default(plan);
    let sort_ctx = sort_ctx.transform_up(ensure_sorting)?.data;
    Ok(sort_ctx.plan)
}

/// Phases 0-2: full requirement enforcement (distribution via
/// [`enforce_distribution_requirements`], then sorting via
/// [`enforce_sorting_requirements`]). This is what the [`EnsureRequirements`]
/// analyzer rule runs, and what an optimizer rule that changes a requirement
/// (`WindowTopN`) calls to re-establish validity for what it rewrote.
pub fn enforce_requirements(
    plan: Arc<dyn ExecutionPlan>,
    context: &dyn PhysicalOptimizerContext,
) -> Result<Arc<dyn ExecutionPlan>> {
    let plan = enforce_distribution_requirements(plan, context)?;
    enforce_sorting_requirements(plan)
}

/// Phase 3: sort and distribution *optimizations* that make an already-valid
/// plan faster (parallelize sorts, order-preserving variants, sort pushdown,
/// partial sort). Split out of enforcement so it can run in the optimizer phase,
/// after other optimizer rules (such as `WindowTopN`) have produced the
/// operators it parallelizes. Exposed as the [`OptimizeSorts`] rule.
pub fn optimize_sorts(
    plan: Arc<dyn ExecutionPlan>,
    config: &ConfigOptions,
) -> Result<Arc<dyn ExecutionPlan>> {
    // 3a: Parallelize sorts (Coalesce+Sort → SPM+Sort)
    use super::enforce_sorting::{
        PlanWithCorrespondingCoalescePartitions, parallelize_sorts,
        replace_with_partial_sort,
    };
    let plan = if config.optimizer.repartition_sorts {
        let ctx = PlanWithCorrespondingCoalescePartitions::new_default(plan);
        ctx.transform_up(parallelize_sorts).data()?.plan
    } else {
        plan
    };

    // 3b: Order-preserving variants
    use super::enforce_sorting::replace_with_order_preserving_variants::{
        OrderPreservationContext, replace_with_order_preserving_variants,
    };
    let ctx = OrderPreservationContext::new_default(plan);
    let plan = ctx
        .transform_up(|c| replace_with_order_preserving_variants(c, false, true, config))
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

/// Sort/distribution **optimizations** (Phase 3) via [`optimize_sorts`]:
/// parallelize sorts, order-preserving variants, sort pushdown, partial sort.
/// Runs in the optimizer phase, after enforcement, on an already-valid plan.
#[derive(Default, Debug)]
pub struct OptimizeSorts {}

impl OptimizeSorts {
    #[expect(missing_docs)]
    pub fn new() -> Self {
        Self {}
    }
}

impl PhysicalOptimizerRule for OptimizeSorts {
    fn optimize(
        &self,
        plan: Arc<dyn ExecutionPlan>,
        config: &ConfigOptions,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        optimize_sorts(plan, config)
    }

    fn name(&self) -> &str {
        "OptimizeSorts"
    }

    fn schema_check(&self) -> bool {
        true
    }
}

/// The analyzer registration: enforcement only (Phases 0-2). The sort
/// optimizations run later as [`OptimizeSorts`], after the optimizer rules that
/// produce the operators they parallelize (such as `WindowTopN`).
impl PhysicalAnalyzerRule for EnsureRequirements {
    fn analyze(
        &self,
        plan: Arc<dyn ExecutionPlan>,
        config: &ConfigOptions,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        enforce_requirements(plan, &ConfigOnlyContext::new(config))
    }

    fn analyze_with_context(
        &self,
        plan: Arc<dyn ExecutionPlan>,
        context: &dyn PhysicalOptimizerContext,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        enforce_requirements(plan, context)
    }

    fn name(&self) -> &str {
        "EnsureRequirements"
    }

    fn schema_check(&self) -> bool {
        true
    }
}

/// The optimizer registration: enforcement plus the sort optimizations in one
/// pass (Phases 0-3), the behavior pipelines that register `EnsureRequirements`
/// in an optimizer list have always had.
impl PhysicalOptimizerRule for EnsureRequirements {
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
        let plan = enforce_requirements(plan, context)?;
        optimize_sorts(plan, context.config_options())
    }

    fn name(&self) -> &str {
        "EnsureRequirements"
    }

    fn schema_check(&self) -> bool {
        true
    }
}

// See tests in datafusion/core/tests/physical_optimizer/ensure_requirements.rs
