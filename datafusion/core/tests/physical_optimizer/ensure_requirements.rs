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

//! Integration tests for `EnsureRequirements`.
//!
//! Ported verbatim from `datafusion/physical-optimizer/src/ensure_requirements/mod.rs`
//! so the tests live alongside the rest of the `physical_optimizer/` integration
//! suite and can use real `ExecutionPlan`s where convenient.

use datafusion_common::config::ConfigOptions;
use datafusion_physical_optimizer::PhysicalOptimizerRule;
use datafusion_physical_optimizer::ensure_requirements::EnsureRequirements;

use std::sync::Arc;

use arrow::compute::SortOptions;
use arrow::datatypes::{DataType, Field, Schema};
use datafusion_common::Result;
use datafusion_physical_expr::expressions::Column;
use datafusion_physical_expr::{
    EquivalenceProperties, LexOrdering, PhysicalExpr, PhysicalSortExpr,
};
use datafusion_physical_plan::execution_plan::{Boundedness, EmissionType};
use datafusion_physical_plan::limit::GlobalLimitExec;
use datafusion_physical_plan::sorts::sort::SortExec;
use datafusion_physical_plan::union::UnionExec;
use datafusion_physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, ExecutionPlanProperties, Partitioning,
    PlanProperties, SendableRecordBatchStream,
};

use datafusion_physical_optimizer::output_requirements::OutputRequirementExec;
use datafusion_physical_optimizer::sanity_checker::SanityCheckPlan;

use datafusion_common::{JoinType, NullEquality};
use datafusion_physical_expr::Distribution;
use datafusion_physical_expr_common::sort_expr::OrderingRequirements;
use datafusion_physical_plan::coalesce_partitions::CoalescePartitionsExec;
use datafusion_physical_plan::joins::{HashJoinExec, PartitionMode, SortMergeJoinExec};
use datafusion_physical_plan::projection::ProjectionExec;
use datafusion_physical_plan::repartition::RepartitionExec;
use datafusion_physical_plan::sorts::sort_preserving_merge::SortPreservingMergeExec;

const TEST_TARGET_PARTITIONS: usize = 8;

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
    fn execute(
        &self,
        _partition: usize,
        _context: Arc<datafusion_execution::TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        unimplemented!()
    }
}

fn test_config() -> ConfigOptions {
    let mut config = ConfigOptions::default();
    // Keep plan-shape tests deterministic across machines with different CPU counts.
    config.execution.target_partitions = TEST_TARGET_PARTITIONS;
    config
}

/// Helper: run EnsureRequirements and verify SanityCheckPlan passes
fn optimize_and_sanity_check(
    plan: Arc<dyn ExecutionPlan>,
) -> Result<Arc<dyn ExecutionPlan>> {
    let config = test_config();
    let optimized = EnsureRequirements::new().optimize(plan, &config)?;
    // SanityCheckPlan must pass
    SanityCheckPlan::new().optimize(Arc::clone(&optimized), &config)?;
    Ok(optimized)
}

/// Helper: verify idempotency — running twice produces the same plan
fn assert_idempotent(plan: Arc<dyn ExecutionPlan>) {
    let config = test_config();
    let p1 = EnsureRequirements::new()
        .optimize(plan, &config)
        .expect("first optimize failed");
    let p2 = EnsureRequirements::new()
        .optimize(Arc::clone(&p1), &config)
        .expect("second optimize failed");

    let s1 = plan_string(&p1);
    let s2 = plan_string(&p2);
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

/// Single-column `LexOrdering` on `(name, idx)` with the given options.
/// Most tests in this file want a one-column ordering on the canonical
/// `a@0` or `b@1` columns; this helper trims the 7-line per-test
/// boilerplate down to a single call.
fn sort_expr_on(
    name: &str,
    idx: usize,
    descending: bool,
    nulls_first: bool,
) -> LexOrdering {
    LexOrdering::new(vec![PhysicalSortExpr::new(
        Arc::new(Column::new(name, idx)),
        SortOptions {
            descending,
            nulls_first,
        },
    )])
    .unwrap()
}

/// Render an execution plan with `displayable(...).indent(true)`.
fn plan_string(plan: &Arc<dyn ExecutionPlan>) -> String {
    datafusion_physical_plan::displayable(plan.as_ref())
        .indent(true)
        .to_string()
}

/// Run `EnsureRequirements`, assert `SanityCheckPlan` passes, snapshot the
/// resulting plan with `insta`, and verify idempotency by running the rule a
/// second time and checking the plan is unchanged. Use this for plan-shape
/// tests so a single call covers "correct plan + sanity + idempotent" and
/// updating an intentional plan change is a single `cargo insta accept`.
macro_rules! assert_ensure_requirements_plan {
    ($plan:expr, @ $snapshot:literal $(,)?) => {{
        let config = test_config();
        let p1 = EnsureRequirements::new()
            .optimize($plan, &config)
            .expect("EnsureRequirements::optimize failed (pass 1)");
        SanityCheckPlan::new()
            .optimize(Arc::clone(&p1), &config)
            .expect("SanityCheckPlan failed (pass 1)");
        let p1_str = plan_string(&p1);
        insta::assert_snapshot!(p1_str, @ $snapshot);

        // Idempotency: a second pass must produce the same plan.
        let p2 = EnsureRequirements::new()
            .optimize(p1, &config)
            .expect("EnsureRequirements::optimize failed (pass 2)");
        let p2_str = plan_string(&p2);
        assert_eq!(
            p1_str, p2_str,
            "EnsureRequirements is NOT idempotent!\nPass 1:\n{p1_str}\nPass 2:\n{p2_str}",
        );
        SanityCheckPlan::new()
            .optimize(p2, &config)
            .expect("SanityCheckPlan failed (pass 2)");
    }};
}

/// Union with mixed partition counts + sort + limit.
#[test]
fn test_union_mixed_partitions_sort_limit() {
    let live = Arc::new(MockMultiPartitionExec::new(32));
    let historical = Arc::new(MockMultiPartitionExec::new(1));

    let union = UnionExec::try_new(vec![live as _, historical as _]).unwrap();

    let sort_expr = sort_expr_on("a", 0, true, true);

    let sort = Arc::new(SortExec::new(sort_expr, union));
    let limit: Arc<dyn ExecutionPlan> = Arc::new(GlobalLimitExec::new(sort, 0, Some(21)));

    assert_ensure_requirements_plan!(limit, @r"
    GlobalLimitExec: skip=0, fetch=21
      SortPreservingMergeExec: [a@0 DESC]
        UnionExec
          SortExec: expr=[a@0 DESC], preserve_partitioning=[true]
            MockMultiPartitionExec
          SortExec: expr=[a@0 DESC], preserve_partitioning=[false]
            MockMultiPartitionExec
    ");
}

/// Idempotency: union with mixed partitions
#[test]
fn test_idempotent_union_mixed_partitions() {
    let live = Arc::new(MockMultiPartitionExec::new(8));
    let hist = Arc::new(MockMultiPartitionExec::new(1));
    let union = UnionExec::try_new(vec![live as _, hist as _]).unwrap();

    let sort_expr = sort_expr_on("a", 0, true, true);

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
    let projection = Arc::new(ProjectionExec::try_new(proj_exprs, source as _).unwrap());

    let sort_expr = sort_expr_on("a", 0, true, true);

    let sort = Arc::new(SortExec::new(sort_expr, projection));
    let limit: Arc<dyn ExecutionPlan> = Arc::new(GlobalLimitExec::new(sort, 0, Some(21)));

    assert_ensure_requirements_plan!(limit, @r"
    GlobalLimitExec: skip=0, fetch=21
      SortPreservingMergeExec: [a@0 DESC]
        SortExec: expr=[a@0 DESC], preserve_partitioning=[true]
          ProjectionExec: expr=[a@0 as a, b@1 as b]
            MockMultiPartitionExec
    ");
}

// ========================================================================
// Single partition tests (no unnecessary operators)
// ========================================================================

/// Single partition source + sort + limit should NOT add SortPreservingMergeExec.
#[test]
fn test_single_partition_no_unnecessary_spm() {
    let source = Arc::new(MockMultiPartitionExec::new(1));

    let sort_expr = sort_expr_on("a", 0, true, true);

    let sort = Arc::new(SortExec::new(sort_expr, source));
    let limit: Arc<dyn ExecutionPlan> = Arc::new(GlobalLimitExec::new(sort, 0, Some(10)));

    // Snapshot asserts the plan-shape property: no `SortPreservingMergeExec`
    // is added on a single-partition source.
    assert_ensure_requirements_plan!(limit, @r"
    GlobalLimitExec: skip=0, fetch=10
      SortExec: expr=[a@0 DESC], preserve_partitioning=[false]
        MockMultiPartitionExec
    ");
}

/// Source already has correct ordering → should not add SortExec.
#[test]
fn test_sort_already_satisfied_no_extra_sort() {
    let source = Arc::new(MockMultiPartitionExec::new(1));

    // Sort ASC matches MockMultiPartitionExec's output ordering (a ASC)
    let sort_expr = sort_expr_on("a", 0, false, false);

    let sort = Arc::new(SortExec::new(sort_expr, source));
    let limit: Arc<dyn ExecutionPlan> = Arc::new(GlobalLimitExec::new(sort, 0, Some(10)));

    // Snapshot asserts the plan-shape property: no `SortExec` is added
    // when the source already satisfies the ordering.
    assert_ensure_requirements_plan!(limit, @r"
    GlobalLimitExec: skip=0, fetch=10
      MockMultiPartitionExec
    ");
}

// ========================================================================
// Various partition counts (stress test)
// ========================================================================

/// Test with different partition counts: 2, 4, 8, 16, 32, 64
#[test]
fn test_various_partition_counts_all_pass_sanity_check() {
    for n in [2, 4, 8, 16, 32, 64] {
        let source = Arc::new(MockMultiPartitionExec::new(n));

        let sort_expr = sort_expr_on("a", 0, true, true);

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

// ========================================================================
// CoalescePartitionsExec tests
// ========================================================================

/// CoalescePartitionsExec + sort should produce valid plan
#[test]
fn test_coalesce_then_sort_limit() {
    let source = Arc::new(MockMultiPartitionExec::new(8));
    let coalesce: Arc<dyn ExecutionPlan> = Arc::new(CoalescePartitionsExec::new(source));

    let sort_expr = sort_expr_on("a", 0, true, true);

    let sort = Arc::new(SortExec::new(sort_expr, coalesce));
    let limit: Arc<dyn ExecutionPlan> = Arc::new(GlobalLimitExec::new(sort, 0, Some(10)));

    assert_ensure_requirements_plan!(limit, @r"
    GlobalLimitExec: skip=0, fetch=10
      SortPreservingMergeExec: [a@0 DESC]
        SortExec: expr=[a@0 DESC], preserve_partitioning=[true]
          MockMultiPartitionExec
    ");
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

    let sort_expr = sort_expr_on("a", 0, true, true);

    let sort = Arc::new(SortExec::new(sort_expr, filter));
    let limit: Arc<dyn ExecutionPlan> = Arc::new(GlobalLimitExec::new(sort, 0, Some(21)));

    assert_ensure_requirements_plan!(limit, @r"
    GlobalLimitExec: skip=0, fetch=21
      SortPreservingMergeExec: [a@0 DESC]
        SortExec: expr=[a@0 DESC], preserve_partitioning=[true]
          FilterExec: true
            MockMultiPartitionExec
    ");
}

// ========================================================================
// RepartitionExec tests
// ========================================================================

/// Existing RepartitionExec + sort + limit must remain valid
#[test]
fn test_repartition_sort_limit_idempotent() {
    let source = Arc::new(MockMultiPartitionExec::new(1));
    let repartition = Arc::new(
        RepartitionExec::try_new(source as _, Partitioning::RoundRobinBatch(8)).unwrap(),
    );

    let sort_expr = sort_expr_on("a", 0, true, true);

    let sort = Arc::new(SortExec::new(sort_expr, repartition));
    let limit: Arc<dyn ExecutionPlan> = Arc::new(GlobalLimitExec::new(sort, 0, Some(10)));

    assert_ensure_requirements_plan!(limit, @r"
    GlobalLimitExec: skip=0, fetch=10
      SortExec: expr=[a@0 DESC], preserve_partitioning=[false]
        MockMultiPartitionExec
    ");
}

// ========================================================================
// Skip + Fetch (offset + limit) tests
// ========================================================================

/// GlobalLimitExec with skip=5, fetch=10 must produce valid plan
#[test]
fn test_skip_and_fetch_multi_partition() {
    let source = Arc::new(MockMultiPartitionExec::new(16));

    let sort_expr = sort_expr_on("a", 0, true, true);

    let sort = Arc::new(SortExec::new(sort_expr, source));
    // skip=5, fetch=10
    let limit: Arc<dyn ExecutionPlan> = Arc::new(GlobalLimitExec::new(sort, 5, Some(10)));

    assert_ensure_requirements_plan!(limit, @r"
    GlobalLimitExec: skip=5, fetch=10
      SortPreservingMergeExec: [a@0 DESC]
        SortExec: expr=[a@0 DESC], preserve_partitioning=[true]
          MockMultiPartitionExec
    ");
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
    let limit: Arc<dyn ExecutionPlan> = Arc::new(GlobalLimitExec::new(sort, 0, Some(21)));

    assert_ensure_requirements_plan!(limit, @r"
    GlobalLimitExec: skip=0, fetch=21
      SortPreservingMergeExec: [a@0 DESC, b@1 ASC NULLS LAST]
        SortExec: expr=[a@0 DESC, b@1 ASC NULLS LAST], preserve_partitioning=[true]
          MockMultiPartitionExec
    ");
}

// ========================================================================
// pushdown_sorts distribution-awareness regression tests.
// These cover the specific bug where pushdown_sorts pushed a SortExec
// through an intermediate node onto a multi-partition source, setting
// preserve_partitioning=true without inserting SortPreservingMergeExec.
// ========================================================================

/// Regression: `OutputRequirementExec(SinglePartition)` wrapping a
/// multi-partition source. `ensure_sorting` must insert a
/// `SortPreservingMergeExec` to satisfy the `SinglePartition` requirement.
///
/// The final `ensure_distribution` pass catches the distribution
/// violation from `pushdown_sorts`, producing a valid plan (via
/// `CoalescePartitionsExec` or `SortPreservingMergeExec`).
#[test]
fn test_output_requirement_single_partition_over_multi_partition_source() {
    let source = Arc::new(MockMultiPartitionExec::new(10));

    let sort_expr = sort_expr_on("a", 0, true, true);

    // OutputRequirementExec with SinglePartition + ordering requirement
    let output_req: Arc<dyn ExecutionPlan> = Arc::new(OutputRequirementExec::new(
        source,
        Some(OrderingRequirements::from(sort_expr)),
        Distribution::SinglePartition,
        Some(21),
    ));

    // SinglePartition must be satisfied (via SPM or Coalesce+Sort) — snapshot
    // documents which one the optimizer chooses.
    assert_ensure_requirements_plan!(output_req, @r"
    OutputRequirementExec: order_by=[(a@0, desc)], dist_by=SinglePartition
      SortPreservingMergeExec: [a@0 DESC], fetch=21
        SortExec: TopK(fetch=21), expr=[a@0 DESC], preserve_partitioning=[true]
          MockMultiPartitionExec
    ");
}

/// Regression: `pushdown_sorts` pushes a sort through a `ProjectionExec`
/// onto a multi-partition source. The result must include a
/// `SortPreservingMergeExec` (or equivalent) when the parent requires
/// `SinglePartition`.
///
/// Without the distribution-aware pushdown the standalone
/// `pushdown_sorts` traversal would not propagate distribution; the
/// final `ensure_distribution` pass then catches the violation and
/// inserts a `CoalescePartitionsExec` to satisfy `SinglePartition`.
#[test]
fn test_sort_pushdown_through_projection_adds_spm() {
    let source: Arc<dyn ExecutionPlan> = Arc::new(MockMultiPartitionExec::new(10));

    // Identity projection
    let proj_exprs: Vec<(Arc<dyn PhysicalExpr>, String)> = vec![
        (Arc::new(Column::new("a", 0)), "a".to_string()),
        (Arc::new(Column::new("b", 1)), "b".to_string()),
    ];
    let projection: Arc<dyn ExecutionPlan> =
        Arc::new(ProjectionExec::try_new(proj_exprs, source).unwrap());

    let sort_expr = sort_expr_on("a", 0, true, true);

    // OutputRequirementExec(SinglePartition) → ProjectionExec → multi-partition source
    let output_req: Arc<dyn ExecutionPlan> = Arc::new(OutputRequirementExec::new(
        projection,
        Some(OrderingRequirements::from(sort_expr)),
        Distribution::SinglePartition,
        Some(21),
    ));

    // SinglePartition must be satisfied. The final ensure_distribution pass
    // adds CoalescePartitionsExec or SortPreservingMergeExec as needed — the
    // snapshot documents which.
    assert_ensure_requirements_plan!(output_req, @r"
    OutputRequirementExec: order_by=[(a@0, desc)], dist_by=SinglePartition
      SortPreservingMergeExec: [a@0 DESC], fetch=21
        SortExec: TopK(fetch=21), expr=[a@0 DESC], preserve_partitioning=[true]
          ProjectionExec: expr=[a@0 as a, b@1 as b]
            MockMultiPartitionExec
    ");
}

// ========================================================================
// Idempotency tests for distribution-fix scenarios
// These verify that the pushdown_sorts distribution fix actually
// makes EnsureRequirements idempotent for the bug-triggering topologies.
// ========================================================================

/// Idempotency for the `OutputRequirementExec(SinglePartition)` +
/// multi-partition source scenario. Running twice must produce the same plan.
#[test]
fn test_idempotent_output_requirement_single_partition() {
    let source = Arc::new(MockMultiPartitionExec::new(10));
    let sort_expr = sort_expr_on("a", 0, true, true);

    let output_req: Arc<dyn ExecutionPlan> = Arc::new(OutputRequirementExec::new(
        source,
        Some(OrderingRequirements::from(sort_expr)),
        Distribution::SinglePartition,
        Some(21),
    ));

    assert_idempotent(output_req);
}

/// Idempotency for the `OutputRequirementExec(SinglePartition)` →
/// `ProjectionExec` → multi-partition source scenario.
#[test]
fn test_idempotent_projection_over_multi_partition_with_single_partition_requirement() {
    let source: Arc<dyn ExecutionPlan> = Arc::new(MockMultiPartitionExec::new(10));
    let proj_exprs: Vec<(Arc<dyn PhysicalExpr>, String)> = vec![
        (Arc::new(Column::new("a", 0)), "a".to_string()),
        (Arc::new(Column::new("b", 1)), "b".to_string()),
    ];
    let projection: Arc<dyn ExecutionPlan> =
        Arc::new(ProjectionExec::try_new(proj_exprs, source).unwrap());

    let sort_expr = sort_expr_on("a", 0, true, true);

    let output_req: Arc<dyn ExecutionPlan> = Arc::new(OutputRequirementExec::new(
        projection,
        Some(OrderingRequirements::from(sort_expr)),
        Distribution::SinglePartition,
        Some(21),
    ));

    assert_idempotent(output_req);
}

/// Regression for #21973 (the issue this PR fixes).
///
/// Topology: `SPM → Sort(preserve=true) → multi-partition`. Under the old
/// two-rule pipeline `EnforceSorting::pushdown_sorts` could mutate
/// `preserve_partitioning` after `EnforceDistribution` had settled
/// distribution, so pass 2 could regress this parallel plan into a serial
/// one. This is the exact topology that caused
/// [`test_pushdown_through_spm`](../enforce_sorting.rs) to fail before this
/// PR; `EnsureRequirements` must keep the SPM-over-parallel-sort shape
/// stable across passes.
///
/// Also acts as the "no extra SPM when already optimal" check — the
/// input plan already contains exactly one `SortPreservingMergeExec`,
/// so the optimised plan must too (we used to have a separate test for
/// this property, but on this input it is implied by idempotency
/// combined with the SPM-count assertion).
#[test]
fn test_issue_21973_idempotent_spm_sort_multi_partition() {
    let source: Arc<dyn ExecutionPlan> = Arc::new(MockMultiPartitionExec::new(10));
    let sort_expr = sort_expr_on("a", 0, true, true);

    let sort = Arc::new(
        SortExec::new(sort_expr.clone(), source).with_preserve_partitioning(true),
    );
    let spm: Arc<dyn ExecutionPlan> =
        Arc::new(SortPreservingMergeExec::new(sort_expr, sort));
    let limit: Arc<dyn ExecutionPlan> = Arc::new(GlobalLimitExec::new(spm, 0, Some(21)));

    // No-extra-SPM property: count SortPreservingMergeExec occurrences in
    // the first optimisation pass — must be ≤ 1 (the original SPM survives,
    // none are added).
    let config = test_config();
    let optimized = EnsureRequirements::new()
        .optimize(Arc::clone(&limit), &config)
        .expect("optimize failed");
    let plan_str = plan_string(&optimized);
    let spm_count = plan_str.matches("SortPreservingMergeExec").count();
    assert!(
        spm_count <= 1,
        "Extra SortPreservingMergeExec added ({spm_count} found):\n{plan_str}"
    );

    assert_idempotent(limit);
}

/// Regression for #21973: the `parallelize_sorts` rewrite path.
///
/// Input: `Sort(DESC) ← CoalescePartitionsExec ← multi-partition`. The first
/// pass must rewrite this into a parallel plan
/// `SortPreservingMergeExec ← Sort(preserve=true) ← multi-partition`, and
/// subsequent passes must keep that parallel shape. Under the old two-rule
/// pipeline `pushdown_sorts` could regress this back into a serial sort.
///
/// Runs 3 passes (one more than the standard idempotency check) and asserts
/// the parallel plan structure survives each one.
#[test]
fn test_issue_21973_parallel_sort_survives_multiple_passes() {
    let source: Arc<dyn ExecutionPlan> = Arc::new(MockMultiPartitionExec::new(8));
    let coalesce: Arc<dyn ExecutionPlan> = Arc::new(CoalescePartitionsExec::new(source));

    let sort_expr = sort_expr_on("a", 0, true, true);
    let sort: Arc<dyn ExecutionPlan> = Arc::new(SortExec::new(sort_expr, coalesce));
    let limit: Arc<dyn ExecutionPlan> = Arc::new(GlobalLimitExec::new(sort, 0, Some(10)));

    let config = test_config();

    let p1 = EnsureRequirements::new()
        .optimize(Arc::clone(&limit), &config)
        .expect("pass 1");
    let s1 = plan_string(&p1);

    let p2 = EnsureRequirements::new()
        .optimize(Arc::clone(&p1), &config)
        .expect("pass 2");
    let s2 = plan_string(&p2);

    let p3 = EnsureRequirements::new()
        .optimize(Arc::clone(&p2), &config)
        .expect("pass 3");
    let s3 = plan_string(&p3);

    // The parallel-sort shape must appear after pass 1 and survive every
    // subsequent pass. Specifically: SortPreservingMergeExec on top of a
    // Sort with preserve_partitioning=true (no CoalescePartitionsExec).
    for (i, plan_str) in [&s1, &s2, &s3].iter().enumerate() {
        assert!(
            plan_str.contains("SortPreservingMergeExec"),
            "pass {} regressed to serial: missing SortPreservingMergeExec:\n{plan_str}",
            i + 1
        );
        assert!(
            plan_str.contains("preserve_partitioning=[true]"),
            "pass {} regressed to serial: Sort lost preserve_partitioning=true (#21973):\n{plan_str}",
            i + 1
        );
        assert!(
            !plan_str.contains("CoalescePartitionsExec"),
            "pass {} regressed to serial: CoalescePartitionsExec re-introduced (#21973):\n{plan_str}",
            i + 1
        );
    }

    assert_eq!(
        s1, s2,
        "not idempotent between pass 1 and 2 (#21973):\n{s1}\nvs\n{s2}"
    );
    assert_eq!(
        s2, s3,
        "not idempotent between pass 2 and 3 (#21973):\n{s2}\nvs\n{s3}"
    );

    // All passes must produce sanity-checkable plans.
    for (i, p) in [p1, p2, p3].into_iter().enumerate() {
        SanityCheckPlan::new()
            .optimize(p, &config)
            .unwrap_or_else(|e| {
                panic!("SanityCheckPlan failed on pass {}: {e:?}", i + 1)
            });
    }
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

    let sort_expr = sort_expr_on("a", 0, false, false);

    let sort: Arc<dyn ExecutionPlan> = Arc::new(SortExec::new(sort_expr, partial_agg));
    let limit: Arc<dyn ExecutionPlan> = Arc::new(GlobalLimitExec::new(sort, 0, Some(10)));

    assert_idempotent(limit);
}

/// Stress test: idempotency with ALL partition counts from 1 to 64
#[test]
fn test_idempotent_all_partition_counts_1_to_64() {
    for n in 1..=64 {
        let source = Arc::new(MockMultiPartitionExec::new(n));
        let sort_expr = sort_expr_on("a", 0, true, true);
        let sort = Arc::new(SortExec::new(sort_expr, source));
        let limit: Arc<dyn ExecutionPlan> =
            Arc::new(GlobalLimitExec::new(sort, 0, Some(10)));
        assert_idempotent(limit);
    }
}

/// Regression for #14150: the standalone distribution enforcement path
/// lost `fetch` when applied twice. Verify `EnsureRequirements`
/// preserves fetch across multiple passes.
#[test]
fn test_issue_14150_fetch_survives_multiple_passes() {
    // Simulate: SELECT * FROM multi_partition_table ORDER BY a LIMIT 5
    // with target_partitions > 1 (triggers RepartitionExec)
    let source: Arc<dyn ExecutionPlan> = Arc::new(MockMultiPartitionExec::new(1));
    let repartition = Arc::new(
        RepartitionExec::try_new(source, Partitioning::RoundRobinBatch(4)).unwrap(),
    );

    let sort_expr = sort_expr_on("a", 0, false, true);

    let sort = Arc::new(SortExec::new(sort_expr, repartition as _).with_fetch(Some(5)));
    let limit: Arc<dyn ExecutionPlan> = Arc::new(GlobalLimitExec::new(sort, 0, Some(5)));

    let config = test_config();

    // Pass 1
    let p1 = EnsureRequirements::new()
        .optimize(Arc::clone(&limit), &config)
        .unwrap();
    let s1 = plan_string(&p1);

    // Pass 2
    let p2 = EnsureRequirements::new()
        .optimize(Arc::clone(&p1), &config)
        .unwrap();
    let s2 = plan_string(&p2);

    // Pass 3
    let p3 = EnsureRequirements::new()
        .optimize(Arc::clone(&p2), &config)
        .unwrap();
    let s3 = plan_string(&p3);

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

/// Sharper #14150 reproduce: input plan already contains a
/// `SortPreservingMergeExec` with an explicit `fetch`, sitting directly
/// above a `SortExec(fetch=…)` on a multi-partition source. This is the
/// exact shape that originally triggered the bug — the old
/// `EnforceDistribution::optimize` path would call
/// `remove_dist_changing_operators()` on this SPM, strip it, and then
/// `add_merge_on_top()` re-create an SPM **without** copying the saved
/// `fetch`. Pass 2 saw an SPM with no fetch and #14150 silently bit.
///
/// `EnsureRequirements` preserves the `fetch` value across every pass.
/// Note: it may legitimately deduplicate the `fetch` field between
/// adjacent operators (e.g. push it onto the surrounding
/// `GlobalLimitExec` and drop it from the SPM), so this test asserts
/// the #14150 property — \"`fetch=5` must appear somewhere in the
/// plan after every pass\" — rather than byte-identical idempotency
/// (which is covered by `test_issue_14150_fetch_survives_multiple_passes`
/// on the more realistic input shape where the SPM is inserted by the
/// optimizer itself).
#[test]
fn test_issue_14150_fetch_survives_with_input_spm() {
    let source: Arc<dyn ExecutionPlan> = Arc::new(MockMultiPartitionExec::new(4));

    let sort_expr = sort_expr_on("a", 0, false, true);

    // Sort with fetch=5 (TopK).
    let sort = Arc::new(
        SortExec::new(sort_expr.clone(), Arc::clone(&source)).with_fetch(Some(5)),
    );

    // SPM with fetch=5 above the sort — this is what `EnforceDistribution`
    // used to strip and re-add without `fetch`.
    let spm: Arc<dyn ExecutionPlan> =
        Arc::new(SortPreservingMergeExec::new(sort_expr, sort).with_fetch(Some(5)));

    let limit: Arc<dyn ExecutionPlan> = Arc::new(GlobalLimitExec::new(spm, 0, Some(5)));

    let config = test_config();

    let p1 = EnsureRequirements::new()
        .optimize(Arc::clone(&limit), &config)
        .unwrap();
    let s1 = plan_string(&p1);

    let p2 = EnsureRequirements::new()
        .optimize(Arc::clone(&p1), &config)
        .unwrap();
    let s2 = plan_string(&p2);

    // The #14150 property: `fetch=5` must survive both passes (the
    // historical bug was that pass 2 dropped it when the SPM got
    // re-created in `add_merge_on_top`).
    assert!(s1.contains("fetch=5"), "fetch=5 lost after pass 1:\n{s1}");
    assert!(
        s2.contains("fetch=5"),
        "fetch=5 lost after pass 2 (#14150 regression):\n{s2}"
    );
}

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
    let coalesce: Arc<dyn ExecutionPlan> = Arc::new(CoalescePartitionsExec::new(source));

    let sort_expr = sort_expr_on("a", 0, true, true);

    // Sort without preserve_partitioning on top of coalesced input
    let sort: Arc<dyn ExecutionPlan> = Arc::new(SortExec::new(sort_expr, coalesce));
    let limit: Arc<dyn ExecutionPlan> = Arc::new(GlobalLimitExec::new(sort, 0, Some(10)));

    // First pass should parallelize the sort (Sort+SPM replaces Coalesce+Sort)
    let config = test_config();
    let p1 = EnsureRequirements::new()
        .optimize(Arc::clone(&limit), &config)
        .expect("first optimize failed");
    let p2 = EnsureRequirements::new()
        .optimize(Arc::clone(&p1), &config)
        .expect("second optimize failed");

    let s1 = plan_string(&p1);
    let s2 = plan_string(&p2);
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

    let sort_expr = sort_expr_on("a", 0, true, true);

    let sort = Arc::new(SortExec::new(sort_expr, join));
    let limit: Arc<dyn ExecutionPlan> = Arc::new(GlobalLimitExec::new(sort, 0, Some(50)));

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

    let sort_expr = sort_expr_on("a", 0, true, true);

    let sort = Arc::new(SortExec::new(sort_expr, window_like));
    let limit: Arc<dyn ExecutionPlan> = Arc::new(GlobalLimitExec::new(sort, 0, Some(20)));

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
    let inner_sort: Arc<dyn ExecutionPlan> =
        Arc::new(SortExec::new(sort_expr_on("a", 0, true, true), source));
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
    let outer_sort: Arc<dyn ExecutionPlan> =
        Arc::new(SortExec::new(sort_expr_on("a", 0, true, true), projection));
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

    let sort_expr = sort_expr_on("a", 0, true, true);

    let sort: Arc<dyn ExecutionPlan> = Arc::new(SortExec::new(sort_expr, repartition));
    let limit: Arc<dyn ExecutionPlan> = Arc::new(GlobalLimitExec::new(sort, 0, Some(15)));

    assert_idempotent(limit);
}

/// EnsureRequirements applied twice on a HashJoinExec plan must produce
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

    let config = test_config();
    let p1 = EnsureRequirements::new()
        .optimize(Arc::clone(&join), &config)
        .expect("first EnsureRequirements pass failed");
    let p2 = EnsureRequirements::new()
        .optimize(Arc::clone(&p1), &config)
        .expect("second EnsureRequirements pass failed");

    let s1 = plan_string(&p1);
    let s2 = plan_string(&p2);

    assert_eq!(
        s1, s2,
        "EnsureRequirements not idempotent for HashJoinExec!\nPass 1:\n{s1}\nPass 2:\n{s2}"
    );
}

/// Idempotency on a complex plan:
/// `GlobalLimitExec → SortExec → ProjectionExec → UnionExec(multi, single)`.
/// The union + projection + sort topology has historically been a fertile
/// ground for non-idempotent behaviour, so we keep it as a separate
/// idempotency test — `assert_idempotent` already proves `f(f(x)) == f(x)`,
/// which for a deterministic optimiser is equivalent to stability across
/// any finite number of passes (the previous 10x sweep was overkill).
#[test]
fn test_idempotent_union_projection_sort() {
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

    let sort_expr = sort_expr_on("a", 0, true, true);

    let sort = Arc::new(SortExec::new(sort_expr, projection));
    let plan: Arc<dyn ExecutionPlan> = Arc::new(GlobalLimitExec::new(sort, 0, Some(21)));

    assert_idempotent(plan);
}
