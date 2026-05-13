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

//! Comprehensive tests for `EnsureRequirements` covering production edge
//! cases: hash joins, aggregates, projections, window-like operators,
//! fetch preservation, and idempotency.

use std::sync::Arc;

use arrow::compute::SortOptions;
use arrow::datatypes::{DataType, Field, Schema};
use datafusion_common::config::ConfigOptions;
use datafusion_common::tree_node::TreeNodeRecursion;
use datafusion_common::{JoinType, NullEquality, Result, ScalarValue};
use datafusion_physical_expr::expressions::{Column, Literal};
use datafusion_physical_expr::{
    Distribution, EquivalenceProperties, LexOrdering, PhysicalExpr, PhysicalSortExpr,
};
use datafusion_physical_expr_common::sort_expr::OrderingRequirements;
use datafusion_physical_plan::aggregates::{
    AggregateExec, AggregateMode, PhysicalGroupBy,
};
use datafusion_physical_plan::execution_plan::{Boundedness, EmissionType};
use datafusion_physical_plan::filter::FilterExec;
use datafusion_physical_plan::joins::{HashJoinExec, PartitionMode};
use datafusion_physical_plan::limit::GlobalLimitExec;
use datafusion_physical_plan::projection::{ProjectionExec, ProjectionExpr};
use datafusion_physical_plan::repartition::RepartitionExec;
use datafusion_physical_plan::sorts::sort::SortExec;
use datafusion_physical_plan::union::UnionExec;
use datafusion_physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, ExecutionPlanProperties, Partitioning,
    PlanProperties, SendableRecordBatchStream,
};

use crate::PhysicalOptimizerRule;
use crate::sanity_checker::SanityCheckPlan;

use super::EnsureRequirements;

// -----------------------------------------------------------------------
// Helpers
// -----------------------------------------------------------------------

fn test_schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("a", DataType::Int64, false),
        Field::new("b", DataType::Int64, false),
    ]))
}

fn sort_expr_col(name: &str, idx: usize, descending: bool) -> PhysicalSortExpr {
    PhysicalSortExpr::new(
        Arc::new(Column::new(name, idx)),
        SortOptions {
            descending,
            nulls_first: !descending,
        },
    )
}

fn plan_display(plan: &dyn ExecutionPlan) -> String {
    datafusion_physical_plan::displayable(plan)
        .indent(true)
        .to_string()
}

fn plan_contains(plan: &dyn ExecutionPlan, needle: &str) -> bool {
    plan_display(plan).contains(needle)
}

fn optimize_and_sanity_check(
    plan: Arc<dyn ExecutionPlan>,
) -> Result<Arc<dyn ExecutionPlan>> {
    let config = ConfigOptions::default();
    let optimized = EnsureRequirements::new().optimize(plan, &config)?;
    SanityCheckPlan::new().optimize(Arc::clone(&optimized), &config)?;
    Ok(optimized)
}

fn assert_idempotent(plan: Arc<dyn ExecutionPlan>) {
    let config = ConfigOptions::default();
    let p1 = EnsureRequirements::new()
        .optimize(plan, &config)
        .expect("first optimize failed");
    let p2 = EnsureRequirements::new()
        .optimize(Arc::clone(&p1), &config)
        .expect("second optimize failed");
    let s1 = plan_display(p1.as_ref());
    let s2 = plan_display(p2.as_ref());
    assert_eq!(s1, s2, "NOT idempotent!\nFirst:\n{s1}\nSecond:\n{s2}");
    SanityCheckPlan::new()
        .optimize(p1, &config)
        .expect("sanity p1");
    SanityCheckPlan::new()
        .optimize(p2, &config)
        .expect("sanity p2");
}

// -----------------------------------------------------------------------
// Mock ExecutionPlans
// -----------------------------------------------------------------------

/// Mock source with configurable partition count and output ordering [a ASC].
#[derive(Debug)]
struct MockExec {
    properties: Arc<PlanProperties>,
}

impl MockExec {
    fn new(partition_count: usize) -> Self {
        let schema = test_schema();
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
        Self {
            properties: Arc::new(PlanProperties::new(
                eq,
                Partitioning::UnknownPartitioning(partition_count),
                EmissionType::Incremental,
                Boundedness::Bounded,
            )),
        }
    }

    fn new_unordered(partition_count: usize) -> Self {
        let schema = test_schema();
        let eq = EquivalenceProperties::new(Arc::clone(&schema));
        Self {
            properties: Arc::new(PlanProperties::new(
                eq,
                Partitioning::UnknownPartitioning(partition_count),
                EmissionType::Incremental,
                Boundedness::Bounded,
            )),
        }
    }
}

impl DisplayAs for MockExec {
    fn fmt_as(
        &self,
        _t: DisplayFormatType,
        f: &mut std::fmt::Formatter,
    ) -> std::fmt::Result {
        write!(f, "MockExec")
    }
}

impl ExecutionPlan for MockExec {
    fn name(&self) -> &str {
        "MockExec"
    }
    fn properties(&self) -> &Arc<PlanProperties> {
        &self.properties
    }
    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![]
    }
    fn with_new_children(
        self: Arc<Self>,
        _c: Vec<Arc<dyn ExecutionPlan>>,
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
        _p: usize,
        _c: Arc<datafusion_execution::TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        unimplemented!()
    }
}

/// Mock operator requiring specific distribution and/or ordering.
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

// -----------------------------------------------------------------------
// Tests
// -----------------------------------------------------------------------

/// 4. HashJoinExec with sort + limit -- needs both distribution AND sorting.
#[test]
fn test_hash_join_distribution_and_sort() {
    let left: Arc<dyn ExecutionPlan> = Arc::new(MockExec::new(4));
    let right: Arc<dyn ExecutionPlan> = Arc::new(MockExec::new(4));
    let on = vec![(
        Arc::new(Column::new("a", 0)) as _,
        Arc::new(Column::new("a", 0)) as _,
    )];
    let join = Arc::new(
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
        .unwrap(),
    );
    let sort_expr = LexOrdering::new(vec![sort_expr_col("a", 0, true)]).unwrap();
    let sort = Arc::new(SortExec::new(sort_expr, join as _));
    let limit: Arc<dyn ExecutionPlan> =
        Arc::new(GlobalLimitExec::new(sort as _, 0, Some(100)));
    let optimized = optimize_and_sanity_check(limit).unwrap();
    assert!(plan_contains(optimized.as_ref(), "HashJoinExec"));
    assert_idempotent(optimized);
}

/// 5. AggregateExec with GROUP BY + sort -- needs repartition + sorting.
#[test]
fn test_aggregate_with_sort() {
    let source: Arc<dyn ExecutionPlan> = Arc::new(MockExec::new(8));
    let schema = source.schema();
    let gb = PhysicalGroupBy::new_single(vec![(
        Arc::new(Column::new("a", 0)) as _,
        "a".into(),
    )]);
    let partial = Arc::new(
        AggregateExec::try_new(
            AggregateMode::Partial,
            gb,
            vec![],
            vec![],
            source,
            Arc::clone(&schema),
        )
        .unwrap(),
    );
    let gb2 = PhysicalGroupBy::new_single(vec![(
        Arc::new(Column::new("a", 0)) as _,
        "a".into(),
    )]);
    let final_agg = Arc::new(
        AggregateExec::try_new(
            AggregateMode::FinalPartitioned,
            gb2,
            vec![],
            vec![],
            partial as _,
            Arc::clone(&schema),
        )
        .unwrap(),
    );
    let sort_expr = LexOrdering::new(vec![sort_expr_col("a", 0, true)]).unwrap();
    let sort = Arc::new(SortExec::new(sort_expr, final_agg as _));
    let limit: Arc<dyn ExecutionPlan> =
        Arc::new(GlobalLimitExec::new(sort as _, 0, Some(50)));
    let optimized = optimize_and_sanity_check(limit).unwrap();
    assert!(plan_contains(optimized.as_ref(), "RepartitionExec"));
    assert_idempotent(optimized);
}

/// 6. Idempotency: ProjectionExec over multi-partition.
#[test]
fn test_idempotent_projection_multi_partition() {
    let source: Arc<dyn ExecutionPlan> = Arc::new(MockExec::new(8));
    let proj = vec![
        ProjectionExpr {
            expr: Arc::new(Column::new("a", 0)),
            alias: "x".into(),
        },
        ProjectionExpr {
            expr: Arc::new(Column::new("b", 1)),
            alias: "y".into(),
        },
    ];
    let projection: Arc<dyn ExecutionPlan> =
        Arc::new(ProjectionExec::try_new(proj, source).unwrap());
    let sort_expr = LexOrdering::new(vec![sort_expr_col("x", 0, true)]).unwrap();
    let sort = Arc::new(SortExec::new(sort_expr, projection));
    assert_idempotent(Arc::new(GlobalLimitExec::new(sort as _, 0, Some(20))));
}

/// 7. Idempotency: plan that already has RepartitionExec.
#[test]
fn test_idempotent_with_existing_repartition() {
    let source: Arc<dyn ExecutionPlan> = Arc::new(MockExec::new(8));
    let repart = Arc::new(
        RepartitionExec::try_new(source, Partitioning::RoundRobinBatch(4)).unwrap(),
    );
    let sort_expr = LexOrdering::new(vec![sort_expr_col("a", 0, true)]).unwrap();
    let sort = Arc::new(SortExec::new(sort_expr, repart as _));
    assert_idempotent(Arc::new(GlobalLimitExec::new(sort as _, 0, Some(10))));
}

/// 8. Fetch preserved across passes (regression for #14150).
#[test]
fn test_fetch_preserved_across_passes_extended() {
    let source: Arc<dyn ExecutionPlan> = Arc::new(MockExec::new(8));
    let sort_expr = LexOrdering::new(vec![sort_expr_col("a", 0, true)]).unwrap();
    let sort = Arc::new(SortExec::new(sort_expr, source));
    let limit: Arc<dyn ExecutionPlan> =
        Arc::new(GlobalLimitExec::new(sort as _, 0, Some(5)));
    let optimized = optimize_and_sanity_check(limit).unwrap();
    assert!(plan_display(optimized.as_ref()).contains("fetch=5"));
    let config = ConfigOptions::default();
    let p2 = EnsureRequirements::new()
        .optimize(optimized, &config)
        .unwrap();
    assert!(plan_display(p2.as_ref()).contains("fetch=5"));
}

/// 9. Window function topology -- needs specific partitioning AND ordering.
#[test]
fn test_window_function_sort_and_distribution() {
    let source: Arc<dyn ExecutionPlan> = Arc::new(MockExec::new(8));
    let ord = LexOrdering::new(vec![
        sort_expr_col("a", 0, false),
        sort_expr_col("b", 1, false),
    ])
    .unwrap();
    let dist = Distribution::HashPartitioned(vec![Arc::new(Column::new("a", 0))]);
    let window_like: Arc<dyn ExecutionPlan> =
        Arc::new(MockReqExec::new(source, dist, Some(ord)));
    let optimized = optimize_and_sanity_check(window_like).unwrap();
    assert!(plan_contains(optimized.as_ref(), "RepartitionExec"));
    assert!(plan_contains(optimized.as_ref(), "SortExec"));
    assert_idempotent(optimized);
}

/// Sort with preserve_partitioning=true needs merge for GlobalLimit.
#[test]
fn test_preserve_partitioning_sort_needs_merge() {
    let source: Arc<dyn ExecutionPlan> = Arc::new(MockExec::new(8));
    let sort_expr = LexOrdering::new(vec![sort_expr_col("a", 0, true)]).unwrap();
    let sort =
        Arc::new(SortExec::new(sort_expr, source).with_preserve_partitioning(true));
    let limit: Arc<dyn ExecutionPlan> =
        Arc::new(GlobalLimitExec::new(sort as _, 0, Some(10)));
    let optimized = optimize_and_sanity_check(limit).unwrap();
    let d = plan_display(optimized.as_ref());
    assert!(
        d.contains("SortPreservingMergeExec") || d.contains("CoalescePartitionsExec"),
        "Missing merge.\nPlan:\n{d}",
    );
    assert_idempotent(optimized);
}

/// Deeply nested projections: Projection -> Projection -> sort -> limit.
#[test]
fn test_nested_projections_sort_limit() {
    let source: Arc<dyn ExecutionPlan> = Arc::new(MockExec::new(8));
    let p1 = vec![
        ProjectionExpr {
            expr: Arc::new(Column::new("a", 0)),
            alias: "x".into(),
        },
        ProjectionExpr {
            expr: Arc::new(Column::new("b", 1)),
            alias: "y".into(),
        },
    ];
    let proj1: Arc<dyn ExecutionPlan> =
        Arc::new(ProjectionExec::try_new(p1, source).unwrap());
    let p2 = vec![ProjectionExpr {
        expr: Arc::new(Column::new("x", 0)),
        alias: "z".into(),
    }];
    let proj2: Arc<dyn ExecutionPlan> =
        Arc::new(ProjectionExec::try_new(p2, proj1).unwrap());
    let sort_expr = LexOrdering::new(vec![sort_expr_col("z", 0, true)]).unwrap();
    let sort = Arc::new(SortExec::new(sort_expr, proj2));
    let optimized =
        optimize_and_sanity_check(Arc::new(GlobalLimitExec::new(sort as _, 0, Some(5))))
            .unwrap();
    assert_idempotent(optimized);
}

/// SinglePartition distribution from a multi-partition source.
#[test]
fn test_single_partition_requirement_from_multi_partition() {
    let source: Arc<dyn ExecutionPlan> = Arc::new(MockExec::new(8));
    let ord = LexOrdering::new(vec![sort_expr_col("a", 0, false)]).unwrap();
    let op: Arc<dyn ExecutionPlan> = Arc::new(MockReqExec::new(
        source,
        Distribution::SinglePartition,
        Some(ord),
    ));
    let optimized = optimize_and_sanity_check(op).unwrap();
    let d = plan_display(optimized.as_ref());
    assert!(
        d.contains("CoalescePartitionsExec") || d.contains("SortPreservingMergeExec"),
        "Must coalesce.\nPlan:\n{d}",
    );
    assert_idempotent(optimized);
}

/// Multiple unions stacked.
#[test]
fn test_nested_unions_sort_limit() {
    let u1 = UnionExec::try_new(vec![
        Arc::new(MockExec::new(4)) as _,
        Arc::new(MockExec::new(8)) as _,
    ])
    .unwrap();
    let u2 = UnionExec::try_new(vec![u1 as _, Arc::new(MockExec::new(1)) as _]).unwrap();
    let sort_expr = LexOrdering::new(vec![sort_expr_col("a", 0, true)]).unwrap();
    let sort = Arc::new(SortExec::new(sort_expr, u2 as _));
    let optimized =
        optimize_and_sanity_check(Arc::new(GlobalLimitExec::new(sort as _, 0, Some(15))))
            .unwrap();
    assert_idempotent(optimized);
}

/// HashJoinExec with asymmetric partition counts.
#[test]
fn test_hash_join_asymmetric_partitions() {
    let left: Arc<dyn ExecutionPlan> = Arc::new(MockExec::new(1));
    let right: Arc<dyn ExecutionPlan> = Arc::new(MockExec::new(16));
    let on = vec![(
        Arc::new(Column::new("a", 0)) as _,
        Arc::new(Column::new("a", 0)) as _,
    )];
    let join = Arc::new(
        HashJoinExec::try_new(
            left,
            right,
            on,
            None,
            &JoinType::Left,
            None,
            PartitionMode::Partitioned,
            NullEquality::NullEqualsNothing,
            false,
        )
        .unwrap(),
    );
    let sort_expr = LexOrdering::new(vec![sort_expr_col("a", 0, false)]).unwrap();
    let sort = Arc::new(SortExec::new(sort_expr, join as _));
    let optimized =
        optimize_and_sanity_check(Arc::new(GlobalLimitExec::new(sort as _, 0, Some(50))))
            .unwrap();
    assert_idempotent(optimized);
}

/// Skip > 0 and fetch preserved.
#[test]
fn test_skip_and_fetch_preserved() {
    let source: Arc<dyn ExecutionPlan> = Arc::new(MockExec::new(8));
    let sort_expr = LexOrdering::new(vec![sort_expr_col("a", 0, true)]).unwrap();
    let sort = Arc::new(SortExec::new(sort_expr, source));
    let limit: Arc<dyn ExecutionPlan> =
        Arc::new(GlobalLimitExec::new(sort as _, 10, Some(5)));
    let optimized = optimize_and_sanity_check(limit).unwrap();
    let d = plan_display(optimized.as_ref());
    assert!(d.contains("skip=10") && d.contains("fetch=5"));
    assert_idempotent(optimized);
}

/// Sort with fetch (TopK) on multi-partition source.
#[test]
fn test_sort_with_fetch_multi_partition() {
    let source: Arc<dyn ExecutionPlan> = Arc::new(MockExec::new(8));
    let sort_expr = LexOrdering::new(vec![sort_expr_col("a", 0, true)]).unwrap();
    let sort = Arc::new(SortExec::new(sort_expr, source).with_fetch(Some(10)));
    let optimized = optimize_and_sanity_check(sort as _).unwrap();
    assert_idempotent(optimized);
}

/// No-ordering source with no sort requested (just a limit).
#[test]
fn test_no_sort_just_limit() {
    let source: Arc<dyn ExecutionPlan> = Arc::new(MockExec::new_unordered(4));
    let limit: Arc<dyn ExecutionPlan> =
        Arc::new(GlobalLimitExec::new(source, 0, Some(10)));
    let optimized = optimize_and_sanity_check(limit).unwrap();
    assert_idempotent(optimized);
}

/// Hash-partitioned requirement with no ordering.
#[test]
fn test_hash_distribution_only_no_ordering() {
    let source: Arc<dyn ExecutionPlan> = Arc::new(MockExec::new(8));
    let dist = Distribution::HashPartitioned(vec![Arc::new(Column::new("a", 0))]);
    let op: Arc<dyn ExecutionPlan> = Arc::new(MockReqExec::new(source, dist, None));
    let optimized = optimize_and_sanity_check(op).unwrap();
    assert!(plan_contains(optimized.as_ref(), "RepartitionExec"));
    assert_idempotent(optimized);
}

/// FilterExec + sort + limit with sanity check and idempotency.
#[test]
fn test_filter_sort_limit_sanity_and_idempotent() {
    let source: Arc<dyn ExecutionPlan> = Arc::new(MockExec::new(8));
    let pred: Arc<dyn PhysicalExpr> =
        Arc::new(Literal::new(ScalarValue::Boolean(Some(true))));
    let filter: Arc<dyn ExecutionPlan> =
        Arc::new(FilterExec::try_new(pred, source).unwrap());
    let sort_expr = LexOrdering::new(vec![sort_expr_col("a", 0, true)]).unwrap();
    let sort = Arc::new(SortExec::new(sort_expr, filter));
    let optimized =
        optimize_and_sanity_check(Arc::new(GlobalLimitExec::new(sort as _, 0, Some(20))))
            .unwrap();
    assert_idempotent(optimized);
}

/// Projection over HashJoinExec + sort + limit.
#[test]
fn test_projection_over_hash_join_sort_limit() {
    let left: Arc<dyn ExecutionPlan> = Arc::new(MockExec::new(4));
    let right: Arc<dyn ExecutionPlan> = Arc::new(MockExec::new(4));
    let on = vec![(
        Arc::new(Column::new("a", 0)) as _,
        Arc::new(Column::new("a", 0)) as _,
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
        .unwrap(),
    );
    let s = join.schema();
    let proj: Vec<ProjectionExpr> = s
        .fields()
        .iter()
        .take(2)
        .enumerate()
        .map(|(i, f)| ProjectionExpr {
            expr: Arc::new(Column::new(f.name(), i)),
            alias: f.name().to_string(),
        })
        .collect();
    let projection: Arc<dyn ExecutionPlan> =
        Arc::new(ProjectionExec::try_new(proj, join).unwrap());
    let sort_expr = LexOrdering::new(vec![sort_expr_col("a", 0, true)]).unwrap();
    let sort = Arc::new(SortExec::new(sort_expr, projection));
    let optimized =
        optimize_and_sanity_check(Arc::new(GlobalLimitExec::new(sort as _, 0, Some(10))))
            .unwrap();
    assert_idempotent(optimized);
}

// =========================================================================
// EnforceDistribution idempotency tests
// Verify that running EnforceDistribution::optimize() twice produces
// identical plans (fixes #14150).
// =========================================================================

/// EnsureRequirements twice on multi-partition + sort + limit
#[test]
fn test_enforce_dist_idempotent_sort_limit() {
    use datafusion_physical_plan::sorts::sort_preserving_merge::SortPreservingMergeExec;

    let source: Arc<dyn ExecutionPlan> = Arc::new(MockExec::new(8));
    let sort_expr = LexOrdering::new(vec![sort_expr_col("a", 0, true)]).unwrap();
    let sort = Arc::new(
        SortExec::new(sort_expr.clone(), Arc::clone(&source))
            .with_preserve_partitioning(true),
    );
    let spm: Arc<dyn ExecutionPlan> =
        Arc::new(SortPreservingMergeExec::new(sort_expr, sort).with_fetch(Some(10)));
    let plan: Arc<dyn ExecutionPlan> = Arc::new(GlobalLimitExec::new(spm, 0, Some(10)));

    let config = ConfigOptions::default();
    let p1 = EnsureRequirements::new()
        .optimize(Arc::clone(&plan), &config)
        .unwrap();
    let p2 = EnsureRequirements::new()
        .optimize(Arc::clone(&p1), &config)
        .unwrap();

    let s1 = plan_display(p1.as_ref());
    let s2 = plan_display(p2.as_ref());
    assert_eq!(
        s1, s2,
        "EnsureRequirements not idempotent (sort+limit):\nPass 1:\n{s1}\nPass 2:\n{s2}"
    );
    // fetch must survive
    assert!(s2.contains("fetch=10"), "fetch lost:\n{s2}");
}

/// EnsureRequirements twice on hash join
#[test]
fn test_enforce_dist_idempotent_hash_join() {
    use datafusion_physical_plan::joins::HashJoinExec;
    use datafusion_physical_plan::joins::utils::JoinOn;

    let left: Arc<dyn ExecutionPlan> = Arc::new(MockExec::new(4));
    let right: Arc<dyn ExecutionPlan> = Arc::new(MockExec::new(4));
    let on: JoinOn = vec![(Arc::new(Column::new("a", 0)), Arc::new(Column::new("a", 0)))];
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
        .unwrap(),
    );

    let config = ConfigOptions::default();
    let p1 = EnsureRequirements::new()
        .optimize(Arc::clone(&join), &config)
        .unwrap();
    let p2 = EnsureRequirements::new()
        .optimize(Arc::clone(&p1), &config)
        .unwrap();

    let s1 = plan_display(p1.as_ref());
    let s2 = plan_display(p2.as_ref());
    assert_eq!(
        s1, s2,
        "EnsureRequirements not idempotent (hash join):\nPass 1:\n{s1}\nPass 2:\n{s2}"
    );
}

/// EnsureRequirements 10x on complex plan
#[test]
fn test_ensure_requirements_10x_stable() {
    let live = Arc::new(MockExec::new(16));
    let hist = Arc::new(MockExec::new(1));
    let union = UnionExec::try_new(vec![live as _, hist as _]).unwrap();

    let proj_exprs: Vec<ProjectionExpr> = vec![
        ProjectionExpr {
            expr: Arc::new(Column::new("a", 0)),
            alias: "a".to_string(),
        },
        ProjectionExpr {
            expr: Arc::new(Column::new("b", 1)),
            alias: "b".to_string(),
        },
    ];
    let projection = Arc::new(ProjectionExec::try_new(proj_exprs, union).unwrap());

    let sort_expr = LexOrdering::new(vec![sort_expr_col("a", 0, true)]).unwrap();
    let sort = Arc::new(SortExec::new(sort_expr, projection));
    let plan: Arc<dyn ExecutionPlan> = Arc::new(GlobalLimitExec::new(sort, 0, Some(21)));

    let config = ConfigOptions::default();
    let mut current = EnsureRequirements::new()
        .optimize(Arc::clone(&plan), &config)
        .unwrap();
    let first = plan_display(current.as_ref());

    for i in 2..=10 {
        current = EnsureRequirements::new()
            .optimize(Arc::clone(&current), &config)
            .unwrap();
        let s = plan_display(current.as_ref());
        assert_eq!(
            first, s,
            "Plan changed on pass {i}!\nFirst:\n{first}\nPass {i}:\n{s}"
        );
    }
    SanityCheckPlan::new()
        .optimize(current, &config)
        .expect("SanityCheckPlan failed after 10 passes");
}
