// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: See the NOTICE file distributed with this work for additional information regarding copyright ownership
// SPDX-FileContributor: Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements

use std::sync::Arc;

use crate::physical_optimizer::test_utils::{parquet_exec, schema, sort_exec, sort_expr};

use arrow::array::{cast::AsArray, record_batch, types::Int32Type};
use datafusion::datasource::memory::MemorySourceConfig;
use datafusion::datasource::source::DataSourceExec;
use datafusion::prelude::SessionContext;
use datafusion_common::config::ConfigOptions;
use datafusion_expr::physical_planning_context::{ScalarSubqueryResults, SubqueryIndex};
use datafusion_physical_expr_common::sort_expr::LexOrdering;
use datafusion_physical_optimizer::PhysicalAnalyzerRule;
use datafusion_physical_optimizer::analyzer::PhysicalAnalyzer;
use datafusion_physical_optimizer::optimizer::PhysicalOptimizer;
use datafusion_physical_optimizer::output_requirements::AddOutputRequirements;
use datafusion_physical_plan::scalar_subquery::{ScalarSubqueryExec, ScalarSubqueryLink};
use datafusion_physical_plan::sorts::sort_preserving_merge::SortPreservingMergeExec;
use datafusion_physical_plan::{ExecutionPlan, collect, displayable, get_plan_string};

/// `AddOutputRequirements` must be idempotent: re-applying it to
/// its own output must not stack additional `OutputRequirementExec` wrappers.
///
/// AQE (datafusion-ballista#1359) re-runs the optimizer chain after every
/// completed stage; without this guarantee, every replan adds another wrapper.
#[test]
fn add_output_requirements_is_idempotent_on_bare_scan() {
    // Exercises the path where `require_top_ordering_helper` returns
    // `is_changed = false` and the rule adds a default (empty-requirement)
    // wrapper.
    assert_add_output_requirements_idempotent(parquet_exec(schema()));
}

#[test]
fn add_output_requirements_is_idempotent_on_sorted_plan() {
    // Exercises the path where the helper recognizes a top-level `SortExec`
    // and produces a wrapper carrying that ordering requirement
    // (`is_changed = true` branch).
    let s = schema();
    let ordering: LexOrdering = [sort_expr("a", &s)].into();
    let plan = sort_exec(ordering, parquet_exec(Arc::clone(&s)));
    assert_add_output_requirements_idempotent(plan);
}

#[test]
fn add_output_requirements_is_idempotent_on_scalar_subquery() {
    // Exercises the below-root case: the wrapper carrying the ordering lands
    // under the `ScalarSubqueryExec`, so the root guard in `require_top_ordering`
    // does not fire on the second pass. Without treating the existing wrapper as
    // already-handled, the second pass would stamp a redundant empty wrapper on
    // top of the subquery.
    let s = schema();
    let ordering: LexOrdering = [sort_expr("a", &s)].into();
    let sort = sort_exec(ordering, parquet_exec(Arc::clone(&s)));

    let subqueries = vec![ScalarSubqueryLink {
        plan: parquet_exec(Arc::clone(&s)),
        index: SubqueryIndex::new(0),
    }];
    let plan = Arc::new(ScalarSubqueryExec::new(
        sort,
        subqueries,
        ScalarSubqueryResults::new(1),
    )) as Arc<dyn ExecutionPlan>;

    assert_add_output_requirements_idempotent(plan);
}

fn assert_add_output_requirements_idempotent(plan: Arc<dyn ExecutionPlan>) {
    let config = ConfigOptions::new();
    let rule = AddOutputRequirements::new();

    let once = rule
        .analyze(plan, &config)
        .expect("first analyze pass should succeed");
    let twice = rule
        .analyze(Arc::clone(&once), &config)
        .expect("second analyze pass should succeed");

    assert_eq!(
        get_plan_string(&once),
        get_plan_string(&twice),
        "second invocation of AddOutputRequirements mutated the plan",
    );
}

/// For a `ScalarSubqueryExec` root, `require_top_ordering_helper` descends
/// through the main input (child 0) and wraps the global `SortExec` with an
/// `OutputRequirementExec` carrying its ordering, leaving the subquery child
/// untouched. Without this, the multi-child root is skipped and the query's
/// global ORDER BY requirement is lost.
#[test]
fn require_top_ordering_descends_through_scalar_subquery() {
    let s = schema();
    let ordering: LexOrdering = [sort_expr("a", &s)].into();
    let sort = sort_exec(ordering, parquet_exec(Arc::clone(&s)));

    // A subquery child makes `children.len() == 2`, exercising the multi-child path.
    let subqueries = vec![ScalarSubqueryLink {
        plan: parquet_exec(Arc::clone(&s)),
        index: SubqueryIndex::new(0),
    }];
    let plan = Arc::new(ScalarSubqueryExec::new(
        sort,
        subqueries,
        ScalarSubqueryResults::new(1),
    )) as Arc<dyn ExecutionPlan>;

    let optimized = AddOutputRequirements::new()
        .analyze(plan, &ConfigOptions::new())
        .expect("analyze should succeed");

    insta::assert_snapshot!(
        displayable(optimized.as_ref()).indent(true).to_string(),
        @r"
    ScalarSubqueryExec: subqueries=1
      OutputRequirementExec: order_by=[(a@0, asc)], dist_by=SinglePartition
        SortExec: expr=[a@0 ASC], preserve_partitioning=[false]
          DataSourceExec: file_groups={1 group: [[x]]}, projection=[a, b, c, d, e], file_type=parquet
      DataSourceExec: file_groups={1 group: [[x]]}, projection=[a, b, c, d, e], file_type=parquet
    ");
}

/// A `ScalarSubqueryExec` plan root must preserve its main input's global
/// ordering end to end.
///
/// The main input is a `SortPreservingMergeExec` over a two-partition ordered
/// source — the shape federated/custom planners hand to the optimizer: an
/// order-preserving merge with no `SortExec` above it. `OutputRequirements`
/// records the global ORDER BY under the multi-child subquery root, the rest of
/// the pipeline keeps the merge, and executing the optimized plan returns the
/// rows in global order regardless of how the source is partitioned.
#[tokio::test]
async fn scalar_subquery_root_preserves_global_ordering_end_to_end() {
    // Two partitions, each already sorted on `a`. Global order requires a sort-preserving merge;
    // a plain concatenation would interleave them as 1, 3, 5, 7, 2, 4, 6, 8.
    let p1 = record_batch!(("a", Int32, [1, 3, 5, 7])).expect("build partition 1 batch");
    let p2 = record_batch!(("a", Int32, [2, 4, 6, 8])).expect("build partition 2 batch");
    let schema = p1.schema();
    let ordering: LexOrdering = [sort_expr("a", &schema)].into();
    let source = DataSourceExec::from_data_source(
        MemorySourceConfig::try_new(&[vec![p1], vec![p2]], Arc::clone(&schema), None)
            .expect("build memory source config")
            .try_with_sort_information(vec![ordering.clone()])
            .expect("attach sort information to source"),
    );
    // The main plan establishes the query's global ordering via an `SortPreservingMergeExec` over the two sorted partitions.
    let main_input = Arc::new(SortPreservingMergeExec::new(ordering, source));

    // Dummy subquery that returns a single row
    let sq_batch = record_batch!(("v", Int32, [42])).expect("build subquery batch");
    let subquery = MemorySourceConfig::try_new_exec(
        &[vec![sq_batch.clone()]],
        sq_batch.schema(),
        None,
    )
    .expect("build subquery exec");

    let plan = Arc::new(ScalarSubqueryExec::new(
        main_input,
        vec![ScalarSubqueryLink {
            plan: subquery,
            index: SubqueryIndex::new(0),
        }],
        ScalarSubqueryResults::new(1),
    )) as Arc<dyn ExecutionPlan>;

    // Run the full default physical pipeline: the analyzer rules, then the
    // optimizer rules.
    let mut config = ConfigOptions::new();
    config.execution.target_partitions = 4;
    let mut optimized = plan;
    for rule in PhysicalAnalyzer::new().rules {
        optimized = rule
            .analyze(optimized, &config)
            .unwrap_or_else(|e| panic!("analyzer rule {} failed: {e}", rule.name()));
    }
    for rule in PhysicalOptimizer::new().rules {
        optimized = rule
            .optimize(optimized, &config)
            .unwrap_or_else(|e| panic!("optimizer rule {} failed: {e}", rule.name()));
    }

    // The executed rows come back in global order: the two sorted partitions
    // are merged into 1, 2, 3, 4, 5, 6, 7, 8.
    let batches = collect(optimized, SessionContext::new().task_ctx())
        .await
        .expect("execute optimized plan");
    let values: Vec<i32> = batches
        .iter()
        .flat_map(|b| {
            b.column(0)
                .as_primitive::<Int32Type>()
                .values()
                .iter()
                .copied()
        })
        .collect();
    assert_eq!(values, vec![1, 2, 3, 4, 5, 6, 7, 8]);
}
