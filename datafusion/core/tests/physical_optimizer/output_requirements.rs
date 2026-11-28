// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: See the NOTICE file distributed with this work for additional information regarding copyright ownership
// SPDX-FileContributor: Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements

use std::sync::Arc;

use crate::physical_optimizer::test_utils::{
    filter_exec, parquet_exec, projection_exec, schema, sort_exec, sort_exec_with_fetch,
    sort_expr, sort_expr_options,
};

use arrow::array::{cast::AsArray, record_batch, types::Int32Type};
use arrow::compute::SortOptions;
use datafusion::datasource::memory::MemorySourceConfig;
use datafusion::datasource::source::DataSourceExec;
use datafusion::prelude::{SessionConfig, SessionContext};
use datafusion_common::config::ConfigOptions;
use datafusion_expr::Operator;
use datafusion_expr::physical_planning_context::{ScalarSubqueryResults, SubqueryIndex};
use datafusion_physical_expr::ScalarFunctionExpr;
use datafusion_physical_expr::expressions::{binary, col, lit};
use datafusion_physical_expr_common::sort_expr::{LexOrdering, PhysicalSortExpr};
use datafusion_physical_optimizer::PhysicalOptimizerRule;
use datafusion_physical_optimizer::optimizer::PhysicalOptimizer;
use datafusion_physical_optimizer::output_requirements::OutputRequirements;
use datafusion_physical_optimizer::sanity_checker::check_plan_tree_sanity;
use datafusion_physical_plan::limit::GlobalLimitExec;
use datafusion_physical_plan::projection::ProjectionExec;
use datafusion_physical_plan::scalar_subquery::{ScalarSubqueryExec, ScalarSubqueryLink};
use datafusion_physical_plan::sorts::sort_preserving_merge::SortPreservingMergeExec;
use datafusion_physical_plan::{
    ExecutionPlan, ExecutionPlanProperties, collect, displayable, get_plan_string,
};

/// `OutputRequirements::new_add_mode()` must be idempotent: re-applying it to
/// its own output must not stack additional `OutputRequirementExec` wrappers.
///
/// AQE (datafusion-ballista#1359) re-runs the optimizer chain after every
/// completed stage; without this guarantee, every replan adds another wrapper.
#[test]
fn add_mode_is_idempotent_on_bare_scan() {
    // Exercises the path where `require_top_ordering_helper` returns
    // `is_changed = false` and the rule adds a default (empty-requirement)
    // wrapper.
    assert_add_mode_idempotent(parquet_exec(schema()));
}

#[test]
fn add_mode_is_idempotent_on_sorted_plan() {
    // Exercises the path where the helper recognizes a top-level `SortExec`
    // and produces a wrapper carrying that ordering requirement
    // (`is_changed = true` branch).
    let s = schema();
    let ordering: LexOrdering = [sort_expr("a", &s)].into();
    let plan = sort_exec(ordering, parquet_exec(Arc::clone(&s)));
    assert_add_mode_idempotent(plan);
}

#[test]
fn add_mode_is_idempotent_on_scalar_subquery() {
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

    assert_add_mode_idempotent(plan);
}

fn assert_add_mode_idempotent(plan: Arc<dyn ExecutionPlan>) {
    let config = ConfigOptions::new();
    let rule = OutputRequirements::new_add_mode();

    let once = rule
        .optimize(plan, &config)
        .expect("first add-mode optimize pass should succeed");
    let twice = rule
        .optimize(Arc::clone(&once), &config)
        .expect("second add-mode optimize pass should succeed");

    assert_eq!(
        get_plan_string(&once),
        get_plan_string(&twice),
        "second invocation of OutputRequirements::new_add_mode mutated the plan",
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

    let optimized = OutputRequirements::new_add_mode()
        .optimize(plan, &ConfigOptions::new())
        .expect("add-mode optimize should succeed");

    insta::assert_snapshot!(
        displayable(optimized.as_ref()).indent(true).to_string(),
        @r"
    OutputRequirementExec: order_by=[(a@0, asc)], dist_by=SinglePartition
      ScalarSubqueryExec: subqueries=1
        OutputRequirementExec: order_by=[(a@0, asc)], dist_by=Unspecified
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

    // Run the full default physical optimizer pipeline.
    let mut config = ConfigOptions::new();
    config.execution.target_partitions = 4;
    let mut optimized = plan;
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

/// A computed projection over a global sort, the plan `DataFrame::sort(..)
/// .select(..)` builds, must keep the sort's order in its output
/// (<https://github.com/apache/datafusion/issues/26862>).
///
/// The projection benefits from input partitioning, so the optimizer may run
/// it in parallel, but the result must then still be a single partition in
/// the sort's order. Checking the partition count and ordering of the plan
/// root makes the test deterministic: once the root has more than one
/// partition, the order of the collected rows depends on scheduling.
#[tokio::test]
async fn computed_projection_over_sort_preserves_global_ordering_end_to_end() {
    // One partition of several unsorted batches; `SortExec` will produce
    // multiple sorted batches because the batch size is 2.
    let batches = vec![
        record_batch!(("a", Int32, [3, 7])).expect("build batch"),
        record_batch!(("a", Int32, [1, 8])).expect("build batch"),
        record_batch!(("a", Int32, [5, 2])).expect("build batch"),
        record_batch!(("a", Int32, [6, 4])).expect("build batch"),
    ];
    let schema = batches[0].schema();
    let source = MemorySourceConfig::try_new_exec(&[batches], Arc::clone(&schema), None)
        .expect("build memory source");

    let a_desc = SortOptions {
        descending: true,
        nulls_first: false,
    };
    let sort = sort_exec([sort_expr_options("a", &schema, a_desc)].into(), source);
    let a = col("a", &schema).expect("column a");
    let a_plus_one =
        binary(Arc::clone(&a), Operator::Plus, lit(1i32), &schema).expect("a + 1");
    let plan = projection_exec(
        vec![(a, "a".to_string()), (a_plus_one, "a_plus_one".to_string())],
        sort,
    )
    .expect("build projection");

    let session_config = SessionConfig::new()
        .with_target_partitions(4)
        .with_batch_size(2);
    let optimized = optimize_checked(plan, &session_config);
    let plan_display = displayable(optimized.as_ref()).indent(true).to_string();

    assert_eq!(
        optimized.output_partitioning().partition_count(),
        1,
        "the root of the optimized plan must be a single partition to keep \
         the global order of the sort:\n{plan_display}"
    );
    let output_ordering = [sort_expr_options("a", &optimized.schema(), a_desc)];
    assert!(
        optimized
            .equivalence_properties()
            .ordering_satisfy(output_ordering)
            .expect("check root ordering"),
        "the root of the optimized plan must be ordered by a DESC:\n{plan_display}"
    );

    let ctx = SessionContext::new_with_config(session_config);
    let batches = collect(optimized, ctx.task_ctx())
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
    assert_eq!(values, vec![8, 7, 6, 5, 4, 3, 2, 1]);
}

/// The projection in
/// [`computed_projection_over_sort_preserves_global_ordering_end_to_end`] keeps
/// the sort key, so it can still run in parallel, with a merge on the key above
/// it restoring the order.
#[test]
fn computed_projection_keeping_sort_key_runs_in_parallel() {
    let (source, schema) = unsorted_four_batch_source();
    let sort = sort_exec([sort_expr_options("a", &schema, A_DESC)].into(), source);
    let a = col("a", &schema).expect("column a");
    let a_plus_one =
        binary(Arc::clone(&a), Operator::Plus, lit(1i32), &schema).expect("a + 1");
    let plan = projection_exec(
        vec![(a, "a".to_string()), (a_plus_one, "a_plus_one".to_string())],
        sort,
    )
    .expect("build projection");

    let optimized = optimize_checked(plan, &small_batch_config());
    let plan_display = displayable(optimized.as_ref()).indent(true).to_string();
    assert_eq!(
        projection_partition_count(&optimized),
        4,
        "the projection should run in parallel:\n{plan_display}"
    );
    assert!(
        plan_display.starts_with("SortPreservingMergeExec: [a@0 DESC NULLS LAST]"),
        "a merge on the sort key should restore the order:\n{plan_display}"
    );
}

/// Like [`computed_projection_over_sort_preserves_global_ordering_end_to_end`],
/// but the projection drops the sort key, as in `DataFrame::sort(a DESC)
/// .select(a + 1 AS c)`. The output has no column named by the sort, so the
/// order exists only in the sequence of rows and cannot be restored by merging
/// on the sort key above the projection.
#[tokio::test]
async fn key_dropping_projection_over_sort_preserves_global_ordering_end_to_end() {
    let batches = vec![
        record_batch!(("a", Int32, [3, 7])).expect("build batch"),
        record_batch!(("a", Int32, [1, 8])).expect("build batch"),
        record_batch!(("a", Int32, [5, 2])).expect("build batch"),
        record_batch!(("a", Int32, [6, 4])).expect("build batch"),
    ];
    let schema = batches[0].schema();
    let source = MemorySourceConfig::try_new_exec(&[batches], Arc::clone(&schema), None)
        .expect("build memory source");

    let a_desc = SortOptions {
        descending: true,
        nulls_first: false,
    };
    let sort = sort_exec([sort_expr_options("a", &schema, a_desc)].into(), source);
    let a_plus_one = binary(
        col("a", &schema).expect("column a"),
        Operator::Plus,
        lit(1i32),
        &schema,
    )
    .expect("a + 1");
    let plan = projection_exec(vec![(a_plus_one, "c".to_string())], sort)
        .expect("build projection");

    let session_config = SessionConfig::new()
        .with_target_partitions(4)
        .with_batch_size(2);
    let optimized = optimize_checked(plan, &session_config);
    let plan_display = displayable(optimized.as_ref()).indent(true).to_string();

    // The root has no ordering to check (it has no column `a`), and a single
    // root partition alone is not enough: a plan that drops the sort and
    // coalesces would pass. So check the path from the root down to the
    // operator that establishes the order.
    assert!(
        single_ordered_partition_below(&optimized, &[("a", a_desc)]),
        "every operator from the root down to one ordered by a DESC must \
         produce a single partition and keep its input order:\n{plan_display}"
    );

    let ctx = SessionContext::new_with_config(session_config);
    let batches = collect(optimized, ctx.task_ctx())
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
    assert_eq!(values, vec![9, 8, 7, 6, 5, 4, 3, 2]);
}

/// Whether `plan` and every operator below it, down to the first whose output
/// is ordered by `ordering`, given as column names, produce a single partition
/// and keep their input order. The rows of that one ordered partition then
/// reach the root in order, whatever the root's schema.
fn single_ordered_partition_below(
    plan: &Arc<dyn ExecutionPlan>,
    ordering: &[(&str, SortOptions)],
) -> bool {
    let mut node = Arc::clone(plan);
    loop {
        if node.output_partitioning().partition_count() != 1 {
            return false;
        }
        let schema = node.schema();
        let sort_exprs: Option<Vec<_>> = ordering
            .iter()
            .map(|(name, options)| {
                col(name, &schema)
                    .ok()
                    .map(|expr| PhysicalSortExpr::new(expr, *options))
            })
            .collect();
        if let Some(sort_exprs) = sort_exprs
            && node
                .equivalence_properties()
                .ordering_satisfy(sort_exprs)
                .expect("check ordering")
        {
            return true;
        }
        let children = node.children();
        if children.len() != 1 || !node.maintains_input_order()[0] {
            return false;
        }
        node = Arc::clone(children[0]);
    }
}

/// Like [`key_dropping_projection_over_sort_preserves_global_ordering_end_to_end`],
/// with `LIMIT 4 OFFSET 1` in the plan shape `DataFrame::sort(a DESC)
/// .select(a + 1 AS c).limit(1, Some(4))` builds: the limit is pushed below the
/// projection and into the sort as a fetch.
#[tokio::test]
async fn key_dropping_projection_over_limited_sort_preserves_global_ordering_end_to_end()
{
    let (source, schema) = unsorted_four_batch_source();
    let sort = sort_exec_with_fetch(
        [sort_expr_options("a", &schema, A_DESC)].into(),
        Some(5),
        source,
    );
    let limit = Arc::new(GlobalLimitExec::new(sort, 1, Some(4)));
    let plan = a_plus_one_projection(limit, "c");

    let session_config = small_batch_config();
    let optimized = optimize_checked(plan, &session_config);
    let plan_display = displayable(optimized.as_ref()).indent(true).to_string();

    assert!(
        single_ordered_partition_below(&optimized, &[("a", A_DESC)]),
        "every operator from the root down to one ordered by a DESC must \
         produce a single partition and keep its input order:\n{plan_display}"
    );
    assert_eq!(collect_first_column(optimized).await, vec![8, 7, 6, 5]);
}

/// Without a global ordering to keep, a computed projection over a single
/// partition must still get a round-robin repartition to run in parallel,
/// whether its input is unordered or ordered by the source. Guards the
/// regression tests above against passing only because the inputs are too
/// small to repartition.
#[test]
fn computed_projection_without_required_ordering_runs_in_parallel() {
    let unordered = record_batch!(("a", Int32, [3, 7, 1, 8, 5, 2, 6, 4])).expect("batch");
    let schema = unordered.schema();
    let unordered_source =
        MemorySourceConfig::try_new_exec(&[vec![unordered]], Arc::clone(&schema), None)
            .expect("build memory source");

    let ordered = record_batch!(("a", Int32, [8, 7, 6, 5, 4, 3, 2, 1])).expect("batch");
    let ordered_source = DataSourceExec::from_data_source(
        MemorySourceConfig::try_new(&[vec![ordered]], Arc::clone(&schema), None)
            .expect("build memory source config")
            .try_with_sort_information(vec![
                [sort_expr_options("a", &schema, A_DESC)].into(),
            ])
            .expect("attach sort information"),
    );

    for source in [unordered_source, ordered_source as Arc<dyn ExecutionPlan>] {
        let plan = a_plus_one_projection(source, "c");
        let optimized = optimize_checked(plan, &small_batch_config());
        let plan_display = displayable(optimized.as_ref()).indent(true).to_string();
        assert_eq!(
            optimized.output_partitioning().partition_count(),
            4,
            "the projection should run in parallel:\n{plan_display}"
        );
        assert!(
            plan_display.contains("RepartitionExec: partitioning=RoundRobinBatch(4)"),
            "the projection should run over a round-robin repartition:\n{plan_display}"
        );
    }
}

/// A limit above a projection over a sort selects rows by the sort's order and
/// returns them in it, whether the projection keeps the sort key or drops it.
#[tokio::test]
async fn limit_over_projection_over_sort_preserves_global_ordering_end_to_end() {
    for keep_key in [true, false] {
        let (source, schema) = unsorted_four_batch_source();
        let sort = sort_exec([sort_expr_options("a", &schema, A_DESC)].into(), source);
        let projection = if keep_key {
            let a = col("a", &schema).expect("column a");
            let a_plus_one = binary(Arc::clone(&a), Operator::Plus, lit(1i32), &schema)
                .expect("a + 1");
            projection_exec(
                vec![(a, "a".to_string()), (a_plus_one, "c".to_string())],
                sort,
            )
            .expect("build projection")
        } else {
            a_plus_one_projection(sort, "c")
        };
        let plan = Arc::new(GlobalLimitExec::new(projection, 1, Some(4)));

        let optimized = optimize_checked(plan, &small_batch_config());
        let plan_display = displayable(optimized.as_ref()).indent(true).to_string();
        assert!(
            single_ordered_partition_below(&optimized, &[("a", A_DESC)])
                || (keep_key
                    && root_ordered_single_partition(&optimized, &[("a", A_DESC)])),
            "the root must be a single partition in the order of a DESC:\n{plan_display}"
        );
        let expected = if keep_key {
            vec![7, 6, 5, 4]
        } else {
            vec![8, 7, 6, 5]
        };
        assert_eq!(collect_columns(optimized).await[0], expected);
    }
}

/// A filter, and a limit over a filter, above a sort keep the sort's order.
/// The filter benefits from input partitioning, like a computed projection.
#[tokio::test]
async fn filter_and_limit_over_sort_preserve_global_ordering_end_to_end() {
    for limit in [None, Some((1, 3))] {
        let (source, schema) = unsorted_four_batch_source();
        let sort = sort_exec([sort_expr_options("a", &schema, A_DESC)].into(), source);
        let predicate = binary(
            col("a", &schema).expect("column a"),
            Operator::Gt,
            lit(2i32),
            &schema,
        )
        .expect("a > 2");
        let mut plan = filter_exec(predicate, sort);
        if let Some((skip, fetch)) = limit {
            plan = Arc::new(GlobalLimitExec::new(plan, skip, Some(fetch)));
        }

        let optimized = optimize_checked(plan, &small_batch_config());
        let plan_display = displayable(optimized.as_ref()).indent(true).to_string();
        assert!(
            root_ordered_single_partition(&optimized, &[("a", A_DESC)]),
            "the root must be a single partition ordered by a DESC:\n{plan_display}"
        );
        let expected = if limit.is_some() {
            vec![7, 6, 5]
        } else {
            vec![8, 7, 6, 5, 4, 3]
        };
        assert_eq!(collect_columns(optimized).await[0], expected);
    }
}

/// A computed projection over a sort on `a DESC, b ASC` keeps the order of
/// both keys. When it keeps both keys the order is restored by merging on
/// them; when it drops `b`, merging on `a` alone could reorder rows with equal
/// `a`, so the order of `s = a + b` among them shows that the sort's order
/// is kept.
#[tokio::test]
async fn multi_key_sort_order_survives_computed_projection_end_to_end() {
    let a_desc_b_asc = [("a", A_DESC), ("b", SortOptions::default())];
    for keep_b in [true, false] {
        let batches = vec![
            record_batch!(("a", Int32, [3, 1]), ("b", Int32, [2, 3])).expect("batch"),
            record_batch!(("a", Int32, [2, 3]), ("b", Int32, [1, 1])).expect("batch"),
            record_batch!(("a", Int32, [1, 2]), ("b", Int32, [1, 2])).expect("batch"),
            record_batch!(("a", Int32, [3, 1]), ("b", Int32, [3, 2])).expect("batch"),
        ];
        let schema = batches[0].schema();
        let source =
            MemorySourceConfig::try_new_exec(&[batches], Arc::clone(&schema), None)
                .expect("build memory source");
        let ordering = LexOrdering::new(
            a_desc_b_asc
                .iter()
                .map(|(name, options)| sort_expr_options(name, &schema, *options)),
        )
        .expect("non-empty ordering");
        let sort = sort_exec(ordering, source);
        let a = col("a", &schema).expect("column a");
        let b = col("b", &schema).expect("column b");
        let s = binary(Arc::clone(&a), Operator::Plus, Arc::clone(&b), &schema)
            .expect("a + b");
        let exprs = if keep_b {
            vec![
                (b, "b".to_string()),
                (a, "a".to_string()),
                (s, "s".to_string()),
            ]
        } else {
            vec![(a, "a".to_string()), (s, "s".to_string())]
        };
        let plan = projection_exec(exprs, sort).expect("build projection");

        let optimized = optimize_checked(plan, &small_batch_config());
        let plan_display = displayable(optimized.as_ref()).indent(true).to_string();
        let ordered = if keep_b {
            root_ordered_single_partition(&optimized, &a_desc_b_asc)
        } else {
            single_ordered_partition_below(&optimized, &a_desc_b_asc)
        };
        assert!(
            ordered,
            "the rows must reach the root in the order of a DESC, b ASC:\n{plan_display}"
        );
        let columns = collect_columns(optimized).await;
        let (a, s) = if keep_b {
            (&columns[1], &columns[2])
        } else {
            (&columns[0], &columns[1])
        };
        assert_eq!(a, &vec![3, 3, 3, 2, 2, 1, 1, 1]);
        assert_eq!(s, &vec![4, 5, 6, 3, 4, 2, 3, 4]);
    }
}

/// A merge on a volatile sort key would evaluate it again and could reorder
/// the rows, so a computed projection or a filter over a sort on `random()`
/// runs in the single partition of the sort, like a projection that drops the
/// sort key.
#[test]
fn operators_over_volatile_sort_run_in_sort_partition() {
    for filter in [false, true] {
        let (source, schema) = unsorted_four_batch_source();
        let random = ScalarFunctionExpr::try_new(
            datafusion::functions::math::random(),
            vec![],
            &schema,
            Arc::new(ConfigOptions::new()),
        )
        .expect("random()");
        let sort = sort_exec(
            [PhysicalSortExpr::new(
                Arc::new(random),
                SortOptions::default(),
            )]
            .into(),
            source,
        );
        let a = col("a", &schema).expect("column a");
        let plan = if filter {
            let predicate = binary(a, Operator::Gt, lit(2i32), &schema).expect("a > 2");
            filter_exec(predicate, sort)
        } else {
            a_plus_one_projection(sort, "c")
        };

        let optimized = optimize_checked(plan, &small_batch_config());
        let plan_display = displayable(optimized.as_ref()).indent(true).to_string();
        assert_eq!(
            optimized.output_partitioning().partition_count(),
            1,
            "the root should be the single partition of the sort:\n{plan_display}"
        );
        assert!(
            !plan_display.contains("RoundRobinBatch"),
            "no round robin should split the sorted rows:\n{plan_display}"
        );
    }
}

/// Whether `plan` is a single partition ordered by `ordering`, given as column
/// names of its schema.
fn root_ordered_single_partition(
    plan: &Arc<dyn ExecutionPlan>,
    ordering: &[(&str, SortOptions)],
) -> bool {
    let schema = plan.schema();
    plan.output_partitioning().partition_count() == 1
        && plan
            .equivalence_properties()
            .ordering_satisfy(ordering.iter().map(|(name, options)| {
                PhysicalSortExpr::new(col(name, &schema).expect("column"), *options)
            }))
            .expect("check ordering")
}

/// The number of output partitions of the first `ProjectionExec` in `plan`.
fn projection_partition_count(plan: &Arc<dyn ExecutionPlan>) -> usize {
    if plan.is::<ProjectionExec>() {
        return plan.output_partitioning().partition_count();
    }
    plan.children()
        .into_iter()
        .map(projection_partition_count)
        .find(|count| *count > 0)
        .unwrap_or(0)
}

async fn collect_columns(plan: Arc<dyn ExecutionPlan>) -> Vec<Vec<i32>> {
    let ctx = SessionContext::new_with_config(small_batch_config());
    let batches = collect(plan, ctx.task_ctx())
        .await
        .expect("execute optimized plan");
    let num_columns = batches.first().map_or(0, |b| b.num_columns());
    (0..num_columns)
        .map(|idx| {
            batches
                .iter()
                .flat_map(|b| {
                    b.column(idx)
                        .as_primitive::<Int32Type>()
                        .values()
                        .iter()
                        .copied()
                        .collect::<Vec<_>>()
                })
                .collect()
        })
        .collect()
}

const A_DESC: SortOptions = SortOptions {
    descending: true,
    nulls_first: false,
};

fn small_batch_config() -> SessionConfig {
    SessionConfig::new()
        .with_target_partitions(4)
        .with_batch_size(2)
}

/// One partition of several unsorted batches of a single `Int32` column `a`.
fn unsorted_four_batch_source() -> (Arc<dyn ExecutionPlan>, arrow::datatypes::SchemaRef) {
    let batches = vec![
        record_batch!(("a", Int32, [3, 7])).expect("build batch"),
        record_batch!(("a", Int32, [1, 8])).expect("build batch"),
        record_batch!(("a", Int32, [5, 2])).expect("build batch"),
        record_batch!(("a", Int32, [6, 4])).expect("build batch"),
    ];
    let schema = batches[0].schema();
    let source = MemorySourceConfig::try_new_exec(&[batches], Arc::clone(&schema), None)
        .expect("build memory source");
    (source, schema)
}

/// `ProjectionExec: a + 1 AS <alias>`, dropping `a`.
fn a_plus_one_projection(
    input: Arc<dyn ExecutionPlan>,
    alias: &str,
) -> Arc<dyn ExecutionPlan> {
    let schema = input.schema();
    let a_plus_one = binary(
        col("a", &schema).expect("column a"),
        Operator::Plus,
        lit(1i32),
        &schema,
    )
    .expect("a + 1");
    projection_exec(vec![(a_plus_one, alias.to_string())], input)
        .expect("build projection")
}

async fn collect_first_column(plan: Arc<dyn ExecutionPlan>) -> Vec<i32> {
    let ctx = SessionContext::new_with_config(small_batch_config());
    collect(plan, ctx.task_ctx())
        .await
        .expect("execute optimized plan")
        .iter()
        .flat_map(|b| {
            b.column(0)
                .as_primitive::<Int32Type>()
                .values()
                .iter()
                .copied()
        })
        .collect()
}

/// Optimizes `plan` with the default physical optimizer rules and checks what
/// the physical planner checks: rules that must keep the schema keep it, and
/// the result passes [`check_plan_tree_sanity`].
///
/// Also re-optimizes the result, as adaptive execution does, until it no
/// longer changes. Re-optimizing may find a simpler plan (a limit over a sort
/// takes two more runs to become a `TopK` without this change too), but each
/// run must keep the partitioning and ordering of the output.
fn optimize_checked(
    plan: Arc<dyn ExecutionPlan>,
    config: &SessionConfig,
) -> Arc<dyn ExecutionPlan> {
    let schema = plan.schema();
    let optimized = optimize_once(plan, config);
    assert_eq!(optimized.schema(), schema, "optimizing changed the schema");
    let mut previous = Arc::clone(&optimized);
    for _ in 0..3 {
        let reoptimized = optimize_once(Arc::clone(&previous), config);
        assert_eq!(
            reoptimized.output_partitioning().partition_count(),
            optimized.output_partitioning().partition_count(),
            "re-optimizing changed the output partitioning"
        );
        assert_eq!(
            reoptimized.output_ordering(),
            optimized.output_ordering(),
            "re-optimizing changed the output ordering"
        );
        if get_plan_string(&reoptimized) == get_plan_string(&previous) {
            return optimized;
        }
        previous = reoptimized;
    }
    panic!(
        "re-optimizing did not reach a fixed point:\n{}",
        displayable(previous.as_ref()).indent(true)
    );
}

fn optimize_once(
    mut plan: Arc<dyn ExecutionPlan>,
    config: &SessionConfig,
) -> Arc<dyn ExecutionPlan> {
    for rule in PhysicalOptimizer::new().rules {
        let schema = plan.schema();
        plan = rule
            .optimize(plan, config.options())
            .unwrap_or_else(|e| panic!("optimizer rule {} failed: {e}", rule.name()));
        if rule.schema_check() {
            assert_eq!(
                plan.schema(),
                schema,
                "optimizer rule {} changed the schema",
                rule.name()
            );
        }
    }
    check_plan_tree_sanity(&plan, &config.options().optimizer)
        .expect("optimized plan passes the sanity check");
    plan
}
