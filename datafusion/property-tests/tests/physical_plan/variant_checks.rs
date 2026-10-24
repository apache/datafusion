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

//! Tests of the checks that compare a node's output with variant runs:
//! rewritten copies of the node (`with_fetch_equivalent`,
//! `limit_pushdown_equivalent`) and runs with other batch sizes and batch
//! layouts (`batch_size_invariance`, `batch_boundary_invariance`).

use std::sync::Arc;

use arrow::compute::SortOptions;
use datafusion_physical_expr::expressions::col;
use datafusion_physical_expr::{LexOrdering, Partitioning, PhysicalSortExpr};
use datafusion_physical_plan::ExecutionPlan;
use datafusion_physical_plan::coalesce_partitions::CoalescePartitionsExec;
use datafusion_physical_plan::limit::GlobalLimitExec;
use datafusion_physical_plan::projection::ProjectionExec;
use datafusion_physical_plan::repartition::RepartitionExec;
use datafusion_physical_plan::sorts::sort::SortExec;
use datafusion_physical_plan::sorts::sort_preserving_merge::SortPreservingMergeExec;
use datafusion_physical_plan::union::UnionExec;
use datafusion_property_tests::physical_plan::fixtures::{
    BatchLayout, ConstantValues, SourceSpec,
};
use datafusion_property_tests::physical_plan::{
    CheckKind, PlanCheck, PlanChecker, Variant, oracle,
};
use datafusion_property_tests::{Finding, Report, Severity};

use crate::common::{
    ConfigurableExec, Effect, FetchMode, OnRerun, Transform, checker, checker_of,
    messages, schema, summary,
};

/// Run only the check named `name`
fn check_with(name: &str, plan: &Arc<dyn ExecutionPlan>) -> Report {
    checker(&[name]).check(plan).unwrap()
}

fn ordering_on_a() -> LexOrdering {
    LexOrdering::new(vec![PhysicalSortExpr::new_default(
        col("a", &schema()).unwrap(),
    )])
    .unwrap()
}

/// A source with one partition per entry of `rows` and a row id column, so
/// that every row is unique
fn rows_source(rows: &[usize]) -> Arc<dyn ExecutionPlan> {
    SourceSpec::new(schema())
        .with_partition_rows(rows)
        .with_row_ids(0)
        .build_arc()
        .unwrap()
}

/// Like [`rows_source`], with every partition sorted on `a`, which has many
/// ties
fn sorted_rows_source(rows: &[usize]) -> Arc<dyn ExecutionPlan> {
    SourceSpec::new(schema())
        .with_partition_rows(rows)
        .with_distinct_values(5)
        .with_ordering(ordering_on_a())
        .with_row_ids(0)
        .build_arc()
        .unwrap()
}

/// A node that applies its fetch correctly
fn fetching(input: Arc<dyn ExecutionPlan>, fetch: Option<usize>) -> ConfigurableExec {
    let mut exec = ConfigurableExec::new(input).effect(Effect::LowerEqual);
    exec.fetch = fetch;
    exec.fetch_mode = FetchMode::Enforce;
    exec
}

#[test]
fn correct_nodes_are_clean() {
    for input in [
        rows_source(&[100]),
        rows_source(&[40, 0, 60]),
        sorted_rows_source(&[50, 0, 70]),
    ] {
        // Nodes that pass their first rows through, and so support limit
        // pushdown
        let plans = [
            ConfigurableExec::new(Arc::clone(&input)),
            fetching(Arc::clone(&input), None),
            fetching(Arc::clone(&input), Some(5)),
        ];
        for mut exec in plans {
            exec.limit_pushdown = true;
            checker_of(&[CheckKind::Variant])
                .check(&exec.build())
                .unwrap()
                .assert_clean();
        }
    }
}

/// A node that passes its rows through in order, but reports that it does not
/// maintain their order, so that its properties do not show what
/// `limit_pushdown_missed` reports
fn unordered(input: Arc<dyn ExecutionPlan>) -> ConfigurableExec {
    let mut exec = ConfigurableExec::new(input);
    exec.maintains_order = Some(false);
    exec
}

#[test]
fn prefix_closed_node_without_limit_pushdown() {
    let report = check_with(
        "limit_pushdown_missed_at_runtime",
        &unordered(rows_source(&[40, 0, 60])).build(),
    );
    assert_eq!(
        messages(&report),
        vec![
            "supports_limit_pushdown() is false, but with every child limited to its \
             first n rows per partition, for n in [1, 7, 101], every output partition \
             has the first n rows of the same partition of the normal output, so a \
             limit above the node could be pushed to its children; return true from \
             supports_limit_pushdown if this holds for every input"
        ]
    );
    assert_eq!(
        summary(&report),
        vec![(Severity::Lint, "limit_pushdown_missed_at_runtime")]
    );

    // A node whose fetch is the only thing that drops rows, over input sorted
    // by an ordering it maintains, that reports GreaterEqual without its
    // fetch, which `limit_pushdown_missed` does not check
    let mut exec = fetching(sorted_rows_source(&[50, 0, 70]), Some(10));
    exec.effect_without_fetch = Some(Effect::GreaterEqual);
    assert_eq!(
        summary(&check_with(
            "limit_pushdown_missed_at_runtime",
            &exec.build()
        )),
        vec![(Severity::Lint, "limit_pushdown_missed_at_runtime")]
    );

    // A node whose properties show that it passes its rows through is reported
    // by `limit_pushdown_missed` instead
    let plan = ConfigurableExec::new(rows_source(&[40, 0, 60])).build();
    check_with("limit_pushdown_missed_at_runtime", &plan).assert_clean();
    assert_eq!(
        summary(&check_with("limit_pushdown_missed", &plan)),
        vec![(Severity::Lint, "limit_pushdown_missed")]
    );
}

#[test]
fn nodes_that_need_more_than_their_first_input_rows() {
    // The first nodes report cardinality_effect() Equal, falsely for most of
    // them, and that they do not maintain the order of their input, so that
    // only their output shows that they are not prefix-closed
    let mut skipping = unordered(rows_source(&[100]));
    skipping.skip = 5;
    let sort_on = |input| {
        Arc::new(SortExec::new(ordering_on_a(), input).with_preserve_partitioning(true))
            as Arc<dyn ExecutionPlan>
    };
    let plans = vec![
        // Keeps half of each batch, so with every partition limited to its first
        // n rows, a partition keeps fewer than the first n rows of its output
        unordered(rows_source(&[40, 0, 60]))
            .transform(Transform::DropHalf)
            .build(),
        unordered(rows_source(&[40, 0, 60]))
            .transform(Transform::Duplicate)
            .build(),
        skipping.build(),
        sort_on(rows_source(&[40, 0, 60])),
        // Sorting input that is already sorted passes it through
        sort_on(sorted_rows_source(&[40, 0, 60])),
        // Pass every row through, but report that they can drop or combine
        // rows, like a final aggregate whose input has each group once, or a
        // join
        unordered(rows_source(&[40, 0, 60]))
            .effect(Effect::LowerEqual)
            .build(),
        unordered(rows_source(&[40, 0, 60]))
            .effect(Effect::Unknown)
            .build(),
    ];
    for plan in plans {
        check_with("limit_pushdown_missed_at_runtime", &plan).assert_clean();
    }

    // Limits that remove no rows show nothing
    let plan = unordered(rows_source(&[1, 0, 1])).build();
    check_with("limit_pushdown_missed_at_runtime", &plan).assert_clean();

    // A hash repartition of input that is already hash partitioned the same
    // way leaves every row where it is, as if it passed its input through.
    // Neither these checks nor `maintains_input_order_missed` report it.
    let a = col("a", &schema()).unwrap();
    let partitioned = SourceSpec::new(schema())
        .with_hash_partitioning(vec![Arc::clone(&a)], 3, 100)
        .with_row_ids(0)
        .build_arc()
        .unwrap();
    let repartition: Arc<dyn ExecutionPlan> = Arc::new(
        RepartitionExec::try_new(
            partitioned,
            Partitioning::Hash(vec![Arc::clone(&a)], 3),
        )
        .unwrap(),
    );
    checker(&[
        "limit_pushdown_missed",
        "limit_pushdown_missed_at_runtime",
        "maintains_input_order_missed",
    ])
    .check(&repartition)
    .unwrap()
    .assert_clean();
    // So does an order preserving one of input with one value of the key per
    // partition, when the values hash to different partitions
    let constant_per_partition = SourceSpec::new(schema())
        .with_partition_rows(&[20, 0, 40])
        .with_constant("a", ConstantValues::PerPartition)
        .with_ordering(ordering_on_a())
        .with_row_ids(0)
        .build_arc()
        .unwrap();
    let repartition: Arc<dyn ExecutionPlan> = Arc::new(
        RepartitionExec::try_new(constant_per_partition, Partitioning::Hash(vec![a], 4))
            .unwrap()
            .with_preserve_order(),
    );
    checker(&["limit_pushdown_missed", "limit_pushdown_missed_at_runtime"])
        .check(&repartition)
        .unwrap()
        .assert_clean();
}

#[test]
fn fetch_that_keeps_too_many_rows() {
    let mut exec = fetching(rows_source(&[40, 0, 60]), None);
    exec.fetch_mode = FetchMode::KeepOneMore;
    let report = check_with("with_fetch_equivalent", &exec.build());
    assert_eq!(
        messages(&report),
        vec![
            "with_fetch(Some(1)): partition 0 has 2 rows, more than the limit of 1",
            "with_fetch(Some(7)): partition 0 has 8 rows, more than the limit of 7",
        ]
    );
    assert!(
        summary(&report)
            .iter()
            .all(|finding| *finding == (Severity::Invariant, "with_fetch_equivalent"))
    );
}

#[test]
fn fetch_that_keeps_too_few_rows() {
    let mut exec = fetching(rows_source(&[100]), None);
    exec.fetch_mode = FetchMode::KeepHalf;
    let report = check_with("with_fetch_equivalent", &exec.build());
    assert_eq!(
        messages(&report),
        vec![
            "with_fetch(Some(1)): the output has 0 rows, but limiting 100 rows to 1 \
             gives 1",
            "with_fetch(Some(7)): the output has 3 rows, but limiting 100 rows to 7 \
             gives 7",
            "with_fetch(Some(101)): the output has 50 rows, but limiting 100 rows to \
             101 gives 100",
        ]
    );
}

#[test]
fn fetch_that_keeps_rows_that_are_not_first() {
    // With (almost) unique sort keys, skipping the first row keeps rows that
    // are not the first in sort order, with one partition or several
    for rows in [&[100][..], &[40, 0, 60]] {
        let source = SourceSpec::new(schema())
            .with_partition_rows(rows)
            .with_distinct_values(100_000)
            .with_ordering(ordering_on_a())
            .with_row_ids(0)
            .build_arc()
            .unwrap();
        let mut exec = fetching(source, None);
        exec.fetch_mode = FetchMode::SkipFirstRow;
        let report = check_with("with_fetch_equivalent", &exec.build());
        let expected = if rows.len() == 1 {
            "with_fetch(Some(1)): the output is not a prefix of the output without \
             the limit sorted by [a@0 ASC]"
        } else {
            "with_fetch(Some(1)): the first 1 rows of all partitions together, in sort \
             order, are not the first rows of the output without the limit sorted by \
             [a@0 ASC]"
        };
        assert!(
            messages(&report).iter().any(|m| m.starts_with(expected)),
            "{report}"
        );
    }

    // Skipping a row that ties with the rows after it is a valid result of
    // the fetch
    let mut exec = fetching(sorted_rows_source(&[100]), None);
    exec.fetch_mode = FetchMode::SkipFirstRow;
    let report = check_with("with_fetch_equivalent", &exec.build());
    assert!(
        messages(&report)
            .iter()
            .all(|m| !m.contains("prefix") || m.starts_with("with_fetch(Some(101))")),
        "{report}"
    );

    // Without an ordering, any rows are a valid result of the fetch. Only the
    // largest fetch shows that a row is missing.
    let mut exec = fetching(rows_source(&[100]), None);
    exec.fetch_mode = FetchMode::SkipFirstRow;
    let report = check_with("with_fetch_equivalent", &exec.build());
    assert_eq!(
        messages(&report),
        vec![
            "with_fetch(Some(101)): the output has 99 rows, but limiting 100 rows to \
             101 gives 100"
        ]
    );
}

#[test]
fn fetch_that_makes_up_rows() {
    let mut exec = fetching(rows_source(&[100]), None);
    exec.fetch_mode = FetchMode::RepeatFirstRow;
    let report = check_with("with_fetch_equivalent", &exec.build());
    assert_eq!(
        messages(&report),
        vec![
            "with_fetch(Some(7)): 6 of its 7 rows do not appear in the output without \
             the limit",
            "with_fetch(Some(101)): 100 of its 101 rows do not appear in the output \
             without the limit",
        ]
    );
}

#[test]
fn own_fetch_is_checked() {
    let mut exec = fetching(rows_source(&[100]), Some(5));
    exec.fetch_mode = FetchMode::KeepOneMore;
    let report = check_with("with_fetch_equivalent", &exec.build());
    assert_eq!(
        messages(&report),
        vec![
            "the node's own fetch of 5: partition 0 has 6 rows, more than the limit of 5",
            "with_fetch(Some(1)): partition 0 has 2 rows, more than the limit of 1",
            "with_fetch(Some(7)): partition 0 has 8 rows, more than the limit of 7",
        ]
    );
}

#[test]
fn with_fetch_none_that_keeps_the_fetch() {
    let mut exec = fetching(rows_source(&[100]), Some(5));
    exec.keep_fetch_on_with_fetch_none = true;
    let report = check_with("with_fetch_equivalent", &exec.build());
    assert!(
        messages(&report).contains(
            &"with_fetch(None): the plan reports fetch() Some(5) instead of None"
        ),
        "{report}"
    );
}

#[test]
fn with_fetch_that_drops_the_ordering_is_a_lint() {
    let mut exec = fetching(sorted_rows_source(&[100]), None);
    exec.with_fetch_drops_ordering = true;
    let report = check_with("with_fetch_equivalent", &exec.build());
    // Reported for the plans of with_fetch(None) and each with_fetch(Some(n))
    assert_eq!(
        summary(&report),
        vec![(Severity::Lint, "with_fetch_equivalent"); 4]
    );
    assert!(
        messages(&report)[0].starts_with(
            "with_fetch(None): the plan does not report the ordering [a@0 ASC] that the \
             node reports"
        ),
        "{report}"
    );
}

#[test]
fn partitioned_topk_can_keep_fewer_rows_per_partition() {
    // Partitions share one TopK threshold, so a partition only keeps rows that
    // can be in the overall top rows, often fewer than the fetch
    let plan: Arc<dyn ExecutionPlan> = Arc::new(
        SortExec::new(ordering_on_a(), rows_source(&[100, 200, 300]))
            .with_preserve_partitioning(true)
            .with_fetch(Some(10)),
    );
    check_with("with_fetch_equivalent", &plan).assert_clean();
}

#[test]
fn limit_pushdown_through_correct_nodes_is_clean() {
    let schema = schema();
    let a = col("a", &schema).unwrap();
    let plans: Vec<Arc<dyn ExecutionPlan>> = vec![
        Arc::new(
            ProjectionExec::try_new(
                vec![(Arc::clone(&a), "a".to_string())],
                rows_source(&[40, 0, 60]),
            )
            .unwrap(),
        ),
        UnionExec::try_new(vec![rows_source(&[40, 0, 60]), rows_source(&[30])]).unwrap(),
        Arc::new(CoalescePartitionsExec::new(rows_source(&[40, 0, 60]))),
        Arc::new(SortPreservingMergeExec::new(
            ordering_on_a(),
            sorted_rows_source(&[40, 0, 60]),
        )),
        Arc::new(
            SortPreservingMergeExec::new(
                ordering_on_a(),
                sorted_rows_source(&[40, 0, 60]),
            )
            .with_fetch(Some(5)),
        ),
    ];
    for plan in plans {
        assert!(plan.supports_limit_pushdown());
        check_with("limit_pushdown_equivalent", &plan).assert_clean();
    }
}

#[test]
fn limit_pushdown_through_a_node_that_needs_later_rows() {
    // Skipping rows needs the rows after a limit: with its input limited to
    // `n` rows, the node produces fewer than `n` rows
    let mut exec = ConfigurableExec::new(rows_source(&[100])).effect(Effect::LowerEqual);
    exec.skip = 5;
    exec.limit_pushdown = true;
    let report = check_with("limit_pushdown_equivalent", &exec.build());
    assert_eq!(
        messages(&report),
        vec![
            "with every child limited to its first 1 rows per partition: the output \
             has 0 rows, but limiting 95 rows to 1 gives 1, so a limit above the node \
             cannot be pushed to its children; return false from \
             supports_limit_pushdown",
            "with every child limited to its first 7 rows per partition: the output \
             has 2 rows, but limiting 95 rows to 7 gives 7, so a limit above the node \
             cannot be pushed to its children; return false from \
             supports_limit_pushdown",
        ]
    );

    // `GlobalLimitExec` does the same, but `LimitPushdown` merges limit nodes
    // instead of pushing a limit through them, so it is not checked. The
    // exemption is by type, so it does not hide the node above.
    let limit: Arc<dyn ExecutionPlan> =
        Arc::new(GlobalLimitExec::new(rows_source(&[100]), 5, Some(10)));
    assert!(limit.supports_limit_pushdown());
    check_with("limit_pushdown_equivalent", &limit).assert_clean();
}

#[test]
fn limit_pushdown_through_a_node_that_combines_partitions() {
    // `LimitPushdown` removes the limit above the node and limits each of the
    // three input partitions, so the single output partition keeps up to
    // three times the limit
    let mut exec = ConfigurableExec::new(rows_source(&[40, 30, 60]));
    exec.coalesce = true;
    exec.limit_pushdown = true;
    let report = check_with("limit_pushdown_equivalent", &exec.build());
    assert_eq!(
        messages(&report),
        vec![
            "with every child limited to its first 1 rows per partition: partition 0 \
             has 3 rows, more than the limit of 1, so a limit above the node cannot be \
             pushed to its children; return false from supports_limit_pushdown",
            "with every child limited to its first 7 rows per partition: partition 0 \
             has 21 rows, more than the limit of 7, so a limit above the node cannot \
             be pushed to its children; return false from supports_limit_pushdown",
        ]
    );

    // `LimitPushdown` gives `CoalescePartitionsExec` a fetch instead, so only
    // its first rows count. The exception is by type, so it does not hide the
    // node above.
    let coalesce: Arc<dyn ExecutionPlan> =
        Arc::new(CoalescePartitionsExec::new(rows_source(&[40, 30, 60])));
    check_with("limit_pushdown_equivalent", &coalesce).assert_clean();
}

#[test]
fn output_that_depends_on_the_batch_size() {
    let mut exec = ConfigurableExec::new(rows_source(&[100])).effect(Effect::LowerEqual);
    exec.stop_after_session_batch = true;
    let broken = exec.build();
    let report = check_with("batch_size_invariance", &broken);
    assert_eq!(
        messages(&report),
        [1, 2, 7]
            .map(|n| format!(
                "with batch size {n}: the node produced {n} rows, 0 of which do not \
                 appear in the normal output, instead of the 100 rows it produces \
                 normally"
            ))
            .to_vec()
    );
    // The batch size does not change the batches the leaves serve
    check_with("batch_boundary_invariance", &broken).assert_clean();

    // A correct parent sees different input under those batch sizes, and is
    // not reported
    let parent = ConfigurableExec::new(Arc::clone(&broken)).build();
    let report = check_with("batch_size_invariance", &parent);
    assert!(
        report.violations.iter().all(|v| v.path == vec![0]),
        "{report}"
    );

    // Above a child whose output does not change, the node is reported
    let child = ConfigurableExec::new(rows_source(&[100])).build();
    let mut exec = ConfigurableExec::new(child).effect(Effect::LowerEqual);
    exec.stop_after_session_batch = true;
    let report = check_with("batch_size_invariance", &exec.build());
    assert_eq!(report.violations.len(), 3, "{report}");
    assert!(
        report.violations.iter().all(|v| v.path.is_empty()),
        "{report}"
    );
}

#[test]
fn output_that_depends_on_batch_boundaries() {
    // Keeping half of each batch keeps different rows for different batches
    let broken = ConfigurableExec::new(rows_source(&[40, 0, 60]))
        .effect(Effect::LowerEqual)
        .transform(Transform::DropHalf)
        .build();
    let report = check_with("batch_boundary_invariance", &broken);
    assert!(!report.violations.is_empty());
    assert!(
        messages(&report)[0].starts_with("with input rows in batches of 1 row: the node"),
        "{report}"
    );
    check_with("batch_size_invariance", &broken).assert_clean();

    // Reported on the node, not on its parent
    let parent = ConfigurableExec::new(Arc::clone(&broken)).build();
    let report = check_with("batch_boundary_invariance", &parent);
    assert!(!report.violations.is_empty());
    assert!(
        report.violations.iter().all(|v| v.path == vec![0]),
        "{report}"
    );

    // Above a child whose output does not change, the node is reported
    let child = ConfigurableExec::new(rows_source(&[40, 0, 60])).build();
    let broken = ConfigurableExec::new(child)
        .effect(Effect::LowerEqual)
        .transform(Transform::DropHalf)
        .build();
    let report = check_with("batch_boundary_invariance", &broken);
    assert!(!report.violations.is_empty());
    assert!(
        report.violations.iter().all(|v| v.path.is_empty()),
        "{report}"
    );
}

#[test]
fn ordering_that_depends_on_batch_boundaries() {
    // Reversing each batch of one sorted batch sorts it the other way, but
    // reversing smaller batches does not
    let source = SourceSpec::new(schema())
        .with_partition_rows(&[50])
        .with_batch_layout(BatchLayout::Single)
        .with_ordering(ordering_on_a())
        .build_arc()
        .unwrap();
    let descending = LexOrdering::new(vec![PhysicalSortExpr::new(
        col("a", &schema()).unwrap(),
        SortOptions {
            descending: true,
            nulls_first: false,
        },
    )])
    .unwrap();
    let mut exec = ConfigurableExec::new(source).transform(Transform::Reverse);
    exec.claimed_ordering = Some(descending);
    let plan = exec.build();
    check_with("orderings_hold", &plan).assert_clean();
    let report = check_with("batch_boundary_invariance", &plan);
    let messages = messages(&report);
    assert_eq!(messages.len(), 2, "{report}");
    assert_eq!(
        messages[0],
        "with input rows in batches of 1 row: partition 0 is not sorted by [a@0 DESC \
         NULLS LAST] at row 3, although the node reports that ordering and its normal \
         output is sorted by it"
    );
    assert!(
        messages[1].starts_with(
            "with input rows in random batches of up to 3 rows and empty batches: \
             partition 0 is not sorted by [a@0 DESC NULLS LAST]"
        ),
        "{report}"
    );
}

/// Reports a lint for every batch layout under which a node produces other
/// rows than normally, to show that a difference exists
const DIFFERENT_ROWS_UNDER_LAYOUTS: PlanCheck = PlanCheck {
    name: "different_rows_under_layouts",
    kind: CheckKind::Variant,
    check: |node, context| {
        let Some(normal) = context.output(node) else {
            return Ok(vec![]);
        };
        Ok(context
            .variant_runs(node)
            .iter()
            .filter(|run| matches!(run.variant, Variant::BatchLayout(_)))
            .filter_map(|run| run.output.as_ref().ok())
            .filter(|output| {
                !oracle::same_rows(&output.batches(), &normal.batches()).unwrap()
            })
            .map(|_| Finding::lint("different rows"))
            .collect())
    },
};

#[test]
fn fetch_may_keep_different_rows_under_other_batch_layouts() {
    // The node reverses each batch before applying its fetch, so which rows it
    // keeps depends on batch boundaries. Without an ordering, any rows are a
    // valid result of a fetch.
    let mut exec = fetching(rows_source(&[100]), Some(10)).transform(Transform::Reverse);
    exec.fetch_mode = FetchMode::Enforce;
    let plan = exec.build();
    let report = PlanChecker::with_checks(vec![DIFFERENT_ROWS_UNDER_LAYOUTS])
        .check(&plan)
        .unwrap();
    assert!(!report.violations.is_empty(), "the kept rows should differ");
    checker_of(&[CheckKind::Variant])
        .check(&plan)
        .unwrap()
        .assert_clean();

    // The fetch does not hide rows lost at batch boundaries: with one row per
    // batch, keeping half of each batch keeps nothing
    let plan = fetching(rows_source(&[100]), Some(10))
        .transform(Transform::DropHalf)
        .build();
    let report = check_with("batch_boundary_invariance", &plan);
    assert!(
        messages(&report)[0].starts_with(
            "with input rows in batches of 1 row: the output has 0 rows, but limiting"
        ),
        "{report}"
    );
}

#[test]
fn reexecution_that_differs() {
    let check = |exec: ConfigurableExec| {
        let report = check_with("reset_state_reexecution", &exec.build());
        messages(&report)
            .into_iter()
            .map(String::from)
            .collect::<Vec<_>>()
    };
    let exec = |on_rerun| {
        let mut exec = ConfigurableExec::new(rows_source(&[40, 0, 60]));
        exec.on_rerun = on_rerun;
        exec
    };
    // Executed again without a reset, the node produces no rows
    assert_eq!(
        check(exec(OnRerun::Empty)),
        vec![
            "with a second execution of the same plan, without a reset: the node \
             produced 0 rows, 0 of which do not appear in the normal output, instead \
             of the 100 rows it produces normally; a plan executed again without \
             reset_state must produce the same rows or return an error"
        ]
    );
    assert_eq!(
        check(exec(OnRerun::Panic)),
        vec![
            "with a second execution of the same plan, without a reset: executing the \
             node panicked: ConfigurableExec partition 0 was already executed; return \
             an error instead"
        ]
    );
    // Its state survives `reset_state`, so it produces no rows after a reset
    // either
    let mut keeping_state = exec(OnRerun::Empty);
    keeping_state.keep_state_on_reset = true;
    let messages = check(keeping_state);
    assert_eq!(messages.len(), 2, "{messages:?}");
    assert!(
        messages[0].starts_with(
            "with a second execution after reset_plan_states: the node produced 0 rows"
        ),
        "{messages:?}"
    );
}

#[test]
fn reexecution_that_gives_the_same_rows_or_an_error() {
    let check = |exec: ConfigurableExec| {
        check_with("reset_state_reexecution", &exec.build()).assert_clean();
    };
    for on_rerun in [OnRerun::Same, OnRerun::Error] {
        let mut exec = ConfigurableExec::new(rows_source(&[40, 0, 60]));
        exec.on_rerun = on_rerun;
        check(exec);
    }
    // A node above a child that produces other rows when executed again is
    // not compared: the child is reported
    let mut child = ConfigurableExec::new(rows_source(&[40, 0, 60]));
    child.on_rerun = OnRerun::Empty;
    let parent = ConfigurableExec::new(child.build()).build();
    let report = check_with("reset_state_reexecution", &parent);
    assert_eq!(report.violations.len(), 1, "{report}");
    assert_eq!(report.violations[0].path, vec![0]);
}

/// Reports every variant run on every node
const LIST_VARIANTS: PlanCheck = PlanCheck {
    name: "list_variants",
    kind: CheckKind::Variant,
    check: |node, context| {
        Ok(context
            .variant_runs(node)
            .iter()
            .map(|run| {
                assert!(run.output.is_ok(), "{run:?}");
                Finding::lint(format!("{:?}", run.variant))
            })
            .collect())
    },
};

#[test]
fn variants_run_on_builtin_plans() {
    let plan: Arc<dyn ExecutionPlan> =
        Arc::new(CoalescePartitionsExec::new(rows_source(&[100, 200, 300])));
    let report = PlanChecker::with_checks(vec![LIST_VARIANTS])
        .check(&plan)
        .unwrap();
    let root: Vec<&str> = report
        .violations
        .iter()
        .filter(|v| v.path.is_empty())
        .map(|v| v.message.as_str())
        .collect();
    let expected: Vec<String> = [
        Variant::WithoutFetch,
        Variant::WithFetch(1),
        Variant::WithFetch(7),
        Variant::WithFetch(601),
        Variant::LimitedInputs(1),
        Variant::LimitedInputs(7),
        Variant::LimitedInputs(601),
        Variant::BatchSize(1),
        Variant::BatchSize(2),
        Variant::BatchSize(7),
        Variant::BatchSize(8192),
        Variant::BatchLayout(BatchLayout::Fixed(1)),
        Variant::BatchLayout(BatchLayout::Random { max_rows: 3 }),
        Variant::BatchLayout(BatchLayout::Single),
        Variant::Reset,
        Variant::Rerun,
    ]
    .iter()
    .map(|variant| format!("{variant:?}"))
    .collect();
    assert_eq!(root, expected);

    // The source has no fetch and no children, so only the batch size,
    // layout and re-execution variants apply to it
    let leaf = report
        .violations
        .iter()
        .filter(|v| v.path == vec![0])
        .count();
    assert_eq!(leaf, 9, "{report}");
}
