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

//! Tests that each execution check reports deliberately broken plans and
//! stays quiet for correct ones.

use std::sync::Arc;
use std::time::Duration;

use arrow::array::{Int32Array, RecordBatch, UInt64Array};
use arrow::datatypes::{DataType, Field, Schema};
use datafusion_common::ScalarValue;
use datafusion_common::stats::Precision;
use datafusion_expr::Operator;
use datafusion_physical_expr::expressions::{
    BinaryExpr, Column, UnKnownColumn, col, lit,
};
use datafusion_physical_expr::{
    AcrossPartitions, ConstExpr, EquivalenceProperties, LexOrdering, Partitioning,
    PhysicalExpr, PhysicalSortExpr,
};
use datafusion_physical_plan::coalesce_partitions::CoalescePartitionsExec;
use datafusion_physical_plan::filter::FilterExec;
use datafusion_physical_plan::repartition::RepartitionExec;
use datafusion_physical_plan::sorts::sort_preserving_merge::SortPreservingMergeExec;
use datafusion_physical_plan::union::UnionExec;
use datafusion_physical_plan::{ExecutionPlan, StatisticsArgs, StatisticsContext};
use datafusion_property_tests::physical_plan::fixtures::{
    BatchLayout, MockSourceExec, ROW_ID_COLUMN, SourceSpec, StatisticsPrecision,
};
use datafusion_property_tests::physical_plan::harness::ROW_ID_RANGE;
use datafusion_property_tests::physical_plan::{CheckKind, PlanChecker};
use datafusion_property_tests::{Report, Severity};

use crate::common::{
    ConfigurableExec, Effect, InvalidPartition, Transform, checker, checker_of,
    exact_source, inexact_source, messages, schema, source, summary,
};

/// The checks that execute the plan
fn execution_checker() -> PlanChecker {
    checker_of(&[CheckKind::Execution, CheckKind::Variant, CheckKind::Stream])
}

/// Run the checks that execute the plan. `Transform::DropHalf` keeps half of
/// each batch, so its output depends on batch boundaries, which
/// `batch_boundary_invariance` reports, and plans that do not report that they
/// maintain the order of their input pass their first rows through without
/// supporting limit pushdown, which `limit_pushdown_missed_at_runtime`
/// reports; those checks are tested in `variant_checks.rs`.
fn check(plan: &Arc<dyn ExecutionPlan>) -> Report {
    execution_checker()
        .allow("batch_boundary_invariance")
        .allow("limit_pushdown_missed_at_runtime")
        .check(plan)
        .unwrap()
}

fn ordering_on_a() -> LexOrdering {
    LexOrdering::new(vec![PhysicalSortExpr::new_default(
        col("a", &schema()).unwrap(),
    )])
    .unwrap()
}

/// A source with several sorted partitions, empty batches and many ties
fn sorted_source() -> Arc<dyn ExecutionPlan> {
    SourceSpec::new(schema())
        .with_partition_rows(&[50, 0, 70])
        .with_batch_layout(BatchLayout::Random { max_rows: 8 })
        .with_distinct_values(5)
        .with_ordering(ordering_on_a())
        .build_arc()
        .unwrap()
}

#[test]
fn correct_passthrough_is_clean() {
    for input in [
        exact_source(100),
        inexact_source(100),
        source(&[10, 0, 30], StatisticsPrecision::Exact),
        sorted_source(),
        row_id_source(&[40, 0, 60]),
    ] {
        let mut exec = ConfigurableExec::new(input);
        exec.limit_pushdown = true;
        PlanChecker::new()
            .check(&exec.build())
            .unwrap()
            .assert_clean();
    }
}

/// A source with one partition per entry of `rows` and a row id column
fn row_id_source(rows: &[usize]) -> Arc<dyn ExecutionPlan> {
    SourceSpec::new(schema())
        .with_partition_rows(rows)
        .with_row_ids(0)
        .build_arc()
        .unwrap()
}

#[test]
fn order_that_is_kept_but_not_reported() {
    let check = |plan: &Arc<dyn ExecutionPlan>| {
        checker(&["maintains_input_order_missed"])
            .check(plan)
            .unwrap()
    };
    let unreported = |input| {
        let mut exec = ConfigurableExec::new(input);
        exec.maintains_order = Some(false);
        exec
    };
    let report = check(&unreported(row_id_source(&[40, 0, 60])).build());
    assert_eq!(
        messages(&report),
        vec![
            "maintains_input_order()[0] is false, but every output partition only has \
             rows of one partition of child 0, in the order of that partition; return \
             true if the node never reorders the rows of this child, and keep the \
             child's orderings in the output equivalence properties"
        ]
    );
    assert_eq!(
        summary(&report),
        vec![(Severity::Lint, "maintains_input_order_missed")]
    );

    // Not reported for a node that reorders rows, or over input it cannot
    // track or that is already sorted, whether the node reports the input's
    // ordering or not
    let sorted = SourceSpec::new(schema())
        .with_partition_rows(&[40, 0, 60])
        .with_ordering(ordering_on_a())
        .with_row_ids(0)
        .build_arc()
        .unwrap();
    let mut without_ordering = unreported(Arc::clone(&sorted));
    without_ordering.drop_ordering = true;
    for exec in [
        unreported(row_id_source(&[40, 0, 60])).transform(Transform::Reverse),
        unreported(row_id_source(&[40, 0, 60])).transform(Transform::Duplicate),
        unreported(source(&[40, 0, 60], StatisticsPrecision::Exact)),
        unreported(sorted),
        without_ordering,
    ] {
        check(&exec.build()).assert_clean();
    }

    // A coalesce keeps the order of a single input partition, and interleaves
    // several
    let coalesce =
        |input| Arc::new(CoalescePartitionsExec::new(input)) as Arc<dyn ExecutionPlan>;
    assert_eq!(
        summary(&check(&coalesce(row_id_source(&[100])))),
        vec![(Severity::Lint, "maintains_input_order_missed")]
    );
    check(&coalesce(row_id_source(&[40, 0, 60]))).assert_clean();

    // The same partition of a coalesce with a fetch can have rows of one
    // input partition only, which shows nothing about the others
    let coalesce_with_fetch: Arc<dyn ExecutionPlan> = Arc::new(
        CoalescePartitionsExec::new(row_id_source(&[40, 0, 60])).with_fetch(Some(5)),
    );
    check(&coalesce_with_fetch).assert_clean();

    // Each partition of a union has rows of one input only
    let other_input = SourceSpec::new(schema())
        .with_partition_rows(&[30])
        .with_row_ids(ROW_ID_RANGE)
        .build_arc()
        .unwrap();
    let union =
        UnionExec::try_new(vec![row_id_source(&[40, 0, 60]), other_input]).unwrap();
    check(&union).assert_clean();
}

/// A source with an `Int32` column `a` and a row id column, with one batch of
/// these `(a, row id)` rows per partition, that declares that every
/// partition is sorted by `column`
fn sorted_rows_source(
    partitions: &[&[(i32, u64)]],
    column: &str,
) -> Arc<dyn ExecutionPlan> {
    let schema = Arc::new(Schema::new(vec![
        Field::new("a", DataType::Int32, false),
        Field::new(ROW_ID_COLUMN, DataType::UInt64, false),
    ]));
    let partitions = partitions
        .iter()
        .map(|rows| {
            let a = Arc::new(Int32Array::from_iter_values(rows.iter().map(|row| row.0)));
            let ids =
                Arc::new(UInt64Array::from_iter_values(rows.iter().map(|row| row.1)));
            vec![RecordBatch::try_new(Arc::clone(&schema), vec![a, ids]).unwrap()]
        })
        .collect();
    let ordering = LexOrdering::new(vec![PhysicalSortExpr::new_default(
        col(column, &schema).unwrap(),
    )])
    .unwrap();
    Arc::new(
        MockSourceExec::try_new(schema, partitions)
            .unwrap()
            .try_with_output_ordering(ordering)
            .unwrap(),
    )
}

#[test]
fn maintained_order_that_does_not_hold() {
    let check = |plan: &Arc<dyn ExecutionPlan>| {
        checker(&["maintains_input_order_holds"])
            .check(plan)
            .unwrap()
    };
    let input =
        sorted_rows_source(&[&[(1, 0), (2, 1), (3, 2)], &[], &[(4, 3), (5, 4)]], "a");
    let report = check(
        &ConfigurableExec::new(Arc::clone(&input))
            .transform(Transform::Reverse)
            .build(),
    );
    assert_eq!(
        messages(&report),
        vec![
            "maintains_input_order()[0] is true, but row 1 of output partition 0 is row 1 \
             of partition 0 of child 0, which comes before row 2 of partition 0 of child \
             0, at row 0 of the output partition",
            "maintains_input_order()[0] is true, but row 1 of output partition 2 is row 0 \
             of partition 2 of child 0, which comes before row 1 of partition 2 of child \
             0, at row 0 of the output partition",
        ]
    );
    assert_eq!(
        summary(&report),
        vec![(Severity::Invariant, "maintains_input_order_holds"); 2]
    );

    // A repeated row must be next to its copy
    let report = check(
        &ConfigurableExec::new(Arc::clone(&input))
            .transform(Transform::Duplicate)
            .build(),
    );
    assert_eq!(
        messages(&report),
        vec![
            "maintains_input_order()[0] is true, but row 3 of output partition 0 is row 0 \
             of partition 0 of child 0, which comes before row 2 of partition 0 of child \
             0, at row 2 of the output partition",
            "maintains_input_order()[0] is true, but row 2 of output partition 2 is row 0 \
             of partition 2 of child 0, which comes before row 1 of partition 2 of child \
             0, at row 1 of the output partition",
        ]
    );

    // A child that reports no ordering is checked too: the claim is that the
    // node never reorders the child's rows
    let report = check(
        &ConfigurableExec::new(row_id_source(&[40, 0, 60]))
            .transform(Transform::Reverse)
            .build(),
    );
    assert_eq!(
        summary(&report),
        vec![(Severity::Invariant, "maintains_input_order_holds"); 2]
    );
}

#[test]
fn maintained_order_that_holds() {
    let check = |plan: &Arc<dyn ExecutionPlan>| {
        checker(&["maintains_input_order_holds"])
            .check(plan)
            .unwrap()
            .assert_clean();
    };
    let input = sorted_rows_source(&[&[(1, 0), (3, 1)], &[], &[(2, 2), (4, 3)]], "a");
    // Filters and merges keep the order
    check(&ConfigurableExec::new(Arc::clone(&input)).build());
    let predicate = Arc::new(BinaryExpr::new(
        col("a", &input.schema()).unwrap(),
        Operator::NotEq,
        lit(3),
    ));
    check(&(Arc::new(FilterExec::try_new(predicate, Arc::clone(&input)).unwrap()) as _));
    let merge = SortPreservingMergeExec::new(ordering_on_a(), Arc::clone(&input));
    check(&(Arc::new(merge) as _));
    // How rows of several partitions are interleaved in one output partition
    // is not checked here: `orderings_hold` checks the ordering the node
    // reports. This coalesce keeps the order of each input partition.
    let mut coalesce = ConfigurableExec::new(sorted_rows_source(
        &[&[(1, 0), (3, 1)], &[(2, 2), (4, 3)]],
        "a",
    ));
    coalesce.coalesce = true;
    coalesce.maintains_order = Some(true);
    check(&coalesce.build());

    // Not checked over a child whose rows cannot be tracked
    check(
        &ConfigurableExec::new(sorted_source())
            .transform(Transform::Reverse)
            .build(),
    );
}

#[test]
fn execute_error_is_reported_on_the_failing_node_only() {
    let mut exec = ConfigurableExec::new(exact_source(100));
    exec.execute_error = true;
    let failing = exec.build();
    let report = check(&failing);
    assert_eq!(
        summary(&report),
        vec![(Severity::Invariant, "execution_succeeds")]
    );
    assert!(
        messages(&report)[0].contains("ConfigurableExec cannot execute"),
        "{report}"
    );

    let parent = ConfigurableExec::new(failing).build();
    let report = check(&parent);
    assert_eq!(report.violations.len(), 1, "{report}");
    assert_eq!(report.violations[0].path, vec![0]);
}

#[test]
fn panic_is_reported() {
    let plan = ConfigurableExec::new(source(&[10, 10], StatisticsPrecision::Inexact))
        .transform(Transform::Panic)
        .build();
    let report = check(&plan);
    assert_eq!(
        summary(&report),
        vec![(Severity::Invariant, "execution_succeeds")]
    );
    assert!(
        messages(&report)[0].contains("panicked: ConfigurableExec panicked"),
        "{report}"
    );
}

#[test]
fn timeout_is_reported() {
    let mut exec = ConfigurableExec::new(exact_source(10));
    exec.hang = true;
    let plan = exec.build();
    let report = execution_checker()
        .with_timeout(Duration::from_millis(100))
        .with_stream_timeout(Duration::from_millis(100))
        .check(&plan)
        .unwrap();
    // The node also hangs when asked for a partition it does not have
    assert_eq!(
        summary(&report),
        vec![
            (Severity::Invariant, "execution_succeeds"),
            (Severity::Invariant, "invalid_partition_errors")
        ]
    );
    assert!(messages(&report)[0].contains("did not finish"), "{report}");
}

#[test]
fn nulls_in_non_nullable_column() {
    let plan = ConfigurableExec::new(inexact_source(100))
        .transform(Transform::NullFirstColumn)
        .build();
    let report = check(&plan);
    // The batches with nulls are an invariant violation. The empty batches,
    // whose field is marked nullable too, are a lint.
    let mut findings = summary(&report);
    findings.sort();
    assert_eq!(
        findings,
        vec![
            (Severity::Invariant, "batch_schema"),
            (Severity::Lint, "batch_schema")
        ]
    );
    assert!(
        messages(&report).iter().any(|message| message
            .ends_with("column 'a' has nulls, but schema() declares it non-nullable")),
        "{report}"
    );
}

#[test]
fn renamed_column() {
    let plan = ConfigurableExec::new(inexact_source(100))
        .transform(Transform::RenameFirstColumn)
        .build();
    let report = check(&plan);
    assert_eq!(
        summary(&report),
        vec![(Severity::Invariant, "batch_schema")]
    );
    assert!(messages(&report)[0].contains("'renamed'"), "{report}");
}

#[test]
fn exact_num_rows_that_is_false() {
    let plan = ConfigurableExec::new(exact_source(100))
        .num_rows(Precision::Exact(50))
        .build();
    let report = check(&plan);
    assert_eq!(
        messages(&report),
        vec![
            "the overall statistics are false: num_rows is Exact(50), but the output \
             has 100 rows",
            "the partition 0 statistics are false: num_rows is Exact(50), but the \
             output has 100 rows",
        ],
        "{report}"
    );
}

#[test]
fn false_exact_statistics_are_listed_per_partition() {
    // The input statistics are passed through, but half of the rows of each
    // batch are dropped, so the exact row counts, and possibly other
    // statistics, are no longer true
    let input = SourceSpec::new(schema())
        .with_partition_rows(&[40, 0, 40])
        .with_batch_layout(BatchLayout::Fixed(10))
        .build_arc()
        .unwrap();
    let plan = ConfigurableExec::new(input)
        .effect(Effect::LowerEqual)
        .transform(Transform::DropHalf)
        .build();
    let report = check(&plan);
    assert!(
        report
            .violations
            .iter()
            .all(|v| v.check == "exact_statistics_hold"),
        "{report}"
    );
    // The statistics of the empty partition hold
    let messages = messages(&report);
    assert_eq!(messages.len(), 3, "{report}");
    for (message, expected) in messages.iter().zip([
        "the overall statistics are false: num_rows is Exact(80), but the output has \
         40 rows",
        "the partition 0 statistics are false: num_rows is Exact(40), but the output \
         has 20 rows",
        "the partition 2 statistics are false: num_rows is Exact(40), but the output \
         has 20 rows",
    ]) {
        assert!(message.starts_with(expected), "{report}");
    }
}

#[test]
fn false_exact_statistics_are_reported_on_the_node_that_makes_them() {
    let check = |plan: &Arc<dyn ExecutionPlan>| {
        checker(&["exact_statistics_hold"]).check(plan).unwrap()
    };
    // A parent that passes statistics through unchanged, as a repartition
    // does, inherits the false row count of its child. It is only reported
    // on the child.
    let lying = ConfigurableExec::new(exact_source(100))
        .num_rows(Precision::Exact(50))
        .build();
    let parent = ConfigurableExec::new(lying).build();
    let report = check(&parent);
    assert_eq!(report.violations.len(), 2, "{report}");
    assert!(
        report.violations.iter().all(|v| v.path == vec![0]),
        "{report}"
    );

    // A node that makes a false claim over a child whose statistics hold is
    // reported, also when the child is not a source
    let correct = ConfigurableExec::new(exact_source(100)).build();
    let lying = ConfigurableExec::new(correct)
        .num_rows(Precision::Exact(50))
        .build();
    let report = check(&lying);
    assert_eq!(report.violations.len(), 2, "{report}");
    assert!(
        report.violations.iter().all(|v| v.path.is_empty()),
        "{report}"
    );

    // A node that makes a false claim of its own over a child with false
    // statistics is reported too, with its statistics computed from the true
    // statistics of the child
    let lying_child = ConfigurableExec::new(exact_source(100))
        .num_rows(Precision::Exact(50))
        .build();
    let lying = ConfigurableExec::new(lying_child)
        .num_rows(Precision::Exact(70))
        .build();
    let report = check(&lying);
    let on_node: Vec<_> = report
        .violations
        .iter()
        .filter(|v| v.path.is_empty())
        .map(|v| v.message.as_str())
        .collect();
    assert_eq!(
        on_node,
        vec![
            "with the false exact statistics of its children replaced by the true \
             values, the overall statistics are false: num_rows is Exact(70), but the \
             output has 100 rows",
            "with the false exact statistics of its children replaced by the true \
             values, the partition 0 statistics are false: num_rows is Exact(70), but \
             the output has 100 rows",
        ],
        "{report}"
    );
    assert_eq!(report.violations.len(), 4, "{report}");
}

#[test]
fn false_exact_sum() {
    // Every row is repeated, but the input statistics are passed through
    let input = exact_source(100);
    let sum = match &StatisticsContext::new()
        .compute(input.as_ref(), &StatisticsArgs::new())
        .unwrap()
        .column_statistics[0]
        .sum_value
    {
        Precision::Exact(ScalarValue::Int64(Some(sum))) => *sum,
        other => panic!("the source reports the sum {other}"),
    };
    let plan = ConfigurableExec::new(input)
        .effect(Effect::GreaterEqual)
        .transform(Transform::Duplicate)
        .build();
    let report = checker(&["exact_statistics_hold"]).check(&plan).unwrap();
    let false_statistics = format!(
        "num_rows is Exact(100), but the output has 200 rows; sum_value of a@0 is \
         Exact(Int64({sum})), but the output has Exact(Int64({}))",
        2 * sum
    );
    assert_eq!(
        messages(&report),
        vec![
            format!("the overall statistics are false: {false_statistics}"),
            format!("the partition 0 statistics are false: {false_statistics}"),
        ],
        "{report}"
    );
}

#[test]
fn inexact_statistics_are_not_checked() {
    let plan = ConfigurableExec::new(inexact_source(100))
        .effect(Effect::LowerEqual)
        .transform(Transform::DropHalf)
        .build();
    check(&plan).assert_clean();
}

#[test]
fn ordering_that_does_not_hold() {
    // The input is sorted, but the node reverses each batch while still
    // reporting the input ordering
    let plan = ConfigurableExec::new(sorted_source())
        .transform(Transform::Reverse)
        .build();
    let report = check(&plan);
    // Partitions 0 and 2 have rows; partition 1 is empty
    assert_eq!(
        summary(&report),
        vec![
            (Severity::Invariant, "orderings_hold"),
            (Severity::Invariant, "orderings_hold"),
        ]
    );

    // An ordering claimed over unsorted input
    let mut exec = ConfigurableExec::new(inexact_source(100));
    exec.claimed_ordering = Some(ordering_on_a());
    let plan = exec.build();
    assert_eq!(
        summary(&check(&plan)),
        vec![(Severity::Invariant, "orderings_hold")]
    );
}

/// A source with a nullable column `a` and one partition per entry of
/// `partitions`, with these values
fn values_source(partitions: &[&[Option<i32>]]) -> Arc<dyn ExecutionPlan> {
    let schema = Arc::new(Schema::new(vec![Field::new("a", DataType::Int32, true)]));
    let partitions = partitions
        .iter()
        .map(|values| {
            let column = Arc::new(Int32Array::from(values.to_vec()));
            vec![RecordBatch::try_new(Arc::clone(&schema), vec![column]).unwrap()]
        })
        .collect();
    Arc::new(MockSourceExec::try_new(schema, partitions).unwrap())
}

/// A node that passes `input` through, and reports `expr` as a constant
fn claiming_constant(
    input: &Arc<dyn ExecutionPlan>,
    expr: Arc<dyn PhysicalExpr>,
    across_partitions: AcrossPartitions,
) -> Arc<dyn ExecutionPlan> {
    let mut eq_properties = EquivalenceProperties::new(input.schema());
    eq_properties
        .add_constants([ConstExpr::new(expr, across_partitions)])
        .unwrap();
    let mut exec = ConfigurableExec::new(Arc::clone(input));
    exec.claimed_eq_properties = Some(eq_properties);
    exec.build()
}

#[test]
fn constants_that_hold() {
    // A filter on `a = 3` makes `a` a constant with the value 3 in every
    // partition
    let a = col("a", &schema()).unwrap();
    let predicate = Arc::new(BinaryExpr::new(Arc::clone(&a), Operator::Eq, lit(3)));
    let input = source(&[40, 0, 60], StatisticsPrecision::Exact);
    let filter: Arc<dyn ExecutionPlan> =
        Arc::new(FilterExec::try_new(predicate, input).unwrap());
    let constants = filter.properties().equivalence_properties().constants();
    assert!(
        constants.contains(&ConstExpr::new(
            a,
            AcrossPartitions::Uniform(Some(ScalarValue::Int32(Some(3))))
        )),
        "{constants:?}"
    );
    checker(&["constants_hold"])
        .check(&filter)
        .unwrap()
        .assert_clean();
}

#[test]
fn constants_that_do_not_hold() {
    let check = |plan: Arc<dyn ExecutionPlan>| {
        let report = checker(&["constants_hold"]).check(&plan).unwrap();
        messages(&report)
            .into_iter()
            .map(String::from)
            .collect::<Vec<_>>()
    };
    let a = || col("a", &schema()).unwrap();
    let uniform = |value: ScalarValue| AcrossPartitions::Uniform(Some(value));

    // Several values within a partition. Null is a value.
    let input =
        values_source(&[&[Some(1), Some(1)], &[], &[Some(2), None, Some(2), Some(3)]]);
    assert_eq!(
        check(claiming_constant(
            &input,
            a(),
            AcrossPartitions::Heterogeneous
        )),
        vec![
            "the node reports that a@0 is constant, but partition 2 has 3 distinct \
             values, such as 2 and NULL"
        ]
    );
    // A constant that refers to a column by a wrong name is not checked; it is
    // reported by `expression_column_refs`
    let wrong_name = Arc::new(Column::new("b", 0));
    assert!(
        check(claiming_constant(
            &input,
            wrong_name,
            AcrossPartitions::Heterogeneous
        ))
        .is_empty()
    );

    // A single value within each partition, but not the same in every one
    let input = values_source(&[&[Some(1), Some(1)], &[], &[Some(2)], &[Some(1)]]);
    assert!(
        check(claiming_constant(
            &input,
            a(),
            AcrossPartitions::Heterogeneous
        ))
        .is_empty()
    );
    assert_eq!(
        check(claiming_constant(
            &input,
            a(),
            AcrossPartitions::Uniform(None)
        )),
        vec![
            "the node reports that a@0 is constant with the same value in every \
             partition, but partition 0 has the value 1 and partition 2 has the value 2"
        ]
    );
    // A given value is compared by value, also when its type is wider than
    // the type of the expression
    for value in [ScalarValue::Int32(Some(1)), ScalarValue::Int64(Some(1))] {
        assert_eq!(
            check(claiming_constant(&input, a(), uniform(value))),
            vec![
                "the node reports that a@0 is constant with the value 1 in every \
                 partition, but partition 2 has the value 2"
            ]
        );
    }

    // A column of nulls is constant, with the value null
    let input = values_source(&[&[None, None], &[None]]);
    for across_partitions in [
        AcrossPartitions::Uniform(None),
        uniform(ScalarValue::Int32(None)),
    ] {
        assert!(check(claiming_constant(&input, a(), across_partitions)).is_empty());
    }
    assert_eq!(
        check(claiming_constant(
            &input,
            a(),
            uniform(ScalarValue::Int32(Some(0)))
        )),
        vec![
            "the node reports that a@0 is constant with the value 0 in every \
             partition, but partition 0 has the value NULL",
            "the node reports that a@0 is constant with the value 0 in every \
             partition, but partition 1 has the value NULL",
        ]
    );
}

/// A source with nullable columns `a` and `b` and one partition per entry of
/// `partitions`, with these rows
fn pairs_source(partitions: &[&[(Option<i32>, Option<i32>)]]) -> Arc<dyn ExecutionPlan> {
    let schema = Arc::new(Schema::new(vec![
        Field::new("a", DataType::Int32, true),
        Field::new("b", DataType::Int32, true),
    ]));
    let partitions = partitions
        .iter()
        .map(|rows| {
            let a = Arc::new(Int32Array::from_iter(rows.iter().map(|row| row.0)));
            let b = Arc::new(Int32Array::from_iter(rows.iter().map(|row| row.1)));
            vec![RecordBatch::try_new(Arc::clone(&schema), vec![a, b]).unwrap()]
        })
        .collect();
    Arc::new(MockSourceExec::try_new(schema, partitions).unwrap())
}

/// A node that passes `input` through, and reports that `left` and `right`
/// are equal
fn claiming_equality(
    input: &Arc<dyn ExecutionPlan>,
    left: Arc<dyn PhysicalExpr>,
    right: Arc<dyn PhysicalExpr>,
) -> Arc<dyn ExecutionPlan> {
    let mut eq_properties = EquivalenceProperties::new(input.schema());
    eq_properties.add_equal_conditions(left, right).unwrap();
    let mut exec = ConfigurableExec::new(Arc::clone(input));
    exec.claimed_eq_properties = Some(eq_properties);
    exec.build()
}

#[test]
fn equivalence_classes_that_hold() {
    let check = |plan: &Arc<dyn ExecutionPlan>| {
        checker(&["equivalence_classes_hold"])
            .check(plan)
            .unwrap()
            .assert_clean();
    };
    // Two nulls are equal
    let input = pairs_source(&[&[(Some(1), Some(1)), (None, None)], &[]]);
    let column = |name: &str| col(name, &input.schema()).unwrap();
    check(&claiming_equality(&input, column("a"), column("b")));

    // A filter on `a = b` makes `a` and `b` equal
    let input = pairs_source(&[&[(Some(1), Some(1)), (Some(2), Some(3)), (None, None)]]);
    let predicate = Arc::new(BinaryExpr::new(column("a"), Operator::Eq, column("b")));
    let filter: Arc<dyn ExecutionPlan> =
        Arc::new(FilterExec::try_new(predicate, input).unwrap());
    let eq_properties = filter.properties().equivalence_properties();
    assert!(
        eq_properties
            .eq_group()
            .exprs_equal(&column("a"), &column("b"))
    );
    check(&filter);
}

#[test]
fn equivalence_classes_that_do_not_hold() {
    let check = |plan: Arc<dyn ExecutionPlan>| {
        let report = checker(&["equivalence_classes_hold"]).check(&plan).unwrap();
        messages(&report)
            .into_iter()
            .map(String::from)
            .collect::<Vec<_>>()
    };
    let input = pairs_source(&[
        &[(Some(1), Some(1)), (Some(2), Some(3))],
        &[(None, None)],
        &[(Some(4), None)],
    ]);
    let column = |name: &str| col(name, &input.schema()).unwrap();
    assert_eq!(
        check(claiming_equality(&input, column("a"), column("b"))),
        vec![
            "the node reports that a@0 and b@1 are equal, but row 1 of partition 0 has \
             2 and 3",
            "the node reports that a@0 and b@1 are equal, but row 0 of partition 2 has \
             4 and NULL",
        ]
    );
    // A class with a literal is constant, which `constants_hold` checks, and
    // an expression that refers to a column by a wrong name is reported by
    // `expression_column_refs`
    for other in [
        lit(5),
        Arc::new(Column::new("c", 1)) as Arc<dyn PhysicalExpr>,
    ] {
        assert_eq!(
            check(claiming_equality(&input, column("a"), other)),
            Vec::<String>::new()
        );
    }
}

/// A node that passes `input` through, and reports `partitioning`
fn claiming_partitioning(
    input: Arc<dyn ExecutionPlan>,
    partitioning: Partitioning,
) -> Arc<dyn ExecutionPlan> {
    let mut exec = ConfigurableExec::new(input);
    exec.claimed_partitioning = Some(partitioning);
    exec.build()
}

#[test]
fn hash_partitioning_that_holds() {
    let check = |plan: &Arc<dyn ExecutionPlan>| {
        checker(&["hash_partitioning_holds"])
            .check(plan)
            .unwrap()
            .assert_clean();
    };
    let input = values_source(&[&[Some(1), Some(2), None], &[Some(3), Some(1)]]);
    let a = col("a", &input.schema()).unwrap();
    let repartition: Arc<dyn ExecutionPlan> = Arc::new(
        RepartitionExec::try_new(
            Arc::clone(&input),
            Partitioning::Hash(vec![Arc::clone(&a)], 3),
        )
        .unwrap(),
    );
    check(&repartition);
    // A filter keeps the hash partitioning of its input
    let predicate = Arc::new(BinaryExpr::new(Arc::clone(&a), Operator::NotEq, lit(2)));
    check(&(Arc::new(FilterExec::try_new(predicate, repartition).unwrap()) as _));
    // A key that is not in the output, or that refers to a column by a wrong
    // name, which `expression_column_refs` reports, is not checked
    for key in [
        Arc::new(UnKnownColumn::new("x")) as Arc<dyn PhysicalExpr>,
        Arc::new(Column::new("x", 0)),
    ] {
        check(&claiming_partitioning(
            Arc::clone(&input),
            Partitioning::Hash(vec![key], 2),
        ));
    }
}

#[test]
fn hash_partitioning_that_does_not_hold() {
    let input = values_source(&[&[Some(1), Some(2), None, Some(3)], &[Some(3), Some(2)]]);
    let a = col("a", &input.schema()).unwrap();
    let claiming = claiming_partitioning(input, Partitioning::Hash(vec![a], 2));
    let report = checker(&["hash_partitioning_holds"])
        .check(&claiming)
        .unwrap();
    assert_eq!(
        messages(&report),
        vec![
            "the node reports the partitioning Hash([a@0], 2), but 2 of the 4 rows of \
             partition 0 belong to other partitions by the hash of their key: one has \
             the key (1), which belongs to partition 1",
            "the node reports the partitioning Hash([a@0], 2), but 1 of the 2 rows of \
             partition 1 belong to other partitions by the hash of their key: one has \
             the key (2), which belongs to partition 0",
        ]
    );
    assert_eq!(
        summary(&report),
        vec![(Severity::Invariant, "hash_partitioning_holds"); 2]
    );

    // A node that passes the false partitioning of its child through is not
    // reported: the child is
    let parent = ConfigurableExec::new(claiming).build();
    let report = checker(&["hash_partitioning_holds"])
        .check(&parent)
        .unwrap();
    assert_eq!(report.violations.len(), 2, "{report}");
    assert!(
        report.violations.iter().all(|v| v.path == vec![0]),
        "{report}"
    );
}

#[test]
fn invalid_partitions() {
    let check = |exec: ConfigurableExec| {
        let report = checker(&["invalid_partition_errors"])
            .check(&exec.build())
            .unwrap();
        messages(&report)
            .into_iter()
            .map(String::from)
            .collect::<Vec<_>>()
    };
    let exec = |invalid_partition| {
        let mut exec =
            ConfigurableExec::new(source(&[10, 0, 30], StatisticsPrecision::Exact));
        exec.invalid_partition = invalid_partition;
        exec
    };
    let reported = |problem: &str| {
        vec![format!(
            "the node has 3 output partitions, but execute(3) {problem}; return an \
             error for a partition at or past the partition count"
        )]
    };
    assert_eq!(
        check(exec(InvalidPartition::PanicInExecute)),
        reported("panicked: ConfigurableExec has no partition 3")
    );
    assert_eq!(
        check(exec(InvalidPartition::PanicInStream)),
        reported(
            "returned a stream that panicked when polled: ConfigurableExec has no \
             partition 3"
        )
    );
    assert_eq!(
        check(exec(InvalidPartition::EmptyStream)),
        reported("returned a stream that ended without an error")
    );
    // A node that produces every input partition in its one partition, for any
    // partition it is asked for
    let mut coalesce =
        ConfigurableExec::new(source(&[10, 0, 30], StatisticsPrecision::Exact));
    coalesce.coalesce = true;
    let batch = check(coalesce);
    assert_eq!(batch.len(), 1);
    assert!(
        batch[0].starts_with(
            "the node has 1 output partitions, but execute(1) returned a stream that \
             produced a batch of"
        ),
        "{batch:?}"
    );

    // An error from `execute`, as the input returns, or from the stream, is
    // what the check expects
    assert_eq!(
        check(exec(InvalidPartition::PassToInput)),
        Vec::<String>::new()
    );
    assert_eq!(
        check(exec(InvalidPartition::ErrorInStream)),
        Vec::<String>::new()
    );
    // A node that passes the partition to a child with as many partitions
    // does what the child does, and is not reported
    let parent = ConfigurableExec::new(exec(InvalidPartition::PanicInExecute).build());
    let report = checker(&["invalid_partition_errors"])
        .check(&parent.build())
        .unwrap();
    assert_eq!(report.violations.len(), 1, "{report}");
    assert_eq!(report.violations[0].path, vec![0]);
}

#[test]
fn equal_cardinality_that_does_not_hold() {
    for transform in [Transform::DropHalf, Transform::Duplicate] {
        let plan = ConfigurableExec::new(inexact_source(100))
            .transform(transform)
            .build();
        assert_eq!(
            summary(&check(&plan)),
            vec![(Severity::Invariant, "cardinality_effect_holds")],
            "{transform:?}"
        );
    }
}

#[test]
fn lower_and_greater_equal_cardinality() {
    let lower = |transform| {
        ConfigurableExec::new(inexact_source(100))
            .effect(Effect::LowerEqual)
            .transform(transform)
            .build()
    };
    check(&lower(Transform::DropHalf)).assert_clean();
    assert_eq!(
        summary(&check(&lower(Transform::Duplicate))),
        vec![(Severity::Invariant, "cardinality_effect_holds")]
    );

    let greater = |transform| {
        ConfigurableExec::new(inexact_source(100))
            .effect(Effect::GreaterEqual)
            .transform(transform)
            .build()
    };
    check(&greater(Transform::Duplicate)).assert_clean();
    assert_eq!(
        summary(&check(&greater(Transform::DropHalf))),
        vec![(Severity::Invariant, "cardinality_effect_holds")]
    );
}

#[test]
fn memory_that_is_never_released() {
    let check = |plan: &Arc<dyn ExecutionPlan>| {
        checker(&["memory_released"]).check(plan).unwrap()
    };
    let mut exec = ConfigurableExec::new(exact_source(10));
    exec.leak_memory = true;
    let leaking = exec.build();
    let report = check(&leaking);
    assert_eq!(
        summary(&report),
        vec![(Severity::Invariant, "memory_released")]
    );
    assert!(
        messages(&report)[0].contains("1024 bytes are still reserved"),
        "{report}"
    );

    // The parent inherits the leak, and is not reported
    let parent = ConfigurableExec::new(leaking).build();
    let report = check(&parent);
    assert_eq!(report.violations.len(), 1, "{report}");
    assert_eq!(report.violations[0].path, vec![0]);
}

#[test]
fn static_checks_do_not_execute() {
    // A plan that would hang is never executed when no enabled check needs it
    let mut exec = ConfigurableExec::new(exact_source(10));
    exec.hang = true;
    exec.limit_pushdown = true;
    let plan = exec.build();
    checker_of(&[CheckKind::Static])
        .check(&plan)
        .unwrap()
        .assert_clean();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn check_async_runs_on_the_current_runtime() {
    let plan = ConfigurableExec::new(inexact_source(100))
        .transform(Transform::Duplicate)
        .build();
    let report = execution_checker().check_async(&plan).await.unwrap();
    assert_eq!(
        summary(&report),
        vec![(Severity::Invariant, "cardinality_effect_holds")]
    );
}
