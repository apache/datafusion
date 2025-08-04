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

use std::sync::{Arc, LazyLock};

use arrow::{
    array::record_batch,
    datatypes::{DataType, Field, Schema, SchemaRef},
    util::pretty::pretty_format_batches,
};
use arrow_schema::SortOptions;
use datafusion::{
    logical_expr::Operator,
    physical_plan::{
        expressions::{BinaryExpr, Column, Literal},
        PhysicalExpr,
    },
    prelude::{ParquetReadOptions, SessionConfig, SessionContext},
    scalar::ScalarValue,
};
use datafusion_common::config::ConfigOptions;
use datafusion_execution::object_store::ObjectStoreUrl;
use datafusion_expr::ScalarUDF;
use datafusion_functions::math::random::RandomFunc;
use datafusion_functions_aggregate::count::count_udaf;
use datafusion_physical_expr::{
    aggregate::AggregateExprBuilder, Partitioning, ScalarFunctionExpr,
};
use datafusion_physical_expr::{expressions::col, LexOrdering, PhysicalSortExpr};
use datafusion_physical_optimizer::{
    filter_pushdown::FilterPushdown, PhysicalOptimizerRule,
};
use datafusion_physical_plan::{
    aggregates::{AggregateExec, AggregateMode, PhysicalGroupBy},
    coalesce_batches::CoalesceBatchesExec,
    filter::FilterExec,
    repartition::RepartitionExec,
    sorts::sort::SortExec,
    ExecutionPlan,
};

use futures::StreamExt;
use object_store::{memory::InMemory, ObjectStore};
use util::{format_plan_for_test, OptimizationTest, TestNode, TestScanBuilder};

mod util;

#[test]
fn test_pushdown_into_scan() {
    let scan = TestScanBuilder::new(schema()).with_support(true).build();
    let predicate = col_lit_predicate("a", "foo", &schema());
    let plan = Arc::new(FilterExec::try_new(predicate, scan).unwrap());

    // expect the predicate to be pushed down into the DataSource
    insta::assert_snapshot!(
        OptimizationTest::new(plan, FilterPushdown::new(), true),
        @r"
    OptimizationTest:
      input:
        - FilterExec: a@0 = foo
        -   DataSourceExec: file_groups={1 group: [[test.parquet]]}, projection=[a, b, c], file_type=test, pushdown_supported=true
      output:
        Ok:
          - DataSourceExec: file_groups={1 group: [[test.parquet]]}, projection=[a, b, c], file_type=test, pushdown_supported=true, predicate=a@0 = foo
    "
    );
}

#[test]
fn test_pushdown_volatile_functions_not_allowed() {
    // Test that we do not push down filters with volatile functions
    // Use random() as an example of a volatile function
    let scan = TestScanBuilder::new(schema()).with_support(true).build();
    let predicate = Arc::new(BinaryExpr::new(
        Arc::new(Column::new_with_schema("a", &schema()).unwrap()),
        Operator::Eq,
        Arc::new(
            ScalarFunctionExpr::try_new(
                Arc::new(ScalarUDF::from(RandomFunc::new())),
                vec![],
                &schema(),
            )
            .unwrap(),
        ),
    )) as Arc<dyn PhysicalExpr>;
    let plan = Arc::new(FilterExec::try_new(predicate, scan).unwrap());
    // expect the filter to not be pushed down
    insta::assert_snapshot!(
        OptimizationTest::new(plan, FilterPushdown::new(), true),
        @r"
    OptimizationTest:
      input:
        - FilterExec: a@0 = random()
        -   DataSourceExec: file_groups={1 group: [[test.parquet]]}, projection=[a, b, c], file_type=test, pushdown_supported=true
      output:
        Ok:
          - FilterExec: a@0 = random()
          -   DataSourceExec: file_groups={1 group: [[test.parquet]]}, projection=[a, b, c], file_type=test, pushdown_supported=true
    ",
    );
}

/// Show that we can use config options to determine how to do pushdown.
#[test]
fn test_pushdown_into_scan_with_config_options() {
    let scan = TestScanBuilder::new(schema()).with_support(true).build();
    let predicate = col_lit_predicate("a", "foo", &schema());
    let plan = Arc::new(FilterExec::try_new(predicate, scan).unwrap()) as _;

    let mut cfg = ConfigOptions::default();
    insta::assert_snapshot!(
        OptimizationTest::new(
            Arc::clone(&plan),
            FilterPushdown::new(),
            false
        ),
        @r"
    OptimizationTest:
      input:
        - FilterExec: a@0 = foo
        -   DataSourceExec: file_groups={1 group: [[test.parquet]]}, projection=[a, b, c], file_type=test, pushdown_supported=true
      output:
        Ok:
          - FilterExec: a@0 = foo
          -   DataSourceExec: file_groups={1 group: [[test.parquet]]}, projection=[a, b, c], file_type=test, pushdown_supported=true
    "
    );

    cfg.execution.parquet.pushdown_filters = true;
    insta::assert_snapshot!(
        OptimizationTest::new(
            plan,
            FilterPushdown::new(),
            true
        ),
        @r"
    OptimizationTest:
      input:
        - FilterExec: a@0 = foo
        -   DataSourceExec: file_groups={1 group: [[test.parquet]]}, projection=[a, b, c], file_type=test, pushdown_supported=true
      output:
        Ok:
          - DataSourceExec: file_groups={1 group: [[test.parquet]]}, projection=[a, b, c], file_type=test, pushdown_supported=true, predicate=a@0 = foo
    "
    );
}

#[tokio::test]
async fn test_dynamic_filter_pushdown_through_hash_join_with_topk() {
    use datafusion_common::JoinType;
    use datafusion_physical_plan::joins::{HashJoinExec, PartitionMode};

    // Create build side with limited values
    let build_batches = vec![record_batch!(
        ("a", Utf8, ["aa", "ab"]),
        ("b", Utf8, ["ba", "bb"]),
        ("c", Float64, [1.0, 2.0])
    )
    .unwrap()];
    let build_side_schema = Arc::new(Schema::new(vec![
        Field::new("a", DataType::Utf8, false),
        Field::new("b", DataType::Utf8, false),
        Field::new("c", DataType::Float64, false),
    ]));
    let build_scan = TestScanBuilder::new(Arc::clone(&build_side_schema))
        .with_support(true)
        .with_batches(build_batches)
        .build();

    // Create probe side with more values
    let probe_batches = vec![record_batch!(
        ("d", Utf8, ["aa", "ab", "ac", "ad"]),
        ("e", Utf8, ["ba", "bb", "bc", "bd"]),
        ("f", Float64, [1.0, 2.0, 3.0, 4.0])
    )
    .unwrap()];
    let probe_side_schema = Arc::new(Schema::new(vec![
        Field::new("d", DataType::Utf8, false),
        Field::new("e", DataType::Utf8, false),
        Field::new("f", DataType::Float64, false),
    ]));
    let probe_scan = TestScanBuilder::new(Arc::clone(&probe_side_schema))
        .with_support(true)
        .with_batches(probe_batches)
        .build();

    // Create HashJoinExec
    let on = vec![(
        col("a", &build_side_schema).unwrap(),
        col("d", &probe_side_schema).unwrap(),
    )];
    let join = Arc::new(
        HashJoinExec::try_new(
            build_scan,
            probe_scan,
            on,
            None,
            &JoinType::Inner,
            None,
            PartitionMode::Partitioned,
            datafusion_common::NullEquality::NullEqualsNothing,
        )
        .unwrap(),
    );

    let join_schema = join.schema();

    // Finally let's add a SortExec on the outside to test pushdown of dynamic filters
    let sort_expr =
        PhysicalSortExpr::new(col("e", &join_schema).unwrap(), SortOptions::default());
    let plan = Arc::new(
        SortExec::new(LexOrdering::new(vec![sort_expr]).unwrap(), join)
            .with_fetch(Some(2)),
    ) as Arc<dyn ExecutionPlan>;

    let mut config = ConfigOptions::default();
    config.optimizer.enable_dynamic_filter_pushdown = true;
    config.execution.parquet.pushdown_filters = true;

    // Appy the FilterPushdown optimizer rule
    let plan = FilterPushdown::new_post_optimization()
        .optimize(Arc::clone(&plan), &config)
        .unwrap();

    // Test that filters are pushed down correctly to each side of the join
    insta::assert_snapshot!(
        format_plan_for_test(&plan),
        @r"
    - SortExec: TopK(fetch=2), expr=[e@4 ASC], preserve_partitioning=[false]
    -   HashJoinExec: mode=Partitioned, join_type=Inner, on=[(a@0, d@0)]
    -     DataSourceExec: file_groups={1 group: [[test.parquet]]}, projection=[a, b, c], file_type=test, pushdown_supported=true
    -     DataSourceExec: file_groups={1 group: [[test.parquet]]}, projection=[d, e, f], file_type=test, pushdown_supported=true, predicate=DynamicFilterPhysicalExpr [ true ]
    "
    );

    // Put some data through the plan to check that the filter is updated to reflect the TopK state
    let session_ctx = SessionContext::new_with_config(SessionConfig::new());
    session_ctx.register_object_store(
        ObjectStoreUrl::parse("test://").unwrap().as_ref(),
        Arc::new(InMemory::new()),
    );
    let state = session_ctx.state();
    let task_ctx = state.task_ctx();
    let mut stream = plan.execute(0, Arc::clone(&task_ctx)).unwrap();
    // Iterate one batch
    stream.next().await.unwrap().unwrap();

    // Test that filters are pushed down correctly to each side of the join
    insta::assert_snapshot!(
        format_plan_for_test(&plan),
        @r"
    - SortExec: TopK(fetch=2), expr=[e@4 ASC], preserve_partitioning=[false], filter=[e@4 IS NULL OR e@4 < bb]
    -   HashJoinExec: mode=Partitioned, join_type=Inner, on=[(a@0, d@0)]
    -     DataSourceExec: file_groups={1 group: [[test.parquet]]}, projection=[a, b, c], file_type=test, pushdown_supported=true
    -     DataSourceExec: file_groups={1 group: [[test.parquet]]}, projection=[d, e, f], file_type=test, pushdown_supported=true, predicate=DynamicFilterPhysicalExpr [ e@1 IS NULL OR e@1 < bb ]
    "
    );
}

// Test both static and dynamic filter pushdown in HashJoinExec.
// Note that static filter pushdown is rare: it should have already happened in the logical optimizer phase.
// However users may manually construct plans that could result in a FilterExec -> HashJoinExec -> Scan setup.
// Dynamic filters arise in cases such as nested inner joins or TopK -> HashJoinExec -> Scan setups.
#[tokio::test]
async fn test_static_filter_pushdown_through_hash_join() {
    use datafusion_common::JoinType;
    use datafusion_physical_plan::joins::{HashJoinExec, PartitionMode};

    // Create build side with limited values
    let build_batches = vec![record_batch!(
        ("a", Utf8, ["aa", "ab"]),
        ("b", Utf8, ["ba", "bb"]),
        ("c", Float64, [1.0, 2.0])
    )
    .unwrap()];
    let build_side_schema = Arc::new(Schema::new(vec![
        Field::new("a", DataType::Utf8, false),
        Field::new("b", DataType::Utf8, false),
        Field::new("c", DataType::Float64, false),
    ]));
    let build_scan = TestScanBuilder::new(Arc::clone(&build_side_schema))
        .with_support(true)
        .with_batches(build_batches)
        .build();

    // Create probe side with more values
    let probe_batches = vec![record_batch!(
        ("d", Utf8, ["aa", "ab", "ac", "ad"]),
        ("e", Utf8, ["ba", "bb", "bc", "bd"]),
        ("f", Float64, [1.0, 2.0, 3.0, 4.0])
    )
    .unwrap()];
    let probe_side_schema = Arc::new(Schema::new(vec![
        Field::new("d", DataType::Utf8, false),
        Field::new("e", DataType::Utf8, false),
        Field::new("f", DataType::Float64, false),
    ]));
    let probe_scan = TestScanBuilder::new(Arc::clone(&probe_side_schema))
        .with_support(true)
        .with_batches(probe_batches)
        .build();

    // Create HashJoinExec
    let on = vec![(
        col("a", &build_side_schema).unwrap(),
        col("d", &probe_side_schema).unwrap(),
    )];
    let join = Arc::new(
        HashJoinExec::try_new(
            build_scan,
            probe_scan,
            on,
            None,
            &JoinType::Inner,
            None,
            PartitionMode::Partitioned,
            datafusion_common::NullEquality::NullEqualsNothing,
        )
        .unwrap(),
    );

    // Create filters that can be pushed down to different sides
    // We need to create filters in the context of the join output schema
    let join_schema = join.schema();

    // Filter on build side column: a = 'aa'
    let left_filter = col_lit_predicate("a", "aa", &join_schema);
    // Filter on probe side column: e = 'ba'
    let right_filter = col_lit_predicate("e", "ba", &join_schema);
    // Filter that references both sides: a = d (should not be pushed down)
    let cross_filter = Arc::new(BinaryExpr::new(
        col("a", &join_schema).unwrap(),
        Operator::Eq,
        col("d", &join_schema).unwrap(),
    )) as Arc<dyn PhysicalExpr>;

    let filter =
        Arc::new(FilterExec::try_new(left_filter, Arc::clone(&join) as _).unwrap());
    let filter = Arc::new(FilterExec::try_new(right_filter, filter).unwrap());
    let plan = Arc::new(FilterExec::try_new(cross_filter, filter).unwrap())
        as Arc<dyn ExecutionPlan>;

    // Test that filters are pushed down correctly to each side of the join
    insta::assert_snapshot!(
        OptimizationTest::new(Arc::clone(&plan), FilterPushdown::new(), true),
        @r"
    OptimizationTest:
      input:
        - FilterExec: a@0 = d@3
        -   FilterExec: e@4 = ba
        -     FilterExec: a@0 = aa
        -       HashJoinExec: mode=Partitioned, join_type=Inner, on=[(a@0, d@0)]
        -         DataSourceExec: file_groups={1 group: [[test.parquet]]}, projection=[a, b, c], file_type=test, pushdown_supported=true
        -         DataSourceExec: file_groups={1 group: [[test.parquet]]}, projection=[d, e, f], file_type=test, pushdown_supported=true
      output:
        Ok:
          - FilterExec: a@0 = d@3
          -   HashJoinExec: mode=Partitioned, join_type=Inner, on=[(a@0, d@0)]
          -     DataSourceExec: file_groups={1 group: [[test.parquet]]}, projection=[a, b, c], file_type=test, pushdown_supported=true, predicate=a@0 = aa
          -     DataSourceExec: file_groups={1 group: [[test.parquet]]}, projection=[d, e, f], file_type=test, pushdown_supported=true, predicate=e@1 = ba
    "
    );

    // Test left join - filters should NOT be pushed down
    let join = Arc::new(
        HashJoinExec::try_new(
            TestScanBuilder::new(Arc::clone(&build_side_schema))
                .with_support(true)
                .build(),
            TestScanBuilder::new(Arc::clone(&probe_side_schema))
                .with_support(true)
                .build(),
            vec![(
                col("a", &build_side_schema).unwrap(),
                col("d", &probe_side_schema).unwrap(),
            )],
            None,
            &JoinType::Left,
            None,
            PartitionMode::Partitioned,
            datafusion_common::NullEquality::NullEqualsNothing,
        )
        .unwrap(),
    );

    let join_schema = join.schema();
    let filter = col_lit_predicate("a", "aa", &join_schema);
    let plan =
        Arc::new(FilterExec::try_new(filter, join).unwrap()) as Arc<dyn ExecutionPlan>;

    // Test that filters are NOT pushed down for left join
    insta::assert_snapshot!(
        OptimizationTest::new(plan, FilterPushdown::new(), true),
        @r"
    OptimizationTest:
      input:
        - FilterExec: a@0 = aa
        -   HashJoinExec: mode=Partitioned, join_type=Left, on=[(a@0, d@0)]
        -     DataSourceExec: file_groups={1 group: [[test.parquet]]}, projection=[a, b, c], file_type=test, pushdown_supported=true
        -     DataSourceExec: file_groups={1 group: [[test.parquet]]}, projection=[d, e, f], file_type=test, pushdown_supported=true
      output:
        Ok:
          - FilterExec: a@0 = aa
          -   HashJoinExec: mode=Partitioned, join_type=Left, on=[(a@0, d@0)]
          -     DataSourceExec: file_groups={1 group: [[test.parquet]]}, projection=[a, b, c], file_type=test, pushdown_supported=true
          -     DataSourceExec: file_groups={1 group: [[test.parquet]]}, projection=[d, e, f], file_type=test, pushdown_supported=true
    "
    );
}

#[test]
fn test_filter_collapse() {
    // filter should be pushed down into the parquet scan with two filters
    let scan = TestScanBuilder::new(schema()).with_support(true).build();
    let predicate1 = col_lit_predicate("a", "foo", &schema());
    let filter1 = Arc::new(FilterExec::try_new(predicate1, scan).unwrap());
    let predicate2 = col_lit_predicate("b", "bar", &schema());
    let plan = Arc::new(FilterExec::try_new(predicate2, filter1).unwrap());

    insta::assert_snapshot!(
        OptimizationTest::new(plan, FilterPushdown::new(), true),
        @r"
    OptimizationTest:
      input:
        - FilterExec: b@1 = bar
        -   FilterExec: a@0 = foo
        -     DataSourceExec: file_groups={1 group: [[test.parquet]]}, projection=[a, b, c], file_type=test, pushdown_supported=true
      output:
        Ok:
          - DataSourceExec: file_groups={1 group: [[test.parquet]]}, projection=[a, b, c], file_type=test, pushdown_supported=true, predicate=a@0 = foo AND b@1 = bar
    "
    );
}

#[test]
fn test_filter_with_projection() {
    let scan = TestScanBuilder::new(schema()).with_support(true).build();
    let projection = vec![1, 0];
    let predicate = col_lit_predicate("a", "foo", &schema());
    let plan = Arc::new(
        FilterExec::try_new(predicate, Arc::clone(&scan))
            .unwrap()
            .with_projection(Some(projection))
            .unwrap(),
    );

    // expect the predicate to be pushed down into the DataSource but the FilterExec to be converted to ProjectionExec
    insta::assert_snapshot!(
        OptimizationTest::new(plan, FilterPushdown::new(), true),
        @r"
    OptimizationTest:
      input:
        - FilterExec: a@0 = foo, projection=[b@1, a@0]
        -   DataSourceExec: file_groups={1 group: [[test.parquet]]}, projection=[a, b, c], file_type=test, pushdown_supported=true
      output:
        Ok:
          - ProjectionExec: expr=[b@1 as b, a@0 as a]
          -   DataSourceExec: file_groups={1 group: [[test.parquet]]}, projection=[a, b, c], file_type=test, pushdown_supported=true, predicate=a@0 = foo
    ",
    );

    // add a test where the filter is on a column that isn't included in the output
    let projection = vec![1];
    let predicate = col_lit_predicate("a", "foo", &schema());
    let plan = Arc::new(
        FilterExec::try_new(predicate, scan)
            .unwrap()
            .with_projection(Some(projection))
            .unwrap(),
    );
    insta::assert_snapshot!(
        OptimizationTest::new(plan, FilterPushdown::new(),true),
        @r"
    OptimizationTest:
      input:
        - FilterExec: a@0 = foo, projection=[b@1]
        -   DataSourceExec: file_groups={1 group: [[test.parquet]]}, projection=[a, b, c], file_type=test, pushdown_supported=true
      output:
        Ok:
          - ProjectionExec: expr=[b@1 as b]
          -   DataSourceExec: file_groups={1 group: [[test.parquet]]}, projection=[a, b, c], file_type=test, pushdown_supported=true, predicate=a@0 = foo
    "
    );
}

#[test]
fn test_push_down_through_transparent_nodes() {
    // expect the predicate to be pushed down into the DataSource
    let scan = TestScanBuilder::new(schema()).with_support(true).build();
    let coalesce = Arc::new(CoalesceBatchesExec::new(scan, 1));
    let predicate = col_lit_predicate("a", "foo", &schema());
    let filter = Arc::new(FilterExec::try_new(predicate, coalesce).unwrap());
    let repartition = Arc::new(
        RepartitionExec::try_new(filter, Partitioning::RoundRobinBatch(1)).unwrap(),
    );
    let predicate = col_lit_predicate("b", "bar", &schema());
    let plan = Arc::new(FilterExec::try_new(predicate, repartition).unwrap());

    // expect the predicate to be pushed down into the DataSource
    insta::assert_snapshot!(
        OptimizationTest::new(plan, FilterPushdown::new(),true),
        @r"
    OptimizationTest:
      input:
        - FilterExec: b@1 = bar
        -   RepartitionExec: partitioning=RoundRobinBatch(1), input_partitions=1
        -     FilterExec: a@0 = foo
        -       CoalesceBatchesExec: target_batch_size=1
        -         DataSourceExec: file_groups={1 group: [[test.parquet]]}, projection=[a, b, c], file_type=test, pushdown_supported=true
      output:
        Ok:
          - RepartitionExec: partitioning=RoundRobinBatch(1), input_partitions=1
          -   CoalesceBatchesExec: target_batch_size=1
          -     DataSourceExec: file_groups={1 group: [[test.parquet]]}, projection=[a, b, c], file_type=test, pushdown_supported=true, predicate=a@0 = foo AND b@1 = bar
    "
    );
}

#[test]
fn test_no_pushdown_through_aggregates() {
    // There are 2 important points here:
    // 1. The outer filter **is not** pushed down at all because we haven't implemented pushdown support
    //    yet for AggregateExec.
    // 2. The inner filter **is** pushed down into the DataSource.
    let scan = TestScanBuilder::new(schema()).with_support(true).build();

    let coalesce = Arc::new(CoalesceBatchesExec::new(scan, 10));

    let filter = Arc::new(
        FilterExec::try_new(col_lit_predicate("a", "foo", &schema()), coalesce).unwrap(),
    );

    let aggregate_expr =
        vec![
            AggregateExprBuilder::new(count_udaf(), vec![col("a", &schema()).unwrap()])
                .schema(schema())
                .alias("cnt")
                .build()
                .map(Arc::new)
                .unwrap(),
        ];
    let group_by = PhysicalGroupBy::new_single(vec![
        (col("a", &schema()).unwrap(), "a".to_string()),
        (col("b", &schema()).unwrap(), "b".to_string()),
    ]);
    let aggregate = Arc::new(
        AggregateExec::try_new(
            AggregateMode::Final,
            group_by,
            aggregate_expr.clone(),
            vec![None],
            filter,
            schema(),
        )
        .unwrap(),
    );

    let coalesce = Arc::new(CoalesceBatchesExec::new(aggregate, 100));

    let predicate = col_lit_predicate("b", "bar", &schema());
    let plan = Arc::new(FilterExec::try_new(predicate, coalesce).unwrap());

    // expect the predicate to be pushed down into the DataSource
    insta::assert_snapshot!(
        OptimizationTest::new(plan, FilterPushdown::new(), true),
        @r"
    OptimizationTest:
      input:
        - FilterExec: b@1 = bar
        -   CoalesceBatchesExec: target_batch_size=100
        -     AggregateExec: mode=Final, gby=[a@0 as a, b@1 as b], aggr=[cnt], ordering_mode=PartiallySorted([0])
        -       FilterExec: a@0 = foo
        -         CoalesceBatchesExec: target_batch_size=10
        -           DataSourceExec: file_groups={1 group: [[test.parquet]]}, projection=[a, b, c], file_type=test, pushdown_supported=true
      output:
        Ok:
          - FilterExec: b@1 = bar
          -   CoalesceBatchesExec: target_batch_size=100
          -     AggregateExec: mode=Final, gby=[a@0 as a, b@1 as b], aggr=[cnt], ordering_mode=PartiallySorted([0])
          -       CoalesceBatchesExec: target_batch_size=10
          -         DataSourceExec: file_groups={1 group: [[test.parquet]]}, projection=[a, b, c], file_type=test, pushdown_supported=true, predicate=a@0 = foo
    "
    );
}

/// Test various combinations of handling of child pushdown results
/// in an ExectionPlan in combination with support/not support in a DataSource.
#[test]
fn test_node_handles_child_pushdown_result() {
    // If we set `with_support(true)` + `inject_filter = true` then the filter is pushed down to the DataSource
    // and no FilterExec is created.
    let scan = TestScanBuilder::new(schema()).with_support(true).build();
    let predicate = col_lit_predicate("a", "foo", &schema());
    let plan = Arc::new(TestNode::new(true, Arc::clone(&scan), predicate));
    insta::assert_snapshot!(
        OptimizationTest::new(plan, FilterPushdown::new(), true),
        @r"
    OptimizationTest:
      input:
        - TestInsertExec { inject_filter: true }
        -   DataSourceExec: file_groups={1 group: [[test.parquet]]}, projection=[a, b, c], file_type=test, pushdown_supported=true
      output:
        Ok:
          - TestInsertExec { inject_filter: true }
          -   DataSourceExec: file_groups={1 group: [[test.parquet]]}, projection=[a, b, c], file_type=test, pushdown_supported=true, predicate=a@0 = foo
    ",
    );

    // If we set `with_support(false)` + `inject_filter = true` then the filter is not pushed down to the DataSource
    // and a FilterExec is created.
    let scan = TestScanBuilder::new(schema()).with_support(false).build();
    let predicate = col_lit_predicate("a", "foo", &schema());
    let plan = Arc::new(TestNode::new(true, Arc::clone(&scan), predicate));
    insta::assert_snapshot!(
        OptimizationTest::new(plan, FilterPushdown::new(), true),
        @r"
    OptimizationTest:
      input:
        - TestInsertExec { inject_filter: true }
        -   DataSourceExec: file_groups={1 group: [[test.parquet]]}, projection=[a, b, c], file_type=test, pushdown_supported=false
      output:
        Ok:
          - TestInsertExec { inject_filter: false }
          -   FilterExec: a@0 = foo
          -     DataSourceExec: file_groups={1 group: [[test.parquet]]}, projection=[a, b, c], file_type=test, pushdown_supported=false
    ",
    );

    // If we set `with_support(false)` + `inject_filter = false` then the filter is not pushed down to the DataSource
    // and no FilterExec is created.
    let scan = TestScanBuilder::new(schema()).with_support(false).build();
    let predicate = col_lit_predicate("a", "foo", &schema());
    let plan = Arc::new(TestNode::new(false, Arc::clone(&scan), predicate));
    insta::assert_snapshot!(
        OptimizationTest::new(plan, FilterPushdown::new(), true),
        @r"
    OptimizationTest:
      input:
        - TestInsertExec { inject_filter: false }
        -   DataSourceExec: file_groups={1 group: [[test.parquet]]}, projection=[a, b, c], file_type=test, pushdown_supported=false
      output:
        Ok:
          - TestInsertExec { inject_filter: false }
          -   DataSourceExec: file_groups={1 group: [[test.parquet]]}, projection=[a, b, c], file_type=test, pushdown_supported=false
    ",
    );
}

#[tokio::test]
async fn test_topk_dynamic_filter_pushdown() {
    let batches = vec![
        record_batch!(
            ("a", Utf8, ["aa", "ab"]),
            ("b", Utf8, ["bd", "bc"]),
            ("c", Float64, [1.0, 2.0])
        )
        .unwrap(),
        record_batch!(
            ("a", Utf8, ["ac", "ad"]),
            ("b", Utf8, ["bb", "ba"]),
            ("c", Float64, [2.0, 1.0])
        )
        .unwrap(),
    ];
    let scan = TestScanBuilder::new(schema())
        .with_support(true)
        .with_batches(batches)
        .build();
    let plan = Arc::new(
        SortExec::new(
            LexOrdering::new(vec![PhysicalSortExpr::new(
                col("b", &schema()).unwrap(),
                SortOptions::new(true, false), // descending, nulls_first
            )])
            .unwrap(),
            Arc::clone(&scan),
        )
        .with_fetch(Some(1)),
    ) as Arc<dyn ExecutionPlan>;

    // expect the predicate to be pushed down into the DataSource
    insta::assert_snapshot!(
        OptimizationTest::new(Arc::clone(&plan), FilterPushdown::new_post_optimization(), true),
        @r"
    OptimizationTest:
      input:
        - SortExec: TopK(fetch=1), expr=[b@1 DESC NULLS LAST], preserve_partitioning=[false]
        -   DataSourceExec: file_groups={1 group: [[test.parquet]]}, projection=[a, b, c], file_type=test, pushdown_supported=true
      output:
        Ok:
          - SortExec: TopK(fetch=1), expr=[b@1 DESC NULLS LAST], preserve_partitioning=[false]
          -   DataSourceExec: file_groups={1 group: [[test.parquet]]}, projection=[a, b, c], file_type=test, pushdown_supported=true, predicate=DynamicFilterPhysicalExpr [ true ]
    "
    );

    // Actually apply the optimization to the plan and put some data through it to check that the filter is updated to reflect the TopK state
    let mut config = ConfigOptions::default();
    config.execution.parquet.pushdown_filters = true;
    let plan = FilterPushdown::new_post_optimization()
        .optimize(plan, &config)
        .unwrap();
    let config = SessionConfig::new().with_batch_size(2);
    let session_ctx = SessionContext::new_with_config(config);
    session_ctx.register_object_store(
        ObjectStoreUrl::parse("test://").unwrap().as_ref(),
        Arc::new(InMemory::new()),
    );
    let state = session_ctx.state();
    let task_ctx = state.task_ctx();
    let mut stream = plan.execute(0, Arc::clone(&task_ctx)).unwrap();
    // Iterate one batch
    stream.next().await.unwrap().unwrap();
    // Now check what our filter looks like
    insta::assert_snapshot!(
        format!("{}", format_plan_for_test(&plan)),
        @r"
    - SortExec: TopK(fetch=1), expr=[b@1 DESC NULLS LAST], preserve_partitioning=[false], filter=[b@1 > bd]
    -   DataSourceExec: file_groups={1 group: [[test.parquet]]}, projection=[a, b, c], file_type=test, pushdown_supported=true, predicate=DynamicFilterPhysicalExpr [ b@1 > bd ]
    "
    );
}

/// Integration test for dynamic filter pushdown with TopK.
/// We use an integration test because there are complex interactions in the optimizer rules
/// that the unit tests applying a single optimizer rule do not cover.
#[tokio::test]
async fn test_topk_dynamic_filter_pushdown_integration() {
    let store = Arc::new(InMemory::new()) as Arc<dyn ObjectStore>;
    let mut cfg = SessionConfig::new();
    cfg.options_mut().execution.parquet.pushdown_filters = true;
    cfg.options_mut().execution.parquet.max_row_group_size = 128;
    let ctx = SessionContext::new_with_config(cfg);
    ctx.register_object_store(
        ObjectStoreUrl::parse("memory://").unwrap().as_ref(),
        Arc::clone(&store),
    );
    ctx.sql(
        r"
COPY  (
  SELECT 1372708800 + value AS t 
  FROM generate_series(0, 99999)
  ORDER BY t
 ) TO 'memory:///1.parquet'
STORED AS PARQUET;
  ",
    )
    .await
    .unwrap()
    .collect()
    .await
    .unwrap();

    // Register the file with the context
    ctx.register_parquet(
        "topk_pushdown",
        "memory:///1.parquet",
        ParquetReadOptions::default(),
    )
    .await
    .unwrap();

    // Create a TopK query that will use dynamic filter pushdown
    let df = ctx
        .sql(r"EXPLAIN ANALYZE SELECT t FROM topk_pushdown ORDER BY t LIMIT 10;")
        .await
        .unwrap();
    let batches = df.collect().await.unwrap();
    let explain = format!("{}", pretty_format_batches(&batches).unwrap());

    assert!(explain.contains("output_rows=128")); // Read 1 row group
    assert!(explain.contains("t@0 < 1372708809")); // Dynamic filter was applied
    assert!(
        explain.contains("pushdown_rows_matched=128, pushdown_rows_pruned=99872"),
        "{explain}"
    );
    // Pushdown pruned most rows
}

/// Schema:
/// a: String
/// b: String
/// c: f64
static TEST_SCHEMA: LazyLock<SchemaRef> = LazyLock::new(|| {
    let fields = vec![
        Field::new("a", DataType::Utf8, false),
        Field::new("b", DataType::Utf8, false),
        Field::new("c", DataType::Float64, false),
    ];
    Arc::new(Schema::new(fields))
});

fn schema() -> SchemaRef {
    Arc::clone(&TEST_SCHEMA)
}

/// Returns a predicate that is a binary expression col = lit
fn col_lit_predicate(
    column_name: &str,
    scalar_value: impl Into<ScalarValue>,
    schema: &Schema,
) -> Arc<dyn PhysicalExpr> {
    let scalar_value = scalar_value.into();
    Arc::new(BinaryExpr::new(
        Arc::new(Column::new_with_schema(column_name, schema).unwrap()),
        Operator::Eq,
        Arc::new(Literal::new(scalar_value)),
    ))
}
