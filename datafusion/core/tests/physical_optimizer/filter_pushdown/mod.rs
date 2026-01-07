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
    array::{Float64Array, Int32Array, RecordBatch, StringArray, record_batch},
    datatypes::{DataType, Field, Schema, SchemaRef},
    util::pretty::pretty_format_batches,
};
use arrow_schema::SortOptions;
use datafusion::{
    assert_batches_eq,
    logical_expr::Operator,
    physical_plan::{
        PhysicalExpr,
        expressions::{BinaryExpr, Column, Literal},
    },
    prelude::{ParquetReadOptions, SessionConfig, SessionContext},
    scalar::ScalarValue,
};
use datafusion_catalog::memory::DataSourceExec;
use datafusion_common::config::ConfigOptions;
use datafusion_datasource::{
    PartitionedFile, file_groups::FileGroup, file_scan_config::FileScanConfigBuilder,
};
use datafusion_execution::object_store::ObjectStoreUrl;
use datafusion_expr::ScalarUDF;
use datafusion_functions::math::random::RandomFunc;
use datafusion_functions_aggregate::{
    count::count_udaf,
    min_max::{max_udaf, min_udaf},
};
use datafusion_physical_expr::{LexOrdering, PhysicalSortExpr, expressions::col};
use datafusion_physical_expr::{
    Partitioning, ScalarFunctionExpr,
    aggregate::{AggregateExprBuilder, AggregateFunctionExpr},
};
use datafusion_physical_optimizer::{
    PhysicalOptimizerRule, filter_pushdown::FilterPushdown,
};
use datafusion_physical_plan::{
    ExecutionPlan,
    aggregates::{AggregateExec, AggregateMode, PhysicalGroupBy},
    coalesce_partitions::CoalescePartitionsExec,
    collect,
    filter::{FilterExec, FilterExecBuilder},
    projection::ProjectionExec,
    repartition::RepartitionExec,
    sorts::sort::SortExec,
};

use datafusion_physical_plan::union::UnionExec;
use futures::StreamExt;
use object_store::{ObjectStore, memory::InMemory};
use regex::Regex;
use util::{OptimizationTest, TestNode, TestScanBuilder, format_plan_for_test};

use crate::physical_optimizer::filter_pushdown::util::TestSource;

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
    let cfg = Arc::new(ConfigOptions::default());
    let predicate = Arc::new(BinaryExpr::new(
        Arc::new(Column::new_with_schema("a", &schema()).unwrap()),
        Operator::Eq,
        Arc::new(
            ScalarFunctionExpr::try_new(
                Arc::new(ScalarUDF::from(RandomFunc::new())),
                vec![],
                &schema(),
                cfg,
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
    let build_batches = vec![
        record_batch!(
            ("a", Utf8, ["aa", "ab"]),
            ("b", Utf8View, ["ba", "bb"]),
            ("c", Float64, [1.0, 2.0])
        )
        .unwrap(),
    ];
    let build_side_schema = Arc::new(Schema::new(vec![
        Field::new("a", DataType::Utf8, false),
        Field::new("b", DataType::Utf8View, false),
        Field::new("c", DataType::Float64, false),
    ]));
    let build_scan = TestScanBuilder::new(Arc::clone(&build_side_schema))
        .with_support(true)
        .with_batches(build_batches)
        .build();

    // Create probe side with more values
    let probe_batches = vec![
        record_batch!(
            ("d", Utf8, ["aa", "ab", "ac", "ad"]),
            ("e", Utf8View, ["ba", "bb", "bc", "bd"]),
            ("f", Float64, [1.0, 2.0, 3.0, 4.0])
        )
        .unwrap(),
    ];
    let probe_side_schema = Arc::new(Schema::new(vec![
        Field::new("d", DataType::Utf8, false),
        Field::new("e", DataType::Utf8View, false),
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
            false,
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

    // Apply the FilterPushdown optimizer rule
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
    -     DataSourceExec: file_groups={1 group: [[test.parquet]]}, projection=[d, e, f], file_type=test, pushdown_supported=true, predicate=DynamicFilter [ empty ] AND DynamicFilter [ empty ]
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
    // NOTE: We dropped the CASE expression here because we now optimize that away if there's only 1 partition
    insta::assert_snapshot!(
        format_plan_for_test(&plan),
        @r"
    - SortExec: TopK(fetch=2), expr=[e@4 ASC], preserve_partitioning=[false], filter=[e@4 IS NULL OR e@4 < bb]
    -   HashJoinExec: mode=Partitioned, join_type=Inner, on=[(a@0, d@0)]
    -     DataSourceExec: file_groups={1 group: [[test.parquet]]}, projection=[a, b, c], file_type=test, pushdown_supported=true
    -     DataSourceExec: file_groups={1 group: [[test.parquet]]}, projection=[d, e, f], file_type=test, pushdown_supported=true, predicate=DynamicFilter [ d@0 >= aa AND d@0 <= ab AND d@0 IN (SET) ([aa, ab]) ] AND DynamicFilter [ e@1 IS NULL OR e@1 < bb ]
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
    let build_batches = vec![
        record_batch!(
            ("a", Utf8, ["aa", "ab"]),
            ("b", Utf8View, ["ba", "bb"]),
            ("c", Float64, [1.0, 2.0])
        )
        .unwrap(),
    ];
    let build_side_schema = Arc::new(Schema::new(vec![
        Field::new("a", DataType::Utf8, false),
        Field::new("b", DataType::Utf8View, false),
        Field::new("c", DataType::Float64, false),
    ]));
    let build_scan = TestScanBuilder::new(Arc::clone(&build_side_schema))
        .with_support(true)
        .with_batches(build_batches)
        .build();

    // Create probe side with more values
    let probe_batches = vec![
        record_batch!(
            ("d", Utf8, ["aa", "ab", "ac", "ad"]),
            ("e", Utf8View, ["ba", "bb", "bc", "bd"]),
            ("f", Float64, [1.0, 2.0, 3.0, 4.0])
        )
        .unwrap(),
    ];
    let probe_side_schema = Arc::new(Schema::new(vec![
        Field::new("d", DataType::Utf8, false),
        Field::new("e", DataType::Utf8View, false),
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
            false,
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
            false,
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
        FilterExecBuilder::new(predicate, Arc::clone(&scan))
            .apply_projection(Some(projection))
            .unwrap()
            .build()
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
        FilterExecBuilder::new(predicate, scan)
            .apply_projection(Some(projection))
            .unwrap()
            .build()
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
    let predicate = col_lit_predicate("a", "foo", &schema());
    let filter = Arc::new(FilterExec::try_new(predicate, scan).unwrap());
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
        -       DataSourceExec: file_groups={1 group: [[test.parquet]]}, projection=[a, b, c], file_type=test, pushdown_supported=true
      output:
        Ok:
          - RepartitionExec: partitioning=RoundRobinBatch(1), input_partitions=1
          -   DataSourceExec: file_groups={1 group: [[test.parquet]]}, projection=[a, b, c], file_type=test, pushdown_supported=true, predicate=a@0 = foo AND b@1 = bar
    "
    );
}

#[test]
fn test_pushdown_through_aggregates_on_grouping_columns() {
    // Test that filters on grouping columns can be pushed through AggregateExec.
    // This test has two filters:
    // 1. An inner filter (a@0 = foo) below the aggregate - gets pushed to DataSource
    // 2. An outer filter (b@1 = bar) above the aggregate - also gets pushed through because 'b' is a grouping column
    let scan = TestScanBuilder::new(schema()).with_support(true).build();

    let filter = Arc::new(
        FilterExecBuilder::new(col_lit_predicate("a", "foo", &schema()), scan)
            .with_batch_size(10)
            .build()
            .unwrap(),
    );

    let aggregate_expr = vec![
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

    let predicate = col_lit_predicate("b", "bar", &schema());
    let plan = Arc::new(
        FilterExecBuilder::new(predicate, aggregate)
            .with_batch_size(100)
            .build()
            .unwrap(),
    );

    // Both filters should be pushed down to the DataSource since both reference grouping columns
    insta::assert_snapshot!(
        OptimizationTest::new(plan, FilterPushdown::new(), true),
        @r"
    OptimizationTest:
      input:
        - FilterExec: b@1 = bar
        -   AggregateExec: mode=Final, gby=[a@0 as a, b@1 as b], aggr=[cnt], ordering_mode=PartiallySorted([0])
        -     FilterExec: a@0 = foo
        -       DataSourceExec: file_groups={1 group: [[test.parquet]]}, projection=[a, b, c], file_type=test, pushdown_supported=true
      output:
        Ok:
          - AggregateExec: mode=Final, gby=[a@0 as a, b@1 as b], aggr=[cnt], ordering_mode=Sorted
          -   DataSourceExec: file_groups={1 group: [[test.parquet]]}, projection=[a, b, c], file_type=test, pushdown_supported=true, predicate=a@0 = foo AND b@1 = bar
    "
    );
}

/// Test various combinations of handling of child pushdown results
/// in an ExecutionPlan in combination with support/not support in a DataSource.
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
          -   DataSourceExec: file_groups={1 group: [[test.parquet]]}, projection=[a, b, c], file_type=test, pushdown_supported=true, predicate=DynamicFilter [ empty ]
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
    -   DataSourceExec: file_groups={1 group: [[test.parquet]]}, projection=[a, b, c], file_type=test, pushdown_supported=true, predicate=DynamicFilter [ b@1 > bd ]
    "
    );
}

#[tokio::test]
async fn test_topk_dynamic_filter_pushdown_multi_column_sort() {
    let batches = vec![
        // We are going to do ORDER BY b ASC NULLS LAST, a DESC
        // And we put the values in such a way that the first batch will fill the TopK
        // and we skip the second batch.
        record_batch!(
            ("a", Utf8, ["ac", "ad"]),
            ("b", Utf8, ["bb", "ba"]),
            ("c", Float64, [2.0, 1.0])
        )
        .unwrap(),
        record_batch!(
            ("a", Utf8, ["aa", "ab"]),
            ("b", Utf8, ["bc", "bd"]),
            ("c", Float64, [1.0, 2.0])
        )
        .unwrap(),
    ];
    let scan = TestScanBuilder::new(schema())
        .with_support(true)
        .with_batches(batches)
        .build();
    let plan = Arc::new(
        SortExec::new(
            LexOrdering::new(vec![
                PhysicalSortExpr::new(
                    col("b", &schema()).unwrap(),
                    SortOptions::default().asc().nulls_last(),
                ),
                PhysicalSortExpr::new(
                    col("a", &schema()).unwrap(),
                    SortOptions::default().desc().nulls_first(),
                ),
            ])
            .unwrap(),
            Arc::clone(&scan),
        )
        .with_fetch(Some(2)),
    ) as Arc<dyn ExecutionPlan>;

    // expect the predicate to be pushed down into the DataSource
    insta::assert_snapshot!(
        OptimizationTest::new(Arc::clone(&plan), FilterPushdown::new_post_optimization(), true),
        @r"
    OptimizationTest:
      input:
        - SortExec: TopK(fetch=2), expr=[b@1 ASC NULLS LAST, a@0 DESC], preserve_partitioning=[false]
        -   DataSourceExec: file_groups={1 group: [[test.parquet]]}, projection=[a, b, c], file_type=test, pushdown_supported=true
      output:
        Ok:
          - SortExec: TopK(fetch=2), expr=[b@1 ASC NULLS LAST, a@0 DESC], preserve_partitioning=[false]
          -   DataSourceExec: file_groups={1 group: [[test.parquet]]}, projection=[a, b, c], file_type=test, pushdown_supported=true, predicate=DynamicFilter [ empty ]
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
    let res = stream.next().await.unwrap().unwrap();
    #[rustfmt::skip]
    let expected = [
        "+----+----+-----+",
        "| a  | b  | c   |",
        "+----+----+-----+",
        "| ad | ba | 1.0 |",
        "| ac | bb | 2.0 |",
        "+----+----+-----+",
    ];
    assert_batches_eq!(expected, &[res]);
    // Now check what our filter looks like
    insta::assert_snapshot!(
        format!("{}", format_plan_for_test(&plan)),
        @r"
    - SortExec: TopK(fetch=2), expr=[b@1 ASC NULLS LAST, a@0 DESC], preserve_partitioning=[false], filter=[b@1 < bb OR b@1 = bb AND (a@0 IS NULL OR a@0 > ac)]
    -   DataSourceExec: file_groups={1 group: [[test.parquet]]}, projection=[a, b, c], file_type=test, pushdown_supported=true, predicate=DynamicFilter [ b@1 < bb OR b@1 = bb AND (a@0 IS NULL OR a@0 > ac) ]
    "
    );
    // There should be no more batches
    assert!(stream.next().await.is_none());
}

#[tokio::test]
async fn test_topk_filter_passes_through_coalesce_partitions() {
    // Create multiple batches for different partitions
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

    // Create a source that supports all batches
    let source = Arc::new(TestSource::new(schema(), true, batches));

    let base_config =
        FileScanConfigBuilder::new(ObjectStoreUrl::parse("test://").unwrap(), source)
            .with_file_groups(vec![
                // Partition 0
                FileGroup::new(vec![PartitionedFile::new("test1.parquet", 123)]),
                // Partition 1
                FileGroup::new(vec![PartitionedFile::new("test2.parquet", 123)]),
            ])
            .build();

    let scan = DataSourceExec::from_data_source(base_config);

    // Add CoalescePartitionsExec to merge the two partitions
    let coalesce = Arc::new(CoalescePartitionsExec::new(scan)) as Arc<dyn ExecutionPlan>;

    // Add SortExec with TopK
    let plan = Arc::new(
        SortExec::new(
            LexOrdering::new(vec![PhysicalSortExpr::new(
                col("b", &schema()).unwrap(),
                SortOptions::new(true, false),
            )])
            .unwrap(),
            coalesce,
        )
        .with_fetch(Some(1)),
    ) as Arc<dyn ExecutionPlan>;

    // Test optimization - the filter SHOULD pass through CoalescePartitionsExec
    // if it properly implements from_children (not all_unsupported)
    insta::assert_snapshot!(
        OptimizationTest::new(Arc::clone(&plan), FilterPushdown::new_post_optimization(), true),
        @r"
    OptimizationTest:
      input:
        - SortExec: TopK(fetch=1), expr=[b@1 DESC NULLS LAST], preserve_partitioning=[false]
        -   CoalescePartitionsExec
        -     DataSourceExec: file_groups={2 groups: [[test1.parquet], [test2.parquet]]}, projection=[a, b, c], file_type=test, pushdown_supported=true
      output:
        Ok:
          - SortExec: TopK(fetch=1), expr=[b@1 DESC NULLS LAST], preserve_partitioning=[false]
          -   CoalescePartitionsExec
          -     DataSourceExec: file_groups={2 groups: [[test1.parquet], [test2.parquet]]}, projection=[a, b, c], file_type=test, pushdown_supported=true, predicate=DynamicFilter [ empty ]
    "
    );
}

#[tokio::test]
async fn test_hashjoin_dynamic_filter_pushdown() {
    use datafusion_common::JoinType;
    use datafusion_physical_plan::joins::{HashJoinExec, PartitionMode};

    // Create build side with limited values
    let build_batches = vec![
        record_batch!(
            ("a", Utf8, ["aa", "ab"]),
            ("b", Utf8, ["ba", "bb"]),
            ("c", Float64, [1.0, 2.0]) // Extra column not used in join
        )
        .unwrap(),
    ];
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
    let probe_batches = vec![
        record_batch!(
            ("a", Utf8, ["aa", "ab", "ac", "ad"]),
            ("b", Utf8, ["ba", "bb", "bc", "bd"]),
            ("e", Float64, [1.0, 2.0, 3.0, 4.0]) // Extra column not used in join
        )
        .unwrap(),
    ];
    let probe_side_schema = Arc::new(Schema::new(vec![
        Field::new("a", DataType::Utf8, false),
        Field::new("b", DataType::Utf8, false),
        Field::new("e", DataType::Float64, false),
    ]));
    let probe_scan = TestScanBuilder::new(Arc::clone(&probe_side_schema))
        .with_support(true)
        .with_batches(probe_batches)
        .build();

    // Create HashJoinExec with dynamic filter
    let on = vec![
        (
            col("a", &build_side_schema).unwrap(),
            col("a", &probe_side_schema).unwrap(),
        ),
        (
            col("b", &build_side_schema).unwrap(),
            col("b", &probe_side_schema).unwrap(),
        ),
    ];
    let plan = Arc::new(
        HashJoinExec::try_new(
            build_scan,
            probe_scan,
            on,
            None,
            &JoinType::Inner,
            None,
            PartitionMode::CollectLeft,
            datafusion_common::NullEquality::NullEqualsNothing,
            false,
        )
        .unwrap(),
    ) as Arc<dyn ExecutionPlan>;

    // expect the predicate to be pushed down into the probe side DataSource
    insta::assert_snapshot!(
        OptimizationTest::new(Arc::clone(&plan), FilterPushdown::new_post_optimization(), true),
        @r"
    OptimizationTest:
      input:
        - HashJoinExec: mode=CollectLeft, join_type=Inner, on=[(a@0, a@0), (b@1, b@1)]
        -   DataSourceExec: file_groups={1 group: [[test.parquet]]}, projection=[a, b, c], file_type=test, pushdown_supported=true
        -   DataSourceExec: file_groups={1 group: [[test.parquet]]}, projection=[a, b, e], file_type=test, pushdown_supported=true
      output:
        Ok:
          - HashJoinExec: mode=CollectLeft, join_type=Inner, on=[(a@0, a@0), (b@1, b@1)]
          -   DataSourceExec: file_groups={1 group: [[test.parquet]]}, projection=[a, b, c], file_type=test, pushdown_supported=true
          -   DataSourceExec: file_groups={1 group: [[test.parquet]]}, projection=[a, b, e], file_type=test, pushdown_supported=true, predicate=DynamicFilter [ empty ]
    ",
    );

    // Actually apply the optimization to the plan and execute to see the filter in action
    let mut config = ConfigOptions::default();
    config.execution.parquet.pushdown_filters = true;
    config.optimizer.enable_dynamic_filter_pushdown = true;
    let plan = FilterPushdown::new_post_optimization()
        .optimize(plan, &config)
        .unwrap();

    // Test for https://github.com/apache/datafusion/pull/17371: dynamic filter linking survives `with_new_children`
    let children = plan.children().into_iter().map(Arc::clone).collect();
    let plan = plan.with_new_children(children).unwrap();

    let config = SessionConfig::new().with_batch_size(10);
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
    - HashJoinExec: mode=CollectLeft, join_type=Inner, on=[(a@0, a@0), (b@1, b@1)]
    -   DataSourceExec: file_groups={1 group: [[test.parquet]]}, projection=[a, b, c], file_type=test, pushdown_supported=true
    -   DataSourceExec: file_groups={1 group: [[test.parquet]]}, projection=[a, b, e], file_type=test, pushdown_supported=true, predicate=DynamicFilter [ a@0 >= aa AND a@0 <= ab AND b@1 >= ba AND b@1 <= bb AND struct(a@0, b@1) IN (SET) ([{c0:aa,c1:ba}, {c0:ab,c1:bb}]) ]
    "
    );
}

#[tokio::test]
async fn test_hashjoin_dynamic_filter_pushdown_partitioned() {
    use datafusion_common::JoinType;
    use datafusion_physical_plan::joins::{HashJoinExec, PartitionMode};

    // Rough sketch of the MRE we're trying to recreate:
    // COPY (select i as k from generate_series(1, 10000000) as t(i))
    // TO 'test_files/scratch/push_down_filter/t1.parquet'
    // STORED AS PARQUET;
    // COPY (select i as k, i as v from generate_series(1, 10000000) as t(i))
    // TO 'test_files/scratch/push_down_filter/t2.parquet'
    // STORED AS PARQUET;
    // create external table t1 stored as parquet location 'test_files/scratch/push_down_filter/t1.parquet';
    // create external table t2 stored as parquet location 'test_files/scratch/push_down_filter/t2.parquet';
    // explain
    // select *
    // from t1
    // join t2 on t1.k = t2.k;
    // +---------------+------------------------------------------------------------+
    // | plan_type     | plan                                                       |
    // +---------------+------------------------------------------------------------+
    // | physical_plan | ┌───────────────────────────┐                              |
    // |               | │        HashJoinExec       │                              |
    // |               | │    --------------------   ├──────────────┐               |
    // |               | │        on: (k = k)        │              │               |
    // |               | └─────────────┬─────────────┘              │               |
    // |               | ┌─────────────┴─────────────┐┌─────────────┴─────────────┐ |
    // |               | │      RepartitionExec      ││      RepartitionExec      │ |
    // |               | │    --------------------   ││    --------------------   │ |
    // |               | │ partition_count(in->out): ││ partition_count(in->out): │ |
    // |               | │          12 -> 12         ││          12 -> 12         │ |
    // |               | │                           ││                           │ |
    // |               | │    partitioning_scheme:   ││    partitioning_scheme:   │ |
    // |               | │      Hash([k@0], 12)      ││      Hash([k@0], 12)      │ |
    // |               | └─────────────┬─────────────┘└─────────────┬─────────────┘ |
    // |               | ┌─────────────┴─────────────┐┌─────────────┴─────────────┐ |
    // |               | │       DataSourceExec      ││       DataSourceExec      │ |
    // |               | │    --------------------   ││    --------------------   │ |
    // |               | │         files: 12         ││         files: 12         │ |
    // |               | │      format: parquet      ││      format: parquet      │ |
    // |               | │                           ││      predicate: true      │ |
    // |               | └───────────────────────────┘└───────────────────────────┘ |
    // |               |                                                            |
    // +---------------+------------------------------------------------------------+

    // Create build side with limited values
    let build_batches = vec![
        record_batch!(
            ("a", Utf8, ["aa", "ab"]),
            ("b", Utf8, ["ba", "bb"]),
            ("c", Float64, [1.0, 2.0]) // Extra column not used in join
        )
        .unwrap(),
    ];
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
    let probe_batches = vec![
        record_batch!(
            ("a", Utf8, ["aa", "ab", "ac", "ad"]),
            ("b", Utf8, ["ba", "bb", "bc", "bd"]),
            ("e", Float64, [1.0, 2.0, 3.0, 4.0]) // Extra column not used in join
        )
        .unwrap(),
    ];
    let probe_side_schema = Arc::new(Schema::new(vec![
        Field::new("a", DataType::Utf8, false),
        Field::new("b", DataType::Utf8, false),
        Field::new("e", DataType::Float64, false),
    ]));
    let probe_scan = TestScanBuilder::new(Arc::clone(&probe_side_schema))
        .with_support(true)
        .with_batches(probe_batches)
        .build();

    // Create RepartitionExec nodes for both sides with hash partitioning on join keys
    let partition_count = 12;

    // Build side: DataSource -> RepartitionExec (Hash)
    let build_hash_exprs = vec![
        col("a", &build_side_schema).unwrap(),
        col("b", &build_side_schema).unwrap(),
    ];
    let build_repartition = Arc::new(
        RepartitionExec::try_new(
            build_scan,
            Partitioning::Hash(build_hash_exprs, partition_count),
        )
        .unwrap(),
    );

    // Probe side: DataSource -> RepartitionExec (Hash)
    let probe_hash_exprs = vec![
        col("a", &probe_side_schema).unwrap(),
        col("b", &probe_side_schema).unwrap(),
    ];
    let probe_repartition = Arc::new(
        RepartitionExec::try_new(
            Arc::clone(&probe_scan),
            Partitioning::Hash(probe_hash_exprs, partition_count),
        )
        .unwrap(),
    );

    // Create HashJoinExec with partitioned inputs
    let on = vec![
        (
            col("a", &build_side_schema).unwrap(),
            col("a", &probe_side_schema).unwrap(),
        ),
        (
            col("b", &build_side_schema).unwrap(),
            col("b", &probe_side_schema).unwrap(),
        ),
    ];
    let hash_join = Arc::new(
        HashJoinExec::try_new(
            build_repartition,
            probe_repartition,
            on,
            None,
            &JoinType::Inner,
            None,
            PartitionMode::Partitioned,
            datafusion_common::NullEquality::NullEqualsNothing,
            false,
        )
        .unwrap(),
    );

    // Top-level CoalescePartitionsExec
    let cp = Arc::new(CoalescePartitionsExec::new(hash_join)) as Arc<dyn ExecutionPlan>;
    // Add a sort for deterministic output
    let plan = Arc::new(SortExec::new(
        LexOrdering::new(vec![PhysicalSortExpr::new(
            col("a", &probe_side_schema).unwrap(),
            SortOptions::new(true, false), // descending, nulls_first
        )])
        .unwrap(),
        cp,
    )) as Arc<dyn ExecutionPlan>;

    // expect the predicate to be pushed down into the probe side DataSource
    insta::assert_snapshot!(
        OptimizationTest::new(Arc::clone(&plan), FilterPushdown::new_post_optimization(), true),
        @r"
    OptimizationTest:
      input:
        - SortExec: expr=[a@0 DESC NULLS LAST], preserve_partitioning=[false]
        -   CoalescePartitionsExec
        -     HashJoinExec: mode=Partitioned, join_type=Inner, on=[(a@0, a@0), (b@1, b@1)]
        -       RepartitionExec: partitioning=Hash([a@0, b@1], 12), input_partitions=1
        -         DataSourceExec: file_groups={1 group: [[test.parquet]]}, projection=[a, b, c], file_type=test, pushdown_supported=true
        -       RepartitionExec: partitioning=Hash([a@0, b@1], 12), input_partitions=1
        -         DataSourceExec: file_groups={1 group: [[test.parquet]]}, projection=[a, b, e], file_type=test, pushdown_supported=true
      output:
        Ok:
          - SortExec: expr=[a@0 DESC NULLS LAST], preserve_partitioning=[false]
          -   CoalescePartitionsExec
          -     HashJoinExec: mode=Partitioned, join_type=Inner, on=[(a@0, a@0), (b@1, b@1)]
          -       RepartitionExec: partitioning=Hash([a@0, b@1], 12), input_partitions=1
          -         DataSourceExec: file_groups={1 group: [[test.parquet]]}, projection=[a, b, c], file_type=test, pushdown_supported=true
          -       RepartitionExec: partitioning=Hash([a@0, b@1], 12), input_partitions=1
          -         DataSourceExec: file_groups={1 group: [[test.parquet]]}, projection=[a, b, e], file_type=test, pushdown_supported=true, predicate=DynamicFilter [ empty ]
    "
    );

    // Actually apply the optimization to the plan and execute to see the filter in action
    let mut config = ConfigOptions::default();
    config.execution.parquet.pushdown_filters = true;
    config.optimizer.enable_dynamic_filter_pushdown = true;
    let plan = FilterPushdown::new_post_optimization()
        .optimize(plan, &config)
        .unwrap();
    let config = SessionConfig::new().with_batch_size(10);
    let session_ctx = SessionContext::new_with_config(config);
    session_ctx.register_object_store(
        ObjectStoreUrl::parse("test://").unwrap().as_ref(),
        Arc::new(InMemory::new()),
    );
    let state = session_ctx.state();
    let task_ctx = state.task_ctx();
    let batches = collect(Arc::clone(&plan), Arc::clone(&task_ctx))
        .await
        .unwrap();

    // Now check what our filter looks like
    #[cfg(not(feature = "force_hash_collisions"))]
    insta::assert_snapshot!(
        format!("{}", format_plan_for_test(&plan)),
        @r"
    - SortExec: expr=[a@0 DESC NULLS LAST], preserve_partitioning=[false]
    -   CoalescePartitionsExec
    -     HashJoinExec: mode=Partitioned, join_type=Inner, on=[(a@0, a@0), (b@1, b@1)]
    -       RepartitionExec: partitioning=Hash([a@0, b@1], 12), input_partitions=1
    -         DataSourceExec: file_groups={1 group: [[test.parquet]]}, projection=[a, b, c], file_type=test, pushdown_supported=true
    -       RepartitionExec: partitioning=Hash([a@0, b@1], 12), input_partitions=1
    -         DataSourceExec: file_groups={1 group: [[test.parquet]]}, projection=[a, b, e], file_type=test, pushdown_supported=true, predicate=DynamicFilter [ CASE hash_repartition % 12 WHEN 2 THEN a@0 >= ab AND a@0 <= ab AND b@1 >= bb AND b@1 <= bb AND struct(a@0, b@1) IN (SET) ([{c0:ab,c1:bb}]) WHEN 4 THEN a@0 >= aa AND a@0 <= aa AND b@1 >= ba AND b@1 <= ba AND struct(a@0, b@1) IN (SET) ([{c0:aa,c1:ba}]) ELSE false END ]
    "
    );

    // When hash collisions force all data into a single partition, we optimize away the CASE expression.
    // This avoids calling create_hashes() for every row on the probe side, since hash % 1 == 0 always,
    // meaning the WHEN 0 branch would always match. This optimization is also important for primary key
    // joins or any scenario where all build-side data naturally lands in one partition.
    #[cfg(feature = "force_hash_collisions")]
    insta::assert_snapshot!(
        format!("{}", format_plan_for_test(&plan)),
        @r"
    - SortExec: expr=[a@0 DESC NULLS LAST], preserve_partitioning=[false]
    -   CoalescePartitionsExec
    -     HashJoinExec: mode=Partitioned, join_type=Inner, on=[(a@0, a@0), (b@1, b@1)]
    -       RepartitionExec: partitioning=Hash([a@0, b@1], 12), input_partitions=1
    -         DataSourceExec: file_groups={1 group: [[test.parquet]]}, projection=[a, b, c], file_type=test, pushdown_supported=true
    -       RepartitionExec: partitioning=Hash([a@0, b@1], 12), input_partitions=1
    -         DataSourceExec: file_groups={1 group: [[test.parquet]]}, projection=[a, b, e], file_type=test, pushdown_supported=true, predicate=DynamicFilter [ a@0 >= aa AND a@0 <= ab AND b@1 >= ba AND b@1 <= bb AND struct(a@0, b@1) IN (SET) ([{c0:aa,c1:ba}, {c0:ab,c1:bb}]) ]
    "
    );

    let result = format!("{}", pretty_format_batches(&batches).unwrap());

    let probe_scan_metrics = probe_scan.metrics().unwrap();

    // The probe side had 4 rows, but after applying the dynamic filter only 2 rows should remain.
    // The number of output rows from the probe side scan should stay consistent across executions.
    // Issue: https://github.com/apache/datafusion/issues/17451
    assert_eq!(probe_scan_metrics.output_rows().unwrap(), 2);

    insta::assert_snapshot!(
        result,
        @r"
    +----+----+-----+----+----+-----+
    | a  | b  | c   | a  | b  | e   |
    +----+----+-----+----+----+-----+
    | ab | bb | 2.0 | ab | bb | 2.0 |
    | aa | ba | 1.0 | aa | ba | 1.0 |
    +----+----+-----+----+----+-----+
    ",
    );
}

#[tokio::test]
async fn test_hashjoin_dynamic_filter_pushdown_collect_left() {
    use datafusion_common::JoinType;
    use datafusion_physical_plan::joins::{HashJoinExec, PartitionMode};

    let build_batches = vec![
        record_batch!(
            ("a", Utf8, ["aa", "ab"]),
            ("b", Utf8, ["ba", "bb"]),
            ("c", Float64, [1.0, 2.0]) // Extra column not used in join
        )
        .unwrap(),
    ];
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
    let probe_batches = vec![
        record_batch!(
            ("a", Utf8, ["aa", "ab", "ac", "ad"]),
            ("b", Utf8, ["ba", "bb", "bc", "bd"]),
            ("e", Float64, [1.0, 2.0, 3.0, 4.0]) // Extra column not used in join
        )
        .unwrap(),
    ];
    let probe_side_schema = Arc::new(Schema::new(vec![
        Field::new("a", DataType::Utf8, false),
        Field::new("b", DataType::Utf8, false),
        Field::new("e", DataType::Float64, false),
    ]));
    let probe_scan = TestScanBuilder::new(Arc::clone(&probe_side_schema))
        .with_support(true)
        .with_batches(probe_batches)
        .build();

    // Create RepartitionExec nodes for both sides with hash partitioning on join keys
    let partition_count = 12;

    // Probe side: DataSource -> RepartitionExec(Hash)
    let probe_hash_exprs = vec![
        col("a", &probe_side_schema).unwrap(),
        col("b", &probe_side_schema).unwrap(),
    ];
    let probe_repartition = Arc::new(
        RepartitionExec::try_new(
            Arc::clone(&probe_scan),
            Partitioning::Hash(probe_hash_exprs, partition_count), // create multi partitions on probSide
        )
        .unwrap(),
    );

    let on = vec![
        (
            col("a", &build_side_schema).unwrap(),
            col("a", &probe_side_schema).unwrap(),
        ),
        (
            col("b", &build_side_schema).unwrap(),
            col("b", &probe_side_schema).unwrap(),
        ),
    ];
    let hash_join = Arc::new(
        HashJoinExec::try_new(
            build_scan,
            probe_repartition,
            on,
            None,
            &JoinType::Inner,
            None,
            PartitionMode::CollectLeft,
            datafusion_common::NullEquality::NullEqualsNothing,
            false,
        )
        .unwrap(),
    );

    // Top-level CoalescePartitionsExec
    let cp = Arc::new(CoalescePartitionsExec::new(hash_join)) as Arc<dyn ExecutionPlan>;
    // Add a sort for deterministic output
    let plan = Arc::new(SortExec::new(
        LexOrdering::new(vec![PhysicalSortExpr::new(
            col("a", &probe_side_schema).unwrap(),
            SortOptions::new(true, false), // descending, nulls_first
        )])
        .unwrap(),
        cp,
    )) as Arc<dyn ExecutionPlan>;

    // expect the predicate to be pushed down into the probe side DataSource
    insta::assert_snapshot!(
        OptimizationTest::new(Arc::clone(&plan), FilterPushdown::new_post_optimization(), true),
        @r"
    OptimizationTest:
      input:
        - SortExec: expr=[a@0 DESC NULLS LAST], preserve_partitioning=[false]
        -   CoalescePartitionsExec
        -     HashJoinExec: mode=CollectLeft, join_type=Inner, on=[(a@0, a@0), (b@1, b@1)]
        -       DataSourceExec: file_groups={1 group: [[test.parquet]]}, projection=[a, b, c], file_type=test, pushdown_supported=true
        -       RepartitionExec: partitioning=Hash([a@0, b@1], 12), input_partitions=1
        -         DataSourceExec: file_groups={1 group: [[test.parquet]]}, projection=[a, b, e], file_type=test, pushdown_supported=true
      output:
        Ok:
          - SortExec: expr=[a@0 DESC NULLS LAST], preserve_partitioning=[false]
          -   CoalescePartitionsExec
          -     HashJoinExec: mode=CollectLeft, join_type=Inner, on=[(a@0, a@0), (b@1, b@1)]
          -       DataSourceExec: file_groups={1 group: [[test.parquet]]}, projection=[a, b, c], file_type=test, pushdown_supported=true
          -       RepartitionExec: partitioning=Hash([a@0, b@1], 12), input_partitions=1
          -         DataSourceExec: file_groups={1 group: [[test.parquet]]}, projection=[a, b, e], file_type=test, pushdown_supported=true, predicate=DynamicFilter [ empty ]
    "
    );

    // Actually apply the optimization to the plan and execute to see the filter in action
    let mut config = ConfigOptions::default();
    config.execution.parquet.pushdown_filters = true;
    config.optimizer.enable_dynamic_filter_pushdown = true;
    let plan = FilterPushdown::new_post_optimization()
        .optimize(plan, &config)
        .unwrap();
    let config = SessionConfig::new().with_batch_size(10);
    let session_ctx = SessionContext::new_with_config(config);
    session_ctx.register_object_store(
        ObjectStoreUrl::parse("test://").unwrap().as_ref(),
        Arc::new(InMemory::new()),
    );
    let state = session_ctx.state();
    let task_ctx = state.task_ctx();
    let batches = collect(Arc::clone(&plan), Arc::clone(&task_ctx))
        .await
        .unwrap();

    // Now check what our filter looks like
    insta::assert_snapshot!(
        format!("{}", format_plan_for_test(&plan)),
        @r"
    - SortExec: expr=[a@0 DESC NULLS LAST], preserve_partitioning=[false]
    -   CoalescePartitionsExec
    -     HashJoinExec: mode=CollectLeft, join_type=Inner, on=[(a@0, a@0), (b@1, b@1)]
    -       DataSourceExec: file_groups={1 group: [[test.parquet]]}, projection=[a, b, c], file_type=test, pushdown_supported=true
    -       RepartitionExec: partitioning=Hash([a@0, b@1], 12), input_partitions=1
    -         DataSourceExec: file_groups={1 group: [[test.parquet]]}, projection=[a, b, e], file_type=test, pushdown_supported=true, predicate=DynamicFilter [ a@0 >= aa AND a@0 <= ab AND b@1 >= ba AND b@1 <= bb AND struct(a@0, b@1) IN (SET) ([{c0:aa,c1:ba}, {c0:ab,c1:bb}]) ]
    "
    );

    let result = format!("{}", pretty_format_batches(&batches).unwrap());

    let probe_scan_metrics = probe_scan.metrics().unwrap();

    // The probe side had 4 rows, but after applying the dynamic filter only 2 rows should remain.
    // The number of output rows from the probe side scan should stay consistent across executions.
    // Issue: https://github.com/apache/datafusion/issues/17451
    assert_eq!(probe_scan_metrics.output_rows().unwrap(), 2);

    insta::assert_snapshot!(
        result,
        @r"
    +----+----+-----+----+----+-----+
    | a  | b  | c   | a  | b  | e   |
    +----+----+-----+----+----+-----+
    | ab | bb | 2.0 | ab | bb | 2.0 |
    | aa | ba | 1.0 | aa | ba | 1.0 |
    +----+----+-----+----+----+-----+
    ",
    );
}

#[tokio::test]
async fn test_nested_hashjoin_dynamic_filter_pushdown() {
    use datafusion_common::JoinType;
    use datafusion_physical_plan::joins::{HashJoinExec, PartitionMode};

    // Create test data for three tables: t1, t2, t3
    // t1: small table with limited values (will be build side of outer join)
    let t1_batches = vec![
        record_batch!(("a", Utf8, ["aa", "ab"]), ("x", Float64, [1.0, 2.0])).unwrap(),
    ];
    let t1_schema = Arc::new(Schema::new(vec![
        Field::new("a", DataType::Utf8, false),
        Field::new("x", DataType::Float64, false),
    ]));
    let t1_scan = TestScanBuilder::new(Arc::clone(&t1_schema))
        .with_support(true)
        .with_batches(t1_batches)
        .build();

    // t2: larger table (will be probe side of inner join, build side of outer join)
    let t2_batches = vec![
        record_batch!(
            ("b", Utf8, ["aa", "ab", "ac", "ad", "ae"]),
            ("c", Utf8, ["ca", "cb", "cc", "cd", "ce"]),
            ("y", Float64, [1.0, 2.0, 3.0, 4.0, 5.0])
        )
        .unwrap(),
    ];
    let t2_schema = Arc::new(Schema::new(vec![
        Field::new("b", DataType::Utf8, false),
        Field::new("c", DataType::Utf8, false),
        Field::new("y", DataType::Float64, false),
    ]));
    let t2_scan = TestScanBuilder::new(Arc::clone(&t2_schema))
        .with_support(true)
        .with_batches(t2_batches)
        .build();

    // t3: largest table (will be probe side of inner join)
    let t3_batches = vec![
        record_batch!(
            ("d", Utf8, ["ca", "cb", "cc", "cd", "ce", "cf", "cg", "ch"]),
            ("z", Float64, [1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0])
        )
        .unwrap(),
    ];
    let t3_schema = Arc::new(Schema::new(vec![
        Field::new("d", DataType::Utf8, false),
        Field::new("z", DataType::Float64, false),
    ]));
    let t3_scan = TestScanBuilder::new(Arc::clone(&t3_schema))
        .with_support(true)
        .with_batches(t3_batches)
        .build();

    // Create nested join structure:
    // Join (t1.a = t2.b)
    // /        \
    // t1    Join(t2.c = t3.d)
    //         /    \
    //        t2   t3

    // First create inner join: t2.c = t3.d
    let inner_join_on =
        vec![(col("c", &t2_schema).unwrap(), col("d", &t3_schema).unwrap())];
    let inner_join = Arc::new(
        HashJoinExec::try_new(
            t2_scan,
            t3_scan,
            inner_join_on,
            None,
            &JoinType::Inner,
            None,
            PartitionMode::Partitioned,
            datafusion_common::NullEquality::NullEqualsNothing,
            false,
        )
        .unwrap(),
    );

    // Then create outer join: t1.a = t2.b (from inner join result)
    let outer_join_on = vec![(
        col("a", &t1_schema).unwrap(),
        col("b", &inner_join.schema()).unwrap(),
    )];
    let outer_join = Arc::new(
        HashJoinExec::try_new(
            t1_scan,
            inner_join as Arc<dyn ExecutionPlan>,
            outer_join_on,
            None,
            &JoinType::Inner,
            None,
            PartitionMode::Partitioned,
            datafusion_common::NullEquality::NullEqualsNothing,
            false,
        )
        .unwrap(),
    ) as Arc<dyn ExecutionPlan>;

    // Test that dynamic filters are pushed down correctly through nested joins
    insta::assert_snapshot!(
        OptimizationTest::new(Arc::clone(&outer_join), FilterPushdown::new_post_optimization(), true),
        @r"
    OptimizationTest:
      input:
        - HashJoinExec: mode=Partitioned, join_type=Inner, on=[(a@0, b@0)]
        -   DataSourceExec: file_groups={1 group: [[test.parquet]]}, projection=[a, x], file_type=test, pushdown_supported=true
        -   HashJoinExec: mode=Partitioned, join_type=Inner, on=[(c@1, d@0)]
        -     DataSourceExec: file_groups={1 group: [[test.parquet]]}, projection=[b, c, y], file_type=test, pushdown_supported=true
        -     DataSourceExec: file_groups={1 group: [[test.parquet]]}, projection=[d, z], file_type=test, pushdown_supported=true
      output:
        Ok:
          - HashJoinExec: mode=Partitioned, join_type=Inner, on=[(a@0, b@0)]
          -   DataSourceExec: file_groups={1 group: [[test.parquet]]}, projection=[a, x], file_type=test, pushdown_supported=true
          -   HashJoinExec: mode=Partitioned, join_type=Inner, on=[(c@1, d@0)]
          -     DataSourceExec: file_groups={1 group: [[test.parquet]]}, projection=[b, c, y], file_type=test, pushdown_supported=true, predicate=DynamicFilter [ empty ]
          -     DataSourceExec: file_groups={1 group: [[test.parquet]]}, projection=[d, z], file_type=test, pushdown_supported=true, predicate=DynamicFilter [ empty ]
    ",
    );

    // Execute the plan to verify the dynamic filters are properly updated
    let mut config = ConfigOptions::default();
    config.execution.parquet.pushdown_filters = true;
    config.optimizer.enable_dynamic_filter_pushdown = true;
    let plan = FilterPushdown::new_post_optimization()
        .optimize(outer_join, &config)
        .unwrap();
    let config = SessionConfig::new().with_batch_size(10);
    let session_ctx = SessionContext::new_with_config(config);
    session_ctx.register_object_store(
        ObjectStoreUrl::parse("test://").unwrap().as_ref(),
        Arc::new(InMemory::new()),
    );
    let state = session_ctx.state();
    let task_ctx = state.task_ctx();
    let mut stream = plan.execute(0, Arc::clone(&task_ctx)).unwrap();
    // Execute to populate the dynamic filters
    stream.next().await.unwrap().unwrap();

    // Verify that both the inner and outer join have updated dynamic filters
    insta::assert_snapshot!(
        format!("{}", format_plan_for_test(&plan)),
        @r"
    - HashJoinExec: mode=Partitioned, join_type=Inner, on=[(a@0, b@0)]
    -   DataSourceExec: file_groups={1 group: [[test.parquet]]}, projection=[a, x], file_type=test, pushdown_supported=true
    -   HashJoinExec: mode=Partitioned, join_type=Inner, on=[(c@1, d@0)]
    -     DataSourceExec: file_groups={1 group: [[test.parquet]]}, projection=[b, c, y], file_type=test, pushdown_supported=true, predicate=DynamicFilter [ b@0 >= aa AND b@0 <= ab AND b@0 IN (SET) ([aa, ab]) ]
    -     DataSourceExec: file_groups={1 group: [[test.parquet]]}, projection=[d, z], file_type=test, pushdown_supported=true, predicate=DynamicFilter [ d@0 >= ca AND d@0 <= cb AND d@0 IN (SET) ([ca, cb]) ]
    "
    );
}

#[tokio::test]
async fn test_hashjoin_parent_filter_pushdown() {
    use datafusion_common::JoinType;
    use datafusion_physical_plan::joins::{HashJoinExec, PartitionMode};

    // Create build side with limited values
    let build_batches = vec![
        record_batch!(
            ("a", Utf8, ["aa", "ab"]),
            ("b", Utf8, ["ba", "bb"]),
            ("c", Float64, [1.0, 2.0])
        )
        .unwrap(),
    ];
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
    let probe_batches = vec![
        record_batch!(
            ("d", Utf8, ["aa", "ab", "ac", "ad"]),
            ("e", Utf8, ["ba", "bb", "bc", "bd"]),
            ("f", Float64, [1.0, 2.0, 3.0, 4.0])
        )
        .unwrap(),
    ];
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
            false,
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
    // Note that we use t * t as the order by expression to avoid
    // the order pushdown optimizer from optimizing away the TopK.
    let df = ctx
        .sql(r"EXPLAIN ANALYZE SELECT t FROM topk_pushdown ORDER BY t * t LIMIT 10;")
        .await
        .unwrap();
    let batches = df.collect().await.unwrap();
    let explain = format!("{}", pretty_format_batches(&batches).unwrap());

    assert!(explain.contains("output_rows=128")); // Read 1 row group
    assert!(explain.contains("t@0 < 1884329474306198481")); // Dynamic filter was applied
    assert!(
        explain.contains("pushdown_rows_matched=128, pushdown_rows_pruned=99.87 K"),
        "{explain}"
    );
    // Pushdown pruned most rows
}

#[test]
fn test_filter_pushdown_through_union() {
    let scan1 = TestScanBuilder::new(schema()).with_support(true).build();
    let scan2 = TestScanBuilder::new(schema()).with_support(true).build();

    let union = UnionExec::try_new(vec![scan1, scan2]).unwrap();

    let predicate = col_lit_predicate("a", "foo", &schema());
    let plan = Arc::new(FilterExec::try_new(predicate, union).unwrap());

    insta::assert_snapshot!(
        OptimizationTest::new(plan, FilterPushdown::new(), true),
        @r"
    OptimizationTest:
      input:
        - FilterExec: a@0 = foo
        -   UnionExec
        -     DataSourceExec: file_groups={1 group: [[test.parquet]]}, projection=[a, b, c], file_type=test, pushdown_supported=true
        -     DataSourceExec: file_groups={1 group: [[test.parquet]]}, projection=[a, b, c], file_type=test, pushdown_supported=true
      output:
        Ok:
          - UnionExec
          -   DataSourceExec: file_groups={1 group: [[test.parquet]]}, projection=[a, b, c], file_type=test, pushdown_supported=true, predicate=a@0 = foo
          -   DataSourceExec: file_groups={1 group: [[test.parquet]]}, projection=[a, b, c], file_type=test, pushdown_supported=true, predicate=a@0 = foo
    "
    );
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

struct ProjectionDynFilterTestCase {
    schema: SchemaRef,
    batches: Vec<RecordBatch>,
    projection: Vec<(Arc<dyn PhysicalExpr>, String)>,
    sort_expr: PhysicalSortExpr,
    expected_plans: Vec<String>,
}

async fn run_projection_dyn_filter_case(case: ProjectionDynFilterTestCase) {
    let ProjectionDynFilterTestCase {
        schema,
        batches,
        projection,
        sort_expr,
        expected_plans,
    } = case;

    let scan = TestScanBuilder::new(Arc::clone(&schema))
        .with_support(true)
        .with_batches(batches)
        .build();

    let projection_exec = Arc::new(ProjectionExec::try_new(projection, scan).unwrap());

    let sort = Arc::new(
        SortExec::new(LexOrdering::new(vec![sort_expr]).unwrap(), projection_exec)
            .with_fetch(Some(2)),
    ) as Arc<dyn ExecutionPlan>;

    let mut config = ConfigOptions::default();
    config.execution.parquet.pushdown_filters = true;
    config.optimizer.enable_dynamic_filter_pushdown = true;

    let optimized_plan = FilterPushdown::new_post_optimization()
        .optimize(Arc::clone(&sort), &config)
        .unwrap();

    pretty_assertions::assert_eq!(
        format_plan_for_test(&optimized_plan).trim(),
        expected_plans[0].trim()
    );

    let config = SessionConfig::new().with_batch_size(2);
    let session_ctx = SessionContext::new_with_config(config);
    session_ctx.register_object_store(
        ObjectStoreUrl::parse("test://").unwrap().as_ref(),
        Arc::new(InMemory::new()),
    );
    let state = session_ctx.state();
    let task_ctx = state.task_ctx();
    let mut stream = optimized_plan.execute(0, Arc::clone(&task_ctx)).unwrap();
    for (idx, expected_plan) in expected_plans.iter().enumerate().skip(1) {
        stream.next().await.unwrap().unwrap();
        let formatted_plan = format_plan_for_test(&optimized_plan);
        pretty_assertions::assert_eq!(
            formatted_plan.trim(),
            expected_plan.trim(),
            "Mismatch at iteration {}",
            idx
        );
    }
}

#[tokio::test]
async fn test_topk_with_projection_transformation_on_dyn_filter() {
    let schema = Arc::new(Schema::new(vec![
        Field::new("a", DataType::Int32, false),
        Field::new("b", DataType::Utf8, false),
        Field::new("c", DataType::Float64, false),
    ]));
    let simple_abc = vec![
        record_batch!(
            ("a", Int32, [1, 2, 3]),
            ("b", Utf8, ["x", "y", "z"]),
            ("c", Float64, [1.0, 2.0, 3.0])
        )
        .unwrap(),
    ];

    // Case 1: Reordering [b, a]
    run_projection_dyn_filter_case(ProjectionDynFilterTestCase {
        schema: Arc::clone(&schema),
        batches: simple_abc.clone(),
        projection: vec![
            (col("b", &schema).unwrap(), "b".to_string()),
            (col("a", &schema).unwrap(), "a".to_string()),
        ],
        sort_expr: PhysicalSortExpr::new(
            Arc::new(Column::new("a", 1)),
            SortOptions::default(),
        ),
        expected_plans: vec![
r#"  - SortExec: TopK(fetch=2), expr=[a@1 ASC], preserve_partitioning=[false]
  -   ProjectionExec: expr=[b@1 as b, a@0 as a]
  -     DataSourceExec: file_groups={1 group: [[test.parquet]]}, projection=[a, b, c], file_type=test, pushdown_supported=true, predicate=DynamicFilter [ empty ]"#.to_string(),
r#"  - SortExec: TopK(fetch=2), expr=[a@1 ASC], preserve_partitioning=[false], filter=[a@1 IS NULL OR a@1 < 2]
  -   ProjectionExec: expr=[b@1 as b, a@0 as a]
  -     DataSourceExec: file_groups={1 group: [[test.parquet]]}, projection=[a, b, c], file_type=test, pushdown_supported=true, predicate=DynamicFilter [ a@0 IS NULL OR a@0 < 2 ]"#.to_string()]
    })
    .await;

    // Case 2: Pruning [a]
    run_projection_dyn_filter_case(ProjectionDynFilterTestCase {
        schema: Arc::clone(&schema),
        batches: simple_abc.clone(),
        projection: vec![(col("a", &schema).unwrap(), "a".to_string())],
        sort_expr: PhysicalSortExpr::new(
            Arc::new(Column::new("a", 0)),
            SortOptions::default(),
        ),
        expected_plans: vec![
            r#"  - SortExec: TopK(fetch=2), expr=[a@0 ASC], preserve_partitioning=[false]
  -   ProjectionExec: expr=[a@0 as a]
  -     DataSourceExec: file_groups={1 group: [[test.parquet]]}, projection=[a, b, c], file_type=test, pushdown_supported=true, predicate=DynamicFilter [ empty ]"#.to_string(),
            r#"  - SortExec: TopK(fetch=2), expr=[a@0 ASC], preserve_partitioning=[false], filter=[a@0 IS NULL OR a@0 < 2]
  -   ProjectionExec: expr=[a@0 as a]
  -     DataSourceExec: file_groups={1 group: [[test.parquet]]}, projection=[a, b, c], file_type=test, pushdown_supported=true, predicate=DynamicFilter [ a@0 IS NULL OR a@0 < 2 ]"#.to_string(),
        ],
    })
    .await;

    // Case 3: Identity [a, b]
    run_projection_dyn_filter_case(ProjectionDynFilterTestCase {
        schema: Arc::clone(&schema),
        batches: simple_abc.clone(),
        projection: vec![
            (col("a", &schema).unwrap(), "a".to_string()),
            (col("b", &schema).unwrap(), "b".to_string()),
        ],
        sort_expr: PhysicalSortExpr::new(
            Arc::new(Column::new("a", 0)),
            SortOptions::default(),
        ),
        expected_plans: vec![
            r#"  - SortExec: TopK(fetch=2), expr=[a@0 ASC], preserve_partitioning=[false]
  -   ProjectionExec: expr=[a@0 as a, b@1 as b]
  -     DataSourceExec: file_groups={1 group: [[test.parquet]]}, projection=[a, b, c], file_type=test, pushdown_supported=true, predicate=DynamicFilter [ empty ]"#.to_string(),
            r#"  - SortExec: TopK(fetch=2), expr=[a@0 ASC], preserve_partitioning=[false], filter=[a@0 IS NULL OR a@0 < 2]
  -   ProjectionExec: expr=[a@0 as a, b@1 as b]
  -     DataSourceExec: file_groups={1 group: [[test.parquet]]}, projection=[a, b, c], file_type=test, pushdown_supported=true, predicate=DynamicFilter [ a@0 IS NULL OR a@0 < 2 ]"#.to_string(),
        ],
    })
    .await;

    // Case 4: Expressions [a + 1, b]
    run_projection_dyn_filter_case(ProjectionDynFilterTestCase {
        schema: Arc::clone(&schema),
        batches: simple_abc.clone(),
        projection: vec![
            (
                Arc::new(BinaryExpr::new(
                    col("a", &schema).unwrap(),
                    Operator::Plus,
                    Arc::new(Literal::new(ScalarValue::Int32(Some(1)))),
                )),
                "a_plus_1".to_string(),
            ),
            (col("b", &schema).unwrap(), "b".to_string()),
        ],
        sort_expr: PhysicalSortExpr::new(
            Arc::new(Column::new("a_plus_1", 0)),
            SortOptions::default(),
        ),
        expected_plans: vec![
            r#"  - SortExec: TopK(fetch=2), expr=[a_plus_1@0 ASC], preserve_partitioning=[false]
  -   ProjectionExec: expr=[a@0 + 1 as a_plus_1, b@1 as b]
  -     DataSourceExec: file_groups={1 group: [[test.parquet]]}, projection=[a, b, c], file_type=test, pushdown_supported=true, predicate=DynamicFilter [ empty ]"#.to_string(),
            r#"  - SortExec: TopK(fetch=2), expr=[a_plus_1@0 ASC], preserve_partitioning=[false], filter=[a_plus_1@0 IS NULL OR a_plus_1@0 < 3]
  -   ProjectionExec: expr=[a@0 + 1 as a_plus_1, b@1 as b]
  -     DataSourceExec: file_groups={1 group: [[test.parquet]]}, projection=[a, b, c], file_type=test, pushdown_supported=true, predicate=DynamicFilter [ a@0 + 1 IS NULL OR a@0 + 1 < 3 ]"#.to_string(),
        ],
    })
    .await;

    // Case 5: [a as b, b as a] (swapped columns)
    run_projection_dyn_filter_case(ProjectionDynFilterTestCase {
        schema: Arc::clone(&schema),
        batches: simple_abc.clone(),
        projection: vec![
            (col("a", &schema).unwrap(), "b".to_string()),
            (col("b", &schema).unwrap(), "a".to_string()),
        ],
        sort_expr: PhysicalSortExpr::new(
            Arc::new(Column::new("b", 0)),
            SortOptions::default(),
        ),
        expected_plans: vec![
            r#"  - SortExec: TopK(fetch=2), expr=[b@0 ASC], preserve_partitioning=[false]
  -   ProjectionExec: expr=[a@0 as b, b@1 as a]
  -     DataSourceExec: file_groups={1 group: [[test.parquet]]}, projection=[a, b, c], file_type=test, pushdown_supported=true, predicate=DynamicFilter [ empty ]"#.to_string(),
            r#"  - SortExec: TopK(fetch=2), expr=[b@0 ASC], preserve_partitioning=[false], filter=[b@0 IS NULL OR b@0 < 2]
  -   ProjectionExec: expr=[a@0 as b, b@1 as a]
  -     DataSourceExec: file_groups={1 group: [[test.parquet]]}, projection=[a, b, c], file_type=test, pushdown_supported=true, predicate=DynamicFilter [ a@0 IS NULL OR a@0 < 2 ]"#.to_string(),
        ],
    })
    .await;

    // Case 6: Confusing expr [a + 1 as a, b]
    run_projection_dyn_filter_case(ProjectionDynFilterTestCase {
        schema: Arc::clone(&schema),
        batches: simple_abc.clone(),
        projection: vec![
            (
                Arc::new(BinaryExpr::new(
                    col("a", &schema).unwrap(),
                    Operator::Plus,
                    Arc::new(Literal::new(ScalarValue::Int32(Some(1)))),
                )),
                "a".to_string(),
            ),
            (col("b", &schema).unwrap(), "b".to_string()),
        ],
        sort_expr: PhysicalSortExpr::new(
            Arc::new(Column::new("a", 0)),
            SortOptions::default(),
        ),
        expected_plans: vec![
            r#"  - SortExec: TopK(fetch=2), expr=[a@0 ASC], preserve_partitioning=[false]
  -   ProjectionExec: expr=[a@0 + 1 as a, b@1 as b]
  -     DataSourceExec: file_groups={1 group: [[test.parquet]]}, projection=[a, b, c], file_type=test, pushdown_supported=true, predicate=DynamicFilter [ empty ]"#.to_string(),
            r#"  - SortExec: TopK(fetch=2), expr=[a@0 ASC], preserve_partitioning=[false], filter=[a@0 IS NULL OR a@0 < 3]
  -   ProjectionExec: expr=[a@0 + 1 as a, b@1 as b]
  -     DataSourceExec: file_groups={1 group: [[test.parquet]]}, projection=[a, b, c], file_type=test, pushdown_supported=true, predicate=DynamicFilter [ a@0 + 1 IS NULL OR a@0 + 1 < 3 ]"#.to_string(),
        ],
    })
    .await;
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

// ==== Aggregate Dynamic Filter tests ====

// ---- Test Utilities ----
struct AggregateDynFilterCase<'a> {
    schema: SchemaRef,
    batches: Vec<RecordBatch>,
    aggr_exprs: Vec<AggregateFunctionExpr>,
    expected_before: Option<&'a str>,
    expected_after: Option<&'a str>,
    scan_support: bool,
}

async fn run_aggregate_dyn_filter_case(case: AggregateDynFilterCase<'_>) {
    let AggregateDynFilterCase {
        schema,
        batches,
        aggr_exprs,
        expected_before,
        expected_after,
        scan_support,
    } = case;

    let scan = TestScanBuilder::new(Arc::clone(&schema))
        .with_support(scan_support)
        .with_batches(batches)
        .build();

    let aggr_exprs: Vec<_> = aggr_exprs
        .into_iter()
        .map(|expr| Arc::new(expr) as Arc<AggregateFunctionExpr>)
        .collect();
    let aggr_len = aggr_exprs.len();

    let plan: Arc<dyn ExecutionPlan> = Arc::new(
        AggregateExec::try_new(
            AggregateMode::Partial,
            PhysicalGroupBy::new_single(vec![]),
            aggr_exprs,
            vec![None; aggr_len],
            scan,
            Arc::clone(&schema),
        )
        .unwrap(),
    );

    let mut config = ConfigOptions::default();
    config.execution.parquet.pushdown_filters = true;
    config.optimizer.enable_dynamic_filter_pushdown = true;

    let optimized = FilterPushdown::new_post_optimization()
        .optimize(plan, &config)
        .unwrap();

    let before = format_plan_for_test(&optimized);
    if let Some(expected) = expected_before {
        assert!(
            before.contains(expected),
            "expected `{expected}` before execution, got: {before}"
        );
    } else {
        assert!(
            !before.contains("DynamicFilter ["),
            "dynamic filter unexpectedly present before execution: {before}"
        );
    }

    let session_ctx = SessionContext::new();
    session_ctx.register_object_store(
        ObjectStoreUrl::parse("test://").unwrap().as_ref(),
        Arc::new(InMemory::new()),
    );
    let task_ctx = session_ctx.state().task_ctx();
    let mut stream = optimized.execute(0, Arc::clone(&task_ctx)).unwrap();
    let _ = stream.next().await.transpose().unwrap();

    let after = format_plan_for_test(&optimized);
    if let Some(expected) = expected_after {
        assert!(
            after.contains(expected),
            "expected `{expected}` after execution, got: {after}"
        );
    } else {
        assert!(
            !after.contains("DynamicFilter ["),
            "dynamic filter unexpectedly present after execution: {after}"
        );
    }
}

// ---- Test Cases ----
// Cases covered below:
// 1. `min(a)` and `max(a)` baseline.
// 2. Unsupported expression input (`min(a+1)`).
// 3. Multiple supported columns (same column vs different columns).
// 4. Mixed supported + unsupported aggregates.
// 5. Entirely NULL input to surface current bound behavior.
// 6. End-to-end tests on parquet files

/// `MIN(a)`: able to pushdown dynamic filter
#[tokio::test]
async fn test_aggregate_dynamic_filter_min_simple() {
    // Single min(a) showcases the base case.
    let schema = Arc::new(Schema::new(vec![Field::new("a", DataType::Int32, true)]));
    let batches = vec![record_batch!(("a", Int32, [5, 1, 3, 8])).unwrap()];

    let min_expr =
        AggregateExprBuilder::new(min_udaf(), vec![col("a", &schema).unwrap()])
            .schema(Arc::clone(&schema))
            .alias("min_a")
            .build()
            .unwrap();

    run_aggregate_dyn_filter_case(AggregateDynFilterCase {
        schema,
        batches,
        aggr_exprs: vec![min_expr],
        expected_before: Some("DynamicFilter [ empty ]"),
        expected_after: Some("DynamicFilter [ a@0 < 1 ]"),
        scan_support: true,
    })
    .await;
}

/// `MAX(a)`: able to pushdown dynamic filter
#[tokio::test]
async fn test_aggregate_dynamic_filter_max_simple() {
    // Single max(a) mirrors the base case on the upper bound.
    let schema = Arc::new(Schema::new(vec![Field::new("a", DataType::Int32, true)]));
    let batches = vec![record_batch!(("a", Int32, [5, 1, 3, 8])).unwrap()];

    let max_expr =
        AggregateExprBuilder::new(max_udaf(), vec![col("a", &schema).unwrap()])
            .schema(Arc::clone(&schema))
            .alias("max_a")
            .build()
            .unwrap();

    run_aggregate_dyn_filter_case(AggregateDynFilterCase {
        schema,
        batches,
        aggr_exprs: vec![max_expr],
        expected_before: Some("DynamicFilter [ empty ]"),
        expected_after: Some("DynamicFilter [ a@0 > 8 ]"),
        scan_support: true,
    })
    .await;
}

/// `MIN(a+1)`: Can't pushdown dynamic filter
#[tokio::test]
async fn test_aggregate_dynamic_filter_min_expression_not_supported() {
    let schema = Arc::new(Schema::new(vec![Field::new("a", DataType::Int32, true)]));
    let batches = vec![record_batch!(("a", Int32, [5, 1, 3, 8])).unwrap()];

    let expr: Arc<dyn PhysicalExpr> = Arc::new(BinaryExpr::new(
        col("a", &schema).unwrap(),
        Operator::Plus,
        Arc::new(Literal::new(ScalarValue::Int32(Some(1)))),
    ));
    let min_expr = AggregateExprBuilder::new(min_udaf(), vec![expr])
        .schema(Arc::clone(&schema))
        .alias("min_a_plus_one")
        .build()
        .unwrap();

    run_aggregate_dyn_filter_case(AggregateDynFilterCase {
        schema,
        batches,
        aggr_exprs: vec![min_expr],
        expected_before: None,
        expected_after: None,
        scan_support: true,
    })
    .await;
}

/// `MIN(a), MAX(a)`: Pushdown dynamic filter like `(a<1) or (a>8)`
#[tokio::test]
async fn test_aggregate_dynamic_filter_min_max_same_column() {
    let schema = Arc::new(Schema::new(vec![Field::new("a", DataType::Int32, true)]));
    let batches = vec![record_batch!(("a", Int32, [5, 1, 3, 8])).unwrap()];

    let min_expr =
        AggregateExprBuilder::new(min_udaf(), vec![col("a", &schema).unwrap()])
            .schema(Arc::clone(&schema))
            .alias("min_a")
            .build()
            .unwrap();
    let max_expr =
        AggregateExprBuilder::new(max_udaf(), vec![col("a", &schema).unwrap()])
            .schema(Arc::clone(&schema))
            .alias("max_a")
            .build()
            .unwrap();

    run_aggregate_dyn_filter_case(AggregateDynFilterCase {
        schema,
        batches,
        aggr_exprs: vec![min_expr, max_expr],
        expected_before: Some("DynamicFilter [ empty ]"),
        expected_after: Some("DynamicFilter [ a@0 < 1 OR a@0 > 8 ]"),
        scan_support: true,
    })
    .await;
}

/// `MIN(a), MAX(b)`: Pushdown dynamic filter like `(a<1) or (b>9)`
#[tokio::test]
async fn test_aggregate_dynamic_filter_min_max_different_columns() {
    let schema = Arc::new(Schema::new(vec![
        Field::new("a", DataType::Int32, true),
        Field::new("b", DataType::Int32, true),
    ]));
    let batches = vec![
        record_batch!(("a", Int32, [5, 1, 3, 8]), ("b", Int32, [7, 2, 4, 9])).unwrap(),
    ];

    let min_expr =
        AggregateExprBuilder::new(min_udaf(), vec![col("a", &schema).unwrap()])
            .schema(Arc::clone(&schema))
            .alias("min_a")
            .build()
            .unwrap();
    let max_expr =
        AggregateExprBuilder::new(max_udaf(), vec![col("b", &schema).unwrap()])
            .schema(Arc::clone(&schema))
            .alias("max_b")
            .build()
            .unwrap();

    run_aggregate_dyn_filter_case(AggregateDynFilterCase {
        schema,
        batches,
        aggr_exprs: vec![min_expr, max_expr],
        expected_before: Some("DynamicFilter [ empty ]"),
        expected_after: Some("DynamicFilter [ a@0 < 1 OR b@1 > 9 ]"),
        scan_support: true,
    })
    .await;
}

/// Mix of supported/unsupported aggregates retains only the valid ones.
/// `MIN(a), MAX(a), MAX(b), MIN(c+1)`: Pushdown dynamic filter like `(a<1) or (a>8) OR (b>12)`
#[tokio::test]
async fn test_aggregate_dynamic_filter_multiple_mixed_expressions() {
    let schema = Arc::new(Schema::new(vec![
        Field::new("a", DataType::Int32, true),
        Field::new("b", DataType::Int32, true),
        Field::new("c", DataType::Int32, true),
    ]));
    let batches = vec![
        record_batch!(
            ("a", Int32, [5, 1, 3, 8]),
            ("b", Int32, [10, 4, 6, 12]),
            ("c", Int32, [100, 70, 90, 110])
        )
        .unwrap(),
    ];

    let min_a = AggregateExprBuilder::new(min_udaf(), vec![col("a", &schema).unwrap()])
        .schema(Arc::clone(&schema))
        .alias("min_a")
        .build()
        .unwrap();
    let max_a = AggregateExprBuilder::new(max_udaf(), vec![col("a", &schema).unwrap()])
        .schema(Arc::clone(&schema))
        .alias("max_a")
        .build()
        .unwrap();
    let max_b = AggregateExprBuilder::new(max_udaf(), vec![col("b", &schema).unwrap()])
        .schema(Arc::clone(&schema))
        .alias("max_b")
        .build()
        .unwrap();
    let expr_c: Arc<dyn PhysicalExpr> = Arc::new(BinaryExpr::new(
        col("c", &schema).unwrap(),
        Operator::Plus,
        Arc::new(Literal::new(ScalarValue::Int32(Some(1)))),
    ));
    let min_c_expr = AggregateExprBuilder::new(min_udaf(), vec![expr_c])
        .schema(Arc::clone(&schema))
        .alias("min_c_plus_one")
        .build()
        .unwrap();

    run_aggregate_dyn_filter_case(AggregateDynFilterCase {
        schema,
        batches,
        aggr_exprs: vec![min_a, max_a, max_b, min_c_expr],
        expected_before: Some("DynamicFilter [ empty ]"),
        expected_after: Some("DynamicFilter [ a@0 < 1 OR a@0 > 8 OR b@1 > 12 ]"),
        scan_support: true,
    })
    .await;
}

/// Don't tighten the dynamic filter if all inputs are null
#[tokio::test]
async fn test_aggregate_dynamic_filter_min_all_nulls() {
    let schema = Arc::new(Schema::new(vec![Field::new("a", DataType::Int32, true)]));
    let batches = vec![record_batch!(("a", Int32, [None, None, None, None])).unwrap()];

    let min_expr =
        AggregateExprBuilder::new(min_udaf(), vec![col("a", &schema).unwrap()])
            .schema(Arc::clone(&schema))
            .alias("min_a")
            .build()
            .unwrap();

    run_aggregate_dyn_filter_case(AggregateDynFilterCase {
        schema,
        batches,
        aggr_exprs: vec![min_expr],
        expected_before: Some("DynamicFilter [ empty ]"),
        // After reading the input it hasn't a meaningful bound to update, so the
        // predicate `true` means don't filter out anything
        expected_after: Some("DynamicFilter [ true ]"),
        scan_support: true,
    })
    .await;
}

/// Test aggregate dynamic filter is working when reading parquet files
///
/// Runs 'select max(id) from test_table where id > 1', and ensure some file ranges
/// pruned by the dynamic filter.
#[tokio::test]
async fn test_aggregate_dynamic_filter_parquet_e2e() {
    let config = SessionConfig::new()
        .with_collect_statistics(true)
        .with_target_partitions(2)
        .set_bool("datafusion.optimizer.enable_dynamic_filter_pushdown", true)
        .set_bool("datafusion.execution.parquet.pushdown_filters", true);
    let ctx = SessionContext::new_with_config(config);

    let data_path = format!(
        "{}/tests/data/test_statistics_per_partition/",
        env!("CARGO_MANIFEST_DIR")
    );

    ctx.register_parquet("test_table", &data_path, ParquetReadOptions::default())
        .await
        .unwrap();

    // partition 1:
    //   files: ..03-01(id=4), ..03-02(id=3)
    // partition 1:
    //   files: ..03-03(id=2), ..03-04(id=1)
    //
    // In partition 1, after reading the first file, the dynamic filter will be update
    // to "id > 4", so the `..03-02` file must be able to get pruned out
    let df = ctx
        .sql("explain analyze select max(id) from test_table where id > 1")
        .await
        .unwrap();

    let result = df.collect().await.unwrap();

    let formatted = pretty_format_batches(&result).unwrap();
    let explain_analyze = format!("{formatted}");

    // Capture "2" from "files_ranges_pruned_statistics=4 total → 2 matched"
    let re = Regex::new(
        r"files_ranges_pruned_statistics\s*=\s*(\d+)\s*total\s*[→>\-]\s*(\d+)\s*matched",
    )
    .unwrap();

    if let Some(caps) = re.captures(&explain_analyze) {
        let matched_num: i32 = caps[2].parse().unwrap();
        assert!(
            matched_num < 4,
            "Total 4 files, if some pruned, the matched count is < 4"
        );
    } else {
        unreachable!("metrics should exist")
    }
}

/// Non-partial (Single) aggregates should skip dynamic filter initialization.
#[test]
fn test_aggregate_dynamic_filter_not_created_for_single_mode() {
    let schema = Arc::new(Schema::new(vec![Field::new("a", DataType::Int32, true)]));
    let batches = vec![record_batch!(("a", Int32, [5, 1, 3, 8])).unwrap()];

    let scan = TestScanBuilder::new(Arc::clone(&schema))
        .with_support(true)
        .with_batches(batches)
        .build();

    let min_expr =
        AggregateExprBuilder::new(min_udaf(), vec![col("a", &schema).unwrap()])
            .schema(Arc::clone(&schema))
            .alias("min_a")
            .build()
            .unwrap();

    let plan: Arc<dyn ExecutionPlan> = Arc::new(
        AggregateExec::try_new(
            AggregateMode::Single,
            PhysicalGroupBy::new_single(vec![]),
            vec![min_expr.into()],
            vec![None],
            scan,
            Arc::clone(&schema),
        )
        .unwrap(),
    );

    let mut config = ConfigOptions::default();
    config.execution.parquet.pushdown_filters = true;
    config.optimizer.enable_dynamic_filter_pushdown = true;

    let optimized = FilterPushdown::new_post_optimization()
        .optimize(plan, &config)
        .unwrap();

    let formatted = format_plan_for_test(&optimized);
    assert!(
        !formatted.contains("DynamicFilter ["),
        "dynamic filter should not be created for AggregateMode::Single: {formatted}"
    );
}

#[tokio::test]
async fn test_aggregate_filter_pushdown() {
    // Test that filters can pass through AggregateExec even with aggregate functions
    // when the filter references grouping columns
    // Simulates: SELECT a, COUNT(b) FROM table WHERE a = 'x' GROUP BY a

    let batches = vec![
        record_batch!(("a", Utf8, ["x", "y"]), ("b", Utf8, ["foo", "bar"])).unwrap(),
    ];

    let scan = TestScanBuilder::new(schema())
        .with_support(true)
        .with_batches(batches)
        .build();

    // Create an aggregate: GROUP BY a with COUNT(b)
    let group_by = PhysicalGroupBy::new_single(vec![(
        col("a", &schema()).unwrap(),
        "a".to_string(),
    )]);

    // Add COUNT aggregate
    let count_expr =
        AggregateExprBuilder::new(count_udaf(), vec![col("b", &schema()).unwrap()])
            .schema(schema())
            .alias("count")
            .build()
            .unwrap();

    let aggregate = Arc::new(
        AggregateExec::try_new(
            AggregateMode::Partial,
            group_by,
            vec![count_expr.into()], // Has aggregate function
            vec![None],              // No filter on the aggregate function
            Arc::clone(&scan),
            schema(),
        )
        .unwrap(),
    );

    // Add a filter on the grouping column 'a'
    let predicate = col_lit_predicate("a", "x", &schema());
    let plan = Arc::new(FilterExec::try_new(predicate, aggregate).unwrap())
        as Arc<dyn ExecutionPlan>;

    // Even with aggregate functions, filter on grouping column should be pushed through
    insta::assert_snapshot!(
        OptimizationTest::new(Arc::clone(&plan), FilterPushdown::new(), true),
        @r"
    OptimizationTest:
      input:
        - FilterExec: a@0 = x
        -   AggregateExec: mode=Partial, gby=[a@0 as a], aggr=[count]
        -     DataSourceExec: file_groups={1 group: [[test.parquet]]}, projection=[a, b, c], file_type=test, pushdown_supported=true
      output:
        Ok:
          - AggregateExec: mode=Partial, gby=[a@0 as a], aggr=[count], ordering_mode=Sorted
          -   DataSourceExec: file_groups={1 group: [[test.parquet]]}, projection=[a, b, c], file_type=test, pushdown_supported=true, predicate=a@0 = x
    "
    );
}

#[tokio::test]
async fn test_no_pushdown_filter_on_aggregate_result() {
    // Test that filters on aggregate results (not grouping columns) are NOT pushed through
    // SELECT a, COUNT(b) as cnt FROM table GROUP BY a HAVING cnt > 5
    // The filter on 'cnt' cannot be pushed down because it's an aggregate result

    let batches = vec![
        record_batch!(("a", Utf8, ["x", "y"]), ("b", Utf8, ["foo", "bar"])).unwrap(),
    ];

    let scan = TestScanBuilder::new(schema())
        .with_support(true)
        .with_batches(batches)
        .build();

    // Create an aggregate: GROUP BY a with COUNT(b)
    let group_by = PhysicalGroupBy::new_single(vec![(
        col("a", &schema()).unwrap(),
        "a".to_string(),
    )]);

    // Add COUNT aggregate
    let count_expr =
        AggregateExprBuilder::new(count_udaf(), vec![col("b", &schema()).unwrap()])
            .schema(schema())
            .alias("count")
            .build()
            .unwrap();

    let aggregate = Arc::new(
        AggregateExec::try_new(
            AggregateMode::Partial,
            group_by,
            vec![count_expr.into()],
            vec![None],
            Arc::clone(&scan),
            schema(),
        )
        .unwrap(),
    );

    // Add a filter on the aggregate output column
    // This simulates filtering on COUNT result, which should NOT be pushed through
    let agg_schema = aggregate.schema();
    let predicate = Arc::new(BinaryExpr::new(
        Arc::new(Column::new_with_schema("count[count]", &agg_schema).unwrap()),
        Operator::Gt,
        Arc::new(Literal::new(ScalarValue::Int64(Some(5)))),
    ));
    let plan = Arc::new(FilterExec::try_new(predicate, aggregate).unwrap())
        as Arc<dyn ExecutionPlan>;

    // The filter should NOT be pushed through the aggregate since it's on an aggregate result
    insta::assert_snapshot!(
        OptimizationTest::new(Arc::clone(&plan), FilterPushdown::new(), true),
        @r"
    OptimizationTest:
      input:
        - FilterExec: count[count]@1 > 5
        -   AggregateExec: mode=Partial, gby=[a@0 as a], aggr=[count]
        -     DataSourceExec: file_groups={1 group: [[test.parquet]]}, projection=[a, b, c], file_type=test, pushdown_supported=true
      output:
        Ok:
          - FilterExec: count[count]@1 > 5
          -   AggregateExec: mode=Partial, gby=[a@0 as a], aggr=[count]
          -     DataSourceExec: file_groups={1 group: [[test.parquet]]}, projection=[a, b, c], file_type=test, pushdown_supported=true
    "
    );
}

#[test]
fn test_pushdown_filter_on_non_first_grouping_column() {
    // Test that filters on non-first grouping columns are still pushed down
    // SELECT a, b, count(*) as cnt FROM table GROUP BY a, b HAVING b = 'bar'
    // The filter is on 'b' (second grouping column), should push down
    let scan = TestScanBuilder::new(schema()).with_support(true).build();

    let aggregate_expr = vec![
        AggregateExprBuilder::new(count_udaf(), vec![col("c", &schema()).unwrap()])
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
            scan,
            schema(),
        )
        .unwrap(),
    );

    let predicate = col_lit_predicate("b", "bar", &schema());
    let plan = Arc::new(FilterExec::try_new(predicate, aggregate).unwrap());

    insta::assert_snapshot!(
        OptimizationTest::new(plan, FilterPushdown::new(), true),
        @r"
    OptimizationTest:
      input:
        - FilterExec: b@1 = bar
        -   AggregateExec: mode=Final, gby=[a@0 as a, b@1 as b], aggr=[cnt]
        -     DataSourceExec: file_groups={1 group: [[test.parquet]]}, projection=[a, b, c], file_type=test, pushdown_supported=true
      output:
        Ok:
          - AggregateExec: mode=Final, gby=[a@0 as a, b@1 as b], aggr=[cnt], ordering_mode=PartiallySorted([1])
          -   DataSourceExec: file_groups={1 group: [[test.parquet]]}, projection=[a, b, c], file_type=test, pushdown_supported=true, predicate=b@1 = bar
    "
    );
}

#[test]
fn test_no_pushdown_grouping_sets_filter_on_missing_column() {
    // Test that filters on columns missing from some grouping sets are NOT pushed through
    let scan = TestScanBuilder::new(schema()).with_support(true).build();

    let aggregate_expr = vec![
        AggregateExprBuilder::new(count_udaf(), vec![col("c", &schema()).unwrap()])
            .schema(schema())
            .alias("cnt")
            .build()
            .map(Arc::new)
            .unwrap(),
    ];

    // Create GROUPING SETS with (a, b) and (b)
    let group_by = PhysicalGroupBy::new(
        vec![
            (col("a", &schema()).unwrap(), "a".to_string()),
            (col("b", &schema()).unwrap(), "b".to_string()),
        ],
        vec![
            (
                Arc::new(Literal::new(ScalarValue::Utf8(None))),
                "a".to_string(),
            ),
            (
                Arc::new(Literal::new(ScalarValue::Utf8(None))),
                "b".to_string(),
            ),
        ],
        vec![
            vec![false, false], // (a, b) - both present
            vec![true, false],  // (b) - a is NULL, b present
        ],
        true,
    );

    let aggregate = Arc::new(
        AggregateExec::try_new(
            AggregateMode::Final,
            group_by,
            aggregate_expr.clone(),
            vec![None],
            scan,
            schema(),
        )
        .unwrap(),
    );

    // Filter on column 'a' which is missing in the second grouping set, should not be pushed down
    let predicate = col_lit_predicate("a", "foo", &schema());
    let plan = Arc::new(FilterExec::try_new(predicate, aggregate).unwrap());

    insta::assert_snapshot!(
        OptimizationTest::new(plan, FilterPushdown::new(), true),
        @r"
    OptimizationTest:
      input:
        - FilterExec: a@0 = foo
        -   AggregateExec: mode=Final, gby=[(a@0 as a, b@1 as b), (NULL as a, b@1 as b)], aggr=[cnt]
        -     DataSourceExec: file_groups={1 group: [[test.parquet]]}, projection=[a, b, c], file_type=test, pushdown_supported=true
      output:
        Ok:
          - FilterExec: a@0 = foo
          -   AggregateExec: mode=Final, gby=[(a@0 as a, b@1 as b), (NULL as a, b@1 as b)], aggr=[cnt]
          -     DataSourceExec: file_groups={1 group: [[test.parquet]]}, projection=[a, b, c], file_type=test, pushdown_supported=true
    "
    );
}

#[test]
fn test_pushdown_grouping_sets_filter_on_common_column() {
    // Test that filters on columns present in ALL grouping sets ARE pushed through
    let scan = TestScanBuilder::new(schema()).with_support(true).build();

    let aggregate_expr = vec![
        AggregateExprBuilder::new(count_udaf(), vec![col("c", &schema()).unwrap()])
            .schema(schema())
            .alias("cnt")
            .build()
            .map(Arc::new)
            .unwrap(),
    ];

    // Create GROUPING SETS with (a, b) and (b)
    let group_by = PhysicalGroupBy::new(
        vec![
            (col("a", &schema()).unwrap(), "a".to_string()),
            (col("b", &schema()).unwrap(), "b".to_string()),
        ],
        vec![
            (
                Arc::new(Literal::new(ScalarValue::Utf8(None))),
                "a".to_string(),
            ),
            (
                Arc::new(Literal::new(ScalarValue::Utf8(None))),
                "b".to_string(),
            ),
        ],
        vec![
            vec![false, false], // (a, b) - both present
            vec![true, false],  // (b) - a is NULL, b present
        ],
        true,
    );

    let aggregate = Arc::new(
        AggregateExec::try_new(
            AggregateMode::Final,
            group_by,
            aggregate_expr.clone(),
            vec![None],
            scan,
            schema(),
        )
        .unwrap(),
    );

    // Filter on column 'b' which is present in all grouping sets will be pushed down
    let predicate = col_lit_predicate("b", "bar", &schema());
    let plan = Arc::new(FilterExec::try_new(predicate, aggregate).unwrap());

    insta::assert_snapshot!(
        OptimizationTest::new(plan, FilterPushdown::new(), true),
        @r"
    OptimizationTest:
      input:
        - FilterExec: b@1 = bar
        -   AggregateExec: mode=Final, gby=[(a@0 as a, b@1 as b), (NULL as a, b@1 as b)], aggr=[cnt]
        -     DataSourceExec: file_groups={1 group: [[test.parquet]]}, projection=[a, b, c], file_type=test, pushdown_supported=true
      output:
        Ok:
          - AggregateExec: mode=Final, gby=[(a@0 as a, b@1 as b), (NULL as a, b@1 as b)], aggr=[cnt], ordering_mode=PartiallySorted([1])
          -   DataSourceExec: file_groups={1 group: [[test.parquet]]}, projection=[a, b, c], file_type=test, pushdown_supported=true, predicate=b@1 = bar
    "
    );
}

#[test]
fn test_pushdown_with_empty_group_by() {
    // Test that filters can be pushed down when GROUP BY is empty (no grouping columns)
    // SELECT count(*) as cnt FROM table WHERE a = 'foo'
    // There are no grouping columns, so the filter should still push down
    let scan = TestScanBuilder::new(schema()).with_support(true).build();

    let aggregate_expr = vec![
        AggregateExprBuilder::new(count_udaf(), vec![col("c", &schema()).unwrap()])
            .schema(schema())
            .alias("cnt")
            .build()
            .map(Arc::new)
            .unwrap(),
    ];

    // Empty GROUP BY - no grouping columns
    let group_by = PhysicalGroupBy::new_single(vec![]);

    let aggregate = Arc::new(
        AggregateExec::try_new(
            AggregateMode::Final,
            group_by,
            aggregate_expr.clone(),
            vec![None],
            scan,
            schema(),
        )
        .unwrap(),
    );

    // Filter on 'a'
    let predicate = col_lit_predicate("a", "foo", &schema());
    let plan = Arc::new(FilterExec::try_new(predicate, aggregate).unwrap());

    // The filter should be pushed down even with empty GROUP BY
    insta::assert_snapshot!(
        OptimizationTest::new(plan, FilterPushdown::new(), true),
        @r"
    OptimizationTest:
      input:
        - FilterExec: a@0 = foo
        -   AggregateExec: mode=Final, gby=[], aggr=[cnt]
        -     DataSourceExec: file_groups={1 group: [[test.parquet]]}, projection=[a, b, c], file_type=test, pushdown_supported=true
      output:
        Ok:
          - AggregateExec: mode=Final, gby=[], aggr=[cnt]
          -   DataSourceExec: file_groups={1 group: [[test.parquet]]}, projection=[a, b, c], file_type=test, pushdown_supported=true, predicate=a@0 = foo
    "
    );
}

#[test]
fn test_pushdown_with_computed_grouping_key() {
    // Test filter pushdown with computed grouping expression
    // SELECT (c + 1.0) as c_plus_1, count(*) FROM table WHERE c > 5.0 GROUP BY (c + 1.0)

    let scan = TestScanBuilder::new(schema()).with_support(true).build();

    let predicate = Arc::new(BinaryExpr::new(
        col("c", &schema()).unwrap(),
        Operator::Gt,
        Arc::new(Literal::new(ScalarValue::Float64(Some(5.0)))),
    )) as Arc<dyn PhysicalExpr>;
    let filter = Arc::new(FilterExec::try_new(predicate, scan).unwrap());

    let aggregate_expr = vec![
        AggregateExprBuilder::new(count_udaf(), vec![col("a", &schema()).unwrap()])
            .schema(schema())
            .alias("cnt")
            .build()
            .map(Arc::new)
            .unwrap(),
    ];

    let c_plus_one = Arc::new(BinaryExpr::new(
        col("c", &schema()).unwrap(),
        Operator::Plus,
        Arc::new(Literal::new(ScalarValue::Float64(Some(1.0)))),
    )) as Arc<dyn PhysicalExpr>;

    let group_by =
        PhysicalGroupBy::new_single(vec![(c_plus_one, "c_plus_1".to_string())]);

    let plan = Arc::new(
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

    // The filter should be pushed down because 'c' is extracted from the grouping expression (c + 1.0)
    insta::assert_snapshot!(
        OptimizationTest::new(plan, FilterPushdown::new(), true),
        @r"
    OptimizationTest:
      input:
        - AggregateExec: mode=Final, gby=[c@2 + 1 as c_plus_1], aggr=[cnt]
        -   FilterExec: c@2 > 5
        -     DataSourceExec: file_groups={1 group: [[test.parquet]]}, projection=[a, b, c], file_type=test, pushdown_supported=true
      output:
        Ok:
          - AggregateExec: mode=Final, gby=[c@2 + 1 as c_plus_1], aggr=[cnt]
          -   DataSourceExec: file_groups={1 group: [[test.parquet]]}, projection=[a, b, c], file_type=test, pushdown_supported=true, predicate=c@2 > 5
    "
    );
}

#[tokio::test]
async fn test_hashjoin_dynamic_filter_all_partitions_empty() {
    use datafusion_common::JoinType;
    use datafusion_physical_plan::joins::{HashJoinExec, PartitionMode};

    // Test scenario where all build-side partitions are empty
    // This validates the code path that sets the filter to `false` when no rows can match

    // Create empty build side
    let build_batches = vec![];
    let build_side_schema = Arc::new(Schema::new(vec![
        Field::new("a", DataType::Utf8, false),
        Field::new("b", DataType::Utf8, false),
    ]));
    let build_scan = TestScanBuilder::new(Arc::clone(&build_side_schema))
        .with_support(true)
        .with_batches(build_batches)
        .build();

    // Create probe side with some data
    let probe_batches = vec![
        record_batch!(
            ("a", Utf8, ["aa", "ab", "ac"]),
            ("b", Utf8, ["ba", "bb", "bc"])
        )
        .unwrap(),
    ];
    let probe_side_schema = Arc::new(Schema::new(vec![
        Field::new("a", DataType::Utf8, false),
        Field::new("b", DataType::Utf8, false),
    ]));
    let probe_scan = TestScanBuilder::new(Arc::clone(&probe_side_schema))
        .with_support(true)
        .with_batches(probe_batches)
        .build();

    // Create RepartitionExec nodes for both sides
    let partition_count = 4;

    let build_hash_exprs = vec![
        col("a", &build_side_schema).unwrap(),
        col("b", &build_side_schema).unwrap(),
    ];
    let build_repartition = Arc::new(
        RepartitionExec::try_new(
            build_scan,
            Partitioning::Hash(build_hash_exprs, partition_count),
        )
        .unwrap(),
    );

    let probe_hash_exprs = vec![
        col("a", &probe_side_schema).unwrap(),
        col("b", &probe_side_schema).unwrap(),
    ];
    let probe_repartition = Arc::new(
        RepartitionExec::try_new(
            Arc::clone(&probe_scan),
            Partitioning::Hash(probe_hash_exprs, partition_count),
        )
        .unwrap(),
    );

    // Create HashJoinExec
    let on = vec![
        (
            col("a", &build_side_schema).unwrap(),
            col("a", &probe_side_schema).unwrap(),
        ),
        (
            col("b", &build_side_schema).unwrap(),
            col("b", &probe_side_schema).unwrap(),
        ),
    ];
    let plan = Arc::new(
        HashJoinExec::try_new(
            build_repartition,
            probe_repartition,
            on,
            None,
            &JoinType::Inner,
            None,
            PartitionMode::Partitioned,
            datafusion_common::NullEquality::NullEqualsNothing,
            false,
        )
        .unwrap(),
    );

    // Apply the filter pushdown optimizer
    let mut config = SessionConfig::new();
    config.options_mut().execution.parquet.pushdown_filters = true;
    let optimizer = FilterPushdown::new_post_optimization();
    let plan = optimizer.optimize(plan, config.options()).unwrap();

    insta::assert_snapshot!(
        format_plan_for_test(&plan),
        @r"
    - HashJoinExec: mode=Partitioned, join_type=Inner, on=[(a@0, a@0), (b@1, b@1)]
    -   RepartitionExec: partitioning=Hash([a@0, b@1], 4), input_partitions=1
    -     DataSourceExec: file_groups={1 group: [[test.parquet]]}, projection=[a, b], file_type=test, pushdown_supported=true
    -   RepartitionExec: partitioning=Hash([a@0, b@1], 4), input_partitions=1
    -     DataSourceExec: file_groups={1 group: [[test.parquet]]}, projection=[a, b], file_type=test, pushdown_supported=true, predicate=DynamicFilter [ empty ]
    "
    );

    // Put some data through the plan to check that the filter is updated to reflect the TopK state
    let session_ctx = SessionContext::new_with_config(config);
    session_ctx.register_object_store(
        ObjectStoreUrl::parse("test://").unwrap().as_ref(),
        Arc::new(InMemory::new()),
    );
    let state = session_ctx.state();
    let task_ctx = state.task_ctx();
    // Execute all partitions (required for partitioned hash join coordination)
    let _batches = collect(Arc::clone(&plan), Arc::clone(&task_ctx))
        .await
        .unwrap();

    // Test that filters are pushed down correctly to each side of the join
    insta::assert_snapshot!(
        format_plan_for_test(&plan),
        @r"
    - HashJoinExec: mode=Partitioned, join_type=Inner, on=[(a@0, a@0), (b@1, b@1)]
    -   RepartitionExec: partitioning=Hash([a@0, b@1], 4), input_partitions=1
    -     DataSourceExec: file_groups={1 group: [[test.parquet]]}, projection=[a, b], file_type=test, pushdown_supported=true
    -   RepartitionExec: partitioning=Hash([a@0, b@1], 4), input_partitions=1
    -     DataSourceExec: file_groups={1 group: [[test.parquet]]}, projection=[a, b], file_type=test, pushdown_supported=true, predicate=DynamicFilter [ false ]
    "
    );
}

#[tokio::test]
async fn test_hashjoin_dynamic_filter_with_nulls() {
    use datafusion_common::JoinType;
    use datafusion_physical_plan::joins::{HashJoinExec, PartitionMode};

    // Test scenario where build side has NULL values in join keys
    // This validates NULL handling in bounds computation and filter generation

    // Create build side with NULL values
    let build_batch = RecordBatch::try_new(
        Arc::new(Schema::new(vec![
            Field::new("a", DataType::Utf8, true),  // nullable
            Field::new("b", DataType::Int32, true), // nullable
        ])),
        vec![
            Arc::new(StringArray::from(vec![Some("aa"), None, Some("ab")])),
            Arc::new(Int32Array::from(vec![Some(1), Some(2), None])),
        ],
    )
    .unwrap();
    let build_batches = vec![build_batch];
    let build_side_schema = Arc::new(Schema::new(vec![
        Field::new("a", DataType::Utf8, true),
        Field::new("b", DataType::Int32, true),
    ]));
    let build_scan = TestScanBuilder::new(Arc::clone(&build_side_schema))
        .with_support(true)
        .with_batches(build_batches)
        .build();

    // Create probe side with nullable fields
    let probe_batch = RecordBatch::try_new(
        Arc::new(Schema::new(vec![
            Field::new("a", DataType::Utf8, true),
            Field::new("b", DataType::Int32, true),
            Field::new("c", DataType::Float64, false),
        ])),
        vec![
            Arc::new(StringArray::from(vec![
                Some("aa"),
                Some("ab"),
                Some("ac"),
                None,
            ])),
            Arc::new(Int32Array::from(vec![Some(1), Some(3), Some(4), Some(5)])),
            Arc::new(Float64Array::from(vec![1.0, 2.0, 3.0, 4.0])),
        ],
    )
    .unwrap();
    let probe_batches = vec![probe_batch];
    let probe_side_schema = Arc::new(Schema::new(vec![
        Field::new("a", DataType::Utf8, true),
        Field::new("b", DataType::Int32, true),
        Field::new("c", DataType::Float64, false),
    ]));
    let probe_scan = TestScanBuilder::new(Arc::clone(&probe_side_schema))
        .with_support(true)
        .with_batches(probe_batches)
        .build();

    // Create HashJoinExec in CollectLeft mode (simpler for this test)
    let on = vec![
        (
            col("a", &build_side_schema).unwrap(),
            col("a", &probe_side_schema).unwrap(),
        ),
        (
            col("b", &build_side_schema).unwrap(),
            col("b", &probe_side_schema).unwrap(),
        ),
    ];
    let plan = Arc::new(
        HashJoinExec::try_new(
            build_scan,
            Arc::clone(&probe_scan),
            on,
            None,
            &JoinType::Inner,
            None,
            PartitionMode::CollectLeft,
            datafusion_common::NullEquality::NullEqualsNothing,
            false,
        )
        .unwrap(),
    );

    // Apply the filter pushdown optimizer
    let mut config = SessionConfig::new();
    config.options_mut().execution.parquet.pushdown_filters = true;
    let optimizer = FilterPushdown::new_post_optimization();
    let plan = optimizer.optimize(plan, config.options()).unwrap();

    insta::assert_snapshot!(
        format_plan_for_test(&plan),
        @r"
    - HashJoinExec: mode=CollectLeft, join_type=Inner, on=[(a@0, a@0), (b@1, b@1)]
    -   DataSourceExec: file_groups={1 group: [[test.parquet]]}, projection=[a, b], file_type=test, pushdown_supported=true
    -   DataSourceExec: file_groups={1 group: [[test.parquet]]}, projection=[a, b, c], file_type=test, pushdown_supported=true, predicate=DynamicFilter [ empty ]
    "
    );

    // Put some data through the plan to check that the filter is updated to reflect the TopK state
    let session_ctx = SessionContext::new_with_config(config);
    session_ctx.register_object_store(
        ObjectStoreUrl::parse("test://").unwrap().as_ref(),
        Arc::new(InMemory::new()),
    );
    let state = session_ctx.state();
    let task_ctx = state.task_ctx();
    // Execute all partitions (required for partitioned hash join coordination)
    let batches = collect(Arc::clone(&plan), Arc::clone(&task_ctx))
        .await
        .unwrap();

    // Test that filters are pushed down correctly to each side of the join
    insta::assert_snapshot!(
        format_plan_for_test(&plan),
        @r"
    - HashJoinExec: mode=CollectLeft, join_type=Inner, on=[(a@0, a@0), (b@1, b@1)]
    -   DataSourceExec: file_groups={1 group: [[test.parquet]]}, projection=[a, b], file_type=test, pushdown_supported=true
    -   DataSourceExec: file_groups={1 group: [[test.parquet]]}, projection=[a, b, c], file_type=test, pushdown_supported=true, predicate=DynamicFilter [ a@0 >= aa AND a@0 <= ab AND b@1 >= 1 AND b@1 <= 2 AND struct(a@0, b@1) IN (SET) ([{c0:aa,c1:1}, {c0:,c1:2}, {c0:ab,c1:}]) ]
    "
    );

    #[rustfmt::skip]
    let expected = [
        "+----+---+----+---+-----+",
        "| a  | b | a  | b | c   |",
        "+----+---+----+---+-----+",
        "| aa | 1 | aa | 1 | 1.0 |",
        "+----+---+----+---+-----+",
    ];
    assert_batches_eq!(&expected, &batches);
}

/// Test that when hash_join_inlist_pushdown_max_size is set to a very small value,
/// the HashTable strategy is used instead of InList strategy, even with small build sides.
/// This test is identical to test_hashjoin_dynamic_filter_pushdown_partitioned except
/// for the config setting that forces the HashTable strategy.
#[tokio::test]
async fn test_hashjoin_hash_table_pushdown_partitioned() {
    use datafusion_common::JoinType;
    use datafusion_physical_plan::joins::{HashJoinExec, PartitionMode};

    // Create build side with limited values
    let build_batches = vec![
        record_batch!(
            ("a", Utf8, ["aa", "ab"]),
            ("b", Utf8, ["ba", "bb"]),
            ("c", Float64, [1.0, 2.0]) // Extra column not used in join
        )
        .unwrap(),
    ];
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
    let probe_batches = vec![
        record_batch!(
            ("a", Utf8, ["aa", "ab", "ac", "ad"]),
            ("b", Utf8, ["ba", "bb", "bc", "bd"]),
            ("e", Float64, [1.0, 2.0, 3.0, 4.0]) // Extra column not used in join
        )
        .unwrap(),
    ];
    let probe_side_schema = Arc::new(Schema::new(vec![
        Field::new("a", DataType::Utf8, false),
        Field::new("b", DataType::Utf8, false),
        Field::new("e", DataType::Float64, false),
    ]));
    let probe_scan = TestScanBuilder::new(Arc::clone(&probe_side_schema))
        .with_support(true)
        .with_batches(probe_batches)
        .build();

    // Create RepartitionExec nodes for both sides with hash partitioning on join keys
    let partition_count = 12;

    // Build side: DataSource -> RepartitionExec (Hash)
    let build_hash_exprs = vec![
        col("a", &build_side_schema).unwrap(),
        col("b", &build_side_schema).unwrap(),
    ];
    let build_repartition = Arc::new(
        RepartitionExec::try_new(
            build_scan,
            Partitioning::Hash(build_hash_exprs, partition_count),
        )
        .unwrap(),
    );

    // Probe side: DataSource -> RepartitionExec (Hash)
    let probe_hash_exprs = vec![
        col("a", &probe_side_schema).unwrap(),
        col("b", &probe_side_schema).unwrap(),
    ];
    let probe_repartition = Arc::new(
        RepartitionExec::try_new(
            Arc::clone(&probe_scan),
            Partitioning::Hash(probe_hash_exprs, partition_count),
        )
        .unwrap(),
    );

    // Create HashJoinExec with partitioned inputs
    let on = vec![
        (
            col("a", &build_side_schema).unwrap(),
            col("a", &probe_side_schema).unwrap(),
        ),
        (
            col("b", &build_side_schema).unwrap(),
            col("b", &probe_side_schema).unwrap(),
        ),
    ];
    let hash_join = Arc::new(
        HashJoinExec::try_new(
            build_repartition,
            probe_repartition,
            on,
            None,
            &JoinType::Inner,
            None,
            PartitionMode::Partitioned,
            datafusion_common::NullEquality::NullEqualsNothing,
            false,
        )
        .unwrap(),
    );

    // Top-level CoalescePartitionsExec
    let cp = Arc::new(CoalescePartitionsExec::new(hash_join)) as Arc<dyn ExecutionPlan>;
    // Add a sort for deterministic output
    let plan = Arc::new(SortExec::new(
        LexOrdering::new(vec![PhysicalSortExpr::new(
            col("a", &probe_side_schema).unwrap(),
            SortOptions::new(true, false), // descending, nulls_first
        )])
        .unwrap(),
        cp,
    )) as Arc<dyn ExecutionPlan>;

    // Apply the optimization with config setting that forces HashTable strategy
    let session_config = SessionConfig::default()
        .with_batch_size(10)
        .set_usize("datafusion.optimizer.hash_join_inlist_pushdown_max_size", 1)
        .set_bool("datafusion.execution.parquet.pushdown_filters", true)
        .set_bool("datafusion.optimizer.enable_dynamic_filter_pushdown", true);
    let plan = FilterPushdown::new_post_optimization()
        .optimize(plan, session_config.options())
        .unwrap();
    let session_ctx = SessionContext::new_with_config(session_config);
    session_ctx.register_object_store(
        ObjectStoreUrl::parse("test://").unwrap().as_ref(),
        Arc::new(InMemory::new()),
    );
    let state = session_ctx.state();
    let task_ctx = state.task_ctx();
    let batches = collect(Arc::clone(&plan), Arc::clone(&task_ctx))
        .await
        .unwrap();

    // Verify that hash_lookup is used instead of IN (SET)
    let plan_str = format_plan_for_test(&plan).to_string();
    assert!(
        plan_str.contains("hash_lookup"),
        "Expected hash_lookup in plan but got: {plan_str}"
    );
    assert!(
        !plan_str.contains("IN (SET)"),
        "Expected no IN (SET) in plan but got: {plan_str}"
    );

    let result = format!("{}", pretty_format_batches(&batches).unwrap());

    let probe_scan_metrics = probe_scan.metrics().unwrap();

    // The probe side had 4 rows, but after applying the dynamic filter only 2 rows should remain.
    assert_eq!(probe_scan_metrics.output_rows().unwrap(), 2);

    // Results should be identical to the InList version
    insta::assert_snapshot!(
        result,
        @r"
    +----+----+-----+----+----+-----+
    | a  | b  | c   | a  | b  | e   |
    +----+----+-----+----+----+-----+
    | ab | bb | 2.0 | ab | bb | 2.0 |
    | aa | ba | 1.0 | aa | ba | 1.0 |
    +----+----+-----+----+----+-----+
    ",
    );
}

/// Test that when hash_join_inlist_pushdown_max_size is set to a very small value,
/// the HashTable strategy is used instead of InList strategy in CollectLeft mode.
/// This test is identical to test_hashjoin_dynamic_filter_pushdown_collect_left except
/// for the config setting that forces the HashTable strategy.
#[tokio::test]
async fn test_hashjoin_hash_table_pushdown_collect_left() {
    use datafusion_common::JoinType;
    use datafusion_physical_plan::joins::{HashJoinExec, PartitionMode};

    let build_batches = vec![
        record_batch!(
            ("a", Utf8, ["aa", "ab"]),
            ("b", Utf8, ["ba", "bb"]),
            ("c", Float64, [1.0, 2.0]) // Extra column not used in join
        )
        .unwrap(),
    ];
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
    let probe_batches = vec![
        record_batch!(
            ("a", Utf8, ["aa", "ab", "ac", "ad"]),
            ("b", Utf8, ["ba", "bb", "bc", "bd"]),
            ("e", Float64, [1.0, 2.0, 3.0, 4.0]) // Extra column not used in join
        )
        .unwrap(),
    ];
    let probe_side_schema = Arc::new(Schema::new(vec![
        Field::new("a", DataType::Utf8, false),
        Field::new("b", DataType::Utf8, false),
        Field::new("e", DataType::Float64, false),
    ]));
    let probe_scan = TestScanBuilder::new(Arc::clone(&probe_side_schema))
        .with_support(true)
        .with_batches(probe_batches)
        .build();

    // Create RepartitionExec nodes for both sides with hash partitioning on join keys
    let partition_count = 12;

    // Probe side: DataSource -> RepartitionExec(Hash)
    let probe_hash_exprs = vec![
        col("a", &probe_side_schema).unwrap(),
        col("b", &probe_side_schema).unwrap(),
    ];
    let probe_repartition = Arc::new(
        RepartitionExec::try_new(
            Arc::clone(&probe_scan),
            Partitioning::Hash(probe_hash_exprs, partition_count), // create multi partitions on probSide
        )
        .unwrap(),
    );

    let on = vec![
        (
            col("a", &build_side_schema).unwrap(),
            col("a", &probe_side_schema).unwrap(),
        ),
        (
            col("b", &build_side_schema).unwrap(),
            col("b", &probe_side_schema).unwrap(),
        ),
    ];
    let hash_join = Arc::new(
        HashJoinExec::try_new(
            build_scan,
            probe_repartition,
            on,
            None,
            &JoinType::Inner,
            None,
            PartitionMode::CollectLeft,
            datafusion_common::NullEquality::NullEqualsNothing,
            false,
        )
        .unwrap(),
    );

    // Top-level CoalescePartitionsExec
    let cp = Arc::new(CoalescePartitionsExec::new(hash_join)) as Arc<dyn ExecutionPlan>;
    // Add a sort for deterministic output
    let plan = Arc::new(SortExec::new(
        LexOrdering::new(vec![PhysicalSortExpr::new(
            col("a", &probe_side_schema).unwrap(),
            SortOptions::new(true, false), // descending, nulls_first
        )])
        .unwrap(),
        cp,
    )) as Arc<dyn ExecutionPlan>;

    // Apply the optimization with config setting that forces HashTable strategy
    let session_config = SessionConfig::default()
        .with_batch_size(10)
        .set_usize("datafusion.optimizer.hash_join_inlist_pushdown_max_size", 1)
        .set_bool("datafusion.execution.parquet.pushdown_filters", true)
        .set_bool("datafusion.optimizer.enable_dynamic_filter_pushdown", true);
    let plan = FilterPushdown::new_post_optimization()
        .optimize(plan, session_config.options())
        .unwrap();
    let session_ctx = SessionContext::new_with_config(session_config);
    session_ctx.register_object_store(
        ObjectStoreUrl::parse("test://").unwrap().as_ref(),
        Arc::new(InMemory::new()),
    );
    let state = session_ctx.state();
    let task_ctx = state.task_ctx();
    let batches = collect(Arc::clone(&plan), Arc::clone(&task_ctx))
        .await
        .unwrap();

    // Verify that hash_lookup is used instead of IN (SET)
    let plan_str = format_plan_for_test(&plan).to_string();
    assert!(
        plan_str.contains("hash_lookup"),
        "Expected hash_lookup in plan but got: {plan_str}"
    );
    assert!(
        !plan_str.contains("IN (SET)"),
        "Expected no IN (SET) in plan but got: {plan_str}"
    );

    let result = format!("{}", pretty_format_batches(&batches).unwrap());

    let probe_scan_metrics = probe_scan.metrics().unwrap();

    // The probe side had 4 rows, but after applying the dynamic filter only 2 rows should remain.
    assert_eq!(probe_scan_metrics.output_rows().unwrap(), 2);

    // Results should be identical to the InList version
    insta::assert_snapshot!(
        result,
        @r"
    +----+----+-----+----+----+-----+
    | a  | b  | c   | a  | b  | e   |
    +----+----+-----+----+----+-----+
    | ab | bb | 2.0 | ab | bb | 2.0 |
    | aa | ba | 1.0 | aa | ba | 1.0 |
    +----+----+-----+----+----+-----+
    ",
    );
}

/// Test HashTable strategy with integer multi-column join keys.
/// Verifies that hash_lookup works correctly with integer data types.
#[tokio::test]
async fn test_hashjoin_hash_table_pushdown_integer_keys() {
    use datafusion_common::JoinType;
    use datafusion_physical_plan::joins::{HashJoinExec, PartitionMode};

    // Create build side with integer keys
    let build_batches = vec![
        record_batch!(
            ("id1", Int32, [1, 2]),
            ("id2", Int32, [10, 20]),
            ("value", Float64, [100.0, 200.0])
        )
        .unwrap(),
    ];
    let build_side_schema = Arc::new(Schema::new(vec![
        Field::new("id1", DataType::Int32, false),
        Field::new("id2", DataType::Int32, false),
        Field::new("value", DataType::Float64, false),
    ]));
    let build_scan = TestScanBuilder::new(Arc::clone(&build_side_schema))
        .with_support(true)
        .with_batches(build_batches)
        .build();

    // Create probe side with more integer rows
    let probe_batches = vec![
        record_batch!(
            ("id1", Int32, [1, 2, 3, 4]),
            ("id2", Int32, [10, 20, 30, 40]),
            ("data", Utf8, ["a", "b", "c", "d"])
        )
        .unwrap(),
    ];
    let probe_side_schema = Arc::new(Schema::new(vec![
        Field::new("id1", DataType::Int32, false),
        Field::new("id2", DataType::Int32, false),
        Field::new("data", DataType::Utf8, false),
    ]));
    let probe_scan = TestScanBuilder::new(Arc::clone(&probe_side_schema))
        .with_support(true)
        .with_batches(probe_batches)
        .build();

    // Create join on multiple integer columns
    let on = vec![
        (
            col("id1", &build_side_schema).unwrap(),
            col("id1", &probe_side_schema).unwrap(),
        ),
        (
            col("id2", &build_side_schema).unwrap(),
            col("id2", &probe_side_schema).unwrap(),
        ),
    ];
    let plan = Arc::new(
        HashJoinExec::try_new(
            build_scan,
            Arc::clone(&probe_scan),
            on,
            None,
            &JoinType::Inner,
            None,
            PartitionMode::CollectLeft,
            datafusion_common::NullEquality::NullEqualsNothing,
            false,
        )
        .unwrap(),
    );

    // Apply optimization with forced HashTable strategy
    let session_config = SessionConfig::default()
        .with_batch_size(10)
        .set_usize("datafusion.optimizer.hash_join_inlist_pushdown_max_size", 1)
        .set_bool("datafusion.execution.parquet.pushdown_filters", true)
        .set_bool("datafusion.optimizer.enable_dynamic_filter_pushdown", true);
    let plan = FilterPushdown::new_post_optimization()
        .optimize(plan, session_config.options())
        .unwrap();
    let session_ctx = SessionContext::new_with_config(session_config);
    session_ctx.register_object_store(
        ObjectStoreUrl::parse("test://").unwrap().as_ref(),
        Arc::new(InMemory::new()),
    );
    let state = session_ctx.state();
    let task_ctx = state.task_ctx();
    let batches = collect(Arc::clone(&plan), Arc::clone(&task_ctx))
        .await
        .unwrap();

    // Verify hash_lookup is used
    let plan_str = format_plan_for_test(&plan).to_string();
    assert!(
        plan_str.contains("hash_lookup"),
        "Expected hash_lookup in plan but got: {plan_str}"
    );
    assert!(
        !plan_str.contains("IN (SET)"),
        "Expected no IN (SET) in plan but got: {plan_str}"
    );

    let result = format!("{}", pretty_format_batches(&batches).unwrap());

    let probe_scan_metrics = probe_scan.metrics().unwrap();
    // Only 2 rows from probe side match the build side
    assert_eq!(probe_scan_metrics.output_rows().unwrap(), 2);

    insta::assert_snapshot!(
        result,
        @r"
    +-----+-----+-------+-----+-----+------+
    | id1 | id2 | value | id1 | id2 | data |
    +-----+-----+-------+-----+-----+------+
    | 1   | 10  | 100.0 | 1   | 10  | a    |
    | 2   | 20  | 200.0 | 2   | 20  | b    |
    +-----+-----+-------+-----+-----+------+
    ",
    );
}

#[tokio::test]
async fn test_hashjoin_dynamic_filter_pushdown_is_used() {
    use datafusion_common::JoinType;
    use datafusion_physical_plan::joins::{HashJoinExec, PartitionMode};

    // Test both cases: probe side with and without filter pushdown support
    for (probe_supports_pushdown, expected_is_used) in [(false, false), (true, true)] {
        let build_side_schema = Arc::new(Schema::new(vec![
            Field::new("a", DataType::Utf8, false),
            Field::new("b", DataType::Utf8, false),
        ]));
        let build_scan = TestScanBuilder::new(Arc::clone(&build_side_schema))
            .with_support(true)
            .with_batches(vec![
                record_batch!(("a", Utf8, ["aa", "ab"]), ("b", Utf8, ["ba", "bb"]))
                    .unwrap(),
            ])
            .build();

        let probe_side_schema = Arc::new(Schema::new(vec![
            Field::new("a", DataType::Utf8, false),
            Field::new("b", DataType::Utf8, false),
        ]));
        let probe_scan = TestScanBuilder::new(Arc::clone(&probe_side_schema))
            .with_support(probe_supports_pushdown)
            .with_batches(vec![
                record_batch!(
                    ("a", Utf8, ["aa", "ab", "ac", "ad"]),
                    ("b", Utf8, ["ba", "bb", "bc", "bd"])
                )
                .unwrap(),
            ])
            .build();

        let on = vec![
            (
                col("a", &build_side_schema).unwrap(),
                col("a", &probe_side_schema).unwrap(),
            ),
            (
                col("b", &build_side_schema).unwrap(),
                col("b", &probe_side_schema).unwrap(),
            ),
        ];
        let plan = Arc::new(
            HashJoinExec::try_new(
                build_scan,
                probe_scan,
                on,
                None,
                &JoinType::Inner,
                None,
                PartitionMode::CollectLeft,
                datafusion_common::NullEquality::NullEqualsNothing,
                false,
            )
            .unwrap(),
        ) as Arc<dyn ExecutionPlan>;

        // Apply filter pushdown optimization
        let mut config = ConfigOptions::default();
        config.execution.parquet.pushdown_filters = true;
        config.optimizer.enable_dynamic_filter_pushdown = true;
        let plan = FilterPushdown::new_post_optimization()
            .optimize(plan, &config)
            .unwrap();

        // Get the HashJoinExec to check the dynamic filter
        let hash_join = plan
            .as_any()
            .downcast_ref::<HashJoinExec>()
            .expect("Plan should be HashJoinExec");

        // Verify that a dynamic filter was created
        let dynamic_filter = hash_join
            .dynamic_filter_for_test()
            .expect("Dynamic filter should be created");

        // Verify that is_used() returns the expected value based on probe side support.
        // When probe_supports_pushdown=false: no consumer holds a reference (is_used=false)
        // When probe_supports_pushdown=true: probe side holds a reference (is_used=true)
        assert_eq!(
            dynamic_filter.is_used(),
            expected_is_used,
            "is_used() should return {expected_is_used} when probe side support is {probe_supports_pushdown}"
        );
    }
}
