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
    array::{RecordBatch, record_batch},
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
    prelude::{SessionConfig, SessionContext},
    scalar::ScalarValue,
};
use datafusion_catalog::memory::DataSourceExec;
use datafusion_common::{
    JoinType,
    config::ConfigOptions,
    tree_node::{TreeNode, TreeNodeRecursion},
};
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
use datafusion_physical_expr::{
    LexOrdering, PhysicalSortExpr,
    expressions::{DynamicFilterPhysicalExpr, col},
    utils::conjunction,
};
use datafusion_physical_expr::{
    Partitioning, RangePartitioning, ScalarFunctionExpr, SplitPoint,
    aggregate::AggregateExprBuilder,
};
use datafusion_physical_optimizer::{
    PhysicalOptimizerRule, filter_pushdown::FilterPushdown,
};
use datafusion_physical_plan::{
    ExecutionPlan,
    aggregates::{AggregateExec, AggregateMode, PhysicalGroupBy},
    coalesce_partitions::CoalescePartitionsExec,
    collect,
    execution_plan::{
        ChildrenPropertiesMode, ReplaceChildrenOptions, plan_contains_expression_id,
    },
    filter::{FilterExec, FilterExecBuilder},
    joins::{HashJoinExec, PartitionMode},
    projection::ProjectionExec,
    repartition::RepartitionExec,
    sorts::sort::SortExec,
};

use super::pushdown_utils::{
    OptimizationTest, TestNode, TestScanBuilder, TestSource, format_plan_for_test,
};
use datafusion_physical_plan::union::UnionExec;
use object_store::memory::InMemory;

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

// Inner-join part is covered by push_down_filter_parquet.slt::test_hashjoin_parent_filter_pushdown.
// The Left-join part stays in Rust: SQL's outer-join-elimination rewrites
// `LEFT JOIN ... WHERE <probe-side-null-rejecting>` into an INNER JOIN
// before physical filter pushdown runs, so the preserved-vs-non-preserved
// distinction this test exercises is not reachable via SQL.
#[tokio::test]
async fn test_static_filter_pushdown_through_hash_join() {
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

    // Test left join: filter on preserved (build) side is pushed down,
    // filter on non-preserved (probe) side is NOT pushed down.
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
    // Filter on build side column (preserved): should be pushed down
    let left_filter = col_lit_predicate("a", "aa", &join_schema);
    // Filter on probe side column (not preserved): should NOT be pushed down
    let right_filter = col_lit_predicate("e", "ba", &join_schema);
    let filter =
        Arc::new(FilterExec::try_new(left_filter, Arc::clone(&join) as _).unwrap());
    let plan = Arc::new(FilterExec::try_new(right_filter, filter).unwrap())
        as Arc<dyn ExecutionPlan>;

    insta::assert_snapshot!(
        OptimizationTest::new(plan, FilterPushdown::new(), true),
        @r"
    OptimizationTest:
      input:
        - FilterExec: e@4 = ba
        -   FilterExec: a@0 = aa
        -     HashJoinExec: mode=Partitioned, join_type=Left, on=[(a@0, d@0)]
        -       DataSourceExec: file_groups={1 group: [[test.parquet]]}, projection=[a, b, c], file_type=test, pushdown_supported=true
        -       DataSourceExec: file_groups={1 group: [[test.parquet]]}, projection=[d, e, f], file_type=test, pushdown_supported=true
      output:
        Ok:
          - FilterExec: e@4 = ba
          -   HashJoinExec: mode=Partitioned, join_type=Left, on=[(a@0, d@0)]
          -     DataSourceExec: file_groups={1 group: [[test.parquet]]}, projection=[a, b, c], file_type=test, pushdown_supported=true, predicate=a@0 = aa
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
fn test_filter_collapse_outer_fetch_preserved() {
    // When the outer filter has fetch and inner does not, the merged filter should preserve fetch
    let scan = TestScanBuilder::new(schema()).with_support(false).build();
    let predicate1 = col_lit_predicate("a", "foo", &schema());
    let filter1 = Arc::new(FilterExec::try_new(predicate1, scan).unwrap());
    let predicate2 = col_lit_predicate("b", "bar", &schema());
    let plan = Arc::new(
        FilterExecBuilder::new(predicate2, filter1)
            .with_fetch(Some(10))
            .build()
            .unwrap(),
    );

    insta::assert_snapshot!(
        OptimizationTest::new(plan, FilterPushdown::new(), true),
        @r"
    OptimizationTest:
      input:
        - FilterExec: b@1 = bar, fetch=10
        -   FilterExec: a@0 = foo
        -     DataSourceExec: file_groups={1 group: [[test.parquet]]}, projection=[a, b, c], file_type=test, pushdown_supported=false
      output:
        Ok:
          - FilterExec: b@1 = bar AND a@0 = foo, fetch=10
          -   DataSourceExec: file_groups={1 group: [[test.parquet]]}, projection=[a, b, c], file_type=test, pushdown_supported=false
    "
    );
}

#[test]
fn test_filter_collapse_inner_fetch_preserved() {
    // When the inner filter has fetch and outer does not, the merged filter should preserve fetch
    let scan = TestScanBuilder::new(schema()).with_support(false).build();
    let predicate1 = col_lit_predicate("a", "foo", &schema());
    let filter1 = Arc::new(
        FilterExecBuilder::new(predicate1, scan)
            .with_fetch(Some(5))
            .build()
            .unwrap(),
    );
    let predicate2 = col_lit_predicate("b", "bar", &schema());
    let plan = Arc::new(FilterExec::try_new(predicate2, filter1).unwrap());

    insta::assert_snapshot!(
        OptimizationTest::new(plan, FilterPushdown::new(), true),
        @r"
    OptimizationTest:
      input:
        - FilterExec: b@1 = bar
        -   FilterExec: a@0 = foo, fetch=5
        -     DataSourceExec: file_groups={1 group: [[test.parquet]]}, projection=[a, b, c], file_type=test, pushdown_supported=false
      output:
        Ok:
          - FilterExec: b@1 = bar AND a@0 = foo, fetch=5
          -   DataSourceExec: file_groups={1 group: [[test.parquet]]}, projection=[a, b, c], file_type=test, pushdown_supported=false
    "
    );
}

#[test]
fn test_filter_collapse_both_fetch_uses_minimum() {
    // When both filters have fetch, the merged filter should use the smaller (tighter) fetch.
    // Inner fetch=5 is tighter than outer fetch=10, so the result should be fetch=5.
    let scan = TestScanBuilder::new(schema()).with_support(false).build();
    let predicate1 = col_lit_predicate("a", "foo", &schema());
    let filter1 = Arc::new(
        FilterExecBuilder::new(predicate1, scan)
            .with_fetch(Some(5))
            .build()
            .unwrap(),
    );
    let predicate2 = col_lit_predicate("b", "bar", &schema());
    let plan = Arc::new(
        FilterExecBuilder::new(predicate2, filter1)
            .with_fetch(Some(10))
            .build()
            .unwrap(),
    );

    insta::assert_snapshot!(
        OptimizationTest::new(plan, FilterPushdown::new(), true),
        @r"
    OptimizationTest:
      input:
        - FilterExec: b@1 = bar, fetch=10
        -   FilterExec: a@0 = foo, fetch=5
        -     DataSourceExec: file_groups={1 group: [[test.parquet]]}, projection=[a, b, c], file_type=test, pushdown_supported=false
      output:
        Ok:
          - FilterExec: b@1 = bar AND a@0 = foo, fetch=5
          -   DataSourceExec: file_groups={1 group: [[test.parquet]]}, projection=[a, b, c], file_type=test, pushdown_supported=false
    "
    );
}

#[test]
fn test_filter_with_fetch_fully_pushed_to_scan() {
    // When a FilterExec has a fetch limit and all predicates are pushed down
    // to a supportive DataSourceExec, the FilterExec is removed and the fetch
    // must be propagated to the DataSourceExec.
    let scan = TestScanBuilder::new(schema()).with_support(true).build();
    let predicate = col_lit_predicate("a", "foo", &schema());
    let plan = Arc::new(
        FilterExecBuilder::new(predicate, scan)
            .with_fetch(Some(10))
            .build()
            .unwrap(),
    );

    insta::assert_snapshot!(
        OptimizationTest::new(plan, FilterPushdown::new(), true),
        @r"
    OptimizationTest:
      input:
        - FilterExec: a@0 = foo, fetch=10
        -   DataSourceExec: file_groups={1 group: [[test.parquet]]}, projection=[a, b, c], file_type=test, pushdown_supported=true
      output:
        Ok:
          - DataSourceExec: file_groups={1 group: [[test.parquet]]}, projection=[a, b, c], limit=10, file_type=test, pushdown_supported=true, predicate=a@0 = foo
    "
    );
}

#[test]
fn test_filter_with_fetch_and_projection_fully_pushed_to_scan() {
    // When a FilterExec has both fetch and projection, and all predicates are
    // pushed down, the filter is replaced by a ProjectionExec and the fetch
    // must still be propagated to the DataSourceExec.
    let scan = TestScanBuilder::new(schema()).with_support(true).build();
    let projection = vec![1, 0];
    let predicate = col_lit_predicate("a", "foo", &schema());
    let plan = Arc::new(
        FilterExecBuilder::new(predicate, scan)
            .with_fetch(Some(5))
            .apply_projection(Some(projection))
            .unwrap()
            .build()
            .unwrap(),
    );

    insta::assert_snapshot!(
        OptimizationTest::new(plan, FilterPushdown::new(), true),
        @r"
    OptimizationTest:
      input:
        - FilterExec: a@0 = foo, projection=[b@1, a@0], fetch=5
        -   DataSourceExec: file_groups={1 group: [[test.parquet]]}, projection=[a, b, c], file_type=test, pushdown_supported=true
      output:
        Ok:
          - ProjectionExec: expr=[b@1 as b, a@0 as a]
          -   DataSourceExec: file_groups={1 group: [[test.parquet]]}, projection=[a, b, c], limit=5, file_type=test, pushdown_supported=true, predicate=a@0 = foo
    "
    );
}

#[test]
fn test_filter_with_fetch_partially_pushed_to_scan() {
    // When a FilterExec has fetch and only some predicates are pushed down,
    // the FilterExec remains with the unpushed predicate and keeps its fetch.
    let scan = TestScanBuilder::new(schema()).with_support(true).build();
    let pushed_predicate = col_lit_predicate("a", "foo", &schema());
    let volatile_predicate = {
        let cfg = Arc::new(ConfigOptions::default());
        Arc::new(BinaryExpr::new(
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
        )) as Arc<dyn PhysicalExpr>
    };
    // Combine: a = 'foo' AND a = random()
    let combined = Arc::new(BinaryExpr::new(
        pushed_predicate,
        Operator::And,
        volatile_predicate,
    )) as Arc<dyn PhysicalExpr>;
    let plan = Arc::new(
        FilterExecBuilder::new(combined, scan)
            .with_fetch(Some(7))
            .build()
            .unwrap(),
    );

    insta::assert_snapshot!(
        OptimizationTest::new(plan, FilterPushdown::new(), true),
        @r"
    OptimizationTest:
      input:
        - FilterExec: a@0 = foo AND a@0 = random(), fetch=7
        -   DataSourceExec: file_groups={1 group: [[test.parquet]]}, projection=[a, b, c], file_type=test, pushdown_supported=true
      output:
        Ok:
          - FilterExec: a@0 = random(), fetch=7
          -   DataSourceExec: file_groups={1 group: [[test.parquet]]}, projection=[a, b, c], file_type=test, pushdown_supported=true, predicate=a@0 = foo
    "
    );
}

#[test]
fn test_filter_with_fetch_not_pushed_to_unsupportive_scan() {
    // When the DataSourceExec does not support pushdown, the FilterExec
    // remains unchanged with its fetch intact.
    let scan = TestScanBuilder::new(schema()).with_support(false).build();
    let predicate = col_lit_predicate("a", "foo", &schema());
    let plan = Arc::new(
        FilterExecBuilder::new(predicate, scan)
            .with_fetch(Some(3))
            .build()
            .unwrap(),
    );

    insta::assert_snapshot!(
        OptimizationTest::new(plan, FilterPushdown::new(), true),
        @r"
    OptimizationTest:
      input:
        - FilterExec: a@0 = foo, fetch=3
        -   DataSourceExec: file_groups={1 group: [[test.parquet]]}, projection=[a, b, c], file_type=test, pushdown_supported=false
      output:
        Ok:
          - FilterExec: a@0 = foo, fetch=3
          -   DataSourceExec: file_groups={1 group: [[test.parquet]]}, projection=[a, b, c], file_type=test, pushdown_supported=false
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

#[test]
fn test_pushdown_through_aggregates_preserves_parent_filter_order() {
    // AggregateExec may push filters on grouping columns to its input, but must
    // keep filters on aggregate outputs above itself. The parent-filter result
    // order must match the incoming filter order, otherwise an unsupported
    // aggregate-output filter can be reported as pushed down and removed.
    let scan = TestScanBuilder::new(schema()).with_support(true).build();

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
            aggregate_expr,
            vec![None],
            scan,
            schema(),
        )
        .unwrap(),
    );

    let aggregate_schema = aggregate.schema();
    let aggregate_output_filter = col_lit_predicate(
        "cnt",
        ScalarValue::Int64(Some(1)),
        aggregate_schema.as_ref(),
    );
    let grouping_key_filter = col_lit_predicate("b", "bar", aggregate_schema.as_ref());
    let predicate = conjunction(vec![aggregate_output_filter, grouping_key_filter]);
    let plan = Arc::new(FilterExec::try_new(predicate, aggregate).unwrap());

    insta::assert_snapshot!(
        OptimizationTest::new(plan, FilterPushdown::new(), true),
        @r"
    OptimizationTest:
      input:
        - FilterExec: cnt@2 = 1 AND b@1 = bar
        -   AggregateExec: mode=Final, gby=[a@0 as a, b@1 as b], aggr=[cnt]
        -     DataSourceExec: file_groups={1 group: [[test.parquet]]}, projection=[a, b, c], file_type=test, pushdown_supported=true
      output:
        Ok:
          - FilterExec: cnt@2 = 1
          -   AggregateExec: mode=Final, gby=[a@0 as a, b@1 as b], aggr=[cnt], ordering_mode=PartiallySorted([1])
          -     DataSourceExec: file_groups={1 group: [[test.parquet]]}, projection=[a, b, c], file_type=test, pushdown_supported=true, predicate=b@1 = bar
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

// Not portable to sqllogictest: requires manually constructing
// `SortExec(CoalescePartitionsExec(scan))`. A SQL `ORDER BY ... LIMIT` over a
// multi-partition scan plans as `SortPreservingMergeExec(SortExec(scan))`
// instead, so the filter-through-coalesce path this test exercises is not
// reachable via SQL.
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

fn hashjoin_pushdown_scans() -> (
    SchemaRef,
    Arc<dyn ExecutionPlan>,
    SchemaRef,
    Arc<dyn ExecutionPlan>,
) {
    let build_side_schema = Arc::new(Schema::new(vec![
        Field::new("a", DataType::Utf8, false),
        Field::new("b", DataType::Utf8, false),
        Field::new("c", DataType::Float64, false),
    ]));
    let build_scan = TestScanBuilder::new(Arc::clone(&build_side_schema))
        .with_support(true)
        .with_batches(vec![
            record_batch!(
                ("a", Utf8, ["aa", "ab"]),
                ("b", Utf8, ["ba", "bb"]),
                ("c", Float64, [1.0, 2.0])
            )
            .unwrap(),
        ])
        .build();

    let probe_side_schema = Arc::new(Schema::new(vec![
        Field::new("a", DataType::Utf8, false),
        Field::new("b", DataType::Utf8, false),
        Field::new("e", DataType::Float64, false),
    ]));
    let probe_scan = TestScanBuilder::new(Arc::clone(&probe_side_schema))
        .with_support(true)
        .with_batches(vec![
            record_batch!(
                ("a", Utf8, ["aa", "ab", "ac", "ad"]),
                ("b", Utf8, ["ba", "bb", "bc", "bd"]),
                ("e", Float64, [1.0, 2.0, 3.0, 4.0])
            )
            .unwrap(),
        ])
        .build();

    (build_side_schema, build_scan, probe_side_schema, probe_scan)
}

async fn optimize_and_collect_pushdown_plan(
    plan: Arc<dyn ExecutionPlan>,
    config: ConfigOptions,
) -> (Arc<dyn ExecutionPlan>, Vec<RecordBatch>) {
    let plan = FilterPushdown::new_post_optimization()
        .optimize(plan, &config)
        .unwrap();
    let session_ctx =
        SessionContext::new_with_config(SessionConfig::from(config).with_batch_size(10));
    session_ctx.register_object_store(
        ObjectStoreUrl::parse("test://").unwrap().as_ref(),
        Arc::new(InMemory::new()),
    );
    let task_ctx = session_ctx.state().task_ctx();
    let batches = collect(Arc::clone(&plan), task_ctx).await.unwrap();
    (plan, batches)
}

// Not portable to sqllogictest: this test pins `PartitionMode::Partitioned`
// by hand-wiring `RepartitionExec(Hash, 12)` on both join sides. A SQL
// INNER JOIN over small parquet inputs plans as `CollectLeft`, so the
// per-partition CASE filter this test exercises is not reachable via SQL.
#[tokio::test]
async fn test_hashjoin_dynamic_filter_pushdown_partitioned() {
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

    let (build_side_schema, build_scan, probe_side_schema, probe_scan) =
        hashjoin_pushdown_scans();

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
    let (plan, batches) = optimize_and_collect_pushdown_plan(plan, config).await;

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
    -         DataSourceExec: file_groups={1 group: [[test.parquet]]}, projection=[a, b, e], file_type=test, pushdown_supported=true, predicate=DynamicFilter [ CASE hash_repartition % 12 WHEN 5 THEN a@0 >= ab AND a@0 <= ab AND b@1 >= bb AND b@1 <= bb AND struct(a@0, b@1) IN (SET) ([{c0:ab,c1:bb}]) WHEN 8 THEN a@0 >= aa AND a@0 <= aa AND b@1 >= ba AND b@1 <= ba AND struct(a@0, b@1) IN (SET) ([{c0:aa,c1:ba}]) ELSE false END ]
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
async fn test_hashjoin_dynamic_filter_pushdown_range_partitioned() {
    // Rough sketch of the Range-partitioned MRE we're trying to recreate. The
    // test hand-wires identical Range repartitioning:
    //
    // EXPLAIN
    // SELECT *
    // FROM build
    // JOIN probe
    //   ON build.a = probe.a AND build.b = probe.b;
    //
    // +---------------+------------------------------------------------------------+
    // | plan_type     | plan                                                       |
    // +---------------+------------------------------------------------------------+
    // | physical_plan | ┌───────────────────────────┐                              |
    // |               | │        HashJoinExec       │                              |
    // |               | │    --------------------   ├──────────────┐               |
    // |               | │ on: (a = a), (b = b)      │              │               |
    // |               | └─────────────┬─────────────┘              │               |
    // |               | ┌─────────────┴─────────────┐┌─────────────┴─────────────┐ |
    // |               | │      RepartitionExec      ││      RepartitionExec      │ |
    // |               | │    --------------------   ││    --------------------   │ |
    // |               | │ partition_count(in->out): ││ partition_count(in->out): │ |
    // |               | │           1 -> 2          ││           1 -> 2          │ |
    // |               | │                           ││                           │ |
    // |               | │    partitioning_scheme:   ││    partitioning_scheme:   │ |
    // |               | │ Range([a ASC, b ASC], 2)  ││ Range([a ASC, b ASC], 2)  │ |
    // |               | │      split: (aa, bb)      ││      split: (aa, bb)      │ |
    // |               | └─────────────┬─────────────┘└─────────────┬─────────────┘ |
    // |               | ┌─────────────┴─────────────┐┌─────────────┴─────────────┐ |
    // |               | │ DataSourceExec (build)    ││ DataSourceExec (probe)    │ |
    // |               | │    --------------------   ││    --------------------   │ |
    // |               | │ rows: (aa,ba), (ab,bb)    ││ rows: (aa,ba) ... (ad,bd) │ |
    // |               | │                           ││ predicate: DynamicFilter  │ |
    // |               | │                           ││ range CASE -> filter_0/1  │ |
    // |               | └───────────────────────────┘└───────────────────────────┘ |
    // |               |                                                            |
    // +---------------+------------------------------------------------------------+

    let (build_side_schema, build_scan, probe_side_schema, probe_scan) =
        hashjoin_pushdown_scans();

    let split_points = vec![SplitPoint::new(vec![
        ScalarValue::Utf8(Some("aa".to_string())),
        ScalarValue::Utf8(Some("bb".to_string())),
    ])];

    // Build side: DataSource -> RepartitionExec (Range)
    let build_range_ordering = LexOrdering::new(vec![
        PhysicalSortExpr::new(
            col("a", &build_side_schema).unwrap(),
            SortOptions::default(),
        ),
        PhysicalSortExpr::new(
            col("b", &build_side_schema).unwrap(),
            SortOptions::default(),
        ),
    ])
    .unwrap();
    let build_repartition = Arc::new(
        RepartitionExec::try_new(
            build_scan,
            Partitioning::Range(
                RangePartitioning::try_new(build_range_ordering, split_points.clone())
                    .unwrap(),
            ),
        )
        .unwrap(),
    );

    // Probe side: DataSource -> RepartitionExec (Range)
    let probe_range_ordering = LexOrdering::new(vec![
        PhysicalSortExpr::new(
            col("a", &probe_side_schema).unwrap(),
            SortOptions::default(),
        ),
        PhysicalSortExpr::new(
            col("b", &probe_side_schema).unwrap(),
            SortOptions::default(),
        ),
    ])
    .unwrap();
    let probe_repartition = Arc::new(
        RepartitionExec::try_new(
            Arc::clone(&probe_scan),
            Partitioning::Range(
                RangePartitioning::try_new(probe_range_ordering, split_points).unwrap(),
            ),
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
        -       RepartitionExec: partitioning=Range([a@0 ASC, b@1 ASC], [(aa, bb)], 2), input_partitions=1
        -         DataSourceExec: file_groups={1 group: [[test.parquet]]}, projection=[a, b, c], file_type=test, pushdown_supported=true
        -       RepartitionExec: partitioning=Range([a@0 ASC, b@1 ASC], [(aa, bb)], 2), input_partitions=1
        -         DataSourceExec: file_groups={1 group: [[test.parquet]]}, projection=[a, b, e], file_type=test, pushdown_supported=true
      output:
        Ok:
          - SortExec: expr=[a@0 DESC NULLS LAST], preserve_partitioning=[false]
          -   CoalescePartitionsExec
          -     HashJoinExec: mode=Partitioned, join_type=Inner, on=[(a@0, a@0), (b@1, b@1)]
          -       RepartitionExec: partitioning=Range([a@0 ASC, b@1 ASC], [(aa, bb)], 2), input_partitions=1
          -         DataSourceExec: file_groups={1 group: [[test.parquet]]}, projection=[a, b, c], file_type=test, pushdown_supported=true
          -       RepartitionExec: partitioning=Range([a@0 ASC, b@1 ASC], [(aa, bb)], 2), input_partitions=1
          -         DataSourceExec: file_groups={1 group: [[test.parquet]]}, projection=[a, b, e], file_type=test, pushdown_supported=true, predicate=DynamicFilter [ empty ]
    "
    );

    // Actually apply the optimization to the plan and execute to see the filter in action
    let mut config = ConfigOptions::default();
    config.execution.parquet.pushdown_filters = true;
    config.optimizer.enable_dynamic_filter_pushdown = true;
    config.optimizer.preserve_file_partitions = 1;
    let (plan, batches) = optimize_and_collect_pushdown_plan(plan, config).await;

    // Now check what our filter looks like
    insta::assert_snapshot!(
        format!("{}", format_plan_for_test(&plan)),
        @r"
    - SortExec: expr=[a@0 DESC NULLS LAST], preserve_partitioning=[false]
    -   CoalescePartitionsExec
    -     HashJoinExec: mode=Partitioned, join_type=Inner, on=[(a@0, a@0), (b@1, b@1)]
    -       RepartitionExec: partitioning=Range([a@0 ASC, b@1 ASC], [(aa, bb)], 2), input_partitions=1
    -         DataSourceExec: file_groups={1 group: [[test.parquet]]}, projection=[a, b, c], file_type=test, pushdown_supported=true
    -       RepartitionExec: partitioning=Range([a@0 ASC, b@1 ASC], [(aa, bb)], 2), input_partitions=1
    -         DataSourceExec: file_groups={1 group: [[test.parquet]]}, projection=[a, b, e], file_type=test, pushdown_supported=true, predicate=DynamicFilter [ CASE range_partition WHEN 0 THEN a@0 >= aa AND a@0 <= aa AND b@1 >= ba AND b@1 <= ba AND struct(a@0, b@1) IN (SET) ([{c0:aa,c1:ba}]) WHEN 1 THEN a@0 >= ab AND a@0 <= ab AND b@1 >= bb AND b@1 <= bb AND struct(a@0, b@1) IN (SET) ([{c0:ab,c1:bb}]) ELSE false END ]
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

// Not portable to sqllogictest: this test specifically pins a
// `RepartitionExec(Hash, 12)` between `HashJoinExec(CollectLeft)` and the
// probe-side scan to verify the dynamic filter link survives that boundary
// (regression for #17451). The same CollectLeft filter content and
// pushdown counters are already covered by the simpler slt port
// (push_down_filter_parquet.slt::test_hashjoin_dynamic_filter_pushdown).
#[tokio::test]
async fn test_hashjoin_dynamic_filter_pushdown_collect_left() {
    let (build_side_schema, build_scan, probe_side_schema, probe_scan) =
        hashjoin_pushdown_scans();

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
    let (plan, batches) = optimize_and_collect_pushdown_plan(plan, config).await;

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

#[test]
fn test_hashjoin_parent_filter_pushdown_same_column_names() {
    let build_side_schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Utf8, false),
        Field::new("build_val", DataType::Utf8, false),
    ]));
    let build_scan = TestScanBuilder::new(Arc::clone(&build_side_schema))
        .with_support(true)
        .build();

    let probe_side_schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Utf8, false),
        Field::new("probe_val", DataType::Utf8, false),
    ]));
    let probe_scan = TestScanBuilder::new(Arc::clone(&probe_side_schema))
        .with_support(true)
        .build();

    let on = vec![(
        col("id", &build_side_schema).unwrap(),
        col("id", &probe_side_schema).unwrap(),
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

    let build_id_filter = col_lit_predicate("id", "aa", &join_schema);
    let probe_val_filter = col_lit_predicate("probe_val", "x", &join_schema);

    let filter =
        Arc::new(FilterExec::try_new(build_id_filter, Arc::clone(&join) as _).unwrap());
    let plan = Arc::new(FilterExec::try_new(probe_val_filter, filter).unwrap())
        as Arc<dyn ExecutionPlan>;

    insta::assert_snapshot!(
        OptimizationTest::new(Arc::clone(&plan), FilterPushdown::new(), true),
        @r"
    OptimizationTest:
      input:
        - FilterExec: probe_val@3 = x
        -   FilterExec: id@0 = aa
        -     HashJoinExec: mode=Partitioned, join_type=Inner, on=[(id@0, id@0)]
        -       DataSourceExec: file_groups={1 group: [[test.parquet]]}, projection=[id, build_val], file_type=test, pushdown_supported=true
        -       DataSourceExec: file_groups={1 group: [[test.parquet]]}, projection=[id, probe_val], file_type=test, pushdown_supported=true
      output:
        Ok:
          - HashJoinExec: mode=Partitioned, join_type=Inner, on=[(id@0, id@0)]
          -   DataSourceExec: file_groups={1 group: [[test.parquet]]}, projection=[id, build_val], file_type=test, pushdown_supported=true, predicate=id@0 = aa
          -   DataSourceExec: file_groups={1 group: [[test.parquet]]}, projection=[id, probe_val], file_type=test, pushdown_supported=true, predicate=probe_val@1 = x
    "
    );
}

#[test]
fn test_hashjoin_parent_filter_pushdown_mark_join() {
    let left_schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Utf8, false),
        Field::new("val", DataType::Utf8, false),
    ]));
    let left_scan = TestScanBuilder::new(Arc::clone(&left_schema))
        .with_support(true)
        .build();

    let right_schema =
        Arc::new(Schema::new(vec![Field::new("id", DataType::Utf8, false)]));
    let right_scan = TestScanBuilder::new(Arc::clone(&right_schema))
        .with_support(true)
        .build();

    let on = vec![(
        col("id", &left_schema).unwrap(),
        col("id", &right_schema).unwrap(),
    )];
    let join = Arc::new(
        HashJoinExec::try_new(
            left_scan,
            right_scan,
            on,
            None,
            &JoinType::LeftMark,
            None,
            PartitionMode::Partitioned,
            datafusion_common::NullEquality::NullEqualsNothing,
            false,
        )
        .unwrap(),
    );

    let join_schema = join.schema();

    let left_filter = col_lit_predicate("val", "x", &join_schema);
    let mark_filter = col_lit_predicate("mark", true, &join_schema);

    let filter =
        Arc::new(FilterExec::try_new(left_filter, Arc::clone(&join) as _).unwrap());
    let plan = Arc::new(FilterExec::try_new(mark_filter, filter).unwrap())
        as Arc<dyn ExecutionPlan>;

    insta::assert_snapshot!(
        OptimizationTest::new(Arc::clone(&plan), FilterPushdown::new(), true),
        @r"
    OptimizationTest:
      input:
        - FilterExec: mark@2 = true
        -   FilterExec: val@1 = x
        -     HashJoinExec: mode=Partitioned, join_type=LeftMark, on=[(id@0, id@0)]
        -       DataSourceExec: file_groups={1 group: [[test.parquet]]}, projection=[id, val], file_type=test, pushdown_supported=true
        -       DataSourceExec: file_groups={1 group: [[test.parquet]]}, projection=[id], file_type=test, pushdown_supported=true
      output:
        Ok:
          - FilterExec: mark@2 = true
          -   HashJoinExec: mode=Partitioned, join_type=LeftMark, on=[(id@0, id@0)]
          -     DataSourceExec: file_groups={1 group: [[test.parquet]]}, projection=[id, val], file_type=test, pushdown_supported=true, predicate=val@1 = x
          -     DataSourceExec: file_groups={1 group: [[test.parquet]]}, projection=[id], file_type=test, pushdown_supported=true
    "
    );
}

/// Semi-join key filters can be pushed to both sides, but anti-join filters must
/// only rely on the output side to preserve their semantics.
#[test]
fn test_hashjoin_parent_filter_pushdown_semi_anti_join() {
    let left_schema = Arc::new(Schema::new(vec![
        Field::new("k", DataType::Utf8, false),
        Field::new("v", DataType::Utf8, false),
    ]));
    let left_scan = TestScanBuilder::new(Arc::clone(&left_schema))
        .with_support(true)
        .build();

    let right_schema = Arc::new(Schema::new(vec![
        Field::new("k", DataType::Utf8, false),
        Field::new("w", DataType::Utf8, false),
    ]));
    let right_scan = TestScanBuilder::new(Arc::clone(&right_schema))
        .with_support(true)
        .build();

    let on = vec![(
        col("k", &left_schema).unwrap(),
        col("k", &right_schema).unwrap(),
    )];

    let join = Arc::new(
        HashJoinExec::try_new(
            left_scan,
            Arc::clone(&right_scan),
            on.clone(),
            None,
            &JoinType::LeftSemi,
            None,
            PartitionMode::Partitioned,
            datafusion_common::NullEquality::NullEqualsNothing,
            false,
        )
        .unwrap(),
    );

    let join_schema = join.schema();
    // Filter on join key column: k = 'x' — should be pushed to BOTH sides
    let key_filter = col_lit_predicate("k", "x", &join_schema);
    // Filter on non-key column: v = 'y' — should only be pushed to the left side
    let val_filter = col_lit_predicate("v", "y", &join_schema);

    let filter =
        Arc::new(FilterExec::try_new(key_filter, Arc::clone(&join) as _).unwrap());
    let plan = Arc::new(FilterExec::try_new(val_filter, filter).unwrap())
        as Arc<dyn ExecutionPlan>;

    insta::assert_snapshot!(
        OptimizationTest::new(Arc::clone(&plan), FilterPushdown::new(), true),
        @r"
    OptimizationTest:
      input:
        - FilterExec: v@1 = y
        -   FilterExec: k@0 = x
        -     HashJoinExec: mode=Partitioned, join_type=LeftSemi, on=[(k@0, k@0)]
        -       DataSourceExec: file_groups={1 group: [[test.parquet]]}, projection=[k, v], file_type=test, pushdown_supported=true
        -       DataSourceExec: file_groups={1 group: [[test.parquet]]}, projection=[k, w], file_type=test, pushdown_supported=true
      output:
        Ok:
          - HashJoinExec: mode=Partitioned, join_type=LeftSemi, on=[(k@0, k@0)]
          -   DataSourceExec: file_groups={1 group: [[test.parquet]]}, projection=[k, v], file_type=test, pushdown_supported=true, predicate=k@0 = x AND v@1 = y
          -   DataSourceExec: file_groups={1 group: [[test.parquet]]}, projection=[k, w], file_type=test, pushdown_supported=true, predicate=k@0 = x
    "
    );

    let join = Arc::new(
        HashJoinExec::try_new(
            TestScanBuilder::new(Arc::clone(&left_schema)).build(),
            right_scan,
            on,
            None,
            &JoinType::LeftAnti,
            None,
            PartitionMode::Partitioned,
            datafusion_common::NullEquality::NullEqualsNothing,
            false,
        )
        .unwrap(),
    );
    let predicate = Arc::new(Literal::new(ScalarValue::Boolean(Some(false))));
    let plan = Arc::new(FilterExec::try_new(predicate, join).unwrap());
    assert_parent_filter_remains(plan);
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

#[test]
fn test_filter_pushdown_through_union_mixed_support() {
    // Test case where one child supports filter pushdown and one doesn't
    let scan1 = TestScanBuilder::new(schema()).with_support(true).build();
    let scan2 = TestScanBuilder::new(schema()).with_support(false).build();

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
        -     DataSourceExec: file_groups={1 group: [[test.parquet]]}, projection=[a, b, c], file_type=test, pushdown_supported=false
      output:
        Ok:
          - UnionExec
          -   DataSourceExec: file_groups={1 group: [[test.parquet]]}, projection=[a, b, c], file_type=test, pushdown_supported=true, predicate=a@0 = foo
          -   FilterExec: a@0 = foo
          -     DataSourceExec: file_groups={1 group: [[test.parquet]]}, projection=[a, b, c], file_type=test, pushdown_supported=false
    "
    );
}

#[test]
fn test_filter_pushdown_through_union_does_not_support() {
    // Test case where one child supports filter pushdown and one doesn't
    let scan1 = TestScanBuilder::new(schema()).with_support(false).build();
    let scan2 = TestScanBuilder::new(schema()).with_support(false).build();

    let union = UnionExec::try_new(vec![scan1, scan2]).unwrap();

    let predicate = col_lit_predicate("a", "foo", &schema());
    let plan = Arc::new(FilterExec::try_new(predicate, union).unwrap());

    insta::assert_snapshot!(
        OptimizationTest::new(plan, FilterPushdown::new(), true),
        @"
    OptimizationTest:
      input:
        - FilterExec: a@0 = foo
        -   UnionExec
        -     DataSourceExec: file_groups={1 group: [[test.parquet]]}, projection=[a, b, c], file_type=test, pushdown_supported=false
        -     DataSourceExec: file_groups={1 group: [[test.parquet]]}, projection=[a, b, c], file_type=test, pushdown_supported=false
      output:
        Ok:
          - UnionExec
          -   FilterExec: a@0 = foo
          -     DataSourceExec: file_groups={1 group: [[test.parquet]]}, projection=[a, b, c], file_type=test, pushdown_supported=false
          -   FilterExec: a@0 = foo
          -     DataSourceExec: file_groups={1 group: [[test.parquet]]}, projection=[a, b, c], file_type=test, pushdown_supported=false
    "
    );
}

#[test]
fn test_filter_with_fetch_fully_pushed_through_union() {
    // When a FilterExec with fetch wraps a UnionExec and all predicates are
    // pushed down, UnionExec does not support with_fetch, so a LocalLimitExec
    // should be inserted to preserve the fetch limit.
    let scan1 = TestScanBuilder::new(schema()).with_support(true).build();
    let scan2 = TestScanBuilder::new(schema()).with_support(true).build();
    let union = UnionExec::try_new(vec![scan1, scan2]).unwrap();
    let predicate = col_lit_predicate("a", "foo", &schema());
    let plan = Arc::new(
        FilterExecBuilder::new(predicate, union)
            .with_fetch(Some(10))
            .build()
            .unwrap(),
    );

    insta::assert_snapshot!(
        OptimizationTest::new(plan, FilterPushdown::new(), true),
        @"
    OptimizationTest:
      input:
        - FilterExec: a@0 = foo, fetch=10
        -   UnionExec
        -     DataSourceExec: file_groups={1 group: [[test.parquet]]}, projection=[a, b, c], file_type=test, pushdown_supported=true
        -     DataSourceExec: file_groups={1 group: [[test.parquet]]}, projection=[a, b, c], file_type=test, pushdown_supported=true
      output:
        Ok:
          - LocalLimitExec: fetch=10
          -   UnionExec
          -     DataSourceExec: file_groups={1 group: [[test.parquet]]}, projection=[a, b, c], file_type=test, pushdown_supported=true, predicate=a@0 = foo
          -     DataSourceExec: file_groups={1 group: [[test.parquet]]}, projection=[a, b, c], file_type=test, pushdown_supported=true, predicate=a@0 = foo
    "
    );
}

#[test]
fn test_filter_with_fetch_and_projection_fully_pushed_through_union() {
    // When a FilterExec with both fetch and projection wraps a UnionExec and
    // all predicates are pushed down, we should get a ProjectionExec on top of
    // a LocalLimitExec wrapping the UnionExec.
    let scan1 = TestScanBuilder::new(schema()).with_support(true).build();
    let scan2 = TestScanBuilder::new(schema()).with_support(true).build();
    let union = UnionExec::try_new(vec![scan1, scan2]).unwrap();
    let projection = vec![1, 0];
    let predicate = col_lit_predicate("a", "foo", &schema());
    let plan = Arc::new(
        FilterExecBuilder::new(predicate, union)
            .with_fetch(Some(5))
            .apply_projection(Some(projection))
            .unwrap()
            .build()
            .unwrap(),
    );

    insta::assert_snapshot!(
        OptimizationTest::new(plan, FilterPushdown::new(), true),
        @"
    OptimizationTest:
      input:
        - FilterExec: a@0 = foo, projection=[b@1, a@0], fetch=5
        -   UnionExec
        -     DataSourceExec: file_groups={1 group: [[test.parquet]]}, projection=[a, b, c], file_type=test, pushdown_supported=true
        -     DataSourceExec: file_groups={1 group: [[test.parquet]]}, projection=[a, b, c], file_type=test, pushdown_supported=true
      output:
        Ok:
          - ProjectionExec: expr=[b@1 as b, a@0 as a]
          -   LocalLimitExec: fetch=5
          -     UnionExec
          -       DataSourceExec: file_groups={1 group: [[test.parquet]]}, projection=[a, b, c], file_type=test, pushdown_supported=true, predicate=a@0 = foo
          -       DataSourceExec: file_groups={1 group: [[test.parquet]]}, projection=[a, b, c], file_type=test, pushdown_supported=true, predicate=a@0 = foo
    "
    );
}

#[test]
fn test_filter_with_fetch_not_fully_pushed_through_union() {
    // When a FilterExec with fetch wraps a UnionExec but children don't support
    // pushdown, the FilterExec remains with its fetch — no LocalLimitExec needed.
    let scan1 = TestScanBuilder::new(schema()).with_support(false).build();
    let scan2 = TestScanBuilder::new(schema()).with_support(false).build();
    let union = UnionExec::try_new(vec![scan1, scan2]).unwrap();
    let predicate = col_lit_predicate("a", "foo", &schema());
    let plan = Arc::new(
        FilterExecBuilder::new(predicate, union)
            .with_fetch(Some(8))
            .build()
            .unwrap(),
    );

    insta::assert_snapshot!(
        OptimizationTest::new(plan, FilterPushdown::new(), true),
        @"
    OptimizationTest:
      input:
        - FilterExec: a@0 = foo, fetch=8
        -   UnionExec
        -     DataSourceExec: file_groups={1 group: [[test.parquet]]}, projection=[a, b, c], file_type=test, pushdown_supported=false
        -     DataSourceExec: file_groups={1 group: [[test.parquet]]}, projection=[a, b, c], file_type=test, pushdown_supported=false
      output:
        Ok:
          - LocalLimitExec: fetch=8
          -   UnionExec
          -     FilterExec: a@0 = foo
          -       DataSourceExec: file_groups={1 group: [[test.parquet]]}, projection=[a, b, c], file_type=test, pushdown_supported=false
          -     FilterExec: a@0 = foo
          -       DataSourceExec: file_groups={1 group: [[test.parquet]]}, projection=[a, b, c], file_type=test, pushdown_supported=false
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

// test_topk_with_projection_transformation_on_dyn_filter has been ported
// to datafusion/sqllogictest/test_files/push_down_filter_parquet.slt; see
// `topk_proj` fixture for the 4 representative cases (reorder, prune,
// expression, alias shadowing). The `run_projection_dyn_filter_case`
// harness was removed along with it.

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

fn assert_parent_filter_remains(plan: Arc<dyn ExecutionPlan>) {
    let mut config = ConfigOptions::default();
    config.execution.parquet.pushdown_filters = true;
    let optimized = FilterPushdown::new().optimize(plan, &config).unwrap();
    assert!(
        optimized.downcast_ref::<FilterExec>().is_some(),
        "parent filter must remain"
    );
}

// ==== Aggregate Dynamic Filter tests ====
//
// The end-to-end min/max dynamic filter cases (simple/min/max/mixed/all-nulls)
// have been ported to
// `datafusion/sqllogictest/test_files/push_down_filter_regression.slt`.
// The `run_aggregate_dyn_filter_case` harness used to drive them was removed
// along with the test functions.

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
          - AggregateExec: mode=Final, gby=[(a@0 as a, b@1 as b), (NULL as a, b@1 as b)], aggr=[cnt]
          -   DataSourceExec: file_groups={1 group: [[test.parquet]]}, projection=[a, b, c], file_type=test, pushdown_supported=true, predicate=b@1 = bar
    "
    );
}

#[tokio::test]
async fn test_no_pushdown_through_global_aggregate_with_name_collision() {
    let input_schema =
        Arc::new(Schema::new(vec![Field::new("a", DataType::Int64, false)]));
    let scan = TestScanBuilder::new(Arc::clone(&input_schema))
        .with_support(true)
        .with_batches(vec![record_batch!(("a", Int64, [1, 20])).unwrap()])
        .build();
    let aggregate_expr = vec![
        AggregateExprBuilder::new(max_udaf(), vec![col("a", &input_schema).unwrap()])
            .schema(Arc::clone(&input_schema))
            .alias("a")
            .build()
            .map(Arc::new)
            .unwrap(),
    ];
    let aggregate = Arc::new(
        AggregateExec::try_new(
            AggregateMode::Single,
            PhysicalGroupBy::new_single(vec![]),
            aggregate_expr,
            vec![None],
            scan,
            input_schema,
        )
        .unwrap(),
    );

    // This is a physical filter above the aggregate, not a SQL WHERE clause.
    // Pushing it through would evaluate input `a` instead of MAX(a).
    let predicate = Arc::new(BinaryExpr::new(
        col("a", aggregate.schema().as_ref()).unwrap(),
        Operator::Lt,
        Arc::new(Literal::new(ScalarValue::Int64(Some(10)))),
    ));
    let plan = Arc::new(FilterExec::try_new(predicate, aggregate).unwrap());

    let mut config = ConfigOptions::default();
    config.execution.parquet.pushdown_filters = true;
    let optimized = FilterPushdown::new().optimize(plan, &config).unwrap();
    assert!(optimized.downcast_ref::<FilterExec>().is_some());

    let session_ctx = SessionContext::new();
    session_ctx.register_object_store(
        ObjectStoreUrl::parse("test://").unwrap().as_ref(),
        Arc::new(InMemory::new()),
    );
    let batches = collect(optimized, session_ctx.state().task_ctx())
        .await
        .unwrap();
    assert!(
        batches.is_empty(),
        "MAX(a) = 20 must be filtered out instead of applying a < 10 to input rows"
    );
}

#[test]
fn test_no_pushdown_constant_false_through_global_aggregate() {
    let scan = TestScanBuilder::new(schema()).with_support(true).build();
    let aggregate_expr = vec![
        AggregateExprBuilder::new(count_udaf(), vec![col("c", &schema()).unwrap()])
            .schema(schema())
            .alias("cnt")
            .build()
            .map(Arc::new)
            .unwrap(),
    ];
    let aggregate = Arc::new(
        AggregateExec::try_new(
            AggregateMode::Final,
            PhysicalGroupBy::new_single(vec![]),
            aggregate_expr,
            vec![None],
            scan,
            schema(),
        )
        .unwrap(),
    );
    let predicate = Arc::new(Literal::new(ScalarValue::Boolean(Some(false))));
    let plan = Arc::new(FilterExec::try_new(predicate, aggregate).unwrap());

    assert_parent_filter_remains(plan);
}

#[test]
fn test_no_pushdown_constant_false_through_empty_grouping_set() {
    let scan = TestScanBuilder::new(schema()).with_support(true).build();
    let group_by = PhysicalGroupBy::new(
        vec![(col("a", &schema()).unwrap(), "a".to_string())],
        vec![(
            Arc::new(Literal::new(ScalarValue::Utf8(None))),
            "a".to_string(),
        )],
        vec![vec![true]],
        true,
    );
    let aggregate_expr = vec![
        AggregateExprBuilder::new(count_udaf(), vec![col("c", &schema()).unwrap()])
            .schema(schema())
            .alias("cnt")
            .build()
            .map(Arc::new)
            .unwrap(),
    ];
    let aggregate = Arc::new(
        AggregateExec::try_new(
            AggregateMode::Final,
            group_by,
            aggregate_expr,
            vec![None],
            scan,
            schema(),
        )
        .unwrap(),
    );
    let predicate = Arc::new(Literal::new(ScalarValue::Boolean(Some(false))));
    let plan = Arc::new(FilterExec::try_new(predicate, aggregate).unwrap());

    assert_parent_filter_remains(plan);
}

#[test]
fn test_pushdown_through_aggregate_with_reordered_input_columns() {
    let scan = TestScanBuilder::new(schema()).with_support(true).build();

    // Reorder scan output from (a, b, c) to (c, a, b)
    let reordered_schema = Arc::new(Schema::new(vec![
        Field::new("c", DataType::Float64, false),
        Field::new("a", DataType::Utf8, false),
        Field::new("b", DataType::Utf8, false),
    ]));
    let projection = Arc::new(
        ProjectionExec::try_new(
            vec![
                (col("c", &schema()).unwrap(), "c".to_string()),
                (col("a", &schema()).unwrap(), "a".to_string()),
                (col("b", &schema()).unwrap(), "b".to_string()),
            ],
            scan,
        )
        .unwrap(),
    );

    let aggregate_expr = vec![
        AggregateExprBuilder::new(
            count_udaf(),
            vec![col("c", &reordered_schema).unwrap()],
        )
        .schema(reordered_schema.clone())
        .alias("cnt")
        .build()
        .map(Arc::new)
        .unwrap(),
    ];

    // Group by a@1, b@2 (input indices in reordered schema)
    let group_by = PhysicalGroupBy::new_single(vec![
        (col("a", &reordered_schema).unwrap(), "a".to_string()),
        (col("b", &reordered_schema).unwrap(), "b".to_string()),
    ]);

    let aggregate = Arc::new(
        AggregateExec::try_new(
            AggregateMode::Final,
            group_by,
            aggregate_expr,
            vec![None],
            projection,
            reordered_schema,
        )
        .unwrap(),
    );

    // Filter on b@1 in aggregate's output schema (a@0, b@1, cnt@2)
    // The grouping expr for b references input index 2, but output index is 1.
    let agg_output_schema = aggregate.schema();
    let predicate = col_lit_predicate("b", "bar", &agg_output_schema);
    let plan = Arc::new(FilterExec::try_new(predicate, aggregate).unwrap());

    // The filter should be pushed down
    insta::assert_snapshot!(
        OptimizationTest::new(plan, FilterPushdown::new(), true),
        @r"
    OptimizationTest:
      input:
        - FilterExec: b@1 = bar
        -   AggregateExec: mode=Final, gby=[a@1 as a, b@2 as b], aggr=[cnt]
        -     ProjectionExec: expr=[c@2 as c, a@0 as a, b@1 as b]
        -       DataSourceExec: file_groups={1 group: [[test.parquet]]}, projection=[a, b, c], file_type=test, pushdown_supported=true
      output:
        Ok:
          - AggregateExec: mode=Final, gby=[a@1 as a, b@2 as b], aggr=[cnt], ordering_mode=PartiallySorted([1])
          -   ProjectionExec: expr=[c@2 as c, a@0 as a, b@1 as b]
          -     DataSourceExec: file_groups={1 group: [[test.parquet]]}, projection=[a, b, c], file_type=test, pushdown_supported=true, predicate=b@1 = bar
    "
    );
}

#[test]
fn test_pushdown_through_aggregate_grouping_sets_with_reordered_input() {
    let scan = TestScanBuilder::new(schema()).with_support(true).build();

    let reordered_schema = Arc::new(Schema::new(vec![
        Field::new("c", DataType::Float64, false),
        Field::new("a", DataType::Utf8, false),
        Field::new("b", DataType::Utf8, false),
    ]));
    let projection = Arc::new(
        ProjectionExec::try_new(
            vec![
                (col("c", &schema()).unwrap(), "c".to_string()),
                (col("a", &schema()).unwrap(), "a".to_string()),
                (col("b", &schema()).unwrap(), "b".to_string()),
            ],
            scan,
        )
        .unwrap(),
    );

    let aggregate_expr = vec![
        AggregateExprBuilder::new(
            count_udaf(),
            vec![col("c", &reordered_schema).unwrap()],
        )
        .schema(reordered_schema.clone())
        .alias("cnt")
        .build()
        .map(Arc::new)
        .unwrap(),
    ];

    // Use grouping sets (a, b) and (b).
    let group_by = PhysicalGroupBy::new(
        vec![
            (col("a", &reordered_schema).unwrap(), "a".to_string()),
            (col("b", &reordered_schema).unwrap(), "b".to_string()),
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
        vec![vec![false, false], vec![true, false]],
        true,
    );

    let aggregate = Arc::new(
        AggregateExec::try_new(
            AggregateMode::Final,
            group_by,
            aggregate_expr,
            vec![None],
            projection,
            reordered_schema,
        )
        .unwrap(),
    );

    let agg_output_schema = aggregate.schema();

    // Filter on b (present in all grouping sets) should be pushed down
    let predicate = col_lit_predicate("b", "bar", &agg_output_schema);
    let plan = Arc::new(FilterExec::try_new(predicate, aggregate.clone()).unwrap());

    insta::assert_snapshot!(
        OptimizationTest::new(plan, FilterPushdown::new(), true),
        @r"
    OptimizationTest:
      input:
        - FilterExec: b@1 = bar
        -   AggregateExec: mode=Final, gby=[(a@1 as a, b@2 as b), (NULL as a, b@2 as b)], aggr=[cnt]
        -     ProjectionExec: expr=[c@2 as c, a@0 as a, b@1 as b]
        -       DataSourceExec: file_groups={1 group: [[test.parquet]]}, projection=[a, b, c], file_type=test, pushdown_supported=true
      output:
        Ok:
          - AggregateExec: mode=Final, gby=[(a@1 as a, b@2 as b), (NULL as a, b@2 as b)], aggr=[cnt]
          -   ProjectionExec: expr=[c@2 as c, a@0 as a, b@1 as b]
          -     DataSourceExec: file_groups={1 group: [[test.parquet]]}, projection=[a, b, c], file_type=test, pushdown_supported=true, predicate=b@1 = bar
    "
    );

    // Filter on a (missing from second grouping set) should not be pushed down
    let predicate = col_lit_predicate("a", "foo", &agg_output_schema);
    let plan = Arc::new(FilterExec::try_new(predicate, aggregate).unwrap());

    insta::assert_snapshot!(
        OptimizationTest::new(plan, FilterPushdown::new(), true),
        @r"
    OptimizationTest:
      input:
        - FilterExec: a@0 = foo
        -   AggregateExec: mode=Final, gby=[(a@1 as a, b@2 as b), (NULL as a, b@2 as b)], aggr=[cnt]
        -     ProjectionExec: expr=[c@2 as c, a@0 as a, b@1 as b]
        -       DataSourceExec: file_groups={1 group: [[test.parquet]]}, projection=[a, b, c], file_type=test, pushdown_supported=true
      output:
        Ok:
          - FilterExec: a@0 = foo
          -   AggregateExec: mode=Final, gby=[(a@1 as a, b@2 as b), (NULL as a, b@2 as b)], aggr=[cnt]
          -     ProjectionExec: expr=[c@2 as c, a@0 as a, b@1 as b]
          -       DataSourceExec: file_groups={1 group: [[test.parquet]]}, projection=[a, b, c], file_type=test, pushdown_supported=true
    "
    );
}

/// Regression test for https://github.com/apache/datafusion/issues/21065.
///
/// Given a plan similar to the following, ensure that the filter is pushed down
/// through an AggregateExec whose input columns are reordered by a ProjectionExec.
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

// Not portable to sqllogictest: in CollectLeft (the mode SQL picks for small
// data), an empty build side short-circuits the HashJoin and the probe scan
// is never executed, so its dynamic filter stays at `[ empty ]` rather than
// collapsing to `[ false ]`. The Rust test uses PartitionMode::Partitioned
// on a hand-wired plan, which does trigger the `false` path.
#[tokio::test]
async fn test_hashjoin_dynamic_filter_all_partitions_empty() {
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

// Not portable to sqllogictest: same reason as
// test_hashjoin_dynamic_filter_pushdown_partitioned — hand-wires
// PartitionMode::Partitioned, which SQL never picks for small parquet inputs.
#[tokio::test]
async fn test_hashjoin_hash_table_pushdown_partitioned() {
    let (build_side_schema, build_scan, probe_side_schema, probe_scan) =
        hashjoin_pushdown_scans();

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
    let mut config = ConfigOptions::default();
    config.optimizer.hash_join_inlist_pushdown_max_size = 1;
    config.execution.parquet.pushdown_filters = true;
    config.optimizer.enable_dynamic_filter_pushdown = true;
    let (plan, batches) = optimize_and_collect_pushdown_plan(plan, config).await;

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

// Ported to push_down_filter_parquet.slt (`hl_build`/`hl_probe` fixture).
// Rust version retained only because the slt port cannot hand-wire the
// RepartitionExec-above-probe shape this test uses; the hash_lookup vs
// IN (SET) invariant is captured in the slt port.
#[tokio::test]
async fn test_hashjoin_hash_table_pushdown_collect_left() {
    let (build_side_schema, build_scan, probe_side_schema, probe_scan) =
        hashjoin_pushdown_scans();

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
    let mut config = ConfigOptions::default();
    config.optimizer.hash_join_inlist_pushdown_max_size = 1;
    config.execution.parquet.pushdown_filters = true;
    config.optimizer.enable_dynamic_filter_pushdown = true;
    let (plan, batches) = optimize_and_collect_pushdown_plan(plan, config).await;

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

// Not portable to sqllogictest: verifies that the HashJoinExec only keeps its
// dynamic filter when the optimized probe-side plan retains the expression, i.e.
// when there is something to consume it. The with_support(false) branch has no
// SQL analog because parquet supports filter pushdown.
#[test]
fn test_hashjoin_dynamic_filter_requires_probe_consumer() {
    for (probe_supports_pushdown, expected_consumer) in [(false, false), (true, true)] {
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

        let mut config = ConfigOptions::default();
        config.execution.parquet.pushdown_filters = true;
        config.optimizer.enable_dynamic_filter_pushdown = true;
        let plan = FilterPushdown::new_post_optimization()
            .optimize(plan, &config)
            .unwrap();
        let hash_join = plan
            .downcast_ref::<HashJoinExec>()
            .expect("Plan should be HashJoinExec");
        let dynamic_filters = hash_join.dynamic_expressions_produced();

        // The join keeps a dynamic filter only if the pushdown left a consumer for it
        // in the probe subtree. Otherwise it is dropped at planning time so that
        // `execute` skips build side bounds accumulation entirely.
        assert_eq!(
            dynamic_filters.len(),
            usize::from(expected_consumer),
            "dynamic filter should {}be produced when pushdown support is {probe_supports_pushdown}",
            if expected_consumer { "" } else { "not " }
        );

        if let Some(dynamic_filter) = dynamic_filters.first() {
            let expression_id = dynamic_filter
                .expression_id()
                .expect("Dynamic filters always have an expression ID");
            assert!(
                plan_contains_expression_id(hash_join.right(), expression_id).unwrap(),
                "probe subtree should contain the dynamic filter it accepted"
            );
        }
    }
}

// Not portable to sqllogictest: stands in for a distributed planner that splits
// an already-optimized plan into stages, leaving the HashJoinExec and the scan
// consuming its dynamic filter on different workers. Whether to produce the
// filter is decided during pushdown, so it has to survive the probe subtree
// being swapped out afterwards.
#[tokio::test]
async fn test_hashjoin_dynamic_filter_survives_probe_subtree_replacement() {
    let (build_side_schema, build_scan, probe_side_schema, probe_scan) =
        hashjoin_pushdown_scans();

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
            Arc::clone(&build_scan),
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

    let mut config = ConfigOptions::default();
    config.execution.parquet.pushdown_filters = true;
    config.optimizer.enable_dynamic_filter_pushdown = true;
    let plan = FilterPushdown::new_post_optimization()
        .optimize(plan, &config)
        .unwrap();

    // The probe scan accepted the filter, so the join kept it.
    assert_eq!(plan.dynamic_expressions_produced().len(), 1);

    // Swap the probe subtree for one that does not hold the filter: in a real
    // deployment the consumer ends up in another stage, on another worker, and is
    // not reachable from this node at all.
    let detached_probe = TestScanBuilder::new(Arc::clone(&probe_side_schema))
        .with_support(true)
        .with_batches(vec![
            record_batch!(
                ("a", Utf8, ["aa", "ab", "ac", "ad"]),
                ("b", Utf8, ["ba", "bb", "bc", "bd"]),
                ("e", Float64, [1.0, 2.0, 3.0, 4.0])
            )
            .unwrap(),
        ])
        .build();
    let plan = plan
        .replace_children(
            vec![build_scan, detached_probe],
            ReplaceChildrenOptions::new(ChildrenPropertiesMode::Recompute),
        )
        .unwrap();
    assert_eq!(plan.dynamic_expressions_produced().len(), 1);

    let session_ctx =
        SessionContext::new_with_config(SessionConfig::from(config).with_batch_size(10));
    session_ctx.register_object_store(
        ObjectStoreUrl::parse("test://").unwrap().as_ref(),
        Arc::new(InMemory::new()),
    );
    collect(Arc::clone(&plan), session_ctx.state().task_ctx())
        .await
        .unwrap();

    // The build side bounds were still computed and published, so whatever holds
    // the other end of this filter sees them.
    let dynamic_filter = plan
        .dynamic_expressions_produced()
        .into_iter()
        .next()
        .expect("dynamic filter should be retained");
    insta::assert_snapshot!(
        format!("{dynamic_filter}"),
        @"DynamicFilter [ a@0 >= aa AND a@0 <= ab AND b@1 >= ba AND b@1 <= bb AND struct(a@0, b@1) IN (SET) ([{c0:aa,c1:ba}, {c0:ab,c1:bb}]) ]",
    );
}

// Not portable to sqllogictest: custom `PhysicalOptimizerRule`s are appended
// after the built in ones, so a user rule runs after the Post phase
// `FilterPushdown` that decides whether to keep the join's dynamic filter. Such a
// rule can bring a consumer back by re-running the pushdown; there is nothing to
// undo first, because a join with no consumer holds no dynamic filter.
//
// Re-running the Post phase is an already supported mode rather than something
// this test invents: see `post_phase_is_idempotent_on_hash_join` below, added by
// apache/datafusion#22523 because AQE (datafusion-ballista#1359) re-runs the
// optimizer chain after every completed stage.
#[test]
fn test_hashjoin_dynamic_filter_recreated_when_pushdown_reruns() {
    let (build_side_schema, build_scan, probe_side_schema, _) = hashjoin_pushdown_scans();

    let probe_batches = vec![
        record_batch!(
            ("a", Utf8, ["aa", "ab", "ac", "ad"]),
            ("b", Utf8, ["ba", "bb", "bc", "bd"]),
            ("e", Float64, [1.0, 2.0, 3.0, 4.0])
        )
        .unwrap(),
    ];
    let unsupported_probe = TestScanBuilder::new(Arc::clone(&probe_side_schema))
        .with_support(false)
        .with_batches(probe_batches.clone())
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
            Arc::clone(&build_scan),
            unsupported_probe,
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

    let mut config = ConfigOptions::default();
    config.execution.parquet.pushdown_filters = true;
    config.optimizer.enable_dynamic_filter_pushdown = true;

    // Nothing in the probe side accepts the filter, so the join drops it.
    let plan = FilterPushdown::new_post_optimization()
        .optimize(plan, &config)
        .unwrap();
    assert_eq!(plan.dynamic_expressions_produced().len(), 0);

    // A later rule swaps in a probe side that does accept filters and re-runs the
    // pushdown. The join creates a fresh filter and finds its consumer.
    let supported_probe = TestScanBuilder::new(Arc::clone(&probe_side_schema))
        .with_support(true)
        .with_batches(probe_batches)
        .build();
    let plan = plan
        .replace_children(
            vec![build_scan, supported_probe],
            ReplaceChildrenOptions::new(ChildrenPropertiesMode::Recompute),
        )
        .unwrap();
    let plan = FilterPushdown::new_post_optimization()
        .optimize(plan, &config)
        .unwrap();
    assert_eq!(plan.dynamic_expressions_produced().len(), 1);
}

/// Regression test for https://github.com/apache/datafusion/issues/20109.
///
/// Not portable to sqllogictest: the regression specifically targets the
/// physical FilterPushdown rule running over *stacked* FilterExecs with
/// projections on a MemorySourceConfig. In SQL the logical optimizer
/// collapses the two filters before physical planning, so the stacked
/// FilterExec shape this test exercises is unreachable.
#[tokio::test]
async fn test_filter_with_projection_pushdown() {
    use arrow::array::{Int64Array, RecordBatch, StringArray};
    use datafusion_physical_plan::collect;
    use datafusion_physical_plan::filter::FilterExecBuilder;

    // Create schema: [time, event, size]
    let schema = Arc::new(Schema::new(vec![
        Field::new("time", DataType::Int64, false),
        Field::new("event", DataType::Utf8, false),
        Field::new("size", DataType::Int64, false),
    ]));

    // Create sample data
    let timestamps = vec![100i64, 200, 300, 400, 500];
    let events = vec!["Ingestion", "Ingestion", "Query", "Ingestion", "Query"];
    let sizes = vec![10i64, 20, 30, 40, 50];

    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Int64Array::from(timestamps)),
            Arc::new(StringArray::from(events)),
            Arc::new(Int64Array::from(sizes)),
        ],
    )
    .unwrap();

    // Create data source
    let memory_exec = datafusion_datasource::memory::MemorySourceConfig::try_new_exec(
        &[vec![batch]],
        schema.clone(),
        None,
    )
    .unwrap();

    // First FilterExec: time < 350 with projection=[event@1, size@2]
    let time_col = col("time", &memory_exec.schema()).unwrap();
    let time_filter = Arc::new(BinaryExpr::new(
        time_col,
        Operator::Lt,
        Arc::new(Literal::new(ScalarValue::Int64(Some(350)))),
    ));
    let filter1 = Arc::new(
        FilterExecBuilder::new(time_filter, memory_exec)
            .apply_projection(Some(vec![1, 2]))
            .unwrap()
            .build()
            .unwrap(),
    );

    // Second FilterExec: event = 'Ingestion' with projection=[size@1]
    let event_col = col("event", &filter1.schema()).unwrap();
    let event_filter = Arc::new(BinaryExpr::new(
        event_col,
        Operator::Eq,
        Arc::new(Literal::new(ScalarValue::Utf8(Some(
            "Ingestion".to_string(),
        )))),
    ));
    let filter2 = Arc::new(
        FilterExecBuilder::new(event_filter, filter1)
            .apply_projection(Some(vec![1]))
            .unwrap()
            .build()
            .unwrap(),
    );

    // Apply filter pushdown optimization
    let config = ConfigOptions::default();
    let optimized_plan = FilterPushdown::new()
        .optimize(Arc::clone(&filter2) as Arc<dyn ExecutionPlan>, &config)
        .unwrap();

    // Execute the optimized plan - this should not error
    let ctx = SessionContext::new();
    let result = collect(optimized_plan, ctx.task_ctx()).await.unwrap();

    // Verify results: should return rows where time < 350 AND event = 'Ingestion'
    // That's rows with time=100,200 (both have event='Ingestion'), so sizes 10,20
    let expected = [
        "+------+", "| size |", "+------+", "| 10   |", "| 20   |", "+------+",
    ];
    assert_batches_eq!(expected, &result);
}

/// Test that ExecutionPlan::apply_expressions() can discover dynamic filters across the plan tree.
///
/// Not portable to sqllogictest: asserts by walking the plan tree with
/// `apply_expressions` + `downcast_ref::<DynamicFilterPhysicalExpr>` and
/// counting nodes. Neither API is observable from SQL.
#[tokio::test]
async fn test_discover_dynamic_filters_via_expressions_api() {
    fn count_dynamic_filters(plan: &Arc<dyn ExecutionPlan>) -> usize {
        let mut count = 0;

        // Check expressions from this node using apply_expressions
        let _ = plan.apply_expressions(&mut |expr| {
            if let Some(_df) = expr.downcast_ref::<DynamicFilterPhysicalExpr>() {
                count += 1;
            }
            Ok(TreeNodeRecursion::Continue)
        });

        // Recursively visit children
        for child in plan.children() {
            count += count_dynamic_filters(child);
        }

        count
    }

    // Create build side (left)
    let build_batches =
        vec![record_batch!(("a", Utf8, ["foo", "bar"]), ("b", Int32, [1, 2])).unwrap()];
    let build_schema = Arc::new(Schema::new(vec![
        Field::new("a", DataType::Utf8, false),
        Field::new("b", DataType::Int32, false),
    ]));
    let build_scan = TestScanBuilder::new(build_schema.clone())
        .with_support(true)
        .with_batches(build_batches)
        .build();

    // Create probe side (right)
    let probe_batches = vec![
        record_batch!(
            ("a", Utf8, ["foo", "bar", "baz", "qux"]),
            ("c", Float64, [1.0, 2.0, 3.0, 4.0])
        )
        .unwrap(),
    ];
    let probe_schema = Arc::new(Schema::new(vec![
        Field::new("a", DataType::Utf8, false),
        Field::new("c", DataType::Float64, false),
    ]));
    let probe_scan = TestScanBuilder::new(probe_schema.clone())
        .with_support(true)
        .with_batches(probe_batches)
        .build();

    // Create HashJoinExec
    let plan = Arc::new(
        HashJoinExec::try_new(
            build_scan,
            probe_scan,
            vec![(
                col("a", &build_schema).unwrap(),
                col("a", &probe_schema).unwrap(),
            )],
            None,
            &JoinType::Inner,
            None,
            PartitionMode::CollectLeft,
            datafusion_common::NullEquality::NullEqualsNothing,
            false,
        )
        .unwrap(),
    ) as Arc<dyn ExecutionPlan>;

    // Before optimization: no dynamic filters
    let count_before = count_dynamic_filters(&plan);
    assert_eq!(
        count_before, 0,
        "Before optimization, should have no dynamic filters"
    );

    // Apply filter pushdown optimization (this creates dynamic filters)
    let mut config = ConfigOptions::default();
    config.optimizer.enable_dynamic_filter_pushdown = true;
    config.execution.parquet.pushdown_filters = true;
    let optimized_plan = FilterPushdown::new_post_optimization()
        .optimize(plan, &config)
        .unwrap();

    // After optimization: should discover dynamic filters
    // We expect 2 dynamic filters:
    // 1. In the HashJoinExec (producer)
    // 2. In the DataSourceExec (consumer, pushed down to the probe side)
    let count_after = count_dynamic_filters(&optimized_plan);
    assert_eq!(
        count_after, 2,
        "After optimization, should discover exactly 2 dynamic filters (1 in HashJoinExec, 1 in DataSourceExec), found {count_after}"
    );
}

#[test]
fn test_discover_dynamic_expression_producers() {
    fn producer_count(plan: &Arc<dyn ExecutionPlan>) -> usize {
        let mut count = 0;
        plan.apply(|node| {
            count += node.dynamic_expressions_produced().len();
            Ok(TreeNodeRecursion::Continue)
        })
        .expect("plan traversal should succeed");
        count
    }

    let build_schema = Arc::new(Schema::new(vec![
        Field::new("a", DataType::Utf8, false),
        Field::new("b", DataType::Int32, false),
    ]));
    let build_scan = TestScanBuilder::new(Arc::clone(&build_schema))
        .with_support(true)
        .with_batches(vec![
            record_batch!(("a", Utf8, ["foo", "bar"]), ("b", Int32, [1, 2])).unwrap(),
        ])
        .build();

    let probe_schema = Arc::new(Schema::new(vec![
        Field::new("a", DataType::Utf8, false),
        Field::new("c", DataType::Float64, false),
    ]));
    let probe_scan = TestScanBuilder::new(Arc::clone(&probe_schema))
        .with_support(true)
        .with_batches(vec![
            record_batch!(
                ("a", Utf8, ["foo", "bar", "baz", "qux"]),
                ("c", Float64, [1.0, 2.0, 3.0, 4.0])
            )
            .unwrap(),
        ])
        .build();

    let plan = Arc::new(
        HashJoinExec::try_new(
            build_scan,
            probe_scan,
            vec![(
                col("a", &build_schema).unwrap(),
                col("a", &probe_schema).unwrap(),
            )],
            None,
            &JoinType::Inner,
            None,
            PartitionMode::CollectLeft,
            datafusion_common::NullEquality::NullEqualsNothing,
            false,
        )
        .unwrap(),
    ) as Arc<dyn ExecutionPlan>;
    assert_eq!(producer_count(&plan), 0);

    let mut config = ConfigOptions::default();
    config.optimizer.enable_dynamic_filter_pushdown = true;
    config.execution.parquet.pushdown_filters = true;
    let optimized_plan = FilterPushdown::new_post_optimization()
        .optimize(plan, &config)
        .unwrap();
    assert_eq!(producer_count(&optimized_plan), 1);
}

// ==== Filter pushdown through SortExec tests ====

/// FilterExec above a plain SortExec (no fetch) should be pushed below it.
/// The scan supports pushdown, so the filter lands in the DataSourceExec.
#[test]
fn test_filter_pushdown_through_sort_into_scan() {
    let scan = TestScanBuilder::new(schema()).with_support(true).build();
    let sort = Arc::new(SortExec::new(
        LexOrdering::new(vec![PhysicalSortExpr::new_default(
            col("a", &schema()).unwrap(),
        )])
        .unwrap(),
        scan,
    ));
    let predicate = col_lit_predicate("a", "foo", &schema());
    let plan = Arc::new(FilterExec::try_new(predicate, sort).unwrap());

    insta::assert_snapshot!(
        OptimizationTest::new(plan, FilterPushdown::new(), true),
        @r"
    OptimizationTest:
      input:
        - FilterExec: a@0 = foo
        -   SortExec: expr=[a@0 ASC], preserve_partitioning=[false]
        -     DataSourceExec: file_groups={1 group: [[test.parquet]]}, projection=[a, b, c], file_type=test, pushdown_supported=true
      output:
        Ok:
          - SortExec: expr=[a@0 ASC], preserve_partitioning=[false]
          -   DataSourceExec: file_groups={1 group: [[test.parquet]]}, projection=[a, b, c], file_type=test, pushdown_supported=true, predicate=a@0 = foo
    "
    );
}

/// FilterExec above a plain SortExec (no fetch) when the scan does NOT
/// support pushdown. The filter should still move below the sort, landing
/// as a new FilterExec between SortExec and DataSourceExec.
#[test]
fn test_filter_pushdown_through_sort_no_scan_support() {
    let scan = TestScanBuilder::new(schema()).with_support(false).build();
    let sort = Arc::new(SortExec::new(
        LexOrdering::new(vec![PhysicalSortExpr::new_default(
            col("a", &schema()).unwrap(),
        )])
        .unwrap(),
        scan,
    ));
    let predicate = col_lit_predicate("a", "foo", &schema());
    let plan = Arc::new(FilterExec::try_new(predicate, sort).unwrap());

    insta::assert_snapshot!(
        OptimizationTest::new(plan, FilterPushdown::new(), false),
        @r"
    OptimizationTest:
      input:
        - FilterExec: a@0 = foo
        -   SortExec: expr=[a@0 ASC], preserve_partitioning=[false]
        -     DataSourceExec: file_groups={1 group: [[test.parquet]]}, projection=[a, b, c], file_type=test, pushdown_supported=false
      output:
        Ok:
          - SortExec: expr=[a@0 ASC], preserve_partitioning=[false]
          -   FilterExec: a@0 = foo
          -     DataSourceExec: file_groups={1 group: [[test.parquet]]}, projection=[a, b, c], file_type=test, pushdown_supported=false
    "
    );
}

/// Multiple conjunctive filters above a plain SortExec should all be
/// pushed below the sort as a single FilterExec.
#[test]
fn test_multiple_filters_pushdown_through_sort() {
    let scan = TestScanBuilder::new(schema()).with_support(false).build();
    let sort = Arc::new(SortExec::new(
        LexOrdering::new(vec![PhysicalSortExpr::new_default(
            col("a", &schema()).unwrap(),
        )])
        .unwrap(),
        scan,
    ));
    // WHERE a = 'foo' AND b = 'bar'
    let predicate = Arc::new(BinaryExpr::new(
        col_lit_predicate("a", "foo", &schema()),
        Operator::And,
        col_lit_predicate("b", "bar", &schema()),
    )) as Arc<dyn PhysicalExpr>;
    let plan = Arc::new(FilterExec::try_new(predicate, sort).unwrap());

    insta::assert_snapshot!(
        OptimizationTest::new(plan, FilterPushdown::new(), false),
        @r"
    OptimizationTest:
      input:
        - FilterExec: a@0 = foo AND b@1 = bar
        -   SortExec: expr=[a@0 ASC], preserve_partitioning=[false]
        -     DataSourceExec: file_groups={1 group: [[test.parquet]]}, projection=[a, b, c], file_type=test, pushdown_supported=false
      output:
        Ok:
          - SortExec: expr=[a@0 ASC], preserve_partitioning=[false]
          -   FilterExec: a@0 = foo AND b@1 = bar
          -     DataSourceExec: file_groups={1 group: [[test.parquet]]}, projection=[a, b, c], file_type=test, pushdown_supported=false
    "
    );
}

/// FilterExec above a SortExec with fetch (TopK) must NOT be pushed below,
/// because limiting happens after filtering — changing the order would alter
/// the result set.
#[test]
fn test_filter_not_pushed_through_sort_with_fetch() {
    let scan = TestScanBuilder::new(schema()).with_support(false).build();
    let sort = Arc::new(
        SortExec::new(
            LexOrdering::new(vec![PhysicalSortExpr::new_default(
                col("a", &schema()).unwrap(),
            )])
            .unwrap(),
            scan,
        )
        .with_fetch(Some(10)),
    );
    let predicate = col_lit_predicate("a", "foo", &schema());
    let plan = Arc::new(FilterExec::try_new(predicate, sort).unwrap());

    insta::assert_snapshot!(
        OptimizationTest::new(plan, FilterPushdown::new(), false),
        @r"
    OptimizationTest:
      input:
        - FilterExec: a@0 = foo
        -   SortExec: TopK(fetch=10), expr=[a@0 ASC], preserve_partitioning=[false]
        -     DataSourceExec: file_groups={1 group: [[test.parquet]]}, projection=[a, b, c], file_type=test, pushdown_supported=false
      output:
        Ok:
          - FilterExec: a@0 = foo
          -   SortExec: TopK(fetch=10), expr=[a@0 ASC], preserve_partitioning=[false]
          -     DataSourceExec: file_groups={1 group: [[test.parquet]]}, projection=[a, b, c], file_type=test, pushdown_supported=false
    "
    );
}

/// FilterExec above a SortExec with fetch (TopK) must NOT be pushed below,
/// because limiting happens after filtering — changing the order would alter
/// the result set.
#[test]
fn test_filter_pushed_through_sort_with_fetch() {
    let scan = TestScanBuilder::new(schema()).with_support(true).build();
    let sort = Arc::new(
        SortExec::new(
            LexOrdering::new(vec![PhysicalSortExpr::new_default(
                col("a", &schema()).unwrap(),
            )])
            .unwrap(),
            scan,
        )
        .with_fetch(Some(10)),
    );
    let predicate = col_lit_predicate("a", "foo", &schema());
    let plan = Arc::new(FilterExec::try_new(predicate, sort).unwrap());

    insta::assert_snapshot!(
        OptimizationTest::new(plan, FilterPushdown::new(), false),
        @r"
    OptimizationTest:
      input:
        - FilterExec: a@0 = foo
        -   SortExec: TopK(fetch=10), expr=[a@0 ASC], preserve_partitioning=[false]
        -     DataSourceExec: file_groups={1 group: [[test.parquet]]}, projection=[a, b, c], file_type=test, pushdown_supported=true
      output:
        Ok:
          - FilterExec: a@0 = foo
          -   SortExec: TopK(fetch=10), expr=[a@0 ASC], preserve_partitioning=[false]
          -     DataSourceExec: file_groups={1 group: [[test.parquet]]}, projection=[a, b, c], file_type=test, pushdown_supported=true
    "
    );
}

/// FilterExec with a projection above SortExec. The filter should be pushed
/// below the sort, and the projection should be preserved as a
/// ProjectionExec on top.
#[test]
fn test_filter_with_projection_pushdown_through_sort() {
    let scan = TestScanBuilder::new(schema()).with_support(false).build();
    let sort = Arc::new(SortExec::new(
        LexOrdering::new(vec![PhysicalSortExpr::new_default(
            col("a", &schema()).unwrap(),
        )])
        .unwrap(),
        scan,
    ));
    // FilterExec: b = 'bar', projection=[a] (only output column a)
    let predicate = col_lit_predicate("b", "bar", &schema());
    let plan = Arc::new(
        FilterExecBuilder::new(predicate, sort)
            .apply_projection(Some(vec![0]))
            .unwrap()
            .build()
            .unwrap(),
    );

    insta::assert_snapshot!(
        OptimizationTest::new(plan, FilterPushdown::new(), false),
        @r"
    OptimizationTest:
      input:
        - FilterExec: b@1 = bar, projection=[a@0]
        -   SortExec: expr=[a@0 ASC], preserve_partitioning=[false]
        -     DataSourceExec: file_groups={1 group: [[test.parquet]]}, projection=[a, b, c], file_type=test, pushdown_supported=false
      output:
        Ok:
          - ProjectionExec: expr=[a@0 as a]
          -   SortExec: expr=[a@0 ASC], preserve_partitioning=[false]
          -     FilterExec: b@1 = bar
          -       DataSourceExec: file_groups={1 group: [[test.parquet]]}, projection=[a, b, c], file_type=test, pushdown_supported=false
    "
    );
}

/// SortExec with preserve_partitioning=true should keep that setting after
/// filters are pushed below it.
#[test]
fn test_filter_pushdown_through_sort_preserves_partitioning() {
    let scan = TestScanBuilder::new(schema()).with_support(false).build();
    let sort = Arc::new(
        SortExec::new(
            LexOrdering::new(vec![PhysicalSortExpr::new_default(
                col("a", &schema()).unwrap(),
            )])
            .unwrap(),
            scan,
        )
        .with_preserve_partitioning(true),
    );
    let predicate = col_lit_predicate("a", "foo", &schema());
    let plan = Arc::new(FilterExec::try_new(predicate, sort).unwrap());

    insta::assert_snapshot!(
        OptimizationTest::new(plan, FilterPushdown::new(), false),
        @r"
    OptimizationTest:
      input:
        - FilterExec: a@0 = foo
        -   SortExec: expr=[a@0 ASC], preserve_partitioning=[true]
        -     DataSourceExec: file_groups={1 group: [[test.parquet]]}, projection=[a, b, c], file_type=test, pushdown_supported=false
      output:
        Ok:
          - SortExec: expr=[a@0 ASC], preserve_partitioning=[true]
          -   FilterExec: a@0 = foo
          -     DataSourceExec: file_groups={1 group: [[test.parquet]]}, projection=[a, b, c], file_type=test, pushdown_supported=false
    "
    );
}

/// FilterExec **with a fetch limit** above a plain SortExec. When the filter
/// is pushed below the sort the fetch should be propagated to the SortExec
/// (turning it into a TopK).
#[test]
fn test_filter_with_fetch_pushdown_through_sort() {
    let scan = TestScanBuilder::new(schema()).with_support(false).build();
    let sort = Arc::new(SortExec::new(
        LexOrdering::new(vec![PhysicalSortExpr::new_default(
            col("a", &schema()).unwrap(),
        )])
        .unwrap(),
        scan,
    ));
    let predicate = col_lit_predicate("a", "foo", &schema());
    let plan = Arc::new(
        FilterExecBuilder::new(predicate, sort)
            .with_fetch(Some(10))
            .build()
            .unwrap(),
    );

    insta::assert_snapshot!(
        OptimizationTest::new(plan, FilterPushdown::new(), false),
        @r"
    OptimizationTest:
      input:
        - FilterExec: a@0 = foo, fetch=10
        -   SortExec: expr=[a@0 ASC], preserve_partitioning=[false]
        -     DataSourceExec: file_groups={1 group: [[test.parquet]]}, projection=[a, b, c], file_type=test, pushdown_supported=false
      output:
        Ok:
          - SortExec: TopK(fetch=10), expr=[a@0 ASC], preserve_partitioning=[false]
          -   FilterExec: a@0 = foo
          -     DataSourceExec: file_groups={1 group: [[test.parquet]]}, projection=[a, b, c], file_type=test, pushdown_supported=false
    "
    );
}

#[test]
fn test_filter_pushdown_through_sort_with_projection() {
    let scan = TestScanBuilder::new(schema()).with_support(false).build();
    let sort = Arc::new(SortExec::new(
        LexOrdering::new(vec![PhysicalSortExpr::new(
            col("a", &schema()).unwrap(),
            SortOptions::new(true, false), // descending, nulls_last
        )])
        .unwrap(),
        scan,
    ));
    // FilterExec: b = 'bar', projection=[a] (only output column a)
    let predicate = col_lit_predicate("b", "bar", &schema());
    let plan = Arc::new(
        FilterExecBuilder::new(predicate, sort)
            .apply_projection(Some(vec![0]))
            .unwrap()
            .build()
            .unwrap(),
    );

    insta::assert_snapshot!(
        OptimizationTest::new(plan, FilterPushdown::new(), false),
        @r"
    OptimizationTest:
      input:
        - FilterExec: b@1 = bar, projection=[a@0]
        -   SortExec: expr=[a@0 DESC NULLS LAST], preserve_partitioning=[false]
        -     DataSourceExec: file_groups={1 group: [[test.parquet]]}, projection=[a, b, c], file_type=test, pushdown_supported=false
      output:
        Ok:
          - ProjectionExec: expr=[a@0 as a]
          -   SortExec: expr=[a@0 DESC NULLS LAST], preserve_partitioning=[false]
          -     FilterExec: b@1 = bar
          -       DataSourceExec: file_groups={1 group: [[test.parquet]]}, projection=[a, b, c], file_type=test, pushdown_supported=false
    "
    );
}

/// `FilterPushdown::new_post_optimization()` must be idempotent. When applied
/// to a HashJoinExec, the rule installs a dynamic filter on the probe-side
/// scan; before the fix in `HashJoinExec::gather_filters_for_pushdown`, every
/// invocation created a *new* `DynamicFilterPhysicalExpr` and ANDed it onto
/// the probe side's existing predicate, producing
/// `DynamicFilter AND DynamicFilter AND ...` after N passes.
///
/// AQE (datafusion-ballista#1359) re-runs the optimizer chain after every
/// completed stage, so this would compound indefinitely without the guard.
#[test]
fn post_phase_is_idempotent_on_hash_join() {
    use crate::physical_optimizer::test_utils::{hash_join_exec, parquet_exec, schema};
    use datafusion_physical_expr::expressions::Column;
    use datafusion_physical_optimizer::filter_pushdown::FilterPushdown;
    use datafusion_physical_plan::get_plan_string;
    use datafusion_physical_plan::joins::utils::JoinOn;

    let s = schema();
    let left = parquet_exec(Arc::clone(&s));
    let right = parquet_exec(Arc::clone(&s));
    let join_on: JoinOn = vec![(
        Arc::new(Column::new_with_schema("a", &left.schema()).unwrap()),
        Arc::new(Column::new_with_schema("a", &right.schema()).unwrap()),
    )];
    let plan = hash_join_exec(left, right, join_on, None, &JoinType::Inner).unwrap();

    let config = ConfigOptions::new();
    let rule = FilterPushdown::new_post_optimization();
    let once = rule.optimize(plan, &config).unwrap();
    let twice = rule.optimize(Arc::clone(&once), &config).unwrap();

    assert_eq!(
        get_plan_string(&once),
        get_plan_string(&twice),
        "second invocation of FilterPushdown::new_post_optimization mutated the plan",
    );
}
