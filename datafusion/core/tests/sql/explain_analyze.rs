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

use super::*;
use insta::assert_snapshot;
use rstest::rstest;

use datafusion::config::ConfigOptions;
use datafusion::physical_plan::display::DisplayableExecutionPlan;
use datafusion::physical_plan::metrics::Timestamp;
use datafusion_common::format::ExplainAnalyzeLevel;
use object_store::path::Path;

#[tokio::test]
async fn explain_analyze_baseline_metrics() {
    // This test uses the execute function to run an actual plan under EXPLAIN ANALYZE
    // and then validate the presence of baseline metrics for supported operators
    let config = SessionConfig::new()
        .with_target_partitions(3)
        .with_batch_size(4096);
    let ctx = SessionContext::new_with_config(config);
    register_aggregate_csv_by_sql(&ctx).await;
    // a query with as many operators as we have metrics for
    let sql = "EXPLAIN ANALYZE \
               SELECT count(*) as cnt FROM \
                 (SELECT count(*), c1 \
                  FROM aggregate_test_100 \
                  WHERE c13 != 'C2GT5KVyOPZpgKVl110TyZO0NcJ434' \
                  GROUP BY c1 \
                  ORDER BY c1 ) AS a \
                 UNION ALL \
               SELECT 1 as cnt \
                 UNION ALL \
               SELECT lead(c1, 1) OVER () as cnt FROM (select 1 as c1) AS b \
               LIMIT 3";
    println!("running query: {sql}");
    let dataframe = ctx.sql(sql).await.unwrap();
    let physical_plan = dataframe.create_physical_plan().await.unwrap();
    let task_ctx = ctx.task_ctx();
    let results = collect(physical_plan.clone(), task_ctx).await.unwrap();
    let formatted = arrow::util::pretty::pretty_format_batches(&results)
        .unwrap()
        .to_string();

    println!("Query Output:\n\n{formatted}");

    assert_metrics!(
        &formatted,
        "AggregateExec: mode=Partial, gby=[]",
        "metrics=[output_rows=3, elapsed_compute=",
        "output_bytes=",
        "output_batches=3"
    );

    assert_metrics!(
        &formatted,
        "AggregateExec: mode=Partial, gby=[c1@0 as c1]",
        "reduction_factor=5.1% (5/99)"
    );

    {
        let expected_batch_count_after_repartition =
            if cfg!(not(feature = "force_hash_collisions")) {
                "output_batches=3"
            } else {
                "output_batches=1"
            };

        assert_metrics!(
            &formatted,
            "AggregateExec: mode=FinalPartitioned, gby=[c1@0 as c1]",
            "metrics=[output_rows=5, elapsed_compute=",
            "output_bytes=",
            expected_batch_count_after_repartition
        );

        assert_metrics!(
            &formatted,
            "RepartitionExec: partitioning=Hash([c1@0], 3), input_partitions=3",
            "metrics=[output_rows=5, elapsed_compute=",
            "output_bytes=",
            expected_batch_count_after_repartition
        );

        assert_metrics!(
            &formatted,
            "ProjectionExec: expr=[]",
            "metrics=[output_rows=5, elapsed_compute=",
            "output_bytes=",
            expected_batch_count_after_repartition
        );

        assert_metrics!(
            &formatted,
            "CoalesceBatchesExec: target_batch_size=4096",
            "metrics=[output_rows=5, elapsed_compute",
            "output_bytes=",
            expected_batch_count_after_repartition
        );
    }

    assert_metrics!(
        &formatted,
        "FilterExec: c13@1 != C2GT5KVyOPZpgKVl110TyZO0NcJ434",
        "metrics=[output_rows=99, elapsed_compute=",
        "output_bytes=",
        "output_batches=1"
    );

    assert_metrics!(
        &formatted,
        "FilterExec: c13@1 != C2GT5KVyOPZpgKVl110TyZO0NcJ434",
        "selectivity=99% (99/100)"
    );

    assert_metrics!(
        &formatted,
        "UnionExec",
        "metrics=[output_rows=3, elapsed_compute=",
        "output_bytes=",
        "output_batches=3"
    );

    assert_metrics!(
        &formatted,
        "WindowAggExec",
        "metrics=[output_rows=1, elapsed_compute=",
        "output_bytes=",
        "output_batches=1"
    );

    fn expected_to_have_metrics(plan: &dyn ExecutionPlan) -> bool {
        use datafusion::physical_plan;
        use datafusion::physical_plan::sorts;

        plan.as_any().downcast_ref::<sorts::sort::SortExec>().is_some()
            || plan.as_any().downcast_ref::<physical_plan::aggregates::AggregateExec>().is_some()
            || plan.as_any().downcast_ref::<physical_plan::filter::FilterExec>().is_some()
            || plan.as_any().downcast_ref::<physical_plan::limit::LocalLimitExec>().is_some()
            || plan.as_any().downcast_ref::<physical_plan::projection::ProjectionExec>().is_some()
            || plan.as_any().downcast_ref::<physical_plan::coalesce_batches::CoalesceBatchesExec>().is_some()
            || plan.as_any().downcast_ref::<physical_plan::coalesce_partitions::CoalescePartitionsExec>().is_some()
            || plan.as_any().downcast_ref::<physical_plan::union::UnionExec>().is_some()
            || plan.as_any().downcast_ref::<physical_plan::windows::WindowAggExec>().is_some()
    }

    // Validate that the recorded elapsed compute time was more than
    // zero for all operators as well as the start/end timestamp are set
    struct TimeValidator {}
    impl ExecutionPlanVisitor for TimeValidator {
        type Error = std::convert::Infallible;

        fn pre_visit(&mut self, plan: &dyn ExecutionPlan) -> Result<bool, Self::Error> {
            if !expected_to_have_metrics(plan) {
                return Ok(true);
            }
            let metrics = plan.metrics().unwrap().aggregate_by_name();

            assert!(
                metrics.output_rows().unwrap() > 0,
                "plan: {}",
                DisplayableExecutionPlan::with_metrics(plan).one_line()
            );
            assert!(
                metrics.elapsed_compute().unwrap() > 0,
                "plan: {}",
                DisplayableExecutionPlan::with_metrics(plan).one_line()
            );

            let mut saw_start = false;
            let mut saw_end = false;
            metrics.iter().for_each(|m| match m.value() {
                MetricValue::StartTimestamp(ts) => {
                    saw_start = true;
                    assert!(nanos_from_timestamp(ts) > 0);
                }
                MetricValue::EndTimestamp(ts) => {
                    saw_end = true;
                    assert!(nanos_from_timestamp(ts) > 0);
                }
                _ => {}
            });

            assert!(saw_start);
            assert!(saw_end);

            Ok(true)
        }
    }

    datafusion::physical_plan::accept(physical_plan.as_ref(), &mut TimeValidator {})
        .unwrap();
}
fn nanos_from_timestamp(ts: &Timestamp) -> i64 {
    ts.value().unwrap().timestamp_nanos_opt().unwrap()
}

// Test different detail level for config `datafusion.explain.analyze_level`

async fn collect_plan_with_context(
    sql_str: &str,
    ctx: &SessionContext,
    level: ExplainAnalyzeLevel,
) -> String {
    {
        let state = ctx.state_ref();
        let mut state = state.write();
        state.config_mut().options_mut().explain.analyze_level = level;
    }
    let dataframe = ctx.sql(sql_str).await.unwrap();
    let batches = dataframe.collect().await.unwrap();
    arrow::util::pretty::pretty_format_batches(&batches)
        .unwrap()
        .to_string()
}

async fn collect_plan(sql_str: &str, level: ExplainAnalyzeLevel) -> String {
    let ctx = SessionContext::new();
    collect_plan_with_context(sql_str, &ctx, level).await
}

#[tokio::test]
async fn explain_analyze_level() {
    let sql = "EXPLAIN ANALYZE \
            SELECT * \
            FROM generate_series(10) as t1(v1) \
            ORDER BY v1 DESC";

    for (level, needle, should_contain) in [
        (ExplainAnalyzeLevel::Summary, "spill_count", false),
        (ExplainAnalyzeLevel::Summary, "output_batches", false),
        (ExplainAnalyzeLevel::Summary, "output_rows", true),
        (ExplainAnalyzeLevel::Summary, "output_bytes", true),
        (ExplainAnalyzeLevel::Dev, "spill_count", true),
        (ExplainAnalyzeLevel::Dev, "output_rows", true),
        (ExplainAnalyzeLevel::Dev, "output_bytes", true),
        (ExplainAnalyzeLevel::Dev, "output_batches", true),
    ] {
        let plan = collect_plan(sql, level).await;
        assert_eq!(
            plan.contains(needle),
            should_contain,
            "plan for level {level:?} unexpected content: {plan}"
        );
    }
}

#[tokio::test]
async fn explain_analyze_level_datasource_parquet() {
    let table_name = "tpch_lineitem_small";
    let parquet_path = "tests/data/tpch_lineitem_small.parquet";
    let sql = format!("EXPLAIN ANALYZE SELECT * FROM {table_name}");

    // Register test parquet file into context
    let ctx = SessionContext::new();
    ctx.register_parquet(table_name, parquet_path, ParquetReadOptions::default())
        .await
        .expect("register parquet table for explain analyze test");

    for (level, needle, should_contain) in [
        (ExplainAnalyzeLevel::Summary, "metadata_load_time", true),
        (ExplainAnalyzeLevel::Summary, "page_index_eval_time", false),
        (ExplainAnalyzeLevel::Dev, "metadata_load_time", true),
        (ExplainAnalyzeLevel::Dev, "page_index_eval_time", true),
    ] {
        let plan = collect_plan_with_context(&sql, &ctx, level).await;

        assert_eq!(
            plan.contains(needle),
            should_contain,
            "plan for level {level:?} unexpected content: {plan}"
        );
    }
}

#[tokio::test]
async fn explain_analyze_parquet_pruning_metrics() {
    let table_name = "tpch_lineitem_small";
    let parquet_path = "tests/data/tpch_lineitem_small.parquet";
    let ctx = SessionContext::new();
    ctx.register_parquet(table_name, parquet_path, ParquetReadOptions::default())
        .await
        .expect("register parquet table for explain analyze test");

    // Test scenario:
    // This table's l_orderkey has range [1, 7]
    // So the following query can't prune the file:
    //  select * from tpch_lineitem_small where l_orderkey = 5;
    // If change filter to `l_orderkey=10`, the whole file can be pruned using stat.
    for (l_orderkey, expected_pruning_metrics) in
        [(5, "1 total → 1 matched"), (10, "1 total → 0 matched")]
    {
        let sql = format!(
            "explain analyze select * from {table_name} where l_orderkey = {l_orderkey};"
        );

        let plan =
            collect_plan_with_context(&sql, &ctx, ExplainAnalyzeLevel::Summary).await;

        let expected_metrics =
            format!("files_ranges_pruned_statistics={expected_pruning_metrics}");

        assert_metrics!(&plan, "DataSourceExec", &expected_metrics);
    }
}

#[tokio::test]
async fn csv_explain_plans() {
    // This test verify the look of each plan in its full cycle plan creation

    let ctx = SessionContext::new();
    register_aggregate_csv_by_sql(&ctx).await;
    let sql = "EXPLAIN SELECT c1 FROM aggregate_test_100 where c2 > 10";

    // Logical plan
    // Create plan
    let msg = format!("Creating logical plan for '{sql}'");
    let dataframe = ctx.sql(sql).await.expect(&msg);
    let logical_schema = dataframe.schema();
    let plan = dataframe.logical_plan();

    //
    println!("SQL: {sql}");
    //
    // Verify schema
    let formatted = plan.display_indent_schema().to_string();
    let actual = formatted.trim();
    assert_snapshot!(
        actual,
        @r"
    Explain [plan_type:Utf8, plan:Utf8]
      Projection: aggregate_test_100.c1 [c1:Utf8View]
        Filter: aggregate_test_100.c2 > Int64(10) [c1:Utf8View, c2:Int8, c3:Int16, c4:Int16, c5:Int32, c6:Int64, c7:Int16, c8:Int32, c9:UInt32, c10:UInt64, c11:Float32, c12:Float64, c13:Utf8View]
          TableScan: aggregate_test_100 [c1:Utf8View, c2:Int8, c3:Int16, c4:Int16, c5:Int32, c6:Int64, c7:Int16, c8:Int32, c9:UInt32, c10:UInt64, c11:Float32, c12:Float64, c13:Utf8View]
    "
    );
    //
    // Verify the text format of the plan
    let formatted = plan.display_indent().to_string();
    let actual = formatted.trim();
    assert_snapshot!(
        actual,
        @r###"
    Explain
      Projection: aggregate_test_100.c1
        Filter: aggregate_test_100.c2 > Int64(10)
          TableScan: aggregate_test_100
    "###
    );
    //
    // verify the grahviz format of the plan
    let formatted = plan.display_graphviz().to_string();
    let actual = formatted.trim();
    assert_snapshot!(
        actual,
        @r#"
    // Begin DataFusion GraphViz Plan,
    // display it online here: https://dreampuf.github.io/GraphvizOnline

    digraph {
      subgraph cluster_1
      {
        graph[label="LogicalPlan"]
        2[shape=box label="Explain"]
        3[shape=box label="Projection: aggregate_test_100.c1"]
        2 -> 3 [arrowhead=none, arrowtail=normal, dir=back]
        4[shape=box label="Filter: aggregate_test_100.c2 > Int64(10)"]
        3 -> 4 [arrowhead=none, arrowtail=normal, dir=back]
        5[shape=box label="TableScan: aggregate_test_100"]
        4 -> 5 [arrowhead=none, arrowtail=normal, dir=back]
      }
      subgraph cluster_6
      {
        graph[label="Detailed LogicalPlan"]
        7[shape=box label="Explain\nSchema: [plan_type:Utf8, plan:Utf8]"]
        8[shape=box label="Projection: aggregate_test_100.c1\nSchema: [c1:Utf8View]"]
        7 -> 8 [arrowhead=none, arrowtail=normal, dir=back]
        9[shape=box label="Filter: aggregate_test_100.c2 > Int64(10)\nSchema: [c1:Utf8View, c2:Int8, c3:Int16, c4:Int16, c5:Int32, c6:Int64, c7:Int16, c8:Int32, c9:UInt32, c10:UInt64, c11:Float32, c12:Float64, c13:Utf8View]"]
        8 -> 9 [arrowhead=none, arrowtail=normal, dir=back]
        10[shape=box label="TableScan: aggregate_test_100\nSchema: [c1:Utf8View, c2:Int8, c3:Int16, c4:Int16, c5:Int32, c6:Int64, c7:Int16, c8:Int32, c9:UInt32, c10:UInt64, c11:Float32, c12:Float64, c13:Utf8View]"]
        9 -> 10 [arrowhead=none, arrowtail=normal, dir=back]
      }
    }
    // End DataFusion GraphViz Plan
    "#
    );

    // Optimized logical plan
    let state = ctx.state();
    let msg = format!("Optimizing logical plan for '{sql}': {plan}");
    let plan = state.optimize(plan).expect(&msg);
    let optimized_logical_schema = plan.schema();
    // Both schema has to be the same
    assert_eq!(logical_schema, optimized_logical_schema.as_ref());
    //
    // Verify schema
    let formatted = plan.display_indent_schema().to_string();
    let actual = formatted.trim();
    assert_snapshot!(
        actual,
        @r"
    Explain [plan_type:Utf8, plan:Utf8]
      Projection: aggregate_test_100.c1 [c1:Utf8View]
        Filter: aggregate_test_100.c2 > Int8(10) [c1:Utf8View, c2:Int8]
          TableScan: aggregate_test_100 projection=[c1, c2], partial_filters=[aggregate_test_100.c2 > Int8(10)] [c1:Utf8View, c2:Int8]
    "
    );
    //
    // Verify the text format of the plan
    let formatted = plan.display_indent().to_string();
    let actual = formatted.trim();
    assert_snapshot!(
        actual,
        @r###"
    Explain
      Projection: aggregate_test_100.c1
        Filter: aggregate_test_100.c2 > Int8(10)
          TableScan: aggregate_test_100 projection=[c1, c2], partial_filters=[aggregate_test_100.c2 > Int8(10)]

    "###
    );
    //
    // verify the grahviz format of the plan
    let formatted = plan.display_graphviz().to_string();
    let actual = formatted.trim();
    assert_snapshot!(
        actual,
        @r#"
    // Begin DataFusion GraphViz Plan,
    // display it online here: https://dreampuf.github.io/GraphvizOnline

    digraph {
      subgraph cluster_1
      {
        graph[label="LogicalPlan"]
        2[shape=box label="Explain"]
        3[shape=box label="Projection: aggregate_test_100.c1"]
        2 -> 3 [arrowhead=none, arrowtail=normal, dir=back]
        4[shape=box label="Filter: aggregate_test_100.c2 > Int8(10)"]
        3 -> 4 [arrowhead=none, arrowtail=normal, dir=back]
        5[shape=box label="TableScan: aggregate_test_100 projection=[c1, c2], partial_filters=[aggregate_test_100.c2 > Int8(10)]"]
        4 -> 5 [arrowhead=none, arrowtail=normal, dir=back]
      }
      subgraph cluster_6
      {
        graph[label="Detailed LogicalPlan"]
        7[shape=box label="Explain\nSchema: [plan_type:Utf8, plan:Utf8]"]
        8[shape=box label="Projection: aggregate_test_100.c1\nSchema: [c1:Utf8View]"]
        7 -> 8 [arrowhead=none, arrowtail=normal, dir=back]
        9[shape=box label="Filter: aggregate_test_100.c2 > Int8(10)\nSchema: [c1:Utf8View, c2:Int8]"]
        8 -> 9 [arrowhead=none, arrowtail=normal, dir=back]
        10[shape=box label="TableScan: aggregate_test_100 projection=[c1, c2], partial_filters=[aggregate_test_100.c2 > Int8(10)]\nSchema: [c1:Utf8View, c2:Int8]"]
        9 -> 10 [arrowhead=none, arrowtail=normal, dir=back]
      }
    }
    // End DataFusion GraphViz Plan
    "#
    );

    // Physical plan
    // Create plan
    let msg = format!("Creating physical plan for '{sql}': {plan}");
    let plan = state.create_physical_plan(&plan).await.expect(&msg);
    //
    // Execute plan
    let msg = format!("Executing physical plan for '{sql}': {plan:?}");
    let results = collect(plan, state.task_ctx()).await.expect(&msg);
    let actual = result_vec(&results);
    // flatten to a single string
    let actual = actual.into_iter().map(|r| r.join("\t")).collect::<String>();
    // Since the plan contains path that are environmentally dependant (e.g. full path of the test file), only verify important content
    assert_contains!(&actual, "logical_plan");
    assert_contains!(&actual, "Projection: aggregate_test_100.c1");
    assert_contains!(actual, "Filter: aggregate_test_100.c2 > Int8(10)");
}

#[tokio::test]
async fn csv_explain_verbose() {
    let ctx = SessionContext::new();
    register_aggregate_csv_by_sql(&ctx).await;
    let sql = "EXPLAIN VERBOSE SELECT c1 FROM aggregate_test_100 where c2 > 10";
    let actual = execute(&ctx, sql).await;

    // flatten to a single string
    let actual = actual.into_iter().map(|r| r.join("\t")).collect::<String>();

    // Don't actually test the contents of the debugging output (as
    // that may change and keeping this test updated will be a
    // pain). Instead just check for a few key pieces.
    assert_contains!(&actual, "logical_plan");
    assert_contains!(&actual, "physical_plan");
    assert_contains!(&actual, "aggregate_test_100.c2 > Int64(10)");

    // ensure the "same text as above" optimization is working
    assert_contains!(actual, "SAME TEXT AS ABOVE");
}

#[tokio::test]
async fn csv_explain_inlist_verbose() {
    let ctx = SessionContext::new();
    register_aggregate_csv_by_sql(&ctx).await;
    // Inlist len <=3 case will be transformed to OR List so we test with len=4
    let sql = "EXPLAIN VERBOSE SELECT c1 FROM aggregate_test_100 where c2 in (1,2,4,5)";
    let actual = execute(&ctx, sql).await;

    // Optimized by PreCastLitInComparisonExpressions rule
    // the data type of c2 is INT8, the type of `1,2,4` is INT64.
    // the value of `1,2,4` will be casted to INT8 and pre-calculated

    // flatten to a single string
    let actual = actual.into_iter().map(|r| r.join("\t")).collect::<String>();

    // before optimization (Int64 literals)
    assert_contains!(
        &actual,
        "aggregate_test_100.c2 IN ([Int64(1), Int64(2), Int64(4), Int64(5)])"
    );
    // after optimization (casted to Int8)
    assert_contains!(
        &actual,
        "aggregate_test_100.c2 IN ([Int8(1), Int8(2), Int8(4), Int8(5)])"
    );
}

#[tokio::test]
async fn csv_explain_verbose_plans() {
    // This test verify the look of each plan in its full cycle plan creation

    let ctx = SessionContext::new();
    register_aggregate_csv_by_sql(&ctx).await;
    let sql = "EXPLAIN VERBOSE SELECT c1 FROM aggregate_test_100 where c2 > 10";

    // Logical plan
    // Create plan
    let msg = format!("Creating logical plan for '{sql}'");
    let dataframe = ctx.sql(sql).await.expect(&msg);
    let logical_schema = dataframe.schema().clone();
    //
    println!("SQL: {sql}");

    //
    // Verify schema
    let formatted = dataframe.logical_plan().display_indent_schema().to_string();
    let actual = formatted.trim();
    assert_snapshot!(
        actual,
        @r"
    Explain [plan_type:Utf8, plan:Utf8]
      Projection: aggregate_test_100.c1 [c1:Utf8View]
        Filter: aggregate_test_100.c2 > Int64(10) [c1:Utf8View, c2:Int8, c3:Int16, c4:Int16, c5:Int32, c6:Int64, c7:Int16, c8:Int32, c9:UInt32, c10:UInt64, c11:Float32, c12:Float64, c13:Utf8View]
          TableScan: aggregate_test_100 [c1:Utf8View, c2:Int8, c3:Int16, c4:Int16, c5:Int32, c6:Int64, c7:Int16, c8:Int32, c9:UInt32, c10:UInt64, c11:Float32, c12:Float64, c13:Utf8View]
    "
    );
    //
    // Verify the text format of the plan
    let formatted = dataframe.logical_plan().display_indent().to_string();
    let actual = formatted.trim();
    assert_snapshot!(
        actual,
        @r###"
    Explain
      Projection: aggregate_test_100.c1
        Filter: aggregate_test_100.c2 > Int64(10)
          TableScan: aggregate_test_100
    "###
    );
    //
    // verify the grahviz format of the plan
    let formatted = dataframe.logical_plan().display_graphviz().to_string();
    let actual = formatted.trim();
    assert_snapshot!(
        actual,
        @r#"
    // Begin DataFusion GraphViz Plan,
    // display it online here: https://dreampuf.github.io/GraphvizOnline

    digraph {
      subgraph cluster_1
      {
        graph[label="LogicalPlan"]
        2[shape=box label="Explain"]
        3[shape=box label="Projection: aggregate_test_100.c1"]
        2 -> 3 [arrowhead=none, arrowtail=normal, dir=back]
        4[shape=box label="Filter: aggregate_test_100.c2 > Int64(10)"]
        3 -> 4 [arrowhead=none, arrowtail=normal, dir=back]
        5[shape=box label="TableScan: aggregate_test_100"]
        4 -> 5 [arrowhead=none, arrowtail=normal, dir=back]
      }
      subgraph cluster_6
      {
        graph[label="Detailed LogicalPlan"]
        7[shape=box label="Explain\nSchema: [plan_type:Utf8, plan:Utf8]"]
        8[shape=box label="Projection: aggregate_test_100.c1\nSchema: [c1:Utf8View]"]
        7 -> 8 [arrowhead=none, arrowtail=normal, dir=back]
        9[shape=box label="Filter: aggregate_test_100.c2 > Int64(10)\nSchema: [c1:Utf8View, c2:Int8, c3:Int16, c4:Int16, c5:Int32, c6:Int64, c7:Int16, c8:Int32, c9:UInt32, c10:UInt64, c11:Float32, c12:Float64, c13:Utf8View]"]
        8 -> 9 [arrowhead=none, arrowtail=normal, dir=back]
        10[shape=box label="TableScan: aggregate_test_100\nSchema: [c1:Utf8View, c2:Int8, c3:Int16, c4:Int16, c5:Int32, c6:Int64, c7:Int16, c8:Int32, c9:UInt32, c10:UInt64, c11:Float32, c12:Float64, c13:Utf8View]"]
        9 -> 10 [arrowhead=none, arrowtail=normal, dir=back]
      }
    }
    // End DataFusion GraphViz Plan
    "#
    );

    // Optimized logical plan
    let msg = format!("Optimizing logical plan for '{sql}': {dataframe:?}");
    let (state, plan) = dataframe.into_parts();
    let plan = state.optimize(&plan).expect(&msg);
    let optimized_logical_schema = plan.schema();
    // Both schema has to be the same
    assert_eq!(&logical_schema, optimized_logical_schema.as_ref());
    //
    // Verify schema
    let formatted = plan.display_indent_schema().to_string();
    let actual = formatted.trim();
    assert_snapshot!(
        actual,
        @r"
    Explain [plan_type:Utf8, plan:Utf8]
      Projection: aggregate_test_100.c1 [c1:Utf8View]
        Filter: aggregate_test_100.c2 > Int8(10) [c1:Utf8View, c2:Int8]
          TableScan: aggregate_test_100 projection=[c1, c2], partial_filters=[aggregate_test_100.c2 > Int8(10)] [c1:Utf8View, c2:Int8]
    "
    );
    //
    // Verify the text format of the plan
    let formatted = plan.display_indent().to_string();
    let actual = formatted.trim();
    assert_snapshot!(
        actual,
        @r###"
    Explain
      Projection: aggregate_test_100.c1
        Filter: aggregate_test_100.c2 > Int8(10)
          TableScan: aggregate_test_100 projection=[c1, c2], partial_filters=[aggregate_test_100.c2 > Int8(10)]
    "###
    );
    //
    // verify the grahviz format of the plan
    let formatted = plan.display_graphviz().to_string();
    let actual = formatted.trim();
    assert_snapshot!(
        actual,
        @r#"
    // Begin DataFusion GraphViz Plan,
    // display it online here: https://dreampuf.github.io/GraphvizOnline

    digraph {
      subgraph cluster_1
      {
        graph[label="LogicalPlan"]
        2[shape=box label="Explain"]
        3[shape=box label="Projection: aggregate_test_100.c1"]
        2 -> 3 [arrowhead=none, arrowtail=normal, dir=back]
        4[shape=box label="Filter: aggregate_test_100.c2 > Int8(10)"]
        3 -> 4 [arrowhead=none, arrowtail=normal, dir=back]
        5[shape=box label="TableScan: aggregate_test_100 projection=[c1, c2], partial_filters=[aggregate_test_100.c2 > Int8(10)]"]
        4 -> 5 [arrowhead=none, arrowtail=normal, dir=back]
      }
      subgraph cluster_6
      {
        graph[label="Detailed LogicalPlan"]
        7[shape=box label="Explain\nSchema: [plan_type:Utf8, plan:Utf8]"]
        8[shape=box label="Projection: aggregate_test_100.c1\nSchema: [c1:Utf8View]"]
        7 -> 8 [arrowhead=none, arrowtail=normal, dir=back]
        9[shape=box label="Filter: aggregate_test_100.c2 > Int8(10)\nSchema: [c1:Utf8View, c2:Int8]"]
        8 -> 9 [arrowhead=none, arrowtail=normal, dir=back]
        10[shape=box label="TableScan: aggregate_test_100 projection=[c1, c2], partial_filters=[aggregate_test_100.c2 > Int8(10)]\nSchema: [c1:Utf8View, c2:Int8]"]
        9 -> 10 [arrowhead=none, arrowtail=normal, dir=back]
      }
    }
    // End DataFusion GraphViz Plan
    "#
    );

    // Physical plan
    // Create plan
    let msg = format!("Creating physical plan for '{sql}': {plan}");
    let plan = state.create_physical_plan(&plan).await.expect(&msg);
    //
    // Execute plan
    let msg = format!("Executing physical plan for '{sql}': {plan:?}");
    let task_ctx = ctx.task_ctx();
    let results = collect(plan, task_ctx).await.expect(&msg);
    let actual = result_vec(&results);
    // flatten to a single string
    let actual = actual.into_iter().map(|r| r.join("\t")).collect::<String>();
    // Since the plan contains path that are environmentally
    // dependant(e.g. full path of the test file), only verify
    // important content
    assert_contains!(&actual, "logical_plan after optimize_projections");
    assert_contains!(&actual, "physical_plan");
    assert_contains!(&actual, "FilterExec: c2@1 > 10");
    assert_contains!(actual, "ProjectionExec: expr=[c1@0 as c1]");
}

#[rstest]
#[tokio::test]
async fn explain_analyze_runs_optimizers(#[values("*", "1")] count_expr: &str) {
    // repro for https://github.com/apache/datafusion/issues/917
    // where EXPLAIN ANALYZE was not correctly running optimizer
    let ctx = SessionContext::new_with_config(
        SessionConfig::new().with_collect_statistics(true),
    );
    register_alltypes_parquet(&ctx).await;

    // This happens as an optimization pass where count(*)/count(1) can be
    // answered using statistics only.
    let expected = "PlaceholderRowExec";

    let sql = format!("EXPLAIN SELECT count({count_expr}) from alltypes_plain");
    let actual = execute_to_batches(&ctx, &sql).await;
    let actual = arrow::util::pretty::pretty_format_batches(&actual)
        .unwrap()
        .to_string();
    assert_contains!(actual, expected);

    // EXPLAIN ANALYZE should work the same
    let sql = format!("EXPLAIN ANALYZE SELECT count({count_expr}) from alltypes_plain");
    let actual = execute_to_batches(&ctx, &sql).await;
    let actual = arrow::util::pretty::pretty_format_batches(&actual)
        .unwrap()
        .to_string();
    assert_contains!(actual, expected);
}

#[tokio::test]
async fn test_physical_plan_display_indent() {
    // Hard code target_partitions as it appears in the RepartitionExec output
    let config = SessionConfig::new()
        .with_target_partitions(9000)
        .with_batch_size(4096);
    let ctx = SessionContext::new_with_config(config);
    register_aggregate_csv(&ctx).await.unwrap();
    let sql = "SELECT c1, MAX(c12), MIN(c12) as the_min \
               FROM aggregate_test_100 \
               WHERE c12 < 10 \
               GROUP BY c1 \
               ORDER BY the_min DESC \
               LIMIT 10";
    let dataframe = ctx.sql(sql).await.unwrap();
    let physical_plan = dataframe.create_physical_plan().await.unwrap();

    let normalizer = ExplainNormalizer::new();
    let actual = format!("{}", displayable(physical_plan.as_ref()).indent(true))
        .trim()
        .lines()
        // normalize paths
        .map(|s| normalizer.normalize(s))
        .collect::<Vec<_>>()
        .join("\n");

    assert_snapshot!(
        actual,
        @r"
    SortPreservingMergeExec: [the_min@2 DESC], fetch=10
      SortExec: TopK(fetch=10), expr=[the_min@2 DESC], preserve_partitioning=[true]
        ProjectionExec: expr=[c1@0 as c1, max(aggregate_test_100.c12)@1 as max(aggregate_test_100.c12), min(aggregate_test_100.c12)@2 as the_min]
          AggregateExec: mode=FinalPartitioned, gby=[c1@0 as c1], aggr=[max(aggregate_test_100.c12), min(aggregate_test_100.c12)]
            CoalesceBatchesExec: target_batch_size=4096
              RepartitionExec: partitioning=Hash([c1@0], 9000), input_partitions=9000
                AggregateExec: mode=Partial, gby=[c1@0 as c1], aggr=[max(aggregate_test_100.c12), min(aggregate_test_100.c12)]
                  FilterExec: c12@1 < 10
                    RepartitionExec: partitioning=RoundRobinBatch(9000), input_partitions=1
                      DataSourceExec: file_groups={1 group: [[ARROW_TEST_DATA/csv/aggregate_test_100.csv]]}, projection=[c1, c12], file_type=csv, has_header=true
    "
    );
}

#[tokio::test]
async fn test_physical_plan_display_indent_multi_children() {
    // Hard code target_partitions as it appears in the RepartitionExec output
    let config = SessionConfig::new()
        .with_target_partitions(9000)
        .with_batch_size(4096);
    let ctx = SessionContext::new_with_config(config);
    // ensure indenting works for nodes with multiple children
    register_aggregate_csv(&ctx).await.unwrap();
    let sql = "SELECT c1 \
               FROM (select c1 from aggregate_test_100) AS a \
               JOIN\
               (select c1 as c2 from aggregate_test_100) AS b \
               ON c1=c2\
               ";

    let dataframe = ctx.sql(sql).await.unwrap();
    let physical_plan = dataframe.create_physical_plan().await.unwrap();

    let normalizer = ExplainNormalizer::new();
    let actual = format!("{}", displayable(physical_plan.as_ref()).indent(true))
        .trim()
        .lines()
        // normalize paths
        .map(|s| normalizer.normalize(s))
        .collect::<Vec<_>>()
        .join("\n");

    assert_snapshot!(
        actual,
        @r"
    CoalesceBatchesExec: target_batch_size=4096
      HashJoinExec: mode=Partitioned, join_type=Inner, on=[(c1@0, c2@0)], projection=[c1@0]
        CoalesceBatchesExec: target_batch_size=4096
          RepartitionExec: partitioning=Hash([c1@0], 9000), input_partitions=1
            DataSourceExec: file_groups={1 group: [[ARROW_TEST_DATA/csv/aggregate_test_100.csv]]}, projection=[c1], file_type=csv, has_header=true
        CoalesceBatchesExec: target_batch_size=4096
          RepartitionExec: partitioning=Hash([c2@0], 9000), input_partitions=1
            DataSourceExec: file_groups={1 group: [[ARROW_TEST_DATA/csv/aggregate_test_100.csv]]}, projection=[c1@0 as c2], file_type=csv, has_header=true
    "
    );
}

#[tokio::test]
#[cfg_attr(tarpaulin, ignore)]
async fn csv_explain_analyze() {
    // This test uses the execute function to run an actual plan under EXPLAIN ANALYZE
    let ctx = SessionContext::new();
    register_aggregate_csv_by_sql(&ctx).await;
    let sql = "EXPLAIN ANALYZE SELECT count(*), c1 FROM aggregate_test_100 group by c1";
    let actual = execute_to_batches(&ctx, sql).await;
    let formatted = arrow::util::pretty::pretty_format_batches(&actual)
        .unwrap()
        .to_string();

    // Only test basic plumbing and try to avoid having to change too
    // many things. explain_analyze_baseline_metrics covers the values
    // in greater depth
    let needle = "ProjectionExec: expr=[count(Int64(1))@1 as count(*), c1@0 as c1], metrics=[output_rows=5";
    assert_contains!(&formatted, needle);

    let verbose_needle = "Output Rows";
    assert_not_contains!(formatted, verbose_needle);
}

#[tokio::test]
#[cfg_attr(tarpaulin, ignore)]
async fn csv_explain_analyze_order_by() {
    let ctx = SessionContext::new();
    register_aggregate_csv_by_sql(&ctx).await;
    let sql = "EXPLAIN ANALYZE SELECT c1 FROM aggregate_test_100 order by c1";
    let actual = execute_to_batches(&ctx, sql).await;
    let formatted = arrow::util::pretty::pretty_format_batches(&actual)
        .unwrap()
        .to_string();

    // Ensure that the ordering is not optimized away from the plan
    // https://github.com/apache/datafusion/issues/6379
    let needle =
        "SortExec: expr=[c1@0 ASC NULLS LAST], preserve_partitioning=[false], metrics=[output_rows=100, elapsed_compute";
    assert_contains!(&formatted, needle);
}

#[tokio::test]
#[cfg_attr(tarpaulin, ignore)]
async fn parquet_explain_analyze() {
    let ctx = SessionContext::new();
    register_alltypes_parquet(&ctx).await;

    let sql = "EXPLAIN ANALYZE select id, float_col, timestamp_col from alltypes_plain where timestamp_col > to_timestamp('2009-02-01T00:00:00'); ";
    let actual = execute_to_batches(&ctx, sql).await;
    let formatted = arrow::util::pretty::pretty_format_batches(&actual)
        .unwrap()
        .to_string();

    // should contain aggregated stats
    assert_contains!(&formatted, "output_rows=8");
    assert_contains!(
        &formatted,
        "row_groups_pruned_bloom_filter=1 total \u{2192} 1 matched"
    );
    assert_contains!(
        &formatted,
        "row_groups_pruned_statistics=1 total \u{2192} 1 matched"
    );
    assert_contains!(&formatted, "scan_efficiency_ratio=14%");

    // The order of metrics is expected to be the same as the actual pruning order
    // (file-> row-group -> page)
    let i_file = formatted.find("files_ranges_pruned_statistics").unwrap();
    let i_rowgroup_stat = formatted.find("row_groups_pruned_statistics").unwrap();
    let i_rowgroup_bloomfilter =
        formatted.find("row_groups_pruned_bloom_filter").unwrap();
    let i_page = formatted.find("page_index_rows_pruned").unwrap();

    assert!(
        (i_file < i_rowgroup_stat)
            && (i_rowgroup_stat < i_rowgroup_bloomfilter)
            && (i_rowgroup_bloomfilter < i_page),
            "The parquet pruning metrics should be displayed in an order of: file range -> row group statistics -> row group bloom filter -> page index."
    );
}

// This test reproduces the behavior described in
// https://github.com/apache/datafusion/issues/16684 where projection
// pushdown with recursive CTEs could fail to remove unused columns
// (e.g. nested/recursive expansion causing full schema to be scanned).
// Keeping this test ensures we don't regress that behavior.
#[tokio::test]
#[cfg_attr(tarpaulin, ignore)]
async fn parquet_recursive_projection_pushdown() -> Result<()> {
    use parquet::arrow::arrow_writer::ArrowWriter;
    use parquet::file::properties::WriterProperties;

    let temp_dir = TempDir::new().unwrap();
    let parquet_path = temp_dir.path().join("hierarchy.parquet");

    let ids = Int64Array::from(vec![1, 2, 3, 4, 5, 6, 7, 8, 9, 10]);
    let parent_ids = Int64Array::from(vec![0, 1, 1, 2, 2, 3, 4, 5, 6, 7]);
    let values = Int64Array::from(vec![10, 20, 30, 40, 50, 60, 70, 80, 90, 100]);

    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("parent_id", DataType::Int64, true),
        Field::new("value", DataType::Int64, false),
    ]));

    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![Arc::new(ids), Arc::new(parent_ids), Arc::new(values)],
    )
    .unwrap();

    let file = File::create(&parquet_path).unwrap();
    let props = WriterProperties::builder().build();
    let mut writer = ArrowWriter::try_new(file, schema, Some(props)).unwrap();
    writer.write(&batch).unwrap();
    writer.close().unwrap();

    let ctx = SessionContext::new();
    ctx.register_parquet(
        "hierarchy",
        parquet_path.to_str().unwrap(),
        ParquetReadOptions::default(),
    )
    .await?;

    let sql = r#"
        WITH RECURSIVE number_series AS (
            SELECT id, 1 as level
            FROM hierarchy
            WHERE id = 1

            UNION ALL

            SELECT ns.id + 1, ns.level + 1
            FROM number_series ns
            WHERE ns.id < 10
        )
        SELECT * FROM number_series ORDER BY id
    "#;

    let dataframe = ctx.sql(sql).await?;
    let physical_plan = dataframe.create_physical_plan().await?;

    let normalizer = ExplainNormalizer::new();
    let mut actual = format!("{}", displayable(physical_plan.as_ref()).indent(true))
        .trim()
        .lines()
        .map(|line| normalizer.normalize(line))
        .collect::<Vec<_>>()
        .join("\n");

    fn replace_path_variants(actual: &mut String, path: &str) {
        let mut candidates = vec![path.to_string()];

        let trimmed = path.trim_start_matches(std::path::MAIN_SEPARATOR);
        if trimmed != path {
            candidates.push(trimmed.to_string());
        }

        let forward_slash = path.replace('\\', "/");
        if forward_slash != path {
            candidates.push(forward_slash.clone());

            let trimmed_forward = forward_slash.trim_start_matches('/');
            if trimmed_forward != forward_slash {
                candidates.push(trimmed_forward.to_string());
            }
        }

        for candidate in candidates {
            *actual = actual.replace(&candidate, "TMP_DIR");
        }
    }

    let temp_dir_path = temp_dir.path();
    let fs_path = temp_dir_path.to_string_lossy().to_string();
    replace_path_variants(&mut actual, &fs_path);

    if let Ok(url_path) = Path::from_filesystem_path(temp_dir_path) {
        replace_path_variants(&mut actual, url_path.as_ref());
    }

    assert_snapshot!(
        actual,
        @r"
    SortExec: expr=[id@0 ASC NULLS LAST], preserve_partitioning=[false]
      RecursiveQueryExec: name=number_series, is_distinct=false
        CoalescePartitionsExec
          ProjectionExec: expr=[id@0 as id, 1 as level]
            FilterExec: id@0 = 1
              RepartitionExec: partitioning=RoundRobinBatch(NUM_CORES), input_partitions=1
                DataSourceExec: file_groups={1 group: [[TMP_DIR/hierarchy.parquet]]}, projection=[id], file_type=parquet, predicate=id@0 = 1, pruning_predicate=id_null_count@2 != row_count@3 AND id_min@0 <= 1 AND 1 <= id_max@1, required_guarantees=[id in (1)]
        CoalescePartitionsExec
          ProjectionExec: expr=[id@0 + 1 as ns.id + Int64(1), level@1 + 1 as ns.level + Int64(1)]
            FilterExec: id@0 < 10
              RepartitionExec: partitioning=RoundRobinBatch(NUM_CORES), input_partitions=1
                WorkTableExec: name=number_series
    "
    );

    Ok(())
}

#[tokio::test]
#[cfg_attr(tarpaulin, ignore)]
async fn parquet_explain_analyze_verbose() {
    let ctx = SessionContext::new();
    register_alltypes_parquet(&ctx).await;

    let sql = "EXPLAIN ANALYZE VERBOSE select id, float_col, timestamp_col from alltypes_plain where timestamp_col > to_timestamp('2009-02-01T00:00:00'); ";
    let actual = execute_to_batches(&ctx, sql).await;
    let formatted = arrow::util::pretty::pretty_format_batches(&actual)
        .unwrap()
        .to_string();

    // should contain the raw per file stats (with the label)
    assert_contains!(&formatted, "row_groups_pruned_bloom_filter{partition=0");
    assert_contains!(&formatted, "row_groups_pruned_statistics{partition=0");
}

#[tokio::test]
#[cfg_attr(tarpaulin, ignore)]
async fn csv_explain_analyze_verbose() {
    // This test uses the execute function to run an actual plan under EXPLAIN VERBOSE ANALYZE
    let ctx = SessionContext::new();
    register_aggregate_csv_by_sql(&ctx).await;
    let sql =
        "EXPLAIN ANALYZE VERBOSE SELECT count(*), c1 FROM aggregate_test_100 group by c1";
    let actual = execute_to_batches(&ctx, sql).await;
    let formatted = arrow::util::pretty::pretty_format_batches(&actual)
        .unwrap()
        .to_string();

    let verbose_needle = "Output Rows";
    assert_contains!(formatted, verbose_needle);
}

#[tokio::test]
async fn explain_logical_plan_only() {
    let mut config = ConfigOptions::new();
    config.explain.logical_plan_only = true;
    let ctx = SessionContext::new_with_config(config.into());
    let sql = "EXPLAIN select count(*) from (values ('a', 1, 100), ('a', 2, 150)) as t (c1,c2,c3)";
    let actual = execute(&ctx, sql).await;
    let actual = normalize_vec_for_explain(actual);
    let actual = actual.into_iter().map(|r| r.join("\n")).collect::<String>();

    assert_snapshot!(
        actual,
        @r#"
    logical_plan
    Projection: count(Int64(1)) AS count(*)
      Aggregate: groupBy=[[]], aggr=[[count(Int64(1))]]
        SubqueryAlias: t
          Projection:
            Values: (Utf8("a"), Int64(1), Int64(100)), (Utf8("a"), Int64(2), Int64(150))
    "#
    );
}

#[tokio::test]
async fn explain_physical_plan_only() {
    let mut config = ConfigOptions::new();
    config.explain.physical_plan_only = true;
    let ctx = SessionContext::new_with_config(config.into());
    let sql = "EXPLAIN select count(*) from (values ('a', 1, 100), ('a', 2, 150)) as t (c1,c2,c3)";
    let actual = execute(&ctx, sql).await;
    let actual = normalize_vec_for_explain(actual);
    let actual = actual.into_iter().map(|r| r.join("\n")).collect::<String>();

    assert_snapshot!(
        actual,
        @r###"
    physical_plan
    ProjectionExec: expr=[2 as count(*)]
      PlaceholderRowExec
    "###
    );
}

#[tokio::test]
async fn csv_explain_analyze_with_statistics() {
    let mut config = ConfigOptions::new();
    config.explain.physical_plan_only = true;
    config.explain.show_statistics = true;
    let ctx = SessionContext::new_with_config(config.into());
    register_aggregate_csv_by_sql(&ctx).await;

    let sql = "EXPLAIN ANALYZE SELECT c1 FROM aggregate_test_100";
    let actual = execute_to_batches(&ctx, sql).await;
    let formatted = arrow::util::pretty::pretty_format_batches(&actual)
        .unwrap()
        .to_string();

    // should contain scan statistics
    assert_contains!(
        &formatted,
        ", statistics=[Rows=Absent, Bytes=Absent, [(Col[0]:)]]"
    );
}

#[tokio::test]
async fn nested_loop_join_selectivity() {
    for (join_type, expected_selectivity) in [
        ("INNER", "1% (1/100)"),
        ("LEFT", "10% (10/100)"),
        ("RIGHT", "10% (10/100)"),
        // 1 match + 9 left + 9 right = 19
        ("FULL", "19% (19/100)"),
    ] {
        let ctx = SessionContext::new();
        let sql = format!(
            "EXPLAIN ANALYZE SELECT * \
                FROM generate_series(1, 10) as t1(a) \
                {join_type} JOIN generate_series(1, 10) as t2(b) \
                ON (t1.a + t2.b) = 20"
        );

        let actual = execute_to_batches(&ctx, sql.as_str()).await;
        let formatted = arrow::util::pretty::pretty_format_batches(&actual)
            .unwrap()
            .to_string();

        assert_metrics!(
            &formatted,
            "NestedLoopJoinExec",
            &format!("selectivity={expected_selectivity}")
        );
    }
}

#[tokio::test]
async fn explain_analyze_hash_join() {
    let sql = "EXPLAIN ANALYZE \
            SELECT * \
            FROM generate_series(10) as t1(a) \
            JOIN generate_series(20) as t2(b) \
            ON t1.a=t2.b";

    for (level, needle, should_contain) in [
        (ExplainAnalyzeLevel::Summary, "probe_hit_rate", true),
        (ExplainAnalyzeLevel::Summary, "avg_fanout", true),
    ] {
        let plan = collect_plan(sql, level).await;
        assert_eq!(
            plan.contains(needle),
            should_contain,
            "plan for level {level:?} unexpected content: {plan}"
        );
    }
}
