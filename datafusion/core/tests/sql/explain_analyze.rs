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
use datafusion::{
    config::{OPT_EXPLAIN_LOGICAL_PLAN_ONLY, OPT_EXPLAIN_PHYSICAL_PLAN_ONLY},
    physical_plan::display::DisplayableExecutionPlan,
};

#[tokio::test]
async fn explain_analyze_baseline_metrics() {
    // This test uses the execute function to run an actual plan under EXPLAIN ANALYZE
    // and then validate the presence of baseline metrics for supported operators
    let config = SessionConfig::new().with_target_partitions(3);
    let ctx = SessionContext::with_config(config);
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
    println!("running query: {}", sql);
    let plan = ctx.create_logical_plan(sql).unwrap();
    let plan = ctx.optimize(&plan).unwrap();
    let physical_plan = ctx.create_physical_plan(&plan).await.unwrap();
    let task_ctx = ctx.task_ctx();
    let results = collect(physical_plan.clone(), task_ctx).await.unwrap();
    let formatted = arrow::util::pretty::pretty_format_batches(&results)
        .unwrap()
        .to_string();
    println!("Query Output:\n\n{}", formatted);

    assert_metrics!(
        &formatted,
        "AggregateExec: mode=Partial, gby=[]",
        "metrics=[output_rows=3, elapsed_compute="
    );
    assert_metrics!(
        &formatted,
        "AggregateExec: mode=FinalPartitioned, gby=[c1@0 as c1]",
        "metrics=[output_rows=5, elapsed_compute="
    );
    assert_metrics!(
        &formatted,
        "SortExec: [c1@0 ASC NULLS LAST]",
        "metrics=[output_rows=5, elapsed_compute="
    );
    assert_metrics!(
        &formatted,
        "FilterExec: c13@1 != C2GT5KVyOPZpgKVl110TyZO0NcJ434",
        "metrics=[output_rows=99, elapsed_compute="
    );
    assert_metrics!(
        &formatted,
        "GlobalLimitExec: skip=0, fetch=3, ",
        "metrics=[output_rows=1, elapsed_compute="
    );
    assert_metrics!(
        &formatted,
        "LocalLimitExec: fetch=3",
        "metrics=[output_rows=3, elapsed_compute="
    );
    assert_metrics!(
        &formatted,
        "ProjectionExec: expr=[COUNT(UInt8(1))",
        "metrics=[output_rows=1, elapsed_compute="
    );
    assert_metrics!(
        &formatted,
        "CoalesceBatchesExec: target_batch_size=4096",
        "metrics=[output_rows=5, elapsed_compute"
    );
    assert_metrics!(
        &formatted,
        "CoalescePartitionsExec",
        "metrics=[output_rows=5, elapsed_compute="
    );
    assert_metrics!(
        &formatted,
        "UnionExec",
        "metrics=[output_rows=3, elapsed_compute="
    );
    assert_metrics!(
        &formatted,
        "WindowAggExec",
        "metrics=[output_rows=1, elapsed_compute="
    );

    fn expected_to_have_metrics(plan: &dyn ExecutionPlan) -> bool {
        use datafusion::physical_plan;
        use datafusion::physical_plan::sorts;

        plan.as_any().downcast_ref::<sorts::sort::SortExec>().is_some()
            || plan.as_any().downcast_ref::<physical_plan::aggregates::AggregateExec>().is_some()
            // CoalescePartitionsExec doesn't do any work so is not included
            || plan.as_any().downcast_ref::<physical_plan::filter::FilterExec>().is_some()
            || plan.as_any().downcast_ref::<physical_plan::limit::GlobalLimitExec>().is_some()
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

        fn pre_visit(
            &mut self,
            plan: &dyn ExecutionPlan,
        ) -> std::result::Result<bool, Self::Error> {
            if !expected_to_have_metrics(plan) {
                return Ok(true);
            }
            let metrics = plan.metrics().unwrap().aggregate_by_partition();

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
                    assert!(ts.value().unwrap().timestamp_nanos() > 0);
                }
                MetricValue::EndTimestamp(ts) => {
                    saw_end = true;
                    assert!(ts.value().unwrap().timestamp_nanos() > 0);
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

#[tokio::test]
async fn csv_explain_plans() {
    // This test verify the look of each plan in its full cycle plan creation

    let ctx = SessionContext::new();
    register_aggregate_csv_by_sql(&ctx).await;
    let sql = "EXPLAIN SELECT c1 FROM aggregate_test_100 where c2 > 10";

    // Logical plan
    // Create plan
    let msg = format!("Creating logical plan for '{}'", sql);
    let plan = ctx.create_logical_plan(sql).expect(&msg);
    let logical_schema = plan.schema();
    //
    println!("SQL: {}", sql);
    //
    // Verify schema
    let expected = vec![
        "Explain [plan_type:Utf8, plan:Utf8]",
        "  Projection: #aggregate_test_100.c1 [c1:Utf8]",
        "    Filter: #aggregate_test_100.c2 > Int64(10) [c1:Utf8, c2:Int8, c3:Int16, c4:Int16, c5:Int32, c6:Int64, c7:Int16, c8:Int32, c9:UInt32, c10:UInt64, c11:Float32, c12:Float64, c13:Utf8]",
        "      TableScan: aggregate_test_100 [c1:Utf8, c2:Int8, c3:Int16, c4:Int16, c5:Int32, c6:Int64, c7:Int16, c8:Int32, c9:UInt32, c10:UInt64, c11:Float32, c12:Float64, c13:Utf8]",
    ];
    let formatted = plan.display_indent_schema().to_string();
    let actual: Vec<&str> = formatted.trim().lines().collect();
    assert_eq!(
        expected, actual,
        "\n\nexpected:\n\n{:#?}\nactual:\n\n{:#?}\n\n",
        expected, actual
    );
    //
    // Verify the text format of the plan
    let expected = vec![
        "Explain",
        "  Projection: #aggregate_test_100.c1",
        "    Filter: #aggregate_test_100.c2 > Int64(10)",
        "      TableScan: aggregate_test_100",
    ];
    let formatted = plan.display_indent().to_string();
    let actual: Vec<&str> = formatted.trim().lines().collect();
    assert_eq!(
        expected, actual,
        "\n\nexpected:\n\n{:#?}\nactual:\n\n{:#?}\n\n",
        expected, actual
    );
    //
    // verify the grahviz format of the plan
    let expected = vec![
        "// Begin DataFusion GraphViz Plan (see https://graphviz.org)",
        "digraph {",
        "  subgraph cluster_1",
        "  {",
        "    graph[label=\"LogicalPlan\"]",
        "    2[shape=box label=\"Explain\"]",
        "    3[shape=box label=\"Projection: #aggregate_test_100.c1\"]",
        "    2 -> 3 [arrowhead=none, arrowtail=normal, dir=back]",
        "    4[shape=box label=\"Filter: #aggregate_test_100.c2 > Int64(10)\"]",
        "    3 -> 4 [arrowhead=none, arrowtail=normal, dir=back]",
        "    5[shape=box label=\"TableScan: aggregate_test_100\"]",
        "    4 -> 5 [arrowhead=none, arrowtail=normal, dir=back]",
        "  }",
        "  subgraph cluster_6",
        "  {",
        "    graph[label=\"Detailed LogicalPlan\"]",
        "    7[shape=box label=\"Explain\\nSchema: [plan_type:Utf8, plan:Utf8]\"]",
        "    8[shape=box label=\"Projection: #aggregate_test_100.c1\\nSchema: [c1:Utf8]\"]",
        "    7 -> 8 [arrowhead=none, arrowtail=normal, dir=back]",
        "    9[shape=box label=\"Filter: #aggregate_test_100.c2 > Int64(10)\\nSchema: [c1:Utf8, c2:Int8, c3:Int16, c4:Int16, c5:Int32, c6:Int64, c7:Int16, c8:Int32, c9:UInt32, c10:UInt64, c11:Float32, c12:Float64, c13:Utf8]\"]",
        "    8 -> 9 [arrowhead=none, arrowtail=normal, dir=back]",
        "    10[shape=box label=\"TableScan: aggregate_test_100\\nSchema: [c1:Utf8, c2:Int8, c3:Int16, c4:Int16, c5:Int32, c6:Int64, c7:Int16, c8:Int32, c9:UInt32, c10:UInt64, c11:Float32, c12:Float64, c13:Utf8]\"]",
        "    9 -> 10 [arrowhead=none, arrowtail=normal, dir=back]",
        "  }",
        "}",
        "// End DataFusion GraphViz Plan",
    ];
    let formatted = plan.display_graphviz().to_string();
    let actual: Vec<&str> = formatted.trim().lines().collect();
    assert_eq!(
        expected, actual,
        "\n\nexpected:\n\n{:#?}\nactual:\n\n{:#?}\n\n",
        expected, actual
    );

    // Optimized logical plan
    //
    let msg = format!("Optimizing logical plan for '{}': {:?}", sql, plan);
    let plan = ctx.optimize(&plan).expect(&msg);
    let optimized_logical_schema = plan.schema();
    // Both schema has to be the same
    assert_eq!(logical_schema.as_ref(), optimized_logical_schema.as_ref());
    //
    // Verify schema
    let expected = vec![
        "Explain [plan_type:Utf8, plan:Utf8]",
        "  Projection: #aggregate_test_100.c1 [c1:Utf8]",
        "    Filter: #aggregate_test_100.c2 > Int8(10) [c1:Utf8, c2:Int8]",
        "      TableScan: aggregate_test_100 projection=[c1, c2], partial_filters=[#aggregate_test_100.c2 > Int8(10)] [c1:Utf8, c2:Int8]",
    ];
    let formatted = plan.display_indent_schema().to_string();
    let actual: Vec<&str> = formatted.trim().lines().collect();
    assert_eq!(
        expected, actual,
        "\n\nexpected:\n\n{:#?}\nactual:\n\n{:#?}\n\n",
        expected, actual
    );
    //
    // Verify the text format of the plan
    let expected = vec![
        "Explain",
        "  Projection: #aggregate_test_100.c1",
        "    Filter: #aggregate_test_100.c2 > Int8(10)",
        "      TableScan: aggregate_test_100 projection=[c1, c2], partial_filters=[#aggregate_test_100.c2 > Int8(10)]",
    ];
    let formatted = plan.display_indent().to_string();
    let actual: Vec<&str> = formatted.trim().lines().collect();
    assert_eq!(
        expected, actual,
        "\n\nexpected:\n\n{:#?}\nactual:\n\n{:#?}\n\n",
        expected, actual
    );
    //
    // verify the grahviz format of the plan
    let expected = vec![
        "// Begin DataFusion GraphViz Plan (see https://graphviz.org)",
        "digraph {",
        "  subgraph cluster_1",
        "  {",
        "    graph[label=\"LogicalPlan\"]",
        "    2[shape=box label=\"Explain\"]",
        "    3[shape=box label=\"Projection: #aggregate_test_100.c1\"]",
        "    2 -> 3 [arrowhead=none, arrowtail=normal, dir=back]",
        "    4[shape=box label=\"Filter: #aggregate_test_100.c2 > Int8(10)\"]",
        "    3 -> 4 [arrowhead=none, arrowtail=normal, dir=back]",
        "    5[shape=box label=\"TableScan: aggregate_test_100 projection=[c1, c2], partial_filters=[#aggregate_test_100.c2 > Int8(10)]\"]",
        "    4 -> 5 [arrowhead=none, arrowtail=normal, dir=back]",
        "  }",
        "  subgraph cluster_6",
        "  {",
        "    graph[label=\"Detailed LogicalPlan\"]",
        "    7[shape=box label=\"Explain\\nSchema: [plan_type:Utf8, plan:Utf8]\"]",
        "    8[shape=box label=\"Projection: #aggregate_test_100.c1\\nSchema: [c1:Utf8]\"]",
        "    7 -> 8 [arrowhead=none, arrowtail=normal, dir=back]",
        "    9[shape=box label=\"Filter: #aggregate_test_100.c2 > Int8(10)\\nSchema: [c1:Utf8, c2:Int8]\"]",
        "    8 -> 9 [arrowhead=none, arrowtail=normal, dir=back]",
        "    10[shape=box label=\"TableScan: aggregate_test_100 projection=[c1, c2], partial_filters=[#aggregate_test_100.c2 > Int8(10)]\\nSchema: [c1:Utf8, c2:Int8]\"]",
        "    9 -> 10 [arrowhead=none, arrowtail=normal, dir=back]",
        "  }",
        "}",
        "// End DataFusion GraphViz Plan",
    ];
    let formatted = plan.display_graphviz().to_string();
    let actual: Vec<&str> = formatted.trim().lines().collect();
    assert_eq!(
        expected, actual,
        "\n\nexpected:\n\n{:#?}\nactual:\n\n{:#?}\n\n",
        expected, actual
    );

    // Physical plan
    // Create plan
    let msg = format!("Creating physical plan for '{}': {:?}", sql, plan);
    let plan = ctx.create_physical_plan(&plan).await.expect(&msg);
    //
    // Execute plan
    let msg = format!("Executing physical plan for '{}': {:?}", sql, plan);
    let task_ctx = ctx.task_ctx();
    let results = collect(plan, task_ctx).await.expect(&msg);
    let actual = result_vec(&results);
    // flatten to a single string
    let actual = actual.into_iter().map(|r| r.join("\t")).collect::<String>();
    // Since the plan contains path that are environmentally dependant (e.g. full path of the test file), only verify important content
    assert_contains!(&actual, "logical_plan");
    assert_contains!(&actual, "Projection: #aggregate_test_100.c1");
    assert_contains!(actual, "Filter: #aggregate_test_100.c2 > Int8(10)");
}

#[tokio::test]
async fn csv_explain_verbose() {
    let ctx = SessionContext::new();
    register_aggregate_csv_by_sql(&ctx).await;
    let sql = "EXPLAIN VERBOSE SELECT c1 FROM aggregate_test_100 where c2 > 10";
    let actual = execute(&ctx, sql).await;

    // flatten to a single string
    let actual = actual.into_iter().map(|r| r.join("\t")).collect::<String>();

    // Don't actually test the contents of the debuging output (as
    // that may change and keeping this test updated will be a
    // pain). Instead just check for a few key pieces.
    assert_contains!(&actual, "logical_plan");
    assert_contains!(&actual, "physical_plan");
    assert_contains!(&actual, "#aggregate_test_100.c2 > Int64(10)");

    // ensure the "same text as above" optimization is working
    assert_contains!(actual, "SAME TEXT AS ABOVE");
}

#[tokio::test]
async fn csv_explain_inlist_verbose() {
    let ctx = SessionContext::new();
    register_aggregate_csv_by_sql(&ctx).await;
    let sql = "EXPLAIN VERBOSE SELECT c1 FROM aggregate_test_100 where c2 in (1,2,4)";
    let actual = execute(&ctx, sql).await;

    // Optimized by PreCastLitInComparisonExpressions rule
    // the data type of c2 is INT8, the type of `1,2,4` is INT64.
    // the value of `1,2,4` will be casted to INT8 and pre-calculated

    // flatten to a single string
    let actual = actual.into_iter().map(|r| r.join("\t")).collect::<String>();

    // before optimization (Int64 literals)
    assert_contains!(
        &actual,
        "#aggregate_test_100.c2 IN ([Int64(1), Int64(2), Int64(4)])"
    );
    // after optimization (casted to Int8)
    assert_contains!(
        &actual,
        "#aggregate_test_100.c2 IN ([Int8(1), Int8(2), Int8(4)])"
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
    let msg = format!("Creating logical plan for '{}'", sql);
    let plan = ctx.create_logical_plan(sql).expect(&msg);
    let logical_schema = plan.schema();
    //
    println!("SQL: {}", sql);

    //
    // Verify schema
    let expected = vec![
        "Explain [plan_type:Utf8, plan:Utf8]",
        "  Projection: #aggregate_test_100.c1 [c1:Utf8]",
        "    Filter: #aggregate_test_100.c2 > Int64(10) [c1:Utf8, c2:Int8, c3:Int16, c4:Int16, c5:Int32, c6:Int64, c7:Int16, c8:Int32, c9:UInt32, c10:UInt64, c11:Float32, c12:Float64, c13:Utf8]",
        "      TableScan: aggregate_test_100 [c1:Utf8, c2:Int8, c3:Int16, c4:Int16, c5:Int32, c6:Int64, c7:Int16, c8:Int32, c9:UInt32, c10:UInt64, c11:Float32, c12:Float64, c13:Utf8]",
    ];
    let formatted = plan.display_indent_schema().to_string();
    let actual: Vec<&str> = formatted.trim().lines().collect();
    assert_eq!(
        expected, actual,
        "\n\nexpected:\n\n{:#?}\nactual:\n\n{:#?}\n\n",
        expected, actual
    );
    //
    // Verify the text format of the plan
    let expected = vec![
        "Explain",
        "  Projection: #aggregate_test_100.c1",
        "    Filter: #aggregate_test_100.c2 > Int64(10)",
        "      TableScan: aggregate_test_100",
    ];
    let formatted = plan.display_indent().to_string();
    let actual: Vec<&str> = formatted.trim().lines().collect();
    assert_eq!(
        expected, actual,
        "\n\nexpected:\n\n{:#?}\nactual:\n\n{:#?}\n\n",
        expected, actual
    );
    //
    // verify the grahviz format of the plan
    let expected = vec![
        "// Begin DataFusion GraphViz Plan (see https://graphviz.org)",
        "digraph {",
        "  subgraph cluster_1",
        "  {",
        "    graph[label=\"LogicalPlan\"]",
        "    2[shape=box label=\"Explain\"]",
        "    3[shape=box label=\"Projection: #aggregate_test_100.c1\"]",
        "    2 -> 3 [arrowhead=none, arrowtail=normal, dir=back]",
        "    4[shape=box label=\"Filter: #aggregate_test_100.c2 > Int64(10)\"]",
        "    3 -> 4 [arrowhead=none, arrowtail=normal, dir=back]",
        "    5[shape=box label=\"TableScan: aggregate_test_100\"]",
        "    4 -> 5 [arrowhead=none, arrowtail=normal, dir=back]",
        "  }",
        "  subgraph cluster_6",
        "  {",
        "    graph[label=\"Detailed LogicalPlan\"]",
        "    7[shape=box label=\"Explain\\nSchema: [plan_type:Utf8, plan:Utf8]\"]",
        "    8[shape=box label=\"Projection: #aggregate_test_100.c1\\nSchema: [c1:Utf8]\"]",
        "    7 -> 8 [arrowhead=none, arrowtail=normal, dir=back]",
        "    9[shape=box label=\"Filter: #aggregate_test_100.c2 > Int64(10)\\nSchema: [c1:Utf8, c2:Int8, c3:Int16, c4:Int16, c5:Int32, c6:Int64, c7:Int16, c8:Int32, c9:UInt32, c10:UInt64, c11:Float32, c12:Float64, c13:Utf8]\"]",
        "    8 -> 9 [arrowhead=none, arrowtail=normal, dir=back]",
        "    10[shape=box label=\"TableScan: aggregate_test_100\\nSchema: [c1:Utf8, c2:Int8, c3:Int16, c4:Int16, c5:Int32, c6:Int64, c7:Int16, c8:Int32, c9:UInt32, c10:UInt64, c11:Float32, c12:Float64, c13:Utf8]\"]",
        "    9 -> 10 [arrowhead=none, arrowtail=normal, dir=back]",
        "  }",
        "}",
        "// End DataFusion GraphViz Plan",
    ];
    let formatted = plan.display_graphviz().to_string();
    let actual: Vec<&str> = formatted.trim().lines().collect();
    assert_eq!(
        expected, actual,
        "\n\nexpected:\n\n{:#?}\nactual:\n\n{:#?}\n\n",
        expected, actual
    );

    // Optimized logical plan
    //
    let msg = format!("Optimizing logical plan for '{}': {:?}", sql, plan);
    let plan = ctx.optimize(&plan).expect(&msg);
    let optimized_logical_schema = plan.schema();
    // Both schema has to be the same
    assert_eq!(logical_schema.as_ref(), optimized_logical_schema.as_ref());
    //
    // Verify schema
    let expected = vec![
        "Explain [plan_type:Utf8, plan:Utf8]",
        "  Projection: #aggregate_test_100.c1 [c1:Utf8]",
        "    Filter: #aggregate_test_100.c2 > Int8(10) [c1:Utf8, c2:Int8]",
        "      TableScan: aggregate_test_100 projection=[c1, c2], partial_filters=[#aggregate_test_100.c2 > Int8(10)] [c1:Utf8, c2:Int8]",
    ];
    let formatted = plan.display_indent_schema().to_string();
    let actual: Vec<&str> = formatted.trim().lines().collect();
    assert_eq!(
        expected, actual,
        "\n\nexpected:\n\n{:#?}\nactual:\n\n{:#?}\n\n",
        expected, actual
    );
    //
    // Verify the text format of the plan
    let expected = vec![
        "Explain",
        "  Projection: #aggregate_test_100.c1",
        "    Filter: #aggregate_test_100.c2 > Int8(10)",
        "      TableScan: aggregate_test_100 projection=[c1, c2], partial_filters=[#aggregate_test_100.c2 > Int8(10)]",
    ];
    let formatted = plan.display_indent().to_string();
    let actual: Vec<&str> = formatted.trim().lines().collect();
    assert_eq!(
        expected, actual,
        "\n\nexpected:\n\n{:#?}\nactual:\n\n{:#?}\n\n",
        expected, actual
    );
    //
    // verify the grahviz format of the plan
    let expected = vec![
        "// Begin DataFusion GraphViz Plan (see https://graphviz.org)",
        "digraph {",
        "  subgraph cluster_1",
        "  {",
        "    graph[label=\"LogicalPlan\"]",
        "    2[shape=box label=\"Explain\"]",
        "    3[shape=box label=\"Projection: #aggregate_test_100.c1\"]",
        "    2 -> 3 [arrowhead=none, arrowtail=normal, dir=back]",
        "    4[shape=box label=\"Filter: #aggregate_test_100.c2 > Int8(10)\"]",
        "    3 -> 4 [arrowhead=none, arrowtail=normal, dir=back]",
        "    5[shape=box label=\"TableScan: aggregate_test_100 projection=[c1, c2], partial_filters=[#aggregate_test_100.c2 > Int8(10)]\"]",
        "    4 -> 5 [arrowhead=none, arrowtail=normal, dir=back]",
        "  }",
        "  subgraph cluster_6",
        "  {",
        "    graph[label=\"Detailed LogicalPlan\"]",
        "    7[shape=box label=\"Explain\\nSchema: [plan_type:Utf8, plan:Utf8]\"]",
        "    8[shape=box label=\"Projection: #aggregate_test_100.c1\\nSchema: [c1:Utf8]\"]",
        "    7 -> 8 [arrowhead=none, arrowtail=normal, dir=back]",
        "    9[shape=box label=\"Filter: #aggregate_test_100.c2 > Int8(10)\\nSchema: [c1:Utf8, c2:Int8]\"]",
        "    8 -> 9 [arrowhead=none, arrowtail=normal, dir=back]",
        "    10[shape=box label=\"TableScan: aggregate_test_100 projection=[c1, c2], partial_filters=[#aggregate_test_100.c2 > Int8(10)]\\nSchema: [c1:Utf8, c2:Int8]\"]",
        "    9 -> 10 [arrowhead=none, arrowtail=normal, dir=back]",
        "  }",
        "}",
        "// End DataFusion GraphViz Plan",
    ];
    let formatted = plan.display_graphviz().to_string();
    let actual: Vec<&str> = formatted.trim().lines().collect();
    assert_eq!(
        expected, actual,
        "\n\nexpected:\n\n{:#?}\nactual:\n\n{:#?}\n\n",
        expected, actual
    );

    // Physical plan
    // Create plan
    let msg = format!("Creating physical plan for '{}': {:?}", sql, plan);
    let plan = ctx.create_physical_plan(&plan).await.expect(&msg);
    //
    // Execute plan
    let msg = format!("Executing physical plan for '{}': {:?}", sql, plan);
    let task_ctx = ctx.task_ctx();
    let results = collect(plan, task_ctx).await.expect(&msg);
    let actual = result_vec(&results);
    // flatten to a single string
    let actual = actual.into_iter().map(|r| r.join("\t")).collect::<String>();
    // Since the plan contains path that are environmentally
    // dependant(e.g. full path of the test file), only verify
    // important content
    assert_contains!(&actual, "logical_plan after projection_push_down");
    assert_contains!(&actual, "physical_plan");
    assert_contains!(&actual, "FilterExec: c2@1 > 10");
    assert_contains!(actual, "ProjectionExec: expr=[c1@0 as c1]");
}

#[tokio::test]
async fn explain_analyze_runs_optimizers() {
    // repro for https://github.com/apache/arrow-datafusion/issues/917
    // where EXPLAIN ANALYZE was not correctly running optiimizer
    let ctx = SessionContext::new();
    register_alltypes_parquet(&ctx).await;

    // This happens as an optimization pass where count(*) can be
    // answered using statistics only.
    let expected = "EmptyExec: produce_one_row=true";

    let sql = "EXPLAIN SELECT count(*) from alltypes_plain";
    let actual = execute_to_batches(&ctx, sql).await;
    let actual = arrow::util::pretty::pretty_format_batches(&actual)
        .unwrap()
        .to_string();
    assert_contains!(actual, expected);

    // EXPLAIN ANALYZE should work the same
    let sql = "EXPLAIN  ANALYZE SELECT count(*) from alltypes_plain";
    let actual = execute_to_batches(&ctx, sql).await;
    let actual = arrow::util::pretty::pretty_format_batches(&actual)
        .unwrap()
        .to_string();
    assert_contains!(actual, expected);
}

#[tokio::test]
async fn tpch_explain_q10() -> Result<()> {
    let ctx = SessionContext::new();

    register_tpch_csv(&ctx, "customer").await?;
    register_tpch_csv(&ctx, "orders").await?;
    register_tpch_csv(&ctx, "lineitem").await?;
    register_tpch_csv(&ctx, "nation").await?;

    let sql = "select
    c_custkey,
    c_name,
    sum(l_extendedprice * (1 - l_discount)) as revenue,
    c_acctbal,
    n_name,
    c_address,
    c_phone,
    c_comment
from
    customer,
    orders,
    lineitem,
    nation
where
        c_custkey = o_custkey
  and l_orderkey = o_orderkey
  and o_orderdate >= date '1993-10-01'
  and o_orderdate < date '1994-01-01'
  and l_returnflag = 'R'
  and c_nationkey = n_nationkey
group by
    c_custkey,
    c_name,
    c_acctbal,
    c_phone,
    n_name,
    c_address,
    c_comment
order by
    revenue desc;";

    let mut plan = ctx.create_logical_plan(sql);
    plan = ctx.optimize(&plan.unwrap());

    let expected = "\
    Sort: #revenue DESC NULLS FIRST\
    \n  Projection: #customer.c_custkey, #customer.c_name, #SUM(lineitem.l_extendedprice * Int64(1) - lineitem.l_discount) AS revenue, #customer.c_acctbal, #nation.n_name, #customer.c_address, #customer.c_phone, #customer.c_comment\
    \n    Aggregate: groupBy=[[#customer.c_custkey, #customer.c_name, #customer.c_acctbal, #customer.c_phone, #nation.n_name, #customer.c_address, #customer.c_comment]], aggr=[[SUM(CAST(#lineitem.l_extendedprice AS Decimal128(38, 4)) * CAST(Decimal128(Some(100),23,2) - CAST(#lineitem.l_discount AS Decimal128(23, 2)) AS Decimal128(38, 4))) AS SUM(lineitem.l_extendedprice * Int64(1) - lineitem.l_discount)]]\
    \n      Inner Join: #customer.c_nationkey = #nation.n_nationkey\
    \n        Inner Join: #orders.o_orderkey = #lineitem.l_orderkey\
    \n          Inner Join: #customer.c_custkey = #orders.o_custkey\
    \n            TableScan: customer projection=[c_custkey, c_name, c_address, c_nationkey, c_phone, c_acctbal, c_comment]\
    \n            Filter: #orders.o_orderdate >= Date32(\"8674\") AND #orders.o_orderdate < Date32(\"8766\")\
    \n              TableScan: orders projection=[o_orderkey, o_custkey, o_orderdate], partial_filters=[#orders.o_orderdate >= Date32(\"8674\"), #orders.o_orderdate < Date32(\"8766\")]\
    \n          Filter: #lineitem.l_returnflag = Utf8(\"R\")\
    \n            TableScan: lineitem projection=[l_orderkey, l_extendedprice, l_discount, l_returnflag], partial_filters=[#lineitem.l_returnflag = Utf8(\"R\")]\
    \n        TableScan: nation projection=[n_nationkey, n_name]";
    assert_eq!(expected, format!("{:?}", plan.unwrap()),);

    Ok(())
}

#[tokio::test]
async fn test_physical_plan_display_indent() {
    // Hard code target_partitions as it appears in the RepartitionExec output
    let config = SessionConfig::new().with_target_partitions(9000);
    let ctx = SessionContext::with_config(config);
    register_aggregate_csv(&ctx).await.unwrap();
    let sql = "SELECT c1, MAX(c12), MIN(c12) as the_min \
               FROM aggregate_test_100 \
               WHERE c12 < 10 \
               GROUP BY c1 \
               ORDER BY the_min DESC \
               LIMIT 10";
    let plan = ctx.create_logical_plan(sql).unwrap();
    let plan = ctx.optimize(&plan).unwrap();

    let physical_plan = ctx.create_physical_plan(&plan).await.unwrap();
    let expected = vec![
        "GlobalLimitExec: skip=0, fetch=10",
        "  SortPreservingMergeExec: [the_min@2 DESC]",
        "    SortExec: [the_min@2 DESC]",
        "      ProjectionExec: expr=[c1@0 as c1, MAX(aggregate_test_100.c12)@1 as MAX(aggregate_test_100.c12), MIN(aggregate_test_100.c12)@2 as the_min]",
        "        AggregateExec: mode=FinalPartitioned, gby=[c1@0 as c1], aggr=[MAX(aggregate_test_100.c12), MIN(aggregate_test_100.c12)]",
        "          CoalesceBatchesExec: target_batch_size=4096",
        "            RepartitionExec: partitioning=Hash([Column { name: \"c1\", index: 0 }], 9000)",
        "              AggregateExec: mode=Partial, gby=[c1@0 as c1], aggr=[MAX(aggregate_test_100.c12), MIN(aggregate_test_100.c12)]",
        "                CoalesceBatchesExec: target_batch_size=4096",
        "                  FilterExec: c12@1 < 10",
        "                    RepartitionExec: partitioning=RoundRobinBatch(9000)",
        "                      CsvExec: files=[ARROW_TEST_DATA/csv/aggregate_test_100.csv], has_header=true, limit=None, projection=[c1, c12]",
    ];

    let normalizer = ExplainNormalizer::new();
    let actual = format!("{}", displayable(physical_plan.as_ref()).indent())
        .trim()
        .lines()
        // normalize paths
        .map(|s| normalizer.normalize(s))
        .collect::<Vec<_>>();
    assert_eq!(
        expected, actual,
        "expected:\n{:#?}\nactual:\n\n{:#?}\n",
        expected, actual
    );
}

#[tokio::test]
async fn test_physical_plan_display_indent_multi_children() {
    // Hard code target_partitions as it appears in the RepartitionExec output
    let config = SessionConfig::new().with_target_partitions(9000);
    let ctx = SessionContext::with_config(config);
    // ensure indenting works for nodes with multiple children
    register_aggregate_csv(&ctx).await.unwrap();
    let sql = "SELECT c1 \
               FROM (select c1 from aggregate_test_100) AS a \
               JOIN\
               (select c1 as c2 from aggregate_test_100) AS b \
               ON c1=c2\
               ";

    let plan = ctx.create_logical_plan(sql).unwrap();
    let plan = ctx.optimize(&plan).unwrap();

    let physical_plan = ctx.create_physical_plan(&plan).await.unwrap();
    let expected = vec![
        "ProjectionExec: expr=[c1@0 as c1]",
        "  CoalesceBatchesExec: target_batch_size=4096",
        "    HashJoinExec: mode=Partitioned, join_type=Inner, on=[(Column { name: \"c1\", index: 0 }, Column { name: \"c2\", index: 0 })]",
        "      CoalesceBatchesExec: target_batch_size=4096",
        "        RepartitionExec: partitioning=Hash([Column { name: \"c1\", index: 0 }], 9000)",
        "          ProjectionExec: expr=[c1@0 as c1]",
        "            ProjectionExec: expr=[c1@0 as c1]",
        "              RepartitionExec: partitioning=RoundRobinBatch(9000)",
        "                CsvExec: files=[ARROW_TEST_DATA/csv/aggregate_test_100.csv], has_header=true, limit=None, projection=[c1]",
        "      CoalesceBatchesExec: target_batch_size=4096",
        "        RepartitionExec: partitioning=Hash([Column { name: \"c2\", index: 0 }], 9000)",
        "          ProjectionExec: expr=[c2@0 as c2]",
        "            ProjectionExec: expr=[c1@0 as c2]",
        "              RepartitionExec: partitioning=RoundRobinBatch(9000)",
        "                CsvExec: files=[ARROW_TEST_DATA/csv/aggregate_test_100.csv], has_header=true, limit=None, projection=[c1]",
    ];

    let normalizer = ExplainNormalizer::new();
    let actual = format!("{}", displayable(physical_plan.as_ref()).indent())
        .trim()
        .lines()
        // normalize paths
        .map(|s| normalizer.normalize(s))
        .collect::<Vec<_>>();

    assert_eq!(
        expected, actual,
        "expected:\n{:#?}\nactual:\n\n{:#?}\n",
        expected, actual
    );
}

#[tokio::test]
#[cfg_attr(tarpaulin, ignore)]
async fn csv_explain() {
    // This test uses the execute function that create full plan cycle: logical, optimized logical, and physical,
    // then execute the physical plan and return the final explain results
    let ctx = SessionContext::new();
    register_aggregate_csv_by_sql(&ctx).await;
    let sql = "EXPLAIN SELECT c1 FROM aggregate_test_100 where c2 > cast(10 as int)";
    let actual = execute(&ctx, sql).await;
    let actual = normalize_vec_for_explain(actual);

    // Note can't use `assert_batches_eq` as the plan needs to be
    // normalized for filenames and number of cores
    let expected = vec![
        vec![
            "logical_plan",
            "Projection: #aggregate_test_100.c1\
             \n  Filter: #aggregate_test_100.c2 > Int8(10)\
             \n    TableScan: aggregate_test_100 projection=[c1, c2], partial_filters=[#aggregate_test_100.c2 > Int8(10)]"
        ],
        vec!["physical_plan",
             "ProjectionExec: expr=[c1@0 as c1]\
              \n  CoalesceBatchesExec: target_batch_size=4096\
              \n    FilterExec: c2@1 > 10\
              \n      RepartitionExec: partitioning=RoundRobinBatch(NUM_CORES)\
              \n        CsvExec: files=[ARROW_TEST_DATA/csv/aggregate_test_100.csv], has_header=true, limit=None, projection=[c1, c2]\
              \n"
        ]];
    assert_eq!(expected, actual);

    // Also, expect same result with lowercase explain
    let sql = "explain SELECT c1 FROM aggregate_test_100 where c2 > 10";
    let actual = execute(&ctx, sql).await;
    let actual = normalize_vec_for_explain(actual);
    assert_eq!(expected, actual);
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
    let needle = "CoalescePartitionsExec, metrics=[output_rows=5, elapsed_compute=";
    assert_contains!(&formatted, needle);

    let verbose_needle = "Output Rows";
    assert_not_contains!(formatted, verbose_needle);
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
    let config = SessionConfig::new().set_bool(OPT_EXPLAIN_LOGICAL_PLAN_ONLY, true);
    let ctx = SessionContext::with_config(config);
    let sql = "EXPLAIN select count(*) from (values ('a', 1, 100), ('a', 2, 150)) as t (c1,c2,c3)";
    let actual = execute(&ctx, sql).await;
    let actual = normalize_vec_for_explain(actual);

    let expected = vec![
        vec![
            "logical_plan", 
            "Projection: #COUNT(UInt8(1))\
            \n  Aggregate: groupBy=[[]], aggr=[[COUNT(UInt8(1))]]\
            \n    Values: (Utf8(\"a\"), Int64(1), Int64(100)), (Utf8(\"a\"), Int64(2), Int64(150))",
        ]];
    assert_eq!(expected, actual);
}

#[tokio::test]
async fn explain_physical_plan_only() {
    let config = SessionConfig::new().set_bool(OPT_EXPLAIN_PHYSICAL_PLAN_ONLY, true);
    let ctx = SessionContext::with_config(config);
    let sql = "EXPLAIN select count(*) from (values ('a', 1, 100), ('a', 2, 150)) as t (c1,c2,c3)";
    let actual = execute(&ctx, sql).await;
    let actual = normalize_vec_for_explain(actual);

    let expected = vec![vec![
        "physical_plan",
        "ProjectionExec: expr=[COUNT(UInt8(1))@0 as COUNT(UInt8(1))]\
        \n  ProjectionExec: expr=[2 as COUNT(UInt8(1))]\
        \n    EmptyExec: produce_one_row=true\
        \n",
    ]];
    assert_eq!(expected, actual);
}
