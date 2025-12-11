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

use std::any::Any;
use std::collections::HashMap;
use std::sync::Arc;

use arrow::datatypes::{DataType, Field, Schema, SchemaRef, TimeUnit};

use datafusion_common::config::ConfigOptions;
use datafusion_common::{plan_err, Result, TableReference};
use datafusion_expr::planner::ExprPlanner;
use datafusion_expr::test::function_stub::sum_udaf;
use datafusion_expr::{AggregateUDF, LogicalPlan, ScalarUDF, TableSource, WindowUDF};
use datafusion_functions_aggregate::average::avg_udaf;
use datafusion_functions_aggregate::count::count_udaf;
use datafusion_functions_aggregate::planner::AggregateFunctionPlanner;
use datafusion_functions_window::planner::WindowFunctionPlanner;
use datafusion_optimizer::analyzer::Analyzer;
use datafusion_optimizer::optimizer::Optimizer;
use datafusion_optimizer::{OptimizerConfig, OptimizerContext, OptimizerRule};
use datafusion_sql::planner::{ContextProvider, SqlToRel};
use datafusion_sql::sqlparser::ast::Statement;
use datafusion_sql::sqlparser::dialect::GenericDialect;
use datafusion_sql::sqlparser::parser::Parser;
use insta::assert_snapshot;

#[cfg(test)]
#[ctor::ctor]
fn init() {
    // enable logging so RUST_LOG works
    let _ = env_logger::try_init();
}

#[test]
fn recursive_cte_with_nested_subquery() -> Result<()> {
    // Covers bailout path in `plan_contains_other_subqueries`, ensuring nested subqueries
    // within recursive CTE branches prevent projection pushdown.
    let sql = r#"
        WITH RECURSIVE numbers(id, level) AS (
            SELECT sub.id, sub.level FROM (
                SELECT col_int32 AS id, 1 AS level FROM test
            ) sub
            UNION ALL
            SELECT t.col_int32, numbers.level + 1
            FROM test t
            JOIN numbers ON t.col_int32 = numbers.id + 1
        )
        SELECT id, level FROM numbers
    "#;

    let plan = test_sql(sql)?;

    assert_snapshot!(
        format!("{plan}"),
        @r#"
        SubqueryAlias: numbers
          Projection: sub.id AS id, sub.level AS level
            RecursiveQuery: is_distinct=false
              Projection: sub.id, sub.level
                SubqueryAlias: sub
                  Projection: test.col_int32 AS id, Int64(1) AS level
                    TableScan: test
              Projection: t.col_int32, numbers.level + Int64(1)
                Inner Join: CAST(t.col_int32 AS Int64) = CAST(numbers.id AS Int64) + Int64(1)
                  SubqueryAlias: t
                    Filter: CAST(test.col_int32 AS Int64) IS NOT NULL
                      TableScan: test
                  Filter: CAST(numbers.id AS Int64) + Int64(1) IS NOT NULL
                    TableScan: numbers
        "#
    );

    Ok(())
}

#[test]
fn case_when() -> Result<()> {
    let sql = "SELECT CASE WHEN col_int32 > 0 THEN 1 ELSE 0 END FROM test";
    let plan = test_sql(sql)?;

    assert_snapshot!(
    format!("{plan}"),
    @r#"
Projection: CASE WHEN test.col_int32 > Int32(0) THEN Int64(1) ELSE Int64(0) END AS CASE WHEN test.col_int32 > Int64(0) THEN Int64(1) ELSE Int64(0) END
  TableScan: test projection=[col_int32]
"#
    );

    let sql = "SELECT CASE WHEN col_uint32 > 0 THEN 1 ELSE 0 END FROM test";
    let plan = test_sql(sql)?;

    assert_snapshot!(
        format!("{plan}"),
    @r#"
    Projection: CASE WHEN test.col_uint32 > UInt32(0) THEN Int64(1) ELSE Int64(0) END AS CASE WHEN test.col_uint32 > Int64(0) THEN Int64(1) ELSE Int64(0) END
      TableScan: test projection=[col_uint32]
    "#
    );
    Ok(())
}

#[test]
fn subquery_filter_with_cast() -> Result<()> {
    // regression test for https://github.com/apache/datafusion/issues/3760
    let sql = "SELECT col_int32 FROM test \
    WHERE col_int32 > (\
      SELECT avg(col_int32) FROM test \
      WHERE col_utf8 BETWEEN '2002-05-08' \
        AND (cast('2002-05-08' as date) + interval '5 days')\
    )";
    let plan = test_sql(sql)?;

    assert_snapshot!(
    format!("{plan}"),
    @r#"
    Projection: test.col_int32
      Inner Join:  Filter: CAST(test.col_int32 AS Float64) > __scalar_sq_1.avg(test.col_int32)
        TableScan: test projection=[col_int32]
        SubqueryAlias: __scalar_sq_1
          Aggregate: groupBy=[[]], aggr=[[avg(CAST(test.col_int32 AS Float64))]]
            Projection: test.col_int32
              Filter: __common_expr_4 >= Date32("2002-05-08") AND __common_expr_4 <= Date32("2002-05-13")
                Projection: CAST(test.col_utf8 AS Date32) AS __common_expr_4, test.col_int32
                  TableScan: test projection=[col_int32, col_utf8]
    "#
    );
    Ok(())
}

#[test]
fn case_when_aggregate() -> Result<()> {
    let sql = "SELECT col_utf8, sum(CASE WHEN col_int32 > 0 THEN 1 ELSE 0 END) AS n FROM test GROUP BY col_utf8";
    let plan = test_sql(sql)?;

    assert_snapshot!(
    format!("{plan}"),
    @r#"
        Projection: test.col_utf8, sum(CASE WHEN test.col_int32 > Int64(0) THEN Int64(1) ELSE Int64(0) END) AS n
          Aggregate: groupBy=[[test.col_utf8]], aggr=[[sum(CASE WHEN test.col_int32 > Int32(0) THEN Int64(1) ELSE Int64(0) END) AS sum(CASE WHEN test.col_int32 > Int64(0) THEN Int64(1) ELSE Int64(0) END)]]
            TableScan: test projection=[col_int32, col_utf8]
        "#
    );
    Ok(())
}

#[test]
fn unsigned_target_type() -> Result<()> {
    let sql = "SELECT col_utf8 FROM test WHERE col_uint32 > 0";
    let plan = test_sql(sql)?;

    assert_snapshot!(
    format!("{plan}"),
    @r#"
    Projection: test.col_utf8
      Filter: test.col_uint32 > UInt32(0)
        TableScan: test projection=[col_uint32, col_utf8]
    "#
    );
    Ok(())
}

#[test]
fn distribute_by() -> Result<()> {
    // regression test for https://github.com/apache/datafusion/issues/3234
    let sql = "SELECT col_int32, col_utf8 FROM test DISTRIBUTE BY (col_utf8)";
    let plan = test_sql(sql)?;

    assert_snapshot!(
    format!("{plan}"),
    @r#"
        Repartition: DistributeBy(test.col_utf8)
          TableScan: test projection=[col_int32, col_utf8]
        "#
    );
    Ok(())
}

#[test]
fn semi_join_with_join_filter() -> Result<()> {
    // regression test for https://github.com/apache/datafusion/issues/2888
    let sql = "SELECT col_utf8 FROM test WHERE EXISTS (\
               SELECT col_utf8 FROM test t2 WHERE test.col_int32 = t2.col_int32 \
               AND test.col_uint32 != t2.col_uint32)";
    let plan = test_sql(sql)?;

    assert_snapshot!(
    format!("{plan}"),
    @r#"
        Projection: test.col_utf8
          LeftSemi Join: test.col_int32 = __correlated_sq_1.col_int32 Filter: test.col_uint32 != __correlated_sq_1.col_uint32
            Filter: test.col_int32 IS NOT NULL
              TableScan: test projection=[col_int32, col_uint32, col_utf8]
            SubqueryAlias: __correlated_sq_1
              SubqueryAlias: t2
                Filter: test.col_int32 IS NOT NULL
                  TableScan: test projection=[col_int32, col_uint32]
        "#
    );
    Ok(())
}

#[test]
fn anti_join_with_join_filter() -> Result<()> {
    // regression test for https://github.com/apache/datafusion/issues/2888
    let sql = "SELECT col_utf8 FROM test WHERE NOT EXISTS (\
               SELECT col_utf8 FROM test t2 WHERE test.col_int32 = t2.col_int32 \
               AND test.col_uint32 != t2.col_uint32)";
    let plan = test_sql(sql)?;

    assert_snapshot!(
    format!("{plan}"),
    @r#"
Projection: test.col_utf8
  LeftAnti Join: test.col_int32 = __correlated_sq_1.col_int32 Filter: test.col_uint32 != __correlated_sq_1.col_uint32
    TableScan: test projection=[col_int32, col_uint32, col_utf8]
    SubqueryAlias: __correlated_sq_1
      SubqueryAlias: t2
        Filter: test.col_int32 IS NOT NULL
          TableScan: test projection=[col_int32, col_uint32]
"#
    );
    Ok(())
}

#[test]
fn where_exists_distinct() -> Result<()> {
    let sql = "SELECT col_int32 FROM test WHERE EXISTS (\
               SELECT DISTINCT col_int32 FROM test t2 WHERE test.col_int32 = t2.col_int32)";
    let plan = test_sql(sql)?;

    assert_snapshot!(
    format!("{plan}"),
    @r#"
LeftSemi Join: test.col_int32 = __correlated_sq_1.col_int32
  Filter: test.col_int32 IS NOT NULL
    TableScan: test projection=[col_int32]
  SubqueryAlias: __correlated_sq_1
    Aggregate: groupBy=[[t2.col_int32]], aggr=[[]]
      SubqueryAlias: t2
        Filter: test.col_int32 IS NOT NULL
          TableScan: test projection=[col_int32]
"#

    );
    Ok(())
}

#[test]
fn intersect() -> Result<()> {
    let sql = "SELECT col_int32, col_utf8 FROM test \
    INTERSECT SELECT col_int32, col_utf8 FROM test \
    INTERSECT SELECT col_int32, col_utf8 FROM test";
    let plan = test_sql(sql)?;

    assert_snapshot!(
    format!("{plan}"),
    @r#"
LeftSemi Join: left.col_int32 = test.col_int32, left.col_utf8 = test.col_utf8
  Aggregate: groupBy=[[left.col_int32, left.col_utf8]], aggr=[[]]
    LeftSemi Join: left.col_int32 = right.col_int32, left.col_utf8 = right.col_utf8
      Aggregate: groupBy=[[left.col_int32, left.col_utf8]], aggr=[[]]
        SubqueryAlias: left
          TableScan: test projection=[col_int32, col_utf8]
      SubqueryAlias: right
        TableScan: test projection=[col_int32, col_utf8]
  TableScan: test projection=[col_int32, col_utf8]
"#
    );
    Ok(())
}

#[test]
fn between_date32_plus_interval() -> Result<()> {
    let sql = "SELECT count(1) FROM test \
    WHERE col_date32 between '1998-03-18' AND cast('1998-03-18' as date) + INTERVAL '90 days'";
    let plan = test_sql(sql)?;

    assert_snapshot!(
    format!("{plan}"),
    @r#"
Aggregate: groupBy=[[]], aggr=[[count(Int64(1))]]
  Projection:
    Filter: test.col_date32 >= Date32("1998-03-18") AND test.col_date32 <= Date32("1998-06-16")
      TableScan: test projection=[col_date32]
"#
    );
    Ok(())
}

#[test]
fn between_date64_plus_interval() -> Result<()> {
    let sql = "SELECT count(1) FROM test \
    WHERE col_date64 between '1998-03-18T00:00:00' AND cast('1998-03-18' as date) + INTERVAL '90 days'";
    let plan = test_sql(sql)?;

    assert_snapshot!(
    format!("{plan}"),
    @r#"
        Aggregate: groupBy=[[]], aggr=[[count(Int64(1))]]
          Projection:
            Filter: test.col_date64 >= Date64("1998-03-18") AND test.col_date64 <= Date64("1998-06-16")
              TableScan: test projection=[col_date64]
        "#
    );
    Ok(())
}

#[test]
fn propagate_empty_relation() {
    let sql = "SELECT test.col_int32 FROM test JOIN ( SELECT col_int32 FROM test WHERE false ) AS ta1 ON test.col_int32 = ta1.col_int32;";
    let plan = test_sql(sql).unwrap();
    // when children exist EmptyRelation, it will bottom-up propagate.

    assert_snapshot!(
    format!("{plan}"),
    @"EmptyRelation: rows=0"
    );
}

#[test]
fn join_keys_in_subquery_alias() {
    let sql = "SELECT * FROM test AS A, ( SELECT col_int32 as key FROM test ) AS B where A.col_int32 = B.key;";
    let plan = test_sql(sql).unwrap();

    assert_snapshot!(
    format!("{plan}"),
    @r#"
        Inner Join: a.col_int32 = b.key
          SubqueryAlias: a
            Filter: test.col_int32 IS NOT NULL
              TableScan: test projection=[col_int32, col_uint32, col_utf8, col_date32, col_date64, col_ts_nano_none, col_ts_nano_utc]
          SubqueryAlias: b
            Projection: test.col_int32 AS key
              Filter: test.col_int32 IS NOT NULL
                TableScan: test projection=[col_int32]
        "#
    );
}

#[test]
fn join_keys_in_subquery_alias_1() {
    let sql = "SELECT * FROM test AS A, ( SELECT test.col_int32 AS key FROM test JOIN test AS C on test.col_int32 = C.col_int32 ) AS B where A.col_int32 = B.key;";
    let plan = test_sql(sql).unwrap();

    assert_snapshot!(
    format!("{plan}"),
    @r#"
        Inner Join: a.col_int32 = b.key
          SubqueryAlias: a
            Filter: test.col_int32 IS NOT NULL
              TableScan: test projection=[col_int32, col_uint32, col_utf8, col_date32, col_date64, col_ts_nano_none, col_ts_nano_utc]
          SubqueryAlias: b
            Projection: test.col_int32 AS key
              Inner Join: test.col_int32 = c.col_int32
                Filter: test.col_int32 IS NOT NULL
                  TableScan: test projection=[col_int32]
                SubqueryAlias: c
                  Filter: test.col_int32 IS NOT NULL
                    TableScan: test projection=[col_int32]
        "#
    );
}

#[test]
fn push_down_filter_groupby_expr_contains_alias() {
    let sql = "SELECT * FROM (SELECT (col_int32 + col_uint32) AS c, count(*) FROM test GROUP BY 1) where c > 3";
    let plan = test_sql(sql).unwrap();

    assert_snapshot!(
    format!("{plan}"),
    @r#"
        Projection: test.col_int32 + test.col_uint32 AS c, count(Int64(1)) AS count(*)
          Aggregate: groupBy=[[CAST(test.col_int32 AS Int64) + CAST(test.col_uint32 AS Int64)]], aggr=[[count(Int64(1))]]
            Filter: CAST(test.col_int32 AS Int64) + CAST(test.col_uint32 AS Int64) > Int64(3)
              TableScan: test projection=[col_int32, col_uint32]
        "#
    );
}

#[test]
// issue: https://github.com/apache/datafusion/issues/5334
fn test_same_name_but_not_ambiguous() {
    let sql = "SELECT t1.col_int32 AS col_int32 FROM test t1 intersect SELECT col_int32 FROM test t2";
    let plan = test_sql(sql).unwrap();

    assert_snapshot!(
    format!("{plan}"),
    @r#"
        LeftSemi Join: t1.col_int32 = t2.col_int32
          Aggregate: groupBy=[[t1.col_int32]], aggr=[[]]
            SubqueryAlias: t1
              TableScan: test projection=[col_int32]
          SubqueryAlias: t2
            TableScan: test projection=[col_int32]
        "#
    );
}

#[test]
fn eliminate_nested_filters() {
    let sql = "\
        SELECT col_int32 FROM test \
        WHERE (1=1) AND (col_int32 > 0) \
        AND (1=1) AND (1=0 OR 1=1)";

    let plan = test_sql(sql).unwrap();

    assert_snapshot!(
          format!("{plan}"),
          @r#"
Filter: test.col_int32 > Int32(0)
  TableScan: test projection=[col_int32]
  "#
    );
}

#[test]
fn eliminate_redundant_null_check_on_count() {
    let sql = "\
        SELECT col_int32, count(*) c
        FROM test
        GROUP BY col_int32
        HAVING c IS NOT NULL";
    let plan = test_sql(sql).unwrap();

    assert_snapshot!(
    format!("{plan}"),
    @r#"
        Projection: test.col_int32, count(Int64(1)) AS count(*) AS c
          Aggregate: groupBy=[[test.col_int32]], aggr=[[count(Int64(1))]]
            TableScan: test projection=[col_int32]
        "#
    );
}

#[test]
fn test_propagate_empty_relation_inner_join_and_unions() {
    let sql = "\
        SELECT A.col_int32 FROM test AS A \
        INNER JOIN ( \
          SELECT col_int32 FROM test WHERE 1 = 0 \
        ) AS B ON A.col_int32 = B.col_int32 \
        UNION ALL \
        SELECT test.col_int32 FROM test WHERE 1 = 1 \
        UNION ALL \
        SELECT test.col_int32 FROM test WHERE 0 = 0 \
        UNION ALL \
        SELECT test.col_int32 FROM test WHERE test.col_int32 < 0 \
        UNION ALL \
        SELECT test.col_int32 FROM test WHERE 1 = 0";

    let plan = test_sql(sql).unwrap();

    assert_snapshot!(
    format!("{plan}"),
    @r#"
Union
  TableScan: test projection=[col_int32]
  TableScan: test projection=[col_int32]
  Filter: test.col_int32 < Int32(0)
    TableScan: test projection=[col_int32]
  "#);
}

#[test]
fn select_wildcard_with_repeated_column_but_is_aliased() {
    let sql = "SELECT *, col_int32 as col_32 FROM test";

    let plan = test_sql(sql).unwrap();

    assert_snapshot!(
    format!("{plan}"),
    @r#"
        Projection: test.col_int32, test.col_uint32, test.col_utf8, test.col_date32, test.col_date64, test.col_ts_nano_none, test.col_ts_nano_utc, test.col_int32 AS col_32
          TableScan: test projection=[col_int32, col_uint32, col_utf8, col_date32, col_date64, col_ts_nano_none, col_ts_nano_utc]
        "#
    );
}

#[test]
fn select_correlated_predicate_subquery_with_uppercase_ident() {
    let sql = r#"
        SELECT *
        FROM
            test
        WHERE
            EXISTS (
                SELECT 1
                FROM (SELECT col_int32 as "COL_INT32", col_uint32 as "COL_UINT32" FROM test) "T1"
                WHERE "T1"."COL_INT32" = test.col_int32
            )
    "#;
    let plan = test_sql(sql).unwrap();

    assert_snapshot!(
    format!("{plan}"),
    @r#"
        LeftSemi Join: test.col_int32 = __correlated_sq_1.COL_INT32
          Filter: test.col_int32 IS NOT NULL
            TableScan: test projection=[col_int32, col_uint32, col_utf8, col_date32, col_date64, col_ts_nano_none, col_ts_nano_utc]
          SubqueryAlias: __correlated_sq_1
            SubqueryAlias: T1
              Projection: test.col_int32 AS COL_INT32
                Filter: test.col_int32 IS NOT NULL
                  TableScan: test projection=[col_int32]
        "#
    );
}

#[test]
fn recursive_cte_projection_pushdown() -> Result<()> {
    // Test that projection pushdown works with recursive CTEs by ensuring
    // only the required columns are projected from the base table, even when
    // the CTE definition includes unused columns
    let sql = "WITH RECURSIVE nodes AS (\
        SELECT col_int32 AS id, col_utf8 AS name, col_uint32 AS extra FROM test \
        UNION ALL \
        SELECT id + 1, name, extra FROM nodes WHERE id < 3\
    ) SELECT id FROM nodes";
    let plan = test_sql(sql)?;

    // The optimizer successfully performs projection pushdown by only selecting the needed
    // columns from the base table and recursive table, eliminating unused columns
    assert_snapshot!(
        format!("{plan}"),
        @r"
    SubqueryAlias: nodes
      RecursiveQuery: is_distinct=false
        Projection: test.col_int32 AS id
          TableScan: test projection=[col_int32]
        Projection: CAST(CAST(nodes.id AS Int64) + Int64(1) AS Int32) AS id
          Filter: nodes.id < Int32(3)
            TableScan: nodes projection=[id]
    "
    );
    Ok(())
}

#[test]
fn recursive_cte_with_aliased_self_reference() -> Result<()> {
    let sql = "WITH RECURSIVE nodes AS (\
        SELECT col_int32 AS id, col_utf8 AS name FROM test \
        UNION ALL \
        SELECT child.id + 1, child.name FROM nodes AS child WHERE child.id < 3\
    ) SELECT id FROM nodes";
    let plan = test_sql(sql)?;

    assert_snapshot!(
        format!("{plan}"),
        @r"
    SubqueryAlias: nodes
      RecursiveQuery: is_distinct=false
        Projection: test.col_int32 AS id
          TableScan: test projection=[col_int32]
        Projection: CAST(CAST(child.id AS Int64) + Int64(1) AS Int32) AS id
          SubqueryAlias: child
            Filter: nodes.id < Int32(3)
              TableScan: nodes projection=[id]
    ",
    );
    Ok(())
}

#[test]
fn recursive_cte_with_unused_columns() -> Result<()> {
    // Test projection pushdown with a recursive CTE where the base case
    // includes columns that are never used in the recursive part or final result
    let sql = "WITH RECURSIVE series AS (\
        SELECT 1 AS n, col_utf8, col_uint32, col_date32 FROM test WHERE col_int32 = 1 \
        UNION ALL \
        SELECT n + 1, col_utf8, col_uint32, col_date32 FROM series WHERE n < 3\
    ) SELECT n FROM series";
    let plan = test_sql(sql)?;

    // The optimizer successfully performs projection pushdown by eliminating unused columns
    // even when they're defined in the CTE but not actually needed
    assert_snapshot!(
        format!("{plan}"),
        @r#"SubqueryAlias: series
  RecursiveQuery: is_distinct=false
    Projection: Int64(1) AS n
      Filter: test.col_int32 = Int32(1)
        TableScan: test projection=[col_int32]
    Projection: series.n + Int64(1)
      Filter: series.n < Int64(3)
        TableScan: series projection=[n]
"#
    );
    Ok(())
}

#[test]
/// Asserts the minimal plan shape once projection pushdown succeeds for a recursive CTE.
/// Unlike the previous two tests that retain extra columns in either the base or recursive
/// branches, this baseline shows the optimizer trimming everything down to the single
/// column required by the final projection.
fn recursive_cte_projection_pushdown_baseline() -> Result<()> {
    // Test case that truly demonstrates projection pushdown working:
    // The base case only selects needed columns
    let sql = "WITH RECURSIVE countdown AS (\
        SELECT col_int32 AS n FROM test WHERE col_int32 = 5 \
        UNION ALL \
        SELECT n - 1 FROM countdown WHERE n > 1\
    ) SELECT n FROM countdown";
    let plan = test_sql(sql)?;

    // This demonstrates optimal projection pushdown where only col_int32 is projected from the base table,
    // and only the needed column is selected from the recursive table
    assert_snapshot!(
        format!("{plan}"),
        @r"
    SubqueryAlias: countdown
      RecursiveQuery: is_distinct=false
        Projection: test.col_int32 AS n
          Filter: test.col_int32 = Int32(5)
            TableScan: test projection=[col_int32]
        Projection: CAST(CAST(countdown.n AS Int64) - Int64(1) AS Int32) AS n
          Filter: countdown.n > Int32(1)
            TableScan: countdown projection=[n]
    "
    );
    Ok(())
}

fn test_sql(sql: &str) -> Result<LogicalPlan> {
    // parse the SQL
    let dialect = GenericDialect {}; // or AnsiDialect, or your own dialect ...
    let ast: Vec<Statement> = Parser::parse_sql(&dialect, sql).unwrap();
    let statement = &ast[0];
    let context_provider = MyContextProvider::default()
        .with_udaf(sum_udaf())
        .with_udaf(count_udaf())
        .with_udaf(avg_udaf())
        .with_expr_planners(vec![
            Arc::new(AggregateFunctionPlanner),
            Arc::new(WindowFunctionPlanner),
        ])
        .with_schema(
            "test",
            Schema::new_with_metadata(
                vec![
                    Field::new("col_int32", DataType::Int32, true),
                    Field::new("col_uint32", DataType::UInt32, true),
                    Field::new("col_utf8", DataType::Utf8, true),
                    Field::new("col_date32", DataType::Date32, true),
                    Field::new("col_date64", DataType::Date64, true),
                    // timestamp with no timezone
                    Field::new(
                        "col_ts_nano_none",
                        DataType::Timestamp(TimeUnit::Nanosecond, None),
                        true,
                    ),
                    // timestamp with UTC timezone
                    Field::new(
                        "col_ts_nano_utc",
                        DataType::Timestamp(TimeUnit::Nanosecond, Some("+00:00".into())),
                        true,
                    ),
                ],
                HashMap::new(),
            ),
        );

    let sql_to_rel = SqlToRel::new(&context_provider);
    let plan = sql_to_rel.sql_statement_to_plan(statement.clone())?;

    let config = OptimizerContext::new().with_skip_failing_rules(false);
    let analyzer = Analyzer::new();
    let optimizer = Optimizer::new();
    // analyze and optimize the logical plan
    let plan = analyzer.execute_and_check(plan, &config.options(), |_, _| {})?;
    optimizer.optimize(plan, &config, observe)
}

fn observe(_plan: &LogicalPlan, _rule: &dyn OptimizerRule) {}

#[derive(Default)]
struct MyContextProvider {
    options: ConfigOptions,
    tables: HashMap<String, Arc<dyn TableSource>>,
    udafs: HashMap<String, Arc<AggregateUDF>>,
    expr_planners: Vec<Arc<dyn ExprPlanner>>,
}

impl MyContextProvider {
    fn with_udaf(mut self, udaf: Arc<AggregateUDF>) -> Self {
        // TODO: change to to_string() if all the function name is converted to lowercase
        self.udafs.insert(udaf.name().to_lowercase(), udaf);
        self
    }

    fn with_expr_planners(mut self, expr_planners: Vec<Arc<dyn ExprPlanner>>) -> Self {
        self.expr_planners = expr_planners;
        self
    }

    fn with_schema(mut self, name: impl Into<String>, schema: Schema) -> Self {
        self.tables.insert(
            name.into(),
            Arc::new(MyTableSource {
                schema: Arc::new(schema),
            }),
        );
        self
    }
}

impl ContextProvider for MyContextProvider {
    fn get_table_source(&self, name: TableReference) -> Result<Arc<dyn TableSource>> {
        let table_name = name.table();
        if let Some(table) = self.tables.get(table_name) {
            Ok(table.clone())
        } else {
            plan_err!("table does not exist")
        }
    }

    fn get_function_meta(&self, _name: &str) -> Option<Arc<ScalarUDF>> {
        None
    }

    fn get_aggregate_meta(&self, name: &str) -> Option<Arc<AggregateUDF>> {
        self.udafs.get(name).cloned()
    }

    fn get_variable_type(&self, _variable_names: &[String]) -> Option<DataType> {
        None
    }

    fn get_window_meta(&self, _name: &str) -> Option<Arc<WindowUDF>> {
        None
    }

    fn create_cte_work_table(
        &self,
        _name: &str,
        schema: SchemaRef,
    ) -> Result<Arc<dyn TableSource>> {
        Ok(Arc::new(MyTableSource { schema }))
    }

    fn options(&self) -> &ConfigOptions {
        &self.options
    }

    fn udf_names(&self) -> Vec<String> {
        Vec::new()
    }

    fn udaf_names(&self) -> Vec<String> {
        Vec::new()
    }

    fn udwf_names(&self) -> Vec<String> {
        Vec::new()
    }

    fn get_expr_planners(&self) -> &[Arc<dyn ExprPlanner>] {
        &self.expr_planners
    }
}

struct MyTableSource {
    schema: SchemaRef,
}

impl TableSource for MyTableSource {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }
}
