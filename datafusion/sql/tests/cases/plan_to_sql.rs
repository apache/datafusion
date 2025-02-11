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

use arrow::datatypes::{DataType, Field, Schema};
use datafusion_common::{assert_contains, DFSchema, DFSchemaRef, Result, TableReference};
use datafusion_expr::test::function_stub::{
    count_udaf, max_udaf, min_udaf, sum, sum_udaf,
};
use datafusion_expr::{
    col, lit, table_scan, wildcard, EmptyRelation, Expr, Extension, LogicalPlan,
    LogicalPlanBuilder, Union, UserDefinedLogicalNode, UserDefinedLogicalNodeCore,
};
use datafusion_functions::unicode;
use datafusion_functions_aggregate::grouping::grouping_udaf;
use datafusion_functions_nested::make_array::make_array_udf;
use datafusion_functions_nested::map::map_udf;
use datafusion_functions_window::rank::rank_udwf;
use datafusion_sql::planner::{ContextProvider, PlannerContext, SqlToRel};
use datafusion_sql::unparser::dialect::{
    CustomDialectBuilder, DefaultDialect as UnparserDefaultDialect, DefaultDialect,
    Dialect as UnparserDialect, MySqlDialect as UnparserMySqlDialect, SqliteDialect,
};
use datafusion_sql::unparser::{expr_to_sql, plan_to_sql, Unparser};
use sqlparser::ast::Statement;
use std::hash::Hash;
use std::sync::Arc;
use std::{fmt, vec};

use crate::common::{MockContextProvider, MockSessionState};
use datafusion_expr::builder::{
    project, table_scan_with_filter_and_fetch, table_scan_with_filters,
};
use datafusion_functions::core::planner::CoreFunctionPlanner;
use datafusion_functions_nested::extract::array_element_udf;
use datafusion_functions_nested::planner::{FieldAccessPlanner, NestedFunctionPlanner};
use datafusion_sql::unparser::ast::{
    DerivedRelationBuilder, QueryBuilder, RelationBuilder, SelectBuilder,
};
use datafusion_sql::unparser::extension_unparser::{
    UnparseToStatementResult, UnparseWithinStatementResult,
    UserDefinedLogicalNodeUnparser,
};
use sqlparser::dialect::{Dialect, GenericDialect, MySqlDialect};
use sqlparser::parser::Parser;

#[test]
fn roundtrip_expr() {
    let tests: Vec<(TableReference, &str, &str)> = vec![
        (TableReference::bare("person"), "age > 35", r#"(age > 35)"#),
        (
            TableReference::bare("person"),
            "id = '10'",
            r#"(id = '10')"#,
        ),
        (
            TableReference::bare("person"),
            "CAST(id AS VARCHAR)",
            r#"CAST(id AS VARCHAR)"#,
        ),
        (
            TableReference::bare("person"),
            "sum((age * 2))",
            r#"sum((age * 2))"#,
        ),
    ];

    let roundtrip = |table, sql: &str| -> Result<String> {
        let dialect = GenericDialect {};
        let sql_expr = Parser::new(&dialect).try_with_sql(sql)?.parse_expr()?;
        let state = MockSessionState::default().with_aggregate_function(sum_udaf());
        let context = MockContextProvider { state };
        let schema = context.get_table_source(table)?.schema();
        let df_schema = DFSchema::try_from(schema.as_ref().clone())?;
        let sql_to_rel = SqlToRel::new(&context);
        let expr =
            sql_to_rel.sql_to_expr(sql_expr, &df_schema, &mut PlannerContext::new())?;

        let ast = expr_to_sql(&expr)?;

        Ok(ast.to_string())
    };

    for (table, query, expected) in tests {
        let actual = roundtrip(table, query).unwrap();
        assert_eq!(actual, expected);
    }
}

#[test]
fn roundtrip_statement() -> Result<()> {
    let tests: Vec<&str> = vec![
            "select 1;",
            "select 1 limit 0;",
            "select ta.j1_id from j1 ta join (select 1 as j1_id) tb on ta.j1_id = tb.j1_id;",
            "select ta.j1_id from j1 ta join (select 1 as j1_id) tb using (j1_id);",
            "select ta.j1_id from j1 ta join (select 1 as j1_id) tb on ta.j1_id = tb.j1_id where ta.j1_id > 1;",
            "select ta.j1_id from (select 1 as j1_id) ta;",
            "select ta.j1_id from j1 ta;",
            "select ta.j1_id from j1 ta order by ta.j1_id;",
            "select * from j1 ta order by ta.j1_id, ta.j1_string desc;",
            "select * from j1 limit 10;",
            "select ta.j1_id from j1 ta where ta.j1_id > 1;",
            "select ta.j1_id, tb.j2_string from j1 ta join j2 tb on (ta.j1_id = tb.j2_id);",
            "select ta.j1_id, tb.j2_string, tc.j3_string from j1 ta join j2 tb on (ta.j1_id = tb.j2_id) join j3 tc on (ta.j1_id = tc.j3_id);",
            "select * from (select id, first_name from person)",
            "select * from (select id, first_name from (select * from person))",
            "select id, count(*) as cnt from (select id from person) group by id",
            "select (id-1)/2, count(*) / (sum(id/10)-1) as agg_expr from (select (id-1) as id from person) group by id",
            "select CAST(id/2 as VARCHAR) NOT LIKE 'foo*' from person where NOT EXISTS (select ta.j1_id, tb.j2_string from j1 ta join j2 tb on (ta.j1_id = tb.j2_id))",
            r#"select "First Name" from person_quoted_cols"#,
            "select DISTINCT id FROM person",
            "select DISTINCT on (id) id, first_name from person",
            "select DISTINCT on (id) id, first_name from person order by id",
            r#"select id, count("First Name") as cnt from (select id, "First Name" from person_quoted_cols) group by id"#,
            "select id, count(*) as cnt from (select p1.id as id from person p1 inner join person p2 on p1.id=p2.id) group by id",
            "select id, count(*), first_name from person group by first_name, id",
            "select id, sum(age), first_name from person group by first_name, id",
            "select id, count(*), first_name
            from person
            where id!=3 and first_name=='test'
            group by first_name, id
            having count(*)>5 and count(*)<10
            order by count(*)",
            r#"select id, count("First Name") as count_first_name, "Last Name"
            from person_quoted_cols
            where id!=3 and "First Name"=='test'
            group by "Last Name", id
            having count_first_name>5 and count_first_name<10
            order by count_first_name, "Last Name""#,
            r#"select p.id, count("First Name") as count_first_name,
            "Last Name", sum(qp.id/p.id - (select sum(id) from person_quoted_cols) ) / (select count(*) from person)
            from (select id, "First Name", "Last Name" from person_quoted_cols) qp
            inner join (select * from person) p
            on p.id = qp.id
            where p.id!=3 and "First Name"=='test' and qp.id in
            (select id from (select id, count(*) from person group by id having count(*) > 0))
            group by "Last Name", p.id
            having count_first_name>5 and count_first_name<10
            order by count_first_name, "Last Name""#,
            r#"SELECT j1_string as string FROM j1
            UNION ALL
            SELECT j2_string as string FROM j2"#,
            r#"SELECT j1_string as string FROM j1
            UNION ALL
            SELECT j2_string as string FROM j2
            ORDER BY string DESC
            LIMIT 10"#,
            r#"SELECT col1, id FROM (
                SELECT j1_string AS col1, j1_id AS id FROM j1
                UNION ALL
                SELECT j2_string AS col1, j2_id AS id FROM j2
                UNION ALL
                SELECT j3_string AS col1, j3_id AS id FROM j3
            ) AS subquery GROUP BY col1, id ORDER BY col1 ASC, id ASC"#,
            "SELECT id, count(*) over (PARTITION BY first_name ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING),
            last_name, sum(id) over (PARTITION BY first_name ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING),
            first_name from person",
            r#"SELECT id, count(distinct id) over (ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING),
            sum(id) OVER (PARTITION BY first_name ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) from person"#,
            "SELECT id, sum(id) OVER (PARTITION BY first_name ROWS BETWEEN 5 PRECEDING AND 2 FOLLOWING) from person",
            "WITH t1 AS (SELECT j1_id AS id, j1_string name FROM j1), t2 AS (SELECT j2_id AS id, j2_string name FROM j2) SELECT * FROM t1 JOIN t2 USING (id, name)",
            "WITH w1 AS (SELECT 'a' as col), w2 AS (SELECT 'b' as col), w3 as (SELECT 'c' as col) SELECT * FROM w1 UNION ALL SELECT * FROM w2 UNION ALL SELECT * FROM w3",
            "WITH w1 AS (SELECT 'a' as col), w2 AS (SELECT 'b' as col), w3 as (SELECT 'c' as col), w4 as (SELECT 'd' as col) SELECT * FROM w1 UNION ALL SELECT * FROM w2 UNION ALL SELECT * FROM w3 UNION ALL SELECT * FROM w4",
            "WITH w1 AS (SELECT 'a' as col), w2 AS (SELECT 'b' as col) SELECT * FROM w1 JOIN w2 ON w1.col = w2.col UNION ALL SELECT * FROM w1 JOIN w2 ON w1.col = w2.col UNION ALL SELECT * FROM w1 JOIN w2 ON w1.col = w2.col",
            r#"SELECT id, first_name,
            SUM(id) AS total_sum,
            SUM(id) OVER (PARTITION BY first_name ROWS BETWEEN 5 PRECEDING AND 2 FOLLOWING) AS moving_sum,
            MAX(SUM(id)) OVER (PARTITION BY first_name ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS max_total
            FROM person JOIN orders ON person.id = orders.customer_id GROUP BY id, first_name"#,
            r#"SELECT id, first_name,
            SUM(id) AS total_sum,
            SUM(id) OVER (PARTITION BY first_name ROWS BETWEEN 5 PRECEDING AND 2 FOLLOWING) AS moving_sum,
            MAX(SUM(id)) OVER (PARTITION BY first_name ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS max_total
            FROM (SELECT id, first_name from person) person JOIN (SELECT customer_id FROM orders) orders ON person.id = orders.customer_id GROUP BY id, first_name"#,
            r#"SELECT id, first_name, last_name, customer_id, SUM(id) AS total_sum
            FROM person
            JOIN orders ON person.id = orders.customer_id
            GROUP BY ROLLUP(id, first_name, last_name, customer_id)"#,
            r#"SELECT id, first_name, last_name,
            SUM(id) AS total_sum,
            COUNT(*) AS total_count,
            SUM(id) OVER (ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_total
            FROM person
            GROUP BY GROUPING SETS ((id, first_name, last_name), (first_name, last_name), (last_name))"#,
            "SELECT ARRAY[1, 2, 3]",
            "SELECT ARRAY[1, 2, 3][1]",
            "SELECT [1, 2, 3]",
            "SELECT [1, 2, 3][1]",
            "SELECT left[1] FROM array",
            "SELECT {a:1, b:2}",
            "SELECT s.a FROM (SELECT {a:1, b:2} AS s)",
            "SELECT MAP {'a': 1, 'b': 2}"
    ];

    // For each test sql string, we transform as follows:
    // sql -> ast::Statement (s1) -> LogicalPlan (p1) -> ast::Statement (s2) -> LogicalPlan (p2)
    // We test not that s1==s2, but rather p1==p2. This ensures that unparser preserves the logical
    // query information of the original sql string and disreguards other differences in syntax or
    // quoting.
    for query in tests {
        let dialect = GenericDialect {};
        let statement = Parser::new(&dialect)
            .try_with_sql(query)?
            .parse_statement()?;
        let state = MockSessionState::default()
            .with_scalar_function(make_array_udf())
            .with_scalar_function(array_element_udf())
            .with_scalar_function(map_udf())
            .with_aggregate_function(sum_udaf())
            .with_aggregate_function(count_udaf())
            .with_aggregate_function(max_udaf())
            .with_expr_planner(Arc::new(CoreFunctionPlanner::default()))
            .with_expr_planner(Arc::new(NestedFunctionPlanner))
            .with_expr_planner(Arc::new(FieldAccessPlanner));
        let context = MockContextProvider { state };
        let sql_to_rel = SqlToRel::new(&context);
        let plan = sql_to_rel.sql_statement_to_plan(statement).unwrap();

        let roundtrip_statement = plan_to_sql(&plan)?;

        let actual = &roundtrip_statement.to_string();
        println!("roundtrip sql: {actual}");
        println!("plan {}", plan.display_indent());

        let plan_roundtrip = sql_to_rel
            .sql_statement_to_plan(roundtrip_statement.clone())
            .unwrap();

        assert_eq!(plan, plan_roundtrip);
    }

    Ok(())
}

#[test]
fn roundtrip_crossjoin() -> Result<()> {
    let query = "select j1.j1_id, j2.j2_string from j1, j2";

    let dialect = GenericDialect {};
    let statement = Parser::new(&dialect)
        .try_with_sql(query)?
        .parse_statement()?;

    let state = MockSessionState::default()
        .with_expr_planner(Arc::new(CoreFunctionPlanner::default()));

    let context = MockContextProvider { state };
    let sql_to_rel = SqlToRel::new(&context);
    let plan = sql_to_rel.sql_statement_to_plan(statement).unwrap();

    let roundtrip_statement = plan_to_sql(&plan)?;

    let actual = &roundtrip_statement.to_string();
    println!("roundtrip sql: {actual}");
    println!("plan {}", plan.display_indent());

    let plan_roundtrip = sql_to_rel
        .sql_statement_to_plan(roundtrip_statement)
        .unwrap();

    let expected = "Projection: j1.j1_id, j2.j2_string\
        \n  Cross Join: \
        \n    TableScan: j1\
        \n    TableScan: j2";

    assert_eq!(plan_roundtrip.to_string(), expected);

    Ok(())
}

#[test]
fn roundtrip_statement_with_dialect() -> Result<()> {
    struct TestStatementWithDialect {
        sql: &'static str,
        expected: &'static str,
        parser_dialect: Box<dyn Dialect>,
        unparser_dialect: Box<dyn UnparserDialect>,
    }
    let tests: Vec<TestStatementWithDialect> = vec![
        TestStatementWithDialect {
            sql: "select min(ta.j1_id) as j1_min from j1 ta order by min(ta.j1_id) limit 10;",
            expected:
                // top projection sort gets derived into a subquery
                // for MySQL, this subquery needs an alias
                "SELECT `j1_min` FROM (SELECT min(`ta`.`j1_id`) AS `j1_min`, min(`ta`.`j1_id`) FROM `j1` AS `ta` ORDER BY min(`ta`.`j1_id`) ASC) AS `derived_sort` LIMIT 10",
            parser_dialect: Box::new(MySqlDialect {}),
            unparser_dialect: Box::new(UnparserMySqlDialect {}),
        },
        TestStatementWithDialect {
            sql: "select min(ta.j1_id) as j1_min from j1 ta order by min(ta.j1_id) limit 10;",
            expected:
                // top projection sort still gets derived into a subquery in default dialect
                // except for the default dialect, the subquery is left non-aliased
                "SELECT j1_min FROM (SELECT min(ta.j1_id) AS j1_min, min(ta.j1_id) FROM j1 AS ta ORDER BY min(ta.j1_id) ASC NULLS LAST) LIMIT 10",
            parser_dialect: Box::new(GenericDialect {}),
            unparser_dialect: Box::new(UnparserDefaultDialect {}),
        },
        TestStatementWithDialect {
            sql: "select min(ta.j1_id) as j1_min, max(tb.j1_max) from j1 ta, (select distinct max(ta.j1_id) as j1_max from j1 ta order by max(ta.j1_id)) tb order by min(ta.j1_id) limit 10;",
            expected:
                "SELECT `j1_min`, `max(tb.j1_max)` FROM (SELECT min(`ta`.`j1_id`) AS `j1_min`, max(`tb`.`j1_max`), min(`ta`.`j1_id`) FROM `j1` AS `ta` CROSS JOIN (SELECT `j1_max` FROM (SELECT DISTINCT max(`ta`.`j1_id`) AS `j1_max` FROM `j1` AS `ta`) AS `derived_distinct`) AS `tb` ORDER BY min(`ta`.`j1_id`) ASC) AS `derived_sort` LIMIT 10",
            parser_dialect: Box::new(MySqlDialect {}),
            unparser_dialect: Box::new(UnparserMySqlDialect {}),
        },
        TestStatementWithDialect {
            sql: "select j1_id from (select 1 as j1_id);",
            expected:
                "SELECT `j1_id` FROM (SELECT 1 AS `j1_id`) AS `derived_projection`",
            parser_dialect: Box::new(MySqlDialect {}),
            unparser_dialect: Box::new(UnparserMySqlDialect {}),
        },
        TestStatementWithDialect {
            sql: "select * from (select * from j1 limit 10);",
            expected:
                "SELECT * FROM (SELECT * FROM `j1` LIMIT 10) AS `derived_limit`",
            parser_dialect: Box::new(MySqlDialect {}),
            unparser_dialect: Box::new(UnparserMySqlDialect {}),
        },
        TestStatementWithDialect {
            sql: "select ta.j1_id from j1 ta order by j1_id limit 10;",
            expected:
                "SELECT `ta`.`j1_id` FROM `j1` AS `ta` ORDER BY `ta`.`j1_id` ASC LIMIT 10",
            parser_dialect: Box::new(MySqlDialect {}),
            unparser_dialect: Box::new(UnparserMySqlDialect {}),
        },
        TestStatementWithDialect {
            sql: "select ta.j1_id from j1 ta order by j1_id limit 10;",
            expected: r#"SELECT ta.j1_id FROM j1 AS ta ORDER BY ta.j1_id ASC NULLS LAST LIMIT 10"#,
            parser_dialect: Box::new(GenericDialect {}),
            unparser_dialect: Box::new(UnparserDefaultDialect {}),
        },
        TestStatementWithDialect {
            sql: "SELECT j1_id FROM j1
                  UNION ALL
                  SELECT tb.j2_id as j1_id FROM j2 tb
                  ORDER BY j1_id
                  LIMIT 10;",
            expected: r#"SELECT j1.j1_id FROM j1 UNION ALL SELECT tb.j2_id AS j1_id FROM j2 AS tb ORDER BY j1_id ASC NULLS LAST LIMIT 10"#,
            parser_dialect: Box::new(GenericDialect {}),
            unparser_dialect: Box::new(UnparserDefaultDialect {}),
        },
        // Test query with derived tables that put distinct,sort,limit on the wrong level
        TestStatementWithDialect {
            sql: "SELECT j1_string from j1 order by j1_id",
            expected: r#"SELECT j1.j1_string FROM j1 ORDER BY j1.j1_id ASC NULLS LAST"#,
            parser_dialect: Box::new(GenericDialect {}),
            unparser_dialect: Box::new(UnparserDefaultDialect {}),
        },
        TestStatementWithDialect {
            sql: "SELECT j1_string AS a from j1 order by j1_id",
            expected: r#"SELECT j1.j1_string AS a FROM j1 ORDER BY j1.j1_id ASC NULLS LAST"#,
            parser_dialect: Box::new(GenericDialect {}),
            unparser_dialect: Box::new(UnparserDefaultDialect {}),
        },
        TestStatementWithDialect {
            sql: "SELECT j1_string from j1 join j2 on j1.j1_id = j2.j2_id order by j1_id",
            expected: r#"SELECT j1.j1_string FROM j1 JOIN j2 ON (j1.j1_id = j2.j2_id) ORDER BY j1.j1_id ASC NULLS LAST"#,
            parser_dialect: Box::new(GenericDialect {}),
            unparser_dialect: Box::new(UnparserDefaultDialect {}),
        },
        TestStatementWithDialect {
            sql: "
                SELECT
                  j1_string,
                  j2_string
                FROM
                  (
                    SELECT
                      distinct j1_id,
                      j1_string,
                      j2_string
                    from
                      j1
                      INNER join j2 ON j1.j1_id = j2.j2_id
                    order by
                      j1.j1_id desc
                    limit
                      10
                  ) abc
                ORDER BY
                  abc.j2_string",
            expected: r#"SELECT abc.j1_string, abc.j2_string FROM (SELECT DISTINCT j1.j1_id, j1.j1_string, j2.j2_string FROM j1 JOIN j2 ON (j1.j1_id = j2.j2_id) ORDER BY j1.j1_id DESC NULLS FIRST LIMIT 10) AS abc ORDER BY abc.j2_string ASC NULLS LAST"#,
            parser_dialect: Box::new(GenericDialect {}),
            unparser_dialect: Box::new(UnparserDefaultDialect {}),
        },
        // more tests around subquery/derived table roundtrip
        TestStatementWithDialect {
            sql: "SELECT string_count FROM (
                    SELECT
                        j1_id,
                        min(j2_string)
                    FROM
                        j1 LEFT OUTER JOIN j2 ON
                                    j1_id = j2_id
                    GROUP BY
                        j1_id
                ) AS agg (id, string_count)
            ",
            expected: r#"SELECT agg.string_count FROM (SELECT j1.j1_id, min(j2.j2_string) FROM j1 LEFT JOIN j2 ON (j1.j1_id = j2.j2_id) GROUP BY j1.j1_id) AS agg (id, string_count)"#,
            parser_dialect: Box::new(GenericDialect {}),
            unparser_dialect: Box::new(UnparserDefaultDialect {}),
        },
        TestStatementWithDialect {
            sql: "
                SELECT
                  j1_string,
                  j2_string
                FROM
                  (
                    SELECT
                      j1_id,
                      j1_string,
                      j2_string
                    from
                      j1
                      INNER join j2 ON j1.j1_id = j2.j2_id
                    group by
                      j1_id,
                      j1_string,
                      j2_string
                    order by
                      j1.j1_id desc
                    limit
                      10
                  ) abc
                ORDER BY
                  abc.j2_string",
            expected: r#"SELECT abc.j1_string, abc.j2_string FROM (SELECT j1.j1_id, j1.j1_string, j2.j2_string FROM j1 JOIN j2 ON (j1.j1_id = j2.j2_id) GROUP BY j1.j1_id, j1.j1_string, j2.j2_string ORDER BY j1.j1_id DESC NULLS FIRST LIMIT 10) AS abc ORDER BY abc.j2_string ASC NULLS LAST"#,
            parser_dialect: Box::new(GenericDialect {}),
            unparser_dialect: Box::new(UnparserDefaultDialect {}),
        },
        // Test query that order by columns are not in select columns
        TestStatementWithDialect {
            sql: "
                SELECT
                  j1_string
                FROM
                  (
                    SELECT
                      j1_string,
                      j2_string
                    from
                      j1
                      INNER join j2 ON j1.j1_id = j2.j2_id
                    order by
                      j1.j1_id desc,
                      j2.j2_id desc
                    limit
                      10
                  ) abc
                ORDER BY
                  j2_string",
            expected: r#"SELECT abc.j1_string FROM (SELECT j1.j1_string, j2.j2_string FROM j1 JOIN j2 ON (j1.j1_id = j2.j2_id) ORDER BY j1.j1_id DESC NULLS FIRST, j2.j2_id DESC NULLS FIRST LIMIT 10) AS abc ORDER BY abc.j2_string ASC NULLS LAST"#,
            parser_dialect: Box::new(GenericDialect {}),
            unparser_dialect: Box::new(UnparserDefaultDialect {}),
        },
        TestStatementWithDialect {
            sql: "SELECT id FROM (SELECT j1_id from j1) AS c (id)",
            expected: r#"SELECT c.id FROM (SELECT j1.j1_id FROM j1) AS c (id)"#,
            parser_dialect: Box::new(GenericDialect {}),
            unparser_dialect: Box::new(UnparserDefaultDialect {}),
        },
        TestStatementWithDialect {
            sql: "SELECT id FROM (SELECT j1_id as id from j1) AS c",
            expected: r#"SELECT c.id FROM (SELECT j1.j1_id AS id FROM j1) AS c"#,
            parser_dialect: Box::new(GenericDialect {}),
            unparser_dialect: Box::new(UnparserDefaultDialect {}),
        },
        // Test query that has calculation in derived table with columns
        TestStatementWithDialect {
            sql: "SELECT id FROM (SELECT j1_id + 1 * 3 from j1) AS c (id)",
            expected: r#"SELECT c.id FROM (SELECT (j1.j1_id + (1 * 3)) FROM j1) AS c (id)"#,
            parser_dialect: Box::new(GenericDialect {}),
            unparser_dialect: Box::new(UnparserDefaultDialect {}),
        },
        // Test query that has limit/distinct/order in derived table with columns
        TestStatementWithDialect {
            sql: "SELECT id FROM (SELECT distinct (j1_id + 1 * 3) FROM j1 LIMIT 1) AS c (id)",
            expected: r#"SELECT c.id FROM (SELECT DISTINCT (j1.j1_id + (1 * 3)) FROM j1 LIMIT 1) AS c (id)"#,
            parser_dialect: Box::new(GenericDialect {}),
            unparser_dialect: Box::new(UnparserDefaultDialect {}),
        },
        TestStatementWithDialect {
            sql: "SELECT id FROM (SELECT j1_id + 1 FROM j1 ORDER BY j1_id DESC LIMIT 1) AS c (id)",
            expected: r#"SELECT c.id FROM (SELECT (j1.j1_id + 1) FROM j1 ORDER BY j1.j1_id DESC NULLS FIRST LIMIT 1) AS c (id)"#,
            parser_dialect: Box::new(GenericDialect {}),
            unparser_dialect: Box::new(UnparserDefaultDialect {}),
        },
        TestStatementWithDialect {
            sql: "SELECT id FROM (SELECT CAST((CAST(j1_id as BIGINT) + 1) as int) * 10 FROM j1 LIMIT 1) AS c (id)",
            expected: r#"SELECT c.id FROM (SELECT (CAST((CAST(j1.j1_id AS BIGINT) + 1) AS INTEGER) * 10) FROM j1 LIMIT 1) AS c (id)"#,
            parser_dialect: Box::new(GenericDialect {}),
            unparser_dialect: Box::new(UnparserDefaultDialect {}),
        },
        TestStatementWithDialect {
            sql: "SELECT id FROM (SELECT CAST(j1_id as BIGINT) + 1 FROM j1 ORDER BY j1_id LIMIT 1) AS c (id)",
            expected: r#"SELECT c.id FROM (SELECT (CAST(j1.j1_id AS BIGINT) + 1) FROM j1 ORDER BY j1.j1_id ASC NULLS LAST LIMIT 1) AS c (id)"#,
            parser_dialect: Box::new(GenericDialect {}),
            unparser_dialect: Box::new(UnparserDefaultDialect {}),
        },
        TestStatementWithDialect {
            sql: "SELECT temp_j.id2 FROM (SELECT j1_id, j1_string FROM j1) AS temp_j(id2, string2)",
            expected: r#"SELECT temp_j.id2 FROM (SELECT j1.j1_id, j1.j1_string FROM j1) AS temp_j (id2, string2)"#,
            parser_dialect: Box::new(GenericDialect {}),
            unparser_dialect: Box::new(UnparserDefaultDialect {}),
        },
        TestStatementWithDialect {
            sql: "SELECT temp_j.id2 FROM (SELECT j1_id, j1_string FROM j1) AS temp_j(id2, string2)",
            expected: r#"SELECT `temp_j`.`id2` FROM (SELECT `j1`.`j1_id` AS `id2`, `j1`.`j1_string` AS `string2` FROM `j1`) AS `temp_j`"#,
            parser_dialect: Box::new(GenericDialect {}),
            unparser_dialect: Box::new(SqliteDialect {}),
        },
        TestStatementWithDialect {
            sql: "SELECT * FROM (SELECT j1_id + 1 FROM j1) AS temp_j(id2)",
            expected: r#"SELECT * FROM (SELECT (`j1`.`j1_id` + 1) AS `id2` FROM `j1`) AS `temp_j`"#,
            parser_dialect: Box::new(GenericDialect {}),
            unparser_dialect: Box::new(SqliteDialect {}),
        },
        TestStatementWithDialect {
            sql: "SELECT * FROM (SELECT j1_id FROM j1 LIMIT 1) AS temp_j(id2)",
            expected: r#"SELECT * FROM (SELECT `j1`.`j1_id` AS `id2` FROM `j1` LIMIT 1) AS `temp_j`"#,
            parser_dialect: Box::new(GenericDialect {}),
            unparser_dialect: Box::new(SqliteDialect {}),
        },
        TestStatementWithDialect {
            sql: "SELECT * FROM UNNEST([1,2,3])",
            expected: r#"SELECT * FROM (SELECT UNNEST([1, 2, 3]) AS "UNNEST(make_array(Int64(1),Int64(2),Int64(3)))")"#,
            parser_dialect: Box::new(GenericDialect {}),
            unparser_dialect: Box::new(UnparserDefaultDialect {}),
        },
        TestStatementWithDialect {
            sql: "SELECT * FROM UNNEST([1,2,3]) AS t1 (c1)",
            expected: r#"SELECT * FROM (SELECT UNNEST([1, 2, 3]) AS "UNNEST(make_array(Int64(1),Int64(2),Int64(3)))") AS t1 (c1)"#,
            parser_dialect: Box::new(GenericDialect {}),
            unparser_dialect: Box::new(UnparserDefaultDialect {}),
        },
        TestStatementWithDialect {
            sql: "SELECT * FROM UNNEST([1,2,3]) AS t1 (c1)",
            expected: r#"SELECT * FROM (SELECT UNNEST([1, 2, 3]) AS "UNNEST(make_array(Int64(1),Int64(2),Int64(3)))") AS t1 (c1)"#,
            parser_dialect: Box::new(GenericDialect {}),
            unparser_dialect: Box::new(UnparserDefaultDialect {}),
        },
        TestStatementWithDialect {
            sql: "SELECT * FROM UNNEST([1,2,3]), j1",
            expected: r#"SELECT * FROM (SELECT UNNEST([1, 2, 3]) AS "UNNEST(make_array(Int64(1),Int64(2),Int64(3)))") CROSS JOIN j1"#,
            parser_dialect: Box::new(GenericDialect {}),
            unparser_dialect: Box::new(UnparserDefaultDialect {}),
        },
        TestStatementWithDialect {
            sql: "SELECT * FROM UNNEST([1,2,3]) u(c1) JOIN j1 ON u.c1 = j1.j1_id",
            expected: r#"SELECT * FROM (SELECT UNNEST([1, 2, 3]) AS "UNNEST(make_array(Int64(1),Int64(2),Int64(3)))") AS u (c1) JOIN j1 ON (u.c1 = j1.j1_id)"#,
            parser_dialect: Box::new(GenericDialect {}),
            unparser_dialect: Box::new(UnparserDefaultDialect {}),
        },
        TestStatementWithDialect {
            sql: "SELECT * FROM UNNEST([1,2,3]) u(c1) UNION ALL SELECT * FROM UNNEST([4,5,6]) u(c1)",
            expected: r#"SELECT * FROM (SELECT UNNEST([1, 2, 3]) AS "UNNEST(make_array(Int64(1),Int64(2),Int64(3)))") AS u (c1) UNION ALL SELECT * FROM (SELECT UNNEST([4, 5, 6]) AS "UNNEST(make_array(Int64(4),Int64(5),Int64(6)))") AS u (c1)"#,
            parser_dialect: Box::new(GenericDialect {}),
            unparser_dialect: Box::new(UnparserDefaultDialect {}),
        },
        TestStatementWithDialect {
            sql: "SELECT * FROM UNNEST([1,2,3])",
            expected: r#"SELECT * FROM UNNEST([1, 2, 3])"#,
            parser_dialect: Box::new(GenericDialect {}),
            unparser_dialect: Box::new(CustomDialectBuilder::default().with_unnest_as_table_factor(true).build()),
        },
        TestStatementWithDialect {
            sql: "SELECT * FROM UNNEST([1,2,3]) AS t1 (c1)",
            expected: r#"SELECT * FROM UNNEST([1, 2, 3]) AS t1 (c1)"#,
            parser_dialect: Box::new(GenericDialect {}),
            unparser_dialect: Box::new(CustomDialectBuilder::default().with_unnest_as_table_factor(true).build()),
        },
        TestStatementWithDialect {
            sql: "SELECT * FROM UNNEST([1,2,3]) AS t1 (c1)",
            expected: r#"SELECT * FROM UNNEST([1, 2, 3]) AS t1 (c1)"#,
            parser_dialect: Box::new(GenericDialect {}),
            unparser_dialect: Box::new(CustomDialectBuilder::default().with_unnest_as_table_factor(true).build()),
        },
        TestStatementWithDialect {
            sql: "SELECT * FROM UNNEST([1,2,3]), j1",
            expected: r#"SELECT * FROM UNNEST([1, 2, 3]) CROSS JOIN j1"#,
            parser_dialect: Box::new(GenericDialect {}),
            unparser_dialect: Box::new(CustomDialectBuilder::default().with_unnest_as_table_factor(true).build()),
        },
        TestStatementWithDialect {
            sql: "SELECT * FROM UNNEST([1,2,3]) u(c1) JOIN j1 ON u.c1 = j1.j1_id",
            expected: r#"SELECT * FROM UNNEST([1, 2, 3]) AS u (c1) JOIN j1 ON (u.c1 = j1.j1_id)"#,
            parser_dialect: Box::new(GenericDialect {}),
            unparser_dialect: Box::new(CustomDialectBuilder::default().with_unnest_as_table_factor(true).build()),
        },
        TestStatementWithDialect {
            sql: "SELECT * FROM UNNEST([1,2,3]) u(c1) UNION ALL SELECT * FROM UNNEST([4,5,6]) u(c1)",
            expected: r#"SELECT * FROM UNNEST([1, 2, 3]) AS u (c1) UNION ALL SELECT * FROM UNNEST([4, 5, 6]) AS u (c1)"#,
            parser_dialect: Box::new(GenericDialect {}),
            unparser_dialect: Box::new(CustomDialectBuilder::default().with_unnest_as_table_factor(true).build()),
        },
        TestStatementWithDialect {
            sql: "SELECT UNNEST([1,2,3])",
            expected: r#"SELECT * FROM UNNEST([1, 2, 3])"#,
            parser_dialect: Box::new(GenericDialect {}),
            unparser_dialect: Box::new(CustomDialectBuilder::default().with_unnest_as_table_factor(true).build()),
        },
        TestStatementWithDialect {
            sql: "SELECT UNNEST([1,2,3]) as c1",
            expected: r#"SELECT UNNEST([1, 2, 3]) AS c1"#,
            parser_dialect: Box::new(GenericDialect {}),
            unparser_dialect: Box::new(CustomDialectBuilder::default().with_unnest_as_table_factor(true).build()),
        },
        TestStatementWithDialect {
            sql: "SELECT UNNEST([1,2,3]), 1",
            expected: r#"SELECT UNNEST([1, 2, 3]) AS UNNEST(make_array(Int64(1),Int64(2),Int64(3))), Int64(1)"#,
            parser_dialect: Box::new(GenericDialect {}),
            unparser_dialect: Box::new(CustomDialectBuilder::default().with_unnest_as_table_factor(true).build()),
        },
        TestStatementWithDialect {
            sql: "SELECT * FROM unnest_table u, UNNEST(u.array_col)",
            expected: r#"SELECT * FROM unnest_table AS u CROSS JOIN UNNEST(u.array_col)"#,
            parser_dialect: Box::new(GenericDialect {}),
            unparser_dialect: Box::new(CustomDialectBuilder::default().with_unnest_as_table_factor(true).build()),
        },
        TestStatementWithDialect {
            sql: "SELECT * FROM unnest_table u, UNNEST(u.array_col) AS t1 (c1)",
            expected: r#"SELECT * FROM unnest_table AS u CROSS JOIN UNNEST(u.array_col) AS t1 (c1)"#,
            parser_dialect: Box::new(GenericDialect {}),
            unparser_dialect: Box::new(CustomDialectBuilder::default().with_unnest_as_table_factor(true).build()),
        },
        TestStatementWithDialect {
            sql: "SELECT * FROM unnest_table u, UNNEST(u.array_col)",
            expected: r#"SELECT * FROM unnest_table AS u CROSS JOIN LATERAL (SELECT UNNEST(u.array_col) AS "UNNEST(outer_ref(u.array_col))")"#,
            parser_dialect: Box::new(GenericDialect {}),
            unparser_dialect: Box::new(UnparserDefaultDialect {}),
        },
        TestStatementWithDialect {
            sql: "SELECT * FROM unnest_table u, UNNEST(u.array_col) AS t1 (c1)",
            expected: r#"SELECT * FROM unnest_table AS u CROSS JOIN LATERAL (SELECT UNNEST(u.array_col) AS "UNNEST(outer_ref(u.array_col))") AS t1 (c1)"#,
            parser_dialect: Box::new(GenericDialect {}),
            unparser_dialect: Box::new(UnparserDefaultDialect {}),
        },
    ];

    for query in tests {
        let statement = Parser::new(&*query.parser_dialect)
            .try_with_sql(query.sql)?
            .parse_statement()?;

        let state = MockSessionState::default()
            .with_aggregate_function(max_udaf())
            .with_aggregate_function(min_udaf())
            .with_expr_planner(Arc::new(CoreFunctionPlanner::default()))
            .with_expr_planner(Arc::new(NestedFunctionPlanner));

        let context = MockContextProvider { state };
        let sql_to_rel = SqlToRel::new(&context);
        let plan = sql_to_rel
            .sql_statement_to_plan(statement)
            .unwrap_or_else(|e| panic!("Failed to parse sql: {}\n{e}", query.sql));

        let unparser = Unparser::new(&*query.unparser_dialect);
        let roundtrip_statement = unparser.plan_to_sql(&plan)?;

        let actual = &roundtrip_statement.to_string();
        println!("roundtrip sql: {actual}");
        println!("plan {}", plan.display_indent());

        assert_eq!(query.expected, actual);
    }

    Ok(())
}

#[test]
fn test_unnest_logical_plan() -> Result<()> {
    let query = "select unnest(struct_col), unnest(array_col), struct_col, array_col from unnest_table";

    let dialect = GenericDialect {};
    let statement = Parser::new(&dialect)
        .try_with_sql(query)?
        .parse_statement()?;

    let context = MockContextProvider {
        state: MockSessionState::default(),
    };
    let sql_to_rel = SqlToRel::new(&context);
    let plan = sql_to_rel.sql_statement_to_plan(statement).unwrap();
    let expected = r#"
Projection: __unnest_placeholder(unnest_table.struct_col).field1, __unnest_placeholder(unnest_table.struct_col).field2, __unnest_placeholder(unnest_table.array_col,depth=1) AS UNNEST(unnest_table.array_col), unnest_table.struct_col, unnest_table.array_col
  Unnest: lists[__unnest_placeholder(unnest_table.array_col)|depth=1] structs[__unnest_placeholder(unnest_table.struct_col)]
    Projection: unnest_table.struct_col AS __unnest_placeholder(unnest_table.struct_col), unnest_table.array_col AS __unnest_placeholder(unnest_table.array_col), unnest_table.struct_col, unnest_table.array_col
      TableScan: unnest_table"#.trim_start();

    assert_eq!(plan.to_string(), expected);

    Ok(())
}

#[test]
fn test_aggregation_without_projection() -> Result<()> {
    let schema = Schema::new(vec![
        Field::new("name", DataType::Utf8, false),
        Field::new("age", DataType::UInt8, false),
    ]);

    let plan = LogicalPlanBuilder::from(
        table_scan(Some("users"), &schema, Some(vec![0, 1]))?.build()?,
    )
    .aggregate(vec![col("name")], vec![sum(col("age"))])?
    .build()?;

    let unparser = Unparser::default();
    let statement = unparser.plan_to_sql(&plan)?;

    let actual = &statement.to_string();

    assert_eq!(
        actual,
        r#"SELECT sum(users.age), users."name" FROM users GROUP BY users."name""#
    );

    Ok(())
}

#[test]
fn test_table_references_in_plan_to_sql() {
    fn test(table_name: &str, expected_sql: &str, dialect: &impl UnparserDialect) {
        let schema = Schema::new(vec![
            Field::new("id", DataType::Utf8, false),
            Field::new("value", DataType::Utf8, false),
        ]);
        let plan = table_scan(Some(table_name), &schema, None)
            .unwrap()
            .project(vec![col("id"), col("value")])
            .unwrap()
            .build()
            .unwrap();

        let unparser = Unparser::new(dialect);
        let sql = unparser.plan_to_sql(&plan).unwrap();

        assert_eq!(sql.to_string(), expected_sql)
    }

    test(
        "catalog.schema.table",
        r#"SELECT "table".id, "table"."value" FROM "catalog"."schema"."table""#,
        &DefaultDialect {},
    );
    test(
        "schema.table",
        r#"SELECT "table".id, "table"."value" FROM "schema"."table""#,
        &DefaultDialect {},
    );
    test(
        "table",
        r#"SELECT "table".id, "table"."value" FROM "table""#,
        &DefaultDialect {},
    );

    let custom_dialect = CustomDialectBuilder::default()
        .with_full_qualified_col(true)
        .with_identifier_quote_style('"')
        .build();

    test(
        "catalog.schema.table",
        r#"SELECT "catalog"."schema"."table"."id", "catalog"."schema"."table"."value" FROM "catalog"."schema"."table""#,
        &custom_dialect,
    );
    test(
        "schema.table",
        r#"SELECT "schema"."table"."id", "schema"."table"."value" FROM "schema"."table""#,
        &custom_dialect,
    );
    test(
        "table",
        r#"SELECT "table"."id", "table"."value" FROM "table""#,
        &custom_dialect,
    );
}

#[test]
fn test_table_scan_with_none_projection_in_plan_to_sql() {
    fn test(table_name: &str, expected_sql: &str) {
        let schema = Schema::new(vec![
            Field::new("id", DataType::Utf8, false),
            Field::new("value", DataType::Utf8, false),
        ]);

        let plan = table_scan(Some(table_name), &schema, None)
            .unwrap()
            .build()
            .unwrap();
        let sql = plan_to_sql(&plan).unwrap();
        assert_eq!(sql.to_string(), expected_sql)
    }

    test(
        "catalog.schema.table",
        r#"SELECT * FROM "catalog"."schema"."table""#,
    );
    test("schema.table", r#"SELECT * FROM "schema"."table""#);
    test("table", r#"SELECT * FROM "table""#);
}

#[test]
fn test_table_scan_with_empty_projection_in_plan_to_sql() {
    fn test(table_name: &str, expected_sql: &str) {
        let schema = Schema::new(vec![
            Field::new("id", DataType::Utf8, false),
            Field::new("value", DataType::Utf8, false),
        ]);

        let plan = table_scan(Some(table_name), &schema, Some(vec![]))
            .unwrap()
            .build()
            .unwrap();
        let sql = plan_to_sql(&plan).unwrap();
        assert_eq!(sql.to_string(), expected_sql)
    }

    test(
        "catalog.schema.table",
        r#"SELECT 1 FROM "catalog"."schema"."table""#,
    );
    test("schema.table", r#"SELECT 1 FROM "schema"."table""#);
    test("table", r#"SELECT 1 FROM "table""#);
}

#[test]
fn test_pretty_roundtrip() -> Result<()> {
    let schema = Schema::new(vec![
        Field::new("id", DataType::Utf8, false),
        Field::new("age", DataType::Utf8, false),
    ]);

    let df_schema = DFSchema::try_from(schema)?;

    let context = MockContextProvider {
        state: MockSessionState::default(),
    };
    let sql_to_rel = SqlToRel::new(&context);

    let unparser = Unparser::default().with_pretty(true);

    let sql_to_pretty_unparse = vec![
        ("((id < 5) OR (age = 8))", "id < 5 OR age = 8"),
        ("((id + 5) * (age * 8))", "(id + 5) * age * 8"),
        ("(3 + (5 * 6) * 3)", "3 + 5 * 6 * 3"),
        ("((3 * (5 + 6)) * 3)", "3 * (5 + 6) * 3"),
        ("((3 AND (5 OR 6)) * 3)", "(3 AND (5 OR 6)) * 3"),
        ("((3 + (5 + 6)) * 3)", "(3 + 5 + 6) * 3"),
        ("((3 + (5 + 6)) + 3)", "3 + 5 + 6 + 3"),
        ("3 + 5 + 6 + 3", "3 + 5 + 6 + 3"),
        ("3 + (5 + (6 + 3))", "3 + 5 + 6 + 3"),
        ("3 + ((5 + 6) + 3)", "3 + 5 + 6 + 3"),
        ("(3 + 5) + (6 + 3)", "3 + 5 + 6 + 3"),
        ("((3 + 5) + (6 + 3))", "3 + 5 + 6 + 3"),
        (
            "((id > 10) OR (age BETWEEN 10 AND 20))",
            "id > 10 OR age BETWEEN 10 AND 20",
        ),
        (
            "((id > 10) * (age BETWEEN 10 AND 20))",
            "(id > 10) * (age BETWEEN 10 AND 20)",
        ),
        ("id - (age - 8)", "id - (age - 8)"),
        ("((id - age) - 8)", "id - age - 8"),
        ("(id OR (age - 8))", "id OR age - 8"),
        ("(id / (age - 8))", "id / (age - 8)"),
        ("((id / age) * 8)", "id / age * 8"),
        ("((age + 10) < 20) IS TRUE", "(age + 10 < 20) IS TRUE"),
        (
            "(20 > (age + 5)) IS NOT FALSE",
            "(20 > age + 5) IS NOT FALSE",
        ),
        ("(true AND false) IS FALSE", "(true AND false) IS FALSE"),
        ("true AND (false IS FALSE)", "true AND false IS FALSE"),
    ];

    for (sql, pretty) in sql_to_pretty_unparse.iter() {
        let sql_expr = Parser::new(&GenericDialect {})
            .try_with_sql(sql)?
            .parse_expr()?;
        let expr =
            sql_to_rel.sql_to_expr(sql_expr, &df_schema, &mut PlannerContext::new())?;
        let round_trip_sql = unparser.expr_to_sql(&expr)?.to_string();
        assert_eq!(pretty.to_string(), round_trip_sql);

        // verify that the pretty string parses to the same underlying Expr
        let pretty_sql_expr = Parser::new(&GenericDialect {})
            .try_with_sql(pretty)?
            .parse_expr()?;

        let pretty_expr = sql_to_rel.sql_to_expr(
            pretty_sql_expr,
            &df_schema,
            &mut PlannerContext::new(),
        )?;

        assert_eq!(expr.to_string(), pretty_expr.to_string());
    }

    Ok(())
}

fn sql_round_trip<D>(dialect: D, query: &str, expect: &str)
where
    D: Dialect,
{
    let statement = Parser::new(&dialect)
        .try_with_sql(query)
        .unwrap()
        .parse_statement()
        .unwrap();

    let context = MockContextProvider {
        state: MockSessionState::default()
            .with_aggregate_function(sum_udaf())
            .with_aggregate_function(max_udaf())
            .with_aggregate_function(grouping_udaf())
            .with_window_function(rank_udwf())
            .with_scalar_function(Arc::new(unicode::substr().as_ref().clone()))
            .with_scalar_function(make_array_udf()),
    };
    let sql_to_rel = SqlToRel::new(&context);
    let plan = sql_to_rel.sql_statement_to_plan(statement).unwrap();

    let roundtrip_statement = plan_to_sql(&plan).unwrap();
    assert_eq!(roundtrip_statement.to_string(), expect);
}

#[test]
fn test_table_scan_alias() -> Result<()> {
    let schema = Schema::new(vec![
        Field::new("id", DataType::Utf8, false),
        Field::new("age", DataType::Utf8, false),
    ]);

    let plan = table_scan(Some("t1"), &schema, None)?
        .project(vec![col("id")])?
        .alias("a")?
        .build()?;
    let sql = plan_to_sql(&plan)?;
    assert_eq!(sql.to_string(), "SELECT * FROM (SELECT t1.id FROM t1) AS a");

    let plan = table_scan(Some("t1"), &schema, None)?
        .project(vec![col("id")])?
        .alias("a")?
        .build()?;

    let sql = plan_to_sql(&plan)?;
    assert_eq!(sql.to_string(), "SELECT * FROM (SELECT t1.id FROM t1) AS a");

    let plan = table_scan(Some("t1"), &schema, None)?
        .filter(col("id").gt(lit(5)))?
        .project(vec![col("id")])?
        .alias("a")?
        .build()?;
    let sql = plan_to_sql(&plan)?;
    assert_eq!(
        sql.to_string(),
        "SELECT * FROM (SELECT t1.id FROM t1 WHERE (t1.id > 5)) AS a"
    );

    let table_scan_with_two_filter = table_scan_with_filters(
        Some("t1"),
        &schema,
        None,
        vec![col("id").gt(lit(1)), col("age").lt(lit(2))],
    )?
    .project(vec![col("id")])?
    .alias("a")?
    .build()?;
    let table_scan_with_two_filter = plan_to_sql(&table_scan_with_two_filter)?;
    assert_eq!(
        table_scan_with_two_filter.to_string(),
        "SELECT a.id FROM t1 AS a WHERE ((a.id > 1) AND (a.age < 2))"
    );

    let table_scan_with_fetch =
        table_scan_with_filter_and_fetch(Some("t1"), &schema, None, vec![], Some(10))?
            .project(vec![col("id")])?
            .alias("a")?
            .build()?;
    let table_scan_with_fetch = plan_to_sql(&table_scan_with_fetch)?;
    assert_eq!(
        table_scan_with_fetch.to_string(),
        "SELECT a.id FROM (SELECT * FROM t1 LIMIT 10) AS a"
    );

    let table_scan_with_pushdown_all = table_scan_with_filter_and_fetch(
        Some("t1"),
        &schema,
        Some(vec![0, 1]),
        vec![col("id").gt(lit(1))],
        Some(10),
    )?
    .project(vec![col("id")])?
    .alias("a")?
    .build()?;
    let table_scan_with_pushdown_all = plan_to_sql(&table_scan_with_pushdown_all)?;
    assert_eq!(
        table_scan_with_pushdown_all.to_string(),
        "SELECT a.id FROM (SELECT a.id, a.age FROM t1 AS a WHERE (a.id > 1) LIMIT 10) AS a"
    );
    Ok(())
}

#[test]
fn test_table_scan_pushdown() -> Result<()> {
    let schema = Schema::new(vec![
        Field::new("id", DataType::Utf8, false),
        Field::new("age", DataType::Utf8, false),
    ]);
    let scan_with_projection =
        table_scan(Some("t1"), &schema, Some(vec![0, 1]))?.build()?;
    let scan_with_projection = plan_to_sql(&scan_with_projection)?;
    assert_eq!(
        scan_with_projection.to_string(),
        "SELECT t1.id, t1.age FROM t1"
    );

    let scan_with_projection = table_scan(Some("t1"), &schema, Some(vec![1]))?.build()?;
    let scan_with_projection = plan_to_sql(&scan_with_projection)?;
    assert_eq!(scan_with_projection.to_string(), "SELECT t1.age FROM t1");

    let scan_with_no_projection = table_scan(Some("t1"), &schema, None)?.build()?;
    let scan_with_no_projection = plan_to_sql(&scan_with_no_projection)?;
    assert_eq!(scan_with_no_projection.to_string(), "SELECT * FROM t1");

    let table_scan_with_projection_alias =
        table_scan(Some("t1"), &schema, Some(vec![0, 1]))?
            .alias("ta")?
            .build()?;
    let table_scan_with_projection_alias =
        plan_to_sql(&table_scan_with_projection_alias)?;
    assert_eq!(
        table_scan_with_projection_alias.to_string(),
        "SELECT ta.id, ta.age FROM t1 AS ta"
    );

    let table_scan_with_projection_alias =
        table_scan(Some("t1"), &schema, Some(vec![1]))?
            .alias("ta")?
            .build()?;
    let table_scan_with_projection_alias =
        plan_to_sql(&table_scan_with_projection_alias)?;
    assert_eq!(
        table_scan_with_projection_alias.to_string(),
        "SELECT ta.age FROM t1 AS ta"
    );

    let table_scan_with_no_projection_alias = table_scan(Some("t1"), &schema, None)?
        .alias("ta")?
        .build()?;
    let table_scan_with_no_projection_alias =
        plan_to_sql(&table_scan_with_no_projection_alias)?;
    assert_eq!(
        table_scan_with_no_projection_alias.to_string(),
        "SELECT * FROM t1 AS ta"
    );

    let query_from_table_scan_with_projection = LogicalPlanBuilder::from(
        table_scan(Some("t1"), &schema, Some(vec![0, 1]))?.build()?,
    )
    .project(vec![col("id"), col("age")])?
    .build()?;
    let query_from_table_scan_with_projection =
        plan_to_sql(&query_from_table_scan_with_projection)?;
    assert_eq!(
        query_from_table_scan_with_projection.to_string(),
        "SELECT t1.id, t1.age FROM t1"
    );

    let query_from_table_scan_with_two_projections = LogicalPlanBuilder::from(
        table_scan(Some("t1"), &schema, Some(vec![0, 1]))?.build()?,
    )
    .project(vec![col("id"), col("age")])?
    .project(vec![wildcard()])?
    .build()?;
    let query_from_table_scan_with_two_projections =
        plan_to_sql(&query_from_table_scan_with_two_projections)?;
    assert_eq!(
        query_from_table_scan_with_two_projections.to_string(),
        "SELECT * FROM (SELECT t1.id, t1.age FROM t1)"
    );

    let table_scan_with_filter = table_scan_with_filters(
        Some("t1"),
        &schema,
        None,
        vec![col("id").gt(col("age"))],
    )?
    .build()?;
    let table_scan_with_filter = plan_to_sql(&table_scan_with_filter)?;
    assert_eq!(
        table_scan_with_filter.to_string(),
        "SELECT * FROM t1 WHERE (t1.id > t1.age)"
    );

    let table_scan_with_two_filter = table_scan_with_filters(
        Some("t1"),
        &schema,
        None,
        vec![col("id").gt(lit(1)), col("age").lt(lit(2))],
    )?
    .build()?;
    let table_scan_with_two_filter = plan_to_sql(&table_scan_with_two_filter)?;
    assert_eq!(
        table_scan_with_two_filter.to_string(),
        "SELECT * FROM t1 WHERE ((t1.id > 1) AND (t1.age < 2))"
    );

    let table_scan_with_filter_alias = table_scan_with_filters(
        Some("t1"),
        &schema,
        None,
        vec![col("id").gt(col("age"))],
    )?
    .alias("ta")?
    .build()?;
    let table_scan_with_filter_alias = plan_to_sql(&table_scan_with_filter_alias)?;
    assert_eq!(
        table_scan_with_filter_alias.to_string(),
        "SELECT * FROM t1 AS ta WHERE (ta.id > ta.age)"
    );

    let table_scan_with_projection_and_filter = table_scan_with_filters(
        Some("t1"),
        &schema,
        Some(vec![0, 1]),
        vec![col("id").gt(col("age"))],
    )?
    .build()?;
    let table_scan_with_projection_and_filter =
        plan_to_sql(&table_scan_with_projection_and_filter)?;
    assert_eq!(
        table_scan_with_projection_and_filter.to_string(),
        "SELECT t1.id, t1.age FROM t1 WHERE (t1.id > t1.age)"
    );

    let table_scan_with_projection_and_filter = table_scan_with_filters(
        Some("t1"),
        &schema,
        Some(vec![1]),
        vec![col("id").gt(col("age"))],
    )?
    .build()?;
    let table_scan_with_projection_and_filter =
        plan_to_sql(&table_scan_with_projection_and_filter)?;
    assert_eq!(
        table_scan_with_projection_and_filter.to_string(),
        "SELECT t1.age FROM t1 WHERE (t1.id > t1.age)"
    );

    let table_scan_with_inline_fetch =
        table_scan_with_filter_and_fetch(Some("t1"), &schema, None, vec![], Some(10))?
            .build()?;
    let table_scan_with_inline_fetch = plan_to_sql(&table_scan_with_inline_fetch)?;
    assert_eq!(
        table_scan_with_inline_fetch.to_string(),
        "SELECT * FROM t1 LIMIT 10"
    );

    let table_scan_with_projection_and_inline_fetch = table_scan_with_filter_and_fetch(
        Some("t1"),
        &schema,
        Some(vec![0, 1]),
        vec![],
        Some(10),
    )?
    .build()?;
    let table_scan_with_projection_and_inline_fetch =
        plan_to_sql(&table_scan_with_projection_and_inline_fetch)?;
    assert_eq!(
        table_scan_with_projection_and_inline_fetch.to_string(),
        "SELECT t1.id, t1.age FROM t1 LIMIT 10"
    );

    let table_scan_with_all = table_scan_with_filter_and_fetch(
        Some("t1"),
        &schema,
        Some(vec![0, 1]),
        vec![col("id").gt(col("age"))],
        Some(10),
    )?
    .build()?;
    let table_scan_with_all = plan_to_sql(&table_scan_with_all)?;
    assert_eq!(
        table_scan_with_all.to_string(),
        "SELECT t1.id, t1.age FROM t1 WHERE (t1.id > t1.age) LIMIT 10"
    );

    let table_scan_with_additional_filter = table_scan_with_filters(
        Some("t1"),
        &schema,
        None,
        vec![col("id").gt(col("age"))],
    )?
    .filter(col("id").eq(lit(5)))?
    .build()?;
    let table_scan_with_filter = plan_to_sql(&table_scan_with_additional_filter)?;
    assert_eq!(
        table_scan_with_filter.to_string(),
        "SELECT * FROM t1 WHERE (t1.id = 5) AND (t1.id > t1.age)"
    );

    Ok(())
}

#[test]
fn test_sort_with_push_down_fetch() -> Result<()> {
    let schema = Schema::new(vec![
        Field::new("id", DataType::Utf8, false),
        Field::new("age", DataType::Utf8, false),
    ]);

    let plan = table_scan(Some("t1"), &schema, None)?
        .project(vec![col("id"), col("age")])?
        .sort_with_limit(vec![col("age").sort(true, true)], Some(10))?
        .build()?;

    let sql = plan_to_sql(&plan)?;
    assert_eq!(
        format!("{}", sql),
        "SELECT t1.id, t1.age FROM t1 ORDER BY t1.age ASC NULLS FIRST LIMIT 10"
    );
    Ok(())
}

#[test]
fn test_join_with_table_scan_filters() -> Result<()> {
    let schema_left = Schema::new(vec![
        Field::new("id", DataType::Utf8, false),
        Field::new("name", DataType::Utf8, false),
    ]);

    let schema_right = Schema::new(vec![
        Field::new("id", DataType::Utf8, false),
        Field::new("age", DataType::Utf8, false),
    ]);

    let left_plan = table_scan_with_filters(
        Some("left_table"),
        &schema_left,
        None,
        vec![col("name").like(lit("some_name"))],
    )?
    .alias("left")?
    .build()?;

    let right_plan = table_scan_with_filters(
        Some("right_table"),
        &schema_right,
        None,
        vec![col("age").gt(lit(10))],
    )?
    .build()?;

    let join_plan_with_filter = LogicalPlanBuilder::from(left_plan.clone())
        .join(
            right_plan.clone(),
            datafusion_expr::JoinType::Inner,
            (vec!["left.id"], vec!["right_table.id"]),
            Some(col("left.id").gt(lit(5))),
        )?
        .build()?;

    let sql = plan_to_sql(&join_plan_with_filter)?;

    let expected_sql = r#"SELECT * FROM left_table AS "left" JOIN right_table ON "left".id = right_table.id AND (("left".id > 5) AND ("left"."name" LIKE 'some_name' AND (age > 10)))"#;

    assert_eq!(sql.to_string(), expected_sql);

    let join_plan_no_filter = LogicalPlanBuilder::from(left_plan.clone())
        .join(
            right_plan,
            datafusion_expr::JoinType::Inner,
            (vec!["left.id"], vec!["right_table.id"]),
            None,
        )?
        .build()?;

    let sql = plan_to_sql(&join_plan_no_filter)?;

    let expected_sql = r#"SELECT * FROM left_table AS "left" JOIN right_table ON "left".id = right_table.id AND ("left"."name" LIKE 'some_name' AND (age > 10))"#;

    assert_eq!(sql.to_string(), expected_sql);

    let right_plan_with_filter = table_scan_with_filters(
        Some("right_table"),
        &schema_right,
        None,
        vec![col("age").gt(lit(10))],
    )?
    .filter(col("right_table.name").eq(lit("before_join_filter_val")))?
    .build()?;

    let join_plan_multiple_filters = LogicalPlanBuilder::from(left_plan.clone())
        .join(
            right_plan_with_filter,
            datafusion_expr::JoinType::Inner,
            (vec!["left.id"], vec!["right_table.id"]),
            Some(col("left.id").gt(lit(5))),
        )?
        .filter(col("left.name").eq(lit("after_join_filter_val")))?
        .build()?;

    let sql = plan_to_sql(&join_plan_multiple_filters)?;

    let expected_sql = r#"SELECT * FROM left_table AS "left" JOIN right_table ON "left".id = right_table.id AND (("left".id > 5) AND (("left"."name" LIKE 'some_name' AND (right_table."name" = 'before_join_filter_val')) AND (age > 10))) WHERE ("left"."name" = 'after_join_filter_val')"#;

    assert_eq!(sql.to_string(), expected_sql);

    let right_plan_with_filter_schema = table_scan_with_filters(
        Some("right_table"),
        &schema_right,
        None,
        vec![
            col("right_table.age").gt(lit(10)),
            col("right_table.age").lt(lit(11)),
        ],
    )?
    .build()?;
    let right_plan_with_duplicated_filter =
        LogicalPlanBuilder::from(right_plan_with_filter_schema.clone())
            .filter(col("right_table.age").gt(lit(10)))?
            .build()?;

    let join_plan_duplicated_filter = LogicalPlanBuilder::from(left_plan)
        .join(
            right_plan_with_duplicated_filter,
            datafusion_expr::JoinType::Inner,
            (vec!["left.id"], vec!["right_table.id"]),
            Some(col("left.id").gt(lit(5))),
        )?
        .build()?;

    let sql = plan_to_sql(&join_plan_duplicated_filter)?;

    let expected_sql = r#"SELECT * FROM left_table AS "left" JOIN right_table ON "left".id = right_table.id AND (("left".id > 5) AND (("left"."name" LIKE 'some_name' AND (right_table.age > 10)) AND (right_table.age < 11)))"#;

    assert_eq!(sql.to_string(), expected_sql);

    Ok(())
}

#[test]
fn test_interval_lhs_eq() {
    sql_round_trip(
        GenericDialect {},
        "select interval '2 seconds' = interval '2 seconds'",
        "SELECT (INTERVAL '2.000000000 SECS' = INTERVAL '2.000000000 SECS')",
    );
}

#[test]
fn test_interval_lhs_lt() {
    sql_round_trip(
        GenericDialect {},
        "select interval '2 seconds' < interval '2 seconds'",
        "SELECT (INTERVAL '2.000000000 SECS' < INTERVAL '2.000000000 SECS')",
    );
}

#[test]
fn test_without_offset() {
    sql_round_trip(MySqlDialect {}, "select 1", "SELECT 1");
}

#[test]
fn test_with_offset0() {
    sql_round_trip(MySqlDialect {}, "select 1 offset 0", "SELECT 1 OFFSET 0");
}

#[test]
fn test_with_offset95() {
    sql_round_trip(MySqlDialect {}, "select 1 offset 95", "SELECT 1 OFFSET 95");
}

#[test]
fn test_order_by_to_sql() {
    // order by aggregation function
    sql_round_trip(
        GenericDialect {},
        r#"SELECT id, first_name, SUM(id) FROM person GROUP BY id, first_name ORDER BY SUM(id) ASC, first_name DESC, id, first_name LIMIT 10"#,
        r#"SELECT person.id, person.first_name, sum(person.id) FROM person GROUP BY person.id, person.first_name ORDER BY sum(person.id) ASC NULLS LAST, person.first_name DESC NULLS FIRST, person.id ASC NULLS LAST, person.first_name ASC NULLS LAST LIMIT 10"#,
    );

    // order by aggregation function alias
    sql_round_trip(
        GenericDialect {},
        r#"SELECT id, first_name, SUM(id) as total_sum FROM person GROUP BY id, first_name ORDER BY total_sum ASC, first_name DESC, id, first_name LIMIT 10"#,
        r#"SELECT person.id, person.first_name, sum(person.id) AS total_sum FROM person GROUP BY person.id, person.first_name ORDER BY total_sum ASC NULLS LAST, person.first_name DESC NULLS FIRST, person.id ASC NULLS LAST, person.first_name ASC NULLS LAST LIMIT 10"#,
    );

    // order by scalar function from projection
    sql_round_trip(
        GenericDialect {},
        r#"SELECT id, first_name, substr(first_name,0,5) FROM person ORDER BY id, substr(first_name,0,5)"#,
        r#"SELECT person.id, person.first_name, substr(person.first_name, 0, 5) FROM person ORDER BY person.id ASC NULLS LAST, substr(person.first_name, 0, 5) ASC NULLS LAST"#,
    );
}

#[test]
fn test_aggregation_to_sql() {
    sql_round_trip(
        GenericDialect {},
        r#"SELECT id, first_name,
        SUM(id) AS total_sum,
        SUM(id) OVER (PARTITION BY first_name ROWS BETWEEN 5 PRECEDING AND 2 FOLLOWING) AS moving_sum,
        MAX(SUM(id)) OVER (PARTITION BY first_name ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS max_total,
        rank() OVER (PARTITION BY grouping(id) + grouping(age), CASE WHEN grouping(age) = 0 THEN id END ORDER BY sum(id) DESC) AS rank_within_parent_1,
        rank() OVER (PARTITION BY grouping(age) + grouping(id), CASE WHEN (CAST(grouping(age) AS BIGINT) = 0) THEN id END ORDER BY sum(id) DESC) AS rank_within_parent_2
        FROM person
        GROUP BY id, first_name;"#,
        r#"SELECT person.id, person.first_name,
sum(person.id) AS total_sum, sum(person.id) OVER (PARTITION BY person.first_name ROWS BETWEEN 5 PRECEDING AND 2 FOLLOWING) AS moving_sum,
max(sum(person.id)) OVER (PARTITION BY person.first_name ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS max_total,
rank() OVER (PARTITION BY (grouping(person.id) + grouping(person.age)), CASE WHEN (grouping(person.age) = 0) THEN person.id END ORDER BY sum(person.id) DESC NULLS FIRST RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS rank_within_parent_1,
rank() OVER (PARTITION BY (grouping(person.age) + grouping(person.id)), CASE WHEN (CAST(grouping(person.age) AS BIGINT) = 0) THEN person.id END ORDER BY sum(person.id) DESC NULLS FIRST RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS rank_within_parent_2
FROM person
GROUP BY person.id, person.first_name"#.replace("\n", " ").as_str(),
    );
}

#[test]
fn test_unnest_to_sql() {
    sql_round_trip(
        GenericDialect {},
        r#"SELECT unnest(array_col) as u1, struct_col, array_col FROM unnest_table WHERE array_col != NULL ORDER BY struct_col, array_col"#,
        r#"SELECT UNNEST(unnest_table.array_col) AS u1, unnest_table.struct_col, unnest_table.array_col FROM unnest_table WHERE (unnest_table.array_col <> NULL) ORDER BY unnest_table.struct_col ASC NULLS LAST, unnest_table.array_col ASC NULLS LAST"#,
    );

    sql_round_trip(
        GenericDialect {},
        r#"SELECT unnest(make_array(1, 2, 2, 5, NULL)) as u1"#,
        r#"SELECT UNNEST([1, 2, 2, 5, NULL]) AS u1"#,
    );
}

#[test]
fn test_join_with_no_conditions() {
    sql_round_trip(
        GenericDialect {},
        "SELECT * FROM j1 JOIN j2",
        "SELECT * FROM j1 CROSS JOIN j2",
    );
    sql_round_trip(
        GenericDialect {},
        "SELECT * FROM j1 CROSS JOIN j2",
        "SELECT * FROM j1 CROSS JOIN j2",
    );
}

#[derive(Debug, PartialEq, Eq, Hash, PartialOrd)]
struct MockUserDefinedLogicalPlan {
    input: LogicalPlan,
}

impl UserDefinedLogicalNodeCore for MockUserDefinedLogicalPlan {
    fn name(&self) -> &str {
        "MockUserDefinedLogicalPlan"
    }

    fn inputs(&self) -> Vec<&LogicalPlan> {
        vec![&self.input]
    }

    fn schema(&self) -> &DFSchemaRef {
        self.input.schema()
    }

    fn expressions(&self) -> Vec<Expr> {
        vec![]
    }

    fn fmt_for_explain(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "MockUserDefinedLogicalPlan")
    }

    fn with_exprs_and_inputs(
        &self,
        _exprs: Vec<Expr>,
        inputs: Vec<LogicalPlan>,
    ) -> Result<Self> {
        Ok(MockUserDefinedLogicalPlan {
            input: inputs.into_iter().next().unwrap(),
        })
    }
}

struct MockStatementUnparser {}

impl UserDefinedLogicalNodeUnparser for MockStatementUnparser {
    fn unparse_to_statement(
        &self,
        node: &dyn UserDefinedLogicalNode,
        unparser: &Unparser,
    ) -> Result<UnparseToStatementResult> {
        if let Some(plan) = node.as_any().downcast_ref::<MockUserDefinedLogicalPlan>() {
            let input = unparser.plan_to_sql(&plan.input)?;
            Ok(UnparseToStatementResult::Modified(input))
        } else {
            Ok(UnparseToStatementResult::Unmodified)
        }
    }
}

struct UnusedUnparser {}

impl UserDefinedLogicalNodeUnparser for UnusedUnparser {
    fn unparse(
        &self,
        _node: &dyn UserDefinedLogicalNode,
        _unparser: &Unparser,
        _query: &mut Option<&mut QueryBuilder>,
        _select: &mut Option<&mut SelectBuilder>,
        _relation: &mut Option<&mut RelationBuilder>,
    ) -> Result<UnparseWithinStatementResult> {
        panic!("This should not be called");
    }

    fn unparse_to_statement(
        &self,
        _node: &dyn UserDefinedLogicalNode,
        _unparser: &Unparser,
    ) -> Result<UnparseToStatementResult> {
        panic!("This should not be called");
    }
}

#[test]
fn test_unparse_extension_to_statement() -> Result<()> {
    let dialect = GenericDialect {};
    let statement = Parser::new(&dialect)
        .try_with_sql("SELECT * FROM j1")?
        .parse_statement()?;
    let state = MockSessionState::default();
    let context = MockContextProvider { state };
    let sql_to_rel = SqlToRel::new(&context);
    let plan = sql_to_rel.sql_statement_to_plan(statement)?;

    let extension = MockUserDefinedLogicalPlan { input: plan };
    let extension = LogicalPlan::Extension(Extension {
        node: Arc::new(extension),
    });
    let unparser = Unparser::default().with_extension_unparsers(vec![
        Arc::new(MockStatementUnparser {}),
        Arc::new(UnusedUnparser {}),
    ]);
    let sql = unparser.plan_to_sql(&extension)?;
    let expected = "SELECT * FROM j1";
    assert_eq!(sql.to_string(), expected);

    if let Some(err) = plan_to_sql(&extension).err() {
        assert_contains!(
            err.to_string(),
            "This feature is not implemented: Unsupported extension node: MockUserDefinedLogicalPlan");
    } else {
        panic!("Expected error");
    }
    Ok(())
}

struct MockSqlUnparser {}

impl UserDefinedLogicalNodeUnparser for MockSqlUnparser {
    fn unparse(
        &self,
        node: &dyn UserDefinedLogicalNode,
        unparser: &Unparser,
        _query: &mut Option<&mut QueryBuilder>,
        _select: &mut Option<&mut SelectBuilder>,
        relation: &mut Option<&mut RelationBuilder>,
    ) -> Result<UnparseWithinStatementResult> {
        if let Some(plan) = node.as_any().downcast_ref::<MockUserDefinedLogicalPlan>() {
            let Statement::Query(input) = unparser.plan_to_sql(&plan.input)? else {
                return Ok(UnparseWithinStatementResult::Unmodified);
            };
            let mut derived_builder = DerivedRelationBuilder::default();
            derived_builder.subquery(input);
            derived_builder.lateral(false);
            if let Some(rel) = relation {
                rel.derived(derived_builder);
            }
        }
        Ok(UnparseWithinStatementResult::Modified)
    }
}

#[test]
fn test_unparse_extension_to_sql() -> Result<()> {
    let dialect = GenericDialect {};
    let statement = Parser::new(&dialect)
        .try_with_sql("SELECT * FROM j1")?
        .parse_statement()?;
    let state = MockSessionState::default();
    let context = MockContextProvider { state };
    let sql_to_rel = SqlToRel::new(&context);
    let plan = sql_to_rel.sql_statement_to_plan(statement)?;

    let extension = MockUserDefinedLogicalPlan { input: plan };
    let extension = LogicalPlan::Extension(Extension {
        node: Arc::new(extension),
    });

    let plan = LogicalPlanBuilder::from(extension)
        .project(vec![col("j1_id").alias("user_id")])?
        .build()?;
    let unparser = Unparser::default().with_extension_unparsers(vec![
        Arc::new(MockSqlUnparser {}),
        Arc::new(UnusedUnparser {}),
    ]);
    let sql = unparser.plan_to_sql(&plan)?;
    let expected = "SELECT j1.j1_id AS user_id FROM (SELECT * FROM j1)";
    assert_eq!(sql.to_string(), expected);

    if let Some(err) = plan_to_sql(&plan).err() {
        assert_contains!(
            err.to_string(),
            "This feature is not implemented: Unsupported extension node: MockUserDefinedLogicalPlan"
        );
    } else {
        panic!("Expected error")
    }
    Ok(())
}

#[test]
fn test_unparse_optimized_multi_union() -> Result<()> {
    let unparser = Unparser::default();

    let schema = Schema::new(vec![
        Field::new("x", DataType::Int32, false),
        Field::new("y", DataType::Utf8, false),
    ]);

    let dfschema = Arc::new(DFSchema::try_from(schema)?);

    let empty = LogicalPlan::EmptyRelation(EmptyRelation {
        produce_one_row: true,
        schema: dfschema.clone(),
    });

    let plan = LogicalPlan::Union(Union {
        inputs: vec![
            project(empty.clone(), vec![lit(1).alias("x"), lit("a").alias("y")])?.into(),
            project(empty.clone(), vec![lit(1).alias("x"), lit("b").alias("y")])?.into(),
            project(empty.clone(), vec![lit(2).alias("x"), lit("a").alias("y")])?.into(),
            project(empty.clone(), vec![lit(2).alias("x"), lit("c").alias("y")])?.into(),
        ],
        schema: dfschema.clone(),
    });

    let sql = "SELECT 1 AS x, 'a' AS y UNION ALL SELECT 1 AS x, 'b' AS y UNION ALL SELECT 2 AS x, 'a' AS y UNION ALL SELECT 2 AS x, 'c' AS y";

    assert_eq!(unparser.plan_to_sql(&plan)?.to_string(), sql);

    let plan = LogicalPlan::Union(Union {
        inputs: vec![project(
            empty.clone(),
            vec![lit(1).alias("x"), lit("a").alias("y")],
        )?
        .into()],
        schema: dfschema.clone(),
    });

    if let Some(err) = plan_to_sql(&plan).err() {
        assert_contains!(err.to_string(), "UNION operator requires at least 2 inputs");
    } else {
        panic!("Expected error")
    }

    Ok(())
}
