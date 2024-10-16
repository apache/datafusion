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

use std::sync::Arc;
use std::vec;

use arrow_schema::*;
use datafusion_common::{DFSchema, Result, TableReference};
use datafusion_expr::test::function_stub::{count_udaf, max_udaf, min_udaf, sum_udaf};
use datafusion_expr::{col, lit, table_scan, wildcard, LogicalPlanBuilder};
use datafusion_functions::unicode;
use datafusion_functions_aggregate::grouping::grouping_udaf;
use datafusion_sql::planner::{ContextProvider, PlannerContext, SqlToRel};
use datafusion_sql::unparser::dialect::{
    DefaultDialect as UnparserDefaultDialect, Dialect as UnparserDialect,
    MySqlDialect as UnparserMySqlDialect, SqliteDialect,
};
use datafusion_sql::unparser::{expr_to_sql, plan_to_sql, Unparser};

use crate::common::{MockContextProvider, MockSessionState};
use datafusion_expr::builder::{
    table_scan_with_filter_and_fetch, table_scan_with_filters,
};
use datafusion_functions::core::planner::CoreFunctionPlanner;
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

        Ok(format!("{}", ast))
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
            r#"SELECT id, first_name,
            SUM(id) AS total_sum,
            SUM(id) OVER (PARTITION BY first_name ROWS BETWEEN 5 PRECEDING AND 2 FOLLOWING) AS moving_sum,
            MAX(SUM(id)) OVER (PARTITION BY first_name ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS max_total
            FROM person GROUP BY id, first_name"#,
            "SELECT id, first_name, last_name, SUM(id) AS total_sum FROM person GROUP BY ROLLUP(id, first_name, last_name)",
            "WITH w1 AS (SELECT 'a' as col), w2 AS (SELECT 'b' as col), w3 as (SELECT 'c' as col) SELECT * FROM w1 UNION ALL SELECT * FROM w2 UNION ALL SELECT * FROM w3",
            "WITH w1 AS (SELECT 'a' as col), w2 AS (SELECT 'b' as col), w3 as (SELECT 'c' as col), w4 as (SELECT 'd' as col) SELECT * FROM w1 UNION ALL SELECT * FROM w2 UNION ALL SELECT * FROM w3 UNION ALL SELECT * FROM w4",
            "WITH w1 AS (SELECT 'a' as col), w2 AS (SELECT 'b' as col) SELECT * FROM w1 JOIN w2 ON w1.col = w2.col UNION ALL SELECT * FROM w1 JOIN w2 ON w1.col = w2.col UNION ALL SELECT * FROM w1 JOIN w2 ON w1.col = w2.col",
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
            .with_aggregate_function(sum_udaf())
            .with_aggregate_function(count_udaf())
            .with_aggregate_function(max_udaf())
            .with_expr_planner(Arc::new(CoreFunctionPlanner::default()));
        let context = MockContextProvider { state };
        let sql_to_rel = SqlToRel::new(&context);
        let plan = sql_to_rel.sql_statement_to_plan(statement).unwrap();

        let roundtrip_statement = plan_to_sql(&plan)?;

        let actual = format!("{}", &roundtrip_statement);
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

    let actual = format!("{}", &roundtrip_statement);
    println!("roundtrip sql: {actual}");
    println!("plan {}", plan.display_indent());

    let plan_roundtrip = sql_to_rel
        .sql_statement_to_plan(roundtrip_statement)
        .unwrap();

    let expected = "Projection: j1.j1_id, j2.j2_string\
        \n  Inner Join:  Filter: Boolean(true)\
        \n    TableScan: j1\
        \n    TableScan: j2";

    assert_eq!(format!("{plan_roundtrip}"), expected);

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
    ];

    for query in tests {
        let statement = Parser::new(&*query.parser_dialect)
            .try_with_sql(query.sql)?
            .parse_statement()?;

        let state = MockSessionState::default()
            .with_aggregate_function(max_udaf())
            .with_aggregate_function(min_udaf())
            .with_expr_planner(Arc::new(CoreFunctionPlanner::default()));

        let context = MockContextProvider { state };
        let sql_to_rel = SqlToRel::new(&context);
        let plan = sql_to_rel
            .sql_statement_to_plan(statement)
            .unwrap_or_else(|e| panic!("Failed to parse sql: {}\n{e}", query.sql));

        let unparser = Unparser::new(&*query.unparser_dialect);
        let roundtrip_statement = unparser.plan_to_sql(&plan)?;

        let actual = format!("{}", &roundtrip_statement);
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

    let expected = "Projection: UNNEST(unnest_table.struct_col).field1, UNNEST(unnest_table.struct_col).field2, UNNEST(unnest_table.array_col), unnest_table.struct_col, unnest_table.array_col\
    \n  Unnest: lists[UNNEST(unnest_table.array_col)] structs[UNNEST(unnest_table.struct_col)]\
    \n    Projection: unnest_table.struct_col AS UNNEST(unnest_table.struct_col), unnest_table.array_col AS UNNEST(unnest_table.array_col), unnest_table.struct_col, unnest_table.array_col\
    \n      TableScan: unnest_table";

    assert_eq!(format!("{plan}"), expected);

    Ok(())
}

#[test]
fn test_table_references_in_plan_to_sql() {
    fn test(table_name: &str, expected_sql: &str) {
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
        let sql = plan_to_sql(&plan).unwrap();

        assert_eq!(format!("{}", sql), expected_sql)
    }

    test("catalog.schema.table", "SELECT catalog.\"schema\".\"table\".id, catalog.\"schema\".\"table\".\"value\" FROM catalog.\"schema\".\"table\"");
    test("schema.table", "SELECT \"schema\".\"table\".id, \"schema\".\"table\".\"value\" FROM \"schema\".\"table\"");
    test(
        "table",
        "SELECT \"table\".id, \"table\".\"value\" FROM \"table\"",
    );
}

#[test]
fn test_table_scan_with_no_projection_in_plan_to_sql() {
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
        assert_eq!(format!("{}", sql), expected_sql)
    }

    test(
        "catalog.schema.table",
        "SELECT * FROM catalog.\"schema\".\"table\"",
    );
    test("schema.table", "SELECT * FROM \"schema\".\"table\"");
    test("table", "SELECT * FROM \"table\"");
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

fn sql_round_trip(query: &str, expect: &str) {
    let statement = Parser::new(&GenericDialect {})
        .try_with_sql(query)
        .unwrap()
        .parse_statement()
        .unwrap();

    let context = MockContextProvider {
        state: MockSessionState::default()
            .with_aggregate_function(sum_udaf())
            .with_aggregate_function(max_udaf())
            .with_aggregate_function(grouping_udaf())
            .with_scalar_function(Arc::new(unicode::substr().as_ref().clone())),
    };
    let sql_to_rel = SqlToRel::new(&context);
    let plan = sql_to_rel.sql_statement_to_plan(statement).unwrap();

    let roundtrip_statement = plan_to_sql(&plan).unwrap();
    assert_eq!(roundtrip_statement.to_string(), expect);
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
        format!("{}", scan_with_projection),
        "SELECT t1.id, t1.age FROM t1"
    );

    let scan_with_no_projection = table_scan(Some("t1"), &schema, None)?.build()?;
    let scan_with_no_projection = plan_to_sql(&scan_with_no_projection)?;
    assert_eq!(format!("{}", scan_with_no_projection), "SELECT * FROM t1");

    let table_scan_with_projection_alias =
        table_scan(Some("t1"), &schema, Some(vec![0, 1]))?
            .alias("ta")?
            .build()?;
    let table_scan_with_projection_alias =
        plan_to_sql(&table_scan_with_projection_alias)?;
    assert_eq!(
        format!("{}", table_scan_with_projection_alias),
        "SELECT ta.id, ta.age FROM t1 AS ta"
    );

    let table_scan_with_no_projection_alias = table_scan(Some("t1"), &schema, None)?
        .alias("ta")?
        .build()?;
    let table_scan_with_no_projection_alias =
        plan_to_sql(&table_scan_with_no_projection_alias)?;
    assert_eq!(
        format!("{}", table_scan_with_no_projection_alias),
        "SELECT * FROM t1 AS ta"
    );

    let query_from_table_scan_with_projection = LogicalPlanBuilder::from(
        table_scan(Some("t1"), &schema, Some(vec![0, 1]))?.build()?,
    )
    .project(vec![wildcard()])?
    .build()?;
    let query_from_table_scan_with_projection =
        plan_to_sql(&query_from_table_scan_with_projection)?;
    assert_eq!(
        format!("{}", query_from_table_scan_with_projection),
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
        format!("{}", table_scan_with_filter),
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
        format!("{}", table_scan_with_two_filter),
        "SELECT * FROM t1 WHERE ((t1.id > 1) AND (t1.age < 2))"
    );

    // TODO: support filters for table scan with alias. Enable this test after #12368 issue is fixed
    // see the issue: https://github.com/apache/datafusion/issues/12368
    // let table_scan_with_filter_alias = table_scan_with_filters(
    //     Some("t1"),
    //     &schema,
    //     None,
    //     vec![col("id").gt(col("age"))],
    // )?.alias("ta")?.build()?;
    // let table_scan_with_filter_alias = plan_to_sql(&table_scan_with_filter_alias)?;
    // assert_eq!(
    //     format!("{}", table_scan_with_filter_alias),
    //     "SELECT * FROM t1 AS ta WHERE (ta.id > ta.age)"
    // );

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
        format!("{}", table_scan_with_projection_and_filter),
        "SELECT t1.id, t1.age FROM t1 WHERE (t1.id > t1.age)"
    );

    let table_scan_with_inline_fetch =
        table_scan_with_filter_and_fetch(Some("t1"), &schema, None, vec![], Some(10))?
            .build()?;
    let table_scan_with_inline_fetch = plan_to_sql(&table_scan_with_inline_fetch)?;
    assert_eq!(
        format!("{}", table_scan_with_inline_fetch),
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
        format!("{}", table_scan_with_projection_and_inline_fetch),
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
        format!("{}", table_scan_with_all),
        "SELECT t1.id, t1.age FROM t1 WHERE (t1.id > t1.age) LIMIT 10"
    );
    Ok(())
}

#[test]
fn test_interval_lhs_eq() {
    sql_round_trip(
        "select interval '2 seconds' = interval '2 seconds'",
        "SELECT (INTERVAL '2.000000000 SECS' = INTERVAL '2.000000000 SECS')",
    );
}

#[test]
fn test_interval_lhs_lt() {
    sql_round_trip(
        "select interval '2 seconds' < interval '2 seconds'",
        "SELECT (INTERVAL '2.000000000 SECS' < INTERVAL '2.000000000 SECS')",
    );
}

#[test]
fn test_order_by_to_sql() {
    // order by aggregation function
    sql_round_trip(
        r#"SELECT id, first_name, SUM(id) FROM person GROUP BY id, first_name ORDER BY SUM(id) ASC, first_name DESC, id, first_name LIMIT 10"#,
        r#"SELECT person.id, person.first_name, sum(person.id) FROM person GROUP BY person.id, person.first_name ORDER BY sum(person.id) ASC NULLS LAST, person.first_name DESC NULLS FIRST, person.id ASC NULLS LAST, person.first_name ASC NULLS LAST LIMIT 10"#,
    );

    // order by aggregation function alias
    sql_round_trip(
        r#"SELECT id, first_name, SUM(id) as total_sum FROM person GROUP BY id, first_name ORDER BY total_sum ASC, first_name DESC, id, first_name LIMIT 10"#,
        r#"SELECT person.id, person.first_name, sum(person.id) AS total_sum FROM person GROUP BY person.id, person.first_name ORDER BY total_sum ASC NULLS LAST, person.first_name DESC NULLS FIRST, person.id ASC NULLS LAST, person.first_name ASC NULLS LAST LIMIT 10"#,
    );

    // order by scalar function from projection
    sql_round_trip(
        r#"SELECT id, first_name, substr(first_name,0,5) FROM person ORDER BY id, substr(first_name,0,5)"#,
        r#"SELECT person.id, person.first_name, substr(person.first_name, 0, 5) FROM person ORDER BY person.id ASC NULLS LAST, substr(person.first_name, 0, 5) ASC NULLS LAST"#,
    );
}

#[test]
fn test_aggregation_to_sql() {
    sql_round_trip(
        r#"SELECT id, first_name,
        SUM(id) AS total_sum,
        SUM(id) OVER (PARTITION BY first_name ROWS BETWEEN 5 PRECEDING AND 2 FOLLOWING) AS moving_sum,
        MAX(SUM(id)) OVER (PARTITION BY first_name ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS max_total,
        rank() OVER (PARTITION BY grouping(id) + grouping(age), CASE WHEN grouping(age) = 0 THEN id END ORDER BY sum(id) DESC) AS rank_within_parent_1,
        rank() OVER (PARTITION BY grouping(age) + grouping(id), CASE WHEN (CAST(grouping(age) AS BIGINT) = 0) THEN id END ORDER BY sum(id) DESC) AS rank_within_parent_2
        FROM person
        GROUP BY id, first_name;"#,
        r#"SELECT person.id, person.first_name,
sum(person.id) AS total_sum, sum(person.id) OVER (PARTITION BY person.first_name ROWS BETWEEN '5' PRECEDING AND '2' FOLLOWING) AS moving_sum,
max(sum(person.id)) OVER (PARTITION BY person.first_name ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS max_total,
RANK() OVER (PARTITION BY (grouping(person.id) + grouping(person.age)), CASE WHEN (grouping(person.age) = 0) THEN person.id END ORDER BY sum(person.id) DESC NULLS FIRST RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS rank_within_parent_1,
RANK() OVER (PARTITION BY (grouping(person.age) + grouping(person.id)), CASE WHEN (CAST(grouping(person.age) AS BIGINT) = 0) THEN person.id END ORDER BY sum(person.id) DESC NULLS FIRST RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS rank_within_parent_2
FROM person
GROUP BY person.id, person.first_name"#.replace("\n", " ").as_str(),
    );
}
