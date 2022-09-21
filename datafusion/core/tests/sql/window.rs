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

/// for window functions without order by the first, last, and nth function call does not make sense
#[tokio::test]
async fn csv_query_window_with_empty_over() -> Result<()> {
    let ctx = SessionContext::new();
    register_aggregate_csv(&ctx).await?;
    let sql = "select \
               c9, \
               count(c5) over (), \
               max(c5) over (), \
               min(c5) over () \
               from aggregate_test_100 \
               order by c9 \
               limit 5";
    let actual = execute_to_batches(&ctx, sql).await;
    let expected = vec![
        "+-----------+------------------------------+----------------------------+----------------------------+",
        "| c9        | COUNT(aggregate_test_100.c5) | MAX(aggregate_test_100.c5) | MIN(aggregate_test_100.c5) |",
        "+-----------+------------------------------+----------------------------+----------------------------+",
        "| 28774375  | 100                          | 2143473091                 | -2141999138                |",
        "| 63044568  | 100                          | 2143473091                 | -2141999138                |",
        "| 141047417 | 100                          | 2143473091                 | -2141999138                |",
        "| 141680161 | 100                          | 2143473091                 | -2141999138                |",
        "| 145294611 | 100                          | 2143473091                 | -2141999138                |",
        "+-----------+------------------------------+----------------------------+----------------------------+",
    ];
    assert_batches_eq!(expected, &actual);
    Ok(())
}

/// for window functions without order by the first, last, and nth function call does not make sense
#[tokio::test]
async fn csv_query_window_with_partition_by() -> Result<()> {
    let ctx = SessionContext::new();
    register_aggregate_csv(&ctx).await?;
    let sql = "select \
               c9, \
               sum(cast(c4 as Int)) over (partition by c3), \
               avg(cast(c4 as Int)) over (partition by c3), \
               count(cast(c4 as Int)) over (partition by c3), \
               max(cast(c4 as Int)) over (partition by c3), \
               min(cast(c4 as Int)) over (partition by c3) \
               from aggregate_test_100 \
               order by c9 \
               limit 5";
    let actual = execute_to_batches(&ctx, sql).await;
    let expected = vec![
        "+-----------+----------------------------+----------------------------+------------------------------+----------------------------+----------------------------+",
        "| c9        | SUM(aggregate_test_100.c4) | AVG(aggregate_test_100.c4) | COUNT(aggregate_test_100.c4) | MAX(aggregate_test_100.c4) | MIN(aggregate_test_100.c4) |",
        "+-----------+----------------------------+----------------------------+------------------------------+----------------------------+----------------------------+",
        "| 28774375  | -16110                     | -16110                     | 1                            | -16110                     | -16110                     |",
        "| 63044568  | 3917                       | 3917                       | 1                            | 3917                       | 3917                       |",
        "| 141047417 | -38455                     | -19227.5                   | 2                            | -16974                     | -21481                     |",
        "| 141680161 | -1114                      | -1114                      | 1                            | -1114                      | -1114                      |",
        "| 145294611 | 15673                      | 15673                      | 1                            | 15673                      | 15673                      |",
        "+-----------+----------------------------+----------------------------+------------------------------+----------------------------+----------------------------+",
    ];
    assert_batches_eq!(expected, &actual);
    Ok(())
}

#[tokio::test]
async fn csv_query_window_with_order_by() -> Result<()> {
    let ctx = SessionContext::new();
    register_aggregate_csv(&ctx).await?;
    let sql = "select \
               c9, \
               sum(c5) over (order by c9), \
               avg(c5) over (order by c9), \
               count(c5) over (order by c9), \
               max(c5) over (order by c9), \
               min(c5) over (order by c9), \
               first_value(c5) over (order by c9), \
               last_value(c5) over (order by c9), \
               nth_value(c5, 2) over (order by c9) \
               from aggregate_test_100 \
               order by c9 \
               limit 5";
    let actual = execute_to_batches(&ctx, sql).await;
    let expected = vec![
        "+-----------+----------------------------+----------------------------+------------------------------+----------------------------+----------------------------+------------------------------------+-----------------------------------+-------------------------------------------+",
        "| c9        | SUM(aggregate_test_100.c5) | AVG(aggregate_test_100.c5) | COUNT(aggregate_test_100.c5) | MAX(aggregate_test_100.c5) | MIN(aggregate_test_100.c5) | FIRST_VALUE(aggregate_test_100.c5) | LAST_VALUE(aggregate_test_100.c5) | NTH_VALUE(aggregate_test_100.c5,Int64(2)) |",
        "+-----------+----------------------------+----------------------------+------------------------------+----------------------------+----------------------------+------------------------------------+-----------------------------------+-------------------------------------------+",
        "| 28774375  | 61035129                   | 61035129                   | 1                            | 61035129                   | 61035129                   | 61035129                           | 61035129                          |                                           |",
        "| 63044568  | -47938237                  | -23969118.5                | 2                            | 61035129                   | -108973366                 | 61035129                           | -108973366                        | -108973366                                |",
        "| 141047417 | 575165281                  | 191721760.33333334         | 3                            | 623103518                  | -108973366                 | 61035129                           | 623103518                         | -108973366                                |",
        "| 141680161 | -1352462829                | -338115707.25              | 4                            | 623103518                  | -1927628110                | 61035129                           | -1927628110                       | -108973366                                |",
        "| 145294611 | -3251637940                | -650327588                 | 5                            | 623103518                  | -1927628110                | 61035129                           | -1899175111                       | -108973366                                |",
        "+-----------+----------------------------+----------------------------+------------------------------+----------------------------+----------------------------+------------------------------------+-----------------------------------+-------------------------------------------+",
    ];
    assert_batches_eq!(expected, &actual);
    Ok(())
}

#[tokio::test]
async fn csv_query_window_with_partition_by_order_by() -> Result<()> {
    let ctx = SessionContext::new();
    register_aggregate_csv(&ctx).await?;
    let sql = "select \
               c9, \
               sum(c5) over (partition by c4 order by c9), \
               avg(c5) over (partition by c4 order by c9), \
               count(c5) over (partition by c4 order by c9), \
               max(c5) over (partition by c4 order by c9), \
               min(c5) over (partition by c4 order by c9), \
               first_value(c5) over (partition by c4 order by c9), \
               last_value(c5) over (partition by c4 order by c9), \
               nth_value(c5, 2) over (partition by c4 order by c9) \
               from aggregate_test_100 \
               order by c9 \
               limit 5";
    let actual = execute_to_batches(&ctx, sql).await;
    let expected = vec![
        "+-----------+----------------------------+----------------------------+------------------------------+----------------------------+----------------------------+------------------------------------+-----------------------------------+-------------------------------------------+",
        "| c9        | SUM(aggregate_test_100.c5) | AVG(aggregate_test_100.c5) | COUNT(aggregate_test_100.c5) | MAX(aggregate_test_100.c5) | MIN(aggregate_test_100.c5) | FIRST_VALUE(aggregate_test_100.c5) | LAST_VALUE(aggregate_test_100.c5) | NTH_VALUE(aggregate_test_100.c5,Int64(2)) |",
        "+-----------+----------------------------+----------------------------+------------------------------+----------------------------+----------------------------+------------------------------------+-----------------------------------+-------------------------------------------+",
        "| 28774375  | 61035129                   | 61035129                   | 1                            | 61035129                   | 61035129                   | 61035129                           | 61035129                          |                                           |",
        "| 63044568  | -108973366                 | -108973366                 | 1                            | -108973366                 | -108973366                 | -108973366                         | -108973366                        |                                           |",
        "| 141047417 | 623103518                  | 623103518                  | 1                            | 623103518                  | 623103518                  | 623103518                          | 623103518                         |                                           |",
        "| 141680161 | -1927628110                | -1927628110                | 1                            | -1927628110                | -1927628110                | -1927628110                        | -1927628110                       |                                           |",
        "| 145294611 | -1899175111                | -1899175111                | 1                            | -1899175111                | -1899175111                | -1899175111                        | -1899175111                       |                                           |",
        "+-----------+----------------------------+----------------------------+------------------------------+----------------------------+----------------------------+------------------------------------+-----------------------------------+-------------------------------------------+"
    ];
    assert_batches_eq!(expected, &actual);
    Ok(())
}

#[tokio::test]
async fn window() -> Result<()> {
    let results = execute_with_partition(
        "SELECT \
        c1, \
        c2, \
        SUM(c2) OVER (), \
        COUNT(c2) OVER (), \
        MAX(c2) OVER (), \
        MIN(c2) OVER (), \
        AVG(c2) OVER () \
        FROM test \
        ORDER BY c1, c2 \
        LIMIT 5",
        4,
    )
    .await?;
    // result in one batch, although e.g. having 2 batches do not change
    // result semantics, having a len=1 assertion upfront keeps surprises
    // at bay
    assert_eq!(results.len(), 1);

    let expected = vec![
        "+----+----+--------------+----------------+--------------+--------------+--------------+",
        "| c1 | c2 | SUM(test.c2) | COUNT(test.c2) | MAX(test.c2) | MIN(test.c2) | AVG(test.c2) |",
        "+----+----+--------------+----------------+--------------+--------------+--------------+",
        "| 0  | 1  | 220          | 40             | 10           | 1            | 5.5          |",
        "| 0  | 2  | 220          | 40             | 10           | 1            | 5.5          |",
        "| 0  | 3  | 220          | 40             | 10           | 1            | 5.5          |",
        "| 0  | 4  | 220          | 40             | 10           | 1            | 5.5          |",
        "| 0  | 5  | 220          | 40             | 10           | 1            | 5.5          |",
        "+----+----+--------------+----------------+--------------+--------------+--------------+",
    ];

    // window function shall respect ordering
    assert_batches_eq!(expected, &results);
    Ok(())
}

#[tokio::test]
async fn window_order_by() -> Result<()> {
    let results = execute_with_partition(
        "SELECT \
        c1, \
        c2, \
        ROW_NUMBER() OVER (ORDER BY c1, c2), \
        FIRST_VALUE(c2) OVER (ORDER BY c1, c2), \
        LAST_VALUE(c2) OVER (ORDER BY c1, c2), \
        NTH_VALUE(c2, 2) OVER (ORDER BY c1, c2), \
        SUM(c2) OVER (ORDER BY c1, c2), \
        COUNT(c2) OVER (ORDER BY c1, c2), \
        MAX(c2) OVER (ORDER BY c1, c2), \
        MIN(c2) OVER (ORDER BY c1, c2), \
        AVG(c2) OVER (ORDER BY c1, c2) \
        FROM test \
        ORDER BY c1, c2 \
        LIMIT 5",
        4,
    )
    .await?;
    // result in one batch, although e.g. having 2 batches do not change
    // result semantics, having a len=1 assertion upfront keeps surprises
    // at bay
    assert_eq!(results.len(), 1);

    let expected = vec![
        "+----+----+--------------+----------------------+---------------------+-----------------------------+--------------+----------------+--------------+--------------+--------------+",
        "| c1 | c2 | ROW_NUMBER() | FIRST_VALUE(test.c2) | LAST_VALUE(test.c2) | NTH_VALUE(test.c2,Int64(2)) | SUM(test.c2) | COUNT(test.c2) | MAX(test.c2) | MIN(test.c2) | AVG(test.c2) |",
        "+----+----+--------------+----------------------+---------------------+-----------------------------+--------------+----------------+--------------+--------------+--------------+",
        "| 0  | 1  | 1            | 1                    | 1                   |                             | 1            | 1              | 1            | 1            | 1            |",
        "| 0  | 2  | 2            | 1                    | 2                   | 2                           | 3            | 2              | 2            | 1            | 1.5          |",
        "| 0  | 3  | 3            | 1                    | 3                   | 2                           | 6            | 3              | 3            | 1            | 2            |",
        "| 0  | 4  | 4            | 1                    | 4                   | 2                           | 10           | 4              | 4            | 1            | 2.5          |",
        "| 0  | 5  | 5            | 1                    | 5                   | 2                           | 15           | 5              | 5            | 1            | 3            |",
        "+----+----+--------------+----------------------+---------------------+-----------------------------+--------------+----------------+--------------+--------------+--------------+",
    ];

    // window function shall respect ordering
    assert_batches_eq!(expected, &results);
    Ok(())
}

#[tokio::test]
async fn window_partition_by() -> Result<()> {
    let results = execute_with_partition(
        "SELECT \
        c1, \
        c2, \
        SUM(c2) OVER (PARTITION BY c2), \
        COUNT(c2) OVER (PARTITION BY c2), \
        MAX(c2) OVER (PARTITION BY c2), \
        MIN(c2) OVER (PARTITION BY c2), \
        AVG(c2) OVER (PARTITION BY c2) \
        FROM test \
        ORDER BY c1, c2 \
        LIMIT 5",
        4,
    )
    .await?;

    let expected = vec![
        "+----+----+--------------+----------------+--------------+--------------+--------------+",
        "| c1 | c2 | SUM(test.c2) | COUNT(test.c2) | MAX(test.c2) | MIN(test.c2) | AVG(test.c2) |",
        "+----+----+--------------+----------------+--------------+--------------+--------------+",
        "| 0  | 1  | 4            | 4              | 1            | 1            | 1            |",
        "| 0  | 2  | 8            | 4              | 2            | 2            | 2            |",
        "| 0  | 3  | 12           | 4              | 3            | 3            | 3            |",
        "| 0  | 4  | 16           | 4              | 4            | 4            | 4            |",
        "| 0  | 5  | 20           | 4              | 5            | 5            | 5            |",
        "+----+----+--------------+----------------+--------------+--------------+--------------+",
    ];

    // window function shall respect ordering
    assert_batches_eq!(expected, &results);
    Ok(())
}

#[tokio::test]
async fn window_partition_by_order_by() -> Result<()> {
    let results = execute_with_partition(
        "SELECT \
        c1, \
        c2, \
        ROW_NUMBER() OVER (PARTITION BY c2 ORDER BY c1), \
        FIRST_VALUE(c2 + c1) OVER (PARTITION BY c2 ORDER BY c1), \
        LAST_VALUE(c2 + c1) OVER (PARTITION BY c2 ORDER BY c1), \
        NTH_VALUE(c2 + c1, 1) OVER (PARTITION BY c2 ORDER BY c1), \
        SUM(c2) OVER (PARTITION BY c2 ORDER BY c1), \
        COUNT(c2) OVER (PARTITION BY c2 ORDER BY c1), \
        MAX(c2) OVER (PARTITION BY c2 ORDER BY c1), \
        MIN(c2) OVER (PARTITION BY c2 ORDER BY c1), \
        AVG(c2) OVER (PARTITION BY c2 ORDER BY c1) \
        FROM test \
        ORDER BY c1, c2 \
        LIMIT 5",
        4,
    )
    .await?;

    let expected = vec![
        "+----+----+--------------+--------------------------------+-------------------------------+---------------------------------------+--------------+----------------+--------------+--------------+--------------+",
        "| c1 | c2 | ROW_NUMBER() | FIRST_VALUE(test.c2 + test.c1) | LAST_VALUE(test.c2 + test.c1) | NTH_VALUE(test.c2 + test.c1,Int64(1)) | SUM(test.c2) | COUNT(test.c2) | MAX(test.c2) | MIN(test.c2) | AVG(test.c2) |",
        "+----+----+--------------+--------------------------------+-------------------------------+---------------------------------------+--------------+----------------+--------------+--------------+--------------+",
        "| 0  | 1  | 1            | 1                              | 1                             | 1                                     | 1            | 1              | 1            | 1            | 1            |",
        "| 0  | 2  | 1            | 2                              | 2                             | 2                                     | 2            | 1              | 2            | 2            | 2            |",
        "| 0  | 3  | 1            | 3                              | 3                             | 3                                     | 3            | 1              | 3            | 3            | 3            |",
        "| 0  | 4  | 1            | 4                              | 4                             | 4                                     | 4            | 1              | 4            | 4            | 4            |",
        "| 0  | 5  | 1            | 5                              | 5                             | 5                                     | 5            | 1              | 5            | 5            | 5            |",
        "+----+----+--------------+--------------------------------+-------------------------------+---------------------------------------+--------------+----------------+--------------+--------------+--------------+",
    ];

    // window function shall respect ordering
    assert_batches_eq!(expected, &results);
    Ok(())
}

#[tokio::test]
async fn window_expr_eliminate() -> Result<()> {
    let ctx = SessionContext::new();

    // window expr is not referenced anywhere, eliminate it.
    let sql = "WITH _sample_data AS (
                SELECT 1 as a, 'aa' AS b
                UNION ALL
                SELECT 3 as a, 'aa' AS b
                UNION ALL
                SELECT 5 as a, 'bb' AS b
                UNION ALL
                SELECT 7 as a, 'bb' AS b
            ), _data2 AS (
                SELECT
                row_number() OVER (PARTITION BY s.b ORDER BY s.a) AS seq,
                s.a,
                s.b
                FROM _sample_data s
            )
            SELECT d.b, MAX(d.a) AS max_a
            FROM _data2 d
            GROUP BY d.b
            ORDER BY d.b;";

    let msg = format!("Creating logical plan for '{}'", sql);
    let plan = ctx
        .create_logical_plan(&("explain ".to_owned() + sql))
        .expect(&msg);
    let state = ctx.state();
    let plan = state.optimize(&plan)?;
    let expected = vec![
        "Explain [plan_type:Utf8, plan:Utf8]",
        "  Sort: #d.b ASC NULLS LAST [b:Utf8, max_a:Int64;N]",
        "    Projection: #d.b, #MAX(d.a) AS max_a [b:Utf8, max_a:Int64;N]",
        "      Aggregate: groupBy=[[#d.b]], aggr=[[MAX(#d.a)]] [b:Utf8, MAX(d.a):Int64;N]",
        "        Projection: #_data2.a, #_data2.b, alias=d [a:Int64, b:Utf8]",
        "          Projection: #s.a, #s.b, alias=_data2 [a:Int64, b:Utf8]",
        "            Projection: #a, #b, alias=s [a:Int64, b:Utf8]",
        "              Union [a:Int64, b:Utf8]",
        "                Projection: Int64(1) AS a, Utf8(\"aa\") AS b [a:Int64, b:Utf8]",
        "                  EmptyRelation []",
        "                Projection: Int64(3) AS a, Utf8(\"aa\") AS b [a:Int64, b:Utf8]",
        "                  EmptyRelation []",
        "                Projection: Int64(5) AS a, Utf8(\"bb\") AS b [a:Int64, b:Utf8]",
        "                  EmptyRelation []",
        "                Projection: Int64(7) AS a, Utf8(\"bb\") AS b [a:Int64, b:Utf8]",
        "                  EmptyRelation []",
    ];
    let formatted = plan.display_indent_schema().to_string();
    let actual: Vec<&str> = formatted.trim().lines().collect();
    assert_eq!(
        expected, actual,
        "\n\nexpected:\n\n{:#?}\nactual:\n\n{:#?}\n\n",
        expected, actual
    );

    let results = execute_to_batches(&ctx, sql).await;
    let expected = vec![
        "+----+-------+",
        "| b  | max_a |",
        "+----+-------+",
        "| aa | 3     |",
        "| bb | 7     |",
        "+----+-------+",
    ];

    assert_batches_eq!(expected, &results);

    // window expr is referenced by the output, keep it
    let sql = "WITH _sample_data AS (
                SELECT 1 as a, 'aa' AS b
                UNION ALL
                SELECT 3 as a, 'aa' AS b
                UNION ALL
                SELECT 5 as a, 'bb' AS b
                UNION ALL
                SELECT 7 as a, 'bb' AS b
            ), _data2 AS (
                SELECT
                row_number() OVER (PARTITION BY s.b ORDER BY s.a) AS seq,
                s.a,
                s.b
                FROM _sample_data s
            )
            SELECT d.b, MAX(d.a) AS max_a, max(d.seq)
            FROM _data2 d
            GROUP BY d.b
            ORDER BY d.b;";

    let plan = ctx
        .create_logical_plan(&("explain ".to_owned() + sql))
        .expect(&msg);
    let plan = state.optimize(&plan)?;
    let expected = vec![
        "Explain [plan_type:Utf8, plan:Utf8]",
        "  Sort: #d.b ASC NULLS LAST [b:Utf8, max_a:Int64;N, MAX(d.seq):UInt64;N]",
        "    Projection: #d.b, #MAX(d.a) AS max_a, #MAX(d.seq) [b:Utf8, max_a:Int64;N, MAX(d.seq):UInt64;N]",
        "      Aggregate: groupBy=[[#d.b]], aggr=[[MAX(#d.a), MAX(#d.seq)]] [b:Utf8, MAX(d.a):Int64;N, MAX(d.seq):UInt64;N]",
        "        Projection: #_data2.seq, #_data2.a, #_data2.b, alias=d [seq:UInt64;N, a:Int64, b:Utf8]",
        "          Projection: #ROW_NUMBER() PARTITION BY [#s.b] ORDER BY [#s.a ASC NULLS LAST] AS seq, #s.a, #s.b, alias=_data2 [seq:UInt64;N, a:Int64, b:Utf8]",
        "            WindowAggr: windowExpr=[[ROW_NUMBER() PARTITION BY [#s.b] ORDER BY [#s.a ASC NULLS LAST]]] [ROW_NUMBER() PARTITION BY [#s.b] ORDER BY [#s.a ASC NULLS LAST]:UInt64;N, a:Int64, b:Utf8]",
        "              Projection: #a, #b, alias=s [a:Int64, b:Utf8]",
        "                Union [a:Int64, b:Utf8]",
        "                  Projection: Int64(1) AS a, Utf8(\"aa\") AS b [a:Int64, b:Utf8]",
        "                    EmptyRelation []",
        "                  Projection: Int64(3) AS a, Utf8(\"aa\") AS b [a:Int64, b:Utf8]",
        "                    EmptyRelation []",
        "                  Projection: Int64(5) AS a, Utf8(\"bb\") AS b [a:Int64, b:Utf8]",
        "                    EmptyRelation []",
        "                  Projection: Int64(7) AS a, Utf8(\"bb\") AS b [a:Int64, b:Utf8]",
        "                    EmptyRelation []",
    ];
    let formatted = plan.display_indent_schema().to_string();
    let actual: Vec<&str> = formatted.trim().lines().collect();
    assert_eq!(
        expected, actual,
        "\n\nexpected:\n\n{:#?}\nactual:\n\n{:#?}\n\n",
        expected, actual
    );

    let results = execute_to_batches(&ctx, sql).await;
    let expected = vec![
        "+----+-------+------------+",
        "| b  | max_a | MAX(d.seq) |",
        "+----+-------+------------+",
        "| aa | 3     | 2          |",
        "| bb | 7     | 2          |",
        "+----+-------+------------+",
    ];

    assert_batches_eq!(expected, &results);
    Ok(())
}

#[tokio::test]
async fn window_in_expression() -> Result<()> {
    let ctx = SessionContext::new();
    let sql = "select 1 - lag(amount, 1) over (order by idx) from (values ('a', 1, 100), ('a', 2, 150)) as t (col1, idx, amount)";
    let actual = execute_to_batches(&ctx, sql).await;
    let expected = vec![
        "+--------------------------------------------------------------------+",
        "| Int64(1) - LAG(t.amount,Int64(1)) ORDER BY [#t.idx ASC NULLS LAST] |",
        "+--------------------------------------------------------------------+",
        "|                                                                    |",
        "| -99                                                                |",
        "+--------------------------------------------------------------------+",
    ];
    assert_batches_eq!(expected, &actual);
    Ok(())
}

#[tokio::test]
async fn window_with_agg_in_expression() -> Result<()> {
    let ctx = SessionContext::new();
    let sql = "select col1, idx, count(*), sum(amount), lag(sum(amount), 1) over (order by idx) as prev_amount, 
        sum(amount) - lag(sum(amount), 1) over (order by idx) as difference from (
        select * from (values ('a', 1, 100), ('a', 2, 150)) as t (col1, idx, amount)
        ) a 
        group by col1, idx;";
    let actual = execute_to_batches(&ctx, sql).await;
    let expected = vec![
        "+------+-----+-----------------+---------------+-------------+------------+",
        "| col1 | idx | COUNT(UInt8(1)) | SUM(a.amount) | prev_amount | difference |",
        "+------+-----+-----------------+---------------+-------------+------------+",
        "| a    | 1   | 1               | 100           |             |            |",
        "| a    | 2   | 1               | 150           | 100         | 50         |",
        "+------+-----+-----------------+---------------+-------------+------------+",
    ];
    assert_batches_eq!(expected, &actual);
    Ok(())
}

fn create_ctx() -> SessionContext {
    // define a schema.
    let schema = Arc::new(Schema::new(vec![Field::new("a", DataType::Float32, false)]));

    // define data in two partitions
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![Arc::new(Float32Array::from_slice(&[
            1.0, 2.0, 3., 4.0, 5., 6., 7., 8.0,
        ]))],
    )
    .unwrap();

    let batch_2 = RecordBatch::try_new(
        schema.clone(),
        vec![Arc::new(Float32Array::from_slice(&[
            9., 10., 11., 12., 13., 14., 15., 16., 17.,
        ]))],
    )
    .unwrap();
    let ctx = SessionContext::new();
    // declare a new context. In spark API, this corresponds to a new spark SQLsession
    // declare a table in memory. In spark API, this corresponds to createDataFrame(...).
    let provider = MemTable::try_new(schema, vec![vec![batch], vec![batch_2]]).unwrap();
    // Register table
    ctx.register_table("t", Arc::new(provider)).unwrap();
    ctx
}

#[tokio::test]
async fn window_frame_empty() -> Result<()> {
    let ctx = create_ctx();

    // execute the query
    let df = ctx
        .sql("SELECT SUM(a) OVER() as summ, COUNT(*) OVER () as cnt FROM t")
        .await?;

    let batches = df.collect().await?;
    let expected = vec![
        "+------+-----+",
        "| summ | cnt |",
        "+------+-----+",
        "| 153  | 17  |",
        "| 153  | 17  |",
        "| 153  | 17  |",
        "| 153  | 17  |",
        "| 153  | 17  |",
        "| 153  | 17  |",
        "| 153  | 17  |",
        "| 153  | 17  |",
        "| 153  | 17  |",
        "| 153  | 17  |",
        "| 153  | 17  |",
        "| 153  | 17  |",
        "| 153  | 17  |",
        "| 153  | 17  |",
        "| 153  | 17  |",
        "| 153  | 17  |",
        "| 153  | 17  |",
        "+------+-----+",
    ];
    assert_batches_eq!(expected, &batches);
    Ok(())
}

#[tokio::test]
async fn window_frame_rows_preceding() -> Result<()> {
    let ctx = create_ctx();

    // execute the query
    let df = ctx
        .sql(
            "SELECT SUM(a) OVER(ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING ) as summ, COUNT(*) OVER(ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING ) as cnt FROM t"
        )
        .await?;

    let batches = df.collect().await?;
    let expected = vec![
        "+------+-----+",
        "| summ | cnt |",
        "+------+-----+",
        "| 3    | 2   |",
        "| 6    | 3   |",
        "| 9    | 3   |",
        "| 12   | 3   |",
        "| 15   | 3   |",
        "| 18   | 3   |",
        "| 21   | 3   |",
        "| 24   | 3   |",
        "| 27   | 3   |",
        "| 30   | 3   |",
        "| 33   | 3   |",
        "| 36   | 3   |",
        "| 39   | 3   |",
        "| 42   | 3   |",
        "| 45   | 3   |",
        "| 48   | 3   |",
        "| 33   | 2   |",
        "+------+-----+",
    ];
    assert_batches_eq!(expected, &batches);
    Ok(())
}
#[tokio::test]
async fn window_frame_rows_preceding_with_partition() -> Result<()> {
    let schema = Arc::new(Schema::new(vec![Field::new("a", DataType::Float32, false)]));

    // define data in two partitions
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![Arc::new(Float32Array::from_slice(&[
            1.0, 2.0, 3., 4.0, 5., 6., 7., 8.0,
        ]))],
    )
    .unwrap();
    // declare a new context. In spark API, this corresponds to a new spark SQLsession
    let ctx = SessionContext::new();
    // declare a table in memory. In spark API, this corresponds to createDataFrame(...).
    let provider = MemTable::try_new(
        schema.clone(),
        vec![vec![batch.clone()], vec![batch.clone()]],
    )
    .unwrap();
    // Register table
    ctx.register_table("t", Arc::new(provider)).unwrap();

    // execute the query
    let df = ctx
        .sql(
            "SELECT SUM(a) OVER(PARTITION BY a ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING ) as summ, COUNT(*) OVER(PARTITION BY a ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING ) as cnt FROM t"
        )
        .await?;

    let batches = df.collect().await?;
    let expected = vec![
        "+------+-----+",
        "| summ | cnt |",
        "+------+-----+",
        "| 2    | 2   |",
        "| 2    | 2   |",
        "| 4    | 2   |",
        "| 4    | 2   |",
        "| 6    | 2   |",
        "| 6    | 2   |",
        "| 8    | 2   |",
        "| 8    | 2   |",
        "| 10   | 2   |",
        "| 10   | 2   |",
        "| 12   | 2   |",
        "| 12   | 2   |",
        "| 14   | 2   |",
        "| 14   | 2   |",
        "| 16   | 2   |",
        "| 16   | 2   |",
        "+------+-----+",
    ];
    assert_batches_eq!(expected, &batches);
    Ok(())
}

#[tokio::test]
async fn window_frame_ranges_preceding_following() -> Result<()> {
    let schema = Arc::new(Schema::new(vec![Field::new("a", DataType::Float32, false)]));

    // define data in two partitions
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![Arc::new(Float32Array::from_slice(&[1.0, 1.0, 2.0, 3.0]))],
    )
    .unwrap();

    let batch_2 = RecordBatch::try_new(
        schema.clone(),
        vec![Arc::new(Float32Array::from_slice(&[5.0, 7.0]))],
    )
    .unwrap();
    // declare a new context. In spark API, this corresponds to a new spark SQLsession
    let ctx = SessionContext::new();

    // declare a table in memory. In spark API, this corresponds to createDataFrame(...).
    let provider = MemTable::try_new(
        schema.clone(),
        vec![vec![batch.clone()], vec![batch_2.clone()]],
    )
    .unwrap();
    // Register table
    ctx.register_table("t", Arc::new(provider)).unwrap();

    // execute the query
    let df = ctx
        .sql(
            "SELECT SUM(a) OVER(ORDER BY a RANGE BETWEEN 1 PRECEDING AND 1 FOLLOWING ) as summ, COUNT(*) OVER(ORDER BY a RANGE BETWEEN 1 PRECEDING AND 1 FOLLOWING ) as cnt FROM t"
        )
        .await?;

    let batches = df.collect().await?;
    let expected = vec![
        "+------+-----+",
        "| summ | cnt |",
        "+------+-----+",
        "| 4    | 3   |",
        "| 4    | 3   |",
        "| 7    | 4   |",
        "| 5    | 2   |",
        "| 5    | 1   |",
        "| 7    | 1   |",
        "+------+-----+",
    ];
    assert_batches_eq!(expected, &batches);
    Ok(())
}

#[tokio::test]
async fn window_frame_empty_inside() -> Result<()> {
    let schema = Arc::new(Schema::new(vec![Field::new("a", DataType::Float32, false)]));

    // define data in two partitions
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![Arc::new(Float32Array::from_slice(&[1.0, 1.0, 2.0, 3.0]))],
    )
    .unwrap();

    let batch_2 = RecordBatch::try_new(
        schema.clone(),
        vec![Arc::new(Float32Array::from_slice(&[5.0, 7.0]))],
    )
    .unwrap();

    // declare a new context. In spark API, this corresponds to a new spark SQLsession
    let ctx = SessionContext::new();
    // declare a table in memory. In spark API, this corresponds to createDataFrame(...).
    let provider = MemTable::try_new(
        schema.clone(),
        vec![vec![batch.clone()], vec![batch_2.clone()]],
    )
    .unwrap();
    // Register table
    ctx.register_table("t", Arc::new(provider)).unwrap();

    // execute the query
    let df = ctx
        .sql("SELECT SUM(a) OVER() as summ, COUNT(*) OVER() as cnt FROM t")
        .await?;

    let batches = df.collect().await?;
    let expected = vec![
        "+------+-----+",
        "| summ | cnt |",
        "+------+-----+",
        "| 19   | 6   |",
        "| 19   | 6   |",
        "| 19   | 6   |",
        "| 19   | 6   |",
        "| 19   | 6   |",
        "| 19   | 6   |",
        "+------+-----+",
    ];
    assert_batches_eq!(expected, &batches);
    Ok(())
}

#[tokio::test]
async fn window_frame_order_by_only() -> Result<()> {
    let schema = Arc::new(Schema::new(vec![Field::new("a", DataType::Float32, false)]));

    // define data in two partitions
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![Arc::new(Float32Array::from_slice(&[1.0, 1.0, 2.0, 3.0]))],
    )
    .unwrap();

    let batch_2 = RecordBatch::try_new(
        schema.clone(),
        vec![Arc::new(Float32Array::from_slice(&[5.0, 7.0]))],
    )
    .unwrap();
    // declare a new context. In spark API, this corresponds to a new spark SQLsession
    let ctx = SessionContext::new();
    // declare a table in memory. In spark API, this corresponds to createDataFrame(...).
    let provider = MemTable::try_new(
        schema.clone(),
        vec![vec![batch.clone()], vec![batch_2.clone()]],
    )
    .unwrap();
    // Register table
    ctx.register_table("t", Arc::new(provider)).unwrap();

    // execute the query
    let df = ctx
        .sql("SELECT SUM(a) OVER(ORDER BY a) as summ, COUNT(*) OVER(ORDER BY a) as cnt FROM t")
        .await?;

    let batches = df.collect().await?;
    let expected = vec![
        "+------+-----+",
        "| summ | cnt |",
        "+------+-----+",
        "| 2    | 2   |",
        "| 2    | 2   |",
        "| 4    | 3   |",
        "| 7    | 4   |",
        "| 12   | 5   |",
        "| 19   | 6   |",
        "+------+-----+",
    ];
    assert_batches_eq!(expected, &batches);
    Ok(())
}

#[tokio::test]
async fn window_frame_ranges_unbounded_preceding_following() -> Result<()> {
    let schema = Arc::new(Schema::new(vec![Field::new("a", DataType::Float32, false)]));

    // define data in two partitions
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![Arc::new(Float32Array::from_slice(&[1.0, 1.0, 2.0, 3.0]))],
    )
    .unwrap();

    let batch_2 = RecordBatch::try_new(
        schema.clone(),
        vec![Arc::new(Float32Array::from_slice(&[5.0, 7.0]))],
    )
    .unwrap();
    // declare a new context. In spark API, this corresponds to a new spark SQLsession
    let ctx = SessionContext::new();
    // declare a table in memory. In spark API, this corresponds to createDataFrame(...).
    let provider = MemTable::try_new(
        schema.clone(),
        vec![vec![batch.clone()], vec![batch_2.clone()]],
    )
    .unwrap();
    // Register table
    ctx.register_table("t", Arc::new(provider)).unwrap();

    // execute the query
    let df = ctx
        .sql(
            "SELECT SUM(a) OVER(ORDER BY a RANGE BETWEEN UNBOUNDED PRECEDING AND 1 FOLLOWING ) as summ, COUNT(*) OVER(ORDER BY a RANGE BETWEEN UNBOUNDED PRECEDING AND 1 FOLLOWING ) as cnt FROM t"
        )
        .await?;

    let batches = df.collect().await?;
    let expected = vec![
        "+------+-----+",
        "| summ | cnt |",
        "+------+-----+",
        "| 4    | 3   |",
        "| 4    | 3   |",
        "| 7    | 4   |",
        "| 7    | 4   |",
        "| 12   | 5   |",
        "| 19   | 6   |",
        "+------+-----+",
    ];
    assert_batches_eq!(expected, &batches);
    Ok(())
}

#[tokio::test]
async fn window_frame_ranges_preceding_and_preceding() -> Result<()> {
    let schema = Arc::new(Schema::new(vec![Field::new("a", DataType::Float32, false)]));

    // define data in two partitions
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![Arc::new(Float32Array::from_slice(&[1.0, 1.0, 2.0, 3.0]))],
    )
    .unwrap();

    let batch_2 = RecordBatch::try_new(
        schema.clone(),
        vec![Arc::new(Float32Array::from_slice(&[5.0, 7.0]))],
    )
    .unwrap();
    // declare a new context. In spark API, this corresponds to a new spark SQLsession
    let ctx = SessionContext::new();
    // declare a table in memory. In spark API, this corresponds to createDataFrame(...).
    let provider = MemTable::try_new(
        schema.clone(),
        vec![vec![batch.clone()], vec![batch_2.clone()]],
    )
    .unwrap();
    // Register table
    ctx.register_table("t", Arc::new(provider)).unwrap();

    // execute the query
    let df = ctx
        .sql(
            "SELECT SUM(a) OVER(ORDER BY a RANGE BETWEEN 3 PRECEDING AND 1 PRECEDING ) as summ, COUNT(*) OVER(ORDER BY a RANGE BETWEEN 3 PRECEDING AND 1 PRECEDING ) as cnt FROM t"
        )
        .await?;

    let batches = df.collect().await?;
    let expected = vec![
        "+------+-----+",
        "| summ | cnt |",
        "+------+-----+",
        "|      |     |",
        "|      |     |",
        "| 2    | 2   |",
        "| 4    | 3   |",
        "| 5    | 2   |",
        "| 5    | 1   |",
        "+------+-----+",
    ];
    assert_batches_eq!(expected, &batches);
    Ok(())
}

#[tokio::test]
async fn window_frame_ranges_unbounded_preceding_following_diff_col() -> Result<()> {
    let schema = Arc::new(Schema::new(vec![
        Field::new("a", DataType::Float32, false),
        Field::new("b", DataType::Float32, false),
    ]));

    // define data in two partitions
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Float32Array::from_slice(&[1.0, 1.0, 2.0, 3.0])),
            Arc::new(Float32Array::from_slice(&[7.0, 5.0, 3.0, 2.0])),
        ],
    )
    .unwrap();

    let batch_2 = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Float32Array::from_slice(&[5.0, 7.0])),
            Arc::new(Float32Array::from_slice(&[1.0, 1.0])),
        ],
    )
    .unwrap();
    // declare a new context. In spark API, this corresponds to a new spark SQLsession
    let ctx = SessionContext::new();
    // declare a table in memory. In spark API, this corresponds to createDataFrame(...).
    let provider =
        MemTable::try_new(schema, vec![vec![batch.clone()], vec![batch_2.clone()]])
            .unwrap();
    // Register table
    ctx.register_table("t", Arc::new(provider)).unwrap();

    // execute the query
    let df = ctx
        .sql(
            "SELECT SUM(a) OVER(ORDER BY b RANGE BETWEEN CURRENT ROW AND 1 FOLLOWING ) as summ, COUNT(*) OVER(ORDER BY b RANGE BETWEEN CURRENT ROW AND 1 FOLLOWING ) as cnt FROM t"
        )
        .await?;

    let batches = df.collect().await?;
    let expected = vec![
        "+------+-----+",
        "| summ | cnt |",
        "+------+-----+",
        "| 15   | 3   |",
        "| 15   | 3   |",
        "| 5    | 2   |",
        "| 2    | 1   |",
        "| 1    | 1   |",
        "| 1    | 1   |",
        "+------+-----+",
    ];
    assert_batches_eq!(expected, &batches);
    Ok(())
}
