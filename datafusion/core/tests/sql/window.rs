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
        "  Sort: d.b ASC NULLS LAST [b:Utf8, max_a:Int64;N]",
        "    Projection: d.b, MAX(d.a) AS max_a [b:Utf8, max_a:Int64;N]",
        "      Aggregate: groupBy=[[d.b]], aggr=[[MAX(d.a)]] [b:Utf8, MAX(d.a):Int64;N]",
        "        Projection: _data2.a, _data2.b, alias=d [a:Int64, b:Utf8]",
        "          Projection: s.a, s.b, alias=_data2 [a:Int64, b:Utf8]",
        "            Projection: a, b, alias=s [a:Int64, b:Utf8]",
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
        "  Sort: d.b ASC NULLS LAST [b:Utf8, max_a:Int64;N, MAX(d.seq):UInt64;N]",
        "    Projection: d.b, MAX(d.a) AS max_a, MAX(d.seq) [b:Utf8, max_a:Int64;N, MAX(d.seq):UInt64;N]",
        "      Aggregate: groupBy=[[d.b]], aggr=[[MAX(d.a), MAX(d.seq)]] [b:Utf8, MAX(d.a):Int64;N, MAX(d.seq):UInt64;N]",
        "        Projection: _data2.seq, _data2.a, _data2.b, alias=d [seq:UInt64;N, a:Int64, b:Utf8]",
        "          Projection: ROW_NUMBER() PARTITION BY [s.b] ORDER BY [s.a ASC NULLS LAST] AS seq, s.a, s.b, alias=_data2 [seq:UInt64;N, a:Int64, b:Utf8]",
        "            WindowAggr: windowExpr=[[ROW_NUMBER() PARTITION BY [s.b] ORDER BY [s.a ASC NULLS LAST]]] [ROW_NUMBER() PARTITION BY [s.b] ORDER BY [s.a ASC NULLS LAST]:UInt64;N, a:Int64, b:Utf8]",
        "              Projection: a, b, alias=s [a:Int64, b:Utf8]",
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
        "+-------------------------------------------------------------------+",
        "| Int64(1) - LAG(t.amount,Int64(1)) ORDER BY [t.idx ASC NULLS LAST] |",
        "+-------------------------------------------------------------------+",
        "|                                                                   |",
        "| -99                                                               |",
        "+-------------------------------------------------------------------+",
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

#[tokio::test]
async fn window_frame_empty() -> Result<()> {
    let ctx = SessionContext::new();
    register_aggregate_csv(&ctx).await?;
    let sql = "SELECT \
               SUM(c3) OVER(),\
               COUNT(*) OVER ()\
               FROM aggregate_test_100 \
               ORDER BY c9 \
               LIMIT 5";
    let actual = execute_to_batches(&ctx, sql).await;
    let expected = vec![
        "+----------------------------+-----------------+",
        "| SUM(aggregate_test_100.c3) | COUNT(UInt8(1)) |",
        "+----------------------------+-----------------+",
        "| 781                        | 100             |",
        "| 781                        | 100             |",
        "| 781                        | 100             |",
        "| 781                        | 100             |",
        "| 781                        | 100             |",
        "+----------------------------+-----------------+",
    ];
    assert_batches_eq!(expected, &actual);
    Ok(())
}

#[tokio::test]
async fn window_frame_rows_preceding() -> Result<()> {
    let ctx = SessionContext::new();
    register_aggregate_csv(&ctx).await?;
    let sql = "SELECT \
               SUM(c4) OVER(ORDER BY c4 ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING),\
               COUNT(*) OVER(ORDER BY c4 ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING)\
               FROM aggregate_test_100 \
               ORDER BY c9 \
               LIMIT 5";
    let actual = execute_to_batches(&ctx, sql).await;
    let expected = vec![
        "+----------------------------+-----------------+",
        "| SUM(aggregate_test_100.c4) | COUNT(UInt8(1)) |",
        "+----------------------------+-----------------+",
        "| -48302                     | 3               |",
        "| 11243                      | 3               |",
        "| -51311                     | 3               |",
        "| -2391                      | 3               |",
        "| 46756                      | 3               |",
        "+----------------------------+-----------------+",
    ];
    assert_batches_eq!(expected, &actual);
    Ok(())
}

#[tokio::test]
async fn window_frame_rows_preceding_with_partition_unique_order_by() -> Result<()> {
    let ctx = SessionContext::new();
    register_aggregate_csv(&ctx).await?;
    let sql = "SELECT \
               SUM(c4) OVER(PARTITION BY c1 ORDER BY c9 ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING),\
               COUNT(*) OVER(PARTITION BY c2 ORDER BY c9 ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING)\
               FROM aggregate_test_100 \
               ORDER BY c9 \
               LIMIT 5";
    let actual = execute_to_batches(&ctx, sql).await;
    let expected = vec![
        "+----------------------------+-----------------+",
        "| SUM(aggregate_test_100.c4) | COUNT(UInt8(1)) |",
        "+----------------------------+-----------------+",
        "| -38611                     | 2               |",
        "| 17547                      | 2               |",
        "| -1301                      | 2               |",
        "| 26638                      | 3               |",
        "| 26861                      | 3               |",
        "+----------------------------+-----------------+",
    ];
    assert_batches_eq!(expected, &actual);
    Ok(())
}
/// The partition by clause conducts sorting according to given partition column by default. If the
/// sorting columns have non unique values, the unstable sorting may produce indeterminate results.
/// Therefore, we are commenting out the following test for now.

// #[tokio::test]
// async fn window_frame_rows_preceding_with_non_unique_partition() -> Result<()> {
//     let ctx = SessionContext::new();
//     register_aggregate_csv(&ctx).await?;
//     let sql = "SELECT \
//                SUM(c4) OVER(PARTITION BY c1 ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING),\
//                COUNT(*) OVER(PARTITION BY c2 ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING)\
//                FROM aggregate_test_100 \
//                ORDER BY c9 \
//                LIMIT 5";
//     let actual = execute_to_batches(&ctx, sql).await;
//     let expected = vec![
//         "+----------------------------+-----------------+",
//         "| SUM(aggregate_test_100.c4) | COUNT(UInt8(1)) |",
//         "+----------------------------+-----------------+",
//         "| -33822                     | 3               |",
//         "| 20808                      | 3               |",
//         "| -29881                     | 3               |",
//         "| -47613                     | 3               |",
//         "| -13474                     | 3               |",
//         "+----------------------------+-----------------+",
//     ];
//     assert_batches_eq!(expected, &actual);
//     Ok(())
// }

#[tokio::test]
async fn window_frame_ranges_preceding_following_desc() -> Result<()> {
    let ctx = SessionContext::new();
    register_aggregate_csv(&ctx).await?;
    let sql = "SELECT \
               SUM(c4) OVER(ORDER BY c2 DESC RANGE BETWEEN 1 PRECEDING AND 1 FOLLOWING),\
               SUM(c3) OVER(ORDER BY c2 DESC RANGE BETWEEN 10000 PRECEDING AND 10000 FOLLOWING),\
               COUNT(*) OVER(ORDER BY c2 DESC RANGE BETWEEN 1 PRECEDING AND 1 FOLLOWING) \
               FROM aggregate_test_100 \
               ORDER BY c9 \
               LIMIT 5";
    let actual = execute_to_batches(&ctx, sql).await;
    let expected = vec![
        "+----------------------------+----------------------------+-----------------+",
        "| SUM(aggregate_test_100.c4) | SUM(aggregate_test_100.c3) | COUNT(UInt8(1)) |",
        "+----------------------------+----------------------------+-----------------+",
        "| 52276                      | 781                        | 56              |",
        "| 260620                     | 781                        | 63              |",
        "| -28623                     | 781                        | 37              |",
        "| 260620                     | 781                        | 63              |",
        "| 260620                     | 781                        | 63              |",
        "+----------------------------+----------------------------+-----------------+",
    ];
    assert_batches_eq!(expected, &actual);
    Ok(())
}

#[tokio::test]
async fn window_frame_order_by_asc_desc_large() -> Result<()> {
    let ctx = SessionContext::new();
    register_aggregate_csv(&ctx).await?;
    let sql = "SELECT
                SUM(c5) OVER (ORDER BY c2 ASC, c6 DESC)
                FROM aggregate_test_100
                LIMIT 5";
    let actual = execute_to_batches(&ctx, sql).await;
    let expected = vec![
        "+----------------------------+",
        "| SUM(aggregate_test_100.c5) |",
        "+----------------------------+",
        "| -1383162419                |",
        "| -3265456275                |",
        "| -3909681744                |",
        "| -5241214934                |",
        "| -4246910946                |",
        "+----------------------------+",
    ];
    assert_batches_eq!(expected, &actual);
    Ok(())
}

#[tokio::test]
async fn window_frame_order_by_desc_large() -> Result<()> {
    let ctx = SessionContext::new();
    register_aggregate_csv(&ctx).await?;
    let sql = "SELECT
                SUM(c5) OVER (ORDER BY c2 DESC, c6 ASC)
                FROM aggregate_test_100
                ORDER BY c9
                LIMIT 5";
    let actual = execute_to_batches(&ctx, sql).await;
    let expected = vec![
        "+----------------------------+",
        "| SUM(aggregate_test_100.c5) |",
        "+----------------------------+",
        "| 11212193439                |",
        "| 22799733943                |",
        "| 2935356871                 |",
        "| 15810962683                |",
        "| 18035025006                |",
        "+----------------------------+",
    ];
    assert_batches_eq!(expected, &actual);
    Ok(())
}

#[tokio::test]
async fn window_frame_order_by_null_timestamp_order_by() -> Result<()> {
    let ctx = SessionContext::new();
    register_aggregate_null_cases_csv(&ctx).await?;
    let sql = "SELECT
                SUM(c1) OVER (ORDER BY c2 DESC)
                FROM null_cases
                LIMIT 5";
    let actual = execute_to_batches(&ctx, sql).await;
    let expected = vec![
        "+--------------------+",
        "| SUM(null_cases.c1) |",
        "+--------------------+",
        "| 962                |",
        "| 962                |",
        "| 962                |",
        "| 962                |",
        "| 962                |",
        "+--------------------+",
    ];
    assert_batches_eq!(expected, &actual);
    Ok(())
}

#[tokio::test]
async fn window_frame_order_by_null_desc() -> Result<()> {
    let ctx = SessionContext::new();
    register_aggregate_null_cases_csv(&ctx).await?;
    let sql = "SELECT
                COUNT(c2) OVER (ORDER BY c1 DESC RANGE BETWEEN 5 PRECEDING AND 3 FOLLOWING)
                FROM null_cases
                LIMIT 5";
    let actual = execute_to_batches(&ctx, sql).await;
    let expected = vec![
        "+----------------------+",
        "| COUNT(null_cases.c2) |",
        "+----------------------+",
        "| 9                    |",
        "| 9                    |",
        "| 9                    |",
        "| 9                    |",
        "| 9                    |",
        "+----------------------+",
    ];
    assert_batches_eq!(expected, &actual);
    Ok(())
}

#[tokio::test]
async fn window_frame_order_by_null_asc() -> Result<()> {
    let ctx = SessionContext::new();
    register_aggregate_null_cases_csv(&ctx).await?;
    let sql = "SELECT
                COUNT(c2) OVER (ORDER BY c1 RANGE BETWEEN 5 PRECEDING AND 3 FOLLOWING)
                FROM null_cases
                ORDER BY c1
                LIMIT 5";
    let actual = execute_to_batches(&ctx, sql).await;
    let expected = vec![
        "+----------------------+",
        "| COUNT(null_cases.c2) |",
        "+----------------------+",
        "| 2                    |",
        "| 2                    |",
        "| 2                    |",
        "| 2                    |",
        "| 5                    |",
        "+----------------------+",
    ];
    assert_batches_eq!(expected, &actual);
    Ok(())
}

#[tokio::test]
async fn window_frame_order_by_null_asc_null_first() -> Result<()> {
    let ctx = SessionContext::new();
    register_aggregate_null_cases_csv(&ctx).await?;
    let sql = "SELECT
                COUNT(c2) OVER (ORDER BY c1 NULLS FIRST RANGE BETWEEN 5 PRECEDING AND 3 FOLLOWING)
                FROM null_cases
                LIMIT 5";
    let actual = execute_to_batches(&ctx, sql).await;
    let expected = vec![
        "+----------------------+",
        "| COUNT(null_cases.c2) |",
        "+----------------------+",
        "| 9                    |",
        "| 9                    |",
        "| 9                    |",
        "| 9                    |",
        "| 9                    |",
        "+----------------------+",
    ];
    assert_batches_eq!(expected, &actual);
    Ok(())
}

#[tokio::test]
async fn window_frame_order_by_null_desc_null_last() -> Result<()> {
    let ctx = SessionContext::new();
    register_aggregate_null_cases_csv(&ctx).await?;
    let sql = "SELECT
                COUNT(c2) OVER (ORDER BY c1 DESC NULLS LAST RANGE BETWEEN 5 PRECEDING AND 3 FOLLOWING)
                FROM null_cases
                LIMIT 5";
    let actual = execute_to_batches(&ctx, sql).await;
    let expected = vec![
        "+----------------------+",
        "| COUNT(null_cases.c2) |",
        "+----------------------+",
        "| 5                    |",
        "| 5                    |",
        "| 5                    |",
        "| 6                    |",
        "| 6                    |",
        "+----------------------+",
    ];
    assert_batches_eq!(expected, &actual);
    Ok(())
}

#[tokio::test]
async fn window_frame_rows_order_by_null() -> Result<()> {
    let ctx = SessionContext::new();
    register_aggregate_null_cases_csv(&ctx).await?;
    let sql = "SELECT
        SUM(c1) OVER (ORDER BY c3 RANGE BETWEEN 10 PRECEDING AND 11 FOLLOWING) as a,
        SUM(c1) OVER (ORDER BY c3 RANGE BETWEEN 10 PRECEDING AND 11 FOLLOWING) as b,
        SUM(c1) OVER (ORDER BY c3 DESC RANGE BETWEEN 10 PRECEDING AND 11 FOLLOWING) as c,
        SUM(c1) OVER (ORDER BY c3 NULLS first RANGE BETWEEN 10 PRECEDING AND 11 FOLLOWING) as d,
        SUM(c1) OVER (ORDER BY c3 DESC NULLS last RANGE BETWEEN 10 PRECEDING AND 11 FOLLOWING) as e,
        SUM(c1) OVER (ORDER BY c3 DESC NULLS first RANGE BETWEEN 10 PRECEDING AND 11 FOLLOWING) as f,
        SUM(c1) OVER (ORDER BY c3 NULLS first RANGE BETWEEN 10 PRECEDING AND 11 FOLLOWING) as g,
        SUM(c1) OVER (ORDER BY c3) as h,
        SUM(c1) OVER (ORDER BY c3 DESC) as i,
        SUM(c1) OVER (ORDER BY c3 NULLS first) as j,
        SUM(c1) OVER (ORDER BY c3 DESC NULLS first) as k,
        SUM(c1) OVER (ORDER BY c3 DESC NULLS last) as l,
        SUM(c1) OVER (ORDER BY c3, c2) as m,
        SUM(c1) OVER (ORDER BY c3, c1 DESC) as n,
        SUM(c1) OVER (ORDER BY c3 DESC, c1) as o,
        SUM(c1) OVER (ORDER BY c3, c1 NULLs first) as p,
        SUM(c1) OVER (ORDER BY c3 RANGE BETWEEN UNBOUNDED PRECEDING AND 11 FOLLOWING) as a1,
        SUM(c1) OVER (ORDER BY c3 RANGE BETWEEN UNBOUNDED PRECEDING AND 11 FOLLOWING) as b1,
        SUM(c1) OVER (ORDER BY c3 DESC RANGE BETWEEN UNBOUNDED PRECEDING AND 11 FOLLOWING) as c1,
        SUM(c1) OVER (ORDER BY c3 NULLS first RANGE BETWEEN UNBOUNDED PRECEDING AND 11 FOLLOWING) as d1,
        SUM(c1) OVER (ORDER BY c3 DESC NULLS last RANGE BETWEEN UNBOUNDED PRECEDING AND 11 FOLLOWING) as e1,
        SUM(c1) OVER (ORDER BY c3 DESC NULLS first RANGE BETWEEN UNBOUNDED PRECEDING AND 11 FOLLOWING) as f1,
        SUM(c1) OVER (ORDER BY c3 NULLS first RANGE BETWEEN UNBOUNDED PRECEDING AND 11 FOLLOWING) as g1,
        SUM(c1) OVER (ORDER BY c3 RANGE BETWEEN UNBOUNDED PRECEDING AND current row) as h1,
        SUM(c1) OVER (ORDER BY c3 RANGE BETWEEN UNBOUNDED PRECEDING AND current row) as j1,
        SUM(c1) OVER (ORDER BY c3 DESC RANGE BETWEEN UNBOUNDED PRECEDING AND current row) as k1,
        SUM(c1) OVER (ORDER BY c3 NULLS first RANGE BETWEEN UNBOUNDED PRECEDING AND current row) as l1,
        SUM(c1) OVER (ORDER BY c3 DESC NULLS last RANGE BETWEEN UNBOUNDED PRECEDING AND current row) as m1,
        SUM(c1) OVER (ORDER BY c3 DESC NULLS first RANGE BETWEEN UNBOUNDED PRECEDING AND current row) as n1,
        SUM(c1) OVER (ORDER BY c3 NULLS first RANGE BETWEEN UNBOUNDED PRECEDING AND current row) as o1,
        SUM(c1) OVER (ORDER BY c3 RANGE BETWEEN current row AND UNBOUNDED FOLLOWING) as h11,
        SUM(c1) OVER (ORDER BY c3 RANGE BETWEEN current row AND UNBOUNDED FOLLOWING) as j11,
        SUM(c1) OVER (ORDER BY c3 DESC RANGE BETWEEN current row AND UNBOUNDED FOLLOWING) as k11,
        SUM(c1) OVER (ORDER BY c3 NULLS first RANGE BETWEEN current row AND UNBOUNDED FOLLOWING) as l11,
        SUM(c1) OVER (ORDER BY c3 DESC NULLS last RANGE BETWEEN current row AND UNBOUNDED FOLLOWING) as m11,
        SUM(c1) OVER (ORDER BY c3 DESC NULLS first RANGE BETWEEN current row AND UNBOUNDED FOLLOWING) as n11,
        SUM(c1) OVER (ORDER BY c3 NULLS first RANGE BETWEEN current row AND UNBOUNDED FOLLOWING) as o11
        FROM null_cases
        ORDER BY c3
        LIMIT 5";
    let actual = execute_to_batches(&ctx, sql).await;
    let expected = vec![
        "+-----+-----+-----+-----+-----+-----+-----+-----+------+-----+------+------+-----+-----+------+-----+-----+-----+------+-----+------+------+-----+-----+-----+------+-----+------+------+-----+------+------+-----+------+-----+-----+------+",
        "| a   | b   | c   | d   | e   | f   | g   | h   | i    | j   | k    | l    | m   | n   | o    | p   | a1  | b1  | c1   | d1  | e1   | f1   | g1  | h1  | j1  | k1   | l1  | m1   | n1   | o1  | h11  | j11  | k11 | l11  | m11 | n11 | o11  |",
        "+-----+-----+-----+-----+-----+-----+-----+-----+------+-----+------+------+-----+-----+------+-----+-----+-----+------+-----+------+------+-----+-----+-----+------+-----+------+------+-----+------+------+-----+------+-----+-----+------+",
        "| 412 | 412 | 339 | 412 | 339 | 339 | 412 |     | 4627 |     | 4627 | 4627 |     |     | 4627 |     | 412 | 412 | 4627 | 412 | 4627 | 4627 | 412 |     |     | 4627 |     | 4627 | 4627 |     | 4627 | 4627 |     | 4627 |     |     | 4627 |",
        "| 488 | 488 | 412 | 488 | 412 | 412 | 488 | 72  | 4627 | 72  | 4627 | 4627 | 72  | 72  | 4627 | 72  | 488 | 488 | 4627 | 488 | 4627 | 4627 | 488 | 72  | 72  | 4627 | 72  | 4627 | 4627 | 72  | 4627 | 4627 | 72  | 4627 | 72  | 72  | 4627 |",
        "| 543 | 543 | 488 | 543 | 488 | 488 | 543 | 96  | 4555 | 96  | 4555 | 4555 | 96  | 96  | 4555 | 96  | 543 | 543 | 4627 | 543 | 4627 | 4627 | 543 | 96  | 96  | 4555 | 96  | 4555 | 4555 | 96  | 4555 | 4555 | 96  | 4555 | 96  | 96  | 4555 |",
        "| 553 | 553 | 543 | 553 | 543 | 543 | 553 | 115 | 4531 | 115 | 4531 | 4531 | 115 | 115 | 4531 | 115 | 553 | 553 | 4627 | 553 | 4627 | 4627 | 553 | 115 | 115 | 4531 | 115 | 4531 | 4531 | 115 | 4531 | 4531 | 115 | 4531 | 115 | 115 | 4531 |",
        "| 553 | 553 | 553 | 553 | 553 | 553 | 553 | 140 | 4512 | 140 | 4512 | 4512 | 140 | 140 | 4512 | 140 | 553 | 553 | 4627 | 553 | 4627 | 4627 | 553 | 140 | 140 | 4512 | 140 | 4512 | 4512 | 140 | 4512 | 4512 | 140 | 4512 | 140 | 140 | 4512 |",
        "+-----+-----+-----+-----+-----+-----+-----+-----+------+-----+------+------+-----+-----+------+-----+-----+-----+------+-----+------+------+-----+-----+-----+------+-----+------+------+-----+------+------+-----+------+-----+-----+------+",
    ];
    assert_batches_eq!(expected, &actual);
    Ok(())
}

#[tokio::test]
async fn window_frame_rows_preceding_with_unique_partition() -> Result<()> {
    let ctx = SessionContext::new();
    register_aggregate_csv(&ctx).await?;
    let sql = "SELECT \
               SUM(c4) OVER(PARTITION BY c1 ORDER BY c9 ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING),\
               COUNT(*) OVER(PARTITION BY c1 ORDER BY c9 ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING)\
               FROM aggregate_test_100 \
               ORDER BY c9 \
               LIMIT 5";
    let actual = execute_to_batches(&ctx, sql).await;
    let expected = vec![
        "+----------------------------+-----------------+",
        "| SUM(aggregate_test_100.c4) | COUNT(UInt8(1)) |",
        "+----------------------------+-----------------+",
        "| -38611                     | 2               |",
        "| 17547                      | 2               |",
        "| -1301                      | 2               |",
        "| 26638                      | 2               |",
        "| 26861                      | 3               |",
        "+----------------------------+-----------------+",
    ];
    assert_batches_eq!(expected, &actual);
    Ok(())
}

#[tokio::test]
async fn window_frame_ranges_preceding_following() -> Result<()> {
    let ctx = SessionContext::new();
    register_aggregate_csv(&ctx).await?;
    let sql = "SELECT \
               SUM(c4) OVER(ORDER BY c2 RANGE BETWEEN 1 PRECEDING AND 1 FOLLOWING),\
               SUM(c3) OVER(ORDER BY c2 RANGE BETWEEN 10000 PRECEDING AND 10000 FOLLOWING),\
               COUNT(*) OVER(ORDER BY c2 RANGE BETWEEN 1 PRECEDING AND 1 FOLLOWING) \
               FROM aggregate_test_100 \
               ORDER BY c9 \
               LIMIT 5";
    let actual = execute_to_batches(&ctx, sql).await;
    let expected = vec![
        "+----------------------------+----------------------------+-----------------+",
        "| SUM(aggregate_test_100.c4) | SUM(aggregate_test_100.c3) | COUNT(UInt8(1)) |",
        "+----------------------------+----------------------------+-----------------+",
        "| 52276                      | 781                        | 56              |",
        "| 260620                     | 781                        | 63              |",
        "| -28623                     | 781                        | 37              |",
        "| 260620                     | 781                        | 63              |",
        "| 260620                     | 781                        | 63              |",
        "+----------------------------+----------------------------+-----------------+",
    ];
    assert_batches_eq!(expected, &actual);
    Ok(())
}

#[tokio::test]
async fn window_frame_ranges_string_check() -> Result<()> {
    let ctx = SessionContext::new();
    register_aggregate_csv(&ctx).await?;
    let sql = "SELECT \
               SUM(LENGTH(c13)) OVER(ORDER BY c13), \
               SUM(LENGTH(c1)) OVER(ORDER BY c1) \
               FROM aggregate_test_100 \
               ORDER BY c9 \
               LIMIT 5";
    let actual = execute_to_batches(&ctx, sql).await;
    let expected = vec![
        "+----------------------------------------------+---------------------------------------------+",
        "| SUM(characterlength(aggregate_test_100.c13)) | SUM(characterlength(aggregate_test_100.c1)) |",
        "+----------------------------------------------+---------------------------------------------+",
        "| 2100                                         | 100                                         |",
        "| 510                                          | 79                                          |",
        "| 1440                                         | 21                                          |",
        "| 1830                                         | 61                                          |",
        "| 2010                                         | 21                                          |",
        "+----------------------------------------------+---------------------------------------------+",
    ];
    assert_batches_eq!(expected, &actual);
    Ok(())
}

#[tokio::test]
async fn window_frame_order_by_unique() -> Result<()> {
    let ctx = SessionContext::new();
    register_aggregate_csv(&ctx).await?;
    let sql = "SELECT \
               SUM(c5) OVER (ORDER BY c5), \
               COUNT(*) OVER (ORDER BY c9) \
               FROM aggregate_test_100 \
               ORDER BY c9 \
               LIMIT 5";
    let actual = execute_to_batches(&ctx, sql).await;
    let expected = vec![
        "+----------------------------+-----------------+",
        "| SUM(aggregate_test_100.c5) | COUNT(UInt8(1)) |",
        "+----------------------------+-----------------+",
        "| -49877765574               | 1               |",
        "| -50025861694               | 2               |",
        "| -45402230071               | 3               |",
        "| -14557735645               | 4               |",
        "| -18365391649               | 5               |",
        "+----------------------------+-----------------+",
    ];
    assert_batches_eq!(expected, &actual);
    Ok(())
}

/// If the sorting columns have non unique values, the unstable sorting may produce
/// indeterminate results. Therefore, we are commenting out the following test for now.
///
// #[tokio::test]
// async fn window_frame_order_by_non_unique() -> Result<()> {
//     let ctx = SessionContext::new();
//     register_aggregate_csv(&ctx).await?;
//     let sql = "SELECT \
//                c2, \
//                c9, \
//                SUM(c5) OVER (ORDER BY c2), \
//                COUNT(*) OVER (ORDER BY c2) \
//                FROM aggregate_test_100 \
//                ORDER BY c2 \
//                LIMIT 5";
//     let actual = execute_to_batches(&ctx, sql).await;
//     let expected = vec![
//         "+----+------------+----------------------------+-----------------+",
//         "| c2 | c9         | SUM(aggregate_test_100.c5) | COUNT(UInt8(1)) |",
//         "+----+------------+----------------------------+-----------------+",
//         "| 1  | 879082834  | -438598674                 | 22              |",
//         "| 1  | 3542840110 | -438598674                 | 22              |",
//         "| 1  | 3275293996 | -438598674                 | 22              |",
//         "| 1  | 774637006  | -438598674                 | 22              |",
//         "| 1  | 4015442341 | -438598674                 | 22              |",
//         "+----+------------+----------------------------+-----------------+",
//     ];
//     assert_batches_eq!(expected, &actual);
//     Ok(())
// }

#[tokio::test]
async fn window_frame_ranges_unbounded_preceding_following() -> Result<()> {
    let ctx = SessionContext::new();
    register_aggregate_csv(&ctx).await?;
    let sql = "SELECT \
               SUM(c2) OVER (ORDER BY c2 RANGE BETWEEN UNBOUNDED PRECEDING AND 1 FOLLOWING) as sum1, \
               COUNT(*) OVER (ORDER BY c2 RANGE BETWEEN UNBOUNDED PRECEDING AND 1 FOLLOWING) as cnt1 \
               FROM aggregate_test_100 \
               ORDER BY c9 \
               LIMIT 5";
    let actual = execute_to_batches(&ctx, sql).await;
    let expected = vec![
        "+------+------+",
        "| sum1 | cnt1 |",
        "+------+------+",
        "| 285  | 100  |",
        "| 123  | 63   |",
        "| 285  | 100  |",
        "| 123  | 63   |",
        "| 123  | 63   |",
        "+------+------+",
    ];
    assert_batches_eq!(expected, &actual);
    Ok(())
}

#[tokio::test]
async fn window_frame_ranges_preceding_and_preceding() -> Result<()> {
    let ctx = SessionContext::new();
    register_aggregate_csv(&ctx).await?;
    let sql = "SELECT \
               SUM(c2) OVER (ORDER BY c2 RANGE BETWEEN 3 PRECEDING AND 1 PRECEDING), \
               COUNT(*) OVER (ORDER BY c2 RANGE BETWEEN 3 PRECEDING AND 1 PRECEDING) \
               FROM aggregate_test_100 \
               ORDER BY c9 \
               LIMIT 5";
    let actual = execute_to_batches(&ctx, sql).await;
    let expected = vec![
        "+----------------------------+-----------------+",
        "| SUM(aggregate_test_100.c2) | COUNT(UInt8(1)) |",
        "+----------------------------+-----------------+",
        "| 123                        | 63              |",
        "| 22                         | 22              |",
        "| 193                        | 64              |",
        "| 22                         | 22              |",
        "| 22                         | 22              |",
        "+----------------------------+-----------------+",
    ];
    assert_batches_eq!(expected, &actual);
    Ok(())
}

#[tokio::test]
async fn window_frame_ranges_unbounded_preceding_following_diff_col() -> Result<()> {
    let ctx = SessionContext::new();
    register_aggregate_csv(&ctx).await?;
    let sql = "SELECT \
               SUM(c2) OVER (ORDER BY c2 RANGE BETWEEN CURRENT ROW AND 1 FOLLOWING), \
               COUNT(*) OVER (ORDER BY c2 RANGE BETWEEN CURRENT ROW AND 1 FOLLOWING) \
               FROM aggregate_test_100 \
               ORDER BY c9 \
               LIMIT 5";
    let actual = execute_to_batches(&ctx, sql).await;
    let expected = vec![
        "+----------------------------+-----------------+",
        "| SUM(aggregate_test_100.c2) | COUNT(UInt8(1)) |",
        "+----------------------------+-----------------+",
        "| 162                        | 37              |",
        "| 101                        | 41              |",
        "| 70                         | 14              |",
        "| 101                        | 41              |",
        "| 101                        | 41              |",
        "+----------------------------+-----------------+",
    ];
    assert_batches_eq!(expected, &actual);
    Ok(())
}

#[tokio::test]
async fn window_frame_partition_by_order_by_desc() -> Result<()> {
    let ctx = SessionContext::new();
    register_aggregate_csv(&ctx).await?;
    let sql = "SELECT
               SUM(c4) OVER(PARTITION BY c1 ORDER BY c2 DESC RANGE BETWEEN 1 PRECEDING AND 1 FOLLOWING)
               FROM aggregate_test_100
               ORDER BY c9
               LIMIT 5";
    let actual = execute_to_batches(&ctx, sql).await;
    let expected = vec![
        "+----------------------------+",
        "| SUM(aggregate_test_100.c4) |",
        "+----------------------------+",
        "| -124618                    |",
        "| 205080                     |",
        "| -40819                     |",
        "| -19517                     |",
        "| 47246                      |",
        "+----------------------------+",
    ];
    assert_batches_eq!(expected, &actual);
    Ok(())
}

#[tokio::test]
async fn window_frame_range_float() -> Result<()> {
    let ctx = SessionContext::new();
    register_aggregate_csv(&ctx).await?;
    let sql = "SELECT
                SUM(c12) OVER (ORDER BY C12 RANGE BETWEEN 0.2 PRECEDING AND 0.2 FOLLOWING)
                FROM aggregate_test_100
                ORDER BY C9
                LIMIT 5";
    let actual = execute_to_batches(&ctx, sql).await;
    let expected = vec![
        "+-----------------------------+",
        "| SUM(aggregate_test_100.c12) |",
        "+-----------------------------+",
        "| 2.5476701803634296          |",
        "| 10.6299412548214            |",
        "| 2.5476701803634296          |",
        "| 20.349518503437288          |",
        "| 21.408674363507753          |",
        "+-----------------------------+",
    ];
    assert_batches_eq!(expected, &actual);
    Ok(())
}

#[tokio::test]
async fn window_frame_ranges_timestamp() -> Result<()> {
    // define a schema.
    let schema = Arc::new(Schema::new(vec![Field::new(
        "ts",
        DataType::Timestamp(TimeUnit::Nanosecond, None),
        false,
    )]));

    // define data in two partitions
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![Arc::new(TimestampNanosecondArray::from_slice(&[
            1664264591000000000,
            1664264592000000000,
            1664264592000000000,
            1664264593000000000,
            1664264594000000000,
            1664364594000000000,
            1664464594000000000,
            1664564594000000000,
        ]))],
    )
    .unwrap();

    let ctx = SessionContext::new();
    // declare a new context. In spark API, this corresponds to a new spark SQLsession
    // declare a table in memory. In spark API, this corresponds to createDataFrame(...).
    let provider = MemTable::try_new(schema, vec![vec![batch]]).unwrap();
    // Register table
    ctx.register_table("t", Arc::new(provider)).unwrap();

    // execute the query
    let df = ctx
        .sql(
            "SELECT
                ts,
                COUNT(*) OVER (ORDER BY ts RANGE BETWEEN INTERVAL '1' DAY PRECEDING AND INTERVAL '2 DAY' FOLLOWING) AS cnt1,
                COUNT(*) OVER (ORDER BY ts RANGE BETWEEN '0 DAY' PRECEDING AND '0' DAY FOLLOWING) as cnt2,
                COUNT(*) OVER (ORDER BY ts RANGE BETWEEN '5' SECOND PRECEDING AND CURRENT ROW) as cnt3
                FROM t
                ORDER BY ts"
        )
        .await?;

    let actual = df.collect().await?;
    let expected = vec![
        "+---------------------+------+------+------+",
        "| ts                  | cnt1 | cnt2 | cnt3 |",
        "+---------------------+------+------+------+",
        "| 2022-09-27 07:43:11 | 6    | 1    | 1    |",
        "| 2022-09-27 07:43:12 | 6    | 2    | 3    |",
        "| 2022-09-27 07:43:12 | 6    | 2    | 3    |",
        "| 2022-09-27 07:43:13 | 6    | 1    | 4    |",
        "| 2022-09-27 07:43:14 | 6    | 1    | 5    |",
        "| 2022-09-28 11:29:54 | 2    | 1    | 1    |",
        "| 2022-09-29 15:16:34 | 2    | 1    | 1    |",
        "| 2022-09-30 19:03:14 | 1    | 1    | 1    |",
        "+---------------------+------+------+------+",
    ];
    assert_batches_eq!(expected, &actual);
    Ok(())
}

#[tokio::test]
async fn window_frame_ranges_unbounded_preceding_err() -> Result<()> {
    let ctx = SessionContext::new();
    register_aggregate_csv(&ctx).await?;
    // execute the query
    let df = ctx
        .sql(
            "SELECT \
               SUM(c2) OVER (ORDER BY c2 RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED PRECEDING), \
               COUNT(*) OVER (ORDER BY c2 RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED PRECEDING) \
               FROM aggregate_test_100 \
               ORDER BY c9 \
               LIMIT 5",
        )
        .await;
    assert_eq!(
        df.err().unwrap().to_string(),
        "Execution error: Invalid window frame: end bound cannot be unbounded preceding"
            .to_owned()
    );
    Ok(())
}

#[tokio::test]
async fn window_frame_groups_query() -> Result<()> {
    let ctx = SessionContext::new();
    register_aggregate_csv(&ctx).await?;
    // execute the query
    let df = ctx
        .sql(
            "SELECT
                COUNT(c1) OVER (ORDER BY c2 GROUPS BETWEEN 1 PRECEDING AND 1 FOLLOWING)
                FROM aggregate_test_100;",
        )
        .await?;
    let results = df.collect().await;
    assert!(results
        .as_ref()
        .err()
        .unwrap()
        .to_string()
        .contains("Window frame definitions involving GROUPS are not supported yet"));
    Ok(())
}

#[tokio::test]
async fn window_frame_creation() -> Result<()> {
    let ctx = SessionContext::new();
    register_aggregate_csv(&ctx).await?;
    // execute the query
    let df = ctx
        .sql(
            "SELECT
                COUNT(c1) OVER (ORDER BY c2 RANGE BETWEEN 1 PRECEDING AND 2 PRECEDING)
                FROM aggregate_test_100;",
        )
        .await?;
    let results = df.collect().await;
    assert_eq!(
        results.err().unwrap().to_string(),
        "Execution error: Invalid window frame: start bound (1 PRECEDING) cannot be larger than end bound (2 PRECEDING)"
    );

    let df = ctx
        .sql(
            "SELECT
                COUNT(c1) OVER (ORDER BY c2 RANGE BETWEEN 2 FOLLOWING AND 1 FOLLOWING)
                FROM aggregate_test_100;",
        )
        .await?;
    let results = df.collect().await;
    assert_eq!(
        results.err().unwrap().to_string(),
        "Execution error: Invalid window frame: start bound (2 FOLLOWING) cannot be larger than end bound (1 FOLLOWING)"
    );

    let df = ctx
        .sql(
            "SELECT
                COUNT(c1) OVER (ORDER BY c2 RANGE BETWEEN '1 DAY' PRECEDING AND '2 DAY' FOLLOWING)
                FROM aggregate_test_100;",
        )
        .await?;
    let results = df.collect().await;
    assert_contains!(
        results.err().unwrap().to_string(),
        "Arrow error: External error: Internal error: Operator - is not implemented for types UInt32(1) and Utf8(\"1 DAY\")"
    );

    Ok(())
}
