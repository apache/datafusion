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
        "+-----------+-------------------------------------------+-------------------------------------------+---------------------------------------------+-------------------------------------------+-------------------------------------------+",
        "| c9        | SUM(CAST(aggregate_test_100.c4 AS Int32)) | AVG(CAST(aggregate_test_100.c4 AS Int32)) | COUNT(CAST(aggregate_test_100.c4 AS Int32)) | MAX(CAST(aggregate_test_100.c4 AS Int32)) | MIN(CAST(aggregate_test_100.c4 AS Int32)) |",
        "+-----------+-------------------------------------------+-------------------------------------------+---------------------------------------------+-------------------------------------------+-------------------------------------------+",
        "| 28774375  | -16110                                    | -16110                                    | 1                                           | -16110                                    | -16110                                    |",
        "| 63044568  | 3917                                      | 3917                                      | 1                                           | 3917                                      | 3917                                      |",
        "| 141047417 | -38455                                    | -19227.5                                  | 2                                           | -16974                                    | -21481                                    |",
        "| 141680161 | -1114                                     | -1114                                     | 1                                           | -1114                                     | -1114                                     |",
        "| 145294611 | 15673                                     | 15673                                     | 1                                           | 15673                                     | 15673                                     |",
        "+-----------+-------------------------------------------+-------------------------------------------+---------------------------------------------+-------------------------------------------+-------------------------------------------+",
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
    let state = ctx.state.read().clone();
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
