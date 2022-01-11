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
    let mut ctx = ExecutionContext::new();
    register_aggregate_csv(&mut ctx).await?;
    let sql = "select \
               c9, \
               count(c5) over (), \
               max(c5) over (), \
               min(c5) over () \
               from aggregate_test_100 \
               order by c9 \
               limit 5";
    let actual = execute_to_batches(&mut ctx, sql).await;
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
    let mut ctx = ExecutionContext::new();
    register_aggregate_csv(&mut ctx).await?;
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
    let actual = execute_to_batches(&mut ctx, sql).await;
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
    let mut ctx = ExecutionContext::new();
    register_aggregate_csv(&mut ctx).await?;
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
    let actual = execute_to_batches(&mut ctx, sql).await;
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
    let mut ctx = ExecutionContext::new();
    register_aggregate_csv(&mut ctx).await?;
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
    let actual = execute_to_batches(&mut ctx, sql).await;
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
