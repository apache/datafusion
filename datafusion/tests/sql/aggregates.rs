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

#[tokio::test]
async fn csv_query_avg_multi_batch() -> Result<()> {
    let mut ctx = ExecutionContext::new();
    register_aggregate_csv(&mut ctx).await?;
    let sql = "SELECT avg(c12) FROM aggregate_test_100";
    let plan = ctx.create_logical_plan(sql).unwrap();
    let plan = ctx.optimize(&plan).unwrap();
    let plan = ctx.create_physical_plan(&plan).await.unwrap();
    let results = collect(plan).await.unwrap();
    let batch = &results[0];
    let column = batch.column(0);
    let array = column.as_any().downcast_ref::<Float64Array>().unwrap();
    let actual = array.value(0);
    let expected = 0.5089725;
    // Due to float number's accuracy, different batch size will lead to different
    // answers.
    assert!((expected - actual).abs() < 0.01);
    Ok(())
}

#[tokio::test]
async fn csv_query_avg() -> Result<()> {
    let mut ctx = ExecutionContext::new();
    register_aggregate_csv(&mut ctx).await?;
    let sql = "SELECT avg(c12) FROM aggregate_test_100";
    let mut actual = execute(&mut ctx, sql).await;
    actual.sort();
    let expected = vec![vec!["0.5089725099127211"]];
    assert_float_eq(&expected, &actual);
    Ok(())
}

#[tokio::test]
async fn csv_query_external_table_count() {
    let mut ctx = ExecutionContext::new();
    register_aggregate_csv_by_sql(&mut ctx).await;
    let sql = "SELECT COUNT(c12) FROM aggregate_test_100";
    let actual = execute_to_batches(&mut ctx, sql).await;
    let expected = vec![
        "+-------------------------------+",
        "| COUNT(aggregate_test_100.c12) |",
        "+-------------------------------+",
        "| 100                           |",
        "+-------------------------------+",
    ];

    assert_batches_eq!(expected, &actual);
}

#[tokio::test]
async fn csv_query_external_table_sum() {
    let mut ctx = ExecutionContext::new();
    // cast smallint and int to bigint to avoid overflow during calculation
    register_aggregate_csv_by_sql(&mut ctx).await;
    let sql =
        "SELECT SUM(CAST(c7 AS BIGINT)), SUM(CAST(c8 AS BIGINT)) FROM aggregate_test_100";
    let actual = execute_to_batches(&mut ctx, sql).await;
    let expected = vec![
        "+-------------------------------------------+-------------------------------------------+",
        "| SUM(CAST(aggregate_test_100.c7 AS Int64)) | SUM(CAST(aggregate_test_100.c8 AS Int64)) |",
        "+-------------------------------------------+-------------------------------------------+",
        "| 13060                                     | 3017641                                   |",
        "+-------------------------------------------+-------------------------------------------+",
    ];
    assert_batches_eq!(expected, &actual);
}

#[tokio::test]
async fn csv_query_count() -> Result<()> {
    let mut ctx = ExecutionContext::new();
    register_aggregate_csv(&mut ctx).await?;
    let sql = "SELECT count(c12) FROM aggregate_test_100";
    let actual = execute_to_batches(&mut ctx, sql).await;
    let expected = vec![
        "+-------------------------------+",
        "| COUNT(aggregate_test_100.c12) |",
        "+-------------------------------+",
        "| 100                           |",
        "+-------------------------------+",
    ];
    assert_batches_eq!(expected, &actual);
    Ok(())
}

#[tokio::test]
async fn csv_query_count_star() {
    let mut ctx = ExecutionContext::new();
    register_aggregate_csv_by_sql(&mut ctx).await;
    let sql = "SELECT COUNT(*) FROM aggregate_test_100";
    let actual = execute_to_batches(&mut ctx, sql).await;
    let expected = vec![
        "+-----------------+",
        "| COUNT(UInt8(1)) |",
        "+-----------------+",
        "| 100             |",
        "+-----------------+",
    ];
    assert_batches_eq!(expected, &actual);
}

#[tokio::test]
async fn csv_query_count_one() {
    let mut ctx = ExecutionContext::new();
    register_aggregate_csv_by_sql(&mut ctx).await;
    let sql = "SELECT COUNT(1) FROM aggregate_test_100";
    let actual = execute_to_batches(&mut ctx, sql).await;
    let expected = vec![
        "+-----------------+",
        "| COUNT(UInt8(1)) |",
        "+-----------------+",
        "| 100             |",
        "+-----------------+",
    ];
    assert_batches_eq!(expected, &actual);
}

#[tokio::test]
async fn csv_query_approx_count() -> Result<()> {
    let mut ctx = ExecutionContext::new();
    register_aggregate_csv(&mut ctx).await?;
    let sql = "SELECT approx_distinct(c9) count_c9, approx_distinct(cast(c9 as varchar)) count_c9_str FROM aggregate_test_100";
    let actual = execute_to_batches(&mut ctx, sql).await;
    let expected = vec![
        "+----------+--------------+",
        "| count_c9 | count_c9_str |",
        "+----------+--------------+",
        "| 100      | 99           |",
        "+----------+--------------+",
    ];
    assert_batches_eq!(expected, &actual);
    Ok(())
}

#[tokio::test]
async fn query_count_without_from() -> Result<()> {
    let mut ctx = ExecutionContext::new();
    let sql = "SELECT count(1 + 1)";
    let actual = execute_to_batches(&mut ctx, sql).await;
    let expected = vec![
        "+----------------------------+",
        "| COUNT(Int64(1) + Int64(1)) |",
        "+----------------------------+",
        "| 1                          |",
        "+----------------------------+",
    ];
    assert_batches_eq!(expected, &actual);
    Ok(())
}

#[tokio::test]
async fn csv_query_array_agg() -> Result<()> {
    let mut ctx = ExecutionContext::new();
    register_aggregate_csv(&mut ctx).await?;
    let sql =
        "SELECT array_agg(c13) FROM (SELECT * FROM aggregate_test_100 ORDER BY c13 LIMIT 2) test";
    let actual = execute_to_batches(&mut ctx, sql).await;
    let expected = vec![
        "+------------------------------------------------------------------+",
        "| ARRAYAGG(test.c13)                                               |",
        "+------------------------------------------------------------------+",
        "| [0VVIHzxWtNOFLtnhjHEKjXaJOSLJfm, 0keZ5G8BffGwgF2RwQD59TFzMStxCB] |",
        "+------------------------------------------------------------------+",
    ];
    assert_batches_eq!(expected, &actual);
    Ok(())
}

#[tokio::test]
async fn csv_query_array_agg_empty() -> Result<()> {
    let mut ctx = ExecutionContext::new();
    register_aggregate_csv(&mut ctx).await?;
    let sql =
        "SELECT array_agg(c13) FROM (SELECT * FROM aggregate_test_100 LIMIT 0) test";
    let actual = execute_to_batches(&mut ctx, sql).await;
    let expected = vec![
        "+--------------------+",
        "| ARRAYAGG(test.c13) |",
        "+--------------------+",
        "| []                 |",
        "+--------------------+",
    ];
    assert_batches_eq!(expected, &actual);
    Ok(())
}

#[tokio::test]
async fn csv_query_array_agg_one() -> Result<()> {
    let mut ctx = ExecutionContext::new();
    register_aggregate_csv(&mut ctx).await?;
    let sql =
        "SELECT array_agg(c13) FROM (SELECT * FROM aggregate_test_100 ORDER BY c13 LIMIT 1) test";
    let actual = execute_to_batches(&mut ctx, sql).await;
    let expected = vec![
        "+----------------------------------+",
        "| ARRAYAGG(test.c13)               |",
        "+----------------------------------+",
        "| [0VVIHzxWtNOFLtnhjHEKjXaJOSLJfm] |",
        "+----------------------------------+",
    ];
    assert_batches_eq!(expected, &actual);
    Ok(())
}
