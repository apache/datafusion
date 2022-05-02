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
use fuzz_utils::{batches_to_vec, partitions_to_sorted_vec};

#[tokio::test]
async fn test_sort_unprojected_col() -> Result<()> {
    let ctx = SessionContext::new();
    register_alltypes_parquet(&ctx).await;
    // execute the query
    let sql = "SELECT id FROM alltypes_plain ORDER BY int_col, double_col";
    let actual = execute_to_batches(&ctx, sql).await;
    #[rustfmt::skip]
    let expected = vec![
        "+----+",
        "| id |",
        "+----+",
        "| 4  |",
        "| 6  |",
        "| 2  |",
        "| 0  |",
        "| 5  |",
        "| 7  |",
        "| 3  |",
        "| 1  |",
        "+----+",
    ];
    assert_batches_eq!(expected, &actual);
    Ok(())
}

#[tokio::test]
async fn test_order_by_agg_expr() -> Result<()> {
    let ctx = SessionContext::new();
    register_aggregate_csv(&ctx).await?;
    let sql = "SELECT MIN(c12) FROM aggregate_test_100 ORDER BY MIN(c12)";
    let actual = execute_to_batches(&ctx, sql).await;
    let expected = vec![
        "+-----------------------------+",
        "| MIN(aggregate_test_100.c12) |",
        "+-----------------------------+",
        "| 0.01479305307777301         |",
        "+-----------------------------+",
    ];
    assert_batches_eq!(expected, &actual);

    let sql = "SELECT MIN(c12) FROM aggregate_test_100 ORDER BY MIN(c12) + 0.1";
    let actual = execute_to_batches(&ctx, sql).await;
    assert_batches_eq!(expected, &actual);
    Ok(())
}

#[tokio::test]
async fn test_nulls_first_asc() -> Result<()> {
    let ctx = SessionContext::new();
    let sql = "SELECT * FROM (VALUES (1, 'one'), (2, 'two'), (null, 'three')) AS t (num,letter) ORDER BY num";
    let actual = execute_to_batches(&ctx, sql).await;
    let expected = vec![
        "+-----+--------+",
        "| num | letter |",
        "+-----+--------+",
        "| 1   | one    |",
        "| 2   | two    |",
        "|     | three  |",
        "+-----+--------+",
    ];
    assert_batches_eq!(expected, &actual);
    Ok(())
}

#[tokio::test]
async fn test_nulls_first_desc() -> Result<()> {
    let ctx = SessionContext::new();
    let sql = "SELECT * FROM (VALUES (1, 'one'), (2, 'two'), (null, 'three')) AS t (num,letter) ORDER BY num DESC";
    let actual = execute_to_batches(&ctx, sql).await;
    let expected = vec![
        "+-----+--------+",
        "| num | letter |",
        "+-----+--------+",
        "|     | three  |",
        "| 2   | two    |",
        "| 1   | one    |",
        "+-----+--------+",
    ];
    assert_batches_eq!(expected, &actual);
    Ok(())
}

#[tokio::test]
async fn test_specific_nulls_last_desc() -> Result<()> {
    let ctx = SessionContext::new();
    let sql = "SELECT * FROM (VALUES (1, 'one'), (2, 'two'), (null, 'three')) AS t (num,letter) ORDER BY num DESC NULLS LAST";
    let actual = execute_to_batches(&ctx, sql).await;
    let expected = vec![
        "+-----+--------+",
        "| num | letter |",
        "+-----+--------+",
        "| 2   | two    |",
        "| 1   | one    |",
        "|     | three  |",
        "+-----+--------+",
    ];
    assert_batches_eq!(expected, &actual);
    Ok(())
}

#[tokio::test]
async fn test_specific_nulls_first_asc() -> Result<()> {
    let ctx = SessionContext::new();
    let sql = "SELECT * FROM (VALUES (1, 'one'), (2, 'two'), (null, 'three')) AS t (num,letter) ORDER BY num ASC NULLS FIRST";
    let actual = execute_to_batches(&ctx, sql).await;
    let expected = vec![
        "+-----+--------+",
        "| num | letter |",
        "+-----+--------+",
        "|     | three  |",
        "| 1   | one    |",
        "| 2   | two    |",
        "+-----+--------+",
    ];
    assert_batches_eq!(expected, &actual);
    Ok(())
}

#[tokio::test]
async fn sort() -> Result<()> {
    let results =
        partitioned_csv::execute("SELECT c1, c2 FROM test ORDER BY c1 DESC, c2 ASC", 4)
            .await?;
    assert_eq!(results.len(), 1);

    let expected: Vec<&str> = vec![
        "+----+----+",
        "| c1 | c2 |",
        "+----+----+",
        "| 3  | 1  |",
        "| 3  | 2  |",
        "| 3  | 3  |",
        "| 3  | 4  |",
        "| 3  | 5  |",
        "| 3  | 6  |",
        "| 3  | 7  |",
        "| 3  | 8  |",
        "| 3  | 9  |",
        "| 3  | 10 |",
        "| 2  | 1  |",
        "| 2  | 2  |",
        "| 2  | 3  |",
        "| 2  | 4  |",
        "| 2  | 5  |",
        "| 2  | 6  |",
        "| 2  | 7  |",
        "| 2  | 8  |",
        "| 2  | 9  |",
        "| 2  | 10 |",
        "| 1  | 1  |",
        "| 1  | 2  |",
        "| 1  | 3  |",
        "| 1  | 4  |",
        "| 1  | 5  |",
        "| 1  | 6  |",
        "| 1  | 7  |",
        "| 1  | 8  |",
        "| 1  | 9  |",
        "| 1  | 10 |",
        "| 0  | 1  |",
        "| 0  | 2  |",
        "| 0  | 3  |",
        "| 0  | 4  |",
        "| 0  | 5  |",
        "| 0  | 6  |",
        "| 0  | 7  |",
        "| 0  | 8  |",
        "| 0  | 9  |",
        "| 0  | 10 |",
        "+----+----+",
    ];

    // Note it is important to NOT use assert_batches_sorted_eq
    // here as we are testing the sortedness of the output
    assert_batches_eq!(expected, &results);

    Ok(())
}

#[tokio::test]
async fn sort_empty() -> Result<()> {
    // The predicate on this query purposely generates no results
    let results = partitioned_csv::execute(
        "SELECT c1, c2 FROM test WHERE c1 > 100000 ORDER BY c1 DESC, c2 ASC",
        4,
    )
    .await
    .unwrap();
    assert_eq!(results.len(), 0);
    Ok(())
}

#[tokio::test]
async fn sort_with_lots_of_repetition_values() -> Result<()> {
    let ctx = SessionContext::new();
    let filename = "tests/parquet/repeat_much.snappy.parquet";

    ctx.register_parquet("rep", filename, ParquetReadOptions::default())
        .await?;
    let sql = "select a from rep order by a";
    let actual = execute_to_batches(&ctx, sql).await;
    let actual = batches_to_vec(&actual);

    let sql1 = "select a from rep";
    let expected = execute_to_batches(&ctx, sql1).await;
    let expected = partitions_to_sorted_vec(&[expected]);

    assert_eq!(actual.len(), expected.len());
    for i in 0..actual.len() {
        assert_eq!(actual[i], expected[i]);
    }
    Ok(())
}
