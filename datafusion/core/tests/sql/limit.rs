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
async fn csv_query_limit() -> Result<()> {
    let ctx = SessionContext::new();
    register_aggregate_csv(&ctx).await?;
    let sql = "SELECT c1 FROM aggregate_test_100 LIMIT 2";
    let actual = execute_to_batches(&ctx, sql).await;
    #[rustfmt::skip]
    let expected = vec![
        "+----+",
        "| c1 |",
        "+----+",
        "| c  |",
        "| d  |",
        "+----+"
    ];
    assert_batches_eq!(expected, &actual);
    Ok(())
}

#[tokio::test]
async fn csv_query_limit_bigger_than_nbr_of_rows() -> Result<()> {
    let ctx = SessionContext::new();
    register_aggregate_csv(&ctx).await?;
    let sql = "SELECT c2 FROM aggregate_test_100 LIMIT 200";
    let actual = execute_to_batches(&ctx, sql).await;
    // println!("{}", pretty_format_batches(&a).unwrap());
    let expected = vec![
        "+----+", "| c2 |", "+----+", "| 2  |", "| 5  |", "| 1  |", "| 1  |", "| 5  |",
        "| 4  |", "| 3  |", "| 3  |", "| 1  |", "| 4  |", "| 1  |", "| 4  |", "| 3  |",
        "| 2  |", "| 1  |", "| 1  |", "| 2  |", "| 1  |", "| 3  |", "| 2  |", "| 4  |",
        "| 1  |", "| 5  |", "| 4  |", "| 2  |", "| 1  |", "| 4  |", "| 5  |", "| 2  |",
        "| 3  |", "| 4  |", "| 2  |", "| 1  |", "| 5  |", "| 3  |", "| 1  |", "| 2  |",
        "| 3  |", "| 3  |", "| 3  |", "| 2  |", "| 4  |", "| 1  |", "| 3  |", "| 2  |",
        "| 5  |", "| 2  |", "| 1  |", "| 4  |", "| 1  |", "| 4  |", "| 2  |", "| 5  |",
        "| 4  |", "| 2  |", "| 3  |", "| 4  |", "| 4  |", "| 4  |", "| 5  |", "| 4  |",
        "| 2  |", "| 1  |", "| 2  |", "| 4  |", "| 2  |", "| 3  |", "| 5  |", "| 1  |",
        "| 1  |", "| 4  |", "| 2  |", "| 1  |", "| 2  |", "| 1  |", "| 1  |", "| 5  |",
        "| 4  |", "| 5  |", "| 2  |", "| 3  |", "| 2  |", "| 4  |", "| 1  |", "| 3  |",
        "| 4  |", "| 3  |", "| 2  |", "| 5  |", "| 3  |", "| 3  |", "| 2  |", "| 5  |",
        "| 5  |", "| 4  |", "| 1  |", "| 3  |", "| 3  |", "| 4  |", "| 4  |", "+----+",
    ];
    assert_batches_eq!(expected, &actual);
    Ok(())
}

#[tokio::test]
async fn csv_query_limit_with_same_nbr_of_rows() -> Result<()> {
    let ctx = SessionContext::new();
    register_aggregate_csv(&ctx).await?;
    let sql = "SELECT c2 FROM aggregate_test_100 LIMIT 100";
    let actual = execute_to_batches(&ctx, sql).await;
    let expected = vec![
        "+----+", "| c2 |", "+----+", "| 2  |", "| 5  |", "| 1  |", "| 1  |", "| 5  |",
        "| 4  |", "| 3  |", "| 3  |", "| 1  |", "| 4  |", "| 1  |", "| 4  |", "| 3  |",
        "| 2  |", "| 1  |", "| 1  |", "| 2  |", "| 1  |", "| 3  |", "| 2  |", "| 4  |",
        "| 1  |", "| 5  |", "| 4  |", "| 2  |", "| 1  |", "| 4  |", "| 5  |", "| 2  |",
        "| 3  |", "| 4  |", "| 2  |", "| 1  |", "| 5  |", "| 3  |", "| 1  |", "| 2  |",
        "| 3  |", "| 3  |", "| 3  |", "| 2  |", "| 4  |", "| 1  |", "| 3  |", "| 2  |",
        "| 5  |", "| 2  |", "| 1  |", "| 4  |", "| 1  |", "| 4  |", "| 2  |", "| 5  |",
        "| 4  |", "| 2  |", "| 3  |", "| 4  |", "| 4  |", "| 4  |", "| 5  |", "| 4  |",
        "| 2  |", "| 1  |", "| 2  |", "| 4  |", "| 2  |", "| 3  |", "| 5  |", "| 1  |",
        "| 1  |", "| 4  |", "| 2  |", "| 1  |", "| 2  |", "| 1  |", "| 1  |", "| 5  |",
        "| 4  |", "| 5  |", "| 2  |", "| 3  |", "| 2  |", "| 4  |", "| 1  |", "| 3  |",
        "| 4  |", "| 3  |", "| 2  |", "| 5  |", "| 3  |", "| 3  |", "| 2  |", "| 5  |",
        "| 5  |", "| 4  |", "| 1  |", "| 3  |", "| 3  |", "| 4  |", "| 4  |", "+----+",
    ];
    assert_batches_eq!(expected, &actual);
    Ok(())
}

#[tokio::test]
async fn csv_query_limit_zero() -> Result<()> {
    let ctx = SessionContext::new();
    register_aggregate_csv(&ctx).await?;
    let sql = "SELECT c1 FROM aggregate_test_100 LIMIT 0";
    let actual = execute_to_batches(&ctx, sql).await;
    let expected = vec!["++", "++"];
    assert_batches_eq!(expected, &actual);
    Ok(())
}

#[tokio::test]
async fn limit() -> Result<()> {
    let tmp_dir = TempDir::new()?;
    let ctx = create_ctx_with_partition(&tmp_dir, 1).await?;
    ctx.register_table("t", table_with_sequence(1, 1000).unwrap())
        .unwrap();

    let results = plan_and_collect(&ctx, "SELECT i FROM t ORDER BY i DESC limit 3")
        .await
        .unwrap();

    #[rustfmt::skip]
    let expected = vec![
        "+------+",
        "| i    |",
        "+------+",
        "| 1000 |",
        "| 999  |",
        "| 998  |",
        "+------+",
    ];

    assert_batches_eq!(expected, &results);

    let results = plan_and_collect(&ctx, "SELECT i FROM t ORDER BY i limit 3")
        .await
        .unwrap();

    #[rustfmt::skip]
    let expected = vec![
        "+---+",
        "| i |",
        "+---+",
        "| 1 |",
        "| 2 |",
        "| 3 |",
        "+---+",
    ];

    assert_batches_eq!(expected, &results);

    let results = plan_and_collect(&ctx, "SELECT i FROM t limit 3")
        .await
        .unwrap();

    // the actual rows are not guaranteed, so only check the count (should be 3)
    let num_rows: usize = results.into_iter().map(|b| b.num_rows()).sum();
    assert_eq!(num_rows, 3);

    Ok(())
}

#[tokio::test]
async fn limit_multi_partitions() -> Result<()> {
    let tmp_dir = TempDir::new()?;
    let ctx = create_ctx_with_partition(&tmp_dir, 1).await?;

    let partitions = vec![
        vec![make_partition(0)],
        vec![make_partition(1)],
        vec![make_partition(2)],
        vec![make_partition(3)],
        vec![make_partition(4)],
        vec![make_partition(5)],
    ];
    let schema = partitions[0][0].schema();
    let provider = Arc::new(MemTable::try_new(schema, partitions).unwrap());

    ctx.register_table("t", provider).unwrap();

    // select all rows
    let results = plan_and_collect(&ctx, "SELECT i FROM t").await.unwrap();

    let num_rows: usize = results.into_iter().map(|b| b.num_rows()).sum();
    assert_eq!(num_rows, 15);

    for limit in 1..10 {
        let query = format!("SELECT i FROM t limit {}", limit);
        let results = plan_and_collect(&ctx, &query).await.unwrap();

        let num_rows: usize = results.into_iter().map(|b| b.num_rows()).sum();
        assert_eq!(num_rows, limit, "mismatch with query {}", query);
    }

    Ok(())
}

#[tokio::test]
async fn csv_offset_without_limit_99() -> Result<()> {
    let ctx = SessionContext::new();
    register_aggregate_csv(&ctx).await?;
    let sql = "SELECT c1 FROM aggregate_test_100 OFFSET 99";
    let actual = execute_to_batches(&ctx, sql).await;

    #[rustfmt::skip]
        let expected = vec![
        "+----+",
        "| c1 |",
        "+----+",
        "| e  |",
        "+----+"];
    assert_batches_eq!(expected, &actual);
    Ok(())
}

#[tokio::test]
async fn csv_offset_without_limit_100() -> Result<()> {
    let ctx = SessionContext::new();
    register_aggregate_csv(&ctx).await?;
    let sql = "SELECT c1 FROM aggregate_test_100 OFFSET 100";
    let actual = execute_to_batches(&ctx, sql).await;
    let expected = vec!["++", "++"];
    assert_batches_eq!(expected, &actual);
    Ok(())
}

#[tokio::test]
async fn csv_offset_without_limit_101() -> Result<()> {
    let ctx = SessionContext::new();
    register_aggregate_csv(&ctx).await?;
    let sql = "SELECT c1 FROM aggregate_test_100 OFFSET 101";
    let actual = execute_to_batches(&ctx, sql).await;
    let expected = vec!["++", "++"];
    assert_batches_eq!(expected, &actual);
    Ok(())
}

#[tokio::test]
async fn csv_query_offset() -> Result<()> {
    let ctx = SessionContext::new();
    register_aggregate_csv(&ctx).await?;
    let sql = "SELECT c1 FROM aggregate_test_100 OFFSET 2 LIMIT 2";
    let actual = execute_to_batches(&ctx, sql).await;

    #[rustfmt::skip]
        let expected = vec![
        "+----+",
        "| c1 |",
        "+----+",
        "| b  |",
        "| a  |",
        "+----+"];
    assert_batches_eq!(expected, &actual);
    Ok(())
}

#[tokio::test]
async fn csv_query_offset_the_same_as_nbr_of_rows() -> Result<()> {
    let ctx = SessionContext::new();
    register_aggregate_csv(&ctx).await?;
    let sql = "SELECT c1 FROM aggregate_test_100 LIMIT 1 OFFSET 100";
    let actual = execute_to_batches(&ctx, sql).await;
    let expected = vec!["++", "++"];
    assert_batches_eq!(expected, &actual);
    Ok(())
}

#[tokio::test]
async fn csv_query_offset_bigger_than_nbr_of_rows() -> Result<()> {
    let ctx = SessionContext::new();
    register_aggregate_csv(&ctx).await?;
    let sql = "SELECT c1 FROM aggregate_test_100 LIMIT 1 OFFSET 101";
    let actual = execute_to_batches(&ctx, sql).await;
    let expected = vec!["++", "++"];
    assert_batches_eq!(expected, &actual);
    Ok(())
}
