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
async fn csv_query_with_predicate() -> Result<()> {
    let mut ctx = ExecutionContext::new();
    register_aggregate_csv(&mut ctx).await?;
    let sql = "SELECT c1, c12 FROM aggregate_test_100 WHERE c12 > 0.376 AND c12 < 0.4";
    let actual = execute_to_batches(&mut ctx, sql).await;
    let expected = vec![
        "+----+---------------------+",
        "| c1 | c12                 |",
        "+----+---------------------+",
        "| e  | 0.39144436569161134 |",
        "| d  | 0.38870280983958583 |",
        "+----+---------------------+",
    ];
    assert_batches_eq!(expected, &actual);
    Ok(())
}

#[tokio::test]
async fn csv_query_with_negative_predicate() -> Result<()> {
    let mut ctx = ExecutionContext::new();
    register_aggregate_csv(&mut ctx).await?;
    let sql = "SELECT c1, c4 FROM aggregate_test_100 WHERE c3 < -55 AND -c4 > 30000";
    let actual = execute_to_batches(&mut ctx, sql).await;
    let expected = vec![
        "+----+--------+",
        "| c1 | c4     |",
        "+----+--------+",
        "| e  | -31500 |",
        "| c  | -30187 |",
        "+----+--------+",
    ];
    assert_batches_eq!(expected, &actual);
    Ok(())
}

#[tokio::test]
async fn csv_query_with_negated_predicate() -> Result<()> {
    let mut ctx = ExecutionContext::new();
    register_aggregate_csv(&mut ctx).await?;
    let sql = "SELECT COUNT(1) FROM aggregate_test_100 WHERE NOT(c1 != 'a')";
    let actual = execute_to_batches(&mut ctx, sql).await;
    let expected = vec![
        "+-----------------+",
        "| COUNT(UInt8(1)) |",
        "+-----------------+",
        "| 21              |",
        "+-----------------+",
    ];
    assert_batches_eq!(expected, &actual);
    Ok(())
}

#[tokio::test]
async fn csv_query_with_is_not_null_predicate() -> Result<()> {
    let mut ctx = ExecutionContext::new();
    register_aggregate_csv(&mut ctx).await?;
    let sql = "SELECT COUNT(1) FROM aggregate_test_100 WHERE c1 IS NOT NULL";
    let actual = execute_to_batches(&mut ctx, sql).await;
    let expected = vec![
        "+-----------------+",
        "| COUNT(UInt8(1)) |",
        "+-----------------+",
        "| 100             |",
        "+-----------------+",
    ];
    assert_batches_eq!(expected, &actual);
    Ok(())
}

#[tokio::test]
async fn csv_query_with_is_null_predicate() -> Result<()> {
    let mut ctx = ExecutionContext::new();
    register_aggregate_csv(&mut ctx).await?;
    let sql = "SELECT COUNT(1) FROM aggregate_test_100 WHERE c1 IS NULL";
    let actual = execute_to_batches(&mut ctx, sql).await;
    let expected = vec![
        "+-----------------+",
        "| COUNT(UInt8(1)) |",
        "+-----------------+",
        "| 0               |",
        "+-----------------+",
    ];
    assert_batches_eq!(expected, &actual);
    Ok(())
}

#[tokio::test]
async fn query_where_neg_num() -> Result<()> {
    let mut ctx = ExecutionContext::new();
    register_aggregate_csv_by_sql(&mut ctx).await;

    // Negative numbers do not parse correctly as of Arrow 2.0.0
    let sql = "select c7, c8 from aggregate_test_100 where c7 >= -2 and c7 < 10";
    let actual = execute_to_batches(&mut ctx, sql).await;
    let expected = vec![
        "+----+-------+",
        "| c7 | c8    |",
        "+----+-------+",
        "| 7  | 45465 |",
        "| 5  | 40622 |",
        "| 0  | 61069 |",
        "| 2  | 20120 |",
        "| 4  | 39363 |",
        "+----+-------+",
    ];
    assert_batches_eq!(expected, &actual);

    // Also check floating point neg numbers
    let sql = "select c7, c8 from aggregate_test_100 where c7 >= -2.9 and c7 < 10";
    let actual = execute_to_batches(&mut ctx, sql).await;
    assert_batches_eq!(expected, &actual);
    Ok(())
}

#[tokio::test]
async fn like() -> Result<()> {
    let mut ctx = ExecutionContext::new();
    register_aggregate_csv_by_sql(&mut ctx).await;
    let sql = "SELECT COUNT(c1) FROM aggregate_test_100 WHERE c13 LIKE '%FB%'";
    // check that the physical and logical schemas are equal
    let actual = execute_to_batches(&mut ctx, sql).await;
    let expected = vec![
        "+------------------------------+",
        "| COUNT(aggregate_test_100.c1) |",
        "+------------------------------+",
        "| 1                            |",
        "+------------------------------+",
    ];
    assert_batches_eq!(expected, &actual);
    Ok(())
}

#[tokio::test]
async fn csv_between_expr() -> Result<()> {
    let mut ctx = ExecutionContext::new();
    register_aggregate_csv(&mut ctx).await?;
    let sql = "SELECT c4 FROM aggregate_test_100 WHERE c12 BETWEEN 0.995 AND 1.0";
    let actual = execute_to_batches(&mut ctx, sql).await;
    let expected = vec![
        "+-------+",
        "| c4    |",
        "+-------+",
        "| 10837 |",
        "+-------+",
    ];
    assert_batches_sorted_eq!(expected, &actual);
    Ok(())
}

#[tokio::test]
async fn csv_between_expr_negated() -> Result<()> {
    let mut ctx = ExecutionContext::new();
    register_aggregate_csv(&mut ctx).await?;
    let sql = "SELECT c4 FROM aggregate_test_100 WHERE c12 NOT BETWEEN 0 AND 0.995";
    let actual = execute_to_batches(&mut ctx, sql).await;
    let expected = vec![
        "+-------+",
        "| c4    |",
        "+-------+",
        "| 10837 |",
        "+-------+",
    ];
    assert_batches_sorted_eq!(expected, &actual);
    Ok(())
}

#[tokio::test]
async fn like_on_strings() -> Result<()> {
    let input = vec![Some("foo"), Some("bar"), None, Some("fazzz")]
        .into_iter()
        .collect::<StringArray>();

    let batch = RecordBatch::try_from_iter(vec![("c1", Arc::new(input) as _)]).unwrap();

    let table = MemTable::try_new(batch.schema(), vec![vec![batch]])?;
    let mut ctx = ExecutionContext::new();
    ctx.register_table("test", Arc::new(table))?;

    let sql = "SELECT * FROM test WHERE c1 LIKE '%a%'";
    let actual = execute_to_batches(&mut ctx, sql).await;
    let expected = vec![
        "+-------+",
        "| c1    |",
        "+-------+",
        "| bar   |",
        "| fazzz |",
        "+-------+",
    ];

    assert_batches_eq!(expected, &actual);
    Ok(())
}

#[tokio::test]
async fn like_on_string_dictionaries() -> Result<()> {
    let input = vec![Some("foo"), Some("bar"), None, Some("fazzz")]
        .into_iter()
        .collect::<DictionaryArray<Int32Type>>();

    let batch = RecordBatch::try_from_iter(vec![("c1", Arc::new(input) as _)]).unwrap();

    let table = MemTable::try_new(batch.schema(), vec![vec![batch]])?;
    let mut ctx = ExecutionContext::new();
    ctx.register_table("test", Arc::new(table))?;

    let sql = "SELECT * FROM test WHERE c1 LIKE '%a%'";
    let actual = execute_to_batches(&mut ctx, sql).await;
    let expected = vec![
        "+-------+",
        "| c1    |",
        "+-------+",
        "| bar   |",
        "| fazzz |",
        "+-------+",
    ];

    assert_batches_eq!(expected, &actual);
    Ok(())
}

#[tokio::test]
async fn test_regexp_is_match() -> Result<()> {
    let input = vec![Some("foo"), Some("Barrr"), Some("Bazzz"), Some("ZZZZZ")]
        .into_iter()
        .collect::<StringArray>();

    let batch = RecordBatch::try_from_iter(vec![("c1", Arc::new(input) as _)]).unwrap();

    let table = MemTable::try_new(batch.schema(), vec![vec![batch]])?;
    let mut ctx = ExecutionContext::new();
    ctx.register_table("test", Arc::new(table))?;

    let sql = "SELECT * FROM test WHERE c1 ~ 'z'";
    let actual = execute_to_batches(&mut ctx, sql).await;
    let expected = vec![
        "+-------+",
        "| c1    |",
        "+-------+",
        "| Bazzz |",
        "+-------+",
    ];
    assert_batches_eq!(expected, &actual);

    let sql = "SELECT * FROM test WHERE c1 ~* 'z'";
    let actual = execute_to_batches(&mut ctx, sql).await;
    let expected = vec![
        "+-------+",
        "| c1    |",
        "+-------+",
        "| Bazzz |",
        "| ZZZZZ |",
        "+-------+",
    ];
    assert_batches_eq!(expected, &actual);

    let sql = "SELECT * FROM test WHERE c1 !~ 'z'";
    let actual = execute_to_batches(&mut ctx, sql).await;
    let expected = vec![
        "+-------+",
        "| c1    |",
        "+-------+",
        "| foo   |",
        "| Barrr |",
        "| ZZZZZ |",
        "+-------+",
    ];
    assert_batches_eq!(expected, &actual);

    let sql = "SELECT * FROM test WHERE c1 !~* 'z'";
    let actual = execute_to_batches(&mut ctx, sql).await;
    let expected = vec![
        "+-------+",
        "| c1    |",
        "+-------+",
        "| foo   |",
        "| Barrr |",
        "+-------+",
    ];
    assert_batches_eq!(expected, &actual);
    Ok(())
}

#[tokio::test]
async fn except_with_null_not_equal() {
    let sql = "SELECT * FROM (SELECT null AS id1, 1 AS id2) t1
            EXCEPT SELECT * FROM (SELECT null AS id1, 2 AS id2) t2";

    let expected = vec![
        "+-----+-----+",
        "| id1 | id2 |",
        "+-----+-----+",
        "|     | 1   |",
        "+-----+-----+",
    ];

    let mut ctx = create_join_context_qualified().unwrap();
    let actual = execute_to_batches(&mut ctx, sql).await;

    assert_batches_eq!(expected, &actual);
}

#[tokio::test]
async fn except_with_null_equal() {
    let sql = "SELECT * FROM (SELECT null AS id1, 1 AS id2) t1
            EXCEPT SELECT * FROM (SELECT null AS id1, 1 AS id2) t2";

    let expected = vec!["++", "++"];
    let mut ctx = create_join_context_qualified().unwrap();
    let actual = execute_to_batches(&mut ctx, sql).await;

    assert_batches_eq!(expected, &actual);
}

#[tokio::test]
async fn test_expect_all() -> Result<()> {
    let mut ctx = ExecutionContext::new();
    register_alltypes_parquet(&mut ctx).await;
    // execute the query
    let sql = "SELECT int_col, double_col FROM alltypes_plain where int_col > 0 EXCEPT ALL SELECT int_col, double_col FROM alltypes_plain where int_col < 1";
    let actual = execute_to_batches(&mut ctx, sql).await;
    let expected = vec![
        "+---------+------------+",
        "| int_col | double_col |",
        "+---------+------------+",
        "| 1       | 10.1       |",
        "| 1       | 10.1       |",
        "| 1       | 10.1       |",
        "| 1       | 10.1       |",
        "+---------+------------+",
    ];
    assert_batches_eq!(expected, &actual);
    Ok(())
}

#[tokio::test]
async fn test_expect_distinct() -> Result<()> {
    let mut ctx = ExecutionContext::new();
    register_alltypes_parquet(&mut ctx).await;
    // execute the query
    let sql = "SELECT int_col, double_col FROM alltypes_plain where int_col > 0 EXCEPT SELECT int_col, double_col FROM alltypes_plain where int_col < 1";
    let actual = execute_to_batches(&mut ctx, sql).await;
    let expected = vec![
        "+---------+------------+",
        "| int_col | double_col |",
        "+---------+------------+",
        "| 1       | 10.1       |",
        "+---------+------------+",
    ];
    assert_batches_eq!(expected, &actual);
    Ok(())
}
