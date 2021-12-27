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
async fn test_sort_unprojected_col() -> Result<()> {
    let mut ctx = ExecutionContext::new();
    register_alltypes_parquet(&mut ctx).await;
    // execute the query
    let sql = "SELECT id FROM alltypes_plain ORDER BY int_col, double_col";
    let actual = execute_to_batches(&mut ctx, sql).await;
    let expected = vec![
        "+----+", "| id |", "+----+", "| 4  |", "| 6  |", "| 2  |", "| 0  |", "| 5  |",
        "| 7  |", "| 3  |", "| 1  |", "+----+",
    ];
    assert_batches_eq!(expected, &actual);
    Ok(())
}

#[tokio::test]
async fn test_nulls_first_asc() -> Result<()> {
    let mut ctx = ExecutionContext::new();
    let sql = "SELECT * FROM (VALUES (1, 'one'), (2, 'two'), (null, 'three')) AS t (num,letter) ORDER BY num";
    let actual = execute_to_batches(&mut ctx, sql).await;
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
    let mut ctx = ExecutionContext::new();
    let sql = "SELECT * FROM (VALUES (1, 'one'), (2, 'two'), (null, 'three')) AS t (num,letter) ORDER BY num DESC";
    let actual = execute_to_batches(&mut ctx, sql).await;
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
    let mut ctx = ExecutionContext::new();
    let sql = "SELECT * FROM (VALUES (1, 'one'), (2, 'two'), (null, 'three')) AS t (num,letter) ORDER BY num DESC NULLS LAST";
    let actual = execute_to_batches(&mut ctx, sql).await;
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
    let mut ctx = ExecutionContext::new();
    let sql = "SELECT * FROM (VALUES (1, 'one'), (2, 'two'), (null, 'three')) AS t (num,letter) ORDER BY num ASC NULLS FIRST";
    let actual = execute_to_batches(&mut ctx, sql).await;
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
