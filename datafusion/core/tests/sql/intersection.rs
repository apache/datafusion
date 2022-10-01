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
async fn intersect_with_null_not_equal() {
    let sql = "SELECT * FROM (SELECT null AS id1, 1 AS id2) t1
            INTERSECT SELECT * FROM (SELECT null AS id1, 2 AS id2) t2";

    let expected = vec!["++", "++"];
    let ctx = create_join_context_qualified("t1", "t2").unwrap();
    let actual = execute_to_batches(&ctx, sql).await;
    assert_batches_eq!(expected, &actual);
}

#[tokio::test]
async fn intersect_with_null_equal() {
    let sql = "SELECT * FROM (SELECT null AS id1, 1 AS id2) t1
            INTERSECT SELECT * FROM (SELECT null AS id1, 1 AS id2) t2";

    let expected = vec![
        "+-----+-----+",
        "| id1 | id2 |",
        "+-----+-----+",
        "|     | 1   |",
        "+-----+-----+",
    ];

    let ctx = create_join_context_qualified("t1", "t2").unwrap();
    let actual = execute_to_batches(&ctx, sql).await;

    assert_batches_eq!(expected, &actual);
}

#[tokio::test]
async fn test_intersect_all() -> Result<()> {
    let ctx = SessionContext::new();
    register_alltypes_parquet(&ctx).await;
    // execute the query
    let sql = "SELECT int_col, double_col FROM alltypes_plain where int_col > 0 INTERSECT ALL SELECT int_col, double_col FROM alltypes_plain LIMIT 4";
    let actual = execute_to_batches(&ctx, sql).await;
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
async fn test_intersect_distinct() -> Result<()> {
    let ctx = SessionContext::new();
    register_alltypes_parquet(&ctx).await;
    // execute the query
    let sql = "SELECT int_col, double_col FROM alltypes_plain where int_col > 0 INTERSECT SELECT int_col, double_col FROM alltypes_plain";
    let actual = execute_to_batches(&ctx, sql).await;
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

#[tokio::test]
#[should_panic(
    expected = "called `Result::unwrap()` on an `Err` value: \"Plan(\\\"INTERSECT/EXCEPT query \
    must have the same number of columns. Left columns count = 3 and right columns count = 2 \
    are not the same.\\\") at Creating logical plan for 'SELECT * FROM (SELECT 1 AS id1, 2 \
    AS id2, 3 as id3) t1\\n            INTERSECT SELECT * FROM (SELECT 1 AS id1, \
    2 AS id2) t2'\""
)]
async fn intersect_with_different_length() {
    let sql = "SELECT * FROM (SELECT 1 AS id1, 2 AS id2, 3 as id3) t1
            INTERSECT SELECT * FROM (SELECT 1 AS id1, 2 AS id2) t2";

    let ctx = create_join_context_qualified("t1", "t2").unwrap();
    execute_to_batches(&ctx, sql).await;
}
