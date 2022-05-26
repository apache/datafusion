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
async fn select_qualified_wildcard() -> Result<()> {
    let ctx = SessionContext::new();
    register_aggregate_simple_csv(&ctx).await?;

    let sql = "SELECT agg.* FROM aggregate_simple as agg order by c1";
    let results = execute_to_batches(&ctx, sql).await;

    let expected = vec![
        "+---------+----------------+-------+",
        "| c1      | c2             | c3    |",
        "+---------+----------------+-------+",
        "| 0.00001 | 0.000000000001 | true  |",
        "| 0.00002 | 0.000000000002 | false |",
        "| 0.00002 | 0.000000000002 | false |",
        "| 0.00003 | 0.000000000003 | true  |",
        "| 0.00003 | 0.000000000003 | true  |",
        "| 0.00003 | 0.000000000003 | true  |",
        "| 0.00004 | 0.000000000004 | false |",
        "| 0.00004 | 0.000000000004 | false |",
        "| 0.00004 | 0.000000000004 | false |",
        "| 0.00004 | 0.000000000004 | false |",
        "| 0.00005 | 0.000000000005 | true  |",
        "| 0.00005 | 0.000000000005 | true  |",
        "| 0.00005 | 0.000000000005 | true  |",
        "| 0.00005 | 0.000000000005 | true  |",
        "| 0.00005 | 0.000000000005 | true  |",
        "+---------+----------------+-------+",
    ];

    assert_batches_eq!(expected, &results);

    Ok(())
}

#[tokio::test]
async fn select_non_alias_qualified_wildcard() -> Result<()> {
    let ctx = SessionContext::new();
    register_aggregate_simple_csv(&ctx).await?;

    let sql = "SELECT aggregate_simple.* FROM aggregate_simple order by c1";
    let results = execute_to_batches(&ctx, sql).await;

    let expected = vec![
        "+---------+----------------+-------+",
        "| c1      | c2             | c3    |",
        "+---------+----------------+-------+",
        "| 0.00001 | 0.000000000001 | true  |",
        "| 0.00002 | 0.000000000002 | false |",
        "| 0.00002 | 0.000000000002 | false |",
        "| 0.00003 | 0.000000000003 | true  |",
        "| 0.00003 | 0.000000000003 | true  |",
        "| 0.00003 | 0.000000000003 | true  |",
        "| 0.00004 | 0.000000000004 | false |",
        "| 0.00004 | 0.000000000004 | false |",
        "| 0.00004 | 0.000000000004 | false |",
        "| 0.00004 | 0.000000000004 | false |",
        "| 0.00005 | 0.000000000005 | true  |",
        "| 0.00005 | 0.000000000005 | true  |",
        "| 0.00005 | 0.000000000005 | true  |",
        "| 0.00005 | 0.000000000005 | true  |",
        "| 0.00005 | 0.000000000005 | true  |",
        "+---------+----------------+-------+",
    ];

    assert_batches_eq!(expected, &results);

    Ok(())
}

#[tokio::test]
async fn select_qualified_wildcard_join() -> Result<()> {
    let ctx = create_join_context("t1_id", "t2_id")?;
    let sql =
        "SELECT tb1.*, tb2.* FROM t1 tb1 JOIN t2 tb2 ON t2_id = t1_id ORDER BY t1_id";
    let expected = vec![
        "+-------+---------+--------+-------+---------+--------+",
        "| t1_id | t1_name | t1_int | t2_id | t2_name | t2_int |",
        "+-------+---------+--------+-------+---------+--------+",
        "| 11    | a       | 1      | 11    | z       | 3      |",
        "| 22    | b       | 2      | 22    | y       | 1      |",
        "| 44    | d       | 4      | 44    | x       | 3      |",
        "+-------+---------+--------+-------+---------+--------+",
    ];

    let results = execute_to_batches(&ctx, sql).await;

    assert_batches_eq!(expected, &results);

    Ok(())
}

#[tokio::test]
async fn select_non_alias_qualified_wildcard_join() -> Result<()> {
    let ctx = create_join_context("t1_id", "t2_id")?;
    let sql = "SELECT t1.*, tb2.* FROM t1 JOIN t2 tb2 ON t2_id = t1_id ORDER BY t1_id";
    let expected = vec![
        "+-------+---------+--------+-------+---------+--------+",
        "| t1_id | t1_name | t1_int | t2_id | t2_name | t2_int |",
        "+-------+---------+--------+-------+---------+--------+",
        "| 11    | a       | 1      | 11    | z       | 3      |",
        "| 22    | b       | 2      | 22    | y       | 1      |",
        "| 44    | d       | 4      | 44    | x       | 3      |",
        "+-------+---------+--------+-------+---------+--------+",
    ];

    let results = execute_to_batches(&ctx, sql).await;

    assert_batches_eq!(expected, &results);

    Ok(())
}

#[tokio::test]
async fn select_wrong_qualified_wildcard() -> Result<()> {
    let ctx = SessionContext::new();
    register_aggregate_simple_csv(&ctx).await?;

    let sql = "SELECT agg.* FROM aggregate_simple order by c1";
    let result = ctx.create_logical_plan(sql);
    match result {
        Ok(_) => panic!("unexpected OK"),
        Err(err) => assert_eq!(
            err.to_string(),
            "Error during planning: Invalid qualifier agg"
        ),
    };

    Ok(())
}
