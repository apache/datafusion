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
async fn union_all() -> Result<()> {
    let mut ctx = ExecutionContext::new();
    let sql = "SELECT 1 as x UNION ALL SELECT 2 as x";
    let actual = execute_to_batches(&mut ctx, sql).await;
    let expected = vec!["+---+", "| x |", "+---+", "| 1 |", "| 2 |", "+---+"];
    assert_batches_eq!(expected, &actual);
    Ok(())
}

#[tokio::test]
async fn csv_union_all() -> Result<()> {
    let mut ctx = ExecutionContext::new();
    register_aggregate_csv(&mut ctx).await?;
    let sql =
        "SELECT c1 FROM aggregate_test_100 UNION ALL SELECT c1 FROM aggregate_test_100";
    let actual = execute(&mut ctx, sql).await;
    assert_eq!(actual.len(), 200);
    Ok(())
}

#[tokio::test]
async fn union_distinct() -> Result<()> {
    let mut ctx = ExecutionContext::new();
    let sql = "SELECT 1 as x UNION SELECT 1 as x";
    let actual = execute_to_batches(&mut ctx, sql).await;
    let expected = vec!["+---+", "| x |", "+---+", "| 1 |", "+---+"];
    assert_batches_eq!(expected, &actual);
    Ok(())
}

#[tokio::test]
async fn union_all_with_aggregate() -> Result<()> {
    let mut ctx = ExecutionContext::new();
    let sql =
        "SELECT SUM(d) FROM (SELECT 1 as c, 2 as d UNION ALL SELECT 1 as c, 3 AS d) as a";
    let actual = execute_to_batches(&mut ctx, sql).await;
    let expected = vec![
        "+----------+",
        "| SUM(a.d) |",
        "+----------+",
        "| 5        |",
        "+----------+",
    ];
    assert_batches_eq!(expected, &actual);
    Ok(())
}
