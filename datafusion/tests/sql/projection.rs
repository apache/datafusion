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
async fn projection_same_fields() -> Result<()> {
    let mut ctx = ExecutionContext::new();

    let sql = "select (1+1) as a from (select 1 as a) as b;";
    let actual = execute_to_batches(&mut ctx, sql).await;

    let expected = vec!["+---+", "| a |", "+---+", "| 2 |", "+---+"];
    assert_batches_eq!(expected, &actual);

    Ok(())
}

#[tokio::test]
async fn projection_type_alias() -> Result<()> {
    let mut ctx = ExecutionContext::new();
    register_aggregate_simple_csv(&mut ctx).await?;

    // Query that aliases one column to the name of a different column
    // that also has a different type (c1 == float32, c3 == boolean)
    let sql = "SELECT c1 as c3 FROM aggregate_simple ORDER BY c3 LIMIT 2";
    let actual = execute_to_batches(&mut ctx, sql).await;

    let expected = vec![
        "+---------+",
        "| c3      |",
        "+---------+",
        "| 0.00001 |",
        "| 0.00002 |",
        "+---------+",
    ];
    assert_batches_eq!(expected, &actual);

    Ok(())
}

#[tokio::test]
async fn csv_query_group_by_avg_with_projection() -> Result<()> {
    let mut ctx = ExecutionContext::new();
    register_aggregate_csv(&mut ctx).await?;
    let sql = "SELECT avg(c12), c1 FROM aggregate_test_100 GROUP BY c1";
    let actual = execute_to_batches(&mut ctx, sql).await;
    let expected = vec![
        "+-----------------------------+----+",
        "| AVG(aggregate_test_100.c12) | c1 |",
        "+-----------------------------+----+",
        "| 0.41040709263815384         | b  |",
        "| 0.48600669271341534         | e  |",
        "| 0.48754517466109415         | a  |",
        "| 0.48855379387549824         | d  |",
        "| 0.6600456536439784          | c  |",
        "+-----------------------------+----+",
    ];
    assert_batches_sorted_eq!(expected, &actual);
    Ok(())
}
