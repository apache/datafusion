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
use datafusion::{error::Result, execution::context::ExecutionContext};

#[tokio::test]
async fn exist_subquery_basic() -> Result<()> {
    let mut ctx = ExecutionContext::new();
    // create t1 table
    let create_t1_sql = "CREATE TABLE t1 AS (SELECT 1 AS t1_id, 'a' AS t1_name  UNION ALL SELECT 2 AS t1_id, 'b' AS t1_name UNION ALL SELECT 3 AS t1_id, 'c' AS t1_name)";
    ctx.sql(create_t1_sql).await.unwrap();

    // create t2 table
    let create_t2_sql = "CREATE TABLE t2 AS (SELECT 1 AS t2_id, 'a' AS t2_name, 4 AS t2_num UNION ALL SELECT 2 AS t2_id, 'p' AS t2_name, 5 AS t2_num UNION ALL SELECT 3 AS t2_id, 'q' AS t2_name, 6 AS t2_num)";
    ctx.sql(create_t2_sql).await.unwrap();

    // exist subquery sql
    let sql = "SELECT t2.t2_id FROM t2 WHERE EXISTS (SELECT t1.t1_id FROM t1 WHERE t1.t1_name = 'a')";

    let actual = execute_to_batches(&mut ctx, sql).await;
    let expected = vec![
        "+---------+",
        "| t2_id   |",
        "+---------+",
        "| 1       |",
        "+---------+",
    ];
    assert_batches_eq!(expected, &actual);
    Ok(())
}
