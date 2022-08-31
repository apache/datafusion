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

use crate::sql::execute_to_batches;
use arrow::record_batch::RecordBatch;
use datafusion::assert_batches_eq;
use datafusion::error::Result;
use datafusion::prelude::SessionContext;

async fn execute_sql(sql: &str) -> Vec<RecordBatch> {
    let ctx = SessionContext::new();
    execute_to_batches(&ctx, sql).await
}

#[tokio::test]
async fn cast_tinyint() -> Result<()> {
    let actual = execute_sql("SELECT cast(10 as tinyint)").await;
    let expected = vec![
        "+-------------------------+",
        "| CAST(Int64(10) AS Int8) |",
        "+-------------------------+",
        "| 10                      |",
        "+-------------------------+",
    ];
    assert_batches_eq!(expected, &actual);
    Ok(())
}

#[tokio::test]
async fn cast_tinyint_operator() -> Result<()> {
    let actual = execute_sql("SELECT 10::tinyint").await;
    let expected = vec![
        "+-------------------------+",
        "| CAST(Int64(10) AS Int8) |",
        "+-------------------------+",
        "| 10                      |",
        "+-------------------------+",
    ];
    assert_batches_eq!(expected, &actual);
    Ok(())
}

#[tokio::test]
async fn cast_unsigned_tinyint() -> Result<()> {
    let actual = execute_sql("SELECT 10::tinyint unsigned").await;
    let expected = vec![
        "+--------------------------+",
        "| CAST(Int64(10) AS UInt8) |",
        "+--------------------------+",
        "| 10                       |",
        "+--------------------------+",
    ];
    assert_batches_eq!(expected, &actual);
    Ok(())
}

#[tokio::test]
async fn cast_unsigned_smallint() -> Result<()> {
    let actual = execute_sql("SELECT 10::smallint unsigned").await;
    let expected = vec![
        "+---------------------------+",
        "| CAST(Int64(10) AS UInt16) |",
        "+---------------------------+",
        "| 10                        |",
        "+---------------------------+",
    ];
    assert_batches_eq!(expected, &actual);
    Ok(())
}

#[tokio::test]
async fn cast_unsigned_int() -> Result<()> {
    let actual = execute_sql("SELECT 10::integer unsigned").await;
    let expected = vec![
        "+---------------------------+",
        "| CAST(Int64(10) AS UInt32) |",
        "+---------------------------+",
        "| 10                        |",
        "+---------------------------+",
    ];
    assert_batches_eq!(expected, &actual);
    Ok(())
}

#[tokio::test]
async fn cast_unsigned_bigint() -> Result<()> {
    let actual = execute_sql("SELECT 10::bigint unsigned").await;
    let expected = vec![
        "+---------------------------+",
        "| CAST(Int64(10) AS UInt64) |",
        "+---------------------------+",
        "| 10                        |",
        "+---------------------------+",
    ];
    assert_batches_eq!(expected, &actual);
    Ok(())
}
