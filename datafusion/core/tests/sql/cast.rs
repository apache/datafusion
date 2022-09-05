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
use arrow::datatypes::DataType;
use arrow::record_batch::RecordBatch;
use datafusion::error::Result;
use datafusion::prelude::SessionContext;

async fn execute_sql(sql: &str) -> Vec<RecordBatch> {
    let ctx = SessionContext::new();
    execute_to_batches(&ctx, sql).await
}

#[tokio::test]
async fn cast_tinyint() -> Result<()> {
    let actual = execute_sql("SELECT cast(10 as tinyint)").await;
    assert_eq!(&DataType::Int8, actual[0].schema().field(0).data_type());
    Ok(())
}

#[tokio::test]
async fn cast_tinyint_operator() -> Result<()> {
    let actual = execute_sql("SELECT 10::tinyint").await;
    assert_eq!(&DataType::Int8, actual[0].schema().field(0).data_type());
    Ok(())
}

#[tokio::test]
async fn cast_unsigned_tinyint() -> Result<()> {
    let actual = execute_sql("SELECT 10::tinyint unsigned").await;
    assert_eq!(&DataType::UInt8, actual[0].schema().field(0).data_type());
    Ok(())
}

#[tokio::test]
async fn cast_unsigned_smallint() -> Result<()> {
    let actual = execute_sql("SELECT 10::smallint unsigned").await;
    assert_eq!(&DataType::UInt16, actual[0].schema().field(0).data_type());
    Ok(())
}

#[tokio::test]
async fn cast_unsigned_int() -> Result<()> {
    let actual = execute_sql("SELECT 10::integer unsigned").await;
    assert_eq!(&DataType::UInt32, actual[0].schema().field(0).data_type());
    Ok(())
}

#[tokio::test]
async fn cast_unsigned_bigint() -> Result<()> {
    let actual = execute_sql("SELECT 10::bigint unsigned").await;
    assert_eq!(&DataType::UInt64, actual[0].schema().field(0).data_type());
    Ok(())
}
