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

use datafusion::arrow::array::{UInt64Array, UInt8Array};
use datafusion::arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::datasource::MemTable;
use datafusion::error::Result;
use datafusion::prelude::SessionContext;
use std::sync::Arc;
use std::time::Duration;
use tokio::time::timeout;

/// This example demonstrates executing a simple query against a Memtable
#[tokio::main]
async fn main() -> Result<()> {
    let mem_table = create_memtable()?;

    // create local execution context
    let ctx = SessionContext::new();

    // Register the in-memory table containing the data
    ctx.register_table("users", Arc::new(mem_table))?;

    let dataframe = ctx.sql("SELECT * FROM users;").await?;

    timeout(Duration::from_secs(10), async move {
        let result = dataframe.collect().await.unwrap();
        let record_batch = result.get(0).unwrap();

        assert_eq!(1, record_batch.column(0).len());
        dbg!(record_batch.columns());
    })
    .await
    .unwrap();

    Ok(())
}

fn create_memtable() -> Result<MemTable> {
    MemTable::try_new(get_schema(), vec![vec![create_record_batch()?]])
}

fn create_record_batch() -> Result<RecordBatch> {
    let id_array = UInt8Array::from(vec![1]);
    let account_array = UInt64Array::from(vec![9000]);

    Ok(RecordBatch::try_new(
        get_schema(),
        vec![Arc::new(id_array), Arc::new(account_array)],
    )
    .unwrap())
}

fn get_schema() -> SchemaRef {
    SchemaRef::new(Schema::new(vec![
        Field::new("id", DataType::UInt8, false),
        Field::new("bank_account", DataType::UInt64, true),
    ]))
}
