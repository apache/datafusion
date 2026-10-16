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

//! Query in-memory data with SQL and the DataFrame API.
//!
//! Run with `cargo run -p datafusion --example query`.
//! This example is also included in the published crate so rustdoc can show
//! its call sites alongside the API documentation.

use std::sync::Arc;

use datafusion::arrow::array::Int32Array;
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::common::assert_batches_eq;
use datafusion::error::Result;
use datafusion::prelude::*;

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<()> {
    let ctx = SessionContext::new();
    let batch = RecordBatch::try_from_iter(vec![
        ("id", Arc::new(Int32Array::from(vec![1, 2, 3])) as _),
        ("price", Arc::new(Int32Array::from(vec![10, 20, 30])) as _),
    ])?;
    ctx.register_batch("items", batch)?;

    // SQL queries produce a DataFrame. Collect executes the query and returns
    // its results as Arrow record batches.
    let sql_results = ctx
        .sql("SELECT id FROM items WHERE price > 10 ORDER BY id")
        .await?
        .collect()
        .await?;

    // Build the same query with the DataFrame API.
    let dataframe_results = ctx
        .table("items")
        .await?
        .filter(col("price").gt(lit(10)))?
        .select(vec![col("id")])?
        .sort(vec![col("id").sort(true, true)])?
        .collect()
        .await?;

    let expected = ["+----+", "| id |", "+----+", "| 2  |", "| 3  |", "+----+"];
    assert_batches_eq!(expected, &sql_results);
    assert_batches_eq!(expected, &dataframe_results);
    Ok(())
}
