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

//! This file contains several examples of how to run SQL queries using DataFusion
//!
//! * [`parquet`]: demonstrates how to run a simple SQL query against a Parquet file

use datafusion::error::Result;
use datafusion::prelude::*;

#[tokio::main]
async fn main() -> Result<()> {
    parquet().await;
    Ok(())

}

/// This example demonstrates executing a simple query against an Arrow data
/// source (Parquet) and fetching results
async fn parquet() -> Result<()> {
    // create local session context
    let ctx = SessionContext::new();

    let testdata = datafusion::test_util::parquet_test_data();

    // register parquet file with the execution context
    ctx.register_parquet(
        "alltypes_plain",
        &format!("{testdata}/alltypes_plain.parquet"),
        ParquetReadOptions::default(),
    )
    .await?;

    // execute the query
    let df = ctx
        .sql(
            "SELECT int_col, double_col, CAST(date_string_col as VARCHAR) \
        FROM alltypes_plain \
        WHERE id > 1 AND tinyint_col < double_col",
        )
        .await?;

    // print the results
    df.show().await?;

    Ok(())
}
