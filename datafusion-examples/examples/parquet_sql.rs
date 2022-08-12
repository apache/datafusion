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

use datafusion::error::Result;
use datafusion::prelude::*;

/// This example demonstrates executing a simple query against an Arrow data source (Parquet) and
/// fetching results
#[tokio::main]
async fn main() -> Result<()> {
    // create local session context
    let ctx = SessionContext::new();

    let testdata = datafusion::test_util::parquet_test_data();

    // register parquet file with the execution context
    ctx.register_parquet(
        "alltypes_plain",
        &format!("{}/alltypes_plain.parquet", testdata),
        ParquetReadOptions::default(),
    )
    .await?;
    let sql = "select now() - interval '1' day from alltypes_plain limit 2";
    let sql = "select now()::date + interval '1 hour'";
    // execute the query
    let df = ctx.sql(sql).await?;

    // print the results
    df.show().await?;

    Ok(())
}
