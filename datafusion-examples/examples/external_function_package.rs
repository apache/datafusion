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
use datafusion_extension_test_scalar_func::TestFunctionPackage;

/// This example demonstrates executing a simple query against an Arrow data source (CSV) and
/// fetching results
#[tokio::main]
async fn main() -> Result<()> {
    let ctx = SessionContext::new();
    let testdata = datafusion::test_util::arrow_test_data();
    ctx.register_csv(
        "aggregate_test_100",
        &format!("{testdata}/csv/aggregate_test_100.csv"),
        CsvReadOptions::new(),
    )
    .await?;

    // Register add_one(x), multiply_two(x) function from `TestFunctionPackage`
    ctx.register_scalar_function_package(Box::new(TestFunctionPackage));

    let df = ctx
        .sql("select add_one(1), multiply_two(c3), add_one(multiply_two(c4)) from aggregate_test_100 limit 5").await?;
    df.show().await?;

    Ok(())
}
