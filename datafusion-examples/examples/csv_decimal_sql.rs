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

use datafusion::arrow::datatypes::{DataType, Field, Schema};
use datafusion::error::Result;
use datafusion::prelude::*;
use std::sync::Arc;

/// This example demonstrates executing a simple query against an Arrow data source (CSV) and
/// fetching results
#[tokio::main]
async fn main() -> Result<()> {
    // create local execution context
    let mut ctx = ExecutionContext::new();

    let testdata = datafusion::test_util::arrow_test_data();

    // schema with decimal type
    let schema = Arc::new(Schema::new(vec![
        Field::new("c1", DataType::Decimal(10, 6), false),
        Field::new("c2", DataType::Float64, false),
        Field::new("c3", DataType::Boolean, false),
    ]));

    // register csv file with the execution context
    ctx.register_csv(
        "aggregate_simple",
        &format!("{}/csv/aggregate_simple.csv", testdata),
        CsvReadOptions::new().schema(&schema),
    )
    .await?;

    // execute the query
    let df = ctx.sql("select c1 from aggregate_simple").await?;

    // print the results
    df.show().await?;

    Ok(())
}
