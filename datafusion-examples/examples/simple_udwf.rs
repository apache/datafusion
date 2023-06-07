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

use datafusion::datasource::file_format::options::CsvReadOptions;

use datafusion::error::Result;
use datafusion::prelude::*;

// create local execution context with `cars.csv` registered as a table named `cars`
async fn create_context() -> Result<SessionContext> {
    // declare a new context. In spark API, this corresponds to a new spark SQLsession
    let ctx = SessionContext::new();

    // declare a table in memory. In spark API, this corresponds to createDataFrame(...).
    println!("pwd: {}", std::env::current_dir().unwrap().display());
    let csv_path = format!("datafusion/core/tests/data/cars.csv");
    let read_options = CsvReadOptions::default().has_header(true);

    ctx.register_csv("cars", &csv_path, read_options).await?;
    Ok(ctx)
}

/// In this example we will declare a user defined window function that computes a moving average and then run it using SQL
#[tokio::main]
async fn main() -> Result<()> {
    let ctx = create_context().await?;

    // Use SQL to run the new window function
    let df = ctx.sql("SELECT * from cars").await?;
    // print the results
    df.show().await?;

    // Use SQL to run the new window function
    // `PARTITION BY car`:each distinct value of car (red, and green) should be treated separately
    // `ORDER BY time`: within each group (greed or green) the values will be orderd by time
    let df = ctx
        .sql(
            "SELECT car, \
                      speed, \
                      lag(speed, 1) OVER (PARTITION BY car ORDER BY time),\
                      time \
                      from cars",
        )
        .await?;
    // print the results
    df.show().await?;

    // ROWS BETWEEN 2 PRECEDING AND 2 FOLLOWING: Run the window functon so that each invocation only sees 5 rows: the 2 before and 2 after) using
    let df = ctx.sql("SELECT car, \
                      speed, \
                      lag(speed, 1) OVER (PARTITION BY car ORDER BY time ROWS BETWEEN 2 PRECEDING AND 2 FOLLOWING),\
                      time \
                      from cars").await?;
    // print the results
    df.show().await?;

    // todo show how to run dataframe API as well

    Ok(())
}
