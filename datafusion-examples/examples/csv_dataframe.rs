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
use datafusion::functions_aggregate::count::count;
use datafusion::prelude::*;

/// This example demonstrates executing a DataFrame operation against an Arrow data source (CSV) and
/// fetching results. See `csv_sql.rs` for a SQL version of this example.
#[tokio::main]
async fn main() -> Result<()> {
    // create local execution context
    let ctx = SessionContext::new();

    let testdata = datafusion::test_util::arrow_test_data();

    // execute the query
    let df = ctx
        .read_csv(
            &format!("{testdata}/csv/aggregate_test_100.csv"),
            CsvReadOptions::new(),
        )
        .await?
        .filter(col("c11").gt(lit(0.1)).and(col("c11").lt(lit(0.9))))?
        .aggregate(
            vec![col("c1")],
            vec![min(col("c12")), max(col("c12")), count(wildcard())],
        )?;

    // print the results
    df.show().await?;

    Ok(())
}
