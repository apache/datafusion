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

//! See `main.rs` for how to run it.

use datafusion::common::test_util::datafusion_test_data;
use datafusion::error::Result;
use datafusion::prelude::*;

/// This example demonstrates executing a simple query against an Arrow data source (CSV) and
/// fetching results with streaming aggregation and streaming window
pub async fn csv_sql_streaming() -> Result<()> {
    // create local execution context
    let ctx = SessionContext::new();

    let testdata = datafusion_test_data();

    // Register a table source and tell DataFusion the file is ordered by `ts ASC`.
    // Note it is the responsibility of the user to make sure
    // that file indeed satisfies this condition or else incorrect answers may be produced.
    let asc = true;
    let nulls_first = true;
    let sort_expr = vec![col("ts").sort(asc, nulls_first)];
    // register csv file with the execution context
    ctx.register_csv(
        "ordered_table",
        &format!("{testdata}/window_1.csv"),
        CsvReadOptions::new().file_sort_order(vec![sort_expr]),
    )
    .await?;

    // execute the query
    // Following query can be executed with unbounded sources because group by expressions (e.g ts) is
    // already ordered at the source.
    //
    // Unbounded sources means that if the input came from a "never ending" source (such as a FIFO
    // file on unix) the query could produce results incrementally as data was read.
    let df = ctx
        .sql(
            "SELECT ts, MIN(inc_col), MAX(inc_col) \
        FROM ordered_table \
        GROUP BY ts",
        )
        .await?;

    df.show().await?;

    // execute the query
    // Following query can be executed with unbounded sources because window executor can calculate
    // its result in streaming fashion, because its required ordering is already satisfied at the source.
    let df = ctx
        .sql(
            "SELECT ts, SUM(inc_col) OVER(ORDER BY ts ASC) \
        FROM ordered_table",
        )
        .await?;

    df.show().await?;

    Ok(())
}
