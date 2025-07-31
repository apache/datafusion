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

async fn register_aggregate_test_data(name: &str, ctx: &SessionContext) -> Result<()> {
    let testdata = datafusion::test_util::arrow_test_data();
    ctx.register_csv(
        name,
        &format!("{testdata}/csv/aggregate_test_100.csv"),
        CsvReadOptions::default(),
    )
    .await?;
    Ok(())
}

#[tokio::main]
async fn main() -> Result<()> {
    // create execution context
    let ctx = SessionContext::new();

    // register the same data twice so we can join it
    register_aggregate_test_data("t1", &ctx).await?;
    register_aggregate_test_data("t2", &ctx).await?;

    // enable memory profiling for the next query
    let _profile = ctx.enable_memory_profiling();

    // run a multi-stage query that joins and aggregates
    let df = ctx
        .sql(
            r#"
            SELECT t1.c1, COUNT(*) AS cnt
            FROM t1 JOIN t2 ON t1.c1 = t2.c1
            GROUP BY t1.c1
            ORDER BY cnt DESC
            "#,
        )
        .await?;

    df.show().await?;

    // print memory usage collected by the profiler
    println!("\nMemory profile:");
    for (op, bytes) in ctx.get_last_query_memory_report() {
        println!("{op}: {bytes}");
    }

    Ok(())
}
