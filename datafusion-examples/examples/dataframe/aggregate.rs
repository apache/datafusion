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

use datafusion::error::Result;
use datafusion::functions_aggregate::expr_fn::{avg, count, max, min};
use datafusion::functions_aggregate::min_max::max_udaf;
use datafusion::logical_expr::expr::WindowFunction;
use datafusion::prelude::*;
use datafusion_examples::utils::datasets::ExampleDataset;

/// This example demonstrates aggregation with the DataFrame API.
///
/// `DataFrame::select` accepts aggregate expressions: a list made only of
/// aggregates (and scalar expressions over them) is planned as a global
/// aggregation, exactly like `aggregate(vec![], ...)`. Grouping still goes
/// through `aggregate(group_by, aggs)`.
///
/// * [global_aggregate]: `aggregate(vec![], aggs)` vs `select(aggs)`
/// * [expressions_over_aggregates]: compute on top of aggregates in a single `select`
/// * [grouped_aggregate]: GROUP BY uses `aggregate`; `select` never infers a GROUP BY
/// * [aggregate_vs_window]: aggregate (one row) vs window (one row per input row)
pub async fn aggregate_example() -> Result<()> {
    let ctx = SessionContext::new();
    let dataset = ExampleDataset::Cars;
    ctx.register_csv("cars", dataset.path_str()?, CsvReadOptions::default())
        .await?;

    global_aggregate(&ctx).await?;
    expressions_over_aggregates(&ctx).await?;
    grouped_aggregate(&ctx).await?;
    aggregate_vs_window(&ctx).await?;
    Ok(())
}

/// SELECT count(car) AS n,
///        avg(speed) AS avg_speed,
///        max(speed) AS max_speed
/// FROM cars
async fn global_aggregate(ctx: &SessionContext) -> Result<()> {
    let df = ctx.table("cars").await?;
    let aggs = vec![
        count(col("car")).alias("n"),
        avg(col("speed")).alias("avg_speed"),
        max(col("speed")).alias("max_speed"),
    ];

    // The explicit form: `aggregate` with no group expressions
    println!("global aggregation via aggregate(vec![], aggs)");
    df.clone().aggregate(vec![], aggs.clone())?.show().await?;

    // The same query via `select`: a list containing only aggregates is a
    // global aggregation and returns exactly one row
    println!("global aggregation via select(aggs)");
    df.select(aggs)?.show().await?;

    Ok(())
}

/// SELECT max(speed) - min(speed) AS speed_range,
///        avg(speed) * 3.6        AS avg_speed_kmh
/// FROM cars
async fn expressions_over_aggregates(ctx: &SessionContext) -> Result<()> {
    let df = ctx.table("cars").await?;

    // Before: aggregate first, then project over the aggregate's output columns
    println!("expressions over aggregates via aggregate(...).select(...)");
    df.clone()
        .aggregate(
            vec![],
            vec![
                max(col("speed")).alias("max_speed"),
                min(col("speed")).alias("min_speed"),
                avg(col("speed")).alias("avg_speed"),
            ],
        )?
        .select(vec![
            (col("max_speed") - col("min_speed")).alias("speed_range"),
            (col("avg_speed") * lit(3.6)).alias("avg_speed_kmh"),
        ])?
        .show()
        .await?;

    // Now: scalar expressions over aggregates go straight into `select`,
    // with no intermediate aliases and no second step.
    println!("expressions over aggregates via select(...)");
    df.select(vec![
        (max(col("speed")) - min(col("speed"))).alias("speed_range"),
        (avg(col("speed")) * lit(3.6)).alias("avg_speed_kmh"),
    ])?
    .show()
    .await?;

    Ok(())
}

/// SELECT car,
///        max(speed) AS max_speed
/// FROM cars
/// GROUP BY car
async fn grouped_aggregate(ctx: &SessionContext) -> Result<()> {
    let df = ctx.table("cars").await?;

    // Grouping is always explicit via `aggregate(group_by, aggs)`
    println!("grouped aggregation via aggregate(group_by, aggs)");
    df.aggregate(vec![col("car")], vec![max(col("speed")).alias("max_speed")])?
        .show()
        .await?;

    // `select` never infers a GROUP BY from the non-aggregated columns:
    //
    //     df.select(vec![col("car"), max(col("speed"))])
    //
    // is planned as an ordinary projection and fails when executed with
    // "Aggregate function 'max(cars.speed)' is not supported in this position".
    // Use `aggregate(group_by, aggs)` as above for grouped results.

    Ok(())
}

/// SELECT car,
///        speed,
///        max(speed) OVER (PARTITION BY car) AS max_speed
/// FROM cars
async fn aggregate_vs_window(ctx: &SessionContext) -> Result<()> {
    let df = ctx.table("cars").await?;

    // An aggregate in `select` collapses the input to a single row ...
    println!("aggregate in select: one row for the whole table");
    df.clone()
        .select(vec![max(col("speed")).alias("max_speed")])?
        .show()
        .await?;

    // ... whereas the same function as a window expression keeps every input
    // row and attaches the per-partition value to each of them
    let max_over_car = Expr::WindowFunction(Box::new(WindowFunction::new(
        max_udaf(),
        vec![col("speed")],
    )))
    .partition_by(vec![col("car")])
    .build()?;

    println!("window function in select: one row per input row");
    df.select(vec![
        col("car"),
        col("speed"),
        max_over_car.alias("max_speed"),
    ])?
    .show()
    .await?;

    Ok(())
}
