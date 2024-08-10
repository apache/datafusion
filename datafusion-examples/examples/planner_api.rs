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
use datafusion::physical_plan::displayable;
use datafusion::physical_planner::DefaultPhysicalPlanner;
use datafusion::prelude::*;
use datafusion_expr::{LogicalPlan, PlanType};

/// This example demonstrates the process of converting logical plan
/// into physical execution plans using DataFusion.
///
/// Planning phase in DataFusion contains several steps:
/// 1. Analyzing and optimizing logical plan
/// 2. Converting logical plan into physical plan
///
/// The code in this example shows two ways to convert a logical plan into
/// physical plan:
/// - Via the combined `create_physical_plan` API.
/// - Utilizing the analyzer, optimizer, and query planner APIs separately.
#[tokio::main]
async fn main() -> Result<()> {
    // Set up a DataFusion context and load a Parquet file
    let ctx = SessionContext::new();
    let testdata = datafusion::test_util::parquet_test_data();
    let df = ctx
        .read_parquet(
            &format!("{testdata}/alltypes_plain.parquet"),
            ParquetReadOptions::default(),
        )
        .await?;

    // Construct the input logical plan using DataFrame API
    let df = df
        .clone()
        .select(vec![
            df.parse_sql_expr("int_col")?,
            df.parse_sql_expr("double_col")?,
        ])?
        .filter(df.parse_sql_expr("int_col < 5 OR double_col = 8.0")?)?
        .aggregate(
            vec![df.parse_sql_expr("double_col")?],
            vec![df.parse_sql_expr("SUM(int_col) as sum_int_col")?],
        )?
        .limit(0, Some(1))?;
    let logical_plan = df.logical_plan().clone();

    to_physical_plan_in_one_api_demo(&logical_plan, &ctx).await?;

    to_physical_plan_step_by_step_demo(logical_plan, &ctx).await?;

    Ok(())
}

/// Converts a logical plan into a physical plan using the combined
/// `create_physical_plan` API. It will first optimize the logical
/// plan and then convert it into physical plan.
async fn to_physical_plan_in_one_api_demo(
    input: &LogicalPlan,
    ctx: &SessionContext,
) -> Result<()> {
    let physical_plan = ctx.state().create_physical_plan(input).await?;

    println!(
        "Physical plan direct from logical plan:\n\n{}\n\n",
        displayable(physical_plan.as_ref())
            .to_stringified(false, PlanType::InitialPhysicalPlan)
            .plan
    );

    Ok(())
}

/// Converts a logical plan into a physical plan by utilizing the analyzer,
/// optimizer, and query planner APIs separately. This flavor gives more
/// control over the planning process.
async fn to_physical_plan_step_by_step_demo(
    input: LogicalPlan,
    ctx: &SessionContext,
) -> Result<()> {
    // First analyze the logical plan
    let analyzed_logical_plan = ctx.state().analyzer().execute_and_check(
        input,
        ctx.state().config_options(),
        |_, _| (),
    )?;
    println!("Analyzed logical plan:\n\n{:?}\n\n", analyzed_logical_plan);

    // Optimize the analyzed logical plan
    let optimized_logical_plan = ctx.state().optimizer().optimize(
        analyzed_logical_plan,
        &ctx.state(),
        |_, _| (),
    )?;
    println!(
        "Optimized logical plan:\n\n{:?}\n\n",
        optimized_logical_plan
    );

    // Create the physical plan
    let physical_plan = ctx
        .state()
        .query_planner()
        .create_physical_plan(&optimized_logical_plan, &ctx.state())
        .await?;
    println!(
        "Final physical plan:\n\n{}\n\n",
        displayable(physical_plan.as_ref())
            .to_stringified(false, PlanType::InitialPhysicalPlan)
            .plan
    );

    // Call the physical optimizer with an existing physical plan (in this
    // case the plan is already optimized, but an unoptimized plan would
    // typically be used in this context)
    // Note that this is not part of the trait but a public method
    // on DefaultPhysicalPlanner. Not all planners will provide this feature.
    let planner = DefaultPhysicalPlanner::default();
    let physical_plan =
        planner.optimize_physical_plan(physical_plan, &ctx.state(), |_, _| {})?;
    println!(
        "Optimized physical plan:\n\n{}\n\n",
        displayable(physical_plan.as_ref())
            .to_stringified(false, PlanType::InitialPhysicalPlan)
            .plan
    );

    Ok(())
}
