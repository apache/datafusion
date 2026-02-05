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

//! # These are all internal mechanics of the query planning and optimization layers
//!
//! These examples demonstrate internal mechanics of the query planning and optimization layers.
//!
//! ## Usage
//! ```bash
//! cargo run --example query_planning -- [all|analyzer_rule|expr_api|optimizer_rule|parse_sql_expr|plan_to_sql|planner_api|pruning|thread_pools]
//! ```
//!
//! Each subcommand runs a corresponding example:
//! - `all` — run all examples included in this module
//!
//! - `analyzer_rule`
//!   (file: analyzer_rule.rs, desc: Custom AnalyzerRule to change query semantics)
//!
//! - `expr_api`
//!   (file: expr_api.rs, desc: Create, execute, analyze, and coerce Exprs)
//!
//! - `optimizer_rule`
//!   (file: optimizer_rule.rs, desc: Replace predicates via a custom OptimizerRule)
//!
//! - `parse_sql_expr`
//!   (file: parse_sql_expr.rs, desc: Parse SQL into DataFusion Expr)
//!
//! - `plan_to_sql`
//!   (file: plan_to_sql.rs, desc: Generate SQL from expressions or plans)
//!
//! - `planner_api`
//!   (file: planner_api.rs, desc: APIs for logical and physical plan manipulation)
//!
//! - `pruning`
//!   (file: pruning.rs, desc: Use pruning to skip irrelevant files)
//!
//! - `thread_pools`
//!   (file: thread_pools.rs, desc: Configure custom thread pools for DataFusion execution)

mod analyzer_rule;
mod expr_api;
mod optimizer_rule;
mod parse_sql_expr;
mod plan_to_sql;
mod planner_api;
mod pruning;
mod thread_pools;

use datafusion::error::{DataFusionError, Result};
use strum::{IntoEnumIterator, VariantNames};
use strum_macros::{Display, EnumIter, EnumString, VariantNames};

#[derive(EnumIter, EnumString, Display, VariantNames)]
#[strum(serialize_all = "snake_case")]
enum ExampleKind {
    All,
    AnalyzerRule,
    ExprApi,
    OptimizerRule,
    ParseSqlExpr,
    PlanToSql,
    PlannerApi,
    Pruning,
    ThreadPools,
}

impl ExampleKind {
    const EXAMPLE_NAME: &str = "query_planning";

    fn runnable() -> impl Iterator<Item = ExampleKind> {
        ExampleKind::iter().filter(|v| !matches!(v, ExampleKind::All))
    }

    async fn run(&self) -> Result<()> {
        match self {
            ExampleKind::All => {
                for example in ExampleKind::runnable() {
                    println!("Running example: {example}");
                    Box::pin(example.run()).await?;
                }
            }
            ExampleKind::AnalyzerRule => analyzer_rule::analyzer_rule().await?,
            ExampleKind::ExprApi => expr_api::expr_api().await?,
            ExampleKind::OptimizerRule => optimizer_rule::optimizer_rule().await?,
            ExampleKind::ParseSqlExpr => parse_sql_expr::parse_sql_expr().await?,
            ExampleKind::PlanToSql => plan_to_sql::plan_to_sql_examples().await?,
            ExampleKind::PlannerApi => planner_api::planner_api().await?,
            ExampleKind::Pruning => pruning::pruning().await?,
            ExampleKind::ThreadPools => thread_pools::thread_pools().await?,
        }
        Ok(())
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    let usage = format!(
        "Usage: cargo run --example {} -- [{}]",
        ExampleKind::EXAMPLE_NAME,
        ExampleKind::VARIANTS.join("|")
    );

    let example: ExampleKind = std::env::args()
        .nth(1)
        .unwrap_or_else(|| ExampleKind::All.to_string())
        .parse()
        .map_err(|_| DataFusionError::Execution(format!("Unknown example. {usage}")))?;

    example.run().await
}
