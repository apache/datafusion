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
//! - `analyzer_rule` — use a custom AnalyzerRule to change a query's semantics (row level access control)
//! - `expr_api` — create, execute, simplify, analyze and coerce `Expr`s
//! - `optimizer_rule` — use a custom OptimizerRule to replace certain predicates
//! - `parse_sql_expr` — parse SQL text into DataFusion `Expr`
//! - `plan_to_sql` — generate SQL from DataFusion `Expr` and `LogicalPlan`
//! - `planner_api` — APIs to manipulate logical and physical plans
//! - `pruning` — APIs to manipulate logical and physical plans
//! - `thread_pools` — demonstrate TrackConsumersPool for memory tracking and debugging with enhanced error messages and shows how to implement memory-aware ExecutionPlan with memory reservation and spilling

mod analyzer_rule;
mod expr_api;
mod optimizer_rule;
mod parse_sql_expr;
mod plan_to_sql;
mod planner_api;
mod pruning;
mod thread_pools;

use std::str::FromStr;

use datafusion::error::{DataFusionError, Result};

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

impl AsRef<str> for ExampleKind {
    fn as_ref(&self) -> &str {
        match self {
            Self::All => "all",
            Self::AnalyzerRule => "analyzer_rule",
            Self::ExprApi => "expr_api",
            Self::OptimizerRule => "optimizer_rule",
            Self::ParseSqlExpr => "parse_sql_expr",
            Self::PlanToSql => "plan_to_sql",
            Self::PlannerApi => "planner_api",
            Self::Pruning => "pruning",
            Self::ThreadPools => "thread_pools",
        }
    }
}

impl FromStr for ExampleKind {
    type Err = DataFusionError;

    fn from_str(s: &str) -> Result<Self> {
        match s {
            "all" => Ok(Self::All),
            "analyzer_rule" => Ok(Self::AnalyzerRule),
            "expr_api" => Ok(Self::ExprApi),
            "optimizer_rule" => Ok(Self::OptimizerRule),
            "parse_sql_expr" => Ok(Self::ParseSqlExpr),
            "plan_to_sql" => Ok(Self::PlanToSql),
            "planner_api" => Ok(Self::PlannerApi),
            "pruning" => Ok(Self::Pruning),
            "thread_pools" => Ok(Self::ThreadPools),
            _ => Err(DataFusionError::Execution(format!("Unknown example: {s}"))),
        }
    }
}

impl ExampleKind {
    const ALL_VARIANTS: [Self; 9] = [
        Self::All,
        Self::AnalyzerRule,
        Self::ExprApi,
        Self::OptimizerRule,
        Self::ParseSqlExpr,
        Self::PlanToSql,
        Self::PlannerApi,
        Self::Pruning,
        Self::ThreadPools,
    ];

    const RUNNABLE_VARIANTS: [Self; 8] = [
        Self::AnalyzerRule,
        Self::ExprApi,
        Self::OptimizerRule,
        Self::ParseSqlExpr,
        Self::PlanToSql,
        Self::PlannerApi,
        Self::Pruning,
        Self::ThreadPools,
    ];

    const EXAMPLE_NAME: &str = "query_planning";

    fn variants() -> Vec<&'static str> {
        Self::ALL_VARIANTS
            .iter()
            .map(|example| example.as_ref())
            .collect()
    }

    async fn run(&self) -> Result<()> {
        match self {
            ExampleKind::AnalyzerRule => analyzer_rule::analyzer_rule().await?,
            ExampleKind::ExprApi => expr_api::expr_api().await?,
            ExampleKind::OptimizerRule => optimizer_rule::optimizer_rule().await?,
            ExampleKind::ParseSqlExpr => parse_sql_expr::parse_sql_expr().await?,
            ExampleKind::PlanToSql => plan_to_sql::plan_to_sql_examples().await?,
            ExampleKind::PlannerApi => planner_api::planner_api().await?,
            ExampleKind::Pruning => pruning::pruning().await?,
            ExampleKind::ThreadPools => thread_pools::thread_pools().await?,
            ExampleKind::All => unreachable!("`All` should be handled in main"),
        }
        Ok(())
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    let usage = format!(
        "Usage: cargo run --example {} -- [{}]",
        ExampleKind::EXAMPLE_NAME,
        ExampleKind::variants().join("|")
    );

    let arg = std::env::args().nth(1).ok_or_else(|| {
        eprintln!("{usage}");
        DataFusionError::Execution("Missing argument".to_string())
    })?;

    match arg.parse::<ExampleKind>()? {
        ExampleKind::All => {
            for example in ExampleKind::RUNNABLE_VARIANTS {
                println!("Running example: {}", example.as_ref());
                example.run().await?;
            }
        }
        example => example.run().await?,
    }

    Ok(())
}
