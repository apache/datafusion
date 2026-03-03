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

//! # SQL Examples
//!
//! These examples demonstrate SQL operations in DataFusion.
//!
//! ## Usage
//! ```bash
//! cargo run --example sql_ops -- [all|analysis|custom_sql_parser|frontend|query]
//! ```
//!
//! Each subcommand runs a corresponding example:
//! - `all` — run all examples included in this module
//!
//! - `analysis`
//!   (file: analysis.rs, desc: Analyze SQL queries)
//!
//! - `custom_sql_parser`
//!   (file: custom_sql_parser.rs, desc: Implement a custom SQL parser to extend DataFusion)
//!
//! - `frontend`
//!   (file: frontend.rs, desc: Build LogicalPlans from SQL)
//!
//! - `query`  
//!   (file: query.rs, desc: Query data using SQL)

mod analysis;
mod custom_sql_parser;
mod frontend;
mod query;

use datafusion::error::{DataFusionError, Result};
use strum::{IntoEnumIterator, VariantNames};
use strum_macros::{Display, EnumIter, EnumString, VariantNames};

#[derive(EnumIter, EnumString, Display, VariantNames)]
#[strum(serialize_all = "snake_case")]
enum ExampleKind {
    All,
    Analysis,
    CustomSqlParser,
    Frontend,
    Query,
}

impl ExampleKind {
    const EXAMPLE_NAME: &str = "sql_ops";

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
            ExampleKind::Analysis => analysis::analysis().await?,
            ExampleKind::CustomSqlParser => {
                custom_sql_parser::custom_sql_parser().await?
            }
            ExampleKind::Frontend => frontend::frontend()?,
            ExampleKind::Query => query::query().await?,
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
