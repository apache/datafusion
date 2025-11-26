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
//! cargo run --example sql_ops -- [all|analysis|dialect|frontend|query]
//! ```
//!
//! Each subcommand runs a corresponding example:
//! - `all` — run all examples included in this module
//! - `analysis` — analyse SQL queries with DataFusion structures
//! - `dialect` — implementing a custom SQL dialect on top of DFParser
//! - `frontend` — create LogicalPlans (only) from sql strings
//! - `query` — query data using SQL (in memory RecordBatches, local Parquet files)

mod analysis;
mod dialect;
mod frontend;
mod query;

use std::str::FromStr;

use datafusion::error::{DataFusionError, Result};

enum ExampleKind {
    All,
    Analysis,
    Dialect,
    Frontend,
    Query,
}

impl AsRef<str> for ExampleKind {
    fn as_ref(&self) -> &str {
        match self {
            Self::All => "all",
            Self::Analysis => "analysis",
            Self::Dialect => "dialect",
            Self::Frontend => "frontend",
            Self::Query => "query",
        }
    }
}

impl FromStr for ExampleKind {
    type Err = DataFusionError;

    fn from_str(s: &str) -> Result<Self> {
        match s {
            "all" => Ok(Self::All),
            "analysis" => Ok(Self::Analysis),
            "dialect" => Ok(Self::Dialect),
            "frontend" => Ok(Self::Frontend),
            "query" => Ok(Self::Query),
            _ => Err(DataFusionError::Execution(format!("Unknown example: {s}"))),
        }
    }
}

impl ExampleKind {
    const ALL_VARIANTS: [Self; 5] = [
        Self::All,
        Self::Analysis,
        Self::Dialect,
        Self::Frontend,
        Self::Query,
    ];

    const RUNNABLE_VARIANTS: [Self; 4] =
        [Self::Analysis, Self::Dialect, Self::Frontend, Self::Query];

    const EXAMPLE_NAME: &str = "sql_ops";

    fn variants() -> Vec<&'static str> {
        Self::ALL_VARIANTS
            .iter()
            .map(|example| example.as_ref())
            .collect()
    }

    async fn run(&self) -> Result<()> {
        match self {
            ExampleKind::Analysis => analysis::analysis().await?,
            ExampleKind::Dialect => dialect::dialect().await?,
            ExampleKind::Frontend => frontend::frontend()?,
            ExampleKind::Query => query::query().await?,
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
