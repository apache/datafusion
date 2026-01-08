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

//! # Relation Planner Examples
//!
//! These examples demonstrate how to use custom relation planners to extend
//! DataFusion's SQL syntax with custom table operators.
//!
//! ## Usage
//! ```bash
//! cargo run --example relation_planner -- [all|match_recognize|pivot_unpivot|table_sample]
//! ```
//!
//! Each subcommand runs a corresponding example:
//! - `all` — run all examples included in this module
//! - `match_recognize` — MATCH_RECOGNIZE pattern matching on event streams
//! - `pivot_unpivot` — PIVOT and UNPIVOT operations for reshaping data
//! - `table_sample` — TABLESAMPLE clause for sampling rows from tables
//!
//! ## Snapshot Testing
//!
//! These examples use [insta](https://insta.rs) for inline snapshot assertions.
//! If query output changes, regenerate the snapshots with:
//! ```bash
//! cargo insta test --example relation_planner --accept
//! ```

mod match_recognize;
mod pivot_unpivot;
mod table_sample;

use datafusion::error::{DataFusionError, Result};
use strum::{IntoEnumIterator, VariantNames};
use strum_macros::{Display, EnumIter, EnumString, VariantNames};

#[derive(EnumIter, EnumString, Display, VariantNames)]
#[strum(serialize_all = "snake_case")]
enum ExampleKind {
    All,
    MatchRecognize,
    PivotUnpivot,
    TableSample,
}

impl ExampleKind {
    const EXAMPLE_NAME: &str = "relation_planner";

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
            ExampleKind::MatchRecognize => match_recognize::match_recognize().await?,
            ExampleKind::PivotUnpivot => pivot_unpivot::pivot_unpivot().await?,
            ExampleKind::TableSample => table_sample::table_sample().await?,
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

/// Test wrappers that enable `cargo insta test --example relation_planner --accept`
/// to regenerate inline snapshots. Without these, insta cannot run the examples
/// in test mode since they only have `main()` functions.
#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_match_recognize() {
        match_recognize::match_recognize().await.unwrap();
    }

    #[tokio::test]
    async fn test_pivot_unpivot() {
        pivot_unpivot::pivot_unpivot().await.unwrap();
    }

    #[tokio::test]
    async fn test_table_sample() {
        table_sample::table_sample().await.unwrap();
    }
}
