// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: See the NOTICE file distributed with this work for additional information regarding copyright ownership
// SPDX-FileContributor: Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements

//! # Pluggable operator statistics (`StatisticsRegistry`)
//!
//! These examples show how a `StatisticsProvider` registered on the session can
//! change a physical plan by supplying statistics the built-in estimators
//! cannot express.
//!
//! ## Usage
//! ```bash
//! cargo run --example statistics -- [all|join_reorder]
//! ```
//!
//! Each subcommand runs a corresponding example:
//! - `all`: run all examples included in this module
//!
//! - `join_reorder`
//!   (file: join_reorder.rs, desc: Supply catalog column statistics via a provider to flip a join order)

mod join_reorder;

use datafusion::error::{DataFusionError, Result};
use strum::{IntoEnumIterator, VariantNames};
use strum_macros::{Display, EnumIter, EnumString, VariantNames};

#[derive(EnumIter, EnumString, Display, VariantNames)]
#[strum(serialize_all = "snake_case")]
enum ExampleKind {
    All,
    JoinReorder,
}

impl ExampleKind {
    const EXAMPLE_NAME: &str = "statistics";

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
                Ok(())
            }
            ExampleKind::JoinReorder => join_reorder::join_reorder().await,
        }
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
