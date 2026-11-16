// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: See the NOTICE file distributed with this work for additional information regarding copyright ownership
// SPDX-FileContributor: Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements

//! # These examples of memory and performance management
//!
//! These examples demonstrate memory and performance management.
//!
//! ## Usage
//! ```bash
//! cargo run --example execution_monitoring -- [all|mem_pool_exec_plan|mem_pool_tracking|tracing]
//! ```
//!
//! Each subcommand runs a corresponding example:
//! - `all` — run all examples included in this module
//!
//! - `mem_pool_exec_plan`
//!   (file: memory_pool_execution_plan.rs, desc: Memory-aware ExecutionPlan with spilling)
//!
//! - `mem_pool_tracking`
//!   (file: memory_pool_tracking.rs, desc: Demonstrates memory tracking)
//!
//! - `tracing`
//!   (file: tracing.rs, desc: Demonstrates tracing integration)

mod memory_pool_execution_plan;
mod memory_pool_tracking;
mod tracing;

use datafusion::error::{DataFusionError, Result};
use strum::{IntoEnumIterator, VariantNames};
use strum_macros::{Display, EnumIter, EnumString, VariantNames};

#[derive(EnumIter, EnumString, Display, VariantNames)]
#[strum(serialize_all = "snake_case")]
enum ExampleKind {
    All,
    MemPoolExecPlan,
    MemPoolTracking,
    Tracing,
}

impl ExampleKind {
    const EXAMPLE_NAME: &str = "execution_monitoring";

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
            ExampleKind::MemPoolExecPlan => {
                memory_pool_execution_plan::memory_pool_execution_plan().await?
            }
            ExampleKind::MemPoolTracking => {
                memory_pool_tracking::mem_pool_tracking().await?
            }
            ExampleKind::Tracing => tracing::tracing().await?,
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
