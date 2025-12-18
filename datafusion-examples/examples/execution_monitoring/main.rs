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
//! - `mem_pool_exec_plan` — shows how to implement memory-aware ExecutionPlan with memory reservation and spilling
//! - `mem_pool_tracking` — demonstrates TrackConsumersPool for memory tracking and debugging with enhanced error messages
//! - `tracing` — demonstrates the tracing injection feature for the DataFusion runtime

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
        .ok_or_else(|| DataFusionError::Execution(format!("Missing argument. {usage}")))?
        .parse()
        .map_err(|_| DataFusionError::Execution(format!("Unknown example. {usage}")))?;

    example.run().await
}
