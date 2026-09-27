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
//! cargo run --example execution_monitoring -- [all|mem_pool_exec_plan|mem_pool_tracking|stage_pause|stage_admission|stage_dependencies|tracing]
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
//! - `stage_pause`
//!   (file: staged_execution.rs, desc: Pause and resume a boundary and measure its drain time)
//!
//! - `stage_admission`
//!   (file: staged_execution.rs, desc: Defer boundary execution until memory is available)
//!
//! - `stage_dependencies`
//!   (file: staged_execution.rs, desc: Execute dependent boundaries in plan order)
//!
//! - `tracing`
//!   (file: tracing.rs, desc: Demonstrates tracing integration)

mod memory_pool_execution_plan;
mod memory_pool_tracking;
mod staged_execution;
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
    StagePause,
    StageAdmission,
    StageDependencies,
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
            ExampleKind::StagePause => staged_execution::pause_and_resume().await?,
            ExampleKind::StageAdmission => staged_execution::memory_admission().await?,
            ExampleKind::StageDependencies => {
                staged_execution::dependent_boundaries().await?
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
