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

use std::str::FromStr;

use datafusion::error::{DataFusionError, Result};

enum ExampleKind {
    All,
    MemoryPoolExecutionPlan,
    MemoryPoolTracking,
    Tracing,
}

impl AsRef<str> for ExampleKind {
    fn as_ref(&self) -> &str {
        match self {
            Self::All => "all",
            Self::MemoryPoolExecutionPlan => "mem_pool_exec_plan",
            Self::MemoryPoolTracking => "mem_pool_tracking",
            Self::Tracing => "tracing",
        }
    }
}

impl FromStr for ExampleKind {
    type Err = DataFusionError;

    fn from_str(s: &str) -> Result<Self> {
        match s {
            "all" => Ok(Self::All),
            "mem_pool_exec_plan" => Ok(Self::MemoryPoolExecutionPlan),
            "mem_pool_tracking" => Ok(Self::MemoryPoolTracking),
            "tracing" => Ok(Self::Tracing),
            _ => Err(DataFusionError::Execution(format!("Unknown example: {s}"))),
        }
    }
}

impl ExampleKind {
    const ALL_VARIANTS: [Self; 4] = [
        Self::All,
        Self::MemoryPoolExecutionPlan,
        Self::MemoryPoolTracking,
        Self::Tracing,
    ];

    const RUNNABLE_VARIANTS: [Self; 3] = [
        Self::MemoryPoolExecutionPlan,
        Self::MemoryPoolTracking,
        Self::Tracing,
    ];

    const EXAMPLE_NAME: &str = "execution_monitoring";

    fn variants() -> Vec<&'static str> {
        Self::ALL_VARIANTS
            .iter()
            .map(|example| example.as_ref())
            .collect()
    }

    async fn run(&self) -> Result<()> {
        match self {
            ExampleKind::MemoryPoolExecutionPlan => {
                memory_pool_execution_plan::memory_pool_execution_plan().await?
            }
            ExampleKind::MemoryPoolTracking => {
                memory_pool_tracking::mem_pool_tracking().await?
            }
            ExampleKind::Tracing => tracing::tracing().await?,
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
