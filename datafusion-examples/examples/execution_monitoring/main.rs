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
//! This example demonstrates memory and performance management.
//!
//! ## Usage
//! ```bash
//! cargo run --example execution_monitoring -- [mem_pool_exec_plan|mem_pool_tracking|tracing]
//! ```
//!
//! Each subcommand runs a corresponding example:
//! - `mem_pool_exec_plan` — shows how to implement memory-aware ExecutionPlan with memory reservation and spilling
//! - `mem_pool_tracking` — demonstrates TrackConsumersPool for memory tracking and debugging with enhanced error messages
//! - `tracing` — demonstrates the tracing injection feature for the DataFusion runtime

mod memory_pool_execution_plan;
mod memory_pool_tracking;
mod tracing;

use std::str::FromStr;

use datafusion::error::{DataFusionError, Result};

enum ExampleKind {
    MemoryPoolExecutionPlan,
    MemoryPoolTracking,
    Tracing,
}

impl FromStr for ExampleKind {
    type Err = DataFusionError;

    fn from_str(s: &str) -> Result<Self> {
        match s {
            "mem_pool_exec_plan" => Ok(Self::MemoryPoolExecutionPlan),
            "mem_pool_tracking" => Ok(Self::MemoryPoolTracking),
            "tracing" => Ok(Self::Tracing),
            _ => Err(DataFusionError::Execution(format!("Unknown example: {s}"))),
        }
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    let arg = std::env::args().nth(1).ok_or_else(|| {
        eprintln!("Usage: cargo run --example execution_monitoring -- [mem_pool_exec_plan|mem_pool_tracking|tracing]");
        DataFusionError::Execution("Missing argument".to_string())
    })?;

    match arg.parse::<ExampleKind>()? {
        ExampleKind::MemoryPoolExecutionPlan => {
            memory_pool_execution_plan::memory_pool_execution_plan().await?
        }
        ExampleKind::MemoryPoolTracking => {
            memory_pool_tracking::mem_pool_tracking().await?
        }
        ExampleKind::Tracing => tracing::tracing().await?,
    }

    Ok(())
}
