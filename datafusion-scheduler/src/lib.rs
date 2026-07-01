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

//! In-process model of stage-based distributed execution for Apache DataFusion.
//!
//! Splits a physical plan into stages at shuffle boundaries, serializes each
//! stage, and runs each task from a freshly deserialized plan with its own
//! `TaskContext` — surfacing physical-plan changes that assume shared
//! in-process state. Data crosses stage boundaries as IPC-serialized frames
//! over a streaming in-memory exchange (no disk, no barrier). See
//! `specs/datafusion-scheduler.md` in ballista-ai-dev.

mod config;
pub mod exchange;
mod executor;
mod scheduler;
mod serde;
mod stage;
pub mod test_util;

pub use config::SchedulerConfig;
pub use exchange::{
    ExchangeCodec, ExchangeSinkExec, ExchangeSourceExec, InMemoryExchange,
};
pub use executor::execute_stage_graph;
pub use scheduler::create_stages;
pub use stage::{QueryStage, StageGraph, StageId};
pub use test_util::assert_distributed_eq;

use std::sync::Arc;

use datafusion::arrow::array::RecordBatch;
use datafusion::error::Result;
use datafusion::physical_plan::ExecutionPlan;
use datafusion::prelude::SessionContext;

/// Splits `plan` into a [`StageGraph`] at shuffle boundaries and runs it
/// through the streaming, no-barrier stage executor
/// ([`execute_stage_graph`]): all tasks of all stages are spawned
/// concurrently, data crosses each boundary as IPC frames over a single shared
/// [`InMemoryExchange`], and the final stage's collected output is returned.
///
/// `ctx` is the driver session; each task rebuilds an isolated session via
/// `config.session_builder` and never touches `ctx`.
pub async fn run_distributed(
    _ctx: &SessionContext,
    plan: Arc<dyn ExecutionPlan>,
    config: SchedulerConfig,
) -> Result<Vec<RecordBatch>> {
    // One exchange instance is threaded through both stage splitting (so
    // sinks/sources capture it) and the executor (so it registers the channels
    // and drives the tasks) — producers and consumers must share ONE wire.
    let exchange = InMemoryExchange::new();
    let graph = create_stages(plan, &exchange)?;
    execute_stage_graph(&graph, &exchange, &config).await
}
