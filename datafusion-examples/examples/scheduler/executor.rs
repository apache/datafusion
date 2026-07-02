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

//! Streaming, no-barrier executor for a [`StageGraph`]. Registers every
//! shuffle edge's channels on the shared [`InMemoryExchange`], then spawns
//! EVERY task of EVERY stage concurrently and only then awaits them.
//!
//! There is no per-stage barrier. A consumer task blocked on a channel is
//! unblocked by a producer task that is already running; the stage DAG is
//! acyclic (producers have lower ids) and the channels are bounded SPSC, so
//! spawn-all-then-await over that dataflow cannot deadlock. Only the final
//! stage's task outputs are collected (in ascending task order); producer
//! stages drive the exchange as a side effect.

use std::sync::Arc;

use datafusion::arrow::array::RecordBatch;
use datafusion::common::runtime::SpawnedTask;
use datafusion::error::{DataFusionError, Result};
use futures::StreamExt;

use crate::config::SchedulerConfig;
use crate::exchange::{ExchangeCodec, InMemoryExchange};
use crate::serde::{decode_plan, encode_plan};
use crate::stage::StageGraph;

/// Number of output partitions (== number of tasks) a stage's plan produces.
fn stage_task_count(plan: &Arc<dyn datafusion::physical_plan::ExecutionPlan>) -> usize {
    plan.properties().output_partitioning().partition_count()
}

/// Executes `graph` over the shared streaming `exchange`.
///
/// 1. Register the channels for every producer stage (every stage except the
///    final one) on `exchange` BEFORE spawning any task — a consumer must be
///    able to take its receiver the moment it starts.
/// 2. Encode each stage's plan once, then spawn ALL tasks of ALL stages
///    concurrently (`SpawnedTask`). Each task rebuilds its own
///    `SessionContext`, decodes its own fresh plan instance with a fresh
///    `ExchangeCodec`, executes its one partition, and drains fully. Final
///    tasks keep their batches; producer tasks discard theirs (their work is
///    the frames they push into the exchange).
/// 3. Await ALL handles — no barrier. Propagate the first genuine
///    execution/join error; otherwise return the final stage's batches in
///    ascending task order.
pub async fn execute_stage_graph(
    graph: &StageGraph,
    exchange: &Arc<InMemoryExchange>,
    config: &SchedulerConfig,
) -> Result<Vec<RecordBatch>> {
    // Register every producer stage's channels up front, before any task is
    // spawned. The final stage produces the query result and has no sink, so
    // it registers nothing.
    for stage in &graph.stages {
        if stage.id != graph.final_stage_id {
            let num_tasks = stage_task_count(&stage.plan);
            exchange.register_stage(
                stage.id,
                num_tasks,
                stage.output_partition_count,
                config.channel_capacity,
            );
        }
    }

    // Spawn every task of every stage. Producer tasks go in one bucket (awaited
    // only for errors); the final stage's tasks go in another (awaited for
    // their batches, in ascending task order).
    let mut producer_handles: Vec<SpawnedTask<Result<()>>> = Vec::new();
    let mut final_handles: Option<Vec<SpawnedTask<Result<Vec<RecordBatch>>>>> = None;

    for stage in &graph.stages {
        // Encode this stage's plan ONCE; every task decodes its own instance.
        let codec = ExchangeCodec {
            exchange: exchange.clone(),
        };
        let bytes = encode_plan(&stage.plan, &codec)?;
        let ntasks = stage_task_count(&stage.plan);
        let is_final = stage.id == graph.final_stage_id;

        if is_final {
            let mut handles = Vec::with_capacity(ntasks);
            for t in 0..ntasks {
                let bytes = bytes.clone();
                let session_builder = config.session_builder.clone();
                let exchange = exchange.clone();
                handles.push(SpawnedTask::spawn(async move {
                    let task_ctx = (session_builder)().task_ctx();
                    let codec = ExchangeCodec { exchange };
                    let plan = decode_plan(&bytes, &task_ctx, &codec)?;
                    let mut stream = plan.execute(t, task_ctx)?;
                    let mut batches = Vec::new();
                    while let Some(b) = stream.next().await {
                        batches.push(b?);
                    }
                    Ok::<_, DataFusionError>(batches)
                }));
            }
            final_handles = Some(handles);
        } else {
            for t in 0..ntasks {
                let bytes = bytes.clone();
                let session_builder = config.session_builder.clone();
                let exchange = exchange.clone();
                producer_handles.push(SpawnedTask::spawn(async move {
                    let task_ctx = (session_builder)().task_ctx();
                    let codec = ExchangeCodec { exchange };
                    let plan = decode_plan(&bytes, &task_ctx, &codec)?;
                    let mut stream = plan.execute(t, task_ctx)?;
                    // Drive the sink to completion; it yields no data rows.
                    while let Some(b) = stream.next().await {
                        b?;
                    }
                    Ok::<_, DataFusionError>(())
                }));
            }
        }
    }

    let final_handles = final_handles.ok_or_else(|| {
        DataFusionError::Internal("no stage matched final_stage_id".to_string())
    })?;

    // Await ALL handles (no barrier — everything is already running). Collect
    // the final batches in ascending task order and the first genuine error
    // from any task, whichever bucket it came from.
    let mut first_error: Option<DataFusionError> = None;

    for h in producer_handles {
        match h.await {
            Ok(Ok(())) => {}
            Ok(Err(e)) => {
                first_error.get_or_insert(e);
            }
            Err(e) => {
                first_error.get_or_insert(DataFusionError::Execution(format!(
                    "producer stage task panicked: {e}"
                )));
            }
        }
    }

    let mut out = Vec::new();
    for h in final_handles {
        match h.await {
            Ok(Ok(batches)) => out.extend(batches),
            Ok(Err(e)) => {
                first_error.get_or_insert(e);
            }
            Err(e) => {
                first_error.get_or_insert(DataFusionError::Execution(format!(
                    "final stage task panicked: {e}"
                )));
            }
        }
    }

    if let Some(e) = first_error {
        return Err(e);
    }
    Ok(out)
}
