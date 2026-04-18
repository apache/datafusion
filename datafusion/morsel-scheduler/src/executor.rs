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

//! Top-level entry point: run an `ExecutionPlan` on a
//! [`WorkerPool`].
//!
//! The executor cuts the plan into pipelines (see
//! [`planner`](crate::planner)), wraps each pipeline's leaves with
//! [`WorkerDispatchExec`] so internal `RepartitionExec` fan-out spreads
//! across workers, then schedules each output partition of each
//! pipeline on a worker. Morsels flow through the inboxes between
//! pipelines; the final pipeline's output is bridged back as a normal
//! [`SendableRecordBatchStream`].

use std::sync::Arc;

use arrow::record_batch::RecordBatch;
use datafusion_common::Result;
use datafusion_common::error::DataFusionError;
use datafusion_execution::TaskContext;
use datafusion_physical_plan::stream::RecordBatchReceiverStreamBuilder;
use datafusion_physical_plan::{ExecutionPlan, SendableRecordBatchStream};
use futures::StreamExt;
use tokio::sync::mpsc::Sender as MpscSender;

use crate::dispatch::wrap_leaves;
use crate::inbox::{DEFAULT_INBOX_CAPACITY, InboxSender};
use crate::planner::plan_to_pipelines_with_capacity;
use crate::runtime::WorkerPool;

/// Options that control how [`execute`] schedules a plan.
#[derive(Debug, Clone)]
pub struct ExecuteOptions {
    /// Bounded capacity of inter-pipeline inboxes in batches.
    pub inbox_capacity: usize,
    /// If `true`, wrap the leaves of each pipeline's rewritten plan
    /// with [`WorkerDispatchExec`] so every leaf partition executes
    /// on a specific worker. This is the v1 workaround for
    /// `RepartitionExec`'s shared-runtime lazy-spawn.
    pub wrap_leaves_for_dispatch: bool,
}

impl Default for ExecuteOptions {
    fn default() -> Self {
        Self {
            inbox_capacity: DEFAULT_INBOX_CAPACITY,
            wrap_leaves_for_dispatch: true,
        }
    }
}

/// Run `plan` on `pool`, returning a stream of its morsels.
pub fn execute(
    plan: &Arc<dyn ExecutionPlan>,
    task_ctx: &Arc<TaskContext>,
    pool: &Arc<WorkerPool>,
) -> Result<SendableRecordBatchStream> {
    execute_with_options(plan, task_ctx, pool, &ExecuteOptions::default())
}

/// Same as [`execute`] but with explicit options.
pub fn execute_with_options(
    plan: &Arc<dyn ExecutionPlan>,
    task_ctx: &Arc<TaskContext>,
    pool: &Arc<WorkerPool>,
    options: &ExecuteOptions,
) -> Result<SendableRecordBatchStream> {
    let graph = plan_to_pipelines_with_capacity(plan, options.inbox_capacity)?;
    let final_idx = graph.final_pipeline;
    let output_schema = graph.pipelines[final_idx].plan.schema();
    let output_partitions = graph.pipelines[final_idx].partition_count();

    // Bridge back to caller: a channel sized to let workers fill while
    // the consumer is slow, without runaway buffering.
    let channel_cap = (output_partitions * 2).max(2);
    let mut caller_builder =
        RecordBatchReceiverStreamBuilder::new(output_schema, channel_cap);
    let caller_tx = caller_builder.tx();

    let worker_count = pool.worker_count();

    for (idx, pipeline) in graph.pipelines.into_iter().enumerate() {
        let is_final = idx == final_idx;

        // Optionally wrap leaves so scans on RepartitionExec inputs
        // distribute across workers.
        let plan = if options.wrap_leaves_for_dispatch {
            wrap_leaves(Arc::clone(&pipeline.plan), pool)?
        } else {
            Arc::clone(&pipeline.plan)
        };

        let n = plan.properties().partitioning.partition_count();
        for p in 0..n {
            let plan = Arc::clone(&plan);
            let task_ctx = Arc::clone(task_ctx);
            let pool_inner = Arc::clone(pool);
            let worker_id = p % worker_count;

            let sink = if is_final {
                Sink::Caller(caller_tx.clone())
            } else {
                let senders = pipeline
                    .output_senders
                    .as_ref()
                    .expect("non-final pipeline missing senders");
                Sink::Inbox(senders[p].clone())
            };

            caller_builder.spawn(async move {
                let job = pool_inner
                    .spawn_on(worker_id, move || run_partition(plan, p, task_ctx, sink));
                match job.await {
                    Ok(Ok(())) => {}
                    Ok(Err(e)) => log::debug!("morsel pipeline partition errored: {e}"),
                    Err(e) => log::debug!("morsel pool dispatch errored: {e}"),
                }
                Ok(())
            });
        }
    }

    // Drop our caller_tx so the stream terminates when the last
    // pipeline task is done.
    drop(caller_tx);
    Ok(caller_builder.build())
}

/// Destination for a pipeline partition's output batches.
enum Sink {
    /// Downstream pipeline's inbox (non-final pipelines).
    Inbox(InboxSender),
    /// Caller-facing channel (final pipeline).
    Caller(MpscSender<Result<RecordBatch>>),
}

impl Sink {
    async fn forward(&self, item: Result<RecordBatch>) -> std::result::Result<(), ()> {
        match self {
            Sink::Inbox(s) => s.send(item).await.map_err(|_| ()),
            Sink::Caller(tx) => tx.send(item).await.map_err(|_| ()),
        }
    }
}

async fn run_partition(
    plan: Arc<dyn ExecutionPlan>,
    partition: usize,
    task_ctx: Arc<TaskContext>,
    sink: Sink,
) -> Result<()> {
    let mut stream = match plan.execute(partition, task_ctx) {
        Ok(s) => s,
        Err(e) => {
            let _ = sink.forward(Err(e)).await;
            return Ok(());
        }
    };
    while let Some(batch) = stream.next().await {
        let is_err = batch.is_err();
        if sink.forward(batch).await.is_err() {
            // Consumer is gone; stop producing.
            break;
        }
        if is_err {
            break;
        }
    }
    // Drop `sink`: when all senders/inbox-senders are dropped, the
    // receiver observes end-of-stream.
    drop(sink);
    Ok::<_, DataFusionError>(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::Int32Array;
    use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
    use arrow::record_batch::RecordBatch;
    use datafusion_physical_plan::coalesce_partitions::CoalescePartitionsExec;
    use datafusion_physical_plan::test::TestMemoryExec;

    fn schema() -> SchemaRef {
        Arc::new(Schema::new(vec![Field::new("x", DataType::Int32, false)]))
    }

    fn batch(schema: &SchemaRef, vals: &[i32]) -> RecordBatch {
        RecordBatch::try_new(
            Arc::clone(schema),
            vec![Arc::new(Int32Array::from(vals.to_vec())) as _],
        )
        .unwrap()
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn run_simple_coalesce_plan() {
        let s = schema();
        // Two partitions, each with one batch.
        let partitions = vec![vec![batch(&s, &[1, 2, 3])], vec![batch(&s, &[10, 20])]];
        let mem: Arc<dyn ExecutionPlan> =
            TestMemoryExec::try_new_exec(&partitions, Arc::clone(&s), None).unwrap();
        let root: Arc<dyn ExecutionPlan> = Arc::new(CoalescePartitionsExec::new(mem));

        let pool = WorkerPool::new(2).unwrap();
        let task_ctx = Arc::new(TaskContext::default());

        let mut stream = execute(&root, &task_ctx, &pool).unwrap();
        let mut rows = 0;
        while let Some(batch) = stream.next().await {
            rows += batch.unwrap().num_rows();
        }
        assert_eq!(rows, 5);
    }
}
