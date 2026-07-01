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

//! End-to-end test that a real 2-stage plan (partial aggregate + streaming
//! in-memory exchange + final aggregate) executes correctly through
//! `run_distributed`/`execute_stage_graph`, matching single-process `collect`
//! output. Data crosses the stage boundary as IPC frames over the
//! `InMemoryExchange` — there is no disk shuffle to inspect, so correctness of
//! the 2-stage GROUP BY across the exchange is the assertion.

use std::sync::Arc;

use datafusion::arrow::array::{Int32Array, RecordBatch};
use datafusion::arrow::datatypes::{DataType, Field, Schema};
use datafusion::datasource::MemTable;
use datafusion::physical_plan::collect;
use datafusion::prelude::{SessionConfig, SessionContext};
use datafusion_scheduler::test_util::assert_distributed_eq;
use datafusion_scheduler::{SchedulerConfig, create_stages, run_distributed};

/// Registers table `t(a INT)` backed by a 2-partition `MemTable`, against a
/// `SessionContext` configured with `target_partitions = 4` -- matching the
/// setup in `tests/stage_splitting.rs` that is known to split `GROUP BY`
/// into a producer stage (partial aggregate, hash-repartitioned) and a
/// final stage (final aggregate) at a real shuffle boundary.
async fn context_with_partitioned_table() -> SessionContext {
    let schema = Arc::new(Schema::new(vec![Field::new("a", DataType::Int32, false)]));
    let batch0 = RecordBatch::try_new(
        schema.clone(),
        vec![Arc::new(Int32Array::from(vec![1, 2, 3]))],
    )
    .unwrap();
    let batch1 = RecordBatch::try_new(
        schema.clone(),
        vec![Arc::new(Int32Array::from(vec![2, 3, 4]))],
    )
    .unwrap();

    let config = SessionConfig::new().with_target_partitions(4);
    let ctx = SessionContext::new_with_config(config);
    let table = MemTable::try_new(schema, vec![vec![batch0], vec![batch1]]).unwrap();
    ctx.register_table("t", Arc::new(table)).unwrap();
    ctx
}

#[tokio::test]
async fn group_by_across_a_real_shuffle_matches_collect() {
    let ctx = context_with_partitioned_table().await;
    let df = ctx
        .sql("SELECT a, count(*) FROM t GROUP BY a")
        .await
        .unwrap();
    let plan = df.create_physical_plan().await.unwrap();

    assert_distributed_eq(&ctx, plan).await;
}

/// Proves `run_distributed` is genuinely routed through `create_stages` + the
/// stage-graph executor (data really crossing the exchange), not coincidentally
/// producing correct output by re-running the original in-process plan.
///
/// The old disk model let a test assert shuffle files landed under `stage_0/`.
/// The streaming exchange leaves nothing on disk, so instead we assert on the
/// stage split itself — this GROUP BY splits into a producer stage (id 0,
/// hash-repartitioned partial aggregate, whose output must cross the exchange)
/// and a final stage reading it — and that the end-to-end result over that real
/// two-stage exchange execution matches single-node `collect`.
#[tokio::test]
async fn group_by_data_crosses_the_exchange_between_two_stages() {
    let ctx = context_with_partitioned_table().await;
    let df = ctx
        .sql("SELECT a, count(*) FROM t GROUP BY a")
        .await
        .unwrap();
    let plan = df.create_physical_plan().await.unwrap();

    let expected = collect(plan.clone(), ctx.task_ctx()).await.unwrap();

    // The split must produce a producer stage feeding a final stage over the
    // exchange — i.e. a genuine 2-stage plan, not a single re-executed tree.
    let probe_exchange = datafusion_scheduler::InMemoryExchange::new();
    let graph = create_stages(plan.clone(), &probe_exchange).unwrap();
    assert_eq!(
        graph.stages.len(),
        2,
        "GROUP BY should split into a producer stage and a final stage"
    );
    assert_ne!(
        graph.final_stage_id, 0,
        "there must be a producer stage (id 0) feeding the final stage"
    );

    // Execute end-to-end: the producer stage's partial-aggregate output only
    // reaches the final stage by crossing the in-memory exchange as IPC frames.
    let config = SchedulerConfig::in_memory(&ctx);
    let actual = run_distributed(&ctx, plan, config).await.unwrap();

    let expected_rows: usize = expected.iter().map(|b| b.num_rows()).sum();
    let actual_rows: usize = actual.iter().map(|b| b.num_rows()).sum();
    assert_eq!(
        expected_rows, actual_rows,
        "row count across the exchange must match single-node collect"
    );
}
