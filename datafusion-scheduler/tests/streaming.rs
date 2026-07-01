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

//! Streaming / backpressure integration test.
//!
//! Runs a real multi-stage query end-to-end through `run_distributed` with
//! `channel_capacity = 1` — the tightest possible backpressure through the FULL
//! executor: producers block on full channels while downstream consumers drain.
//! The whole run is wrapped in a `tokio::time::timeout` so a deadlock FAILS the
//! test rather than hanging CI.
//!
//! This is the end-to-end proof that the spawn-all, no-barrier executor
//! pipelines correctly under backpressure: a consumer blocked on a channel is
//! unblocked by a producer that is already running (all tasks of all stages are
//! spawned before any is awaited). If the executor used a per-stage barrier
//! instead, capacity-1 channels would deadlock and this test would time out.

use std::sync::Arc;
use std::time::Duration;

use datafusion::arrow::array::{Int32Array, RecordBatch};
use datafusion::arrow::datatypes::{DataType, Field, Schema};
use datafusion::datasource::MemTable;
use datafusion::physical_plan::collect;
use datafusion::prelude::{SessionConfig, SessionContext};
use datafusion_scheduler::{SchedulerConfig, run_distributed};

/// Registers table `t(a INT)` backed by a 2-partition `MemTable` with enough
/// rows/batches that capacity-1 channels actually force repeated blocking,
/// against a `SessionContext` at `target_partitions = 4`.
async fn context_with_many_rows() -> SessionContext {
    let schema = Arc::new(Schema::new(vec![Field::new("a", DataType::Int32, false)]));

    // Many small batches per partition so the sink blocks on `send().await`
    // over and over against the capacity-1 channels.
    let mut p0 = Vec::new();
    let mut p1 = Vec::new();
    for i in 0..200i32 {
        p0.push(
            RecordBatch::try_new(
                schema.clone(),
                vec![Arc::new(Int32Array::from(vec![i % 17]))],
            )
            .unwrap(),
        );
        p1.push(
            RecordBatch::try_new(
                schema.clone(),
                vec![Arc::new(Int32Array::from(vec![(i + 5) % 17]))],
            )
            .unwrap(),
        );
    }

    let config = SessionConfig::new().with_target_partitions(4);
    let ctx = SessionContext::new_with_config(config);
    let table = MemTable::try_new(schema, vec![p0, p1]).unwrap();
    ctx.register_table("t", Arc::new(table)).unwrap();
    ctx
}

/// Three-stage chain (`GROUP BY` hash boundary + `ORDER BY` gather boundary)
/// run under `channel_capacity = 1`, inside a timeout. Asserts multiset
/// equivalence with single-node `collect`. A hang (deadlock) fails via timeout;
/// a wrong result fails via the equivalence check.
#[tokio::test]
async fn tight_backpressure_multi_stage_pipeline_matches_collect() {
    let result = tokio::time::timeout(Duration::from_secs(30), async {
        let ctx = context_with_many_rows().await;
        let df = ctx
            .sql("SELECT a, count(*) AS c FROM t GROUP BY a ORDER BY a")
            .await
            .unwrap();
        let plan = df.create_physical_plan().await.unwrap();

        let expected = collect(plan.clone(), ctx.task_ctx()).await.unwrap();

        // capacity = 1: the tightest bound that still makes progress, exercised
        // through the full run_distributed executor (not just sink/source).
        let state = ctx.state();
        let config = SchedulerConfig {
            session_builder: Arc::new(move || {
                SessionContext::new_with_state(state.clone())
            }),
            channel_capacity: 1,
        };
        let actual = run_distributed(&ctx, plan, config).await.unwrap();

        (expected, actual)
    })
    .await;

    let (expected, actual) =
        result.expect("multi-stage pipeline deadlocked / timed out under capacity=1");

    // Order-insensitive multiset equivalence via sorted pretty-printed rows.
    let e = datafusion::arrow::util::pretty::pretty_format_batches(&expected)
        .unwrap()
        .to_string();
    let a = datafusion::arrow::util::pretty::pretty_format_batches(&actual)
        .unwrap()
        .to_string();
    let mut el: Vec<&str> = e.lines().collect();
    el.sort();
    let mut al: Vec<&str> = a.lines().collect();
    al.sort();
    assert_eq!(
        el, al,
        "streaming distributed output != collect output under capacity=1"
    );
}
