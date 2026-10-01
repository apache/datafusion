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

use std::sync::Arc;

use arrow::array::{ArrayRef, Int32Array, RecordBatch};
use datafusion::{
    assert_batches_sorted_eq,
    prelude::{SessionConfig, SessionContext},
};
use datafusion_catalog::MemTable;
use datafusion_common::tree_node::{Transformed, TreeNode};
use datafusion_execution::runtime_env::RuntimeEnvBuilder;
use datafusion_physical_plan::{ExecutionPlanProperties, repartition::RepartitionExec};
use futures::TryStreamExt;
use itertools::Itertools;

/// End to end test for spilling in RepartitionExec.
/// The idea is to make a real world query with a relatively low memory limit and
/// then drive one partition at a time, simulating dissimilar execution speed in partitions.
/// Just as some examples of real world scenarios where this can happen consider
/// lopsided groups in a group by especially if one partitions spills and others don't,
/// or in distributed systems if one upstream node is slower than others.
#[tokio::test]
async fn test_repartition_memory_limit() {
    let runtime = RuntimeEnvBuilder::new()
        .with_memory_limit(1024 * 1024, 1.0)
        .build()
        .unwrap();
    let config = SessionConfig::new()
        .with_batch_size(32)
        .with_target_partitions(2);
    let ctx = SessionContext::new_with_config_rt(config, Arc::new(runtime));
    let batches = vec![
        RecordBatch::try_from_iter(vec![(
            "c1",
            Arc::new(Int32Array::from_iter_values((0..10).cycle().take(100_000)))
                as ArrayRef,
        )])
        .unwrap(),
    ];
    let table = Arc::new(MemTable::try_new(batches[0].schema(), vec![batches]).unwrap());
    ctx.register_table("t", table).unwrap();
    let plan = ctx
        .state()
        .create_logical_plan("SELECT c1, count(*) as c FROM t GROUP BY c1;")
        .await
        .unwrap();
    let plan = ctx.state().create_physical_plan(&plan).await.unwrap();
    assert_eq!(plan.output_partitioning().partition_count(), 2);
    // Execute partition 0, this should cause items going into the rest of the partitions to queue up and because
    // of the low memory limit should spill to disk.
    let batches0 = Arc::clone(&plan)
        .execute(0, ctx.task_ctx())
        .unwrap()
        .try_collect::<Vec<_>>()
        .await
        .unwrap();

    let mut metrics = None;
    Arc::clone(&plan)
        .transform_down(|node| {
            if node.is::<RepartitionExec>() {
                metrics = node.metrics();
            }
            Ok(Transformed::no(node))
        })
        .unwrap();

    let metrics = metrics.unwrap();
    assert!(metrics.spilled_bytes().unwrap() > 0);
    assert!(metrics.spilled_rows().unwrap() > 0);
    assert!(metrics.spill_count().unwrap() > 0);

    // Execute the other partition
    let batches1 = Arc::clone(&plan)
        .execute(1, ctx.task_ctx())
        .unwrap()
        .try_collect::<Vec<_>>()
        .await
        .unwrap();

    let all_batches = batches0
        .into_iter()
        .chain(batches1.into_iter())
        .collect_vec();
    #[rustfmt::skip]
    let expected = &[
    "+----+-------+",
    "| c1 | c     |",
    "+----+-------+",
    "| 0  | 10000 |",
    "| 1  | 10000 |",
    "| 2  | 10000 |",
    "| 3  | 10000 |",
    "| 4  | 10000 |",
    "| 5  | 10000 |",
    "| 6  | 10000 |",
    "| 7  | 10000 |",
    "| 8  | 10000 |",
    "| 9  | 10000 |",
    "+----+-------+",
    ];
    assert_batches_sorted_eq!(expected, &all_batches);
}

/// Regression test for <https://github.com/apache/datafusion/issues/24883>.
///
/// In non-preserve-order mode every input task of a `RepartitionExec` shares one
/// spill pool per output partition. When two input tasks spilled concurrently the
/// pool could end up with two open spill files while the reader only ever drained
/// the head file: it parked on a drained-but-unfinished head file even though the
/// batch it was waiting for had been written to the second file. Once every
/// distributor channel was non-empty the gate closed, both input tasks parked in
/// `send`, no sink was dropped, and nothing could wake anybody.
///
/// This module is not compiled with `force_hash_collisions`. That feature makes
/// every hash 0, so the hash `RepartitionExec` sends all rows to one output
/// partition. The gate closes only when every channel holds data, so the
/// deadlock cannot occur and the test cannot test anything. The feature also
/// makes the group-by hash table degenerate, which made one attempt of this
/// query approximately 36 times slower and thus longer than the limit below.
#[cfg(not(feature = "force_hash_collisions"))]
mod spill_pool_deadlock {
    use super::*;

    use std::time::Duration;

    use datafusion::execution::session_state::SessionStateBuilder;
    use datafusion_execution::disk_manager::{DiskManagerBuilder, DiskManagerMode};
    use datafusion_execution::memory_pool::{GreedyMemoryPool, MemoryPool};
    use datafusion_physical_plan::collect;

    /// Memory limit that puts `RepartitionExec` into its spilling path for this
    /// query without failing the aggregation outright.
    const MEMORY_LIMIT: usize = 4 * 1024 * 1024;

    /// Number of attempts. On an unfixed tree the first attempt deadlocks; the
    /// budget is only there so a fix cannot pass by luck.
    const ATTEMPTS: usize = 12;

    /// Generous per-attempt budget: a healthy run of this query takes well under
    /// a second.
    const PER_ATTEMPT_TIMEOUT: Duration = Duration::from_secs(20);

    /// Runs the query once. Returns the number of output rows and the number
    /// of spills the `RepartitionExec`s did.
    async fn run_once() -> datafusion::error::Result<(usize, usize)> {
        let pool: Arc<dyn MemoryPool> = Arc::new(GreedyMemoryPool::new(MEMORY_LIMIT));
        let runtime = RuntimeEnvBuilder::new()
            // Spilling must be possible: with the disk manager disabled the query
            // fails with a resource error instead of deadlocking.
            .with_disk_manager_builder(
                DiskManagerBuilder::default().with_mode(DiskManagerMode::OsTmpDirectory),
            )
            .with_memory_pool(pool)
            .build_arc()?;

        let config = SessionConfig::new()
            // Two input partitions feeding one hash RepartitionExec is the
            // smallest configuration with two concurrent writers per spill pool.
            .with_target_partitions(2)
            // Small batches so many small batches reach the repartition spill
            // path while the spill file stays far below
            // `max_spill_file_size_bytes` and so never rotates.
            .with_batch_size(64);

        let state = SessionStateBuilder::new()
            .with_config(config)
            .with_runtime_env(runtime)
            .with_default_features()
            .build();
        let ctx = SessionContext::new_with_state(state);

        ctx.sql(
            "create table trace_events as
             select v % 13 as g,
                    case when v % 29 = 0 then null
                         else md5(cast(v % 337 as varchar)) end as trace_id
             from generate_series(1, 40000) as t(v)",
        )
        .await?
        .collect()
        .await?;

        ctx.sql(
            "create view tv as
             select g, arrow_cast(trace_id, 'Utf8View') as trace_id from trace_events",
        )
        .await?
        .collect()
        .await?;

        let logical = ctx
            .state()
            .create_logical_plan(
                "select g, count(distinct trace_id) as n from tv group by g",
            )
            .await?;
        let plan = ctx.state().create_physical_plan(&logical).await?;
        let batches = collect(Arc::clone(&plan), ctx.task_ctx()).await?;

        // Count the spills of the `RepartitionExec`s. The caller uses this to
        // make sure the query still goes through the spill path: if a change
        // stops it spilling, this test keeps passing but no longer covers the
        // deadlock, and that must fail loudly instead.
        let mut spills = 0;
        plan.transform_down(|node| {
            if node.is::<RepartitionExec>() {
                spills += node.metrics().and_then(|m| m.spill_count()).unwrap_or(0);
            }
            Ok(Transformed::no(node))
        })?;

        Ok((batches.iter().map(|b| b.num_rows()).sum(), spills))
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 8)]
    async fn repartition_spill_pool_does_not_deadlock() {
        let mut attempts_that_spilled = 0;
        for attempt in 0..ATTEMPTS {
            match tokio::time::timeout(PER_ATTEMPT_TIMEOUT, run_once()).await {
                Err(elapsed) => panic!(
                    "attempt {attempt}: grouped COUNT(DISTINCT Utf8View) never completed \
                     within {PER_ATTEMPT_TIMEOUT:?} ({elapsed}); RepartitionExec spill \
                     pool deadlock"
                ),
                Ok(Ok((rows, spills))) => {
                    assert_eq!(rows, 13, "attempt {attempt}: wrong row count");
                    if spills > 0 {
                        attempts_that_spilled += 1;
                    }
                }
                // The budget is deliberately far too small for the query, and the
                // greedy pool hands memory out first come first served, so once in
                // a few thousand attempts an aggregate's allocation is refused
                // while the repartition reservations hold the pool. That is a
                // legitimate outcome of the limit, not the hang this test guards
                // against.
                Ok(Err(e))
                    if matches!(
                        e.find_root(),
                        datafusion::error::DataFusionError::ResourcesExhausted(_)
                    ) => {}
                Ok(Err(e)) => panic!("attempt {attempt}: query failed: {e}"),
            }
        }

        // The memory limit must be tight enough that `RepartitionExec` spills,
        // because the deadlock is in its spill pool. Timing decides how many
        // attempts spill, but if none of them do, the limit no longer forces
        // the spill path and this test has stopped testing anything.
        assert!(
            attempts_that_spilled > 0,
            "no attempt reached the RepartitionExec spill path in {ATTEMPTS} \
             attempts; the memory limit no longer forces a spill"
        );
    }
}
