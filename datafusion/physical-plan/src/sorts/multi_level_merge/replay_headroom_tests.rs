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

use super::*;

use super::tests::{
    build_merge_builder, build_spill_manager, make_sorted_spill_file, test_schema,
};
use arrow::array::{AsArray, Int64Array, StringArray};
use arrow::compute::concat_batches;
use arrow::datatypes::{DataType, Field, Int64Type, Schema};
use datafusion_execution::memory_pool::{GreedyMemoryPool, MemoryConsumer, MemoryPool};
use datafusion_execution::runtime_env::{RuntimeEnv, RuntimeEnvBuilder};
use datafusion_physical_expr_common::metrics::{ExecutionPlanMetricsSet, SpillMetrics};
use std::sync::atomic::{AtomicBool, Ordering};

struct ReplayMergeFixture {
    builder: MultiLevelMergeBuilder,
    env: Arc<RuntimeEnv>,
    pool: Arc<dyn MemoryPool>,
    pool_size: usize,
    metrics: SpillMetrics,
    input_bytes: usize,
}

fn replay_merge_builder(
    spill_manager: SpillManager,
    schema: SchemaRef,
    spills: Vec<SortedSpillFile>,
    pool: &Arc<dyn MemoryPool>,
    batch_size: usize,
) -> MultiLevelMergeBuilder {
    build_merge_builder(spill_manager, schema, spills, pool, batch_size)
        .with_replay_headroom(true)
}

/// Create equally sized, interleaved input runs.
fn replay_merge_fixture(
    run_count: usize,
    rows_per_run: usize,
    memory_batches: usize,
    max_fan_in: usize,
) -> Result<ReplayMergeFixture> {
    let env = RuntimeEnvBuilder::new()
        .with_max_spill_merge_fan_in(max_fan_in)
        .build_arc()?;
    let schema = test_schema();
    let spill_manager = build_spill_manager(&env, &schema);
    let metrics = spill_manager.metrics.clone();
    let spills = (0..run_count)
        .map(|run| {
            let values = (0..rows_per_run)
                .map(|row| (row * run_count + run) as i64)
                .collect();
            make_sorted_spill_file(&spill_manager, &schema, values)
        })
        .collect::<Vec<_>>();
    let input_bytes = metrics.spilled_bytes.value();
    let pool_size = memory_batches * spills[0].max_record_batch_memory;
    let pool: Arc<dyn MemoryPool> = Arc::new(GreedyMemoryPool::new(pool_size));
    let builder =
        replay_merge_builder(spill_manager, schema, spills, &pool, rows_per_run);
    Ok(ReplayMergeFixture {
        builder,
        env,
        pool,
        pool_size,
        metrics,
        input_bytes,
    })
}

/// Return additional spill metrics, checking final replay space and cleanup.
async fn merge_replay_runs(
    run_count: usize,
    rows_per_run: usize,
    memory_batches: usize,
) -> Result<(usize, usize, usize)> {
    let ReplayMergeFixture {
        builder,
        env,
        pool,
        pool_size,
        metrics,
        input_bytes,
    } = replay_merge_fixture(run_count, rows_per_run, memory_batches, 0)?;
    let schema = Arc::clone(&builder.schema);
    let replay = MemoryConsumer::new("replay consumer").register(&pool);
    let mut stream = builder.create_spillable_merge_stream();
    let mut batches = Vec::new();
    while let Some(batch) = stream.try_next().await? {
        assert!(pool.reserved() <= pool_size / 2);
        assert!(crate::spill::get_record_batch_memory_size(&batch) <= pool.reserved());
        // Check that another consumer can actually claim the replay allowance.
        replay.try_grow(pool_size / 2)?;
        replay.free();
        batches.push(batch);
    }
    let merged = concat_batches(&schema, &batches)?;
    let expected = Int64Array::from_iter_values(0..(run_count * rows_per_run) as i64);
    assert_eq!(merged.column(0).as_primitive::<Int64Type>(), &expected);
    assert_eq!(pool.reserved(), 0);
    drop(stream);
    assert_eq!(env.disk_manager.spilling_progress().active_files_count, 0);
    assert_eq!(env.disk_manager.used_disk_space(), 0);

    Ok((
        metrics.spill_file_count.value() - run_count,
        metrics.spilled_rows.value() - run_count * rows_per_run,
        metrics.spilled_bytes.value() - input_bytes,
    ))
}

#[rstest::rstest]
#[case::intermediate_reuses_admitted_buffers(6, 16, 0, 2, 8)]
#[case::intermediate_holds_back_a_run(3, 16, 0, 1, 8)]
#[case::final_disables_read_ahead(3, 12, 0, 0, 6)]
#[case::fan_in_limited_intermediate(4, 8, 2, 2, 4)]
#[case::fan_in_limited_final(2, 8, 2, 0, 4)]
#[tokio::test]
async fn replay_headroom_depends_on_merge_phase(
    #[case] run_count: usize,
    #[case] memory_batches: usize,
    #[case] max_fan_in: usize,
    #[case] remaining_runs: usize,
    #[case] reserved_batches: usize,
) -> Result<()> {
    let ReplayMergeFixture {
        mut builder,
        env,
        pool,
        ..
    } = replay_merge_fixture(run_count, 128, memory_batches, max_fan_in)?;
    let batch_memory = builder.sorted_spill_files[0].0.max_record_batch_memory;
    let MergeStep::Stream { stream, .. } =
        builder.merge_sorted_runs_within_mem_limit(false)?
    else {
        panic!("the merge should fit without splitting a run");
    };
    assert_eq!(builder.sorted_spill_files.len(), remaining_runs);
    assert_eq!(pool.reserved(), reserved_batches * batch_memory);
    let batches: Vec<RecordBatch> = stream.try_collect().await?;
    assert_eq!(
        batches.iter().map(RecordBatch::num_rows).sum::<usize>(),
        (run_count - remaining_runs) * 128
    );
    drop(builder);
    assert_eq!(pool.reserved(), 0);
    assert_eq!(env.disk_manager.spilling_progress().active_files_count, 0);
    assert_eq!(env.disk_manager.used_disk_space(), 0);
    Ok(())
}

#[rstest::rstest]
#[case::original_read_ahead(false, 4)]
#[case::widened_intermediate(true, 8)]
#[tokio::test]
async fn intermediate_merge_preserves_competing_replay_budget(
    #[case] widen: bool,
    #[case] selected_runs: usize,
) -> Result<()> {
    let ReplayMergeFixture {
        mut builder,
        env,
        pool,
        ..
    } = replay_merge_fixture(10, 128, 40, 0)?;
    let batch_memory = builder.sorted_spill_files[0].0.max_record_batch_memory;
    let peer = MemoryConsumer::new("another partition replay").register(&pool);
    peer.try_grow(8 * batch_memory)?;
    builder.widen_intermediate_merges = widen;

    let MergeStep::Stream { stream, retry, .. } =
        builder.merge_sorted_runs_within_mem_limit(false)?
    else {
        panic!("the merge should fit without splitting a run");
    };
    assert_eq!(builder.sorted_spill_files.len(), 10 - selected_runs);
    assert_eq!(retry.is_some(), widen);
    if let Some(retry) = &retry {
        assert_eq!(retry.spills.len(), selected_runs);
        assert_eq!(retry.original_count, 4);
        assert_eq!(retry.buffer_size, 2);
        assert_eq!(retry.reservation.size(), 16 * batch_memory);
    }
    // Both selections retain the same sixteen batches of memory. Widening
    // used to grow this grant to thirty-two and consume all of the peer's
    // remaining replay budget before its next allocation.
    assert_eq!(pool.reserved() - peer.size(), 16 * batch_memory);
    peer.try_grow(4 * batch_memory)?;
    assert_eq!(pool.reserved(), 28 * batch_memory);
    let batches: Vec<RecordBatch> = stream.try_collect().await?;
    assert_eq!(
        batches.iter().map(RecordBatch::num_rows).sum::<usize>(),
        selected_runs * 128
    );
    drop(retry);
    drop(builder);
    peer.free();
    assert_eq!(pool.reserved(), 0);
    assert_eq!(env.disk_manager.used_disk_space(), 0);
    Ok(())
}

#[tokio::test]
async fn replay_headroom_keeps_split_retries_before_intermediate_merges() -> Result<()> {
    let ReplayMergeFixture {
        mut builder, pool, ..
    } = replay_merge_fixture(3, 128, 6, 0)?;
    assert!(matches!(
        builder.merge_sorted_runs_within_mem_limit(false)?,
        MergeStep::SplitThenRetry(_)
    ));
    assert_eq!(builder.sorted_spill_files.len(), 3);
    assert_eq!(pool.reserved(), 0);
    Ok(())
}

#[rstest::rstest]
#[case::indivisible_split_retry(3, 32, 6, true)]
#[case::widened_short_batches(6, 3, 16, false)]
#[tokio::test]
async fn replay_headroom_preserves_short_run_batch_limits(
    #[case] run_count: usize,
    #[case] batches_per_run: usize,
    #[case] memory_batches: usize,
    #[case] expect_split: bool,
) -> Result<()> {
    let env = Arc::new(RuntimeEnv::default());
    let schema = Arc::new(Schema::new(vec![Field::new("x", DataType::Utf8, false)]));
    let manager = build_spill_manager(&env, &schema);
    let values = (0..run_count * batches_per_run)
        .map(|value| format!("{value:04}{}", "x".repeat(1024)))
        .collect::<Vec<_>>();
    let spills = (0..run_count)
        .map(|run| {
            let batches = values.iter().skip(run).step_by(run_count).map(|value| {
                RecordBatch::try_new(
                    Arc::clone(&schema),
                    vec![Arc::new(StringArray::from(vec![value.as_str()]))],
                )
                .map_err(Into::into)
            });
            let (file, max_record_batch_memory) = manager
                .spill_record_batch_iter_and_return_max_batch_memory(
                    batches,
                    "short replay input",
                )?
                .expect("a nonempty input must spill");
            Ok(SortedSpillFile {
                file,
                max_record_batch_memory,
            })
        })
        .collect::<Result<Vec<_>>>()?;
    let pool_size = memory_batches * spills[0].max_record_batch_memory;
    let pool: Arc<dyn MemoryPool> = Arc::new(GreedyMemoryPool::new(pool_size));
    let builder = replay_merge_builder(manager, Arc::clone(&schema), spills, &pool, 8192);
    let mut stream = builder.create_spillable_merge_stream();
    let mut batches = Vec::new();
    while let Some(batch) = stream.try_next().await? {
        if expect_split {
            // Unlike the parent module's direct minimum-admission test, this
            // carries many wide singleton batches through intermediate passes.
            // Their discovered one-row limit must survive the nominal 8192 size.
            assert_eq!(batch.num_rows(), 1);
        }
        assert!(pool.reserved() <= pool_size);
        batches.push(batch);
    }
    let merged = concat_batches(&schema, &batches)?;
    assert_eq!(
        merged.column(0).as_string::<i32>(),
        &StringArray::from(values)
    );
    drop(stream);
    assert_eq!(pool.reserved(), 0);
    assert_eq!(env.disk_manager.spilling_progress().active_files_count, 0);
    assert_eq!(env.disk_manager.used_disk_space(), 0);
    Ok(())
}

#[tokio::test]
async fn replay_headroom_does_not_rewrite_intermediate_runs_twice() -> Result<()> {
    // The admitted grant holds four inputs with read-ahead, or eight without it.
    // Four intermediate merges of eight runs leave four runs for the final merge.
    // Keeping read-ahead for every pass instead writes ten intermediate files,
    // rewriting every input row twice before returning the final merge.
    let (spill_count, spilled_rows, spilled_bytes) =
        merge_replay_runs(32, 256, 32).await?;
    assert_eq!(
        spill_count, 4,
        "intermediate spill: {spilled_rows} rows, {spilled_bytes} bytes"
    );
    assert_eq!(spilled_rows, 32 * 256);
    Ok(())
}

#[tokio::test]
async fn replay_headroom_is_restored_after_intermediate_split_retries() -> Result<()> {
    // Two inputs need four batches of workspace. Three available batches force
    // an intermediate split; the final merge then needs further splitting to
    // leave replay headroom. Both phases must preserve every row and release
    // all reservations and temporary files when the stream finishes.
    let (spill_count, _, _) = merge_replay_runs(3, 128, 3).await?;
    assert!(spill_count > 1, "the merge must spill and split runs");
    Ok(())
}

#[rstest::rstest]
#[case::sufficient_quota(true)]
#[case::insufficient_quota(false)]
#[tokio::test]
async fn intermediate_merge_preserves_disk_quota(
    #[case] sufficient_quota: bool,
) -> Result<()> {
    check_intermediate_merge_disk_quota(sufficient_quota).await
}

#[rstest::rstest]
#[case::sufficient_quota(true)]
#[case::insufficient_quota(false)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn intermediate_merge_preserves_disk_quota_with_read_ahead_tasks(
    #[case] sufficient_quota: bool,
) -> Result<()> {
    check_intermediate_merge_disk_quota(sufficient_quota).await
}

async fn check_intermediate_merge_disk_quota(sufficient_quota: bool) -> Result<()> {
    const RUN_COUNT: usize = 4;
    const BATCHES_PER_RUN: usize = 100;
    const BATCH_SIZE: usize = 128;
    let env = Arc::new(RuntimeEnv::default());
    let schema = Arc::new(Schema::new(vec![Field::new("x", DataType::Int64, false)]));
    let manager = SpillManager::new(
        Arc::clone(&env),
        SpillMetrics::new(&ExecutionPlanMetricsSet::new(), 0),
        Arc::clone(&schema),
    );
    let mut spills = Vec::new();
    for run in 0..RUN_COUNT {
        let batches = (0..BATCHES_PER_RUN).map(|batch_index| {
            let values =
                Int64Array::from_iter_values((0..BATCH_SIZE).map(|row| {
                    ((batch_index * BATCH_SIZE + row) * RUN_COUNT + run) as i64
                }));
            RecordBatch::try_new(Arc::clone(&schema), vec![Arc::new(values)])
                .map_err(Into::into)
        });
        let (file, max_record_batch_memory) = manager
            .spill_record_batch_iter_and_return_max_batch_memory(
                batches,
                "disk quota replay input",
            )?
            .expect("a nonempty input must spill");
        spills.push(SortedSpillFile {
            file,
            max_record_batch_memory,
        });
    }
    // Interleaving keeps all inputs live while the output grows. This quota
    // permits two-input intermediate merges but not a three-input merge. The
    // smaller quota cannot accommodate either and must fail without leaking.
    let initial_disk = env.disk_manager.used_disk_space();
    let quota_numerator = if sufficient_quota { 13 } else { 9 };
    env.disk_manager
        .set_max_temp_directory_size(initial_disk * quota_numerator / 8)?;
    let pool: Arc<dyn MemoryPool> = Arc::new(GreedyMemoryPool::new(
        16 * spills[0].max_record_batch_memory,
    ));
    let builder =
        replay_merge_builder(manager, Arc::clone(&schema), spills, &pool, BATCH_SIZE);
    let result: Result<Vec<RecordBatch>> =
        builder.create_spillable_merge_stream().try_collect().await;
    if sufficient_quota {
        let batches = result?;
        let merged = concat_batches(&schema, &batches)?;
        let expected = Int64Array::from_iter_values(
            0..(RUN_COUNT * BATCHES_PER_RUN * BATCH_SIZE) as i64,
        );
        assert_eq!(merged.column(0).as_primitive::<Int64Type>(), &expected);
    } else {
        let error = result.expect_err("the quota must reject even a two-input merge");
        let message = error.to_string();
        assert!(
            message.contains("Retrying the narrower intermediate merge after:"),
            "the error must identify the failed wider attempt: {message}"
        );
        assert_eq!(
            message.matches("max_temp_directory_size").count(),
            2,
            "both the original and narrower write errors must survive: {message}"
        );
    }
    // On a multithreaded runtime, dropping a failed merge cancels background
    // read-ahead tasks. Allow their cancellation to release input file handles.
    tokio::time::timeout(std::time::Duration::from_secs(5), async {
        while pool.reserved() != 0
            || env.disk_manager.used_disk_space() != 0
            || env.disk_manager.spilling_progress().active_files_count != 0
        {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("merge completion must release reservations and temporary files");
    assert_eq!(pool.reserved(), 0);
    assert_eq!(env.disk_manager.used_disk_space(), 0);
    Ok(())
}

#[rstest::rstest]
#[case::with_read_ahead(4, 16, 0, 15)]
#[case::without_read_ahead_under_pressure(5, 12, 6, 5)]
#[test]
fn intermediate_merge_keeps_admitted_buffers(
    #[case] run_count: usize,
    #[case] memory_batches: usize,
    #[case] contender_headroom_batches: usize,
    #[case] contender_handoff_batches: usize,
) -> Result<()> {
    // Model another operator acquiring pool capacity immediately after the
    // merge gives up its already-admitted buffers. All allocations use the
    // underlying pool's normal fallible admission.
    // With read-ahead disabled, the contender first takes the released replay
    // headroom. Retrying admission with read-ahead enabled would then fail to
    // seat two inputs and relinquish the usable minimum merge's grant.
    #[derive(Debug)]
    struct HandoffPool {
        inner: Arc<dyn MemoryPool>,
        contender: MemoryReservation,
        contender_headroom_bytes: usize,
        contender_handoff_bytes: usize,
        armed: AtomicBool,
        handed_off: AtomicBool,
        saw_headroom_release: AtomicBool,
    }

    impl std::fmt::Display for HandoffPool {
        fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
            write!(f, "HandoffPool")
        }
    }

    impl MemoryPool for HandoffPool {
        fn name(&self) -> &str {
            "HandoffPool"
        }

        fn grow(&self, reservation: &MemoryReservation, additional: usize) {
            self.inner.grow(reservation, additional);
        }

        fn try_grow(
            &self,
            reservation: &MemoryReservation,
            additional: usize,
        ) -> Result<()> {
            self.inner.try_grow(reservation, additional)
        }

        fn shrink(&self, reservation: &MemoryReservation, subtractive: usize) {
            self.inner.shrink(reservation, subtractive);
            if self.armed.load(Ordering::SeqCst) && subtractive != 0 {
                if reservation.size() != 0 {
                    if !self.saw_headroom_release.swap(true, Ordering::SeqCst) {
                        self.contender
                            .try_grow(self.contender_headroom_bytes)
                            .unwrap();
                    }
                } else if self.saw_headroom_release.load(Ordering::SeqCst)
                    && self.armed.swap(false, Ordering::SeqCst)
                {
                    // Releasing replay headroom leaves the admitted buffers
                    // intact. Only a later full free lets this allocation fit.
                    self.contender
                        .try_grow(self.contender_handoff_bytes)
                        .unwrap();
                    self.handed_off.store(true, Ordering::SeqCst);
                }
            }
        }

        fn reserved(&self) -> usize {
            self.inner.reserved()
        }
    }

    let ReplayMergeFixture {
        mut builder,
        env,
        pool: inner,
        ..
    } = replay_merge_fixture(run_count, 128, memory_batches, 0)?;
    let batch_memory = builder.sorted_spill_files[0].0.max_record_batch_memory;
    let contender = MemoryConsumer::new("competing operator").register(&inner);
    let hooked = Arc::new(HandoffPool {
        inner,
        contender,
        contender_headroom_bytes: contender_headroom_batches * batch_memory,
        contender_handoff_bytes: contender_handoff_batches * batch_memory,
        armed: AtomicBool::new(true),
        handed_off: AtomicBool::new(false),
        saw_headroom_release: AtomicBool::new(false),
    });
    let pool: Arc<dyn MemoryPool> = Arc::clone(&hooked) as Arc<dyn MemoryPool>;
    builder.reservation = MemoryConsumer::new("replay headroom test").register(&pool);
    let admission = builder.merge_sorted_runs_within_mem_limit(false);
    // Disable the scheduling hook before normal stream and builder cleanup.
    hooked.armed.store(false, Ordering::SeqCst);
    let MergeStep::Stream { stream, .. } = admission? else {
        panic!("a previously admitted minimum merge must remain usable");
    };
    assert!(hooked.saw_headroom_release.load(Ordering::SeqCst));
    assert!(!hooked.handed_off.load(Ordering::SeqCst));
    assert_eq!(
        hooked.contender.size(),
        contender_headroom_batches * batch_memory
    );
    drop(stream);
    drop(builder);
    hooked.contender.free();
    assert_eq!(pool.reserved(), 0);
    assert_eq!(env.disk_manager.used_disk_space(), 0);
    Ok(())
}
