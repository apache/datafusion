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

use crate::expressions::PhysicalSortExpr;
use arrow::array::{AsArray, Int64Array, StringArray};
use arrow::compute::concat_batches;
use arrow::datatypes::{DataType, Field, Int64Type, Schema};
use datafusion_execution::memory_pool::{GreedyMemoryPool, MemoryConsumer, MemoryPool};
use datafusion_execution::runtime_env::{RuntimeEnv, RuntimeEnvBuilder};
use datafusion_physical_expr::expressions::Column;
use datafusion_physical_expr_common::metrics::{ExecutionPlanMetricsSet, SpillMetrics};

struct ReplayMergeFixture {
    builder: MultiLevelMergeBuilder,
    env: Arc<RuntimeEnv>,
    pool: Arc<dyn MemoryPool>,
    pool_size: usize,
    metrics: SpillMetrics,
    input_bytes: usize,
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
    let schema = Arc::new(Schema::new(vec![Field::new("x", DataType::Int64, false)]));
    let metrics = SpillMetrics::new(&ExecutionPlanMetricsSet::new(), 0);
    let spill_manager =
        SpillManager::new(Arc::clone(&env), metrics.clone(), Arc::clone(&schema));
    let mut spills = Vec::with_capacity(run_count);
    for run in 0..run_count {
        let values = Int64Array::from_iter_values(
            (0..rows_per_run).map(|row| (row * run_count + run) as i64),
        );
        let batch = RecordBatch::try_new(Arc::clone(&schema), vec![Arc::new(values)])?;
        let (file, max_record_batch_memory) = spill_manager
            .spill_record_batch_iter_and_return_max_batch_memory(
                std::iter::once(Ok(batch)),
                "replay headroom test input",
            )?
            .expect("a nonempty input must spill");
        spills.push(SortedSpillFile {
            file,
            max_record_batch_memory,
        });
    }
    let input_bytes = metrics.spilled_bytes.value();
    let pool_size = memory_batches * spills[0].max_record_batch_memory;
    let pool: Arc<dyn MemoryPool> = Arc::new(GreedyMemoryPool::new(pool_size));
    let reservation = MemoryConsumer::new("replay headroom test").register(&pool);
    let expr = [PhysicalSortExpr::new_default(Arc::new(Column::new("x", 0)))].into();
    let builder = MultiLevelMergeBuilder::new(
        spill_manager,
        Arc::clone(&schema),
        spills,
        vec![],
        expr,
        BaselineMetrics::new(&ExecutionPlanMetricsSet::new(), 0),
        rows_per_run,
        reservation,
        None,
        false,
    )
    .with_replay_headroom(true);
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
#[case::intermediate_uses_full_pool(6, 16, 0, 2, 16)]
#[case::intermediate_holds_back_a_run(3, 16, 0, 1, 8)]
#[case::final_disables_read_ahead(3, 12, 0, 0, 6)]
#[case::fan_in_limited_intermediate(4, 8, 2, 2, 8)]
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

#[tokio::test]
async fn replay_headroom_preserves_indivisible_run_batch_limits() -> Result<()> {
    let env = Arc::new(RuntimeEnv::default());
    let schema = Arc::new(Schema::new(vec![Field::new("x", DataType::Utf8, false)]));
    let spill_manager = SpillManager::new(
        Arc::clone(&env),
        SpillMetrics::new(&ExecutionPlanMetricsSet::new(), 0),
        Arc::clone(&schema),
    );
    let values = (0..96)
        .map(|value| format!("{value:03}{}", "x".repeat(1024)))
        .collect::<Vec<_>>();
    let mut spills = Vec::new();
    for run in 0..3 {
        // Every batch is already indivisible, but the configured merge batch
        // size is much larger. The split/retry path must discover the one-row
        // limit before an intermediate pass can concatenate these batches.
        let batches = values.iter().skip(run).step_by(3).map(|value| {
            RecordBatch::try_new(
                Arc::clone(&schema),
                vec![Arc::new(StringArray::from(vec![value.as_str()]))],
            )
            .map_err(Into::into)
        });
        let (file, max_record_batch_memory) = spill_manager
            .spill_record_batch_iter_and_return_max_batch_memory(
                batches,
                "indivisible replay input",
            )?
            .expect("a nonempty input must spill");
        spills.push(SortedSpillFile {
            file,
            max_record_batch_memory,
        });
    }
    let pool_size = 6 * spills[0].max_record_batch_memory;
    let pool: Arc<dyn MemoryPool> = Arc::new(GreedyMemoryPool::new(pool_size));
    let reservation = MemoryConsumer::new("indivisible replay test").register(&pool);
    let builder = MultiLevelMergeBuilder::new(
        spill_manager,
        Arc::clone(&schema),
        spills,
        vec![],
        [PhysicalSortExpr::new_default(Arc::new(Column::new("x", 0)))].into(),
        BaselineMetrics::new(&ExecutionPlanMetricsSet::new(), 0),
        8192,
        reservation,
        None,
        false,
    )
    .with_replay_headroom(true);
    let mut stream = builder.create_spillable_merge_stream();
    let mut batches = Vec::new();
    while let Some(batch) = stream.try_next().await? {
        assert_eq!(batch.num_rows(), 1);
        assert!(crate::spill::get_record_batch_memory_size(&batch) <= pool.reserved());
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
    // The pool holds eight read-ahead inputs, or four plus equal replay headroom.
    // Four intermediate merges of eight runs leave four runs for the final merge.
    // Reserving headroom for every pass instead writes ten intermediate files,
    // rewriting every input row twice before returning the final merge.
    let (spill_count, spilled_rows, spilled_bytes) =
        merge_replay_runs(32, 256, 32).await?;
    println!(
        "Intermediate spill: {spill_count} files, {spilled_rows} rows, {spilled_bytes} bytes"
    );
    assert_eq!(spill_count, 4, "additional spill bytes: {spilled_bytes}");
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
