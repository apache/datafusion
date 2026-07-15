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

//! Fuzz Test for different operators in memory constrained environment

use std::pin::Pin;
use std::sync::Arc;

use crate::fuzz_cases::aggregate_fuzz::assert_spill_count_metric;
use crate::fuzz_cases::once_exec::OnceExec;
use arrow::array::UInt64Array;
use arrow::{array::StringArray, compute::SortOptions, record_batch::RecordBatch};
use arrow_schema::{DataType, Field, Schema};
use datafusion::common::Result;
use datafusion::execution::runtime_env::RuntimeEnvBuilder;
use datafusion::physical_plan::ExecutionPlan;
use datafusion::physical_plan::expressions::PhysicalSortExpr;
use datafusion::physical_plan::sorts::sort::SortExec;
use datafusion::prelude::SessionConfig;
use datafusion_common::units::{KB, MB};
use datafusion_execution::memory_pool::{
    FairSpillPool, MemoryConsumer, MemoryReservation,
};
use datafusion_execution::{SendableRecordBatchStream, TaskContext};
use datafusion_functions_aggregate::array_agg::array_agg_udaf;
use datafusion_physical_expr::aggregate::AggregateExprBuilder;
use datafusion_physical_expr::expressions::{Column, col};
use datafusion_physical_expr_common::metrics::MetricsSet;
use datafusion_physical_expr_common::sort_expr::LexOrdering;
use datafusion_physical_plan::aggregates::{
    AggregateExec, AggregateMode, PhysicalGroupBy,
};
use datafusion_physical_plan::metrics::MetricValue;
use datafusion_physical_plan::stream::RecordBatchStreamAdapter;
use futures::StreamExt;

use arrow::array::Int32Array;
use datafusion::datasource::memory::MemorySourceConfig;
use datafusion::physical_plan::sorts::sort_preserving_merge::SortPreservingMergeExec;
use datafusion_execution::memory_pool::{
    MemoryPool, TrackConsumersPool, UnboundedMemoryPool,
};
use datafusion_physical_plan::metrics::{ExecutionPlanMetricsSet, SpillMetrics};
use datafusion_physical_plan::spill::SpillManager;
use std::num::NonZeroUsize;

#[tokio::test]
async fn test_sort_with_limited_memory() -> Result<()> {
    let record_batch_size = 8192;
    let pool_size = 2 * MB as usize;
    let task_ctx = {
        let memory_pool = Arc::new(FairSpillPool::new(pool_size));
        TaskContext::default()
            .with_session_config(
                SessionConfig::new()
                    .with_batch_size(record_batch_size)
                    .with_sort_spill_reservation_bytes(1),
            )
            .with_runtime(Arc::new(
                RuntimeEnvBuilder::new()
                    .with_memory_pool(memory_pool)
                    .build()?,
            ))
    };

    let record_batch_size = pool_size / 16;

    // Basic test with a lot of groups that cannot all fit in memory and 1 record batch
    // from each spill file is too much memory
    let metrics = run_sort_test_with_limited_memory(RunTestWithLimitedMemoryArgs {
        pool_size,
        task_ctx: Arc::new(task_ctx),
        number_of_record_batches: 100,
        get_size_of_record_batch_to_generate: Box::pin(move |_| record_batch_size),
        memory_behavior: Default::default(),
        assert_all_output_batches_roughly_match_batch_size_conf: true,
    })
    .await?;

    let total_spill_files_size =
        metrics.spill_count().unwrap_or_default() * record_batch_size;
    assert!(
        total_spill_files_size > pool_size,
        "Total spill files size {total_spill_files_size} should be greater than pool size {pool_size}",
    );

    Ok(())
}

#[tokio::test]
async fn test_sort_with_limited_memory_and_different_sizes_of_record_batch() -> Result<()>
{
    let record_batch_size = 8192;
    let pool_size = 2 * MB as usize;
    let task_ctx = {
        let memory_pool = Arc::new(FairSpillPool::new(pool_size));
        TaskContext::default()
            .with_session_config(
                SessionConfig::new()
                    .with_batch_size(record_batch_size)
                    .with_sort_spill_reservation_bytes(1),
            )
            .with_runtime(Arc::new(
                RuntimeEnvBuilder::new()
                    .with_memory_pool(memory_pool)
                    .build()?,
            ))
    };

    run_sort_test_with_limited_memory(RunTestWithLimitedMemoryArgs {
        pool_size,
        task_ctx: Arc::new(task_ctx),
        number_of_record_batches: 100,
        get_size_of_record_batch_to_generate: Box::pin(move |i| {
            if i % 25 == 1 {
                pool_size / 6
            } else {
                16 * KB as usize
            }
        }),
        memory_behavior: Default::default(),
        assert_all_output_batches_roughly_match_batch_size_conf: true,
    })
    .await?;

    Ok(())
}

#[tokio::test]
async fn test_sort_with_limited_memory_and_different_sizes_of_record_batch_and_changing_memory_reservation()
-> Result<()> {
    let record_batch_size = 8192;
    let pool_size = 2 * MB as usize;
    let task_ctx = {
        let memory_pool = Arc::new(FairSpillPool::new(pool_size));
        TaskContext::default()
            .with_session_config(
                SessionConfig::new()
                    .with_batch_size(record_batch_size)
                    .with_sort_spill_reservation_bytes(1),
            )
            .with_runtime(Arc::new(
                RuntimeEnvBuilder::new()
                    .with_memory_pool(memory_pool)
                    .build()?,
            ))
    };

    run_sort_test_with_limited_memory(RunTestWithLimitedMemoryArgs {
        pool_size,
        task_ctx: Arc::new(task_ctx),
        number_of_record_batches: 100,
        get_size_of_record_batch_to_generate: Box::pin(move |i| {
            if i % 25 == 1 {
                pool_size / 6
            } else {
                16 * KB as usize
            }
        }),
        memory_behavior: MemoryBehavior::TakeAllMemoryAndReleaseEveryNthBatch(10),
        assert_all_output_batches_roughly_match_batch_size_conf: true,
    })
    .await?;

    Ok(())
}

#[tokio::test]
async fn test_sort_with_limited_memory_and_different_sizes_of_record_batch_and_take_all_memory()
-> Result<()> {
    let record_batch_size = 8192;
    let pool_size = 2 * MB as usize;
    let task_ctx = {
        let memory_pool = Arc::new(FairSpillPool::new(pool_size));
        TaskContext::default()
            .with_session_config(
                SessionConfig::new()
                    .with_batch_size(record_batch_size)
                    .with_sort_spill_reservation_bytes(1),
            )
            .with_runtime(Arc::new(
                RuntimeEnvBuilder::new()
                    .with_memory_pool(memory_pool)
                    .build()?,
            ))
    };

    run_sort_test_with_limited_memory(RunTestWithLimitedMemoryArgs {
        pool_size,
        task_ctx: Arc::new(task_ctx),
        number_of_record_batches: 100,
        get_size_of_record_batch_to_generate: Box::pin(move |i| {
            if i % 25 == 1 {
                pool_size / 6
            } else {
                16 * KB as usize
            }
        }),
        memory_behavior: MemoryBehavior::TakeAllMemoryAtTheBeginning,
        assert_all_output_batches_roughly_match_batch_size_conf: true,
    })
    .await?;

    Ok(())
}

#[tokio::test]
async fn test_sort_with_limited_memory_and_large_record_batch() -> Result<()> {
    let record_batch_size = 8192;
    let pool_size = 2 * MB as usize;
    let task_ctx = {
        let memory_pool = Arc::new(FairSpillPool::new(pool_size));
        TaskContext::default()
            .with_session_config(
                SessionConfig::new()
                    .with_batch_size(record_batch_size)
                    .with_sort_spill_reservation_bytes(1),
            )
            .with_runtime(Arc::new(
                RuntimeEnvBuilder::new()
                    .with_memory_pool(memory_pool)
                    .build()?,
            ))
    };

    // Test that the merge degree of multi level merge sort cannot be fixed size when there is not enough memory
    run_sort_test_with_limited_memory(RunTestWithLimitedMemoryArgs {
        pool_size,
        task_ctx: Arc::new(task_ctx),
        number_of_record_batches: 100,
        get_size_of_record_batch_to_generate: Box::pin(move |_| pool_size / 6),
        memory_behavior: Default::default(),
        assert_all_output_batches_roughly_match_batch_size_conf: true,
    })
    .await?;

    Ok(())
}

#[tokio::test]
async fn test_sort_with_limited_memory_and_oversized_record_batch() -> Result<()> {
    let record_batch_size = 8192;
    let pool_size = 2 * MB as usize;
    let task_ctx = {
        let memory_pool = Arc::new(FairSpillPool::new(pool_size));
        TaskContext::default()
            .with_session_config(
                SessionConfig::new()
                    .with_batch_size(record_batch_size)
                    .with_sort_spill_reservation_bytes(1),
            )
            .with_runtime(Arc::new(
                RuntimeEnvBuilder::new()
                    .with_memory_pool(memory_pool)
                    .build()?,
            ))
    };

    let number_of_record_batches = 100;

    // Each spilled run's largest batch is so big that two merge streams cannot be
    // reserved at once even at the smallest read-buffer size (`2 * (2 * batch) >
    // pool`), yet a single stream still fits (`2 * batch < pool`). Reducing the
    // buffer size therefore cannot help, the multi-level merge has to re-spill a
    // run with a smaller batch size to make progress instead of failing with
    // `ResourcesExhausted`.
    let metrics = run_sort_test_with_limited_memory(RunTestWithLimitedMemoryArgs {
        pool_size,
        task_ctx: Arc::new(task_ctx),
        number_of_record_batches,
        get_size_of_record_batch_to_generate: Box::pin(move |_| pool_size / 3),
        memory_behavior: Default::default(),

        assert_all_output_batches_roughly_match_batch_size_conf: false,
    })
    .await?;

    let output_batches = get_output_batches_from_metrics(&metrics);

    // minimum 2 batches more
    assert!(
        output_batches >= number_of_record_batches + 2,
        "output_batches {output_batches} should be greater than number_of_record_batches ({number_of_record_batches}) + 2"
    );

    Ok(())
}

#[tokio::test]
async fn test_sort_preserving_merge_peak_memory_with_spilled_input_round_robin()
-> Result<()> {
    run_sort_preserving_merge_peak_memory_with_spilled_input(true).await
}

#[tokio::test]
async fn test_sort_preserving_merge_peak_memory_with_spilled_input_no_round_robin()
-> Result<()> {
    run_sort_preserving_merge_peak_memory_with_spilled_input(false).await
}

/// Intended to measure the maximum number of record batches held in memory by
/// the SortPreservingMergeStream in a convoluted way by measuring the peak
/// memory reservation. Relevant for merging spilled streams, where the produced
/// record batches suffer from the following issue:
/// https://github.com/apache/arrow-rs/issues/6363
///
/// After an IPC roundtrip, all columns in a [`RecordBatch`] share a single
/// parent buffer. It causes the memory reservation to be inflated, but the
/// bigger issue is the increase in the peak allocated memory caused by
/// prev_cursors in SortPreservingMergeExec. The increase is caused by the fact
/// that the FieldCursor inside prev_cursors holds a reference for the entire
/// Buffer allocated for the input record batch, preventing it from being
/// dropped and thus increasing the number of concomitent input record batches
/// living during the merging phase
async fn run_sort_preserving_merge_peak_memory_with_spilled_input(
    round_robin: bool,
) -> Result<()> {
    let num_batches = 10usize;
    let num_rows_per_batch = 100usize;
    // payload is ~100x larger than the sort key (i32 = 4 bytes, string ≈ 400 bytes)
    let large_string = "x".repeat(400);

    let schema = Arc::new(Schema::new(vec![
        Field::new("sort_key", DataType::Int32, false),
        Field::new("payload", DataType::Utf8, false),
    ]));

    // Unbounded env used only for spilling the input; the merge runs under its
    // own pool below.
    let spill_env = Arc::new(RuntimeEnvBuilder::new().build()?);

    let mut partition_batches: Vec<Vec<RecordBatch>> = Vec::new();

    for stream_idx in 0..2usize {
        // Each stream covers a non-overlapping key range so both are individually
        // sorted: stream 0 → [0, 1000), stream 1 → [1000, 2000).
        let batches: Vec<RecordBatch> = (0..num_batches)
            .map(|b| {
                // Interleave streams: stream 0 → even slots [0,200,400,...],
                // stream 1 → odd slots [100,300,500,...] so the merge
                // alternates between them on every batch.
                let base = ((b * 2 + stream_idx) * num_rows_per_batch) as i32;
                let sort_col: Int32Array =
                    (base..base + num_rows_per_batch as i32).collect();
                let payload_col: StringArray =
                    std::iter::repeat_n(large_string.as_str(), num_rows_per_batch)
                        .map(Some)
                        .collect();
                RecordBatch::try_new(
                    Arc::clone(&schema),
                    vec![Arc::new(sort_col), Arc::new(payload_col)],
                )
                .unwrap()
            })
            .collect();

        // Spill to disk then read back: each RecordBatch is now IPC-backed,
        // meaning all columns share a single parent buffer.  As a result,
        // get_buffer_memory_size() on the sort_key column returns the full
        // parent-buffer capacity (≈ batch size of both columns combined) rather
        // than just the key data (num_rows * 4 bytes).
        let metrics = SpillMetrics::new(&ExecutionPlanMetricsSet::new(), 0);
        let manager =
            SpillManager::new(Arc::clone(&spill_env), metrics, Arc::clone(&schema));
        let spill_file = manager
            .spill_record_batch_and_finish(&batches, "stream")?
            .expect("non-empty input should produce a spill file");

        let mut stream = manager.read_spill_as_stream(spill_file, None)?;
        let mut ipc_batches: Vec<RecordBatch> = Vec::new();
        while let Some(batch) = stream.next().await {
            ipc_batches.push(batch?);
        }
        if stream_idx == 0 {
            use datafusion_physical_plan::spill::get_record_batch_memory_size;
        }
        partition_batches.push(ipc_batches);
    }

    use datafusion_physical_plan::spill::get_record_batch_memory_size;
    let ipc_batch_size = get_record_batch_memory_size(&partition_batches[0][0]);

    // Build a 2-partition plan from the IPC-recovered batches.
    let input =
        MemorySourceConfig::try_new_exec(&partition_batches, Arc::clone(&schema), None)?;

    let sort_expr = PhysicalSortExpr {
        expr: col("sort_key", &schema)?,
        options: SortOptions {
            descending: false,
            nulls_first: true,
        },
    };
    let merge = Arc::new(
        SortPreservingMergeExec::new(LexOrdering::new(vec![sort_expr]).unwrap(), input)
            .with_round_robin_repartition(round_robin),
    );

    // TrackConsumersPool wraps an unbounded pool so the merge never OOMs;
    // we keep the typed Arc to call .metrics() after the run.
    let tracking_pool = Arc::new(TrackConsumersPool::new(
        UnboundedMemoryPool::default(),
        NonZeroUsize::new(10).unwrap(),
    ));
    let runtime = RuntimeEnvBuilder::new()
        .with_memory_pool(Arc::clone(&tracking_pool) as Arc<dyn MemoryPool>)
        .build()?;
    let task_ctx = Arc::new(
        TaskContext::default()
            .with_session_config(SessionConfig::new().with_batch_size(num_rows_per_batch))
            .with_runtime(Arc::new(runtime)),
    );

    let mut output = merge.execute(0, task_ctx)?;
    let mut total_rows = 0usize;
    while let Some(batch) = output.next().await {
        total_rows += batch?.num_rows();
    }
    assert_eq!(total_rows, 2 * num_batches * num_rows_per_batch);

    let mut metrics = tracking_pool.metrics();
    metrics.sort_by_key(|m| std::cmp::Reverse(m.peak));
    let peak_bytes: usize = metrics.iter().map(|m| m.peak).sum();

    // with round robin enabled, 2 extra record batches live in memory
    // see https://github.com/apache/datafusion/issues/23604
    // 5 comes from:
    // BatchBuilder needs to hold 3 Record batches simultaneously to merge two
    // streams (because a stream can cross a record batch boundary)
    // the `cursors` also need 1 record batch worth of memory each (because of
    // the IPC issue)
    let max_batches = if round_robin { 7 } else { 5 };
    let max_peak = max_batches * ipc_batch_size;

    assert!(
        peak_bytes <= max_peak,
        "peak reservation {peak_bytes} bytes exceeds {max_batches}x IPC batch size \
         ({max_peak} bytes); round_robin={round_robin}",
    );

    Ok(())
}

struct RunTestWithLimitedMemoryArgs {
    pool_size: usize,
    task_ctx: Arc<TaskContext>,
    number_of_record_batches: usize,
    get_size_of_record_batch_to_generate:
        Pin<Box<dyn Fn(usize) -> usize + Send + 'static>>,
    memory_behavior: MemoryBehavior,

    /// When true we would `assert_eq(the number of output_rows metric / output_batches metric == task_ctx.batch_size)`
    assert_all_output_batches_roughly_match_batch_size_conf: bool,
}

#[derive(Default)]
enum MemoryBehavior {
    #[default]
    AsIs,
    TakeAllMemoryAtTheBeginning,
    TakeAllMemoryAndReleaseEveryNthBatch(usize),
}

async fn run_sort_test_with_limited_memory(
    mut args: RunTestWithLimitedMemoryArgs,
) -> Result<MetricsSet> {
    let get_size_of_record_batch_to_generate = std::mem::replace(
        &mut args.get_size_of_record_batch_to_generate,
        Box::pin(move |_| unreachable!("should not be called after take")),
    );

    let scan_schema = Arc::new(Schema::new(vec![
        Field::new("col_0", DataType::UInt64, true),
        Field::new("col_1", DataType::Utf8, true),
    ]));

    let record_batch_size = args.task_ctx.session_config().batch_size() as u64;

    let schema = Arc::clone(&scan_schema);
    let plan: Arc<dyn ExecutionPlan> =
        Arc::new(OnceExec::new(Box::pin(RecordBatchStreamAdapter::new(
            Arc::clone(&schema),
            futures::stream::iter((0..args.number_of_record_batches as u64).map(
                move |index| {
                    let mut record_batch_memory_size =
                        get_size_of_record_batch_to_generate(index as usize);
                    record_batch_memory_size = record_batch_memory_size
                        .saturating_sub(size_of::<u64>() * record_batch_size as usize);

                    let string_item_size =
                        record_batch_memory_size / record_batch_size as usize;
                    let string_array =
                        Arc::new(StringArray::from_iter_values(std::iter::repeat_n(
                            "a".repeat(string_item_size),
                            record_batch_size as usize,
                        )));

                    RecordBatch::try_new(
                        Arc::clone(&schema),
                        vec![
                            Arc::new(UInt64Array::from_iter_values(
                                (index * record_batch_size)
                                    ..(index * record_batch_size) + record_batch_size,
                            )),
                            string_array,
                        ],
                    )
                    .map_err(|err| err.into())
                },
            )),
        ))));
    let sort_exec = Arc::new(SortExec::new(
        LexOrdering::new(vec![PhysicalSortExpr {
            expr: col("col_0", &scan_schema).unwrap(),
            options: SortOptions {
                descending: false,
                nulls_first: true,
            },
        }])
        .unwrap(),
        plan,
    ));

    let result = sort_exec.execute(0, Arc::clone(&args.task_ctx))?;

    let number_of_record_batches = args.number_of_record_batches;
    let assert_output_batch_size =
        args.assert_all_output_batches_roughly_match_batch_size_conf;

    let metrics = run_test(args, sort_exec, result).await?;

    assert_baseline_metrics_for_non_empty_output(
        &metrics,
        number_of_record_batches * record_batch_size as usize,
        if assert_output_batch_size {
            Some(record_batch_size as usize)
        } else {
            None
        },
    );

    Ok(metrics)
}

fn grow_memory_as_much_as_possible(
    memory_step: usize,
    memory_reservation: &mut MemoryReservation,
) -> Result<bool> {
    let mut was_able_to_grow = false;
    while memory_reservation.try_grow(memory_step).is_ok() {
        was_able_to_grow = true;
    }

    Ok(was_able_to_grow)
}

#[tokio::test]
async fn test_aggregate_with_high_cardinality_with_limited_memory() -> Result<()> {
    let record_batch_size = 8192;
    let pool_size = 2 * MB as usize;
    let task_ctx = {
        let memory_pool = Arc::new(FairSpillPool::new(pool_size));
        TaskContext::default()
            .with_session_config(SessionConfig::new().with_batch_size(record_batch_size))
            .with_runtime(Arc::new(
                RuntimeEnvBuilder::new()
                    .with_memory_pool(memory_pool)
                    .build()?,
            ))
    };

    let record_batch_size = pool_size / 16;

    // Basic test with a lot of groups that cannot all fit in memory and 1 record batch
    // from each spill file is too much memory
    let metrics =
        run_test_aggregate_with_high_cardinality(RunTestWithLimitedMemoryArgs {
            pool_size,
            task_ctx: Arc::new(task_ctx),
            number_of_record_batches: 100,
            get_size_of_record_batch_to_generate: Box::pin(move |_| record_batch_size),
            memory_behavior: Default::default(),
            assert_all_output_batches_roughly_match_batch_size_conf: true,
        })
        .await?;

    let total_spill_files_size =
        metrics.spill_count().unwrap_or_default() * record_batch_size;
    assert!(
        total_spill_files_size > pool_size,
        "Total spill files size {total_spill_files_size} should be greater than pool size {pool_size}",
    );

    Ok(())
}

#[tokio::test]
async fn test_aggregate_with_high_cardinality_with_limited_memory_and_different_sizes_of_record_batch()
-> Result<()> {
    let record_batch_size = 8192;
    let pool_size = 2 * MB as usize;
    let task_ctx = {
        let memory_pool = Arc::new(FairSpillPool::new(pool_size));
        TaskContext::default()
            .with_session_config(SessionConfig::new().with_batch_size(record_batch_size))
            .with_runtime(Arc::new(
                RuntimeEnvBuilder::new()
                    .with_memory_pool(memory_pool)
                    .build()?,
            ))
    };

    run_test_aggregate_with_high_cardinality(RunTestWithLimitedMemoryArgs {
        pool_size,
        task_ctx: Arc::new(task_ctx),
        number_of_record_batches: 100,
        get_size_of_record_batch_to_generate: Box::pin(move |i| {
            if i % 25 == 1 {
                pool_size / 6
            } else {
                (16 * KB) as usize
            }
        }),
        memory_behavior: Default::default(),
        assert_all_output_batches_roughly_match_batch_size_conf: true,
    })
    .await?;

    Ok(())
}

#[tokio::test]
async fn test_aggregate_with_high_cardinality_with_limited_memory_and_different_sizes_of_record_batch_and_changing_memory_reservation()
-> Result<()> {
    let record_batch_size = 8192;
    let pool_size = 2 * MB as usize;
    let task_ctx = {
        let memory_pool = Arc::new(FairSpillPool::new(pool_size));
        TaskContext::default()
            .with_session_config(SessionConfig::new().with_batch_size(record_batch_size))
            .with_runtime(Arc::new(
                RuntimeEnvBuilder::new()
                    .with_memory_pool(memory_pool)
                    .build()?,
            ))
    };

    run_test_aggregate_with_high_cardinality(RunTestWithLimitedMemoryArgs {
        pool_size,
        task_ctx: Arc::new(task_ctx),
        number_of_record_batches: 100,
        get_size_of_record_batch_to_generate: Box::pin(move |i| {
            if i % 25 == 1 {
                pool_size / 6
            } else {
                (16 * KB) as usize
            }
        }),
        memory_behavior: MemoryBehavior::TakeAllMemoryAndReleaseEveryNthBatch(10),
        assert_all_output_batches_roughly_match_batch_size_conf: true,
    })
    .await?;

    Ok(())
}

#[tokio::test]
async fn test_aggregate_with_high_cardinality_with_limited_memory_and_different_sizes_of_record_batch_and_take_all_memory()
-> Result<()> {
    let record_batch_size = 8192;
    let pool_size = 2 * MB as usize;
    let task_ctx = {
        let memory_pool = Arc::new(FairSpillPool::new(pool_size));
        TaskContext::default()
            .with_session_config(SessionConfig::new().with_batch_size(record_batch_size))
            .with_runtime(Arc::new(
                RuntimeEnvBuilder::new()
                    .with_memory_pool(memory_pool)
                    .build()?,
            ))
    };

    run_test_aggregate_with_high_cardinality(RunTestWithLimitedMemoryArgs {
        pool_size,
        task_ctx: Arc::new(task_ctx),
        number_of_record_batches: 100,
        get_size_of_record_batch_to_generate: Box::pin(move |i| {
            if i % 25 == 1 {
                pool_size / 6
            } else {
                (16 * KB) as usize
            }
        }),
        memory_behavior: MemoryBehavior::TakeAllMemoryAtTheBeginning,
        assert_all_output_batches_roughly_match_batch_size_conf: true,
    })
    .await?;

    Ok(())
}

#[tokio::test]
async fn test_aggregate_with_high_cardinality_with_limited_memory_and_large_record_batch()
-> Result<()> {
    let record_batch_size = 8192;
    let pool_size = 2 * MB as usize;
    let task_ctx = {
        let memory_pool = Arc::new(FairSpillPool::new(pool_size));
        TaskContext::default()
            .with_session_config(SessionConfig::new().with_batch_size(record_batch_size))
            .with_runtime(Arc::new(
                RuntimeEnvBuilder::new()
                    .with_memory_pool(memory_pool)
                    .build()?,
            ))
    };

    // Test that the merge degree of multi level merge sort cannot be fixed size when there is not enough memory
    run_test_aggregate_with_high_cardinality(RunTestWithLimitedMemoryArgs {
        pool_size,
        task_ctx: Arc::new(task_ctx),
        number_of_record_batches: 100,
        get_size_of_record_batch_to_generate: Box::pin(move |_| pool_size / 6),
        memory_behavior: Default::default(),
        assert_all_output_batches_roughly_match_batch_size_conf: true,
    })
    .await?;

    Ok(())
}

async fn run_test_aggregate_with_high_cardinality(
    mut args: RunTestWithLimitedMemoryArgs,
) -> Result<MetricsSet> {
    let get_size_of_record_batch_to_generate = std::mem::replace(
        &mut args.get_size_of_record_batch_to_generate,
        Box::pin(move |_| unreachable!("should not be called after take")),
    );
    let scan_schema = Arc::new(Schema::new(vec![
        Field::new("col_0", DataType::UInt64, true),
        Field::new("col_1", DataType::Utf8, true),
    ]));

    let group_by = PhysicalGroupBy::new_single(vec![(
        Arc::new(Column::new("col_0", 0)),
        "col_0".to_string(),
    )]);

    let aggregate_expressions = vec![Arc::new(
        AggregateExprBuilder::new(
            array_agg_udaf(),
            vec![col("col_1", &scan_schema).unwrap()],
        )
        .schema(Arc::clone(&scan_schema))
        .alias("array_agg(col_1)")
        .build()?,
    )];

    let record_batch_size = args.task_ctx.session_config().batch_size() as u64;

    let schema = Arc::clone(&scan_schema);
    let plan: Arc<dyn ExecutionPlan> =
        Arc::new(OnceExec::new(Box::pin(RecordBatchStreamAdapter::new(
            Arc::clone(&schema),
            futures::stream::iter((0..args.number_of_record_batches as u64).map(
                move |index| {
                    let mut record_batch_memory_size =
                        get_size_of_record_batch_to_generate(index as usize);
                    record_batch_memory_size = record_batch_memory_size
                        .saturating_sub(size_of::<u64>() * record_batch_size as usize);

                    let string_item_size =
                        record_batch_memory_size / record_batch_size as usize;
                    let string_array =
                        Arc::new(StringArray::from_iter_values(std::iter::repeat_n(
                            "a".repeat(string_item_size),
                            record_batch_size as usize,
                        )));

                    RecordBatch::try_new(
                        Arc::clone(&schema),
                        vec![
                            // Grouping key
                            Arc::new(UInt64Array::from_iter_values(
                                (index * record_batch_size)
                                    ..(index * record_batch_size) + record_batch_size,
                            )),
                            // Grouping value
                            string_array,
                        ],
                    )
                    .map_err(|err| err.into())
                },
            )),
        ))));

    let aggregate_exec = Arc::new(AggregateExec::try_new(
        AggregateMode::Partial,
        group_by.clone(),
        aggregate_expressions.clone(),
        vec![None; aggregate_expressions.len()],
        plan,
        Arc::clone(&scan_schema),
    )?);
    let aggregate_final = Arc::new(AggregateExec::try_new(
        AggregateMode::Final,
        group_by,
        aggregate_expressions.clone(),
        vec![None; aggregate_expressions.len()],
        aggregate_exec,
        Arc::clone(&scan_schema),
    )?);

    let result = aggregate_final.execute(0, Arc::clone(&args.task_ctx))?;

    run_test(args, aggregate_final, result).await
}

async fn run_test(
    args: RunTestWithLimitedMemoryArgs,
    plan: Arc<dyn ExecutionPlan>,
    result_stream: SendableRecordBatchStream,
) -> Result<MetricsSet> {
    let number_of_record_batches = args.number_of_record_batches;

    consume_stream_and_simulate_other_running_memory_consumers(args, result_stream)
        .await?;

    let metrics = plan.metrics().expect("must have metrics");
    let spill_count = assert_spill_count_metric(true, plan);

    assert!(
        spill_count > 0,
        "Expected spill, but did not, number of record batches: {number_of_record_batches}",
    );

    Ok(metrics)
}

/// Consume the stream and change the amount of memory used while consuming it based on the [`MemoryBehavior`] provided
async fn consume_stream_and_simulate_other_running_memory_consumers(
    args: RunTestWithLimitedMemoryArgs,
    mut result_stream: SendableRecordBatchStream,
) -> Result<()> {
    let mut number_of_rows = 0;
    let record_batch_size = args.task_ctx.session_config().batch_size() as u64;

    let memory_pool = args.task_ctx.memory_pool();
    let memory_consumer = MemoryConsumer::new("mock_memory_consumer");
    let mut memory_reservation = memory_consumer.register(memory_pool);

    let mut index = 0;
    let mut memory_took = false;

    while let Some(batch) = result_stream.next().await {
        match args.memory_behavior {
            MemoryBehavior::AsIs => {
                // Do nothing
            }
            MemoryBehavior::TakeAllMemoryAtTheBeginning => {
                if !memory_took {
                    memory_took = true;
                    grow_memory_as_much_as_possible(10, &mut memory_reservation)?;
                }
            }
            MemoryBehavior::TakeAllMemoryAndReleaseEveryNthBatch(n) => {
                if !memory_took {
                    memory_took = true;
                    grow_memory_as_much_as_possible(
                        args.pool_size,
                        &mut memory_reservation,
                    )?;
                } else if index % n == 0 {
                    // release memory
                    memory_reservation.free();
                }
            }
        }

        let batch = batch?;
        number_of_rows += batch.num_rows();

        index += 1;
    }

    assert_eq!(
        number_of_rows,
        args.number_of_record_batches * record_batch_size as usize
    );

    Ok(())
}

/// Assert baseline metrics are as expected or around that
///
/// `output_batch_size` should be `None` when you expect to not get batched at the same size
/// `Some(session conf batch size)` for the rest
fn assert_baseline_metrics_for_non_empty_output(
    metrics: &MetricsSet,
    expected_output_rows: usize,
    output_batch_size: Option<usize>,
) {
    let end_time = metrics
        .iter()
        .find_map(|item| match item.value() {
            MetricValue::EndTimestamp(end) => Some(end),
            _ => None,
        })
        .expect("Must have end time metric since it exists in the baseline");

    assert_ne!(end_time.value(), None);

    assert_eq!(metrics.output_rows(), Some(expected_output_rows));

    let output_bytes = metrics
        .iter()
        .find_map(|item| match item.value() {
            MetricValue::OutputBytes(total) => Some(total),
            _ => None,
        })
        .expect("Must have output_bytes metric since it exists in the baseline");

    assert_ne!(output_bytes.value(), 0_usize);

    let output_batches = get_output_batches_from_metrics(metrics);

    if let Some(output_batch_size) = output_batch_size {
        assert_eq!(
            output_batches,
            expected_output_rows.div_ceil(output_batch_size)
        );
    } else {
        assert_ne!(output_batches, 0,);
    }
}

fn get_output_batches_from_metrics(metrics: &MetricsSet) -> usize {
    metrics
        .iter()
        .find_map(|item| match item.value() {
            MetricValue::OutputBatches(total) => Some(total.value()),
            _ => None,
        })
        .expect("Must have output_batches metric since it exists in the baseline")
}
