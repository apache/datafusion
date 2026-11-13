// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: See the NOTICE file distributed with this work for additional information regarding copyright ownership
// SPDX-FileContributor: Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements

//! Single-partition external sort with no competing pool consumers.
//!
//! Measures spill-merge overhead for 60 batches of 8192 rows, each containing
//! an Int64 key and a 120-byte Utf8 payload. Pool and workspace sizes use decimal MB.
//! Input generation, plan setup, output validation, and metric reporting are untimed.
//! Spill counts include intermediate rewrites; they are not merge-pass counts.
//!
//! Run the same benchmark source on both revisions, on the same machine:
//! `cargo bench -p datafusion-physical-plan --bench sort_spill --features test_utils -- --save-baseline before`
//! `cargo bench -p datafusion-physical-plan --bench sort_spill --features test_utils -- --baseline before`
//! Preserve Criterion's target directory between runs. For a headroom-on/off A/B,
//! use otherwise identical revisions differing only in the sort's headroom opt-in.
//! There is no public policy toggle; workspace=0 does not turn headroom off.

use std::hint::black_box;
use std::sync::Arc;

use arrow::array::{Array, Int64Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use criterion::{BatchSize, BenchmarkId, Criterion, criterion_group, criterion_main};
use datafusion_common::Result;
use datafusion_common::config::SpillCompression;
use datafusion_execution::TaskContext;
use datafusion_execution::config::SessionConfig;
use datafusion_execution::memory_pool::{GreedyMemoryPool, MemoryPool};
use datafusion_execution::runtime_env::RuntimeEnvBuilder;
use datafusion_physical_expr::{LexOrdering, PhysicalSortExpr, expressions::col};
use datafusion_physical_plan::ExecutionPlan;
use datafusion_physical_plan::sorts::sort::SortExec;
use datafusion_physical_plan::test::TestMemoryExec;
use futures::TryStreamExt;
use rand::{SeedableRng, seq::SliceRandom};

const BATCH_SIZE: usize = 8192;
const BATCHES: usize = 60;
const ROWS: usize = BATCHES * BATCH_SIZE;
const PAYLOAD: &str = "012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789";

fn input() -> Vec<RecordBatch> {
    let schema = Arc::new(Schema::new(vec![
        Field::new("key", DataType::Int64, false),
        Field::new("payload", DataType::Utf8, false),
    ]));
    let mut keys: Vec<i64> = (0..ROWS as i64).collect();
    keys.shuffle(&mut rand::rngs::StdRng::seed_from_u64(42));
    keys.chunks(BATCH_SIZE)
        .map(|chunk| {
            RecordBatch::try_new(
                Arc::clone(&schema),
                vec![
                    Arc::new(Int64Array::from(chunk.to_vec())),
                    Arc::new(StringArray::from_iter_values(std::iter::repeat_n(
                        PAYLOAD,
                        chunk.len(),
                    ))),
                ],
            )
            .unwrap()
        })
        .collect()
}

fn setup(
    input: &[RecordBatch],
    workspace_mb: usize,
    pool_mb: usize,
) -> (
    Arc<dyn ExecutionPlan>,
    Arc<TaskContext>,
    Arc<dyn MemoryPool>,
) {
    let schema = input[0].schema();
    let source =
        TestMemoryExec::try_new_exec(&[input.to_vec()], Arc::clone(&schema), None)
            .unwrap();
    let ordering =
        LexOrdering::new([PhysicalSortExpr::new_default(col("key", &schema).unwrap())])
            .unwrap();
    let plan: Arc<dyn ExecutionPlan> = Arc::new(SortExec::new(ordering, source));
    let pool: Arc<dyn MemoryPool> = Arc::new(GreedyMemoryPool::new(pool_mb * 1_000_000));
    let runtime = RuntimeEnvBuilder::new()
        .with_memory_pool(Arc::clone(&pool))
        .build_arc()
        .unwrap();
    let config = SessionConfig::new()
        .with_target_partitions(1)
        .with_batch_size(BATCH_SIZE)
        .with_sort_spill_reservation_bytes(workspace_mb * 1_000_000)
        .with_spill_compression(SpillCompression::Uncompressed);
    let context = Arc::new(
        TaskContext::default()
            .with_session_config(config)
            .with_runtime(runtime),
    );
    (plan, context, pool)
}

async fn run(
    plan: Arc<dyn ExecutionPlan>,
    context: Arc<TaskContext>,
    validate: bool,
) -> Result<usize> {
    let mut stream = plan.execute(0, context)?;
    let mut count = 0;
    while let Some(batch) = stream.try_next().await? {
        if validate {
            let keys = batch
                .column(0)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap();
            let payload = batch
                .column(1)
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap();
            assert_eq!(keys.null_count(), 0);
            assert_eq!(payload.null_count(), 0);
            for row in 0..batch.num_rows() {
                // The shuffled keys are exactly the integers 0..ROWS.
                assert_eq!(keys.value(row), (count + row) as i64);
                assert_eq!(payload.value(row), PAYLOAD);
            }
        }
        count += batch.num_rows();
    }
    Ok(count)
}

fn benchmark(c: &mut Criterion) {
    assert_eq!(PAYLOAD.len(), 120);
    let input = input();
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    let mut group = c.benchmark_group("sort_spill");
    group.sample_size(10);
    for (workspace_mb, pool_mb) in [(1, 16), (1, 32), (10, 32), (10, 24)] {
        let id = format!("workspace{workspace_mb}_pool{pool_mb}");
        group.bench_function(BenchmarkId::new("mb", &id), |b| {
            let (plan, context, pool) = setup(&input, workspace_mb, pool_mb);
            let count = runtime
                .block_on(run(Arc::clone(&plan), context, true))
                .unwrap();
            assert_eq!(count, ROWS);
            let metrics = plan.metrics().unwrap();
            let spills = metrics.spill_count().unwrap();
            assert!(spills > 0, "{id}: expected spills");
            eprintln!(
                "{id}: spill_count={spills}, spilled_rows={}, spilled_bytes={}",
                metrics.spilled_rows().unwrap(),
                metrics.spilled_bytes().unwrap()
            );
            drop(plan);
            assert_eq!(pool.reserved(), 0, "{id}: memory reservation leaked");
            b.iter_batched(
                || setup(&input, workspace_mb, pool_mb),
                |(plan, context, _pool)| {
                    let count = runtime.block_on(run(plan, context, false)).unwrap();
                    assert_eq!(count, ROWS);
                    black_box(count);
                },
                BatchSize::PerIteration,
            );
        });
    }
    group.finish();
}

criterion_group!(benches, benchmark);
criterion_main!(benches);
