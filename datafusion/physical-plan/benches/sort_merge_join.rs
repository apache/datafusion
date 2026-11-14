// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: See the NOTICE file distributed with this work for additional information regarding copyright ownership
// SPDX-FileContributor: Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements

//! Criterion benchmarks for Sort Merge Join
//!
//! These benchmarks measure the join kernel in isolation by feeding
//! pre-sorted RecordBatches directly into SortMergeJoinExec, avoiding
//! sort / scan overhead.

use std::sync::Arc;

use arrow::array::{ArrayRef, Int64Array, RecordBatch, StringArray};
use arrow::compute::SortOptions;
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use criterion::{BatchSize, BenchmarkId, Criterion, criterion_group, criterion_main};
use datafusion_common::{JoinSide, JoinType, NullEquality};
use datafusion_common_runtime::SpawnedTask;
use datafusion_execution::{
    TaskContext, config::SessionConfig, memory_pool::FairSpillPool,
    runtime_env::RuntimeEnvBuilder,
};
use datafusion_expr::Operator;
use datafusion_physical_expr::expressions::{BinaryExpr, Column, col};
use datafusion_physical_plan::joins::{
    SortMergeJoinExec,
    utils::{ColumnIndex, JoinFilter, JoinOn},
};
use datafusion_physical_plan::test::TestMemoryExec;
use datafusion_physical_plan::{ExecutionPlan, collect};
use tokio::runtime::Runtime;

/// Build pre-sorted RecordBatches (split into ~8192-row chunks).
///
/// Schema: (key: Int64, data: Int64, payload: Utf8)
///
/// `key_mod` controls distinct key count: key = row_index % key_mod.
fn build_sorted_batches(
    num_rows: usize,
    key_mod: usize,
    schema: &SchemaRef,
) -> Vec<RecordBatch> {
    build_sorted_batches_with_size(num_rows, key_mod, 8192, schema)
}

/// Like [`build_sorted_batches`], but with an explicit output batch size.
///
/// `SortMergeJoinExec` takes arbitrary children, so the buffered side is not
/// guaranteed to arrive in `batch_size`-sized batches. Small `batch_size`
/// values make a single key group span many buffered batches, which is what
/// drives `materialize_right_columns` onto its multi-source `interleave` path.
fn build_sorted_batches_with_size(
    num_rows: usize,
    key_mod: usize,
    batch_size: usize,
    schema: &SchemaRef,
) -> Vec<RecordBatch> {
    let mut rows: Vec<(i64, i64)> = (0..num_rows)
        .map(|i| ((i % key_mod) as i64, i as i64))
        .collect();
    rows.sort_unstable();

    let keys: Vec<i64> = rows.iter().map(|(k, _)| *k).collect();
    let data: Vec<i64> = rows.iter().map(|(_, d)| *d).collect();
    let payload: Vec<String> = data.iter().map(|d| format!("val_{d}")).collect();

    let batch = RecordBatch::try_new(
        Arc::clone(schema),
        vec![
            Arc::new(Int64Array::from(keys)),
            Arc::new(Int64Array::from(data)),
            Arc::new(StringArray::from(payload)),
        ],
    )
    .unwrap();

    let mut batches = Vec::new();
    let mut offset = 0;
    while offset < batch.num_rows() {
        let len = (batch.num_rows() - offset).min(batch_size);
        batches.push(batch.slice(offset, len));
        offset += len;
    }
    batches
}

fn make_exec(batches: &[RecordBatch], schema: &SchemaRef) -> Arc<dyn ExecutionPlan> {
    TestMemoryExec::try_new_exec(&[batches.to_vec()], Arc::clone(schema), None).unwrap()
}

fn schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("key", DataType::Int64, false),
        Field::new("data", DataType::Int64, false),
        Field::new("payload", DataType::Utf8, false),
    ]))
}

fn data_filter(op: Operator, data_type: &DataType) -> JoinFilter {
    let column_indices = vec![
        ColumnIndex {
            index: 1,
            side: JoinSide::Left,
        },
        ColumnIndex {
            index: 1,
            side: JoinSide::Right,
        },
    ];
    let intermediate_schema = Schema::new(vec![
        Field::new("left_data", data_type.clone(), false),
        Field::new("right_data", data_type.clone(), false),
    ]);
    let expression = Arc::new(BinaryExpr::new(
        Arc::new(Column::new("left_data", 0)),
        op,
        Arc::new(Column::new("right_data", 1)),
    ));
    JoinFilter::new(expression, column_indices, Arc::new(intermediate_schema))
}

fn do_join(
    left: Arc<dyn ExecutionPlan>,
    right: Arc<dyn ExecutionPlan>,
    join_type: JoinType,
    rt: &Runtime,
) -> usize {
    do_join_with_filter(left, right, join_type, None, rt)
}

fn do_join_with_filter(
    left: Arc<dyn ExecutionPlan>,
    right: Arc<dyn ExecutionPlan>,
    join_type: JoinType,
    filter: Option<JoinFilter>,
    rt: &Runtime,
) -> usize {
    let on: JoinOn = vec![(
        col("key", &left.schema()).unwrap(),
        col("key", &right.schema()).unwrap(),
    )];
    let join = SortMergeJoinExec::try_new(
        left,
        right,
        on,
        filter,
        join_type,
        vec![SortOptions::default()],
        NullEquality::NullEqualsNothing,
    )
    .unwrap();

    let task_ctx = Arc::new(TaskContext::default());
    rt.block_on(async {
        let batches = collect(Arc::new(join), task_ctx).await.unwrap();
        batches.iter().map(|b| b.num_rows()).sum()
    })
}

fn bench_smj(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();
    let s = schema();

    let mut group = c.benchmark_group("sort_merge_join");

    // 1:1 Inner Join — 100K rows each, unique keys
    // Best case for contiguous-range optimization: every index array is [0,1,2,...].
    {
        let n = 100_000;
        let left_batches = build_sorted_batches(n, n, &s);
        let right_batches = build_sorted_batches(n, n, &s);
        group.bench_function(BenchmarkId::new("inner_1to1", n), |b| {
            b.iter(|| {
                let left = make_exec(&left_batches, &s);
                let right = make_exec(&right_batches, &s);
                do_join(left, right, JoinType::Inner, &rt)
            })
        });
    }

    // 1:10 Inner Join — 100K left, 100K right, 10K distinct keys
    {
        let n = 100_000;
        let key_mod = 10_000;
        let left_batches = build_sorted_batches(n, key_mod, &s);
        let right_batches = build_sorted_batches(n, key_mod, &s);
        group.bench_function(BenchmarkId::new("inner_1to10", n), |b| {
            b.iter(|| {
                let left = make_exec(&left_batches, &s);
                let right = make_exec(&right_batches, &s);
                do_join(left, right, JoinType::Inner, &rt)
            })
        });
    }

    // 1:100 Inner Join — 20K left, 20K right, 200 distinct keys
    // Every streamed row matches 100 buffered rows, so the join emits 2M rows.
    {
        let n = 20_000;
        let key_mod = 200;
        let left_batches = build_sorted_batches(n, key_mod, &s);
        let right_batches = build_sorted_batches(n, key_mod, &s);
        group.bench_function(BenchmarkId::new("inner_1to100", n), |b| {
            b.iter(|| {
                let left = make_exec(&left_batches, &s);
                let right = make_exec(&right_batches, &s);
                do_join(left, right, JoinType::Inner, &rt)
            })
        });
    }

    // 1:100 Left Join with a filter — 20K left, 20K right, 200 distinct keys
    // As in `inner_1to100`, every streamed row matches 100 buffered rows on the key,
    // but here the pairs go through deferred filtering. `left.data < right.data`
    // keeps about half of them, and none of the last streamed row of each key, which
    // is null-joined.
    {
        let n = 20_000;
        let key_mod = 200;
        let left_batches = build_sorted_batches(n, key_mod, &s);
        let right_batches = build_sorted_batches(n, key_mod, &s);
        group.bench_function(BenchmarkId::new("left_1to100_filtered", n), |b| {
            b.iter(|| {
                let left = make_exec(&left_batches, &s);
                let right = make_exec(&right_batches, &s);
                do_join_with_filter(
                    left,
                    right,
                    JoinType::Left,
                    Some(data_filter(Operator::Lt, &DataType::Int64)),
                    &rt,
                )
            })
        });
    }

    // Left Join — 100K each, ~5% unmatched on left
    {
        let n = 100_000;
        let left_batches = build_sorted_batches(n, n + n / 20, &s);
        let right_batches = build_sorted_batches(n, n, &s);
        group.bench_function(BenchmarkId::new("left_1to1_unmatched", n), |b| {
            b.iter(|| {
                let left = make_exec(&left_batches, &s);
                let right = make_exec(&right_batches, &s);
                do_join(left, right, JoinType::Left, &rt)
            })
        });
    }

    // Left Semi Join — 100K left, 100K right, 10K keys
    {
        let n = 100_000;
        let key_mod = 10_000;
        let left_batches = build_sorted_batches(n, key_mod, &s);
        let right_batches = build_sorted_batches(n, key_mod, &s);
        group.bench_function(BenchmarkId::new("left_semi_1to10", n), |b| {
            b.iter(|| {
                let left = make_exec(&left_batches, &s);
                let right = make_exec(&right_batches, &s);
                do_join(left, right, JoinType::LeftSemi, &rt)
            })
        });
    }

    // Left Anti Join — 100K left, 100K right, partial match
    {
        let n = 100_000;
        let left_batches = build_sorted_batches(n, n + n / 5, &s);
        let right_batches = build_sorted_batches(n, n, &s);
        group.bench_function(BenchmarkId::new("left_anti_partial", n), |b| {
            b.iter(|| {
                let left = make_exec(&left_batches, &s);
                let right = make_exec(&right_batches, &s);
                do_join(left, right, JoinType::LeftAnti, &rt)
            })
        });
    }

    // Multi-source interleave path — one buffered key group spanning many
    // small buffered batches.
    //
    // Every other case here keeps a key group inside a single buffered batch,
    // so `materialize_right_columns` takes its single-source `take` fast path
    // and never reaches `interleave`. Shrinking the buffered batch size makes
    // a group span `group_rows / rows_per_batch` batches, which is what the
    // source-index mapping is actually paid for. Four streamed rows share each
    // key, so the buffered scan is re-walked per streamed row and freezes wrap
    // mid-group.
    {
        let keys = 8;
        let group_rows = 8192;
        let left_batches = build_sorted_batches(keys * 4, keys, &s);
        for rows_per_batch in [512, 64, 8] {
            let right_batches = build_sorted_batches_with_size(
                keys * group_rows,
                keys,
                rows_per_batch,
                &s,
            );
            group.bench_function(
                BenchmarkId::new("inner_group_spans_buffered_batches", rows_per_batch),
                |b| {
                    b.iter(|| {
                        let left = make_exec(&left_batches, &s);
                        let right = make_exec(&right_batches, &s);
                        do_join(left, right, JoinType::Inner, &rt)
                    })
                },
            );
        }
    }

    group.finish();
}

/// Rows before `first_witness` equal the outer value. All later rows are greater.
fn residual_input(
    groups: usize,
    rows_per_group: usize,
    first_witness: Option<usize>,
    data_type: &DataType,
) -> Arc<dyn ExecutionPlan> {
    let rows = groups * rows_per_group;
    let batches: Vec<_> = (0..rows)
        .step_by(127)
        .map(|start| {
            let indices = start..(start + 127).min(rows);
            let values = indices.clone().map(|i| {
                7 + i64::from(
                    first_witness.is_some_and(|first| i % rows_per_group >= first),
                )
            });
            let data: ArrayRef = match data_type {
                DataType::Int64 => Arc::new(Int64Array::from_iter_values(values)),
                DataType::Utf8 => Arc::new(StringArray::from_iter_values(
                    values.map(|v| format!("{:x>120}{v:08}", "")),
                )),
                _ => unreachable!(),
            };
            let keys = Int64Array::from_iter_values(
                indices.map(|i| (i / rows_per_group) as i64),
            );
            RecordBatch::try_from_iter(vec![
                ("key", Arc::new(keys) as ArrayRef),
                ("data", data),
            ])
            .unwrap()
        })
        .collect();
    make_exec(&batches, &batches[0].schema())
}

fn bench_semi_anti_filter(c: &mut Criterion) {
    let mut group = c.benchmark_group("sort_merge_join_semi_anti_filter");
    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(4)
        .enable_all()
        .build()
        .unwrap();

    // Only the seven-row case runs concurrently: it stresses per-group pool traffic.
    for (name, groups, outer_rows, inner_rows, first_witness, op, kind, compare_pools) in [
        (
            "not_equal_no_match",
            1,
            256,
            4096,
            None,
            Operator::NotEq,
            JoinType::LeftAnti,
            false,
        ),
        (
            "range_no_match",
            1,
            256,
            4096,
            None,
            Operator::Gt,
            JoinType::LeftSemi,
            false,
        ),
        (
            "range_early_match_many_probes",
            16,
            64,
            4096,
            Some(0),
            Operator::Lt,
            JoinType::LeftSemi,
            false,
        ),
        (
            "not_equal_third_witness",
            16,
            64,
            4096,
            Some(2),
            Operator::NotEq,
            JoinType::LeftSemi,
            false,
        ),
        (
            "range_early_match_one_probe",
            16,
            1,
            4096,
            Some(0),
            Operator::Lt,
            JoinType::LeftSemi,
            false,
        ),
        (
            "singleton_no_match",
            16,
            1,
            4096,
            None,
            Operator::Lt,
            JoinType::LeftAnti,
            false,
        ),
        (
            "small_group_seven_no_match",
            4096,
            7,
            7,
            None,
            Operator::NotEq,
            JoinType::LeftAnti,
            true,
        ),
    ] {
        for data_type in [DataType::Int64, DataType::Utf8] {
            for (pool_mode, workers, shared_pool) in [
                ("", 1, false),
                ("_shared_fair_pool_4_tasks", 4, true),
                ("_isolated_fair_pools_4_tasks", 4, false),
            ] {
                if workers > 1 && !compare_pools {
                    continue;
                }
                let id = BenchmarkId::new(name, format!("{data_type}{pool_mode}"));
                group.bench_function(id, |b| {
                    let left = residual_input(groups, outer_rows, None, &data_type);
                    let right =
                        residual_input(groups, inner_rows, first_witness, &data_type);
                    let new_pool = || Arc::new(FairSpillPool::new(256 * 1024 * 1024));
                    let shared = new_pool();
                    let contexts: Vec<_> = (0..workers)
                        .map(|_| {
                            let ctx = TaskContext::default().with_session_config(
                                SessionConfig::new().with_batch_size(512),
                            );
                            if workers == 1 {
                                return Arc::new(ctx);
                            }
                            let pool = if shared_pool {
                                Arc::clone(&shared)
                            } else {
                                new_pool()
                            };
                            let runtime = RuntimeEnvBuilder::new()
                                .with_memory_pool(pool)
                                .build_arc()
                                .unwrap();
                            Arc::new(ctx.with_runtime(runtime))
                        })
                        .collect();

                    let make_plans = || {
                        (0..workers)
                            .map(|_| {
                                let on: JoinOn = vec![(
                                    col("key", &left.schema()).unwrap(),
                                    col("key", &right.schema()).unwrap(),
                                )];
                                let plan = SortMergeJoinExec::try_new(
                                    Arc::clone(&left),
                                    Arc::clone(&right),
                                    on,
                                    Some(data_filter(op, &data_type)),
                                    kind,
                                    vec![SortOptions::default()],
                                    NullEquality::NullEqualsNothing,
                                )
                                .unwrap();
                                Arc::new(plan) as Arc<dyn ExecutionPlan>
                            })
                            .collect::<Vec<_>>()
                    };
                    let execute = |plans: Vec<Arc<dyn ExecutionPlan>>| {
                        rt.block_on(async {
                            let count = |plan, ctx| async move {
                                collect(plan, ctx)
                                    .await
                                    .unwrap()
                                    .iter()
                                    .map(RecordBatch::num_rows)
                                    .sum::<usize>()
                            };
                            if workers == 1 {
                                return count(
                                    Arc::clone(&plans[0]),
                                    Arc::clone(&contexts[0]),
                                )
                                .await;
                            }
                            let barrier = Arc::new(tokio::sync::Barrier::new(workers));
                            let handles: Vec<_> = plans
                                .into_iter()
                                .zip(&contexts)
                                .map(|(plan, ctx)| {
                                    let ctx = Arc::clone(ctx);
                                    let barrier = Arc::clone(&barrier);
                                    SpawnedTask::spawn(async move {
                                        barrier.wait().await;
                                        count(plan, ctx).await
                                    })
                                })
                                .collect();
                            let mut rows = 0;
                            for handle in handles {
                                rows += handle.await.unwrap();
                            }
                            rows
                        })
                    };
                    let verification = make_plans();
                    let emits = (kind == JoinType::LeftSemi) == first_witness.is_some();
                    let expected_rows = if emits {
                        workers * groups * outer_rows
                    } else {
                        0
                    };
                    assert_eq!(execute(verification.clone()), expected_rows);
                    for plan in verification {
                        assert_eq!(plan.metrics().unwrap().spill_count(), Some(0));
                    }
                    b.iter_batched(make_plans, execute, BatchSize::PerIteration);
                });
            }
        }
    }
    group.finish();
}

criterion_group!(benches, bench_smj, bench_semi_anti_filter);
criterion_main!(benches);
