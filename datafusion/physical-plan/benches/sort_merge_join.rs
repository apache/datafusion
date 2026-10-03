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

//! Criterion benchmarks for Sort Merge Join
//!
//! These benchmarks measure the join kernel in isolation by feeding
//! pre-sorted RecordBatches directly into SortMergeJoinExec, avoiding
//! sort / scan overhead.

use std::sync::Arc;

use arrow::array::{ArrayRef, Int64Array, RecordBatch, StringArray};
use arrow::compute::SortOptions;
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use criterion::{
    BatchSize, BenchmarkId, Criterion, Throughput, criterion_group, criterion_main,
};
use datafusion_common::{JoinSide, JoinType, NullEquality, ScalarValue};
use datafusion_common_runtime::SpawnedTask;
use datafusion_execution::{
    TaskContext, config::SessionConfig, memory_pool::FairSpillPool,
    runtime_env::RuntimeEnvBuilder,
};
use datafusion_expr::Operator;
use datafusion_physical_expr::expressions::{BinaryExpr, Column, Literal, col};
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

fn do_join(
    left: Arc<dyn ExecutionPlan>,
    right: Arc<dyn ExecutionPlan>,
    join_type: JoinType,
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
        None,
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

/// Compare exact base and candidate builds with identical pre-sorted inputs.
/// SQL versions also measure sorting and the optimizer's join selection; these
/// cases isolate residual semi/anti joins, including output collection.
fn bench_semi_anti_filter(c: &mut Criterion) {
    let mut group = c.benchmark_group("sort_merge_join_semi_anti_filter");

    // Vary group count, rows per side, selectivity, and value type. Early-match
    // cases measure the cost of summarizing when the first inner row suffices;
    // the one-probe case gives the summary no reuse within the group.
    for (name, groups, probe_rows, inner_rows, early_match, op, kind) in [
        (
            "not_equal_no_match",
            1,
            256,
            4096,
            false,
            Operator::NotEq,
            JoinType::LeftAnti,
        ),
        (
            "not_equal_early_match",
            16,
            64,
            128,
            true,
            Operator::NotEq,
            JoinType::LeftSemi,
        ),
        // The first probe fails, but the second inner row matches every
        // outer row. This exposes unnecessary full-group work after a miss.
        (
            "not_equal_second_witness",
            16,
            64,
            4096,
            true,
            Operator::NotEq,
            JoinType::LeftSemi,
        ),
        (
            "small_group_no_match",
            4096,
            2,
            3,
            false,
            Operator::NotEq,
            JoinType::LeftAnti,
        ),
        (
            "small_group_four_no_match",
            4096,
            4,
            4,
            false,
            Operator::NotEq,
            JoinType::LeftAnti,
        ),
        (
            "small_group_seven_no_match",
            4096,
            7,
            7,
            false,
            Operator::NotEq,
            JoinType::LeftAnti,
        ),
        (
            "singleton_early_match",
            8192,
            1,
            1,
            true,
            Operator::Lt,
            JoinType::LeftSemi,
        ),
        (
            "range_no_match",
            1,
            256,
            4096,
            false,
            Operator::Gt,
            JoinType::LeftSemi,
        ),
        (
            "range_early_match_one_probe",
            16,
            1,
            4096,
            true,
            Operator::Lt,
            JoinType::LeftSemi,
        ),
        (
            "range_early_match_many_probes",
            16,
            64,
            4096,
            true,
            Operator::Lt,
            JoinType::LeftSemi,
        ),
        // An OR residual remains on the pairwise path and controls for the
        // eligibility check rather than measuring the min/max optimization.
        (
            "unsupported_or_no_match",
            16,
            64,
            128,
            false,
            Operator::Or,
            JoinType::LeftAnti,
        ),
        (
            "unsupported_guard_no_match",
            16,
            64,
            128,
            false,
            Operator::And,
            JoinType::LeftAnti,
        ),
    ] {
        for value_type in [DataType::Int64, DataType::Utf8] {
            let expected = if op == Operator::Gt {
                0
            } else {
                groups * probe_rows
            };
            let modes: &[&str] = if name.starts_with("small_group") {
                &[
                    "single",
                    "shared_fair_pool_4_tasks",
                    "isolated_fair_pools_4_tasks",
                ]
            } else {
                &["single"]
            };
            for mode in modes {
                let tasks = if *mode == "single" { 1 } else { 4 };
                // Input rows, not hypothetical pair comparisons, are the throughput unit.
                group.throughput(Throughput::Elements(
                    (tasks * groups * (probe_rows + inner_rows)) as u64,
                ));
                let parameter = if tasks == 1 {
                    value_type.to_string()
                } else {
                    format!("{value_type}_{mode}")
                };
                group.bench_function(BenchmarkId::new(name, parameter), |b| {
                    // Build and verify only the selected fixture. Criterion may
                    // invoke this callback repeatedly, outside its timed iterations.
                    let rt = tokio::runtime::Builder::new_multi_thread()
                        .worker_threads(4)
                        .enable_all()
                        .build()
                        .unwrap();
                    let input = |rows_per_group: usize, early_match: bool| {
                        let rows = groups * rows_per_group;
                        let values = (0..rows).map(|row| {
                            7 + if early_match {
                                row % rows_per_group
                                    + usize::from(name != "not_equal_second_witness")
                            } else {
                                0
                            }
                        });
                        let values: ArrayRef = if value_type == DataType::Utf8 {
                            // Wide owned strings make the min/max pass more expensive
                            // than Int64; creation stays outside the timed region.
                            Arc::new(StringArray::from_iter_values(
                                values.map(|value| format!("{:x>120}{value:08}", "")),
                            ))
                        } else {
                            Arc::new(Int64Array::from_iter_values(
                                values.map(|v| v as i64),
                            ))
                        };
                        let batch = RecordBatch::try_from_iter(vec![
                            (
                                "key",
                                Arc::new(Int64Array::from_iter_values(
                                    (0..rows).map(|row| (row / rows_per_group) as i64),
                                )) as ArrayRef,
                            ),
                            ("value", values),
                        ])
                        .unwrap();
                        // Exercise groups spanning batches and boundaries within a batch.
                        let batches = (0..rows)
                            .step_by(127)
                            .map(|offset| batch.slice(offset, 127.min(rows - offset)))
                            .collect::<Vec<_>>();
                        make_exec(&batches, &batch.schema())
                    };
                    let left = input(probe_rows, false);
                    let right = input(inner_rows, early_match);
                    let make_plan = || {
                        let comparison = |op| {
                            Arc::new(BinaryExpr::new(
                                Arc::new(Column::new("left_value", 0)),
                                op,
                                Arc::new(Column::new("right_value", 1)),
                            ))
                        };
                        let mut columns = vec![
                            ColumnIndex {
                                index: 1,
                                side: JoinSide::Left,
                            },
                            ColumnIndex {
                                index: 1,
                                side: JoinSide::Right,
                            },
                        ];
                        let mut fields = vec![
                            Field::new("left_value", value_type.clone(), false),
                            Field::new("right_value", value_type.clone(), false),
                        ];
                        let expression = match op {
                            Operator::Or => Arc::new(BinaryExpr::new(
                                comparison(Operator::NotEq),
                                Operator::Or,
                                comparison(Operator::Gt),
                            )),
                            Operator::And => {
                                columns.push(ColumnIndex {
                                    index: 0,
                                    side: JoinSide::Right,
                                });
                                fields.push(Field::new(
                                    "right_key",
                                    DataType::Int64,
                                    false,
                                ));
                                Arc::new(BinaryExpr::new(
                                    comparison(Operator::NotEq),
                                    Operator::And,
                                    Arc::new(BinaryExpr::new(
                                        Arc::new(Column::new("right_key", 2)),
                                        Operator::GtEq,
                                        Arc::new(Literal::new(ScalarValue::Int64(Some(
                                            0,
                                        )))),
                                    )),
                                ))
                            }
                            _ => comparison(op),
                        };
                        let filter = JoinFilter::new(
                            expression,
                            columns,
                            Arc::new(Schema::new(fields)),
                        );
                        Arc::new(
                            SortMergeJoinExec::try_new(
                                Arc::clone(&left),
                                Arc::clone(&right),
                                vec![(
                                    Arc::new(Column::new("key", 0)),
                                    Arc::new(Column::new("key", 0)),
                                )],
                                Some(filter),
                                kind,
                                vec![SortOptions::default()],
                                NullEquality::NullEqualsNothing,
                            )
                            .unwrap(),
                        ) as Arc<dyn ExecutionPlan>
                    };
                    let make_context = || {
                        TaskContext::default().with_session_config(
                            SessionConfig::new().with_batch_size(512),
                        )
                    };
                    let contexts = if tasks == 1 {
                        vec![Arc::new(make_context())]
                    } else {
                        let new_runtime = || {
                            RuntimeEnvBuilder::new()
                                .with_memory_pool(Arc::new(FairSpillPool::new(
                                    256 * 1024 * 1024,
                                )))
                                .build_arc()
                                .unwrap()
                        };
                        let shared = new_runtime();
                        (0..tasks)
                            .map(|_| {
                                Arc::new(make_context().with_runtime(
                                    if *mode == "shared_fair_pool_4_tasks" {
                                        Arc::clone(&shared)
                                    } else {
                                        new_runtime()
                                    },
                                ))
                            })
                            .collect::<Vec<_>>()
                    };
                    let make_plans =
                        || (0..tasks).map(|_| make_plan()).collect::<Vec<_>>();
                    let execute = |plans: Vec<Arc<dyn ExecutionPlan>>| {
                        rt.block_on(async {
                            if tasks == 1 {
                                let batches = collect(
                                    Arc::clone(&plans[0]),
                                    Arc::clone(&contexts[0]),
                                )
                                .await
                                .unwrap();
                                return batches
                                    .iter()
                                    .map(RecordBatch::num_rows)
                                    .sum::<usize>();
                            }
                            // Actually spawn onto runtime workers; joining futures on this
                            // thread would not exercise contention on the shared pool mutex.
                            let barrier = Arc::new(tokio::sync::Barrier::new(tasks));
                            let handles = plans
                                .into_iter()
                                .zip(&contexts)
                                .map(|(plan, context)| {
                                    let context = Arc::clone(context);
                                    let barrier = Arc::clone(&barrier);
                                    SpawnedTask::spawn(async move {
                                        barrier.wait().await;
                                        let batches =
                                            collect(plan, context).await.unwrap();
                                        batches
                                            .iter()
                                            .map(RecordBatch::num_rows)
                                            .sum::<usize>()
                                    })
                                })
                                .collect::<Vec<_>>();
                            let mut rows = 0;
                            for handle in handles {
                                rows += handle.await.unwrap();
                            }
                            rows
                        })
                    };
                    let verification = make_plans();
                    assert_eq!(execute(verification.clone()), tasks * expected);
                    for plan in verification {
                        assert_eq!(plan.metrics().unwrap().spill_count(), Some(0));
                    }
                    // Execution, task overhead, collection and output destruction
                    // are timed. Input and fresh plan construction are excluded.
                    b.iter_batched(make_plans, execute, BatchSize::PerIteration);
                });
            }
        }
    }
    group.finish();
}

criterion_group!(benches, bench_smj, bench_semi_anti_filter);
criterion_main!(benches);
