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
use datafusion_common::{JoinSide, JoinType, NullEquality};
use datafusion_execution::{TaskContext, config::SessionConfig};
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

/// Compare execution with summaries enabled and disabled in the same binary.
/// Inputs are already sorted: SQL versions of these EXISTS/NOT EXISTS queries
/// also measure sorting and depend on the optimizer's choice of join algorithm.
/// These cases isolate the residual semi/anti join, including output collection.
fn bench_existence_summary(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();
    let mut group = c.benchmark_group("sort_merge_join_existence_summary");
    group.sample_size(10);
    group.warm_up_time(std::time::Duration::from_millis(500));
    group.measurement_time(std::time::Duration::from_secs(2));

    // Vary group count, rows per side, residual selectivity, and join type.
    // The early-witness case controls for the generic join's short circuit;
    // the small-group case measures the cost of repeatedly resetting summaries.
    for (name, groups, probe_rows, inner_rows, distinct, op, kind) in [
        (
            "not_equal_no_witness",
            1,
            256,
            4096,
            false,
            Operator::NotEq,
            JoinType::LeftAnti,
        ),
        (
            "not_equal_early_witness",
            16,
            64,
            128,
            true,
            Operator::NotEq,
            JoinType::LeftSemi,
        ),
        (
            "small_group_no_witness",
            4096,
            2,
            3,
            false,
            Operator::NotEq,
            JoinType::LeftAnti,
        ),
        (
            "range_no_witness",
            1,
            256,
            4096,
            false,
            Operator::Gt,
            JoinType::LeftSemi,
        ),
    ] {
        let input = |rows_per_group: usize, distinct: bool| {
            let rows = groups * rows_per_group;
            let batch = RecordBatch::try_from_iter(vec![
                (
                    "key",
                    Arc::new(Int64Array::from_iter_values(
                        (0..rows).map(|row| (row / rows_per_group) as i64),
                    )) as ArrayRef,
                ),
                (
                    "value",
                    Arc::new(Int64Array::from_iter_values(
                        (0..rows).map(|row| 7 + i64::from(distinct && row % 2 == 1)),
                    )),
                ),
            ])
            .unwrap();
            // Exercise groups spanning batches as well as boundaries inside a batch.
            let batches = (0..rows)
                .step_by(127)
                .map(|offset| batch.slice(offset, 127.min(rows - offset)))
                .collect::<Vec<_>>();
            make_exec(&batches, &batch.schema())
        };
        let left = input(probe_rows, false);
        let right = input(inner_rows, distinct);
        let make_plan = || {
            let filter = JoinFilter::new(
                Arc::new(BinaryExpr::new(
                    Arc::new(Column::new("left_value", 0)),
                    op,
                    Arc::new(Column::new("right_value", 1)),
                )),
                vec![
                    ColumnIndex {
                        index: 1,
                        side: JoinSide::Left,
                    },
                    ColumnIndex {
                        index: 1,
                        side: JoinSide::Right,
                    },
                ],
                Arc::new(Schema::new(vec![
                    Field::new("left_value", DataType::Int64, false),
                    Field::new("right_value", DataType::Int64, false),
                ])),
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
        let expected = if op == Operator::Gt {
            0
        } else {
            groups * probe_rows
        };
        // Input rows, not hypothetical pair comparisons, are the throughput unit.
        group.throughput(Throughput::Elements(
            (groups * (probe_rows + inner_rows)) as u64,
        ));
        for enabled in [false, true] {
            let mut config = SessionConfig::new().with_batch_size(512);
            config
                .options_mut()
                .execution
                .enable_sort_merge_join_existence_summary = enabled;
            let context = Arc::new(TaskContext::default().with_session_config(config));
            let execute = |plan| {
                rt.block_on(async {
                    let batches = collect(plan, Arc::clone(&context)).await.unwrap();
                    // Keep output destruction inside the measured execution.
                    batches.iter().map(RecordBatch::num_rows).sum::<usize>()
                })
            };
            let verification = make_plan();
            assert_eq!(execute(Arc::clone(&verification)), expected);
            let counter = |name| {
                verification
                    .metrics()
                    .unwrap()
                    .iter()
                    .filter(|metric| metric.value().name() == name)
                    .map(|metric| metric.value().as_usize())
                    .sum::<usize>()
            };
            assert_eq!(counter("existence_summary_enabled"), usize::from(enabled));
            eprintln!(
                "{name} summary={enabled}: groups={}, inner_rows={}, probe_rows={}, state_bytes={}",
                counter("existence_summary_groups"),
                counter("existence_summary_inner_rows"),
                counter("existence_summary_probe_rows"),
                counter("existence_summary_state_bytes"),
            );
            group.bench_function(
                BenchmarkId::new(name, if enabled { "on" } else { "off" }),
                |b| {
                    b.iter_batched(make_plan, execute, BatchSize::PerIteration);
                },
            );
        }
    }
    group.finish();
}

criterion_group!(benches, bench_smj, bench_existence_summary);
criterion_main!(benches);
