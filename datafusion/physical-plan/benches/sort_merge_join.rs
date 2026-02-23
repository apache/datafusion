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

use arrow::array::{Int64Array, RecordBatch, StringArray};
use arrow::compute::SortOptions;
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use criterion::{BenchmarkId, Criterion, criterion_group, criterion_main};
use datafusion_common::NullEquality;
use datafusion_execution::TaskContext;
use datafusion_physical_expr::expressions::col;
use datafusion_physical_plan::collect;
use datafusion_physical_plan::joins::{SortMergeJoinExec, utils::JoinOn};
use datafusion_physical_plan::test::TestMemoryExec;
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
    let mut rows: Vec<(i64, i64)> = (0..num_rows)
        .map(|i| ((i % key_mod) as i64, i as i64))
        .collect();
    rows.sort();

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

    let batch_size = 8192;
    let mut batches = Vec::new();
    let mut offset = 0;
    while offset < batch.num_rows() {
        let len = (batch.num_rows() - offset).min(batch_size);
        batches.push(batch.slice(offset, len));
        offset += len;
    }
    batches
}

fn make_exec(
    batches: &[RecordBatch],
    schema: &SchemaRef,
) -> Arc<dyn datafusion_physical_plan::ExecutionPlan> {
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
    left: Arc<dyn datafusion_physical_plan::ExecutionPlan>,
    right: Arc<dyn datafusion_physical_plan::ExecutionPlan>,
    join_type: datafusion_common::JoinType,
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
                do_join(left, right, datafusion_common::JoinType::Inner, &rt)
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
                do_join(left, right, datafusion_common::JoinType::Inner, &rt)
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
                do_join(left, right, datafusion_common::JoinType::Left, &rt)
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
                do_join(left, right, datafusion_common::JoinType::LeftSemi, &rt)
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
                do_join(left, right, datafusion_common::JoinType::LeftAnti, &rt)
            })
        });
    }

    group.finish();
}

criterion_group!(benches, bench_smj);
criterion_main!(benches);
