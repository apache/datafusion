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

//! Criterion benchmarks for Hash Join with RightSemi/RightAnti joins with Int32 keys.
//!
//! ## Key Benchmark Axes
//!
//! - **Density**: How tightly distinct keys pack into their numeric range.
//!   `density = num_distinct_keys / (max_key - min_key + 1)`.
//!   Examples for 5 distinct keys:
//!     - `[0, 1, 2, 3, 4]`      → 5/5  = 100% (fully packed)
//!     - `[0, 2, 4, 6, 8]`      → 5/9  ≈  55% (every 2nd slot)
//!     - `[0, 10, 20, 30, 40]`  → 5/41 ≈  12% (every 10th slot)
//!
//!   Why it matters for this workload: future potential semi/anti-join
//!   fast paths could exploit densely packed build keys to outperform the
//!   general hash-table path, which is largely insensitive to density.
//!   Varying density across benchmarks helps surface those potential gains
//!   under different key distributions. Density describes only the
//!   build-side key layout; the per-probe match count is tracked
//!   separately as fanout.
//!
//! - **Hit Rate**: The percentage of probe rows that find a match in the build side.
//!   This controls how often the join produces output rows.
//!
//! Semi/anti joins can short-circuit after finding the first match, so these
//! benchmarks help evaluate optimization strategies for existence checks.

use std::sync::Arc;

use arrow::array::{Int32Array, RecordBatch, StringArray};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use criterion::{BenchmarkId, Criterion, criterion_group, criterion_main};
use datafusion_common::{JoinType, NullEquality};
use datafusion_execution::TaskContext;
use datafusion_physical_expr::expressions::col;
use datafusion_physical_plan::collect;
use datafusion_physical_plan::joins::{HashJoinExec, PartitionMode, utils::JoinOn};
use datafusion_physical_plan::test::TestMemoryExec;
use tokio::runtime::Runtime;

/// Build RecordBatches with Int32 keys.
///
/// Schema: (key: Int32, data: Int32, payload: Utf8)
///
/// `key_mod` controls distinct key count: key = row_index % key_mod.
/// `key_offset` shifts keys to control hit rate.
fn build_batches(
    num_rows: usize,
    key_mod: usize,
    key_offset: i32,
    schema: &SchemaRef,
) -> Vec<RecordBatch> {
    let keys: Vec<i32> = (0..num_rows)
        .map(|i| ((i % key_mod) as i32) + key_offset)
        .collect();
    let data: Vec<i32> = (0..num_rows).map(|i| i as i32).collect();
    let payload: Vec<String> = data.iter().map(|d| format!("val_{d}")).collect();

    let batch = RecordBatch::try_new(
        Arc::clone(schema),
        vec![
            Arc::new(Int32Array::from(keys)),
            Arc::new(Int32Array::from(data)),
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
        Field::new("key", DataType::Int32, false),
        Field::new("data", DataType::Int32, false),
        Field::new("payload", DataType::Utf8, false),
    ]))
}

fn do_hash_join(
    left: Arc<dyn datafusion_physical_plan::ExecutionPlan>,
    right: Arc<dyn datafusion_physical_plan::ExecutionPlan>,
    join_type: JoinType,
    rt: &Runtime,
) -> usize {
    let on: JoinOn = vec![(
        col("key", &left.schema()).unwrap(),
        col("key", &right.schema()).unwrap(),
    )];
    let join = HashJoinExec::try_new(
        left,
        right,
        on,
        None,
        &join_type,
        None,
        PartitionMode::CollectLeft,
        NullEquality::NullEqualsNothing,
        false,
    )
    .unwrap();

    let task_ctx = Arc::new(TaskContext::default());
    rt.block_on(async {
        let batches = collect(Arc::new(join), task_ctx).await.unwrap();
        batches.iter().map(|b| b.num_rows()).sum()
    })
}

/// Build batches with sparse keys (key = row_index % key_mod * multiplier + key_offset).
/// The `multiplier` controls density: 1 = 100%, 2 = 50%, 10 = 10%.
fn build_batches_sparse(
    num_rows: usize,
    key_mod: usize,
    key_offset: i32,
    multiplier: i32,
    schema: &SchemaRef,
) -> Vec<RecordBatch> {
    let keys: Vec<i32> = (0..num_rows)
        .map(|i| ((i % key_mod) as i32) * multiplier + key_offset)
        .collect();
    let data: Vec<i32> = (0..num_rows).map(|i| i as i32).collect();
    let payload: Vec<String> = data.iter().map(|d| format!("val_{d}")).collect();

    let batch = RecordBatch::try_new(
        Arc::clone(schema),
        vec![
            Arc::new(Int32Array::from(keys)),
            Arc::new(Int32Array::from(data)),
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

fn bench_hash_join_semi_anti(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();
    let s = schema();

    let mut group = c.benchmark_group("hash_join_semi_anti");

    // Build side: 100K rows, Probe side: 1M rows
    // Matching ratio: 1:1 (build keys are unique, each probe matches at most 1 build row)
    let build_rows = 100_000;
    let probe_rows = 1_000_000;

    // =========================================================================
    // RightSemi Join benchmarks
    // =========================================================================

    // RightSemi - 100% Density, 100% hit rate
    // Keys: 0..100K contiguous, all probe rows find a match
    {
        let left_batches = build_batches(build_rows, build_rows, 0, &s);
        let right_batches = build_batches(probe_rows, build_rows, 0, &s);
        group.bench_function(BenchmarkId::new("right_semi_d100_h100", probe_rows), |b| {
            b.iter(|| {
                let left = make_exec(&left_batches, &s);
                let right = make_exec(&right_batches, &s);
                do_hash_join(left, right, JoinType::RightSemi, &rt)
            })
        });
    }

    // RightSemi - 100% Density, 10% hit rate
    // Keys: 0..100K contiguous, only 10% of probe rows find a match
    {
        let left_batches = build_batches(build_rows, build_rows, 0, &s);
        let right_batches = build_batches(probe_rows, build_rows * 10, 0, &s);
        group.bench_function(BenchmarkId::new("right_semi_d100_h10", probe_rows), |b| {
            b.iter(|| {
                let left = make_exec(&left_batches, &s);
                let right = make_exec(&right_batches, &s);
                do_hash_join(left, right, JoinType::RightSemi, &rt)
            })
        });
    }

    // RightSemi - 50% Density, 100% hit rate
    // Keys: 0, 2, 4, ... (sparse, multiplier=2), all probe rows find a match
    {
        let left_batches = build_batches_sparse(build_rows, build_rows, 0, 2, &s);
        let right_batches = build_batches_sparse(probe_rows, build_rows, 0, 2, &s);
        group.bench_function(BenchmarkId::new("right_semi_d50_h100", probe_rows), |b| {
            b.iter(|| {
                let left = make_exec(&left_batches, &s);
                let right = make_exec(&right_batches, &s);
                do_hash_join(left, right, JoinType::RightSemi, &rt)
            })
        });
    }

    // RightSemi - 50% Density, 10% hit rate
    // Keys: 0, 2, 4, ... (sparse), only 10% of probe rows find a match
    {
        let left_batches = build_batches_sparse(build_rows, build_rows, 0, 2, &s);
        let right_batches = build_batches_sparse(probe_rows, build_rows * 10, 0, 2, &s);
        group.bench_function(BenchmarkId::new("right_semi_d50_h10", probe_rows), |b| {
            b.iter(|| {
                let left = make_exec(&left_batches, &s);
                let right = make_exec(&right_batches, &s);
                do_hash_join(left, right, JoinType::RightSemi, &rt)
            })
        });
    }

    // RightSemi - 10% Density, 100% hit rate
    // Keys: 0, 10, 20, ... (very sparse, multiplier=10), all probe rows find a match
    {
        let left_batches = build_batches_sparse(build_rows, build_rows, 0, 10, &s);
        let right_batches = build_batches_sparse(probe_rows, build_rows, 0, 10, &s);
        group.bench_function(BenchmarkId::new("right_semi_d10_h100", probe_rows), |b| {
            b.iter(|| {
                let left = make_exec(&left_batches, &s);
                let right = make_exec(&right_batches, &s);
                do_hash_join(left, right, JoinType::RightSemi, &rt)
            })
        });
    }

    // RightSemi - 10% Density, 10% hit rate
    // Keys: 0, 10, 20, ... (very sparse), only 10% of probe rows find a match
    {
        let left_batches = build_batches_sparse(build_rows, build_rows, 0, 10, &s);
        let right_batches = build_batches_sparse(probe_rows, build_rows * 10, 0, 10, &s);
        group.bench_function(BenchmarkId::new("right_semi_d10_h10", probe_rows), |b| {
            b.iter(|| {
                let left = make_exec(&left_batches, &s);
                let right = make_exec(&right_batches, &s);
                do_hash_join(left, right, JoinType::RightSemi, &rt)
            })
        });
    }

    // RightSemi - 100% Density, ~1% hit rate, fanout ~100
    // Build keys are duplicated: 100K rows over 1K distinct keys. Matching
    // probe rows produce many duplicate probe indices before RightSemi
    // deduplication.
    {
        let fanout_keys = 1_000;
        let left_batches = build_batches(build_rows, fanout_keys, 0, &s);
        let right_batches = build_batches(probe_rows, build_rows, 0, &s);
        group.bench_function(
            BenchmarkId::new("right_semi_fanout100_h1", probe_rows),
            |b| {
                b.iter(|| {
                    let left = make_exec(&left_batches, &s);
                    let right = make_exec(&right_batches, &s);
                    do_hash_join(left, right, JoinType::RightSemi, &rt)
                })
            },
        );
    }

    // =========================================================================
    // RightAnti Join benchmarks
    // =========================================================================

    // RightAnti - 100% Density, 100% hit rate (no output)
    // Keys: 0..100K contiguous, all probe rows find a match -> no output
    {
        let left_batches = build_batches(build_rows, build_rows, 0, &s);
        let right_batches = build_batches(probe_rows, build_rows, 0, &s);
        group.bench_function(BenchmarkId::new("right_anti_d100_h100", probe_rows), |b| {
            b.iter(|| {
                let left = make_exec(&left_batches, &s);
                let right = make_exec(&right_batches, &s);
                do_hash_join(left, right, JoinType::RightAnti, &rt)
            })
        });
    }

    // RightAnti - 100% Density, 10% hit rate (90% output)
    // Keys: 0..100K contiguous, only 10% of probe rows find a match -> 90% output
    {
        let left_batches = build_batches(build_rows, build_rows, 0, &s);
        let right_batches = build_batches(probe_rows, build_rows * 10, 0, &s);
        group.bench_function(BenchmarkId::new("right_anti_d100_h10", probe_rows), |b| {
            b.iter(|| {
                let left = make_exec(&left_batches, &s);
                let right = make_exec(&right_batches, &s);
                do_hash_join(left, right, JoinType::RightAnti, &rt)
            })
        });
    }

    // RightAnti - 50% Density, 100% hit rate (no output)
    // Keys: 0, 2, 4, ... (sparse), all probe rows find a match -> no output
    {
        let left_batches = build_batches_sparse(build_rows, build_rows, 0, 2, &s);
        let right_batches = build_batches_sparse(probe_rows, build_rows, 0, 2, &s);
        group.bench_function(BenchmarkId::new("right_anti_d50_h100", probe_rows), |b| {
            b.iter(|| {
                let left = make_exec(&left_batches, &s);
                let right = make_exec(&right_batches, &s);
                do_hash_join(left, right, JoinType::RightAnti, &rt)
            })
        });
    }

    // RightAnti - 50% Density, 10% hit rate (90% output)
    // Keys: 0, 2, 4, ... (sparse), only 10% of probe rows find a match -> 90% output
    {
        let left_batches = build_batches_sparse(build_rows, build_rows, 0, 2, &s);
        let right_batches = build_batches_sparse(probe_rows, build_rows * 10, 0, 2, &s);
        group.bench_function(BenchmarkId::new("right_anti_d50_h10", probe_rows), |b| {
            b.iter(|| {
                let left = make_exec(&left_batches, &s);
                let right = make_exec(&right_batches, &s);
                do_hash_join(left, right, JoinType::RightAnti, &rt)
            })
        });
    }

    // RightAnti - 10% Density, 100% hit rate (no output)
    // Keys: 0, 10, 20, ... (very sparse), all probe rows find a match -> no output
    {
        let left_batches = build_batches_sparse(build_rows, build_rows, 0, 10, &s);
        let right_batches = build_batches_sparse(probe_rows, build_rows, 0, 10, &s);
        group.bench_function(BenchmarkId::new("right_anti_d10_h100", probe_rows), |b| {
            b.iter(|| {
                let left = make_exec(&left_batches, &s);
                let right = make_exec(&right_batches, &s);
                do_hash_join(left, right, JoinType::RightAnti, &rt)
            })
        });
    }

    // RightAnti - 10% Density, 10% hit rate (90% output)
    // Keys: 0, 10, 20, ... (very sparse), only 10% of probe rows find a match -> 90% output
    {
        let left_batches = build_batches_sparse(build_rows, build_rows, 0, 10, &s);
        let right_batches = build_batches_sparse(probe_rows, build_rows * 10, 0, 10, &s);
        group.bench_function(BenchmarkId::new("right_anti_d10_h10", probe_rows), |b| {
            b.iter(|| {
                let left = make_exec(&left_batches, &s);
                let right = make_exec(&right_batches, &s);
                do_hash_join(left, right, JoinType::RightAnti, &rt)
            })
        });
    }

    group.finish();
}

criterion_group!(benches, bench_hash_join_semi_anti);
criterion_main!(benches);
