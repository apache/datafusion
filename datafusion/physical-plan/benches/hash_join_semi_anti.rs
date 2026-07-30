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

//! Criterion benchmarks for Hash Join with RightSemi/RightAnti joins with
//! Int32 and Utf8 keys.
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
//!
//! Each scenario below describes a build/probe data shape and is benchmarked
//! under both RightSemi and RightAnti.

use std::sync::Arc;

use arrow::array::{Int32Array, RecordBatch, StringArray};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use criterion::measurement::WallTime;
use criterion::{
    BenchmarkGroup, BenchmarkId, Criterion, criterion_group, criterion_main,
};
use datafusion_common::{JoinType, NullEquality};
use datafusion_execution::TaskContext;
use datafusion_physical_expr::expressions::col;
use datafusion_physical_plan::joins::{HashJoinExec, PartitionMode, utils::JoinOn};
use datafusion_physical_plan::test::TestMemoryExec;
use datafusion_physical_plan::{ExecutionPlan, collect};
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
    build_batches_from_key_fn(num_rows, schema, |i| ((i % key_mod) as i32) + key_offset)
}

fn make_exec(batches: &[RecordBatch], schema: &SchemaRef) -> Arc<dyn ExecutionPlan> {
    TestMemoryExec::try_new_exec(&[batches.to_vec()], Arc::clone(schema), None).unwrap()
}

fn schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("key", DataType::Int32, false),
        Field::new("data", DataType::Int32, false),
        Field::new("payload", DataType::Utf8, false),
    ]))
}

fn utf8_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("key", DataType::Utf8, false),
        Field::new("data", DataType::Int32, false),
        Field::new("payload", DataType::Utf8, false),
    ]))
}

fn do_hash_join(
    left: Arc<dyn ExecutionPlan>,
    right: Arc<dyn ExecutionPlan>,
    join_type: JoinType,
    rt: &Runtime,
) -> HashJoinRun {
    let on: JoinOn = vec![(
        col("key", &left.schema()).unwrap(),
        col("key", &right.schema()).unwrap(),
    )];
    let join = Arc::new(
        HashJoinExec::try_new(
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
        .unwrap(),
    );

    let task_ctx = Arc::new(TaskContext::default());
    let output_rows = rt.block_on(async {
        let batches = collect(join.clone(), task_ctx).await.unwrap();
        batches.iter().map(|b| b.num_rows()).sum::<usize>()
    });
    let metrics = join.metrics().unwrap();
    let array_map_created_count = metrics
        .sum_by_name(HashJoinExec::ARRAY_MAP_CREATED_COUNT_METRIC_NAME)
        .map(|v| v.as_usize())
        .unwrap_or(0);

    HashJoinRun {
        output_rows,
        array_map_created_count,
    }
}

struct HashJoinRun {
    output_rows: usize,
    array_map_created_count: usize,
}

/// A build/probe data shape benchmarked under both RightSemi and RightAnti.
struct BenchScenario {
    /// Name suffix shared by the `right_semi_*` / `right_anti_*` variants.
    suffix: &'static str,
    probe_rows: usize,
    left_batches: Vec<RecordBatch>,
    right_batches: Vec<RecordBatch>,
    /// Expected RightSemi output row count. The expected RightAnti output is
    /// the complement (`probe_rows - semi_output_rows`), since every probe
    /// row is emitted by exactly one of the two join types.
    semi_output_rows: usize,
    /// Whether the build side is expected to select the ArrayMap strategy.
    uses_array_map: bool,
}

fn bench_scenario(
    group: &mut BenchmarkGroup<'_, WallTime>,
    schema: &SchemaRef,
    rt: &Runtime,
    scenario: &BenchScenario,
) {
    for join_type in [JoinType::RightSemi, JoinType::RightAnti] {
        let (prefix, expected_output_rows) = match join_type {
            JoinType::RightSemi => ("right_semi", scenario.semi_output_rows),
            _ => (
                "right_anti",
                scenario.probe_rows - scenario.semi_output_rows,
            ),
        };
        let name = format!("{prefix}_{}", scenario.suffix);

        // Validate the output row count and map strategy once before measuring.
        let run = do_hash_join(
            make_exec(&scenario.left_batches, schema),
            make_exec(&scenario.right_batches, schema),
            join_type,
            rt,
        );
        assert_eq!(
            run.output_rows, expected_output_rows,
            "unexpected output row count for {name}"
        );
        assert_eq!(
            run.array_map_created_count > 0,
            scenario.uses_array_map,
            "unexpected map strategy for {name}"
        );

        group.bench_function(BenchmarkId::new(name, scenario.probe_rows), |b| {
            b.iter(|| {
                let left = make_exec(&scenario.left_batches, schema);
                let right = make_exec(&scenario.right_batches, schema);
                do_hash_join(left, right, join_type, rt).output_rows
            })
        });
    }
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
    build_batches_from_key_fn(num_rows, schema, |i| {
        ((i % key_mod) as i32) * multiplier + key_offset
    })
}

/// Build batches from an arbitrary deterministic key generator.
fn build_batches_from_key_fn<F>(
    num_rows: usize,
    schema: &SchemaRef,
    mut key_fn: F,
) -> Vec<RecordBatch>
where
    F: FnMut(usize) -> i32,
{
    let keys: Vec<i32> = (0..num_rows).map(&mut key_fn).collect();
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

    slice_batches(&batch)
}

/// Build batches with Utf8 keys: `key = "key_<i % key_mod>"`, zero-padded so
/// every key has the same length.
fn build_utf8_batches(
    num_rows: usize,
    key_mod: usize,
    schema: &SchemaRef,
) -> Vec<RecordBatch> {
    let keys: Vec<String> = (0..num_rows)
        .map(|i| format!("key_{:08}", i % key_mod))
        .collect();
    let data: Vec<i32> = (0..num_rows).map(|i| i as i32).collect();
    let payload: Vec<String> = data.iter().map(|d| format!("val_{d}")).collect();

    let batch = RecordBatch::try_new(
        Arc::clone(schema),
        vec![
            Arc::new(StringArray::from(keys)),
            Arc::new(Int32Array::from(data)),
            Arc::new(StringArray::from(payload)),
        ],
    )
    .unwrap();

    slice_batches(&batch)
}

/// Split `batch` into 8192-row slices.
fn slice_batches(batch: &RecordBatch) -> Vec<RecordBatch> {
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

/// Skewed 50%-hit key generator for a sparse (HashMap) build side whose keys are
/// spaced by 10. Matching keys land on multiples of 10; misses are offset by 1.
fn skewed_h50_hashmap_key(row: usize, build_rows: usize, hot_keys: usize) -> i32 {
    let logical_row = row / 2;
    if row.is_multiple_of(2) {
        let key = if logical_row % 10 < 8 {
            logical_row % hot_keys
        } else {
            hot_keys + logical_row % (build_rows - hot_keys)
        };
        (key as i32) * 10
    } else {
        ((logical_row % build_rows) as i32) * 10 + 1
    }
}

fn bench_hash_join_semi_anti(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();
    let s = schema();

    let mut group = c.benchmark_group("hash_join_semi_anti");

    // Build side: 100K rows, Probe side: 1M rows
    // Matching ratio: 1:1 (build keys are unique, each probe matches at most 1 build row)
    let build_rows = 100_000;
    let probe_rows = 1_000_000;

    // 100% Density, 100% hit rate
    // Keys: 0..100K contiguous, all probe rows find a match
    bench_scenario(
        &mut group,
        &s,
        &rt,
        &BenchScenario {
            suffix: "d100_h100",
            probe_rows,
            left_batches: build_batches(build_rows, build_rows, 0, &s),
            right_batches: build_batches(probe_rows, build_rows, 0, &s),
            semi_output_rows: probe_rows,
            uses_array_map: true,
        },
    );

    // 100% Density, 10% hit rate
    // Keys: 0..100K contiguous, only 10% of probe rows find a match
    bench_scenario(
        &mut group,
        &s,
        &rt,
        &BenchScenario {
            suffix: "d100_h10",
            probe_rows,
            left_batches: build_batches(build_rows, build_rows, 0, &s),
            right_batches: build_batches(probe_rows, build_rows * 10, 0, &s),
            semi_output_rows: build_rows,
            uses_array_map: true,
        },
    );

    // 100% Density, 50% hit rate
    // Keys: 0..100K contiguous, half of probe rows find a match.
    bench_scenario(
        &mut group,
        &s,
        &rt,
        &BenchScenario {
            suffix: "d100_h50",
            probe_rows,
            left_batches: build_batches(build_rows, build_rows, 0, &s),
            right_batches: build_batches(probe_rows, build_rows * 2, 0, &s),
            semi_output_rows: probe_rows / 2,
            uses_array_map: true,
        },
    );

    // 50% Density, 100% hit rate
    // Keys: 0, 2, 4, ... (sparse, multiplier=2), all probe rows find a match
    bench_scenario(
        &mut group,
        &s,
        &rt,
        &BenchScenario {
            suffix: "d50_h100",
            probe_rows,
            left_batches: build_batches_sparse(build_rows, build_rows, 0, 2, &s),
            right_batches: build_batches_sparse(probe_rows, build_rows, 0, 2, &s),
            semi_output_rows: probe_rows,
            uses_array_map: true,
        },
    );

    // 50% Density, 10% hit rate
    // Keys: 0, 2, 4, ... (sparse), only 10% of probe rows find a match
    bench_scenario(
        &mut group,
        &s,
        &rt,
        &BenchScenario {
            suffix: "d50_h10",
            probe_rows,
            left_batches: build_batches_sparse(build_rows, build_rows, 0, 2, &s),
            right_batches: build_batches_sparse(probe_rows, build_rows * 10, 0, 2, &s),
            semi_output_rows: build_rows,
            uses_array_map: true,
        },
    );

    // 10% Density, 100% hit rate
    // Keys: 0, 10, 20, ... (very sparse, multiplier=10), all probe rows find a match
    bench_scenario(
        &mut group,
        &s,
        &rt,
        &BenchScenario {
            suffix: "d10_h100",
            probe_rows,
            left_batches: build_batches_sparse(build_rows, build_rows, 0, 10, &s),
            right_batches: build_batches_sparse(probe_rows, build_rows, 0, 10, &s),
            semi_output_rows: probe_rows,
            uses_array_map: false,
        },
    );

    // 10% Density, 10% hit rate
    // Keys: 0, 10, 20, ... (very sparse), only 10% of probe rows find a match
    bench_scenario(
        &mut group,
        &s,
        &rt,
        &BenchScenario {
            suffix: "d10_h10",
            probe_rows,
            left_batches: build_batches_sparse(build_rows, build_rows, 0, 10, &s),
            right_batches: build_batches_sparse(probe_rows, build_rows * 10, 0, 10, &s),
            semi_output_rows: build_rows,
            uses_array_map: false,
        },
    );

    // 10% Density, 50% hit rate
    // Sparse build keys force the HashMap path while avoiding all-match/no-match extremes.
    bench_scenario(
        &mut group,
        &s,
        &rt,
        &BenchScenario {
            suffix: "d10_h50",
            probe_rows,
            left_batches: build_batches_sparse(build_rows, build_rows, 0, 10, &s),
            right_batches: build_batches_sparse(probe_rows, build_rows * 2, 0, 10, &s),
            semi_output_rows: probe_rows / 2,
            uses_array_map: false,
        },
    );

    // 100% Density, ~1% hit rate, fanout ~100
    // Build keys are duplicated: 100K rows over 1K distinct keys, so each
    // matching key has fanout ~100.
    {
        let fanout_keys = 1_000;
        bench_scenario(
            &mut group,
            &s,
            &rt,
            &BenchScenario {
                suffix: "fanout100_h1",
                probe_rows,
                left_batches: build_batches(build_rows, fanout_keys, 0, &s),
                right_batches: build_batches(probe_rows, build_rows, 0, &s),
                semi_output_rows: probe_rows / 100,
                uses_array_map: true,
            },
        );
    }

    // 100% Density, 50% hit rate, fanout ~10
    // This is a moderate duplicate-heavy case: enough fanout to stress pair
    // materialization, without relying on a tiny match rate.
    {
        let fanout_keys = 10_000;
        bench_scenario(
            &mut group,
            &s,
            &rt,
            &BenchScenario {
                suffix: "fanout10_h50",
                probe_rows,
                left_batches: build_batches(build_rows, fanout_keys, 0, &s),
                right_batches: build_batches(probe_rows, fanout_keys * 2, 0, &s),
                semi_output_rows: probe_rows / 2,
                uses_array_map: true,
            },
        );
    }

    // HashMap path, 50% hit rate, fanout ~10
    // A large sparse key range prevents ArrayMap selection while preserving the
    // same moderate fanout and hit-rate shape.
    {
        let fanout_keys = 10_000;
        bench_scenario(
            &mut group,
            &s,
            &rt,
            &BenchScenario {
                suffix: "fanout10_h50_hashmap",
                probe_rows,
                left_batches: build_batches_sparse(build_rows, fanout_keys, 0, 1000, &s),
                right_batches: build_batches_sparse(
                    probe_rows,
                    fanout_keys * 2,
                    0,
                    1000,
                    &s,
                ),
                semi_output_rows: probe_rows / 2,
                uses_array_map: false,
            },
        );
    }

    // HashMap path, skewed 50% hit rate
    {
        let hot_keys = 1_000;
        bench_scenario(
            &mut group,
            &s,
            &rt,
            &BenchScenario {
                suffix: "skewed_h50_hashmap",
                probe_rows,
                left_batches: build_batches_sparse(build_rows, build_rows, 0, 10, &s),
                right_batches: build_batches_from_key_fn(probe_rows, &s, |row| {
                    skewed_h50_hashmap_key(row, build_rows, hot_keys)
                }),
                semi_output_rows: probe_rows / 2,
                uses_array_map: false,
            },
        );
    }

    // Utf8 keys, unique build keys, 50% hit rate
    // String keys always take the HashMap path (ArrayMap supports only
    // integer keys), and key equality is substantially more expensive than
    // for Int32.
    {
        let utf8_s = utf8_schema();
        bench_scenario(
            &mut group,
            &utf8_s,
            &rt,
            &BenchScenario {
                suffix: "utf8_h50",
                probe_rows,
                left_batches: build_utf8_batches(build_rows, build_rows, &utf8_s),
                right_batches: build_utf8_batches(probe_rows, build_rows * 2, &utf8_s),
                semi_output_rows: probe_rows / 2,
                uses_array_map: false,
            },
        );

        // Utf8 keys, fanout ~10, 50% hit rate
        // Duplicate build keys exercise the hash-chain walk with expensive
        // key comparisons.
        let fanout_keys = 10_000;
        bench_scenario(
            &mut group,
            &utf8_s,
            &rt,
            &BenchScenario {
                suffix: "utf8_fanout10_h50",
                probe_rows,
                left_batches: build_utf8_batches(build_rows, fanout_keys, &utf8_s),
                right_batches: build_utf8_batches(probe_rows, fanout_keys * 2, &utf8_s),
                semi_output_rows: probe_rows / 2,
                uses_array_map: false,
            },
        );
    }

    group.finish();
}

criterion_group!(benches, bench_hash_join_semi_anti);
criterion_main!(benches);
