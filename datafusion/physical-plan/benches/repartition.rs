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

use std::hint::black_box;
use std::sync::Arc;

use arrow::array::{ArrayRef, Int32Array, Int64Array, RecordBatch, StringViewArray};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use criterion::{
    BatchSize, BenchmarkId, Criterion, Throughput, criterion_group, criterion_main,
};
use datafusion_execution::TaskContext;
use datafusion_physical_expr::PhysicalExpr;
use datafusion_physical_expr::expressions::col;
use datafusion_physical_plan::metrics::Time;
use datafusion_physical_plan::repartition::{BatchPartitioner, RepartitionExec};
use datafusion_physical_plan::test::TestMemoryExec;
use datafusion_physical_plan::{Partitioning, collect};
use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};
use tokio::runtime::Runtime;

const BATCH_SIZE: usize = 8192;
const SEED: u64 = 42;

// Intentionally mixes powers-of-two (8, 16, 32, 64, 128) and non-powers-of-two
// (10, 100) so both StrengthReducedU64 branches (bit-mask and reciprocal) are
// exercised in every benchmark run.
const PARTITION_COUNTS: &[usize] = &[8, 10, 16, 32, 64, 100, 128];

fn make_i64_batch(schema: &SchemaRef, num_rows: usize) -> RecordBatch {
    let mut rng = StdRng::seed_from_u64(SEED);
    let keys: Vec<i64> = (0..num_rows)
        .map(|_| rng.random_range(0..1_000_000i64))
        .collect();
    let vals: Vec<i64> = (0..num_rows as i64).collect();
    RecordBatch::try_new(
        Arc::clone(schema),
        vec![
            Arc::new(Int64Array::from(keys)) as ArrayRef,
            Arc::new(Int64Array::from(vals)),
        ],
    )
    .unwrap()
}

fn make_i32_batch(schema: &SchemaRef, num_rows: usize) -> RecordBatch {
    let mut rng = StdRng::seed_from_u64(SEED);
    let keys: Vec<i32> = (0..num_rows)
        .map(|_| rng.random_range(0..1_000_000i32))
        .collect();
    let vals: Vec<i64> = (0..num_rows as i64).collect();
    RecordBatch::try_new(
        Arc::clone(schema),
        vec![
            Arc::new(Int32Array::from(keys)) as ArrayRef,
            Arc::new(Int64Array::from(vals)),
        ],
    )
    .unwrap()
}

fn make_utf8view_batch(schema: &SchemaRef, num_rows: usize) -> RecordBatch {
    let mut rng = StdRng::seed_from_u64(SEED);
    let keys: Vec<String> = (0..num_rows)
        .map(|_| format!("key_{:08}", rng.random_range(0..1_000_000usize)))
        .collect();
    let vals: Vec<i64> = (0..num_rows as i64).collect();
    RecordBatch::try_new(
        Arc::clone(schema),
        vec![
            Arc::new(StringViewArray::from_iter_values(
                keys.iter().map(String::as_str),
            )) as ArrayRef,
            Arc::new(Int64Array::from(vals)),
        ],
    )
    .unwrap()
}

fn make_multi_key_batch(
    schema: &SchemaRef,
    num_keys: usize,
    num_rows: usize,
) -> RecordBatch {
    let mut rng = StdRng::seed_from_u64(SEED);
    let mut columns: Vec<ArrayRef> = (0..num_keys)
        .map(|_| {
            let v: Vec<i64> = (0..num_rows)
                .map(|_| rng.random_range(0..1_000_000i64))
                .collect();
            Arc::new(Int64Array::from(v)) as ArrayRef
        })
        .collect();
    let vals: Vec<i64> = (0..num_rows as i64).collect();
    columns.push(Arc::new(Int64Array::from(vals)) as ArrayRef);
    RecordBatch::try_new(Arc::clone(schema), columns).unwrap()
}

/// Build `num_partitions` input partition slots, each holding `rows_per_partition / batch_size`
/// batches of random Int64 data.
fn make_partitioned_input(
    num_partitions: usize,
    rows_per_partition: usize,
) -> (Vec<Vec<RecordBatch>>, SchemaRef) {
    let schema = Arc::new(Schema::new(vec![
        Field::new("key", DataType::Int64, false),
        Field::new("val", DataType::Int64, false),
    ]));
    let mut rng = StdRng::seed_from_u64(SEED);
    let num_batches = (rows_per_partition + BATCH_SIZE - 1) / BATCH_SIZE;
    let partitions = (0..num_partitions)
        .map(|_| {
            (0..num_batches)
                .map(|_| {
                    let keys: Vec<i64> = (0..BATCH_SIZE)
                        .map(|_| rng.random_range(0..1_000_000i64))
                        .collect();
                    let vals: Vec<i64> = (0..BATCH_SIZE as i64).collect();
                    RecordBatch::try_new(
                        Arc::clone(&schema),
                        vec![
                            Arc::new(Int64Array::from(keys)) as ArrayRef,
                            Arc::new(Int64Array::from(vals)),
                        ],
                    )
                    .unwrap()
                })
                .collect()
        })
        .collect();
    (partitions, schema)
}

/// Hash routing at varying partition counts.
///
/// Power-of-two counts (8, 16, 32 …) use a bitmask inside `StrengthReducedU64`;
/// non-powers (10, 100 …) use a reciprocal multiply. Both paths are included so
/// the difference in routing cost is visible.
fn bench_hash_partitioner_partition_count(c: &mut Criterion) {
    let mut group = c.benchmark_group("hash_partitioner/partition_count");
    group.throughput(Throughput::Elements(BATCH_SIZE as u64));

    let schema = Arc::new(Schema::new(vec![
        Field::new("key", DataType::Int64, false),
        Field::new("val", DataType::Int64, false),
    ]));
    let batch = make_i64_batch(&schema, BATCH_SIZE);
    let key_expr = col("key", &schema).unwrap();

    for &n in PARTITION_COUNTS {
        group.bench_with_input(BenchmarkId::from_parameter(n), &n, |b, &n| {
            let mut partitioner = BatchPartitioner::new_hash_partitioner(
                vec![Arc::clone(&key_expr)],
                n,
                Time::default(),
            )
            .unwrap();
            b.iter(|| {
                partitioner
                    .partition(batch.clone(), |p, b| {
                        black_box((p, b));
                        Ok(())
                    })
                    .unwrap();
            });
        });
    }
    group.finish();
}

/// Hash routing cost per key column type at a fixed 32 output partitions.
fn bench_hash_partitioner_key_types(c: &mut Criterion) {
    let mut group = c.benchmark_group("hash_partitioner/key_type");
    group.throughput(Throughput::Elements(BATCH_SIZE as u64));
    const N: usize = 32;

    // Int32
    {
        let schema = Arc::new(Schema::new(vec![
            Field::new("key", DataType::Int32, false),
            Field::new("val", DataType::Int64, false),
        ]));
        let batch = make_i32_batch(&schema, BATCH_SIZE);
        let key_expr = col("key", &schema).unwrap();
        group.bench_function("int32", |b| {
            let mut p = BatchPartitioner::new_hash_partitioner(
                vec![Arc::clone(&key_expr)],
                N,
                Time::default(),
            )
            .unwrap();
            b.iter(|| {
                p.partition(batch.clone(), |p, b| {
                    black_box((p, b));
                    Ok(())
                })
                .unwrap();
            });
        });
    }

    // Int64
    {
        let schema = Arc::new(Schema::new(vec![
            Field::new("key", DataType::Int64, false),
            Field::new("val", DataType::Int64, false),
        ]));
        let batch = make_i64_batch(&schema, BATCH_SIZE);
        let key_expr = col("key", &schema).unwrap();
        group.bench_function("int64", |b| {
            let mut p = BatchPartitioner::new_hash_partitioner(
                vec![Arc::clone(&key_expr)],
                N,
                Time::default(),
            )
            .unwrap();
            b.iter(|| {
                p.partition(batch.clone(), |p, b| {
                    black_box((p, b));
                    Ok(())
                })
                .unwrap();
            });
        });
    }

    // Utf8View
    {
        let schema = Arc::new(Schema::new(vec![
            Field::new("key", DataType::Utf8View, false),
            Field::new("val", DataType::Int64, false),
        ]));
        let batch = make_utf8view_batch(&schema, BATCH_SIZE);
        let key_expr = col("key", &schema).unwrap();
        group.bench_function("utf8", |b| {
            let mut p = BatchPartitioner::new_hash_partitioner(
                vec![Arc::clone(&key_expr)],
                N,
                Time::default(),
            )
            .unwrap();
            b.iter(|| {
                p.partition(batch.clone(), |p, b| {
                    black_box((p, b));
                    Ok(())
                })
                .unwrap();
            });
        });
    }

    group.finish();
}

/// Composite hash cost for 1, 2, and 3 key columns.
///
/// Each extra key column adds an independent hash pass over all rows, so this
/// shows how multi-column GROUP BY / JOIN keys scale the routing cost.
fn bench_hash_partitioner_key_count(c: &mut Criterion) {
    let mut group = c.benchmark_group("hash_partitioner/key_count");
    group.throughput(Throughput::Elements(BATCH_SIZE as u64));
    const N: usize = 32;

    for num_keys in [1usize, 2, 3] {
        let fields: Vec<Field> = (0..num_keys)
            .map(|i| Field::new(format!("k{i}"), DataType::Int64, false))
            .chain(std::iter::once(Field::new("val", DataType::Int64, false)))
            .collect();
        let schema = Arc::new(Schema::new(fields));
        let batch = make_multi_key_batch(&schema, num_keys, BATCH_SIZE);
        let key_exprs: Vec<Arc<dyn PhysicalExpr>> = (0..num_keys)
            .map(|i| col(&format!("k{i}"), &schema).unwrap())
            .collect();

        group.bench_with_input(
            BenchmarkId::from_parameter(num_keys),
            &num_keys,
            |b, _| {
                let mut p = BatchPartitioner::new_hash_partitioner(
                    key_exprs.clone(),
                    N,
                    Time::default(),
                )
                .unwrap();
                b.iter(|| {
                    p.partition(batch.clone(), |p, b| {
                        black_box((p, b));
                        Ok(())
                    })
                    .unwrap();
                });
            },
        );
    }
    group.finish();
}

/// Round-robin routing at varying partition counts.
///
/// Round-robin does no hashing, it simply increments a counter. This is the
/// baseline for the `partition()` interface overhead. Compare against
/// `hash_partitioner/partition_count` to isolate pure hashing cost.
fn bench_round_robin_partitioner(c: &mut Criterion) {
    let mut group = c.benchmark_group("round_robin_partitioner/partition_count");
    group.throughput(Throughput::Elements(BATCH_SIZE as u64));

    let schema = Arc::new(Schema::new(vec![
        Field::new("key", DataType::Int64, false),
        Field::new("val", DataType::Int64, false),
    ]));
    let batch = make_i64_batch(&schema, BATCH_SIZE);

    for &n in PARTITION_COUNTS {
        group.bench_with_input(BenchmarkId::from_parameter(n), &n, |b, &n| {
            let mut partitioner =
                BatchPartitioner::new_round_robin_partitioner(n, Time::default(), 0, 1);
            b.iter(|| {
                partitioner
                    .partition(batch.clone(), |p, b| {
                        black_box((p, b));
                        Ok(())
                    })
                    .unwrap();
            });
        });
    }
    group.finish();
}

const E2E_ROWS: usize = 5_000_000;

/// Hash repartition: 1 input partition → N output partitions.
///
/// Measures the full operator path: batch partitioning, channel sends,
/// coalescing, memory reservation, and downstream collection.
fn bench_repartition_exec_hash_1_to_n(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();
    let task_ctx = Arc::new(TaskContext::default());
    let mut group = c.benchmark_group("repartition_exec/hash_1_to_n");
    group.throughput(Throughput::Elements(E2E_ROWS as u64));

    let (partitions, schema) = make_partitioned_input(1, E2E_ROWS);
    let key_expr = col("key", &schema).unwrap();

    for &n in &[4usize, 8, 16, 32] {
        group.bench_with_input(BenchmarkId::from_parameter(n), &n, |b, &n| {
            b.iter_batched(
                || {
                    let input =
                        TestMemoryExec::try_new_exec(&partitions, schema.clone(), None)
                            .unwrap();
                    Arc::new(
                        RepartitionExec::try_new(
                            input,
                            Partitioning::Hash(vec![Arc::clone(&key_expr)], n),
                        )
                        .unwrap(),
                    )
                },
                |plan| {
                    rt.block_on(async {
                        collect(plan, task_ctx.clone()).await.unwrap();
                    });
                },
                BatchSize::LargeInput,
            );
        });
    }
    group.finish();
}

/// Round-robin repartition: 1 input partition → N output partitions.
///
/// Compare against `hash_1_to_n` to isolate the cost of hash computation
/// versus channel/coalescing overhead that both modes share.
fn bench_repartition_exec_round_robin_1_to_n(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();
    let task_ctx = Arc::new(TaskContext::default());
    let mut group = c.benchmark_group("repartition_exec/round_robin_1_to_n");
    group.throughput(Throughput::Elements(E2E_ROWS as u64));

    let (partitions, schema) = make_partitioned_input(1, E2E_ROWS);

    for &n in &[4usize, 8, 16, 32] {
        group.bench_with_input(BenchmarkId::from_parameter(n), &n, |b, &n| {
            b.iter_batched(
                || {
                    let input =
                        TestMemoryExec::try_new_exec(&partitions, schema.clone(), None)
                            .unwrap();
                    Arc::new(
                        RepartitionExec::try_new(input, Partitioning::RoundRobinBatch(n))
                            .unwrap(),
                    )
                },
                |plan| {
                    rt.block_on(async {
                        collect(plan, task_ctx.clone()).await.unwrap();
                    });
                },
                BatchSize::LargeInput,
            );
        });
    }
    group.finish();
}

/// Hash repartition: N input partitions → N output partitions.
///
/// Models the typical distributed query scenario where N concurrent producer
/// tasks each hash-route their share of rows to N output channels. The
/// contention on shared channels and coalescer locks shows up here but not
/// in the 1→N benchmarks.
fn bench_repartition_exec_hash_n_to_n(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();
    let task_ctx = Arc::new(TaskContext::default());
    let mut group = c.benchmark_group("repartition_exec/hash_n_to_n");
    group.throughput(Throughput::Elements(E2E_ROWS as u64));

    for &n in &[4usize, 8, 16] {
        let rows_per_partition = E2E_ROWS / n;
        let (partitions, schema) = make_partitioned_input(n, rows_per_partition);
        let key_expr = col("key", &schema).unwrap();

        group.bench_with_input(BenchmarkId::from_parameter(n), &n, |b, &n| {
            b.iter_batched(
                || {
                    let input =
                        TestMemoryExec::try_new_exec(&partitions, schema.clone(), None)
                            .unwrap();
                    Arc::new(
                        RepartitionExec::try_new(
                            input,
                            Partitioning::Hash(vec![Arc::clone(&key_expr)], n),
                        )
                        .unwrap(),
                    )
                },
                |plan| {
                    rt.block_on(async {
                        collect(plan, task_ctx.clone()).await.unwrap();
                    });
                },
                BatchSize::LargeInput,
            );
        });
    }
    group.finish();
}

/// Hash repartition across asymmetric M→N topologies.
///
/// Covers two directions:
/// - M > N (consolidation): many input partitions funnel into fewer outputs.
///   More producers contend on fewer channels.
/// - M < N (fan-out): few input partitions scatter to many outputs.
///   Each producer writes to more channels per batch.
fn bench_repartition_exec_hash_m_to_n(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();
    let task_ctx = Arc::new(TaskContext::default());
    let mut group = c.benchmark_group("repartition_exec/hash_m_to_n");
    group.throughput(Throughput::Elements(E2E_ROWS as u64));

    // (input_partitions, output_partitions)
    let cases: &[(usize, usize)] = &[
        // M > N: consolidation
        (16, 4),
        (16, 8),
        (32, 8),
        // M < N: fan-out
        (4, 16),
        (8, 16),
        (8, 32),
    ];

    for &(m, n) in cases {
        let rows_per_partition = E2E_ROWS / m;
        let (partitions, schema) = make_partitioned_input(m, rows_per_partition);
        let key_expr = col("key", &schema).unwrap();

        group.bench_with_input(
            BenchmarkId::new("m_to_n", format!("{m}_{n}")),
            &(m, n),
            |b, &(_, n)| {
                b.iter_batched(
                    || {
                        let input = TestMemoryExec::try_new_exec(
                            &partitions,
                            schema.clone(),
                            None,
                        )
                        .unwrap();
                        Arc::new(
                            RepartitionExec::try_new(
                                input,
                                Partitioning::Hash(vec![Arc::clone(&key_expr)], n),
                            )
                            .unwrap(),
                        )
                    },
                    |plan| {
                        rt.block_on(async {
                            collect(plan, task_ctx.clone()).await.unwrap();
                        });
                    },
                    BatchSize::LargeInput,
                );
            },
        );
    }
    group.finish();
}

criterion_group!(
    benches,
    bench_hash_partitioner_partition_count,
    bench_hash_partitioner_key_types,
    bench_hash_partitioner_key_count,
    bench_round_robin_partitioner,
    bench_repartition_exec_hash_1_to_n,
    bench_repartition_exec_round_robin_1_to_n,
    bench_repartition_exec_hash_n_to_n,
    bench_repartition_exec_hash_m_to_n,
);
criterion_main!(benches);
