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

use std::collections::HashMap;
use std::hint::black_box;
use std::sync::Arc;

use arrow::array::{
    ArrayRef, BinaryArray, FixedSizeBinaryArray, Int32Array, Int64Array, ListArray,
    RecordBatch, StringArray, StringDictionaryBuilder, StringViewArray, UInt64Array,
};
use arrow::buffer::OffsetBuffer;
use arrow::datatypes::{DataType, Field, Int32Type, Schema, SchemaRef};
use criterion::{
    BatchSize, BenchmarkId, Criterion, Throughput, criterion_group, criterion_main,
};
use datafusion_execution::TaskContext;
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
const DICT_CARDINALITY: usize = 100;

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

/// Build `num_partitions` input partition slots, each holding `rows_per_partition / batch_size`
/// batches of mixed-type data: an Int64 key, an Int64 value, and a Utf8View tag column.
fn make_partitioned_input(
    num_partitions: usize,
    rows_per_partition: usize,
) -> (Vec<Vec<RecordBatch>>, SchemaRef) {
    let list_item_field = Arc::new(Field::new("item", DataType::Utf8, true));
    let schema = Arc::new(Schema::new(vec![
        Field::new("key", DataType::Int64, false),
        Field::new("val", DataType::UInt64, false),
        Field::new("tag", DataType::Utf8View, false),
        Field::new("label", DataType::Utf8, false),
        Field::new("blob", DataType::Binary, false),
        Field::new("uuid", DataType::FixedSizeBinary(16), false),
        Field::new(
            "tags_list",
            DataType::List(Arc::clone(&list_item_field)),
            false,
        ),
        Field::new(
            "category",
            DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::Utf8)),
            false,
        ),
    ]));
    let mut rng = StdRng::seed_from_u64(SEED);
    let num_batches = rows_per_partition.div_ceil(BATCH_SIZE);

    let tag_pool: Vec<String> = (0..1_000).map(|i| format!("tag_{i:08}")).collect();
    let label_pool: Vec<String> = (0..DICT_CARDINALITY)
        .map(|i| format!("lbl_{i:06}"))
        .collect();
    let item_pool: Vec<String> = (0..1_000).map(|i| format!("item_{i:06}")).collect();
    let cat_pool: Vec<String> = (0..DICT_CARDINALITY)
        .map(|i| format!("cat_{i:03}"))
        .collect();

    let partitions = (0..num_partitions)
        .map(|_| {
            (0..num_batches)
                .map(|_| {
                    let keys: Vec<i64> = (0..BATCH_SIZE)
                        .map(|_| rng.random_range(0..1_000_000i64))
                        .collect();
                    let vals: Vec<u64> = (0..BATCH_SIZE as u64).collect();
                    let tags: Vec<&str> = (0..BATCH_SIZE)
                        .map(|_| tag_pool[rng.random_range(0..tag_pool.len())].as_str())
                        .collect();
                    let labels: Vec<&str> = (0..BATCH_SIZE)
                        .map(|_| {
                            label_pool[rng.random_range(0..label_pool.len())].as_str()
                        })
                        .collect();
                    let blobs: Vec<Vec<u8>> = (0..BATCH_SIZE)
                        .map(|_| {
                            let len = rng.random_range(8..=64usize);
                            (0..len).map(|_| rng.random_range(0..=255u8)).collect()
                        })
                        .collect();
                    let uuids: Vec<[u8; 16]> =
                        (0..BATCH_SIZE).map(|_| rng.random::<[u8; 16]>()).collect();
                    let uuid_array = FixedSizeBinaryArray::try_from_iter(
                        uuids.iter().map(|u| u.as_slice()),
                    )
                    .unwrap();

                    let mut offsets = Vec::with_capacity(BATCH_SIZE + 1);
                    offsets.push(0i32);
                    let mut list_values: Vec<&str> = Vec::new();
                    for _ in 0..BATCH_SIZE {
                        let count = rng.random_range(3..=8usize);
                        for _ in 0..count {
                            list_values.push(
                                item_pool[rng.random_range(0..item_pool.len())].as_str(),
                            );
                        }
                        offsets.push(list_values.len() as i32);
                    }
                    let list_array = ListArray::new(
                        Arc::clone(&list_item_field),
                        OffsetBuffer::new(offsets.into()),
                        Arc::new(StringArray::from(list_values)),
                        None,
                    );

                    let mut dict_builder = StringDictionaryBuilder::<Int32Type>::new();
                    for _ in 0..BATCH_SIZE {
                        dict_builder.append_value(
                            cat_pool[rng.random_range(0..cat_pool.len())].as_str(),
                        );
                    }
                    let dict_array = dict_builder.finish();

                    RecordBatch::try_new(
                        Arc::clone(&schema),
                        vec![
                            Arc::new(Int64Array::from(keys)) as ArrayRef,
                            Arc::new(UInt64Array::from(vals)),
                            Arc::new(StringViewArray::from_iter_values(tags)),
                            Arc::new(StringArray::from(labels)),
                            Arc::new(BinaryArray::from_iter_values(
                                blobs.iter().map(Vec::as_slice),
                            )),
                            Arc::new(uuid_array),
                            Arc::new(list_array),
                            Arc::new(dict_array),
                        ],
                    )
                    .unwrap()
                })
                .collect()
        })
        .collect();
    (partitions, schema)
}

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

    {
        let schema = Arc::new(Schema::new(vec![
            Field::new("key", DataType::Utf8View, false),
            Field::new("val", DataType::Int64, false),
        ]));
        let batch = make_utf8view_batch(&schema, BATCH_SIZE);
        let key_expr = col("key", &schema).unwrap();
        group.bench_function("utf8_view", |b| {
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

    {
        let mut rng = StdRng::seed_from_u64(SEED);
        let keys: Vec<String> = (0..BATCH_SIZE)
            .map(|_| format!("key_{:08}", rng.random_range(0..1_000_000usize)))
            .collect();
        let vals: Vec<i64> = (0..BATCH_SIZE as i64).collect();
        let schema = Arc::new(Schema::new(vec![
            Field::new("key", DataType::Utf8, false),
            Field::new("val", DataType::Int64, false),
        ]));
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(StringArray::from(keys)) as ArrayRef,
                Arc::new(Int64Array::from(vals)),
            ],
        )
        .unwrap();
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

    {
        let mut rng = StdRng::seed_from_u64(SEED);
        let blobs: Vec<Vec<u8>> = (0..BATCH_SIZE)
            .map(|_| {
                let len = rng.random_range(8..=64usize);
                (0..len).map(|_| rng.random_range(0..=255u8)).collect()
            })
            .collect();
        let vals: Vec<i64> = (0..BATCH_SIZE as i64).collect();
        let schema = Arc::new(Schema::new(vec![
            Field::new("key", DataType::Binary, false),
            Field::new("val", DataType::Int64, false),
        ]));
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(BinaryArray::from_iter_values(
                    blobs.iter().map(Vec::as_slice),
                )) as ArrayRef,
                Arc::new(Int64Array::from(vals)),
            ],
        )
        .unwrap();
        let key_expr = col("key", &schema).unwrap();
        group.bench_function("binary", |b| {
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

fn bench_repartition_exec(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();
    let task_ctx = Arc::new(TaskContext::default());

    const E2E_ROWS: usize = 10_000_000;

    // (label, input_partitions, output_partitions)
    let cases: &[(&str, usize, usize)] = &[
        ("1_to_n", 1, 4),
        ("1_to_n", 1, 8),
        ("1_to_n", 1, 16),
        ("1_to_n", 1, 32),
        ("n_to_n", 4, 4),
        ("n_to_n", 8, 8),
        ("n_to_n", 16, 16),
        ("consolidation", 16, 4),
        ("consolidation", 16, 8),
        ("consolidation", 32, 8),
        ("fan_out", 4, 16),
        ("fan_out", 8, 16),
        ("fan_out", 8, 32),
        ("high_fan_out", 8, 128),
        ("high_fan_out", 8, 256),
        ("high_fan_out", 32, 128),
        ("high_fan_out", 32, 256),
        ("high_fan_out", 32, 512),
    ];

    let mut prebuilt: HashMap<usize, (Vec<Vec<RecordBatch>>, SchemaRef)> = HashMap::new();
    for &(_, input_partitions, _) in cases {
        prebuilt.entry(input_partitions).or_insert_with(|| {
            make_partitioned_input(input_partitions, E2E_ROWS / input_partitions)
        });
    }

    let mut group = c.benchmark_group("repartition_exec");
    group.throughput(Throughput::Elements(E2E_ROWS as u64));

    for &(label, input_partitions, output_partitions) in cases {
        let (partitions, schema) = prebuilt.get(&input_partitions).unwrap();
        let key_expr = col("key", schema).unwrap();

        group.bench_with_input(
            BenchmarkId::new(label, format!("{input_partitions}_{output_partitions}")),
            &(input_partitions, output_partitions),
            |b, &(_, output_partitions)| {
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
                                Partitioning::Hash(
                                    vec![Arc::clone(&key_expr)],
                                    output_partitions,
                                ),
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

/// Sweep `with_batch_size` across representative topologies to quantify how
/// output batch size affects coalescing throughput.  The default session batch
/// size is 8 192; values above it reduce flush frequency and amortise
/// per-flush overhead (Utf8View builder finalisation, List offset walks,
/// channel sends).
fn bench_repartition_batch_size(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();
    let task_ctx = Arc::new(TaskContext::default());

    const E2E_ROWS: usize = 10_000_000;

    // Batch sizes to sweep: 0.5× default, 1× default, 2×, 4×, 8×.
    const BATCH_SIZES: &[usize] = &[4_096, 8_192, 16_384, 32_768, 65_536];

    // Topologies chosen to stress each bottleneck identified in profiling:
    //   n_to_n 8→8:       balanced repartition, moderate coalescing
    //   consolidation 32→8: Utf8View coalesce-heavy (18 % CPU at default)
    //   fan_out 8→32:     List-offset-heavy + growing channel pressure
    //   high_fan_out 32→128: channel-dominated at default batch size
    let cases: &[(&str, usize, usize)] = &[
        ("n_to_n", 8, 8),
        ("consolidation", 32, 8),
        ("fan_out", 8, 32),
        ("high_fan_out", 32, 128),
    ];

    let mut prebuilt: HashMap<usize, (Vec<Vec<RecordBatch>>, SchemaRef)> = HashMap::new();
    for &(_, input_partitions, _) in cases {
        prebuilt.entry(input_partitions).or_insert_with(|| {
            make_partitioned_input(input_partitions, E2E_ROWS / input_partitions)
        });
    }

    let mut group = c.benchmark_group("repartition_exec/batch_size");
    group.throughput(Throughput::Elements(E2E_ROWS as u64));

    for &(label, input_partitions, output_partitions) in cases {
        let (partitions, schema) = prebuilt.get(&input_partitions).unwrap();
        let key_expr = col("key", schema).unwrap();
        let topology = format!("{label}/{input_partitions}_{output_partitions}");

        for &batch_size in BATCH_SIZES {
            group.bench_with_input(
                BenchmarkId::new(&topology, batch_size),
                &batch_size,
                |b, &batch_size| {
                    b.iter_batched(
                        || {
                            let input = TestMemoryExec::try_new_exec(
                                partitions,
                                schema.clone(),
                                None,
                            )
                            .unwrap();
                            Arc::new(
                                RepartitionExec::try_new(
                                    input,
                                    Partitioning::Hash(
                                        vec![Arc::clone(&key_expr)],
                                        output_partitions,
                                    ),
                                )
                                .unwrap()
                                .with_batch_size(batch_size)
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
    }
    group.finish();
}

criterion_group!(
    benches,
    bench_hash_partitioner_partition_count,
    bench_hash_partitioner_key_types,
    bench_repartition_exec,
    bench_repartition_batch_size,
);
criterion_main!(benches);
