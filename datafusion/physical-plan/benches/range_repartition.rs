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

use arrow::array::{ArrayRef, Int64Array, RecordBatch, StringArray};
use arrow::compute::SortOptions;
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use criterion::{BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};
use datafusion_common::ScalarValue;
use datafusion_physical_expr::expressions::col;
use datafusion_physical_expr::{
    LexOrdering, PhysicalExpr, PhysicalSortExpr, RangePartitioning, SplitPoint,
};
use datafusion_physical_plan::metrics::Time;
use datafusion_physical_plan::repartition::{BatchPartitioner, RangeExpr};
use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};

const BATCH_SIZE: usize = 8192;
const PARTITION_COUNTS: [usize; 7] = [8, 16, 32, 64, 128, 256, 512];
const SEED: u64 = 42;

fn create_i64_uniform_batch(schema: &SchemaRef, max_val: i64) -> RecordBatch {
    let mut rng = StdRng::seed_from_u64(SEED);
    let key_values: Vec<i64> = (0..BATCH_SIZE)
        .map(|_| rng.random_range(0..max_val))
        .collect();
    let payload_values: Vec<i64> = (0..BATCH_SIZE).map(|i| i as i64).collect();

    RecordBatch::try_new(
        Arc::clone(schema),
        vec![
            Arc::new(Int64Array::from(key_values)) as ArrayRef,
            Arc::new(Int64Array::from(payload_values)) as ArrayRef,
        ],
    )
    .unwrap()
}

fn create_i64_sequential_batch(schema: &SchemaRef, max_val: i64) -> RecordBatch {
    let key_values: Vec<i64> = (0..BATCH_SIZE)
        .map(|i| ((i as i64) * max_val) / (BATCH_SIZE as i64))
        .collect();
    let payload_values: Vec<i64> = (0..BATCH_SIZE).map(|i| i as i64).collect();

    RecordBatch::try_new(
        Arc::clone(schema),
        vec![
            Arc::new(Int64Array::from(key_values)) as ArrayRef,
            Arc::new(Int64Array::from(payload_values)) as ArrayRef,
        ],
    )
    .unwrap()
}

fn create_utf8_uniform_batch(schema: &SchemaRef, max_val: usize) -> RecordBatch {
    let mut rng = StdRng::seed_from_u64(SEED);
    let key_strings: Vec<String> = (0..BATCH_SIZE)
        .map(|_| format!("key_{:010}", rng.random_range(0..max_val)))
        .collect();
    let payload_values: Vec<i64> = (0..BATCH_SIZE).map(|i| i as i64).collect();

    RecordBatch::try_new(
        Arc::clone(schema),
        vec![
            Arc::new(StringArray::from_iter_values(
                key_strings.iter().map(String::as_str),
            )) as ArrayRef,
            Arc::new(Int64Array::from(payload_values)) as ArrayRef,
        ],
    )
    .unwrap()
}

fn create_composite_i64_batch(schema: &SchemaRef, max_val: i64) -> RecordBatch {
    let mut rng = StdRng::seed_from_u64(SEED);
    let key1_values: Vec<i64> = (0..BATCH_SIZE)
        .map(|_| rng.random_range(0..max_val))
        .collect();
    let key2_values: Vec<i64> = (0..BATCH_SIZE)
        .map(|_| rng.random_range(0..max_val))
        .collect();
    let payload_values: Vec<i64> = (0..BATCH_SIZE).map(|i| i as i64).collect();

    RecordBatch::try_new(
        Arc::clone(schema),
        vec![
            Arc::new(Int64Array::from(key1_values)) as ArrayRef,
            Arc::new(Int64Array::from(key2_values)) as ArrayRef,
            Arc::new(Int64Array::from(payload_values)) as ArrayRef,
        ],
    )
    .unwrap()
}

fn bench_range_repartition_i64_uniform(c: &mut Criterion) {
    let mut group = c.benchmark_group("range_repartition_i64_uniform");
    group.throughput(Throughput::Elements(BATCH_SIZE as u64));

    let schema = Arc::new(Schema::new(vec![
        Field::new("key", DataType::Int64, false),
        Field::new("payload", DataType::Int64, false),
    ]));

    let max_val = 1_000_000i64;
    let batch = create_i64_uniform_batch(&schema, max_val);

    for &num_partitions in &PARTITION_COUNTS {
        let ordering = LexOrdering::new(vec![PhysicalSortExpr::new(
            col("key", &schema).unwrap(),
            SortOptions::default(),
        )])
        .unwrap();

        let split_points: Vec<SplitPoint> = (1..num_partitions)
            .map(|i| {
                let val = (i as i64 * max_val) / (num_partitions as i64);
                SplitPoint::new(vec![ScalarValue::Int64(Some(val))])
            })
            .collect();

        let range_part = RangePartitioning::try_new(ordering, split_points).unwrap();

        group.bench_with_input(
            BenchmarkId::new("partitions", num_partitions),
            &num_partitions,
            |b, _| {
                let mut partitioner = BatchPartitioner::try_new_range_partitioner(
                    &range_part,
                    &schema,
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
            },
        );
    }
    group.finish();
}

fn bench_range_repartition_i64_sequential(c: &mut Criterion) {
    let mut group = c.benchmark_group("range_repartition_i64_sequential");
    group.throughput(Throughput::Elements(BATCH_SIZE as u64));

    let schema = Arc::new(Schema::new(vec![
        Field::new("key", DataType::Int64, false),
        Field::new("payload", DataType::Int64, false),
    ]));

    let max_val = 1_000_000i64;
    let batch = create_i64_sequential_batch(&schema, max_val);

    for &num_partitions in &PARTITION_COUNTS {
        let ordering = LexOrdering::new(vec![PhysicalSortExpr::new(
            col("key", &schema).unwrap(),
            SortOptions::default(),
        )])
        .unwrap();

        let split_points: Vec<SplitPoint> = (1..num_partitions)
            .map(|i| {
                let val = (i as i64 * max_val) / (num_partitions as i64);
                SplitPoint::new(vec![ScalarValue::Int64(Some(val))])
            })
            .collect();

        let range_part = RangePartitioning::try_new(ordering, split_points).unwrap();

        group.bench_with_input(
            BenchmarkId::new("partitions", num_partitions),
            &num_partitions,
            |b, _| {
                let mut partitioner = BatchPartitioner::try_new_range_partitioner(
                    &range_part,
                    &schema,
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
            },
        );
    }
    group.finish();
}

fn bench_range_repartition_utf8_uniform(c: &mut Criterion) {
    let mut group = c.benchmark_group("range_repartition_utf8_uniform");
    group.throughput(Throughput::Elements(BATCH_SIZE as u64));

    let schema = Arc::new(Schema::new(vec![
        Field::new("key", DataType::Utf8, false),
        Field::new("payload", DataType::Int64, false),
    ]));

    let max_val = 1_000_000usize;
    let batch = create_utf8_uniform_batch(&schema, max_val);

    for &num_partitions in &PARTITION_COUNTS {
        let ordering = LexOrdering::new(vec![PhysicalSortExpr::new(
            col("key", &schema).unwrap(),
            SortOptions::default(),
        )])
        .unwrap();

        let split_points: Vec<SplitPoint> = (1..num_partitions)
            .map(|i| {
                let val = (i * max_val) / num_partitions;
                SplitPoint::new(vec![ScalarValue::Utf8(Some(format!("key_{val:010}")))])
            })
            .collect();

        let range_part = RangePartitioning::try_new(ordering, split_points).unwrap();

        group.bench_with_input(
            BenchmarkId::new("partitions", num_partitions),
            &num_partitions,
            |b, _| {
                let mut partitioner = BatchPartitioner::try_new_range_partitioner(
                    &range_part,
                    &schema,
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
            },
        );
    }
    group.finish();
}

fn bench_range_repartition_composite_i64(c: &mut Criterion) {
    let mut group = c.benchmark_group("range_repartition_composite_i64");
    group.throughput(Throughput::Elements(BATCH_SIZE as u64));

    let schema = Arc::new(Schema::new(vec![
        Field::new("key1", DataType::Int64, false),
        Field::new("key2", DataType::Int64, false),
        Field::new("payload", DataType::Int64, false),
    ]));

    let max_val = 1_000_000i64;
    let batch = create_composite_i64_batch(&schema, max_val);

    for &num_partitions in &PARTITION_COUNTS {
        let ordering = LexOrdering::new(vec![
            PhysicalSortExpr::new(col("key1", &schema).unwrap(), SortOptions::default()),
            PhysicalSortExpr::new(col("key2", &schema).unwrap(), SortOptions::default()),
        ])
        .unwrap();

        let split_points: Vec<SplitPoint> = (1..num_partitions)
            .map(|i| {
                let val1 = (i as i64 * max_val) / (num_partitions as i64);
                let val2 = 0i64;
                SplitPoint::new(vec![
                    ScalarValue::Int64(Some(val1)),
                    ScalarValue::Int64(Some(val2)),
                ])
            })
            .collect();

        let range_part = RangePartitioning::try_new(ordering, split_points).unwrap();

        group.bench_with_input(
            BenchmarkId::new("partitions", num_partitions),
            &num_partitions,
            |b, _| {
                let mut partitioner = BatchPartitioner::try_new_range_partitioner(
                    &range_part,
                    &schema,
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
            },
        );
    }
    group.finish();
}

fn bench_range_expr_routing_i64(c: &mut Criterion) {
    let mut group = c.benchmark_group("range_expr_routing_i64");
    group.throughput(Throughput::Elements(BATCH_SIZE as u64));

    let schema = Arc::new(Schema::new(vec![
        Field::new("key", DataType::Int64, false),
        Field::new("payload", DataType::Int64, false),
    ]));

    let max_val = 1_000_000i64;
    let batch = create_i64_uniform_batch(&schema, max_val);

    for &num_partitions in &PARTITION_COUNTS {
        let col_expr = col("key", &schema).unwrap();
        let ordering = LexOrdering::new(vec![PhysicalSortExpr::new(
            Arc::clone(&col_expr),
            SortOptions::default(),
        )])
        .unwrap();

        let split_points: Vec<SplitPoint> = (1..num_partitions)
            .map(|i| {
                let val = (i as i64 * max_val) / (num_partitions as i64);
                SplitPoint::new(vec![ScalarValue::Int64(Some(val))])
            })
            .collect();

        let range_part = RangePartitioning::try_new(ordering, split_points).unwrap();
        let range_expr =
            RangeExpr::try_new_with_schema(vec![col_expr], &range_part, &batch.schema())
                .unwrap();

        group.bench_with_input(
            BenchmarkId::new("partitions", num_partitions),
            &num_partitions,
            |b, _| {
                b.iter(|| {
                    let res = range_expr.evaluate(&batch).unwrap();
                    black_box(res);
                });
            },
        );
    }
    group.finish();
}

fn bench_range_partitioner_construction(c: &mut Criterion) {
    let mut group = c.benchmark_group("range_partitioner_construction");

    let schema = Arc::new(Schema::new(vec![
        Field::new("key", DataType::Int64, false),
        Field::new("payload", DataType::Int64, false),
    ]));

    let max_val = 1_000_000i64;

    for &num_partitions in &PARTITION_COUNTS {
        let ordering = LexOrdering::new(vec![PhysicalSortExpr::new(
            col("key", &schema).unwrap(),
            SortOptions::default(),
        )])
        .unwrap();

        let split_points: Vec<SplitPoint> = (1..num_partitions)
            .map(|i| {
                let val = (i as i64 * max_val) / (num_partitions as i64);
                SplitPoint::new(vec![ScalarValue::Int64(Some(val))])
            })
            .collect();

        let range_part = RangePartitioning::try_new(ordering, split_points).unwrap();

        group.bench_with_input(
            BenchmarkId::new("partitions", num_partitions),
            &num_partitions,
            |b, _| {
                b.iter(|| {
                    let partitioner = BatchPartitioner::try_new_range_partitioner(
                        black_box(&range_part),
                        black_box(&schema),
                        Time::default(),
                    )
                    .unwrap();
                    black_box(partitioner);
                });
            },
        );
    }
    group.finish();
}

criterion_group!(
    benches,
    bench_range_repartition_i64_uniform,
    bench_range_repartition_i64_sequential,
    bench_range_repartition_utf8_uniform,
    bench_range_repartition_composite_i64,
    bench_range_expr_routing_i64,
    bench_range_partitioner_construction
);
criterion_main!(benches);
