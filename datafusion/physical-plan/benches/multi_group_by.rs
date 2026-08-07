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

//! Benchmarks for multi-column GROUP BY performance comparing vectorized
//! (`GroupValuesColumn`) vs row-based (`GroupValuesRows`) implementations.
//!
//! Motivated by <https://github.com/apache/datafusion/issues/17850> which
//! showed vectorized can regress for low-cardinality, high-row-count scenarios.
//!
//! Uses the direct `GroupValues::intern()` API with identical Int32 data for
//! both implementations — a fair apples-to-apples comparison with the same
//! hashing and data layout.

use arrow::array::{ArrayRef, Int32Array};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use criterion::{BenchmarkId, Criterion, criterion_group, criterion_main};
use datafusion_physical_plan::aggregates::group_values::GroupValues;
use datafusion_physical_plan::aggregates::group_values::GroupValuesRows;
use datafusion_physical_plan::aggregates::group_values::multi_group_by::GroupValuesColumn;
use std::hint::black_box;
use std::sync::Arc;

const DEFAULT_BATCH_SIZE: usize = 8192;

fn make_schema(num_cols: usize) -> SchemaRef {
    let fields: Vec<Field> = (0..num_cols)
        .map(|i| Field::new(format!("col_{i}"), DataType::Int32, false))
        .collect();
    Arc::new(Schema::new(fields))
}

fn generate_batches(
    num_cols: usize,
    num_distinct_groups: usize,
    num_rows: usize,
    batch_size: usize,
) -> Vec<Vec<ArrayRef>> {
    let per_col_card = (num_distinct_groups as f64)
        .powf(1.0 / num_cols as f64)
        .ceil() as usize;

    let num_full_batches = num_rows / batch_size;
    let remainder = num_rows % batch_size;
    let num_batches = num_full_batches + if remainder > 0 { 1 } else { 0 };

    (0..num_batches)
        .map(|batch_idx| {
            let batch_start = batch_idx * batch_size;
            let current_batch_size = if batch_idx == num_batches - 1 && remainder > 0 {
                remainder
            } else {
                batch_size
            };
            (0..num_cols)
                .map(|col_idx| {
                    let values: Vec<i32> = (0..current_batch_size)
                        .map(|row| {
                            let global_row = batch_start + row;
                            let group_id = global_row % num_distinct_groups;
                            let divisor = per_col_card.pow(col_idx as u32);
                            ((group_id / divisor) % per_col_card) as i32
                        })
                        .collect();
                    Arc::new(Int32Array::from(values)) as ArrayRef
                })
                .collect()
        })
        .collect()
}

fn create_group_values(schema: &SchemaRef, vectorized: bool) -> Box<dyn GroupValues> {
    if vectorized {
        Box::new(GroupValuesColumn::<false>::try_new(Arc::clone(schema)).unwrap())
    } else {
        Box::new(GroupValuesRows::try_new(Arc::clone(schema)).unwrap())
    }
}

fn bench_intern(
    gv: &mut Box<dyn GroupValues>,
    batches: &[Vec<ArrayRef>],
    groups: &mut Vec<usize>,
) {
    for batch in batches {
        groups.clear();
        gv.intern(batch, groups).unwrap();
    }
    black_box(&*groups);
}

/// Experiment 1: Issue #17850 regression scenario.
/// 3 columns, 64 groups (4^3), scaling row count.
fn bench_issue_17850_regression(c: &mut Criterion) {
    let mut group = c.benchmark_group("issue_17850_regression");
    group.sample_size(10);

    let num_cols = 3;
    let num_groups = 64;
    let schema = make_schema(num_cols);

    for num_rows in [1_000_000, 5_000_000, 10_000_000, 20_000_000, 50_000_000] {
        let batches =
            generate_batches(num_cols, num_groups, num_rows, DEFAULT_BATCH_SIZE);

        for vectorized in [true, false] {
            let label = if vectorized {
                "vectorized"
            } else {
                "row_based"
            };
            group.bench_with_input(
                BenchmarkId::new(label, format!("{num_rows}_rows")),
                &batches,
                |b, batches| {
                    b.iter_batched_ref(
                        || {
                            (
                                create_group_values(&schema, vectorized),
                                Vec::<usize>::with_capacity(DEFAULT_BATCH_SIZE),
                            )
                        },
                        |(gv, groups)| bench_intern(gv, batches, groups),
                        criterion::BatchSize::LargeInput,
                    );
                },
            );
        }
    }
    group.finish();
}

/// Experiment 2: Low cardinality sweep.
fn bench_low_cardinality(c: &mut Criterion) {
    let mut group = c.benchmark_group("low_cardinality");
    group.sample_size(15);

    for (num_cols, per_col_card) in
        [(3usize, 2usize), (3, 4), (3, 8), (4, 2), (4, 4), (4, 8)]
    {
        let num_groups = per_col_card.pow(num_cols as u32);
        let schema = make_schema(num_cols);
        let batches =
            generate_batches(num_cols, num_groups, 1_000_000, DEFAULT_BATCH_SIZE);

        for vectorized in [true, false] {
            let label = if vectorized {
                "vectorized"
            } else {
                "row_based"
            };
            group.bench_with_input(
                BenchmarkId::new(
                    label,
                    format!("cols_{num_cols}_card_{per_col_card}_grp_{num_groups}"),
                ),
                &batches,
                |b, batches| {
                    b.iter_batched_ref(
                        || {
                            (
                                create_group_values(&schema, vectorized),
                                Vec::<usize>::with_capacity(DEFAULT_BATCH_SIZE),
                            )
                        },
                        |(gv, groups)| bench_intern(gv, batches, groups),
                        criterion::BatchSize::LargeInput,
                    );
                },
            );
        }
    }
    group.finish();
}

/// Experiment 3: Batch size sensitivity.
fn bench_batch_size_sensitivity(c: &mut Criterion) {
    let mut group = c.benchmark_group("batch_size_sensitivity");
    group.sample_size(10);

    let num_cols = 3;
    let num_groups = 64;
    let schema = make_schema(num_cols);

    for batch_size in [1024, 4096, 8192, 16384, 32768] {
        let batches = generate_batches(num_cols, num_groups, 1_000_000, batch_size);

        for vectorized in [true, false] {
            let label = if vectorized {
                "vectorized"
            } else {
                "row_based"
            };
            group.bench_with_input(
                BenchmarkId::new(label, format!("batch_{batch_size}")),
                &batches,
                |b, batches| {
                    b.iter_batched_ref(
                        || {
                            (
                                create_group_values(&schema, vectorized),
                                Vec::<usize>::with_capacity(batch_size),
                            )
                        },
                        |(gv, groups)| bench_intern(gv, batches, groups),
                        criterion::BatchSize::LargeInput,
                    );
                },
            );
        }
    }
    group.finish();
}

/// Experiment 4: Column count scaling with low groups.
fn bench_column_scaling(c: &mut Criterion) {
    let mut group = c.benchmark_group("column_scaling");
    group.sample_size(15);

    let cases: &[(usize, usize)] =
        &[(2, 100), (3, 125), (4, 81), (6, 729), (8, 256), (10, 1024)];

    for &(num_cols, num_groups) in cases {
        let schema = make_schema(num_cols);
        let batches =
            generate_batches(num_cols, num_groups, 1_000_000, DEFAULT_BATCH_SIZE);

        for vectorized in [true, false] {
            let label = if vectorized {
                "vectorized"
            } else {
                "row_based"
            };
            group.bench_with_input(
                BenchmarkId::new(label, format!("cols_{num_cols}_grp_{num_groups}")),
                &batches,
                |b, batches| {
                    b.iter_batched_ref(
                        || {
                            (
                                create_group_values(&schema, vectorized),
                                Vec::<usize>::with_capacity(DEFAULT_BATCH_SIZE),
                            )
                        },
                        |(gv, groups)| bench_intern(gv, batches, groups),
                        criterion::BatchSize::LargeInput,
                    );
                },
            );
        }
    }
    group.finish();
}

/// Experiment 5: High cardinality column scaling (~1M groups).
fn bench_high_cardinality_scaling(c: &mut Criterion) {
    let mut group = c.benchmark_group("high_cardinality_scaling");
    group.sample_size(10);

    for num_cols in [2, 3, 4, 6, 8, 10] {
        let num_groups = 1_000_000;
        let schema = make_schema(num_cols);
        let batches =
            generate_batches(num_cols, num_groups, 1_000_000, DEFAULT_BATCH_SIZE);

        for vectorized in [true, false] {
            let label = if vectorized {
                "vectorized"
            } else {
                "row_based"
            };
            group.bench_with_input(
                BenchmarkId::new(label, format!("cols_{num_cols}_grp_1M")),
                &batches,
                |b, batches| {
                    b.iter_batched_ref(
                        || {
                            (
                                create_group_values(&schema, vectorized),
                                Vec::<usize>::with_capacity(DEFAULT_BATCH_SIZE),
                            )
                        },
                        |(gv, groups)| bench_intern(gv, batches, groups),
                        criterion::BatchSize::LargeInput,
                    );
                },
            );
        }
    }
    group.finish();
}

/// Experiment 6: Group count sweep with fixed 4 columns.
fn bench_group_count_sweep(c: &mut Criterion) {
    let mut group = c.benchmark_group("group_count_sweep");
    group.sample_size(15);

    let num_cols = 4;
    let schema = make_schema(num_cols);

    for num_groups in [
        16, 64, 256, 1000, 5000, 10_000, 50_000, 100_000, 500_000, 1_000_000,
    ] {
        let batches =
            generate_batches(num_cols, num_groups, 1_000_000, DEFAULT_BATCH_SIZE);

        for vectorized in [true, false] {
            let label = if vectorized {
                "vectorized"
            } else {
                "row_based"
            };
            group.bench_with_input(
                BenchmarkId::new(label, format!("grp_{num_groups}")),
                &batches,
                |b, batches| {
                    b.iter_batched_ref(
                        || {
                            (
                                create_group_values(&schema, vectorized),
                                Vec::<usize>::with_capacity(DEFAULT_BATCH_SIZE),
                            )
                        },
                        |(gv, groups)| bench_intern(gv, batches, groups),
                        criterion::BatchSize::LargeInput,
                    );
                },
            );
        }
    }
    group.finish();
}

criterion_group!(
    benches,
    bench_issue_17850_regression,
    bench_low_cardinality,
    bench_batch_size_sensitivity,
    bench_column_scaling,
    bench_high_cardinality_scaling,
    bench_group_count_sweep,
);
criterion_main!(benches);
