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

//! Benchmarks comparing hash-based `GroupValuesPrimitive` vs direct-indexed
//! `GroupValuesFlatPrimitive` for single-column integer GROUP BY.
//!
//! Measures only `GroupValues::intern()` — construction and emit are excluded
//! from timing to isolate the group lookup hot path.

use arrow::array::{ArrayRef, UInt64Array};
use arrow::datatypes::{DataType, UInt64Type};
use criterion::{BatchSize, BenchmarkId, Criterion, criterion_group, criterion_main};
use datafusion_physical_plan::aggregates::group_values::GroupValues;
use datafusion_physical_plan::aggregates::group_values::single_group_by::{
    flat_primitive::GroupValuesFlatPrimitive, primitive::GroupValuesPrimitive,
};
use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};
use std::hint::black_box;
use std::sync::Arc;

const SEED: u64 = 0xBEEF;
const NUM_ROWS: usize = 1_000_000;
const BATCH_SIZE: usize = 8192;

fn generate_batches(
    num_groups: u64,
    num_rows: usize,
    batch_size: usize,
) -> Vec<ArrayRef> {
    let mut rng = StdRng::seed_from_u64(SEED);
    let mut batches = Vec::new();
    let mut remaining = num_rows;

    while remaining > 0 {
        let this_batch = remaining.min(batch_size);
        let values: Vec<u64> = (0..this_batch)
            .map(|_| rng.random_range(0..num_groups))
            .collect();
        batches.push(Arc::new(UInt64Array::from(values)) as ArrayRef);
        remaining -= this_batch;
    }
    batches
}

fn bench_intern(
    gv: &mut Box<dyn GroupValues>,
    batches: &[ArrayRef],
    groups: &mut Vec<usize>,
) {
    for batch in batches {
        gv.intern(std::slice::from_ref(batch), groups).unwrap();
    }
    black_box(&*groups);
}

/// Group count sweep: measures intern() only, construction in setup.
/// Tests how both implementations scale with increasing group cardinality.
fn bench_group_count(c: &mut Criterion) {
    let group_counts: Vec<u64> = vec![10, 100, 1_000, 10_000, 100_000];

    let mut group = c.benchmark_group("single_group_by_u64");
    group.sample_size(20);

    for &num_groups in &group_counts {
        let batches = generate_batches(num_groups, NUM_ROWS, BATCH_SIZE);

        group.bench_with_input(
            BenchmarkId::new("hash", num_groups),
            &batches,
            |b, batches| {
                b.iter_batched_ref(
                    || {
                        (
                            Box::new(GroupValuesPrimitive::<UInt64Type>::new(
                                DataType::UInt64,
                            )) as Box<dyn GroupValues>,
                            Vec::<usize>::with_capacity(BATCH_SIZE),
                        )
                    },
                    |(gv, groups)| bench_intern(gv, batches, groups),
                    BatchSize::LargeInput,
                );
            },
        );

        group.bench_with_input(
            BenchmarkId::new("flat", num_groups),
            &batches,
            |b, batches| {
                b.iter_batched_ref(
                    || {
                        (
                            Box::new(GroupValuesFlatPrimitive::<UInt64Type>::new(
                                DataType::UInt64,
                                0,
                                num_groups - 1,
                            )) as Box<dyn GroupValues>,
                            Vec::<usize>::with_capacity(BATCH_SIZE),
                        )
                    },
                    |(gv, groups)| bench_intern(gv, batches, groups),
                    BatchSize::LargeInput,
                );
            },
        );
    }
    group.finish();
}

/// Density sweep: fixed 10K distinct groups, varying the key range.
/// Tests how flat degrades as the array becomes sparser.
fn bench_density(c: &mut Criterion) {
    let mut group = c.benchmark_group("single_group_by_u64_density");
    group.sample_size(20);

    let num_distinct: u64 = 10_000;
    let densities: Vec<(f64, u64)> = vec![
        (1.0, num_distinct),
        (0.5, num_distinct * 2),
        (0.1, num_distinct * 10),
    ];

    for (density, range) in &densities {
        let mut rng = StdRng::seed_from_u64(SEED);
        let mut keys: Vec<u64> = Vec::new();
        let mut seen = std::collections::HashSet::new();
        while keys.len() < num_distinct as usize {
            let k = rng.random_range(0..*range);
            if seen.insert(k) {
                keys.push(k);
            }
        }

        let mut batches = Vec::new();
        let mut remaining = NUM_ROWS;
        while remaining > 0 {
            let this_batch = remaining.min(BATCH_SIZE);
            let values: Vec<u64> = (0..this_batch)
                .map(|_| keys[rng.random_range(0..keys.len())])
                .collect();
            batches.push(Arc::new(UInt64Array::from(values)) as ArrayRef);
            remaining -= this_batch;
        }

        let label = format!("density_{:.0}pct", density * 100.0);

        group.bench_with_input(
            BenchmarkId::new("hash", &label),
            &batches,
            |b, batches| {
                b.iter_batched_ref(
                    || {
                        (
                            Box::new(GroupValuesPrimitive::<UInt64Type>::new(
                                DataType::UInt64,
                            )) as Box<dyn GroupValues>,
                            Vec::<usize>::with_capacity(BATCH_SIZE),
                        )
                    },
                    |(gv, groups)| bench_intern(gv, batches, groups),
                    BatchSize::LargeInput,
                );
            },
        );

        group.bench_with_input(
            BenchmarkId::new("flat", &label),
            &batches,
            |b, batches| {
                b.iter_batched_ref(
                    || {
                        (
                            Box::new(GroupValuesFlatPrimitive::<UInt64Type>::new(
                                DataType::UInt64,
                                0,
                                range - 1,
                            )) as Box<dyn GroupValues>,
                            Vec::<usize>::with_capacity(BATCH_SIZE),
                        )
                    },
                    |(gv, groups)| bench_intern(gv, batches, groups),
                    BatchSize::LargeInput,
                );
            },
        );
    }
    group.finish();
}

/// Row count scaling: fixed 10K groups, varying input size.
/// Shows how the per-row cost difference compounds.
fn bench_row_scaling(c: &mut Criterion) {
    let mut group = c.benchmark_group("single_group_by_u64_rows");
    group.sample_size(10);

    let num_groups: u64 = 10_000;

    for num_rows in [1_000_000, 5_000_000, 10_000_000] {
        let batches = generate_batches(num_groups, num_rows, BATCH_SIZE);

        group.bench_with_input(
            BenchmarkId::new("hash", format!("{num_rows}_rows")),
            &batches,
            |b, batches| {
                b.iter_batched_ref(
                    || {
                        (
                            Box::new(GroupValuesPrimitive::<UInt64Type>::new(
                                DataType::UInt64,
                            )) as Box<dyn GroupValues>,
                            Vec::<usize>::with_capacity(BATCH_SIZE),
                        )
                    },
                    |(gv, groups)| bench_intern(gv, batches, groups),
                    BatchSize::LargeInput,
                );
            },
        );

        group.bench_with_input(
            BenchmarkId::new("flat", format!("{num_rows}_rows")),
            &batches,
            |b, batches| {
                b.iter_batched_ref(
                    || {
                        (
                            Box::new(GroupValuesFlatPrimitive::<UInt64Type>::new(
                                DataType::UInt64,
                                0,
                                num_groups - 1,
                            )) as Box<dyn GroupValues>,
                            Vec::<usize>::with_capacity(BATCH_SIZE),
                        )
                    },
                    |(gv, groups)| bench_intern(gv, batches, groups),
                    BatchSize::LargeInput,
                );
            },
        );
    }
    group.finish();
}

criterion_group!(benches, bench_group_count, bench_density, bench_row_scaling);
criterion_main!(benches);
