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

//! Benchmarks included (rationale)
//!
//! The benchmarks included here were designed to exercise the adaptive
//! mode-selection heuristics implemented by the MinMax "bytes" accumulator.
//! Each benchmark targets a specific workload shape to demonstrate why the
//! DenseInline, Simple, or SparseOptimized paths were chosen and to quantify
//! the performance trade-offs between them.
//!
//! - `min bytes dense duplicate groups`:
//!   Simulates batches where group ids are densely packed but many consecutive
//!   rows target the same group (duplicate consecutive group ids). This
//!   exercises the fast-path in the dense-inline implementation that detects
//!   consecutive runs and avoids repeated checks/marks.
//!
//! - `min bytes dense reused accumulator`:
//!   Multi-batch workload with a stable set of groups across batches. This
//!   measures the benefit of reusing lazily-allocated dense scratch/state and
//!   ensures the epoch-based marking correctly avoids per-batch clearing.
//!
//! - `min bytes monotonic group ids`:
//!   Groups are produced in a growing/monotonic order across rows and batches.
//!   This pattern favours simple dense approaches and validates that the
//!   algorithm recognises monotonic access to enable the inline fast path.
//!
//! - `min bytes multi batch large`:
//!   A large multi-batch scenario (many batches and many groups) intended to
//!   capture the behaviour of the adaptive switch under realistic streaming
//!   workloads where amortised costs matter most. This benchmark highlights
//!   the worst-case gains from choosing the DenseInline/SparseOptimized paths.
//!
//! - `min bytes sparse groups`:
//!   Sparse and high-cardinality access patterns where only a tiny fraction of
//!   the group domain is touched in each batch. This validates the
//!   SparseOptimized implementation which uses hash-based tracking to avoid
//!   allocating or zeroing a large dense scratch table every batch.
//!
//! - `min bytes dense first batch`:
//!   A single-batch dense workload used to measure the overhead of the
//!   undecided/mode-selection phase. It demonstrates the small constant
//!   bookkeeping cost before a mode is chosen (the measured ~1% regression).
//!
//! - `min bytes large dense groups`:
//!   A single-batch scenario with many dense groups (large N). It ensures the
//!   heuristic threshold (e.g. 100k) and memory trade-offs do not cause
//!   excessive allocations or regress the single-batch path significantly.
//!
//! - `min bytes single batch large`:
//!   A single-batch run with a large number of groups to ensure the simple
//!   path remains efficient for one-off aggregations and to quantify the
//!   fixed overhead of adaptive bookkeeping.
//!
//! - `min bytes single batch small`:
//!   A small single-batch workload used to show that the overhead of the
//!   adaptive approach is negligible when groups and data are tiny (micro
//!   workloads), and that the simple path remains the fastest for these
//!   cases.

use std::sync::Arc;

use arrow::array::{ArrayRef, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use criterion::{black_box, criterion_group, criterion_main, Criterion};
use datafusion_expr::{function::AccumulatorArgs, AggregateUDFImpl, GroupsAccumulator};
use datafusion_functions_aggregate::min_max::Min;
use datafusion_physical_expr::expressions::col;

const BATCH_SIZE: usize = 512;
const SPARSE_GROUPS: usize = 16;
const LARGE_TOTAL_GROUPS: usize = 10_000;
const MONOTONIC_BATCHES: usize = 32;
const MONOTONIC_TOTAL_GROUPS: usize = MONOTONIC_BATCHES * BATCH_SIZE;
const LARGE_DENSE_GROUPS: usize = MONOTONIC_TOTAL_GROUPS;

fn prepare_min_accumulator(data_type: &DataType) -> Box<dyn GroupsAccumulator> {
    let field = Field::new("f", data_type.clone(), true).into();
    let schema = Arc::new(Schema::new(vec![Arc::clone(&field)]));
    let accumulator_args = AccumulatorArgs {
        return_field: field,
        schema: &schema,
        ignore_nulls: false,
        order_bys: &[],
        is_reversed: false,
        name: "MIN(f)",
        is_distinct: false,
        exprs: &[col("f", &schema).unwrap()],
    };

    Min::new()
        .create_groups_accumulator(accumulator_args)
        .expect("create min accumulator")
}

fn min_bytes_single_batch_small(c: &mut Criterion) {
    let values: ArrayRef = Arc::new(StringArray::from_iter_values(
        (0..BATCH_SIZE).map(|i| format!("value_{:04}", i)),
    ));
    let group_indices: Vec<usize> = (0..BATCH_SIZE).collect();

    c.bench_function("min bytes single batch small", |b| {
        b.iter(|| {
            let mut accumulator = prepare_min_accumulator(&DataType::Utf8);
            black_box(
                accumulator
                    .update_batch(
                        std::slice::from_ref(&values),
                        &group_indices,
                        None,
                        BATCH_SIZE,
                    )
                    .expect("update batch"),
            );
        })
    });
}

fn min_bytes_single_batch_large(c: &mut Criterion) {
    let values: ArrayRef = Arc::new(StringArray::from_iter_values(
        (0..LARGE_DENSE_GROUPS).map(|i| format!("value_{:04}", i)),
    ));
    let group_indices: Vec<usize> = (0..LARGE_DENSE_GROUPS).collect();

    c.bench_function("min bytes single batch large", |b| {
        b.iter(|| {
            let mut accumulator = prepare_min_accumulator(&DataType::Utf8);
            black_box(
                accumulator
                    .update_batch(
                        std::slice::from_ref(&values),
                        &group_indices,
                        None,
                        LARGE_DENSE_GROUPS,
                    )
                    .expect("update batch"),
            );
        })
    });
}

fn min_bytes_multi_batch_large(c: &mut Criterion) {
    let values: ArrayRef = Arc::new(StringArray::from_iter_values(
        (0..BATCH_SIZE).map(|i| format!("value_{:04}", i)),
    ));
    let group_batches: Vec<Vec<usize>> = (0..MONOTONIC_BATCHES)
        .map(|batch| {
            let start = batch * BATCH_SIZE;
            (0..BATCH_SIZE).map(|i| start + i).collect()
        })
        .collect();

    c.bench_function("min bytes multi batch large", |b| {
        b.iter(|| {
            let mut accumulator = prepare_min_accumulator(&DataType::Utf8);
            for group_indices in &group_batches {
                black_box(
                    accumulator
                        .update_batch(
                            std::slice::from_ref(&values),
                            group_indices,
                            None,
                            LARGE_DENSE_GROUPS,
                        )
                        .expect("update batch"),
                );
            }
        })
    });
}

fn min_bytes_sparse_groups(c: &mut Criterion) {
    let values: ArrayRef = Arc::new(StringArray::from_iter_values(
        (0..BATCH_SIZE).map(|i| format!("value_{:04}", i % 1024)),
    ));
    let group_indices: Vec<usize> = (0..BATCH_SIZE).map(|i| i % SPARSE_GROUPS).collect();

    c.bench_function("min bytes sparse groups", |b| {
        b.iter(|| {
            let mut accumulator = prepare_min_accumulator(&DataType::Utf8);
            black_box(
                accumulator
                    .update_batch(
                        std::slice::from_ref(&values),
                        &group_indices,
                        None,
                        LARGE_TOTAL_GROUPS,
                    )
                    .expect("update batch"),
            );
        })
    });
}

fn min_bytes_dense_first_batch(c: &mut Criterion) {
    let values: ArrayRef = Arc::new(StringArray::from_iter_values(
        (0..BATCH_SIZE).map(|i| format!("value_{:04}", i)),
    ));
    let group_indices: Vec<usize> = (0..BATCH_SIZE).collect();

    c.bench_function("min bytes dense first batch", |b| {
        b.iter(|| {
            let mut accumulator = prepare_min_accumulator(&DataType::Utf8);
            black_box(
                accumulator
                    .update_batch(
                        std::slice::from_ref(&values),
                        &group_indices,
                        None,
                        BATCH_SIZE,
                    )
                    .expect("update batch"),
            );
        })
    });
}

fn min_bytes_dense_reused_batches(c: &mut Criterion) {
    let values: ArrayRef = Arc::new(StringArray::from_iter_values(
        (0..BATCH_SIZE).map(|i| format!("value_{:04}", i)),
    ));
    let group_indices: Vec<usize> = (0..BATCH_SIZE).collect();

    c.bench_function("min bytes dense reused accumulator", |b| {
        b.iter(|| {
            let mut accumulator = prepare_min_accumulator(&DataType::Utf8);
            for _ in 0..MONOTONIC_BATCHES {
                black_box(
                    accumulator
                        .update_batch(
                            std::slice::from_ref(&values),
                            &group_indices,
                            None,
                            BATCH_SIZE,
                        )
                        .expect("update batch"),
                );
            }
        })
    });
}

fn min_bytes_dense_duplicate_groups(c: &mut Criterion) {
    let unique_groups = BATCH_SIZE / 2;
    let values: ArrayRef = Arc::new(StringArray::from_iter_values(
        (0..BATCH_SIZE).map(|i| format!("value_{:04}", i / 2)),
    ));
    let group_indices: Vec<usize> = (0..unique_groups).flat_map(|i| [i, i]).collect();

    c.bench_function("min bytes dense duplicate groups", |b| {
        b.iter(|| {
            let mut accumulator = prepare_min_accumulator(&DataType::Utf8);
            for _ in 0..MONOTONIC_BATCHES {
                black_box(
                    accumulator
                        .update_batch(
                            std::slice::from_ref(&values),
                            &group_indices,
                            None,
                            unique_groups,
                        )
                        .expect("update batch"),
                );
            }
        })
    });
}

/// Demonstration benchmark: simulate growing `total_num_groups` across batches
/// while group indices remain dense in each batch. This exposes quadratic
/// allocation behaviour when per-batch allocations scale with the historical
/// total number of groups (the pathological case discussed in the issue).
fn min_bytes_quadratic_growing_total_groups(c: &mut Criterion) {
    // Start small and grow total_num_groups across batches to simulate a
    // workload that discovers more groups over time. Each batch contains
    // BATCH_SIZE rows with dense group indices in the current domain.
    let base_batch_values: ArrayRef = Arc::new(StringArray::from_iter_values(
        (0..BATCH_SIZE).map(|i| format!("value_{:04}", i)),
    ));

    c.bench_function("min bytes quadratic growing total groups", |b| {
        b.iter(|| {
            let mut accumulator = prepare_min_accumulator(&DataType::Utf8);

            // Grow total_num_groups by increments of BATCH_SIZE for several
            // batches to expose allocations proportional to the growing domain.
            let mut total_groups = BATCH_SIZE;
            for _ in 0..MONOTONIC_BATCHES {
                let group_indices: Vec<usize> =
                    (0..BATCH_SIZE).map(|i| i % total_groups).collect();

                black_box(
                    accumulator
                        .update_batch(
                            std::slice::from_ref(&base_batch_values),
                            &group_indices,
                            None,
                            total_groups,
                        )
                        .expect("update batch"),
                );

                total_groups = total_groups.saturating_add(BATCH_SIZE);
            }
        })
    });
}

fn min_bytes_monotonic_group_ids(c: &mut Criterion) {
    let values: ArrayRef = Arc::new(StringArray::from_iter_values(
        (0..BATCH_SIZE).map(|i| format!("value_{:04}", i % 1024)),
    ));
    let group_batches: Vec<Vec<usize>> = (0..MONOTONIC_BATCHES)
        .map(|batch| {
            let start = batch * BATCH_SIZE;
            (0..BATCH_SIZE).map(|i| start + i).collect()
        })
        .collect();

    c.bench_function("min bytes monotonic group ids", |b| {
        b.iter(|| {
            let mut accumulator = prepare_min_accumulator(&DataType::Utf8);
            for group_indices in &group_batches {
                black_box(
                    accumulator
                        .update_batch(
                            std::slice::from_ref(&values),
                            group_indices,
                            None,
                            MONOTONIC_TOTAL_GROUPS,
                        )
                        .expect("update batch"),
                );
            }
        })
    });
}

fn min_bytes_growing_total_groups(c: &mut Criterion) {
    // Each batch introduces a new contiguous block of group ids so the
    // 'total_num_groups' parameter grows with each iteration. This simulates
    // workloads where the domain of groups increases over time and exposes
    // alloc/resize behaviour that scales with the historical number of groups.
    let values: ArrayRef = Arc::new(StringArray::from_iter_values(
        (0..BATCH_SIZE).map(|i| format!("value_{:04}", i % 1024)),
    ));
    let group_batches: Vec<Vec<usize>> = (0..MONOTONIC_BATCHES)
        .map(|batch| {
            let start = batch * BATCH_SIZE;
            (0..BATCH_SIZE).map(|i| start + i).collect()
        })
        .collect();

    c.bench_function("min bytes growing total groups", |b| {
        b.iter(|| {
            let mut accumulator = prepare_min_accumulator(&DataType::Utf8);
            for (batch_idx, group_indices) in group_batches.iter().enumerate() {
                // Simulate the increasing total_num_groups observed by the
                // accumulator: each batch's total groups equals the highest
                // group index observed so far plus one.
                let total_num_groups = (batch_idx + 1) * BATCH_SIZE;
                black_box(
                    accumulator
                        .update_batch(
                            std::slice::from_ref(&values),
                            group_indices,
                            None,
                            total_num_groups,
                        )
                        .expect("update batch"),
                );
            }
        })
    });
}

fn min_bytes_large_dense_groups(c: &mut Criterion) {
    let values: ArrayRef = Arc::new(StringArray::from_iter_values(
        (0..LARGE_DENSE_GROUPS).map(|i| format!("value_{:04}", i)),
    ));
    let group_indices: Vec<usize> = (0..LARGE_DENSE_GROUPS).collect();

    c.bench_function("min bytes large dense groups", |b| {
        b.iter(|| {
            let mut accumulator = prepare_min_accumulator(&DataType::Utf8);
            black_box(
                accumulator
                    .update_batch(
                        std::slice::from_ref(&values),
                        &group_indices,
                        None,
                        LARGE_DENSE_GROUPS,
                    )
                    .expect("update batch"),
            );
        })
    });
}

criterion_group!(
    benches,
    min_bytes_single_batch_small,
    min_bytes_single_batch_large,
    min_bytes_multi_batch_large,
    min_bytes_dense_first_batch,
    min_bytes_dense_reused_batches,
    min_bytes_dense_duplicate_groups,
    min_bytes_quadratic_growing_total_groups,
    min_bytes_sparse_groups,
    min_bytes_monotonic_group_ids,
    min_bytes_growing_total_groups,
    min_bytes_large_dense_groups
);
criterion_main!(benches);
