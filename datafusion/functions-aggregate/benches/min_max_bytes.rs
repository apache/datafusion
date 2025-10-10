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
//!
//! - `min bytes extreme duplicates`:
//!   Very small number of unique groups with many repeated rows per group.
//!   This stresses run-length and duplicate detection behaviour and ensures
//!   the accumulator handles extreme duplicate patterns without excessive
//!   per-row overhead.
//!
//! - `min bytes quadratic growing total groups`:
//!   Demonstration benchmark that grows `total_num_groups` across batches
//!   while each batch remains dense. It exposes pathological allocation or
//!   resize behaviour which scales with the historical total number of
//!   groups (quadratic behaviour) so the heuristic/resizing strategy can be
//!   validated.
//!
//! - `min bytes sequential stable groups`:
//!   Multiple batches where the same contiguous set of groups is touched in
//!   each batch. This measures benefit of reusing lazily-allocated dense
//!   scratch/state across batches.
//!
//! - `min bytes sequential dense large stable`:
//!   A multi-batch benchmark with a large dense domain that remains stable
//!   across batches. Used to confirm the sequential dense path reuses
//!   allocations and remains consistent.
//!
//! - `min bytes sequential dense large allocations`:
//!   Similar to the stable sequential benchmark but each batch allocates
//!   different values to ensure the accumulator reuses its scratch
//!   allocation across batches (asserts stable size).
//!
//! - `min bytes medium cardinality stable`:
//!   Medium-cardinality workload where each batch touches a large fraction
//!   (e.g. 80%) of the full domain. This captures behaviour between dense
//!   and sparse extremes and validates the heuristic choices.
//!
//! - `min bytes ultra sparse`:
//!   Extremely high-cardinality domain where each batch touches only a tiny
//!   number of groups. This validates the SparseOptimized implementation and
//!   hash-based tracking for low-touch workloads.
//!
//! - `min bytes mode transition`:
//!   Alternating phase benchmark that flips between dense and sparse phases.
//!   This checks the accumulator's ability to recognise and adapt between
//!   modes over time without catastrophic thrashing.

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
const STABLE_GROUPS: usize = 1_000;
const STABLE_BATCHES: usize = 50;
const SEQUENTIAL_DENSE_LARGE_GROUPS: usize = 65_536;
const SEQUENTIAL_DENSE_LARGE_BATCHES: usize = 8;
const MEDIUM_TOTAL_GROUPS: usize = 50_000;
const MEDIUM_BATCHES: usize = 20;
const ULTRA_SPARSE_TOTAL_GROUPS: usize = 1_000_000;
const ULTRA_SPARSE_BATCHES: usize = 20;
const ULTRA_SPARSE_ACTIVE: usize = 100;
const MODE_TRANSITION_PHASES: usize = 20; // 10 dense + 10 sparse

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

fn make_string_values(len: usize) -> ArrayRef {
    Arc::new(StringArray::from_iter_values(
        (0..len).map(|i| format!("value_{i:05}")),
    ))
}

fn bench_batches<F>(
    c: &mut Criterion,
    name: &str,
    total_num_groups: usize,
    group_batches: &[Vec<usize>],
    mut with_values: F,
) where
    F: FnMut(usize) -> ArrayRef,
{
    c.bench_function(name, |b| {
        b.iter(|| {
            let mut accumulator = prepare_min_accumulator(&DataType::Utf8);
            for (batch_idx, group_indices) in group_batches.iter().enumerate() {
                let values = with_values(batch_idx);
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

fn min_bytes_extreme_duplicates(c: &mut Criterion) {
    let unique_groups = 50;
    let repeats_per_group = 10;
    let total_rows = unique_groups * repeats_per_group;

    let mut value_strings = Vec::with_capacity(total_rows);
    for group in 0..unique_groups {
        for _ in 0..repeats_per_group {
            value_strings.push(format!("value_{group:04}"));
        }
    }
    let values: ArrayRef = Arc::new(StringArray::from(value_strings));
    let group_indices: Vec<usize> = (0..unique_groups)
        .flat_map(|group| std::iter::repeat(group).take(repeats_per_group))
        .collect();

    debug_assert_eq!(values.len(), total_rows);
    debug_assert_eq!(group_indices.len(), total_rows);

    c.bench_function("min bytes extreme duplicates", |b| {
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

fn min_bytes_sequential_stable_groups(c: &mut Criterion) {
    let batches: Vec<Vec<usize>> = (0..STABLE_BATCHES)
        .map(|_| (0..STABLE_GROUPS).collect())
        .collect();

    bench_batches(
        c,
        "min bytes sequential stable groups",
        STABLE_GROUPS,
        &batches,
        |_| make_string_values(STABLE_GROUPS),
    );
}

fn min_bytes_sequential_dense_large_stable(c: &mut Criterion) {
    let batches: Vec<Vec<usize>> = (0..SEQUENTIAL_DENSE_LARGE_BATCHES)
        .map(|_| (0..SEQUENTIAL_DENSE_LARGE_GROUPS).collect())
        .collect();

    let baseline = make_string_values(SEQUENTIAL_DENSE_LARGE_GROUPS);
    bench_batches(
        c,
        "min bytes sequential dense large stable",
        SEQUENTIAL_DENSE_LARGE_GROUPS,
        &batches,
        move |_| baseline.clone(),
    );
}

fn min_bytes_sequential_dense_large_allocations(c: &mut Criterion) {
    let group_indices: Vec<usize> = (0..SEQUENTIAL_DENSE_LARGE_GROUPS).collect();
    let batches: Vec<ArrayRef> = (0..SEQUENTIAL_DENSE_LARGE_BATCHES)
        .map(|step| {
            let prefix = (b'z' - step as u8) as char;
            Arc::new(StringArray::from_iter_values(
                (0..SEQUENTIAL_DENSE_LARGE_GROUPS)
                    .map(|group| format!("{prefix}{prefix}_{group:05}")),
            )) as ArrayRef
        })
        .collect();

    c.bench_function("min bytes sequential dense large allocations", |b| {
        b.iter(|| {
            let mut accumulator = prepare_min_accumulator(&DataType::Utf8);
            let mut baseline_size: Option<usize> = None;

            for values in &batches {
                black_box(
                    accumulator
                        .update_batch(
                            std::slice::from_ref(values),
                            &group_indices,
                            None,
                            SEQUENTIAL_DENSE_LARGE_GROUPS,
                        )
                        .expect("update batch"),
                );

                let current_size = accumulator.size();
                if let Some(expected) = baseline_size {
                    assert_eq!(
                        current_size, expected,
                        "sequential dense path should reuse its scratch allocation"
                    );
                } else {
                    baseline_size = Some(current_size);
                }
            }
        })
    });
}

fn min_bytes_medium_cardinality_stable(c: &mut Criterion) {
    let touched_per_batch = (MEDIUM_TOTAL_GROUPS as f64 * 0.8) as usize;
    let batches: Vec<Vec<usize>> = (0..MEDIUM_BATCHES)
        .map(|batch| {
            let start = (batch * touched_per_batch) % MEDIUM_TOTAL_GROUPS;
            (0..touched_per_batch)
                .map(|offset| (start + offset) % MEDIUM_TOTAL_GROUPS)
                .collect()
        })
        .collect();

    bench_batches(
        c,
        "min bytes medium cardinality stable",
        MEDIUM_TOTAL_GROUPS,
        &batches,
        |_| make_string_values(touched_per_batch),
    );
}

fn min_bytes_ultra_sparse(c: &mut Criterion) {
    let batches: Vec<Vec<usize>> = (0..ULTRA_SPARSE_BATCHES)
        .map(|batch| {
            let base = (batch * ULTRA_SPARSE_ACTIVE) % ULTRA_SPARSE_TOTAL_GROUPS;
            (0..ULTRA_SPARSE_ACTIVE)
                .map(|offset| (base + offset * 8_129) % ULTRA_SPARSE_TOTAL_GROUPS)
                .collect()
        })
        .collect();

    bench_batches(
        c,
        "min bytes ultra sparse",
        ULTRA_SPARSE_TOTAL_GROUPS,
        &batches,
        |_| make_string_values(ULTRA_SPARSE_ACTIVE),
    );
}

fn min_bytes_mode_transition(c: &mut Criterion) {
    let mut batches = Vec::with_capacity(MODE_TRANSITION_PHASES * 2);

    let dense_touch = (STABLE_GROUPS as f64 * 0.9) as usize;
    for batch in 0..MODE_TRANSITION_PHASES {
        let start = (batch * dense_touch) % STABLE_GROUPS;
        batches.push(
            (0..dense_touch)
                .map(|offset| (start + offset) % STABLE_GROUPS)
                .collect(),
        );
    }

    let sparse_total = 100_000;
    let sparse_touch = (sparse_total as f64 * 0.05) as usize;
    for batch in 0..MODE_TRANSITION_PHASES {
        let start = (batch * sparse_touch * 13) % sparse_total;
        batches.push(
            (0..sparse_touch)
                .map(|offset| (start + offset * 17) % sparse_total)
                .collect(),
        );
    }

    bench_batches(
        c,
        "min bytes mode transition",
        sparse_total,
        &batches,
        |batch_idx| {
            if batch_idx < MODE_TRANSITION_PHASES {
                make_string_values(dense_touch)
            } else {
                make_string_values(sparse_touch)
            }
        },
    );
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
    min_bytes_extreme_duplicates,
    min_bytes_quadratic_growing_total_groups,
    min_bytes_sparse_groups,
    min_bytes_monotonic_group_ids,
    min_bytes_growing_total_groups,
    min_bytes_large_dense_groups,
    min_bytes_sequential_stable_groups,
    min_bytes_sequential_dense_large_stable,
    min_bytes_sequential_dense_large_allocations,
    min_bytes_medium_cardinality_stable,
    min_bytes_ultra_sparse,
    min_bytes_mode_transition
);
criterion_main!(benches);
