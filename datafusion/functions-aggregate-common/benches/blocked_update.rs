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

//! How the per-group state is addressed in the accumulator update loop.
//!
//! `flat` is `values[g]` on one `Vec`. `naive_blocked` is
//! `blocks[g >> SHIFT][g & MASK]`, the dependent lookup that made earlier
//! blocked designs slower at high cardinality: the block pointer must load
//! before the state access can issue, so misses on the state serialize.
//! `gather_blocked` resolves a chunk of addresses in an independent pass
//! first, then updates through them. `accumulator` is the real
//! [`PrimitiveGroupsAccumulator`] (single block up to `1 << 20` groups,
//! address gather beyond).
//!
//! Group indices are uniformly random so nothing but the addressing differs
//! between the variants; the interesting sizes are the ones whose state no
//! longer fits in cache.

use std::hint::black_box;
use std::sync::Arc;

use arrow::array::{ArrayRef, Int64Array};
use arrow::datatypes::{DataType, Int64Type};
use criterion::{BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};
use datafusion_expr_common::groups_accumulator::GroupsAccumulator;
use datafusion_functions_aggregate_common::aggregate::groups_accumulator::accumulate::RESOLVE_CHUNK;
use datafusion_functions_aggregate_common::aggregate::groups_accumulator::blocks::BlockedVec;
use datafusion_functions_aggregate_common::aggregate::groups_accumulator::prim_op::PrimitiveGroupsAccumulator;

const BATCH_SIZE: usize = 8192;
const BATCHES: usize = 128;
/// Small blocks so every size is blocked; the block count does not matter.
const BENCH_SHIFT: u32 = 16;

/// Group indices as `intern` hands them out: numbered in order of first
/// appearance, so every batch's new groups are dense and `NullState` takes
/// its fast path. `4 * num_groups` rows; the stream is replayed from an
/// empty accumulator each iteration.
fn dense_stream(num_groups: usize) -> (Vec<Vec<usize>>, Vec<usize>) {
    let mut state = 0x9E37_79B9_7F4A_7C15u64;
    let mut ids = vec![usize::MAX; num_groups];
    let mut next = 0usize;
    let mut batches = vec![];
    let mut totals = vec![];
    for _ in 0..(4 * num_groups).div_ceil(BATCH_SIZE) {
        let batch: Vec<usize> = (0..BATCH_SIZE)
            .map(|_| {
                state ^= state << 13;
                state ^= state >> 7;
                state ^= state << 17;
                let key = (state % num_groups as u64) as usize;
                if ids[key] == usize::MAX {
                    ids[key] = next;
                    next += 1;
                }
                ids[key]
            })
            .collect();
        batches.push(batch);
        totals.push(next);
    }
    (batches, totals)
}

fn group_batches(num_groups: usize) -> Vec<Vec<usize>> {
    let mut state = 0x9E37_79B9_7F4A_7C15u64;
    let mut next = move || {
        state ^= state << 13;
        state ^= state >> 7;
        state ^= state << 17;
        (state % num_groups as u64) as usize
    };
    (0..BATCHES)
        .map(|_| (0..BATCH_SIZE).map(|_| next()).collect())
        .collect()
}

fn flat(values: &mut [i64], groups: &[usize], input: &[i64]) {
    for (&g, &v) in groups.iter().zip(input) {
        values[g] += v;
    }
}

fn naive_blocked(
    values: &mut BlockedVec<i64, BENCH_SHIFT>,
    groups: &[usize],
    input: &[i64],
) {
    for (&g, &v) in groups.iter().zip(input) {
        *values.get_mut(g) += v;
    }
}

fn gather_blocked(
    values: &mut BlockedVec<i64, BENCH_SHIFT>,
    groups: &[usize],
    input: &[i64],
) {
    let resolver = values.address_resolver();
    let mut addrs = [std::ptr::null_mut::<i64>(); RESOLVE_CHUNK];
    for (groups, input) in groups
        .chunks(RESOLVE_CHUNK)
        .zip(input.chunks(RESOLVE_CHUNK))
    {
        let addrs = &mut addrs[..groups.len()];
        resolver.resolve(groups, addrs);
        for (&addr, &v) in addrs.iter().zip(input) {
            // SAFETY: resolved from live blocks the resolver borrows
            unsafe { *addr += v };
        }
    }
}

fn criterion_benchmark(c: &mut Criterion) {
    let input: Vec<i64> = (0..BATCH_SIZE as i64).collect();
    let input_array: ArrayRef = Arc::new(Int64Array::from(input.clone()));
    let mut group = c.benchmark_group("blocked_update");
    group.sample_size(10);
    group.throughput(Throughput::Elements((BATCHES * BATCH_SIZE) as u64));

    for (label, num_groups) in [("100k", 100_000), ("1M", 1_000_000), ("10M", 10_000_000)]
    {
        let batches = group_batches(num_groups);

        let mut values = vec![0i64; num_groups];
        group.bench_with_input(
            BenchmarkId::new("flat", label),
            &batches,
            |b, batches| {
                b.iter(|| {
                    for groups in batches {
                        flat(&mut values, black_box(groups), black_box(&input));
                    }
                })
            },
        );

        let mut values = BlockedVec::<i64, BENCH_SHIFT>::new();
        values.resize(num_groups, 0);
        group.bench_with_input(
            BenchmarkId::new("naive_blocked", label),
            &batches,
            |b, batches| {
                b.iter(|| {
                    for groups in batches {
                        naive_blocked(&mut values, black_box(groups), black_box(&input));
                    }
                })
            },
        );

        let mut values = BlockedVec::<i64, BENCH_SHIFT>::new();
        values.resize(num_groups, 0);
        group.bench_with_input(
            BenchmarkId::new("gather_blocked", label),
            &batches,
            |b, batches| {
                b.iter(|| {
                    for groups in batches {
                        gather_blocked(&mut values, black_box(groups), black_box(&input));
                    }
                })
            },
        );

        let mut accumulator = PrimitiveGroupsAccumulator::<Int64Type, _>::new(
            &DataType::Int64,
            |current, value| *current += value,
        );
        group.bench_with_input(
            BenchmarkId::new("accumulator", label),
            &batches,
            |b, batches| {
                b.iter(|| {
                    for groups in batches {
                        accumulator
                            .update_batch(
                                std::slice::from_ref(&input_array),
                                black_box(groups),
                                None,
                                num_groups,
                            )
                            .unwrap();
                    }
                })
            },
        );
    }
    group.finish();

    // The partial stage's loop: dense new groups, no nulls, no filter.
    let mut group = c.benchmark_group("blocked_update_dense");
    group.sample_size(10);
    for (label, num_groups) in
        [("1M", 1_000_000), ("10M", 10_000_000), ("20M", 20_000_000)]
    {
        let (batches, totals) = dense_stream(num_groups);
        group.throughput(Throughput::Elements((batches.len() * BATCH_SIZE) as u64));
        group.bench_with_input(
            BenchmarkId::new("accumulator", label),
            &(batches, totals),
            |b, (batches, totals)| {
                b.iter(|| {
                    let mut accumulator = PrimitiveGroupsAccumulator::<Int64Type, _>::new(
                        &DataType::Int64,
                        |current, value| *current += value,
                    );
                    for (groups, &total) in batches.iter().zip(totals) {
                        accumulator
                            .update_batch(
                                std::slice::from_ref(&input_array),
                                black_box(groups),
                                None,
                                total,
                            )
                            .unwrap();
                    }
                    black_box(accumulator.size())
                })
            },
        );
    }
    group.finish();
}

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);
