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

//! Benchmarks for [`take_n_offsets`], which splits an offset vector at index
//! `n` (returning `[offsets[0..=n]]` and leaving the shifted remainder in
//! place). It underpins `ByteGroupValueBuilder::take_n`, but is measured here in
//! isolation on a plain `Vec<O>` — no arrays, no builder, no `emit` — so the
//! offset-splitting work is the whole signal.

use arrow::array::OffsetSizeTrait;
use criterion::{BenchmarkId, Criterion, criterion_group, criterion_main};
use datafusion_common::utils::take_n_offsets;
use std::hint::black_box;

/// Offset width under test: 32-bit (`Utf8`/`Binary`) vs 64-bit
/// (`LargeUtf8`/`LargeBinary`) offsets. 64-bit doubles the bytes moved.
#[derive(Clone, Copy)]
enum OffsetWidth {
    I32,
    I64,
}

impl OffsetWidth {
    fn label(self) -> &'static str {
        match self {
            OffsetWidth::I32 => "i32_offsets",
            OffsetWidth::I64 => "i64_offsets",
        }
    }
}

/// The three split-branch cases, keyed on `n` vs the number of groups `g`.
/// `take_n_offsets` allocates for whichever side is smaller (predicate
/// `n < offsets.len() - n`, where `offsets.len() == g + 1`):
///
/// * `shift_larger` (`n = g/4`): copy the smaller prefix out, shift the larger
///   remainder in place.
/// * `shift_midsize` (`n = g/2`): the balanced point; also the in-place-shift
///   branch, but with source/dest halves disjoint.
/// * `shift_smaller` (`n = g - g/4`): the other branch — allocate the smaller
///   remainder, return the larger prefix via `mem::replace`.
fn cases(num_groups: usize) -> [(&'static str, usize); 3] {
    [
        ("shift_larger", num_groups / 4),
        ("shift_midsize", num_groups / 2),
        ("shift_smaller", num_groups - num_groups / 4),
    ]
}

/// A monotonic offset vector of length `num_groups + 1` (`0, 1, …, g`), matching
/// the shape a byte builder holds for `g` groups.
fn make_offsets<O: OffsetSizeTrait>(num_groups: usize) -> Vec<O> {
    (0..=num_groups).map(O::usize_as).collect()
}

fn bench_take_n_offsets(
    c: &mut Criterion,
    group_name: &str,
    num_groups: usize,
    sample_size: usize,
    batch_size_hint: criterion::BatchSize,
) {
    let mut group = c.benchmark_group(group_name);
    group.sample_size(sample_size);

    for width in [OffsetWidth::I32, OffsetWidth::I64] {
        for (branch, n) in cases(num_groups) {
            let id =
                BenchmarkId::new(branch, format!("{}_grp_{num_groups}", width.label()));
            group.bench_function(id, |b| {
                // `take_n_offsets` mutates its input, so rebuild a fresh offset
                // vector per iteration in the untimed setup. Monomorphize per
                // width; only the `take_n_offsets` call is timed.
                match width {
                    OffsetWidth::I32 => b.iter_batched_ref(
                        || make_offsets::<i32>(num_groups),
                        |offsets| drop(black_box(take_n_offsets(offsets, n))),
                        batch_size_hint,
                    ),
                    OffsetWidth::I64 => b.iter_batched_ref(
                        || make_offsets::<i64>(num_groups),
                        |offsets| drop(black_box(take_n_offsets(offsets, n))),
                        batch_size_hint,
                    ),
                }
            });
        }
    }
    group.finish();
}

/// Low-cardinality (1K groups): offset vectors fit in cache.
fn bench_take_n_offsets_small(c: &mut Criterion) {
    bench_take_n_offsets(
        c,
        "take_n_offsets_small",
        1_000,
        200,
        criterion::BatchSize::SmallInput,
    );
}

/// High-cardinality (1M groups): offset vectors exceed cache, so the split's
/// memory-access pattern dominates.
fn bench_take_n_offsets_large(c: &mut Criterion) {
    bench_take_n_offsets(
        c,
        "take_n_offsets_large",
        1_000_000,
        50,
        criterion::BatchSize::LargeInput,
    );
}

criterion_group!(
    benches,
    bench_take_n_offsets_small,
    bench_take_n_offsets_large
);
criterion_main!(benches);
