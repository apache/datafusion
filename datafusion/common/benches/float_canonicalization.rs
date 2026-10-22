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
//! Benchmarks for `normalize_float_zero`, which runs on every float key
//! column before it is compared, hashed or grouped. The common case is a
//! batch with nothing to canonicalize, where only the check runs.

use std::hint::black_box;
use std::sync::Arc;

use arrow::array::{ArrayRef, Float32Array, Float64Array};
use criterion::{BenchmarkId, Criterion, criterion_group, criterion_main};
use datafusion_common::utils::normalize_float_zero;
use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};

const BATCH_SIZE: usize = 8192;

fn values(rng: &mut StdRng) -> Vec<f64> {
    (0..BATCH_SIZE)
        .map(|_| rng.random_range(-1000.0..1000.0))
        .collect()
}

fn bench_float_canonicalization(c: &mut Criterion) {
    let mut group = c.benchmark_group("float_canonicalization");
    let mut rng = StdRng::seed_from_u64(42);
    let clean = values(&mut rng);
    // A single `-0.0` in the last row: the check scans everything, then copies.
    let mut dirty = clean.clone();
    dirty[BATCH_SIZE - 1] = -0.0;

    for (name, values) in [("clean", &clean), ("neg_zero_last", &dirty)] {
        let f64_array: ArrayRef = Arc::new(Float64Array::from(values.clone()));
        group.bench_with_input(BenchmarkId::new("f64", name), &f64_array, |b, a| {
            b.iter(|| black_box(normalize_float_zero(black_box(a))))
        });

        let f32_values: Vec<f32> = values.iter().map(|&v| v as f32).collect();
        let f32_array: ArrayRef = Arc::new(Float32Array::from(f32_values));
        group.bench_with_input(BenchmarkId::new("f32", name), &f32_array, |b, a| {
            b.iter(|| black_box(normalize_float_zero(black_box(a))))
        });
    }
    group.finish();
}

criterion_group!(benches, bench_float_canonicalization);
criterion_main!(benches);
