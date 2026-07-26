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

use arrow::array::{ArrayRef, Int64Array, StringArray};
use arrow::datatypes::DataType;
use criterion::{BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};
use datafusion_expr::Accumulator;
use datafusion_functions_aggregate::min_max::SlidingMaxAccumulator;
use rand::Rng;
use rand::SeedableRng;
use rand::rngs::StdRng;
use std::sync::Arc;

fn generate_random_i64(size: usize) -> Vec<i64> {
    let mut rng = StdRng::seed_from_u64(42);
    (0..size).map(|_| rng.random_range(0..1_000_000)).collect()
}

fn generate_random_strings(size: usize) -> Vec<String> {
    let mut rng = StdRng::seed_from_u64(42);
    (0..size)
        .map(|_| {
            let len = rng.random_range(10..40);
            (0..len)
                .map(|_| rng.random_range(b'a'..=b'z') as char)
                .collect()
        })
        .collect()
}

/// Simulates a sliding window by calling update_batch and retract_batch
/// on SlidingMaxAccumulator, mirroring how the query engine uses it.
fn bench_sliding_max_for(
    c: &mut Criterion,
    label: &str,
    data_type: DataType,
    array: ArrayRef,
    data_size: usize,
    window_size: usize,
) {
    let mut group = c.benchmark_group(format!("sliding_window_max_{label}"));
    group.throughput(Throughput::Elements(data_size as u64));

    group.bench_with_input(
        BenchmarkId::new("sliding_max", window_size),
        &window_size,
        |b, &w| {
            b.iter(|| {
                let mut acc = SlidingMaxAccumulator::try_new(&data_type).unwrap();
                // Warm up the window
                let init_batch = array.slice(0, w);
                acc.update_batch(&[init_batch]).unwrap();

                // Slide: for each subsequent element, add it and retract one
                for i in w..data_size {
                    let new_val = array.slice(i, 1);
                    let old_val = array.slice(i - w, 1);
                    acc.update_batch(&[new_val]).unwrap();
                    acc.retract_batch(&[old_val]).unwrap();
                    std::hint::black_box(acc.evaluate().unwrap());
                }
            });
        },
    );

    group.finish();
}

fn bench_sliding_max(c: &mut Criterion) {
    let data_size = 50_000;

    let i64_data: Vec<i64> = generate_random_i64(data_size);
    let str_data: Vec<String> = generate_random_strings(data_size);

    let i64_array: ArrayRef = Arc::new(Int64Array::from(i64_data));
    let str_array: ArrayRef = Arc::new(StringArray::from(str_data));

    for window_size in [100, 1000, 5000] {
        bench_sliding_max_for(
            c,
            "int64",
            DataType::Int64,
            Arc::clone(&i64_array),
            data_size,
            window_size,
        );
        bench_sliding_max_for(
            c,
            "utf8",
            DataType::Utf8,
            Arc::clone(&str_array),
            data_size,
            window_size,
        );
    }
}

criterion_group!(benches, bench_sliding_max);
criterion_main!(benches);
