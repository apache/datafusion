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

use criterion::{BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};
use datafusion_common::ScalarValue;
use datafusion_functions_aggregate::min_max::MovingMax;
use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};

// === Generator Utilities ===
fn generate_random_i64(size: usize) -> Vec<i64> {
    let mut rng = StdRng::seed_from_u64(42);
    (0..size).map(|_| rng.random_range(0..1000000)).collect()
}

fn generate_random_f64(size: usize) -> Vec<f64> {
    let mut rng = StdRng::seed_from_u64(42);
    (0..size).map(|_| rng.random()).collect()
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

fn bench_sliding_max(c: &mut Criterion) {
    let data_size = 50000;

    // Generate raw inputs
    let i64_raw = generate_random_i64(data_size);
    let f64_raw = generate_random_f64(data_size);
    let str_raw = generate_random_strings(data_size);

    // Map to ScalarValue types
    let scalar_int_data: Vec<ScalarValue> = i64_raw
        .iter()
        .map(|&val| ScalarValue::Int64(Some(val)))
        .collect();
    let scalar_float_data: Vec<ScalarValue> = f64_raw
        .iter()
        .map(|&val| ScalarValue::Float64(Some(val)))
        .collect();
    let scalar_timestamp_data: Vec<ScalarValue> = i64_raw
        .iter()
        .map(|&val| ScalarValue::TimestampNanosecond(Some(val), None))
        .collect();
    let scalar_decimal_data: Vec<ScalarValue> = i64_raw
        .iter()
        .map(|&val| ScalarValue::Decimal128(Some(val as i128), 38, 10))
        .collect();
    let scalar_utf8_data: Vec<ScalarValue> = str_raw
        .iter()
        .map(|val| ScalarValue::Utf8(Some(val.clone())))
        .collect();

    let datasets = vec![
        ("scalar_int64", scalar_int_data),
        ("scalar_float64", scalar_float_data),
        ("scalar_timestamp", scalar_timestamp_data),
        ("scalar_decimal128", scalar_decimal_data),
        ("scalar_utf8_string", scalar_utf8_data),
    ];

    for (label, data) in datasets {
        let mut group = c.benchmark_group(format!("sliding_window_max_{label}"));

        for window_size in [100, 1000, 5000] {
            group.throughput(Throughput::Elements(data_size as u64));

            group.bench_with_input(
                BenchmarkId::new("monotonic_deque", window_size),
                &window_size,
                |b, &w| {
                    b.iter(|| {
                        let mut q = MovingMax::new();
                        for (i, val) in data.iter().enumerate() {
                            q.push(val.clone());
                            if i >= w {
                                q.pop();
                            }
                            let _res = q.max();
                        }
                    });
                },
            );
        }
        group.finish();
    }
}

criterion_group!(benches, bench_sliding_max);
criterion_main!(benches);
