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

extern crate criterion;

use criterion::{black_box, criterion_group, criterion_main, Criterion};
use datafusion_expr::ScalarUDFImpl;
use datafusion_functions::math::random::RandomFunc;

fn criterion_benchmark(c: &mut Criterion) {
    let random_func = RandomFunc::new();

    // Benchmark to evaluate 1M rows in batch size 8192
    let iterations = 1_000_000 / 8192; // Calculate how many iterations are needed to reach approximately 1M rows
    c.bench_function("random_1M_rows_batch_8192", |b| {
        b.iter(|| {
            for _ in 0..iterations {
                black_box(random_func.invoke_no_args(8192).unwrap());
            }
        })
    });

    // Benchmark to evaluate 1M rows in batch size 128
    let iterations_128 = 1_000_000 / 128; // Calculate how many iterations are needed to reach approximately 1M rows with batch size 128
    c.bench_function("random_1M_rows_batch_128", |b| {
        b.iter(|| {
            for _ in 0..iterations_128 {
                black_box(random_func.invoke_no_args(128).unwrap());
            }
        })
    });
}

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);
