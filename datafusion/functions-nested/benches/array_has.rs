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

use criterion::{
    criterion_group, criterion_main, {BenchmarkId, Criterion},
};
use datafusion_expr::lit;
use datafusion_functions_nested::expr_fn::{
    array_has, array_has_all, array_has_any, make_array,
};
use std::hint::black_box;

// If not explicitly stated, `array` and `array_size` refer to the haystack array.
fn criterion_benchmark(c: &mut Criterion) {
    // Test different array sizes
    let array_sizes = vec![1, 10, 100, 1000, 10000];

    for &size in &array_sizes {
        bench_array_has(c, size);
        bench_array_has_all(c, size);
        bench_array_has_any(c, size);
    }

    // Specific benchmarks for string arrays (common use case)
    bench_array_has_strings(c);
    bench_array_has_all_strings(c);
    bench_array_has_any_strings(c);

    // Edge cases
    bench_array_has_edge_cases(c);
}

fn bench_array_has(c: &mut Criterion, array_size: usize) {
    let mut group = c.benchmark_group("array_has_i64");

    // Benchmark: element found at beginning
    group.bench_with_input(
        BenchmarkId::new("found_at_start", array_size),
        &array_size,
        |b, &size| {
            let array = (0..size).map(|i| lit(i as i64)).collect::<Vec<_>>();
            let list_array = make_array(array);
            let needle = lit(0_i64);

            b.iter(|| black_box(array_has(list_array.clone(), needle.clone())))
        },
    );

    // Benchmark: element found at end
    group.bench_with_input(
        BenchmarkId::new("found_at_end", array_size),
        &array_size,
        |b, &size| {
            let array = (0..size).map(|i| lit(i as i64)).collect::<Vec<_>>();
            let list_array = make_array(array);
            let needle = lit((size - 1) as i64);

            b.iter(|| black_box(array_has(list_array.clone(), needle.clone())))
        },
    );

    // Benchmark: element not found
    group.bench_with_input(
        BenchmarkId::new("not_found", array_size),
        &array_size,
        |b, &size| {
            let array = (0..size).map(|i| lit(i as i64)).collect::<Vec<_>>();
            let list_array = make_array(array);
            let needle = lit(-1_i64); // Not in array

            b.iter(|| black_box(array_has(list_array.clone(), needle.clone())))
        },
    );

    group.finish();
}

fn bench_array_has_all(c: &mut Criterion, array_size: usize) {
    let mut group = c.benchmark_group("array_has_all");

    // Benchmark: all elements found (small needle)
    group.bench_with_input(
        BenchmarkId::new("all_found_small_needle", array_size),
        &array_size,
        |b, &size| {
            let array = (0..size).map(|i| lit(i as i64)).collect::<Vec<_>>();
            let list_array = make_array(array);
            let needle_array = make_array(vec![lit(0_i64), lit(1_i64), lit(2_i64)]);

            b.iter(|| black_box(array_has_all(list_array.clone(), needle_array.clone())))
        },
    );

    // Benchmark: all elements found (medium needle - 10% of haystack)
    group.bench_with_input(
        BenchmarkId::new("all_found_medium_needle", array_size),
        &array_size,
        |b, &size| {
            let array = (0..size).map(|i| lit(i as i64)).collect::<Vec<_>>();
            let list_array = make_array(array);
            let needle_size = (size / 10).max(1);
            let needle = (0..needle_size).map(|i| lit(i as i64)).collect::<Vec<_>>();
            let needle_array = make_array(needle);

            b.iter(|| black_box(array_has_all(list_array.clone(), needle_array.clone())))
        },
    );

    // Benchmark: not all found (early exit)
    group.bench_with_input(
        BenchmarkId::new("early_exit", array_size),
        &array_size,
        |b, &size| {
            let array = (0..size).map(|i| lit(i as i64)).collect::<Vec<_>>();
            let list_array = make_array(array);
            let needle_array = make_array(vec![lit(0_i64), lit(-1_i64)]); // -1 not in array

            b.iter(|| black_box(array_has_all(list_array.clone(), needle_array.clone())))
        },
    );

    group.finish();
}

fn bench_array_has_any(c: &mut Criterion, array_size: usize) {
    let mut group = c.benchmark_group("array_has_any");

    // Benchmark: first element matches (best case)
    group.bench_with_input(
        BenchmarkId::new("first_match", array_size),
        &array_size,
        |b, &size| {
            let array = (0..size).map(|i| lit(i as i64)).collect::<Vec<_>>();
            let list_array = make_array(array);
            let needle_array = make_array(vec![lit(0_i64), lit(-1_i64), lit(-2_i64)]);

            b.iter(|| black_box(array_has_any(list_array.clone(), needle_array.clone())))
        },
    );

    // Benchmark: last element matches (worst case)
    group.bench_with_input(
        BenchmarkId::new("last_match", array_size),
        &array_size,
        |b, &size| {
            let array = (0..size).map(|i| lit(i as i64)).collect::<Vec<_>>();
            let list_array = make_array(array);
            let needle_array = make_array(vec![lit(-1_i64), lit(-2_i64), lit(0_i64)]);

            b.iter(|| black_box(array_has_any(list_array.clone(), needle_array.clone())))
        },
    );

    // Benchmark: no match
    group.bench_with_input(
        BenchmarkId::new("no_match", array_size),
        &array_size,
        |b, &size| {
            let array = (0..size).map(|i| lit(i as i64)).collect::<Vec<_>>();
            let list_array = make_array(array);
            let needle_array = make_array(vec![lit(-1_i64), lit(-2_i64), lit(-3_i64)]);

            b.iter(|| black_box(array_has_any(list_array.clone(), needle_array.clone())))
        },
    );

    group.finish();
}

fn bench_array_has_strings(c: &mut Criterion) {
    let mut group = c.benchmark_group("array_has_strings");

    // Benchmark with string arrays (common use case for tickers, tags, etc.)
    let sizes = vec![10, 100, 1000];

    for &size in &sizes {
        group.bench_with_input(BenchmarkId::new("found", size), &size, |b, &size| {
            let array = (0..size)
                .map(|i| lit(format!("TICKER{i:04}")))
                .collect::<Vec<_>>();
            let list_array = make_array(array);
            let needle = lit("TICKER0005");

            b.iter(|| black_box(array_has(list_array.clone(), needle.clone())))
        });

        group.bench_with_input(BenchmarkId::new("not_found", size), &size, |b, &size| {
            let array = (0..size)
                .map(|i| lit(format!("TICKER{i:04}")))
                .collect::<Vec<_>>();
            let list_array = make_array(array);
            let needle = lit("NOTFOUND");

            b.iter(|| black_box(array_has(list_array.clone(), needle.clone())))
        });
    }

    group.finish();
}

fn bench_array_has_all_strings(c: &mut Criterion) {
    let mut group = c.benchmark_group("array_has_all_strings");

    // Realistic scenario: checking if a portfolio contains certain tickers
    let portfolio_size = 100;
    let check_sizes = vec![1, 3, 5, 10];

    for &check_size in &check_sizes {
        group.bench_with_input(
            BenchmarkId::new("all_found", check_size),
            &check_size,
            |b, &check_size| {
                let portfolio = (0..portfolio_size)
                    .map(|i| lit(format!("TICKER{i:04}")))
                    .collect::<Vec<_>>();
                let list_array = make_array(portfolio);

                let checking = (0..check_size)
                    .map(|i| lit(format!("TICKER{i:04}")))
                    .collect::<Vec<_>>();
                let needle_array = make_array(checking);

                b.iter(|| {
                    black_box(array_has_all(list_array.clone(), needle_array.clone()))
                })
            },
        );

        group.bench_with_input(
            BenchmarkId::new("some_missing", check_size),
            &check_size,
            |b, &check_size| {
                let portfolio = (0..portfolio_size)
                    .map(|i| lit(format!("TICKER{i:04}")))
                    .collect::<Vec<_>>();
                let list_array = make_array(portfolio);

                let mut checking = (0..check_size - 1)
                    .map(|i| lit(format!("TICKER{i:04}")))
                    .collect::<Vec<_>>();
                checking.push(lit("NOTFOUND".to_string()));
                let needle_array = make_array(checking);

                b.iter(|| {
                    black_box(array_has_all(list_array.clone(), needle_array.clone()))
                })
            },
        );
    }

    group.finish();
}

fn bench_array_has_any_strings(c: &mut Criterion) {
    let mut group = c.benchmark_group("array_has_any_strings");

    let portfolio_size = 100;
    let check_sizes = vec![1, 3, 5, 10];

    for &check_size in &check_sizes {
        group.bench_with_input(
            BenchmarkId::new("first_matches", check_size),
            &check_size,
            |b, &check_size| {
                let portfolio = (0..portfolio_size)
                    .map(|i| lit(format!("TICKER{i:04}")))
                    .collect::<Vec<_>>();
                let list_array = make_array(portfolio);

                let mut checking = vec![lit("TICKER0000".to_string())];
                checking.extend((1..check_size).map(|_| lit("NOTFOUND".to_string())));
                let needle_array = make_array(checking);

                b.iter(|| {
                    black_box(array_has_any(list_array.clone(), needle_array.clone()))
                })
            },
        );

        group.bench_with_input(
            BenchmarkId::new("none_match", check_size),
            &check_size,
            |b, &check_size| {
                let portfolio = (0..portfolio_size)
                    .map(|i| lit(format!("TICKER{i:04}")))
                    .collect::<Vec<_>>();
                let list_array = make_array(portfolio);

                let checking = (0..check_size)
                    .map(|i| lit(format!("NOTFOUND{i}")))
                    .collect::<Vec<_>>();
                let needle_array = make_array(checking);

                b.iter(|| {
                    black_box(array_has_any(list_array.clone(), needle_array.clone()))
                })
            },
        );
    }

    group.finish();
}

fn bench_array_has_edge_cases(c: &mut Criterion) {
    let mut group = c.benchmark_group("array_has_edge_cases");

    // Empty array
    group.bench_function("empty_array", |b| {
        let list_array = make_array(vec![]);
        let needle = lit(1_i64);

        b.iter(|| black_box(array_has(list_array.clone(), needle.clone())))
    });

    // Single element array - found
    group.bench_function("single_element_found", |b| {
        let list_array = make_array(vec![lit(1_i64)]);
        let needle = lit(1_i64);

        b.iter(|| black_box(array_has(list_array.clone(), needle.clone())))
    });

    // Single element array - not found
    group.bench_function("single_element_not_found", |b| {
        let list_array = make_array(vec![lit(1_i64)]);
        let needle = lit(2_i64);

        b.iter(|| black_box(array_has(list_array.clone(), needle.clone())))
    });

    // Array with duplicates
    group.bench_function("array_with_duplicates", |b| {
        let array = vec![lit(1_i64); 1000];
        let list_array = make_array(array);
        let needle = lit(1_i64);

        b.iter(|| black_box(array_has(list_array.clone(), needle.clone())))
    });

    // array_has_all: empty needle
    group.bench_function("array_has_all_empty_needle", |b| {
        let array = (0..1000).map(|i| lit(i as i64)).collect::<Vec<_>>();
        let list_array = make_array(array);
        let needle_array = make_array(vec![]);

        b.iter(|| black_box(array_has_all(list_array.clone(), needle_array.clone())))
    });

    // array_has_any: empty needle
    group.bench_function("array_has_any_empty_needle", |b| {
        let array = (0..1000).map(|i| lit(i as i64)).collect::<Vec<_>>();
        let list_array = make_array(array);
        let needle_array = make_array(vec![]);

        b.iter(|| black_box(array_has_any(list_array.clone(), needle_array.clone())))
    });

    group.finish();
}

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);
