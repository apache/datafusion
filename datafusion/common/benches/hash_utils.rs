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

use ahash::RandomState;
use arrow::array::{ArrayRef, Int32Array, StringArray};
use criterion::{black_box, criterion_group, criterion_main, Criterion};
use datafusion_common::hash_utils::create_hashes;
use std::sync::Arc;

/// Benchmark for create_hashes function that demonstrates the Arc cloning
/// reduction benefit of the Hash API refactor (PR #18433).
///
/// The refactor changes create_hashes_from_arrays to accept &[&dyn Array]
/// instead of &[ArrayRef], avoiding unnecessary Arc cloning at call sites.
/// Internal functions now use create_hashes_from_arrays directly with
/// array references, eliminating Arc::clone() calls for dictionary values,
/// struct columns, map entries, and list/array values.

fn create_test_string_array(size: usize) -> ArrayRef {
    let arr: StringArray = (0..size)
        .map(|i| Some(format!("string_{:04}", i % 100)))
        .collect();
    Arc::new(arr)
}

fn create_test_int_array(size: usize) -> ArrayRef {
    let arr: Int32Array = (0..size).map(|i| Some(i as i32)).collect();
    Arc::new(arr)
}

/// Benchmark hashing with single string column
fn benchmark_hash_single_string_column(c: &mut Criterion) {
    let mut group = c.benchmark_group("hash_single_string");

    for size in [1024, 10_000, 100_000].iter() {
        group.bench_function(format!("create_hashes_string_{}", size), |b| {
            let array = create_test_string_array(*size);
            let random_state = RandomState::with_seeds(0, 0, 0, 0);
            let size_copy = *size;

            b.iter(|| {
                let mut hashes = vec![0u64; size_copy];
                let _ = create_hashes(
                    &[array.clone()],
                    &random_state,
                    &mut hashes,
                );
                hashes
            })
        });
    }

    group.finish();
}

/// Benchmark hashing with multiple columns (string + int)
fn benchmark_hash_multi_column(c: &mut Criterion) {
    let mut group = c.benchmark_group("hash_multi_column");

    for size in [1024, 10_000, 100_000].iter() {
        group.bench_function(format!("create_hashes_multi_column_{}", size), |b| {
            let string_array = create_test_string_array(*size);
            let int_array = create_test_int_array(*size);
            let random_state = RandomState::with_seeds(0, 0, 0, 0);
            let size_copy = *size;

            b.iter(|| {
                let mut hashes = vec![0u64; size_copy];
                let _ = create_hashes(
                    &[string_array.clone(), int_array.clone()],
                    &random_state,
                    &mut hashes
                );
                hashes
            })
        });
    }

    group.finish();
}

/// Benchmark hashing with integer arrays
/// Tests performance with primitive type arrays
fn benchmark_hash_integers(c: &mut Criterion) {
    let mut group = c.benchmark_group("hash_integers");

    for size in [1024, 10_000, 100_000].iter() {
        group.bench_function(format!("create_hashes_integers_{}", size), |b| {
            let array = create_test_int_array(*size);
            let random_state = RandomState::with_seeds(0, 0, 0, 0);
            let size_copy = *size;

            b.iter(|| {
                let mut hashes = vec![0u64; size_copy];
                let _ = create_hashes(
                    &[array.clone()],
                    &random_state,
                    &mut hashes,
                );
                hashes
            })
        });
    }

    group.finish();
}

criterion_group!(
    benches,
    benchmark_hash_single_string_column,
    benchmark_hash_multi_column,
    benchmark_hash_integers
);
criterion_main!(benches);
