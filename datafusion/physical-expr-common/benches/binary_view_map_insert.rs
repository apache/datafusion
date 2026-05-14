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

//! Bench for ArrowBytesViewMap insert_if_new on inline (len<=12) and
//! out-of-line (len>12) string arrays.
//!
//! The inline case exercises the bypass added in this PR: for strings
//! where len <= 12, the new-value branch now extracts bytes from the
//! view u128 directly rather than calling values.value(i).

use arrow::array::{ArrayRef, StringViewArray};
use criterion::{BenchmarkId, Criterion, criterion_group, criterion_main};
use datafusion_physical_expr_common::binary_map::OutputType;
use datafusion_physical_expr_common::binary_view_map::ArrowBytesViewSet;
use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};
use std::hint::black_box;
use std::sync::Arc;

/// Build an array of `n` distinct strings, each of `len` bytes.
/// All characters are ASCII lowercase letters.
fn make_string_view_array(n: usize, len: usize, seed: u64) -> ArrayRef {
    let mut rng = StdRng::seed_from_u64(seed);
    let alphabet: Vec<u8> = (b'a'..=b'z').collect();
    let strings: Vec<Option<String>> = (0..n)
        .map(|_| {
            let s: String = (0..len)
                .map(|_| alphabet[rng.random_range(0..alphabet.len())] as char)
                .collect();
            Some(s)
        })
        .collect();
    Arc::new(StringViewArray::from(strings))
}

fn bench_insert(c: &mut Criterion) {
    let mut group = c.benchmark_group("ArrowBytesViewSet_insert_if_new");

    // Sizes: number of distinct strings inserted per benchmark iteration.
    // Each iteration creates a fresh set and inserts all values (worst case:
    // all values are new, so every row hits the new-value branch).
    for &n in &[1_000usize, 10_000, 100_000] {
        // Inline strings: len=8 (well within the 12-byte inline threshold).
        let inline_array = make_string_view_array(n, 8, 42);
        group.bench_with_input(
            BenchmarkId::new("inline_len8", n),
            &inline_array,
            |b, arr| {
                b.iter(|| {
                    let mut set = ArrowBytesViewSet::new(OutputType::Utf8View);
                    set.insert(arr);
                    black_box(set.len());
                });
            },
        );

        // Out-of-line strings: len=64 (far above the 12-byte threshold).
        // Used as a control: this path is unchanged and should show no delta.
        let ool_array = make_string_view_array(n, 64, 42);
        group.bench_with_input(
            BenchmarkId::new("out_of_line_len64", n),
            &ool_array,
            |b, arr| {
                b.iter(|| {
                    let mut set = ArrowBytesViewSet::new(OutputType::Utf8View);
                    set.insert(arr);
                    black_box(set.len());
                });
            },
        );
    }

    group.finish();
}

criterion_group!(benches, bench_insert);
criterion_main!(benches);
