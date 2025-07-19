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

use arrow::array::{Array, ArrayRef};
use arrow::datatypes::StringViewType;
use arrow::util::bench_util::{
    create_string_view_array_with_len, create_string_view_array_with_max_len,
};
use criterion::{criterion_group, criterion_main, Criterion};
use datafusion_physical_plan::aggregates::group_values::multi_group_by::bytes_view::ByteViewGroupValueBuilder;
use datafusion_physical_plan::aggregates::group_values::multi_group_by::GroupColumn;
use std::sync::Arc;

const SIZES: [usize; 3] = [1_000, 10_000, 100_000];
const NULL_DENSITIES: [f32; 3] = [0.0, 0.1, 0.5];

fn bench_vectorized_append(c: &mut Criterion) {
    let mut group = c.benchmark_group("ByteViewGroupValueBuilder_vectorized_append");

    // Inlined-only scenarios
    for &size in &SIZES {
        for &null_density in &NULL_DENSITIES {
            let input = create_string_view_array_with_len(size, null_density, 8, false);
            let input: ArrayRef = Arc::new(input);
            let id = format!("inlined_null_{null_density:.1}_size_{size}");
            group.bench_function(id, |b| {
                b.iter(|| {
                    let mut builder = ByteViewGroupValueBuilder::<StringViewType>::new();
                    builder
                        .vectorized_append(&input, &(0..input.len()).collect::<Vec<_>>())
                        .unwrap();
                });
            });
        }
    }

    // Mixed-length scenarios
    for &size in &SIZES {
        for &null_density in &NULL_DENSITIES {
            let input = create_string_view_array_with_len(size, null_density, 64, true);
            let input: ArrayRef = Arc::new(input);
            let id = format!("mixed_null_{null_density:.1}_size_{size}");
            group.bench_function(id, |b| {
                b.iter(|| {
                    let mut builder = ByteViewGroupValueBuilder::<StringViewType>::new();
                    builder
                        .vectorized_append(&input, &(0..input.len()).collect::<Vec<_>>())
                        .unwrap();
                });
            });
        }
    }

    // Random max-length scenarios
    for &size in &SIZES {
        for &null_density in &NULL_DENSITIES {
            let input = create_string_view_array_with_max_len(size, null_density, 400);
            let input: ArrayRef = Arc::new(input);
            let id = format!("random_null_{null_density:.1}_size_{size}");
            group.bench_function(id, |b| {
                b.iter(|| {
                    let mut builder = ByteViewGroupValueBuilder::<StringViewType>::new();
                    builder
                        .vectorized_append(&input, &(0..input.len()).collect::<Vec<_>>())
                        .unwrap();
                });
            });
        }
    }

    group.finish();
}

criterion_group!(benches, bench_vectorized_append);
criterion_main!(benches);
