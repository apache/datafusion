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

use arrow::array::ArrayRef;
use arrow::datatypes::StringViewType;
use arrow::util::bench_util::{
    create_string_view_array_with_len, create_string_view_array_with_max_len,
};
use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion};
use datafusion_physical_plan::aggregates::group_values::multi_group_by::bytes_view::ByteViewGroupValueBuilder;
use datafusion_physical_plan::aggregates::group_values::multi_group_by::GroupColumn;
use std::sync::Arc;

const SIZES: [usize; 3] = [1_000, 10_000, 100_000];
const NULL_DENSITIES: [f32; 3] = [0.0, 0.1, 0.5];

fn bench_vectorized_append(c: &mut Criterion) {
    let mut group = c.benchmark_group("ByteViewGroupValueBuilder_vectorized_append");

    for &size in &SIZES {
        let rows: Vec<usize> = (0..size).collect();

        for &null_density in &NULL_DENSITIES {
            let input = create_string_view_array_with_len(size, null_density, 8, false);
            let input: ArrayRef = Arc::new(input);

            // vectorized_append
            let id = BenchmarkId::new(
                format!("inlined_null_{null_density:.1}_size_{size}"),
                "vectorized_append",
            );
            group.bench_function(id, |b| {
                b.iter(|| {
                    let mut builder = ByteViewGroupValueBuilder::<StringViewType>::new();
                    builder.vectorized_append(&input, &rows).unwrap();
                });
            });

            // append_val
            let id = BenchmarkId::new(
                format!("inlined_null_{null_density:.1}_size_{size}"),
                "append_val",
            );
            group.bench_function(id, |b| {
                b.iter(|| {
                    let mut builder = ByteViewGroupValueBuilder::<StringViewType>::new();
                    for &i in &rows {
                        builder.append_val(&input, i).unwrap();
                    }
                });
            });

            // vectorized_equal_to
            let id = BenchmarkId::new(
                format!("inlined_null_{null_density:.1}_size_{size}"),
                "vectorized_equal_to",
            );
            group.bench_function(id, |b| {
                let mut builder = ByteViewGroupValueBuilder::<StringViewType>::new();
                builder.vectorized_append(&input, &rows).unwrap();
                let mut results = vec![true; size];
                b.iter(|| {
                    builder.vectorized_equal_to(&rows, &input, &rows, &mut results);
                });
            });
        }
    }

    for &size in &SIZES {
        let rows: Vec<usize> = (0..size).collect();

        for &null_density in &NULL_DENSITIES {
            let scenario = "mixed";
            let input = create_string_view_array_with_len(size, null_density, 64, true);
            let input: ArrayRef = Arc::new(input);

            // vectorized_append
            let id = BenchmarkId::new(
                format!("{scenario}_null_{null_density:.1}_size_{size}"),
                "vectorized_append",
            );
            group.bench_function(id, |b| {
                b.iter(|| {
                    let mut builder = ByteViewGroupValueBuilder::<StringViewType>::new();
                    builder.vectorized_append(&input, &rows).unwrap();
                });
            });

            // append_val
            let id = BenchmarkId::new(
                format!("{scenario}_null_{null_density:.1}_size_{size}"),
                "append_val",
            );
            group.bench_function(id, |b| {
                b.iter(|| {
                    let mut builder = ByteViewGroupValueBuilder::<StringViewType>::new();
                    for &i in &rows {
                        builder.append_val(&input, i).unwrap();
                    }
                });
            });

            // vectorized_equal_to
            let id = BenchmarkId::new(
                format!("{scenario}_null_{null_density:.1}_size_{size}"),
                "vectorized_equal_to",
            );
            group.bench_function(id, |b| {
                let mut builder = ByteViewGroupValueBuilder::<StringViewType>::new();
                builder.vectorized_append(&input, &rows).unwrap();
                let mut results = vec![true; size];
                b.iter(|| {
                    builder.vectorized_equal_to(&rows, &input, &rows, &mut results);
                });
            });
        }
    }

    for &size in &SIZES {
        let rows: Vec<usize> = (0..size).collect();

        for &null_density in &NULL_DENSITIES {
            let scenario = "random";
            let input = create_string_view_array_with_max_len(size, null_density, 400);
            let input: ArrayRef = Arc::new(input);

            // vectorized_append
            let id = BenchmarkId::new(
                format!("{scenario}_null_{null_density:.1}_size_{size}"),
                "vectorized_append",
            );
            group.bench_function(id, |b| {
                b.iter(|| {
                    let mut builder = ByteViewGroupValueBuilder::<StringViewType>::new();
                    builder.vectorized_append(&input, &rows).unwrap();
                });
            });

            // append_val
            let id = BenchmarkId::new(
                format!("{scenario}_null_{null_density:.1}_size_{size}"),
                "append_val",
            );
            group.bench_function(id, |b| {
                b.iter(|| {
                    let mut builder = ByteViewGroupValueBuilder::<StringViewType>::new();
                    for &i in &rows {
                        builder.append_val(&input, i).unwrap();
                    }
                });
            });

            // vectorized_equal_to
            let id = BenchmarkId::new(
                format!("{scenario}_null_{null_density:.1}_size_{size}"),
                "vectorized_equal_to",
            );
            group.bench_function(id, |b| {
                let mut builder = ByteViewGroupValueBuilder::<StringViewType>::new();
                builder.vectorized_append(&input, &rows).unwrap();
                let mut results = vec![true; size];
                b.iter(|| {
                    builder.vectorized_equal_to(&rows, &input, &rows, &mut results);
                });
            });
        }
    }

    group.finish();
}

criterion_group!(benches, bench_vectorized_append);
criterion_main!(benches);
