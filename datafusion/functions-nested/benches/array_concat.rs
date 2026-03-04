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

use std::hint::black_box;
use std::sync::Arc;

use arrow::array::{ArrayRef, Int32Array, ListArray};
use arrow::buffer::{NullBuffer, OffsetBuffer, ScalarBuffer};
use arrow::datatypes::{DataType, Field};
use criterion::{BenchmarkId, Criterion, criterion_group, criterion_main};
use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};

use datafusion_functions_nested::concat::array_concat_inner;

const SEED: u64 = 42;

/// Build a `ListArray<i32>` with `num_lists` rows, each containing
/// `elements_per_list` random i32 values. Every 10th row is null.
fn make_list_array(
    rng: &mut StdRng,
    num_lists: usize,
    elements_per_list: usize,
) -> ArrayRef {
    let total_values = num_lists * elements_per_list;
    let values: Vec<i32> = (0..total_values).map(|_| rng.random()).collect();
    let values = Arc::new(Int32Array::from(values));

    let offsets: Vec<i32> = (0..=num_lists)
        .map(|i| (i * elements_per_list) as i32)
        .collect();
    let offsets = OffsetBuffer::new(ScalarBuffer::from(offsets));

    let nulls: Vec<bool> = (0..num_lists).map(|i| i % 10 != 0).collect();
    let nulls = Some(NullBuffer::from(nulls));

    Arc::new(ListArray::new(
        Arc::new(Field::new("item", DataType::Int32, false)),
        offsets,
        values,
        nulls,
    ))
}

fn criterion_benchmark(c: &mut Criterion) {
    let mut group = c.benchmark_group("array_concat");

    // Benchmark: varying number of rows, 20 elements per list
    for num_rows in [100, 1000, 10000] {
        let mut rng = StdRng::seed_from_u64(SEED);
        let list_a = make_list_array(&mut rng, num_rows, 20);
        let list_b = make_list_array(&mut rng, num_rows, 20);
        let args: Vec<ArrayRef> = vec![list_a, list_b];

        group.bench_with_input(BenchmarkId::new("rows", num_rows), &args, |b, args| {
            b.iter(|| black_box(array_concat_inner(args).unwrap()));
        });
    }

    // Benchmark: 1000 rows, varying element counts per list
    for elements_per_list in [5, 50, 500] {
        let mut rng = StdRng::seed_from_u64(SEED);
        let list_a = make_list_array(&mut rng, 1000, elements_per_list);
        let list_b = make_list_array(&mut rng, 1000, elements_per_list);
        let args: Vec<ArrayRef> = vec![list_a, list_b];

        group.bench_with_input(
            BenchmarkId::new("elements_per_list", elements_per_list),
            &args,
            |b, args| {
                b.iter(|| black_box(array_concat_inner(args).unwrap()));
            },
        );
    }

    group.finish();
}

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);
