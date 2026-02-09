// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not this file except in compliance
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

#[macro_use]
extern crate criterion;

use arrow::array::{ArrayRef, Int64Array, ListArray};
use arrow::buffer::OffsetBuffer;
use arrow::datatypes::{DataType, Field};
use criterion::{BenchmarkId, Criterion};
use datafusion_common::config::ConfigOptions;
use datafusion_expr::{ColumnarValue, ScalarFunctionArgs, ScalarUDFImpl};
use datafusion_functions_nested::set_ops::{ArrayUnion, ArrayIntersect};
use rand::Rng;
use rand::SeedableRng;
use rand::rngs::StdRng;
use std::hint::black_box;
use std::sync::Arc;

const NUM_ROWS: usize = 1000;
const ARRAY_SIZES: &[usize] = &[10, 50, 100];
const SEED: u64 = 42;

fn criterion_benchmark(c: &mut Criterion) {
    bench_array_union(c);
    bench_array_intersect(c);
}

fn invoke_array_union(udf: &impl ScalarUDFImpl, array1: &ArrayRef, array2: &ArrayRef) {
    let args = vec![
        ColumnarValue::Array(array1.clone()),
        ColumnarValue::Array(array2.clone()),
    ];
    black_box(
        udf.invoke_with_args(ScalarFunctionArgs {
            args,
            arg_fields: vec![
                Field::new("arr1", array1.data_type().clone(), false).into(),
                Field::new("arr2", array2.data_type().clone(), false).into(),
            ],
            number_rows: NUM_ROWS,
            return_field: Field::new("result", array1.data_type().clone(), false).into(),
            config_options: Arc::new(ConfigOptions::default()),
        })
        .unwrap(),
    );
}

fn invoke_array_intersect(udf: &impl ScalarUDFImpl, array1: &ArrayRef, array2: &ArrayRef) {
    let args = vec![
        ColumnarValue::Array(array1.clone()),
        ColumnarValue::Array(array2.clone()),
    ];
    black_box(
        udf.invoke_with_args(ScalarFunctionArgs {
            args,
            arg_fields: vec![
                Field::new("arr1", array1.data_type().clone(), false).into(),
                Field::new("arr2", array2.data_type().clone(), false).into(),
            ],
            number_rows: NUM_ROWS,
            return_field: Field::new("result", array1.data_type().clone(), false).into(),
            config_options: Arc::new(ConfigOptions::default()),
        })
        .unwrap(),
    );
}

fn bench_array_union(c: &mut Criterion) {
    let mut group = c.benchmark_group("array_union");
    let udf = ArrayUnion::new();

    for &array_size in ARRAY_SIZES {
        let (array1, array2) = create_arrays_with_overlap(NUM_ROWS, array_size, 0.8);
        group.bench_with_input(
            BenchmarkId::new("high_overlap", array_size),
            &array_size,
            |b, _| b.iter(|| invoke_array_union(&udf, &array1, &array2)),
        );
    }

    for &array_size in ARRAY_SIZES {
        let (array1, array2) = create_arrays_with_overlap(NUM_ROWS, array_size, 0.2);
        group.bench_with_input(
            BenchmarkId::new("low_overlap", array_size),
            &array_size,
            |b, _| b.iter(|| invoke_array_union(&udf, &array1, &array2)),
        );
    }

    group.finish();
}

fn bench_array_intersect(c: &mut Criterion) {
    let mut group = c.benchmark_group("array_intersect");
    let udf = ArrayIntersect::new();

    for &array_size in ARRAY_SIZES {
        let (array1, array2) = create_arrays_with_overlap(NUM_ROWS, array_size, 0.8);
        group.bench_with_input(
            BenchmarkId::new("high_overlap", array_size),
            &array_size,
            |b, _| b.iter(|| invoke_array_intersect(&udf, &array1, &array2)),
        );
    }

    for &array_size in ARRAY_SIZES {
        let (array1, array2) = create_arrays_with_overlap(NUM_ROWS, array_size, 0.2);
        group.bench_with_input(
            BenchmarkId::new("low_overlap", array_size),
            &array_size,
            |b, _| b.iter(|| invoke_array_intersect(&udf, &array1, &array2)),
        );
    }

    group.finish();
}

fn create_arrays_with_overlap(num_rows: usize, array_size: usize, overlap_ratio: f64) -> (ArrayRef, ArrayRef) {
    let mut rng = StdRng::seed_from_u64(SEED);

    let values1 = (0..num_rows * array_size)
        .map(|_| Some(rng.random_range(0..(array_size * 10) as i64)))
        .collect::<Int64Array>();
    let offsets1 = (0..=num_rows)
        .map(|i| (i * array_size) as i32)
        .collect::<Vec<i32>>();

    let mut values2 = Vec::new();
    for row in 0..num_rows {
        let start = row * array_size;
        let _end = start + array_size;
        let overlap_count = (array_size as f64 * overlap_ratio) as usize;

        for i in 0..overlap_count {
            values2.push(values1.value(start + i));
        }
        for _ in overlap_count..array_size {
            values2.push(rng.random_range(0..(array_size * 10) as i64));
        }
    }

    let values2 = Int64Array::from(values2);
    let offsets2 = (0..=num_rows)
        .map(|i| (i * array_size) as i32)
        .collect::<Vec<i32>>();

    let array1 = Arc::new(
        ListArray::try_new(
            Arc::new(Field::new("item", DataType::Int64, true)),
            OffsetBuffer::new(offsets1.into()),
            Arc::new(values1),
            None,
        )
        .unwrap(),
    );

    let array2 = Arc::new(
        ListArray::try_new(
            Arc::new(Field::new("item", DataType::Int64, true)),
            OffsetBuffer::new(offsets2.into()),
            Arc::new(values2),
            None,
        )
        .unwrap(),
    );

    (array1, array2)
}

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);
