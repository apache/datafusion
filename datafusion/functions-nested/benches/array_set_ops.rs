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

#[macro_use]
extern crate criterion;

use arrow::array::{ArrayRef, Int64Array, ListArray};
use arrow::buffer::OffsetBuffer;
use arrow::datatypes::{DataType, Field};
use criterion::{BenchmarkId, Criterion};
use datafusion_common::config::ConfigOptions;
use datafusion_expr::{ColumnarValue, ScalarFunctionArgs, ScalarUDFImpl};
use datafusion_functions_nested::set_ops::{ArrayIntersect, ArrayUnion};
use rand::SeedableRng;
use rand::prelude::SliceRandom;
use rand::rngs::StdRng;
use std::collections::HashSet;
use std::hint::black_box;
use std::sync::Arc;

const NUM_ROWS: usize = 1000;
const ARRAY_SIZES: &[usize] = &[10, 50, 100];
const SEED: u64 = 42;

fn criterion_benchmark(c: &mut Criterion) {
    bench_array_union(c);
    bench_array_intersect(c);
}

fn invoke_udf(udf: &impl ScalarUDFImpl, array1: &ArrayRef, array2: &ArrayRef) {
    black_box(
        udf.invoke_with_args(ScalarFunctionArgs {
            args: vec![
                ColumnarValue::Array(array1.clone()),
                ColumnarValue::Array(array2.clone()),
            ],
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

    for (overlap_label, overlap_ratio) in &[("high_overlap", 0.8), ("low_overlap", 0.2)] {
        for &array_size in ARRAY_SIZES {
            let (array1, array2) =
                create_arrays_with_overlap(NUM_ROWS, array_size, *overlap_ratio);
            group.bench_with_input(
                BenchmarkId::new(*overlap_label, array_size),
                &array_size,
                |b, _| b.iter(|| invoke_udf(&udf, &array1, &array2)),
            );
        }
    }

    group.finish();
}

fn bench_array_intersect(c: &mut Criterion) {
    let mut group = c.benchmark_group("array_intersect");
    let udf = ArrayIntersect::new();

    for (overlap_label, overlap_ratio) in &[("high_overlap", 0.8), ("low_overlap", 0.2)] {
        for &array_size in ARRAY_SIZES {
            let (array1, array2) =
                create_arrays_with_overlap(NUM_ROWS, array_size, *overlap_ratio);
            group.bench_with_input(
                BenchmarkId::new(*overlap_label, array_size),
                &array_size,
                |b, _| b.iter(|| invoke_udf(&udf, &array1, &array2)),
            );
        }
    }

    group.finish();
}

fn create_arrays_with_overlap(
    num_rows: usize,
    array_size: usize,
    overlap_ratio: f64,
) -> (ArrayRef, ArrayRef) {
    assert!((0.0..=1.0).contains(&overlap_ratio));
    let overlap_count = ((array_size as f64) * overlap_ratio).round() as usize;

    let mut rng = StdRng::seed_from_u64(SEED);

    let mut values1 = Vec::with_capacity(num_rows * array_size);
    let mut values2 = Vec::with_capacity(num_rows * array_size);

    for row in 0..num_rows {
        let base = (row as i64) * (array_size as i64) * 2;

        for i in 0..array_size {
            values1.push(base + i as i64);
        }

        let mut positions: Vec<usize> = (0..array_size).collect();
        positions.shuffle(&mut rng);

        let overlap_positions: HashSet<_> =
            positions[..overlap_count].iter().copied().collect();

        for i in 0..array_size {
            if overlap_positions.contains(&i) {
                values2.push(base + i as i64);
            } else {
                values2.push(base + array_size as i64 + i as i64);
            }
        }
    }

    let values1 = Int64Array::from(values1);
    let values2 = Int64Array::from(values2);

    let field = Arc::new(Field::new("item", DataType::Int64, true));

    let offsets = (0..=num_rows)
        .map(|i| (i * array_size) as i32)
        .collect::<Vec<i32>>();

    let array1 = Arc::new(
        ListArray::try_new(
            field.clone(),
            OffsetBuffer::new(offsets.clone().into()),
            Arc::new(values1),
            None,
        )
        .unwrap(),
    );

    let array2 = Arc::new(
        ListArray::try_new(
            field,
            OffsetBuffer::new(offsets.into()),
            Arc::new(values2),
            None,
        )
        .unwrap(),
    );

    (array1, array2)
}

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);
