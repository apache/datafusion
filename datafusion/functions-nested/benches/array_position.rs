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

use arrow::array::{ArrayRef, Int64Array, ListArray};
use arrow::buffer::OffsetBuffer;
use arrow::datatypes::{DataType, Field};
use criterion::{
    criterion_group, criterion_main, {BenchmarkId, Criterion},
};
use datafusion_common::ScalarValue;
use datafusion_common::config::ConfigOptions;
use datafusion_expr::{ColumnarValue, ScalarFunctionArgs, ScalarUDFImpl};
use datafusion_functions_nested::position::ArrayPosition;
use rand::Rng;
use rand::SeedableRng;
use rand::rngs::StdRng;
use std::hint::black_box;
use std::sync::Arc;

const NUM_ROWS: usize = 10000;
const SEED: u64 = 42;
const NULL_DENSITY: f64 = 0.1;
const SENTINEL_NEEDLE: i64 = -1;

fn criterion_benchmark(c: &mut Criterion) {
    for size in [10, 100, 500] {
        bench_array_position(c, size);
    }
}

fn bench_array_position(c: &mut Criterion, array_size: usize) {
    let mut group = c.benchmark_group("array_position_i64");
    let haystack_found_once = create_haystack_with_sentinel(
        NUM_ROWS,
        array_size,
        NULL_DENSITY,
        SENTINEL_NEEDLE,
        0,
    );
    let haystack_found_many = create_haystack_with_sentinels(
        NUM_ROWS,
        array_size,
        NULL_DENSITY,
        SENTINEL_NEEDLE,
    );
    let haystack_not_found =
        create_haystack_without_sentinel(NUM_ROWS, array_size, NULL_DENSITY);
    let num_rows = haystack_not_found.len();
    let arg_fields: Vec<Arc<Field>> = vec![
        Field::new("haystack", haystack_not_found.data_type().clone(), false).into(),
        Field::new("needle", DataType::Int64, false).into(),
    ];
    let return_field: Arc<Field> = Field::new("result", DataType::UInt64, true).into();
    let config_options = Arc::new(ConfigOptions::default());
    let needle = ScalarValue::Int64(Some(SENTINEL_NEEDLE));

    // Benchmark: one match per row.
    let args_found_once = vec![
        ColumnarValue::Array(haystack_found_once.clone()),
        ColumnarValue::Scalar(needle.clone()),
    ];
    group.bench_with_input(
        BenchmarkId::new("found_once", array_size),
        &array_size,
        |b, _| {
            let udf = ArrayPosition::new();
            b.iter(|| {
                black_box(
                    udf.invoke_with_args(ScalarFunctionArgs {
                        args: args_found_once.clone(),
                        arg_fields: arg_fields.clone(),
                        number_rows: num_rows,
                        return_field: return_field.clone(),
                        config_options: config_options.clone(),
                    })
                    .unwrap(),
                )
            })
        },
    );

    // Benchmark: many matches per row.
    let args_found_many = vec![
        ColumnarValue::Array(haystack_found_many.clone()),
        ColumnarValue::Scalar(needle.clone()),
    ];
    group.bench_with_input(
        BenchmarkId::new("found_many", array_size),
        &array_size,
        |b, _| {
            let udf = ArrayPosition::new();
            b.iter(|| {
                black_box(
                    udf.invoke_with_args(ScalarFunctionArgs {
                        args: args_found_many.clone(),
                        arg_fields: arg_fields.clone(),
                        number_rows: num_rows,
                        return_field: return_field.clone(),
                        config_options: config_options.clone(),
                    })
                    .unwrap(),
                )
            })
        },
    );

    // Benchmark: needle is not found in any row.
    let args_not_found = vec![
        ColumnarValue::Array(haystack_not_found.clone()),
        ColumnarValue::Scalar(needle.clone()),
    ];
    group.bench_with_input(
        BenchmarkId::new("not_found", array_size),
        &array_size,
        |b, _| {
            let udf = ArrayPosition::new();
            b.iter(|| {
                black_box(
                    udf.invoke_with_args(ScalarFunctionArgs {
                        args: args_not_found.clone(),
                        arg_fields: arg_fields.clone(),
                        number_rows: num_rows,
                        return_field: return_field.clone(),
                        config_options: config_options.clone(),
                    })
                    .unwrap(),
                )
            })
        },
    );

    group.finish();
}

fn create_haystack_without_sentinel(
    num_rows: usize,
    array_size: usize,
    null_density: f64,
) -> ArrayRef {
    create_haystack_from_fn(num_rows, array_size, |_, _, rng| {
        random_haystack_value(rng, array_size, null_density)
    })
}

fn create_haystack_with_sentinel(
    num_rows: usize,
    array_size: usize,
    null_density: f64,
    sentinel: i64,
    sentinel_index: usize,
) -> ArrayRef {
    assert!(sentinel_index < array_size);

    create_haystack_from_fn(num_rows, array_size, |_, col, rng| {
        if col == sentinel_index {
            Some(sentinel)
        } else {
            random_haystack_value(rng, array_size, null_density)
        }
    })
}

fn create_haystack_with_sentinels(
    num_rows: usize,
    array_size: usize,
    null_density: f64,
    sentinel: i64,
) -> ArrayRef {
    create_haystack_from_fn(num_rows, array_size, |_, col, rng| {
        // Place the sentinel in half the positions to create many matches per row.
        if col % 2 == 0 {
            Some(sentinel)
        } else {
            random_haystack_value(rng, array_size, null_density)
        }
    })
}

fn create_haystack_from_fn<F>(
    num_rows: usize,
    array_size: usize,
    mut value_at: F,
) -> ArrayRef
where
    F: FnMut(usize, usize, &mut StdRng) -> Option<i64>,
{
    let mut rng = StdRng::seed_from_u64(SEED);
    let mut values = Vec::with_capacity(num_rows * array_size);
    for row in 0..num_rows {
        for col in 0..array_size {
            values.push(value_at(row, col, &mut rng));
        }
    }
    let values = values.into_iter().collect::<Int64Array>();
    let offsets = (0..=num_rows)
        .map(|i| (i * array_size) as i32)
        .collect::<Vec<i32>>();

    Arc::new(
        ListArray::try_new(
            Arc::new(Field::new("item", DataType::Int64, true)),
            OffsetBuffer::new(offsets.into()),
            Arc::new(values),
            None,
        )
        .unwrap(),
    )
}

fn random_haystack_value(
    rng: &mut StdRng,
    array_size: usize,
    null_density: f64,
) -> Option<i64> {
    if rng.random::<f64>() < null_density {
        None
    } else {
        Some(rng.random_range(0..array_size as i64))
    }
}

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);
