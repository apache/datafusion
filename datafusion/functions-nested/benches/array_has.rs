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

use arrow::array::{ArrayRef, Int64Array, ListArray, StringArray};
use arrow::buffer::OffsetBuffer;
use arrow::datatypes::{DataType, Field};
use criterion::{
    criterion_group, criterion_main, {BenchmarkId, Criterion},
};
use datafusion_common::ScalarValue;
use datafusion_common::config::ConfigOptions;
use datafusion_expr::{ColumnarValue, ScalarFunctionArgs, ScalarUDFImpl};
use datafusion_functions_nested::array_has::{ArrayHas, ArrayHasAll, ArrayHasAny};
use rand::Rng;
use rand::SeedableRng;
use rand::rngs::StdRng;
use std::hint::black_box;
use std::sync::Arc;

const NUM_ROWS: usize = 10000;
const SEED: u64 = 42;
const NULL_DENSITY: f64 = 0.1;
const NEEDLE_SIZE: usize = 3;

// If not explicitly stated, `array` and `array_size` refer to the haystack array.
fn criterion_benchmark(c: &mut Criterion) {
    // Test different array sizes
    let array_sizes = vec![10, 100, 500];

    for &size in &array_sizes {
        bench_array_has(c, size);
        bench_array_has_all(c, size);
        bench_array_has_any(c, size);
    }

    // Specific benchmarks for string arrays (common use case)
    bench_array_has_strings(c);
    bench_array_has_all_strings(c);
    bench_array_has_any_strings(c);
}

fn bench_array_has(c: &mut Criterion, array_size: usize) {
    let mut group = c.benchmark_group("array_has_i64");
    let list_array = create_int64_list_array(NUM_ROWS, array_size, NULL_DENSITY);
    let config_options = Arc::new(ConfigOptions::default());
    let return_field: Arc<Field> = Field::new("result", DataType::Boolean, true).into();
    let arg_fields: Vec<Arc<Field>> = vec![
        Field::new("arr", list_array.data_type().clone(), false).into(),
        Field::new("el", DataType::Int64, false).into(),
    ];

    // Benchmark: element found
    let args_found = vec![
        ColumnarValue::Array(list_array.clone()),
        ColumnarValue::Scalar(ScalarValue::Int64(Some(1))),
    ];
    group.bench_with_input(
        BenchmarkId::new("found", array_size),
        &array_size,
        |b, _| {
            let udf = ArrayHas::new();
            b.iter(|| {
                black_box(
                    udf.invoke_with_args(ScalarFunctionArgs {
                        args: args_found.clone(),
                        arg_fields: arg_fields.clone(),
                        number_rows: NUM_ROWS,
                        return_field: return_field.clone(),
                        config_options: config_options.clone(),
                    })
                    .unwrap(),
                )
            })
        },
    );

    // Benchmark: element not found
    let args_not_found = vec![
        ColumnarValue::Array(list_array.clone()),
        ColumnarValue::Scalar(ScalarValue::Int64(Some(-999))),
    ];
    group.bench_with_input(
        BenchmarkId::new("not_found", array_size),
        &array_size,
        |b, _| {
            let udf = ArrayHas::new();
            b.iter(|| {
                black_box(
                    udf.invoke_with_args(ScalarFunctionArgs {
                        args: args_not_found.clone(),
                        arg_fields: arg_fields.clone(),
                        number_rows: NUM_ROWS,
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

fn bench_array_has_all(c: &mut Criterion, array_size: usize) {
    let mut group = c.benchmark_group("array_has_all");
    let haystack = create_int64_list_array(NUM_ROWS, array_size, NULL_DENSITY);
    let list_type = haystack.data_type().clone();
    let config_options = Arc::new(ConfigOptions::default());
    let return_field: Arc<Field> = Field::new("result", DataType::Boolean, true).into();
    let arg_fields: Vec<Arc<Field>> = vec![
        Field::new("haystack", list_type.clone(), false).into(),
        Field::new("needle", list_type.clone(), false).into(),
    ];

    // Benchmark: all elements found (small needle)
    let needle_found = create_int64_list_array(NUM_ROWS, NEEDLE_SIZE, 0.0);
    let args_found = vec![
        ColumnarValue::Array(haystack.clone()),
        ColumnarValue::Array(needle_found),
    ];
    group.bench_with_input(
        BenchmarkId::new("all_found_small_needle", array_size),
        &array_size,
        |b, _| {
            let udf = ArrayHasAll::new();
            b.iter(|| {
                black_box(
                    udf.invoke_with_args(ScalarFunctionArgs {
                        args: args_found.clone(),
                        arg_fields: arg_fields.clone(),
                        number_rows: NUM_ROWS,
                        return_field: return_field.clone(),
                        config_options: config_options.clone(),
                    })
                    .unwrap(),
                )
            })
        },
    );

    // Benchmark: not all found (needle contains elements outside haystack range)
    let needle_missing =
        create_int64_list_array_with_offset(NUM_ROWS, NEEDLE_SIZE, array_size as i64);
    let args_missing = vec![
        ColumnarValue::Array(haystack.clone()),
        ColumnarValue::Array(needle_missing),
    ];
    group.bench_with_input(
        BenchmarkId::new("not_all_found", array_size),
        &array_size,
        |b, _| {
            let udf = ArrayHasAll::new();
            b.iter(|| {
                black_box(
                    udf.invoke_with_args(ScalarFunctionArgs {
                        args: args_missing.clone(),
                        arg_fields: arg_fields.clone(),
                        number_rows: NUM_ROWS,
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

fn bench_array_has_any(c: &mut Criterion, array_size: usize) {
    let mut group = c.benchmark_group("array_has_any");
    let haystack = create_int64_list_array(NUM_ROWS, array_size, NULL_DENSITY);
    let list_type = haystack.data_type().clone();
    let config_options = Arc::new(ConfigOptions::default());
    let return_field: Arc<Field> = Field::new("result", DataType::Boolean, true).into();
    let arg_fields: Vec<Arc<Field>> = vec![
        Field::new("haystack", list_type.clone(), false).into(),
        Field::new("needle", list_type.clone(), false).into(),
    ];

    // Benchmark: some elements match
    let needle_match = create_int64_list_array(NUM_ROWS, NEEDLE_SIZE, 0.0);
    let args_match = vec![
        ColumnarValue::Array(haystack.clone()),
        ColumnarValue::Array(needle_match),
    ];
    group.bench_with_input(
        BenchmarkId::new("some_match", array_size),
        &array_size,
        |b, _| {
            let udf = ArrayHasAny::new();
            b.iter(|| {
                black_box(
                    udf.invoke_with_args(ScalarFunctionArgs {
                        args: args_match.clone(),
                        arg_fields: arg_fields.clone(),
                        number_rows: NUM_ROWS,
                        return_field: return_field.clone(),
                        config_options: config_options.clone(),
                    })
                    .unwrap(),
                )
            })
        },
    );

    // Benchmark: no match
    let needle_no_match =
        create_int64_list_array_with_offset(NUM_ROWS, NEEDLE_SIZE, array_size as i64);
    let args_no_match = vec![
        ColumnarValue::Array(haystack.clone()),
        ColumnarValue::Array(needle_no_match),
    ];
    group.bench_with_input(
        BenchmarkId::new("no_match", array_size),
        &array_size,
        |b, _| {
            let udf = ArrayHasAny::new();
            b.iter(|| {
                black_box(
                    udf.invoke_with_args(ScalarFunctionArgs {
                        args: args_no_match.clone(),
                        arg_fields: arg_fields.clone(),
                        number_rows: NUM_ROWS,
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

fn bench_array_has_strings(c: &mut Criterion) {
    let mut group = c.benchmark_group("array_has_strings");
    let config_options = Arc::new(ConfigOptions::default());
    let return_field: Arc<Field> = Field::new("result", DataType::Boolean, true).into();

    let sizes = vec![10, 100, 500];

    for &size in &sizes {
        let list_array = create_string_list_array(NUM_ROWS, size, NULL_DENSITY);
        let arg_fields: Vec<Arc<Field>> = vec![
            Field::new("arr", list_array.data_type().clone(), false).into(),
            Field::new("el", DataType::Utf8, false).into(),
        ];

        let args_found = vec![
            ColumnarValue::Array(list_array.clone()),
            ColumnarValue::Scalar(ScalarValue::Utf8(Some("value_1".to_string()))),
        ];
        group.bench_with_input(BenchmarkId::new("found", size), &size, |b, _| {
            let udf = ArrayHas::new();
            b.iter(|| {
                black_box(
                    udf.invoke_with_args(ScalarFunctionArgs {
                        args: args_found.clone(),
                        arg_fields: arg_fields.clone(),
                        number_rows: NUM_ROWS,
                        return_field: return_field.clone(),
                        config_options: config_options.clone(),
                    })
                    .unwrap(),
                )
            })
        });

        let args_not_found = vec![
            ColumnarValue::Array(list_array.clone()),
            ColumnarValue::Scalar(ScalarValue::Utf8(Some("NOTFOUND".to_string()))),
        ];
        group.bench_with_input(BenchmarkId::new("not_found", size), &size, |b, _| {
            let udf = ArrayHas::new();
            b.iter(|| {
                black_box(
                    udf.invoke_with_args(ScalarFunctionArgs {
                        args: args_not_found.clone(),
                        arg_fields: arg_fields.clone(),
                        number_rows: NUM_ROWS,
                        return_field: return_field.clone(),
                        config_options: config_options.clone(),
                    })
                    .unwrap(),
                )
            })
        });
    }

    group.finish();
}

fn bench_array_has_all_strings(c: &mut Criterion) {
    let mut group = c.benchmark_group("array_has_all_strings");
    let config_options = Arc::new(ConfigOptions::default());
    let return_field: Arc<Field> = Field::new("result", DataType::Boolean, true).into();

    let sizes = vec![10, 100, 500];

    for &size in &sizes {
        let haystack = create_string_list_array(NUM_ROWS, size, NULL_DENSITY);
        let list_type = haystack.data_type().clone();
        let arg_fields: Vec<Arc<Field>> = vec![
            Field::new("haystack", list_type.clone(), false).into(),
            Field::new("needle", list_type.clone(), false).into(),
        ];

        let needle_found = create_string_list_array(NUM_ROWS, NEEDLE_SIZE, 0.0);
        let args_found = vec![
            ColumnarValue::Array(haystack.clone()),
            ColumnarValue::Array(needle_found),
        ];
        group.bench_with_input(BenchmarkId::new("all_found", size), &size, |b, _| {
            let udf = ArrayHasAll::new();
            b.iter(|| {
                black_box(
                    udf.invoke_with_args(ScalarFunctionArgs {
                        args: args_found.clone(),
                        arg_fields: arg_fields.clone(),
                        number_rows: NUM_ROWS,
                        return_field: return_field.clone(),
                        config_options: config_options.clone(),
                    })
                    .unwrap(),
                )
            })
        });

        let needle_missing =
            create_string_list_array_with_prefix(NUM_ROWS, NEEDLE_SIZE, "missing_");
        let args_missing = vec![
            ColumnarValue::Array(haystack.clone()),
            ColumnarValue::Array(needle_missing),
        ];
        group.bench_with_input(BenchmarkId::new("not_all_found", size), &size, |b, _| {
            let udf = ArrayHasAll::new();
            b.iter(|| {
                black_box(
                    udf.invoke_with_args(ScalarFunctionArgs {
                        args: args_missing.clone(),
                        arg_fields: arg_fields.clone(),
                        number_rows: NUM_ROWS,
                        return_field: return_field.clone(),
                        config_options: config_options.clone(),
                    })
                    .unwrap(),
                )
            })
        });
    }

    group.finish();
}

fn bench_array_has_any_strings(c: &mut Criterion) {
    let mut group = c.benchmark_group("array_has_any_strings");
    let config_options = Arc::new(ConfigOptions::default());
    let return_field: Arc<Field> = Field::new("result", DataType::Boolean, true).into();

    let sizes = vec![10, 100, 500];

    for &size in &sizes {
        let haystack = create_string_list_array(NUM_ROWS, size, NULL_DENSITY);
        let list_type = haystack.data_type().clone();
        let arg_fields: Vec<Arc<Field>> = vec![
            Field::new("haystack", list_type.clone(), false).into(),
            Field::new("needle", list_type.clone(), false).into(),
        ];

        let needle_match = create_string_list_array(NUM_ROWS, NEEDLE_SIZE, 0.0);
        let args_match = vec![
            ColumnarValue::Array(haystack.clone()),
            ColumnarValue::Array(needle_match),
        ];
        group.bench_with_input(BenchmarkId::new("some_match", size), &size, |b, _| {
            let udf = ArrayHasAny::new();
            b.iter(|| {
                black_box(
                    udf.invoke_with_args(ScalarFunctionArgs {
                        args: args_match.clone(),
                        arg_fields: arg_fields.clone(),
                        number_rows: NUM_ROWS,
                        return_field: return_field.clone(),
                        config_options: config_options.clone(),
                    })
                    .unwrap(),
                )
            })
        });

        let needle_no_match =
            create_string_list_array_with_prefix(NUM_ROWS, NEEDLE_SIZE, "missing_");
        let args_no_match = vec![
            ColumnarValue::Array(haystack.clone()),
            ColumnarValue::Array(needle_no_match),
        ];
        group.bench_with_input(BenchmarkId::new("no_match", size), &size, |b, _| {
            let udf = ArrayHasAny::new();
            b.iter(|| {
                black_box(
                    udf.invoke_with_args(ScalarFunctionArgs {
                        args: args_no_match.clone(),
                        arg_fields: arg_fields.clone(),
                        number_rows: NUM_ROWS,
                        return_field: return_field.clone(),
                        config_options: config_options.clone(),
                    })
                    .unwrap(),
                )
            })
        });
    }

    group.finish();
}

fn create_int64_list_array(
    num_rows: usize,
    array_size: usize,
    null_density: f64,
) -> ArrayRef {
    let mut rng = StdRng::seed_from_u64(SEED);
    let values = (0..num_rows * array_size)
        .map(|_| {
            if rng.random::<f64>() < null_density {
                None
            } else {
                Some(rng.random_range(0..array_size as i64))
            }
        })
        .collect::<Int64Array>();
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

/// Like `create_int64_list_array` but values are offset so they won't
/// appear in a standard list array (useful for "not found" benchmarks).
fn create_int64_list_array_with_offset(
    num_rows: usize,
    array_size: usize,
    offset: i64,
) -> ArrayRef {
    let mut rng = StdRng::seed_from_u64(SEED + 1);
    let values = (0..num_rows * array_size)
        .map(|_| Some(rng.random_range(0..array_size as i64) + offset))
        .collect::<Int64Array>();
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

fn create_string_list_array(
    num_rows: usize,
    array_size: usize,
    null_density: f64,
) -> ArrayRef {
    let mut rng = StdRng::seed_from_u64(SEED);
    let values = (0..num_rows * array_size)
        .map(|_| {
            if rng.random::<f64>() < null_density {
                None
            } else {
                let idx = rng.random_range(0..array_size);
                Some(format!("value_{idx}"))
            }
        })
        .collect::<StringArray>();
    let offsets = (0..=num_rows)
        .map(|i| (i * array_size) as i32)
        .collect::<Vec<i32>>();

    Arc::new(
        ListArray::try_new(
            Arc::new(Field::new("item", DataType::Utf8, true)),
            OffsetBuffer::new(offsets.into()),
            Arc::new(values),
            None,
        )
        .unwrap(),
    )
}

/// Like `create_string_list_array` but values use a different prefix so
/// they won't appear in a standard string list array.
fn create_string_list_array_with_prefix(
    num_rows: usize,
    array_size: usize,
    prefix: &str,
) -> ArrayRef {
    let mut rng = StdRng::seed_from_u64(SEED + 1);
    let values = (0..num_rows * array_size)
        .map(|_| {
            let idx = rng.random_range(0..array_size);
            Some(format!("{prefix}{idx}"))
        })
        .collect::<StringArray>();
    let offsets = (0..=num_rows)
        .map(|i| (i * array_size) as i32)
        .collect::<Vec<i32>>();

    Arc::new(
        ListArray::try_new(
            Arc::new(Field::new("item", DataType::Utf8, true)),
            OffsetBuffer::new(offsets.into()),
            Arc::new(values),
            None,
        )
        .unwrap(),
    )
}

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);
