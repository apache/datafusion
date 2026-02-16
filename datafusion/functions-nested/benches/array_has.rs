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

    // Benchmark for array_has_any with one scalar arg
    bench_array_has_any_scalar(c);
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
    let first_arr = create_int64_list_array(NUM_ROWS, array_size, NULL_DENSITY);
    let list_type = first_arr.data_type().clone();
    let config_options = Arc::new(ConfigOptions::default());
    let return_field: Arc<Field> = Field::new("result", DataType::Boolean, true).into();
    let arg_fields: Vec<Arc<Field>> = vec![
        Field::new("first", list_type.clone(), false).into(),
        Field::new("second", list_type.clone(), false).into(),
    ];

    // Benchmark: some elements match
    let second_match = create_int64_list_array(NUM_ROWS, NEEDLE_SIZE, 0.0);
    let args_match = vec![
        ColumnarValue::Array(first_arr.clone()),
        ColumnarValue::Array(second_match),
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
    let second_no_match =
        create_int64_list_array_with_offset(NUM_ROWS, NEEDLE_SIZE, array_size as i64);
    let args_no_match = vec![
        ColumnarValue::Array(first_arr.clone()),
        ColumnarValue::Array(second_no_match),
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

    // Benchmark: scalar second arg, some match
    let scalar_second_match = create_int64_scalar_list(NEEDLE_SIZE, 0);
    let args_scalar_match = vec![
        ColumnarValue::Array(first_arr.clone()),
        ColumnarValue::Scalar(scalar_second_match),
    ];
    group.bench_with_input(
        BenchmarkId::new("scalar_some_match", array_size),
        &array_size,
        |b, _| {
            let udf = ArrayHasAny::new();
            b.iter(|| {
                black_box(
                    udf.invoke_with_args(ScalarFunctionArgs {
                        args: args_scalar_match.clone(),
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

    // Benchmark: scalar second arg, no match
    let scalar_second_no_match = create_int64_scalar_list(NEEDLE_SIZE, array_size as i64);
    let args_scalar_no_match = vec![
        ColumnarValue::Array(first_arr.clone()),
        ColumnarValue::Scalar(scalar_second_no_match),
    ];
    group.bench_with_input(
        BenchmarkId::new("scalar_no_match", array_size),
        &array_size,
        |b, _| {
            let udf = ArrayHasAny::new();
            b.iter(|| {
                black_box(
                    udf.invoke_with_args(ScalarFunctionArgs {
                        args: args_scalar_no_match.clone(),
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
        let first_arr = create_string_list_array(NUM_ROWS, size, NULL_DENSITY);
        let list_type = first_arr.data_type().clone();
        let arg_fields: Vec<Arc<Field>> = vec![
            Field::new("first", list_type.clone(), false).into(),
            Field::new("second", list_type.clone(), false).into(),
        ];

        let second_match = create_string_list_array(NUM_ROWS, NEEDLE_SIZE, 0.0);
        let args_match = vec![
            ColumnarValue::Array(first_arr.clone()),
            ColumnarValue::Array(second_match),
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

        let second_no_match =
            create_string_list_array_with_prefix(NUM_ROWS, NEEDLE_SIZE, "missing_");
        let args_no_match = vec![
            ColumnarValue::Array(first_arr.clone()),
            ColumnarValue::Array(second_no_match),
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

        // Benchmark: scalar second arg, some match
        let scalar_second_match = create_string_scalar_list(NEEDLE_SIZE, "value_");
        let args_scalar_match = vec![
            ColumnarValue::Array(first_arr.clone()),
            ColumnarValue::Scalar(scalar_second_match),
        ];
        group.bench_with_input(
            BenchmarkId::new("scalar_some_match", size),
            &size,
            |b, _| {
                let udf = ArrayHasAny::new();
                b.iter(|| {
                    black_box(
                        udf.invoke_with_args(ScalarFunctionArgs {
                            args: args_scalar_match.clone(),
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

        // Benchmark: scalar second arg, no match
        let scalar_second_no_match = create_string_scalar_list(NEEDLE_SIZE, "missing_");
        let args_scalar_no_match = vec![
            ColumnarValue::Array(first_arr.clone()),
            ColumnarValue::Scalar(scalar_second_no_match),
        ];
        group.bench_with_input(
            BenchmarkId::new("scalar_no_match", size),
            &size,
            |b, _| {
                let udf = ArrayHasAny::new();
                b.iter(|| {
                    black_box(
                        udf.invoke_with_args(ScalarFunctionArgs {
                            args: args_scalar_no_match.clone(),
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
    }

    group.finish();
}

/// Benchmarks array_has_any with one scalar arg.  Varies the scalar argument
/// size while keeping the columnar array small (3 elements per row).
fn bench_array_has_any_scalar(c: &mut Criterion) {
    let mut group = c.benchmark_group("array_has_any_scalar");
    let config_options = Arc::new(ConfigOptions::default());
    let return_field: Arc<Field> = Field::new("result", DataType::Boolean, true).into();

    let array_size = 3;
    let scalar_sizes = vec![1, 10, 100, 1000];

    // i64 benchmarks
    let first_arr_i64 = create_int64_list_array(NUM_ROWS, array_size, NULL_DENSITY);
    let list_type_i64 = first_arr_i64.data_type().clone();
    let arg_fields_i64: Vec<Arc<Field>> = vec![
        Field::new("first", list_type_i64.clone(), false).into(),
        Field::new("second", list_type_i64.clone(), false).into(),
    ];

    for &scalar_size in &scalar_sizes {
        let scalar_arg = create_int64_scalar_list(scalar_size, 0);
        let args = vec![
            ColumnarValue::Array(first_arr_i64.clone()),
            ColumnarValue::Scalar(scalar_arg),
        ];
        group.bench_with_input(
            BenchmarkId::new("i64_no_match", scalar_size),
            &scalar_size,
            |b, _| {
                let udf = ArrayHasAny::new();
                b.iter(|| {
                    black_box(
                        udf.invoke_with_args(ScalarFunctionArgs {
                            args: args.clone(),
                            arg_fields: arg_fields_i64.clone(),
                            number_rows: NUM_ROWS,
                            return_field: return_field.clone(),
                            config_options: config_options.clone(),
                        })
                        .unwrap(),
                    )
                })
            },
        );
    }

    // String benchmarks
    let first_arr_str = create_string_list_array(NUM_ROWS, array_size, NULL_DENSITY);
    let list_type_str = first_arr_str.data_type().clone();
    let arg_fields_str: Vec<Arc<Field>> = vec![
        Field::new("first", list_type_str.clone(), false).into(),
        Field::new("second", list_type_str.clone(), false).into(),
    ];

    for &scalar_size in &scalar_sizes {
        let scalar_arg = create_string_scalar_list(scalar_size, "missing_");
        let args = vec![
            ColumnarValue::Array(first_arr_str.clone()),
            ColumnarValue::Scalar(scalar_arg),
        ];
        group.bench_with_input(
            BenchmarkId::new("string_no_match", scalar_size),
            &scalar_size,
            |b, _| {
                let udf = ArrayHasAny::new();
                b.iter(|| {
                    black_box(
                        udf.invoke_with_args(ScalarFunctionArgs {
                            args: args.clone(),
                            arg_fields: arg_fields_str.clone(),
                            number_rows: NUM_ROWS,
                            return_field: return_field.clone(),
                            config_options: config_options.clone(),
                        })
                        .unwrap(),
                    )
                })
            },
        );
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

/// Create a `ScalarValue::List` containing a single list of `size` i64 elements,
/// with values starting at `offset`.
fn create_int64_scalar_list(size: usize, offset: i64) -> ScalarValue {
    let values = (0..size as i64)
        .map(|i| Some(i + offset))
        .collect::<Int64Array>();
    let list = ListArray::try_new(
        Arc::new(Field::new("item", DataType::Int64, true)),
        OffsetBuffer::new(vec![0, size as i32].into()),
        Arc::new(values),
        None,
    )
    .unwrap();
    ScalarValue::List(Arc::new(list))
}

/// Create a `ScalarValue::List` containing a single list of `size` string elements,
/// with values like "{prefix}0", "{prefix}1", etc.
fn create_string_scalar_list(size: usize, prefix: &str) -> ScalarValue {
    let values = (0..size)
        .map(|i| Some(format!("{prefix}{i}")))
        .collect::<StringArray>();
    let list = ListArray::try_new(
        Arc::new(Field::new("item", DataType::Utf8, true)),
        OffsetBuffer::new(vec![0, size as i32].into()),
        Arc::new(values),
        None,
    )
    .unwrap();
    ScalarValue::List(Arc::new(list))
}

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);
