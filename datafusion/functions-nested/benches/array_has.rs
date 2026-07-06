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

use arrow::array::{
    ArrayRef, Int64Array, LargeStringArray, ListArray, StringArray, StringViewArray,
};
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
        bench_array_has_array(c, size);
        bench_array_has_all(c, size);
        bench_array_has_any(c, size);
    }

    // Specific benchmarks for string arrays (common use case)
    bench_array_has_strings(c);
    bench_array_has_array_strings(c);
    bench_array_has_all_strings(c);
    bench_array_has_any_strings(c);

    // Benchmark for array_has_any with one scalar arg
    bench_array_has_any_scalar(c);

    // Array-needle fast-path profile: null patterns, list length, row height.
    bench_array_has_array_null_patterns(c);
    bench_array_has_array_by_size(c);
    bench_array_has_array_by_rows(c);
}

/// Invoke `array_has` once with an array (column) needle -- exercises the
/// `array_has_dispatch_for_array` fast path.
fn run_array_needle_case(
    group: &mut criterion::BenchmarkGroup<'_, criterion::measurement::WallTime>,
    id: String,
    haystack: ArrayRef,
    needle: ArrayRef,
    rows: usize,
) {
    let config_options = Arc::new(ConfigOptions::default());
    let return_field: Arc<Field> = Field::new("result", DataType::Boolean, true).into();
    let arg_fields: Vec<Arc<Field>> = vec![
        Field::new("arr", haystack.data_type().clone(), false).into(),
        Field::new("el", needle.data_type().clone(), false).into(),
    ];
    let args = vec![ColumnarValue::Array(haystack), ColumnarValue::Array(needle)];
    group.bench_function(id, |b| {
        let udf = ArrayHas::new();
        b.iter(|| {
            black_box(
                udf.invoke_with_args(ScalarFunctionArgs {
                    args: args.clone(),
                    arg_fields: arg_fields.clone(),
                    number_rows: rows,
                    return_field: return_field.clone(),
                    config_options: config_options.clone(),
                })
                .unwrap(),
            )
        });
    });
}

/// Build a `List` of `array_size` string elements per row (`{prefix}{i}`) with
/// the given element type (`Utf8` / `LargeUtf8` / `Utf8View`) and null density.
/// The prefix controls element length: a short one stays inline in a `Utf8View`
/// (<= 12 bytes), a long one spills to the data buffer.
fn string_list_array(
    num_rows: usize,
    array_size: usize,
    null_density: f64,
    prefix: &str,
    element_type: &DataType,
) -> ArrayRef {
    let mut rng = StdRng::seed_from_u64(SEED);
    let data = (0..num_rows * array_size).map(|_| {
        if rng.random::<f64>() < null_density {
            None
        } else {
            Some(format!("{prefix}{}", rng.random_range(0..array_size)))
        }
    });
    let values: ArrayRef = match element_type {
        DataType::Utf8 => Arc::new(data.collect::<StringArray>()),
        DataType::LargeUtf8 => Arc::new(data.collect::<LargeStringArray>()),
        DataType::Utf8View => Arc::new(data.collect::<StringViewArray>()),
        other => panic!("unsupported string element type: {other}"),
    };
    let offsets = (0..=num_rows)
        .map(|i| (i * array_size) as i32)
        .collect::<Vec<i32>>();
    Arc::new(
        ListArray::try_new(
            Arc::new(Field::new("item", element_type.clone(), true)),
            OffsetBuffer::new(offsets.into()),
            values,
            None,
        )
        .unwrap(),
    )
}

/// Build a string needle column (one value per row) of the given element type.
fn string_value_array(
    num_rows: usize,
    range: usize,
    prefix: &str,
    element_type: &DataType,
) -> ArrayRef {
    let mut rng = StdRng::seed_from_u64(SEED + 2);
    let data =
        (0..num_rows).map(|_| Some(format!("{prefix}{}", rng.random_range(0..range))));
    match element_type {
        DataType::Utf8 => Arc::new(data.collect::<StringArray>()),
        DataType::LargeUtf8 => Arc::new(data.collect::<LargeStringArray>()),
        DataType::Utf8View => Arc::new(data.collect::<StringViewArray>()),
        other => panic!("unsupported string element type: {other}"),
    }
}

/// Array needle, fixed list length (64), across null patterns. i64 covers
/// no-nulls found/not-found, 30% nulls found/not-found, all-null, and a
/// null-fill collision. Each string
/// element type (`Utf8`, `LargeUtf8`, `Utf8View`) covers no nulls / 30% nulls at
/// both short (inline, <= 12 byte) and long (> 12 byte, shared-prefix) element
/// lengths, plus all-null. `not_found` shifts the needle out of the value range.
fn bench_array_has_array_null_patterns(c: &mut Criterion) {
    let (rows, size) = (10_000usize, 64usize);
    let s = size as i64;
    let mut group = c.benchmark_group("array_has_array_null_patterns");
    run_array_needle_case(
        &mut group,
        "i64/no_nulls".to_string(),
        create_int64_list_array(rows, size, 0.0),
        create_int64_value_array(rows, s, 0),
        rows,
    );
    // Worst case for the all-valid fold: non-null, no match -> the whole row is
    // scanned (the branchless OR-reduction never short-circuits).
    run_array_needle_case(
        &mut group,
        "i64/no_nulls_not_found".to_string(),
        create_int64_list_array(rows, size, 0.0),
        create_int64_value_array(rows, s, s),
        rows,
    );
    run_array_needle_case(
        &mut group,
        "i64/nulls30_found".to_string(),
        create_int64_list_array(rows, size, 0.3),
        create_int64_value_array(rows, s, 0),
        rows,
    );
    run_array_needle_case(
        &mut group,
        "i64/nulls30_not_found".to_string(),
        create_int64_list_array(rows, size, 0.3),
        create_int64_value_array(rows, s, s),
        rows,
    );
    run_array_needle_case(
        &mut group,
        "i64/all_null".to_string(),
        create_int64_list_array(rows, size, 1.0),
        create_int64_value_array(rows, s, 0),
        rows,
    );
    run_array_needle_case(
        &mut group,
        "i64/collision".to_string(),
        create_int64_list_array(rows, size, 1.0),
        create_int64_value_array(rows, 1, 0),
        rows,
    );
    // Short elements stay inline in a `Utf8View` (<= 12 bytes); long elements
    // share a 4-byte prefix (the realistic case where the view prefix can't
    // reject, forcing a buffer compare). `_short` / `_long` labels distinguish
    // them; all-null has no content so it is length-independent.
    let short = "value_"; // "value_0".."value_63": <= 8 bytes, inline
    let long = "long_element_string_value_"; // ~28 bytes, spills to the buffer
    for (type_label, element_type) in [
        ("utf8", DataType::Utf8),
        ("largeutf8", DataType::LargeUtf8),
        ("utf8view", DataType::Utf8View),
    ] {
        for (len_label, prefix) in [("short", short), ("long", long)] {
            for (pat_label, density) in [("no_nulls", 0.0), ("nulls30", 0.3)] {
                run_array_needle_case(
                    &mut group,
                    format!("{type_label}_{len_label}/{pat_label}"),
                    string_list_array(rows, size, density, prefix, &element_type),
                    string_value_array(rows, size, prefix, &element_type),
                    rows,
                );
            }
        }
        run_array_needle_case(
            &mut group,
            format!("{type_label}/all_null"),
            string_list_array(rows, size, 1.0, short, &element_type),
            string_value_array(rows, size, short, &element_type),
            rows,
        );
    }
    group.finish();
}

/// Array needle, i64, 30% element nulls, not found, across list lengths.
fn bench_array_has_array_by_size(c: &mut Criterion) {
    let rows = 10_000usize;
    let mut group = c.benchmark_group("array_has_array_by_size");
    for size in [8usize, 32, 128, 256, 512, 1024] {
        let s = size as i64;
        run_array_needle_case(
            &mut group,
            size.to_string(),
            create_int64_list_array(rows, size, 0.3),
            create_int64_value_array(rows, s, s),
            rows,
        );
    }
    group.finish();
}

/// Array needle, i64, 8 elems/row, 30% nulls, not found, across row counts.
fn bench_array_has_array_by_rows(c: &mut Criterion) {
    let (size, s) = (8usize, 8i64);
    let mut group = c.benchmark_group("array_has_array_by_rows");
    for rows in [10_000usize, 100_000, 1_000_000] {
        run_array_needle_case(
            &mut group,
            rows.to_string(),
            create_int64_list_array(rows, size, 0.3),
            create_int64_value_array(rows, s, s),
            rows,
        );
    }
    group.finish();
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

/// Benchmarks array_has where the needle is an array (a column with one value
/// per row) rather than a scalar.
fn bench_array_has_array(c: &mut Criterion, array_size: usize) {
    let mut group = c.benchmark_group("array_has_array_i64");
    let haystack = create_int64_list_array(NUM_ROWS, array_size, NULL_DENSITY);
    let config_options = Arc::new(ConfigOptions::default());
    let return_field: Arc<Field> = Field::new("result", DataType::Boolean, true).into();
    let arg_fields: Vec<Arc<Field>> = vec![
        Field::new("arr", haystack.data_type().clone(), false).into(),
        Field::new("el", DataType::Int64, false).into(),
    ];

    // Needle values drawn from the same range as the haystack values, so many
    // rows find a match (and the inner loop can short-circuit).
    let needle_found = create_int64_value_array(NUM_ROWS, array_size as i64, 0);
    let args_found = vec![
        ColumnarValue::Array(haystack.clone()),
        ColumnarValue::Array(needle_found),
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

    // Needle values outside the haystack range: never matches, so every row
    // scans its full element list (worst case for the inner loop).
    let needle_not_found =
        create_int64_value_array(NUM_ROWS, array_size as i64, array_size as i64);
    let args_not_found = vec![
        ColumnarValue::Array(haystack.clone()),
        ColumnarValue::Array(needle_not_found),
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

fn bench_array_has_array_strings(c: &mut Criterion) {
    let mut group = c.benchmark_group("array_has_array_strings");
    let config_options = Arc::new(ConfigOptions::default());
    let return_field: Arc<Field> = Field::new("result", DataType::Boolean, true).into();

    let sizes = vec![10, 100, 500];

    for &size in &sizes {
        let haystack = create_string_list_array(NUM_ROWS, size, NULL_DENSITY);
        let arg_fields: Vec<Arc<Field>> = vec![
            Field::new("arr", haystack.data_type().clone(), false).into(),
            Field::new("el", DataType::Utf8, false).into(),
        ];

        let needle_found = create_string_value_array(NUM_ROWS, size, "value_");
        let args_found = vec![
            ColumnarValue::Array(haystack.clone()),
            ColumnarValue::Array(needle_found),
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

        let needle_not_found = create_string_value_array(NUM_ROWS, size, "missing_");
        let args_not_found = vec![
            ColumnarValue::Array(haystack.clone()),
            ColumnarValue::Array(needle_not_found),
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

const SMALL_ARRAY_SIZE: usize = NEEDLE_SIZE;

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
    let second_match = create_int64_list_array(NUM_ROWS, SMALL_ARRAY_SIZE, 0.0);
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
    let second_no_match = create_int64_list_array_with_offset(
        NUM_ROWS,
        SMALL_ARRAY_SIZE,
        array_size as i64,
    );
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
    let scalar_second_match = create_int64_scalar_list(SMALL_ARRAY_SIZE, 0);
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
    let scalar_second_no_match =
        create_int64_scalar_list(SMALL_ARRAY_SIZE, array_size as i64);
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

        let second_match = create_string_list_array(NUM_ROWS, SMALL_ARRAY_SIZE, 0.0);
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
            create_string_list_array_with_prefix(NUM_ROWS, SMALL_ARRAY_SIZE, "missing_");
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
        let scalar_second_match = create_string_scalar_list(SMALL_ARRAY_SIZE, "value_");
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
        let scalar_second_no_match =
            create_string_scalar_list(SMALL_ARRAY_SIZE, "missing_");
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
        let scalar_arg = create_int64_scalar_list(scalar_size, array_size as i64);
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

/// Create an `Int64Array` of `num_rows` non-null values in `[offset, offset +
/// range)`, used as an array needle for `array_has`.
fn create_int64_value_array(num_rows: usize, range: i64, offset: i64) -> ArrayRef {
    let mut rng = StdRng::seed_from_u64(SEED + 2);
    let values = (0..num_rows)
        .map(|_| Some(rng.random_range(0..range) + offset))
        .collect::<Int64Array>();
    Arc::new(values)
}

/// Create a `StringArray` of `num_rows` non-null values like "{prefix}{idx}"
/// where `idx` is drawn from `[0, range)`, used as an array needle for
/// `array_has`.
fn create_string_value_array(num_rows: usize, range: usize, prefix: &str) -> ArrayRef {
    let mut rng = StdRng::seed_from_u64(SEED + 2);
    let values = (0..num_rows)
        .map(|_| {
            let idx = rng.random_range(0..range);
            Some(format!("{prefix}{idx}"))
        })
        .collect::<StringArray>();
    Arc::new(values)
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
