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

use arrow::array::{ArrayRef, BooleanArray, Float64Array, Int64Array, ListArray};
use arrow::buffer::OffsetBuffer;
use arrow::datatypes::{DataType, Field};
use criterion::{
    criterion_group, criterion_main, {BenchmarkId, Criterion},
};
use datafusion_common::ScalarValue;
use datafusion_common::config::ConfigOptions;
use datafusion_expr::{ColumnarValue, ScalarFunctionArgs, ScalarUDFImpl};
use datafusion_functions_nested::repeat::ArrayRepeat;
use rand::Rng;
use rand::SeedableRng;
use rand::rngs::StdRng;
use std::hint::black_box;
use std::sync::Arc;

const NUM_ROWS: &[usize] = &[100, 1000, 10000];
const REPEAT_COUNTS: &[u64] = &[5, 50];
const SEED: u64 = 42;
const NULL_DENSITY: f64 = 0.1;

fn criterion_benchmark(c: &mut Criterion) {
    // Test array_repeat with different element types
    bench_array_repeat_int64(c);
    bench_array_repeat_string(c);
    bench_array_repeat_float64(c);
    bench_array_repeat_boolean(c);

    // Test array_repeat with list element (nested arrays)
    bench_array_repeat_nested_int64_list(c);
    bench_array_repeat_nested_string_list(c);
}

fn bench_array_repeat_int64(c: &mut Criterion) {
    let mut group = c.benchmark_group("array_repeat_int64");

    for &num_rows in NUM_ROWS {
        let element_array = create_int64_array(num_rows, NULL_DENSITY);

        for &repeat_count in REPEAT_COUNTS {
            let args = vec![
                ColumnarValue::Array(element_array.clone()),
                ColumnarValue::Scalar(ScalarValue::from(repeat_count)),
            ];

            group.bench_with_input(
                BenchmarkId::new(format!("repeat_{repeat_count}_count"), num_rows),
                &num_rows,
                |b, _| {
                    let udf = ArrayRepeat::new();
                    b.iter(|| {
                        black_box(
                            udf.invoke_with_args(ScalarFunctionArgs {
                                args: args.clone(),
                                arg_fields: vec![
                                    Field::new("element", DataType::Int64, false).into(),
                                    Field::new("count", DataType::UInt64, false).into(),
                                ],
                                number_rows: num_rows,
                                return_field: Field::new(
                                    "result",
                                    DataType::List(Arc::new(Field::new_list_field(
                                        DataType::Int64,
                                        true,
                                    ))),
                                    false,
                                )
                                .into(),
                                config_options: Arc::new(ConfigOptions::default()),
                            })
                            .unwrap(),
                        )
                    })
                },
            );
        }
    }

    group.finish();
}

fn bench_array_repeat_string(c: &mut Criterion) {
    let mut group = c.benchmark_group("array_repeat_string");

    for &num_rows in NUM_ROWS {
        let element_array = create_string_array(num_rows, NULL_DENSITY);

        for &repeat_count in REPEAT_COUNTS {
            let args = vec![
                ColumnarValue::Array(element_array.clone()),
                ColumnarValue::Scalar(ScalarValue::from(repeat_count)),
            ];

            group.bench_with_input(
                BenchmarkId::new(format!("repeat_{repeat_count}_count"), num_rows),
                &num_rows,
                |b, _| {
                    let udf = ArrayRepeat::new();
                    b.iter(|| {
                        black_box(
                            udf.invoke_with_args(ScalarFunctionArgs {
                                args: args.clone(),
                                arg_fields: vec![
                                    Field::new("element", DataType::Utf8, false).into(),
                                    Field::new("count", DataType::UInt64, false).into(),
                                ],
                                number_rows: num_rows,
                                return_field: Field::new(
                                    "result",
                                    DataType::List(Arc::new(Field::new_list_field(
                                        DataType::Utf8,
                                        true,
                                    ))),
                                    false,
                                )
                                .into(),
                                config_options: Arc::new(ConfigOptions::default()),
                            })
                            .unwrap(),
                        )
                    })
                },
            );
        }
    }

    group.finish();
}

fn bench_array_repeat_nested_int64_list(c: &mut Criterion) {
    let mut group = c.benchmark_group("array_repeat_nested_int64");

    for &num_rows in NUM_ROWS {
        let list_array = create_int64_list_array(num_rows, 5, NULL_DENSITY);

        for &repeat_count in REPEAT_COUNTS {
            let args = vec![
                ColumnarValue::Array(list_array.clone()),
                ColumnarValue::Scalar(ScalarValue::from(repeat_count)),
            ];

            group.bench_with_input(
                BenchmarkId::new(format!("repeat_{repeat_count}_count"), num_rows),
                &num_rows,
                |b, _| {
                    let udf = ArrayRepeat::new();
                    b.iter(|| {
                        black_box(
                            udf.invoke_with_args(ScalarFunctionArgs {
                                args: args.clone(),
                                arg_fields: vec![
                                    Field::new(
                                        "element",
                                        list_array.data_type().clone(),
                                        false,
                                    )
                                    .into(),
                                    Field::new("count", DataType::UInt64, false).into(),
                                ],
                                number_rows: num_rows,
                                return_field: Field::new(
                                    "result",
                                    DataType::List(Arc::new(Field::new_list_field(
                                        list_array.data_type().clone(),
                                        true,
                                    ))),
                                    false,
                                )
                                .into(),
                                config_options: Arc::new(ConfigOptions::default()),
                            })
                            .unwrap(),
                        )
                    })
                },
            );
        }
    }

    group.finish();
}

fn bench_array_repeat_float64(c: &mut Criterion) {
    let mut group = c.benchmark_group("array_repeat_float64");

    for &num_rows in NUM_ROWS {
        let element_array = create_float64_array(num_rows, NULL_DENSITY);

        for &repeat_count in REPEAT_COUNTS {
            let args = vec![
                ColumnarValue::Array(element_array.clone()),
                ColumnarValue::Scalar(ScalarValue::from(repeat_count)),
            ];

            group.bench_with_input(
                BenchmarkId::new(format!("repeat_{repeat_count}_count"), num_rows),
                &num_rows,
                |b, _| {
                    let udf = ArrayRepeat::new();
                    b.iter(|| {
                        black_box(
                            udf.invoke_with_args(ScalarFunctionArgs {
                                args: args.clone(),
                                arg_fields: vec![
                                    Field::new("element", DataType::Float64, false)
                                        .into(),
                                    Field::new("count", DataType::UInt64, false).into(),
                                ],
                                number_rows: num_rows,
                                return_field: Field::new(
                                    "result",
                                    DataType::List(Arc::new(Field::new_list_field(
                                        DataType::Float64,
                                        true,
                                    ))),
                                    false,
                                )
                                .into(),
                                config_options: Arc::new(ConfigOptions::default()),
                            })
                            .unwrap(),
                        )
                    })
                },
            );
        }
    }

    group.finish();
}

fn bench_array_repeat_boolean(c: &mut Criterion) {
    let mut group = c.benchmark_group("array_repeat_boolean");

    for &num_rows in NUM_ROWS {
        let element_array = create_boolean_array(num_rows, NULL_DENSITY);

        for &repeat_count in REPEAT_COUNTS {
            let args = vec![
                ColumnarValue::Array(element_array.clone()),
                ColumnarValue::Scalar(ScalarValue::from(repeat_count)),
            ];

            group.bench_with_input(
                BenchmarkId::new(format!("repeat_{repeat_count}_count"), num_rows),
                &num_rows,
                |b, _| {
                    let udf = ArrayRepeat::new();
                    b.iter(|| {
                        black_box(
                            udf.invoke_with_args(ScalarFunctionArgs {
                                args: args.clone(),
                                arg_fields: vec![
                                    Field::new("element", DataType::Boolean, false)
                                        .into(),
                                    Field::new("count", DataType::UInt64, false).into(),
                                ],
                                number_rows: num_rows,
                                return_field: Field::new(
                                    "result",
                                    DataType::List(Arc::new(Field::new_list_field(
                                        DataType::Boolean,
                                        true,
                                    ))),
                                    false,
                                )
                                .into(),
                                config_options: Arc::new(ConfigOptions::default()),
                            })
                            .unwrap(),
                        )
                    })
                },
            );
        }
    }

    group.finish();
}

fn bench_array_repeat_nested_string_list(c: &mut Criterion) {
    let mut group = c.benchmark_group("array_repeat_nested_string");

    for &num_rows in NUM_ROWS {
        let list_array = create_string_list_array(num_rows, 5, NULL_DENSITY);

        for &repeat_count in REPEAT_COUNTS {
            let args = vec![
                ColumnarValue::Array(list_array.clone()),
                ColumnarValue::Scalar(ScalarValue::from(repeat_count)),
            ];

            group.bench_with_input(
                BenchmarkId::new(format!("repeat_{repeat_count}_count"), num_rows),
                &num_rows,
                |b, _| {
                    let udf = ArrayRepeat::new();
                    b.iter(|| {
                        black_box(
                            udf.invoke_with_args(ScalarFunctionArgs {
                                args: args.clone(),
                                arg_fields: vec![
                                    Field::new(
                                        "element",
                                        list_array.data_type().clone(),
                                        false,
                                    )
                                    .into(),
                                    Field::new("count", DataType::UInt64, false).into(),
                                ],
                                number_rows: num_rows,
                                return_field: Field::new(
                                    "result",
                                    DataType::List(Arc::new(Field::new_list_field(
                                        list_array.data_type().clone(),
                                        true,
                                    ))),
                                    false,
                                )
                                .into(),
                                config_options: Arc::new(ConfigOptions::default()),
                            })
                            .unwrap(),
                        )
                    })
                },
            );
        }
    }

    group.finish();
}

fn create_int64_array(num_rows: usize, null_density: f64) -> ArrayRef {
    let mut rng = StdRng::seed_from_u64(SEED);
    let values = (0..num_rows)
        .map(|_| {
            if rng.random::<f64>() < null_density {
                None
            } else {
                Some(rng.random_range(0..1000))
            }
        })
        .collect::<Int64Array>();

    Arc::new(values)
}

fn create_string_array(num_rows: usize, null_density: f64) -> ArrayRef {
    let mut rng = StdRng::seed_from_u64(SEED);
    use arrow::array::StringArray;

    let values = (0..num_rows)
        .map(|_| {
            if rng.random::<f64>() < null_density {
                None
            } else {
                Some(format!("value_{}", rng.random_range(0..100)))
            }
        })
        .collect::<StringArray>();

    Arc::new(values)
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
                Some(rng.random_range(0..1000))
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

fn create_float64_array(num_rows: usize, null_density: f64) -> ArrayRef {
    let mut rng = StdRng::seed_from_u64(SEED);
    let values = (0..num_rows)
        .map(|_| {
            if rng.random::<f64>() < null_density {
                None
            } else {
                Some(rng.random_range(0.0..1000.0))
            }
        })
        .collect::<Float64Array>();

    Arc::new(values)
}

fn create_boolean_array(num_rows: usize, null_density: f64) -> ArrayRef {
    let mut rng = StdRng::seed_from_u64(SEED);
    let values = (0..num_rows)
        .map(|_| {
            if rng.random::<f64>() < null_density {
                None
            } else {
                Some(rng.random())
            }
        })
        .collect::<BooleanArray>();

    Arc::new(values)
}

fn create_string_list_array(
    num_rows: usize,
    array_size: usize,
    null_density: f64,
) -> ArrayRef {
    let mut rng = StdRng::seed_from_u64(SEED);
    use arrow::array::StringArray;

    let values = (0..num_rows * array_size)
        .map(|_| {
            if rng.random::<f64>() < null_density {
                None
            } else {
                Some(format!("value_{}", rng.random_range(0..100)))
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

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);
