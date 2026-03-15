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
    Array, ArrayRef, BinaryArray, BooleanArray, Decimal128Array, FixedSizeBinaryArray,
    Float64Array, Int64Array, ListArray, StringArray,
};
use arrow::buffer::OffsetBuffer;
use arrow::datatypes::{DataType, Field};
use criterion::{
    criterion_group, criterion_main, {BenchmarkId, Criterion},
};
use datafusion_common::ScalarValue;
use datafusion_common::config::ConfigOptions;
use datafusion_expr::{ColumnarValue, ScalarFunctionArgs, ScalarUDFImpl};
use datafusion_functions_nested::remove::ArrayRemove;
use rand::Rng;
use rand::SeedableRng;
use rand::rngs::StdRng;
use std::hint::black_box;
use std::sync::Arc;

const NUM_ROWS: usize = 10000;
const ARRAY_SIZES: &[usize] = &[10, 100, 500];
const SEED: u64 = 42;
const NULL_DENSITY: f64 = 0.1;

fn criterion_benchmark(c: &mut Criterion) {
    // Test array_remove with different data types and array sizes
    // TODO: Add performance tests for nested datatypes
    bench_array_remove_int64(c);
    bench_array_remove_f64(c);
    bench_array_remove_strings(c);
    bench_array_remove_binary(c);
    bench_array_remove_boolean(c);
    bench_array_remove_decimal64(c);
    bench_array_remove_fixed_size_binary(c);
}

fn bench_array_remove_int64(c: &mut Criterion) {
    let mut group = c.benchmark_group("array_remove_int64");

    for &array_size in ARRAY_SIZES {
        let list_array = create_int64_list_array(NUM_ROWS, array_size, NULL_DENSITY);
        let element_to_remove = ScalarValue::Int64(Some(1));
        let args = create_args(list_array.clone(), element_to_remove.clone());

        group.bench_with_input(
            BenchmarkId::new("remove", array_size),
            &array_size,
            |b, _| {
                let udf = ArrayRemove::new();
                b.iter(|| {
                    black_box(
                        udf.invoke_with_args(ScalarFunctionArgs {
                            args: args.clone(),
                            arg_fields: vec![
                                Field::new("arr", list_array.data_type().clone(), false)
                                    .into(),
                                Field::new("el", DataType::Int64, false).into(),
                            ],
                            number_rows: NUM_ROWS,
                            return_field: Field::new(
                                "result",
                                list_array.data_type().clone(),
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

    group.finish();
}

fn bench_array_remove_f64(c: &mut Criterion) {
    let mut group = c.benchmark_group("array_remove_f64");

    for &array_size in ARRAY_SIZES {
        let list_array = create_f64_list_array(NUM_ROWS, array_size, NULL_DENSITY);
        let element_to_remove = ScalarValue::Float64(Some(1.0));
        let args = create_args(list_array.clone(), element_to_remove.clone());

        group.bench_with_input(
            BenchmarkId::new("remove", array_size),
            &array_size,
            |b, _| {
                let udf = ArrayRemove::new();
                b.iter(|| {
                    black_box(
                        udf.invoke_with_args(ScalarFunctionArgs {
                            args: args.clone(),
                            arg_fields: vec![
                                Field::new("arr", list_array.data_type().clone(), false)
                                    .into(),
                                Field::new("el", DataType::Float64, false).into(),
                            ],
                            number_rows: NUM_ROWS,
                            return_field: Field::new(
                                "result",
                                list_array.data_type().clone(),
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

    group.finish();
}

fn bench_array_remove_strings(c: &mut Criterion) {
    let mut group = c.benchmark_group("array_remove_strings");

    for &array_size in ARRAY_SIZES {
        let list_array = create_string_list_array(NUM_ROWS, array_size, NULL_DENSITY);
        let element_to_remove = ScalarValue::Utf8(Some("value_1".to_string()));
        let args = create_args(list_array.clone(), element_to_remove.clone());

        group.bench_with_input(
            BenchmarkId::new("remove", array_size),
            &array_size,
            |b, _| {
                let udf = ArrayRemove::new();
                b.iter(|| {
                    black_box(
                        udf.invoke_with_args(ScalarFunctionArgs {
                            args: args.clone(),
                            arg_fields: vec![
                                Field::new("arr", list_array.data_type().clone(), false)
                                    .into(),
                                Field::new("el", DataType::Utf8, false).into(),
                            ],
                            number_rows: NUM_ROWS,
                            return_field: Field::new(
                                "result",
                                list_array.data_type().clone(),
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

    group.finish();
}

fn bench_array_remove_binary(c: &mut Criterion) {
    let mut group = c.benchmark_group("array_remove_binary");

    for &array_size in ARRAY_SIZES {
        let list_array = create_binary_list_array(NUM_ROWS, array_size, NULL_DENSITY);
        let element_to_remove = ScalarValue::Binary(Some(b"value_1".to_vec()));
        let args = create_args(list_array.clone(), element_to_remove.clone());

        group.bench_with_input(
            BenchmarkId::new("remove", array_size),
            &array_size,
            |b, _| {
                let udf = ArrayRemove::new();
                b.iter(|| {
                    black_box(
                        udf.invoke_with_args(ScalarFunctionArgs {
                            args: args.clone(),
                            arg_fields: vec![
                                Field::new("arr", list_array.data_type().clone(), false)
                                    .into(),
                                Field::new("el", DataType::Binary, false).into(),
                            ],
                            number_rows: NUM_ROWS,
                            return_field: Field::new(
                                "result",
                                list_array.data_type().clone(),
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

    group.finish();
}

fn bench_array_remove_boolean(c: &mut Criterion) {
    let mut group = c.benchmark_group("array_remove_boolean");

    for &array_size in ARRAY_SIZES {
        let list_array = create_boolean_list_array(NUM_ROWS, array_size, NULL_DENSITY);
        let element_to_remove = ScalarValue::Boolean(Some(true));
        let args = create_args(list_array.clone(), element_to_remove.clone());

        group.bench_with_input(
            BenchmarkId::new("remove", array_size),
            &array_size,
            |b, _| {
                let udf = ArrayRemove::new();
                b.iter(|| {
                    black_box(
                        udf.invoke_with_args(ScalarFunctionArgs {
                            args: args.clone(),
                            arg_fields: vec![
                                Field::new("arr", list_array.data_type().clone(), false)
                                    .into(),
                                Field::new("el", DataType::Boolean, false).into(),
                            ],
                            number_rows: NUM_ROWS,
                            return_field: Field::new(
                                "result",
                                list_array.data_type().clone(),
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

    group.finish();
}

fn bench_array_remove_decimal64(c: &mut Criterion) {
    let mut group = c.benchmark_group("array_remove_decimal64");

    for &array_size in ARRAY_SIZES {
        let list_array = create_decimal64_list_array(NUM_ROWS, array_size, NULL_DENSITY);
        let element_to_remove = ScalarValue::Decimal128(Some(100_i128), 10, 2);
        let args = create_args(list_array.clone(), element_to_remove.clone());

        group.bench_with_input(
            BenchmarkId::new("remove", array_size),
            &array_size,
            |b, _| {
                let udf = ArrayRemove::new();
                b.iter(|| {
                    black_box(
                        udf.invoke_with_args(ScalarFunctionArgs {
                            args: args.clone(),
                            arg_fields: vec![
                                Field::new("arr", list_array.data_type().clone(), false)
                                    .into(),
                                Field::new("el", DataType::Decimal128(10, 2), false)
                                    .into(),
                            ],
                            number_rows: NUM_ROWS,
                            return_field: Field::new(
                                "result",
                                list_array.data_type().clone(),
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

    group.finish();
}

fn bench_array_remove_fixed_size_binary(c: &mut Criterion) {
    let mut group = c.benchmark_group("array_remove_fixed_size_binary");

    for &array_size in ARRAY_SIZES {
        let list_array =
            create_fixed_size_binary_list_array(NUM_ROWS, array_size, NULL_DENSITY);
        let element_to_remove = ScalarValue::FixedSizeBinary(16, Some(vec![1u8; 16]));
        let args = create_args(list_array.clone(), element_to_remove.clone());

        group.bench_with_input(
            BenchmarkId::new("remove", array_size),
            &array_size,
            |b, _| {
                let udf = ArrayRemove::new();
                b.iter(|| {
                    black_box(
                        udf.invoke_with_args(ScalarFunctionArgs {
                            args: args.clone(),
                            arg_fields: vec![
                                Field::new("arr", list_array.data_type().clone(), false)
                                    .into(),
                                Field::new("el", DataType::FixedSizeBinary(16), false)
                                    .into(),
                            ],
                            number_rows: NUM_ROWS,
                            return_field: Field::new(
                                "result",
                                list_array.data_type().clone(),
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

    group.finish();
}

fn create_args(list_array: ArrayRef, element: ScalarValue) -> Vec<ColumnarValue> {
    vec![
        ColumnarValue::Array(list_array),
        ColumnarValue::Scalar(element),
    ]
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

fn create_f64_list_array(
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
                Some(rng.random_range(0.0..array_size as f64))
            }
        })
        .collect::<Float64Array>();
    let offsets = (0..=num_rows)
        .map(|i| (i * array_size) as i32)
        .collect::<Vec<i32>>();

    Arc::new(
        ListArray::try_new(
            Arc::new(Field::new("item", DataType::Float64, true)),
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

fn create_binary_list_array(
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
                Some(format!("value_{idx}").into_bytes())
            }
        })
        .collect::<BinaryArray>();
    let offsets = (0..=num_rows)
        .map(|i| (i * array_size) as i32)
        .collect::<Vec<i32>>();

    Arc::new(
        ListArray::try_new(
            Arc::new(Field::new("item", DataType::Binary, true)),
            OffsetBuffer::new(offsets.into()),
            Arc::new(values),
            None,
        )
        .unwrap(),
    )
}

fn create_boolean_list_array(
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
                Some(rng.random::<bool>())
            }
        })
        .collect::<BooleanArray>();
    let offsets = (0..=num_rows)
        .map(|i| (i * array_size) as i32)
        .collect::<Vec<i32>>();

    Arc::new(
        ListArray::try_new(
            Arc::new(Field::new("item", DataType::Boolean, true)),
            OffsetBuffer::new(offsets.into()),
            Arc::new(values),
            None,
        )
        .unwrap(),
    )
}

fn create_decimal64_list_array(
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
                Some(rng.random_range(0..array_size) as i128 * 100)
            }
        })
        .collect::<Decimal128Array>()
        .with_precision_and_scale(10, 2)
        .unwrap();
    let offsets = (0..=num_rows)
        .map(|i| (i * array_size) as i32)
        .collect::<Vec<i32>>();

    Arc::new(
        ListArray::try_new(
            Arc::new(Field::new("item", DataType::Decimal128(10, 2), true)),
            OffsetBuffer::new(offsets.into()),
            Arc::new(values),
            None,
        )
        .unwrap(),
    )
}

fn create_fixed_size_binary_list_array(
    num_rows: usize,
    array_size: usize,
    null_density: f64,
) -> ArrayRef {
    let mut rng = StdRng::seed_from_u64(SEED);
    let mut buffer = Vec::with_capacity(num_rows * array_size * 16);
    let mut null_buffer = Vec::with_capacity(num_rows * array_size);
    for _ in 0..num_rows * array_size {
        if rng.random::<f64>() < null_density {
            null_buffer.push(false);
            buffer.extend_from_slice(&[0u8; 16]);
        } else {
            null_buffer.push(true);
            let mut bytes = [0u8; 16];
            rng.fill(&mut bytes);
            buffer.extend_from_slice(&bytes);
        }
    }
    let nulls = arrow::buffer::NullBuffer::from_iter(null_buffer.iter().copied());
    let values = FixedSizeBinaryArray::new(16, buffer.into(), Some(nulls));
    let offsets = (0..=num_rows)
        .map(|i| (i * array_size) as i32)
        .collect::<Vec<i32>>();

    Arc::new(
        ListArray::try_new(
            Arc::new(Field::new("item", DataType::FixedSizeBinary(16), true)),
            OffsetBuffer::new(offsets.into()),
            Arc::new(values),
            None,
        )
        .unwrap(),
    )
}

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);
