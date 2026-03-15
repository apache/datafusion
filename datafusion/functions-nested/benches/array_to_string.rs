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

use arrow::array::{ArrayRef, Float64Array, Int64Array, ListArray, StringArray};
use arrow::buffer::OffsetBuffer;
use arrow::datatypes::{DataType, Field};
use criterion::{BenchmarkId, Criterion, criterion_group, criterion_main};
use datafusion_common::ScalarValue;
use datafusion_common::config::ConfigOptions;
use datafusion_expr::{ColumnarValue, ScalarFunctionArgs, ScalarUDFImpl};
use datafusion_functions_nested::string::ArrayToString;
use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};
use std::hint::black_box;
use std::sync::Arc;

const NUM_ROWS: usize = 1000;
const ARRAY_SIZES: &[usize] = &[5, 20, 100];
const NESTED_ARRAY_SIZE: usize = 3;
const SEED: u64 = 42;
const NULL_DENSITY: f64 = 0.1;

fn criterion_benchmark(c: &mut Criterion) {
    bench_array_to_string(c, "array_to_string_int64", create_int64_list_array);
    bench_array_to_string(c, "array_to_string_float64", create_float64_list_array);
    bench_array_to_string(c, "array_to_string_string", create_string_list_array);
    bench_array_to_string(
        c,
        "array_to_string_nested_int64",
        create_nested_int64_list_array,
    );
}

fn bench_array_to_string(
    c: &mut Criterion,
    group_name: &str,
    make_array: impl Fn(usize) -> ArrayRef,
) {
    let mut group = c.benchmark_group(group_name);

    for &array_size in ARRAY_SIZES {
        let list_array = make_array(array_size);
        let args = vec![
            ColumnarValue::Array(list_array.clone()),
            ColumnarValue::Scalar(ScalarValue::Utf8(Some(",".to_string()))),
        ];
        let arg_fields = vec![
            Field::new("array", list_array.data_type().clone(), true).into(),
            Field::new("delimiter", DataType::Utf8, false).into(),
        ];

        group.bench_with_input(
            BenchmarkId::from_parameter(array_size),
            &array_size,
            |b, _| {
                let udf = ArrayToString::new();
                b.iter(|| {
                    black_box(
                        udf.invoke_with_args(ScalarFunctionArgs {
                            args: args.clone(),
                            arg_fields: arg_fields.clone(),
                            number_rows: NUM_ROWS,
                            return_field: Field::new("result", DataType::Utf8, true)
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

fn create_int64_list_array(array_size: usize) -> ArrayRef {
    let mut rng = StdRng::seed_from_u64(SEED);
    let values = (0..NUM_ROWS * array_size)
        .map(|_| {
            if rng.random::<f64>() < NULL_DENSITY {
                None
            } else {
                Some(rng.random_range(0..1000))
            }
        })
        .collect::<Int64Array>();
    let offsets = (0..=NUM_ROWS)
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

fn create_nested_int64_list_array(array_size: usize) -> ArrayRef {
    let inner = create_int64_list_array(array_size);
    let inner_rows = NUM_ROWS;
    let outer_rows = inner_rows / NESTED_ARRAY_SIZE;
    let offsets = (0..=outer_rows)
        .map(|i| (i * NESTED_ARRAY_SIZE) as i32)
        .collect::<Vec<i32>>();
    Arc::new(
        ListArray::try_new(
            Arc::new(Field::new("item", inner.data_type().clone(), true)),
            OffsetBuffer::new(offsets.into()),
            inner,
            None,
        )
        .unwrap(),
    )
}

fn create_float64_list_array(array_size: usize) -> ArrayRef {
    let mut rng = StdRng::seed_from_u64(SEED);
    let values = (0..NUM_ROWS * array_size)
        .map(|_| {
            if rng.random::<f64>() < NULL_DENSITY {
                None
            } else {
                Some(rng.random_range(-1000.0..1000.0))
            }
        })
        .collect::<Float64Array>();
    let offsets = (0..=NUM_ROWS)
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

fn create_string_list_array(array_size: usize) -> ArrayRef {
    let mut rng = StdRng::seed_from_u64(SEED);
    let values = (0..NUM_ROWS * array_size)
        .map(|_| {
            if rng.random::<f64>() < NULL_DENSITY {
                None
            } else {
                Some(format!("value_{}", rng.random_range(0..100)))
            }
        })
        .collect::<StringArray>();
    let offsets = (0..=NUM_ROWS)
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
