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

extern crate criterion;

use arrow::array::Int64Array;
use arrow::datatypes::{DataType, Field, Int32Type, Int64Type};
use arrow::util::bench_util::create_primitive_array;
use criterion::{Criterion, SamplingMode, criterion_group, criterion_main};
use datafusion_common::ScalarValue;
use datafusion_common::config::ConfigOptions;
use datafusion_expr::{ColumnarValue, ScalarFunctionArgs};
use datafusion_functions::string;
use std::hint::black_box;
use std::sync::Arc;
use std::time::Duration;

fn criterion_benchmark(c: &mut Criterion) {
    let hex = string::to_hex();
    let config_options = Arc::new(ConfigOptions::default());

    c.bench_function("to_hex/scalar_i32", |b| {
        let args = vec![ColumnarValue::Scalar(ScalarValue::Int32(Some(2147483647)))];
        let arg_fields = vec![Field::new("a", DataType::Int32, true).into()];
        b.iter(|| {
            black_box(
                hex.invoke_with_args(ScalarFunctionArgs {
                    args: args.clone(),
                    arg_fields: arg_fields.clone(),
                    number_rows: 1,
                    return_field: Field::new("f", DataType::Utf8, true).into(),
                    config_options: Arc::clone(&config_options),
                })
                .unwrap(),
            )
        })
    });

    c.bench_function("to_hex/scalar_i64", |b| {
        let args = vec![ColumnarValue::Scalar(ScalarValue::Int64(Some(
            9223372036854775807,
        )))];
        let arg_fields = vec![Field::new("a", DataType::Int64, true).into()];
        b.iter(|| {
            black_box(
                hex.invoke_with_args(ScalarFunctionArgs {
                    args: args.clone(),
                    arg_fields: arg_fields.clone(),
                    number_rows: 1,
                    return_field: Field::new("f", DataType::Utf8, true).into(),
                    config_options: Arc::clone(&config_options),
                })
                .unwrap(),
            )
        })
    });

    for size in [1024, 4096, 8192] {
        let mut group = c.benchmark_group(format!("to_hex size={size}"));
        group.sampling_mode(SamplingMode::Flat);
        group.sample_size(10);
        group.measurement_time(Duration::from_secs(10));

        // i32 array with random values
        let i32_array = Arc::new(create_primitive_array::<Int32Type>(size, 0.1));
        let batch_len = i32_array.len();
        let i32_args = vec![ColumnarValue::Array(i32_array)];

        group.bench_function("i32_random", |b| {
            b.iter(|| {
                let args_cloned = i32_args.clone();
                black_box(
                    hex.invoke_with_args(ScalarFunctionArgs {
                        args: args_cloned,
                        arg_fields: vec![Field::new("a", DataType::Int32, true).into()],
                        number_rows: batch_len,
                        return_field: Field::new("f", DataType::Utf8, true).into(),
                        config_options: Arc::clone(&config_options),
                    })
                    .unwrap(),
                )
            })
        });

        // i64 array with random values (produces longer hex strings)
        let i64_array = Arc::new(create_primitive_array::<Int64Type>(size, 0.1));
        let batch_len = i64_array.len();
        let i64_args = vec![ColumnarValue::Array(i64_array)];

        group.bench_function("i64_random", |b| {
            b.iter(|| {
                let args_cloned = i64_args.clone();
                black_box(
                    hex.invoke_with_args(ScalarFunctionArgs {
                        args: args_cloned,
                        arg_fields: vec![Field::new("a", DataType::Int64, true).into()],
                        number_rows: batch_len,
                        return_field: Field::new("f", DataType::Utf8, true).into(),
                        config_options: Arc::clone(&config_options),
                    })
                    .unwrap(),
                )
            })
        });

        // i64 array with large values (max length hex strings)
        let i64_large_array = Arc::new(Int64Array::from(
            (0..size)
                .map(|i| {
                    if i % 10 == 0 {
                        None
                    } else {
                        Some(i64::MAX - i as i64)
                    }
                })
                .collect::<Vec<_>>(),
        ));
        let batch_len = i64_large_array.len();
        let i64_large_args = vec![ColumnarValue::Array(i64_large_array)];

        group.bench_function("i64_large_values", |b| {
            b.iter(|| {
                let args_cloned = i64_large_args.clone();
                black_box(
                    hex.invoke_with_args(ScalarFunctionArgs {
                        args: args_cloned,
                        arg_fields: vec![Field::new("a", DataType::Int64, true).into()],
                        number_rows: batch_len,
                        return_field: Field::new("f", DataType::Utf8, true).into(),
                        config_options: Arc::clone(&config_options),
                    })
                    .unwrap(),
                )
            })
        });

        group.finish();
    }
}

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);
