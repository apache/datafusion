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

use std::hint::black_box;
use std::sync::Arc;

use arrow::array::{ArrayRef, Int64Array};
use arrow::datatypes::{DataType, Field};
use arrow::util::bench_util::{
    create_string_array_with_len, create_string_view_array_with_len,
};
use criterion::{BenchmarkId, Criterion, criterion_group, criterion_main};
use datafusion_common::config::ConfigOptions;
use datafusion_expr::{ColumnarValue, ScalarFunctionArgs};
use datafusion_functions::unicode::right;

fn create_args(
    size: usize,
    str_len: usize,
    use_negative: bool,
    is_string_view: bool,
) -> Vec<ColumnarValue> {
    let string_arg = if is_string_view {
        ColumnarValue::Array(Arc::new(create_string_view_array_with_len(
            size, 0.1, str_len, true,
        )))
    } else {
        ColumnarValue::Array(Arc::new(create_string_array_with_len::<i32>(
            size, 0.1, str_len,
        )))
    };

    // For negative n, we want to trigger the double-iteration code path
    let n_values: Vec<i64> = if use_negative {
        (0..size).map(|i| -((i % 10 + 1) as i64)).collect()
    } else {
        (0..size).map(|i| (i % 10 + 1) as i64).collect()
    };
    let n_array = Arc::new(Int64Array::from(n_values));

    vec![
        string_arg,
        ColumnarValue::Array(Arc::clone(&n_array) as ArrayRef),
    ]
}

fn criterion_benchmark(c: &mut Criterion) {
    for is_string_view in [false, true] {
        for size in [1024, 4096] {
            let mut group = c.benchmark_group(format!("right size={size}"));

            // Benchmark with positive n (no optimization needed)
            let mut function_name = if is_string_view {
                "string_view_array positive n"
            } else {
                "string_array positive n"
            };
            let args = create_args(size, 32, false, is_string_view);
            group.bench_function(BenchmarkId::new(function_name, size), |b| {
                let arg_fields = args
                    .iter()
                    .enumerate()
                    .map(|(idx, arg)| {
                        Field::new(format!("arg_{idx}"), arg.data_type(), true).into()
                    })
                    .collect::<Vec<_>>();
                let config_options = Arc::new(ConfigOptions::default());

                b.iter(|| {
                    black_box(
                        right()
                            .invoke_with_args(ScalarFunctionArgs {
                                args: args.clone(),
                                arg_fields: arg_fields.clone(),
                                number_rows: size,
                                return_field: Field::new("f", DataType::Utf8, true)
                                    .into(),
                                config_options: Arc::clone(&config_options),
                            })
                            .expect("right should work"),
                    )
                })
            });

            // Benchmark with negative n (triggers optimization)
            function_name = if is_string_view {
                "string_view_array negative n"
            } else {
                "string_array negative n"
            };
            let args = create_args(size, 32, true, is_string_view);
            group.bench_function(BenchmarkId::new(function_name, size), |b| {
                let arg_fields = args
                    .iter()
                    .enumerate()
                    .map(|(idx, arg)| {
                        Field::new(format!("arg_{idx}"), arg.data_type(), true).into()
                    })
                    .collect::<Vec<_>>();
                let config_options = Arc::new(ConfigOptions::default());

                b.iter(|| {
                    black_box(
                        right()
                            .invoke_with_args(ScalarFunctionArgs {
                                args: args.clone(),
                                arg_fields: arg_fields.clone(),
                                number_rows: size,
                                return_field: Field::new("f", DataType::Utf8, true)
                                    .into(),
                                config_options: Arc::clone(&config_options),
                            })
                            .expect("right should work"),
                    )
                })
            });

            group.finish();
        }
    }
}

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);
