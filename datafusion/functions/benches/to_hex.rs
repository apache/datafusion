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

use arrow::datatypes::{DataType, Field, Int32Type, Int64Type};
use arrow::util::bench_util::create_primitive_array;
use criterion::{black_box, criterion_group, criterion_main, Criterion};
use datafusion_common::config::ConfigOptions;
use datafusion_expr::{ColumnarValue, ScalarFunctionArgs};
use datafusion_functions::string;
use std::sync::Arc;

fn criterion_benchmark(c: &mut Criterion) {
    let hex = string::to_hex();
    let size = 1024;
    let i32_array = Arc::new(create_primitive_array::<Int32Type>(size, 0.2));
    let batch_len = i32_array.len();
    let i32_args = vec![ColumnarValue::Array(i32_array)];
    let config_options = Arc::new(ConfigOptions::default());

    c.bench_function(&format!("to_hex i32 array: {size}"), |b| {
        b.iter(|| {
            let args_cloned = i32_args.clone();
            black_box(
                hex.invoke_with_args(ScalarFunctionArgs {
                    args: args_cloned,
                    arg_fields: vec![Field::new("a", DataType::Int32, false).into()],
                    number_rows: batch_len,
                    return_field: Field::new("f", DataType::Utf8, true).into(),
                    config_options: Arc::clone(&config_options),
                })
                .unwrap(),
            )
        })
    });
    let i64_array = Arc::new(create_primitive_array::<Int64Type>(size, 0.2));
    let batch_len = i64_array.len();
    let i64_args = vec![ColumnarValue::Array(i64_array)];
    c.bench_function(&format!("to_hex i64 array: {size}"), |b| {
        b.iter(|| {
            let args_cloned = i64_args.clone();
            black_box(
                hex.invoke_with_args(ScalarFunctionArgs {
                    args: args_cloned,
                    arg_fields: vec![Field::new("a", DataType::Int64, false).into()],
                    number_rows: batch_len,
                    return_field: Field::new("f", DataType::Utf8, true).into(),
                    config_options: Arc::clone(&config_options),
                })
                .unwrap(),
            )
        })
    });
}

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);
