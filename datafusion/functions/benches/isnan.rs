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

use arrow::datatypes::{DataType, Field};
use arrow::{
    datatypes::{Float32Type, Float64Type},
    util::bench_util::create_primitive_array,
};
use criterion::{black_box, criterion_group, criterion_main, Criterion};
use datafusion_common::config::ConfigOptions;
use datafusion_expr::{ColumnarValue, ScalarFunctionArgs};
use datafusion_functions::math::isnan;
use std::sync::Arc;

fn criterion_benchmark(c: &mut Criterion) {
    let isnan = isnan();
    for size in [1024, 4096, 8192] {
        let f32_array = Arc::new(create_primitive_array::<Float32Type>(size, 0.2));
        let f32_args = vec![ColumnarValue::Array(f32_array)];
        let arg_fields = f32_args
            .iter()
            .enumerate()
            .map(|(idx, arg)| {
                Field::new(format!("arg_{idx}"), arg.data_type(), true).into()
            })
            .collect::<Vec<_>>();
        let config_options = Arc::new(ConfigOptions::default());

        c.bench_function(&format!("isnan f32 array: {size}"), |b| {
            b.iter(|| {
                black_box(
                    isnan
                        .invoke_with_args(ScalarFunctionArgs {
                            args: f32_args.clone(),
                            arg_fields: arg_fields.clone(),
                            number_rows: size,
                            return_field: Field::new("f", DataType::Boolean, true).into(),
                            config_options: Arc::clone(&config_options),
                        })
                        .unwrap(),
                )
            })
        });
        let f64_array = Arc::new(create_primitive_array::<Float64Type>(size, 0.2));
        let f64_args = vec![ColumnarValue::Array(f64_array)];
        let arg_fields = f64_args
            .iter()
            .enumerate()
            .map(|(idx, arg)| {
                Field::new(format!("arg_{idx}"), arg.data_type(), true).into()
            })
            .collect::<Vec<_>>();
        c.bench_function(&format!("isnan f64 array: {size}"), |b| {
            b.iter(|| {
                black_box(
                    isnan
                        .invoke_with_args(ScalarFunctionArgs {
                            args: f64_args.clone(),
                            arg_fields: arg_fields.clone(),
                            number_rows: size,
                            return_field: Field::new("f", DataType::Boolean, true).into(),
                            config_options: Arc::clone(&config_options),
                        })
                        .unwrap(),
                )
            })
        });
    }
}

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);
