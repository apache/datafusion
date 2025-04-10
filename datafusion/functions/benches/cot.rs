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

use arrow::datatypes::DataType;
use arrow::{
    datatypes::{Float32Type, Float64Type},
    util::bench_util::create_primitive_array,
};
use criterion::{black_box, criterion_group, criterion_main, Criterion};
use datafusion_common::config::ConfigOptions;
use datafusion_expr::{ColumnarValue, ScalarFunctionArgs};
use datafusion_functions::math::cot;

use std::sync::Arc;

fn criterion_benchmark(c: &mut Criterion) {
    let cot_fn = cot();
    let config_options = ConfigOptions::default_singleton_arc();

    for size in [1024, 4096, 8192] {
        let f32_array = Arc::new(create_primitive_array::<Float32Type>(size, 0.2));
        let f32_args = vec![ColumnarValue::Array(f32_array)];
        c.bench_function(&format!("cot f32 array: {}", size), |b| {
            b.iter(|| {
                black_box(
                    cot_fn
                        .invoke_with_args(ScalarFunctionArgs {
                            args: f32_args.clone(),
                            number_rows: size,
                            return_type: &DataType::Float32,
                            config_options,
                        })
                        .unwrap(),
                )
            })
        });
        let f64_array = Arc::new(create_primitive_array::<Float64Type>(size, 0.2));
        let f64_args = vec![ColumnarValue::Array(f64_array)];
        c.bench_function(&format!("cot f64 array: {}", size), |b| {
            b.iter(|| {
                black_box(
                    cot_fn
                        .invoke_with_args(ScalarFunctionArgs {
                            args: f64_args.clone(),
                            number_rows: size,
                            return_type: &DataType::Float64,
                            config_options,
                        })
                        .unwrap(),
                )
            })
        });
    }
}

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);
