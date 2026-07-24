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

use arrow::array::{ArrayRef, Float32Array, Float64Array};
use arrow::datatypes::{DataType, Field};
use arrow::util::bench_util::create_primitive_array;
use criterion::{Criterion, criterion_group, criterion_main};
use datafusion_common::ScalarValue;
use datafusion_common::config::ConfigOptions;
use datafusion_expr::{ColumnarValue, ScalarFunctionArgs, ScalarUDFImpl};
use datafusion_functions::math::log::LogFunc;
use std::hint::black_box;
use std::sync::Arc;

fn run(c: &mut Criterion, name: &str, args: Vec<ColumnarValue>, return_type: DataType) {
    let log = LogFunc::new();
    let size = args
        .iter()
        .filter_map(|arg| match arg {
            ColumnarValue::Array(array) => Some(array.len()),
            ColumnarValue::Scalar(_) => None,
        })
        .max()
        .unwrap();
    let arg_fields: Vec<_> = args
        .iter()
        .enumerate()
        .map(|(idx, arg)| Field::new(format!("arg_{idx}"), arg.data_type(), true).into())
        .collect();
    let return_field = Arc::new(Field::new("f", return_type, true));
    let config_options = Arc::new(ConfigOptions::default());

    c.bench_function(name, |b| {
        b.iter(|| {
            black_box(
                log.invoke_with_args(ScalarFunctionArgs {
                    args: args.clone(),
                    arg_fields: arg_fields.clone(),
                    number_rows: size,
                    return_field: Arc::clone(&return_field),
                    config_options: Arc::clone(&config_options),
                })
                .unwrap(),
            )
        })
    });
}

fn criterion_benchmark(c: &mut Criterion) {
    let size = 8192;
    let f64_array = Arc::new(create_primitive_array::<arrow::datatypes::Float64Type>(
        size, 0.1,
    )) as ArrayRef;
    let f32_array = Arc::new(create_primitive_array::<arrow::datatypes::Float32Type>(
        size, 0.1,
    )) as ArrayRef;

    // log(x), which defaults to base 10
    run(
        c,
        "log_f64_default_base",
        vec![ColumnarValue::Array(Arc::clone(&f64_array))],
        DataType::Float64,
    );

    // log(base, x) with a scalar base
    run(
        c,
        "log_f64_scalar_base",
        vec![
            ColumnarValue::Scalar(ScalarValue::Float64(Some(2.0))),
            ColumnarValue::Array(Arc::clone(&f64_array)),
        ],
        DataType::Float64,
    );

    run(
        c,
        "log_f32_scalar_base",
        vec![
            ColumnarValue::Scalar(ScalarValue::Float32(Some(2.0))),
            ColumnarValue::Array(Arc::clone(&f32_array)),
        ],
        DataType::Float32,
    );

    // log(base, x) with a base column, which cannot hoist the base logarithm
    let f64_bases = Arc::new(Float64Array::from_iter_values(
        (0..size).map(|i| 2.0 + (i % 8) as f64),
    )) as ArrayRef;
    run(
        c,
        "log_f64_array_base",
        vec![
            ColumnarValue::Array(f64_bases),
            ColumnarValue::Array(Arc::clone(&f64_array)),
        ],
        DataType::Float64,
    );

    let f32_bases = Arc::new(Float32Array::from_iter_values(
        (0..size).map(|i| 2.0 + (i % 8) as f32),
    )) as ArrayRef;
    run(
        c,
        "log_f32_array_base",
        vec![
            ColumnarValue::Array(f32_bases),
            ColumnarValue::Array(Arc::clone(&f32_array)),
        ],
        DataType::Float32,
    );
}

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);
