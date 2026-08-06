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
use criterion::{Criterion, criterion_group, criterion_main};
use datafusion_common::ScalarValue;
use datafusion_common::config::ConfigOptions;
use datafusion_expr::{ColumnarValue, ScalarFunctionArgs};
use datafusion_functions::math::nanvl;
use std::hint::black_box;
use std::sync::Arc;

fn criterion_benchmark(c: &mut Criterion) {
    let nanvl_fn = nanvl();
    let config_options = Arc::new(ConfigOptions::default());

    // Scalar benchmarks
    c.bench_function("nanvl/scalar_f64", |b| {
        let args = ScalarFunctionArgs {
            args: vec![
                ColumnarValue::Scalar(ScalarValue::Float64(Some(f64::NAN))),
                ColumnarValue::Scalar(ScalarValue::Float64(Some(1.0))),
            ],
            arg_fields: vec![
                Field::new("a", DataType::Float64, true).into(),
                Field::new("b", DataType::Float64, true).into(),
            ],
            number_rows: 1,
            return_field: Field::new("f", DataType::Float64, true).into(),
            config_options: Arc::clone(&config_options),
        };

        b.iter(|| black_box(nanvl_fn.invoke_with_args(args.clone()).unwrap()))
    });

    c.bench_function("nanvl/scalar_f32", |b| {
        let args = ScalarFunctionArgs {
            args: vec![
                ColumnarValue::Scalar(ScalarValue::Float32(Some(f32::NAN))),
                ColumnarValue::Scalar(ScalarValue::Float32(Some(1.0))),
            ],
            arg_fields: vec![
                Field::new("a", DataType::Float32, true).into(),
                Field::new("b", DataType::Float32, true).into(),
            ],
            number_rows: 1,
            return_field: Field::new("f", DataType::Float32, true).into(),
            config_options: Arc::clone(&config_options),
        };

        b.iter(|| black_box(nanvl_fn.invoke_with_args(args.clone()).unwrap()))
    });

    // Array benchmarks
    for size in [1024, 4096, 8192] {
        let a64: ArrayRef = Arc::new(Float64Array::from(vec![f64::NAN; size]));
        let b64: ArrayRef = Arc::new(Float64Array::from(vec![1.0; size]));
        c.bench_function(&format!("nanvl/array_f64/{size}"), |bench| {
            let args = ScalarFunctionArgs {
                args: vec![
                    ColumnarValue::Array(Arc::clone(&a64)),
                    ColumnarValue::Array(Arc::clone(&b64)),
                ],
                arg_fields: vec![
                    Field::new("a", DataType::Float64, true).into(),
                    Field::new("b", DataType::Float64, true).into(),
                ],
                number_rows: size,
                return_field: Field::new("f", DataType::Float64, true).into(),
                config_options: Arc::clone(&config_options),
            };
            bench.iter(|| black_box(nanvl_fn.invoke_with_args(args.clone()).unwrap()))
        });

        let a32: ArrayRef = Arc::new(Float32Array::from(vec![f32::NAN; size]));
        let b32: ArrayRef = Arc::new(Float32Array::from(vec![1.0; size]));
        c.bench_function(&format!("nanvl/array_f32/{size}"), |bench| {
            let args = ScalarFunctionArgs {
                args: vec![
                    ColumnarValue::Array(Arc::clone(&a32)),
                    ColumnarValue::Array(Arc::clone(&b32)),
                ],
                arg_fields: vec![
                    Field::new("a", DataType::Float32, true).into(),
                    Field::new("b", DataType::Float32, true).into(),
                ],
                number_rows: size,
                return_field: Field::new("f", DataType::Float32, true).into(),
                config_options: Arc::clone(&config_options),
            };
            bench.iter(|| black_box(nanvl_fn.invoke_with_args(args.clone()).unwrap()))
        });
    }

    // Partially-null array benchmarks exercise the null-aware match arms that
    // the fully-populated benchmarks above never reach: only-x-null,
    // only-y-null, and both-null.
    let bench_pair =
        |c: &mut Criterion, name: &str, x: ArrayRef, y: ArrayRef, size: usize| {
            c.bench_function(name, |bench| {
                let args = ScalarFunctionArgs {
                    args: vec![
                        ColumnarValue::Array(Arc::clone(&x)),
                        ColumnarValue::Array(Arc::clone(&y)),
                    ],
                    arg_fields: vec![
                        Field::new("a", DataType::Float64, true).into(),
                        Field::new("b", DataType::Float64, true).into(),
                    ],
                    number_rows: size,
                    return_field: Field::new("f", DataType::Float64, true).into(),
                    config_options: Arc::clone(&config_options),
                };
                bench.iter(|| black_box(nanvl_fn.invoke_with_args(args.clone()).unwrap()))
            });
        };

    for size in [1024, 4096, 8192] {
        // `x` mixes non-NaN, NaN, and null so every code path is taken.
        let x_nulls: ArrayRef = Arc::new(Float64Array::from(
            (0..size)
                .map(|i| match i % 3 {
                    0 => Some(1.0),
                    1 => Some(f64::NAN),
                    _ => None,
                })
                .collect::<Vec<_>>(),
        ));
        // `x` without nulls, alternating non-NaN and NaN.
        let x_full: ArrayRef = Arc::new(Float64Array::from(
            (0..size)
                .map(|i| if i % 2 == 0 { 1.0 } else { f64::NAN })
                .collect::<Vec<_>>(),
        ));
        // `y` with roughly a quarter nulls.
        let y_nulls: ArrayRef = Arc::new(Float64Array::from(
            (0..size)
                .map(|i| if i % 4 == 3 { None } else { Some(2.0) })
                .collect::<Vec<_>>(),
        ));
        let y_full: ArrayRef = Arc::new(Float64Array::from(vec![2.0; size]));

        // (Some, None): only `x` has nulls.
        bench_pair(
            c,
            &format!("nanvl/array_f64_x_nulls/{size}"),
            Arc::clone(&x_nulls),
            Arc::clone(&y_full),
            size,
        );
        // (None, Some): only `y` has nulls.
        bench_pair(
            c,
            &format!("nanvl/array_f64_y_nulls/{size}"),
            Arc::clone(&x_full),
            Arc::clone(&y_nulls),
            size,
        );
        // (Some, Some): both inputs have nulls.
        bench_pair(
            c,
            &format!("nanvl/array_f64_both_nulls/{size}"),
            Arc::clone(&x_nulls),
            Arc::clone(&y_nulls),
            size,
        );
    }
}

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);
