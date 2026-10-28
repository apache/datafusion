// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: See the NOTICE file distributed with this work for additional information regarding copyright ownership
// SPDX-FileContributor: Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements

//! Benchmarks the `trunc(value, precision)` array path where `precision` is a
//! constant (scalar) argument.

use arrow::datatypes::{DataType, Field, Float32Type, Float64Type};
use arrow::util::bench_util::create_primitive_array;
use criterion::{Criterion, criterion_group};
use datafusion_common::ScalarValue;
use datafusion_common::config::ConfigOptions;
use datafusion_expr::{ColumnarValue, ScalarFunctionArgs};
use datafusion_functions::math::trunc;
use std::hint::black_box;
use std::sync::Arc;

fn criterion_benchmark(c: &mut Criterion) {
    let trunc = trunc();
    let config_options = Arc::new(ConfigOptions::default());

    for size in [1024, 4096, 8192] {
        let f64_array = Arc::new(create_primitive_array::<Float64Type>(size, 0.2));
        let f64_args = vec![
            ColumnarValue::Array(f64_array),
            ColumnarValue::Scalar(ScalarValue::Int64(Some(3))),
        ];
        let arg_fields = vec![
            Field::new("a", DataType::Float64, true).into(),
            Field::new("p", DataType::Int64, false).into(),
        ];
        let return_field = Field::new("f", DataType::Float64, true).into();
        c.bench_function(&format!("trunc f64 precision array: {size}"), |b| {
            b.iter(|| {
                black_box(
                    trunc
                        .invoke_with_args(ScalarFunctionArgs {
                            args: f64_args.clone(),
                            arg_fields: arg_fields.clone(),
                            number_rows: size,
                            return_field: Arc::clone(&return_field),
                            config_options: Arc::clone(&config_options),
                        })
                        .unwrap(),
                )
            })
        });

        let f32_array = Arc::new(create_primitive_array::<Float32Type>(size, 0.2));
        let f32_args = vec![
            ColumnarValue::Array(f32_array),
            ColumnarValue::Scalar(ScalarValue::Int64(Some(3))),
        ];
        let arg_fields = vec![
            Field::new("a", DataType::Float32, true).into(),
            Field::new("p", DataType::Int64, false).into(),
        ];
        let return_field = Field::new("f", DataType::Float32, true).into();
        c.bench_function(&format!("trunc f32 precision array: {size}"), |b| {
            b.iter(|| {
                black_box(
                    trunc
                        .invoke_with_args(ScalarFunctionArgs {
                            args: f32_args.clone(),
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
}

criterion_group!(benches, criterion_benchmark);
