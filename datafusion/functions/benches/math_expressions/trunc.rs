// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: See the NOTICE file distributed with this work for additional information regarding copyright ownership
// SPDX-FileContributor: Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements

use arrow::{
    datatypes::{Field, Float32Type, Float64Type},
    util::bench_util::create_primitive_array,
};
use criterion::{Criterion, criterion_group};
use datafusion_expr::{ColumnarValue, ScalarFunctionArgs};
use datafusion_functions::math::trunc;
use std::hint::black_box;

use arrow::datatypes::DataType;
use datafusion_common::config::ConfigOptions;
use std::sync::Arc;

fn criterion_benchmark(c: &mut Criterion) {
    let trunc = trunc();
    let config_options = Arc::new(ConfigOptions::default());

    for size in [1024, 4096, 8192] {
        let f32_array = Arc::new(create_primitive_array::<Float32Type>(size, 0.2));
        let f32_args = vec![ColumnarValue::Array(f32_array)];
        let arg_fields = vec![Field::new("a", DataType::Float32, false).into()];
        let return_field = Field::new("f", DataType::Float32, true).into();

        c.bench_function(&format!("trunc f32 array: {size}"), |b| {
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
        let f64_array = Arc::new(create_primitive_array::<Float64Type>(size, 0.2));
        let f64_args = vec![ColumnarValue::Array(f64_array)];
        let arg_fields = vec![Field::new("a", DataType::Float64, true).into()];
        let return_field = Field::new("f", DataType::Float64, true).into();
        c.bench_function(&format!("trunc f64 array: {size}"), |b| {
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
    }

    // Scalar benchmarks - to measure optimized performance
    let scalar_f64_args = vec![ColumnarValue::Scalar(
        datafusion_common::ScalarValue::Float64(Some(std::f64::consts::PI)),
    )];
    let scalar_arg_fields = vec![Field::new("a", DataType::Float64, false).into()];
    let scalar_return_field = Field::new("f", DataType::Float64, false).into();

    c.bench_function("trunc f64 scalar", |b| {
        b.iter(|| {
            black_box(
                trunc
                    .invoke_with_args(ScalarFunctionArgs {
                        args: scalar_f64_args.clone(),
                        arg_fields: scalar_arg_fields.clone(),
                        number_rows: 1,
                        return_field: Arc::clone(&scalar_return_field),
                        config_options: Arc::clone(&config_options),
                    })
                    .unwrap(),
            )
        })
    });

    let scalar_f32_args = vec![ColumnarValue::Scalar(
        datafusion_common::ScalarValue::Float32(Some(std::f32::consts::PI)),
    )];
    let scalar_f32_arg_fields = vec![Field::new("a", DataType::Float32, false).into()];
    let scalar_f32_return_field = Field::new("f", DataType::Float32, false).into();

    c.bench_function("trunc f32 scalar", |b| {
        b.iter(|| {
            black_box(
                trunc
                    .invoke_with_args(ScalarFunctionArgs {
                        args: scalar_f32_args.clone(),
                        arg_fields: scalar_f32_arg_fields.clone(),
                        number_rows: 1,
                        return_field: Arc::clone(&scalar_f32_return_field),
                        config_options: Arc::clone(&config_options),
                    })
                    .unwrap(),
            )
        })
    });
}

criterion_group!(benches, criterion_benchmark);
