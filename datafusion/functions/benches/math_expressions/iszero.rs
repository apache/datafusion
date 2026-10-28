// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: See the NOTICE file distributed with this work for additional information regarding copyright ownership
// SPDX-FileContributor: Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements

use arrow::datatypes::{DataType, Field};
use arrow::{
    datatypes::{Float32Type, Float64Type},
    util::bench_util::create_primitive_array,
};
use criterion::{Criterion, criterion_group};
use datafusion_common::ScalarValue;
use datafusion_common::config::ConfigOptions;
use datafusion_expr::{ColumnarValue, ScalarFunctionArgs};
use datafusion_functions::math::iszero;
use std::hint::black_box;
use std::sync::Arc;

fn criterion_benchmark(c: &mut Criterion) {
    let iszero = iszero();
    let config_options = Arc::new(ConfigOptions::default());

    for size in [1024, 4096, 8192] {
        let f32_array = Arc::new(create_primitive_array::<Float32Type>(size, 0.2));
        let batch_len = f32_array.len();
        let f32_args = vec![ColumnarValue::Array(f32_array)];
        let arg_fields = f32_args
            .iter()
            .enumerate()
            .map(|(idx, arg)| {
                Field::new(format!("arg_{idx}"), arg.data_type(), true).into()
            })
            .collect::<Vec<_>>();
        let return_field = Arc::new(Field::new("f", DataType::Boolean, true));

        c.bench_function(&format!("iszero f32 array: {size}"), |b| {
            b.iter(|| {
                black_box(
                    iszero
                        .invoke_with_args(ScalarFunctionArgs {
                            args: f32_args.clone(),
                            arg_fields: arg_fields.clone(),
                            number_rows: batch_len,
                            return_field: Arc::clone(&return_field),
                            config_options: Arc::clone(&config_options),
                        })
                        .unwrap(),
                )
            })
        });

        let f64_array = Arc::new(create_primitive_array::<Float64Type>(size, 0.2));
        let batch_len = f64_array.len();
        let f64_args = vec![ColumnarValue::Array(f64_array)];
        let arg_fields = f64_args
            .iter()
            .enumerate()
            .map(|(idx, arg)| {
                Field::new(format!("arg_{idx}"), arg.data_type(), true).into()
            })
            .collect::<Vec<_>>();
        let return_field = Arc::new(Field::new("f", DataType::Boolean, true));

        c.bench_function(&format!("iszero f64 array: {size}"), |b| {
            b.iter(|| {
                black_box(
                    iszero
                        .invoke_with_args(ScalarFunctionArgs {
                            args: f64_args.clone(),
                            arg_fields: arg_fields.clone(),
                            number_rows: batch_len,
                            return_field: Arc::clone(&return_field),
                            config_options: Arc::clone(&config_options),
                        })
                        .unwrap(),
                )
            })
        });
    }

    // Scalar benchmarks - run once since size doesn't affect scalar performance
    let scalar_f32_args = vec![ColumnarValue::Scalar(ScalarValue::Float32(Some(1.0)))];
    let scalar_f32_arg_fields = vec![Field::new("a", DataType::Float32, false).into()];
    let return_field_scalar = Arc::new(Field::new("f", DataType::Boolean, false));

    c.bench_function("iszero f32 scalar", |b| {
        b.iter(|| {
            black_box(
                iszero
                    .invoke_with_args(ScalarFunctionArgs {
                        args: scalar_f32_args.clone(),
                        arg_fields: scalar_f32_arg_fields.clone(),
                        number_rows: 1,
                        return_field: Arc::clone(&return_field_scalar),
                        config_options: Arc::clone(&config_options),
                    })
                    .unwrap(),
            )
        })
    });

    let scalar_f64_args = vec![ColumnarValue::Scalar(ScalarValue::Float64(Some(1.0)))];
    let scalar_f64_arg_fields = vec![Field::new("a", DataType::Float64, false).into()];

    c.bench_function("iszero f64 scalar", |b| {
        b.iter(|| {
            black_box(
                iszero
                    .invoke_with_args(ScalarFunctionArgs {
                        args: scalar_f64_args.clone(),
                        arg_fields: scalar_f64_arg_fields.clone(),
                        number_rows: 1,
                        return_field: Arc::clone(&return_field_scalar),
                        config_options: Arc::clone(&config_options),
                    })
                    .unwrap(),
            )
        })
    });
}

criterion_group!(benches, criterion_benchmark);
