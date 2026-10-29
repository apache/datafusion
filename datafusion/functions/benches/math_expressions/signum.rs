// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: See the NOTICE file distributed with this work for additional information regarding copyright ownership
// SPDX-FileContributor: Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements

use arrow::datatypes::DataType;
use arrow::{
    datatypes::{Field, Float32Type, Float64Type},
    util::bench_util::create_primitive_array,
};
use criterion::{Criterion, criterion_group};
use datafusion_common::ScalarValue;
use datafusion_common::config::ConfigOptions;
use datafusion_expr::{ColumnarValue, ScalarFunctionArgs};
use datafusion_functions::math::signum;
use std::hint::black_box;
use std::sync::Arc;

fn criterion_benchmark(c: &mut Criterion) {
    let signum = signum();
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
        let return_field = Field::new("f", DataType::Float32, true).into();
        let config_options = Arc::new(ConfigOptions::default());

        c.bench_function(&format!("signum f32 array: {size}"), |b| {
            b.iter(|| {
                black_box(
                    signum
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
        let return_field = Field::new("f", DataType::Float64, true).into();

        c.bench_function(&format!("signum f64 array: {size}"), |b| {
            b.iter(|| {
                black_box(
                    signum
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

        // Scalar benchmarks (the optimization we added)
        let scalar_f32_args =
            vec![ColumnarValue::Scalar(ScalarValue::Float32(Some(-42.5)))];
        let scalar_f32_arg_fields =
            vec![Field::new("a", DataType::Float32, false).into()];
        let return_field_f32 = Field::new("f", DataType::Float32, false).into();

        c.bench_function(&format!("signum f32 scalar: {size}"), |b| {
            b.iter(|| {
                black_box(
                    signum
                        .invoke_with_args(ScalarFunctionArgs {
                            args: scalar_f32_args.clone(),
                            arg_fields: scalar_f32_arg_fields.clone(),
                            number_rows: 1,
                            return_field: Arc::clone(&return_field_f32),
                            config_options: Arc::clone(&config_options),
                        })
                        .unwrap(),
                )
            })
        });

        let scalar_f64_args =
            vec![ColumnarValue::Scalar(ScalarValue::Float64(Some(-42.5)))];
        let scalar_f64_arg_fields =
            vec![Field::new("a", DataType::Float64, false).into()];
        let return_field_f64 = Field::new("f", DataType::Float64, false).into();

        c.bench_function(&format!("signum f64 scalar: {size}"), |b| {
            b.iter(|| {
                black_box(
                    signum
                        .invoke_with_args(ScalarFunctionArgs {
                            args: scalar_f64_args.clone(),
                            arg_fields: scalar_f64_arg_fields.clone(),
                            number_rows: 1,
                            return_field: Arc::clone(&return_field_f64),
                            config_options: Arc::clone(&config_options),
                        })
                        .unwrap(),
                )
            })
        });
    }
}

criterion_group!(benches, criterion_benchmark);
