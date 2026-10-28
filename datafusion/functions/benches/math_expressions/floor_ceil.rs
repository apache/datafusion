// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: See the NOTICE file distributed with this work for additional information regarding copyright ownership
// SPDX-FileContributor: Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements

use arrow::datatypes::{DataType, Field, Float64Type};
use arrow::util::bench_util::create_primitive_array;
use criterion::{Criterion, SamplingMode, criterion_group};
use datafusion_common::ScalarValue;
use datafusion_common::config::ConfigOptions;
use datafusion_expr::{ColumnarValue, ScalarFunctionArgs};
use datafusion_functions::math::{ceil, floor};
use std::hint::black_box;
use std::sync::Arc;
use std::time::Duration;

fn criterion_benchmark(c: &mut Criterion) {
    let floor_fn = floor();
    let ceil_fn = ceil();
    let config_options = Arc::new(ConfigOptions::default());

    for size in [1024, 4096, 8192] {
        let mut group = c.benchmark_group(format!("floor_ceil size={size}"));
        group.sampling_mode(SamplingMode::Flat);
        group.sample_size(10);
        group.measurement_time(Duration::from_secs(10));

        // Float64 array benchmark
        let f64_array = Arc::new(create_primitive_array::<Float64Type>(size, 0.1));
        let batch_len = f64_array.len();
        let f64_args = vec![ColumnarValue::Array(f64_array)];

        group.bench_function("floor_f64_array", |b| {
            b.iter(|| {
                let args_cloned = f64_args.clone();
                black_box(
                    floor_fn
                        .invoke_with_args(ScalarFunctionArgs {
                            args: args_cloned,
                            arg_fields: vec![
                                Field::new("a", DataType::Float64, true).into(),
                            ],
                            number_rows: batch_len,
                            return_field: Field::new("f", DataType::Float64, true).into(),
                            config_options: Arc::clone(&config_options),
                        })
                        .unwrap(),
                )
            })
        });

        group.bench_function("ceil_f64_array", |b| {
            b.iter(|| {
                let args_cloned = f64_args.clone();
                black_box(
                    ceil_fn
                        .invoke_with_args(ScalarFunctionArgs {
                            args: args_cloned,
                            arg_fields: vec![
                                Field::new("a", DataType::Float64, true).into(),
                            ],
                            number_rows: batch_len,
                            return_field: Field::new("f", DataType::Float64, true).into(),
                            config_options: Arc::clone(&config_options),
                        })
                        .unwrap(),
                )
            })
        });

        // Scalar benchmark (the optimization we added)
        let scalar_args = vec![ColumnarValue::Scalar(ScalarValue::Float64(Some(
            std::f64::consts::PI,
        )))];

        group.bench_function("floor_f64_scalar", |b| {
            b.iter(|| {
                let args_cloned = scalar_args.clone();
                black_box(
                    floor_fn
                        .invoke_with_args(ScalarFunctionArgs {
                            args: args_cloned,
                            arg_fields: vec![
                                Field::new("a", DataType::Float64, false).into(),
                            ],
                            number_rows: 1,
                            return_field: Field::new("f", DataType::Float64, false)
                                .into(),
                            config_options: Arc::clone(&config_options),
                        })
                        .unwrap(),
                )
            })
        });

        group.bench_function("ceil_f64_scalar", |b| {
            b.iter(|| {
                let args_cloned = scalar_args.clone();
                black_box(
                    ceil_fn
                        .invoke_with_args(ScalarFunctionArgs {
                            args: args_cloned,
                            arg_fields: vec![
                                Field::new("a", DataType::Float64, false).into(),
                            ],
                            number_rows: 1,
                            return_field: Field::new("f", DataType::Float64, false)
                                .into(),
                            config_options: Arc::clone(&config_options),
                        })
                        .unwrap(),
                )
            })
        });

        group.finish();
    }
}

criterion_group!(benches, criterion_benchmark);
