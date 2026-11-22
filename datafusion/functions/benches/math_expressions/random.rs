// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: See the NOTICE file distributed with this work for additional information regarding copyright ownership
// SPDX-FileContributor: Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements

use arrow::datatypes::{DataType, Field};
use criterion::{Criterion, criterion_group};
use datafusion_common::config::ConfigOptions;
use datafusion_expr::{ScalarFunctionArgs, ScalarUDFImpl};
use datafusion_functions::math::random::RandomFunc;
use std::hint::black_box;
use std::sync::Arc;

fn criterion_benchmark(c: &mut Criterion) {
    let random_func = RandomFunc::new();
    let return_field = Field::new("f", DataType::Float64, true).into();
    let config_options = Arc::new(ConfigOptions::default());

    // Benchmark to evaluate 1M rows in batch size 8192
    let iterations = 1_000_000 / 8192; // Calculate how many iterations are needed to reach approximately 1M rows
    c.bench_function("random_1M_rows_batch_8192", |b| {
        b.iter(|| {
            for _ in 0..iterations {
                black_box(
                    random_func
                        .invoke_with_args(ScalarFunctionArgs {
                            args: vec![],
                            arg_fields: vec![],
                            number_rows: 8192,
                            return_field: Arc::clone(&return_field),
                            config_options: Arc::clone(&config_options),
                        })
                        .unwrap(),
                );
            }
        })
    });

    let return_field = Field::new("f", DataType::Float64, true).into();
    // Benchmark to evaluate 1M rows in batch size 128
    let iterations_128 = 1_000_000 / 128; // Calculate how many iterations are needed to reach approximately 1M rows with batch size 128
    c.bench_function("random_1M_rows_batch_128", |b| {
        b.iter(|| {
            for _ in 0..iterations_128 {
                black_box(
                    random_func
                        .invoke_with_args(ScalarFunctionArgs {
                            args: vec![],
                            arg_fields: vec![],
                            number_rows: 128,
                            return_field: Arc::clone(&return_field),
                            config_options: Arc::clone(&config_options),
                        })
                        .unwrap(),
                );
            }
        })
    });
}

criterion_group!(benches, criterion_benchmark);
