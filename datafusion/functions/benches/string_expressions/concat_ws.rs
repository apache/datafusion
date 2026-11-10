// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: See the NOTICE file distributed with this work for additional information regarding copyright ownership
// SPDX-FileContributor: Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements

use arrow::array::ArrayRef;
use arrow::datatypes::{DataType, Field};
use arrow::util::bench_util::create_string_array_with_len;
use criterion::{BenchmarkId, Criterion, criterion_group};
use datafusion_common::ScalarValue;
use datafusion_common::config::ConfigOptions;
use datafusion_expr::{ColumnarValue, ScalarFunctionArgs};
use datafusion_functions::string::concat_ws;
use rand::distr::Alphanumeric;
use rand::prelude::*;
use std::hint::black_box;
use std::sync::Arc;

fn create_array_args(size: usize, str_len: usize) -> Vec<ColumnarValue> {
    let array = Arc::new(create_string_array_with_len::<i32>(size, 0.2, str_len));
    let scalar = ScalarValue::Utf8(Some(", ".to_string()));
    vec![
        ColumnarValue::Scalar(scalar),
        ColumnarValue::Array(Arc::clone(&array) as ArrayRef),
        ColumnarValue::Array(array),
    ]
}

fn generate_random_string(rng: &mut StdRng, str_len: usize) -> String {
    rng.sample_iter(&Alphanumeric)
        .take(str_len)
        .map(char::from)
        .collect()
}

fn create_scalar_args(count: usize, str_len: usize) -> Vec<ColumnarValue> {
    let mut args = Vec::with_capacity(count + 1);

    args.push(ColumnarValue::Scalar(ScalarValue::Utf8(Some(
        ",".to_string(),
    ))));

    let mut rng = StdRng::seed_from_u64(0);
    for _ in 0..count {
        let s = generate_random_string(&mut rng, str_len);
        args.push(ColumnarValue::Scalar(ScalarValue::Utf8(Some(s))));
    }
    args
}

fn criterion_benchmark(c: &mut Criterion) {
    // Benchmark for array concat_ws
    for size in [1024, 4096, 8192] {
        let args = create_array_args(size, 32);
        let arg_fields = args
            .iter()
            .enumerate()
            .map(|(idx, arg)| {
                Field::new(format!("arg_{idx}"), arg.data_type(), true).into()
            })
            .collect::<Vec<_>>();
        let config_options = Arc::new(ConfigOptions::default());

        let mut group = c.benchmark_group("concat_ws function");
        group.bench_function(BenchmarkId::new("concat_ws", size), |b| {
            b.iter(|| {
                let args_cloned = args.clone();
                black_box(
                    concat_ws()
                        .invoke_with_args(ScalarFunctionArgs {
                            args: args_cloned,
                            arg_fields: arg_fields.clone(),
                            number_rows: size,
                            return_field: Field::new("f", DataType::Utf8, true).into(),
                            config_options: Arc::clone(&config_options),
                        })
                        .unwrap(),
                )
            })
        });
        group.finish();
    }

    // Benchmark for scalar concat_ws
    let scalar_args = create_scalar_args(10, 100);
    let scalar_arg_fields = scalar_args
        .iter()
        .enumerate()
        .map(|(idx, arg)| Field::new(format!("arg_{idx}"), arg.data_type(), true).into())
        .collect::<Vec<_>>();
    let mut group = c.benchmark_group("concat_ws function");
    group.bench_function(BenchmarkId::new("concat_ws", "scalar"), |b| {
        b.iter(|| {
            let args_cloned = scalar_args.clone();
            black_box(
                concat_ws()
                    .invoke_with_args(ScalarFunctionArgs {
                        args: args_cloned,
                        arg_fields: scalar_arg_fields.clone(),
                        number_rows: 1,
                        return_field: Field::new("f", DataType::Utf8, true).into(),
                        config_options: Arc::new(ConfigOptions::default()),
                    })
                    .unwrap(),
            )
        })
    });
}

criterion_group!(benches, criterion_benchmark);
