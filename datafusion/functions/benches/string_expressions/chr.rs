// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: See the NOTICE file distributed with this work for additional information regarding copyright ownership
// SPDX-FileContributor: Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements

use arrow::{array::PrimitiveArray, datatypes::Int64Type};
use criterion::{Criterion, criterion_group};
use datafusion_common::ScalarValue;
use datafusion_expr::{ColumnarValue, ScalarFunctionArgs};
use datafusion_functions::string::chr;
use rand::{Rng, SeedableRng};
use std::hint::black_box;

use arrow::datatypes::{DataType, Field};
use datafusion_common::config::ConfigOptions;
use rand::rngs::StdRng;
use std::sync::Arc;

/// Returns fixed seedable RNG
pub fn seedable_rng() -> StdRng {
    StdRng::seed_from_u64(42)
}

fn criterion_benchmark(c: &mut Criterion) {
    let chr_fn = chr();
    let config_options = Arc::new(ConfigOptions::default());

    // Scalar benchmarks
    c.bench_function("chr/scalar", |b| {
        let args = vec![ColumnarValue::Scalar(ScalarValue::Int64(Some(65)))];
        let arg_fields = vec![Field::new("arg_0", DataType::Int64, true).into()];
        b.iter(|| {
            black_box(
                chr_fn
                    .invoke_with_args(ScalarFunctionArgs {
                        args: args.clone(),
                        arg_fields: arg_fields.clone(),
                        number_rows: 1,
                        return_field: Field::new("f", DataType::Utf8, true).into(),
                        config_options: Arc::clone(&config_options),
                    })
                    .unwrap(),
            )
        })
    });

    let size = 1024;
    let input: PrimitiveArray<Int64Type> = {
        let null_density = 0.2;
        let mut rng = seedable_rng();
        (0..size)
            .map(|_| {
                if rng.random::<f32>() < null_density {
                    None
                } else {
                    Some(rng.random_range::<i64, _>(1i64..10_000))
                }
            })
            .collect()
    };
    let input = Arc::new(input);
    let args = vec![ColumnarValue::Array(input)];
    let arg_fields = args
        .iter()
        .enumerate()
        .map(|(idx, arg)| Field::new(format!("arg_{idx}"), arg.data_type(), true).into())
        .collect::<Vec<_>>();

    c.bench_function("chr/array", |b| {
        b.iter(|| {
            black_box(
                chr_fn
                    .invoke_with_args(ScalarFunctionArgs {
                        args: args.clone(),
                        arg_fields: arg_fields.clone(),
                        number_rows: size,
                        return_field: Field::new("f", DataType::Utf8, true).into(),
                        config_options: Arc::clone(&config_options),
                    })
                    .unwrap(),
            )
        })
    });
}

criterion_group!(benches, criterion_benchmark);
