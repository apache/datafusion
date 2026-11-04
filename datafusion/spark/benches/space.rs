// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: See the NOTICE file distributed with this work for additional information regarding copyright ownership
// SPDX-FileContributor: Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements

use arrow::array::PrimitiveArray;
use arrow::datatypes::{DataType, Field, Int32Type};
use criterion::{Criterion, criterion_group, criterion_main};
use datafusion_common::config::ConfigOptions;
use datafusion_expr::{ColumnarValue, ScalarFunctionArgs};
use datafusion_spark::function::string::space;
use rand::prelude::StdRng;
use rand::{Rng, SeedableRng};
use std::hint::black_box;
use std::sync::Arc;

fn criterion_benchmark(c: &mut Criterion) {
    let space_func = space();
    let size = 1024;
    let input: PrimitiveArray<Int32Type> = {
        let null_density = 0.2;
        let mut rng = StdRng::seed_from_u64(42);
        (0..size)
            .map(|_| {
                if rng.random::<f32>() < null_density {
                    None
                } else {
                    Some(rng.random_range::<i32, _>(1i32..10))
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
    let config_options = Arc::new(ConfigOptions::default());
    c.bench_function("space", |b| {
        b.iter(|| {
            black_box(
                space_func
                    .invoke_with_args(ScalarFunctionArgs {
                        args: args.clone(),
                        arg_fields: arg_fields.clone(),
                        number_rows: size,
                        return_field: Arc::new(Field::new("f", DataType::Utf8, true)),
                        config_options: Arc::clone(&config_options),
                    })
                    .unwrap(),
            )
        })
    });
}

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);
