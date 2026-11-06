// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: See the NOTICE file distributed with this work for additional information regarding copyright ownership
// SPDX-FileContributor: Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements

use arrow::datatypes::Field;
use arrow::{
    array::{ArrayRef, Int64Array},
    datatypes::DataType,
};
use criterion::{Criterion, criterion_group};
use datafusion_common::config::ConfigOptions;
use datafusion_expr::{ColumnarValue, ScalarFunctionArgs};
use datafusion_functions::math::lcm;
use rand::prelude::*;
use std::hint::black_box;
use std::sync::Arc;

fn generate_i64_array(rng: &mut StdRng, n_rows: usize) -> ArrayRef {
    let values = (0..n_rows)
        .map(|_| rng.random_range(0..1000))
        .collect::<Vec<_>>();
    Arc::new(Int64Array::from(values)) as ArrayRef
}

fn criterion_benchmark(c: &mut Criterion) {
    let n_rows = 100000;
    let mut rng = StdRng::seed_from_u64(0);
    let array_a = ColumnarValue::Array(generate_i64_array(&mut rng, n_rows));
    let array_b = ColumnarValue::Array(generate_i64_array(&mut rng, n_rows));
    let udf = lcm();
    let config_options = Arc::new(ConfigOptions::default());

    c.bench_function("lcm both array", |b| {
        b.iter(|| {
            black_box(
                udf.invoke_with_args(ScalarFunctionArgs {
                    args: vec![array_a.clone(), array_b.clone()],
                    arg_fields: vec![
                        Field::new("a", array_a.data_type(), true).into(),
                        Field::new("b", array_b.data_type(), true).into(),
                    ],
                    number_rows: n_rows,
                    return_field: Field::new("f", DataType::Int64, true).into(),
                    config_options: Arc::clone(&config_options),
                })
                .expect("lcm should work on valid values"),
            )
        })
    });
}

criterion_group!(benches, criterion_benchmark);
