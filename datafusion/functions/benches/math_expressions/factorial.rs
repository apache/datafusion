// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: See the NOTICE file distributed with this work for additional information regarding copyright ownership
// SPDX-FileContributor: Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements

use arrow::array::Int64Array;
use arrow::datatypes::{DataType, Field};
use criterion::{Criterion, criterion_group};
use datafusion_common::ScalarValue;
use datafusion_common::config::ConfigOptions;
use datafusion_expr::ScalarFunctionArgs;
use datafusion_expr_common::columnar_value::ColumnarValue;
use datafusion_functions::math::factorial;
use std::hint::black_box;
use std::sync::Arc;

fn criterion_benchmark(c: &mut Criterion) {
    let factorial = factorial();
    let config_options = Arc::new(ConfigOptions::default());

    let arr_args = vec![ColumnarValue::Array(Arc::new(Int64Array::from_iter(
        (0..1024).map(|i| Some(i % 21)),
    )))];
    c.bench_function(&format!("{}_array", factorial.name()), |b| {
        b.iter(|| {
            let args_cloned = arr_args.clone();
            black_box(factorial.invoke_with_args(ScalarFunctionArgs {
                args: args_cloned,
                arg_fields: vec![Field::new("a", DataType::Utf8, true).into()],
                number_rows: arr_args.len(),
                return_field: Field::new("f", DataType::Utf8, true).into(),
                config_options: Arc::clone(&config_options),
            }))
        })
    });

    let scalar_args = vec![ColumnarValue::Scalar(ScalarValue::Int64(Some(20)))];
    c.bench_function(&format!("{}_scalar", factorial.name()), |b| {
        b.iter(|| {
            let args_cloned = scalar_args.clone();
            black_box(factorial.invoke_with_args(ScalarFunctionArgs {
                args: args_cloned,
                arg_fields: vec![Field::new("a", DataType::Utf8, true).into()],
                number_rows: 1,
                return_field: Field::new("f", DataType::Utf8, true).into(),
                config_options: Arc::clone(&config_options),
            }))
        })
    });
}

criterion_group!(benches, criterion_benchmark);
