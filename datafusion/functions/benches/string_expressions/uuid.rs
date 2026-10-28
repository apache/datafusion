// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: See the NOTICE file distributed with this work for additional information regarding copyright ownership
// SPDX-FileContributor: Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements

use arrow::datatypes::{DataType, Field};
use criterion::{Criterion, criterion_group};
use datafusion_common::config::ConfigOptions;
use datafusion_expr::ScalarFunctionArgs;
use datafusion_functions::string;
use std::hint::black_box;
use std::sync::Arc;

fn criterion_benchmark(c: &mut Criterion) {
    let uuid = string::uuid();
    let config_options = Arc::new(ConfigOptions::default());

    c.bench_function("uuid", |b| {
        b.iter(|| {
            black_box(uuid.invoke_with_args(ScalarFunctionArgs {
                args: vec![],
                arg_fields: vec![],
                number_rows: 1024,
                return_field: Field::new("f", DataType::Utf8, true).into(),
                config_options: Arc::clone(&config_options),
            }))
        })
    });
}

criterion_group!(benches, criterion_benchmark);
