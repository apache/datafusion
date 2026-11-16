// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: See the NOTICE file distributed with this work for additional information regarding copyright ownership
// SPDX-FileContributor: Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements

use arrow::array::Array;
use arrow::datatypes::{DataType, Field};
use arrow::util::bench_util::create_binary_array;
use criterion::{Criterion, criterion_group, criterion_main};
use datafusion_common::config::ConfigOptions;
use datafusion_expr::{ColumnarValue, ScalarFunctionArgs};
use datafusion_functions::encoding;
use std::hint::black_box;
use std::sync::Arc;

fn criterion_benchmark(c: &mut Criterion) {
    let decode = encoding::decode();
    let encode = encoding::encode();
    let config_options = Arc::new(ConfigOptions::default());

    for size in [1024, 4096, 8192] {
        let bin_array = Arc::new(create_binary_array::<i32>(size, 0.2));

        c.bench_function(&format!("hex_encode/{size}"), |b| {
            let method = ColumnarValue::Scalar("hex".into());
            let arg_fields = vec![
                Field::new("a", bin_array.data_type().to_owned(), true).into(),
                Field::new("b", method.data_type().to_owned(), true).into(),
            ];
            let args = vec![ColumnarValue::Array(bin_array.clone()), method];
            let return_field = Field::new("f", DataType::Utf8, true).into();

            b.iter(|| {
                black_box(
                    encode
                        .invoke_with_args(ScalarFunctionArgs {
                            args: args.clone(),
                            arg_fields: arg_fields.clone(),
                            number_rows: size,
                            return_field: Arc::clone(&return_field),
                            config_options: Arc::clone(&config_options),
                        })
                        .unwrap(),
                )
            })
        });
        c.bench_function(&format!("base64_decode/{size}"), |b| {
            let method = ColumnarValue::Scalar("base64".into());
            let encoded = encoding::encode()
                .invoke_with_args(ScalarFunctionArgs {
                    args: vec![ColumnarValue::Array(bin_array.clone()), method.clone()],
                    arg_fields: vec![
                        Field::new("a", bin_array.data_type().to_owned(), true).into(),
                        Field::new("b", method.data_type().to_owned(), true).into(),
                    ],
                    number_rows: size,
                    return_field: Field::new("f", DataType::Utf8, true).into(),
                    config_options: Arc::clone(&config_options),
                })
                .unwrap()
                .cast_to(&DataType::Binary, None)
                .unwrap();

            let arg_fields = vec![
                Field::new("a", encoded.data_type().to_owned(), true).into(),
                Field::new("b", method.data_type().to_owned(), true).into(),
            ];
            let args = vec![encoded, method];

            b.iter(|| {
                black_box(
                    decode
                        .invoke_with_args(ScalarFunctionArgs {
                            args: args.clone(),
                            arg_fields: arg_fields.clone(),
                            number_rows: size,
                            return_field: Field::new("f", DataType::Binary, true).into(),
                            config_options: Arc::clone(&config_options),
                        })
                        .unwrap(),
                )
            })
        });

        c.bench_function(&format!("hex_decode/{size}"), |b| {
            let method = ColumnarValue::Scalar("hex".into());
            let arg_fields = vec![
                Field::new("a", bin_array.data_type().to_owned(), true).into(),
                Field::new("b", method.data_type().to_owned(), true).into(),
            ];
            let encoded = encoding::encode()
                .invoke_with_args(ScalarFunctionArgs {
                    args: vec![ColumnarValue::Array(bin_array.clone()), method.clone()],
                    arg_fields,
                    number_rows: size,
                    return_field: Field::new("f", DataType::Utf8, true).into(),
                    config_options: Arc::clone(&config_options),
                })
                .unwrap()
                .cast_to(&DataType::Binary, None)
                .unwrap();

            let arg_fields = vec![
                Field::new("a", encoded.data_type().to_owned(), true).into(),
                Field::new("b", method.data_type().to_owned(), true).into(),
            ];
            let return_field = Field::new("f", DataType::Binary, true).into();
            let args = vec![encoded, method];

            b.iter(|| {
                black_box(
                    decode
                        .invoke_with_args(ScalarFunctionArgs {
                            args: args.clone(),
                            arg_fields: arg_fields.clone(),
                            number_rows: size,
                            return_field: Arc::clone(&return_field),
                            config_options: Arc::clone(&config_options),
                        })
                        .unwrap(),
                )
            })
        });
    }
}

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);
