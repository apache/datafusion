// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: See the NOTICE file distributed with this work for additional information regarding copyright ownership
// SPDX-FileContributor: Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements

use crate::helper::gen_string_array;
use arrow::datatypes::{DataType, Field};
use criterion::{Criterion, criterion_group};
use datafusion_common::config::ConfigOptions;
use datafusion_expr::ScalarFunctionArgs;
use std::hint::black_box;
use std::sync::Arc;

fn criterion_benchmark(c: &mut Criterion) {
    // All benches are single batch run with 8192 rows
    let character_length = datafusion_functions::unicode::character_length();

    let return_field = Arc::new(Field::new("f", DataType::Utf8, true));
    let config_options = Arc::new(ConfigOptions::default());

    let n_rows = 8192;
    for str_len in [8, 32, 128, 4096] {
        // StringArray ASCII only
        let args_string_ascii = gen_string_array(n_rows, str_len, 0.1, 0.0, false);
        let arg_fields = args_string_ascii
            .iter()
            .enumerate()
            .map(|(idx, arg)| {
                Field::new(format!("arg_{idx}"), arg.data_type(), true).into()
            })
            .collect::<Vec<_>>();
        c.bench_function(
            &format!("character_length_StringArray_ascii_str_len_{str_len}"),
            |b| {
                b.iter(|| {
                    black_box(character_length.invoke_with_args(ScalarFunctionArgs {
                        args: args_string_ascii.clone(),
                        arg_fields: arg_fields.clone(),
                        number_rows: n_rows,
                        return_field: Arc::clone(&return_field),
                        config_options: Arc::clone(&config_options),
                    }))
                })
            },
        );

        // StringArray UTF8
        let args_string_utf8 = gen_string_array(n_rows, str_len, 0.1, 0.5, false);
        let arg_fields = args_string_utf8
            .iter()
            .enumerate()
            .map(|(idx, arg)| {
                Field::new(format!("arg_{idx}"), arg.data_type(), true).into()
            })
            .collect::<Vec<_>>();
        c.bench_function(
            &format!("character_length_StringArray_utf8_str_len_{str_len}"),
            |b| {
                b.iter(|| {
                    black_box(character_length.invoke_with_args(ScalarFunctionArgs {
                        args: args_string_utf8.clone(),
                        arg_fields: arg_fields.clone(),
                        number_rows: n_rows,
                        return_field: Arc::clone(&return_field),
                        config_options: Arc::clone(&config_options),
                    }))
                })
            },
        );

        // StringViewArray ASCII only
        let args_string_view_ascii = gen_string_array(n_rows, str_len, 0.1, 0.0, true);
        let arg_fields = args_string_view_ascii
            .iter()
            .enumerate()
            .map(|(idx, arg)| {
                Field::new(format!("arg_{idx}"), arg.data_type(), true).into()
            })
            .collect::<Vec<_>>();
        c.bench_function(
            &format!("character_length_StringViewArray_ascii_str_len_{str_len}"),
            |b| {
                b.iter(|| {
                    black_box(character_length.invoke_with_args(ScalarFunctionArgs {
                        args: args_string_view_ascii.clone(),
                        arg_fields: arg_fields.clone(),
                        number_rows: n_rows,
                        return_field: Arc::clone(&return_field),
                        config_options: Arc::clone(&config_options),
                    }))
                })
            },
        );

        // StringViewArray UTF8
        let args_string_view_utf8 = gen_string_array(n_rows, str_len, 0.1, 0.5, true);
        let arg_fields = args_string_view_utf8
            .iter()
            .enumerate()
            .map(|(idx, arg)| {
                Field::new(format!("arg_{idx}"), arg.data_type(), true).into()
            })
            .collect::<Vec<_>>();
        c.bench_function(
            &format!("character_length_StringViewArray_utf8_str_len_{str_len}"),
            |b| {
                b.iter(|| {
                    black_box(character_length.invoke_with_args(ScalarFunctionArgs {
                        args: args_string_view_utf8.clone(),
                        arg_fields: arg_fields.clone(),
                        number_rows: n_rows,
                        return_field: Arc::clone(&return_field),
                        config_options: Arc::clone(&config_options),
                    }))
                })
            },
        );
    }
}

criterion_group!(benches, criterion_benchmark);
