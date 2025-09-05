// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

extern crate criterion;
mod helper;

use arrow::datatypes::{DataType, Field};
use criterion::{black_box, criterion_group, criterion_main, Criterion};
use datafusion_common::config::ConfigOptions;
use datafusion_expr::ScalarFunctionArgs;
use helper::gen_string_array;
use std::sync::Arc;

fn criterion_benchmark(c: &mut Criterion) {
    // All benches are single batch run with 8192 rows
    let reverse = datafusion_functions::unicode::reverse();
    let config_options = Arc::new(ConfigOptions::default());

    const N_ROWS: usize = 8192;
    const NULL_DENSITY: f32 = 0.1;
    const UTF8_DENSITY_OF_ALL_ASCII: f32 = 0.0;
    const NORMAL_UTF8_DENSITY: f32 = 0.8;
    for str_len in [8, 32, 128, 4096] {
        // StringArray ASCII only
        let args_string_ascii = gen_string_array(
            N_ROWS,
            str_len,
            NULL_DENSITY,
            UTF8_DENSITY_OF_ALL_ASCII,
            false,
        );
        c.bench_function(
            &format!("reverse_StringArray_ascii_str_len_{str_len}"),
            |b| {
                b.iter(|| {
                    black_box(reverse.invoke_with_args(ScalarFunctionArgs {
                        args: args_string_ascii.clone(),
                        arg_fields: vec![Field::new(
                            "a",
                            args_string_ascii[0].data_type(),
                            true,
                        ).into()],
                        number_rows: N_ROWS,
                        return_field: Field::new("f", DataType::Utf8, true).into(),
                        config_options: Arc::clone(&config_options),
                    }))
                })
            },
        );

        // StringArray UTF8
        let args_string_utf8 =
            gen_string_array(N_ROWS, str_len, NULL_DENSITY, NORMAL_UTF8_DENSITY, false);
        c.bench_function(
            &format!(
                "reverse_StringArray_utf8_density_{NORMAL_UTF8_DENSITY}_str_len_{str_len}"
            ),
            |b| {
                b.iter(|| {
                    black_box(reverse.invoke_with_args(ScalarFunctionArgs {
                        args: args_string_utf8.clone(),
                        arg_fields: vec![
                            Field::new("a", args_string_utf8[0].data_type(), true).into(),
                        ],
                        number_rows: N_ROWS,
                        return_field: Field::new("f", DataType::Utf8, true).into(),
                        config_options: Arc::clone(&config_options),
                    }))
                })
            },
        );

        // StringViewArray ASCII only
        let args_string_view_ascii = gen_string_array(
            N_ROWS,
            str_len,
            NULL_DENSITY,
            UTF8_DENSITY_OF_ALL_ASCII,
            true,
        );
        c.bench_function(
            &format!("reverse_StringViewArray_ascii_str_len_{str_len}"),
            |b| {
                b.iter(|| {
                    black_box(reverse.invoke_with_args(ScalarFunctionArgs {
                        args: args_string_view_ascii.clone(),
                        arg_fields: vec![Field::new(
                            "a",
                            args_string_view_ascii[0].data_type(),
                            true,
                        ).into()],
                        number_rows: N_ROWS,
                        return_field: Field::new("f", DataType::Utf8, true).into(),
                        config_options: Arc::clone(&config_options),
                    }))
                })
            },
        );

        // StringViewArray UTF8
        let args_string_view_utf8 =
            gen_string_array(N_ROWS, str_len, NULL_DENSITY, NORMAL_UTF8_DENSITY, true);
        c.bench_function(
            &format!(
                "reverse_StringViewArray_utf8_density_{NORMAL_UTF8_DENSITY}_str_len_{str_len}"
            ),
            |b| {
                b.iter(|| {
                    black_box(reverse.invoke_with_args(ScalarFunctionArgs {
                        args: args_string_view_utf8.clone(),
                        arg_fields: vec![Field::new(
                            "a",
                            args_string_view_utf8[0].data_type(),
                            true,
                        ).into()],
                        number_rows: N_ROWS,
                        return_field: Field::new("f", DataType::Utf8, true).into(),
                        config_options: Arc::clone(&config_options),
                    }))
                })
            },
        );
    }
}

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);
