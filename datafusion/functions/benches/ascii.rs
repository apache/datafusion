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
use datafusion_expr::ScalarFunctionArgs;
use helper::gen_string_array;

fn criterion_benchmark(c: &mut Criterion) {
    let ascii = datafusion_functions::string::ascii();

    // All benches are single batch run with 8192 rows
    const N_ROWS: usize = 8192;
    const STR_LEN: usize = 16;
    const UTF8_DENSITY_OF_ALL_ASCII: f32 = 0.0;
    const NORMAL_UTF8_DENSITY: f32 = 0.8;

    for null_density in [0.0, 0.5] {
        // StringArray ASCII only
        let args_string_ascii = gen_string_array(
            N_ROWS,
            STR_LEN,
            null_density,
            UTF8_DENSITY_OF_ALL_ASCII,
            false,
        );
        c.bench_function(
            format!("ascii/string_ascii_only (null_density={null_density})").as_str(),
            |b| {
                b.iter(|| {
                    black_box(ascii.invoke_with_args(ScalarFunctionArgs {
                        args: args_string_ascii.clone(),
                        arg_fields: vec![&Field::new(
                            "a",
                            args_string_ascii[0].data_type(),
                            true,
                        )],
                        number_rows: N_ROWS,
                        return_field: &Field::new("f", DataType::Utf8, true),
                    }))
                })
            },
        );

        // StringArray UTF8
        let args_string_utf8 =
            gen_string_array(N_ROWS, STR_LEN, null_density, NORMAL_UTF8_DENSITY, false);
        c.bench_function(
            format!("ascii/string_utf8 (null_density={null_density})").as_str(),
            |b| {
                b.iter(|| {
                    black_box(ascii.invoke_with_args(ScalarFunctionArgs {
                        args: args_string_utf8.clone(),
                        arg_fields: vec![&Field::new(
                            "a",
                            args_string_utf8[0].data_type(),
                            true,
                        )],
                        number_rows: N_ROWS,
                        return_field: &Field::new("f", DataType::Utf8, true),
                    }))
                })
            },
        );

        // StringViewArray ASCII only
        let args_string_view_ascii = gen_string_array(
            N_ROWS,
            STR_LEN,
            null_density,
            UTF8_DENSITY_OF_ALL_ASCII,
            true,
        );
        c.bench_function(
            format!("ascii/string_view_ascii_only (null_density={null_density})")
                .as_str(),
            |b| {
                b.iter(|| {
                    black_box(ascii.invoke_with_args(ScalarFunctionArgs {
                        args: args_string_view_ascii.clone(),
                        arg_fields: vec![&Field::new(
                            "a",
                            args_string_view_ascii[0].data_type(),
                            true,
                        )],
                        number_rows: N_ROWS,
                        return_field: &Field::new("f", DataType::Utf8, true),
                    }))
                })
            },
        );

        // StringViewArray UTF8
        let args_string_view_utf8 =
            gen_string_array(N_ROWS, STR_LEN, null_density, NORMAL_UTF8_DENSITY, true);
        c.bench_function(
            format!("ascii/string_view_utf8 (null_density={null_density})").as_str(),
            |b| {
                b.iter(|| {
                    black_box(ascii.invoke_with_args(ScalarFunctionArgs {
                        args: args_string_view_utf8.clone(),
                        arg_fields: vec![&Field::new(
                            "a",
                            args_string_view_utf8[0].data_type(),
                            true,
                        )],
                        number_rows: N_ROWS,
                        return_field: &Field::new("f", DataType::Utf8, true),
                    }))
                })
            },
        );
    }
}

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);
