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

use criterion::{black_box, criterion_group, criterion_main, Criterion};
use helper::gen_string_array;

fn criterion_benchmark(c: &mut Criterion) {
    // All benches are single batch run with 8192 rows
    let reverse = datafusion_functions::unicode::reverse();

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
            &format!("reverse_StringArray_ascii_str_len_{}", str_len),
            |b| {
                b.iter(|| {
                    // TODO use invoke_with_args
                    black_box(reverse.invoke_batch(&args_string_ascii, N_ROWS))
                })
            },
        );

        // StringArray UTF8
        let args_string_utf8 =
            gen_string_array(N_ROWS, str_len, NULL_DENSITY, NORMAL_UTF8_DENSITY, false);
        c.bench_function(
            &format!(
                "reverse_StringArray_utf8_density_{}_str_len_{}",
                NORMAL_UTF8_DENSITY, str_len
            ),
            |b| {
                b.iter(|| {
                    // TODO use invoke_with_args
                    black_box(reverse.invoke_batch(&args_string_utf8, N_ROWS))
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
            &format!("reverse_StringViewArray_ascii_str_len_{}", str_len),
            |b| {
                b.iter(|| {
                    // TODO use invoke_with_args
                    black_box(reverse.invoke_batch(&args_string_view_ascii, N_ROWS))
                })
            },
        );

        // StringViewArray UTF8
        let args_string_view_utf8 =
            gen_string_array(N_ROWS, str_len, NULL_DENSITY, NORMAL_UTF8_DENSITY, true);
        c.bench_function(
            &format!(
                "reverse_StringViewArray_utf8_density_{}_str_len_{}",
                NORMAL_UTF8_DENSITY, str_len
            ),
            |b| {
                b.iter(|| {
                    // TODO use invoke_with_args
                    black_box(reverse.invoke_batch(&args_string_view_utf8, N_ROWS))
                })
            },
        );
    }
}

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);
