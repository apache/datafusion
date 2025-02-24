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

use criterion::{black_box, criterion_group, criterion_main, Criterion};
use helper::gen_string_array;

mod helper;

fn criterion_benchmark(c: &mut Criterion) {
    // All benches are single batch run with 8192 rows
    let character_length = datafusion_functions::unicode::character_length();

    let n_rows = 8192;
    for str_len in [8, 32, 128, 4096] {
        // StringArray ASCII only
        let args_string_ascii = gen_string_array(n_rows, str_len, 0.1, 0.0, false);
        c.bench_function(
            &format!("character_length_StringArray_ascii_str_len_{}", str_len),
            |b| {
                b.iter(|| {
                    // TODO use invoke_with_args
                    black_box(character_length.invoke_batch(&args_string_ascii, n_rows))
                })
            },
        );

        // StringArray UTF8
        let args_string_utf8 = gen_string_array(n_rows, str_len, 0.1, 0.5, false);
        c.bench_function(
            &format!("character_length_StringArray_utf8_str_len_{}", str_len),
            |b| {
                b.iter(|| {
                    // TODO use invoke_with_args
                    black_box(character_length.invoke_batch(&args_string_utf8, n_rows))
                })
            },
        );

        // StringViewArray ASCII only
        let args_string_view_ascii = gen_string_array(n_rows, str_len, 0.1, 0.0, true);
        c.bench_function(
            &format!("character_length_StringViewArray_ascii_str_len_{}", str_len),
            |b| {
                b.iter(|| {
                    // TODO use invoke_with_args
                    black_box(
                        character_length.invoke_batch(&args_string_view_ascii, n_rows),
                    )
                })
            },
        );

        // StringViewArray UTF8
        let args_string_view_utf8 = gen_string_array(n_rows, str_len, 0.1, 0.5, true);
        c.bench_function(
            &format!("character_length_StringViewArray_utf8_str_len_{}", str_len),
            |b| {
                b.iter(|| {
                    // TODO use invoke_with_args
                    black_box(
                        character_length.invoke_batch(&args_string_view_utf8, n_rows),
                    )
                })
            },
        );
    }
}

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);
