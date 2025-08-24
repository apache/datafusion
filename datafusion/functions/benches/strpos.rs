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

use arrow::array::{StringArray, StringViewArray};
use arrow::datatypes::{DataType, Field};
use criterion::{black_box, criterion_group, criterion_main, Criterion};
use datafusion_common::config::ConfigOptions;
use datafusion_expr::{ColumnarValue, ScalarFunctionArgs};
use rand::distr::Alphanumeric;
use rand::prelude::StdRng;
use rand::{Rng, SeedableRng};
use std::str::Chars;
use std::sync::Arc;

/// gen_arr(4096, 128, 0.1, 0.1, true) will generate a StringViewArray with
/// 4096 rows, each row containing a string with 128 random characters.
/// around 10% of the rows are null, around 10% of the rows are non-ASCII.
fn gen_string_array(
    n_rows: usize,
    str_len_chars: usize,
    null_density: f32,
    utf8_density: f32,
    is_string_view: bool, // false -> StringArray, true -> StringViewArray
) -> Vec<ColumnarValue> {
    let mut rng = StdRng::seed_from_u64(42);
    let rng_ref = &mut rng;

    let utf8 = "DatafusionДатаФусион数据融合📊🔥"; // includes utf8 encoding with 1~4 bytes
    let corpus_char_count = utf8.chars().count();

    let mut output_string_vec: Vec<Option<String>> = Vec::with_capacity(n_rows);
    let mut output_sub_string_vec: Vec<Option<String>> = Vec::with_capacity(n_rows);
    for _ in 0..n_rows {
        let rand_num = rng_ref.random::<f32>(); // [0.0, 1.0)
        if rand_num < null_density {
            output_sub_string_vec.push(None);
            output_string_vec.push(None);
        } else if rand_num < null_density + utf8_density {
            // Generate random UTF8 string
            let mut generated_string = String::with_capacity(str_len_chars);
            for _ in 0..str_len_chars {
                let idx = rng_ref.random_range(0..corpus_char_count);
                let char = utf8.chars().nth(idx).unwrap();
                generated_string.push(char);
            }
            output_sub_string_vec.push(Some(random_substring(generated_string.chars())));
            output_string_vec.push(Some(generated_string));
        } else {
            // Generate random ASCII-only string
            let value = rng_ref
                .sample_iter(&Alphanumeric)
                .take(str_len_chars)
                .collect();
            let value = String::from_utf8(value).unwrap();
            output_sub_string_vec.push(Some(random_substring(value.chars())));
            output_string_vec.push(Some(value));
        }
    }

    if is_string_view {
        let string_view_array: StringViewArray = output_string_vec.into_iter().collect();
        let sub_string_view_array: StringViewArray =
            output_sub_string_vec.into_iter().collect();
        vec![
            ColumnarValue::Array(Arc::new(string_view_array)),
            ColumnarValue::Array(Arc::new(sub_string_view_array)),
        ]
    } else {
        let string_array: StringArray = output_string_vec.clone().into_iter().collect();
        let sub_string_array: StringArray = output_sub_string_vec.into_iter().collect();
        vec![
            ColumnarValue::Array(Arc::new(string_array)),
            ColumnarValue::Array(Arc::new(sub_string_array)),
        ]
    }
}

fn random_substring(chars: Chars) -> String {
    // get the substring of a random length from the input string by byte unit
    let mut rng = StdRng::seed_from_u64(44);
    let count = chars.clone().count();
    let start = rng.random_range(0..count - 1);
    let end = rng.random_range(start + 1..count);
    chars
        .enumerate()
        .filter(|(i, _)| *i >= start && *i < end)
        .map(|(_, c)| c)
        .collect()
}

fn criterion_benchmark(c: &mut Criterion) {
    // All benches are single batch run with 8192 rows
    let strpos = datafusion_functions::unicode::strpos();

    let n_rows = 8192;
    for str_len in [8, 32, 128, 4096] {
        // StringArray ASCII only
        let args_string_ascii = gen_string_array(n_rows, str_len, 0.1, 0.0, false);
        let arg_fields =
            vec![Field::new("a", args_string_ascii[0].data_type(), true).into()];
        let return_field = Field::new("f", DataType::Int32, true).into();
        let config_options = Arc::new(ConfigOptions::default());

        c.bench_function(
            &format!("strpos_StringArray_ascii_str_len_{str_len}"),
            |b| {
                b.iter(|| {
                    black_box(strpos.invoke_with_args(ScalarFunctionArgs {
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
        let arg_fields =
            vec![Field::new("a", args_string_utf8[0].data_type(), true).into()];
        let return_field = Field::new("f", DataType::Int32, true).into();
        c.bench_function(&format!("strpos_StringArray_utf8_str_len_{str_len}"), |b| {
            b.iter(|| {
                black_box(strpos.invoke_with_args(ScalarFunctionArgs {
                    args: args_string_utf8.clone(),
                    arg_fields: arg_fields.clone(),
                    number_rows: n_rows,
                    return_field: Arc::clone(&return_field),
                    config_options: Arc::clone(&config_options),
                }))
            })
        });

        // StringViewArray ASCII only
        let args_string_view_ascii = gen_string_array(n_rows, str_len, 0.1, 0.0, true);
        let arg_fields =
            vec![Field::new("a", args_string_view_ascii[0].data_type(), true).into()];
        let return_field = Field::new("f", DataType::Int32, true).into();
        c.bench_function(
            &format!("strpos_StringViewArray_ascii_str_len_{str_len}"),
            |b| {
                b.iter(|| {
                    black_box(strpos.invoke_with_args(ScalarFunctionArgs {
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
        let arg_fields =
            vec![Field::new("a", args_string_view_utf8[0].data_type(), true).into()];
        let return_field = Field::new("f", DataType::Int32, true).into();
        c.bench_function(
            &format!("strpos_StringViewArray_utf8_str_len_{str_len}"),
            |b| {
                b.iter(|| {
                    black_box(strpos.invoke_with_args(ScalarFunctionArgs {
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
criterion_main!(benches);
