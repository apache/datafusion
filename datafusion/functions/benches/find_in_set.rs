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
use arrow::util::bench_util::{
    create_string_array_with_len, create_string_view_array_with_len,
};
use criterion::{black_box, criterion_group, criterion_main, Criterion, SamplingMode};
use datafusion_common::config::ConfigOptions;
use datafusion_common::ScalarValue;
use datafusion_expr::{ColumnarValue, ScalarFunctionArgs};
use rand::distr::Alphanumeric;
use rand::prelude::StdRng;
use rand::{Rng, SeedableRng};
use std::sync::Arc;
use std::time::Duration;

/// gen_arr(4096, 128, 0.1, 0.1, true) will generate a StringViewArray with
/// 4096 rows, each row containing a string with 128 random characters.
/// around 10% of the rows are null, around 10% of the rows are non-ASCII.
fn gen_args_array(
    n_rows: usize,
    str_len_chars: usize,
    null_density: f32,
    utf8_density: f32,
    is_string_view: bool, // false -> StringArray, true -> StringViewArray
) -> Vec<ColumnarValue> {
    let mut rng = StdRng::seed_from_u64(42);
    let rng_ref = &mut rng;

    let num_elements = 5; // 5 elements separated by comma
    let utf8 = "DataFusionДатаФусион数据融合📊🔥"; // includes utf8 encoding with 1~4 bytes
    let corpus_char_count = utf8.chars().count();

    let mut output_set_vec: Vec<Option<String>> = Vec::with_capacity(n_rows);
    let mut output_element_vec: Vec<Option<String>> = Vec::with_capacity(n_rows);
    for _ in 0..n_rows {
        let rand_num = rng_ref.random::<f32>(); // [0.0, 1.0)
        if rand_num < null_density {
            output_element_vec.push(None);
            output_set_vec.push(None);
        } else if rand_num < null_density + utf8_density {
            // Generate random UTF-8 string with comma separators
            let mut generated_string = String::with_capacity(str_len_chars);
            for i in 0..num_elements {
                for _ in 0..str_len_chars {
                    let idx = rng_ref.random_range(0..corpus_char_count);
                    let char = utf8.chars().nth(idx).unwrap();
                    generated_string.push(char);
                }
                if i < num_elements - 1 {
                    generated_string.push(',');
                }
            }
            output_element_vec.push(Some(random_element_in_set(&generated_string)));
            output_set_vec.push(Some(generated_string));
        } else {
            // Generate random ASCII-only string with comma separators
            let mut generated_string = String::with_capacity(str_len_chars);
            for i in 0..num_elements {
                for _ in 0..str_len_chars {
                    let c = rng_ref.sample(Alphanumeric);
                    generated_string.push(c as char);
                }
                if i < num_elements - 1 {
                    generated_string.push(',');
                }
            }
            output_element_vec.push(Some(random_element_in_set(&generated_string)));
            output_set_vec.push(Some(generated_string));
        }
    }

    if is_string_view {
        let set_array: StringViewArray = output_set_vec.into_iter().collect();
        let element_array: StringViewArray = output_element_vec.into_iter().collect();
        vec![
            ColumnarValue::Array(Arc::new(element_array)),
            ColumnarValue::Array(Arc::new(set_array)),
        ]
    } else {
        let set_array: StringArray = output_set_vec.clone().into_iter().collect();
        let element_array: StringArray = output_element_vec.into_iter().collect();
        vec![
            ColumnarValue::Array(Arc::new(element_array)),
            ColumnarValue::Array(Arc::new(set_array)),
        ]
    }
}

fn random_element_in_set(string: &str) -> String {
    let elements: Vec<&str> = string.split(',').collect();

    if elements.is_empty() || (elements.len() == 1 && elements[0].is_empty()) {
        return String::new();
    }

    let mut rng = StdRng::seed_from_u64(44);
    let random_index = rng.random_range(0..elements.len());

    elements[random_index].to_string()
}

fn gen_args_scalar(
    n_rows: usize,
    str_len_chars: usize,
    null_density: f32,
    is_string_view: bool, // false -> StringArray, true -> StringViewArray
) -> Vec<ColumnarValue> {
    let str_list = "Apache,DataFusion,SQL,Query,Engine".to_string();
    if is_string_view {
        let string =
            create_string_view_array_with_len(n_rows, null_density, str_len_chars, false);
        vec![
            ColumnarValue::Array(Arc::new(string)),
            ColumnarValue::Scalar(ScalarValue::Utf8(Some(str_list))),
        ]
    } else {
        let string =
            create_string_array_with_len::<i32>(n_rows, null_density, str_len_chars);
        vec![
            ColumnarValue::Array(Arc::new(string)),
            ColumnarValue::Scalar(ScalarValue::Utf8(Some(str_list))),
        ]
    }
}

fn criterion_benchmark(c: &mut Criterion) {
    // All benches are single batch run with 8192 rows
    let find_in_set = datafusion_functions::unicode::find_in_set();

    let n_rows = 8192;
    for str_len in [8, 32, 1024] {
        let mut group = c.benchmark_group("find_in_set");
        group.sampling_mode(SamplingMode::Flat);
        group.sample_size(50);
        group.measurement_time(Duration::from_secs(10));

        let args = gen_args_array(n_rows, str_len, 0.1, 0.5, false);
        let arg_fields = args
            .iter()
            .map(|arg| Field::new("a", arg.data_type().clone(), true).into())
            .collect::<Vec<_>>();
        let return_field = Field::new("f", DataType::Int32, true).into();
        group.bench_function(format!("string_len_{str_len}"), |b| {
            b.iter(|| {
                black_box(find_in_set.invoke_with_args(ScalarFunctionArgs {
                    args: args.clone(),
                    arg_fields: arg_fields.clone(),
                    number_rows: n_rows,
                    return_field: Arc::clone(&return_field),
                    config_options: Arc::new(ConfigOptions::default()),
                }))
            })
        });

        let args = gen_args_array(n_rows, str_len, 0.1, 0.5, true);
        let arg_fields = args
            .iter()
            .map(|arg| Field::new("a", arg.data_type().clone(), true).into())
            .collect::<Vec<_>>();
        let return_field = Arc::new(Field::new("f", DataType::Int32, true));
        group.bench_function(format!("string_view_len_{str_len}"), |b| {
            b.iter(|| {
                black_box(find_in_set.invoke_with_args(ScalarFunctionArgs {
                    args: args.clone(),
                    arg_fields: arg_fields.clone(),
                    number_rows: n_rows,
                    return_field: Arc::clone(&return_field),
                    config_options: Arc::new(ConfigOptions::default()),
                }))
            })
        });

        group.finish();

        let mut group = c.benchmark_group("find_in_set_scalar");

        let args = gen_args_scalar(n_rows, str_len, 0.1, false);
        let arg_fields = args
            .iter()
            .map(|arg| Field::new("a", arg.data_type().clone(), true).into())
            .collect::<Vec<_>>();
        let return_field = Arc::new(Field::new("f", DataType::Int32, true));
        group.bench_function(format!("string_len_{str_len}"), |b| {
            b.iter(|| {
                black_box(find_in_set.invoke_with_args(ScalarFunctionArgs {
                    args: args.clone(),
                    arg_fields: arg_fields.clone(),
                    number_rows: n_rows,
                    return_field: Arc::clone(&return_field),
                    config_options: Arc::new(ConfigOptions::default()),
                }))
            })
        });

        let args = gen_args_scalar(n_rows, str_len, 0.1, true);
        let arg_fields = args
            .iter()
            .map(|arg| Field::new("a", arg.data_type().clone(), true).into())
            .collect::<Vec<_>>();
        let return_field = Arc::new(Field::new("f", DataType::Int32, true));
        let config_options = Arc::new(ConfigOptions::default());

        group.bench_function(format!("string_view_len_{str_len}"), |b| {
            b.iter(|| {
                black_box(find_in_set.invoke_with_args(ScalarFunctionArgs {
                    args: args.clone(),
                    arg_fields: arg_fields.clone(),
                    number_rows: n_rows,
                    return_field: Arc::clone(&return_field),
                    config_options: Arc::clone(&config_options),
                }))
            })
        });

        group.finish();
    }
}

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);
