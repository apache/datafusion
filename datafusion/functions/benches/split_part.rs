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

use arrow::array::{ArrayRef, Int64Array, StringArray, StringViewArray};
use arrow::datatypes::{DataType, Field};
use criterion::{BenchmarkId, Criterion, criterion_group, criterion_main};
use datafusion_common::config::ConfigOptions;
use datafusion_expr::{ColumnarValue, ScalarFunctionArgs};
use datafusion_functions::string::split_part;
use rand::distr::Alphanumeric;
use rand::prelude::StdRng;
use rand::{Rng, SeedableRng};
use std::hint::black_box;
use std::sync::Arc;

const N_ROWS: usize = 8192;

/// Generate test data for split_part benchmarks
/// Creates strings with multiple parts separated by the delimiter
fn gen_split_part_data(
    n_rows: usize,
    num_parts: usize, // number of parts in each string (separated by delimiter)
    part_len: usize,  // length of each part
    delimiter: &str,  // the delimiter to use
    use_string_view: bool, // false -> StringArray, true -> StringViewArray
) -> (ColumnarValue, ColumnarValue) {
    let mut rng = StdRng::seed_from_u64(42);

    let mut strings: Vec<String> = Vec::with_capacity(n_rows);
    for _ in 0..n_rows {
        let mut parts: Vec<String> = Vec::with_capacity(num_parts);
        for _ in 0..num_parts {
            let part: String = (&mut rng)
                .sample_iter(&Alphanumeric)
                .take(part_len)
                .map(char::from)
                .collect();
            parts.push(part);
        }
        strings.push(parts.join(delimiter));
    }

    let delimiters: Vec<String> = vec![delimiter.to_string(); n_rows];

    if use_string_view {
        let string_array: StringViewArray = strings.into_iter().map(Some).collect();
        let delimiter_array: StringViewArray = delimiters.into_iter().map(Some).collect();
        (
            ColumnarValue::Array(Arc::new(string_array) as ArrayRef),
            ColumnarValue::Array(Arc::new(delimiter_array) as ArrayRef),
        )
    } else {
        let string_array: StringArray = strings.into_iter().map(Some).collect();
        let delimiter_array: StringArray = delimiters.into_iter().map(Some).collect();
        (
            ColumnarValue::Array(Arc::new(string_array) as ArrayRef),
            ColumnarValue::Array(Arc::new(delimiter_array) as ArrayRef),
        )
    }
}

fn gen_positions(n_rows: usize, position: i64) -> ColumnarValue {
    let positions: Vec<i64> = vec![position; n_rows];
    ColumnarValue::Array(Arc::new(Int64Array::from(positions)) as ArrayRef)
}

fn criterion_benchmark(c: &mut Criterion) {
    let split_part_func = split_part();
    let config_options = Arc::new(ConfigOptions::default());

    let mut group = c.benchmark_group("split_part");

    // Test different scenarios
    // Scenario 1: Single-char delimiter, first position (should be fastest with optimization)
    {
        let (strings, delimiters) = gen_split_part_data(N_ROWS, 10, 8, ".", false);
        let positions = gen_positions(N_ROWS, 1);
        let args = vec![strings, delimiters, positions];
        let arg_fields: Vec<_> = args
            .iter()
            .enumerate()
            .map(|(idx, arg)| {
                Field::new(format!("arg_{idx}"), arg.data_type(), true).into()
            })
            .collect();
        let return_field = Field::new("f", DataType::Utf8, true).into();

        group.bench_function(BenchmarkId::new("single_char_delim", "pos_first"), |b| {
            b.iter(|| {
                black_box(
                    split_part_func
                        .invoke_with_args(ScalarFunctionArgs {
                            args: args.clone(),
                            arg_fields: arg_fields.clone(),
                            number_rows: N_ROWS,
                            return_field: Arc::clone(&return_field),
                            config_options: Arc::clone(&config_options),
                        })
                        .expect("split_part should work"),
                )
            })
        });
    }

    // Scenario 2: Single-char delimiter, middle position
    {
        let (strings, delimiters) = gen_split_part_data(N_ROWS, 10, 8, ".", false);
        let positions = gen_positions(N_ROWS, 5);
        let args = vec![strings, delimiters, positions];
        let arg_fields: Vec<_> = args
            .iter()
            .enumerate()
            .map(|(idx, arg)| {
                Field::new(format!("arg_{idx}"), arg.data_type(), true).into()
            })
            .collect();
        let return_field = Field::new("f", DataType::Utf8, true).into();

        group.bench_function(BenchmarkId::new("single_char_delim", "pos_middle"), |b| {
            b.iter(|| {
                black_box(
                    split_part_func
                        .invoke_with_args(ScalarFunctionArgs {
                            args: args.clone(),
                            arg_fields: arg_fields.clone(),
                            number_rows: N_ROWS,
                            return_field: Arc::clone(&return_field),
                            config_options: Arc::clone(&config_options),
                        })
                        .expect("split_part should work"),
                )
            })
        });
    }

    // Scenario 3: Single-char delimiter, last position
    {
        let (strings, delimiters) = gen_split_part_data(N_ROWS, 10, 8, ".", false);
        let positions = gen_positions(N_ROWS, 10);
        let args = vec![strings, delimiters, positions];
        let arg_fields: Vec<_> = args
            .iter()
            .enumerate()
            .map(|(idx, arg)| {
                Field::new(format!("arg_{idx}"), arg.data_type(), true).into()
            })
            .collect();
        let return_field = Field::new("f", DataType::Utf8, true).into();

        group.bench_function(BenchmarkId::new("single_char_delim", "pos_last"), |b| {
            b.iter(|| {
                black_box(
                    split_part_func
                        .invoke_with_args(ScalarFunctionArgs {
                            args: args.clone(),
                            arg_fields: arg_fields.clone(),
                            number_rows: N_ROWS,
                            return_field: Arc::clone(&return_field),
                            config_options: Arc::clone(&config_options),
                        })
                        .expect("split_part should work"),
                )
            })
        });
    }

    // Scenario 4: Single-char delimiter, negative position (last element)
    {
        let (strings, delimiters) = gen_split_part_data(N_ROWS, 10, 8, ".", false);
        let positions = gen_positions(N_ROWS, -1);
        let args = vec![strings, delimiters, positions];
        let arg_fields: Vec<_> = args
            .iter()
            .enumerate()
            .map(|(idx, arg)| {
                Field::new(format!("arg_{idx}"), arg.data_type(), true).into()
            })
            .collect();
        let return_field = Field::new("f", DataType::Utf8, true).into();

        group.bench_function(
            BenchmarkId::new("single_char_delim", "pos_negative"),
            |b| {
                b.iter(|| {
                    black_box(
                        split_part_func
                            .invoke_with_args(ScalarFunctionArgs {
                                args: args.clone(),
                                arg_fields: arg_fields.clone(),
                                number_rows: N_ROWS,
                                return_field: Arc::clone(&return_field),
                                config_options: Arc::clone(&config_options),
                            })
                            .expect("split_part should work"),
                    )
                })
            },
        );
    }

    // Scenario 5: Multi-char delimiter, first position
    {
        let (strings, delimiters) = gen_split_part_data(N_ROWS, 10, 8, "~@~", false);
        let positions = gen_positions(N_ROWS, 1);
        let args = vec![strings, delimiters, positions];
        let arg_fields: Vec<_> = args
            .iter()
            .enumerate()
            .map(|(idx, arg)| {
                Field::new(format!("arg_{idx}"), arg.data_type(), true).into()
            })
            .collect();
        let return_field = Field::new("f", DataType::Utf8, true).into();

        group.bench_function(BenchmarkId::new("multi_char_delim", "pos_first"), |b| {
            b.iter(|| {
                black_box(
                    split_part_func
                        .invoke_with_args(ScalarFunctionArgs {
                            args: args.clone(),
                            arg_fields: arg_fields.clone(),
                            number_rows: N_ROWS,
                            return_field: Arc::clone(&return_field),
                            config_options: Arc::clone(&config_options),
                        })
                        .expect("split_part should work"),
                )
            })
        });
    }

    // Scenario 6: Multi-char delimiter, middle position
    {
        let (strings, delimiters) = gen_split_part_data(N_ROWS, 10, 8, "~@~", false);
        let positions = gen_positions(N_ROWS, 5);
        let args = vec![strings, delimiters, positions];
        let arg_fields: Vec<_> = args
            .iter()
            .enumerate()
            .map(|(idx, arg)| {
                Field::new(format!("arg_{idx}"), arg.data_type(), true).into()
            })
            .collect();
        let return_field = Field::new("f", DataType::Utf8, true).into();

        group.bench_function(BenchmarkId::new("multi_char_delim", "pos_middle"), |b| {
            b.iter(|| {
                black_box(
                    split_part_func
                        .invoke_with_args(ScalarFunctionArgs {
                            args: args.clone(),
                            arg_fields: arg_fields.clone(),
                            number_rows: N_ROWS,
                            return_field: Arc::clone(&return_field),
                            config_options: Arc::clone(&config_options),
                        })
                        .expect("split_part should work"),
                )
            })
        });
    }

    // Scenario 7: StringViewArray, single-char delimiter, first position
    {
        let (strings, delimiters) = gen_split_part_data(N_ROWS, 10, 8, ".", true);
        let positions = gen_positions(N_ROWS, 1);
        let args = vec![strings, delimiters, positions];
        let arg_fields: Vec<_> = args
            .iter()
            .enumerate()
            .map(|(idx, arg)| {
                Field::new(format!("arg_{idx}"), arg.data_type(), true).into()
            })
            .collect();
        let return_field = Field::new("f", DataType::Utf8, true).into();

        group.bench_function(
            BenchmarkId::new("string_view_single_char", "pos_first"),
            |b| {
                b.iter(|| {
                    black_box(
                        split_part_func
                            .invoke_with_args(ScalarFunctionArgs {
                                args: args.clone(),
                                arg_fields: arg_fields.clone(),
                                number_rows: N_ROWS,
                                return_field: Arc::clone(&return_field),
                                config_options: Arc::clone(&config_options),
                            })
                            .expect("split_part should work"),
                    )
                })
            },
        );
    }

    // Scenario 8: Many parts (20), position near end - shows benefit of early termination
    {
        let (strings, delimiters) = gen_split_part_data(N_ROWS, 20, 8, ".", false);
        let positions = gen_positions(N_ROWS, 2);
        let args = vec![strings, delimiters, positions];
        let arg_fields: Vec<_> = args
            .iter()
            .enumerate()
            .map(|(idx, arg)| {
                Field::new(format!("arg_{idx}"), arg.data_type(), true).into()
            })
            .collect();
        let return_field = Field::new("f", DataType::Utf8, true).into();

        group.bench_function(BenchmarkId::new("many_parts_20", "pos_second"), |b| {
            b.iter(|| {
                black_box(
                    split_part_func
                        .invoke_with_args(ScalarFunctionArgs {
                            args: args.clone(),
                            arg_fields: arg_fields.clone(),
                            number_rows: N_ROWS,
                            return_field: Arc::clone(&return_field),
                            config_options: Arc::clone(&config_options),
                        })
                        .expect("split_part should work"),
                )
            })
        });
    }

    // Scenario 9: Long strings with many parts - worst case for old implementation
    {
        let (strings, delimiters) = gen_split_part_data(N_ROWS, 50, 16, "/", false);
        let positions = gen_positions(N_ROWS, 1);
        let args = vec![strings, delimiters, positions];
        let arg_fields: Vec<_> = args
            .iter()
            .enumerate()
            .map(|(idx, arg)| {
                Field::new(format!("arg_{idx}"), arg.data_type(), true).into()
            })
            .collect();
        let return_field = Field::new("f", DataType::Utf8, true).into();

        group.bench_function(
            BenchmarkId::new("long_strings_50_parts", "pos_first"),
            |b| {
                b.iter(|| {
                    black_box(
                        split_part_func
                            .invoke_with_args(ScalarFunctionArgs {
                                args: args.clone(),
                                arg_fields: arg_fields.clone(),
                                number_rows: N_ROWS,
                                return_field: Arc::clone(&return_field),
                                config_options: Arc::clone(&config_options),
                            })
                            .expect("split_part should work"),
                    )
                })
            },
        );
    }

    group.finish();
}

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);
