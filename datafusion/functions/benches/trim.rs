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

use arrow::array::{ArrayRef, LargeStringArray, StringArray, StringViewArray};
use arrow::datatypes::{DataType, Field};
use criterion::{
    BenchmarkGroup, Criterion, SamplingMode, criterion_group, criterion_main,
    measurement::Measurement,
};
use datafusion_common::ScalarValue;
use datafusion_common::config::ConfigOptions;
use datafusion_expr::{ColumnarValue, ScalarFunctionArgs, ScalarUDF};
use datafusion_functions::string;
use rand::{Rng, SeedableRng, distr::Alphanumeric, rngs::StdRng};
use std::hint::black_box;
use std::{fmt, sync::Arc};

#[derive(Clone, Copy)]
pub enum StringArrayType {
    Utf8View,
    Utf8,
    LargeUtf8,
}

impl fmt::Display for StringArrayType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            StringArrayType::Utf8View => f.write_str("string_view"),
            StringArrayType::Utf8 => f.write_str("string"),
            StringArrayType::LargeUtf8 => f.write_str("large_string"),
        }
    }
}

#[derive(Clone, Copy)]
pub enum TrimType {
    Ltrim,
    Rtrim,
    Btrim,
}

impl fmt::Display for TrimType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            TrimType::Ltrim => f.write_str("ltrim"),
            TrimType::Rtrim => f.write_str("rtrim"),
            TrimType::Btrim => f.write_str("btrim"),
        }
    }
}

/// Returns an array of strings with trim characters positioned according to trim type,
/// and `characters` as a ScalarValue.
///
/// For ltrim: trim characters are at the start (prefix)
/// For rtrim: trim characters are at the end (suffix)
/// For btrim: trim characters are at both start and end
fn create_string_array_and_characters(
    size: usize,
    characters: &str,
    trimmed: &str,
    remaining_len: usize,
    string_array_type: StringArrayType,
    trim_type: TrimType,
) -> (ArrayRef, ScalarValue) {
    let rng = &mut StdRng::seed_from_u64(42);

    // Create `size` rows:
    //   - 10% rows will be `None`
    //   - Other 90% will be strings with `remaining_len` content length
    let string_iter = (0..size).map(|_| {
        if rng.random::<f32>() < 0.1 {
            None
        } else {
            let content: String = rng
                .sample_iter(&Alphanumeric)
                .take(remaining_len)
                .map(char::from)
                .collect();

            let value = match trim_type {
                TrimType::Ltrim => format!("{trimmed}{content}"),
                TrimType::Rtrim => format!("{content}{trimmed}"),
                TrimType::Btrim => format!("{trimmed}{content}{trimmed}"),
            };
            Some(value)
        }
    });

    // Build the target `string array` and `characters` according to `string_array_type`
    match string_array_type {
        StringArrayType::Utf8View => (
            Arc::new(string_iter.collect::<StringViewArray>()),
            ScalarValue::Utf8View(Some(characters.to_string())),
        ),
        StringArrayType::Utf8 => (
            Arc::new(string_iter.collect::<StringArray>()),
            ScalarValue::Utf8(Some(characters.to_string())),
        ),
        StringArrayType::LargeUtf8 => (
            Arc::new(string_iter.collect::<LargeStringArray>()),
            ScalarValue::LargeUtf8(Some(characters.to_string())),
        ),
    }
}

/// Create args for the trim benchmark
fn create_args(
    size: usize,
    characters: &str,
    trimmed: &str,
    remaining_len: usize,
    string_array_type: StringArrayType,
    trim_type: TrimType,
) -> Vec<ColumnarValue> {
    let (string_array, pattern) = create_string_array_and_characters(
        size,
        characters,
        trimmed,
        remaining_len,
        string_array_type,
        trim_type,
    );
    vec![
        ColumnarValue::Array(string_array),
        ColumnarValue::Scalar(pattern),
    ]
}

/// Create args for trim benchmark where space characters are being trimmed
fn create_space_trim_args(
    size: usize,
    pad_len: usize,
    remaining_len: usize,
    string_array_type: StringArrayType,
    trim_type: TrimType,
) -> Vec<ColumnarValue> {
    let rng = &mut StdRng::seed_from_u64(42);
    let spaces = " ".repeat(pad_len);

    let string_iter = (0..size).map(|_| {
        if rng.random::<f32>() < 0.1 {
            None
        } else {
            let content: String = rng
                .sample_iter(&Alphanumeric)
                .take(remaining_len)
                .map(char::from)
                .collect();

            let value = match trim_type {
                TrimType::Ltrim => format!("{spaces}{content}"),
                TrimType::Rtrim => format!("{content}{spaces}"),
                TrimType::Btrim => format!("{spaces}{content}{spaces}"),
            };
            Some(value)
        }
    });

    let string_array: ArrayRef = match string_array_type {
        StringArrayType::Utf8View => Arc::new(string_iter.collect::<StringViewArray>()),
        StringArrayType::Utf8 => Arc::new(string_iter.collect::<StringArray>()),
        StringArrayType::LargeUtf8 => Arc::new(string_iter.collect::<LargeStringArray>()),
    };

    vec![ColumnarValue::Array(string_array)]
}

#[expect(clippy::too_many_arguments)]
fn run_with_string_type<M: Measurement>(
    group: &mut BenchmarkGroup<'_, M>,
    trim_func: &ScalarUDF,
    trim_type: TrimType,
    size: usize,
    total_len: usize,
    characters: &str,
    trimmed: &str,
    remaining_len: usize,
    string_type: StringArrayType,
) {
    let args = create_args(
        size,
        characters,
        trimmed,
        remaining_len,
        string_type,
        trim_type,
    );
    let arg_fields = args
        .iter()
        .enumerate()
        .map(|(idx, arg)| Field::new(format!("arg_{idx}"), arg.data_type(), true).into())
        .collect::<Vec<_>>();
    let config_options = Arc::new(ConfigOptions::default());

    group.bench_function(
        format!(
            "{trim_type} {string_type} [size={size}, len={total_len}, remaining={remaining_len}]",
        ),
        |b| {
            b.iter(|| {
                let args_cloned = args.clone();
                black_box(trim_func.invoke_with_args(ScalarFunctionArgs {
                    args: args_cloned,
                    arg_fields: arg_fields.clone(),
                    number_rows: size,
                    return_field: Field::new("f", DataType::Utf8, true).into(),
                    config_options: Arc::clone(&config_options),
                }))
            })
        },
    );
}

#[expect(clippy::too_many_arguments)]
fn run_trim_benchmark(
    c: &mut Criterion,
    group_name: &str,
    trim_func: &ScalarUDF,
    trim_type: TrimType,
    string_types: &[StringArrayType],
    size: usize,
    total_len: usize,
    characters: &str,
    trimmed: &str,
    remaining_len: usize,
) {
    let mut group = c.benchmark_group(group_name);
    group.sampling_mode(SamplingMode::Flat);
    group.sample_size(10);

    for string_type in string_types {
        run_with_string_type(
            &mut group,
            trim_func,
            trim_type,
            size,
            total_len,
            characters,
            trimmed,
            remaining_len,
            *string_type,
        );
    }

    group.finish();
}

#[expect(clippy::too_many_arguments)]
fn run_space_trim_benchmark(
    c: &mut Criterion,
    group_name: &str,
    trim_func: &ScalarUDF,
    trim_type: TrimType,
    string_types: &[StringArrayType],
    size: usize,
    pad_len: usize,
    remaining_len: usize,
) {
    let mut group = c.benchmark_group(group_name);
    group.sampling_mode(SamplingMode::Flat);
    group.sample_size(10);

    let total_len = match trim_type {
        TrimType::Btrim => 2 * pad_len + remaining_len,
        _ => pad_len + remaining_len,
    };

    for string_type in string_types {
        let args =
            create_space_trim_args(size, pad_len, remaining_len, *string_type, trim_type);
        let arg_fields = args
            .iter()
            .enumerate()
            .map(|(idx, arg)| {
                Field::new(format!("arg_{idx}"), arg.data_type(), true).into()
            })
            .collect::<Vec<_>>();
        let config_options = Arc::new(ConfigOptions::default());

        group.bench_function(
            format!(
                "{trim_type} {string_type} [size={size}, len={total_len}, pad={pad_len}]",
            ),
            |b| {
                b.iter(|| {
                    let args_cloned = args.clone();
                    black_box(trim_func.invoke_with_args(ScalarFunctionArgs {
                        args: args_cloned,
                        arg_fields: arg_fields.clone(),
                        number_rows: size,
                        return_field: Field::new("f", DataType::Utf8, true).into(),
                        config_options: Arc::clone(&config_options),
                    }))
                })
            },
        );
    }

    group.finish();
}

fn criterion_benchmark(c: &mut Criterion) {
    let ltrim = string::ltrim();
    let rtrim = string::rtrim();
    let btrim = string::btrim();

    let characters = ",!()";

    let string_types = [
        StringArrayType::Utf8View,
        StringArrayType::Utf8,
        StringArrayType::LargeUtf8,
    ];

    let trim_funcs = [
        (&ltrim, TrimType::Ltrim),
        (&rtrim, TrimType::Rtrim),
        (&btrim, TrimType::Btrim),
    ];

    for size in [4096] {
        for (trim_func, trim_type) in &trim_funcs {
            // Scenario 1: Short strings (len <= 12, inline in StringView)
            // trimmed_len=4, remaining_len=8
            let total_len = 12;
            let trimmed = characters;
            let remaining_len = total_len - trimmed.len();
            run_trim_benchmark(
                c,
                "short strings (len <= 12)",
                trim_func,
                *trim_type,
                &string_types,
                size,
                total_len,
                characters,
                trimmed,
                remaining_len,
            );

            // Scenario 2: Long strings, short trim (len > 12, output > 12)
            // trimmed_len=4, remaining_len=60
            let total_len = 64;
            let trimmed = characters;
            let remaining_len = total_len - trimmed.len();
            run_trim_benchmark(
                c,
                "long strings, short trim",
                trim_func,
                *trim_type,
                &string_types,
                size,
                total_len,
                characters,
                trimmed,
                remaining_len,
            );

            // Scenario 3: Long strings, long trim (len > 12, output <= 12)
            // trimmed_len=56, remaining_len=8
            let total_len = 64;
            let trimmed = characters.repeat(14);
            let remaining_len = total_len - trimmed.len();
            run_trim_benchmark(
                c,
                "long strings, long trim",
                trim_func,
                *trim_type,
                &string_types,
                size,
                total_len,
                characters,
                &trimmed,
                remaining_len,
            );

            // Scenario 4: Trim spaces, short strings (len <= 12)
            // pad_len=4, remaining_len=8
            run_space_trim_benchmark(
                c,
                "trim spaces, short strings (len <= 12)",
                trim_func,
                *trim_type,
                &string_types,
                size,
                4,
                8,
            );

            // Scenario 5: Trim spaces, long strings (len > 12)
            // pad_len=4, remaining_len=60
            run_space_trim_benchmark(
                c,
                "trim spaces, long strings",
                trim_func,
                *trim_type,
                &string_types,
                size,
                4,
                60,
            );

            // Scenario 6: Trim spaces, long strings, heavy padding
            // pad_len=56, remaining_len=8
            run_space_trim_benchmark(
                c,
                "trim spaces, heavy padding",
                trim_func,
                *trim_type,
                &string_types,
                size,
                56,
                8,
            );
        }
    }
}

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);
