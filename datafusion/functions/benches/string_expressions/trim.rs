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
use std::hint::black_box;
use std::ops::RangeInclusive;
use std::sync::Arc;

use arrow::array::{StringArray, StringViewArray};
use arrow::datatypes::Field;
use criterion::{Criterion, criterion_group};
use datafusion_common::ScalarValue;
use datafusion_common::config::ConfigOptions;
use datafusion_expr::{ColumnarValue, ReturnFieldArgs, ScalarFunctionArgs};
use datafusion_functions::string::{btrim, ltrim, rtrim};
use rand::distr::Alphanumeric;
use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};

const BATCH_SIZE: usize = 8192;

/// The characters to trim.
enum Pattern {
    /// Spaces, as in `trim(s)`.
    Spaces,
    /// Any of the given characters, as in `trim(s, ',!()')`.
    Chars(&'static str),
}

/// Returns strings of alphanumeric content, with padding to trim at the start
/// and/or end. Each row's content and padding lengths are drawn from the given
/// ranges.
fn create_strings(
    content_len: &RangeInclusive<usize>,
    pad_len: &RangeInclusive<usize>,
    pattern: &Pattern,
    pad_start: bool,
    pad_end: bool,
) -> Vec<Option<String>> {
    let mut rng = StdRng::seed_from_u64(42);
    let pad_chars = match pattern {
        Pattern::Spaces => " ",
        Pattern::Chars(chars) => chars,
    }
    .as_bytes();
    let padding = |rng: &mut StdRng| -> String {
        let len = rng.random_range(pad_len.clone());
        (0..len)
            .map(|_| pad_chars[rng.random_range(0..pad_chars.len())] as char)
            .collect()
    };

    (0..BATCH_SIZE)
        .map(|_| {
            if rng.random::<f32>() < 0.1 {
                return None;
            }
            let len = rng.random_range(content_len.clone());
            let content: String = (&mut rng)
                .sample_iter(&Alphanumeric)
                .take(len)
                .map(char::from)
                .collect();
            let start = if pad_start {
                padding(&mut rng)
            } else {
                String::new()
            };
            let end = if pad_end {
                padding(&mut rng)
            } else {
                String::new()
            };
            Some(format!("{start}{content}{end}"))
        })
        .collect()
}

fn criterion_benchmark(c: &mut Criterion) {
    // Content and padding lengths vary within each case, as in real data: with
    // fixed lengths, per-row work that depends on them is unrealistically
    // predictable.
    let cases = [
        // A little padding. Results of up to 12 bytes are stored inline in
        // StringView arrays, and longer ones out of line.
        ("spaces", 1..=64, 0..=8, Pattern::Spaces),
        // Short values padded to a long width, as in `CHAR(n)` data.
        ("heavy_padding", 1..=12, 32..=64, Pattern::Spaces),
        // Values that are already trimmed.
        ("nothing_to_trim", 1..=64, 0..=0, Pattern::Spaces),
        // Short values with long padding made of several different characters.
        ("char_set", 1..=12, 32..=64, Pattern::Chars(",!()")),
    ];
    let config_options = Arc::new(ConfigOptions::default());

    for (function, pad_start, pad_end) in [
        (ltrim(), true, false),
        (rtrim(), false, true),
        (btrim(), true, true),
    ] {
        let mut group = c.benchmark_group(function.name().to_string());

        for is_string_view in [false, true] {
            let array_type = if is_string_view {
                "string_view"
            } else {
                "string"
            };

            for (case_name, content_len, pad_len, pattern) in &cases {
                let strings =
                    create_strings(content_len, pad_len, pattern, pad_start, pad_end);
                let mut args = vec![if is_string_view {
                    ColumnarValue::Array(Arc::new(
                        strings.into_iter().collect::<StringViewArray>(),
                    ))
                } else {
                    ColumnarValue::Array(Arc::new(
                        strings.into_iter().collect::<StringArray>(),
                    ))
                }];
                if let Pattern::Chars(chars) = pattern {
                    args.push(ColumnarValue::Scalar(ScalarValue::from(*chars)));
                }

                let arg_fields: Vec<_> = args
                    .iter()
                    .enumerate()
                    .map(|(idx, arg)| {
                        Field::new(format!("arg_{idx}"), arg.data_type(), true).into()
                    })
                    .collect();
                let scalar_arguments = vec![None; arg_fields.len()];
                let return_field = function
                    .return_field_from_args(ReturnFieldArgs {
                        arg_fields: &arg_fields,
                        scalar_arguments: &scalar_arguments,
                    })
                    .expect("should resolve return field");

                group.bench_function(format!("{array_type} {case_name}"), |b| {
                    b.iter(|| {
                        black_box(
                            function
                                .invoke_with_args(ScalarFunctionArgs {
                                    args: args.clone(),
                                    arg_fields: arg_fields.clone(),
                                    number_rows: BATCH_SIZE,
                                    return_field: Arc::clone(&return_field),
                                    config_options: Arc::clone(&config_options),
                                })
                                .expect("should work"),
                        )
                    })
                });
            }
        }

        group.finish();
    }
}

criterion_group!(benches, criterion_benchmark);
