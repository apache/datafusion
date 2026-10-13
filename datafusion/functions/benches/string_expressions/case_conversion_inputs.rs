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

//! Inputs shared by the `upper` and `lower` benchmarks, so both functions are
//! measured on the same arrays.

use std::hint::black_box;
use std::sync::Arc;

use crate::helper::gen_string_array;
use arrow::array::{Array, ArrayRef, LargeStringArray, StringArray, StringViewArray};
use arrow::datatypes::Field;
use arrow::util::bench_util::{
    create_string_array_with_len, create_string_view_array_with_len,
};
use criterion::Criterion;
use datafusion_common::config::ConfigOptions;
use datafusion_expr::{ColumnarValue, ScalarFunctionArgs, ScalarUDF};

/// Rows in each array built by [`build_values`].
const SIZE: usize = 4096;

const WORDS: [&str; 8] = [
    "café",
    "straße",
    "αλφα",
    "こんにちは",
    "münchen",
    "naïve",
    " órdenes",
    "tschüß",
];

/// Builds [`SIZE`] values: every tenth is NULL and the rest come from `value`.
fn build_values(value: impl Fn(usize) -> String) -> Vec<Option<String>> {
    (0..SIZE).map(|i| (i % 10 != 0).then(|| value(i))).collect()
}

/// Three words from [`WORDS`], so the value is non-ASCII.
fn word_value(row: usize) -> String {
    let a = WORDS[row % WORDS.len()];
    let b = WORDS[(row * 7 + 3) % WORDS.len()];
    format!("{a} {b} {a}")
}

/// ASCII text of exactly `len` bytes that differs from row to row.
fn ascii_value(row: usize, len: usize) -> String {
    let base = format!("datafusion row {row} ");
    base.repeat(len / base.len() + 1)[..len].to_string()
}

/// Returns the named input arrays.
///
/// Entirely ASCII, which take the all-ASCII array fast path:
///
/// - `all_ascii_utf8_rows*`: 32-byte values, 20% NULL.
/// - `all_ascii_utf8_sliced`: 128 rows sliced from the middle of a larger
///   array, so the input's buffers are much larger than the rows it uses.
/// - `all_ascii_utf8view_*`: values of `len` bytes, or with `_mixed`, half
///   under 12 bytes (stored inline in the view) and half of 12 to
///   `max(30, len)` bytes.
///
/// Not entirely ASCII, so each row is converted on its own:
///
/// - `some_non_ascii_utf8view_*`: 30-character values, about 10% of them
///   non-ASCII.
/// - `non_ascii_words_*`: every value is three words from [`WORDS`].
/// - `mostly_ascii_*`: ASCII values plus one non-ASCII value in the middle.
/// - `late_non_ascii_*`: every value is ASCII text followed by one non-ASCII
///   character.
pub fn inputs() -> Vec<(String, ArrayRef)> {
    let mut inputs: Vec<(String, ArrayRef)> = Vec::new();

    for size in [1024, 4096, 8192] {
        inputs.push((
            format!("all_ascii_utf8_rows{size}"),
            Arc::new(create_string_array_with_len::<i32>(size, 0.2, 32)),
        ));
    }

    let parent_size = 65536;
    let slice_len = 128;
    let parent = create_string_array_with_len::<i32>(parent_size, 0.2, 32);
    inputs.push((
        "all_ascii_utf8_sliced".to_string(),
        Arc::new(parent.slice((parent_size - slice_len) / 2, slice_len)),
    ));

    for size in [4096, 8192] {
        for str_len in [10, 64, 128] {
            for null_density in [0.0, 0.1] {
                for mixed in [false, true] {
                    let suffix = if mixed { "_mixed" } else { "" };
                    inputs.push((
                        format!(
                            "all_ascii_utf8view_rows{size}_len{str_len}_nulls{null_density}{suffix}"
                        ),
                        Arc::new(create_string_view_array_with_len(
                            size,
                            null_density,
                            str_len,
                            mixed,
                        )),
                    ));
                }
            }
        }
    }

    for size in [4096, 8192] {
        for null_density in [0.0, 0.1] {
            let array = gen_string_array(size, 30, null_density, 0.1, true)
                .remove(0)
                .into_array(size)
                .unwrap();
            inputs.push((
                format!("some_non_ascii_utf8view_rows{size}_nulls{null_density}"),
                array,
            ));
        }
    }

    let words = build_values(word_value);
    inputs.push((
        "non_ascii_words_utf8".to_string(),
        Arc::new(StringArray::from(words.clone())),
    ));
    inputs.push((
        "non_ascii_words_large_utf8".to_string(),
        Arc::new(LargeStringArray::from(words.clone())),
    ));
    inputs.push((
        "non_ascii_words_utf8view".to_string(),
        Arc::new(StringViewArray::from(words)),
    ));

    for str_len in [8, 32, 128] {
        let mostly_ascii = build_values(|i| {
            if i == SIZE / 2 {
                "café".to_string()
            } else {
                ascii_value(i, str_len)
            }
        });
        let late_non_ascii = build_values(|i| ascii_value(i, str_len) + "é");
        for (shape, values) in [
            ("mostly_ascii", mostly_ascii),
            ("late_non_ascii", late_non_ascii),
        ] {
            inputs.push((
                format!("{shape}_utf8_len{str_len}"),
                Arc::new(StringArray::from(values.clone())),
            ));
            inputs.push((
                format!("{shape}_utf8view_len{str_len}"),
                Arc::new(StringViewArray::from(values)),
            ));
        }
    }

    inputs
}

/// Benchmarks `func` on `array` under the name `name`.
pub fn bench_array(c: &mut Criterion, func: &ScalarUDF, name: &str, array: &ArrayRef) {
    let data_type = array.data_type().clone();
    let arg_fields = vec![Field::new("a", data_type.clone(), true).into()];
    let return_field = Field::new("f", data_type, true).into();
    let config_options = Arc::new(ConfigOptions::default());
    c.bench_function(name, |b| {
        b.iter(|| {
            black_box(
                func.invoke_with_args(ScalarFunctionArgs {
                    args: vec![ColumnarValue::Array(Arc::clone(array))],
                    arg_fields: arg_fields.clone(),
                    number_rows: array.len(),
                    return_field: Arc::clone(&return_field),
                    config_options: Arc::clone(&config_options),
                })
                .unwrap(),
            )
        })
    });
}
