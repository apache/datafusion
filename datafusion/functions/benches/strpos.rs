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

use arrow::array::{StringArray, StringViewArray};
use arrow::datatypes::{DataType, Field};
use criterion::{Criterion, criterion_group, criterion_main};
use datafusion_common::ScalarValue;
use datafusion_common::config::ConfigOptions;
use datafusion_expr::{ColumnarValue, ScalarFunctionArgs};
use rand::distr::Alphanumeric;
use rand::prelude::StdRng;
use rand::{Rng, SeedableRng};
use std::hint::black_box;
use std::sync::Arc;

#[rustfmt::skip]
const UTF8_CORPUS: &[char] = &[
    // Cyrillic (2 bytes each)
    'А', 'Б', 'В', 'Г', 'Д', 'Е', 'Ж', 'З', 'И', 'К', 'Л', 'М', 'Н', 'О', 'П', 'Р', 'С',
    'Т', 'У', 'Ф', 'Х', 'Ц', 'Ч', 'Ш', 'Щ', 'Э', 'Ю', 'Я',
    // CJK (3 bytes each)
    '数', '据', '融', '合', '查', '询', '引', '擎', '优', '化', '执', '行', '计', '划',
    '表', '达',
    // Emoji (4 bytes each)
    '📊', '🔥', '🚀', '⚡', '🎯', '💡', '🔧', '📈',
];
const N_ROWS: usize = 8192;

/// Returns a random string of `len` characters. If `ascii` is true, the string
/// is ASCII-only; otherwise it is drawn from `UTF8_CORPUS`.
fn random_string(rng: &mut StdRng, len: usize, ascii: bool) -> String {
    if ascii {
        let value: Vec<u8> = rng.sample_iter(&Alphanumeric).take(len).collect();
        String::from_utf8(value).unwrap()
    } else {
        (0..len)
            .map(|_| UTF8_CORPUS[rng.random_range(0..UTF8_CORPUS.len())])
            .collect()
    }
}

/// Wraps `strings` into either a `StringArray` or `StringViewArray`.
fn to_columnar_value(
    strings: Vec<Option<String>>,
    is_string_view: bool,
) -> ColumnarValue {
    if is_string_view {
        let arr: StringViewArray = strings.into_iter().collect();
        ColumnarValue::Array(Arc::new(arr))
    } else {
        let arr: StringArray = strings.into_iter().collect();
        ColumnarValue::Array(Arc::new(arr))
    }
}

/// Returns haystack and needle, where both are arrays. Each needle is a
/// contiguous substring of its corresponding haystack. Around `null_density`
/// fraction of rows are null and `utf8_density` fraction contain non-ASCII
/// characters.
fn make_array_needle_args(
    rng: &mut StdRng,
    str_len_chars: usize,
    null_density: f32,
    utf8_density: f32,
    is_string_view: bool,
) -> Vec<ColumnarValue> {
    let mut haystacks: Vec<Option<String>> = Vec::with_capacity(N_ROWS);
    let mut needles: Vec<Option<String>> = Vec::with_capacity(N_ROWS);
    for _ in 0..N_ROWS {
        let r = rng.random::<f32>();
        if r < null_density {
            haystacks.push(None);
            needles.push(None);
        } else {
            let ascii = r >= null_density + utf8_density;
            let s = random_string(rng, str_len_chars, ascii);
            needles.push(Some(random_substring(rng, &s)));
            haystacks.push(Some(s));
        }
    }

    vec![
        to_columnar_value(haystacks, is_string_view),
        to_columnar_value(needles, is_string_view),
    ]
}

/// Returns haystack array with a fixed scalar needle inserted into each row.
/// `utf8_density` fraction of rows contain non-ASCII characters.
/// The needle must be ASCII.
fn make_scalar_needle_args(
    rng: &mut StdRng,
    str_len_chars: usize,
    needle: &str,
    utf8_density: f32,
    is_string_view: bool,
) -> Vec<ColumnarValue> {
    let needle_len = needle.len();

    let mut haystacks: Vec<Option<String>> = Vec::with_capacity(N_ROWS);
    for _ in 0..N_ROWS {
        let ascii = rng.random::<f32>() >= utf8_density;
        if ascii {
            let mut value: Vec<u8> = (&mut *rng)
                .sample_iter(&Alphanumeric)
                .take(str_len_chars)
                .collect();
            if str_len_chars >= needle_len {
                let pos = rng.random_range(0..=str_len_chars - needle_len);
                value[pos..pos + needle_len].copy_from_slice(needle.as_bytes());
            }
            haystacks.push(Some(String::from_utf8(value).unwrap()));
        } else {
            let mut s = random_string(rng, str_len_chars, false);
            let char_positions: Vec<usize> = s.char_indices().map(|(i, _)| i).collect();
            let insert_pos = if char_positions.len() > 1 {
                char_positions[rng.random_range(0..char_positions.len())]
            } else {
                0
            };
            s.insert_str(insert_pos, needle);
            haystacks.push(Some(s));
        }
    }

    let needle_cv = ColumnarValue::Scalar(ScalarValue::Utf8(Some(needle.to_string())));
    vec![to_columnar_value(haystacks, is_string_view), needle_cv]
}

/// Extracts a random contiguous substring from `s`.
fn random_substring(rng: &mut StdRng, s: &str) -> String {
    let count = s.chars().count();
    let start = rng.random_range(0..count - 1);
    let end = rng.random_range(start + 1..count);
    s.chars().skip(start).take(end - start).collect()
}

fn bench_strpos(
    c: &mut Criterion,
    name: &str,
    args: Vec<ColumnarValue>,
    strpos: &datafusion_expr::ScalarUDF,
) {
    let arg_fields = vec![Field::new("a", args[0].data_type(), true).into()];
    let return_field: Arc<Field> = Field::new("f", DataType::Int32, true).into();
    let config_options = Arc::new(ConfigOptions::default());

    c.bench_function(name, |b| {
        b.iter(|| {
            black_box(strpos.invoke_with_args(ScalarFunctionArgs {
                args: args.clone(),
                arg_fields: arg_fields.clone(),
                number_rows: N_ROWS,
                return_field: Arc::clone(&return_field),
                config_options: Arc::clone(&config_options),
            }))
        })
    });
}

fn criterion_benchmark(c: &mut Criterion) {
    let strpos = datafusion_functions::unicode::strpos();
    let mut rng = StdRng::seed_from_u64(42);

    for str_len in [8, 32, 128, 4096] {
        // Array needle benchmarks
        for (label, utf8_density, is_view) in [
            ("StringArray_ascii", 0.0, false),
            ("StringArray_utf8", 0.5, false),
            ("StringViewArray_ascii", 0.0, true),
            ("StringViewArray_utf8", 0.5, true),
        ] {
            let args =
                make_array_needle_args(&mut rng, str_len, 0.1, utf8_density, is_view);
            bench_strpos(
                c,
                &format!("strpos_{label}_str_len_{str_len}"),
                args,
                strpos.as_ref(),
            );
        }

        // Scalar needle benchmarks
        let needle = "xyz";
        for (label, utf8_density, is_view) in [
            ("StringArray_scalar_needle_ascii", 0.0, false),
            ("StringArray_scalar_needle_utf8", 0.5, false),
            ("StringViewArray_scalar_needle_ascii", 0.0, true),
            ("StringViewArray_scalar_needle_utf8", 0.5, true),
        ] {
            let args =
                make_scalar_needle_args(&mut rng, str_len, needle, utf8_density, is_view);
            bench_strpos(
                c,
                &format!("strpos_{label}_str_len_{str_len}"),
                args,
                strpos.as_ref(),
            );
        }
    }
}

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);
