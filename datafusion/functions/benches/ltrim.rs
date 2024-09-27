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

use arrow::array::{ArrayRef, LargeStringArray, StringArray, StringViewArray};
use criterion::{
    black_box, criterion_group, criterion_main, measurement::Measurement, BenchmarkGroup,
    Criterion, SamplingMode,
};
use datafusion_common::ScalarValue;
use datafusion_expr::{ColumnarValue, ScalarUDF};
use datafusion_functions::string;
use rand::{distributions::Alphanumeric, rngs::StdRng, Rng, SeedableRng};
use std::{fmt, sync::Arc};

pub fn seedable_rng() -> StdRng {
    StdRng::seed_from_u64(42)
}

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

/// returns an array of strings, and `characters` as a ScalarValue
pub fn create_string_array_and_characters(
    size: usize,
    characters: &str,
    trimmed: &str,
    remaining_len: usize,
    string_array_type: StringArrayType,
) -> (ArrayRef, ScalarValue) {
    let rng = &mut seedable_rng();

    // Create `size` rows:
    //   - 10% rows will be `None`
    //   - Other 90% will be strings with same `remaining_len` lengths
    // We will build the string array on it later.
    let string_iter = (0..size).map(|_| {
        if rng.gen::<f32>() < 0.1 {
            None
        } else {
            let mut value = trimmed.as_bytes().to_vec();
            let generated = rng.sample_iter(&Alphanumeric).take(remaining_len);
            value.extend(generated);
            Some(String::from_utf8(value).unwrap())
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

/// Create args for the ltrim benchmark
/// Inputs:
///   - size: rows num of the test array
///   - characters: the characters we need to trim
///   - trimmed: the part in the testing string that will be trimmed
///   - remaining_len: the len of the remaining part of testing string after trimming
///   - string_array_type: the method used to store the testing strings
///
/// Outputs:
///   - testing string array
///   - trimmed characters
///
fn create_args(
    size: usize,
    characters: &str,
    trimmed: &str,
    remaining_len: usize,
    string_array_type: StringArrayType,
) -> Vec<ColumnarValue> {
    let (string_array, pattern) = create_string_array_and_characters(
        size,
        characters,
        trimmed,
        remaining_len,
        string_array_type,
    );
    vec![
        ColumnarValue::Array(string_array),
        ColumnarValue::Scalar(pattern),
    ]
}

#[allow(clippy::too_many_arguments)]
fn run_with_string_type<M: Measurement>(
    group: &mut BenchmarkGroup<'_, M>,
    ltrim: &ScalarUDF,
    size: usize,
    len: usize,
    characters: &str,
    trimmed: &str,
    remaining_len: usize,
    string_type: StringArrayType,
) {
    let args = create_args(size, characters, trimmed, remaining_len, string_type);
    group.bench_function(
        format!(
            "{string_type} [size={size}, len_before={len}, len_after={remaining_len}]",
        ),
        |b| b.iter(|| black_box(ltrim.invoke(&args))),
    );
}

#[allow(clippy::too_many_arguments)]
fn run_one_group(
    c: &mut Criterion,
    group_name: &str,
    ltrim: &ScalarUDF,
    string_types: &[StringArrayType],
    size: usize,
    len: usize,
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
            ltrim,
            size,
            len,
            characters,
            trimmed,
            remaining_len,
            *string_type,
        );
    }

    group.finish();
}

fn criterion_benchmark(c: &mut Criterion) {
    let ltrim = string::ltrim();
    let characters = ",!()";

    let string_types = [
        StringArrayType::Utf8View,
        StringArrayType::Utf8,
        StringArrayType::LargeUtf8,
    ];
    for size in [1024, 4096, 8192] {
        // len=12, trimmed_len=4, len_after_ltrim=8
        let len = 12;
        let trimmed = characters;
        let remaining_len = len - trimmed.len();
        run_one_group(
            c,
            "INPUT LEN <= 12",
            &ltrim,
            &string_types,
            size,
            len,
            characters,
            trimmed,
            remaining_len,
        );

        // len=64, trimmed_len=4, len_after_ltrim=60
        let len = 64;
        let trimmed = characters;
        let remaining_len = len - trimmed.len();
        run_one_group(
            c,
            "INPUT LEN > 12, OUTPUT LEN > 12",
            &ltrim,
            &string_types,
            size,
            len,
            characters,
            trimmed,
            remaining_len,
        );

        // len=64, trimmed_len=56, len_after_ltrim=8
        let len = 64;
        let trimmed = characters.repeat(15);
        let remaining_len = len - trimmed.len();
        run_one_group(
            c,
            "INPUT LEN > 12, OUTPUT LEN <= 12",
            &ltrim,
            &string_types,
            size,
            len,
            characters,
            &trimmed,
            remaining_len,
        );
    }
}

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);
