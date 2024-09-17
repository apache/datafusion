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

use arrow::{
    array::{ArrayRef, LargeStringArray, StringArray, StringViewArray},
};
use criterion::{black_box, criterion_group, criterion_main, Criterion, SamplingMode};
use datafusion_common::ScalarValue;
use datafusion_expr::ColumnarValue;
use datafusion_functions::string;
use rand::{distributions::Alphanumeric, rngs::StdRng, Rng, SeedableRng};
use std::sync::Arc;

pub fn seedable_rng() -> StdRng {
    StdRng::seed_from_u64(42)
}

pub enum StringArrayType {
    Utf8View,
    Utf8,
    LargeUtf8,
}

pub fn create_prefixed_string_array_and_pattern(
    size: usize,
    prefix: &str,
    generated_len: usize,
    string_array_type: StringArrayType,
) -> (ArrayRef, ScalarValue) {
    let rng = &mut seedable_rng();

    let lens = vec![generated_len; size];
    let string_iter = lens.into_iter().map(|len| {
        if rng.gen::<f32>() < 0.1 {
            None
        } else {
            let mut value = prefix.as_bytes().to_vec();
            let generated = rng.sample_iter(&Alphanumeric).take(len);
            value.extend(generated);
            Some(String::from_utf8(value).unwrap())
        }
    });

    match string_array_type {
        StringArrayType::Utf8View => (
            Arc::new(string_iter.collect::<StringViewArray>()),
            ScalarValue::Utf8View(Some(prefix.to_string())),
        ),
        StringArrayType::Utf8 => (
            Arc::new(string_iter.collect::<StringArray>()),
            ScalarValue::Utf8(Some(prefix.to_string())),
        ),
        StringArrayType::LargeUtf8 => (
            Arc::new(string_iter.collect::<LargeStringArray>()),
            ScalarValue::LargeUtf8(Some(prefix.to_string())),
        ),
    }
}

fn create_args(
    size: usize,
    characters: &str,
    generated_len: usize,
    string_array_type: StringArrayType,
) -> Vec<ColumnarValue> {
    let (string_array, pattern) = create_prefixed_string_array_and_pattern(
        size,
        characters,
        generated_len,
        string_array_type,
    );
    vec![
        ColumnarValue::Array(string_array),
        ColumnarValue::Scalar(pattern),
    ]
}

fn criterion_benchmark(c: &mut Criterion) {
    let ltrim = string::ltrim();
    let prefix = ",!()";
    for size in [1024, 4096, 8192] {
        // len=12, prefix=4, len_after_ltrim=8
        let len = 12;
        let len_exclude_prefix = len - prefix.len();
        let mut group = c.benchmark_group("INPUT LEN <= 12");
        group.sampling_mode(SamplingMode::Flat);
        group.sample_size(10);

        let args = create_args(size, &prefix, len_exclude_prefix, StringArrayType::Utf8View);
        group.bench_function(
            format!("ltrim_string_view [size={}, strlen={}]", size, len),
            |b| b.iter(|| black_box(ltrim.invoke(&args))),
        );

        let args = create_args(size, &prefix, len_exclude_prefix, StringArrayType::Utf8);
        group.bench_function(
            format!("ltrim_string [size={}, strlen={}]", size, len),
            |b| b.iter(|| black_box(ltrim.invoke(&args))),
        );

        let args = create_args(size, &prefix, len_exclude_prefix, StringArrayType::LargeUtf8);
        group.bench_function(
            format!("ltrim_large_string [size={}, strlen={}]", size, len),
            |b| b.iter(|| black_box(ltrim.invoke(&args))),
        );

        group.finish();

        // len=64, prefix=4, len_after_ltrim=60
        let len = 64;
        let len_exclude_prefix = len - prefix.len();
        let mut group = c.benchmark_group("INPUT LEN > 12, OUTPUT LEN > 12");
        group.sampling_mode(SamplingMode::Flat);
        group.sample_size(10);

        let args = create_args(size, &prefix, len_exclude_prefix, StringArrayType::Utf8View);
        group.bench_function(
            format!("ltrim_string_view [size={}, strlen={}]", size, len),
            |b| b.iter(|| black_box(ltrim.invoke(&args))),
        );

        let args = create_args(size, &prefix, len_exclude_prefix, StringArrayType::Utf8);
        group.bench_function(
            format!("ltrim_string [size={}, strlen={}]", size, len),
            |b| b.iter(|| black_box(ltrim.invoke(&args))),
        );

        let args = create_args(size, &prefix, len_exclude_prefix, StringArrayType::LargeUtf8);
        group.bench_function(
            format!("ltrim_large_string [size={}, strlen={}]", size, len),
            |b| b.iter(|| black_box(ltrim.invoke(&args))),
        );

        group.finish();

        // len=15, prefix=4, len_after_ltrim=11
        let len = 15;
        let len_exclude_prefix = len - prefix.len();
        let mut group = c.benchmark_group("INPUT LEN > 12, OUTPUT LEN <= 12");
        group.sampling_mode(SamplingMode::Flat);
        group.sample_size(10);

        let args = create_args(size, &prefix, len_exclude_prefix, StringArrayType::Utf8View);
        group.bench_function(
            format!("ltrim_string_view [size={}, strlen={}]", size, len),
            |b| b.iter(|| black_box(ltrim.invoke(&args))),
        );

        let args = create_args(size, &prefix, len_exclude_prefix, StringArrayType::Utf8);
        group.bench_function(
            format!("ltrim_string [size={}, strlen={}]", size, len),
            |b| b.iter(|| black_box(ltrim.invoke(&args))),
        );

        let args = create_args(size, &prefix, len_exclude_prefix, StringArrayType::LargeUtf8);
        group.bench_function(
            format!("ltrim_large_string [size={}, strlen={}]", size, len),
            |b| b.iter(|| black_box(ltrim.invoke(&args))),
        );

        group.finish();
    }
}

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);
