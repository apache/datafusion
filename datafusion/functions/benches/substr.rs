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

use arrow::array::{ArrayRef, Int64Array, OffsetSizeTrait};
use arrow::util::bench_util::{
    create_string_array_with_len, create_string_view_array_with_len,
};
use criterion::{black_box, criterion_group, criterion_main, Criterion, SamplingMode};
use datafusion_expr::ColumnarValue;
use datafusion_functions::unicode;
use std::sync::Arc;

fn create_args_without_count<O: OffsetSizeTrait>(
    size: usize,
    str_len: usize,
    start_half_way: bool,
    force_view_types: bool,
) -> Vec<ColumnarValue> {
    let start_array = Arc::new(Int64Array::from(
        (0..size)
            .map(|_| {
                if start_half_way {
                    (str_len / 2) as i64
                } else {
                    1i64
                }
            })
            .collect::<Vec<_>>(),
    ));

    if force_view_types {
        let string_array =
            Arc::new(create_string_view_array_with_len(size, 0.1, str_len, false));
        vec![
            ColumnarValue::Array(string_array),
            ColumnarValue::Array(start_array),
        ]
    } else {
        let string_array =
            Arc::new(create_string_array_with_len::<O>(size, 0.1, str_len));

        vec![
            ColumnarValue::Array(string_array),
            ColumnarValue::Array(Arc::clone(&start_array) as ArrayRef),
        ]
    }
}

fn create_args_with_count<O: OffsetSizeTrait>(
    size: usize,
    str_len: usize,
    count_max: usize,
    force_view_types: bool,
) -> Vec<ColumnarValue> {
    let start_array =
        Arc::new(Int64Array::from((0..size).map(|_| 1).collect::<Vec<_>>()));
    let count = count_max.min(str_len) as i64;
    let count_array = Arc::new(Int64Array::from(
        (0..size).map(|_| count).collect::<Vec<_>>(),
    ));

    if force_view_types {
        let string_array =
            Arc::new(create_string_view_array_with_len(size, 0.1, str_len, false));
        vec![
            ColumnarValue::Array(string_array),
            ColumnarValue::Array(start_array),
            ColumnarValue::Array(count_array),
        ]
    } else {
        let string_array =
            Arc::new(create_string_array_with_len::<O>(size, 0.1, str_len));

        vec![
            ColumnarValue::Array(string_array),
            ColumnarValue::Array(Arc::clone(&start_array) as ArrayRef),
            ColumnarValue::Array(Arc::clone(&count_array) as ArrayRef),
        ]
    }
}

fn criterion_benchmark(c: &mut Criterion) {
    let substr = unicode::substr();
    for size in [1024, 4096] {
        // string_len = 12, substring_len=6 (see `create_args_without_count`)
        let len = 12;
        let mut group = c.benchmark_group("SHORTER THAN 12");
        group.sampling_mode(SamplingMode::Flat);
        group.sample_size(10);

        let args = create_args_without_count::<i32>(size, len, true, true);
        group.bench_function(
            format!("substr_string_view [size={}, strlen={}]", size, len),
            |b| b.iter(|| black_box(substr.invoke(&args))),
        );

        let args = create_args_without_count::<i32>(size, len, false, false);
        group.bench_function(
            format!("substr_string [size={}, strlen={}]", size, len),
            |b| b.iter(|| black_box(substr.invoke(&args))),
        );

        let args = create_args_without_count::<i64>(size, len, true, false);
        group.bench_function(
            format!("substr_large_string [size={}, strlen={}]", size, len),
            |b| b.iter(|| black_box(substr.invoke(&args))),
        );

        group.finish();

        // string_len = 128, start=1, count=64, substring_len=64
        let len = 128;
        let count = 64;
        let mut group = c.benchmark_group("LONGER THAN 12");
        group.sampling_mode(SamplingMode::Flat);
        group.sample_size(10);

        let args = create_args_with_count::<i32>(size, len, count, true);
        group.bench_function(
            format!(
                "substr_string_view [size={}, count={}, strlen={}]",
                size, count, len,
            ),
            |b| b.iter(|| black_box(substr.invoke(&args))),
        );

        let args = create_args_with_count::<i32>(size, len, count, false);
        group.bench_function(
            format!(
                "substr_string [size={}, count={}, strlen={}]",
                size, count, len,
            ),
            |b| b.iter(|| black_box(substr.invoke(&args))),
        );

        let args = create_args_with_count::<i64>(size, len, count, false);
        group.bench_function(
            format!(
                "substr_large_string [size={}, count={}, strlen={}]",
                size, count, len,
            ),
            |b| b.iter(|| black_box(substr.invoke(&args))),
        );

        group.finish();

        // string_len = 128, start=1, count=6, substring_len=6
        let len = 128;
        let count = 6;
        let mut group = c.benchmark_group("SRC_LEN > 12, SUB_LEN < 12");
        group.sampling_mode(SamplingMode::Flat);
        group.sample_size(10);

        let args = create_args_with_count::<i32>(size, len, count, true);
        group.bench_function(
            format!(
                "substr_string_view [size={}, count={}, strlen={}]",
                size, count, len,
            ),
            |b| b.iter(|| black_box(substr.invoke(&args))),
        );

        let args = create_args_with_count::<i32>(size, len, count, false);
        group.bench_function(
            format!(
                "substr_string [size={}, count={}, strlen={}]",
                size, count, len,
            ),
            |b| b.iter(|| black_box(substr.invoke(&args))),
        );

        let args = create_args_with_count::<i64>(size, len, count, false);
        group.bench_function(
            format!(
                "substr_large_string [size={}, count={}, strlen={}]",
                size, count, len,
            ),
            |b| b.iter(|| black_box(substr.invoke(&args))),
        );

        group.finish();
    }
}

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);
