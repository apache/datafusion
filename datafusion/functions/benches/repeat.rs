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
use datafusion_functions::string;
use std::sync::Arc;
use std::time::Duration;

fn create_args<O: OffsetSizeTrait>(
    size: usize,
    str_len: usize,
    repeat_times: i64,
    force_view_types: bool,
) -> Vec<ColumnarValue> {
    let number_array = Arc::new(Int64Array::from(
        (0..size).map(|_| repeat_times).collect::<Vec<_>>(),
    ));

    if force_view_types {
        let string_array =
            Arc::new(create_string_view_array_with_len(size, 0.1, str_len, false));
        vec![
            ColumnarValue::Array(string_array),
            ColumnarValue::Array(number_array),
        ]
    } else {
        let string_array =
            Arc::new(create_string_array_with_len::<O>(size, 0.1, str_len));

        vec![
            ColumnarValue::Array(string_array),
            ColumnarValue::Array(Arc::clone(&number_array) as ArrayRef),
        ]
    }
}

fn criterion_benchmark(c: &mut Criterion) {
    let repeat = string::repeat();
    for size in [1024, 4096] {
        // REPEAT 3 TIMES
        let repeat_times = 3;
        let mut group = c.benchmark_group(format!("repeat {} times", repeat_times));
        group.sampling_mode(SamplingMode::Flat);
        group.sample_size(10);
        group.measurement_time(Duration::from_secs(10));

        let args = create_args::<i32>(size, 32, repeat_times, true);
        group.bench_function(
            format!(
                "repeat_string_view [size={}, repeat_times={}]",
                size, repeat_times
            ),
            |b| b.iter(|| black_box(repeat.invoke(&args))),
        );

        let args = create_args::<i32>(size, 32, repeat_times, false);
        group.bench_function(
            format!(
                "repeat_string [size={}, repeat_times={}]",
                size, repeat_times
            ),
            |b| b.iter(|| black_box(repeat.invoke(&args))),
        );

        let args = create_args::<i64>(size, 32, repeat_times, false);
        group.bench_function(
            format!(
                "repeat_large_string [size={}, repeat_times={}]",
                size, repeat_times
            ),
            |b| b.iter(|| black_box(repeat.invoke(&args))),
        );

        group.finish();

        // REPEAT 30 TIMES
        let repeat_times = 30;
        let mut group = c.benchmark_group(format!("repeat {} times", repeat_times));
        group.sampling_mode(SamplingMode::Flat);
        group.sample_size(10);
        group.measurement_time(Duration::from_secs(10));

        let args = create_args::<i32>(size, 32, repeat_times, true);
        group.bench_function(
            format!(
                "repeat_string_view [size={}, repeat_times={}]",
                size, repeat_times
            ),
            |b| b.iter(|| black_box(repeat.invoke(&args))),
        );

        let args = create_args::<i32>(size, 32, repeat_times, false);
        group.bench_function(
            format!(
                "repeat_string [size={}, repeat_times={}]",
                size, repeat_times
            ),
            |b| b.iter(|| black_box(repeat.invoke(&args))),
        );

        let args = create_args::<i64>(size, 32, repeat_times, false);
        group.bench_function(
            format!(
                "repeat_large_string [size={}, repeat_times={}]",
                size, repeat_times
            ),
            |b| b.iter(|| black_box(repeat.invoke(&args))),
        );

        group.finish();
    }
}

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);
