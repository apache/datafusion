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
use arrow::datatypes::{DataType, Field};
use arrow::util::bench_util::{
    create_string_array_with_len, create_string_view_array_with_len,
};
use criterion::{black_box, criterion_group, criterion_main, Criterion, SamplingMode};
use datafusion_common::config::ConfigOptions;
use datafusion_common::DataFusionError;
use datafusion_expr::{ColumnarValue, ScalarFunctionArgs};
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

fn invoke_repeat_with_args(
    args: Vec<ColumnarValue>,
    repeat_times: i64,
) -> Result<ColumnarValue, DataFusionError> {
    let arg_fields = args
        .iter()
        .enumerate()
        .map(|(idx, arg)| Field::new(format!("arg_{idx}"), arg.data_type(), true).into())
        .collect::<Vec<_>>();
    let config_options = Arc::new(ConfigOptions::default());

    string::repeat().invoke_with_args(ScalarFunctionArgs {
        args,
        arg_fields,
        number_rows: repeat_times as usize,
        return_field: Field::new("f", DataType::Utf8, true).into(),
        config_options: Arc::clone(&config_options),
    })
}

fn criterion_benchmark(c: &mut Criterion) {
    for size in [1024, 4096] {
        // REPEAT 3 TIMES
        let repeat_times = 3;
        let mut group = c.benchmark_group(format!("repeat {repeat_times} times"));
        group.sampling_mode(SamplingMode::Flat);
        group.sample_size(10);
        group.measurement_time(Duration::from_secs(10));

        let args = create_args::<i32>(size, 32, repeat_times, true);
        group.bench_function(
            format!("repeat_string_view [size={size}, repeat_times={repeat_times}]"),
            |b| {
                b.iter(|| {
                    let args_cloned = args.clone();
                    black_box(invoke_repeat_with_args(args_cloned, repeat_times))
                })
            },
        );

        let args = create_args::<i32>(size, 32, repeat_times, false);
        group.bench_function(
            format!("repeat_string [size={size}, repeat_times={repeat_times}]"),
            |b| {
                b.iter(|| {
                    let args_cloned = args.clone();
                    black_box(invoke_repeat_with_args(args_cloned, repeat_times))
                })
            },
        );

        let args = create_args::<i64>(size, 32, repeat_times, false);
        group.bench_function(
            format!("repeat_large_string [size={size}, repeat_times={repeat_times}]"),
            |b| {
                b.iter(|| {
                    let args_cloned = args.clone();
                    black_box(invoke_repeat_with_args(args_cloned, repeat_times))
                })
            },
        );

        group.finish();

        // REPEAT 30 TIMES
        let repeat_times = 30;
        let mut group = c.benchmark_group(format!("repeat {repeat_times} times"));
        group.sampling_mode(SamplingMode::Flat);
        group.sample_size(10);
        group.measurement_time(Duration::from_secs(10));

        let args = create_args::<i32>(size, 32, repeat_times, true);
        group.bench_function(
            format!("repeat_string_view [size={size}, repeat_times={repeat_times}]"),
            |b| {
                b.iter(|| {
                    let args_cloned = args.clone();
                    black_box(invoke_repeat_with_args(args_cloned, repeat_times))
                })
            },
        );

        let args = create_args::<i32>(size, 32, repeat_times, false);
        group.bench_function(
            format!("repeat_string [size={size}, repeat_times={repeat_times}]"),
            |b| {
                b.iter(|| {
                    let args_cloned = args.clone();
                    black_box(invoke_repeat_with_args(args_cloned, repeat_times))
                })
            },
        );

        let args = create_args::<i64>(size, 32, repeat_times, false);
        group.bench_function(
            format!("repeat_large_string [size={size}, repeat_times={repeat_times}]"),
            |b| {
                b.iter(|| {
                    let args_cloned = args.clone();
                    black_box(invoke_repeat_with_args(args_cloned, repeat_times))
                })
            },
        );

        group.finish();

        // REPEAT overflow
        let repeat_times = 1073741824;
        let mut group = c.benchmark_group(format!("repeat {repeat_times} times"));
        group.sampling_mode(SamplingMode::Flat);
        group.sample_size(10);
        group.measurement_time(Duration::from_secs(10));

        let args = create_args::<i32>(size, 2, repeat_times, false);
        group.bench_function(
            format!("repeat_string overflow [size={size}, repeat_times={repeat_times}]"),
            |b| {
                b.iter(|| {
                    let args_cloned = args.clone();
                    black_box(invoke_repeat_with_args(args_cloned, repeat_times))
                })
            },
        );

        group.finish();
    }
}

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);
