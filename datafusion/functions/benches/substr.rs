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

use arrow::array::{ArrayRef, Int64Array, OffsetSizeTrait};
use arrow::datatypes::{DataType, Field};
use arrow::util::bench_util::{
    create_string_array_with_len, create_string_view_array_with_len,
};
use criterion::{Criterion, SamplingMode, criterion_group, criterion_main};
use datafusion_common::config::ConfigOptions;
use datafusion_common::{DataFusionError, ScalarValue};
use datafusion_expr::{ColumnarValue, ScalarFunctionArgs};
use datafusion_functions::unicode;
use std::hint::black_box;
use std::sync::Arc;

fn make_i64_arg(value: i64, size: usize, as_scalar: bool) -> ColumnarValue {
    if as_scalar {
        ColumnarValue::Scalar(ScalarValue::from(value))
    } else {
        ColumnarValue::Array(Arc::new(Int64Array::from(vec![value; size])))
    }
}

fn create_args_without_count<O: OffsetSizeTrait>(
    size: usize,
    str_len: usize,
    start_half_way: bool,
    force_view_types: bool,
    scalar_start: bool,
) -> Vec<ColumnarValue> {
    let start_val = if start_half_way {
        (str_len / 2) as i64
    } else {
        1i64
    };
    let start = make_i64_arg(start_val, size, scalar_start);

    let string_array: ArrayRef = if force_view_types {
        Arc::new(create_string_view_array_with_len(size, 0.1, str_len, false))
    } else {
        Arc::new(create_string_array_with_len::<O>(size, 0.1, str_len))
    };

    vec![ColumnarValue::Array(string_array), start]
}

fn create_args_with_count<O: OffsetSizeTrait>(
    size: usize,
    str_len: usize,
    count_max: usize,
    force_view_types: bool,
    scalar_args: bool,
) -> Vec<ColumnarValue> {
    let count = count_max.min(str_len) as i64;
    let start = make_i64_arg(1i64, size, scalar_args);
    let count = make_i64_arg(count, size, scalar_args);

    let string_array: ArrayRef = if force_view_types {
        Arc::new(create_string_view_array_with_len(size, 0.1, str_len, false))
    } else {
        Arc::new(create_string_array_with_len::<O>(size, 0.1, str_len))
    };

    vec![ColumnarValue::Array(string_array), start, count]
}

#[expect(clippy::needless_pass_by_value)]
fn invoke_substr_with_args(
    args: Vec<ColumnarValue>,
    number_rows: usize,
) -> Result<ColumnarValue, DataFusionError> {
    let arg_fields = args
        .iter()
        .enumerate()
        .map(|(idx, arg)| Field::new(format!("arg_{idx}"), arg.data_type(), true).into())
        .collect::<Vec<_>>();
    let config_options = Arc::new(ConfigOptions::default());

    unicode::substr().invoke_with_args(ScalarFunctionArgs {
        args: args.clone(),
        arg_fields,
        number_rows,
        return_field: Field::new("f", DataType::Utf8View, true).into(),
        config_options: Arc::clone(&config_options),
    })
}

fn criterion_benchmark(c: &mut Criterion) {
    for size in [1024, 4096] {
        // string_len = 12, substring_len=6 (see `create_args_without_count`)
        let len = 12;
        let mut group = c.benchmark_group("substr, no count, short strings");
        group.sampling_mode(SamplingMode::Flat);
        group.sample_size(10);

        let args = create_args_without_count::<i32>(size, len, true, true, false);
        group.bench_function(
            format!("substr_string_view [size={size}, strlen={len}]"),
            |b| b.iter(|| black_box(invoke_substr_with_args(args.clone(), size))),
        );

        let args = create_args_without_count::<i32>(size, len, false, false, false);
        group.bench_function(format!("substr_string [size={size}, strlen={len}]"), |b| {
            b.iter(|| black_box(invoke_substr_with_args(args.clone(), size)))
        });

        let args = create_args_without_count::<i64>(size, len, true, false, false);
        group.bench_function(
            format!("substr_large_string [size={size}, strlen={len}]"),
            |b| b.iter(|| black_box(invoke_substr_with_args(args.clone(), size))),
        );

        group.finish();

        // string_len = 128, start=1, count=64, substring_len=64
        let len = 128;
        let count = 64;
        let mut group = c.benchmark_group("substr, with count, long strings");
        group.sampling_mode(SamplingMode::Flat);
        group.sample_size(10);

        let args = create_args_with_count::<i32>(size, len, count, true, false);
        group.bench_function(
            format!("substr_string_view [size={size}, count={count}, strlen={len}]",),
            |b| b.iter(|| black_box(invoke_substr_with_args(args.clone(), size))),
        );

        let args = create_args_with_count::<i32>(size, len, count, false, false);
        group.bench_function(
            format!("substr_string [size={size}, count={count}, strlen={len}]",),
            |b| b.iter(|| black_box(invoke_substr_with_args(args.clone(), size))),
        );

        let args = create_args_with_count::<i64>(size, len, count, false, false);
        group.bench_function(
            format!("substr_large_string [size={size}, count={count}, strlen={len}]",),
            |b| b.iter(|| black_box(invoke_substr_with_args(args.clone(), size))),
        );

        group.finish();

        // string_len = 128, start=1, count=6, substring_len=6
        let len = 128;
        let count = 6;
        let mut group = c.benchmark_group("substr, short count, long strings");
        group.sampling_mode(SamplingMode::Flat);
        group.sample_size(10);

        let args = create_args_with_count::<i32>(size, len, count, true, false);
        group.bench_function(
            format!("substr_string_view [size={size}, count={count}, strlen={len}]",),
            |b| b.iter(|| black_box(invoke_substr_with_args(args.clone(), size))),
        );

        let args = create_args_with_count::<i32>(size, len, count, false, false);
        group.bench_function(
            format!("substr_string [size={size}, count={count}, strlen={len}]",),
            |b| b.iter(|| black_box(invoke_substr_with_args(args.clone(), size))),
        );

        let args = create_args_with_count::<i64>(size, len, count, false, false);
        group.bench_function(
            format!("substr_large_string [size={size}, count={count}, strlen={len}]",),
            |b| b.iter(|| black_box(invoke_substr_with_args(args.clone(), size))),
        );

        group.finish();

        // Scalar start, no count, short strings
        let len = 12;
        let mut group =
            c.benchmark_group("substr, scalar start, no count, short strings");
        group.sampling_mode(SamplingMode::Flat);
        group.sample_size(10);

        let args = create_args_without_count::<i32>(size, len, true, true, true);
        group.bench_function(
            format!("substr_string_view [size={size}, strlen={len}]"),
            |b| b.iter(|| black_box(invoke_substr_with_args(args.clone(), size))),
        );

        let args = create_args_without_count::<i32>(size, len, false, false, true);
        group.bench_function(format!("substr_string [size={size}, strlen={len}]"), |b| {
            b.iter(|| black_box(invoke_substr_with_args(args.clone(), size)))
        });

        group.finish();

        // Scalar start, no count, long strings
        let len = 128;
        let mut group = c.benchmark_group("substr, scalar start, no count, long strings");
        group.sampling_mode(SamplingMode::Flat);
        group.sample_size(10);

        let args = create_args_without_count::<i32>(size, len, true, true, true);
        group.bench_function(
            format!("substr_string_view [size={size}, strlen={len}]"),
            |b| b.iter(|| black_box(invoke_substr_with_args(args.clone(), size))),
        );

        let args = create_args_without_count::<i32>(size, len, false, false, true);
        group.bench_function(format!("substr_string [size={size}, strlen={len}]"), |b| {
            b.iter(|| black_box(invoke_substr_with_args(args.clone(), size)))
        });

        group.finish();

        // Scalar start and count, short strings
        let len = 12;
        let count = 6;
        let mut group = c.benchmark_group("substr, scalar args, short strings");
        group.sampling_mode(SamplingMode::Flat);
        group.sample_size(10);

        let args = create_args_with_count::<i32>(size, len, count, true, true);
        group.bench_function(
            format!("substr_string_view [size={size}, count={count}, strlen={len}]"),
            |b| b.iter(|| black_box(invoke_substr_with_args(args.clone(), size))),
        );

        let args = create_args_with_count::<i32>(size, len, count, false, true);
        group.bench_function(
            format!("substr_string [size={size}, count={count}, strlen={len}]"),
            |b| b.iter(|| black_box(invoke_substr_with_args(args.clone(), size))),
        );

        group.finish();

        // Scalar start and count, long strings
        let len = 128;
        let count = 64;
        let mut group = c.benchmark_group("substr, scalar args, long strings");
        group.sampling_mode(SamplingMode::Flat);
        group.sample_size(10);

        let args = create_args_with_count::<i32>(size, len, count, true, true);
        group.bench_function(
            format!("substr_string_view [size={size}, count={count}, strlen={len}]"),
            |b| b.iter(|| black_box(invoke_substr_with_args(args.clone(), size))),
        );

        let args = create_args_with_count::<i32>(size, len, count, false, true);
        group.bench_function(
            format!("substr_string [size={size}, count={count}, strlen={len}]"),
            |b| b.iter(|| black_box(invoke_substr_with_args(args.clone(), size))),
        );

        let args = create_args_with_count::<i64>(size, len, count, false, true);
        group.bench_function(
            format!("substr_large_string [size={size}, count={count}, strlen={len}]"),
            |b| b.iter(|| black_box(invoke_substr_with_args(args.clone(), size))),
        );

        group.finish();
    }
}

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);
