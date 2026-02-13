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

use arrow::array::{ArrayRef, OffsetSizeTrait, StringArray, StringViewBuilder};
use arrow::datatypes::{DataType, Field};
use arrow::util::bench_util::{
    create_string_array_with_len, create_string_view_array_with_len,
};
use criterion::{Criterion, SamplingMode, criterion_group, criterion_main};
use datafusion_common::ScalarValue;
use datafusion_common::config::ConfigOptions;
use datafusion_expr::{ColumnarValue, ScalarFunctionArgs};
use datafusion_functions::unicode;
use std::hint::black_box;
use std::sync::Arc;
use std::time::Duration;

fn create_args<O: OffsetSizeTrait>(
    size: usize,
    str_len: usize,
    force_view_types: bool,
) -> Vec<ColumnarValue> {
    if force_view_types {
        let string_array =
            Arc::new(create_string_view_array_with_len(size, 0.2, str_len, false));

        vec![ColumnarValue::Array(string_array)]
    } else {
        let string_array =
            Arc::new(create_string_array_with_len::<O>(size, 0.2, str_len));

        vec![ColumnarValue::Array(string_array)]
    }
}

/// Create a Utf8 array where every value contains non-ASCII Unicode text.
fn create_unicode_utf8_args(size: usize) -> Vec<ColumnarValue> {
    let items: Vec<String> = (0..size)
        .map(|_| "ñAnDÚ ÁrBOL ОлЕГ ÍslENsku".to_string())
        .collect();
    let array = Arc::new(StringArray::from(items)) as ArrayRef;
    vec![ColumnarValue::Array(array)]
}

/// Create a Utf8View array where every value contains non-ASCII Unicode text.
fn create_unicode_utf8view_args(size: usize) -> Vec<ColumnarValue> {
    let mut builder = StringViewBuilder::with_capacity(size);
    for _ in 0..size {
        builder.append_value("ñAnDÚ ÁrBOL ОлЕГ ÍslENsku");
    }
    let array = Arc::new(builder.finish()) as ArrayRef;
    vec![ColumnarValue::Array(array)]
}

fn criterion_benchmark(c: &mut Criterion) {
    let initcap = unicode::initcap();
    let config_options = Arc::new(ConfigOptions::default());

    // Array benchmarks: vary both row count and string length
    for size in [1024, 4096, 8192] {
        for str_len in [16, 128] {
            let mut group =
                c.benchmark_group(format!("initcap size={size} str_len={str_len}"));
            group.sampling_mode(SamplingMode::Flat);
            group.sample_size(10);
            group.measurement_time(Duration::from_secs(10));

            // Utf8
            let array_args = create_args::<i32>(size, str_len, false);
            let array_arg_fields = vec![Field::new("arg_0", DataType::Utf8, true).into()];

            group.bench_function("array_utf8", |b| {
                b.iter(|| {
                    black_box(initcap.invoke_with_args(ScalarFunctionArgs {
                        args: array_args.clone(),
                        arg_fields: array_arg_fields.clone(),
                        number_rows: size,
                        return_field: Field::new("f", DataType::Utf8, true).into(),
                        config_options: Arc::clone(&config_options),
                    }))
                })
            });

            // Utf8View
            let array_view_args = create_args::<i32>(size, str_len, true);
            let array_view_arg_fields =
                vec![Field::new("arg_0", DataType::Utf8View, true).into()];

            group.bench_function("array_utf8view", |b| {
                b.iter(|| {
                    black_box(initcap.invoke_with_args(ScalarFunctionArgs {
                        args: array_view_args.clone(),
                        arg_fields: array_view_arg_fields.clone(),
                        number_rows: size,
                        return_field: Field::new("f", DataType::Utf8View, true).into(),
                        config_options: Arc::clone(&config_options),
                    }))
                })
            });

            group.finish();
        }
    }

    // Unicode array benchmarks
    for size in [1024, 4096, 8192] {
        let mut group = c.benchmark_group(format!("initcap unicode size={size}"));
        group.sampling_mode(SamplingMode::Flat);
        group.sample_size(10);
        group.measurement_time(Duration::from_secs(10));

        let unicode_args = create_unicode_utf8_args(size);
        let unicode_arg_fields = vec![Field::new("arg_0", DataType::Utf8, true).into()];

        group.bench_function("array_utf8", |b| {
            b.iter(|| {
                black_box(initcap.invoke_with_args(ScalarFunctionArgs {
                    args: unicode_args.clone(),
                    arg_fields: unicode_arg_fields.clone(),
                    number_rows: size,
                    return_field: Field::new("f", DataType::Utf8, true).into(),
                    config_options: Arc::clone(&config_options),
                }))
            })
        });

        let unicode_view_args = create_unicode_utf8view_args(size);
        let unicode_view_arg_fields =
            vec![Field::new("arg_0", DataType::Utf8View, true).into()];

        group.bench_function("array_utf8view", |b| {
            b.iter(|| {
                black_box(initcap.invoke_with_args(ScalarFunctionArgs {
                    args: unicode_view_args.clone(),
                    arg_fields: unicode_view_arg_fields.clone(),
                    number_rows: size,
                    return_field: Field::new("f", DataType::Utf8View, true).into(),
                    config_options: Arc::clone(&config_options),
                }))
            })
        });

        group.finish();
    }

    // Scalar benchmarks: independent of array size, run once
    {
        let mut group = c.benchmark_group("initcap scalar");
        group.sampling_mode(SamplingMode::Flat);
        group.sample_size(10);
        group.measurement_time(Duration::from_secs(10));

        // Utf8
        let scalar_args = vec![ColumnarValue::Scalar(ScalarValue::Utf8(Some(
            "hello world test string".to_string(),
        )))];
        let scalar_arg_fields = vec![Field::new("arg_0", DataType::Utf8, false).into()];

        group.bench_function("scalar_utf8", |b| {
            b.iter(|| {
                black_box(initcap.invoke_with_args(ScalarFunctionArgs {
                    args: scalar_args.clone(),
                    arg_fields: scalar_arg_fields.clone(),
                    number_rows: 1,
                    return_field: Field::new("f", DataType::Utf8, false).into(),
                    config_options: Arc::clone(&config_options),
                }))
            })
        });

        // Utf8View
        let scalar_view_args = vec![ColumnarValue::Scalar(ScalarValue::Utf8View(Some(
            "hello world test string".to_string(),
        )))];
        let scalar_view_arg_fields =
            vec![Field::new("arg_0", DataType::Utf8View, false).into()];

        group.bench_function("scalar_utf8view", |b| {
            b.iter(|| {
                black_box(initcap.invoke_with_args(ScalarFunctionArgs {
                    args: scalar_view_args.clone(),
                    arg_fields: scalar_view_arg_fields.clone(),
                    number_rows: 1,
                    return_field: Field::new("f", DataType::Utf8View, false).into(),
                    config_options: Arc::clone(&config_options),
                }))
            })
        });

        group.finish();
    }
}

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);
