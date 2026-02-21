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

use arrow::array::OffsetSizeTrait;
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

fn criterion_benchmark(c: &mut Criterion) {
    let initcap = unicode::initcap();
    let config_options = Arc::new(ConfigOptions::default());

    // Grouped benchmarks for array sizes - to compare with scalar performance
    for size in [1024, 4096, 8192] {
        let mut group = c.benchmark_group(format!("initcap size={size}"));
        group.sampling_mode(SamplingMode::Flat);
        group.sample_size(10);
        group.measurement_time(Duration::from_secs(10));

        // Array benchmark - Utf8
        let array_args = create_args::<i32>(size, 16, false);
        let array_arg_fields = vec![Field::new("arg_0", DataType::Utf8, true).into()];
        let batch_len = size;

        group.bench_function("array_utf8", |b| {
            b.iter(|| {
                black_box(initcap.invoke_with_args(ScalarFunctionArgs {
                    args: array_args.clone(),
                    arg_fields: array_arg_fields.clone(),
                    number_rows: batch_len,
                    return_field: Field::new("f", DataType::Utf8, true).into(),
                    config_options: Arc::clone(&config_options),
                }))
            })
        });

        // Array benchmark - Utf8View
        let array_view_args = create_args::<i32>(size, 16, true);
        let array_view_arg_fields =
            vec![Field::new("arg_0", DataType::Utf8View, true).into()];

        group.bench_function("array_utf8view", |b| {
            b.iter(|| {
                black_box(initcap.invoke_with_args(ScalarFunctionArgs {
                    args: array_view_args.clone(),
                    arg_fields: array_view_arg_fields.clone(),
                    number_rows: batch_len,
                    return_field: Field::new("f", DataType::Utf8View, true).into(),
                    config_options: Arc::clone(&config_options),
                }))
            })
        });

        // Scalar benchmark - Utf8 (the optimization we added)
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

        // Scalar benchmark - Utf8View
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
