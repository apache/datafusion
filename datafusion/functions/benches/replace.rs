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

use arrow::array::{GenericStringArray, OffsetSizeTrait, StringViewArray};
use arrow::datatypes::{DataType, Field};
use arrow::util::bench_util::{
    create_string_array_with_len, create_string_view_array_with_len,
};
use criterion::{Criterion, SamplingMode, criterion_group, criterion_main};
use datafusion_common::DataFusionError;
use datafusion_common::config::ConfigOptions;
use datafusion_expr::{ColumnarValue, ScalarFunctionArgs};
use datafusion_functions::string;
use std::hint::black_box;
use std::sync::Arc;
use std::time::Duration;

/// Build a string array, dropping the null buffer when `null_density == 0.0`
fn make_string_array<O: OffsetSizeTrait>(
    size: usize,
    null_density: f32,
    str_len: usize,
) -> GenericStringArray<O> {
    let arr = create_string_array_with_len::<O>(size, null_density, str_len);
    if null_density == 0.0 {
        let (offsets, values, _) = arr.into_parts();
        GenericStringArray::<O>::new(offsets, values, None)
    } else {
        arr
    }
}

fn make_string_view_array(
    size: usize,
    null_density: f32,
    str_len: usize,
) -> StringViewArray {
    let arr = create_string_view_array_with_len(size, null_density, str_len, false);
    if null_density == 0.0 {
        let (views, buffers, _) = arr.into_parts();
        StringViewArray::new(views, buffers, None)
    } else {
        arr
    }
}

fn create_args<O: OffsetSizeTrait>(
    size: usize,
    str_len: usize,
    force_view_types: bool,
    from_len: usize,
    to_len: usize,
    null_density: f32,
) -> Vec<ColumnarValue> {
    // Apply `null_density` only to the string column; `from` and `to` are
    // typically not NULL in real-world workloads.
    if force_view_types {
        let string_array = Arc::new(make_string_view_array(size, null_density, str_len));
        let from_array = Arc::new(make_string_view_array(size, 0.0, from_len));
        let to_array = Arc::new(make_string_view_array(size, 0.0, to_len));
        vec![
            ColumnarValue::Array(string_array),
            ColumnarValue::Array(from_array),
            ColumnarValue::Array(to_array),
        ]
    } else {
        let string_array = Arc::new(make_string_array::<O>(size, null_density, str_len));
        let from_array = Arc::new(make_string_array::<O>(size, 0.0, from_len));
        let to_array = Arc::new(make_string_array::<O>(size, 0.0, to_len));

        vec![
            ColumnarValue::Array(string_array),
            ColumnarValue::Array(from_array),
            ColumnarValue::Array(to_array),
        ]
    }
}

fn invoke_replace_with_args(
    args: Vec<ColumnarValue>,
    number_rows: usize,
) -> Result<ColumnarValue, DataFusionError> {
    let arg_fields = args
        .iter()
        .enumerate()
        .map(|(idx, arg)| Field::new(format!("arg_{idx}"), arg.data_type(), true).into())
        .collect::<Vec<_>>();
    let config_options = Arc::new(ConfigOptions::default());

    string::replace().invoke_with_args(ScalarFunctionArgs {
        args,
        arg_fields,
        number_rows,
        return_field: Field::new("f", DataType::Utf8, true).into(),
        config_options: Arc::clone(&config_options),
    })
}

fn criterion_benchmark(c: &mut Criterion) {
    for size in [1024, 4096] {
        let mut group = c.benchmark_group(format!("replace size={size}"));
        group.sampling_mode(SamplingMode::Flat);
        group.sample_size(10);
        group.measurement_time(Duration::from_secs(10));

        for &nulls in &[0.0_f32, 0.2] {
            for &str_len in &[32_usize, 128] {
                // ASCII single character replacement (fast path)
                let args = create_args::<i32>(size, str_len, false, 1, 1, nulls);
                group.bench_function(
                    format!(
                        "replace_string_ascii_single [size={size}, str_len={str_len}, nulls={nulls}]"
                    ),
                    |b| {
                        b.iter(|| {
                            let args_cloned = args.clone();
                            black_box(invoke_replace_with_args(args_cloned, size))
                        })
                    },
                );

                // Multi-character strings (general path)
                let args = create_args::<i32>(size, str_len, true, 3, 5, nulls);
                group.bench_function(
                    format!(
                        "replace_string_view [size={size}, str_len={str_len}, nulls={nulls}]"
                    ),
                    |b| {
                        b.iter(|| {
                            let args_cloned = args.clone();
                            black_box(invoke_replace_with_args(args_cloned, size))
                        })
                    },
                );

                let args = create_args::<i32>(size, str_len, false, 3, 5, nulls);
                group.bench_function(
                    format!(
                        "replace_string [size={size}, str_len={str_len}, nulls={nulls}]"
                    ),
                    |b| {
                        b.iter(|| {
                            let args_cloned = args.clone();
                            black_box(invoke_replace_with_args(args_cloned, size))
                        })
                    },
                );
            }
        }

        group.finish();
    }
}

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);
