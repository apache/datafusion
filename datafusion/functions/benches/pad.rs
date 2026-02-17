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

use arrow::array::{ArrowPrimitiveType, OffsetSizeTrait, PrimitiveArray};
use arrow::datatypes::{DataType, Field, Int64Type};
use arrow::util::bench_util::{
    create_string_array_with_len, create_string_view_array_with_len,
};
use criterion::{Criterion, SamplingMode, criterion_group, criterion_main};
use datafusion_common::config::ConfigOptions;
use datafusion_expr::{ColumnarValue, ScalarFunctionArgs};
use datafusion_functions::unicode;
use rand::Rng;
use rand::distr::{Distribution, Uniform};
use std::hint::black_box;
use std::sync::Arc;
use std::time::Duration;

struct Filter<Dist> {
    dist: Dist,
}

impl<T, Dist> Distribution<T> for Filter<Dist>
where
    Dist: Distribution<T>,
{
    fn sample<R: Rng + ?Sized>(&self, rng: &mut R) -> T {
        self.dist.sample(rng)
    }
}

pub fn create_primitive_array<T>(
    size: usize,
    null_density: f32,
    len: usize,
) -> PrimitiveArray<T>
where
    T: ArrowPrimitiveType<Native = i64>,
{
    let dist = Filter {
        dist: Uniform::new_inclusive::<i64, i64>(0, len as i64),
    };

    let mut rng = rand::rng();
    (0..size)
        .map(|_| {
            if rng.random::<f32>() < null_density {
                None
            } else {
                Some(rng.sample(dist.dist.unwrap()))
            }
        })
        .collect()
}

/// Create args for pad benchmark
fn create_pad_args<O: OffsetSizeTrait>(
    size: usize,
    str_len: usize,
    target_len: usize,
    use_string_view: bool,
) -> Vec<ColumnarValue> {
    let length_array =
        Arc::new(create_primitive_array::<Int64Type>(size, 0.0, target_len));

    if use_string_view {
        let string_array = create_string_view_array_with_len(size, 0.1, str_len, false);
        let fill_array = create_string_view_array_with_len(size, 0.1, str_len, false);
        vec![
            ColumnarValue::Array(Arc::new(string_array)),
            ColumnarValue::Array(length_array),
            ColumnarValue::Array(Arc::new(fill_array)),
        ]
    } else {
        let string_array = create_string_array_with_len::<O>(size, 0.1, str_len);
        let fill_array = create_string_array_with_len::<O>(size, 0.1, str_len);
        vec![
            ColumnarValue::Array(Arc::new(string_array)),
            ColumnarValue::Array(length_array),
            ColumnarValue::Array(Arc::new(fill_array)),
        ]
    }
}

fn criterion_benchmark(c: &mut Criterion) {
    for size in [1024, 4096] {
        let mut group = c.benchmark_group(format!("lpad size={size}"));
        group.sampling_mode(SamplingMode::Flat);
        group.sample_size(10);
        group.measurement_time(Duration::from_secs(10));

        // Utf8 type
        let args = create_pad_args::<i32>(size, 5, 20, false);
        let arg_fields = args
            .iter()
            .enumerate()
            .map(|(idx, arg)| {
                Field::new(format!("arg_{idx}"), arg.data_type(), true).into()
            })
            .collect::<Vec<_>>();
        let config_options = Arc::new(ConfigOptions::default());

        group.bench_function(
            format!("lpad utf8 [size={size}, str_len=5, target=20]"),
            |b| {
                b.iter(|| {
                    let args_cloned = args.clone();
                    black_box(unicode::lpad().invoke_with_args(ScalarFunctionArgs {
                        args: args_cloned,
                        arg_fields: arg_fields.clone(),
                        number_rows: size,
                        return_field: Field::new("f", DataType::Utf8, true).into(),
                        config_options: Arc::clone(&config_options),
                    }))
                })
            },
        );

        // StringView type
        let args = create_pad_args::<i32>(size, 5, 20, true);
        let arg_fields = args
            .iter()
            .enumerate()
            .map(|(idx, arg)| {
                Field::new(format!("arg_{idx}"), arg.data_type(), true).into()
            })
            .collect::<Vec<_>>();

        group.bench_function(
            format!("lpad stringview [size={size}, str_len=5, target=20]"),
            |b| {
                b.iter(|| {
                    let args_cloned = args.clone();
                    black_box(unicode::lpad().invoke_with_args(ScalarFunctionArgs {
                        args: args_cloned,
                        arg_fields: arg_fields.clone(),
                        number_rows: size,
                        return_field: Field::new("f", DataType::Utf8View, true).into(),
                        config_options: Arc::clone(&config_options),
                    }))
                })
            },
        );

        // Utf8 type with longer strings
        let args = create_pad_args::<i32>(size, 20, 50, false);
        let arg_fields = args
            .iter()
            .enumerate()
            .map(|(idx, arg)| {
                Field::new(format!("arg_{idx}"), arg.data_type(), true).into()
            })
            .collect::<Vec<_>>();

        group.bench_function(
            format!("lpad utf8 [size={size}, str_len=20, target=50]"),
            |b| {
                b.iter(|| {
                    let args_cloned = args.clone();
                    black_box(unicode::lpad().invoke_with_args(ScalarFunctionArgs {
                        args: args_cloned,
                        arg_fields: arg_fields.clone(),
                        number_rows: size,
                        return_field: Field::new("f", DataType::Utf8, true).into(),
                        config_options: Arc::clone(&config_options),
                    }))
                })
            },
        );

        // StringView type with longer strings
        let args = create_pad_args::<i32>(size, 20, 50, true);
        let arg_fields = args
            .iter()
            .enumerate()
            .map(|(idx, arg)| {
                Field::new(format!("arg_{idx}"), arg.data_type(), true).into()
            })
            .collect::<Vec<_>>();

        group.bench_function(
            format!("lpad stringview [size={size}, str_len=20, target=50]"),
            |b| {
                b.iter(|| {
                    let args_cloned = args.clone();
                    black_box(unicode::lpad().invoke_with_args(ScalarFunctionArgs {
                        args: args_cloned,
                        arg_fields: arg_fields.clone(),
                        number_rows: size,
                        return_field: Field::new("f", DataType::Utf8View, true).into(),
                        config_options: Arc::clone(&config_options),
                    }))
                })
            },
        );

        group.finish();
    }

    for size in [1024, 4096] {
        let mut group = c.benchmark_group(format!("rpad size={size}"));
        group.sampling_mode(SamplingMode::Flat);
        group.sample_size(10);
        group.measurement_time(Duration::from_secs(10));

        // Utf8 type
        let args = create_pad_args::<i32>(size, 5, 20, false);
        let arg_fields = args
            .iter()
            .enumerate()
            .map(|(idx, arg)| {
                Field::new(format!("arg_{idx}"), arg.data_type(), true).into()
            })
            .collect::<Vec<_>>();
        let config_options = Arc::new(ConfigOptions::default());

        group.bench_function(
            format!("rpad utf8 [size={size}, str_len=5, target=20]"),
            |b| {
                b.iter(|| {
                    let args_cloned = args.clone();
                    black_box(unicode::rpad().invoke_with_args(ScalarFunctionArgs {
                        args: args_cloned,
                        arg_fields: arg_fields.clone(),
                        number_rows: size,
                        return_field: Field::new("f", DataType::Utf8, true).into(),
                        config_options: Arc::clone(&config_options),
                    }))
                })
            },
        );

        // StringView type
        let args = create_pad_args::<i32>(size, 5, 20, true);
        let arg_fields = args
            .iter()
            .enumerate()
            .map(|(idx, arg)| {
                Field::new(format!("arg_{idx}"), arg.data_type(), true).into()
            })
            .collect::<Vec<_>>();

        group.bench_function(
            format!("rpad stringview [size={size}, str_len=5, target=20]"),
            |b| {
                b.iter(|| {
                    let args_cloned = args.clone();
                    black_box(unicode::rpad().invoke_with_args(ScalarFunctionArgs {
                        args: args_cloned,
                        arg_fields: arg_fields.clone(),
                        number_rows: size,
                        return_field: Field::new("f", DataType::Utf8View, true).into(),
                        config_options: Arc::clone(&config_options),
                    }))
                })
            },
        );

        // Utf8 type with longer strings
        let args = create_pad_args::<i32>(size, 20, 50, false);
        let arg_fields = args
            .iter()
            .enumerate()
            .map(|(idx, arg)| {
                Field::new(format!("arg_{idx}"), arg.data_type(), true).into()
            })
            .collect::<Vec<_>>();

        group.bench_function(
            format!("rpad utf8 [size={size}, str_len=20, target=50]"),
            |b| {
                b.iter(|| {
                    let args_cloned = args.clone();
                    black_box(unicode::rpad().invoke_with_args(ScalarFunctionArgs {
                        args: args_cloned,
                        arg_fields: arg_fields.clone(),
                        number_rows: size,
                        return_field: Field::new("f", DataType::Utf8, true).into(),
                        config_options: Arc::clone(&config_options),
                    }))
                })
            },
        );

        // StringView type with longer strings
        let args = create_pad_args::<i32>(size, 20, 50, true);
        let arg_fields = args
            .iter()
            .enumerate()
            .map(|(idx, arg)| {
                Field::new(format!("arg_{idx}"), arg.data_type(), true).into()
            })
            .collect::<Vec<_>>();

        group.bench_function(
            format!("rpad stringview [size={size}, str_len=20, target=50]"),
            |b| {
                b.iter(|| {
                    let args_cloned = args.clone();
                    black_box(unicode::rpad().invoke_with_args(ScalarFunctionArgs {
                        args: args_cloned,
                        arg_fields: arg_fields.clone(),
                        number_rows: size,
                        return_field: Field::new("f", DataType::Utf8View, true).into(),
                        config_options: Arc::clone(&config_options),
                    }))
                })
            },
        );

        group.finish();
    }
}

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);
