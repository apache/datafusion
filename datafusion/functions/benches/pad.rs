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

use arrow::array::{ArrayRef, ArrowPrimitiveType, OffsetSizeTrait, PrimitiveArray};
use arrow::datatypes::{DataType, Field, Int64Type};
use arrow::util::bench_util::{
    create_string_array_with_len, create_string_view_array_with_len,
};
use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion};
use datafusion_common::config::ConfigOptions;
use datafusion_common::DataFusionError;
use datafusion_expr::{ColumnarValue, ScalarFunctionArgs};
use datafusion_functions::unicode::{lpad, rpad};
use rand::distr::{Distribution, Uniform};
use rand::Rng;
use std::sync::Arc;

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

fn create_args<O: OffsetSizeTrait>(
    size: usize,
    str_len: usize,
    force_view_types: bool,
) -> Vec<ColumnarValue> {
    let length_array = Arc::new(create_primitive_array::<Int64Type>(size, 0.0, str_len));

    if !force_view_types {
        let string_array =
            Arc::new(create_string_array_with_len::<O>(size, 0.1, str_len));
        let fill_array = Arc::new(create_string_array_with_len::<O>(size, 0.1, str_len));

        vec![
            ColumnarValue::Array(string_array),
            ColumnarValue::Array(Arc::clone(&length_array) as ArrayRef),
            ColumnarValue::Array(fill_array),
        ]
    } else {
        let string_array =
            Arc::new(create_string_view_array_with_len(size, 0.1, str_len, false));
        let fill_array =
            Arc::new(create_string_view_array_with_len(size, 0.1, str_len, false));

        vec![
            ColumnarValue::Array(string_array),
            ColumnarValue::Array(Arc::clone(&length_array) as ArrayRef),
            ColumnarValue::Array(fill_array),
        ]
    }
}

fn invoke_pad_with_args(
    args: Vec<ColumnarValue>,
    number_rows: usize,
    left_pad: bool,
) -> Result<ColumnarValue, DataFusionError> {
    let arg_fields = args
        .iter()
        .enumerate()
        .map(|(idx, arg)| Field::new(format!("arg_{idx}"), arg.data_type(), true).into())
        .collect::<Vec<_>>();
    let config_options = Arc::new(ConfigOptions::default());

    let scalar_args = ScalarFunctionArgs {
        args: args.clone(),
        arg_fields,
        number_rows,
        return_field: Field::new("f", DataType::Utf8, true).into(),
        config_options: Arc::clone(&config_options),
    };

    if left_pad {
        lpad().invoke_with_args(scalar_args)
    } else {
        rpad().invoke_with_args(scalar_args)
    }
}

fn criterion_benchmark(c: &mut Criterion) {
    for size in [1024, 2048] {
        let mut group = c.benchmark_group("lpad function");

        let args = create_args::<i32>(size, 32, false);

        group.bench_function(BenchmarkId::new("utf8 type", size), |b| {
            b.iter(|| {
                criterion::black_box(
                    invoke_pad_with_args(args.clone(), size, true).unwrap(),
                )
            })
        });

        let args = create_args::<i64>(size, 32, false);
        group.bench_function(BenchmarkId::new("largeutf8 type", size), |b| {
            b.iter(|| {
                criterion::black_box(
                    invoke_pad_with_args(args.clone(), size, true).unwrap(),
                )
            })
        });

        let args = create_args::<i32>(size, 32, true);
        group.bench_function(BenchmarkId::new("stringview type", size), |b| {
            b.iter(|| {
                criterion::black_box(
                    invoke_pad_with_args(args.clone(), size, true).unwrap(),
                )
            })
        });

        group.finish();

        let mut group = c.benchmark_group("rpad function");

        let args = create_args::<i32>(size, 32, false);
        group.bench_function(BenchmarkId::new("utf8 type", size), |b| {
            b.iter(|| {
                criterion::black_box(
                    invoke_pad_with_args(args.clone(), size, false).unwrap(),
                )
            })
        });

        let args = create_args::<i64>(size, 32, false);
        group.bench_function(BenchmarkId::new("largeutf8 type", size), |b| {
            b.iter(|| {
                criterion::black_box(
                    invoke_pad_with_args(args.clone(), size, false).unwrap(),
                )
            })
        });

        // rpad for stringview type
        let args = create_args::<i32>(size, 32, true);
        group.bench_function(BenchmarkId::new("stringview type", size), |b| {
            b.iter(|| {
                criterion::black_box(
                    invoke_pad_with_args(args.clone(), size, false).unwrap(),
                )
            })
        });

        group.finish();
    }
}

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);
