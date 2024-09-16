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
use arrow::datatypes::Int64Type;
use arrow::util::bench_util::{
    create_string_array_with_len, create_string_view_array_with_len,
};
use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion};
use datafusion_expr::ColumnarValue;
use datafusion_functions::unicode::{lpad, rpad};
use rand::distributions::{Distribution, Uniform};
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

    let mut rng = rand::thread_rng();
    (0..size)
        .map(|_| {
            if rng.gen::<f32>() < null_density {
                None
            } else {
                Some(rng.sample(&dist))
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

fn criterion_benchmark(c: &mut Criterion) {
    for size in [1024, 2048] {
        let mut group = c.benchmark_group("lpad function");

        let args = create_args::<i32>(size, 32, false);
        group.bench_function(BenchmarkId::new("utf8 type", size), |b| {
            b.iter(|| criterion::black_box(lpad().invoke(&args).unwrap()))
        });

        let args = create_args::<i64>(size, 32, false);
        group.bench_function(BenchmarkId::new("largeutf8 type", size), |b| {
            b.iter(|| criterion::black_box(lpad().invoke(&args).unwrap()))
        });

        let args = create_args::<i32>(size, 32, true);
        group.bench_function(BenchmarkId::new("stringview type", size), |b| {
            b.iter(|| criterion::black_box(lpad().invoke(&args).unwrap()))
        });

        group.finish();

        let mut group = c.benchmark_group("rpad function");

        let args = create_args::<i32>(size, 32, false);
        group.bench_function(BenchmarkId::new("utf8 type", size), |b| {
            b.iter(|| criterion::black_box(rpad().invoke(&args).unwrap()))
        });

        let args = create_args::<i64>(size, 32, false);
        group.bench_function(BenchmarkId::new("largeutf8 type", size), |b| {
            b.iter(|| criterion::black_box(rpad().invoke(&args).unwrap()))
        });

        // rpad for stringview type
        let args = create_args::<i32>(size, 32, true);
        group.bench_function(BenchmarkId::new("stringview type", size), |b| {
            b.iter(|| criterion::black_box(rpad().invoke(&args).unwrap()))
        });

        group.finish();
    }
}

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);
