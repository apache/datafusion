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

use arrow::{
    array::{ArrayRef, Int64Array},
    datatypes::DataType,
};
use criterion::{black_box, criterion_group, criterion_main, Criterion};
use datafusion_common::ScalarValue;
use datafusion_expr::{ColumnarValue, ScalarFunctionArgs};
use datafusion_functions::math::gcd;
use rand::Rng;
use std::sync::Arc;

fn generate_i64_array(n_rows: usize) -> ArrayRef {
    let mut rng = rand::thread_rng();
    let values = (0..n_rows)
        .map(|_| rng.gen_range(0..1000))
        .collect::<Vec<_>>();
    Arc::new(Int64Array::from(values)) as ArrayRef
}

fn criterion_benchmark(c: &mut Criterion) {
    let n_rows = 100000;
    let array_a = ColumnarValue::Array(generate_i64_array(n_rows));
    let array_b = ColumnarValue::Array(generate_i64_array(n_rows));
    let udf = gcd();

    c.bench_function("gcd both array", |b| {
        b.iter(|| {
            black_box(
                udf.invoke_with_args(ScalarFunctionArgs {
                    args: vec![array_a.clone(), array_b.clone()],
                    number_rows: 0,
                    return_type: &DataType::Int64,
                })
                .expect("date_bin should work on valid values"),
            )
        })
    });

    // 10! = 3628800
    let scalar_b = ColumnarValue::Scalar(ScalarValue::Int64(Some(3628800)));

    c.bench_function("gcd array and scalar", |b| {
        b.iter(|| {
            black_box(
                udf.invoke_with_args(ScalarFunctionArgs {
                    args: vec![array_a.clone(), scalar_b.clone()],
                    number_rows: 0,
                    return_type: &DataType::Int64,
                })
                .expect("date_bin should work on valid values"),
            )
        })
    });

    // scalar and scalar
    let scalar_a = ColumnarValue::Scalar(ScalarValue::Int64(Some(3628800)));

    c.bench_function("gcd both scalar", |b| {
        b.iter(|| {
            black_box(
                udf.invoke_with_args(ScalarFunctionArgs {
                    args: vec![scalar_a.clone(), scalar_b.clone()],
                    number_rows: 0,
                    return_type: &DataType::Int64,
                })
                .expect("date_bin should work on valid values"),
            )
        })
    });
}

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);
