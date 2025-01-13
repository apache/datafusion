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

use std::sync::Arc;

use arrow::array::{Array, ArrayRef, Int32Array};
use criterion::{black_box, criterion_group, criterion_main, Criterion};
use rand::rngs::ThreadRng;
use rand::Rng;

use datafusion_common::ScalarValue;
use datafusion_expr::ColumnarValue;
use datafusion_functions::datetime::make_date;

fn years(rng: &mut ThreadRng) -> Int32Array {
    let mut years = vec![];
    for _ in 0..1000 {
        years.push(rng.gen_range(1900..2050));
    }

    Int32Array::from(years)
}

fn months(rng: &mut ThreadRng) -> Int32Array {
    let mut months = vec![];
    for _ in 0..1000 {
        months.push(rng.gen_range(1..13));
    }

    Int32Array::from(months)
}

fn days(rng: &mut ThreadRng) -> Int32Array {
    let mut days = vec![];
    for _ in 0..1000 {
        days.push(rng.gen_range(1..29));
    }

    Int32Array::from(days)
}
fn criterion_benchmark(c: &mut Criterion) {
    c.bench_function("make_date_col_col_col_1000", |b| {
        let mut rng = rand::thread_rng();
        let years_array = Arc::new(years(&mut rng)) as ArrayRef;
        let batch_len = years_array.len();
        let years = ColumnarValue::Array(years_array);
        let months = ColumnarValue::Array(Arc::new(months(&mut rng)) as ArrayRef);
        let days = ColumnarValue::Array(Arc::new(days(&mut rng)) as ArrayRef);

        b.iter(|| {
            // TODO use invoke_with_args
            black_box(
                make_date()
                    .invoke_batch(
                        &[years.clone(), months.clone(), days.clone()],
                        batch_len,
                    )
                    .expect("make_date should work on valid values"),
            )
        })
    });

    c.bench_function("make_date_scalar_col_col_1000", |b| {
        let mut rng = rand::thread_rng();
        let year = ColumnarValue::Scalar(ScalarValue::Int32(Some(2025)));
        let months_arr = Arc::new(months(&mut rng)) as ArrayRef;
        let batch_len = months_arr.len();
        let months = ColumnarValue::Array(months_arr);
        let days = ColumnarValue::Array(Arc::new(days(&mut rng)) as ArrayRef);

        b.iter(|| {
            // TODO use invoke_with_args
            black_box(
                make_date()
                    .invoke_batch(
                        &[year.clone(), months.clone(), days.clone()],
                        batch_len,
                    )
                    .expect("make_date should work on valid values"),
            )
        })
    });

    c.bench_function("make_date_scalar_scalar_col_1000", |b| {
        let mut rng = rand::thread_rng();
        let year = ColumnarValue::Scalar(ScalarValue::Int32(Some(2025)));
        let month = ColumnarValue::Scalar(ScalarValue::Int32(Some(11)));
        let day_arr = Arc::new(days(&mut rng));
        let batch_len = day_arr.len();
        let days = ColumnarValue::Array(day_arr);

        b.iter(|| {
            // TODO use invoke_with_args
            black_box(
                make_date()
                    .invoke_batch(&[year.clone(), month.clone(), days.clone()], batch_len)
                    .expect("make_date should work on valid values"),
            )
        })
    });

    c.bench_function("make_date_scalar_scalar_scalar", |b| {
        let year = ColumnarValue::Scalar(ScalarValue::Int32(Some(2025)));
        let month = ColumnarValue::Scalar(ScalarValue::Int32(Some(11)));
        let day = ColumnarValue::Scalar(ScalarValue::Int32(Some(26)));

        b.iter(|| {
            // TODO use invoke_with_args
            black_box(
                make_date()
                    .invoke_batch(&[year.clone(), month.clone(), day.clone()], 1)
                    .expect("make_date should work on valid values"),
            )
        })
    });
}

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);
