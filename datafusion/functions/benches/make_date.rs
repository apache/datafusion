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
use arrow::datatypes::{DataType, Field};
use criterion::{black_box, criterion_group, criterion_main, Criterion};
use datafusion_common::config::ConfigOptions;
use datafusion_common::ScalarValue;
use datafusion_expr::{ColumnarValue, ScalarFunctionArgs};
use datafusion_functions::datetime::make_date;
use rand::rngs::ThreadRng;
use rand::Rng;

fn years(rng: &mut ThreadRng) -> Int32Array {
    let mut years = vec![];
    for _ in 0..1000 {
        years.push(rng.random_range(1900..2050));
    }

    Int32Array::from(years)
}

fn months(rng: &mut ThreadRng) -> Int32Array {
    let mut months = vec![];
    for _ in 0..1000 {
        months.push(rng.random_range(1..13));
    }

    Int32Array::from(months)
}

fn days(rng: &mut ThreadRng) -> Int32Array {
    let mut days = vec![];
    for _ in 0..1000 {
        days.push(rng.random_range(1..29));
    }

    Int32Array::from(days)
}
fn criterion_benchmark(c: &mut Criterion) {
    c.bench_function("make_date_col_col_col_1000", |b| {
        let mut rng = rand::rng();
        let years_array = Arc::new(years(&mut rng)) as ArrayRef;
        let batch_len = years_array.len();
        let years = ColumnarValue::Array(years_array);
        let months = ColumnarValue::Array(Arc::new(months(&mut rng)) as ArrayRef);
        let days = ColumnarValue::Array(Arc::new(days(&mut rng)) as ArrayRef);
        let arg_fields = vec![
            Field::new("a", years.data_type(), true).into(),
            Field::new("a", months.data_type(), true).into(),
            Field::new("a", days.data_type(), true).into(),
        ];
        let return_field = Field::new("f", DataType::Date32, true).into();
        let config_options = Arc::new(ConfigOptions::default());

        b.iter(|| {
            black_box(
                make_date()
                    .invoke_with_args(ScalarFunctionArgs {
                        args: vec![years.clone(), months.clone(), days.clone()],
                        arg_fields: arg_fields.clone(),
                        number_rows: batch_len,
                        return_field: Arc::clone(&return_field),
                        config_options: Arc::clone(&config_options),
                    })
                    .expect("make_date should work on valid values"),
            )
        })
    });

    c.bench_function("make_date_scalar_col_col_1000", |b| {
        let mut rng = rand::rng();
        let year = ColumnarValue::Scalar(ScalarValue::Int32(Some(2025)));
        let months_arr = Arc::new(months(&mut rng)) as ArrayRef;
        let batch_len = months_arr.len();
        let months = ColumnarValue::Array(months_arr);
        let days = ColumnarValue::Array(Arc::new(days(&mut rng)) as ArrayRef);
        let arg_fields = vec![
            Field::new("a", year.data_type(), true).into(),
            Field::new("a", months.data_type(), true).into(),
            Field::new("a", days.data_type(), true).into(),
        ];
        let return_field = Field::new("f", DataType::Date32, true).into();
        let config_options = Arc::new(ConfigOptions::default());

        b.iter(|| {
            black_box(
                make_date()
                    .invoke_with_args(ScalarFunctionArgs {
                        args: vec![year.clone(), months.clone(), days.clone()],
                        arg_fields: arg_fields.clone(),
                        number_rows: batch_len,
                        return_field: Arc::clone(&return_field),
                        config_options: Arc::clone(&config_options),
                    })
                    .expect("make_date should work on valid values"),
            )
        })
    });

    c.bench_function("make_date_scalar_scalar_col_1000", |b| {
        let mut rng = rand::rng();
        let year = ColumnarValue::Scalar(ScalarValue::Int32(Some(2025)));
        let month = ColumnarValue::Scalar(ScalarValue::Int32(Some(11)));
        let day_arr = Arc::new(days(&mut rng));
        let batch_len = day_arr.len();
        let days = ColumnarValue::Array(day_arr);
        let arg_fields = vec![
            Field::new("a", year.data_type(), true).into(),
            Field::new("a", month.data_type(), true).into(),
            Field::new("a", days.data_type(), true).into(),
        ];
        let return_field = Field::new("f", DataType::Date32, true).into();
        let config_options = Arc::new(ConfigOptions::default());

        b.iter(|| {
            black_box(
                make_date()
                    .invoke_with_args(ScalarFunctionArgs {
                        args: vec![year.clone(), month.clone(), days.clone()],
                        arg_fields: arg_fields.clone(),
                        number_rows: batch_len,
                        return_field: Arc::clone(&return_field),
                        config_options: Arc::clone(&config_options),
                    })
                    .expect("make_date should work on valid values"),
            )
        })
    });

    c.bench_function("make_date_scalar_scalar_scalar", |b| {
        let year = ColumnarValue::Scalar(ScalarValue::Int32(Some(2025)));
        let month = ColumnarValue::Scalar(ScalarValue::Int32(Some(11)));
        let day = ColumnarValue::Scalar(ScalarValue::Int32(Some(26)));
        let arg_fields = vec![
            Field::new("a", year.data_type(), true).into(),
            Field::new("a", month.data_type(), true).into(),
            Field::new("a", day.data_type(), true).into(),
        ];
        let return_field = Field::new("f", DataType::Date32, true).into();
        let config_options = Arc::new(ConfigOptions::default());

        b.iter(|| {
            black_box(
                make_date()
                    .invoke_with_args(ScalarFunctionArgs {
                        args: vec![year.clone(), month.clone(), day.clone()],
                        arg_fields: arg_fields.clone(),
                        number_rows: 1,
                        return_field: Arc::clone(&return_field),
                        config_options: Arc::clone(&config_options),
                    })
                    .expect("make_date should work on valid values"),
            )
        })
    });
}

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);
