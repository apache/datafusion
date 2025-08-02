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

use arrow::array::{ArrayRef, Date32Array, StringArray};
use arrow::datatypes::{DataType, Field};
use chrono::prelude::*;
use chrono::TimeDelta;
use criterion::{black_box, criterion_group, criterion_main, Criterion};
use datafusion_common::config::ConfigOptions;
use datafusion_common::ScalarValue;
use datafusion_common::ScalarValue::TimestampNanosecond;
use datafusion_expr::{ColumnarValue, ScalarFunctionArgs};
use datafusion_functions::datetime::to_char;
use rand::prelude::IndexedRandom;
use rand::rngs::ThreadRng;
use rand::Rng;

fn random_date_in_range(
    rng: &mut ThreadRng,
    start_date: NaiveDate,
    end_date: NaiveDate,
) -> NaiveDate {
    let days_in_range = (end_date - start_date).num_days();
    let random_days: i64 = rng.random_range(0..days_in_range);
    start_date + TimeDelta::try_days(random_days).unwrap()
}

fn data(rng: &mut ThreadRng) -> Date32Array {
    let mut data: Vec<i32> = vec![];
    let unix_days_from_ce = NaiveDate::from_ymd_opt(1970, 1, 1)
        .unwrap()
        .num_days_from_ce();
    let start_date = "1970-01-01"
        .parse::<NaiveDate>()
        .expect("Date should parse");
    let end_date = "2050-12-31"
        .parse::<NaiveDate>()
        .expect("Date should parse");
    for _ in 0..1000 {
        data.push(
            random_date_in_range(rng, start_date, end_date).num_days_from_ce()
                - unix_days_from_ce,
        );
    }

    Date32Array::from(data)
}

fn patterns(rng: &mut ThreadRng) -> StringArray {
    let samples = [
        "%Y:%m:%d".to_string(),
        "%d-%m-%Y".to_string(),
        "%d%m%Y".to_string(),
        "%Y%m%d".to_string(),
        "%Y...%m...%d".to_string(),
    ];
    let mut data: Vec<String> = vec![];
    for _ in 0..1000 {
        data.push(samples.choose(rng).unwrap().to_string());
    }

    StringArray::from(data)
}

fn criterion_benchmark(c: &mut Criterion) {
    let config_options = Arc::new(ConfigOptions::default());

    c.bench_function("to_char_array_array_1000", |b| {
        let mut rng = rand::rng();
        let data_arr = data(&mut rng);
        let batch_len = data_arr.len();
        let data = ColumnarValue::Array(Arc::new(data_arr) as ArrayRef);
        let patterns = ColumnarValue::Array(Arc::new(patterns(&mut rng)) as ArrayRef);

        b.iter(|| {
            black_box(
                to_char()
                    .invoke_with_args(ScalarFunctionArgs {
                        args: vec![data.clone(), patterns.clone()],
                        arg_fields: vec![
                            Field::new("a", data.data_type(), true).into(),
                            Field::new("b", patterns.data_type(), true).into(),
                        ],
                        number_rows: batch_len,
                        return_field: Field::new("f", DataType::Utf8, true).into(),
                        config_options: Arc::clone(&config_options),
                    })
                    .expect("to_char should work on valid values"),
            )
        })
    });

    c.bench_function("to_char_array_scalar_1000", |b| {
        let mut rng = rand::rng();
        let data_arr = data(&mut rng);
        let batch_len = data_arr.len();
        let data = ColumnarValue::Array(Arc::new(data_arr) as ArrayRef);
        let patterns =
            ColumnarValue::Scalar(ScalarValue::Utf8(Some("%Y-%m-%d".to_string())));

        b.iter(|| {
            black_box(
                to_char()
                    .invoke_with_args(ScalarFunctionArgs {
                        args: vec![data.clone(), patterns.clone()],
                        arg_fields: vec![
                            Field::new("a", data.data_type(), true).into(),
                            Field::new("b", patterns.data_type(), true).into(),
                        ],
                        number_rows: batch_len,
                        return_field: Field::new("f", DataType::Utf8, true).into(),
                        config_options: Arc::clone(&config_options),
                    })
                    .expect("to_char should work on valid values"),
            )
        })
    });

    c.bench_function("to_char_scalar_scalar_1000", |b| {
        let timestamp = "2026-07-08T09:10:11"
            .parse::<NaiveDateTime>()
            .unwrap()
            .with_nanosecond(56789)
            .unwrap()
            .and_utc()
            .timestamp_nanos_opt()
            .unwrap();
        let data = ColumnarValue::Scalar(TimestampNanosecond(Some(timestamp), None));
        let pattern = ColumnarValue::Scalar(ScalarValue::Utf8(Some(
            "%d-%m-%Y %H:%M:%S".to_string(),
        )));

        b.iter(|| {
            black_box(
                to_char()
                    .invoke_with_args(ScalarFunctionArgs {
                        args: vec![data.clone(), pattern.clone()],
                        arg_fields: vec![
                            Field::new("a", data.data_type(), true).into(),
                            Field::new("b", pattern.data_type(), true).into(),
                        ],
                        number_rows: 1,
                        return_field: Field::new("f", DataType::Utf8, true).into(),
                        config_options: Arc::clone(&config_options),
                    })
                    .expect("to_char should work on valid values"),
            )
        })
    });
}

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);
