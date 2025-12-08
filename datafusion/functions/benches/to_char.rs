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

use std::hint::black_box;
use std::sync::Arc;

use arrow::array::{ArrayRef, Date32Array, StringArray};
use arrow::datatypes::{DataType, Field};
use chrono::TimeDelta;
use chrono::prelude::*;
use criterion::{Criterion, criterion_group, criterion_main};
use datafusion_common::ScalarValue;
use datafusion_common::ScalarValue::TimestampNanosecond;
use datafusion_common::config::ConfigOptions;
use datafusion_expr::{ColumnarValue, ScalarFunctionArgs};
use datafusion_functions::datetime::to_char;
use rand::Rng;
use rand::prelude::IndexedRandom;
use rand::rngs::ThreadRng;

fn pick_date_in_range(
    rng: &mut ThreadRng,
    start_date: NaiveDate,
    end_date: NaiveDate,
) -> NaiveDate {
    let days_in_range = (end_date - start_date).num_days();
    let random_days: i64 = rng.random_range(0..days_in_range);
    start_date + TimeDelta::try_days(random_days).unwrap()
}

fn generate_date32_array(rng: &mut ThreadRng) -> Date32Array {
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
            pick_date_in_range(rng, start_date, end_date).num_days_from_ce()
                - unix_days_from_ce,
        );
    }

    Date32Array::from(data)
}

const DATE_PATTERNS: [&str; 5] =
    ["%Y:%m:%d", "%d-%m-%Y", "%d%m%Y", "%Y%m%d", "%Y...%m...%d"];

const DATETIME_PATTERNS: [&str; 8] = [
    "%Y:%m:%d %H:%M%S",
    "%Y:%m:%d %_H:%M%S",
    "%Y:%m:%d %k:%M%S",
    "%d-%m-%Y %I%P-%M-%S %f",
    "%d%m%Y %H",
    "%Y%m%d %M-%S %.3f",
    "%Y...%m...%d %T%3f",
    "%c",
];

fn pick_date_pattern(rng: &mut ThreadRng) -> String {
    (*DATE_PATTERNS
        .choose(rng)
        .expect("Empty list of date patterns"))
    .to_string()
}

fn pick_date_time_pattern(rng: &mut ThreadRng) -> String {
    (*DATETIME_PATTERNS
        .choose(rng)
        .expect("Empty list of date time patterns"))
    .to_string()
}

fn pick_date_and_date_time_mixed_pattern(rng: &mut ThreadRng) -> String {
    match rng.random_bool(0.5) {
        true => pick_date_pattern(rng),
        false => pick_date_time_pattern(rng),
    }
}

fn generate_pattern_array(
    rng: &mut ThreadRng,
    pick_fn: impl Fn(&mut ThreadRng) -> String,
) -> StringArray {
    let mut data = Vec::with_capacity(1000);

    for _ in 0..1000 {
        data.push(pick_fn(rng));
    }

    StringArray::from(data)
}

fn generate_date_pattern_array(rng: &mut ThreadRng) -> StringArray {
    generate_pattern_array(rng, pick_date_pattern)
}

fn generate_datetime_pattern_array(rng: &mut ThreadRng) -> StringArray {
    generate_pattern_array(rng, pick_date_time_pattern)
}

fn generate_mixed_pattern_array(rng: &mut ThreadRng) -> StringArray {
    generate_pattern_array(rng, pick_date_and_date_time_mixed_pattern)
}

fn criterion_benchmark(c: &mut Criterion) {
    let config_options = Arc::new(ConfigOptions::default());

    c.bench_function("to_char_array_date_only_patterns_1000", |b| {
        let mut rng = rand::rng();
        let data_arr = generate_date32_array(&mut rng);
        let batch_len = data_arr.len();
        let data = ColumnarValue::Array(Arc::new(data_arr) as ArrayRef);
        let patterns = ColumnarValue::Array(Arc::new(generate_date_pattern_array(
            &mut rng,
        )) as ArrayRef);

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

    c.bench_function("to_char_array_datetime_patterns_1000", |b| {
        let mut rng = rand::rng();
        let data_arr = generate_date32_array(&mut rng);
        let batch_len = data_arr.len();
        let data = ColumnarValue::Array(Arc::new(data_arr) as ArrayRef);
        let patterns = ColumnarValue::Array(Arc::new(generate_datetime_pattern_array(
            &mut rng,
        )) as ArrayRef);

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

    c.bench_function("to_char_array_mixed_patterns_1000", |b| {
        let mut rng = rand::rng();
        let data_arr = generate_date32_array(&mut rng);
        let batch_len = data_arr.len();
        let data = ColumnarValue::Array(Arc::new(data_arr) as ArrayRef);
        let patterns = ColumnarValue::Array(Arc::new(generate_mixed_pattern_array(
            &mut rng,
        )) as ArrayRef);

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

    c.bench_function("to_char_scalar_date_only_pattern_1000", |b| {
        let mut rng = rand::rng();
        let data_arr = generate_date32_array(&mut rng);
        let batch_len = data_arr.len();
        let data = ColumnarValue::Array(Arc::new(data_arr) as ArrayRef);
        let patterns =
            ColumnarValue::Scalar(ScalarValue::Utf8(Some(pick_date_pattern(&mut rng))));

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

    c.bench_function("to_char_scalar_datetime_pattern_1000", |b| {
        let mut rng = rand::rng();
        let data_arr = generate_date32_array(&mut rng);
        let batch_len = data_arr.len();
        let data = ColumnarValue::Array(Arc::new(data_arr) as ArrayRef);
        let patterns = ColumnarValue::Scalar(ScalarValue::Utf8(Some(
            pick_date_time_pattern(&mut rng),
        )));

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

    c.bench_function("to_char_scalar_1000", |b| {
        let mut rng = rand::rng();
        let timestamp = "2026-07-08T09:10:11"
            .parse::<NaiveDateTime>()
            .unwrap()
            .with_nanosecond(56789)
            .unwrap()
            .and_utc()
            .timestamp_nanos_opt()
            .unwrap();
        let data = ColumnarValue::Scalar(TimestampNanosecond(Some(timestamp), None));
        let pattern =
            ColumnarValue::Scalar(ScalarValue::Utf8(Some(pick_date_pattern(&mut rng))));

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
