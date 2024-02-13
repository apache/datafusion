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

use arrow_array::{ArrayRef, Date32Array, StringArray};
use chrono::prelude::*;
use chrono::Duration;
use criterion::{black_box, criterion_group, criterion_main, Criterion};
use rand::rngs::ThreadRng;
use rand::seq::SliceRandom;
use rand::Rng;

use datafusion_common::ScalarValue;
use datafusion_common::ScalarValue::TimestampNanosecond;
use datafusion_expr::ColumnarValue;
use datafusion_physical_expr::datetime_expressions::to_char;

fn random_date_in_range(
    rng: &mut ThreadRng,
    start_date: NaiveDate,
    end_date: NaiveDate,
) -> NaiveDate {
    let days_in_range = (end_date - start_date).num_days();
    let random_days: i64 = rng.gen_range(0..days_in_range);
    start_date + Duration::days(random_days)
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
    let samples = vec![
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
    c.bench_function("to_char_array_array_1000", |b| {
        let mut rng = rand::thread_rng();
        let data = ColumnarValue::Array(Arc::new(data(&mut rng)) as ArrayRef);
        let patterns = ColumnarValue::Array(Arc::new(patterns(&mut rng)) as ArrayRef);

        b.iter(|| {
            black_box(
                to_char(&[data.clone(), patterns.clone()])
                    .expect("to_char should work on valid values"),
            )
        })
    });

    c.bench_function("to_char_array_scalar_1000", |b| {
        let mut rng = rand::thread_rng();
        let data = ColumnarValue::Array(Arc::new(data(&mut rng)) as ArrayRef);
        let patterns =
            ColumnarValue::Scalar(ScalarValue::Utf8(Some("%Y-%m-%d".to_string())));

        b.iter(|| {
            black_box(
                to_char(&[data.clone(), patterns.clone()])
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
            .timestamp_nanos_opt()
            .unwrap();
        let data = ColumnarValue::Scalar(TimestampNanosecond(Some(timestamp), None));
        let pattern = ColumnarValue::Scalar(ScalarValue::Utf8(Some(
            "%d-%m-%Y %H:%M:%S".to_string(),
        )));

        b.iter(|| {
            black_box(
                to_char(&[data.clone(), pattern.clone()])
                    .expect("to_char should work on valid values"),
            )
        })
    });
}

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);
