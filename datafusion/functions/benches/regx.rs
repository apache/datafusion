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

use arrow::array::builder::StringBuilder;
use arrow::array::{ArrayRef, AsArray, Int64Array, StringArray};
use arrow::compute::cast;
use arrow::datatypes::DataType;
use criterion::{black_box, criterion_group, criterion_main, Criterion};
use datafusion_functions::regex::regexpcount::regexp_count_func;
use datafusion_functions::regex::regexplike::regexp_like;
use datafusion_functions::regex::regexpmatch::regexp_match;
use datafusion_functions::regex::regexpreplace::regexp_replace;
use rand::distributions::Alphanumeric;
use rand::rngs::ThreadRng;
use rand::seq::SliceRandom;
use rand::Rng;
use std::iter;
use std::sync::Arc;
fn data(rng: &mut ThreadRng) -> StringArray {
    let mut data: Vec<String> = vec![];
    for _ in 0..1000 {
        data.push(
            rng.sample_iter(&Alphanumeric)
                .take(7)
                .map(char::from)
                .collect(),
        );
    }

    StringArray::from(data)
}

fn regex(rng: &mut ThreadRng) -> StringArray {
    let samples = [
        ".*([A-Z]{1}).*".to_string(),
        "^(A).*".to_string(),
        r#"[\p{Letter}-]+"#.to_string(),
        r#"[\p{L}-]+"#.to_string(),
        "[a-zA-Z]_[a-zA-Z]{2}".to_string(),
    ];
    let mut data: Vec<String> = vec![];
    for _ in 0..1000 {
        data.push(samples.choose(rng).unwrap().to_string());
    }

    StringArray::from(data)
}

fn start(rng: &mut ThreadRng) -> Int64Array {
    let mut data: Vec<i64> = vec![];
    for _ in 0..1000 {
        data.push(rng.gen_range(1..5));
    }

    Int64Array::from(data)
}

fn flags(rng: &mut ThreadRng) -> StringArray {
    let samples = [Some("i".to_string()), Some("im".to_string()), None];
    let mut sb = StringBuilder::new();
    for _ in 0..1000 {
        let sample = samples.choose(rng).unwrap();
        if sample.is_some() {
            sb.append_value(sample.clone().unwrap());
        } else {
            sb.append_null();
        }
    }

    sb.finish()
}

fn criterion_benchmark(c: &mut Criterion) {
    c.bench_function("regexp_count_1000 string", |b| {
        let mut rng = rand::thread_rng();
        let data = Arc::new(data(&mut rng)) as ArrayRef;
        let regex = Arc::new(regex(&mut rng)) as ArrayRef;
        let start = Arc::new(start(&mut rng)) as ArrayRef;
        let flags = Arc::new(flags(&mut rng)) as ArrayRef;

        b.iter(|| {
            black_box(
                regexp_count_func(&[
                    Arc::clone(&data),
                    Arc::clone(&regex),
                    Arc::clone(&start),
                    Arc::clone(&flags),
                ])
                .expect("regexp_count should work on utf8"),
            )
        })
    });

    c.bench_function("regexp_count_1000 utf8view", |b| {
        let mut rng = rand::thread_rng();
        let data = cast(&data(&mut rng), &DataType::Utf8View).unwrap();
        let regex = cast(&regex(&mut rng), &DataType::Utf8View).unwrap();
        let start = Arc::new(start(&mut rng)) as ArrayRef;
        let flags = cast(&flags(&mut rng), &DataType::Utf8View).unwrap();

        b.iter(|| {
            black_box(
                regexp_count_func(&[
                    Arc::clone(&data),
                    Arc::clone(&regex),
                    Arc::clone(&start),
                    Arc::clone(&flags),
                ])
                .expect("regexp_count should work on utf8view"),
            )
        })
    });

    c.bench_function("regexp_like_1000", |b| {
        let mut rng = rand::thread_rng();
        let data = Arc::new(data(&mut rng)) as ArrayRef;
        let regex = Arc::new(regex(&mut rng)) as ArrayRef;
        let flags = Arc::new(flags(&mut rng)) as ArrayRef;

        b.iter(|| {
            black_box(
                regexp_like(&[Arc::clone(&data), Arc::clone(&regex), Arc::clone(&flags)])
                    .expect("regexp_like should work on valid values"),
            )
        })
    });

    c.bench_function("regexp_match_1000", |b| {
        let mut rng = rand::thread_rng();
        let data = Arc::new(data(&mut rng)) as ArrayRef;
        let regex = Arc::new(regex(&mut rng)) as ArrayRef;
        let flags = Arc::new(flags(&mut rng)) as ArrayRef;

        b.iter(|| {
            black_box(
                regexp_match::<i32>(&[
                    Arc::clone(&data),
                    Arc::clone(&regex),
                    Arc::clone(&flags),
                ])
                .expect("regexp_match should work on valid values"),
            )
        })
    });

    c.bench_function("regexp_replace_1000", |b| {
        let mut rng = rand::thread_rng();
        let data = Arc::new(data(&mut rng)) as ArrayRef;
        let regex = Arc::new(regex(&mut rng)) as ArrayRef;
        let flags = Arc::new(flags(&mut rng)) as ArrayRef;
        let replacement =
            Arc::new(StringArray::from_iter_values(iter::repeat("XX").take(1000)))
                as ArrayRef;

        b.iter(|| {
            black_box(
                regexp_replace::<i32, _, _>(
                    data.as_string::<i32>(),
                    regex.as_string::<i32>(),
                    replacement.as_string::<i32>(),
                    Some(&flags),
                )
                .expect("regexp_replace should work on valid values"),
            )
        })
    });
}

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);
