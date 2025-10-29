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

use arrow::array::{ArrayRef, Int64Array, StringArray};
use arrow::datatypes::{DataType, Field};
use criterion::{black_box, criterion_group, criterion_main, Criterion};
use datafusion_common::config::ConfigOptions;
use datafusion_expr::{ColumnarValue, ScalarFunctionArgs};
use datafusion_functions::unicode::substr_index;
use rand::distr::{Alphanumeric, Uniform};
use rand::prelude::Distribution;
use rand::Rng;

struct Filter<Dist, Test> {
    dist: Dist,
    test: Test,
}

impl<T, Dist, Test> Distribution<T> for Filter<Dist, Test>
where
    Dist: Distribution<T>,
    Test: Fn(&T) -> bool,
{
    fn sample<R: Rng + ?Sized>(&self, rng: &mut R) -> T {
        loop {
            let x = self.dist.sample(rng);
            if (self.test)(&x) {
                return x;
            }
        }
    }
}

fn data() -> (StringArray, StringArray, Int64Array) {
    let dist = Filter {
        dist: Uniform::new(-4, 5),
        test: |x: &i64| x != &0,
    };
    let mut rng = rand::rng();
    let mut strings: Vec<String> = vec![];
    let mut delimiters: Vec<String> = vec![];
    let mut counts: Vec<i64> = vec![];

    for _ in 0..1000 {
        let length = rng.random_range(20..50);
        let text: String = (&mut rng)
            .sample_iter(&Alphanumeric)
            .take(length)
            .map(char::from)
            .collect();
        let char = rng.random_range(0..text.len());
        let delimiter = &text.chars().nth(char).unwrap();
        let count = rng.sample(dist.dist.unwrap());

        strings.push(text);
        delimiters.push(delimiter.to_string());
        counts.push(count);
    }

    (
        StringArray::from(strings),
        StringArray::from(delimiters),
        Int64Array::from(counts),
    )
}

fn criterion_benchmark(c: &mut Criterion) {
    c.bench_function("substr_index_array_array_1000", |b| {
        let (strings, delimiters, counts) = data();
        let batch_len = counts.len();
        let strings = ColumnarValue::Array(Arc::new(strings) as ArrayRef);
        let delimiters = ColumnarValue::Array(Arc::new(delimiters) as ArrayRef);
        let counts = ColumnarValue::Array(Arc::new(counts) as ArrayRef);

        let args = vec![strings, delimiters, counts];
        let arg_fields = args
            .iter()
            .enumerate()
            .map(|(idx, arg)| {
                Field::new(format!("arg_{idx}"), arg.data_type(), true).into()
            })
            .collect::<Vec<_>>();
        let config_options = Arc::new(ConfigOptions::default());

        b.iter(|| {
            black_box(
                substr_index()
                    .invoke_with_args(ScalarFunctionArgs {
                        args: args.clone(),
                        arg_fields: arg_fields.clone(),
                        number_rows: batch_len,
                        return_field: Field::new("f", DataType::Utf8, true).into(),
                        config_options: Arc::clone(&config_options),
                    })
                    .expect("substr_index should work on valid values"),
            )
        })
    });
}

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);
